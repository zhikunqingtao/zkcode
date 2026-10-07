//! Actual Rust Query -> run-private Python STDIO MCP -> immutable result and cleanup ledger.
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_llm::{
    ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry,
};
use zk_protocol::Usage;
use zk_server::{
    config::Config, engine_bridge::wire_engine, routes::build_router, state::AppState,
};
const MODEL: &str = "qwen3.8-max-0902";
const TOOL: &str = "mcp__query_private__pwd";
const SCRIPT: &str = r"
import sys,json,os
for line in sys.stdin:
    r=json.loads(line)
    if 'id' not in r: continue
    m=r['method']
    if m=='initialize': v={'protocolVersion':'2024-11-05','serverInfo':{'name':'fixture','version':'1'},'capabilities':{'tools':{}}}
    elif m=='tools/list': v={'tools':[{'name':'pwd','description':'Workspace identity','inputSchema':{'type':'object'},'annotations':{'readOnlyHint':True}}]}
    elif m=='tools/call': v={'content':[{'type':'text','text':os.getcwd()}]}
    else: v={}
    print(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':v}),flush=True)
";
#[derive(Default)]
struct Provider(Mutex<Vec<ChatRequest>>);
impl ChatProvider for Provider {
    fn provider_name(&self) -> &'static str {
        "query-mcp-fixture"
    }
    fn chat_stream(
        &self,
        request: ChatRequest,
        _: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let mut calls = self.0.lock().unwrap();
        assert!(calls.len() < 2, "no replay or hidden retry");
        assert!(request.tools.iter().any(|tool| tool.name == TOOL));
        let first = calls.is_empty();
        calls.push(request);
        let mut events = if first {
            vec![
                ProviderEvent::ToolUseStart {
                    id: "cwd-call".into(),
                    name: TOOL.into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "cwd-call".into(),
                    delta: "{}".into(),
                },
            ]
        } else {
            vec![ProviderEvent::TextDelta {
                text: "checked".into(),
            }]
        };
        events.push(ProviderEvent::Finish {
            finish_reason: if first {
                FinishReason::ToolUse
            } else {
                FinishReason::EndTurn
            },
            usage: Some(Usage {
                input_tokens: 10,
                output_tokens: 3,
                ..Usage::default()
            }),
        });
        Ok(stream::iter(events).boxed())
    }
}
#[tokio::test]
async fn private_mcp_is_executable_owned_cleaned_and_never_installed_globally() {
    let path = std::env::temp_dir().join(format!("zk-query-mcp-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    let db = Db::open(path.join("audit.sqlite")).unwrap();
    let mut config = Config::test_config();
    config.default_model = MODEL.into();
    config.workspace_default_root = path.to_string_lossy().into_owned();
    config.snapshot_dir = Some(path.join("snapshots"));
    config.mcp_registry_path = path.join("mcp.json");
    let provider = Arc::new(Provider::default());
    let mut providers = ProviderRegistry::new();
    providers.register("query-mcp-fixture", provider.clone(), vec![MODEL.into()]);
    let state =
        AppState::new(db.clone(), config).with_providers(providers.with_default_model(MODEL));
    state
        .set_startup_epoch(db.begin_runtime_startup_epoch().await.unwrap())
        .unwrap();
    let _engine = wire_engine(&state);
    let session = db
        .create_session(MODEL, path.to_str().unwrap())
        .await
        .unwrap();
    // Fixture explicitly grants execution; mcp-config itself authorizes only connection setup.
    state
        .authz
        .modes
        .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
        .await
        .unwrap();
    let global = state.tools();
    let mcp = state.mcp();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            build_router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let response=reqwest::Client::new().post(format!("http://{address}/api/query")).header("origin","http://127.0.0.1:5273").json(&json!({
        "prompt":"check workspace","sessionId":session.id,"allowedTools":[TOOL],
        "mcpConfig":{"mcpServers":{"query_private":{"command":"/usr/bin/python3","args":["-u","-c",SCRIPT],"env":{"SECRET_TOKEN":"PRIVATE_CONFIG_SHOULD_NOT_PERSIST_7193"}}}}
    })).timeout(std::time::Duration::from_secs(30)).send().await.unwrap();
    let status = response.status();
    let outcome: Value = response.json().await.unwrap();
    assert!(status.is_success(), "{status}: {outcome}");
    assert!(outcome["error"].is_null(), "{outcome}");
    assert_eq!(outcome["toolCalls"].as_array().unwrap().len(), 1);
    assert_eq!(outcome["toolCalls"][0]["isError"], false, "{outcome}");
    assert!(
        outcome["toolCalls"][0]["output"]
            .as_str()
            .unwrap()
            .contains(path.file_name().unwrap().to_str().unwrap())
    );
    assert_eq!(provider.0.lock().unwrap().len(), 2);
    assert!(global.get(TOOL).is_none());
    assert_eq!(mcp.connection_count(), 0);
    let run = db
        .find_run_by_id(outcome["runId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.cleanup_status, "confirmed");
    let run_id = run.id.clone();
    let (scopes,resources)=db.with_reader(move |conn| Ok((
        conn.query_row("SELECT count(*) FROM tool_invocations WHERE run_id=?1 AND tool_name='RunToolScope' AND status='succeeded' AND input_json='{}'",[&run_id],|row|row.get::<_,i64>(0))?,
        conn.query_row("SELECT count(*) FROM execution_resources WHERE run_id=?1 AND status='released'",[&run_id],|row|row.get::<_,i64>(0))?
    ))).await.unwrap();
    assert_eq!(scopes, 1);
    assert!(resources >= 1);
    for name in ["audit.sqlite", "audit.sqlite-wal"] {
        if let Ok(bytes) = std::fs::read(path.join(name)) {
            assert!(
                !String::from_utf8_lossy(&bytes).contains("PRIVATE_CONFIG_SHOULD_NOT_PERSIST_7193")
            );
        }
    }
    server.abort();
    let _ = server.await;
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
