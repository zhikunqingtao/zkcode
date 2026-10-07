//! WP-12 real-chain evidence: a workspace PRE hook executes through `/bin/sh`,
//! its untrusted output is submitted to the production authorization/admission
//! stack, and only the admitted value reaches the real `Read` tool.

use std::sync::Arc;

use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_authz::PermissionMode;
use zk_db::time;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_engine::hook::{HookContext, HookService, PreHookDecision};
use zk_server::authz::EngineAdmission;
use zk_server::state::AppState;
use zk_tools::{ReadFileTool, ToolContext, ToolRegistry};

const SESSION: &str = "session-hook-admission";
const RUN: &str = "run-hook-admission";

struct TempRoot {
    path: std::path::PathBuf,
}

impl TempRoot {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("zk-hook-admission-real-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temporary root");
        Self {
            path: path.canonicalize().expect("canonical temporary root"),
        }
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).ok();
    }
}

fn write_transform_hook(workspace: &std::path::Path, file_path: &std::path::Path) {
    std::fs::create_dir_all(workspace.join(".zk")).expect("create hook directory");
    let output = json!({
        "decision": "continue",
        "input": { "file_path": file_path.to_string_lossy() }
    })
    .to_string();
    let config = format!(
        r#"
[[hook]]
name = "rewrite-read-path"
event = "pre-tool-execution"
role = "transform"
matcher = "^Read$"
priority = 1
command = '''printf '%s' '{output}' '''
timeout_secs = 5
"#
    );
    std::fs::write(workspace.join(".zk/hooks.toml"), config).expect("write hook config");
}

async fn seed_running_context(state: &AppState, workspace: &str) -> HookContext {
    state
        .db
        .create_project("Hook Admission Project", workspace)
        .await
        .expect("create trusted project");
    let workspace_root = std::path::PathBuf::from(workspace);
    let workspace = workspace.to_owned();
    state
        .db
        .with_writer(move |conn| {
            let now = time::format_rfc3339_micros(time::now_millis());
            conn.execute(
                "INSERT INTO sessions(id,model,working_dir,created_at,updated_at) \
                 VALUES(?1,'test-model',?2,?3,?3)",
                rusqlite::params![SESSION, workspace, now],
            )?;
            Ok(())
        })
        .await
        .expect("seed session");
    state
        .db
        .start_root_run_with_budget(
            RUN,
            SESSION,
            None,
            "test-model",
            &zk_db::TaskBudgetLimits {
                deadline_at_ms: Some(time::now_millis() + 60_000),
                ..Default::default()
            },
        )
        .await
        .expect("start owned root with a finite deadline");
    state
        .authz
        .modes
        .set_mode(SESSION, PermissionMode::AutoApprove)
        .await
        .expect("approve the fixture host operation independently of Read");
    let run = state.db.find_run_by_id(RUN).await.unwrap().unwrap();
    state
        .execution_supervisor
        .hook_context(
            &run.task_id,
            RUN,
            SESSION,
            &workspace_root,
            CancellationToken::new(),
        )
        .with_tool("Read")
}

async fn apply_hook(service: &HookService, context: &HookContext, input: &Value) -> Value {
    match service.evaluate_pre_tool(context, input).await {
        PreHookDecision::Continue { input } => input,
        denial @ PreHookDecision::Deny { .. } => {
            panic!("hook should return a transformed input, got {denial:?}")
        }
    }
}

#[tokio::test]
async fn transformed_input_is_readmitted_before_real_tool_execution() {
    let root = TempRoot::new();
    let workspace = root.path.join("workspace");
    let outside = root.path.join("outside");
    std::fs::create_dir_all(&workspace).expect("create workspace");
    std::fs::create_dir_all(&outside).expect("create outside directory");
    let allowed_file = workspace.join("allowed.txt");
    let outside_file = outside.join("outside.txt");
    std::fs::write(&allowed_file, "real admitted payload\n").expect("write allowed file");
    std::fs::write(&outside_file, "must not be read\n").expect("write outside file");
    let workspace_text = workspace.to_string_lossy().into_owned();

    let state = AppState::for_tests();
    let context = seed_running_context(&state, &workspace_text).await;

    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(ReadFileTool));
    let registry = Arc::new(registry);
    let admission = EngineAdmission::new_dont_ask(state.authz.clone(), Arc::clone(&registry));

    // A real shell hook rewrites an initially outside-workspace request to a
    // trusted file. Production Admission sees the rewritten input and permits it.
    write_transform_hook(&workspace, &allowed_file);
    let transformed = apply_hook(
        &state.hooks,
        &context,
        &json!({ "file_path": outside_file }),
    )
    .await;
    assert_eq!(transformed, json!({ "file_path": allowed_file }));
    let execution_input = match admission
        .admit(AdmissionRequest {
            session_id: SESSION,
            run_id: RUN,
            tool_use_id: "tool-use-hook-allow",
            tool_name: "Read",
            input: &transformed,
            working_directory: Some(&workspace_text),
        })
        .await
    {
        Admission::Allow { execution_input } => execution_input,
        other => panic!("trusted transformed path must be admitted, got {other:?}"),
    };

    let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
    let output = registry
        .get("Read")
        .expect("real Read tool registered")
        .execute(
            execution_input,
            ToolContext::new(CancellationToken::new(), progress_tx)
                .with_working_dir(&workspace)
                .with_session_id(SESSION)
                .with_tool_use_id("tool-use-hook-allow")
                .with_run_id(RUN),
        )
        .await;
    assert!(
        !output.is_error,
        "admitted real Read must succeed: {output:?}"
    );
    assert!(output.content.contains("real admitted payload"));

    // Hot-reload the same real workspace hook to rewrite to an untrusted path.
    // Re-admission must reject it, so the tool is never called with that value.
    write_transform_hook(&workspace, &outside_file);
    let transformed = apply_hook(
        &state.hooks,
        &context,
        &json!({ "file_path": allowed_file }),
    )
    .await;
    assert_eq!(transformed, json!({ "file_path": outside_file }));
    let denied = admission
        .admit(AdmissionRequest {
            session_id: SESSION,
            run_id: RUN,
            tool_use_id: "tool-use-hook-deny",
            tool_name: "Read",
            input: &transformed,
            working_directory: Some(&workspace_text),
        })
        .await;
    assert!(
        matches!(denied, Admission::Denied { .. }),
        "outside-workspace hook rewrite must be denied, got {denied:?}"
    );
    state.hooks.drain_run(RUN).await;
    assert_released_hook_resources(&state).await;
}

async fn assert_released_hook_resources(state: &AppState) {
    let resources: Vec<(String, Option<String>, String)> = state
        .db
        .with_reader(|conn| {
            let mut query = conn.prepare(
                "SELECT resource_kind,external_id,status FROM execution_resources WHERE run_id=?1",
            )?;
            Ok(query
                .query_map([RUN], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .unwrap();
    assert_eq!(
        resources.len(),
        2,
        "both real shell Hooks must have an owner"
    );
    for (kind, pid, status) in resources {
        assert_eq!(kind, "processGroup");
        assert_eq!(status, "released");
        assert!(
            !std::process::Command::new("/bin/kill")
                .args(["-0", &format!("-{}", pid.unwrap())])
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap()
                .success()
        );
    }
}
