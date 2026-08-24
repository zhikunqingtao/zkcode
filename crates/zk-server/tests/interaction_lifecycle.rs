//! 2.5 权限管线 + 交互生命周期的**闭环实测证据**（brief 硬门禁 3-5）。
//!
//! 本文件不是旧测试的翻译（旧对应物是 Spring 的 `@SpringBootTest` 端到端
//! `PermissionFlowIntegrationTest` 一族），而是把 brief 要求的三项证据落成
//! 可复现断言，全部经**真实装配**（`AppState::for_tests` → `AuthzStack::build`
//! → `EngineAdmission` → 真 `ToolRegistry`）与**真实 REST 决策面**
//! （`POST /api/interactions/{id}/decisions`，旧 `InteractionController.java:52-172`）：
//!
//! | 门禁 | 测试 |
//! |---|---|
//! | deny 100% 拦截（`ABSOLUTE_DENY`） | [`absolute_deny_blocks_bash_before_any_interaction`] |
//! | deny 100% 拦截（受保护路径） | [`protected_path_read_is_denied_without_interaction`] |
//! | deny 100% 拦截（用户 deny） | [`user_deny_blocks_tool_execution`] |
//! | 同 hash 免弹（session grant） | [`session_grant_skips_second_prompt`] |
//! | 重启后 pending 重现可决策 | [`restart_recovers_pending_interaction_and_stays_decidable`] |
//!
//! # 形状偏离（详见 `docs/compatibility.md` §8）
//!
//! - **EV-01**：WS 帧不参与本文件断言。`WsHub::register` 是 `pub(crate)`，集成测试
//!   无法造连接，故投递闸门（`mark_dispatched` / `claim_redelivery` / 代次自增）在
//!   `ws::delivery` 的 crate 内联测试里断言；本文件直接调服务 API 模拟「已投递 +
//!   已 ACK」，验证的是决策链与授权缓存语义。
//! - **EV-02**：重启用「同一 `Db` 上新建第二个 `DurableInteractionService` 实例」
//!   模拟——交互权威全在库里，进程内只有等待器与容量信号量。旧实例的
//!   `await_terminal` 等待器在真实重启中随进程消失，故测试显式 `abort()` 它。

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use zk_authz::interaction::{InteractionRecord, InteractionStatus};
use zk_db::{Db, time};
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_server::authz::EngineAdmission;
use zk_server::interaction::runs;
use zk_server::interaction::service::{
    DurableInteractionService, NoopInteractionPublisher, RunTerminationRequest,
};
use zk_server::routes::build_router;
use zk_server::state::AppState;
use zk_tools::{BashTool, ReadFileTool, ToolRegistry};

/// 会话/Run 常量（旧集成测试同样固定 id 便于断言）。
const SESSION: &str = "s-lifecycle";
const RUN: &str = "r-lifecycle";

/// 自清理临时根（与 `zk-authz/tests/common` 同法，不引 `tempfile`）。
struct TempRoot {
    path: std::path::PathBuf,
}

impl TempRoot {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("zk-server-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("temp root");
        Self {
            path: path.canonicalize().expect("canonical temp root"),
        }
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// 静默 Run 终止端口（本文件不断言 Run 终止，旧 `ApplicationEventPublisher` 等价）。
#[derive(Debug)]
struct SilentTermination;

impl RunTerminationRequest for SilentTermination {
    fn request<'a>(
        &'a self,
        _run_id: &'a str,
        _exit_reason: &'a str,
        _detail: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(std::future::ready(()))
    }
}

/// 真实装配夹具：内存库 + 真 `AuthzStack` + 真 `ToolRegistry` + 真 Router。
struct Fixture {
    state: AppState,
    router: Router,
    admission: Arc<EngineAdmission>,
    /// 会话工作目录（授权根）。
    workspace: String,
    /// 工作区**外**的真实文件——`FileAnalyzer#fileRisk` 判 GUARDED，即需弹窗素材。
    outside_file: String,
    _temp: TempRoot,
}

impl Fixture {
    async fn new(tag: &str) -> Self {
        let temp = TempRoot::new(tag);
        let workspace = temp.path.join("workspace");
        let outside = temp.path.join("outside");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        std::fs::create_dir_all(&outside).expect("outside dir");
        let outside_file = outside.join("target.txt");
        std::fs::write(&outside_file, b"outside payload\n").expect("write outside file");

        let state = AppState::for_tests();
        let workspace_text = workspace.to_string_lossy().to_string();
        {
            let workspace_text = workspace_text.clone();
            state
                .db
                .with_writer(move |conn| {
                    let now = time::format_rfc3339_micros(time::now_millis());
                    conn.execute(
                        "INSERT INTO sessions(id,model,working_dir,created_at,updated_at) \
                         VALUES(?1,'test-model',?2,?3,?3)",
                        rusqlite::params![SESSION, workspace_text, now],
                    )?;
                    runs::start_in_current_write(
                        conn,
                        RUN,
                        SESSION,
                        None,
                        Some("main"),
                        "test-model",
                    )
                })
                .await
                .expect("seed session and run");
        }

        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(ReadFileTool));
        registry.register(Arc::new(BashTool));
        let admission = Arc::new(EngineAdmission::new(
            state.authz.clone(),
            Arc::new(registry),
        ));
        let router = build_router(state.clone());
        Self {
            state,
            router,
            admission,
            workspace: workspace_text,
            outside_file: outside_file.to_string_lossy().to_string(),
            _temp: temp,
        }
    }

    fn interactions(&self) -> &Arc<DurableInteractionService> {
        &self.state.authz.interactions
    }

    /// 同步准入（不弹窗路径：拒绝/放行当场返回）。
    async fn admit(&self, tool_use_id: &str, tool: &str, input: &Value) -> Admission {
        self.admission
            .admit(AdmissionRequest {
                session_id: SESSION,
                run_id: RUN,
                tool_use_id,
                tool_name: tool,
                input,
                working_directory: Some(&self.workspace),
            })
            .await
    }

    /// 后台准入（弹窗路径：`authorize` 会阻塞在 `await_terminal` 直到用户决策）。
    fn admit_in_background(
        &self,
        tool_use_id: &str,
        tool: &str,
        input: Value,
    ) -> tokio::task::JoinHandle<Admission> {
        let admission = self.admission.clone();
        let workspace = self.workspace.clone();
        let (tool_use_id, tool) = (tool_use_id.to_owned(), tool.to_owned());
        tokio::spawn(async move {
            admission
                .admit(AdmissionRequest {
                    session_id: SESSION,
                    run_id: RUN,
                    tool_use_id: &tool_use_id,
                    tool_name: &tool,
                    input: &input,
                    working_directory: Some(&workspace),
                })
                .await
        })
    }

    /// 读文件的 GUARDED 入参。
    fn guarded_read_input(&self) -> Value {
        json!({ "file_path": self.outside_file })
    }
}

/// 等待授权链把交互落库（旧集成测试的 `awaitPendingInteraction`）。
async fn wait_for_pending(
    service: &DurableInteractionService,
    session_id: &str,
) -> InteractionRecord {
    for _ in 0..250_u32 {
        let pending = service.pending(session_id).await.expect("pending query");
        if let Some(record) = pending.into_iter().next() {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("authorization chain never persisted a pending interaction");
}

/// 模拟「已投递 + 前端已 ACK」——投递闸门本体在 `ws::delivery` 内联测试断言（EV-01）。
async fn deliver_and_ack(
    service: &DurableInteractionService,
    record: &InteractionRecord,
    transport_id: &str,
) -> InteractionRecord {
    assert!(
        service
            .mark_dispatched(&record.interaction_id, transport_id)
            .await
            .expect("mark dispatched"),
        "首投必须 claim 成功"
    );
    let dispatched = service
        .find_by_id(&record.interaction_id)
        .await
        .expect("reload dispatched")
        .expect("interaction row exists");
    assert!(
        service
            .acknowledge_received(
                &dispatched.interaction_id,
                Some(transport_id),
                dispatched.delivery_generation,
            )
            .await
            .expect("acknowledge"),
        "同代次 ACK 必须打开决策窗口"
    );
    service
        .find_by_id(&record.interaction_id)
        .await
        .expect("reload acknowledged")
        .expect("interaction row exists")
}

/// 经真实 REST 面下决策（用户决策的唯一入口）。
async fn decide_via_rest(
    router: &mut Router,
    record: &InteractionRecord,
    option_id: &str,
) -> (StatusCode, Value) {
    let view = DurableInteractionService::view(record).expect("permission view");
    let operation_hash = view.operation_hash.expect("permission view carries hash");
    let body = json!({
        "expectedVersion": record.version,
        "optionId": option_id,
        "operationHash": operation_hash,
        "deliveryGeneration": record.delivery_generation,
    });
    let request = common::local_with_headers(
        &format!("/api/interactions/{}/decisions", record.interaction_id),
        Method::POST,
        Some(body.to_string()),
        &[("X-Session-Id", SESSION)],
    );
    let (status, _, bytes) = common::call(router, request).await;
    (status, common::json_body(&bytes))
}

/// `permission_grants` 行数（授权缓存是否落库的观测点）。
async fn grant_count(db: &Db) -> i64 {
    db.with_reader(|conn| {
        Ok(
            conn.query_row("SELECT COUNT(*) FROM permission_grants", [], |row| {
                row.get(0)
            })?,
        )
    })
    .await
    .expect("count grants")
}

// ── 门禁：deny 100% 拦截 ──────────────────────────────────────────────────

/// `ABSOLUTE_DENY` 走投无路：`rm -rf /` 在准入阶段即拒，且不进交互生命周期。
#[tokio::test]
async fn absolute_deny_blocks_bash_before_any_interaction() {
    let fixture = Fixture::new("absolute-deny").await;
    let input = json!({ "command": "rm -rf /" });

    let admission = fixture.admit("tu-deny-abs", "Bash", &input).await;

    let Admission::Denied { code, .. } = admission else {
        panic!("ABSOLUTE_DENY 必须拒绝执行，实得 {admission:?}");
    };
    assert_eq!(code, "COMMAND_ABSOLUTELY_DENIED");
    assert!(
        fixture
            .interactions()
            .pending(SESSION)
            .await
            .expect("pending query")
            .is_empty(),
        "硬不变量不得开交互（不弹窗）"
    );
    assert_eq!(grant_count(&fixture.state.db).await, 0, "不得写授权记录");
}

/// 受保护路径读：8 层路径安全在准入阶段拒绝，同样不弹窗。
#[tokio::test]
async fn protected_path_read_is_denied_without_interaction() {
    let fixture = Fixture::new("protected-path").await;
    let input = json!({ "file_path": "/dev/null" });

    let admission = fixture.admit("tu-deny-path", "Read", &input).await;

    let Admission::Denied { code, .. } = admission else {
        panic!("受保护路径必须拒绝执行，实得 {admission:?}");
    };
    assert_eq!(code, "PROTECTED_PATH_DENIED");
    assert!(
        fixture
            .interactions()
            .pending(SESSION)
            .await
            .expect("pending query")
            .is_empty(),
        "受保护路径不得开交互"
    );
}

/// 用户 deny：交互走完全程，最终仍不执行工具。
#[tokio::test]
async fn user_deny_blocks_tool_execution() {
    let mut fixture = Fixture::new("user-deny").await;
    let input = fixture.guarded_read_input();
    let pending = fixture.admit_in_background("tu-user-deny", "Read", input);

    let created = wait_for_pending(fixture.interactions(), SESSION).await;
    let acknowledged = deliver_and_ack(fixture.interactions(), &created, "t-deny").await;
    let (status, _) = decide_via_rest(&mut fixture.router, &acknowledged, "deny").await;
    assert_eq!(status, StatusCode::OK, "REST 决策必须落库成功");

    let admission = pending.await.expect("admission task joins");
    let Admission::Denied { code, .. } = admission else {
        panic!("用户 deny 必须阻断执行，实得 {admission:?}");
    };
    assert_eq!(code, "PERMISSION_USER_DENIED");
    assert_eq!(
        grant_count(&fixture.state.db).await,
        0,
        "deny 不得写授权记录"
    );
    let after = fixture
        .interactions()
        .find_by_id(&created.interaction_id)
        .await
        .expect("reload")
        .expect("row exists");
    assert_eq!(after.status, InteractionStatus::Denied);
}

// ── 门禁：同 hash 免弹 ───────────────────────────────────────────────────

/// `remember_choice = session` 后，同 `operationHash` 的第二次调用直接放行、零弹窗。
#[tokio::test]
async fn session_grant_skips_second_prompt() {
    let mut fixture = Fixture::new("session-grant").await;
    let input = fixture.guarded_read_input();
    let first = fixture.admit_in_background("tu-grant-1", "Read", input.clone());

    let created = wait_for_pending(fixture.interactions(), SESSION).await;
    let acknowledged = deliver_and_ack(fixture.interactions(), &created, "t-grant").await;
    let (status, body) = decide_via_rest(&mut fixture.router, &acknowledged, "allow_session").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["status"], "ANSWERED",
        "REST 回的是交互行本身（旧 `ResponseEntity.ok(updated)`）"
    );

    let first = first.await.expect("first admission joins");
    assert!(
        matches!(first, Admission::Allow { .. }),
        "用户 allow_session 后首次调用必须放行，实得 {first:?}"
    );
    assert_eq!(
        grant_count(&fixture.state.db).await,
        1,
        "remember=session 必须落一条授权记录"
    );

    // 第二次同 hash 调用：无交互、无等待，直接放行。
    let second = fixture.admit("tu-grant-2", "Read", &input).await;
    assert!(
        matches!(second, Admission::Allow { .. }),
        "同 hash 必须免弹直接放行，实得 {second:?}"
    );
    assert!(
        fixture
            .interactions()
            .pending(SESSION)
            .await
            .expect("pending query")
            .is_empty(),
        "免弹路径不得新建交互"
    );
    assert_eq!(
        grant_count(&fixture.state.db).await,
        1,
        "缓存命中不得重复写授权记录"
    );
}

// ── 门禁：重启后 pending 重现可决策 ──────────────────────────────────────

/// 断线重连 + 进程重启：pending 交互经容量对账重现、代次前进使旧 ACK 失效、
/// 新实例仍可下决策。
#[tokio::test]
async fn restart_recovers_pending_interaction_and_stays_decidable() {
    let fixture = Fixture::new("restart").await;
    let input = fixture.guarded_read_input();
    let before_restart = fixture.admit_in_background("tu-restart", "Read", input);

    let created = wait_for_pending(fixture.interactions(), SESSION).await;
    // 重启前只完成首投、前端**未** ACK 就断线——这正是恢复投递的适用前提
    // （旧 `prepareRecoveryDelivery` 的 `received_at IS NULL` 守卫）。
    assert!(
        fixture
            .interactions()
            .mark_dispatched(&created.interaction_id, "t-old")
            .await
            .expect("mark dispatched"),
        "首投必须 claim 成功"
    );
    let dispatched = fixture
        .interactions()
        .find_by_id(&created.interaction_id)
        .await
        .expect("reload dispatched")
        .expect("row exists");
    assert_eq!(dispatched.delivery_generation, 1);
    assert!(dispatched.received_at.is_none(), "断线场景：ACK 未到达");

    // 真实重启：进程内等待器随进程消失（EV-02）。
    before_restart.abort();

    // 重启后的新实例（同一库；容量信号量按库内 pending 重建）。
    let restarted = Arc::new(DurableInteractionService::new(
        fixture.state.db.clone(),
        Arc::new(NoopInteractionPublisher),
        Arc::new(SilentTermination),
    ));
    let recovered = restarted
        .reconcile_capacity_after_restart()
        .await
        .expect("capacity reconciles");
    assert_eq!(recovered, 1, "启动期对账必须重现 1 条 pending 交互");

    // 断线重连补齐：视图带全套决策选项，前端可直接重弹。
    let views = restarted
        .pending_views(SESSION)
        .await
        .expect("pending views");
    assert_eq!(views.len(), 1);
    let options = views[0].options.as_ref().expect("permission view options");
    assert!(
        options.iter().any(
            |option| option.get("optionId") == Some(&Value::String("allow_session".to_owned()))
        ),
        "重放视图必须携带权威选项集"
    );

    // RECOVERY 档投递：代次 +1 → 旧代次 ACK 一律失效。
    let recovered_row = restarted
        .prepare_recovery_delivery(&created.interaction_id, "t-new")
        .await
        .expect("recovery delivery")
        .expect("row exists");
    assert_eq!(recovered_row.delivery_generation, 2, "恢复投递必须推进代次");
    assert!(
        !restarted
            .acknowledge_received(&created.interaction_id, Some("t-old"), 1)
            .await
            .expect("stale ack"),
        "旧代次（旧连接）的迟到 ACK 必须无效"
    );
    assert!(
        restarted
            .acknowledge_received(&created.interaction_id, Some("t-new"), 2)
            .await
            .expect("fresh ack"),
        "新代次 ACK 必须打开决策窗口"
    );

    // 重启后的决策仍然可落库（同 `operationHash`，无需重新分析）。
    let current = restarted
        .find_by_id(&created.interaction_id)
        .await
        .expect("reload")
        .expect("row exists");
    let view = DurableInteractionService::view(&current).expect("permission view");
    let operation_hash = view.operation_hash.expect("hash");
    let decided = restarted
        .decide_request(
            &current.interaction_id,
            current.version,
            InteractionStatus::Answered,
            Some(json!({
                "decision": "allow",
                "scope": "once",
                "remember": false,
                "optionId": "allow_once",
                "operationHash": operation_hash,
                "deliveryGeneration": current.delivery_generation,
            })),
            Some("user_allow"),
        )
        .await
        .expect("restarted instance decides the recovered interaction");
    assert_eq!(decided.status, InteractionStatus::Answered);
    assert_eq!(
        grant_count(&fixture.state.db).await,
        0,
        "allow_once 不写授权缓存"
    );
}
