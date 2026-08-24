//! 任务 #58 回归：`PROJECT_FILE_SCOPE` 终局复检的 writer `Mutex` 重入死锁。
//!
//! 死锁链（thread sample 确证）：`ToolExecutionGateway::admit_inner` 在
//! `Db::with_writer` 闭包内调 `final_grant_recheck_in_current_transaction`，
//! 对 `PROJECT_FILE_SCOPE` 重验 `WorkspaceTrustProbe::is_trusted_file_scope`
//! → `Db::find_project_by_workspace_root_blocking` 再锁同一 writer `Mutex`
//! → 永久死锁（`std::sync::Mutex` 不可重入），run 卡 running。
//!
//! 修复：复验改走 `is_trusted_file_scope_in_current_write(conn, ...)`，复用
//! 已持有的事务连接（旧源 `isTrustedFileScope` 走同线程 JDBC 事务上下文的
//! Rust 等价）。两个测试均以 `tokio::time::timeout` 包裹——修复前该路径
//! 永久挂起，超时护栏保证回归时 CI 失败而非挂死。

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use zk_authz::model::DiagnosticSource;
use zk_authz::tool_facts::{ToolFacts, ToolUseContext};
use zk_db::time;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_server::authz::EngineAdmission;
use zk_server::interaction::runs;
use zk_server::state::AppState;
use zk_tools::{ToolRegistry, WriteFileTool};

const SESSION: &str = "session-project-recheck";
const RUN: &str = "run-project-recheck";

/// 修复前死锁在 30 秒内必现（原始复现卡 3 分钟+）；修复后全链毫秒级返回。
const DEADLOCK_GUARD: Duration = Duration::from_secs(30);

/// 自删临时根（与 `interaction_lifecycle.rs` 同式样）。
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

/// 真实装配夹具：内存库 + 真 `AuthzStack`，workspace 已选定为 Project。
struct Fixture {
    state: AppState,
    /// 已落库 Project 的 id（撤销场景用）。
    project_id: String,
    /// 会话工作目录 = Project `workspace_root`（授权根）。
    workspace: String,
    _temp: TempRoot,
}

impl Fixture {
    async fn new(tag: &str) -> Self {
        let temp = TempRoot::new(tag);
        let workspace = temp.path.join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        // TempRoot 已 canonicalize；子目录无符号链接，直接取文本即为 canonical 键。
        let workspace_text = workspace.to_string_lossy().to_string();

        let state = AppState::for_tests();
        let project = state
            .db
            .create_project("Trusted Project", &workspace_text)
            .await
            .expect("create project");
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
        Self {
            state,
            project_id: project.id,
            workspace: workspace_text,
            _temp: temp,
        }
    }

    /// 工作区内写入的 GUARDED 入参（Project 信任 → `PROJECT_FILE_SCOPE` 放行）。
    fn project_write_input(&self) -> serde_json::Value {
        json!({
            "file_path": format!("{}/notes.txt", self.workspace),
            "content": "regression payload",
        })
    }
}

/// `Write` 的授权身份面（`FileAnalyzer` 按工具名路由 `file-v1`，入参自带
/// `file_path`——与 zk-authz `decision_matrix` 测试的 stub 同式样）。
#[derive(Debug)]
struct WriteFacts;

impl ToolFacts for WriteFacts {
    fn name(&self) -> &'static str {
        "Write"
    }
}

/// 修复前：本用例在 `gateway.admit` 内部 writer `Mutex` 重入，永久死锁
/// （超时护栏触发失败）；修复后：复验走事务内连接，准入放行。
#[tokio::test]
async fn project_file_scope_admit_completes_without_deadlock() {
    let fixture = Fixture::new("project-recheck-allow").await;

    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(WriteFileTool::new()));
    let admission = Arc::new(EngineAdmission::new(
        fixture.state.authz.clone(),
        Arc::new(registry),
    ));

    let input = fixture.project_write_input();
    let admitted = tokio::time::timeout(
        DEADLOCK_GUARD,
        admission.admit(AdmissionRequest {
            session_id: SESSION,
            run_id: RUN,
            tool_use_id: "tool-use-allow",
            tool_name: "Write",
            input: &input,
            working_directory: Some(&fixture.workspace),
        }),
    )
    .await
    .expect("admit must not deadlock on PROJECT_FILE_SCOPE final recheck");

    match admitted {
        Admission::Allow { .. } => {}
        other => panic!("trusted project write must be admitted, got {other:?}"),
    }
}

/// 撤销语义保持：授权判定后、准入前删除 Project → 终局复检在**事务内**看到
/// 撤销，拒绝 `AUTHORIZATION_FINAL_RECHECK_DENIED`（且同样不得死锁）。
#[tokio::test]
async fn project_file_scope_recheck_denies_after_project_revoked() {
    let fixture = Fixture::new("project-recheck-revoke").await;
    let stack = &fixture.state.authz;

    let facts = WriteFacts;
    let input = fixture.project_write_input();
    let frozen = stack.frozen.freeze("Write", &input).expect("freeze input");
    let context = ToolUseContext::new(
        Some(RUN.to_owned()),
        Some("tool-use-revoke".to_owned()),
        Some(SESSION.to_owned()),
    )
    .with_shell(Some(SESSION.to_owned()), Some(fixture.workspace.clone()));

    let allowed = stack
        .authorization
        .authorize(&facts, &frozen, input.clone(), &context)
        .await
        .expect("trusted project write is authorized");
    assert_eq!(allowed.reason_code, "PROJECT_FILE_SCOPE");
    assert_eq!(allowed.source, DiagnosticSource::Policy);

    // 授权与准入之间 Project 被删除——复验必须在当前写事务内看到最新状态。
    let deleted = fixture
        .state
        .db
        .delete_project(&fixture.project_id)
        .await
        .expect("delete project");
    assert!(deleted, "seeded project must exist before revocation");

    let denied = tokio::time::timeout(
        DEADLOCK_GUARD,
        stack.gateway.admit(&facts, &allowed, &context),
    )
    .await
    .expect("admit must not deadlock on revoked-project final recheck")
    .expect_err("revoked project must fail the final grant recheck");

    match denied {
        zk_authz::gateway::GatewayError::Denied(error) => {
            assert_eq!(error.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
        }
        other => panic!("expected final recheck denial, got {other:?}"),
    }
}
