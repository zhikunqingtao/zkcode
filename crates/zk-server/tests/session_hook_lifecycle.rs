//! Real REST session mutations, durable host owners, authorization and physical process cleanup.
mod common;

use std::{path::PathBuf, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use common::{call, json_body, local_delete, local_post};
use zk_authz::PermissionMode;
use zk_db::{MessageRole, NewMessage, StoredBlock};
use zk_server::{config::Config, engine_bridge::wire_engine, state::AppState};

struct Fixture {
    state: AppState,
    root: PathBuf,
    session: String,
}
impl Fixture {
    async fn new(mode: PermissionMode) -> Self {
        Self::with_storage(mode, false).await
    }
    async fn with_storage(mode: PermissionMode, file_database: bool) -> Self {
        Self::with_deadline(mode, file_database, Duration::from_mins(30)).await
    }
    async fn with_deadline(mode: PermissionMode, file_database: bool, deadline: Duration) -> Self {
        let root = std::env::temp_dir().join(format!("zk-session-hook-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".zk")).unwrap();
        let root = root.canonicalize().unwrap();
        let db = if file_database {
            zk_db::Db::open(root.join("fixture.sqlite")).unwrap()
        } else {
            zk_db::Db::open_in_memory().unwrap()
        };
        let mut config = Config::test_config();
        config.workspace_default_root = root.to_string_lossy().into_owned();
        config.scratchpad_system_root = root.join("scratch");
        config.mcp_registry_path = root.join("mcp.json");
        config.root_task_budget_policy.deadline = deadline;
        let state = AppState::new(db.clone(), config);
        state
            .set_startup_epoch(db.begin_runtime_startup_epoch().await.unwrap())
            .unwrap();
        let _engine = wire_engine(&state);
        let session = db
            .create_session("fixture", root.to_str().unwrap())
            .await
            .unwrap()
            .id;
        state.authz.modes.set_mode(&session, mode).await.unwrap();
        Self {
            state,
            root,
            session,
        }
    }
    fn router(&self) -> Router {
        zk_server::routes::build_router(self.state.clone())
    }
    async fn request(&self, request: Request<Body>) -> (StatusCode, serde_json::Value) {
        let mut app = self.router();
        let (status, _, body) =
            tokio::time::timeout(Duration::from_secs(15), call(&mut app, request))
                .await
                .expect("bounded lifecycle request");
        (status, json_body(&body))
    }
    fn write(&self, hooks: &str) {
        std::fs::write(self.root.join(".zk/hooks.toml"), hooks).unwrap();
    }
    fn delete(&self) -> Request<Body> {
        local_delete(&format!("/api/sessions/{}", self.session))
    }
    fn compact(&self) -> Request<Body> {
        local_post(&format!("/api/sessions/{}/compact", self.session), None)
    }
    async fn task_ids(&self) -> Vec<String> {
        self.state
            .db
            .with_reader(|conn| {
                let mut stmt = conn.prepare("SELECT id FROM tasks ORDER BY created_at,id")?;
                Ok(stmt
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<Vec<_>, _>>()?)
            })
            .await
            .unwrap()
    }
    async fn seed(&self) {
        for n in 0..6 {
            for (role, text) in [
                (
                    MessageRole::User,
                    format!("requested revision {n} in the fixture project"),
                ),
                (
                    MessageRole::Assistant,
                    "A long completed answer for deterministic compaction. ".repeat(80),
                ),
            ] {
                self.state
                    .db
                    .append_message(
                        &self.session,
                        NewMessage {
                            meta: None,
                            role,
                            content: vec![StoredBlock::Text { text }],
                            stop_reason: Some("end_turn".into()),
                            input_tokens: 0,
                            output_tokens: 0,
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    }
    async fn assert_clean_task(&self, expected: zk_db::TaskStatus) {
        let ids = self.task_ids().await;
        assert_eq!(ids.len(), 1);
        let task = self
            .state
            .db
            .find_runtime_task_by_id(&ids[0])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.status, expected);
        assert!(matches!(
            task.cleanup_status,
            zk_db::CleanupStatus::Confirmed | zk_db::CleanupStatus::NotRequired
        ));
        assert_eq!(task.task_type, "shell");
        assert_eq!(task.description, "Local session lifecycle hooks");
        let config: serde_json::Value = serde_json::from_str(&task.execution_config_json).unwrap();
        assert_eq!(config["executor"], "localHook");
        assert!(task.prompt.is_none());
        self.state
            .db
            .ensure_session_idle(&self.session)
            .await
            .unwrap();
        let tool_calls: i64 = self
            .state
            .db
            .with_reader(|conn| {
                Ok(conn.query_row(
                    "SELECT COALESCE(SUM(tool_call_count),0) FROM run_envelopes",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(
            tool_calls, 0,
            "host Hooks do not fabricate model tool usage"
        );
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn no_hooks_preserve_compact_and_delete_without_allocating_empty_tasks() {
    let f = Fixture::new(PermissionMode::DontAsk).await;
    f.seed().await;
    assert_eq!(f.request(f.compact()).await.0, StatusCode::OK);
    assert!(
        f.state
            .db
            .get_session(&f.session)
            .await
            .unwrap()
            .unwrap()
            .summary
            .is_some()
    );
    assert!(f.task_ids().await.is_empty());
    assert_eq!(f.request(f.delete()).await.0, StatusCode::OK);
    assert!(f.state.db.get_session(&f.session).await.unwrap().is_none());
    assert!(f.task_ids().await.is_empty());
}

#[tokio::test]
async fn required_session_end_hook_denial_preserves_session_and_has_clean_owner() {
    for mode in [PermissionMode::Plan, PermissionMode::DontAsk] {
        let f = Fixture::new(mode).await;
        f.write("[[hook]]\nname='deletion-gate'\nevent='SESSION_END'\nrole='security'\ncommand=\"printf forbidden > marker; printf '{\\\"decision\\\":\\\"continue\\\"}'\"\n");
        let (status, body) = f.request(f.delete()).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(body["code"].as_str().unwrap().starts_with("HOOK_"));
        assert!(!f.root.join("marker").exists());
        assert!(f.state.db.get_session(&f.session).await.unwrap().is_some());
        assert!(
            f.state
                .authz
                .interactions
                .pending_views(&f.session)
                .await
                .unwrap()
                .is_empty()
        );
        f.assert_clean_task(zk_db::TaskStatus::Failed).await;
    }
}

#[tokio::test]
async fn session_end_notification_runs_before_delete_and_awaits_async_process_cleanup() {
    let f = Fixture::new(PermissionMode::AutoApprove).await;
    f.write("[[hook]]\nname='deletion-notice'\nevent='SESSION_END'\nrole='notification'\nasync=true\ncommand='cat > notice.json; sleep 0.05; printf completed > marker'\n");
    let (status, body) = f.request(f.delete()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        std::fs::read_to_string(f.root.join("marker")).unwrap(),
        "completed"
    );
    let notice = std::fs::read_to_string(f.root.join("notice.json")).unwrap();
    assert!(notice.contains("deletion has not committed"), "{notice}");
    assert!(f.state.db.get_session(&f.session).await.unwrap().is_none());
}

#[tokio::test]
async fn precompact_security_denial_does_not_save_summary_and_post_error_does_not_undo_save() {
    let f = Fixture::new(PermissionMode::AutoApprove).await;
    f.seed().await;
    f.write("[[hook]]\nname='precompact-gate'\nevent='PRE_COMPACT'\nrole='security'\ncommand=\"printf '{\\\"decision\\\":\\\"deny\\\",\\\"message\\\":\\\"fixture rejection\\\"}'\"\n");
    let (status, body) = f.request(f.compact()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        f.state
            .db
            .get_session(&f.session)
            .await
            .unwrap()
            .unwrap()
            .summary
            .is_none()
    );
    f.assert_clean_task(zk_db::TaskStatus::Failed).await;

    let f = Fixture::new(PermissionMode::AutoApprove).await;
    f.seed().await;
    f.write("[[hook]]\nname='precompact-gate'\nevent='PRE_COMPACT'\nrole='security'\ncommand=\"printf pre > pre-marker; printf '{\\\"decision\\\":\\\"continue\\\"}'\"\n[[hook]]\nname='postcompact-notice'\nevent='POST_COMPACT'\nrole='notification'\nasync=true\ncommand='printf post > post-marker; exit 17'\n");
    let (status, body) = f.request(f.compact()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        f.state
            .db
            .get_session(&f.session)
            .await
            .unwrap()
            .unwrap()
            .summary
            .is_some()
    );
    assert!(f.root.join("pre-marker").exists());
    assert!(f.root.join("post-marker").exists());
    f.assert_clean_task(zk_db::TaskStatus::Succeeded).await;
}

#[tokio::test]
async fn postcompact_is_not_fired_without_a_saved_summary() {
    let f = Fixture::new(PermissionMode::AutoApprove).await;
    f.write("[[hook]]\nname='postcompact-notice'\nevent='POST_COMPACT'\nrole='notification'\ncommand='touch marker'\n");
    let (status, body) = f.request(f.compact()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!f.root.join("marker").exists());
    assert!(
        f.state
            .db
            .get_session(&f.session)
            .await
            .unwrap()
            .unwrap()
            .summary
            .is_none()
    );
    f.assert_clean_task(zk_db::TaskStatus::Succeeded).await;
}

#[tokio::test]
async fn dropped_rest_request_cancels_owned_process_and_retains_mutation_lease_until_cleanup() {
    let f = Fixture::new(PermissionMode::AutoApprove).await;
    f.write("[[hook]]\nname='slow-deletion-notice'\nevent='SESSION_END'\nrole='notification'\ncommand='printf started > started; sleep 30; printf forbidden > marker'\ntimeout_secs=60\n");
    let mut router = f.router();
    let request = f.delete();
    let pending = tokio::spawn(async move { call(&mut router, request).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !f.root.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Hook started under owned root");
    let conversation = f.state.conversation().unwrap();
    assert!(
        conversation
            .try_reserve_session_mutation(&f.session)
            .is_none()
    );
    let (status, body) = f.request(f.compact()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "SESSION_BUSY");
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if f.state.db.ensure_session_idle(&f.session).await.is_ok()
                && conversation
                    .try_reserve_session_mutation(&f.session)
                    .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dropped request's finalizer confirms cleanup and releases lease");
    assert!(!f.root.join("marker").exists());
    assert!(f.state.db.get_session(&f.session).await.unwrap().is_some());
    f.assert_clean_task(zk_db::TaskStatus::Cancelled).await;
}

#[tokio::test]
async fn sqlite_read_failure_retains_cleanup_owner_and_session_lease_until_recovery() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let f = Fixture::with_storage(PermissionMode::AutoApprove, true).await;
    let unavailable = Arc::new(AtomicBool::new(false));
    let rejected_reads = Arc::new(AtomicUsize::new(0));
    // Reader-local views fault only actual SQLite reads. The durable writer and
    // process supervisor remain live; no production mock observer is substituted.
    let reader_count = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    for _ in 0..reader_count {
        let unavailable = unavailable.clone();
        let rejected = rejected_reads.clone();
        f.state.db.with_reader(move |conn| {
            conn.create_scalar_function("zk_test_hook_read_fault", 0,
                rusqlite::functions::FunctionFlags::SQLITE_UTF8, move |_| -> rusqlite::Result<i64> {
                    if unavailable.load(Ordering::SeqCst) {
                        rejected.fetch_add(1, Ordering::SeqCst);
                        Err(rusqlite::Error::UserFunctionError(Box::new(std::io::Error::other("fixture read outage"))))
                    } else { Ok(0) }
                })?;
            conn.execute_batch("CREATE TEMP VIEW tasks AS SELECT * FROM main.tasks WHERE zk_test_hook_read_fault()=0")?;
            Ok(())
        }).await.unwrap();
    }
    f.write("[[hook]]\nname='slow-deletion-notice'\nevent='SESSION_END'\nrole='notification'\ncommand='printf started > started; sleep 30; printf forbidden > marker'\ntimeout_secs=60\n");
    let mut router = f.router();
    let request = f.delete();
    let pending = tokio::spawn(async move { call(&mut router, request).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !f.root.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    unavailable.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        while rejected_reads.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fault reaches Task output reader and cancellation/reconciliation");
    let conversation = f.state.conversation().unwrap();
    assert!(
        conversation
            .try_reserve_session_mutation(&f.session)
            .is_none(),
        "read outage must not release mutation lease"
    );
    assert!(
        !pending.is_finished(),
        "finalizer must still own physical cleanup"
    );
    unavailable.store(false, Ordering::SeqCst);
    let (status, _, body) = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, StatusCode::CONFLICT, "{}", json_body(&body));
    assert!(!f.root.join("marker").exists());
    assert!(f.state.db.get_session(&f.session).await.unwrap().is_some());
    assert!(
        conversation
            .try_reserve_session_mutation(&f.session)
            .is_some()
    );
    f.state.db.ensure_session_idle(&f.session).await.unwrap();
}

#[tokio::test]
async fn lifecycle_uses_stricter_root_deadline_and_preserves_timeout_after_cleanup() {
    let f = Fixture::with_deadline(
        PermissionMode::AutoApprove,
        false,
        Duration::from_millis(500),
    )
    .await;
    f.write("[[hook]]\nname='slow-deletion-notice'\nevent='SESSION_END'\nrole='notification'\ncommand='printf started > started; sleep 30; printf forbidden > marker'\ntimeout_secs=60\n");
    let (status, body) = f.request(f.delete()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "TIMEOUT");
    assert!(
        f.root.join("started").exists(),
        "fixture must enter actual process execution"
    );
    assert!(!f.root.join("marker").exists());
    assert!(f.state.db.get_session(&f.session).await.unwrap().is_some());
    f.assert_clean_task(zk_db::TaskStatus::Failed).await;
    let task_id = f.task_ids().await.remove(0);
    let task = f
        .state
        .db
        .find_runtime_task_by_id(&task_id)
        .await
        .unwrap()
        .unwrap();
    let run = f
        .state
        .db
        .find_run_by_id(task.current_run_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.exit_reason.as_deref(), Some("timeout"));
}

#[tokio::test]
async fn rest_created_persistent_session_observes_authoritative_permission_changes() {
    let f = Fixture::new(PermissionMode::DontAsk).await;
    let (status, body) = f.request(local_post("/api/sessions", None)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let created = body["sessionId"].as_str().unwrap();
    assert_eq!(
        f.state.authz.modes.get_mode(created),
        PermissionMode::AutoApprove
    );
    f.state
        .db
        .set_session_permission_mode(created.into(), "PLAN".into())
        .await
        .unwrap();
    assert_eq!(
        f.state.authz.modes.get_mode(created),
        PermissionMode::Plan,
        "a persisted session must not inherit a process-local ephemeral permission override"
    );
}
