//! Production Hook authority: real `SQLite`, local permission transport and supervised commands.
mod common;
use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;
use zk_authz::PermissionMode;
use zk_engine::hook::{HookContext, HookEvent};
use zk_server::{config::Config, state::AppState};

struct Fixture {
    state: AppState,
    root: PathBuf,
    session: String,
    run: String,
    context: HookContext,
}
impl Fixture {
    async fn new(mode: PermissionMode) -> Self {
        let root = std::env::temp_dir().join(format!("zk-host-hook-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".zk")).unwrap();
        let root = root.canonicalize().unwrap();
        let db = zk_db::Db::open_in_memory().unwrap();
        let session = db
            .create_session("fixture", root.to_str().unwrap())
            .await
            .unwrap()
            .id;
        let run = uuid::Uuid::new_v4().to_string();
        db.start_root_run_with_budget(
            &run,
            &session,
            None,
            "fixture",
            &zk_db::TaskBudgetLimits {
                deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut config = Config::test_config();
        config.workspace_default_root = root.to_string_lossy().into();
        let state = AppState::new(db.clone(), config);
        state.authz.modes.set_mode(&session, mode).await.unwrap();
        let record = db.find_run_by_id(&run).await.unwrap().unwrap();
        let context = state.execution_supervisor.hook_context(
            &record.task_id,
            &run,
            &session,
            &root,
            CancellationToken::new(),
        );
        Self {
            state,
            root,
            session,
            run,
            context,
        }
    }
    fn write_config(&self, role: &str, asynchronous: bool, prefix: &str) {
        let command = if role == "security" {
            "printf x >> marker; printf '{\"decision\":\"continue\"}'"
        } else {
            "printf x >> marker"
        };
        std::fs::write(self.root.join(".zk/hooks.toml"), format!("{prefix}\n[[hook]]\nname='fixture'\nevent='PRE_TOOL_USE'\nrole='{role}'\ncommand={}\nasync={asynchronous}\n", serde_json::to_string(command).unwrap())).unwrap();
    }
    async fn approve(&self, record: zk_protocol::InteractionView, scope: &str) {
        let interactions = &self.state.authz.interactions;
        assert!(
            interactions
                .mark_dispatched(&record.interaction_id, "test-browser")
                .await
                .unwrap()
        );
        let current = interactions
            .find_by_id(&record.interaction_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            interactions
                .acknowledge_received(
                    &record.interaction_id,
                    Some("test-browser"),
                    current.delivery_generation
                )
                .await
                .unwrap()
        );
        let current = interactions
            .find_by_id(&record.interaction_id)
            .await
            .unwrap()
            .unwrap();
        let view = zk_server::interaction::DurableInteractionService::view(&current).unwrap();
        let mut app = zk_server::routes::build_router(self.state.clone());
        let response = call(&mut app, local_with_headers(&format!("/api/interactions/{}/decisions", view.interaction_id), Method::POST,
            Some(json!({"expectedVersion":view.version,"optionId":format!("allow_{scope}"),"operationHash":view.operation_hash,"deliveryGeneration":view.delivery_generation}).to_string()), &[("x-session-id", &self.session)])).await;
        assert_eq!(response.0, StatusCode::OK, "{}", json_body(&response.2));
    }
    async fn fire_with_approval(&self, scope: &str, change: bool) {
        let execution = self
            .state
            .hooks
            .fire(HookEvent::PreToolExecution, &self.context);
        tokio::pin!(execution);
        let record = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! { () = &mut execution => panic!("Hook skipped local permission"), () = tokio::time::sleep(Duration::from_millis(5)) => {} }
                if let Some(record) = self.state.authz.interactions.pending_views(&self.session).await.unwrap().into_iter().next() { break record; }
            }
        }).await.unwrap();
        if change {
            self.write_config("notification", false, "# changed declaration\n");
            let path = self.root.join(".zk/hooks.toml");
            let text = std::fs::read_to_string(&path)
                .unwrap()
                .replace("printf x >> marker", "printf changed >> marker");
            std::fs::write(path, text).unwrap();
        }
        self.approve(record, scope).await;
        tokio::time::timeout(Duration::from_secs(5), &mut execution)
            .await
            .unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn plan_and_dont_ask_skip_optional_hook_but_required_security_denies() {
    for mode in [PermissionMode::Plan, PermissionMode::DontAsk] {
        let f = Fixture::new(mode).await;
        f.write_config("notification", false, "");
        let input = json!({"file_path":"safe.txt"});
        assert!(matches!(
            f.state.hooks.evaluate_pre_tool(&f.context, &input).await,
            zk_engine::PreHookDecision::Continue { .. }
        ));
        assert!(!f.root.join("marker").exists());
        f.write_config("security", false, "");
        assert!(matches!(
            f.state.hooks.evaluate_pre_tool(&f.context, &input).await,
            zk_engine::PreHookDecision::Deny { .. }
        ));
        assert!(!f.root.join("marker").exists());
        assert!(
            f.state
                .authz
                .interactions
                .pending_views(&f.session)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn session_grant_reuses_only_unchanged_semantics_and_revocation_blocks() {
    let f = Fixture::new(PermissionMode::Default).await;
    f.write_config("notification", false, "");
    f.fire_with_approval("session", false).await;
    assert_eq!(std::fs::read_to_string(f.root.join("marker")).unwrap(), "x");
    f.state
        .authz
        .modes
        .set_mode(&f.session, PermissionMode::DontAsk)
        .await
        .unwrap();
    f.write_config("notification", false, "# harmless comment\n\n");
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    assert_eq!(
        std::fs::read_to_string(f.root.join("marker")).unwrap(),
        "xx"
    );
    let grants = f
        .state
        .authz
        .grants
        .list_active_for_session(&f.session, 100)
        .await
        .unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].kind, "EXACT_GUARDED");
    f.state
        .authz
        .grants
        .revoke(&grants[0].grant_id)
        .await
        .unwrap();
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    assert_eq!(
        std::fs::read_to_string(f.root.join("marker")).unwrap(),
        "xx"
    );
}

#[tokio::test]
async fn config_change_while_waiting_never_executes_approved_old_command() {
    let f = Fixture::new(PermissionMode::Default).await;
    f.write_config("notification", false, "");
    f.fire_with_approval("once", true).await;
    assert!(!f.root.join("marker").exists());
}

#[tokio::test]
async fn once_permission_is_not_reused_and_async_hook_retains_owned_cleanup() {
    let f = Fixture::new(PermissionMode::Default).await;
    f.write_config("notification", false, "");
    f.fire_with_approval("once", false).await;
    f.state
        .authz
        .modes
        .set_mode(&f.session, PermissionMode::DontAsk)
        .await
        .unwrap();
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    assert_eq!(std::fs::read_to_string(f.root.join("marker")).unwrap(), "x");
    f.state
        .authz
        .modes
        .set_mode(&f.session, PermissionMode::AutoApprove)
        .await
        .unwrap();
    f.write_config("notification", true, "");
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    f.state.hooks.drain_run(&f.run).await;
    assert_eq!(
        std::fs::read_to_string(f.root.join("marker")).unwrap(),
        "xx"
    );
}

#[tokio::test]
async fn failed_admission_audit_never_starts_command_and_hook_does_not_count_as_model_tool() {
    let f = Fixture::new(PermissionMode::AutoApprove).await;
    f.write_config("notification", false, "");
    f.state.db.with_writer(|conn| {
        conn.execute_batch("CREATE TRIGGER deny_hook_audit BEFORE INSERT ON run_event_log WHEN NEW.event_type='hook_admitted' BEGIN SELECT RAISE(ABORT,'test hook audit write failure'); END;")?;
        Ok(())
    }).await.unwrap();
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    assert!(!f.root.join("marker").exists());
    f.state
        .db
        .with_writer(|conn| {
            conn.execute_batch("DROP TRIGGER deny_hook_audit")?;
            Ok(())
        })
        .await
        .unwrap();
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    assert_eq!(std::fs::read_to_string(f.root.join("marker")).unwrap(), "x");
    let run = f.state.db.find_run_by_id(&f.run).await.unwrap().unwrap();
    assert_eq!(run.tool_call_count, 0);
    let run_id = f.run.clone();
    let admitted = f
        .state
        .db
        .with_reader(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM run_event_log WHERE run_id=?1 AND event_type='hook_admitted'",
                [run_id],
                |r| r.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(admitted, 1);
}

#[tokio::test]
async fn revocation_after_resource_bind_blocks_physical_command_start() {
    let f = Fixture::new(PermissionMode::Default).await;
    f.write_config("notification", false, "");
    f.fire_with_approval("session", false).await;
    f.state
        .authz
        .modes
        .set_mode(&f.session, PermissionMode::DontAsk)
        .await
        .unwrap();
    f.state.db.with_writer(|conn| {
        conn.execute_batch("CREATE TRIGGER revoke_hook_before_start AFTER UPDATE OF external_id ON execution_resources WHEN NEW.resource_kind='processGroup' AND OLD.external_id IS NULL AND NEW.external_id IS NOT NULL BEGIN UPDATE permission_grants SET revoked_at='2026-10-07T00:00:00Z',version=version+1 WHERE revoked_at IS NULL AND tool_name='Hook'; END;")?;
        Ok(())
    }).await.unwrap();
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    assert_eq!(
        std::fs::read_to_string(f.root.join("marker")).unwrap(),
        "x",
        "revoking the exact Session grant during resource bind must prevent sending the command start gate"
    );
}

#[tokio::test]
async fn plan_mode_after_resource_bind_blocks_physical_command_start() {
    let f = Fixture::new(PermissionMode::AutoApprove).await;
    f.write_config("notification", false, "");
    f.state.db.with_writer(|conn| {
        conn.execute_batch("CREATE TRIGGER plan_hook_before_start AFTER UPDATE OF external_id ON execution_resources WHEN NEW.resource_kind='processGroup' AND OLD.external_id IS NULL AND NEW.external_id IS NOT NULL BEGIN UPDATE sessions SET permission_mode='PLAN' WHERE id=(SELECT session_id FROM run_envelopes WHERE id=NEW.run_id); END;")?;
        Ok(())
    }).await.unwrap();
    f.state
        .hooks
        .fire(HookEvent::PreToolExecution, &f.context)
        .await;
    assert!(
        !f.root.join("marker").exists(),
        "PLAN entered before physical start must win the admission/bind gap"
    );
}
