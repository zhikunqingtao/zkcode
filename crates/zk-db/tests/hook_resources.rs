//! Hook dispatch is admitted atomically against its exact live Task/Run owner.
use zk_db::{CasOutcome, Db, NewExecutionResource, TaskBudgetLimits};

async fn fixture() -> (Db, String) {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap();
    let run = uuid::Uuid::new_v4().to_string();
    db.start_root_run_with_budget_at_epoch(
        &run,
        &session.id,
        Some("query"),
        "fixture",
        &TaskBudgetLimits {
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
            ..TaskBudgetLimits::default()
        },
        1,
    )
    .await
    .unwrap();
    (db, run)
}
fn resource(run: &str, id: &str) -> NewExecutionResource {
    NewExecutionResource {
        resource_id: id.into(),
        task_id: run.into(),
        run_id: run.into(),
        invocation_id: None,
        resource_kind: "processGroup".into(),
        external_id: None,
        metadata_json: "{}".into(),
    }
}
async fn cancel(db: &Db, run: &str) {
    let run = run.to_owned();
    db.with_writer(move |conn| {
        zk_db::run::request_cancel_in_current_write(conn, &run, zk_db::run::EXIT_USER_CANCELLED)?;
        Ok(())
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn live_hook_binding_is_idempotent_but_cancellation_closes_both_gates() {
    let (db, run) = fixture().await;
    db.register_hook_execution_resource(&resource(&run, "before-cancel"))
        .await
        .unwrap();
    assert_eq!(
        db.bind_hook_execution_resource_external("before-cancel", "owned-pid")
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    assert_eq!(
        db.bind_hook_execution_resource_external("before-cancel", "owned-pid")
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    assert_eq!(
        db.bind_hook_execution_resource_external("before-cancel", "other-pid")
            .await
            .unwrap(),
        CasOutcome::InvalidTransition
    );
    db.register_hook_execution_resource(&resource(&run, "allocated-only"))
        .await
        .unwrap();
    cancel(&db, &run).await;
    assert!(
        db.bind_hook_execution_resource_external("allocated-only", "must-not-start")
            .await
            .unwrap_err()
            .to_string()
            .contains("HOOK_RUN_NOT_ACTIVE")
    );
    assert!(
        db.register_hook_execution_resource(&resource(&run, "late"))
            .await
            .unwrap_err()
            .to_string()
            .contains("HOOK_RUN_NOT_ACTIVE")
    );
    let external: Option<String> = db
        .with_reader(|conn| {
            Ok(conn.query_row(
                "SELECT external_id FROM execution_resources WHERE resource_id='allocated-only'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert!(external.is_none());
}
#[tokio::test]
async fn terminal_owner_and_foreign_invocation_never_allocate_hook_resources() {
    let (db, run) = fixture().await;
    let mut foreign = resource(&run, "foreign");
    foreign.invocation_id = Some("other-invocation".into());
    assert!(
        db.register_hook_execution_resource(&foreign)
            .await
            .unwrap_err()
            .to_string()
            .contains("HOOK_INVOCATION_NOT_OWNED")
    );
    db.finish_run(&run, zk_db::run::EXIT_MODEL_FINISHED, None)
        .await
        .unwrap();
    assert!(
        db.register_hook_execution_resource(&resource(&run, "terminal"))
            .await
            .unwrap_err()
            .to_string()
            .contains("HOOK_RUN_NOT_ACTIVE")
    );
    let count: i64 = db
        .with_reader(|conn| {
            Ok(
                conn.query_row("SELECT count(*) FROM execution_resources", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn legacy_run_termination_accepts_a_parent_waiting_for_dependencies() {
    let (db, run) = fixture().await;
    let waiting_run = run.clone();
    db.with_writer(move |conn| {
        conn.execute(
            "UPDATE run_envelopes SET status='waitingDependencies' WHERE id=?1",
            [&waiting_run],
        )?;
        assert_eq!(
            zk_db::run::request_cancel_in_current_write(
                conn,
                &waiting_run,
                zk_db::run::EXIT_USER_CANCELLED
            )?,
            zk_db::run::TransitionResult::Applied
        );
        Ok(())
    })
    .await
    .unwrap();
    let actual = db.find_run_by_id(&run).await.unwrap().unwrap();
    assert_eq!(actual.status, "cancelling");
    assert_eq!(
        actual.requested_exit_reason.as_deref(),
        Some(zk_db::run::EXIT_USER_CANCELLED)
    );
    assert!(
        db.register_hook_execution_resource(&resource(&run, "after-waiting-cancel"))
            .await
            .is_err()
    );
}
