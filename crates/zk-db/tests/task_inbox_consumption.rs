//! Consumption is a transcript transaction, not an in-memory transport acknowledgement.
use zk_db::{Db, InboxStatus, TaskBudgetLimits};

async fn active_root(db: &Db, session: &str) -> String {
    let run = uuid::Uuid::new_v4().to_string();
    db.start_root_run_with_budget_at_epoch(
        &run,
        session,
        Some("query"),
        "fixture",
        &TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(1_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    run
}

#[tokio::test]
async fn inbox_consumes_once_and_rolls_back_before_exposing_failed_transcript() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap().id;
    let run = active_root(&db, &session).await;
    let inbox = db
        .enqueue_task_message(&session, &run, None, "durable collaboration")
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        db.consume_task_inbox_at_boundary(&run, &run),
        db.consume_task_inbox_at_boundary(&run, &run)
    );
    assert_eq!(first.unwrap().len() + second.unwrap().len(), 1);
    assert_eq!(
        db.read_task_inbox(&run, &[], 100).await.unwrap()[0].status,
        InboxStatus::Consumed
    );
    let history = db.get_session(&session).await.unwrap().unwrap();
    assert_eq!(
        history
            .messages
            .iter()
            .filter(|message| message
                .meta
                .as_ref()
                .is_some_and(|meta| meta["inboxMessageId"] == inbox.message_id))
            .count(),
        1
    );
    let pending = db
        .enqueue_task_message(
            &session,
            &run,
            None,
            "must not enter context when persistence fails",
        )
        .await
        .unwrap();
    db.with_writer(|conn|{conn.execute_batch("CREATE TRIGGER fail_inbox_event BEFORE INSERT ON run_event_log WHEN NEW.event_type='teammate_message_consumed' BEGIN SELECT RAISE(ABORT,'injected failure'); END;")?;Ok(())}).await.unwrap();
    assert!(db.consume_task_inbox_at_boundary(&run, &run).await.is_err());
    let rows = db
        .read_task_inbox(&run, &[InboxStatus::Queued], 100)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].message_id, pending.message_id);
    let history = db.get_session(&session).await.unwrap().unwrap();
    assert!(!history.messages.iter().any(|message| {
        message
            .meta
            .as_ref()
            .is_some_and(|meta| meta["inboxMessageId"] == pending.message_id)
    }));
    assert!(
        db.consume_task_inbox_at_boundary(&run, &uuid::Uuid::new_v4().to_string())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn private_inbox_consumption_uses_same_ram_scope_and_refuses_expired_body() {
    let db = Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    let run = active_root(&db, &session).await;
    db.enqueue_task_message(&session, &run, None, "private inbox body")
        .await
        .unwrap();
    let messages = db.consume_task_inbox_at_boundary(&run, &run).await.unwrap();
    assert_eq!(messages.len(), 1);
    db.with_reader(|conn| {
        let (content, meta): (String, String) = conn.query_row(
            "SELECT content_json,metadata_json FROM messages WHERE role='user'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!(content.starts_with("{\"$zkEphemeralContent\""));
        assert!(meta.starts_with("{\"$zkEphemeralContent\""));
        Ok(())
    })
    .await
    .unwrap();
    db.enqueue_task_message(&session, &run, None, "expired inbox body")
        .await
        .unwrap();
    drop(lease);
    assert!(db.consume_task_inbox_at_boundary(&run, &run).await.is_err());
    db.reconcile_runtime_after_restart().await.unwrap();
    db.with_reader(|conn| {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM task_inbox_messages WHERE status='rejected'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn final_result_rejects_late_messages_instead_of_carrying_them_to_another_run() {
    for status in [
        zk_db::ResultStatus::Complete,
        zk_db::ResultStatus::Error,
        zk_db::ResultStatus::Cancelled,
    ] {
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session("fixture", "/tmp").await.unwrap().id;
        let run = active_root(&db, &session).await;
        db.enqueue_task_message(&session, &run, None, "arrived after final safe boundary")
            .await
            .unwrap();
        db.ensure_task_final_assistant(&run, &run, "actual final response")
            .await
            .unwrap();
        let task = db.find_runtime_task_by_id(&run).await.unwrap().unwrap();
        let result = db
            .commit_task_result_with_run_usage_fallback(
                &zk_db::CommitTaskResult {
                    task_id: run.clone(),
                    run_id: run.clone(),
                    expected_task_version: task.version,
                    status,
                    content: "actual final response".into(),
                    media_type: "text/plain".into(),
                    error_code: None,
                    cleanup_status: zk_db::CleanupStatus::NotRequired,
                    verification_status: zk_db::VerificationStatus::NotRequested,
                },
                zk_db::RunUsageFallback {
                    usage_complete: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            zk_db::CommitTaskResultOutcome::Committed { .. }
        ));
        let messages = db.read_task_inbox(&run, &[], 100).await.unwrap();
        assert_eq!(messages[0].status, InboxStatus::Rejected);
        assert_eq!(
            messages[0].rejection_reason.as_deref(),
            Some("INBOX_TARGET_RUN_CLOSED")
        );
        assert!(db.consume_task_inbox_at_boundary(&run, &run).await.is_err());
    }
}
