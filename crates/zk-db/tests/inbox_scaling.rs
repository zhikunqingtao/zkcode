//! Regression coverage for indexed inbox updates beyond bounded list pages.

use zk_db::{CasOutcome, CreateTaskWithRun, Db, InboxStatus};

fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn request(
    root_session_id: &str,
    task_id: String,
    run_id: String,
    transcript_session_id: String,
    parent: Option<(&str, &str)>,
) -> CreateTaskWithRun {
    let execution_config_json = parent.map_or_else(
        || {
            serde_json::json!({
                "budget": {
                    "tokenLimit": 1_000_000,
                    "costLimitNanosUsd": 1_000_000_000_000_i64,
                    "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                }
            })
            .to_string()
        },
        |_| r#"{"isolation":"readOnly","lifecycle":"attached"}"#.to_owned(),
    );
    CreateTaskWithRun {
        task_id,
        run_id,
        root_session_id: root_session_id.to_owned(),
        transcript_session_id,
        parent_task_id: parent.map(|(task_id, _)| task_id.to_owned()),
        parent_run_id: parent.map(|(_, run_id)| run_id.to_owned()),
        creator_tool_use_id: parent.map(|_| id()),
        ordinal: 0,
        description: "inbox scaling".to_owned(),
        prompt: parent.map(|_| "consume every durable message".to_owned()),
        task_type: "agent".to_owned(),
        model: "script".to_owned(),
        working_dir: "/tmp/zkcode-inbox-scaling".to_owned(),
        execution_config_json,
        startup_epoch: 1,
    }
}

#[tokio::test]
async fn scoped_inbox_cas_reaches_messages_beyond_the_listing_limit() {
    let db = Db::open_in_memory().expect("database");
    let session = db
        .create_session("script", "/tmp/zkcode-inbox-scaling")
        .await
        .expect("session");
    let root = db
        .create_task_with_run(&request(&session.id, id(), id(), session.id.clone(), None))
        .await
        .expect("root");
    let child = db
        .create_task_with_run(&request(
            &session.id,
            id(),
            id(),
            id(),
            Some((&root.task.id, &root.run_id)),
        ))
        .await
        .expect("child");

    let mut message_ids = Vec::with_capacity(1_001);
    for ordinal in 0..=1_000 {
        let message_id = db
            .enqueue_task_message(
                &session.id,
                &child.task.id,
                Some(&root.task.id),
                &format!("message {ordinal}"),
            )
            .await
            .expect("enqueue")
            .message_id;
        message_ids.push(message_id);
    }

    let first_page = db
        .read_task_inbox(&child.task.id, &[], 1_000)
        .await
        .expect("bounded listing");
    assert_eq!(first_page.len(), 1_000);
    let listed = first_page
        .iter()
        .map(|message| message.message_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let omitted_message_id = message_ids
        .iter()
        .find(|message_id| !listed.contains(message_id.as_str()))
        .expect("one durable message is necessarily outside the bounded page");

    assert_eq!(
        db.mark_task_inbox_message_for_task(
            &child.task.id,
            omitted_message_id,
            InboxStatus::Queued,
            InboxStatus::Delivered,
            None,
        )
        .await
        .expect("scoped delivery"),
        CasOutcome::Applied
    );
    assert_eq!(
        db.mark_task_inbox_message_for_task(
            &root.task.id,
            omitted_message_id,
            InboxStatus::Delivered,
            InboxStatus::Consumed,
            None,
        )
        .await
        .expect("wrong owner remains a normal miss"),
        CasOutcome::NotFound
    );
    assert_eq!(
        db.mark_task_inbox_message_for_task(
            &child.task.id,
            omitted_message_id,
            InboxStatus::Delivered,
            InboxStatus::Consumed,
            None,
        )
        .await
        .expect("scoped consumption"),
        CasOutcome::Applied
    );
}
