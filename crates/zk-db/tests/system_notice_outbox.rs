//! A persisted notice remains owned by its original execution across late delivery.
use serde_json::json;
use zk_db::{Db, MessageAttribution, MessageRole, NewMessage, StoredBlock};

#[tokio::test]
async fn late_notice_uses_persisted_owner_and_rejects_foreign_message_ids() {
    let db = Db::open_in_memory().unwrap();
    for session in ["owner", "other"] {
        db.create_session_with_id(session, "model", "/tmp")
            .await
            .unwrap();
    }
    db.start_run("original", "owner", None, None, "model")
        .await
        .unwrap();
    let message = db
        .append_attributed_message(
            "owner",
            NewMessage {
                meta: Some(json!({"subtype":"image_notice"})),
                role: MessageRole::System,
                content: vec![StoredBlock::Text {
                    text: "Image unavailable".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
            MessageAttribution {
                task_id: Some("original".into()),
                run_id: Some("original".into()),
                origin: "runtime".into(),
                source_task_id: None,
            },
        )
        .await
        .unwrap();
    db.complete_run("original", 0, 0.0, 1).await.unwrap();
    db.start_run("newer", "owner", None, None, "model")
        .await
        .unwrap();
    let payload = json!({"type":"system_message","message":{
        "type":"system","uuid":message.id,"timestamp":1,
        "content":"Image unavailable","subtype":"image_notice"
    }});
    let event = db
        .append_ws_outbox_event(
            "owner",
            "owner",
            None,
            None,
            "ws_system_message",
            None,
            &payload,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.source_run_id, "original");
    assert_eq!(event.source_task_id, "original");
    assert!(
        db.append_ws_outbox_event(
            "other",
            "other",
            None,
            None,
            "ws_system_message",
            None,
            &payload
        )
        .await
        .is_err()
    );
    assert!(
        db.append_ws_outbox_event(
            "owner",
            "owner",
            None,
            None,
            "ws_system_message",
            None,
            &json!({"message":{"uuid":"unknown"}})
        )
        .await
        .is_err()
    );
}
