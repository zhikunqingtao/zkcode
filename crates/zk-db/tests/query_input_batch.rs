//! One JSONL request must preserve user boundaries without partial input commits.
use zk_db::{Db, MessageAttribution, MessageRole, NewMessage, StoredBlock};

fn message(text: &str) -> NewMessage {
    NewMessage {
        role: MessageRole::User,
        content: vec![StoredBlock::Text { text: text.into() }],
        meta: None,
        stop_reason: None,
        input_tokens: 0,
        output_tokens: 0,
    }
}

#[tokio::test]
async fn input_batch_preserves_order_and_rolls_back_all_rows_on_failure() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("test", "/tmp").await.unwrap();
    db.with_writer(|conn| {
        conn.execute_batch("CREATE TRIGGER fail_second_user BEFORE INSERT ON messages WHEN NEW.content_json LIKE '%second%' BEGIN SELECT RAISE(ABORT,'fixture failure'); END")?;
        Ok(())
    }).await.unwrap();
    assert!(
        db.append_user_input_batch(
            &session.id,
            vec![message("first"), message("second")],
            MessageAttribution::conversation()
        )
        .await
        .is_err()
    );
    assert!(
        db.get_session(&session.id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .is_empty()
    );
    db.with_writer(|conn| {
        conn.execute_batch("DROP TRIGGER fail_second_user")?;
        Ok(())
    })
    .await
    .unwrap();
    let records = db
        .append_user_input_batch(
            &session.id,
            vec![message("first"), message("second")],
            MessageAttribution::conversation(),
        )
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].seq_num + 1, records[1].seq_num);
    assert_ne!(records[0].id, records[1].id);
    assert_eq!(records[1].content, message("second").content);
}

#[tokio::test]
async fn batch_cannot_forge_assistant_role_or_another_run_owner() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("test", "/tmp").await.unwrap();
    let mut forged = message("untrusted");
    forged.role = MessageRole::Assistant;
    assert!(
        db.append_user_input_batch(
            &session.id,
            vec![forged],
            MessageAttribution::conversation()
        )
        .await
        .is_err()
    );
    let attribution = MessageAttribution {
        task_id: Some("foreign".into()),
        run_id: None,
        ..MessageAttribution::conversation()
    };
    assert!(
        db.append_user_input_batch(&session.id, vec![message("untrusted")], attribution)
            .await
            .is_err()
    );
    assert!(
        db.get_session(&session.id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .is_empty()
    );
}
