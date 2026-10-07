//! Workbench and session preferences materialize live bodies without `SQLite` copies.

use serde_json::json;
use sha2::{Digest, Sha256};
use zk_db::{
    AcceptanceCriterionRecord, Db, DbError, MessageRole, NewMessage, SessionExecutionPreferences,
    StoredBlock, WorkbenchBindingRecord,
};

fn message(role: MessageRole, text: &str) -> NewMessage {
    NewMessage {
        meta: None,
        role,
        content: vec![StoredBlock::Text { text: text.into() }],
        stop_reason: None,
        input_tokens: 0,
        output_tokens: 0,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One scope spanning criteria, preferences and the actual disk scan.
async fn temporary_workbench_criteria_and_preferences_never_persist_body_or_hash() {
    let root = std::env::temp_dir().join(format!("zk-workbench-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("content.sqlite");
    let db = Db::open(&path).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    let marker = format!("private-workbench-{}", uuid::Uuid::new_v4());
    let next = format!("replacement-{marker}");
    let request = db
        .append_message(&session, message(MessageRole::User, &marker))
        .await
        .unwrap();
    db.start_run("workbench-run", &session, None, None, "fixture")
        .await
        .unwrap();
    let binding = WorkbenchBindingRecord {
        root_run_id: "workbench-run".into(),
        request_message_id: request.id,
        result_message_id: None,
        created_at: "2026-10-07T00:00:00Z".into(),
        updated_at: "2026-10-07T00:00:00Z".into(),
    };
    let mut criterion = AcceptanceCriterionRecord {
        criterion_id: "criterion".into(),
        root_run_id: binding.root_run_id.clone(),
        ordinal: 0,
        criterion_type: "business".into(),
        source_text: marker.clone(),
        status: "not_verified".into(),
        evidence_bundle_id: None,
        created_at: binding.created_at.clone(),
        updated_at: binding.updated_at.clone(),
    };
    db.initialize_workbench(&binding, &[criterion.clone()])
        .await
        .unwrap();
    assert_eq!(
        db.find_workbench("workbench-run")
            .await
            .unwrap()
            .unwrap()
            .criteria[0]
            .source_text,
        marker
    );
    criterion.source_text.clone_from(&next);
    db.replace_acceptance_criteria("workbench-run", &[criterion.clone()])
        .await
        .unwrap();
    assert_eq!(
        db.find_current_workbench_for_session(&session)
            .await
            .unwrap()
            .unwrap()
            .criteria[0]
            .source_text,
        next
    );
    assert!(
        !db.has_reviewable_run_result(&session, "workbench-run", "2000", "2099")
            .await
            .unwrap()
    );
    db.append_message(&session, message(MessageRole::Assistant, &marker))
        .await
        .unwrap();
    assert!(
        db.has_reviewable_run_result(&session, "workbench-run", "2000", "2099")
            .await
            .unwrap()
    );
    assert!(
        !db.has_reviewable_run_result(&session, "workbench-run", "2098", "2099")
            .await
            .unwrap()
    );

    let (owner, private) = (session.clone(), marker.clone());
    db.with_writer(move |conn| {
        let encoded =
            zk_db::content::store_text(conn, &owner, &json!({"preserve": private}).to_string())?;
        conn.execute(
            "UPDATE sessions SET metadata_json=?2 WHERE id=?1",
            rusqlite::params![owner, encoded],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let saved = db
        .set_session_execution_preferences(
            &session,
            0,
            SessionExecutionPreferences {
                revision: 0,
                effort: Some("high".into()),
                fast: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(saved.revision, 1);
    assert_eq!(
        db.session_execution_preferences(&session).await.unwrap(),
        saved
    );
    assert!(matches!(
        db.set_session_execution_preferences(&session, 0, SessionExecutionPreferences::default())
            .await,
        Err(DbError::Conflict(_))
    ));
    let (owner, private) = (session.clone(), marker.clone());
    db.with_reader(move |conn| {
        let raw = conn.query_row(
            "SELECT metadata_json FROM sessions WHERE id=?1",
            [&owner],
            |row| row.get(0),
        )?;
        let body = zk_db::content::load_optional(conn, &owner, raw)?.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body)?["preserve"],
            private
        );
        Ok(())
    })
    .await
    .unwrap();
    let private = marker.clone();
    assert!(
        db.with_writer(move |conn| {
            conn.execute(
                "UPDATE run_acceptance_criteria SET source_text=?1",
                [&private],
            )?;
            Ok(())
        })
        .await
        .is_err()
    );
    for entry in std::fs::read_dir(&root).unwrap() {
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        for value in [&marker, &next] {
            for needle in [
                value.clone(),
                format!("{:x}", Sha256::digest(value.as_bytes())),
            ] {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|part| part == needle.as_bytes())
                );
            }
        }
    }
    let reopened = Db::open(&path).unwrap();
    assert!(reopened.find_workbench("workbench-run").await.is_err());
    assert!(
        reopened
            .session_execution_preferences(&session)
            .await
            .is_err()
    );
    drop(lease);
    assert!(db.find_workbench("workbench-run").await.is_err());
    assert!(
        db.has_reviewable_run_result(&session, "workbench-run", "2000", "2099")
            .await
            .is_err()
    );
    assert!(db.session_execution_preferences(&session).await.is_err());
    assert!(
        db.replace_acceptance_criteria("workbench-run", &[criterion])
            .await
            .is_err()
    );
    drop(reopened);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
