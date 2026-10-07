//! Source search scope and running-badge regressions use actual `SQLite` projections.
use serde_json::json;
use zk_db::{Db, MessageRole, NewMessage, StoredBlock, TaskBudgetLimits};

async fn seed(db: &Db, session: &str, role: MessageRole, content: &str) {
    db.append_message(
        session,
        NewMessage {
            role,
            content: vec![StoredBlock::Text {
                text: content.into(),
            }],
            meta: None,
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn search_uses_only_decoded_text_blocks_in_first_user_message() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("model", "/tmp").await.unwrap();
    seed(&db, &session.id, MessageRole::Assistant, "assistant-only").await;
    seed(&db, &session.id, MessageRole::User, "placeholder").await;
    seed(&db, &session.id, MessageRole::User, "later-only").await;
    let id = session.id.clone();
    db.with_writer(move |conn| {
        conn.execute("UPDATE messages SET content_json=?1 WHERE id=(SELECT id FROM messages WHERE session_id=?2 AND role='user' ORDER BY seq_num LIMIT 1)", rusqlite::params![json!([
            {"type":"text","text":"a".repeat(120)},
            {"type":"image","text":"image-only"},
            "not-a-block", null, 42,
            {"type":"text","text":123},
            {"type":"text","text":"中文 needle C:\\temp \"quoted\" 100% a_b"}
        ]).to_string(), id])?;
        Ok(())
    }).await.unwrap();
    for query in ["中文", "needle", "C:\\temp", "\"quoted\"", "100%", "a_b"] {
        let found = db.search_sessions(None, 10, query).await.unwrap();
        assert_eq!(found.sessions.len(), 1, "{query}");
        assert_eq!(found.sessions[0].id, session.id);
    }
    for query in [
        "assistant-only",
        "later-only",
        "image-only",
        "not-a-block",
        "123",
        "type",
        "text",
        "1000",
        "axb",
    ] {
        assert!(
            db.search_sessions(None, 10, query)
                .await
                .unwrap()
                .sessions
                .is_empty(),
            "{query}"
        );
    }
    assert!(
        !db.search_sessions(None, 10, "needle")
            .await
            .unwrap()
            .sessions[0]
            .goal_preview
            .as_deref()
            .unwrap()
            .contains("needle")
    );
}

#[tokio::test]
async fn malformed_and_non_array_history_never_breaks_title_or_empty_search() {
    let db = Db::open_in_memory().unwrap();
    let malformed = [
        "{broken",
        "null",
        "123",
        "\"needle\"",
        "{}",
        "[\"needle\",null,42,{\"type\":\"text\",\"text\":123}]",
    ];
    for content in malformed {
        let session = db.create_session("model", "/tmp").await.unwrap();
        seed(&db, &session.id, MessageRole::User, "placeholder").await;
        let id = session.id;
        let content = content.to_owned();
        db.with_writer(move |conn| {
            conn.execute("UPDATE sessions SET title='title-match' WHERE id=?1", [&id])?;
            conn.execute(
                "UPDATE messages SET content_json=?1 WHERE session_id=?2",
                (content, id),
            )?;
            Ok(())
        })
        .await
        .unwrap();
    }
    assert!(
        db.search_sessions(None, 20, "needle")
            .await
            .unwrap()
            .sessions
            .is_empty()
    );
    for query in ["title-match", "", "  "] {
        assert_eq!(
            db.search_sessions(None, 20, query)
                .await
                .unwrap()
                .sessions
                .len(),
            malformed.len()
        );
    }
}

#[tokio::test]
async fn running_badge_tracks_latest_root_run_without_relaxing_busy_tasks() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("model", "/tmp").await.unwrap();
    assert!(!db.search_sessions(None, 20, "").await.unwrap().sessions[0].running);
    let run = uuid::Uuid::new_v4().to_string();
    db.start_root_run_with_budget(
        &run,
        &session.id,
        Some("main"),
        "model",
        &TaskBudgetLimits {
            token_limit: None,
            cost_limit_nanos_usd: None,
            deadline_at_ms: Some(zk_db::time::now_millis() + 60000),
        },
    )
    .await
    .unwrap();
    for (status, expected) in [
        ("running", true),
        ("waitingInteraction", false),
        ("waitingDependencies", false),
        ("cancelling", false),
    ] {
        let id = run.clone();
        db.with_writer(move |conn| {
            conn.execute(
                "UPDATE run_envelopes SET status=?1 WHERE id=?2",
                (status, id),
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            db.search_sessions(None, 20, "").await.unwrap().sessions[0].running,
            expected,
            "{status}"
        );
        assert!(
            !db.find_runtime_task_by_id(&run)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_terminal(),
            "badge must not alter execution gate state"
        );
    }
}
