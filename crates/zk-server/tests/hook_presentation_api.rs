//! Trusted Hook notes survive history reload without changing canonical tool facts.
mod common;
use axum::http::{HeaderValue, StatusCode};
use common::{call, json_body, local_get};
use zk_server::{config::Config, interaction::runs, routes::build_router, state::AppState};

async fn seed(db: &zk_db::Db, session: &str) -> String {
    let session_id = session.to_owned();
    let session = session.to_owned();
    db.with_writer(move |conn| {
        runs::start_in_current_write(conn, "hook-run", &session, None, Some("main"), "test")?;
        conn.execute("INSERT INTO tool_invocations(invocation_id,task_id,run_id,tool_use_id,tool_name,status,input_json,side_effect_class,cleanup_status,created_at,updated_at,terminal_at) SELECT 'hook-inv',task_id,id,'hook-call','Read','cancelled',NULL,'read','notRequired',started_at,started_at,started_at FROM run_envelopes WHERE id='hook-run'", [])?;
        Ok(())
    }).await.unwrap();
    db.append_attributed_message(
        &session_id,
        zk_db::NewMessage {
            meta: None,
            role: zk_db::MessageRole::Assistant,
            content: vec![zk_db::StoredBlock::ToolUse {
                id: "hook-call".into(),
                name: "Read".into(),
                input: serde_json::json!({}),
            }],
            stop_reason: Some("tool_use".into()),
            input_tokens: 0,
            output_tokens: 0,
        },
        zk_db::MessageAttribution {
            task_id: Some("hook-run".into()),
            run_id: Some("hook-run".into()),
            origin: "conversation".into(),
            source_task_id: None,
        },
    )
    .await
    .unwrap()
    .id
}
fn request(path: &str, session: &str) -> axum::http::Request<axum::body::Body> {
    let mut request = local_get(path);
    request
        .headers_mut()
        .insert("x-session-id", HeaderValue::from_str(session).unwrap());
    request
}
#[tokio::test]
async fn reload_restores_only_owned_trusted_notes() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("test", "/tmp").await.unwrap().id;
    let other = db.create_session("test", "/tmp").await.unwrap().id;
    let assistant = seed(&db, &session).await;
    assert!(
        db.save_hook_presentation(&other, "hook-run", "hook-call", "forged")
            .await
            .is_err()
    );
    db.save_hook_presentation(&session, "hook-run", "hook-call", "Hook note")
        .await
        .unwrap();
    let mut router = build_router(AppState::new(db.clone(), Config::test_config()));
    let path = format!("/api/sessions/{session}/tool-presentations");
    let (status, _, body) = call(&mut router, request(&path, &session)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    let json = json_body(&body);
    assert_eq!(json["presentations"][0]["text"], "Hook note");
    assert_eq!(json["presentations"][0]["runId"], "hook-run");
    assert_eq!(json["presentations"][0]["toolUseId"], "hook-call");
    assert_eq!(json["presentations"][0]["assistantMessageId"], assistant);
    assert_eq!(
        call(&mut router, request(&path, &other)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&mut router, request(&format!("{path}?after=-1"), &session))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&mut router, request(&format!("{path}?after=999"), &session))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        db.get_session(&session)
            .await
            .unwrap()
            .unwrap()
            .messages
            .len(),
        1
    );
}
#[tokio::test]
async fn ephemeral_notes_are_guarded_and_disappear_with_content_scope() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("test", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    seed(&db, &session).await;
    db.save_hook_presentation(&session, "hook-run", "hook-call", "private hook text")
        .await
        .unwrap();
    assert_eq!(
        db.hook_presentations(&session, 0, 10).await.unwrap()[0].text,
        "private hook text"
    );
    db.with_writer(|conn| {
        let raw: String =
            conn.query_row("SELECT text FROM hook_result_presentations", [], |row| {
                row.get(0)
            })?;
        assert!(!raw.contains("private hook text"));
        assert!(
            conn.execute("UPDATE hook_result_presentations SET text='plaintext'", [])
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
    drop(lease);
    assert!(db.hook_presentations(&session, 0, 10).await.is_err());
}
