//! Migration contracts exercised through the actual HTTP router and fresh `SQLite`.
mod common;

use axum::http::{Method, StatusCode};
use common::{app_with_db, call, json_body, local_get, local_post, local_put, local_with_headers};
use serde_json::json;
use sha2::{Digest, Sha256};
use zk_db::{MessageRole, NewMessage, SessionMergeRequest, StoredBlock};

#[tokio::test]
async fn saved_default_changes_only_new_sessions_and_explicit_selection_wins() {
    let (mut app, db) = app_with_db();
    let (status, _, body) = call(&mut app, local_post("/api/sessions", None)).await;
    assert_eq!(status, StatusCode::CREATED);
    let first = json_body(&body);
    let id = first["sessionId"].as_str().unwrap();
    let (status, _, _) = call(
        &mut app,
        local_put(
            "/api/config",
            Some(json!({"defaultModel":"kimi-k3"}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body) = call(&mut app, local_post("/api/sessions", None)).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json_body(&body)["model"], "kimi-k3");
    assert_eq!(
        db.get_session(id).await.unwrap().unwrap().model,
        first["model"].as_str().unwrap()
    );
    let (status, _, body) = call(
        &mut app,
        local_post(
            "/api/sessions",
            Some(json!({"model": first["model"]}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json_body(&body)["model"], first["model"]);
    let (_, _, config) = call(&mut app, local_get("/api/config")).await;
    assert_eq!(json_body(&config)["defaultModel"], "kimi-k3");
}

#[tokio::test]
async fn sealed_asset_download_requires_target_session_and_survives_source_deletion() {
    use base64::Engine as _;
    let (mut app, db) = app_with_db();
    let a = db.create_session("kimi-k3", "/tmp").await.unwrap();
    let b = db.create_session("kimi-k3", "/tmp").await.unwrap();
    let bytes = b"immutable image identity";
    db.append_message(
        &a.id,
        NewMessage {
            meta: None,
            role: MessageRole::User,
            content: vec![StoredBlock::Image {
                source: zk_db::ImageSource {
                    kind: "base64".into(),
                    media_type: Some("image/png".into()),
                    data: Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
                    url: None,
                },
                width: None,
                height: None,
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
    let op = db
        .start_session_merge(
            "download-test".into(),
            SessionMergeRequest {
                source_session_ids: vec![a.id.clone(), b.id.clone()],
                primary_session_id: a.id.clone(),
                title: None,
                model: None,
            },
        )
        .await
        .unwrap();
    let summary = "Fixture summary only";
    db.publish_merge_summary(
        &op.operation_id,
        op.run_epoch,
        summary.into(),
        json!({}),
        format!("{:x}", Sha256::digest(summary)),
    )
    .await
    .unwrap();
    let done = db
        .complete_session_merge(&op.operation_id, op.run_epoch)
        .await
        .unwrap();
    let catalog = db
        .query_handoff(
            &done.target_session_id,
            zk_db::HandoffQuery {
                action: "list".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let asset = catalog["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["ref"]
                .as_str()
                .is_some_and(|r| r.starts_with("asset:"))
        })
        .unwrap();
    let url = format!(
        "/api/session-merges/{}/assets/{}",
        done.operation_id,
        asset["ref"].as_str().unwrap()
    );
    let (status, _, _) = call(
        &mut app,
        local_with_headers(&url, Method::GET, None, &[("X-Session-Id", &a.id)]),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    db.delete_session(&a.id).await.unwrap();
    db.delete_session(&b.id).await.unwrap();
    let (status, headers, body) = call(
        &mut app,
        local_with_headers(
            &url,
            Method::GET,
            None,
            &[("X-Session-Id", &done.target_session_id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, bytes.as_slice());
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(headers["cache-control"], "private, no-store");
}

#[tokio::test]
async fn activity_decision_rejects_invalid_values_without_writing_and_checks_session() {
    let (mut app, db) = app_with_db();
    let session = db.create_session("kimi-k3", "/tmp").await.unwrap();
    let id = session.id.clone();
    db.with_writer(move |conn| {
        conn.execute("INSERT INTO activities(id,session_id,operation_type,summary,status,timestamp,created_at,updated_at) VALUES('a',?1,'edit','change','completed',0,'now','now')", [id])?;
        Ok(())
    }).await.unwrap();
    let uri = format!("/api/sessions/{}/activities/a/decision", session.id);
    for body in [
        json!({"decision":"invalid"}),
        json!({}),
        json!({"decision":null}),
        json!({"decision":7}),
    ] {
        let (status, _, bytes) = call(&mut app, local_put(&uri, Some(body.to_string()))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json_body(&bytes)["code"], "ACTIVITY_DECISION_INVALID");
        assert!(
            db.find_activities_by_session_paged(&session.id, 0, 10)
                .await
                .unwrap()[0]["decision"]
                .is_null()
        );
    }
    let (status, _, _) = call(
        &mut app,
        local_put(
            "/api/sessions/foreign/activities/a/decision",
            Some(json!({"decision":"approved"}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, bytes) = call(
        &mut app,
        local_put(&uri, Some(json!({"decision":"approved"}).to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&bytes)["sessionId"], session.id);
    assert_eq!(
        db.find_activities_by_session_paged(&session.id, 0, 10)
            .await
            .unwrap()[0]["decision"],
        "approved"
    );
}
