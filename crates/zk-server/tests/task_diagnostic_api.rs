//! Real-router coverage for the redacted `TaskRuntime` diagnostic endpoint.

mod common;

use axum::http::{Method, StatusCode};
use common::{app_with_db, call, json_body, local_with_headers};
use rusqlite::params;

async fn seed_complete_diagnostic(db: &zk_db::Db) -> (String, String) {
    let session = db
        .create_session("model", "/diagnostic/secret-workspace")
        .await
        .expect("root session");
    let task_id = "diagnostic-route-task".to_owned();
    db.start_run(&task_id, &session.id, None, Some("query"), "model")
        .await
        .expect("task run");
    db.start_run(
        "diagnostic-route-consumer",
        &session.id,
        None,
        Some("query"),
        "model",
    )
    .await
    .expect("consumer task");
    db.with_conn_blocking({
        let session_id = session.id.clone();
        let task_id = task_id.clone();
        move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE tasks SET prompt='route-secret-prompt',description='route-secret-description',
                 execution_config_json='{\"secret\":\"route-secret-config\"}' WHERE id=?1",
                params![task_id],
            )?;
            transaction.execute(
                "INSERT INTO agent_checkpoints(id,run_id,session_id,agent_id,seq,messages_json,
                 file_state_json,tool_call_count,turn_count,tokens_consumed,working_dir,created_at)
                 VALUES('route-checkpoint',?1,?2,'route-agent',1,
                 '[\"route-secret-message\"]','{\"route-secret-file\":true}',1,1,5,
                 '/diagnostic/secret-workspace','2026-01-01T00:00:00Z')",
                params![task_id, session_id],
            )?;
            transaction.execute(
                "UPDATE run_envelopes SET checkpoint_id='route-checkpoint',
                 error_summary='route-secret-error' WHERE id=?1",
                params![task_id],
            )?;
            transaction.execute(
                "INSERT INTO tool_invocations(invocation_id,task_id,run_id,tool_use_id,tool_name,
                 status,input_json,output_ref,side_effect_class,cleanup_status,version,terminal_at,
                 created_at,updated_at) VALUES('route-invocation',?1,?1,'route-tool-use','Read',
                 'succeeded','{\"secret\":\"route-secret-input\"}','route-secret-output','read',
                 'confirmed',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',
                 '2026-01-01T00:00:00Z')",
                params![task_id],
            )?;
            transaction.execute(
                "INSERT INTO execution_resources(resource_id,task_id,run_id,invocation_id,
                 resource_kind,external_id,status,metadata_json,version,created_at,updated_at,released_at)
                 VALUES('route-resource',?1,?1,'route-invocation','process','route-secret-pid',
                 'released','{\"secret\":\"route-secret-metadata\"}',1,
                 '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
                params![task_id],
            )?;
            let hash = "b".repeat(64);
            transaction.execute(
                "INSERT INTO task_results(result_id,task_id,run_id,result_version,status,
                 inline_text,byte_len,content_sha256,media_type,created_at)
                 VALUES('route-result',?1,?1,1,'partial','route-secret-result',19,?2,
                 'text/plain','2026-01-01T00:00:00Z')",
                params![task_id, hash],
            )?;
            transaction.execute(
                "INSERT INTO messages(id,session_id,role,content_json,origin,created_at,seq_num)
                 VALUES('route-receipt-message',?1,'assistant',
                 '[{\"text\":\"route-secret-receipt\"}]','task_result',
                 '2026-01-01T00:00:00Z',100)",
                params![session_id],
            )?;
            transaction.execute(
                "INSERT INTO task_result_receipts(receipt_id,consumer_task_id,producer_task_id,
                 result_version,message_id,result_sha256,created_at)
                 VALUES('route-receipt','diagnostic-route-consumer',?1,1,
                 'route-receipt-message',?2,'2026-01-01T00:00:00Z')",
                params![task_id, hash],
            )?;
            transaction.execute(
                "INSERT INTO llm_calls(call_id,task_id,run_id,provider,model,route,
                 provider_request_id,status,input_tokens,output_tokens,cache_read_tokens,
                 cache_create_tokens,cost_nanos_usd,usage_complete,started_at,finished_at,
                 created_at,updated_at) VALUES('route-call',?1,?1,'script','model','primary',
                 'route-secret-provider-id','completed',10,4,0,0,10,1,
                 '2026-01-01T00:00:00Z','2026-01-01T00:00:01Z',
                 '2026-01-01T00:00:00Z','2026-01-01T00:00:01Z')",
                params![task_id],
            )?;
            transaction.commit()?;
            Ok(())
        }
    })
    .expect("seed diagnostic ledgers");
    (session.id, task_id)
}

#[tokio::test]
async fn diagnostic_returns_complete_redacted_aggregate() {
    let (mut router, db) = app_with_db();
    let (session_id, task_id) = seed_complete_diagnostic(&db).await;
    let request = local_with_headers(
        &format!("/api/tasks/{task_id}/diagnostic"),
        Method::GET,
        None,
        &[("x-session-id", &session_id)],
    );
    let (status, _, body) = call(&mut router, request).await;
    assert_eq!(status, StatusCode::OK);
    let json = json_body(&body);
    assert_eq!(json["task"]["taskId"], task_id);
    for collection in [
        "attempts",
        "toolInvocations",
        "executionResources",
        "results",
        "receipts",
        "llmCalls",
        "checkpoints",
        "resumeEligibility",
    ] {
        assert_eq!(
            json[collection].as_array().map(Vec::len),
            Some(1),
            "{collection}"
        );
    }
    let wire = String::from_utf8(body.to_vec()).expect("utf8 JSON");
    for secret in [
        "route-secret-prompt",
        "route-secret-description",
        "route-secret-config",
        "route-secret-message",
        "route-secret-file",
        "route-secret-error",
        "route-secret-input",
        "route-secret-output",
        "route-secret-pid",
        "route-secret-metadata",
        "route-secret-result",
        "route-secret-receipt",
        "route-secret-provider-id",
        "/diagnostic/secret-workspace",
    ] {
        assert!(!wire.contains(secret), "response leaked {secret}");
    }
}

#[tokio::test]
async fn diagnostic_returns_404_for_missing_task() {
    let (mut router, db) = app_with_db();
    let session = db.create_session("model", "/tmp").await.expect("session");
    let request = local_with_headers(
        "/api/tasks/missing-task/diagnostic",
        Method::GET,
        None,
        &[("x-session-id", &session.id)],
    );
    let (status, _, body) = call(&mut router, request).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "TASK_NOT_FOUND");
}

#[tokio::test]
async fn diagnostic_returns_403_for_another_root_session() {
    let (mut router, db) = app_with_db();
    let (owner_session, task_id) = seed_complete_diagnostic(&db).await;
    let other = db
        .create_session("model", "/tmp")
        .await
        .expect("other session");
    assert_ne!(owner_session, other.id);
    let request = local_with_headers(
        &format!("/api/tasks/{task_id}/diagnostic"),
        Method::GET,
        None,
        &[("x-session-id", &other.id)],
    );
    let (status, _, body) = call(&mut router, request).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&body)["code"], "TASK_ACCESS_DENIED");
}
