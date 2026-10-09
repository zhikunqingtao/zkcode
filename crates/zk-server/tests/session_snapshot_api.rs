//! Session snapshot REST persistence and atomic restore tests.

mod common;

use axum::http::StatusCode;
use common::{call, json_body, local_delete, local_get, local_post};
use zk_db::{MessageRole, NewMessage, StoredBlock};
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;

fn fixture(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "zkcode-snapshot-api-{tag}-{}",
        uuid::Uuid::new_v4()
    ));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).expect("create fixture workspace");
    (
        std::fs::canonicalize(workspace).expect("canonical workspace"),
        root.join("snapshots"),
    )
}

#[tokio::test]
async fn temporary_conversations_cannot_export_or_resume_durable_snapshots() {
    let (workspace, snapshot_dir) = fixture("temporary");
    let db = zk_db::Db::open_in_memory().unwrap();
    let (session, _lease) = db
        .create_ephemeral_session("fixture", workspace.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let mut config = Config::test_config();
    config.snapshot_dir = Some(snapshot_dir.clone());
    let mut router = build_router(AppState::new(db, config));
    for suffix in ["snapshot", "snapshot/resume"] {
        let (status, _, body) = call(
            &mut router,
            local_post(&format!("/api/sessions/{session}/{suffix}"), None),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{}", json_body(&body));
        assert_eq!(json_body(&body)["code"], "EPHEMERAL_OPERATION_UNSUPPORTED");
    }
    assert!(std::fs::read_dir(snapshot_dir).unwrap().next().is_none());
}

#[tokio::test]
async fn temporary_live_content_cannot_use_export_or_resume_routes() {
    let (workspace, _) = fixture("temporary-live");
    let db = zk_db::Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", workspace.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    db.append_message(
        &session,
        NewMessage {
            meta: None,
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "private live content".into(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
    let mut router = build_router(AppState::new(db, Config::test_config()));
    for suffix in ["", "/messages"] {
        let (status, _, body) = call(
            &mut router,
            local_get(&format!("/api/sessions/{session}{suffix}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
        assert!(String::from_utf8_lossy(&body).contains("private live content"));
    }
    let mut lease = Some(lease);
    for expired in [false, true] {
        if expired {
            drop(lease.take());
        }
        for suffix in ["export", "export?format=markdown", "resume"] {
            let (status, _, body) = call(
                &mut router,
                local_post(&format!("/api/sessions/{session}/{suffix}"), None),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{}", json_body(&body));
            assert_eq!(json_body(&body)["code"], "EPHEMERAL_OPERATION_UNSUPPORTED");
            assert!(!String::from_utf8_lossy(&body).contains("private live content"));
        }
    }
    for suffix in ["export", "resume"] {
        let (status, _, body) = call(
            &mut router,
            local_post(&format!("/api/sessions/missing/{suffix}"), None),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json_body(&body)["code"], "SESSION_NOT_FOUND");
    }
}

#[tokio::test]
async fn save_restart_resume_is_idempotent_and_delete_is_durable() {
    let (workspace, snapshot_dir) = fixture("roundtrip");
    let db = zk_db::Db::open_in_memory().expect("db");
    let session = db
        .create_session("model-before", &workspace.to_string_lossy())
        .await
        .expect("session");
    db.append_message(
        &session.id,
        NewMessage {
            meta: None,
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "saved message".to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("message");

    let mut config = Config::test_config();
    config.snapshot_dir = Some(snapshot_dir.clone());
    let mut router = build_router(AppState::new(db.clone(), config.clone()));
    let path = format!("/api/sessions/{}/snapshot", session.id);
    let (status, _, body) = call(&mut router, local_post(&path, None)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    assert_eq!(json_body(&body)["messageCount"], 1);

    db.update_session_model(&session.id, "model-after")
        .await
        .expect("update model");
    db.append_message(
        &session.id,
        NewMessage {
            meta: None,
            role: MessageRole::Assistant,
            content: vec![StoredBlock::Text {
                text: "unsaved message".to_owned(),
            }],
            stop_reason: Some("end_turn".to_owned()),
            input_tokens: 1,
            output_tokens: 1,
        },
    )
    .await
    .expect("second message");

    // Rebuild AppState with the same DB and snapshot directory: no in-memory snapshot state.
    let mut router = build_router(AppState::new(db.clone(), config));
    let list_path = "/api/sessions/snapshots";
    let (status, _, body) = call(&mut router, local_get(list_path)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body).as_array().expect("list").len(), 1);

    let resume_path = format!("/api/sessions/{}/snapshot/resume", session.id);
    for _ in 0..2 {
        let (status, _, body) = call(&mut router, local_post(&resume_path, None)).await;
        assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    }
    let restored = db
        .get_session(&session.id)
        .await
        .expect("read")
        .expect("exists");
    assert_eq!(restored.model, "model-before");
    assert_eq!(
        restored.messages.len(),
        1,
        "resume must not duplicate messages"
    );

    let delete_path = format!("/api/sessions/snapshots/{}", session.id);
    let (status, _, body) = call(&mut router, local_delete(&delete_path)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    let (status, _, body) = call(&mut router, local_post(&resume_path, None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "SNAPSHOT_NOT_FOUND");
}

#[tokio::test]
async fn invalid_snapshot_id_is_rejected_without_filesystem_traversal() {
    let mut router = build_router(AppState::for_tests());
    let (status, _, body) = call(
        &mut router,
        local_post("/api/sessions/%2E%2E%2Fsnapshot/resume", None),
    )
    .await;
    assert!(
        matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND),
        "{}",
        json_body(&body)
    );
}

#[tokio::test]
async fn failed_snapshot_save_is_not_reported_as_successful_old_snapshot() {
    let (workspace, snapshot_dir) = fixture("save-failure");
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db
        .create_session("fixture", workspace.to_str().unwrap())
        .await
        .unwrap();
    let mut config = Config::test_config();
    config.snapshot_dir = Some(snapshot_dir.clone());
    let mut router = build_router(AppState::new(db, config));
    std::fs::create_dir(snapshot_dir.join(format!("{}.json", session.id))).unwrap();
    let (status, _, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{}/snapshot", session.id), None),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{}",
        json_body(&body)
    );
    assert_eq!(json_body(&body)["code"], "SNAPSHOT_WRITE_FAILED");
}

async fn restore_fixture(tag: &str) -> (AppState, String) {
    let (workspace, snapshot_dir) = fixture(tag);
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db
        .create_session("saved-model", workspace.to_str().unwrap())
        .await
        .unwrap();
    let mut config = Config::test_config();
    config.snapshot_dir = Some(snapshot_dir.clone());
    config.mcp_registry_path = snapshot_dir.with_file_name("absent-mcp.json");
    config.scratchpad_system_root = snapshot_dir.with_file_name("scratchpad");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.workspace_allowed_roots = vec![workspace];
    let state = AppState::new(db, config);
    let mut router = build_router(state.clone());
    let (status, _, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{}/snapshot", session.id), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    (state, session.id)
}

#[tokio::test]
async fn snapshot_restore_shares_query_admission_without_blocking_other_sessions() {
    let (state, session) = restore_fixture("query-admission").await;
    let engine = zk_server::engine_bridge::wire_engine(&state);
    state
        .db
        .update_session_model(&session, "current-model")
        .await
        .unwrap();
    let before = state.db.get_session(&session).await.unwrap().unwrap();
    // The real query lease exists before a durable Run has been created.
    let conversation = state.conversation().unwrap();
    let query = conversation.reserve(&session).unwrap();
    state.db.ensure_session_idle(&session).await.unwrap();
    let mut router = build_router(state.clone());
    let path = format!("/api/sessions/{session}/snapshot/resume");
    let (status, _, body) = call(&mut router, local_post(&path, None)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{}", json_body(&body));
    assert_eq!(json_body(&body)["code"], "SESSION_BUSY");
    assert_eq!(
        state.db.get_session(&session).await.unwrap().unwrap(),
        before
    );
    assert!(conversation.reserve(&session).is_none());

    let other = state
        .db
        .create_session("other-model", &before.working_dir)
        .await
        .unwrap();
    for suffix in ["snapshot", "snapshot/resume"] {
        let (status, _, body) = call(
            &mut router,
            local_post(&format!("/api/sessions/{}/{suffix}", other.id), None),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    }
    drop(query);
    let (status, _, body) = call(&mut router, local_post(&path, None)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    assert_eq!(
        state.db.get_session(&session).await.unwrap().unwrap().model,
        "saved-model"
    );
    assert!(engine.try_reserve_session_mutation(&session).is_some());
}

async fn seed_pending_recording(state: &AppState, session: &str, needs_attention: bool) -> String {
    use serde_json::json;
    use zk_db::{
        CleanupStatus, CommitToolInvocationResult, ExecutionResourceStatus, NewExecutionResource,
        NewToolInvocation, ToolInvocationStatus,
    };

    let db = &state.db;
    db.start_run("run", session, None, None, "fixture")
        .await
        .unwrap();
    let run = db.find_run_by_id("run").await.unwrap().unwrap();
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: "invocation".into(),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        tool_use_id: "tool-use".into(),
        tool_name: "VerifyJourney".into(),
        input_json: Some("{}".into()),
        side_effect_class: "read".into(),
        directory_generation: None,
        connection_generation: None,
    })
    .await
    .unwrap();
    let identity = json!({"session_id":session,"run_id":run.id,"invocation_id":"invocation"});
    let recording = json!({"kind":"browserSession","recordingFinalization":{
        "version":1,"phase":"sealed","identity":identity,
        "manifest":{"identity":identity},"dispositions":[]
    }})
    .to_string();
    db.register_execution_resource(&NewExecutionResource {
        resource_id: "recording".into(),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        invocation_id: Some("invocation".into()),
        resource_kind: "stream".into(),
        external_id: Some("fixture-browser".into()),
        metadata_json: recording.clone(),
    })
    .await
    .unwrap();
    db.finalize_execution_resource("recording", ExecutionResourceStatus::Released)
        .await
        .unwrap();
    let metadata = json!({"structuredResult":{"evidence":{
        "schemaVersion":1,"kind":"browser_journey","verdict":"verified",
        "observedAt":"2026-10-08T00:00:00Z","items":[]
    }}});
    db.commit_tool_invocation_result(&CommitToolInvocationResult {
        invocation_id: "invocation".into(),
        expected_version: 0,
        session_id: session.to_owned(),
        target: ToolInvocationStatus::Succeeded,
        input_json: Some("{}".into()),
        content: "original result".into(),
        is_error: false,
        metadata: Some(metadata.clone()),
        output_sha256: None,
        error_code: None,
        cleanup_status: CleanupStatus::Confirmed,
        postprocessing: Some(json!({"schemaVersion":1,"toolName":"VerifyJourney",
            "requiredKinds":["evidence"],"metadata":metadata})),
    })
    .await
    .unwrap();
    if needs_attention {
        let task = db
            .find_runtime_task_by_id(&run.task_id)
            .await
            .unwrap()
            .unwrap();
        db.mark_task_run_needs_attention(
            &task.id,
            &run.id,
            task.version,
            "fixture postprocessing failure",
            CleanupStatus::Confirmed,
        )
        .await
        .unwrap();
    }
    recording
}

#[tokio::test]
async fn snapshot_restore_preserves_running_and_needs_attention_results() {
    for needs_attention in [false, true] {
        let (state, session) = restore_fixture("durable-result").await;
        let recording = seed_pending_recording(&state, &session, needs_attention).await;
        let db = &state.db;
        // Wiring a fresh Engine must not hide persisted work from the DB guard.
        let engine = zk_server::engine_bridge::wire_engine(&state);
        let before = db.get_session(&session).await.unwrap().unwrap();
        let obligation = db
            .recorded_journey_postprocessing("invocation")
            .await
            .unwrap();
        assert!(obligation.is_some());
        let mut router = build_router(state.clone());
        let (status, _, body) = call(
            &mut router,
            local_post(&format!("/api/sessions/{session}/snapshot/resume"), None),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{}", json_body(&body));
        assert_eq!(db.get_session(&session).await.unwrap().unwrap(), before);
        assert_eq!(
            db.recorded_journey_postprocessing("invocation")
                .await
                .unwrap(),
            obligation
        );
        let remaining: String = db
            .with_conn_blocking(|conn| {
                Ok(conn.query_row(
                    "SELECT metadata_json FROM execution_resources WHERE resource_id='recording'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(remaining, recording);
        assert!(engine.try_reserve_session_mutation(&session).is_some());
    }
}

struct DurableSnapshotFixture {
    db: zk_db::Db,
    session: String,
    config: Config,
    root: std::path::PathBuf,
}

impl DurableSnapshotFixture {
    async fn new(tag: &str) -> Self {
        let (workspace, snapshot_dir) = fixture(tag);
        let db = zk_db::Db::open_in_memory().unwrap();
        let session = db
            .create_session("normal-model", workspace.to_str().unwrap())
            .await
            .unwrap()
            .id;
        let mut config = Config::test_config();
        config.snapshot_dir = Some(snapshot_dir.clone());
        config.mcp_registry_path = snapshot_dir.with_file_name("absent-mcp.json");
        config.scratchpad_system_root = snapshot_dir.with_file_name("scratchpad");
        config.workspace_default_root = workspace.to_string_lossy().into_owned();
        config.workspace_allowed_roots = vec![workspace.clone()];
        config.python_uds_path = snapshot_dir.with_file_name("unused-python.sock");
        Self {
            db,
            session,
            config,
            root: workspace.parent().unwrap().to_path_buf(),
        }
    }

    fn state(&self) -> AppState {
        AppState::new(self.db.clone(), self.config.clone())
    }

    fn snapshot_path(&self, resume: bool) -> String {
        let suffix = if resume { "/resume" } else { "" };
        format!("/api/sessions/{}/snapshot{suffix}", self.session)
    }
}

impl Drop for DurableSnapshotFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Public runtime APIs create the final message, immutable result and real usage
/// projection together; a hand-inserted assistant message would miss the FK defect.
async fn complete_snapshot_turn(db: &zk_db::Db, session: &str, run: &str) {
    use zk_db::{
        CleanupStatus, CommitTaskResult, CommitTaskResultOutcome, MessageAttribution, ResultStatus,
        RunUsageFallback, VerificationStatus,
    };
    db.start_run(run, session, None, Some("query"), "normal-model")
        .await
        .unwrap();
    let task = db.find_run_by_id(run).await.unwrap().unwrap().task_id;
    db.append_attributed_message(
        session,
        NewMessage {
            meta: None,
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: format!("normal user prompt for {run}"),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
        MessageAttribution {
            task_id: Some(task.clone()),
            run_id: Some(run.into()),
            origin: "conversation".into(),
            source_task_id: None,
        },
    )
    .await
    .unwrap();
    let answer = format!("normal final response for {run}");
    let final_id = db
        .ensure_task_final_assistant(&task, run, &answer)
        .await
        .unwrap();
    let version = db
        .find_runtime_task_by_id(&task)
        .await
        .unwrap()
        .unwrap()
        .version;
    let outcome = db
        .commit_task_result_with_run_usage_fallback(
            &CommitTaskResult {
                task_id: task.clone(),
                run_id: run.into(),
                expected_task_version: version,
                status: ResultStatus::Complete,
                content: answer,
                media_type: "text/plain".into(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            },
            RunUsageFallback {
                input_tokens: 10,
                output_tokens: 2,
                cache_read_tokens: 3,
                cache_create_tokens: 4,
                cost_nanos_usd: 1_000,
                usage_complete: true,
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CommitTaskResultOutcome::Committed { .. }));
    let result = db
        .read_task_result(&task, None, 0, 1024)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.result.status, ResultStatus::Complete);
    assert_eq!(
        result.result.final_message_id.as_deref(),
        Some(final_id.as_str())
    );
    db.ensure_session_idle(session).await.unwrap();
}

type DatabaseFacts = std::collections::BTreeMap<String, Vec<Vec<String>>>;

/// Include raw attribution, accounting and dependency rows rather than only the
/// display projection. Only successful restore may touch `sessions.updated_at`;
/// refusal tests retain that raw column as part of their exact comparison.
async fn database_facts(db: &zk_db::Db, allow_session_touch: bool) -> DatabaseFacts {
    db.with_reader(move |connection| {
        let tables = connection
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut facts = DatabaseFacts::new();
        for table in tables {
            let mut statement = connection.prepare(&format!(
                "SELECT * FROM \"{}\"",
                table.replace('"', "\"\"")
            ))?;
            let columns: Vec<_> = statement
                .column_names()
                .iter()
                .enumerate()
                .filter_map(|(index, name)| {
                    (!allow_session_touch || table != "sessions" || *name != "updated_at")
                        .then_some(index)
                })
                .collect();
            let mut rows = statement.query([])?;
            let mut values = Vec::new();
            while let Some(row) = rows.next()? {
                let value = columns
                    .iter()
                    .map(|index| row.get_ref(*index).map(|value| format!("{value:?}")))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                values.push(value);
            }
            values.sort();
            facts.insert(table, values);
        }
        let violations: i64 = connection.query_row(
            "SELECT COUNT(*) FROM pragma_foreign_key_check",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(violations, 0, "restored runtime references remain valid");
        Ok(facts)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn completed_task_snapshot_resumes_twice_after_restart_without_changing_runtime_facts() {
    let fixture = DurableSnapshotFixture::new("complete-roundtrip").await;
    complete_snapshot_turn(&fixture.db, &fixture.session, "complete-first").await;
    let mut router = build_router(fixture.state());
    let (status, _, body) =
        call(&mut router, local_post(&fixture.snapshot_path(false), None)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    let saved_summary = json_body(&body);
    assert_eq!(saved_summary["messageCount"], 2);
    drop(router);

    // A fresh service loads the on-disk snapshot rather than an in-memory copy.
    let mut router = build_router(fixture.state());
    let before = fixture
        .db
        .get_session(&fixture.session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.total_usage.cache_read_input_tokens, 3);
    assert_eq!(before.total_usage.cache_creation_input_tokens, 4);
    assert!(before.total_cost_usd > 0.0);
    let facts = database_facts(&fixture.db, true).await;
    for _ in 0..2 {
        let (status, _, body) =
            call(&mut router, local_post(&fixture.snapshot_path(true), None)).await;
        assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
        assert_eq!(json_body(&body), saved_summary);
        assert_eq!(database_facts(&fixture.db, true).await, facts);
    }
}

#[tokio::test]
async fn older_snapshot_after_a_second_completed_turn_returns_conflict_without_changes() {
    let fixture = DurableSnapshotFixture::new("complete-conflict").await;
    complete_snapshot_turn(&fixture.db, &fixture.session, "complete-first").await;
    let mut router = build_router(fixture.state());
    let (status, _, body) =
        call(&mut router, local_post(&fixture.snapshot_path(false), None)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    complete_snapshot_turn(&fixture.db, &fixture.session, "complete-second").await;
    drop(router);

    let mut router = build_router(fixture.state());
    let before = fixture
        .db
        .get_session(&fixture.session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.messages.len(), 4);
    let facts = database_facts(&fixture.db, false).await;
    let (status, _, body) = call(&mut router, local_post(&fixture.snapshot_path(true), None)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{}", json_body(&body));
    assert_eq!(json_body(&body)["code"], "SNAPSHOT_HISTORY_CONFLICT");
    assert_eq!(
        fixture
            .db
            .get_session(&fixture.session)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(database_facts(&fixture.db, false).await, facts);
}

#[tokio::test]
async fn invalid_snapshot_messages_return_bad_request_without_changes() {
    let fixture = DurableSnapshotFixture::new("invalid-messages").await;
    fixture
        .db
        .append_message(
            &fixture.session,
            NewMessage {
                meta: None,
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: "ordinary history".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
    let state = fixture.state();
    let before = fixture
        .db
        .get_session(&fixture.session)
        .await
        .unwrap()
        .unwrap();
    let mut snapshot = zk_engine::SessionSnapshot::from_session_detail(&before);
    snapshot.messages.push(snapshot.messages[0].clone());
    state
        .session_snapshots
        .save_snapshot(&fixture.session, &snapshot)
        .await
        .unwrap();
    let mut router = build_router(state);
    let facts = database_facts(&fixture.db, false).await;
    let (status, _, body) = call(&mut router, local_post(&fixture.snapshot_path(true), None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", json_body(&body));
    assert_eq!(json_body(&body)["code"], "SNAPSHOT_MESSAGES_INVALID");
    assert_eq!(
        fixture
            .db
            .get_session(&fixture.session)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(database_facts(&fixture.db, false).await, facts);
}
