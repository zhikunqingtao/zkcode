//! An explicit operation identity protects effects independently of transport ids.
use serde_json::json;
use zk_db::*;

fn service_request(session: &str) -> CreateTaskWithRun {
    CreateTaskWithRun {
        task_id: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        root_session_id: session.into(),
        transcript_session_id: session.into(),
        parent_task_id: None,
        parent_run_id: None,
        creator_tool_use_id: None,
        ordinal: 0,
        description: "fixture MCP".into(),
        prompt: None,
        task_type: "mcp".into(),
        model: "fixture".into(),
        working_dir: "/tmp".into(),
        execution_config_json: json!({"executor":"localMcp","budget":{
            "tokenLimit":1000,"costLimitNanosUsd":1_000_000,
            "deadlineAtMs":time::now_millis()+60_000}})
        .to_string(),
        startup_epoch: 0,
    }
}

async fn service(db: &Db, session: &str) -> (String, String) {
    let request = service_request(session);
    let created = db.create_task_with_run(&request).await.unwrap();
    assert_eq!(
        db.claim_task_run_cas(&created.task.id, &created.run_id, created.task.version)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    (created.task.id, created.run_id)
}

#[tokio::test]
async fn operation_claim_is_atomic_and_unknown_effects_are_never_replayed() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap().id;
    let (_, run) = service(&db, &session).await;
    let input = json!({"path":"local.txt","content":"one attempt"});
    let (a, b) = tokio::join!(
        db.admit_external_tool_operation(&session, &run, "operation-1", "Write", &input),
        db.admit_external_tool_operation(&session, &run, "operation-1", "Write", &input)
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|item| matches!(item, ExternalToolAdmission::Accepted { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|item| matches!(item, ExternalToolAdmission::Unconfirmed))
            .count(),
        1
    );
    assert!(
        db.admit_external_tool_operation(
            &session,
            &run,
            "operation-1",
            "Write",
            &json!({"content":"changed"})
        )
        .await
        .is_err()
    );
    assert!(
        db.admit_external_tool_operation("other-session", &run, "operation-2", "Write", &input)
            .await
            .is_err()
    );
    assert!(
        db.admit_external_tool_operation(&session, &run, "not an operation id", "Write", &input)
            .await
            .is_err()
    );
    let other_session = db.create_session("fixture", "/tmp").await.unwrap().id;
    let guarded_run = run.clone();
    db.with_writer(move |conn| {
        let error=conn.execute("INSERT INTO external_tool_requests(run_id,operation_id,session_id,tool_name,input_json,tool_use_id) VALUES(?1,'foreign-insert',?2,'Write','{}','foreign-tool-use')",rusqlite::params![guarded_run,other_session]).unwrap_err();
        assert!(error.to_string().contains("EXTERNAL_OPERATION_OWNER_MISMATCH"));
        Ok(())
    }).await.unwrap();
    // A new operation is allowed even if an SDK reused its transport request id.
    assert!(matches!(
        db.admit_external_tool_operation(&session, &run, "operation-2", "Write", &input)
            .await
            .unwrap(),
        ExternalToolAdmission::Accepted { .. }
    ));
}

#[tokio::test]
async fn replay_requires_canonical_result_and_completed_postprocessing_in_the_same_scope() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap().id;
    let (task, run) = service(&db, &session).await;
    let input = json!({"content":"sensitive operation body"});
    let ExternalToolAdmission::Accepted { tool_use_id } = db
        .admit_external_tool_operation(&session, &run, "effect", "Write", &input)
        .await
        .unwrap()
    else {
        panic!("first claim");
    };
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: "physical".into(),
        task_id: task,
        run_id: run.clone(),
        tool_use_id,
        tool_name: "Write".into(),
        input_json: Some(input.to_string()),
        side_effect_class: "write".into(),
        directory_generation: None,
        connection_generation: None,
    })
    .await
    .unwrap();
    assert_eq!(
        db.transition_tool_invocation_cas(
            "physical",
            0,
            ToolInvocationStatus::Running,
            Some(&input.to_string()),
            None,
            None,
            CleanupStatus::NotRequired
        )
        .await
        .unwrap(),
        CasOutcome::Applied
    );
    let CommitToolInvocationResultOutcome::Committed(committed) = db
        .commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: "physical".into(),
            expected_version: 1,
            session_id: session.clone(),
            target: ToolInvocationStatus::Succeeded,
            input_json: Some(input.to_string()),
            content: "sensitive original result".into(),
            is_error: false,
            metadata: None,
            output_sha256: None,
            error_code: None,
            cleanup_status: CleanupStatus::NotRequired,
            postprocessing: Some(json!({"artifact":"sensitive receipt"})),
        })
        .await
        .unwrap()
    else {
        panic!("result commit");
    };
    assert!(
        db.complete_external_tool_operation(&session, &run, "effect", &committed.message.id)
            .await
            .is_err()
    );
    assert_eq!(
        db.complete_tool_result_postprocessing_cas("physical", 0)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    assert!(
        db.complete_external_tool_operation(&session, &run, "effect", "wrong-message")
            .await
            .is_err()
    );
    let facts = db
        .complete_external_tool_operation(&session, &run, "effect", &committed.message.id)
        .await
        .unwrap();
    assert_eq!(facts.invocation_id, "physical");
    assert!(
        matches!(db.admit_external_tool_operation(&session,&run,"effect","Write",&input).await.unwrap(),ExternalToolAdmission::Completed {message_id,..} if message_id==committed.message.id)
    );
    db.with_writer(|conn| {
        let stored: String =
            conn.query_row("SELECT input_json FROM external_tool_requests", [], |row| {
                row.get(0)
            })?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored).unwrap()["content"],
            "sensitive operation body"
        );
        assert!(
            conn.execute("UPDATE external_tool_requests SET input_json='{}'", [])
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Close and reopen the same real database between original effect and replay assertions"
)]
async fn reopening_preserves_unknown_operations_and_original_results_with_separate_hook_notes() {
    let directory =
        std::env::temp_dir().join(format!("zk-operation-reopen-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("store.sqlite");
    let db = Db::open(&path).unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap().id;
    let (task, run) = service(&db, &session).await;
    let input = json!({"content":"one durable effect"});
    assert!(matches!(
        db.admit_external_tool_operation(&session, &run, "unknown", "Write", &input)
            .await
            .unwrap(),
        ExternalToolAdmission::Accepted { .. }
    ));
    let ExternalToolAdmission::Accepted { tool_use_id } = db
        .admit_external_tool_operation(&session, &run, "completed", "Write", &input)
        .await
        .unwrap()
    else {
        panic!("first claim");
    };
    let assistant = db
        .append_attributed_message(
            &session,
            NewMessage {
                role: MessageRole::Assistant,
                content: vec![StoredBlock::ToolUse {
                    id: tool_use_id.clone(),
                    name: "Write".into(),
                    input: input.clone(),
                }],
                meta: None,
                stop_reason: Some("tool_use".into()),
                input_tokens: 0,
                output_tokens: 0,
            },
            MessageAttribution {
                task_id: Some(task.clone()),
                run_id: Some(run.clone()),
                origin: "runtime".into(),
                source_task_id: None,
            },
        )
        .await
        .unwrap();
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: "completed-invocation".into(),
        task_id: task,
        run_id: run.clone(),
        tool_use_id: tool_use_id.clone(),
        tool_name: "Write".into(),
        input_json: Some(input.to_string()),
        side_effect_class: "write".into(),
        directory_generation: None,
        connection_generation: None,
    })
    .await
    .unwrap();
    let CommitToolInvocationResultOutcome::Committed(result) = db
        .commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: "completed-invocation".into(),
            expected_version: 0,
            session_id: session.clone(),
            target: ToolInvocationStatus::Succeeded,
            input_json: Some(input.to_string()),
            content: "immutable original output".into(),
            is_error: false,
            metadata: None,
            output_sha256: None,
            error_code: None,
            cleanup_status: CleanupStatus::NotRequired,
            postprocessing: None,
        })
        .await
        .unwrap()
    else {
        panic!("result commit");
    };
    db.complete_external_tool_operation(&session, &run, "completed", &result.message.id)
        .await
        .unwrap();
    db.save_hook_presentation(&session, &run, &tool_use_id, "trusted display note")
        .await
        .unwrap();
    drop(db);
    let reopened = Db::open(&path).unwrap();
    assert!(matches!(
        reopened
            .admit_external_tool_operation(&session, &run, "unknown", "Write", &input)
            .await
            .unwrap(),
        ExternalToolAdmission::Unconfirmed
    ));
    assert!(
        matches!(reopened.admit_external_tool_operation(&session,&run,"completed","Write",&input).await.unwrap(),ExternalToolAdmission::Completed {message_id,..} if message_id==result.message.id)
    );
    let original = reopened
        .get_message_by_id(&result.message.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.content, result.message.content);
    let notes = reopened.hook_presentations(&session, 0, 200).await.unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(
        notes[0].assistant_message_id.as_deref(),
        Some(assistant.id.as_str())
    );
    assert_eq!(notes[0].text, "trusted display note");
    assert!(
        reopened
            .hook_presentations("foreign-session", 0, 200)
            .await
            .unwrap()
            .is_empty()
    );
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn temporary_sessions_cannot_create_external_services_or_operation_bodies() {
    let db = Db::open_in_memory().unwrap();
    let (session, _lease) = db
        .create_ephemeral_session("fixture", "/tmp", "DEFAULT")
        .await
        .unwrap();
    let request = service_request(&session);
    let error = db.create_task_with_run(&request).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("EPHEMERAL_OPERATION_UNSUPPORTED")
    );
    assert!(
        db.admit_external_tool_operation(
            &session,
            &request.run_id,
            "operation",
            "Write",
            &json!({"content":"never store this body"})
        )
        .await
        .is_err()
    );
    db.with_reader(|conn| {
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM external_tool_requests", [], |row| row
                .get::<_, i64>(0))?,
            0
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get::<_, i64>(0))?,
            0
        );
        Ok(())
    })
    .await
    .unwrap();
}
