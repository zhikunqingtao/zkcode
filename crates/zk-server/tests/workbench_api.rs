//! WP-06 workbench persistence, evidence binding and authorization tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use zk_db::{
    AcceptanceCriterionRecord, ArtifactEntryRecord, ArtifactManifestRecord, CleanupStatus,
    CreateTaskWithRun, EvidenceBundleRecord, EvidenceOrigin, LlmCallBudgetReservation,
    LlmUsageCompletion, NewLlmCall, NewToolInvocation, ProducedResearchCapture,
    ProducedResearchEntry, ProducedResearchKind, ToolInvocationStatus, WorkbenchBindingRecord,
    model::{MessageRole, NewMessage, StoredBlock},
};

#[tokio::test]
#[allow(clippy::too_many_lines)] // full real Router/SQLite workbench round trip
async fn workbench_round_trip_requires_owned_run_and_owned_evidence() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-api")
        .await
        .expect("session");
    let other = db
        .create_session("test-model", "/tmp/workbench-api")
        .await
        .expect("other session");
    let request_message = db
        .append_message(
            &session.id,
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: "tests must pass".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("request message");
    db.start_run(
        "workbench-run",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("run");
    let now = "2026-08-22T00:00:00.000000Z".to_owned();
    db.initialize_workbench(
        &WorkbenchBindingRecord {
            root_run_id: "workbench-run".into(),
            request_message_id: request_message.id.clone(),
            result_message_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        &[AcceptanceCriterionRecord {
            criterion_id: "criterion-api".into(),
            root_run_id: "workbench-run".into(),
            ordinal: 0,
            criterion_type: "business".into(),
            source_text: "tests must pass".into(),
            status: "not_verified".into(),
            evidence_bundle_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        }],
    )
    .await
    .expect("workbench");
    db.save_evidence_bundle(&EvidenceBundleRecord {
        bundle_id: "evidence-api".into(),
        session_id: session.id.clone(),
        agent_id: None,
        kind: "test".into(),
        claim: Some("tests pass".into()),
        origin: EvidenceOrigin::Human,
        producer_invocation_id: None,
        verdict: "verified".into(),
        created_at: now,
        run_id: Some("workbench-run".into()),
        items: Vec::new(),
    })
    .await
    .expect("evidence");
    let root_task_id = db
        .find_run_by_id("workbench-run")
        .await
        .expect("run lookup")
        .expect("run")
        .task_id;
    let research_invocation_id = uuid::Uuid::new_v4().to_string();
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: research_invocation_id.clone(),
        task_id: root_task_id.clone(),
        run_id: "workbench-run".into(),
        tool_use_id: "research-api".into(),
        tool_name: "WebSearch".into(),
        input_json: Some(r#"{"query":"durability"}"#.into()),
        side_effect_class: "read".into(),
        directory_generation: Some(1),
        connection_generation: None,
    })
    .await
    .expect("research invocation");
    db.transition_tool_invocation_cas(
        &research_invocation_id,
        0,
        ToolInvocationStatus::Succeeded,
        Some(r#"{"query":"durability"}"#),
        Some("toolResult:research-api"),
        None,
        CleanupStatus::NotRequired,
    )
    .await
    .expect("research invocation terminal");
    db.record_research_capture(&ProducedResearchCapture {
        task_id: root_task_id,
        run_id: "workbench-run".into(),
        producer_invocation_id: research_invocation_id,
        kind: ProducedResearchKind::WebSearch,
        query: Some("durability".into()),
        fetched_at: "2026-08-22T00:00:01.000000Z".into(),
        entries: vec![ProducedResearchEntry {
            url: "https://example.com/durability".into(),
            title: Some("Durability".into()),
            provider: Some("fixture".into()),
            excerpt: Some("A bounded cited finding.".into()),
            rank: Some(1),
            http_status: None,
            content_type: None,
            truncated: false,
        }],
    })
    .await
    .expect("research capture");

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/workbench/workbench-run",
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workbench = json_body(&body);
    assert_eq!(workbench["binding"]["rootRunId"], "workbench-run");
    assert_eq!(workbench["research"]["sources"][0]["title"], "Durability");
    assert_eq!(
        workbench["research"]["findings"][0]["excerpt"],
        "A bounded cited finding."
    );

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/sessions/{}/workbench/current", session.id),
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = json_body(&body);
    assert_eq!(current["correlationMode"], "EXACT");
    assert_eq!(current["rootRun"]["id"], "workbench-run");
    assert_eq!(current["runTree"].as_array().expect("run tree").len(), 1);
    assert_eq!(current["usage"]["inputTokens"], 0);
    assert_eq!(current["usage"]["complete"], true);
    assert_eq!(current["research"]["sources"].as_array().unwrap().len(), 1);
    assert_eq!(current["requestMessageId"], request_message.id);
    assert_eq!(
        current["verification"]["businessCriteria"][0]["status"],
        "NOT_VERIFIED"
    );

    let update = serde_json::json!({
        "criteria": [{
            "criterionId": "criterion-api",
            "status": "passed",
            "evidenceBundleId": "evidence-api"
        }]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/workbench/workbench-run",
            Method::PUT,
            Some(update),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let updated = json_body(&body);
    assert_eq!(updated["criteria"][0]["status"], "passed");
    assert_eq!(updated["criteria"][0]["evidenceBundleId"], "evidence-api");

    db.save_evidence_bundle(&EvidenceBundleRecord {
        bundle_id: "model-assertion-api".into(),
        session_id: session.id.clone(),
        agent_id: Some("agent-claim".into()),
        kind: "claim".into(),
        claim: Some("the model says tests passed".into()),
        origin: EvidenceOrigin::ModelAssertion,
        producer_invocation_id: None,
        verdict: "pending".into(),
        created_at: "2026-08-22T00:01:00.000000Z".into(),
        run_id: Some("workbench-run".into()),
        items: Vec::new(),
    })
    .await
    .expect("model assertion");
    let forged_update = serde_json::json!({
        "criteria": [{
            "criterionId": "criterion-api",
            "status": "passed",
            "evidenceBundleId": "model-assertion-api"
        }]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/workbench/workbench-run",
            Method::PUT,
            Some(forged_update),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "EVIDENCE_VERDICT_MISMATCH");

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/workbench/workbench-run",
            Method::GET,
            None,
            &[("x-session-id", &other.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "RUN_NOT_FOUND");
}

#[tokio::test]
async fn session_without_run_returns_an_empty_projection() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-empty")
        .await
        .expect("session");
    db.append_message(
        &session.id,
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "must not be guessed into an execution".into(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("unbound conversation message");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/sessions/{}/workbench/current", session.id),
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = json_body(&body);
    assert_eq!(current["correlationMode"], "EMPTY");
    assert!(current["request"].is_null());
    assert!(current["result"].is_null());
    assert!(current["rootTask"].is_null());
    assert_eq!(current["taskTree"], serde_json::json!([]));
    assert!(current["rootRun"].is_null());
    assert_eq!(current["runTree"], serde_json::json!([]));
    assert_eq!(
        current["usage"],
        serde_json::json!({
            "inputTokens": 0,
            "outputTokens": 0,
            "cacheReadTokens": 0,
            "cacheCreateTokens": 0,
            "costNanosUsd": 0,
            "complete": true,
        })
    );
    assert_eq!(current["eventHighWater"], 0);
    assert_eq!(current["activeTools"], serde_json::json!([]));
    assert_eq!(current["delivery"]["totalFiles"], 0);
    assert_eq!(current["pendingActionCount"], 0);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One projection fixture proves the recursive run tree and subtree usage together.
async fn current_projection_includes_internal_child_runs_and_subtree_usage() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-run-tree")
        .await
        .expect("root session");
    let root = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
            root_session_id: session.id.clone(),
            transcript_session_id: session.id.clone(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "root research".into(),
            prompt: Some("coordinate one child".into()),
            task_type: "agent".into(),
            model: "test-model".into(),
            working_dir: "/tmp/workbench-run-tree".into(),
            execution_config_json: serde_json::json!({
                "isolation": "readOnly",
                "budget": {
                    "tokenLimit": 1_000_000,
                    "costLimitNanosUsd": 1_000_000_000_000_i64,
                    "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                },
            })
            .to_string(),
            startup_epoch: 1,
        })
        .await
        .expect("root task/run");
    let child_transcript_session_id = uuid::Uuid::new_v4().to_string();
    let child = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
            root_session_id: session.id.clone(),
            transcript_session_id: child_transcript_session_id.clone(),
            parent_task_id: Some(root.task.id.clone()),
            parent_run_id: Some(root.run_id.clone()),
            creator_tool_use_id: Some("agent-call-1".into()),
            ordinal: 0,
            description: "internal child research".into(),
            prompt: Some("collect evidence".into()),
            task_type: "agent".into(),
            model: "test-model".into(),
            working_dir: "/tmp/workbench-run-tree".into(),
            execution_config_json: r#"{"isolation":"readOnly"}"#.into(),
            startup_epoch: 1,
        })
        .await
        .expect("child task/run");

    for (task_id, run_id) in [
        (root.task.id.clone(), root.run_id.clone()),
        (child.task.id.clone(), child.run_id.clone()),
    ] {
        db.with_conn_blocking(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE tasks SET status='running',version=version+1 WHERE id=?1 AND status='queued'",
                [&task_id],
            )?;
            transaction.execute(
                "UPDATE run_envelopes SET status='running',version=version+1 WHERE id=?1 AND status='queued'",
                [&run_id],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .expect("claim fixture task and run");
    }

    for (task_id, run_id, input, output, cache_read, cost) in [
        (&root.task.id, &root.run_id, 11, 5, 2, 125_000_000),
        (&child.task.id, &child.run_id, 7, 3, 1, 75_000_000),
    ] {
        let call_id = uuid::Uuid::new_v4().to_string();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                provider: "script-provider".into(),
                model: "test-model".into(),
                route: Some("primary".into()),
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: input,
                output_tokens: output,
                cost_nanos_usd: cost,
            },
        )
        .await
        .expect("physical llm call starts");
        db.finish_llm_call(
            &call_id,
            "completed",
            &LlmUsageCompletion {
                input_tokens: Some(input),
                output_tokens: Some(output),
                cache_read_tokens: Some(cache_read),
                cache_create_tokens: Some(0),
                cost_nanos_usd: Some(cost),
                usage_complete: true,
                error_code: None,
            },
        )
        .await
        .expect("physical llm call finishes");
    }

    let active_invocation_id = uuid::Uuid::new_v4().to_string();
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: active_invocation_id.clone(),
        task_id: child.task.id.clone(),
        run_id: child.run_id.clone(),
        tool_use_id: "child-read-active".into(),
        tool_name: "Read".into(),
        input_json: Some(r#"{"filePath":"README.md"}"#.into()),
        side_effect_class: "read".into(),
        directory_generation: Some(1),
        connection_generation: None,
    })
    .await
    .expect("active child invocation");
    db.append_run_event(
        &child.run_id,
        "workbench_snapshot_fixture",
        Some("child-read-active"),
        &serde_json::json!({"phase":"preparing"}),
    )
    .await
    .expect("child event");

    // The session-local query cannot see the child's internal transcript run.
    let root_session_runs = db
        .find_runs_by_session(&session.id, 20)
        .await
        .expect("root session runs");
    assert_eq!(root_session_runs.len(), 1);
    assert_eq!(root_session_runs[0].id, root.run_id);

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/sessions/{}/workbench/current", session.id),
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = json_body(&body);
    assert_eq!(current["correlationMode"], "UNBOUND");
    assert!(current["request"].is_null());
    assert!(current["result"].is_null());
    assert_eq!(current["rootTask"]["id"], root.task.id);
    assert_eq!(current["taskTree"].as_array().expect("task tree").len(), 2);
    let runs = current["runTree"].as_array().expect("recursive run tree");
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0]["id"], root.run_id);
    assert_eq!(runs[0]["sessionId"], session.id);
    assert_eq!(runs[1]["id"], child.run_id);
    assert_eq!(runs[1]["sessionId"], child_transcript_session_id);
    assert_eq!(runs[1]["parentRunId"], root.run_id);
    assert_eq!(runs[1]["taskId"], child.task.id);
    assert_eq!(current["usage"]["inputTokens"], 18);
    assert_eq!(current["usage"]["outputTokens"], 8);
    assert_eq!(current["usage"]["cacheReadTokens"], 3);
    assert_eq!(current["usage"]["cacheCreateTokens"], 0);
    assert_eq!(current["usage"]["costNanosUsd"], 200_000_000);
    assert_eq!(current["usage"]["complete"], true);
    assert!(current["eventHighWater"].as_i64().unwrap_or_default() > 0);
    let active_tools = current["activeTools"].as_array().expect("active tools");
    assert_eq!(active_tools.len(), 1);
    assert_eq!(active_tools[0]["invocationId"], active_invocation_id);
    assert_eq!(active_tools[0]["taskId"], child.task.id);
    assert_eq!(active_tools[0]["runId"], child.run_id);
    assert_eq!(active_tools[0]["status"], "preparing");
}

#[tokio::test]
async fn workbench_task_search_returns_global_reference_groups_without_session_header() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-task-search")
        .await
        .expect("session");
    db.append_message(
        &session.id,
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "verify release".into(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("goal message");
    let unrelated = db
        .create_session("test-model", "/tmp/unrelated-folder")
        .await
        .expect("unrelated session");
    db.update_session_title(&unrelated.id, "different task")
        .await
        .expect("unrelated title");

    let (status, _, body) = call(
        &mut app,
        common::local_get("/api/workbench/tasks?query=release"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response = json_body(&body);
    let groups = response["groups"].as_array().expect("groups");
    assert_eq!(groups.len(), 4);
    assert_eq!(groups[0]["status"], "ACTION_REQUIRED");
    assert_eq!(groups[0]["label"], "待我处理");
    assert_eq!(groups[1]["status"], "RUNNING");
    assert_eq!(groups[2]["status"], "REVIEWABLE");
    assert_eq!(groups[3]["status"], "OTHER");
    let tasks = groups[3]["tasks"].as_array().expect("other tasks");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["sessionId"], session.id);
    assert_eq!(tasks[0]["title"], "verify release");
    assert_eq!(tasks[0]["folderName"], "workbench-task-search");
    assert_eq!(tasks[0]["pendingCount"], 0);
    assert_eq!(tasks[0]["hint"], "尚未开始执行");
    assert!(tasks[0]["updatedAt"].as_str().is_some());
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // current/previous delivery fallback integration matrix
async fn failed_current_run_exposes_the_previous_completed_delivery() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-previous-delivery")
        .await
        .expect("session");
    let previous_request = db
        .append_message(
            &session.id,
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: "build the report".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("request");
    let previous_result = db
        .append_message(
            &session.id,
            NewMessage {
                role: MessageRole::Assistant,
                content: vec![StoredBlock::Text {
                    text: "report delivered".into(),
                }],
                stop_reason: Some("end_turn".into()),
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("result");
    db.start_run(
        "previous-root",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("previous run");
    db.complete_run("previous-root", 0, 0.0, 1)
        .await
        .expect("complete previous");
    let now = "2026-08-22T00:00:00.000000Z".to_owned();
    db.initialize_workbench(
        &WorkbenchBindingRecord {
            root_run_id: "previous-root".into(),
            request_message_id: previous_request.id,
            result_message_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        &[],
    )
    .await
    .expect("previous workbench");
    db.bind_workbench_result("previous-root", &previous_result.id)
        .await
        .expect("bind previous result");
    db.save_artifact_manifest(&ArtifactManifestRecord {
        manifest_id: "previous-manifest".into(),
        run_id: "previous-root".into(),
        session_id: session.id.clone(),
        workspace_root: "/tmp/workbench-previous-delivery".into(),
        state: "verified".into(),
        created_at: now.clone(),
        updated_at: now.clone(),
        entries: vec![ArtifactEntryRecord {
            artifact_id: "previous-artifact".into(),
            tool_use_id: "write-report".into(),
            producer_invocation_id: None,
            canonical_path: "/tmp/workbench-previous-delivery/report.md".into(),
            operation: "created".into(),
            state: "integrity_verified".into(),
            sealed_hash: Some("abc".into()),
            actual_hash: Some("abc".into()),
            file_size: Some(12),
            required_validator_id: None,
            validator_result: None,
            failure_code: None,
            created_at: now.clone(),
            updated_at: now,
        }],
    })
    .await
    .expect("manifest");

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    db.start_run(
        "failed-root",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("failed run");
    db.terminate_run(
        "failed-root",
        zk_db::run::EXIT_INTERNAL_ERROR,
        Some("provider unavailable"),
    )
    .await
    .expect("terminate current");

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/sessions/{}/workbench/current", session.id),
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = json_body(&body);
    assert_eq!(current["rootRun"]["id"], "failed-root");
    assert_eq!(current["currentFailure"]["status"], "failed");
    assert_eq!(
        current["previousAvailableDelivery"]["rootRunId"],
        "previous-root"
    );
    assert_eq!(
        current["previousAvailableDelivery"]["delivery"]["totalFiles"],
        1
    );
    assert_eq!(
        current["previousAvailableDelivery"]["result"]["text"],
        "report delivered"
    );
}
