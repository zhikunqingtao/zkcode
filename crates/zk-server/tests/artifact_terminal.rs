//! Production `TaskRuntime` completion performs one read-only artifact integrity projection.
use sha2::{Digest, Sha256};
use std::time::Duration;
use zk_db::{
    CleanupStatus, NewToolInvocation, ProducedFileArtifactRecord, TaskBudgetLimits,
    ToolInvocationStatus,
};
use zk_engine::{ExternalRootSubmission, TaskExecutionResult, TaskOutputRequest};
use zk_server::{config::Config, state::AppState};

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Follow one immutable manifest through real completion, repeated delivery, disk mutation and restart reconciliation"
)]
async fn terminal_projection_is_automatic_idempotent_and_never_runs_artifacts() {
    let directory =
        std::env::temp_dir().join(format!("zk-terminal-artifact-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let workspace = std::fs::canonicalize(&directory).unwrap();
    let db = zk_db::Db::open(workspace.join("data.sqlite")).unwrap();
    let epoch = db.begin_runtime_startup_epoch().await.unwrap();
    let session = db
        .create_session("test-model", workspace.to_str().unwrap())
        .await
        .unwrap();
    let mut config = Config::test_config();
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(workspace.join("snapshots"));
    config.mcp_registry_path = workspace.join("mcp.json");
    let state = AppState::new(db.clone(), config);
    let body = b"#!/bin/sh\nprintf SHOULD_NOT_RUN > side-effect.txt\n";
    let path = workspace.join("generated-script.sh");
    let task_db = db.clone();
    let root = workspace.clone();
    let file = path.clone();
    let receipt = state
        .task_runtime()
        .submit_external_root(
            ExternalRootSubmission {
                session_id: session.id.clone(),
                startup_epoch: epoch,
                timeout: Duration::from_secs(20),
                budget: TaskBudgetLimits {
                    token_limit: Some(100),
                    cost_limit_nanos_usd: Some(1000),
                    deadline_at_ms: None,
                },
            },
            move |context| async move {
                // Fixture supplies one real file write and its succeeded physical invocation.
                std::fs::write(&file, body).unwrap();
                let invocation = uuid::Uuid::new_v4().to_string();
                task_db
                    .create_tool_invocation(&NewToolInvocation {
                        invocation_id: invocation.clone(),
                        task_id: context.task_id.clone(),
                        run_id: context.run_id.clone(),
                        tool_use_id: "fixture-write".into(),
                        tool_name: "Write".into(),
                        input_json: Some("{}".into()),
                        side_effect_class: "write".into(),
                        directory_generation: Some(0),
                        connection_generation: None,
                    })
                    .await
                    .unwrap();
                task_db
                    .transition_tool_invocation_cas(
                        &invocation,
                        0,
                        ToolInvocationStatus::Succeeded,
                        Some("{}"),
                        Some("toolResult:fixture"),
                        None,
                        CleanupStatus::Confirmed,
                    )
                    .await
                    .unwrap();
                task_db
                    .record_produced_file_artifact(&ProducedFileArtifactRecord {
                        run_id: context.run_id,
                        session_id: context.transcript_session_id,
                        workspace_root: root.to_string_lossy().into_owned(),
                        tool_use_id: "fixture-write".into(),
                        producer_invocation_id: invocation,
                        canonical_path: file.to_string_lossy().into_owned(),
                        operation: "created".into(),
                        sealed_hash: format!("{:x}", Sha256::digest(body)),
                        file_size: i64::try_from(body.len()).unwrap(),
                    })
                    .await
                    .unwrap();
                TaskExecutionResult::complete("fixture generation finished")
            },
        )
        .await
        .unwrap();
    let output = state
        .task_runtime()
        .read_output(TaskOutputRequest {
            root_session_id: session.id,
            task_id: receipt.task.id,
            wait_ms: 5000,
            result_version: None,
            cursor: 0,
            max_bytes: 1024,
        })
        .await
        .unwrap();
    assert_eq!(
        output.result.unwrap().content,
        "fixture generation finished"
    );
    // read_output can observe SQLite commit immediately; await the separately owned projection.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if db
                .artifact_terminal_check(&receipt.run_id)
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let first = db
        .artifact_terminal_check(&receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, "verified");
    assert!(!workspace.join("side-effect.txt").exists());
    let sealed = db
        .find_artifact_manifest_by_run(&receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sealed.entries[0].actual_hash, sealed.entries[0].sealed_hash);
    // A duplicate terminal notification cannot repeatedly read or bless a changed file.
    std::fs::write(&path, b"changed after the point-in-time observation").unwrap();
    state
        .task_runtime()
        .observe_terminal_run(&receipt.run_id)
        .await;
    let repeated = db
        .artifact_terminal_check(&receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.checked_at, repeated.checked_at);
    assert_eq!(
        db.find_artifact_manifest_by_run(&receipt.run_id)
            .await
            .unwrap()
            .unwrap(),
        sealed
    );
    std::fs::remove_dir_all(directory).unwrap();
}
