//! Atomic native Bash declaration batches, replay and ownership regression.
use serde_json::json;
use zk_db::{
    CasOutcome, CleanupStatus, Db, NewToolInvocation, ProducedShellArtifactRecord,
    ToolInvocationStatus,
};

async fn completed_declaration_batch() -> (Db, String) {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("model", "/tmp/artifacts").await.unwrap();
    db.start_run("batch-run", &session.id, None, Some("query"), "model")
        .await
        .unwrap();
    let run = db.find_run_by_id("batch-run").await.unwrap().unwrap();
    let input = json!({"command":"printf artifact","declared_outputs":[{"path":"a","operation":"created"},{"path":"b","operation":"created"}]}).to_string();
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: "batch-invocation".into(),
        task_id: run.task_id,
        run_id: "batch-run".into(),
        tool_use_id: "bash-batch".into(),
        tool_name: "Bash".into(),
        input_json: Some(input.clone()),
        side_effect_class: "write".into(),
        directory_generation: Some(1),
        connection_generation: None,
    })
    .await
    .unwrap();
    assert_eq!(
        db.transition_tool_invocation_cas(
            "batch-invocation",
            0,
            ToolInvocationStatus::Succeeded,
            Some(&input),
            Some("result:batch"),
            None,
            CleanupStatus::NotRequired
        )
        .await
        .unwrap(),
        CasOutcome::Applied
    );
    (db, session.id)
}

#[tokio::test]
async fn declared_batches_match_each_original_declaration_atomically_and_are_idempotent() {
    let (db, session) = completed_declaration_batch().await;
    let receipt = |path: &str| ProducedShellArtifactRecord {
        requested_path: path.into(),
        canonical_path: format!("/tmp/artifacts/{path}"),
        operation: "created".into(),
        previous_hash: None,
        sealed_hash: Some("a".repeat(64)),
        file_size: Some(1),
        required_validator_id: None,
    };
    let record = |receipts| {
        db.record_declared_shell_artifacts(
            "batch-run",
            &session,
            "/tmp/artifacts",
            "bash-batch",
            "batch-invocation",
            receipts,
        )
    };
    let mut duplicate = receipt("b");
    duplicate.requested_path = "a".into();
    assert!(
        record(vec![receipt("a"), duplicate]).await.is_err(),
        "one original declaration cannot stand in for the complete batch"
    );
    assert!(
        db.find_artifact_manifest_by_run("batch-run")
            .await
            .unwrap()
            .is_none()
    );
    let mut invalid = receipt("b");
    invalid.sealed_hash = Some("invalid".into());
    assert!(record(vec![receipt("a"), invalid]).await.is_err());
    assert!(
        db.find_artifact_manifest_by_run("batch-run")
            .await
            .unwrap()
            .is_none(),
        "the valid prefix must not publish separately"
    );
    record(vec![receipt("a"), receipt("b")]).await.unwrap();
    let manifest = db
        .find_artifact_manifest_by_run("batch-run")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.entries.len(), 2);
    record(vec![receipt("a"), receipt("b")]).await.unwrap();
    assert_eq!(
        db.find_artifact_manifest_by_run("batch-run")
            .await
            .unwrap()
            .unwrap(),
        manifest
    );
    assert!(
        db.record_declared_shell_artifacts(
            "batch-run",
            "foreign",
            "/tmp/artifacts",
            "bash-batch",
            "batch-invocation",
            vec![receipt("a"), receipt("b")]
        )
        .await
        .is_err()
    );
}
