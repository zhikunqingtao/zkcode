//! Positive retained-owner evidence can repair cleanup without rewriting an outcome.
use zk_db::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // One owner proof, failed cleanup and immutable-result reconciliation lifecycle.
async fn retained_owner_cleanup_requires_exact_identity_and_preserves_terminal_result() {
    let db = Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    let run = uuid::Uuid::new_v4().to_string();
    db.start_root_run_with_budget_at_epoch(
        &run,
        &session,
        Some("query"),
        "fixture",
        &TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(1_000_000),
            deadline_at_ms: Some(time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    db.create_run_scope_invocation(&NewToolInvocation {
        invocation_id: "scope".into(),
        task_id: run.clone(),
        run_id: run.clone(),
        tool_use_id: "scope-use".into(),
        tool_name: "RunToolScope".into(),
        input_json: Some("{}".into()),
        side_effect_class: "write".into(),
        directory_generation: None,
        connection_generation: None,
    })
    .await
    .unwrap();
    db.register_execution_resource(&NewExecutionResource {
        resource_id: "process".into(),
        task_id: run.clone(),
        run_id: run.clone(),
        invocation_id: Some("scope".into()),
        resource_kind: "processGroup".into(),
        external_id: Some("retained-process-identity".into()),
        metadata_json: "{}".into(),
    })
    .await
    .unwrap();
    db.finalize_execution_resource("process", ExecutionResourceStatus::Unconfirmed)
        .await
        .unwrap();
    assert_eq!(
        db.finish_run_scope_invocation(
            "scope",
            0,
            ToolInvocationStatus::Interrupted,
            None,
            CleanupStatus::Unconfirmed
        )
        .await
        .unwrap(),
        CasOutcome::Applied
    );
    db.ensure_task_final_assistant(&run, &run, "cleanup was unconfirmed")
        .await
        .unwrap();
    let task = db.find_runtime_task_by_id(&run).await.unwrap().unwrap();
    assert!(matches!(
        db.commit_task_result_with_run_usage_fallback(
            &CommitTaskResult {
                task_id: run.clone(),
                run_id: run.clone(),
                expected_task_version: task.version,
                status: ResultStatus::Error,
                content: "cleanup was unconfirmed".into(),
                media_type: "text/plain".into(),
                error_code: None,
                cleanup_status: CleanupStatus::Unconfirmed,
                verification_status: VerificationStatus::NotRequested,
            },
            RunUsageFallback {
                usage_complete: true,
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        CommitTaskResultOutcome::Committed { .. }
    ));
    let before: String = db
        .with_reader(|conn| {
            Ok(conn.query_row(
                "SELECT ephemeral_content_ref FROM task_results",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    drop(lease);
    assert_eq!(
        db.retry_confirmed_run_cleanup(&run).await.unwrap(),
        CleanupStatus::Unconfirmed
    );
    assert!(
        !db.invocation_resources_released("scope", &run)
            .await
            .unwrap()
    );
    assert!(
        !db.invocation_resources_released("missing", &run)
            .await
            .unwrap()
    );
    let proof = db
        .execution_resource_release_proof("process")
        .await
        .unwrap()
        .unwrap();
    let mut wrong = proof.clone();
    wrong.run_id = "other-run".into();
    assert!(
        db.reconcile_execution_resource_release(&wrong)
            .await
            .is_err()
    );
    let mut wrong = proof.clone();
    wrong.external_id = "different-process".into();
    assert!(
        db.reconcile_execution_resource_release(&wrong)
            .await
            .is_err()
    );
    let mut stale = proof.clone();
    stale.version -= 1;
    assert_eq!(
        db.reconcile_execution_resource_release(&stale)
            .await
            .unwrap(),
        CasOutcome::VersionConflict
    );
    assert_eq!(
        db.finalize_execution_resource("process", ExecutionResourceStatus::Released)
            .await
            .unwrap(),
        CasOutcome::InvalidTransition
    );
    assert_eq!(
        db.reconcile_execution_resource_release(&proof)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    assert_eq!(
        db.reconcile_execution_resource_release(&proof)
            .await
            .unwrap(),
        CasOutcome::VersionConflict
    );
    assert!(
        db.invocation_resources_released("scope", &run)
            .await
            .unwrap()
    );
    assert!(
        !db.invocation_resources_released("scope", "foreign-run")
            .await
            .unwrap()
    );
    assert_eq!(
        db.reconcile_run_scope_cleanup("scope", "foreign-run")
            .await
            .unwrap(),
        CasOutcome::NotFound
    );
    assert_eq!(
        db.reconcile_run_scope_cleanup("scope", &run).await.unwrap(),
        CasOutcome::Applied
    );
    assert_eq!(
        db.retry_confirmed_run_cleanup(&run).await.unwrap(),
        CleanupStatus::Confirmed
    );
    assert_eq!(
        db.retry_confirmed_run_cleanup(&run).await.unwrap(),
        CleanupStatus::Confirmed
    );
    db.with_reader(move |conn| {
        let after: String = conn.query_row(
            "SELECT ephemeral_content_ref FROM task_results",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            before, after,
            "immutable result reference must not be rewritten"
        );
        let (status, cleanup): (String, String) = conn.query_row(
            "SELECT status,cleanup_status FROM tool_invocations WHERE invocation_id='scope'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(status, "interrupted");
        assert_eq!(cleanup, "confirmed");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn scope_retry_cannot_ignore_an_unreleased_resource_or_spoofed_tool_name() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap();
    let run = uuid::Uuid::new_v4().to_string();
    db.start_root_run_with_budget_at_epoch(
        &run,
        &session.id,
        Some("query"),
        "fixture",
        &TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(1_000_000),
            deadline_at_ms: Some(time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    let record = NewToolInvocation {
        invocation_id: "scope".into(),
        task_id: run.clone(),
        run_id: run.clone(),
        tool_use_id: "use".into(),
        tool_name: "RunToolScope".into(),
        input_json: Some("{}".into()),
        side_effect_class: "write".into(),
        directory_generation: None,
        connection_generation: None,
    };
    db.create_run_scope_invocation(&record).await.unwrap();
    db.register_execution_resource(&NewExecutionResource {
        resource_id: "pending".into(),
        task_id: run.clone(),
        run_id: run.clone(),
        invocation_id: Some("scope".into()),
        resource_kind: "processGroup".into(),
        external_id: Some("identity".into()),
        metadata_json: "{}".into(),
    })
    .await
    .unwrap();
    db.finish_run_scope_invocation(
        "scope",
        0,
        ToolInvocationStatus::Interrupted,
        None,
        CleanupStatus::Unconfirmed,
    )
    .await
    .unwrap();
    assert!(db.reconcile_run_scope_cleanup("scope", &run).await.is_err());
    let mut spoof = record;
    spoof.invocation_id = "spoof".into();
    spoof.tool_use_id = "spoof-use".into();
    db.create_tool_invocation(&spoof).await.unwrap();
    assert_eq!(
        db.reconcile_run_scope_cleanup("spoof", &run).await.unwrap(),
        CasOutcome::NotFound
    );
    assert_eq!(
        db.reconcile_execution_resource_release(
            &db.execution_resource_release_proof("pending")
                .await
                .unwrap()
                .unwrap()
        )
        .await
        .unwrap(),
        CasOutcome::InvalidTransition
    );
}
