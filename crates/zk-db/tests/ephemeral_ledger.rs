//! Physical ledgers keep attribution and fees while scoped bodies never touch WAL.
use sha2::{Digest, Sha256};
use zk_db::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // One RAM-content expiry with late authoritative usage settlement.
async fn physical_tool_and_llm_bodies_stay_in_ram_but_expired_usage_is_accounted() {
    let path = std::env::temp_dir().join(format!("zk-private-ledger-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    let db = Db::open(path.join("ledger.sqlite")).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", path.to_str().unwrap(), "DONT_ASK")
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
            cost_limit_nanos_usd: Some(1_000_000_000),
            deadline_at_ms: Some(time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    let secret = format!("private-ledger-input-{}", uuid::Uuid::new_v4());
    let digest = format!("{:x}", Sha256::digest(secret.as_bytes()));
    let input = serde_json::json!({"command":secret}).to_string();
    let invocation = NewToolInvocation {
        invocation_id: "invocation".into(),
        task_id: run.clone(),
        run_id: run.clone(),
        tool_use_id: "call".into(),
        tool_name: "Bash".into(),
        input_json: Some(input.clone()),
        side_effect_class: "read".into(),
        directory_generation: None,
        connection_generation: None,
    };
    assert_eq!(
        db.create_tool_invocation(&invocation)
            .await
            .unwrap()
            .input_json
            .as_deref(),
        Some(input.as_str())
    );
    assert_eq!(
        db.transition_tool_invocation_cas(
            "invocation",
            0,
            ToolInvocationStatus::Running,
            Some(&input),
            None,
            None,
            CleanupStatus::Pending
        )
        .await
        .unwrap(),
        CasOutcome::Applied
    );
    let committed = db
        .commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: "invocation".into(),
            expected_version: 1,
            session_id: session.clone(),
            target: ToolInvocationStatus::Succeeded,
            input_json: Some(input),
            content: secret.clone(),
            is_error: false,
            metadata: Some(serde_json::json!({"body":secret})),
            output_sha256: Some(digest.clone()),
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            postprocessing: Some(serde_json::json!({"body":secret})),
        })
        .await
        .unwrap();
    match committed {
        CommitToolInvocationResultOutcome::Committed(record) => {
            assert!(record.invocation.output_ref.unwrap().contains(&digest));
        }
        _ => panic!("not committed"),
    }
    db.complete_tool_result_postprocessing_cas("invocation", 0)
        .await
        .unwrap();
    db.start_llm_call_with_budget(
        &NewLlmCall {
            call_id: "physical".into(),
            task_id: run.clone(),
            run_id: run.clone(),
            provider: "fixture".into(),
            model: "fixture".into(),
            route: Some(secret.clone()),
            provider_request_id: Some(secret.clone()),
        },
        &LlmCallBudgetReservation {
            input_tokens: 10,
            output_tokens: 10,
            cost_nanos_usd: 10000,
        },
    )
    .await
    .unwrap();
    // Raw SQL paths must fail even when the Rust codec is accidentally bypassed.
    db.with_writer(|conn| {
        assert!(
            conn.execute(
                "UPDATE tool_invocations SET input_json='{\"command\":\"raw\"}'",
                []
            )
            .is_err()
        );
        assert!(
            conn.execute("UPDATE llm_calls SET route='raw'", [])
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
    let mut scope = invocation.clone();
    scope.invocation_id = "runtime-scope".into();
    scope.tool_use_id = "runtime-scope".into();
    scope.tool_name = "RunToolScope".into();
    scope.side_effect_class = "write".into();
    scope.input_json = Some("{}".into());
    db.create_run_scope_invocation(&scope).await.unwrap();
    db.start_tool_invocation_for_active_run_cas("runtime-scope", 0, "{}", "write")
        .await
        .unwrap();
    assert_eq!(
        db.finish_run_scope_invocation(
            "invocation",
            2,
            ToolInvocationStatus::Succeeded,
            None,
            CleanupStatus::Confirmed
        )
        .await
        .unwrap(),
        CasOutcome::NotFound
    );
    drop(lease);
    assert_eq!(
        db.finish_run_scope_invocation(
            "runtime-scope",
            1,
            ToolInvocationStatus::Succeeded,
            None,
            CleanupStatus::Confirmed
        )
        .await
        .unwrap(),
        CasOutcome::Applied
    );
    assert_eq!(
        db.finish_llm_call(
            "physical",
            "failed",
            &LlmUsageCompletion {
                input_tokens: Some(7),
                output_tokens: Some(3),
                cache_read_tokens: Some(0),
                cache_create_tokens: Some(0),
                cost_nanos_usd: Some(234),
                usage_complete: true,
                error_code: Some(secret.clone())
            }
        )
        .await
        .unwrap(),
        CasOutcome::Applied
    );
    let ledger = db.find_run_by_id(&run).await.unwrap().unwrap();
    assert_eq!(ledger.input_tokens, 7);
    assert_eq!(ledger.cost_nanos_usd, 234);
    db.with_reader(|conn| {
        let error: String =
            conn.query_row("SELECT error_code FROM llm_calls", [], |row| row.get(0))?;
        assert_eq!(error, content::UNAVAILABLE_DIAGNOSTIC);
        Ok(())
    })
    .await
    .unwrap();
    for file in std::fs::read_dir(&path).unwrap() {
        let file = file.unwrap().path();
        if file.is_file() {
            let bytes = std::fs::read(&file).unwrap();
            for needle in [&secret, &digest] {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|part| part == needle.as_bytes()),
                    "leaked into {}",
                    file.display()
                );
            }
        }
    }
    db.request_runtime_shutdown().await.unwrap();
    db.reconcile_runtime_after_restart().await.unwrap();
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn exhausted_body_capacity_does_not_drop_provider_usage_or_cost() {
    let db = Db::open_in_memory().unwrap();
    let (session, _lease) = db
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
            cost_limit_nanos_usd: Some(100_000),
            deadline_at_ms: Some(time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    db.start_llm_call_with_budget(
        &NewLlmCall {
            call_id: "capacity-call".into(),
            task_id: run.clone(),
            run_id: run.clone(),
            provider: "fixture".into(),
            model: "fixture".into(),
            route: None,
            provider_request_id: None,
        },
        &LlmCallBudgetReservation {
            input_tokens: 10,
            output_tokens: 10,
            cost_nanos_usd: 10_000,
        },
    )
    .await
    .unwrap();
    let store = db.memory_content_store();
    // Exhaust the real production limit without relying on entry accounting internals.
    for size in [1024 * 1024, 1024, 1, 0] {
        let chunk = vec![b'x'; size];
        while store.put(&session, &chunk).is_ok() {}
    }
    assert!(store.put(&session, b"new private body").is_err());
    assert_eq!(
        db.finish_llm_call(
            "capacity-call",
            "failed",
            &LlmUsageCompletion {
                input_tokens: Some(7),
                output_tokens: Some(3),
                cache_read_tokens: Some(0),
                cache_create_tokens: Some(0),
                cost_nanos_usd: Some(234),
                usage_complete: true,
                error_code: Some("private provider diagnostic".into()),
            }
        )
        .await
        .unwrap(),
        CasOutcome::Applied
    );
    let ledger = db.find_run_by_id(&run).await.unwrap().unwrap();
    assert_eq!(ledger.input_tokens, 7);
    assert_eq!(ledger.output_tokens, 3);
    assert_eq!(ledger.cost_nanos_usd, 234);
    db.with_reader(|conn| {
        let error: String = conn.query_row(
            "SELECT error_code FROM llm_calls WHERE call_id='capacity-call'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(error, content::UNAVAILABLE_DIAGNOSTIC);
        Ok(())
    })
    .await
    .unwrap();
}
