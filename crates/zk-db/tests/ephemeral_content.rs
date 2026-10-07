//! Real file/WAL checks for the content-store boundary; full Run privacy tests
//! additionally cover execution ledgers, tools, hooks and filesystem snapshots.

use sha2::{Digest, Sha256};
use zk_db::{Db, MessageRole, NewMessage, StoredBlock};

fn input(text: &str) -> NewMessage {
    NewMessage {
        meta: Some(serde_json::json!({"privateMetadata":text})),
        role: MessageRole::User,
        content: vec![StoredBlock::Text { text: text.into() }],
        stop_reason: None,
        input_tokens: 0,
        output_tokens: 0,
    }
}

#[tokio::test]
async fn ephemeral_messages_and_session_projections_never_reach_database_or_wal() {
    let path = std::env::temp_dir().join(format!("zk-content-store-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    let database = path.join("content.sqlite");
    let db = Db::open(&database).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("local-fixture", path.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let marker = format!("private-conversation-body-{}", uuid::Uuid::new_v4());
    let hash = format!("{:x}", Sha256::digest(marker.as_bytes()));
    let id = uuid::Uuid::new_v4().to_string();
    let record = db
        .append_message_with_id(&id, &session, input(&marker))
        .await
        .unwrap()
        .unwrap();
    assert!(
        db.append_message_with_id(&id, &session, input(&marker))
            .await
            .unwrap()
            .is_none()
    );
    db.update_session_title(&session, &marker).await.unwrap();
    db.update_session_summary(&session, &marker).await.unwrap();
    let loaded = db.get_session(&session).await.unwrap().unwrap();
    assert_eq!(loaded.title.as_deref(), Some(marker.as_str()));
    assert_eq!(loaded.summary.as_deref(), Some(marker.as_str()));
    assert_eq!(loaded.messages[0], record);
    assert_eq!(
        db.list_messages(&session, None, 50)
            .await
            .unwrap()
            .unwrap()
            .messages
            .as_slice(),
        std::slice::from_ref(&record)
    );
    assert_eq!(db.get_message_by_id(&id).await.unwrap(), Some(record));
    assert!(
        db.list_sessions(None, 50)
            .await
            .unwrap()
            .sessions
            .is_empty()
    );
    let sid = session.clone();
    let secret = marker.clone();
    assert!(
        db.with_writer(move |conn| {
            conn.execute(
                "UPDATE messages SET content_json=?1 WHERE session_id=?2",
                rusqlite::params![secret, sid],
            )?;
            Ok(())
        })
        .await
        .is_err(),
        "unadapted SQL writers cannot spill raw bodies"
    );
    for file in std::fs::read_dir(&path).unwrap() {
        let file = file.unwrap().path();
        if file.is_file() {
            let bytes = std::fs::read(&file).unwrap();
            for needle in [&marker, &hash] {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|part| part == needle.as_bytes()),
                    "body or body hash leaked into {}",
                    file.display()
                );
            }
        }
    }
    let reopened = Db::open(&database).unwrap();
    assert!(
        reopened.get_session(&session).await.is_err(),
        "restarting cannot recover temporary bodies"
    );
    drop(lease);
    assert_eq!(db.memory_content_store().retained_bytes(), 0);
    assert!(db.get_session(&session).await.is_err());
    assert!(
        db.append_message(&session, input("late body"))
            .await
            .is_err()
    );
    drop(reopened);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn persistent_policy_and_legitimate_marker_shaped_text_remain_unchanged() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap();
    let text = r#"{"$zkEphemeralContent":"this is ordinary user text"}"#;
    let record = db.append_message(&session.id, input(text)).await.unwrap();
    assert_eq!(
        db.get_session(&session.id).await.unwrap().unwrap().messages,
        [record]
    );
    let id = session.id;
    assert!(
        db.with_writer(move |conn| {
            conn.execute(
                "UPDATE sessions SET content_retention='ephemeral' WHERE id=?1",
                [id],
            )?;
            Ok(())
        })
        .await
        .is_err(),
        "retention cannot change after creation"
    );
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Exercise one real content-scope lifetime, immutable records and DB/WAL privacy scan together"
)]
async fn temporary_run_result_inbox_and_events_keep_only_metadata_and_fees_on_disk() {
    let path = std::env::temp_dir().join(format!("zk-content-run-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    let db = Db::open(path.join("run.sqlite")).unwrap();
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
        &zk_db::TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(1_000_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    let secret = format!("private-result-command-{}", uuid::Uuid::new_v4());
    let digest = format!("{:x}", Sha256::digest(secret.as_bytes()));
    let inbox = db
        .enqueue_task_message(&session, &run, None, &secret)
        .await
        .unwrap();
    assert_eq!(inbox.content, secret);
    assert_eq!(
        db.read_task_inbox(&run, &[], 10).await.unwrap()[0].content,
        secret
    );
    db.ensure_task_final_assistant(&run, &run, &secret)
        .await
        .unwrap();
    let task = db.find_runtime_task_by_id(&run).await.unwrap().unwrap();
    let committed = db
        .commit_task_result_with_run_usage_fallback(
            &zk_db::CommitTaskResult {
                task_id: run.clone(),
                run_id: run.clone(),
                expected_task_version: task.version,
                status: zk_db::ResultStatus::Complete,
                content: secret.clone(),
                media_type: "text/plain".into(),
                error_code: None,
                cleanup_status: zk_db::CleanupStatus::NotRequired,
                verification_status: zk_db::VerificationStatus::NotRequested,
            },
            zk_db::RunUsageFallback {
                input_tokens: 20,
                output_tokens: 7,
                cost_nanos_usd: 1234,
                usage_complete: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        committed,
        zk_db::CommitTaskResultOutcome::Committed { .. }
    ));
    let result = db
        .read_task_result(&run, None, 0, 65536)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.content, secret);
    assert_eq!(result.result.content_sha256, digest);
    assert!(
        db.get_run_events(&run, -1, 100)
            .await
            .unwrap()
            .iter()
            .any(|event| event.event_data.contains(&digest))
    );
    let persisted = db.find_run_by_id(&run).await.unwrap().unwrap();
    assert_eq!(persisted.cost_nanos_usd, 1234);
    let audit=db.with_reader(|conn|Ok((conn.query_row("SELECT content_sha256 IS NULL AND inline_text IS NULL AND blob_sha256 IS NULL FROM task_results",[],|r|r.get::<_,bool>(0))?,
        conn.query_row("SELECT COUNT(*) FROM task_result_blobs",[],|r|r.get::<_,i64>(0))?))).await.unwrap();
    assert_eq!(audit, (true, 0));
    for file in std::fs::read_dir(&path).unwrap() {
        let bytes = std::fs::read(file.unwrap().path()).unwrap();
        for needle in [&secret, &digest] {
            assert!(
                !bytes
                    .windows(needle.len())
                    .any(|part| part == needle.as_bytes())
            );
        }
    }
    drop(lease);
    assert!(db.read_task_result(&run, None, 0, 65536).await.is_err());
    let audit = db.find_run_by_id(&run).await.unwrap().unwrap();
    assert_eq!(audit.cost_nanos_usd, 1234);
    assert_eq!(audit.input_tokens, 20);
    assert_eq!(audit.status, "completed");
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn expired_content_cannot_block_cancellation_restart_or_cost_audit() {
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
        &zk_db::TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(1_000_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    let task = db.find_runtime_task_by_id(&run).await.unwrap().unwrap();
    drop(lease);
    let shutdown = db.request_runtime_shutdown().await.unwrap();
    assert_eq!(shutdown.runs_requested, 1);
    let interrupted = db.reconcile_runtime_after_restart().await.unwrap();
    assert_eq!(interrupted.runs_interrupted, 1);
    assert_eq!(
        db.reconcile_runtime_after_restart()
            .await
            .unwrap()
            .runs_interrupted,
        0
    );
    let run = db.find_run_by_id(&run).await.unwrap().unwrap();
    assert_eq!(run.status, "interrupted");
    assert_eq!(run.exit_reason.as_deref(), Some("serviceRestart"));
    assert_eq!(
        run.error_summary.as_deref(),
        Some("EPHEMERAL_CONTENT_UNAVAILABLE")
    );
    assert_eq!(run.cost_nanos_usd, 0);
    assert!(
        db.get_run_events(&run.id, -1, 100).await.is_err(),
        "expired events cannot become resumable history"
    );
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
        &zk_db::TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(1_000_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
        },
        1,
    )
    .await
    .unwrap();
    drop(lease);
    db.mark_task_run_needs_attention(
        &run,
        &run,
        task.version,
        "private cleanup diagnostic",
        zk_db::CleanupStatus::Unconfirmed,
    )
    .await
    .unwrap();
    let run = db.find_run_by_id(&run).await.unwrap().unwrap();
    assert_eq!(run.status, "interrupted");
    assert_eq!(run.cleanup_status, "unconfirmed");
    assert_eq!(
        run.error_summary.as_deref(),
        Some("EPHEMERAL_CONTENT_UNAVAILABLE")
    );
}

async fn produced_invocation(
    db: &Db,
    run: &str,
    tool: &str,
    effect: &str,
    input: &str,
) -> (String, String) {
    let invocation = uuid::Uuid::new_v4().to_string();
    let tool_use = uuid::Uuid::new_v4().to_string();
    db.create_tool_invocation(&zk_db::NewToolInvocation {
        invocation_id: invocation.clone(),
        task_id: run.into(),
        run_id: run.into(),
        tool_use_id: tool_use.clone(),
        tool_name: tool.into(),
        input_json: Some(input.into()),
        side_effect_class: effect.into(),
        directory_generation: Some(1),
        connection_generation: None,
    })
    .await
    .unwrap();
    assert_eq!(
        db.transition_tool_invocation_cas(
            &invocation,
            0,
            zk_db::ToolInvocationStatus::Succeeded,
            Some(input),
            Some("toolResult:test"),
            None,
            zk_db::CleanupStatus::NotRequired
        )
        .await
        .unwrap(),
        zk_db::CasOutcome::Applied
    );
    (invocation, tool_use)
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Exercise one real content-scope lifetime, immutable records and DB/WAL privacy scan together"
)]
async fn ephemeral_file_effects_and_research_keep_hashes_in_ram_and_preserve_authorized_files() {
    let path = std::env::temp_dir().join(format!("zk-private-artifact-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    let db = Db::open(path.join("runtime.sqlite")).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", path.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let run = uuid::Uuid::new_v4().to_string();
    db.start_run(&run, &session, None, Some("query"), "fixture")
        .await
        .unwrap();
    let secret = format!("private-write-search-content-{}", uuid::Uuid::new_v4());
    let digest = format!("{:x}", Sha256::digest(secret.as_bytes()));
    let input = serde_json::json!({"content":secret}).to_string();
    let (write, use_id) = produced_invocation(&db, &run, "Write", "write", &input).await;
    let file = path.join("authorized-output.txt");
    std::fs::write(&file, &secret).unwrap();
    let produced = zk_db::ProducedFileArtifactRecord {
        run_id: run.clone(),
        session_id: session.clone(),
        workspace_root: path.to_string_lossy().into_owned(),
        tool_use_id: use_id,
        producer_invocation_id: write,
        canonical_path: file.to_string_lossy().into_owned(),
        operation: "created".into(),
        sealed_hash: digest.clone(),
        file_size: i64::try_from(secret.len()).unwrap(),
    };
    let manifest = db.record_produced_file_artifact(&produced).await.unwrap();
    assert_eq!(
        manifest.entries[0].sealed_hash.as_deref(),
        Some(digest.as_str())
    );
    assert_eq!(
        db.record_produced_file_artifact(&produced).await.unwrap(),
        manifest,
        "exact retry remains idempotent"
    );
    assert_eq!(
        db.find_artifact_manifest_by_run(&run).await.unwrap(),
        Some(manifest)
    );
    let (search, _) = produced_invocation(&db, &run, "WebSearch", "read", &input).await;
    let capture = zk_db::ProducedResearchCapture {
        task_id: run.clone(),
        run_id: run.clone(),
        producer_invocation_id: search,
        kind: zk_db::ProducedResearchKind::WebSearch,
        query: Some(secret.clone()),
        fetched_at: zk_db::time::format_rfc3339_micros(zk_db::time::now_millis()),
        entries: vec![zk_db::ProducedResearchEntry {
            url: format!("https://example.invalid/{secret}"),
            title: Some(secret.clone()),
            provider: Some("fixture".into()),
            excerpt: Some(secret.clone()),
            rank: Some(1),
            http_status: None,
            content_type: None,
            truncated: false,
        }],
    };
    let receipt_hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&capture).unwrap())
    );
    db.record_research_capture(&capture).await.unwrap();
    db.record_research_capture(&capture).await.unwrap();
    let projection = db
        .find_research_projection_by_root_run(&run, &session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(projection.captures.len(), 1);
    assert_eq!(
        projection.captures[0].query.as_deref(),
        Some(secret.as_str())
    );
    assert_eq!(projection.findings[0].excerpt, secret);
    for entry in std::fs::read_dir(&path).unwrap() {
        let candidate = entry.unwrap().path();
        if candidate == file {
            continue;
        }
        let bytes = std::fs::read(&candidate).unwrap();
        for needle in [&secret, &digest, &receipt_hash] {
            assert!(
                !bytes
                    .windows(needle.len())
                    .any(|part| part == needle.as_bytes()),
                "temporary artifact/research content leaked into {}",
                candidate.display()
            );
        }
    }
    drop(lease);
    assert!(db.find_artifact_manifest_by_run(&run).await.is_err());
    assert!(
        db.find_research_projection_by_root_run(&run, &session)
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        secret,
        "authorized output is not a disposable conversation cache"
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
