//! Snapshot restoration preserves durable execution facts and cumulative usage.

use serde_json::{Value, json};
use zk_db::{
    CasOutcome, CleanupStatus, CommitTaskResult, CommitTaskResultOutcome,
    CommitToolInvocationResult, CommitToolInvocationResultOutcome, CreateTaskWithRun, Db,
    ExternalToolAdmission, MessageAttribution, MessageRecord, MessageRole, NewMessage,
    NewToolInvocation, ResultStatus, RunUsageFallback, SnapshotRestoreOutcome, StoredBlock,
    ToolInvocationStatus, VerificationStatus, WorkbenchBindingRecord,
};

const WORKSPACE: &str = "/snapshot-test-workspace";

fn message(role: MessageRole, text: &str) -> NewMessage {
    NewMessage {
        meta: None,
        role,
        content: vec![StoredBlock::Text { text: text.into() }],
        stop_reason: None,
        input_tokens: 0,
        output_tokens: 0,
    }
}

async fn database() -> (Db, String) {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("original", WORKSPACE).await.unwrap().id;
    (db, session)
}

async fn messages(db: &Db, session: &str) -> Vec<MessageRecord> {
    db.get_session(session).await.unwrap().unwrap().messages
}

async fn restore(db: &Db, session: &str, snapshot: Vec<MessageRecord>) -> SnapshotRestoreOutcome {
    db.restore_session_snapshot(
        session,
        WORKSPACE,
        "restored",
        "active",
        Some("restored title"),
        snapshot,
    )
    .await
    .unwrap()
}

// Exact SQL values, including raw JSON, timestamps, attribution and all dependent
// tables. Successful restore may change only these four session display columns.
async fn database_facts(db: &Db, allow_display_change: bool) -> Value {
    db.with_reader(move |conn| {
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )?;
        let tables = stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut dump = serde_json::Map::new();
        for table in tables {
            let mut stmt = conn.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
            let columns: Vec<_> = stmt.column_names().iter().map(ToString::to_string).collect();
            let mut rows = stmt.query_map([], |row| {
                columns.iter().enumerate().map(|(index, column)| {
                    if allow_display_change && table == "sessions"
                        && matches!(column.as_str(), "title" | "model" | "status" | "updated_at") {
                        Ok(format!("{column}:<display>"))
                    } else {
                        Ok(format!("{column}:{:?}", row.get_ref(index)?))
                    }
                }).collect::<rusqlite::Result<Vec<String>>>()
            })?.collect::<rusqlite::Result<Vec<_>>>()?;
            rows.sort();
            dump.insert(table, json!(rows));
        }
        Ok(Value::Object(dump))
    }).await.unwrap()
}

async fn assert_healthy(db: &Db) {
    db.with_reader(|conn| {
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get::<_, i64>(0)
            })?,
            0
        );
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
            "ok"
        );
        Ok(())
    })
    .await
    .unwrap();
}

async fn assert_rejected(
    db: &Db,
    session: &str,
    snapshot: Vec<MessageRecord>,
    expected: SnapshotRestoreOutcome,
) {
    let before = database_facts(db, false).await;
    assert_eq!(restore(db, session, snapshot).await, expected);
    assert_eq!(
        database_facts(db, false).await,
        before,
        "conflict must leave every table unchanged"
    );
    assert_healthy(db).await;
}

async fn assert_same_history_preserved(db: &Db, session: &str) {
    // Exercise the actual MessageRecord file-format round trip, including nulls.
    let saved = serde_json::to_vec(&messages(db, session).await).unwrap();
    let before = database_facts(db, true).await;
    for _ in 0..2 {
        let snapshot = serde_json::from_slice(&saved).unwrap();
        assert_eq!(
            restore(db, session, snapshot).await,
            SnapshotRestoreOutcome::Applied
        );
        assert_eq!(database_facts(db, true).await, before);
    }
    assert_healthy(db).await;
}

async fn finish_task(db: &Db, task: &str, run: &str, status: ResultStatus) {
    let final_message = if status == ResultStatus::Complete {
        Some(
            db.ensure_task_final_assistant(task, run, "completed answer")
                .await
                .unwrap(),
        )
    } else {
        None
    };
    let version = db
        .find_runtime_task_by_id(task)
        .await
        .unwrap()
        .unwrap()
        .version;
    assert!(matches!(
        db.commit_task_result_with_run_usage_fallback(
            &CommitTaskResult {
                task_id: task.into(),
                run_id: run.into(),
                expected_task_version: version,
                status,
                content: "completed answer".into(),
                media_type: "text/plain".into(),
                error_code: (status != ResultStatus::Complete).then(|| "FIXTURE_TERMINAL".into()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            },
            RunUsageFallback {
                input_tokens: 23,
                output_tokens: 7,
                cache_read_tokens: 5,
                cache_create_tokens: 3,
                cost_nanos_usd: 2_500_000,
                usage_complete: true,
            },
        )
        .await
        .unwrap(),
        CommitTaskResultOutcome::Committed { .. }
    ));
    let result = db
        .read_task_result(task, None, 0, 4096)
        .await
        .unwrap()
        .unwrap()
        .result;
    assert_eq!(result.status, status);
    assert_eq!(result.final_message_id, final_message);
}

async fn terminal_turn(db: &Db, session: &str, run: &str, status: ResultStatus) -> String {
    db.start_run(run, session, None, Some("query"), "original")
        .await
        .unwrap();
    let task = db.find_run_by_id(run).await.unwrap().unwrap().task_id;
    db.append_attributed_message(
        session,
        message(MessageRole::System, "runtime diagnostic"),
        MessageAttribution {
            task_id: Some(task.clone()),
            run_id: Some(run.into()),
            origin: "runtime".into(),
            source_task_id: None,
        },
    )
    .await
    .unwrap();
    finish_task(db, &task, run, status).await;
    db.ensure_session_idle(session).await.unwrap();
    task
}

async fn usage(db: &Db, session: &str) -> (i64, i64, i64, i64, f64) {
    let session = session.to_owned();
    db.with_reader(move |conn| {
        Ok(conn.query_row(
            "SELECT total_input_tokens,total_output_tokens,total_cache_read,total_cache_create,total_cost_usd FROM sessions WHERE id=?1",
            [session], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
        )?)
    }).await.unwrap()
}

#[tokio::test]
async fn complete_failed_and_cancelled_same_snapshot_preserve_all_runtime_facts_and_usage() {
    for status in [
        ResultStatus::Complete,
        ResultStatus::Error,
        ResultStatus::Cancelled,
    ] {
        let (db, session) = database().await;
        terminal_turn(&db, &session, "terminal-run", status).await;
        assert_eq!(usage(&db, &session).await, (23, 7, 5, 3, 0.0025));
        assert_same_history_preserved(&db, &session).await;
        assert_eq!(usage(&db, &session).await, (23, 7, 5, 3, 0.0025));
    }
}

#[tokio::test]
async fn old_snapshot_cannot_remove_a_completed_run_or_its_later_unbound_message() {
    let (db, session) = database().await;
    terminal_turn(&db, &session, "first", ResultStatus::Complete).await;
    let saved = messages(&db, &session).await;
    terminal_turn(&db, &session, "second", ResultStatus::Complete).await;
    assert_rejected(
        &db,
        &session,
        saved,
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
    let saved = messages(&db, &session).await;
    // Attribution is not a read watermark: execution already loaded this transcript.
    db.append_message(
        &session,
        message(MessageRole::User, "later ordinary message"),
    )
    .await
    .unwrap();
    assert_rejected(
        &db,
        &session,
        saved,
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
    assert_eq!(usage(&db, &session).await, (46, 14, 10, 6, 0.005));
}

#[tokio::test]
async fn an_idle_run_without_any_attributed_messages_still_prevents_history_truncation() {
    let (db, session) = database().await;
    db.append_message(
        &session,
        message(MessageRole::User, "prompt read by the run"),
    )
    .await
    .unwrap();
    db.start_run("empty-run", &session, None, Some("query"), "original")
        .await
        .unwrap();
    let task = db
        .find_run_by_id("empty-run")
        .await
        .unwrap()
        .unwrap()
        .task_id;
    finish_task(&db, &task, "empty-run", ResultStatus::Error).await;
    db.ensure_session_idle(&session).await.unwrap();
    assert_rejected(
        &db,
        &session,
        vec![],
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
    assert_same_history_preserved(&db, &session).await;
}

#[tokio::test]
async fn ordinary_history_can_only_truncate_an_unchanged_prefix_and_keeps_usage() {
    let (db, session) = database().await;
    for text in ["first", "second", "third"] {
        db.append_message(&session, message(MessageRole::User, text))
            .await
            .unwrap();
    }
    db.add_session_usage(
        &session,
        &zk_protocol::Usage {
            input_tokens: 13,
            output_tokens: 11,
            cache_read_input_tokens: 7,
            cache_creation_input_tokens: 5,
        },
        0.75,
    )
    .await
    .unwrap();
    let original = messages(&db, &session).await;
    let before = database_facts(&db, true).await;
    let prefix = original[..2].to_vec();
    assert_eq!(
        restore(&db, &session, prefix.clone()).await,
        SnapshotRestoreOutcome::Applied
    );
    assert_eq!(messages(&db, &session).await, prefix);
    assert_eq!(usage(&db, &session).await, (13, 11, 7, 5, 0.75));
    let after = database_facts(&db, true).await;
    assert_eq!(before["messages"].as_array().unwrap().len(), 3);
    for row in after["messages"].as_array().unwrap() {
        assert!(
            before["messages"].as_array().unwrap().contains(row),
            "retained raw rows must not be rewritten"
        );
    }
    assert_rejected(
        &db,
        &session,
        original,
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
    assert_eq!(
        restore(&db, &session, vec![]).await,
        SnapshotRestoreOutcome::Applied
    );
    assert!(messages(&db, &session).await.is_empty());
    assert_eq!(usage(&db, &session).await, (13, 11, 7, 5, 0.75));
    assert_healthy(&db).await;
}

#[tokio::test]
async fn modified_missing_inserted_and_reordered_snapshot_messages_are_rejected_atomically() {
    let (db, session) = database().await;
    for text in ["first", "second"] {
        db.append_message(&session, message(MessageRole::User, text))
            .await
            .unwrap();
    }
    let original = messages(&db, &session).await;
    let mut changed = original.clone();
    changed[0].content = vec![StoredBlock::Text {
        text: "changed".into(),
    }];
    let mut inserted = original.clone();
    inserted[0].id = uuid::Uuid::new_v4().to_string();
    let mut reversed = original.clone();
    reversed.reverse();
    // Still structurally valid, but the identities now claim another history order.
    reversed[0].seq_num = original[0].seq_num;
    reversed[1].seq_num = original[1].seq_num;
    let mut metadata = original.clone();
    metadata[0].meta = Some(json!({"changed": true}));
    let mut timestamp = original.clone();
    timestamp[0].created_at += 1;
    let mut tokens = original.clone();
    tokens[0].input_tokens = 1;
    for snapshot in [
        changed,
        inserted,
        reversed,
        metadata,
        timestamp,
        tokens,
        original[1..].to_vec(),
    ] {
        assert_rejected(
            &db,
            &session,
            snapshot,
            SnapshotRestoreOutcome::HistoryConflict,
        )
        .await;
    }
}

#[tokio::test]
async fn duplicate_identity_sequence_and_foreign_session_are_invalid_without_writes() {
    let (db, session) = database().await;
    for text in ["first", "second"] {
        db.append_message(&session, message(MessageRole::User, text))
            .await
            .unwrap();
    }
    let original = messages(&db, &session).await;
    let mut duplicate_id = original.clone();
    duplicate_id[1].id = duplicate_id[0].id.clone();
    let mut duplicate_seq = original.clone();
    duplicate_seq[1].seq_num = duplicate_seq[0].seq_num;
    let mut descending = original.clone();
    descending.reverse();
    let mut foreign = original;
    foreign[0].session_id = "another-session".into();
    // Descending sequence numbers are malformed input, unlike a valid sequence
    // that assigns another ordering to the original identities above.
    for snapshot in [duplicate_id, duplicate_seq, descending, foreign] {
        assert_rejected(
            &db,
            &session,
            snapshot,
            SnapshotRestoreOutcome::InvalidMessages,
        )
        .await;
    }
}

#[tokio::test]
async fn normal_null_metadata_and_equivalent_default_and_timestamp_encodings_keep_raw_rows() {
    let (db, session) = database().await;
    let direct = db
        .append_message(
            &session,
            NewMessage {
                meta: Some(Value::Null),
                role: MessageRole::User,
                content: vec![StoredBlock::ToolResult {
                    tool_use_id: "historical-tool".into(),
                    content: "original result".into(),
                    is_error: false,
                    metadata: Some(Value::Null),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
    let before = database_facts(&db, true).await;
    assert_eq!(
        restore(&db, &session, vec![direct]).await,
        SnapshotRestoreOutcome::Applied
    );
    assert_eq!(database_facts(&db, true).await, before);
    // The public API permits Some(JSON null); saving a snapshot turns it into None.
    assert_same_history_preserved(&db, &session).await;
    db.with_writer(|conn| {
        conn.execute(
            "UPDATE messages SET content_json='[{\"content\":\"original result\",\"is_error\":false,\"metadata\":null,\"tool_use_id\":\"historical-tool\",\"type\":\"tool_result\"}]',created_at='2026-10-09T08:00:00.123456+08:00'",
            [],
        )?;
        Ok(())
    }).await.unwrap();
    assert_same_history_preserved(&db, &session).await;
}

#[tokio::test]
async fn unreadable_current_messages_are_not_authenticated_by_tolerant_projection() {
    for (content, timestamp, role, metadata) in [
        ("not-json", "2026-10-09T00:00:00Z", "user", None),
        (
            r#"[{"type":"text","text":"kept"},{"type":"unknown"}]"#,
            "2026-10-09T00:00:00Z",
            "user",
            None,
        ),
        (r#"[{"type":"text"}]"#, "2026-10-09T00:00:00Z", "user", None),
        (
            r#"[{"type":"text","text":"kept"}]"#,
            "invalid-time",
            "user",
            None,
        ),
        (
            r#"[{"type":"text","text":"kept"}]"#,
            "2026-10-09T00:00:00Z",
            "unknown-role",
            None,
        ),
        (
            r#"[{"type":"text","text":"kept"}]"#,
            "2026-10-09T00:00:00Z",
            "user",
            Some("not-json"),
        ),
    ] {
        let (db, session) = database().await;
        db.append_message(&session, message(MessageRole::User, "kept"))
            .await
            .unwrap();
        let saved = messages(&db, &session).await;
        db.with_writer(move |conn| {
            conn.execute(
                "UPDATE messages SET content_json=?1,created_at=?2,role=?3,metadata_json=?4",
                rusqlite::params![content, timestamp, role, metadata],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        // The display loader tolerates bad roles/blocks/timestamps, but already
        // rejects malformed metadata. In that case use the pre-corruption save.
        let lossy = if metadata.is_some() {
            assert!(db.get_session(&session).await.is_err());
            saved
        } else {
            messages(&db, &session).await
        };
        assert_rejected(
            &db,
            &session,
            lossy,
            SnapshotRestoreOutcome::HistoryConflict,
        )
        .await;
    }
}

fn task_request(session: &str, task_type: &str) -> CreateTaskWithRun {
    CreateTaskWithRun {
        task_id: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        root_session_id: session.into(),
        transcript_session_id: session.into(),
        parent_task_id: None,
        parent_run_id: None,
        creator_tool_use_id: None,
        ordinal: 0,
        description: "snapshot dependency fixture".into(),
        prompt: None,
        task_type: task_type.into(),
        model: "original".into(),
        working_dir: WORKSPACE.into(),
        execution_config_json: json!({"executor":"localMcp","budget":{
            "tokenLimit":1_000_000,"costLimitNanosUsd":1_000_000_000,
            "deadlineAtMs":zk_db::time::now_millis()+60_000}})
        .to_string(),
        startup_epoch: 0,
    }
}

async fn claim(db: &Db, task: &str, run: &str) {
    let task_record = db.find_runtime_task_by_id(task).await.unwrap().unwrap();
    assert_eq!(
        db.claim_task_run_cas(task, run, task_record.version)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
}

async fn terminal_external_tool(db: &Db, session: &str, task: &str, run: &str) -> String {
    let input = json!({"path":"fixture.txt","content":"fixture"});
    let ExternalToolAdmission::Accepted { tool_use_id } = db
        .admit_external_tool_operation(session, run, "snapshot-operation", "Write", &input)
        .await
        .unwrap()
    else {
        panic!("first external admission must own execution")
    };
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: "snapshot-external-invocation".into(),
        task_id: task.into(),
        run_id: run.into(),
        tool_use_id,
        tool_name: "Write".into(),
        input_json: Some(input.to_string()),
        side_effect_class: "write".into(),
        directory_generation: None,
        connection_generation: None,
    })
    .await
    .unwrap();
    let CommitToolInvocationResultOutcome::Committed(committed) = db
        .commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: "snapshot-external-invocation".into(),
            expected_version: 0,
            session_id: session.into(),
            target: ToolInvocationStatus::Succeeded,
            input_json: Some(input.to_string()),
            content: "immutable external result".into(),
            is_error: false,
            metadata: Some(Value::Null),
            output_sha256: None,
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            postprocessing: Some(json!({"schemaVersion":1,"toolName":"Write"})),
        })
        .await
        .unwrap()
    else {
        panic!("tool result must commit")
    };
    assert_eq!(
        db.complete_tool_result_postprocessing_cas("snapshot-external-invocation", 0)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    let facts = db
        .complete_external_tool_operation(session, run, "snapshot-operation", &committed.message.id)
        .await
        .unwrap();
    assert_eq!(facts.message_id, committed.message.id);
    committed.message.id
}

#[tokio::test]
async fn same_snapshot_preserves_final_result_journal_workbench_external_and_output_references() {
    let (db, session) = database().await;
    let request = task_request(&session, "mcp");
    db.create_task_with_run(&request).await.unwrap();
    claim(&db, &request.task_id, &request.run_id).await;
    let prompt = db
        .append_message(&session, message(MessageRole::User, "write request"))
        .await
        .unwrap();
    let tool_message =
        terminal_external_tool(&db, &session, &request.task_id, &request.run_id).await;
    db.save_workbench_binding(&WorkbenchBindingRecord {
        root_run_id: request.run_id.clone(),
        request_message_id: prompt.id,
        result_message_id: None,
        created_at: "2026-10-09T00:00:00Z".into(),
        updated_at: "2026-10-09T00:00:00Z".into(),
    })
    .await
    .unwrap();
    finish_task(
        &db,
        &request.task_id,
        &request.run_id,
        ResultStatus::Complete,
    )
    .await;
    let result = db
        .read_task_result(&request.task_id, None, 0, 4096)
        .await
        .unwrap()
        .unwrap()
        .result;
    assert!(
        db.bind_workbench_result(&request.run_id, result.final_message_id.as_deref().unwrap())
            .await
            .unwrap()
    );
    db.ensure_session_idle(&session).await.unwrap();
    db.insert_file_snapshot(
        &session,
        Some(&tool_message),
        "/fixture.txt",
        "before",
        "write",
    )
    .await
    .unwrap();
    assert_same_history_preserved(&db, &session).await;
    // Replay still resolves the original durable message after a successful restore.
    assert!(
        matches!(db.admit_external_tool_operation(&session, &request.run_id,
        "snapshot-operation", "Write", &json!({"path":"fixture.txt","content":"fixture"}))
        .await.unwrap(), ExternalToolAdmission::Completed { message_id, .. } if message_id == tool_message)
    );
    let before = database_facts(&db, false).await;
    for table in [
        "task_results",
        "tool_result_postprocessing",
        "run_workbench_bindings",
        "external_tool_requests",
        "file_snapshots",
    ] {
        assert_eq!(
            before[table].as_array().unwrap().len(),
            1,
            "fixture must exercise {table}"
        );
    }
    let expected_output_ref = format!("message:{tool_message}");
    db.with_reader(move |conn| {
        let output_ref: String = conn.query_row(
            "SELECT output_ref FROM tool_invocations WHERE invocation_id='snapshot-external-invocation'",
            [], |row| row.get(0),
        )?;
        assert_eq!(output_ref, expected_output_ref);
        Ok(())
    }).await.unwrap();
    assert_rejected(
        &db,
        &session,
        vec![],
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
}

#[tokio::test]
async fn same_snapshot_preserves_consumed_child_receipt_and_message_source_attribution() {
    let (db, session) = database().await;
    let mut root_request = task_request(&session, "agent");
    root_request.execution_config_json = json!({"budget":{
        "tokenLimit":1_000_000,"costLimitNanosUsd":1_000_000_000,
        "deadlineAtMs":zk_db::time::now_millis()+60_000}})
    .to_string();
    let root = db.create_task_with_run(&root_request).await.unwrap();
    claim(&db, &root.task.id, &root.run_id).await;
    let mut child_request = task_request(&session, "agent");
    child_request.transcript_session_id = uuid::Uuid::new_v4().to_string();
    child_request.parent_task_id = Some(root.task.id.clone());
    child_request.parent_run_id = Some(root.run_id.clone());
    child_request.creator_tool_use_id = Some(uuid::Uuid::new_v4().to_string());
    child_request.execution_config_json =
        r#"{"isolation":"readOnly","lifecycle":"attached"}"#.into();
    let child = db.create_task_with_run(&child_request).await.unwrap();
    claim(&db, &child.task.id, &child.run_id).await;
    finish_task(&db, &child.task.id, &child.run_id, ResultStatus::Complete).await;
    let receipt = db
        .ingest_task_result_at_safe_boundary(&root.task.id, &child.task.id, 1, "child completed")
        .await
        .unwrap()
        .expect("attached child receipt at parent safe boundary");
    finish_task(&db, &root.task.id, &root.run_id, ResultStatus::Complete).await;
    db.ensure_session_idle(&session).await.unwrap();
    let receipt_message = db
        .get_message_by_id(&receipt.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt_message.session_id, session);
    let receipt_id = receipt.message_id.clone();
    let child_task = child.task.id.clone();
    db.with_reader(move |conn| {
        assert_eq!(
            conn.query_row(
                "SELECT source_task_id FROM messages WHERE id=?1",
                [receipt_id],
                |row| row.get::<_, String>(0)
            )?,
            child_task
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM task_result_receipts", [], |row| row
                .get::<_, i64>(
                0
            ))?,
            1
        );
        Ok(())
    })
    .await
    .unwrap();
    assert_same_history_preserved(&db, &session).await;
    let repeated = db
        .ingest_task_result_at_safe_boundary(&root.task.id, &child.task.id, 1, "child completed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated.message_id, receipt.message_id);
    assert!(!repeated.created);
    assert_rejected(
        &db,
        &session,
        vec![],
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
}

#[tokio::test]
async fn file_snapshot_on_retained_prefix_allows_truncation_but_deleted_message_link_blocks_it() {
    let (db, session) = database().await;
    for text in ["saved", "unsaved"] {
        db.append_message(&session, message(MessageRole::User, text))
            .await
            .unwrap();
    }
    let original = messages(&db, &session).await;
    db.insert_file_snapshot(
        &session,
        Some(&original[0].id),
        "/kept.txt",
        "original bytes",
        "edit",
    )
    .await
    .unwrap();
    let files = db.list_file_snapshots(&session).await.unwrap();
    assert_eq!(
        restore(&db, &session, original[..1].to_vec()).await,
        SnapshotRestoreOutcome::Applied
    );
    assert_eq!(db.list_file_snapshots(&session).await.unwrap(), files);
    assert_eq!(messages(&db, &session).await, original[..1]);
    assert_rejected(
        &db,
        &session,
        vec![],
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
}

#[tokio::test]
async fn cross_session_file_reference_cannot_be_dangled_by_truncating_its_message() {
    let (db, session) = database().await;
    let linked = db
        .append_message(&session, message(MessageRole::User, "referenced message"))
        .await
        .unwrap();
    let other = db.create_session("other", WORKSPACE).await.unwrap().id;
    // file_snapshots.message_id is a logical link: its owning session alone
    // must not make a reference to the target's deleted message disappear.
    db.insert_file_snapshot(&other, Some(&linked.id), "/other.txt", "before", "edit")
        .await
        .unwrap();
    assert_rejected(
        &db,
        &session,
        vec![],
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
    assert_same_history_preserved(&db, &session).await;
}

#[tokio::test]
async fn unresolved_file_history_scope_blocks_truncation_but_not_identical_restore() {
    for unresolved in [None, Some("missing-message"), Some("foreign-message")] {
        let (db, session) = database().await;
        db.append_message(&session, message(MessageRole::User, "target"))
            .await
            .unwrap();
        let foreign = if unresolved == Some("foreign-message") {
            let other = db.create_session("other", WORKSPACE).await.unwrap().id;
            Some(
                db.append_message(&other, message(MessageRole::User, "foreign"))
                    .await
                    .unwrap()
                    .id,
            )
        } else {
            None
        };
        db.insert_file_snapshot(
            &session,
            foreign.as_deref().or(unresolved),
            "/unknown.txt",
            "before",
            "edit",
        )
        .await
        .unwrap();
        assert_rejected(
            &db,
            &session,
            vec![],
            SnapshotRestoreOutcome::HistoryConflict,
        )
        .await;
        assert_same_history_preserved(&db, &session).await;
    }
}

#[tokio::test]
async fn unrelated_session_runtime_and_file_history_do_not_block_ordinary_prefix() {
    let (db, session) = database().await;
    let other = db.create_session("other", WORKSPACE).await.unwrap().id;
    terminal_turn(&db, &other, "other-run", ResultStatus::Complete).await;
    db.insert_file_snapshot(&other, None, "/other.txt", "other bytes", "edit")
        .await
        .unwrap();
    for text in ["saved", "unsaved"] {
        db.append_message(&session, message(MessageRole::User, text))
            .await
            .unwrap();
    }
    let mut saved = messages(&db, &session).await;
    saved.truncate(1);
    let other_before = db.get_session(&other).await.unwrap().unwrap();
    let files = db.list_file_snapshots(&other).await.unwrap();
    assert_eq!(
        restore(&db, &session, saved.clone()).await,
        SnapshotRestoreOutcome::Applied
    );
    assert_eq!(messages(&db, &session).await, saved);
    assert_eq!(db.get_session(&other).await.unwrap().unwrap(), other_before);
    assert_eq!(db.list_file_snapshots(&other).await.unwrap(), files);
    assert_healthy(&db).await;
}

#[tokio::test]
async fn runtime_origin_without_task_ids_is_not_treated_as_ordinary_chat() {
    let (db, session) = database().await;
    db.append_attributed_message(
        &session,
        message(MessageRole::System, "unbound runtime fact"),
        MessageAttribution {
            task_id: None,
            run_id: None,
            origin: "runtime".into(),
            source_task_id: None,
        },
    )
    .await
    .unwrap();
    assert_rejected(
        &db,
        &session,
        vec![],
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
    assert_same_history_preserved(&db, &session).await;
}

#[tokio::test]
async fn foreign_workbench_request_or_result_reference_protects_target_without_its_own_run() {
    for target_is_result in [false, true] {
        let (db, session) = database().await;
        let target_message = db
            .append_message(&session, message(MessageRole::User, "linked history"))
            .await
            .unwrap();
        let other = db.create_session("other", WORKSPACE).await.unwrap().id;
        terminal_turn(&db, &other, "workbench-run", ResultStatus::Complete).await;
        let other_message = messages(&db, &other).await.remove(0).id;
        let (request_id, result_id) = if target_is_result {
            (other_message, target_message.id)
        } else {
            (target_message.id, other_message)
        };
        db.save_workbench_binding(&WorkbenchBindingRecord {
            root_run_id: "workbench-run".into(),
            request_message_id: request_id,
            result_message_id: Some(result_id),
            created_at: "2026-10-09T00:00:00Z".into(),
            updated_at: "2026-10-09T00:00:00Z".into(),
        })
        .await
        .unwrap();
        // The Run belongs to the other transcript. This independently exercises
        // the message dependency, rather than passing only because of Run scope.
        assert_rejected(
            &db,
            &session,
            vec![],
            SnapshotRestoreOutcome::HistoryConflict,
        )
        .await;
        assert_same_history_preserved(&db, &session).await;
    }
}

#[tokio::test]
async fn foreign_invocation_message_output_reference_protects_target_without_its_own_run() {
    for suffix in ["", "#sha256:fixture-digest"] {
        let (db, session) = database().await;
        let target_message = db
            .append_message(&session, message(MessageRole::User, "tool output"))
            .await
            .unwrap();
        let other = db.create_session("other", WORKSPACE).await.unwrap().id;
        db.start_run("output-ref-run", &other, None, Some("query"), "original")
            .await
            .unwrap();
        let task = db
            .find_run_by_id("output-ref-run")
            .await
            .unwrap()
            .unwrap()
            .task_id;
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: "output-ref-invocation".into(),
            task_id: task.clone(),
            run_id: "output-ref-run".into(),
            tool_use_id: "output-tool".into(),
            tool_name: "Read".into(),
            input_json: Some("{}".into()),
            side_effect_class: "read".into(),
            directory_generation: None,
            connection_generation: None,
        })
        .await
        .unwrap();
        assert_eq!(
            db.transition_tool_invocation_cas(
                "output-ref-invocation",
                0,
                ToolInvocationStatus::Running,
                Some("{}"),
                None,
                None,
                CleanupStatus::NotRequired
            )
            .await
            .unwrap(),
            CasOutcome::Applied
        );
        assert_eq!(
            db.transition_tool_invocation_cas(
                "output-ref-invocation",
                1,
                ToolInvocationStatus::Succeeded,
                Some("{}"),
                Some(&format!("message:{}{suffix}", target_message.id)),
                None,
                CleanupStatus::Confirmed
            )
            .await
            .unwrap(),
            CasOutcome::Applied
        );
        finish_task(&db, &task, "output-ref-run", ResultStatus::Error).await;
        assert_rejected(
            &db,
            &session,
            vec![],
            SnapshotRestoreOutcome::HistoryConflict,
        )
        .await;
        assert_same_history_preserved(&db, &session).await;
    }
}

#[tokio::test]
async fn terminal_task_with_missing_run_does_not_authorize_ordinary_history_truncation() {
    let (db, session) = database().await;
    db.append_message(&session, message(MessageRole::User, "unbound prompt"))
        .await
        .unwrap();
    db.start_run("removed-run", &session, None, Some("query"), "original")
        .await
        .unwrap();
    let task = db
        .find_run_by_id("removed-run")
        .await
        .unwrap()
        .unwrap()
        .task_id;
    finish_task(&db, &task, "removed-run", ResultStatus::Error).await;
    // Model a surviving terminal Task with no resolvable transcript read scope.
    // No constraints/triggers are disabled and no message attribution is forged.
    db.with_writer(|conn| {
        assert_eq!(
            conn.execute("DELETE FROM run_envelopes WHERE id='removed-run'", [])?,
            1
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get::<_, i64>(0))?,
            1
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM run_envelopes", [], |row| row
                .get::<_, i64>(0))?,
            0
        );
        Ok(())
    })
    .await
    .unwrap();
    db.ensure_session_idle(&session).await.unwrap();
    assert_rejected(
        &db,
        &session,
        vec![],
        SnapshotRestoreOutcome::HistoryConflict,
    )
    .await;
    assert_same_history_preserved(&db, &session).await;
}

#[tokio::test]
async fn internal_repl_run_owned_by_root_does_not_freeze_unread_root_transcript() {
    let (db, session) = database().await;
    for text in ["saved", "unsaved"] {
        db.append_message(&session, message(MessageRole::User, text))
            .await
            .unwrap();
    }
    let mut request = task_request(&session, "repl");
    request.transcript_session_id = uuid::Uuid::new_v4().to_string();
    let mut config: Value = serde_json::from_str(&request.execution_config_json).unwrap();
    config["executor"] = json!("localRepl");
    request.execution_config_json = config.to_string();
    let repl = db.create_task_with_run(&request).await.unwrap();
    claim(&db, &repl.task.id, &repl.run_id).await;
    finish_task(&db, &repl.task.id, &repl.run_id, ResultStatus::Error).await;
    db.ensure_session_idle(&session).await.unwrap();
    assert_eq!(repl.task.session_id, session);
    assert_ne!(repl.transcript_session_id, session);
    let internal = db
        .get_session(&repl.transcript_session_id)
        .await
        .unwrap()
        .unwrap();
    let before = database_facts(&db, true).await;
    let mut saved = messages(&db, &session).await;
    saved.truncate(1);
    assert_eq!(
        restore(&db, &session, saved.clone()).await,
        SnapshotRestoreOutcome::Applied
    );
    assert_eq!(messages(&db, &session).await, saved);
    assert_eq!(
        db.get_session(&repl.transcript_session_id)
            .await
            .unwrap()
            .unwrap(),
        internal
    );
    let after = database_facts(&db, true).await;
    for (table, facts) in before.as_object().unwrap() {
        if table != "messages" {
            assert_eq!(
                &after[table], facts,
                "unrelated {table} facts must be retained"
            );
        }
    }
    assert_healthy(&db).await;
}

#[tokio::test]
async fn sql_failure_after_legal_tail_delete_rolls_back_all_rows_and_keeps_the_tail() {
    let (db, session) = database().await;
    for text in ["saved", "unsaved"] {
        db.append_message(&session, message(MessageRole::User, text))
            .await
            .unwrap();
    }
    let original = messages(&db, &session).await;
    db.with_writer(|conn| {
        // The trigger fires only if the tail DELETE already happened inside
        // this transaction. It exists only in this private in-memory fixture.
        conn.execute_batch(
            "CREATE TEMP TRIGGER snapshot_fixture_fail_after_delete
             BEFORE UPDATE ON sessions
             WHEN (SELECT COUNT(*) FROM messages WHERE session_id=NEW.id)=1
             BEGIN SELECT RAISE(ABORT,'SNAPSHOT_FIXTURE_AFTER_DELETE'); END;",
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let before = database_facts(&db, false).await;
    let error = db
        .restore_session_snapshot(
            &session,
            WORKSPACE,
            "restored",
            "active",
            Some("restored title"),
            original[..1].to_vec(),
        )
        .await
        .unwrap_err();
    assert!(matches!(&error, zk_db::DbError::Sqlite(_)), "{error}");
    assert!(
        error.to_string().contains("SNAPSHOT_FIXTURE_AFTER_DELETE"),
        "{error}"
    );
    assert_eq!(database_facts(&db, false).await, before);
    assert_eq!(messages(&db, &session).await, original);
    assert_healthy(&db).await;
}
