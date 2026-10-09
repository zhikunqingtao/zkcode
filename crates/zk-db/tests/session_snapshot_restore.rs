//! Destructive transcript restore must retain facts still needed by execution.

use serde_json::{Value, json};
use zk_db::{
    CasOutcome, CleanupStatus, CommitTaskResult, CommitTaskResultOutcome,
    CommitToolInvocationResult, Db, EvidenceBundleRecord, EvidenceItemRecord, EvidenceOrigin,
    ExecutionResourceStatus, MessageRecord, MessageRole, NewExecutionResource, NewMessage,
    NewToolInvocation, ResultStatus, SnapshotRestoreOutcome, StoredBlock, ToolInvocationStatus,
    VerificationStatus,
};

struct Fixture {
    db: Db,
    session: String,
    task: String,
}

const RUN: &str = "snapshot-run";
const INVOCATION: &str = "snapshot-invocation";
const RESOURCE: &str = "snapshot-recording";
const OBSERVED: &str = "2026-10-08T00:00:00Z";

async fn fixture(recording: bool) -> Fixture {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("before", "/tmp").await.unwrap().id;
    db.start_run(RUN, &session, None, Some("query"), "before")
        .await
        .unwrap();
    let task = db.find_run_by_id(RUN).await.unwrap().unwrap().task_id;
    let tool_name = if recording { "VerifyJourney" } else { "Write" };
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: INVOCATION.into(),
        task_id: task.clone(),
        run_id: RUN.into(),
        tool_use_id: "snapshot-tool".into(),
        tool_name: tool_name.into(),
        input_json: Some("{}".into()),
        side_effect_class: "read".into(),
        directory_generation: None,
        connection_generation: None,
    })
    .await
    .unwrap();
    if recording {
        let identity = json!({"batch_id":uuid::Uuid::new_v4().to_string(),
            "session_id":session,"run_id":RUN,"invocation_id":INVOCATION});
        db.register_execution_resource(&NewExecutionResource {
            resource_id: RESOURCE.into(),
            task_id: task.clone(),
            run_id: RUN.into(),
            invocation_id: Some(INVOCATION.into()),
            resource_kind: "stream".into(),
            external_id: Some("fixture-browser".into()),
            metadata_json: json!({"kind":"browserSession","recordingFinalization":{
                "version":1,"phase":"reserved","identity":identity}})
            .to_string(),
        })
        .await
        .unwrap();
        db.finalize_execution_resource(RESOURCE, ExecutionResourceStatus::Released)
            .await
            .unwrap();
        db.seal_browser_recording(
            RESOURCE,
            json!({"identity":identity,"manifest_sha256":"a".repeat(64),"files":[]}),
            json!([]),
        )
        .await
        .unwrap();
    }
    db.commit_tool_invocation_result(&CommitToolInvocationResult {
        invocation_id: INVOCATION.into(),
        expected_version: 0,
        session_id: session.clone(),
        target: ToolInvocationStatus::Succeeded,
        input_json: Some("{}".into()),
        content: "original tool result".into(),
        is_error: false,
        metadata: None,
        output_sha256: None,
        error_code: None,
        cleanup_status: CleanupStatus::Confirmed,
        postprocessing: Some(json!({"schemaVersion":1,"toolName":tool_name,
            "requiredKinds":[if recording {"evidence"} else {"artifact"}]})),
    })
    .await
    .unwrap();
    Fixture { db, session, task }
}

async fn restore(
    db: &Db,
    session: &str,
    messages: Vec<MessageRecord>,
) -> Result<SnapshotRestoreOutcome, zk_db::DbError> {
    db.restore_session_snapshot(
        session,
        "/tmp",
        "restored",
        "active",
        Some("restored title"),
        messages,
    )
    .await
}

async fn facts(f: &Fixture) -> Value {
    let detail = f.db.get_session(&f.session).await.unwrap().unwrap();
    let ledger = f.db.with_reader(|conn| {
        let journals: String = conn.query_row(
            "SELECT json_group_array(json_object('invocation',invocation_id,'message',result_message_id,'payload',payload_json,'status',status,'version',version)) FROM tool_result_postprocessing",
            [], |row| row.get(0))?;
        let resources: String = conn.query_row(
            "SELECT json_group_array(json_object('id',resource_id,'status',status,'metadata',metadata_json,'version',version)) FROM execution_resources",
            [], |row| row.get(0))?;
        Ok(json!({"journals":journals,"resources":resources}))
    }).await.unwrap();
    json!({"session":detail,"ledger":ledger})
}

async fn finish(f: &Fixture) {
    assert_eq!(
        f.db.complete_tool_result_postprocessing_cas(INVOCATION, 0)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    // The verifier succeeded, but a later Run failure has no final assistant.
    // Keep this fixture distinct from a Complete result's final-message reference;
    // these tests isolate admission and recording-finalization constraints.
    let task =
        f.db.find_runtime_task_by_id(&f.task)
            .await
            .unwrap()
            .unwrap();
    assert!(matches!(
        f.db.commit_task_result(&CommitTaskResult {
            task_id: f.task.clone(),
            run_id: RUN.into(),
            expected_task_version: task.version,
            status: ResultStatus::Error,
            content: "fixture later run failure".into(),
            media_type: "text/plain".into(),
            error_code: Some("FIXTURE_RUN_FAILED".into()),
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        })
        .await
        .unwrap(),
        CommitTaskResultOutcome::Committed { .. }
    ));
    f.db.ensure_session_idle(&f.session).await.unwrap();
}

async fn save_recording_evidence(f: &Fixture) {
    f.db.save_evidence_bundle(&EvidenceBundleRecord {
        bundle_id: "snapshot-evidence".into(),
        session_id: f.session.clone(),
        agent_id: None,
        kind: "browser_journey".into(),
        claim: None,
        origin: EvidenceOrigin::Machine,
        producer_invocation_id: Some(INVOCATION.into()),
        verdict: "verified".into(),
        created_at: OBSERVED.into(),
        run_id: Some(RUN.into()),
        items: vec![EvidenceItemRecord {
            id: "snapshot-evidence-item".into(),
            producer_invocation_id: Some(INVOCATION.into()),
            item_type: "recording_manifest".into(),
            summary: None,
            blob_sha256: None,
            meta: Some(json!({"recording_manifest_sha256":"a".repeat(64),
                "recording_dispositions":[]})),
            sort_order: 0,
        }],
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn running_and_needs_attention_restore_leave_messages_and_journal_unchanged() {
    for needs_attention in [false, true] {
        let f = fixture(true).await;
        if needs_attention {
            let task =
                f.db.find_runtime_task_by_id(&f.task)
                    .await
                    .unwrap()
                    .unwrap();
            f.db.mark_task_run_needs_attention(
                &f.task,
                RUN,
                task.version,
                "fixture storage outage",
                CleanupStatus::Confirmed,
            )
            .await
            .unwrap();
        }
        let before = facts(&f).await;
        let error = restore(&f.db, &f.session, Vec::new()).await.unwrap_err();
        assert!(error.to_string().contains("active tasks"), "{error}");
        assert_eq!(facts(&f).await, before);
    }
}

#[tokio::test]
async fn idle_pending_postprocessing_is_protected_with_or_without_recording() {
    for recording in [false, true] {
        let f = fixture(recording).await;
        finish(&f).await;
        // Isolate this guard from the idle guard: public terminal commit normally
        // requires completion, so reopen only this fixture's journal via SQL.
        // All production FK/CHECK constraints remain enabled.
        f.db.with_writer(|conn| {
            conn.execute("UPDATE tool_result_postprocessing SET status='pending',completed_at=NULL,version=version+1", [])?;
            Ok(())
        }).await.unwrap();
        f.db.ensure_session_idle(&f.session).await.unwrap();
        let before = facts(&f).await;
        let error = restore(&f.db, &f.session, Vec::new()).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("SESSION_SNAPSHOT_MESSAGE_DEPENDENCY_PENDING"),
            "{error}"
        );
        assert_eq!(facts(&f).await, before);
    }
}

#[tokio::test]
async fn idle_sealed_success_keeps_completed_journal_even_when_restoring_same_messages() {
    let f = fixture(true).await;
    save_recording_evidence(&f).await;
    finish(&f).await;
    let before = facts(&f).await;
    let messages =
        f.db.get_session(&f.session)
            .await
            .unwrap()
            .unwrap()
            .messages;
    let error = restore(&f.db, &f.session, messages).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("SESSION_SNAPSHOT_MESSAGE_DEPENDENCY_PENDING"),
        "{error}"
    );
    assert_eq!(facts(&f).await, before);
}

#[tokio::test]
async fn ack_eligible_recording_can_restore_same_history_and_finish_ack_with_journal() {
    let f = fixture(true).await;
    save_recording_evidence(&f).await;
    finish(&f).await;
    let entry =
        f.db.browser_recording_finalization(RESOURCE)
            .await
            .unwrap()
            .unwrap();
    assert!(
        f.db.advance_browser_recording(RESOURCE, entry.version, false)
            .await
            .unwrap()
    );
    let messages =
        f.db.get_session(&f.session)
            .await
            .unwrap()
            .unwrap()
            .messages;
    let before = facts(&f).await["ledger"].clone();
    assert_eq!(
        restore(&f.db, &f.session, messages).await.unwrap(),
        SnapshotRestoreOutcome::Applied
    );
    let journal_count: i64 =
        f.db.with_reader(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM tool_result_postprocessing",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(journal_count, 1);
    assert_eq!(facts(&f).await["ledger"], before);
    assert!(
        f.db.advance_browser_recording(RESOURCE, entry.version + 1, true)
            .await
            .unwrap()
    );
    assert_eq!(
        f.db.browser_recording_finalization(RESOURCE)
            .await
            .unwrap()
            .unwrap()
            .state["phase"],
        "acknowledged"
    );
}

#[tokio::test]
async fn descendant_sealed_recording_does_not_block_replacing_only_parent_messages() {
    let f = fixture(true).await;
    save_recording_evidence(&f).await;
    finish(&f).await;
    let parent = f.db.create_session("parent", "/tmp").await.unwrap().id;
    for text in ["saved parent", "unsaved parent"] {
        f.db.append_message(
            &parent,
            NewMessage {
                meta: None,
                role: MessageRole::User,
                content: vec![StoredBlock::Text { text: text.into() }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
    }
    let mut parent_snapshot = f.db.get_session(&parent).await.unwrap().unwrap().messages;
    parent_snapshot.truncate(1);
    let (child_session, child_task, parent_id) =
        (f.session.clone(), f.task.clone(), parent.clone());
    // Reparent this completed fixture into an internal transcript; all identities,
    // content and its completed journal remain owned by that transcript.
    f.db.with_writer(move |conn| {
        conn.execute("UPDATE tasks SET session_id=?1 WHERE id=?2", [&parent_id, &child_task])?;
        conn.execute("UPDATE sessions SET kind='internal',parent_session_id=?1,parent_task_id=?2 WHERE id=?3", [&parent_id, &child_task, &child_session])?;
        Ok(())
    }).await.unwrap();
    f.db.ensure_session_idle(&parent).await.unwrap();
    let before = facts(&f).await;
    assert_eq!(
        restore(&f.db, &parent, parent_snapshot.clone())
            .await
            .unwrap(),
        SnapshotRestoreOutcome::Applied
    );
    assert_eq!(
        f.db.get_session(&parent).await.unwrap().unwrap().messages,
        parent_snapshot
    );
    assert_eq!(facts(&f).await, before);
    let entry =
        f.db.browser_recording_finalization(RESOURCE)
            .await
            .unwrap()
            .unwrap();
    assert!(
        f.db.advance_browser_recording(RESOURCE, entry.version, false)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn idle_restore_still_replaces_unsaved_history_and_is_idempotent() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("before", "/tmp").await.unwrap().id;
    for text in ["saved", "unsaved"] {
        db.append_message(
            &session,
            NewMessage {
                meta: None,
                role: MessageRole::User,
                content: vec![StoredBlock::Text { text: text.into() }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
    }
    let mut messages = db.get_session(&session).await.unwrap().unwrap().messages;
    messages.truncate(1);
    for _ in 0..2 {
        assert_eq!(
            restore(&db, &session, messages.clone()).await.unwrap(),
            SnapshotRestoreOutcome::Applied
        );
    }
    let restored = db.get_session(&session).await.unwrap().unwrap();
    assert_eq!(restored.model, "restored");
    assert_eq!(
        serde_json::to_value(restored.messages).unwrap(),
        serde_json::to_value(messages).unwrap()
    );
}

#[tokio::test]
async fn active_merge_reservation_blocks_even_empty_snapshot_restore() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("before", "/tmp").await.unwrap().id;
    let owner = session.clone();
    db.with_writer(move |conn| {
        conn.execute("INSERT INTO session_merges(id,idempotency_key,request_json,target_session_id,status,stage,created_at,updated_at) VALUES('snapshot-merge','snapshot-key','{}','merge-target','preparing','snapshotting','now','now')", [])?;
        conn.execute("INSERT INTO session_merge_locks(session_id,operation_id) VALUES(?1,'snapshot-merge')", [owner])?;
        Ok(())
    }).await.unwrap();
    let before = db.get_session(&session).await.unwrap().unwrap();
    let error = restore(&db, &session, Vec::new()).await.unwrap_err();
    assert!(
        error.to_string().contains("reserved by an active merge"),
        "{error}"
    );
    assert_eq!(
        serde_json::to_value(db.get_session(&session).await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    // A historical merge record is not a reservation after its lock is released.
    db.with_writer(|conn| {
        conn.execute(
            "DELETE FROM session_merge_locks WHERE operation_id='snapshot-merge'",
            [],
        )?;
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM session_merges", [], |row| {
                row.get::<_, i64>(0)
            })?,
            1
        );
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        restore(&db, &session, Vec::new()).await.unwrap(),
        SnapshotRestoreOutcome::Applied
    );
}
