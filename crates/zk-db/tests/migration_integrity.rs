//! New-version invariants; no historical database import is required.
use sha2::{Digest, Sha256};
use zk_db::{
    Db, DbError, MemoryTarget, MemoryUpsert, MessageRole, NewMessage, SessionMergeRequest,
    StoredBlock,
};

fn memory(id: Option<&str>, content: &str) -> MemoryUpsert {
    MemoryUpsert {
        id: id.map(str::to_owned),
        category: "SEMANTIC".into(),
        title: "test".into(),
        content: content.into(),
        keywords: None,
        source: None,
    }
}

#[tokio::test]
async fn document_cas_observes_tool_writes_and_rolls_back_cross_scope_ids() {
    let db = Db::open_in_memory().unwrap();
    let project = MemoryTarget::project("/project").unwrap();
    let empty = db.memory_snapshot(project.clone()).await.unwrap();
    assert_eq!(empty.revision, 0);
    db.create_memory(project.clone(), memory(Some("a"), "tool fact"))
        .await
        .unwrap();
    assert!(matches!(
        db.replace_memory_scope(project.clone(), empty.revision, vec![])
            .await,
        Err(DbError::Conflict(_))
    ));
    db.create_memory(MemoryTarget::global(), memory(Some("global"), "secret"))
        .await
        .unwrap();
    let before = db.memory_snapshot(project.clone()).await.unwrap();
    assert!(matches!(
        db.replace_memory_scope(
            project.clone(),
            before.revision,
            vec![
                memory(Some("a"), "changed"),
                memory(Some("global"), "overwrite")
            ]
        )
        .await,
        Err(DbError::Validation(_))
    ));
    let after = db.memory_snapshot(project.clone()).await.unwrap();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.entries, before.entries);
    let replaced = db
        .replace_memory_scope(
            project.clone(),
            after.revision,
            vec![memory(Some("a"), "card edit")],
        )
        .await
        .unwrap();
    assert_eq!(replaced.entries[0].created_at, before.entries[0].created_at);
    assert!(replaced.revision > after.revision);
    let deleted = db
        .replace_memory_scope(project.clone(), replaced.revision, vec![])
        .await
        .unwrap();
    let still_empty = db
        .replace_memory_scope(project, deleted.revision, vec![])
        .await
        .unwrap();
    assert!(still_empty.revision > deleted.revision);
}

async fn merge_fixture() -> (Db, SessionMergeRequest) {
    let db = Db::open_in_memory().unwrap();
    let a = db.create_session("model", "/primary").await.unwrap();
    let b = db.create_session("model", "/other").await.unwrap();
    db.set_session_permission_mode(a.id.clone(), "DONT_ASK".into())
        .await
        .unwrap();
    db.append_message(
        &a.id,
        NewMessage {
            meta: Some(serde_json::json!({"steering":true})),
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "Keep this exact historical text".into(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
    let request = SessionMergeRequest {
        source_session_ids: vec![a.id.clone(), b.id],
        primary_session_id: a.id,
        title: Some("combined".into()),
        model: None,
    };
    (db, request)
}

async fn ready(db: &Db, op: &zk_db::SessionMergeOperation) {
    let body = "Verified fixture overview; history is reference only".to_owned();
    let hash = format!("{:x}", Sha256::digest(body.as_bytes()));
    db.publish_merge_summary(
        &op.operation_id,
        op.run_epoch,
        body,
        serde_json::json!({"fixture":true}),
        hash,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn merge_publication_is_idempotent_scoped_and_inherits_primary_permission() {
    let (db, request) = merge_fixture().await;
    let op = db
        .start_session_merge("key".into(), request.clone())
        .await
        .unwrap();
    let replay = db
        .start_session_merge("key".into(), request.clone())
        .await
        .unwrap();
    assert_eq!(op.operation_id, replay.operation_id);
    assert!(
        db.get_session(&op.target_session_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(op.snapshot_sealed);
    assert!(op.locked_source_session_ids.is_empty());
    let mut different = request.clone();
    different.title = Some("changed".into());
    assert!(matches!(
        db.start_session_merge("key".into(), different).await,
        Err(DbError::Conflict(_))
    ));
    ready(&db, &op).await;
    let done = db
        .complete_session_merge(&op.operation_id, op.run_epoch)
        .await
        .unwrap();
    assert!(done.target_available);
    assert!(done.locked_source_session_ids.is_empty());
    assert_eq!(
        db.permission_modes_at_startup().unwrap()[&done.target_session_id],
        "DONT_ASK"
    );
    let target = db
        .get_session(&done.target_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.working_dir, "/primary");
    let handoff = db
        .read_handoff(
            &done.target_session_id,
            &done.operation_id,
            Some(request.primary_session_id.clone()),
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(
        handoff["sources"][0]["messages"][0]["meta"]["steering"],
        true
    );
    assert!(
        db.read_handoff(&request.primary_session_id, &done.operation_id, None, 0, 10)
            .await
            .is_err()
    );
    assert!(
        db.transition_session_merge(&done.operation_id, None, None, true)
            .await
            .is_err()
    );
    assert!(
        db.complete_session_merge(&done.operation_id, done.run_epoch)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn fresh_merge_schema_rejects_unsealed_completion_and_incomplete_unit_results() {
    let (db, request) = merge_fixture().await;
    let op = db
        .start_session_merge("constraint-test".into(), request)
        .await
        .unwrap();
    let id = op.operation_id;
    db.with_writer(move |conn| {
        assert!(conn.execute("UPDATE session_merges SET status='completed' WHERE id=?1", [&id]).is_err());
        assert!(conn.execute("UPDATE session_merges SET run_epoch=0 WHERE id=?1", [&id]).is_err());
        assert!(conn.execute("INSERT INTO session_merge_units(operation_id,unit_id,stage,ordinal,input_json,input_hash,state,model) VALUES(?1,'u','extracting',0,'{}','h','completed','model')", [&id]).is_err());
        assert!(conn.execute("INSERT INTO session_merge_units(operation_id,unit_id,stage,ordinal,input_json,input_hash,state,model) VALUES('missing','u','extracting',0,'{}','h','pending','model')", []).is_err());
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn merge_restart_resume_and_cancel_fence_old_workers() {
    let (db, request) = merge_fixture().await;
    let op = db
        .start_session_merge("key".into(), request.clone())
        .await
        .unwrap();
    db.pause_session_merges_at_startup().unwrap();
    let paused = db.session_merge(&op.operation_id).await.unwrap().unwrap();
    assert!(paused.can_resume);
    assert!(paused.run_epoch > op.run_epoch);
    assert!(
        db.complete_session_merge(&op.operation_id, op.run_epoch)
            .await
            .is_err()
    );
    assert!(
        db.transition_session_merge(&op.operation_id, Some(op.run_epoch), None, false)
            .await
            .is_err()
    );
    let resumed = db
        .transition_session_merge(
            &op.operation_id,
            Some(paused.run_epoch),
            Some("replacement-model".into()),
            false,
        )
        .await
        .unwrap();
    let replay = db.start_session_merge("key".into(), request).await.unwrap();
    assert_eq!(replay.operation_id, op.operation_id);
    assert_eq!(replay.request.model.as_deref(), Some("replacement-model"));
    assert!(
        db.transition_session_merge(&op.operation_id, Some(op.run_epoch), None, true)
            .await
            .is_err(),
        "an old local cancellation owner must not mutate a resumed generation"
    );
    assert_eq!(
        db.session_merge(&op.operation_id)
            .await
            .unwrap()
            .unwrap()
            .run_epoch,
        resumed.run_epoch
    );
    let cancelled = db
        .transition_session_merge(&op.operation_id, None, None, true)
        .await
        .unwrap();
    assert!(cancelled.locked_source_session_ids.is_empty());
    assert!(
        db.complete_session_merge(&op.operation_id, resumed.run_epoch)
            .await
            .is_err()
    );
    assert!(
        db.get_session(&op.target_session_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.transition_session_merge(&op.operation_id, None, None, true)
            .await
            .unwrap()
            .status,
        "cancelled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_cancel_and_publish_have_exactly_one_winner() {
    for _ in 0..12 {
        let (db, request) = merge_fixture().await;
        let op = db
            .start_session_merge("race".into(), request)
            .await
            .unwrap();
        ready(&db, &op).await;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let publish = {
            let db = db.clone();
            let op = op.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                db.complete_session_merge(&op.operation_id, op.run_epoch)
                    .await
            })
        };
        barrier.wait().await;
        let cancel = db
            .transition_session_merge(&op.operation_id, None, None, true)
            .await;
        let publish = publish.await.unwrap();
        assert_ne!(publish.is_ok(), cancel.is_ok());
        let terminal = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        assert!(terminal.locked_source_session_ids.is_empty());
        assert_eq!(
            db.get_session(&op.target_session_id)
                .await
                .unwrap()
                .is_some(),
            terminal.status == "completed"
        );
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One sealed-snapshot lifecycle through source deletion and paging.
async fn handoff_preserves_owned_files_and_paginates_original_text_after_source_deletion() {
    let directory = std::env::temp_dir().join(format!("zk-merge-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Db::open_in_memory().unwrap();
    let a = db
        .create_session("model", directory.to_str().unwrap())
        .await
        .unwrap();
    let b = db
        .create_session("model", directory.to_str().unwrap())
        .await
        .unwrap();
    let scratch = directory.join("scratch");
    let owned = scratch.join(&a.id);
    std::fs::create_dir_all(&owned).unwrap();
    let original = "甲乙丙 丁😀\n".repeat(2500);
    std::fs::write(owned.join("report.txt"), &original).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("/etc/hosts", owned.join("escape.txt")).unwrap();
    let request = SessionMergeRequest {
        source_session_ids: vec![a.id.clone(), b.id.clone()],
        primary_session_id: a.id.clone(),
        title: None,
        model: None,
    };
    let op = db
        .start_session_merge_with_assets("owned-files".into(), request, Some(scratch))
        .await
        .unwrap();
    let inputs = db
        .merge_summary_inputs(&op.operation_id, op.run_epoch)
        .await
        .unwrap();
    let text = inputs
        .iter()
        .find(|input| input.reference.starts_with("text:asset:"))
        .unwrap();
    assert_eq!(text.text, original);
    assert!(
        db.complete_session_merge(&op.operation_id, op.run_epoch)
            .await
            .is_err()
    );
    ready(&db, &op).await;
    let done = db
        .complete_session_merge(&op.operation_id, op.run_epoch)
        .await
        .unwrap();
    #[cfg(unix)]
    assert_eq!(done.result["warningCount"], 1);
    db.delete_session(&a.id).await.unwrap();
    db.delete_session(&b.id).await.unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    let mut cursor = None;
    let mut restored = String::new();
    loop {
        let page = db
            .query_handoff(
                &done.target_session_id,
                zk_db::HandoffQuery {
                    action: "read".into(),
                    reference: Some(text.reference.clone()),
                    cursor: cursor.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        restored.push_str(page["result"]["text"].as_str().unwrap());
        cursor = page["nextCursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
        assert!(
            db.query_handoff(
                &done.target_session_id,
                zk_db::HandoffQuery {
                    action: "list".into(),
                    cursor: cursor.clone(),
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
    }
    assert_eq!(restored, original);
    let asset = text.reference.strip_prefix("text:").unwrap();
    let too_large = db
        .handoff_asset(&done.target_session_id, None, asset, original.len() - 1)
        .await
        .unwrap_err();
    assert!(too_large.to_string().contains("HANDOFF_ASSET_TOO_LARGE"));
    assert_eq!(
        db.handoff_asset(&done.target_session_id, None, asset, 100_000)
            .await
            .unwrap(),
        original.as_bytes()
    );
    assert!(
        db.handoff_asset(&a.id, Some(done.operation_id), asset, 100_000)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn search_paginates_matching_text_with_deleted_anchor_and_timestamp_ties() {
    let db = Db::open_in_memory().unwrap();
    for id in ["a", "b", "c", "d"] {
        db.create_session_with_permission(id, "model", "/tmp", None)
            .await
            .unwrap();
        db.append_message(
            id,
            NewMessage {
                meta: None,
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: if id == "d" {
                        "different"
                    } else {
                        "needle %_ literal"
                    }
                    .into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
    }
    db.with_writer(|conn| {
        conn.execute(
            "UPDATE sessions SET updated_at='2026-10-07T00:00:00.000000Z'",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let first = db.search_sessions(None, 1, "%_ literal").await.unwrap();
    assert_eq!(first.sessions[0].id, "c");
    db.delete_session("c").await.unwrap();
    let second = db
        .search_sessions(first.next_cursor.as_deref(), 1, "%_ literal")
        .await
        .unwrap();
    assert_eq!(second.sessions[0].id, "b");
    let last = db
        .search_sessions(second.next_cursor.as_deref(), 1, "%_ literal")
        .await
        .unwrap();
    assert_eq!(last.sessions[0].id, "a");
    assert!(!last.has_more);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one two-generation handoff scenario verifies independent ownership after deleting original sources and package"
)]
async fn remerging_inherits_sealed_ancestors_without_borrowing_their_access() {
    use base64::Engine as _;
    let (db, request) = merge_fixture().await;
    db.append_message(
        &request.primary_session_id,
        NewMessage {
            meta: None,
            role: MessageRole::User,
            content: vec![StoredBlock::Image {
                source: zk_db::ImageSource {
                    kind: "base64".into(),
                    media_type: Some("image/png".into()),
                    data: Some(
                        base64::engine::general_purpose::STANDARD.encode(b"ancestral image"),
                    ),
                    url: None,
                },
                width: None,
                height: None,
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
    let original = db
        .start_session_merge("first-merge".into(), request.clone())
        .await
        .unwrap();
    ready(&db, &original).await;
    let first = db
        .complete_session_merge(&original.operation_id, original.run_epoch)
        .await
        .unwrap();
    for source in &request.source_session_ids {
        db.delete_session(source).await.unwrap();
    }
    let third = db.create_session("model", "/tmp").await.unwrap();
    let next = db
        .start_session_merge(
            "second-merge".into(),
            SessionMergeRequest {
                source_session_ids: vec![first.target_session_id.clone(), third.id.clone()],
                primary_session_id: first.target_session_id.clone(),
                title: None,
                model: None,
            },
        )
        .await
        .unwrap();
    ready(&db, &next).await;
    let next = db
        .complete_session_merge(&next.operation_id, next.run_epoch)
        .await
        .unwrap();
    db.delete_session(&first.target_session_id).await.unwrap();
    // Simulate retiring the older package, proving the newer snapshot owns its bytes.
    let previous = first.operation_id.clone();
    db.with_writer(move |conn| {
        conn.execute(
            "DELETE FROM session_merge_sources WHERE operation_id=?1",
            [&previous],
        )?;
        conn.execute("DELETE FROM session_merges WHERE id=?1", [previous])?;
        Ok(())
    })
    .await
    .unwrap();
    let search = db
        .query_handoff(
            &next.target_session_id,
            zk_db::HandoffQuery {
                action: "search".into(),
                query: Some("Keep this exact historical text".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!search["result"]["entries"].as_array().unwrap().is_empty());
    let catalog = db
        .query_handoff(
            &next.target_session_id,
            zk_db::HandoffQuery {
                action: "list".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let asset = catalog["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["asset"]["status"] == "copied")
        .find_map(|entry| entry["ref"].as_str().filter(|r| r.starts_with("asset:")))
        .unwrap();
    assert_eq!(
        db.handoff_asset(&next.target_session_id, None, asset, 100)
            .await
            .unwrap(),
        b"ancestral image"
    );
    assert!(
        db.handoff_asset(&third.id, Some(next.operation_id), asset, 100)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn merge_projection_filters_archive_blocks_and_edit_preview_without_changing_sealed_bytes() {
    use serde_json::json;
    let (db, request) = merge_fixture().await;
    let source = request.primary_session_id.clone();
    let preview = "DISPLAY_ONLY_PREVIEW".repeat(4000);
    let blocks = vec![
        StoredBlock::Thinking {
            thinking: "ARCHIVE_REASONING".into(),
        },
        StoredBlock::ProviderResponseState {
            provider: "test".into(),
            model: "test".into(),
            output: vec![json!({"state":"PRIVATE_CONTINUATION"})],
        },
        StoredBlock::ToolResult {
            tool_use_id: "edit".into(),
            content: "RETAIN_TOOL_FAILURE".into(),
            is_error: true,
            metadata: Some(
                json!({"verification":"KEEP_VERIFICATION", "structuredResult":{"schema":"edit-diff/v1","diff":preview}}),
            ),
        },
        StoredBlock::ToolResult {
            tool_use_id: "resource".into(),
            content: "RESOURCE_BODY".into(),
            is_error: false,
            metadata: Some(
                json!({"structuredResult":{"schema":"external-resource/v1","value":"KEEP_RESOURCE_METADATA"}, "__zkTrustedImageProducer":true, "inlineImages":[{"data":"PRIVATE_IMAGE_BYTES","sourceDigest":"digest"}]}),
            ),
        },
        StoredBlock::Text {
            text: "User literal metadata=edit-diff/v1".into(),
        },
    ];
    db.append_message(
        &source,
        NewMessage {
            role: MessageRole::User,
            content: blocks.clone(),
            meta: None,
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
    let checkpoint =
        json!([{"role":"user","content":blocks,"toolUseResult":"KEEP_CHECKPOINT_FAILURE"}])
            .to_string();
    db.with_conn_blocking(|conn| { conn.execute("INSERT INTO agent_checkpoints(id,run_id,session_id,agent_id,seq,messages_json,created_at) VALUES('cp','run',?1,'agent',0,?2,'now')", (&source,&checkpoint))?; Ok(()) }).unwrap();
    let op = db
        .start_session_merge("filtered-projection".into(), request)
        .await
        .unwrap();
    let raw: String = db.with_conn_blocking(|conn| Ok(conn.query_row("SELECT snapshot_json FROM session_merge_sources WHERE operation_id=?1 AND source_session_id=?2", (&op.operation_id,&source), |row| row.get(0))?)).unwrap();
    assert!(
        raw.contains("ARCHIVE_REASONING")
            && raw.contains("DISPLAY_ONLY_PREVIEW")
            && raw.contains("PRIVATE_CONTINUATION")
            && raw.contains("PRIVATE_IMAGE_BYTES")
    );
    let first = db
        .merge_summary_inputs(&op.operation_id, op.run_epoch)
        .await
        .unwrap();
    let text = first
        .iter()
        .map(|input| input.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for visible in [
        "RETAIN_TOOL_FAILURE",
        "KEEP_VERIFICATION",
        "KEEP_RESOURCE_METADATA",
        "User literal metadata=edit-diff/v1",
        "KEEP_CHECKPOINT_FAILURE",
    ] {
        assert!(text.contains(visible), "{visible}");
    }
    for archive in [
        "ARCHIVE_REASONING",
        "DISPLAY_ONLY_PREVIEW",
        "PRIVATE_CONTINUATION",
        "PRIVATE_IMAGE_BYTES",
    ] {
        assert!(!text.contains(archive), "{archive}");
    }
    db.delete_session(&source).await.unwrap();
    let again = db
        .merge_summary_inputs(&op.operation_id, op.run_epoch)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&again).unwrap()
    );
    let raw_again: String = db.with_conn_blocking(|conn| Ok(conn.query_row("SELECT snapshot_json FROM session_merge_sources WHERE operation_id=?1 AND source_session_id=?2", (&op.operation_id,&source), |row| row.get(0))?)).unwrap();
    assert_eq!(raw, raw_again);
}

async fn published_text_handoff(
    text: String,
) -> (Db, zk_db::SessionMergeOperation, zk_db::MergeSummaryInput) {
    let (db, request) = merge_fixture().await;
    let source = request.primary_session_id.clone();
    db.append_message(
        &source,
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text { text }],
            meta: None,
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
    let op = db
        .start_session_merge("indexed-handoff".into(), request)
        .await
        .unwrap();
    let input = db
        .merge_summary_inputs(&op.operation_id, op.run_epoch)
        .await
        .unwrap()
        .into_iter()
        .filter(|input| input.reference.starts_with("message:"))
        .max_by_key(|input| input.text.len())
        .unwrap();
    ready(&db, &op).await;
    let op = db
        .complete_session_merge(&op.operation_id, op.run_epoch)
        .await
        .unwrap();
    db.delete_session(&source).await.unwrap();
    (db, op, input)
}

#[tokio::test]
async fn handoff_pages_every_match_and_evidence_span_with_a_full_json_response_limit() {
    let needle = "EVIDENCE_甲";
    let content = format!(
        "{needle}{} {needle}{} {needle}",
        "😀\"\n".repeat(6000),
        "\\".repeat(12000)
    );
    let (db, op, input) = published_text_handoff(content).await;
    let expected = input
        .text
        .match_indices(needle)
        .map(|(start, _)| start)
        .collect::<Vec<_>>();
    let mut found = Vec::new();
    let mut cursor = None;
    loop {
        let page = db
            .query_handoff(
                &op.target_session_id,
                zk_db::HandoffQuery {
                    action: "search".into(),
                    query: Some(needle.into()),
                    limit: Some(1),
                    cursor,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(page.to_string().len() <= 16_384);
        for entry in page["result"]["entries"].as_array().unwrap() {
            assert_eq!(entry["ref"], input.reference);
            found.push(usize::try_from(entry["start"].as_u64().unwrap()).unwrap());
        }
        cursor = page["nextCursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(found, expected);
    let mut recovered = String::new();
    let mut cursor = None;
    loop {
        let page = db
            .query_handoff(
                &op.target_session_id,
                zk_db::HandoffQuery {
                    action: "read".into(),
                    reference: Some(input.reference.clone()),
                    cursor,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(page.to_string().len() <= 16_384);
        recovered.push_str(page["result"]["text"].as_str().unwrap());
        cursor = page["nextCursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(recovered, input.text);
    let span = format!(
        "{}@{}:{}",
        input.reference,
        expected[1],
        expected[1] + needle.len()
    );
    let page = db
        .query_handoff(
            &op.target_session_id,
            zk_db::HandoffQuery {
                action: "read".into(),
                reference: Some(span),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page["result"]["text"], needle);
    assert_eq!(page["complete"], true);
    db.with_conn_blocking(|conn| {conn.execute_batch("DROP TRIGGER session_handoff_chunks_immutable")?;conn.execute("UPDATE session_handoff_chunks SET content=X'00' WHERE operation_id=?1 AND reference=?2 AND start_byte=0",(&op.operation_id,&input.reference))?;Ok(())}).unwrap();
    assert!(
        db.query_handoff(
            &op.target_session_id,
            zk_db::HandoffQuery {
                action: "read".into(),
                reference: Some(input.reference),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn handoff_search_scan_budget_returns_a_progressing_cursor_without_losing_later_matches() {
    let (db, op, _) =
        published_text_handoff(format!("{}LATER_MATCH", "x".repeat(9 * 1024 * 1024))).await;
    let first = db
        .query_handoff(
            &op.target_session_id,
            zk_db::HandoffQuery {
                action: "search".into(),
                query: Some("LATER_MATCH".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first["complete"], false);
    assert!(first["result"]["entries"].as_array().unwrap().is_empty());
    assert!(first["result"]["scannedBytes"].as_u64().unwrap() <= 8 * 1024 * 1024);
    let second = db
        .query_handoff(
            &op.target_session_id,
            zk_db::HandoffQuery {
                action: "search".into(),
                query: Some("LATER_MATCH".into()),
                cursor: first["nextCursor"].as_str().map(str::to_owned),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(second["result"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(second["complete"], true);
}
