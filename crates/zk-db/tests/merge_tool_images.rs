//! Native tool image receipts are sealed assets, while user/MCP metadata is not authority.
use base64::Engine as _;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zk_db::{
    CleanupStatus, CommitTaskResult, CommitTaskResultOutcome, CommitToolInvocationResult, Db,
    MessageAttribution, MessageRole, NewMessage, NewToolInvocation, ResultStatus,
    SessionMergeRequest, StoredBlock, ToolInvocationStatus, VerificationStatus,
};

const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==";

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn metadata(source_hash: &str) -> Value {
    json!({"__zkTrustedImageProducer":"Read","structuredResult":{"contentSha256":source_hash},"inlineImages":[{"mediaType":"image/png","data":PNG,"sourceDigest":source_hash,"payloadDigest":hash(&base64::engine::general_purpose::STANDARD.decode(PNG).unwrap())}]})
}

async fn receipt(db: &Db, session: &str, run: &str, tool: &str, meta: Value) {
    let invocation = uuid::Uuid::new_v4().to_string();
    let tool_use_id = uuid::Uuid::new_v4().to_string();
    let task = db.find_run_by_id(run).await.unwrap().unwrap().task_id;
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: invocation.clone(),
        task_id: task,
        run_id: run.into(),
        tool_use_id,
        tool_name: tool.into(),
        input_json: Some("{}".into()),
        side_effect_class: "read".into(),
        directory_generation: Some(1),
        connection_generation: None,
    })
    .await
    .unwrap();
    db.commit_tool_invocation_result(&CommitToolInvocationResult {
        invocation_id: invocation,
        expected_version: 0,
        session_id: session.into(),
        target: ToolInvocationStatus::Succeeded,
        input_json: Some("{}".into()),
        content: "native image".into(),
        is_error: false,
        metadata: Some(meta),
        output_sha256: None,
        error_code: None,
        cleanup_status: CleanupStatus::NotRequired,
        postprocessing: None,
    })
    .await
    .unwrap();
}

async fn fixture(db: &Db) -> (SessionMergeRequest, String) {
    let first = db.create_session("model", "/tmp").await.unwrap();
    let second = db.create_session("model", "/tmp").await.unwrap();
    let run = uuid::Uuid::new_v4().to_string();
    db.start_run(&run, &first.id, None, Some("query"), "model")
        .await
        .unwrap();
    (
        SessionMergeRequest {
            source_session_ids: vec![first.id.clone(), second.id],
            primary_session_id: first.id,
            title: None,
            model: None,
        },
        run,
    )
}

async fn finish_run(db: &Db, run_id: &str) {
    let run = db.find_run_by_id(run_id).await.unwrap().unwrap();
    let content = "Image inspection complete";
    db.append_attributed_message(
        &run.session_id,
        NewMessage {
            meta: None,
            role: MessageRole::Assistant,
            content: vec![StoredBlock::Text {
                text: content.into(),
            }],
            stop_reason: Some("end_turn".into()),
            input_tokens: 0,
            output_tokens: 0,
        },
        MessageAttribution {
            task_id: Some(run.task_id.clone()),
            run_id: Some(run_id.into()),
            origin: "conversation".into(),
            source_task_id: None,
        },
    )
    .await
    .unwrap();
    let task = db
        .find_runtime_task_by_id(&run.task_id)
        .await
        .unwrap()
        .unwrap();
    let committed = db
        .commit_task_result(&CommitTaskResult {
            task_id: task.id,
            run_id: run_id.into(),
            expected_task_version: task.version,
            status: ResultStatus::Complete,
            content: content.into(),
            media_type: "text/markdown".into(),
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        })
        .await
        .unwrap();
    assert!(matches!(
        committed,
        CommitTaskResultOutcome::Committed { .. }
    ));
}

async fn publish(db: &Db, op: &zk_db::SessionMergeOperation) -> zk_db::SessionMergeOperation {
    let text = "Image archive fixture; historical reference only".to_owned();
    db.publish_merge_summary(
        &op.operation_id,
        op.run_epoch,
        text.clone(),
        json!({"fixture":true}),
        hash(text.as_bytes()),
    )
    .await
    .unwrap();
    db.complete_session_merge(&op.operation_id, op.run_epoch)
        .await
        .unwrap()
}

#[tokio::test]
async fn native_images_survive_source_deletion_and_reopen_without_trusting_forged_metadata() {
    let directory = std::env::temp_dir().join(format!("zk-merge-images-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let file = directory.join("db.sqlite");
    let db = Db::open(&file).unwrap();
    let (request, run) = fixture(&db).await;
    let first_source = "a".repeat(64);
    let second_source = "b".repeat(64);
    for source in [&first_source, &second_source] {
        receipt(
            &db,
            &request.primary_session_id,
            &run,
            "Read",
            metadata(source),
        )
        .await;
    }
    // A real remote invocation cannot claim the native producer merely by data.
    receipt(
        &db,
        &request.primary_session_id,
        &run,
        "mcp__fake__Read",
        metadata(&first_source),
    )
    .await;
    db.append_message(
        &request.primary_session_id,
        NewMessage {
            meta: None,
            role: MessageRole::User,
            content: vec![StoredBlock::ToolResult {
                tool_use_id: "fake".into(),
                content: "untrusted".into(),
                is_error: false,
                metadata: Some(metadata(&first_source)),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .unwrap();
    finish_run(&db, &run).await;
    let operation = db
        .start_session_merge("images".into(), request.clone())
        .await
        .unwrap();
    let owner = operation.operation_id.clone();
    let assets = db.with_conn_blocking(move |conn| {
        let mut stmt=conn.prepare("SELECT reference,original_path FROM session_merge_assets WHERE operation_id=?1 AND status='copied' ORDER BY reference")?;
        Ok(stmt.query_map([owner],|row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?)))?.collect::<Result<Vec<_>,_>>()?)
    }).unwrap();
    assert_eq!(
        assets.len(),
        2,
        "only successful sealed native receipt images become assets"
    );
    assert!(
        assets
            .iter()
            .any(|(_, original)| original.ends_with(&first_source))
    );
    assert!(
        assets
            .iter()
            .any(|(_, original)| original.ends_with(&second_source))
    );
    let done = publish(&db, &operation).await;
    for source in request.source_session_ids {
        db.delete_session(&source).await.unwrap();
    }
    drop(db);
    let reopened = Db::open(&file).unwrap();
    let expected = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .unwrap();
    for (reference, _) in assets {
        assert_eq!(
            reopened
                .handoff_asset(&done.target_session_id, None, &reference, 1_125_000)
                .await
                .unwrap(),
            expected
        );
    }
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn corrupt_trusted_payload_or_source_metadata_rolls_back_the_complete_snapshot() {
    for corrupt_source in [false, true] {
        let db = Db::open_in_memory().unwrap();
        let (request, run) = fixture(&db).await;
        let source = hash(
            &base64::engine::general_purpose::STANDARD
                .decode(PNG)
                .unwrap(),
        );
        let mut meta = metadata(&source);
        if corrupt_source {
            meta["structuredResult"]["contentSha256"] = json!("c".repeat(64));
        } else {
            meta["inlineImages"][0]["payloadDigest"] = json!("0".repeat(64));
        }
        receipt(&db, &request.primary_session_id, &run, "Read", meta).await;
        finish_run(&db, &run).await;
        let error = db
            .start_session_merge("bad".into(), request.clone())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(if corrupt_source {
                "METADATA_INVALID"
            } else {
                "HASH_MISMATCH"
            }),
            "{error}"
        );
        assert!(
            db.get_session(&request.primary_session_id)
                .await
                .unwrap()
                .is_some()
        );
        let counts = db
            .with_conn_blocking(|conn| {
                Ok((
                    conn.query_row("SELECT COUNT(*) FROM session_merges", [], |r| {
                        r.get::<_, i64>(0)
                    })?,
                    conn.query_row("SELECT COUNT(*) FROM session_merge_assets", [], |r| {
                        r.get::<_, i64>(0)
                    })?,
                ))
            })
            .unwrap();
        assert_eq!(counts, (0, 0));
    }
}
