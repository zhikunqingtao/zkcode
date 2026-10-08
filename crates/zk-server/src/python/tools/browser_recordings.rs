//! Private-spool recording intake and durable, evidence-gated consumption ACK.
use super::{BROWSER_AUTOMATION, PythonEnvelope};
use crate::python::{Correlation, PythonClient};
use nix::{
    fcntl::{OFlag, open, openat},
    sys::stat::Mode,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Duration,
};
use zk_db::Db;
use zk_tools::EvidenceReceiptItem;

fn spool_root() -> Result<PathBuf, String> {
    std::env::var_os("ZK_BROWSER_RECORDING_SPOOL")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(|home| PathBuf::from(home).join(".zkcode/browser-recordings"))
        })
        .ok_or_else(|| "RECORDING_SPOOL_UNAVAILABLE".into())
}
fn directory(path: &Path) -> Result<File, String> {
    let file = File::from(
        open(
            path,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| "RECORDING_SPOOL_UNAVAILABLE")?,
    );
    let meta = file.metadata().map_err(|_| "RECORDING_SPOOL_UNAVAILABLE")?;
    if meta.mode() & 0o077 != 0 {
        return Err("RECORDING_SPOOL_NOT_PRIVATE".into());
    }
    Ok(file)
}
fn child(parent: &File, name: &str, is_dir: bool) -> Result<File, String> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err("RECORDING_PATH_INVALID".into());
    }
    let flags = OFlag::O_RDONLY
        | OFlag::O_NOFOLLOW
        | OFlag::O_CLOEXEC
        | OFlag::O_NONBLOCK
        | if is_dir {
            OFlag::O_DIRECTORY
        } else {
            OFlag::empty()
        };
    Ok(File::from(
        openat(parent, name, flags, Mode::empty()).map_err(|_| "RECORDING_FILE_UNAVAILABLE")?,
    ))
}
fn read_bounded(file: &mut File, limit: u64) -> Result<Vec<u8>, String> {
    let metadata = file.metadata().map_err(|_| "RECORDING_FILE_UNAVAILABLE")?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err("RECORDING_BYTE_BUDGET_EXCEEDED".into());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "RECORDING_READ_FAILED")?;
    if bytes.len() as u64 > limit {
        return Err("RECORDING_BYTE_BUDGET_EXCEEDED".into());
    }
    Ok(bytes)
}
fn verified_batch_directory_at(root: &Path, manifest: &Value) -> Result<File, String> {
    let batch = manifest["batch_id"]
        .as_str()
        .ok_or("RECORDING_BATCH_INVALID")?;
    let id = uuid::Uuid::parse_str(batch).map_err(|_| "RECORDING_BATCH_INVALID")?;
    if id.to_string() != batch {
        return Err("RECORDING_BATCH_INVALID".into());
    }
    let root = directory(root)?;
    let batch_dir = child(&root, batch, true)?;
    let mut manifest_file = child(&batch_dir, "manifest.json", false)?;
    let bytes = read_bounded(&mut manifest_file, 65536)?;
    if format!("{:x}", Sha256::digest(&bytes))
        != manifest["manifest_sha256"].as_str().unwrap_or_default()
    {
        return Err("RECORDING_MANIFEST_MISMATCH".into());
    }
    let mut expected = manifest.clone();
    expected
        .as_object_mut()
        .ok_or("RECORDING_MANIFEST_INVALID")?
        .remove("manifest_sha256");
    let saved: Value = serde_json::from_slice(&bytes).map_err(|_| "RECORDING_MANIFEST_INVALID")?;
    if saved != expected {
        return Err("RECORDING_MANIFEST_MISMATCH".into());
    }
    Ok(batch_dir)
}
fn read_verified_at(root: &Path, manifest: &Value, item: &Value) -> Result<Vec<u8>, String> {
    let batch_dir = verified_batch_directory_at(root, manifest)?;
    let path = item["path"].as_str().ok_or("RECORDING_PATH_INVALID")?;
    let mut file = if let Some(name) = path.strip_prefix("video/") {
        let video = child(&batch_dir, "video", true)?;
        child(&video, name, false)?
    } else if matches!(path, "trace.zip" | "network.har") {
        child(&batch_dir, path, false)?
    } else {
        return Err("RECORDING_PATH_INVALID".into());
    };
    let before = file.metadata().map_err(|_| "RECORDING_FILE_UNAVAILABLE")?;
    let bytes = read_bounded(&mut file, 10 * 1024 * 1024)?;
    let after = file.metadata().map_err(|_| "RECORDING_FILE_UNAVAILABLE")?;
    if !before.is_file()
        || before.dev() != item["device"].as_u64().unwrap_or(u64::MAX)
        || before.ino() != item["inode"].as_u64().unwrap_or(u64::MAX)
        || before.len() != item["size"].as_u64().unwrap_or(u64::MAX)
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || format!("{:x}", Sha256::digest(&bytes)) != item["sha256"].as_str().unwrap_or_default()
    {
        return Err("RECORDING_FILE_IDENTITY_MISMATCH".into());
    }
    Ok(bytes)
}

pub(super) async fn archive(
    db: &Db,
    session_id: &str,
    workspace: &Path,
    response: &Value,
    items: &mut Vec<EvidenceReceiptItem>,
) -> Result<(), String> {
    if response.get("recording_manifest").is_none() {
        return Ok(());
    }
    archive_at(db, session_id, workspace, response, items, &spool_root()?).await
}

async fn archive_at(
    db: &Db,
    session_id: &str,
    workspace: &Path,
    response: &Value,
    items: &mut Vec<EvidenceReceiptItem>,
    spool: &Path,
) -> Result<(), String> {
    let Some(manifest) = response.get("recording_manifest") else {
        return Ok(());
    };
    let files = manifest["files"]
        .as_array()
        .ok_or("RECORDING_MANIFEST_INVALID")?;
    if files.len() > 50 {
        return Err("RECORDING_FILE_COUNT_EXCEEDED".into());
    }
    let checked = manifest.clone();
    let root = spool.to_owned();
    tokio::task::spawn_blocking(move || verified_batch_directory_at(&root, &checked).map(drop))
        .await
        .map_err(|_| "RECORDING_MANIFEST_INVALID")??;
    let resource = response["recording_resource_id"]
        .as_str()
        .ok_or("RECORDING_OWNER_MISSING")?;
    db.seal_browser_recording(resource, manifest.clone(), json!([]))
        .await
        .map_err(|error| format!("RECORDING_FINALIZATION_STORE_FAILED: {error}"))?;
    let mut dispositions = Vec::new();
    let mut remaining = 20 * 1024 * 1024u64;
    for item in files {
        let mut disposition = json!({"path": item["path"], "kind": item["kind"], "status": item["status"], "size": item["size"], "error_code": item["error_code"]});
        let mut digest = None;
        if item["status"] == "available" {
            let size = item["size"].as_u64().ok_or("RECORDING_MANIFEST_INVALID")?;
            if size > remaining || items.len() >= zk_tools::MAX_EVIDENCE_RECEIPT_ITEMS {
                disposition["status"] = json!("omitted_budget");
                disposition["error_code"] = json!(if size > remaining {
                    "RECORDING_BYTE_BUDGET_EXCEEDED"
                } else {
                    "RECORDING_ITEM_BUDGET_EXCEEDED"
                });
            } else {
                let (manifest, item, root) = (manifest.clone(), item.clone(), spool.to_owned());
                let bytes =
                    tokio::task::spawn_blocking(move || read_verified_at(&root, &manifest, &item))
                        .await
                        .map_err(|_| "RECORDING_READ_FAILED")??;
                remaining -= size;
                digest = Some(
                    crate::api::evidence::store_blob(db, session_id, workspace.to_owned(), bytes)
                        .await
                        .map_err(|_| "RECORDING_BLOB_STORE_FAILED")?,
                );
                disposition["status"] = json!("archived");
                disposition["blob_sha256"] = json!(digest);
            }
        } else if !matches!(item["status"].as_str(), Some("missing" | "omitted_budget")) {
            return Err("RECORDING_DISPOSITION_INVALID".into());
        }
        if items.len() < zk_tools::MAX_EVIDENCE_RECEIPT_ITEMS {
            items.push(EvidenceReceiptItem {
                item_type: "journey_recording".into(),
                summary: Some(format!(
                    "{}: {}",
                    item["kind"].as_str().unwrap_or("recording"),
                    disposition["status"].as_str().unwrap_or("missing")
                )),
                blob_sha256: digest,
                meta: Some(disposition.clone()),
                sort_order: u32::try_from(items.len())
                    .map_err(|_| "RECORDING_ITEM_BUDGET_EXCEEDED")?,
            });
        }
        dispositions.push(disposition);
    }
    if items.is_empty() {
        items.push(EvidenceReceiptItem {
            item_type: "journey_recording".into(),
            summary: Some("Sealed recording contains no files".into()),
            blob_sha256: None,
            meta: None,
            sort_order: 0,
        });
    }
    let last = items.last_mut().ok_or("RECORDING_EVIDENCE_MISSING")?;
    let meta = last.meta.get_or_insert_with(|| json!({}));
    meta["recording_manifest_sha256"] = manifest["manifest_sha256"].clone();
    meta["recording_dispositions"] = json!(dispositions);
    let resource = response["recording_resource_id"]
        .as_str()
        .ok_or("RECORDING_OWNER_MISSING")?;
    db.seal_browser_recording(resource, manifest.clone(), json!(dispositions))
        .await
        .map_err(|error| format!("RECORDING_FINALIZATION_STORE_FAILED: {error}"))?;
    Ok(())
}

/// Called by the existing maintenance supervisor; never replays browser work.
///
/// # Errors
/// Returns stable store or protocol errors while retaining unconsumed batches.
pub async fn reconcile_browser_recordings(db: &Db, client: &PythonClient) -> Result<usize, String> {
    reconcile_browser_recordings_at(db, client, &spool_root()?).await
}

enum RecordingCloseOutcome {
    Pending,
    Sealed,
    NotCreated,
}

async fn close_stopped_recording(
    db: &Db,
    client: &PythonClient,
    entry: &zk_db::BrowserRecordingFinalization,
) -> Result<RecordingCloseOutcome, String> {
    let proof = db
        .execution_resource_release_proof(&entry.resource_id)
        .await
        .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?
        .ok_or("RECORDING_OWNER_MISSING")?;
    if entry.external_id.as_deref() != Some(proof.external_id.as_str()) {
        return Err("RECORDING_OWNER_MISMATCH".into());
    }
    let response: Option<PythonEnvelope> = client
        .call_if_available_with_timeout(
            BROWSER_AUTOMATION,
            "/api/browser/close_session",
            &json!({"session_id":proof.external_id,"recording":entry.state["identity"]}),
            &Correlation::for_session(entry.state["identity"]["session_id"].as_str()),
            Duration::from_secs(7),
        )
        .await;
    let Some(data) = response
        .filter(|response| response.success)
        .and_then(|response| response.data)
        .filter(|data| data["closed"] == true)
    else {
        return Ok(RecordingCloseOutcome::Pending);
    };
    let released = if entry.physical_status == "unconfirmed" {
        db.reconcile_execution_resource_release(&proof).await
    } else {
        db.finalize_execution_resource(&entry.resource_id, zk_db::ExecutionResourceStatus::Released)
            .await
    }
    .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?;
    if released != zk_db::CasOutcome::Applied {
        return Ok(RecordingCloseOutcome::Pending);
    }
    // This refreshes only orthogonal cleanup after a positive close. The
    // original result (including cancellation) is never changed.
    let _ = db.retry_confirmed_run_cleanup(&proof.run_id).await;
    if let Some(manifest) = data.get("recording_manifest") {
        db.seal_browser_recording(
            &entry.resource_id,
            manifest.clone(),
            entry.state["dispositions"]
                .as_array()
                .map_or_else(|| json!([]), |items| json!(items)),
        )
        .await
        .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?;
    } else if let Some(absence) = data.get("recording_finalization") {
        db.acknowledge_uncreated_browser_recording(&entry.resource_id, absence.clone())
            .await
            .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?;
        return Ok(RecordingCloseOutcome::NotCreated);
    } else {
        return Ok(RecordingCloseOutcome::Pending);
    }
    Ok(RecordingCloseOutcome::Sealed)
}

async fn reconcile_one(
    db: &Db,
    client: &PythonClient,
    mut entry: zk_db::BrowserRecordingFinalization,
    spool: &Path,
) -> Result<bool, String> {
    if entry.recoverable
        && (entry.state["phase"] == "reserved" || entry.physical_status != "released")
    {
        // Only a terminal failed/cancelled/interrupted invocation transfers its
        // durable reservation here. An active Journey is never closed by maintenance.
        match close_stopped_recording(db, client, &entry).await? {
            RecordingCloseOutcome::Pending => return Ok(false),
            RecordingCloseOutcome::NotCreated => return Ok(true),
            RecordingCloseOutcome::Sealed => {}
        }
        entry = refresh_recording(db, &entry.resource_id).await?;
    }
    if entry.state["phase"] == "reserved" {
        return Ok(false);
    }
    if entry.state["phase"] == "sealed" && entry.recoverable {
        if entry.state["dispositions"]
            .as_array()
            .is_none_or(Vec::is_empty)
        {
            let session = entry.state["identity"]["session_id"]
                .as_str()
                .ok_or("RECORDING_OWNER_MISSING")?;
            let detail = db
                .get_session(session)
                .await
                .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?
                .ok_or("RECORDING_OWNER_MISSING")?;
            archive_at(db, session, Path::new(&detail.working_dir), &json!({"recording_manifest":entry.state["manifest"],"recording_resource_id":entry.resource_id}), &mut Vec::new(), spool).await?;
        }
        db.complete_failed_browser_recording(&entry.resource_id)
            .await
            .map_err(|_| "RECORDING_RECOVERY_EVIDENCE_FAILED")?;
        entry = refresh_recording(db, &entry.resource_id).await?;
    }
    if entry.state["phase"] == "sealed" && !entry.recoverable {
        let invocation = entry.state["identity"]["invocation_id"]
            .as_str()
            .ok_or("RECORDING_OWNER_MISSING")?;
        if zk_engine::complete_recorded_verify_journey_evidence(db, invocation).await? {
            entry = refresh_recording(db, &entry.resource_id).await?;
        }
    }
    let mut version = entry.version;
    if entry.state["phase"] == "sealed" {
        if !db
            .advance_browser_recording(&entry.resource_id, version, false)
            .await
            .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?
        {
            return Ok(false);
        }
        version += 1;
    }
    let response: Option<PythonEnvelope> = client.call_if_available_with_timeout(BROWSER_AUTOMATION, "/api/browser/recordings/ack", &json!({"identity":entry.state["identity"],"manifest_sha256":entry.state["manifest"]["manifest_sha256"]}), &Correlation::for_session(entry.state["identity"]["session_id"].as_str()), Duration::from_secs(5)).await;
    if response.is_some_and(|response| {
        response.success
            && response
                .data
                .is_some_and(|data| data["acknowledged"] == true)
    }) {
        return db
            .advance_browser_recording(&entry.resource_id, version, true)
            .await
            .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED".into());
    }
    Ok(false)
}

async fn refresh_recording(
    db: &Db,
    resource: &str,
) -> Result<zk_db::BrowserRecordingFinalization, String> {
    db.browser_recording_finalization(resource)
        .await
        .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?
        .ok_or_else(|| "RECORDING_OWNER_MISSING".into())
}

async fn reconcile_browser_recordings_at(
    db: &Db,
    client: &PythonClient,
    spool: &Path,
) -> Result<usize, String> {
    let pending = db
        .pending_browser_recordings()
        .await
        .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?;
    let mut acknowledged = 0;
    let mut failure = None;
    for entry in pending {
        match reconcile_one(db, client, entry, spool).await {
            Ok(true) => acknowledged += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, "recording retained for later reconciliation");
                failure = Some(error);
            }
        }
    }
    let candidates: Option<PythonEnvelope> = client
        .call_if_available_with_timeout(
            BROWSER_AUTOMATION,
            "/api/browser/recordings/orphans",
            &json!({}),
            &Correlation::for_session(None),
            Duration::from_secs(5),
        )
        .await;
    if let Some(response) = candidates.filter(|response| response.success)
        && let Some(candidates) = response
            .data
            .as_ref()
            .and_then(|data| data["candidates"].as_array())
    {
        for batch in candidates.iter().take(50).filter_map(Value::as_str) {
            if uuid::Uuid::parse_str(batch).is_err()
                || db
                    .browser_recording_batch_protected(batch)
                    .await
                    .map_err(|_| "RECORDING_FINALIZATION_STORE_FAILED")?
            {
                continue;
            }
            let _: Option<PythonEnvelope> = client
                .call_if_available_with_timeout(
                    BROWSER_AUTOMATION,
                    "/api/browser/recordings/prune",
                    &json!({"batch_id":batch}),
                    &Correlation::for_session(None),
                    Duration::from_secs(5),
                )
                .await;
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(acknowledged),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn fixture() -> (Fixture, Value, Value) {
        let root = std::env::temp_dir().join(format!("zk-recording-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let batch = uuid::Uuid::new_v4().to_string();
        let path = root.join(&batch);
        std::fs::create_dir(&path).unwrap();
        let file = path.join("network.har");
        std::fs::write(&file, b"fixture recording").unwrap();
        let metadata = std::fs::metadata(file).unwrap();
        let item = json!({"path":"network.har","kind":"har","status":"available","size":metadata.len(),"device":metadata.dev(),"inode":metadata.ino(),"sha256":format!("{:x}",Sha256::digest(b"fixture recording"))});
        let mut manifest = json!({"version":1,"batch_id":batch,"files":[item]});
        let encoded = serde_json::to_vec(&manifest).unwrap();
        std::fs::write(path.join("manifest.json"), &encoded).unwrap();
        manifest["manifest_sha256"] = json!(format!("{:x}", Sha256::digest(encoded)));
        (Fixture(root), manifest, item)
    }
    #[test]
    fn recording_reader_binds_manifest_and_actual_bytes() {
        let (root, manifest, item) = fixture();
        assert_eq!(
            read_verified_at(&root.0, &manifest, &item).unwrap(),
            b"fixture recording"
        );
        let path = root
            .0
            .join(manifest["batch_id"].as_str().unwrap())
            .join("network.har");
        std::fs::write(path, b"different content").unwrap();
        assert!(
            read_verified_at(&root.0, &manifest, &item)
                .unwrap_err()
                .contains("IDENTITY")
        );
    }
    #[test]
    fn recording_reader_rejects_symlink_and_manifest_replacement() {
        let (root, manifest, item) = fixture();
        let batch = root.0.join(manifest["batch_id"].as_str().unwrap());
        let outside = root.0.join("fixture-outside");
        std::fs::write(&outside, b"must not be consumed").unwrap();
        std::fs::remove_file(batch.join("network.har")).unwrap();
        symlink(&outside, batch.join("network.har")).unwrap();
        assert!(read_verified_at(&root.0, &manifest, &item).is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"must not be consumed");
        std::fs::write(batch.join("manifest.json"), b"{}").unwrap();
        assert!(
            verified_batch_directory_at(&root.0, &manifest)
                .unwrap_err()
                .contains("MANIFEST_MISMATCH")
        );
    }
    async fn recovery_fixture() -> (Fixture, Db, String, Value) {
        let (root, mut manifest, _) = fixture();
        let db = Db::open(root.0.join("recording.sqlite")).unwrap();
        let session = db
            .create_session("m", root.0.to_str().unwrap())
            .await
            .unwrap()
            .id;
        db.start_run("recovery-run", &session, None, Some("fixture"), "m")
            .await
            .unwrap();
        let run = db.find_run_by_id("recovery-run").await.unwrap().unwrap();
        db.create_tool_invocation(&zk_db::NewToolInvocation {
            invocation_id: "recovery-invocation".into(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            tool_use_id: "tool".into(),
            tool_name: "VerifyJourney".into(),
            input_json: Some("{}".into()),
            side_effect_class: "read".into(),
            directory_generation: None,
            connection_generation: None,
        })
        .await
        .unwrap();
        let identity = json!({"batch_id":manifest["batch_id"],"session_id":session,"run_id":run.id,"invocation_id":"recovery-invocation"});
        manifest.as_object_mut().unwrap().remove("manifest_sha256");
        manifest["identity"] = identity.clone();
        let encoded = serde_json::to_vec(&manifest).unwrap();
        std::fs::write(
            root.0
                .join(manifest["batch_id"].as_str().unwrap())
                .join("manifest.json"),
            &encoded,
        )
        .unwrap();
        manifest["manifest_sha256"] = json!(format!("{:x}", Sha256::digest(encoded)));
        db.register_execution_resource(&zk_db::NewExecutionResource {resource_id:"recovery-resource".into(),task_id:run.task_id,run_id:run.id,invocation_id:Some("recovery-invocation".into()),resource_kind:"stream".into(),external_id:Some("rv-recovery".into()),metadata_json:json!({"kind":"browserSession","recordingFinalization":{"version":1,"phase":"reserved","identity":identity}}).to_string()}).await.unwrap();
        (root, db, session, manifest)
    }

    async fn fail_invocation(db: &Db, session: &str, outcome: zk_db::ToolInvocationStatus) {
        db.commit_tool_invocation_result(&zk_db::CommitToolInvocationResult {
            invocation_id: "recovery-invocation".into(),
            expected_version: 0,
            session_id: session.into(),
            target: outcome,
            input_json: None,
            content: "original stopped Journey".into(),
            is_error: true,
            metadata: None,
            output_sha256: None,
            error_code: Some("ORIGINAL_STOP".into()),
            cleanup_status: zk_db::CleanupStatus::Unconfirmed,
            postprocessing: None,
        })
        .await
        .unwrap();
    }

    struct RecordingStub {
        server: tokio::task::JoinHandle<()>,
        socket: PathBuf,
        calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }
    impl Drop for RecordingStub {
        fn drop(&mut self) {
            self.server.abort();
            let _ = std::fs::remove_file(&self.socket);
        }
    }
    fn recording_stub(manifest: Value, batch: PathBuf) -> RecordingStub {
        use axum::{
            Json, Router,
            response::IntoResponse,
            routing::{get, post},
        };
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        let socket = PathBuf::from(format!("/tmp/zk-rec-{}.sock", uuid::Uuid::new_v4()));
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let closes = Arc::new(AtomicUsize::new(0));
        let acks = Arc::new(AtomicUsize::new(0));
        let router=Router::new()
            .route("/api/health/capabilities",get(||async {Json(json!({"BROWSER_AUTOMATION":{"name":"browser","available":true}}))}))
            .route("/api/browser/close_session",post({let calls=calls.clone(); move |Json(body):Json<Value>| {let calls=calls.clone();let closes=closes.clone();let manifest=manifest.clone();async move {
                assert_eq!(body["session_id"],"rv-recovery");assert_eq!(body["recording"],manifest["identity"]);
                calls.lock().unwrap().push("close".into());
                if closes.fetch_add(1,Ordering::SeqCst)==0 { return "lost close response".into_response(); }
                if manifest["not_created"] == true {
                    return Json(json!({"success":true,"data":{"closed":true,"recording_finalization":{"phase":"not_created","identity":manifest["identity"]}}})).into_response();
                }
                Json(json!({"success":true,"data":{"closed":true,"recording_manifest":manifest}})).into_response()
            }}}))
            .route("/api/browser/recordings/ack",post({let calls=calls.clone();move |Json(_):Json<Value>| {let calls=calls.clone();let acks=acks.clone();let batch=batch.clone();async move {
                calls.lock().unwrap().push("ack".into());
                if acks.fetch_add(1,Ordering::SeqCst)==0 {return Json(json!({"success":false}));}
                let _=std::fs::remove_file(batch.join("network.har"));
                std::fs::write(batch.join("ack.json"),"{}").unwrap();
                Json(json!({"success":true,"data":{"acknowledged":true}}))
            }}}))
            .route("/api/browser/recordings/orphans",post(||async {Json(json!({"success":true,"data":{"candidates":[]}}))}))
            .fallback(||async {axum::http::StatusCode::NOT_FOUND});
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        RecordingStub {
            server,
            socket,
            calls,
        }
    }

    async fn succeeded_recording_fixture() -> (Fixture, Db, String, Value) {
        let (root, db, session, manifest) = recovery_fixture().await;
        db.finalize_execution_resource(
            "recovery-resource",
            zk_db::ExecutionResourceStatus::Released,
        )
        .await
        .unwrap();
        let mut items = Vec::new();
        archive_at(
            &db,
            &session,
            &root.0,
            &json!({"recording_manifest":manifest,"recording_resource_id":"recovery-resource"}),
            &mut items,
            &root.0,
        )
        .await
        .unwrap();
        let receipt = zk_tools::EvidenceReceipt {
            schema_version: zk_tools::EVIDENCE_RECEIPT_SCHEMA_VERSION,
            kind: "browser_journey".into(),
            claim: Some("original claim".into()),
            verdict: zk_tools::EvidenceReceiptVerdict::Verified,
            observed_at: "2026-10-08T00:00:00.000000Z".into(),
            items,
        };
        assert!(receipt.is_valid());
        let metadata = json!({"structuredResult":{"evidence":receipt}});
        db.commit_tool_invocation_result(&zk_db::CommitToolInvocationResult {
            invocation_id:"recovery-invocation".into(), expected_version:0,session_id:session.clone(),
            target:zk_db::ToolInvocationStatus::Succeeded,input_json:Some("{}".into()),
            content:"immutable completed Journey".into(),is_error:false,metadata:Some(metadata.clone()),
            output_sha256:None,error_code:None,cleanup_status:zk_db::CleanupStatus::Confirmed,
            postprocessing:Some(json!({"schemaVersion":1,"toolName":"VerifyJourney","requiredKinds":["evidence"],"metadata":metadata})),
        }).await.unwrap();
        let run = db.find_run_by_id("recovery-run").await.unwrap().unwrap();
        let task = db
            .find_runtime_task_by_id(&run.task_id)
            .await
            .unwrap()
            .unwrap();
        db.mark_task_run_needs_attention(
            &task.id,
            &run.id,
            task.version,
            "fixture postprocessing outage",
            zk_db::CleanupStatus::Confirmed,
        )
        .await
        .unwrap();
        (root, db, session, manifest)
    }

    #[tokio::test]
    async fn succeeded_recording_replays_pending_evidence_after_reopen() {
        let (root, db, session, manifest) = succeeded_recording_fixture().await;
        let batch = root.0.join(manifest["batch_id"].as_str().unwrap());
        let original = db.list_messages(&session, None, 20).await.unwrap().unwrap();
        drop(db);
        let db = Db::open(root.0.join("recording.sqlite")).unwrap();
        let stub = recording_stub(manifest, batch.clone());
        let client = PythonClient::new(stub.socket.clone());
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            db.find_evidence_by_session(&session).await.unwrap().len(),
            1,
            "maintenance must restore succeeded producer evidence without browser actions"
        );
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            1
        );
        assert!(batch.join("ack.json").exists());
        assert!(db.pending_browser_recordings().await.unwrap().is_empty());
        assert_eq!(stub.calls.lock().unwrap().as_slice(), &["ack", "ack"]);
        let after = db.list_messages(&session, None, 20).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(original).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        let batch_id = batch.file_name().unwrap().to_str().unwrap();
        assert!(
            !db.browser_recording_batch_protected(batch_id)
                .await
                .unwrap()
        );
        assert!(
            db.delete_session(&session)
                .await
                .unwrap_err()
                .to_string()
                .contains("active tasks"),
            "the separate Task quarantine must remain"
        );
    }

    #[tokio::test]
    async fn recorded_journey_evidence_and_journal_rollback_together_and_retry() {
        for fault in [
            "CREATE TEMP TRIGGER reject_evidence BEFORE INSERT ON evidence_bundles WHEN NEW.origin='machine' BEGIN SELECT RAISE(ABORT,'injected evidence failure'); END",
            "CREATE TEMP TRIGGER reject_evidence BEFORE UPDATE OF status ON tool_result_postprocessing WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'injected completion failure'); END",
        ] {
            let (_root, db, session, _) = succeeded_recording_fixture().await;
            db.with_conn_blocking(move |conn| {
                conn.execute_batch(fault)?;
                Ok(())
            })
            .unwrap();
            assert!(
                zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation")
                    .await
                    .is_err()
            );
            assert!(
                db.find_evidence_by_session(&session)
                    .await
                    .unwrap()
                    .is_empty()
            );
            db.with_conn_blocking(|conn| {
                assert_eq!(
                    conn.query_row("SELECT status FROM tool_result_postprocessing", [], |r| {
                        r.get::<_, String>(0)
                    })?,
                    "pending"
                );
                conn.execute_batch("DROP TRIGGER reject_evidence")?;
                Ok(())
            })
            .unwrap();
            let (left, right) = tokio::join!(
                zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation"),
                zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation")
            );
            assert!(left.unwrap() && right.unwrap());
            assert_eq!(
                db.find_evidence_by_session(&session).await.unwrap().len(),
                1
            );
        }
    }

    fn old_recorded_bundle(
        snapshot: &zk_db::RecordedJourneyPostprocessing,
    ) -> zk_db::EvidenceBundleRecord {
        let receipt: zk_tools::EvidenceReceipt =
            serde_json::from_value(snapshot.receipt.clone()).unwrap();
        let old_id = uuid::Uuid::new_v4().to_string();
        zk_db::EvidenceBundleRecord {
            bundle_id: old_id.clone(),
            session_id: snapshot.session_id.clone(),
            agent_id: None,
            kind: receipt.kind,
            claim: receipt.claim,
            origin: zk_db::EvidenceOrigin::Machine,
            producer_invocation_id: Some("recovery-invocation".into()),
            verdict: receipt.verdict.as_db().into(),
            created_at: receipt.observed_at,
            run_id: Some("recovery-run".into()),
            items: receipt
                .items
                .into_iter()
                .map(|item| zk_db::EvidenceItemRecord {
                    id: uuid::Uuid::new_v4().to_string(),
                    producer_invocation_id: Some("recovery-invocation".into()),
                    item_type: item.item_type,
                    summary: item.summary,
                    blob_sha256: item.blob_sha256,
                    meta: item.meta,
                    sort_order: i64::from(item.sort_order),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn recorded_journey_reuses_old_ids_and_preserves_human_verdict() {
        let (_root, db, session, _) = succeeded_recording_fixture().await;
        let snapshot = db
            .recorded_journey_postprocessing("recovery-invocation")
            .await
            .unwrap()
            .unwrap();
        let bundle = old_recorded_bundle(&snapshot);
        let old_id = bundle.bundle_id.clone();
        db.save_evidence_bundle(&bundle).await.unwrap();
        db.update_evidence_verdict(&old_id, "failed").await.unwrap();
        let before = db.find_evidence_verdict_events(&old_id).await.unwrap();
        for _ in 0..2 {
            assert!(
                zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation")
                    .await
                    .unwrap()
            );
        }
        let saved = db.find_evidence_by_session(&session).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].bundle_id, old_id);
        assert_eq!(saved[0].items, bundle.items);
        assert_eq!(saved[0].origin, zk_db::EvidenceOrigin::Human);
        assert_eq!(saved[0].verdict, "failed");
        assert_eq!(
            db.find_evidence_verdict_events(&old_id).await.unwrap(),
            before
        );
        // Completed is not blind permission to accept a conflicting receipt.
        db.with_conn_blocking(|conn| {conn.execute("UPDATE tool_result_postprocessing SET payload_json=json_set(payload_json,'$.metadata.structuredResult.evidence.claim','different')",[])?;Ok(())}).unwrap();
        assert!(
            zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn recorded_journey_rejects_old_evidence_content_conflicts() {
        for change in ["meta", "blob", "items", "session"] {
            let (_root, db, session, _) = succeeded_recording_fixture().await;
            let snapshot = db
                .recorded_journey_postprocessing("recovery-invocation")
                .await
                .unwrap()
                .unwrap();
            let mut bundle = old_recorded_bundle(&snapshot);
            match change {
                "meta" => bundle.items[0].meta.as_mut().unwrap()["unexpected"] = json!(true),
                "blob" => bundle.items[0].blob_sha256 = Some("b".repeat(64)),
                "items" => {
                    bundle.items.clear();
                }
                "session" => {
                    bundle.session_id = "unowned".into();
                }
                _ => unreachable!(),
            }
            if change == "session" {
                assert!(
                    db.complete_recorded_journey_postprocessing(&snapshot, &bundle)
                        .await
                        .is_err()
                );
            } else {
                db.save_evidence_bundle(&bundle).await.unwrap();
                assert!(
                    zk_engine::complete_recorded_verify_journey_evidence(
                        &db,
                        "recovery-invocation"
                    )
                    .await
                    .is_err()
                );
                assert_eq!(
                    db.find_evidence_by_session(&session).await.unwrap().len(),
                    1
                );
            }
            db.with_conn_blocking(|conn| {
                assert_eq!(
                    conn.query_row("SELECT status FROM tool_result_postprocessing", [], |r| {
                        r.get::<_, String>(0)
                    })?,
                    "pending"
                );
                Ok(())
            })
            .unwrap();
        }
    }

    #[tokio::test]
    async fn recorded_journey_rejects_unknown_receipt_fields_without_data_loss() {
        for suffix in [".unexpected", ".items[0].unexpected"] {
            let (_root, db, session, _) = succeeded_recording_fixture().await;
            db.with_conn_blocking(move |conn| {
                conn.execute("UPDATE tool_result_postprocessing SET payload_json=json_set(payload_json,?1,1)",[format!("$.metadata.structuredResult.evidence{suffix}")])?;
                conn.execute("UPDATE messages SET content_json=json_set(content_json,?1,1) WHERE origin='tool_result'",[format!("$[0].metadata.structuredResult.evidence{suffix}")])?;
                Ok(())
            }).unwrap();
            assert!(
                zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation")
                    .await
                    .is_err()
            );
            assert!(
                db.find_evidence_by_session(&session)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn recorded_journey_live_snapshot_survives_maintenance_completion_and_ack() {
        let (root, db, session, manifest) = succeeded_recording_fixture().await;
        let snapshot = db
            .recorded_journey_postprocessing("recovery-invocation")
            .await
            .unwrap()
            .unwrap();
        let bundle = old_recorded_bundle(&snapshot);
        let batch = root.0.join(manifest["batch_id"].as_str().unwrap());
        let stub = recording_stub(manifest, batch);
        let client = PythonClient::new(stub.socket.clone());
        // Suspend a live completion after its reader snapshot. Maintenance wins
        // both completion and resource CASes; the live writer must accept proof.
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            1
        );
        db.complete_recorded_journey_postprocessing(&snapshot, &bundle)
            .await
            .unwrap();
        assert_eq!(
            db.find_evidence_by_session(&session).await.unwrap().len(),
            1
        );
        assert_eq!(stub.calls.lock().unwrap().as_slice(), &["ack", "ack"]);
    }

    #[tokio::test]
    async fn recorded_journey_rejects_corrupt_archive_and_unrelated_obligations() {
        let (root, db, session, manifest) = succeeded_recording_fixture().await;
        let digest = manifest["files"][0]["sha256"].as_str().unwrap();
        std::fs::write(
            root.0.join(".zk/blobs").join(&digest[..2]).join(digest),
            b"corrupt",
        )
        .unwrap();
        assert!(
            zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation")
                .await
                .is_err()
        );
        assert!(
            db.find_evidence_by_session(&session)
                .await
                .unwrap()
                .is_empty()
        );
        db.with_conn_blocking(|conn| {conn.execute("UPDATE tool_result_postprocessing SET payload_json=json_set(payload_json,'$.requiredKinds',json('[\"evidence\",\"artifact\"]'))",[])?;Ok(())}).unwrap();
        assert!(
            !zk_engine::complete_recorded_verify_journey_evidence(&db, "recovery-invocation")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn terminal_recording_recovers_lost_close_seal_blob_and_ack_failures_after_reopen() {
        let (root, db, session, manifest) = recovery_fixture().await;
        let batch = root.0.join(manifest["batch_id"].as_str().unwrap());
        let stub = recording_stub(manifest, batch.clone());
        let client = PythonClient::new(stub.socket.clone());
        // A live invocation's reserved allocation must never be touched.
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            0
        );
        assert!(stub.calls.lock().unwrap().is_empty());
        fail_invocation(&db, &session, zk_db::ToolInvocationStatus::Cancelled).await;
        db.finalize_execution_resource(
            "recovery-resource",
            zk_db::ExecutionResourceStatus::Unconfirmed,
        )
        .await
        .unwrap();
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            refresh_recording(&db, "recovery-resource")
                .await
                .unwrap()
                .state["phase"],
            "reserved"
        );
        db.with_conn_blocking(|conn| {conn.execute_batch("CREATE TEMP TRIGGER reject_first_seal BEFORE UPDATE ON execution_resources WHEN json_extract(NEW.metadata_json,'$.recordingFinalization.phase')='sealed' BEGIN SELECT RAISE(ABORT,'injected seal outage'); END;")?;Ok(())}).unwrap();
        assert!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .is_err()
        );
        db.with_conn_blocking(|conn| {
            conn.execute_batch("DROP TRIGGER reject_first_seal;")?;
            Ok(())
        })
        .unwrap();
        std::fs::write(root.0.join(".zk"), b"block blob directory").unwrap();
        assert!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .is_err()
        );
        assert_eq!(
            refresh_recording(&db, "recovery-resource")
                .await
                .unwrap()
                .state["phase"],
            "sealed"
        );
        assert!(!batch.join("ack.json").exists());
        drop(db);
        let reopened = Db::open(root.0.join("recording.sqlite")).unwrap();
        std::fs::remove_file(root.0.join(".zk")).unwrap();
        assert_eq!(
            reconcile_browser_recordings_at(&reopened, &client, &root.0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            refresh_recording(&reopened, "recovery-resource")
                .await
                .unwrap()
                .state["phase"],
            "ackEligible"
        );
        assert_eq!(
            reconcile_browser_recordings_at(&reopened, &client, &root.0)
                .await
                .unwrap(),
            1
        );
        assert!(batch.join("ack.json").exists());
        assert!(!batch.join("network.har").exists());
        assert!(
            reopened
                .pending_browser_recordings()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            stub.calls.lock().unwrap().as_slice(),
            ["close", "close", "close", "ack", "ack"]
        );
        let bundles = reopened.find_evidence_by_session(&session).await.unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].verdict, "inconclusive");
        let (status,code): (String,String)=reopened.with_conn_blocking(|conn| Ok(conn.query_row("SELECT status,error_code FROM tool_invocations WHERE invocation_id='recovery-invocation'",[],|row|Ok((row.get(0)?,row.get(1)?)))?)).unwrap();
        assert_eq!((status, code), ("cancelled".into(), "ORIGINAL_STOP".into()));
    }
    #[tokio::test]
    async fn failed_uncreated_recording_requires_positive_absence_before_releasing_reservation() {
        let (root, db, session, manifest) = recovery_fixture().await;
        let batch = root.0.join(manifest["batch_id"].as_str().unwrap());
        std::fs::remove_dir_all(&batch).unwrap();
        let stub = recording_stub(
            json!({"identity":manifest["identity"],"not_created":true}),
            batch,
        );
        let client = PythonClient::new(stub.socket.clone());
        fail_invocation(&db, &session, zk_db::ToolInvocationStatus::Failed).await;
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            0
        );
        assert!(
            db.browser_recording_batch_protected(manifest["batch_id"].as_str().unwrap())
                .await
                .unwrap()
        );
        assert_eq!(
            reconcile_browser_recordings_at(&db, &client, &root.0)
                .await
                .unwrap(),
            1
        );
        assert!(
            !db.browser_recording_batch_protected(manifest["batch_id"].as_str().unwrap())
                .await
                .unwrap()
        );
        assert!(
            db.find_evidence_by_session(&session)
                .await
                .unwrap()
                .is_empty(),
            "absence proof cannot invent a recording"
        );
        assert_eq!(stub.calls.lock().unwrap().as_slice(), ["close", "close"]);
    }
}
