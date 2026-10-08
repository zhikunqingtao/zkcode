//! Private capture files isolate source reads and asset IO from the main `SQLite` writer.
use super::{Db, DbError, Snapshot, digest, read_operation, snapshot};
use crate::session_merge_budget::MergeWriteBudget;
use rusqlite::{Connection, OptionalExtension, params, types::Value as SqlValue};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) const RECORD_BYTES: usize = 32 * 1024 * 1024;
const METADATA_BYTES: usize = 64 * 1024 * 1024;
const METADATA_ROWS: usize = 20_000;
const TABLES: &[&str] = &["session_merge_sources", "session_merge_assets"];

#[cfg(test)]
type CaptureGate = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
#[cfg(test)]
static CAPTURE_GATES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, CaptureGate>>,
> = std::sync::OnceLock::new();

pub(super) struct CaptureBudget {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    rows: usize,
    bytes: usize,
}
impl CaptureBudget {
    fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            deadline: Instant::now() + Duration::from_mins(2),
            cancelled,
            rows: 0,
            bytes: 0,
        }
    }
    pub(super) fn check(&self) -> Result<(), DbError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(DbError::Conflict("MERGE_CAPTURE_CANCELLED".into()));
        }
        if Instant::now() >= self.deadline {
            return Err(DbError::Invalid("MERGE_CAPTURE_TIMEOUT".into()));
        }
        Ok(())
    }
    pub(super) fn record<T: Serialize>(&mut self, value: &T) -> Result<String, DbError> {
        self.check()?;
        if self.rows >= METADATA_ROWS {
            return Err(DbError::Invalid("MERGE_METADATA_ROW_LIMIT".into()));
        }
        let encoded = bounded_json(value)?;
        self.bytes = self
            .bytes
            .checked_add(encoded.len())
            .ok_or_else(|| DbError::Invalid("MERGE_METADATA_LIMIT".into()))?;
        if self.bytes > METADATA_BYTES {
            return Err(DbError::Invalid("MERGE_METADATA_LIMIT".into()));
        }
        self.rows += 1;
        Ok(encoded)
    }
    pub(super) fn preflight(&self, size: usize) -> Result<(), DbError> {
        self.check()?;
        if size > RECORD_BYTES {
            return Err(DbError::Invalid("MERGE_RECORD_TOO_LARGE".into()));
        }
        if size > METADATA_BYTES.saturating_sub(self.bytes) {
            return Err(DbError::Invalid("MERGE_METADATA_LIMIT".into()));
        }
        if self.rows >= METADATA_ROWS {
            return Err(DbError::Invalid("MERGE_METADATA_ROW_LIMIT".into()));
        }
        Ok(())
    }
    pub(super) fn inherited_snapshot(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<String, DbError> {
        self.count_inherited_rows(value)?;
        self.record(value)
    }
    fn count_inherited_rows(&mut self, value: &serde_json::Value) -> Result<(), DbError> {
        self.check()?;
        let messages = value.get("messages").and_then(serde_json::Value::as_array);
        let records = value.get("records").and_then(serde_json::Value::as_array);
        let additional = messages
            .map_or(0, Vec::len)
            .saturating_add(records.map_or(0, Vec::len));
        self.rows = self.rows.saturating_add(additional);
        if self.rows >= METADATA_ROWS {
            return Err(DbError::Invalid("MERGE_METADATA_ROW_LIMIT".into()));
        }
        if let Some(records) = records {
            for record in records {
                self.check()?;
                if record
                    .get("reference")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|reference| reference.contains(":prior_handoff:"))
                {
                    let text = record
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            DbError::Invalid("MERGE_INVALID_INHERITED_SNAPSHOT".into())
                        })?;
                    self.count_inherited_rows(&serde_json::from_str(text)?)?;
                }
            }
        }
        Ok(())
    }
}
pub(super) fn bounded_json<T: Serialize>(value: &T) -> Result<String, DbError> {
    struct Bounded(Vec<u8>);
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.0.len().saturating_add(bytes.len()) > RECORD_BYTES {
                return Err(std::io::Error::other("MERGE_RECORD_TOO_LARGE"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded(Vec::new());
    serde_json::to_writer(&mut output, value).map_err(|error| {
        if error.is_io() {
            DbError::Invalid("MERGE_RECORD_TOO_LARGE".into())
        } else {
            DbError::from(error)
        }
    })?;
    String::from_utf8(output.0).map_err(|_| DbError::Invalid("MERGE_INVALID_UTF8".into()))
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "Owned error adapter is passed directly to IO Result::map_err"
)]
fn io(error: std::io::Error) -> DbError {
    DbError::Invalid(format!("MERGE_STAGING_IO: {error}"))
}
fn fence(conn: &Connection, id: &str, epoch: i64) -> Result<(), DbError> {
    let valid: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM session_merges WHERE id=?1 AND run_epoch=?2 AND status='preparing' AND snapshot_sealed=0 AND summary_body IS NULL)", params![id, epoch], |r| r.get(0))?;
    if valid {
        Ok(())
    } else {
        Err(DbError::Conflict("MERGE_STALE_EPOCH".into()))
    }
}
fn private_dir(path: &Path) -> Result<(), DbError> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path).map_err(io)?;
    let metadata = std::fs::symlink_metadata(path).map_err(io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(DbError::Invalid("MERGE_STAGING_DIRECTORY_INVALID".into()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(io)?;
    }
    Ok(())
}
fn create_file(path: &Path) -> Result<std::fs::File, DbError> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(io)
}
fn file_hash(path: &Path, budget: &CaptureBudget) -> Result<(String, u64), DbError> {
    use sha2::{Digest, Sha256};
    let mut file = std::fs::File::open(path).map_err(io)?;
    let mut hash = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        budget.check()?;
        let read = file.read(&mut buffer).map_err(io)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        size += read as u64;
    }
    Ok((format!("{:x}", hash.finalize()), size))
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    operation: String,
    database: String,
    epoch: i64,
    captured_at_ms: i64,
    request_hash: String,
    sha256: String,
    bytes: u64,
}
struct Captured {
    directory: PathBuf,
    manifest: Manifest,
    manifest_hash: String,
}
struct TemporaryCatalog(PathBuf);
impl Drop for TemporaryCatalog {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        if let Some(parent) = self.0.parent() {
            let _ = std::fs::remove_dir(parent);
        }
    }
}

fn staging_base(conn: &Connection) -> PathBuf {
    conn.path().filter(|path| !path.is_empty()).map_or_else(
        || std::env::temp_dir().join(format!("zk-merge-staging-{}", std::process::id())),
        |path| {
            let path = Path::new(path);
            path.parent().unwrap_or(Path::new(".")).join(format!(
                ".{}-merge-staging",
                path.file_name().unwrap_or_default().to_string_lossy()
            ))
        },
    )
}

impl Db {
    /// Remove only incomplete private staging from fenced generations after the host
    /// has joined their workers. Frozen manifests and unknown directory entries survive.
    /// # Errors
    /// Requires a paused/failed unsealed operation at the exact expected epoch.
    pub async fn cleanup_stopped_merge_staging(&self, id: &str, epoch: i64) -> Result<(), DbError> {
        let id = id.to_owned();
        self.with_reader(move |conn| {
            let op = read_operation(conn, &id)?.ok_or_else(||DbError::Invalid("merge disappeared".into()))?;
            if op.run_epoch != epoch || !matches!(op.status.as_str(), "paused"|"failed") {
                return Err(DbError::Conflict("MERGE_STALE_EPOCH".into()));
            }
            if op.snapshot_sealed || op.result.get("captureManifest").is_some() { return Ok(()); }
            let has_summary:bool=conn.query_row("SELECT summary_body IS NOT NULL OR EXISTS(SELECT 1 FROM session_merge_units WHERE operation_id=?1) FROM session_merges WHERE id=?1",[&id],|row|row.get(0))?;
            if has_summary {return Err(DbError::Conflict("MERGE_UNSEALED_SUMMARY_EXISTS".into()));}
            // The identity is generated by the host, never a path accepted from a request.
            if uuid::Uuid::parse_str(&id).is_err() { return Err(DbError::Invalid("MERGE_STAGING_IDENTITY_INVALID".into())); }
            let base = staging_base(conn);
            let directory = base.join(id);
            for path in [&base, &directory] {
                match std::fs::symlink_metadata(path) {
                    Err(error) if error.kind()==std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(io(error)),
                    Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => return Err(DbError::Invalid("MERGE_STAGING_DIRECTORY_INVALID".into())),
                    Ok(_) => {},
                }
            }
            for entry in std::fs::read_dir(&directory).map_err(io)? {
                let entry = entry.map_err(io)?;
                let Some(old_epoch) = entry.file_name().to_str().and_then(|name|name.parse::<i64>().ok()) else {continue;};
                if old_epoch < 1 || old_epoch >= epoch || !entry.file_type().map_err(io)?.is_dir() {continue;}
                match std::fs::symlink_metadata(entry.path().join("manifest.json")) {
                    Ok(_) => continue,
                    Err(error) if error.kind()==std::io::ErrorKind::NotFound => {},
                    Err(error) => return Err(io(error)),
                }
                // remove_dir_all removes encountered symlinks themselves, never their targets.
                std::fs::remove_dir_all(entry.path()).map_err(io)?;
            }
            Ok(())
        }).await
    }

    /// Capture source history in a consistent reader transaction, then import immutable rows in bounded writes.
    /// # Errors
    /// Limits, interrupted capture, changed epoch, storage failure and manifest corruption are explicit.
    #[allow(
        clippy::too_many_lines,
        reason = "Keep the single deadline and frozen manifest identity visible across capture, bounded import and sealing phases"
    )]
    pub async fn prepare_session_merge_capture(
        &self,
        id: &str,
        epoch: i64,
        scratchpad: Option<PathBuf>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<(), DbError> {
        let operation = self
            .session_merge(id)
            .await?
            .ok_or_else(|| DbError::Invalid("merge disappeared".into()))?;
        if operation.snapshot_sealed {
            return Ok(());
        }
        let id = id.to_owned();
        let database_memory_identity = format!(
            "memory-{}-{:p}",
            std::process::id(),
            Arc::as_ptr(&self.writer)
        );
        let deadline = Instant::now() + Duration::from_mins(2);
        let capture_id = id.clone();
        let cancellation = cancelled.clone();
        let captured = self.with_reader(move |conn| {
            let mut budget = CaptureBudget::new(cancellation);
            budget.deadline=deadline;
            let database = conn.path().filter(|path| !path.is_empty()).map_or(database_memory_identity, str::to_owned);
            let base = staging_base(conn);
            let transaction = conn.transaction()?;
            fence(&transaction, &capture_id, epoch)?;
            #[cfg(test)] {
                let gate=CAPTURE_GATES.get_or_init(Default::default).lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&capture_id);
                if let Some((entered,release))=gate {let _=entered.send(());let _=release.recv_timeout(Duration::from_secs(5));}
            }
            let op = read_operation(&transaction, &capture_id)?.ok_or_else(|| DbError::Invalid("merge disappeared".into()))?;
            let original_request:String=transaction.query_row("SELECT request_json FROM session_merges WHERE id=?1",[&capture_id],|row|row.get(0))?;
            let request_hash = digest(original_request.as_bytes());
            let captured_epoch = op.result.get("captureEpoch").and_then(serde_json::Value::as_i64).unwrap_or(epoch);
            let directory = base.join(&capture_id).join(captured_epoch.to_string());
            if let Some(expected) = op.result.get("captureManifest").and_then(serde_json::Value::as_str) {
                let mut encoded=Vec::new();
                std::fs::File::open(directory.join("manifest.json")).map_err(io)?.take(4097).read_to_end(&mut encoded).map_err(io)?;
                if encoded.len() > 4096 || digest(&encoded) != expected { return Err(DbError::Invalid("MERGE_MANIFEST_MISMATCH".into())); }
                let manifest: Manifest = serde_json::from_slice(&encoded)?;
                if manifest.operation != capture_id || manifest.database != database || manifest.epoch != captured_epoch || manifest.request_hash != request_hash { return Err(DbError::Invalid("MERGE_MANIFEST_MISMATCH".into())); }
                let (hash, bytes) = file_hash(&directory.join("capture.sqlite"), &budget)?;
                if hash != manifest.sha256 || bytes != manifest.bytes { return Err(DbError::Invalid("MERGE_MANIFEST_MISMATCH".into())); }
                return Ok(Captured { directory, manifest, manifest_hash: expected.to_owned() });
            }
            if op.locked_source_session_ids.is_empty() { return Err(DbError::Conflict("MERGE_SOURCES_NOT_RESERVED".into())); }
            private_dir(&base)?; private_dir(&base.join(&capture_id))?; private_dir(&directory)?;
            let stage_path = directory.join("capture.sqlite");
            create_file(&stage_path)?.sync_all().map_err(io)?;
            let stage = Connection::open(&stage_path)?;
            // This private transport contains only immutable rows; their parent
            // operations live in the main database, where imports enforce FKs.
            stage.pragma_update(None, "foreign_keys", "OFF")?;
            for table in TABLES { let sql: String = transaction.query_row("SELECT sql FROM sqlite_master WHERE type='table' AND name=?1", [table], |r| r.get(0))?; stage.execute_batch(&sql)?; }
            let mut disk = MergeWriteBudget::new(&stage);
            let mut copied_bytes = 0; let mut serialized_bytes = 0_usize;
            for (ordinal, source) in op.locked_source_session_ids.iter().enumerate() {
                budget.check()?;
                let header_bytes:i64=transaction.query_row("SELECT length(CAST(id AS BLOB))+COALESCE(length(CAST(title AS BLOB)),0)+length(CAST(model AS BLOB))+length(CAST(working_dir AS BLOB))+COALESCE(length(CAST(permission_mode AS BLOB)),0)+COALESCE(length(CAST(summary AS BLOB)),0) FROM sessions WHERE id=?1",[source],|row|row.get(0))?;
                budget.preflight(usize::try_from(header_bytes).map_err(|_|DbError::Invalid("MERGE_RECORD_TOO_LARGE".into()))?)?;
                let mut value = transaction.query_row("SELECT id,title,model,working_dir,permission_mode,summary FROM sessions WHERE id=?1", [source], |r| Ok(Snapshot { session_id:r.get(0)?,title:r.get(1)?,model:r.get(2)?,working_directory:r.get(3)?,permission_mode:r.get(4)?,summary:r.get(5)?,messages:Vec::new(),records:Vec::new() }))?;
                budget.record(&value)?;
                value.messages = snapshot::messages(&transaction, source, &mut budget)?;
                value.records = snapshot::records(&transaction, source, &mut budget)?;
                snapshot::copy_assets(&transaction, &stage, &capture_id, &value, scratchpad.as_deref(), &mut copied_bytes, &mut disk, &mut budget)?;
                let encoded = bounded_json(&value)?;
                serialized_bytes = serialized_bytes.saturating_add(encoded.len());
                if serialized_bytes > METADATA_BYTES { return Err(DbError::Invalid("MERGE_METADATA_LIMIT".into())); }
                disk.reserve(encoded.len().saturating_add(1024))?;
                stage.execute("INSERT INTO session_merge_sources VALUES(?1,?2,?3,?4,?5)", params![capture_id, source, i64::try_from(ordinal).map_err(|_|DbError::Invalid("MERGE_METADATA_ROW_LIMIT".into()))?, encoded, digest(encoded.as_bytes())])?;
            }
            transaction.commit()?;
            drop(stage);
            std::fs::File::open(&stage_path).map_err(io)?.sync_all().map_err(io)?;
            let (sha256, bytes) = file_hash(&stage_path, &budget)?;
            let manifest = Manifest { operation:capture_id, database, epoch, captured_at_ms:crate::time::now_millis(), request_hash, sha256, bytes };
            let encoded = serde_json::to_vec(&manifest)?;
            let temporary = directory.join(format!("manifest-{}.tmp", uuid::Uuid::new_v4()));
            let mut file = create_file(&temporary)?; file.write_all(&encoded).map_err(io)?; file.sync_all().map_err(io)?;
            std::fs::rename(&temporary, directory.join("manifest.json")).map_err(io)?;
            std::fs::File::open(&directory).map_err(io)?.sync_all().map_err(io)?;
            Ok(Captured { directory, manifest, manifest_hash:digest(&encoded) })
        }).await?;
        let identity = captured.manifest_hash.clone();
        let capture_epoch = captured.manifest.epoch;
        let captured_at = crate::time::format_rfc3339_micros(captured.manifest.captured_at_ms);
        let operation_id = id.clone();
        let publication_cancelled = cancelled.clone();
        self.with_writer(move |conn| { let tx=conn.transaction()?; fence(&tx,&operation_id,epoch)?;
            CaptureBudget {deadline,cancelled:publication_cancelled,rows:0,bytes:0}.check()?;
            tx.execute("UPDATE session_merges SET stage='importing',result_json=json_set(result_json,'$.captureManifest',?1,'$.captureEpoch',?2,'$.capturedAt',?4) WHERE id=?3",params![identity,capture_epoch,operation_id,captured_at])?;
            tx.execute("DELETE FROM session_merge_locks WHERE operation_id=?1",[&operation_id])?; tx.commit()?;Ok(()) }).await?;
        let mut import_budget = CaptureBudget::new(cancelled);
        import_budget.deadline = deadline;
        for table in TABLES {
            let mut cursor = 0_i64;
            loop {
                import_budget.check()?;
                let path = captured.directory.join("capture.sqlite");
                let read_table = (*table).to_owned();
                let loaded = tokio::task::spawn_blocking(
                    move || -> Result<Option<(i64, Vec<SqlValue>)>, DbError> {
                        let stage = Connection::open_with_flags(
                            path,
                            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                        )?;
                        let mut query = stage.prepare(&format!(
                            "SELECT rowid,* FROM {read_table} WHERE rowid>?1 ORDER BY rowid LIMIT 1"
                        ))?;
                        let columns = query.column_count();
                        let result = query
                            .query_row([cursor], |row| {
                                Ok((
                                    row.get(0)?,
                                    (1..columns)
                                        .map(|index| row.get::<_, SqlValue>(index))
                                        .collect::<Result<Vec<_>, _>>()?,
                                ))
                            })
                            .optional()?;
                        if let Some((_, values)) = &result {
                            validate_frozen_row(&read_table, values)?;
                        }
                        Ok(result)
                    },
                )
                .await??;
                let Some((rowid, values)) = loaded else {
                    break;
                };
                cursor = rowid;
                let table = (*table).to_owned();
                let id = id.clone();
                let cancelled = import_budget.cancelled.clone();
                self.with_writer(move |conn| {
                    if cancelled.load(Ordering::Acquire) {
                        return Err(DbError::Conflict("MERGE_CAPTURE_CANCELLED".into()));
                    }
                    if Instant::now() >= deadline {
                        return Err(DbError::Invalid("MERGE_CAPTURE_TIMEOUT".into()));
                    }
                    let budget = CaptureBudget {
                        deadline,
                        cancelled,
                        rows: 0,
                        bytes: 0,
                    };
                    import_row(conn, &table, &id, epoch, &values, &budget)
                })
                .await?;
            }
        }
        import_budget.check()?;
        let counts_path = captured.directory.join("capture.sqlite");
        let expected_counts =
            tokio::task::spawn_blocking(move || -> Result<(i64, i64), DbError> {
                let conn = Connection::open_with_flags(
                    counts_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                Ok((
                    conn.query_row("SELECT count(*) FROM session_merge_sources", [], |r| {
                        r.get(0)
                    })?,
                    conn.query_row("SELECT count(*) FROM session_merge_assets", [], |r| {
                        r.get(0)
                    })?,
                ))
            })
            .await??;
        let id_clone = id.clone();
        let expected = captured.manifest_hash;
        let publication_cancelled = import_budget.cancelled.clone();
        self.with_writer(move|conn| { let tx=conn.transaction()?; fence(&tx,&id_clone,epoch)?;
            CaptureBudget {deadline,cancelled:publication_cancelled,rows:0,bytes:0}.check()?;
            let actual:String=tx.query_row("SELECT json_extract(result_json,'$.captureManifest') FROM session_merges WHERE id=?1",[&id_clone],|r|r.get(0))?;
            if actual!=expected {return Err(DbError::Invalid("MERGE_MANIFEST_MISMATCH".into()));}
            let counts:(i64,i64)=tx.query_row("SELECT (SELECT count(*) FROM session_merge_sources WHERE operation_id=?1),(SELECT count(*) FROM session_merge_assets WHERE operation_id=?1)",[&id_clone],|row|Ok((row.get(0)?,row.get(1)?)))?;
            if counts!=expected_counts {return Err(DbError::Invalid("MERGE_IMPORT_COUNT_MISMATCH".into()));}
            tx.execute("UPDATE session_merges SET snapshot_sealed=1,stage='sealed' WHERE id=?1",[&id_clone])?;
            tx.commit()?;Ok(()) }).await?;
        // Sealed SQLite rows are now the immutable authority; only failed imports need their staging file for resume.
        let directory = captured.directory;
        if let Err(error) = tokio::task::spawn_blocking(move || {
            std::fs::remove_dir_all(&directory)?;
            if let Some(parent) = directory.parent() {
                let _ = std::fs::remove_dir(parent);
            }
            Ok::<_, std::io::Error>(())
        })
        .await?
        {
            tracing::warn!(%error,"sealed merge staging cleanup deferred");
        }
        Ok(())
    }
}
fn validate_frozen_row(table: &str, values: &[SqlValue]) -> Result<(), DbError> {
    let valid = if table == "session_merge_sources" {
        matches!((&values[3],&values[4]),(SqlValue::Text(text),SqlValue::Text(hash)) if text.len()<=RECORD_BYTES && digest(text.as_bytes())==*hash)
    } else if values[4] == SqlValue::Text("copied".into()) {
        matches!((&values[6],&values[8],&values[9]),(SqlValue::Text(hash),SqlValue::Integer(size),SqlValue::Blob(bytes)) if u64::try_from(*size).ok()==u64::try_from(bytes.len()).ok() && digest(bytes)==*hash)
    } else {
        true
    };
    if valid {
        Ok(())
    } else {
        Err(DbError::Invalid("MERGE_FROZEN_ROW_MISMATCH".into()))
    }
}

fn import_row(
    conn: &mut Connection,
    table: &str,
    id: &str,
    epoch: i64,
    values: &[SqlValue],
    budget: &CaptureBudget,
) -> Result<(), DbError> {
    let tx = conn.transaction()?;
    fence(&tx, id, epoch)?;
    let key_column = if table == "session_merge_sources" {
        "source_session_id"
    } else {
        "reference"
    };
    let identity_columns = if table == "session_merge_sources" {
        "operation_id,source_session_id,ordinal,snapshot_hash"
    } else {
        "operation_id,reference,source_session_id,original_path,status,reason,sha256,mime_type,size"
    };
    let identity = if table == "session_merge_sources" {
        vec![
            values[0].clone(),
            values[1].clone(),
            values[2].clone(),
            values[4].clone(),
        ]
    } else {
        values[..9].to_vec()
    };
    let previous = tx
        .query_row(
            &format!(
                "SELECT {identity_columns} FROM {table} WHERE operation_id=?1 AND {key_column}=?2"
            ),
            rusqlite::params_from_iter([&values[0], &values[1]]),
            |row| {
                (0..identity.len())
                    .map(|index| row.get::<_, SqlValue>(index))
                    .collect::<Result<Vec<_>, _>>()
            },
        )
        .optional()?;
    if let Some(previous) = previous {
        if previous != identity {
            return Err(DbError::Invalid("MERGE_IMPORT_IDENTITY_MISMATCH".into()));
        }
    } else {
        let bytes = values
            .iter()
            .map(|value| match value {
                SqlValue::Text(text) => text.len(),
                SqlValue::Blob(bytes) => bytes.len(),
                _ => 16,
            })
            .sum::<usize>();
        MergeWriteBudget::new(&tx).reserve(bytes.saturating_add(4096))?;
        let placeholders = (1..=values.len())
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(",");
        tx.execute(
            &format!("INSERT INTO {table} VALUES({placeholders})"),
            rusqlite::params_from_iter(values),
        )?;
    }
    budget.check()?;
    tx.commit()?;
    Ok(())
}

fn catalog_fence(conn: &Connection, id: &str, epoch: i64) -> Result<(), DbError> {
    let valid:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM session_merges WHERE id=?1 AND run_epoch=?2 AND status='preparing' AND snapshot_sealed=1)",params![id,epoch],|row|row.get(0))?;
    if valid {
        Ok(())
    } else {
        Err(DbError::Conflict("MERGE_STALE_EPOCH".into()))
    }
}
impl Db {
    #[allow(
        clippy::too_many_lines,
        reason = "The derived catalog staging and bounded identity-checked import form one cancellation-fenced publication phase"
    )]
    pub(super) async fn prepare_session_merge_catalog(
        &self,
        id: &str,
        epoch: i64,
        inputs: Vec<super::MergeSummaryInput>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<(), DbError> {
        let id = id.to_owned();
        let stage_id = id.clone();
        let path = self
            .with_reader(move |conn| {
                catalog_fence(conn, &stage_id, epoch)?;
                let base = conn.path().filter(|path| !path.is_empty()).map_or_else(
                    std::env::temp_dir,
                    |path| {
                        Path::new(path)
                            .parent()
                            .unwrap_or(Path::new("."))
                            .to_owned()
                    },
                );
                let directory = base.join(format!(".zk-merge-catalog-{stage_id}"));
                private_dir(&directory)?;
                let path = directory.join(format!("{epoch}-{}.sqlite", uuid::Uuid::new_v4()));
                create_file(&path)?;
                let stage = Connection::open(&path)?;
                stage.pragma_update(None, "foreign_keys", "OFF")?;
                for table in ["session_handoff_catalog", "session_handoff_chunks"] {
                    let sql: String = conn.query_row(
                        "SELECT sql FROM sqlite_master WHERE name=?1 AND type='table'",
                        [table],
                        |row| row.get(0),
                    )?;
                    stage.execute_batch(&sql)?;
                }
                super::catalog::seal(&stage, &stage_id, inputs)?;
                drop(stage);
                std::fs::File::open(&path)
                    .map_err(io)?
                    .sync_all()
                    .map_err(io)?;
                Ok(path)
            })
            .await?;
        let temporary = TemporaryCatalog(path);
        for table in ["session_handoff_catalog", "session_handoff_chunks"] {
            let mut cursor = 0_i64;
            loop {
                if cancelled.load(Ordering::Acquire) {
                    return Err(DbError::Conflict("MERGE_WORKER_CANCELLED".into()));
                }
                let path = temporary.0.clone();
                let batch = tokio::task::spawn_blocking(
                    move || -> Result<Vec<(i64, Vec<SqlValue>)>, DbError> {
                        let conn = Connection::open_with_flags(
                            path,
                            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                        )?;
                        let mut statement = conn.prepare(&format!(
                            "SELECT rowid,* FROM {table} WHERE rowid>?1 ORDER BY rowid LIMIT 128"
                        ))?;
                        let columns = statement.column_count();
                        let mut rows = statement.query([cursor])?;
                        let mut batch = Vec::new();
                        let mut bytes = 0_usize;
                        while let Some(row) = rows.next()? {
                            let values = (1..columns)
                                .map(|index| row.get::<_, SqlValue>(index))
                                .collect::<Result<Vec<_>, _>>()?;
                            let size = values
                                .iter()
                                .map(|value| match value {
                                    SqlValue::Text(text) => text.len(),
                                    SqlValue::Blob(data) => data.len(),
                                    _ => 16,
                                })
                                .sum::<usize>();
                            if !batch.is_empty() && bytes.saturating_add(size) > 1024 * 1024 {
                                break;
                            }
                            bytes += size;
                            batch.push((row.get(0)?, values));
                        }
                        Ok(batch)
                    },
                )
                .await??;
                let Some((last, _)) = batch.last() else {
                    break;
                };
                cursor = *last;
                let id = id.clone();
                let cancelled = cancelled.clone();
                self.with_writer(move |conn| {
                    if cancelled.load(Ordering::Acquire) {
                        return Err(DbError::Conflict("MERGE_WORKER_CANCELLED".into()));
                    }
                    let tx = conn.transaction()?;
                    catalog_fence(&tx, &id, epoch)?;
                    let mut disk = MergeWriteBudget::new(&tx);
                    for (_, values) in batch {
                        let key = if table == "session_handoff_chunks" {
                            "operation_id=?1 AND reference=?2 AND start_byte=?3"
                        } else {
                            "operation_id=?1 AND reference=?2"
                        };
                        let key_length = if table == "session_handoff_chunks" {
                            3
                        } else {
                            2
                        };
                        let previous = tx
                            .query_row(
                                &format!("SELECT * FROM {table} WHERE {key}"),
                                rusqlite::params_from_iter(values[..key_length].iter()),
                                |row| {
                                    (0..values.len())
                                        .map(|index| row.get::<_, SqlValue>(index))
                                        .collect::<Result<Vec<_>, _>>()
                                },
                            )
                            .optional()?;
                        if let Some(previous) = previous {
                            if previous != values {
                                return Err(DbError::Invalid(
                                    "MERGE_CATALOG_IDENTITY_MISMATCH".into(),
                                ));
                            }
                        } else {
                            let size = values
                                .iter()
                                .map(|value| match value {
                                    SqlValue::Text(text) => text.len(),
                                    SqlValue::Blob(data) => data.len(),
                                    _ => 16,
                                })
                                .sum::<usize>();
                            disk.reserve(size.saturating_add(1024))?;
                            let placeholders = (1..=values.len())
                                .map(|n| format!("?{n}"))
                                .collect::<Vec<_>>()
                                .join(",");
                            tx.execute(
                                &format!("INSERT INTO {table} VALUES({placeholders})"),
                                rusqlite::params_from_iter(values),
                            )?;
                        }
                    }
                    if cancelled.load(Ordering::Acquire) {
                        return Err(DbError::Conflict("MERGE_WORKER_CANCELLED".into()));
                    }
                    tx.commit()?;
                    Ok(())
                })
                .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MessageRole, NewMessage, SessionMergeRequest, StoredBlock};
    #[test]
    fn inherited_snapshot_rows_are_not_hidden_inside_one_serialized_record() {
        let mut budget = CaptureBudget::new(Arc::new(AtomicBool::new(false)));
        let inherited = serde_json::json!({
            "messages":vec![serde_json::json!({"id":"message"}); METADATA_ROWS],
            "records":[]
        });
        assert!(
            matches!(budget.inherited_snapshot(&inherited), Err(DbError::Invalid(error)) if error == "MERGE_METADATA_ROW_LIMIT")
        );
    }
    async fn reserved(db: &Db) -> super::super::SessionMergeOperation {
        let a = db.create_session("fixture", "/tmp").await.unwrap();
        let b = db.create_session("fixture", "/tmp").await.unwrap();
        db.append_message(
            &a.id,
            NewMessage {
                meta: None,
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: "frozen input".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
        db.reserve_session_merge(
            uuid::Uuid::new_v4().to_string(),
            SessionMergeRequest {
                source_session_ids: vec![a.id.clone(), b.id],
                primary_session_id: a.id,
                title: None,
                model: None,
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn stopped_capture_cleanup_preserves_manifests_new_epochs_and_symlink_targets() {
        let root =
            std::env::temp_dir().join(format!("zk-capture-cleanup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let db = Db::open(root.join("data.sqlite")).unwrap();
        let op = reserved(&db).await;
        db.pause_session_merge(op.operation_id.clone(), op.run_epoch, "stopped".into())
            .await
            .unwrap();
        let paused = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        let directory = root
            .join(".data.sqlite-merge-staging")
            .join(&op.operation_id);
        let previous = directory.join(op.run_epoch.to_string());
        let current = directory.join(paused.run_epoch.to_string());
        private_dir(&previous).unwrap();
        private_dir(&current).unwrap();
        std::fs::write(previous.join("capture.sqlite"), b"partial").unwrap();
        std::fs::write(current.join("capture.sqlite"), b"new epoch").unwrap();
        let outside = root.join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("keep"), b"user data").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, previous.join("linked")).unwrap();
        db.cleanup_stopped_merge_staging(&op.operation_id, paused.run_epoch)
            .await
            .unwrap();
        assert!(!previous.exists());
        assert!(current.join("capture.sqlite").exists());
        assert_eq!(std::fs::read(outside.join("keep")).unwrap(), b"user data");
        private_dir(&previous).unwrap();
        std::fs::write(previous.join("manifest.json"), b"protected frozen manifest").unwrap();
        db.cleanup_stopped_merge_staging(&op.operation_id, paused.run_epoch)
            .await
            .unwrap();
        assert!(previous.join("manifest.json").exists());
        #[cfg(unix)]
        {
            std::fs::remove_dir_all(&previous).unwrap();
            std::os::unix::fs::symlink(&outside, &previous).unwrap();
            db.cleanup_stopped_merge_staging(&op.operation_id, paused.run_epoch)
                .await
                .unwrap();
            assert!(
                std::fs::symlink_metadata(&previous)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert!(outside.join("keep").exists());
        }
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn cancelled_capture_cannot_seal_and_explicit_resume_recaptures_unsealed_sources() {
        let db = Db::open_in_memory().unwrap();
        let op = reserved(&db).await;
        let cancelled = Arc::new(AtomicBool::new(true));
        assert!(
            db.prepare_session_merge_capture(&op.operation_id, op.run_epoch, None, cancelled)
                .await
                .is_err()
        );
        db.pause_session_merge(op.operation_id.clone(), op.run_epoch, "interrupted".into())
            .await
            .unwrap();
        let paused = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        assert!(!paused.snapshot_sealed);
        assert!(paused.locked_source_session_ids.is_empty());
        assert!(paused.run_epoch > op.run_epoch);
        let resumed = db
            .transition_session_merge(&op.operation_id, Some(paused.run_epoch), None, false)
            .await
            .unwrap();
        assert_eq!(resumed.locked_source_session_ids.len(), 2);
        db.prepare_session_merge_capture(
            &op.operation_id,
            resumed.run_epoch,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let sealed = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        assert!(sealed.snapshot_sealed);
        assert!(sealed.locked_source_session_ids.is_empty());
        assert!(
            db.prepare_session_merge_capture(
                &op.operation_id,
                resumed.run_epoch,
                None,
                Arc::new(AtomicBool::new(false))
            )
            .await
            .is_ok()
        );
    }
    #[test]
    fn metadata_limits_are_applied_before_accumulating_records() {
        let mut budget = CaptureBudget::new(Arc::new(AtomicBool::new(false)));
        assert!(budget.preflight(RECORD_BYTES + 1).is_err());
        budget.rows = METADATA_ROWS;
        assert!(budget.record(&"small").is_err());
        budget.rows = 0;
        budget.bytes = METADATA_BYTES - 1;
        assert!(budget.record(&"small").is_err());
        assert!(bounded_json(&"\\".repeat(RECORD_BYTES / 2 + 1)).is_err());
    }
    #[tokio::test]
    async fn metadata_oversize_pauses_without_sealing_or_publishing_target() {
        let db = Db::open_in_memory().unwrap();
        let op = reserved(&db).await;
        let source = op.request.primary_session_id.clone();
        // SQLite constructs the oversized value, so the capture must reject its byte length before loading it.
        db.with_writer(move |conn| {
            conn.execute(
                "UPDATE messages SET content_json=CAST(zeroblob(?1) AS TEXT) WHERE session_id=?2",
                params![i64::try_from(RECORD_BYTES + 1).unwrap(), source],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let error = db
            .prepare_session_merge_capture(
                &op.operation_id,
                op.run_epoch,
                None,
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("MERGE_RECORD_TOO_LARGE"),
            "{error}"
        );
        assert!(
            db.get_session(&op.target_session_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !db.session_merge(&op.operation_id)
                .await
                .unwrap()
                .unwrap()
                .snapshot_sealed
        );
    }

    #[tokio::test]
    async fn complete_manifest_resumes_after_import_failure_without_live_sources() {
        let db = Db::open_in_memory().unwrap();
        let op = reserved(&db).await;
        db.with_writer(|conn|{conn.execute_batch("CREATE TRIGGER reject_capture_import BEFORE INSERT ON session_merge_sources BEGIN SELECT RAISE(ABORT,'fixture import failure'); END;")?;Ok(())}).await.unwrap();
        assert!(
            db.prepare_session_merge_capture(
                &op.operation_id,
                op.run_epoch,
                None,
                Arc::new(AtomicBool::new(false))
            )
            .await
            .is_err()
        );
        let partial = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        assert!(partial.result["captureManifest"].is_string());
        assert!(!partial.snapshot_sealed);
        assert!(partial.locked_source_session_ids.is_empty());
        db.pause_session_merge(
            op.operation_id.clone(),
            op.run_epoch,
            "import failure".into(),
        )
        .await
        .unwrap();
        for source in &op.request.source_session_ids {
            db.delete_session(source).await.unwrap();
        }
        db.with_writer(|conn| {
            conn.execute_batch("DROP TRIGGER reject_capture_import;")?;
            Ok(())
        })
        .await
        .unwrap();
        let paused = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        let resumed = db
            .transition_session_merge(
                &op.operation_id,
                Some(paused.run_epoch),
                Some("another-model".into()),
                false,
            )
            .await
            .unwrap();
        assert!(resumed.locked_source_session_ids.is_empty());
        db.prepare_session_merge_capture(
            &op.operation_id,
            resumed.run_epoch,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let inputs = db
            .merge_summary_inputs(&op.operation_id, resumed.run_epoch)
            .await
            .unwrap();
        assert!(
            inputs
                .iter()
                .any(|input| input.text.contains("frozen input"))
        );
    }

    #[tokio::test]
    async fn partially_imported_capture_reopens_and_resumes_without_rewriting_imported_rows() {
        let root = std::env::temp_dir().join(format!("zk-capture-reopen-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("data.sqlite");
        let db = Db::open(&path).unwrap();
        let op = reserved(&db).await;
        db.with_writer(|conn|{conn.execute_batch("CREATE TRIGGER reject_second_source BEFORE INSERT ON session_merge_sources WHEN NEW.ordinal=1 BEGIN SELECT RAISE(ABORT,'fixture interrupted second batch'); END;")?;Ok(())}).await.unwrap();
        assert!(
            db.prepare_session_merge_capture(
                &op.operation_id,
                op.run_epoch,
                None,
                Arc::new(AtomicBool::new(false))
            )
            .await
            .is_err()
        );
        let id = op.operation_id.clone();
        let before: (i64,String)=db.with_reader(move|conn|Ok(conn.query_row("SELECT count(*),snapshot_json FROM session_merge_sources WHERE operation_id=?1",[id],|row|Ok((row.get(0)?,row.get(1)?)))?)).await.unwrap();
        assert_eq!(before.0, 1);
        assert!(
            !db.session_merge(&op.operation_id)
                .await
                .unwrap()
                .unwrap()
                .snapshot_sealed
        );
        drop(db);
        let db = Db::open(&path).unwrap();
        db.pause_session_merges_at_startup().unwrap();
        for source in &op.request.source_session_ids {
            db.delete_session(source).await.unwrap();
        }
        db.with_writer(|conn|{conn.execute_batch("DROP TRIGGER reject_second_source; CREATE TRIGGER reject_duplicate_source BEFORE INSERT ON session_merge_sources WHEN NEW.ordinal=0 BEGIN SELECT RAISE(ABORT,'completed batch must not be reinserted'); END;")?;Ok(())}).await.unwrap();
        let paused = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        let resumed = db
            .transition_session_merge(&op.operation_id, Some(paused.run_epoch), None, false)
            .await
            .unwrap();
        db.prepare_session_merge_capture(
            &op.operation_id,
            resumed.run_epoch,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let id = op.operation_id.clone();
        let (count,first):(i64,String)=db.with_reader(move|conn|Ok((
            conn.query_row("SELECT count(*) FROM session_merge_sources WHERE operation_id=?1",[&id],|row|row.get(0))?,
            conn.query_row("SELECT snapshot_json FROM session_merge_sources WHERE operation_id=?1 AND ordinal=0",[&id],|row|row.get(0))?,
        ))).await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(first, before.1);
        assert!(
            db.session_merge(&op.operation_id)
                .await
                .unwrap()
                .unwrap()
                .snapshot_sealed
        );
        assert!(
            db.merge_summary_inputs(&op.operation_id, resumed.run_epoch)
                .await
                .unwrap()
                .iter()
                .any(|input| input.text.contains("frozen input"))
        );
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn import_fixture_row(
        db: &Db,
        op: &super::super::SessionMergeOperation,
        table: &'static str,
        values: Vec<SqlValue>,
    ) -> Result<(), DbError> {
        validate_frozen_row(table, &values)?;
        let id = op.operation_id.clone();
        let epoch = op.run_epoch;
        db.with_writer(move |conn| {
            import_row(
                conn,
                table,
                &id,
                epoch,
                &values,
                &CaptureBudget::new(Arc::new(AtomicBool::new(false))),
            )
        })
        .await
    }

    async fn imported_fixture_rows(db: &Db, table: &'static str) -> Vec<Vec<SqlValue>> {
        db.with_reader(move |conn| {
            let mut statement = conn.prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))?;
            let columns = statement.column_count();
            Ok(statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| row.get(index))
                        .collect::<Result<Vec<_>, _>>()
                })?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn imported_rows_are_idempotent_and_conflicting_identity_length_or_hash_never_overwrite()
    {
        let db = Db::open_in_memory().unwrap();
        let op = reserved(&db).await;
        let payload = b"one".to_vec();
        let asset = vec![
            SqlValue::Text(op.operation_id.clone()),
            SqlValue::Text("asset:fixture".into()),
            SqlValue::Text(op.request.primary_session_id.clone()),
            SqlValue::Text("/fixture.txt".into()),
            SqlValue::Text("copied".into()),
            SqlValue::Null,
            SqlValue::Text(digest(&payload)),
            SqlValue::Text("text/plain".into()),
            SqlValue::Integer(3),
            SqlValue::Blob(payload),
        ];
        import_fixture_row(&db, &op, "session_merge_assets", asset.clone())
            .await
            .unwrap();
        import_fixture_row(&db, &op, "session_merge_assets", asset.clone())
            .await
            .unwrap();
        for kind in ["identity", "length", "hash"] {
            let mut changed = asset.clone();
            if kind == "identity" {
                changed[2] = SqlValue::Text("different-source".into());
            } else {
                let bytes = if kind == "length" {
                    b"long".to_vec()
                } else {
                    b"two".to_vec()
                };
                changed[6] = SqlValue::Text(digest(&bytes));
                changed[8] = SqlValue::Integer(i64::try_from(bytes.len()).unwrap());
                changed[9] = SqlValue::Blob(bytes);
            }
            assert!(
                matches!(import_fixture_row(&db,&op,"session_merge_assets",changed).await,Err(DbError::Invalid(error)) if error=="MERGE_IMPORT_IDENTITY_MISMATCH"),
                "{kind}"
            );
            assert_eq!(
                imported_fixture_rows(&db, "session_merge_assets").await,
                vec![asset.clone()]
            );
        }
        let mut invalid_length = asset.clone();
        invalid_length[8] = SqlValue::Integer(999);
        assert!(
            matches!(import_fixture_row(&db,&op,"session_merge_assets",invalid_length).await,Err(DbError::Invalid(error)) if error=="MERGE_FROZEN_ROW_MISMATCH")
        );
        let source = vec![
            SqlValue::Text(op.operation_id.clone()),
            SqlValue::Text(op.request.primary_session_id.clone()),
            SqlValue::Integer(0),
            SqlValue::Text("{}".into()),
            SqlValue::Text(digest(b"{}")),
        ];
        import_fixture_row(&db, &op, "session_merge_sources", source.clone())
            .await
            .unwrap();
        import_fixture_row(&db, &op, "session_merge_sources", source.clone())
            .await
            .unwrap();
        for ordinal in [false, true] {
            let mut changed = source.clone();
            if ordinal {
                changed[2] = SqlValue::Integer(1);
            } else {
                changed[3] = SqlValue::Text("{\"changed\":true}".into());
                changed[4] = SqlValue::Text(digest(b"{\"changed\":true}"));
            }
            assert!(
                matches!(import_fixture_row(&db,&op,"session_merge_sources",changed).await,Err(DbError::Invalid(error)) if error=="MERGE_IMPORT_IDENTITY_MISMATCH")
            );
            assert_eq!(
                imported_fixture_rows(&db, "session_merge_sources").await,
                vec![source.clone()]
            );
        }
    }

    #[tokio::test]
    async fn capture_reader_does_not_block_unrelated_writer_or_cancel_fence() {
        let directory =
            std::env::temp_dir().join(format!("zk-capture-writer-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let db = Db::open(directory.join("data.sqlite")).unwrap();
        let op = reserved(&db).await;
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        CAPTURE_GATES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(op.operation_id.clone(), (entered_tx, release_rx));
        let cancel = Arc::new(AtomicBool::new(false));
        let worker_db = db.clone();
        let worker_id = op.operation_id.clone();
        let worker_cancel = cancel.clone();
        let capture = tokio::spawn(async move {
            worker_db
                .prepare_session_merge_capture(&worker_id, op.run_epoch, None, worker_cancel)
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let writer = tokio::time::timeout(
            Duration::from_millis(500),
            db.create_session("unrelated", "/tmp"),
        )
        .await;
        let cancelled = tokio::time::timeout(
            Duration::from_millis(500),
            db.transition_session_merge(&op.operation_id, None, None, true),
        )
        .await;
        cancel.store(true, Ordering::Release);
        release_tx.send(()).unwrap();
        assert!(writer.unwrap().is_ok(), "capture held the shared writer");
        assert_eq!(cancelled.unwrap().unwrap().status, "cancelled");
        assert!(capture.await.unwrap().is_err());
        db.release_stopped_merge_sources(&op.operation_id)
            .await
            .unwrap();
        let ended = db.session_merge(&op.operation_id).await.unwrap().unwrap();
        assert!(!ended.snapshot_sealed);
        assert!(ended.locked_source_session_ids.is_empty());
        assert!(!ended.target_available);
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
