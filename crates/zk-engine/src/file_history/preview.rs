//! Explicit file-only rewind preview. Tokens authorize exactly the reviewed
//! checkpoint/file set; hashes and temporary content never enter a new disk log.
use super::{FileHistoryService, RewindResult, lock, resolve_prospective};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    time::{Duration, Instant},
};
use zk_tools::{
    MAX_SNAPSHOT_BYTES,
    atomic::{ExpectedOldState, sha256_hex, write_checked_bytes_authorized},
};

const TTL: Duration = Duration::from_mins(5);
const MAX_PREVIEWS: usize = 128;

/// A file and its reviewed content sizes; this is not a preview of other side effects.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RewindPreviewFile {
    /// Exact workspace-bound snapshot path.
    pub file_path: String,
    /// Current file length, absent when the file is missing.
    pub current_bytes: Option<usize>,
    /// Exact snapshot length which will be restored.
    pub restored_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn workspace() -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("zk-rewind-preview-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root.canonicalize().unwrap()
    }

    #[tokio::test]
    async fn later_write_failure_reports_applied_and_unprocessed_files_without_rollback() {
        let root = workspace();
        let locked = root.join("locked");
        std::fs::create_dir(&locked).unwrap();
        let files = vec![root.join("one"), locked.join("two"), root.join("three")];
        let db = zk_db::Db::open_in_memory().unwrap();
        let session = db
            .create_session("fixture", root.to_str().unwrap())
            .await
            .unwrap();
        for path in &files {
            std::fs::write(path, "new").unwrap();
            db.insert_file_snapshot(
                &session.id,
                Some("turn"),
                path.to_str().unwrap(),
                "old",
                "edit",
            )
            .await
            .unwrap();
        }
        let service = FileHistoryService::new(db);
        let preview = service
            .preview_rewind(
                &session.id,
                "turn",
                &files
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
        let result = service
            .confirm_rewind(&session.id, &preview.preview_token)
            .await;
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(!result.success, "{result:?}");
        assert_eq!(result.restored_files, vec![files[0].to_string_lossy()]);
        assert_eq!(result.skipped_files, vec![files[2].to_string_lossy()]);
        assert!(result.errors[0].contains(files[1].to_str().unwrap()));
        assert_eq!(std::fs::read_to_string(&files[0]).unwrap(), "old");
        for path in &files[1..] {
            assert_eq!(std::fs::read_to_string(path).unwrap(), "new");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn expired_preview_and_changed_snapshot_never_apply_files() {
        let root = workspace();
        let file = root.join("text");
        std::fs::write(&file, "current").unwrap();
        let db = zk_db::Db::open_in_memory().unwrap();
        let session = db
            .create_session("fixture", root.to_str().unwrap())
            .await
            .unwrap();
        let snapshot_id = db
            .insert_file_snapshot(
                &session.id,
                Some("turn"),
                file.to_str().unwrap(),
                "old",
                "edit",
            )
            .await
            .unwrap();
        let service = FileHistoryService::new(db.clone());
        let paths = vec![file.to_string_lossy().into_owned()];
        let preview = service
            .preview_rewind(&session.id, "turn", &paths)
            .await
            .unwrap();
        lock(&service.rewind_previews)
            .get_mut(&preview.preview_token)
            .unwrap()
            .expires = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        assert_eq!(
            service
                .confirm_rewind(&session.id, &preview.preview_token)
                .await
                .errors,
            vec!["REWIND_PREVIEW_EXPIRED"]
        );
        let preview = service
            .preview_rewind(&session.id, "turn", &paths)
            .await
            .unwrap();
        db.with_conn_blocking(|conn| {
            conn.execute(
                "UPDATE file_snapshots SET content='tampered' WHERE id=?1",
                [&snapshot_id],
            )?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            service
                .confirm_rewind(&session.id, &preview.preview_token)
                .await
                .errors,
            vec!["REWIND_SNAPSHOT_UNAVAILABLE"]
        );
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "current");
        // Replacing the checkpoint with another internally valid snapshot must
        // still fail against the frozen preview identity, independently of decoding.
        db.with_conn_blocking(|conn| {
            // The repository stores this column as BLOB; restoring a SQL TEXT
            // literal would leave an invalid fixture, not a changed checkpoint.
            conn.execute(
                "UPDATE file_snapshots SET content=?1 WHERE id=?2",
                (b"old".as_slice(), &snapshot_id),
            )?;
            Ok(())
        })
        .unwrap();
        let preview = service
            .preview_rewind(&session.id, "turn", &paths)
            .await
            .unwrap();
        db.insert_file_snapshot(
            &session.id,
            Some("turn"),
            file.to_str().unwrap(),
            "replacement",
            "edit",
        )
        .await
        .unwrap();
        db.with_conn_blocking(|conn| {
            conn.execute("DELETE FROM file_snapshots WHERE id=?1", [&snapshot_id])?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            service
                .confirm_rewind(&session.id, &preview.preview_token)
                .await
                .errors,
            vec!["REWIND_SNAPSHOT_CHANGED"]
        );
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "current");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn temporary_preview_hashes_follow_ram_lease_and_never_enter_database_or_wal() {
        let root = workspace();
        let file = root.join("text");
        let body = "unique temporary preview current bytes";
        std::fs::write(&file, body).unwrap();
        let db_path = root.join("state/data.db");
        let db = zk_db::Db::open(&db_path).unwrap();
        let (session, lease) = db
            .create_ephemeral_session("fixture", root.to_str().unwrap(), "DONT_ASK")
            .await
            .unwrap();
        db.insert_file_snapshot(
            &session,
            Some("turn"),
            file.to_str().unwrap(),
            "unique temporary snapshot old bytes",
            "edit",
        )
        .await
        .unwrap();
        let service = FileHistoryService::new(db);
        let preview = service
            .preview_rewind(&session, "turn", &[file.to_string_lossy().into_owned()])
            .await
            .unwrap();
        for path in [&db_path, &db_path.with_extension("db-wal")] {
            if let Ok(bytes) = std::fs::read(path) {
                let raw = String::from_utf8_lossy(&bytes);
                assert!(!raw.contains(body));
                assert!(!raw.contains(&sha256_hex(body.as_bytes())));
                assert!(!raw.contains(&sha256_hex(b"unique temporary snapshot old bytes")));
            }
        }
        drop(lease);
        assert_eq!(
            service
                .confirm_rewind(&session, &preview.preview_token)
                .await
                .errors,
            vec!["REWIND_TEMPORARY_CONTENT_EXPIRED"]
        );
        assert_eq!(std::fs::read_to_string(&file).unwrap(), body);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}
/// In-memory single-use proof of the chosen checkpoint and file identities.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RewindPreview {
    /// Random single-use identifier, never a content hash.
    pub preview_token: String,
    /// User-visible maximum validity period.
    pub expires_in_seconds: u64,
    /// Explicit selected files; unselected paths cannot be restored.
    pub files: Vec<RewindPreviewFile>,
}
#[derive(Deserialize, Serialize)]
struct FrozenFile {
    path: String,
    snapshot_id: String,
    snapshot_hash: String,
    current_hash: Option<String>,
}
#[derive(Deserialize, Serialize)]
struct FrozenPreview {
    message: String,
    workspace: String,
    files: Vec<FrozenFile>,
}
enum PreviewBody {
    Persistent(FrozenPreview),
    Temporary(zk_db::content::ContentRef),
}
pub(super) struct PreviewSlot {
    session: String,
    expires: Instant,
    body: PreviewBody,
}

fn read_current(path: &Path) -> Result<Option<Vec<u8>>, String> {
    use std::io::Read;
    let file = match zk_tools::safe_file::open_bound_regular(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("REWIND_FILE_UNSAFE_OR_UNREADABLE".into()),
    };
    let mut bytes = Vec::new();
    file.take((MAX_SNAPSHOT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "REWIND_FILE_UNREADABLE")?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err("REWIND_FILE_TOO_LARGE".into());
    }
    Ok(Some(bytes))
}
async fn current(path: String) -> Result<Option<Vec<u8>>, String> {
    tokio::task::spawn_blocking(move || read_current(Path::new(&path)))
        .await
        .map_err(|_| "REWIND_READ_FAILED")?
}

impl FileHistoryService {
    /// Freeze an explicit file set before user confirmation. No files are written.
    ///
    /// # Errors
    /// Invalid ownership, snapshots, paths, limits and unreadable files fail closed.
    pub async fn preview_rewind(
        &self,
        session: &str,
        message: &str,
        files: &[String],
    ) -> Result<RewindPreview, String> {
        if files.is_empty()
            || files.len() > 128
            || files.iter().collect::<HashSet<_>>().len() != files.len()
        {
            return Err("REWIND_EXPLICIT_FILES_REQUIRED".into());
        }
        let detail = self
            .db
            .get_session(session)
            .await
            .map_err(|_| "REWIND_SESSION_UNAVAILABLE")?
            .ok_or("SESSION_NOT_FOUND")?;
        let rows = self
            .db
            .list_by_message_id(session, message)
            .await
            .map_err(|_| "REWIND_SNAPSHOT_UNAVAILABLE")?;
        let mut first = BTreeMap::new();
        for row in rows {
            first.entry(row.file_path.clone()).or_insert(row);
        }
        let mut selected = Vec::new();
        let mut display = Vec::new();
        let mut total_bytes = 0usize;
        for path in files {
            let row = first.get(path).ok_or("REWIND_SNAPSHOT_NOT_FOUND")?;
            let bound = resolve_prospective(Path::new(path), &detail.working_dir)
                .map_err(|_| "REWIND_PATH_UNSAFE")?;
            if bound.to_str() != Some(path.as_str()) {
                return Err("REWIND_PATH_CHANGED".into());
            }
            let bytes = current(path.clone()).await?;
            let original = row
                .original_bytes
                .as_deref()
                .unwrap_or(row.content.as_bytes());
            if original.len() > MAX_SNAPSHOT_BYTES {
                return Err("REWIND_SNAPSHOT_TOO_LARGE".into());
            }
            total_bytes = total_bytes
                .saturating_add(original.len())
                .saturating_add(bytes.as_ref().map_or(0, Vec::len));
            if total_bytes > 64 * 1024 * 1024 {
                return Err("REWIND_BATCH_TOO_LARGE".into());
            }
            display.push(RewindPreviewFile {
                file_path: path.clone(),
                current_bytes: bytes.as_ref().map(Vec::len),
                restored_bytes: original.len(),
            });
            selected.push(FrozenFile {
                path: path.clone(),
                snapshot_id: row.id.clone(),
                snapshot_hash: sha256_hex(original),
                current_hash: bytes.as_ref().map(|bytes| sha256_hex(bytes)),
            });
        }
        let frozen = FrozenPreview {
            message: message.to_owned(),
            workspace: detail.working_dir,
            files: selected,
        };
        let token = self.remember_preview(session, frozen).await?;
        Ok(RewindPreview {
            preview_token: token,
            expires_in_seconds: TTL.as_secs(),
            files: display,
        })
    }

    async fn remember_preview(
        &self,
        session: &str,
        frozen: FrozenPreview,
    ) -> Result<String, String> {
        let retention = self
            .db
            .session_retention(session)
            .await
            .map_err(|_| "REWIND_SESSION_UNAVAILABLE")?;
        let body = if retention == zk_db::content::ContentRetention::Ephemeral {
            let bytes = serde_json::to_vec(&frozen).map_err(|_| "REWIND_PREVIEW_INVALID")?;
            PreviewBody::Temporary(
                self.db
                    .memory_content_store()
                    .put(session, &bytes)
                    .map_err(|_| "REWIND_TEMPORARY_CONTENT_UNAVAILABLE")?,
            )
        } else {
            PreviewBody::Persistent(frozen)
        };
        let token = uuid::Uuid::new_v4().to_string();
        let mut previews = lock(&self.rewind_previews);
        previews.retain(|_, slot| slot.expires > Instant::now());
        if previews.len() >= MAX_PREVIEWS
            || previews
                .values()
                .filter(|slot| slot.session == session)
                .count()
                >= 4
        {
            return Err("REWIND_PREVIEW_CAPACITY".into());
        }
        previews.insert(
            token.clone(),
            PreviewSlot {
                session: session.to_owned(),
                expires: Instant::now() + TTL,
                body,
            },
        );
        Ok(token)
    }

    /// Consume one reviewed token, check the entire batch, then use per-file CAS.
    /// The caller holds the normal session mutation reservation and idle gate.
    pub async fn confirm_rewind(&self, session: &str, token: &str) -> RewindResult {
        match self.confirm_frozen_rewind(session, token).await {
            Ok(result) => result,
            Err(code) => RewindResult::failed(code),
        }
    }
    fn consume_preview(&self, session: &str, token: &str) -> Result<FrozenPreview, String> {
        let slot = {
            let mut previews = lock(&self.rewind_previews);
            let slot = previews.get(token).ok_or("REWIND_PREVIEW_EXPIRED")?;
            if slot.session != session {
                return Err("REWIND_PREVIEW_NOT_FOUND".into());
            }
            previews.remove(token).ok_or("REWIND_PREVIEW_EXPIRED")?
        };
        if slot.expires <= Instant::now() {
            return Err("REWIND_PREVIEW_EXPIRED".into());
        }
        let frozen = match slot.body {
            PreviewBody::Persistent(frozen) => frozen,
            PreviewBody::Temporary(reference) => {
                let bytes = self
                    .db
                    .memory_content_store()
                    .get(session, &reference)
                    .map_err(|_| "REWIND_TEMPORARY_CONTENT_EXPIRED")?;
                serde_json::from_slice(&bytes).map_err(|_| "REWIND_PREVIEW_INVALID")?
            }
        };
        Ok(frozen)
    }

    async fn confirm_frozen_rewind(
        &self,
        session: &str,
        token: &str,
    ) -> Result<RewindResult, String> {
        let frozen = self.consume_preview(session, token)?;
        let detail = self
            .db
            .get_session(session)
            .await
            .map_err(|_| "REWIND_SESSION_UNAVAILABLE")?
            .ok_or("SESSION_NOT_FOUND")?;
        if detail.working_dir != frozen.workspace {
            return Err("REWIND_WORKSPACE_CHANGED".into());
        }
        let rows = self
            .db
            .list_by_message_id(session, &frozen.message)
            .await
            .map_err(|_| "REWIND_SNAPSHOT_UNAVAILABLE")?;
        let mut batch = Vec::new();
        for selected in frozen.files {
            let path = resolve_prospective(Path::new(&selected.path), &frozen.workspace)
                .map_err(|_| "REWIND_PATH_UNSAFE")?;
            if path.to_str() != Some(selected.path.as_str()) {
                return Err("REWIND_PATH_CHANGED".into());
            }
            let row = rows
                .iter()
                .find(|row| row.id == selected.snapshot_id && row.file_path == selected.path)
                .ok_or("REWIND_SNAPSHOT_CHANGED")?;
            let original = row
                .original_bytes
                .as_deref()
                .unwrap_or(row.content.as_bytes());
            if sha256_hex(original) != selected.snapshot_hash {
                return Err("REWIND_SNAPSHOT_CHANGED".into());
            }
            let now = current(selected.path.clone()).await?;
            if now.as_ref().map(|bytes| sha256_hex(bytes)) != selected.current_hash {
                return Err("REWIND_FILE_CHANGED".into());
            }
            batch.push((selected, now, original.to_vec()));
        }
        let checkpoint = format!("rewind_{}", uuid::Uuid::new_v4());
        let mut result = RewindResult::default();
        // Finish all durable/RAM backups before the first external file change.
        for (selected, now, _) in &batch {
            if let Some(bytes) = now.as_deref() {
                // Preserve exact current bytes even when they are not valid UTF-8.
                self.db
                    .insert_file_snapshot_with_bytes(
                        session,
                        Some(&checkpoint),
                        &selected.path,
                        &String::from_utf8_lossy(bytes),
                        "rewind",
                        Some(bytes),
                    )
                    .await
                    .map_err(|_| "REWIND_BACKUP_FAILED")?;
            }
        }
        let mut writes = batch.into_iter();
        while let Some((selected, _, original)) = writes.next() {
            let expected = selected
                .current_hash
                .map_or(ExpectedOldState::Absent, ExpectedOldState::Sha256);
            let path = Path::new(&selected.path);
            let outcome =
                write_checked_bytes_authorized(path, &original, &expected, Some(path)).await;
            if !outcome.success {
                let code = if outcome.effect == zk_tools::atomic::WriteEffect::NotStarted {
                    "REWIND_FILE_CONFLICT_OR_WRITE_FAILED"
                } else {
                    "REWIND_WRITE_EFFECT_UNCERTAIN"
                };
                result.errors.push(format!("{}: {code}", selected.path));
                result
                    .skipped_files
                    .extend(writes.map(|(remaining, _, _)| remaining.path));
                // A new preview is required; never replay already restored files.
                break;
            }
            zk_tools::file_state::global().mark_modified(session, &selected.path);
            result.restored_files.push(selected.path);
        }
        result.success = result.errors.is_empty();
        Ok(result)
    }
}
