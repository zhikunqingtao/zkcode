//! Persistent Browser Replay timeline.
//!
//! Each session is stored as one atomic JSON array below the authorized workspace's
//! `.zk/browser-replay` directory. Filenames are SHA-256 digests of session ids, so
//! caller-controlled ids cannot escape the store. The store bounds frames, files,
//! age, and bytes; screenshots are discarded before semantic data when necessary.

use std::fs::{self, OpenOptions};
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::error::ApiError;
use crate::state::AppState;

const MAX_SESSION_FILES: usize = 100;
const MAX_FRAMES_PER_SESSION: usize = 100;
const MAX_REPLAY_BYTES: usize = 2 * 1024 * 1024;
const RETENTION: Duration = Duration::from_hours(168);

pub struct BrowserReplayStore {
    root: PathBuf,
    io_lock: Mutex<()>,
}

impl BrowserReplayStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            io_lock: Mutex::new(()),
        }
    }

    /// Replace a session timeline with caller-supplied JSON. Production snapshots
    /// normally use [`Self::append_snapshot`].
    pub fn insert(&self, id: &str, data: &Value) -> std::io::Result<()> {
        let _guard = self.io_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.prepare_and_prune()?;
        let encoded = serde_json::to_vec(data).map_err(Error::other)?;
        if encoded.len() > MAX_REPLAY_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "browser replay exceeds the 2MiB session limit",
            ));
        }
        atomic_write(&self.path_for(id), &encoded)
    }

    /// Append one semantic snapshot, retaining the newest bounded timeline.
    pub fn append_snapshot(&self, id: &str, mut snapshot: Value) -> std::io::Result<()> {
        let _guard = self.io_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.prepare_and_prune()?;
        let path = self.path_for(id);
        let mut frames = read_json(&path)?
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        frames.push(snapshot.clone());
        if frames.len() > MAX_FRAMES_PER_SESSION {
            frames.drain(..frames.len() - MAX_FRAMES_PER_SESSION);
        }

        let mut encoded = serde_json::to_vec(&frames).map_err(Error::other)?;
        if encoded.len() > MAX_REPLAY_BYTES {
            if let Some(object) = snapshot.as_object_mut() {
                object.insert("screenshotBase64".to_owned(), Value::Null);
            }
            if let Some(last) = frames.last_mut() {
                *last = snapshot;
            }
            encoded = serde_json::to_vec(&frames).map_err(Error::other)?;
        }
        while encoded.len() > MAX_REPLAY_BYTES && frames.len() > 1 {
            frames.remove(0);
            encoded = serde_json::to_vec(&frames).map_err(Error::other)?;
        }
        if encoded.len() > MAX_REPLAY_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "browser replay frame exceeds the 2MiB session limit",
            ));
        }
        atomic_write(&path, &encoded)
    }

    /// Normalize a Python semantic-snapshot response to the frontend contract,
    /// persist it, and return the stored frame.
    pub fn append_python_snapshot(&self, id: &str, data: &Value) -> std::io::Result<Value> {
        let snapshot = json!({
            "snapshotId": uuid::Uuid::new_v4().to_string(),
            "sessionId": id,
            "capturedAt": data.get("timestamp").cloned()
                .unwrap_or_else(|| json!(crate::iso::now_millis().to_string())),
            "url": data.get("url").cloned().unwrap_or(Value::Null),
            "title": data.get("title").cloned().unwrap_or(Value::Null),
            "selector": data.get("selector").cloned().unwrap_or(Value::Null),
            "nodeCount": data.get("node_count").or_else(|| data.get("nodeCount"))
                .cloned().unwrap_or(json!(0)),
            "interactive": data.get("interactive").cloned().unwrap_or_else(|| json!([])),
            "tree": data.get("tree").cloned().unwrap_or(Value::Null),
            "screenshotBase64": data.get("screenshot_base64")
                .or_else(|| data.get("screenshotBase64"))
                .cloned().unwrap_or(Value::Null),
        });
        self.append_snapshot(id, snapshot.clone())?;
        Ok(snapshot)
    }

    pub fn get(&self, id: &str) -> std::io::Result<Option<Value>> {
        let _guard = self.io_lock.lock().unwrap_or_else(PoisonError::into_inner);
        read_json(&self.path_for(id))
    }

    pub fn remove(&self, id: &str) -> std::io::Result<bool> {
        let _guard = self.io_lock.lock().unwrap_or_else(PoisonError::into_inner);
        match fs::remove_file(self.path_for(id)) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        replay_files(&self.root).map_or(0, |files| files.len())
    }

    fn path_for(&self, id: &str) -> PathBuf {
        let digest = format!("{:x}", Sha256::digest(id.as_bytes()));
        self.root.join(format!("{digest}.json"))
    }

    fn prepare_and_prune(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.root)?;
        set_private_dir_permissions(&self.root)?;
        let now = SystemTime::now();
        let mut files = replay_files(&self.root)?;
        for (path, modified) in &files {
            if now
                .duration_since(*modified)
                .is_ok_and(|age| age > RETENTION)
            {
                fs::remove_file(path).ok();
            }
        }
        files = replay_files(&self.root)?;
        files.sort_by_key(|(_, modified)| *modified);
        let excess = files.len().saturating_sub(MAX_SESSION_FILES - 1);
        for (path, _) in files.into_iter().take(excess) {
            fs::remove_file(path).ok();
        }
        Ok(())
    }
}

fn read_json(path: &Path) -> std::io::Result<Option<Value>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "invalid replay file type",
        ));
    }
    if metadata.len() > u64::try_from(MAX_REPLAY_BYTES).unwrap_or(u64::MAX) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "browser replay file is too large",
        ));
    }
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

fn replay_files(root: &Path) -> std::io::Result<Vec<(PathBuf, SystemTime)>> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    Ok(entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                return None;
            }
            let metadata = entry.metadata().ok()?;
            if !metadata.is_file() {
                return None;
            }
            Some((path, metadata.modified().ok()?))
        })
        .collect())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "replay path has no parent"))?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".replay-{}-{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        set_private_file_permissions(&temp)?;
        fs::rename(&temp, path)
    })();
    if result.is_err() {
        fs::remove_file(&temp).ok();
    }
    result
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

pub(crate) async fn get_replay(
    State(state): State<AppState>,
    AxumPath(replay_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    match state.browser_replay.get(&replay_id) {
        Ok(Some(data)) => Ok(Json(data)),
        Ok(None) => Err(ApiError::not_found(
            "REPLAY_NOT_FOUND",
            &format!("Browser replay not found: {replay_id}"),
        )),
        Err(error) => {
            tracing::error!(error_type = ?error.kind(), "browser replay read failed");
            Err(ApiError::internal())
        }
    }
}

pub(crate) async fn delete_replay(
    State(state): State<AppState>,
    AxumPath(replay_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    match state.browser_replay.remove(&replay_id) {
        Ok(true) => Ok(Json(json!({ "status": "deleted", "replayId": replay_id }))),
        Ok(false) => Err(ApiError::not_found(
            "REPLAY_NOT_FOUND",
            &format!("Browser replay not found: {replay_id}"),
        )),
        Err(error) => {
            tracing::error!(error_type = ?error.kind(), "browser replay delete failed");
            Err(ApiError::internal())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::BrowserReplayStore;

    fn temp_root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("zk-browser-replay-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn atomic_store_survives_restart_and_remove() {
        let root = temp_root();
        let store = BrowserReplayStore::new(&root);
        store
            .insert("r1", &json!([{"url": "https://example.com"}]))
            .expect("insert");
        assert_eq!(store.len(), 1);

        let restarted = BrowserReplayStore::new(&root);
        assert_eq!(
            restarted.get("r1").expect("read"),
            Some(json!([{"url": "https://example.com"}]))
        );
        assert!(restarted.remove("r1").expect("remove"));
        assert!(!restarted.remove("r1").expect("remove missing"));
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn append_bounds_frames_and_omits_oversized_screenshot() {
        let root = temp_root();
        let store = BrowserReplayStore::new(&root);
        for index in 0..105 {
            store
                .append_snapshot(
                    "session-a",
                    json!({"snapshotId": index, "screenshotBase64": "x"}),
                )
                .expect("append");
        }
        let frames = store
            .get("session-a")
            .expect("read")
            .and_then(|value| value.as_array().cloned())
            .expect("array");
        assert_eq!(frames.len(), 100);
        assert_eq!(frames[0]["snapshotId"], 5);

        store
            .append_snapshot(
                "large",
                json!({"snapshotId": "large", "screenshotBase64": "x".repeat(3 * 1024 * 1024)}),
            )
            .expect("semantic frame fits after screenshot removal");
        assert!(store.get("large").expect("read").expect("value")[0]["screenshotBase64"].is_null());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn python_snapshot_is_normalized_to_frontend_contract() {
        let root = temp_root();
        let store = BrowserReplayStore::new(&root);
        let frame = store
            .append_python_snapshot(
                "session-b",
                &json!({
                    "timestamp": "2026-08-22T00:00:00Z",
                    "url": "https://example.test",
                    "title": "Example",
                    "node_count": 7,
                    "interactive": [{"role": "button", "name": "Run"}],
                    "tree": {"aria": "- button Run"}
                }),
            )
            .expect("normalize and append");
        assert_eq!(frame["sessionId"], "session-b");
        assert_eq!(frame["nodeCount"], 7);
        assert!(
            frame["snapshotId"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
        );
        assert_eq!(
            store.get("session-b").expect("read").expect("value")[0],
            frame
        );
        fs::remove_dir_all(root).ok();
    }
}
