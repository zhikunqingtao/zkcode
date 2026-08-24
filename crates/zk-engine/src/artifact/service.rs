//! 产物清单服务（内存 `DashMap` + `~/.zk/artifacts/{run_id}.json` 落盘）。
//!
//! 语义来源（旧仓库只读）：`artifact/ArtifactManifestService.java` 与其
//! `ArtifactManifestRepository`。
//!
//! # 有意差异
//!
//! - 旧端以 SQLite 双表（`artifact_manifest` / `artifact_manifest_entry`）
//!   持久化，并有 `declared` → `sealed` 两阶段状态机（先登记路径、回合末尾
//!   再补算 hash）。本移植改为「内存权威 + 一 run 一 JSON 文件」：登记即
//!   同步算 hash，不保留 `declared` 中间态。理由：Rust 侧工具执行完即知文件
//!   终态，两阶段只会引入一个必须由引擎主循环驱动的收尾钩子，而本批被约束
//!   为「不触碰 engine.rs 主循环」。
//! - 旧端失败码词汇表与状态判定矩阵逐字保留（见 [`super::manifest`]）。
//! - IO 失败一律只 `warn`/`error` 不上抛（对照旧仓库落盘失败不阻断工具执行），
//!   内存清单仍然可读。

use std::path::{Path, PathBuf};

use dashmap::DashMap;
use sha2::{Digest, Sha256};
use zk_db::time::{format_rfc3339_micros, now_millis};

use super::manifest::{
    ArtifactAction, ArtifactEntry, ArtifactManifest, MAX_VERIFICATION_BYTES, VerificationFailure,
    VerificationResult,
};

/// 落盘子目录名（`~/.zk/artifacts/`）。
pub const ARTIFACT_DIR_NAME: &str = "artifacts";

/// 清单文件后缀。
pub const ARTIFACT_FILE_SUFFIX: &str = ".json";

/// `run_id` 不过路径守卫（含 `..` / 路径分隔符 / 空白）。
///
/// 落盘被跳过，内存清单不受影响。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidRunId;

/// 产物清单服务。
#[derive(Debug)]
pub struct ArtifactManifestService {
    /// `run_id` → 清单（权威副本）。
    manifests: DashMap<String, ArtifactManifest>,
    /// 落盘目录。
    dir: PathBuf,
}

impl Default for ArtifactManifestService {
    fn default() -> Self {
        Self::new()
    }
}

impl ArtifactManifestService {
    /// 用户级默认目录 `~/.zk/artifacts/`。
    #[must_use]
    pub fn new() -> Self {
        Self::with_dir(zk_core::paths::user_config_dir().join(ARTIFACT_DIR_NAME))
    }

    /// 指定落盘目录（测试与自定义部署用）。
    ///
    /// 建目录失败只 `warn`（对照 `SessionSnapshotService::with_dir`）：落盘能力
    /// 随后整体不可用，但内存清单与服务本身照常工作。
    #[must_use]
    pub fn with_dir(dir: PathBuf) -> Self {
        match std::fs::create_dir_all(&dir) {
            Ok(()) => tracing::debug!(dir = %dir.display(), "Artifact manifest directory ready"),
            Err(error) => tracing::warn!(
                dir = %dir.display(),
                %error,
                "Failed to create artifact directory. Manifest persistence unavailable."
            ),
        }
        Self {
            manifests: DashMap::new(),
            dir,
        }
    }

    /// 落盘目录。
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 登记一次文件变更（工具执行后调用）。
    ///
    /// 同路径重复登记按 [`ArtifactManifest::upsert`] 规则合并。返回本次登记
    /// 生成的条目（已含 size / hash 快照）。
    pub async fn record_file_change(
        &self,
        run_id: &str,
        path: &Path,
        action: ArtifactAction,
    ) -> ArtifactEntry {
        self.record(run_id, None, path, action).await
    }

    /// 登记一次文件变更并绑定会话（供 workbench 按会话聚合）。
    pub async fn record_file_change_for_session(
        &self,
        run_id: &str,
        session_id: &str,
        path: &Path,
        action: ArtifactAction,
    ) -> ArtifactEntry {
        self.record(run_id, Some(session_id.to_owned()), path, action)
            .await
    }

    async fn record(
        &self,
        run_id: &str,
        session_id: Option<String>,
        path: &Path,
        action: ArtifactAction,
    ) -> ArtifactEntry {
        let now = format_rfc3339_micros(now_millis());
        let (size, hash) = if action.expects_present() {
            snapshot_file(path).await
        } else {
            (0, None)
        };
        let entry = ArtifactEntry {
            path: path.to_string_lossy().into_owned(),
            action,
            size,
            hash,
            recorded_at: now.clone(),
        };

        let snapshot = {
            let mut manifest = self
                .manifests
                .entry(run_id.to_owned())
                .or_insert_with(|| ArtifactManifest::new(run_id, session_id.clone(), now));
            if manifest.session_id.is_none() {
                manifest.session_id = session_id;
            }
            manifest.upsert(entry.clone());
            manifest.clone()
        };
        self.persist(&snapshot).await;
        entry
    }

    /// 取某 run 的完整清单（内存缺失时回落磁盘并回填）。
    pub async fn get_manifest(&self, run_id: &str) -> Option<ArtifactManifest> {
        if let Some(found) = self.manifests.get(run_id) {
            return Some(found.clone());
        }
        let loaded = self.load(run_id).await?;
        self.manifests
            .entry(run_id.to_owned())
            .or_insert_with(|| loaded.clone());
        Some(loaded)
    }

    /// 重算 hash 校验清单完整性。
    ///
    /// 清单不存在 → `None`（端点侧映射 404）。
    pub async fn verify_manifest(&self, run_id: &str) -> Option<VerificationResult> {
        let manifest = self.get_manifest(run_id).await?;
        let mut verified = 0usize;
        let mut failed = 0usize;
        let mut unverified = 0usize;
        let mut failures = Vec::new();

        for entry in &manifest.files {
            match verify_entry(entry).await {
                EntryVerdict::Verified => verified += 1,
                EntryVerdict::Failed(failure) => {
                    failed += 1;
                    failures.push(failure);
                }
                EntryVerdict::Unverified(failure) => {
                    unverified += 1;
                    failures.push(failure);
                }
            }
        }
        Some(VerificationResult::new(
            run_id, verified, failed, unverified, failures,
        ))
    }

    /// 某会话下的全部清单（内存视图；按 `created_at` 升序）。
    #[must_use]
    pub fn manifests_for_session(&self, session_id: &str) -> Vec<ArtifactManifest> {
        let mut found: Vec<ArtifactManifest> = self
            .manifests
            .iter()
            .filter(|item| item.session_id.as_deref() == Some(session_id))
            .map(|item| item.clone())
            .collect();
        found.sort_by(|left, right| left.created_at.cmp(&right.created_at));
        found
    }

    /// 内存中登记过的 run 数（可观测/测试用）。
    #[must_use]
    pub fn tracked_runs(&self) -> usize {
        self.manifests.len()
    }

    fn manifest_file(&self, run_id: &str) -> Result<PathBuf, InvalidRunId> {
        validate_run_id(run_id)?;
        Ok(self.dir.join(format!("{run_id}{ARTIFACT_FILE_SUFFIX}")))
    }

    async fn persist(&self, manifest: &ArtifactManifest) {
        let Ok(file) = self.manifest_file(&manifest.run_id) else {
            tracing::warn!(
                run_id = %manifest.run_id,
                "Rejected artifact manifest run id; keeping in-memory copy only"
            );
            return;
        };
        match serde_json::to_vec_pretty(manifest) {
            Ok(bytes) => {
                if let Err(error) = replace_file(&file, &bytes).await {
                    tracing::error!(
                        run_id = %manifest.run_id,
                        %error,
                        "Failed to persist artifact manifest"
                    );
                }
            }
            Err(error) => tracing::error!(
                run_id = %manifest.run_id,
                %error,
                "Failed to serialize artifact manifest"
            ),
        }
    }

    async fn load(&self, run_id: &str) -> Option<ArtifactManifest> {
        let file = self.manifest_file(run_id).ok()?;
        let bytes = match tokio::fs::read(&file).await {
            Ok(bytes) => bytes,
            Err(error) => {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::error!(run_id, %error, "Failed to read artifact manifest");
                }
                return None;
            }
        };
        match serde_json::from_slice::<ArtifactManifest>(&bytes) {
            Ok(manifest) => Some(manifest),
            Err(error) => {
                tracing::error!(run_id, %error, "Failed to parse artifact manifest");
                None
            }
        }
    }
}

/// 单条目校验结论。
enum EntryVerdict {
    Verified,
    Failed(VerificationFailure),
    Unverified(VerificationFailure),
}

async fn verify_entry(entry: &ArtifactEntry) -> EntryVerdict {
    let path = Path::new(&entry.path);
    if !entry.action.expects_present() {
        return if tokio::fs::symlink_metadata(path).await.is_ok() {
            EntryVerdict::Failed(VerificationFailure::new(
                &entry.path,
                "DELETE_TARGET_STILL_EXISTS",
                "Artifact was recorded as deleted but still exists on disk",
            ))
        } else {
            EntryVerdict::Verified
        };
    }

    let Some(expected) = entry.hash.as_deref() else {
        return EntryVerdict::Unverified(VerificationFailure::new(
            &entry.path,
            "SEALED_HASH_MISSING",
            "No recorded hash for artifact; integrity cannot be verified",
        ));
    };

    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) => {
            return EntryVerdict::Failed(VerificationFailure::new(
                &entry.path,
                "VERIFICATION_ERROR",
                format!("Failed to stat artifact: {error}"),
            ));
        }
    };
    if !metadata.is_file() {
        return EntryVerdict::Failed(VerificationFailure::new(
            &entry.path,
            "ARTIFACT_NOT_REGULAR_FILE",
            "Artifact path is not a regular file",
        ));
    }
    if metadata.len() > MAX_VERIFICATION_BYTES {
        return EntryVerdict::Unverified(VerificationFailure::new(
            &entry.path,
            "VERIFICATION_SIZE_LIMIT",
            format!(
                "Artifact exceeds verification size limit ({} > {MAX_VERIFICATION_BYTES} bytes)",
                metadata.len()
            ),
        ));
    }

    match hash_file(path).await {
        Ok(actual) if actual == expected => EntryVerdict::Verified,
        Ok(actual) => EntryVerdict::Failed(VerificationFailure::new(
            &entry.path,
            "HASH_MISMATCH",
            format!("Expected sha256 {expected} but found {actual}"),
        )),
        Err(error) => EntryVerdict::Failed(VerificationFailure::new(
            &entry.path,
            "VERIFICATION_ERROR",
            format!("Failed to read artifact: {error}"),
        )),
    }
}

/// 采集文件当前 size + sha256（不可读 / 非普通文件 / 超限 → hash 为 `None`）。
async fn snapshot_file(path: &Path) -> (u64, Option<String>) {
    let Ok(metadata) = tokio::fs::metadata(path).await else {
        return (0, None);
    };
    if !metadata.is_file() {
        return (metadata.len(), None);
    }
    if metadata.len() > MAX_VERIFICATION_BYTES {
        return (metadata.len(), None);
    }
    match hash_file(path).await {
        Ok(hash) => (metadata.len(), Some(hash)),
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "Failed to hash artifact");
            (metadata.len(), None)
        }
    }
}

/// sha256 十六进制小写（对照旧 `HexFormat.of().formatHex`）。
async fn hash_file(path: &Path) -> Result<String, std::io::Error> {
    let bytes = tokio::fs::read(path).await?;
    let digest = Sha256::digest(&bytes);
    Ok(format!("{digest:x}"))
}

/// tmp + rename 原子替换（同 `session_snapshot::replace_file`）。
async fn replace_file(file: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let mut temp = file.to_path_buf().into_os_string();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, file).await
}

/// 文件名安全守卫。
fn validate_run_id(run_id: &str) -> Result<(), InvalidRunId> {
    if run_id.trim().is_empty()
        || run_id.contains("..")
        || run_id.contains('/')
        || run_id.contains('\\')
    {
        return Err(InvalidRunId);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "zk-artifact-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn service(tag: &str) -> (ArtifactManifestService, PathBuf) {
        let root = temp_dir(tag);
        let service = ArtifactManifestService::with_dir(root.join("manifests"));
        (service, root)
    }

    #[tokio::test]
    async fn record_created_captures_size_and_hash() {
        let (service, root) = service("record");
        let file = root.join("a.txt");
        tokio::fs::write(&file, b"hello").await.expect("write");

        let entry = service
            .record_file_change("run-1", &file, ArtifactAction::Created)
            .await;

        assert_eq!(entry.action, ArtifactAction::Created);
        assert_eq!(entry.size, 5);
        // sha256("hello")
        assert_eq!(
            entry.hash.as_deref(),
            Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        );
    }

    #[tokio::test]
    async fn deleted_entry_carries_no_hash() {
        let (service, root) = service("deleted");
        let file = root.join("gone.txt");
        let entry = service
            .record_file_change("run-1", &file, ArtifactAction::Deleted)
            .await;
        assert_eq!(entry.size, 0);
        assert!(entry.hash.is_none());
    }

    #[tokio::test]
    async fn manifest_persists_and_reloads_from_disk() {
        let (service, root) = service("persist");
        let file = root.join("b.txt");
        tokio::fs::write(&file, b"payload").await.expect("write");
        service
            .record_file_change_for_session("run-2", "sess-1", &file, ArtifactAction::Modified)
            .await;

        let reopened = ArtifactManifestService::with_dir(service.dir().to_path_buf());
        assert_eq!(reopened.tracked_runs(), 0);
        let manifest = reopened.get_manifest("run-2").await.expect("manifest");
        assert_eq!(manifest.run_id, "run-2");
        assert_eq!(manifest.session_id.as_deref(), Some("sess-1"));
        assert_eq!(manifest.files.len(), 1);
        // 磁盘回落后回填内存。
        assert_eq!(reopened.tracked_runs(), 1);
    }

    #[tokio::test]
    async fn missing_manifest_is_none() {
        let (service, _root) = service("missing");
        assert!(service.get_manifest("nope").await.is_none());
        assert!(service.verify_manifest("nope").await.is_none());
    }

    #[tokio::test]
    async fn unsafe_run_id_keeps_memory_only() {
        let (service, root) = service("unsafe-id");
        let file = root.join("c.txt");
        tokio::fs::write(&file, b"x").await.expect("write");
        service
            .record_file_change("../escape", &file, ArtifactAction::Created)
            .await;

        assert!(service.get_manifest("../escape").await.is_some());
        let mut entries = tokio::fs::read_dir(service.dir()).await.expect("read dir");
        assert!(entries.next_entry().await.expect("entry").is_none());
    }

    #[tokio::test]
    async fn verify_passes_for_untouched_files() {
        let (service, root) = service("verify-ok");
        let file = root.join("d.txt");
        tokio::fs::write(&file, b"stable").await.expect("write");
        service
            .record_file_change("run-3", &file, ArtifactAction::Created)
            .await;

        let result = service.verify_manifest("run-3").await.expect("result");
        assert_eq!(result.status, "verified");
        assert_eq!(result.verified, 1);
        assert_eq!(result.total, 1);
        assert!(result.passed());
        assert!(result.failures.is_empty());
    }

    #[tokio::test]
    async fn verify_detects_hash_mismatch() {
        let (service, root) = service("verify-mismatch");
        let file = root.join("e.txt");
        tokio::fs::write(&file, b"before").await.expect("write");
        service
            .record_file_change("run-4", &file, ArtifactAction::Created)
            .await;
        tokio::fs::write(&file, b"after").await.expect("rewrite");

        let result = service.verify_manifest("run-4").await.expect("result");
        assert_eq!(result.status, "failed");
        assert_eq!(result.failed, 1);
        assert!(!result.passed());
        assert_eq!(result.failures[0].code, "HASH_MISMATCH");
    }

    #[tokio::test]
    async fn verify_detects_vanished_file() {
        let (service, root) = service("verify-gone");
        let file = root.join("f.txt");
        tokio::fs::write(&file, b"temp").await.expect("write");
        service
            .record_file_change("run-5", &file, ArtifactAction::Created)
            .await;
        tokio::fs::remove_file(&file).await.expect("remove");

        let result = service.verify_manifest("run-5").await.expect("result");
        assert_eq!(result.failed, 1);
        assert_eq!(result.failures[0].code, "VERIFICATION_ERROR");
    }

    #[tokio::test]
    async fn verify_rejects_resurrected_delete_target() {
        let (service, root) = service("verify-resurrect");
        let file = root.join("g.txt");
        service
            .record_file_change("run-6", &file, ArtifactAction::Deleted)
            .await;
        tokio::fs::write(&file, b"back").await.expect("write");

        let result = service.verify_manifest("run-6").await.expect("result");
        assert_eq!(result.failed, 1);
        assert_eq!(result.failures[0].code, "DELETE_TARGET_STILL_EXISTS");
    }

    #[tokio::test]
    async fn verify_reports_partial_when_mixed() {
        let (service, root) = service("verify-partial");
        let good = root.join("good.txt");
        let bad = root.join("bad.txt");
        tokio::fs::write(&good, b"g").await.expect("write");
        tokio::fs::write(&bad, b"b").await.expect("write");
        service
            .record_file_change("run-7", &good, ArtifactAction::Created)
            .await;
        service
            .record_file_change("run-7", &bad, ArtifactAction::Created)
            .await;
        tokio::fs::write(&bad, b"changed").await.expect("rewrite");

        let result = service.verify_manifest("run-7").await.expect("result");
        assert_eq!(result.status, "partial");
        assert_eq!(result.verified, 1);
        assert_eq!(result.failed, 1);
        assert_eq!(result.total, 2);
    }

    #[tokio::test]
    async fn session_filter_sorts_by_creation() {
        let (service, root) = service("session-filter");
        let file = root.join("h.txt");
        tokio::fs::write(&file, b"h").await.expect("write");
        service
            .record_file_change_for_session("run-a", "sess-x", &file, ArtifactAction::Created)
            .await;
        service
            .record_file_change_for_session("run-b", "sess-x", &file, ArtifactAction::Modified)
            .await;
        service
            .record_file_change_for_session("run-c", "sess-y", &file, ArtifactAction::Modified)
            .await;

        let found = service.manifests_for_session("sess-x");
        assert_eq!(found.len(), 2);
        assert!(found[0].created_at <= found[1].created_at);
        assert_eq!(service.manifests_for_session("sess-y").len(), 1);
        assert!(service.manifests_for_session("sess-none").is_empty());
    }

    #[tokio::test]
    async fn repeat_record_merges_into_single_entry() {
        let (service, root) = service("merge");
        let file = root.join("i.txt");
        tokio::fs::write(&file, b"one").await.expect("write");
        service
            .record_file_change("run-8", &file, ArtifactAction::Created)
            .await;
        tokio::fs::write(&file, b"two").await.expect("rewrite");
        service
            .record_file_change("run-8", &file, ArtifactAction::Modified)
            .await;

        let manifest = service.get_manifest("run-8").await.expect("manifest");
        assert_eq!(manifest.files.len(), 1);
        // 先建后改仍视为本 run 新增，但 hash 已刷新到最新内容。
        assert_eq!(manifest.files[0].action, ArtifactAction::Created);
        assert_eq!(manifest.files[0].size, 3);
        let verified = service.verify_manifest("run-8").await.expect("result");
        assert!(verified.passed());
    }

    #[test]
    fn run_id_guard_rejects_traversal() {
        assert!(validate_run_id("run-1").is_ok());
        assert_eq!(validate_run_id("  "), Err(InvalidRunId));
        assert_eq!(validate_run_id("../x"), Err(InvalidRunId));
        assert_eq!(validate_run_id("a/b"), Err(InvalidRunId));
        assert_eq!(validate_run_id("a\\b"), Err(InvalidRunId));
    }
}
