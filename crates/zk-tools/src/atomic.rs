//! CAS 原子写入器——「预期旧状态校验 → 同目录临时文件 + fsync → 原子 rename
//! → 回读校验」四步流程。
//!
//! 逐字对照旧 `tool/impl/AtomicFileWriter.java`（只读权威规格）：
//!
//! - `ExpectedOldState` 必填，`Absent` / `Sha256(64-hex)` 两态；
//! - 同路径写入经 `ManagedPathLockManager.withLock` 串行化；
//! - 临时文件建在**目标同目录**（保证 rename 同设备）、`FileChannel.force(true)`
//!   落盘后才 move；
//! - `Files.move(ATOMIC_MOVE, REPLACE_EXISTING)`——**故意不提供非原子降级**；
//! - move 后重读磁盘比对 SHA-256，不等即 `WRITE_VERIFY_FAILED`（已 move，
//!   故错误前缀 `POST_MOVE_FAILURE:`、effect 为 `UNKNOWN`）；
//! - 父目录 fsync 为 best-effort，失败不影响成功判定。
//!
//! 差异（留痕 docs/compatibility.md §9）：
//!
//! - 旧 `PathSecurityService` / `ManagedWorkspacePathResolver` 的三次路径复检
//!   （pre / before-move / after-move）未移植——路径准入面由 2.5 的
//!   `ToolExecutionGateway` 在工具**之前**承担，本 crate 不重复实现；对应地
//!   `PRE_MOVE_SECURITY_DENIED` / `POST_MOVE_SECURITY_DENIED` 两码不产生。
//! - 旧 `FileVersionTracker.recordWrite` 的 best-effort 版本登记未移植：
//!   `Edit` 的读后写冲突检测在工具内自包含完成（见 [`crate::file_edit`]），
//!   不需要跨调用的全局版本表。因此 `WriteResult.historyRecorded` /
//!   `directorySyncConfirmed` / `guaranteeLevel` 三个仅供诊断的字段未进
//!   [`WriteOutcome`]（旧 `FileEditTool` 亦只消费 success / newHash / error）。
//! - 旧 `expected == null → EXPECTED_OLD_STATE_REQUIRED` 由类型系统保证
//!   （`&ExpectedOldState` 不可为空），该错误码不产生。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::file_state::normalize_path;

/// 临时文件名唯一性重试次数（`create_new` 撞名时换名重试）。
const TEMP_NAME_ATTEMPTS: u32 = 8;

/// 路径锁表清理阈值（超过此规模时回收无人持有的条目）。
const PATH_LOCK_SWEEP_THRESHOLD: usize = 1024;

/// 写入前对目标文件的预期旧状态（对照旧 `ExpectedOldState` 密封接口）。
///
/// 该值**必填**：旧实现明确「null 绝不表示无条件覆盖」，Rust 侧以
/// `&ExpectedOldState` 参数由类型系统强制。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpectedOldState {
    /// 目标必须不存在（新建语义；已存在 → `FILE_CONFLICT_EXPECTED_ABSENT`）。
    Absent,
    /// 目标必须存在且内容 SHA-256 等于该小写 hex（不等 → `OLD_HASH_CONFLICT`）。
    Sha256(String),
}

impl ExpectedOldState {
    /// 由 hex 摘要构造（统一转小写，对照旧 `Sha256` record 的
    /// `value.toLowerCase()` 归一化）。
    ///
    /// 旧实现对非 64 位 hex 抛 `IllegalArgumentException`；工具路径禁止 panic，
    /// 故此处仅告警并原样保留——非法摘要必然比对失败，落 `OLD_HASH_CONFLICT`，
    /// 语义仍是「拒绝写入」（留痕 docs/compatibility.md §9）。
    #[must_use]
    pub fn sha256(hex: &str) -> Self {
        if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            tracing::warn!(
                hash = hex,
                "expected old hash is not 64 hexadecimal characters"
            );
        }
        Self::Sha256(hex.to_ascii_lowercase())
    }
}

/// 本次写入对磁盘权威状态的实际影响（对照旧 `WriteEffect`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteEffect {
    /// 未开始改动（校验失败 / 临时文件阶段失败）。
    NotStarted,
    /// 已生效（rename 完成且回读校验通过）。
    Applied,
    /// 无法判定（rename 已完成但后续校验失败）。
    Unknown,
}

/// 写入结果（对照旧 `WriteResult` 中被 `FileEditTool` 消费的三元组 + effect）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOutcome {
    /// 是否成功。
    pub success: bool,
    /// 落盘后的内容摘要（失败且已 move 时为磁盘实际摘要，取不到则 `None`）。
    pub new_hash: Option<String>,
    /// 失败原因（成功时 `None`）。
    pub error: Option<String>,
    /// 对磁盘权威状态的影响。
    pub effect: WriteEffect,
}

impl WriteOutcome {
    /// 未触碰磁盘的失败（对照旧 `new WriteResult(false, null, error)`：
    /// effect = `NOT_STARTED`）。
    fn not_started(error: impl Into<String>) -> Self {
        Self {
            success: false,
            new_hash: None,
            error: Some(error.into()),
            effect: WriteEffect::NotStarted,
        }
    }
}

/// 带 CAS 前置校验的原子写入（旧 `writeAuthorized` 的等价入口）。
///
/// 返回 [`WriteOutcome`]；调用方按 `success` 分支，失败文案已含旧实现逐字错误码。
pub async fn write_checked(
    target: &Path,
    content: &str,
    expected: &ExpectedOldState,
) -> WriteOutcome {
    let path = absolute_normalized(target);
    // 同路径串行化（对照旧 `pathLocks.withLock(normalized, …)`）：避免并发
    // Edit/Write 在「读旧摘要 → rename」窗口内互相覆盖。
    let lock = path_lock(&path);
    let _guard = lock.lock().await;

    let parent = path.parent().map(Path::to_path_buf);
    if let Some(parent) = parent.as_deref()
        && let Err(error) = tokio::fs::create_dir_all(parent).await
    {
        return WriteOutcome::not_started(format!("Atomic write failed: {error}"));
    }

    // `symlink_metadata` 对应旧 `Files.exists(…, NOFOLLOW_LINKS)`：符号链接
    // 自身存在即视为存在，不跟随。
    let exists = tokio::fs::symlink_metadata(&path).await.is_ok();
    match expected {
        ExpectedOldState::Absent => {
            if exists {
                return WriteOutcome::not_started("FILE_CONFLICT_EXPECTED_ABSENT");
            }
        }
        ExpectedOldState::Sha256(expected_hash) => {
            if !exists {
                return WriteOutcome::not_started("FILE_CONFLICT_EXPECTED_EXISTING");
            }
            match tokio::fs::read(&path).await {
                Ok(bytes) => {
                    if sha256_hex(&bytes) != *expected_hash {
                        return WriteOutcome::not_started("OLD_HASH_CONFLICT");
                    }
                }
                Err(error) => {
                    return WriteOutcome::not_started(format!("Atomic write failed: {error}"));
                }
            }
        }
    }

    let Some(parent) = parent else {
        return WriteOutcome::not_started("Atomic write failed: target has no parent directory");
    };
    let temp = match create_temp_sibling(&parent, &path).await {
        Ok(temp) => temp,
        Err(error) => return WriteOutcome::not_started(format!("Atomic write failed: {error}")),
    };
    if let Err(error) = write_and_sync(&temp, content.as_bytes()).await {
        remove_quietly(&temp).await;
        return WriteOutcome::not_started(format!("Atomic write failed: {error}"));
    }
    // 仅 ATOMIC_MOVE，无非原子降级路径（旧实现的显式设计）。
    if let Err(error) = tokio::fs::rename(&temp, &path).await {
        remove_quietly(&temp).await;
        return WriteOutcome::not_started(format!("Atomic write failed: {error}"));
    }

    let new_hash = sha256_hex(content.as_bytes());
    let actual_hash = tokio::fs::read(&path)
        .await
        .ok()
        .map(|bytes| sha256_hex(&bytes));
    if actual_hash.as_deref() != Some(new_hash.as_str()) {
        // 旧实现在此抛 `IOException("WRITE_VERIFY_FAILED")`，被外层捕获后因
        // moved=true 转为 POST_MOVE_FAILURE；effect 只在磁盘摘要与目标内容
        // 一致时才回到 APPLIED——此分支恒不一致，故为 UNKNOWN。
        return WriteOutcome {
            success: false,
            new_hash: actual_hash,
            error: Some("POST_MOVE_FAILURE: WRITE_VERIFY_FAILED".to_owned()),
            effect: WriteEffect::Unknown,
        };
    }
    sync_directory_best_effort(&parent);
    WriteOutcome {
        success: true,
        new_hash: Some(new_hash),
        error: None,
        effect: WriteEffect::Applied,
    }
}

/// SHA-256 小写 hex（对照旧 `HexFormat.of().formatHex(MessageDigest…)`）。
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // 写入 String 永不失败。
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// 绝对化 + 归一化（对照旧 `targetPath.toAbsolutePath().normalize()`）。
fn absolute_normalized(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    normalize_path(&absolute)
}

/// 取（或建）该路径的写入互斥锁（对照旧 `ManagedPathLockManager`）。
///
/// 锁表规模越过 [`PATH_LOCK_SWEEP_THRESHOLD`] 时回收无人持有的条目，避免
/// 长跑进程无界增长（旧实现同为进程内缓存）。
fn path_lock(path: &Path) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let registry = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = registry.lock().unwrap_or_else(PoisonError::into_inner);
    if locks.len() > PATH_LOCK_SWEEP_THRESHOLD {
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
    }
    Arc::clone(
        locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
    )
}

/// 在目标同目录建独占临时文件（对照旧
/// `Files.createTempFile(parent, "." + fileName, ".tmp")`）。
async fn create_temp_sibling(parent: &Path, target: &Path) -> std::io::Result<PathBuf> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let stem = target.file_name().map_or_else(
        || std::ffi::OsString::from("zk-edit"),
        std::ffi::OsStr::to_os_string,
    );
    let mut last_error = None;
    for _ in 0..TEMP_NAME_ATTEMPTS {
        let serial = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.subsec_nanos());
        let mut name = std::ffi::OsString::from(".");
        name.push(&stem);
        name.push(format!(".{}-{serial}-{nanos}.tmp", std::process::id()));
        let candidate = parent.join(name);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .await
        {
            Ok(_) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique temporary file",
        )
    }))
}

/// 写满 + fsync（对照旧 `FileChannel.write` 循环 + `channel.force(true)`）。
async fn write_and_sync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    file.sync_all().await
}

/// 父目录 fsync（对照旧 `syncDirectoryBestEffort`：失败仅 debug）。
///
/// 目录 fsync 是常数级元数据操作，沿用旧实现的同步调用形态。
fn sync_directory_best_effort(directory: &Path) {
    match std::fs::File::open(directory).and_then(|handle| handle.sync_all()) {
        Ok(()) => {}
        Err(error) => {
            tracing::debug!(dir = %directory.display(), %error, "directory fsync unavailable");
        }
    }
}

/// 清理残留临时文件（对照旧 `Files.deleteIfExists` + 失败仅告警）。
async fn remove_quietly(path: &Path) {
    if let Err(error) = tokio::fs::remove_file(path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), %error, "failed to cleanup atomic-write temp file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-atomic-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn sha256_hex_matches_known_digest() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn expected_sha256_is_lowercased() {
        let expected = ExpectedOldState::sha256(&"AB".repeat(32));
        assert_eq!(expected, ExpectedOldState::Sha256("ab".repeat(32)));
    }

    #[tokio::test]
    async fn creates_file_when_absent_expected() {
        let path = temp_dir("create").join("a.txt");
        let _ = std::fs::remove_file(&path);
        let outcome = write_checked(&path, "hello", &ExpectedOldState::Absent).await;
        assert!(outcome.success, "{:?}", outcome.error);
        assert_eq!(outcome.effect, WriteEffect::Applied);
        assert_eq!(
            outcome.new_hash.as_deref(),
            Some(sha256_hex(b"hello").as_str())
        );
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "hello");
    }

    #[tokio::test]
    async fn rejects_absent_expectation_on_existing_file() {
        let path = temp_dir("absent-conflict").join("b.txt");
        std::fs::write(&path, "there").expect("seed");
        let outcome = write_checked(&path, "x", &ExpectedOldState::Absent).await;
        assert!(!outcome.success);
        assert_eq!(
            outcome.error.as_deref(),
            Some("FILE_CONFLICT_EXPECTED_ABSENT")
        );
        assert_eq!(outcome.effect, WriteEffect::NotStarted);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "there");
    }

    #[tokio::test]
    async fn rejects_sha_expectation_when_file_missing_or_changed() {
        let dir = temp_dir("sha-conflict");
        let missing = dir.join("gone.txt");
        let _ = std::fs::remove_file(&missing);
        let outcome =
            write_checked(&missing, "x", &ExpectedOldState::sha256(&"0".repeat(64))).await;
        assert_eq!(
            outcome.error.as_deref(),
            Some("FILE_CONFLICT_EXPECTED_EXISTING")
        );

        let path = dir.join("c.txt");
        std::fs::write(&path, "old").expect("seed");
        let stale = write_checked(&path, "new", &ExpectedOldState::sha256(&"1".repeat(64))).await;
        assert_eq!(stale.error.as_deref(), Some("OLD_HASH_CONFLICT"));
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "old");

        let fresh =
            write_checked(&path, "new", &ExpectedOldState::sha256(&sha256_hex(b"old"))).await;
        assert!(fresh.success, "{:?}", fresh.error);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "new");
    }

    #[tokio::test]
    async fn leaves_no_temporary_files_behind() {
        let dir = temp_dir("no-temp");
        let path = dir.join("d.txt");
        let outcome = write_checked(&path, "body", &ExpectedOldState::Absent).await;
        assert!(outcome.success || outcome.error.is_some());
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .expect("read dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporary files leaked: {leftovers:?}"
        );
    }

    #[tokio::test]
    async fn creates_missing_parent_directories() {
        let path = temp_dir("parents").join("nested/deep/e.txt");
        let _ = std::fs::remove_file(&path);
        let outcome = write_checked(&path, "deep", &ExpectedOldState::Absent).await;
        assert!(outcome.success, "{:?}", outcome.error);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "deep");
    }
}
