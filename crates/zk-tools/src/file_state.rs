//! Read-before-Edit 状态台账——会话维度「已读文件」记录 + 外部改动过期检测。
//!
//! 逐字对照旧 `service/FileStateCache.java`（只读权威规格）：LRU `MAX_ENTRIES`
//! 100 条 / `MAX_SIZE_BYTES` 25 MiB 双上限（每次 put 最多驱逐一条最久未访问
//! 条目，对照 `removeEldestEntry` 的单条语义）、路径 key 一律经
//! `Path.normalize()`、四个语义入口 `markRead` / `markModified` /
//! `hasBeenRead` / `isStale`；会话分桶对照旧
//! `SessionManager.getFileStateCache(sessionId)`（按需建桶）。
//!
//! 依赖方向：本模块不依赖任何兄弟 crate。[`global`] 提供进程级单例（等价旧
//! `SessionManager` 单例 bean 持有 per-session `FileStateCache`），`Read` /
//! `Write` / `Edit` 三件工具直接消费，无需组合根显式装配——会话隔离由
//! `session_id` 分桶保证（旧实现同为“单例 + 会话分桶”）。
//!
//! 差异（留痕 docs/compatibility.md §9）：旧 `cloneCache` / `merge` /
//! `toMap` 三个子代理与压缩导出入口未移植（子代理域尚未落地，压缩侧不消费
//! 本台账），按「不提前引入」原则待相应能力落地时再补。

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// LRU 条目上限（旧 `MAX_ENTRIES = 100` 逐字对照）。
pub const MAX_FILE_STATE_ENTRIES: usize = 100;

/// 缓存内容字节上限（旧 `MAX_SIZE_BYTES = 25 * 1024 * 1024` 逐字对照）。
pub const MAX_FILE_STATE_BYTES: u64 = 25 * 1024 * 1024;

/// 一次读取留下的文件状态（形状对齐旧 `FileStateCache.FileState`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileState {
    /// 读到的正文（不含 `Read` 工具的行号前缀，对照旧
    /// `String.join("\n", selectedLines)`）。
    pub content: String,
    /// 记录时刻（Unix 毫秒，对照旧 `System.currentTimeMillis()`）。
    pub timestamp_ms: u64,
    /// 起始行（`None` = 完整读取，对照旧 `offset > 0 ? offset : null`）。
    pub offset: Option<usize>,
    /// 行数上限（`None` = 无限制）。
    pub limit: Option<usize>,
    /// 是否为截断视图（旧 `isPartialView`；`markModified` 强制置真以要求重读）。
    pub is_partial_view: bool,
}

/// 缓存内部条目——状态 + 访问序（LRU 依据，等价旧 `LinkedHashMap` accessOrder）。
#[derive(Clone, Debug)]
struct Entry {
    /// 文件状态。
    state: FileState,
    /// 最近一次访问的逻辑时钟值（越小越久未访问）。
    access_seq: u64,
}

/// 单会话文件状态缓存（旧 `FileStateCache` 的等价物）。
///
/// 非线程安全（旧实现靠 `synchronized`）；跨任务共享请用 [`FileStateStore`]。
#[derive(Debug, Default)]
pub struct FileStateCache {
    /// 归一化路径 → 条目。
    entries: HashMap<String, Entry>,
    /// 已记录正文的字节总量。
    total_size_bytes: u64,
    /// 访问序发号器。
    access_clock: u64,
}

impl FileStateCache {
    /// 建空缓存。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一次读取（旧 `markRead`）。
    pub fn mark_read(
        &mut self,
        path: &str,
        content: &str,
        offset: Option<usize>,
        limit: Option<usize>,
        is_partial: bool,
    ) {
        let key = normalize_key(path);
        let access_seq = self.next_seq();
        let entry = Entry {
            state: FileState {
                content: content.to_owned(),
                timestamp_ms: now_ms(),
                offset,
                limit,
                is_partial_view: is_partial,
            },
            access_seq,
        };
        if let Some(previous) = self.entries.insert(key, entry) {
            self.total_size_bytes = self
                .total_size_bytes
                .saturating_sub(byte_len(&previous.state.content));
        }
        self.total_size_bytes = self.total_size_bytes.saturating_add(byte_len(content));
        self.evict_eldest_if_needed();
    }

    /// 标记文件已被本系统改写（旧 `markModified`：仅在已有记录上刷新时间戳并
    /// 置 `isPartialView = true`；无记录则不建记录）。
    pub fn mark_modified(&mut self, path: &str) {
        let key = normalize_key(path);
        let access_seq = self.next_seq();
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.state.timestamp_ms = now_ms();
            entry.state.is_partial_view = true;
            entry.access_seq = access_seq;
        }
    }

    /// 是否读过（旧 `hasBeenRead` = `containsKey`，**不**提升 LRU 位次）。
    #[must_use]
    pub fn has_been_read(&self, path: &str) -> bool {
        self.entries.contains_key(&normalize_key(path))
    }

    /// 记录是否已过期（旧 `isStale`：无记录 → `true`；磁盘 mtime 晚于记录时刻
    /// → `true`；取 mtime 失败 → `true`）。
    pub fn is_stale(&mut self, path: &str) -> bool {
        let key = normalize_key(path);
        let access_seq = self.next_seq();
        let Some(entry) = self.entries.get_mut(&key) else {
            return true;
        };
        entry.access_seq = access_seq;
        let recorded = entry.state.timestamp_ms;
        modified_ms(Path::new(path)).is_none_or(|mtime| mtime > recorded)
    }

    /// 当前条目数（测试用）。
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空（测试用）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 已记录正文字节总量（测试用）。
    #[must_use]
    pub fn total_size_bytes(&self) -> u64 {
        self.total_size_bytes
    }

    /// 取指定路径的状态副本（测试与诊断用）。
    #[must_use]
    pub fn state_of(&self, path: &str) -> Option<FileState> {
        self.entries
            .get(&normalize_key(path))
            .map(|entry| entry.state.clone())
    }

    /// 发一个新的访问序号。
    fn next_seq(&mut self) -> u64 {
        self.access_clock = self.access_clock.wrapping_add(1);
        self.access_clock
    }

    /// 越限时驱逐**一条**最久未访问条目（对照旧 `removeEldestEntry` 每次 put
    /// 至多移除一条的语义）。
    fn evict_eldest_if_needed(&mut self) {
        if self.entries.len() <= MAX_FILE_STATE_ENTRIES
            && self.total_size_bytes <= MAX_FILE_STATE_BYTES
        {
            return;
        }
        let eldest = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.access_seq)
            .map(|(key, _)| key.clone());
        if let Some(key) = eldest
            && let Some(removed) = self.entries.remove(&key)
        {
            self.total_size_bytes = self
                .total_size_bytes
                .saturating_sub(byte_len(&removed.state.content));
        }
    }
}

/// 会话分桶的文件状态台账（旧 `SessionManager.getFileStateCache(sessionId)`）。
///
/// 组合根装配为单例并以 `Arc` 注入文件族三件工具，使 `Read` 的记账对
/// `Edit` 的门禁可见。
#[derive(Debug, Default)]
pub struct FileStateStore {
    /// 会话 ID → 该会话的缓存。
    sessions: Mutex<HashMap<String, FileStateCache>>,
}

impl FileStateStore {
    /// 建空台账。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一次读取（会话桶按需创建）。
    pub fn mark_read(
        &self,
        session_id: &str,
        path: &str,
        content: &str,
        offset: Option<usize>,
        limit: Option<usize>,
        is_partial: bool,
    ) {
        self.with_session(session_id, |cache| {
            cache.mark_read(path, content, offset, limit, is_partial);
        });
    }

    /// 标记文件已被本系统改写（会话桶按需创建）。
    pub fn mark_modified(&self, session_id: &str, path: &str) {
        self.with_session(session_id, |cache| cache.mark_modified(path));
    }

    /// 是否读过（未知会话 → `false`，等价旧空缓存 `containsKey`）。
    ///
    /// 读路径**不**建桶：避免伪造会话 ID 撑大内存；语义与旧实现一致。
    #[must_use]
    pub fn has_been_read(&self, session_id: &str, path: &str) -> bool {
        self.lock()
            .get(session_id)
            .is_some_and(|cache| cache.has_been_read(path))
    }

    /// 记录是否已过期（未知会话 → `true`，等价旧空缓存 `isStale`）。
    #[must_use]
    pub fn is_stale(&self, session_id: &str, path: &str) -> bool {
        let mut sessions = self.lock();
        sessions
            .get_mut(session_id)
            .is_none_or(|cache| cache.is_stale(path))
    }

    /// 已建桶的会话数（测试用）。
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.lock().len()
    }

    /// 在会话桶上执行一次写操作（按需建桶）。
    fn with_session<R>(
        &self,
        session_id: &str,
        action: impl FnOnce(&mut FileStateCache) -> R,
    ) -> R {
        let mut sessions = self.lock();
        action(sessions.entry(session_id.to_owned()).or_default())
    }

    /// 取锁（毒化后继续用内层值——台账是尽力而为的记账面，不应因 panic 传播失败）。
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, FileStateCache>> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// 进程级台账单例（等价旧 `SessionManager` 单例 bean 持有 per-session
/// `FileStateCache`）。
#[must_use]
pub fn global() -> &'static FileStateStore {
    static STORE: LazyLock<FileStateStore> = LazyLock::new(FileStateStore::new);
    &STORE
}

/// 无会话上下文时的分桶 key。
///
/// 旧 `ToolUseContext.sessionId()` 恒有值；Rust 侧 [`crate::ToolContext`] 将其
/// 建模为 `Option`（单测可不注入）。缺失时统一落匿名桶而非绕过门禁，
/// 保证 Read-before-Edit 语义在任何上下文下都成立。
pub const ANONYMOUS_SESSION: &str = "";

/// 台账分桶 key（无会话时回落 [`ANONYMOUS_SESSION`]）。
#[must_use]
pub fn session_key(session_id: Option<&str>) -> &str {
    session_id.unwrap_or(ANONYMOUS_SESSION)
}

/// 路径 key 归一化（对照旧 `Path.of(path).normalize().toString()`）。
#[must_use]
pub fn normalize_key(path: &str) -> String {
    normalize_path(Path::new(path))
        .to_string_lossy()
        .into_owned()
}

/// 纯词法路径归一化（对照旧 JDK `Path.normalize()`，不触磁盘、不解符号链接）。
///
/// `.` 由 [`Path::components`] 自然吸收；`..` 在可回退时弹出上一级，紧跟根
/// 之后时丢弃（与 JDK `UnixPath.normalize` 一致），否则保留。
#[must_use]
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match normalized.components().next_back() {
                Some(Component::Normal(_)) => {
                    normalized.pop();
                }
                Some(Component::RootDir | Component::Prefix(_)) => {}
                _ => normalized.push(Component::ParentDir),
            },
            other => normalized.push(other),
        }
    }
    normalized
}

/// 当前 Unix 毫秒（时钟早于纪元时回落 0）。
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// 磁盘 mtime（Unix 毫秒）；取不到时 `None`（调用方按「已过期」处理）。
fn modified_ms(path: &Path) -> Option<u64> {
    let modified = std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()?;
    let since_epoch = modified.duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(since_epoch.as_millis()).ok()
}

/// 正文字节数（`usize` → `u64` 无损转换的显式写法，规避 cast lint）。
fn byte_len(text: &str) -> u64 {
    u64::try_from(text.len()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-file-state-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn normalize_key_matches_jdk_normalize() {
        assert_eq!(normalize_key("/a/./b"), "/a/b");
        assert_eq!(normalize_key("/a/b/../c"), "/a/c");
        assert_eq!(normalize_key("./a"), "a");
        assert_eq!(normalize_key("a/.."), "");
        assert_eq!(normalize_key("/.."), "/");
        assert_eq!(normalize_key("../a"), "../a");
    }

    #[test]
    fn mark_read_then_has_been_read() {
        let mut cache = FileStateCache::new();
        assert!(!cache.has_been_read("/tmp/a.txt"));
        cache.mark_read("/tmp/./a.txt", "body", Some(2), Some(10), false);
        // 归一化后同一 key。
        assert!(cache.has_been_read("/tmp/a.txt"));
        let state = cache.state_of("/tmp/a.txt").expect("state");
        assert_eq!(state.content, "body");
        assert_eq!(state.offset, Some(2));
        assert_eq!(state.limit, Some(10));
        assert!(!state.is_partial_view);
        assert_eq!(cache.total_size_bytes(), 4);
    }

    #[test]
    fn mark_modified_only_refreshes_existing_entry() {
        let mut cache = FileStateCache::new();
        cache.mark_modified("/tmp/missing.txt");
        assert!(cache.is_empty());

        cache.mark_read("/tmp/b.txt", "body", None, None, false);
        cache.mark_modified("/tmp/b.txt");
        let state = cache.state_of("/tmp/b.txt").expect("state");
        assert!(state.is_partial_view, "markModified 强制置截断视图");
        assert_eq!(state.content, "body", "正文保留");
    }

    #[test]
    fn is_stale_reports_true_without_record_and_after_external_touch() {
        let path = temp_dir("stale").join("c.txt");
        std::fs::write(&path, "one").expect("seed");
        let display = path.to_str().expect("utf8");
        let mut cache = FileStateCache::new();
        assert!(cache.is_stale(display), "无记录即过期");

        cache.mark_read(display, "one", None, None, false);
        // 记录时刻取 now，磁盘 mtime 不晚于它 → 未过期。
        assert!(!cache.is_stale(display));

        // 外部改写：把 mtime 推到未来 → 过期。
        let future = SystemTime::now() + std::time::Duration::from_mins(1);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .and_then(|file| file.set_modified(future))
            .expect("touch");
        assert!(cache.is_stale(display));
    }

    #[test]
    fn evicts_single_eldest_entry_per_insert() {
        let mut cache = FileStateCache::new();
        for index in 0..=MAX_FILE_STATE_ENTRIES {
            cache.mark_read(&format!("/tmp/f{index}.txt"), "x", None, None, false);
        }
        assert_eq!(cache.len(), MAX_FILE_STATE_ENTRIES);
        assert!(!cache.has_been_read("/tmp/f0.txt"), "最久未访问条目被驱逐");
        assert!(cache.has_been_read(&format!("/tmp/f{MAX_FILE_STATE_ENTRIES}.txt")));
    }

    #[test]
    fn store_isolates_sessions_and_lazily_creates_buckets() {
        let store = FileStateStore::new();
        assert!(!store.has_been_read("s1", "/tmp/d.txt"));
        assert!(store.is_stale("s1", "/tmp/d.txt"));
        assert_eq!(store.session_count(), 0, "读路径不建桶");

        store.mark_read("s1", "/tmp/d.txt", "body", None, None, false);
        assert!(store.has_been_read("s1", "/tmp/d.txt"));
        assert!(!store.has_been_read("s2", "/tmp/d.txt"), "会话隔离");
        assert_eq!(store.session_count(), 1);
    }
}
