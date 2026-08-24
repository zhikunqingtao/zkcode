//! 文件历史与写前快照事务（Batch 5 Step 5）——对照旧
//! `history/FileHistoryService.java`（425 行，只读权威规格）。
//!
//! 三份内存台账逐条对齐旧字段：
//! - `read_timestamps`（旧 `readTimestamps`）：路径 → 最近一次「读/写」时刻，
//!   供 [`FileHistoryService::is_modified_since_last_read`] 与
//!   [`FileHistoryService::changed_files_since`] 判定外部改动；
//! - `active_transactions`（旧 `activeTransactions`）：会话 → 当前助手回合的
//!   变更集（F3 事务）；
//! - `committed_transactions`（旧 `committedTransactions`）：会话 → 已提交回合
//!   列表，供 `/undo` 类能力按回合回退。
//!
//! Rewind 语义（旧 `rewindFiles`）：按 `messageId` 取该回合的全部写前快照，
//! **回退前对当前磁盘内容再做一次快照**（`operation = "rewind"`），再以
//! CAS（`ExpectedOldState`）原子写回旧内容——保证回退操作本身可再次回退，且
//! 并发写入不会被静默覆盖。
//!
//! # 差异留痕
//!
//! 1. **工作区边界自建**：旧实现调 `atomicFileWriter.write(...)`（严格
//!    Project-bound 入口），边界校验发生在写入器内部的
//!    `ManagedWorkspacePathResolver.resolveProspective`。本仓
//!    [`zk_tools::write_checked`] 不带 `workspace_root` 参数（工具侧边界由
//!    zk-authz 权限管线前置把关），故本模块自带 [`resolve_prospective`]
//!    逐条复刻那三条判定 + 段校验：`workspace path changed` /
//!    `target is the workspace root itself` / `target escapes workspace` /
//!    `path segment is not a real directory` / `path segment escapes workspace`
//!    / `symbolic-link targets are not writable`。缺这层会让快照表里的任意
//!    绝对路径（含历史遗留的越界记录）被 rewind 写到工作区之外。
//! 2. **`ATOMIC_FILE_WRITER_UNAVAILABLE` 分支未移植**：旧实现的原子写入器是
//!    可空注入字段，本仓为自由函数，恒可用，该分支不可达。
//! 3. **`trackAppliedEdit` 不在本模块**：写后快照（`Write` / `Edit` 工具路径）
//!    已由 [`zk_tools::SnapshotSink`] + zk-server `DbSnapshotSink` 实现，此处
//!    不再重复一份，避免两条持久化路径。
//! 4. **`changed_files_since` 返回值排序**：旧实现遍历 `ConcurrentHashMap`，
//!    次序不定；本实现按路径升序返回（便于测试与展示，无消费方依赖原序）。
//! 5. **`compute_diff_stats` 遍历序收紧**：旧实现 `allFiles` 是 `HashSet`，
//!    `changedFiles` 次序不定；本实现用 `BTreeSet` 取路径升序。计数三元组
//!    与旧实现逐字一致。
//! 6. **`list_snapshots_by_session` 形状**：旧返回
//!    `LinkedHashMap<messageId, List<SnapshotInfo>>`（`messageId` 为 `null`
//!    时聚在 `null` 键）。Rust 用 [`TurnSnapshots`] 有序 `Vec` 表达同一语义，
//!    `message_id: Option<String>` 保留 `null` 分组，序列化侧决定如何呈现。
//! 7. **`track_edit` 持久化失败**：旧 `snapshotRepository.save` 抛
//!    `RuntimeException` 时穿出 `catch (IOException)`，由调用方
//!    （`rewindFiles` 的 `catch (Exception)`）转为该文件的错误条目。Rust 以
//!    `Result<(), String>` 显式表达同一控制流；读取类 IO 失败仍只 `warn`
//!    并返回 `Ok`（与旧 `catch (IOException)` 一致）。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use zk_db::{Db, DbError, FileSnapshotRecord};
use zk_tools::{ExpectedOldState, MAX_SNAPSHOT_BYTES, sha256_hex, write_checked};

/// 单个快照的对外摘要（对照旧 `record SnapshotInfo(String filePath, String timestamp)`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// 被快照文件的绝对路径。
    pub file_path: String,
    /// 快照落库时刻（原样透传 `file_snapshots.created_at`）。
    pub timestamp: String,
}

/// 一个助手回合内的全部快照（旧 `Map<messageId, List<SnapshotInfo>>` 的一项）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnSnapshots {
    /// 触发写入的消息 ID（`None` 对应旧实现的 `null` 分组键）。
    pub message_id: Option<String>,
    /// 该回合的快照列表（按落库次序）。
    pub files: Vec<SnapshotInfo>,
}

/// 回退结果（对照旧 `record RewindResult(...)`）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RewindResult {
    /// 是否全部成功（`errors` 为空）。
    pub success: bool,
    /// 已恢复的文件路径。
    pub restored_files: Vec<String>,
    /// 跳过的文件路径（旧实现恒空，保留字段以对齐响应形状）。
    pub skipped_files: Vec<String>,
    /// 逐文件失败原因（`"<path>: <reason>"`）或整体失败码。
    pub errors: Vec<String>,
}

impl RewindResult {
    /// 整体失败（无任何文件被处理）。
    fn failed(error: impl Into<String>) -> Self {
        Self {
            success: false,
            restored_files: Vec::new(),
            skipped_files: Vec::new(),
            errors: vec![error.into()],
        }
    }
}

/// 两个回合之间的变更统计（对照旧 `record DiffStats(...)`）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffStats {
    /// 仅出现在 `to` 侧的文件数。
    pub files_added: usize,
    /// 两侧都有且内容不同的文件数。
    pub files_modified: usize,
    /// 仅出现在 `from` 侧的文件数。
    pub files_deleted: usize,
    /// 上述三类的全部路径（升序）。
    pub changed_files: Vec<String>,
}

/// 已提交的回合事务（对照旧 `record TransactionRecord(...)`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionRecord {
    /// 该回合的助手消息 ID。
    pub message_id: String,
    /// 回合内发生写入的文件（首次出现次序，去重）。
    pub changed_files: Vec<String>,
    /// 事务开始时刻（旧 `ActiveTransaction.startTime`，提交时原样搬运）。
    pub timestamp: SystemTime,
    /// 事务开始时的消息序号（供 `/undo` 截断历史）。
    pub start_seq_num: usize,
}

/// 活跃事务（旧 `record ActiveTransaction(...)`）。
///
/// 旧 record 另有 `sessionId` 字段；此处该值即 `active_transactions` 的键，
/// 不再冗余存一份。
#[derive(Clone, Debug)]
struct ActiveTransaction {
    /// 该回合的助手消息 ID。
    message_id: String,
    /// 事务开始时刻。
    start_time: SystemTime,
    /// 事务开始时的消息序号。
    start_seq_num: usize,
    /// 回合内发生写入的文件（首次出现次序，去重）。
    changed_files: Vec<String>,
}

/// 文件历史服务（旧 `FileHistoryService` `@Service` 单例的等价物）。
pub struct FileHistoryService {
    /// 快照持久化与会话读取。
    db: Db,
    /// 路径 → 最近一次读/写时刻。
    read_timestamps: Mutex<HashMap<String, SystemTime>>,
    /// 会话 → 活跃事务。
    active_transactions: Mutex<HashMap<String, ActiveTransaction>>,
    /// 会话 → 已提交事务列表（追加序）。
    committed_transactions: Mutex<HashMap<String, Vec<TransactionRecord>>>,
}

impl std::fmt::Debug for FileHistoryService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileHistoryService")
            .field("tracked_files", &self.tracked_file_count())
            .finish_non_exhaustive()
    }
}

impl FileHistoryService {
    /// 建服务（快照落库与会话读取共用同一 [`Db`]）。
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self {
            db,
            read_timestamps: Mutex::new(HashMap::new()),
            active_transactions: Mutex::new(HashMap::new()),
            committed_transactions: Mutex::new(HashMap::new()),
        }
    }

    // ==================== F3 事务边界 ====================

    /// 记录事务起点（对照旧 `beginTransaction`）。
    ///
    /// 同一会话重复调用会**覆盖**未提交的活跃事务（旧 `Map.put` 语义）：
    /// 助手回合是串行推进的，上一回合若因中断未提交，其变更集随之丢弃。
    pub fn begin_transaction(&self, session_id: &str, message_id: &str, current_seq_num: usize) {
        lock(&self.active_transactions).insert(
            session_id.to_owned(),
            ActiveTransaction {
                message_id: message_id.to_owned(),
                start_time: SystemTime::now(),
                start_seq_num: current_seq_num,
                changed_files: Vec::new(),
            },
        );
        tracing::debug!(
            session_id,
            message_id,
            start_seq_num = current_seq_num,
            "文件历史事务开始"
        );
    }

    /// 提交事务（对照旧 `commitTransaction`）。
    ///
    /// 无活跃事务时静默返回；变更集为空时**不**产出已提交记录（旧实现同样以
    /// `changedFiles.isEmpty()` 短路，避免无文件改动的回合污染 `/undo` 栈）。
    pub fn commit_transaction(&self, session_id: &str) {
        let Some(active) = lock(&self.active_transactions).remove(session_id) else {
            tracing::debug!(session_id, "无活跃文件历史事务可提交");
            return;
        };
        if active.changed_files.is_empty() {
            tracing::debug!(
                session_id,
                message_id = active.message_id,
                "事务无文件变更，不记录"
            );
            return;
        }
        let record = TransactionRecord {
            message_id: active.message_id,
            changed_files: active.changed_files,
            timestamp: active.start_time,
            start_seq_num: active.start_seq_num,
        };
        tracing::debug!(
            session_id,
            message_id = record.message_id,
            changed = record.changed_files.len(),
            "文件历史事务提交"
        );
        lock(&self.committed_transactions)
            .entry(session_id.to_owned())
            .or_default()
            .push(record);
    }

    /// 取最后一个已提交事务（对照旧 `getLastTransaction`）。
    #[must_use]
    pub fn last_transaction(&self, session_id: &str) -> Option<TransactionRecord> {
        lock(&self.committed_transactions)
            .get(session_id)
            .and_then(|records| records.last().cloned())
    }

    /// 弹出最后一个已提交事务（对照旧 `removeLastTransaction`）。
    pub fn remove_last_transaction(&self, session_id: &str) -> Option<TransactionRecord> {
        lock(&self.committed_transactions)
            .get_mut(session_id)
            .and_then(Vec::pop)
    }

    // ==================== 读时间戳台账 ====================

    /// 记录一次文件读取（对照旧 `trackFileRead`）。
    pub fn track_file_read(&self, absolute_path: &str) {
        lock(&self.read_timestamps).insert(absolute_path.to_owned(), SystemTime::now());
    }

    /// 列出台账中「记录后又被外部改动」的文件（对照旧 `getChangedFilesSince`）。
    #[must_use]
    pub fn changed_files_since(&self) -> Vec<String> {
        let mut changed: Vec<String> = lock(&self.read_timestamps)
            .iter()
            .filter(|(path, recorded)| modified_after(Path::new(path.as_str()), **recorded))
            .map(|(path, _)| path.clone())
            .collect();
        changed.sort();
        changed
    }

    /// 该文件自最近一次记录后是否被改动（对照旧 `isModifiedSinceLastRead`）。
    ///
    /// 无记录返回 `false`（旧实现同：未读过的文件不算「被改动」）；取
    /// 元数据失败亦返回 `false`（旧实现 `catch (IOException) { return false; }`）。
    #[must_use]
    pub fn is_modified_since_last_read(&self, absolute_path: &str) -> bool {
        let recorded = lock(&self.read_timestamps).get(absolute_path).copied();
        recorded.is_some_and(|recorded| modified_after(Path::new(absolute_path), recorded))
    }

    /// 清空读时间戳台账（对照旧 `clear`）。
    pub fn clear(&self) {
        lock(&self.read_timestamps).clear();
    }

    /// 台账条目数（对照旧 `getTrackedFileCount`）。
    #[must_use]
    pub fn tracked_file_count(&self) -> usize {
        lock(&self.read_timestamps).len()
    }

    // ==================== 写前快照 ====================

    /// 写前快照（对照旧 `trackEdit`）。
    ///
    /// 判定链逐字对齐：目标不存在或非普通文件 → 不快照；体积超过
    /// [`MAX_SNAPSHOT_BYTES`]（10 MiB）→ **早退**（旧实现在此 `return`，
    /// 因此**不**更新读时间戳）；读取失败 → 只告警。快照落库成功后把该路径
    /// 挂到活跃事务的变更集（去重）。
    ///
    /// # Errors
    ///
    /// 快照落库失败时返回错误文案（旧实现此路径抛 `RuntimeException` 穿出
    /// `catch (IOException)`，由调用方转为该文件的错误条目）。
    pub async fn track_edit(
        &self,
        file_path: &str,
        session_id: &str,
        message_id: Option<&str>,
        operation: &str,
    ) -> Result<(), String> {
        let path = Path::new(file_path);
        if let Some(metadata) = tokio::fs::metadata(path)
            .await
            .ok()
            .filter(std::fs::Metadata::is_file)
        {
            if metadata.len() > u64::try_from(MAX_SNAPSHOT_BYTES).unwrap_or(u64::MAX) {
                tracing::debug!(file_path, "文件超过 10MB，跳过写前快照");
                return Ok(());
            }
            match tokio::fs::read_to_string(path).await {
                Ok(content) => {
                    self.db
                        .insert_file_snapshot(
                            session_id, message_id, file_path, &content, operation,
                        )
                        .await
                        .map_err(|error| {
                            tracing::warn!(file_path, %error, "写前快照落库失败");
                            error.to_string()
                        })?;
                    tracing::debug!(
                        session_id,
                        file_path,
                        size = content.len(),
                        "写前快照已保存"
                    );
                    let mut active = lock(&self.active_transactions);
                    if let Some(transaction) = active.get_mut(session_id)
                        && !transaction
                            .changed_files
                            .iter()
                            .any(|tracked| tracked == file_path)
                    {
                        transaction.changed_files.push(file_path.to_owned());
                    }
                }
                Err(error) => tracing::warn!(file_path, %error, "写前快照读取失败"),
            }
        }
        lock(&self.read_timestamps).insert(file_path.to_owned(), SystemTime::now());
        Ok(())
    }

    // ==================== Rewind ====================

    /// 按会话列出快照，按 `message_id` 分组（对照旧 `listSnapshotsBySession`）。
    ///
    /// # Errors
    ///
    /// 底层查询失败时返回 [`DbError`]。
    pub async fn list_snapshots_by_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<TurnSnapshots>, DbError> {
        let records = self.db.list_file_snapshots(session_id).await?;
        let mut grouped: Vec<TurnSnapshots> = Vec::new();
        for record in records {
            let FileSnapshotRecord {
                message_id,
                file_path,
                created_at,
                ..
            } = record;
            let info = SnapshotInfo {
                file_path,
                timestamp: created_at,
            };
            if let Some(turn) = grouped
                .iter_mut()
                .find(|turn| turn.message_id == message_id)
            {
                turn.files.push(info);
            } else {
                grouped.push(TurnSnapshots {
                    message_id,
                    files: vec![info],
                });
            }
        }
        Ok(grouped)
    }

    /// 回退某回合的文件写入（对照旧 `rewindFiles`）。
    ///
    /// `file_paths` 为 `None` 或空切片时回退该回合全部快照；否则只回退交集。
    /// 每个文件在写回前先做一次 `operation = "rewind"` 的二次快照，再以
    /// CAS 原子写入——磁盘当前内容与预期不符（并发写）时该文件落错误条目而
    /// 不被覆盖。
    pub async fn rewind_files(
        &self,
        session_id: &str,
        message_id: &str,
        file_paths: Option<&[String]>,
    ) -> RewindResult {
        let working_dir = match self.db.get_session(session_id).await {
            Ok(Some(detail)) => detail.working_dir,
            Ok(None) => return RewindResult::failed("SESSION_NOT_FOUND"),
            Err(error) => return RewindResult::failed(format!("SESSION_LOAD_FAILED: {error}")),
        };
        let snapshots = match self.db.list_by_message_id(session_id, message_id).await {
            Ok(snapshots) => snapshots,
            Err(error) => return RewindResult::failed(format!("SNAPSHOT_LOAD_FAILED: {error}")),
        };
        if snapshots.is_empty() {
            return RewindResult::failed(format!("No snapshots found for messageId: {message_id}"));
        }
        let filter: Option<HashSet<&str>> = file_paths
            .filter(|paths| !paths.is_empty())
            .map(|paths| paths.iter().map(String::as_str).collect());
        // 二次快照挂到独立的合成回合上（旧 `"rewind_" + uuid.substring(0, 8)`）。
        let rewind_message_id =
            format!("rewind_{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let mut result = RewindResult::default();
        for snapshot in snapshots {
            if let Some(filter) = filter.as_ref()
                && !filter.contains(snapshot.file_path.as_str())
            {
                continue;
            }
            let target = Path::new(snapshot.file_path.as_str());
            // 安全措施：恢复前对当前内容做一次快照，确保恢复操作本身可再次回退。
            if tokio::fs::metadata(target)
                .await
                .is_ok_and(|metadata| metadata.is_file())
                && let Err(error) = self
                    .track_edit(
                        &snapshot.file_path,
                        session_id,
                        Some(&rewind_message_id),
                        "rewind",
                    )
                    .await
            {
                result
                    .errors
                    .push(format!("{}: {error}", snapshot.file_path));
                continue;
            }
            // CAS 预期旧态：符号链接自身存在即视为存在（旧 NOFOLLOW_LINKS）。
            let expected = if tokio::fs::symlink_metadata(target).await.is_ok() {
                match tokio::fs::read(target).await {
                    Ok(bytes) => ExpectedOldState::sha256(&sha256_hex(&bytes)),
                    Err(error) => {
                        result
                            .errors
                            .push(format!("{}: {error}", snapshot.file_path));
                        continue;
                    }
                }
            } else {
                ExpectedOldState::Absent
            };
            let resolved = match resolve_prospective(target, &working_dir) {
                Ok(resolved) => resolved,
                Err(reason) => {
                    result
                        .errors
                        .push(format!("{}: {reason}", snapshot.file_path));
                    continue;
                }
            };
            let outcome = write_checked(&resolved, &snapshot.content, &expected).await;
            if !outcome.success {
                let reason = outcome
                    .error
                    .unwrap_or_else(|| "atomic write failed".to_owned());
                result
                    .errors
                    .push(format!("{}: {reason}", snapshot.file_path));
                continue;
            }
            // 回退后强制该路径重读（旧 `fileStateCache.markModified`）。
            zk_tools::file_state::global().mark_modified(session_id, &snapshot.file_path);
            result.restored_files.push(snapshot.file_path);
        }
        result.success = result.errors.is_empty();
        result
    }

    /// 统计两个回合之间的文件变更（对照旧 `computeDiffStats`）。
    ///
    /// # Errors
    ///
    /// 底层查询失败时返回 [`DbError`]。
    pub async fn compute_diff_stats(
        &self,
        session_id: &str,
        from_message_id: &str,
        to_message_id: &str,
    ) -> Result<DiffStats, DbError> {
        let from = content_map(
            self.db
                .list_by_message_id(session_id, from_message_id)
                .await?,
        );
        let to = content_map(
            self.db
                .list_by_message_id(session_id, to_message_id)
                .await?,
        );
        let mut all: BTreeSet<&String> = from.keys().collect();
        all.extend(to.keys());
        let mut stats = DiffStats::default();
        for path in all {
            match (from.get(path), to.get(path)) {
                (None, Some(_)) => stats.files_added += 1,
                (Some(_), None) => stats.files_deleted += 1,
                (Some(before), Some(after)) if before != after => stats.files_modified += 1,
                _ => continue,
            }
            stats.changed_files.push(path.clone());
        }
        Ok(stats)
    }
}

/// 路径 → 内容映射（同路径多条快照保留**最后**一条，对照旧 `(a, b) -> b`）。
fn content_map(records: Vec<FileSnapshotRecord>) -> HashMap<String, String> {
    records
        .into_iter()
        .map(|record| (record.file_path, record.content))
        .collect()
}

/// 中毒锁降级为取内值（历史台账不因一次 panic 而整体不可用）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 文件的最后修改时刻是否严格晚于 `recorded`（旧 `lastModified.isAfter(...)`）。
fn modified_after(path: &Path, recorded: SystemTime) -> bool {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|modified| modified > recorded)
}

/// 工作区受限的目标路径解析（逐条复刻旧
/// `ManagedWorkspacePathResolver.resolveProspective(raw, root, false, false)`）。
///
/// # Errors
///
/// 工作区根不是自身的真实路径、目标即工作区根、目标越界、路径段不是真实
/// 目录 / 越界、目标本身是符号链接——六类情形返回旧实现的逐字文案。
pub fn resolve_prospective(raw: &Path, workspace_root: &str) -> Result<PathBuf, String> {
    if workspace_root.trim().is_empty() {
        return Err("working directory is required".to_owned());
    }
    let lexical_root = lexical_absolute(Path::new(workspace_root));
    let root = std::fs::canonicalize(&lexical_root)
        .map_err(|error| format!("workspace path changed: {error}"))?;
    if root != lexical_root {
        return Err("workspace path changed".to_owned());
    }
    let candidate = if raw.is_absolute() {
        let lexical = lexical_absolute(raw);
        match lexical.strip_prefix(&lexical_root) {
            Ok(relative) => root.join(relative),
            Err(_) => lexical,
        }
    } else {
        lexical_absolute(&root.join(raw))
    };
    if candidate == root {
        return Err("target is the workspace root itself".to_owned());
    }
    if !candidate.starts_with(&root) {
        return Err("target escapes workspace".to_owned());
    }
    validate_existing_segments(&root, &candidate)?;
    Ok(candidate)
}

/// 逐段校验已存在的父目录 + 目标自身非符号链接（旧 `validateExistingSegments`）。
fn validate_existing_segments(root: &Path, candidate: &Path) -> Result<(), String> {
    if let Some(parent) = candidate.parent()
        && let Ok(relative) = parent.strip_prefix(root)
    {
        let mut current = root.to_path_buf();
        for segment in relative.components() {
            current.push(segment);
            if std::fs::symlink_metadata(&current).is_err() {
                break;
            }
            assert_real_directory(root, &current)?;
        }
    }
    if std::fs::symlink_metadata(candidate).is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        return Err("symbolic-link targets are not writable".to_owned());
    }
    Ok(())
}

/// 该路径段必须是真实目录且其真实路径仍在工作区内（旧 `assertRealDirectory`）。
fn assert_real_directory(root: &Path, directory: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(directory).map_err(|error| {
        format!(
            "path segment is not readable: {}: {error}",
            directory.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "path segment is not a real directory: {}",
            directory.display()
        ));
    }
    let real = std::fs::canonicalize(directory).map_err(|error| {
        format!(
            "path segment is not resolvable: {}: {error}",
            directory.display()
        )
    })?;
    if !real.starts_with(root) {
        return Err("path segment escapes workspace".to_owned());
    }
    Ok(())
}

/// 纯词法绝对化 + 规范化（等价旧 `toAbsolutePath().normalize()`，**不**触碰
/// 文件系统、不解析符号链接）。
fn lexical_absolute(path: &Path) -> PathBuf {
    let mut resolved = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().unwrap_or_default()
    };
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(Component::RootDir.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(segment) => resolved.push(segment),
        }
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::{FileHistoryService, resolve_prospective};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};
    use zk_db::Db;

    /// 独占临时目录（本仓无 `tempfile` 依赖，沿用兄弟模块的 `temp_dir` 惯例）；
    /// 返回值已 `canonicalize`——`resolve_prospective` 要求工作区根即其真实路径
    /// （macOS 的 `/var` → `/private/var` 会让未规范化的根直接判 workspace
    /// path changed）。
    fn temp_root(tag: &str) -> PathBuf {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let base =
            std::env::temp_dir().join(format!("zk_filehist_{}_{tag}_{seq}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("mkdir");
        std::fs::canonicalize(&base).expect("canonicalize")
    }

    /// 建库 + 建会话（`working_dir` 取给定工作区根）。
    async fn boot(root: &Path) -> (Db, String) {
        let db = Db::open_in_memory().expect("boot");
        let session = db
            .create_session("gpt-4o", root.to_str().expect("utf8"))
            .await
            .expect("session");
        (db, session.id)
    }

    /// begin → 写前快照关联 → commit：变更集进已提交栈，`start_seq_num` 保真。
    #[tokio::test]
    async fn transaction_collects_changed_files_on_commit() {
        let root = temp_root("tx");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db);
        let file = root.join("a.txt");
        std::fs::write(&file, "before").expect("write");
        let path = file.to_str().expect("utf8").to_owned();

        history.begin_transaction(&session_id, "turn-1", 7);
        history
            .track_edit(&path, &session_id, Some("turn-1"), "edit")
            .await
            .expect("snapshot");
        // 同一路径重复快照不重复入变更集（旧 `!contains` 去重）。
        history
            .track_edit(&path, &session_id, Some("turn-1"), "edit")
            .await
            .expect("snapshot again");
        history.commit_transaction(&session_id);

        let record = history.last_transaction(&session_id).expect("committed");
        assert_eq!(record.message_id, "turn-1");
        assert_eq!(record.start_seq_num, 7);
        assert_eq!(record.changed_files, vec![path.clone()]);
        // 读时间戳在写前快照路径上一并更新。
        assert_eq!(history.tracked_file_count(), 1);

        assert_eq!(
            history.remove_last_transaction(&session_id),
            Some(record),
            "弹出后栈空"
        );
        assert!(history.last_transaction(&session_id).is_none());
    }

    /// 无文件变更的回合不入已提交栈；无活跃事务时 commit 静默返回。
    #[tokio::test]
    async fn empty_transaction_is_not_recorded() {
        let root = temp_root("tx-empty");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db);

        history.commit_transaction(&session_id); // 无活跃事务
        history.begin_transaction(&session_id, "turn-1", 1);
        history.commit_transaction(&session_id);
        assert!(history.last_transaction(&session_id).is_none());
        assert!(history.remove_last_transaction(&session_id).is_none());
    }

    /// 读时间戳：未记录 → false；记录后被改动 → true；`clear` 清空。
    #[tokio::test]
    async fn read_timestamps_detect_external_modification() {
        let root = temp_root("mtime");
        let (db, _session_id) = boot(&root).await;
        let history = FileHistoryService::new(db);
        let file = root.join("watched.txt");
        std::fs::write(&file, "v1").expect("write");
        let path = file.to_str().expect("utf8").to_owned();

        assert!(
            !history.is_modified_since_last_read(&path),
            "未记录的路径不算被改动"
        );
        history.track_file_read(&path);
        assert!(history.changed_files_since().is_empty());

        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&file, "v2").expect("rewrite");
        assert!(history.is_modified_since_last_read(&path));
        assert_eq!(history.changed_files_since(), vec![path.clone()]);

        assert_eq!(history.tracked_file_count(), 1);
        history.clear();
        assert_eq!(history.tracked_file_count(), 0);
        assert!(!history.is_modified_since_last_read(&path));
    }

    /// 不存在 / 非普通文件 → 不快照，但读时间戳仍更新（旧非早退路径）。
    #[tokio::test]
    async fn missing_target_records_timestamp_without_snapshot() {
        let root = temp_root("missing");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        let path = root.join("ghost.txt").to_str().expect("utf8").to_owned();

        history
            .track_edit(&path, &session_id, Some("turn-1"), "write")
            .await
            .expect("no-op");
        assert_eq!(history.tracked_file_count(), 1);
        assert!(
            db.list_file_snapshots(&session_id)
                .await
                .expect("list")
                .is_empty()
        );
    }

    /// >10MB 早退：既不快照也**不**更新读时间戳（逐字对照旧 `return`）。
    #[tokio::test]
    async fn oversized_file_skips_snapshot_and_timestamp() {
        let root = temp_root("large");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        let file = root.join("big.bin");
        std::fs::write(&file, vec![b'x'; 10 * 1024 * 1024 + 1]).expect("write");
        let path = file.to_str().expect("utf8").to_owned();

        history.begin_transaction(&session_id, "turn-1", 1);
        history
            .track_edit(&path, &session_id, Some("turn-1"), "write")
            .await
            .expect("skipped");
        assert_eq!(history.tracked_file_count(), 0, "早退不更新读时间戳");
        assert!(
            db.list_file_snapshots(&session_id)
                .await
                .expect("list")
                .is_empty()
        );
        history.commit_transaction(&session_id);
        assert!(history.last_transaction(&session_id).is_none());
    }

    /// 按会话列出快照：按 `message_id` 分组，组内与组间均保持落库次序。
    #[tokio::test]
    async fn snapshots_group_by_message_in_insertion_order() {
        let root = temp_root("group");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        db.insert_file_snapshot(&session_id, Some("turn-1"), "/tmp/a.txt", "a1", "edit")
            .await
            .expect("a1");
        db.insert_file_snapshot(&session_id, Some("turn-1"), "/tmp/b.txt", "b1", "edit")
            .await
            .expect("b1");
        db.insert_file_snapshot(&session_id, Some("turn-2"), "/tmp/a.txt", "a2", "edit")
            .await
            .expect("a2");
        db.insert_file_snapshot(&session_id, None, "/tmp/c.txt", "c1", "edit")
            .await
            .expect("c1");

        let grouped = history
            .list_snapshots_by_session(&session_id)
            .await
            .expect("list");
        assert_eq!(grouped.len(), 3);
        assert_eq!(grouped[0].message_id.as_deref(), Some("turn-1"));
        assert_eq!(grouped[0].files.len(), 2);
        assert_eq!(grouped[0].files[0].file_path, "/tmp/a.txt");
        assert!(!grouped[0].files[0].timestamp.is_empty());
        assert_eq!(grouped[1].message_id.as_deref(), Some("turn-2"));
        assert_eq!(grouped[2].message_id, None, "null 分组保留");
    }

    /// Rewind 主路径：内容写回 + 二次快照留痕 + `success = true`。
    #[tokio::test]
    async fn rewind_restores_content_and_snapshots_current_state() {
        let root = temp_root("rewind");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        let file = root.join("src.txt");
        std::fs::write(&file, "old").expect("write");
        let path = file.to_str().expect("utf8").to_owned();
        db.insert_file_snapshot(&session_id, Some("turn-1"), &path, "old", "edit")
            .await
            .expect("snapshot");
        std::fs::write(&file, "new").expect("rewrite");

        let result = history.rewind_files(&session_id, "turn-1", None).await;
        assert!(result.success, "errors: {:?}", result.errors);
        assert_eq!(result.restored_files, vec![path.clone()]);
        assert!(result.skipped_files.is_empty());
        assert_eq!(std::fs::read_to_string(&file).expect("read"), "old");

        let rewind_snapshots: Vec<_> = db
            .list_file_snapshots(&session_id)
            .await
            .expect("list")
            .into_iter()
            .filter(|record| record.operation == "rewind")
            .collect();
        assert_eq!(rewind_snapshots.len(), 1);
        assert_eq!(rewind_snapshots[0].content, "new", "回退前的磁盘内容入库");
        assert!(
            rewind_snapshots[0]
                .message_id
                .as_deref()
                .is_some_and(|id| id.starts_with("rewind_")),
            "合成回合 id: {:?}",
            rewind_snapshots[0].message_id
        );
    }

    /// `file_paths` 过滤只回退交集，其余原样保留。
    #[tokio::test]
    async fn rewind_honours_file_filter() {
        let root = temp_root("rewind-filter");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        let kept = root.join("kept.txt");
        let target = root.join("target.txt");
        std::fs::write(&kept, "kept-old").expect("write");
        std::fs::write(&target, "target-old").expect("write");
        let kept_path = kept.to_str().expect("utf8").to_owned();
        let target_path = target.to_str().expect("utf8").to_owned();
        db.insert_file_snapshot(&session_id, Some("turn-1"), &kept_path, "kept-old", "edit")
            .await
            .expect("s1");
        db.insert_file_snapshot(
            &session_id,
            Some("turn-1"),
            &target_path,
            "target-old",
            "edit",
        )
        .await
        .expect("s2");
        std::fs::write(&kept, "kept-new").expect("rewrite");
        std::fs::write(&target, "target-new").expect("rewrite");

        let result = history
            .rewind_files(
                &session_id,
                "turn-1",
                Some(std::slice::from_ref(&target_path)),
            )
            .await;
        assert!(result.success, "errors: {:?}", result.errors);
        assert_eq!(result.restored_files, vec![target_path.clone()]);
        assert_eq!(
            std::fs::read_to_string(&kept).expect("read"),
            "kept-new",
            "未选中的文件不动"
        );
        assert_eq!(
            std::fs::read_to_string(&target).expect("read"),
            "target-old"
        );
    }

    /// 会话缺失 / 该回合无快照 → 整体失败码（不触碰磁盘）。
    #[tokio::test]
    async fn rewind_reports_missing_session_and_snapshots() {
        let root = temp_root("rewind-missing");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db);

        let ghost = history.rewind_files("ghost", "turn-1", None).await;
        assert!(!ghost.success);
        assert_eq!(ghost.errors, vec!["SESSION_NOT_FOUND".to_owned()]);

        let empty = history.rewind_files(&session_id, "turn-x", None).await;
        assert!(!empty.success);
        assert_eq!(
            empty.errors,
            vec!["No snapshots found for messageId: turn-x".to_owned()]
        );
    }

    /// 快照里的越界路径不得被写回工作区之外（旧写入器的 Project-bound 语义）。
    #[tokio::test]
    async fn rewind_rejects_targets_outside_workspace() {
        let root = temp_root("rewind-escape");
        let outside = temp_root("rewind-outside");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        let victim = outside.join("victim.txt");
        std::fs::write(&victim, "untouched").expect("write");
        let victim_path = victim.to_str().expect("utf8").to_owned();
        db.insert_file_snapshot(
            &session_id,
            Some("turn-1"),
            &victim_path,
            "injected",
            "edit",
        )
        .await
        .expect("snapshot");

        let result = history.rewind_files(&session_id, "turn-1", None).await;
        assert!(!result.success);
        assert!(result.restored_files.is_empty());
        assert_eq!(result.errors.len(), 1);
        assert!(
            result.errors[0].contains("target escapes workspace"),
            "unexpected: {:?}",
            result.errors
        );
        assert_eq!(
            std::fs::read_to_string(&victim).expect("read"),
            "untouched",
            "工作区外文件必须保持原状"
        );
    }

    /// CAS 冲突：磁盘内容与预期不符时该文件落错误条目（此处以并发替身构造——
    /// 直接把 `content` 写成与快照不同再手工传入过期预期不可行，故用符号链接
    /// 目标被拒的等价安全路径覆盖 CAS 之外的守卫，见下一个用例）。
    #[tokio::test]
    async fn rewind_rejects_symlink_targets() {
        let root = temp_root("rewind-symlink");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        let real = root.join("real.txt");
        std::fs::write(&real, "real").expect("write");
        let link = root.join("link.txt");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &link).expect("symlink");
        let link_path = link.to_str().expect("utf8").to_owned();
        db.insert_file_snapshot(&session_id, Some("turn-1"), &link_path, "injected", "edit")
            .await
            .expect("snapshot");

        let result = history.rewind_files(&session_id, "turn-1", None).await;
        assert!(!result.success);
        assert!(
            result.errors[0].contains("symbolic-link targets are not writable"),
            "unexpected: {:?}",
            result.errors
        );
        assert_eq!(
            std::fs::read_to_string(&real).expect("read"),
            "real",
            "链接指向的真实文件不得被改写"
        );
    }

    /// 回合 diff：新增 / 修改 / 删除三类计数与路径集合。
    #[tokio::test]
    async fn diff_stats_classify_added_modified_deleted() {
        let root = temp_root("diff");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        // from 侧：a（内容 a1）、gone（仅 from）
        db.insert_file_snapshot(&session_id, Some("from"), "/tmp/a.txt", "a1", "edit")
            .await
            .expect("a1");
        db.insert_file_snapshot(&session_id, Some("from"), "/tmp/gone.txt", "g", "edit")
            .await
            .expect("g");
        db.insert_file_snapshot(&session_id, Some("from"), "/tmp/same.txt", "s", "edit")
            .await
            .expect("s-from");
        // to 侧：a（内容 a2 → modified）、fresh（仅 to → added）、same（未变）
        db.insert_file_snapshot(&session_id, Some("to"), "/tmp/a.txt", "a2", "edit")
            .await
            .expect("a2");
        db.insert_file_snapshot(&session_id, Some("to"), "/tmp/fresh.txt", "f", "edit")
            .await
            .expect("f");
        db.insert_file_snapshot(&session_id, Some("to"), "/tmp/same.txt", "s", "edit")
            .await
            .expect("s-to");

        let stats = history
            .compute_diff_stats(&session_id, "from", "to")
            .await
            .expect("diff");
        assert_eq!(stats.files_added, 1);
        assert_eq!(stats.files_modified, 1);
        assert_eq!(stats.files_deleted, 1);
        assert_eq!(
            stats.changed_files,
            vec![
                "/tmp/a.txt".to_owned(),
                "/tmp/fresh.txt".to_owned(),
                "/tmp/gone.txt".to_owned(),
            ]
        );
    }

    /// 同一回合同一路径多条快照时保留最后一条内容（旧 `(a, b) -> b`）。
    #[tokio::test]
    async fn diff_stats_keep_last_snapshot_per_path() {
        let root = temp_root("diff-last");
        let (db, session_id) = boot(&root).await;
        let history = FileHistoryService::new(db.clone());
        db.insert_file_snapshot(&session_id, Some("from"), "/tmp/a.txt", "v1", "edit")
            .await
            .expect("v1");
        db.insert_file_snapshot(&session_id, Some("from"), "/tmp/a.txt", "v2", "edit")
            .await
            .expect("v2");
        db.insert_file_snapshot(&session_id, Some("to"), "/tmp/a.txt", "v2", "edit")
            .await
            .expect("to");

        let stats = history
            .compute_diff_stats(&session_id, "from", "to")
            .await
            .expect("diff");
        assert_eq!(stats.files_modified, 0, "末条 v2 == v2 视为未变");
        assert!(stats.changed_files.is_empty());
    }

    /// 工作区边界三条判定的逐字文案。
    #[test]
    fn resolve_prospective_enforces_workspace_boundary() {
        let root = temp_root("resolve");
        let root_str = root.to_str().expect("utf8");

        assert_eq!(
            resolve_prospective(&root, root_str),
            Err("target is the workspace root itself".to_owned())
        );
        assert_eq!(
            resolve_prospective(Path::new("/etc/passwd"), root_str),
            Err("target escapes workspace".to_owned())
        );
        assert_eq!(
            resolve_prospective(Path::new("a.txt"), ""),
            Err("working directory is required".to_owned())
        );
        assert_eq!(
            resolve_prospective(&root.join("nested").join("a.txt"), root_str),
            Ok(root.join("nested").join("a.txt")),
            "尚不存在的父目录不阻断解析"
        );
        // `..` 词法归一后仍在根内 → 允许；越出根 → 拒绝。
        assert_eq!(
            resolve_prospective(&root.join("nested").join("..").join("a.txt"), root_str),
            Ok(root.join("a.txt"))
        );
        assert_eq!(
            resolve_prospective(&root.join("..").join("sibling.txt"), root_str),
            Err("target escapes workspace".to_owned())
        );
    }

    /// 父目录段是符号链接时整体拒绝（旧 `assertRealDirectory`）。
    #[cfg(unix)]
    #[test]
    fn resolve_prospective_rejects_symlinked_parent_segment() {
        let root = temp_root("resolve-seg");
        let outside = temp_root("resolve-seg-out");
        let link = root.join("escape");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink");

        let error = resolve_prospective(&link.join("a.txt"), root.to_str().expect("utf8"))
            .expect_err("must reject");
        assert!(
            error.starts_with("path segment is not a real directory:"),
            "unexpected: {error}"
        );
    }
}
