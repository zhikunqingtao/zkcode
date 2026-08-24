//! zk-db 错误类型。

/// 持久化层错误——SQLite / 迁移 / 编解码 / 后台任务故障的统一出口。
///
/// 设计约定：
/// - 「会话不存在」显式建模为 [`DbError::SessionNotFound`]（由外键违例等
///   底层信号映射而来），供 zk-server 直接映射 404（对齐旧系统
///   `SessionNotFoundException` 语义）；
/// - 游标**不**产生错误：无效游标照旧系统行为回退为「从最新开始」
///   （`SessionController.listSessions` 捕获后仅 log.warn），见 `cursor.rs`。
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// rusqlite 底层错误（含约束冲突、IO 故障等）。
    #[error("sqlite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// refinery 迁移执行错误（启动期迁移失败、迁移历史不一致等）。
    #[error("migration failure: {0}")]
    Migration(#[from] refinery::Error),
    /// `spawn_blocking` 任务 join 失败（panic 或取消）。
    #[error("blocking task join failure: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// JSON 编解码失败（`content_json` / `metadata_json` 写入路径）。
    #[error("json codec failure: {0}")]
    Json(#[from] serde_json::Error),
    /// 打开/创建数据库文件及父目录的 IO 错误。
    #[error("io failure: {0}")]
    Io(#[from] std::io::Error),
    /// 目标会话不存在（写入路径外键违例 / 显式判空）。
    #[error("session not found: {0}")]
    SessionNotFound(String),
    /// 调用方输入或库内状态不可调和（zk-authz 授权记录身份冲突、ONCE 作用域
    /// 试图落库等）。zk-server 归入 500 分支，细节只进日志。
    #[error("invalid state: {0}")]
    Invalid(String),
    /// 工作区根目录已被其他项目占用（`projects.workspace_root` UNIQUE 违例，
    /// 由 `project::map_unique_violation` 归一；zk-server 映射 409
    /// `PROJECT_PATH_DUPLICATE`，对齐旧 `DuplicateKeyException` 捕获语义）。
    #[error("workspace root already bound to a project: {0}")]
    WorkspaceRootTaken(String),
}

/// 将 rusqlite 外键违例归一为 [`DbError::SessionNotFound`]。
///
/// 会话域写入（`append_message*`）唯一的外键依赖是
/// `messages.session_id → sessions.id`；`SQLite` 外键违例（扩展码 787，
/// `SQLITE_CONSTRAINT_FOREIGNKEY`）在此场景下即「会话不存在」，显式映射
/// 避免调用方逐一 sniff 底层错误码。
pub(super) fn map_fk_violation(session_id: &str, err: rusqlite::Error) -> DbError {
    // SQLITE_CONSTRAINT_FOREIGNKEY = 787（rusqlite ErrorCode 无细粒度变体，
    // 以扩展码精确判定）。
    if matches!(
        err,
        rusqlite::Error::SqliteFailure(ref e, _) if e.extended_code == 787
    ) {
        DbError::SessionNotFound(session_id.to_owned())
    } else {
        DbError::Sqlite(err)
    }
}
