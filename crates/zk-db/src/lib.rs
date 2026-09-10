//! zk-db：zkcode 持久化层——基于 `SQLite` 的会话/消息存储及迁移管理（S5）。
//!
//! crate 拓扑定位：位于 `zk-protocol` 之上的存储 crate，被 `zk-engine`（读写
//! 会话与消息）和 `zk-server`（组装）依赖。遵循 D6：单库默认 `zkcode/.zk/data.db`、
//! 对外全部 async fn（同步 `SQLite` 操作经 `spawn_blocking` 派发）、refinery 迁移
//! 启动自动执行、PRAGMA 全套（WAL / `busy_timeout=5s` / `foreign_keys` / NORMAL /
//! `cache_size` / `temp_store`，逐项照抄旧系统 `SqliteConfig` 的 Xerial 属性集）。
//!
//! # Greenfield final schema
//!
//! - schema：单份绿地基线一步创建 Session/Task/Run/Result/Invocation/
//!   Resource/Usage 最终模型；不保留历史数据迁移或双写路径。
//! - 并发模型（D-P2-4）：读写分离——单 writer `Mutex<Connection>` 串行化全部
//!   写入 + 只读 reader 连接池（数量 = CPU 核数，轮转分发），全部走 WAL；
//!   [`Db::with_reader`] / [`Db::with_writer`] 取代 Phase 1 的单一 `with_conn`
//!   （保留 `spawn_blocking` 模式与锁毒化恢复语义）。
//!
//! # 模块导航
//!
//! - [`error`]：`DbError`（含外键违例 → `SessionNotFound` 归一）。
//! - [`model`]：`SessionSummary` / `SessionDetail` / `MessageRecord` /
//!   `StoredBlock`（存储形状内容块）等领域 struct。
//! - [`cursor`]：两类游标原语（会话 `updated_at|id` / 消息索引）。
//! - [`time`]：epoch 毫秒 ↔ RFC 3339 及 serde 适配。
//! - `session` / `message` / `config` / `project` / `snapshot` / `activity` /
//!   `memory`：[`Db`] 的会话域 / 消息域 / 配置域 / 项目域（projects +
//!   `project_config`，2.1）/ 文件快照域（`file_snapshots`，2.3）/ 长期记忆域
//!   （`memories`，Batch 5）方法（多文件 impl）。
//! - [`run`]：Run 生命周期（`run_envelopes` / `run_event_log`）的唯一 SQL 权威。
//!   `zk-engine`（生产 Run 的编排方）与 `zk-server`（交互闭环 / 授权诊断事件）
//!   都要写这两张表，而铁律禁止 `zk-engine → zk-server`，故 SQL 下沉至二者共同
//!   依赖的本 crate；`zk_server::interaction::runs` 现为本模块的再导出。

mod activity;
mod anomaly;
mod artifact;
mod checkpoint;
mod config;
mod cron;
mod evidence;
mod memory;
mod message;
mod project;
mod project_context;
mod research;
mod restart_reconciliation;
mod runtime_health;
mod runtime_ledger;
mod safe_recovery;
mod session;
mod snapshot;
mod swarm;
mod task_budget;
mod task_diagnostic;
mod task_runtime;
mod workbench;

pub mod convert;
pub mod cursor;
pub mod error;
pub mod model;
pub mod run;
pub mod time;

pub use anomaly::AnomalyEventRecord;
pub use artifact::{ArtifactEntryRecord, ArtifactManifestRecord, ProducedFileArtifactRecord};
pub use checkpoint::{AgentCheckpointRecord, new_agent_checkpoint};
pub use cron::{
    ClaimCronOccurrence, CronClaimOutcome, CronJobRecord, CronOccurrenceRecord, MAX_CRON_JOBS,
    NewCronJob,
};
pub use error::DbError;
pub use evidence::{
    EvidenceBundleRecord, EvidenceItemRecord, EvidenceOrigin, EvidenceVerdictEventRecord,
};
pub use memory::{MemoryRecord, MemoryScope, MemoryTarget, MemoryUpsert};
pub use message::MessageAttribution;
pub use model::{
    ImageSource, MessagePage, MessageRecord, MessageRole, NewMessage, SessionDetail, SessionPage,
    SessionSummary, StoredBlock, goal_preview,
};
pub use project::{ProjectRecord, find_project_by_workspace_root_in_current_write};
pub use project_context::ProjectContextRecord;
pub use research::{
    MAX_RESEARCH_EXCERPT_BYTES, MAX_RESEARCH_PROJECTION_ROWS, MAX_RESEARCH_PROVIDER_BYTES,
    MAX_RESEARCH_QUERY_BYTES, MAX_RESEARCH_RECEIPT_ENTRIES, MAX_RESEARCH_TITLE_BYTES,
    MAX_RESEARCH_URL_BYTES, ProducedResearchCapture, ProducedResearchEntry, ProducedResearchKind,
    ResearchCaptureRecord, ResearchConflictRecord, ResearchFindingRecord,
    ResearchOpenQuestionRecord, ResearchProjection, ResearchRequirementCoverageRecord,
    ResearchSourceRecord,
};
pub use restart_reconciliation::{RestartReconciliationReport, RuntimeShutdownIntentReport};
pub use run::{RunEnvelopeView, RunEventView, WsOutboxEvent, WsReplayEvent};
pub use runtime_health::RuntimeHealthSnapshot;
pub use runtime_ledger::{
    CommitToolInvocationResult, CommitToolInvocationResultOutcome, CommittedToolInvocationResult,
    ExecutionResourceStatus, LlmCallBudgetReservation, LlmUsageCompletion, LlmUsageIntegrity,
    NewExecutionResource, NewLlmCall, NewToolInvocation, ToolInvocationRecord,
    ToolInvocationStatus,
};
pub use safe_recovery::{
    CreateSafeRecoveryAttempt, RecoveryEligibility, SafeRecoveryAttempt, SafeRecoveryCandidate,
};
pub use session::{
    RestoreCostSummary, RestoredToolCall, SessionRuntimeRestore, SnapshotRestoreOutcome,
};
pub use snapshot::FileSnapshotRecord;
pub use swarm::SwarmRecord;
pub use task_budget::{
    BudgetReservationStatus, DIRECT_CHILD_BUDGET_PERCENT, ROOT_BUDGET_RESERVE_PERCENT,
    TaskBudgetLimits, TaskBudgetReservationRecord, TaskBudgetSnapshot,
};
pub use task_diagnostic::{
    TaskDiagnostic, TaskDiagnosticCheckpoint, TaskDiagnosticExecutionResource,
    TaskDiagnosticLlmCall, TaskDiagnosticReceipt, TaskDiagnosticResult, TaskDiagnosticRun,
    TaskDiagnosticTask, TaskDiagnosticToolInvocation, TaskResumeEligibility,
    TaskResumeIneligibilityReason,
};
pub use task_runtime::{
    CasOutcome, CleanupStatus, CommitTaskResult, CommitTaskResultOutcome, CreateTaskWithRun,
    CreateTaskWithRunOutcome, ExitReason, INLINE_RESULT_LIMIT, InboxStatus,
    MarkTaskNeedsAttentionOutcome, RESULT_HARD_LIMIT, ResultStatus, RunStatus, RunUsageFallback,
    RuntimeTaskRecord, TaskInboxMessage, TaskResultChunk, TaskResultReceiptRecord,
    TaskResultRecord, TaskStatus, VerificationStatus,
};
pub use workbench::{
    AcceptanceCriterionRecord, CurrentWorkbenchProjection, PreviousWorkbenchDelivery,
    WorkbenchActiveTool, WorkbenchBindingRecord, WorkbenchRecord, WorkbenchSubtreeUsage,
};

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension};

/// Database baseline compiled into this binary.
pub const GREENFIELD_SCHEMA_VERSION: u16 = 2;
/// Immutable identity stored with the one-shot final schema.
pub const GREENFIELD_SCHEMA_KIND: &str = "greenfieldFinal";
/// Existing databases are accepted only when they already match the final schema.
pub const DATABASE_MIGRATION_MODE: &str = "greenfieldOnly";
/// Old write protocols and backfill adapters are not part of the database contract.
pub const LEGACY_WRITE_COMPATIBILITY: bool = false;

const SCHEMA_METADATA_TABLE: &str = "zk_schema_metadata";
const REFINERY_HISTORY_TABLE: &str = "refinery_schema_history";
const FINAL_SCHEMA_TABLES: &[&str] = &[
    "activities",
    "agent_checkpoints",
    "anomaly_events",
    "artifact_entries",
    "artifact_manifests",
    "auth_tokens",
    "config",
    "cron_jobs",
    "cron_occurrences",
    "evidence_bundles",
    "evidence_items",
    "evidence_verdict_events",
    "execution_resources",
    "file_snapshots",
    "interaction_requests",
    "llm_calls",
    "memories",
    "messages",
    "permission_grants",
    "project_config",
    "project_context",
    "projects",
    "research_captures",
    "research_conflicts",
    "research_findings",
    "research_open_questions",
    "research_requirement_coverage",
    "research_sources",
    "regression_scripts",
    "run_acceptance_criteria",
    "run_envelopes",
    "run_event_log",
    "run_workbench_bindings",
    "sessions",
    "task_budget_reservations",
    "task_dependencies",
    "task_inbox_messages",
    "task_result_blobs",
    "task_result_receipts",
    "task_results",
    "tasks",
    "tool_invocations",
    "tool_result_postprocessing",
    "websocket_session_binding",
    SCHEMA_METADATA_TABLE,
];

/// 内嵌迁移（refinery `embed_migrations!`，编译期打入二进制，无需外部
/// 迁移文件即可在任意环境启动自迁移）。
mod migrations {
    // embed_migrations! 在调用点内再生成一层 `pub mod migrations`，
    // 故 runner 实际路径为 migrations::migrations::runner（refinery 宏布局）。
    refinery::embed_migrations!();
}

/// 只读 reader 连接池（D-P2-4）：固定连接数 + 原子计数轮转分发。
///
/// 设计取舍（手写而非 r2d2）：连接常驻 `Vec`（不进出借还），每条自带
/// `Mutex`——`with_reader` 轮转选中后与 writer 同套「`spawn_blocking` 内
/// 锁 + 毒化恢复」语义；无借出/归还生命周期，连接不会因任务 panic 而
/// 从池中丢失。极端情况下两个读任务撞同一连接仅退化为串行，无死锁面。
struct ReaderPool {
    /// 只读连接（`SQLITE_OPEN_READ_ONLY` 打开，写语句在 `SQLite` 层报错，
    /// 兜底拦截误路由到 reader 的写操作）。
    conns: Vec<Mutex<Connection>>,
    /// 轮转游标（Relaxed 足够：仅做负载分摊，无同步语义）。
    next: AtomicUsize,
}

/// `SQLite` 会话库句柄——克隆共享同一 writer 连接与 reader 池。
///
/// 并发模型（D-P2-4 读写分离，全部走 WAL）：
/// - **writer**：单连接 `Mutex<Connection>`，进程内全部写入串行化
///   （`SQLite` 单写者模型的自然映射；`busy_timeout` 兜底跨进程冲突）；
/// - **readers**：CPU 核数条只读连接，WAL 快照读不阻塞 writer；
///   内存库无 reader 池（`SQLite` 内存库不可跨连接共享），读路径
///   自动回落 writer 连接，语义不变。
///
/// 所有公开方法均为 `async fn`，同步 `SQLite` 操作经
/// [`tokio::task::spawn_blocking`] 派发，不阻塞 runtime worker。
#[derive(Clone)]
pub struct Db {
    /// 唯一写连接（含迁移执行；亦是内存库的全部）。
    writer: Arc<Mutex<Connection>>,
    /// 只读连接池；`None` = 内存库（读写共用 writer）。
    readers: Option<Arc<ReaderPool>>,
}

impl Db {
    /// 打开（或创建）文件库：建父目录 → writer 连接（PRAGMA 全套 + 自动
    /// 迁移）→ reader 池（CPU 核数条只读连接）。
    ///
    /// 默认路径约定 `zkcode/.zk/data.db`（D6），由调用方（zk-server 装配）
    /// 传入；重复打开同一文件幂等（迁移历史表判定，无重复建表副作用）。
    /// reader 必须晚于 writer 打开：writer 先建库并落 WAL 模式，只读连接
    /// 才能挂上已存在的 `-shm`/`-wal`。
    ///
    /// # Errors
    /// 建目录 / 打开连接 / PRAGMA / 迁移任一失败时返回。
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            let parent_existed = parent.try_exists()?;
            std::fs::create_dir_all(parent)?;
            Self::restrict_runtime_directory(parent, !parent_existed)?;
        }
        // Pre-create with 0600 on Unix so there is no 0644 window between
        // SQLite's create(2) and a later chmod. Existing files are tightened
        // before SQLite reads their header.
        Self::prepare_database_file(path)?;
        let mut writer = Connection::open(path)?;
        Self::init_writer(&mut writer)?;
        Self::restrict_sqlite_files(path)?;
        let reader_count = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        let mut conns = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            conns.push(Mutex::new(Self::open_reader(path)?));
        }
        Self::restrict_sqlite_files(path)?;
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: Some(Arc::new(ReaderPool {
                conns,
                next: AtomicUsize::new(0),
            })),
        })
    }

    /// 内存库（集成测试用）：与文件库同套 PRAGMA 与迁移。
    ///
    /// 注：WAL 对内存库无意义（引擎回报 `memory`），仅其余 PRAGMA 生效；
    /// `SQLite` 内存库不可跨连接共享，故无 reader 池，读路径回落 writer。
    ///
    /// # Errors
    /// 打开连接 / PRAGMA / 迁移任一失败时返回。
    pub fn open_in_memory() -> Result<Self, DbError> {
        let mut conn = Connection::open_in_memory()?;
        Self::init_writer(&mut conn)?;
        Ok(Self {
            writer: Arc::new(Mutex::new(conn)),
            readers: None,
        })
    }

    /// writer 连接初始化：`busy_timeout` 后先执行 greenfield 身份门禁，再施加
    /// 其余 PRAGMA 并运行单份 refinery 基线。已有业务表但缺少最终 schema
    /// identity 的数据库会失败关闭；不会尝试回填或叠加建表。
    ///
    /// # Errors
    /// PRAGMA 执行失败、迁移失败或打开连接失败时返回。
    fn init_writer(conn: &mut Connection) -> Result<(), DbError> {
        conn.busy_timeout(Duration::from_secs(5))?;
        Self::ensure_greenfield_startup_eligible(conn)?;
        // busy_timeout=5s、WAL、NORMAL、cache_size=-8000（8MB）、
        // temp_store=MEMORY、enforceForeignKeys。
        // journal_mode 为「有返回值」的 PRAGMA，须走查询而非 execute。
        let _mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "cache_size", -8000)?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        migrations::migrations::runner().run(conn)?;
        Self::validate_greenfield_schema(conn)?;
        Ok(())
    }

    /// Refuse to run the baseline over any unidentified existing application DB.
    /// An empty file (and refinery's empty bookkeeping table) is eligible for the
    /// one-shot migration; a previously initialized DB must already be exact.
    fn ensure_greenfield_startup_eligible(conn: &Connection) -> Result<(), DbError> {
        let tables = Self::application_tables(conn)?;
        if tables.is_empty() {
            return Ok(());
        }
        if !tables.contains(SCHEMA_METADATA_TABLE) {
            return Err(DbError::IncompatibleSchema(
                "GREENFIELD_DATABASE_REQUIRED: existing application tables have no final-schema \
                 identity; in-place upgrade and legacy backfill are disabled"
                    .to_owned(),
            ));
        }
        Self::validate_schema_identity(conn)?;
        Self::validate_final_table_set(&tables)
    }

    fn validate_greenfield_schema(conn: &Connection) -> Result<(), DbError> {
        Self::validate_schema_identity(conn)?;
        let tables = Self::application_tables(conn)?;
        Self::validate_final_table_set(&tables)
    }

    fn validate_schema_identity(conn: &Connection) -> Result<(), DbError> {
        let marker = conn
            .query_row(
                "SELECT schema_version,schema_kind,migration_mode,legacy_write_compatibility \
                   FROM zk_schema_metadata WHERE singleton=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| {
                DbError::IncompatibleSchema(format!("SCHEMA_IDENTITY_UNREADABLE: {error}"))
            })?;
        let Some((version, kind, mode, legacy_writes)) = marker else {
            return Err(DbError::IncompatibleSchema(
                "SCHEMA_IDENTITY_MISSING: final-schema marker row is absent".to_owned(),
            ));
        };
        if version != i64::from(GREENFIELD_SCHEMA_VERSION)
            || kind != GREENFIELD_SCHEMA_KIND
            || mode != DATABASE_MIGRATION_MODE
            || legacy_writes != i64::from(LEGACY_WRITE_COMPATIBILITY)
        {
            return Err(DbError::IncompatibleSchema(format!(
                "SCHEMA_IDENTITY_MISMATCH: expected version={GREENFIELD_SCHEMA_VERSION}, \
                 kind={GREENFIELD_SCHEMA_KIND}, mode={DATABASE_MIGRATION_MODE}, legacyWrites=0"
            )));
        }
        Ok(())
    }

    fn application_tables(conn: &Connection) -> Result<BTreeSet<String>, DbError> {
        let mut statement =
            conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut tables = BTreeSet::new();
        for row in rows {
            let table = row?;
            if !table.starts_with("sqlite_") && table != REFINERY_HISTORY_TABLE {
                tables.insert(table);
            }
        }
        Ok(tables)
    }

    fn validate_final_table_set(tables: &BTreeSet<String>) -> Result<(), DbError> {
        let expected: BTreeSet<String> = FINAL_SCHEMA_TABLES
            .iter()
            .map(|table| (*table).to_owned())
            .collect();
        if tables == &expected {
            return Ok(());
        }
        let missing: Vec<&str> = expected.difference(tables).map(String::as_str).collect();
        let unexpected: Vec<&str> = tables.difference(&expected).map(String::as_str).collect();
        Err(DbError::IncompatibleSchema(format!(
            "FINAL_SCHEMA_TABLE_SET_MISMATCH: missing={missing:?}, unexpected={unexpected:?}"
        )))
    }

    /// 打开一条只读 reader 连接并施加读侧 PRAGMA。
    ///
    /// 读侧不设 `journal_mode`/`synchronous`（前者是库文件属性、由 writer
    /// 已置 WAL；后者仅约束写路径），其余沿用 writer 同款。
    fn open_reader(path: &Path) -> Result<Connection, DbError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "cache_size", -8000)?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        Ok(conn)
    }

    /// 收紧 zkcode 自有运行目录权限。
    ///
    /// 仅处理本次调用新建的精确父目录，或名称明确为 `.zk` / `.zkcode` 的
    /// 已有目录；绝不修改任意已有用户目录（例如 workspace 根目录或 HOME）。
    #[cfg(unix)]
    fn restrict_runtime_directory(path: &Path, created_by_us: bool) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let is_known_runtime_dir = path
            .file_name()
            .is_some_and(|name| name == ".zk" || name == ".zkcode");
        if created_by_us || is_known_runtime_dir {
            let mut permissions = std::fs::metadata(path)?.permissions();
            permissions.set_mode(0o700);
            std::fs::set_permissions(path, permissions)?;
        }
        Ok(())
    }

    /// 非 Unix 平台保持原行为；zkcode 当前仅发布 macOS，但该空实现保证
    /// crate 的跨平台编译边界清晰。
    #[cfg(not(unix))]
    fn restrict_runtime_directory(_path: &Path, _created_by_us: bool) -> std::io::Result<()> {
        Ok(())
    }

    #[cfg(unix)]
    fn prepare_database_file(path: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::OpenOptionsExt;

        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;
        Self::restrict_file_permissions(path)
    }

    #[cfg(not(unix))]
    fn prepare_database_file(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    /// 若文件存在，将权限设为仅当前用户可读写。WAL/SHM 是按需文件，尚未
    /// 生成时不视为错误。
    #[cfg(unix)]
    fn restrict_file_permissions(path: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = match std::fs::metadata(path) {
            Ok(metadata) => metadata.permissions(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions)
    }

    #[cfg(not(unix))]
    fn restrict_file_permissions(_path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    /// 复核数据库主体及 `SQLite` 的两个并行副文件。
    fn restrict_sqlite_files(path: &Path) -> std::io::Result<()> {
        Self::restrict_file_permissions(path)?;
        Self::restrict_file_permissions(&Self::sqlite_sidecar_path(path, "-wal"))?;
        Self::restrict_file_permissions(&Self::sqlite_sidecar_path(path, "-shm"))?;
        Ok(())
    }

    fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        PathBuf::from(sidecar)
    }

    /// 读路径统一出口：轮转选取只读连接 → `spawn_blocking` 执行。
    ///
    /// 内存库（无 reader 池）回落 [`Self::with_writer`]，读写语义不变。
    /// 误将写操作路由到本方法时由只读连接在 `SQLite` 层报错兜底。
    ///
    /// `pub` 可见性（2.5）：`zk-authz` 的 `GrantStore` 与 `zk-server` 的交互仓储
    /// 在本 crate 之外自持 SQL，但必须复用同一读写出口——否则会绕过 reader
    /// 池轮转与 `spawn_blocking` 派发。
    ///
    /// # Errors
    /// `job` 自身返回的错误原样上抛；`spawn_blocking` 任务 panic 时返回
    /// [`DbError`] 的 join 失败变体。
    pub async fn with_reader<T, F>(&self, job: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut Connection) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static,
    {
        let Some(pool) = &self.readers else {
            return self.with_writer(job).await;
        };
        let pool = Arc::clone(pool);
        tokio::task::spawn_blocking(move || {
            let idx = pool.next.fetch_add(1, Ordering::Relaxed) % pool.conns.len();
            let mut guard = pool.conns[idx]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            job(&mut guard)
        })
        .await?
    }

    /// 写路径统一出口：锁 writer 连接 → `spawn_blocking` 执行。
    ///
    /// 互斥锁中毒恢复策略：持锁期间的业务 panic **不代表连接损坏**
    ///（rusqlite Connection 无进程内内存不变形，SQLite 自身有跨进程崩溃
    /// 恢复语义），故取 `into_inner` 继续服务而非毒化传播——对单用户本地
    /// 工具，可用性优先于「错误快失败」。
    ///
    /// `pub` 可见性理由同 [`Self::with_reader`]：外部 crate 的写事务（授权记录、
    /// 交互 CAS 决策）必须经同一 writer `Mutex` 串行化。
    ///
    /// # Errors
    /// `job` 自身返回的错误原样上抛；`spawn_blocking` 任务 panic 时返回
    /// [`DbError`] 的 join 失败变体。
    pub async fn with_writer<T, F>(&self, job: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut Connection) -> Result<T, DbError> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            job(&mut guard)
        })
        .await?
    }

    /// 底层同步访问入口（`#[doc(hidden)]`，走 writer 连接）：仅供集成测试
    /// 直连布数（改 updated_at / 注入 subagent 元数据等无法经公开 API 表达
    /// 的旧库数据形态），生产代码禁止使用。
    #[doc(hidden)]
    pub fn with_conn_blocking<T, F>(&self, job: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut Connection) -> Result<T, DbError>,
    {
        let mut guard = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        job(&mut guard)
    }
}

/// 启动冒烟：内存库打开 → refinery 迁移 → PRAGMA 接线全链路。
/// （真实语义测试见 `src/*` 内联单测与 `tests/repository.rs` 集成测试；
/// 读写分离并发压测见 `tests/concurrency.rs`。）
#[test]
fn crate_boots() {
    let db = crate::Db::open_in_memory().expect("in-memory db boots with migrations");
    drop(db);
}

#[cfg(test)]
mod greenfield_schema_tests {
    use super::{
        DATABASE_MIGRATION_MODE, Db, GREENFIELD_SCHEMA_KIND, GREENFIELD_SCHEMA_VERSION,
        LEGACY_WRITE_COMPATIBILITY,
    };
    use crate::DbError;
    use rusqlite::Connection;
    use std::path::PathBuf;

    struct TestDatabase {
        root: PathBuf,
        path: PathBuf,
    }

    impl TestDatabase {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "zk-db-greenfield-{label}-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir(&root).expect("create test database directory");
            let path = root.join("data.db");
            Self { root, path }
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn expect_incompatible(error: DbError, code: &str) {
        match error {
            DbError::IncompatibleSchema(message) => assert!(
                message.contains(code),
                "expected {code} in schema failure, got {message}"
            ),
            other => panic!("expected incompatible schema, got {other}"),
        }
    }

    #[test]
    fn fresh_database_records_exact_greenfield_identity() {
        let db = Db::open_in_memory().expect("fresh database migrates");
        db.with_conn_blocking(|conn| {
            let identity: (i64, String, String, i64) = conn.query_row(
                "SELECT schema_version,schema_kind,migration_mode,legacy_write_compatibility \
                   FROM zk_schema_metadata WHERE singleton=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            assert_eq!(identity.0, i64::from(GREENFIELD_SCHEMA_VERSION));
            assert_eq!(identity.1, GREENFIELD_SCHEMA_KIND);
            assert_eq!(identity.2, DATABASE_MIGRATION_MODE);
            assert_eq!(identity.3, i64::from(LEGACY_WRITE_COMPATIBILITY));
            Ok(())
        })
        .expect("read schema identity");
    }

    #[test]
    fn existing_legacy_database_is_rejected_without_being_overlaid() {
        let database = TestDatabase::new("legacy");
        let connection = Connection::open(&database.path).expect("open legacy fixture");
        connection
            .execute_batch(
                "CREATE TABLE sessions(id TEXT PRIMARY KEY);\
                 INSERT INTO sessions(id) VALUES('legacy-session');",
            )
            .expect("seed legacy fixture");
        drop(connection);

        let Err(error) = Db::open(&database.path) else {
            panic!("legacy database must not be upgraded in place");
        };
        expect_incompatible(error, "GREENFIELD_DATABASE_REQUIRED");

        let connection = Connection::open(&database.path).expect("reopen legacy fixture");
        let rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
            .expect("legacy data remains readable");
        let marker_tables: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='zk_schema_metadata'",
                [],
                |row| row.get(0),
            )
            .expect("inspect table set");
        assert_eq!(rows, 1, "startup gate must preserve forensic data");
        assert_eq!(
            marker_tables, 0,
            "baseline must not be layered onto legacy DB"
        );
    }

    #[test]
    fn forged_or_partial_schema_identity_is_rejected() {
        let wrong = TestDatabase::new("wrong-identity");
        let connection = Connection::open(&wrong.path).expect("open wrong marker fixture");
        connection
            .execute_batch(
                "CREATE TABLE zk_schema_metadata(\
                    singleton INTEGER PRIMARY KEY, schema_version INTEGER, schema_kind TEXT,\
                    migration_mode TEXT, legacy_write_compatibility INTEGER);\
                 INSERT INTO zk_schema_metadata VALUES(1,3,'legacy','upgrade',1);",
            )
            .expect("seed wrong marker");
        drop(connection);
        let Err(error) = Db::open(&wrong.path) else {
            panic!("wrong schema identity must fail closed");
        };
        expect_incompatible(error, "SCHEMA_IDENTITY_MISMATCH");

        let partial = TestDatabase::new("partial-schema");
        let connection = Connection::open(&partial.path).expect("open partial fixture");
        connection
            .execute_batch(
                "CREATE TABLE zk_schema_metadata(\
                    singleton INTEGER PRIMARY KEY, schema_version INTEGER, schema_kind TEXT,\
                    migration_mode TEXT, legacy_write_compatibility INTEGER);\
                 INSERT INTO zk_schema_metadata \
                    VALUES(1,2,'greenfieldFinal','greenfieldOnly',0);\
                 CREATE TABLE sessions(id TEXT PRIMARY KEY);",
            )
            .expect("seed partial marker");
        drop(connection);
        let Err(error) = Db::open(&partial.path) else {
            panic!("partial final schema must not be repaired implicitly");
        };
        expect_incompatible(error, "FINAL_SCHEMA_TABLE_SET_MISMATCH");
    }
}

#[cfg(all(test, unix))]
mod unix_permission_tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "zk-db-permissions-{label}-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir(&path).expect("create unique test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = std::fs::metadata(path)
            .unwrap_or_else(|error| panic!("read permissions for {}: {error}", path.display()))
            .permissions();
        permissions.set_mode(mode);
        std::fs::set_permissions(path, permissions)
            .unwrap_or_else(|error| panic!("set permissions for {}: {error}", path.display()));
    }

    fn assert_mode(path: &Path, expected: u32) {
        let actual = std::fs::metadata(path)
            .unwrap_or_else(|error| panic!("read metadata for {}: {error}", path.display()))
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            actual,
            expected,
            "unexpected permissions for {}",
            path.display()
        );
    }

    fn sidecar(path: &Path, suffix: &str) -> PathBuf {
        let mut value = path.as_os_str().to_os_string();
        value.push(suffix);
        PathBuf::from(value)
    }

    fn assert_sqlite_files_private(path: &Path) {
        assert_mode(path, 0o600);
        assert_mode(&sidecar(path, "-wal"), 0o600);
        assert_mode(&sidecar(path, "-shm"), 0o600);
    }

    #[test]
    fn new_runtime_directory_and_sqlite_files_are_private() {
        let root = TestDirectory::create("new");
        let runtime_dir = root.0.join("runtime");
        let database = runtime_dir.join("data.db");

        let db = crate::Db::open(&database).expect("open database in new runtime directory");

        assert_mode(&runtime_dir, 0o700);
        assert_sqlite_files_private(&database);
        drop(db);
    }

    #[test]
    fn existing_arbitrary_parent_is_untouched_and_existing_files_are_repaired() {
        let root = TestDirectory::create("existing-parent");
        set_mode(&root.0, 0o751);
        let database = root.0.join("data.db");

        let first = crate::Db::open(&database).expect("create database");
        assert_mode(&root.0, 0o751);

        let wal = sidecar(&database, "-wal");
        let shm = sidecar(&database, "-shm");
        set_mode(&database, 0o666);
        set_mode(&wal, 0o666);
        set_mode(&shm, 0o666);

        let reopened = crate::Db::open(&database).expect("reopen database and repair permissions");
        assert_mode(&root.0, 0o751);
        assert_sqlite_files_private(&database);
        drop(reopened);
        drop(first);
    }

    #[test]
    fn existing_named_runtime_directory_is_hardened() {
        let root = TestDirectory::create("named-runtime");
        let runtime_dir = root.0.join(".zk");
        std::fs::create_dir(&runtime_dir).expect("create .zk directory");
        set_mode(&runtime_dir, 0o777);
        let database = runtime_dir.join("data.db");

        let db = crate::Db::open(&database).expect("open database in .zk directory");

        assert_mode(&runtime_dir, 0o700);
        assert_sqlite_files_private(&database);
        drop(db);
    }
}
