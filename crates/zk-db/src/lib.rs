//! zk-db：zkcode 持久化层——基于 `SQLite` 的会话/消息存储及迁移管理（S5）。
//!
//! crate 拓扑定位：位于 `zk-protocol` 之上的存储 crate，被 `zk-engine`（读写
//! 会话与消息）和 `zk-server`（组装）依赖。遵循 D6：单库默认 `zkcode/.zk/data.db`、
//! 对外全部 async fn（同步 `SQLite` 操作经 `spawn_blocking` 派发）、refinery 迁移
//! 启动自动执行、PRAGMA 全套（WAL / `busy_timeout=5s` / `foreign_keys` / NORMAL /
//! `cache_size` / `temp_store`，逐项照抄旧系统 `SqliteConfig` 的 Xerial 属性集）。
//!
//! # Phase 2 子阶段 2.0
//!
//! - schema：绿地基线扩至全量 25 张业务表（§12.8 清单 + 多路核查，见
//!   `migrations/V2__init_session_message.sql` 与 docs/phase2-schema-checklist.md）。
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
mod evidence;
mod memory;
mod message;
mod project;
mod project_context;
mod session;
mod snapshot;
mod swarm;
mod task;
mod workbench;

pub mod convert;
pub mod cursor;
pub mod error;
pub mod model;
pub mod run;
pub mod time;

pub use anomaly::AnomalyEventRecord;
pub use artifact::{ArtifactEntryRecord, ArtifactManifestRecord};
pub use checkpoint::{AgentCheckpointRecord, new_agent_checkpoint};
pub use error::DbError;
pub use evidence::{EvidenceBundleRecord, EvidenceItemRecord};
pub use memory::{MemoryRecord, MemoryUpsert};
pub use model::{
    ImageSource, MessagePage, MessageRecord, MessageRole, NewMessage, SessionDetail, SessionPage,
    SessionSummary, StoredBlock, goal_preview,
};
pub use project::{ProjectRecord, find_project_by_workspace_root_in_current_write};
pub use project_context::ProjectContextRecord;
pub use run::{RunEnvelopeView, RunEventView};
pub use session::SnapshotRestoreOutcome;
pub use snapshot::FileSnapshotRecord;
pub use swarm::SwarmRecord;
pub use task::{TaskRecord, new_task_record};
pub use workbench::{AcceptanceCriterionRecord, WorkbenchBindingRecord, WorkbenchRecord};

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut writer = Connection::open(path)?;
        Self::init_writer(&mut writer)?;
        let reader_count = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        let mut conns = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            conns.push(Mutex::new(Self::open_reader(path)?));
        }
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

    /// writer 连接初始化：`busy_timeout` 先行（迁移写入亦需锁兜底），再其余
    /// PRAGMA，最后跑 refinery 迁移（`embed_migrations!` 内嵌，见
    /// `migrations` 模块）。
    ///
    /// # Errors
    /// PRAGMA 执行失败、迁移失败或打开连接失败时返回。
    fn init_writer(conn: &mut Connection) -> Result<(), DbError> {
        // 对齐旧系统 SqliteConfig：busy_timeout=5s、WAL、NORMAL、
        // cache_size=-8000（8MB）、temp_store=MEMORY、enforceForeignKeys。
        conn.busy_timeout(Duration::from_secs(5))?;
        // journal_mode 为「有返回值」的 PRAGMA，须走查询而非 execute。
        let _mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "cache_size", -8000)?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        migrations::migrations::runner().run(conn)?;
        Ok(())
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
