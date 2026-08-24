//! `DurableInteractionService` 的逐行移植（旧源 `interaction/DurableInteractionService.java`，782 行）。
//!
//! # 唯一权威模型
//!
//! `interaction_requests` 行是**唯一**权威：状态、投递代次、决策截止与乐观锁
//! `version` 全部落库，进程内的唤醒 channel 只负责「把等待者叫醒」。因此
//! 重启后 pending 交互仍可被重新投递并决策——这是硬门禁「重启后 pending
//! interaction 重现可决策」的实现基础。
//!
//! # 生命周期
//!
//! ```text
//! create_authorization                       ← 授权链（AuthorizationService.interact）
//!   ├─ capacity.try_acquire()                （MAX_WAITING=32，满则请求终止 Run）
//!   ├─ write { ensure_run_waiting → INSERT pending → run 事件 interaction_created }
//!   └─ finish_created_interaction → 注册唤醒器 → 推 WS interaction_created
//! await_terminal                             ← 授权链阻塞点
//!   └─ 二次读取闭合竞争 → 按 delivery_window / decision_deadline 定时唤醒 → expire_if_due
//! mark_dispatched / claim_redelivery / prepare_recovery_delivery
//!   └─ delivery_generation 单调自增（旧页面无法确认新请求）
//! acknowledge_received(id, generation)       ← 前端 interaction_ack 上行
//!   └─ received_at + decision_deadline_at(+300s) → run 事件 interaction_updated
//! decide_request(id, version, terminal, ...) ← 前端 permission_response 上行
//!   └─ PERMISSION 六层协议校验 → CAS UPDATE → [remember → grant] → 3 类 run 事件
//!      → 该 run 无剩余 pending 则 mark_running（恢复引擎）
//! expire_deadlines（1s 定时器）/ begin_run_termination / reconcile_capacity_after_restart
//! ```
//!
//! # 与旧源的结构性偏离（详见 `docs/compatibility.md` §8）
//!
//! - **I-01**：旧源 `safePublish(new InteractionCreatedEvent(...))` 经 Spring 事件
//!   总线异步转给 WS 层；zkcode 直接调 [`WsHub::push`]。旧监听器唯一动作就是
//!   `pushInteractionView`，中间层无其他订阅者，语义等价。
//! - **I-02**：旧源 `RunTerminationRequestedEvent` 同样是事件总线消息，唯一消费者
//!   是 `RunTerminationCoordinator.onTerminationRequested`
//!   （`RunTerminationCoordinator.java:78-81`）。zkcode 以
//!   [`RunTerminationRequest`] 端口 + [`crate::run_termination::RunTerminationCoordinator`]
//!   实现表达；Spring 默认同步派发事件，故本端口同样在调用点内联 `await`。
//! - **I-03**：Java 的 5 层 `catch`（`DataIntegrityViolationException` /
//!   `DatabaseWriteUnavailableException` / `IllegalStateException` /
//!   `DataAccessException` / `RuntimeException`）在 Rust 统一为对
//!   [`DbError`] 的分类判定：唯一键违例（扩展码 2067/1555）走「复用已有交互」，
//!   其余写失败一律先 `find_after_create_failure` 再决定复用/上抛。
//! - **I-04**：`@Scheduled(fixedRate = 1000)` → [`DurableInteractionService::spawn_deadline_timer`]
//!   的 `tokio::time::interval(1s)`。
//! - **I-05**：`@PostConstruct reconcileCapacityAfterRestart` → 组装根显式
//!   `await` 调用（Rust 无容器生命周期回调）。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::{Map, Value, json};
use tokio::sync::{Semaphore, watch};
use zk_authz::interaction::{
    AuthorizationInteractionContext, AuthorizationInteractionSpec, InteractionRecord,
    InteractionStatus, InteractionType, PROTOCOL_VERSION,
};
use zk_authz::model::{AuthzError, AuthzResult, PermissionScope};
use zk_db::{Db, DbError, time};

use super::runs::{self, TransitionResult};

/// 未确认投递的窗口（旧 `DELIVERY_WINDOW_SECONDS`，L27）。
pub const DELIVERY_WINDOW_SECONDS: i64 = 30;
/// 单次投递的 ACK 窗口（旧 `ACK_WINDOW_SECONDS`，L28）。
pub const ACK_WINDOW_SECONDS: i64 = 5;
/// ACK 后的用户决策期限（旧 `DECISION_SECONDS`，L29）。
pub const DECISION_SECONDS: i64 = 300;
/// 同时待决交互上限（旧 `MAX_WAITING`，L30）。
pub const MAX_WAITING: usize = 32;

/// `interaction_requests` 全列 SELECT（旧 `SELECT *` + `map(rs, row)` 的显式列序版）。
const SELECT_COLUMNS: &str = "interaction_id,correlation_key,session_id,run_id,type,status,\
     prompt_json,allowed_decisions_json,scope_options_json,response_json,created_at,\
     delivery_window_ends_at,first_dispatched_at,delivery_ack_deadline_at,received_at,\
     decision_deadline_at,decided_at,terminal_reason,source,child_session_id,\
     delivery_generation,dispatch_attempts,last_transport_id,authorization_context_json,\
     updated_at,version";

/// 旧 `InteractionOperationException`（L32-35）的稳定错误码载体。
///
/// 旧源是 `RuntimeException` 子类且只携带 code；此处直接复用
/// [`AuthzError`]（同为 `code` + `message` 二元组），避免在 zk-server 内再造一套
/// 错误壳——授权链的调用方本就只读 `code`。
fn operation_error(code: &str, detail: impl std::fmt::Display) -> AuthzError {
    AuthzError::new(code, detail.to_string())
}

/// Run 终止请求端口（旧 `RunTerminationRequestedEvent` 的 Rust 替身，见 I-02）。
///
/// 三个入参逐一对齐旧 record 的三个字段
/// （`RunTerminationRequestedEvent.java`：`String runId` /
/// `RunEnvelope.RunExitReason reason` / `String detail`）。生产实现是
/// [`crate::run_termination::RunTerminationCoordinator`]——旧源里同样只有
/// `RunTerminationCoordinator` 一个监听者。
pub trait RunTerminationRequest: Send + Sync {
    /// 请求终止 Run。
    ///
    /// `exit_reason` 为 `run_envelopes.exit_reason` 字面量（旧
    /// `RunExitReason.dbValue()`，常量见 [`zk_db::run`]）；`detail` 保留旧 record
    /// 的**可空**语义（`None` → 旧 `detail == null`）。
    ///
    /// 返回 `Pin<Box<dyn Future>>` 而非 `async fn`：本端口须 `dyn` 分发
    /// （服务持 `Arc<dyn RunTerminationRequest>`），而 async fn in trait 不支持
    /// `dyn`，且 workspace 未引入 `async-trait`。
    ///
    /// 无返回值：旧源经 `safePublish`（`DurableInteractionService.java:753-760`）
    /// 派发，监听者抛出的 `RuntimeException` 只记 `log.error` 且不回滚已提交的
    /// 交互事务。实现方因此自行吞掉并记录失败。
    fn request<'a>(
        &'a self,
        run_id: &'a str,
        exit_reason: &'a str,
        detail: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// 交互视图/事件的下行出口（旧 Spring 事件监听器 `pushInteractionView`，见 I-01）。
pub trait InteractionPublisher: Send + Sync {
    /// 推 `interaction_created` / `interaction_updated` / `interaction_terminal`。
    fn publish(&self, session_id: &str, event_type: &'static str, record: &InteractionRecord);
}

/// 无出口实现（单测装配：只验证持久层与状态机，不涉 WS）。
#[derive(Debug, Default)]
pub struct NoopInteractionPublisher;

impl InteractionPublisher for NoopInteractionPublisher {
    fn publish(&self, _session_id: &str, _event_type: &'static str, _record: &InteractionRecord) {}
}

/// 通用交互创建规格（旧 `create(correlationKey, sessionId, runId, type, prompt,
/// decisions, scopes, source, childSessionId)`，L73-78 的九个入参）。
///
/// 授权交互走 [`DurableInteractionService::create_authorization`]（旧 L80-88），
/// 它额外校验协议版本并携带 [`AuthorizationInteractionContext`]；本结构刻意**不**
/// 暴露授权上下文字段，保证「PERMISSION 交互只能经协议校验入口创建」这一不变量
/// 在类型层成立（旧源靠 `createInternal` 私有 + 两个公开入口达到同一效果）。
#[derive(Debug, Clone)]
pub struct InteractionCreateSpec {
    /// 关联键（同 run 内唯一）。
    pub correlation_key: String,
    /// 归属会话。
    pub session_id: String,
    /// 归属 Run（缺失即 `INTERACTION_REQUIRES_RUN`）。
    pub run_id: Option<String>,
    /// 交互类型。
    pub kind: InteractionType,
    /// 下行 prompt（旧 `Object prompt`，序列化后落 `prompt_json`）。
    pub prompt: Value,
    /// 允许的决策字面量。
    pub allowed_decisions: Vec<String>,
    /// 可记住范围。
    pub scope_options: Vec<String>,
    /// 来源（`None`/空 → `direct`，旧 `source == null ? "direct" : source`）。
    pub source: Option<String>,
    /// 子会话 id（Agent 派生交互用）。
    pub child_session_id: Option<String>,
}

/// 取消一个 Run 的全部待决交互的结果（旧 `CancellationResult` record，L664-665）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancellationResult {
    /// Run 自身的迁移结果。
    pub run_transition: TransitionResult,
    /// 被级联取消的交互条数。
    pub interactions_cancelled: usize,
}

/// 持久交互服务（旧 `DurableInteractionService`）。
pub struct DurableInteractionService {
    db: Db,
    publisher: Arc<dyn InteractionPublisher>,
    terminations: Arc<dyn RunTerminationRequest>,
    /// 旧 `Semaphore capacity = new Semaphore(MAX_WAITING)`（L31）。
    capacity: Arc<Semaphore>,
    /// 旧 `Map<String, CompletableFuture<Status>> wakeups`（L36）。
    ///
    /// `watch` 而非 `oneshot`：旧 `CompletableFuture` 可被多个 `awaitTerminal`
    /// 调用者 `whenComplete` 挂载，`watch::Receiver` 同样支持多订阅者。
    wakeups: Mutex<HashMap<String, watch::Sender<InteractionStatus>>>,
    /// 过期定时器扫描轮次（测试断言定时器确实在跑）。
    sweeps: AtomicU64,
}

impl std::fmt::Debug for DurableInteractionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableInteractionService")
            .field("available_permits", &self.capacity.available_permits())
            .finish_non_exhaustive()
    }
}

// ══════════════════ 行映射 / 读路径 ══════════════════

/// 旧 `map(ResultSet, int)`（`DurableInteractionService.java:770-781`）。
///
/// 旧源 `Instant.parse` 在字面量非法时抛异常；zkcode 的时间列恒为
/// [`time::format_rfc3339_micros`] 产物，直接按 TEXT 透传（同构，无需解析）。
/// 未知 `type` / `status` 字面量在旧源抛 `IllegalArgumentException`
///（`Enum.valueOf`），此处映射为 [`DbError::Invalid`]（失败关闭）。
fn map_row(row: &Row<'_>) -> Result<InteractionRecord, rusqlite::Error> {
    let kind: String = row.get(4)?;
    let status: String = row.get(5)?;
    let parse = |value: &str, label: &str| -> Result<(), rusqlite::Error> {
        Err(rusqlite::Error::InvalidColumnType(
            0,
            format!("INTERACTION_ENUM_INVALID:{label}:{value}"),
            rusqlite::types::Type::Text,
        ))
    };
    let Some(kind) = InteractionType::from_db(&kind) else {
        parse(&kind, "type")?;
        unreachable!()
    };
    let Some(status) = InteractionStatus::from_db(&status) else {
        parse(&status, "status")?;
        unreachable!()
    };
    Ok(InteractionRecord {
        interaction_id: row.get(0)?,
        correlation_key: row.get(1)?,
        session_id: row.get(2)?,
        run_id: row.get(3)?,
        kind,
        status,
        prompt_json: row.get(6)?,
        allowed_decisions_json: row.get(7)?,
        scope_options_json: row.get(8)?,
        response_json: row.get(9)?,
        created_at: row.get(10)?,
        delivery_window_ends_at: row.get(11)?,
        first_dispatched_at: row.get(12)?,
        delivery_ack_deadline_at: row.get(13)?,
        received_at: row.get(14)?,
        decision_deadline_at: row.get(15)?,
        decided_at: row.get(16)?,
        terminal_reason: row.get(17)?,
        source: row.get(18)?,
        child_session_id: row.get(19)?,
        delivery_generation: row.get(20)?,
        dispatch_attempts: row.get(21)?,
        last_transport_id: row.get(22)?,
        authorization_context_json: row.get(23)?,
        updated_at: row.get(24)?,
        version: row.get(25)?,
    })
}

/// 旧 `findById(id)`（L487-489）的当前连接版。
///
/// # Errors
/// 查询失败或枚举字面量非法时返回 [`DbError`]。
pub fn find_by_id_in_tx(
    conn: &Connection,
    interaction_id: &str,
) -> Result<Option<InteractionRecord>, DbError> {
    let sql = format!("SELECT {SELECT_COLUMNS} FROM interaction_requests WHERE interaction_id=?1");
    Ok(conn
        .query_row(&sql, params![interaction_id], map_row)
        .optional()?)
}

/// 旧 `findByCorrelationKey(runId, key)`（L481-486）的当前连接版。
///
/// # Errors
/// 查询失败或枚举字面量非法时返回 [`DbError`]。
pub fn find_by_correlation_key_in_tx(
    conn: &Connection,
    run_id: &str,
    correlation_key: &str,
) -> Result<Option<InteractionRecord>, DbError> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS} FROM interaction_requests WHERE run_id=?1 AND correlation_key=?2"
    );
    Ok(conn
        .query_row(&sql, params![run_id, correlation_key], map_row)
        .optional()?)
}

/// 旧 `pending(sessionId)`（L428-431）的当前连接版。
///
/// # Errors
/// 查询失败或枚举字面量非法时返回 [`DbError`]。
pub fn pending_in_tx(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<InteractionRecord>, DbError> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS} FROM interaction_requests \
         WHERE session_id=?1 AND status='pending' ORDER BY created_at"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![session_id], map_row)?;
    let mut out = Vec::new();
    for record in rows {
        out.push(record?);
    }
    Ok(out)
}

/// 旧 `requireRunTransition(result, operation)`（L730-734）。
fn require_run_transition(result: TransitionResult, operation: &str) -> Result<(), DbError> {
    if result == TransitionResult::Applied {
        Ok(())
    } else {
        Err(DbError::Invalid(format!(
            "INTERACTION_RUN_TRANSITION_FAILED: {operation}: {}",
            result.as_str()
        )))
    }
}

/// 旧 `appendTerminalEventInCurrentWrite(request)`（L735-741）。
fn append_terminal_event_in_current_write(
    conn: &Connection,
    record: &InteractionRecord,
) -> Result<(), DbError> {
    runs::append_event_in_current_write(
        conn,
        &record.run_id,
        "interaction_terminal",
        None,
        &json!({
            "interactionId": record.interaction_id,
            "type": record.kind.db(),
            "status": record.status.db(),
            "terminalReason": record.terminal_reason.clone().unwrap_or_default(),
        }),
    )?;
    Ok(())
}

/// 旧 `ensureRunWaitingInCurrentWrite(runId, reason)`（L742-748）。
fn ensure_run_waiting_in_current_write(
    conn: &Connection,
    run_id: &str,
    reason: &str,
) -> Result<TransitionResult, DbError> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM run_envelopes WHERE id=?1",
            params![run_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(status) = status else {
        return Ok(TransitionResult::NotFound);
    };
    if status == "waiting_interaction" {
        return Ok(TransitionResult::Applied);
    }
    runs::mark_waiting_in_current_write(conn, run_id, reason)
}

/// `SQLite` 唯一键违例判定（旧 `DataIntegrityViolationException` 分支）。
///
/// 2067 = `SQLITE_CONSTRAINT_UNIQUE`，1555 = `SQLITE_CONSTRAINT_PRIMARYKEY`。
fn is_unique_violation(error: &DbError) -> bool {
    matches!(
        error,
        DbError::Sqlite(rusqlite::Error::SqliteFailure(inner, _))
            if inner.extended_code == 2067 || inner.extended_code == 1555
    )
}

// ══════════════════ 服务本体 ══════════════════

impl DurableInteractionService {
    /// 装配（旧 `@Autowired` 构造器）。
    #[must_use]
    pub fn new(
        db: Db,
        publisher: Arc<dyn InteractionPublisher>,
        terminations: Arc<dyn RunTerminationRequest>,
    ) -> Self {
        Self {
            db,
            publisher,
            terminations,
            capacity: Arc::new(Semaphore::new(MAX_WAITING)),
            wakeups: Mutex::new(HashMap::new()),
            sweeps: AtomicU64::new(0),
        }
    }

    /// 单测装配（无 WS 下行出口，终止端口仍是生产协调器）。
    #[doc(hidden)]
    #[must_use]
    pub fn for_tests(db: Db) -> Arc<Self> {
        crate::run_termination::assemble(db, Arc::new(NoopInteractionPublisher)).0
    }

    /// 剩余容量配额（旧 `capacity.availablePermits()`）。
    #[must_use]
    pub fn available_permits(&self) -> usize {
        self.capacity.available_permits()
    }

    /// 过期定时器已完成的扫描轮次（测试可观测）。
    #[must_use]
    pub fn sweeps(&self) -> u64 {
        self.sweeps.load(Ordering::Relaxed)
    }

    /// 旧 `findById(id)`（L487-489）。
    ///
    /// 旧源用 `queryForObject`，行不存在时抛 `EmptyResultDataAccessException`；
    /// 此处返回 `None`，由调用方决定失败关闭码（§8 I-06）。
    ///
    /// # Errors
    /// 读失败时返回 [`DbError`]。
    pub async fn find_by_id(
        &self,
        interaction_id: &str,
    ) -> Result<Option<InteractionRecord>, DbError> {
        let id = interaction_id.to_owned();
        self.db
            .with_reader(move |conn| find_by_id_in_tx(conn, &id))
            .await
    }

    /// 旧 `findByCorrelationKey(runId, key)`（L481-486）。
    ///
    /// # Errors
    /// 读失败时返回 [`DbError`]。
    pub async fn find_by_correlation_key(
        &self,
        run_id: &str,
        correlation_key: &str,
    ) -> Result<Option<InteractionRecord>, DbError> {
        let run_id = run_id.to_owned();
        let key = correlation_key.to_owned();
        self.db
            .with_reader(move |conn| find_by_correlation_key_in_tx(conn, &run_id, &key))
            .await
    }

    /// 旧 `pending(sessionId)`（L428-431）：会话待决交互（`ORDER BY created_at`）。
    ///
    /// # Errors
    /// 读失败时返回 [`DbError`]。
    pub async fn pending(&self, session_id: &str) -> Result<Vec<InteractionRecord>, DbError> {
        let session_id = session_id.to_owned();
        self.db
            .with_reader(move |conn| pending_in_tx(conn, &session_id))
            .await
    }

    /// 旧 `pendingViews(sessionId)`（L433-435）：断线重连 `session_restored` 的
    /// `pendingInteractions` 数据源。
    ///
    /// # Errors
    /// 读失败或存量授权上下文协议不符时返回 [`AuthzError`]。
    pub async fn pending_views(
        &self,
        session_id: &str,
    ) -> AuthzResult<Vec<zk_protocol::InteractionView>> {
        let records = self
            .pending(session_id)
            .await
            .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?;
        records.iter().map(Self::view).collect()
    }

    // ── 创建 ────────────────────────────────────────────────────────────

    /// 旧 `createAuthorization(...)`（`DurableInteractionService.java:60-70`）。
    ///
    /// # Errors
    /// 协议版本不为 3 → `PERMISSION_PROTOCOL_MISMATCH`；容量耗尽 →
    /// `INTERACTION_CAPACITY_EXCEEDED`；Run 非 running →
    /// `INTERACTION_RUN_NOT_RUNNING`；写失败 → `INTERACTION_STORE_FAILED`。
    pub async fn create_authorization(
        &self,
        spec: AuthorizationInteractionSpec,
    ) -> AuthzResult<InteractionRecord> {
        if spec.authorization_context.protocol_version != PROTOCOL_VERSION {
            return Err(operation_error(
                "PERMISSION_PROTOCOL_MISMATCH",
                "authorization context protocol version must be 3",
            ));
        }
        let authorization = spec.authorization_context;
        self.create_internal(
            InteractionCreateSpec {
                correlation_key: spec.correlation_key,
                session_id: spec.root_session_id,
                run_id: spec.run_id,
                kind: InteractionType::Permission,
                prompt: Value::Object(spec.prompt),
                allowed_decisions: spec.allowed_decisions,
                scope_options: spec.scope_options,
                source: Some(spec.source),
                child_session_id: None,
            },
            Some(authorization),
        )
        .await
    }

    /// 旧 `create(...)`（L73-78）：非授权交互（`ELICITATION` / `PLAN_APPROVAL` /
    /// `AGENT_HANDOFF` …）的创建入口，`authorization_context_json` 恒为 `NULL`。
    ///
    /// # Errors
    /// 同 [`Self::create_authorization`]，去掉协议版本一层。
    pub async fn create(&self, spec: InteractionCreateSpec) -> AuthzResult<InteractionRecord> {
        self.create_internal(spec, None).await
    }

    /// 旧 `createInternal(...)`（L86-192）。
    #[allow(clippy::too_many_lines)]
    async fn create_internal(
        &self,
        spec: InteractionCreateSpec,
        authorization: Option<AuthorizationInteractionContext>,
    ) -> AuthzResult<InteractionRecord> {
        // 旧源 L87-89：runId 缺失即拒（交互必须挂在某个 Run 上）。
        let Some(run_id) = spec.run_id.clone().filter(|id| !id.trim().is_empty()) else {
            return Err(operation_error(
                "INTERACTION_REQUIRES_RUN",
                "interaction requires a persisted run",
            ));
        };
        // 旧源 L94-102：容量闸门在任何写之前，且失败时请求终止 Run。
        let Ok(permit) = self.capacity.clone().try_acquire_owned() else {
            tracing::warn!(
                run_id,
                session_id = %spec.session_id,
                limit = MAX_WAITING,
                "interaction capacity exhausted"
            );
            self.terminations
                .request(
                    &run_id,
                    runs::EXIT_INTERACTION_CAPACITY_EXCEEDED,
                    Some("Maximum pending interactions reached"),
                )
                .await;
            return Err(operation_error(
                "INTERACTION_CAPACITY_EXCEEDED",
                "maximum pending interactions reached",
            ));
        };
        // 旧源 permit 用 `capacity.release()` 在每条失败路径显式归还；Rust 用
        // owned permit 的 `forget()`（成功路径）/ drop（失败路径）表达同一语义，
        // 且不可能漏归还。
        let now_millis = time::now_millis();
        let now = time::format_rfc3339_micros(now_millis);
        let deadline = time::format_rfc3339_micros(now_millis + DELIVERY_WINDOW_SECONDS * 1000);
        let id = uuid::Uuid::new_v4().to_string();
        // 旧源 L104-121：序列化失败 → INTERACTION_PAYLOAD_INVALID（先归还配额）。
        // serde_json 对 Map/Vec/struct 序列化不会失败，此分支静态不可达，但保留
        // 错误码路径以对齐旧契约。
        let prompt_json = serde_json::to_string(&spec.prompt)
            .map_err(|e| operation_error("INTERACTION_PAYLOAD_INVALID", e))?;
        let decisions_json = serde_json::to_string(&spec.allowed_decisions)
            .map_err(|e| operation_error("INTERACTION_PAYLOAD_INVALID", e))?;
        let scopes_json = serde_json::to_string(&spec.scope_options)
            .map_err(|e| operation_error("INTERACTION_PAYLOAD_INVALID", e))?;
        let authorization_json = authorization
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| operation_error("INTERACTION_PAYLOAD_INVALID", e))?;

        let created_event = Self::created_event_payload(&id, spec.kind, authorization.as_ref());
        let kind_db = spec.kind.db();
        let source = spec
            .source
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "direct".to_owned());
        let write = {
            let (id, run_id, session_id) = (id.clone(), run_id.clone(), spec.session_id.clone());
            let correlation_key = spec.correlation_key.clone();
            let child_session_id = spec.child_session_id.clone();
            let kind_db = kind_db.clone();
            self.db
                .with_writer(move |conn| {
                    let tx = conn.transaction()?;
                    let waiting = ensure_run_waiting_in_current_write(&tx, &run_id, &kind_db)?;
                    if waiting != TransitionResult::Applied {
                        return Ok(Err(operation_error(
                            "INTERACTION_RUN_NOT_RUNNING",
                            format!("INTERACTION_RUN_NOT_RUNNING: {}", waiting.as_str()),
                        )));
                    }
                    tx.execute(
                        "INSERT INTO interaction_requests(interaction_id,correlation_key,session_id,\
                           run_id,type,status,prompt_json,allowed_decisions_json,scope_options_json,\
                           created_at,delivery_window_ends_at,source,child_session_id,updated_at,\
                           authorization_context_json) \
                         VALUES(?1,?2,?3,?4,?5,'pending',?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                        params![
                            id,
                            correlation_key,
                            session_id,
                            run_id,
                            kind_db,
                            prompt_json,
                            decisions_json,
                            scopes_json,
                            now,
                            deadline,
                            source,
                            child_session_id,
                            now,
                            authorization_json
                        ],
                    )?;
                    runs::append_event_in_current_write(
                        &tx,
                        &run_id,
                        "interaction_created",
                        None,
                        &created_event,
                    )?;
                    tx.commit()?;
                    Ok(Ok(()))
                })
                .await
        };
        match write {
            Ok(Ok(())) => {
                permit.forget();
                self.finish_created_interaction(&id).await
            }
            // 旧源 L164-171：INTERACTION_RUN_NOT_RUNNING 包装后上抛（配额已归还）。
            Ok(Err(rejected)) => Err(rejected),
            Err(error) => {
                // 旧源 L152-163 + L172-181：唯一键竞争与写不可用都先尝试复用已有
                // 交互；命中且是**别人**创建的则归还配额并复用，命中且是自己创建
                // 的（写已提交但响应丢失）则照常收尾。
                let existing = self
                    .find_after_create_failure(&run_id, &spec.correlation_key)
                    .await;
                if let Some(existing) = existing {
                    if existing.interaction_id == id {
                        permit.forget();
                        return self.finish_created_interaction(&id).await;
                    }
                    tracing::debug!(
                        run_id,
                        correlation_key = %spec.correlation_key,
                        interaction_id = %existing.interaction_id,
                        "joined existing interaction after create failure"
                    );
                    return Ok(existing);
                }
                if is_unique_violation(&error) {
                    return Err(operation_error("INTERACTION_STORE_FAILED", error));
                }
                Err(operation_error("INTERACTION_STORE_FAILED", error))
            }
        }
    }

    /// 旧 `findAfterCreateFailure(runId, correlationKey, failure)`（L194-201）：
    /// 二次查询本身失败时旧源 `addSuppressed` 后返回 null，此处降级为 `None` + warn。
    async fn find_after_create_failure(
        &self,
        run_id: &str,
        correlation_key: &str,
    ) -> Option<InteractionRecord> {
        match self.find_by_correlation_key(run_id, correlation_key).await {
            Ok(found) => found,
            Err(error) => {
                tracing::warn!(run_id, %error, "interaction lookup after create failure failed");
                None
            }
        }
    }

    /// 旧 `finishCreatedInteraction(id)`（L203-208）：注册唤醒器 → 读回 → 推下行。
    async fn finish_created_interaction(&self, id: &str) -> AuthzResult<InteractionRecord> {
        self.register_wakeup(id);
        let created = self
            .find_by_id(id)
            .await
            .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?
            .ok_or_else(|| {
                operation_error("INTERACTION_STORE_FAILED", "created interaction vanished")
            })?;
        self.publisher
            .publish(&created.session_id, "interaction_created", &created);
        Ok(created)
    }

    /// 旧 `createInternal` 中 `interaction_created` run 事件的载荷（L133-148）。
    ///
    /// 旧源把 `AuthorizationDiagnostic.payload(...)` 的全部键 `putAll` 进
    /// `{interactionId,type,status}` 三键之上；此处调 [`zk_authz::diagnostic::payload`]
    /// 并做同样的合并，`Outcome.ASK` / `EvaluationStage.INTERACTION` /
    /// `Source.POLICY` / `USER_DECISION_REQUIRED` 逐字一致。非授权交互
    /// （`authorization == None`，旧 `create(...)` 路径）只落三键——旧源 L137 的
    /// `if (authorizationContext != null)` 同一分支。
    fn created_event_payload(
        id: &str,
        kind: InteractionType,
        authorization: Option<&AuthorizationInteractionContext>,
    ) -> Value {
        use zk_authz::diagnostic;
        use zk_authz::model::{DiagnosticOutcome, DiagnosticSource, EvaluationStage};
        use zk_authz::tool_facts::ToolUseContext;

        let mut merged = Map::new();
        merged.insert("interactionId".to_owned(), json!(id));
        merged.insert("type".to_owned(), json!(kind.db()));
        merged.insert("status".to_owned(), json!("pending"));
        let Some(authorization) = authorization else {
            return Value::Object(merged);
        };
        let subject = authorization.subject.to_subject();
        let context = ToolUseContext {
            current_run_id: Some(subject.current_run_id.clone()),
            tool_use_id: Some(authorization.tool_use_id.clone()),
            root_session_id: Some(subject.root_session_id.clone()),
            session_id: Some(subject.root_session_id.clone()),
            working_directory: None,
        };
        let diagnostic = diagnostic::payload(
            &subject,
            &authorization.operation,
            &context,
            &authorization.execution_attempt_id,
            DiagnosticOutcome::Ask,
            EvaluationStage::Interaction,
            DiagnosticSource::Policy,
            "USER_DECISION_REQUIRED",
            None,
            None,
            Some(id),
        );
        if let Value::Object(map) = diagnostic {
            merged.extend(map);
        }
        Value::Object(merged)
    }
    /// 旧 `finishCreatedInteraction` 之外的唤醒器注册点（旧 `wakeups.computeIfAbsent`）。
    ///
    /// 旧源是 `Map<String, CompletableFuture<Status>>`；Rust 用 `watch` channel：
    /// 初值 `PENDING`，终态时 `send` 一次即唤醒全部 `await_terminal` 等待者。
    fn register_wakeup(&self, id: &str) -> watch::Receiver<InteractionStatus> {
        let mut guard = self
            .wakeups
            .lock()
            .expect("interaction wakeups mutex poisoned");
        if let Some(sender) = guard.get(id) {
            return sender.subscribe();
        }
        let (sender, receiver) = watch::channel(InteractionStatus::Pending);
        guard.insert(id.to_owned(), sender);
        receiver
    }

    /// 旧 `finish(request)`（L727-732）：归还配额 → 唤醒等待者 → 推终态视图。
    fn finish(&self, record: &InteractionRecord) {
        self.capacity.add_permits(1);
        let sender = self
            .wakeups
            .lock()
            .expect("interaction wakeups mutex poisoned")
            .remove(&record.interaction_id);
        if let Some(sender) = sender
            && sender.send(record.status).is_err()
        {
            // 无接收者：等待者已走 `await_terminal` 的二次读取分支自行收敛。
            // 忽略即为旧源 `future.complete()` 对已完成 Future 的 no-op。
            tracing::trace!(
                interaction_id = %record.interaction_id,
                "no waiter for interaction wakeup"
            );
        }
        self.publisher
            .publish(&record.session_id, "interaction_terminal", record);
    }

    // ── 投递路径 ─────────────────────────────────────────────────────────

    /// 旧 `markDispatched(id, transportId)`（L211-228）：首投打点 + 代次自增。
    ///
    /// `COALESCE` 保证 `first_dispatched_at` / `delivery_ack_deadline_at` 只在首投
    /// 时写入；`updated != 1` 时回查「已 ACK」以保持幂等（前端 ACK 与本次投递
    /// 竞争时视为成功）。
    ///
    /// # Errors
    /// 写库失败时返回 [`DbError`]。
    pub async fn mark_dispatched(&self, id: &str, transport_id: &str) -> Result<bool, DbError> {
        let now_millis = time::now_millis();
        let now = time::format_rfc3339_micros(now_millis);
        let ack_deadline = time::format_rfc3339_micros(now_millis + ACK_WINDOW_SECONDS * 1000);
        let (id, transport_id) = (id.to_owned(), transport_id.to_owned());
        self.db
            .with_writer(move |conn| {
                let updated = conn.execute(
                    "UPDATE interaction_requests SET first_dispatched_at=COALESCE(first_dispatched_at,?1),\
                       delivery_ack_deadline_at=COALESCE(delivery_ack_deadline_at,?2),\
                       delivery_generation=delivery_generation+1,dispatch_attempts=dispatch_attempts+1,\
                       last_transport_id=?3,updated_at=?4 \
                     WHERE interaction_id=?5 AND status='pending' AND received_at IS NULL",
                    params![now, ack_deadline, transport_id, now, id],
                )?;
                if updated == 1 {
                    return Ok(true);
                }
                let acked: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM interaction_requests \
                     WHERE interaction_id=?1 AND status='pending' AND received_at IS NOT NULL",
                    params![id],
                    |row| row.get(0),
                )?;
                Ok(acked == 1)
            })
            .await
    }

    /// 旧 `claimRedelivery(id, transportId, expectedAttempts)`（L234-243）。
    ///
    /// `expectedAttempts ∈ [1, 4)` 是硬闸门：重投最多 3 次，且必须携带**读到的**
    /// `dispatch_attempts` 做 CAS，避免两个通道同时重投把代次推到无法 ACK。
    ///
    /// # Errors
    /// 写库失败时返回 [`DbError`]。
    pub async fn claim_redelivery(
        &self,
        id: &str,
        transport_id: &str,
        expected_attempts: i64,
    ) -> Result<bool, DbError> {
        if !(1..4).contains(&expected_attempts) {
            return Ok(false);
        }
        let now = time::format_rfc3339_micros(time::now_millis());
        let (id, transport_id) = (id.to_owned(), transport_id.to_owned());
        self.db
            .with_writer(move |conn| {
                let updated = conn.execute(
                    "UPDATE interaction_requests SET dispatch_attempts=dispatch_attempts+1,\
                       last_transport_id=?1,delivery_generation=delivery_generation+1,updated_at=?2 \
                     WHERE interaction_id=?3 AND status='pending' AND received_at IS NULL \
                       AND dispatch_attempts=?4 AND delivery_window_ends_at>?5",
                    params![transport_id, now, id, expected_attempts, now],
                )?;
                Ok(updated == 1)
            })
            .await
    }

    /// 旧 `redeliveryCandidates()`（L245-262）：待重投候选 + 1s/2s/4s 退避过滤。
    ///
    /// SQL 侧只筛 `dispatch_attempts BETWEEN 1 AND 3` 且投递窗口未过；退避阈值在
    /// 内存按 `first_dispatched_at` 的年龄过滤，与旧源 `switch` 逐格一致。
    ///
    /// # Errors
    /// 读库失败时返回 [`DbError`]。
    pub async fn redelivery_candidates(&self) -> Result<Vec<InteractionRecord>, DbError> {
        let now_millis = time::now_millis();
        let now = time::format_rfc3339_micros(now_millis);
        let rows = self
            .db
            .with_reader(move |conn| {
                let sql = format!(
                    "SELECT {SELECT_COLUMNS} FROM interaction_requests \
                     WHERE status='pending' AND received_at IS NULL \
                       AND first_dispatched_at IS NOT NULL \
                       AND dispatch_attempts BETWEEN 1 AND 3 AND delivery_window_ends_at>?1 \
                     ORDER BY first_dispatched_at"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt
                    .query_map(params![now], map_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        Ok(rows
            .into_iter()
            .filter(|record| {
                let backoff = match record.dispatch_attempts {
                    1 => 1000,
                    2 => 2000,
                    3 => 4000,
                    _ => i64::MAX,
                };
                record
                    .first_dispatched_at
                    .as_deref()
                    .and_then(time::parse_rfc3339_millis)
                    .is_some_and(|dispatched| now_millis - dispatched >= backoff)
            })
            .collect())
    }

    /// 旧 `acknowledgeReceived(id, transportId, deliveryGeneration)`（L264-294） 。
    ///
    /// ACK 必须携带**投递代次**：`delivery_generation=?` 让上一次投递（旧页面/旧
    /// 连接）的迟到 ACK 无法把决策窗口打开。成功时写 `received_at` 与
    /// `decision_deadline_at(+300s)` 并落 `interaction_updated` run 事件；`updated != 1`
    /// 时回查同代次已 ACK 记录以保证幂等重复 ACK 返回 true。
    ///
    /// # Errors
    /// 写库失败时返回 [`DbError`]。
    pub async fn acknowledge_received(
        &self,
        id: &str,
        transport_id: Option<&str>,
        delivery_generation: i64,
    ) -> Result<bool, DbError> {
        let Some(transport_id) = transport_id else {
            return Ok(false);
        };
        if delivery_generation < 1 {
            tracing::warn!(
                interaction_id = id,
                delivery_generation,
                "rejected interaction ack with invalid delivery generation"
            );
            return Ok(false);
        }
        let now_millis = time::now_millis();
        let now = time::format_rfc3339_micros(now_millis);
        let decision_deadline_millis = now_millis + DECISION_SECONDS * 1000;
        let decision_deadline = time::format_rfc3339_micros(decision_deadline_millis);
        let (id, transport_id) = (id.to_owned(), transport_id.to_owned());
        self.db
            .with_writer(move |conn| {
                let tx = conn.transaction()?;
                let updated = tx.execute(
                    "UPDATE interaction_requests SET received_at=?1,decision_deadline_at=?2,\
                       last_transport_id=?3,updated_at=?4 \
                     WHERE interaction_id=?5 AND status='pending' AND received_at IS NULL \
                       AND delivery_generation=?6 AND delivery_window_ends_at>=?7",
                    params![
                        now,
                        decision_deadline,
                        transport_id,
                        now,
                        id,
                        delivery_generation,
                        now
                    ],
                )?;
                if updated == 1 {
                    let run_id: String = tx.query_row(
                        "SELECT run_id FROM interaction_requests WHERE interaction_id=?1",
                        params![id],
                        |row| row.get(0),
                    )?;
                    runs::append_event_in_current_write(
                        &tx,
                        &run_id,
                        "interaction_updated",
                        None,
                        &json!({
                            "interactionId": id,
                            "status": "pending",
                            "received": true,
                            "decisionDeadlineAt": decision_deadline_millis,
                        }),
                    )?;
                    tx.commit()?;
                    return Ok(true);
                }
                let already: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM interaction_requests WHERE interaction_id=?1 \
                       AND received_at IS NOT NULL AND delivery_generation=?2",
                    params![id, delivery_generation],
                    |row| row.get(0),
                )?;
                tx.commit()?;
                Ok(already == 1)
            })
            .await
    }

    /// 旧 `prepareRecoveryDelivery(id, transportId)`（L296-309）：断线重连前把代次
    /// 推进一格，使旧连接的 ACK 全部失效，然后回读最新行用于重推。
    ///
    /// 这是「断线重放不重复弹窗」的服务端一半：交互行本身不变（同
    /// `correlation_key` 命中既有行），只有投递代次前进。
    ///
    /// # Errors
    /// 写库失败时返回 [`DbError`]。
    pub async fn prepare_recovery_delivery(
        &self,
        id: &str,
        transport_id: &str,
    ) -> Result<Option<InteractionRecord>, DbError> {
        let now_millis = time::now_millis();
        let now = time::format_rfc3339_micros(now_millis);
        let ack_deadline = time::format_rfc3339_micros(now_millis + ACK_WINDOW_SECONDS * 1000);
        {
            let (id, transport_id) = (id.to_owned(), transport_id.to_owned());
            self.db
                .with_writer(move |conn| {
                    conn.execute(
                        "UPDATE interaction_requests SET delivery_generation=delivery_generation+1,\
                           dispatch_attempts=dispatch_attempts+1,last_transport_id=?1,\
                           delivery_ack_deadline_at=?2,updated_at=?3 \
                         WHERE interaction_id=?4 AND status='pending' AND received_at IS NULL",
                        params![transport_id, ack_deadline, now, id],
                    )?;
                    Ok(())
                })
                .await?;
        }
        self.find_by_id(id).await
    }

    // ── 决策路径 ─────────────────────────────────────────────────────────

    /// 旧 `decide(id, expectedVersion, terminal, response, reason)`（L313-316）。
    ///
    /// # Errors
    /// 见 [`Self::decide_request`]。
    pub async fn decide(
        &self,
        id: &str,
        expected_version: i64,
        terminal: InteractionStatus,
        response: Option<Value>,
        reason: Option<&str>,
    ) -> AuthzResult<InteractionStatus> {
        self.decide_request(id, expected_version, terminal, response, reason)
            .await
            .map(|record| record.status)
    }

    /// 旧 `decideRequest(id, expectedVersion, terminal, response, reason)`（L318-425）。
    ///
    /// 单事务内完成「交互终态 + 可选持久授权 + Run 恢复」，任一步失败整体回滚 ——
    /// 旧源注释明确这是为了避免「前端显示已允许，但授权或 Run 状态未落库」的分裂
    /// 状态。PERMISSION 交互额外走**六层协议校验**，任一层不过即拒绝且不落决策：
    ///
    /// 1. `authorization_context_json` 存在（否则 `PERMISSION_PROTOCOL_MISMATCH`）
    /// 2. `protocolVersion == 3` 且响应 `operationHash` 与存储一致（同上码）
    /// 3. 响应 `deliveryGeneration` == 当前代次，且 `received_at` 非空
    ///    （`PERMISSION_DELIVERY_STALE`——旧页面/未 ACK 的决策不作数）
    /// 4. `optionId` 必须在创建时冻结的 `options` 内（`PERMISSION_OPTION_NOT_ALLOWED`）
    /// 5. 响应的 `decision` / `scope` / `remember` 与选中项完全一致
    ///    （`PERMISSION_OPTION_MISMATCH`——前端不能自行拼装范围更大的授权）
    /// 6. `remember` 时 `scope` 必须可解析且与选中项一致（`PERMISSION_SCOPE_NOT_ALLOWED`）
    ///
    /// # Errors
    /// `terminal` 非 `ANSWERED`/`DENIED`/`CANCELLED` 时返回
    /// `INTERACTION_TERMINAL_INVALID`（旧 `IllegalArgumentException`）；六层校验任一
    /// 失败返回对应稳定码；写库不可用返回 `INTERACTION_STORE_FAILED`。
    #[allow(clippy::too_many_lines)]
    pub async fn decide_request(
        &self,
        id: &str,
        expected_version: i64,
        terminal: InteractionStatus,
        response: Option<Value>,
        reason: Option<&str>,
    ) -> AuthzResult<InteractionRecord> {
        if !matches!(
            terminal,
            InteractionStatus::Answered | InteractionStatus::Denied | InteractionStatus::Cancelled
        ) {
            // 旧源 L321-323：`IllegalArgumentException("Invalid user terminal status")`。
            return Err(operation_error(
                "INTERACTION_TERMINAL_INVALID",
                "Invalid user terminal status",
            ));
        }
        let now_millis = time::now_millis();
        let now = time::format_rfc3339_micros(now_millis);
        // 旧源 L325-327：序列化失败 → `INTERACTION_RESPONSE_INVALID`。
        let response_json = match response.as_ref() {
            None => None,
            Some(value) => Some(
                serde_json::to_string(value)
                    .map_err(|e| operation_error("INTERACTION_RESPONSE_INVALID", e))?,
            ),
        };
        let terminal_db = terminal.db();
        let applied = {
            let (id, now, terminal_db) = (id.to_owned(), now.clone(), terminal_db.clone());
            let response_json = response_json.clone();
            let response = response.clone();
            let reason = reason.map(str::to_owned);
            self.db
                .with_writer(move |conn| {
                    let tx = conn.transaction()?;
                    let Some(before) = find_by_id_in_tx(&tx, &id)? else {
                        return Ok(Ok(false));
                    };
                    if before.status != InteractionStatus::Pending
                        || before.version != expected_version
                    {
                        return Ok(Ok(false));
                    }
                    let mut authorization: Option<AuthorizationInteractionContext> = None;
                    let mut permission_response = Map::new();
                    if before.kind == InteractionType::Permission {
                        let parsed = match response.as_ref() {
                            // 旧源 L336-338：`responseJson == null` → `Map.of()`。
                            None => Map::new(),
                            Some(Value::Object(map)) => map.clone(),
                            Some(_) => {
                                return Ok(Err(operation_error(
                                    "PERMISSION_PROTOCOL_MISMATCH",
                                    "permission response must be a JSON object",
                                )));
                            }
                        };
                        let context = match decode_authorization_context(&before) {
                            Ok(context) => context,
                            Err(rejected) => return Ok(Err(rejected)),
                        };
                        if let Err(rejected) = validate_permission_response(&before, &context, &parsed)
                        {
                            return Ok(Err(rejected));
                        }
                        permission_response = parsed;
                        authorization = Some(context);
                    }
                    // 旧源 L365-370：CAS —— 只有仍 pending 且版本相符才落决策。
                    let updated = tx.execute(
                        "UPDATE interaction_requests SET status=?1,response_json=?2,decided_at=?3,\
                           terminal_reason=?4,updated_at=?5,version=version+1 \
                         WHERE interaction_id=?6 AND status='pending' AND version=?7",
                        params![
                            terminal_db,
                            response_json,
                            now,
                            reason,
                            now,
                            id,
                            expected_version
                        ],
                    )?;
                    if updated != 1 {
                        tx.commit()?;
                        return Ok(Ok(false));
                    }
                    let Some(record) = find_by_id_in_tx(&tx, &id)? else {
                        return Ok(Err(operation_error(
                            "INTERACTION_STORE_FAILED",
                            "decided interaction vanished",
                        )));
                    };
                    if let Some(authorization) = authorization.as_ref() {
                        if terminal == InteractionStatus::Answered
                            && permission_response.get("remember") == Some(&Value::Bool(true))
                        {
                            match persist_remembered_grant(
                                &tx,
                                &now,
                                now_millis,
                                &id,
                                &record,
                                authorization,
                                &permission_response,
                            ) {
                                Ok(()) => {}
                                Err(Ok(rejected)) => return Ok(Err(rejected)),
                                Err(Err(error)) => return Err(error),
                            }
                        }
                        // 旧源 L403-409：permission_resolved 事件（5 键）。
                        runs::append_event_in_current_write(
                            &tx,
                            &record.run_id,
                            "permission_resolved",
                            None,
                            &json!({
                                "interactionId": id,
                                "outcome": terminal_db,
                                "reasonCode": reason.clone().unwrap_or_default(),
                                "inputHash": authorization.input_hash,
                                "operationHash": authorization.operation_hash,
                            }),
                        )?;
                    }
                    append_terminal_event_in_current_write(&tx, &record)?;
                    // 旧源 L412-418：该 Run 已无待决交互 → 恢复 running（引擎继续）。
                    let remaining: i64 = tx.query_row(
                        "SELECT COUNT(*) FROM interaction_requests WHERE run_id=?1 AND status='pending'",
                        params![record.run_id],
                        |row| row.get(0),
                    )?;
                    if remaining == 0 {
                        let transition = runs::mark_running_in_current_write(&tx, &record.run_id)?;
                        require_run_transition(
                            transition,
                            "resume after final interaction decision",
                        )?;
                    }
                    tx.commit()?;
                    Ok(Ok(true))
                })
                .await
        };
        let applied = match applied {
            Ok(Ok(applied)) => applied,
            Ok(Err(rejected)) => return Err(rejected),
            Err(error) => return Err(operation_error("INTERACTION_STORE_FAILED", error)),
        };
        // 旧源 L421-433：事务外回读当前行，applied 才收尾（归还配额 + 唤醒 + 推终态）。
        let current = self
            .find_by_id(id)
            .await
            .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?
            .ok_or_else(|| {
                operation_error("INTERACTION_STORE_FAILED", "decided interaction vanished")
            })?;
        if applied {
            tracing::info!(
                interaction_id = id,
                run_id = %current.run_id,
                kind = %current.kind,
                status = %current.status,
                "interaction decided"
            );
            self.finish(&current);
        } else {
            tracing::debug!(
                interaction_id = id,
                expected_version,
                current_version = current.version,
                status = %current.status,
                "interaction decision lost CAS race"
            );
        }
        Ok(current)
    }

    // ── 等待与期限 ───────────────────────────────────────────────────────

    /// 旧 `awaitTerminal(id)`（L535-556）+ `waitForCurrentDeadline`（L563-599）。
    ///
    /// **只有数据库终态能结束等待**（旧源注释原文）：未收到 ACK 时等待完整投递
    /// 窗口，收到 ACK 后切到用户决策期限；短 ACK 期限只记录单次投递状态，不能提前
    /// 终结等待。Java 用 `CompletableFuture.delayedExecutor` 递归续期，Rust 用
    /// `loop` + `tokio::select!`：唤醒 channel 与期限定时器竞争，语义一致且无栈增长。
    ///
    /// 注册唤醒器后的**二次读取**闭合「首次读取 → 注册」之间的终态竞争，并清理竞争
    /// 中创建的孤立唤醒器——这一步不可省，否则决策恰好落在两次读取之间时等待者
    /// 永不醒。
    ///
    /// # Errors
    /// 交互不存在、期限列缺失（`INTERACTION_DEADLINE_MISSING`）或读库失败时返回错误。
    ///
    /// # Panics
    /// 唤醒器表的互斥锁被投毒时 panic（不可恢复的进程内状态损坏）。
    pub async fn await_terminal(&self, id: &str) -> AuthzResult<InteractionStatus> {
        let before = self.require_record(id).await?;
        if before.status.is_terminal() {
            return Ok(before.status);
        }
        let mut wakeup = self.register_wakeup(id);
        // 旧源 L546-553：二次读取 + 孤立唤醒器清理。
        let after_registration = self.require_record(id).await?;
        if after_registration.status.is_terminal() {
            self.wakeups
                .lock()
                .expect("interaction wakeups mutex poisoned")
                .remove(id);
            return Ok(after_registration.status);
        }
        loop {
            let current = self.require_record(id).await?;
            if current.status.is_terminal() {
                return Ok(current.status);
            }
            let deadline = if current.received_at.is_none() {
                current.delivery_window_ends_at.as_str()
            } else {
                current.decision_deadline_at.as_deref().unwrap_or_default()
            };
            let Some(deadline_millis) = time::parse_rfc3339_millis(deadline) else {
                return Err(operation_error(
                    "INTERACTION_DEADLINE_MISSING",
                    "interaction deadline column is missing",
                ));
            };
            // 旧源 L580：`deadline.plusSeconds(2)` —— 定时器晚 2 秒过闸，避免与
            // `expireDeadlines` 抢同一毫秒导致空转。
            let delay = (deadline_millis + 2000 - time::now_millis()).max(0);
            let delay = tokio::time::Duration::from_millis(u64::try_from(delay).unwrap_or(0));
            tokio::select! {
                changed = wakeup.changed() => {
                    if changed.is_err() {
                        // 唤醒器被 `finish` 摘除并 drop：回到循环顶部按 DB 权威判定。
                        continue;
                    }
                    let status = *wakeup.borrow_and_update();
                    if status.is_terminal() {
                        return Ok(status);
                    }
                }
                () = tokio::time::sleep(delay) => {
                    self.expire_if_due(id).await?;
                }
            }
        }
    }

    /// 旧 `findById` 的「必须存在」包装（旧源直接抛 `EmptyResultDataAccessException`）。
    async fn require_record(&self, id: &str) -> AuthzResult<InteractionRecord> {
        self.find_by_id(id)
            .await
            .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?
            .ok_or_else(|| operation_error("INTERACTION_NOT_FOUND", id))
    }

    /// 旧 `expireIfDue(id)`（L601-614）。
    async fn expire_if_due(&self, id: &str) -> AuthzResult<()> {
        let current = self.require_record(id).await?;
        if current.status.is_terminal() {
            return Ok(());
        }
        let now = time::now_millis();
        let due = |value: Option<&str>| -> bool {
            value
                .and_then(time::parse_rfc3339_millis)
                .is_some_and(|deadline| deadline <= now)
        };
        if current.received_at.is_none() {
            if due(Some(current.delivery_window_ends_at.as_str())) {
                self.expire(
                    id,
                    InteractionStatus::Undeliverable,
                    "delivery_not_acknowledged",
                )
                .await?;
            }
        } else if due(current.decision_deadline_at.as_deref()) {
            self.expire(id, InteractionStatus::Expired, "decision_deadline_exceeded")
                .await?;
        }
        Ok(())
    }

    /// 旧 `expireDeadlines()`（L688-703，`@Scheduled(fixedRate = 1000)`）。
    ///
    /// 两条 `ORDER BY created_at LIMIT 100` 查询分别收割「投递窗口耗尽（未 ACK）」
    /// 与「决策期限耗尽」的交互，逐个走 [`Self::expire`] 的 CAS 终结。
    ///
    /// # Errors
    /// 读写库失败时返回错误。
    pub async fn expire_deadlines(&self) -> AuthzResult<()> {
        let now = time::format_rfc3339_micros(time::now_millis());
        let undelivered = {
            let now = now.clone();
            self.db
                .with_reader(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT interaction_id FROM interaction_requests \
                         WHERE status='pending' AND received_at IS NULL \
                           AND delivery_window_ends_at<=?1 \
                         ORDER BY created_at LIMIT 100",
                    )?;
                    let ids = stmt
                        .query_map(params![now], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(ids)
                })
                .await
                .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?
        };
        for id in undelivered {
            self.expire(
                &id,
                InteractionStatus::Undeliverable,
                "delivery_not_acknowledged",
            )
            .await?;
        }
        let expired = self
            .db
            .with_reader(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT interaction_id FROM interaction_requests \
                     WHERE status='pending' AND decision_deadline_at<=?1 \
                     ORDER BY created_at LIMIT 100",
                )?;
                let ids = stmt
                    .query_map(params![now], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ids)
            })
            .await
            .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?;
        for id in expired {
            self.expire(
                &id,
                InteractionStatus::Expired,
                "decision_deadline_exceeded",
            )
            .await?;
        }
        self.sweeps.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 旧 `expire(id, status, reason)`（L705-726）。
    async fn expire(&self, id: &str, status: InteractionStatus, reason: &str) -> AuthzResult<bool> {
        let now = time::format_rfc3339_micros(time::now_millis());
        let applied = {
            let (id, status_db, reason) = (id.to_owned(), status.db(), reason.to_owned());
            self.db
                .with_writer(move |conn| {
                    let tx = conn.transaction()?;
                    let updated = tx.execute(
                        "UPDATE interaction_requests SET status=?1,terminal_reason=?2,decided_at=?3,\
                           updated_at=?4,version=version+1 \
                         WHERE interaction_id=?5 AND status='pending'",
                        params![status_db, reason, now, now, id],
                    )?;
                    if updated == 1
                        && let Some(record) = find_by_id_in_tx(&tx, &id)?
                    {
                        append_terminal_event_in_current_write(&tx, &record)?;
                    }
                    tx.commit()?;
                    Ok(updated == 1)
                })
                .await
                .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?
        };
        if applied {
            let record = self.require_record(id).await?;
            tracing::info!(
                interaction_id = id,
                run_id = %record.run_id,
                status = %status,
                reason,
                "interaction deadline reached"
            );
            self.finish(&record);
            // 旧源 L721-724：过期即请求终止 Run。逐值对齐——
            // `reason = RunExitReason.INTERACTION_EXPIRED`（undeliverable / expired
            // 两条路径同值），`detail = request.terminalReason()`
            // （`delivery_not_acknowledged` / `decision_deadline_exceeded`）。
            self.terminations
                .request(
                    &record.run_id,
                    runs::EXIT_INTERACTION_EXPIRED,
                    record.terminal_reason.as_deref(),
                )
                .await;
        }
        Ok(applied)
    }

    /// 旧 `@Scheduled(fixedRate = 1000) expireDeadlines` 的 Rust 载体（见 I-04）。
    ///
    /// 返回的 [`tokio::task::JoinHandle`] 由调用方持有；`Arc` 弱引用不适用——旧源
    /// 的调度器与 Bean 同生命周期，此处同样让 handle 随进程存活。
    pub fn spawn_deadline_timer(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Err(error) = service.expire_deadlines().await {
                    tracing::warn!(code = %error.code, "interaction deadline sweep failed");
                }
            }
        })
    }

    // ── Run 终止 ─────────────────────────────────────────────────────────

    /// 旧 `beginRunCancellation(runId, reason)`（L621-623）。
    ///
    /// # Errors
    /// 见 [`Self::begin_run_termination`]。
    pub async fn begin_run_cancellation(
        &self,
        run_id: &str,
        reason: &str,
    ) -> AuthzResult<CancellationResult> {
        self.begin_run_termination(run_id, "user_cancelled", reason)
            .await
    }

    /// 旧 `beginRunTermination(runId, exitReason, reason)`（L625-651）。
    ///
    /// 同事务内把 Run 原子切到 `cancelling` 并终结其**全部**待决交互；
    /// `updated != ids.len()` 时抛 `INTERACTION_CANCEL_COUNT_MISMATCH`（并发写入
    /// 导致的计数漂移必须整体回滚，不允许出现半取消状态）。
    ///
    /// # Errors
    /// 计数不符或写库失败时返回错误。
    pub async fn begin_run_termination(
        &self,
        run_id: &str,
        exit_reason: &str,
        reason: &str,
    ) -> AuthzResult<CancellationResult> {
        let now = time::format_rfc3339_micros(time::now_millis());
        let (run_id_owned, exit_reason_owned, reason_owned) =
            (run_id.to_owned(), exit_reason.to_owned(), reason.to_owned());
        let outcome = self
            .db
            .with_writer(move |conn| {
                let tx = conn.transaction()?;
                let transition =
                    runs::request_cancel_in_current_write(&tx, &run_id_owned, &exit_reason_owned)?;
                if transition != TransitionResult::Applied {
                    tx.commit()?;
                    return Ok(Ok((transition, Vec::new())));
                }
                let ids = {
                    let mut stmt = tx.prepare(
                        "SELECT interaction_id FROM interaction_requests \
                         WHERE run_id=?1 AND status='pending'",
                    )?;
                    stmt.query_map(params![run_id_owned], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<String>, _>>()?
                };
                if !ids.is_empty() {
                    let updated = tx.execute(
                        "UPDATE interaction_requests SET status='cancelled',terminal_reason=?1,\
                           decided_at=?2,updated_at=?3,version=version+1 \
                         WHERE run_id=?4 AND status='pending'",
                        params![reason_owned, now, now, run_id_owned],
                    )?;
                    if updated != ids.len() {
                        return Ok(Err(operation_error(
                            "INTERACTION_CANCEL_COUNT_MISMATCH",
                            format!("expected {} rows, updated {updated}", ids.len()),
                        )));
                    }
                    for id in &ids {
                        if let Some(record) = find_by_id_in_tx(&tx, id)? {
                            append_terminal_event_in_current_write(&tx, &record)?;
                        }
                    }
                }
                tx.commit()?;
                Ok(Ok((transition, ids)))
            })
            .await
            .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?;
        let (transition, ids) = match outcome {
            Ok(value) => value,
            Err(rejected) => return Err(rejected),
        };
        // 旧源 `completeCancelled(ids)`（L653-660）：逐个归还配额 + 唤醒 + 推终态。
        for id in &ids {
            if let Some(record) = self
                .find_by_id(id)
                .await
                .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?
            {
                self.finish(&record);
            }
        }
        Ok(CancellationResult {
            run_transition: transition,
            interactions_cancelled: ids.len(),
        })
    }

    /// 旧 `reconcileCapacityAfterRestart()`（L667-686，`@PostConstruct`）。
    ///
    /// 重启后先把「Run 已终态但交互仍 pending」的孤儿标为 `service_restart` 取消，
    /// 再按剩余 pending 数预扣信号量配额——超过 `MAX_WAITING` 或扣不下来即
    /// `INTERACTION_CAPACITY_CORRUPT`（宁可拒绝启动也不允许配额账目失真）。
    ///
    /// 此调用是「重启后 pending interaction 重现可决策」的前置：孤儿被清理，仍属活动
    /// Run 的 pending 交互被保留并重新占额，等待前端重连后重投。
    ///
    /// # Errors
    /// 写库失败或配额账目损坏时返回错误。
    pub async fn reconcile_capacity_after_restart(&self) -> AuthzResult<usize> {
        let now = time::format_rfc3339_micros(time::now_millis());
        let pending = self
            .db
            .with_writer(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "UPDATE interaction_requests SET status='cancelled',\
                       terminal_reason='service_restart',decided_at=?1,updated_at=?2,\
                       version=version+1 \
                     WHERE status='pending' AND run_id IN (\
                       SELECT id FROM run_envelopes \
                       WHERE status IN ('completed','failed','cancelled','interrupted'))",
                    params![now, now],
                )?;
                let pending: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM interaction_requests WHERE status='pending'",
                    [],
                    |row| row.get(0),
                )?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))?;
        let count = usize::try_from(pending).unwrap_or(usize::MAX);
        if count > MAX_WAITING {
            return Err(operation_error(
                "INTERACTION_CAPACITY_CORRUPT",
                format!("pending={count}"),
            ));
        }
        let requested = u32::try_from(count).map_err(|_| {
            operation_error("INTERACTION_CAPACITY_CORRUPT", format!("pending={count}"))
        })?;
        // 旧源 `capacity.tryAcquire(count)`：非阻塞预扣，扣不下来即视为账目损坏。
        let Ok(permits) = self.capacity.clone().try_acquire_many_owned(requested) else {
            return Err(operation_error(
                "INTERACTION_CAPACITY_CORRUPT",
                format!("pending={count}"),
            ));
        };
        permits.forget();
        tracing::info!(
            pending = count,
            available = self.capacity.available_permits(),
            "interaction capacity reconciled after restart"
        );
        Ok(count)
    }

    // ── ONCE 复检 + 视图 ─────────────────────────────────────────────────

    /// 旧 `requireAnsweredOnce(interactionId, subject, operation, toolUseId)`（L492-533）。
    ///
    /// `USER_ONCE` 授权的**最终权威复检**，旧源注释要求「必须在 Gateway 的有界写
    /// 事务中调用」——一次性批准若不在执行同一事务内复检，就能在「检查」与「执行」
    /// 之间被并发复用。8 项联合判定（协议版本 / `toolUseId` / `inputHash` /
    /// `operationHash` / 主体 4 字段全等 / 选项 `decision==allow` / 选项
    /// `scope==once` / 响应回传的 `operationHash`）任一不符即 `..._STALE`。
    ///
    /// # Errors
    /// `interaction_id` 为空 → `PERMISSION_ONCE_DECISION_MISSING`；状态/主体/操作
    /// 不符 → `PERMISSION_ONCE_DECISION_STALE`；载荷不可解析 →
    /// `PERMISSION_ONCE_DECISION_INVALID`。
    pub fn require_answered_once(
        conn: &Connection,
        interaction_id: Option<&str>,
        subject: &zk_authz::model::AuthorizationSubject,
        operation: &zk_authz::model::OperationDescriptor,
        tool_use_id: &str,
    ) -> AuthzResult<()> {
        let Some(interaction_id) = interaction_id else {
            return Err(operation_error(
                "PERMISSION_ONCE_DECISION_MISSING",
                "once decision requires an interaction id",
            ));
        };
        let stale = || {
            operation_error(
                "PERMISSION_ONCE_DECISION_STALE",
                "one-time approval is no longer valid",
            )
        };
        let invalid = |detail: &str| operation_error("PERMISSION_ONCE_DECISION_INVALID", detail);
        let row = conn
            .query_row(
                "SELECT status,run_id,response_json,authorization_context_json \
                 FROM interaction_requests WHERE interaction_id=?1",
                params![interaction_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| invalid(&error.to_string()))?;
        // 旧源 `queryForMap` 行缺失时抛 `EmptyResultDataAccessException`（→ 500，
        // 不执行工具）；此处按失败关闭归入 `..._STALE`。
        let Some((status, run_id, response_json, context_json)) = row else {
            return Err(stale());
        };
        if status != "answered" || run_id != subject.current_run_id {
            return Err(stale());
        }
        let stored: AuthorizationInteractionContext = context_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| invalid(&error.to_string()))?
            .ok_or_else(|| invalid("authorization context is absent"))?;
        let response: Map<String, Value> = response_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| invalid(&error.to_string()))?
            .ok_or_else(|| invalid("interaction response is absent"))?;
        let selected_option_id = string_value(response.get("optionId"));
        // 旧源 `findFirst().orElseThrow()` 抛 `NoSuchElementException`（非
        // `IllegalStateException`）→ 落到 `..._INVALID` 分支。
        let option = stored
            .options
            .iter()
            .find(|candidate| candidate.option_id == selected_option_id)
            .ok_or_else(|| invalid("selected option is not present in stored options"))?;
        let exact_subject = stored.subject.root_session_id == subject.root_session_id
            && stored.subject.root_run_id == subject.root_run_id
            && stored.subject.current_run_id == subject.current_run_id
            && stored.subject.workspace_key == subject.workspace_key;
        if stored.protocol_version != PROTOCOL_VERSION
            || stored.tool_use_id != tool_use_id
            || stored.input_hash != operation.input_hash
            || stored.operation_hash != operation.operation_hash
            || !exact_subject
            || option.decision != "allow"
            || option.scope != "once"
            || response.get("operationHash") != Some(&Value::String(stored.operation_hash.clone()))
        {
            return Err(stale());
        }
        Ok(())
    }

    /// 旧 `view(request)`（L437-479）：交互行 → 下行视图。
    ///
    /// PERMISSION 交互额外投出 `protocolVersion=3` / `operationHash` /
    /// `actorRunId` / `actorType`（`rootRunId == currentRunId ? direct : descendant`）
    /// 与冻结的 `options`；非 PERMISSION 交互 `protocolVersion=2` 且 `options` 为空。
    /// `source` 字段在有 `actorType` 时被其覆盖（旧源 L472 三元）。
    ///
    /// # Errors
    /// 授权上下文缺失或协议版本不符时返回 `PERMISSION_PROTOCOL_MISMATCH`；
    /// 其余载荷不可解析时返回 `INTERACTION_PROTOCOL_INVALID`。
    pub fn view(record: &InteractionRecord) -> AuthzResult<zk_protocol::InteractionView> {
        let invalid = |detail: String| operation_error("INTERACTION_PROTOCOL_INVALID", detail);
        let prompt: Value =
            serde_json::from_str(&record.prompt_json).map_err(|e| invalid(e.to_string()))?;
        let decisions: Vec<String> = serde_json::from_str(&record.allowed_decisions_json)
            .map_err(|e| invalid(e.to_string()))?;
        let scopes: Vec<String> =
            serde_json::from_str(&record.scope_options_json).map_err(|e| invalid(e.to_string()))?;
        let response: Option<Value> = record
            .response_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e| invalid(e.to_string()))?;
        let mut protocol = 2;
        let mut operation_hash = None;
        let mut actor_run_id = None;
        let mut actor_type = None;
        let mut options = Vec::new();
        if record.kind == InteractionType::Permission {
            let context = decode_authorization_context(record)?;
            protocol = context.protocol_version;
            operation_hash = Some(context.operation_hash.clone());
            let current = context.subject.current_run_id.clone();
            actor_type = Some(
                if context.subject.root_run_id == current {
                    "direct"
                } else {
                    "descendant"
                }
                .to_owned(),
            );
            actor_run_id = Some(current);
            options = context
                .options
                .iter()
                .map(|option| {
                    json!({
                        "optionId": option.option_id,
                        "decision": option.decision,
                        "scope": option.scope,
                    })
                })
                .collect();
        }
        Ok(zk_protocol::InteractionView {
            interaction_id: record.interaction_id.clone(),
            protocol_version: Some(protocol),
            correlation_key: Some(record.correlation_key.clone()),
            session_id: Some(record.session_id.clone()),
            run_id: Some(record.run_id.clone()),
            interaction_type: Some(record.kind.db()),
            status: Some(record.status.db()),
            prompt: Some(prompt),
            allowed_decisions: Some(decisions),
            scope_options: Some(scopes),
            response,
            source: Some(actor_type.clone().unwrap_or_else(|| record.source.clone())),
            child_session_id: record.child_session_id.clone(),
            actor_run_id,
            actor_type,
            delivery_generation: Some(record.delivery_generation),
            dispatch_attempts: Some(record.dispatch_attempts),
            created_at: epoch(Some(record.created_at.as_str())),
            received_at: epoch(record.received_at.as_deref()),
            decision_deadline_at: epoch(record.decision_deadline_at.as_deref()),
            delivery_window_ends_at: epoch(Some(record.delivery_window_ends_at.as_str())),
            decided_at: epoch(record.decided_at.as_deref()),
            terminal_reason: record.terminal_reason.clone(),
            version: Some(record.version),
            server_now: Some(time::now_millis()),
            operation_hash,
            options: Some(options),
        })
    }

    /// 旧 ACK 路径的四字段子集视图（`WebSocketController.java:1207-1211`）。
    ///
    /// 旧源 ACK 成功后回查交互再推 `interaction_updated`，载荷**只有**
    /// `interactionId` / `decisionDeadlineAt` / `serverNow` / `version` 四项——
    /// 不是完整 [`Self::view`]。协议层 `InteractionView` 全 Option 字段兼容两种
    /// 形状（见 `server_message.rs` 的 `InteractionUpdated` 建模说明）。
    ///
    /// # Panics
    ///
    /// 四字段子集由本函数就地构造，形状静态可知；若 `InteractionView` 的协议面
    /// 被改成与该子集不兼容，则 panic（编译期无法拦截的协议冻结面回归）。
    #[must_use]
    pub fn ack_view(record: &InteractionRecord) -> zk_protocol::InteractionView {
        // 全 Option 字段以 JSON 反序列化构造四字段子集：协议 crate 是冻结面，
        // 不为组装根新增 builder（27 字段 struct 字面量会掩盖「只投四项」的语义）。
        let payload = serde_json::json!({
            "interactionId": record.interaction_id,
            "decisionDeadlineAt": epoch(record.decision_deadline_at.as_deref()),
            "serverNow": time::now_millis(),
            "version": record.version,
        });
        serde_json::from_value(payload).expect("ack view subset is statically valid")
    }
}

// ══════════════════ 自由辅助函数 ══════════════════

/// ISO 微秒文本 → `FlexEpoch`（协议层时间字段恒以 epoch 毫秒下行）。
fn epoch(value: Option<&str>) -> Option<zk_protocol::FlexEpoch> {
    value
        .and_then(time::parse_rfc3339_millis)
        .map(zk_protocol::FlexEpoch::from_millis)
}

/// 旧 `String.valueOf(map.get(key))`：缺失键得到字面量 `"null"`。
fn string_value(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "null".to_owned(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

/// 解析并校验 `authorization_context_json`（旧 `decideRequest` L339-347 与
/// `view` L451-459 共用的两步：存在性 + `protocolVersion == 3`）。
fn decode_authorization_context(
    record: &InteractionRecord,
) -> AuthzResult<AuthorizationInteractionContext> {
    let mismatch = || {
        operation_error(
            "PERMISSION_PROTOCOL_MISMATCH",
            "stored authorization context is absent or of an unsupported protocol version",
        )
    };
    let stored = record
        .authorization_context_json
        .as_deref()
        .ok_or_else(mismatch)?;
    let context: AuthorizationInteractionContext =
        serde_json::from_str(stored).map_err(|_| mismatch())?;
    if context.protocol_version != PROTOCOL_VERSION {
        return Err(mismatch());
    }
    Ok(context)
}

/// 旧 `decideRequest` 的 PERMISSION 协议校验第 2-5 层（L344-364）。
fn validate_permission_response(
    before: &InteractionRecord,
    context: &AuthorizationInteractionContext,
    response: &Map<String, Value>,
) -> AuthzResult<()> {
    // 第 2 层：响应必须回传与存储一致的 operationHash。
    if response.get("operationHash") != Some(&Value::String(context.operation_hash.clone())) {
        return Err(operation_error(
            "PERMISSION_PROTOCOL_MISMATCH",
            "response operationHash does not match the stored authorization",
        ));
    }
    // 第 3 层：投递代次必须是当前代次，且必须已 ACK。
    let claimed_generation = match response.get("deliveryGeneration") {
        None => -1,
        Some(Value::Number(number)) => number.as_i64().ok_or_else(|| {
            operation_error(
                "PERMISSION_DELIVERY_STALE",
                "deliveryGeneration is not an integer",
            )
        })?,
        Some(_) => {
            return Err(operation_error(
                "PERMISSION_DELIVERY_STALE",
                "deliveryGeneration is not a number",
            ));
        }
    };
    if before.delivery_generation != claimed_generation || before.received_at.is_none() {
        return Err(operation_error(
            "PERMISSION_DELIVERY_STALE",
            "decision refers to a superseded or unacknowledged delivery",
        ));
    }
    // 第 4 层：optionId 必须命中创建时冻结的选项集。
    let option_id = string_value(response.get("optionId"));
    let selected = context
        .options
        .iter()
        .find(|option| option.option_id == option_id)
        .ok_or_else(|| {
            operation_error(
                "PERMISSION_OPTION_NOT_ALLOWED",
                "selected option was not offered",
            )
        })?;
    // 第 5 层：decision / scope / remember 必须与选中项完全一致。
    let selected_remember = selected.decision == "allow" && selected.scope != "once";
    let claimed_remember = response.get("remember") == Some(&Value::Bool(true));
    if response.get("decision") != Some(&Value::String(selected.decision.clone()))
        || response.get("scope") != Some(&Value::String(selected.scope.clone()))
        || selected_remember != claimed_remember
    {
        return Err(operation_error(
            "PERMISSION_OPTION_MISMATCH",
            "decision payload does not match the selected option",
        ));
    }
    Ok(())
}

/// 旧 `decideRequest` 的 remember 分支（L374-402）：范围复核 + 授权落库 + 事件。
///
/// 返回 `Err(Ok(_))` 表示业务拒绝（回滚事务），`Err(Err(_))` 表示存储故障。
fn persist_remembered_grant(
    conn: &Connection,
    now: &str,
    now_millis: i64,
    id: &str,
    record: &InteractionRecord,
    authorization: &AuthorizationInteractionContext,
    response: &Map<String, Value>,
) -> Result<(), Result<AuthzError, DbError>> {
    let scope_text = string_value(response.get("scope"));
    let Some(scope) = PermissionScope::parse(&scope_text.to_ascii_uppercase()) else {
        return Err(Ok(operation_error(
            "PERMISSION_SCOPE_NOT_ALLOWED",
            format!("unsupported scope: {scope_text}"),
        )));
    };
    let selected_option_id = string_value(response.get("optionId"));
    let Some(selected) = authorization
        .options
        .iter()
        .find(|option| option.option_id == selected_option_id)
    else {
        return Err(Ok(operation_error(
            "PERMISSION_OPTION_NOT_ALLOWED",
            "selected option was not offered",
        )));
    };
    if !scope.as_str().eq_ignore_ascii_case(&selected.scope) {
        return Err(Ok(operation_error(
            "PERMISSION_SCOPE_NOT_ALLOWED",
            "scope does not match the selected option",
        )));
    }
    let grant_id = zk_authz::grants::create_in_tx(
        conn,
        now,
        now_millis,
        &authorization.subject.to_subject(),
        &authorization.operation,
        Some(scope),
        Some(id),
    )
    .map_err(Err)?;
    if let Some(grant_id) = grant_id {
        runs::append_event_in_current_write(
            conn,
            &record.run_id,
            "permission_grant_created",
            None,
            &json!({
                "interactionId": id,
                "grantId": grant_id,
                "operationHash": authorization.operation_hash,
            }),
        )
        .map_err(Err)?;
    }
    Ok(())
}

// ══════════════════ 授权链端口实现 ══════════════════

impl zk_authz::interaction::InteractionGateway for DurableInteractionService {
    fn find_by_correlation_key<'a>(
        &'a self,
        run_id: Option<&'a str>,
        correlation_key: &'a str,
    ) -> zk_authz::interaction::BoxFuture<'a, AuthzResult<Option<InteractionRecord>>> {
        Box::pin(async move {
            // 旧源 `findByCorrelationKey(runId, key)` 的 `run_id=?` 绑定 null 时
            // SQLite 恒不匹配，等价于「无 run 即无可复用交互」。
            let Some(run_id) = run_id else {
                return Ok(None);
            };
            self.find_by_correlation_key(run_id, correlation_key)
                .await
                .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))
        })
    }

    fn create_authorization(
        &self,
        spec: AuthorizationInteractionSpec,
    ) -> zk_authz::interaction::BoxFuture<'_, AuthzResult<InteractionRecord>> {
        Box::pin(async move { self.create_authorization(spec).await })
    }

    fn await_terminal<'a>(
        &'a self,
        interaction_id: &'a str,
    ) -> zk_authz::interaction::BoxFuture<'a, AuthzResult<InteractionStatus>> {
        Box::pin(async move { self.await_terminal(interaction_id).await })
    }

    fn find_by_id<'a>(
        &'a self,
        interaction_id: &'a str,
    ) -> zk_authz::interaction::BoxFuture<'a, AuthzResult<Option<InteractionRecord>>> {
        Box::pin(async move {
            self.find_by_id(interaction_id)
                .await
                .map_err(|error| operation_error("INTERACTION_STORE_FAILED", error))
        })
    }

    fn require_answered_once_in_tx(
        &self,
        conn: &Connection,
        interaction_id: Option<&str>,
        subject: &zk_authz::model::AuthorizationSubject,
        descriptor: &zk_authz::model::OperationDescriptor,
        tool_use_id: &str,
    ) -> AuthzResult<()> {
        Self::require_answered_once(conn, interaction_id, subject, descriptor, tool_use_id)
    }
}
