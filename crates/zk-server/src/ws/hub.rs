//! `WsHub`——连接注册表、会话订阅路由、bindingEpoch 与 critical 暂存重放。
//!
//! 对照旧 `WebSocketSessionManager`（principal/transport 双映射 + TTL 清理）+
//! `WebSocketController` 的三个 push 方法（`push` / `pushToUser` /
//! `pushToPrincipal`）：路由键从「principal 用户名」改为「应用 sessionId →
//! 订阅连接集合」，下行信封由 hub 统一组装（`ServerEnvelope` 扁平格式 + 路由
//! 字段 `_sessionId` / `_bindingEpoch`）。
//!
//! # 并发结构（D-S8-1：`RwLock<HashMap>`，不引入 dashmap）
//!
//! push 热路径只读锁（会话→订阅者、连接表）；bind/disconnect 写锁。Phase 1
//! 单用户 localhost、低连接密度，锁竞争可忽略——少一个依赖、少一类迭代器
//! 语义坑。锁纪律：**锁内绝不 await**（持锁期间只做内存操作，快照后释放，
//! 再进入带超时的 async send），杜绝跨 await 持锁。
//!
//! # 背压与 pending 状态机（D9 / D-S8-4）
//!
//! ```text
//! 订阅者 Active ──critical send 超时──► Degraded ──(消息入 pending)
//!      ▲                                                    │
//!      └──────────── bind_session（degraded 重置）◄──────────┘
//! ```
//!
//! degraded 连接的后续 critical 直接入 pending（不再尝试 send），delta 丢弃；
//! pending 为 per-session FIFO **无上界**（对齐旧 `WebSocketSessionManager` 侧
//! Java `List` 的无限追加语义：critical 永不因积压被丢弃；深度经
//! `zk_ws_pending_depth` gauge 监控），bind 成功后按序重放（多订阅者场景重放可能
//! 对已收到的连接重复投递，Phase 1 单用户单连接不构成问题，幂等由前端
//! interaction ACK 层吸收——Phase 2 多端接入时引入按连接去重）。

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use tokio::sync::mpsc;
use tokio::time::timeout;
use zk_protocol::{ServerEnvelope, ServerMessage};

use super::config::WsConfig;
use super::metrics as ws_metrics;
use super::{EngineHook, NoopEngineHook, is_critical_message};
use crate::iso::now_millis;

/// 连接标识（UUID；替代旧 Spring principal 用户名体系）。
pub type ConnId = String;
/// 会话标识（应用层 sessionId）。
pub type SessionId = String;

/// 写循环出站帧（协议层心跳 Ping 由写循环定时器直接发，不经此通道）。
pub(crate) enum OutboundFrame {
    /// 已序列化的下行 JSON 文本帧。
    Text(String),
    /// 协议层 Pong 应答（读循环收到客户端 Ping 帧时显式回送；见 connection）。
    Pong,
}

/// 连接绑定态（会话 + 该连接的 bindingEpoch）。
struct BoundSession {
    session_id: SessionId,
    epoch: u64,
}

/// 连接可变状态（锁内短暂访问，快照后立即释放）。
struct ConnState {
    bound: Option<BoundSession>,
    degraded: bool,
    last_seen: Instant,
}

/// 注册表条目。
struct ConnectionEntry {
    /// 写循环入口（背压档位作用点）。
    tx: mpsc::Sender<OutboundFrame>,
    /// 可变状态。
    state: Mutex<ConnState>,
}

/// bind 失败（错误码字符串对齐旧 `IllegalStateException` 消息）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    /// bindingEpoch 未严格大于该连接当前 epoch（旧 `STALE_BINDING_EPOCH`）。
    #[error("STALE_BINDING_EPOCH")]
    StaleEpoch,
    /// 连接不在注册表（升级握手与注册之间的竞态，理论不可达）。
    #[error("TRANSPORT_NOT_REGISTERED")]
    NotRegistered,
}

/// hub 内部统计（与 metrics 打点同源；测试与日志直读，u64 单调）。
#[derive(Default)]
struct HubStats {
    delta_dropped: AtomicU64,
    critical_timeouts: AtomicU64,
    pending_replayed: AtomicU64,
}

/// hub 可变状态全集。
struct HubInner {
    config: WsConfig,
    /// 连接注册表（`conn_id` → entry）。
    connections: RwLock<HashMap<ConnId, Arc<ConnectionEntry>>>,
    /// 会话 → 订阅连接集合。
    session_subscribers: RwLock<HashMap<SessionId, HashSet<ConnId>>>,
    /// 会话 → 已见最大 bindingEpoch（服务端权威记录，兼容客户端提供的递增值）。
    session_epochs: Mutex<HashMap<SessionId, u64>>,
    /// 会话 → 下行 seq 计数器（U1 顶层递增序列号）。
    seq_counters: Mutex<HashMap<SessionId, u64>>,
    /// 会话 → critical 暂存队列（FIFO，bind 重放）。
    pending: Mutex<HashMap<SessionId, VecDeque<ServerEnvelope>>>,
    /// 全会话 pending 总深度（gauge 值）。
    pending_depth: AtomicU64,
    /// 会话 → 最后一个订阅者离线时刻（信息性 offline 标记）。
    offline_since: Mutex<HashMap<SessionId, Instant>>,
    /// S9 引擎挂点。
    engine: RwLock<Arc<dyn EngineHook>>,
    stats: HubStats,
}

/// WS 通道中枢——连接生命周期与会话路由的共享句柄（克隆即共享）。
#[derive(Clone)]
pub struct WsHub {
    inner: Arc<HubInner>,
}

impl WsHub {
    /// 以给定配置装配 hub（engine 默认 [`NoopEngineHook`]）。
    #[must_use]
    pub fn new(config: WsConfig) -> Self {
        Self {
            inner: Arc::new(HubInner {
                config,
                connections: RwLock::new(HashMap::new()),
                session_subscribers: RwLock::new(HashMap::new()),
                session_epochs: Mutex::new(HashMap::new()),
                seq_counters: Mutex::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
                pending_depth: AtomicU64::new(0),
                offline_since: Mutex::new(HashMap::new()),
                engine: RwLock::new(Arc::new(NoopEngineHook)),
                stats: HubStats::default(),
            }),
        }
    }

    /// 运行配置（只读）。
    #[must_use]
    pub fn config(&self) -> &WsConfig {
        &self.inner.config
    }

    /// 注入 S9 引擎挂点（Phase 1 为 Noop）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    pub fn set_engine(&self, engine: Arc<dyn EngineHook>) {
        *self.inner.engine.write().expect("engine lock") = engine;
    }

    // ── 连接注册 ──────────────────────────────────────────────────────────

    /// 注册新连接（升级握手完成后由 connection 层调用；crate 内接口）。
    pub(crate) fn register(&self, conn_id: &str, tx: mpsc::Sender<OutboundFrame>) {
        self.inner
            .connections
            .write()
            .expect("connections lock")
            .insert(
                conn_id.to_owned(),
                Arc::new(ConnectionEntry {
                    tx,
                    state: Mutex::new(ConnState {
                        bound: None,
                        degraded: false,
                        last_seen: Instant::now(),
                    }),
                }),
            );
        let total = self.connection_count();
        ws_metrics::set_connections(total);
        tracing::info!(conn_id, connections = total, "ws connected");
    }

    /// 刷新连接活跃时刻（任何入帧调用；对齐旧 `refreshTransport`）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    pub fn touch(&self, conn_id: &str) {
        let Some(entry) = self
            .inner
            .connections
            .read()
            .expect("connections lock")
            .get(conn_id)
            .cloned()
        else {
            return;
        };
        entry.state.lock().expect("conn state lock").last_seen = Instant::now();
    }

    /// 连接关闭清理：摘除注册与订阅；若会话再无订阅者则打 offline 标记。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障， fail fast）。
    pub fn disconnect(&self, conn_id: &str) {
        let removed = self
            .inner
            .connections
            .write()
            .expect("connections lock")
            .remove(conn_id);
        let Some(entry) = removed else {
            return;
        };
        let bound = entry.state.lock().expect("conn state lock").bound.take();
        if let Some(bound) = bound
            && self.remove_subscriber(&bound.session_id, conn_id)
        {
            // 返回 true = 该会话已无任何订阅者：打 offline 标记（信息性，
            // 对齐旧 disconnectTransport；pending 不受影响）。
            self.inner
                .offline_since
                .lock()
                .expect("offline lock")
                .insert(bound.session_id.clone(), Instant::now());
        }
        let total = self.connection_count();
        ws_metrics::set_connections(total);
        tracing::info!(conn_id, connections = total, "ws disconnected");
    }

    // ── 会话绑定 ──────────────────────────────────────────────────────────

    /// 绑定连接到会话（旧 `bindSession` 语义）。
    ///
    /// - `client_epoch` 必须 ≥1（调用方协议层校验）且**严格大于**该连接当前
    ///   epoch，否则 [`BindError::StaleEpoch`]（旧 `STALE_BINDING_EPOCH`）；
    /// - 换绑 = 携带更大 epoch 再次 bind 到新会话（旧连接旧会话订阅自动摘除）；
    /// - bind 重置 degraded（背压降级连接经重放路径恢复）；
    /// - 返回生效 epoch（即 `client_epoch`；服务端记录会话级 max）。
    ///
    /// # Errors
    ///
    /// [`BindError::StaleEpoch`]：epoch 非递增；[`BindError::NotRegistered`]：
    /// 连接未注册。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    pub fn bind(
        &self,
        conn_id: &str,
        session_id: &str,
        client_epoch: u64,
    ) -> Result<u64, BindError> {
        let Some(entry) = self
            .inner
            .connections
            .read()
            .expect("connections lock")
            .get(conn_id)
            .cloned()
        else {
            return Err(BindError::NotRegistered);
        };
        let previous_session = {
            let mut state = entry.state.lock().expect("conn state lock");
            if let Some(bound) = &state.bound
                && client_epoch <= bound.epoch
            {
                return Err(BindError::StaleEpoch);
            }
            // 先留旧值再覆盖（覆盖后读出的是新会话，旧订阅将永不摘除）。
            let previous = state
                .bound
                .as_ref()
                .map(|previous| previous.session_id.clone());
            state.bound = Some(BoundSession {
                session_id: session_id.to_owned(),
                epoch: client_epoch,
            });
            state.degraded = false;
            previous
        };
        // 换绑：摘除旧会话订阅（previous_session 与新 session 不同名时）。
        if let Some(previous) = previous_session
            && previous != session_id
        {
            self.remove_subscriber(&previous, conn_id);
        }
        self.inner
            .session_subscribers
            .write()
            .expect("subscribers lock")
            .entry(session_id.to_owned())
            .or_default()
            .insert(conn_id.to_owned());
        self.inner
            .session_epochs
            .lock()
            .expect("epochs lock")
            .entry(session_id.to_owned())
            .and_modify(|max| *max = (*max).max(client_epoch))
            .or_insert(client_epoch);
        self.inner
            .offline_since
            .lock()
            .expect("offline lock")
            .remove(session_id);
        tracing::info!(
            conn_id,
            session_id,
            epoch = client_epoch,
            "ws session bound"
        );
        Ok(client_epoch)
    }

    /// 摘除连接的会话绑定（bind 后置校验失败的回滚路径；旧 `unbindSession`）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    pub fn unbind(&self, conn_id: &str) {
        let Some(entry) = self
            .inner
            .connections
            .read()
            .expect("connections lock")
            .get(conn_id)
            .cloned()
        else {
            return;
        };
        let bound = entry.state.lock().expect("conn state lock").bound.take();
        if let Some(bound) = bound {
            self.remove_subscriber(&bound.session_id, conn_id);
            tracing::info!(conn_id, session_id = %bound.session_id, "ws session unbound");
        }
    }

    /// 连接当前绑定的（会话，epoch）快照（未绑定返回 `None`）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    #[must_use]
    pub fn bound_session(&self, conn_id: &str) -> Option<(SessionId, u64)> {
        let entry = self
            .inner
            .connections
            .read()
            .expect("connections lock")
            .get(conn_id)
            .cloned()?;
        let state = entry.state.lock().expect("conn state lock");
        state
            .bound
            .as_ref()
            .map(|bound| (bound.session_id.clone(), bound.epoch))
    }

    /// 会话是否仍有在线订阅者（旧 `isSessionOnline`）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    #[must_use]
    pub fn is_session_online(&self, session_id: &str) -> bool {
        self.inner
            .session_subscribers
            .read()
            .expect("subscribers lock")
            .get(session_id)
            .is_some_and(|ids| ids.iter().any(|id| self.connection(id).is_some()))
    }

    /// 全部仍有在线订阅者的会话（旧 `getActiveSessionIds`
    /// `WebSocketSessionManager.java:255-258`：`sessionToTransports` 中至少有
    /// 一个 transport 仍在 `transports` 注册表内的键集）。
    ///
    /// 旧源回 无序 `Set`；此处按 `session_id` 字典序返回，保证
    /// `GET /api/remote/status` 的列表顺序可复现（同 D-01 的取舍）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    #[must_use]
    pub fn active_sessions(&self) -> Vec<SessionId> {
        let subscribers = self
            .inner
            .session_subscribers
            .read()
            .expect("subscribers lock");
        let mut active: Vec<SessionId> = subscribers
            .iter()
            .filter(|(_, conn_ids)| conn_ids.iter().any(|id| self.connection(id).is_some()))
            .map(|(session_id, _)| session_id.clone())
            .collect();
        active.sort_unstable();
        active
    }

    /// 会话已绑定的首个 transport（旧 `wsSessionManager
    /// .getTransportIdsForSession(sessionId).stream().findFirst()`，
    /// `WebSocketController.java:288-289` 的交互投递闸门数据源）。
    ///
    /// 旧源在无序 `Set` 上 `findFirst()`；此处按 `conn_id` 字典序取最小，
    /// 保证同一会话多连接时投递目标可复现（见 §8 偏离表 D-01）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    #[must_use]
    pub fn first_transport_for_session(&self, session_id: &str) -> Option<ConnId> {
        let subscribers = self
            .inner
            .session_subscribers
            .read()
            .expect("subscribers lock");
        let mut live: Vec<&ConnId> = subscribers
            .get(session_id)?
            .iter()
            .filter(|id| self.connection(id).is_some())
            .collect();
        live.sort_unstable();
        live.first().map(|id| (*id).clone())
    }

    // ── 下行推送（S9 引擎侧唯一入口） ────────────────────────────────────

    /// 推送消息到会话订阅者（旧 `push` / `pushToUser` 语义）。
    ///
    /// 信封组装：`type` / payload 平铺 + `ts` + `seq`（会话级递增）+
    /// `_sessionId` + `_bindingEpoch`（按订阅者连接逐份附加）。
    ///
    /// 分发规则（D9 双档）：
    /// - critical：`send` 带 `critical_send_timeout`（200ms）超时；超时则该
    ///   订阅者转 degraded 且消息入 per-session pending；
    /// - delta：`try_send`，满则丢弃 + 计数（不阻塞引擎）；
    /// - 无订阅者：critical 入 pending（等 bind 重放），delta 丢弃 + 计数。
    ///
    /// 本方法对调用方承诺**永不阻塞、永不失败**（背压由档位策略消化）。
    pub async fn push(&self, session_id: &str, msg: ServerMessage) {
        let critical = is_critical_message(msg.kind());
        let seq = self.next_seq(session_id);
        let mut envelope = ServerEnvelope::new(msg, now_millis(), Some(seq));
        envelope.session_id = Some(session_id.to_owned());
        self.deliver(session_id, envelope, critical).await;
    }

    /// 直发单连接（旧 `pushToPrincipal` 语义：不带 `_sessionId` /
    /// `_bindingEpoch` 路由字段——`session_restored` / 未绑定 `pong` /
    /// `protocol_error` 等握手帧的出口）。
    ///
    /// `try_send` 满即丢弃 + debug 日志（握手响应是连接自身请求的答复，
    /// 连接濒死时无需暂存）。
    pub fn push_direct(&self, conn_id: &str, msg: ServerMessage) {
        let envelope = ServerEnvelope::new(msg, now_millis(), None);
        let Some(entry) = self.connection(conn_id) else {
            return;
        };
        match serde_json::to_string(&envelope) {
            Ok(text) => {
                if entry.tx.try_send(OutboundFrame::Text(text)).is_err() {
                    tracing::debug!(conn_id, "direct push dropped (backpressure)");
                } else {
                    ws_metrics::count_message("out", envelope.kind());
                }
            }
            Err(err) => tracing::error!(conn_id, error = %err, "envelope serialization failed"),
        }
    }

    /// 重放会话全部 pending critical（bind 成功后调用，FIFO 序）。
    ///
    /// 返回重放条数（0 = 无暂存）。重放走与 [`WsHub::push`] 相同的双档
    /// 分发；若重放中再次超时，消息重新入队尾（下次 bind 再试）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    pub async fn replay_pending(&self, session_id: &str) -> usize {
        let drained: Vec<ServerEnvelope> = {
            let mut pending = self.inner.pending.lock().expect("pending lock");
            pending
                .remove(session_id)
                .map_or_else(Vec::new, |queue| queue.into_iter().collect())
        };
        if drained.is_empty() {
            return 0;
        }
        let count = drained.len();
        ws_metrics::set_pending_depth(self.pending_depth_total());
        self.inner
            .stats
            .pending_replayed
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
        ws_metrics::count_pending_replayed(u64::try_from(count).unwrap_or(u64::MAX));
        tracing::info!(session_id, count, "ws pending replay started");
        for envelope in drained {
            let critical = is_critical_message(envelope.kind());
            self.deliver(session_id, envelope, critical).await;
        }
        count
    }

    /// 会话 pending 深度（测试/诊断直读）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    #[must_use]
    pub fn pending_len(&self, session_id: &str) -> usize {
        self.inner
            .pending
            .lock()
            .expect("pending lock")
            .get(session_id)
            .map_or(0, VecDeque::len)
    }

    /// delta 档累计丢弃数（测试/诊断直读）。
    #[must_use]
    pub fn delta_dropped_total(&self) -> u64 {
        self.inner.stats.delta_dropped.load(Ordering::Relaxed)
    }

    /// critical 档累计投递超时数（测试/诊断直读）。
    #[must_use]
    pub fn critical_timeouts_total(&self) -> u64 {
        self.inner.stats.critical_timeouts.load(Ordering::Relaxed)
    }

    /// 当前连接总数。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    #[must_use]
    pub fn connection_count(&self) -> u64 {
        u64::try_from(
            self.inner
                .connections
                .read()
                .expect("connections lock")
                .len(),
        )
        .unwrap_or(u64::MAX)
    }

    /// 会话级最大 bindingEpoch（未见过绑定返回 0）。
    ///
    /// # Panics
    ///
    /// 内部锁中毒时 panic（进程级故障，fail fast）。
    #[must_use]
    pub fn session_epoch(&self, session_id: &str) -> u64 {
        self.inner
            .session_epochs
            .lock()
            .expect("epochs lock")
            .get(session_id)
            .copied()
            .unwrap_or(0)
    }

    /// S9 挂点调用入口（inbound 层转发非通道类上行）。
    pub(crate) fn dispatch_to_engine(&self, session_id: &str, message: zk_protocol::ClientMessage) {
        let engine = Arc::clone(&self.inner.engine.read().expect("engine lock"));
        engine.on_client_message(session_id, message);
    }

    // ── cleanup（旧 cleanupStaleEntries） ─────────────────────────────────

    /// 启动周期清理任务（main 装配时调用一次；句柄由调用方持有控制生命周期）。
    #[must_use]
    pub fn spawn_cleanup(self) -> tokio::task::JoinHandle<()> {
        let period = self.inner.config.cleanup_period;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // interval 首拍立即完成，吃掉。
            loop {
                ticker.tick().await;
                self.cleanup_once();
            }
        })
    }

    /// 单轮清理：TTL 过期连接摘除 + offline 标记 grace 回收。
    fn cleanup_once(&self) {
        let now = Instant::now();
        let stale_ttl = self.inner.config.stale_ttl;
        let stale: Vec<ConnId> = {
            let connections = self.inner.connections.read().expect("connections lock");
            connections
                .iter()
                .filter(|(_, entry)| {
                    now.duration_since(entry.state.lock().expect("conn state lock").last_seen)
                        > stale_ttl
                })
                .map(|(id, _)| id.clone())
                .collect()
        };
        for conn_id in &stale {
            self.disconnect(conn_id);
        }
        let grace = self.inner.config.offline_grace;
        self.inner
            .offline_since
            .lock()
            .expect("offline lock")
            .retain(|session_id, since| {
                now.duration_since(*since) <= grace || self.is_session_online(session_id)
            });
        if !stale.is_empty() {
            tracing::info!(
                cleaned = stale.len(),
                connections = self.connection_count(),
                "ws cleanup removed stale entries"
            );
        }
    }

    // ── 内部 ──────────────────────────────────────────────────────────────

    /// 连接条目快照（Arc 克隆，锁立即释放）。
    fn connection(&self, conn_id: &str) -> Option<Arc<ConnectionEntry>> {
        self.inner
            .connections
            .read()
            .expect("connections lock")
            .get(conn_id)
            .cloned()
    }

    /// 会话 seq 递增分配（U1；bind 不重置——重放窗口与正常窗口连续编号）。
    fn next_seq(&self, session_id: &str) -> u64 {
        let mut counters = self.inner.seq_counters.lock().expect("seq lock");
        let next = counters.entry(session_id.to_owned()).or_insert(0);
        *next += 1;
        *next
    }

    /// 核心分发段：订阅者扫描 + 双档投递 + pending 回退。
    async fn deliver(&self, session_id: &str, mut envelope: ServerEnvelope, critical: bool) {
        let subscribers: Vec<Arc<ConnectionEntry>> = {
            let mapping = self
                .inner
                .session_subscribers
                .read()
                .expect("subscribers lock");
            match mapping.get(session_id) {
                Some(ids) if !ids.is_empty() => {
                    let connections = self.inner.connections.read().expect("connections lock");
                    ids.iter()
                        .filter_map(|id| connections.get(id).cloned())
                        .collect()
                }
                _ => Vec::new(),
            }
        };
        if subscribers.is_empty() {
            if critical {
                self.enqueue_pending(session_id, envelope);
            } else {
                self.note_delta_dropped(session_id, "no subscriber");
            }
            return;
        }
        let mut pending_needed = false;
        let mut delivered = false;
        for entry in &subscribers {
            // 锁内只取快照，绝不跨 await 持锁。
            let snapshot = {
                let state = entry.state.lock().expect("conn state lock");
                state
                    .bound
                    .as_ref()
                    .map(|bound| (bound.epoch, state.degraded))
            };
            // 订阅表与绑定态竞态残留（`unbind` 先 take 绑定、后摘订阅，二者之间
            // 存在窗口）：critical 必须回落 pending，否则该帧静默丢失且永不重放。
            let Some((epoch, degraded)) = snapshot else {
                if critical {
                    pending_needed = true;
                } else {
                    self.note_delta_dropped(session_id, "unbound subscriber");
                }
                continue;
            };
            if degraded {
                if critical {
                    pending_needed = true;
                } else {
                    self.note_delta_dropped(session_id, "degraded subscriber");
                }
                continue;
            }
            envelope.binding_epoch = Some(epoch);
            let payload = match serde_json::to_string(&envelope) {
                Ok(text) => OutboundFrame::Text(text),
                Err(err) => {
                    tracing::error!(session_id, error = %err, "envelope serialization failed");
                    continue;
                }
            };
            if critical {
                let sent = timeout(
                    self.inner.config.critical_send_timeout,
                    entry.tx.send(payload),
                )
                .await;
                match sent {
                    Ok(Ok(())) => delivered = true,
                    Ok(Err(_receiver_gone)) => {
                        pending_needed = true;
                        Self::degrade(entry);
                    }
                    Err(_elapsed) => {
                        self.inner
                            .stats
                            .critical_timeouts
                            .fetch_add(1, Ordering::Relaxed);
                        ws_metrics::count_critical_timeout();
                        pending_needed = true;
                        Self::degrade(entry);
                        tracing::warn!(session_id, "critical send timeout, subscriber degraded");
                    }
                }
            } else if entry.tx.try_send(payload).is_err() {
                self.note_delta_dropped(session_id, "outbound full");
            } else {
                delivered = true;
            }
        }
        // 与 `push_direct` 一致：`out` 只统计真正写入出站通道的帧，避免
        // “已发送”指标掩盖 degraded / 未绑定 / 序列化失败的投递缺口。
        if delivered {
            ws_metrics::count_message("out", envelope.kind());
        }
        if pending_needed {
            self.enqueue_pending(session_id, envelope);
        }
    }

    /// 订阅者转 degraded（critical 超时 / 接收端消失）。
    fn degrade(entry: &Arc<ConnectionEntry>) {
        entry.state.lock().expect("conn state lock").degraded = true;
    }

    /// delta 丢弃计数 + debug 日志（不落消息体）。
    fn note_delta_dropped(&self, session_id: &str, reason: &str) {
        self.inner
            .stats
            .delta_dropped
            .fetch_add(1, Ordering::Relaxed);
        ws_metrics::count_delta_dropped();
        tracing::debug!(session_id, reason, "delta message dropped");
    }

    /// critical 入 pending 队（FIFO **无界**追加：对齐旧 Java `List` 语义，
    /// 不截断不淘汰；深度只上报 gauge 供监控）。
    fn enqueue_pending(&self, session_id: &str, envelope: ServerEnvelope) {
        let mut pending = self.inner.pending.lock().expect("pending lock");
        pending
            .entry(session_id.to_owned())
            .or_default()
            .push_back(envelope);
        // 持锁期间直接求和：不可经 `pending_depth_total` 重入取同一把锁
        //（std `Mutex` 不可重入，重入即死锁——首次集成跑测实测命中）。
        let depth =
            u64::try_from(pending.values().map(VecDeque::len).sum::<usize>()).unwrap_or(u64::MAX);
        self.inner.pending_depth.store(depth, Ordering::Relaxed);
        ws_metrics::set_pending_depth(depth);
        drop(pending);
        tracing::debug!(session_id, "critical message pended");
    }

    /// 全会话 pending 总深度。
    fn pending_depth_total(&self) -> u64 {
        let pending = self.inner.pending.lock().expect("pending lock");
        u64::try_from(pending.values().map(VecDeque::len).sum::<usize>()).unwrap_or(u64::MAX)
    }

    /// 摘除订阅者；返回 `true` 表示该会话已无任何存活订阅者。
    fn remove_subscriber(&self, session_id: &str, conn_id: &str) -> bool {
        let mut mapping = self
            .inner
            .session_subscribers
            .write()
            .expect("subscribers lock");
        let empty = mapping.get_mut(session_id).is_some_and(|ids| {
            ids.retain(|id| id != conn_id);
            ids.is_empty()
        });
        if empty {
            mapping.remove(session_id);
        }
        empty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use zk_protocol::ServerMessage;

    /// 测试用小配置（critical 超时 1ms 让超时路径必触发；容量 2）。
    fn test_config() -> WsConfig {
        WsConfig {
            critical_send_timeout: Duration::from_millis(1),
            outbound_capacity: 2,
            stale_ttl: Duration::from_millis(80),
            offline_grace: Duration::from_millis(30),
            ..WsConfig::fast_for_tests()
        }
    }

    /// error 消息（critical 档代表）。
    fn critical_msg(code: &str) -> ServerMessage {
        ServerMessage::Error {
            code: code.to_owned(),
            message: "test".to_owned(),
            retryable: false,
        }
    }

    /// `stream_delta` 消息（delta 档代表）。
    fn delta_msg(text: &str) -> ServerMessage {
        ServerMessage::StreamDelta {
            delta: text.to_owned(),
        }
    }

    /// `unbind` 先摘绑定、后摘订阅，两步之间存在窗口：该窗口内投递的 critical
    /// 帧必须回落 pending，否则帧静默丢失且永不重放（交互投递会被直接判定为
    /// UNDELIVERABLE，用户永远看不到权限弹窗）。
    #[tokio::test]
    async fn critical_pends_when_subscriber_binding_snapshot_is_missing() {
        let (hub, mut rx) = setup(test_config());
        hub.bind("conn-1", "s-1", 1).expect("bind");
        // 复现竞态：订阅表仍持有该连接，连接自身的绑定态已被摘除。
        hub.connection("conn-1")
            .expect("registered connection")
            .state
            .lock()
            .expect("conn state lock")
            .bound = None;

        hub.push("s-1", critical_msg("E_UNBOUND_RACE")).await;

        assert!(
            rx.try_recv().is_err(),
            "unbound subscriber must not receive the frame"
        );
        assert_eq!(
            hub.pending_depth_total(),
            1,
            "critical frame must fall back to pending for replay"
        );
    }

    /// 注册一条假连接（容量 2，背压快速触发），返回 (hub, receiver)。
    fn setup(config: WsConfig) -> (WsHub, mpsc::Receiver<OutboundFrame>) {
        setup_with_capacity(config, 2)
    }

    /// 注册指定出向容量的假连接（重放类测试需容量 ≥ pending 深度）。
    fn setup_with_capacity(
        config: WsConfig,
        capacity: usize,
    ) -> (WsHub, mpsc::Receiver<OutboundFrame>) {
        let hub = WsHub::new(config);
        let (tx, rx) = mpsc::channel(capacity);
        hub.register("conn-1", tx);
        (hub, rx)
    }

    fn bind_ok(hub: &WsHub, epoch: u64) {
        hub.bind("conn-1", "session-a", epoch).expect("bind ok");
    }

    #[tokio::test]
    async fn bind_routes_and_epoch_attaches() {
        let (hub, mut rx) = setup(test_config());
        bind_ok(&hub, 7);
        assert_eq!(hub.session_epoch("session-a"), 7);
        assert!(hub.is_session_online("session-a"));
        hub.push("session-a", delta_msg("hello")).await;
        let frame = rx.recv().await.expect("frame delivered");
        let OutboundFrame::Text(text) = frame else {
            panic!("unexpected control frame in test");
        };
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        // 扁平信封：type/ts/seq/_sessionId/_bindingEpoch 全部顶层。
        assert_eq!(value["type"], "stream_delta");
        assert_eq!(value["delta"], "hello");
        assert_eq!(value["_sessionId"], "session-a");
        assert_eq!(value["_bindingEpoch"], 7);
        assert_eq!(value["seq"], 1);
        assert!(value["ts"].as_i64().is_some());
    }

    #[tokio::test]
    async fn stale_epoch_rejected_and_rebind_increments() {
        let (hub, _rx) = setup(test_config());
        bind_ok(&hub, 5);
        // 非递增 epoch 拒绝（旧 STALE_BINDING_EPOCH）。
        assert_eq!(
            hub.bind("conn-1", "session-a", 5),
            Err(BindError::StaleEpoch)
        );
        assert_eq!(
            hub.bind("conn-1", "session-a", 1),
            Err(BindError::StaleEpoch)
        );
        // 递增换绑成功，会话 max epoch 记录。
        hub.bind("conn-1", "session-b", 9).expect("rebind ok");
        assert_eq!(hub.bound_session("conn-1"), Some(("session-b".into(), 9)));
        assert_eq!(hub.session_epoch("session-b"), 9);
        // 旧会话订阅已摘除。
        assert!(!hub.is_session_online("session-a"));
        assert!(hub.is_session_online("session-b"));
        // 未注册连接拒绝。
        assert_eq!(
            hub.bind("ghost", "session-a", 3),
            Err(BindError::NotRegistered)
        );
    }

    #[tokio::test]
    async fn delta_without_subscriber_dropped_critical_pended() {
        let (hub, _rx) = setup(test_config());
        hub.push("nobody", delta_msg("x")).await;
        assert_eq!(hub.delta_dropped_total(), 1);
        hub.push("nobody", critical_msg("E1")).await;
        assert_eq!(hub.pending_len("nobody"), 1);
    }

    #[tokio::test]
    async fn delta_backpressure_drops_without_blocking() {
        let (hub, _rx) = setup(test_config());
        bind_ok(&hub, 1);
        // 容量 2 且无人消费：后续 delta 全部丢弃，push 即刻返回。
        for i in 0..50 {
            let started = Instant::now();
            hub.push("session-a", delta_msg(&format!("d{i}"))).await;
            assert!(
                started.elapsed() < Duration::from_millis(500),
                "delta push blocked"
            );
        }
        assert!(
            hub.delta_dropped_total() >= 48,
            "drops counted: {}",
            hub.delta_dropped_total()
        );
        assert_eq!(hub.pending_len("session-a"), 0, "delta never pends");
    }

    #[tokio::test]
    async fn critical_timeout_degrades_and_pends_then_replays_in_order() {
        let (hub, mut rx) = setup_with_capacity(test_config(), 4);
        bind_ok(&hub, 2);
        // 占满容量（4 条 delta），critical send 1ms 超时 → degraded + pending。
        hub.push("session-a", delta_msg("fill-1")).await;
        hub.push("session-a", delta_msg("fill-2")).await;
        hub.push("session-a", delta_msg("fill-3")).await;
        hub.push("session-a", delta_msg("fill-4")).await;
        hub.push("session-a", critical_msg("C1")).await;
        assert!(hub.critical_timeouts_total() >= 1);
        assert_eq!(hub.pending_len("session-a"), 1);
        // degraded 后续 critical 直接 pending（不再计时等待）。
        let fast = Instant::now();
        hub.push("session-a", critical_msg("C2")).await;
        hub.push("session-a", critical_msg("C3")).await;
        assert!(
            fast.elapsed() < Duration::from_millis(50),
            "degraded critical must not wait for timeout"
        );
        assert_eq!(hub.pending_len("session-a"), 3);
        // 排干消费端，模拟重连：bind（degraded 重置）→ 按序重放。
        for _ in 0..4 {
            let _ = rx.recv().await;
        }
        hub.bind("conn-1", "session-a", 3).expect("rebind");
        let replayed = hub.replay_pending("session-a").await;
        assert_eq!(replayed, 3);
        let mut codes = Vec::new();
        for _ in 0..3 {
            let frame = rx.recv().await.expect("replayed frame");
            let OutboundFrame::Text(text) = frame else {
                panic!("unexpected control frame in test");
            };
            let value: serde_json::Value = serde_json::from_str(&text).expect("json");
            assert_eq!(value["_bindingEpoch"], 3, "replay carries new epoch");
            codes.push(value["code"].as_str().expect("code").to_owned());
        }
        assert_eq!(codes, vec!["C1", "C2", "C3"], "FIFO order preserved");
        assert_eq!(hub.pending_len("session-a"), 0);
    }

    /// pending 无上界（对齐旧 Java `List` 无限追加）：远超历史 1024 上界
    /// 后仍全量保留，重放条数等于入队条数、队首仍为最旧一条。
    #[tokio::test]
    async fn pending_queue_is_unbounded() {
        let (hub, _rx) = setup(test_config());
        for i in 0..1500 {
            hub.push("offline", critical_msg(&format!("P{i}"))).await;
        }
        assert_eq!(hub.pending_len("offline"), 1500);
        let (tx, mut rx2) = mpsc::channel(2048);
        hub.register("conn-2", tx);
        hub.bind("conn-2", "offline", 1).expect("bind");
        assert_eq!(hub.replay_pending("offline").await, 1500);
        let OutboundFrame::Text(first) = rx2.recv().await.expect("replayed frame") else {
            panic!("unexpected control frame in test");
        };
        let value: serde_json::Value = serde_json::from_str(&first).expect("json");
        assert_eq!(value["code"], "P0", "最旧一条未被淘汰");
    }

    #[tokio::test]
    async fn disconnect_marks_offline_and_cleanup_reclaims() {
        let (hub, _rx) = setup(test_config());
        bind_ok(&hub, 1);
        assert!(hub.is_session_online("session-a"));
        hub.disconnect("conn-1");
        assert!(!hub.is_session_online("session-a"));
        assert_eq!(hub.connection_count(), 0);
        // TTL cleanup：再注册一条不活跃连接（不 touch），超过 stale_ttl 后被清理。
        let (tx2, _rx2) = mpsc::channel(2);
        hub.register("conn-2", tx2);
        tokio::time::sleep(Duration::from_millis(120)).await;
        hub.cleanup_once();
        assert_eq!(hub.connection_count(), 0, "stale entry reclaimed");
        // 活跃连接（持续 touch）不被误清。
        let (tx3, _rx3) = mpsc::channel(2);
        hub.register("conn-3", tx3);
        hub.touch("conn-3");
        hub.cleanup_once();
        assert_eq!(hub.connection_count(), 1);
    }

    #[tokio::test]
    async fn push_direct_has_no_routing_fields() {
        let (hub, mut rx) = setup(test_config());
        hub.push_direct("conn-1", critical_msg("D1"));
        let frame = rx.recv().await.expect("direct frame");
        let OutboundFrame::Text(text) = frame else {
            panic!("unexpected control frame in test");
        };
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert!(
            value.get("_sessionId").is_none(),
            "pushToPrincipal semantics"
        );
        assert!(value.get("_bindingEpoch").is_none());
        assert_eq!(value["type"], "error");
    }
}
