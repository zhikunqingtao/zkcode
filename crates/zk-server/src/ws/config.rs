//! ws 运行参数——心跳 / 判死 / TTL / cleanup / 背压档位（数值对齐旧系统实查）。
//!
//! | 参数 | 默认值 | 旧系统对照 |
//! |---|---|---|
//! | `heartbeat_interval` | 10s | 旧 STOMP `heartbeatIncoming/Outgoing: 10000`（方案 §4.2「心跳保留 10s 双向语义，改用 WS ping/pong 帧」） |
//! | `idle_timeout` | 30s（3×心跳） | 服务端 Ping 后连续 3 个周期无任何入帧即判死（zkcode 裁定；旧 STOMP 由 broker 心跳协商承担） |
//! | `stale_ttl` | 180s | 旧 `WebSocketSessionManager.STALE_ENTRY_TTL_MS = 180_000`（L50） |
//! | `cleanup_period` | 10s | 旧 `cleanupScheduler.scheduleAtFixedRate(…, 10, 10, SECONDS)`（L85-86） |
//! | `offline_grace` | 30s | 旧 `OFFLINE_GRACE_MS = 30_000`（L51，信息性标记——从不取消 Run，pending 不因 grace 删除） |
//! | `critical_send_timeout` | 200ms | D9「critical 类 send 带 200ms 超时」 |
//! | `outbound_capacity` | 256 | 每连接下行 mpsc 容量（256 帧 ≈ 单回合 delta 密集窗口） |
//! | `pending_capacity_per_session` | 1024 | 单会话内存重放缓存上限；权威重放来自 `SQLite` outbox |
//! | `pending_capacity_total` | 8192 | 全局内存重放缓存上限，防离线会话基数耗尽内存 |
//!
//! pending 只是已提交 outbox 的投递加速层，不是状态权威。达到上限会淘汰旧的
//! 内存副本；客户端按 `eventId` 从 `SQLite` 恢复，不会因缓存淘汰丢失持久事件。
//!
//! 全部字段可注入（测试缩短心跳/超时，避免慢测试）。

use std::time::Duration;

/// ws 通道运行配置（不可变，hub 构造期一次性装配）。
#[derive(Clone, Debug)]
pub struct WsConfig {
    /// 服务端协议层 Ping 帧间隔（写循环定时器）。
    pub heartbeat_interval: Duration,
    /// 读循环空闲判死窗口（任何入帧刷新；超时断开清理）。
    pub idle_timeout: Duration,
    /// 连接注册条目 TTL（cleanup 兜底清理，防读循环失联泄漏）。
    pub stale_ttl: Duration,
    /// cleanup 扫描周期。
    pub cleanup_period: Duration,
    /// 会话离线标记保留期（信息性；对齐旧 offline grace）。
    pub offline_grace: Duration,
    /// critical 下行投递超时（超时转 degraded + pending）。
    pub critical_send_timeout: Duration,
    /// 每连接下行 mpsc 容量。
    pub outbound_capacity: usize,
    /// 单会话 critical 内存重放缓存上限。
    pub pending_capacity_per_session: usize,
    /// 所有会话 critical 内存重放缓存总上限。
    pub pending_capacity_total: usize,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            stale_ttl: Duration::from_mins(3),
            cleanup_period: Duration::from_secs(10),
            offline_grace: Duration::from_secs(30),
            critical_send_timeout: Duration::from_millis(200),
            outbound_capacity: 256,
            pending_capacity_per_session: 1024,
            pending_capacity_total: 8192,
        }
    }
}

impl WsConfig {
    /// 集成测试参数（心跳 50ms / 判死 150ms / 小容量背压快速触发）。
    ///
    /// `#[doc(hidden)]`：仅供测试装配，生产代码禁止使用。
    #[doc(hidden)]
    #[must_use]
    pub fn fast_for_tests() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(50),
            idle_timeout: Duration::from_millis(400),
            stale_ttl: Duration::from_millis(150),
            cleanup_period: Duration::from_millis(50),
            offline_grace: Duration::from_millis(30),
            critical_send_timeout: Duration::from_millis(200),
            outbound_capacity: 4,
            pending_capacity_per_session: 64,
            pending_capacity_total: 256,
        }
    }
}
