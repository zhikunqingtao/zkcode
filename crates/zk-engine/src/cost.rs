//! 费用追踪窄端口——引擎与费率累加器解耦点。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `service/CostTrackerService.java`：`recordUsage(sessionId, model, usage)`
//!   实时累加 `sessionCost` / `globalCost`（`sessionCosts` `ConcurrentHashMap` +
//!   `globalCost` `AtomicReference`）；
//! - `websocket/WebSocketController.java` L1143-1150 `WsMessageHandler.onUsage`：
//!   在每轮 `handler.onUsage(usage)` 回调点读取会话/全局累计费用并推送
//!   `cost_update`（旧 record `CostUpdate(sessionCost, totalCost, usage)`）。
//!
//! 依赖方向铁律：`zk-engine` 不依赖 `zk-server`；本 trait 是窄端口，具体
//! `AtomicU64` 累加器由 zk-server 侧的 `AtomicCostTracker` 实现并在组装根
//! 注入。未装配时走 [`NoopCostTracker`]（`session_cost` / `global_cost` 恒 0，
//! `add_usage` 空转返回 0）——行为与本 Step 接入前一致，不影响既有可观察序列。
//!
//! # 费率与公式
//!
//! 单次 API 调用的美元成本由 [`crate::query_config::usage_cost_usd`] 计算，
//! 逐行对照旧 `CostTrackerService.recordUsage` L45-49：
//! `input + output − cacheRead × 0.9`（缓存读折让）。费率来自
//! [`zk_llm::capabilities_for`]（`cost_per_1k_input` / `cost_per_1k_output`
//! 与旧 `ModelCapabilities.costPer1kInput/costPer1kOutput` 逐字对齐）。

use zk_protocol::model::Usage;

/// 会话级费用追踪端口（旧 `CostTrackerService` 的窄接口投影）。
///
/// 实现方持有 per-session 累加器（`AtomicU64` 存 `f64::to_bits`——无锁 CAS
/// 累加）+ 全局累加器 + 最近一次单模型成本快照。
///
/// # 形态裁决
///
/// 与 [`crate::MessageSink`] 同样采用 object-safe 同步方法：`add_usage` 逐次
/// 在轮末调用，热路径开销必须最小化；实现方内部若有阻塞代价（如 lock）应
/// 自行控制到 O(session count) 级别。
pub trait CostTracker: Send + Sync {
    /// 记录一次用量并返回**累加后**的会话总费用（USD）。
    ///
    /// 逐字对照旧 `CostTrackerService.recordUsage`：
    /// 1. 由 `model` 查费率（未知模型费率为 0 → 增量 0）；
    /// 2. 按 `input + output − cacheRead × 0.9` 累加到会话与全局；
    /// 3. 快照本 `model` 的最近单次成本（供 [`Self::last_model_cost`] 读取）；
    /// 4. 返回本次累加**后**的 `session_cost`。
    fn add_usage(&self, session_id: &str, model: &str, usage: &Usage) -> f64;

    /// 读取当前会话累计费用（USD）；会话未记录时返回 0.0。
    fn session_cost(&self, session_id: &str) -> f64;

    /// 读取全局累计费用（USD）；进程重启清零（对照旧 MVP「重启清零」）。
    fn global_cost(&self) -> f64;

    /// 最近一次记入的单模型成本（USD）；模型未记录时返回 0.0。
    fn last_model_cost(&self, model: &str) -> f64;

    /// 清除指定会话累计（对照旧 `CostTrackerService.clearSession`）。
    fn reset(&self, session_id: &str);
}

/// 默认空实现——未装配 tracker 时的直通桩（对照旧未接入 `CostTrackerService`
/// 场景，与 Step 0-6 接入前行为一致）。
///
/// 所有查询返回 0.0；`add_usage` 亦返回 0.0（不累计）。仅在测试或 tracker
/// 尚未装配的组合根使用；生产装配路径见 `zk-server::engine_bridge`。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopCostTracker;

impl CostTracker for NoopCostTracker {
    fn add_usage(&self, _session_id: &str, _model: &str, _usage: &Usage) -> f64 {
        0.0
    }

    fn session_cost(&self, _session_id: &str) -> f64 {
        0.0
    }

    fn global_cost(&self) -> f64 {
        0.0
    }

    fn last_model_cost(&self, _model: &str) -> f64 {
        0.0
    }

    fn reset(&self, _session_id: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Noop 桩的五个契约点：查询恒 0、`add_usage` 恒 0、`reset` 不 panic。
    #[test]
    fn noop_tracker_returns_zero_and_is_reset_safe() {
        let tracker = NoopCostTracker;
        let usage = Usage {
            input_tokens: 1_000,
            output_tokens: 2_000,
            cache_read_input_tokens: 500,
            cache_creation_input_tokens: 0,
        };
        assert!((tracker.add_usage("s1", "kimi-k3", &usage) - 0.0).abs() < f64::EPSILON);
        assert!((tracker.session_cost("s1") - 0.0).abs() < f64::EPSILON);
        assert!((tracker.global_cost() - 0.0).abs() < f64::EPSILON);
        assert!((tracker.last_model_cost("kimi-k3") - 0.0).abs() < f64::EPSILON);
        tracker.reset("s1");
    }
}
