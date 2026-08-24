//! 会话/全局费用累加器——[`CostTracker`] 的 zk-server 侧实现。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `service/CostTrackerService.java` L21-84：`sessionCosts`
//!   `ConcurrentHashMap<String, CostSummary>` + `globalCost`
//!   `AtomicReference<CostSummary>`，`recordUsage` 按
//!   `inputCost + outputCost − cacheDiscount` 累加（`cacheDiscount =
//!   cacheReadInputTokens × costPer1kInput × 0.9 / 1000`）。
//!
//! # 存储与并发形态
//!
//! Java 侧 `CostSummary` 是聚合记录（`inputTokens / outputTokens /
//! cacheReadTokens / totalCost / apiCalls`）；下行 WS `cost_update` 只用到
//! `session.totalCost` 与 `global.totalCost`。本实现窄化为 f64 累加：
//!
//! - `session_costs: RwLock<HashMap<String, Arc<AtomicU64>>>`——每会话 `f64`
//!   累计（`AtomicU64` 存 `f64::to_bits`，无锁 CAS 累加；HashMap 增删条目由
//!   `RwLock` 保护）；
//! - `global_cost: AtomicU64`——全局累加，同格式；
//! - `last_model_costs: RwLock<HashMap<String, Arc<AtomicU64>>>`——每模型最近
//!   一次单调用成本快照（对照 Java 侧 debug 日志显示的 `totalCost` 单次值）。
//!
//! **无 `unsafe`**：`AtomicU64` 由 `std::sync::atomic` 提供，`f64::to_bits`
//! 是安全转换；`compare_exchange_weak` 循环即 CAS。
//!
//! # 未采用聚合 `CostSummary` 的理由
//!
//! `CostUpdate` 载荷字段就是 `session_cost: f64 / total_cost: f64 + Usage`，
//! 聚合 `inputTokens / apiCalls` 目前无下行消费方（Java 侧 `getAllSessionCosts`
//! 亦无 WS 出口），故保持窄化。若后续 REST 端点需暴露聚合，可再向 tracker 追
//! 加计数字段而不破坏当前 trait。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use zk_engine::CostTracker;
use zk_engine::query_config::usage_cost_usd;
use zk_protocol::model::Usage;

/// 会话/全局费用累加器（对照旧 `CostTrackerService`）。
///
/// 实现要点：所有累加走 `AtomicU64` + `f64::to_bits` 的无锁 CAS 循环，与旧
/// `AtomicReference<CostSummary>.updateAndGet` 语义等价（无锁读改写）；条目
/// 增删由外层 `RwLock<HashMap>` 保护（Rust 标准库无 concurrent map，写路径
/// 短——仅在首次遇到新 sessionId / model 时上写锁）。
#[derive(Debug, Default)]
pub struct AtomicCostTracker {
    /// 会话 → 累计费用（USD）。`Arc<AtomicU64>` 允许 `add_usage` 释放读锁后仍
    /// 安全 CAS（避免持读锁时长时间循环）。
    session_costs: RwLock<HashMap<String, Arc<AtomicU64>>>,

    /// 进程全局累计费用（USD）。重启清零，与旧 MVP 一致。
    global_cost: AtomicU64,

    /// 模型 → 最近一次单调用成本（USD）。
    last_model_costs: RwLock<HashMap<String, Arc<AtomicU64>>>,
}

impl AtomicCostTracker {
    /// 新建空累加器（会话/全局/模型快照均 0）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// CAS 循环累加：`atomic += delta`，返回累加后的 f64。
    ///
    /// 用 `compare_exchange_weak` + `f64::to_bits` 无锁累加，对齐旧
    /// `AtomicReference.updateAndGet(current -> current.add(delta))` 语义。
    fn fetch_add_f64(atomic: &AtomicU64, delta: f64) -> f64 {
        let mut current_bits = atomic.load(Ordering::Acquire);
        loop {
            let new_value = f64::from_bits(current_bits) + delta;
            match atomic.compare_exchange_weak(
                current_bits,
                new_value.to_bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return new_value,
                Err(actual) => current_bits = actual,
            }
        }
    }

    /// 取到指定 key 的 `Arc<AtomicU64>` 累加器（首次访问上写锁初始化 0）。
    fn get_or_init(map: &RwLock<HashMap<String, Arc<AtomicU64>>>, key: &str) -> Arc<AtomicU64> {
        if let Some(cell) = map.read().expect("cost tracker RwLock poisoned").get(key) {
            return Arc::clone(cell);
        }
        let mut writer = map.write().expect("cost tracker RwLock poisoned");
        Arc::clone(
            writer
                .entry(key.to_owned())
                .or_insert_with(|| Arc::new(AtomicU64::new(0))),
        )
    }
}

impl CostTracker for AtomicCostTracker {
    fn add_usage(&self, session_id: &str, model: &str, usage: &Usage) -> f64 {
        // 单次成本（复用 zk-engine 中的公式实现，费率与旧 recordUsage 逐字对齐）。
        let delta = usage_cost_usd(model, usage);
        // 未知模型 → 费率 0 → delta 0；仍然为会话/全局占位（首次访问预热条目）
        // 与旧行为等价（Java 侧亦对 caps=DEFAULT 走同一累加分支）。
        let session_cell = Self::get_or_init(&self.session_costs, session_id);
        let session_after = Self::fetch_add_f64(&session_cell, delta);
        Self::fetch_add_f64(&self.global_cost, delta);
        // 记录本次模型单调用成本（供 last_model_cost 读取）。
        let model_cell = Self::get_or_init(&self.last_model_costs, model);
        model_cell.store(delta.to_bits(), Ordering::Release);
        session_after
    }

    fn session_cost(&self, session_id: &str) -> f64 {
        self.session_costs
            .read()
            .expect("cost tracker RwLock poisoned")
            .get(session_id)
            .map_or(0.0, |cell| f64::from_bits(cell.load(Ordering::Acquire)))
    }

    fn global_cost(&self) -> f64 {
        f64::from_bits(self.global_cost.load(Ordering::Acquire))
    }

    fn last_model_cost(&self, model: &str) -> f64 {
        self.last_model_costs
            .read()
            .expect("cost tracker RwLock poisoned")
            .get(model)
            .map_or(0.0, |cell| f64::from_bits(cell.load(Ordering::Acquire)))
    }

    fn reset(&self, session_id: &str) {
        self.session_costs
            .write()
            .expect("cost tracker RwLock poisoned")
            .remove(session_id);
        // 注意：全局累加不清零——对照 Java 侧 `clearSession` 亦仅 `sessionCosts.remove`，
        // `globalCost` 只在进程重启时归零。
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_usage() -> Usage {
        Usage {
            input_tokens: 10_000,
            output_tokens: 2_000,
            cache_read_input_tokens: 4_000,
            cache_creation_input_tokens: 0,
        }
    }

    /// 费率公式：kimi-k3 = 0.002 / 1k 输入、0.012 / 1k 输出，缓存读 9 折折让。
    /// 逐字对照 [`usage_cost_usd`] 单测的期望值（同一公式）。
    fn expected_kimi_delta() -> f64 {
        10_000.0 * 0.002 / 1000.0 + 2_000.0 * 0.012 / 1000.0 - 4_000.0 * 0.002 * 0.9 / 1000.0
    }

    /// (1) `add_usage` 首次调用：返回本次 session 累加值（== 本次 delta）；
    /// (2) 再次调用：等于两次 delta 之和；
    /// (3) `session_cost` / `global_cost` 与累加值一致；
    /// (4) `last_model_cost` 记录本次单调用成本。
    #[test]
    fn add_usage_accumulates_and_snapshots_last_model_cost() {
        let tracker = AtomicCostTracker::new();
        let usage = sample_usage();
        let expected = expected_kimi_delta();

        let first = tracker.add_usage("session-1", "kimi-k3", &usage);
        assert!((first - expected).abs() < 1e-12, "{first} vs {expected}");
        assert!((tracker.session_cost("session-1") - expected).abs() < 1e-12);
        assert!((tracker.global_cost() - expected).abs() < 1e-12);
        assert!((tracker.last_model_cost("kimi-k3") - expected).abs() < 1e-12);

        let second = tracker.add_usage("session-1", "kimi-k3", &usage);
        assert!((second - 2.0 * expected).abs() < 1e-12);
        assert!((tracker.session_cost("session-1") - 2.0 * expected).abs() < 1e-12);
        assert!((tracker.global_cost() - 2.0 * expected).abs() < 1e-12);
        // 单模型最近一次成本仍为一次 delta（不是累计）。
        assert!((tracker.last_model_cost("kimi-k3") - expected).abs() < 1e-12);
    }

    /// 多会话独立累加，全局 = 各会话之和。
    #[test]
    fn multiple_sessions_accumulate_independently() {
        let tracker = AtomicCostTracker::new();
        let usage = sample_usage();
        let delta = expected_kimi_delta();

        tracker.add_usage("s-A", "kimi-k3", &usage);
        tracker.add_usage("s-B", "kimi-k3", &usage);
        tracker.add_usage("s-B", "kimi-k3", &usage);

        assert!((tracker.session_cost("s-A") - delta).abs() < 1e-12);
        assert!((tracker.session_cost("s-B") - 2.0 * delta).abs() < 1e-12);
        assert!((tracker.global_cost() - 3.0 * delta).abs() < 1e-12);
    }

    /// `reset(session)` 只清会话；全局累计保持（对照 Java `clearSession`）。
    #[test]
    fn reset_clears_session_but_not_global() {
        let tracker = AtomicCostTracker::new();
        let usage = sample_usage();
        let delta = expected_kimi_delta();

        tracker.add_usage("s-A", "kimi-k3", &usage);
        tracker.add_usage("s-B", "kimi-k3", &usage);
        tracker.reset("s-A");

        assert!(tracker.session_cost("s-A").abs() < f64::EPSILON);
        assert!((tracker.session_cost("s-B") - delta).abs() < 1e-12);
        assert!((tracker.global_cost() - 2.0 * delta).abs() < 1e-12);
    }

    /// 未知模型费率 0 → delta 0 → 累加不变，`last_model_cost` 记 0（保持行为
    /// 与旧 `caps=DEFAULT` 通道一致：不特殊化未知模型分支）。
    #[test]
    fn unknown_model_yields_zero_delta() {
        let tracker = AtomicCostTracker::new();
        let usage = sample_usage();
        let value = tracker.add_usage("s", "nope-9000", &usage);
        assert!(value.abs() < f64::EPSILON);
        assert!(tracker.session_cost("s").abs() < f64::EPSILON);
        assert!(tracker.global_cost().abs() < f64::EPSILON);
        assert!(tracker.last_model_cost("nope-9000").abs() < f64::EPSILON);
    }

    /// 并发多线程 CAS 累加正确性（`AtomicU64` + CAS 循环无丢失）。
    #[test]
    fn concurrent_add_usage_is_lossless() {
        use std::thread;

        let tracker = Arc::new(AtomicCostTracker::new());
        let usage = sample_usage();
        let delta = expected_kimi_delta();

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let t = Arc::clone(&tracker);
                let u = usage;
                thread::spawn(move || {
                    for _ in 0..100 {
                        t.add_usage("shared", "kimi-k3", &u);
                    }
                })
            })
            .collect();
        for th in threads {
            th.join().expect("worker thread panicked");
        }
        let expected = 800.0 * delta;
        let session = tracker.session_cost("shared");
        // f64 累加 800 次的相对误差保守放宽（远小于任何 f64 精度门槛）。
        assert!(
            (session - expected).abs() < 1e-9,
            "session={session} expected={expected}"
        );
        assert!((tracker.global_cost() - expected).abs() < 1e-9);
    }
}
