//! 3A.4 三级并发门控——全局 / 会话 / 嵌套深度三层硬限制 + RAII 槽位守卫。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `tool/agent/AgentConcurrencyController.java`：控制器主体（`acquireSlot`
//!   判定序、`AgentSlot` 释放序、`getActiveCount` / `getSessionActiveCount`）；
//! - `tool/agent/AgentLimitExceededException.java`：越限异常。
//!
//! # 逐值对齐（Java `main@581d407b` 实值为准）
//!
//! | 常量 | Java 实值 | 本模块 |
//! |---|---|---|
//! | `MAX_CONCURRENT_AGENTS` | 30 | [`MAX_CONCURRENT_AGENTS`] |
//! | `MAX_CONCURRENT_AGENTS_PER_SESSION` | 10 | [`MAX_CONCURRENT_AGENTS_PER_SESSION`] |
//! | `MAX_AGENT_NESTING_DEPTH` | 3 | [`MAX_AGENT_NESTING_DEPTH`] |
//!
//! # 判定序（严格对照 Java `acquireSlot`）
//!
//! 1. 嵌套深度检查：`nesting_depth > 3` → 抛（**纯比较，无锁**，depth 3 允许、
//!    4 拒绝）；
//! 2. 全局并发检查：`tryAcquire` 失败 → 抛 `Concurrent agent limit reached (30)`；
//! 3. 会话级检查：`incrementAndGet` 后 `> 10` → 回退（session-- + global
//!    release）+ 抛 `Session {id} concurrent agent limit reached ({n}/{10})`；
//! 4. `activeAgentCount++` → 返回 [`AgentSlot`]。
//!
//! 释放序（[`AgentSlot`] Drop，对照 Java `AgentSlot.close`）：
//! `activeAgentCount--` → global release → `sessionCount--`。
//!
//! # 与任务书 / Java 的偏离（以 Java 为准，留痕）
//!
//! - **超时语义**：任务书要求「获取超时语义对齐 Java」。Java `acquireSlot` 用
//!   `globalSemaphore.tryAcquire()`——**非阻塞、无超时**：无空位即刻抛异常，
//!   不等待。本实现同为非阻塞 try（无 timeout 等待），与 Java 逐语义对齐。
//! - **锁序**：任务书写「固定锁序 global→session→nesting」。Java 实际是
//!   *检查序* nesting(纯比较)→global→session，且底层用 lock-free 原语
//!   （`Semaphore` / `AtomicInteger` / `ConcurrentHashMap`）无显式锁排序。本
//!   实现以**单个内部 [`std::sync::Mutex`]** 守护全部计数：既无多锁排序隐患
//!   （单锁不可能死锁），又提供比 Java 分立原语**更强**的事务原子性；对外可
//!   观察计数（active / session）与 Java 逐值一致。判定序 / 释放序严格保留
//!   Java 顺序。
//!
//! # Feature-flag（沿用 #34 `OnceLock` 进程级读取风格）
//!
//! [`concurrency_gate_enabled`] 读取 [`CONCURRENCY_GATE_ENABLED_ENV`]，缺省开启；
//! 置 `false` / `0` / `off` / `no` 时 [`AgentConcurrencyController::new`] 构造的
//! 控制器**一键退回接入前行为**：[`AgentConcurrencyController::acquire_slot`]
//! 恒成功、不计数、不设上限（门控仅为资源安全，不削弱模型推理档位）。

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

/// 全局并发代理数硬限制（逐值对照旧 `MAX_CONCURRENT_AGENTS`）。
pub const MAX_CONCURRENT_AGENTS: u32 = 30;

/// 单会话并发代理数硬限制（逐值对照旧 `MAX_CONCURRENT_AGENTS_PER_SESSION`）。
pub const MAX_CONCURRENT_AGENTS_PER_SESSION: u32 = 10;

/// 代理嵌套深度硬限制（逐值对照旧 `MAX_AGENT_NESTING_DEPTH`）。
pub const MAX_AGENT_NESTING_DEPTH: u32 = 3;

/// 并发门控总开关环境变量（`false` / `0` / `off` / `no` → 关闭，其余含缺省
/// → 开启）。
pub const CONCURRENCY_GATE_ENABLED_ENV: &str = "ZK_AGENT_CONCURRENCY_ENABLED";

/// 并发门控总开关（进程级一次性读取环境变量；缺省开启）。
///
/// 关闭时 [`AgentConcurrencyController::new`] 的控制器行为与接入前逐字一致：
/// 不设任何并发上限、不计数——用于热路径回归时一键退回。
#[must_use]
pub fn concurrency_gate_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var(CONCURRENCY_GATE_ENABLED_ENV) {
        Ok(raw) => !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "false" | "0" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// 越限异常（对照旧 `AgentLimitExceededException`）。
///
/// 消息文案逐字对齐 Java，供上层按关键短语识别越限类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLimitExceededError {
    message: String,
}

impl AgentLimitExceededError {
    fn new(message: String) -> Self {
        Self { message }
    }

    /// 越限说明文案（含 Java 关键短语）。
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for AgentLimitExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AgentLimitExceededError {}

/// 控制器内部共享计数（单 [`Mutex`] 守护——见模块文档「锁序」偏离说明）。
#[derive(Debug, Default)]
struct ControllerState {
    /// 活跃代理数（对照旧 `activeAgentCount`）。
    active_agent_count: u32,
    /// 已占用的全局槽位数（对照旧 `globalSemaphore` 的已获取许可数，0..=30）。
    global_in_use: u32,
    /// 会话级活跃代理数（对照旧 `sessionAgentCounts`；条目命中 0 不删除，与
    /// Java `ConcurrentHashMap` 保留 `AtomicInteger(0)` 一致）。
    session_counts: HashMap<String, u32>,
}

/// 代理并发控制器（对照旧 `AgentConcurrencyController`）。
///
/// 每个子代理执行前经 [`acquire_slot`](Self::acquire_slot) 申请槽位，得
/// [`AgentSlot`] RAII 守卫；守卫 Drop 时自动释放三层计数。
pub struct AgentConcurrencyController {
    state: Arc<Mutex<ControllerState>>,
    gate_enabled: bool,
}

impl fmt::Debug for AgentConcurrencyController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentConcurrencyController")
            .field("gate_enabled", &self.gate_enabled)
            .finish_non_exhaustive()
    }
}

impl Default for AgentConcurrencyController {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentConcurrencyController {
    /// 构造控制器，门控开关取 [`concurrency_gate_enabled`]（读环境变量）。
    #[must_use]
    pub fn new() -> Self {
        Self::with_gate(concurrency_gate_enabled())
    }

    /// 以显式门控开关构造控制器（供引擎显式配置 / 测试确定性覆盖两条路径）。
    ///
    /// `gate_enabled == false` 时 [`acquire_slot`](Self::acquire_slot) 恒成功、
    /// 不计数、不设上限（退回接入前行为）。
    #[must_use]
    pub fn with_gate(gate_enabled: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(ControllerState::default())),
            gate_enabled,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ControllerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 尝试获取代理执行槽位（严格对照旧 `acquireSlot` 判定序）。
    ///
    /// 成功返回 [`AgentSlot`] RAII 守卫；越限返回 [`AgentLimitExceededError`]。
    /// 门控关闭时恒返回不计数的直通守卫。
    ///
    /// # Errors
    ///
    /// - 嵌套深度 `> 3`：`Agent nesting depth {d} exceeds max 3`；
    /// - 全局并发满 30：`Concurrent agent limit reached (30)`；
    /// - 会话并发满 10：`Session {id} concurrent agent limit reached (10/10)`。
    pub fn acquire_slot(
        &self,
        agent_id: &str,
        nesting_depth: u32,
        session_id: &str,
    ) -> Result<AgentSlot, AgentLimitExceededError> {
        // 门控关闭 → 直通守卫（Drop 无操作，不触碰计数）。
        if !self.gate_enabled {
            return Ok(AgentSlot {
                agent_id: agent_id.to_owned(),
                session_id: session_id.to_owned(),
                state: None,
            });
        }

        // 1. 嵌套深度检查（纯比较，无锁；对照 Java 步骤 1）。
        if nesting_depth > MAX_AGENT_NESTING_DEPTH {
            return Err(AgentLimitExceededError::new(format!(
                "Agent nesting depth {nesting_depth} exceeds max {MAX_AGENT_NESTING_DEPTH}"
            )));
        }

        let mut state = self.lock();

        // 2. 全局并发检查（对照 Java `globalSemaphore.tryAcquire()`，非阻塞）。
        if state.global_in_use >= MAX_CONCURRENT_AGENTS {
            return Err(AgentLimitExceededError::new(format!(
                "Concurrent agent limit reached ({MAX_CONCURRENT_AGENTS})"
            )));
        }
        state.global_in_use += 1;

        // 3. 会话级检查（对照 Java computeIfAbsent + incrementAndGet + 回退）。
        let current_session_count = {
            let count = state
                .session_counts
                .entry(session_id.to_owned())
                .or_insert(0);
            *count += 1;
            *count
        };
        if current_session_count > MAX_CONCURRENT_AGENTS_PER_SESSION {
            // 回退：session-- + global release（对照 Java 回退两步）。
            if let Some(count) = state.session_counts.get_mut(session_id) {
                *count = count.saturating_sub(1);
            }
            state.global_in_use = state.global_in_use.saturating_sub(1);
            return Err(AgentLimitExceededError::new(format!(
                "Session {session_id} concurrent agent limit reached ({}/{})",
                current_session_count - 1,
                MAX_CONCURRENT_AGENTS_PER_SESSION
            )));
        }

        // 4. 活跃计数 + 返回守卫（对照 Java 步骤 4/5）。
        state.active_agent_count += 1;
        drop(state);
        Ok(AgentSlot {
            agent_id: agent_id.to_owned(),
            session_id: session_id.to_owned(),
            state: Some(Arc::clone(&self.state)),
        })
    }

    /// 当前活跃代理数（对照旧 `getActiveCount`）。
    #[must_use]
    pub fn active_count(&self) -> u32 {
        self.lock().active_agent_count
    }

    /// 指定会话的活跃代理数（对照旧 `getSessionActiveCount`；未知会话为 0）。
    #[must_use]
    pub fn session_active_count(&self, session_id: &str) -> u32 {
        self.lock()
            .session_counts
            .get(session_id)
            .copied()
            .unwrap_or(0)
    }
}

/// RAII 槽位守卫（对照旧 `AgentSlot`，`AutoCloseable` → Rust [`Drop`]）。
///
/// Drop 时自动释放三层计数（释放序：active → global → session，对照
/// `AgentSlot.close`）。门控关闭时 `state` 为 `None`，Drop 无操作。
pub struct AgentSlot {
    agent_id: String,
    session_id: String,
    state: Option<Arc<Mutex<ControllerState>>>,
}

impl AgentSlot {
    /// 代理唯一标识。
    #[must_use]
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// 会话 ID。
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

impl fmt::Debug for AgentSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentSlot")
            .field("agent_id", &self.agent_id)
            .field("session_id", &self.session_id)
            .field("gated", &self.state.is_some())
            .finish()
    }
}

impl Drop for AgentSlot {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let mut guard = state.lock().unwrap_or_else(PoisonError::into_inner);
        // 释放序严格对照 Java `close`：active-- → global release → session--。
        guard.active_agent_count = guard.active_agent_count.saturating_sub(1);
        guard.global_in_use = guard.global_in_use.saturating_sub(1);
        if let Some(count) = guard.session_counts.get_mut(&self.session_id) {
            *count = count.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentConcurrencyController, MAX_CONCURRENT_AGENTS, MAX_CONCURRENT_AGENTS_PER_SESSION,
    };

    /// 全局 30 槽位全成功、第 31 失败（对照 shouldAcquire30GlobalSlots /
    /// shouldRejectThe31stGlobalSlot）。
    #[test]
    fn global_limit_admits_30_and_rejects_31st() {
        let controller = AgentConcurrencyController::with_gate(true);
        let mut slots = Vec::new();
        for index in 0..MAX_CONCURRENT_AGENTS {
            let slot = controller
                .acquire_slot(&format!("agent-{index}"), 1, &format!("session-{index}"))
                .expect("前 30 个应全部成功");
            slots.push(slot);
        }
        assert_eq!(controller.active_count(), MAX_CONCURRENT_AGENTS);
        let err = controller
            .acquire_slot("agent-31", 1, "session-31")
            .expect_err("第 31 个应越限");
        assert!(err.message().contains("Concurrent agent limit reached"));
    }

    /// 释放 1 个后可再次获取（对照 shouldAcquireAfterRelease）。
    #[test]
    fn slot_released_by_drop_frees_global_capacity() {
        let controller = AgentConcurrencyController::with_gate(true);
        let mut slots = Vec::new();
        for index in 0..MAX_CONCURRENT_AGENTS {
            slots.push(
                controller
                    .acquire_slot(&format!("agent-{index}"), 1, &format!("session-{index}"))
                    .expect("填满"),
            );
        }
        drop(slots.remove(0));
        assert_eq!(controller.active_count(), MAX_CONCURRENT_AGENTS - 1);
        let fresh = controller
            .acquire_slot("agent-new", 1, "session-new")
            .expect("释放后应可再获取");
        assert_eq!(controller.active_count(), MAX_CONCURRENT_AGENTS);
        drop(fresh);
    }

    /// 单会话 10 成功、第 11 失败（对照 shouldAcquire10SessionSlots /
    /// shouldRejectThe11thSessionSlot），且消息含 Java 关键短语与 (10/10)。
    #[test]
    fn session_limit_admits_10_and_rejects_11th() {
        let controller = AgentConcurrencyController::with_gate(true);
        let session = "session-single";
        let mut slots = Vec::new();
        for index in 0..MAX_CONCURRENT_AGENTS_PER_SESSION {
            slots.push(
                controller
                    .acquire_slot(&format!("agent-{index}"), 1, session)
                    .expect("单会话前 10 成功"),
            );
        }
        assert_eq!(controller.session_active_count(session), 10);
        let err = controller
            .acquire_slot("agent-11", 1, session)
            .expect_err("第 11 越限");
        assert!(err.message().contains("Session"));
        assert!(err.message().contains("concurrent agent limit reached"));
        assert!(err.message().contains("(10/10)"));
        // 会话越限后全局槽位已回退（未泄漏）。
        assert_eq!(controller.active_count(), 10);
    }

    /// 嵌套深度 1..=3 成功、4 失败（对照 shouldAllowNestingUpTo3 /
    /// shouldRejectNestingDepth4）。
    #[test]
    fn nesting_depth_admits_up_to_3_and_rejects_4() {
        let controller = AgentConcurrencyController::with_gate(true);
        for depth in 1..=3 {
            let slot = controller
                .acquire_slot(&format!("agent-depth-{depth}"), depth, "session-nest")
                .expect("深度 1..=3 成功");
            drop(slot);
        }
        let err = controller
            .acquire_slot("agent-deep", 4, "session-nest")
            .expect_err("深度 4 越限");
        assert!(err.message().contains("nesting depth"));
        assert!(err.message().contains("exceeds max"));
    }

    /// RAII 自动释放：作用域结束后 active / session 均归零（对照
    /// shouldAutoReleaseSlotOnClose 的 try-with-resources）。
    #[test]
    fn raii_drop_releases_all_three_layers() {
        let controller = AgentConcurrencyController::with_gate(true);
        {
            let _slot = controller
                .acquire_slot("agent-auto", 1, "session-auto")
                .expect("获取成功");
            assert_eq!(controller.active_count(), 1);
            assert_eq!(controller.session_active_count("session-auto"), 1);
        }
        assert_eq!(controller.active_count(), 0);
        assert_eq!(controller.session_active_count("session-auto"), 0);
    }

    /// 门控关闭 → 一键退回接入前行为：越上限仍恒成功、不计数。
    #[test]
    fn disabled_gate_bypasses_all_limits() {
        let controller = AgentConcurrencyController::with_gate(false);
        let mut slots = Vec::new();
        // 远超 30 全局 / 10 会话仍全部成功。
        for index in 0..100 {
            slots.push(
                controller
                    .acquire_slot(&format!("agent-{index}"), 9, "session-x")
                    .expect("门控关闭恒成功"),
            );
        }
        // 不计数（退回接入前无门控行为）。
        assert_eq!(controller.active_count(), 0);
        assert_eq!(controller.session_active_count("session-x"), 0);
    }

    /// 会话计数隔离：不同会话独立计数，互不影响全局回退正确性。
    #[test]
    fn distinct_sessions_count_independently() {
        let controller = AgentConcurrencyController::with_gate(true);
        let _a = controller.acquire_slot("a", 1, "s1").expect("s1");
        let _b = controller.acquire_slot("b", 1, "s2").expect("s2");
        assert_eq!(controller.session_active_count("s1"), 1);
        assert_eq!(controller.session_active_count("s2"), 1);
        assert_eq!(controller.active_count(), 2);
    }
}
