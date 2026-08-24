//! zk-engine 引擎侧集成测试（零网络）：3A.4 三级并发门控 +
//! 3A.5 `ToolResultSummarizer` 与 `ContextCascade` / `TokenBudget` 家族协作。
//!
//! 覆盖：多线程真实并发压 `AgentConcurrencyController`（全局/会话上限、
//! RAII Drop 释放、嵌套深度上界），以及 `ToolResultSummarizer` 对真实
//! `ChatMessage` 历史的轮末截断经 `zk_engine::estimate_tokens` 验证 token 下降。

use std::sync::{Arc, Barrier, Mutex};

use zk_engine::{
    AgentConcurrencyController, MAX_AGENT_NESTING_DEPTH, MAX_CONCURRENT_AGENTS,
    MAX_CONCURRENT_AGENTS_PER_SESSION, SOFT_LIMIT_CHARS, SUMMARIZE_TOOL_RESULTS_SECTION,
    ToolResultSummarizer, URGENT_SUMMARIZE_HINT, estimate_tokens,
};
use zk_llm::ChatMessage;

/// 全局并发上限在真实多线程竞争下精确封顶（对照旧 `globalSemaphore(30)`）。
///
/// 40 线程同栅栏齐发、各用独立会话（会话上限不成为约束），持槽不释放：
/// 恰好 [`MAX_CONCURRENT_AGENTS`] 个成功、其余失败；活跃计数等于上限；
/// 全部 Drop 后计数归零（验证 RAII 释放线程安全）。
#[test]
fn global_limit_holds_under_concurrent_pressure() {
    let controller = Arc::new(AgentConcurrencyController::new());
    let n = MAX_CONCURRENT_AGENTS as usize + 10;
    let barrier = Arc::new(Barrier::new(n));
    let results = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..n {
        let controller = Arc::clone(&controller);
        let barrier = Arc::clone(&barrier);
        let results = Arc::clone(&results);
        handles.push(std::thread::spawn(move || {
            // 齐发：最大化真实并发争用。
            barrier.wait();
            // 每线程独立会话 → 会话上限不绑定，仅全局上限封顶。
            let slot = controller.acquire_slot(&format!("agent-{i}"), 0, &format!("sess-{i}"));
            // Ok 槽位移入共享 Vec 持有（不 Drop），使成功者占满上限。
            results.lock().expect("results lock").push(slot);
        }));
    }
    for handle in handles {
        handle.join().expect("thread join");
    }

    let granted = results.lock().expect("results lock");
    let ok = granted.iter().filter(|r| r.is_ok()).count();
    let err = granted.iter().filter(|r| r.is_err()).count();
    assert_eq!(ok, MAX_CONCURRENT_AGENTS as usize, "恰好全局上限个获批");
    assert_eq!(err, 10, "超出者被拒");
    assert_eq!(
        controller.active_count(),
        MAX_CONCURRENT_AGENTS,
        "活跃计数封顶"
    );
    drop(granted);

    // 全部释放 → RAII Drop 归零。
    results.lock().expect("results lock").clear();
    assert_eq!(controller.active_count(), 0, "全部 Drop 后活跃归零");
}

/// 单会话并发上限精确封顶（对照旧 `sessionAgentCounts` + 每会话 10）。
///
/// 20 线程同会话齐发：恰好 [`MAX_CONCURRENT_AGENTS_PER_SESSION`] 个成功。
#[test]
fn per_session_limit_holds_under_concurrent_pressure() {
    let controller = Arc::new(AgentConcurrencyController::new());
    let n = MAX_CONCURRENT_AGENTS_PER_SESSION as usize + 10;
    let barrier = Arc::new(Barrier::new(n));
    let results = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..n {
        let controller = Arc::clone(&controller);
        let barrier = Arc::clone(&barrier);
        let results = Arc::clone(&results);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            // 同一会话 → 会话上限先于全局上限绑定。
            let slot = controller.acquire_slot(&format!("agent-{i}"), 0, "shared-session");
            results.lock().expect("results lock").push(slot);
        }));
    }
    for handle in handles {
        handle.join().expect("thread join");
    }

    let granted = results.lock().expect("results lock");
    let ok = granted.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        ok, MAX_CONCURRENT_AGENTS_PER_SESSION as usize,
        "恰好会话上限个获批"
    );
    assert_eq!(
        controller.session_active_count("shared-session"),
        MAX_CONCURRENT_AGENTS_PER_SESSION,
        "会话计数封顶"
    );
    drop(granted);
    results.lock().expect("results lock").clear();
    assert_eq!(
        controller.session_active_count("shared-session"),
        0,
        "释放后会话计数归零"
    );
}

/// 嵌套深度上界（纯比较，对照旧步骤 1）：depth 0..=3 允许、4 拒绝，
/// 失败不占用任何计数（RAII 与计数不变式）。
#[test]
fn nesting_depth_upper_bound() {
    let controller = AgentConcurrencyController::new();
    let mut slots = Vec::new();
    for depth in 0..=MAX_AGENT_NESTING_DEPTH {
        let slot = controller
            .acquire_slot("agent", depth, "sess")
            .expect("depth within bound acquires");
        slots.push(slot);
    }
    // 超界深度被拒。
    let over = controller.acquire_slot("agent", MAX_AGENT_NESTING_DEPTH + 1, "sess");
    assert!(over.is_err(), "超出嵌套上限被拒");
    // 拒绝不改变已有计数（4 个深度获批仍在）。
    assert_eq!(controller.active_count(), MAX_AGENT_NESTING_DEPTH + 1);
    drop(slots);
    assert_eq!(controller.active_count(), 0);
}

/// 3A.5 与 ContextCascade/TokenBudget 家族协作：轮末截断过大工具结果，
/// 经 `estimate_tokens` 验证上下文 token 真实下降；小消息不受影响。
#[test]
fn summarizer_reduces_context_tokens_measured_by_estimator() {
    let summarizer = ToolResultSummarizer::new();

    // 构造真实历史：系统 + 用户 + 助手 + 一个远超软上限的工具结果。
    let oversized = "x".repeat(SOFT_LIMIT_CHARS * 2);
    let messages = vec![
        ChatMessage::system("you are a coding agent"),
        ChatMessage::user("read the whole file"),
        ChatMessage::assistant("calling Read"),
        ChatMessage::tool("call-1", oversized.clone()),
        ChatMessage::user("thanks"),
    ];

    // 空模型名 → DEFAULT_CAPABILITIES 3.5 字符/token（复现 Java 无模型口径）。
    let before = estimate_tokens(&messages, "");
    let processed = summarizer.process_tool_results(&messages, 0);
    let after = estimate_tokens(&processed, "");

    assert!(
        after < before,
        "截断后上下文 token 必须下降：before={before} after={after}"
    );
    // 工具结果被截断（长度显著缩短），其余消息逐字不变。
    assert!(
        processed[3].content.len() < oversized.len(),
        "工具结果被截断"
    );
    assert_eq!(processed[0].content, "you are a coding agent");
    assert_eq!(processed[1].content, "read the whole file");
    assert_eq!(processed[2].content, "calling Read");
    assert_eq!(processed[4].content, "thanks");
    assert_eq!(processed.len(), messages.len(), "消息条数不变");
}

/// 小工具结果（<= 软上限）不触发截断——保证接入不破坏正常轮末回填。
#[test]
fn summarizer_leaves_small_tool_results_untouched() {
    let summarizer = ToolResultSummarizer::new();
    let messages = vec![
        ChatMessage::user("hi"),
        ChatMessage::tool("call-1", "small result"),
    ];
    let processed = summarizer.process_tool_results(&messages, 0);
    assert_eq!(processed, messages, "小结果逐字不变");
}

/// 系统提示动态段二选一（对照旧 `SystemPromptBuilder.getSummarizeToolResultsSection`
/// L1211-1221——旧仓库中 `shouldInjectSummarizeHint` 的唯一生产调用点）：
/// 真实历史逼近上下文上限 70% 时切 [`URGENT_SUMMARIZE_HINT`]，否则用基础段。
///
/// 该方法**不**向消息流注入新消息（旧同），故此处以段落身份而非消息序列断言。
#[test]
fn summarize_section_switches_to_urgent_near_context_limit() {
    let summarizer = ToolResultSummarizer::new();
    let messages = vec![
        ChatMessage::system("you are a coding agent"),
        ChatMessage::user("read the whole file"),
        ChatMessage::tool("call-1".to_owned(), "x".repeat(7_000)),
    ];
    let tokens = estimate_tokens(&messages, "");

    // 上限等于当前 token 数 → tokens > limit × 0.7 命中，切紧急提示。
    let tight = tokens;
    assert_eq!(
        summarizer.summarize_tool_results_section(&messages, tight),
        URGENT_SUMMARIZE_HINT,
        "越过 70% 应切紧急提示（tokens={tokens}, limit={tight}）"
    );

    // 上限放宽一倍 → 基础段。
    assert_eq!(
        summarizer.summarize_tool_results_section(&messages, tokens * 4),
        SUMMARIZE_TOOL_RESULTS_SECTION
    );
    // limit == 0（旧 `limit > 0` 守卫）与空历史 → 基础段。
    assert_eq!(
        summarizer.summarize_tool_results_section(&messages, 0),
        SUMMARIZE_TOOL_RESULTS_SECTION
    );
    assert_eq!(
        summarizer.summarize_tool_results_section(&[], tight),
        SUMMARIZE_TOOL_RESULTS_SECTION
    );
}
