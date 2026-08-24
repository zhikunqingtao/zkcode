//! `REPL` 工具族——长驻解释器会话（进程池 + 空闲回收 + 输出截断）。
//!
//! 对照旧 `tool/repl/`（只读权威规格）三文件：
//! - `REPLTool.java`（176L）——工具入口，入参 `language` / `code` / `sessionId`；
//! - `ReplManager.java`（250L）——进程池、语言白名单、LRU 淘汰、周期清理；
//! - `ReplSession.java`（100L）——单会话状态（进程 + stdin/stdout/stderr）。
//!
//! # 生命周期常量（逐条对齐旧 `ReplManager` 静态字段）
//!
//! | 常量 | 值 | 旧字段 |
//! |---|---|---|
//! | [`MAX_CONCURRENT_SESSIONS`] | 3 | `MAX_CONCURRENT_SESSIONS` |
//! | [`EXEC_TIMEOUT`] | 30s | `EXEC_TIMEOUT` |
//! | [`IDLE_TIMEOUT`] | 10min | `IDLE_TIMEOUT` |
//! | [`SESSION_MAX_LIFETIME`] | 1h | `SESSION_MAX_LIFETIME` |
//! | [`MAX_OUTPUT_BYTES`] | 100 KiB | `MAX_OUTPUT_BYTES` |
//!
//! # 有意差异（留痕 docs/compatibility.md §4）
//!
//! 1. **P2 语言激活**：旧 `SUPPORTED_LANGUAGES = {python}`，`node` / `ruby` 落在
//!    `P2_LANGUAGES` 并抛 `UnsupportedOperationException("… planned for P2")`。
//!    本批即 P2 波次，故三语言一并放行，解释器 argv 逐字取自旧
//!    `getInterpreterCommand`（[`interpreter_argv`]）。
//! 2. **非阻塞读**：旧 `BufferedReader.ready()` 轮询在 Rust 无对应物。本实现
//!    每会话起两条 `tokio::io::BufReader` 读行任务，把 stdout / stderr 持续
//!    汇入各自缓冲区；`read_available_output` 改为「静默窗收敛」——出现输出后
//!    再等一个 [`QUIET_WINDOW`] 无新字节即收工，最长不超过 `EXEC_TIMEOUT`。
//!    旧实现在 200 ms 内无输出即返回空串（慢代码的输出漏到下次调用），本实现
//!    会等到首个字节，是旧行为的严格改进。
//! 3. **周期清理**：旧靠 Spring `@Scheduled(fixedDelay = 60_000)`。本实现无容器，
//!    改为**每次 `get_or_create` 前顺带清扫**（[`ReplManager::cleanup_sessions`]
//!    仍公开，便于外部定时器调用），语义等价且不额外占一条后台任务。
//! 4. **PTY 降级分支退场**：旧 `createPtySession` 是恒抛异常的桩（L131
//!    `"pty4j not available in current classpath"`），随后无条件降级到
//!    `ProcessBuilder`。本实现直接走等价的 `tokio::process::Command`，
//!    不复刻这条死代码。

mod manager;
mod session;
mod tool;

use std::time::Duration;

pub use manager::{
    MAX_CONCURRENT_SESSIONS, ReplError, ReplManager, interpreter_argv, is_supported_language,
};
pub use session::ReplSession;
pub use tool::{REPLTool, REPORTED_OUTPUT_LIMIT};

/// 单次执行超时（旧 `EXEC_TIMEOUT = Duration.ofSeconds(30)`）。
pub const EXEC_TIMEOUT: Duration = Duration::from_secs(30);

/// 会话空闲超时（旧 `IDLE_TIMEOUT = Duration.ofMinutes(10)`）。
pub const IDLE_TIMEOUT: Duration = Duration::from_mins(10);

/// 会话总寿命（旧 `SESSION_MAX_LIFETIME = Duration.ofHours(1)`）。
pub const SESSION_MAX_LIFETIME: Duration = Duration::from_hours(1);

/// 单次输出字节上限（旧 `MAX_OUTPUT_BYTES = 100 * 1024`）。
pub const MAX_OUTPUT_BYTES: usize = 100 * 1024;

/// 静默窗——连续该时长无新字节即认为本轮输出收敛。
pub const QUIET_WINDOW: Duration = Duration::from_millis(200);

/// 三种可用语言（旧 `SUPPORTED_LANGUAGES` ∪ `P2_LANGUAGES`）。
pub const LANGUAGES: [&str; 3] = ["python", "node", "ruby"];

/// 输出截断（逐字对齐旧 `ReplManager.truncateOutput`，含尾注文案）。
///
/// 旧实现按 `String.length()`（UTF-16 码元）判定，本实现按**字符**边界切分，
/// 既不会切开多字节字符，也保持「超限 → 追加尾注」的可观测形态。
#[must_use]
pub fn truncate_output(output: &str) -> String {
    if output.len() <= MAX_OUTPUT_BYTES {
        return output.to_owned();
    }
    let cut = output
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= MAX_OUTPUT_BYTES)
        .last()
        .unwrap_or(0);
    format!(
        "{}\n... [output truncated at {MAX_OUTPUT_BYTES} bytes]",
        &output[..cut]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 未超限原样返回；超限按字符边界切分并追加旧尾注文案。
    #[test]
    fn truncation_matches_legacy_tail_note() {
        assert_eq!(truncate_output("hello"), "hello");

        let long = "x".repeat(MAX_OUTPUT_BYTES + 10);
        let cut = truncate_output(&long);
        assert!(cut.ends_with(&format!(
            "\n... [output truncated at {MAX_OUTPUT_BYTES} bytes]"
        )));
        assert_eq!(cut.lines().next().expect("body").len(), MAX_OUTPUT_BYTES);
    }

    /// 多字节字符不被切开（旧按 UTF-16 码元切分会产生半个代理对）。
    #[test]
    fn truncation_never_splits_a_character() {
        let long = "字".repeat(MAX_OUTPUT_BYTES);
        let cut = truncate_output(&long);
        assert!(cut.chars().all(|ch| ch == '字' || ch.is_ascii()));
    }

    /// 生命周期常量逐条对齐旧 `ReplManager`。
    #[test]
    fn lifecycle_constants_match_legacy() {
        assert_eq!(MAX_CONCURRENT_SESSIONS, 3);
        assert_eq!(EXEC_TIMEOUT, Duration::from_secs(30));
        assert_eq!(IDLE_TIMEOUT, Duration::from_mins(10));
        assert_eq!(SESSION_MAX_LIFETIME, Duration::from_hours(1));
        assert_eq!(MAX_OUTPUT_BYTES, 102_400);
    }
}
