//! 自修正循环主控逻辑。
//!
//! 对齐旧 `engine/correction/SelfCorrectionLoop.java`（396 行）+
//! `CorrectionInstruction.java`（record 结构）。负责检测工具输出中的编译错误 /
//! 测试失败，生成结构化修复指令，并判定是否中止修复循环。
//!
//! 修复流程（对齐旧 `detectAndPrepareCorrection`）：
//! 1. 超限检查（仓库感知，SWE-bench 上下文放宽到 7 次）；
//! 2. 检测错误——**编译错误优先于测试失败**（编译修复后测试失败可能自动消除）；
//! 3. 生成结构化指令，受 [`MAX_INSTRUCTION_TOKENS`] 约束（超限按 0.9 安全系数截断）。
//!
//! # 与 Java 基线的有意差异（已核对，本 Batch 决策）
//!
//! 1. **`CorrectionType::RuntimeError` 为预留值**——旧 `CorrectionType` 仅
//!    `COMPILE_ERROR` / `TEST_FAILURE`；本枚举按任务规格补 `RuntimeError`，当前
//!    [`detect_and_prepare_correction`] 不产出该型（无对应解析器），留作后续运行时
//!    错误检测接入点。
//! 2. **`should_abort` 语义为「停滞即止」而非旧「回归即止」**——旧
//!    `SelfCorrectionLoop.shouldAbort` 在「新错误更多 / 出现新错误文件 / 新错误
//!    类型」（修复引入回归）时中止。本 Batch 按任务规格改为三条件：① 新旧输出
//!    完全相同 ② 尝试次数达上限（由 [`detect_and_prepare_correction`] 前置守卫，
//!    见下）③ 无新信息（新错误标识集合是旧集合子集）。即从「修复变糟就停」改为
//!    「不再产生新信息就停」。条件 ② 因签名不含 `attempts`，改由
//!    [`detect_and_prepare_correction`] 的超限检查（`attempts >= max_attempts`
//!    → `None`）在上游落实，语义等价。

use std::collections::HashSet;
use std::fmt::Write;

use super::{ParsedError, ParsedTestFailure, compile_error_parser, test_failure_parser};

/// 最大修复尝试次数（默认值，非 SWE-bench 上下文；对齐旧 `MAX_ATTEMPTS_DEFAULT`）。
pub const MAX_ATTEMPTS_DEFAULT: u32 = 3;

/// SWE-bench 上下文下的最大修复尝试次数（对齐旧 `MAX_ATTEMPTS_SWE_BENCH`）。
pub const MAX_ATTEMPTS_SWE_BENCH: u32 = 7;

/// 修复指令最大 token 数（对齐旧 `MAX_INSTRUCTION_TOKENS`）。
pub const MAX_INSTRUCTION_TOKENS: u32 = 800;

/// 堆栈摘要最大行数（对齐旧 `MAX_STACK_TRACE_LINES`）。
pub const MAX_STACK_TRACE_LINES: usize = 3;

/// 修正类型（对齐旧 `CorrectionInstruction.CorrectionType`，`RuntimeError` 为预留）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorrectionType {
    /// 编译错误。
    CompileError,
    /// 测试失败。
    TestFailure,
    /// 运行时错误（本 Batch 预留，暂无解析器产出）。
    RuntimeError,
}

/// 错误上下文（对齐旧 `CorrectionInstruction.ErrorContext`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorContext {
    /// 文件名（测试失败时为测试名）。
    pub file_name: String,
    /// 行号（测试失败时为 0）。
    pub line_number: u32,
    /// 错误消息。
    pub error_message: String,
    /// 相关代码片段（当前恒为空串，对齐旧实现）。
    pub relevant_code: String,
}

/// 修复指令（对齐旧 `CorrectionInstruction` record）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrectionInstruction {
    /// 修复指令文本（≤ [`MAX_INSTRUCTION_TOKENS`]）。
    pub instruction: String,
    /// 错误上下文。
    pub error_context: ErrorContext,
    /// 修正类型。
    pub correction_type: CorrectionType,
    /// 当前尝试次数（1 起）。
    pub attempt_number: u32,
}

/// 解析最大修复尝试次数（对齐旧 `resolveMaxAttempts`）。
///
/// `repo_name` 为空白或 `"unknown"` → [`MAX_ATTEMPTS_DEFAULT`]；否则视为 SWE-bench
/// 上下文 → [`MAX_ATTEMPTS_SWE_BENCH`]。
fn resolve_max_attempts(repo_name: &str) -> u32 {
    let trimmed = repo_name.trim();
    if trimmed.is_empty() || trimmed == "unknown" {
        MAX_ATTEMPTS_DEFAULT
    } else {
        MAX_ATTEMPTS_SWE_BENCH
    }
}

/// 检测工具输出中的错误并生成修复指令（对齐旧 `detectAndPrepareCorrection`）。
///
/// 优先级：编译错误 > 测试失败。返回 `None` 表示无可修复错误或已超限。
#[must_use]
pub fn detect_and_prepare_correction(
    output: &str,
    attempts: u32,
    repo_name: &str,
) -> Option<CorrectionInstruction> {
    // 1. 超限检查（仓库感知）——同时落实 should_abort 条件 ②。
    let max_attempts = resolve_max_attempts(repo_name);
    if attempts >= max_attempts {
        return None;
    }
    if output.trim().is_empty() {
        return None;
    }

    let attempt_number = attempts + 1;

    // 2. 先尝试编译错误解析。
    if let Some(errors) = compile_error_parser::parse(output) {
        let instruction = truncate_to_token_limit(
            &generate_compile_instruction(&errors),
            MAX_INSTRUCTION_TOKENS,
        );
        let first = &errors[0];
        let ctx = ErrorContext {
            file_name: first.file_name.clone(),
            line_number: first.line_number,
            error_message: first.error_message.clone(),
            relevant_code: String::new(),
        };
        return Some(CorrectionInstruction {
            instruction,
            error_context: ctx,
            correction_type: CorrectionType::CompileError,
            attempt_number,
        });
    }

    // 3. 无编译错误则尝试测试失败解析。
    if let Some(failures) = test_failure_parser::parse(output) {
        let instruction = truncate_to_token_limit(
            &generate_test_instruction(&failures),
            MAX_INSTRUCTION_TOKENS,
        );
        let first = &failures[0];
        let error_message = match (&first.actual, &first.expected) {
            (Some(actual), expected) => format!(
                "Expected: {}, Actual: {actual}",
                expected.as_deref().unwrap_or("")
            ),
            (None, _) if !first.stack_trace.is_empty() => first.stack_trace.clone(),
            _ => "Test failed".to_owned(),
        };
        let ctx = ErrorContext {
            file_name: first.test_name.clone(),
            line_number: 0,
            error_message,
            relevant_code: String::new(),
        };
        return Some(CorrectionInstruction {
            instruction,
            error_context: ctx,
            correction_type: CorrectionType::TestFailure,
            attempt_number,
        });
    }

    // 4. 无可修复错误。
    None
}

/// 中止检查（对齐任务规格，见模块文档「有意差异」条 2）。
///
/// 满足任一即中止：① 新旧输出完全相同；③ 新错误标识集合是旧集合子集（无新信息）。
/// 条件 ② 由 [`detect_and_prepare_correction`] 的超限守卫在上游落实。
#[must_use]
pub fn should_abort(new_output: &str, prev_output: Option<&str>) -> bool {
    let Some(prev) = prev_output else {
        // 首轮无历史可比 → 不中止。
        return false;
    };
    // ① 输出与上次完全相同 → 停滞。
    if new_output == prev {
        return true;
    }
    // ③ 无新信息：新错误标识集合 ⊆ 旧集合。
    let new_ids = collect_error_identifiers(new_output);
    let prev_ids = collect_error_identifiers(prev);
    if !new_ids.is_empty() && new_ids.is_subset(&prev_ids) {
        return true;
    }
    false
}

/// 汇集工具输出中的错误标识集合（编译错误 + 测试失败）。
///
/// 编译错误标识为 `lang:file:line:msg`，测试失败标识为 `framework:test`。
fn collect_error_identifiers(output: &str) -> HashSet<String> {
    let mut ids = HashSet::new();
    if let Some(errors) = compile_error_parser::parse(output) {
        for e in &errors {
            ids.insert(compile_error_id(e));
        }
    }
    if let Some(failures) = test_failure_parser::parse(output) {
        for f in &failures {
            ids.insert(test_failure_id(f));
        }
    }
    ids
}

fn compile_error_id(e: &ParsedError) -> String {
    format!(
        "{}:{}:{}:{}",
        e.language, e.file_name, e.line_number, e.error_message
    )
}

fn test_failure_id(f: &ParsedTestFailure) -> String {
    format!("{}:{}", f.framework, f.test_name)
}

/// 生成结构化编译错误修复指令（对齐旧 `generateCompileInstruction`）。
fn generate_compile_instruction(errors: &[ParsedError]) -> String {
    let mut sb = String::from("The following compilation error(s) need to be fixed:\n\n");
    for error in errors {
        let _ = writeln!(sb, "File: {}, Line: {}", error.file_name, error.line_number);
        let _ = writeln!(sb, "Error: {}", error.error_message);
        let _ = writeln!(sb, "Language: {}\n", error.language);
    }
    sb.push_str("Please fix these errors. Do not introduce new errors.");
    sb
}

/// 生成结构化测试失败修复指令（对齐旧 `generateTestInstruction`）。
fn generate_test_instruction(failures: &[ParsedTestFailure]) -> String {
    let mut sb = String::from("The following test(s) are failing:\n\n");
    for failure in failures {
        let _ = writeln!(sb, "Test: {} ({})", failure.test_name, failure.framework);
        if let (Some(expected), Some(actual)) = (&failure.expected, &failure.actual) {
            let _ = writeln!(sb, "Expected: {expected}");
            let _ = writeln!(sb, "Actual: {actual}");
        }
        if !failure.stack_trace.trim().is_empty() {
            let _ = writeln!(sb, "Stack: {}", summarize_stack_trace(&failure.stack_trace));
        }
        sb.push('\n');
    }
    sb.push_str("Please fix the code to make these tests pass. Do not break other tests.");
    sb
}

/// 提取堆栈摘要（前 N 行，对齐旧 `summarizeStackTrace`）。
fn summarize_stack_trace(stack_trace: &str) -> String {
    let lines: Vec<&str> = stack_trace.split('\n').collect();
    if lines.len() <= MAX_STACK_TRACE_LINES {
        return stack_trace.to_owned();
    }
    let mut sb = String::new();
    for (i, line) in lines.iter().take(MAX_STACK_TRACE_LINES).enumerate() {
        if i > 0 {
            sb.push('\n');
        }
        sb.push_str(line);
    }
    let _ = write!(
        sb,
        "\n... ({} more lines)",
        lines.len() - MAX_STACK_TRACE_LINES
    );
    sb
}

/// 按 token 限制截断（对齐旧 `truncateToTokenLimit`，0.9 安全系数）。
///
/// 用 [`crate::context::token_counter::count_text`]（旧 `TokenCounter.estimateTokens`
/// 等价式，模型名留空走内容类型检测）估算；超限则按比例 * 0.9 截断（按字符边界，
/// 避免切断 UTF-8）并附截断标记。
fn truncate_to_token_limit(text: &str, max_tokens: u32) -> String {
    let estimated = crate::context::token_counter::count_text(text, "");
    if estimated <= max_tokens {
        return text.to_owned();
    }
    let ratio = f64::from(max_tokens) / f64::from(estimated);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let char_limit = (text.chars().count() as f64 * ratio * 0.9) as usize;
    let truncated: String = text.chars().take(char_limit).collect();
    format!("{truncated}\n... [truncated due to token limit]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_compile_error_first() {
        let out = "src/Main.java:42: error: cannot find symbol";
        let instr = detect_and_prepare_correction(out, 0, "").expect("compile correction");
        assert_eq!(instr.correction_type, CorrectionType::CompileError);
        assert_eq!(instr.attempt_number, 1);
        assert!(instr.instruction.contains("compilation error"));
        assert_eq!(instr.error_context.file_name, "src/Main.java");
        assert_eq!(instr.error_context.line_number, 42);
    }

    #[test]
    fn detects_test_failure_when_no_compile_error() {
        let out = "FAIL src/app.test.ts\n● suite > case\nExpected: 1\nReceived: 2";
        let instr = detect_and_prepare_correction(out, 0, "").expect("test correction");
        assert_eq!(instr.correction_type, CorrectionType::TestFailure);
        assert!(instr.instruction.contains("test(s) are failing"));
    }

    #[test]
    fn returns_none_when_attempts_exhausted_default() {
        let out = "src/Main.java:1: error: boom";
        // 默认上限 3，attempts=3 → None。
        assert!(detect_and_prepare_correction(out, MAX_ATTEMPTS_DEFAULT, "").is_none());
        // SWE-bench 上下文放宽到 7，attempts=3 仍可修复。
        assert!(
            detect_and_prepare_correction(out, MAX_ATTEMPTS_DEFAULT, "django/django").is_some()
        );
    }

    #[test]
    fn returns_none_for_blank_or_clean_output() {
        assert!(detect_and_prepare_correction("   ", 0, "").is_none());
        assert!(detect_and_prepare_correction("build succeeded", 0, "").is_none());
    }

    #[test]
    fn should_abort_on_identical_output() {
        let out = "src/Main.java:1: error: boom";
        assert!(should_abort(out, Some(out)));
    }

    #[test]
    fn should_abort_when_no_new_info_subset() {
        let prev = "src/A.java:1: error: e1\nsrc/B.java:2: error: e2";
        // 新输出错误标识是旧的子集（仅剩 A）→ 无新信息 → 中止。
        let new = "src/A.java:1: error: e1";
        assert!(should_abort(new, Some(prev)));
    }

    #[test]
    fn should_not_abort_on_new_distinct_error() {
        let prev = "src/A.java:1: error: e1";
        // 出现旧集合中不存在的新错误 → 有新信息 → 继续。
        let new = "src/C.java:9: error: brand-new";
        assert!(!should_abort(new, Some(prev)));
    }

    #[test]
    fn should_not_abort_first_attempt() {
        assert!(!should_abort("src/A.java:1: error: e1", None));
    }

    #[test]
    fn truncates_long_instruction() {
        // 解析器封顶 MAX_ERRORS=5，故需靠单条超长错误消息把指令推过 800 token。
        let long_msg = "x".repeat(4000);
        let mut out = String::new();
        for i in 0..5 {
            let _ = writeln!(out, "src/File{i}.java:{i}: error: {long_msg}");
        }
        let instr = detect_and_prepare_correction(&out, 0, "").expect("correction for huge output");
        assert!(
            instr
                .instruction
                .ends_with("[truncated due to token limit]")
        );
    }

    #[test]
    fn summarize_stack_trace_caps_lines() {
        let stack = "l1\nl2\nl3\nl4\nl5";
        let summary = summarize_stack_trace(stack);
        assert!(summary.contains("l1"));
        assert!(summary.contains("(2 more lines)"));
    }
}
