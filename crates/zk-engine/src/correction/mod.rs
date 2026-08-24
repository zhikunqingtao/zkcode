//! 自修正循环——编译错误 / 测试失败解析器 + 主控逻辑。
//!
//! 对齐旧 `engine/correction/` 包：`CompileErrorParser`、`TestFailureParser`、
//! `SelfCorrectionLoop`、`CorrectionInstruction`（含 `ParsedError` /
//! `ParsedTestFailure` / `ErrorContext` / `CorrectionType`）。
//!
//! 消费方为引擎多轮循环（[`crate::engine`]）：Feature Flag `SELF_CORRECTION_LOOP`
//! 开启时，对 `Bash` 工具结果调用 [`loop_ctrl::detect_and_prepare_correction`]
//! 检测可修复错误并注入修复指令。
//!
//! # 子模块
//!
//! - [`compile_error_parser`]：Java / TypeScript / Python / **Rust** 编译错误；
//! - [`test_failure_parser`]：`JUnit` / Jest / pytest 测试失败；
//! - [`loop_ctrl`]：主控（检测优先级、超限、token 截断、中止判定）。

pub mod compile_error_parser;
pub mod loop_ctrl;
pub mod test_failure_parser;

/// 解析后的编译错误——对齐旧 `CorrectionInstruction.ParsedError`（record）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedError {
    /// 文件名。
    pub file_name: String,
    /// 行号。
    pub line_number: u32,
    /// 错误消息。
    pub error_message: String,
    /// 语言标识（`"java"` / `"typescript"` / `"python"` / `"rust"`）。
    pub language: String,
}

/// 解析后的测试失败——对齐旧 `CorrectionInstruction.ParsedTestFailure`（record）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedTestFailure {
    /// 测试名（pytest 为 `file::test`）。
    pub test_name: String,
    /// 期望值（无则 `None`）。
    pub expected: Option<String>,
    /// 实际值（无则 `None`）。
    pub actual: Option<String>,
    /// 堆栈摘要（可空串）。
    pub stack_trace: String,
    /// 框架标识（`"junit"` / `"jest"` / `"pytest"`）。
    pub framework: String,
}
