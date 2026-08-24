//! 检查种类 / 状态 / 结果与报告渲染——`VerifyJourney` 的纯数据层。
//!
//! 语义来源（旧仓库只读）：`tool/verify/VerifyJourneyTool.java`（744L）的
//! 「多步验证 + 逐步证据 + 汇总判定」骨架，以及其 `categorizeError` 的
//! 「失败按稳定分类码归一」形制。
//!
//! # 有意差异（对照旧实现）
//!
//! 旧 `VerifyJourneyTool` 的验证步骤是**运行时浏览器旅程**（拉起 dev server →
//! `BrowserVerifier` / `HttpApiVerifier` 逐步骤断言 → 证据包落盘）。本迁移按
//! Batch 8F 判据把「旅程」重定义为**工程校验流水线**（编译 / 测试 / lint /
//! 类型检查 / 格式 / 构建 / 自定义），不引入无头浏览器依赖；保留旧实现的
//! 三件形制：分组归属 `verify`、逐步骤证据（命令 + 退出码 + 输出）、汇总
//! 状态（全通过才通过）。

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use std::fmt::Write as _;

/// 单步检查种类（任务判据 7 类）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CheckKind {
    /// 编译检查（Rust `cargo check` / Node 构建脚本 / Python `compileall`）。
    Compile,
    /// 测试。
    Test,
    /// 静态检查（clippy / eslint / ruff）。
    Lint,
    /// 类型检查（`tsc --noEmit` / mypy）。
    TypeCheck,
    /// 格式检查（`--check` 只读模式，不改写工作树）。
    Format,
    /// 生产构建。
    Build,
    /// 自定义命令（必须显式给出 `command`）。
    Custom,
}

impl CheckKind {
    /// 全部种类（供 JSON Schema 枚举与测试覆盖）。
    pub const ALL: [Self; 7] = [
        Self::Compile,
        Self::Test,
        Self::Lint,
        Self::TypeCheck,
        Self::Format,
        Self::Build,
        Self::Custom,
    ];

    /// 线格式名（JSON 报告与入参共用的稳定字符串）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compile => "compile",
            Self::Test => "test",
            Self::Lint => "lint",
            Self::TypeCheck => "typecheck",
            Self::Format => "format",
            Self::Build => "build",
            Self::Custom => "custom",
        }
    }

    /// 解析入参种类名（大小写不敏感，兼容 `type_check` / `type-check` 写法）。
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let normalized = raw.trim().to_ascii_lowercase().replace(['-', '_'], "");
        match normalized.as_str() {
            "compile" | "check" => Some(Self::Compile),
            "test" | "tests" => Some(Self::Test),
            "lint" | "clippy" | "eslint" => Some(Self::Lint),
            "typecheck" | "types" | "tsc" | "mypy" => Some(Self::TypeCheck),
            "format" | "fmt" => Some(Self::Format),
            "build" => Some(Self::Build),
            "custom" | "command" => Some(Self::Custom),
            _ => None,
        }
    }
}

/// 单步终态（任务判据 `Pass` / `Fail` / `Skip` 三值）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckStatus {
    /// 通过（退出码 0）。
    Pass,
    /// 失败（非零退出码 / 超时 / 无法启动）。
    Fail,
    /// 跳过（无可用默认命令 / `fail_fast` 提前终止 / 预算耗尽 / 已取消）。
    Skip,
}

impl CheckStatus {
    /// 线格式名。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skip => "skip",
        }
    }

    /// Markdown 摘要用记号。
    #[must_use]
    const fn glyph(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Skip => "SKIP",
        }
    }
}

/// 项目类型探测结果——决定各种类的默认命令。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectKind {
    /// 存在 `Cargo.toml`。
    Rust,
    /// 存在 `package.json`。
    Node,
    /// 存在 `pyproject.toml` / `setup.py` / `requirements.txt`。
    Python,
    /// 未识别——除 `Custom` 外全部跳过（不猜命令）。
    Unknown,
}

impl ProjectKind {
    /// 线格式名。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Node => "node",
            Self::Python => "python",
            Self::Unknown => "unknown",
        }
    }

    /// 按标志文件探测项目类型（优先序 Rust → Node → Python）。
    ///
    /// 只看给定目录本层，不向上回溯：工具入参 `working_dir` 即调用方声明的
    /// 验证根，向上探测会把校验命令跑到目标之外。
    #[must_use]
    pub fn detect(dir: &Path) -> Self {
        if dir.join("Cargo.toml").is_file() {
            return Self::Rust;
        }
        if dir.join("package.json").is_file() {
            return Self::Node;
        }
        if dir.join("pyproject.toml").is_file()
            || dir.join("setup.py").is_file()
            || dir.join("requirements.txt").is_file()
        {
            return Self::Python;
        }
        Self::Unknown
    }
}

/// 按（种类, 项目类型）解析默认命令；`None` 表示该组合无默认命令。
///
/// Rust 的 `TypeCheck` 恒 `None`——`cargo check` 即类型检查，重复执行只是
/// 白烧一次编译预算（该步会以 `Skip` + 理由入报告，而非静默通过）。
#[must_use]
pub fn default_command(kind: CheckKind, project: ProjectKind) -> Option<&'static str> {
    match (kind, project) {
        (CheckKind::Compile, ProjectKind::Rust) => Some("cargo check --workspace --all-targets"),
        (CheckKind::Test, ProjectKind::Rust) => Some("cargo test --workspace"),
        (CheckKind::Lint, ProjectKind::Rust) => {
            Some("cargo clippy --workspace --all-targets -- -D warnings")
        }
        (CheckKind::Format, ProjectKind::Rust) => Some("cargo fmt --all -- --check"),
        (CheckKind::Build, ProjectKind::Rust) => Some("cargo build --workspace --release"),

        (CheckKind::Compile | CheckKind::Build, ProjectKind::Node) => {
            Some("npm run build --if-present")
        }
        (CheckKind::Test, ProjectKind::Node) => Some("npm test --if-present"),
        (CheckKind::Lint, ProjectKind::Node) => Some("npx --no-install eslint ."),
        (CheckKind::TypeCheck, ProjectKind::Node) => Some("npx --no-install tsc --noEmit"),
        (CheckKind::Format, ProjectKind::Node) => Some("npx --no-install prettier --check ."),

        (CheckKind::Compile, ProjectKind::Python) => Some("python3 -m compileall -q ."),
        (CheckKind::Test, ProjectKind::Python) => Some("python3 -m pytest -q"),
        (CheckKind::Lint, ProjectKind::Python) => Some("python3 -m ruff check ."),
        (CheckKind::TypeCheck, ProjectKind::Python) => Some("python3 -m mypy ."),
        (CheckKind::Format, ProjectKind::Python) => Some("python3 -m ruff format --check ."),
        (CheckKind::Build, ProjectKind::Python) => Some("python3 -m build"),

        // Rust 的 TypeCheck、任意种类 × Unknown、以及 Custom：无默认命令。
        _ => None,
    }
}

/// 一步检查的执行计划（种类 + 命令 + 超时）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckPlan {
    /// 种类。
    pub kind: CheckKind,
    /// 待执行命令；`None` 表示按项目类型解析默认命令。
    pub command: Option<String>,
    /// 本步超时。
    pub timeout: Duration,
}

/// 一步检查的结果（任务判据 `{ kind, status, output, duration_ms }` 的超集：
/// 追加命令 / 退出码 / 超时 / 截断 / 跳过理由，保证证据可复现）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckResult {
    /// 种类。
    pub kind: CheckKind,
    /// 终态。
    pub status: CheckStatus,
    /// 实际执行的命令（跳过时为 `None`）。
    pub command: Option<String>,
    /// 合并输出（stdout + stderr，按上限截断）。
    pub output: String,
    /// 退出码（未执行时 `None`）。
    pub exit_code: Option<i32>,
    /// 本步耗时（毫秒）。
    pub duration_ms: u64,
    /// 是否因超时被终止。
    pub timed_out: bool,
    /// 输出是否被截断。
    pub truncated: bool,
    /// 跳过理由（仅 `Skip` 时有值）。
    pub skip_reason: Option<String>,
}

impl CheckResult {
    /// 构造跳过结果。
    #[must_use]
    pub fn skipped(kind: CheckKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            status: CheckStatus::Skip,
            command: None,
            output: String::new(),
            exit_code: None,
            duration_ms: 0,
            timed_out: false,
            truncated: false,
            skip_reason: Some(reason.into()),
        }
    }

    /// 导出 JSON（camelCase，对齐 REST 层既有响应风格）。
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "kind": self.kind.as_str(),
            "status": self.status.as_str(),
            "command": self.command,
            "output": self.output,
            "exitCode": self.exit_code,
            "durationMs": self.duration_ms,
            "timedOut": self.timed_out,
            "truncated": self.truncated,
            "skipReason": self.skip_reason,
        })
    }
}

/// 一次流水线的完整报告。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JourneyReport {
    /// 验证根目录。
    pub working_dir: String,
    /// 探测到的项目类型。
    pub project: ProjectKind,
    /// 是否启用「一步失败即终止」。
    pub fail_fast: bool,
    /// 逐步结果（顺序即执行序）。
    pub results: Vec<CheckResult>,
    /// 流水线总耗时（毫秒）。
    pub total_duration_ms: u64,
}

impl JourneyReport {
    /// 通过计数。
    #[must_use]
    pub fn passed(&self) -> usize {
        self.count(CheckStatus::Pass)
    }

    /// 失败计数。
    #[must_use]
    pub fn failed(&self) -> usize {
        self.count(CheckStatus::Fail)
    }

    /// 跳过计数。
    #[must_use]
    pub fn skipped(&self) -> usize {
        self.count(CheckStatus::Skip)
    }

    fn count(&self, status: CheckStatus) -> usize {
        self.results
            .iter()
            .filter(|result| result.status == status)
            .count()
    }

    /// 汇总状态：有失败即 `Fail`；无失败且有通过即 `Pass`；全跳过即 `Skip`。
    #[must_use]
    pub fn status(&self) -> CheckStatus {
        if self.failed() > 0 {
            CheckStatus::Fail
        } else if self.passed() > 0 {
            CheckStatus::Pass
        } else {
            CheckStatus::Skip
        }
    }

    /// 导出 JSON 报告。
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "status": self.status().as_str(),
            "workingDir": self.working_dir,
            "projectKind": self.project.as_str(),
            "failFast": self.fail_fast,
            "totalDurationMs": self.total_duration_ms,
            "passed": self.passed(),
            "failed": self.failed(),
            "skipped": self.skipped(),
            "checks": self.results.iter().map(CheckResult::to_json).collect::<Vec<_>>(),
        })
    }

    /// 渲染 Markdown 摘要（汇总行 + 步骤表格 + 失败步骤输出块）。
    #[must_use]
    pub fn to_markdown(&self) -> String {
        let status = self.status();
        let mut out = format!(
            "## Verify Journey: {}\n\n{} pass / {} fail / {} skip，共 {} 步，\
             耗时 {} ms（project={}, fail_fast={}）\n\n",
            status.glyph(),
            self.passed(),
            self.failed(),
            self.skipped(),
            self.results.len(),
            self.total_duration_ms,
            self.project.as_str(),
            self.fail_fast,
        );
        out.push_str("| # | Check | Status | Duration | Exit | Command |\n");
        out.push_str("|---|---|---|---|---|---|\n");
        for (index, result) in self.results.iter().enumerate() {
            let detail = result.command.as_deref().map_or_else(
                || escape_cell(result.skip_reason.as_deref().unwrap_or("-")),
                |command| format!("`{}`", escape_cell(command)),
            );
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} ms | {} | {} |",
                index + 1,
                result.kind.as_str(),
                result.status.glyph(),
                result.duration_ms,
                result
                    .exit_code
                    .map_or_else(|| "-".to_owned(), |code| code.to_string()),
                detail,
            );
        }
        for result in self
            .results
            .iter()
            .filter(|result| result.status == CheckStatus::Fail)
        {
            let body = if result.output.trim().is_empty() {
                "(no output)"
            } else {
                result.output.trim_end()
            };
            let _ = write!(
                out,
                "\n### FAIL {}{}\n\n```\n{}\n```\n",
                result.kind.as_str(),
                if result.timed_out { "（超时）" } else { "" },
                body,
            );
        }
        out
    }
}

/// 转义 Markdown 表格单元格中的竖线与换行。
fn escape_cell(raw: &str) -> String {
    raw.replace('|', "\\|").replace('\n', " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_round_trips_wire_names() {
        for kind in CheckKind::ALL {
            assert_eq!(CheckKind::parse(kind.as_str()), Some(kind));
        }
    }

    #[test]
    fn kind_parse_is_lenient() {
        assert_eq!(CheckKind::parse("  TypeCheck "), Some(CheckKind::TypeCheck));
        assert_eq!(CheckKind::parse("type_check"), Some(CheckKind::TypeCheck));
        assert_eq!(CheckKind::parse("type-check"), Some(CheckKind::TypeCheck));
        assert_eq!(CheckKind::parse("FMT"), Some(CheckKind::Format));
        assert_eq!(CheckKind::parse("deploy"), None);
    }

    #[test]
    fn detect_prefers_cargo_then_package_json() {
        let dir = std::env::temp_dir().join(format!("zk-vj-detect-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        assert_eq!(ProjectKind::detect(&dir), ProjectKind::Unknown);
        std::fs::write(dir.join("requirements.txt"), "").expect("write");
        assert_eq!(ProjectKind::detect(&dir), ProjectKind::Python);
        std::fs::write(dir.join("package.json"), "{}").expect("write");
        assert_eq!(ProjectKind::detect(&dir), ProjectKind::Node);
        std::fs::write(dir.join("Cargo.toml"), "").expect("write");
        assert_eq!(ProjectKind::detect(&dir), ProjectKind::Rust);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn rust_type_check_and_custom_have_no_default_command() {
        assert!(default_command(CheckKind::TypeCheck, ProjectKind::Rust).is_none());
        for project in [
            ProjectKind::Rust,
            ProjectKind::Node,
            ProjectKind::Python,
            ProjectKind::Unknown,
        ] {
            assert!(default_command(CheckKind::Custom, project).is_none());
        }
    }

    #[test]
    fn unknown_project_has_no_default_commands() {
        for kind in CheckKind::ALL {
            assert!(default_command(kind, ProjectKind::Unknown).is_none());
        }
    }

    #[test]
    fn format_defaults_never_rewrite_the_worktree() {
        for project in [ProjectKind::Rust, ProjectKind::Node, ProjectKind::Python] {
            let command = default_command(CheckKind::Format, project).expect("has default");
            assert!(
                command.contains("--check"),
                "format 步必须只读校验: {command}"
            );
        }
    }

    fn result(kind: CheckKind, status: CheckStatus) -> CheckResult {
        CheckResult {
            kind,
            status,
            command: Some("echo hi".to_owned()),
            output: "hi".to_owned(),
            exit_code: Some(i32::from(status != CheckStatus::Pass)),
            duration_ms: 7,
            timed_out: false,
            truncated: false,
            skip_reason: None,
        }
    }

    fn report(results: Vec<CheckResult>) -> JourneyReport {
        JourneyReport {
            working_dir: "/tmp/ws".to_owned(),
            project: ProjectKind::Rust,
            fail_fast: true,
            results,
            total_duration_ms: 21,
        }
    }

    #[test]
    fn status_is_fail_when_any_step_fails() {
        let report = report(vec![
            result(CheckKind::Compile, CheckStatus::Pass),
            result(CheckKind::Test, CheckStatus::Fail),
        ]);
        assert_eq!(report.status(), CheckStatus::Fail);
        assert_eq!(report.passed(), 1);
        assert_eq!(report.failed(), 1);
        assert_eq!(report.skipped(), 0);
    }

    #[test]
    fn status_is_skip_when_nothing_ran() {
        let report = report(vec![CheckResult::skipped(CheckKind::Test, "no toolchain")]);
        assert_eq!(report.status(), CheckStatus::Skip);
        assert_eq!(report.skipped(), 1);
    }

    #[test]
    fn json_report_exposes_camel_case_keys() {
        let body = report(vec![result(CheckKind::Compile, CheckStatus::Pass)]).to_json();
        assert_eq!(body["status"], "pass");
        assert_eq!(body["projectKind"], "rust");
        assert_eq!(body["failFast"], true);
        assert_eq!(body["totalDurationMs"], 21);
        assert_eq!(body["checks"][0]["kind"], "compile");
        assert_eq!(body["checks"][0]["exitCode"], 0);
        assert_eq!(body["checks"][0]["durationMs"], 7);
    }

    #[test]
    fn markdown_contains_table_and_failure_block() {
        let markdown = report(vec![
            result(CheckKind::Compile, CheckStatus::Pass),
            result(CheckKind::Lint, CheckStatus::Fail),
        ])
        .to_markdown();
        assert!(markdown.contains("| # | Check | Status | Duration | Exit | Command |"));
        assert!(markdown.contains("| 1 | compile | PASS |"));
        assert!(markdown.contains("### FAIL lint"));
    }

    #[test]
    fn markdown_escapes_pipe_in_command() {
        let mut failing = result(CheckKind::Custom, CheckStatus::Fail);
        failing.command = Some("echo a | wc -l".to_owned());
        let markdown = report(vec![failing]).to_markdown();
        assert!(markdown.contains("echo a \\| wc -l"));
    }

    #[test]
    fn skip_result_has_no_command_and_keeps_reason() {
        let skipped = CheckResult::skipped(CheckKind::Build, "budget exhausted");
        assert_eq!(skipped.command, None);
        assert_eq!(skipped.duration_ms, 0);
        let body = skipped.to_json();
        assert_eq!(body["status"], "skip");
        assert_eq!(body["skipReason"], "budget exhausted");
        assert!(body["exitCode"].is_null());
    }
}
