//! `VerifyJourney` 工具本体——入参解析 + 流水线执行 + 报告装配。
//!
//! 语义来源（旧仓库只读）：`tool/verify/VerifyJourneyTool.java`（744L）。
//! 沿用旧实现的四件形制：
//!
//! 1. 工具超时取硬上限（旧 `VERIFY_TIMEOUT_MS = 600_000`，本实现取
//!    [`MAX_TOOL_TIMEOUT`] 同值 10 分钟）；
//! 2. 非只读、非并发安全（旧 `isReadOnly=false` / `isConcurrencySafe=false`）；
//! 3. 逐步骤证据（命令 + 退出码 + 输出）随结构化元数据回传；
//! 4. 汇总判定「有失败即失败」。
//!
//! # 有意差异
//!
//! - 步骤语义由「浏览器旅程」改为「工程校验流水线」（见 [`super::checks`]
//!   模块文档的差异留痕）；
//! - 旧实现把证据包写进 `.aicode/verify/`，本工具**不落盘**：证据经
//!   `structuredResult` 上行，落盘归 `zk-server` 的 evidence 端点，避免工具层
//!   与存储层耦合。

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde_json::{Value, json};

use super::checks::{
    CheckKind, CheckPlan, CheckResult, CheckStatus, JourneyReport, ProjectKind, default_command,
};
use crate::input::{RESULTS_TRUNCATED, failure, truncate_chars};
use crate::process;
use crate::tool::{MAX_TOOL_TIMEOUT, Tool, ToolContext, ToolOutput};

/// 单步默认超时（5 分钟）。
pub const CHECK_TIMEOUT_DEFAULT: Duration = Duration::from_mins(5);

/// 单步超时上限（10 分钟，与 [`MAX_TOOL_TIMEOUT`] 同值）。
pub const CHECK_TIMEOUT_MAX: Duration = MAX_TOOL_TIMEOUT;

/// 单步超时下限（1 秒——低于此值的入参按 1 秒钳制，不接受 0）。
pub const CHECK_TIMEOUT_MIN: Duration = Duration::from_secs(1);

/// 流水线总预算（9 分钟，为执行器 10 分钟硬超时留 1 分钟收尾余量：
/// 被执行器超时砍掉时报告整体丢失，主动收尾能保住已完成步骤的证据）。
pub const PIPELINE_BUDGET: Duration = Duration::from_mins(9);

/// 步骤最小可用预算（剩余不足此值时后续步骤直接跳过）。
const MIN_STEP_BUDGET: Duration = Duration::from_secs(2);

/// 单步输出保留上限（字符）。
const MAX_CHECK_OUTPUT_CHARS: usize = 4_000;

/// 单次流水线步骤数上限。
pub const MAX_CHECKS: usize = 12;

/// 入参解析失败（错误码 + 文案；工具层转 [`ToolOutput`]，REST 层转 400）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestError {
    /// 稳定错误码。
    pub code: String,
    /// 面向调用方的文案。
    pub message: String,
}

impl RequestError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
        }
    }
}

/// 一次流水线请求（解析后的规范形态）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JourneyRequest {
    /// 逐步计划（顺序即执行序）。
    pub plans: Vec<CheckPlan>,
    /// 验证根目录（已校验存在且为目录）。
    pub working_dir: PathBuf,
    /// 一步失败即终止后续（缺省 `true`）。
    pub fail_fast: bool,
}

/// 解析入参为规范请求（工具层与 REST 层共用的唯一解析口）。
///
/// `checks` 元素支持两种写法：字符串（种类名）或对象
/// （`{ "kind": …, "command": …, "timeout_ms": … }`）。`Custom` 种类必须显式给出
/// `command`。`working_dir` 缺省取 `default_dir`，相对路径以其为基准解析。
///
/// # Errors
/// `checks` 缺失 / 非数组 / 空 / 超上限、种类不可识别、`Custom` 缺命令、
/// `working_dir` 不存在或非目录时返回 [`RequestError`]。
pub fn parse_request(input: &Value, default_dir: &Path) -> Result<JourneyRequest, RequestError> {
    let raw_checks = input
        .get("checks")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            RequestError::new(
                "VERIFY_CHECKS_MISSING",
                "Required parameter 'checks' is missing or not an array",
            )
        })?;
    if raw_checks.is_empty() {
        return Err(RequestError::new(
            "VERIFY_CHECKS_MISSING",
            "Parameter 'checks' must contain at least one check",
        ));
    }
    if raw_checks.len() > MAX_CHECKS {
        return Err(RequestError::new(
            "VERIFY_CHECKS_LIMIT_EXCEEDED",
            format!(
                "Parameter 'checks' accepts at most {MAX_CHECKS} entries, got {}",
                raw_checks.len()
            ),
        ));
    }

    let mut plans = Vec::with_capacity(raw_checks.len());
    for entry in raw_checks {
        plans.push(parse_plan(entry)?);
    }

    let working_dir = match input.get("working_dir").and_then(Value::as_str) {
        Some(raw) if !raw.trim().is_empty() => {
            let candidate = PathBuf::from(raw.trim());
            if candidate.is_absolute() {
                candidate
            } else {
                default_dir.join(candidate)
            }
        }
        _ => default_dir.to_path_buf(),
    };
    if !working_dir.is_dir() {
        return Err(RequestError::new(
            "VERIFY_WORKING_DIR_INVALID",
            format!(
                "Working directory does not exist or is not a directory: {}",
                working_dir.display()
            ),
        ));
    }

    Ok(JourneyRequest {
        plans,
        working_dir,
        fail_fast: input
            .get("fail_fast")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    })
}

/// 解析单个 `checks` 元素。
fn parse_plan(entry: &Value) -> Result<CheckPlan, RequestError> {
    let (raw_kind, command, timeout_ms) = match entry {
        Value::String(name) => (name.as_str(), None, None),
        Value::Object(_) => {
            let raw_kind = entry
                .get("kind")
                .or_else(|| entry.get("type"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    RequestError::new(
                        "VERIFY_CHECK_INVALID",
                        "Each check object requires a string 'kind'",
                    )
                })?;
            let command = entry
                .get("command")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            (
                raw_kind,
                command,
                entry.get("timeout_ms").and_then(Value::as_u64),
            )
        }
        _ => {
            return Err(RequestError::new(
                "VERIFY_CHECK_INVALID",
                "Each check must be a kind string or an object",
            ));
        }
    };

    let kind = CheckKind::parse(raw_kind).ok_or_else(|| {
        RequestError::new(
            "VERIFY_CHECK_KIND_UNSUPPORTED",
            format!(
                "Unknown check kind '{raw_kind}'; expected one of: {}",
                CheckKind::ALL
                    .iter()
                    .map(|kind| kind.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
    })?;
    if kind == CheckKind::Custom && command.is_none() {
        return Err(RequestError::new(
            "VERIFY_CUSTOM_COMMAND_MISSING",
            "Check kind 'custom' requires a non-empty 'command'",
        ));
    }

    Ok(CheckPlan {
        kind,
        command,
        timeout: timeout_ms.map_or(CHECK_TIMEOUT_DEFAULT, |millis| {
            Duration::from_millis(millis).clamp(CHECK_TIMEOUT_MIN, CHECK_TIMEOUT_MAX)
        }),
    })
}

/// 按序执行流水线并汇总报告（工具层与 REST 层共用的唯一执行口）。
///
/// 三条终止路径：`fail_fast` 命中失败、取消令牌触发、总预算耗尽——均把剩余
/// 步骤记为 `Skip` + 理由，不静默丢步。
pub async fn run_journey(request: &JourneyRequest, ctx: &ToolContext) -> JourneyReport {
    let started = Instant::now();
    let project = ProjectKind::detect(&request.working_dir);
    let mut results: Vec<CheckResult> = Vec::with_capacity(request.plans.len());
    let mut aborted: Option<String> = None;

    for plan in &request.plans {
        if let Some(reason) = aborted.as_deref() {
            results.push(CheckResult::skipped(plan.kind, reason));
            continue;
        }
        if ctx.cancel.is_cancelled() {
            aborted = Some("cancelled".to_owned());
            results.push(CheckResult::skipped(plan.kind, "cancelled"));
            continue;
        }
        let remaining = PIPELINE_BUDGET.saturating_sub(started.elapsed());
        if remaining < MIN_STEP_BUDGET {
            aborted = Some("pipeline time budget exhausted".to_owned());
            results.push(CheckResult::skipped(
                plan.kind,
                "pipeline time budget exhausted",
            ));
            continue;
        }

        let Some(command) = plan
            .command
            .clone()
            .or_else(|| default_command(plan.kind, project).map(str::to_owned))
        else {
            // 无默认命令不算失败（不触发 fail_fast）：调用方要么换项目根，
            // 要么显式给 command，报告里带理由即可自证。
            results.push(CheckResult::skipped(
                plan.kind,
                format!(
                    "no default command for kind '{}' on '{}' project",
                    plan.kind.as_str(),
                    project.as_str()
                ),
            ));
            continue;
        };

        let timeout = plan.timeout.min(remaining);
        let result = execute_check(plan.kind, &command, timeout, &request.working_dir, ctx).await;
        let failed = result.status == CheckStatus::Fail;
        results.push(result);
        if failed && request.fail_fast {
            aborted = Some("skipped by fail_fast".to_owned());
        }
    }

    JourneyReport {
        working_dir: request.working_dir.display().to_string(),
        project,
        fail_fast: request.fail_fast,
        results,
        total_duration_ms: millis(started.elapsed()),
    }
}

/// 执行单步检查。
async fn execute_check(
    kind: CheckKind,
    command: &str,
    timeout: Duration,
    working_dir: &Path,
    ctx: &ToolContext,
) -> CheckResult {
    ctx.report_progress(format!("[verify] {}: {command}\n", kind.as_str()));
    let started = Instant::now();
    let outcome = process::run_shell(command, working_dir, timeout, ctx).await;
    let duration_ms = millis(started.elapsed());

    match outcome {
        Ok(outcome) => {
            let merged = if outcome.stderr.trim().is_empty() {
                outcome.stdout.clone()
            } else {
                format!("{}{}", outcome.stdout, outcome.stderr)
            };
            let (output, clipped) = truncate_chars(merged, MAX_CHECK_OUTPUT_CHARS);
            let output = if clipped {
                format!("{output}{RESULTS_TRUNCATED}")
            } else {
                output
            };
            let (status, skip_reason) = if outcome.cancelled {
                (CheckStatus::Skip, Some("cancelled".to_owned()))
            } else if outcome.exit_code == 0 && !outcome.timed_out {
                (CheckStatus::Pass, None)
            } else {
                (CheckStatus::Fail, None)
            };
            tracing::debug!(
                kind = kind.as_str(),
                status = status.as_str(),
                exit_code = outcome.exit_code,
                duration_ms,
                "verify journey check finished"
            );
            CheckResult {
                kind,
                status,
                command: Some(command.to_owned()),
                output,
                exit_code: Some(outcome.exit_code),
                duration_ms,
                timed_out: outcome.timed_out,
                truncated: outcome.truncated || clipped,
                skip_reason,
            }
        }
        // spawn 失败（bash 缺失 / 工作目录消失）：记失败并带稳定错误码，
        // 由 fail_fast 决定是否继续。
        Err(err) => CheckResult {
            kind,
            status: CheckStatus::Fail,
            command: Some(command.to_owned()),
            output: format!("VERIFY_SPAWN_FAILED: {err}"),
            exit_code: None,
            duration_ms,
            timed_out: false,
            truncated: false,
            skip_reason: None,
        },
    }
}

/// 毫秒换算（`u128` → `u64` 饱和，避免 `as` 截断）。
fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Internal engineering-check runner; browser journeys are implemented by the server bridge.
#[derive(Clone, Copy, Debug, Default)]
pub struct EngineeringCheckRunnerTool;

impl Tool for EngineeringCheckRunnerTool {
    fn name(&self) -> &'static str {
        "EngineeringCheckRunner"
    }

    fn description(&self) -> &'static str {
        "按序执行端到端验证流水线（编译/测试/lint/类型检查/格式/构建/自定义），\
         返回逐步证据与汇总判定"
    }

    fn parameters(&self) -> Value {
        let kinds: Vec<&str> = CheckKind::ALL.iter().map(|kind| kind.as_str()).collect();
        json!({
            "type": "object",
            "required": ["checks"],
            "properties": {
                "checks": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": MAX_CHECKS,
                    "description": "按序执行的检查列表；元素可为种类名字符串，或 \
                                    {kind, command, timeout_ms} 对象（custom 必须给 command）",
                    "items": {
                        "oneOf": [
                            { "type": "string", "enum": kinds.clone() },
                            {
                                "type": "object",
                                "required": ["kind"],
                                "properties": {
                                    "kind": { "type": "string", "enum": kinds },
                                    "command": {
                                        "type": "string",
                                        "description": "覆盖该步默认命令（custom 必填）"
                                    },
                                    "timeout_ms": {
                                        "type": "integer",
                                        "description": "该步超时毫秒（1s~10min 之间钳制）"
                                    }
                                }
                            }
                        ]
                    }
                },
                "working_dir": {
                    "type": "string",
                    "description": "验证根目录（缺省取会话工作目录；相对路径以其为基准）"
                },
                "fail_fast": {
                    "type": "boolean",
                    "description": "一步失败即跳过后续（默认 true）"
                }
            }
        })
    }

    fn timeout(&self) -> Duration {
        MAX_TOOL_TIMEOUT
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let request = match parse_request(&input, ctx.working_dir()) {
                Ok(request) => request,
                Err(err) => return failure(&err.code, err.message),
            };
            let report = run_journey(&request, &ctx).await;
            let is_error = report.status() == CheckStatus::Fail;
            let content = report.to_markdown();
            let metadata = Some(json!({ "structuredResult": report.to_json() }));
            ToolOutput {
                content,
                is_error,
                metadata,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    static SEQ: AtomicU32 = AtomicU32::new(0);

    /// 独占临时目录（同进程内并行测试互不干扰）。
    fn temp_dir(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("zk-vj-{tag}-{}-{seq}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn ctx(working_dir: &Path, cancel: CancellationToken) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(cancel, tx).with_working_dir(working_dir)
    }

    #[test]
    fn parse_rejects_missing_or_empty_checks() {
        let dir = temp_dir("missing");
        for input in [json!({}), json!({ "checks": [] }), json!({ "checks": 3 })] {
            let err = parse_request(&input, &dir).expect_err("rejected");
            assert_eq!(err.code, "VERIFY_CHECKS_MISSING");
        }
    }

    #[test]
    fn parse_rejects_too_many_checks() {
        let dir = temp_dir("limit");
        let checks: Vec<Value> = (0..=MAX_CHECKS).map(|_| json!("test")).collect();
        let err = parse_request(&json!({ "checks": checks }), &dir).expect_err("rejected");
        assert_eq!(err.code, "VERIFY_CHECKS_LIMIT_EXCEEDED");
    }

    #[test]
    fn parse_rejects_unknown_kind_and_bad_entry() {
        let dir = temp_dir("kind");
        let err = parse_request(&json!({ "checks": ["deploy"] }), &dir).expect_err("rejected");
        assert_eq!(err.code, "VERIFY_CHECK_KIND_UNSUPPORTED");
        let err = parse_request(&json!({ "checks": [7] }), &dir).expect_err("rejected");
        assert_eq!(err.code, "VERIFY_CHECK_INVALID");
        let err = parse_request(&json!({ "checks": [{}] }), &dir).expect_err("rejected");
        assert_eq!(err.code, "VERIFY_CHECK_INVALID");
    }

    #[test]
    fn parse_requires_command_for_custom_kind() {
        let dir = temp_dir("custom");
        let err = parse_request(&json!({ "checks": ["custom"] }), &dir).expect_err("rejected");
        assert_eq!(err.code, "VERIFY_CUSTOM_COMMAND_MISSING");
        let err = parse_request(
            &json!({ "checks": [{ "kind": "custom", "command": "  " }] }),
            &dir,
        )
        .expect_err("rejected");
        assert_eq!(err.code, "VERIFY_CUSTOM_COMMAND_MISSING");
    }

    #[test]
    fn parse_rejects_invalid_working_dir() {
        let dir = temp_dir("wd");
        let input = json!({ "checks": ["test"], "working_dir": "no-such-child" });
        let err = parse_request(&input, &dir).expect_err("rejected");
        assert_eq!(err.code, "VERIFY_WORKING_DIR_INVALID");
    }

    #[test]
    fn parse_defaults_fail_fast_and_timeout() {
        let dir = temp_dir("defaults");
        let request =
            parse_request(&json!({ "checks": ["compile", "test"] }), &dir).expect("parsed");
        assert!(request.fail_fast);
        assert_eq!(request.working_dir, dir);
        assert_eq!(request.plans.len(), 2);
        assert_eq!(request.plans[0].timeout, CHECK_TIMEOUT_DEFAULT);
        assert_eq!(request.plans[0].command, None);
    }

    #[test]
    fn parse_clamps_step_timeout_into_bounds() {
        let dir = temp_dir("clamp");
        let input = json!({
            "checks": [
                { "kind": "test", "timeout_ms": 1 },
                { "kind": "build", "timeout_ms": 24 * 60 * 60 * 1000_u64 }
            ],
            "fail_fast": false
        });
        let request = parse_request(&input, &dir).expect("parsed");
        assert!(!request.fail_fast);
        assert_eq!(request.plans[0].timeout, CHECK_TIMEOUT_MIN);
        assert_eq!(request.plans[1].timeout, CHECK_TIMEOUT_MAX);
    }

    #[test]
    fn parse_resolves_relative_working_dir_against_default() {
        let dir = temp_dir("relative");
        std::fs::create_dir_all(dir.join("sub")).expect("sub dir");
        let input = json!({ "checks": ["custom", ], "working_dir": "sub" });
        // custom 缺 command 会先失败，改用合法入参验证路径解析。
        assert!(parse_request(&input, &dir).is_err());
        let input = json!({ "checks": ["test"], "working_dir": "sub" });
        let request = parse_request(&input, &dir).expect("parsed");
        assert_eq!(request.working_dir, dir.join("sub"));
    }

    #[tokio::test]
    async fn custom_checks_run_in_order_and_pass() {
        let dir = temp_dir("run-pass");
        let output = EngineeringCheckRunnerTool
            .execute(
                json!({
                    "checks": [
                        { "kind": "custom", "command": "echo first" },
                        { "kind": "custom", "command": "echo second" }
                    ]
                }),
                ctx(&dir, CancellationToken::new()),
            )
            .await;
        assert!(!output.is_error, "content={}", output.content);
        let report = &output.metadata.expect("metadata")["structuredResult"];
        assert_eq!(report["status"], "pass");
        assert_eq!(report["passed"], 2);
        assert_eq!(
            report["checks"][0]["output"].as_str().expect("out").trim(),
            "first"
        );
        assert_eq!(
            report["checks"][1]["output"].as_str().expect("out").trim(),
            "second"
        );
        assert!(output.content.contains("| 1 | custom | PASS |"));
    }

    #[tokio::test]
    async fn fail_fast_skips_remaining_steps() {
        let dir = temp_dir("run-failfast");
        let output = EngineeringCheckRunnerTool
            .execute(
                json!({
                    "checks": [
                        { "kind": "custom", "command": "exit 3" },
                        { "kind": "custom", "command": "echo never" }
                    ]
                }),
                ctx(&dir, CancellationToken::new()),
            )
            .await;
        assert!(output.is_error);
        let report = &output.metadata.expect("metadata")["structuredResult"];
        assert_eq!(report["status"], "fail");
        assert_eq!(report["failed"], 1);
        assert_eq!(report["skipped"], 1);
        assert_eq!(report["checks"][0]["exitCode"], 3);
        assert_eq!(report["checks"][1]["status"], "skip");
        assert_eq!(report["checks"][1]["skipReason"], "skipped by fail_fast");
    }

    #[tokio::test]
    async fn fail_fast_disabled_runs_all_steps() {
        let dir = temp_dir("run-continue");
        let output = EngineeringCheckRunnerTool
            .execute(
                json!({
                    "fail_fast": false,
                    "checks": [
                        { "kind": "custom", "command": "exit 1" },
                        { "kind": "custom", "command": "echo after" }
                    ]
                }),
                ctx(&dir, CancellationToken::new()),
            )
            .await;
        assert!(output.is_error);
        let report = &output.metadata.expect("metadata")["structuredResult"];
        assert_eq!(report["failed"], 1);
        assert_eq!(report["passed"], 1);
        assert_eq!(report["skipped"], 0);
    }

    #[tokio::test]
    async fn unknown_project_skips_kinds_without_default_command() {
        let dir = temp_dir("run-skip");
        let output = EngineeringCheckRunnerTool
            .execute(
                json!({ "checks": ["compile", "test"] }),
                ctx(&dir, CancellationToken::new()),
            )
            .await;
        assert!(!output.is_error);
        let report = &output.metadata.expect("metadata")["structuredResult"];
        assert_eq!(report["status"], "skip");
        assert_eq!(report["projectKind"], "unknown");
        assert_eq!(report["skipped"], 2);
        assert!(
            report["checks"][0]["skipReason"]
                .as_str()
                .expect("reason")
                .contains("no default command")
        );
    }

    #[tokio::test]
    async fn rust_project_uses_cargo_defaults() {
        let dir = temp_dir("run-rust");
        std::fs::write(dir.join("Cargo.toml"), "[package]\nname=\"x\"\n").expect("write");
        // typecheck 在 Rust 项目下无默认命令（cargo check 即类型检查）。
        let output = EngineeringCheckRunnerTool
            .execute(
                json!({ "checks": ["typecheck"] }),
                ctx(&dir, CancellationToken::new()),
            )
            .await;
        let report = &output.metadata.expect("metadata")["structuredResult"];
        assert_eq!(report["projectKind"], "rust");
        assert_eq!(report["checks"][0]["status"], "skip");
    }

    #[tokio::test]
    async fn cancelled_token_skips_everything() {
        let dir = temp_dir("run-cancel");
        let cancel = CancellationToken::new();
        cancel.cancel();
        let output = EngineeringCheckRunnerTool
            .execute(
                json!({ "checks": [{ "kind": "custom", "command": "echo hi" }] }),
                ctx(&dir, cancel),
            )
            .await;
        assert!(!output.is_error);
        let report = &output.metadata.expect("metadata")["structuredResult"];
        assert_eq!(report["checks"][0]["status"], "skip");
        assert_eq!(report["checks"][0]["skipReason"], "cancelled");
    }

    #[tokio::test]
    async fn invalid_input_returns_error_code_in_content() {
        let dir = temp_dir("run-invalid");
        let output = EngineeringCheckRunnerTool
            .execute(json!({}), ctx(&dir, CancellationToken::new()))
            .await;
        assert!(output.is_error);
        assert!(output.content.starts_with("VERIFY_CHECKS_MISSING: "));
        assert!(output.metadata.is_none());
    }

    #[test]
    fn tool_spec_declares_seven_kinds_and_is_not_read_only() {
        let tool = EngineeringCheckRunnerTool;
        assert_eq!(tool.name(), "EngineeringCheckRunner");
        assert_eq!(tool.timeout(), MAX_TOOL_TIMEOUT);
        assert!(!tool.is_read_only(&json!({})));
        assert!(!tool.is_destructive(&json!({})));
        let schema = tool.parameters();
        let kinds = schema["properties"]["checks"]["items"]["oneOf"][0]["enum"]
            .as_array()
            .expect("enum array");
        assert_eq!(kinds.len(), CheckKind::ALL.len());
        assert_eq!(schema["required"][0], "checks");
    }
}
