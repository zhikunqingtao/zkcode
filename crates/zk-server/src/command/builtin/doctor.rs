//! `/doctor`——环境诊断（旧 `command/impl/DoctorCommand.java`）。
//!
//! 旧实现组装 9 项检查后回 `jsx({action:"diagnosticReport", checks, summary})`，
//! 由前端 `DiagnosticPanel` 渲染。每项检查是有序 map
//! `{category, name, value, status[, hint]}`（`hint` 为 null 时**整键剥离**，
//! 旧 `buildCheck` L117-126 的 if-put）。状态词表恒为 `ok` / `warn` / `error`
//! ——**不同于** [`crate::api::doctor`] 的 `ok` / `warning` / `error`（旧仓库两
//! 处就用两套词表，不可混用）。
//!
//! # 检查项映射（旧 9 项 → 本移植 7 项）
//!
//! | # | 旧检查项 | 本实现 |
//! |---|---|---|
//! | 1 | `runtime` / `Java Version`（`java.version`，`>= 21` 判 ok） | `runtime` / `Rust Version`：值取编译期 MSRV（见下方差异 1） |
//! | 2 | `llm` / `LLM Providers`（`providerRegistry.hasProviders()`） | [`crate::llm::ProviderRegistry::names`] 非空 |
//! | 3 | `env` / `Working Directory`（`context.workingDir()`） | [`CommandContext::working_dir`] 非空白 |
//! | 4 | `auth` / `Authentication`（`context.isAuthenticated()`） | `CommandContext::is_authenticated`（旧 `of()` 同样恒 false） |
//! | 5 | `session` / `Active Session`（`context.sessionId() != null`） | `CommandContext::session_id` 非空 |
//! | 6 | `tool` / `Git`（`git --version` 退出码 0） | 复用 [`crate::api::doctor::check_external_tool`] |
//! | 7 | `runtime` / `JVM Memory` | **未移植**（见差异 2） |
//! | 8 | `service` / `Python Service`（打侧车 `/api/health`） | 复用 [`crate::api::doctor::python_service_check`] |
//! | 9 | `env` / `Disk Space` | **未移植**（见差异 3） |
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! 1. **`Rust Version` 的值与判据**：旧项读 JVM 运行时版本并与 21 比较。Rust
//!    进程无「运行时版本」等价物（rustc 版本需 build script 注入），且 MSRV 由
//!    workspace `rust-version` 在**编译期**强制——低于门槛根本编译不出二进制，
//!    故本项值取 `CARGO_PKG_RUST_VERSION` 且状态恒 `ok`（旧「版本过低告警」在
//!    Rust 侧不可能发生）。
//! 2. **`JVM Memory` 未移植**：旧项读 JVM 托管堆（`Runtime.totalMemory /
//!    freeMemory / maxMemory`），Rust 无托管堆；改测 RSS 需新增依赖
//!    （`sysinfo`），超出本 Batch 范围。与 [`crate::api::doctor`] 的同名偏离
//!    一致（Batch 1 已留痕）。
//! 3. **`Disk Space` 未移植**：旧项调 `File.getFreeSpace()`；Rust 标准库无该
//!    能力，同样需新增依赖（`sysinfo` / `nix::statvfs`），归后续 Batch。
//! 4. **Git / Python 两项复用 REST 端点的探测函数**：判据（退出码 0 / 侧车
//!    Running）与旧一致，但两处词表不同，故本命令只读复用函数的**布尔结论**
//!    （`status() == "ok"`），文案仍按旧 `/doctor` 命令的中文逐字输出。
//! 5. **`summary` 的键序**：旧 `Map.of(...)` 无序，本实现以 JSON 对象序
//!    `ok/warn/error/total` 输出（前端按键取值，与序无关）。

use futures::future::BoxFuture;
use serde_json::{Map, Value};

use crate::api::doctor::{STATUS_OK, check_external_tool, python_service_check};
use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 旧检查项状态：通过。
const CHECK_OK: &str = "ok";
/// 旧检查项状态：告警（旧字面量为 `warn`，**非** `warning`）。
const CHECK_WARN: &str = "warn";
/// 旧检查项状态：失败。
const CHECK_ERROR: &str = "error";

/// `/doctor` 命令。
pub(super) struct DoctorCommand;

impl Command for DoctorCommand {
    fn name(&self) -> &'static str {
        "doctor"
    }

    fn description(&self) -> &'static str {
        "Run diagnostics on your setup"
    }

    fn command_type(&self) -> CommandType {
        CommandType::LocalJsx
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let checks = collect_checks(ctx).await;
            // 旧 L100-114 的汇总统计。
            CommandResult::jsx(serde_json::json!({
                "action": "diagnosticReport",
                "summary": {
                    "ok": count_status(&checks, CHECK_OK),
                    "warn": count_status(&checks, CHECK_WARN),
                    "error": count_status(&checks, CHECK_ERROR),
                    "total": checks.len(),
                },
                "checks": checks,
            }))
        })
    }
}

/// 七项探测（旧 `execute` L34-98 的检查块，顺序即旧声明序）。
///
/// 从 `execute` 抽出纯为可读性——旧实现把检查块与汇总统计写在同一方法内；
/// 每项先把 `(value, status, hint)` 归约成元组再交 [`build_check`]，判据与旧
/// 三元表达式逐条对应。
async fn collect_checks(ctx: &CommandContext) -> Vec<Value> {
    // 1. 运行时版本（旧 L36-40；判据差异见模块文档差异 1）。
    let runtime = build_check(
        "runtime",
        "Rust Version",
        option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unknown"),
        CHECK_OK,
        None,
    );
    // 2. LLM providers（旧 L42-47）。
    let (value, status, hint) = if ctx.state.providers.load().names().is_empty() {
        ("未注册", CHECK_ERROR, Some("请配置 LLM API Key"))
    } else {
        ("已注册", CHECK_OK, None)
    };
    let providers = build_check("llm", "LLM Providers", value, status, hint);
    // 3. 工作目录（旧 L49-54：`null || isBlank` → error，无 hint）。
    let (value, status) = if ctx.working_dir.trim().is_empty() {
        ("未设置", CHECK_ERROR)
    } else {
        (ctx.working_dir.as_str(), CHECK_OK)
    };
    let working_dir = build_check("env", "Working Directory", value, status, None);
    // 4. 认证（旧 L56-60）。
    let (value, status, hint) = if ctx.is_authenticated {
        ("已认证", CHECK_OK, None)
    } else {
        ("未认证", CHECK_WARN, Some("部分功能可能受限"))
    };
    let auth = build_check("auth", "Authentication", value, status, hint);
    // 5. 活跃会话（旧 L62-66：`sessionId != null`，无 hint）。
    let (value, status) = if ctx.session_id.is_empty() {
        ("无活跃会话", CHECK_WARN)
    } else {
        (ctx.session_id.as_str(), CHECK_OK)
    };
    let session = build_check("session", "Active Session", value, status, None);
    // 6. Git（旧 L68-73 + `checkGitAvailable` L128-136）。
    let git_available = check_external_tool("git", "git", &["--version"])
        .await
        .status()
        == STATUS_OK;
    let (value, status, hint) = if git_available {
        ("可用", CHECK_OK, None)
    } else {
        ("未找到", CHECK_WARN, Some("安装 Git 以启用版本控制功能"))
    };
    let git = build_check("tool", "Git", value, status, hint);
    // 8. Python 服务（旧 L86-87 + `checkPythonService` L138-157；旧第 7 项
    // JVM Memory 与第 9 项 Disk Space 不移植，见模块文档）。
    let (value, status, hint) = if python_service_check(&ctx.state).status() == STATUS_OK {
        ("运行中", CHECK_OK, None)
    } else {
        (
            "未运行或不可达",
            CHECK_WARN,
            Some("启动 python-service 以获得完整功能"),
        )
    };
    let python = build_check("service", "Python Service", value, status, hint);
    vec![runtime, providers, working_dir, auth, session, git, python]
}

/// 旧 `buildCheck`（L117-126）：`hint` 为 null 时整键剥离，其余四键恒在且按
/// `category` / `name` / `value` / `status` 的声明序落地（旧 `LinkedHashMap`）。
fn build_check(category: &str, name: &str, value: &str, status: &str, hint: Option<&str>) -> Value {
    let mut map = Map::new();
    map.insert("category".to_owned(), Value::from(category));
    map.insert("name".to_owned(), Value::from(name));
    map.insert("value".to_owned(), Value::from(value));
    map.insert("status".to_owned(), Value::from(status));
    if let Some(hint) = hint {
        map.insert("hint".to_owned(), Value::from(hint));
    }
    Value::Object(map)
}

/// 旧 L101-103 的 `checks.stream().filter(...).count()`。
fn count_status(checks: &[Value], status: &str) -> usize {
    checks
        .iter()
        .filter(|check| check.get("status").and_then(Value::as_str) == Some(status))
        .count()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{CHECK_OK, CHECK_WARN, build_check};
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(session_id: &str, working_dir: &str) -> Value {
        let ctx = CommandContext::of(session_id, working_dir, "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let doctor = registry.find_command("doctor").expect("registered");
        let CommandResult::Jsx(data) = doctor.execute("", &ctx).await else {
            panic!("/doctor must be jsx");
        };
        data
    }

    /// `hint` 为 null 时整键剥离；四键齐备。
    ///
    /// 键序差异（留痕）：旧 `LinkedHashMap` 让 JSON 保持声明序
    /// `category/name/value/status`；`serde_json::Map` 默认是 `BTreeMap`
    /// （workspace 未启 `preserve_order`），故落地为字典序。JSON 对象键序无
    /// 语义，前端按键取值，不构成行为差异。
    #[test]
    fn hint_is_stripped_when_absent_and_key_order_is_stable() {
        let bare = build_check("env", "Working Directory", "/tmp", CHECK_OK, None);
        let keys: Vec<&str> = bare
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, vec!["category", "name", "status", "value"]);

        let hinted = build_check("auth", "Authentication", "未认证", CHECK_WARN, Some("x"));
        assert_eq!(hinted["hint"], "x");
    }

    /// 报告信封（`action` / 7 项检查 / 汇总四键）与旧结构逐键对齐。
    #[tokio::test]
    async fn report_envelope_matches_the_legacy_shape() {
        let data = run("s-1", "/tmp/zk-doctor").await;
        assert_eq!(data["action"], "diagnosticReport");
        let checks = data["checks"].as_array().expect("checks array");
        assert_eq!(checks.len(), 7);
        let names: Vec<&str> = checks
            .iter()
            .map(|check| check["name"].as_str().expect("name"))
            .collect();
        assert_eq!(
            names,
            vec![
                "Rust Version",
                "LLM Providers",
                "Working Directory",
                "Authentication",
                "Active Session",
                "Git",
                "Python Service"
            ]
        );
        assert_eq!(data["summary"]["total"], 7);
        let tally = ["ok", "warn", "error"]
            .iter()
            .map(|key| data["summary"][*key].as_u64().expect("count"))
            .sum::<u64>();
        assert_eq!(tally, 7, "每项恰计入一个状态桶");
    }

    /// 测试装配下的确定性判据：无 provider → error；认证位 false → warn。
    #[tokio::test]
    async fn deterministic_checks_follow_the_legacy_predicates() {
        let data = run("s-1", "  ").await;
        let checks = data["checks"].as_array().expect("checks array");
        let by_name = |name: &str| -> &Value {
            checks
                .iter()
                .find(|check| check["name"] == name)
                .unwrap_or_else(|| panic!("check {name} missing"))
        };
        // `ProviderRegistry::new()` 无注册项（旧「请配置 LLM API Key」）。
        assert_eq!(by_name("LLM Providers")["status"], "error");
        assert_eq!(by_name("LLM Providers")["hint"], "请配置 LLM API Key");
        // 空白工作目录 → error 且值为「未设置」，旧该项无 hint。
        assert_eq!(by_name("Working Directory")["value"], "未设置");
        assert_eq!(by_name("Working Directory")["status"], "error");
        assert!(by_name("Working Directory").get("hint").is_none());
        // `CommandContext::of` 的认证位恒 false。
        assert_eq!(by_name("Authentication")["value"], "未认证");
        assert_eq!(by_name("Authentication")["hint"], "部分功能可能受限");
        // 会话 ID 非空 → ok 且值为 ID 本身，旧该项无 hint。
        assert_eq!(by_name("Active Session")["value"], "s-1");
        assert_eq!(by_name("Active Session")["status"], "ok");
    }
}
