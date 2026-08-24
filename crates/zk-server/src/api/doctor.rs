//! 环境诊断端点——`GET /api/doctor`（Batch 1 Step 1-6，P0-14）。
//!
//! 语义来源（旧仓库只读，逐字对照）：
//! `backend/src/main/java/com/aicodeassistant/controller/HealthController.java`
//! L88-112（`doctor()`）+ L136-172（`checkExternalTool` / `doctorCheck`），
//! 响应形状另与实采样例 `docs/baseline/samples/GET_api-doctor.json` 逐键互锁。
//!
//! # 信封与检查项形状（旧 `doctorCheck` L163-172）
//!
//! ```json
//! { "checks": [ { "name": "...", "status": "ok|warning|error",
//!                 "version": "...", "message": "...", "latencyMs": 27 } ] }
//! ```
//!
//! `version` / `message` / `latencyMs` 为 null 时**整键剥离**（旧 if-put
//! 语义，对齐 Jackson `NON_NULL`）；`name` / `status` 恒在。状态词表恒为
//! 小写三值 `ok` / `warning` / `error`（**不同于** `/api/health` 的
//! `UP` / `DEGRADED`，两端点在旧实现中就用两套词表，不可混用）。
//!
//! # 检查项映射
//!
//! | 本端点检查项 | 旧权威源 | 说明 |
//! |---|---|---|
//! | `runtime` | `HealthController:93-95` 的 `java` 检查 | JVM → Rust 化重命名（同 `/api/health` 的 `java`→`runtime` 既有偏离，见 S7 报告）；`version` 取 crate 版本 |
//! | `git` | `HealthController:98`（`checkExternalTool("git","git","--version")`） | 逐字复刻：退出码 0 → `ok` + 首行版本；非 0 → `error`；无法启动 → `warning` |
//! | `ripgrep` | `HealthController:101`（`rg --version`） | 同上 |
//! | `database` | `HealthController:116-122`（`checkDatabase`）| 旧 doctor 未含此项，旧 `checkDatabase` 对内嵌 `SQLite` 恒 UP；此处以一次真实只读查询落实探测（与 `/api/health` 同探测强度） |
//! | `llm_providers` | `command/impl/DoctorCommand.java:43-47`（`LLM Providers`） | 旧 `/doctor` 斜杠命令的检查项；判据同为「是否有已注册 provider」，不发网络请求 |
//! | `python_service` | `command/impl/DoctorCommand.java:138-157`（`Python Service`） | 同上；Rust 侧读进程内侧车状态而非再打一次 `/api/health`（探测由侧车 30s 轮询承担） |
//!
//! 旧 `jvm_memory`（`HealthController:104-109`）**未复刻**：它读的是 JVM 托管
//! 堆（`Runtime.totalMemory/freeMemory/maxMemory`），Rust 无托管堆等价物；
//! 若改测 RSS 需引入新依赖（`sysinfo`），超出本 Step 范围。其"运行时资源可
//! 观测"的意图由 `runtime` 检查的进程 uptime 承接。
//!
//! # 超时
//!
//! 旧 `checkExternalTool` 用 `process.waitFor()` **无超时**等待；本移植保持
//! 同一语义（不自行加超时改变可观察行为）。

use std::process::Stdio;
use std::time::Instant;

use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::python::ProcessState;
use crate::state::AppState;

/// 检查通过（旧 `"ok"`）。
pub(crate) const STATUS_OK: &str = "ok";
/// 检查告警（旧 `"warning"`：外部工具不可启动 / 可选组件未就绪）。
const STATUS_WARNING: &str = "warning";
/// 检查失败（旧 `"error"`：外部工具非 0 退出 / 必需依赖不可用）。
const STATUS_ERROR: &str = "error";

/// 诊断响应体（旧 `Map.of("checks", checks)`）。
#[derive(Debug, Serialize)]
pub(crate) struct DoctorReport {
    /// 检查项清单（顺序即旧 `checks.add` 的追加顺序，稳定）。
    checks: Vec<DoctorCheck>,
}

/// 单项检查（旧 `doctorCheck` 组装的 `LinkedHashMap`：`name` / `status` 恒在，
/// 其余三键为 null 时剥离）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DoctorCheck {
    /// 检查项名。
    name: &'static str,
    /// 状态（`ok` / `warning` / `error`）。
    status: &'static str,
    /// 版本号（外部工具输出首行 / 运行时版本；无则剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    /// 人读说明（无则剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    /// 探测耗时（毫秒；未计时的检查项剥离该键）。
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_ms: Option<u64>,
}

impl DoctorCheck {
    /// 组装检查项（旧 `doctorCheck(name, status, version, message, latencyMs)`）。
    fn new(
        name: &'static str,
        status: &'static str,
        version: Option<String>,
        message: Option<String>,
        latency_ms: Option<u64>,
    ) -> Self {
        Self {
            name,
            status,
            version,
            message,
            latency_ms,
        }
    }

    /// 状态词（`ok` / `warning` / `error`）。
    ///
    /// 对外可见（Batch 3）：`/doctor` 斜杠命令复用本模块的探测函数，但输出
    /// 旧 `DoctorCommand` 的检查项形状与**另一套**状态词表（`warn` 而非
    /// `warning`），故只读取此处的布尔结论、不透传状态字面量。
    pub(crate) fn status(&self) -> &'static str {
        self.status
    }
}

/// 毫秒耗时（`u128` → `u64` 饱和转换；`unsafe` 禁用下不做裸 as 截断）。
fn elapsed_millis(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `GET /api/doctor`——环境诊断（旧 `HealthController.doctor` L88-112）。
///
/// 恒 200：检查项各自携带状态，端点本身不因某项 `error` 改状态码（旧实现
/// 直接返回 `Map`，无 `ResponseEntity.status` 分支）。
#[utoipa::path(
    get,
    path = "/api/doctor",
    tag = "system",
    responses(
        (status = 200, description = "{checks:[{name,status,version?,message?,latencyMs?}]}")
    )
)]
pub(crate) async fn doctor(State(state): State<AppState>) -> Json<DoctorReport> {
    let checks = vec![
        runtime_check(&state),
        check_external_tool("git", "git", &["--version"]).await,
        check_external_tool("ripgrep", "rg", &["--version"]).await,
        database_check(&state).await,
        llm_providers_check(&state),
        python_service_check(&state),
    ];
    Json(DoctorReport { checks })
}

/// 运行时检查（旧 `java` 检查：状态恒 `ok`，`version` 取运行时版本，
/// `message` 为「runtime available」）。进程 uptime 并入 message——承接旧
/// `jvm_memory` 的运行时可观测意图（见模块文档）。
fn runtime_check(state: &AppState) -> DoctorCheck {
    let uptime_secs = state.started_at.elapsed().as_secs();
    DoctorCheck::new(
        "runtime",
        STATUS_OK,
        Some(env!("CARGO_PKG_VERSION").to_owned()),
        Some(format!("Rust runtime available; uptime {uptime_secs}s")),
        None,
    )
}

/// 外部工具探测（旧 `checkExternalTool` L136-161 逐字复刻）：
///
/// - 退出码 0 → `ok`，`version` 取合并输出首行（取不到 → `"unknown"`）；
/// - 退出码非 0 → `error`，`message` 为 `"<name> exited with code <n>"`；
/// - 无法启动（可执行文件缺失等）→ `warning`，`message` 为
///   `"<name> not found: <err>"`。
///
/// 三分支均带 `latencyMs`。stdin 置 null：旧 `ProcessBuilder` 不喂输入，
/// 避免子进程误继承服务端 stdin 而阻塞。
pub(crate) async fn check_external_tool(
    name: &'static str,
    command: &str,
    args: &[&str],
) -> DoctorCheck {
    let start = Instant::now();
    let output = tokio::process::Command::new(command)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await;
    let latency = elapsed_millis(start);
    match output {
        Ok(output) if output.status.success() => {
            // 旧 `redirectErrorStream(true)` + `output.lines().findFirst()`：
            // stdout 优先，为空再看 stderr（合并流的等价观察）。
            let version = first_line(&output.stdout)
                .or_else(|| first_line(&output.stderr))
                .unwrap_or_else(|| "unknown".to_owned());
            DoctorCheck::new(
                name,
                STATUS_OK,
                Some(version),
                Some(format!("{name} available")),
                Some(latency),
            )
        }
        Ok(output) => {
            // 旧 `exitCode` 取 `Process.waitFor()`；信号终止时 Java 侧为
            // 128+signo，Rust 侧 `code()` 为 None，此处以 `signal` 标记。
            let code = output
                .status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string());
            DoctorCheck::new(
                name,
                STATUS_ERROR,
                None,
                Some(format!("{name} exited with code {code}")),
                Some(latency),
            )
        }
        Err(err) => DoctorCheck::new(
            name,
            STATUS_WARNING,
            None,
            Some(format!("{name} not found: {err}")),
            Some(latency),
        ),
    }
}

/// 输出首行（去除行尾 `\r`，空输出 → `None`）。
fn first_line(bytes: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(bytes);
    text.lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
}

/// 数据库连通性（旧 `checkDatabase` L116-122 的语义 + 一次真实只读查询）。
async fn database_check(state: &AppState) -> DoctorCheck {
    let start = Instant::now();
    let ok = state.db.list_sessions(None, 1).await.is_ok();
    let latency = elapsed_millis(start);
    if ok {
        DoctorCheck::new(
            "database",
            STATUS_OK,
            None,
            Some("SQLite embedded database available".to_owned()),
            Some(latency),
        )
    } else {
        DoctorCheck::new(
            "database",
            STATUS_ERROR,
            None,
            Some("SQLite embedded database query failed".to_owned()),
            Some(latency),
        )
    }
}

/// LLM provider 可达性（旧 `DoctorCommand:43-47`：判据为「是否有已注册
/// provider」，不发网络请求；未注册时旧 hint 为「请配置 LLM API Key」，
/// 本端点消息统一英文以对齐本端点其余检查项的文案语种）。
fn llm_providers_check(state: &AppState) -> DoctorCheck {
    let names = state.providers.names();
    if names.is_empty() {
        DoctorCheck::new(
            "llm_providers",
            STATUS_ERROR,
            None,
            Some("no LLM provider registered; configure an API key".to_owned()),
            None,
        )
    } else {
        DoctorCheck::new(
            "llm_providers",
            STATUS_OK,
            None,
            Some(format!(
                "{} provider(s) registered: {}",
                names.len(),
                names.join(", ")
            )),
            None,
        )
    }
}

/// Python 侧车状态（旧 `DoctorCommand:138-157`：运行中 → `ok`，其余一律
/// 告警；本移植读进程内侧车状态，不再打一次侧车 `/api/health`——真实探测由
/// 侧车 30s 健康轮询与 1s liveness 巡检承担，诊断端点保持轻量）。
pub(crate) fn python_service_check(state: &AppState) -> DoctorCheck {
    if !state.config.python_enabled {
        return DoctorCheck::new(
            "python_service",
            STATUS_WARNING,
            None,
            Some("Python sidecar disabled (ZK_PYTHON_ENABLED=false)".to_owned()),
            None,
        );
    }
    let Some(sidecar) = state.python_sidecar.as_ref() else {
        // 侧车非本进程托管（外部 uvicorn / 集成测试）：退化为客户端能力缓存视角。
        let (status, message) = if state.python.last_refresh_succeeded() {
            (STATUS_OK, "unmanaged sidecar reachable".to_owned())
        } else {
            (STATUS_WARNING, "unmanaged sidecar unreachable".to_owned())
        };
        return DoctorCheck::new("python_service", status, None, Some(message), None);
    };
    let process = sidecar.state();
    let status = if matches!(process, ProcessState::Running) {
        STATUS_OK
    } else {
        STATUS_WARNING
    };
    DoctorCheck::new(
        "python_service",
        status,
        None,
        Some(format!(
            "sidecar process {}; restarts {}",
            process.as_str(),
            sidecar.restart_count()
        )),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        DoctorCheck, STATUS_OK, STATUS_WARNING, check_external_tool, first_line,
        llm_providers_check, python_service_check, runtime_check,
    };
    use crate::state::AppState;

    /// null 键整键剥离（旧 `doctorCheck` 的 if-put），`name`/`status` 恒在。
    #[test]
    fn null_fields_are_stripped_from_the_check_object() {
        let check = DoctorCheck::new("probe", STATUS_OK, None, None, None);
        let json = serde_json::to_value(&check).expect("serializes");
        let keys: Vec<&str> = json
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, vec!["name", "status"]);
    }

    /// 三可选键齐备时键名对齐旧样例（`latencyMs` 为 camelCase）。
    #[test]
    fn optional_fields_use_baseline_key_names() {
        let check = DoctorCheck::new(
            "git",
            STATUS_OK,
            Some("git version 2.50.1".to_owned()),
            Some("git available".to_owned()),
            Some(27),
        );
        let json = serde_json::to_value(&check).expect("serializes");
        assert_eq!(json["name"], "git");
        assert_eq!(json["status"], "ok");
        assert_eq!(json["version"], "git version 2.50.1");
        assert_eq!(json["message"], "git available");
        assert_eq!(json["latencyMs"], 27);
    }

    /// 可执行文件缺失 → `warning` + `not found` 文案 + 带 latency（旧 catch 分支）。
    #[tokio::test]
    async fn missing_executable_yields_warning_check() {
        let check = check_external_tool(
            "nonexistent",
            "zk-doctor-nonexistent-binary",
            &["--version"],
        )
        .await;
        assert_eq!(check.status, STATUS_WARNING);
        assert!(
            check
                .message
                .as_deref()
                .is_some_and(|message| message.starts_with("nonexistent not found: ")),
            "unexpected message: {:?}",
            check.message
        );
        assert!(check.version.is_none());
        assert!(check.latency_ms.is_some());
    }

    /// 首行提取：跳过空输出、裁剪行尾空白。
    #[test]
    fn first_line_trims_and_rejects_empty_output() {
        assert_eq!(first_line(b""), None);
        assert_eq!(first_line(b"\n\n"), None);
        assert_eq!(
            first_line(b"git version 2.50.1\r\nextra\n"),
            Some("git version 2.50.1".to_owned())
        );
    }

    /// 测试装配（无 provider / 侧车非本进程托管）下的确定性状态。
    #[test]
    fn test_state_yields_deterministic_optional_component_checks() {
        let state = AppState::for_tests();
        let runtime = runtime_check(&state);
        assert_eq!(runtime.status, STATUS_OK);
        assert_eq!(runtime.version.as_deref(), Some(env!("CARGO_PKG_VERSION")));

        // `ProviderRegistry::new()` 无注册项 → error（旧「请配置 LLM API Key」）。
        let llm = llm_providers_check(&state);
        assert_eq!(llm.status, super::STATUS_ERROR);

        // 侧车未由本进程托管且能力缓存未刷新成功 → warning。
        let python = python_service_check(&state);
        assert_eq!(python.status, STATUS_WARNING);
    }
}
