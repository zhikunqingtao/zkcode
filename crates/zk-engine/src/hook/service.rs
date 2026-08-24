//! Hook 服务：事件触发 + 本地命令 / HTTP 通道分派（Batch 8B Step 3）。
//!
//! 对照旧 `hook/HookService.java`（369L）。语义偏离留痕：
//!
//! - **H-04 通知 vs 函数**：旧服务是进程内函数 hook 编排（`executePreToolUse` /
//!   `executePostToolUse` / `executeStopHooks` / …），带 role 校验、matcher 正则、
//!   上下文改写传递、异常→deny、会话快照写入——hook **参与主流程裁决**。本端
//!   [`HookService::fire`] 仅**通知**：把事件+上下文投递给外部命令 / HTTP 端点，
//!   不读返回、不改主流程。准入唯一权威仍是 [`crate::admission::ToolAdmission`]。
//! - **H-04b 错误隔离**：旧 hook 异常映射为 `HookResult.proceed=false`（拒绝工具）。
//!   本端 hook 失败**只 `warn!`**，绝不冒泡——Batch 8B 硬约束「Hook 失败必须隔离
//!   不阻塞」。同步模式最多等 `timeout_secs`（缺省 30s），到点 kill 子进程并 warn。
//!
//! 本地命令约定：`sh -c <command>`，payload JSON 经 stdin 传入，事件/工具/会话/
//! 工作目录另经环境变量 `ZK_HOOK_EVENT` / `ZK_HOOK_TOOL` / `ZK_HOOK_SESSION` /
//! `ZK_HOOK_WORKING_DIR` 暴露。stdout/stderr 丢弃（通知无回读语义）。

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use super::event::{HookConfig, HookEvent, HookRole};
use super::http_executor::{HttpHookError, HttpHookExecutor};
use super::registry::HookRegistry;
use crate::observability::{NoopObservabilityRecorder, ObservabilityEvent, ObservabilityRecorder};

/// 单次 hook 触发的上下文（投递给外部命令 / HTTP 端点）。
///
/// 全字段可选：不同触发点携带的信息不同（如 run 起止无 `tool_name`，工具执行
/// 前无 `result_preview`）。缺省 [`HookContext::default`] 后按需 `with_*` 填充。
#[derive(Debug, Clone, Default)]
pub struct HookContext {
    /// 工具名（`PreToolExecution` / `PostToolExecution` 携带）。
    pub tool_name: Option<String>,
    /// 会话 ID。
    pub session_id: Option<String>,
    /// 工作目录绝对路径。
    pub working_dir: Option<String>,
    /// 工具结果预览（`PostToolExecution` 携带，已截断）。
    pub result_preview: Option<String>,
}

impl HookContext {
    /// 空上下文。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 附工具名。
    #[must_use]
    pub fn with_tool(mut self, tool_name: impl Into<String>) -> Self {
        self.tool_name = Some(tool_name.into());
        self
    }

    /// 附会话 ID。
    #[must_use]
    pub fn with_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// 附工作目录。
    #[must_use]
    pub fn with_working_dir(mut self, working_dir: impl Into<String>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    /// 附结果预览（自动截断至 [`RESULT_PREVIEW_LIMIT`] 字符）。
    #[must_use]
    pub fn with_result_preview(mut self, preview: impl Into<String>) -> Self {
        let preview = preview.into();
        self.result_preview = Some(truncate_chars(&preview, RESULT_PREVIEW_LIMIT));
        self
    }
}

/// 结果预览上限（字符数）——避免超长工具输出灌爆 hook payload。
pub const RESULT_PREVIEW_LIMIT: usize = 2000;
const PRE_HOOK_OUTPUT_LIMIT: usize = 64 * 1024;

/// Functional PRE hook result. The returned input is not trusted: the caller
/// must submit it to Admission again before execution.
#[derive(Debug, Clone, PartialEq)]
pub enum PreHookDecision {
    /// Continue with the accumulated (possibly modified) input.
    Continue {
        /// Final untrusted input that must be re-admitted.
        input: Value,
    },
    /// Reject before Admission/tool execution.
    Deny {
        /// Stable machine-readable denial code.
        code: String,
        /// Bounded model-facing denial message.
        message: String,
    },
}

/// 按字符边界截断（不在多字节码点中间切开）。
fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    value.chars().take(limit).collect()
}

/// Hook 服务：持注册表，`fire` 时按事件分派到已注册 hook。
pub struct HookService {
    registry: HookRegistry,
    recorder: Arc<dyn ObservabilityRecorder>,
}

impl HookService {
    fn registry_for_context(&self, context: &HookContext) -> HookRegistry {
        context
            .working_dir
            .as_deref()
            .map(Path::new)
            .filter(|root| root.is_absolute())
            .map_or_else(|| self.registry.clone(), HookRegistry::load_from_dir)
    }

    /// 以给定注册表构造。
    #[must_use]
    pub fn new(registry: HookRegistry) -> Self {
        Self {
            registry,
            recorder: Arc::new(NoopObservabilityRecorder),
        }
    }

    /// Attach the process-wide best-effort recorder.
    #[must_use]
    pub fn with_observability(mut self, recorder: Arc<dyn ObservabilityRecorder>) -> Self {
        self.recorder = recorder;
        self
    }

    /// 从工作根目录加载 `.zk/hooks.toml` 构造。
    #[must_use]
    pub fn load_from_dir(root: &Path) -> Self {
        Self::new(HookRegistry::load_from_dir(root))
    }

    /// 无 hook 的空服务（`fire` 恒空转，near-zero cost）。
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(HookRegistry::new())
    }

    /// 是否无任何已注册 hook。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }

    /// 触发某事件下的全部 hook（外部通知，错误隔离）。
    ///
    /// 同步 hook（`async_mode == false`）按声明顺序**等待完成**（各自受
    /// `timeout_secs` 约束）；异步 hook 经 `tokio::spawn` 派发后**不等待**。
    /// 任一 hook 失败仅 `warn!`，绝不影响调用方主流程。
    pub async fn fire(&self, event: HookEvent, context: &HookContext) {
        let registry = self.registry_for_context(context);
        let hooks = registry.hooks_for(event);
        if hooks.is_empty() {
            return;
        }
        let payload = build_payload(event, context);
        for hook in hooks {
            if hook.async_mode {
                let hook = hook.clone();
                let context = context.clone();
                let payload = payload.clone();
                let recorder = Arc::clone(&self.recorder);
                tokio::spawn(async move {
                    let started = std::time::Instant::now();
                    let outcome = execute_one(&hook, event, &context, &payload).await;
                    record_hook_outcome(&recorder, &hook, &context, started, &outcome);
                });
            } else {
                let started = std::time::Instant::now();
                let outcome = execute_one(hook, event, context, &payload).await;
                record_hook_outcome(&self.recorder, hook, context, started, &outcome);
            }
        }
    }

    /// Execute matching PRE hooks in priority order.
    ///
    /// Notification/presentation hooks cannot alter input. Transform/security
    /// hooks use a bounded JSON response on stdout. Security failures deny;
    /// ordinary hook failures are logged and isolated.
    pub async fn evaluate_pre_tool(&self, context: &HookContext, input: &Value) -> PreHookDecision {
        let Some(tool_name) = context.tool_name.as_deref() else {
            return PreHookDecision::Continue {
                input: input.clone(),
            };
        };
        let registry = self.registry_for_context(context);
        if registry.has_invalid_security_config() {
            return PreHookDecision::Deny {
                code: "HOOK_SECURITY_CONFIG_INVALID".to_owned(),
                message: "security hook configuration is invalid".to_owned(),
            };
        }
        let mut current = input.clone();
        for hook in registry.hooks_for(HookEvent::PreToolExecution) {
            if !hook.matches_tool(tool_name) {
                continue;
            }
            if matches!(hook.role, HookRole::Notification | HookRole::Presentation) {
                let started = std::time::Instant::now();
                let outcome = execute_one(
                    hook,
                    HookEvent::PreToolExecution,
                    context,
                    &build_pre_payload(context, &current),
                )
                .await;
                record_hook_outcome(&self.recorder, hook, context, started, &outcome);
                continue;
            }
            let before = current.clone();
            let decision = evaluate_functional_hook(hook, context, &current).await;
            match decision {
                Ok(PreHookDecision::Continue { input }) => {
                    if input != before {
                        record_pre_decision(&self.recorder, hook, context, "modified", false);
                    }
                    current = input;
                }
                Ok(deny @ PreHookDecision::Deny { .. }) => {
                    record_pre_decision(&self.recorder, hook, context, "denied", true);
                    return deny;
                }
                Err(error) if hook.fails_closed() => {
                    tracing::error!(hook = %hook.name, %error, "security PRE hook failed closed");
                    record_pre_decision(&self.recorder, hook, context, "error", true);
                    return PreHookDecision::Deny {
                        code: "HOOK_SECURITY_FAILED".to_owned(),
                        message: "security hook could not validate the tool input".to_owned(),
                    };
                }
                Err(error) => {
                    tracing::warn!(hook = %hook.name, %error, "PRE hook failure isolated");
                    record_pre_decision(&self.recorder, hook, context, "error", false);
                }
            }
        }
        PreHookDecision::Continue { input: current }
    }
}

fn record_pre_decision(
    recorder: &Arc<dyn ObservabilityRecorder>,
    hook: &HookConfig,
    context: &HookContext,
    outcome: &str,
    security_audit: bool,
) {
    let mut event = ObservabilityEvent::new("hook", "pre", outcome);
    event.session_id.clone_from(&context.session_id);
    event.security_audit = security_audit;
    event
        .attributes
        .insert("hook".to_owned(), Value::String(hook.name.clone()));
    recorder.record(event);
}

fn record_hook_outcome(
    recorder: &Arc<dyn ObservabilityRecorder>,
    hook: &HookConfig,
    context: &HookContext,
    started: std::time::Instant,
    outcome: &Result<(), String>,
) {
    let mut event = ObservabilityEvent::new(
        "hook",
        "notify",
        if outcome.is_ok() { "ok" } else { "error" },
    );
    event.session_id.clone_from(&context.session_id);
    event.duration_ms = Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
    event.security_audit = hook.fails_closed() && outcome.is_err();
    event
        .attributes
        .insert("hook".to_owned(), Value::String(hook.name.clone()));
    recorder.record(event);
}

fn build_pre_payload(context: &HookContext, input: &Value) -> Value {
    let mut payload = build_payload(HookEvent::PreToolExecution, context);
    if let Some(object) = payload.as_object_mut() {
        object.insert("input".to_owned(), input.clone());
    }
    payload
}

async fn evaluate_functional_hook(
    hook: &HookConfig,
    context: &HookContext,
    input: &Value,
) -> Result<PreHookDecision, String> {
    if hook.async_mode {
        return Err("functional PRE hook cannot be asynchronous".to_owned());
    }
    if hook.is_http() {
        return Err("functional HTTP PRE hook responses are not enabled".to_owned());
    }
    let stdout = run_command_capture(
        hook,
        HookEvent::PreToolExecution,
        context,
        &build_pre_payload(context, input),
    )
    .await
    .map_err(|error| error.to_string())?;
    let value: Value = serde_json::from_slice(&stdout)
        .map_err(|error| format!("invalid PRE hook JSON: {error}"))?;
    let decision = value
        .get("decision")
        .and_then(Value::as_str)
        .unwrap_or("continue");
    match decision {
        "continue" => {
            let next = value.get("input").cloned().unwrap_or_else(|| input.clone());
            if !next.is_object() {
                return Err("PRE hook input must be a JSON object".to_owned());
            }
            Ok(PreHookDecision::Continue { input: next })
        }
        "deny" => {
            let code = value
                .get("code")
                .and_then(Value::as_str)
                .filter(|code| {
                    !code.is_empty()
                        && code.len() <= 64
                        && code
                            .chars()
                            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
                })
                .unwrap_or("HOOK_DENIED")
                .to_owned();
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("tool input denied by PRE hook");
            Ok(PreHookDecision::Deny {
                code,
                message: truncate_chars(message, 512),
            })
        }
        other => Err(format!("unknown PRE hook decision {other:?}")),
    }
}

/// 构造 hook payload JSON（HTTP body / 本地命令 stdin 共用）。
fn build_payload(event: HookEvent, context: &HookContext) -> Value {
    json!({
        "event": event.as_str(),
        "tool": context.tool_name,
        "sessionId": context.session_id,
        "workingDir": context.working_dir,
        "resultPreview": context.result_preview,
    })
}

/// 执行单条 hook（HTTP 或本地命令），失败仅 `warn!`（错误隔离）。
async fn execute_one(
    hook: &HookConfig,
    event: HookEvent,
    context: &HookContext,
    payload: &Value,
) -> Result<(), String> {
    let outcome = if hook.is_http() {
        run_http(hook, payload)
            .await
            .map_err(|error| error.to_string())
    } else {
        run_command(hook, event, context, payload)
            .await
            .map_err(|error| error.to_string())
    };
    if let Err(error) = outcome {
        tracing::warn!(
            hook = %hook.name,
            event = %event,
            %error,
            "hook execution failed (isolated; main flow unaffected)"
        );
        return Err(error);
    }
    Ok(())
}

/// HTTP 通道：经 SSRF 安全执行器 POST payload。
async fn run_http(hook: &HookConfig, payload: &Value) -> Result<(), HttpHookError> {
    let url = hook.url.as_deref().unwrap_or_default();
    HttpHookExecutor.send(url, payload).await
}

/// 本地命令通道执行失败原因（内部；均降级为 `warn!`）。
#[derive(Debug, thiserror::Error)]
enum CommandError {
    /// 子进程派生失败。
    #[error("spawn failed: {0}")]
    Spawn(String),
    /// 等待超时（已尝试 kill 子进程）。
    #[error("timed out after {0}s")]
    Timeout(u64),
    /// 子进程退出为非零状态。
    #[error("exited with {0}")]
    NonZeroExit(String),
    /// 等待子进程时 I/O 失败。
    #[error("wait failed: {0}")]
    Wait(String),
}

/// 本地命令通道：`sh -c <command>`，payload 走 stdin，元信息走环境变量。
async fn run_command(
    hook: &HookConfig,
    event: HookEvent,
    context: &HookContext,
    payload: &Value,
) -> Result<(), CommandError> {
    let command = hook.command.as_deref().unwrap_or_default();
    let mut builder = tokio::process::Command::new("/bin/sh");
    builder
        .arg("-c")
        .arg(command)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("ZK_HOOK_EVENT", event.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(tool) = &context.tool_name {
        builder.env("ZK_HOOK_TOOL", tool);
    }
    if let Some(session) = &context.session_id {
        builder.env("ZK_HOOK_SESSION", session);
    }
    if let Some(working_dir) = &context.working_dir {
        builder.env("ZK_HOOK_WORKING_DIR", working_dir);
    }

    let mut child = builder
        .spawn()
        .map_err(|error| CommandError::Spawn(error.to_string()))?;

    // payload → stdin（写失败不致命：命令可能不读 stdin，忽略即可）。
    if let Some(mut stdin) = child.stdin.take() {
        let bytes = serde_json::to_vec(payload).unwrap_or_default();
        let _ = stdin.write_all(&bytes).await;
        let _ = stdin.shutdown().await;
    }

    // `let` 绑定确保 `child.wait()` 临时借用在本语句 `;` 处即释放，
    // 超时分支得以再次可变借用 child 执行 kill。
    let waited = tokio::time::timeout(Duration::from_secs(hook.timeout_secs), child.wait()).await;
    match waited {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(CommandError::NonZeroExit(status.to_string())),
        Ok(Err(error)) => Err(CommandError::Wait(error.to_string())),
        Err(_elapsed) => {
            let _ = child.start_kill();
            Err(CommandError::Timeout(hook.timeout_secs))
        }
    }
}

async fn run_command_capture(
    hook: &HookConfig,
    event: HookEvent,
    context: &HookContext,
    payload: &Value,
) -> Result<Vec<u8>, CommandError> {
    let command = hook.command.as_deref().unwrap_or_default();
    let mut builder = tokio::process::Command::new("/bin/sh");
    builder
        .arg("-c")
        .arg(command)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("ZK_HOOK_EVENT", event.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(tool) = &context.tool_name {
        builder.env("ZK_HOOK_TOOL", tool);
    }
    if let Some(session) = &context.session_id {
        builder.env("ZK_HOOK_SESSION", session);
    }
    if let Some(working_dir) = &context.working_dir {
        builder.env("ZK_HOOK_WORKING_DIR", working_dir);
    }
    let mut child = builder
        .spawn()
        .map_err(|error| CommandError::Spawn(error.to_string()))?;
    if let Some(mut stdin) = child.stdin.take() {
        let bytes = serde_json::to_vec(payload).unwrap_or_default();
        let _ = stdin.write_all(&bytes).await;
        let _ = stdin.shutdown().await;
    }
    let output = tokio::time::timeout(
        Duration::from_secs(hook.timeout_secs),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| CommandError::Timeout(hook.timeout_secs))?
    .map_err(|error| CommandError::Wait(error.to_string()))?;
    if !output.status.success() {
        return Err(CommandError::NonZeroExit(output.status.to_string()));
    }
    if output.stdout.len() > PRE_HOOK_OUTPUT_LIMIT {
        return Err(CommandError::Wait("stdout exceeds 64KiB".to_owned()));
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command_hook(name: &str, command: &str, async_mode: bool, timeout_secs: u64) -> HookConfig {
        HookConfig {
            name: name.to_owned(),
            event: HookEvent::PreToolExecution,
            role: HookRole::Notification,
            matcher: None,
            priority: 0,
            command: Some(command.to_owned()),
            url: None,
            async_mode,
            timeout_secs,
        }
    }

    fn functional_hook(name: &str, command: &str, role: HookRole) -> HookConfig {
        HookConfig {
            name: name.to_owned(),
            event: HookEvent::PreToolExecution,
            role,
            matcher: Some("^Read$".to_owned()),
            priority: 0,
            command: Some(command.to_owned()),
            url: None,
            async_mode: false,
            timeout_secs: 5,
        }
    }

    #[tokio::test]
    async fn fire_with_no_hooks_is_noop() {
        let service = HookService::disabled();
        assert!(service.is_empty());
        // 不 panic、不阻塞即通过。
        service.fire(HookEvent::RunStart, &HookContext::new()).await;
    }

    #[tokio::test]
    async fn sync_command_hook_runs_to_completion() {
        let mut registry = HookRegistry::new();
        registry.register(command_hook("ok", "exit 0", false, 5));
        let service = HookService::new(registry);
        // 命令成功——fire 返回即代表已等待完成且无 warn 冒泡。
        service
            .fire(
                HookEvent::PreToolExecution,
                &HookContext::new().with_tool("Read"),
            )
            .await;
    }

    #[tokio::test]
    async fn failing_command_hook_is_isolated() {
        let mut registry = HookRegistry::new();
        registry.register(command_hook("boom", "exit 3", false, 5));
        let service = HookService::new(registry);
        // 非零退出被隔离：fire 正常返回，不 panic、不冒泡。
        service
            .fire(HookEvent::PreToolExecution, &HookContext::new())
            .await;
    }

    #[tokio::test]
    async fn timeout_kills_and_isolates() {
        let mut registry = HookRegistry::new();
        registry.register(command_hook("slow", "sleep 10", false, 1));
        let service = HookService::new(registry);
        let start = std::time::Instant::now();
        service
            .fire(HookEvent::PreToolExecution, &HookContext::new())
            .await;
        // 1s 超时应远早于命令自身的 10s。
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn transform_pre_hook_modifies_matching_read_input() {
        let mut registry = HookRegistry::new();
        registry.register(functional_hook(
            "rewrite",
            r#"printf '%s' '{"decision":"continue","input":{"path":"README.md"}}'"#,
            HookRole::Transform,
        ));
        let service = HookService::new(registry);
        let decision = service
            .evaluate_pre_tool(
                &HookContext::new().with_tool("Read"),
                &json!({"path": "before.txt"}),
            )
            .await;
        assert_eq!(
            decision,
            PreHookDecision::Continue {
                input: json!({"path": "README.md"})
            }
        );
    }

    #[tokio::test]
    async fn matcher_skips_non_matching_tool_and_deny_is_stable() {
        let mut registry = HookRegistry::new();
        registry.register(functional_hook(
            "deny-read",
            r#"printf '%s' '{"decision":"deny","code":"READ_BLOCKED","message":"policy"}'"#,
            HookRole::Security,
        ));
        let service = HookService::new(registry);
        let input = json!({"command": "pwd"});
        assert_eq!(
            service
                .evaluate_pre_tool(&HookContext::new().with_tool("Bash"), &input)
                .await,
            PreHookDecision::Continue {
                input: input.clone()
            }
        );
        assert_eq!(
            service
                .evaluate_pre_tool(
                    &HookContext::new().with_tool("Read"),
                    &json!({"path": "README.md"}),
                )
                .await,
            PreHookDecision::Deny {
                code: "READ_BLOCKED".to_owned(),
                message: "policy".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn security_failure_closes_while_notification_failure_is_isolated() {
        let mut security = HookRegistry::new();
        security.register(functional_hook("security", "exit 7", HookRole::Security));
        assert!(matches!(
            HookService::new(security)
                .evaluate_pre_tool(
                    &HookContext::new().with_tool("Read"),
                    &json!({"path": "README.md"}),
                )
                .await,
            PreHookDecision::Deny { code, .. } if code == "HOOK_SECURITY_FAILED"
        ));

        let mut notification = HookRegistry::new();
        notification.register(command_hook("notify", "exit 9", false, 5));
        let input = json!({"path": "README.md"});
        assert_eq!(
            HookService::new(notification)
                .evaluate_pre_tool(&HookContext::new().with_tool("Read"), &input)
                .await,
            PreHookDecision::Continue { input }
        );
    }

    #[tokio::test]
    async fn workspace_hooks_are_isolated_and_reload_on_next_call() {
        let root =
            std::env::temp_dir().join(format!("zkcode-hook-workspace-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".zk")).expect("hook dir");
        let write_config = |path: &str| {
            std::fs::write(
                root.join(".zk/hooks.toml"),
                format!(
                    r#"
[[hook]]
name = "workspace-transform"
event = "pre-tool-execution"
role = "transform"
matcher = "^Read$"
priority = 1
command = '''printf '%s' '{{"decision":"continue","input":{{"path":"{path}"}}}}' '''
"#
                ),
            )
            .expect("write hook config");
        };
        let service = HookService::disabled();
        let context = HookContext::new()
            .with_tool("Read")
            .with_working_dir(root.to_string_lossy());
        write_config("first.txt");
        assert_eq!(
            service
                .evaluate_pre_tool(&context, &json!({"path": "before"}))
                .await,
            PreHookDecision::Continue {
                input: json!({"path": "first.txt"})
            }
        );
        write_config("second.txt");
        assert_eq!(
            service
                .evaluate_pre_tool(&context, &json!({"path": "before"}))
                .await,
            PreHookDecision::Continue {
                input: json!({"path": "second.txt"})
            }
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn result_preview_truncates_on_char_boundary() {
        let long = "é".repeat(RESULT_PREVIEW_LIMIT + 100);
        let ctx = HookContext::new().with_result_preview(long);
        let preview = ctx.result_preview.expect("preview present");
        assert_eq!(preview.chars().count(), RESULT_PREVIEW_LIMIT);
    }
}
