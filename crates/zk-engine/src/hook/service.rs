//! External hook orchestration with truthful execution and bounded output.
//!
//! Transform output is untrusted and always re-enters normal admission. Security
//! hooks cannot mutate input and fail closed. POST text affects only UI metadata;
//! Stop may request one ordinary counted correction at a natural answer boundary.
//! Ephemeral bodies never leave through hook commands or HTTP notifications.
//! Commands receive JSON on stdin and a small explicit environment; an owned
//! supervisor confirms process-group cleanup even after caller cancellation.
//!
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use super::event::{HookConfig, HookEvent, HookRole};
use super::http_executor::{HttpHookError, HttpHookExecutor};
use super::registry::HookRegistry;
use crate::observability::{NoopObservabilityRecorder, ObservabilityEvent, ObservabilityRecorder};

/// Trusted per-call ceiling supplied by the host for external executions.
/// This type has no wire deserializer and is never read from tool arguments.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExternalHookPolicy {
    /// Explicit local write capability approval.
    pub write: bool,
    /// Explicit local arbitrary-process capability approval.
    pub process: bool,
    /// Explicit local network capability approval.
    pub network: bool,
}

#[derive(Clone)]
struct HookExecution {
    context: zk_tools::ToolContext,
    supervisor: zk_tools::ToolExecutor,
}
impl std::fmt::Debug for HookExecution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HookExecution(owned)")
    }
}

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
    /// Ephemeral conversation bodies must not be sent to external hooks.
    pub ephemeral_content: bool,
    /// Additional external connection limits, independent of workspace Hook configuration.
    pub external_policy: Option<ExternalHookPolicy>,
    /// Owning execution cancellation; never serialized into the hook payload.
    pub cancellation: Option<tokio_util::sync::CancellationToken>,
    execution: Option<HookExecution>,
    ownership_required: bool,
    permission_interaction_disabled: bool,
    admission: Option<Arc<dyn super::HookAdmission>>,
}

impl HookContext {
    /// Reverse transports without a local interaction channel cannot request new grants.
    #[must_use]
    pub fn without_permission_interaction(mut self) -> Self {
        self.permission_interaction_disabled = true;
        self
    }

    /// Whether this host transport permits local permission interactions.
    #[must_use]
    pub fn permission_interaction_disabled(&self) -> bool {
        self.permission_interaction_disabled
    }

    /// Current persisted execution owner. This identity is never taken from Hook input.
    #[must_use]
    pub fn execution_run_id(&self) -> Option<&str> {
        self.execution
            .as_ref()
            .and_then(|owner| owner.context.run_id())
    }

    async fn admit(
        &self,
        hook: &HookConfig,
        event: HookEvent,
    ) -> Result<Box<dyn super::HookStartPermit>, String> {
        self.ensure_execution_allowed(hook).map_err(str::to_owned)?;
        let permit = self
            .admission
            .as_ref()
            .ok_or("HOOK_ADMISSION_UNAVAILABLE")?
            .admit(hook, event, self)
            .await?;
        self.ensure_execution_allowed(hook).map_err(str::to_owned)?;
        Ok(permit)
    }

    async fn recheck_start(
        &self,
        permit: &dyn super::HookStartPermit,
        hook: &HookConfig,
        event: HookEvent,
    ) -> Result<(), String> {
        self.ensure_execution_allowed(hook).map_err(str::to_owned)?;
        permit.recheck(hook, event, self).await?;
        self.ensure_execution_allowed(hook).map_err(str::to_owned)
    }

    #[cfg(test)]
    fn test_authorized(mut self) -> Self {
        self.admission = Some(Arc::new(super::admission::TestHookAdmission));
        self
    }

    /// Bind the host-approved capability snapshot to every Hook phase of this call.
    #[must_use]
    pub fn with_external_policy(mut self, policy: Option<ExternalHookPolicy>) -> Self {
        self.external_policy = policy;
        self
    }

    /// Bind a genuine host execution owner; this is never part of a wire payload.
    #[must_use]
    pub(crate) fn with_execution(
        mut self,
        context: zk_tools::ToolContext,
        supervisor: zk_tools::ToolExecutor,
    ) -> Self {
        self.execution = Some(HookExecution {
            context,
            supervisor,
        });
        self.ownership_required = true;
        self
    }

    #[must_use]
    pub(crate) fn require_execution_owner(mut self) -> Self {
        self.ownership_required = true;
        self
    }

    fn ensure_execution_allowed(&self, hook: &HookConfig) -> Result<(), &'static str> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
        {
            return Err("HOOK_CALL_CANCELLED");
        }
        if self.ephemeral_content {
            return Err("HOOK_EPHEMERAL_EXTERNAL_UNSUPPORTED");
        }
        if self.ownership_required && self.execution.is_none() {
            return Err("HOOK_EXECUTION_OWNER_UNAVAILABLE");
        }
        if let Some(policy) = self.external_policy {
            let permitted = if hook.is_http() {
                policy.network
            } else {
                policy.write && policy.process && policy.network
            };
            if !permitted {
                return Err("HOOK_EXTERNAL_CAPABILITY_DENIED");
            }
        }
        Ok(())
    }

    /// 空上下文。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Select body retention before any external execution.
    #[must_use]
    pub fn with_ephemeral_content(mut self, value: bool) -> Self {
        self.ephemeral_content = value;
        self
    }
    /// Bind external hook processes to the owning run without cancelling the parent.
    #[must_use]
    pub fn with_cancellation(mut self, token: &tokio_util::sync::CancellationToken) -> Self {
        self.cancellation = Some(token.clone());
        self
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

/// A Stop hook may only influence a natural final answer, never cancellation,
/// deadlines, budget exhaustion, or permission revocation.
#[derive(Debug, PartialEq)]
pub enum StopHookDecision {
    /// Accept the current answer.
    Accept,
    /// Ask for a bounded correction with ordinary authorization and accounting.
    Correct(String),
    /// Explicitly prevent any further continuation.
    Prevent,
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
    admission: Option<Arc<dyn super::HookAdmission>>,
    recorder: Arc<dyn ObservabilityRecorder>,
    snapshots: Mutex<HashMap<String, HookRegistry>>,
    pending: Mutex<HashMap<String, Vec<tokio::sync::oneshot::Receiver<()>>>>,
}

impl HookService {
    fn registry_for_context(&self, context: &HookContext) -> HookRegistry {
        let Some(root) = context
            .working_dir
            .as_deref()
            .map(Path::new)
            .filter(|root| root.is_absolute())
        else {
            return self.registry.clone();
        };
        let key = root.to_string_lossy().into_owned();
        if let Ok(registry) = HookRegistry::try_load_from_dir(root) {
            self.snapshots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, registry.clone());
            registry
        } else {
            tracing::error!(
                error_code = "HOOK_CONFIG_INVALID",
                "hook replacement rejected; retaining last valid configuration"
            );
            self.snapshots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&key)
                .cloned()
                .unwrap_or_else(|| HookRegistry::load_from_dir(root))
        }
    }

    /// 以给定注册表构造。
    #[must_use]
    pub fn new(registry: HookRegistry) -> Self {
        Self {
            registry,
            admission: None,
            recorder: Arc::new(NoopObservabilityRecorder),
            snapshots: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Install the host's permission authority. Absent authority fails closed.
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<dyn super::HookAdmission>) -> Self {
        self.admission = Some(admission);
        self
    }

    fn authorized_context(&self, context: &HookContext) -> HookContext {
        let mut context = context.clone();
        // Freeze a physical working root before declaration loading and approval;
        // changing a caller's symlink alias must not redirect an approved command.
        if let Some(root) = context.working_dir.as_deref()
            && let Ok(physical) = Path::new(root).canonicalize()
        {
            context.working_dir = Some(physical.to_string_lossy().into_owned());
        }
        if let Some(admission) = &self.admission {
            context.admission = Some(Arc::clone(admission));
        }
        context
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

    /// Whether these host phases have configured work or an invalid required gate.
    /// This is only an allocation hint; every actual execution rechecks configuration.
    #[must_use]
    pub fn has_lifecycle_hooks(&self, events: &[HookEvent], context: &HookContext) -> bool {
        let registry = self.registry_for_context(context);
        registry.has_invalid_security_config()
            || events
                .iter()
                .any(|event| !registry.hooks_for(*event).is_empty())
    }

    /// Drain already-dispatched asynchronous notifications before the owning Run closes.
    /// Each notification retains its original asynchronous execution while the Run lives.
    pub async fn drain_run(&self, run_id: &str) {
        loop {
            let pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(run_id);
            let Some(pending) = pending else {
                return;
            };
            for completed in pending {
                let _ = completed.await;
            }
        }
    }

    /// 触发某事件下的全部 hook（外部通知，错误隔离）。
    ///
    /// 同步 hook（`async_mode == false`）按声明顺序**等待完成**（各自受
    /// `timeout_secs` 约束）；异步 hook 经 `tokio::spawn` 派发后**不等待**。
    /// 任一 hook 失败仅 `warn!`，绝不影响调用方主流程。
    pub async fn fire(&self, event: HookEvent, context: &HookContext) {
        let context = &self.authorized_context(context);
        if context.ephemeral_content {
            return;
        }
        let registry = self.registry_for_context(context);
        let hooks = registry.hooks_for(event);
        if hooks.is_empty() {
            return;
        }
        let payload = build_payload(event, context);
        for hook in hooks {
            if !hook.matches_tool(context.tool_name.as_deref().unwrap_or_default()) {
                continue;
            }
            self.notify_hook(hook, event, context, &payload).await;
        }
    }

    async fn notify_hook(
        &self,
        hook: &HookConfig,
        event: HookEvent,
        context: &HookContext,
        payload: &Value,
    ) {
        if hook.async_mode {
            let hook = hook.clone();
            let context = context.clone();
            let payload = payload.clone();
            let recorder = Arc::clone(&self.recorder);
            let owner = context.execution.clone();
            let (completed, pending) = tokio::sync::oneshot::channel();
            if let Some(run_id) = owner.as_ref().and_then(|owner| owner.context.run_id()) {
                self.pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .entry(run_id.to_owned())
                    .or_default()
                    .push(pending);
            }
            let future = Box::pin(async move {
                let started = std::time::Instant::now();
                let outcome = execute_one(&hook, event, &context, &payload).await;
                record_hook_outcome(&recorder, &hook, &context, started, &outcome);
                let _ = completed.send(());
            });
            if let Some(owner) = owner {
                if owner
                    .supervisor
                    .spawn_owned_finalizer(owner.context.cancel.clone(), future)
                    .is_err()
                {
                    tracing::warn!(
                        code = "HOOK_SUPERVISOR_UNAVAILABLE",
                        "asynchronous hook rejected before dispatch"
                    );
                }
            } else {
                tokio::spawn(future);
            }
        } else {
            let started = std::time::Instant::now();
            let outcome = execute_one(hook, event, context, payload).await;
            record_hook_outcome(&self.recorder, hook, context, started, &outcome);
        }
    }

    /// Run a lifecycle event inside its existing owner. Required security Hooks fail closed;
    /// optional notification errors remain observable without changing successful tool facts.
    /// # Errors
    /// Returns a fixed denial when a required Hook is unavailable or rejects the lifecycle.
    pub async fn fire_lifecycle(
        &self,
        event: HookEvent,
        context: &HookContext,
    ) -> Result<(), String> {
        let context = &self.authorized_context(context);
        let registry = self.registry_for_context(context);
        if registry.has_invalid_security_config() {
            return Err("HOOK_SECURITY_CONFIG_INVALID".into());
        }
        for hook in registry.hooks_for(event) {
            if !hook.matches_tool(context.tool_name.as_deref().unwrap_or_default()) {
                continue;
            }
            if hook.fails_closed() {
                context
                    .ensure_execution_allowed(hook)
                    .map_err(str::to_owned)?;
                match evaluate_functional_hook(hook, event, context, &json!({})).await {
                    Ok(PreHookDecision::Continue { input }) if input == json!({}) => {}
                    Ok(PreHookDecision::Deny { code, .. }) => return Err(code),
                    _ => return Err("HOOK_LIFECYCLE_SECURITY_FAILED".into()),
                }
            } else if !context.ephemeral_content {
                self.notify_hook(hook, event, context, &build_payload(event, context))
                    .await;
            }
        }
        Ok(())
    }

    /// Execute matching PRE hooks in priority order.
    ///
    /// Notification/presentation hooks cannot alter input. Transform/security
    /// hooks use a bounded JSON response on stdout. Security failures deny;
    /// ordinary hook failures are logged and isolated.
    pub async fn evaluate_pre_tool(&self, context: &HookContext, input: &Value) -> PreHookDecision {
        self.evaluate_input(HookEvent::PreToolExecution, context, input)
            .await
    }

    /// Apply an explicitly configured user-input hook without replacing the
    /// durable original. The caller stores any transformed text as an untrusted projection.
    pub async fn evaluate_user_prompt(&self, context: &HookContext, text: &str) -> PreHookDecision {
        self.evaluate_input(HookEvent::UserPromptSubmit, context, &json!({"text":text}))
            .await
    }

    async fn evaluate_input(
        &self,
        event: HookEvent,
        context: &HookContext,
        input: &Value,
    ) -> PreHookDecision {
        let context = &self.authorized_context(context);
        let tool_name = context.tool_name.as_deref().unwrap_or_default();
        let registry = self.registry_for_context(context);
        if registry.has_invalid_security_config() {
            return PreHookDecision::Deny {
                code: "HOOK_SECURITY_CONFIG_INVALID".to_owned(),
                message: "security hook configuration is invalid".to_owned(),
            };
        }
        if context.ephemeral_content {
            if registry
                .hooks_for(event)
                .iter()
                .any(|hook| hook.role == HookRole::Security && hook.matches_tool(tool_name))
            {
                return PreHookDecision::Deny { code: "HOOK_EPHEMERAL_SECURITY_UNSUPPORTED".into(), message: "The required external security hook cannot receive ephemeral conversation content".into() };
            }
            return PreHookDecision::Continue {
                input: input.clone(),
            };
        }
        let mut current = input.clone();
        for hook in registry.hooks_for(event) {
            if !hook.matches_tool(tool_name) {
                continue;
            }
            if let Err(code) = context.ensure_execution_allowed(hook) {
                tracing::warn!(code, event = %event, "PRE hook rejected by caller capability policy");
                record_pre_decision(&self.recorder, hook, context, "capabilityDenied", true);
                if hook.fails_closed() {
                    return PreHookDecision::Deny { code: code.into(), message: "Required Hook execution was rejected by the caller capability or cancellation policy".into() };
                }
                continue;
            }
            if matches!(hook.role, HookRole::Notification | HookRole::Presentation) {
                let started = std::time::Instant::now();
                let outcome = execute_one(
                    hook,
                    event,
                    context,
                    &build_input_payload(event, context, &current),
                )
                .await;
                record_hook_outcome(&self.recorder, hook, context, started, &outcome);
                continue;
            }
            let before = current.clone();
            let decision = evaluate_functional_hook(hook, event, context, &current).await;
            match decision {
                Ok(PreHookDecision::Continue { input }) => {
                    if hook.role == HookRole::Security && input != before {
                        return PreHookDecision::Deny {
                            code: "SECURITY_HOOK_ATTEMPTED_INPUT_MUTATION".into(),
                            message: "Security hooks cannot change tool arguments".into(),
                        };
                    }
                    if input != before {
                        record_pre_decision(&self.recorder, hook, context, "modified", false);
                    }
                    current = input;
                }
                Ok(deny @ PreHookDecision::Deny { .. }) => {
                    record_pre_decision(&self.recorder, hook, context, "denied", true);
                    return deny;
                }
                Err(_) if hook.fails_closed() => {
                    tracing::error!(
                        code = "HOOK_SECURITY_FAILED",
                        "security PRE hook failed closed"
                    );
                    record_pre_decision(&self.recorder, hook, context, "error", true);
                    return PreHookDecision::Deny {
                        code: "HOOK_SECURITY_FAILED".to_owned(),
                        message: "security hook could not validate the tool input".to_owned(),
                    };
                }
                Err(_) => {
                    tracing::warn!(code = "HOOK_PRE_FAILED", "PRE hook failure isolated");
                    record_pre_decision(&self.recorder, hook, context, "error", false);
                }
            }
        }
        PreHookDecision::Continue { input: current }
    }
    /// Obtain optional presentation text after the immutable tool result commits.
    /// This text is UI metadata only; it cannot change success, evidence or model context.
    pub async fn post_tool_presentation(&self, context: &HookContext) -> Option<String> {
        let context = &self.authorized_context(context);
        if context.ephemeral_content {
            return None;
        }
        let registry = self.registry_for_context(context);
        let mut presentation = None;
        for hook in registry.hooks_for(HookEvent::PostToolExecution) {
            if !hook.matches_tool(context.tool_name.as_deref().unwrap_or_default()) {
                continue;
            }
            if let Err(code) = context.ensure_execution_allowed(hook) {
                tracing::warn!(
                    code,
                    event = "PostToolExecution",
                    "POST hook rejected by caller capability policy"
                );
                record_pre_decision(&self.recorder, hook, context, "capabilityDenied", true);
                continue;
            }
            if hook.role != HookRole::Presentation {
                let started = std::time::Instant::now();
                let result = execute_one(
                    hook,
                    HookEvent::PostToolExecution,
                    context,
                    &build_payload(HookEvent::PostToolExecution, context),
                )
                .await;
                record_hook_outcome(&self.recorder, hook, context, started, &result);
                continue;
            }
            let mut payload = build_payload(HookEvent::PostToolExecution, context);
            payload["presentation"] = presentation.clone().map_or(Value::Null, Value::String);
            match capture_value(hook, HookEvent::PostToolExecution, context, &payload).await {
                Ok(value) => {
                    if let Some(text) = value["presentation"].as_str() {
                        presentation = Some(truncate_chars(text, RESULT_PREVIEW_LIMIT));
                    }
                }
                Err(_) => {
                    tracing::warn!(
                        code = "HOOK_PRESENTATION_FAILED",
                        "post hook presentation failed; actual tool result preserved"
                    );
                }
            }
        }
        presentation
    }

    /// Evaluate only natural completion, with no body export in ephemeral mode.
    pub async fn evaluate_stop(&self, context: &HookContext) -> StopHookDecision {
        let context = &self.authorized_context(context);
        if context.ephemeral_content {
            return StopHookDecision::Accept;
        }
        let registry = self.registry_for_context(context);
        let mut corrections = Vec::new();
        for hook in registry.hooks_for(HookEvent::Stop) {
            if let Err(code) = context.ensure_execution_allowed(hook) {
                tracing::warn!(
                    code,
                    event = "Stop",
                    "Stop hook rejected by caller capability policy"
                );
                if code == "HOOK_CALL_CANCELLED" {
                    return StopHookDecision::Accept;
                }
                if hook.fails_closed() {
                    return StopHookDecision::Prevent;
                }
                continue;
            }
            if hook.role == HookRole::Notification {
                let started = std::time::Instant::now();
                let result = execute_one(
                    hook,
                    HookEvent::Stop,
                    context,
                    &build_payload(HookEvent::Stop, context),
                )
                .await;
                record_hook_outcome(&self.recorder, hook, context, started, &result);
                continue;
            }
            if let Ok(value) = capture_value(
                hook,
                HookEvent::Stop,
                context,
                &build_payload(HookEvent::Stop, context),
            )
            .await
            {
                match value["decision"].as_str().unwrap_or("continue") {
                    "prevent" => return StopHookDecision::Prevent,
                    "deny" | "correct" => corrections.push(truncate_chars(
                        value["message"]
                            .as_str()
                            .unwrap_or("Review and correct the final answer."),
                        512,
                    )),
                    "continue" => {}
                    _ => tracing::warn!(hook=%hook.name, "invalid Stop hook decision"),
                }
            } else {
                if hook.fails_closed() {
                    return StopHookDecision::Prevent;
                }
                tracing::warn!(
                    code = "HOOK_STOP_FAILED",
                    "Stop hook failed; existing termination policy retained"
                );
            }
        }
        if corrections.is_empty() {
            StopHookDecision::Accept
        } else {
            StopHookDecision::Correct(truncate_chars(
                &corrections.join("\n"),
                RESULT_PREVIEW_LIMIT,
            ))
        }
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

fn build_input_payload(event: HookEvent, context: &HookContext, input: &Value) -> Value {
    let mut payload = build_payload(event, context);
    if let Some(object) = payload.as_object_mut() {
        object.insert("input".to_owned(), input.clone());
    }
    payload
}

async fn capture_value(
    hook: &HookConfig,
    event: HookEvent,
    context: &HookContext,
    payload: &Value,
) -> Result<Value, String> {
    context
        .ensure_execution_allowed(hook)
        .map_err(str::to_owned)?;
    if hook.async_mode || hook.is_http() {
        return Err("functional hooks require a synchronous local command".into());
    }
    let output = run_command_capture(hook, event, context, payload)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&output).map_err(|error| format!("invalid hook response: {error}"))
}

async fn evaluate_functional_hook(
    hook: &HookConfig,
    event: HookEvent,
    context: &HookContext,
    input: &Value,
) -> Result<PreHookDecision, String> {
    context
        .ensure_execution_allowed(hook)
        .map_err(str::to_owned)?;
    if hook.async_mode {
        return Err("functional PRE hook cannot be asynchronous".to_owned());
    }
    if hook.is_http() {
        return Err("functional HTTP PRE hook responses are not enabled".to_owned());
    }
    let stdout = run_command_capture(
        hook,
        event,
        context,
        &build_input_payload(event, context, input),
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
    if let Err(code) = context.ensure_execution_allowed(hook) {
        tracing::warn!(code, event = %event, "external hook execution rejected before side effects");
        return Err(code.into());
    }
    let permit = if hook.is_http() {
        Some(context.admit(hook, event).await?)
    } else {
        None
    };
    let network_lease = if hook.is_http() {
        if let Some(owner) = &context.execution {
            owner
                .context
                .register_execution_resource(
                    "stream",
                    None,
                    json!({"purpose":"hookHttp", "event":event.as_str()}),
                )
                .await
                .map_err(|_| "HOOK_RESOURCE_REGISTER_FAILED".to_owned())?
        } else {
            None
        }
    } else {
        None
    };
    // Registration and physical-start binding each atomically verify the same live Run.
    // A cancelled/closed owner never obtains a network side effect between those stages.
    let start = if let (Some(owner), Some(lease)) = (&context.execution, &network_lease) {
        owner
            .context
            .bind_execution_resource_external(lease, "hook-http".into())
            .await
            .map_err(|_| "HOOK_RESOURCE_BIND_FAILED".to_owned())
            .and_then(|()| {
                context
                    .ensure_execution_allowed(hook)
                    .map_err(str::to_owned)
            })
    } else {
        Ok(())
    };
    let start = match (start, permit) {
        (Ok(()), Some(permit)) => context.recheck_start(permit.as_ref(), hook, event).await,
        (result, _) => result,
    };
    let outcome = if let Err(code) = start {
        Err(code)
    } else if hook.is_http() {
        if let Some(cancel) = &context.cancellation {
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err("hook caller cancelled".into()),
                result = run_http(hook, payload) => result.map_err(|error|error.to_string()),
            }
        } else {
            run_http(hook, payload)
                .await
                .map_err(|error| error.to_string())
        }
    } else {
        run_command(hook, event, context, payload)
            .await
            .map_err(|error| error.to_string())
    };
    if let (Some(owner), Some(lease)) = (&context.execution, network_lease) {
        owner
            .context
            .finish_execution_resource(lease, zk_tools::ExecutionResourceTerminal::Released)
            .await
            .map_err(|_| "HOOK_RESOURCE_RELEASE_UNCONFIRMED".to_owned())?;
    }
    if let Err(error) = outcome {
        tracing::warn!(
            code = "HOOK_NOTIFICATION_FAILED",
            event = %event,
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
    /// The trusted caller policy denies this external effect.
    #[error("{0}")]
    Policy(&'static str),
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

/// A dropped caller still leaves an owned supervisor which confirms whole-group
/// cleanup. The deadline covers stdin, output collection, and process exit.
async fn run_command(
    hook: &HookConfig,
    event: HookEvent,
    context: &HookContext,
    payload: &Value,
) -> Result<(), CommandError> {
    run_command_capture(hook, event, context, payload)
        .await
        .map(|_| ())
}

struct CancelHookOnDrop(tokio_util::sync::CancellationToken);
impl Drop for CancelHookOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn run_command_capture(
    hook: &HookConfig,
    event: HookEvent,
    context: &HookContext,
    payload: &Value,
) -> Result<Vec<u8>, CommandError> {
    let permit = context
        .admit(hook, event)
        .await
        .map_err(CommandError::Wait)?;
    let hook = hook.clone();
    let context = context.clone();
    let bytes =
        serde_json::to_vec(payload).map_err(|error| CommandError::Wait(error.to_string()))?;
    if bytes.len() > 256 * 1024 {
        return Err(CommandError::Wait("hook input exceeds 256KiB".into()));
    }
    let cancelled = context.cancellation.as_ref().map_or_else(
        tokio_util::sync::CancellationToken::new,
        tokio_util::sync::CancellationToken::child_token,
    );
    let guard = CancelHookOnDrop(cancelled.clone());
    let owner = context.execution.clone();
    let (completed, result) = tokio::sync::oneshot::channel();
    let future = Box::pin(async move {
        let outcome = supervise_command(hook, event, context, bytes, cancelled, permit).await;
        let _ = completed.send(outcome);
    });
    if let Some(owner) = owner {
        owner
            .supervisor
            .spawn_owned_finalizer(owner.context.cancel.clone(), future)
            .map_err(|_| CommandError::Policy("HOOK_SUPERVISOR_UNAVAILABLE"))?;
    } else {
        tokio::spawn(future);
    }
    let result = result
        .await
        .map_err(|_| CommandError::Policy("HOOK_OWNER_STOPPED"))?;
    drop(guard);
    result
}

#[allow(
    clippy::too_many_lines,
    reason = "The launch gate, physical process owner and durable cleanup form one lifecycle"
)]
async fn supervise_command(
    hook: HookConfig,
    event: HookEvent,
    context: HookContext,
    bytes: Vec<u8>,
    cancelled: tokio_util::sync::CancellationToken,
    permit: Box<dyn super::HookStartPermit>,
) -> Result<Vec<u8>, CommandError> {
    use tokio::io::AsyncReadExt;
    let mut builder = tokio::process::Command::new("/bin/sh");
    builder
        .args([
            "-p",
            "-c",
            "IFS= read -r gate && [ \"$gate\" = start ] || exit 125; exec /bin/sh -p -c \"$1\"",
            "zk-hook-gate",
        ])
        .arg(hook.command.as_deref().unwrap_or_default())
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("ZK_HOOK_EVENT", event.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    builder.process_group(0);
    if let Some(tool) = &context.tool_name {
        builder.env("ZK_HOOK_TOOL", tool);
    }
    if let Some(session) = &context.session_id {
        builder.env("ZK_HOOK_SESSION", session);
    }
    if let Some(working_dir) = &context.working_dir {
        builder
            .env("ZK_HOOK_WORKING_DIR", working_dir)
            .current_dir(working_dir);
    }
    if cancelled.is_cancelled() {
        return Err(CommandError::Wait("hook caller cancelled".into()));
    }
    let lease = if let Some(owner) = &context.execution {
        owner
            .context
            .register_execution_resource(
                "processGroup",
                None,
                json!({"purpose":"hook", "event":event.as_str()}),
            )
            .await
            .map_err(|_| CommandError::Policy("HOOK_RESOURCE_REGISTER_FAILED"))?
    } else {
        None
    };
    let mut child = match builder.spawn() {
        Ok(child) => child,
        Err(error) => {
            if let (Some(owner), Some(lease)) = (&context.execution, &lease) {
                let _ = owner
                    .context
                    .finish_execution_resource(
                        lease.clone(),
                        zk_tools::ExecutionResourceTerminal::Released,
                    )
                    .await;
            }
            return Err(CommandError::Spawn(error.to_string()));
        }
    };
    let pid = child
        .id()
        .ok_or_else(|| CommandError::Wait("missing process id".into()))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| CommandError::Wait("missing stdin".into()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| CommandError::Wait("missing stdout".into()))?;
    if let (Some(owner), Some(lease)) = (&context.execution, &lease)
        && owner
            .context
            .bind_execution_resource_external(lease, pid.to_string())
            .await
            .is_err()
    {
        let _ = zk_tools::process::terminate_process_group(&mut child, pid).await;
        let _ = owner
            .context
            .finish_execution_resource(
                lease.clone(),
                zk_tools::ExecutionResourceTerminal::Unconfirmed,
            )
            .await;
        return Err(CommandError::Policy("HOOK_RESOURCE_BIND_FAILED"));
    }
    if let Err(error) = context.recheck_start(permit.as_ref(), &hook, event).await {
        let released = zk_tools::process::terminate_process_group(&mut child, pid).await;
        if let (Some(owner), Some(lease)) = (&context.execution, &lease) {
            owner
                .context
                .finish_execution_resource(
                    lease.clone(),
                    if released {
                        zk_tools::ExecutionResourceTerminal::Released
                    } else {
                        zk_tools::ExecutionResourceTerminal::Unconfirmed
                    },
                )
                .await
                .map_err(|_| CommandError::Policy("HOOK_RESOURCE_RELEASE_UNCONFIRMED"))?;
        }
        return Err(CommandError::Wait(error));
    }
    if !cancelled.is_cancelled() && stdin.write_all(b"start\n").await.is_err() {
        let released = zk_tools::process::terminate_process_group(&mut child, pid).await;
        if let (Some(owner), Some(lease)) = (&context.execution, &lease) {
            let _ = owner
                .context
                .finish_execution_resource(
                    lease.clone(),
                    if released {
                        zk_tools::ExecutionResourceTerminal::Released
                    } else {
                        zk_tools::ExecutionResourceTerminal::Unconfirmed
                    },
                )
                .await;
        }
        return Err(CommandError::Policy("HOOK_START_GATE_FAILED"));
    }
    let deadline = Duration::from_secs(hook.timeout_secs.clamp(1, 300));
    let outcome = tokio::select! {
        biased;
        () = cancelled.cancelled() => Err(CommandError::Wait("hook caller cancelled".into())),
        result = tokio::time::timeout(deadline, async {
            let write = async move {
                // A hook may deliberately ignore input; a closed pipe is not an execution failure.
                let _ = stdin.write_all(&bytes).await;
                let _ = stdin.shutdown().await;
                // ChildStdin::shutdown does not close the OS pipe. EOF readers (cat,
                // JSON parsers) must see closure before we wait for process exit.
                drop(stdin);
                Ok::<(), CommandError>(())
            };
            let read = async {
                let mut output = Vec::new();
                stdout.take((PRE_HOOK_OUTPUT_LIMIT + 1) as u64).read_to_end(&mut output).await
                    .map_err(|error|CommandError::Wait(error.to_string()))?;
                if output.len() > PRE_HOOK_OUTPUT_LIMIT { return Err(CommandError::Wait("stdout exceeds 64KiB".into())); }
                Ok(output)
            };
            let wait = async { child.wait().await.map_err(|error|CommandError::Wait(error.to_string())) };
            let ((), output, status) = tokio::try_join!(write, read, wait)?;
            if !status.success() { return Err(CommandError::NonZeroExit(status.to_string())); }
            Ok(output)
        }) => result.unwrap_or(Err(CommandError::Timeout(hook.timeout_secs))),
    };
    // Even a successful shell may leave grandchildren. Success is not reported
    // until the process group is gone; no descendant inherits a detached lifetime.
    let released = zk_tools::process::terminate_process_group(&mut child, pid).await;
    if let (Some(owner), Some(lease)) = (&context.execution, lease) {
        owner
            .context
            .finish_execution_resource(
                lease,
                if released {
                    zk_tools::ExecutionResourceTerminal::Released
                } else {
                    zk_tools::ExecutionResourceTerminal::Unconfirmed
                },
            )
            .await
            .map_err(|_| CommandError::Policy("HOOK_RESOURCE_RELEASE_UNCONFIRMED"))?;
    }
    if !released {
        return Err(CommandError::Wait("HOOK_CLEANUP_UNCONFIRMED".into()));
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_service(registry: HookRegistry) -> HookService {
        HookService::new(registry)
            .with_admission(Arc::new(super::super::admission::TestHookAdmission))
    }

    #[derive(Debug)]
    struct RevokedAtStart(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl super::super::HookAdmission for RevokedAtStart {
        fn admit<'a>(
            &'a self,
            _: &'a HookConfig,
            _: HookEvent,
            _: &'a HookContext,
        ) -> futures::future::BoxFuture<'a, Result<Box<dyn super::super::HookStartPermit>, String>>
        {
            Box::pin(async {
                Ok(Box::new(Self(self.0.clone())) as Box<dyn super::super::HookStartPermit>)
            })
        }
    }
    impl super::super::HookStartPermit for RevokedAtStart {
        fn recheck<'a>(
            &'a self,
            _: &'a HookConfig,
            _: HookEvent,
            _: &'a HookContext,
        ) -> futures::future::BoxFuture<'a, Result<(), String>> {
            Box::pin(async move {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err("TEST_HOOK_START_RECHECK_DENIED".into())
            })
        }
    }

    #[tokio::test]
    async fn physical_start_receipt_is_rechecked_for_both_command_and_http() {
        let root =
            std::env::temp_dir().join(format!("zk-hook-start-gate-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let checks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut context = HookContext::new().with_working_dir(root.to_string_lossy());
        context.admission = Some(std::sync::Arc::new(RevokedAtStart(checks.clone())));
        let command = command_hook("start-gate", "touch marker", false, 3);
        let result = execute_one(&command, HookEvent::RunStart, &context, &json!({})).await;
        assert!(
            result
                .unwrap_err()
                .contains("TEST_HOOK_START_RECHECK_DENIED")
        );
        assert!(!root.join("marker").exists());
        let mut http = command;
        http.url = Some("http://127.0.0.1:1/never-requested".into());
        let result = execute_one(&http, HookEvent::RunStart, &context, &json!({})).await;
        // The receipt refusal, rather than the later HTTP/SSRF parser, wins.
        assert_eq!(result.unwrap_err(), "TEST_HOOK_START_RECHECK_DENIED");
        assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 2);
        std::fs::remove_dir_all(root).unwrap();
    }

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

    #[derive(Debug, Default)]
    struct DenyingAdmission(std::sync::atomic::AtomicUsize);
    impl super::super::HookAdmission for DenyingAdmission {
        fn admit<'a>(
            &'a self,
            _: &'a HookConfig,
            _: HookEvent,
            _: &'a HookContext,
        ) -> futures::future::BoxFuture<'a, Result<Box<dyn super::super::HookStartPermit>, String>>
        {
            Box::pin(async move {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err("HOOK_TEST_DENIED".into())
            })
        }
    }

    #[tokio::test]
    async fn every_notification_event_and_async_dispatch_requires_host_admission() {
        let marker =
            std::env::temp_dir().join(format!("zk-hook-all-events-{}", uuid::Uuid::new_v4()));
        for asynchronous in [false, true] {
            let mut registry = HookRegistry::new();
            for event in HookEvent::ALL {
                let mut hook = command_hook(
                    event.as_str(),
                    &format!("touch '{}'", marker.display()),
                    asynchronous,
                    5,
                );
                hook.event = event;
                registry.register(hook);
            }
            let admission = Arc::new(DenyingAdmission::default());
            let service = HookService::new(registry).with_admission(admission.clone());
            for event in HookEvent::ALL {
                service.fire(event, &HookContext::new()).await;
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                while admission.0.load(std::sync::atomic::Ordering::SeqCst) < HookEvent::ALL.len() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("every dispatched event reached admission");
            assert!(!marker.exists());
        }
    }

    #[tokio::test]
    async fn denied_optional_transforms_preserve_input_and_required_security_fails_closed() {
        for role in [HookRole::Transform, HookRole::Security] {
            let mut registry = HookRegistry::new();
            registry.register(functional_hook("denied", "exit 99", role));
            let service =
                HookService::new(registry).with_admission(Arc::new(DenyingAdmission::default()));
            let input = json!({"file_path":"unchanged"});
            let result = service
                .evaluate_pre_tool(&HookContext::new().with_tool("Read"), &input)
                .await;
            if role == HookRole::Security {
                assert!(matches!(result, PreHookDecision::Deny { .. }));
            } else {
                assert_eq!(result, PreHookDecision::Continue { input });
            }
        }
    }

    #[tokio::test]
    async fn missing_host_admission_cannot_execute_project_command() {
        let marker =
            std::env::temp_dir().join(format!("zk-hook-no-admission-{}", uuid::Uuid::new_v4()));
        let mut registry = HookRegistry::new();
        registry.register(command_hook(
            "unapproved",
            &format!("touch '{}'", marker.display()),
            false,
            5,
        ));
        HookService::new(registry)
            .fire(HookEvent::PreToolExecution, &HookContext::new())
            .await;
        assert!(
            !marker.exists(),
            "a Hook declaration is not execution authorization"
        );
    }

    #[tokio::test]
    async fn fire_with_no_hooks_is_noop() {
        let service = test_service(HookRegistry::new());
        assert!(service.is_empty());
        // 不 panic、不阻塞即通过。
        service
            .fire(HookEvent::RunStart, &HookContext::new().test_authorized())
            .await;
    }

    #[tokio::test]
    async fn sync_command_hook_runs_to_completion() {
        let mut registry = HookRegistry::new();
        registry.register(command_hook("ok", "exit 0", false, 5));
        let service = test_service(registry);
        // 命令成功——fire 返回即代表已等待完成且无 warn 冒泡。
        service
            .fire(
                HookEvent::PreToolExecution,
                &HookContext::new().test_authorized().with_tool("Read"),
            )
            .await;
    }

    #[tokio::test]
    async fn failing_command_hook_is_isolated() {
        let mut registry = HookRegistry::new();
        registry.register(command_hook("boom", "exit 3", false, 5));
        let service = test_service(registry);
        // 非零退出被隔离：fire 正常返回，不 panic、不冒泡。
        service
            .fire(
                HookEvent::PreToolExecution,
                &HookContext::new().test_authorized(),
            )
            .await;
    }

    #[tokio::test]
    async fn timeout_kills_and_isolates() {
        let mut registry = HookRegistry::new();
        registry.register(command_hook("slow", "sleep 10", false, 1));
        let service = test_service(registry);
        let start = std::time::Instant::now();
        service
            .fire(
                HookEvent::PreToolExecution,
                &HookContext::new().test_authorized(),
            )
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
        let service = test_service(registry);
        let decision = service
            .evaluate_pre_tool(
                &HookContext::new().test_authorized().with_tool("Read"),
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
        let service = test_service(registry);
        let input = json!({"command": "pwd"});
        assert_eq!(
            service
                .evaluate_pre_tool(
                    &HookContext::new().test_authorized().with_tool("Bash"),
                    &input
                )
                .await,
            PreHookDecision::Continue {
                input: input.clone()
            }
        );
        assert_eq!(
            service
                .evaluate_pre_tool(
                    &HookContext::new().test_authorized().with_tool("Read"),
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
            test_service(security)
                .evaluate_pre_tool(
                    &HookContext::new().test_authorized().with_tool("Read"),
                    &json!({"path": "README.md"}),
                )
                .await,
            PreHookDecision::Deny { code, .. } if code == "HOOK_SECURITY_FAILED"
        ));

        let mut notification = HookRegistry::new();
        notification.register(command_hook("notify", "exit 9", false, 5));
        let input = json!({"path": "README.md"});
        assert_eq!(
            test_service(notification)
                .evaluate_pre_tool(
                    &HookContext::new().test_authorized().with_tool("Read"),
                    &input
                )
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
        let service = test_service(HookRegistry::new());
        let context = HookContext::new()
            .test_authorized()
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
        let ctx = HookContext::new()
            .test_authorized()
            .with_result_preview(long);
        let preview = ctx.result_preview.expect("preview present");
        assert_eq!(preview.chars().count(), RESULT_PREVIEW_LIMIT);
    }

    #[tokio::test]
    async fn security_is_immutable_and_ephemeral_never_exports_bodies() {
        let mut registry = HookRegistry::new();
        registry.register(functional_hook(
            "illegal-mutation",
            r#"printf '%s' '{"input":{"path":"changed"}}'"#,
            HookRole::Security,
        ));
        let service = test_service(registry);
        let context = HookContext::new().test_authorized().with_tool("Read");
        assert!(
            matches!(service.evaluate_pre_tool(&context, &json!({"path":"original"})).await, PreHookDecision::Deny {code,..} if code=="SECURITY_HOOK_ATTEMPTED_INPUT_MUTATION")
        );
        assert!(
            matches!(service.evaluate_pre_tool(&context.with_ephemeral_content(true), &json!({"path":"secret"})).await, PreHookDecision::Deny {code,..} if code=="HOOK_EPHEMERAL_SECURITY_UNSUPPORTED")
        );
        let mut registry = HookRegistry::new();
        registry.register(functional_hook(
            "must-not-run",
            "exit 9",
            HookRole::Transform,
        ));
        assert_eq!(
            test_service(registry)
                .evaluate_pre_tool(
                    &HookContext::new()
                        .test_authorized()
                        .with_tool("Read")
                        .with_ephemeral_content(true),
                    &json!({"path":"secret"})
                )
                .await,
            PreHookDecision::Continue {
                input: json!({"path":"secret"})
            }
        );
    }

    #[tokio::test]
    async fn malformed_reload_retains_last_valid_security_and_first_load_fails_closed() {
        let root = std::env::temp_dir().join(format!("zk-hook-cache-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".zk")).unwrap();
        let path = root.join(".zk/hooks.toml");
        std::fs::write(&path, "[[hook]]\nname='deny'\nevent='PRE_TOOL_USE'\nrole='security'\ncommand='''printf '%s' '{\"decision\":\"deny\",\"code\":\"DECLARED_POLICY\"}' '''\n").unwrap();
        let service = test_service(HookRegistry::new());
        let context = HookContext::new()
            .test_authorized()
            .with_working_dir(root.to_string_lossy())
            .with_tool("Read");
        assert!(
            matches!(service.evaluate_pre_tool(&context, &json!({})).await, PreHookDecision::Deny{code,..} if code=="DECLARED_POLICY")
        );
        std::fs::write(&path, "[[broken").unwrap();
        assert!(
            matches!(service.evaluate_pre_tool(&context, &json!({})).await, PreHookDecision::Deny{code,..} if code=="DECLARED_POLICY")
        );
        assert!(
            matches!(test_service(HookRegistry::new()).evaluate_pre_tool(&context, &json!({})).await, PreHookDecision::Deny{code,..} if code=="HOOK_SECURITY_CONFIG_INVALID")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn command_output_and_stdin_are_bounded_and_descendants_are_reaped() {
        let context = HookContext::new().test_authorized();
        let oversized = command_hook("large", "/usr/bin/yes x", false, 5);
        let error = run_command_capture(&oversized, HookEvent::RunStart, &context, &json!({}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("64KiB"), "{error}");
        let root = std::env::temp_dir().join(format!("zk-hook-process-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let pid_file = root.join("child.pid");
        let command = format!("sleep 30 & echo $! > '{}'; wait", pid_file.display());
        let hook = command_hook("blocked-input", &command, false, 1);
        let started = std::time::Instant::now();
        let result = run_command_capture(
            &hook,
            HookEvent::RunStart,
            &context,
            &json!({"body":"x".repeat(200_000)}),
        )
        .await;
        assert!(
            matches!(result, Err(CommandError::Timeout(1))),
            "{result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn post_presentation_is_matching_only_and_preserves_the_original() {
        let mut registry = HookRegistry::new();
        let mut hook = functional_hook(
            "display",
            r#"printf '%s' '{"presentation":"display-only"}'"#,
            HookRole::Presentation,
        );
        hook.event = HookEvent::PostToolExecution;
        registry.register(hook);
        let service = test_service(registry);
        assert_eq!(
            service
                .post_tool_presentation(&HookContext::new().test_authorized().with_tool("Bash"))
                .await,
            None
        );
        let context = HookContext::new()
            .test_authorized()
            .with_tool("Read")
            .with_result_preview("actual error");
        assert_eq!(
            service.post_tool_presentation(&context).await.as_deref(),
            Some("display-only")
        );
        assert_eq!(context.result_preview.as_deref(), Some("actual error"));
    }
}
