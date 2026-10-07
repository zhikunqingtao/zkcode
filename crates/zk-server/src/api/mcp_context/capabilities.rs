//! A client can request candidate capabilities; only a durable local-user decision opens them.
use super::{
    ApiError, AppState, Context, Deserialize, HeaderMap, Json, Ordering, Path, State, StatusCode,
    Value, allowed_tool, json,
};
use crate::interaction::service::InteractionCreateSpec;
use serde::Serialize;
use std::sync::atomic::{AtomicU8, AtomicU64};
use zk_authz::interaction::{InteractionStatus, InteractionType};

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Ceiling {
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub process: bool,
    #[serde(default)]
    pub network: bool,
}
impl Ceiling {
    fn bits(self) -> u8 {
        u8::from(self.write) | (u8::from(self.process) << 1) | (u8::from(self.network) << 2)
    }
    fn from_bits(bits: u8) -> Self {
        Self {
            write: bits & 1 != 0,
            process: bits & 2 != 0,
            network: bits & 4 != 0,
        }
    }
}
struct Approval {
    id: String,
    allow_value: String,
    requested: Ceiling,
    epoch: u64,
    version: i64,
}
#[derive(Default)]
pub(super) struct Capabilities {
    enabled: AtomicU8,
    epoch: AtomicU64,
    pending: tokio::sync::Mutex<Option<Approval>>,
}
impl Context {
    pub(crate) fn hook_policy(&self) -> zk_engine::hook::ExternalHookPolicy {
        let ceiling = if self.cancel.is_cancelled() || self.closing.load(Ordering::Acquire) {
            Ceiling::default()
        } else {
            Ceiling::from_bits(self.capabilities.enabled.load(Ordering::Acquire))
        };
        zk_engine::hook::ExternalHookPolicy {
            write: ceiling.write,
            process: ceiling.process,
            network: ceiling.network,
        }
    }

    pub(crate) fn allows_tool(&self, name: &str, tool: &dyn zk_tools::Tool) -> bool {
        // An external MCP with an attractive native name is never a native capability.
        if self.cancel.is_cancelled()
            || self.closing.load(Ordering::Acquire)
            || tool.name() != name
            || tool.mcp_identity().is_some()
        {
            return false;
        }
        if allowed_tool(name) {
            return true;
        }
        let ceiling = Ceiling::from_bits(self.capabilities.enabled.load(Ordering::Acquire));
        match name {
            "Write" | "Edit" | "NotebookEdit" => ceiling.write,
            // Unrestricted native programs can write and use the network. Never
            // pretend the process switch alone creates an OS-level sandbox.
            "Bash" => ceiling.process && ceiling.write && ceiling.network,
            "WebSearch" | "WebFetch" | "VerifyJourney" => ceiling.network,
            _ => false,
        }
    }
    pub(crate) fn allows_input(
        &self,
        name: &str,
        tool: &dyn zk_tools::Tool,
        input: &Value,
    ) -> bool {
        if !self.allows_tool(name, tool) {
            return false;
        }
        if allowed_tool(name) {
            return tool.is_read_only(input);
        }
        // Supplying an existing URL avoids starting user project code. Any
        // explicit/auto-detected preview command has Bash-equivalent effects.
        if name == "VerifyJourney"
            && (input.get("start_command").is_some()
                || input.get("base_url").and_then(Value::as_str).is_none())
        {
            let ceiling = Ceiling::from_bits(self.capabilities.enabled.load(Ordering::Acquire));
            return ceiling.write && ceiling.process && ceiling.network;
        }
        true
    }
    pub(crate) fn ceiling_epoch(&self) -> u64 {
        self.capabilities.epoch.load(Ordering::Acquire)
    }
}
async fn live(state: &AppState, context: &Context) -> Result<(), ApiError> {
    let run = state
        .db
        .find_run_by_id(&context.run_id)
        .await?
        .ok_or_else(ApiError::access_denied)?;
    if context.cancel.is_cancelled()
        || context.closing.load(Ordering::Acquire)
        || run.session_id != context.session_id
        || run.startup_epoch != state.startup_epoch()
        || run.finished_at.is_some()
        || run.requested_exit_reason.is_some()
    {
        return Err(ApiError::access_denied());
    }
    Ok(())
}
pub(crate) async fn refresh(state: &AppState, context: &Context) -> Result<(), ApiError> {
    live(state, context).await?;
    let mut pending = context.capabilities.pending.lock().await;
    let Some(approval) = pending.as_ref() else {
        return Ok(());
    };
    let record = state
        .authz
        .interactions
        .find_by_id(&approval.id)
        .await?
        .ok_or_else(ApiError::access_denied)?;
    if record.status == InteractionStatus::Pending {
        return Ok(());
    }
    let response = record
        .response_json
        .as_deref()
        .and_then(|text| serde_json::from_str::<Value>(text).ok());
    let decided = record
        .decided_at
        .as_deref()
        .and_then(zk_db::time::parse_rfc3339_millis);
    let deadline = record
        .decision_deadline_at
        .as_deref()
        .unwrap_or(&record.delivery_window_ends_at);
    let timely = decided
        .zip(zk_db::time::parse_rfc3339_millis(deadline))
        .is_some_and(|(decided, deadline)| decided < deadline);
    if record.status == InteractionStatus::Answered
        && record.kind == InteractionType::Elicitation
        && record.session_id == context.session_id
        && record.run_id == context.run_id
        && record.version > approval.version
        && timely
        && response.as_ref().and_then(Value::as_str) == Some(approval.allow_value.as_str())
        && context.capabilities.epoch.load(Ordering::Acquire) == approval.epoch
    {
        context
            .capabilities
            .enabled
            .fetch_or(approval.requested.bits(), Ordering::AcqRel);
        context.capabilities.epoch.fetch_add(1, Ordering::AcqRel);
    }
    pending.take();
    Ok(())
}
async fn view(state: &AppState, context: &Context) -> Result<Value, ApiError> {
    refresh(state, context).await?;
    let pending = context.capabilities.pending.lock().await;
    Ok(
        json!({"sessionId":context.session_id,"runId":context.run_id,"ceiling":Ceiling::from_bits(context.capabilities.enabled.load(Ordering::Acquire)),"epoch":context.ceiling_epoch(),"pendingRequestId":pending.as_ref().map(|request|&request.id),"requestedCeiling":pending.as_ref().map(|request|request.requested),"executionAuthorization":"DEFAULT: each tool still requires ordinary admission"}),
    )
}
#[utoipa::path(get,path="/api/mcp/contexts/{runId}/capabilities",tag="mcp",responses((status=200,description="Current candidate ceiling and pending local-user confirmation")))]
pub(crate) async fn get(
    State(state): State<AppState>,
    Path(run): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let context = state.mcp_contexts.authorized(&headers)?;
    if run != context.run_id {
        return Err(ApiError::access_denied());
    }
    Ok(Json(view(&state, &context).await?))
}
#[utoipa::path(post,path="/api/mcp/contexts/{runId}/capabilities/requests",tag="mcp",responses((status=202,description="Candidate capabilities requested; only a persistent user answer can open them")))]
pub(crate) async fn request(
    State(state): State<AppState>,
    Path(run): Path<String>,
    headers: HeaderMap,
    Json(requested): Json<Ceiling>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let context = state.mcp_contexts.authorized(&headers)?;
    if run != context.run_id || requested.bits() == 0 {
        return Err(ApiError::access_denied());
    }
    if requested.process && !(requested.write && requested.network) {
        return Err(ApiError::validation_with_code(
            "MCP_PROCESS_EFFECTS_REQUIRED",
            "Native commands can write and use the network; request all three candidate capabilities explicitly",
        ));
    }
    refresh(&state, &context).await?;
    let mut pending = context.capabilities.pending.lock().await;
    if pending
        .as_ref()
        .is_some_and(|approval| approval.requested == requested)
        || context.capabilities.enabled.load(Ordering::Acquire) & requested.bits()
            == requested.bits()
    {
        drop(pending);
        return Ok((StatusCode::ACCEPTED, Json(view(&state, &context).await?)));
    }
    if pending.is_some() {
        return Err(ApiError::validation_with_code(
            "MCP_CAPABILITY_REQUEST_PENDING",
            "A capability request is awaiting a local user decision",
        ));
    }
    let epoch = context.ceiling_epoch();
    let allow_value = format!("allow-mcp-candidates-{}", uuid::Uuid::new_v4());
    let record=state.authz.interactions.create(InteractionCreateSpec {
        correlation_key:format!("mcp-capabilities:{epoch}:{}",uuid::Uuid::new_v4()),
        session_id:context.session_id.clone(),run_id:Some(context.run_id.clone()),kind:InteractionType::Elicitation,
        prompt:json!({"question":format!("外部 MCP 连接申请开放候选工具：写文件={}，本地程序={}，网络={}。这只开放可申请目录；每个工具仍遵循 DEFAULT 权限确认。程序可能写文件或联网。",requested.write,requested.process,requested.network),"multiSelect":false,"allowFreeText":false,"inputType":"select","options":[{"label":"允许此连接申请这些工具","value":allow_value,"description":"仅当前连接有效；执行仍需逐工具权限检查"},{"label":"保持只读","value":"deny","description":"不增加候选能力"}]}),
        allowed_decisions:vec!["answer".into(),"cancel".into()],scope_options:Vec::new(),source:Some("direct".into()),child_session_id:None,
    }).await.map_err(|_| ApiError::internal())?;
    *pending = Some(Approval {
        id: record.interaction_id,
        allow_value,
        requested,
        epoch,
        version: record.version,
    });
    drop(pending);
    Ok((StatusCode::ACCEPTED, Json(view(&state, &context).await?)))
}

#[cfg(test)]
mod tests {
    use super::{Capabilities, Context, Ordering, Value, json};
    use std::sync::{Arc, atomic::AtomicBool};
    use tokio::sync::{RwLock, Semaphore};
    use tokio_util::sync::CancellationToken;
    use zk_tools::Tool;
    struct Native(&'static str);
    impl Tool for Native {
        fn name(&self) -> &'static str {
            self.0
        }
        fn description(&self) -> &'static str {
            "fixture"
        }
        fn parameters(&self) -> Value {
            json!({})
        }
        fn is_read_only(&self, _: &Value) -> bool {
            self.0 == "Read"
        }
        fn execute(
            &self,
            _: Value,
            _: zk_tools::ToolContext,
        ) -> futures::future::BoxFuture<'_, zk_tools::ToolOutput> {
            Box::pin(async { panic!("ceiling checks must not execute a tool") })
        }
    }
    fn context(bits: u8) -> Context {
        let capabilities = Capabilities::default();
        capabilities.enabled.store(bits, Ordering::Release);
        Context {
            session_id: "session".into(),
            run_id: "run".into(),
            token_hash: [0; 32],
            capabilities,
            cancel: CancellationToken::new(),
            closing: AtomicBool::new(false),
            active: Arc::new(RwLock::new(())),
            last_seen: std::sync::Mutex::new(std::time::Instant::now()),
            _capacity: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        }
    }
    #[test]
    fn network_candidate_cannot_start_a_preview_or_a_general_program() {
        let context = context(4);
        let verify = Native("VerifyJourney");
        assert!(context.allows_input(
            "VerifyJourney",
            &verify,
            &json!({"base_url":"http://127.0.0.1:9000"})
        ));
        for input in [
            json!({}),
            json!({"base_url":"http://127.0.0.1:9000","start_command":"npm start"}),
        ] {
            assert!(!context.allows_input("VerifyJourney", &verify, &input));
        }
        assert!(!context.allows_input("Bash", &Native("Bash"), &json!({"command":"echo ok"})));
        assert!(!context.allows_tool("Read", &Native("Write")));
        context.cancel.cancel();
        assert!(!context.allows_tool("Read", &Native("Read")));
    }
    #[test]
    fn complete_candidate_set_allows_programs_only_until_connection_close() {
        let context = context(7);
        let verify = Native("VerifyJourney");
        assert!(context.allows_input(
            "VerifyJourney",
            &verify,
            &json!({"start_command":"npm start"})
        ));
        assert!(context.allows_input("Bash", &Native("Bash"), &json!({"command":"echo ok"})));
        context.closing.store(true, Ordering::Release);
        assert!(!context.allows_tool("Bash", &Native("Bash")));
    }
}
