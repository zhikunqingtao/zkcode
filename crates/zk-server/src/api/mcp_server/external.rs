//! Native external tools share the existing Engine transaction and permission pipeline.
use super::super::mcp_context::{ActiveCall, Context};
use super::{
    AbortWatcher, Admission, AdmissionRequest, AppState, Arc, CancellationToken, EngineAdmission,
    RpcFailure, ToolAdmission, Value, json,
};
use futures::future::BoxFuture;

/// Check both PRE-transformed and final authorized inputs. A candidate ceiling
/// never replaces ordinary permission decisions or the registry's live binding.
struct CeilingAdmission {
    context: Arc<Context>,
    ordinary: EngineAdmission,
}
impl CeilingAdmission {
    fn denied() -> Admission {
        Admission::Denied {
            code: "MCP_CAPABILITY_CEILING".into(),
            message: "This connection has not received the required local capability approval"
                .into(),
        }
    }
}
impl ToolAdmission for CeilingAdmission {
    fn admit<'a>(&'a self, _request: AdmissionRequest<'a>) -> BoxFuture<'a, Admission> {
        Box::pin(async { Self::denied() }) // An unbound name cannot authorize a capability.
    }
    fn admit_bound<'a>(
        &'a self,
        request: AdmissionRequest<'a>,
        tool: Arc<dyn zk_tools::Tool>,
    ) -> BoxFuture<'a, Admission> {
        Box::pin(async move {
            if request.session_id != self.context.session_id
                || request.run_id != self.context.run_id
                || !self
                    .context
                    .allows_input(request.tool_name, tool.as_ref(), request.input)
            {
                return Self::denied();
            }
            let outcome = self.ordinary.admit_bound(request, tool.clone()).await;
            let (Admission::Allow {
                execution_input: input,
            }
            | Admission::AllowWithShellCwd {
                execution_input: input,
                ..
            }) = &outcome
            else {
                return outcome;
            };
            if !self
                .context
                .allows_input(request.tool_name, tool.as_ref(), input)
            {
                return Self::denied();
            }
            outcome
        })
    }
}

pub(super) async fn call(
    state: &AppState,
    active: ActiveCall,
    tools: Arc<zk_tools::ToolRegistry>,
    name: &str,
    input: Value,
    metadata: Option<&serde_json::Map<String, Value>>,
    cancel: CancellationToken,
) -> Result<Value, RpcFailure> {
    let tool = tools
        .get(name)
        .ok_or_else(|| RpcFailure::new(-32601, "Tool not available in this connection"))?;
    if !active.context.allows_input(name, tool.as_ref(), &input) {
        return Err(RpcFailure::new(-32003, "MCP_CAPABILITY_CEILING"));
    }
    let operation_id =
        match metadata.and_then(|meta| meta.get("operationId").or_else(|| meta.get("toolUseId"))) {
            None => uuid::Uuid::new_v4().to_string(),
            Some(Value::String(id))
                if !id.is_empty()
                    && id.len() <= 128
                    && id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte)) =>
            {
                id.clone()
            }
            Some(_) => {
                return Err(RpcFailure::new(
                    -32602,
                    "_meta.operationId must be a nonempty safe identifier of at most 128 bytes",
                ));
            }
        };
    let service = state
        .conversation()
        .ok_or_else(|| RpcFailure::new(-32001, "MCP_ENGINE_UNAVAILABLE"))?;
    let request = zk_engine::ExternalToolCall {
        session_id: active.context.session_id.clone(),
        run_id: active.context.run_id.clone(),
        operation_id: operation_id.clone(),
        name: name.to_owned(),
        input,
        cancel: cancel.clone(),
        hook_policy: active.context.hook_policy(),
        allowed_tools: tools
            .names()
            .into_iter()
            .filter(|name| {
                tools
                    .get(name)
                    .is_some_and(|tool| active.context.allows_tool(name, tool.as_ref()))
            })
            .collect(),
    };
    let admission: Arc<dyn ToolAdmission> = Arc::new(CeilingAdmission {
        context: active.context.clone(),
        ordinary: EngineAdmission::new_external_default(state.authz.clone(), tools),
    });
    let lifetime = active.context.cancel.clone();
    let request_cancel = cancel.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    state.execution_supervisor.spawn_owned_finalizer(cancel,Box::pin(async move {
        let _active=active;
        let _watch=AbortWatcher(tokio::spawn(async move {tokio::select!{()=lifetime.cancelled()=>request_cancel.cancel(),()=request_cancel.cancelled()=>{}}}));
        let result=service.execute_external_bound_tool(request,admission).await.map(|result|json!({
            "content":[{"type":"text","text":result.result.content}],"isError":result.result.is_error,
            "structuredContent":result.result.metadata,
            "_meta":{"operationId":result.operation_id,"toolUseId":result.tool_use_id,"invocationId":result.invocation_id,"messageId":result.result_message_id,"status":result.status,"cleanupStatus":result.cleanup_status.as_db(),"replayed":result.replayed}
        })).map_err(|error|RpcFailure::with_data(-32004,error,json!({"operationId":operation_id,"executionUnconfirmed":true})));
        let _=tx.send(result);
    })).map_err(|_|RpcFailure::new(-32001,"EXECUTION_SUPERVISOR_SHUTTING_DOWN"))?;
    rx.await
        .map_err(|_| RpcFailure::new(-32603, "MCP_TOOL_FINALIZER_FAILED"))?
}
