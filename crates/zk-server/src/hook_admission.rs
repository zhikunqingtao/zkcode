//! Host Hook admission shares the tool permission service and transaction boundary.
use std::{path::Path, sync::Arc};

use futures::future::BoxFuture;
use serde_json::{Value, json};
use zk_authz::{
    PermissionMode,
    tool_facts::{ToolFacts, ToolUseContext},
};
use zk_engine::hook::{
    HookAdmission, HookConfig, HookContext, HookEvent, HookRegistry, HookStartPermit,
};
use zk_tools::Tool;

use crate::authz::AuthzStack;

/// Installed once by the composition root. Never registered in the model directory.
#[derive(Debug)]
pub(crate) struct HostHookAdmission(pub Arc<AuthzStack>);

/// No deserializer, constructor or registry adapter exposes this identity to tools.
struct HostHookFacts(Value);
impl ToolFacts for HostHookFacts {
    fn name(&self) -> &'static str {
        "Hook"
    }
    fn host_hook_facts(&self) -> Option<&Value> {
        Some(&self.0)
    }
    fn is_destructive(&self, input: &Value) -> bool {
        zk_tools::bash::BashTool.is_destructive(input)
    }
}

fn facts(hook: &HookConfig, event: HookEvent, context: &HookContext) -> Result<Value, String> {
    let root = context
        .working_dir
        .as_deref()
        .ok_or("HOOK_ROOT_UNAVAILABLE")?;
    let physical = Path::new(root)
        .canonicalize()
        .map_err(|_| "HOOK_ROOT_UNAVAILABLE")?;
    let current =
        HookRegistry::try_load_from_dir(&physical).map_err(|_| "HOOK_CONFIG_UNAVAILABLE")?;
    if hook.event != event || !current.hooks_for(event).contains(hook) {
        return Err("HOOK_DECLARATION_CHANGED".into());
    }
    let mut declaration = serde_json::to_value(hook).map_err(|_| "HOOK_DECLARATION_INVALID")?;
    if hook.is_http() {
        declaration["command"] = Value::Null;
    } else {
        declaration["url"] = Value::Null;
    }
    Ok(json!({
        "workingRoot":physical,
        "source":physical.join(zk_engine::hook::registry::HOOKS_FILE_REL),
        "declaration":declaration,
        "environment":{
            "PATH":"/usr/bin:/bin", "shell":"/bin/sh", "privilegedShell":true,
            "ZK_HOOK_EVENT":event.as_str(), "ZK_HOOK_TOOL":context.tool_name,
            "ZK_HOOK_SESSION":context.session_id, "ZK_HOOK_WORKING_DIR":context.working_dir,
        }
    }))
}

/// Transport ceilings can restrict confirmation but never bypass PLAN.
fn admission_mode(
    stack: &AuthzStack,
    root_session_id: &str,
    context: &HookContext,
) -> PermissionMode {
    let mode = stack.modes.get_mode(root_session_id);
    if mode == PermissionMode::Plan {
        mode
    } else if context.permission_interaction_disabled() {
        PermissionMode::DontAsk
    } else if context.external_policy.is_some() && mode == PermissionMode::AutoApprove {
        PermissionMode::Default
    } else {
        mode
    }
}

impl HookAdmission for HostHookAdmission {
    fn admit<'a>(
        &'a self,
        hook: &'a HookConfig,
        event: HookEvent,
        context: &'a HookContext,
    ) -> BoxFuture<'a, Result<Box<dyn HookStartPermit>, String>> {
        Box::pin(async move {
            let run_id = context
                .execution_run_id()
                .ok_or("HOOK_EXECUTION_OWNER_UNAVAILABLE")?;
            let session_id = context
                .session_id
                .as_deref()
                .ok_or("HOOK_SESSION_UNAVAILABLE")?;
            let identity = facts(hook, event, context)?;
            let tool = HostHookFacts(identity.clone());
            let input = identity.clone();
            let frozen = self
                .0
                .frozen
                .freeze("Hook", &input)
                .map_err(|_| "HOOK_IDENTITY_INVALID")?;
            let use_context = ToolUseContext::new(
                Some(run_id.into()),
                Some(format!("host-hook-{}", uuid::Uuid::new_v4())),
                Some(session_id.into()),
            )
            .with_shell(
                Some(session_id.into()),
                identity["workingRoot"].as_str().map(str::to_owned),
            );
            let run = self
                .0
                .db
                .find_run_by_id(run_id)
                .await
                .map_err(|_| "HOOK_OWNER_STORE_UNAVAILABLE")?
                .ok_or("HOOK_EXECUTION_OWNER_UNAVAILABLE")?;
            if run.session_id != session_id {
                return Err("HOOK_OWNER_MISMATCH".into());
            }
            let task = self
                .0
                .db
                .find_runtime_task_by_id(&run.task_id)
                .await
                .map_err(|_| "HOOK_OWNER_STORE_UNAVAILABLE")?
                .ok_or("HOOK_EXECUTION_OWNER_UNAVAILABLE")?;
            let deadline = task.deadline_at_ms.ok_or("HOOK_DEADLINE_UNAVAILABLE")?;
            let remaining = u64::try_from(deadline.saturating_sub(zk_db::time::now_millis()))
                .ok()
                .filter(|ms| *ms > 0)
                .ok_or("HOOK_DEADLINE_EXCEEDED")?;
            let mode = admission_mode(&self.0, &task.session_id, context);
            let authorize =
                self.0
                    .authorization
                    .authorize_with_mode(&tool, &frozen, input, &use_context, mode);
            let cancel = context
                .cancellation
                .clone()
                .ok_or("HOOK_EXECUTION_OWNER_UNAVAILABLE")?;
            let allowed = tokio::select! {
                biased;
                () = cancel.cancelled() => return Err("HOOK_CALL_CANCELLED".into()),
                outcome = tokio::time::timeout(std::time::Duration::from_millis(remaining), authorize) => outcome
                    .map_err(|_| "HOOK_DEADLINE_EXCEEDED")?.map_err(|error| error.code)?,
            };
            if facts(hook, event, context)? != identity {
                return Err("HOOK_DECLARATION_CHANGED".into());
            }
            let task_id = task.id;
            let action_task_id = task_id.clone();
            let action: zk_authz::gateway::AdmissionAction = Arc::new(
                move |conn: &rusqlite::Connection| {
                    let permitted = conn.query_row("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1 AND status IN ('running','waitingInteraction','waitingDependencies') AND deadline_at_ms>?2)",
                    rusqlite::params![action_task_id, zk_db::time::now_millis()], |row| row.get::<_, bool>(0)).unwrap_or(false);
                    if permitted && !cancel.is_cancelled() {
                        Ok(())
                    } else {
                        Err(zk_authz::gateway::AdmissionRejection::new(
                            "HOOK_OWNER_ADMISSION_CLOSED",
                            "Hook owner no longer accepts execution",
                        ))
                    }
                },
            );
            self.0
                .gateway
                .admit_with(&tool, &allowed, &use_context, Some(action))
                .await
                .map_err(|error| error.to_string())?;
            if facts(hook, event, context)? != identity {
                return Err("HOOK_DECLARATION_CHANGED".into());
            }
            Ok(Box::new(HostHookStartPermit {
                stack: Arc::clone(&self.0),
                identity,
                allowed,
                context: use_context,
                task_id,
            }) as Box<dyn HookStartPermit>)
        })
    }
}

/// A single admitted launch. It cannot be cloned, deserialized or reused by tools.
struct HostHookStartPermit {
    stack: Arc<AuthzStack>,
    identity: Value,
    allowed: zk_authz::model::AuthorizedOperation,
    context: ToolUseContext,
    task_id: String,
}
impl std::fmt::Debug for HostHookStartPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HostHookStartPermit(admitted)")
    }
}
impl HookStartPermit for HostHookStartPermit {
    fn recheck<'a>(
        &'a self,
        hook: &'a HookConfig,
        event: HookEvent,
        context: &'a HookContext,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.context.current_run_id.as_deref() != context.execution_run_id() {
                return Err("HOOK_OWNER_MISMATCH".into());
            }
            if facts(hook, event, context)? != self.identity {
                return Err("HOOK_DECLARATION_CHANGED".into());
            }
            self.stack
                .authorization
                .final_dynamic_recheck(
                    &HostHookFacts(self.identity.clone()),
                    &self.allowed,
                    &self.context,
                )
                .map_err(|error| error.code)?;
            let authority = Arc::clone(&self.stack.authorization);
            let allowed = self.allowed.clone();
            let use_context = self.context.clone();
            let task_id = self.task_id.clone();
            let cancel = context
                .cancellation
                .clone()
                .ok_or("HOOK_EXECUTION_OWNER_UNAVAILABLE")?;
            self.stack.db.with_writer(move |conn| {
                let tx = conn.transaction()?;
                // This existing recheck validates the same once decision/grant. It
                // neither asks again nor records a second admitted invocation.
                if let Err(error) = authority.final_grant_recheck_in_current_transaction(
                    &tx, &allowed, &use_context,
                ) { return Ok(Err(error.code)); }
                let live: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1 AND status IN ('running','waitingInteraction','waitingDependencies') AND deadline_at_ms>?2)",
                    rusqlite::params![task_id, zk_db::time::now_millis()], |row| row.get(0),
                )?;
                if !live || cancel.is_cancelled() { return Ok(Err("HOOK_OWNER_ADMISSION_CLOSED".into())); }
                tx.commit()?;
                Ok(Ok(()))
            }).await.map_err(|_| "HOOK_AUTHORIZATION_STORE_UNAVAILABLE")??;
            if facts(hook, event, context)? != self.identity {
                return Err("HOOK_DECLARATION_CHANGED".into());
            }
            Ok(())
        })
    }
}
