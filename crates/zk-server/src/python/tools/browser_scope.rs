//! Temporary interactive browser contexts owned by a real Run setup invocation.
use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use base64::Engine as _;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use zk_db::{Db, content::ContentRetention};
use zk_tools::{
    EVIDENCE_RECEIPT_SCHEMA_VERSION, EvidenceReceipt, EvidenceReceiptItem, EvidenceReceiptVerdict,
    ExecutionResourceLease, ExecutionResourceTerminal, RunToolScope, RunToolScopeFactory, Tool,
    ToolContext, ToolOutput, ToolRegistry,
};

use super::{BROWSER_AUTOMATION, PythonEnvelope, WebBrowserTool, failure};
use crate::python::{Correlation, PythonClient};

/// Adapts the already enabled browser capability; preparation does not start it.
pub struct BrowserRunScopeFactory {
    client: Arc<PythonClient>,
    db: Db,
}
impl fmt::Debug for BrowserRunScopeFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrowserRunScopeFactory")
            .finish_non_exhaustive()
    }
}
impl BrowserRunScopeFactory {
    #[must_use]
    /// Adapt temporary browser calls to one supervised Run-owned sidecar scope.
    pub fn new(client: Arc<PythonClient>, db: Db) -> Self {
        Self { client, db }
    }
}
struct Slot {
    id: String,
    lease: ExecutionResourceLease,
    created: bool,
}
struct Manager {
    context: ToolContext,
    client: Arc<PythonClient>,
    db: Db,
    // Serializes startup, actions and teardown; aliases never leave this Run.
    slots: Mutex<BTreeMap<String, Slot>>,
}
struct Scope {
    directory: Arc<ToolRegistry>,
    manager: Option<Arc<Manager>>,
}
impl RunToolScope for Scope {
    fn registry(&self) -> Arc<ToolRegistry> {
        self.directory.clone()
    }
    fn cleanup(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async {
            match &self.manager {
                Some(manager) => manager.cleanup().await,
                None => Ok(()),
            }
        })
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(manager) = &self.manager {
            manager.context.cancel.cancel();
            let manager = manager.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = manager.cleanup().await;
                });
            }
        }
    }
}
impl RunToolScopeFactory for BrowserRunScopeFactory {
    fn prepare(
        &self,
        mut context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        Box::pin(async move {
            let binding = base.resolve("WebBrowser");
            if binding.is_none() {
                return Ok(Arc::new(Scope {
                    directory: base,
                    manager: None,
                }) as Arc<dyn RunToolScope>);
            }
            if !context.is_ephemeral() {
                return super::browser_session_scope::prepare(
                    context,
                    base,
                    self.client.clone(),
                    self.db.clone(),
                )
                .await;
            }
            let binding = binding.expect("checked binding");
            if context.execution_resource_owner().is_none() || context.run_id().is_none() {
                return Err("BROWSER_SCOPE_OWNER_REQUIRED".into());
            }
            let session = context.session_id().ok_or("BROWSER_SESSION_REQUIRED")?;
            if self
                .db
                .session_retention(session)
                .await
                .map_err(|_| "BROWSER_RETENTION_UNAVAILABLE")?
                != ContentRetention::Ephemeral
            {
                return Err("BROWSER_RETENTION_MISMATCH".into());
            }
            context = context.fork_execution_resource_tracking();
            context.cancel = context.cancel.child_token();
            let manager = Arc::new(Manager {
                context,
                client: self.client.clone(),
                db: self.db.clone(),
                slots: Mutex::new(BTreeMap::new()),
            });
            let tool = Arc::new(OwnedBrowserTool {
                source: binding.tool(),
                manager: manager.clone(),
            });
            let directory = Arc::new(ToolRegistry::adapt_bound(base, binding, tool)?);
            Ok(Arc::new(Scope {
                directory,
                manager: Some(manager),
            }) as Arc<dyn RunToolScope>)
        })
    }
}
struct OwnedBrowserTool {
    source: Arc<dyn Tool>,
    manager: Arc<Manager>,
}
impl Tool for OwnedBrowserTool {
    fn name(&self) -> &'static str {
        "WebBrowser"
    }
    fn description(&self) -> &str {
        self.source.description()
    }
    fn parameters(&self) -> Value {
        self.source.parameters()
    }
    fn timeout(&self) -> Duration {
        self.source.timeout()
    }
    fn child_access(&self) -> zk_tools::ChildToolAccess {
        self.source.child_access()
    }
    fn produces_machine_evidence(&self) -> bool {
        true
    }
    fn execute(&self, input: Value, context: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.manager.invoke(input, context).await })
    }
}
impl Manager {
    async fn close_slot(&self, slot: &Slot) -> Result<(), String> {
        let response: Option<PythonEnvelope> = self
            .client
            .call_if_available_with_timeout(
                BROWSER_AUTOMATION,
                "/api/browser/close_session",
                &json!({"session_id":slot.id,"ephemeral_content":true}),
                &Correlation::for_session(self.context.session_id()),
                Duration::from_secs(8),
            )
            .await;
        let confirmed = response.is_some_and(|response| response.success);
        let persisted = self
            .context
            .finish_execution_resource(
                slot.lease.clone(),
                if confirmed {
                    ExecutionResourceTerminal::Released
                } else {
                    ExecutionResourceTerminal::Unconfirmed
                },
            )
            .await;
        if let Err(error) = persisted {
            if !confirmed {
                return Err(error);
            }
            self.context
                .reconcile_execution_resource(&slot.lease, slot.id.clone())
                .await?;
        }
        if confirmed {
            Ok(())
        } else {
            Err("BROWSER_CLEANUP_UNCONFIRMED".into())
        }
    }
    async fn cleanup(&self) -> Result<(), String> {
        self.context.cancel.cancel();
        let mut slots = self.slots.lock().await;
        let aliases = slots.keys().cloned().collect::<Vec<_>>();
        let mut failed = false;
        for alias in aliases {
            if self.close_slot(&slots[&alias]).await.is_ok() {
                slots.remove(&alias);
            } else {
                failed = true;
            }
        }
        if failed {
            Err("BROWSER_CLEANUP_UNCONFIRMED".into())
        } else {
            Ok(())
        }
    }
    async fn invoke(&self, input: Value, context: ToolContext) -> ToolOutput {
        if context.session_id() != self.context.session_id()
            || context.run_id() != self.context.run_id()
            || !context.is_ephemeral()
        {
            return failure("BROWSER_SCOPE_MISMATCH", "Browser belongs to another Run");
        }
        let action = match WebBrowserTool::validate(&input) {
            Ok(action) => action.to_owned(),
            Err(output) => return output,
        };
        let alias = input
            .get("session_id")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .unwrap_or("default");
        if alias.len() > 128 {
            return failure("BROWSER_ALIAS_INVALID", "Browser alias exceeds 128 bytes");
        }
        let mut slots = self.slots.lock().await;
        if self.context.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return failure("BROWSER_CANCELLED", "Run was stopped");
        }
        if action == "close_session" {
            return match slots.get(alias) {
                None => ToolOutput::ok("{\"closed\":false}"),
                Some(slot) => match self.close_slot(slot).await {
                    Ok(()) => {
                        slots.remove(alias);
                        ToolOutput::ok("{\"closed\":true}")
                    }
                    Err(error) => ToolOutput::error(error),
                },
            };
        }
        if !slots.contains_key(alias)
            && let Err(output) = self.create_slot(&mut slots, alias, &action, &context).await
        {
            return output;
        }
        let slot = &slots[alias];
        if !slot.created {
            return failure(
                "BROWSER_CLEANUP_PENDING",
                "Previous browser creation needs cleanup",
            );
        }
        let body = json!({"session_id":slot.id,"ephemeral_content":true,"action":action,"parameters":input});
        let timeout = input
            .get("timeout")
            .and_then(Value::as_u64)
            .unwrap_or(30_000)
            .clamp(1, 600_000);
        let correlation = Correlation::for_session(context.session_id());
        let response: Option<PythonEnvelope> = tokio::select! {
            biased;
            () = context.cancel.cancelled() => None,
            () = self.context.cancel.cancelled() => None,
            result = self.client.call_if_available_with_timeout(BROWSER_AUTOMATION,"/api/browser/owned/action",&body,&correlation, Duration::from_millis(timeout).saturating_add(Duration::from_secs(3))) => result,
        };
        let Some(response) = response else {
            if self.close_slot(slot).await.is_ok() {
                slots.remove(alias);
            }
            let mut output = failure(
                "BROWSER_ACTION_UNCONFIRMED",
                "The action may have taken effect; inspect state before any retry",
            );
            output.metadata = Some(json!({"retryability":"NEVER","effectState":"UNKNOWN"}));
            return output;
        };
        if !response.success {
            return super::web_browser::browser_failure(&action, &response);
        }
        let data = response.data.unwrap_or(Value::Null);
        drop(slots);
        if matches!(action.as_str(), "screenshot" | "snapshot-semantic") {
            self.observation(&action, data, &context).await
        } else {
            let encoded = data.to_string();
            if encoded.len() > 1024 * 1024 {
                failure(
                    "BROWSER_OUTPUT_TOO_LARGE",
                    "Browser result exceeds the 1 MiB response limit",
                )
            } else {
                ToolOutput::ok(encoded)
            }
        }
    }
    async fn create_slot(
        &self,
        slots: &mut BTreeMap<String, Slot>,
        alias: &str,
        action: &str,
        context: &ToolContext,
    ) -> Result<(), ToolOutput> {
        if action != "navigate" {
            return Err(failure(
                "BROWSER_SESSION_NOT_FOUND",
                "Navigate first in this Run",
            ));
        }
        if slots.len() >= 3 {
            return Err(failure(
                "BROWSER_RUN_CAPACITY",
                "Close a browser context before creating another",
            ));
        }
        let id = format!("owned-{}", uuid::Uuid::new_v4());
        let Ok(Some(lease)) = self
            .context
            .register_execution_resource(
                "stream",
                Some(id.clone()),
                json!({"kind":"browserSession","sidecarSessionId":id}),
            )
            .await
        else {
            return Err(failure(
                "BROWSER_RESOURCE_RESERVATION_FAILED",
                "No browser was started",
            ));
        };
        slots.insert(
            alias.to_owned(),
            Slot {
                id,
                lease,
                created: false,
            },
        );
        let slot = &slots[alias];
        let mut deadline = crate::iso::now_millis() + 30_000;
        if let Some(owner) = self.context.execution_resource_owner() {
            match self.db.read_task_budget(&owner.task_id).await {
                Ok(Some(budget)) => {
                    if let Some(root_deadline) = budget.deadline_at_ms {
                        deadline = deadline.min(root_deadline);
                    }
                }
                Ok(None) => {}
                Err(_) => {
                    return Err(failure(
                        "BROWSER_BUDGET_UNAVAILABLE",
                        "Browser deadline cannot be verified",
                    ));
                }
            }
        }
        let response: Option<PythonEnvelope> = self
            .client
            .call_if_available_with_timeout(
                BROWSER_AUTOMATION,
                "/api/browser/owned/create",
                &json!({"session_id":slot.id,"ephemeral_content":true,"deadline_epoch_ms":deadline}),
                &Correlation::for_session(context.session_id()),
                Duration::from_secs(30),
            )
            .await;
        if !response.is_some_and(|response| response.success) {
            let slot = &slots[alias];
            if self.close_slot(slot).await.is_ok() {
                slots.remove(alias);
            }
            return Err(failure(
                "BROWSER_START_UNCONFIRMED",
                "Browser creation did not confirm success; no action was executed",
            ));
        }
        slots.get_mut(alias).expect("inserted").created = true;
        Ok(())
    }

    async fn observation(
        &self,
        action: &str,
        mut data: Value,
        context: &ToolContext,
    ) -> ToolOutput {
        let session = context.session_id().expect("validated context");
        let bytes = if action == "screenshot" {
            let Some(encoded) = data.get("screenshot_base64").and_then(Value::as_str) else {
                return failure(
                    "BROWSER_SCREENSHOT_MISSING",
                    "Browser returned no screenshot",
                );
            };
            if encoded.len() > 14 * 1024 * 1024 {
                return failure("BROWSER_SCREENSHOT_TOO_LARGE", "Screenshot exceeds budget");
            }
            match base64::engine::general_purpose::STANDARD.decode(encoded) {
                Ok(bytes) if bytes.starts_with(b"\x89PNG\r\n\x1a\n") => bytes,
                _ => {
                    return failure(
                        "BROWSER_SCREENSHOT_INVALID",
                        "Browser returned invalid PNG bytes",
                    );
                }
            }
        } else {
            if let Some(object) = data.as_object_mut() {
                object.remove("screenshot_base64");
            }
            data.to_string().into_bytes()
        };
        let Ok(hash) = crate::api::evidence::store_blob(
            &self.db,
            session,
            context.working_dir().to_owned(),
            bytes,
        )
        .await
        else {
            return failure(
                "BROWSER_EVIDENCE_STORE_FAILED",
                "Capture completed but could not be retained; no automatic retry",
            );
        };
        let receipt = EvidenceReceipt {
            schema_version: EVIDENCE_RECEIPT_SCHEMA_VERSION,
            kind: "browser_observation".into(),
            claim: None,
            verdict: EvidenceReceiptVerdict::Inconclusive,
            observed_at: crate::iso::format_rfc3339_micros(crate::iso::now_millis()),
            items: vec![EvidenceReceiptItem {
                item_type: action.into(),
                summary: Some(format!("Browser {action} captured")),
                blob_sha256: Some(hash.clone()),
                meta: Some(json!({"format":if action=="screenshot" {"png"}else{"json"}})),
                sort_order: 0,
            }],
        };
        ToolOutput {
            content: format!("Browser {action} retained in this temporary session"),
            is_error: false,
            metadata: Some(
                json!({"structuredResult":{"evidence":receipt,"screenshot_sha256":hash,"ephemeral":true}}),
            ),
        }
    }
}
