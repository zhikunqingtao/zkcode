//! Ordinary browser contexts survive turns; each actual user Run borrows a lease.
use super::{BROWSER_AUTOMATION, PythonEnvelope, failure};
use crate::python::{Correlation, PythonClient};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_tools::{
    ExecutionResourceLease, ExecutionResourceTerminal, RunToolScope, Tool, ToolContext, ToolOutput,
    ToolRegistry,
};
struct Slot {
    id: String,
    generation: Option<String>,
    usage_epoch: String,
    resource: ExecutionResourceLease,
}
struct Manager {
    context: ToolContext,
    client: Arc<PythonClient>,
    deadline: i64,
    slots: Mutex<BTreeMap<String, Slot>>,
    stop: CancellationToken,
}
struct Scope {
    directory: Arc<ToolRegistry>,
    manager: Arc<Manager>,
}
impl RunToolScope for Scope {
    fn registry(&self) -> Arc<ToolRegistry> {
        self.directory.clone()
    }
    fn cleanup(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(self.manager.cleanup())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        self.manager.stop.cancel();
        let manager = self.manager.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = manager.cleanup().await;
            });
        }
    }
}
pub(super) async fn prepare(
    context: ToolContext,
    base: Arc<ToolRegistry>,
    client: Arc<PythonClient>,
    db: Db,
) -> Result<Arc<dyn RunToolScope>, String> {
    let binding = base
        .resolve("WebBrowser")
        .ok_or("BROWSER_SCOPE_BINDING_MISSING")?;
    let owner = context
        .execution_resource_owner()
        .ok_or("BROWSER_SCOPE_OWNER_REQUIRED")?;
    let deadline = db
        .read_task_budget(&owner.task_id)
        .await
        .map_err(|_| "BROWSER_BUDGET_UNAVAILABLE")?
        .and_then(|budget| budget.deadline_at_ms)
        .ok_or("BROWSER_DEADLINE_REQUIRED")?;
    let manager = Arc::new(Manager {
        context: context.fork_execution_resource_tracking(),
        client,
        deadline,
        slots: Mutex::new(BTreeMap::new()),
        stop: CancellationToken::new(),
    });
    let tool = Arc::new(Browser {
        source: binding.tool(),
        manager: manager.clone(),
    });
    let directory = Arc::new(ToolRegistry::adapt_bound(base, binding, tool)?);
    let weak = Arc::downgrade(&manager);
    let stop = manager.stop.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! { biased; () = stop.cancelled() => break, () = tokio::time::sleep(Duration::from_secs(15)) => {} }
            let Some(manager) = weak.upgrade() else {
                break;
            };
            if manager.context.cancel.is_cancelled() || crate::iso::now_millis() >= manager.deadline
            {
                let _ = manager.cleanup().await;
                break;
            }
            let slots = manager.slots.lock().await;
            for slot in slots.values() {
                if slot.generation.is_none() {
                    continue;
                }
                if manager
                    .call_lease(
                        "acquire",
                        &slot.id,
                        slot.generation.as_deref(),
                        &slot.usage_epoch,
                    )
                    .await
                    .is_err()
                {
                    // A lost renewal must not silently recreate context or
                    // continue acting on an unowned physical browser.
                    manager.stop.cancel();
                    break;
                }
            }
        }
    });
    Ok(Arc::new(Scope { directory, manager }))
}
struct Browser {
    source: Arc<dyn Tool>,
    manager: Arc<Manager>,
}
impl Tool for Browser {
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
        self.source.produces_machine_evidence()
    }
    fn execute(&self, mut input: Value, context: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            if context.session_id() != self.manager.context.session_id()
                || context.run_id() != self.manager.context.run_id()
                || context.is_ephemeral()
            {
                return failure(
                    "BROWSER_SCOPE_OWNER_MISMATCH",
                    "Browser scope belongs to another execution",
                );
            }
            if let Err(output) = super::WebBrowserTool::validate(&input) {
                return output;
            }
            if self.manager.stop.is_cancelled() || context.cancel.is_cancelled() {
                return failure("BROWSER_LEASE_EXPIRED", "Browser usage lease has ended");
            }
            let alias = input["session_id"].as_str().unwrap_or("default").to_owned();
            let mut slots = self.manager.slots.lock().await;
            if let Some(slot) = slots.get(&alias)
                && slot.generation.is_none()
            {
                // An unanswered allocation is a cleanup obligation, not a usable
                // browser. Retire that exact usage before reserving another one.
                if let Err(code) = self.manager.release_slot(slot).await {
                    return failure(&code, "Previous browser usage remains unconfirmed");
                }
                slots.remove(&alias);
            }
            if self.manager.stop.is_cancelled() || context.cancel.is_cancelled() {
                return failure("BROWSER_LEASE_EXPIRED", "Browser usage lease has ended");
            }
            if !slots.contains_key(&alias)
                && let Err(output) = self.manager.acquire_slot(&mut slots, &alias).await
            {
                return output;
            }
            let slot = &slots[&alias];
            if self.manager.stop.is_cancelled()
                || context.cancel.is_cancelled()
                || crate::iso::now_millis() >= self.manager.deadline
            {
                return failure(
                    "BROWSER_LEASE_EXPIRED",
                    "Browser usage ended during acquisition",
                );
            }
            let Some(generation) = slot.generation.as_deref() else {
                return failure("BROWSER_LEASE_UNCONFIRMED", "Browser usage is not ready");
            };
            input["session_id"] = json!(slot.id);
            input["strict_session"] = json!(true);
            input["deadline_epoch_ms"] = json!(self.manager.deadline);
            input["managed_lease"] = json!({
                "owner_session_id": context.session_id(),
                "run_id": context.run_id(),
                "host_epoch": slot.usage_epoch,
                "generation": generation,
            });
            // Serialize this Run's operations, not other Sessions. The renewal
            // loop uses the same map and releases it before physical tool I/O.
            if input["action"] == "close_session" {
                let output = self.source.execute(input, context).await;
                if !output.is_error {
                    let slot = slots.remove(&alias).expect("owned browser slot");
                    if let Err(code) = self.manager.release_slot(&slot).await {
                        slots.insert(alias, slot);
                        return failure(&code, "Browser closed but usage release is not confirmed");
                    }
                }
                return output;
            }
            drop(slots);
            self.source.execute(input, context).await
        })
    }
}
impl Manager {
    async fn acquire_slot(
        &self,
        slots: &mut BTreeMap<String, Slot>,
        alias: &str,
    ) -> Result<(), ToolOutput> {
        let id = session_browser_id(self.context.session_id().unwrap_or_default(), alias);
        let Ok(Some(resource)) = self
            .context
            .register_execution_resource(
                "stream",
                Some(id.clone()),
                json!({"kind":"browserUsageLease","sidecarSessionId":id}),
            )
            .await
        else {
            return Err(failure(
                "BROWSER_LEASE_REGISTRATION_FAILED",
                "Browser lease was not registered",
            ));
        };
        let usage_epoch = uuid::Uuid::new_v4().to_string();
        // Install ownership before I/O, including when its reply is lost.
        slots.insert(
            alias.to_owned(),
            Slot {
                id: id.clone(),
                generation: None,
                usage_epoch: usage_epoch.clone(),
                resource,
            },
        );
        let generation = self
            .call_lease("acquire", &id, None, &usage_epoch)
            .await
            .map_err(|code| {
                failure(
                    &code,
                    "Browser usage lease was not confirmed; cleanup ownership retained",
                )
            })?;
        slots.get_mut(alias).expect("reserved slot").generation = Some(generation);
        Ok(())
    }

    async fn call_lease(
        &self,
        action: &str,
        id: &str,
        generation: Option<&str>,
        usage_epoch: &str,
    ) -> Result<String, String> {
        let remaining_ms = self
            .deadline
            .saturating_sub(crate::iso::now_millis())
            .clamp(0, 60_000);
        let remaining =
            f64::from(u32::try_from(remaining_ms).map_err(|_| "BROWSER_LEASE_INVALID")?) / 1000.0;
        if action == "acquire" && remaining <= 0.0 {
            return Err("BROWSER_LEASE_EXPIRED".into());
        }
        let response:Option<PythonEnvelope> = self.client.call_if_available_with_timeout(BROWSER_AUTOMATION,&format!("/api/browser/lease/{action}"),&json!({"session_id":id,"owner_session_id":self.context.session_id(),"run_id":self.context.run_id(),"host_epoch":usage_epoch,"generation":generation,"ttl":remaining.clamp(0.001,60.0),"deadline_epoch_ms":self.deadline}),&Correlation::for_session(self.context.session_id()),Duration::from_secs(if generation.is_some() || action == "release" {5}else{36})).await;
        let response = response.ok_or("BROWSER_LEASE_UNAVAILABLE")?;
        if !response.success {
            return Err(response.code().to_owned());
        }
        if action == "release" {
            return Ok(String::new());
        }
        response
            .data
            .and_then(|value| value["generation"].as_str().map(str::to_owned))
            .filter(|generation| !generation.is_empty())
            .ok_or_else(|| "BROWSER_LEASE_INVALID".into())
    }
    async fn release_slot(&self, slot: &Slot) -> Result<(), String> {
        let released = self
            .call_lease(
                "release",
                &slot.id,
                slot.generation.as_deref(),
                &slot.usage_epoch,
            )
            .await
            .is_ok();
        let persisted = self
            .context
            .finish_execution_resource(
                slot.resource.clone(),
                if released {
                    ExecutionResourceTerminal::Released
                } else {
                    ExecutionResourceTerminal::Unconfirmed
                },
            )
            .await;
        if released && persisted.is_err() {
            return self
                .context
                .reconcile_execution_resource(&slot.resource, slot.id.clone())
                .await;
        }
        persisted?;
        if released {
            Ok(())
        } else {
            Err("BROWSER_LEASE_RELEASE_UNCONFIRMED".into())
        }
    }
    async fn cleanup(&self) -> Result<(), String> {
        self.stop.cancel();
        let mut slots = self.slots.lock().await;
        let mut failed = false;
        for slot in slots.values() {
            if self.release_slot(slot).await.is_err() {
                failed = true;
            }
        }
        if failed {
            Err("BROWSER_LEASE_RELEASE_UNCONFIRMED".into())
        } else {
            slots.clear();
            Ok(())
        }
    }
}

/// Close only idle contexts owned by a Session after host execution admission
/// has been sealed for deletion/merge. Active Run leases are never interrupted.
///
/// # Errors
/// Returns store, busy or cleanup-unconfirmed errors; never assumes absence.
pub async fn close_session_browser_contexts(
    db: &Db,
    client: &PythonClient,
    session: &str,
) -> Result<(), String> {
    let owners = db
        .session_browser_owner_ids_for_deletion(session)
        .await
        .map_err(|_| "BROWSER_OWNER_STORE_FAILED")?;
    for owner in owners {
        close_browser_owner(client, &owner).await?;
    }
    Ok(())
}

async fn close_browser_owner(client: &PythonClient, session: &str) -> Result<(), String> {
    let response: Option<PythonEnvelope> = client
        .call_if_available_with_timeout(
            BROWSER_AUTOMATION,
            "/api/browser/owner/close",
            &json!({"owner_session_id":session}),
            &Correlation::for_session(Some(session)),
            Duration::from_secs(55),
        )
        .await;
    match response {
        Some(response)
            if response.success
                && response
                    .data
                    .as_ref()
                    .is_some_and(|data| data["closed"] == true) =>
        {
            Ok(())
        }
        Some(response) => Err(response.code().to_owned()),
        None => Err("BROWSER_CLEANUP_UNCONFIRMED".into()),
    }
}

/// Stable private context identity for one authorized Session and logical alias.
#[must_use]
pub fn session_browser_id(session: &str, alias: &str) -> String {
    format!(
        "session-{:x}",
        Sha256::digest(format!("{}:{session}{alias}", session.len()).as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post,
    };
    use std::{collections::VecDeque, sync::Mutex as StdMutex};
    use zk_tools::{
        ExecutionResourceAllocation, ExecutionResourceObserver, ExecutionResourceOwner,
    };

    #[derive(Default)]
    struct Resources(StdMutex<Vec<(String, ExecutionResourceTerminal)>>);
    impl ExecutionResourceObserver for Resources {
        fn register(
            &self,
            _: ExecutionResourceOwner,
            allocation: ExecutionResourceAllocation,
        ) -> BoxFuture<'static, Result<ExecutionResourceLease, String>> {
            Box::pin(async move {
                Ok(ExecutionResourceLease {
                    resource_id: allocation.resource_id,
                })
            })
        }
        fn bind_external(
            &self,
            _: ExecutionResourceLease,
            _: String,
        ) -> BoxFuture<'static, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn finish(
            &self,
            lease: ExecutionResourceLease,
            terminal: ExecutionResourceTerminal,
        ) -> BoxFuture<'static, Result<(), String>> {
            self.0.lock().unwrap().push((lease.resource_id, terminal));
            Box::pin(async { Ok(()) })
        }
    }
    #[derive(Default)]
    struct Actions(StdMutex<Vec<Value>>);
    impl Tool for Actions {
        fn name(&self) -> &'static str {
            "WebBrowser"
        }
        fn description(&self) -> &'static str {
            "local fixture"
        }
        fn parameters(&self) -> Value {
            json!({})
        }
        fn execute(&self, input: Value, _: ToolContext) -> BoxFuture<'_, ToolOutput> {
            self.0.lock().unwrap().push(input);
            Box::pin(async { ToolOutput::ok("fixture action") })
        }
    }
    struct Protocol {
        replies: StdMutex<VecDeque<Option<Value>>>,
        calls: StdMutex<Vec<Value>>,
        cancel: Option<CancellationToken>,
    }
    async fn lease(
        State(state): State<Arc<Protocol>>,
        Json(body): Json<Value>,
    ) -> axum::response::Response {
        state.calls.lock().unwrap().push(body);
        let reply = state
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected lease request");
        if let Some(cancel) = &state.cancel {
            cancel.cancel();
        }
        match reply {
            Some(reply) => Json(reply).into_response(),
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "reply lost after admission",
            )
                .into_response(),
        }
    }
    struct Fixture {
        browser: Browser,
        context: ToolContext,
        protocol: Arc<Protocol>,
        actions: Arc<Actions>,
        resources: Arc<Resources>,
        server: tokio::task::JoinHandle<()>,
        socket: std::path::PathBuf,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
            let _ = std::fs::remove_file(&self.socket);
        }
    }
    fn fixture(replies: Vec<Option<Value>>, cancel_on_reply: bool) -> Fixture {
        let socket = std::path::PathBuf::from(format!("/tmp/zk-r3-{}.sock", uuid::Uuid::new_v4()));
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let cancel = CancellationToken::new();
        let protocol = Arc::new(Protocol {
            replies: StdMutex::new(replies.into()),
            calls: StdMutex::new(Vec::new()),
            cancel: cancel_on_reply.then(|| cancel.clone()),
        });
        let router = Router::new()
            .route("/api/browser/lease/acquire", post(lease))
            .route("/api/browser/lease/release", post(lease))
            .with_state(protocol.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let client = Arc::new(PythonClient::new(socket.clone()));
        client.replace_capabilities_for_tests(BTreeMap::from([(
            BROWSER_AUTOMATION.into(),
            crate::python::CapabilityStatus {
                name: "browser".into(),
                available: true,
                reason: None,
            },
        )]));
        let (sender, _) = tokio::sync::mpsc::unbounded_channel();
        let resources = Arc::new(Resources::default());
        let context = ToolContext::new(cancel, sender)
            .with_session_id("session")
            .with_run_id("run")
            .with_execution_resources(
                ExecutionResourceOwner {
                    task_id: "task".into(),
                    run_id: "run".into(),
                    invocation_id: "setup".into(),
                },
                resources.clone(),
            );
        let actions = Arc::new(Actions::default());
        let browser = Browser {
            source: actions.clone(),
            manager: Arc::new(Manager {
                context: context.clone(),
                client,
                deadline: crate::iso::now_millis() + 60_000,
                slots: Mutex::new(BTreeMap::new()),
                stop: CancellationToken::new(),
            }),
        };
        Fixture {
            browser,
            context,
            protocol,
            actions,
            resources,
            server,
            socket,
        }
    }
    fn acquired() -> Value {
        json!({"success":true,"data":{"generation":"generation"}})
    }
    fn rejected() -> Value {
        json!({"success":false,"error_code":"BROWSER_CAPACITY_REACHED","error_message":"full"})
    }
    fn input() -> Value {
        json!({"action":"navigate","url":"https://fixture.invalid"})
    }

    async fn retry_checks(first: Option<Value>) {
        let f = fixture(
            vec![first, Some(json!({"success":true})), Some(acquired())],
            false,
        );
        assert!(f.browser.execute(input(), f.context.clone()).await.is_error);
        assert!(f.actions.0.lock().unwrap().is_empty());
        let result = f.browser.execute(input(), f.context.clone()).await;
        assert!(!result.is_error, "{}", result.content);
        let calls = f.protocol.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            3,
            "failed reservation must be released and reacquired before action"
        );
        assert_eq!(calls[0]["host_epoch"], calls[1]["host_epoch"]);
        assert_ne!(calls[1]["host_epoch"], calls[2]["host_epoch"]);
        let actions = f.actions.0.lock().unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0]["managed_lease"]["generation"], "generation");
        assert_eq!(actions[0]["managed_lease"]["owner_session_id"], "session");
        assert_eq!(
            actions[0]["managed_lease"]["host_epoch"],
            calls[2]["host_epoch"]
        );
        assert_eq!(f.resources.0.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn rejected_acquire_is_not_a_ready_browser_slot() {
        retry_checks(Some(rejected())).await;
    }
    #[tokio::test]
    async fn lost_acquire_reply_is_reconciled_before_retry() {
        retry_checks(None).await;
    }
    #[tokio::test]
    async fn unconfirmed_release_keeps_slot_and_blocks_action() {
        let f = fixture(vec![None, None], false);
        assert!(f.browser.execute(input(), f.context.clone()).await.is_error);
        let first_id = f.browser.manager.slots.lock().await["default"]
            .resource
            .resource_id
            .clone();
        assert!(f.browser.execute(input(), f.context.clone()).await.is_error);
        assert!(f.actions.0.lock().unwrap().is_empty());
        assert_eq!(
            f.browser.manager.slots.lock().await["default"]
                .resource
                .resource_id,
            first_id
        );
    }
    #[tokio::test]
    async fn cancellation_during_acquire_cannot_start_action() {
        let f = fixture(vec![Some(acquired())], true);
        assert!(f.browser.execute(input(), f.context.clone()).await.is_error);
        assert!(f.actions.0.lock().unwrap().is_empty());
        assert_eq!(
            f.browser.manager.slots.lock().await.len(),
            1,
            "cleanup owner retained"
        );
    }

    #[tokio::test]
    async fn confirmed_slot_reuses_lease_and_overwrites_caller_identity() {
        let f = fixture(vec![Some(acquired())], false);
        let mut request = input();
        request["managed_lease"] = json!({"run_id":"caller","generation":"caller"});
        for _ in 0..2 {
            assert!(
                !f.browser
                    .execute(request.clone(), f.context.clone())
                    .await
                    .is_error
            );
        }
        assert_eq!(f.protocol.calls.lock().unwrap().len(), 1);
        let actions = f.actions.0.lock().unwrap();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0]["managed_lease"], actions[1]["managed_lease"]);
        assert_eq!(actions[0]["managed_lease"]["run_id"], "run");
        assert_eq!(actions[0]["managed_lease"]["generation"], "generation");
        assert!(f.resources.0.lock().unwrap().is_empty());
    }
}
