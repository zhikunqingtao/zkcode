//! Run-local interpreter state, supervised resource ownership and bounded protocol.
use crate::{
    ExecutionResourceLease, ExecutionResourceTerminal, RunToolScope, RunToolScopeFactory, Tool,
    ToolContext, ToolOutput, ToolRegistry,
};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    process::Stdio,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};

/// Trusted host adapter. Merely preparing a Run never starts an interpreter.
#[derive(Debug, Default)]
pub struct ReplRunScopeFactory;

/// Internal `TaskRuntime` service adapter for a persistent Session. The host must
/// supply a distinct admitted service Run; ordinary model input cannot select it.
#[derive(Debug)]
pub struct ReplServiceScopeFactory {
    authorized_session: String,
    handle: Arc<OnceLock<ReplServiceHandle>>,
}
impl ReplServiceScopeFactory {
    /// The `TaskRuntime` root Session owns the private service transcript and its aliases.
    #[must_use]
    pub fn new(authorized_session: String) -> Self {
        Self {
            authorized_session,
            handle: Arc::new(OnceLock::new()),
        }
    }

    /// Retrieve the interpreter port only after this service's owned scope is prepared.
    /// # Errors
    /// Returns an explicit setup error before the real service Run is established.
    pub fn prepared_handle(&self) -> Result<ReplServiceHandle, String> {
        self.handle
            .get()
            .cloned()
            .ok_or_else(|| "REPL_SERVICE_NOT_PREPARED".into())
    }
}

/// Physical interpreter operations inside an already admitted native REPL call.
/// This is not a Tool, registry or authorization gateway; the host's `BridgeTool`
/// remains the sole Tool execution admitted by the Engine. The retained manager
/// owns the separate service Run and enforces the original Session namespace.
#[derive(Clone)]
pub struct ReplServiceHandle(Arc<Manager>);
impl std::fmt::Debug for ReplServiceHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReplServiceHandle(owned)")
    }
}
impl ReplServiceHandle {
    /// Execute one authorized native REPL operation while retaining service ownership.
    pub async fn execute_authorized(&self, input: Value, context: ToolContext) -> ToolOutput {
        self.0.invoke(input, context).await
    }
}
struct Peer {
    language: String,
    child: Child,
    pid: u32,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: tokio::task::JoinHandle<()>,
    lease: ExecutionResourceLease,
    terminated: bool,
    created: Instant,
    seen: Instant,
}
struct Manager {
    context: ToolContext,
    peers: Mutex<BTreeMap<String, Peer>>,
    authorized_session: String,
    session_service: bool,
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
impl RunToolScopeFactory for ReplRunScopeFactory {
    fn prepare(
        &self,
        context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        if !context.is_ephemeral() {
            return Box::pin(async move {
                Ok(Arc::new(Scope {
                    directory: base,
                    manager: None,
                }) as Arc<dyn RunToolScope>)
            });
        }
        prepare_scope(context, base, None, None)
    }
}
impl RunToolScopeFactory for ReplServiceScopeFactory {
    fn prepare(
        &self,
        context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        if context.is_ephemeral() {
            return Box::pin(async { Err("REPL_PERSISTENT_SERVICE_REQUIRED".into()) });
        }
        prepare_scope(
            context,
            base,
            Some(self.authorized_session.clone()),
            Some(self.handle.clone()),
        )
    }
}
fn prepare_scope(
    mut context: ToolContext,
    base: Arc<ToolRegistry>,
    service_session: Option<String>,
    service_handle: Option<Arc<OnceLock<ReplServiceHandle>>>,
) -> BoxFuture<'static, Result<Arc<dyn RunToolScope>, String>> {
    Box::pin(async move {
        let Some(binding) = base.resolve("REPL") else {
            return Ok(Arc::new(Scope {
                directory: base,
                manager: None,
            }) as Arc<dyn RunToolScope>);
        };
        if context.execution_resource_owner().is_none()
            || context.session_id().is_none()
            || context.run_id().is_none()
        {
            return Err("REPL_SCOPE_OWNER_REQUIRED".into());
        }
        context = context.fork_execution_resource_tracking();
        context.cancel = context.cancel.child_token();
        let session_service = service_session.is_some();
        let authorized_session = service_session
            .unwrap_or_else(|| context.session_id().expect("checked session").to_owned());
        let manager = Arc::new(Manager {
            context,
            peers: Mutex::new(BTreeMap::new()),
            authorized_session,
            session_service,
        });
        let replacement = Arc::new(OwnedReplTool {
            source: binding.tool(),
            manager: manager.clone(),
        });
        let directory = Arc::new(ToolRegistry::adapt_bound(base, binding, replacement)?);
        if let Some(handle) = service_handle {
            handle
                .set(ReplServiceHandle(manager.clone()))
                .map_err(|_| "REPL_SERVICE_ALREADY_PREPARED")?;
        }
        Ok(Arc::new(Scope {
            directory,
            manager: Some(manager),
        }) as Arc<dyn RunToolScope>)
    })
}

struct OwnedReplTool {
    source: Arc<dyn Tool>,
    manager: Arc<Manager>,
}
impl Tool for OwnedReplTool {
    fn name(&self) -> &'static str {
        "REPL"
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
    fn child_access(&self) -> crate::ChildToolAccess {
        self.source.child_access()
    }
    fn is_destructive(&self, input: &Value) -> bool {
        self.source.is_destructive(input)
    }
    fn execute(&self, input: Value, context: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.manager.invoke(input, context).await })
    }
}
impl Manager {
    async fn finish(&self, peer: &mut Peer) -> Result<(), String> {
        let confirmed = peer.terminated
            || crate::process::terminate_process_group(&mut peer.child, peer.pid).await;
        peer.terminated = confirmed;
        peer.stderr.abort();
        let recorded = self
            .context
            .finish_execution_resource(
                peer.lease.clone(),
                if confirmed {
                    ExecutionResourceTerminal::Released
                } else {
                    ExecutionResourceTerminal::Unconfirmed
                },
            )
            .await;
        if let Err(error) = recorded {
            if !confirmed {
                return Err(error);
            }
            self.context
                .reconcile_execution_resource(&peer.lease, peer.pid.to_string())
                .await?;
        }
        if confirmed {
            Ok(())
        } else {
            Err("REPL_CLEANUP_UNCONFIRMED".into())
        }
    }
    async fn cleanup(&self) -> Result<(), String> {
        self.context.cancel.cancel();
        let mut peers = self.peers.lock().await;
        let ids = peers.keys().cloned().collect::<Vec<_>>();
        let mut failed = false;
        for id in ids {
            if self
                .finish(peers.get_mut(&id).expect("owned peer"))
                .await
                .is_ok()
            {
                peers.remove(&id);
            } else {
                failed = true;
            }
        }
        if failed {
            Err("REPL_CLEANUP_UNCONFIRMED".into())
        } else {
            Ok(())
        }
    }
    async fn invoke(&self, input: Value, context: ToolContext) -> ToolOutput {
        match self.run(input, context).await {
            Ok(output) => output,
            Err(error) => ToolOutput::error(error),
        }
    }
    #[allow(clippy::too_many_lines)] // Interpreter ownership, startup gate and request completion share one lock lifetime.
    async fn run(&self, input: Value, context: ToolContext) -> Result<ToolOutput, String> {
        if context.session_id() != Some(self.authorized_session.as_str())
            || (!self.session_service && context.run_id() != self.context.run_id())
            || context.is_ephemeral() != self.context.is_ephemeral()
        {
            return Err("REPL_SCOPE_MISMATCH".into());
        }
        let code = input
            .get("code")
            .and_then(Value::as_str)
            .filter(|code| !code.is_empty())
            .ok_or("MISSING_PARAMETER: code is required")?;
        if code.len() > 1024 * 1024 {
            return Err("REPL_INPUT_TOO_LARGE".into());
        }
        let language = input
            .get("language")
            .and_then(Value::as_str)
            .unwrap_or("python");
        let (program, args) =
            super::drivers::command(language).ok_or("REPL_OPERATION_UNSUPPORTED")?;
        let canonical = input.get("sessionId").and_then(Value::as_str);
        let alias = input.get("session_id").and_then(Value::as_str);
        if canonical.zip(alias).is_some_and(|(a, b)| a != b) {
            return Err("REPL_SESSION_ID_CONFLICT".into());
        }
        let id = canonical
            .or(alias)
            .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
        if id.is_empty() || id.len() > 128 {
            return Err("REPL_SESSION_ID_INVALID".into());
        }
        let mut peers = self.peers.lock().await;
        if self.context.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return Err("REPL_CANCELLED".into());
        }
        // Preserve the existing three-interpreter LRU and idle/lifetime rules,
        // but require actual release before reclaiming a slot.
        let expired = peers
            .iter()
            .filter(|(_, peer)| {
                peer.seen.elapsed() >= super::IDLE_TIMEOUT
                    || peer.created.elapsed() >= super::SESSION_MAX_LIFETIME
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for expired in expired {
            self.finish(peers.get_mut(&expired).expect("owned peer"))
                .await?;
            peers.remove(&expired);
        }
        if !peers.contains_key(&id) {
            if peers.len() >= super::MAX_CONCURRENT_SESSIONS {
                let oldest = peers
                    .iter()
                    .min_by_key(|(_, peer)| peer.seen)
                    .map(|(id, _)| id.clone())
                    .expect("nonempty peer pool");
                self.finish(peers.get_mut(&oldest).expect("owned peer"))
                    .await?;
                peers.remove(&oldest);
            }
            let lease = self
                .context
                .register_execution_resource(
                    "processGroup",
                    None,
                    json!({"protocol":"repl","language":language}),
                )
                .await?
                .ok_or("REPL_SCOPE_OWNER_REQUIRED")?;
            // The fixed gate cannot execute the driver until the OS process-group identity
            // is durable. User code is not sent until after this ownership handshake.
            let mut command = Command::new("/bin/sh");
            command.args(["-p","-c","ulimit -c 0; IFS= read -r gate && [ \"$gate\" = start ] || exit 125; exec \"$@\"","zk-repl-gate"])
    .arg(program).args(args).current_dir(context.working_dir()).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).process_group(0).kill_on_drop(true)
    .env("PYTHONHISTFILE","/dev/null").env("NODE_REPL_HISTORY","/dev/null").env("IRBRC","/dev/null").env("HISTFILE","/dev/null").env("PYTHONDONTWRITEBYTECODE","1");
            for key in crate::process::SENSITIVE_ENV_VARS.iter().copied().chain([
                "PYTHONSTARTUP",
                "RUBYOPT",
                "NODE_OPTIONS",
                "BASH_ENV",
                "ENV",
            ]) {
                command.env_remove(key);
            }
            let Ok(mut child) = command.spawn() else {
                self.context
                    .finish_execution_resource(lease, ExecutionResourceTerminal::Released)
                    .await?;
                return Err("REPL_INTERPRETER_UNAVAILABLE".into());
            };
            let pid = child.id().ok_or("REPL_PROCESS_ID_MISSING")?;
            let stdin = child.stdin.take().ok_or("REPL_STDIN_MISSING")?;
            let stdout = child.stdout.take().ok_or("REPL_STDOUT_MISSING")?;
            let mut stderr = child.stderr.take().ok_or("REPL_STDERR_MISSING")?;
            let drain = tokio::spawn(async move {
                let mut chunk = [0u8; 8192];
                while stderr.read(&mut chunk).await.is_ok_and(|n| n > 0) {}
            });
            peers.insert(
                id.clone(),
                Peer {
                    language: language.into(),
                    child,
                    pid,
                    stdin,
                    stdout: BufReader::new(stdout),
                    stderr: drain,
                    lease: lease.clone(),
                    terminated: false,
                    created: Instant::now(),
                    seen: Instant::now(),
                },
            );
            let bind = self
                .context
                .bind_execution_resource_external(&lease, pid.to_string())
                .await;
            if bind.is_err() || self.context.cancel.is_cancelled() || context.cancel.is_cancelled()
            {
                if self
                    .finish(peers.get_mut(&id).expect("inserted"))
                    .await
                    .is_ok()
                {
                    peers.remove(&id);
                }
                return Err("REPL_RESOURCE_BIND_FAILED".into());
            }
            if peers
                .get_mut(&id)
                .expect("inserted")
                .stdin
                .write_all(b"start\n")
                .await
                .is_err()
            {
                if self
                    .finish(peers.get_mut(&id).expect("inserted"))
                    .await
                    .is_ok()
                {
                    peers.remove(&id);
                }
                return Err("REPL_START_FAILED".into());
            }
        }
        let peer = peers.get_mut(&id).expect("owned peer");
        if peer.language != language {
            return Err(
                "REPL_LANGUAGE_MISMATCH: use a separate sessionId for another language".into(),
            );
        }
        peer.seen = Instant::now();
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut bytes = serde_json::to_vec(&json!({"id":request_id,"code":code}))
            .map_err(|_| "REPL_INPUT_INVALID")?;
        bytes.push(b'\n');
        let operation = async {
            peer.stdin
                .write_all(&bytes)
                .await
                .map_err(|_| "REPL_INPUT_FAILED")?;
            peer.stdin.flush().await.map_err(|_| "REPL_INPUT_FAILED")?;
            let mut frame = Vec::new();
            loop {
                let available = peer
                    .stdout
                    .fill_buf()
                    .await
                    .map_err(|_| "REPL_OUTPUT_FAILED")?;
                if available.is_empty() {
                    return Err("REPL_INTERPRETER_EXITED");
                }
                let take = available
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(available.len(), |position| position + 1);
                if frame.len() + take > 2 * 1024 * 1024 {
                    return Err("REPL_OUTPUT_TOO_LARGE");
                }
                let complete = available[take - 1] == b'\n';
                frame.extend_from_slice(&available[..take]);
                peer.stdout.consume(take);
                if complete {
                    break;
                }
            }
            let result: Value =
                serde_json::from_slice(&frame).map_err(|_| "REPL_PROTOCOL_INVALID")?;
            if result["id"] != request_id {
                return Err("REPL_RESPONSE_ID_MISMATCH");
            }
            Ok(result)
        };
        let result = tokio::select! {biased;()=self.context.cancel.cancelled()=>Err("REPL_CANCELLED"),()=context.cancel.cancelled()=>Err("REPL_CANCELLED"),result=tokio::time::timeout(super::EXEC_TIMEOUT,operation)=>result.unwrap_or(Err("REPL_EXECUTION_TIMEOUT"))};
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if self
                    .finish(peers.get_mut(&id).expect("owned peer"))
                    .await
                    .is_ok()
                {
                    peers.remove(&id);
                }
                return Err(format!(
                    "{error}: execution stopped; side effects may have occurred, do not retry automatically"
                ));
            }
        };
        peers.get_mut(&id).expect("owned peer").seen = Instant::now();
        let output = result
            .get("stdout")
            .and_then(Value::as_str)
            .ok_or("REPL_PROTOCOL_INVALID")?;
        let stderr = result
            .get("stderr")
            .and_then(Value::as_str)
            .ok_or("REPL_PROTOCOL_INVALID")?;
        let is_error = result
            .get("isError")
            .and_then(Value::as_bool)
            .ok_or("REPL_PROTOCOL_INVALID")?;
        let mut rendered = super::truncate_output(output);
        if is_error {
            rendered.push_str(&super::truncate_output(stderr));
        }
        Ok(ToolOutput {
            content: rendered,
            is_error,
            metadata: Some(
                json!({"replSessionId":id,"language":language,"stderr":super::truncate_output(stderr),"activeSessions":peers.len(),"truncated":result["truncated"]}),
            ),
        })
    }
}
