//! Explicit, secret-redacted MCP configuration isolated to one admitted Run.

use std::{collections::BTreeMap, fmt, sync::Arc};

use futures::future::BoxFuture;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use zk_tools::{RunToolScope, RunToolScopeFactory, Tool, ToolContext, ToolRegistry};

use crate::{
    ApprovalPort, McpClientManager, McpClientManagerBuilder, McpConfigScope, McpServerConfig,
    McpToolSink, McpTransportType,
};

/// In-memory credential carrier. Intentionally does not implement Serialize.
#[derive(Clone)]
pub struct RunMcpConfig(Vec<McpServerConfig>);

impl fmt::Debug for RunMcpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunMcpConfig")
            .field("service_count", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunMcpConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

impl RunMcpConfig {
    /// Match only a service namespace explicitly supplied for this Run. Discovery
    /// must still validate the actual tool name before any model request.
    #[must_use]
    pub fn may_define_tool(&self, name: &str) -> bool {
        self.0.iter().any(|service| {
            name.strip_prefix(&format!("mcp__{}__", service.name))
                .is_some_and(|suffix| !suffix.is_empty())
        })
    }

    /// Parse the standard `mcpServers` object, rejecting malformed entries as a whole.
    /// # Errors
    /// Malformed, oversized or unsafe server configurations are rejected without side effects.
    #[allow(clippy::too_many_lines)] // Validate all nested transport and secret fields before constructing any scope.
    pub fn parse(value: &Value) -> Result<Self, &'static str> {
        let root = value.as_object().ok_or("MCP_CONFIG_OBJECT_REQUIRED")?;
        let servers = if let Some(servers) = root.get("mcpServers") {
            if root.len() != 1 {
                return Err("MCP_CONFIG_UNKNOWN_FIELD");
            }
            servers
                .as_object()
                .ok_or("MCP_CONFIG_SERVERS_OBJECT_REQUIRED")?
        } else {
            root
        };
        if servers.is_empty() || servers.len() > 16 {
            return Err("MCP_CONFIG_SERVICE_COUNT_INVALID");
        }
        if value.to_string().len() > 256 * 1024 {
            return Err("MCP_CONFIG_TOO_LARGE");
        }
        let mut configs = Vec::new();
        for (name, node) in servers {
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
            {
                return Err("MCP_CONFIG_SERVICE_NAME_INVALID");
            }
            let node = node
                .as_object()
                .ok_or("MCP_CONFIG_SERVICE_OBJECT_REQUIRED")?;
            if node.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "command" | "args" | "env" | "url" | "headers" | "type"
                )
            }) {
                return Err("MCP_CONFIG_UNKNOWN_FIELD");
            }
            let optional_text = |key: &str| -> Result<Option<String>, &'static str> {
                node.get(key)
                    .map(|value| {
                        value
                            .as_str()
                            .filter(|s| !s.is_empty() && !s.contains('\0'))
                            .map(str::to_owned)
                            .ok_or("MCP_CONFIG_STRING_REQUIRED")
                    })
                    .transpose()
            };
            let string_map = |key: &str| -> Result<BTreeMap<String, String>, &'static str> {
                let Some(value) = node.get(key) else {
                    return Ok(BTreeMap::new());
                };
                value
                    .as_object()
                    .ok_or("MCP_CONFIG_STRING_MAP_REQUIRED")?
                    .iter()
                    .map(|(name, value)| {
                        let text = value
                            .as_str()
                            .filter(|s| !s.contains('\0'))
                            .ok_or("MCP_CONFIG_STRING_MAP_REQUIRED")?;
                        if name.is_empty() || name.contains(['\0', '\r', '\n']) {
                            return Err("MCP_CONFIG_KEY_INVALID");
                        }
                        Ok((name.clone(), text.to_owned()))
                    })
                    .collect()
            };
            let command = optional_text("command")?;
            let url = optional_text("url")?;
            let kind = optional_text("type")?.map(|s| s.to_ascii_uppercase());
            let args = node
                .get("args")
                .map(|value| {
                    value
                        .as_array()
                        .ok_or("MCP_CONFIG_ARGS_INVALID")?
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .filter(|s| !s.contains('\0'))
                                .map(str::to_owned)
                                .ok_or("MCP_CONFIG_ARGS_INVALID")
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            let transport = match (command.as_ref(), url.as_ref()) {
                (Some(command), None) => {
                    if command.trim().is_empty()
                        || kind.as_deref().is_some_and(|kind| kind != "STDIO")
                    {
                        return Err("MCP_CONFIG_TRANSPORT_INVALID");
                    }
                    McpTransportType::Stdio
                }
                (None, Some(url)) => {
                    let parsed = reqwest::Url::parse(url).map_err(|_| "MCP_CONFIG_URL_INVALID")?;
                    if !matches!(parsed.scheme(), "https" | "http")
                        || !parsed.username().is_empty()
                        || parsed.password().is_some()
                        || parsed.fragment().is_some()
                    {
                        return Err("MCP_CONFIG_URL_INVALID");
                    }
                    match kind.as_deref().unwrap_or("SSE") {
                        "HTTP" | "STREAMABLE-HTTP" => McpTransportType::Http,
                        "SSE" => McpTransportType::Sse,
                        _ => return Err("MCP_CONFIG_TRANSPORT_UNSUPPORTED"),
                    }
                }
                _ => return Err("MCP_CONFIG_COMMAND_OR_URL_REQUIRED"),
            };
            let env = string_map("env")?;
            let headers = string_map("headers")?;
            if env.keys().any(|name| name.contains('='))
                || headers.iter().any(|(name, value)| {
                    reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err()
                        || reqwest::header::HeaderValue::from_str(value).is_err()
                })
            {
                return Err("MCP_CONFIG_ENV_OR_HEADER_INVALID");
            }
            if (transport == McpTransportType::Stdio && !headers.is_empty())
                || (transport != McpTransportType::Stdio && (!env.is_empty() || !args.is_empty()))
            {
                return Err("MCP_CONFIG_TRANSPORT_FIELDS_INVALID");
            }
            configs.push(McpServerConfig {
                name: name.clone(),
                transport,
                command,
                args,
                env,
                url,
                headers,
                scope: McpConfigScope::Dynamic,
            });
        }
        Ok(Self(configs))
    }
}

/// Only construct from an authenticated user's explicit per-run configuration.
/// The supplied configuration authorizes connection setup, never tool execution.
pub struct RunMcpScopeFactory {
    config: RunMcpConfig,
    global: Arc<McpClientManager>,
}
impl fmt::Debug for RunMcpScopeFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunMcpScopeFactory")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl RunMcpScopeFactory {
    /// Reuse the global manager's live disable policy without sharing credentials or connections.
    pub fn new(config: RunMcpConfig, global: Arc<McpClientManager>) -> Self {
        Self { config, global }
    }
}
struct ExplicitConfigurationApproval(Vec<McpServerConfig>);
impl ApprovalPort for ExplicitConfigurationApproval {
    fn is_trusted(&self, config: &McpServerConfig) -> bool {
        self.0.contains(config)
    }
    fn record_approval(&self, _config: &McpServerConfig, _source: &str) {}
}
struct ScopeSink(Arc<ToolRegistry>);
impl McpToolSink for ScopeSink {
    fn register_dynamic(&self, tool: Arc<dyn Tool>) {
        self.0.register_dynamic(tool);
    }
    fn unregister_by_prefix(&self, prefix: &str) {
        self.0.unregister_by_prefix(prefix);
    }
}
struct McpRunScope {
    manager: Arc<McpClientManager>,
    directory: Arc<ToolRegistry>,
    context: ToolContext,
    cleanup: tokio::sync::Mutex<()>,
    startup: tokio::sync::Mutex<
        Option<tokio::task::JoinHandle<Result<(), crate::manager::ManagerError>>>,
    >,
}
impl RunToolScope for McpRunScope {
    fn registry(&self) -> Arc<ToolRegistry> {
        self.directory.clone()
    }
    fn cleanup(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let _cleanup = self.cleanup.lock().await;
            self.context.cancel.cancel();
            if let Some(startup) = self.startup.lock().await.take() {
                let _ = startup.await;
            }
            self.manager.shutdown().await;
            match self.context.execution_cleanup_status() {
                zk_tools::tool::ToolCleanupStatus::NotRequired
                | zk_tools::tool::ToolCleanupStatus::Confirmed => Ok(()),
                _ => Err("MCP_SCOPE_CLEANUP_UNCONFIRMED".into()),
            }
        })
    }
}
impl Drop for McpRunScope {
    fn drop(&mut self) {
        self.context.cancel.cancel();
        let manager = self.manager.clone();
        let startup = self.startup.get_mut().take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(startup) = startup {
                    let _ = startup.await;
                }
                manager.shutdown().await;
            });
        }
    }
}
impl RunToolScopeFactory for RunMcpScopeFactory {
    fn prepare(
        &self,
        mut context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        Box::pin(async move {
            if context.execution_resource_owner().is_none_or(|owner| {
                Some(owner.run_id.as_str()) != context.run_id()
                    || owner.invocation_id.is_empty()
                    || owner.task_id.is_empty()
            }) {
                return Err("MCP_SCOPE_DURABLE_OWNER_REQUIRED".into());
            }
            if context.run_id().is_none() || context.session_id().is_none() {
                return Err("MCP_SCOPE_OWNER_REQUIRED".into());
            }
            let global = self
                .global
                .list_services()
                .await
                .map_err(|error| error.to_string())?;
            if self.config.0.iter().any(|config| {
                global.iter().any(|known| known.name == config.name)
                    || !self.global.is_service_enabled(&config.name)
            }) {
                return Err("MCP_SCOPE_GLOBAL_SERVICE_CONFLICT_OR_DISABLED".into());
            }
            context = context.fork_execution_resource_tracking();
            context.cancel = context.cancel.child_token();
            let directory = Arc::new(ToolRegistry::overlay(base));
            let manager = McpClientManagerBuilder::new(
                Arc::new(ExplicitConfigurationApproval(self.config.0.clone())),
                Arc::new(ScopeSink(directory.clone())),
            )
            .run_scope(context.clone(), &self.global)
            .result_cache(Arc::new(crate::tool_adapter::ResultCache::new()))
            .build();
            let scope = Arc::new(McpRunScope {
                manager: manager.clone(),
                directory,
                context,
                cleanup: tokio::sync::Mutex::new(()),
                startup: tokio::sync::Mutex::new(None),
            });
            let configs = self.config.0.clone();
            let startup = tokio::spawn(async move { manager.start_scoped(configs).await });
            let started = {
                let mut slot = scope.startup.lock().await;
                *slot = Some(startup);
                let result = slot.as_mut().expect("startup stored").await;
                slot.take();
                result
            };
            if !matches!(started, Ok(Ok(()))) {
                scope.cleanup().await?;
                return Err("MCP_SCOPE_SETUP_FAILED".into());
            }
            Ok(scope as Arc<dyn RunToolScope>)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn strict_configuration_never_debugs_secrets_or_silently_drops_entries() {
        let config:RunMcpConfig = serde_json::from_value(json!({"mcpServers":{"private":{"command":"/bin/tool","args":["secret-arg"],"env":{"API_KEY":"secret-key"}}}})).unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret") && !debug.contains("/bin/tool"));
        for invalid in [
            json!({}),
            json!({"s":{"command":"ok","scope":"USER"}}),
            json!({"s":{"command":"ok","args":[42]}}),
            json!({"s":{"command":"ok","env":{"K":null}}}),
            json!({"s":{"url":"https://user:secret@example.com"}}),
            json!({"s":{"url":"https://example.com","type":"SDK"}}),
            json!({"s":{"command":"ok","url":"https://example.com"}}),
        ] {
            assert!(RunMcpConfig::parse(&invalid).is_err());
        }
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;
    use std::sync::Mutex;
    use tokio_util::sync::CancellationToken;
    use zk_tools::tool::{
        ExecutionResourceAllocation, ExecutionResourceLease, ExecutionResourceObserver,
        ExecutionResourceOwner, ExecutionResourceTerminal,
    };

    #[derive(Default)]
    struct Observer {
        fail_release_once: std::sync::atomic::AtomicBool,
        allocations: Mutex<Vec<ExecutionResourceAllocation>>,
        bound: Mutex<Vec<String>>,
        finished: Mutex<Vec<ExecutionResourceTerminal>>,
    }
    impl ExecutionResourceObserver for Observer {
        fn register(
            &self,
            _owner: ExecutionResourceOwner,
            allocation: ExecutionResourceAllocation,
        ) -> BoxFuture<'static, Result<ExecutionResourceLease, String>> {
            let lease = ExecutionResourceLease {
                resource_id: allocation.resource_id.clone(),
            };
            self.allocations.lock().unwrap().push(allocation);
            Box::pin(async move { Ok(lease) })
        }
        fn bind_external(
            &self,
            _lease: ExecutionResourceLease,
            id: String,
        ) -> BoxFuture<'static, Result<(), String>> {
            self.bound.lock().unwrap().push(id);
            Box::pin(async { Ok(()) })
        }
        fn finish(
            &self,
            _lease: ExecutionResourceLease,
            status: ExecutionResourceTerminal,
        ) -> BoxFuture<'static, Result<(), String>> {
            self.finished.lock().unwrap().push(status);
            if status == ExecutionResourceTerminal::Released
                && self
                    .fail_release_once
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return Box::pin(async { Err("injected durable finish outage".into()) });
            }
            Box::pin(async { Ok(()) })
        }
    }
    fn context(observer: Arc<Observer>, path: &std::path::Path) -> ToolContext {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_working_dir(path)
            .with_session_id("scope-session")
            .with_run_id("scope-run")
            .with_execution_resources(
                ExecutionResourceOwner {
                    task_id: "scope-task".into(),
                    run_id: "scope-run".into(),
                    invocation_id: "real-setup-invocation".into(),
                },
                observer,
            )
    }
    fn global(base: Arc<ToolRegistry>) -> Arc<McpClientManager> {
        McpClientManagerBuilder::new(
            Arc::new(ExplicitConfigurationApproval(Vec::new())),
            Arc::new(ScopeSink(base)),
        )
        .build()
    }
    const PYTHON_SERVER: &str = r"
import sys,json,os
for line in sys.stdin:
    request=json.loads(line)
    if 'id' not in request: continue
    method=request['method']
    if method=='initialize':
        result={'protocolVersion':'2024-11-05','serverInfo':{'name':'scope-test','version':'1'},'capabilities':{'tools':{}}}
    elif method=='tools/list':
        result={'tools':[{'name':'pwd','description':'Workspace identity','inputSchema':{'type':'object','properties':{}}}]}
    elif method=='tools/call': result={'content':[{'type':'text','text':os.getcwd()}]}
    elif method=='resources/list': result={'resources':[]}
    elif method=='prompts/list': result={'prompts':[]}
    else: result={}
    print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}),flush=True)
";

    #[tokio::test]
    async fn real_stdio_scope_has_private_directory_workspace_and_confirmed_cleanup() {
        let path = std::env::temp_dir().join(format!("zk-mcp-run-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        let observer = Arc::new(Observer::default());
        let ctx = context(observer.clone(), &path);
        let base = Arc::new(ToolRegistry::new());
        base.register_dynamic(Arc::new(zk_tools::EchoTool));
        let global = global(base.clone());
        let config=RunMcpConfig::parse(&serde_json::json!({"mcpServers":{"run_private_fixture":{"command":"/usr/bin/python3","args":["-u","-c",PYTHON_SERVER]}}})).unwrap();
        let factory = RunMcpScopeFactory::new(config, global.clone());
        let scope = factory.prepare(ctx.clone(), base.clone()).await.unwrap();
        let registry = scope.registry();
        assert!(registry.get("Echo").is_some());
        let tool = registry.get("mcp__run_private_fixture__pwd").unwrap();
        let output = tool.execute(serde_json::json!({}), ctx.clone()).await;
        assert!(!output.is_error, "{}", output.content);
        assert!(
            output
                .content
                .contains(&path.file_name().unwrap().to_string_lossy().to_string())
        );
        assert!(base.get(tool.name()).is_none());
        assert_eq!(global.connection_count(), 0);
        assert_eq!(observer.bound.lock().unwrap().len(), 1);
        scope.cleanup().await.unwrap();
        scope.cleanup().await.unwrap();
        assert!(registry.get(tool.name()).is_none());
        assert!(
            !ctx.cancel.is_cancelled(),
            "scope cleanup must not cancel its parent Run"
        );
        assert!(
            observer
                .finished
                .lock()
                .unwrap()
                .iter()
                .all(|terminal| *terminal == ExecutionResourceTerminal::Released)
        );
        let output = tool.execute(serde_json::json!({}), ctx).await;
        assert!(
            output.is_error,
            "retained adapters must be revoked after cleanup"
        );
        std::fs::remove_dir(path).unwrap();
    }

    #[tokio::test]
    async fn cancelled_prepare_still_reaps_its_inflight_stdio_process() {
        let observer = Arc::new(Observer::default());
        let path = std::env::temp_dir();
        let ctx = context(observer.clone(), &path);
        let base = Arc::new(ToolRegistry::new());
        let config=RunMcpConfig::parse(&serde_json::json!({"mcpServers":{"run_cancel_fixture":{"command":"/bin/sleep","args":["100"]}}})).unwrap();
        let factory = RunMcpScopeFactory::new(config, global(base.clone()));
        let task = tokio::spawn(async move { factory.prepare(ctx, base).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if !observer.bound.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            loop {
                if !observer.finished.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            *observer.finished.lock().unwrap(),
            vec![ExecutionResourceTerminal::Released]
        );
    }

    #[tokio::test]
    async fn scope_cleanup_retries_storage_failure_without_losing_process_ownership() {
        let observer = Arc::new(Observer::default());
        observer
            .fail_release_once
            .store(true, std::sync::atomic::Ordering::Release);
        let ctx = context(observer.clone(), &std::env::temp_dir());
        let base = Arc::new(ToolRegistry::new());
        let config=RunMcpConfig::parse(&serde_json::json!({"mcpServers":{"run_finish_retry":{"command":"/usr/bin/python3","args":["-u","-c",PYTHON_SERVER]}}})).unwrap();
        let scope = RunMcpScopeFactory::new(config, global(base.clone()))
            .prepare(ctx, base)
            .await
            .unwrap();
        assert!(
            scope.cleanup().await.is_err(),
            "a failed durable cleanup cannot report success"
        );
        scope.cleanup().await.unwrap();
        assert_eq!(
            observer.bound.lock().unwrap().len(),
            1,
            "retry must not spawn a replacement process"
        );
        assert_eq!(
            observer.finished.lock().unwrap().last(),
            Some(&ExecutionResourceTerminal::Released)
        );
    }
}
