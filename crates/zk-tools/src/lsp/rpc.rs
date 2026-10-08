use super::toolchains::{Installation, ServerCommand};
use crate::{
    ToolContext,
    tool::{ExecutionResourceLease, ExecutionResourceTerminal},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::Path,
    process::Stdio,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Notify, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const MAX_MESSAGE: usize = 64 * 1024 * 1024;
type Pending = oneshot::Sender<Result<Value, String>>;
type Writer = Arc<tokio::sync::Mutex<Option<ChildStdin>>>;
struct Shared {
    pending: Mutex<HashMap<i64, Pending>>,
    diagnostics: Mutex<HashMap<String, Value>>,
    changed: Notify,
    alive: AtomicBool,
    settings: Value,
    workspace: String,
}
impl Shared {
    fn disconnect(&self) {
        self.alive.store(false, Ordering::Release);
        for (_, sender) in self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
        {
            let _ = sender.send(Err("LSP_SERVER_DISCONNECTED".into()));
        }
        self.changed.notify_waiters();
    }
}
pub(super) struct Peer {
    context: ToolContext,
    lease: Mutex<Option<ExecutionResourceLease>>,
    child: Mutex<Option<(Child, u32)>>,
    original_pid: u32,
    writer: Writer,
    shared: Arc<Shared>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    next: AtomicI64,
    pub capabilities: std::sync::OnceLock<Value>,
    close: tokio::sync::Mutex<()>,
}
impl Peer {
    #[allow(clippy::too_many_lines)] // One ordered process lease/startup/handshake transaction.
    pub async fn start(
        installation: Arc<Installation>,
        config: ServerCommand,
        language: String,
        state: &Path,
        context: ToolContext,
        retained: Arc<Mutex<Vec<Arc<Self>>>>,
    ) -> Result<Arc<Self>, String> {
        if context.cancel.is_cancelled() {
            return Err("LSP_RUN_CANCELLED".into());
        }
        let workspace = url::Url::from_directory_path(context.working_dir())
            .map_err(|()| "LSP_WORKSPACE_URI_INVALID")?
            .to_string();
        tokio::fs::create_dir_all(state.join("home"))
            .await
            .map_err(|_| "LSP_STATE_DIRECTORY_FAILED")?;
        tokio::fs::create_dir_all(state.join("tmp"))
            .await
            .map_err(|_| "LSP_STATE_DIRECTORY_FAILED")?;
        let profile = super::sandbox::profile(
            context.working_dir(),
            &installation.bundle,
            &installation.rust_toolchain_root,
            state,
        )
        .await?;
        if language == "java" {
            let configuration = state.join("java-config");
            tokio::fs::create_dir_all(&configuration)
                .await
                .map_err(|_| "LSP_JAVA_CONFIGURATION_FAILED")?;
            tokio::fs::copy(
                installation.bundle.join("jdtls/config_mac_arm/config.ini"),
                configuration.join("config.ini"),
            )
            .await
            .map_err(|_| "LSP_JAVA_CONFIGURATION_FAILED")?;
        }
        let lease = context
            .register_execution_resource(
                "processGroup",
                None,
                json!({"protocol":"lsp","language":language}),
            )
            .await?;
        let settings = settings(
            &language,
            &installation.bundle,
            &installation.rust_source_root,
        );
        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
            diagnostics: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            alive: AtomicBool::new(true),
            settings: settings.clone(),
            workspace: workspace.clone(),
        });
        let mut command = Command::new("/usr/bin/sandbox-exec");
        command
            .arg("-f")
            .arg(profile)
            .arg(installation.bundle.join(&config.program));
        let expand = |argument: &str| {
            argument
                .replace("{bundle}", &installation.bundle.to_string_lossy())
                .replace("{state}", &state.to_string_lossy())
        };
        command
            .args(config.args.iter().map(|arg| expand(arg)))
            .current_dir(context.working_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.env_clear();
        for key in [
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "RUSTUP_HOME",
            "CARGO_HOME",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
        command
            .env(
                "PATH",
                format!(
                    "{}:{}:{}:{}",
                    installation.rust_toolchain_root.join("bin").display(),
                    installation.bundle.join("node/bin").display(),
                    installation.bundle.join("go/bin").display(),
                    path
                ),
            )
            .env("HOME", state.join("home"))
            .env("TMPDIR", state.join("tmp"))
            .env("RUSTUP_TOOLCHAIN", &installation.rust_compiler_version)
            .env("CARGO_TARGET_DIR", state.join("rust-target"))
            .env("JAVA_HOME", installation.bundle.join("java/Contents/Home"))
            .env("GOTOOLCHAIN", "local")
            .env("CGO_ENABLED", "0")
            .env("GOPROXY", "off")
            .env("GOSUMDB", "off")
            .env("GOFLAGS", "-mod=readonly")
            .env("GOPATH", state.join("go"))
            .env("GOCACHE", state.join("go-cache"))
            .env("GOMODCACHE", state.join("go-modules"))
            .env("RUSTC_WRAPPER", "")
            .env("RUSTC_WORKSPACE_WRAPPER", "")
            .env("CARGO_NET_OFFLINE", "true");
        #[cfg(unix)]
        command.process_group(0);
        let child = command.spawn();
        let Ok(mut child) = child else {
            if let Some(lease) = lease {
                context
                    .finish_execution_resource(lease, ExecutionResourceTerminal::Released)
                    .await?;
            }
            return Err("LSP_SERVER_START_FAILED".into());
        };
        let Some(pid) = child.id() else {
            return Err("LSP_PROCESS_ID_MISSING".into());
        };
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let writer = Arc::new(tokio::sync::Mutex::new(child.stdin.take()));
        let peer = Arc::new(Self {
            context: context.clone(),
            lease: Mutex::new(lease),
            child: Mutex::new(Some((child, pid))),
            original_pid: pid,
            writer: writer.clone(),
            shared: shared.clone(),
            tasks: Mutex::new(Vec::new()),
            next: AtomicI64::new(1),
            capabilities: std::sync::OnceLock::new(),
            close: tokio::sync::Mutex::new(()),
        });
        retained
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(peer.clone());
        let resource = peer
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(resource) = resource
            && context
                .bind_execution_resource_external(&resource, pid.to_string())
                .await
                .is_err()
        {
            peer.close().await;
            return Err("LSP_RESOURCE_BIND_FAILED".into());
        }
        let reader = tokio::spawn(read_loop(stdout, shared, writer));
        let drain = tokio::spawn(async move {
            if let Some(mut stderr) = stderr {
                let mut bytes = [0u8; 8192];
                while stderr.read(&mut bytes).await.is_ok_and(|n| n > 0) {}
            }
        });
        peer.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend([reader, drain]);
        let initialized=peer.request("initialize",json!({"processId":null,"rootUri":workspace,"workspaceFolders":[{"uri":workspace,"name":"workspace"}],"capabilities":{"general":{"positionEncodings":["utf-16"]},"workspace":{"configuration":true,"workspaceFolders":true,"applyEdit":false},"textDocument":{"synchronization":{"didSave":true},"hover":{"contentFormat":["markdown","plaintext"]},"publishDiagnostics":{"versionSupport":true},"callHierarchy":{},"documentSymbol":{"hierarchicalDocumentSymbolSupport":true}}},"initializationOptions":initialization(&language,&settings,&installation.bundle)}),Duration::from_secs(90),&context.cancel).await;
        match initialized {
            Ok(result) => {
                let _ = peer
                    .capabilities
                    .set(result.get("capabilities").cloned().unwrap_or(Value::Null));
            }
            Err(error) => {
                peer.close().await;
                return Err(error);
            }
        }
        if peer
            .capabilities
            .get()
            .and_then(|capabilities| capabilities.get("positionEncoding"))
            .and_then(Value::as_str)
            .is_some_and(|encoding| encoding != "utf-16")
        {
            peer.close().await;
            return Err("LSP_POSITION_ENCODING_UNSUPPORTED".into());
        }
        if let Err(error) = async {
            peer.notify("initialized", json!({})).await?;
            peer.notify(
                "workspace/didChangeConfiguration",
                json!({"settings":settings}),
            )
            .await
        }
        .await
        {
            peer.close().await;
            return Err(error);
        }
        Ok(peer)
    }
    pub async fn request(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Value, String> {
        struct Guard<'a>(&'a Shared, i64);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&self.1);
            }
        }
        if !self.shared.alive.load(Ordering::Acquire) {
            return Err("LSP_SERVER_DISCONNECTED".into());
        }
        let id = self.next.fetch_add(1, Ordering::AcqRel);
        let (sender, receiver) = oneshot::channel();
        self.shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, sender);
        let _guard = Guard(&self.shared, id);
        let result = tokio::select! {
            biased;
            ()=self.context.cancel.cancelled()=>Err("LSP_RUN_CANCELLED".into()),
            ()=cancel.cancelled()=>Err("LSP_REQUEST_CANCELLED".into()),
            result=tokio::time::timeout(timeout,async{write(&self.writer,&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})).await?;receiver.await.map_err(|_|"LSP_SERVER_DISCONNECTED".to_owned())?})=>result.map_err(|_|"LSP_REQUEST_TIMEOUT".to_owned())?,
        };
        if result.is_err() {
            let _ = self.notify("$/cancelRequest", json!({"id":id})).await;
        }
        result
    }
    pub async fn notify(&self, method: &str, params: Value) -> Result<(), String> {
        write(
            &self.writer,
            &json!({"jsonrpc":"2.0","method":method,"params":params}),
        )
        .await
    }
    pub fn clear_diagnostics(&self, uri: &str) {
        self.shared
            .diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(uri);
    }
    pub async fn diagnostics(
        &self,
        uri: &str,
        version: i64,
        cancel: &CancellationToken,
    ) -> Result<Value, String> {
        tokio::time::timeout(Duration::from_secs(30),async{
            loop{
                let notified=self.shared.changed.notified();
                let result=self.shared.diagnostics.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(uri).cloned();
                if let Some(result)=result && result.get("version").and_then(Value::as_i64).is_none_or(|v|v==version){return Ok(json!({"items":result["diagnostics"],"documentVersion":version,"reportedVersion":result.get("version"),"freshness":if result.get("version").and_then(Value::as_i64).is_some(){"version-matched"}else{"unverified-unversioned"}}));}
                tokio::select!{biased;()=cancel.cancelled()=>return Err("LSP_REQUEST_CANCELLED".into()),()=self.context.cancel.cancelled()=>return Err("LSP_RUN_CANCELLED".into()),()=notified=>{}}
                if !self.shared.alive.load(Ordering::Acquire){return Err("LSP_SERVER_DISCONNECTED".into());}
            }
        }).await.map_err(|_|"LSP_DIAGNOSTICS_NOT_READY".to_owned())?
    }
    pub async fn close(&self) {
        let _close = self.close.lock().await;
        self.shared.disconnect();
        let child = self
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let released = if let Some((mut child, pid)) = child {
            let released = crate::process::terminate_process_group(&mut child, pid).await;
            if !released {
                *self
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((child, pid));
            }
            released
        } else {
            true
        };
        self.writer.lock().await.take();
        for task in self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            task.abort();
        }
        let lease = self
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(lease) = lease {
            let mut persisted = self
                .context
                .finish_execution_resource(
                    lease.clone(),
                    if released {
                        ExecutionResourceTerminal::Released
                    } else {
                        ExecutionResourceTerminal::Unconfirmed
                    },
                )
                .await;
            if released && persisted.is_err() {
                persisted = self
                    .context
                    .reconcile_execution_resource(&lease, self.original_pid.to_string())
                    .await;
            }
            if released && persisted.is_ok() {
                self.lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
            }
        }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        let child = self
            .child
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let lease = self
            .lease
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let context = self.context.clone();
        for task in self
            .tasks
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            task.abort();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let released = if let Some((mut child, pid)) = child {
                    crate::process::terminate_process_group(&mut child, pid).await
                } else {
                    true
                };
                if let Some(lease) = lease {
                    let _ = context
                        .finish_execution_resource(
                            lease,
                            if released {
                                ExecutionResourceTerminal::Released
                            } else {
                                ExecutionResourceTerminal::Unconfirmed
                            },
                        )
                        .await;
                }
            });
        }
    }
}
async fn write(writer: &Writer, value: &Value) -> Result<(), String> {
    let payload = serde_json::to_vec(value).map_err(|_| "LSP_SERIALIZATION_FAILED")?;
    if payload.len() > MAX_MESSAGE {
        return Err("LSP_MESSAGE_TOO_LARGE".into());
    }
    let mut guard = writer.lock().await;
    let stdin = guard.as_mut().ok_or("LSP_INPUT_CLOSED")?;
    tokio::time::timeout(Duration::from_secs(5), async {
        stdin
            .write_all(format!("Content-Length: {}\r\n\r\n", payload.len()).as_bytes())
            .await?;
        stdin.write_all(&payload).await?;
        stdin.flush().await
    })
    .await
    .map_err(|_| "LSP_WRITE_TIMEOUT")?
    .map_err(|_| "LSP_WRITE_FAILED".into())
}
async fn read_message(reader: &mut BufReader<ChildStdout>) -> Result<Option<Value>, String> {
    let mut length = None;
    let mut header_bytes = 0;
    loop {
        let mut line = Vec::new();
        let read = (&mut *reader)
            .take(8193)
            .read_until(b'\n', &mut line)
            .await
            .map_err(|_| "LSP_READ_FAILED")?;
        if read == 0 {
            return Ok(None);
        }
        header_bytes += line.len();
        if header_bytes > 8192 {
            return Err("LSP_HEADER_TOO_LARGE".into());
        }
        if line == b"\r\n" {
            break;
        }
        let text = std::str::from_utf8(&line).map_err(|_| "LSP_HEADER_INVALID")?;
        let (key, value) = text
            .trim_end()
            .split_once(':')
            .ok_or("LSP_HEADER_INVALID")?;
        if key.eq_ignore_ascii_case("Content-Length") {
            if length.is_some() {
                return Err("LSP_HEADER_DUPLICATE".into());
            }
            length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| "LSP_LENGTH_INVALID")?,
            );
        }
    }
    let length = length
        .filter(|length| *length <= MAX_MESSAGE)
        .ok_or("LSP_MESSAGE_TOO_LARGE")?;
    let mut payload = vec![0; length];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|_| "LSP_READ_FAILED")?;
    serde_json::from_slice(&payload)
        .map(Some)
        .map_err(|_| "LSP_MESSAGE_INVALID".into())
}
async fn read_loop(stdout: Option<ChildStdout>, shared: Arc<Shared>, writer: Writer) {
    let Some(stdout) = stdout else {
        shared.disconnect();
        return;
    };
    let mut reader = BufReader::new(stdout);
    while let Ok(Some(message)) = read_message(&mut reader).await {
        if let Some(method) = message.get("method").and_then(Value::as_str) {
            if let Some(id) = message.get("id") {
                let response = match method {
                    "workspace/configuration" => Some(Value::Array(
                        message["params"]["items"]
                            .as_array()
                            .map_or_else(Vec::new, |items| {
                                items
                                    .iter()
                                    .map(|item| {
                                        item.get("section").and_then(Value::as_str).map_or(
                                            shared.settings.clone(),
                                            |section| {
                                                section
                                                    .split('.')
                                                    .fold(&shared.settings, |value, key| {
                                                        &value[key]
                                                    })
                                                    .clone()
                                            },
                                        )
                                    })
                                    .collect()
                            }),
                    )),
                    "workspace/workspaceFolders" => {
                        Some(json!([{"uri":shared.workspace,"name":"workspace"}]))
                    }
                    "client/registerCapability"
                    | "client/unregisterCapability"
                    | "window/workDoneProgress/create" => Some(Value::Null),
                    "workspace/applyEdit" => {
                        Some(json!({"applied":false,"failureReason":"LSP queries are read-only"}))
                    }
                    _ => None,
                };
                let value = match response {
                    Some(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
                    None => {
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Unsupported client request"}})
                    }
                };
                if write(&writer, &value).await.is_err() {
                    break;
                }
            } else if method == "textDocument/publishDiagnostics" {
                let params = &message["params"];
                if let Some(uri) = params["uri"].as_str()
                    && params["diagnostics"].is_array()
                {
                    let mut diagnostics = shared
                        .diagnostics
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if diagnostics.len() < 256 || diagnostics.contains_key(uri) {
                        diagnostics.insert(uri.to_owned(), params.clone());
                    }
                    shared.changed.notify_waiters();
                }
            }
        } else if let Some(id) = message.get("id").and_then(Value::as_i64)
            && let Some(sender) = shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id)
        {
            let result = if message.get("error").is_some() {
                Err(format!("LSP_REQUEST_FAILED: {}", message["error"]["code"]))
            } else {
                Ok(message.get("result").cloned().unwrap_or(Value::Null))
            };
            let _ = sender.send(result);
        }
    }
    shared.disconnect();
}
fn settings(language: &str, bundle: &Path, rust_source: &Path) -> Value {
    match language {
        "rust" => {
            json!({"rust-analyzer":{"cargo":{"sysrootSrc":rust_source,"buildScripts":{"enable":false},"autoreload":false,"extraArgs":["--locked"]},"procMacro":{"enable":false},"checkOnSave":false,"check":{"enable":false}}})
        }
        "java" => {
            json!({"java":{"home":bundle.join("java/Contents/Home"),"autobuild":{"enabled":false},"import":{"gradle":{"enabled":false},"maven":{"enabled":false}},"configuration":{"updateBuildConfiguration":"disabled"}}})
        }
        "python" => {
            json!({"python":{"analysis":{"autoImportCompletions":false,"diagnosticMode":"openFilesOnly","typeCheckingMode":"basic"}}})
        }
        "go" => {
            json!({"gopls":{"ui.diagnostic.analyses":{},"ui.completion.usePlaceholders":false}})
        }
        _ => {
            json!({"typescript":{"disableAutomaticTypeAcquisition":true},"javascript":{"disableAutomaticTypeAcquisition":true}})
        }
    }
}
fn initialization(language: &str, settings: &Value, bundle: &Path) -> Value {
    match language {
        "rust" => settings["rust-analyzer"].clone(),
        "java" => json!({"settings":settings}),
        "typescript" => {
            json!({"tsserver":{"path":bundle.join("npm/node_modules/typescript/lib/tsserver.js"),"logVerbosity":"off"},"disableAutomaticTypingAcquisition":true,"plugins":[],"preferences":{"includeCompletionsForModuleExports":false}})
        }
        "go" => json!({"analyses":{},"usePlaceholders":false}),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod settings_tests {
    use super::*;
    #[test]
    fn rust_initialization_pins_private_sources_without_enabling_workspace_execution() {
        let bundle = Path::new("/private/lsp/bundle");
        let source = bundle.join("rust-src/rust-src/lib/rustlib/src/rust/library");
        let settings = settings("rust", bundle, &source);
        let options = initialization("rust", &settings, bundle);
        assert_eq!(options["cargo"]["sysrootSrc"], source.to_str().unwrap());
        assert_eq!(options["cargo"]["buildScripts"]["enable"], false);
        assert_eq!(options["procMacro"]["enable"], false);
        assert_eq!(options["checkOnSave"], false);
        assert_eq!(options["cargo"]["extraArgs"], json!(["--locked"]));
    }
}
