//! Run-owned, read-only language intelligence over actual supervised LSP servers.
mod rpc;
mod sandbox;
mod toolchains;

use crate::{
    ChildToolAccess, RunToolScope, RunToolScopeFactory, Tool, ToolContext, ToolOutput, ToolRegistry,
};
use futures::future::BoxFuture;
use rpc::Peer;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, OnceCell};
use toolchains::Installation;

/// Locate the application-owned installation independently of the session project.
/// Explicit host configuration is preserved; model inputs never choose this path.
#[must_use]
pub fn default_manifest_path() -> PathBuf {
    if let Some(path) = std::env::var_os("ZK_LSP_MANIFEST").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }
    if let Ok(executable) = std::env::current_exe() {
        for ancestor in executable.ancestors().skip(1).take(6) {
            if ancestor.join("configuration/lsp-toolchain.json").is_file() {
                return ancestor.join(".runtime/lsp/current.json");
            }
        }
    }
    zk_core::paths::user_config_dir().join("lsp/current.json")
}

/// Default host factory. Preparing a Run never starts a process or downloads tools.
#[derive(Debug)]
pub struct LspRunScopeFactory {
    manifest: PathBuf,
}
impl LspRunScopeFactory {
    /// Use an installer-owned manifest, never a model-supplied executable path.
    #[must_use]
    pub fn new(manifest: PathBuf) -> Self {
        Self { manifest }
    }
}
struct Startup {
    job: Option<tokio::task::JoinHandle<Result<Arc<Peer>, String>>>,
    ready: Option<Result<Arc<Peer>, String>>,
}
struct Slot {
    startup: Mutex<Startup>,
    operation: Mutex<()>,
    documents: Mutex<HashMap<String, (String, i64)>>,
}
struct Manager {
    manifest: PathBuf,
    installation: OnceCell<Result<Arc<Installation>, String>>,
    context: ToolContext,
    slots: Mutex<HashMap<String, Arc<Slot>>>,
    retained: Arc<std::sync::Mutex<Vec<Arc<Peer>>>>,
    state_directories: Mutex<std::collections::HashSet<PathBuf>>,
}
impl Manager {
    async fn slot(&self, language: &str, workspace: &Path) -> Result<Arc<Slot>, String> {
        let key = format!("{language}:{}", workspace.display());
        let mut slots = self.slots.lock().await;
        if let Some(slot) = slots.get(&key) {
            return Ok(slot.clone());
        }
        if slots.len() >= 10 {
            return Err("LSP_RUN_SERVER_LIMIT".into());
        }
        let slot = Arc::new(Slot {
            startup: Mutex::new(Startup {
                job: None,
                ready: None,
            }),
            operation: Mutex::new(()),
            documents: Mutex::new(HashMap::new()),
        });
        slots.insert(key, slot.clone());
        Ok(slot)
    }
    async fn peer(
        &self,
        slot: &Slot,
        language: &str,
        workspace: &Path,
    ) -> Result<Arc<Peer>, String> {
        let mut startup = slot.startup.lock().await;
        if let Some(result) = &startup.ready {
            return result.clone();
        }
        if startup.job.is_none() {
            let installation = self
                .installation
                .get_or_init(|| async { Installation::load(&self.manifest).await.map(Arc::new) })
                .await
                .clone()?;
            let command = installation
                .commands
                .get(language)
                .cloned()
                .ok_or("LSP_LANGUAGE_NOT_INSTALLED")?;
            let state = self
                .manifest
                .parent()
                .ok_or("LSP_MANIFEST_PATH_INVALID")?
                .join("state")
                .join(self.context.run_id().ok_or("LSP_RUN_REQUIRED")?)
                .join(crate::atomic::sha256_hex(
                    workspace.to_string_lossy().as_bytes(),
                ));
            tokio::fs::create_dir_all(&state)
                .await
                .map_err(|_| "LSP_STATE_DIRECTORY_FAILED")?;
            self.state_directories.lock().await.insert(state.clone());
            let context = self.context.clone().with_working_dir(workspace);
            let language = language.to_owned();
            let retained = self.retained.clone();
            startup.job = Some(tokio::spawn(async move {
                Peer::start(installation, command, language, &state, context, retained).await
            }));
        }
        let result = startup
            .job
            .as_mut()
            .expect("startup is present")
            .await
            .map_err(|_| "LSP_SERVER_START_INTERRUPTED".to_owned())
            .and_then(|result| result);
        startup.job.take();
        startup.ready = Some(result.clone());
        result
    }
    async fn cleanup(&self) -> Result<(), String> {
        self.context.cancel.cancel();
        let slots = self
            .slots
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for slot in slots {
            let mut startup = slot.startup.lock().await;
            if let Some(job) = startup.job.take() {
                startup.ready = Some(
                    job.await
                        .map_err(|_| "LSP_SERVER_START_INTERRUPTED".to_owned())
                        .and_then(|result| result),
                );
            }
            if let Some(Ok(peer)) = &startup.ready {
                peer.close().await;
            }
        }
        let peers = self
            .retained
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for peer in peers {
            peer.close().await;
        }
        match self.context.execution_cleanup_status() {
            crate::tool::ToolCleanupStatus::NotRequired
            | crate::tool::ToolCleanupStatus::Confirmed => {
                let mut directories = self.state_directories.lock().await;
                for path in directories.clone() {
                    match tokio::fs::remove_dir_all(&path).await {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => return Err("LSP_STATE_CLEANUP_FAILED".into()),
                    }
                    directories.remove(&path);
                    // Best effort for the now empty Run directory; never remove siblings.
                    if let Some(parent) = path.parent() {
                        let _ = tokio::fs::remove_dir(parent).await;
                    }
                }
                Ok(())
            }
            _ => Err("LSP_CLEANUP_UNCONFIRMED".into()),
        }
    }
}
struct Scope {
    manager: Arc<Manager>,
    directory: Arc<ToolRegistry>,
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
        self.manager.context.cancel.cancel();
        let manager = self.manager.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = manager.cleanup().await;
            });
        }
    }
}
impl RunToolScopeFactory for LspRunScopeFactory {
    fn prepare(
        &self,
        mut context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        Box::pin(async move {
            if context.execution_resource_owner().is_none()
                || context.run_id().is_none_or(|id| {
                    id.is_empty()
                        || id.len() > 128
                        || !id
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                })
            {
                return Err("LSP_SCOPE_OWNER_REQUIRED".into());
            }
            context = context.fork_execution_resource_tracking();
            context.cancel = context.cancel.child_token();
            let manager = Arc::new(Manager {
                manifest: self.manifest.clone(),
                installation: OnceCell::new(),
                context,
                slots: Mutex::new(HashMap::new()),
                retained: Arc::new(std::sync::Mutex::new(Vec::new())),
                state_directories: Mutex::new(std::collections::HashSet::new()),
            });
            let directory = Arc::new(ToolRegistry::overlay(base));
            directory.register_dynamic(Arc::new(LspTool {
                manager: manager.clone(),
            }));
            Ok(Arc::new(Scope { manager, directory }) as Arc<dyn RunToolScope>)
        })
    }
}
struct LspTool {
    manager: Arc<Manager>,
}
impl fmt::Debug for LspTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LspTool").finish_non_exhaustive()
    }
}
impl Tool for LspTool {
    fn name(&self) -> &'static str {
        "LSP"
    }
    fn description(&self) -> &'static str {
        "Read-only semantic code intelligence using installed TypeScript/JavaScript, Python, Rust, Go and Java language servers. Definitions, references, hover, symbols, diagnostics, implementations and call hierarchy. Lines/columns are 1-based Unicode character offsets. Workspace symbols requires language when file_path is omitted. No automatic package downloads, build scripts or workspace edits."
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","additionalProperties":false,"anyOf":[{"required":["action"]},{"required":["operation"]}],"properties":{"action":{"type":"string","enum":["definition","references","hover","symbols","diagnostics","implementation","prepareCallHierarchy","incomingCalls","outgoingCalls"]},"operation":{"type":"string","enum":["goToDefinition","findReferences","hover","documentSymbol","workspaceSymbol","goToImplementation","prepareCallHierarchy","incomingCalls","outgoingCalls"]},"character":{"type":"integer","minimum":1},"file_path":{"type":"string"},"filePath":{"type":"string"},"line":{"type":"integer","minimum":1},"column":{"type":"integer","minimum":1},"query":{"type":"string"},"language":{"type":"string","enum":["typescript","javascript","python","rust","go","java"]}}})
    }
    fn timeout(&self) -> Duration {
        Duration::from_mins(3)
    }
    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }
    fn child_access(&self) -> ChildToolAccess {
        ChildToolAccess::ReadOnly
    }
    fn path_of(&self, input: &Value) -> Option<String> {
        input
            .get("file_path")
            .or_else(|| input.get("filePath"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    }
    fn execute(&self, input: Value, context: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            match self.invoke(input, context).await {
                Ok(value) => ToolOutput::ok(value.to_string()),
                Err(error) => ToolOutput::error(error),
            }
        })
    }
}
impl LspTool {
    #[allow(clippy::too_many_lines)] // Request validation and method dispatch retain their ordered capability checks.
    async fn invoke(&self, input: Value, context: ToolContext) -> Result<Value, String> {
        if self.manager.context.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return Err("LSP_RUN_CANCELLED".into());
        }
        let workspace = context
            .working_dir()
            .canonicalize()
            .map_err(|_| "LSP_WORKSPACE_UNAVAILABLE")?;
        let bound_workspace = self
            .manager
            .context
            .working_dir()
            .canonicalize()
            .map_err(|_| "LSP_WORKSPACE_UNAVAILABLE")?;
        if !workspace.starts_with(bound_workspace) {
            return Err("LSP_WORKSPACE_OUTSIDE_RUN".into());
        }
        let file = self
            .path_of(&input)
            .map(|path| {
                let path = PathBuf::from(path);
                let path = if path.is_absolute() {
                    path
                } else {
                    workspace.join(path)
                };
                path.canonicalize()
                    .map_err(|_| "LSP_FILE_UNAVAILABLE".to_owned())
            })
            .transpose()?;
        if file
            .as_ref()
            .is_some_and(|file| !file.starts_with(&workspace))
        {
            return Err("LSP_FILE_OUTSIDE_WORKSPACE".into());
        }
        let action = input
            .get("action")
            .or_else(|| input.get("operation"))
            .and_then(Value::as_str)
            .ok_or("LSP_ACTION_REQUIRED")?;
        let workspace_symbols = action == "workspaceSymbol";
        let action = match action {
            "goToDefinition" => "definition",
            "findReferences" => "references",
            "documentSymbol" | "workspaceSymbol" => "symbols",
            "goToImplementation" => "implementation",
            other => other,
        };
        if !matches!(
            action,
            "definition"
                | "references"
                | "hover"
                | "symbols"
                | "diagnostics"
                | "implementation"
                | "prepareCallHierarchy"
                | "incomingCalls"
                | "outgoingCalls"
        ) {
            return Err("LSP_ACTION_UNSUPPORTED".into());
        }
        if action != "symbols" && file.is_none() {
            return Err("LSP_FILE_REQUIRED".into());
        }
        let language = match input.get("language").and_then(Value::as_str) {
            Some("javascript" | "typescript") => "typescript",
            Some(language) => language,
            None => language_for(
                file.as_deref()
                    .ok_or("LSP_WORKSPACE_SYMBOLS_LANGUAGE_REQUIRED")?,
            )
            .ok_or("LSP_LANGUAGE_UNSUPPORTED")?,
        };
        if !["typescript", "python", "rust", "go", "java"].contains(&language) {
            return Err("LSP_LANGUAGE_UNSUPPORTED".into());
        }
        if language == "java" && context.is_ephemeral() {
            return Err("LSP_JAVA_EPHEMERAL_UNAVAILABLE: JDT workspace error logging cannot guarantee body-free disk output".into());
        }
        let slot = self.manager.slot(language, &workspace).await?;
        let _operation = slot.operation.lock().await;
        let peer = self.manager.peer(&slot, language, &workspace).await?;
        let (uri, text, version) = if let Some(file) = file {
            let text = read_document(file.clone()).await?;
            let uri = url::Url::from_file_path(&file)
                .map_err(|()| "LSP_FILE_URI_INVALID")?
                .to_string();
            let hash = crate::atomic::sha256_hex(text.as_bytes());
            let mut documents = slot.documents.lock().await;
            let previous = documents.get(&uri).cloned();
            let version = previous.as_ref().map_or(1, |(_, version)| version + 1);
            if previous.as_ref().is_none_or(|(old, _)| old != &hash) {
                peer.clear_diagnostics(&uri);
                match previous{
                    None=>peer.notify("textDocument/didOpen",json!({"textDocument":{"uri":uri,"languageId":document_language(&file,language),"version":version,"text":text}})).await?,
                    Some(_)=>peer.notify("textDocument/didChange",json!({"textDocument":{"uri":uri,"version":version},"contentChanges":[{"text":text}]})).await?,
                }
                documents.insert(uri.clone(), (hash, version));
            }
            let version = documents[&uri].1;
            (Some(uri), Some(text), version)
        } else {
            (None, None, 0)
        };
        let method = match action {
            "definition" => "textDocument/definition",
            "references" => "textDocument/references",
            "hover" => "textDocument/hover",
            "symbols" if uri.is_none() || workspace_symbols => "workspace/symbol",
            "symbols" => "textDocument/documentSymbol",
            "diagnostics" => "textDocument/diagnostic",
            "implementation" => "textDocument/implementation",
            "prepareCallHierarchy" | "incomingCalls" | "outgoingCalls" => {
                "textDocument/prepareCallHierarchy"
            }
            _ => return Err("LSP_ACTION_UNSUPPORTED".into()),
        };
        let mut params = match uri.as_ref().filter(|_| !workspace_symbols) {
            Some(uri) => json!({"textDocument":{"uri":uri}}),
            None => json!({"query":input.get("query").and_then(Value::as_str).unwrap_or("")}),
        };
        if !matches!(action, "symbols" | "diagnostics") {
            params["position"] = position(
                text.as_deref().ok_or("LSP_FILE_REQUIRED")?,
                input
                    .get("line")
                    .and_then(Value::as_u64)
                    .ok_or("LSP_LINE_REQUIRED")?,
                input
                    .get("column")
                    .or_else(|| input.get("character"))
                    .and_then(Value::as_u64)
                    .ok_or("LSP_COLUMN_REQUIRED")?,
            )?;
        }
        if action == "references" {
            params["context"] = json!({"includeDeclaration":true});
        }
        let result = if action == "diagnostics" {
            let uri = uri.as_deref().ok_or("LSP_FILE_REQUIRED")?;
            if peer
                .capabilities
                .get()
                .and_then(|capabilities| capabilities.get("diagnosticProvider"))
                .is_some_and(|value| value != &Value::Bool(false))
            {
                peer.request(method, params, Duration::from_mins(1), &context.cancel)
                    .await?
            } else {
                peer.diagnostics(uri, version, &context.cancel).await?
            }
        } else {
            let initial = peer
                .request(method, params, Duration::from_mins(1), &context.cancel)
                .await?;
            if matches!(action, "incomingCalls" | "outgoingCalls") {
                let mut results = Vec::new();
                for item in initial
                    .as_array()
                    .ok_or("LSP_CALL_HIERARCHY_UNAVAILABLE")?
                    .iter()
                    .take(32)
                {
                    results.push(
                        peer.request(
                            if action == "incomingCalls" {
                                "callHierarchy/incomingCalls"
                            } else {
                                "callHierarchy/outgoingCalls"
                            },
                            json!({"item":item}),
                            Duration::from_mins(1),
                            &context.cancel,
                        )
                        .await?,
                    );
                }
                Value::Array(results)
            } else {
                initial
            }
        };
        let installation = self
            .manager
            .installation
            .get()
            .and_then(|result| result.as_ref().ok());
        let mut readable_roots = vec![workspace];
        readable_roots.extend(sandbox::authorized_dependencies()?);
        if let Some(installation) = installation {
            readable_roots.push(installation.bundle.clone());
            readable_roots.push(installation.rust_toolchain_root.clone());
        }
        let mut omitted = 0;
        let result = restrict_locations(result, &readable_roots, &mut omitted);
        let output = json!({"language":language,"action":action,"result":result,"omittedOutsideAllowedRoots":omitted,"toolchainVersions":installation.map(|i|&i.versions)});
        if output.to_string().len() > 1024 * 1024 {
            return Err("LSP_RESULT_TOO_LARGE: narrow the query".into());
        }
        Ok(output)
    }
}
fn language_for(path: &Path) -> Option<&'static str> {
    match path.extension()?.to_str()? {
        "ts" | "tsx" | "js" | "jsx" | "mjs" | "cjs" | "mts" | "cts" => Some("typescript"),
        "py" | "pyi" => Some("python"),
        "rs" => Some("rust"),
        "go" => Some("go"),
        "java" => Some("java"),
        _ => None,
    }
}
fn document_language<'a>(path: &Path, language: &'a str) -> &'a str {
    match path.extension().and_then(|x| x.to_str()) {
        Some("tsx") => "typescriptreact",
        Some("jsx") => "javascriptreact",
        Some("js" | "mjs" | "cjs") => "javascript",
        _ => language,
    }
}
async fn read_document(path: PathBuf) -> Result<String, String> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let file = crate::safe_file::open_bound_regular(&path)
            .map_err(|_| "LSP_FILE_CHANGED_OR_UNREADABLE")?;
        if !file
            .metadata()
            .map_err(|_| "LSP_FILE_UNAVAILABLE")?
            .is_file()
        {
            return Err("LSP_FILE_NOT_REGULAR".into());
        }
        let mut bytes = Vec::new();
        file.take(10 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "LSP_FILE_READ_FAILED")?;
        if bytes.len() > 10 * 1024 * 1024 {
            return Err("LSP_FILE_TOO_LARGE".into());
        }
        String::from_utf8(bytes).map_err(|_| "LSP_FILE_NOT_UTF8".into())
    })
    .await
    .map_err(|_| "LSP_FILE_READ_FAILED")?
}
fn position(text: &str, line: u64, column: u64) -> Result<Value, String> {
    let line_index = usize::try_from(line.checked_sub(1).ok_or("LSP_POSITION_INVALID")?)
        .map_err(|_| "LSP_POSITION_INVALID")?;
    let column_index = usize::try_from(column.checked_sub(1).ok_or("LSP_POSITION_INVALID")?)
        .map_err(|_| "LSP_POSITION_INVALID")?;
    let line_text = text
        .split('\n')
        .nth(line_index)
        .ok_or("LSP_POSITION_INVALID")?
        .trim_end_matches('\r');
    if column_index > line_text.chars().count() {
        return Err("LSP_POSITION_INVALID".into());
    }
    let utf16 = line_text
        .chars()
        .take(column_index)
        .map(char::len_utf16)
        .sum::<usize>();
    Ok(json!({"line":line_index,"character":utf16}))
}
fn restrict_locations(value: Value, readable_roots: &[PathBuf], omitted: &mut usize) -> Value {
    match value {
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| restrict_locations(value, readable_roots, omitted))
                .filter(|v| !v.is_null())
                .collect(),
        ),
        Value::Object(mut map) => {
            for key in ["uri", "targetUri"] {
                if let Some(Value::String(uri)) = map.get(key) {
                    let allowed = url::Url::parse(uri)
                        .ok()
                        .and_then(|url| url.to_file_path().ok())
                        .and_then(|path| path.canonicalize().ok())
                        .is_some_and(|path| {
                            readable_roots.iter().any(|root| path.starts_with(root))
                        });
                    if !allowed {
                        *omitted += 1;
                        return Value::Null;
                    }
                }
            }
            for value in map.values_mut() {
                *value = restrict_locations(value.take(), readable_roots, omitted);
            }
            Value::Object(map)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn converts_unicode_character_columns_to_utf16() {
        assert_eq!(
            position("a😀文\nnext", 1, 3).unwrap(),
            json!({"line":0,"character":3})
        );
        assert!(position("hi", 0, 1).is_err());
        assert!(position("hi", 1, 4).is_err());
    }
    #[test]
    fn all_five_languages_have_explicit_routing() {
        for (path, language) in [
            ("file.tsx", "typescript"),
            ("file.js", "typescript"),
            ("file.py", "python"),
            ("file.rs", "rust"),
            ("file.go", "go"),
            ("File.java", "java"),
        ] {
            assert_eq!(language_for(Path::new(path)), Some(language));
        }
        assert_eq!(language_for(Path::new("file.sh")), None);
    }
}
