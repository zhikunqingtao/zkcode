//! 显式、封闭的操作分析器注册表；未知远程或动态工具只能获得 ONCE 授权。
//!
//! 逐字移植 `authorization/OperationAnalyzerRegistry.java`（623 行）+
//! `authorization/OperationAnalyzer.java`。
//!
//! # 6 个分析器（旧源 L50-56）
//!
//! | analyzerId | 覆盖工具 | 旧源内部类 |
//! |---|---|---|
//! | `bash-v2` | `Bash` | `BashAnalyzer`（L186-265） |
//! | `file-v1` | `FILE_READ` ∪ `FILE_WRITE` | `FileAnalyzer`（L293-455） |
//! | `network-v1` | `NETWORK` | `NetworkAnalyzer`（L457-467） |
//! | `artifact-publish-v1` | `PublishArtifact` | `ArtifactPublishAnalyzer`（L469-529） |
//! | `mcp-v1` | `tool.isMcp()` | `GenericAnalyzer("mcp-v1")` |
//! | `static-or-remote-v1` | 其余全部（含 `mcp__` 前缀伪装、`PowerShell`） | `GenericAnalyzer("static-or-remote-v1")` |
//!
//! # 未移植项（DEFERRED）
//!
//! - `hasUnsafeRelativeSegments`（旧源 L604-616）在基线中**无任何调用点**（死代码），
//!   不移植。见 docs §8 偏离表 A-09。

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use regex::Regex;
use serde_json::{Map, Value};

use crate::canonicalizer;
use crate::frozen::FrozenToolInput;
use crate::hashing;
use crate::model::{
    AuthorizationSubject, AuthzError, AuthzResult, EffectClass, OperationDescriptor, ResourceRef,
    RiskClass, TypedFileOperation,
};
use crate::path_security::{PathCheckResult, PathSecurityService};
use crate::tool_facts::{
    ArtifactPublicationPort, BashParseOutcome, BashSecurityPort, PassthroughFilter,
    PublicationSnapshot, SensitiveDataFilterPort, ShellStatePort, StatelessShellState, ToolFacts,
    ToolUseContext,
};

/// 旧源 `FILE_READ`（`OperationAnalyzerRegistry.java:34`）。
pub const FILE_READ: [&str; 5] = ["Read", "Glob", "Grep", "LSP", "Snip"];
/// 旧源 `FILE_WRITE`（L35）。
pub const FILE_WRITE: [&str; 3] = ["Write", "Edit", "NotebookEdit"];
/// 旧源 `NETWORK`（L36）。
pub const NETWORK: [&str; 3] = ["WebFetch", "WebSearch", "WebBrowser"];
/// 旧源 `CONTROL`（L37-39）。
pub const CONTROL: [&str; 17] = [
    "Config",
    "CronCreate",
    "CronDelete",
    "Worktree",
    "Agent",
    "TaskCreate",
    "TaskUpdate",
    "TaskStop",
    "SendMessage",
    "Git",
    "Skill",
    "REPL",
    "Memory",
    "Monitor",
    "TerminalCapture",
    "Visualization",
    "ReadMcpResource",
];
/// 旧源 `VERIFY_CONTROL`（L40）。
pub const VERIFY_CONTROL: [&str; 2] = ["VerifyPlanExecution", "VerifyJourney"];
/// 旧源 `SAFE_INTERNAL`（L41-43）。
pub const SAFE_INTERNAL: [&str; 15] = [
    "TodoWrite",
    "TaskList",
    "TaskGet",
    "TaskOutput",
    "AskUserQuestion",
    "Brief",
    "Sleep",
    "CtxInspect",
    "ToolSearch",
    "SyntheticOutput",
    "EnterPlanMode",
    "ExitPlanMode",
    "CronList",
    "ListMcpResources",
    "CodeIntel",
];

/// 旧源 `redactCommand` 的正则（`OperationAnalyzerRegistry.java:598-601`）。
static SECRET_ASSIGNMENT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(token|password|secret|api[_-]?key)=\S+").expect("static regex")
});

/// 分析器身份（旧源 6 个 `OperationAnalyzer` 实例的判别式）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzerKind {
    /// `bash-v2`
    Bash,
    /// `file-v1`
    File,
    /// `network-v1`
    Network,
    /// `artifact-publish-v1`
    ArtifactPublish,
    /// `mcp-v1`
    Mcp,
    /// `static-or-remote-v1`
    Generic,
}

impl AnalyzerKind {
    /// 旧源各分析器的 `id()`。
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Bash => "bash-v2",
            Self::File => "file-v1",
            Self::Network => "network-v1",
            Self::ArtifactPublish => "artifact-publish-v1",
            Self::Mcp => "mcp-v1",
            Self::Generic => "static-or-remote-v1",
        }
    }
}

/// 操作分析器注册表。
pub struct OperationAnalyzerRegistry {
    bash_security: Option<Arc<dyn BashSecurityPort>>,
    sensitive: Arc<dyn SensitiveDataFilterPort>,
    path_security: Arc<PathSecurityService>,
    shell_state: Arc<dyn ShellStatePort>,
    artifact_publication: Option<Arc<dyn ArtifactPublicationPort>>,
}

impl std::fmt::Debug for OperationAnalyzerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationAnalyzerRegistry")
            .finish_non_exhaustive()
    }
}

impl OperationAnalyzerRegistry {
    /// 完整构造（对应旧 `@Autowired` 构造器 L59-68 + `setArtifactPublicationPolicy`）。
    #[must_use]
    pub fn new(
        bash_security: Option<Arc<dyn BashSecurityPort>>,
        sensitive: Arc<dyn SensitiveDataFilterPort>,
        path_security: Arc<PathSecurityService>,
        shell_state: Arc<dyn ShellStatePort>,
    ) -> Self {
        Self {
            bash_security,
            sensitive,
            path_security,
            shell_state,
            artifact_publication: None,
        }
    }

    /// 旧源 `OperationAnalyzerRegistry(mapper, bashSecurity, filter, pathSecurity)`
    /// 测试构造器（L70-74）：`ShellStateManager` 取默认实例。
    #[must_use]
    pub fn for_tests(path_security: Arc<PathSecurityService>) -> Self {
        Self::new(
            None,
            Arc::new(PassthroughFilter),
            path_security,
            Arc::new(StatelessShellState),
        )
    }

    /// 旧源 `setArtifactPublicationPolicy`（L77-79，`@Autowired(required = false)`）。
    #[must_use]
    pub fn with_artifact_publication(mut self, policy: Arc<dyn ArtifactPublicationPort>) -> Self {
        self.artifact_publication = Some(policy);
        self
    }

    /// 旧源 `analyzerFor(Tool)`（`OperationAnalyzerRegistry.java:81-95`）。
    #[must_use]
    pub fn analyzer_for(&self, tool: &dyn ToolFacts) -> AnalyzerKind {
        if tool.is_mcp() {
            return AnalyzerKind::Mcp;
        }
        // 名称看似 MCP 但没有适配器身份的动态工具仍按未知工具处理，
        // 不能继承 MCP 持久授权。
        let name = tool.name();
        if name.starts_with("mcp__") {
            return AnalyzerKind::Generic;
        }
        if name == "Bash" {
            return AnalyzerKind::Bash;
        }
        // Bash 语法无法证明 PowerShell 的语义，因此 PowerShell 保持精确 ONCE 授权。
        if name == "PowerShell" {
            return AnalyzerKind::Generic;
        }
        if name == "PublishArtifact" {
            return AnalyzerKind::ArtifactPublish;
        }
        if FILE_READ.contains(&name) || FILE_WRITE.contains(&name) {
            return AnalyzerKind::File;
        }
        if NETWORK.contains(&name) {
            return AnalyzerKind::Network;
        }
        // CONTROL / VERIFY_CONTROL / SAFE_INTERNAL 与未登记动态工具同走 generic。
        AnalyzerKind::Generic
    }

    /// 旧源 `isExplicitCoreTool(String)`（L97-103）。
    #[must_use]
    pub fn is_explicit_core_tool(name: &str) -> bool {
        name == "Bash"
            || name == "PowerShell"
            || name == "PublishArtifact"
            || FILE_READ.contains(&name)
            || FILE_WRITE.contains(&name)
            || NETWORK.contains(&name)
            || CONTROL.contains(&name)
            || VERIFY_CONTROL.contains(&name)
            || SAFE_INTERNAL.contains(&name)
    }

    /// 旧源 `bindExecutionInput`（L105-146）。
    ///
    /// 只替换文件资源字段为已分析并批准的规范目标；其余参数与冻结输入逐字节等价，
    /// 使后续 symlink 别名重绑无法把执行重定向到另一个文件。
    #[must_use]
    pub fn bind_execution_input(
        tool: &dyn ToolFacts,
        descriptor: &OperationDescriptor,
        input: &Value,
        subject: &AuthorizationSubject,
    ) -> Option<Value> {
        if descriptor.analyzer_id != "file-v1" {
            return None;
        }
        let resource = descriptor
            .resources
            .iter()
            .find(|candidate| candidate.kind == "path")?;
        let canonical = if resource.outside_workspace {
            PathBuf::from(&resource.value)
        } else {
            subject.authorization_root.join(&resource.value)
        };
        let path = crate::workspace::absolute_normalized(&canonical)
            .to_string_lossy()
            .into_owned();
        let mut bound = match input {
            Value::Object(map) => map.clone(),
            _ => Map::new(),
        };
        match tool.name() {
            "Glob" | "Grep" => {
                bound.insert("path".into(), path.into());
            }
            "LSP" => {
                let mut replaced = false;
                if bound.contains_key("filePath") {
                    bound.insert("filePath".into(), path.clone().into());
                    replaced = true;
                }
                if bound.contains_key("file_path") {
                    bound.insert("file_path".into(), path.into());
                    replaced = true;
                }
                if !replaced {
                    return None;
                }
            }
            "NotebookEdit" => {
                bound.insert("notebook_path".into(), path.into());
            }
            _ => {
                if !bound.contains_key("file_path") {
                    return None;
                }
                bound.insert("file_path".into(), path.into());
            }
        }
        Some(Value::Object(bound))
    }

    /// 旧源 `descriptor(...)` 全部三个重载的最终实现（L166-184）。
    ///
    /// 哈希与描述符共享同一份规范化事实，避免 `List` 顺序在构造后变化导致最终复检
    /// 误拒绝。
    #[allow(clippy::too_many_arguments)]
    fn descriptor(
        analyzer: &str,
        tool_name: &str,
        input_hash: &str,
        action: &str,
        effects: &[EffectClass],
        resources: &[ResourceRef],
        environment: &[String],
        endpoints: &[String],
        risk: RiskClass,
        summary: &str,
        authorization_input: &Value,
    ) -> OperationDescriptor {
        let canonical_effects = canonicalizer::effects(effects);
        let canonical_resources = canonicalizer::resources(resources);
        let canonical_environment = canonicalizer::strings(environment);
        let canonical_endpoints = canonicalizer::strings(endpoints);
        let mut facts = Map::new();
        facts.insert("schema".into(), Value::from(1));
        facts.insert("tool".into(), tool_name.into());
        facts.insert("action".into(), action.into());
        facts.insert("authorizationInput".into(), authorization_input.clone());
        facts.insert("analyzer".into(), analyzer.into());
        facts.insert(
            "effects".into(),
            Value::Array(
                canonical_effects
                    .iter()
                    .map(|e| Value::from(e.as_str()))
                    .collect(),
            ),
        );
        facts.insert(
            "resources".into(),
            Value::Array(canonical_resources.iter().map(resource_facts).collect()),
        );
        facts.insert(
            "environment".into(),
            Value::Array(
                canonical_environment
                    .iter()
                    .map(|v| Value::from(v.as_str()))
                    .collect(),
            ),
        );
        facts.insert(
            "endpoints".into(),
            Value::Array(
                canonical_endpoints
                    .iter()
                    .map(|v| Value::from(v.as_str()))
                    .collect(),
            ),
        );
        facts.insert("risk".into(), risk.as_str().into());
        let operation_hash = hashing::operation_hash(&Value::Object(facts));
        OperationDescriptor {
            authorization_schema_version: 1,
            tool_name: tool_name.to_owned(),
            action: action.to_owned(),
            input_hash: input_hash.to_owned(),
            analyzer_id: analyzer.to_owned(),
            effects: canonical_effects,
            resources: canonical_resources,
            inherited_environment_names: canonical_environment,
            endpoints: canonical_endpoints,
            risk,
            operation_hash,
            redacted_summary: summary.to_owned(),
        }
    }

    /// 旧源 `OperationAnalyzer#analyze` 的统一入口（按 [`AnalyzerKind`] 分派）。
    ///
    /// # Errors
    /// 命令黑名单绝对拒绝（`COMMAND_ABSOLUTELY_DENIED`）、工作目录非法、路径协议
    /// 拒绝、产物发布策略拒绝等分析期失败原样上抛 —— 全部先于决策链，不可绕过。
    pub fn analyze(
        &self,
        kind: AnalyzerKind,
        tool: &dyn ToolFacts,
        frozen: &FrozenToolInput,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<OperationDescriptor> {
        match kind {
            AnalyzerKind::Bash => {
                self.analyze_bash(tool, frozen.input_hash(), input, context, subject)
            }
            AnalyzerKind::File => {
                self.analyze_file(tool, frozen.input_hash(), input, context, subject)
            }
            AnalyzerKind::Network => Ok(Self::analyze_network(tool, frozen.input_hash(), input)),
            AnalyzerKind::ArtifactPublish => {
                let snapshot = self.publication_snapshot(input, context)?;
                Ok(Self::publication_descriptor(
                    tool,
                    frozen.input_hash(),
                    &snapshot,
                ))
            }
            AnalyzerKind::Mcp => Ok(Self::analyze_mcp(tool, frozen.input_hash())),
            AnalyzerKind::Generic => {
                Ok(Self::analyze_generic(kind.id(), tool, frozen.input_hash()))
            }
        }
    }

    /// 旧源 `OperationAnalyzer#recheck` 的统一入口（最终动态复检）。
    ///
    /// # Errors
    /// 风险等级上升、资源集漂移、继承环境变量集漂移或操作身份变化时返回拒绝。
    pub fn recheck(
        &self,
        kind: AnalyzerKind,
        tool: &dyn ToolFacts,
        descriptor: &OperationDescriptor,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<()> {
        match kind {
            AnalyzerKind::Bash => self.recheck_bash(tool, descriptor, input, context, subject),
            AnalyzerKind::File => self.recheck_file(tool, descriptor, input, context, subject),
            AnalyzerKind::ArtifactPublish => {
                let current = self.publication_snapshot(input, context)?;
                let recomputed =
                    Self::publication_descriptor(tool, &descriptor.input_hash, &current);
                if descriptor.operation_hash != recomputed.operation_hash
                    || descriptor.resources != recomputed.resources
                    || descriptor.risk != RiskClass::High
                {
                    return Err(final_recheck_denied(
                        "Artifact or OSS publication facts changed before execution",
                    ));
                }
                Ok(())
            }
            // NetworkAnalyzer / GenericAnalyzer 的 recheck 为空实现（旧源 L466、L495）。
            AnalyzerKind::Network | AnalyzerKind::Mcp | AnalyzerKind::Generic => Ok(()),
        }
    }
}

/// `ResourceRef` 参与 `operationHash` 的 JSON 形状（Jackson record 序列化：
/// 字段声明序 `kind` / `value` / `outsideWorkspace`）。
fn resource_facts(resource: &ResourceRef) -> Value {
    let mut map = Map::new();
    map.insert("kind".into(), resource.kind.clone().into());
    map.insert("value".into(), resource.value.clone().into());
    map.insert("outsideWorkspace".into(), resource.outside_workspace.into());
    Value::Object(map)
}

fn final_recheck_denied(message: &str) -> AuthzError {
    AuthzError::new("AUTHORIZATION_FINAL_RECHECK_DENIED", message)
}

// ==================== ToolInput 读取原语 ====================

/// 旧 `ToolInput#getString(name, default)`。
#[must_use]
pub fn input_string(input: &Value, name: &str, default: &str) -> String {
    match input.get(name) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null) | None => default.to_owned(),
        Some(other) => other.to_string(),
    }
}

/// 旧 `ToolInput#getBoolean(name, default)`。
#[must_use]
pub fn input_bool(input: &Value, name: &str, default: bool) -> bool {
    input.get(name).and_then(Value::as_bool).unwrap_or(default)
}

/// 旧 `ToolInput#has(name)`。
#[must_use]
pub fn input_has(input: &Value, name: &str) -> bool {
    input.get(name).is_some_and(|value| !value.is_null())
}

/// 旧源 `first(ToolInput, String...)`（L580-583）。
#[must_use]
pub fn first(input: &Value, names: &[&str]) -> Option<String> {
    for name in names {
        if input_has(input, name) {
            return match input.get(*name) {
                Some(Value::String(text)) => Some(text.clone()),
                Some(other) => Some(other.to_string()),
                None => None,
            };
        }
    }
    None
}

/// 旧源 `fileOperation(String)`（L584-591）。
#[must_use]
pub fn file_operation(name: &str) -> TypedFileOperation {
    match name {
        "Glob" | "Grep" => TypedFileOperation::ListDirectory,
        "Write" => TypedFileOperation::ReplaceFile,
        "Edit" | "NotebookEdit" => TypedFileOperation::PatchFile,
        _ => TypedFileOperation::ReadFile,
    }
}

/// 旧源 `redactCommand(String)`（L597-602）：秘密赋值脱敏 + 240 字符截断。
#[must_use]
pub fn redact_command(command: &str) -> String {
    let compact = SECRET_ASSIGNMENT
        .replace_all(command, "$1=<redacted>")
        .into_owned();
    if compact.chars().count() > 240 {
        let head: String = compact.chars().take(240).collect();
        format!("{head}…")
    } else {
        compact
    }
}

/// 旧源 `redactEndpoint(String)`（L617-620）：只留 `scheme://host[:port]`。
#[must_use]
pub fn redact_endpoint(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return "<remote-endpoint>".to_owned();
    };
    if scheme.is_empty() {
        return "<remote-endpoint>".to_owned();
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    // 去掉 userinfo（`URI#getHost` 不含）。
    let authority = authority.rsplit('@').next().unwrap_or_default();
    if authority.is_empty() {
        return "<remote-endpoint>".to_owned();
    }
    format!("{scheme}://{authority}")
}

/// 旧源 `cwdResource(Path, AuthorizationSubject)`（L573-579）。
#[must_use]
pub fn cwd_resource(cwd: &Path, subject: &AuthorizationSubject) -> ResourceRef {
    relative_resource("cwd", cwd, subject)
}

/// 旧源 `canonicalResource(Path, AuthorizationSubject)`（L564-570）。
#[must_use]
pub fn canonical_resource(target: &Path, subject: &AuthorizationSubject) -> ResourceRef {
    relative_resource("path", target, subject)
}

fn relative_resource(kind: &str, target: &Path, subject: &AuthorizationSubject) -> ResourceRef {
    let normalized = crate::workspace::absolute_normalized(target);
    let outside = !normalized.starts_with(&subject.authorization_root);
    let value = if outside {
        normalized.to_string_lossy().into_owned()
    } else {
        normalized
            .strip_prefix(&subject.authorization_root)
            .map(|rest| rest.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default()
    };
    ResourceRef::new(kind, if value.is_empty() { "." } else { &value }, outside)
}

// ==================== 各分析器私有实现（旧源 L186-562）====================

/// `BashAnalyzer#analyze` / `#recheck` 共享的一次性事实（旧源 L189-214 与
/// L221-244 逐字重复的那一段）。
struct BashFacts {
    command: String,
    cwd: PathBuf,
    inherited: Vec<String>,
    risk: RiskClass,
    effects: Vec<EffectClass>,
    authorization_input: Value,
}

/// 旧源 `FilePathFacts`（`OperationAnalyzerRegistry.java:621`）。
struct FilePathFacts {
    resource: ResourceRef,
    sensitive: bool,
}

/// 旧源 `descriptor(analyzer, tool, frozen, ..., summary)` 十参重载（L145-151）
/// 隐式注入的 `authorizationInput`：**不是空 Map，而是 `{"inputHash": …}`**。
///
/// `NetworkAnalyzer`（L462-463）与 `GenericAnalyzer`（L542-544）都走该重载，
/// 因此它们的 `operationHash` 含 `inputHash` —— 即「精确一次调用」语义：输入变则
/// 身份变，无法复用授权。此细节直接决定 `TOOL_GUARDED` 之外的免弹范围，必须逐字保留。
fn input_hash_facts(input_hash: &str) -> Value {
    let mut map = Map::new();
    map.insert("inputHash".into(), input_hash.into());
    Value::Object(map)
}

/// 旧 `Path.of(String)` 对含 NUL 的字符串抛 `InvalidPathException`；Rust 的
/// `PathBuf` 会接受它，故在两个 `Path.of` 调用点显式补检查以保持失败关闭语义。
fn has_nul(value: &str) -> bool {
    value.contains('\0')
}

impl OperationAnalyzerRegistry {
    // ---------- BashAnalyzer（旧源 L186-265）----------

    /// 旧源 L189-214 / L221-244：解析 → 绝对拒绝 → 环境引用 → 风险 → 副作用 →
    /// `authorizationInput`。
    fn bash_facts(
        &self,
        tool: &dyn ToolFacts,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<BashFacts> {
        let command = input_string(input, "command", "");
        let cwd = self.shell_working_directory(context, subject)?;
        let Some(bash_security) = self.bash_security.as_ref() else {
            // 旧源该字段由构造器注入且非空；缺失在 Java 侧会 NPE（500）。此处失败关闭。
            return Err(AuthzError::new(
                "BASH_SECURITY_ANALYZER_UNAVAILABLE",
                "Shell security analyzer is unavailable",
            ));
        };
        match bash_security.parse_for_security(&command, &cwd, &subject.authorization_root) {
            // 旧源 L192-195：`nodeType == "command-blacklist-deny"` 是 ABSOLUTE_DENY
            // 的唯一入口，先于任何风险分级与授权匹配抛出，不可绕过。
            BashParseOutcome::BlacklistDeny { reason } => {
                return Err(AuthzError::new("COMMAND_ABSOLUTELY_DENIED", reason));
            }
            // 旧源 L196-198：解析过于复杂只记日志并回落 GUARDED，不拒绝。
            BashParseOutcome::TooComplex { reason } => {
                tracing::debug!(
                    reason = %reason,
                    "Shell parse too complex for command, defaulting to GUARDED"
                );
            }
            BashParseOutcome::Parsed => {}
        }
        let mut inherited = bash_security.inherited_environment_references(&command);
        inherited.sort();
        let risk = if tool.is_destructive(input) {
            RiskClass::High
        } else if tool.is_read_only(input) {
            RiskClass::Safe
        } else {
            RiskClass::Guarded
        };
        let effects = if tool.is_read_only(input) {
            vec![EffectClass::Process, EffectClass::ReadResource]
        } else {
            vec![EffectClass::Process, EffectClass::WriteResource]
        };
        let mut authorization_input = Map::new();
        authorization_input.insert("command".into(), command.clone().into());
        authorization_input.insert(
            "isBackground".into(),
            input_bool(input, "is_background", false).into(),
        );
        authorization_input.insert(
            "dynamicEnvironmentHash".into(),
            self.dynamic_environment_hash(&inherited).into(),
        );
        Ok(BashFacts {
            command,
            cwd,
            inherited,
            risk,
            effects,
            authorization_input: Value::Object(authorization_input),
        })
    }

    /// 旧源 `BashAnalyzer#analyze`（L188-219）。
    fn analyze_bash(
        &self,
        tool: &dyn ToolFacts,
        input_hash: &str,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<OperationDescriptor> {
        let facts = self.bash_facts(tool, input, context, subject)?;
        let summary = self.sensitive.filter(&redact_command(&facts.command));
        Ok(Self::descriptor(
            AnalyzerKind::Bash.id(),
            tool.name(),
            input_hash,
            "execute",
            &facts.effects,
            &[cwd_resource(&facts.cwd, subject)],
            &facts.inherited,
            &[],
            facts.risk,
            &summary,
            &facts.authorization_input,
        ))
    }

    /// 旧源 `BashAnalyzer#recheck`（L221-264）：四路比对，任一变化即最终拒绝。
    fn recheck_bash(
        &self,
        tool: &dyn ToolFacts,
        descriptor: &OperationDescriptor,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<()> {
        let facts = self.bash_facts(tool, input, context, subject)?;
        let current_resources = vec![cwd_resource(&facts.cwd, subject)];
        // 旧源刻意复用「已批准」的 effects 与 summary，只让 risk / resources /
        // environment / authorizationInput 参与漂移检测。
        let current = Self::descriptor(
            AnalyzerKind::Bash.id(),
            tool.name(),
            &descriptor.input_hash,
            "execute",
            &descriptor.effects,
            &current_resources,
            &facts.inherited,
            &[],
            facts.risk,
            &descriptor.redacted_summary,
            &facts.authorization_input,
        );
        let risk_changed = current.risk != descriptor.risk;
        let resources_changed = current_resources != descriptor.resources;
        let environment_changed = facts.inherited != descriptor.inherited_environment_names;
        let operation_changed = current.operation_hash != descriptor.operation_hash;
        if risk_changed || resources_changed || environment_changed || operation_changed {
            tracing::info!(
                risk_changed,
                resources_changed,
                environment_changed,
                operation_hash_changed = operation_changed,
                "Bash authorization facts changed before execution"
            );
            return Err(final_recheck_denied(
                "Shell security facts changed before execution",
            ));
        }
        Ok(())
    }

    /// 旧源 `shellWorkingDirectory`（L267-280）。
    fn shell_working_directory(
        &self,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<PathBuf> {
        let configured = match context.working_directory.as_deref() {
            None => subject.authorization_root.to_string_lossy().into_owned(),
            Some(configured) => configured.to_owned(),
        };
        let resolved = match context.session_id.as_deref() {
            None => configured,
            Some(session_id) => self
                .shell_state
                .resolve_working_directory(session_id, &configured),
        };
        let invalid = || {
            AuthzError::new(
                "BASH_WORKING_DIRECTORY_INVALID",
                "Shell working directory cannot be resolved",
            )
        };
        if has_nul(&resolved) {
            return Err(invalid());
        }
        let requested = Path::new(&resolved);
        let candidate = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            subject.authorization_root.join(requested)
        };
        crate::workspace::absolute_normalized(&candidate)
            .canonicalize()
            .map_err(|_| invalid())
    }

    /// 旧源 `dynamicEnvironmentHash`（L286-291）：把会改变 Shell 语义的有效环境
    /// 指纹并入 `authorizationInput`，使可复用授权自动随环境失效。
    fn dynamic_environment_hash(&self, inherited: &[String]) -> String {
        hashing::operation_hash(&self.shell_state.authorization_environment_facts(inherited))
    }

    // ---------- FileAnalyzer（旧源 L293-455）----------

    /// 旧源 `FileAnalyzer#analyze`（L296-314）。
    fn analyze_file(
        &self,
        tool: &dyn ToolFacts,
        input_hash: &str,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<OperationDescriptor> {
        let write = FILE_WRITE.contains(&tool.name());
        let path_facts = self.file_path_facts(tool, write, input, context, subject)?;
        let mut resources = Self::file_resources(tool, path_facts.as_ref());
        let risk = Self::file_risk(write, path_facts.as_ref());
        let operation = Self::file_typed_operation(tool, &resources);
        if resources.is_empty() && operation == TypedFileOperation::ListDirectory {
            resources = vec![ResourceRef::new("path", ".", false)];
        }
        let summary = Self::file_summary(tool, path_facts.as_ref());
        Ok(Self::descriptor(
            AnalyzerKind::File.id(),
            tool.name(),
            input_hash,
            operation.as_str(),
            &[if write {
                EffectClass::WriteResource
            } else {
                EffectClass::ReadResource
            }],
            &resources,
            &[],
            &[],
            risk,
            &summary,
            &Value::Object(Map::new()),
        ))
    }

    /// 旧源 `FileAnalyzer#recheck`（L315-338）。
    fn recheck_file(
        &self,
        tool: &dyn ToolFacts,
        descriptor: &OperationDescriptor,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<()> {
        let write = FILE_WRITE.contains(&tool.name());
        let current = self.file_path_facts(tool, write, input, context, subject)?;
        let mut resources = Self::file_resources(tool, current.as_ref());
        let operation = Self::file_typed_operation(tool, &resources);
        if resources.is_empty() && operation == TypedFileOperation::ListDirectory {
            resources = vec![ResourceRef::new("path", ".", false)];
        }
        if descriptor.resources != canonicalizer::resources(&resources)
            || Self::risk_increased(Self::file_risk(write, current.as_ref()), descriptor.risk)
        {
            return Err(final_recheck_denied(
                "File target or security facts changed before execution",
            ));
        }
        Ok(())
    }

    /// 旧源 `raw == null || raw.isBlank() ? null : inspect(...)`（L298-300、L317-319）。
    fn file_path_facts(
        &self,
        tool: &dyn ToolFacts,
        write: bool,
        input: &Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<Option<FilePathFacts>> {
        let raw = Self::raw_path(tool, input, context);
        match raw.as_deref() {
            Some(raw) if !raw.trim().is_empty() => {
                Ok(Some(self.inspect(tool, write, raw, context, subject)?))
            }
            _ => Ok(None),
        }
    }

    /// 旧源 `pathFacts == null ? List.of() : List.of(pathFacts.resource())`。
    fn file_resources(_tool: &dyn ToolFacts, facts: Option<&FilePathFacts>) -> Vec<ResourceRef> {
        facts
            .map(|facts| vec![facts.resource.clone()])
            .unwrap_or_default()
    }

    /// 旧源 `"LSP".equals(tool.getName()) && resources.isEmpty() ? LIST_DIRECTORY
    /// : fileOperation(tool.getName())`（L303-304、L321-325）。
    fn file_typed_operation(tool: &dyn ToolFacts, resources: &[ResourceRef]) -> TypedFileOperation {
        if tool.name() == "LSP" && resources.is_empty() {
            TypedFileOperation::ListDirectory
        } else {
            file_operation(tool.name())
        }
    }

    /// 旧源 `FileAnalyzer#inspect`（L340-368）。
    ///
    /// 关键不变量：**授权用 canonical（`toRealPath` 后）资源身份，但保护检查用
    /// lexical（仅词法规范化）路径**。前者让符号链接改绑触发身份漂移，后者阻止用
    /// 「解析后落回工作区」的软链绕过受保护路径。
    fn inspect(
        &self,
        tool: &dyn ToolFacts,
        write: bool,
        raw: &str,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<FilePathFacts> {
        if has_nul(raw) {
            return Err(AuthzError::new(
                "PROTECTED_PATH_DENIED",
                "Invalid file path",
            ));
        }
        let base = Self::file_path_base(context, subject)?;
        // 旧源 L349-359：`resolvePath` 对 UNC 抛 `IllegalArgumentException`，
        // 被 catch 后统一回 `PROTECTED_PATH_DENIED`（消息透传）。
        let canonical = self
            .path_security
            .resolve_path(raw, &base.to_string_lossy())
            .map_err(|unsafe_path| AuthzError::new("PROTECTED_PATH_DENIED", unsafe_path))?;
        let requested = Path::new(raw);
        let lexical = crate::workspace::absolute_normalized(&if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            base.join(requested)
        });
        let check = self.path_check(tool, write, &lexical, subject);
        if !check.is_allowed {
            return Err(AuthzError::new(
                "PROTECTED_PATH_DENIED",
                check
                    .message
                    .unwrap_or_else(|| "Path access denied".to_owned()),
            ));
        }
        Ok(FilePathFacts {
            resource: canonical_resource(&canonical, subject),
            sensitive: check.needs_confirmation,
        })
    }

    /// 旧源 `FileAnalyzer#pathCheck`（L370-386）。
    fn path_check(
        &self,
        tool: &dyn ToolFacts,
        write: bool,
        absolute: &Path,
        subject: &AuthorizationSubject,
    ) -> PathCheckResult {
        let workspace = subject.authorization_root.to_string_lossy();
        let target = absolute.to_string_lossy();
        if write {
            return self
                .path_security
                .check_authorized_write_permission(&target, &workspace);
        }
        if tool.name() == "Glob" || tool.name() == "Grep" {
            return self
                .path_security
                .check_authorized_recursive_read_root_permission(&target, &workspace);
        }
        self.path_security
            .check_authorized_read_permission(&target, &workspace)
    }

    /// 旧源 `FileAnalyzer#rawPath`（L388-400）。
    fn raw_path(tool: &dyn ToolFacts, input: &Value, context: &ToolUseContext) -> Option<String> {
        match tool.name() {
            // 旧源 `input.getString("path", context.workingDirectory())`：缺省回落
            // 到上下文工作目录（可为 null）。
            "Glob" | "Grep" => {
                if input_has(input, "path") {
                    first(input, &["path"])
                } else {
                    context.working_directory.clone()
                }
            }
            "LSP" => first(input, &["filePath", "file_path"]),
            _ => tool
                .path_of(input)
                .or_else(|| first(input, &["file_path", "path", "notebook_path"])),
        }
    }

    /// 旧源 `FileAnalyzer#fileRisk`（L402-412）。
    fn file_risk(write: bool, facts: Option<&FilePathFacts>) -> RiskClass {
        if facts.is_some_and(|facts| facts.sensitive) {
            return RiskClass::High;
        }
        if write || facts.is_some_and(|facts| facts.resource.outside_workspace) {
            return RiskClass::Guarded;
        }
        RiskClass::Safe
    }

    /// 旧源 `FileAnalyzer#fileSummary`（L414-441）。
    ///
    /// 工作区外目标把 `$HOME` 前缀折叠为 `~/`（旧源读 `user.home` 系统属性；Rust
    /// 侧等价读 `HOME` 环境变量）。
    fn file_summary(tool: &dyn ToolFacts, facts: Option<&FilePathFacts>) -> String {
        let Some(facts) = facts else {
            return tool.name().to_owned();
        };
        let resource = &facts.resource;
        if !resource.outside_workspace {
            return format!("{} inside Project: {}", tool.name(), resource.value);
        }
        let mut value = resource.value.clone();
        if let Some(configured_home) = std::env::var_os("HOME")
            && !configured_home.is_empty()
        {
            let home = crate::workspace::absolute_normalized(Path::new(&configured_home));
            let target = crate::workspace::absolute_normalized(Path::new(&value));
            if let Ok(relative) = target.strip_prefix(&home) {
                value = format!("~/{}", relative.to_string_lossy().replace('\\', "/"));
            }
        }
        format!("{} outside Project: {}", tool.name(), value)
    }

    /// 旧源 `FileAnalyzer#riskIncreased` + `#riskRank`（L443-454）。
    fn risk_increased(current: RiskClass, authorized: RiskClass) -> bool {
        current.rank() > authorized.rank()
    }

    /// 旧源 `filePathBase`（L549-562）。
    fn file_path_base(
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
    ) -> AuthzResult<PathBuf> {
        let mut base = subject.authorization_root.clone();
        if let Some(configured) = context.working_directory.as_deref() {
            if has_nul(configured) {
                return Err(AuthzError::new(
                    "WORKSPACE_PATH_INVALID",
                    "Invalid file working directory",
                ));
            }
            let configured = Path::new(configured);
            base = if configured.is_absolute() {
                configured.to_path_buf()
            } else {
                subject.authorization_root.join(configured)
            };
        }
        Ok(crate::workspace::absolute_normalized(&base))
    }

    // ---------- NetworkAnalyzer（旧源 L457-467）----------

    /// 旧源 `NetworkAnalyzer#analyze`（L459-464）。`recheck` 为空实现。
    fn analyze_network(
        tool: &dyn ToolFacts,
        input_hash: &str,
        input: &Value,
    ) -> OperationDescriptor {
        let url = first(input, &["url", "uri", "endpoint"]);
        let endpoints: Vec<String> = url
            .map(|url| vec![redact_endpoint(&url)])
            .unwrap_or_default();
        Self::descriptor(
            AnalyzerKind::Network.id(),
            tool.name(),
            input_hash,
            "network",
            &[EffectClass::Network],
            &[],
            &[],
            &endpoints,
            RiskClass::Guarded,
            &format!("{} remote request", tool.name()),
            &input_hash_facts(input_hash),
        )
    }

    // ---------- ArtifactPublishAnalyzer（旧源 L469-529）----------

    /// 旧源 `ArtifactPublishAnalyzer#publicationSnapshot`（L491-510）。
    ///
    /// 旧源把三类异常分别映射为 `denied.code()` / `OssConfigurationException` 消息 /
    /// `ARTIFACT_PUBLISH_POLICY_DENIED`；本 crate 通过 [`ArtifactPublicationPort`]
    /// 返回 `(code, message)`，映射由 zk-server 侧适配器完成，语义一致。
    fn publication_snapshot(
        &self,
        input: &Value,
        context: &ToolUseContext,
    ) -> AuthzResult<PublicationSnapshot> {
        let Some(policy) = self.artifact_publication.as_ref() else {
            return Err(AuthzError::new(
                "ARTIFACT_PUBLISH_ANALYZER_UNAVAILABLE",
                "Artifact publication policy is unavailable",
            ));
        };
        policy
            .inspect(
                &input_string(input, "file_path", ""),
                context.current_run_id.as_deref(),
            )
            .map_err(|(code, message)| AuthzError::new(code, message))
    }

    /// 旧源 `ArtifactPublishAnalyzer#publicationDescriptor`（L512-527）。
    fn publication_descriptor(
        tool: &dyn ToolFacts,
        input_hash: &str,
        snapshot: &PublicationSnapshot,
    ) -> OperationDescriptor {
        let summary = format!(
            "PERMANENT PUBLIC OSS upload\nFile: {}\nSize: {} bytes\nSHA-256: {}\nBucket: {}\nPublic URL: {}",
            snapshot.relative_path,
            snapshot.size,
            snapshot.sha256,
            snapshot.bucket,
            snapshot.public_url
        );
        Self::descriptor(
            AnalyzerKind::ArtifactPublish.id(),
            tool.name(),
            input_hash,
            "publish-public-artifact",
            &[
                EffectClass::ReadResource,
                EffectClass::Network,
                EffectClass::WriteResource,
            ],
            &[ResourceRef::new(
                "path",
                snapshot.relative_path.clone(),
                false,
            )],
            &[],
            &[redact_endpoint(&snapshot.endpoint)],
            RiskClass::High,
            &summary,
            &snapshot.authorization_facts,
        )
    }

    // ---------- GenericAnalyzer（旧源 L531-547）----------

    /// MCP 专属描述符。身份事实进入 operation hash，server/config/capability 同时
    /// 进入 action 与 resource，持久授权不会被同名工具或重配置后的 server 复用。
    fn analyze_mcp(tool: &dyn ToolFacts, input_hash: &str) -> OperationDescriptor {
        let server_id = tool.mcp_server_id().unwrap_or("unknown-server");
        let server_name = tool.mcp_server().unwrap_or(server_id);
        let remote_tool = tool.mcp_tool_name().unwrap_or(tool.name());
        let capability_id = tool.mcp_capability_id().unwrap_or("unregistered");
        let config_hash = tool.mcp_config_hash().unwrap_or("missing-config-hash");
        let resource_scope = tool
            .mcp_resource_scope()
            .map_or_else(|| format!("mcp://{server_id}/{remote_tool}"), str::to_owned);
        let facts = serde_json::json!({
            "inputHash": input_hash,
            "serverId": server_id,
            "serverName": server_name,
            "toolName": remote_tool,
            "capabilityId": capability_id,
            "resourceScope": resource_scope,
            "domainScope": tool.mcp_domain_scope(),
            "configHash": config_hash,
        });
        let action = format!("invoke:{capability_id}:{config_hash}");
        let summary = format!("MCP {server_name}/{remote_tool} exact invocation");
        Self::descriptor(
            AnalyzerKind::Mcp.id(),
            tool.name(),
            input_hash,
            &action,
            &[EffectClass::Unknown],
            &[ResourceRef::new("mcp", resource_scope, false)],
            &[],
            &[],
            RiskClass::Guarded,
            &summary,
            &facts,
        )
    }

    /// 旧源 `GenericAnalyzer#analyze`（L539-545）。`recheck` 为空实现。
    ///
    /// `SAFE_INTERNAL` 名单命中但 `isMcp()` 为真时**不**降级为安全内部操作 —— MCP
    /// 服务器可任意声明与内建工具同名的工具，名单不可跨信任域生效。
    fn analyze_generic(
        analyzer_id: &str,
        tool: &dyn ToolFacts,
        input_hash: &str,
    ) -> OperationDescriptor {
        let safe = SAFE_INTERNAL.contains(&tool.name()) && !tool.is_mcp();
        let effect = if safe {
            EffectClass::SafeInternal
        } else if CONTROL.contains(&tool.name()) || VERIFY_CONTROL.contains(&tool.name()) {
            EffectClass::ControlPlane
        } else {
            EffectClass::Unknown
        };
        let summary = if safe {
            tool.name().to_owned()
        } else {
            format!("{} exact invocation", tool.name())
        };
        Self::descriptor(
            analyzer_id,
            tool.name(),
            input_hash,
            if safe { "internal" } else { "invoke" },
            &[effect],
            &[],
            &[],
            &[],
            if safe {
                RiskClass::Safe
            } else {
                RiskClass::Guarded
            },
            &summary,
            &input_hash_facts(input_hash),
        )
    }
}

#[cfg(test)]
mod mcp_identity_tests {
    use super::*;

    struct McpFacts {
        server: &'static str,
        config_hash: &'static str,
    }

    impl ToolFacts for McpFacts {
        fn name(&self) -> &'static str {
            // 故意让两个 server 的注册名相同，证明隔离不依赖外层名称前缀。
            "mcp__shared__echo"
        }

        fn is_mcp(&self) -> bool {
            true
        }

        fn mcp_server(&self) -> Option<&str> {
            Some(self.server)
        }

        fn mcp_server_id(&self) -> Option<&str> {
            Some(self.server)
        }

        fn mcp_tool_name(&self) -> Option<&str> {
            Some("echo")
        }

        fn mcp_capability_id(&self) -> Option<&str> {
            Some("cap-echo")
        }

        fn mcp_domain_scope(&self) -> Option<&str> {
            Some("testing")
        }

        fn mcp_config_hash(&self) -> Option<&str> {
            Some(self.config_hash)
        }
    }

    #[test]
    fn same_named_tools_on_different_servers_have_distinct_authorization_identity() {
        let first = OperationAnalyzerRegistry::analyze_mcp(
            &McpFacts {
                server: "server-a",
                config_hash: "hash-a",
            },
            "same-input",
        );
        let second = OperationAnalyzerRegistry::analyze_mcp(
            &McpFacts {
                server: "server-b",
                config_hash: "hash-b",
            },
            "same-input",
        );
        assert_ne!(first.operation_hash, second.operation_hash);
        assert_ne!(first.action, second.action);
        assert_ne!(first.resources, second.resources);
        assert_eq!(first.resources[0].kind, "mcp");
    }

    #[test]
    fn server_reconfiguration_invalidates_mcp_authorization_identity() {
        let before = OperationAnalyzerRegistry::analyze_mcp(
            &McpFacts {
                server: "server-a",
                config_hash: "old-config",
            },
            "same-input",
        );
        let after = OperationAnalyzerRegistry::analyze_mcp(
            &McpFacts {
                server: "server-a",
                config_hash: "new-config",
            },
            "same-input",
        );
        assert_ne!(before.operation_hash, after.operation_hash);
        assert_ne!(before.action, after.action);
    }
}
