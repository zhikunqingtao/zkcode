//! zk-mcp 宿主端口的组装根实现（Batch 4B）。
//!
//! zk-mcp 走依赖倒置：凡需宿主能力处均以 trait 端口表达，本模块提供四个实现，
//! 由 [`crate::state::AppState`] 装配后注入 `McpClientManagerBuilder`。
//!
//! | 端口 | 实现 | 语义来源（Java，只读对照） |
//! |---|---|---|
//! | [`zk_mcp::ApprovalPort`] | [`TrustFileApproval`] | `mcp/McpApprovalService.java`（144 行） |
//! | [`zk_mcp::McpToolSink`] | [`RegistryToolSink`] | `tool/ToolRegistry` 的运行时 `registerTool` / 前缀摘除 |
//! | [`zk_mcp::McpHealthObserver`] | [`HubHealthObserver`] | `mcp/McpClientManager.broadcastHealthStatus` L738-764 |
//! | [`zk_mcp::ProgressTracker`] | [`HubProgressTracker`] | `mcp/progress/McpProgressTracker.java`（110 行） |
//!
//! # 同步端口 → 异步出口的桥
//!
//! 三个 trait 方法均为同步 `fn`（管理器在非 async 上下文亦调），而
//! [`crate::ws::WsHub::push`] 是 `async`。故广播经 `tokio::spawn` 移交运行时；
//! 无运行时（纯单测）时降级为 `debug!` 丢弃——不 panic，与 Java
//! `catch (Exception e) { log.debug(..) }` 的「推送失败不影响主流程」一致。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use zk_mcp::{
    ApprovalPort, McpClientManager, McpConnectionStatus, McpHealthObserver, McpServerConfig,
    McpToolSink, ProgressTracker,
};
use zk_protocol::ServerMessage;
use zk_tools::{Tool, ToolRegistry};

use crate::ws::WsHub;

/// 信任表文件名（旧 `TRUST_FILE = ~/.zhikun/mcp-trusted.json`；目录名随 #65
/// 的 `.zk` 统一，文件名逐字保留）。
pub const TRUST_FILE_NAME: &str = "mcp-trusted.json";

/// 信任表文件权限（旧 `PosixFilePermissions.fromString("rw-------")`）。
#[cfg(unix)]
const TRUST_FILE_MODE: u32 = 0o600;

/// 信任表落盘的单条记录（旧 `record TrustRecord(serverName, configHash,
/// approvedAt, scope)`；`approvedAt` 用 ISO-8601 毫秒串与 Jackson
/// `Instant` 的默认形制一致）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustRecord {
    /// MCP 服务器名（同时是表的键）。
    #[serde(rename = "serverName")]
    pub server_name: String,
    /// 批准时刻该配置的 SHA-256 指纹（配置改动即失信）。
    #[serde(rename = "configHash")]
    pub config_hash: String,
    /// 批准时刻。
    #[serde(rename = "approvedAt")]
    pub approved_at: String,
    /// 批准来源（`USER` / `PROJECT` / 自动信任的 scope 名）。
    pub scope: String,
}

/// 落盘信任表的批准端口（旧 `McpApprovalService`）。
#[derive(Debug)]
pub struct TrustFileApproval {
    trust_file: PathBuf,
}

impl TrustFileApproval {
    /// 按显式路径构造（集成测试指向 temp，避免污染真实 `$HOME`）。
    #[must_use]
    pub fn new(trust_file: impl Into<PathBuf>) -> Self {
        Self {
            trust_file: trust_file.into(),
        }
    }

    /// 按默认路径构造：`$HOME/.zk/mcp-trusted.json`。
    #[must_use]
    pub fn with_default_path() -> Self {
        Self::new(zk_core::paths::user_config_dir().join(TRUST_FILE_NAME))
    }

    /// 信任表文件路径。
    #[must_use]
    pub fn trust_file(&self) -> &Path {
        &self.trust_file
    }

    /// 读取信任表（旧 `loadTrustRecords`：文件缺失/解析失败一律 warn 后返回
    /// 空表——绝不因表损坏而误判为「已信任」）。
    fn load(&self) -> HashMap<String, TrustRecord> {
        if !self.trust_file.exists() {
            return HashMap::new();
        }
        match std::fs::read_to_string(&self.trust_file)
            .map_err(|error| error.to_string())
            .and_then(|text| serde_json::from_str(&text).map_err(|error| error.to_string()))
        {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(
                    path = %self.trust_file.display(),
                    %error,
                    "Failed to load MCP trust records"
                );
                HashMap::new()
            }
        }
    }

    /// 写回信任表（旧 `saveTrustRecords`：建父目录 + pretty JSON + 600 权限，
    /// 失败仅 warn）。
    fn save(&self, records: &HashMap<String, TrustRecord>) {
        if let Some(parent) = self.trust_file.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            tracing::warn!(%error, "Failed to save MCP trust records");
            return;
        }
        match serde_json::to_string_pretty(records)
            .map_err(|error| error.to_string())
            .and_then(|text| write_private(&self.trust_file, &text).map_err(|e| e.to_string()))
        {
            Ok(()) => {}
            Err(error) => tracing::warn!(%error, "Failed to save MCP trust records"),
        }
    }
}

impl ApprovalPort for TrustFileApproval {
    /// 旧 `isTrusted`：表里有该服务器**且**配置指纹未变才算已信任。
    fn is_trusted(&self, config: &McpServerConfig) -> bool {
        let hash = compute_config_hash(config);
        self.load()
            .get(&config.name)
            .is_some_and(|record| record.config_hash == hash)
    }

    /// 旧 `recordApproval`：写入指纹 + 时刻 + scope 后落盘并 info 留痕。
    fn record_approval(&self, config: &McpServerConfig, scope: &str) {
        let mut records = self.load();
        records.insert(
            config.name.clone(),
            TrustRecord {
                server_name: config.name.clone(),
                config_hash: compute_config_hash(config),
                approved_at: iso8601_now(),
                scope: scope.to_owned(),
            },
        );
        self.save(&records);
        tracing::info!(server = %config.name, %scope, "MCP trust recorded");
    }
}

/// 配置指纹（旧 `computeConfigHash`：`type|command|args(逗号连接)|url` 的
/// SHA-256 小写 hex；`null` 字段以空串参与，摘要异常时旧源返回 `""`——Rust 的
/// `Sha256` 无失败路径，故恒得指纹）。
#[must_use]
pub fn compute_config_hash(config: &McpServerConfig) -> String {
    let material = format!(
        "{}|{}|{}|{}",
        config.transport.as_str(),
        config.command.as_deref().unwrap_or_default(),
        config.args.join(","),
        config.url.as_deref().unwrap_or_default()
    );
    let digest = Sha256::digest(material.as_bytes());
    digest
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// 属主独占权限写入（形制同 [`crate::access_token`] 的 token 落盘）。
fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(TRUST_FILE_MODE);
    }
    let mut file = options.open(path)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    // `mode()` 只在新建时生效；已存在的文件补收敛一次（旧源 setPosixFilePermissions
    // 亦是无条件调用）。
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(TRUST_FILE_MODE))?;
    }
    Ok(())
}

/// 当前时刻的 ISO-8601 串（对齐 Jackson 序列化 `Instant.now()` 的
/// `write-dates-as-timestamps: false` 形制；微秒位数与本 crate 其他 REST 出口
/// 统一走 [`crate::iso::format_rfc3339_micros`]）。
fn iso8601_now() -> String {
    crate::iso::format_rfc3339_micros(crate::iso::now_millis())
}

/// 不落盘的批准端口（`Config::mcp_trust_file == None` 时使用，语义同
/// `access_token_path == None` 的“只存活于进程内”）。
///
/// 判定规则与 [`TrustFileApproval`] 逐字一致（名命中 + 指纹未变），只是存储
/// 介质为内存——集成测试因此不会写用户 `~/.zk/`。
#[derive(Debug, Default)]
pub struct InMemoryApproval {
    /// 服务器名 → 批准时的配置指纹。
    records: Mutex<HashMap<String, String>>,
}

impl InMemoryApproval {
    /// 构造空信任表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn table(&self) -> std::sync::MutexGuard<'_, HashMap<String, String>> {
        self.records.lock().unwrap_or_else(|poisoned| {
            self.records.clear_poison();
            poisoned.into_inner()
        })
    }
}

impl ApprovalPort for InMemoryApproval {
    fn is_trusted(&self, config: &McpServerConfig) -> bool {
        let hash = compute_config_hash(config);
        self.table()
            .get(&config.name)
            .is_some_and(|recorded| *recorded == hash)
    }

    fn record_approval(&self, config: &McpServerConfig, scope: &str) {
        self.table()
            .insert(config.name.clone(), compute_config_hash(config));
        tracing::info!(server = %config.name, %scope, "MCP trust recorded (in-memory)");
    }
}

/// 工具注册表 sink——把 MCP 发现的工具接进进程内唯一的 [`ToolRegistry`]。
///
/// 只持 `Arc<ToolRegistry>` 而非 `AppState`：注册表由 `AppState::tools()`
/// 惰性装配，若此处反持 `AppState` 就成环（state → mcp → state）。
pub struct RegistryToolSink {
    tools: Arc<ToolRegistry>,
    hub: WsHub,
}

impl RegistryToolSink {
    /// 绑定目标注册表。
    #[must_use]
    pub fn new(tools: Arc<ToolRegistry>, hub: WsHub) -> Self {
        Self { tools, hub }
    }
}

impl McpToolSink for RegistryToolSink {
    fn register_dynamic(&self, tool: Arc<dyn Tool>) {
        self.tools.register_dynamic(tool);
    }

    fn unregister_by_prefix(&self, prefix: &str) {
        self.tools.unregister_by_prefix(prefix);
        if let Some(server_id) = prefix
            .strip_prefix("mcp__")
            .and_then(|rest| rest.strip_suffix("__"))
        {
            broadcast_to_active_sessions(
                &self.hub,
                ServerMessage::McpToolUpdate {
                    server_id: server_id.to_owned(),
                    tools: Vec::new(),
                },
                "mcp tool removal",
            );
        }
    }
}

/// 健康状态观察者——把连接状态变更广播到全部活跃会话（旧
/// `broadcastHealthStatus` L738-764 的逐会话 `convertAndSendToUser`）。
///
/// 持 `Arc<OnceLock<Arc<McpClientManager>>>` 而非 `Arc<McpClientManager>`：
/// 观察者要在 builder 阶段注入，而彼时管理器尚未 `build()`。回填后即可读取
/// `consecutive_failures` / `last_successful_ping` 填满
/// [`ServerMessage::McpHealthStatus`] 的四字段（旧端 payload 只放了
/// `serverName` / `status` / `timestamp`，见交付报告偏离 B4B-01）。
pub struct HubHealthObserver {
    hub: WsHub,
    manager: Arc<OnceLock<Arc<McpClientManager>>>,
}

impl HubHealthObserver {
    /// 绑定下行出口与（待回填的）管理器格。
    #[must_use]
    pub fn new(hub: WsHub, manager: Arc<OnceLock<Arc<McpClientManager>>>) -> Self {
        Self { hub, manager }
    }
}

impl McpHealthObserver for HubHealthObserver {
    fn on_health_status(&self, server_name: &str, status: McpConnectionStatus) {
        let manager = self.manager.get();
        let consecutive_failures = manager.map_or(0, |manager| {
            i64::from(manager.consecutive_failures(server_name))
        });
        let last_successful_ping = manager
            .and_then(|manager| manager.last_successful_ping(server_name))
            .and_then(|instant| instant.duration_since(UNIX_EPOCH).ok())
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok());
        let message = ServerMessage::McpHealthStatus {
            server_name: server_name.to_owned(),
            status: status.as_str().to_owned(),
            consecutive_failures,
            last_successful_ping,
        };
        broadcast_to_active_sessions(&self.hub, message, "mcp health status");
    }
}

/// 进度追踪器——`notifications/progress` → `mcp_tool_progress` 定向推送
/// （旧 `McpProgressTracker`）。
pub struct HubProgressTracker {
    hub: WsHub,
    /// `progressToken` → 归属元数据（旧 `activeProgress` `ConcurrentHashMap`）。
    active: Mutex<HashMap<String, ProgressInfo>>,
}

/// 进度追踪元数据（旧 `record ProgressInfo(sessionId, serverName, toolName)`）。
#[derive(Debug, Clone)]
struct ProgressInfo {
    session_id: String,
    server_name: String,
    tool_name: String,
}

impl HubProgressTracker {
    /// 绑定下行出口。
    #[must_use]
    pub fn new(hub: WsHub) -> Self {
        Self {
            hub,
            active: Mutex::new(HashMap::new()),
        }
    }

    /// 活跃追踪数（旧 `activeCount()`，测试与监控用）。
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.table().len()
    }

    fn table(&self) -> std::sync::MutexGuard<'_, HashMap<String, ProgressInfo>> {
        self.active.lock().unwrap_or_else(|poisoned| {
            self.active.clear_poison();
            poisoned.into_inner()
        })
    }
}

impl ProgressTracker for HubProgressTracker {
    /// 旧 `registerProgress`：空 token 直接忽略。
    fn register_progress(&self, token: &str, session_id: &str, server_name: &str, tool_name: &str) {
        if token.is_empty() {
            return;
        }
        self.table().insert(
            token.to_owned(),
            ProgressInfo {
                session_id: session_id.to_owned(),
                server_name: server_name.to_owned(),
                tool_name: tool_name.to_owned(),
            },
        );
    }

    /// 旧 `unregisterProgress`：空 token 直接忽略。
    fn unregister_progress(&self, token: &str) {
        if token.is_empty() {
            return;
        }
        self.table().remove(token);
    }

    /// 旧 `handleProgressNotification`：未知 token 静默丢弃（迟到通知或服务器
    /// 误发都不得触发跨会话广播）；`progress` / `total` 缺省 0、`message`
    /// 缺省空串。
    fn handle_progress_notification(&self, notification: &Value) {
        let params = notification.get("params");
        let token = params
            .and_then(|params| params.get("progressToken"))
            .and_then(json_as_text)
            .unwrap_or_default();
        let Some(info) = self.table().get(&token).cloned() else {
            tracing::debug!(%token, "Ignored progress notification for unknown token");
            return;
        };
        let number = |key: &str| {
            params
                .and_then(|params| params.get(key))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        };
        let message = params
            .and_then(|params| params.get("message"))
            .and_then(json_as_text)
            .unwrap_or_default();
        let payload = ServerMessage::McpToolProgress {
            progress_token: token,
            server_name: info.server_name,
            tool_name: info.tool_name,
            progress: number("progress"),
            total: number("total"),
            message,
        };
        push_to_session(&self.hub, info.session_id, payload, "mcp tool progress");
    }
}

/// Jackson `asText("")` 的等价物：字符串取原值，其他标量取其 JSON 表示，
/// `null` / 容器 → `None`（落回缺省）。
fn json_as_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

/// 广播到全部活跃会话（旧 `getActiveSessionIds().forEach(..)`）。
pub(crate) fn broadcast_to_active_sessions(
    hub: &WsHub,
    message: ServerMessage,
    what: &'static str,
) {
    let sessions = hub.active_sessions();
    if sessions.is_empty() {
        return;
    }
    let hub = hub.clone();
    spawn_or_drop(what, async move {
        for session_id in sessions {
            hub.push(&session_id, message.clone()).await;
        }
    });
}

/// 定向推送到单会话（旧 `convertAndSendToUser(principal, ..)`）。
pub(crate) fn push_to_session(
    hub: &WsHub,
    session_id: String,
    message: ServerMessage,
    what: &'static str,
) {
    let hub = hub.clone();
    spawn_or_drop(what, async move {
        hub.push(&session_id, message).await;
    });
}

/// 把异步推送移交当前运行时；无运行时（纯单测）则 debug 丢弃（旧端推送异常
/// 亦仅 `log.debug`，不得让 MCP 健康检查/进度通知反噬主流程）。
fn spawn_or_drop<F>(what: &'static str, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(future);
    } else {
        tracing::debug!(what, "no tokio runtime: MCP push dropped");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use zk_mcp::{McpConfigScope, McpTransportType};

    use super::*;

    fn stdio_config(name: &str, command: &str) -> McpServerConfig {
        McpServerConfig {
            name: name.to_owned(),
            transport: McpTransportType::Stdio,
            command: Some(command.to_owned()),
            args: vec!["--port".to_owned(), "1".to_owned()],
            env: std::collections::BTreeMap::new(),
            url: None,
            headers: std::collections::BTreeMap::new(),
            scope: McpConfigScope::User,
        }
    }

    fn temp_trust_file(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "zk-mcp-trust-{tag}-{}",
            u64::from(std::process::id()) + tag.len() as u64
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir.join(TRUST_FILE_NAME)
    }

    /// 指纹取 `type|command|args|url` 四段：改任一段即换指纹，改无关段
    /// （env / headers / scope）则不变——旧 `computeConfigHash` 的取材范围。
    #[test]
    fn config_hash_covers_exactly_four_fields() {
        let base = stdio_config("weather", "npx");
        let hash = compute_config_hash(&base);
        assert_eq!(hash.len(), 64, "SHA-256 小写 hex");

        let mut same_scope = base.clone();
        same_scope.scope = McpConfigScope::Project;
        same_scope.env.insert("K".to_owned(), "V".to_owned());
        assert_eq!(compute_config_hash(&same_scope), hash, "env/scope 不入指纹");

        let mut other_command = base.clone();
        other_command.command = Some("uvx".to_owned());
        assert_ne!(compute_config_hash(&other_command), hash);

        let mut other_args = base.clone();
        other_args.args = vec!["--port".to_owned(), "2".to_owned()];
        assert_ne!(compute_config_hash(&other_args), hash);
    }

    /// 信任表往返：未记录 → 不信任；记录后 → 信任；配置改动 → 立即失信。
    #[test]
    fn trust_survives_round_trip_and_dies_on_config_change() {
        let approval = TrustFileApproval::new(temp_trust_file("round-trip"));
        let _ = std::fs::remove_file(approval.trust_file());
        let config = stdio_config("weather", "npx");

        assert!(!approval.is_trusted(&config), "空表不得误判为已信任");
        approval.record_approval(&config, "USER");
        assert!(approval.is_trusted(&config));

        let mut tampered = config.clone();
        tampered.command = Some("evil".to_owned());
        assert!(!approval.is_trusted(&tampered), "配置指纹变化即失信");

        // 复读一次以确认落盘形制可回读（而非只命中内存）。
        let reopened = TrustFileApproval::new(approval.trust_file());
        assert!(reopened.is_trusted(&config));
        let _ = std::fs::remove_file(approval.trust_file());
    }

    /// 损坏的信任表按空表处理（绝不因解析失败而放行）。
    #[test]
    fn corrupt_trust_file_is_treated_as_empty() {
        let path = temp_trust_file("corrupt");
        std::fs::write(&path, "{ not json").expect("write");
        let approval = TrustFileApproval::new(&path);
        assert!(!approval.is_trusted(&stdio_config("weather", "npx")));
        let _ = std::fs::remove_file(&path);
    }

    /// 不落盘的批准端口判定规则与落盘版逐字一致。
    #[test]
    fn in_memory_approval_mirrors_file_semantics() {
        let approval = InMemoryApproval::new();
        let config = stdio_config("weather", "npx");
        assert!(!approval.is_trusted(&config));
        approval.record_approval(&config, "USER");
        assert!(approval.is_trusted(&config));

        let mut tampered = config.clone();
        tampered.url = Some("http://evil".to_owned());
        assert!(!approval.is_trusted(&tampered));
    }

    /// sink 把工具接进真实注册表，前缀摘除亦生效。
    #[test]
    fn tool_sink_writes_through_to_registry() {
        let registry = Arc::new(ToolRegistry::new());
        let sink = RegistryToolSink::new(
            Arc::clone(&registry),
            WsHub::new(crate::ws::WsConfig::default()),
        );
        sink.register_dynamic(Arc::new(crate::mcp::tests::support::NoopTool));
        assert_eq!(registry.len(), 1);
        sink.unregister_by_prefix("mcp__weather__");
        assert!(registry.is_empty());
    }

    /// 未注册 token 的进度通知不得推送（否则脏 token 可跨会话投递）。
    #[test]
    fn unknown_progress_token_is_silently_dropped() {
        let tracker = HubProgressTracker::new(WsHub::new(crate::ws::WsConfig::default()));
        tracker.handle_progress_notification(&json!({
            "params": { "progressToken": "ghost", "progress": 1 }
        }));
        assert_eq!(tracker.active_count(), 0);
    }

    /// 注册/注销的空 token 守卫与计数（旧 `registerProgress` 首行 guard）。
    #[test]
    fn progress_registration_guards_empty_token() {
        let tracker = HubProgressTracker::new(WsHub::new(crate::ws::WsConfig::default()));
        tracker.register_progress("", "s1", "weather", "forecast");
        assert_eq!(tracker.active_count(), 0);
        tracker.register_progress("t1", "s1", "weather", "forecast");
        assert_eq!(tracker.active_count(), 1);
        tracker.unregister_progress("");
        assert_eq!(tracker.active_count(), 1);
        tracker.unregister_progress("t1");
        assert_eq!(tracker.active_count(), 0);
    }

    /// 信任表 `approvedAt` 走与其他 REST 出口同一时间形制（恒 6 位微秒 + `Z`）。
    #[test]
    fn approved_at_uses_shared_rfc3339_shape() {
        let approval = TrustFileApproval::new(temp_trust_file("stamp"));
        let _ = std::fs::remove_file(approval.trust_file());
        approval.record_approval(&stdio_config("weather", "npx"), "USER");
        let text = std::fs::read_to_string(approval.trust_file()).expect("trust file");
        let records: HashMap<String, TrustRecord> =
            serde_json::from_str(&text).expect("trust table parses");
        let stamp = &records["weather"].approved_at;
        assert!(stamp.ends_with('Z'), "{stamp}");
        assert_eq!(stamp.len(), "2026-08-19T00:00:00.000000Z".len(), "{stamp}");
        let _ = std::fs::remove_file(approval.trust_file());
    }

    pub(super) mod support {
        use futures::future::BoxFuture;
        use zk_tools::{Tool, ToolContext, ToolOutput};

        /// 名字带 MCP 前缀的桩工具（只为验证 sink 的写穿与前缀摘除）。
        pub struct NoopTool;

        #[allow(clippy::unnecessary_literal_bound)] // trait 签名固定为 &str，不能收窄成 &'static str
        impl Tool for NoopTool {
            fn name(&self) -> &str {
                "mcp__weather__forecast"
            }

            fn description(&self) -> &str {
                "stub"
            }

            fn parameters(&self) -> serde_json::Value {
                serde_json::json!({ "type": "object", "properties": {} })
            }

            fn execute(
                &self,
                _input: serde_json::Value,
                _ctx: ToolContext,
            ) -> BoxFuture<'_, ToolOutput> {
                Box::pin(futures::future::ready(ToolOutput::ok("stub")))
            }
        }
    }
}
