//! MCP 工具适配器 — 把 MCP 服务器暴露的工具包装为 zk-tools 的 [`Tool`]。
//!
//! 逐字对照 Java 基线 `mcp/McpToolAdapter.java`（336 行）：
//! - 工具名格式 `mcp__<serverName>__<toolName>`（前缀由 `McpClientManager`
//!   拼装后传入，本类型只承载 `name` 与 `originalToolName` 两个值）；
//! - 描述优先级：注册表 `enhancedDescription` → 服务端自报 `description` →
//!   `"MCP tool: <originalToolName>"`；
//! - 入参 Schema：缺失 → `{"type":"object"}`；
//! - 执行上限 5 分钟（`getMaxExecutionTimeMs() == 300_000`），与注册表配置的
//!   **单次 JSON-RPC 请求超时** `timeoutMs` 是两个独立值；
//! - 非 `CONNECTED` 时：`DEGRADED` / `PENDING` 先轮询等待 3s（每 200ms）；
//!   等待失败或其他状态直接走缓存降级；
//! - `tools/call` 前生成 `UUIDv4` `progressToken` 注册进度追踪、注册取消回调，
//!   `finally` 注销（无论成功/失败/超时）；
//! - 结果按 MCP `content[]` 仅取 `type == "text"` 项拼接；无 `content` 键时
//!   退化为整体 JSON 字面量（`null` → `"{}"`）；
//! - 结果超 1MB 截断并追加 `"\n[Truncated: exceeded 1048576 chars]"`；
//! - 成功且非实时性工具的结果写入 TTL 5min / 上限 200 条的**进程级共享**缓存；
//!   失败时命中缓存则返回 `"[cached] " + 缓存值` 并标记 `cached=true`；
//! - 实时性工具（名字含 `search`/`web`/`fetch`/`browse`/`realtime`/`live`）既不
//!   写缓存也不读缓存。
//!
//! 形态差异（Rust 侧必然）：
//! - Java `Tool` 接口的 `getGroup()` / `getPermissionRequirement()` /
//!   `isConcurrencySafe()` / `shouldDefer()` / `isMcp()` 五个覆写在 zkcode 的
//!   [`Tool`] trait 上并不存在——分组与权限档位由 zk-server `tool_catalog.rs`
//!   按工具名推导（`mcp__*` → 分组 `general`、权限 `NONE`），故本类型不实现；
//! - Caffeine `Cache` → [`ResultCache`]（`HashMap` + 惰性过期清理 + 满时淘汰
//!   最早写入项）。Caffeine 的 W-TinyLFU 按访问频率淘汰，此处按写入时间淘汰
//!   （200 条量级的降级缓存，淘汰策略差异无行为意义）；
//! - `AbortContext.onAbortDo(...)` → [`ToolContext::cancel`] 的旁路监听任务：
//!   取消触发时发 `notifications/cancelled`，**不**中断正在等待的请求
//!   （与 Java 一致——Java 也只是发通知，等服务端回错误或请求超时）。
//!   Java 因 `abortContextLookup` 是 `sessionId → AbortContext` 的查找而必须
//!   `sessionId != null` 才注册；Rust 的取消令牌恒在上下文内，故无此门槛。
//!
//! 行为偏离（在 `docs/compatibility.md` 留痕）：
//! 1. **错误码进正文**：Java `ToolResult.networkError(code, msg, NEVER, UNKNOWN)`
//!    有 failureType / retryability / effectState 三维分类；zkcode 冻结的
//!    [`ToolOutput`] 只有 `is_error` 一维，故沿用 zk-tools 既有约定把错误码写成
//!    正文首段 `"CODE: message"`（同 `zk-tools/src/input.rs` 的 `failure()`）。
//! 2. **缓存键含服务器名**：Java 键为 `originalToolName + ":" + args.hashCode()`，
//!    而缓存是 `static` 跨实例共享——两个 MCP 服务器提供同名工具时会互相读到
//!    对方的缓存结果（Java 侧缺陷）。此处键为
//!    `"<server>:<tool>:<入参规范化 JSON 的 hash>"`。
//! 3. **超时文案取生效值**：Java 用字段原值 `timeoutMs`，未配置（0）时会打印
//!    `"timed out after 0ms"`；此处构造时即经 [`timeout_or_default`] 归一，文案
//!    与实际生效的超时一致。
//! 4. **无 `SchemaCompressor` 体积压缩**：Java 的 `compressedInputSchema` /
//!    `getFullSchema()` 双份 schema 在此收敛为单份 schema，但对齐 zhikuncode
//!    `SchemaCompressor.normalizeTypeAliases` 在构造时做一次性 schema 清洗
//!    （`bool` → `boolean` 类型别名、非字符串 `description` 强制字符串化、
//!    `required` 剔除非字符串元素，见 [`normalize_schema_node`]）并缓存结果；
//!    examples 移除 / description 截断 / enum 裁剪等体积压缩仍不做（P2）。

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zk_tools::{McpToolIdentity, Tool, ToolContext, ToolOutput};

use crate::connection::{McpConnectionStatus, McpServerConnection};
use crate::error::{McpProtocolError, REQUEST_TIMEOUT};
use crate::protocol::ProgressTracker;
use crate::sse::lock;
use crate::transport::timeout_or_default;

/// MCP 工具名前缀（对照 Java `"mcp__" + serverName + "__" + toolName`）。
pub const MCP_TOOL_NAME_PREFIX: &str = "mcp__";

/// MCP 结果最大字节数（对照 Java `MAX_MCP_RESULT_SIZE = 1024 * 1024`）。
pub const MAX_MCP_RESULT_SIZE: usize = 1024 * 1024;

/// MCP 工具的执行上限（对照 Java `getMaxExecutionTimeMs() == 300_000L`）。
pub const MCP_TOOL_MAX_EXECUTION: Duration = Duration::from_mins(5);

/// 降级缓存 TTL（对照 Java `expireAfterWrite(5, TimeUnit.MINUTES)`）。
const RESULT_CACHE_TTL: Duration = Duration::from_mins(5);

/// 降级缓存条目上限（对照 Java `maximumSize(200)`）。
const RESULT_CACHE_MAX_ENTRIES: usize = 200;

/// 透明重连的最长等待（对照 Java `RECONNECT_WAIT_MS = 3000`）。
const RECONNECT_WAIT: Duration = Duration::from_secs(3);

/// 透明重连的轮询间隔（对照 Java `Thread.sleep(200)`）。
const RECONNECT_POLL: Duration = Duration::from_millis(200);

/// 连接不可用的错误码（对照 Java `"MCP_CONNECTION_UNAVAILABLE"`）。
const CODE_CONNECTION_UNAVAILABLE: &str = "MCP_CONNECTION_UNAVAILABLE";
/// 调用超时的错误码（对照 Java `"MCP_CALL_DEADLINE_EXCEEDED"`）。
const CODE_DEADLINE_EXCEEDED: &str = "MCP_CALL_DEADLINE_EXCEEDED";
/// 协议错误的错误码（对照 Java `"MCP_PROTOCOL_ERROR"`）。
const CODE_PROTOCOL_ERROR: &str = "MCP_PROTOCOL_ERROR";

/// 实时性工具的名字关键词（对照 Java `isRealtimeTool()` 的六个 `contains`）。
const REALTIME_KEYWORDS: [&str; 6] = ["search", "web", "fetch", "browse", "realtime", "live"];

/// 进程级共享的降级缓存（对照 Java `private static final Cache RESULT_CACHE`）。
static SHARED_RESULT_CACHE: LazyLock<Arc<ResultCache>> =
    LazyLock::new(|| Arc::new(ResultCache::new()));

/// 取进程级共享降级缓存（默认注入值；测试可用 [`ResultCache::new`] 造隔离实例）。
#[must_use]
pub fn shared_result_cache() -> Arc<ResultCache> {
    Arc::clone(&SHARED_RESULT_CACHE)
}

/// 单条缓存记录。
#[derive(Debug, Clone)]
struct CacheEntry {
    /// 缓存的结果正文。
    value: String,
    /// 写入时刻（TTL 与淘汰的判据）。
    written_at: Instant,
}

/// MCP 调用结果的降级缓存 — TTL + 条目上限（对照 Java Caffeine 缓存）。
///
/// 只在连接不可用 / 调用失败时被读取（降级模式）；连接正常时始终打远端。
#[derive(Debug)]
pub struct ResultCache {
    /// 键 → 记录（键由 [`McpToolAdapter`] 按「服务器:工具:入参 hash」构造）。
    entries: Mutex<HashMap<String, CacheEntry>>,
    /// 记录存活时长。
    ttl: Duration,
    /// 条目上限（满时淘汰最早写入项）。
    max_entries: usize,
}

impl ResultCache {
    /// 按 Java 参数（TTL 5min / 上限 200 条）构造。
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(RESULT_CACHE_TTL, RESULT_CACHE_MAX_ENTRIES)
    }

    /// 自定义 TTL 与条目上限（供测试构造小容量 / 短 TTL 实例）。
    #[must_use]
    pub fn with_limits(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
            max_entries,
        }
    }

    /// 读缓存（过期即视为未命中，并顺手清理）。
    #[must_use]
    pub fn get(&self, key: &str) -> Option<String> {
        let mut entries = lock(&self.entries);
        let now = Instant::now();
        match entries.get(key) {
            Some(entry) if now.duration_since(entry.written_at) < self.ttl => {
                Some(entry.value.clone())
            }
            Some(_) => {
                entries.remove(key);
                None
            }
            None => None,
        }
    }

    /// 写缓存（先清过期项，仍满则淘汰最早写入项）。
    pub fn put(&self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let mut entries = lock(&self.entries);
        let now = Instant::now();
        entries.retain(|_, entry| now.duration_since(entry.written_at) < self.ttl);
        if !entries.contains_key(&key) && entries.len() >= self.max_entries {
            let oldest = entries
                .iter()
                .min_by_key(|(_, entry)| entry.written_at)
                .map(|(oldest_key, _)| oldest_key.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            CacheEntry {
                value: value.into(),
                written_at: now,
            },
        );
    }

    /// 当前条目数（含尚未清理的过期项）。
    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.entries).len()
    }

    /// 是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 清空（测试隔离用）。
    pub fn clear(&self) {
        lock(&self.entries).clear();
    }
}

impl Default for ResultCache {
    fn default() -> Self {
        Self::new()
    }
}

/// 单个 MCP 工具的 [`Tool`] 适配器。
pub struct McpToolAdapter {
    /// 对外工具名（`mcp__<server>__<tool>`）。
    name: String,
    /// 生效描述（构造时按 enhanced → 服务端 → 兜底三级算定）。
    description: String,
    /// 入参 Schema（缺失时为 `{"type":"object"}`）。
    input_schema: Value,
    /// 所属连接。
    connection: Arc<McpServerConnection>,
    /// MCP 服务器上的原始工具名。
    original_tool_name: String,
    /// 服务端自报描述（`None` = 服务端未给）。
    server_description: Option<String>,
    /// 注册表覆盖描述（`None` / 空串 = 不覆盖）。
    enhanced_description: Option<String>,
    /// 单次 `tools/call` 的请求超时（构造时已归一，`0` → 传输默认 30s）。
    request_timeout: Duration,
    /// 进度追踪端口（`None` = 不注入 progressToken，对照 Java `tracker == null`）。
    progress_tracker: Option<Arc<dyn ProgressTracker>>,
    /// 降级缓存（默认进程级共享实例）。
    cache: Arc<ResultCache>,
    /// 授权链消费的 MCP 专属身份。
    identity: McpToolIdentity,
}

impl std::fmt::Debug for McpToolAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpToolAdapter")
            .field("name", &self.name)
            .field("original_tool_name", &self.original_tool_name)
            .field("server", &self.connection.name())
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl McpToolAdapter {
    /// 装配适配器（对照 Java 的基础构造函数）。
    ///
    /// `description` / `input_schema` 传 `None` 表示服务端未提供；注册表覆盖与
    /// 进度追踪分别用 [`Self::with_registry_overrides`] /
    /// [`Self::with_progress_tracker`] 追加。
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: Option<String>,
        input_schema: Option<Value>,
        connection: Arc<McpServerConnection>,
        original_tool_name: impl Into<String>,
    ) -> Self {
        let original_tool_name = original_tool_name.into();
        let server_description = description.filter(|text| !text.is_empty());
        let server_name = connection.name().to_owned();
        let config_hash = mcp_config_hash(connection.config());
        let mut adapter = Self {
            name: name.into(),
            description: String::new(),
            input_schema: normalize_schema(input_schema, &original_tool_name),
            connection,
            original_tool_name: original_tool_name.clone(),
            server_description,
            enhanced_description: None,
            request_timeout: timeout_or_default(Duration::ZERO),
            progress_tracker: None,
            cache: shared_result_cache(),
            identity: McpToolIdentity {
                server_id: server_name.clone(),
                server_name,
                tool_name: original_tool_name.clone(),
                capability_id: None,
                resource_scope: None,
                domain_scope: None,
                config_hash,
            },
        };
        adapter.description = adapter.resolve_description();
        adapter
    }

    /// 应用注册表覆盖（对照 Java 增强构造函数的 `enhancedDescription` /
    /// `timeoutMs`）：描述为空串视为不覆盖，`timeout_ms <= 0` 视为沿用默认。
    #[must_use]
    pub fn with_registry_overrides(
        mut self,
        enhanced_description: Option<String>,
        timeout_ms: i64,
    ) -> Self {
        self.enhanced_description = enhanced_description.filter(|text| !text.is_empty());
        self.request_timeout = timeout_or_default(Duration::from_millis(
            u64::try_from(timeout_ms).unwrap_or(0),
        ));
        self.description = self.resolve_description();
        self
    }

    /// 注入进度追踪端口（对照 Java 完整构造函数的 `progressTracker`）。
    #[must_use]
    pub fn with_progress_tracker(mut self, tracker: Arc<dyn ProgressTracker>) -> Self {
        self.progress_tracker = Some(tracker);
        self
    }

    /// 绑定能力注册表身份。domain 同时形成显式资源范围，授权不可跨 server/domain。
    #[must_use]
    pub fn with_capability_identity(
        mut self,
        capability_id: Option<String>,
        domain: Option<String>,
    ) -> Self {
        self.identity.capability_id = capability_id;
        self.identity.domain_scope.clone_from(&domain);
        self.identity.resource_scope =
            domain.map(|domain| format!("mcp://{}/{}", self.identity.server_id, domain));
        self
    }

    /// 注入独立降级缓存（默认为进程级共享实例；测试用于隔离）。
    #[must_use]
    pub fn with_result_cache(mut self, cache: Arc<ResultCache>) -> Self {
        self.cache = cache;
        self
    }

    /// MCP 服务器上的原始工具名（对照 Java `getOriginalToolName()`）。
    #[must_use]
    pub fn original_tool_name(&self) -> &str {
        &self.original_tool_name
    }

    /// 所属 MCP 服务器名（对照 Java `getServerName()`）。
    #[must_use]
    pub fn server_name(&self) -> &str {
        self.connection.name()
    }

    /// 单次 `tools/call` 的生效请求超时。
    #[must_use]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// 描述三级回落（对照 Java `getDescription()`）。
    fn resolve_description(&self) -> String {
        if let Some(enhanced) = self.enhanced_description.as_deref() {
            return enhanced.to_owned();
        }
        match self.server_description.as_deref() {
            Some(description) => description.to_owned(),
            None => format!("MCP tool: {}", self.original_tool_name),
        }
    }

    /// 缓存键（模块级偏离 2：含服务器名）。
    fn cache_key(&self, input: &Value) -> String {
        let mut hasher = DefaultHasher::new();
        input.to_string().hash(&mut hasher);
        format!(
            "{}:{}:{:016x}",
            self.connection.name(),
            self.original_tool_name,
            hasher.finish()
        )
    }

    /// 是否为实时性工具（对照 Java `isRealtimeTool()`）。
    fn is_realtime_tool(&self) -> bool {
        let lower = self.original_tool_name.to_lowercase();
        REALTIME_KEYWORDS
            .iter()
            .any(|keyword| lower.contains(keyword))
    }

    /// 轮询等待重连（对照 Java `waitForReconnect()`：3s 上限、每 200ms 一次）。
    ///
    /// 用 [`tokio::time::Instant`] 而非 [`std::time::Instant`] 计时，使虚拟时钟
    /// （`#[tokio::test(start_paused = true)]`）下的等待同样可推进。
    async fn wait_for_reconnect(&self) -> bool {
        let deadline = tokio::time::Instant::now() + RECONNECT_WAIT;
        while tokio::time::Instant::now() < deadline {
            if self.connection.status() == McpConnectionStatus::Connected {
                return true;
            }
            tokio::time::sleep(RECONNECT_POLL).await;
        }
        self.connection.status() == McpConnectionStatus::Connected
    }

    /// 降级：命中缓存则返回 `"[cached] …"`，否则返回带错误码的失败结果
    /// （对照 Java `fallbackToCacheOrError`）。
    fn fallback_to_cache_or_error(
        &self,
        cache_key: &str,
        error_code: &str,
        error_message: &str,
    ) -> ToolOutput {
        if !self.is_realtime_tool()
            && let Some(cached) = self.cache.get(cache_key)
        {
            tracing::info!(
                server = %self.connection.name(),
                tool = %self.original_tool_name,
                reason = error_message,
                "Returning cached result for MCP tool (connection issue)"
            );
            return ToolOutput {
                content: format!("[cached] {cached}"),
                is_error: false,
                metadata: Some(json!({
                    "mcpServer": self.connection.name(),
                    "mcpTool": self.original_tool_name,
                    "cached": "true",
                })),
            };
        }
        ToolOutput {
            content: format!("{error_code}: {error_message}"),
            is_error: true,
            metadata: Some(json!({
                "mcpServer": self.connection.name(),
                "mcpTool": self.original_tool_name,
                "errorCode": error_code,
            })),
        }
    }

    /// 连接可用性门禁（对照 Java `call()` 开头的状态分支）。
    ///
    /// 返回 `Some(_)` 表示已产出降级结果、不得发起 `tools/call`。
    async fn ensure_connected(&self, cache_key: &str) -> Option<ToolOutput> {
        let status = self.connection.status();
        if status == McpConnectionStatus::Connected {
            return None;
        }
        let waitable = matches!(
            status,
            McpConnectionStatus::Degraded | McpConnectionStatus::Pending
        );
        if waitable && self.wait_for_reconnect().await {
            tracing::info!(
                server = %self.connection.name(),
                "MCP server reconnected during wait, proceeding with call"
            );
            return None;
        }
        let message = if waitable {
            format!(
                "MCP server '{}' not connected after wait (status: {})",
                self.connection.name(),
                self.connection.status()
            )
        } else {
            format!(
                "MCP server '{}' is not connected (status: {status})",
                self.connection.name()
            )
        };
        Some(self.fallback_to_cache_or_error(cache_key, CODE_CONNECTION_UNAVAILABLE, &message))
    }

    /// 将 `tools/call` 结果映射为 [`ToolOutput`]（对照 Java 的 try / catch 三分支）。
    fn finish(
        &self,
        cache_key: String,
        outcome: Result<Option<Value>, McpProtocolError>,
    ) -> ToolOutput {
        match outcome {
            Ok(result) => {
                let content = truncate_result(extract_text_content(result.as_ref()));
                if !self.is_realtime_tool() && !content.is_empty() {
                    self.cache.put(cache_key, content.clone());
                }
                ToolOutput {
                    content,
                    is_error: false,
                    metadata: Some(json!({
                        "mcpServer": self.connection.name(),
                        "mcpTool": self.original_tool_name,
                    })),
                }
            }
            Err(error) if error.code() == REQUEST_TIMEOUT => {
                tracing::warn!(
                    server = %self.connection.name(),
                    tool = %self.original_tool_name,
                    timeout_ms = self.request_timeout.as_millis(),
                    "MCP tool call timed out"
                );
                self.fallback_to_cache_or_error(
                    &cache_key,
                    CODE_DEADLINE_EXCEEDED,
                    &format!(
                        "MCP tool call timed out after {}ms",
                        self.request_timeout.as_millis()
                    ),
                )
            }
            Err(error) => {
                tracing::error!(
                    server = %self.connection.name(),
                    tool = %self.original_tool_name,
                    error = %error,
                    "MCP tool call failed"
                );
                self.fallback_to_cache_or_error(
                    &cache_key,
                    CODE_PROTOCOL_ERROR,
                    &format!("MCP error: {}", error.message()),
                )
            }
        }
    }

    /// 单次调用主体（对照 Java `call(input, context)` 的方法体）。
    async fn invoke(&self, input: Value, ctx: ToolContext) -> ToolOutput {
        let cache_key = self.cache_key(&input);

        // 连接不可用：DEGRADED/PENDING 先等一小会儿重连，其余直接降级。
        if let Some(fallback) = self.ensure_connected(&cache_key).await {
            return fallback;
        }

        // progressToken 注册 + 取消旁路监听（对照 Java M4 段）。
        let progress_token = uuid::Uuid::new_v4().to_string();
        let tracked = if let (Some(tracker), Some(session_id)) =
            (self.progress_tracker.as_ref(), ctx.session_id())
        {
            tracker.register_progress(
                &progress_token,
                session_id,
                self.connection.name(),
                &self.original_tool_name,
            );
            true
        } else {
            false
        };
        let cancel_watcher = if ctx.cancel.is_cancelled() {
            // 对照 Java `AbortContext.register`：注册时已 aborted 则回调**立即同步
            // 执行**（早于 `tools/call` 发出）。
            self.connection
                .send_cancel_notification(&progress_token, Some("user_cancelled"))
                .await;
            None
        } else {
            spawn_cancel_watcher(
                Arc::clone(&self.connection),
                progress_token.clone(),
                ctx.cancel.clone(),
            )
        };

        let outcome = self
            .connection
            .call_tool(
                &self.original_tool_name,
                Some(input),
                self.request_timeout,
                Some(&progress_token),
            )
            .await;

        // finally：无论成败都注销进度并撤下取消监听。
        if let Some(watcher) = cancel_watcher {
            watcher.abort();
        }
        if tracked && let Some(tracker) = self.progress_tracker.as_ref() {
            tracker.unregister_progress(&progress_token);
        }

        self.finish(cache_key, outcome)
    }
}

impl Tool for McpToolAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters(&self) -> Value {
        self.input_schema.clone()
    }

    fn timeout(&self) -> Duration {
        MCP_TOOL_MAX_EXECUTION
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(self.invoke(input, ctx))
    }

    fn mcp_identity(&self) -> Option<&McpToolIdentity> {
        Some(&self.identity)
    }
}

/// 生成不含 headers/env 的稳定配置摘要，与宿主信任表的材料格式一致。
pub(crate) fn mcp_config_hash(config: &crate::config::McpServerConfig) -> String {
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
        .fold(String::with_capacity(64), |mut out, byte| {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// 取消旁路监听（对照 Java `abortCtx.onAbortDo(() -> sendCancelNotification(...))`）。
///
/// 仅用于「发起时尚未取消」的情形；无 tokio 运行时（同步上下文的单元测试）
/// 返回 `None`。发出通知后**不**中断正在等待的请求——与 Java 一致（等服务
/// 端回错误或请求自行超时）。
fn spawn_cancel_watcher(
    connection: Arc<McpServerConnection>,
    progress_token: String,
    cancel: tokio_util::sync::CancellationToken,
) -> Option<tokio::task::JoinHandle<()>> {
    if tokio::runtime::Handle::try_current().is_err() {
        return None;
    }
    Some(tokio::spawn(async move {
        cancel.cancelled().await;
        connection
            .send_cancel_notification(&progress_token, Some("user_cancelled"))
            .await;
    }))
}

/// 入参 Schema 归一（缺失 / `null` → `{"type":"object"}`，对照 Java
/// `compressedInputSchema != null ? … : Map.of("type", "object")`），并对
/// 服务端透传的 schema 做一次性递归清洗（对齐 zhikuncode
/// `SchemaCompressor.normalizeTypeAliases` 并扩展 description / required
/// 防御，见 [`normalize_schema_node`]）——结果缓存进
/// `McpToolAdapter::input_schema`，后续 `parameters()` 直接返回已清洗版本。
fn normalize_schema(schema: Option<Value>, tool_name: &str) -> Value {
    match schema {
        Some(Value::Null) | None => json!({ "type": "object" }),
        Some(mut schema) => {
            if normalize_schema_node(&mut schema) {
                tracing::debug!(
                    tool = %tool_name,
                    "normalized non-standard fields in MCP tool input schema"
                );
            }
            schema
        }
    }
}

/// Python 风格类型名 → JSON Schema 标准名；非别名（含标准名本身）返回
/// `None` 以保持幂等。
fn canonical_type_name(alias: &str) -> Option<&'static str> {
    match alias {
        "bool" => Some("boolean"),
        "int" | "long" => Some("integer"),
        "float" | "double" => Some("number"),
        "dict" | "map" => Some("object"),
        "list" => Some("array"),
        _ => None,
    }
}

/// 递归规范化 schema 节点（真实线上形态触发 Moonshot 严格校验 400）：
/// - `type` 别名替换（如智谱 MCP webSearchPro 的 `"type": "bool"`）：字符串
///   与数组内各元素均替换（数组本身不塌缩，对齐 zhikuncode）；
/// - `description` 强制字符串化（如 `DashScope` MCP 某工具 `address` 属性的
///   `description: null`，Moonshot 报 `description must be a string`）：
///   `null` → 移除；数字 / 布尔 → 字符串表示；对象 / 数组 → 紧凑 JSON
///   字符串（保留信息优于丢弃）；
/// - `required` 剔除非字符串元素（Moonshot 要求字符串数组）。
///
/// 递归面为 `properties` 各属性、`items`（对象或元组数组）、`anyOf` /
/// `oneOf` / `allOf` 各分支。返回是否发生了实际修改（合法节点不改动，
/// 幂等）。
fn normalize_schema_node(node: &mut Value) -> bool {
    let Some(obj) = node.as_object_mut() else {
        return false;
    };
    let mut modified = false;
    match obj.get_mut("type") {
        Some(Value::String(single)) => {
            if let Some(canonical) = canonical_type_name(single) {
                *single = canonical.to_owned();
                modified = true;
            }
        }
        Some(Value::Array(types)) => {
            for item in types {
                if let Value::String(name) = item
                    && let Some(canonical) = canonical_type_name(name)
                {
                    *name = canonical.to_owned();
                    modified = true;
                }
            }
        }
        _ => {}
    }
    match obj.get("description") {
        None | Some(Value::String(_)) => {}
        Some(Value::Null) => {
            obj.remove("description");
            modified = true;
        }
        Some(non_string) => {
            let text = match non_string {
                Value::Number(number) => number.to_string(),
                Value::Bool(flag) => flag.to_string(),
                composite => composite.to_string(),
            };
            obj.insert("description".to_owned(), Value::String(text));
            modified = true;
        }
    }
    if let Some(required) = obj.get_mut("required").and_then(Value::as_array_mut) {
        let before = required.len();
        required.retain(Value::is_string);
        modified |= required.len() != before;
    }
    if let Some(properties) = obj.get_mut("properties").and_then(Value::as_object_mut) {
        for property in properties.values_mut() {
            modified |= normalize_schema_node(property);
        }
    }
    if let Some(items) = obj.get_mut("items") {
        if let Value::Array(tuple_items) = items {
            for item in tuple_items {
                modified |= normalize_schema_node(item);
            }
        } else {
            modified |= normalize_schema_node(items);
        }
    }
    for union_key in ["anyOf", "oneOf", "allOf"] {
        if let Some(branches) = obj.get_mut(union_key).and_then(Value::as_array_mut) {
            for branch in branches {
                modified |= normalize_schema_node(branch);
            }
        }
    }
    modified
}

/// 提取 MCP 响应文本（对照 Java `extractContent`）：有 `content` 键则遍历并只取
/// `type == "text"` 项的 `text`；否则整体 JSON 字面量（`null` → `"{}"`）。
fn extract_text_content(result: Option<&Value>) -> String {
    let Some(result) = result else {
        return "{}".to_owned();
    };
    let Some(content) = result.get("content") else {
        return result.to_string();
    };
    // Jackson 的 `for (JsonNode item : node)` 对非数组节点不迭代（对象迭代其
    // 值）——MCP 规范下 `content` 恒为数组，非数组时按 Java 的「无可迭代项」
    // 语义返回空串。
    let Some(items) = content.as_array() else {
        return String::new();
    };
    let mut text = String::new();
    for item in items {
        if item.get("type").and_then(Value::as_str) == Some("text")
            && let Some(chunk) = item.get("text")
        {
            text.push_str(
                &chunk
                    .as_str()
                    .map_or_else(|| chunk.to_string(), std::borrow::ToOwned::to_owned),
            );
        }
    }
    text
}

/// 超 1MB 截断并追加标记（对照 Java `substring(0, MAX) + "\n[Truncated: …]"`）。
///
/// Java 按 UTF-16 `length()` 计数，Rust 按字节并落在字符边界上——多字节文本的
/// 截断位置会有差异，属编码模型差异而非行为差异。
fn truncate_result(content: String) -> String {
    if content.len() <= MAX_MCP_RESULT_SIZE {
        return content;
    }
    let mut boundary = MAX_MCP_RESULT_SIZE;
    while boundary > 0 && !content.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!(
        "{}\n[Truncated: exceeded {MAX_MCP_RESULT_SIZE} chars]",
        &content[..boundary]
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::config::McpServerConfig;
    use crate::error::JsonRpcError;
    use crate::jsonrpc::RequestId;
    use crate::transport::{DisconnectCallback, McpTransport, NotificationHandler};

    type Responder =
        Arc<dyn Fn(&str, Option<Value>) -> Result<Option<Value>, McpProtocolError> + Send + Sync>;

    /// 可编程传输替身 — 记录 `tools/call` 入参与出站通知。
    struct StubTransport {
        responder: Responder,
        connected: AtomicBool,
        delay: Duration,
        requests: Mutex<Vec<(String, Option<Value>)>>,
        notifications: Mutex<Vec<(String, Option<Value>)>>,
    }

    impl StubTransport {
        fn new(responder: Responder) -> Arc<Self> {
            Arc::new(Self {
                responder,
                connected: AtomicBool::new(true),
                delay: Duration::ZERO,
                requests: Mutex::new(Vec::new()),
                notifications: Mutex::new(Vec::new()),
            })
        }

        fn ok(result: Value) -> Arc<Self> {
            Self::new(Arc::new(move |_, _| Ok(Some(result.clone()))))
        }

        /// 应答前先 sleep —— 给取消旁路留出介入窗口。
        fn delayed(result: Value, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                responder: Arc::new(move |_, _| Ok(Some(result.clone()))),
                connected: AtomicBool::new(true),
                delay,
                requests: Mutex::new(Vec::new()),
                notifications: Mutex::new(Vec::new()),
            })
        }

        fn failing(error: McpProtocolError) -> Arc<Self> {
            Self::new(Arc::new(move |_, _| Err(error.clone())))
        }

        fn requests(&self) -> Vec<(String, Option<Value>)> {
            lock(&self.requests).clone()
        }

        fn notifications(&self) -> Vec<(String, Option<Value>)> {
            lock(&self.notifications).clone()
        }
    }

    impl McpTransport for StubTransport {
        fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
            Box::pin(async { Ok(()) })
        }

        fn send_request<'a>(
            &'a self,
            method: &'a str,
            params: Option<Value>,
            _timeout: Duration,
        ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
            lock(&self.requests).push((method.to_owned(), params.clone()));
            let outcome = (self.responder)(method, params);
            let delay = self.delay;
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                outcome
            })
        }

        fn send_notification<'a>(
            &'a self,
            method: &'a str,
            params: Option<Value>,
        ) -> BoxFuture<'a, ()> {
            lock(&self.notifications).push((method.to_owned(), params));
            Box::pin(async {})
        }

        fn send_response(&self, _id: RequestId, _result: Value) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::SeqCst)
        }

        fn set_notification_handler(&self, _handler: NotificationHandler) {}

        fn set_disconnect_callback(&self, _callback: DisconnectCallback) {}

        fn close(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    fn stdio_config(name: &str) -> McpServerConfig {
        McpServerConfig::stdio(name, "/bin/true", Vec::new())
    }

    fn connected(transport: Arc<dyn McpTransport>) -> Arc<McpServerConnection> {
        let connection = McpServerConnection::new(stdio_config("weather"));
        connection.set_transport_for_test(transport);
        connection.set_status(McpConnectionStatus::Connected);
        connection
    }

    fn adapter(connection: Arc<McpServerConnection>, tool: &str) -> McpToolAdapter {
        McpToolAdapter::new(
            format!("{MCP_TOOL_NAME_PREFIX}weather__{tool}"),
            Some("server side description".to_owned()),
            Some(json!({ "type": "object", "properties": { "city": { "type": "string" } } })),
            connection,
            tool,
        )
        .with_result_cache(Arc::new(ResultCache::new()))
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    #[test]
    fn name_description_and_schema_follow_java_fallbacks() {
        let connection = connected(StubTransport::ok(json!({})));
        let full = adapter(Arc::clone(&connection), "forecast");
        assert_eq!(full.name(), "mcp__weather__forecast");
        assert_eq!(full.description(), "server side description");
        assert_eq!(full.timeout(), Duration::from_mins(5));
        assert_eq!(full.original_tool_name(), "forecast");
        assert_eq!(full.server_name(), "weather");

        let bare = McpToolAdapter::new(
            "mcp__weather__forecast",
            None,
            None,
            Arc::clone(&connection),
            "forecast",
        );
        assert_eq!(bare.description(), "MCP tool: forecast");
        assert_eq!(bare.parameters(), json!({ "type": "object" }));

        let enhanced = adapter(connection, "forecast")
            .with_registry_overrides(Some("注册表中文描述".to_owned()), 7_000);
        assert_eq!(enhanced.description(), "注册表中文描述");
        assert_eq!(enhanced.request_timeout(), Duration::from_secs(7));

        // 空串覆盖视为不覆盖；timeout_ms <= 0 沿用传输默认 30s。
        let blank = McpToolAdapter::new(
            "mcp__weather__forecast",
            Some(String::new()),
            Some(Value::Null),
            McpServerConnection::new(stdio_config("weather")),
            "forecast",
        )
        .with_registry_overrides(Some(String::new()), -1);
        assert_eq!(blank.description(), "MCP tool: forecast");
        assert_eq!(blank.parameters(), json!({ "type": "object" }));
        assert_eq!(blank.request_timeout(), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn successful_call_extracts_text_content_and_injects_progress_token() {
        let transport = StubTransport::ok(json!({
            "content": [
                { "type": "text", "text": "sunny" },
                { "type": "image", "data": "ignored" },
                { "type": "text", "text": " 25C" },
            ]
        }));
        let connection = connected(Arc::clone(&transport) as Arc<dyn McpTransport>);
        let adapter = adapter(connection, "forecast");

        let output = adapter.execute(json!({ "city": "hz" }), ctx()).await;
        assert!(!output.is_error);
        assert_eq!(output.content, "sunny 25C");
        assert_eq!(
            output.metadata,
            Some(json!({ "mcpServer": "weather", "mcpTool": "forecast" }))
        );

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "tools/call");
        let params = requests[0].1.clone().expect("params");
        assert_eq!(params["name"], json!("forecast"));
        assert_eq!(params["arguments"], json!({ "city": "hz" }));
        assert!(
            params["_meta"]["progressToken"]
                .as_str()
                .is_some_and(|token| token.len() == 36),
            "progressToken should be a UUIDv4 string: {params}"
        );
    }

    #[tokio::test]
    async fn result_without_content_key_falls_back_to_raw_json() {
        let transport = StubTransport::ok(json!({ "answer": 42 }));
        let connection = connected(transport as Arc<dyn McpTransport>);
        let output = adapter(connection, "compute")
            .execute(json!({}), ctx())
            .await;
        assert_eq!(output.content, r#"{"answer":42}"#);

        let empty = StubTransport::new(Arc::new(|_, _| Ok(None)));
        let connection = connected(empty as Arc<dyn McpTransport>);
        let output = adapter(connection, "compute")
            .execute(json!({}), ctx())
            .await;
        assert_eq!(output.content, "{}");
    }

    #[tokio::test]
    async fn progress_tracker_is_registered_and_always_unregistered() {
        struct RecordingTracker {
            registered: Mutex<Vec<(String, String, String, String)>>,
            unregistered: Mutex<Vec<String>>,
        }
        impl ProgressTracker for RecordingTracker {
            fn register_progress(
                &self,
                token: &str,
                session_id: &str,
                server_name: &str,
                tool_name: &str,
            ) {
                lock(&self.registered).push((
                    token.to_owned(),
                    session_id.to_owned(),
                    server_name.to_owned(),
                    tool_name.to_owned(),
                ));
            }
            fn unregister_progress(&self, token: &str) {
                lock(&self.unregistered).push(token.to_owned());
            }
            fn handle_progress_notification(&self, _notification: &Value) {}
        }

        let tracker = Arc::new(RecordingTracker {
            registered: Mutex::new(Vec::new()),
            unregistered: Mutex::new(Vec::new()),
        });
        let connection = connected(
            StubTransport::failing(McpProtocolError::internal("boom")) as Arc<dyn McpTransport>
        );
        let adapter = adapter(connection, "forecast")
            .with_progress_tracker(Arc::clone(&tracker) as Arc<dyn ProgressTracker>);

        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = ToolContext::new(CancellationToken::new(), tx).with_session_id("sess-1");
        let output = adapter.execute(json!({}), ctx).await;
        assert!(output.is_error, "protocol error must surface as is_error");

        let registered = lock(&tracker.registered).clone();
        assert_eq!(registered.len(), 1);
        assert_eq!(registered[0].1, "sess-1");
        assert_eq!(registered[0].2, "weather");
        assert_eq!(registered[0].3, "forecast");
        // finally 语义：失败路径同样注销，且 token 与注册值一致。
        assert_eq!(
            lock(&tracker.unregistered).clone(),
            vec![registered[0].0.clone()]
        );
    }

    #[tokio::test]
    async fn missing_session_id_skips_progress_registration() {
        struct CountingTracker(AtomicUsize);
        impl ProgressTracker for CountingTracker {
            fn register_progress(&self, _t: &str, _s: &str, _sv: &str, _tn: &str) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn unregister_progress(&self, _token: &str) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn handle_progress_notification(&self, _notification: &Value) {}
        }
        let tracker = Arc::new(CountingTracker(AtomicUsize::new(0)));
        let connection = connected(StubTransport::ok(json!({})) as Arc<dyn McpTransport>);
        let adapter = adapter(connection, "forecast")
            .with_progress_tracker(Arc::clone(&tracker) as Arc<dyn ProgressTracker>);
        adapter.execute(json!({}), ctx()).await;
        assert_eq!(tracker.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn pre_cancelled_context_sends_cancelled_notification_synchronously() {
        let transport = StubTransport::ok(json!({}));
        let connection = connected(Arc::clone(&transport) as Arc<dyn McpTransport>);
        let adapter = adapter(connection, "forecast");
        let cancel = CancellationToken::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = ToolContext::new(cancel.clone(), tx);
        cancel.cancel();
        adapter.execute(json!({}), ctx).await;

        // 对照 Java `AbortContext.register`：已 aborted 时回调立即执行，故取消通知
        // 先于 `tools/call` 到达传输层。
        let notifications = transport.notifications();
        assert_eq!(notifications.len(), 1, "expected one cancel notification");
        assert_eq!(notifications[0].0, "notifications/cancelled");
        let params = notifications[0].1.clone().expect("params");
        assert_eq!(params["reason"], json!("user_cancelled"));
        let request_id = params["requestId"].as_str().expect("requestId");
        assert_eq!(request_id.len(), 36);
        // 取消 token 与本次调用的 progressToken 同一个值。
        let call = transport.requests();
        assert_eq!(
            call[0].1.clone().expect("params")["_meta"]["progressToken"],
            json!(request_id)
        );
    }

    #[tokio::test]
    async fn cancellation_during_call_sends_cancelled_notification() {
        let transport = StubTransport::delayed(json!({}), Duration::from_millis(300));
        let connection = connected(Arc::clone(&transport) as Arc<dyn McpTransport>);
        let adapter = adapter(connection, "forecast");
        let cancel = CancellationToken::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = ToolContext::new(cancel.clone(), tx);
        tokio::spawn({
            let cancel = cancel.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                cancel.cancel();
            }
        });
        // 取消只发通知、不中断等待（与 Java 一致）——调用仍等到服务端应答。
        let output = adapter.execute(json!({}), ctx).await;
        assert!(!output.is_error);

        let notifications = transport.notifications();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].0, "notifications/cancelled");
    }

    #[tokio::test]
    async fn not_connected_falls_back_without_wait() {
        let connection = McpServerConnection::new(stdio_config("weather"));
        connection.set_status(McpConnectionStatus::Failed);
        let output = adapter(connection, "compute")
            .execute(json!({}), ctx())
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "MCP_CONNECTION_UNAVAILABLE: MCP server 'weather' is not connected (status: FAILED)"
        );
        assert_eq!(
            output.metadata.expect("metadata")["errorCode"],
            json!("MCP_CONNECTION_UNAVAILABLE")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn degraded_status_waits_before_falling_back() {
        let connection = McpServerConnection::new(stdio_config("weather"));
        connection.set_status(McpConnectionStatus::Degraded);
        let started = tokio::time::Instant::now();
        let output = adapter(connection, "compute")
            .execute(json!({}), ctx())
            .await;
        assert!(started.elapsed() >= RECONNECT_WAIT, "must poll for 3s");
        assert!(output.is_error);
        assert!(
            output.content.starts_with(
                "MCP_CONNECTION_UNAVAILABLE: MCP server 'weather' not connected after wait"
            ),
            "unexpected: {}",
            output.content
        );
    }

    #[tokio::test]
    async fn timeout_error_maps_to_deadline_exceeded_code() {
        let error = McpProtocolError::from_rpc(JsonRpcError::new(REQUEST_TIMEOUT, "timeout"));
        let connection = connected(StubTransport::failing(error) as Arc<dyn McpTransport>);
        let output = adapter(connection, "compute")
            .with_registry_overrides(None, 1_500)
            .execute(json!({}), ctx())
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "MCP_CALL_DEADLINE_EXCEEDED: MCP tool call timed out after 1500ms"
        );
    }

    #[tokio::test]
    async fn cached_result_serves_next_failure_but_not_realtime_tools() {
        let cache = Arc::new(ResultCache::new());
        let ok = connected(StubTransport::ok(json!({
            "content": [{ "type": "text", "text": "cached body" }]
        })) as Arc<dyn McpTransport>);
        let warm = adapter(ok, "compute").with_result_cache(Arc::clone(&cache));
        let first = warm.execute(json!({ "q": 1 }), ctx()).await;
        assert_eq!(first.content, "cached body");
        assert_eq!(cache.len(), 1);

        // 同一入参 + 断连 → 命中缓存，标 [cached] 且不算错误。
        let down = McpServerConnection::new(stdio_config("weather"));
        down.set_status(McpConnectionStatus::Failed);
        let degraded = adapter(down, "compute").with_result_cache(Arc::clone(&cache));
        let second = degraded.execute(json!({ "q": 1 }), ctx()).await;
        assert!(!second.is_error);
        assert_eq!(second.content, "[cached] cached body");
        assert_eq!(second.metadata.expect("metadata")["cached"], json!("true"));

        // 入参不同 → 键不同 → 未命中。
        let down = McpServerConnection::new(stdio_config("weather"));
        down.set_status(McpConnectionStatus::Failed);
        let miss = adapter(down, "compute")
            .with_result_cache(Arc::clone(&cache))
            .execute(json!({ "q": 2 }), ctx())
            .await;
        assert!(miss.is_error);

        // 实时性工具既不写也不读缓存。
        let realtime_cache = Arc::new(ResultCache::new());
        let ok = connected(StubTransport::ok(json!({
            "content": [{ "type": "text", "text": "fresh" }]
        })) as Arc<dyn McpTransport>);
        let realtime = adapter(ok, "web_search").with_result_cache(Arc::clone(&realtime_cache));
        assert_eq!(realtime.execute(json!({}), ctx()).await.content, "fresh");
        assert_eq!(realtime_cache.len(), 0);
    }

    #[test]
    fn realtime_keywords_match_java_predicate() {
        let connection = McpServerConnection::new(stdio_config("weather"));
        for tool in [
            "WebSearch",
            "search_docs",
            "fetchPage",
            "browse_site",
            "realtime_quote",
            "live-feed",
        ] {
            assert!(
                adapter(Arc::clone(&connection), tool).is_realtime_tool(),
                "{tool} should be realtime"
            );
        }
        for tool in ["forecast", "compute", "read_file"] {
            assert!(!adapter(Arc::clone(&connection), tool).is_realtime_tool());
        }
    }

    #[test]
    fn cache_key_is_scoped_by_server_and_tool() {
        let one = McpServerConnection::new(stdio_config("alpha"));
        let two = McpServerConnection::new(stdio_config("beta"));
        let input = json!({ "a": 1 });
        let key_one = adapter(one, "same").cache_key(&input);
        let key_two = adapter(two, "same").cache_key(&input);
        assert_ne!(
            key_one, key_two,
            "same tool name on two servers must not share cache entries"
        );
        assert!(key_one.starts_with("alpha:same:"));
    }

    #[test]
    fn result_is_truncated_at_one_megabyte() {
        let content = "x".repeat(MAX_MCP_RESULT_SIZE + 10);
        let truncated = truncate_result(content);
        assert_eq!(
            truncated.len(),
            MAX_MCP_RESULT_SIZE + "\n[Truncated: exceeded 1048576 chars]".len()
        );
        assert!(truncated.ends_with("\n[Truncated: exceeded 1048576 chars]"));

        let exact = "y".repeat(MAX_MCP_RESULT_SIZE);
        assert_eq!(truncate_result(exact.clone()), exact);

        // 多字节文本在字符边界上截断（不 panic、不产生非法 UTF-8）。
        let wide = "宽".repeat(MAX_MCP_RESULT_SIZE);
        let truncated = truncate_result(wide);
        assert!(truncated.ends_with("\n[Truncated: exceeded 1048576 chars]"));
    }

    #[test]
    fn extract_text_content_mirrors_jackson_semantics() {
        assert_eq!(extract_text_content(None), "{}");
        assert_eq!(
            extract_text_content(Some(&json!({ "content": [] }))),
            String::new()
        );
        assert_eq!(
            extract_text_content(Some(&json!({ "content": { "type": "text" } }))),
            String::new()
        );
        assert_eq!(
            extract_text_content(Some(&json!({
                "content": [{ "type": "text", "text": 7 }]
            }))),
            "7"
        );
        assert_eq!(extract_text_content(Some(&json!(["a"]))), r#"["a"]"#);
    }

    #[test]
    fn result_cache_expires_and_evicts_oldest() {
        let cache = ResultCache::with_limits(Duration::from_millis(0), 8);
        cache.put("k", "v");
        assert_eq!(cache.get("k"), None, "zero TTL means never fresh");

        let cache = ResultCache::with_limits(Duration::from_mins(1), 2);
        cache.put("a", "1");
        std::thread::sleep(Duration::from_millis(2));
        cache.put("b", "2");
        std::thread::sleep(Duration::from_millis(2));
        cache.put("c", "3");
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get("a"), None, "oldest entry must be evicted");
        assert_eq!(cache.get("b").as_deref(), Some("2"));
        assert_eq!(cache.get("c").as_deref(), Some("3"));

        // 覆盖已有键不触发淘汰。
        cache.put("b", "22");
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get("b").as_deref(), Some("22"));

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn shared_cache_is_process_wide_singleton() {
        assert!(Arc::ptr_eq(&shared_result_cache(), &shared_result_cache()));
    }

    #[test]
    fn type_alias_bool_is_normalized_to_boolean() {
        let mut schema = json!({ "type": "bool" });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema, json!({ "type": "boolean" }));
    }

    #[test]
    fn type_alias_int_is_normalized_to_integer() {
        let mut schema = json!({ "type": "int" });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema, json!({ "type": "integer" }));
    }

    #[test]
    fn type_array_elements_are_replaced_without_collapsing() {
        let mut schema = json!({ "type": ["bool", "null"] });
        assert!(normalize_schema_node(&mut schema));
        // 对齐 zhikuncode：只替换元素，不塌缩数组本身。
        assert_eq!(schema, json!({ "type": ["boolean", "null"] }));
    }

    #[test]
    fn nested_nodes_are_normalized_recursively() {
        let mut schema = json!({
            "type": "dict",
            "properties": {
                "flag": { "type": "bool" },
                "nested": {
                    "type": "object",
                    "properties": { "count": { "type": "long" } }
                },
                "scores": { "type": "list", "items": { "type": "float" } },
                "tuple": { "type": "array", "items": [{ "type": "int" }, { "type": "double" }] },
                "choice": {
                    "anyOf": [{ "type": "bool" }, { "type": "string" }],
                    "oneOf": [{ "type": "map" }],
                    "allOf": [{ "type": "list" }]
                }
            }
        });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["flag"]["type"], "boolean");
        assert_eq!(
            schema["properties"]["nested"]["properties"]["count"]["type"],
            "integer"
        );
        assert_eq!(schema["properties"]["scores"]["type"], "array");
        assert_eq!(schema["properties"]["scores"]["items"]["type"], "number");
        assert_eq!(schema["properties"]["tuple"]["items"][0]["type"], "integer");
        assert_eq!(schema["properties"]["tuple"]["items"][1]["type"], "number");
        assert_eq!(
            schema["properties"]["choice"]["anyOf"][0]["type"],
            "boolean"
        );
        assert_eq!(schema["properties"]["choice"]["anyOf"][1]["type"], "string");
        assert_eq!(schema["properties"]["choice"]["oneOf"][0]["type"], "object");
        assert_eq!(schema["properties"]["choice"]["allOf"][0]["type"], "array");
    }

    #[test]
    fn zhipu_web_search_pro_prompt_extend_regression() {
        // 复现智谱 MCP webSearchPro 触发 Moonshot 400 的真实字段形态；
        // 经适配器构造后 parameters() 必须直接返回已清洗版本。
        let connection = McpServerConnection::new(stdio_config("zhipu-websearch"));
        let adapter = McpToolAdapter::new(
            "mcp__zhipu-websearch__webSearchPro",
            None,
            Some(json!({
                "type": "object",
                "properties": {
                    "prompt_extend": {
                        "type": "bool",
                        "description": "whether to extend the prompt"
                    }
                }
            })),
            connection,
            "webSearchPro",
        );
        let parameters = adapter.parameters();
        assert_eq!(parameters["properties"]["prompt_extend"]["type"], "boolean");
        assert_eq!(
            parameters["properties"]["prompt_extend"]["description"],
            "whether to extend the prompt"
        );
    }

    #[test]
    fn standard_schema_is_untouched_and_idempotent() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "city": { "type": "string" },
                "days": { "type": "integer" },
                "optional": { "type": ["boolean", "null"] },
                "tags": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["city"]
        });
        let original = schema.clone();
        assert!(!normalize_schema_node(&mut schema));
        assert_eq!(schema, original);

        // 清洗输出再过一遍零改动（幂等）。
        let mut aliased = json!({ "type": "bool", "description": null });
        assert!(normalize_schema_node(&mut aliased));
        let first_pass = aliased.clone();
        assert!(!normalize_schema_node(&mut aliased));
        assert_eq!(aliased, first_pass);
    }

    #[test]
    fn null_description_is_removed() {
        let mut schema = json!({ "type": "string", "description": null });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema, json!({ "type": "string" }));
    }

    #[test]
    fn numeric_and_boolean_descriptions_are_stringified() {
        let mut schema = json!({ "type": "string", "description": 123 });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema["description"], json!("123"));

        let mut schema = json!({ "type": "string", "description": true });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema["description"], json!("true"));
    }

    #[test]
    fn composite_descriptions_are_serialized_to_compact_json() {
        let mut schema = json!({ "type": "string", "description": { "foo": "bar" } });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema["description"], json!(r#"{"foo":"bar"}"#));

        let mut schema = json!({ "type": "string", "description": ["a", 1] });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema["description"], json!(r#"["a",1]"#));
    }

    #[test]
    fn nested_invalid_descriptions_are_normalized_recursively() {
        let mut schema = json!({
            "type": "object",
            "description": null,
            "properties": {
                "outer": {
                    "type": "object",
                    "properties": { "inner": { "type": "string", "description": 42 } }
                },
                "list": { "type": "array", "items": { "type": "string", "description": null } },
                "choice": { "anyOf": [{ "type": "string", "description": { "k": "v" } }] }
            }
        });
        assert!(normalize_schema_node(&mut schema));
        assert!(schema.get("description").is_none());
        assert_eq!(
            schema["properties"]["outer"]["properties"]["inner"]["description"],
            json!("42")
        );
        assert!(
            schema["properties"]["list"]["items"]
                .get("description")
                .is_none()
        );
        assert_eq!(
            schema["properties"]["choice"]["anyOf"][0]["description"],
            json!(r#"{"k":"v"}"#)
        );
    }

    #[test]
    fn required_drops_non_string_elements() {
        let mut schema = json!({
            "type": "object",
            "properties": { "city": { "type": "string" } },
            "required": ["city", null, 7, { "bad": true }]
        });
        assert!(normalize_schema_node(&mut schema));
        assert_eq!(schema["required"], json!(["city"]));

        // 合法字符串数组不算修改。
        let mut clean = json!({ "type": "object", "required": ["city"] });
        assert!(!normalize_schema_node(&mut clean));
    }

    #[test]
    fn dashscope_null_address_description_regression() {
        // 复现 DashScope MCP 触发 Moonshot 400（`At path
        // 'properties.address.description': description must be a string`）的
        // 真实字段形态；经适配器构造后 parameters() 必须不含非字符串
        // description。
        let connection = McpServerConnection::new(stdio_config("dashscope"));
        let adapter = McpToolAdapter::new(
            "mcp__dashscope__geocode",
            None,
            Some(json!({
                "type": "object",
                "properties": {
                    "address": { "type": "string", "description": null }
                }
            })),
            connection,
            "geocode",
        );
        let parameters = adapter.parameters();
        assert_eq!(
            parameters,
            json!({
                "type": "object",
                "properties": { "address": { "type": "string" } }
            })
        );
    }
}
