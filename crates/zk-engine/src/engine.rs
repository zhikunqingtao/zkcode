//! 引擎主体——run 生命周期（busy 防护 / 有限多轮工具循环 / interrupt / 落库时点）。
//!
//! 可观察行为对照旧 `WebSocketController` / `QueryEngine.java`（只读权威规格）：
//! - busy 拒绝（L629-644）：`AtomicBoolean` CAS 失败 → `error`
//!   code=`query_busy`、retryable=false、文案逐字一致；
//! - 多轮循环（`QueryEngine` 主循环）：user → LLM 流式 → 助手落库 →（含
//!   `tool_use` 块）→ 工具执行 → `tool_result` 回填 → 续轮，直至无工具块
//!   或达 [`MAX_TURNS`]（=1024，逐字对照旧 `QueryConfig.java` L48）；最终
//!   stopReason = `turnCount >= maxTurns ? "max_turns" : "end_turn"`
//!   （旧 L406-407；非 `tool_use` 终轮原值透传）；
//! - 工具推送时序（对照旧 `WsMessageHandler` / 直推点）：流中 id+name 齐备
//!   → `tool_use_start`（input 空对象）；流终态 flush 后逐块 →
//!   `tool_use_input`（完整入参，旧 onToolUseComplete L1103）；执行中
//!   stdout 增量 → `tool_use_progress`；完成 → `tool_result`；
//! - 工具落库形状（对照旧 `buildToolResultMessage` / `SessionManager`）：
//!   助手消息含 `tool_use` 块（id/name/input）；**每工具结果一条**
//!   role=user 消息、单 `tool_result` 块（`structuredResultMetadata` 语义：
//!   metadata 仅保留 `structuredResult` 键且值须为对象）；
//! - interrupt（旧 L1223-1244 + FIX-02 L1035-1076）：取消对应 run 的令牌；
//!   `interrupt_ack` 无论有无进行中 run 均推送；流中中断 → 部分助手
//!   **不落库**（旧 L826-841 修正版）；工具阶段中断 → 未完成工具合成
//!   [`INTERRUPTED_TOOL_RESULT`] **落库不推送**，`USER_INTERRUPT` 时追加
//!   用户可见通知消息 [`USER_INTERRUPT_NOTICE`]；终态 `message_complete`
//!   照常推送（committedMessages 含合成结果）；
//! - 成功序列（executeQueryInternal L876-1005）：deltas →
//!   `message_complete{usage(跨轮累计), stopReason, sessionId, runId,
//!   replaceAfterMessageId, committedMessages(锚点后全部持久化消息)}` →
//!   `session_list_updated`；
//! - 失败序列（handler.onError + finally 兜底）：`error`
//!   code=`query_error`、`retryable=true` → `message_complete{Usage::zero,
//!   "error"}`；错误码与可重试性均不分错误类型（旧 L690 catch 与
//!   `WsMessageHandler.onError` L1139 统一 `query_error` + `true`；会话不存在
//!   亦经 `requireSessionWorkingDirectory` 的 `IllegalStateException` 走同一
//!   归一路径）；
//! - 落库时点：用户消息在 provider 调用**前**入库（失败 run 用户消息保留）；
//!   助手/工具结果消息在各自终态后、`message_complete` 推送**前**入库。
//! - 图片模型路由：当前 Session 模型不支持图片时，从已配置 provider 中选择
//!   视觉模型，只覆盖本次 Run；附件数量按路由后能力校验，并在调用 provider
//!   前推送 `model_routed`，Session 模型保持不变。
//!
//! 取消令牌三层树：session → run → `tool_call`（第三层由
//! [`zk_tools::ToolExecutor`] 派生）；取消语义对齐 D-S6-5（清空积压、
//! Finish 不误发、busy 槽正确释放）。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_db::convert::record_to_ws_message;
use zk_db::model::{MessageRecord, MessageRole, NewMessage, StoredBlock};
use zk_db::run::{AGENT_TYPE_QUERY, EXIT_INTERNAL_ERROR, EXIT_USER_CANCELLED};
use zk_db::{AcceptanceCriterionRecord, Db, WorkbenchBindingRecord};
use zk_llm::{
    ChatMessage, ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent,
    SystemPrompt, ThinkingMode, ToolCallRequest, VisionProviderView,
};
use zk_protocol::model::Usage;
use zk_protocol::{Attachment, ClientMessage, Reference, ServerMessage, ToolResultContent};
use zk_tools::{CallEnv, ToolEvent, ToolExecutor, ToolOutput, ToolRegistry};

use crate::admission::{Admission, AdmissionRequest, ModeSwitcher, ToolAdmission};
use crate::agent::AgentMailboxMessage;
use crate::context::compact::Summarizer;
use crate::context::{
    AutoCompactTrackingState, ContextCascade, cascade_enabled, context_window_for,
};
use crate::cost::{CostTracker, NoopCostTracker};
use crate::file_history::FileHistoryService;
// Batch 8B：Hook 系统（事件驱动外部副作用通知）。触发点 append-only 嵌入热路径，
// 未装配（`hooks: None`）时全程空转。
use crate::hook::{HookContext, HookEvent, HookService, PreHookDecision};
use crate::observability::{NoopObservabilityRecorder, ObservabilityEvent, ObservabilityRecorder};
use crate::prompt::{DynamicSectionContext, ProjectPromptLoader};
use crate::query_config::{
    ESCALATED_MAX_TOKENS, MAX_OUTPUT_TOKENS_RECOVERY_LIMIT, MAX_TOKENS_RECOVERY_MESSAGE,
    recommended_max_tokens, usage_cost_usd,
};
use crate::recovery::{ContextRecovery, RecoveryOutcome, RecoveryState, is_context_limit_error};
use crate::summarizer::{LightModelSummarizer, ToolResultSummarizer};
use crate::system_prompt::build_system_prompt_segmented;
// Batch 7b：工具调用追踪器 + 终止策略 + 自修正循环。
use crate::correction::loop_ctrl::{detect_and_prepare_correction, should_abort};
use crate::termination::{LoopContext, TerminationDecision, evaluate};
use crate::tool_tracker::ToolCallTracker;
use zk_core::feature_flags::{FeatureFlags, SELF_CORRECTION_LOOP};

use crate::sink::MessageSink;

/// busy 拒绝文案（逐字对齐旧 `WebSocketController` L641）。
const QUERY_BUSY_MESSAGE: &str = "当前会话正在处理中，请等待上一个请求完成";

/// 多轮循环上界（逐字对照旧 `QueryConfig.java` L48 `maxTurns = 1024`）。
const MAX_TURNS: usize = 1024;

/// 中断原因：用户主动中断（旧 `AbortReason.USER_INTERRUPT`）。
const REASON_USER_INTERRUPT: &str = "USER_INTERRUPT";

/// 中断原因：提交新输入引发的中断（旧 `AbortReason.SUBMIT_INTERRUPT`）。
const REASON_SUBMIT_INTERRUPT: &str = "SUBMIT_INTERRUPT";

/// 中断时合成的未完成工具结果（逐字对照旧 FIX-02，L1035-1076）。
const INTERRUPTED_TOOL_RESULT: &str = "<tool_use_error>Interrupted by user</tool_use_error>";

/// `USER_INTERRUPT` 时追加的用户可见通知（逐字对照旧 FIX-02）。
const USER_INTERRUPT_NOTICE: &str = "[User interrupted the assistant's response]";

/// 加载会话历史时，为孤儿 `tool_use` 合成的兜底 `tool_result` 内容（逐字
/// 对照 Java `QueryEngine.java:1740-1742` 三层保护第三层的 synthetic error）。
/// 触发条件：进程崩溃 / DB 写工具结果失败 / 收尾未跑等边缘路径导致 DB 中
/// `assistant.tool_use` 无匹配 `user.tool_result` 后随——若不合成，回放到
/// provider 即触发 400（`must be followed by tool messages`）令会话永久损坏。
const ORPHAN_TOOL_RESULT: &str = "<tool_use_error>Tool execution did not complete: \
     executor contract violated or watchdog timeout</tool_use_error>";

/// 进行中 run 句柄（取消令牌 + 中断原因单次写入槽 + 取消时间戳）。
#[derive(Clone)]
struct RunHandle {
    cancel: CancellationToken,
    abort_reason: Arc<OnceLock<&'static str>>,
    /// 取消时间戳——`interrupt` 时写入，供周期清理判断滞留 run。
    cancelled_at: Arc<OnceLock<Instant>>,
}

/// 进行中 run 注册表（session → run 句柄；并发防护 + interrupt 寻址）。
type RunMap = Arc<Mutex<HashMap<String, RunHandle>>>;

/// Transport-scoped limits installed immediately before a query starts.
#[derive(Clone, Debug)]
pub struct ConversationRunOptions {
    /// Per-request turn ceiling.
    pub max_turns: usize,
    /// Replace the generated system prompt when present.
    pub system_prompt: Option<String>,
    /// Text appended after the selected base system prompt.
    pub append_system_prompt: Option<String>,
    /// Optional allowlist; `None` means all registered tools.
    pub allowed_tools: Option<HashSet<String>>,
    /// Explicit denylist, applied after the allowlist.
    pub disallowed_tools: HashSet<String>,
    /// Optional caller policy. `None` selects adaptive thinking only for capable models.
    pub thinking: Option<ThinkingMode>,
}

impl Default for ConversationRunOptions {
    fn default() -> Self {
        Self {
            max_turns: MAX_TURNS,
            system_prompt: None,
            append_system_prompt: None,
            allowed_tools: None,
            disallowed_tools: HashSet::new(),
            thinking: None,
        }
    }
}

impl ConversationRunOptions {
    fn allows(&self, tool_name: &str) -> bool {
        self.allowed_tools
            .as_ref()
            .is_none_or(|allowed| allowed.contains(tool_name))
            && !self.disallowed_tools.contains(tool_name)
    }
}

type ConversationOptionsMap = Arc<Mutex<HashMap<String, ConversationRunOptions>>>;

pub(crate) struct ConversationOptionsGuard {
    options: ConversationOptionsMap,
    session_id: String,
}

impl Drop for ConversationOptionsGuard {
    fn drop(&mut self) {
        lock_mutex(&self.options).remove(&self.session_id);
    }
}

/// 剪贴板图片 URL 信任校验端口（对照旧
/// `OssPublishProperties.isTrustedClipboardImageUrl`；组合根注入，未注入
/// 时 url 附件一律拒绝——fail-closed）。
pub type TrustedImageUrlCheck = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// 对话引擎（多轮工具循环；以 `Arc<Engine>` 共享给适配层与 run task）。
pub struct Engine {
    db: Db,
    provider: Arc<dyn ChatProvider>,
    sink: Arc<dyn MessageSink>,
    tools: Arc<ToolRegistry>,
    executor: ToolExecutor,
    runs: RunMap,
    conversation_options: ConversationOptionsMap,
    /// session 层取消令牌（三层树根；run 令牌由此派生 child）。会话数量
    /// 有限且令牌轻量，不主动回收（session 级取消归 2.5 权限管线）。
    sessions: Mutex<HashMap<String, CancellationToken>>,
    /// 工具执行准入端口（2.5 权限管线；未装配时为 `AllowAllAdmission`）。
    admission: Arc<dyn ToolAdmission>,
    /// Batch 7：权限模式切换端口（工具 metadata `mode` 字段触发）。
    mode_switcher: Option<Arc<dyn ModeSwitcher>>,
    /// Pre-API 六层级联压缩编排器（对照旧 `ContextCascade`；缺省无 LLM 摘要端口）。
    cascade: ContextCascade,
    /// 413 上下文超限三阶段恢复器（对照旧 `QueryEngine` 413 分支）。
    recovery: ContextRecovery,
    /// 3A.5 工具结果摘要器（对照旧 `ToolResultSummarizer`；缺省无 LLM 端口，
    /// 仅截断策略）。轮末对过大工具结果截断，节省下一轮上下文 token。
    summarizer: ToolResultSummarizer,
    /// Session workspace-aware six-layer project prompt loader. Its cache is keyed by
    /// the actual workspace passed to each request.
    project_prompts: ProjectPromptLoader,
    /// Batch 0 Step 0-6：会话级费用追踪窄端口（对照旧 `CostTrackerService`）。
    /// 缺省 [`NoopCostTracker`]（`with_admission` 装配点默认注入），组装根经
    /// [`Self::with_cost_tracker`] 注入生产实现（`zk-server` 侧 `AtomicCostTracker`，
    /// `AtomicU64` + `f64::to_bits` 无锁累加）。
    cost_tracker: Arc<dyn CostTracker>,
    /// Batch 5 Step 5：文件历史事务端口（对照旧 `QueryEngine` 注入的
    /// `FileHistoryService`）。`None` 时 begin/commit 全程空转，行为与接入前
    /// 逐字一致——组合根经 [`Self::with_file_history`] 注入生产实例。
    file_history: Option<Arc<FileHistoryService>>,
    /// Batch 8B：Hook 服务（事件驱动外部通知）。`None` = 未装配，全部触发点
    /// 空转（near-zero cost）；组合根经 [`Self::with_hooks`] 注入生产实例。
    /// hook 失败一律隔离（仅 `warn!`），绝不影响本引擎主流程。
    hooks: Option<Arc<HookService>>,
    /// Best-effort operations telemetry, separate from Run recovery events.
    observability: Arc<dyn ObservabilityRecorder>,
    /// 剪贴板图片 URL 信任校验（`None` = 未装配，url 附件一律拒绝）。
    trusted_image_url: Option<TrustedImageUrlCheck>,
    /// 当前已配置 provider/model 的只读视图。图片输入需要从此视图选择本次
    /// 请求可调用的视觉模型；未装配时保持 fail-closed，不猜测 provider。
    vision_providers: Option<Arc<dyn VisionProviderView>>,
}

/// run 前置装配结果（会话校验 + 用户消息落库 + 请求构建的产物）。
struct RunSetup {
    run_id: String,
    replace_after_message_id: Option<String>,
    user_record: MessageRecord,
    request: ChatRequest,
    /// 工具调用环境（2.3）：会话 ID + 会话工作目录，逐调用注入
    /// `ToolContext`（文件/Bash 工具的相对路径基准与写前快照归属键）。
    call_env: CallEnv,
    conversation_options: ConversationRunOptions,
}

#[derive(Default)]
struct UserContentInput {
    text: String,
    attachments: Vec<Attachment>,
    references: Vec<Reference>,
}

/// 流中累积的一次工具调用草稿（`ToolUseStart` 建档、`ToolInputDelta` 累积）。
struct ToolDraft {
    id: String,
    name: String,
    arguments: String,
}

/// flush 后的完整工具调用（arguments 已解析为 JSON；原始字符串保留用于
/// 续轮请求回填 `tool_calls.function.arguments`）。
struct FlushedCall {
    id: String,
    name: String,
    input: serde_json::Value,
    arguments: String,
}

/// 流式消费聚合终态。
#[derive(Default)]
struct StreamOutcome {
    text: String,
    thinking: String,
    finish: Option<FinishReason>,
    usage: Option<Usage>,
    last_error: Option<ProviderError>,
    tool_drafts: Vec<ToolDraft>,
    cancelled: bool,
}

/// 单轮执行结果 → 主循环流转指令。
enum TurnFlow {
    /// 工具已执行、请求已回填 → 续轮。
    Continue,
    /// 终态提交（携带最终 stopReason；`None` = 流耗尽无 finish 的宽容路径）。
    Stop(Option<String>),
    /// 已推送失败序列（error + 兜底 complete），run 直接结束；携带错误摘要，
    /// 由 [`Engine::execute_turns`] 写入 `run_envelopes.error_summary`
    /// （对照旧 `QueryEngine` L343 `runTracker.failRun(currentRunId, e.getMessage())`）。
    Failed(String),
    /// 413 上下文超限已恢复：`request.messages` 已替换为更小上下文，重试当前轮
    /// （对照旧 `QueryEngine` 413 恢复后 continue 主循环）。
    RecoverAndRetry,
}

/// 工具执行阶段结果。
enum ToolPhase {
    /// 全部结果就绪（按调用声明序的 tool 消息，供续轮回填）。
    Completed(Vec<ChatMessage>),
    /// 中断放弃（FIX-02 合成落库已完成）。
    Aborted,
}

/// 子代理单次运行配置（Batch 8C：Coordinator 真实引擎激活）。
///
/// 由 [`Engine::run_sub_agent`] 消费——描述一个**自包含**的子代理多轮
/// 工具循环所需的全部输入。与主循环（[`Engine::execute_turns`]）的关键
/// 差异仅在精简的回合入口；子 Session/Run、消息、检查点均持久化，并继承
/// 生产 admission、hook、费用、快照和摘要服务。
pub struct SubAgentRunConfig {
    /// 子代理会话 ID（必须已在 DB 中创建）。
    pub session_id: String,
    /// 子代理 Run ID（必须已在 DB 中创建并指向父 Run）。
    pub run_id: String,
    /// 模型 ID（决定输出预算 [`recommended_max_tokens`] 与上下文窗口）。
    pub model: String,
    /// 系统提示（由 `coordinator::prompt` / `agent::executor` 构建后传入）。
    pub system_prompt: String,
    /// 用户提示（子代理任务描述——多轮循环的首条 user 消息）。
    pub user_prompt: String,
    /// 工作目录（注入 [`CallEnv`]，作为文件 / Bash 工具的相对路径基准）。
    pub work_dir: String,
    /// 最大轮次（对照旧 `AgentDefinition.maxTurns`）。
    pub max_turns: u32,
    /// Process-local delivery channel backed by durable parent Run events.
    pub mailbox: mpsc::UnboundedReceiver<AgentMailboxMessage>,
}

/// 子代理运行终态——与 [`crate::agent::SubAgentEngineFactory::create_and_run`]
/// 的返回三元组 `(stop_reason, assistant_text, has_error)` 一一对应。
pub struct SubAgentRunOutcome {
    /// 终止原因（`end_turn` / `max_turns` / `cancelled` / `error` / provider 原值）。
    pub stop_reason: Option<String>,
    /// 最后一轮非空助手正文（无正文时为 `None`）。
    pub assistant_text: Option<String>,
    /// 是否因错误终止（provider 建立失败 / 流内致命错误 / 非法工具入参 JSON）。
    pub has_error: bool,
}

impl Engine {
    /// 装配引擎（无工具注册表——Phase 1 兼容入口，委托 [`Self::with_tools`]）。
    #[must_use]
    pub fn new(db: Db, provider: Arc<dyn ChatProvider>, sink: Arc<dyn MessageSink>) -> Self {
        Self::with_tools(db, provider, sink, Arc::new(ToolRegistry::new()))
    }

    /// 装配引擎（组装根注入 DB / provider / 下行 sink / 工具注册表）。
    #[must_use]
    pub fn with_tools(
        db: Db,
        provider: Arc<dyn ChatProvider>,
        sink: Arc<dyn MessageSink>,
        tools: Arc<ToolRegistry>,
    ) -> Self {
        Self::with_admission(db, provider, sink, tools, crate::admission::allow_all())
    }

    /// 装配引擎并注入工具执行准入端口（2.5 权限管线接入点）。
    ///
    /// `admission` 在工具阶段真正派发执行器之前逐个调用：`Allow` 以其
    /// `execution_input` 替换模型原始入参后执行（旧
    /// `AuthorizedOperation.executionInput()` 语义），`Deny` 则不执行工具并
    /// 回喂 `permissionDenied` 结果 + 推 `tool_permission_denied` 下行
    /// （对照旧 `ToolExecutionPipeline` L335-343）。
    #[must_use]
    pub fn with_admission(
        db: Db,
        provider: Arc<dyn ChatProvider>,
        sink: Arc<dyn MessageSink>,
        tools: Arc<ToolRegistry>,
        admission: Arc<dyn ToolAdmission>,
    ) -> Self {
        Self {
            db,
            provider,
            sink,
            tools,
            executor: ToolExecutor::new(),
            runs: Arc::new(Mutex::new(HashMap::new())),
            conversation_options: Arc::new(Mutex::new(HashMap::new())),
            sessions: Mutex::new(HashMap::new()),
            admission,
            mode_switcher: None,
            cascade: ContextCascade::new(),
            recovery: ContextRecovery::new(),
            summarizer: ToolResultSummarizer::new(),
            project_prompts: ProjectPromptLoader::new(),
            cost_tracker: Arc::new(NoopCostTracker),
            file_history: None,
            hooks: None,
            observability: Arc::new(NoopObservabilityRecorder),
            trusted_image_url: None,
            vision_providers: None,
        }
    }

    /// 注入费用追踪端口（Batch 0 Step 0-6 组装根装配点）。
    ///
    /// 未调用时保留缺省 [`NoopCostTracker`]，`cost_update` 下行的
    /// `session_cost` / `total_cost` 恒 0（行为与本 Step 接入前一致）。
    #[must_use]
    pub fn with_cost_tracker(mut self, cost_tracker: Arc<dyn CostTracker>) -> Self {
        self.cost_tracker = cost_tracker;
        self
    }

    /// Install the same lightweight LLM adapter across automatic compaction,
    /// context-limit recovery, and oversized tool-result summarization.
    #[must_use]
    pub fn with_summarizers(
        mut self,
        compact: Arc<dyn Summarizer>,
        tool_results: Arc<dyn LightModelSummarizer>,
    ) -> Self {
        self.cascade = ContextCascade::with_summarizer(Arc::clone(&compact));
        self.recovery = ContextRecovery::with_summarizer(compact);
        self.summarizer = ToolResultSummarizer::with_light_model(tool_results);
        self
    }

    /// 注入文件历史事务端口（Batch 5 Step 5 组装根装配点）。
    ///
    /// 未调用时事务边界空转：写前快照仍由 `zk-tools` 的 `SnapshotSink` 落库
    /// （与本端口无关），仅「按回合聚合变更集」的内存台账不建立。
    #[must_use]
    pub fn with_file_history(mut self, file_history: Arc<FileHistoryService>) -> Self {
        self.file_history = Some(file_history);
        self
    }

    /// 注入 Hook 服务（Batch 8B 组合根装配点）。
    ///
    /// 未调用时所有 hook 触发点空转（`hooks` 恒 `None`，行为与接入前逐字一致）。
    #[must_use]
    pub fn with_hooks(mut self, hooks: Arc<HookService>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// Attach the process-wide bounded operations recorder.
    #[must_use]
    pub fn with_observability(mut self, recorder: Arc<dyn ObservabilityRecorder>) -> Self {
        self.observability = recorder;
        self
    }

    /// 注入剪贴板图片 URL 信任校验端口（组合根装配点）。
    ///
    /// 未调用时 url 附件一律拒绝（fail-closed，对齐旧 WS 入站
    /// `isTrustedClipboardImageUrl` 强制校验——SSRF 红线）。
    #[must_use]
    pub fn with_trusted_image_url(mut self, check: TrustedImageUrlCheck) -> Self {
        self.trusted_image_url = Some(check);
        self
    }

    /// 注入视觉路由使用的已配置 provider/model 视图。
    ///
    /// 生产组装根传入与 [`ChatProvider`] 相同的热替换注册表代理，保证路由只会
    /// 选择当前配置可调用的模型；测试可传入真实 [`zk_llm::ProviderRegistry`]。
    #[must_use]
    pub fn with_vision_provider_view(mut self, providers: Arc<dyn VisionProviderView>) -> Self {
        self.vision_providers = Some(providers);
        self
    }

    /// 触发一个 hook 事件（外部通知；未装配 hook 服务时空转）。
    ///
    /// 错误隔离由 [`HookService::fire`] 内部保证（仅 `warn!`），本 helper 绝不
    /// 返回错误、绝不 panic——调用方主流程完全不受 hook 影响。
    async fn fire_hook(&self, event: HookEvent, context: HookContext) {
        if let Some(hooks) = &self.hooks {
            hooks.fire(event, &context).await;
        }
    }

    /// Batch 7：注入权限模式切换端口（工具 metadata `mode` 字段触发切换）。
    #[must_use]
    pub fn with_mode_switcher(mut self, switcher: Arc<dyn ModeSwitcher>) -> Self {
        self.mode_switcher = Some(switcher);
        self
    }

    /// 创建共享生产安全端口的子代理会话引擎。
    ///
    /// 子会话特征（与父引擎的差异）：
    /// - **共享** LLM provider（`Arc<dyn ChatProvider>` clone）与 DB 句柄
    ///   （[`Db`] 是 `Clone` 的轻量句柄）；
    /// - **独立** 工具注册表（`tools`——调用方注入子代理工具子集）、独立
    ///   `runs` / `sessions` 注册表与全新 [`ToolExecutor`]；
    /// - 共享生产 sink、授权准入、费用、Hook 与文件历史端口；
    /// - 独立运行/会话取消表与独立工具注册表；
    /// - 全新级联 / 恢复 / 摘要器。
    ///
    /// `db` / `provider` 由父级传入其自身克隆——避免子会话工厂持有父 [`Engine`]
    /// 引用而形成 `Engine → tools → SubAgentExecutor → factory → Engine` 循环。
    #[must_use]
    #[allow(clippy::too_many_arguments)] // composition root: each security service is explicit
    pub fn sub_session(
        db: Db,
        provider: Arc<dyn ChatProvider>,
        sink: Arc<dyn MessageSink>,
        tools: Arc<ToolRegistry>,
        admission: Arc<dyn ToolAdmission>,
        cost_tracker: Arc<dyn CostTracker>,
        file_history: Arc<FileHistoryService>,
        hooks: Arc<HookService>,
    ) -> Self {
        Self {
            db,
            provider,
            sink,
            tools,
            executor: ToolExecutor::new(),
            runs: Arc::new(Mutex::new(HashMap::new())),
            conversation_options: Arc::new(Mutex::new(HashMap::new())),
            sessions: Mutex::new(HashMap::new()),
            admission,
            mode_switcher: None,
            cascade: ContextCascade::new(),
            recovery: ContextRecovery::new(),
            summarizer: ToolResultSummarizer::new(),
            project_prompts: ProjectPromptLoader::new(),
            cost_tracker,
            file_history: Some(file_history),
            hooks: Some(hooks),
            observability: Arc::new(NoopObservabilityRecorder),
            trusted_image_url: None,
            vision_providers: None,
        }
    }

    /// 执行一个**自包含**的子代理多轮工具循环（Batch 8C Step 1）。
    ///
    /// 与 [`Self::execute_turns`] 复用生产 DB、授权、Hook、观测与下行端口，
    /// 并复用 [`Self::consume_stream`] 聚合流式终态与
    /// [`flush_tool_drafts`] / [`to_tool_call_requests`] / [`llm_tool_specs`]
    /// 等既有 helper——语义与主循环保持一致（多轮直至无工具块或达
    /// `max_turns`；`cancel` 触发即刻退出）。
    ///
    /// 取消传播：`cancel` 由 [`crate::coordinator`] 侧的 `CancellationToken`
    /// 派生，贯穿 provider 流（[`Self::consume_stream`] biased select）与工具
    /// 执行（[`Self::run_sub_agent_tools`] biased select + [`ToolExecutor`]
    /// 子令牌），任一阶段观察到取消即以 `stop_reason = "cancelled"` 返回。
    #[allow(clippy::too_many_lines)]
    pub async fn run_sub_agent(
        &self,
        config: SubAgentRunConfig,
        cancel: CancellationToken,
    ) -> SubAgentRunOutcome {
        let SubAgentRunConfig {
            session_id,
            run_id,
            model,
            system_prompt,
            user_prompt,
            work_dir,
            max_turns,
            mut mailbox,
        } = config;

        let mut call_env = CallEnv::new()
            .with_session_id(&session_id)
            .with_run_id(&run_id);
        if !work_dir.is_empty() {
            call_env = call_env.with_working_dir(&work_dir);
        }

        // 输出预算按模型能力表取值（与主循环 `prepare_run` 同源）。
        let max_tokens = recommended_max_tokens(&model);
        let thinking = if zk_llm::capabilities_for(&model).supports_thinking {
            ThinkingMode::Adaptive
        } else {
            ThinkingMode::Disabled
        };
        let mut request = ChatRequest::new(model)
            .with_tools(llm_tool_specs(&self.tools))
            .with_max_tokens(max_tokens)
            .with_thinking(thinking)
            .with_system_prompt(Some(system_prompt));
        request.messages = vec![ChatMessage::user(user_prompt)];

        let initial_message = NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: request.messages[0].content.clone(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        };
        if let Err(error) = self.db.append_message(&session_id, initial_message).await {
            tracing::error!(%session_id, %error, "failed to persist child user message");
            return SubAgentRunOutcome {
                stop_reason: Some("error".to_owned()),
                assistant_text: None,
                has_error: true,
            };
        }

        let mut assistant_text: Option<String> = None;
        let mut turn: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                return SubAgentRunOutcome {
                    stop_reason: Some("cancelled".to_owned()),
                    assistant_text,
                    has_error: false,
                };
            }
            if turn >= max_turns {
                return SubAgentRunOutcome {
                    stop_reason: Some("max_turns".to_owned()),
                    assistant_text,
                    has_error: false,
                };
            }
            self.drain_sub_agent_mailbox(&session_id, &mut mailbox, &mut request)
                .await;
            turn += 1;

            // 消息标准化（与主循环一致，防非法序列触发 provider 400）。
            crate::normalize::normalize(&mut request.messages);
            let llm_started = Instant::now();
            let mut llm_start = ObservabilityEvent::new("llm", "request", "started");
            llm_start.session_id = Some(session_id.clone());
            llm_start.run_id = Some(run_id.clone());
            self.observability.record(llm_start);
            let stream = match self.provider.chat_stream(request.clone(), cancel.clone()) {
                Ok(stream) => stream,
                Err(error) => {
                    let mut event = ObservabilityEvent::new("llm", "request", "error");
                    event.session_id = Some(session_id.clone());
                    event.run_id = Some(run_id.clone());
                    event.duration_ms =
                        Some(u64::try_from(llm_started.elapsed().as_millis()).unwrap_or(u64::MAX));
                    self.observability.record(event);
                    tracing::warn!(
                        %session_id,
                        error = %error,
                        "sub-agent provider stream establishment failed"
                    );
                    return SubAgentRunOutcome {
                        stop_reason: Some("error".to_owned()),
                        assistant_text,
                        has_error: true,
                    };
                }
            };
            let outcome = self.consume_stream(&session_id, stream, &cancel).await;
            let mut llm_end = ObservabilityEvent::new(
                "llm",
                "request",
                if outcome.last_error.is_some() {
                    "error"
                } else {
                    "completed"
                },
            );
            llm_end.session_id = Some(session_id.clone());
            llm_end.run_id = Some(run_id.clone());
            llm_end.duration_ms =
                Some(u64::try_from(llm_started.elapsed().as_millis()).unwrap_or(u64::MAX));
            self.observability.record(llm_end);
            if outcome.cancelled {
                return SubAgentRunOutcome {
                    stop_reason: Some("cancelled".to_owned()),
                    assistant_text,
                    has_error: false,
                };
            }
            // 无 finish 且有流内错误 → 失败终止（与主循环 `run_single_turn` 裁定一致；
            // 子代理不接入 413 恢复，直接透传失败）。
            if outcome.finish.is_none() && outcome.last_error.is_some() {
                return SubAgentRunOutcome {
                    stop_reason: Some("error".to_owned()),
                    assistant_text,
                    has_error: true,
                };
            }
            let stop_reason = outcome
                .finish
                .as_ref()
                .map(|reason| reason.as_str().to_owned());
            if !outcome.text.is_empty() {
                assistant_text = Some(outcome.text.clone());
            }
            let calls = match flush_tool_drafts(outcome.tool_drafts) {
                Ok(calls) => calls,
                Err(message) => {
                    tracing::warn!(%session_id, %message, "sub-agent invalid tool input json");
                    return SubAgentRunOutcome {
                        stop_reason: Some("error".to_owned()),
                        assistant_text,
                        has_error: true,
                    };
                }
            };
            let mut stored_blocks = Vec::with_capacity(calls.len() + 1);
            if !outcome.text.is_empty() {
                stored_blocks.push(StoredBlock::Text {
                    text: outcome.text.clone(),
                });
            }
            for call in &calls {
                stored_blocks.push(StoredBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: call.input.clone(),
                });
            }
            if let Err(error) = self
                .db
                .append_message(
                    &session_id,
                    NewMessage {
                        role: MessageRole::Assistant,
                        content: stored_blocks,
                        stop_reason: stop_reason.clone(),
                        input_tokens: outcome.usage.as_ref().map_or(0, |usage| usage.input_tokens),
                        output_tokens: outcome
                            .usage
                            .as_ref()
                            .map_or(0, |usage| usage.output_tokens),
                    },
                )
                .await
            {
                tracing::error!(%session_id, %error, "failed to persist child assistant message");
                return SubAgentRunOutcome {
                    stop_reason: Some("error".to_owned()),
                    assistant_text,
                    has_error: true,
                };
            }
            if calls.is_empty() {
                if self
                    .drain_sub_agent_mailbox(&session_id, &mut mailbox, &mut request)
                    .await
                    > 0
                {
                    continue;
                }
                return SubAgentRunOutcome {
                    stop_reason,
                    assistant_text,
                    has_error: false,
                };
            }
            // 续轮回填：assistant(tool_calls) + 每结果一条 tool 消息（与主循环同构）。
            request.messages.push(ChatMessage::assistant_tool_calls(
                outcome.text,
                to_tool_call_requests(&calls),
            ));
            match self
                .run_sub_agent_tools(&session_id, &calls, &call_env, &cancel)
                .await
            {
                Some(tool_messages) => request.messages.extend(tool_messages),
                None => {
                    return SubAgentRunOutcome {
                        stop_reason: Some("cancelled".to_owned()),
                        assistant_text,
                        has_error: false,
                    };
                }
            }
        }
    }

    async fn drain_sub_agent_mailbox(
        &self,
        session_id: &str,
        mailbox: &mut mpsc::UnboundedReceiver<AgentMailboxMessage>,
        request: &mut ChatRequest,
    ) -> usize {
        let mut consumed = 0;
        while let Ok(message) = mailbox.try_recv() {
            request
                .messages
                .push(ChatMessage::user(message.content.clone()));
            let persisted = NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: message.content.clone(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            };
            if let Err(error) = self.db.append_message(session_id, persisted).await {
                tracing::error!(session_id, %error, "failed to persist consumed agent message");
            }
            if let Err(error) = self
                .db
                .append_run_event(
                    &message.parent_run_id,
                    "teammate_message_consumed",
                    None,
                    &json!({
                        "messageId": message.message_id,
                        "targetSessionId": session_id,
                        "fromId": message.from_id,
                    }),
                )
                .await
            {
                tracing::error!(session_id, %error, "failed to persist consumed mailbox event");
            }
            consumed += 1;
        }
        consumed
    }

    /// 子代理工具阶段（精简编排，对照 [`Self::run_tool_phase`]）。
    ///
    /// 复用生产 `ToolExecutor`、admission 与 hook，并按声明序汇齐结果供续轮
    /// 回填。未知工具合成错误结果回喂模型（复用 [`unknown_tool_message`]）。
    ///
    /// 返回 `None` 表示中断（`cancel` 触发或存在缺失结果的取消竞态）——调用
    /// 方据此以 `cancelled` 终止。
    #[allow(clippy::too_many_lines)] // one ordered Hook → Admission → execution transaction
    async fn run_sub_agent_tools(
        &self,
        session_id: &str,
        calls: &[FlushedCall],
        env: &CallEnv,
        cancel: &CancellationToken,
    ) -> Option<Vec<ChatMessage>> {
        let mut results: HashMap<String, ToolOutput> = HashMap::new();
        let mut streams = Vec::with_capacity(calls.len());
        for call in calls {
            if let Some(tool) = self.tools.get(&call.name) {
                let mut context = HookContext::new()
                    .with_tool(call.name.clone())
                    .with_session(session_id);
                if let Some(working_dir) = env.working_dir_str() {
                    context = context.with_working_dir(working_dir);
                }
                let pre_input = if let Some(hooks) = &self.hooks {
                    match hooks.evaluate_pre_tool(&context, &call.input).await {
                        PreHookDecision::Continue { input } => input,
                        PreHookDecision::Deny { code, message } => {
                            results.insert(
                                call.id.clone(),
                                ToolOutput::error(format!("{code}: {message}")),
                            );
                            continue;
                        }
                    }
                } else {
                    call.input.clone()
                };
                let (Some(run_id), Some(root_session_id)) =
                    (env.run_id_str(), env.session_id_str())
                else {
                    results.insert(
                        call.id.clone(),
                        ToolOutput::error("CHILD_ADMISSION_CONTEXT_INCOMPLETE"),
                    );
                    continue;
                };
                let execution_input = match self
                    .admission
                    .admit(AdmissionRequest {
                        session_id: root_session_id,
                        run_id,
                        tool_use_id: &call.id,
                        tool_name: &call.name,
                        input: &pre_input,
                        working_directory: env.working_dir_str(),
                    })
                    .await
                {
                    Admission::Allow { execution_input } => execution_input,
                    Admission::Denied { code, message } | Admission::Failed { code, message } => {
                        results.insert(
                            call.id.clone(),
                            ToolOutput::error(format!("{code}: {message}")),
                        );
                        continue;
                    }
                };
                let rx = self.executor.spawn_call_in(
                    tool,
                    call.id.clone(),
                    execution_input,
                    cancel,
                    env.clone(),
                );
                streams.push(tool_event_stream(rx));
            } else {
                results.insert(
                    call.id.clone(),
                    ToolOutput::error(unknown_tool_message(&call.name, &self.tools.names())),
                );
            }
        }
        let mut merged = futures::stream::select_all(streams);
        loop {
            let event = tokio::select! {
                biased;
                () = cancel.cancelled() => return None,
                event = merged.next() => event,
            };
            let Some(event) = event else { break };
            match event {
                // 子代理无前端时间线：progress 增量丢弃。
                ToolEvent::Progress { .. } => {}
                ToolEvent::Finished {
                    tool_use_id,
                    output,
                } => {
                    if let Some(hooks) = &self.hooks {
                        let tool_name = calls
                            .iter()
                            .find(|call| call.id == tool_use_id)
                            .map_or("unknown", |call| call.name.as_str());
                        let mut context = HookContext::new()
                            .with_tool(tool_name)
                            .with_session(session_id)
                            .with_result_preview(output.content.clone());
                        if let Some(working_dir) = env.working_dir_str() {
                            context = context.with_working_dir(working_dir);
                        }
                        hooks.fire(HookEvent::PostToolExecution, &context).await;
                    }
                    results.insert(tool_use_id, output);
                }
            }
        }
        // 按声明序组装续轮 tool 消息；缺失结果 = 取消竞态（执行器取消不产 Finished）。
        let mut messages = Vec::with_capacity(calls.len());
        for call in calls {
            let output = results.get(&call.id)?;
            if let Err(error) = self
                .db
                .append_message(
                    session_id,
                    NewMessage {
                        role: MessageRole::User,
                        content: vec![StoredBlock::ToolResult {
                            tool_use_id: call.id.clone(),
                            content: output.content.clone(),
                            is_error: output.is_error,
                            metadata: output.metadata.clone(),
                        }],
                        stop_reason: None,
                        input_tokens: 0,
                        output_tokens: 0,
                    },
                )
                .await
            {
                tracing::error!(%session_id, tool_use_id = %call.id, %error, "failed to persist child tool result");
                return None;
            }
            messages.push(ChatMessage::tool(call.id.clone(), output.content.clone()));
        }
        Some(messages)
    }

    /// 提交当前会话的文件历史事务（未装配端口时空转）。
    ///
    /// 对照旧 `QueryEngine` L1183-1186：位于工具阶段之后、继续/终止判定
    /// 之前，**每轮**执行一次（工具集为空的终轮同样提交——旧实现的
    /// `commitTransaction` 不在 `toolUseBlocks` 分支内）。工具阶段因中断
    /// 放弃时不提交（旧实现该路径抛出/跳出，未到提交点），残留的活跃事务
    /// 由下一轮 `begin_transaction` 覆盖。
    fn commit_file_history(&self, session_id: &str) {
        if let Some(history) = self.file_history.as_ref() {
            history.commit_transaction(session_id);
        }
    }

    /// 上行分发入口（zk-server `EngineHook` 适配层桥接调用，同步不阻塞）。
    ///
    /// `user_message` → 每 run 一 `tokio::spawn`；`interrupt` → 取消对应
    /// run 并推送 `interrupt_ack`；其余 14 类上行 Phase 2.3+ 接管，当前仅
    /// debug 记录。
    pub fn handle_client_message(self: &Arc<Self>, session_id: &str, message: ClientMessage) {
        match message {
            ClientMessage::UserMessage {
                text,
                attachments,
                references,
            } => {
                drop(tokio::spawn(Arc::clone(self).run_user_content(
                    session_id.to_owned(),
                    UserContentInput {
                        text,
                        attachments: attachments.unwrap_or_default(),
                        references: references.unwrap_or_default(),
                    },
                )));
            }
            ClientMessage::Interrupt {
                is_submit_interrupt,
            } => {
                let reason = if is_submit_interrupt == Some(true) {
                    REASON_SUBMIT_INTERRUPT
                } else {
                    REASON_USER_INTERRUPT
                };
                self.interrupt(session_id, reason);
            }
            other => {
                tracing::debug!(
                    session_id,
                    kind = other.kind(),
                    "engine ignores non-chat upstream (Phase 2.3+)"
                );
            }
        }
    }

    /// 中断会话进行中的 run（取消令牌 + `interrupt_ack` 推送）。
    ///
    /// 对照旧 handleInterrupt（L1223-1244）：`interrupt_ack{reason}`
    /// **无论有无进行中 run 均推送**（幂等确认）；取消语义对齐 D-S6-5
    /// （run 流静默终止，终态 `message_complete` 由 run 自身的 abort
    /// 路径推送）。
    pub fn interrupt(self: &Arc<Self>, session_id: &str, reason: &'static str) {
        let handle = lock_runs(&self.runs).get(session_id).cloned();
        if let Some(handle) = handle {
            // 原因先于取消写入（run 侧观察到 cancelled 时 reason 已就绪）。
            let _ = handle.abort_reason.set(reason);
            let _ = handle.cancelled_at.set(Instant::now());
            handle.cancel.cancel();
            tracing::info!(session_id, reason, "run interrupted");
        }
        let engine = Arc::clone(self);
        let session = session_id.to_owned();
        tokio::spawn(async move {
            engine
                .sink
                .push(
                    &session,
                    ServerMessage::InterruptAck {
                        reason: reason.to_owned(),
                    },
                )
                .await;
        });
    }

    /// 周期清理已取消但未移除的 run 注册表条目（滞留 run 回收）。
    ///
    /// 清除 `cancelled_at` 已设置且超过 `max_age` 的条目。正常 run 由
    /// `RunGuard::drop` 移除；此方法只兜底因 task 卡住等异常路径滞留的条目。
    pub fn cleanup_expired_runs(&self, max_age: Duration) {
        let mut runs = lock_runs(&self.runs);
        let now = Instant::now();
        let before = runs.len();
        runs.retain(|_, handle| match handle.cancelled_at.get() {
            Some(cancelled) => now.duration_since(*cancelled) < max_age,
            None => true,
        });
        let removed = before - runs.len();
        if removed > 0 {
            tracing::warn!(
                removed,
                remaining = runs.len(),
                "cleaned up stale run entries"
            );
        }
    }

    /// 启动 run 注册表周期清理任务（30min interval，随进程生命周期）。
    ///
    /// 返回可 abort 的任务句柄（关停时终止）。
    #[must_use]
    pub fn spawn_run_cleanup(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let interval = Duration::from_mins(30);
            loop {
                tokio::time::sleep(interval).await;
                engine.cleanup_expired_runs(interval);
            }
        })
    }

    /// 派生一次 run（返回句柄供测试/关停路径 join）。
    #[must_use]
    pub fn spawn_user_message(
        self: &Arc<Self>,
        session_id: &str,
        text: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(Arc::clone(self).run_user_message(session_id.to_owned(), text))
    }

    /// Install request-scoped query limits. The returned guard removes the
    /// options even when the caller future is cancelled by a timeout.
    pub(crate) fn install_conversation_options(
        &self,
        session_id: &str,
        options: ConversationRunOptions,
    ) -> ConversationOptionsGuard {
        lock_mutex(&self.conversation_options).insert(session_id.to_owned(), options);
        ConversationOptionsGuard {
            options: Arc::clone(&self.conversation_options),
            session_id: session_id.to_owned(),
        }
    }

    /// run 全生命周期（busy 防护 → 多轮执行 → 槽位清除）。
    ///
    /// busy 语义对齐旧 handleUserMessage：同会话已有进行中 run 时立即回
    /// `query_busy`（retryable=false），不排队不抢占。
    pub async fn run_user_message(self: Arc<Self>, session_id: String, text: String) {
        self.run_user_content(
            session_id,
            UserContentInput {
                text,
                ..UserContentInput::default()
            },
        )
        .await;
    }

    async fn run_user_content(self: Arc<Self>, session_id: String, input: UserContentInput) {
        let Some((guard, run)) = self.try_begin_run(&session_id) else {
            self.push_error(
                &session_id,
                "query_busy",
                QUERY_BUSY_MESSAGE.to_owned(),
                false,
            )
            .await;
            return;
        };
        self.execute_turns(&session_id, input, &run).await;
        // 槽位守卫显式活到 run 终点（含内部提前 return 的全部路径）。
        drop(guard);
    }

    /// 尝试占用会话 run 槽位；已占用返回 `None`（busy）。
    ///
    /// run 令牌 = session 令牌的 child（三层树第二层；interrupt 取消 run
    /// 层，session 层预留给 2.5 权限/生命周期管线）。
    fn try_begin_run(&self, session_id: &str) -> Option<(RunGuard, RunHandle)> {
        let session_token = self.session_token(session_id);
        let mut runs = lock_runs(&self.runs);
        if runs.contains_key(session_id) {
            return None;
        }
        let handle = RunHandle {
            cancel: session_token.child_token(),
            abort_reason: Arc::new(OnceLock::new()),
            cancelled_at: Arc::new(OnceLock::new()),
        };
        runs.insert(session_id.to_owned(), handle.clone());
        drop(runs);
        Some((
            RunGuard {
                runs: Arc::clone(&self.runs),
                session_id: session_id.to_owned(),
            },
            handle,
        ))
    }

    /// 取（或建）session 层取消令牌。
    fn session_token(&self, session_id: &str) -> CancellationToken {
        let mut sessions = match self.sessions.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        sessions.entry(session_id.to_owned()).or_default().clone()
    }

    /// 有限多轮主循环（前置装配 → 逐轮执行 → 终态提交）。
    ///
    /// 上界语义对照旧 `QueryEngine`：达 [`MAX_TURNS`] → stopReason =
    /// `"max_turns"`（旧 L406-407）；工具续轮条件 = 助手消息含 `tool_use`
    /// 块（与旧按块存在性判定一致，非 `finish_reason`）。
    #[allow(clippy::too_many_lines)]
    async fn execute_turns(&self, session_id: &str, input: UserContentInput, run: &RunHandle) {
        let Some(setup) = self.prepare_run(session_id, input).await else {
            return;
        };
        let RunSetup {
            run_id,
            replace_after_message_id,
            user_record,
            mut request,
            call_env,
            mut conversation_options,
        } = setup;
        // Batch 8B：RunStart hook（用户消息已落库、请求已构建、run_id 已知）。
        if self.hooks.is_some() {
            let mut context = HookContext::new().with_session(session_id);
            if let Some(working_dir) = call_env.working_dir_str() {
                context = context.with_working_dir(working_dir);
            }
            self.fire_hook(HookEvent::RunStart, context).await;
        }
        let mut committed = vec![user_record];
        let mut total_usage = Usage::default();
        let mut turn_count: usize = 0;
        // 截断恢复状态（per-run，对照旧 `QueryLoopState.maxTokensOverride` /
        // `maxOutputTokensRecoveryCount`；旧 `resetRecoveryCount` 仅 stop hook
        // 路径调用，本实现无该路径故不重置）。
        let mut max_tokens_override: Option<u32> = None;
        let mut recovery_count: u32 = 0;
        // 413 上下文超限恢复的跨轮状态（Phase 去重 / 单次守卫 / 耗尽标记）。
        let mut recovery_state = RecoveryState::default();
        // L3 AutoCompact 熔断追踪（跨轮累计连续失败，达阈值打开电路）。
        let mut tracking = AutoCompactTrackingState::initial();
        // Batch 7b Step 1/2：工具调用追踪器 + 终止策略评估上下文。
        let mut tracker = ToolCallTracker::new();
        // Batch 7b Step 4：自修正循环状态（feature-gated）。
        let mut correction_attempts: u32 = 0;
        let mut previous_tool_output: Option<String> = None;
        let final_stop = loop {
            if turn_count >= conversation_options.max_turns {
                break Some("max_turns".to_owned());
            }
            turn_count += 1;
            match self
                .run_single_turn(
                    session_id,
                    &run_id,
                    turn_count,
                    &mut request,
                    run,
                    &mut committed,
                    &mut total_usage,
                    &call_env,
                    &mut recovery_state,
                    &mut tracking,
                    &mut tracker,
                    &mut correction_attempts,
                    &mut previous_tool_output,
                    &mut conversation_options,
                )
                .await
            {
                TurnFlow::Continue => {
                    // Batch 7b Step 2：每轮末尾终止策略评估（预算 / 连续错误 /
                    // 正常成功 / 轮次上界 / 滑动窗口全失败）。
                    let ctx = LoopContext {
                        turn: u32::try_from(turn_count).unwrap_or(u32::MAX),
                        max_turns: u32::try_from(conversation_options.max_turns)
                            .unwrap_or(u32::MAX),
                        consecutive_errors: tracker.consecutive_errors(),
                        tool_calls_this_turn: 0,
                        has_tool_calls: true,
                        stop_reason: None,
                        total_tokens: u64::try_from(total_usage.total_tokens()).unwrap_or(u64::MAX),
                        token_budget: 0,
                        recent_records: tracker.recent_records(5).to_vec(),
                    };
                    match evaluate(&ctx) {
                        TerminationDecision::Continue => {}
                        TerminationDecision::TerminateSuccess => {
                            break Some("end_turn".to_owned());
                        }
                        TerminationDecision::TerminateBudget => {
                            break Some("budget_exhausted".to_owned());
                        }
                        TerminationDecision::TerminateError => {
                            break Some("termination_error".to_owned());
                        }
                        TerminationDecision::RequestUserInput => {
                            break Some("request_user_input".to_owned());
                        }
                    }
                }
                TurnFlow::RecoverAndRetry => {
                    // 恢复成功不计入轮次预算（回退 turn_count）；重试次数由
                    // recovery_state 的 Phase 守卫上界，耗尽后走 Failed 分支。
                    turn_count = turn_count.saturating_sub(1);
                }
                TurnFlow::Stop(stop_reason) => {
                    // 6b `max_tokens` 恢复（逐条对照旧 `QueryEngine` L1397-1421，
                    // 判定值含旧 `"length"`——本实现在 `FinishReason` 归一化阶段
                    // 已折叠为 `max_tokens`）：恢复次数达上限 → 原 stopReason
                    // 终止；首次截断 → 升级有效 max_tokens 重试；此后每次 →
                    // 计数 +1 并注入续写用户消息重试。
                    if stop_reason.as_deref() == Some("max_tokens") {
                        if recovery_count >= MAX_OUTPUT_TOKENS_RECOVERY_LIMIT {
                            tracing::warn!(
                                session_id,
                                recovery_count,
                                "max_tokens 恢复次数已达上限，终止循环"
                            );
                            break stop_reason;
                        }
                        if max_tokens_override.is_none() {
                            tracing::info!(
                                session_id,
                                from = request.max_tokens,
                                to = ESCALATED_MAX_TOKENS,
                                "升级 maxTokens"
                            );
                            max_tokens_override = Some(ESCALATED_MAX_TOKENS);
                            request.max_tokens = ESCALATED_MAX_TOKENS;
                        } else {
                            recovery_count += 1;
                            self.append_recovery_message(session_id, &mut request, &mut committed)
                                .await;
                        }
                        continue;
                    }
                    break stop_reason;
                }
                TurnFlow::Failed(summary) => {
                    // 旧 `QueryEngine` L343：非取消异常 → `failRun(e.getMessage())`。
                    self.terminate_run(&run_id, EXIT_INTERNAL_ERROR, &summary)
                        .await;
                    return;
                }
            }
        };
        self.record_run_outcome(
            &run_id,
            run,
            recovery_state.recovery_exhausted,
            &request.model,
            &total_usage,
            turn_count,
        )
        .await;
        let model = std::mem::take(&mut request.model);
        self.commit_run(
            session_id,
            &model,
            run_id,
            replace_after_message_id,
            committed,
            total_usage,
            final_stop,
        )
        .await;
        // Batch 8B：RunEnd hook（终态已提交）。
        if self.hooks.is_some() {
            let mut context = HookContext::new().with_session(session_id);
            if let Some(working_dir) = call_env.working_dir_str() {
                context = context.with_working_dir(working_dir);
            }
            self.fire_hook(HookEvent::RunEnd, context).await;
        }
    }

    /// 注入截断续写用户消息（对照旧 6b 的 `state.addMessage(recovery)`：既进
    /// 下一轮请求消息序列，也经 `SessionMessagePersistence` 监听器落库——故此处
    /// 同时回填 `request` 与 DB / `committed`）。落库失败仅告警（旧监听器亦为
    /// best-effort），续轮照常进行。
    async fn append_recovery_message(
        &self,
        session_id: &str,
        request: &mut ChatRequest,
        committed: &mut Vec<MessageRecord>,
    ) {
        request
            .messages
            .push(ChatMessage::user(MAX_TOKENS_RECOVERY_MESSAGE));
        let recovery = NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: MAX_TOKENS_RECOVERY_MESSAGE.to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        };
        match self.db.append_message(session_id, recovery).await {
            Ok(record) => committed.push(record),
            Err(error) => {
                tracing::warn!(
                    session_id,
                    error = %error,
                    "failed to persist max_tokens recovery message"
                );
            }
        }
    }

    /// 执行一轮：LLM 流式 → 助手落库 →（有工具块）工具阶段 → 回填续轮。
    #[expect(
        clippy::too_many_lines,
        reason = "单轮状态机各分支强内聚，拆分反降可读性"
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "单轮状态机所需上下文全为必需入参（2.5 追加 run_id 供授权链定位 Run；turn 对齐旧 Step 7 `processToolResults(msgs, turn)`）"
    )]
    async fn run_single_turn(
        &self,
        session_id: &str,
        run_id: &str,
        turn: usize,
        request: &mut ChatRequest,
        run: &RunHandle,
        committed: &mut Vec<MessageRecord>,
        total_usage: &mut Usage,
        env: &CallEnv,
        recovery_state: &mut RecoveryState,
        tracking: &mut AutoCompactTrackingState,
        tracker: &mut ToolCallTracker,
        correction_attempts: &mut u32,
        previous_tool_output: &mut Option<String>,
        conversation_options: &mut ConversationRunOptions,
    ) -> TurnFlow {
        // ===== Step 1 Pre-API ContextCascade（对照旧 `QueryEngine` L647） =====
        // 逐条对齐旧调用模式：**每轮无条件调用一次**级联，不设外层 token 阈值
        // 守卫。旧 `ContextCascade` 类注释明言「Level 0-1 每次 API 调用前无条件
        // 执行（代价极低）」，L1.5 Collapse 同为无条件；**仅** L3 AutoCompact 受
        // buffer-based 阈值 + 熔断门控，且该门控在 `execute_pre_api_cascade` 内部
        // 由 `should_auto_compact`（→ `above_auto_compact_threshold`，Collapse 后
        // 重算）判定，与旧 `isAboveAutoCompactThreshold` 一致。
        //
        // L0-L2 属**上下文卫生**（截断超预算单条工具结果、清除旧工具结果、折叠），
        // 不削弱推理：既不下调推理档位也不削减 max_tokens 输出预算，故无条件执行
        // 与「追求最强推理，不在乎成本」不冲突。
        //
        // `ZK_CONTEXT_CASCADE_ENABLED=false` 时整体旁路（行为与接入前一致）。
        if cascade_enabled() {
            let messages = std::mem::take(&mut request.messages);
            let result = self
                .cascade
                .execute_pre_api_cascade(messages, &request.model, tracking);
            // L3 熔断追踪推进（连续失败达阈值 → 后续轮跳过 AutoCompact）。
            if result.auto_compact_executed {
                *tracking = tracking.with_success(run_id);
            } else if result.auto_compact_attempted {
                *tracking = tracking.with_failure();
            }
            if result.total_tokens_freed() > 0 {
                // Batch 0 Step 0-6：级联/压缩遥测三段（对照旧
                // `WsMessageHandler.onCompactEvent` → `sendCompactStart` +
                // `sendCompactComplete` 的旧序列，加发 `compact_event` 明细）。
                self.push_compact_start(session_id).await;
                self.push_compact_complete(
                    session_id,
                    &result.summary(),
                    i64::from(result.total_tokens_freed()),
                )
                .await;
                let phase = result.telemetry_phase();
                let (percent, current) = result.telemetry_snapshot();
                self.push_compact_event(session_id, phase, percent, current)
                    .await;
            }
            request.messages = result.messages;
        }
        // Step 1.5 五步消息标准化（系统消息过滤、连续同角色合并、孤儿 tool_use
        // 补 result、空 assistant 过滤），防止回放非法序列触发 provider 400。
        crate::normalize::normalize(&mut request.messages);
        let llm_started = Instant::now();
        let mut llm_start = ObservabilityEvent::new("llm", "request", "started");
        llm_start.session_id = Some(session_id.to_owned());
        llm_start.run_id = Some(run_id.to_owned());
        self.observability.record(llm_start);
        // 惰性流建立：Err 仅覆盖建立期失败（配置/序列化，D-S6 契约）。
        let stream = match self
            .provider
            .chat_stream(request.clone(), run.cancel.clone())
        {
            Ok(stream) => stream,
            Err(error) => {
                let mut event = ObservabilityEvent::new("llm", "request", "error");
                event.session_id = Some(session_id.to_owned());
                event.run_id = Some(run_id.to_owned());
                event.duration_ms =
                    Some(u64::try_from(llm_started.elapsed().as_millis()).unwrap_or(u64::MAX));
                self.observability.record(event);
                let summary = error.to_string();
                self.push_provider_failure(session_id, &error).await;
                return TurnFlow::Failed(summary);
            }
        };
        let outcome = self.consume_stream(session_id, stream, &run.cancel).await;
        let mut llm_end = ObservabilityEvent::new(
            "llm",
            "request",
            if outcome.last_error.is_some() {
                "error"
            } else if outcome.cancelled {
                "cancelled"
            } else {
                "completed"
            },
        );
        llm_end.session_id = Some(session_id.to_owned());
        llm_end.run_id = Some(run_id.to_owned());
        llm_end.duration_ms =
            Some(u64::try_from(llm_started.elapsed().as_millis()).unwrap_or(u64::MAX));
        self.observability.record(llm_end);
        // 流中中断：部分助手**不落库**（对照旧 L826-841 修正版），按
        // end_turn 终态提交（committed 保留至上一完成点）。
        if outcome.cancelled {
            return TurnFlow::Stop(Some("end_turn".to_owned()));
        }
        // 终态裁定：无 Finish 且有流内错误 → 失败（HTTP/网络类错误终止流）；
        // 单 chunk 解析错误后正常 Finish → 成功（宽容行为对齐 D-S6-3）。
        if outcome.finish.is_none()
            && let Some(error) = outcome.last_error
        {
            // ===== 413 上下文超限三阶段恢复（对照旧 QueryEngine 413 分支） =====
            // 默认启用；关闭 flag 时直接透传失败（行为与接入前一致）。
            if cascade_enabled() {
                let (status, message) = provider_error_parts(&error);
                if is_context_limit_error(status, &message) {
                    let context_window = context_window_for(&request.model);
                    if let RecoveryOutcome::Recovered {
                        messages,
                        phase,
                        before_tokens,
                        after_tokens,
                    } = self.recovery.recover(
                        &request.messages,
                        &request.model,
                        context_window,
                        &message,
                        recovery_state,
                    ) {
                        tracing::warn!(
                            session_id,
                            ?phase,
                            before_tokens,
                            after_tokens,
                            "413 上下文超限已恢复，以更小上下文重试当前轮"
                        );
                        request.messages = messages;
                        // Batch 0 Step 0-6：413 恢复亦发 compact_start / _event 遥测。
                        self.push_compact_start(session_id).await;
                        self.push_compact_complete(
                            session_id,
                            &format!("413 recovery via {phase:?}"),
                            i64::from(before_tokens.saturating_sub(after_tokens)),
                        )
                        .await;
                        let (percent, current) =
                            crate::context::compact_percent_freed(before_tokens, after_tokens);
                        self.push_compact_event(
                            session_id,
                            phase.telemetry_name(),
                            percent,
                            current,
                        )
                        .await;
                        return TurnFlow::RecoverAndRetry;
                    }
                }
            }
            let summary = error.to_string();
            self.push_provider_failure(session_id, &error).await;
            return TurnFlow::Failed(summary);
        }
        add_usage(
            total_usage,
            outcome.usage.as_ref().unwrap_or(&Usage::default()),
        );
        // Batch 0 Step 0-6：LLM 响应完成后推送 `cost_update`（对照旧
        // `WsMessageHandler.onUsage` → `sendCostUpdate` 每轮触发点）。仅在
        // 本轮 provider 实报 usage 时推送——旧 `handler.onUsage(assistantMessage.usage())`
        // 亦仅在 `assistantMessage.usage() != null` 分支调用（`QueryEngine` L1020-1022）。
        if let Some(delta) = outcome.usage.as_ref() {
            self.push_cost_update(session_id, &request.model, delta)
                .await;
        }
        // flush 草稿：arguments 空 → `{}`；JSON 非法 → INVALID_TOOL_INPUT_JSON
        // 致命（对照旧 flushToolBlock；失败走旧失败序列，retryable 恒 true）。
        let calls = match flush_tool_drafts(outcome.tool_drafts) {
            Ok(calls) => calls,
            Err(message) => {
                let summary = message.clone();
                self.push_error(session_id, "query_error", message, true)
                    .await;
                self.push_fallback_complete(session_id).await;
                return TurnFlow::Failed(summary);
            }
        };
        // 流耗尽无 finish_reason 的宽容路径（对齐旧 L221-222）：stopReason 为 null。
        let stop_reason = outcome
            .finish
            .as_ref()
            .map(|reason| reason.as_str().to_owned());
        let mut blocks = Vec::new();
        if !outcome.thinking.is_empty() {
            blocks.push(StoredBlock::Thinking {
                thinking: outcome.thinking.clone(),
            });
        }
        // text 块：**非空才写**（逐字对照旧 `Collector.flushTextBlock`
        // L2096-2102 的 `if (!currentText.isEmpty())`——旧实现从不产出空
        // `TextBlock`）。空正文助手轮（典型：thinking 耗尽输出预算的
        // `max_tokens` 截断轮）落库为无 text 块的消息，不再写入 `text: ""`：
        // 后者回放时会以 `content: ""` 发给 provider，触发
        // `the message at position N with role 'assistant' must not be empty`
        // 400，令会话永久不可用。
        if !outcome.text.is_empty() {
            blocks.push(StoredBlock::Text {
                text: outcome.text.clone(),
            });
        }
        for call in &calls {
            blocks.push(StoredBlock::ToolUse {
                id: call.id.clone(),
                name: call.name.clone(),
                input: call.input.clone(),
            });
        }
        let assistant_message = NewMessage {
            role: MessageRole::Assistant,
            content: blocks,
            stop_reason: stop_reason.clone(),
            input_tokens: outcome.usage.as_ref().map_or(0, |u| u.input_tokens),
            output_tokens: outcome.usage.as_ref().map_or(0, |u| u.output_tokens),
        };
        let assistant_record = match self.db.append_message(session_id, assistant_message).await {
            Ok(record) => {
                // ===== 文件历史事务边界：开始（对照旧 `QueryEngine` L1004-1010）=====
                // 起点取本轮助手消息 id 与「当前请求消息条数」，与旧
                // `beginTransaction(sessionId, assistantMessage.uuid(),
                // state.getMessages().size())` 一致：后续写前快照自动挂到该回合。
                if let Some(history) = self.file_history.as_ref() {
                    history.begin_transaction(session_id, &record.id, request.messages.len());
                }
                committed.push(record.clone());
                record
            }
            Err(error) => {
                let summary = format!("failed to persist assistant message: {error}");
                self.push_error(session_id, "query_error", summary.clone(), true)
                    .await;
                self.push_fallback_complete(session_id).await;
                return TurnFlow::Failed(summary);
            }
        };
        if calls.is_empty() {
            // 终轮助手消息回填请求消息序列（对照旧 `state.addMessage(assistant)`
            // 恒执行 + `MessageNormalizer` 丢弃空白正文 assistant 的组合语义）：
            // 非空白正文才回填，供 `max_tokens` 截断恢复续轮携带已生成片段；
            // 正常终止路径 `request` 随即废弃，无可观察影响。
            if !outcome.text.trim().is_empty() {
                request.messages.push(
                    ChatMessage::assistant(outcome.text.clone())
                        .with_thinking(Some(outcome.thinking.clone())),
                );
            }
            if stop_reason.as_deref() == Some("end_turn")
                && !outcome.text.trim().is_empty()
                && let Err(error) = self
                    .db
                    .bind_workbench_result(run_id, &assistant_record.id)
                    .await
            {
                tracing::error!(run_id, %error, "failed to bind workbench result message");
            }
            // ===== 文件历史事务边界：提交（无工具调用的终轮）=====
            self.commit_file_history(session_id);
            return TurnFlow::Stop(stop_reason);
        }
        // 完整入参补发（对照旧 onToolUseComplete L1103：流式收齐后逐块推送）。
        for call in &calls {
            self.sink
                .push(
                    session_id,
                    ServerMessage::ToolUseInput {
                        tool_use_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        input: call.input.clone(),
                    },
                )
                .await;
        }
        match self
            .run_tool_phase(
                session_id,
                run_id,
                &calls,
                run,
                committed,
                env,
                tracker,
                request,
                conversation_options,
            )
            .await
        {
            ToolPhase::Aborted => TurnFlow::Stop(Some("end_turn".to_owned())),
            ToolPhase::Completed(tool_messages) => {
                // Self-correction is result-driven rather than tool-name-driven: any
                // admitted tool can return compiler/test diagnostics. Repeated output is
                // stopped by the loop guard before a repair instruction is injected.
                let mut correction_instruction = None;
                let mut tool_output_for_state = None;
                if FeatureFlags::from_env().is_enabled(SELF_CORRECTION_LOOP) {
                    for call in &calls {
                        if let Some(msg) = tool_messages
                            .iter()
                            .find(|m| m.tool_call_id.as_deref() == Some(&call.id))
                        {
                            let repo_name = env.working_dir_str().unwrap_or("");
                            if let Some(instr) = detect_and_prepare_correction(
                                &msg.content,
                                *correction_attempts,
                                repo_name,
                            ) && !should_abort(&msg.content, previous_tool_output.as_deref())
                            {
                                tool_output_for_state = Some(msg.content.clone());
                                correction_instruction = Some(instr);
                                break;
                            }
                        }
                    }
                }
                // 续轮回填：assistant(tool_calls) + 每结果一条 tool 消息
                // （对照旧请求构造：assistant 带 tool_calls、tool_result →
                // {role:"tool", tool_call_id, content}）。
                request.messages.push(
                    ChatMessage::assistant_tool_calls(outcome.text, to_tool_call_requests(&calls))
                        .with_thinking(Some(outcome.thinking)),
                );
                request.messages.extend(tool_messages);
                // 注入自修正修复指令（如检测到可修复错误且未中止）。
                if let Some(instr) = correction_instruction {
                    request.messages.push(ChatMessage::user(instr.instruction));
                    *correction_attempts += 1;
                    *previous_tool_output = tool_output_for_state;
                }
                // Step 7（对照旧 `QueryEngine` L1443 轮末 `processToolResults`）：
                // 对过大工具结果截断，供下一轮 API 携带更小上下文。小结果
                // （<= SOFT_LIMIT_CHARS）不触发。第二参逐字对齐旧调用点传入的
                // `turn`（旧 `processToolResults` 内部同样未使用该参数）。
                //
                // 门控收敛：feature-flag 仅在 `ToolResultSummarizer::new()`
                // （引擎构造期）读取一次 env，此处恒调用、由摘要器内部 gate 决定
                // 是否旁路——关闭态逐字返回原消息，行为与接入前一致。
                request.messages = self.summarizer.process_tool_results(
                    &request.messages,
                    u32::try_from(turn).unwrap_or(u32::MAX),
                );
                // ===== 文件历史事务边界：提交（对照旧 `QueryEngine` L1183-1186）=====
                self.commit_file_history(session_id);
                TurnFlow::Continue
            }
        }
    }

    /// 消费 provider 事件流（推流式增量 + 聚合终态；biased 取消优先）。
    ///
    /// 取消语义对齐 D-S6-5：观察到取消即刻返回（`cancelled = true`），
    /// 丢弃流与一切积压事件——不再推任何增量、不产出假成功终态。
    async fn consume_stream(
        &self,
        session_id: &str,
        mut stream: BoxStream<'static, ProviderEvent>,
        cancel: &CancellationToken,
    ) -> StreamOutcome {
        let mut outcome = StreamOutcome::default();
        let mut first_token_recorded = false;
        loop {
            let event = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    outcome.cancelled = true;
                    return outcome;
                }
                event = stream.next() => event,
            };
            let Some(event) = event else { break };
            if !first_token_recorded
                && matches!(
                    &event,
                    ProviderEvent::TextDelta { .. }
                        | ProviderEvent::ThinkingDelta { .. }
                        | ProviderEvent::ToolUseStart { .. }
                )
            {
                let mut first = ObservabilityEvent::new("llm", "first_token", "ok");
                first.session_id = Some(session_id.to_owned());
                self.observability.record(first);
                first_token_recorded = true;
            }
            match event {
                ProviderEvent::TextDelta { text } => {
                    outcome.text.push_str(&text);
                    self.sink
                        .push(session_id, ServerMessage::StreamDelta { delta: text })
                        .await;
                }
                ProviderEvent::ThinkingDelta { thinking } => {
                    outcome.thinking.push_str(&thinking);
                    self.sink
                        .push(session_id, ServerMessage::ThinkingDelta { delta: thinking })
                        .await;
                }
                ProviderEvent::ToolUseStart { id, name } => {
                    // 旧 sendToolUseStart（L345）：id+name 齐备即推，input
                    // 为空对象占位（完整入参 flush 后由 tool_use_input 补发）。
                    self.sink
                        .push(
                            session_id,
                            ServerMessage::ToolUseStart {
                                tool_use_id: id.clone(),
                                tool_name: name.clone(),
                                input: json!({}),
                            },
                        )
                        .await;
                    outcome.tool_drafts.push(ToolDraft {
                        id,
                        name,
                        arguments: String::new(),
                    });
                }
                ProviderEvent::ToolInputDelta { id, delta } => {
                    if let Some(draft) = outcome.tool_drafts.iter_mut().find(|draft| draft.id == id)
                    {
                        draft.arguments.push_str(&delta);
                    } else {
                        // provider 层已保证 Start 先行；孤儿分片仅告警丢弃。
                        tracing::warn!(
                            session_id,
                            tool_use_id = %id,
                            "input delta for unknown tool call dropped"
                        );
                    }
                }
                ProviderEvent::UsageUpdate { usage } => outcome.usage = Some(usage),
                ProviderEvent::Finish {
                    finish_reason,
                    usage,
                } => {
                    if usage.is_some() {
                        outcome.usage = usage;
                    }
                    outcome.finish = Some(finish_reason);
                }
                ProviderEvent::Error { error } => {
                    // 宽容行为（D-S6-3）：记录后继续消费；是否致命由流自身
                    // 是否终止决定（run_single_turn 依据 finish 缺失裁定）。
                    tracing::warn!(session_id, error = %error, "provider stream error");
                    outcome.last_error = Some(error);
                }
            }
        }
        outcome
    }

    /// 工具执行阶段：并发派发全部调用 → 按到达序推 progress / result →
    /// 汇齐后按声明序组装续轮 tool 消息。
    ///
    /// 中断（biased 取消优先）→ [`Self::abort_tool_phase`]（FIX-02 合成
    /// 落库）→ `Aborted`。未知工具不派发执行器——直接合成错误结果回喂
    /// 模型（旧逐字文案，含可用工具清单）。
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    async fn run_tool_phase(
        &self,
        session_id: &str,
        run_id: &str,
        calls: &[FlushedCall],
        run: &RunHandle,
        committed: &mut Vec<MessageRecord>,
        env: &CallEnv,
        tracker: &mut ToolCallTracker,
        request: &mut ChatRequest,
        conversation_options: &mut ConversationRunOptions,
    ) -> ToolPhase {
        let mut results: HashMap<String, ToolResultContent> = HashMap::new();
        let mut streams = Vec::with_capacity(calls.len());
        let mut tool_started: HashMap<String, Instant> = HashMap::new();
        for call in calls {
            if conversation_options.allows(&call.name)
                && let Some(tool) = self.tools.get(&call.name)
            {
                tool_started.insert(call.id.clone(), Instant::now());
                let mut event = ObservabilityEvent::new("tool", "execute", "started");
                event.session_id = Some(session_id.to_owned());
                event.run_id = Some(run_id.to_owned());
                event.tool_use_id = Some(call.id.clone());
                event.attributes.insert(
                    "tool".to_owned(),
                    serde_json::Value::String(call.name.clone()),
                );
                self.observability.record(event);
                // PRE hooks run before Admission. Any modified input is treated
                // as untrusted and passes through the full admission stack.
                let mut hook_context = HookContext::new()
                    .with_tool(call.name.clone())
                    .with_session(session_id);
                if let Some(working_dir) = env.working_dir_str() {
                    hook_context = hook_context.with_working_dir(working_dir);
                }
                let pre_input = if let Some(hooks) = &self.hooks {
                    match hooks.evaluate_pre_tool(&hook_context, &call.input).await {
                        PreHookDecision::Continue { input } => input,
                        PreHookDecision::Deny { code, message } => {
                            tracing::warn!(session_id, run_id, tool = %call.name, %code, "tool denied by PRE hook");
                            let result = self
                                .record_tool_result(
                                    session_id,
                                    &call.id,
                                    ToolOutput::error(format!("{code}: {message}")),
                                    committed,
                                )
                                .await;
                            results.insert(call.id.clone(), result);
                            let mut event = ObservabilityEvent::new("tool", "execute", "denied");
                            event.session_id = Some(session_id.to_owned());
                            event.run_id = Some(run_id.to_owned());
                            event.tool_use_id = Some(call.id.clone());
                            event.security_audit = true;
                            event.duration_ms = tool_started.get(&call.id).map(|started| {
                                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
                            });
                            self.observability.record(event);
                            continue;
                        }
                    }
                } else {
                    call.input.clone()
                };
                // ── 2.5 准入：工具执行前拦截（旧 ToolExecutionPipeline 阶段 4/5）──
                let admitted = self
                    .admission
                    .admit(AdmissionRequest {
                        session_id,
                        run_id,
                        tool_use_id: &call.id,
                        tool_name: &call.name,
                        input: &pre_input,
                        working_directory: env.working_dir_str(),
                    })
                    .await;
                let execution_input = match admitted {
                    Admission::Allow { execution_input } => execution_input,
                    Admission::Denied { code, message } => {
                        // 旧 L336-343：先推 tool_permission_denied（前端清理
                        // changedFiles），再以 permissionDenied 结果回喂模型。
                        tracing::info!(
                            session_id,
                            run_id,
                            tool = %call.name,
                            code = %code,
                            "tool authorization ended"
                        );
                        self.sink
                            .push(
                                session_id,
                                ServerMessage::ToolPermissionDenied {
                                    tool_use_id: call.id.clone(),
                                    tool_name: call.name.clone(),
                                },
                            )
                            .await;
                        let result = self
                            .record_tool_result(
                                session_id,
                                &call.id,
                                ToolOutput::error(message),
                                committed,
                            )
                            .await;
                        results.insert(call.id.clone(), result);
                        let mut event = ObservabilityEvent::new("tool", "execute", "denied");
                        event.session_id = Some(session_id.to_owned());
                        event.run_id = Some(run_id.to_owned());
                        event.tool_use_id = Some(call.id.clone());
                        event.security_audit = true;
                        event.duration_ms = tool_started.get(&call.id).map(|started| {
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
                        });
                        self.observability.record(event);
                        continue;
                    }
                    Admission::Failed { code, message } => {
                        // 旧 L344-386：准入/授权存储/交互落库/入参校验四类失败**不推**
                        // 任何下行，只把 ToolResult.failed(...) 回喂模型。
                        tracing::warn!(
                            session_id,
                            run_id,
                            tool = %call.name,
                            code = %code,
                            "tool admission failed before execution"
                        );
                        let result = self
                            .record_tool_result(
                                session_id,
                                &call.id,
                                ToolOutput::error(message),
                                committed,
                            )
                            .await;
                        results.insert(call.id.clone(), result);
                        let mut event = ObservabilityEvent::new("tool", "execute", "error");
                        event.session_id = Some(session_id.to_owned());
                        event.run_id = Some(run_id.to_owned());
                        event.tool_use_id = Some(call.id.clone());
                        event.duration_ms = tool_started.get(&call.id).map(|started| {
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
                        });
                        self.observability.record(event);
                        continue;
                    }
                };
                let rx = self.executor.spawn_call_in(
                    tool,
                    call.id.clone(),
                    execution_input,
                    &run.cancel,
                    env.clone(),
                );
                streams.push(tool_event_stream(rx));
            } else {
                let output = ToolOutput::error(unknown_tool_message(
                    &call.name,
                    &filtered_tool_names(&self.tools, conversation_options),
                ));
                let result = self
                    .record_tool_result(session_id, &call.id, output, committed)
                    .await;
                results.insert(call.id.clone(), result);
            }
        }
        // 合并事件流（空集时立即耗尽）；事件按各调用产出的到达序交错。
        let mut merged = futures::stream::select_all(streams);
        loop {
            let event = tokio::select! {
                biased;
                () = run.cancel.cancelled() => {
                    self.abort_tool_phase(session_id, calls, &results, run, committed)
                        .await;
                    return ToolPhase::Aborted;
                }
                event = merged.next() => event,
            };
            let Some(event) = event else { break };
            match event {
                ToolEvent::Progress { tool_use_id, text } => {
                    self.sink
                        .push(
                            session_id,
                            ServerMessage::ToolUseProgress {
                                tool_use_id,
                                progress: text,
                            },
                        )
                        .await;
                }
                ToolEvent::Finished {
                    tool_use_id,
                    output,
                } => {
                    // Batch 7b Step 1：记录工具调用结果（连续错误 / 滑动窗口）。
                    let tool_name = calls
                        .iter()
                        .find(|c| c.id == tool_use_id)
                        .map_or("unknown", |c| c.name.as_str());
                    let is_success = !output.is_error;
                    let mut event = ObservabilityEvent::new(
                        "tool",
                        "execute",
                        if is_success { "completed" } else { "error" },
                    );
                    event.session_id = Some(session_id.to_owned());
                    event.run_id = Some(run_id.to_owned());
                    event.tool_use_id = Some(tool_use_id.clone());
                    event.duration_ms = tool_started.get(&tool_use_id).map(|started| {
                        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
                    });
                    self.observability.record(event);
                    let error_message = if output.is_error {
                        Some(output.content.clone())
                    } else {
                        None
                    };
                    tracker.record(tool_name, is_success, error_message);
                    if tool_name == "Skill" && !output.is_error {
                        apply_skill_directive(
                            request,
                            conversation_options,
                            output.metadata.as_ref(),
                        );
                    }
                    if tool_name == "Visualization"
                        && !output.is_error
                        && let Some((uuid, view_type, props)) =
                            visualization_message(output.metadata.as_ref())
                    {
                        self.sink
                            .push(
                                session_id,
                                ServerMessage::Visualization {
                                    uuid,
                                    view_type,
                                    props,
                                },
                            )
                            .await;
                    }
                    // Batch 7：检测 metadata 中的 mode 字段触发权限模式切换。
                    if let Some(switcher) = &self.mode_switcher
                        && let Some(mode_str) = output
                            .metadata
                            .as_ref()
                            .and_then(|m| m.get("mode"))
                            .and_then(|v| v.as_str())
                    {
                        switcher.switch_mode(session_id, mode_str).await;
                    }
                    let result = self
                        .record_tool_result(session_id, &tool_use_id, output, committed)
                        .await;
                    // Batch 8B：PostToolExecution hook（结果已落库；预览取落库后内容）。
                    if self.hooks.is_some() {
                        let mut context = HookContext::new()
                            .with_tool(tool_name)
                            .with_session(session_id)
                            .with_result_preview(result.content.clone());
                        if let Some(working_dir) = env.working_dir_str() {
                            context = context.with_working_dir(working_dir);
                        }
                        self.fire_hook(HookEvent::PostToolExecution, context).await;
                    }
                    results.insert(tool_use_id, result);
                }
            }
        }
        // 通道全关但结果缺失 = 取消竞态（执行器取消路径不产 Finished）。
        let mut messages = Vec::with_capacity(calls.len());
        for call in calls {
            let Some(result) = results.get(&call.id) else {
                self.abort_tool_phase(session_id, calls, &results, run, committed)
                    .await;
                return ToolPhase::Aborted;
            };
            messages.push(ChatMessage::tool(call.id.clone(), result.content.clone()));
        }
        ToolPhase::Completed(messages)
    }

    /// 单个工具结果终态：推 `tool_result` + 落库（每结果一条 role=user
    /// 消息、单 `tool_result` 块，对照旧 `buildToolResultMessage`）。
    async fn record_tool_result(
        &self,
        session_id: &str,
        tool_use_id: &str,
        output: ToolOutput,
        committed: &mut Vec<MessageRecord>,
    ) -> ToolResultContent {
        let result = ToolResultContent {
            content: output.content,
            is_error: output.is_error,
            metadata: structured_result_metadata(output.metadata),
        };
        self.sink
            .push(
                session_id,
                ServerMessage::ToolResult {
                    tool_use_id: tool_use_id.to_owned(),
                    result: result.clone(),
                },
            )
            .await;
        self.persist_tool_result(session_id, tool_use_id, &result, committed)
            .await;
        result
    }

    /// 工具结果落库（不推送——中断合成路径复用；失败仅告警不阻断，
    /// 续轮上下文在内存请求中仍完整，留痕 §3）。
    async fn persist_tool_result(
        &self,
        session_id: &str,
        tool_use_id: &str,
        result: &ToolResultContent,
        committed: &mut Vec<MessageRecord>,
    ) {
        let message = NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::ToolResult {
                tool_use_id: tool_use_id.to_owned(),
                content: result.content.clone(),
                is_error: result.is_error,
                metadata: result.metadata.clone(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        };
        match self.db.append_message(session_id, message).await {
            Ok(record) => committed.push(record),
            Err(error) => {
                tracing::warn!(
                    session_id,
                    tool_use_id,
                    error = %error,
                    "failed to persist tool result"
                );
            }
        }
    }

    /// 中断收尾（逐字对照旧 FIX-02，L1035-1076）：未完成工具合成
    /// [`INTERRUPTED_TOOL_RESULT`] **落库不推送**；`USER_INTERRUPT` 时追加
    /// 用户可见通知消息（`SUBMIT_INTERRUPT` 不追加）。
    async fn abort_tool_phase(
        &self,
        session_id: &str,
        calls: &[FlushedCall],
        results: &HashMap<String, ToolResultContent>,
        run: &RunHandle,
        committed: &mut Vec<MessageRecord>,
    ) {
        for call in calls {
            if results.contains_key(&call.id) {
                continue;
            }
            let synthesized = ToolResultContent {
                content: INTERRUPTED_TOOL_RESULT.to_owned(),
                is_error: true,
                metadata: None,
            };
            self.persist_tool_result(session_id, &call.id, &synthesized, committed)
                .await;
        }
        // 原因缺省按 USER_INTERRUPT（旧 AbortReason 默认值；interrupt()
        // 恒先写原因再取消，缺省仅覆盖 session 层取消等边缘路径）。
        let reason = run
            .abort_reason
            .get()
            .copied()
            .unwrap_or(REASON_USER_INTERRUPT);
        if reason == REASON_USER_INTERRUPT {
            let notice = NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: USER_INTERRUPT_NOTICE.to_owned(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            };
            match self.db.append_message(session_id, notice).await {
                Ok(record) => committed.push(record),
                Err(error) => {
                    tracing::warn!(
                        session_id,
                        error = %error,
                        "failed to persist interrupt notice"
                    );
                }
            }
        }
    }

    /// Run 终态回写分派（对照旧 `QueryEngine` L375-393，在返回 `QueryResult`
    /// ——即下行 `message_complete`——**之前**）：中断 → `abortRun`、恢复耗尽 →
    /// `failRun`、否则 `completeRun`。
    async fn record_run_outcome(
        &self,
        run_id: &str,
        run: &RunHandle,
        recovery_exhausted: bool,
        model: &str,
        total_usage: &Usage,
        turn_count: usize,
    ) {
        if run.cancel.is_cancelled() {
            // 旧 `abortRun`：USER_INTERRUPT / SUBMIT_INTERRUPT → `cancelByUser`，
            // detail 取 `cancellationDetail(reason)` = `"user_cancelled"`
            // （旧 L546-553）。zkcode 的中断仅这两种原因（`interrupt` 恒先写
            // 原因再取消），故 detail 恒为 `user_cancelled`。
            self.terminate_run(run_id, EXIT_USER_CANCELLED, EXIT_USER_CANCELLED)
                .await;
        } else if recovery_exhausted {
            // 旧 L385-386 逐字字面量。
            self.terminate_run(
                run_id,
                EXIT_INTERNAL_ERROR,
                "context budget recovery exhausted",
            )
            .await;
        } else {
            // 旧源向 Run 传 0.0 会让审计 API 与会话费用永久不一致；这里复用
            // 会话级同一费率函数，保证 Run 与 Session 的成本口径一致。
            self.complete_run(
                run_id,
                total_usage.total_tokens(),
                usage_cost_usd(model, total_usage),
                turn_count,
            )
            .await;
        }
    }

    /// Run 正常完成回写（旧 `RunTracker.completeRun`，见
    /// [`Db::complete_run`]）。
    ///
    /// 迁移失败或非 `APPLIED` 只告警：旧源同样把状态回写包在
    /// `catch (Exception e) { log.warn(...) }` 里（L391-393）——Run 状态是审计
    /// 面，不得反噬已成功的会话输出。
    async fn complete_run(
        &self,
        run_id: &str,
        total_tokens: i64,
        total_cost_usd: f64,
        turn_count: usize,
    ) {
        let turns = i64::try_from(turn_count).unwrap_or(i64::MAX);
        match self
            .db
            .complete_run(run_id, total_tokens, total_cost_usd, turns)
            .await
        {
            Ok(zk_db::run::TransitionResult::Applied) => {}
            Ok(other) => tracing::debug!(
                run_id,
                result = other.as_str(),
                "run completion transition ignored"
            ),
            Err(error) => tracing::warn!(run_id, error = %error, "failed to complete run"),
        }
    }

    /// Run 异常/中断终态回写（旧 `RunTracker.failRun` / `abortRun`，见
    /// [`Db::terminate_run`]）。告警语义同 [`Self::complete_run`]。
    async fn terminate_run(&self, run_id: &str, exit_reason: &str, detail: &str) {
        match self
            .db
            .terminate_run(run_id, exit_reason, Some(detail))
            .await
        {
            Ok(zk_db::run::TransitionResult::Applied) => {}
            Ok(other) => tracing::debug!(
                run_id,
                exit_reason,
                result = other.as_str(),
                "run termination transition ignored"
            ),
            Err(error) => tracing::warn!(run_id, error = %error, "failed to terminate run"),
        }
    }

    /// run 终态提交：`message_complete`（跨轮累计 usage + 锚点后全部持久化
    /// 消息）→ `session_list_updated`（对照旧 executeQueryInternal 尾序）。
    #[expect(
        clippy::too_many_arguments,
        reason = "终态提交所需上下文全为必需入参（会话用量回写追加 model 费率键）"
    )]
    async fn commit_run(
        &self,
        session_id: &str,
        model: &str,
        run_id: String,
        replace_after_message_id: Option<String>,
        committed: Vec<MessageRecord>,
        total_usage: Usage,
        stop_reason: Option<String>,
    ) {
        // 会话级用量 / 成本回写：费用公式逐行对照旧
        // `CostTrackerService.recordUsage`（input + output − cacheRead×0.9）。
        // 旧实现该服务与 `SessionRepository.updateUsage` 均无生产调用方，
        // sessions 累计列恒 0；此处补齐落库使累计列真实可用（缺陷修复）。
        // 成本对 token 线性，故 per-run 汇总与旧 per-call 累加等值。
        if total_usage != Usage::default() {
            let cost_usd = usage_cost_usd(model, &total_usage);
            if let Err(error) = self
                .db
                .add_session_usage(session_id, &total_usage, cost_usd)
                .await
            {
                tracing::warn!(
                    session_id,
                    error = %error,
                    "failed to persist session usage totals"
                );
            }
        }
        self.sink
            .push(
                session_id,
                ServerMessage::MessageComplete {
                    usage: total_usage,
                    stop_reason,
                    session_id: Some(session_id.to_owned()),
                    run_id: Some(run_id),
                    replace_after_message_id,
                    committed_messages: Some(
                        committed.into_iter().map(record_to_ws_message).collect(),
                    ),
                },
            )
            .await;
        self.sink
            .push(session_id, ServerMessage::SessionListUpdated)
            .await;
    }

    /// run 前置装配：会话校验 → 历史回放 → 用户消息落库（provider 调用
    /// **前**，失败 run 用户消息保留）→ 请求构建（tools 随请求下发）。
    #[allow(clippy::too_many_lines)] // one durable run-creation transaction with ordered guards
    async fn prepare_run(&self, session_id: &str, input: UserContentInput) -> Option<RunSetup> {
        let conversation_options = lock_mutex(&self.conversation_options)
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        let detail = match self.db.get_session(session_id).await {
            Ok(Some(detail)) => detail,
            Ok(None) => {
                // 会话不存在：旧 `requireSessionWorkingDirectory` 抛
                // `IllegalStateException`，由 handleUserMessage 的 catch 归一为
                // `query_error` + retryable=true（旧 L690）。该异常发生在
                // executeQueryInternal 的 try 之前，旧 finally 未执行——故仍只
                // 发 `error`，无兜底 complete。
                self.push_error(
                    session_id,
                    "query_error",
                    format!("会话不存在: {session_id}"),
                    true,
                )
                .await;
                return None;
            }
            Err(error) => {
                self.push_error(
                    session_id,
                    "query_error",
                    format!("failed to load session: {error}"),
                    true,
                )
                .await;
                self.push_fallback_complete(session_id).await;
                return None;
            }
        };
        let replace_after_message_id = detail.messages.last().map(|record| record.id.clone());
        let acceptance_sources = extract_acceptance_criteria(&input.text);
        let mut messages = history_to_chat_messages(&detail.messages);
        // 图片路由只覆盖本次 Run 的有效模型，不回写 `sessions.model`。候选必须
        // 来自生产注入的已配置 provider 视图；无候选时保持原模型，让下方能力
        // 校验稳定返回 ATTACHMENT_MODEL_UNSUPPORTED。
        let routed_model = if !input.attachments.is_empty()
            && !zk_llm::capabilities_for(&detail.model).supports_images
        {
            self.vision_providers
                .as_deref()
                .and_then(|providers| providers.resolve_vision_model(&detail.model))
        } else {
            None
        };
        let effective_model = routed_model.as_deref().unwrap_or(&detail.model).to_owned();
        let (stored_content, current_message) = match resolve_user_content(
            &detail,
            &effective_model,
            input,
            self.trusted_image_url.as_ref(),
        ) {
            Ok(content) => content,
            Err((code, message)) => {
                self.push_error(session_id, code, message, false).await;
                return None;
            }
        };
        if let Some(routed_model) = routed_model {
            let routed_model_name = zk_llm::capabilities_for(&routed_model)
                .display_name
                .to_string();
            self.sink
                .push(
                    session_id,
                    ServerMessage::ModelRouted {
                        original_model: detail.model.clone(),
                        routed_model: routed_model.clone(),
                        routed_model_name: routed_model_name.clone(),
                        reason: format!("当前模型不支持图片，已自动切换到 {routed_model_name}"),
                    },
                )
                .await;
        }
        messages.push(current_message);
        let user_message = NewMessage {
            role: MessageRole::User,
            content: stored_content,
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        };
        let user_record = match self.db.append_message(session_id, user_message).await {
            Ok(record) => record,
            Err(error) => {
                self.push_error(
                    session_id,
                    "query_error",
                    format!("failed to persist user message: {error}"),
                    true,
                )
                .await;
                self.push_fallback_complete(session_id).await;
                return None;
            }
        };
        // 思考显式关闭：Phase 1 基线保持（思考参数接线归多提供商任务 2.7）。
        // 工具环境（2.3）：会话工作目录为空时不注入——`ToolContext` 回落
        // 进程当前目录（对齐旧 `requireSessionWorkingDirectory` 之外的兜底）。
        let mut call_env = CallEnv::new().with_session_id(session_id);
        if !detail.working_dir.is_empty() {
            call_env = call_env.with_working_dir(&detail.working_dir);
        }
        // 输出预算按模型能力表取值（对照旧
        // `QueryConfig.getRecommendedMaxTokens` = min(模型输出上限, 65536)）：
        // 此前恒用 `ChatRequest::new` 的 8192 默认档，长思考模型（如 kimi-k3
        // 的 reasoning_effort=max）会在 thinking 阶段耗尽预算并返回空正文。
        let max_tokens = recommended_max_tokens(&effective_model);
        // Run 落库（对照旧 `QueryEngine.executeQueryInternal` L284-307：
        // `runTracker.startRun(sessionId, parentRunId, agentType, config.model())`
        // → `currentRunId = run.id()` → 经 `withCurrentRunId` 传播至工具上下文）。
        // 必须在首轮 LLM/工具调用**之前**：工具阶段的授权祖先链
        // （`zk-authz` 的 `AuthorizationSubjectResolver::load_root`）逐层上溯
        // `run_envelopes`，缺行即 `Run ancestry contains a missing parent`，
        // 全部工具调用被拒。
        //
        // `parent_run_id` 恒 `None`（根 Run）：旧源取
        // `state.getToolUseContext().currentRunId()`，即**子代理**场景下父 run
        // 的 id（`agentType` 随之为 `"subagent"`）。zkcode 尚无 Task/子代理入口
        // （M-SUBAGENT），故此处恒为根 Run；届时把父 run 沿调用链传入
        // [`Db::start_run`] 的第三参并把 `agent_type` 换为 `"subagent"` 即可，
        // 无需改签名。
        let run_id = uuid::Uuid::new_v4().to_string();
        if let Err(error) = self
            .db
            .start_run(
                &run_id,
                session_id,
                None,
                Some(AGENT_TYPE_QUERY),
                &effective_model,
            )
            .await
        {
            // 旧源 L306-318：`startRun` 抛异常 → `handler.onError(e)` +
            // 返回 stopReason=`error` 的 QueryResult（`currentRunId` 仍为
            // null，故**不**做 failRun）。zkcode 的等价终态序列即
            // error + 兜底 complete。
            tracing::error!(
                session_id,
                error = %error,
                "failed to establish Run execution authority"
            );
            self.push_error(
                session_id,
                "query_error",
                format!("RUN_EXECUTION_REGISTRATION_FAILED: {error}"),
                true,
            )
            .await;
            self.push_fallback_complete(session_id).await;
            return None;
        }
        let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
        let criteria = acceptance_sources
            .into_iter()
            .enumerate()
            .map(|(ordinal, source_text)| AcceptanceCriterionRecord {
                criterion_id: uuid::Uuid::new_v4().to_string(),
                root_run_id: run_id.clone(),
                ordinal: i64::try_from(ordinal).unwrap_or(i64::MAX),
                criterion_type: "business".into(),
                source_text,
                status: "not_verified".into(),
                evidence_bundle_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            })
            .collect::<Vec<_>>();
        if let Err(error) = self
            .db
            .initialize_workbench(
                &WorkbenchBindingRecord {
                    root_run_id: run_id.clone(),
                    request_message_id: user_record.id.clone(),
                    result_message_id: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
                &criteria,
            )
            .await
        {
            let summary = format!("WORKBENCH_INITIALIZATION_FAILED: {error}");
            self.terminate_run(&run_id, EXIT_INTERNAL_ERROR, &summary)
                .await;
            self.push_error(session_id, "query_error", summary, true)
                .await;
            self.push_fallback_complete(session_id).await;
            return None;
        }
        // Run 已确立 → 传播至工具上下文（对照旧 `withCurrentRunId(runId)`：
        // `ToolUseContext.currentRunId` 是 `ELICITATION` 交互的归属键，缺失即
        // 被持久交互侧以 `INTERACTION_REQUIRES_RUN` 拒绝，`AskUserQuestion`
        // 将无法发问）。必须在 `start_run` 成功之后赋值——失败路径直接返回，
        // 不存在半确立的 run id。
        call_env = call_env.with_run_id(&run_id);
        // Production default uses an explicit cache boundary. Only the cross-session
        // static prefix is cacheable; workspace, language, project rules, memory,
        // enabled tools and the urgent-summary hint remain in the dynamic suffix.
        let enabled_tools = self
            .tools
            .names()
            .into_iter()
            .filter(|name| conversation_options.allows(name))
            .collect::<BTreeSet<_>>();
        let language = self
            .db
            .get_config_value("user_config")
            .await
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .and_then(|config| {
                config
                    .get("locale")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        let flags = FeatureFlags::from_env();
        let urgent = self
            .summarizer
            .should_inject_summarize_hint(&messages, context_window_for(&effective_model));
        let working_dir_hash = {
            use sha2::Digest as _;
            format!("{:x}", sha2::Sha256::digest(detail.working_dir.as_bytes()))
        };
        let durable_project_context = self
            .db
            .find_project_context(&working_dir_hash)
            .await
            .ok()
            .flatten()
            .and_then(|record| match record.snapshot {
                serde_json::Value::String(text) => Some(text),
                value => serde_json::to_string_pretty(&value).ok(),
            });
        let dynamic = DynamicSectionContext::new(&flags, &detail.working_dir, &enabled_tools)
            .with_language(language.as_deref())
            .with_urgent_summarize(urgent)
            .with_project_loader(&self.project_prompts)
            .with_durable_project_context(durable_project_context.as_deref());
        let mut segmented = build_system_prompt_segmented(&effective_model, &dynamic);
        let append = conversation_options
            .append_system_prompt
            .as_deref()
            .filter(|text| !text.trim().is_empty());
        let supports_thinking = zk_llm::capabilities_for(&effective_model).supports_thinking;
        let thinking = match conversation_options.thinking {
            Some(requested) if requested.requires_support() && !supports_thinking => {
                self.sink
                    .push(
                        session_id,
                        ServerMessage::Notification {
                            key: "thinking_mode_downgraded".into(),
                            level: "warning".into(),
                            message: format!(
                                "Model {effective_model} does not support requested thinking; using disabled mode"
                            ),
                            timeout: 6_000,
                        },
                    )
                    .await;
                ThinkingMode::Disabled
            }
            Some(requested) => requested,
            None if supports_thinking => ThinkingMode::Adaptive,
            None => ThinkingMode::Disabled,
        };
        let mut request = ChatRequest::new(effective_model)
            .with_tools(filtered_tool_specs(&self.tools, &conversation_options))
            .with_max_tokens(max_tokens)
            .with_thinking(thinking);
        if let Some(mut override_prompt) = conversation_options.system_prompt.clone() {
            if let Some(append) = append {
                override_prompt.push_str("\n\n");
                override_prompt.push_str(append);
            }
            request = request.with_system_prompt(Some(override_prompt));
        } else {
            if let Some(append) = append {
                if !segmented.dynamic_suffix.trim().is_empty() {
                    segmented.dynamic_suffix.push_str("\n\n");
                }
                segmented.dynamic_suffix.push_str(append);
            }
            request = request.with_system(SystemPrompt::segmented(
                segmented.static_prefix,
                segmented.dynamic_suffix,
            ));
        }
        request.messages = messages;
        Some(RunSetup {
            run_id,
            replace_after_message_id,
            user_record,
            request,
            call_env,
            conversation_options,
        })
    }

    /// 下行 `error`（code / message / retryable 对照旧 sendError 形状）。
    async fn push_error(&self, session_id: &str, code: &str, message: String, retryable: bool) {
        self.sink
            .push(
                session_id,
                ServerMessage::Error {
                    code: code.to_owned(),
                    message,
                    retryable,
                },
            )
            .await;
    }

    /// 失败兜底完成信号（对照旧 finally：零用量 + stopReason=error，无
    /// runId / committedMessages）。
    async fn push_fallback_complete(&self, session_id: &str) {
        self.sink
            .push(
                session_id,
                ServerMessage::MessageComplete {
                    usage: Usage::default(),
                    stop_reason: Some("error".to_owned()),
                    session_id: None,
                    run_id: None,
                    replace_after_message_id: None,
                    committed_messages: None,
                },
            )
            .await;
    }

    /// 下行 `compact_complete`（对照旧 `sendCompactComplete`）：级联 / 413 恢复
    /// 压缩落地后推送摘要与节省 token 数，供前端观测（`CompactEvent` 建模已激活）。
    async fn push_compact_complete(&self, session_id: &str, summary: &str, tokens_saved: i64) {
        self.sink
            .push(
                session_id,
                ServerMessage::CompactComplete {
                    summary: summary.to_owned(),
                    tokens_saved,
                },
            )
            .await;
    }

    /// 下行 `compact_start`（对照旧 `sendCompactStart`，`WebSocketController` L457）：
    /// 压缩事件序列的起始信号，前端 `messageDispatcher` 据此把 sessionStore 状态
    /// 切到 `compacting`（`compact_complete` 到达时切回 `idle`）。旧
    /// `WsMessageHandler.onCompactEvent` L1153-1157：仅 `auto_compact` / `reactive_compact`
    /// 触发 `sendCompactStart`；本实现在**所有实际 token 释放**的层级前推送
    /// 起始信号，与 `push_compact_complete` 一一配对（Batch 0 Step 0-6）。
    async fn push_compact_start(&self, session_id: &str) {
        self.sink
            .push(session_id, ServerMessage::CompactStart)
            .await;
    }

    /// 下行 `compact_event`（对照旧 record `CompactEvent(phase, usagePercent,
    /// currentTokens)`）：压缩进度事件——`phase` 为触发层级名（`auto_compact` /
    /// `reactive_compact` / `context_collapse` / `context_collapse_drain` 等），
    /// `usage_percent` 为**已释放**的 token 百分比（`(before-after)*100/before`），
    /// `current_tokens` 为压缩后剩余 token 数（`after_tokens`）。Batch 0 Step 0-6。
    async fn push_compact_event(
        &self,
        session_id: &str,
        phase: &str,
        usage_percent: i64,
        current_tokens: i64,
    ) {
        self.sink
            .push(
                session_id,
                ServerMessage::CompactEvent {
                    phase: phase.to_owned(),
                    usage_percent,
                    current_tokens,
                },
            )
            .await;
    }

    /// 下行 `cost_update`（对照旧 `sendCostUpdate`，`WebSocketController` L500-510）：
    /// 每轮 LLM 响应完成后推送本次 usage + 会话累计 + 全局累计费用。
    ///
    /// 会话/全局累计经 [`CostTracker::add_usage`] 无锁累加（`AtomicU64` +
    /// `f64::to_bits`，见 `zk-server::cost::AtomicCostTracker`）；未装配 tracker
    /// 时 [`NoopCostTracker`] 返回 0，与本 Step 接入前一致。Batch 0 Step 0-6。
    async fn push_cost_update(&self, session_id: &str, model: &str, usage: &Usage) {
        let session_cost = self.cost_tracker.add_usage(session_id, model, usage);
        let total_cost = self.cost_tracker.global_cost();
        self.sink
            .push(
                session_id,
                ServerMessage::CostUpdate {
                    session_cost,
                    total_cost,
                    usage: *usage,
                },
            )
            .await;
    }

    /// 下行 `token_budget_nudge`（对照旧 `WsMessageHandler.onTokenBudgetNudge`
    /// L1161-1164 / `QueryEngine` L1315 触发点）：token 预算续写提示。
    ///
    /// **本 Step 仅铺设推送端**——生产者 `TokenBudgetTracker` 在后续 Batch 移植
    /// （Rust 侧尚无预算续写决策链路），该方法暂由单测直接调用验证 wire。
    #[cfg(test)]
    async fn push_token_budget_nudge(
        &self,
        session_id: &str,
        pct: i64,
        current_tokens: i64,
        budget_tokens: i64,
    ) {
        self.sink
            .push(
                session_id,
                ServerMessage::TokenBudgetNudge {
                    pct,
                    current_tokens,
                    budget_tokens,
                },
            )
            .await;
    }

    /// provider 失败序列：`error`（`query_error` + `retryable=true`——旧
    /// catch/onError 分支恒发 true，不按错误类型分类）→ 兜底
    /// `message_complete`。
    async fn push_provider_failure(&self, session_id: &str, error: &ProviderError) {
        self.push_error(session_id, "query_error", error.to_string(), true)
            .await;
        self.push_fallback_complete(session_id).await;
    }
}

/// run 槽位守卫：Drop 时移除注册表条目（含 panic 路径，busy 槽不泄漏）。
struct RunGuard {
    runs: RunMap,
    session_id: String,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        lock_runs(&self.runs).remove(&self.session_id);
    }
}

/// 锁 run 注册表（毒锁降级恢复：内层状态为纯注册表数据，恒可继续）。
fn lock_runs(runs: &RunMap) -> MutexGuard<'_, HashMap<String, RunHandle>> {
    lock_mutex(runs)
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// 执行器事件通道 → Stream（供 `select_all` 合并；通道关闭即流终止）。
fn tool_event_stream(rx: mpsc::UnboundedReceiver<ToolEvent>) -> BoxStream<'static, ToolEvent> {
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|event| (event, rx))
    })
    .boxed()
}

/// 从 [`ProviderError`] 提取 HTTP 状态码与消息，供 413 上下文超限判定
/// （[`is_context_limit_error`]）。非 HTTP 错误状态为 `None`、消息取 Display。
fn provider_error_parts(error: &ProviderError) -> (Option<u16>, String) {
    match error {
        ProviderError::Http {
            status, message, ..
        } => (Some(*status), message.clone()),
        other => (None, other.to_string()),
    }
}

/// flush 工具调用草稿（对照旧 flushToolBlock）：arguments 空 → `{}`；
/// JSON 非法 → `INVALID_TOOL_INPUT_JSON` 致命错误。
fn flush_tool_drafts(drafts: Vec<ToolDraft>) -> Result<Vec<FlushedCall>, String> {
    drafts
        .into_iter()
        .map(|draft| {
            let ToolDraft {
                id,
                name,
                arguments,
            } = draft;
            if arguments.trim().is_empty() {
                return Ok(FlushedCall {
                    id,
                    name,
                    input: json!({}),
                    arguments: "{}".to_owned(),
                });
            }
            match serde_json::from_str::<serde_json::Value>(&arguments) {
                Ok(input) => Ok(FlushedCall {
                    id,
                    name,
                    input,
                    arguments,
                }),
                Err(error) => Err(format!(
                    "INVALID_TOOL_INPUT_JSON: tool call '{id}' ({name}) carries invalid arguments JSON: {error}"
                )),
            }
        })
        .collect()
}

/// 续轮回填载体：flush 结果 → `tool_calls`（arguments 保留原始 JSON 串）。
fn to_tool_call_requests(calls: &[FlushedCall]) -> Vec<ToolCallRequest> {
    calls
        .iter()
        .map(|call| ToolCallRequest {
            id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
        })
        .collect()
}

/// 未知工具错误文案（逐字对照旧 `QueryEngine` 未知工具分支；list =
/// 注册表全量工具名 join(", ")）。
fn unknown_tool_message(name: &str, available: &[String]) -> String {
    let list = available.join(", ");
    format!(
        "<tool_use_error>Error: Tool '{name}' does not exist in this environment. \
         You ONLY have access to these tools: [{list}]. Do NOT attempt to use '{name}' again. \
         Continue solving the problem using ONLY the available tools listed above.</tool_use_error>"
    )
}

/// 结构化元数据过滤（对照旧 structuredResultMetadata）：仅保留
/// `structuredResult` 键且其值须为 JSON 对象，否则整体丢弃。
fn structured_result_metadata(metadata: Option<serde_json::Value>) -> Option<serde_json::Value> {
    let value = metadata?;
    let structured = value.get("structuredResult")?.clone();
    structured.as_object()?;
    Some(json!({ "structuredResult": structured }))
}

fn visualization_message(
    metadata: Option<&serde_json::Value>,
) -> Option<(String, String, serde_json::Value)> {
    let envelope = metadata?.get("visualization")?;
    let uuid = envelope.get("uuid")?.as_str()?.to_owned();
    let view_type = envelope.get("viewType")?.as_str()?.to_owned();
    let props = envelope.get("props")?.clone();
    props.as_object()?;
    Some((uuid, view_type, props))
}

/// 注册表规格 → LLM 请求 tools 参数（zk-tools 与 zk-llm 平级互不依赖，
/// 同构三元组在引擎侧转换）。
fn llm_tool_specs(tools: &ToolRegistry) -> Vec<zk_llm::ToolSpec> {
    tools
        .specs()
        .into_iter()
        .map(|spec| zk_llm::ToolSpec {
            name: spec.name,
            description: spec.description,
            parameters: spec.parameters,
        })
        .collect()
}

fn filtered_tool_specs(
    tools: &ToolRegistry,
    options: &ConversationRunOptions,
) -> Vec<zk_llm::ToolSpec> {
    llm_tool_specs(tools)
        .into_iter()
        .filter(|spec| options.allows(&spec.name))
        .collect()
}

fn filtered_tool_names(tools: &ToolRegistry, options: &ConversationRunOptions) -> Vec<String> {
    tools
        .names()
        .into_iter()
        .filter(|name| options.allows(name))
        .collect()
}

/// Applies a trusted directive emitted only by the production `Skill` tool. The
/// policy can narrow an existing request but can never re-enable a disallowed tool.
/// The rendered skill prompt itself remains a tool result below the immutable system
/// prompt, so skill content cannot replace absolute safety instructions.
fn apply_skill_directive(
    request: &mut ChatRequest,
    options: &mut ConversationRunOptions,
    metadata: Option<&serde_json::Value>,
) {
    let Some(directive) = metadata.and_then(|value| value.get("skillDirective")) else {
        return;
    };
    if let Some(allowed) = directive
        .get("allowedTools")
        .and_then(|value| value.as_array())
        && !allowed.is_empty()
    {
        let requested: HashSet<String> = allowed
            .iter()
            .filter_map(serde_json::Value::as_str)
            .map(str::to_owned)
            .collect();
        options.allowed_tools = Some(match options.allowed_tools.take() {
            Some(existing) => existing.intersection(&requested).cloned().collect(),
            None => requested,
        });
        request.tools.retain(|tool| options.allows(&tool.name));
        request.tool_cache_breakpoint = None;
    }
    if let Some(model) = directive.get("model").and_then(serde_json::Value::as_str)
        && !model.trim().is_empty()
    {
        model.clone_into(&mut request.model);
    }
}

#[cfg(test)]
mod skill_directive_tests {
    use super::*;

    fn spec(name: &str) -> zk_llm::ToolSpec {
        zk_llm::ToolSpec {
            name: name.to_owned(),
            description: name.to_owned(),
            parameters: json!({"type": "object"}),
        }
    }

    #[test]
    fn directive_narrows_existing_tools_and_applies_resolved_model() {
        let mut request =
            ChatRequest::new("parent").with_tools(vec![spec("Read"), spec("Write"), spec("Bash")]);
        let mut options = ConversationRunOptions {
            allowed_tools: Some(HashSet::from(["Read".to_owned(), "Write".to_owned()])),
            ..ConversationRunOptions::default()
        };
        apply_skill_directive(
            &mut request,
            &mut options,
            Some(&json!({
                "skillDirective": {"allowedTools": ["Read", "Bash"], "model": "child"}
            })),
        );
        assert_eq!(
            options.allowed_tools,
            Some(HashSet::from(["Read".to_owned()]))
        );
        assert_eq!(
            request
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["Read"]
        );
        assert_eq!(request.model, "child");
    }

    #[test]
    fn empty_skill_tool_list_does_not_broaden_or_clear_parent_policy() {
        let mut request = ChatRequest::new("parent").with_tools(vec![spec("Read")]);
        let mut options = ConversationRunOptions {
            allowed_tools: Some(HashSet::from(["Read".to_owned()])),
            ..ConversationRunOptions::default()
        };
        apply_skill_directive(
            &mut request,
            &mut options,
            Some(&json!({"skillDirective": {"allowedTools": [], "model": null}})),
        );
        assert_eq!(
            options.allowed_tools,
            Some(HashSet::from(["Read".to_owned()]))
        );
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.model, "parent");
    }

    #[test]
    fn visualization_metadata_maps_to_the_frontend_message_shape() {
        let metadata = json!({
            "visualization": {
                "uuid": "viz-1",
                "viewType": "mermaid",
                "props": {"source": "flowchart TD"}
            }
        });
        let (uuid, view_type, props) =
            visualization_message(Some(&metadata)).expect("visualization");
        assert_eq!(uuid, "viz-1");
        assert_eq!(view_type, "mermaid");
        assert_eq!(props["source"], "flowchart TD");
    }
}

/// Extract explicit checklist/list requirements without asking an LLM to reinterpret the request.
/// A prose-only request is retained as one conservative business criterion.
fn extract_acceptance_criteria(text: &str) -> Vec<String> {
    let mut criteria = text
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let bullet = line
                .strip_prefix("- ")
                .or_else(|| line.strip_prefix("* "))
                .or_else(|| line.strip_prefix("• "))
                .or_else(|| line.strip_prefix("[ ] "));
            let numbered = line
                .char_indices()
                .find(|(_, ch)| matches!(ch, '.' | '、' | ')'))
                .filter(|(index, _)| {
                    *index > 0 && line[..*index].chars().all(|ch| ch.is_ascii_digit())
                })
                .map(|(index, separator)| line[index + separator.len_utf8()..].trim());
            bullet.or(numbered).filter(|value| !value.is_empty())
        })
        .take(20)
        .map(|value| value.chars().take(4_096).collect::<String>())
        .collect::<Vec<_>>();
    if criteria.is_empty() {
        let fallback = text.trim().chars().take(4_096).collect::<String>();
        if !fallback.is_empty() {
            criteria.push(fallback);
        }
    }
    criteria
}

const MAX_REFERENCE_COUNT: usize = 20;
const MAX_REFERENCE_BYTES: u64 = 1024 * 1024;
const MAX_REFERENCE_LINES: i64 = 2_000;
const MAX_ATTACHMENT_BYTES: u64 = 10 * 1024 * 1024;
const MAX_ATTACHMENT_TOTAL_BYTES: u64 = 20 * 1024 * 1024;
const MAX_IMAGE_PIXELS: u64 = 40_000_000;

#[allow(clippy::too_many_lines)] // 引用/url/base64 附件三类准入是同一边界
fn resolve_user_content(
    session: &zk_db::model::SessionDetail,
    effective_model: &str,
    input: UserContentInput,
    trusted_image_url: Option<&TrustedImageUrlCheck>,
) -> Result<(Vec<StoredBlock>, ChatMessage), (&'static str, String)> {
    let workspace = std::fs::canonicalize(&session.working_dir).map_err(|_| {
        (
            "USER_CONTENT_WORKSPACE_UNAVAILABLE",
            "Authorized session workspace is unavailable".to_owned(),
        )
    })?;
    let mut text = input.text;
    let mut stored = vec![StoredBlock::Text { text: text.clone() }];
    if input.references.len() > MAX_REFERENCE_COUNT {
        return Err((
            "REFERENCE_COUNT_EXCEEDED",
            format!("At most {MAX_REFERENCE_COUNT} references are allowed"),
        ));
    }
    for reference in input.references {
        let rendered = resolve_reference(&workspace, &reference)?;
        text.push_str("\n\n");
        text.push_str(&rendered);
        stored.push(StoredBlock::Text { text: rendered });
    }

    // 图片数量和支持性必须按本次请求的有效模型裁定；视觉路由不会修改
    // `session.model`，因此不能继续从持久会话读取能力。
    let capabilities = zk_llm::capabilities_for(effective_model);
    if !input.attachments.is_empty() && !capabilities.supports_images {
        return Err((
            "ATTACHMENT_MODEL_UNSUPPORTED",
            format!("Model {effective_model} does not support image attachments"),
        ));
    }
    let max_images = usize::try_from(capabilities.max_images).unwrap_or(usize::MAX);
    if input.attachments.len() > max_images {
        return Err((
            "ATTACHMENT_COUNT_EXCEEDED",
            format!("Model {effective_model} accepts at most {max_images} images"),
        ));
    }
    let upload_dir = zk_core::paths::user_config_dir().join("uploads");
    let mut images = Vec::with_capacity(input.attachments.len());
    let mut total_bytes = 0_u64;
    for attachment in input.attachments {
        // url 附件：信任校验通过后直存 url 型图片块（旧 WS 入站 url 分支——
        // 校验失败整条消息报错中止；无 base64 载荷，不计体积预算）。
        if attachment.kind == "image"
            && let Some(url) = attachment
                .url
                .as_deref()
                .filter(|url| !url.trim().is_empty())
        {
            if !trusted_image_url.is_some_and(|check| check(url)) {
                tracing::warn!("rejected untrusted clipboard image url attachment");
                return Err((
                    "image_url_untrusted",
                    "图片地址不是当前服务生成的可信 OSS 地址".to_owned(),
                ));
            }
            let media_type = attachment
                .media_type
                .clone()
                .unwrap_or_else(|| "image/png".to_owned());
            stored.push(StoredBlock::Image {
                source: zk_db::model::ImageSource {
                    kind: "url".into(),
                    media_type: None,
                    data: None,
                    url: Some(url.to_owned()),
                },
                width: None,
                height: None,
            });
            images.push(zk_llm::ImageSource {
                media_type,
                data: None,
                url: Some(url.to_owned()),
            });
            continue;
        }
        let image = resolve_uploaded_image(&upload_dir, &attachment)?;
        total_bytes = total_bytes.saturating_add(image.bytes.len() as u64);
        if total_bytes > MAX_ATTACHMENT_TOTAL_BYTES {
            return Err((
                "ATTACHMENT_TOTAL_SIZE_EXCEEDED",
                "Image attachments exceed the 20 MiB total limit".to_owned(),
            ));
        }
        let data = base64::engine::general_purpose::STANDARD.encode(&image.bytes);
        stored.push(StoredBlock::Image {
            source: zk_db::model::ImageSource {
                kind: "base64".into(),
                media_type: Some(image.media_type.clone()),
                data: Some(data.clone()),
                url: None,
            },
            width: Some(i64::from(image.width)),
            height: Some(i64::from(image.height)),
        });
        images.push(zk_llm::ImageSource {
            media_type: image.media_type,
            data: Some(data),
            url: None,
        });
    }
    Ok((stored, ChatMessage::user_with_images(text, images)))
}

fn resolve_reference(
    workspace: &std::path::Path,
    reference: &Reference,
) -> Result<String, (&'static str, String)> {
    if reference.kind != "file" {
        return Err((
            "REFERENCE_TYPE_UNSUPPORTED",
            "Only file references are supported".to_owned(),
        ));
    }
    let requested = std::path::Path::new(&reference.path);
    let lexical = if requested.is_absolute() {
        zk_tools::file_state::normalize_path(requested)
    } else {
        zk_tools::file_state::normalize_path(&workspace.join(requested))
    };
    let canonical = std::fs::canonicalize(&lexical).map_err(|_| {
        (
            "REFERENCE_NOT_FOUND",
            format!("Referenced file was not found: {}", reference.path),
        )
    })?;
    if !canonical.starts_with(workspace) || canonical != lexical {
        return Err((
            "REFERENCE_PATH_FORBIDDEN",
            "Reference must be a regular file inside the authorized workspace".to_owned(),
        ));
    }
    let metadata = std::fs::symlink_metadata(&lexical).map_err(|_| {
        (
            "REFERENCE_NOT_FOUND",
            format!("Referenced file was not found: {}", reference.path),
        )
    })?;
    if !metadata.is_file() || metadata.len() > MAX_REFERENCE_BYTES {
        return Err((
            "REFERENCE_FILE_INVALID",
            "Reference must be a regular UTF-8 file no larger than 1 MiB".to_owned(),
        ));
    }
    let raw = std::fs::read_to_string(&canonical).map_err(|_| {
        (
            "REFERENCE_FILE_INVALID",
            "Reference must contain valid UTF-8 text".to_owned(),
        )
    })?;
    let lines = raw.lines().collect::<Vec<_>>();
    let start = reference.start_line.unwrap_or(1);
    let end = reference
        .end_line
        .unwrap_or_else(|| i64::try_from(lines.len()).unwrap_or(i64::MAX));
    if start < 1 || end < start || end.saturating_sub(start) >= MAX_REFERENCE_LINES {
        return Err((
            "REFERENCE_RANGE_INVALID",
            format!("Reference lines must be a valid range of at most {MAX_REFERENCE_LINES}"),
        ));
    }
    let start_index = usize::try_from(start - 1).unwrap_or(usize::MAX);
    let take = usize::try_from(end - start + 1).unwrap_or(usize::MAX);
    if start_index >= lines.len() && !lines.is_empty() {
        return Err((
            "REFERENCE_RANGE_INVALID",
            "Reference startLine exceeds the file length".to_owned(),
        ));
    }
    let selected = lines
        .iter()
        .skip(start_index)
        .take(take)
        .copied()
        .collect::<Vec<_>>()
        .join("\n");
    let relative = canonical
        .strip_prefix(workspace)
        .unwrap_or(&canonical)
        .to_string_lossy();
    Ok(format!(
        "<reference path=\"{relative}\" lines=\"{start}-{end}\">\n{selected}\n</reference>"
    ))
}

#[derive(Debug)]
struct ResolvedImage {
    media_type: String,
    bytes: Vec<u8>,
    width: u32,
    height: u32,
}

fn resolve_uploaded_image(
    upload_dir: &std::path::Path,
    attachment: &Attachment,
) -> Result<ResolvedImage, (&'static str, String)> {
    if attachment.kind != "image" || attachment.base64_data.is_some() || attachment.url.is_some() {
        return Err((
            "ATTACHMENT_SOURCE_FORBIDDEN",
            "Images must reference a file UUID issued by the upload service".to_owned(),
        ));
    }
    let file_uuid = attachment.path.as_deref().ok_or_else(|| {
        (
            "ATTACHMENT_UUID_REQUIRED",
            "Attachment path must contain an uploaded file UUID".to_owned(),
        )
    })?;
    uuid::Uuid::parse_str(file_uuid).map_err(|_| {
        (
            "ATTACHMENT_UUID_INVALID",
            "Attachment path must be an uploaded file UUID, not a local path".to_owned(),
        )
    })?;
    let entries = std::fs::read_dir(upload_dir).map_err(|_| {
        (
            "ATTACHMENT_NOT_FOUND",
            "Uploaded attachment was not found".to_owned(),
        )
    })?;
    let mut matches = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == file_uuid
            || name
                .strip_prefix(file_uuid)
                .is_some_and(|tail| tail.starts_with('.'))
        {
            matches.push(entry.path());
        }
    }
    if matches.len() != 1 {
        return Err((
            "ATTACHMENT_NOT_FOUND",
            "Uploaded attachment was not found or was ambiguous".to_owned(),
        ));
    }
    let path = &matches[0];
    let metadata = std::fs::symlink_metadata(path).map_err(|_| {
        (
            "ATTACHMENT_NOT_FOUND",
            "Uploaded attachment was not found".to_owned(),
        )
    })?;
    if !metadata.is_file() || metadata.len() > MAX_ATTACHMENT_BYTES {
        return Err((
            "ATTACHMENT_FILE_INVALID",
            "Attachment must be a regular image no larger than 10 MiB".to_owned(),
        ));
    }
    let bytes = std::fs::read(path).map_err(|_| {
        (
            "ATTACHMENT_FILE_INVALID",
            "Uploaded attachment could not be read".to_owned(),
        )
    })?;
    let (media_type, width, height) = image_header(&bytes).ok_or_else(|| {
        (
            "ATTACHMENT_FORMAT_UNSUPPORTED",
            "Attachment magic bytes are not a supported PNG, JPEG, or GIF image".to_owned(),
        )
    })?;
    if attachment
        .media_type
        .as_deref()
        .is_some_and(|declared| declared != media_type)
    {
        return Err((
            "ATTACHMENT_MIME_MISMATCH",
            "Attachment MIME type does not match its magic bytes".to_owned(),
        ));
    }
    if u64::from(width).saturating_mul(u64::from(height)) > MAX_IMAGE_PIXELS {
        return Err((
            "ATTACHMENT_PIXEL_BUDGET_EXCEEDED",
            "Attachment dimensions exceed the 40 megapixel safety limit".to_owned(),
        ));
    }
    Ok(ResolvedImage {
        media_type: media_type.into(),
        bytes,
        width,
        height,
    })
}

fn image_header(bytes: &[u8]) -> Option<(&'static str, u32, u32)> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") && bytes.len() >= 24 {
        return Some((
            "image/png",
            u32::from_be_bytes(bytes[16..20].try_into().ok()?),
            u32::from_be_bytes(bytes[20..24].try_into().ok()?),
        ));
    }
    if (bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) && bytes.len() >= 10 {
        return Some((
            "image/gif",
            u32::from(u16::from_le_bytes(bytes[6..8].try_into().ok()?)),
            u32::from(u16::from_le_bytes(bytes[8..10].try_into().ok()?)),
        ));
    }
    jpeg_dimensions(bytes).map(|(width, height)| ("image/jpeg", width, height))
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if !bytes.starts_with(&[0xff, 0xd8]) {
        return None;
    }
    let mut index = 2;
    while index + 4 <= bytes.len() {
        if bytes[index] != 0xff {
            index += 1;
            continue;
        }
        let marker = bytes[index + 1];
        index += 2;
        if matches!(marker, 0xd8 | 0xd9) {
            continue;
        }
        let length = usize::from(u16::from_be_bytes(
            bytes.get(index..index + 2)?.try_into().ok()?,
        ));
        if length < 2 || index + length > bytes.len() {
            return None;
        }
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) && length >= 7 {
            let height = u32::from(u16::from_be_bytes(
                bytes.get(index + 3..index + 5)?.try_into().ok()?,
            ));
            let width = u32::from(u16::from_be_bytes(
                bytes.get(index + 5..index + 7)?.try_into().ok()?,
            ));
            return Some((width, height));
        }
        index += length;
    }
    None
}

#[cfg(test)]
mod user_content_tests {
    use super::*;

    fn tiny_png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes
    }

    #[test]
    fn uploaded_image_requires_uuid_magic_mime_and_pixel_budget() {
        let directory =
            std::env::temp_dir().join(format!("zk-engine-uploaded-image-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).expect("upload dir");
        let directory = std::fs::canonicalize(directory).expect("canonical");
        let id = uuid::Uuid::new_v4().to_string();
        std::fs::write(directory.join(format!("{id}.png")), tiny_png(1, 1)).expect("image");
        let image = resolve_uploaded_image(
            &directory,
            &Attachment {
                kind: "image".into(),
                path: Some(id),
                media_type: Some("image/png".into()),
                base64_data: None,
                url: None,
            },
        )
        .expect("valid image");
        assert_eq!(image.media_type, "image/png");
        assert_eq!((image.width, image.height), (1, 1));

        let rejected = resolve_uploaded_image(
            &directory,
            &Attachment {
                kind: "image".into(),
                path: Some("/tmp/local.png".into()),
                media_type: Some("image/png".into()),
                base64_data: None,
                url: None,
            },
        )
        .expect_err("local path rejected");
        assert_eq!(rejected.0, "ATTACHMENT_UUID_INVALID");
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn url_attachments_require_trusted_validator_and_pass_through() {
        let workspace =
            std::env::temp_dir().join(format!("zk-engine-url-image-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&workspace).expect("workspace");
        let detail = zk_db::model::SessionDetail {
            session_id: "s-1".into(),
            model: "deepseek-v4-flash-vision-exp".into(),
            working_dir: workspace.to_string_lossy().into_owned(),
            title: None,
            status: "active".into(),
            messages: Vec::new(),
            config: serde_json::Map::new(),
            total_usage: Usage::default(),
            total_cost_usd: 0.0,
            summary: None,
            created_at: 0,
            updated_at: 0,
        };
        let trusted = "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png";
        let input = || UserContentInput {
            text: "inspect".into(),
            attachments: vec![Attachment {
                kind: "image".into(),
                path: None,
                media_type: None,
                base64_data: None,
                url: Some(trusted.into()),
            }],
            references: Vec::new(),
        };
        // 未装配校验端口：一律拒绝（fail-closed，SSRF 红线）。
        let rejected =
            resolve_user_content(&detail, &detail.model, input(), None).expect_err("no validator");
        assert_eq!(rejected.0, "image_url_untrusted");
        // 校验端口拒绝：同错误码。
        let deny: TrustedImageUrlCheck = Arc::new(|_| false);
        let rejected =
            resolve_user_content(&detail, &detail.model, input(), Some(&deny)).expect_err("denied");
        assert_eq!(rejected.0, "image_url_untrusted");
        // 校验通过：url 型存储块 + provider 侧 url 图片，media_type 缺省 image/png。
        let allow: TrustedImageUrlCheck = Arc::new(|_| true);
        let (stored, message) =
            resolve_user_content(&detail, &detail.model, input(), Some(&allow)).expect("trusted");
        assert!(matches!(
            &stored[1],
            StoredBlock::Image { source, .. }
                if source.kind == "url"
                    && source.remote_url() == Some(trusted)
                    && source.media_type.is_none()
                    && source.data.is_none()
        ));
        assert_eq!(message.images[0].url.as_deref(), Some(trusted));
        assert_eq!(message.images[0].media_type, "image/png");
        assert_eq!(message.images[0].data, None);
        std::fs::remove_dir_all(workspace).expect("cleanup");
    }

    #[test]
    fn references_are_workspace_scoped_and_line_bounded() {
        let workspace =
            std::env::temp_dir().join(format!("zk-engine-reference-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&workspace).expect("workspace");
        let workspace = std::fs::canonicalize(workspace).expect("canonical");
        std::fs::write(workspace.join("notes.txt"), "one\ntwo\nthree\n").expect("notes");
        let rendered = resolve_reference(
            &workspace,
            &Reference {
                kind: "file".into(),
                path: "notes.txt".into(),
                start_line: Some(2),
                end_line: Some(3),
            },
        )
        .expect("reference");
        assert!(rendered.contains("path=\"notes.txt\" lines=\"2-3\""));
        assert!(rendered.contains("two\nthree"));
        let outside = resolve_reference(
            &workspace,
            &Reference {
                kind: "file".into(),
                path: "/etc/hosts".into(),
                start_line: Some(1),
                end_line: Some(1),
            },
        )
        .expect_err("outside rejected");
        assert_eq!(outside.0, "REFERENCE_PATH_FORBIDDEN");
        std::fs::remove_dir_all(workspace).expect("cleanup");
    }
}

/// 跨轮 usage 累计（`message_complete` 报告跨轮总量，对照旧实现）。
fn add_usage(total: &mut Usage, delta: &Usage) {
    total.input_tokens += delta.input_tokens;
    total.output_tokens += delta.output_tokens;
    total.cache_read_input_tokens += delta.cache_read_input_tokens;
    total.cache_creation_input_tokens += delta.cache_creation_input_tokens;
}

/// 落库历史 → LLM 请求消息回放。
///
/// 对外可见（Batch 3）：`/compact` 斜杠命令要按**与真实请求完全相同**的
/// 回放规则估算压缩前 token，若命令侧自建一套转换就会与引擎分叉。
///
/// 对照旧 buildMessages：assistant 的 `tool_use` 块 → `tool_calls`
/// （arguments 序列化回 JSON 串）；user 消息含 `tool_result` 块 → 每块
/// 一条 `{role:"tool", tool_call_id, content}`；其余按拼接文本回放；
/// image 块恢复为 provider-neutral image source；thinking 恢复为独立字段，
/// 由各 Provider 决定是否能安全回传（例如需要签名的协议可忽略）。
#[must_use]
pub fn history_to_chat_messages(records: &[MessageRecord]) -> Vec<ChatMessage> {
    let mut messages = Vec::new();
    // 三层保护第三层（对照 Java `QueryEngine.java:1724-1746` 兜底扫描）：
    // 记录当前尚未匹配到 `tool_result` 的 `tool_use_id`；切换到非 tool_result
    // 消息前先合成 [`ORPHAN_TOOL_RESULT`]，防止历史中孤儿 tool_use 令 provider
    // 400 永久污染会话。顺序保持稳定（`Vec` 首次命中位置移除，未匹配按插入序
    // 合成）。
    let mut pending_tool_use_ids: Vec<String> = Vec::new();
    for record in records {
        match record.role {
            MessageRole::Assistant => {
                // 进入新 assistant 前，先补齐上一轮遗留的 tool_use（避免连续
                // 两条 assistant 之间被夹进 provider 请求时触发 400）。
                flush_orphan_tool_results(&mut messages, &mut pending_tool_use_ids);
                let tool_calls: Vec<ToolCallRequest> = record
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        StoredBlock::ToolUse { id, name, input } => Some(ToolCallRequest {
                            id: id.clone(),
                            name: name.clone(),
                            arguments: input.to_string(),
                        }),
                        _ => None,
                    })
                    .collect();
                let text = concat_text(&record.content);
                let thinking = record
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        StoredBlock::Thinking { thinking } => Some(thinking.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if tool_calls.is_empty() {
                    // 空 / 全空白正文且无 tool_calls 的 assistant **整条丢弃**
                    // （对照旧 `MessageNormalizer` Phase 3 与
                    // `filterEmptyAssistantMessages`：content 为空或全为空白
                    // 文本块的 assistant 不进请求）。既覆盖 thinking-only 轮，
                    // 也让历史中已落库的空正文助手消息不再触发 provider 400，
                    // 使被毒化的旧会话恢复可用。
                    if !text.trim().is_empty() {
                        messages.push(ChatMessage::assistant(text).with_thinking(Some(thinking)));
                    }
                } else {
                    for call in &tool_calls {
                        pending_tool_use_ids.push(call.id.clone());
                    }
                    messages.push(
                        ChatMessage::assistant_tool_calls(text, tool_calls)
                            .with_thinking(Some(thinking)),
                    );
                }
            }
            MessageRole::User => {
                let mut has_tool_result = false;
                for block in &record.content {
                    if let StoredBlock::ToolResult {
                        tool_use_id,
                        content,
                        ..
                    } = block
                    {
                        has_tool_result = true;
                        // 命中即从 pending 中移除；未在 pending 中的 id 也照
                        // 常回放（历史真实存在的 tool_result 优先于合成兜底）。
                        if let Some(idx) =
                            pending_tool_use_ids.iter().position(|id| id == tool_use_id)
                        {
                            pending_tool_use_ids.remove(idx);
                        }
                        messages.push(ChatMessage::tool(tool_use_id.clone(), content.clone()));
                    }
                }
                if !has_tool_result {
                    // 非 tool_result 的 user 消息：先补齐剩余 pending 再入队，
                    // 保证「assistant(tool_calls) → tool(id, result)... → user(text)」
                    // 顺序对 provider 合法。
                    flush_orphan_tool_results(&mut messages, &mut pending_tool_use_ids);
                    let images = record
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            StoredBlock::Image { source, .. } => Some(zk_llm::ImageSource {
                                media_type: source.media_type_or_default().to_owned(),
                                data: source.data.clone(),
                                url: source.remote_url().map(str::to_owned),
                            }),
                            _ => None,
                        })
                        .collect();
                    messages.push(ChatMessage::user_with_images(
                        concat_text(&record.content),
                        images,
                    ));
                }
            }
            // 系统消息不回放（Phase 1/2.2 无系统角色落库路径）。
            MessageRole::System => {}
        }
    }
    // 历史末尾仍有 pending：整批合成，兜底至最后一步。
    flush_orphan_tool_results(&mut messages, &mut pending_tool_use_ids);
    messages
}

/// 为所有未匹配到 `tool_result` 的 `tool_use_id` 合成 [`ORPHAN_TOOL_RESULT`]
/// 并按插入序追加到回放序列（对照 Java `QueryEngine.java:1724-1746`）。
fn flush_orphan_tool_results(messages: &mut Vec<ChatMessage>, pending: &mut Vec<String>) {
    for tool_use_id in pending.drain(..) {
        messages.push(ChatMessage::tool(
            tool_use_id,
            ORPHAN_TOOL_RESULT.to_owned(),
        ));
    }
}

/// 拼接消息的全部文本块。
fn concat_text(blocks: &[StoredBlock]) -> String {
    let mut text = String::new();
    for block in blocks {
        if let StoredBlock::Text { text: chunk } = block {
            text.push_str(chunk);
        }
    }
    text
}

#[cfg(test)]
mod history_orphan_tests {
    //! 历史加载时孤儿 `tool_use` 兜底合成的回归护栏（对照 Java
    //! `QueryEngine.java:1724-1746` 三层保护第三层）。
    //!
    //! 缺陷背景：进程崩溃 / DB 写工具结果失败 / 收尾未跑等边缘路径会令 DB 中
    //! 存在 `assistant.tool_use` 但缺失对应的 `user.tool_result` 后随；回放到
    //! provider 会触发 400（`must be followed by tool messages`）令会话永久
    //! 损坏。本模块钉住 `history_to_chat_messages` 在此类历史下的合成语义。

    use super::{ORPHAN_TOOL_RESULT, history_to_chat_messages};
    use serde_json::json;
    use zk_db::model::{MessageRecord, MessageRole, StoredBlock};
    use zk_llm::Role;

    fn assistant_tool_use(seq: i64, id: &str) -> MessageRecord {
        MessageRecord {
            id: format!("assistant-{seq}"),
            session_id: "s".to_owned(),
            role: MessageRole::Assistant,
            content: vec![
                StoredBlock::Text {
                    text: "picking a tool".to_owned(),
                },
                StoredBlock::ToolUse {
                    id: id.to_owned(),
                    name: "Write".to_owned(),
                    input: json!({"path":"a.txt"}),
                },
            ],
            stop_reason: Some("tool_use".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            seq_num: seq,
            created_at: seq,
        }
    }

    fn user_text(seq: i64, text: &str) -> MessageRecord {
        MessageRecord {
            id: format!("user-{seq}"),
            session_id: "s".to_owned(),
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: text.to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            seq_num: seq,
            created_at: seq,
        }
    }

    fn user_tool_result(seq: i64, id: &str, content: &str) -> MessageRecord {
        MessageRecord {
            id: format!("tr-{seq}"),
            session_id: "s".to_owned(),
            role: MessageRole::User,
            content: vec![StoredBlock::ToolResult {
                tool_use_id: id.to_owned(),
                content: content.to_owned(),
                is_error: false,
                metadata: None,
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            seq_num: seq,
            created_at: seq,
        }
    }

    /// 孤儿 `tool_use` 后紧跟 user 文本：兜底合成夹入 assistant 与 user 之间。
    #[test]
    fn orphan_tool_use_before_user_text_gets_synthesized() {
        let records = vec![
            user_text(1, "帮我做一个介绍大模型的网页"),
            assistant_tool_use(2, "Write_0"),
            user_text(3, "帮我做一个介绍大模型的网页"),
        ];
        let msgs = history_to_chat_messages(&records);
        // 期望顺序：user → assistant(tool_calls) → tool(orphan) → user
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0].role, Role::User);
        assert_eq!(msgs[1].role, Role::Assistant);
        assert_eq!(msgs[1].tool_calls.len(), 1);
        assert_eq!(msgs[1].tool_calls[0].id, "Write_0");
        assert_eq!(msgs[2].role, Role::Tool);
        assert_eq!(msgs[2].tool_call_id.as_deref(), Some("Write_0"));
        assert_eq!(msgs[2].content, ORPHAN_TOOL_RESULT);
        assert_eq!(msgs[3].role, Role::User);
    }

    /// 历史末尾即孤儿：兜底合成落在末尾（防止下一轮拼上新 user 后依然孤儿）。
    #[test]
    fn orphan_tool_use_at_history_tail_gets_synthesized() {
        let records = vec![user_text(1, "hi"), assistant_tool_use(2, "Write_0")];
        let msgs = history_to_chat_messages(&records);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2].role, Role::Tool);
        assert_eq!(msgs[2].tool_call_id.as_deref(), Some("Write_0"));
        assert_eq!(msgs[2].content, ORPHAN_TOOL_RESULT);
    }

    /// 连续两条 assistant 的病态历史：第一条的孤儿在第二条之前被兜底。
    #[test]
    fn orphan_before_next_assistant_gets_synthesized() {
        let records = vec![
            user_text(1, "hi"),
            assistant_tool_use(2, "Write_0"),
            assistant_tool_use(3, "Write_1"),
        ];
        let msgs = history_to_chat_messages(&records);
        // user → assistant#1 → tool(orphan Write_0) → assistant#2 → tool(orphan Write_1)
        assert_eq!(msgs.len(), 5);
        assert_eq!(msgs[1].role, Role::Assistant);
        assert_eq!(msgs[2].role, Role::Tool);
        assert_eq!(msgs[2].tool_call_id.as_deref(), Some("Write_0"));
        assert_eq!(msgs[3].role, Role::Assistant);
        assert_eq!(msgs[4].role, Role::Tool);
        assert_eq!(msgs[4].tool_call_id.as_deref(), Some("Write_1"));
    }

    /// 健康历史（每个 `tool_use` 都有匹配 `tool_result`）：不合成任何兜底。
    #[test]
    fn well_formed_history_yields_no_orphan_synthesis() {
        let records = vec![
            user_text(1, "hi"),
            assistant_tool_use(2, "Write_0"),
            user_tool_result(3, "Write_0", "ok"),
        ];
        let msgs = history_to_chat_messages(&records);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2].role, Role::Tool);
        assert_eq!(msgs[2].tool_call_id.as_deref(), Some("Write_0"));
        assert_eq!(msgs[2].content, "ok");
    }

    /// 多个 `tool_use`，仅一个孤儿：只为孤儿合成，其他真实结果保留。
    #[test]
    fn partial_orphans_only_synthesize_missing_ids() {
        let records = vec![
            user_text(1, "hi"),
            MessageRecord {
                id: "a".to_owned(),
                session_id: "s".to_owned(),
                role: MessageRole::Assistant,
                content: vec![
                    StoredBlock::ToolUse {
                        id: "Write_0".to_owned(),
                        name: "Write".to_owned(),
                        input: json!({}),
                    },
                    StoredBlock::ToolUse {
                        id: "Write_1".to_owned(),
                        name: "Write".to_owned(),
                        input: json!({}),
                    },
                ],
                stop_reason: Some("tool_use".to_owned()),
                input_tokens: 0,
                output_tokens: 0,
                seq_num: 2,
                created_at: 2,
            },
            user_tool_result(3, "Write_0", "ok"),
            user_text(4, "继续"),
        ];
        let msgs = history_to_chat_messages(&records);
        // user → assistant(2 tool_calls) → tool(Write_0="ok") → tool(orphan Write_1) → user
        assert_eq!(msgs.len(), 5);
        assert_eq!(msgs[2].role, Role::Tool);
        assert_eq!(msgs[2].tool_call_id.as_deref(), Some("Write_0"));
        assert_eq!(msgs[2].content, "ok");
        assert_eq!(msgs[3].role, Role::Tool);
        assert_eq!(msgs[3].tool_call_id.as_deref(), Some("Write_1"));
        assert_eq!(msgs[3].content, ORPHAN_TOOL_RESULT);
        assert_eq!(msgs[4].role, Role::User);
    }

    #[test]
    fn durable_images_and_thinking_are_reconstructed_for_provider_history() {
        let records = vec![
            MessageRecord {
                content: vec![
                    StoredBlock::Text {
                        text: "inspect".into(),
                    },
                    StoredBlock::Image {
                        source: zk_db::model::ImageSource {
                            kind: "base64".into(),
                            media_type: Some("image/png".into()),
                            data: Some("aGVsbG8=".into()),
                            url: None,
                        },
                        width: Some(1),
                        height: Some(1),
                    },
                ],
                ..user_text(1, "unused")
            },
            MessageRecord {
                id: "assistant-thinking".into(),
                session_id: "s".into(),
                role: MessageRole::Assistant,
                content: vec![
                    StoredBlock::Thinking {
                        thinking: "durable reasoning".into(),
                    },
                    StoredBlock::Text {
                        text: "answer".into(),
                    },
                ],
                stop_reason: Some("end_turn".into()),
                input_tokens: 0,
                output_tokens: 0,
                seq_num: 2,
                created_at: 2,
            },
        ];
        let messages = history_to_chat_messages(&records);
        assert_eq!(messages[0].images.len(), 1);
        assert_eq!(messages[0].images[0].media_type, "image/png");
        assert_eq!(messages[1].thinking.as_deref(), Some("durable reasoning"));
        assert_eq!(messages[1].content, "answer");
    }
}

#[cfg(test)]
mod telemetry_push_tests {
    //! Batch 0 Step 0-6：验证四个 push 辅助的 wire 语义——每个方法把入参
    //! 逐字段映射到对应 `ServerMessage` variant，然后经 `MessageSink::push`
    //! 下行；`push_cost_update` 额外与 [`CostTracker`] 联动（`session_cost` /
    //! `total_cost` 直接反映 tracker 的累加结果）。
    //!
    //! 使用最小 `Engine` 装配（in-memory `Db` + panic-on-call 的 `ChatProvider`
    //! 桩，因为 push 辅助不触发 provider）+ 一个直接构造的 [`Engine`]（复用
    //! [`NoopCostTracker`] 或本地 `StubCostTracker`）+ 录制 sink。
    #![allow(
        clippy::float_cmp,
        reason = "费用为固定字面量常数，测试用 == 与 abs<EPS 等价，且更直观"
    )]

    use std::sync::Mutex;

    use futures::future::BoxFuture;
    use futures::stream::{self, BoxStream};
    use tokio_util::sync::CancellationToken;
    use zk_db::Db;
    use zk_llm::{ChatProvider, ChatRequest, ProviderError, ProviderEvent};
    use zk_protocol::ServerMessage;
    use zk_protocol::model::Usage;

    use super::{Arc, CostTracker, Engine, MessageSink, NoopCostTracker};

    /// 录制型 sink：按推送序保存 (`session_id`, message)，供断言消费。
    #[derive(Default)]
    struct RecordingSink {
        pushed: Mutex<Vec<(String, ServerMessage)>>,
    }

    impl MessageSink for RecordingSink {
        fn push<'a>(&'a self, session_id: &'a str, message: ServerMessage) -> BoxFuture<'a, ()> {
            self.pushed
                .lock()
                .expect("sink lock")
                .push((session_id.to_owned(), message));
            Box::pin(futures::future::ready(()))
        }
    }

    impl RecordingSink {
        fn take(&self) -> Vec<(String, ServerMessage)> {
            std::mem::take(&mut self.pushed.lock().expect("sink lock"))
        }
    }

    /// panic-on-call 的 provider 桩——push 辅助不会触发 `chat_stream`，
    /// 若测试意外走到 provider 路径应直接暴露。
    struct PanicProvider;

    impl ChatProvider for PanicProvider {
        fn provider_name(&self) -> &'static str {
            "panic"
        }

        fn chat_stream(
            &self,
            _request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            // 若走到此说明测试拓扑错误——返回空流也可，但 panic 更醒目。
            Ok(Box::pin(stream::empty()))
        }
    }

    /// 可编程 tracker：`add_usage` 恒返回构造时给定的 `session_cost`，
    /// `global_cost` 恒返回构造时给定的 `total_cost`；用于把 `push_cost_update`
    /// 的下行字段与 tracker 输出严格解耦断言。
    struct StubCostTracker {
        session_cost: f64,
        total_cost: f64,
    }

    impl CostTracker for StubCostTracker {
        fn add_usage(&self, _session_id: &str, _model: &str, _usage: &Usage) -> f64 {
            self.session_cost
        }

        fn session_cost(&self, _session_id: &str) -> f64 {
            self.session_cost
        }

        fn global_cost(&self) -> f64 {
            self.total_cost
        }

        fn last_model_cost(&self, _model: &str) -> f64 {
            0.0
        }

        fn reset(&self, _session_id: &str) {}
    }

    /// 装配最小 `Engine`（缺省 `NoopCostTracker`，测试内按需替换）。
    fn build_engine(cost_tracker: Arc<dyn CostTracker>) -> (Arc<Engine>, Arc<RecordingSink>) {
        let db = Db::open_in_memory().expect("in-memory db");
        let sink = Arc::new(RecordingSink::default());
        let engine = Arc::new(
            Engine::new(
                db,
                Arc::new(PanicProvider) as Arc<dyn ChatProvider>,
                Arc::clone(&sink) as Arc<dyn MessageSink>,
            )
            .with_cost_tracker(cost_tracker),
        );
        (engine, sink)
    }

    /// `push_compact_start` → `ServerMessage::CompactStart`（无 payload）。
    #[tokio::test]
    async fn push_compact_start_emits_bare_variant() {
        let (engine, sink) = build_engine(Arc::new(NoopCostTracker));
        engine.push_compact_start("sess-1").await;
        let pushed = sink.take();
        assert_eq!(pushed.len(), 1);
        assert_eq!(pushed[0].0, "sess-1");
        assert!(matches!(pushed[0].1, ServerMessage::CompactStart));
    }

    /// `push_compact_event` → phase / `usage_percent` / `current_tokens` 全字段直传。
    #[tokio::test]
    async fn push_compact_event_forwards_fields_verbatim() {
        let (engine, sink) = build_engine(Arc::new(NoopCostTracker));
        engine
            .push_compact_event("sess-1", "auto_compact", 42, 3_000)
            .await;
        let pushed = sink.take();
        assert_eq!(pushed.len(), 1);
        match &pushed[0].1 {
            ServerMessage::CompactEvent {
                phase,
                usage_percent,
                current_tokens,
            } => {
                assert_eq!(phase, "auto_compact");
                assert_eq!(*usage_percent, 42);
                assert_eq!(*current_tokens, 3_000);
            }
            other => panic!("expected CompactEvent, got {other:?}"),
        }
    }

    /// `push_cost_update` → 从 tracker 取 `session_cost` / `total_cost` 并透传 usage。
    #[tokio::test]
    async fn push_cost_update_projects_tracker_and_usage() {
        let tracker = Arc::new(StubCostTracker {
            session_cost: 1.25,
            total_cost: 9.75,
        });
        let (engine, sink) = build_engine(tracker);
        let usage = Usage {
            input_tokens: 100,
            output_tokens: 200,
            cache_read_input_tokens: 50,
            cache_creation_input_tokens: 0,
        };
        engine.push_cost_update("sess-1", "kimi-k3", &usage).await;
        let pushed = sink.take();
        assert_eq!(pushed.len(), 1);
        match &pushed[0].1 {
            ServerMessage::CostUpdate {
                session_cost,
                total_cost,
                usage: reported,
            } => {
                assert_eq!(*session_cost, 1.25);
                assert_eq!(*total_cost, 9.75);
                assert_eq!(reported.input_tokens, 100);
                assert_eq!(reported.output_tokens, 200);
                assert_eq!(reported.cache_read_input_tokens, 50);
                assert_eq!(reported.cache_creation_input_tokens, 0);
            }
            other => panic!("expected CostUpdate, got {other:?}"),
        }
    }

    /// `push_token_budget_nudge` → pct / `current_tokens` / `budget_tokens` 全字段直传。
    /// 该方法目前无生产者（`TokenBudgetTracker` 在后续 Batch 移植），本单测
    /// 兼作 wire 冒烟 + 消除 `dead_code` 警告。
    #[tokio::test]
    async fn push_token_budget_nudge_forwards_fields_verbatim() {
        let (engine, sink) = build_engine(Arc::new(NoopCostTracker));
        engine
            .push_token_budget_nudge("sess-1", 85, 40_000, 47_000)
            .await;
        let pushed = sink.take();
        assert_eq!(pushed.len(), 1);
        match &pushed[0].1 {
            ServerMessage::TokenBudgetNudge {
                pct,
                current_tokens,
                budget_tokens,
            } => {
                assert_eq!(*pct, 85);
                assert_eq!(*current_tokens, 40_000);
                assert_eq!(*budget_tokens, 47_000);
            }
            other => panic!("expected TokenBudgetNudge, got {other:?}"),
        }
    }
}
