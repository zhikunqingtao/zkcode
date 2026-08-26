//! zk-engine——对话引擎（Phase 1 单轮闭环 + Phase 2.2 有限多轮工具循环）。
//!
//! 职责：接收 chat 上行（`user_message` / `interrupt`）→ 校验会话与并发
//! 防护 → 用户消息落库 → 构建 [`zk_llm::ChatRequest`]（会话历史回放 +
//! tools 下发）→ 流式消费 [`zk_llm::ProviderEvent`] → 经 [`MessageSink`]
//! 下行推送 `stream_delta` / `thinking_delta` / `tool_use_start` /
//! `tool_use_input` / `tool_use_progress` / `tool_result` /
//! `message_complete` / `error` / `interrupt_ack` → 助手与工具结果落库 →
//! （模型请求工具时）经 [`zk_tools::ToolRegistry`] 执行并回填续轮，直至
//! `end_turn` 或 `max_turns` 上界。行为规格逐条对照旧
//! `WebSocketController` / `QueryEngine.java` 的可观察序列（消息类型 /
//! 落库时点 / 字段），实现按 Rust 组织。
//!
//! # 依赖方向（铁律）
//!
//! `zk-engine → {zk-protocol, zk-llm, zk-tools, zk-db}`，**不依赖
//! zk-server**。下行推送经 [`MessageSink`] 窄接口解耦：zk-server 侧为
//! `WsHub` 实现该 trait 并在组装根装配（server → engine 方向合法）；上行
//! 经 zk-server 的 `EngineHook` 适配层桥接到
//! [`Engine::handle_client_message`]。
//!
//! # 2.5 接入点
//!
//! 权限管线经 [`admission::ToolAdmission`] 窄端口反转注入：工具阶段在真正
//! 派发执行器之前逐个调用准入，拒绝即不执行并回喂 `permissionDenied` 结果
//! （对照旧 `ToolExecutionPipeline` 阶段 4/5 与其 `AuthorizationException`
//! 捕获路径）。未装配时为 [`admission::AllowAllAdmission`] 直通。
//!
//! # 2.2 边界（后续子阶段待办）
//!
//! 真实工具族（2.3）、系统提示注入（Phase 3 引擎域；已落地
//! [`env_info`] 环境信息段——模型需据此知晓主工作目录，缺失会导致其按
//! 容器习惯发出 `/mnt/user-data/...` 绝对路径——与 [`system_prompt`]
//! Batch 0 的 6 个 P0 静态段，其余段属后续批次）、token 预算 / compact
//! （M8）；附件/引用仅接收不消费
//! （`ChatMessage` 纯文本，D-S9-7）。

pub mod admission;
pub mod concurrency;
pub mod context;
pub mod conversation_service;
// Batch 7b Step 4/5：自修正循环（编译错误/测试失败解析器 + 主控）。消费方为
// 引擎多轮循环，Feature Flag `SELF_CORRECTION_LOOP` 门控。
pub mod correction;
pub mod cost;
pub mod engine;
pub mod env_info;
// Batch 5 Step 5：文件写前快照 + 回合事务 + Rewind（对照旧
// `FileHistoryService`）。消费方为引擎多轮循环的事务边界与
// `/api/sessions/{id}/history/*` 端点。
pub mod file_history;
// Batch 5 Step 2：用户级长期记忆（`~/.zk/MEMORY.md` + BM25 检索），对照旧
// `MemdirService` / `MemorySearchEngine`。生产入口为 `Memory` 工具与
// `/api/memory*` 端点；**不**参与系统提示注入（理由见模块文档）。
pub mod llm_summarizer;
pub mod memdir;
pub mod normalize;
pub mod observability;
// Batch 5 Step 3：项目级记忆（项目根 `zhikun.md` / `zhikun.local.md`），对照旧
// `ProjectMemoryService`。系统提示 `memory` 段的唯一数据源。
pub mod project_memory;
pub mod query_config;
pub mod recovery;
pub mod session_snapshot;
pub mod sink;
pub mod summarizer;
pub mod system_prompt;
// Batch 7b Step 1：工具调用追踪器（连续错误 / 滑动窗口 / 相同错误重复）。
pub mod tool_tracker;
// Batch 6: 子代理核心引擎 + 任务系统。
pub mod agent;
pub mod task;
// Batch 7b Step 2：Agent Loop 终止策略（每轮末尾评估继续/终止/请求用户输入）。
pub mod termination;
// Batch 0 P0-16：项目提示六层加载 + 5s 轮询热重载（对照旧
// `ProjectPromptLoader` / `SettingsWatcher`）。产出内容供后续 Batch 1
// 系统提示动态段消费；本批不改 `system_prompt` 组装逻辑。
pub mod prompt;
// Batch 6 Phase C：协调器/编排器层（CoordinatorWorkflow + CoordinatorService + TeamManager + Swarm）。
pub mod coordinator;
// Batch 8B：Hook 系统（事件驱动外部副作用通知；本地命令 / HTTP POST + SSRF 防护）。
pub mod hook;
// Batch 8G：Plugin 系统（manifest.toml 扫描 + DashMap 管理器）。
pub mod plugin;

pub use admission::{
    Admission, AdmissionRequest, AllowAllAdmission, ModeSwitcher, ToolAdmission, allow_all,
};
pub use concurrency::{
    AgentConcurrencyController, AgentLimitExceededError, AgentSlot, CONCURRENCY_GATE_ENABLED_ENV,
    MAX_AGENT_NESTING_DEPTH, MAX_CONCURRENT_AGENTS, MAX_CONCURRENT_AGENTS_PER_SESSION,
    concurrency_gate_enabled,
};
pub use context::{
    AutoCompactTrackingState, CASCADE_ENABLED_ENV, CLEARED_MESSAGE, CascadeLevel, CascadeResult,
    ContextCascade, IncrementalCollapseManager, MICRO_COMPACT_PROTECTED_TAIL, TokenWarningState,
    calculate_token_warning_state, cascade_enabled, estimate_tokens,
};
pub use conversation_service::{ConversationOutcome, ConversationService, ConversationToolCall};
pub use correction::loop_ctrl::{
    CorrectionInstruction, CorrectionType, ErrorContext, MAX_ATTEMPTS_DEFAULT,
    MAX_ATTEMPTS_SWE_BENCH, MAX_INSTRUCTION_TOKENS, MAX_STACK_TRACE_LINES,
    detect_and_prepare_correction, should_abort,
};
pub use correction::{ParsedError, ParsedTestFailure};
pub use cost::{CostTracker, NoopCostTracker};
pub use engine::{ConversationRunOptions, Engine, TrustedImageUrlCheck};
pub use env_info::environment_section;
pub use file_history::{
    DiffStats, FileHistoryService, RewindResult, SnapshotInfo, TransactionRecord, TurnSnapshots,
    resolve_prospective,
};
pub use llm_summarizer::{
    LlmSummarizer, MAX_SUMMARY_INPUT_CHARS, SUMMARY_TIMEOUT, SummarizerMetrics,
};
pub use memdir::{
    DocumentEntry, ENTRYPOINT_NAME, MAX_ENTRYPOINT_BYTES, MAX_ENTRYPOINT_LINES,
    MAX_MEMORY_AGE_DAYS, MAX_MEMORY_SIZE, MemdirError, MemdirStore, Memory, MemoryCategory,
    MemoryEntry, MemorySource, ScoredResult, compact_memories, extract_title, parse_entries,
    remove_matching_entries, search_bm25, truncate_for_prompt,
};
pub use project_memory::{
    MEMORY_FILES, PROJECT_MEMORY_FILE, PROJECT_MEMORY_LOCAL_FILE, has_memory, load_memory,
    memory_prompt_section, write_memory,
};
pub use prompt::{
    CACHE_TTL, MAX_INCLUDE_BYTES, MAX_PARENT_DEPTH, PROJECT_LOCAL_MD_FILE, PROJECT_MD_FILE,
    ProjectMdSection, ProjectPromptLoader, ProjectPromptWatcher, RULES_DIR_NAME,
    WATCH_POLL_INTERVAL,
};
pub use query_config::{
    DEFAULT_MAX_TOKENS, ESCALATED_MAX_TOKENS, MAX_OUTPUT_TOKENS_RECOVERY_LIMIT,
    MAX_TOKENS_RECOVERY_MESSAGE, recommended_max_tokens, usage_cost_usd,
};
pub use recovery::{
    ContextRecovery, ModelTierService, RecoveryOutcome, RecoveryPhase, RecoveryState,
    is_context_limit_error, is_media_related_error, strip_largest_block, strip_media_payloads,
};
pub use session_snapshot::{
    InvalidSessionId, SNAPSHOT_DIR_NAME, SNAPSHOT_FILE_SUFFIX, SessionSnapshot,
    SessionSnapshotService, SessionSnapshotSummary,
};
pub use sink::MessageSink;
pub use summarizer::{
    HARD_LIMIT_CHARS, LightModelSummarizer, SOFT_LIMIT_CHARS, STALE_TURN_THRESHOLD,
    SUMMARIZE_TOOL_RESULTS_SECTION, SUMMARIZER_ENABLED_ENV, ToolResultSummarizer,
    URGENT_SUMMARIZE_HINT, summarizer_enabled,
};
pub use system_prompt::{
    ACTIONS_SECTION, DOING_TASKS_SECTION, INTRO_SECTION, SYSTEM_SECTION, USING_TOOLS_BASE,
    build_system_prompt, scratchpad_section,
};
pub use termination::{
    CONSECUTIVE_ERROR_THRESHOLD, LoopContext, SLIDING_WINDOW_SIZE, TerminationDecision, evaluate,
};
pub use tool_tracker::{ToolCallRecord, ToolCallTracker};

// Batch 6 re-exports
pub use agent::{
    AgentDefinition, AgentMailboxMessage, AgentMailboxRouter, AgentRequest, AgentResult,
    AgentStatus, AgentTimeoutConfig, BackgroundAgentTracker, ChildExecutionContext,
    GitCommandOutput, GitCommandRunner, IsolationMode, MAX_RESULT_SIZE_CHARS,
    RealSubAgentEngineFactory, SUB_AGENT_TOOL_NAMES, SubAgentEngineFactory, SubAgentExecutor,
    SystemGitCommandRunner, WorktreeManager, build_sub_agent_registry,
    build_sub_agent_registry_with_policy,
};
pub use task::{TaskCoordinator, TaskState, TaskStatus};

// Batch 6 Phase C re-exports
pub use coordinator::{
    CoordinatorEvent, CoordinatorEventBus, CoordinatorService, CoordinatorWorkflow,
    CoordinatorWorkflowEngine, InProcessBackend, PhaseRecord, SwarmPhase, SwarmResults,
    SwarmService, TeamInfo, TeamManager, ValidationResult, ValidationSeverity, WorkerState,
    WorkerStatus, WorkflowPhase, WorkflowStatus,
};

// Batch 8B re-exports（Hook 系统：组合根经 `with_hooks` 注入引擎，AppState 装配）。
pub use hook::{
    HookConfig, HookContext, HookEvent, HookRegistry, HookRole, HookService, PreHookDecision,
};
pub use observability::{
    BoundedObservabilityRecorder, JsonlObservabilitySink, NoopObservabilityRecorder,
    ObservabilityEvent, ObservabilityHealth, ObservabilityRecorder, ObservabilitySink,
};

// Batch 8G re-exports（Plugin 系统：PluginManager 由 AppState 持有 Arc）。
pub use plugin::manager::PluginManager;
pub use plugin::{Plugin, PluginInfo, PluginManifest};
