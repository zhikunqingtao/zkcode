//! zk-tools——工具框架（Phase 2 子阶段 2.2：Tool trait / 注册表 / 受控执行器）。
//!
//! 语义骨架对照旧 backend `tool/Tool.java` / `ToolRegistry.java` /
//! `ToolExecutionPipeline.java`（只读权威规格）。
//!
//! # 子阶段 2.3：基础工具族
//!
//! 文件族 [`ReadFileTool`] / [`WriteFileTool`] / [`ListDirectoryTool`] /
//! [`GlobTool`] / [`GrepTool`]、shell 基座 [`BashTool`]（+ 受控进程模块
//! [`process`]）、Git 只读族 [`GitDiffTool`] / [`GitLogTool`] /
//! [`GitStatusTool`]；写前快照经 [`SnapshotSink`] 反转依赖（zk-tools 不依赖
//! zk-db，实现在 zk-server 组合根）。[`EchoTool`] 保留为测试工具（生产
//! 注册清单已换成上述真实工具）。
//!
//! # 子阶段 2.4：定点编辑与交互工具
//!
//! [`EditFileTool`]（名 `Edit`）——5 策略容错匹配 + 唯一性校验 + 写前
//! SHA-256 冲突检测 + 经 [`atomic`] 的 CAS 原子写；配套 [`file_state`]
//! Read-before-Edit 台账（进程级单例，会话分桶）。
//! [`TodoWriteTool`]（merge / replace 双模式 + `.zk/todos.md` 镜像）、
//! [`AskUserQuestionTool`]（经 [`ElicitationSink`] 端口阻塞发问，
//! 交互终态 / 5 分钟看门狗 / 取消令牌三路竞态）。
//!
//! 配置族 [`ConfigTool`]（`get` / `set` / `list` 三动作，`model` 键可选值经
//! [`ModelCatalog`] 端口动态取自提供商注册表）、[`SyntheticOutputTool`]
//! （动态注入 JSON Schema 后回收模型的结构化输出）。
//!
//! # Batch 5：长期记忆工具
//!
//! [`MemoryTool`]（名 `Memory`）——`read` / `write` / `delete` 三动作读写跨会话
//! 记忆；存储经 [`MemoryStore`] 端口反转（生产实现 `zk_engine::MemdirStore`
//! 的 `~/.zk/MEMORY.md`）。
//!
//! # Batch 8D：P2 工具域第一波
//!
//! [`MonitorTool`]（系统资源，经 `RESOURCE_MONITOR` 执行期门控）、
//! [`REPLTool`]（长驻解释器会话，见 [`repl`]）、Cron 三件
//! [`CronCreateTool`] / [`CronListTool`] / [`CronDeleteTool`]（经
//! `AGENT_TRIGGERS` 注册期门控，见 [`cron`]）、[`WorktreeTool`]
//! （`git worktree` 三子命令）、[`TerminalCaptureTool`]。
//!
//! # Batch 8E：可视化 / 工具检索 / 简报 / 休眠 / Notebook
//!
//! [`VisualizationTool`]（三载体白名单 + 标准化载荷，见 [`visualization`]，
//! 配套 [`IntentClassifier`] 关键词选型与 [`VisualizationAutoRouter`] 带缓存
//! 路由）、[`ToolSearchTool`]（`select:` / `+` / 关键词三形态检索，清单经
//! [`ToolCatalogPort`] 端口反转，见 [`tool_search`]）、[`BriefTool`]、
//! [`SleepTool`]、[`NotebookEditTool`]（`.ipynb` 的 cell 级编辑，经 [`atomic`]
//! CAS 原子写）。
//!
//! # Batch 8F：验证流水线
//!
//! [`EngineeringCheckRunnerTool`]——按序执行编译 / 测试 / lint /
//! 类型检查 / 格式 / 构建 / 自定义七类检查，逐步证据 + 汇总判定（见
//! [`verify_journey`]）；解析与执行入口对外公开，`zk-server` 的
//! `/api/verify/*` 端点复用同一实现。
//!
//! # 依赖方向（铁律）
//!
//! `zk-engine → (zk-tools, zk-llm)`；zk-tools 仅依赖最底层 [`zk_core`]
//! （`.zk/` 路径单一事实源），不依赖其余兄弟 crate，[`ToolSpec`] 自持
//! （引擎侧转换为 `zk_llm::ToolSpec` 随请求下发 LLM）。
//!
//! # 执行模型（对照旧源码锚点）
//!
//! - 每工具调用一 `tokio::spawn`（对照旧执行管线每调用一任务）；
//! - 全局 `Semaphore(16)` 并发上限（对照旧 `process.runner.max-concurrent`
//!   默认值，`ManagedProcessRunner.java` L43）；
//! - 三层取消树 session → run → `tool_call`（[`ToolExecutor::spawn_call`]
//!   内以 `child_token` 派生第三层；前两层由 zk-engine 维护）；
//! - 默认 120s / 上限 600s 超时（对照旧 `BASH_DEFAULT_TIMEOUT_MS` /
//!   `BASH_MAX_TIMEOUT_MS`，`BashTool.java` L51-54）；
//! - 输出采集上限 1 MiB（超限按 char 边界截断并追加标记）。

mod input;

pub mod ask_user_question;
pub mod atomic;
pub mod bash;
pub mod builtin;
pub mod config_tool;
pub mod elicitation;
pub mod executor;
pub mod file_edit;
pub mod file_read;
pub mod file_state;
pub mod file_write;
pub mod git;
pub mod glob;
pub mod grep;
pub mod list_dir;
pub mod memory;
pub mod process;
pub mod registry;
pub mod snapshot;
pub mod synthetic_output;
pub mod todo_write;
// Batch 7: plan mode + snip + ctx_inspect + verify_plan
pub mod ctx_inspect;
pub mod plan_mode;
pub mod snip;
pub mod verify_plan;
// Batch 6: agent + task tools
pub mod agent_tool;
pub mod send_message;
pub mod task_tools;
pub mod tool;
// Batch 8D: monitor + repl + cron + worktree + terminal capture
pub mod cron;
pub mod monitor;
pub mod repl;
pub mod terminal_capture;
pub mod worktree;
// Batch 8E: visualization + tool search + brief + sleep + notebook
pub mod brief;
pub mod notebook;
pub mod sleep;
pub mod tool_search;
pub mod visualization;
// Batch 8F: verify journey 流水线
pub mod verify_journey;
// WP-08: network tools use injected ports; zk-tools never owns credentials.
pub mod web_fetch;
pub mod web_search;

pub use ask_user_question::{AskUserQuestionTool, QUESTION_TIMEOUT};
pub use atomic::{ExpectedOldState, WriteEffect, WriteOutcome, sha256_hex, write_checked};
pub use bash::BashTool;
pub use builtin::EchoTool;
pub use config_tool::{BUILTIN_MODEL_ALIASES, ConfigTool, ModelCatalog};
pub use elicitation::{ElicitationOption, ElicitationOutcome, ElicitationRequest, ElicitationSink};
pub use executor::{CallEnv, MAX_CONCURRENT_TOOLS, MAX_TOOL_OUTPUT_BYTES, ToolEvent, ToolExecutor};
pub use file_edit::{EditFileTool, MAX_EDIT_FILE_BYTES};
pub use file_read::ReadFileTool;
pub use file_state::{
    FileState, FileStateCache, FileStateStore, MAX_FILE_STATE_BYTES, MAX_FILE_STATE_ENTRIES,
};
pub use file_write::WriteFileTool;
pub use git::{GitDiffTool, GitLogTool, GitStatusTool};
pub use glob::GlobTool;
pub use grep::GrepTool;
pub use list_dir::ListDirectoryTool;
pub use memory::{MemoryStore, MemoryTool};
pub use process::{MAX_CAPTURE_BYTES, ProcessOutcome, run_shell};
pub use registry::ToolRegistry;
pub use snapshot::{MAX_SNAPSHOT_BYTES, SnapshotRequest, SnapshotSink};
pub use synthetic_output::SyntheticOutputTool;
pub use todo_write::TodoWriteTool;
// Batch 6 re-exports
pub use agent_tool::{AgentInvocation, AgentTool, AgentToolBackend};
pub use send_message::{SendMessageBackend, SendMessageInvocation, SendMessageTool};
pub use task_tools::{
    TaskCoordinatorPort, TaskCreateTool, TaskGetTool, TaskInvocation, TaskListTool, TaskOutputTool,
    TaskSnapshot, TaskStopTool, TaskUpdateTool,
};
pub use tool::{
    DEFAULT_TOOL_TIMEOUT, MAX_TOOL_TIMEOUT, McpToolIdentity, Tool, ToolContext, ToolOutput,
    ToolSpec,
};
// Batch 7 re-exports
pub use ctx_inspect::{ContextInfo, ContextInfoPort, CtxInspectTool};
pub use plan_mode::{EnterPlanModeTool, ExitPlanModeTool};
pub use snip::SnipTool;
pub use verify_plan::VerifyPlanExecutionTool;
// Batch 8D re-exports
pub use cron::{CronCreateTool, CronDeleteTool, CronListTool, CronTask, CronTaskService, MAX_JOBS};
pub use monitor::MonitorTool;
pub use repl::{MAX_CONCURRENT_SESSIONS, REPLTool, ReplManager, ReplSession};
pub use terminal_capture::{DEFAULT_CAPTURE_LINES, TerminalCaptureTool};
pub use worktree::WorktreeTool;
// Batch 8E re-exports
pub use brief::{BriefTool, DEFAULT_MAX_LINES, SMART_EDGE_LINES};
pub use notebook::{CELL_TYPES, MAX_NOTEBOOK_BYTES, NotebookEditTool};
pub use sleep::{MAX_SLEEP_SECONDS, SLEEP_INTERRUPTED, SleepTool};
pub use tool_search::{
    DEFAULT_MAX_RESULTS, MAX_RESULTS, ScoredTool, SearchStrategy, StaticToolCatalog,
    TOOL_SEARCH_NAME, ToolCatalogPort, ToolDescriptor, ToolSearchTool,
};
pub use visualization::{
    ALLOWED_DIAGRAM_TYPES, DiagramKind, IntentClassifier, MAX_DIAGRAM_BYTES, RoutedVisualization,
    VisualizationAutoRouter, VisualizationHint, VisualizationPayload, VisualizationTool,
};
// Batch 8F re-exports
pub use verify_journey::{
    CheckKind, CheckPlan, CheckResult, CheckStatus, EngineeringCheckRunnerTool, JourneyReport,
    JourneyRequest, ProjectKind, parse_request as parse_verify_request, run_journey,
};
pub use web_fetch::{
    MAX_FETCH_BYTES, MAX_REDIRECTS, WebFetchError, WebFetchPort, WebFetchRequest, WebFetchResponse,
    WebFetchTool, is_public_web_ip,
};
pub use web_search::{
    MAX_SEARCH_QUERY_CHARS, MAX_SEARCH_RESULTS, SearchBackend, SearchBackendError, SearchRequest,
    SearchResult, UnavailableSearchBackend, WebSearchTool,
};
