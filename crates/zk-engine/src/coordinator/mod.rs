//! 协调器/编排器层——对照旧 `com.aicodeassistant.coordinator` 包。
//!
//! # 模块导航
//!
//! - [`workflow`]：四阶段工作流状态机（`WorkflowPhase` /
//!   `CoordinatorWorkflow`，对照旧 `CoordinatorWorkflow.java`）。
//! - [`prompt`]：Coordinator 系统提示构建器（对照旧
//!   `CoordinatorPromptBuilder.java`）。
//! - [`service`]：Coordinator 服务核心——模式检测 + 工具上下文
//!   （对照旧 `CoordinatorService.java`）。
//! - [`workflow_engine`]：四阶段编排引擎——自动检测 + 委派验证
//!   （对照旧 `CoordinatorWorkflowEngine.java`）。
//! - [`team`]：团队生命周期管理 + 进程内 Worker 后端
//!   （对照旧 `TeamManager.java` + `InProcessBackend.java`）。
//!
//! # 有意差异
//!
//! - [`event_bus`]：Coordinator 事件总线（`tokio::sync::broadcast` 进程内
//!   广播，对照旧 `CoordinatorEventBus.java`）。
//! - [`team`] 内含 `SwarmService` / `WorkerState` / `SwarmResults`——Swarm
//!   运行时状态 + 结果聚合（对照旧 `SwarmState.java` + `SwarmService.java`
//!   + `ResultAggregator.java`）。旧 `SwarmConfig` 暂未移植。
//! - Feature Flags `COORDINATOR_MODE` 和 `ENABLE_AGENT_SWARMS` 已在
//!   `zk-core::feature_flags` 中注册（出厂默认值 `true`，与旧 YAML 一致）。
//!   本模块不重复声明。

pub mod event_bus;
pub mod prompt;
pub mod service;
pub mod team;
pub mod workflow;
pub mod workflow_engine;

pub use event_bus::{CoordinatorEvent, CoordinatorEventBus};
pub use service::{COORDINATOR_MODE_ENV, CoordinatorService, SwarmPhase};
pub use team::{
    AgentRequest, AgentResult, InProcessBackend, MAX_WORKERS_PER_TEAM, SwarmResults, SwarmService,
    TaskSpec, TeamInfo, TeamManager, WORKER_TIMEOUT_SECONDS, WorkerState, WorkerStatus,
};
pub use workflow::{CoordinatorWorkflow, PhaseRecord, WorkflowPhase, WorkflowStatus};
pub use workflow_engine::{CoordinatorWorkflowEngine, ValidationResult, ValidationSeverity};
