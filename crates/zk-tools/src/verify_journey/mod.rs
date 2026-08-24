//! `VerifyJourney` 工具——端到端验证流水线（Batch 8F）。
//!
//! 语义来源（旧仓库只读）：`tool/verify/` 包（`VerifyJourneyTool.java` 744L）。
//!
//! - [`checks`]：种类 / 状态 / 结果与报告渲染（纯数据层，无 IO）；
//! - [`tool`]：入参解析（[`parse_request`]）、流水线执行（[`run_journey`]）与
//!   [`Tool`](crate::tool::Tool) 实现。
//!
//! [`parse_request`] / [`run_journey`] 对外公开，供 `zk-server` 的
//! `POST /api/verify/run-checks` 复用同一套解析与执行语义——REST 与 LLM 两条
//! 入口共用一份实现，不做第二套解析。

pub mod checks;
pub mod tool;

pub use checks::{
    CheckKind, CheckPlan, CheckResult, CheckStatus, JourneyReport, ProjectKind, default_command,
};
pub use tool::{
    CHECK_TIMEOUT_DEFAULT, CHECK_TIMEOUT_MAX, CHECK_TIMEOUT_MIN, EngineeringCheckRunnerTool,
    JourneyRequest, MAX_CHECKS, PIPELINE_BUDGET, RequestError, parse_request, run_journey,
};
