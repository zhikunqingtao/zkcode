//! Cron 定时任务工具族——`CronCreate` / `CronList` / `CronDelete` 三件套。
//!
//! 对照旧四文件（只读权威规格）：
//! - `service/CronTaskService.java`（179L）——任务台账（见 [`service`]）；
//! - `tool/impl/CronCreateTool.java`（163L）——见 [`create`]；
//! - `tool/impl/CronListTool.java`（95L）——见 [`list`]；
//! - `tool/impl/CronDeleteTool.java`（88L）——见 [`delete`]。
//!
//! # 门控
//!
//! 三件工具的旧 `isEnabled()` 一律返回
//! `featureFlags.isEnabled("AGENT_TRIGGERS")`，出厂默认 **false**。本移植沿用
//! **同一 flag 名** [`zk_core::feature_flags::AGENT_TRIGGERS`]（zk-core 的出厂
//! 默认表已含该条，注释亦明示「`Cron*` 工具门控」），门在
//! `zk-server` 的 `build_tool_registry` 做**注册期**判定——flag 关则三件工具
//! 不进注册表，模型看不见（等价于旧 `isEnabled() == false` 时 Spring 侧
//! 不纳入 `ToolRegistry`）。
//!
//! 三件工具共享组合根注入的 [`CronTaskPort`]；`SQLite` 是唯一权威。

pub mod create;
pub mod delete;
pub mod list;
pub mod service;

pub use create::{CREATE_PROMPT_CLIP, CronCreateTool};
pub use delete::CronDeleteTool;
pub use list::{CronListTool, LIST_PROMPT_CLIP, NO_TASKS};
pub use service::{
    CronCreateRequest, CronDeleteReceipt, CronPortError, CronTask, CronTaskPort,
    DEFAULT_MISSED_POLICY, DEFAULT_OVERLAP_POLICY, DEFAULT_TIMEZONE, MAX_JOBS, clip,
    format_timestamp_ms, next_run_after_ms, parse_schedule, parse_timezone,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_is_default_off_and_service_is_a_port() {
        fn accepts_port(_: Option<&dyn CronTaskPort>) {}

        let flags = zk_core::feature_flags::FeatureFlags::with_defaults();
        assert_eq!(zk_core::feature_flags::AGENT_TRIGGERS, "AGENT_TRIGGERS");
        assert!(!flags.is_enabled(zk_core::feature_flags::AGENT_TRIGGERS));
        accepts_port(None);
    }
}
