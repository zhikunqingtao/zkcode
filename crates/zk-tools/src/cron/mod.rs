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
//! 台账 [`CronTaskService`] 由组合根构造一次并以 `Arc` 三处共享——三件工具必须
//! 看同一张表，否则 `CronList` / `CronDelete` 看不到 `CronCreate` 建的任务。

pub mod create;
pub mod delete;
pub mod list;
pub mod service;

pub use create::{CREATE_PROMPT_CLIP, CronCreateTool};
pub use delete::CronDeleteTool;
pub use list::{CronListTool, LIST_PROMPT_CLIP, NO_TASKS};
pub use service::{
    CLEANUP_INTERVAL, CronTask, CronTaskService, DEFAULT_EXPIRY_DAYS, DURABLE_STORE_FILE,
    LimitReached, MAX_JOBS, clip, next_run, parse_schedule,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::tool::{Tool, ToolContext};

    /// 三件工具共享同一张台账——create 建的任务 list 看得见、delete 删得掉。
    #[tokio::test]
    async fn the_three_tools_share_one_ledger() {
        let cwd = std::env::temp_dir().join(format!("zk-cron-trio-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cwd);
        std::fs::create_dir_all(&cwd).expect("mkdir");
        let ledger = Arc::new(CronTaskService::new(&cwd));

        let create = CronCreateTool::new(Arc::clone(&ledger));
        let list = CronListTool::new(Arc::clone(&ledger));
        let delete = CronDeleteTool::new(Arc::clone(&ledger));

        let ctx = || {
            let (tx, _rx) = mpsc::unbounded_channel();
            ToolContext::new(CancellationToken::new(), tx)
        };

        // 起始为空表。
        assert_eq!(list.execute(json!({}), ctx()).await.content, NO_TASKS);

        let created = create
            .execute(json!({ "cron": "*/10 * * * *", "prompt": "sync" }), ctx())
            .await;
        assert!(!created.is_error, "{}", created.content);
        let id = serde_json::from_str::<serde_json::Value>(&created.content).expect("json")["id"]
            .as_str()
            .expect("id")
            .to_owned();

        let listed = list.execute(json!({}), ctx()).await;
        assert!(listed.content.contains(&id), "{}", listed.content);

        let deleted = delete.execute(json!({ "id": &id }), ctx()).await;
        assert!(!deleted.is_error, "{}", deleted.content);
        assert!(
            deleted.content.contains("remaining=0"),
            "{}",
            deleted.content
        );
        assert_eq!(list.execute(json!({}), ctx()).await.content, NO_TASKS);
    }

    /// 门控 flag 名沿用旧 `AGENT_TRIGGERS`，且出厂默认关。
    #[test]
    fn gate_flag_matches_the_legacy_name_and_default() {
        let flags = zk_core::feature_flags::FeatureFlags::with_defaults();
        assert_eq!(zk_core::feature_flags::AGENT_TRIGGERS, "AGENT_TRIGGERS");
        assert!(!flags.is_enabled(zk_core::feature_flags::AGENT_TRIGGERS));
    }
}
