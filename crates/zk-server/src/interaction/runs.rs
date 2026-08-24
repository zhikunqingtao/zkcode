//! Run 生命周期在 zk-server 侧的**接入面**——SQL 实现已下沉 [`zk_db::run`]。
//!
//! 旧源 `run/RunControlService.java`。原先本模块自持 `run_envelopes` /
//! `run_event_log` 的全部 SQL，但 **`zk-engine` 同样必须写这两张表**：Run 不先
//! 落库，`zk-authz` 的授权祖先链（`AuthorizationSubjectResolver::load_root`）
//! 就查不到根 Run，全部工具调用被 `Run ancestry contains a missing parent` 拒绝。
//! 而依赖铁律禁止 `zk-engine → zk-server`，若在引擎侧另写一份 INSERT 则出现
//! 逻辑分叉（旧源里 `RunControlService` 是**唯一写权威**）。
//!
//! 故 SQL 整体下沉到二者共同依赖的 `zk-db`（见 [`zk_db::run`] 的层级归属说明），
//! 本模块只保留两件事：
//!
//! 1. 交互链/授权链既有调用点的**再导出**（签名与语义不变，仅实现位置迁移）；
//! 2. [`RunEvents`]——`zk_authz` 的 `RunEventSink` 端口实现，即旧源中
//!    `AuthorizationService` / `ToolExecutionGateway` 注入的 `runs` 方法面。
//!
//! 逐行对照旧源的表格、事务边界偏离（R-01）与其余留痕均在 [`zk_db::run`]
//! 模块文档内。

use rusqlite::Connection;
use serde_json::Value;
use zk_authz::tool_facts::RunEventSink;
use zk_db::Db;

pub use zk_db::run::{
    EXIT_INTERACTION_CAPACITY_EXCEEDED, EXIT_INTERACTION_EXPIRED, EXIT_USER_CANCELLED,
    TransitionResult, append_event_in_current_write, interrupt_in_current_write,
    mark_running_in_current_write, mark_waiting_in_current_write, request_cancel_in_current_write,
    start_in_current_write, transition_in_current_write,
};

/// `zk_authz` 的 `RunEventSink` 端口实现——授权诊断事件的 `run_event_log` 出口。
///
/// 旧源即 `AuthorizationService` / `ToolExecutionGateway` 注入的
/// `RunControlService`（`appendEvent` + `appendEventInCurrentWrite` 两个方法面）。
#[derive(Clone)]
pub struct RunEvents {
    db: Db,
}

impl RunEvents {
    /// 装配（组装根注入同一 [`Db`] 句柄）。
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

impl std::fmt::Debug for RunEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunEvents").finish_non_exhaustive()
    }
}

impl RunEventSink for RunEvents {
    /// 旧 `appendEvent(runId, type, toolUseId, data)`（L147-149）：自开写事务。
    ///
    /// 旧源是同步阻塞写，调用点（`recordDenial` / `recordAnalysisFailure`）在
    /// 返回后即认为审计事件已落库。`RunEventSink` 因此也是同步签名，Rust 侧只能
    /// 走 [`Db::with_conn_blocking`]（writer `Mutex` + 无 `await`），语义与旧源
    /// 逐一对应（§8 R-05）。失败仅告警：旧源同样不让审计写失败阻断拒绝路径——
    /// 拒绝本身已由返回值传达。
    fn append_event(
        &self,
        run_id: &str,
        event_type: &str,
        tool_use_id: Option<&str>,
        payload: &Value,
    ) {
        let outcome = self.db.with_conn_blocking(|conn| {
            let tx = conn.transaction()?;
            append_event_in_current_write(&tx, run_id, event_type, tool_use_id, payload)?;
            tx.commit()?;
            Ok(())
        });
        if let Err(error) = outcome {
            tracing::warn!(run_id, event_type, %error, "run event append failed");
        }
    }

    /// 旧 `appendEventInCurrentWrite(...)`（L261-274）：复用调用方事务。
    fn append_event_in_current_write(
        &self,
        conn: &Connection,
        run_id: &str,
        event_type: &str,
        tool_use_id: Option<&str>,
        payload: &Value,
    ) {
        if let Err(error) =
            append_event_in_current_write(conn, run_id, event_type, tool_use_id, payload)
        {
            tracing::warn!(run_id, event_type, %error, "run event append failed in current write");
        }
    }
}
