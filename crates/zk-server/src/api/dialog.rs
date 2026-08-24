//! 对话框决策端点（Batch 8B Step 8，旧 `controller/DialogController.java` 79 行）。
//!
//! 后端流程遇到需用户决策的点时，登记一个 pending 请求（`requestId` →
//! [`oneshot::Sender`]）并经 WS 推对话框到前端；前端操作后 `POST` 决策到此，
//! 服务端据 `requestId` 取出发送端解除对应流程的阻塞。
//!
//! 两端点：
//! - `POST /api/dialogs/snapshot-update/{requestId}/decision`：快照更新决策
//!   （`action` = `merge` | `keep` | `replace`）。
//! - `POST /api/dialogs/plugin-permission/{requestId}/decision`：插件权限决策
//!   （`allowed` 布尔）。
//!
//! 两端点**恒 200 空体**（旧 `ResponseEntity.ok().build()`）：`requestId` 不存在
//!（已超时移除或从未登记）时静默——决策的接收方早已按超时路径继续，重复投递
//! 不应回错。

use std::collections::HashMap;
use std::sync::Mutex;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::oneshot;

use crate::state::AppState;

/// 待决策请求登记表（旧 `Map<String, CompletableFuture<Object>> pendingRequests`）。
///
/// 决策结果统一以 [`serde_json::Value`] 承载——快照决策投递字符串 `action`、
/// 权限决策投递布尔 `allowed`，与旧 `future.complete(action)` /
/// `future.complete(allowed)` 的 `Object` 泛型同构。
#[derive(Default)]
pub(crate) struct DialogRegistry {
    /// `requestId` → 解除阻塞的一次性发送端。
    ///
    /// `Mutex` 而非无锁表：注册/完成均为低频点操作，锁竞争可忽略；`oneshot`
    /// 发送端只能 `send` 一次，故取出即从表中移除（与旧 `remove` 同序）。
    pending: Mutex<HashMap<String, oneshot::Sender<Value>>>,
}

impl DialogRegistry {
    /// 新建空登记表。
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 登记一个待决策请求，返回等待端（旧 `createPendingRequest`）。
    ///
    /// 调用方 `await` 返回的 [`oneshot::Receiver`] 即阻塞至决策到达；超时语义由
    /// 调用方用 [`tokio::time::timeout`] 施加（旧 `orTimeout(60, SECONDS)`），
    /// 超时后丢弃 receiver 即可，本表的发送端在下次 `complete` 时因接收端已关闭
    /// 而被丢弃、条目移除。
    ///
    /// 目前尚无 Rust 生产端调用——旧 `createPendingRequest` 的调用方（快照更新 /
    /// 插件权限阻塞流程）尚未迁移，故 `#[allow(dead_code)]` 标注为待接线 API；
    /// 单测已覆盖其登记→完成语义。
    ///
    /// # Panics
    ///
    /// 锁中毒时 panic（进程级故障，fail fast——与本 crate 其余 `Mutex` 一致）。
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn register(&self, request_id: String) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("dialog registry lock")
            .insert(request_id, tx);
        rx
    }

    /// 完成一个待决策请求（旧 `pendingRequests.remove(id)` + `future.complete`）。
    ///
    /// `requestId` 不存在时静默返回——对齐旧 `if (future != null)` 的宽容分支。
    ///
    /// # Panics
    ///
    /// 锁中毒时 panic（同 [`Self::register`]）。
    pub(crate) fn complete(&self, request_id: &str, value: Value) {
        let sender = self
            .pending
            .lock()
            .expect("dialog registry lock")
            .remove(request_id);
        if let Some(sender) = sender {
            // 接收端可能已因超时被丢弃：`send` 返回 `Err` 属正常路径，忽略。
            let _ = sender.send(value);
        }
    }
}

/// `POST /api/dialogs/snapshot-update/{requestId}/decision` 请求体（旧
/// `record SnapshotDecision(String action)`）。
#[derive(Debug, Default, Deserialize)]
pub(crate) struct SnapshotDecision {
    /// 用户选择：`merge` | `keep` | `replace`。
    #[serde(default)]
    action: Option<String>,
}

/// `POST /api/dialogs/plugin-permission/{requestId}/decision` 请求体（旧
/// `record PermissionDecisionRequest(boolean allowed)`）。
#[derive(Debug, Default, Deserialize)]
pub(crate) struct PermissionDecisionRequest {
    /// 是否放行（旧 `boolean` 缺省零值 `false`）。
    #[serde(default)]
    allowed: bool,
}

/// 快照更新决策（旧 `resolveSnapshotUpdate` L35-45）。
#[utoipa::path(
    post,
    path = "/api/dialogs/snapshot-update/{requestId}/decision",
    tag = "dialog",
    params(("requestId" = String, Path, description = "待决策请求 id")),
    responses((status = 200, description = "已投递决策（恒 200 空体）"))
)]
pub(crate) async fn resolve_snapshot_update(
    State(state): State<AppState>,
    AxumPath(request_id): AxumPath<String>,
    axum::Json(decision): axum::Json<SnapshotDecision>,
) -> StatusCode {
    let action = decision.action.unwrap_or_default();
    tracing::info!(request_id = %request_id, action = %action, "Snapshot update decision");
    state.dialogs.complete(&request_id, json!(action));
    StatusCode::OK
}

/// 插件权限决策（旧 `resolvePluginPermission` L50-60）。
#[utoipa::path(
    post,
    path = "/api/dialogs/plugin-permission/{requestId}/decision",
    tag = "dialog",
    params(("requestId" = String, Path, description = "待决策请求 id")),
    responses((status = 200, description = "已投递决策（恒 200 空体）"))
)]
pub(crate) async fn resolve_plugin_permission(
    State(state): State<AppState>,
    AxumPath(request_id): AxumPath<String>,
    axum::Json(decision): axum::Json<PermissionDecisionRequest>,
) -> StatusCode {
    tracing::info!(request_id = %request_id, allowed = decision.allowed, "Plugin permission decision");
    state.dialogs.complete(&request_id, json!(decision.allowed));
    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_then_complete_delivers_value() {
        let registry = DialogRegistry::new();
        let rx = registry.register("req-1".to_owned());
        registry.complete("req-1", json!("merge"));
        let value = rx.await.expect("sender delivered");
        assert_eq!(value, json!("merge"));
    }

    #[tokio::test]
    async fn complete_unknown_id_is_silent() {
        let registry = DialogRegistry::new();
        // 不 panic、不阻塞——对齐旧 `if (future != null)` 的宽容分支。
        registry.complete("missing", json!(true));
    }

    #[tokio::test]
    async fn complete_after_receiver_dropped_is_silent() {
        let registry = DialogRegistry::new();
        let rx = registry.register("req-2".to_owned());
        drop(rx);
        // send 返回 Err 被忽略，不 panic。
        registry.complete("req-2", json!(false));
    }
}
