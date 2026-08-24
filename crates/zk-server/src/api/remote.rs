//! 远程控制域端点——手机端紧急中断（旧 `RemoteControlController`）。
//!
//! 安全模型完全复用准入守卫（[`crate::middleware::access_guard`]）的三层递进
//! 认证：localhost 免认证 / 局域网 Token / 会话 Cookie，本模块不做额外鉴权
//! （同旧源类注释）。
//!
//! - `GET /api/remote/status`：活跃会话快照 + 服务运行时长，供手机端 3s 轮询；
//! - `POST /api/remote/interrupt`：遍历活跃会话终止其非终态 Run 并推送
//!   `interrupt_ack`。
//!
//! 旧源两端点均恒 200（异常在会话粒度被吞并落 `partialFailure`），此处保持
//! 同形：不抛错误信封。

use axum::Json;
use axum::extract::State;
use serde_json::json;

use crate::state::AppState;

/// 每会话扫描的 Run 上限（旧 `runs.findBySession(sessionId, 100)` 的字面量）。
const RUN_SCAN_LIMIT: i64 = 100;

/// 终止明细（旧 `termination.cancelByUser(run.id(), "remote_user_cancelled")`）。
const REMOTE_CANCEL_DETAIL: &str = "remote_user_cancelled";

/// 中断确认原因（旧 `AbortReason.USER_INTERRUPT.name()`）。
const REASON_USER_INTERRUPT: &str = "USER_INTERRUPT";

/// `GET /api/remote/status`——活跃会话快照（旧 `getStatus`）。
///
/// `sessions[].online` 恒 `true`：会话来源即「有活连接」的集合，旧源同样写死
/// 字面量 `true`。
#[utoipa::path(
    get,
    path = "/api/remote/status",
    tag = "remote",
    responses((status = 200, description = "活跃会话数 / 会话列表 / 运行时长（`Xh Ym`）"))
)]
pub(crate) async fn status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let active_ids = state.hub.active_sessions();
    let sessions: Vec<serde_json::Value> = active_ids
        .iter()
        .map(|session_id| json!({ "sessionId": session_id, "online": true }))
        .collect();
    Json(json!({
        "activeSessions": active_ids.len(),
        "sessions": sessions,
        "serverUptime": format_uptime(state.started_at.elapsed()),
    }))
}

/// `POST /api/remote/interrupt`——一键中断全部活跃会话（旧 `interruptAll`）。
///
/// 逐字对齐旧循环：每会话先按 `started_at` 倒序取最近 100 个 Run，跳过终态，
/// 对其余调用 `cancel_by_user`；随后推 `interrupt_ack` 让前端 UI 回 idle；
/// 计数 +1。会话内任一步失败 → 记 `partialFailure`、该会话不计数（旧
/// `catch (Exception e)` 包住整段会话体，与此同构）。
///
/// `interrupted` 的旧语义：`count > 0 || activeIds.isEmpty()`——无会话时视为
/// 「已无可中断者」而非失败。
#[utoipa::path(
    post,
    path = "/api/remote/interrupt",
    tag = "remote",
    responses((status = 200, description = "中断结果（interrupted/sessionCount/partialFailure?）"))
)]
pub(crate) async fn interrupt(State(state): State<AppState>) -> Json<serde_json::Value> {
    let active_ids = state.hub.active_sessions();
    let mut count: usize = 0;
    let mut any_error = false;

    for session_id in &active_ids {
        match interrupt_session(&state, session_id).await {
            Ok(()) => {
                count += 1;
                tracing::info!(session_id = %session_id, "Remote interrupt");
            }
            Err(error) => {
                any_error = true;
                tracing::warn!(session_id = %session_id, %error, "Failed to interrupt session");
            }
        }
    }
    tracing::info!(count, "Remote interrupt completed");

    let mut body = json!({
        "interrupted": count > 0 || active_ids.is_empty(),
        "sessionCount": count,
    });
    if any_error {
        body["partialFailure"] = json!(true);
    }
    Json(body)
}

/// 单会话中断（旧 `interruptAll` 的 try 块体）。
///
/// 数据库 Run 状态是取消的权威裁决者——唤醒与进程终止都在 `requestCancel`
/// 竞胜之后发生（旧源同点注释）。
async fn interrupt_session(state: &AppState, session_id: &str) -> Result<(), String> {
    let runs = state
        .db
        .find_runs_by_session(session_id, RUN_SCAN_LIMIT)
        .await
        .map_err(|error| error.to_string())?;
    for run in runs {
        if run.is_terminal() {
            continue;
        }
        state
            .authz
            .terminations
            .cancel_by_user(&run.id, Some(REMOTE_CANCEL_DETAIL))
            .await
            .map_err(|error| error.to_string())?;
    }
    state
        .hub
        .push(
            session_id,
            zk_protocol::server_message::ServerMessage::InterruptAck {
                reason: REASON_USER_INTERRUPT.to_owned(),
            },
        )
        .await;
    Ok(())
}

/// 运行时长文本（旧 `formatUptime`：`toHours()` + `toMinutesPart()`，
/// 小时为 0 时只留分钟）。
fn format_uptime(uptime: std::time::Duration) -> String {
    let total_minutes = uptime.as_secs() / 60;
    let hours = total_minutes / 60;
    let minutes = total_minutes % 60;
    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 运行时长文本逐档对齐旧 `formatUptime`。
    #[test]
    fn uptime_matches_legacy_format() {
        let cases = [
            (0, "0m"),
            (59, "0m"),
            (60, "1m"),
            (3599, "59m"),
            (3600, "1h 0m"),
            (3660, "1h 1m"),
            (90_061, "25h 1m"),
        ];
        for (secs, expected) in cases {
            assert_eq!(
                format_uptime(Duration::from_secs(secs)),
                expected,
                "{secs}s"
            );
        }
    }

    /// 无活跃会话：`interrupted` 为 true（旧 `activeIds.isEmpty()` 分支）、
    /// 不带 `partialFailure` 键。
    #[tokio::test]
    async fn interrupt_reports_success_without_sessions() {
        let state = AppState::for_tests();
        let body = interrupt(State(state)).await;
        assert_eq!(body.0, json!({ "interrupted": true, "sessionCount": 0 }));
    }

    /// 无活跃会话时的状态快照形状（`sessions` 空数组、`serverUptime` 文本）。
    #[tokio::test]
    async fn status_reports_empty_snapshot() {
        let state = AppState::for_tests();
        let body = status(State(state)).await;
        assert_eq!(body.0["activeSessions"], json!(0));
        assert_eq!(body.0["sessions"], json!([]));
        assert_eq!(body.0["serverUptime"], json!("0m"));
    }
}
