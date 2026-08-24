//! Run 域 3 读端点 handler（Batch 2 Step 2-5，旧 `RunController.java`；路由
//! 注册见 `routes`）。`POST /{runId}/cancel` 不在本步范围（需
//! `RunTerminationCoordinator`，随生命周期域后续移植）。
//!
//! # 端点对照（旧 `@RequestMapping("/api/runs")`）
//!
//! | 方法 + 路径 | 旧 handler | 授权失败 | 命中 |
//! |---|---|---|---|
//! | `GET /session/{sessionId}` | `listRuns` | 403 空体 | 200 `[RunEnvelope]` |
//! | `GET /{runId}` | `getRun` | 404 空体 | 200 `RunEnvelope` |
//! | `GET /{runId}/events` | `getEvents` | 403 空体 | 200 `RunEventsResponse` |
//!
//! # 关键行为（逐字对照）
//!
//! - `listRuns`：`canAccessSession(sessionId, X-Session-Id)` 失败 →
//!   `ResponseEntity.status(403).build()`（**空体**，非错误信封）；否则
//!   `findBySession(sessionId, limit)`。
//! - `getRun`：`accessibleRun(runId, X-Session-Id).map(ok).orElse(notFound()
//!   .build())` → 未授权/不存在均回 **404 空体**。
//! - `getEvents`：`accessibleRun` 为空 → **403 空体**；`boundedLimit =
//!   max(1, min(limit, 500))`；`hasMore = events.size() == boundedLimit`；
//!   `nextSeq = events.isEmpty() ? afterSeq : events.getLast().seq()`。
//! - 参数解析顺序镜像 Spring 声明序：先 `X-Session-Id` 头（缺 → 400
//!   `MISSING_HEADER`），再 `int` query 参数（非法 → 500，见
//!   [`crate::api::http_params`]）。

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use std::collections::HashMap;
use zk_db::RunEventView;

use crate::api::http_params::parse_spring_int;
use crate::error::ApiError;
use crate::session_access::{accessible_run, can_access_session, require_session_header};
use crate::state::AppState;

/// `getEvents` 200 响应体（旧 `RunController.RunEventsResponse` record：
/// `{events, hasMore, nextSeq}`，Jackson camelCase）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunEventsResponse {
    /// 事件列表（游标增量）。
    events: Vec<RunEventView>,
    /// 是否还有更多（`events.len() == boundedLimit`）。
    has_more: bool,
    /// 下一游标（空页回 `afterSeq`，否则末事件 `seq`）。
    next_seq: i64,
}

/// `GET /api/runs/session/{sessionId}`——会话运行列表（旧 `listRuns`）。
#[utoipa::path(
    get,
    path = "/api/runs/session/{sessionId}",
    tag = "runs",
    params(("sessionId" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "运行信封数组（started_at 降序）"),
        (status = 400, description = "缺 X-Session-Id 头（MISSING_HEADER）"),
        (status = 403, description = "对象授权失败（空体）")
    )
)]
pub(crate) async fn list_runs(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let asserted = require_session_header(&headers)?;
    let limit = parse_spring_int(query.get("limit").map(String::as_str), 20)?;
    if !can_access_session(&state, &session_id, &asserted).await? {
        return Ok(StatusCode::FORBIDDEN.into_response());
    }
    let runs = state
        .db
        .find_runs_by_session(&session_id, i64::from(limit))
        .await?;
    Ok(Json(runs).into_response())
}

/// `GET /api/runs/{runId}`——单个运行详情（旧 `getRun`）。
#[utoipa::path(
    get,
    path = "/api/runs/{runId}",
    tag = "runs",
    params(("runId" = String, Path, description = "运行 ID")),
    responses(
        (status = 200, description = "运行信封"),
        (status = 400, description = "缺 X-Session-Id 头（MISSING_HEADER）"),
        (status = 404, description = "不存在或对象授权失败（空体）")
    )
)]
pub(crate) async fn get_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let session_id = require_session_header(&headers)?;
    match accessible_run(&state, &run_id, &session_id).await? {
        Some(run) => Ok(Json(run).into_response()),
        None => Ok(StatusCode::NOT_FOUND.into_response()),
    }
}

/// `GET /api/runs/{runId}/events`——运行事件游标分页（旧 `getEvents`）。
#[utoipa::path(
    get,
    path = "/api/runs/{runId}/events",
    tag = "runs",
    params(("runId" = String, Path, description = "运行 ID")),
    responses(
        (status = 200, description = "RunEventsResponse{events,hasMore,nextSeq}"),
        (status = 400, description = "缺 X-Session-Id 头（MISSING_HEADER）"),
        (status = 403, description = "不存在或对象授权失败（空体）")
    )
)]
pub(crate) async fn get_events(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let session_id = require_session_header(&headers)?;
    let after_seq = parse_spring_int(query.get("afterSeq").map(String::as_str), 0)?;
    let limit = parse_spring_int(query.get("limit").map(String::as_str), 100)?;
    if accessible_run(&state, &run_id, &session_id)
        .await?
        .is_none()
    {
        return Ok(StatusCode::FORBIDDEN.into_response());
    }
    // 旧 `Math.max(1, Math.min(limit, 500))`。
    let bounded_limit = limit.clamp(1, 500);
    let events = state
        .db
        .get_run_events(&run_id, i64::from(after_seq), i64::from(bounded_limit))
        .await?;
    let has_more = events.len() == usize::try_from(bounded_limit).unwrap_or(0);
    // 旧 `events.isEmpty() ? afterSeq : events.getLast().seq()`。
    let next_seq = events
        .last()
        .map_or_else(|| i64::from(after_seq), |event| event.seq);
    Ok(Json(RunEventsResponse {
        events,
        has_more,
        next_seq,
    })
    .into_response())
}

/// `POST /api/runs/{runId}/cancel` — object-authorized, idempotent Run cancel.
pub(crate) async fn cancel_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let session_id = require_session_header(&headers)?;
    let Some(run) = accessible_run(&state, &run_id, &session_id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    if run.is_terminal() {
        return Ok(Json(serde_json::json!({
            "runId": run_id,
            "status": run.status,
            "cancelled": false,
            "alreadyTerminal": true
        }))
        .into_response());
    }
    let transition = state
        .authz
        .terminations
        .cancel_by_user(&run_id, Some("REST_CANCEL"))
        .await
        .map_err(|error| {
            tracing::error!(%run_id, %error, "REST run cancellation failed");
            ApiError::internal()
        })?;
    Ok(Json(serde_json::json!({
        "runId": run_id,
        "status": transition.as_str(),
        "cancelled": transition == zk_db::run::TransitionResult::Applied,
        "alreadyTerminal": transition == zk_db::run::TransitionResult::AlreadyTerminal
    }))
    .into_response())
}
