//! Activity 域 1 端点 handler（Batch 2 Step 2-6，旧 `ActivityController.java`；
//! 路由注册见 `routes`，持久化查询见 zk-db `activity` 模块）。
//!
//! # 端点对照（旧 `@RequestMapping("/api/sessions")`）
//!
//! | 方法 + 路径 | 旧 handler | 响应 |
//! |---|---|---|
//! | `GET /{sessionId}/activities` | `getActivities` | 200 `{activities,hasMore,total}` |
//!
//! # 关键行为（逐字对照）
//!
//! - 参数：`offset`（默认 0）、`limit`（默认 50，`MAX_LIMIT = 100`）；
//!   `if (offset < 0) offset = 0; if (limit <= 0 || limit > 100) limit = 50;`。
//! - **无** `X-Session-Id` 头校验、**无**会话存在性校验——会话不存在时
//!   `total = 0`、`activities = []`、`hasMore = false`（旧实现同样不 orElseThrow）。
//! - `total = countBySessionId`；`activities = findBySessionIdPaged(sessionId,
//!   offset, limit)`（原始 `snake_case` 列名 Map 列表，见 zk-db `activity`）；
//!   `hasMore = (offset + activities.size()) < total`。
//! - 响应体为 `Map.of("activities", .., "hasMore", .., "total", ..)`——Java
//!   `Map.of` 迭代序不确定，前端按键取值不依赖键序，故此处用固定键序装配。

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use serde_json::{Value, json};
use std::collections::HashMap;

use crate::api::http_params::parse_spring_int;
use crate::error::ApiError;
use crate::state::AppState;

/// 旧 `ActivityController.MAX_LIMIT`。
const MAX_LIMIT: i32 = 100;

/// `GET /api/sessions/{sessionId}/activities`——会话活动分页（旧 `getActivities`）。
#[utoipa::path(
    get,
    path = "/api/sessions/{sessionId}/activities",
    tag = "sessions",
    params(("sessionId" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "{activities:[{...}], hasMore:bool, total:number}")
    )
)]
pub(crate) async fn get_activities(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let mut offset = parse_spring_int(query.get("offset").map(String::as_str), 0)?;
    let mut limit = parse_spring_int(query.get("limit").map(String::as_str), 50)?;
    // 旧参数校验：`if (offset < 0) offset = 0; if (limit <= 0 || limit > 100) limit = 50;`。
    if offset < 0 {
        offset = 0;
    }
    if limit <= 0 || limit > MAX_LIMIT {
        limit = 50;
    }

    let total = state.db.count_activities_by_session(&session_id).await?;
    let activities = state
        .db
        .find_activities_by_session_paged(&session_id, i64::from(offset), i64::from(limit))
        .await?;
    // 旧 `(offset + activities.size()) < total`。
    let has_more =
        (i64::from(offset) + i64::try_from(activities.len()).unwrap_or(i64::MAX)) < total;

    Ok(Json(json!({
        "activities": activities,
        "hasMore": has_more,
        "total": total,
    })))
}
