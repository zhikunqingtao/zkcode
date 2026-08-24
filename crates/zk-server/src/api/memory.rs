//! 记忆域 5 端点 handler（Batch 5 Step 6，旧 `MemoryController.java` 145 行
//! 逐分支复刻；路由注册见 `routes`，SQL 下沉见 zk-db `memory` 模块，
//! `MEMORY.md` 侧见 zk-engine [`MemdirStore`](zk_engine::MemdirStore)）。
//!
//! # 端点对照（旧 `@RequestMapping("/api/memory")`）
//!
//! | 方法 + 路径 | 旧 handler | 响应 |
//! |---|---|---|
//! | `GET /api/memory` | `getMemories` | 200 `{"entries":[…]}` |
//! | `PUT /api/memory` | `updateMemories` | 200 `{"success":true}` |
//! | `POST /api/memory` | `createMemory` | 201 `{"success":true,"id":"…"}` |
//! | `GET /api/memory/all` | `getAllMemories` | 200 `{"sqlite":[…],"memoryMd":[…]}` |
//! | `DELETE /api/memory/{memoryId}` | `deleteMemory` | 204 / 404（空体） |
//!
//! # 关键行为（逐字对照）
//!
//! - **无** `X-Session-Id` 头校验、**无**会话/项目归属校验（记忆为用户级资源，
//!   旧五个端点都直打 `globalJdbcTemplate`）。
//! - `GET` 与 `GET /all` 的 sqlite 分支是**同一条** SQL（`ORDER BY updated_at
//!   DESC`），故共用 [`Db::list_memories`](zk_db::Db::list_memories)。
//! - `PUT` 是**逐条** upsert（先 UPDATE，影响行数为 0 时 INSERT），非全量替换：
//!   请求里没提到的记忆不会被删除。每条各自自动提交，中途失败时前面的已生效
//!   （旧 controller 内 for 循环无事务包裹）。
//! - `POST` 恒走 INSERT（不 upsert）：调用方自带的 `id` 若已存在则主键冲突
//!   → 500（旧实现同为 `DataIntegrityViolationException`）。
//! - 请求体里的 `createdAt` / `updatedAt` 被忽略，落库时刻由服务端取（见
//!   zk-db `memory` 模块的写入路径）。
//! - `DELETE` 按影响行数分流：`> 0` → 204 空体，否则 404 空体（**非**错误
//!   信封——旧 `ResponseEntity.notFound().build()`）。
//!
//! # 差异留痕
//!
//! 1. **`PUT` 缺 `entries` 键**：旧实现 `request.entries()` 为 `null` 时 for
//!    循环抛 `NullPointerException` → 500。本层以 `#[serde(default)]` 视作空
//!    列表 → 200 `{"success":true}`（0 条更新）。崩溃属旧实现缺陷面而非契约面。
//! 2. **`memoryMd` 的 `timestamp` 精度**：旧实现取 `Instant.toString()`（小数位
//!    动态 0/3/6/9 位）；本层统一走 [`crate::iso::format_rfc3339_micros`]（恒 6
//!    位微秒），与 zkcode 全库时间戳写出约定一致（理由见 zk-db `time` 模块）。
//! 3. **键序**：旧 `Map.of(...)` 迭代序不确定；本层按固定键序装配，前端按键
//!    取值不依赖键序。

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use zk_db::MemoryUpsert;

use crate::error::ApiError;
use crate::iso::format_rfc3339_micros;
use crate::state::AppState;

/// `PUT /api/memory` 请求体（旧 `UpdateMemoriesRequest` record）。
///
/// 差异留痕 1：缺 `entries` 键时落空列表（旧实现为 `null` → NPE → 500）。
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct UpdateMemoriesRequest {
    /// 待 upsert 的记忆条目。
    pub entries: Vec<MemoryUpsert>,
}

/// `GET /api/memory`——记忆列表（`updated_at` 降序，旧 `getMemories`）。
#[utoipa::path(
    get,
    path = "/api/memory",
    tag = "memory",
    responses((status = 200, description = "{\"entries\":[{id,category,title,content,keywords,scope,source,createdAt,updatedAt}]}"))
)]
pub(crate) async fn get_memories(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let entries = state.db.list_memories().await?;
    Ok(Json(json!({ "entries": entries })))
}

/// `PUT /api/memory`——批量 upsert（旧 `updateMemories`）。
#[utoipa::path(
    put,
    path = "/api/memory",
    tag = "memory",
    responses(
        (status = 200, description = "{\"success\":true}"),
        (status = 400, description = "体缺失或非法 JSON（INVALID_REQUEST_BODY）")
    )
)]
pub(crate) async fn update_memories(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let request = serde_json::from_slice::<UpdateMemoriesRequest>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    let total = request.entries.len();
    // 旧 controller 的 for 循环：逐条各自提交，不做批事务包裹。
    for entry in request.entries {
        state.db.update_memory(entry).await?;
    }
    tracing::info!(total, "Updated memory entries");
    Ok(Json(json!({ "success": true })))
}

/// `POST /api/memory`——新建单条记忆（旧 `createMemory`，201）。
#[utoipa::path(
    post,
    path = "/api/memory",
    tag = "memory",
    responses(
        (status = 201, description = "{\"success\":true,\"id\":\"…\"}"),
        (status = 400, description = "体缺失或非法 JSON（INVALID_REQUEST_BODY）")
    )
)]
pub(crate) async fn create_memory(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let entry = serde_json::from_slice::<MemoryUpsert>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    // `source` 缺省兜底 `"USER"` 在 zk-db 写入路径内完成（旧实现在 controller
    // 内三元表达）；此处先取一份供日志，与落库值一致。
    let logged_source = entry.source.clone().unwrap_or_else(|| "USER".to_owned());
    let id = state.db.create_memory(entry).await?;
    tracing::info!(id = %id, source = %logged_source, "Created memory entry");
    Ok((
        StatusCode::CREATED,
        Json(json!({ "success": true, "id": id })),
    ))
}

/// `GET /api/memory/all`——`SQLite` 与 `MEMORY.md` 双源合并（旧 `getAllMemories`）。
#[utoipa::path(
    get,
    path = "/api/memory/all",
    tag = "memory",
    responses((status = 200, description = "{\"sqlite\":[…],\"memoryMd\":[{source,category,timestamp,content}]}"))
)]
pub(crate) async fn get_all_memories(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let sqlite = state.db.list_memories().await?;
    // `MEMORY.md` 缺失时 `list_entries` 回空列表（不报错），与旧
    // `MemdirService.listEntries` 的读失败降级一致。
    let memory_md: Vec<Value> = state
        .memdir
        .list_entries()
        .await
        .into_iter()
        .map(|entry| {
            json!({
                "source": entry.source.name(),
                "category": entry.category.tag(),
                // 差异留痕 2：恒 6 位微秒（旧 `Instant.toString()` 小数位动态）。
                "timestamp": format_rfc3339_micros(entry.timestamp_millis),
                "content": entry.content,
            })
        })
        .collect();
    Ok(Json(json!({ "sqlite": sqlite, "memoryMd": memory_md })))
}

/// `DELETE /api/memory/{memoryId}`——删除单条（旧 `deleteMemory`，204 / 404 空体）。
#[utoipa::path(
    delete,
    path = "/api/memory/{memoryId}",
    tag = "memory",
    params(("memoryId" = String, Path, description = "记忆 ID")),
    responses(
        (status = 204, description = "已删除（空体）"),
        (status = 404, description = "无此记忆（空体，非错误信封）")
    )
)]
pub(crate) async fn delete_memory(
    State(state): State<AppState>,
    AxumPath(memory_id): AxumPath<String>,
) -> Result<Response, ApiError> {
    if state.db.delete_memory(&memory_id).await? {
        tracing::info!(id = %memory_id, "Deleted memory entry");
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    Ok(StatusCode::NOT_FOUND.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 差异留痕 1：缺 `entries` 键 → 空列表（旧实现 NPE → 500）。
    #[test]
    fn update_body_defaults_entries_to_empty() {
        let request =
            serde_json::from_str::<UpdateMemoriesRequest>("{}").expect("empty object parses");
        assert!(request.entries.is_empty());
    }

    /// 条目按 camelCase 绑定，`createdAt` / `updatedAt` 被忽略（服务端取时刻）。
    #[test]
    fn update_body_binds_entries_and_drops_timestamps() {
        let request = serde_json::from_str::<UpdateMemoriesRequest>(
            r#"{"entries":[{"id":"m-1","category":"USER_PREFERENCE","title":"t",
                 "content":"c","keywords":"k","scope":"global","source":"AUTO",
                 "createdAt":"2020-01-01T00:00:00Z","updatedAt":"2020-01-01T00:00:00Z"}]}"#,
        )
        .expect("parses");
        assert_eq!(request.entries.len(), 1);
        let entry = &request.entries[0];
        assert_eq!(entry.id.as_deref(), Some("m-1"));
        assert_eq!(entry.category.as_deref(), Some("USER_PREFERENCE"));
        assert_eq!(entry.source.as_deref(), Some("AUTO"));
    }

    /// `POST` 体的九字段全可缺省（旧 record 允许 `null`）。
    #[test]
    fn create_body_fields_are_optional() {
        let entry = serde_json::from_str::<MemoryUpsert>("{}").expect("empty object parses");
        assert_eq!(entry, MemoryUpsert::default());
    }
}
