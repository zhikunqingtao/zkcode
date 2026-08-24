//! 交互恢复查询与数据库 CAS 决策入口（2.5；旧
//! `controller/InteractionController.java` 177 行逐分支复刻）。
//!
//! 两个端点：
//! - `GET /api/interactions/pending?sessionId=`（旧 L40-50）
//! - `POST /api/interactions/{interactionId}/decisions`（旧 L52-172）
//!
//! # 响应信封
//!
//! 本域**不用** [`crate::error::ApiError`] 的全局信封：旧 Controller 对业务拒绝
//! 直接 `ResponseEntity.body(Map.of("code", X))`（裸 `{"code":...}`），403/404
//! 为**空体**，200/409 回 `InteractionRequest` 行本身。只有 Spring 抛异常的路径
//!（缺参 / 缺头 / 体不可读 / DB 异常）才走 `GlobalExceptionHandler` 信封，本模块
//! 以 `ApiError` 表达这些路径。
//!
//! # 权限交互的权威性
//!
//! 前端提交的 `decision` / `scope` / `remember` 对 PERMISSION 类型**一律不采信**：
//! 旧 L70-92 用 `optionId` 在库内权威视图的 `options` 中反查，`decision` / `scope`
//! 只能取自被选中项，`remember` 由 `decision=="allow" && scope!="once"` 推出。
//! 这是「前端不能自行拼装范围更大的授权」这条安全不变量的第一道闸；第二道在
//! [`crate::interaction::DurableInteractionService::decide_request`] 的六层校验。

use std::collections::HashMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use zk_authz::interaction::{InteractionRecord, InteractionStatus, InteractionType};
use zk_protocol::InteractionView;

use crate::error::ApiError;
use crate::interaction::DurableInteractionService;
use crate::session_access::{can_access_session, require_session_header};
use crate::state::AppState;

/// `POST /api/interactions/{interactionId}/decisions` 请求体（旧
/// `DecisionRequest` record，L174-176）。
///
/// 全字段 `default`：旧 Java record 由 Jackson 构造，缺失的 `long` / `int` /
/// `boolean` 取零值，引用类型取 null；未知键被忽略（Spring Boot 默认关闭
/// `FAIL_ON_UNKNOWN_PROPERTIES`）。
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct DecisionRequest {
    /// 客户端持有的乐观锁版本（CAS 输入）。
    expected_version: i64,
    /// 原始决策字面量（PERMISSION 类型下被库内权威选项覆盖）。
    decision: Option<String>,
    /// 非 PERMISSION 类型的自由响应体。
    response: Option<Value>,
    /// 是否记住（PERMISSION 类型下由选中项推出）。
    remember: bool,
    /// 记住范围（PERMISSION 类型下由选中项推出）。
    scope: Option<String>,
    /// 选中的决策选项 id（PERMISSION 类型必填）。
    option_id: Option<String>,
    /// 前端回显的 `operationHash`（PERMISSION 类型必填，须与库内一致）。
    operation_hash: Option<String>,
    /// 前端回显的投递世代（写入响应体，供审计追溯）。
    delivery_generation: i64,
}

/// 交互拒绝统一进入公共 REST 错误契约。
fn bare_code(status: StatusCode, code: &str) -> Response {
    ApiError {
        status,
        code: code.to_owned(),
        message: code.to_owned(),
    }
    .into_response()
}

/// 旧 `ResponseEntity.status(...).body(current)`——交互行本身作为体。
fn record_body(status: StatusCode, record: &InteractionRecord) -> Response {
    (status, Json(record)).into_response()
}

/// 旧 Jackson `JsonNode.asText()`：值节点转文本，容器节点回空串。
fn json_text(node: &Value) -> String {
    match node {
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Null => "null".to_owned(),
        Value::Array(_) | Value::Object(_) => String::new(),
    }
}

/// 旧 `for (var option : json.readTree(...))`：数组迭代元素、对象迭代字段值、
/// 值节点无子元素（故 `allowed` 保持 false）。
fn json_children(node: &Value) -> Vec<&Value> {
    match node {
        Value::Array(items) => items.iter().collect(),
        Value::Object(fields) => fields.values().collect(),
        _ => Vec::new(),
    }
}

/// 旧 L67-69 的三个可变局部量（PERMISSION 分支会整体改写）。
struct Effective {
    /// `effectiveDecision`。
    decision: Option<String>,
    /// `effectiveScope`。
    scope: Option<String>,
    /// `effectiveRemember`。
    remember: bool,
}

/// 旧 L70-92：PERMISSION 类型的权威选项反查。
///
/// `Err(Response)` 直接作为最终响应返回（409/400/500 三种拒绝）。
// `Err` 变体即 axum `Response`（128B）——本文件的拒绝路径就是「直接把最终响应
// 回给调用方」，装箱只会在每个调用点多一次解引用，不改变语义收益。
#[allow(clippy::result_large_err)]
fn permission_effective(
    interaction_id: &str,
    view: Result<InteractionView, String>,
    body: &DecisionRequest,
) -> Result<Effective, Response> {
    /// 旧 catch 分支（L87-91）：库内协议不合法 → 500 裸码。
    fn invalid(interaction_id: &str, detail: &str) -> Response {
        tracing::error!(
            interaction_id,
            detail,
            "Stored permission interaction protocol is invalid"
        );
        bare_code(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERACTION_PROTOCOL_INVALID",
        )
    }

    // 旧 L72-74：两个必填字段缺一即协议不匹配。
    let (Some(option_id), Some(operation_hash)) =
        (body.option_id.as_deref(), body.operation_hash.as_deref())
    else {
        return Err(bare_code(
            StatusCode::CONFLICT,
            "PERMISSION_PROTOCOL_MISMATCH",
        ));
    };
    // 旧 L75：`interactions.view(current)` 抛异常 → catch → 500。
    let view = view.map_err(|error| invalid(interaction_id, &error))?;
    // 旧 L76-79：协议版本与 operationHash 双校验。
    if view.protocol_version != Some(zk_authz::interaction::PROTOCOL_VERSION)
        || view.operation_hash.as_deref() != Some(operation_hash)
    {
        return Err(bare_code(
            StatusCode::CONFLICT,
            "PERMISSION_PROTOCOL_MISMATCH",
        ));
    }
    // 旧 L80-82：`authoritative.options()` 为 null 时 NPE → catch → 500。
    let options = view
        .options
        .as_ref()
        .ok_or_else(|| invalid(interaction_id, "authoritative view carries no options"))?;
    let selected = options
        .iter()
        .find(|option| option.get("optionId").and_then(Value::as_str) == Some(option_id));
    // 旧 L83：选项不在权威名单 → 400。
    let Some(selected) = selected else {
        return Err(bare_code(
            StatusCode::BAD_REQUEST,
            "PERMISSION_OPTION_NOT_ALLOWED",
        ));
    };
    // 旧 L84：`selected.get("decision")` 为 null 时 L86 的 `.equals` NPE → 500。
    let decision = selected
        .get("decision")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(interaction_id, "selected option carries no decision"))?
        .to_owned();
    // 旧 L85：`getOrDefault("scope", "once")`。
    let scope = selected
        .get("scope")
        .and_then(Value::as_str)
        .unwrap_or("once")
        .to_owned();
    // 旧 L86：allow 且非 once 才记住。
    let remember = decision == "allow" && scope != "once";
    Ok(Effective {
        decision: Some(decision),
        scope: Some(scope),
        remember,
    })
}

/// 旧 L93-102：决策字面量 → 目标终态（`equalsIgnoreCase`，null 落 else 分支）。
fn requested_status(decision: Option<&str>) -> Option<InteractionStatus> {
    let decision = decision?;
    if decision.eq_ignore_ascii_case("allow") || decision.eq_ignore_ascii_case("answer") {
        Some(InteractionStatus::Answered)
    } else if decision.eq_ignore_ascii_case("deny") {
        Some(InteractionStatus::Denied)
    } else if decision.eq_ignore_ascii_case("cancel") {
        Some(InteractionStatus::Cancelled)
    } else {
        None
    }
}

/// 旧 L111-125：非 PERMISSION 类型的库内白名单校验。
// 同 `permission_effective`：`Err` 就是最终响应本体，不装箱。
#[allow(clippy::result_large_err)]
fn validate_stored_options(
    current: &InteractionRecord,
    effective: &Effective,
    normalized_decision: &str,
) -> Result<(), Response> {
    /// 旧 catch 分支（L126-130）：库内选项不合法 → 500 裸码。
    fn invalid(current: &InteractionRecord, detail: &str) -> Response {
        tracing::error!(
            interaction_id = %current.interaction_id,
            kind = %current.kind,
            detail,
            "Stored interaction options are invalid"
        );
        bare_code(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERACTION_PROTOCOL_INVALID",
        )
    }

    // 旧 L111-117：允许决策名单。
    let allowed_decisions: Value = serde_json::from_str(&current.allowed_decisions_json)
        .map_err(|error| invalid(current, &error.to_string()))?;
    let allowed = json_children(&allowed_decisions)
        .into_iter()
        .any(|option| normalized_decision.eq_ignore_ascii_case(&json_text(option)));
    if !allowed {
        return Err(bare_code(
            StatusCode::BAD_REQUEST,
            "INTERACTION_DECISION_NOT_ALLOWED",
        ));
    }
    // 旧 L118-125：remember 时 scope 名单（null → "session"，比对忽略大小写）。
    if effective.remember {
        let requested_scope = effective
            .scope
            .as_ref()
            .map_or_else(|| "session".to_owned(), |scope| scope.to_lowercase());
        let scope_options: Value = serde_json::from_str(&current.scope_options_json)
            .map_err(|error| invalid(current, &error.to_string()))?;
        let scope_allowed = json_children(&scope_options)
            .into_iter()
            .any(|option| requested_scope.eq_ignore_ascii_case(&json_text(option)));
        if !scope_allowed {
            return Err(bare_code(
                StatusCode::BAD_REQUEST,
                "PERMISSION_SCOPE_NOT_ALLOWED",
            ));
        }
    }
    Ok(())
}

/// 旧 L131-147：落库响应体。
fn decision_response(
    current: &InteractionRecord,
    effective: &Effective,
    normalized_decision: &str,
    body: &DecisionRequest,
) -> Value {
    if current.kind == InteractionType::Permission {
        // 旧 L133-142：六键权限响应（旧用 LinkedHashMap 再 `Map.copyOf`，
        // 后者顺序不确定，故键序非契约面）。
        json!({
            "decision": normalized_decision,
            "remember": effective.remember,
            "scope": if effective.remember {
                effective
                    .scope
                    .as_ref()
                    .map_or_else(|| "session".to_owned(), |scope| scope.to_lowercase())
            } else {
                "once".to_owned()
            },
            "optionId": body.option_id,
            "operationHash": body.operation_hash,
            "deliveryGeneration": body.delivery_generation,
        })
    } else {
        // 旧 L144-146：透传前端响应，缺省补单键决策。
        body.response
            .clone()
            .unwrap_or_else(|| json!({ "decision": normalized_decision }))
    }
}

/// `GET /api/interactions/pending`——断线重连后的待决交互补齐（旧 L40-50）。
#[utoipa::path(
    get,
    path = "/api/interactions/pending",
    tag = "interactions",
    responses(
        (status = 200, description = "该会话全部 pending 交互视图（创建时刻升序）"),
        (status = 403, description = "会话归属校验失败（空体）")
    )
)]
pub(crate) async fn pending(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    // 旧 `@RequestParam String sessionId` 缺失 → `handleMissingParam`（L73-78）。
    let session_id = query.get("sessionId").ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "MISSING_PARAMETER".to_owned(),
        message: "Required parameter 'sessionId' is missing".to_owned(),
    })?;
    let header_session_id = require_session_header(&headers)?;
    if !can_access_session(&state, session_id, &header_session_id).await? {
        tracing::warn!(
            requested_session_id = %session_id,
            caller_session_id = %header_session_id,
            "Pending interaction access denied"
        );
        return Ok(StatusCode::FORBIDDEN.into_response());
    }
    let views = state
        .authz
        .interactions
        .pending_views(session_id)
        .await
        .map_err(|error| {
            tracing::error!(
                session_id = %session_id,
                code = %error.code,
                error = %error.message,
                "pending interaction views unavailable"
            );
            ApiError::internal()
        })?;
    Ok(Json(views).into_response())
}

/// `POST /api/interactions/{interactionId}/decisions`——用户决策的唯一入口
///（旧 L52-172；WS 上行 `/app/permission` 一律回 `interaction_rest_required`）。
#[utoipa::path(
    post,
    path = "/api/interactions/{interactionId}/decisions",
    tag = "interactions",
    responses(
        (status = 200, description = "决策已落库，回交互行"),
        (status = 400, description = "决策/选项/范围非法（裸 {\"code\"}）"),
        (status = 403, description = "会话归属校验失败（空体）"),
        (status = 404, description = "交互不存在（空体）"),
        (status = 409, description = "协议不匹配或 CAS 失败（裸码或交互行）"),
        (status = 503, description = "写库不可用（裸 {\"code\"}）")
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn decide(
    State(state): State<AppState>,
    AxumPath(interaction_id): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let session_id = require_session_header(&headers)?;
    // 旧 `@RequestBody DecisionRequest` 不可读 → `handleHttpMessageNotReadable`。
    let body: DecisionRequest =
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid_request_body())?;
    // 旧 L56-61：`findById` 抛 `EmptyResultDataAccessException` → 404 空体。
    let Some(current) = state.authz.interactions.find_by_id(&interaction_id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    // 旧 L62-66：交互归属会话与调用方自称会话必须一致。
    if !can_access_session(&state, &current.session_id, &session_id).await? {
        tracing::warn!(
            interaction_id = %interaction_id,
            owner_session_id = %current.session_id,
            caller_session_id = %session_id,
            "Interaction decision access denied"
        );
        return Ok(StatusCode::FORBIDDEN.into_response());
    }
    // 旧 L67-69：默认取前端字段；PERMISSION 分支整体改写。
    let mut effective = Effective {
        decision: body.decision.clone(),
        scope: body.scope.clone(),
        remember: body.remember,
    };
    if current.kind == InteractionType::Permission {
        let view = DurableInteractionService::view(&current).map_err(|error| error.to_string());
        match permission_effective(&interaction_id, view, &body) {
            Ok(resolved) => effective = resolved,
            Err(response) => return Ok(response),
        }
    }
    // 旧 L93-102：无法识别的决策字面量 → 400。
    let Some(requested) = requested_status(effective.decision.as_deref()) else {
        return Ok(bare_code(
            StatusCode::BAD_REQUEST,
            "INTERACTION_DECISION_INVALID",
        ));
    };
    // 旧 L103-106：已终态——同终态幂等 200，异终态 409，两者都回当前行。
    if current.status != InteractionStatus::Pending {
        let status = if current.status == requested {
            StatusCode::OK
        } else {
            StatusCode::CONFLICT
        };
        return Ok(record_body(status, &current));
    }
    // 旧 L107-109：归一决策名（PERMISSION 的 ANSWERED 写 "allow"）。
    let normalized_decision = match requested {
        InteractionStatus::Answered => {
            if current.kind == InteractionType::Permission {
                "allow"
            } else {
                "answer"
            }
        }
        InteractionStatus::Denied => "deny",
        _ => "cancel",
    };
    if current.kind != InteractionType::Permission
        && let Err(response) = validate_stored_options(&current, &effective, normalized_decision)
    {
        return Ok(response);
    }
    let response = decision_response(&current, &effective, normalized_decision, &body);
    // 旧 L150-152：三个终态原因码。
    let reason_code = match requested {
        InteractionStatus::Answered => "USER_APPROVED",
        InteractionStatus::Denied => "USER_DENIED",
        _ => "USER_CANCELLED",
    };
    let decided = state
        .authz
        .interactions
        .decide_request(
            &interaction_id,
            body.expected_version,
            requested,
            Some(response),
            Some(reason_code),
        )
        .await;
    let decided = match decided {
        Ok(decided) => decided,
        // 旧 L155-158：写库不可用 → 503 裸码（旧码 `AUTHORIZATION_STORE_BUSY`
        // 来自 `executeBoundedWrite`；zk-db 单 writer + `busy_timeout` 把该态归一
        // 到 `INTERACTION_STORE_FAILED`，见 §8 偏离表 IC-01）。
        Err(error) if error.code == "INTERACTION_STORE_FAILED" => {
            tracing::warn!(
                interaction_id = %interaction_id,
                code = %error.code,
                error = %error.message,
                "Interaction decision database unavailable"
            );
            return Ok(bare_code(StatusCode::SERVICE_UNAVAILABLE, &error.code));
        }
        // 旧 L159-166：校验类拒绝——`PERMISSION_` 前缀 409，其余 400。
        Err(error) => {
            let status = if error.code.starts_with("PERMISSION_") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            tracing::debug!(
                interaction_id = %interaction_id,
                code = %error.code,
                expected_version = body.expected_version,
                "Interaction decision rejected"
            );
            return Ok(bare_code(status, &error.code));
        }
    };
    // 旧 L168-171：CAS 结果与请求终态不符（并发抢先）→ 409 回权威行。
    let status = if decided.status == requested {
        StatusCode::OK
    } else {
        StatusCode::CONFLICT
    };
    Ok(record_body(status, &decided))
}
