//! S7b 集成测试——模型目录 / 用户配置 / `OpenAPI` 文档端点。
//!
//! 形状权威：`GET_api-models.json` / `GET_api-config.json` /
//! `PUT_api-config.json` 样例逐键对齐（`assert_same_shape`）；错误路径
//! 断言统一扁平错误响应 `{code,message,requestId}`。

mod common;

use axum::http::StatusCode;
use serde_json::json;

use common::{app, assert_same_shape, call, json_body, local_get, local_put, sample};

// ───── GET /api/models ─────

/// 形状逐键对齐样例；15 条目录、ID 集与样例逐一相同、defaultModel 来自配置。
#[tokio::test]
async fn models_shape_matches_sample() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/models")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    let sample = sample("GET_api-models.json");
    assert_same_shape(&sample, &body, "models");
    let ids = |value: &serde_json::Value| -> Vec<String> {
        value["models"]
            .as_array()
            .expect("array")
            .iter()
            .map(|model| model["id"].as_str().expect("id str").to_owned())
            .collect()
    };
    // 目录与样例逐条同序同 ID（Phase 1 静态目录照抄样例）。
    assert_eq!(ids(&body), ids(&sample));
    assert_eq!(body["models"].as_array().expect("array").len(), 15);
    // 测试配置的默认模型（Config::test_config）。
    assert_eq!(body["defaultModel"], "qwen3.8-max");
    // 每个条目 11 键齐全（assert_same_shape 只锁首元素，此处全量锁）。
    for model in body["models"].as_array().expect("array") {
        assert_eq!(model.as_object().expect("obj").len(), 11, "model: {model}");
    }
}

/// `?modelId=` 已知模型：仍返回全量目录（旧端点语义）。
#[tokio::test]
async fn models_known_model_id_passes() {
    let mut router = app();
    let (status, _headers, body) =
        call(&mut router, local_get("/api/models?modelId=kimi-k3")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body)["models"].as_array().expect("arr").len(),
        15
    );
}

/// `?modelId=` 未知模型：400 `INVALID_REQUEST` 信封（旧
/// `IllegalArgumentException("Invalid model: ...")` 映射）。
#[tokio::test]
async fn models_unknown_model_id_rejected() {
    let mut router = app();
    let (status, _headers, body) =
        call(&mut router, local_get("/api/models?modelId=no-such-model")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body = json_body(&body);
    assert_eq!(body["code"], "INVALID_REQUEST");
    assert_eq!(body["message"], "Invalid model: no-such-model");
}

// ───── GET/PUT /api/config ─────

/// 无存量行：返回代码内置默认值，形状逐键对齐样例（12 键）。
#[tokio::test]
async fn config_get_default_shape_matches_sample() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/config")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_same_shape(&sample("GET_api-config.json"), &body, "config");
    assert_eq!(body["authType"], "localhost");
    assert_eq!(body["theme"], "dark");
    assert_eq!(body["defaultModel"], "qwen3.8-max");
    assert_eq!(body["autoCompactThreshold"], 80);
}

/// PUT 幂等回写（样例 note：write-back of current theme value）：
/// 响应形状对齐 PUT 样例，且写后读回逐键一致。
#[tokio::test]
async fn config_put_idempotent_write_back() {
    let mut router = app();
    let put_body = json!({"theme": "dark"}).to_string();
    let (status, _headers, body) =
        call(&mut router, local_put("/api/config", Some(put_body))).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_same_shape(&sample("PUT_api-config.json"), &body, "put_config");
    assert_eq!(body["success"], true);
    assert_eq!(body["config"]["theme"], "dark");
    // 写后读回：GET 与 PUT 回显逐键一致（幂等）。
    let (status, _headers, read_back) = call(&mut router, local_get("/api/config")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&read_back), body["config"]);
}

/// PATCH 语义：部分更新只覆盖给定键，其余保留；再次读回持久生效。
#[tokio::test]
async fn config_put_partial_update_persists() {
    let mut router = app();
    let updates = json!({"theme": "light", "locale": "zh-CN", "autoCompactThreshold": 60});
    let (status, _headers, body) = call(
        &mut router,
        local_put("/api/config", Some(updates.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(body["config"]["theme"], "light");
    assert_eq!(body["config"]["locale"], "zh-CN");
    assert_eq!(body["config"]["autoCompactThreshold"], 60);
    // 未更新键保留默认。
    assert_eq!(body["config"]["authType"], "localhost");
    assert_eq!(body["config"]["autoCompactEnabled"], true);
    // 读回持久生效（同库同 Router）。
    let (_, _, read_back) = call(&mut router, local_get("/api/config")).await;
    let read_back = json_body(&read_back);
    assert_eq!(read_back["theme"], "light");
    assert_eq!(read_back["locale"], "zh-CN");
    assert_eq!(read_back["autoCompactThreshold"], 60);
}

/// 错误路径：非法 JSON 体 / 非对象体 → 400 `INVALID_REQUEST_BODY`。
#[tokio::test]
async fn config_put_malformed_body_rejected() {
    let mut router = app();
    for bad in ["not-json", "[1,2]", "\"str\"", ""] {
        let (status, _headers, body) =
            call(&mut router, local_put("/api/config", Some(bad.to_owned()))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {bad:?}");
        assert_eq!(json_body(&body)["code"], "INVALID_REQUEST_BODY");
    }
}

/// 错误路径：字段类型非法（threshold 传字符串）→ 500 `INTERNAL_ERROR`
/// （旧 `ConfigService.updateUserConfig` 的 Jackson 失败包 `RuntimeException`，
/// 经 `handleGeneric` 落 500 泛化文案），且不落库（后续读回仍为默认值）。
#[tokio::test]
async fn config_put_bad_field_type_returns_internal_error() {
    let mut router = app();
    let bad = json!({"autoCompactThreshold": "high"}).to_string();
    let (status, _headers, body) = call(&mut router, local_put("/api/config", Some(bad))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let error = json_body(&body);
    assert_eq!(error["code"], "INTERNAL_ERROR");
    assert_eq!(error["message"], "An unexpected error occurred");
    assert!(error["requestId"].is_string());
    let (_, _, read_back) = call(&mut router, local_get("/api/config")).await;
    assert_eq!(json_body(&read_back)["autoCompactThreshold"], 80);
}

// ───── GET /api/openapi.json ─────

/// 200 + `openapi`/`info`/`paths` 顶层字段 + paths 覆盖 Phase 1 全部
/// 15 个业务端点（method 级逐一断言）。
#[tokio::test]
async fn openapi_document_covers_phase1_endpoints() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
    let doc = json_body(&body);
    assert!(doc["openapi"].as_str().is_some_and(|v| v.starts_with('3')));
    assert!(doc["info"]["title"].as_str().is_some());
    assert!(doc["info"]["version"].as_str().is_some());
    let paths = doc["paths"].as_object().expect("paths object");
    // (path, method) 全覆盖：sessions 8 + health×3 + auth/status + models + config×2。
    let expected: [(&str, &str); 15] = [
        ("/api/sessions", "post"),
        ("/api/sessions", "get"),
        ("/api/sessions/{id}", "get"),
        ("/api/sessions/{id}", "delete"),
        ("/api/sessions/{id}/resume", "post"),
        ("/api/sessions/{id}/compact", "post"),
        ("/api/sessions/{id}/export", "post"),
        ("/api/sessions/{id}/messages", "get"),
        ("/api/health", "get"),
        ("/api/health/live", "get"),
        ("/api/health/ready", "get"),
        ("/api/auth/status", "get"),
        ("/api/models", "get"),
        ("/api/config", "get"),
        ("/api/config", "put"),
    ];
    for (path, method) in expected {
        assert!(
            paths.get(path).and_then(|item| item.get(method)).is_some(),
            "missing {method} {path} in openapi paths"
        );
    }
}
