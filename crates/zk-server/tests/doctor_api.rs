//! Batch 1 Step 1-6 集成测试——`GET /api/doctor`（旧
//! `HealthController.doctor` L88-112）。
//!
//! 响应形状与实采样例 `docs/baseline/samples/GET_api-doctor.json` 逐键互锁：
//! 检查项键集恒为样例键集的子集（`name` / `status` 必在，`version` /
//! `message` / `latencyMs` 为 null 时整键剥离），状态词表恒为小写三值
//! （**不同于** `/api/health` 的 `UP` / `DEGRADED`，旧端本就是两套词表）。

mod common;

use std::collections::BTreeSet;

use axum::http::StatusCode;

use common::{app, call, json_body, local_get, remote_get, sample};

fn external_probe_gate_enabled() -> bool {
    std::env::var("ZK_RUN_GIT_TESTS").as_deref() == Ok("true")
}

/// 检查项清单（顺序即旧 `checks.add` 追加顺序）。
const CHECK_NAMES: [&str; 6] = [
    "runtime",
    "git",
    "ripgrep",
    "database",
    "llm_providers",
    "python_service",
];

/// 旧状态词表（`doctorCheck` 的三值）。
const STATUSES: [&str; 3] = ["ok", "warning", "error"];

/// 200 + `{checks:[…]}`：6 项齐备、顺序稳定、状态取值在词表内、
/// `name`/`status` 恒在。
#[tokio::test]
async fn doctor_reports_all_checks() {
    if !external_probe_gate_enabled() {
        return;
    }
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/doctor")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    let mut top_keys: Vec<&str> = body
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    top_keys.sort_unstable();
    assert_eq!(top_keys, vec!["checks"], "envelope is exactly {{checks}}");

    let checks = body["checks"].as_array().expect("checks array");
    let names: Vec<&str> = checks
        .iter()
        .map(|check| check["name"].as_str().expect("name str"))
        .collect();
    assert_eq!(names, CHECK_NAMES);

    for check in checks {
        let status = check["status"].as_str().expect("status str");
        assert!(
            STATUSES.contains(&status),
            "status outside legacy vocabulary: {check}"
        );
        assert!(
            check["name"].as_str().is_some_and(|name| !name.is_empty()),
            "name must be present: {check}"
        );
    }
}

/// 每项键集 ⊆ 样例键集，且 `version`/`message`/`latencyMs` 类型对齐样例
/// （null 时整键剥离，故不做逐索引形状比对——样例的 git 成功 / ripgrep 缺失
/// 与本机环境无关）。
#[tokio::test]
async fn check_objects_match_baseline_key_shape() {
    if !external_probe_gate_enabled() {
        return;
    }
    let baseline = sample("GET_api-doctor.json");
    let allowed: BTreeSet<String> = baseline["checks"]
        .as_array()
        .expect("baseline checks array")
        .iter()
        .flat_map(|check| check.as_object().expect("object").keys().cloned())
        .collect();
    assert_eq!(
        allowed.iter().map(String::as_str).collect::<Vec<&str>>(),
        vec!["latencyMs", "message", "name", "status", "version"],
        "baseline key vocabulary"
    );

    let mut router = app();
    let (_status, _headers, body) = call(&mut router, local_get("/api/doctor")).await;
    let body = json_body(&body);
    for check in body["checks"].as_array().expect("checks array") {
        let object = check.as_object().expect("object");
        for key in object.keys() {
            assert!(allowed.contains(key), "unknown key {key} in {check}");
        }
        assert!(object.contains_key("name"), "name required: {check}");
        assert!(object.contains_key("status"), "status required: {check}");
        assert!(
            object
                .get("version")
                .is_none_or(serde_json::Value::is_string),
            "version must be a string when present: {check}"
        );
        assert!(
            object
                .get("message")
                .is_none_or(serde_json::Value::is_string),
            "message must be a string when present: {check}"
        );
        assert!(
            object
                .get("latencyMs")
                .is_none_or(|value| value.as_u64().is_some()),
            "latencyMs must be a non-negative integer when present: {check}"
        );
    }
}

/// 运行时检查恒 `ok` 且带版本（旧 `java` 检查的等价项）；外部工具探测三分支
/// 均带 `latencyMs`（旧 `checkExternalTool` 无论成功失败都记录耗时）。
#[tokio::test]
async fn runtime_and_external_tool_checks_carry_expected_fields() {
    if !external_probe_gate_enabled() {
        return;
    }
    let mut router = app();
    let (_status, _headers, body) = call(&mut router, local_get("/api/doctor")).await;
    let body = json_body(&body);
    let checks = body["checks"].as_array().expect("checks array").clone();
    let find = |name: &str| {
        checks
            .iter()
            .find(|check| check["name"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("check {name} missing"))
    };

    let runtime = find("runtime");
    assert_eq!(runtime["status"], "ok");
    assert!(
        runtime["version"]
            .as_str()
            .is_some_and(|version| !version.is_empty())
    );
    assert!(
        runtime["message"]
            .as_str()
            .is_some_and(|message| message.contains("uptime"))
    );
    assert!(
        runtime.get("latencyMs").is_none(),
        "runtime check is not timed (baseline java check has no latencyMs)"
    );

    for name in ["git", "ripgrep"] {
        let check = find(name);
        assert!(
            check["latencyMs"].as_u64().is_some(),
            "external tool probe is always timed: {check}"
        );
        let status = check["status"].as_str().expect("status");
        if status == "ok" {
            assert!(
                check["version"]
                    .as_str()
                    .is_some_and(|version| !version.is_empty()),
                "ok probe carries the first output line: {check}"
            );
            assert_eq!(check["message"], format!("{name} available"));
        } else {
            assert!(
                check.get("version").is_none(),
                "failed probe strips version: {check}"
            );
        }
    }
}

/// 数据库检查在测试装配（内存库 + 已迁移）下恒 `ok` 且带耗时；LLM / 侧车
/// 检查在无 provider、侧车非本进程托管时为 `error` / `warning`（可选组件
/// 不拖垮端点状态码——旧实现恒 200）。
#[tokio::test]
async fn optional_component_checks_are_deterministic_under_test_wiring() {
    if !external_probe_gate_enabled() {
        return;
    }
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/doctor")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checks never change the status code"
    );
    let body = json_body(&body);
    let checks = body["checks"].as_array().expect("checks array").clone();
    let find = |name: &str| {
        checks
            .iter()
            .find(|check| check["name"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("check {name} missing"))
    };

    let database = find("database");
    assert_eq!(database["status"], "ok", "in-memory db is queryable");
    assert!(database["latencyMs"].as_u64().is_some());
    assert!(database.get("version").is_none());

    assert_eq!(
        find("llm_providers")["status"],
        "error",
        "no provider registered in test wiring"
    );
    assert_eq!(
        find("python_service")["status"],
        "warning",
        "sidecar not managed by this process in test wiring"
    );
}

/// 诊断端点同栈过 `access_guard`（公网对端 403）。
#[tokio::test]
async fn doctor_rejects_remote_peer() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, remote_get("/api/doctor")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&body)["code"], "ACCESS_DENIED");
}

/// `OpenAPI` 文档收录诊断路径（与 `api::openapi` 单测的 27 条计数互锁）。
#[tokio::test]
async fn openapi_document_lists_doctor_path() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
    let document = json_body(&body);
    let paths = document["paths"].as_object().expect("paths object");
    assert!(paths.contains_key("/api/doctor"));
}
