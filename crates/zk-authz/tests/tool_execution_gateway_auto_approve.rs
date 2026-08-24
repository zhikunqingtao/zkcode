//! `ToolExecutionGatewayAutoApproveTest.java`（75 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - GW-01：旧 `execute(...)` 在准入后**直接** `tool.call(...)` 并返回 `ToolResult`；
//!   zk-authz 受依赖铁律不得依赖 zk-tools，网关只表达准入
//!   （[`ToolExecutionGateway::admit`]），`tool.call` 由 zk-engine 的 `ToolExecutor`
//!   在 `admit` 返回 `Ok` 之后执行。故旧 L58 `result.isError()==false` 与 L61
//!   `verify(tool, times(1)).call(...)` 的等价观测是「`admit` 返回 `Ok` 且
//!   `tool_started` 事件恰好写入一次」——一次准入放行对应一次执行。
//! - 旧 L59/L60 `verify(authorization).finalDynamicRecheck(...)` /
//!   `finalGrantRecheckInCurrentTransaction(...)`：Rust 侧 `AuthorizationService`
//!   是具体类型无法 spy，但这两步就在 `admit_with` 主体内且**任一失败即 Err**，
//!   `Ok` 返回本身即证明二者都跑过（`admit` 无第二条通往 `tool_started` 的路径）。

mod common;

use common::{FakeTool, Harness};
use serde_json::json;
use zk_authz::model::{
    AuthorizationSubject, AuthorizedOperation, DiagnosticSource, EffectClass, OperationDescriptor,
    RiskClass,
};

/// 旧源 `ToolExecutionGatewayAutoApproveTest.java:30-74`
/// `recordsModeAuditAndCallsToolExactlyOnce`。
#[tokio::test]
async fn records_mode_audit_and_admits_tool_exactly_once() {
    let harness = Harness::new();

    // L34-38
    let tool = FakeTool::new("TestTool");
    let input = json!({"value": "safe-test-value"});
    let context = harness.context("run", "tool-use", "session");

    // L39-44
    let subject = AuthorizationSubject {
        root_session_id: "session".to_owned(),
        root_run_id: "run".to_owned(),
        current_run_id: "run".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: harness.workspace.clone(),
    };
    let descriptor = OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: "TestTool".to_owned(),
        action: "invoke".to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: "generic-v1".to_owned(),
        effects: vec![EffectClass::Process],
        resources: Vec::new(),
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk: RiskClass::High,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "test operation".to_owned(),
    };

    // L45-47
    let allowed = AuthorizedOperation {
        subject,
        descriptor,
        execution_input: input.clone(),
        source: DiagnosticSource::Mode,
        reason_code: "AUTO_APPROVE".to_owned(),
        grant_id: None,
        grant_scope: None,
        interaction_id: None,
        execution_attempt_id: "attempt".to_owned(),
    };

    // L55-58：准入放行（等价于旧「工具被调用恰好一次且结果非错误」）。
    harness
        .execution_gateway()
        .admit(&tool, &allowed, &context)
        .await
        .expect("auto approved operation is admitted");

    // L63-66：`tool_started` 事件带 run / toolUse 定位。
    let events = harness.events.of_type("tool_started");
    assert_eq!(events.len(), 1, "tool_started must be written exactly once");
    let event = &events[0];
    assert_eq!(event.run_id, "run");
    assert_eq!(event.tool_use_id.as_deref(), Some("tool-use"));

    // L67-73：六项审计条目逐格对齐。
    let payload = event.payload.as_object().expect("payload object");
    assert_eq!(payload.get("outcome"), Some(&json!("ALLOW")));
    assert_eq!(payload.get("authorizationSource"), Some(&json!("MODE")));
    assert_eq!(payload.get("reasonCode"), Some(&json!("AUTO_APPROVE")));
    assert_eq!(payload.get("risk"), Some(&json!("HIGH")));
    assert_eq!(payload.get("operationHash"), Some(&json!("operation-hash")));
    assert_eq!(payload.get("inputHash"), Some(&json!("input-hash")));
}
