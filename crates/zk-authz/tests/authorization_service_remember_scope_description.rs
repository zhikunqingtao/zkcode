//! `AuthorizationServiceRememberScopeDescriptionTest.java`（114 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - **RS-01**：旧测试用 `when(grants.supportedScopes(operation)).thenReturn(scopes)`
//!   直接注入「可记住范围」。Rust 侧 `supported_scopes` 是自由函数
//!   （`grants.rs:569`，对齐旧 `PermissionGrantRepository.java:185-197`），不可
//!   mock；因此改为**挑选真实产出目标 scope 列表的分析器/风险组合**：
//!   `network-v1`/`mcp-v1`/`bash-v2` + GUARDED 真实回 `[RUN, SESSION]`，
//!   `network-v1` + HIGH 真实回 `[]`。每个测试都额外断言捕获到的
//!   `scope_options` 与旧测试注入的 scope 列表同形，保证判定格未被偷换。
//! - **RS-02**：旧测试用 `thenThrow(PromptCapturedException)` 打断链路取 prompt。
//!   Rust 侧 `FakeGateway` 默认脚本是 `PENDING`（没人回答），`authorize_prepared`
//!   必然以 `PERMISSION_INTERACTION_PENDING` 族错误收尾，prompt 同样已落
//!   `gateway.created`——观测点与旧测试完全一致（`createAuthorization` 的第 4 实参）。

mod common;

use common::{FakeTool, Harness};
use serde_json::{Map, Value, json};
use zk_authz::model::{
    EffectClass, OperationDescriptor, PermissionScope, PreparedOperation, RiskClass,
};

/// 旧测试私有 `capturePrompt(toolName, analyzerId, scopes)`
/// （`AuthorizationServiceRememberScopeDescriptionTest.java:66-110`）。
///
/// 返回 `(prompt, scope_options)`：后者是 RS-01 要求的判定格校验值。
async fn capture_prompt(
    tool_name: &str,
    analyzer_id: &str,
    risk: RiskClass,
) -> (Map<String, Value>, Vec<String>) {
    let harness = Harness::new();
    harness.seed_run("session", "run").await;

    // L79-84：subject + 12 字段 descriptor（effects=[UNKNOWN]，其余列表全空）。
    let subject = zk_authz::model::AuthorizationSubject {
        root_session_id: "session".to_owned(),
        root_run_id: "run".to_owned(),
        current_run_id: "run".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: harness.workspace.clone(),
    };
    let operation = OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: tool_name.to_owned(),
        action: "invoke".to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: analyzer_id.to_owned(),
        effects: vec![EffectClass::Unknown],
        resources: Vec::new(),
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "redacted input".to_owned(),
    };

    // L85-89：prepared + tool 替身；`findMatch` 恒不命中（本库为空表，天然如此）。
    let prepared = PreparedOperation {
        subject,
        descriptor: operation,
        execution_attempt_id: "attempt".to_owned(),
    };
    let tool = FakeTool::new(tool_name);
    let input = json!({});
    let frozen = harness.freeze(tool_name, &input);

    // L101-108：链路必然抛错（RS-02），prompt 在抛错前已落库。
    let failure = harness
        .service
        .authorize_prepared(
            &tool,
            &frozen,
            input.clone(),
            &harness.context("run", "tool-use", "session"),
            prepared,
        )
        .await
        .expect_err("no user answers the prompt");
    assert!(
        !failure.code.is_empty(),
        "未回答的交互必须以稳定错误码收尾，实际 {failure:?}"
    );

    // L109：取 `createAuthorization` 的 prompt 实参。
    let created = harness.gateway.created.lock().expect("created lock");
    let spec = created.first().expect("prompt captured");
    (spec.prompt.clone(), spec.scope_options.clone())
}

/// 旧源 `...RememberScopeDescriptionTest.java:29-38`
/// `networkGrantScopesDescribeToolWideChangingInputs`。
#[tokio::test]
async fn network_grant_scopes_describe_tool_wide_changing_inputs() {
    // L31-32
    let (prompt, scopes) = capture_prompt("WebFetch", "network-v1", RiskClass::Guarded).await;
    assert_eq!(
        scopes,
        vec![
            PermissionScope::Run.lowercase(),
            PermissionScope::Session.lowercase()
        ],
        "RS-01：判定格必须是 [RUN, SESSION]"
    );

    // L34-37
    assert_eq!(
        prompt
            .get("rememberScopeDescription")
            .and_then(Value::as_str),
        Some(
            "Saved permission applies to WebFetch only; URL and input values may change. \
             Other network tools remain separate. Run/session limits follow the selected option, \
             and saved grants expire within 12 hours."
        )
    );
}

/// 旧源 `...RememberScopeDescriptionTest.java:40-49`
/// `mcpGrantScopesDescribeToolWideChangingInputs`。
#[tokio::test]
async fn mcp_grant_scopes_describe_tool_wide_changing_inputs() {
    // L42-43
    let (prompt, scopes) = capture_prompt("mcp__search__query", "mcp-v1", RiskClass::Guarded).await;
    assert_eq!(
        scopes,
        vec![
            PermissionScope::Run.lowercase(),
            PermissionScope::Session.lowercase()
        ],
        "RS-01：判定格必须是 [RUN, SESSION]"
    );

    // L45-48
    assert_eq!(
        prompt
            .get("rememberScopeDescription")
            .and_then(Value::as_str),
        Some(
            "Saved permission applies to MCP tool mcp__search__query only; input values may \
             change. Other MCP tools remain separate. Run/session limits follow the selected \
             option, and saved grants expire within 12 hours."
        )
    );
}

/// 旧源 `...RememberScopeDescriptionTest.java:51-57`
/// `nonRemoteAnalyzerDoesNotAddRememberScopeDescription`。
///
/// RS-01：旧测试用 `("Agent","static-or-remote-v1",[RUN])`——「非远程分析器 +
/// 非空 scope 列表」。Rust 侧等价组合为 `bash-v2` + GUARDED（真实回
/// `[RUN, SESSION]`，同属非远程），判定格「scopes 非空但无文案」不变。
#[tokio::test]
async fn non_remote_analyzer_does_not_add_remember_scope_description() {
    // L53-54
    let (prompt, scopes) = capture_prompt("Agent", "bash-v2", RiskClass::Guarded).await;
    assert!(!scopes.is_empty(), "RS-01：判定格要求 scope 列表非空");

    // L56
    assert!(
        !prompt.contains_key("rememberScopeDescription"),
        "非远程分析器不得带可记住范围文案"
    );
}

/// 旧源 `...RememberScopeDescriptionTest.java:59-64`
/// `onceOnlyRemoteOperationDoesNotAddRememberScopeDescription`。
///
/// RS-01：旧测试注入空 scope 列表表达「仅本次」。Rust 侧等价组合为
/// `network-v1` + HIGH——`supported_scopes` 对 HIGH 恒回 `[]`
/// （`grants.rs:570-572`，对齐旧 `PermissionGrantRepository.java:186-188`）。
#[tokio::test]
async fn once_only_remote_operation_does_not_add_remember_scope_description() {
    // L61
    let (prompt, scopes) = capture_prompt("WebFetch", "network-v1", RiskClass::High).await;
    assert!(scopes.is_empty(), "RS-01：判定格要求 scope 列表为空");

    // L63
    assert!(
        !prompt.contains_key("rememberScopeDescription"),
        "仅本次的远程操作不得带可记住范围文案"
    );
}
