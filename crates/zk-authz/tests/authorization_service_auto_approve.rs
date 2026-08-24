//! `AuthorizationServiceAutoApproveTest.java`（118 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录，编号 AA-01）：
//! 旧测试用 Mockito `verify(grants, never()).findMatch(...)` 直接观测「授权记录
//! 匹配根本没被调用」。Rust 侧 `GrantStore` 是具体类型（非 trait），无法 spy；
//! 等价观测为 `grantId`/`grantScope` 恒 `None` 且弹窗数恒 0 —— 若判定真的进了
//! grant 匹配链，命中会写 `grantId`、未命中会一路走到 `interact()` 使弹窗数 >0，
//! 两者皆不发生即证明第 2 步 `AUTO_APPROVE` 短路生效。

mod common;

use common::{FakeTool, Harness};
use serde_json::json;
use zk_authz::model::{
    DiagnosticSource, EffectClass, OperationDescriptor, PermissionMode, PreparedOperation,
    ResourceRef, RiskClass,
};

/// 旧测试私有 `descriptor(...)`（`AuthorizationServiceAutoApproveTest.java:89-95`）。
fn descriptor(
    tool_name: &str,
    analyzer: &str,
    risk: RiskClass,
    resources: Vec<ResourceRef>,
) -> OperationDescriptor {
    OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: tool_name.to_owned(),
        action: "invoke".to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: analyzer.to_owned(),
        effects: vec![EffectClass::Process],
        resources,
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "summary".to_owned(),
    }
}

/// 旧测试私有 `Fixture.authorize(...)`（`AuthorizationServiceAutoApproveTest.java:103-116`）。
async fn authorize(
    harness: &Harness,
    operation: OperationDescriptor,
) -> zk_authz::model::AuthorizedOperation {
    let subject = zk_authz::model::AuthorizationSubject {
        root_session_id: "session".to_owned(),
        root_run_id: "run".to_owned(),
        current_run_id: "run".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: harness.workspace.clone(),
    };
    let tool = FakeTool::new(&operation.tool_name);
    let input = json!({});
    let frozen = harness.freeze(&operation.tool_name, &input);
    let prepared = PreparedOperation {
        subject,
        descriptor: operation,
        execution_attempt_id: "attempt".to_owned(),
    };
    harness
        .service
        .authorize_prepared(
            &tool,
            &frozen,
            input.clone(),
            &harness.context("run", "tool-use", "session"),
            prepared,
        )
        .await
        .expect("auto approve authorizes")
}

/// 旧源 `AuthorizationServiceAutoApproveTest.java:31-55`
/// `autoApprovesGuardedHighAndExternalOperationsWithoutInteractionOrGrantLookup`。
#[tokio::test]
async fn auto_approves_guarded_high_and_external_operations_without_interaction_or_grant_lookup() {
    // L33-41：四类操作（bash GUARDED / bash HIGH / 工作区外读 GUARDED / 网络 GUARDED）。
    let operations = vec![
        descriptor(
            "Bash",
            "bash-v2",
            RiskClass::Guarded,
            vec![ResourceRef::new("cwd", ".", false)],
        ),
        descriptor(
            "Bash",
            "bash-v2",
            RiskClass::High,
            vec![ResourceRef::new("cwd", ".", false)],
        ),
        descriptor(
            "Read",
            "file-v1",
            RiskClass::Guarded,
            vec![ResourceRef::new("path", "/outside/file.txt", true)],
        ),
        descriptor("WebFetch", "network-v1", RiskClass::Guarded, Vec::new()),
    ];

    for operation in operations {
        // L42：每轮一套全新协作者。
        let harness = Harness::new();
        harness.modes.set(PermissionMode::AutoApprove);
        let tool_name = operation.tool_name.clone();
        let risk = operation.risk;

        // L44
        let authorized = authorize(&harness, operation).await;

        // L46-50
        assert_eq!(
            authorized.source,
            DiagnosticSource::Mode,
            "{tool_name}/{risk:?} source"
        );
        assert_eq!(authorized.reason_code, "AUTO_APPROVE");
        assert!(authorized.grant_id.is_none());
        assert!(authorized.grant_scope.is_none());
        assert!(authorized.interaction_id.is_none());

        // L51-53：既未查授权记录、也未建交互（等价观测见模块文档 AA-01）。
        assert_eq!(harness.gateway.prompt_count(), 0);
    }
}

/// 旧源 `AuthorizationServiceAutoApproveTest.java:57-69`
/// `safeInternalKeepsBuiltinPolicySemantics`。
#[tokio::test]
async fn safe_internal_keeps_builtin_policy_semantics() {
    // L59：同样是 AUTO_APPROVE 模式。
    let harness = Harness::new();
    harness.modes.set(PermissionMode::AutoApprove);

    // L60-63：SAFE_INTERNAL + SAFE。
    let operation = OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: "Internal".to_owned(),
        action: "invoke".to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: "generic-v1".to_owned(),
        effects: vec![EffectClass::SafeInternal],
        resources: Vec::new(),
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk: RiskClass::Safe,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "summary".to_owned(),
    };

    // L65-68：内建安全策略先于模式短路（决策链第 1 步）。
    let authorized = authorize(&harness, operation).await;
    assert_eq!(authorized.source, DiagnosticSource::Policy);
    assert_eq!(authorized.reason_code, "BUILTIN_SAFE");
}
