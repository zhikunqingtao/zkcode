//! 旧 `authorization/AuthorizationDiagnosticTest.java`（41 行）逐条翻译。
//!
//! | 旧 `@Test` | 旧源行号 | 本文件 |
//! |---|---|---|
//! | `allowDiagnosticContainsStableCorrelationAndNoRawInput` | L13-40 | [`allow_diagnostic_contains_stable_correlation_and_no_raw_input`] |

use std::path::PathBuf;

use zk_authz::diagnostic;
use zk_authz::model::{
    AuthorizationSubject, DiagnosticOutcome, DiagnosticSource, EffectClass, EvaluationStage,
    OperationDescriptor, PermissionScope, ResourceRef, RiskClass,
};
use zk_authz::tool_facts::ToolUseContext;

/// 旧源 `AuthorizationDiagnosticTest.java:13-40`。
#[test]
fn allow_diagnostic_contains_stable_correlation_and_no_raw_input() {
    // 旧源 L15-16
    let subject = AuthorizationSubject {
        root_session_id: "session-root".to_owned(),
        root_run_id: "run-root".to_owned(),
        current_run_id: "run-child".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: PathBuf::from("/workspace"),
    };
    // 旧源 L17-21
    let operation = OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: "Bash".to_owned(),
        action: "execute".to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: "bash-v1".to_owned(),
        effects: vec![EffectClass::Process, EffectClass::ReadResource],
        resources: vec![ResourceRef::new("cwd", ".", false)],
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk: RiskClass::Guarded,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "ls <redacted>".to_owned(),
    };
    // 旧源 L22-23：`ToolUseContext.of(cwd, sessionId).withCurrentRunId(..).withToolUseId(..)`
    let context = ToolUseContext::new(
        Some("run-child".to_owned()),
        Some("tool-use".to_owned()),
        None,
    )
    .with_shell(
        Some("synthetic-child".to_owned()),
        Some("/workspace".to_owned()),
    );

    // 旧源 L25-29
    let payload = diagnostic::payload(
        &subject,
        &operation,
        &context,
        "attempt-1",
        DiagnosticOutcome::Allow,
        EvaluationStage::FinalRecheck,
        DiagnosticSource::Grant,
        "GRANT_MATCH",
        Some("grant-1"),
        Some(PermissionScope::Session),
        Some("interaction-1"),
    );

    // 旧源 L31-37
    assert_eq!(payload["rootRunId"], "run-root");
    assert_eq!(payload["currentRunId"], "run-child");
    assert_eq!(payload["toolUseId"], "tool-use");
    assert_eq!(payload["executionAttemptId"], "attempt-1");
    assert_eq!(payload["authorizationSource"], "GRANT");
    assert_eq!(payload["evaluationStage"], "FINAL_RECHECK");
    assert_eq!(payload["grantScope"], "SESSION");
    // 旧源 L38：原始入参三键永不出现。
    let object = payload
        .as_object()
        .expect("diagnostic payload is an object");
    assert!(!object.contains_key("input"));
    assert!(!object.contains_key("command"));
    assert!(!object.contains_key("canonicalJson"));
    // 旧源 L39
    assert!(!payload.to_string().contains("secret-value"));
}
