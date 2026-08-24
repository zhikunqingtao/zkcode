//! 授权诊断载荷。
//!
//! 逐字移植 `authorization/AuthorizationDiagnostic.java`（69 行）。字段顺序与
//! 旧 `LinkedHashMap` 插入顺序一致：`serde_json::Map` 在启用 `preserve_order`
//! 时保序；未启用时旧序仅影响可读性、不影响任何判定，故不构成偏离。

use serde_json::{Map, Value};

use crate::model::{
    AuthorizationSubject, DiagnosticOutcome, DiagnosticSource, EvaluationStage,
    OperationDescriptor, PermissionScope,
};
use crate::tool_facts::ToolUseContext;

/// 构造稳定、无秘密的授权诊断载荷（旧 `payload(...)`，`AuthorizationDiagnostic.java:17-47`）。
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn payload(
    subject: &AuthorizationSubject,
    operation: &OperationDescriptor,
    context: &ToolUseContext,
    execution_attempt_id: &str,
    outcome: DiagnosticOutcome,
    stage: EvaluationStage,
    source: DiagnosticSource,
    reason_code: &str,
    grant_id: Option<&str>,
    grant_scope: Option<PermissionScope>,
    interaction_id: Option<&str>,
) -> Value {
    let mut data = Map::new();
    data.insert("outcome".into(), outcome.as_str().into());
    data.insert("evaluationStage".into(), stage.as_str().into());
    data.insert("authorizationSource".into(), source.as_str().into());
    data.insert("reasonCode".into(), reason_code.into());
    data.insert("rootRunId".into(), subject.root_run_id.clone().into());
    data.insert("currentRunId".into(), subject.current_run_id.clone().into());
    data.insert(
        "toolUseId".into(),
        context.tool_use_id.clone().unwrap_or_default().into(),
    );
    data.insert("executionAttemptId".into(), execution_attempt_id.into());
    data.insert("toolName".into(), operation.tool_name.clone().into());
    data.insert("analyzerId".into(), operation.analyzer_id.clone().into());
    data.insert("risk".into(), operation.risk.as_str().into());
    data.insert("inputHash".into(), operation.input_hash.clone().into());
    data.insert(
        "operationHash".into(),
        operation.operation_hash.clone().into(),
    );
    data.insert(
        "redactedSummary".into(),
        operation.redacted_summary.clone().into(),
    );
    if let Some(grant_id) = grant_id {
        data.insert("grantId".into(), grant_id.into());
    }
    if let Some(scope) = grant_scope {
        data.insert("grantScope".into(), scope.as_str().into());
    }
    if let Some(interaction_id) = interaction_id {
        data.insert("interactionId".into(), interaction_id.into());
    }
    Value::Object(data)
}

/// 分析阶段失败的诊断载荷（旧 `analysisFailure(...)`，`AuthorizationDiagnostic.java:49-67`）。
///
/// 注意：旧源此处不含 `analyzerId` / `risk` / `operationHash` / `redactedSummary`
/// ——分析尚未产出 descriptor，逐字保持缺省。
#[must_use]
pub fn analysis_failure(
    subject: &AuthorizationSubject,
    tool_name: &str,
    input_hash: &str,
    context: &ToolUseContext,
    execution_attempt_id: &str,
    reason_code: &str,
) -> Value {
    let mut data = Map::new();
    data.insert("outcome".into(), DiagnosticOutcome::Deny.as_str().into());
    data.insert(
        "evaluationStage".into(),
        EvaluationStage::Initial.as_str().into(),
    );
    data.insert(
        "authorizationSource".into(),
        DiagnosticSource::Invariant.as_str().into(),
    );
    data.insert("reasonCode".into(), reason_code.into());
    data.insert("rootRunId".into(), subject.root_run_id.clone().into());
    data.insert("currentRunId".into(), subject.current_run_id.clone().into());
    data.insert(
        "toolUseId".into(),
        context.tool_use_id.clone().unwrap_or_default().into(),
    );
    data.insert("executionAttemptId".into(), execution_attempt_id.into());
    data.insert("toolName".into(), tool_name.into());
    data.insert("inputHash".into(), input_hash.into());
    Value::Object(data)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::model::RiskClass;

    fn subject() -> AuthorizationSubject {
        AuthorizationSubject {
            root_session_id: "s1".into(),
            root_run_id: "r1".into(),
            current_run_id: "r1".into(),
            workspace_key: "wk".into(),
            authorization_root: PathBuf::from("/tmp/ws"),
        }
    }

    fn descriptor() -> OperationDescriptor {
        OperationDescriptor {
            authorization_schema_version: 1,
            tool_name: "Read".into(),
            action: "READ_FILE".into(),
            input_hash: "ih".into(),
            analyzer_id: "file-v1".into(),
            effects: Vec::new(),
            resources: Vec::new(),
            inherited_environment_names: Vec::new(),
            endpoints: Vec::new(),
            risk: RiskClass::Safe,
            operation_hash: "oh".into(),
            redacted_summary: "Read a.txt".into(),
        }
    }

    /// 旧源 `AuthorizationDiagnosticTest.java:12-27`：完整载荷键集与取值。
    #[test]
    fn payload_carries_stable_authorization_facts() {
        let value = payload(
            &subject(),
            &descriptor(),
            &ToolUseContext::new(Some("r1".into()), Some("tu1".into()), Some("s1".into())),
            "att1",
            DiagnosticOutcome::Allow,
            EvaluationStage::Initial,
            DiagnosticSource::Grant,
            "GRANT_MATCH",
            Some("g1"),
            Some(PermissionScope::Session),
            Some("i1"),
        );
        assert_eq!(value["outcome"], "ALLOW");
        assert_eq!(value["evaluationStage"], "INITIAL");
        assert_eq!(value["authorizationSource"], "GRANT");
        assert_eq!(value["reasonCode"], "GRANT_MATCH");
        assert_eq!(value["toolUseId"], "tu1");
        assert_eq!(value["grantId"], "g1");
        assert_eq!(value["grantScope"], "SESSION");
        assert_eq!(value["interactionId"], "i1");
    }

    /// 旧源 `AuthorizationDiagnosticTest.java:29-40`：可选字段缺省时不出现在载荷中。
    #[test]
    fn payload_omits_absent_optional_facts() {
        let value = payload(
            &subject(),
            &descriptor(),
            &ToolUseContext::default(),
            "att1",
            DiagnosticOutcome::Deny,
            EvaluationStage::FinalRecheck,
            DiagnosticSource::Invariant,
            "AUTHORIZATION_FINAL_RECHECK_DENIED",
            None,
            None,
            None,
        );
        assert_eq!(value["toolUseId"], "");
        assert!(value.get("grantId").is_none());
        assert!(value.get("grantScope").is_none());
        assert!(value.get("interactionId").is_none());
    }

    /// 旧源 `AuthorizationDiagnostic.java:49-67`：分析失败载荷不含 descriptor 派生字段。
    #[test]
    fn analysis_failure_omits_descriptor_facts() {
        let value = analysis_failure(
            &subject(),
            "Bash",
            "ih",
            &ToolUseContext::default(),
            "att1",
            "AUTHORIZATION_ANALYSIS_DENIED",
        );
        assert_eq!(value["outcome"], "DENY");
        assert_eq!(value["authorizationSource"], "INVARIANT");
        assert_eq!(value["toolName"], "Bash");
        assert!(value.get("analyzerId").is_none());
        assert!(value.get("operationHash").is_none());
    }
}
