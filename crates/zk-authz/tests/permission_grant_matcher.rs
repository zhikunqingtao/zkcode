//! 旧 `authorization/PermissionGrantMatcherTest.java`（42 行）逐条翻译。
//!
//! | 旧 `@Test` | 旧源行号 | 本文件 |
//! |---|---|---|
//! | `prefixMatchingUsesPathSegmentsAndChecksEveryResource` | L11-22 | [`prefix_matching_uses_path_segments_and_checks_every_resource`] |
//! | `constraintsRejectTraversalAbsoluteAndCrossClassOperations` | L24-35 | [`constraints_reject_traversal_absolute_and_cross_class_operations`] |
//!
//! 唯一形状差异：旧构造器用 `IllegalArgumentException` 表达非法约束，Rust 侧改
//! `Result<_, ConstraintError>`（`unsafe_code = forbid` 下不存在异常机制；语义
//! 等价——非法输入不可能构造出约束值）。

use zk_authz::constraint::{self, ConstraintError, GrantConstraint};
use zk_authz::model::{
    EffectClass, OperationDescriptor, ResourceRef, RiskClass, TypedFileOperation,
};

/// 旧源私有工厂 `operation(action, resources...)`（`PermissionGrantMatcherTest.java:37-41`）。
fn operation(action: TypedFileOperation, resources: Vec<ResourceRef>) -> OperationDescriptor {
    OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: "Read".to_owned(),
        action: action.as_str().to_owned(),
        input_hash: "input".to_owned(),
        analyzer_id: "file-v1".to_owned(),
        effects: vec![EffectClass::ReadResource],
        resources,
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk: RiskClass::Safe,
        operation_hash: "operation".to_owned(),
        redacted_summary: "read".to_owned(),
    }
}

/// 旧源 `PermissionGrantMatcherTest.java:11-22`。
#[test]
fn prefix_matching_uses_path_segments_and_checks_every_resource() {
    // 旧源 L13-14
    let read = GrantConstraint::workspace_read(
        &["src/main".to_owned()],
        vec![TypedFileOperation::ReadFile],
    )
    .expect("valid read constraint");
    // 旧源 L15-16：前缀命中。
    assert!(constraint::matches(
        &read,
        &operation(
            TypedFileOperation::ReadFile,
            vec![ResourceRef::new("path", "src/main/App.java", false)]
        )
    ));
    // 旧源 L17-18：`src/main2` 不是 `src/main` 的路径段前缀。
    assert!(!constraint::matches(
        &read,
        &operation(
            TypedFileOperation::ReadFile,
            vec![ResourceRef::new("path", "src/main2/App.java", false)]
        )
    ));
    // 旧源 L19-21：任一资源不满足即整体不匹配。
    assert!(!constraint::matches(
        &read,
        &operation(
            TypedFileOperation::ReadFile,
            vec![
                ResourceRef::new("path", "src/main/App.java", false),
                ResourceRef::new("path", "test/Other.java", false),
            ]
        )
    ));
}

/// 旧源 `PermissionGrantMatcherTest.java:24-35`。
#[test]
fn constraints_reject_traversal_absolute_and_cross_class_operations() {
    // 旧源 L26-28：`..` 穿越。
    assert!(matches!(
        GrantConstraint::workspace_read(
            &["src/../secret".to_owned()],
            vec![TypedFileOperation::ReadFile]
        ),
        Err(ConstraintError::InvalidPrefix(_))
    ));
    // 旧源 L29-31：绝对路径。
    assert!(matches!(
        GrantConstraint::workspace_read(&["/tmp".to_owned()], vec![TypedFileOperation::ReadFile]),
        Err(ConstraintError::InvalidPrefix(_))
    ));
    // 旧源 L32-34：写约束不得携带读类操作。
    assert!(matches!(
        GrantConstraint::workspace_edit(&["src".to_owned()], vec![TypedFileOperation::ReadFile]),
        Err(ConstraintError::OperationMismatch)
    ));
}
