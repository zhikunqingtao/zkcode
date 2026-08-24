//! 授权记录约束的封闭代数与匹配规则。
//!
//! 逐字对照 `authorization/GrantConstraint.java`（L1-40）与
//! `authorization/PermissionGrantMatcher.java`（L1-65）。匹配为**封闭**判定：
//! 任何无法识别的约束一律不匹配，绝不隐式放行。

use serde::{Deserialize, Serialize};

use crate::model::{OperationDescriptor, TypedFileOperation};

/// 相对目录前缀规范化失败的原因。
///
/// 对照 `GrantConstraint.java:44-63` 抛出的 `IllegalArgumentException` 各分支。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConstraintError {
    /// 前缀为空、含 NUL、含反斜杠、绝对路径、含空段或 `.`/`..` 段等。
    #[error("invalid relative directory prefix: {0}")]
    InvalidPrefix(String),
    /// 前缀集合为空（`normalized()` 要求至少一项）。
    #[error("relative directory prefixes must not be empty")]
    EmptyPrefixes,
    /// 允许操作集合与约束语义不符（read 约束含写操作，或 edit 约束含读操作）。
    #[error("allowed operations do not match the constraint kind")]
    OperationMismatch,
}

/// 授权记录的封闭约束代数（旧 sealed interface 的 4 个 permit）。
///
/// 对照 `GrantConstraint.java:9-18`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrantConstraint {
    /// 精确操作哈希绑定。对照 `GrantConstraint.java:10`。
    Exact {
        /// 授权时冻结的 `operation_hash`。
        operation_hash: String,
    },
    /// 工具级放行（Bash / 远程能力）。对照 `GrantConstraint.java:11`。
    ToolWide,
    /// 工作区读能力。对照 `GrantConstraint.java:12-14`。
    WorkspaceRead {
        /// 规范化后的相对目录前缀（去重升序）。
        relative_directory_prefixes: Vec<String>,
        /// 允许的类型化文件操作。
        allowed_operations: Vec<TypedFileOperation>,
    },
    /// 工作区写能力。对照 `GrantConstraint.java:15-17`。
    WorkspaceEdit {
        /// 规范化后的相对目录前缀（去重升序）。
        relative_directory_prefixes: Vec<String>,
        /// 允许的类型化文件操作。
        allowed_operations: Vec<TypedFileOperation>,
    },
}

impl GrantConstraint {
    /// 构造 `WorkspaceRead`：允许操作必须全部是 `READ_FILE` / `LIST_DIRECTORY`。
    ///
    /// 对照 `GrantConstraint.java:12-14` 的紧凑构造器校验。
    ///
    /// # Errors
    /// 含写类操作时返回 [`ConstraintError::OperationMismatch`]；前缀集为空或任一
    /// 前缀不是规范相对路径时返回 [`ConstraintError::EmptyPrefixes`] /
    /// [`ConstraintError::InvalidPrefix`]。
    pub fn workspace_read(
        prefixes: &[String],
        operations: Vec<TypedFileOperation>,
    ) -> Result<Self, ConstraintError> {
        if !operations.iter().all(|op| {
            matches!(
                op,
                TypedFileOperation::ReadFile | TypedFileOperation::ListDirectory
            )
        }) {
            return Err(ConstraintError::OperationMismatch);
        }
        Ok(Self::WorkspaceRead {
            relative_directory_prefixes: normalized(prefixes)?,
            allowed_operations: operations,
        })
    }

    /// 构造 `WorkspaceEdit`：允许操作不得含 `READ_FILE` / `LIST_DIRECTORY`。
    ///
    /// 对照 `GrantConstraint.java:15-17` 的紧凑构造器校验。
    ///
    /// # Errors
    /// 含读类操作时返回 [`ConstraintError::OperationMismatch`]；前缀集为空或任一
    /// 前缀不是规范相对路径时返回 [`ConstraintError::EmptyPrefixes`] /
    /// [`ConstraintError::InvalidPrefix`]。
    pub fn workspace_edit(
        prefixes: &[String],
        operations: Vec<TypedFileOperation>,
    ) -> Result<Self, ConstraintError> {
        if operations.iter().any(|op| {
            matches!(
                op,
                TypedFileOperation::ReadFile | TypedFileOperation::ListDirectory
            )
        }) {
            return Err(ConstraintError::OperationMismatch);
        }
        Ok(Self::WorkspaceEdit {
            relative_directory_prefixes: normalized(prefixes)?,
            allowed_operations: operations,
        })
    }

    /// 约束落库时的 `grant_kind` 名称片段（用于 `capability_hash`）。
    #[must_use]
    pub const fn kind_hint(&self) -> &'static str {
        match self {
            Self::Exact { .. } => "EXACT",
            Self::ToolWide => "TOOL_WIDE",
            Self::WorkspaceRead { .. } => "WORKSPACE_READ",
            Self::WorkspaceEdit { .. } => "WORKSPACE_EDIT",
        }
    }
}

/// 相对目录前缀集合规范化：逐项校验 + 去重 + 升序，空集合视为非法。
///
/// 对照 `GrantConstraint.java:19-24`（`normalized()`）。
fn normalized(values: &[String]) -> Result<Vec<String>, ConstraintError> {
    let mut out: Vec<String> = Vec::with_capacity(values.len());
    for value in values {
        let prefix = normalize_relative_path(value)?;
        if !out.contains(&prefix) {
            out.push(prefix);
        }
    }
    if out.is_empty() {
        return Err(ConstraintError::EmptyPrefixes);
    }
    out.sort_unstable();
    Ok(out)
}

/// 相对路径规范化的完整校验链。
///
/// 逐条对照 `GrantConstraint.java:44-63`：
/// 1. null / 空白 / 含 `\0` / 含 `\` → 非法
/// 2. `"."` 直接放行（工作区根）
/// 3. 以 `/` 开头、以 `/` 结尾、含 `//` → 非法
/// 4. 绝对路径 → 非法
/// 5. 任一段为空 / `.` / `..` → 非法
///
/// # Errors
/// 任一层校验不通过时返回 [`ConstraintError::InvalidPrefix`]。
pub fn normalize_relative_path(value: &str) -> Result<String, ConstraintError> {
    let invalid = || ConstraintError::InvalidPrefix(value.to_string());
    if value.trim().is_empty() || value.contains('\0') || value.contains('\\') {
        return Err(invalid());
    }
    if value == "." {
        return Ok(value.to_string());
    }
    if value.starts_with('/') || value.ends_with('/') || value.contains("//") {
        return Err(invalid());
    }
    for segment in value.split('/') {
        if segment.trim().is_empty() || segment == "." || segment == ".." {
            return Err(invalid());
        }
    }
    Ok(value.to_string())
}

/// 授权记录与当前操作的匹配判定（唯一入口）。
///
/// 对照 `PermissionGrantMatcher.java:10-18`。
#[must_use]
pub fn matches(constraint: &GrantConstraint, operation: &OperationDescriptor) -> bool {
    match constraint {
        GrantConstraint::Exact { operation_hash } => *operation_hash == operation.operation_hash,
        GrantConstraint::ToolWide => true,
        GrantConstraint::WorkspaceRead {
            relative_directory_prefixes,
            allowed_operations,
        }
        | GrantConstraint::WorkspaceEdit {
            relative_directory_prefixes,
            allowed_operations,
        } => matches_file(relative_directory_prefixes, allowed_operations, operation),
    }
}

/// 工作区能力与文件操作的逐项匹配。
///
/// 逐条对照 `PermissionGrantMatcher.java:20-34`：
/// 1. `action` 不是 `TypedFileOperation` → false
/// 2. `allowedOperations` 不含该操作 → false
/// 3. 资源为空 → false
/// 4. 任一资源 `outsideWorkspace` 或 `kind != "path"` → false
/// 5. 任一资源不落在任一前缀下 → false
fn matches_file(
    prefixes: &[String],
    allowed: &[TypedFileOperation],
    operation: &OperationDescriptor,
) -> bool {
    let Some(action) = TypedFileOperation::parse(&operation.action) else {
        return false;
    };
    if !allowed.contains(&action) {
        return false;
    }
    if operation.resources.is_empty() {
        return false;
    }
    operation.resources.iter().all(|resource| {
        !resource.outside_workspace
            && resource.kind == "path"
            && prefixes
                .iter()
                .any(|prefix| segment_contains(prefix, &resource.value))
    })
}

/// 目录前缀的段级包含判定。
///
/// 对照 `PermissionGrantMatcher.java:36-42`：`"."` 覆盖全工作区；否则要求相等或
/// `candidate` 以 `prefix + "/"` 开头（**段边界**匹配，避免 `src` 命中 `srcx`）。
#[must_use]
pub fn segment_contains(prefix: &str, candidate: &str) -> bool {
    prefix == "." || prefix == candidate || candidate.starts_with(&format!("{prefix}/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EffectClass, RiskClass};

    fn operation(action: &str, resources: Vec<crate::model::ResourceRef>) -> OperationDescriptor {
        OperationDescriptor {
            authorization_schema_version: 1,
            tool_name: "Read".to_string(),
            action: action.to_string(),
            input_hash: "input".to_string(),
            analyzer_id: "file-v1".to_string(),
            effects: vec![EffectClass::ReadResource],
            resources,
            inherited_environment_names: Vec::new(),
            endpoints: Vec::new(),
            risk: RiskClass::Safe,
            operation_hash: "hash".to_string(),
            redacted_summary: "summary".to_string(),
        }
    }

    /// 旧源 `PermissionGrantMatcherTest.java:14-19`：Exact 只在哈希相等时匹配。
    #[test]
    fn exact_constraint_matches_only_the_frozen_operation_hash() {
        let exact = GrantConstraint::Exact {
            operation_hash: "hash".to_string(),
        };
        assert!(matches(&exact, &operation("READ_FILE", vec![])));
        let other = GrantConstraint::Exact {
            operation_hash: "different".to_string(),
        };
        assert!(!matches(&other, &operation("READ_FILE", vec![])));
    }

    /// 旧源 `PermissionGrantMatcherTest.java:21-26`：`ToolWide` 恒匹配。
    #[test]
    fn tool_wide_constraint_always_matches() {
        assert!(matches(
            &GrantConstraint::ToolWide,
            &operation("execute", vec![])
        ));
    }

    /// 旧源 `PermissionGrantMatcher.java:36-42`：段边界匹配，`src` 不得命中 `srcx`。
    #[test]
    fn segment_contains_requires_a_path_separator_boundary() {
        assert!(segment_contains("src", "src"));
        assert!(segment_contains("src", "src/main.rs"));
        assert!(!segment_contains("src", "srcx/main.rs"));
        assert!(segment_contains(".", "anything/at/all"));
    }

    /// 旧源 `PermissionGrantMatcher.java:26-31`：区外资源永不匹配工作区能力。
    #[test]
    fn workspace_constraint_rejects_outside_workspace_resources() {
        let constraint =
            GrantConstraint::workspace_read(&[".".to_string()], vec![TypedFileOperation::ReadFile])
                .unwrap();
        let outside = operation(
            "READ_FILE",
            vec![crate::model::ResourceRef::new("path", "/etc/passwd", true)],
        );
        assert!(!matches(&constraint, &outside));
    }

    /// 旧源 `GrantConstraint.java:12-14`：read 约束不得含写操作。
    #[test]
    fn workspace_read_rejects_write_operations() {
        assert_eq!(
            GrantConstraint::workspace_read(
                &[".".to_string()],
                vec![TypedFileOperation::PatchFile]
            ),
            Err(ConstraintError::OperationMismatch)
        );
    }

    /// 旧源 `GrantConstraint.java:15-17`：edit 约束不得含读操作。
    #[test]
    fn workspace_edit_rejects_read_operations() {
        assert_eq!(
            GrantConstraint::workspace_edit(&[".".to_string()], vec![TypedFileOperation::ReadFile]),
            Err(ConstraintError::OperationMismatch)
        );
    }

    /// 旧源 `GrantConstraint.java:44-63`：绝对路径 / 遍历段 / 反斜杠 / NUL 全部拒绝。
    #[test]
    fn normalize_relative_path_rejects_every_escape_shape() {
        for bad in [
            "/abs", "a/", "a//b", "..", "a/../b", "a\\b", "a\0b", "", "  ", "a/./b",
        ] {
            assert!(
                normalize_relative_path(bad).is_err(),
                "expected reject: {bad:?}"
            );
        }
        assert_eq!(normalize_relative_path(".").unwrap(), ".");
        assert_eq!(normalize_relative_path("src/main").unwrap(), "src/main");
    }

    /// 旧源 `GrantConstraint.java:19-24`：前缀去重升序，空集合非法。
    #[test]
    fn prefixes_are_deduplicated_sorted_and_non_empty() {
        let constraint = GrantConstraint::workspace_read(
            &["b".to_string(), "a".to_string(), "b".to_string()],
            vec![TypedFileOperation::ReadFile],
        )
        .unwrap();
        match constraint {
            GrantConstraint::WorkspaceRead {
                relative_directory_prefixes,
                ..
            } => {
                assert_eq!(
                    relative_directory_prefixes,
                    vec!["a".to_string(), "b".to_string()]
                );
            }
            other => panic!("unexpected constraint: {other:?}"),
        }
        assert_eq!(
            GrantConstraint::workspace_read(&[], vec![TypedFileOperation::ReadFile]),
            Err(ConstraintError::EmptyPrefixes)
        );
    }
}
