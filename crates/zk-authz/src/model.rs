//! 授权事实的封闭枚举与不可变描述符（逐字对照旧 `authorization/` 包的
//! enum / record 家族）。
//!
//! 旧源锚点：`RiskClass.java` L3、`GrantKind.java` L3、`EffectClass.java`
//! L4-6、`DelegationPolicy.java` L3、`TypedFileOperation.java` L4-6、
//! `ResourceRef.java` L4-9、`OperationDescriptor.java` L1-27、
//! `AuthorizationSubject.java` L6-12、`PreparedOperation.java` L4-5、
//! `AuthorizedOperation.java` L7-10、`AuthorizationException.java` L3-8、
//! `model/PermissionScope.java`、`model/PermissionMode.java`。

use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// 宏：为「Java enum 名 ↔ 字符串」双向映射生成 `as_str` / `parse` / `Display`。
///
/// 旧 Java 侧统一用 `Enum::name()` 落库与入哈希，`valueOf` 解析且非法值抛异常；
/// 本宏把该语义一次性固化，避免每个枚举手写两份 match 造成漂移。
macro_rules! java_enum {
    (
        $(#[$meta:meta])*
        $name:ident { $( $(#[$vmeta:meta])* $variant:ident = $text:literal ),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(
                $(#[$vmeta])*
                #[doc = concat!("旧 Java 枚举常量 `", $text, "`。")]
                #[serde(rename = $text)]
                $variant,
            )+
        }

        impl $name {
            /// 旧 `Enum::name()` 的等价物（落库值 / 哈希事实值）。
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $( Self::$variant => $text, )+ }
            }

            /// 旧 `Enum.valueOf` 的等价物；未知字面量返回 `None`（调用方失败关闭）。
            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                match value { $( $text => Some(Self::$variant), )+ _ => None }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

java_enum! {
    /// 风险分级（旧 `RiskClass.java` L3：`SAFE, GUARDED, HIGH`）。
    ///
    /// 注意：旧基线**没有** `RiskLevel` 类型，权限管线全程只用 `RiskClass`；
    /// 下行协议的 `riskLevel` 字段取 `risk.name().toLowerCase()`
    ///（`AuthorizationService.java` L380）。
    RiskClass { Safe = "SAFE", Guarded = "GUARDED", High = "HIGH" }
}

impl RiskClass {
    /// 风险单调序（旧 `FileAnalyzer.riskRank`，`OperationAnalyzerRegistry.java`
    /// L448-454）：SAFE=0 / GUARDED=1 / HIGH=2。
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Safe => 0,
            Self::Guarded => 1,
            Self::High => 2,
        }
    }
}

java_enum! {
    /// 授权记录种类（旧 `GrantKind.java` L3）。
    GrantKind {
        ExactGuarded = "EXACT_GUARDED",
        ToolGuarded = "TOOL_GUARDED",
        ReadCapability = "READ_CAPABILITY",
        EditCapability = "EDIT_CAPABILITY",
    }
}

java_enum! {
    /// 副作用分类（旧 `EffectClass.java` L4-6）。
    ///
    /// **变体声明序即 Java 声明序**，但规范化排序取 `Enum::name()` 字典序
    ///（`AuthorizationFactCanonicalizer.effects` 用
    /// `Comparator.comparing(Enum::name)`），故派生的 `Ord` 不可直接用于
    /// 规范化——见 [`crate::canonicalizer::effects`]。
    EffectClass {
        SafeInternal = "SAFE_INTERNAL",
        ReadResource = "READ_RESOURCE",
        WriteResource = "WRITE_RESOURCE",
        Process = "PROCESS",
        Network = "NETWORK",
        ControlPlane = "CONTROL_PLANE",
        Unknown = "UNKNOWN",
    }
}

java_enum! {
    /// 委派策略（旧 `DelegationPolicy.java` L3）。
    DelegationPolicy {
        DirectOnly = "DIRECT_ONLY",
        RootAndDescendants = "ROOT_AND_DESCENDANTS",
    }
}

java_enum! {
    /// 封闭文件能力操作（旧 `TypedFileOperation.java` L4-6）。
    ///
    /// 变体序 = Java 声明序，`GrantConstraint` 的 `allowedOperations` 排序用
    /// `Comparable`（即声明序），故此处派生 `Ord` 与旧语义一致。
    TypedFileOperation {
        ReadFile = "READ_FILE",
        ListDirectory = "LIST_DIRECTORY",
        CreateFile = "CREATE_FILE",
        PatchFile = "PATCH_FILE",
        ReplaceFile = "REPLACE_FILE",
        DeleteFile = "DELETE_FILE",
    }
}

java_enum! {
    /// 授权范围（旧 `model/PermissionScope.java`）。
    ///
    /// 变体序 = Java 声明序（`ONCE, RUN, SESSION, WORKSPACE`）。
    PermissionScope {
        Once = "ONCE",
        Run = "RUN",
        Session = "SESSION",
        Workspace = "WORKSPACE",
    }
}

impl PermissionScope {
    /// 交互 prompt / 协议下行使用的小写形态（旧
    /// `scope.name().toLowerCase()`，`AuthorizationService.java` L395、L461）。
    #[must_use]
    pub fn lowercase(self) -> String {
        self.as_str().to_ascii_lowercase()
    }
}

java_enum! {
    /// 权限模式（旧 `model/PermissionMode.java`）。
    PermissionMode {
        Default = "DEFAULT",
        Plan = "PLAN",
        AcceptEdits = "ACCEPT_EDITS",
        DontAsk = "DONT_ASK",
        AutoApprove = "AUTO_APPROVE",
    }
}

/// 已规范化且不含秘密的资源事实（旧 `ResourceRef.java` L4-9）。
///
/// 字段名与 Jackson 序列化形状一致（`kind` / `value` / `outsideWorkspace`），
/// 入 `operationHash` 时按 key 字典序重排为 `kind, outsideWorkspace, value`。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRef {
    /// 资源族（`path` / `cwd`）。
    pub kind: String,
    /// 规范化后的资源值（工作区内为相对路径，区外为绝对路径）。
    pub value: String,
    /// 是否位于授权根之外。
    pub outside_workspace: bool,
}

impl ResourceRef {
    /// 构造资源事实。
    #[must_use]
    pub fn new(kind: impl Into<String>, value: impl Into<String>, outside_workspace: bool) -> Self {
        Self {
            kind: kind.into(),
            value: value.into(),
            outside_workspace,
        }
    }
}

/// 稳定授权身份描述符（旧 `OperationDescriptor.java` L1-27）。
///
/// 旧 record 字段序：`authorizationSchemaVersion, toolName, action, inputHash,
/// analyzerId, effects, resources, inheritedEnvironmentNames, endpoints, risk,
/// operationHash, redactedSummary`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationDescriptor {
    /// 授权 schema 版本（恒 1；DB `CHECK(authorization_schema_version=1)`）。
    pub authorization_schema_version: i64,
    /// 工具稳定名。
    pub tool_name: String,
    /// 语义动作（`execute` / `network` / `invoke` / `internal` /
    /// `TypedFileOperation::as_str` / `publish-public-artifact`）。
    pub action: String,
    /// 冻结规范输入哈希。
    pub input_hash: String,
    /// 分析器 id。
    pub analyzer_id: String,
    /// 规范化副作用集。
    pub effects: Vec<EffectClass>,
    /// 规范化资源集。
    pub resources: Vec<ResourceRef>,
    /// 规范化继承环境变量名集。
    pub inherited_environment_names: Vec<String>,
    /// 规范化脱敏端点集。
    pub endpoints: Vec<String>,
    /// 风险分级。
    pub risk: RiskClass,
    /// 授权身份哈希。
    pub operation_hash: String,
    /// 脱敏摘要（前端展示用，绝不含原始入参）。
    pub redacted_summary: String,
}

/// 仅由持久化 Run/会话状态推导的可信授权主体（旧
/// `AuthorizationSubject.java` L6-12）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationSubject {
    /// 根会话 id。
    pub root_session_id: String,
    /// 根 run id。
    pub root_run_id: String,
    /// 当前 run id（子代理场景与 root 不同）。
    pub current_run_id: String,
    /// 稳定工作区身份键。
    pub workspace_key: String,
    /// 规范授权根（资源边界）。
    pub authorization_root: PathBuf,
}

/// 从策略钩子传到裁决的不可变分析结果（旧 `PreparedOperation.java` L4-5）。
#[derive(Debug, Clone)]
pub struct PreparedOperation {
    /// 授权主体。
    pub subject: AuthorizationSubject,
    /// 操作描述符。
    pub descriptor: OperationDescriptor,
    /// 本次执行尝试 id。
    pub execution_attempt_id: String,
}

/// 传给唯一执行网关的授权结果（旧 `AuthorizedOperation.java` L7-10）。
#[derive(Debug, Clone)]
pub struct AuthorizedOperation {
    /// 授权主体。
    pub subject: AuthorizationSubject,
    /// 操作描述符。
    pub descriptor: OperationDescriptor,
    /// 绑定后的执行入参（file-v1 已把 path 字段替换为规范目标）。
    pub execution_input: serde_json::Value,
    /// 裁决来源。
    pub source: DiagnosticSource,
    /// 稳定原因码。
    pub reason_code: String,
    /// 命中的授权记录 id。
    pub grant_id: Option<String>,
    /// 命中的授权范围。
    pub grant_scope: Option<PermissionScope>,
    /// 产生本次裁决的交互 id。
    pub interaction_id: Option<String>,
    /// 本次执行尝试 id。
    pub execution_attempt_id: String,
}

java_enum! {
    /// 诊断结果（旧 `AuthorizationDiagnostic.Outcome`，L11）。
    DiagnosticOutcome { Allow = "ALLOW", Ask = "ASK", Deny = "DENY" }
}

java_enum! {
    /// 判定阶段（旧 `AuthorizationDiagnostic.EvaluationStage`，L12）。
    EvaluationStage {
        Initial = "INITIAL",
        Interaction = "INTERACTION",
        FinalRecheck = "FINAL_RECHECK",
    }
}

java_enum! {
    /// 裁决来源（旧 `AuthorizationDiagnostic.Source`，L13）。
    DiagnosticSource {
        Invariant = "INVARIANT",
        Policy = "POLICY",
        Grant = "GRANT",
        UserOnce = "USER_ONCE",
        Mode = "MODE",
    }
}

/// 授权拒绝/失败（旧 `AuthorizationException.java` L3-8：`code` + `message`）。
#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct AuthzError {
    /// 稳定错误码（前端与留痕依赖，逐字对齐旧 `code()`）。
    pub code: String,
    /// 人读消息。
    pub message: String,
}

impl AuthzError {
    /// 构造授权错误。
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// 授权存储不可用时的稳定错误码。
///
/// 旧源 `PermissionGrantRepository` 的 `DataAccessException` 不是
/// `AuthorizationException`，会一路冒泡成 500；Rust 无非受检异常，`?` 必须有
/// 转换目标。本转换把存储故障归一为**拒绝**（失败关闭），语义强于旧源：旧源
/// 500 同样不会执行工具，故对安全不变量无削弱。
impl From<zk_db::DbError> for AuthzError {
    fn from(value: zk_db::DbError) -> Self {
        Self::new("AUTHORIZATION_STORE_UNAVAILABLE", value.to_string())
    }
}

/// 授权路径的统一结果别名。
pub type AuthzResult<T> = Result<T, AuthzError>;

#[cfg(test)]
mod tests {
    use super::{EffectClass, PermissionScope, RiskClass, TypedFileOperation};

    /// 旧 `RiskClass.java` L3：三值封闭枚举，字面量即落库值。
    #[test]
    fn risk_class_literals_match_java_enum_names() {
        assert_eq!(RiskClass::Safe.as_str(), "SAFE");
        assert_eq!(RiskClass::Guarded.as_str(), "GUARDED");
        assert_eq!(RiskClass::High.as_str(), "HIGH");
        assert_eq!(RiskClass::parse("GUARDED"), Some(RiskClass::Guarded));
        assert_eq!(RiskClass::parse("guarded"), None, "valueOf 大小写敏感");
    }

    /// 旧 `OperationAnalyzerRegistry.java` L448-454：riskRank 单调序。
    #[test]
    fn risk_rank_is_monotonic() {
        assert!(RiskClass::Safe.rank() < RiskClass::Guarded.rank());
        assert!(RiskClass::Guarded.rank() < RiskClass::High.rank());
    }

    /// 旧 `EffectClass.java` L4-6：7 个变体，字面量逐字对齐。
    #[test]
    fn effect_class_covers_all_seven_java_variants() {
        for (variant, text) in [
            (EffectClass::SafeInternal, "SAFE_INTERNAL"),
            (EffectClass::ReadResource, "READ_RESOURCE"),
            (EffectClass::WriteResource, "WRITE_RESOURCE"),
            (EffectClass::Process, "PROCESS"),
            (EffectClass::Network, "NETWORK"),
            (EffectClass::ControlPlane, "CONTROL_PLANE"),
            (EffectClass::Unknown, "UNKNOWN"),
        ] {
            assert_eq!(variant.as_str(), text);
            assert_eq!(EffectClass::parse(text), Some(variant));
        }
    }

    /// 旧 `AuthorizationService.java` L380 / L395：scope 小写化。
    #[test]
    fn permission_scope_lowercase_matches_prompt_shape() {
        assert_eq!(PermissionScope::Session.lowercase(), "session");
        assert_eq!(PermissionScope::Workspace.lowercase(), "workspace");
    }

    /// 旧 `TypedFileOperation.java` L4-6：声明序即 `Comparable` 序。
    #[test]
    fn typed_file_operation_declaration_order_is_comparable_order() {
        let mut ops = vec![
            TypedFileOperation::DeleteFile,
            TypedFileOperation::ReadFile,
            TypedFileOperation::PatchFile,
        ];
        ops.sort_unstable();
        assert_eq!(
            ops,
            [
                TypedFileOperation::ReadFile,
                TypedFileOperation::PatchFile,
                TypedFileOperation::DeleteFile,
            ]
        );
    }
}
