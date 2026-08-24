//! 授权管线（M7）与交互裁决（M-INT）的策略核心。
//!
//! 逐字移植自 zhikuncode main@581d407b 的 `com.aicodeassistant.authorization`
//! 与 `com.aicodeassistant.security` 两个包。本 crate 只承载**策略与判定**，
//! 不承载传输层：交互投递/等待经 [`interaction::InteractionGateway`] 反转到
//! zk-server，工具身份经 [`tool_facts::ToolFacts`] 反转到 zk-tools。
//!
//! 依赖方向铁律：`zk-authz → (zk-protocol, zk-db)`。不得反向依赖
//! zk-server / zk-engine / zk-tools。
//!
//! # 旧源映射
//!
//! | 本 crate 模块 | 旧源 |
//! |---|---|
//! | [`model`] | `authorization/{RiskClass,GrantKind,EffectClass,DelegationPolicy,TypedFileOperation,ResourceRef,OperationDescriptor,AuthorizationSubject,PreparedOperation,AuthorizedOperation,AuthorizationException}.java` |
//! | [`hashing`] | `authorization/OperationHashing.java`、`WorkspaceIdentityService` 与 `PermissionGrantRepository` 的 `hash()` |
//! | [`canonicalizer`] | `authorization/AuthorizationFactCanonicalizer.java` |
//! | [`constraint`] | `authorization/{GrantConstraint,PermissionGrantMatcher}.java` |
//! | [`frozen`] | `authorization/{FrozenToolInput,FrozenToolInputFactory}.java` |
//! | [`workspace`] | `authorization/WorkspaceIdentityService.java` |
//! | [`path_security`] | `security/{PathSecurityService,SystemScratchpadPathPolicy}.java` |
//! | [`sensitive`] | `security/SensitiveDataFilter.java` |
//! | [`diagnostic`] | `authorization/AuthorizationDiagnostic.java` |
//! | [`grants`] | `authorization/PermissionGrantRepository.java` |
//! | [`subject`] | `authorization/AuthorizationSubjectResolver.java` |
//! | [`analyzer`] | `authorization/OperationAnalyzerRegistry.java` + 6 个 `OperationAnalyzer` |
//! | [`interaction`] | `authorization/AuthorizationInteractionContext.java` + `interaction/InteractionRequest.java` |
//! | [`service`] | `authorization/AuthorizationService.java` |
//! | [`gateway`] | `authorization/ToolExecutionGateway.java` |
//! | [`tool_safety`] | `service/ToolSafetyGuard.java`（§环境安全层；路径/命令两层已由 [`path_security`] 与 zk-tools `bash::security` 更严覆盖） |

pub mod analyzer;
pub mod canonicalizer;
pub mod constraint;
pub mod diagnostic;
pub mod frozen;
pub mod gateway;
pub mod grants;
pub mod hashing;
pub mod interaction;
pub mod model;
pub mod path_security;
pub mod sensitive;
pub mod service;
pub mod subject;
pub mod tool_facts;
pub mod tool_safety;
pub mod workspace;

pub use constraint::GrantConstraint;
pub use frozen::{FrozenToolInput, FrozenToolInputFactory};
pub use model::{
    AuthorizationSubject, AuthorizedOperation, AuthzError, AuthzResult, DelegationPolicy,
    EffectClass, GrantKind, OperationDescriptor, PermissionMode, PermissionScope,
    PreparedOperation, ResourceRef, RiskClass, TypedFileOperation,
};
pub use service::AuthorizationService;
