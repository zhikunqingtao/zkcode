//! 工具执行准入端口（2.5 权限管线接入点）。
//!
//! 旧 `ToolExecutionPipeline.java` 的阶段 4/5 把「冻结入参 → `AuthorizationService`
//! `prepare` + `authorizePrepared` → `ToolExecutionGateway.execute`」串在工具真正
//! 执行之前；拒绝路径（`catch (AuthorizationException denied)`，L335-343）推
//! `tool_permission_denied` 下行并把 `ToolResult.permissionDenied(code, message)`
//! 回喂模型。
//!
//! 依赖方向铁律不允许 zk-engine 依赖 zk-authz（zk-authz 只可依赖 zk-protocol /
//! zk-db），故此处把准入抽成窄端口反转：zk-server 组装根实现本 trait，桥接
//! `AuthorizationService` + `ToolExecutionGateway`。
//!
//! 旧管线在工具执行前有 5 类 catch（`ToolExecutionPipeline.java:335-386`），按
//! 「是否推 `tool_permission_denied` 下行」二分：
//!
//! | 旧异常 | 旧源 | 推下行 | 本端口结局 |
//! |---|---|---|---|
//! | `AuthorizationException` | L335-343 | 是 | [`Admission::Denied`] |
//! | `AdmissionException` | L344-354 | 否 | [`Admission::Failed`] |
//! | `DatabaseWriteUnavailableException` | L355-368 | 否 | [`Admission::Failed`] |
//! | `InteractionOperationException` | L369-383 | 否 | [`Admission::Failed`] |
//! | `ToolInputValidationException` | L384-386 | 否 | [`Admission::Failed`] |
//!
//! 故引擎侧认三种结局：放行 + 最终入参、拒绝（推下行）、准入失败（不推下行）。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;

/// 一次工具调用的准入上下文（旧 `ToolUseContext` 中授权链实际读取的字段子集）。
#[derive(Debug, Clone, Copy)]
pub struct AdmissionRequest<'a> {
    /// 会话 ID（旧 `context.sessionId()`，同时是 root session）。
    pub session_id: &'a str,
    /// Run ID（旧 `context.currentRunId()`；授权链无 Run 即拒）。
    pub run_id: &'a str,
    /// 工具调用 ID（旧 `context.toolUseId()`）。
    pub tool_use_id: &'a str,
    /// 工具名（旧 `tool.getName()`）。
    pub tool_name: &'a str,
    /// 模型给出的原始入参（冻结前）。
    pub input: &'a Value,
    /// 工作目录（旧 `context.workingDirectory()`）。
    pub working_directory: Option<&'a str>,
}

/// 准入结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// 放行；`execution_input` 为授权链绑定后的最终入参
    /// （旧 `AuthorizedOperation.executionInput()`，可能被冻结/重写）。
    Allow {
        /// 最终执行入参。
        execution_input: Value,
    },
    /// 授权拒绝（旧 `catch (AuthorizationException denied)`，L335-343）：先推
    /// `tool_permission_denied` 下行，再以 `ToolResult.permissionDenied(code,
    /// message)` 回喂模型。
    Denied {
        /// 稳定错误码（如 `COMMAND_ABSOLUTELY_DENIED` / `PERMISSION_USER_DENIED`）。
        code: String,
        /// 回喂模型的文案（旧 `denied.getMessage()`）。
        message: String,
    },
    /// 准入失败（旧 `catch (AdmissionException | DatabaseWriteUnavailableException
    /// | InteractionOperationException | ToolInputValidationException)`，
    /// L344-386）：**不推**任何下行，只把 `ToolResult.failed(...)` 回喂模型。
    Failed {
        /// 稳定错误码（如 `AUTHORIZATION_STORE_UNAVAILABLE` /
        /// `INTERACTION_STORE_FAILED` / `INVALID_TOOL_INPUT`）。
        code: String,
        /// 回喂模型的文案（旧各 catch 的定文案）。
        message: String,
    },
}

impl Admission {
    /// 拒绝结局的稳定错误码（放行时为 `None`）。
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Allow { .. } => None,
            Self::Denied { code, .. } | Self::Failed { code, .. } => Some(code),
        }
    }
}

/// 工具执行准入端口。
pub trait ToolAdmission: Send + Sync {
    /// 在工具真正执行之前裁决。
    fn admit<'a>(&'a self, request: AdmissionRequest<'a>) -> BoxFuture<'a, Admission>;
}

/// 直通准入（无权限管线装配时的等价行为；2.3/2.4 的既有语义）。
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAllAdmission;

impl ToolAdmission for AllowAllAdmission {
    fn admit<'a>(&'a self, request: AdmissionRequest<'a>) -> BoxFuture<'a, Admission> {
        let execution_input = request.input.clone();
        Box::pin(async move { Admission::Allow { execution_input } })
    }
}

/// 权限模式切换端口——引擎检测工具 metadata 中的 `"mode"` 字段后调用。
///
/// 生产实现由 `zk-server` 组合根注入（桥接 `PermissionModeRegistry.set_mode`）。
pub trait ModeSwitcher: Send + Sync {
    /// 切换会话的权限模式（`"plan"` → `Plan`，`"default"` → `Default`）。
    fn switch_mode<'a>(&'a self, session_id: &'a str, mode: &'a str) -> BoxFuture<'a, ()>;
}

/// 组装根缺省端口（`Arc<dyn ToolAdmission>` 的直通实例）。
#[must_use]
pub fn allow_all() -> Arc<dyn ToolAdmission> {
    Arc::new(AllowAllAdmission)
}

#[cfg(test)]
mod tests {
    use super::{Admission, AdmissionRequest, allow_all};

    /// 直通端口原样回传入参——2.3/2.4 行为不因端口引入而改变。
    #[tokio::test]
    async fn allow_all_passes_input_through() {
        let port = allow_all();
        let input = serde_json::json!({"command": "ls"});
        let outcome = port
            .admit(AdmissionRequest {
                session_id: "s-1",
                run_id: "r-1",
                tool_use_id: "toolu_1",
                tool_name: "Bash",
                input: &input,
                working_directory: Some("/tmp"),
            })
            .await;
        assert_eq!(
            outcome,
            Admission::Allow {
                execution_input: input
            }
        );
    }
}
