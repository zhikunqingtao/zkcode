//! 命令执行上下文（旧 `command/CommandContext.java`）。
//!
//! 旧 record 为
//! `(sessionId, workingDir, currentModel, appState, isAuthenticated, isRemoteMode, isBridgeMode)`，
//! 静态工厂 `of(sessionId, workingDir, currentModel, appState)` 把三个布尔位
//! 全填 `false`。
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! 旧 `WebSocketController.handleSlashCommand` L1352-1355 构造上下文时把
//! `currentModel` 与 `appState` **都传 null**：
//!
//! - `/session` 因此输出 `Model:     null`；
//! - `/config` 无参分支直接解引用 `context.appState()` → `NullPointerException`
//!   （该异常不在 `handleSlashCommand` 的 catch 覆盖内，只捕
//!   `CommandNotFoundException`），即旧 WS 路径下 `/config` 必然失败。
//!
//! zkcode 不复刻这两处空值：上下文在构造时就从会话仓储读出真实
//! `model` / `working_dir`，`state` 恒非空（[`AppState`] 是 `Clone` 句柄集）。
//! 三个布尔位仍按旧 `of()` 填 `false`——WS 通道没有「远程/桥接模式」概念，
//! 认证位亦无连接级事实源（旧侧同样恒 false），故保持一致。

use crate::state::AppState;

/// 单次斜杠命令执行的上下文（旧 `CommandContext`）。
pub struct CommandContext {
    /// 当前会话 ID（旧 `sessionId`）。
    pub session_id: String,
    /// 会话工作目录（旧 `workingDir`：经存量绑定复核后的绝对路径）。
    pub working_dir: String,
    /// 当前模型（旧 `currentModel`）。
    pub current_model: String,
    /// 是否已认证（旧 `isAuthenticated`；`of()` 恒 false）。
    pub is_authenticated: bool,
    /// 是否远程模式（旧 `isRemoteMode`；`of()` 恒 false）。
    pub is_remote_mode: bool,
    /// 是否 IDE 桥接模式（旧 `isBridgeMode`；`of()` 恒 false）。
    pub is_bridge_mode: bool,
    /// 应用状态句柄（旧 `appState`；命令经此访问仓储 / 注册表 / 权限管线）。
    pub state: AppState,
}

impl CommandContext {
    /// 旧 `CommandContext.of(...)`：三个布尔位填 `false`。
    #[must_use]
    pub fn of(
        session_id: impl Into<String>,
        working_dir: impl Into<String>,
        current_model: impl Into<String>,
        state: AppState,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            working_dir: working_dir.into(),
            current_model: current_model.into(),
            is_authenticated: false,
            is_remote_mode: false,
            is_bridge_mode: false,
            state,
        }
    }

    /// 从会话仓储解析上下文（旧 `requireSessionWorkingDirectory` L1748-1754
    /// 的等价物：`loadSession` → 取 `workingDir` → `requireCurrentBinding`）。
    ///
    /// # Errors
    /// 返回给调用方直接下行的失败文案：
    /// - 会话不存在 / 读库失败 → 旧 `IllegalStateException` 的文案
    ///   `Session is no longer available`；
    /// - 存量绑定复核不过 → [`crate::workspace::require_current_binding`] 的
    ///   `ApiError.message`（旧 `requireCurrentBinding` 同源文案）。
    pub(crate) async fn resolve(state: &AppState, session_id: &str) -> Result<Self, String> {
        const SESSION_GONE: &str = "Session is no longer available";
        let detail = match state.db.get_session(session_id).await {
            Ok(Some(detail)) => detail,
            Ok(None) => return Err(SESSION_GONE.to_owned()),
            Err(err) => {
                tracing::warn!(session_id, error = %err, "slash command session load failed");
                return Err(SESSION_GONE.to_owned());
            }
        };
        // `require_current_binding` 做 `canonicalize` 等阻塞文件系统调用——与
        // `create_session` 的项目解析同样经 `spawn_blocking`，不占用 WS 读循环
        // 所在的 runtime worker。
        let config = state.config.clone();
        let saved = detail.working_dir.clone();
        let resolved = tokio::task::spawn_blocking(move || {
            crate::workspace::require_current_binding(&config, &saved)
        })
        .await;
        let working_dir = match resolved {
            Ok(Ok(path)) => path.to_string_lossy().into_owned(),
            Ok(Err(err)) => return Err(err.message),
            Err(err) => {
                tracing::error!(session_id, error = %err, "workspace binding task failed");
                return Err(SESSION_GONE.to_owned());
            }
        };
        Ok(Self::of(
            session_id,
            working_dir,
            detail.model,
            state.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::CommandContext;
    use crate::state::AppState;

    /// 旧 `of()` 的三个布尔位恒 false（`/doctor` 的 Authentication 行依赖）。
    #[test]
    fn of_defaults_all_mode_flags_to_false() {
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests());
        assert_eq!(ctx.session_id, "s-1");
        assert_eq!(ctx.working_dir, "/tmp");
        assert_eq!(ctx.current_model, "kimi-k3");
        assert!(!ctx.is_authenticated);
        assert!(!ctx.is_remote_mode);
        assert!(!ctx.is_bridge_mode);
    }

    /// 会话不存在 → 旧 `IllegalStateException` 的文案。
    #[tokio::test]
    async fn resolve_reports_missing_session_with_the_legacy_message() {
        let state = AppState::for_tests();
        let Err(err) = CommandContext::resolve(&state, "no-such-session").await else {
            panic!("会话缺失必须失败");
        };
        assert_eq!(err, "Session is no longer available");
    }

    /// 会话存在 → 工作目录经绑定复核落地，模型取库内值。
    #[tokio::test]
    async fn resolve_reads_working_dir_and_model_from_the_repository() {
        let state = AppState::for_tests();
        let cwd = std::env::current_dir().expect("cwd");
        let summary = state
            .db
            .create_session("kimi-k3", &cwd.to_string_lossy())
            .await
            .expect("session created");
        let ctx = CommandContext::resolve(&state, &summary.id)
            .await
            .expect("resolves");
        assert_eq!(ctx.current_model, "kimi-k3");
        assert_eq!(
            ctx.working_dir,
            std::fs::canonicalize(&cwd)
                .expect("canonical cwd")
                .to_string_lossy()
        );
    }
}
