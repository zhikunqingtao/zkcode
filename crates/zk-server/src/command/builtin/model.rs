//! `/model [model_name]`——查看/切换 LLM 模型
//! （旧 `command/impl/ModelCommand.java`）。
//!
//! 两条分支逐字对照旧实现：
//! 1. 无参 → `text` 模型清单（当前模型带 ` (current)` 标记 + 用法尾行）；
//! 2. 有参 → 校验存在性：不存在 `error("Unknown model: ...")`，存在则切换并回
//!    `text("Model switched to: <model>")`。
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! | 旧行为 | 本实现 | 理由 |
//! |---|---|---|
//! | `getDescription()` 动态拼当前模型（`"Set the AI model (currently X)"`，读全局 `AppStateStore`） | 静态 `"Set the AI model"` | zkcode 无进程级「当前模型」单例（模型是会话列，见 `sessions.model`），描述又是 `&'static str`（`/help` 清单不带会话上下文），故取旧文案的无状态部分 |
//! | 校验为 `available.contains(modelName)` 严格匹配 | 沿用 [`crate::ws::inbound`] `set_model` 的规则：注册表为空时放行 | 两条入口（WS `set_model` 与 `/model`）改同一个字段，校验强度必须一致；注册表为空是「单 provider 回退」装配（Phase 1 既有偏离），此时严格匹配会拒掉一切模型 |
//! | 切换只改内存 `AppStateStore.session().currentModel()` | 落库 `sessions.model` + 推 `model_changed` | zkcode 的模型事实源是会话行；不落库则切换在下一次请求即丢失（旧实现在 WS 路径下同样只影响进程内状态，属旧侧缺陷） |

use std::fmt::Write as _;

use futures::future::BoxFuture;
use zk_protocol::ServerMessage;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/model` 命令。
pub(super) struct ModelCommand;

impl Command for ModelCommand {
    fn name(&self) -> &'static str {
        "model"
    }

    fn description(&self) -> &'static str {
        "Set the AI model"
    }

    fn command_type(&self) -> CommandType {
        CommandType::LocalJsx
    }

    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let providers = ctx.state.providers.load();
            let available = providers.models();
            // 旧 L41-51：无参列清单。
            if args.trim().is_empty() {
                let mut text = String::from("Available Models:\n\n");
                for model in available {
                    let marker = if *model == ctx.current_model {
                        " (current)"
                    } else {
                        ""
                    };
                    let _ = writeln!(text, "  {model}{marker}");
                }
                text.push_str("\nUsage: /model <model_name>");
                return CommandResult::text(text);
            }

            let model_name = args.trim();
            // 旧 L56-61 的存在性校验（放行条件见模块文档的差异表）。
            let supported = available.is_empty() || available.iter().any(|m| m == model_name);
            if !supported {
                return CommandResult::error(format!(
                    "Unknown model: {model_name}\nAvailable models: {joined}",
                    joined = available.join(", ")
                ));
            }

            // 旧 L64-65 的状态写入在 zkcode 落到会话行；写失败仅告警不阻断，
            // 与 `set_model` 上行处理的宽松路径一致。
            if let Err(err) = ctx
                .state
                .db
                .update_session_model(&ctx.session_id, model_name)
                .await
            {
                tracing::warn!(session_id = %ctx.session_id, error = %err, "persist session model failed");
            }
            ctx.state
                .hub
                .push(
                    &ctx.session_id,
                    ServerMessage::ModelChanged {
                        model: model_name.to_owned(),
                    },
                )
                .await;
            tracing::info!(model = model_name, "Model switched to");
            CommandResult::text(format!("Model switched to: {model_name}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(args: &str, session_id: &str, state: AppState) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp/zk-model", "kimi-k3", state);
        let registry = CommandRegistry::with_builtin_commands();
        let model = registry.find_command("model").expect("registered");
        model.execute(args, &ctx).await
    }

    /// 无参 → 清单块（空注册表下只有标题与用法尾行，中间无模型行）。
    #[tokio::test]
    async fn no_args_lists_available_models_with_the_usage_footer() {
        let result = run("", "s-1", AppState::for_tests()).await;
        assert_eq!(
            result,
            CommandResult::text("Available Models:\n\n\nUsage: /model <model_name>")
        );
    }

    /// 有参且校验通过 → 落库 + 旧成功文案。
    #[tokio::test]
    async fn switching_persists_the_session_model_and_returns_the_legacy_text() {
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("kimi-k3", "/tmp/zk-model")
            .await
            .expect("session created");
        let result = run("  kimi-k4  ", &session.id, state.clone()).await;
        assert_eq!(result, CommandResult::text("Model switched to: kimi-k4"));
        let detail = state
            .db
            .get_session(&session.id)
            .await
            .expect("load")
            .expect("present");
        assert_eq!(detail.model, "kimi-k4");
    }
}
