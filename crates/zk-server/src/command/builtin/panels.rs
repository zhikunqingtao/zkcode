//! Thin entry points into existing local product surfaces. Opening a panel does
//! not execute tools, change preferences, or grant permissions.
use crate::command::{
    CommandContext,
    traits::{Command, CommandResult, CommandType},
};
use futures::future::BoxFuture;

pub(super) struct PanelCommand(pub(super) &'static str);

impl Command for PanelCommand {
    fn name(&self) -> &'static str {
        self.0
    }
    fn description(&self) -> &'static str {
        match self.0 {
            "theme" => "打开主题与动效设置",
            "skills" => "管理所有项目共用的 Skill 开关",
            "tasks" => "查看当前会话任务",
            "hooks" => "查看与编辑当前项目的 Hook 配置",
            "rewind" => "预览并选择要恢复的文件检查点",
            "export" => "选择并下载当前会话的 JSON 或 Markdown",
            _ => "打开本地面板",
        }
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
            let args = args.trim();
            let component = match self.0 {
                "theme" if args.is_empty() => "ThemePicker",
                "skills" if args.is_empty() || args == "list" => "SkillsManager",
                "tasks" if args.is_empty() || args == "list" => "TaskManager",
                "hooks" if matches!(args, "" | "list" | "edit") => "HooksEditor",
                "rewind" if args.is_empty() => "RewindDialog",
                "export" if matches!(args, "" | "json" | "markdown" | "md") => {
                    match ctx.state.db.session_retention(&ctx.session_id).await {
                        Ok(zk_db::content::ContentRetention::Persistent) => {}
                        Ok(zk_db::content::ContentRetention::Ephemeral) => {
                            return CommandResult::error("EPHEMERAL_OPERATION_UNSUPPORTED");
                        }
                        Err(_) => return CommandResult::error("SESSION_NOT_AVAILABLE"),
                    }
                    "ExportDialog"
                }
                _ => {
                    return CommandResult::error(format!(
                        "Unsupported arguments for /{}; open the panel to choose an explicit action",
                        self.0
                    ));
                }
            };
            CommandResult::jsx(
                serde_json::json!({"component":component,"sessionId":ctx.session_id,"format":if matches!(args,"markdown"|"md") {"markdown"} else {"json"}}),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    #[tokio::test]
    async fn panels_do_not_mutate_preferences_and_export_checks_retention() {
        let state = AppState::for_tests();
        let session = state.db.create_session("fixture", "/tmp").await.unwrap();
        let ctx = CommandContext::of(&session.id, "/tmp", "fixture", state.clone());
        for (name, component) in [
            ("theme", "ThemePicker"),
            ("skills", "SkillsManager"),
            ("tasks", "TaskManager"),
            ("export", "ExportDialog"),
        ] {
            assert!(
                matches!(PanelCommand(name).execute("",&ctx).await,CommandResult::Jsx(value) if value["component"]==component && value["sessionId"]==session.id)
            );
            assert!(matches!(
                PanelCommand(name).execute("unexpected", &ctx).await,
                CommandResult::Error(_)
            ));
        }
        let (temporary, _lease) = state
            .db
            .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
            .await
            .unwrap();
        let ctx = CommandContext::of(&temporary, "/tmp", "fixture", state);
        assert_eq!(
            PanelCommand("export").execute("", &ctx).await,
            CommandResult::error("EPHEMERAL_OPERATION_UNSUPPORTED")
        );
    }
}
