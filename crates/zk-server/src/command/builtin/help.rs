//! `/help [command_name]`——命令清单与详情（旧 `command/impl/HelpCommand.java`）。
//!
//! 三条分支逐字对照旧实现：
//! 1. 无参 → `jsx({action:"helpCommandList", groups, total})`，按类型分三组；
//! 2. 有参且命中 → `text` 详情块；
//! 3. 有参未命中 → `error("Unknown command: /" + name + ". " + suggestion)`。

use std::fmt::Write as _;

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/help` 命令。
pub(super) struct HelpCommand;

impl Command for HelpCommand {
    fn name(&self) -> &'static str {
        "help"
    }

    fn description(&self) -> &'static str {
        "Show available commands"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            // 旧 L36-39：`args != null && !args.isBlank()` → 详情，否则清单。
            if args.trim().is_empty() {
                command_list(ctx)
            } else {
                command_detail(args.trim(), ctx)
            }
        })
    }
}

/// 旧 `showCommandList()` L42-65：三组固定顺序，空组跳过。
fn command_list(ctx: &CommandContext) -> CommandResult {
    let registry = &ctx.state.commands;
    let visible = registry.visible_commands();
    let mut groups = Vec::new();
    // 旧 `addGroupData` 的三次调用顺序即分组输出顺序。
    for (title, title_zh, command_type) in [
        ("Local Commands", "本地命令", CommandType::Local),
        ("Interactive Commands", "交互命令", CommandType::LocalJsx),
        ("Prompt Commands", "提示词命令", CommandType::Prompt),
    ] {
        let items: Vec<serde_json::Value> = visible
            .iter()
            .filter(|command| command.command_type() == command_type)
            .map(|command| {
                serde_json::json!({
                    "name": command.name(),
                    "description": command.description(),
                    "aliases": slashed_aliases(command.aliases()),
                })
            })
            .collect();
        // 旧 `addGroupData` L69：空组 return，不入 groups。
        if items.is_empty() {
            continue;
        }
        groups.push(serde_json::json!({
            "title": title,
            "titleZh": title_zh,
            "commands": items,
        }));
    }
    CommandResult::jsx(serde_json::json!({
        "action": "helpCommandList",
        "groups": groups,
        "total": visible.len(),
    }))
}

/// 旧 `showCommandDetail(String)` L82-112。
fn command_detail(command_name: &str, ctx: &CommandContext) -> CommandResult {
    let registry = &ctx.state.commands;
    let Some(command) = registry.find_command(command_name) else {
        let suggestion = registry.suggest_commands(command_name);
        return CommandResult::error(format!("Unknown command: /{command_name}. {suggestion}"));
    };
    let mut text = format!(
        "/{name} — {description}\n\n",
        name = command.name(),
        description = command.description()
    );
    let _ = writeln!(text, "  Type:      {}", command.command_type());
    let _ = writeln!(text, "  Version:   {}", command.version());
    // 旧 L95-99：别名为空则整行不输出。
    if !command.aliases().is_empty() {
        let _ = writeln!(
            text,
            "  Aliases:   {}",
            slashed_aliases(command.aliases()).join(", ")
        );
    }
    let _ = writeln!(text, "  Immediate: {}", command.is_immediate());
    let _ = writeln!(text, "  Hidden:    {}", command.is_hidden());
    // 旧 L104-109 的 `PromptCommand` 追加块（Content / Tools）仍不输出：
    // zkcode 只有单一 [`Command`] trait，未建模旧 `PromptCommand` 的
    // `getContentLength()` / `getAllowedTools()`——旧侧这两项也只被本详情块消费，
    // 不参与执行语义。见 `crate::command` 模块文档的偏离表。
    CommandResult::text(text)
}

/// 旧 `getAliases().stream().map(a -> "/" + a)`。
fn slashed_aliases(aliases: &[&str]) -> Vec<String> {
    aliases.iter().map(|alias| format!("/{alias}")).collect()
}

#[cfg(test)]
mod tests {
    use crate::command::CommandContext;
    use crate::command::traits::CommandResult;
    use crate::state::AppState;

    fn ctx() -> CommandContext {
        CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests())
    }

    async fn run(args: &str) -> CommandResult {
        let ctx = ctx();
        let help = ctx.state.commands.find_command("help").expect("registered");
        help.execute(args, &ctx).await
    }

    /// 无参 → JSX 清单：Batch 8A 起三组均非空（`PROMPT` 组有
    /// `/git-review`、`/init`、`/retry`），`total` 即可见命令数。
    #[tokio::test]
    async fn no_args_returns_the_grouped_jsx_listing() {
        let CommandResult::Jsx(data) = run("").await else {
            panic!("help without args must return jsx");
        };
        assert_eq!(data["action"], "helpCommandList");
        // 32 个内建命令无一隐藏 → `total` 即全量。
        assert_eq!(data["total"], 32);
        let groups = data["groups"].as_array().expect("groups array");
        let titles: Vec<&str> = groups
            .iter()
            .map(|group| group["title"].as_str().expect("title"))
            .collect();
        // 三组均非空 → 旧 `addGroupData` 全部输出（顺序即旧调用序）。
        assert_eq!(
            titles,
            ["Local Commands", "Interactive Commands", "Prompt Commands"]
        );
        assert_eq!(groups[0]["titleZh"], "本地命令");
        assert_eq!(groups[1]["titleZh"], "交互命令");
        assert_eq!(groups[2]["titleZh"], "提示词命令");
        // 组内按名升序，别名带前导斜杠。
        let local: Vec<&str> = groups[0]["commands"]
            .as_array()
            .expect("commands")
            .iter()
            .map(|item| item["name"].as_str().expect("name"))
            .collect();
        assert_eq!(
            local,
            [
                "browser-snapshot",
                "clear",
                "compact",
                "cost",
                "diff",
                "env-vars",
                "exit",
                "extra-usage",
                "git-commit",
                "help",
                "logout",
                "mcp",
                "plan",
                "rate-limit-options",
                "set-env",
                "status",
                "undo",
                "upgrade",
                "usage",
                "version",
            ]
        );
        assert_eq!(
            groups[0]["commands"][0]["aliases"],
            serde_json::json!(["/snapshot"])
        );
        let prompt: Vec<&str> = groups[2]["commands"]
            .as_array()
            .expect("commands")
            .iter()
            .map(|item| item["name"].as_str().expect("name"))
            .collect();
        assert_eq!(prompt, ["git-review", "init", "retry"]);
    }

    /// 有参命中 → 详情块逐字（别名行仅在非空时出现）。
    #[tokio::test]
    async fn detail_block_matches_the_legacy_layout() {
        let CommandResult::Text(text) = run("clear").await else {
            panic!("help <name> must return text");
        };
        assert_eq!(
            text,
            "/clear — Clear conversation history\n\n\
             \x20 Type:      LOCAL\n\
             \x20 Version:   1.0\n\
             \x20 Aliases:   /reset, /new\n\
             \x20 Immediate: false\n\
             \x20 Hidden:    false\n"
        );
    }

    /// 无别名的命令不输出 `Aliases:` 行（旧 L95 的 if）。
    #[tokio::test]
    async fn detail_block_omits_the_alias_line_when_there_are_none() {
        let CommandResult::Text(text) = run("help").await else {
            panic!("help help must return text");
        };
        assert!(!text.contains("Aliases:"), "unexpected alias line: {text}");
        assert!(text.starts_with("/help — Show available commands\n\n"));
    }

    /// 别名亦可查详情（旧 `findCommand` 走别名索引），但标题用主名。
    #[tokio::test]
    async fn detail_resolves_aliases_but_prints_the_primary_name() {
        let CommandResult::Text(text) = run("reset").await else {
            panic!("alias lookup must return text");
        };
        assert!(text.starts_with("/clear — "));
    }

    /// 未命中 → 旧错误文案（含模糊建议句）。
    #[tokio::test]
    async fn unknown_command_yields_the_legacy_error_text() {
        let CommandResult::Error(message) = run("claer").await else {
            panic!("unknown command must be an error");
        };
        assert_eq!(
            message,
            "Unknown command: /claer. Did you mean: /clear, /plan?"
        );
    }
}
