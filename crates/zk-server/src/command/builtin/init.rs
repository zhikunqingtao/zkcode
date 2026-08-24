//! `/init`——初始化项目配置（Batch 8A）。
//!
//! 语义来源（旧仓库只读）：`InitCommand.java`（39L，`PromptCommand` +
//! `ContentLength.LONG`）——生成一段「扫描项目结构并写项目配置文件」的提示词
//! 交给 LLM，命令本身不落任何文件。
//!
//! # 有意差异
//!
//! - **先落骨架再发提示词**：旧侧只发提示词，模型得自己猜配置目录该长什么样；
//!   zkcode 的六层加载器（[`zk_engine::prompt::ProjectPromptLoader`]）读的是
//!   `.zk/PROJECT.md` 与 `.zk/rules/*.md`，故本端先幂等建出
//!   `.zk/` + `.zk/rules/` + `.zk/PROJECT.md` 骨架（**已存在者一律不覆写**），
//!   再把真实路径写进提示词。任务书要求的「创建 `.zk/` + PROJECT.md 模板 +
//!   `rules/`」即此。
//! - **提示词里的目标文件名**：旧文案写 `zhikun.md`（旧侧项目记忆文件），此处
//!   改为 `.zk/PROJECT.md`——zkcode 的项目提示事实源是后者，沿用旧名会让模型
//!   写到加载器读不到的位置。`zhikun.md`（项目记忆）另由 `/memory init` 负责。
//! - **落盘失败即 `error`**：旧侧无落盘故无此分支；目录不可写时提前失败，胜过
//!   让模型基于错误前提往下跑。

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use futures::future::BoxFuture;
use zk_core::paths;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `.zk/PROJECT.md` 骨架（仅在文件缺失时写入）。
const PROJECT_MD_TEMPLATE: &str = "\
# 项目配置

<!-- 本文件由 /init 生成，作为 AI 助手的项目级上下文事实源。 -->
<!-- 拆分规则可放到同目录 rules/*.md，会按文件名序自动合并。 -->

## 构建与测试
<!-- 构建 / 测试 / lint 的确切命令 -->

## 技术栈
<!-- 语言、框架、关键依赖 -->

## 项目约定
<!-- 目录布局、命名与编码规范、提交要求 -->

## 注意事项
<!-- 易错点、禁止操作 -->
";

/// `/init` 命令。
pub(super) struct InitCommand;

impl Command for InitCommand {
    fn name(&self) -> &'static str {
        "init"
    }

    fn description(&self) -> &'static str {
        "Initialize project configuration (PROJECT.md, rules)"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Prompt
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            if ctx.working_dir.trim().is_empty() {
                return CommandResult::error("工作目录未设置，无法初始化项目配置");
            }
            let config_dir = paths::project_config_dir(Path::new(&ctx.working_dir));
            let project_md = match scaffold(&config_dir).await {
                Ok(path) => path,
                Err(error) => {
                    tracing::warn!(dir = %config_dir.display(), %error, "/init scaffold failed");
                    return CommandResult::error(format!("初始化项目配置失败: {error}"));
                }
            };

            // 旧 `InitCommand.execute` 的提示词（目标文件名按上方留痕替换）。
            let mut prompt = String::from(
                "Please help initialize this project. \
Scan the project structure and detect:\n\
1. Build system and commands (build, test, lint)\n\
2. Programming languages and frameworks\n\
3. Project conventions and coding standards\n\n\
Then create or update the .zk/PROJECT.md file with relevant project information \
that would be helpful for an AI assistant working on this project.\n\n",
            );
            let _ = writeln!(prompt, "Project directory: {}", ctx.working_dir);
            let _ = writeln!(prompt, "Project config file: {}", project_md.display());
            CommandResult::text(prompt)
        })
    }
}

/// 幂等建出 `.zk/` + `.zk/rules/` + `.zk/PROJECT.md`；返回主配置文件路径。
///
/// 已存在的目录与文件一律保留原样——`/init` 可重复执行，不得吞掉用户已写内容。
async fn scaffold(config_dir: &Path) -> std::io::Result<PathBuf> {
    tokio::fs::create_dir_all(config_dir.join(zk_engine::prompt::RULES_DIR_NAME)).await?;
    let project_md = config_dir.join(zk_engine::prompt::PROJECT_MD_FILE);
    if !tokio::fs::try_exists(&project_md).await? {
        tokio::fs::write(&project_md, PROJECT_MD_TEMPLATE).await?;
    }
    Ok(project_md)
}

#[cfg(test)]
mod tests {
    use crate::command::traits::{CommandResult, CommandType};
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    /// 独立临时工作目录（每个用例一份，`/init` 会往里写 `.zk/`）。
    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-init-cmd-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// 跑一次 `/init`。
    async fn run(working_dir: &str) -> CommandResult {
        let ctx = CommandContext::of("s-init", working_dir, "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("init").expect("registered");
        assert_eq!(command.command_type(), CommandType::Prompt);
        command.execute("", &ctx).await
    }

    /// 工作目录缺失 → 不落盘，直接错误。
    #[tokio::test]
    async fn blank_working_dir_reports_error() {
        match run("   ").await {
            CommandResult::Error(message) => {
                assert_eq!(message, "工作目录未设置，无法初始化项目配置");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// 首次执行：建出 `.zk/rules/` 与 `.zk/PROJECT.md`，提示词带两条真实路径。
    #[tokio::test]
    async fn first_run_scaffolds_config_dir_and_returns_prompt() {
        let dir = temp_dir("fresh");
        let _ = std::fs::remove_dir_all(dir.join(".zk"));

        let CommandResult::Text(prompt) = run(dir.to_str().expect("utf8 path")).await else {
            panic!("expected text prompt");
        };
        assert!(
            prompt.starts_with("Please help initialize this project. "),
            "unexpected prompt: {prompt}"
        );
        assert!(prompt.contains("create or update the .zk/PROJECT.md file"));
        assert!(prompt.contains(&format!("Project directory: {}", dir.display())));
        assert!(prompt.contains("Project config file: "));

        assert!(dir.join(".zk").join("rules").is_dir());
        let written = std::fs::read_to_string(dir.join(".zk").join("PROJECT.md")).expect("read");
        assert!(written.starts_with("# 项目配置"), "template not written");
    }

    /// 重复执行幂等：既有 `PROJECT.md` 内容不被模板覆盖。
    #[tokio::test]
    async fn rerun_keeps_existing_project_md() {
        let dir = temp_dir("idempotent");
        let config_dir = dir.join(".zk");
        std::fs::create_dir_all(&config_dir).expect("config dir");
        std::fs::write(config_dir.join("PROJECT.md"), "# 用户已写内容\n").expect("seed");

        assert!(matches!(
            run(dir.to_str().expect("utf8 path")).await,
            CommandResult::Text(_)
        ));

        let kept = std::fs::read_to_string(config_dir.join("PROJECT.md")).expect("read");
        assert_eq!(kept, "# 用户已写内容\n");
        assert!(config_dir.join("rules").is_dir(), "rules dir still created");
    }
}
