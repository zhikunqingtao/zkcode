//! Skill 系统——Markdown 技能定义的解析、注册、六级来源加载与热重载。
//!
//! 语义来源（旧仓库只读，`581d407b`）：
//! `backend/src/main/java/com/aicodeassistant/skill/`（`SkillRegistry` /
//! `SkillDefinition` / `FrontmatterData` / `FrontmatterParser` /
//! `ArgumentSubstitution`）、`controller/SkillController.java`（两个 REST
//! 端点）、`command/impl/SkillCommand.java`（`/skill <name> [args]` 渲染）。
//!
//! 分层：
//! - [`parser`]：frontmatter 与正文切分 + `{{arg}}` 模板替换（纯函数，无 IO）；
//! - [`registry`]：技能定义模型、六级来源优先级、并发注册表（内置技能
//!   `include_str!` 编译期嵌入，进程内恒可用，不依赖运行目录）；
//! - [`loader`]：磁盘目录扫描（六级来源）与 500ms 轮询热重载。
//!
//! [`catalog`] binds project/plugin views to persisted Session/Project workspaces;
//! global preferences remain in [`registry`]. REST, WS and model invocation use
//! that same scoped catalog, while [`loader`] publishes complete directory views.

pub mod catalog;
mod filesystem;
pub mod loader;
pub mod parser;
pub mod registry;
pub mod tool;

pub use parser::{FrontmatterData, ParsedMarkdown};
pub use registry::{BUILTIN_SKILL_NAMES, SkillDefinition, SkillRegistry, SkillSource};
pub use tool::SkillTool;
