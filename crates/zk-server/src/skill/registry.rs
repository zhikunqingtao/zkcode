//! 技能注册表——技能定义模型、6 级来源优先级与并发安全的注册/解析。
//!
//! 语义来源（旧仓库只读，`581d407b`）：
//! `backend/src/main/java/com/aicodeassistant/skill/SkillRegistry.java`
//! （`ConcurrentHashMap` 双表 + 14 内置技能清单 + `resolve` 大小写不敏感
//! 三级匹配 + `registerBuiltin` / `register` 双入口）、`SkillDefinition.java`
//! （record 六字段 + `effectiveName` / `effectiveDescription` / `parseArgs`
//! / `renderTemplate` / `fromMarkdown`）。
//!
//! # 来源优先级（6 级）
//!
//! 旧类注释给出的链为 `managed > user > project > plugin > bundled > mcp`，
//! 而旧代码是「无条件 `put` 覆盖 + 固定加载序（bundled → project → user）」，
//! 二者仅在乱序注册时才会分叉。Rust 侧把优先级显式建模为
//! [`SkillSource::priority`]，[`SkillRegistry::register`] 仅在「新来源优先级
//! ≥ 在册来源」时覆盖——终态与旧加载序一致，且热重载事件不再受注册时序影响
//! （旧实现里 `WatchService` 回调的 `register` 会把同名 user 技能顶掉）。
//!
//! # 内置技能载入方式
//!
//! 旧实现走 `ClassPathResource("skills/bundled/<name>.md")` 读 jar 内资源；
//! Rust 侧用 `include_str!` 编译期嵌入 `resources/skills/bundled/*.md`
//! （文件逐字节取自旧仓库同名资源），单二进制部署无需外部资源目录。

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use serde::Serialize;

use super::parser::{self, FrontmatterData};

/// 内置技能清单（旧 `BUILTIN_SKILL_NAMES`，14 件，顺序一致）。
pub const BUILTIN_SKILL_NAMES: [&str; 14] = [
    "commit",
    "review",
    "fix",
    "test",
    "pr",
    "debug",
    "verify",
    "stuck",
    "remember",
    "software-architecture",
    "csv-data-summarizer",
    "prompt-engineering",
    "test-driven-development",
    "publish-oss",
];

/// 内置技能正文（编译期嵌入，与 [`BUILTIN_SKILL_NAMES`] 一一对应）。
const BUILTIN_SKILL_SOURCES: [&str; 14] = [
    include_str!("../../resources/skills/bundled/commit.md"),
    include_str!("../../resources/skills/bundled/review.md"),
    include_str!("../../resources/skills/bundled/fix.md"),
    include_str!("../../resources/skills/bundled/test.md"),
    include_str!("../../resources/skills/bundled/pr.md"),
    include_str!("../../resources/skills/bundled/debug.md"),
    include_str!("../../resources/skills/bundled/verify.md"),
    include_str!("../../resources/skills/bundled/stuck.md"),
    include_str!("../../resources/skills/bundled/remember.md"),
    include_str!("../../resources/skills/bundled/software-architecture.md"),
    include_str!("../../resources/skills/bundled/csv-data-summarizer.md"),
    include_str!("../../resources/skills/bundled/prompt-engineering.md"),
    include_str!("../../resources/skills/bundled/test-driven-development.md"),
    include_str!("../../resources/skills/bundled/publish-oss.md"),
];

/// 技能加载来源（旧 `SkillDefinition.SkillSource` 六值，序列化形状与旧
/// `source().name()` 一致——`SCREAMING_SNAKE_CASE`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SkillSource {
    /// 企业策略管理目录（`ZK_MANAGED_SKILLS_DIR`）。
    Managed,
    /// 用户全局目录（`~/.zkcode/skills/`）。
    User,
    /// 项目目录（`<workspace>/.zkcode/skills/`）。
    Project,
    /// 插件提供（`<workspace>/.zkcode/plugins/*/skills/`）。
    Plugin,
    /// 内置技能（编译期嵌入）。
    Bundled,
    /// MCP 构建的技能（运行时经 [`SkillRegistry::register`] 注入）。
    Mcp,
}

impl SkillSource {
    /// 稳定字符串名（旧 `enum.name()`，供日志与 REST 直出）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Managed => "MANAGED",
            Self::User => "USER",
            Self::Project => "PROJECT",
            Self::Plugin => "PLUGIN",
            Self::Bundled => "BUNDLED",
            Self::Mcp => "MCP",
        }
    }

    /// 覆盖优先级（数值越大越高；见模块文档的优先级链）。
    #[must_use]
    pub const fn priority(self) -> u8 {
        match self {
            Self::Managed => 6,
            Self::User => 5,
            Self::Project => 4,
            Self::Plugin => 3,
            Self::Bundled => 2,
            Self::Mcp => 1,
        }
    }
}

/// 技能定义（旧 record `SkillDefinition` 六字段）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDefinition {
    /// 技能名（文件名去 `.md`，不含 `/` 前缀）。
    pub name: String,
    /// 原始文件名。
    pub file_name: String,
    /// frontmatter 元数据。
    pub frontmatter: FrontmatterData,
    /// Markdown 正文（模板渲染输入）。
    pub content: String,
    /// 加载来源。
    pub source: SkillSource,
    /// 文件绝对路径（`None` = 内置技能）。
    pub file_path: Option<String>,
}

impl SkillDefinition {
    /// 从 Markdown 文件内容构建（旧 `fromMarkdown`）。
    #[must_use]
    pub fn from_markdown(
        file_name: &str,
        raw_content: &str,
        source: SkillSource,
        file_path: Option<String>,
    ) -> Self {
        let parsed = parser::parse(raw_content);
        let name = file_name
            .strip_suffix(".md")
            .unwrap_or(file_name)
            .to_owned();
        Self {
            name,
            file_name: file_name.to_owned(),
            frontmatter: parsed.frontmatter,
            content: parsed.content,
            source,
            file_path,
        }
    }

    /// 有效名称（旧 `effectiveName`：frontmatter.name 优先，回落文件名）。
    #[must_use]
    pub fn effective_name(&self) -> &str {
        self.frontmatter.name.as_deref().unwrap_or(&self.name)
    }

    /// 有效描述（旧 `effectiveDescription`：缺省时 `Skill: <name>`）。
    #[must_use]
    pub fn effective_description(&self) -> String {
        self.frontmatter
            .description
            .clone()
            .unwrap_or_else(|| format!("Skill: {}", self.name))
    }

    /// 是否允许用户直接调用（旧 `isUserInvocable`）。
    #[must_use]
    pub fn is_user_invocable(&self) -> bool {
        self.frontmatter.user_invocable
    }

    /// 解析调用参数（旧 `parseArgs`，委托模板参数替换器）。
    #[must_use]
    pub fn parse_args(&self, args: &str) -> std::collections::BTreeMap<String, String> {
        parser::parse_args(args, &self.frontmatter.arguments)
    }

    /// 渲染模板（旧 `renderTemplate`：替换 `{{param}}`）。
    #[must_use]
    pub fn render_template(&self, params: &std::collections::BTreeMap<String, String>) -> String {
        parser::substitute(&self.content, params)
    }
}

/// 技能注册表（旧 `SkillRegistry` 的 `skills` / `builtinSkills` 双表）。
///
/// 读多写少：`RwLock` + 锁内克隆出参，锁作用域恒为常数级；poison 一律
/// `into_inner` 恢复（技能表是可重建的派生状态，无需以 panic 传播）。
#[derive(Debug, Default)]
pub struct SkillRegistry {
    /// 全部在册技能（name → definition，按来源优先级覆盖）。
    skills: RwLock<HashMap<String, SkillDefinition>>,
    /// 内置技能独立缓存（旧 `builtinSkills`，供自定义技能删除后回填）。
    builtin: RwLock<HashMap<String, SkillDefinition>>,
}

impl SkillRegistry {
    /// 空注册表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 载入 14 件内置技能后的注册表（旧 `@PostConstruct
    /// registerBuiltinSkills` 的等价装配入口）。
    #[must_use]
    pub fn with_builtin_skills() -> Self {
        let registry = Self::new();
        registry.register_builtin_skills();
        registry
    }

    /// 注册内置技能（旧 `registerBuiltinSkills`：14 件逐个解析入双表）。
    pub fn register_builtin_skills(&self) {
        for (name, raw) in BUILTIN_SKILL_NAMES.iter().zip(BUILTIN_SKILL_SOURCES) {
            let skill = SkillDefinition::from_markdown(
                &format!("{name}.md"),
                raw,
                SkillSource::Bundled,
                None,
            );
            self.register_builtin(skill);
        }
        tracing::info!(
            count = BUILTIN_SKILL_NAMES.len(),
            "builtin skills registered"
        );
    }

    /// 注册内置技能（旧 `registerBuiltin`：同时进 `builtin` 与总表）。
    pub fn register_builtin(&self, skill: SkillDefinition) {
        let name = skill.name.clone();
        self.builtin
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.clone(), skill.clone());
        self.skills
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name, skill);
    }

    /// 注册任意来源技能（旧 `register`，附加来源优先级守卫）。
    ///
    /// 返回 `true` = 已写入；`false` = 被更高优先级来源的同名技能挡下。
    pub fn register(&self, skill: SkillDefinition) -> bool {
        let mut skills = self.skills.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = skills.get(&skill.name)
            && existing.source.priority() > skill.source.priority()
        {
            tracing::debug!(
                skill = %skill.name,
                incoming = skill.source.as_str(),
                registered = existing.source.as_str(),
                "skill registration skipped (lower priority source)"
            );
            return false;
        }
        tracing::debug!(skill = %skill.name, source = skill.source.as_str(), "skill registered");
        skills.insert(skill.name.clone(), skill);
        true
    }

    /// 按名称解析（旧 `resolve`：去 `/` 前缀 + 小写精确命中 → 遍历大小写
    /// 不敏感匹配 `name` / `effectiveName`）。
    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<SkillDefinition> {
        let normalized = name.strip_prefix('/').unwrap_or(name).to_lowercase();
        let skills = self.skills.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(skill) = skills.get(&normalized) {
            return Some(skill.clone());
        }
        skills
            .values()
            .find(|skill| {
                skill.name.eq_ignore_ascii_case(&normalized)
                    || skill.effective_name().eq_ignore_ascii_case(&normalized)
            })
            .cloned()
    }

    /// 全部在册技能（旧 `getAllSkills`）。
    ///
    /// 按 [`SkillDefinition::effective_name`] 升序返回：旧实现直接暴露
    /// `ConcurrentHashMap.values()`（顺序不定），REST 列表需要确定序。
    #[must_use]
    pub fn all_skills(&self) -> Vec<SkillDefinition> {
        let mut all: Vec<SkillDefinition> = self
            .skills
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        all.sort_by(|left, right| left.effective_name().cmp(right.effective_name()));
        all
    }

    /// 全部内置技能（旧 `getBuiltinSkills`，同样按有效名升序）。
    #[must_use]
    pub fn builtin_skills(&self) -> Vec<SkillDefinition> {
        let mut all: Vec<SkillDefinition> = self
            .builtin
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        all.sort_by(|left, right| left.effective_name().cmp(right.effective_name()));
        all
    }

    /// 在册技能数（旧 `size`）。
    #[must_use]
    pub fn len(&self) -> usize {
        self.skills
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// 是否为空（`clippy::len_without_is_empty`）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 反注册（旧 `WatchService` 的 `ENTRY_DELETE` 分支：按名移除）。
    ///
    /// 返回被移除的技能。移除后若同名内置技能仍在缓存中，则回填内置版本
    /// （自定义覆盖被删 → 退回内置行为；旧实现无此回填，见模块文档）。
    pub fn unregister(&self, name: &str) -> Option<SkillDefinition> {
        let mut skills = self.skills.write().unwrap_or_else(PoisonError::into_inner);
        let removed = skills.remove(name)?;
        if let Some(builtin) = self
            .builtin
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
        {
            skills.insert(name.to_owned(), builtin.clone());
        }
        Some(removed)
    }

    /// 按文件路径反注册（热重载删除事件入口：以路径而非文件名定位，
    /// 避免同名不同源技能被误删）。
    pub fn unregister_by_path(&self, file_path: &str) -> Option<SkillDefinition> {
        let name = {
            let skills = self.skills.read().unwrap_or_else(PoisonError::into_inner);
            skills
                .values()
                .find(|skill| skill.file_path.as_deref() == Some(file_path))
                .map(|skill| skill.name.clone())?
        };
        self.unregister(&name)
    }

    /// 清空双表（旧 `clear`，测试与重载全量重建使用）。
    pub fn clear(&self) {
        self.skills
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.builtin
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 14 件内置技能全部载入，名称与旧 `BUILTIN_SKILL_NAMES` 逐一对齐。
    #[test]
    fn builtin_skills_cover_fourteen_names() {
        let registry = SkillRegistry::with_builtin_skills();
        assert_eq!(registry.len(), 14);
        for name in BUILTIN_SKILL_NAMES {
            let skill = registry
                .resolve(name)
                .unwrap_or_else(|| panic!("builtin skill {name} must resolve"));
            assert_eq!(skill.source, SkillSource::Bundled);
            assert_eq!(skill.file_name, format!("{name}.md"));
            assert!(skill.file_path.is_none(), "bundled skill has no file path");
            assert!(!skill.content.trim().is_empty(), "{name} body not empty");
            assert!(
                !skill.effective_description().trim().is_empty(),
                "{name} description not empty"
            );
        }
    }

    /// 内置技能 description 来源二分：带 frontmatter 者取 YAML，
    /// 无 frontmatter 者取正文首段落兜底。
    #[test]
    fn builtin_descriptions_come_from_frontmatter_or_first_paragraph() {
        let registry = SkillRegistry::with_builtin_skills();
        let debug = registry.resolve("debug").expect("debug skill");
        assert_eq!(
            debug.frontmatter.description.as_deref(),
            Some("系统化调试流程，从错误复现到根因定位到修复验证的完整闭环")
        );
        assert_eq!(debug.effective_name(), "debug");
        // commit.md 无 frontmatter → 首段落兜底（跳过 `#` 标题行）。
        let commit = registry.resolve("commit").expect("commit skill");
        assert_eq!(
            commit.effective_description(),
            "分析暂存区的变更，创建结构良好的 git commit。"
        );
    }

    /// `resolve` 三级匹配：精确、带 `/` 前缀、大小写不敏感。
    #[test]
    fn resolve_normalizes_slash_prefix_and_case() {
        let registry = SkillRegistry::with_builtin_skills();
        assert!(registry.resolve("/commit").is_some());
        assert!(registry.resolve("COMMIT").is_some());
        assert!(registry.resolve("does-not-exist").is_none());
    }

    /// 来源优先级：project 覆盖 bundled；bundled 不能反向覆盖 project。
    #[test]
    fn register_respects_source_priority() {
        let registry = SkillRegistry::with_builtin_skills();
        let project = SkillDefinition::from_markdown(
            "commit.md",
            "---\ndescription: 项目版提交技能\n---\n项目正文",
            SkillSource::Project,
            Some("/tmp/project/.zkcode/skills/commit.md".to_owned()),
        );
        assert!(registry.register(project));
        let resolved = registry.resolve("commit").expect("commit skill");
        assert_eq!(resolved.source, SkillSource::Project);
        assert_eq!(resolved.effective_description(), "项目版提交技能");

        let bundled_again =
            SkillDefinition::from_markdown("commit.md", "内置回写尝试", SkillSource::Bundled, None);
        assert!(!registry.register(bundled_again));
        assert_eq!(
            registry.resolve("commit").expect("commit skill").source,
            SkillSource::Project
        );
        assert_eq!(registry.len(), 14, "同名覆盖不增加计数");
    }

    /// 反注册按路径定位，且同名内置技能回填。
    #[test]
    fn unregister_by_path_restores_builtin() {
        let registry = SkillRegistry::with_builtin_skills();
        let path = "/tmp/project/.zkcode/skills/commit.md".to_owned();
        registry.register(SkillDefinition::from_markdown(
            "commit.md",
            "---\ndescription: 覆盖版\n---\n正文",
            SkillSource::Project,
            Some(path.clone()),
        ));
        let removed = registry.unregister_by_path(&path).expect("removed skill");
        assert_eq!(removed.source, SkillSource::Project);
        let restored = registry.resolve("commit").expect("builtin restored");
        assert_eq!(restored.source, SkillSource::Bundled);
        assert_eq!(registry.len(), 14);
    }

    /// 自定义技能（无同名内置）反注册后彻底消失。
    #[test]
    fn unregister_removes_custom_skill() {
        let registry = SkillRegistry::new();
        let path = "/tmp/project/.zkcode/skills/deploy.md".to_owned();
        registry.register(SkillDefinition::from_markdown(
            "deploy.md",
            "---\ndescription: 部署\n---\n正文",
            SkillSource::Project,
            Some(path.clone()),
        ));
        assert_eq!(registry.len(), 1);
        assert!(registry.unregister_by_path(&path).is_some());
        assert!(registry.is_empty());
        assert!(registry.unregister_by_path(&path).is_none());
    }

    /// `all_skills` 按有效名升序（REST 列表确定序）。
    #[test]
    fn all_skills_sorted_by_effective_name() {
        let registry = SkillRegistry::with_builtin_skills();
        let names: Vec<String> = registry
            .all_skills()
            .iter()
            .map(|skill| skill.effective_name().to_owned())
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        assert_eq!(registry.builtin_skills().len(), 14);
    }

    /// 来源字符串与优先级链（序列化形状即旧 `enum.name()`）。
    #[test]
    fn source_names_and_priority_chain() {
        assert_eq!(SkillSource::Bundled.as_str(), "BUNDLED");
        assert_eq!(
            serde_json::to_string(&SkillSource::Mcp).expect("serialize"),
            "\"MCP\""
        );
        assert!(SkillSource::Managed.priority() > SkillSource::User.priority());
        assert!(SkillSource::User.priority() > SkillSource::Project.priority());
        assert!(SkillSource::Project.priority() > SkillSource::Plugin.priority());
        assert!(SkillSource::Plugin.priority() > SkillSource::Bundled.priority());
        assert!(SkillSource::Bundled.priority() > SkillSource::Mcp.priority());
    }

    /// 模板渲染：位置参数 + 命名参数 + 未提供变量保留占位符。
    #[test]
    fn render_template_substitutes_arguments() {
        let skill = SkillDefinition::from_markdown(
            "deploy.md",
            "---\ndescription: 部署\narguments:\n  - env\n  - tag\n---\n发布 {{tag}} 到 {{env}}，备注 {{note}}",
            SkillSource::Project,
            None,
        );
        assert_eq!(skill.frontmatter.arguments, vec!["env", "tag"]);
        let params = skill.parse_args("prod v1.2.0");
        assert_eq!(params.get("env").map(String::as_str), Some("prod"));
        assert_eq!(params.get("tag").map(String::as_str), Some("v1.2.0"));
        assert_eq!(
            skill.render_template(&params),
            "发布 v1.2.0 到 prod，备注 {{note}}"
        );
    }
}
