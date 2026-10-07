//! 技能注册表——技能定义模型、6 级来源优先级与并发安全的注册/解析。
//!
//! 语义来源（旧仓库只读，`581d407b`）：
//! `backend/src/main/java/com/aicodeassistant/skill/SkillRegistry.java`
//! （`ConcurrentHashMap` 双表 + 旧内置技能清单 + `resolve` 大小写不敏感
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

/// 内置技能清单（适用的 `BUILTIN_SKILL_NAMES`，13 件，不包含发布能力）。
pub const BUILTIN_SKILL_NAMES: [&str; 13] = [
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
];

/// 内置技能正文（编译期嵌入，与 [`BUILTIN_SKILL_NAMES`] 一一对应）。
const BUILTIN_SKILL_SOURCES: [&str; 13] = [
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
    /// Validated filesystem provenance; never accepted from model/tool input.
    pub(super) read_authority: Option<super::filesystem::ReadAuthority>,
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
            read_authority: None,
        }
    }

    fn source_authorized(&self) -> bool {
        self.read_authority
            .as_ref()
            .is_none_or(super::filesystem::ReadAuthority::permits_snapshot)
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
        let mut params = parser::parse_args(args, &self.frontmatter.arguments);
        if self.source == SkillSource::Bundled && self.name == "review" {
            params.insert("review_scope".into(), args.into());
        }
        params
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
pub struct SkillRegistry {
    /// 全部在册技能（name → definition，按来源优先级覆盖）。
    skills: RwLock<HashMap<String, SkillDefinition>>,
    /// 内置技能独立缓存（旧 `builtinSkills`，供自定义技能删除后回填）。
    builtin: RwLock<HashMap<String, SkillDefinition>>,
    switches: RwLock<HashMap<String, bool>>,
    state_error: RwLock<Option<String>>,
    source_error: RwLock<Option<&'static str>>,
    state_loaded: std::sync::atomic::AtomicBool,
    store: Option<zk_db::Db>,
    update_lock: tokio::sync::Mutex<()>,
}

impl Default for SkillRegistry {
    fn default() -> Self {
        Self {
            skills: RwLock::new(HashMap::new()),
            builtin: RwLock::new(HashMap::new()),
            switches: RwLock::new(HashMap::new()),
            state_error: RwLock::new(None),
            source_error: RwLock::new(None),
            state_loaded: std::sync::atomic::AtomicBool::new(true),
            store: None,
            update_lock: tokio::sync::Mutex::new(()),
        }
    }
}

impl std::fmt::Debug for SkillRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkillRegistry")
            .field("count", &self.len())
            .field("state_error", &self.state_error())
            .finish_non_exhaustive()
    }
}

impl SkillRegistry {
    /// 空注册表。
    #[must_use]
    pub fn new() -> Self {
        Self {
            state_loaded: std::sync::atomic::AtomicBool::new(true),
            ..Self::default()
        }
    }

    /// Attach authoritative global switches before loading any skills.
    pub fn with_persisted_state(db: zk_db::Db) -> Self {
        let stored = db.skill_states_at_startup();
        let registry = Self {
            store: Some(db),
            ..Self::new()
        };
        match stored {
            Ok(states) => {
                *registry
                    .switches
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = states.into_iter().fold(
                    HashMap::new(),
                    |mut normalized: HashMap<String, bool>, (name, enabled)| {
                        normalized
                            .entry(name.to_lowercase())
                            .and_modify(|previous| *previous &= enabled)
                            .or_insert(enabled);
                        normalized
                    },
                );
            }
            Err(error) => {
                tracing::error!(%error, "skill state unavailable; invocation disabled");
                registry
                    .state_loaded
                    .store(false, std::sync::atomic::Ordering::Release);
                *registry
                    .state_error
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) =
                    Some("Skill state could not be loaded".into());
            }
        }
        registry.register_builtin_skills();
        registry
    }

    /// Whether authoritative preferences loaded successfully; save failures keep this true.
    #[must_use]
    pub fn state_available(&self) -> bool {
        self.state_loaded.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Last persistence failure, displayed by every management entry point.
    pub fn state_error(&self) -> Option<String> {
        self.state_error
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .or_else(|| {
                self.source_error
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .map(str::to_owned)
            })
            .or_else(|| {
                self.skills
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .values()
                    .any(|skill| !skill.source_authorized())
                    .then(|| "SKILL_SOURCE_UNAUTHORIZED".to_owned())
            })
    }

    pub(super) fn set_source_error(&self, error: Option<&'static str>) {
        *self
            .source_error
            .write()
            .unwrap_or_else(PoisonError::into_inner) = error;
    }

    /// Switches use the canonical file name, so aliases cannot bypass them.
    pub fn is_enabled(&self, name: &str) -> bool {
        self.state_loaded.load(std::sync::atomic::Ordering::Acquire)
            && self
                .switches
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&name.to_lowercase())
                .copied()
                .unwrap_or(true)
    }

    /// Serialize DB/cache publication so concurrent toggles cannot reorder.
    /// # Errors
    /// Rejects unknown skills and failed transactions while retaining the last valid state.
    pub async fn set_enabled(&self, name: &str, enabled: bool) -> Result<(), zk_db::DbError> {
        let skill = self
            .resolve_including_disabled(name)
            .ok_or_else(|| zk_db::DbError::Validation("unknown skill".into()))?;
        self.set_known_enabled(&skill.name, enabled).await
    }

    /// Persist an identity already resolved inside an authorized Skill view.
    pub(super) async fn set_known_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> Result<(), zk_db::DbError> {
        let _guard = self.update_lock.lock().await;
        if !self.state_loaded.load(std::sync::atomic::Ordering::Acquire) {
            return Err(zk_db::DbError::Invalid("skill state is unavailable".into()));
        }
        if let Some(db) = &self.store
            && let Err(error) = db.set_skill_enabled(name.to_lowercase(), enabled).await
        {
            *self
                .state_error
                .write()
                .unwrap_or_else(PoisonError::into_inner) =
                Some("Skill state could not be saved; last valid state retained".into());
            return Err(error);
        }
        self.switches
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_lowercase(), enabled);
        *self
            .state_error
            .write()
            .unwrap_or_else(PoisonError::into_inner) = None;
        Ok(())
    }

    /// 载入 13 件内置技能后的注册表（旧 `@PostConstruct
    /// registerBuiltinSkills` 的等价装配入口）。
    #[must_use]
    pub fn with_builtin_skills() -> Self {
        let registry = Self::new();
        registry.register_builtin_skills();
        registry
    }

    /// 注册内置技能（旧 `registerBuiltinSkills`：13 件逐个解析入双表）。
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
        if skill.name.trim().is_empty() {
            return;
        }
        let name = skill.name.to_lowercase();
        self.builtin
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.clone(), skill.clone());
        self.register(skill);
    }

    /// 注册任意来源技能（旧 `register`，附加来源优先级守卫）。
    ///
    /// 返回 `true` = 已写入；`false` = 被更高优先级来源的同名技能挡下。
    pub fn register(&self, skill: SkillDefinition) -> bool {
        if skill.name.trim().is_empty() {
            return false;
        }
        let key = skill.name.to_lowercase();
        let mut skills = self.skills.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = skills.get(&key)
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
        skills.insert(key, skill);
        true
    }

    /// Replace only filesystem-owned definitions in one publication. The loader
    /// supplies candidates in stable source/path priority order; other runtime
    /// sources and the independent global switches remain untouched.
    pub(super) fn replace_directory_skills(
        &self,
        candidates: Vec<SkillDefinition>,
        roots: &[(SkillSource, std::path::PathBuf)],
    ) -> Vec<(Option<SkillDefinition>, Option<SkillDefinition>)> {
        let builtins = self
            .builtin
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut skills = self.skills.write().unwrap_or_else(PoisonError::into_inner);
        let previous = skills.clone();
        skills.retain(|_, skill| {
            !skill.file_path.as_deref().is_some_and(|path| {
                roots.iter().any(|(source, root)| {
                    *source == skill.source && std::path::Path::new(path).starts_with(root)
                })
            })
        });
        for (name, skill) in builtins {
            skills.entry(name).or_insert(skill);
        }
        for skill in candidates {
            if skill.name.trim().is_empty() {
                continue;
            }
            let name = skill.name.to_lowercase();
            if skills
                .get(&name)
                .is_none_or(|current| current.source.priority() <= skill.source.priority())
            {
                skills.insert(name, skill);
            }
        }
        let names = previous
            .keys()
            .chain(skills.keys())
            .collect::<std::collections::BTreeSet<_>>();
        names
            .into_iter()
            .filter_map(|name| {
                let (old, new) = (previous.get(name), skills.get(name));
                (old != new).then(|| (old.cloned(), new.cloned()))
            })
            .collect()
    }

    /// 按名称解析（旧 `resolve`：去 `/` 前缀 + 小写精确命中 → 遍历大小写
    /// 不敏感匹配 `name` / `effectiveName`）。
    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<SkillDefinition> {
        self.resolve_including_disabled(name)
            .filter(|skill| self.is_enabled(&skill.name))
    }

    /// Management-only lookup; execution must always use `resolve`.
    pub fn resolve_including_disabled(&self, name: &str) -> Option<SkillDefinition> {
        let normalized = name.strip_prefix('/').unwrap_or(name).to_lowercase();
        let skills = self.skills.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(skill) = skills
            .get(&normalized)
            .filter(|skill| skill.source_authorized())
        {
            return Some(skill.clone());
        }
        skills
            .values()
            .filter(|skill| skill.source_authorized())
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
        self.manage_skills()
            .into_iter()
            .filter(|skill| self.is_enabled(&skill.name))
            .collect()
    }

    /// Management list including disabled definitions.
    pub fn manage_skills(&self) -> Vec<SkillDefinition> {
        let mut all: Vec<SkillDefinition> = self
            .skills
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|skill| skill.source_authorized())
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
            .filter(|skill| self.is_enabled(&skill.name))
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
        let name = name.to_lowercase();
        let mut skills = self.skills.write().unwrap_or_else(PoisonError::into_inner);
        let removed = skills.remove(&name)?;
        if let Some(builtin) = self
            .builtin
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&name)
        {
            skills.insert(name.clone(), builtin.clone());
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

    #[tokio::test]
    async fn global_switches_survive_reload_and_failed_save_keeps_last_state() {
        let db = zk_db::Db::open_in_memory().unwrap();
        let registry = SkillRegistry::with_persisted_state(db.clone());
        registry.set_enabled("/FIX", false).await.unwrap();
        assert!(registry.resolve("fix").is_none());
        assert!(!registry.all_skills().iter().any(|s| s.name == "fix"));
        assert!(!registry.builtin_skills().iter().any(|s| s.name == "fix"));
        registry.register(SkillDefinition::from_markdown(
            "fix.md",
            "---\nname: custom-fix\n---\ncustom",
            SkillSource::Project,
            None,
        ));
        assert!(
            registry.resolve("custom-fix").is_none(),
            "display aliases cannot bypass canonical switch"
        );
        let restarted = SkillRegistry::with_persisted_state(db.clone());
        assert!(restarted.resolve("fix").is_none());
        db.with_conn_blocking(|conn| {
            conn.execute_batch("DROP TABLE skill_states")?;
            Ok(())
        })
        .unwrap();
        assert!(registry.set_enabled("fix", true).await.is_err());
        assert!(registry.resolve("custom-fix").is_none());
        assert!(registry.state_error().is_some());
        let unavailable = SkillRegistry::with_persisted_state(db);
        assert!(unavailable.all_skills().is_empty());
        assert!(unavailable.state_error().is_some());
    }

    #[tokio::test]
    async fn case_changed_reload_preserves_global_switch_and_invalid_names_are_skipped() {
        let db = zk_db::Db::open_in_memory().unwrap();
        let registry = SkillRegistry::with_persisted_state(db.clone());
        let before = registry.len();
        let definition = |name: &str, alias: &str, source| {
            SkillDefinition::from_markdown(
                &format!("{name}.md"),
                &format!("---\nname: {alias}\n---\nbody"),
                source,
                None,
            )
        };
        assert!(!registry.register(definition(" ", "invalid", SkillSource::User)));
        registry.register_builtin(definition("", "invalid", SkillSource::Bundled));
        assert_eq!(registry.len(), before);
        registry.register(definition("Example", "old-alias", SkillSource::Bundled));
        registry.set_enabled("EXAMPLE", false).await.unwrap();
        registry.register(definition("example", "new-alias", SkillSource::Project));
        assert_eq!(registry.len(), before + 1);
        assert!(registry.resolve("new-alias").is_none());
        assert!(registry.resolve("/EXAMPLE").is_none());
        let restarted = SkillRegistry::with_persisted_state(db);
        restarted.register(definition("Example", "new-alias", SkillSource::Project));
        assert!(restarted.resolve("new-alias").is_none());
        restarted.set_enabled("example", true).await.unwrap();
        assert!(restarted.resolve("new-alias").is_some());
    }

    #[test]
    fn migrated_project_skills_parse_with_real_tool_names_and_project_priority() {
        let directory =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.zkcode/skills");
        for name in ["batch", "deep-research", "deploy", "refactor", "skillify"] {
            let file = directory.join(format!("{name}.md"));
            let content = std::fs::read_to_string(&file).unwrap();
            let skill = SkillDefinition::from_markdown(
                &format!("{name}.md"),
                &content,
                SkillSource::Project,
                Some(file.to_string_lossy().into_owned()),
            );
            assert_eq!(skill.effective_name(), name);
            assert!(!skill.frontmatter.allowed_tools.is_empty());
            assert!(!skill.content.contains(".zhikun/"));
            assert!(skill.frontmatter.user_invocable);
            assert_eq!(skill.frontmatter.context, "inline");
        }
    }

    /// 13 件内置技能全部载入，名称与旧 `BUILTIN_SKILL_NAMES` 逐一对齐。
    #[test]
    fn builtin_skills_cover_applicable_names() {
        let registry = SkillRegistry::with_builtin_skills();
        assert_eq!(registry.len(), 13);
        assert!(registry.resolve("publish-oss").is_none());
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

    #[test]
    fn removing_bundled_publishing_does_not_block_user_owned_skills() {
        let registry = SkillRegistry::with_builtin_skills();
        let skill = SkillDefinition::from_markdown(
            "publish-oss.md",
            "User-managed workflow",
            SkillSource::User,
            Some("/user/skills/publish-oss.md".into()),
        );
        assert!(registry.register(skill));
        assert_eq!(
            registry.resolve("publish-oss").unwrap().source,
            SkillSource::User
        );
    }

    /// 内置技能 description 来源二分：带 frontmatter 者取 YAML，
    /// 无 frontmatter 者取正文首段落兜底。
    #[test]
    fn builtin_descriptions_come_from_frontmatter_or_first_paragraph() {
        let registry = SkillRegistry::with_builtin_skills();
        let debug = registry.resolve("debug").expect("debug skill");
        assert_eq!(
            debug.frontmatter.description.as_deref(),
            Some("基于复现证据定位根因，按用户授权进行最小修复；无进展时回顾假设并选择下一步")
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
        assert_eq!(registry.len(), 13, "同名覆盖不增加计数");
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
        assert_eq!(registry.len(), 13);
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
        assert_eq!(registry.builtin_skills().len(), 13);
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
