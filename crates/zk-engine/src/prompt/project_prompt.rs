//! `ProjectPromptLoader`——`PROJECT.md` 六层加载器（逐字对照旧
//! `com.aicodeassistant.config.ProjectPromptLoader`）。
//!
//! 六层顺序、`rules/*.md` 展开、`@include` 展开与 60s TTL 缓存的语义见
//! [父模块文档](crate::prompt)。本文件只承载实现与单元测试。

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use zk_core::paths;

/// 项目提示主文件名（对照旧 `PROJECT_MD = "PROJECT.md"`）。
pub const PROJECT_MD_FILE: &str = "PROJECT.md";

/// 个人本地覆盖文件名（对照旧 `"PROJECT.local.md"`）。
pub const PROJECT_LOCAL_MD_FILE: &str = "PROJECT.local.md";

/// 规则目录名（相对项目配置目录，对照旧 `.zhikun/rules`）。
pub const RULES_DIR_NAME: &str = "rules";

/// 父目录向上遍历的最大层数（对照旧 `maxDepth = 5`）。
pub const MAX_PARENT_DEPTH: usize = 5;

/// 缓存过期时间（对照旧 `CACHE_TTL_MS = 60_000`）。
pub const CACHE_TTL: Duration = Duration::from_mins(1);

/// `@include` 单文件字节上限（对照旧 `size > 100_000`）。
pub const MAX_INCLUDE_BYTES: u64 = 100_000;

/// 段间分隔符（对照旧 `loadMergedContent` 的 `"\n---\n"`）。
const SECTION_SEPARATOR: &str = "\n---\n";

/// 一个 `PROJECT.md` 配置段（对照旧 record `ProjectMdSection`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectMdSection {
    /// 来源标签（如 `user` / `project` / `rule:foo.md` / `parent-0`）。
    pub label: String,
    /// 来源文件的绝对路径字符串（`@include` 展开后保留原段来源）。
    pub file_path: String,
    /// 段内容（已 `trim`）。
    pub content: String,
}

/// 缓存条目（对照旧 record `CachedConfig`）。
struct CachedConfig {
    sections: Vec<ProjectMdSection>,
    timestamp: Instant,
}

impl CachedConfig {
    fn is_expired(&self) -> bool {
        self.timestamp.elapsed() > CACHE_TTL
    }
}

/// `PROJECT.md` 六层加载器。
///
/// `user_config_dir` 是可注入的用户级配置目录（默认经
/// [`zk_core::paths::user_config_dir`] 解析为 `~/.zk`）；抽出为字段仅为单测可控
/// 用户层（Rust 2024 下 `std::env::set_var` 为 `unsafe`，本 workspace 禁止
/// `unsafe`，故不能改 `HOME`）——生产构造 [`ProjectPromptLoader::new`] 恒用默认，
/// 行为与旧实现一致。
pub struct ProjectPromptLoader {
    user_config_dir: PathBuf,
    cache: Mutex<HashMap<PathBuf, CachedConfig>>,
}

impl Default for ProjectPromptLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectPromptLoader {
    /// 生产构造：用户级目录取 [`zk_core::paths::user_config_dir`]（`~/.zk`）。
    #[must_use]
    pub fn new() -> Self {
        Self::with_user_config_dir(paths::user_config_dir())
    }

    /// 显式注入用户级配置目录的构造。
    ///
    /// 仅供本 crate 内部（`prompt::watcher` 单测）隔离真实 `~/.zk` 使用；生产
    /// 路径一律走 [`ProjectPromptLoader::new`]。
    pub(crate) fn with_user_config_dir(user_config_dir: PathBuf) -> Self {
        Self {
            user_config_dir,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// 加载并合并全部六层配置段（逐字对照旧 `loadAll`）。
    ///
    /// `working_directory` 为 `None` 时只返回用户层（不缓存、不做 `@include`
    /// 展开，对照旧 `if (workingDirectory == null) return loadUserLevel();`）。
    #[must_use]
    pub fn load_all(&self, working_directory: Option<&Path>) -> Vec<ProjectMdSection> {
        self.load_all_with_cli(working_directory, None)
    }

    /// 六层加载 + 预留 CLI 覆盖层。
    ///
    /// `cli_override` 为 `None` 时与 [`load_all`](Self::load_all) 逐字等价（当前
    /// 所有真实调用路径，因 Rust 侧尚无 CLI 层）；为 `Some` 且非空白时，将其作为
    /// 最高优先级段（label `cli`）前置。旧 Java 基线无此层，故仅作惰性预留入口
    /// 供 Batch 1 接线，见[父模块文档](crate::prompt)「CLI arg 层」小节。
    #[must_use]
    pub fn load_all_with_cli(
        &self,
        working_directory: Option<&Path>,
        cli_override: Option<&str>,
    ) -> Vec<ProjectMdSection> {
        let mut sections = match working_directory {
            None => self.load_user_level(),
            Some(cwd) => self.load_all_cached(cwd),
        };
        if let Some(cli) = cli_override {
            let trimmed = cli.trim();
            if !trimmed.is_empty() {
                sections.insert(
                    0,
                    ProjectMdSection {
                        label: "cli".to_owned(),
                        file_path: "<cli-arg>".to_owned(),
                        content: trimmed.to_owned(),
                    },
                );
            }
        }
        sections
    }

    /// 合并纯文本产出（逐字对照旧 `loadMergedContent`）。段间以 `\n---\n` 分隔，
    /// 每段形如 `# <label>\n<content>`；无任何段时返回 `None`。
    #[must_use]
    pub fn load_merged_content(&self, working_directory: Option<&Path>) -> Option<String> {
        self.load_merged_content_with_cli(working_directory, None)
    }

    /// 合并纯文本产出 + 预留 CLI 覆盖层（语义见
    /// [`load_all_with_cli`](Self::load_all_with_cli)）。
    #[must_use]
    pub fn load_merged_content_with_cli(
        &self,
        working_directory: Option<&Path>,
        cli_override: Option<&str>,
    ) -> Option<String> {
        let sections = self.load_all_with_cli(working_directory, cli_override);
        if sections.is_empty() {
            return None;
        }
        let mut buf = String::new();
        for section in &sections {
            if !buf.is_empty() {
                buf.push_str(SECTION_SEPARATOR);
            }
            buf.push_str("# ");
            buf.push_str(&section.label);
            buf.push('\n');
            buf.push_str(&section.content);
        }
        Some(buf)
    }

    /// 清空全部缓存（对照旧 `clearCache`，供 watcher 变更时调用）。
    pub fn clear_cache(&self) {
        self.cache_guard().clear();
    }

    /// 失效指定工作目录的缓存（对照旧 `invalidateCache`）。
    pub fn invalidate_cache(&self, working_directory: &Path) {
        let normalized = to_absolute_normalized(working_directory);
        self.cache_guard().remove(&normalized);
    }

    /// watcher 的候选监视路径集：六层的全部候选文件 + 规则目录（其 mtime 捕获
    /// 规则文件的增删）+ 当前存在的规则 `*.md`。`@include` 目标不在监视面内
    /// （旧 `SettingsWatcher` 亦仅监视用户配置目录，不追踪包含目标）。
    #[must_use]
    pub(crate) fn watch_paths(&self, cwd: &Path) -> Vec<PathBuf> {
        let normalized = to_absolute_normalized(cwd);
        let mut out = Vec::new();
        out.push(self.user_config_dir.join(PROJECT_MD_FILE));
        out.push(normalized.join(PROJECT_MD_FILE));
        let project_dir = paths::project_config_dir(&normalized);
        out.push(project_dir.join(PROJECT_MD_FILE));
        out.push(project_dir.join(PROJECT_LOCAL_MD_FILE));
        let rules_dir = project_dir.join(RULES_DIR_NAME);
        out.push(rules_dir.clone());
        if let Ok(entries) = std::fs::read_dir(&rules_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().ends_with(".md") {
                    out.push(path);
                }
            }
        }
        let mut parent = normalized.parent();
        let mut depth = 0;
        while let Some(dir) = parent {
            if depth >= MAX_PARENT_DEPTH {
                break;
            }
            out.push(dir.join(PROJECT_MD_FILE));
            out.push(paths::project_config_dir(dir).join(PROJECT_MD_FILE));
            parent = dir.parent();
            depth += 1;
        }
        out
    }

    // ===== 内部实现 =====

    fn cache_guard(&self) -> MutexGuard<'_, HashMap<PathBuf, CachedConfig>> {
        // 中毒恢复：读回内部值而非 panic（无锁读者不受影响，此处仅串行写者）。
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 六层加载主流程（带 60s TTL 缓存），仅在 `working_directory` 非空时走此路径。
    fn load_all_cached(&self, cwd: &Path) -> Vec<ProjectMdSection> {
        let normalized = to_absolute_normalized(cwd);

        {
            let cache = self.cache_guard();
            if let Some(cached) = cache.get(&normalized).filter(|c| !c.is_expired()) {
                return cached.sections.clone();
            }
        }

        let mut sections = self.load_user_level();

        // Layer 2：{cwd}/PROJECT.md
        if let Some(section) = load_file(&normalized.join(PROJECT_MD_FILE), "project") {
            sections.push(section);
        }
        let project_dir = paths::project_config_dir(&normalized);
        // Layer 3：{cwd}/.zk/PROJECT.md
        if let Some(section) = load_file(&project_dir.join(PROJECT_MD_FILE), "project-local") {
            sections.push(section);
        }
        // Layer 4：{cwd}/.zk/PROJECT.local.md
        if let Some(section) = load_file(&project_dir.join(PROJECT_LOCAL_MD_FILE), "local") {
            sections.push(section);
        }
        // Layer 5：{cwd}/.zk/rules/*.md（按路径升序）
        sections.extend(load_rules_directory(&project_dir));
        // Layer 6：父目录向上遍历（最多 MAX_PARENT_DEPTH 层）
        let mut parent = normalized.parent();
        let mut depth = 0;
        while let Some(dir) = parent {
            if depth >= MAX_PARENT_DEPTH {
                break;
            }
            if let Some(section) = load_file(&dir.join(PROJECT_MD_FILE), &format!("parent-{depth}"))
            {
                sections.push(section);
            }
            if let Some(section) = load_file(
                &paths::project_config_dir(dir).join(PROJECT_MD_FILE),
                &format!("parent-local-{depth}"),
            ) {
                sections.push(section);
            }
            parent = dir.parent();
            depth += 1;
        }

        // @include 展开（相对 cwd 解析）
        let sections = resolve_includes(sections, &normalized);

        self.cache_guard().insert(
            normalized,
            CachedConfig {
                sections: sections.clone(),
                timestamp: Instant::now(),
            },
        );
        sections
    }

    /// Layer 1：用户级 `~/.zk/PROJECT.md`（对照旧 `loadUserLevel`）。
    fn load_user_level(&self) -> Vec<ProjectMdSection> {
        let mut out = Vec::new();
        if let Some(section) = load_file(&self.user_config_dir.join(PROJECT_MD_FILE), "user") {
            out.push(section);
        }
        out
    }
}

/// Layer 5：扫描 `<project_dir>/rules/*.md`，按路径升序（对照旧
/// `loadRulesDirectory`：`Files.list` → 过滤 `.md` → `sorted()`）。
fn load_rules_directory(project_dir: &Path) -> Vec<ProjectMdSection> {
    let rules_dir = project_dir.join(RULES_DIR_NAME);
    let Ok(entries) = std::fs::read_dir(&rules_dir) else {
        // 目录不存在 / 非目录：与旧 `Files.isDirectory` 假分支一致，返回空。
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.to_string_lossy().ends_with(".md"))
        .collect();
    paths.sort();
    let mut sections = Vec::new();
    for path in paths {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Some(section) = load_file(&path, &format!("rule:{name}")) {
            sections.push(section);
        }
    }
    sections
}

/// 读取单文件为一个段（对照旧 `loadFile`）：仅当存在、是常规文件、内容非空白时
/// 产出；内容做 `trim`；读失败仅告警并跳过。
fn load_file(path: &Path, label: &str) -> Option<ProjectMdSection> {
    let meta = std::fs::metadata(path).ok()?;
    if !meta.is_file() {
        return None;
    }
    match std::fs::read_to_string(path) {
        Ok(content) => {
            let trimmed = content.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(ProjectMdSection {
                    label: label.to_owned(),
                    file_path: path.to_string_lossy().into_owned(),
                    content: trimmed.to_owned(),
                })
            }
        }
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "读取 PROJECT.md 失败");
            None
        }
    }
}

/// `@include` 展开（对照旧 `resolveIncludes`）：逐行匹配 `@include <path>`，命中则
/// 以相对 `base_path` 解析的目标文件内容替换该行（超 [`MAX_INCLUDE_BYTES`] 或读
/// 失败则该行被丢弃，与旧 `continue` 一致）；否则原样保留。段内容最终 `trim`。
fn resolve_includes(sections: Vec<ProjectMdSection>, base_path: &Path) -> Vec<ProjectMdSection> {
    let mut resolved = Vec::with_capacity(sections.len());
    for section in sections {
        let mut content = String::new();
        for line in section.content.split('\n') {
            if let Some(include_path) = parse_include(line) {
                // 命中 @include：无论目标是否可读，原指令行一律丢弃（旧实现无
                // else 分支，仅在成功时 append 目标内容 + 换行）。
                if let Some(text) = read_include(base_path, &include_path) {
                    content.push_str(&text);
                    content.push('\n');
                }
            } else {
                content.push_str(line);
                content.push('\n');
            }
        }
        resolved.push(ProjectMdSection {
            label: section.label,
            file_path: section.file_path,
            content: content.trim().to_owned(),
        });
    }
    resolved
}

/// 读取 `@include` 目标（相对 `base_path` 解析后词法归一）。非常规文件 / 超
/// [`MAX_INCLUDE_BYTES`] / 读失败一律返回 `None`（对齐旧 `continue` 与 catch）。
fn read_include(base_path: &Path, include_path: &str) -> Option<String> {
    let include_file = normalize_lexical(&base_path.join(include_path));
    let meta = std::fs::metadata(&include_file)
        .ok()
        .filter(std::fs::Metadata::is_file)?;
    if meta.len() > MAX_INCLUDE_BYTES {
        tracing::warn!(
            path = %include_file.display(),
            bytes = meta.len(),
            "@include 文件超上限，跳过"
        );
        return None;
    }
    match std::fs::read_to_string(&include_file) {
        Ok(text) => Some(text),
        Err(err) => {
            tracing::warn!(path = %include_file.display(), error = %err, "@include 读取失败");
            None
        }
    }
}

/// 解析一行是否为 `@include <path>`（对照旧正则 `@include\s+(.+)`，`matches()`
/// 全行匹配；先对整行 `trim`）。返回 `trim` 后的包含路径。
fn parse_include(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let after = trimmed.strip_prefix("@include")?;
    // `\s+`：`@include` 与路径之间至少一个空白（否则如 `@includefoo` 不匹配）。
    if !after.chars().next().is_some_and(is_java_whitespace) {
        return None;
    }
    let path = after.trim();
    if path.is_empty() {
        None
    } else {
        Some(path.to_owned())
    }
}

/// Java 正则 `\s` 字符类：`[ \t\n\x0B\f\r]`。
const fn is_java_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{000B}' | '\u{000C}' | '\r')
}

/// `toAbsolutePath().normalize()` 的对应：相对路径先并到进程当前目录，再做词法
/// 归一（不触盘、不解析符号链接，与 Java `Path.normalize` 一致）。
fn to_absolute_normalized(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
    };
    normalize_lexical(&absolute)
}

/// 词法归一：丢弃 `.`；`..` 弹出前一个普通分量（不越过根，越根即吞掉，对齐
/// Java `Path.normalize`）。
fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                Some(Component::RootDir | Component::Prefix(_)) => {}
                _ => out.push(component.as_os_str()),
            },
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// 唯一临时目录（进程隔离 + 计数器 + 纳秒），Drop 时递归清理。
    struct TempTree {
        root: PathBuf,
    }

    impl TempTree {
        fn new(tag: &str) -> Self {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "zk-prompt-{tag}-{}-{nanos}-{n}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("create temp root");
            Self { root }
        }

        fn write(&self, rel: &str, content: &str) -> PathBuf {
            let path = self.root.join(rel);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent dirs");
            }
            fs::write(&path, content).expect("write file");
            path
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    /// 配置目录相对路径：`<prefix>/.zk/<rel>`，目录名取自
    /// [`zk_core::paths::CONFIG_DIR_NAME`]，测试不复写字面量。
    fn cfg(prefix: &str, rel: &str) -> String {
        let dir = paths::CONFIG_DIR_NAME;
        if prefix.is_empty() {
            format!("{dir}/{rel}")
        } else {
            format!("{prefix}/{dir}/{rel}")
        }
    }

    fn labels(sections: &[ProjectMdSection]) -> Vec<String> {
        sections.iter().map(|s| s.label.clone()).collect()
    }

    #[test]
    fn six_layers_each_hit_in_priority_order() {
        let tree = TempTree::new("six");
        // 用户层（注入的 user_config_dir 下）。
        tree.write("home/PROJECT.md", "USER");
        // 工作目录 = root/proj，父目录 = root。
        tree.write("proj/PROJECT.md", "PROJECT");
        tree.write(&cfg("proj", "PROJECT.md"), "PROJECT_LOCAL");
        tree.write(&cfg("proj", "PROJECT.local.md"), "LOCAL");
        tree.write(&cfg("proj", "rules/a.md"), "RULE_A");
        tree.write(&cfg("proj", "rules/b.md"), "RULE_B");
        tree.write("PROJECT.md", "PARENT");
        tree.write(&cfg("", "PROJECT.md"), "PARENT_LOCAL");

        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let sections = loader.load_all(Some(&tree.root.join("proj")));

        let got = labels(&sections);
        let expected = [
            "user",
            "project",
            "project-local",
            "local",
            "rule:a.md",
            "rule:b.md",
            "parent-0",
            "parent-local-0",
        ];
        assert!(
            got.len() >= expected.len() && got[..expected.len()] == expected,
            "labels prefix mismatch: {got:?}"
        );
        // 各层内容对位。
        assert_eq!(sections[0].content, "USER");
        assert_eq!(sections[1].content, "PROJECT");
        assert_eq!(sections[2].content, "PROJECT_LOCAL");
        assert_eq!(sections[3].content, "LOCAL");
        assert_eq!(sections[4].content, "RULE_A");
        assert_eq!(sections[5].content, "RULE_B");
        assert_eq!(sections[6].content, "PARENT");
        assert_eq!(sections[7].content, "PARENT_LOCAL");
    }

    #[test]
    fn rules_are_expanded_in_path_sorted_order() {
        let tree = TempTree::new("rules");
        tree.write(&cfg("proj", "rules/10.md"), "TEN");
        tree.write(&cfg("proj", "rules/2.md"), "TWO");
        tree.write(&cfg("proj", "rules/1.md"), "ONE");
        tree.write(&cfg("proj", "rules/a.md"), "A");
        // 非 .md 与子目录不得进入。
        tree.write(&cfg("proj", "rules/ignore.txt"), "SKIP");
        fs::create_dir_all(tree.root.join(cfg("proj", "rules/sub.md"))).expect("dir named .md");

        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let sections = loader.load_all(Some(&tree.root.join("proj")));

        // 词法升序：1.md < 10.md < 2.md < a.md（与 Java Path.sorted 一致）。
        assert_eq!(
            labels(&sections),
            ["rule:1.md", "rule:10.md", "rule:2.md", "rule:a.md"]
        );
    }

    #[test]
    fn cli_override_is_prepended_as_highest_priority() {
        let tree = TempTree::new("cli");
        tree.write("proj/PROJECT.md", "PROJECT");
        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let cwd = tree.root.join("proj");

        let with_cli = loader.load_all_with_cli(Some(&cwd), Some("  CLI_TEXT  "));
        assert_eq!(with_cli[0].label, "cli");
        assert_eq!(with_cli[0].content, "CLI_TEXT");
        assert_eq!(with_cli[1].label, "project");

        // None 时逐字等价于 load_all（无 cli 段）。
        let without = loader.load_all_with_cli(Some(&cwd), None);
        assert_eq!(labels(&without), ["project"]);
        // 空白 cli 视为未提供（对齐 loadFile 空白跳过语义）。
        let blank = loader.load_all_with_cli(Some(&cwd), Some("   "));
        assert_eq!(labels(&blank), ["project"]);
    }

    #[test]
    fn null_working_dir_returns_user_level_only() {
        let tree = TempTree::new("null");
        tree.write("home/PROJECT.md", "USER");
        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let sections = loader.load_all(None);
        assert_eq!(labels(&sections), ["user"]);
    }

    #[test]
    fn merged_content_matches_java_format() {
        let tree = TempTree::new("merged");
        tree.write("home/PROJECT.md", "U");
        tree.write("proj/PROJECT.md", "P");
        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let merged = loader
            .load_merged_content(Some(&tree.root.join("proj")))
            .expect("merged content");
        assert_eq!(merged, "# user\nU\n---\n# project\nP");
    }

    #[test]
    fn merged_content_is_none_when_no_sections() {
        let tree = TempTree::new("empty");
        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        assert!(
            loader
                .load_merged_content(Some(&tree.root.join("proj")))
                .is_none()
        );
    }

    #[test]
    fn blank_files_are_skipped() {
        let tree = TempTree::new("blank");
        tree.write("proj/PROJECT.md", "   \n\t\n  ");
        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        assert!(loader.load_all(Some(&tree.root.join("proj"))).is_empty());
    }

    #[test]
    fn include_directive_is_expanded_relative_to_cwd() {
        let tree = TempTree::new("inc");
        tree.write("proj/PROJECT.md", "before\n@include inc.md\nafter");
        tree.write("proj/inc.md", "INCLUDED");
        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let sections = loader.load_all(Some(&tree.root.join("proj")));
        assert_eq!(sections[0].content, "before\nINCLUDED\nafter");
    }

    #[test]
    fn include_missing_or_oversize_drops_the_line() {
        let tree = TempTree::new("inc2");
        tree.write("proj/PROJECT.md", "a\n@include missing.md\nb");
        // 超上限文件：内容被丢弃，指令行也被丢弃。
        let oversize = usize::try_from(MAX_INCLUDE_BYTES + 1).expect("64-bit usize");
        let big = "Z".repeat(oversize);
        tree.write(&cfg("proj", "big.md"), &big);
        // @include 相对 cwd 解析，故目标写作 `<配置目录>/big.md`。
        let big_ref = format!("x\n@include {}/big.md\ny", paths::CONFIG_DIR_NAME);
        tree.write(&cfg("proj", "PROJECT.local.md"), &big_ref);

        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let cwd = tree.root.join("proj");
        let sections = loader.load_all(Some(&cwd));
        let project = sections
            .iter()
            .find(|s| s.label == "project")
            .expect("project section");
        assert_eq!(project.content, "a\nb");
        let local = sections
            .iter()
            .find(|s| s.label == "local")
            .expect("local section");
        assert_eq!(local.content, "x\ny");

        // 反证：同一路径换成限内文件必须被真正包含——否则上面的 "x\ny" 可能只是
        // 因路径解析错误（文件"找不到"）而假通过，掩盖超上限分支未被覆盖。
        tree.write(&cfg("proj", "big.md"), "SMALL");
        loader.clear_cache();
        let sections = loader.load_all(Some(&cwd));
        let local = sections
            .iter()
            .find(|s| s.label == "local")
            .expect("local section");
        assert_eq!(local.content, "x\nSMALL\ny");
    }

    #[test]
    fn ttl_cache_serves_stale_until_invalidated() {
        let tree = TempTree::new("cache");
        let file = tree.write("proj/PROJECT.md", "V1");
        let loader = ProjectPromptLoader::with_user_config_dir(tree.root.join("home"));
        let cwd = tree.root.join("proj");

        assert_eq!(loader.load_all(Some(&cwd))[0].content, "V1");
        // 磁盘改动但 TTL 未过期 → 命中缓存返回旧值。
        fs::write(&file, "V2").expect("rewrite");
        assert_eq!(loader.load_all(Some(&cwd))[0].content, "V1");
        // 失效后重载见新值。
        loader.invalidate_cache(&cwd);
        assert_eq!(loader.load_all(Some(&cwd))[0].content, "V2");
        // clear_cache 亦然。
        fs::write(&file, "V3").expect("rewrite");
        loader.clear_cache();
        assert_eq!(loader.load_all(Some(&cwd))[0].content, "V3");
    }

    #[test]
    fn parse_include_matches_java_regex_semantics() {
        assert_eq!(parse_include("@include foo.md"), Some("foo.md".to_owned()));
        assert_eq!(
            parse_include("  @include\tbar.md  "),
            Some("bar.md".to_owned())
        );
        assert_eq!(
            parse_include("@include   a b.md"),
            Some("a b.md".to_owned())
        );
        // 无分隔空白 / 无路径 / 非指令行 → 非匹配。
        assert_eq!(parse_include("@includefoo"), None);
        assert_eq!(parse_include("@include"), None);
        assert_eq!(parse_include("@include   "), None);
        assert_eq!(parse_include("plain line"), None);
    }

    #[test]
    fn normalize_lexical_matches_java_normalize() {
        assert_eq!(
            normalize_lexical(Path::new("/a/b/../c")),
            PathBuf::from("/a/c")
        );
        assert_eq!(
            normalize_lexical(Path::new("/a/./b")),
            PathBuf::from("/a/b")
        );
        assert_eq!(normalize_lexical(Path::new("/a/..")), PathBuf::from("/"));
        assert_eq!(normalize_lexical(Path::new("/..")), PathBuf::from("/"));
    }
}
