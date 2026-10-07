//! 技能加载器——6 级来源目录扫描与热重载轮询。
//!
//! 语义来源（旧仓库只读，`581d407b`）：
//! `backend/src/main/java/com/aicodeassistant/skill/SkillRegistry.java` 的
//! 加载与监听半边——`loadSkillsFromDir`（`Files.walk` + `.md` 过滤 + 隐藏文件
//! 跳过 + 单文件读失败仅告警）、`getProjectSkills` / `getUserSkills`、
//! `resolveWorkingDirectory`（当前目录 → 上级目录二段探测）、
//! `startWatching` / `watchLoop` / `handleFileEvent`（`WatchService` 三事件
//! + 500 ms 防抖 + `ENTRY_DELETE` 反注册）。
//!
//! # 6 级来源与目录
//!
//! | 来源 | 目录 | 旧实现 |
//! |---|---|---|
//! | `MANAGED` | `$ZK_MANAGED_SKILLS_DIR` | 仅枚举值，无加载器 |
//! | `USER` | `~/.zkcode/skills/` | `USER_SKILLS_DIR`（旧为 `~/<legacy>/skills`） |
//! | `PROJECT` | `<workspace>/.zkcode/skills/` | `PROJECT_SKILLS_DIR`（旧为 `<legacy>/skills`） |
//! | `PLUGIN` | `<workspace>/.zkcode/plugins/*/skills/` | 仅枚举值，无加载器 |
//! | `BUNDLED` | 编译期嵌入 | `ClassPathResource` |
//! | `MCP` | 无目录，运行时经 `SkillRegistry::register` 注入 | 仅枚举值 |
//!
//! 用户与项目来源同时兼容 `LEGACY_CONFIG_DIR_NAME/skills`、
//! `CONFIG_DIR_NAME/skills` 和既有 `.zkcode/skills`；同来源后者优先，
//! 来源间优先级保持不变。加载不移动或删除用户文件。
//!
//! # 热重载：轮询而非 `inotify`
//!
//! 旧实现用 `WatchService` + 500 ms 防抖。Rust 侧**不引入 `notify` crate**：
//! 其全版本许可证为 `CC0-1.0`，不在本仓库 `deny.toml` 的
//! `[licenses].allow` 白名单内（CI `cargo-deny` 会红）。改为 500 ms 周期
//! 轮询「词法路径 → (mtime, ctime, 大小, device, inode)」指纹并 diff：
//! - 新增/内容变化 → 重新解析并注册（等价 `ENTRY_CREATE` / `ENTRY_MODIFY`）；
//! - 路径消失 → 按路径反注册（等价 `ENTRY_DELETE`）；
//! - 轮询周期天然吸收半写状态，等价旧实现的 500 ms 防抖窗口。
//!
//! 扫描和读取使用已授权目录描述符；内容身份或来源在两阶段间变化时保留
//! 最近有效快照并报告错误。来源授权失效的快照不会进入发现、详情或执行。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::filesystem::{self, BoundSkillFile, FileStamp, SourceRoot};

use super::registry::{SkillDefinition, SkillRegistry, SkillSource};

/// 项目技能目录（相对 workspace 根）。
pub const PROJECT_SKILLS_DIR: &str = ".zkcode/skills";
/// 用户全局技能目录（相对 `HOME`）。
pub const USER_SKILLS_DIR: &str = ".zkcode/skills";
/// 插件根目录（相对 workspace 根；其下 `*/skills/` 为插件技能目录）。
pub const PLUGIN_ROOT_DIR: &str = ".zkcode/plugins";
/// 插件技能子目录名。
pub const PLUGIN_SKILLS_SUBDIR: &str = "skills";
/// 企业策略管理技能目录环境变量。
pub const MANAGED_SKILLS_DIR_ENV: &str = "ZK_MANAGED_SKILLS_DIR";
/// 热重载轮询周期（对齐旧 `DEBOUNCE_MS = 500`）。
pub const WATCH_INTERVAL: Duration = Duration::from_millis(500);
/// 目录递归深度上限（旧 `Files.walk` 无限深；此处设界防病态深树）。
const MAX_WALK_DEPTH: usize = 8;

/// 一个受监听的技能目录及其来源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDir {
    /// 目录绝对路径（可以不存在，扫描时静默跳过）。
    pub path: PathBuf,
    /// 该目录下技能的来源标签。
    pub source: SkillSource,
    /// Physical authority is distinct from the lexical reload/cache identity.
    pub(super) authority: SourceRoot,
}

/// 目录扫描统计（启动加载日志用）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadStats {
    /// 扫描到的技能文件数。
    pub scanned: usize,
    /// 实际写入注册表的技能数（被更高优先级来源挡下的不计）。
    pub registered: usize,
}

/// 一轮热重载的变更统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReloadStats {
    /// 新增注册。
    pub registered: usize,
    /// 内容变化后重新注册。
    pub reloaded: usize,
    /// 文件删除后反注册。
    pub unregistered: usize,
}

impl ReloadStats {
    /// 本轮是否有任何变更（决定是否落 INFO 日志）。
    #[must_use]
    pub const fn changed(&self) -> bool {
        self.registered > 0 || self.reloaded > 0 || self.unregistered > 0
    }
}

/// 轮询基线快照（路径 → 指纹）。
#[derive(Debug, Clone, Default)]
pub struct SkillSnapshot(HashMap<PathBuf, FileStamp>, bool);

impl SkillSnapshot {
    /// 立即采集一份基线（启动加载后调用，避免首轮把已加载技能当新增）。
    #[must_use]
    pub fn capture(dirs: &[SkillDir]) -> Self {
        let Ok(files) = collect_files(dirs) else {
            tracing::warn!(
                code = "SKILL_SCAN_FAILED",
                "skill baseline unavailable; next poll will retry"
            );
            return Self::default();
        };
        Self(
            files
                .into_iter()
                .flat_map(|(_, files)| files.into_iter().map(|file| (file.path, file.stamp)))
                .collect(),
            true,
        )
    }

    /// 已跟踪的文件数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// 是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// 6 级来源的目录清单（按优先级**升序**排列：先加载低优先级，
/// 高优先级后到覆盖；`BUNDLED` 走编译期嵌入、`MCP` 无目录，故不在此列）。
#[must_use]
pub fn skill_dirs(working_dir: &Path) -> Vec<SkillDir> {
    skill_dirs_with(working_dir, managed_dir_from_env(), home_dir())
}

/// 注入 `managed` / `HOME` 的目录清单构造（单测用，避免改进程环境变量）。
#[must_use]
pub fn skill_dirs_with(
    working_dir: &Path,
    managed: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Vec<SkillDir> {
    let authority = SourceRoot::new(working_dir);
    let mut dirs = project_skill_dirs(working_dir, &authority).unwrap_or_else(|error| {
        tracing::warn!(
            code = error,
            "project Skill sources could not be authorized"
        );
        compatible_skill_dirs(working_dir)
            .into_iter()
            .map(|path| SkillDir {
                path,
                source: SkillSource::Project,
                authority: authority.clone(),
            })
            .collect()
    });
    if let Some(home) = home {
        for path in compatible_skill_dirs(&home) {
            dirs.push(SkillDir {
                authority: SourceRoot::new(&path),
                path,
                source: SkillSource::User,
            });
        }
    }
    if let Some(managed) = managed {
        dirs.push(SkillDir {
            authority: SourceRoot::new(&managed),
            path: managed,
            source: SkillSource::Managed,
        });
    }
    dirs
}

/// Project/plugin discovery must use the same pinned root as existing leases.
pub(super) fn project_skill_dirs(
    working_dir: &Path,
    authority: &SourceRoot,
) -> Result<Vec<SkillDir>, &'static str> {
    let mut dirs = plugin_skill_dirs(working_dir, authority)
        .map_err(|error| filesystem::diagnostic(&error))?
        .into_iter()
        .map(|path| SkillDir {
            path,
            source: SkillSource::Plugin,
            authority: authority.clone(),
        })
        .collect::<Vec<_>>();
    dirs.extend(
        compatible_skill_dirs(working_dir)
            .into_iter()
            .map(|path| SkillDir {
                path,
                source: SkillSource::Project,
                authority: authority.clone(),
            }),
    );
    Ok(dirs)
}

/// Compatibility order within one source; existing `.zkcode` preferences win.
fn compatible_skill_dirs(root: &Path) -> [PathBuf; 3] {
    [
        root.join(zk_core::paths::LEGACY_CONFIG_DIR_NAME)
            .join("skills"),
        root.join(zk_core::paths::CONFIG_DIR_NAME).join("skills"),
        root.join(PROJECT_SKILLS_DIR),
    ]
}

/// 加载并注册全部目录来源（旧 `loadAndRegister` 的 6 级扩展版）。
pub fn load_and_register(registry: &SkillRegistry, dirs: &[SkillDir]) -> LoadStats {
    let mut stats = LoadStats::default();
    for dir in dirs {
        let skills = load_skills_from_dir(dir);
        let scanned = skills.len();
        let registered = skills
            .into_iter()
            .filter(|skill| registry.register(skill.clone()))
            .count();
        if scanned > 0 {
            tracing::info!(
                dir = %dir.path.display(),
                source = dir.source.as_str(),
                scanned,
                registered,
                "skills loaded from directory"
            );
        }
        stats.scanned += scanned;
        stats.registered += registered;
    }
    tracing::info!(
        scanned = stats.scanned,
        registered = stats.registered,
        total = registry.len(),
        "skill registry loaded"
    );
    stats
}

/// 扫描单个目录（旧 `loadSkillsFromDir`：递归 + `.md` + 跳隐藏 + 读失败告警）。
#[must_use]
pub fn load_skills_from_dir(dir: &SkillDir) -> Vec<SkillDefinition> {
    let files = match scan_dir(dir) {
        Ok(files) => files,
        Err(error) => {
            tracing::warn!(
                code = filesystem::diagnostic(&error),
                "Skill source could not be scanned"
            );
            return Vec::new();
        }
    };
    let mut result = Vec::new();
    for file in files {
        match file.read() {
            Ok(raw) => {
                if let Some(mut skill) = definition_from_file(&file.path, &raw, dir.source) {
                    skill.read_authority = Some(file.authority);
                    result.push(skill);
                }
            }
            Err(error) => {
                tracing::warn!(path = %file.path.display(), %error, "failed to read Skill file");
            }
        }
    }
    result.sort_by(|left, right| left.name.cmp(&right.name));
    result
}

/// 项目工作目录探测（旧 `resolveWorkingDirectory`：当前目录 → 上级 → 当前）。
#[must_use]
pub fn resolve_working_directory() -> Option<PathBuf> {
    let current = std::env::current_dir().ok()?;
    if compatible_skill_dirs(&current)
        .iter()
        .any(|path| path.is_dir())
    {
        return Some(current);
    }
    if let Some(parent) = current.parent()
        && compatible_skill_dirs(parent)
            .iter()
            .any(|path| path.is_dir())
    {
        return Some(parent.to_path_buf());
    }
    Some(current)
}

/// 启动热重载轮询任务（返回句柄，生命周期由调用方持有——对齐
/// `WsHub::spawn_cleanup` 的常驻任务风格）。
#[must_use]
pub fn spawn_watcher(
    registry: Arc<SkillRegistry>,
    dirs: Vec<SkillDir>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let baseline_dirs = dirs.clone();
        let baseline_registry = Arc::clone(&registry);
        let mut snapshot = match tokio::task::spawn_blocking(move || {
            // A forced first reconciliation closes the load → watcher race,
            // including a winner deleted before baseline collection.
            let mut baseline = SkillSnapshot::default();
            poll_once(&baseline_registry, &baseline_dirs, &mut baseline);
            baseline
        })
        .await
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::warn!(error = %err, "skill watcher baseline failed");
                return;
            }
        };
        tracing::info!(
            dirs = dirs.len(),
            files = snapshot.len(),
            interval_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            "skill hot reload watcher started"
        );
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // interval 首拍立即完成，吃掉。
        loop {
            ticker.tick().await;
            let registry = Arc::clone(&registry);
            let dirs = dirs.clone();
            let mut taken = std::mem::take(&mut snapshot);
            let polled = tokio::task::spawn_blocking(move || {
                let stats = poll_once(&registry, &dirs, &mut taken);
                (taken, stats)
            })
            .await;
            match polled {
                Ok((next, stats)) => {
                    snapshot = next;
                    if stats.changed() {
                        tracing::info!(
                            registered = stats.registered,
                            reloaded = stats.reloaded,
                            unregistered = stats.unregistered,
                            "skills hot reloaded"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "skill watcher poll task failed");
                }
            }
        }
    })
}

/// 单轮轮询：指纹 diff → 注册/重载/反注册（纯同步，单测直接调用）。
pub fn poll_once(
    registry: &SkillRegistry,
    dirs: &[SkillDir],
    snapshot: &mut SkillSnapshot,
) -> ReloadStats {
    poll_checked(registry, dirs, snapshot).unwrap_or_default()
}

/// Refresh with a stable diagnostic while preserving the last complete view.
pub(super) fn poll_checked(
    registry: &SkillRegistry,
    dirs: &[SkillDir],
    snapshot: &mut SkillSnapshot,
) -> Result<ReloadStats, &'static str> {
    let result = reconcile(registry, dirs, snapshot);
    registry.set_source_error(result.as_ref().err().copied());
    result
}

fn reconcile(
    registry: &SkillRegistry,
    dirs: &[SkillDir],
    snapshot: &mut SkillSnapshot,
) -> Result<ReloadStats, &'static str> {
    let current = collect_files(dirs).map_err(|error| filesystem::diagnostic(&error))?;
    let next: HashMap<PathBuf, FileStamp> = current
        .iter()
        .flat_map(|(_, files)| files.iter().map(|file| (file.path.clone(), file.stamp)))
        .collect();
    if snapshot.1 && snapshot.0 == next {
        return Ok(ReloadStats::default());
    }
    // Complete descriptor-bound scan and read before publishing any candidates.
    let mut candidates = Vec::new();
    for (source, files) in current {
        let mut directory_candidates = Vec::new();
        for file in files {
            let raw = file.read().map_err(|error| {
                if filesystem::diagnostic(&error) == "SKILL_SOURCE_UNAUTHORIZED" {
                    "SKILL_SOURCE_UNAUTHORIZED"
                } else {
                    "SKILL_READ_FAILED"
                }
            })?;
            if let Some(mut skill) = definition_from_file(&file.path, &raw, source) {
                skill.read_authority = Some(file.authority);
                directory_candidates.push(skill);
            }
        }
        directory_candidates.sort_by(|left, right| left.name.cmp(&right.name));
        candidates.extend(directory_candidates);
    }
    let roots = dirs
        .iter()
        .map(|dir| (dir.source, PathBuf::from(absolute_string(&dir.path))))
        .collect::<Vec<_>>();
    let changed = registry.replace_directory_skills(candidates, &roots);
    let mut stats = ReloadStats::default();
    for (previous, replacement) in changed {
        if previous
            .as_ref()
            .and_then(|skill| skill.file_path.as_deref())
            .is_some_and(|path| !next.contains_key(Path::new(path)))
        {
            stats.unregistered += 1;
        } else if let Some(path) = replacement
            .as_ref()
            .and_then(|skill| skill.file_path.as_deref())
        {
            if snapshot.0.contains_key(Path::new(path)) {
                stats.reloaded += 1;
            } else {
                stats.registered += 1;
            }
        }
    }
    snapshot.0 = next;
    snapshot.1 = true;
    Ok(stats)
}

/// 由文件路径 + 内容构建技能定义（文件名非法（无 `file_name`）时跳过）。
fn definition_from_file(path: &Path, raw: &str, source: SkillSource) -> Option<SkillDefinition> {
    let file_name = path.file_name()?.to_string_lossy().into_owned();
    Some(SkillDefinition::from_markdown(
        &file_name,
        raw,
        source,
        Some(absolute_string(path)),
    ))
}

/// 绝对路径字符串（对齐旧 `p.toAbsolutePath().toString()`——只做词法绝对化，
/// **不**解析符号链接：删除事件发生时文件已不存在，`canonicalize` 会失败，
/// 注册与反注册两侧必须用同一种可离线计算的路径形态）。
fn absolute_string(path: &Path) -> String {
    std::path::absolute(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

/// Scan metadata through pinned directory descriptors; keep source ordering.
fn collect_files(dirs: &[SkillDir]) -> std::io::Result<Vec<(SkillSource, Vec<BoundSkillFile>)>> {
    dirs.iter()
        .map(|dir| Ok((dir.source, scan_dir(dir)?)))
        .collect()
}

fn scan_dir(dir: &SkillDir) -> std::io::Result<Vec<BoundSkillFile>> {
    match dir.authority.source(&dir.path) {
        Ok(source) => source.scan(MAX_WALK_DEPTH),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

/// Dynamic plugin discovery uses the project anchor, including ancestor aliases.
fn plugin_skill_dirs(working_dir: &Path, authority: &SourceRoot) -> std::io::Result<Vec<PathBuf>> {
    let root = working_dir.join(PLUGIN_ROOT_DIR);
    let source = match authority.source(&root) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    Ok(source
        .child_directories()?
        .into_iter()
        .map(|path| path.join(PLUGIN_SKILLS_SUBDIR))
        .collect())
}

/// `ZK_MANAGED_SKILLS_DIR` 读取（空值视作未配置）。
fn managed_dir_from_env() -> Option<PathBuf> {
    std::env::var(MANAGED_SKILLS_DIR_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// `HOME` 读取（缺失时用户级来源整体缺席）。
fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 独立临时目录（对齐 zk-db / zk-authz 测试的 `temp_dir + uuid` 约定）。
    fn temp_root(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("zk-skill-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("temp root created");
        root
    }

    /// 写入技能文件（自动建父目录）。
    fn write_skill(dir: &Path, name: &str, body: &str) -> PathBuf {
        std::fs::create_dir_all(dir).expect("skills dir created");
        let path = dir.join(name);
        std::fs::write(&path, body).expect("skill file written");
        path
    }

    /// 目录扫描：递归子目录、跳隐藏文件、忽略非 `.md`。
    #[test]
    fn load_skills_from_dir_filters_and_recurses() {
        let root = temp_root("scan");
        let skills = root.join(PROJECT_SKILLS_DIR);
        write_skill(&skills, "deploy.md", "---\ndescription: 部署\n---\n正文");
        write_skill(&skills.join("nested"), "audit.md", "# 审计\n\n审计正文");
        write_skill(&skills, ".hidden.md", "隐藏");
        write_skill(&skills, "notes.txt", "非技能");

        let loaded = load_skills_from_dir(&SkillDir {
            path: skills.clone(),
            source: SkillSource::Project,
            authority: SourceRoot::new(&root),
        });
        let names: Vec<&str> = loaded.iter().map(|skill| skill.name.as_str()).collect();
        assert_eq!(names, vec!["audit", "deploy"]);
        for skill in &loaded {
            assert_eq!(skill.source, SkillSource::Project);
            assert!(skill.file_path.is_some(), "file path recorded");
        }
        assert_eq!(
            loaded[0].effective_description(),
            "审计正文",
            "无 frontmatter 时取正文首段落"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// 不存在的目录静默回空（旧 `Files.isDirectory` 守卫）。
    #[test]
    fn load_skills_from_missing_dir_is_empty() {
        let root = temp_root("missing");
        let loaded = load_skills_from_dir(&SkillDir {
            path: root.join("nope"),
            source: SkillSource::User,
            authority: SourceRoot::new(&root),
        });
        assert!(loaded.is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    /// 6 级目录清单按优先级升序，且插件目录被枚举。
    #[test]
    fn skill_dirs_ordered_by_ascending_priority() {
        let root = temp_root("dirs");
        let home = temp_root("home");
        let managed = temp_root("managed");
        std::fs::create_dir_all(root.join(PLUGIN_ROOT_DIR).join("git-pack").join("skills"))
            .expect("plugin skills dir");

        let dirs = skill_dirs_with(&root, Some(managed.clone()), Some(home.clone()));
        let sources: Vec<SkillSource> = dirs.iter().map(|dir| dir.source).collect();
        assert_eq!(
            sources,
            vec![
                SkillSource::Plugin,
                SkillSource::Project,
                SkillSource::Project,
                SkillSource::Project,
                SkillSource::User,
                SkillSource::User,
                SkillSource::User,
                SkillSource::Managed
            ]
        );
        assert_eq!(dirs[0].path, root.join(".zkcode/plugins/git-pack/skills"));
        assert_eq!(
            dirs[1].path,
            root.join(zk_core::paths::LEGACY_CONFIG_DIR_NAME)
                .join("skills")
        );
        assert_eq!(
            dirs[2].path,
            root.join(zk_core::paths::CONFIG_DIR_NAME).join("skills")
        );
        assert_eq!(dirs[3].path, root.join(PROJECT_SKILLS_DIR));
        assert_eq!(dirs[6].path, home.join(USER_SKILLS_DIR));
        assert_eq!(dirs[7].path, managed);
        // 无 HOME / 无 managed 时对应来源整体缺席。
        let minimal = skill_dirs_with(&root, None, None);
        assert_eq!(
            minimal.iter().map(|dir| dir.source).collect::<Vec<_>>(),
            vec![
                SkillSource::Plugin,
                SkillSource::Project,
                SkillSource::Project,
                SkillSource::Project
            ]
        );
        for dir in [root, home, managed] {
            std::fs::remove_dir_all(dir).ok();
        }
    }

    /// 加载注册：user 覆盖 project，project 覆盖 bundled。
    #[test]
    fn load_and_register_applies_source_priority() {
        let root = temp_root("priority");
        let home = temp_root("priority-home");
        write_skill(
            &root.join(PROJECT_SKILLS_DIR),
            "commit.md",
            "---\ndescription: 项目提交\n---\n项目正文",
        );
        write_skill(
            &root.join(PROJECT_SKILLS_DIR),
            "deploy.md",
            "---\ndescription: 项目部署\n---\n项目部署正文",
        );
        write_skill(
            &home.join(USER_SKILLS_DIR),
            "commit.md",
            "---\ndescription: 用户提交\n---\n用户正文",
        );

        let registry = SkillRegistry::with_builtin_skills();
        let dirs = skill_dirs_with(&root, None, Some(home.clone()));
        let stats = load_and_register(&registry, &dirs);
        assert_eq!(stats.scanned, 3);
        assert_eq!(stats.registered, 3);
        assert_eq!(registry.len(), 14, "13 内置 + deploy，commit 被同名覆盖");
        let commit = registry.resolve("commit").expect("commit skill");
        assert_eq!(commit.source, SkillSource::User);
        assert_eq!(commit.effective_description(), "用户提交");
        assert_eq!(
            registry.resolve("deploy").expect("deploy skill").source,
            SkillSource::Project
        );
        for dir in [root, home] {
            std::fs::remove_dir_all(dir).ok();
        }
    }

    /// 热重载三事件：新增 → 修改 → 删除（删除后内置版本回填）。
    #[test]
    fn poll_once_detects_create_modify_delete() {
        let root = temp_root("reload");
        let skills = root.join(PROJECT_SKILLS_DIR);
        std::fs::create_dir_all(&skills).expect("skills dir");
        let registry = SkillRegistry::with_builtin_skills();
        let dirs = skill_dirs_with(&root, None, None);
        let mut snapshot = SkillSnapshot::capture(&dirs);
        assert!(snapshot.is_empty());

        // 1. 新增自定义技能。
        let path = write_skill(&skills, "deploy.md", "---\ndescription: v1\n---\n正文 v1");
        let created = poll_once(&registry, &dirs, &mut snapshot);
        assert_eq!(
            created,
            ReloadStats {
                registered: 1,
                reloaded: 0,
                unregistered: 0
            }
        );
        assert!(created.changed());
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            registry
                .resolve("deploy")
                .expect("deploy skill")
                .effective_description(),
            "v1"
        );

        // 2. 无变化的一轮：零事件。
        assert_eq!(
            poll_once(&registry, &dirs, &mut snapshot),
            ReloadStats::default()
        );

        // 3. 内容变化 → 重新注册（长度变化即指纹变化，不依赖 mtime 精度）。
        std::fs::write(&path, "---\ndescription: v2 更新后的描述\n---\n正文 v2")
            .expect("skill rewritten");
        let reloaded = poll_once(&registry, &dirs, &mut snapshot);
        assert_eq!(reloaded.reloaded, 1);
        assert_eq!(reloaded.registered, 0);
        assert_eq!(
            registry
                .resolve("deploy")
                .expect("deploy skill")
                .effective_description(),
            "v2 更新后的描述"
        );

        // 4. 覆盖内置技能 → 内置被顶替。
        write_skill(
            &skills,
            "commit.md",
            "---\ndescription: 项目提交\n---\n正文",
        );
        let overridden = poll_once(&registry, &dirs, &mut snapshot);
        assert_eq!(overridden.registered, 1);
        assert_eq!(
            registry.resolve("commit").expect("commit skill").source,
            SkillSource::Project
        );

        // 5. 删除：自定义技能消失，被覆盖的内置技能回填。
        std::fs::remove_file(&path).expect("deploy removed");
        std::fs::remove_file(skills.join("commit.md")).expect("commit override removed");
        let removed = poll_once(&registry, &dirs, &mut snapshot);
        assert_eq!(removed.unregistered, 2);
        assert!(registry.resolve("deploy").is_none());
        assert_eq!(
            registry.resolve("commit").expect("commit skill").source,
            SkillSource::Bundled
        );
        assert_eq!(registry.len(), 13);
        assert!(snapshot.is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    /// 工作目录探测恒有结果（当前目录兜底，旧 `resolveWorkingDirectory` 语义）。
    #[test]
    fn resolve_working_directory_falls_back_to_current_dir() {
        assert!(resolve_working_directory().is_some());
    }

    /// 常量与旧实现锚点：轮询周期 = 旧防抖窗口 500 ms。
    #[test]
    fn watch_interval_matches_legacy_debounce() {
        assert_eq!(WATCH_INTERVAL, Duration::from_millis(500));
        assert_eq!(PROJECT_SKILLS_DIR, ".zkcode/skills");
        assert_eq!(USER_SKILLS_DIR, ".zkcode/skills");
    }
    #[tokio::test]
    async fn compatible_paths_reload_deterministically_and_keep_global_disable() {
        let project = temp_root("compat-project");
        let home = temp_root("compat-home");
        let paths = [
            (
                project
                    .join(zk_core::paths::LEGACY_CONFIG_DIR_NAME)
                    .join("skills"),
                "legacy project",
            ),
            (
                project.join(zk_core::paths::CONFIG_DIR_NAME).join("skills"),
                "core project",
            ),
            (project.join(PROJECT_SKILLS_DIR), "current project"),
            (
                home.join(zk_core::paths::LEGACY_CONFIG_DIR_NAME)
                    .join("skills"),
                "legacy user",
            ),
            (
                home.join(zk_core::paths::CONFIG_DIR_NAME).join("skills"),
                "core user",
            ),
            (home.join(USER_SKILLS_DIR), "current user"),
        ];
        let files = paths
            .iter()
            .map(|(dir, body)| write_skill(dir, "commit.md", body))
            .collect::<Vec<_>>();
        let db = zk_db::Db::open_in_memory().unwrap();
        let registry = SkillRegistry::with_persisted_state(db.clone());
        let dirs = skill_dirs_with(&project, None, Some(home.clone()));
        load_and_register(&registry, &dirs);
        let mut snapshot = SkillSnapshot::capture(&dirs);
        assert_eq!(registry.resolve("commit").unwrap().content, "current user");
        registry.set_enabled("commit", false).await.unwrap();
        // Editing every lower-ranked duplicate cannot replace the current winner.
        for path in &files[..5] {
            std::fs::write(path, "changed lower candidate").unwrap();
        }
        assert_eq!(
            poll_once(&registry, &dirs, &mut snapshot),
            ReloadStats::default()
        );
        assert_eq!(
            registry
                .resolve_including_disabled("commit")
                .unwrap()
                .content,
            "current user"
        );
        for index in (0..files.len()).rev() {
            std::fs::remove_file(&files[index]).unwrap();
            assert!(poll_once(&registry, &dirs, &mut snapshot).changed());
            assert!(
                registry.resolve("commit").is_none(),
                "candidate fallback cannot reenable a global switch"
            );
            let winner = registry.resolve_including_disabled("commit").unwrap();
            if index > 0 {
                assert_eq!(
                    winner.file_path.as_deref(),
                    Some(absolute_string(&files[index - 1]).as_str())
                );
            } else {
                assert_eq!(winner.source, SkillSource::Bundled);
            }
        }
        assert!(!SkillRegistry::with_persisted_state(db).is_enabled("commit"));
        std::fs::remove_dir_all(project).unwrap();
        std::fs::remove_dir_all(home).unwrap();
    }

    #[test]
    fn failed_candidate_read_retains_last_valid_registry_and_retries() {
        let root = temp_root("reload-unreadable");
        let legacy = root
            .join(zk_core::paths::LEGACY_CONFIG_DIR_NAME)
            .join("skills");
        let current = root.join(PROJECT_SKILLS_DIR);
        write_skill(&legacy, "deploy.md", "legacy valid");
        let winner = write_skill(&current, "deploy.md", "current valid");
        let registry = SkillRegistry::with_builtin_skills();
        let dirs = skill_dirs_with(&root, None, None);
        load_and_register(&registry, &dirs);
        let mut snapshot = SkillSnapshot::capture(&dirs);
        std::fs::write(&winner, [0xff, 0xfe, 0xfd]).unwrap();
        assert_eq!(
            poll_once(&registry, &dirs, &mut snapshot),
            ReloadStats::default()
        );
        assert_eq!(registry.resolve("deploy").unwrap().content, "current valid");
        std::fs::write(&winner, "restored current valid").unwrap();
        assert_eq!(poll_once(&registry, &dirs, &mut snapshot).reloaded, 1);
        assert_eq!(
            registry.resolve("deploy").unwrap().content,
            "restored current valid"
        );
        std::fs::remove_file(&winner).unwrap();
        assert_eq!(poll_once(&registry, &dirs, &mut snapshot).unregistered, 1);
        assert_eq!(registry.resolve("deploy").unwrap().content, "legacy valid");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn duplicate_files_in_one_directory_keep_stable_path_order_after_reload() {
        let root = temp_root("reload-order");
        let current = root.join(PROJECT_SKILLS_DIR);
        let first = write_skill(&current.join("a"), "deploy.md", "a candidate");
        let last = write_skill(&current.join("z"), "deploy.md", "z candidate");
        let registry = SkillRegistry::with_builtin_skills();
        let dirs = skill_dirs_with(&root, None, None);
        load_and_register(&registry, &dirs);
        let mut snapshot = SkillSnapshot::capture(&dirs);
        for iteration in 0..8 {
            std::fs::write(&first, format!("lower candidate {iteration}")).unwrap();
            poll_once(&registry, &dirs, &mut snapshot);
            assert_eq!(registry.resolve("deploy").unwrap().content, "z candidate");
        }
        std::fs::remove_file(last).unwrap();
        poll_once(&registry, &dirs, &mut snapshot);
        assert_eq!(
            registry.resolve("deploy").unwrap().content,
            "lower candidate 7"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn first_reconciliation_removes_a_file_deleted_after_startup_load() {
        let root = temp_root("watcher-baseline");
        let file = write_skill(
            &root.join(PROJECT_SKILLS_DIR),
            "deploy.md",
            "loaded before watcher",
        );
        let registry = SkillRegistry::with_builtin_skills();
        let dirs = skill_dirs_with(&root, None, None);
        load_and_register(&registry, &dirs);
        std::fs::remove_file(file).unwrap();
        let mut snapshot = SkillSnapshot::default();
        assert_eq!(poll_once(&registry, &dirs, &mut snapshot).unregistered, 1);
        assert!(registry.resolve("deploy").is_none());
        std::fs::remove_dir_all(root).unwrap();
    }
}
