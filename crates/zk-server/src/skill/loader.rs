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
//! 上表 `<legacy>` 指旧布局目录名，其字面量在全仓只有一处定义——
//! `zk_core::paths::LEGACY_CONFIG_DIR_NAME`（#65）。目录名从旧布局迁到
//! `.zkcode`：与本仓库既有用户态路径约定一致（`~/.zkcode/python.sock`）。
//! `MANAGED` / `PLUGIN` 的目录约定为本次补齐（旧仓库只有枚举值），`MCP`
//! 保留编程注册入口不做目录扫描。
//!
//! 待办（#65 报备，另立任务）：本仓库当前三套目录约定并存——`.zk/`
//!（zk-core 基座 + `data.db` + scratchpad）、`.zkcode/`（技能与侧车 socket）、
//! 旧布局（仅迁移源与保护面）。技能目录向 `.zk/skills` 的收敛需连带改
//! `registry` 断言与侧车 UDS 路径，不在 #65 范围内。
//!
//! # 热重载：轮询而非 `inotify`
//!
//! 旧实现用 `WatchService` + 500 ms 防抖。Rust 侧**不引入 `notify` crate**：
//! 其全版本许可证为 `CC0-1.0`，不在本仓库 `deny.toml` 的
//! `[licenses].allow` 白名单内（CI `cargo-deny` 会红）。改为 500 ms 周期
//! 轮询「路径 → (mtime, 大小)」指纹并 diff：
//! - 新增/内容变化 → 重新解析并注册（等价 `ENTRY_CREATE` / `ENTRY_MODIFY`）；
//! - 路径消失 → 按路径反注册（等价 `ENTRY_DELETE`）；
//! - 轮询周期天然吸收半写状态，等价旧实现的 500 ms 防抖窗口。
//!
//! 差异留痕：文件在一个周期内「改回原样且大小与 mtime 不变」不会触发重载；
//! 反之旧 `WatchService` 会各触发一次事件（对技能表终态无影响）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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

/// 文件指纹（mtime + 字节数；`mtime` 不可用的文件系统上退化为按大小判定）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    /// 最后修改时刻。
    modified: Option<SystemTime>,
    /// 字节数。
    len: u64,
}

/// 轮询基线快照（路径 → 指纹）。
#[derive(Debug, Clone, Default)]
pub struct SkillSnapshot(HashMap<PathBuf, FileStamp>);

impl SkillSnapshot {
    /// 立即采集一份基线（启动加载后调用，避免首轮把已加载技能当新增）。
    #[must_use]
    pub fn capture(dirs: &[SkillDir]) -> Self {
        let mut snapshot = HashMap::new();
        for (path, (stamp, _)) in collect_files(dirs) {
            snapshot.insert(path, stamp);
        }
        Self(snapshot)
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
    let mut dirs = Vec::new();
    // PLUGIN（最低）→ PROJECT → USER → MANAGED（最高）。
    for path in plugin_skill_dirs(working_dir) {
        dirs.push(SkillDir {
            path,
            source: SkillSource::Plugin,
        });
    }
    dirs.push(SkillDir {
        path: working_dir.join(PROJECT_SKILLS_DIR),
        source: SkillSource::Project,
    });
    if let Some(home) = home {
        dirs.push(SkillDir {
            path: home.join(USER_SKILLS_DIR),
            source: SkillSource::User,
        });
    }
    if let Some(managed) = managed {
        dirs.push(SkillDir {
            path: managed,
            source: SkillSource::Managed,
        });
    }
    dirs
}

/// 加载并注册全部目录来源（旧 `loadAndRegister` 的 6 级扩展版）。
pub fn load_and_register(registry: &SkillRegistry, dirs: &[SkillDir]) -> LoadStats {
    let mut stats = LoadStats::default();
    for dir in dirs {
        let skills = load_skills_from_dir(&dir.path, dir.source);
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
pub fn load_skills_from_dir(dir: &Path, source: SkillSource) -> Vec<SkillDefinition> {
    let mut result = Vec::new();
    for path in walk_markdown(dir, 0) {
        match std::fs::read_to_string(&path) {
            Ok(raw) => {
                if let Some(skill) = definition_from_file(&path, &raw, source) {
                    result.push(skill);
                }
            }
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "failed to read skill file");
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
    if current.join(PROJECT_SKILLS_DIR).is_dir() {
        return Some(current);
    }
    if let Some(parent) = current.parent()
        && parent.join(PROJECT_SKILLS_DIR).is_dir()
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
        let mut snapshot =
            match tokio::task::spawn_blocking(move || SkillSnapshot::capture(&baseline_dirs)).await
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
    let mut stats = ReloadStats::default();
    let current = collect_files(dirs);
    let mut next: HashMap<PathBuf, FileStamp> = HashMap::new();

    for (path, (stamp, source)) in &current {
        let previous = snapshot.0.get(path);
        if previous == Some(stamp) {
            next.insert(path.clone(), *stamp);
            continue;
        }
        match std::fs::read_to_string(path) {
            Ok(raw) => {
                if let Some(skill) = definition_from_file(path, &raw, *source)
                    && registry.register(skill)
                {
                    if previous.is_some() {
                        stats.reloaded += 1;
                    } else {
                        stats.registered += 1;
                    }
                }
                next.insert(path.clone(), *stamp);
            }
            Err(err) => {
                // 读失败（半写 / 权限）不记指纹，下一周期重试。
                tracing::warn!(path = %path.display(), error = %err, "skill reload read failed");
            }
        }
    }
    for path in snapshot.0.keys() {
        if current.contains_key(path) {
            continue;
        }
        if registry
            .unregister_by_path(&absolute_string(path))
            .is_some()
        {
            stats.unregistered += 1;
            tracing::info!(path = %path.display(), "skill unregistered (file removed)");
        }
    }
    snapshot.0 = next;
    stats
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

/// 采集全部目录的文件指纹（路径 → (指纹, 来源)）。
fn collect_files(dirs: &[SkillDir]) -> HashMap<PathBuf, (FileStamp, SkillSource)> {
    let mut files = HashMap::new();
    for dir in dirs {
        for path in walk_markdown(&dir.path, 0) {
            let Ok(meta) = std::fs::metadata(&path) else {
                continue;
            };
            let stamp = FileStamp {
                modified: meta.modified().ok(),
                len: meta.len(),
            };
            // 同一路径被多个来源覆盖时以后者（更高优先级）为准。
            files.insert(path, (stamp, dir.source));
        }
    }
    files
}

/// 递归收集 `.md` 文件（跳隐藏项、不跟随符号链接、深度设界）。
fn walk_markdown(dir: &Path, depth: usize) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if depth > MAX_WALK_DEPTH || !dir.is_dir() {
        return result;
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::warn!(dir = %dir.display(), error = %err, "failed to scan skills directory");
            return result;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            result.extend(walk_markdown(&path, depth + 1));
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "md") {
            result.push(path);
        }
    }
    result.sort();
    result
}

/// 插件技能目录枚举（`<workspace>/.zkcode/plugins/*/skills`）。
///
/// 目录清单在装配时定格：进程运行期新装插件需重启才纳入监听（旧仓库无插件
/// 技能加载器，此为补齐实现的已知边界）。
fn plugin_skill_dirs(working_dir: &Path) -> Vec<PathBuf> {
    let root = working_dir.join(PLUGIN_ROOT_DIR);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        .map(|entry| entry.path().join(PLUGIN_SKILLS_SUBDIR))
        .filter(|path| path.is_dir())
        .collect();
    dirs.sort();
    dirs
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

        let loaded = load_skills_from_dir(&skills, SkillSource::Project);
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
        let loaded = load_skills_from_dir(&root.join("nope"), SkillSource::User);
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
                SkillSource::User,
                SkillSource::Managed
            ]
        );
        assert_eq!(dirs[0].path, root.join(".zkcode/plugins/git-pack/skills"));
        assert_eq!(dirs[1].path, root.join(PROJECT_SKILLS_DIR));
        assert_eq!(dirs[2].path, home.join(USER_SKILLS_DIR));
        assert_eq!(dirs[3].path, managed);
        // 无 HOME / 无 managed 时对应来源整体缺席。
        let minimal = skill_dirs_with(&root, None, None);
        assert_eq!(
            minimal.iter().map(|dir| dir.source).collect::<Vec<_>>(),
            vec![SkillSource::Plugin, SkillSource::Project]
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
        assert_eq!(registry.len(), 15, "14 内置 + deploy，commit 被同名覆盖");
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
        assert_eq!(registry.len(), 14);
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
}
