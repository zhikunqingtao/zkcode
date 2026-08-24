//! `ProjectPromptWatcher`——项目提示热重载（对照旧
//! `com.aicodeassistant.service.SettingsWatcher`）。
//!
//! # 与旧实现的机制差异（计划批准的定向重设计）
//!
//! 旧 `SettingsWatcher` 用原生 `WatchService` 只监视 `~/.zhikun` 目录的
//! `.md`/`.yml` 变更事件，命中即 `clearCache()` + `configService.reload()`。
//! 本步按已批准计划改用 **5s 轮询 `fs::metadata` mtime 对比**：好处是跨平台、
//! 无原生 `notify` 供应链面，且监视面覆盖某工作目录的**全部六层来源文件**
//! （而非旧实现仅盯用户目录、项目层靠 60s TTL 兜底）——即行为是旧语义的超集。
//! 命中变更时先 `invalidate_cache(cwd)`（对齐旧 `clearCache`）再重载合并文本。
//!
//! 内容缓存用 [`arc_swap::ArcSwap`]：读路径（后续 Batch 1 系统提示动态段消费方）
//! 无锁、无写者阻塞；写仅在探测到 mtime 变更时触发一次原子换出。`@include`
//! 目标不在监视面内（旧实现同样不追踪包含目标）。

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use tokio_util::sync::CancellationToken;

use super::project_prompt::ProjectPromptLoader;

/// 默认轮询间隔（对齐计划的 5s）。
pub const WATCH_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// 某工作目录候选文件的 mtime 签名条目（路径 → 修改时间）。
type Signature = Vec<(PathBuf, SystemTime)>;

/// 项目提示热重载器：轮询 mtime，变更时把合并文本原子换入 [`ArcSwap`]。
pub struct ProjectPromptWatcher {
    loader: ProjectPromptLoader,
    working_dir: PathBuf,
    poll_interval: Duration,
    content: ArcSwap<String>,
    signature: Mutex<Signature>,
}

impl ProjectPromptWatcher {
    /// 以默认轮询间隔（[`WATCH_POLL_INTERVAL`]）构造，并立即完成一次初始加载。
    #[must_use]
    pub fn new(loader: ProjectPromptLoader, working_dir: PathBuf) -> Self {
        Self::with_poll_interval(loader, working_dir, WATCH_POLL_INTERVAL)
    }

    /// 以自定义轮询间隔构造（测试可注入短间隔以规避 5s 等待），并立即初始加载。
    #[must_use]
    pub fn with_poll_interval(
        loader: ProjectPromptLoader,
        working_dir: PathBuf,
        poll_interval: Duration,
    ) -> Self {
        let watcher = Self {
            loader,
            working_dir,
            poll_interval,
            content: ArcSwap::from_pointee(String::new()),
            signature: Mutex::new(Vec::new()),
        };
        // 初始装载：签名与内容一次到位。
        let signature = watcher.compute_signature();
        *watcher.signature_guard() = signature;
        watcher.reload_content();
        watcher
    }

    /// 当前合并文本快照（无锁读取；无任何段时为空串）。
    #[must_use]
    pub fn current(&self) -> Arc<String> {
        self.content.load_full()
    }

    /// 执行一次轮询：签名有变则重载并原子换出内容，返回是否发生变更。
    ///
    /// 供 5s 循环 [`watch`](Self::watch) 调用，亦供测试直接触发（免等真实间隔）。
    pub fn poll_once(&self) -> bool {
        let latest = self.compute_signature();
        let mut current = self.signature_guard();
        if *current == latest {
            return false;
        }
        *current = latest;
        drop(current);
        self.reload_content();
        true
    }

    /// 5s（或注入间隔）轮询循环，直至 `cancel` 取消。
    pub async fn watch(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(self.poll_interval) => {
                    let _changed = self.poll_once();
                }
            }
        }
    }

    // ===== 内部实现 =====

    fn signature_guard(&self) -> MutexGuard<'_, Signature> {
        self.signature
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// 采集候选文件的 mtime 签名（缺失文件跳过；按路径排序保证可比）。
    fn compute_signature(&self) -> Signature {
        let mut entries: Signature = self
            .loader
            .watch_paths(&self.working_dir)
            .into_iter()
            .filter_map(|path| {
                let modified = std::fs::metadata(&path).and_then(|m| m.modified()).ok()?;
                Some((path, modified))
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// 失效缓存（对齐旧 `clearCache`）后重载合并文本并原子换出。
    fn reload_content(&self) {
        self.loader.invalidate_cache(&self.working_dir);
        let merged = self
            .loader
            .load_merged_content(Some(&self.working_dir))
            .unwrap_or_default();
        self.content.store(Arc::new(merged));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_root(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("zk-watch-{tag}-{}-{nanos}-{n}", std::process::id()));
        fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn write(root: &Path, rel: &str, content: &str) -> PathBuf {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parents");
        }
        fs::write(&path, content).expect("write");
        path
    }

    /// 把文件 mtime 显式推到未来，令 mtime 对比确定性成立（免受同秒写入的分辨率影响）。
    fn bump_mtime(path: &Path) {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open for mtime");
        file.set_modified(SystemTime::now() + Duration::from_hours(1))
            .expect("set mtime");
    }

    /// 规则文件相对路径 `proj/.zk/rules/<name>`，目录名取自单一事实源。
    fn rule_path(name: &str) -> String {
        format!(
            "proj/{}/{}/{name}",
            zk_core::paths::CONFIG_DIR_NAME,
            super::super::RULES_DIR_NAME
        )
    }

    fn loader_for(root: &Path) -> ProjectPromptLoader {
        // 用 root 下不存在的 home 子目录作用户层，隔离真实 ~/.zk。
        ProjectPromptLoader::with_user_config_dir(root.join("home"))
    }

    #[test]
    fn poll_detects_mtime_change_and_swaps_content() {
        let root = temp_root("mtime");
        let file = write(&root, "proj/PROJECT.md", "V1");
        let watcher = ProjectPromptWatcher::new(loader_for(&root), root.join("proj"));
        assert!(watcher.current().contains("V1"));

        // 内容改动 + mtime 前移 → 轮询捕获。
        fs::write(&file, "V2").expect("rewrite");
        bump_mtime(&file);
        assert!(watcher.poll_once(), "mtime 变更应被捕获");
        assert!(watcher.current().contains("V2"));
        assert!(!watcher.current().contains("V1"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn poll_reports_no_change_when_untouched() {
        let root = temp_root("stable");
        write(&root, "proj/PROJECT.md", "STABLE");
        let watcher = ProjectPromptWatcher::new(loader_for(&root), root.join("proj"));
        assert!(!watcher.poll_once(), "无改动不应触发重载");
        assert!(watcher.current().contains("STABLE"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn poll_detects_newly_added_rule_file() {
        let root = temp_root("newrule");
        write(&root, &rule_path("a.md"), "RULE_A");
        let watcher = ProjectPromptWatcher::new(loader_for(&root), root.join("proj"));
        assert!(watcher.current().contains("RULE_A"));
        assert!(!watcher.current().contains("RULE_B"));

        // 新增规则文件 → 其路径进入签名 → 被捕获。
        write(&root, &rule_path("b.md"), "RULE_B");
        assert!(watcher.poll_once());
        assert!(watcher.current().contains("RULE_B"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn default_poll_interval_is_five_seconds() {
        assert_eq!(WATCH_POLL_INTERVAL, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn watch_loop_picks_up_change_within_short_interval() {
        let root = temp_root("loop");
        let file = write(&root, "proj/PROJECT.md", "L1");
        let watcher = Arc::new(ProjectPromptWatcher::with_poll_interval(
            loader_for(&root),
            root.join("proj"),
            Duration::from_millis(20),
        ));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watcher.clone().watch(cancel.clone()));

        fs::write(&file, "L2").expect("rewrite");
        bump_mtime(&file);

        // 轮询间隔 20ms，给足几个周期。
        let mut caught = false;
        for _ in 0..50 {
            if watcher.current().contains("L2") {
                caught = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
        let _ = task.await;
        assert!(caught, "5s 循环应在若干轮询内捕获变更");
        let _ = fs::remove_dir_all(&root);
    }
}
