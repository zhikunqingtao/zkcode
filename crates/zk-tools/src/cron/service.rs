//! Cron 定时任务服务——内存表 + durable 落盘 + 过期清理。
//!
//! 对照旧 `service/CronTaskService.java`（179L，只读权威规格）：
//! `MAX_JOBS = 50`、`DEFAULT_EXPIRY_DAYS = 30`、`ConcurrentHashMap<String,
//! CronTask>`、`addTask` / `listAll` / `getTask` / `remove` / `taskCount` /
//! `cleanupExpiredTasks`（`@Scheduled(fixedRate = 60000)`）、
//! `persistDurableTasks`（tmp + `ATOMIC_MOVE`）/ `loadDurableTasks`（跳过已过期）。
//! 任务 ID = `UUID.randomUUID().toString().substring(0, 8)`。
//!
//! # 有意差异（留痕 docs/compatibility.md §4）
//!
//! 1. **落盘路径**：旧 `{user.dir}/.ai-code-assistant/scheduled_tasks.json` →
//!    `{cwd}/.zk/scheduled_tasks.json`，经 [`zk_core::paths::project_config_dir`]
//!    单一事实源（Batch 0 Step 0-1 已全局统一 `.zk/`，不得自行 `join`）。
//! 2. **cron 方言**：旧用 cron-utils 的 `CronType.UNIX`（5 字段：分 时 日 月 周）；
//!    Rust 侧取 `cron` crate（6/7 字段，首段是秒）。故 [`parse_schedule`] 在解析
//!    5 字段输入时**前置补 `0` 秒段**，对外契约仍是「标准 5 字段 Unix cron」，
//!    与旧入参 schema 描述逐字一致；同时宽容接受 6 字段写法。
//! 3. **周期清理**：旧靠 Spring `@Scheduled`。本实现把清理做成显式
//!    [`CronTaskService::cleanup_expired_tasks`]，并提供
//!    [`CronTaskService::spawn_sweeper`] 由组合根按需起一条 60s 巡检任务
//!    （zk-tools 无容器，且 `build_tool_registry` 是同步函数、不保证在
//!    tokio runtime 内被调用，故不在构造期 spawn）。
//! 4. **触发派发不在本层**：旧基线同样只有过期清理，没有「到点跑 prompt」的
//!    调度器——派发由工具的 `shouldDefer()` 交给引擎侧延迟执行通道。本移植
//!    保持同一边界（zk-tools 不得依赖 zk-engine），仅负责任务台账与下次
//!    触发时刻计算（[`next_run`]）。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// 任务数上限（旧 `MAX_JOBS = 50`）。
pub const MAX_JOBS: usize = 50;

/// 任务默认寿命（旧 `DEFAULT_EXPIRY_DAYS = 30`）。
pub const DEFAULT_EXPIRY_DAYS: i64 = 30;

/// durable 落盘文件名（旧 `scheduled_tasks.json`）。
pub const DURABLE_STORE_FILE: &str = "scheduled_tasks.json";

/// 过期巡检周期（旧 `@Scheduled(fixedRate = 60000)`）。
pub const CLEANUP_INTERVAL: Duration = Duration::from_mins(1);

/// 定时任务记录（旧 `record CronTask(...)`，字段名与顺序逐条对齐）。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CronTask {
    /// 8 位短 ID（旧 `UUID…substring(0, 8)`）。
    pub id: String,
    /// 5 字段 Unix cron 表达式（旧 `cron`）。
    pub cron: String,
    /// 触发时执行的指令（旧 `prompt`）。
    pub prompt: String,
    /// 是否重复（旧 `recurring`）。
    pub recurring: bool,
    /// 是否跨重启存活（旧 `durable`）。
    pub durable: bool,
    /// 创建时刻（旧 `createdAt`）。
    pub created_at: DateTime<Utc>,
    /// 过期时刻（旧 `expiresAt`）。
    pub expires_at: DateTime<Utc>,
    /// 归属会话 / 代理（旧 `agentId`，取自 `context.sessionId()`）。
    #[serde(default)]
    pub agent_id: Option<String>,
}

impl CronTask {
    /// ISO-8601 创建时刻（旧 `createdAt.toString()`）。
    #[must_use]
    pub fn created_at_iso(&self) -> String {
        self.created_at.to_rfc3339_opts(SecondsFormat::Secs, true)
    }

    /// ISO-8601 过期时刻（旧 `expiresAt.toString()`）。
    #[must_use]
    pub fn expires_at_iso(&self) -> String {
        self.expires_at.to_rfc3339_opts(SecondsFormat::Secs, true)
    }
}

/// 任务数触顶（旧 `IllegalStateException`，工具侧回 `CRON_TASK_INVALID`）。
#[derive(Debug)]
pub struct LimitReached;

impl std::fmt::Display for LimitReached {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 逐字对齐旧 `"Maximum number of scheduled tasks reached (" + MAX_JOBS + ")"`。
        write!(
            formatter,
            "Maximum number of scheduled tasks reached ({MAX_JOBS})"
        )
    }
}

impl std::error::Error for LimitReached {}

/// 定时任务台账（旧 `CronTaskService`，Spring `@Service` → 组合根持 `Arc`）。
#[derive(Debug)]
pub struct CronTaskService {
    /// 任务表（旧 `ConcurrentHashMap` → [`DashMap`]，分片锁）。
    tasks: DashMap<String, CronTask>,
    /// durable 落盘路径（`{cwd}/.zk/scheduled_tasks.json`）。
    durable_store: PathBuf,
}

impl CronTaskService {
    /// 绑定工作目录并载入 durable 任务（旧构造器 + `loadDurableTasks`）。
    #[must_use]
    pub fn new(cwd: &Path) -> Self {
        let service = Self {
            tasks: DashMap::new(),
            durable_store: zk_core::paths::project_config_dir(cwd).join(DURABLE_STORE_FILE),
        };
        service.load_durable_tasks();
        service
    }

    /// durable 落盘路径（测试与诊断端点用）。
    #[must_use]
    pub fn durable_store(&self) -> &Path {
        &self.durable_store
    }

    /// 添加任务（旧 `addTask`）。
    ///
    /// # Errors
    /// 任务数已达 [`MAX_JOBS`] 时返回 [`LimitReached`]（旧 `IllegalStateException`）。
    pub fn add_task(
        &self,
        cron: &str,
        prompt: &str,
        recurring: bool,
        durable: bool,
        agent_id: Option<&str>,
    ) -> Result<CronTask, LimitReached> {
        if self.tasks.len() >= MAX_JOBS {
            return Err(LimitReached);
        }
        let now = Utc::now();
        let task = CronTask {
            id: short_id(),
            cron: cron.to_owned(),
            prompt: prompt.to_owned(),
            recurring,
            durable,
            created_at: now,
            expires_at: now + TimeDelta::try_days(DEFAULT_EXPIRY_DAYS).unwrap_or(TimeDelta::zero()),
            agent_id: agent_id.map(str::to_owned),
        };
        self.tasks.insert(task.id.clone(), task.clone());
        if durable {
            self.persist_durable_tasks();
        }
        info!(
            id = %task.id,
            cron = %task.cron,
            recurring, durable,
            "Cron task created"
        );
        Ok(task)
    }

    /// 列出全部任务（旧 `listAll`）。
    ///
    /// 旧返回 `ConcurrentHashMap.values()` 的无序快照；本实现按
    /// `created_at`（再 `id`）定序，让 `CronList` 输出可复现。
    #[must_use]
    pub fn list_all(&self) -> Vec<CronTask> {
        let mut tasks: Vec<CronTask> = self
            .tasks
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        tasks.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        tasks
    }

    /// 取单个任务（旧 `getTask`）。
    #[must_use]
    pub fn get_task(&self, id: &str) -> Option<CronTask> {
        self.tasks.get(id).map(|entry| entry.value().clone())
    }

    /// 删除任务（旧 `remove`；durable 任务被删则重写落盘文件）。
    pub fn remove(&self, id: &str) -> Option<CronTask> {
        let (_, removed) = self.tasks.remove(id)?;
        if removed.durable {
            self.persist_durable_tasks();
        }
        info!(id = %id, "Cron task removed");
        Some(removed)
    }

    /// 当前任务数（旧 `taskCount`）。
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// 清理过期任务（旧 `cleanupExpiredTasks`），返回清掉的条数。
    pub fn cleanup_expired_tasks(&self) -> usize {
        let now = Utc::now();
        let expired: Vec<String> = self
            .tasks
            .iter()
            .filter(|entry| now > entry.value().expires_at)
            .map(|entry| entry.key().clone())
            .collect();
        let mut has_durable = false;
        let mut cleaned = 0;
        for id in expired {
            if let Some((_, task)) = self.tasks.remove(&id) {
                has_durable |= task.durable;
                cleaned += 1;
                info!(id = %id, "Expired cron task cleaned");
            }
        }
        if has_durable {
            self.persist_durable_tasks();
        }
        cleaned
    }

    /// 起一条 60s 过期巡检任务（旧 `@Scheduled(fixedRate = 60000)` 的等价物）。
    ///
    /// 由组合根在已有 tokio runtime 的上下文调用；返回的句柄丢弃即长驻。
    #[must_use]
    pub fn spawn_sweeper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(CLEANUP_INTERVAL);
            loop {
                ticker.tick().await;
                let cleaned = service.cleanup_expired_tasks();
                if cleaned > 0 {
                    debug!(cleaned, "Cron sweeper pass");
                }
            }
        })
    }

    // ═══ 持久化（旧 persistDurableTasks / loadDurableTasks） ═══

    /// 把 durable 任务写进落盘文件（tmp + rename，旧 `ATOMIC_MOVE`）。
    ///
    /// 失败只记日志、不上抛——与旧 `catch (IOException) { log.error(...) }` 同口径：
    /// 落盘是尽力而为，内存台账已经生效。
    fn persist_durable_tasks(&self) {
        let durables: Vec<CronTask> = self
            .tasks
            .iter()
            .filter(|entry| entry.value().durable)
            .map(|entry| entry.value().clone())
            .collect();
        if let Err(error) = self.write_durables(&durables) {
            warn!(
                path = %self.durable_store.display(),
                %error,
                "Failed to persist durable cron tasks"
            );
            return;
        }
        debug!(count = durables.len(), "Durable cron tasks persisted");
    }

    /// 落盘实作（拆出来让错误处理集中在 [`Self::persist_durable_tasks`]）。
    fn write_durables(&self, durables: &[CronTask]) -> std::io::Result<()> {
        if let Some(parent) = self.durable_store.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(durables).map_err(std::io::Error::other)?;
        let tmp = self.durable_store.with_extension("json.tmp");
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, &self.durable_store)
    }

    /// 载入 durable 任务（旧 `loadDurableTasks`：文件不存在直接返回，
    /// 已过期的条目跳过，解析失败只 warn）。
    fn load_durable_tasks(&self) {
        let Ok(body) = std::fs::read_to_string(&self.durable_store) else {
            return;
        };
        match serde_json::from_str::<Vec<CronTask>>(&body) {
            Ok(loaded) => {
                let now = Utc::now();
                for task in loaded {
                    if now < task.expires_at {
                        self.tasks.insert(task.id.clone(), task);
                    }
                }
                info!(
                    count = self.tasks.len(),
                    path = %self.durable_store.display(),
                    "Loaded durable cron tasks"
                );
            }
            Err(error) => warn!(%error, "Failed to load durable cron tasks"),
        }
    }
}

/// 8 位短 ID（旧 `UUID.randomUUID().toString().substring(0, 8)`）。
fn short_id() -> String {
    uuid::Uuid::new_v4()
        .to_string()
        .chars()
        .take(8)
        .collect::<String>()
}

/// 解析 cron 表达式（旧 `cronParser.parse(cron)` + `parsed.validate()`）。
///
/// 对外契约是标准 5 字段 Unix cron（分 时 日 月 周）；`cron` crate 要求首段为
/// 秒，故 5 字段输入自动前置 `0`。6 / 7 字段写法原样透传（宽容扩展）。
///
/// # Errors
/// 字段数非法或任一字段不可解析时返回可直接回给模型的原因串。
pub fn parse_schedule(expression: &str) -> Result<::cron::Schedule, String> {
    use std::str::FromStr as _;

    let trimmed = expression.trim();
    let fields = trimmed.split_whitespace().count();
    let normalized = match fields {
        5 => format!("0 {trimmed}"),
        6 | 7 => trimmed.to_owned(),
        other => {
            return Err(format!(
                "expected a 5-field Unix cron expression \
                 (minute hour day-of-month month day-of-week), got {other} field(s)"
            ));
        }
    };
    ::cron::Schedule::from_str(&normalized).map_err(|error| error.to_string())
}

/// 下次触发时刻的 ISO-8601 串（旧 `ExecutionTime.nextExecution(now)`）。
///
/// 无未来匹配时返回 `None`（旧 `Optional.empty()` → 校验期回 `INVALID_CRON`，
/// 执行期回字面量 `"unknown"`）。
#[must_use]
pub fn next_run(schedule: &::cron::Schedule) -> Option<String> {
    schedule
        .after(&Utc::now())
        .next()
        .map(|at| at.to_rfc3339_opts(SecondsFormat::Secs, true))
}

/// 按字符数截断并追加 `"..."`（旧 `substring(0, n) + "..."` 的等价物）。
///
/// 旧按 UTF-16 码元切分；本实现按字符边界，绝不切开多字节字符。
#[must_use]
pub fn clip(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    let head: String = text.chars().take(max_chars).collect();
    format!("{head}...")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(tag: &str) -> CronTaskService {
        let cwd = std::env::temp_dir().join(format!("zk-cron-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cwd);
        std::fs::create_dir_all(&cwd).expect("mkdir");
        CronTaskService::new(&cwd)
    }

    /// 5 字段输入被补秒段后可解析；6/7 字段原样透传；字段数非法被拒。
    #[test]
    fn five_field_unix_expressions_are_accepted() {
        assert!(parse_schedule("*/5 * * * *").is_ok());
        assert!(parse_schedule("30 9 1,15 5-8 Mon").is_ok());
        assert!(parse_schedule("0 30 9 1 5 Mon").is_ok());

        let error = parse_schedule("* *").expect_err("too few");
        assert!(error.contains("5-field"), "{error}");
        let error = parse_schedule("not-a-cron").expect_err("one field");
        assert!(error.contains("got 1 field(s)"), "{error}");
        assert!(parse_schedule("bogus * * * *").is_err());
    }

    /// 下次触发时刻是 ISO-8601 Z 串（旧 `ZonedDateTime::toString` 的等价形态）。
    #[test]
    fn next_run_is_iso8601_utc() {
        let schedule = parse_schedule("* * * * *").expect("parse");
        let next = next_run(&schedule).expect("has a future match");
        assert!(next.ends_with('Z'), "{next}");
        assert_eq!(next.len(), "2026-01-01T00:00:00Z".len(), "{next}");
    }

    /// 增删查计数四联，8 位 ID，30 天寿命（旧 `DEFAULT_EXPIRY_DAYS`）。
    #[test]
    fn crud_matches_legacy_semantics() {
        let service = service("crud");
        assert_eq!(service.task_count(), 0);

        let task = service
            .add_task("*/5 * * * *", "say hi", true, false, Some("sess-1"))
            .expect("added");
        assert_eq!(task.id.len(), 8);
        assert!(task.recurring);
        assert!(!task.durable);
        assert_eq!(task.agent_id.as_deref(), Some("sess-1"));
        let lifetime = task.expires_at - task.created_at;
        assert_eq!(lifetime.num_days(), DEFAULT_EXPIRY_DAYS);
        assert!(task.created_at_iso().ends_with('Z'));
        assert!(task.expires_at_iso().ends_with('Z'));

        assert_eq!(service.task_count(), 1);
        assert_eq!(service.get_task(&task.id).expect("found").cron, task.cron);
        assert_eq!(service.list_all().len(), 1);

        assert_eq!(service.remove(&task.id).expect("removed").id, task.id);
        assert!(service.remove(&task.id).is_none());
        assert_eq!(service.task_count(), 0);
    }

    /// 触顶回 `LimitReached`，文案逐字对齐旧 `IllegalStateException`。
    #[test]
    fn task_cap_is_enforced() {
        let service = service("cap");
        for index in 0..MAX_JOBS {
            service
                .add_task("* * * * *", &format!("job {index}"), true, false, None)
                .expect("added");
        }
        assert_eq!(service.task_count(), MAX_JOBS);
        let error = service
            .add_task("* * * * *", "one too many", true, false, None)
            .expect_err("capped");
        assert_eq!(
            error.to_string(),
            "Maximum number of scheduled tasks reached (50)"
        );
    }

    /// durable 任务落 `.zk/scheduled_tasks.json` 并被新实例载回；
    /// 非 durable 任务不落盘。
    #[test]
    fn durable_tasks_survive_a_restart() {
        let cwd = std::env::temp_dir().join(format!("zk-cron-durable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cwd);
        std::fs::create_dir_all(&cwd).expect("mkdir");

        let first = CronTaskService::new(&cwd);
        let kept = first
            .add_task("0 3 * * *", "nightly", true, true, None)
            .expect("added");
        first
            .add_task("0 4 * * *", "ephemeral", true, false, None)
            .expect("added");
        assert!(first.durable_store().is_file());
        assert!(
            first.durable_store().ends_with(".zk/scheduled_tasks.json"),
            "{}",
            first.durable_store().display()
        );

        let reloaded = CronTaskService::new(&cwd);
        assert_eq!(reloaded.task_count(), 1);
        assert_eq!(reloaded.get_task(&kept.id).expect("kept").prompt, "nightly");

        // durable 任务被删 → 落盘文件同步为空表。
        reloaded.remove(&kept.id);
        let third = CronTaskService::new(&cwd);
        assert_eq!(third.task_count(), 0);
    }

    /// 过期任务被清扫（直接改写 `expires_at` 到过去以免等 30 天）。
    #[test]
    fn expired_tasks_are_cleaned() {
        let service = service("expiry");
        let task = service
            .add_task("* * * * *", "stale", true, false, None)
            .expect("added");
        if let Some(mut entry) = service.tasks.get_mut(&task.id) {
            entry.expires_at = Utc::now() - TimeDelta::try_seconds(1).expect("delta");
        }
        assert_eq!(service.cleanup_expired_tasks(), 1);
        assert_eq!(service.task_count(), 0);
        assert_eq!(service.cleanup_expired_tasks(), 0);
    }

    /// 损坏的落盘文件只 warn，不阻断构造（旧 `catch (IOException) { log.warn }`）。
    #[test]
    fn corrupt_durable_store_is_tolerated() {
        let cwd = std::env::temp_dir().join(format!("zk-cron-corrupt-{}", std::process::id()));
        let dir = zk_core::paths::project_config_dir(&cwd);
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(dir.join(DURABLE_STORE_FILE), "{ not json").expect("write");
        let service = CronTaskService::new(&cwd);
        assert_eq!(service.task_count(), 0);
    }

    /// 列表按创建时刻定序（旧无序 `values()` 的可复现替代）。
    #[test]
    fn listing_is_deterministically_ordered() {
        let service = service("order");
        for index in 0..5 {
            service
                .add_task("* * * * *", &format!("job {index}"), true, false, None)
                .expect("added");
        }
        let ids: Vec<String> = service.list_all().into_iter().map(|task| task.id).collect();
        let again: Vec<String> = service.list_all().into_iter().map(|task| task.id).collect();
        assert_eq!(ids, again);
        assert_eq!(ids.len(), 5);
    }

    /// 字符截断不切开多字节字符，且超限才追加省略号。
    #[test]
    fn clip_respects_character_boundaries() {
        assert_eq!(clip("short", 80), "short");
        assert_eq!(clip("中文很长的一段提示", 4), "中文很长...");
        assert_eq!(clip("abcdef", 6), "abcdef");
        assert_eq!(clip("abcdefg", 6), "abcdef...");
    }

    /// 巡检任务能起、能清、能中止。
    #[tokio::test(start_paused = true)]
    async fn sweeper_cleans_on_its_interval() {
        let service = Arc::new(service("sweeper"));
        let task = service
            .add_task("* * * * *", "stale", true, false, None)
            .expect("added");
        if let Some(mut entry) = service.tasks.get_mut(&task.id) {
            entry.expires_at = Utc::now() - TimeDelta::try_seconds(1).expect("delta");
        }
        let handle = service.spawn_sweeper();
        tokio::time::sleep(CLEANUP_INTERVAL * 2).await;
        assert_eq!(service.task_count(), 0);
        handle.abort();
    }
}
