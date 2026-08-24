//! REPL 进程池管理器——语言白名单、并发上限、LRU 淘汰、空闲 / 寿命清理。
//!
//! 对照旧 `tool/repl/ReplManager.java`（250L，只读权威规格）：
//! `getOrCreate` / `createSession` / `getInterpreterCommand` / `getSession` /
//! `destroySession` / `evictLeastRecentlyUsed` / `cleanupSessions` /
//! `truncateOutput` / `getActiveSessionCount` / `getSessionIds` / `destroyAll`。
//!
//! 差异：旧 `ConcurrentHashMap<String, ReplSession>` → [`DashMap`]（分片锁，
//! 读写不互斥）；旧 `@Scheduled(fixedDelay = 60_000) cleanupSessions()` →
//! 每次 [`ReplManager::get_or_create`] 入口顺带清扫（无 Spring 容器可挂定时器，
//! 且工具型负载天然由调用驱动）。

use std::process::Stdio;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::process::Command;
use tracing::{info, warn};

use super::session::ReplSession;
use super::{IDLE_TIMEOUT, LANGUAGES, SESSION_MAX_LIFETIME};

/// 最大并发 REPL 会话数（旧 `MAX_CONCURRENT_SESSIONS = 3`）。
pub const MAX_CONCURRENT_SESSIONS: usize = 3;

/// 会话获取失败的两类原因（对照旧两条异常通道）。
#[derive(Debug)]
pub enum ReplError {
    /// 语言不在白名单（旧 `UnsupportedOperationException` /
    /// `IllegalArgumentException`）→ 工具侧回 `REPL_OPERATION_UNSUPPORTED`。
    UnsupportedLanguage(String),
    /// 解释器进程起不来（旧 `ReplManager.ReplException`）→ 工具侧回
    /// `REPL_SESSION_ERROR`。
    Spawn(String),
}

impl std::fmt::Display for ReplError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedLanguage(language) => {
                write!(formatter, "Unsupported REPL language: {language}")
            }
            Self::Spawn(detail) => {
                write!(formatter, "Failed to start REPL process: {detail}")
            }
        }
    }
}

impl std::error::Error for ReplError {}

/// 解释器 argv（**逐字**取自旧 `getInterpreterCommand` 的 switch 三分支）。
#[must_use]
pub fn interpreter_argv(language: &str) -> Option<&'static [&'static str]> {
    match language {
        "python" => Some(&["python3", "-u", "-i"]),
        "node" => Some(&["node", "--interactive"]),
        "ruby" => Some(&["irb", "--simple-prompt"]),
        _ => None,
    }
}

/// REPL 进程池（旧 `ReplManager`，Spring `@Service` → 组合根持 `Arc`）。
#[derive(Default)]
pub struct ReplManager {
    /// 会话表：`session_id` → 会话（旧 `sessions`）。
    sessions: DashMap<String, Arc<ReplSession>>,
}

impl ReplManager {
    /// 构造空进程池。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 取用或新建会话（旧 `getOrCreate`）。
    ///
    /// 顺序逐条对齐旧实现：语言校验 → 命中活会话则刷新活动时刻返回 →
    /// 命中死会话则移除 → 触顶则 LRU 淘汰 → 新建。入口先清扫过期会话
    /// （旧由 `@Scheduled` 承担）。
    ///
    /// # Errors
    /// 语言不在白名单或解释器进程 spawn 失败。
    pub fn get_or_create(
        &self,
        session_id: &str,
        language: &str,
        working_dir: &std::path::Path,
    ) -> Result<Arc<ReplSession>, ReplError> {
        let argv = interpreter_argv(language)
            .ok_or_else(|| ReplError::UnsupportedLanguage(language.to_owned()))?;

        // 返回值只对观测/测试有意义，取会话这条路径不关心清扫了几个。
        let _ = self.cleanup_sessions();

        if let Some(existing) = self.sessions.get(session_id) {
            if existing.is_alive() {
                existing.update_last_active();
                return Ok(Arc::clone(existing.value()));
            }
            drop(existing);
            // 进程已死，清理（旧 `sessions.remove(sessionId)`）。
            self.sessions.remove(session_id);
        }

        if self.sessions.len() >= MAX_CONCURRENT_SESSIONS {
            self.evict_least_recently_used();
        }

        let session = Arc::new(Self::create_session(
            session_id,
            language,
            argv,
            working_dir,
        )?);
        self.sessions
            .insert(session_id.to_owned(), Arc::clone(&session));
        info!(
            session_id = %session_id,
            language = %language,
            "REPL session created"
        );
        Ok(session)
    }

    /// spawn 解释器进程（旧 `createProcessBuilderSession`：三流 piped，
    /// **不** `redirectErrorStream` —— stdout / stderr 分离）。
    fn create_session(
        id: &str,
        language: &str,
        argv: &[&str],
        working_dir: &std::path::Path,
    ) -> Result<ReplSession, ReplError> {
        let (program, flags) = argv.split_first().expect("argv is never empty");
        let mut command = Command::new(program);
        command
            .args(flags)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // 会话被淘汰时 `Child` 已被 kill，无需 tokio 的收割保证。
            .kill_on_drop(true);
        if working_dir.is_dir() {
            command.current_dir(working_dir);
        }
        let child = command
            .spawn()
            .map_err(|error| ReplError::Spawn(error.to_string()))?;
        Ok(ReplSession::new(id, language, child))
    }

    /// 取现有会话（旧 `getSession`）。
    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<Arc<ReplSession>> {
        self.sessions
            .get(session_id)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// 销毁指定会话（旧 `destroySession`）。
    pub fn destroy_session(&self, session_id: &str) -> bool {
        match self.sessions.remove(session_id) {
            Some((_, session)) => {
                session.destroy();
                info!(session_id = %session_id, "REPL session destroyed");
                true
            }
            None => false,
        }
    }

    /// 淘汰最久未使用会话（旧 `evictLeastRecentlyUsed`）。
    pub fn evict_least_recently_used(&self) {
        let victim = self
            .sessions
            .iter()
            .min_by_key(|entry| entry.value().last_active_millis())
            .map(|entry| entry.key().clone());
        if let Some(id) = victim
            && let Some((_, session)) = self.sessions.remove(&id)
        {
            session.destroy();
            info!(session_id = %id, "REPL session evicted (LRU)");
        }
    }

    /// 清扫空闲 / 超寿命 / 已死会话（旧 `cleanupSessions`，三判据同序）。
    ///
    /// 返回被清掉的会话数，便于观测与测试断言。
    #[must_use]
    pub fn cleanup_sessions(&self) -> usize {
        let doomed: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| {
                let session = entry.value();
                let idle = session.idle_for() > IDLE_TIMEOUT;
                let expired = session.age() > SESSION_MAX_LIFETIME;
                let dead = !session.is_alive();
                if idle || expired || dead {
                    warn!(
                        session_id = %entry.key(),
                        idle, expired, dead,
                        "REPL session cleaned"
                    );
                }
                idle || expired || dead
            })
            .map(|entry| entry.key().clone())
            .collect();
        let mut cleaned = 0;
        for id in doomed {
            if let Some((_, session)) = self.sessions.remove(&id) {
                session.destroy();
                cleaned += 1;
            }
        }
        cleaned
    }

    /// 活跃会话数（旧 `getActiveSessionCount`）。
    #[must_use]
    pub fn active_session_count(&self) -> usize {
        self.sessions.len()
    }

    /// 全部会话 ID（旧 `getSessionIds`，旧返回不可变视图 → 本实现返回快照）。
    #[must_use]
    pub fn session_ids(&self) -> Vec<String> {
        self.sessions
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// 销毁全部会话（旧 `destroyAll`，进程退出前的兜底）。
    pub fn destroy_all(&self) {
        for entry in &self.sessions {
            entry.value().destroy();
        }
        self.sessions.clear();
        info!("All REPL sessions destroyed");
    }
}

impl std::fmt::Debug for ReplManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplManager")
            .field("sessions", &self.sessions.len())
            .finish()
    }
}

/// 语言白名单（`LANGUAGES` 的成员检查，供工具侧 schema 与校验共用）。
#[must_use]
pub fn is_supported_language(language: &str) -> bool {
    LANGUAGES.contains(&language)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-repl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    /// 解释器 argv 逐分支对齐旧 switch，未知语言回 `None`。
    #[test]
    fn interpreter_commands_match_legacy_switch() {
        assert_eq!(
            interpreter_argv("python"),
            Some(&["python3", "-u", "-i"][..])
        );
        assert_eq!(
            interpreter_argv("node"),
            Some(&["node", "--interactive"][..])
        );
        assert_eq!(
            interpreter_argv("ruby"),
            Some(&["irb", "--simple-prompt"][..])
        );
        assert_eq!(interpreter_argv("perl"), None);
        assert!(is_supported_language("python"));
        assert!(is_supported_language("node"));
        assert!(is_supported_language("ruby"));
        assert!(!is_supported_language("perl"));
    }

    /// 未知语言在建会话前即被拒（旧 `IllegalArgumentException` 通道）。
    #[test]
    fn unknown_language_is_rejected_before_spawn() {
        let manager = ReplManager::new();
        let error = manager
            .get_or_create("s", "perl", &temp_dir())
            .expect_err("rejected");
        assert!(matches!(error, ReplError::UnsupportedLanguage(_)));
        assert_eq!(manager.active_session_count(), 0);
        assert!(
            error
                .to_string()
                .contains("Unsupported REPL language: perl")
        );
    }

    /// 解释器缺失 → `Spawn` 错误（旧 `ReplException` 的等价通道），
    /// 且会话表不留残项（错误路径在 `insert` 之前返回）。
    #[test]
    fn spawn_failure_never_leaves_a_half_built_session() {
        let manager = ReplManager::new();
        // 直接调私有 `create_session` 走一条必然不存在的 argv——等价于
        // 白名单语言但解释器未安装的部署环境。
        let error = ReplManager::create_session(
            "ghost",
            "python",
            &["zk-no-such-interpreter-9f2a"],
            &temp_dir(),
        )
        .expect_err("spawn fails");
        assert!(matches!(error, ReplError::Spawn(_)));
        assert!(
            error
                .to_string()
                .starts_with("Failed to start REPL process: ")
        );
        assert_eq!(manager.active_session_count(), 0);
    }

    /// 同 ID 复用同一会话对象（状态跨调用保持的前提）。
    #[tokio::test]
    async fn same_session_id_reuses_the_process() {
        let manager = ReplManager::new();
        let dir = temp_dir();
        let Ok(first) = manager.get_or_create("reuse", "python", &dir) else {
            return; // 无 python3 的环境跳过。
        };
        let second = manager
            .get_or_create("reuse", "python", &dir)
            .expect("reuse");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(manager.active_session_count(), 1);
        assert_eq!(manager.session_ids(), vec!["reuse".to_owned()]);

        assert!(manager.destroy_session("reuse"));
        assert!(!manager.destroy_session("reuse"));
        assert_eq!(manager.active_session_count(), 0);
    }

    /// 并发上限触顶 → LRU 淘汰最久未用者，池容量恒 ≤ 3。
    #[tokio::test]
    async fn concurrency_cap_evicts_least_recently_used() {
        let manager = ReplManager::new();
        let dir = temp_dir();
        for index in 0..MAX_CONCURRENT_SESSIONS {
            if manager
                .get_or_create(&format!("s{index}"), "python", &dir)
                .is_err()
            {
                return; // 无 python3 的环境跳过。
            }
            // 拉开 last_active 以便 LRU 判定稳定。
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(manager.active_session_count(), MAX_CONCURRENT_SESSIONS);

        manager
            .get_or_create("s-new", "python", &dir)
            .expect("spawn");
        assert_eq!(manager.active_session_count(), MAX_CONCURRENT_SESSIONS);
        // 最早创建的 `s0` 被淘汰。
        assert!(!manager.session_ids().contains(&"s0".to_owned()));
        assert!(manager.session_ids().contains(&"s-new".to_owned()));

        manager.destroy_all();
        assert_eq!(manager.active_session_count(), 0);
    }

    /// 会话进程被外部杀死 → 清扫把它移出池（旧 `dead` 判据）。
    #[tokio::test]
    async fn cleanup_drops_dead_sessions() {
        let manager = ReplManager::new();
        let dir = temp_dir();
        let Ok(session) = manager.get_or_create("dead", "python", &dir) else {
            return;
        };
        session.destroy();
        for _ in 0..50 {
            if manager.cleanup_sessions() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(manager.active_session_count(), 0);
        assert!(manager.get_session("dead").is_none());
    }

    /// `Debug` 只暴露会话计数（不泄漏进程句柄）。
    #[test]
    fn debug_reports_session_count_only() {
        let manager = ReplManager::new();
        assert_eq!(format!("{manager:?}"), "ReplManager { sessions: 0 }");
    }
}
