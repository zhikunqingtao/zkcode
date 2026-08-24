//! 后台代理追踪器——对照旧 `BackgroundAgentTracker.java`（272L）。

#![allow(clippy::doc_markdown)]
//!
//! 管理异步启动的子代理的生命周期和进度。经 [`tokio::sync::Notify`]
//! 实现 `await_all` 等待语义（Java 用 `ReentrantLock` + `Condition`）。
//!
//! 有意差异：Java 用 `ConcurrentHashMap` + `@Scheduled` 定时清理，
//! 本实现取 `DashMap` + 手动 `cleanup_stale` 调用（调用方按需触发）。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::agent::types::AgentResult;

/// 已完成/失败代理记录的保留时长（对照旧 `30 minutes`）。
const STALE_THRESHOLD: Duration = Duration::from_mins(30);

/// 单个代理追踪状态（对照旧 `AgentStatus` record）。
#[derive(Clone, Debug)]
pub struct AgentTrackingState {
    /// 代理 ID。
    pub agent_id: String,
    /// 所属会话 ID。
    pub session_id: String,
    /// 任务描述。
    pub prompt: String,
    /// 输出文件路径。
    pub output_file: Option<String>,
    /// 状态字符串（`running` / `completed` / `failed`）。
    pub status: String,
    /// 启动时间（epoch 毫秒）。
    pub started_at: i64,
    /// 完成时间（epoch 毫秒，未完成时 `None`）。
    pub completed_at: Option<i64>,
    /// 错误信息。
    pub error: Option<String>,
}

impl AgentTrackingState {
    /// 构造 running 状态。
    fn running(
        agent_id: String,
        session_id: String,
        prompt: String,
        output_file: Option<String>,
    ) -> Self {
        let started_at = now_millis();
        Self {
            agent_id,
            session_id,
            prompt,
            output_file,
            status: "running".to_owned(),
            started_at,
            completed_at: None,
            error: None,
        }
    }

    /// 是否仍在运行。
    fn is_running(&self) -> bool {
        self.status == "running"
    }
}

/// 当前 epoch 毫秒。
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

/// 后台代理追踪器。
pub struct BackgroundAgentTracker {
    /// 活跃代理映射：agent_id → 状态。
    agents: DashMap<String, AgentTrackingState>,
    /// 会话级通知：session_id → Notify。
    notifies: DashMap<String, Arc<Notify>>,
}

use std::sync::Arc;

impl Default for BackgroundAgentTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl BackgroundAgentTracker {
    /// 构造空追踪器。
    #[must_use]
    pub fn new() -> Self {
        Self {
            agents: DashMap::new(),
            notifies: DashMap::new(),
        }
    }

    /// 注册后台代理（对照旧 `register`）。
    pub fn register(
        &self,
        agent_id: impl Into<String>,
        session_id: impl Into<String>,
        prompt: impl Into<String>,
        output_file: Option<String>,
    ) {
        let agent_id = agent_id.into();
        let session_id = session_id.into();
        let prompt = prompt.into();
        info!("Background agent registered: {agent_id} (session: {session_id})");
        self.agents.insert(
            agent_id.clone(),
            AgentTrackingState::running(agent_id, session_id, prompt, output_file),
        );
    }

    /// 标记代理完成（对照旧 `markCompleted`）。
    pub fn mark_completed(&self, agent_id: &str, _result: &AgentResult) {
        if let Some(mut entry) = self.agents.get_mut(agent_id) {
            "completed".clone_into(&mut entry.status);
            entry.completed_at = Some(now_millis());
            let session_id = entry.session_id.clone();
            drop(entry);
            self.signal_session(&session_id);
            info!("Background agent completed: {agent_id}");
        }
    }

    /// 标记代理失败（对照旧 `markFailed`）。
    pub fn mark_failed(&self, agent_id: &str, error: &str) {
        if let Some(mut entry) = self.agents.get_mut(agent_id) {
            "failed".clone_into(&mut entry.status);
            entry.completed_at = Some(now_millis());
            entry.error = Some(error.to_owned());
            let session_id = entry.session_id.clone();
            drop(entry);
            self.signal_session(&session_id);
            warn!("Background agent failed: {agent_id} — {error}");
        }
    }

    /// 列出指定会话的活跃代理（对照旧 `listActive`）。
    #[must_use]
    pub fn list_active(&self, session_id: Option<&str>) -> Vec<AgentTrackingState> {
        self.agents
            .iter()
            .filter(|e| e.is_running())
            .filter(|e| session_id.is_none_or(|s| s == e.session_id))
            .map(|e| e.clone())
            .collect()
    }

    /// 获取指定代理状态。
    #[must_use]
    pub fn get_status(&self, agent_id: &str) -> Option<AgentTrackingState> {
        self.agents.get(agent_id).map(|e| e.clone())
    }

    /// 获取指定会话中仍在运行的代理 ID 列表（对照旧 `getActiveAgentIds`）。
    #[must_use]
    pub fn get_active_agent_ids(&self, session_id: &str) -> Vec<String> {
        self.agents
            .iter()
            .filter(|e| e.session_id == session_id && e.is_running())
            .map(|e| e.agent_id.clone())
            .collect()
    }

    /// 等待指定会话的所有后台代理完成（对照旧 `awaitAllAgents`）。
    ///
    /// # 返回
    /// `true` = 全部完成，`false` = 超时。
    pub async fn await_all(&self, session_id: &str, timeout_duration: Duration) -> bool {
        loop {
            let running = self.get_active_agent_ids(session_id);
            if running.is_empty() {
                return true;
            }
            let notify = self
                .notifies
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone();
            let timeout_result = tokio::time::timeout(timeout_duration, notify.notified()).await;
            if timeout_result.is_err() {
                warn!(
                    "Timeout waiting for {} agents in session {session_id}",
                    running.len()
                );
                return false;
            }
        }
    }

    /// 清理会话相关的所有追踪数据（对照旧 `removeSession`）。
    pub fn remove_session(&self, session_id: &str) {
        self.agents.retain(|_, v| v.session_id != session_id);
        self.notifies.remove(session_id);
        debug!("Removed tracking data for session {session_id}");
    }

    /// 清理超过 30 分钟的已完成/失败代理记录（对照旧 `cleanup`）。
    pub fn cleanup_stale(&self) {
        let cutoff = now_millis() - i64::try_from(STALE_THRESHOLD.as_millis()).unwrap_or(0);
        let mut removed = 0;
        self.agents.retain(|_, status| {
            if status.is_running() {
                return true;
            }
            if let Some(completed) = status.completed_at
                && completed < cutoff
            {
                removed += 1;
                return false;
            }
            true
        });
        if removed > 0 {
            info!("Cleaned up {removed} stale agent records");
        }
    }

    /// 唤醒等待指定会话代理完成的线程（对照旧 `signalSession`）。
    fn signal_session(&self, session_id: &str) {
        if let Some(notify) = self.notifies.get(session_id) {
            notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_list() {
        let tracker = BackgroundAgentTracker::new();
        tracker.register("a1", "s1", "prompt1", None);
        tracker.register("a2", "s1", "prompt2", None);
        tracker.register("a3", "s2", "prompt3", None);

        let active_s1 = tracker.list_active(Some("s1"));
        assert_eq!(active_s1.len(), 2);

        let active_all = tracker.list_active(None);
        assert_eq!(active_all.len(), 3);
    }

    #[test]
    fn mark_completed_and_failed() {
        let tracker = BackgroundAgentTracker::new();
        tracker.register("a1", "s1", "p", None);

        let result = AgentResult::completed("done", "p");
        tracker.mark_completed("a1", &result);
        assert!(tracker.get_active_agent_ids("s1").is_empty());

        tracker.register("a2", "s1", "p", None);
        tracker.mark_failed("a2", "boom");
        assert!(tracker.get_active_agent_ids("s1").is_empty());
    }

    #[tokio::test]
    async fn await_all_returns_true_when_empty() {
        let tracker = BackgroundAgentTracker::new();
        assert!(tracker.await_all("s1", Duration::from_millis(100)).await);
    }

    #[test]
    fn cleanup_stale_removes_old() {
        let tracker = BackgroundAgentTracker::new();
        tracker.register("a1", "s1", "p", None);
        let result = AgentResult::completed("done", "p");
        tracker.mark_completed("a1", &result);

        // 手动设为 31 分钟前完成
        if let Some(mut entry) = tracker.agents.get_mut("a1") {
            entry.completed_at = Some(now_millis() - 31 * 60 * 1000);
        }
        drop(tracker.agents.get_mut("a1"));

        tracker.cleanup_stale();
        assert!(tracker.get_status("a1").is_none());
    }

    #[test]
    fn remove_session_clears_agents() {
        let tracker = BackgroundAgentTracker::new();
        tracker.register("a1", "s1", "p", None);
        tracker.register("a2", "s2", "p", None);
        tracker.remove_session("s1");
        assert!(tracker.list_active(Some("s1")).is_empty());
        assert_eq!(tracker.list_active(Some("s2")).len(), 1);
    }

    #[test]
    fn get_status_returns_state() {
        let tracker = BackgroundAgentTracker::new();
        tracker.register("a1", "s1", "test prompt", Some("/tmp/out.txt".to_owned()));
        let status = tracker.get_status("a1").expect("exists");
        assert_eq!(status.agent_id, "a1");
        assert_eq!(status.prompt, "test prompt");
        assert_eq!(status.output_file, Some("/tmp/out.txt".into()));
        assert!(status.is_running());
        assert_eq!(status.status, "running");
        assert!(status.completed_at.is_none());
        assert!(status.error.is_none());
    }
}
