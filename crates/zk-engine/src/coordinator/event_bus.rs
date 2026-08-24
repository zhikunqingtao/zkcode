//! Coordinator 事件总线——多 Agent 协作过程事件的进程内广播
//! （对照旧 `CoordinatorEventBus.java`，151L）。
//!
//! # 与 Java 基线的映射
//!
//! Java 侧 `CoordinatorEventBus` 经 `SimpMessagingTemplate` 把协作事件
//! 单播到 STOMP topic `/user/queue/coordinator/{sessionId}`，与主消息
//! 信道解耦、推送失败仅 `log.warn` 兜底。zkcode 无 STOMP，采用
//! `tokio::sync::broadcast` 进程内多订阅者广播：`zk-server` 侧的 WS 网关
//! 订阅后转换为下行 [`zk_protocol::ServerMessage`] 推送前端（`agent_started`
//! / `agent_completed` / `agent_failed` / `task_update`）。
//!
//! # 有意差异
//!
//! - Java 单播到指定 principal；本实现为进程内广播（多订阅者），路由到
//!   具体会话由订阅侧（`zk-server`）依 `session_id` 负责。
//! - Java 的 `phase_transition` / `mailbox_write` / `mailbox_broadcast`
//!   事件类型属工作流内部时间线；本批仅承载与前端 `ServerMessage` 一一
//!   对应的四类核心事件（agent 生命周期 + 任务状态），其余留待按需扩展。
//! - 发布采用 best-effort 语义：无订阅者时 `send` 返回 `Err` 被静默吞没
//!   （对照 Java `safeSend` 吞异常、业务线程零感知）。

use std::collections::BTreeMap;

use tokio::sync::broadcast;
use zk_protocol::{ServerMessage, WorkerSnapshot};

/// broadcast channel 容量（滞后的订阅者丢弃最旧事件，不阻塞发布者）。
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Coordinator 协作事件——复用 [`zk_protocol`] 已建模的四类前端可见事件语义。
///
/// 每个变体携带 `session_id` 供订阅侧路由到对应会话的下行信道
/// （Java 侧该路由由 `convertAndSendToUser` + principal 承担）。
#[derive(Clone, Debug, PartialEq)]
pub enum CoordinatorEvent {
    /// 子代理启动（对照 `agent_started`）。
    AgentStarted {
        /// 归属会话 ID（订阅侧路由键）。
        session_id: String,
        /// 代理 ID。
        agent_id: String,
        /// 任务提示。
        prompt: String,
    },
    /// 子代理完成（对照 `agent_completed`）。
    AgentCompleted {
        /// 归属会话 ID。
        session_id: String,
        /// 代理 ID。
        agent_id: String,
        /// 结果文本。
        result: String,
    },
    /// 子代理失败（对照 `agent_failed`）。
    AgentFailed {
        /// 归属会话 ID。
        session_id: String,
        /// 代理 ID。
        agent_id: String,
        /// 错误信息。
        error: String,
    },
    /// 任务状态变更（对照 `task_update`）。
    TaskUpdate {
        /// 归属会话 ID。
        session_id: String,
        /// 任务 ID。
        task_id: String,
        /// 状态。
        status: String,
    },
    /// Complete Swarm state projection.
    SwarmStateUpdate {
        /// Owning session.
        session_id: String,
        /// Swarm identifier.
        swarm_id: String,
        /// Lifecycle phase.
        phase: String,
        /// Active workers.
        active_workers: i64,
        /// Total workers.
        total_workers: i64,
        /// Completed tasks.
        completed_tasks: i64,
        /// Total tasks.
        total_tasks: i64,
        /// Worker snapshots keyed by worker ID.
        workers: BTreeMap<String, WorkerSnapshot>,
    },
    /// One worker's live progress.
    WorkerProgress {
        /// Owning session.
        session_id: String,
        /// Swarm identifier.
        swarm_id: String,
        /// Worker identifier.
        worker_id: String,
        /// Stable worker status.
        status: String,
        /// Current task description.
        current_task: Option<String>,
        /// Progress percentage.
        progress_percent: Option<i64>,
        /// Terminal error.
        error_message: Option<String>,
        /// Terminal reason.
        termination_reason: Option<String>,
    },
    /// Coordinator workflow phase update.
    WorkflowPhaseUpdate {
        /// Owning session.
        session_id: String,
        /// Workflow identifier.
        workflow_id: String,
        /// Phase name.
        phase_name: String,
        /// Phase status.
        status: String,
        /// Zero-based phase index.
        phase_index: i64,
        /// Phase count.
        total_phases: i64,
        /// Phase prompt.
        phase_prompt: String,
        /// Workflow objective.
        objective: String,
    },
    /// Durable collaboration message projected to the live DAG.
    TeammateMessage {
        /// Owning session.
        session_id: String,
        /// Sender identity.
        from_id: String,
        /// Redacted content.
        content: String,
    },
}

impl CoordinatorEvent {
    /// 事件归属会话 ID（订阅侧据此路由下行信道）。
    #[must_use]
    pub fn session_id(&self) -> &str {
        match self {
            Self::AgentStarted { session_id, .. }
            | Self::AgentCompleted { session_id, .. }
            | Self::AgentFailed { session_id, .. }
            | Self::TaskUpdate { session_id, .. }
            | Self::SwarmStateUpdate { session_id, .. }
            | Self::WorkerProgress { session_id, .. }
            | Self::WorkflowPhaseUpdate { session_id, .. }
            | Self::TeammateMessage { session_id, .. } => session_id,
        }
    }

    /// 转换为下行 [`ServerMessage`]（丢弃 `session_id`——它是路由键而非
    /// 载荷字段，与 Java envelope 把 `sessionId` 放在顶层而非 payload 一致）。
    #[must_use]
    pub fn to_server_message(&self) -> ServerMessage {
        match self.clone() {
            Self::AgentStarted {
                agent_id, prompt, ..
            } => ServerMessage::AgentStarted { agent_id, prompt },
            Self::AgentCompleted {
                agent_id, result, ..
            } => ServerMessage::AgentCompleted { agent_id, result },
            Self::AgentFailed {
                agent_id, error, ..
            } => ServerMessage::AgentFailed { agent_id, error },
            Self::TaskUpdate {
                task_id, status, ..
            } => ServerMessage::TaskUpdate {
                task_id,
                status,
                progress: None,
                output: None,
            },
            Self::SwarmStateUpdate {
                swarm_id,
                phase,
                active_workers,
                total_workers,
                completed_tasks,
                total_tasks,
                workers,
                ..
            } => ServerMessage::SwarmStateUpdate {
                swarm_id,
                phase,
                active_workers,
                total_workers,
                completed_tasks,
                total_tasks,
                workers,
            },
            Self::WorkerProgress {
                swarm_id,
                worker_id,
                status,
                current_task,
                progress_percent,
                error_message,
                termination_reason,
                ..
            } => ServerMessage::WorkerProgress {
                swarm_id,
                worker_id,
                status,
                current_task,
                tool_call_count: 0,
                token_consumed: 0,
                recent_tool_calls: None,
                progress_percent,
                total_steps: None,
                completed_steps: None,
                error_message,
                current_step_description: None,
                termination_reason,
            },
            Self::WorkflowPhaseUpdate {
                workflow_id,
                phase_name,
                status,
                phase_index,
                total_phases,
                phase_prompt,
                objective,
                ..
            } => ServerMessage::WorkflowPhaseUpdate {
                workflow_id,
                phase_name,
                status,
                phase_index,
                total_phases,
                phase_prompt,
                objective,
            },
            Self::TeammateMessage {
                from_id, content, ..
            } => ServerMessage::TeammateMessage { from_id, content },
        }
    }
}

/// Coordinator 事件总线——`tokio::sync::broadcast` 进程内多订阅广播。
///
/// 发布 best-effort：无订阅者时静默丢弃（不阻塞、不报错），对照 Java
/// `safeSend` 的「业务线程零感知」语义。可 `Clone`（内部共享同一 sender）。
#[derive(Clone)]
pub struct CoordinatorEventBus {
    tx: broadcast::Sender<CoordinatorEvent>,
}

impl CoordinatorEventBus {
    /// 创建事件总线（缺省容量 [`EVENT_CHANNEL_CAPACITY`]）。
    #[must_use]
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self { tx }
    }

    /// 订阅事件流——返回一个 [`broadcast::Receiver`]，可持续 `recv().await`。
    ///
    /// 滞后订阅者（缓冲溢出）会收到 `RecvError::Lagged`，跳过最旧事件后
    /// 继续接收（broadcast 语义），不影响其他订阅者与发布者。
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CoordinatorEvent> {
        self.tx.subscribe()
    }

    /// 发布一个事件（best-effort）。
    ///
    /// 返回当前活跃订阅者数量；无订阅者（`send` 返回 `Err`）时返回 0 并
    /// 静默丢弃事件——发布侧业务线程零感知。
    #[must_use]
    pub fn publish(&self, event: CoordinatorEvent) -> usize {
        self.tx.send(event).unwrap_or(0)
    }
}

impl Default for CoordinatorEventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_without_subscribers_is_silent() {
        let bus = CoordinatorEventBus::new();
        let delivered = bus.publish(CoordinatorEvent::TaskUpdate {
            session_id: "s1".to_owned(),
            task_id: "t1".to_owned(),
            status: "in_progress".to_owned(),
        });
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn subscriber_receives_published_event() {
        let bus = CoordinatorEventBus::new();
        let mut rx = bus.subscribe();
        let delivered = bus.publish(CoordinatorEvent::AgentStarted {
            session_id: "s1".to_owned(),
            agent_id: "a1".to_owned(),
            prompt: "do work".to_owned(),
        });
        assert_eq!(delivered, 1);
        let event = rx.recv().await.expect("event received");
        assert_eq!(event.session_id(), "s1");
        match event {
            CoordinatorEvent::AgentStarted {
                agent_id, prompt, ..
            } => {
                assert_eq!(agent_id, "a1");
                assert_eq!(prompt, "do work");
            }
            other => panic!("expected AgentStarted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let bus = CoordinatorEventBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        let delivered = bus.publish(CoordinatorEvent::AgentCompleted {
            session_id: "s".to_owned(),
            agent_id: "a".to_owned(),
            result: "ok".to_owned(),
        });
        assert_eq!(delivered, 2);
        assert!(rx1.recv().await.is_ok());
        assert!(rx2.recv().await.is_ok());
    }

    #[test]
    fn to_server_message_maps_variants() {
        let started = CoordinatorEvent::AgentStarted {
            session_id: "s".to_owned(),
            agent_id: "a".to_owned(),
            prompt: "p".to_owned(),
        };
        assert!(matches!(
            started.to_server_message(),
            ServerMessage::AgentStarted { .. }
        ));

        let failed = CoordinatorEvent::AgentFailed {
            session_id: "s".to_owned(),
            agent_id: "a".to_owned(),
            error: "boom".to_owned(),
        };
        match failed.to_server_message() {
            ServerMessage::AgentFailed { agent_id, error } => {
                assert_eq!(agent_id, "a");
                assert_eq!(error, "boom");
            }
            other => panic!("expected AgentFailed, got {other:?}"),
        }

        let swarm = CoordinatorEvent::SwarmStateUpdate {
            session_id: "s".into(),
            swarm_id: "swarm".into(),
            phase: "RUNNING".into(),
            active_workers: 1,
            total_workers: 2,
            completed_tasks: 0,
            total_tasks: 2,
            workers: BTreeMap::new(),
        };
        assert!(matches!(
            swarm.to_server_message(),
            ServerMessage::SwarmStateUpdate { phase, .. } if phase == "RUNNING"
        ));

        let progress = CoordinatorEvent::WorkerProgress {
            session_id: "s".into(),
            swarm_id: "swarm".into(),
            worker_id: "worker".into(),
            status: "TERMINATED".into(),
            current_task: None,
            progress_percent: Some(100),
            error_message: None,
            termination_reason: Some("completed".into()),
        };
        assert!(matches!(
            progress.to_server_message(),
            ServerMessage::WorkerProgress {
                progress_percent: Some(100),
                ..
            }
        ));
    }
}
