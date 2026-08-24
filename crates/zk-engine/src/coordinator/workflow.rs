//! 四阶段工作流状态机——对照旧 `CoordinatorWorkflow.java`（247L）。
//!
//! 管理工作流的完整生命周期：
//! Research → Synthesis → Implementation → Verification（严格顺序）。
//! 每次阶段转换记录时间戳和结果摘要。阶段转换不可跳过，必须按顺序推进。
//!
//! # 有意差异
//!
//! - Java 使用 `AtomicReference` + `synchronizedList`，本实现取 `Mutex` 统一
//!   保护（Rust 标准库 `Mutex` 已含 poison recovery，语义等价）。
//! - `WorkflowPhase` 在旧实现是 `sealed interface` + 4 个 record 子类，本实现
//!   取 enum + 变体关联数据，阶段特有字段暂只保留核心（objective / summary /
//!   spec / checks），后续按需扩展。

use std::sync::{Mutex, PoisonError};
use std::time::Instant;

/// 工作流阶段——四阶段严格顺序推进。
///
/// 对照旧 `WorkflowPhase.java` 的 sealed interface + 4 record 子类。
#[derive(Clone, Debug)]
pub enum WorkflowPhase {
    /// 研究阶段——调查代码库、理解问题。
    Research {
        /// 研究目标描述。
        objective: String,
    },
    /// 综合阶段——阅读发现、制定实现规格。
    Synthesis {
        /// 综合摘要。
        summary: String,
    },
    /// 实现阶段——按规格进行针对性变更。
    Implementation {
        /// 实现规格描述。
        spec: String,
    },
    /// 验证阶段——测试变更是否有效。
    Verification {
        /// 校验项列表。
        checks: Vec<String>,
    },
}

impl WorkflowPhase {
    /// 阶段名称（对齐旧 `WorkflowPhase.name()`）。
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Research { .. } => "Research",
            Self::Synthesis { .. } => "Synthesis",
            Self::Implementation { .. } => "Implementation",
            Self::Verification { .. } => "Verification",
        }
    }

    /// 阶段索引（0-3，对齐旧 `phaseIndex()`）。
    #[must_use]
    pub fn phase_index(&self) -> usize {
        match self {
            Self::Research { .. } => 0,
            Self::Synthesis { .. } => 1,
            Self::Implementation { .. } => 2,
            Self::Verification { .. } => 3,
        }
    }

    /// 阶段总数（对齐旧 `WorkflowPhase.TOTAL_PHASES = 4`）。
    pub const TOTAL_PHASES: usize = 4;

    /// 阶段提示文本（对齐旧 `phasePrompt()`）。
    #[must_use]
    pub fn phase_prompt(&self) -> String {
        match self {
            Self::Research { objective } => {
                format!(
                    "Research phase: investigate and understand the problem. Objective: {objective}"
                )
            }
            Self::Synthesis { summary } => {
                if summary.is_empty() {
                    "Synthesis phase: analyze findings and create implementation plan.".to_owned()
                } else {
                    format!("Synthesis phase: {summary}")
                }
            }
            Self::Implementation { spec } => {
                if spec.is_empty() {
                    "Implementation phase: make targeted code changes.".to_owned()
                } else {
                    format!("Implementation phase: {spec}")
                }
            }
            Self::Verification { checks } => {
                if checks.is_empty() {
                    "Verification phase: prove the code works.".to_owned()
                } else {
                    format!("Verification phase: verify [{}]", checks.join(", "))
                }
            }
        }
    }
}

/// 工作流状态枚举（对齐旧 `CoordinatorWorkflow.WorkflowStatus`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowStatus {
    /// 尚未启动。
    NotStarted,
    /// 正在运行。
    Running,
    /// 已完成所有阶段。
    Completed,
    /// 执行失败。
    Failed,
    /// 已取消。
    Cancelled,
}

impl WorkflowStatus {
    /// 状态名（对齐旧 `WorkflowStatus.name()`）。
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotStarted => "NOT_STARTED",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    /// 是否处于终态（完成 / 失败 / 取消）。
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// 阶段历史记录（对齐旧 `CoordinatorWorkflow.PhaseRecord`）。
#[derive(Clone, Debug)]
pub struct PhaseRecord {
    /// 阶段。
    pub phase: WorkflowPhase,
    /// 阶段开始时刻。
    pub started_at: Instant,
    /// 阶段完成时刻（`None` = 尚未完成）。
    pub completed_at: Option<Instant>,
    /// 结果摘要（`None` = 尚未完成）。
    pub result: Option<String>,
}

/// 四阶段工作流引擎核心类。
///
/// 管理 Research → Synthesis → Implementation → Verification 的完整生命周期。
/// 阶段转换严格顺序，每次转换记录时间戳与结果摘要。
pub struct CoordinatorWorkflow {
    /// 工作流唯一 ID。
    workflow_id: String,
    /// 工作流目标描述。
    objective: String,
    /// 当前阶段（`None` = 未启动或已完成）。
    current_phase: Mutex<Option<WorkflowPhase>>,
    /// 阶段历史记录。
    phase_history: Mutex<Vec<PhaseRecord>>,
    /// 工作流状态。
    status: Mutex<WorkflowStatus>,
    /// 工作流启动时刻。
    start_time: Instant,
    /// 工作流结束时刻。
    end_time: Mutex<Option<Instant>>,
}

impl CoordinatorWorkflow {
    /// 创建工作流实例（尚未启动）。
    ///
    /// 对齐旧 `new CoordinatorWorkflow(workflowId, objective)`。
    #[must_use]
    pub fn new(workflow_id: String, objective: String) -> Self {
        Self {
            workflow_id,
            objective,
            current_phase: Mutex::new(None),
            phase_history: Mutex::new(Vec::new()),
            status: Mutex::new(WorkflowStatus::NotStarted),
            start_time: Instant::now(),
            end_time: Mutex::new(None),
        }
    }

    // ═══ 核心方法 ═══

    /// 启动工作流——初始化并进入 Research 阶段。
    ///
    /// 对齐旧 `startWorkflow()`。
    ///
    /// # Errors
    ///
    /// 工作流已启动时返回 `Err`。
    pub fn start_workflow(&self) -> Result<WorkflowPhase, String> {
        let mut status = self.status.lock().unwrap_or_else(PoisonError::into_inner);
        if *status != WorkflowStatus::NotStarted {
            return Err(format!("Workflow already started: {}", self.workflow_id));
        }

        let research = WorkflowPhase::Research {
            objective: self.objective.clone(),
        };

        *self
            .current_phase
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(research.clone());
        *status = WorkflowStatus::Running;

        self.phase_history
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(PhaseRecord {
                phase: research.clone(),
                started_at: Instant::now(),
                completed_at: None,
                result: None,
            });

        tracing::info!(
            workflow_id = %self.workflow_id,
            objective = %truncate(&self.objective, 80),
            "Workflow started — entering Research phase"
        );

        Ok(research)
    }

    /// 推进到下一阶段。
    ///
    /// 阶段转换严格顺序：Research → Synthesis → Implementation → Verification。
    /// 完成 Verification 后工作流标记为 `COMPLETED`。
    ///
    /// 对齐旧 `advancePhase(resultSummary)`。
    ///
    /// # Errors
    ///
    /// 工作流未运行或无当前阶段时返回 `Err`。
    pub fn advance_phase(&self, result_summary: &str) -> Result<Option<WorkflowPhase>, String> {
        let mut status = self.status.lock().unwrap_or_else(PoisonError::into_inner);
        if *status != WorkflowStatus::Running {
            return Err(format!(
                "Workflow not running: {} (status={})",
                self.workflow_id,
                status.as_str()
            ));
        }

        let current = {
            let current_phase = self
                .current_phase
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            current_phase
                .clone()
                .ok_or_else(|| format!("No current phase in workflow: {}", self.workflow_id))?
        };

        // 记录当前阶段完成
        self.close_phase_summary(&current, result_summary);

        // 推进到下一阶段
        let next = Self::resolve_next_phase(&current);
        match next {
            None => {
                // Verification 完成 → 工作流结束
                *status = WorkflowStatus::Completed;
                *self.end_time.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(Instant::now());
                *self
                    .current_phase
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = None;

                let duration = self.start_time.elapsed().as_secs();
                tracing::info!(
                    workflow_id = %self.workflow_id,
                    duration_secs = duration,
                    "Workflow completed"
                );
                Ok(None)
            }
            Some(next_phase) => {
                *self
                    .current_phase
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(next_phase.clone());
                self.phase_history
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(PhaseRecord {
                        phase: next_phase.clone(),
                        started_at: Instant::now(),
                        completed_at: None,
                        result: None,
                    });

                tracing::info!(
                    workflow_id = %self.workflow_id,
                    from = current.name(),
                    to = next_phase.name(),
                    summary = %truncate(result_summary, 100),
                    "Workflow advanced"
                );
                Ok(Some(next_phase))
            }
        }
    }

    /// 获取当前阶段（`None` = 未启动或已完成）。
    #[must_use]
    pub fn get_current_phase(&self) -> Option<WorkflowPhase> {
        self.current_phase
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// 获取阶段历史记录（不可变副本）。
    #[must_use]
    pub fn get_phase_history(&self) -> Vec<PhaseRecord> {
        self.phase_history
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// 工作流是否已完成所有阶段。
    #[must_use]
    pub fn is_complete(&self) -> bool {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) == WorkflowStatus::Completed
    }

    /// 工作流是否处于终态（完成 / 失败 / 取消）。
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_terminal()
    }

    /// 标记工作流失败。
    pub fn mark_failed(&self, reason: &str) {
        let current = self
            .current_phase
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(ref phase) = current {
            self.close_phase_summary(phase, &format!("FAILED: {reason}"));
        }
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = WorkflowStatus::Failed;
        *self.end_time.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());

        tracing::error!(
            workflow_id = %self.workflow_id,
            phase = current.as_ref().map_or("N/A", |p| p.name()),
            reason = reason,
            "Workflow failed"
        );
    }

    /// 取消工作流。
    pub fn cancel(&self) {
        let current = self
            .current_phase
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(ref phase) = current {
            self.close_phase_summary(phase, "CANCELLED");
        }
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = WorkflowStatus::Cancelled;
        *self.end_time.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());

        tracing::info!(
            workflow_id = %self.workflow_id,
            phase = current.as_ref().map_or("N/A", |p| p.name()),
            "Workflow cancelled"
        );
    }

    // ═══ Getters ═══

    /// 工作流 ID。
    #[must_use]
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// 工作流目标。
    #[must_use]
    pub fn objective(&self) -> &str {
        &self.objective
    }

    /// 工作流状态。
    #[must_use]
    pub fn status(&self) -> WorkflowStatus {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 工作流启动时刻。
    #[must_use]
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// 工作流结束时刻。
    #[must_use]
    pub fn end_time(&self) -> Option<Instant> {
        *self.end_time.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 获取当前阶段索引 (0-3)，未启动或已完成返回 -1。
    #[must_use]
    pub fn current_phase_index(&self) -> i32 {
        self.current_phase
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map_or(-1, |p| i32::try_from(p.phase_index()).unwrap_or(-1))
    }

    // ═══ 内部方法 ═══

    /// 根据当前阶段解析下一阶段。
    /// 严格顺序：Research → Synthesis → Implementation → Verification → None。
    fn resolve_next_phase(current: &WorkflowPhase) -> Option<WorkflowPhase> {
        match current {
            WorkflowPhase::Research { .. } => Some(WorkflowPhase::Synthesis {
                summary: String::new(),
            }),
            WorkflowPhase::Synthesis { .. } => Some(WorkflowPhase::Implementation {
                spec: String::new(),
            }),
            WorkflowPhase::Implementation { .. } => {
                Some(WorkflowPhase::Verification { checks: Vec::new() })
            }
            WorkflowPhase::Verification { .. } => None,
        }
    }

    /// 关闭阶段历史记录（填充 `completed_at` 和 `result`）。
    fn close_phase_summary(&self, phase: &WorkflowPhase, summary: &str) {
        let mut history = self
            .phase_history
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for record in history.iter_mut().rev() {
            if record.phase.name() == phase.name() && record.completed_at.is_none() {
                record.completed_at = Some(Instant::now());
                record.result = Some(summary.to_owned());
                break;
            }
        }
    }
}

/// 截断文本（对齐旧 `truncate` 辅助方法）。
fn truncate(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_owned()
    } else {
        format!("{}...", &text[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_lifecycle_happy_path() {
        let wf = CoordinatorWorkflow::new("wf-001".into(), "Test objective".into());
        assert_eq!(wf.status(), WorkflowStatus::NotStarted);

        let phase = wf.start_workflow().expect("start ok");
        assert_eq!(phase.name(), "Research");
        assert_eq!(wf.status(), WorkflowStatus::Running);
        assert_eq!(wf.current_phase_index(), 0);

        let next = wf
            .advance_phase("found relevant files")
            .expect("advance ok");
        let next = next.expect("not completed yet");
        assert_eq!(next.name(), "Synthesis");
        assert_eq!(wf.current_phase_index(), 1);

        let next = wf.advance_phase("plan created").expect("advance ok");
        let next = next.expect("not completed yet");
        assert_eq!(next.name(), "Implementation");
        assert_eq!(wf.current_phase_index(), 2);

        let next = wf.advance_phase("code changes done").expect("advance ok");
        let next = next.expect("not completed yet");
        assert_eq!(next.name(), "Verification");
        assert_eq!(wf.current_phase_index(), 3);

        let next = wf.advance_phase("all tests pass").expect("advance ok");
        assert!(next.is_none(), "workflow completed");
        assert_eq!(wf.status(), WorkflowStatus::Completed);
        assert!(wf.is_complete());
        assert!(wf.is_terminal());
        assert_eq!(wf.current_phase_index(), -1);

        let history = wf.get_phase_history();
        assert_eq!(history.len(), 4);
        for record in &history {
            assert!(record.completed_at.is_some());
            assert!(record.result.is_some());
        }
    }

    #[test]
    fn workflow_start_twice_errors() {
        let wf = CoordinatorWorkflow::new("wf-002".into(), "obj".into());
        wf.start_workflow().expect("first start ok");
        assert!(wf.start_workflow().is_err());
    }

    #[test]
    fn workflow_cancel() {
        let wf = CoordinatorWorkflow::new("wf-003".into(), "obj".into());
        wf.start_workflow().expect("start ok");
        wf.cancel();
        assert_eq!(wf.status(), WorkflowStatus::Cancelled);
        assert!(wf.is_terminal());
    }

    #[test]
    fn workflow_mark_failed() {
        let wf = CoordinatorWorkflow::new("wf-004".into(), "obj".into());
        wf.start_workflow().expect("start ok");
        wf.mark_failed("unexpected error");
        assert_eq!(wf.status(), WorkflowStatus::Failed);
        assert!(wf.is_terminal());
    }

    #[test]
    fn phase_properties() {
        assert_eq!(WorkflowPhase::TOTAL_PHASES, 4);

        let r = WorkflowPhase::Research {
            objective: "test".into(),
        };
        assert_eq!(r.name(), "Research");
        assert_eq!(r.phase_index(), 0);

        let v = WorkflowPhase::Verification {
            checks: vec!["test1".into()],
        };
        assert_eq!(v.name(), "Verification");
        assert_eq!(v.phase_index(), 3);
    }
}
