//! Coordinator 四阶段工作流编排引擎——对照旧 `CoordinatorWorkflowEngine.java`（447L）。
//!
//! 职责：
//! - 管理工作流实例生命周期
//! - 阶段自动检测：根据 LLM 输出内容判断当前应处于哪个阶段
//! - "不委派理解"原则验证：检测是否存在未经充分研究就直接实现的情况
//! - 假阳性处理：误判时记录 WARN 日志但不阻塞流程
//! - 通过 `MessageSink` 推送阶段变更到前端
//!
//! # 有意差异
//!
//! - Java 注入 `WebSocketController` + `CoordinatorEventBus` 推送；本实现
//!   统一走 `MessageSink` 窄接口（与引擎其他模块一致）。
//! - Java 的 `SwarmService` 依赖暂不引入（工作流编排与 Swarm 执行解耦）。

use std::sync::Arc;

use dashmap::DashMap;
use regex::Regex;
use std::sync::OnceLock;

use super::workflow::{CoordinatorWorkflow, WorkflowPhase};

/// 模糊指令正则——检测委派理解的反模式（对齐旧 `VAGUE_PATTERNS`）。
static VAGUE_PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();

fn vague_patterns() -> &'static Vec<Regex> {
    VAGUE_PATTERNS.get_or_init(|| {
        [
            r"(?i)based on (your|the) (findings|research)",
            r"(?i)fix the (bug|issue|problem)",
            r"(?i)using what you (learned|found)",
            r"(?i)implement the (solution|fix)",
        ]
        .iter()
        .filter_map(|p| Regex::new(p).ok())
        .collect()
    })
}

/// Prompt 最短有效长度——低于此值警告"委派理解"（ASCII/英文）。
const MIN_PROMPT_LENGTH: usize = 100;
/// Prompt 最短有效长度（CJK 场景）。
const MIN_PROMPT_LENGTH_CJK: usize = 50;

/// 阶段检测关键词集（对齐旧 4 个 `Set<String>`）。
const RESEARCH_KEYWORDS: &[&str] = &[
    "explore",
    "investigate",
    "search",
    "find",
    "look for",
    "research",
    "调研",
    "搜索",
    "查找",
    "探索",
];
const SYNTHESIS_KEYWORDS: &[&str] = &[
    "synthesize",
    "summarize",
    "plan",
    "design",
    "craft",
    "综合",
    "总结",
    "规划",
    "设计",
];
const IMPLEMENTATION_KEYWORDS: &[&str] = &[
    "implement",
    "execute",
    "apply",
    "modify",
    "write",
    "create",
    "实现",
    "执行",
    "修改",
    "编写",
    "创建",
];
const VERIFICATION_KEYWORDS: &[&str] = &[
    "verify", "test", "validate", "check", "lint", "build", "验证", "测试", "校验", "检查",
];

/// 验证严重度（对齐旧 `ValidationSeverity`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationSeverity {
    /// 通过。
    Ok,
    /// 警告。
    Warn,
    /// 错误。
    Error,
}

/// 验证结果（对齐旧 `ValidationResult` record）。
#[derive(Clone, Debug)]
pub struct ValidationResult {
    /// 是否通过验证。
    pub valid: bool,
    /// 警告列表。
    pub warnings: Vec<String>,
    /// 严重度。
    pub severity: ValidationSeverity,
}

/// Coordinator 四阶段工作流编排引擎。
///
/// 管理 `sessionId` → `CoordinatorWorkflow` 映射。
pub struct CoordinatorWorkflowEngine {
    /// 活跃工作流（`sessionId` → `CoordinatorWorkflow`）。
    active_workflows: DashMap<String, Arc<CoordinatorWorkflow>>,
}

impl CoordinatorWorkflowEngine {
    /// 创建引擎实例。
    #[must_use]
    pub fn new() -> Self {
        Self {
            active_workflows: DashMap::new(),
        }
    }

    // ═══ 1. 工作流管理 ═══

    /// 创建并启动完整工作流。
    ///
    /// 对齐旧 `executeWorkflow(sessionId, objective)`。
    pub fn execute_workflow(&self, session_id: &str, objective: &str) -> Arc<CoordinatorWorkflow> {
        let workflow_id = format!("wf-{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let workflow = Arc::new(CoordinatorWorkflow::new(
            workflow_id.clone(),
            objective.to_owned(),
        ));

        // 进入 Research 阶段
        let _research = workflow.start_workflow();
        self.active_workflows
            .insert(session_id.to_owned(), Arc::clone(&workflow));

        tracing::info!(
            workflow_id = %workflow_id,
            session_id = session_id,
            objective = %truncate(objective, 80),
            "Workflow engine started"
        );

        workflow
    }

    /// 推进工作流到下一阶段。
    ///
    /// 对齐旧 `advanceWorkflow(sessionId, resultSummary)`。
    pub fn advance_workflow(
        &self,
        session_id: &str,
        result_summary: &str,
    ) -> Option<WorkflowPhase> {
        let Some(workflow) = self
            .active_workflows
            .get(session_id)
            .map(|e| Arc::clone(e.value()))
        else {
            tracing::warn!(session_id = session_id, "No active workflow for session");
            return None;
        };

        let previous = workflow.get_current_phase();
        match workflow.advance_phase(result_summary) {
            Ok(Some(next)) => {
                tracing::info!(
                    from = previous.as_ref().map_or("N/A", |p| p.name()),
                    to = next.name(),
                    session = session_id,
                    "Workflow advanced"
                );
                Some(next)
            }
            Ok(None) => {
                // 工作流完成
                tracing::info!(session = session_id, "Workflow completed");
                self.active_workflows.remove(session_id);
                None
            }
            Err(e) => {
                tracing::warn!(error = %e, session = session_id, "Failed to advance workflow");
                None
            }
        }
    }

    /// 获取当前会话的活跃工作流。
    #[must_use]
    pub fn get_active_workflow(&self, session_id: &str) -> Option<Arc<CoordinatorWorkflow>> {
        self.active_workflows
            .get(session_id)
            .map(|e| Arc::clone(e.value()))
    }

    /// 取消工作流。
    pub fn cancel_workflow(&self, session_id: &str) {
        if let Some((_, workflow)) = self.active_workflows.remove(session_id) {
            workflow.cancel();
            tracing::info!(
                workflow_id = workflow.workflow_id(),
                session = session_id,
                "Workflow cancelled"
            );
        }
    }

    /// Mark an active workflow failed and remove it from the active scheduler.
    pub fn fail_workflow(&self, session_id: &str, reason: &str) {
        if let Some((_, workflow)) = self.active_workflows.remove(session_id) {
            workflow.mark_failed(reason);
            tracing::warn!(
                workflow_id = workflow.workflow_id(),
                session = session_id,
                reason,
                "Workflow failed"
            );
        }
    }

    // ═══ 2. 阶段自动检测 ═══

    /// 根据 LLM 输出内容自动检测当前应处于哪个阶段。
    ///
    /// 基于关键词启发式判断。重要：这是推断而非硬编码，允许假阳性（记录日志但不阻塞）。
    /// 对齐旧 `detectPhase(llmOutput)`。
    #[must_use]
    pub fn detect_phase(&self, llm_output: &str) -> WorkflowPhase {
        if llm_output.is_empty() {
            return WorkflowPhase::Research {
                objective: String::new(),
            };
        }

        let lower = llm_output.to_lowercase();

        // 反向优先级检测（高阶段优先）
        let verify_score = count_keyword_matches(&lower, VERIFICATION_KEYWORDS);
        let impl_score = count_keyword_matches(&lower, IMPLEMENTATION_KEYWORDS);
        let synth_score = count_keyword_matches(&lower, SYNTHESIS_KEYWORDS);
        let research_score = count_keyword_matches(&lower, RESEARCH_KEYWORDS);

        // 工具调用模式检测
        let has_file_edit = lower.contains("fileedit") || lower.contains("filewrite");
        let has_test_or_build = lower.contains("bash")
            && (lower.contains("test") || lower.contains("build") || lower.contains("lint"));
        let has_synthetic_output = lower.contains("syntheticoutput");

        // 调整分数
        let impl_score = if has_file_edit {
            impl_score + 3
        } else {
            impl_score
        };
        let verify_score = if has_test_or_build {
            verify_score + 3
        } else {
            verify_score
        };
        let synth_score = if has_synthetic_output {
            synth_score + 2
        } else {
            synth_score
        };

        let max_score = research_score
            .max(synth_score)
            .max(impl_score)
            .max(verify_score);

        if max_score == 0 {
            return WorkflowPhase::Research {
                objective: String::new(),
            };
        }

        if verify_score == max_score {
            WorkflowPhase::Verification { checks: Vec::new() }
        } else if impl_score == max_score {
            WorkflowPhase::Implementation {
                spec: String::new(),
            }
        } else if synth_score == max_score {
            WorkflowPhase::Synthesis {
                summary: String::new(),
            }
        } else {
            WorkflowPhase::Research {
                objective: String::new(),
            }
        }
    }

    /// 检测阶段并与工作流当前阶段对比。
    ///
    /// 如果检测到阶段跳跃（如跳过 Synthesis 直接 Implementation），记录 WARN 日志。
    /// 对齐旧 `detectAndValidatePhase(sessionId, llmOutput)`。
    #[must_use]
    pub fn detect_and_validate_phase(&self, session_id: &str, llm_output: &str) -> WorkflowPhase {
        let detected = self.detect_phase(llm_output);

        let Some(workflow) = self
            .active_workflows
            .get(session_id)
            .map(|e| Arc::clone(e.value()))
        else {
            return detected;
        };

        let Some(current) = workflow.get_current_phase() else {
            return detected;
        };

        // 检测阶段跳跃
        if detected.phase_index() > current.phase_index() + 1 {
            tracing::warn!(
                workflow_id = workflow.workflow_id(),
                current = current.name(),
                current_index = current.phase_index(),
                detected = detected.name(),
                detected_index = detected.phase_index(),
                "Phase skip detected — may be a false positive, not blocking execution"
            );
        }

        detected
    }

    // ═══ 3. "不委派理解"原则验证 ═══

    /// 验证 Agent 派发指令的质量。
    ///
    /// 检查 `AgentTool` 的 prompt 参数是否包含足够具体的信息。
    /// 初期仅作为 WARN 日志，不阻断执行（防止假阳性影响体验）。
    /// 对齐旧 `validateDelegation(phase, agentPrompt)`。
    #[must_use]
    pub fn validate_delegation(
        &self,
        phase: &WorkflowPhase,
        agent_prompt: &str,
    ) -> ValidationResult {
        let mut warnings = Vec::new();

        // 1. 检查 prompt 长度
        let effective_min_length = if contains_cjk(agent_prompt) {
            MIN_PROMPT_LENGTH_CJK
        } else {
            MIN_PROMPT_LENGTH
        };
        if agent_prompt.len() < effective_min_length {
            warnings.push(format!(
                "Prompt too short ({} chars < {} minimum). Likely delegating understanding.",
                agent_prompt.len(),
                effective_min_length
            ));
        }

        // 2. 检查是否包含具体文件路径、行号、变量名等
        let has_specific_info = agent_prompt.contains('/')
            || agent_prompt.contains(':') && agent_prompt.chars().any(|c| c.is_ascii_digit())
            || agent_prompt.contains('.');
        if !has_specific_info && matches!(phase, WorkflowPhase::Implementation { .. }) {
            warnings.push(
                "Prompt lacks specific file paths, line numbers, or method names \
                 (required in Implementation phase)."
                    .to_owned(),
            );
        }

        // 3. 检查是否包含模糊指令
        for pattern in vague_patterns() {
            if pattern.is_match(agent_prompt) {
                warnings.push(format!("Vague delegation detected: '{}'", pattern.as_str()));
            }
        }

        let severity = if warnings.is_empty() {
            ValidationSeverity::Ok
        } else {
            ValidationSeverity::Warn
        };

        if !warnings.is_empty() {
            tracing::warn!(
                phase = phase.name(),
                warnings = ?warnings,
                "Delegation quality warning"
            );
        }

        ValidationResult {
            valid: warnings.is_empty(),
            warnings,
            severity,
        }
    }

    /// 验证并推送委派警告（对齐旧 `validateAndNotify`）。
    #[must_use]
    pub fn validate_and_notify(
        &self,
        _session_id: &str,
        phase: &WorkflowPhase,
        agent_prompt: &str,
    ) -> ValidationResult {
        let result = self.validate_delegation(phase, agent_prompt);
        // 在 Rust 实现中，WS 推送由上层调用方处理
        // （避免引擎层直接依赖 WS 通道）
        if !result.valid && !result.warnings.is_empty() {
            tracing::warn!(
                warnings = ?result.warnings,
                "Delegation quality warnings (push to frontend via caller)"
            );
        }
        result
    }
}

impl Default for CoordinatorWorkflowEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// 关键词匹配计数（对齐旧 `countKeywordMatches`）。
fn count_keyword_matches(text: &str, keywords: &[&str]) -> u32 {
    u32::try_from(keywords.iter().filter(|kw| text.contains(**kw)).count()).unwrap_or(u32::MAX)
}

/// CJK 字符检测——用于动态调整 Prompt 长度阈值。
/// 对齐旧 `containsCjk`：CJK 字符占比超过 30% 即为 CJK 文本。
fn contains_cjk(text: &str) -> bool {
    if text.is_empty() {
        return false;
    }
    let cjk_count = text
        .chars()
        .filter(|c| {
            let cp = *c as u32;
            // CJK Unified Ideographs, Hiragana, Katakana, Hangul
            (0x4E00..=0x9FFF).contains(&cp)
                || (0x3040..=0x309F).contains(&cp)
                || (0x30A0..=0x30FF).contains(&cp)
                || (0xAC00..=0xD7AF).contains(&cp)
        })
        .count();
    cjk_count > text.len() * 3 / 10
}

/// 截断文本。
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
    fn execute_and_advance_workflow() {
        let engine = CoordinatorWorkflowEngine::new();
        let wf = engine.execute_workflow("session-1", "Build a feature");
        assert_eq!(wf.status(), super::super::workflow::WorkflowStatus::Running);
        assert_eq!(wf.get_current_phase().map(|p| p.name()), Some("Research"));

        let next = engine.advance_workflow("session-1", "research done");
        assert_eq!(next.as_ref().map(WorkflowPhase::name), Some("Synthesis"));

        let next = engine.advance_workflow("session-1", "plan ready");
        assert_eq!(
            next.as_ref().map(WorkflowPhase::name),
            Some("Implementation")
        );

        let next = engine.advance_workflow("session-1", "code done");
        assert_eq!(next.as_ref().map(WorkflowPhase::name), Some("Verification"));

        let next = engine.advance_workflow("session-1", "all pass");
        assert!(next.is_none(), "workflow completed");

        // Active workflow should be removed
        assert!(engine.get_active_workflow("session-1").is_none());
    }

    #[test]
    fn detect_phase_keywords() {
        let engine = CoordinatorWorkflowEngine::new();

        let phase = engine.detect_phase("let me investigate and explore the codebase");
        assert_eq!(phase.name(), "Research");

        let phase = engine.detect_phase("I will synthesize the findings and create a plan");
        assert_eq!(phase.name(), "Synthesis");

        let phase = engine.detect_phase("fileedit src/main.rs to implement the fix");
        assert_eq!(phase.name(), "Implementation");

        let phase = engine.detect_phase("bash test and build to verify the changes");
        assert_eq!(phase.name(), "Verification");

        let phase = engine.detect_phase("");
        assert_eq!(phase.name(), "Research");
    }

    #[test]
    fn validate_delegation_good_prompt() {
        let engine = CoordinatorWorkflowEngine::new();
        let phase = WorkflowPhase::Implementation {
            spec: "fix bug".into(),
        };
        let result = engine.validate_delegation(
            &phase,
            "Fix the null pointer in src/auth/validate.ts:42. \
             The user field on Session (src/auth/types.ts:15) is undefined. \
             Add a null check before user.id access and return 401.",
        );
        assert!(result.valid);
        assert_eq!(result.severity, ValidationSeverity::Ok);
    }

    #[test]
    fn validate_delegation_vague_prompt() {
        let engine = CoordinatorWorkflowEngine::new();
        let phase = WorkflowPhase::Implementation {
            spec: "fix bug".into(),
        };
        let result = engine.validate_delegation(&phase, "Based on your findings, fix the bug");
        assert!(!result.valid);
        assert_eq!(result.severity, ValidationSeverity::Warn);
        assert!(!result.warnings.is_empty());
    }

    #[test]
    fn validate_delegation_short_prompt() {
        let engine = CoordinatorWorkflowEngine::new();
        let phase = WorkflowPhase::Research {
            objective: "test".into(),
        };
        let result = engine.validate_delegation(&phase, "fix it");
        assert!(!result.valid);
    }

    #[test]
    fn contains_cjk_detection() {
        assert!(!contains_cjk("hello world"));
        assert!(contains_cjk("这是一个测试用例"));
        assert!(!contains_cjk(""));
    }

    #[test]
    fn cancel_workflow() {
        let engine = CoordinatorWorkflowEngine::new();
        engine.execute_workflow("s1", "test");
        assert!(engine.get_active_workflow("s1").is_some());

        engine.cancel_workflow("s1");
        assert!(engine.get_active_workflow("s1").is_none());
    }
}
