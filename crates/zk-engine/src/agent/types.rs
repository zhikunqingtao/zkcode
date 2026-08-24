//! 子代理数据模型——对照旧 `SubAgentExecutor` 内部类与 `AgentDefinition`。
//!
//! 有意差异：Java 的 `AgentRequest` / `AgentResult` / `AgentDefinition` 为
//! `record`（不可变值对象），本实现取同构 `struct` + `Clone`；`IsolationMode`
//! 独立为本模块枚举而非 `SubAgentExecutor` 内部类（Rust 无内部类型）。
//! 5 种内置代理的系统提示模板移植旧 `SubAgentExecutor` 的 5 个
//! `static final String` 常量，但仅保留核心段（完整 200+ 行验证提示
//! 后续按需补全，当前覆盖关键约束与输出格式）。

use std::collections::HashSet;

// ═══ 隔离模式 ═══

/// 子代理隔离模式（对照旧 `SubAgentExecutor.IsolationMode`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationMode {
    /// 无隔离——与父代理共享工作目录。
    None,
    /// Git Worktree 隔离——独立工作副本。
    Worktree,
    /// 远程隔离（Phase 2+ 建模未激活）。
    Remote,
}

impl IsolationMode {
    /// 从字符串解析（对照旧 `AgentTool.call` 的 `switch` 分支）。
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "worktree" => Self::Worktree,
            "remote" => Self::Remote,
            _ => Self::None,
        }
    }
}

// ═══ 代理状态 ═══

/// 子代理执行结果状态（对照旧 `AgentResult` 的 6 个 `STATUS_*` 常量）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentStatus {
    /// 正常完成（`STATUS_COMPLETED`）。
    Completed,
    /// 超时（`STATUS_TIMEOUT`）。
    Timeout,
    /// 异步已启动（`STATUS_ASYNC_LAUNCHED`）。
    AsyncLaunched,
    /// 执行失败（`STATUS_FAILED`）。
    Failed,
    /// 被中断（`STATUS_INTERRUPTED`）。
    Interrupted,
    /// 达到最大轮次（`STATUS_MAX_TURNS`）。
    MaxTurns,
}

impl AgentStatus {
    /// 状态字符串（对照旧 `AgentResult.status()` 的返回值）。
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Timeout => "timeout",
            Self::AsyncLaunched => "async_launched",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
            Self::MaxTurns => "max_turns",
        }
    }

    /// 从 `stop_reason` 分类最终状态（对照旧 `classifyAgentStatus`）。
    #[must_use]
    pub fn classify(stop_reason: Option<&str>, has_messages: bool, has_error: bool) -> Self {
        if has_error {
            return Self::Failed;
        }
        match stop_reason {
            Some("end_turn" | "stop") => Self::Completed,
            Some("max_turns") => Self::MaxTurns,
            Some("aborted" | "cancelled") => Self::Interrupted,
            Some(_) | None if has_messages => Self::Completed,
            Some(_) | None => Self::Failed,
        }
    }
}

// ═══ 请求与结果 ═══

/// 子代理请求（对照旧 `SubAgentExecutor.AgentRequest` record）。
#[derive(Clone, Debug)]
pub struct AgentRequest {
    /// 代理唯一标识。
    pub agent_id: String,
    /// 任务提示词。
    pub prompt: String,
    /// 代理类型（explore / verification / plan / general-purpose / guide）。
    pub agent_type: Option<String>,
    /// 模型覆盖（别名或真实模型 ID）。
    pub model: Option<String>,
    /// 隔离模式。
    pub isolation: IsolationMode,
    /// 是否后台运行。
    pub run_in_background: bool,
    /// 团队名（Phase 2+ team 路由，本批不激活）。
    pub team_name: Option<String>,
    /// Fork 模式（Phase 2+，本批不激活）。
    pub fork: bool,
}

impl AgentRequest {
    /// 构造基本请求（无 team / fork）。
    #[must_use]
    pub fn new(
        agent_id: impl Into<String>,
        prompt: impl Into<String>,
        agent_type: Option<String>,
        model: Option<String>,
        isolation: IsolationMode,
        run_in_background: bool,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            prompt: prompt.into(),
            agent_type,
            model,
            isolation,
            run_in_background,
            team_name: None,
            fork: false,
        }
    }
}

/// 子代理执行结果（对照旧 `SubAgentExecutor.AgentResult` record）。
#[derive(Clone, Debug)]
pub struct AgentResult {
    /// 最终状态。
    pub status: AgentStatus,
    /// 结果文本（可能被截断至 `MAX_RESULT_SIZE_CHARS`）。
    pub result: Option<String>,
    /// 原始提示词（用于结果关联）。
    pub prompt: String,
    /// 异步模式下的输出文件路径。
    pub output_file: Option<String>,
}

/// 子代理结果最大字符数（对照旧 `MAX_RESULT_SIZE_CHARS = 100_000`）。
pub const MAX_RESULT_SIZE_CHARS: usize = 100_000;

impl AgentResult {
    /// 构造完成结果。
    #[must_use]
    pub fn completed(result: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            status: AgentStatus::Completed,
            result: Some(result.into()),
            prompt: prompt.into(),
            output_file: None,
        }
    }

    /// 构造失败结果。
    #[must_use]
    pub fn failed(message: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            status: AgentStatus::Failed,
            result: Some(message.into()),
            prompt: prompt.into(),
            output_file: None,
        }
    }

    /// 构造超时结果。
    #[must_use]
    pub fn timeout(message: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            status: AgentStatus::Timeout,
            result: Some(message.into()),
            prompt: prompt.into(),
            output_file: None,
        }
    }

    /// 截断结果文本至 `MAX_RESULT_SIZE_CHARS`（对照旧 `executeSyncInternal`
    /// 的截断逻辑）。
    pub fn truncate_result(&mut self) {
        if let Some(ref text) = self.result
            && text.chars().count() > MAX_RESULT_SIZE_CHARS
        {
            let kept: String = text.chars().take(MAX_RESULT_SIZE_CHARS).collect();
            self.result = Some(format!("{kept}\n...[truncated]"));
        }
    }

    /// 是否超时（对照旧 `AgentResult.isTimeout()`）。
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        self.status == AgentStatus::Timeout
    }
}

// ═══ 代理定义 ═══

/// 内置代理默认最大轮次（对照旧 `BuiltInAgentDefinition.DEFAULT_MAX_TURNS`）。
pub const DEFAULT_MAX_TURNS: u32 = 50;

/// 代理定义（对照旧 `SubAgentExecutor.AgentDefinition` record）。
///
/// 5 种内置代理的 `system_prompt_template` 移植旧 `SubAgentExecutor` 的
/// 5 个 `static final String` 常量。有意差异：完整验证提示 200+ 行仅保留
/// 核心约束段（禁止修改 / 9 验证类别 / VERDICT 输出格式），全文后续补全。
#[derive(Clone, Debug)]
pub struct AgentDefinition {
    /// 显示名。
    pub name: &'static str,
    /// 最大轮次。
    pub max_turns: u32,
    /// 默认模型（`None` = 继承父会话）。
    pub default_model: Option<&'static str>,
    /// 允许的工具集（`None` 或含 `"*"` = 全部）。
    pub allowed_tools: Option<HashSet<&'static str>>,
    /// 禁用的工具集。
    pub denied_tools: Option<HashSet<&'static str>>,
    /// 是否省略项目提示段。
    pub omit_project_prompt: bool,
    /// 系统提示模板。
    pub system_prompt_template: &'static str,
}

impl AgentDefinition {
    /// 按类型名解析代理定义（对照旧 `resolveAgentDefinition`）。
    #[must_use]
    pub fn resolve(agent_type: Option<&str>) -> &'static Self {
        match agent_type.map(str::to_ascii_lowercase).as_deref() {
            Some("explore") => &Self::EXPLORE,
            Some("verification") => &Self::VERIFICATION,
            Some("plan") => &Self::PLAN,
            Some("guide") => &Self::GUIDE,
            _ => &Self::GENERAL_PURPOSE,
        }
    }

    /// 工具是否被本定义允许（对照旧 `assembleToolPool` 的过滤逻辑）。
    #[must_use]
    pub fn is_tool_allowed(&self, tool_name: &str) -> bool {
        if let Some(denied) = &self.denied_tools
            && denied.contains(tool_name)
        {
            return false;
        }
        match &self.allowed_tools {
            None => true,
            Some(set) => set.contains("*") || set.contains(tool_name),
        }
    }
}

// ═══ 全局禁用工具集 ═══

/// 所有子代理禁用的工具名集合（对照旧 `assembleToolPool` 的 `denied` 变量）。
/// 防止递归：子代理不能再启动子代理或创建任务。
pub const GLOBALLY_DENIED_TOOLS: &[&str] = &[
    "Agent",
    "TaskCreate",
    "TaskUpdate",
    "TaskList",
    "TaskGet",
    "TaskStop",
];

// ═══ 系统提示模板 ═══

/// Explore 代理提示（对照旧 `EXPLORE_AGENT_PROMPT`，保留核心约束与搜索策略）。
const EXPLORE_PROMPT: &str = "\
你是一个搜索和探索专家。你在严格的只读模式下运行。\n\
\n\
## 约束条件\n\
- 你不能编辑、创建或删除任何文件\n\
- 你不能执行修改状态的命令\n\
- 你只能使用只读工具\n\
- 如果被要求进行修改，拒绝并说明你是只读模式\n\
\n\
## 搜索策略\n\
1. search_codebase —— 语义/概念搜索\n\
2. search_symbol —— 查找特定类/方法/变量定义\n\
3. Grep —— 精确文本模式匹配\n\
4. Glob —— 按文件名/扩展名查找文件\n\
5. Read —— 读取已确定的特定文件\n\
\n\
## 输出格式\n\
- 列出相关文件路径和行号\n\
- 引用关键代码片段（保持简短）\n\
- 总结组件之间的关系\n\
- 如果找不到某些内容，明确说明而不是猜测";

/// Verification 代理提示（对照旧 `VERIFICATION_AGENT_PROMPT`，保留核心约束）。
const VERIFICATION_PROMPT: &str = "\
你是一个验证专家。你的工作不是确认实现能工作——而是尝试破坏它。\n\
\n\
=== 关键：禁止修改项目 ===\n\
你被严格禁止：\n\
- 在项目目录中创建、修改或删除任何文件\n\
- 安装依赖或包\n\
- 运行 git 写操作\n\
\n\
=== 验证策略（9 个类别） ===\n\
前端变更 / 后端API变更 / CLI脚本变更 / 基础设施变更 / 库包变更 / \
Bug修复 / 数据管道 / 数据库迁移 / 重构\n\
\n\
=== 必要步骤 ===\n\
1. 读取项目 README 了解构建/测试命令\n\
2. 运行构建（如适用）。构建失败就自动 FAIL\n\
3. 运行测试套件。测试失败就自动 FAIL\n\
4. 运行 linter/类型检查器\n\
5. 检查相关代码的回归问题\n\
\n\
=== 输出格式 ===\n\
每个检查必须包含 Command run / Output observed / Result (PASS/FAIL)\n\
\n\
以如下行结尾：\n\
VERDICT: PASS 或 VERDICT: FAIL 或 VERDICT: PARTIAL";

/// Plan 代理提示（对照旧 `PLAN_AGENT_PROMPT`）。
const PLAN_PROMPT: &str = "\
你是一个软件架构师和规划专家。你在只读模式下运行。\n\
\n\
## 你的角色\n\
分析需求、探索代码库，并生成详细的实现计划。\n\
你不负责实现——你负责规划。\n\
\n\
## 约束条件\n\
- 你不能编辑、创建或删除任何文件\n\
- 你不能执行修改状态的命令\n\
- 你的输出就是计划——它必须能被另一个 agent 或开发者直接执行\n\
\n\
## 规划流程\n\
1. 理解需求：澄清任务范围和验收标准\n\
2. 探索代码库：查找相关文件、类和模式\n\
3. 设计方案：选择最符合现有模式的方法\n\
4. 创建实现计划：列出要创建/修改的具体文件\n\
\n\
## 输出格式\n\
你的计划必须以 \"实现关键文件\" 部分结尾：\n\
- Files to Modify\n\
- Files to Create\n\
- Files to Read\n\
- Execution Order";

/// General-purpose 代理提示（对照旧 `GENERAL_PURPOSE_AGENT_PROMPT`）。
const GENERAL_PURPOSE_PROMPT: &str = "\
你是一个通用 worker 代理。高效、正确地完成分配的任务。\n\
\n\
## 核心原则\n\
- 严格按照任务提示执行——不要添加未要求的功能或改进\n\
- 在修改之前先阅读现有代码\n\
- 修改后运行测试以验证正确性\n\
- 清晰地报告你的结果：你做了什么，什么成功了，什么没成功\n\
\n\
## 工作风格\n\
- 彻底但不过度工程化\n\
- 匹配现有代码风格和模式\n\
- 如果任务模糊，做出合理选择并记录你的假设";

/// Guide 代理提示（对照旧 `GUIDE_AGENT_PROMPT`）。
const GUIDE_PROMPT: &str = "\
你是一个专业的向导代理，精通 zkcode、工具系统和 LLM API。\n\
\n\
## 你的专业领域\n\
- zkcode 命令、配置和工具用法\n\
- 工具系统模式（工具调用、多轮对话、流式传输）\n\
- LLM API（聊天补全、工具调用、上下文优化）\n\
- MCP 服务器开发和配置\n\
\n\
## 输出风格\n\
- 提供具体的代码示例，而不是抽象描述\n\
- 包含 CLI 用法的命令行示例\n\
- 相关时引用代码库中的具体文件";

impl AgentDefinition {
    /// Explore 代理定义（对照旧 `AgentDefinition.EXPLORE`）。
    pub const EXPLORE: Self = Self {
        name: "Explore",
        max_turns: DEFAULT_MAX_TURNS,
        default_model: None,
        allowed_tools: None,
        denied_tools: None,
        omit_project_prompt: true,
        system_prompt_template: EXPLORE_PROMPT,
    };

    /// Verification 代理定义（对照旧 `AgentDefinition.VERIFICATION`）。
    pub const VERIFICATION: Self = Self {
        name: "Verification",
        max_turns: DEFAULT_MAX_TURNS,
        default_model: None,
        allowed_tools: None,
        denied_tools: None,
        omit_project_prompt: false,
        system_prompt_template: VERIFICATION_PROMPT,
    };

    /// Plan 代理定义（对照旧 `AgentDefinition.PLAN`）。
    pub const PLAN: Self = Self {
        name: "Plan",
        max_turns: DEFAULT_MAX_TURNS,
        default_model: None,
        allowed_tools: None,
        denied_tools: None,
        omit_project_prompt: true,
        system_prompt_template: PLAN_PROMPT,
    };

    /// General-purpose 代理定义（对照旧 `AgentDefinition.GENERAL_PURPOSE`）。
    pub const GENERAL_PURPOSE: Self = Self {
        name: "GeneralPurpose",
        max_turns: DEFAULT_MAX_TURNS,
        default_model: None,
        allowed_tools: None,
        denied_tools: None,
        omit_project_prompt: false,
        system_prompt_template: GENERAL_PURPOSE_PROMPT,
    };

    /// Guide 代理定义（对照旧 `AgentDefinition.GUIDE`）。
    pub const GUIDE: Self = Self {
        name: "Guide",
        max_turns: DEFAULT_MAX_TURNS,
        default_model: None,
        allowed_tools: None,
        denied_tools: None,
        omit_project_prompt: false,
        system_prompt_template: GUIDE_PROMPT,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolation_mode_parse() {
        assert_eq!(IsolationMode::parse("none"), IsolationMode::None);
        assert_eq!(IsolationMode::parse("worktree"), IsolationMode::Worktree);
        assert_eq!(IsolationMode::parse("NONE"), IsolationMode::None);
        assert_eq!(IsolationMode::parse(""), IsolationMode::None);
    }

    #[test]
    fn agent_status_classify() {
        assert_eq!(
            AgentStatus::classify(Some("end_turn"), true, false),
            AgentStatus::Completed
        );
        assert_eq!(
            AgentStatus::classify(Some("max_turns"), true, false),
            AgentStatus::MaxTurns
        );
        assert_eq!(
            AgentStatus::classify(Some("aborted"), true, false),
            AgentStatus::Interrupted
        );
        assert_eq!(
            AgentStatus::classify(None, false, false),
            AgentStatus::Failed
        );
        assert_eq!(
            AgentStatus::classify(None, true, false),
            AgentStatus::Completed
        );
        assert_eq!(
            AgentStatus::classify(None, false, true),
            AgentStatus::Failed
        );
    }

    #[test]
    fn agent_result_truncate() {
        let long = "x".repeat(MAX_RESULT_SIZE_CHARS + 100);
        let mut r = AgentResult::completed(long, "p");
        r.truncate_result();
        let result = r.result.expect("has result");
        assert!(result.ends_with("\n...[truncated]"));
        assert!(result.chars().count() <= MAX_RESULT_SIZE_CHARS + 20);
    }

    #[test]
    fn resolve_agent_definition() {
        assert_eq!(AgentDefinition::resolve(Some("explore")).name, "Explore");
        assert_eq!(
            AgentDefinition::resolve(Some("verification")).name,
            "Verification"
        );
        assert_eq!(AgentDefinition::resolve(Some("plan")).name, "Plan");
        assert_eq!(AgentDefinition::resolve(Some("guide")).name, "Guide");
        assert_eq!(
            AgentDefinition::resolve(Some("general-purpose")).name,
            "GeneralPurpose"
        );
        assert_eq!(AgentDefinition::resolve(None).name, "GeneralPurpose");
        assert_eq!(
            AgentDefinition::resolve(Some("unknown")).name,
            "GeneralPurpose"
        );
    }

    #[test]
    fn globally_denied_tools_prevents_recursion() {
        assert!(GLOBALLY_DENIED_TOOLS.contains(&"Agent"));
        assert!(GLOBALLY_DENIED_TOOLS.contains(&"TaskCreate"));
        assert!(GLOBALLY_DENIED_TOOLS.contains(&"TaskStop"));
    }
}
