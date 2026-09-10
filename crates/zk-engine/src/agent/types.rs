//! 子代理数据模型——对照旧 `SubAgentExecutor` 内部类与 `AgentDefinition`。
//!
//! 有意差异：Java 的 `AgentRequest` / `AgentResult` / `AgentDefinition` 为
//! `record`（不可变值对象），本实现取同构 `struct` + `Clone`；`IsolationMode`
//! 独立为本模块枚举而非 `SubAgentExecutor` 内部类（Rust 无内部类型）。
//! 5 种内置代理的系统提示模板以当前生产工具契约为准，并保留旧
//! `SubAgentExecutor` 的角色边界。

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
    /// 持久化硬预算在发起下一次模型请求前耗尽。
    BudgetExhausted,
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
            Self::BudgetExhausted => "budget_exhausted",
        }
    }

    /// 从 `stop_reason` 分类最终状态（对照旧 `classifyAgentStatus`）。
    #[must_use]
    pub fn classify(stop_reason: Option<&str>, has_messages: bool, has_error: bool) -> Self {
        if matches!(stop_reason, Some("timeout" | "TASK_DEADLINE_EXCEEDED")) {
            return Self::Timeout;
        }
        match stop_reason {
            // Stable budget failures are failed terminal *events*, but retain
            // their distinct partial-result status. Therefore exact budget
            // classification must precede the generic has_error fallback.
            Some(
                "budget_exhausted"
                | "BUDGET_EXHAUSTED"
                | "TOKEN_BUDGET_EXHAUSTED"
                | "COST_BUDGET_EXHAUSTED",
            ) => Self::BudgetExhausted,
            _ if has_error => Self::Failed,
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
    /// 完整结果文本。大小与内联/BLOB/partial 规则由统一 `TaskResult`
    /// 提交事务负责，执行器不得在持久化前静默截断。
    pub result: Option<String>,
    /// 原始提示词（用于结果关联）。
    pub prompt: String,
    /// 异步模式下的输出文件路径。
    pub output_file: Option<String>,
    /// Stable terminal code propagated into TaskResult and Agent failure
    /// notifications. `None` denotes a successful result.
    pub error_code: Option<String>,
}

impl AgentResult {
    /// 构造完成结果。
    #[must_use]
    pub fn completed(result: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            status: AgentStatus::Completed,
            result: Some(result.into()),
            prompt: prompt.into(),
            output_file: None,
            error_code: None,
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
            error_code: Some("AGENT_EXECUTION_FAILED".to_owned()),
        }
    }

    /// Construct a failure while preserving its stable originating code.
    #[must_use]
    pub fn failed_with_code(
        message: impl Into<String>,
        prompt: impl Into<String>,
        error_code: impl Into<String>,
    ) -> Self {
        Self {
            status: AgentStatus::Failed,
            result: Some(message.into()),
            prompt: prompt.into(),
            output_file: None,
            error_code: Some(error_code.into()),
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
            error_code: Some("TIMEOUT".to_owned()),
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
/// 5 种内置代理的 `system_prompt_template` 定义稳定角色边界；具体可用能力
/// 由执行期真实工具目录追加，模板不得假设某个工具一定存在。
#[derive(Clone, Debug)]
pub struct AgentDefinition {
    /// 显示名。
    pub name: &'static str,
    /// 最大轮次。
    pub max_turns: u32,
    /// 默认模型（`None` = 继承父会话）。
    pub default_model: Option<&'static str>,
    /// 允许的工具集（`None` 或含 `"*"` = 全部）。
    pub allowed_tools: Option<&'static [&'static str]>,
    /// 禁用的工具集。
    pub denied_tools: Option<&'static [&'static str]>,
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
            && denied.contains(&tool_name)
        {
            return false;
        }
        match &self.allowed_tools {
            None => true,
            Some(set) => set.contains(&"*") || set.contains(&tool_name),
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
    "TaskOutput",
    "TaskStop",
];

/// 在安全冻结阶段可向默认子 Agent 暴露的只读工具。
///
/// 这是一份显式的能力合同，而不是对工具名前缀的猜测。参数敏感、
/// 既可读又可写的工具（如 Bash / Config / Memory）不在此列；它们必须
/// 通过后续明确的可写能力门禁，不得因为某个空参数样例被判定为
/// `is_read_only` 就进入子 Agent 目录。
pub const READ_ONLY_CHILD_TOOLS: &[&str] = &[
    "Read",
    "ListDir",
    "Glob",
    "Grep",
    "GitDiff",
    "GitLog",
    "GitStatus",
    "WebSearch",
    "WebFetch",
    "ListMcpResources",
    "ReadMcpResource",
    "Snip",
    "TerminalCapture",
    "ToolSearch",
];

/// 通过子 Agent 写入门禁后仍只允许的有限可写工具。
///
/// 管理配置、记忆、Cron、Swarm 和递归 Agent/Task 工具即使在写模式下
/// 也不得暴露给 child，避免一个布尔开关意外放大权限。
pub const WRITE_CHILD_TOOLS: &[&str] = &["Write", "Edit", "Bash", "NotebookEdit"];

// ═══ 系统提示模板 ═══

/// Explore 代理提示。
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
- 从当前工具目录识别实际可用的只读搜索能力\n\
- 先缩小候选范围，再精确匹配，最后核对上下文；对应能力未提供时跳过该步骤\n\
- 只能调用当前工具目录列出的工具，不得尝试调用未列出的能力\n\
\n\
## 输出格式\n\
- 列出相关文件路径和行号\n\
- 引用关键代码片段（保持简短）\n\
- 总结组件之间的关系\n\
- 如果找不到某些内容，明确说明而不是猜测";

/// Verification 代理提示。
const VERIFICATION_PROMPT: &str = "\
你是一个验证专家。你的工作不是确认实现能工作——而是尝试破坏它。\n\
\n\
=== 关键：禁止修改项目 ===\n\
你被严格禁止：\n\
- 在项目目录中创建、修改或删除任何文件\n\
- 安装依赖或包\n\
- 运行 git 写操作\n\
即使工具目录中存在 Bash，也只能运行不会改变项目、依赖、仓库或外部系统状态的检查命令。\n\
\n\
=== 风险分类 ===\n\
- HIGH：认证授权、密钥、数据库迁移、并发/恢复、账本或不可逆状态变更\n\
- MEDIUM：公共 API、协议、CLI、配置、数据管道、跨组件行为或依赖边界\n\
- LOW：局部实现、文案或不改变外部契约的重构\n\
先声明风险等级和依据，再覆盖相关类别：前端、后端 API、CLI、基础设施、库包、Bug 修复、数据管道、数据库迁移、重构。\n\
\n\
=== 对抗性检查 ===\n\
1. 从改动和契约推导失败模式、边界输入与负向路径\n\
2. 检查错误传播、权限收窄、竞态、恢复/重试及兼容性\n\
3. 若 Bash 可用，运行范围最小且只读的构建、测试、lint 或类型检查\n\
4. 若 Bash 不可用，只能做静态检查，不得声称运行过命令或测试\n\
5. 对未覆盖、不可执行或证据不足的项目明确记录限制\n\
\n\
=== 证据格式 ===\n\
每个检查必须包含：\n\
- CHECK：检查对象与失败假设\n\
- METHOD：实际使用的工具或命令；未执行命令时写 STATIC REVIEW\n\
- EVIDENCE：文件路径/行号，或命令及观察到的关键输出\n\
- RESULT：PASS / FAIL / UNVERIFIED\n\
- LIMITATION：缺失能力或未覆盖风险；没有则写 NONE\n\
\n\
=== 最终裁决 ===\n\
- 任一确认缺陷或相关检查失败：VERDICT: FAIL；纯静态审查确认的缺陷也适用，且此规则优先\n\
- 只有全部相关检查均有执行证据且通过时：VERDICT: PASS\n\
- 未确认缺陷，但 Bash 不可用、存在 UNVERIFIED 或证据不完整时：VERDICT: PARTIAL\n\
最终一行必须且只能是：VERDICT: PASS、VERDICT: FAIL 或 VERDICT: PARTIAL";

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

/// General-purpose 代理提示。
const GENERAL_PURPOSE_PROMPT: &str = "\
你是一个通用 worker 代理。高效、正确地完成分配的任务。\n\
\n\
## 核心原则\n\
- 严格按照任务提示执行——不要添加未要求的功能或改进\n\
- 以当前工具目录为唯一能力来源；不要假设命令执行或写工具存在\n\
- 若目录提供写工具，在修改之前先阅读现有代码；否则保持只读\n\
- 若目录提供 Bash，在修改后运行相关测试；否则明确说明未执行验证\n\
- 缺少写工具时不得声称已经修改、创建或删除文件\n\
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
        allowed_tools: Some(READ_ONLY_CHILD_TOOLS),
        denied_tools: None,
        omit_project_prompt: true,
        system_prompt_template: EXPLORE_PROMPT,
    };

    /// Verification 代理定义（对照旧 `AgentDefinition.VERIFICATION`）。
    pub const VERIFICATION: Self = Self {
        name: "Verification",
        max_turns: DEFAULT_MAX_TURNS,
        default_model: None,
        allowed_tools: Some(READ_ONLY_CHILD_TOOLS),
        denied_tools: None,
        omit_project_prompt: false,
        system_prompt_template: VERIFICATION_PROMPT,
    };

    /// Plan 代理定义（对照旧 `AgentDefinition.PLAN`）。
    pub const PLAN: Self = Self {
        name: "Plan",
        max_turns: DEFAULT_MAX_TURNS,
        default_model: None,
        allowed_tools: Some(READ_ONLY_CHILD_TOOLS),
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
        allowed_tools: Some(READ_ONLY_CHILD_TOOLS),
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
        assert_eq!(
            AgentStatus::classify(Some("TASK_DEADLINE_EXCEEDED"), true, true),
            AgentStatus::Timeout
        );
        assert_eq!(
            AgentStatus::classify(Some("BUDGET_USAGE_INCOMPLETE"), true, true),
            AgentStatus::Failed
        );
        for reason in [
            "budget_exhausted",
            "BUDGET_EXHAUSTED",
            "TOKEN_BUDGET_EXHAUSTED",
            "COST_BUDGET_EXHAUSTED",
        ] {
            assert_eq!(
                AgentStatus::classify(Some(reason), true, true),
                AgentStatus::BudgetExhausted,
                "reason={reason}"
            );
        }
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
        for agent_type in ["explore", "verification", "plan", "guide"] {
            let definition = AgentDefinition::resolve(Some(agent_type));
            assert!(definition.is_tool_allowed("Read"));
            assert!(definition.is_tool_allowed("WebSearch"));
            assert!(!definition.is_tool_allowed("Write"));
            assert!(!definition.is_tool_allowed("Bash"));
        }
    }

    #[test]
    fn globally_denied_tools_prevents_recursion() {
        assert!(GLOBALLY_DENIED_TOOLS.contains(&"Agent"));
        assert!(GLOBALLY_DENIED_TOOLS.contains(&"TaskCreate"));
        assert!(GLOBALLY_DENIED_TOOLS.contains(&"TaskOutput"));
        assert!(GLOBALLY_DENIED_TOOLS.contains(&"TaskStop"));
    }

    #[test]
    fn built_in_prompts_defer_capabilities_and_define_verification_evidence() {
        for invented in ["search_codebase", "search_symbol", "GlobTool", "GrepTool"] {
            assert!(
                !EXPLORE_PROMPT.contains(invented),
                "invented tool: {invented}"
            );
        }
        for runtime_capability in ["Glob", "Grep", "Read", "CodeIntel"] {
            assert!(
                !EXPLORE_PROMPT.contains(runtime_capability),
                "static prompt assumed runtime capability: {runtime_capability}"
            );
        }
        assert!(EXPLORE_PROMPT.contains("当前工具目录"));
        for evidence_field in ["CHECK", "METHOD", "EVIDENCE", "RESULT", "LIMITATION"] {
            assert!(
                VERIFICATION_PROMPT.contains(evidence_field),
                "missing evidence field: {evidence_field}"
            );
        }
        assert!(VERIFICATION_PROMPT.contains("Bash 不可用"));
        assert!(VERIFICATION_PROMPT.contains("纯静态审查确认的缺陷也适用"));
        assert!(VERIFICATION_PROMPT.contains("未确认缺陷，但 Bash 不可用"));
        assert!(VERIFICATION_PROMPT.contains("VERDICT: PARTIAL"));
        assert!(GENERAL_PURPOSE_PROMPT.contains("不得声称已经修改"));
    }
}
