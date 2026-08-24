//! 3A.5 工具结果摘要器——压缩过大的工具结果，防止上下文溢出。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `engine/ToolResultSummarizer.java`：三级截断策略 + LLM 智能摘要 + 旧轮次
//!   清理 + 摘要提示注入；
//! - `engine/TokenCounter.java`：`estimateTokens(String)` = `text.length()` /
//!   `detectCharsPerToken(text)`（含 JSON / 中文占比 / `looksLikeCode` 检测）。
//!
//! # 逐值对齐（Java `main@581d407b` 实值为准）
//!
//! | 常量 | Java 实值 | 本模块 |
//! |---|---|---|
//! | `SOFT_LIMIT_CHARS` | `18_000` | [`SOFT_LIMIT_CHARS`] |
//! | `HARD_LIMIT_CHARS` | `50_000` | [`HARD_LIMIT_CHARS`] |
//! | `TRUNCATE_HEAD_CHARS` | `12_000` | [`TRUNCATE_HEAD_CHARS`] |
//! | `TRUNCATE_TAIL_CHARS` | `3_000` | [`TRUNCATE_TAIL_CHARS`] |
//! | `STALE_TURN_THRESHOLD` | 8 | [`STALE_TURN_THRESHOLD`] |
//! | `MAX_INPUT_LENGTH` | 300 | [`MAX_INPUT_LENGTH`] |
//! | `SUMMARY_MAX_TOKENS` | 256 | [`SUMMARY_MAX_TOKENS`] |
//! | `TIMEOUT_MS` | `10_000` | [`SUMMARY_TIMEOUT_MS`] |
//!
//! # 消息模型映射（对照 [`crate::context`] 同款映射）
//!
//! 旧 `Message.UserMessage`（`toolUseResult != null`）承载工具结果，本域映射为
//! [`zk_llm::Role::Tool`] 消息，其 `content` 即工具结果正文。故 Java 对
//! `UserMessage.toolResult` 的判定 / 截断 / 清理逐条落到 [`Role::Tool`]
//! 消息的 `content` 上（保留 `tool_call_id` 等其余字段不变）。
//!
//! # LLM 摘要端口（narrow port，非复用 `compact::Summarizer`）
//!
//! 旧 `ToolResultSummarizer` 的 LLM 路径对**单条 (工具名/入参/输出) 三元组**用
//! 轻量模型（`resolveModelAlias("light")`）以 git-commit 风格 prompt 生成单行
//! 摘要——与 [`crate::context::compact::Summarizer`]「摘要整段消息列表以做
//! 会话压缩」契约**语义不同**（入参形状、目标、prompt 均异）。强行复用会造成
//! 语义错配，故此处引入独立窄端口 [`LightModelSummarizer`]：全部 Java 常量与
//! prompt 逻辑仍留在本模块内，端口仅为轻量模型的同步适配层。缺省不装配
//! （`None`）时逐条对照 Java `providerRegistry == null` 分支——降级为截断。
//! 与 `compact.rs` 一致，同步 LLM + 超时编排归后续接线，[`SUMMARY_TIMEOUT_MS`]
//! 作为对齐常量留档。
//!
//! # Java 调用点核验（`git grep` 全仓核验，`main@581d407b`）
//!
//! 本模块四个公开策略方法在旧仓库的**实际**调用点如下（排除类自身）：
//!
//! | 方法 | 旧仓库调用点 | 本仓库接线 |
//! |---|---|---|
//! | `processToolResults` | `engine/QueryEngine.java` L1443（轮末） | 已接入引擎主循环 |
//! | `shouldInjectSummarizeHint` | `prompt/SystemPromptBuilder.java` L1217 | 见下 |
//! | `clearStaleToolResults` | **零调用** | 不接主循环（留痕） |
//! | `summarizeIfNeeded` | **零调用** | 不接主循环（留痕） |
//!
//! 三点结论（以 Java 实现为准）：
//!
//! 1. `shouldInjectSummarizeHint` 的语义**不是**向消息流注入提示消息，而是
//!    `SystemPromptBuilder.getSummarizeToolResultsSection()` 的系统提示**动态段
//!    二选一**。本模块以 [`ToolResultSummarizer::summarize_tool_results_section`]
//!    忠实还原（含两条逐字常量）。系统提示装配域尚未迁移（引擎从不设
//!    `ChatRequest::system_prompt`），故该选择器暂无引擎侧调用方，待 prompt 域
//!    落地即可接线。
//! 2. `clearStaleToolResults` 在 `main@581d407b` **无任何调用方**（其 Javadoc
//!    自称「由 `CompactService` / `tryAutoCompact` 调用」，但该调用并不存在），
//!    属被取代的残留代码：同一职责（旧工具结果替换为 cleared 标记、保护尾部）
//!    已由 **L1 `MicroCompactService`** 在
//!    [`crate::context::ContextCascade::execute_pre_api_cascade`] 中每轮无条件
//!    执行。若再接入主循环会在 L1 之上**二次清理**，既偏离旧行为又额外销毁
//!    上下文，故**不接线**，仅保留公开 API 维持能力对等。
//! 3. `summarizeIfNeeded` 同为零调用；其能力已由 L0 Snip 截断与
//!    `CompactService` 的 LLM 摘要覆盖。保留公开 API，不接主循环。
//!
//! # Feature-flag（沿用 #34 `OnceLock` 进程级读取风格）
//!
//! [`summarizer_enabled`] 读取 [`SUMMARIZER_ENABLED_ENV`]，缺省开启；置 `false`
//! / `0` / `off` / `no` 时 [`ToolResultSummarizer::new`] 构造的摘要器**一键退回
//! 接入前行为**：截断 / 清理 / 摘要 / 提示注入全部直通（不改动上下文）。

#![allow(
    clippy::module_name_repetitions,
    reason = "ToolResultSummarizer / LightModelSummarizer 对齐旧领域术语，不按模块前缀裁剪"
)]

use std::sync::{Arc, OnceLock};

use zk_llm::{ChatMessage, Role};

// ===== 截断策略常量（逐值对照旧 ToolResultSummarizer）=====

/// 软限制：超过此字符数触发截断（约 5000 token）。
pub const SOFT_LIMIT_CHARS: usize = 18_000;

/// 硬限制：超过此字符数触发硬截断（约 15000 token）。
pub const HARD_LIMIT_CHARS: usize = 50_000;

/// 截断后保留的头部字符数。
pub const TRUNCATE_HEAD_CHARS: usize = 12_000;

/// 截断后保留的尾部字符数。
pub const TRUNCATE_TAIL_CHARS: usize = 3_000;

/// 旧轮次清理阈值：超过此轮次数的工具结果标记为可清理。
pub const STALE_TURN_THRESHOLD: u32 = 8;

// ===== LLM 摘要相关常量（P-AG-01）=====

/// LLM 摘要输入截断长度（工具名 / 输入 / 输出各截断到此长度作为 LLM 输入）。
pub const MAX_INPUT_LENGTH: usize = 300;

/// LLM 摘要最大生成 token 数。
pub const SUMMARY_MAX_TOKENS: u32 = 256;

/// LLM 摘要调用超时（毫秒，对齐常量；同步超时编排归后续接线）。
pub const SUMMARY_TIMEOUT_MS: u64 = 10_000;

/// 摘要器总开关环境变量（`false` / `0` / `off` / `no` → 关闭，其余含缺省
/// → 开启）。
pub const SUMMARIZER_ENABLED_ENV: &str = "ZK_TOOL_RESULT_SUMMARIZER_ENABLED";

/// LLM 摘要系统提示（逐字对照旧 `SUMMARY_SYSTEM_PROMPT`，git-commit 风格）。
pub const SUMMARY_SYSTEM_PROMPT: &str =
    "你是一个工具结果摘要器。生成一份关于工具调用完成内容的简洁摘要。摘要应当：
- 单行，不超过 100 个字符
- 过去时态动词 + 关键名词（类似 git commit 标题）
- 省略冠词和连接词

示例：
- Searched authentication logic in auth/
- Read 15 files matching *.java pattern
- Listed directory contents of /src/main
- Executed 'npm test' with 3 failures";

/// `summarize_tool_results` 系统提示基础段（逐字对照旧
/// `SystemPromptBuilder.SUMMARIZE_TOOL_RESULTS_SECTION`）。
pub const SUMMARIZE_TOOL_RESULTS_SECTION: &str = "处理工具结果时，在你的回复中记录任何你可能之后需要的重要信息，因为原始工具结果可能会在之后被清除。";

/// 上下文接近限制时的紧急摘要提示（逐字对照旧
/// `SystemPromptBuilder.URGENT_SUMMARIZE_HINT`）。
pub const URGENT_SUMMARIZE_HINT: &str = "重要：你的上下文正在变大。早期回合的工具结果可能很快被清除。如果你还未记录工具结果中的重要信息（文件内容、测试输出、搜索结果等），现在就在你的回复中记录下来。一旦原始工具结果被清除，你将无法再访问它们。";

/// 旧工具结果清理占位标记（逐字对照旧 `clearStaleToolResults` 的替换文案）。
const STALE_CLEARED_MARKER: &str = "[tool result cleared — write down important information from tool results to ensure you don't lose it]";

// ===== detectCharsPerToken 相关常量（逐值对照旧 TokenCounter）=====

/// 默认每 token 字符数（英文约 4、中文约 2、混合取 3.5）。
const DEFAULT_CHARS_PER_TOKEN: f64 = 3.5;

/// JSON 内容每 token 字符数（结构化更紧凑）。
const JSON_CHARS_PER_TOKEN: f64 = 2.0;

/// 代码内容每 token 字符数。
const CODE_CHARS_PER_TOKEN: f64 = 3.5;

/// 中文内容每 token 字符数。
const CHINESE_CHARS_PER_TOKEN: f64 = 2.0;

/// 摘要器总开关（进程级一次性读取环境变量；缺省开启）。
///
/// 关闭时 [`ToolResultSummarizer::new`] 的摘要器逐字退回接入前行为：不截断、
/// 不清理、不摘要、不注入提示。
#[must_use]
pub fn summarizer_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var(SUMMARIZER_ENABLED_ENV) {
        Ok(raw) => !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "false" | "0" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// 轻量模型摘要窄端口（LLM 智能摘要的同步适配层；见模块文档）。
///
/// 缺省不装配时降级为截断（对照旧 `providerRegistry == null`）。
pub trait LightModelSummarizer: Send + Sync {
    /// 以轻量模型对给定 system / user 提示生成单行摘要；`None` → 不可用
    /// （触发降级截断，对照旧 LLM 调用失败分支）。
    fn summarize(&self, system_prompt: &str, user_prompt: &str, max_tokens: u32) -> Option<String>;
}

/// 工具结果摘要器（对照旧 `ToolResultSummarizer`）。
pub struct ToolResultSummarizer {
    light_model: Option<Arc<dyn LightModelSummarizer>>,
    gate_enabled: bool,
}

impl std::fmt::Debug for ToolResultSummarizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolResultSummarizer")
            .field("light_model", &self.light_model.is_some())
            .field("gate_enabled", &self.gate_enabled)
            .finish()
    }
}

impl Default for ToolResultSummarizer {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolResultSummarizer {
    /// 构造摘要器（无 LLM，仅截断策略；对照旧兼容构造函数），门控开关取
    /// [`summarizer_enabled`]。
    #[must_use]
    pub fn new() -> Self {
        Self::with_gate(summarizer_enabled())
    }

    /// 以显式门控开关构造摘要器（无 LLM；供测试确定性覆盖两条路径）。
    #[must_use]
    pub fn with_gate(gate_enabled: bool) -> Self {
        Self {
            light_model: None,
            gate_enabled,
        }
    }

    /// 装配轻量模型端口以启用 LLM 智能摘要（对照旧 `@Autowired` 构造函数）。
    #[must_use]
    pub fn with_light_model(light_model: Arc<dyn LightModelSummarizer>) -> Self {
        Self {
            light_model: Some(light_model),
            gate_enabled: summarizer_enabled(),
        }
    }

    /// 处理当前轮次的工具结果——截断过大的结果（对照旧 `processToolResults`）。
    ///
    /// `current_turn` 保留以对齐 Java 签名（Java 本方法同样未使用该参数）。
    #[must_use]
    pub fn process_tool_results(
        &self,
        messages: &[ChatMessage],
        current_turn: u32,
    ) -> Vec<ChatMessage> {
        let _ = current_turn;
        if !self.gate_enabled {
            return messages.to_vec();
        }
        messages
            .iter()
            .map(|message| {
                if message.role == Role::Tool && char_count(&message.content) > SOFT_LIMIT_CHARS {
                    truncate_tool_result(message)
                } else {
                    message.clone()
                }
            })
            .collect()
    }

    /// 清理旧轮次的工具结果——替换为占位标记（对照旧 `clearStaleToolResults`）。
    #[must_use]
    pub fn clear_stale_tool_results(
        &self,
        messages: &[ChatMessage],
        current_turn: u32,
    ) -> Vec<ChatMessage> {
        if !self.gate_enabled {
            return messages.to_vec();
        }
        let total = messages.len();
        messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let stale = current_turn.saturating_sub(estimate_turn(index, total, current_turn))
                    > STALE_TURN_THRESHOLD;
                if message.role == Role::Tool && stale {
                    ChatMessage {
                        content: STALE_CLEARED_MARKER.to_owned(),
                        ..message.clone()
                    }
                } else {
                    message.clone()
                }
            })
            .collect()
    }

    /// 检查是否需要注入摘要提示（当上下文接近限制时；对照旧
    /// `shouldInjectSummarizeHint`）。
    ///
    /// Java 用无模型 `estimateTokens(messages)`（固定 3.5 字符/token 比），本
    /// 实现以 [`crate::context::estimate_tokens`] 传空模型名复现（空名 →
    /// `DEFAULT_CAPABILITIES` 的 3.5 比）。`context_limit == 0` 时返回
    /// `false`（Java 为 `<= 0`，u32 域内等价）。
    #[must_use]
    pub fn should_inject_summarize_hint(
        &self,
        messages: &[ChatMessage],
        context_limit: u32,
    ) -> bool {
        if !self.gate_enabled || context_limit == 0 {
            return false;
        }
        let current_tokens = crate::context::estimate_tokens(messages, "");
        f64::from(current_tokens) > f64::from(context_limit) * 0.7
    }

    /// `summarize_tool_results` 系统提示动态段（逐字对照旧
    /// `SystemPromptBuilder.getSummarizeToolResultsSection`）。
    ///
    /// 这是旧仓库中 [`Self::should_inject_summarize_hint`] 的**唯一生产调用点**
    /// （`prompt/SystemPromptBuilder.java` L1217）：命中则以
    /// [`URGENT_SUMMARIZE_HINT`] 替换基础段 [`SUMMARIZE_TOOL_RESULTS_SECTION`]，
    /// 而非向消息流注入新消息。旧守卫 `msgs != null && limit > 0` 对应本实现的
    /// 空切片与 `context_limit == 0` 分支（后者由
    /// [`Self::should_inject_summarize_hint`] 内部处理）。
    ///
    /// 系统提示装配域（旧 `SystemPromptBuilder`）尚未迁移，故本方法当前无引擎侧
    /// 调用方；待该域落地即可直接接线（见模块文档「Java 调用点核验」）。
    #[must_use]
    pub fn summarize_tool_results_section(
        &self,
        messages: &[ChatMessage],
        context_limit: u32,
    ) -> &'static str {
        if self.should_inject_summarize_hint(messages, context_limit) {
            URGENT_SUMMARIZE_HINT
        } else {
            SUMMARIZE_TOOL_RESULTS_SECTION
        }
    }

    /// 对工具输出生成摘要（如超过阈值；对照旧 `summarizeIfNeeded`）。
    ///
    /// 判定序：输出空白 → 原样返回；估算 token `<= max_tokens` → 原样返回；
    /// 未装配轻量模型（`None`）→ 截断；装配则调端口，返回 `None` / 失败 →
    /// 降级截断。
    #[must_use]
    pub fn summarize_if_needed(
        &self,
        tool_name: &str,
        tool_input: &str,
        tool_output: &str,
        max_tokens: u32,
    ) -> String {
        if !self.gate_enabled || tool_output.trim().is_empty() {
            return tool_output.to_owned();
        }
        if estimate_text_tokens(tool_output) <= max_tokens {
            return tool_output.to_owned();
        }
        match &self.light_model {
            None => truncate(tool_output, max_tokens),
            Some(port) => {
                let user_prompt = build_user_prompt(tool_name, tool_input, tool_output);
                match port.summarize(SUMMARY_SYSTEM_PROMPT, &user_prompt, SUMMARY_MAX_TOKENS) {
                    Some(response) => wrap_summary(&response, char_count(tool_output)),
                    None => truncate(tool_output, max_tokens),
                }
            }
        }
    }
}

/// 截断过大的工具结果，保留头尾和截断提示（对照旧 `truncateToolResult`）。
fn truncate_tool_result(message: &ChatMessage) -> ChatMessage {
    let result = &message.content;
    let len = char_count(result);
    if len <= SOFT_LIMIT_CHARS {
        return message.clone();
    }
    let head = take_chars(result, TRUNCATE_HEAD_CHARS);
    let tail = last_chars(result, TRUNCATE_TAIL_CHARS);
    let truncated = if len > HARD_LIMIT_CHARS {
        // 硬截断：只保留头尾。
        format!(
            "{head}\n\n... [TRUNCATED: result was {len} chars, showing first \
             {TRUNCATE_HEAD_CHARS} chars. Write down any important information you \
             need.] ...\n\n{tail}"
        )
    } else {
        // 软截断：保留头尾 + 省略计数。
        let omitted = len - TRUNCATE_HEAD_CHARS - TRUNCATE_TAIL_CHARS;
        format!("{head}\n\n... [TRUNCATED: {omitted} chars omitted] ...\n\n{tail}")
    };
    ChatMessage {
        content: truncated,
        ..message.clone()
    }
}

/// 估算消息所在轮次（对照旧 `estimateTurn`：按索引 / 总数比例）。
fn estimate_turn(message_index: usize, total_messages: usize, current_turn: u32) -> u32 {
    if total_messages == 0 {
        return current_turn;
    }
    truncate_to_u32(as_f64(message_index) / as_f64(total_messages) * f64::from(current_turn))
}

/// 构造 LLM 摘要 user prompt（对照旧 `generateSummary` 的 `userPrompt`）。
fn build_user_prompt(tool_name: &str, tool_input: &str, tool_output: &str) -> String {
    format!(
        "Tool: {tool_name}\nInput: {}\nOutput: {}\n\nSummary:",
        truncate_str(tool_input, MAX_INPUT_LENGTH),
        truncate_str(tool_output, MAX_INPUT_LENGTH)
    )
}

/// 包装 LLM 摘要输出（对照旧 `generateSummary` 的返回拼接）。
fn wrap_summary(response: &str, original_len: usize) -> String {
    format!(
        "[工具摘要] {}\n\n[原始输出已压缩, 原长度: {original_len} 字符]",
        response.trim()
    )
}

/// 按 token 预算截断文本（LLM 摘要降级时使用；对照旧 `truncate`）。
fn truncate(text: &str, max_tokens: u32) -> String {
    let max_chars = usize::try_from(max_tokens)
        .unwrap_or(usize::MAX)
        .saturating_mul(4);
    let len = char_count(text);
    if len <= max_chars {
        return text.to_owned();
    }
    format!(
        "{}\n... [已截断, 总长 {len} 字符]",
        take_chars(text, max_chars)
    )
}

/// 截断字符串到指定最大长度（对照旧 `truncateStr`）。
fn truncate_str(text: &str, max_length: usize) -> String {
    if char_count(text) <= max_length {
        text.to_owned()
    } else {
        format!("{}...", take_chars(text, max_length))
    }
}

/// 单文本 token 估算（对照旧 `TokenCounter.estimateTokens(String)`）。
///
/// `(int)(text.length() / detectCharsPerToken(text))`——**向零截断**（非
/// [`crate::context::estimate_tokens`] 消息列表路径的向上取整）。空文本 → 0。
fn estimate_text_tokens(text: &str) -> u32 {
    if text.is_empty() {
        return 0;
    }
    truncate_to_u32(as_f64(char_count(text)) / detect_chars_per_token(text))
}

/// 自动检测内容类型并返回字符 / token 比（对照旧 `detectCharsPerToken`）。
fn detect_chars_per_token(text: &str) -> f64 {
    let len = char_count(text);
    if len < 10 {
        return DEFAULT_CHARS_PER_TOKEN;
    }
    let trimmed = text.trim();
    if (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
    {
        return JSON_CHARS_PER_TOKEN;
    }
    let chinese_chars = text.chars().filter(|c| is_han(*c)).count();
    let chinese_ratio = as_f64(chinese_chars) / as_f64(len);
    if chinese_ratio > 0.3 {
        return CHINESE_CHARS_PER_TOKEN.mul_add(
            chinese_ratio,
            DEFAULT_CHARS_PER_TOKEN * (1.0 - chinese_ratio),
        );
    }
    if looks_like_code(trimmed) {
        return CODE_CHARS_PER_TOKEN;
    }
    DEFAULT_CHARS_PER_TOKEN
}

/// 启发式检测文本是否看起来像代码（对照旧 `looksLikeCode`）。
fn looks_like_code(text: &str) -> bool {
    // 取前 500 字符检测，避免大文本性能问题。
    let sample: String = text.chars().take(500).collect();
    let mut indicators = 0_u32;

    // 花括号 / 分号密度。
    let braces = sample.chars().filter(|c| *c == '{' || *c == '}').count();
    let semicolons = sample.chars().filter(|c| *c == ';').count();
    if braces > 2 || semicolons > 3 {
        indicators += 1;
    }

    // 常见代码关键字。
    if sample.contains("import ")
        || sample.contains("function ")
        || sample.contains("class ")
        || sample.contains("def ")
        || sample.contains("public ")
        || sample.contains("private ")
        || sample.contains("const ")
        || sample.contains("let ")
        || sample.contains("var ")
        || sample.contains("return ")
    {
        indicators += 1;
    }

    // 缩进模式（连续空格 / 制表符开头行）。
    let indented = sample
        .lines()
        .filter(|line| line.starts_with("    ") || line.starts_with('\t'))
        .count();
    if indented > 3 {
        indicators += 1;
    }

    indicators >= 2
}

/// 是否为 Han（汉字）标量——零依赖近似旧 `Character.UnicodeScript.HAN`。
///
/// 覆盖主要 CJK 区块（供应链纪律：不引入 unicode 脚本依赖，见任务书约束 6）。
/// 阈值判定为占比 `> 0.3`，扩展区罕见字的微小差异不影响典型中文内容判定。
fn is_han(c: char) -> bool {
    matches!(
        u32::from(c),
        0x2E80..=0x2EFF      // CJK 部首补充
        | 0x3005            // 表意文字迭代号
        | 0x3007            // 表意数字零
        | 0x3021..=0x3029   // 苏州码子
        | 0x3400..=0x4DBF   // 扩展 A
        | 0x4E00..=0x9FFF   // 基本区
        | 0xF900..=0xFAFF   // 兼容表意文字
        | 0x20000..=0x2A6DF // 扩展 B
        | 0x2A700..=0x2EBEF // 扩展 C-F
        | 0x2F800..=0x2FA1F // 兼容补充
    )
}

// ===== 字符边界 / 数值转换辅助（对齐 #34 Unicode 标量计数约定）=====

/// 字符数（Unicode 标量计数；旧 Java `String.length()` 为 UTF-16 码元数，二者
/// 在 BMP（含全部 CJK 常用字）一致）。
fn char_count(text: &str) -> usize {
    text.chars().count()
}

/// 取前 `n` 个字符。
fn take_chars(text: &str, n: usize) -> String {
    text.chars().take(n).collect()
}

/// 取末 `n` 个字符。
fn last_chars(text: &str, n: usize) -> String {
    let total = char_count(text);
    text.chars().skip(total.saturating_sub(n)).collect()
}

/// usize → f64（字符计数量级远低于 f64 精确整数上界）。
#[expect(
    clippy::cast_precision_loss,
    reason = "字符 / 索引计数量级远低于 f64 精确整数上界（2^53），旧实现同为 long→double 提升"
)]
fn as_f64(value: usize) -> f64 {
    value as f64
}

/// 向零截断并饱和到 [`u32::MAX`]（复刻旧 `(int)` 强转语义 + 上下界夹紧）。
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "上界由 f64::from(u32::MAX) 比较夹紧、下界由 <= 0 分支拦截，转换无溢出；向零截断复刻 Java (int) 强转"
)]
fn truncate_to_u32(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    if value >= f64::from(u32::MAX) {
        return u32::MAX;
    }
    value as u32
}

#[cfg(test)]
mod tests {
    use super::{
        HARD_LIMIT_CHARS, LightModelSummarizer, SOFT_LIMIT_CHARS, STALE_TURN_THRESHOLD,
        SUMMARIZE_TOOL_RESULTS_SECTION, ToolResultSummarizer, URGENT_SUMMARIZE_HINT,
        estimate_text_tokens, estimate_turn, truncate_str,
    };
    use std::sync::Arc;
    use zk_llm::{ChatMessage, Role};

    /// 软截断：SOFT < len <= HARD → 头尾 + 省略计数标记。
    #[test]
    fn process_soft_truncates_between_soft_and_hard() {
        let output = "a".repeat(20_000);
        let summarizer = ToolResultSummarizer::with_gate(true);
        let processed = summarizer.process_tool_results(&[ChatMessage::tool("t1", output)], 0);
        let content = &processed[0].content;
        assert!(content.contains("[TRUNCATED: 5000 chars omitted]"));
        assert!(super::char_count(content) < 20_000);
        // tool_call_id 保留。
        assert_eq!(processed[0].tool_call_id.as_deref(), Some("t1"));
        assert_eq!(processed[0].role, Role::Tool);
    }

    /// 硬截断：len > HARD → 头尾 + "result was N chars" 提示。
    #[test]
    fn process_hard_truncates_above_hard_limit() {
        let output = "b".repeat(60_000);
        let summarizer = ToolResultSummarizer::with_gate(true);
        let processed = summarizer.process_tool_results(&[ChatMessage::tool("t1", output)], 0);
        let content = &processed[0].content;
        assert!(content.contains("result was 60000 chars, showing first 12000 chars"));
        assert!(content.contains("Write down any important information"));
    }

    /// <= SOFT 原样保留；非 Tool 消息一律不动。
    #[test]
    fn process_leaves_small_and_non_tool_untouched() {
        let summarizer = ToolResultSummarizer::with_gate(true);
        let small = "x".repeat(SOFT_LIMIT_CHARS); // 恰等于 SOFT，不 > SOFT。
        let big_user = "y".repeat(HARD_LIMIT_CHARS + 1);
        let input = vec![
            ChatMessage::tool("t1", small.clone()),
            ChatMessage::user(big_user.clone()),
        ];
        let processed = summarizer.process_tool_results(&input, 0);
        assert_eq!(processed[0].content, small);
        assert_eq!(processed[1].content, big_user);
    }

    /// 旧轮次工具结果被替换为清理标记；较新的保留。
    #[test]
    fn clear_stale_replaces_old_tool_results() {
        // 20 条消息，current_turn = 10。索引 0 的 estimateTurn = 0，
        // 10 - 0 = 10 > 8 → 清理；末尾索引 estimateTurn 接近 10 → 保留。
        let mut messages: Vec<ChatMessage> = Vec::new();
        for _ in 0..20 {
            messages.push(ChatMessage::tool("t", "some tool result body"));
        }
        let summarizer = ToolResultSummarizer::with_gate(true);
        let cleaned = summarizer.clear_stale_tool_results(&messages, 10);
        assert!(cleaned[0].content.contains("tool result cleared"));
        // 末条不 stale（estimateTurn ≈ 9，10-9=1 <= 8）。
        assert_eq!(cleaned[19].content, "some tool result body");
        // tool_call_id 保留。
        assert_eq!(cleaned[0].tool_call_id.as_deref(), Some("t"));
    }

    /// estimateTurn 边界（对照旧公式）。
    #[test]
    fn estimate_turn_matches_legacy_formula() {
        assert_eq!(estimate_turn(0, 0, 7), 7); // total<=0 → currentTurn
        assert_eq!(estimate_turn(0, 20, 10), 0); // 0/20*10 = 0
        assert_eq!(estimate_turn(10, 20, 10), 5); // 0.5*10 = 5
        assert_eq!(estimate_turn(19, 20, 10), 9); // 0.95*10 = 9.5 → (int)9
    }

    /// 摘要提示注入阈值（70%）。
    #[test]
    fn should_inject_hint_at_seventy_percent() {
        let summarizer = ToolResultSummarizer::with_gate(true);
        // 单条 3500 字符 user：estimate = ceil(3500/3.5 + 4) = 1004 tokens。
        let messages = vec![ChatMessage::user("a".repeat(3_500))];
        assert!(!summarizer.should_inject_summarize_hint(&messages, 0)); // 0 → false
        assert!(summarizer.should_inject_summarize_hint(&messages, 1_000)); // 1004 > 700
        assert!(!summarizer.should_inject_summarize_hint(&messages, 2_000)); // 1004 <= 1400
    }

    /// 系统提示动态段二选一（对照旧 `getSummarizeToolResultsSection`）。
    #[test]
    fn summarize_tool_results_section_switches_to_urgent_above_threshold() {
        let summarizer = ToolResultSummarizer::with_gate(true);
        let messages = vec![ChatMessage::user("a".repeat(3_500))];
        // 命中阈值 → 紧急提示。
        assert_eq!(
            summarizer.summarize_tool_results_section(&messages, 1_000),
            URGENT_SUMMARIZE_HINT
        );
        // 未命中 → 基础段。
        assert_eq!(
            summarizer.summarize_tool_results_section(&messages, 2_000),
            SUMMARIZE_TOOL_RESULTS_SECTION
        );
        // limit == 0（对照旧 `limit > 0` 守卫）→ 基础段。
        assert_eq!(
            summarizer.summarize_tool_results_section(&messages, 0),
            SUMMARIZE_TOOL_RESULTS_SECTION
        );
        // 空消息 → 基础段。
        assert_eq!(
            summarizer.summarize_tool_results_section(&[], 1_000),
            SUMMARIZE_TOOL_RESULTS_SECTION
        );
    }

    /// summarizeIfNeeded：空白 / 未超阈值 → 原样返回。
    #[test]
    fn summarize_returns_input_when_blank_or_under_threshold() {
        let summarizer = ToolResultSummarizer::with_gate(true);
        assert_eq!(summarizer.summarize_if_needed("Read", "", "   ", 10), "   ");
        // 20 字符 ascii → est (int)(20/3.5)=5 <= 100 → 原样。
        let small = "z".repeat(20);
        assert_eq!(
            summarizer.summarize_if_needed("Read", "{}", &small, 100),
            small
        );
    }

    /// summarizeIfNeeded：超阈值 + 无 LLM → 截断降级。
    #[test]
    fn summarize_truncates_without_llm() {
        let summarizer = ToolResultSummarizer::with_gate(true);
        let output = "q".repeat(100); // est (int)(100/3.5)=28 > 10
        let result = summarizer.summarize_if_needed("Bash", "ls", &output, 10);
        // maxChars = 10*4 = 40 → 头 40 + 截断提示。
        assert!(result.contains("[已截断, 总长 100 字符]"));
        assert!(super::char_count(&result) < 100 + 30);
    }

    /// summarizeIfNeeded：超阈值 + LLM 可用 → 包装摘要。
    #[test]
    fn summarize_wraps_llm_output() {
        struct Fixed;
        impl LightModelSummarizer for Fixed {
            fn summarize(&self, _sys: &str, _user: &str, _max: u32) -> Option<String> {
                Some("  Read config files  ".to_owned())
            }
        }
        let summarizer = ToolResultSummarizer::with_light_model(Arc::new(Fixed));
        let output = "w".repeat(100);
        let result = summarizer.summarize_if_needed("Read", "cfg", &output, 10);
        assert!(result.contains("[工具摘要] Read config files"));
        assert!(result.contains("原长度: 100 字符"));
    }

    /// summarizeIfNeeded：LLM 返回 None → 降级截断。
    #[test]
    fn summarize_falls_back_to_truncate_on_llm_none() {
        struct Unavailable;
        impl LightModelSummarizer for Unavailable {
            fn summarize(&self, _sys: &str, _user: &str, _max: u32) -> Option<String> {
                None
            }
        }
        let summarizer = ToolResultSummarizer::with_light_model(Arc::new(Unavailable));
        let output = "e".repeat(100);
        let result = summarizer.summarize_if_needed("Read", "x", &output, 10);
        assert!(result.contains("[已截断"));
    }

    /// `estimate_text_tokens` 分类：JSON / 中文 / 短文 / 普通。
    #[test]
    fn estimate_text_tokens_detects_content_type() {
        // JSON：ratio 2.0 → (int)(len/2.0)。
        let json = format!("{{{}}}", "\"k\":1,".repeat(10));
        let len = super::char_count(&json);
        // JSON ratio 2.0 → floor(len / 2.0) 与整数除法 len / 2 等价。
        let expected = u32::try_from(len / 2).expect("token count fits u32");
        assert_eq!(estimate_text_tokens(&json), expected);
        // 短文（<10 字符）→ ratio 3.5 → (int)(3 / 3.5) = 0。
        assert_eq!(estimate_text_tokens("abc"), 0);
        // 空 → 0。
        assert_eq!(estimate_text_tokens(""), 0);
        // 中文占比 > 0.3 → 加权比，token 数高于纯英文同长。
        let chinese = "这是一段中文内容用于测试比率检测逻辑".to_owned();
        assert!(estimate_text_tokens(&chinese) > 0);
    }

    /// `truncate_str` 边界。
    #[test]
    fn truncate_str_appends_ellipsis_when_over_limit() {
        assert_eq!(truncate_str("hello", 10), "hello");
        assert_eq!(truncate_str("hello", 3), "hel...");
        assert_eq!(truncate_str("", 5), "");
    }

    /// 门控关闭 → 全部直通（退回接入前行为）。
    #[test]
    fn disabled_gate_passes_through() {
        let summarizer = ToolResultSummarizer::with_gate(false);
        let big = "a".repeat(60_000);
        let msgs = vec![ChatMessage::tool("t", big.clone())];
        // process 不截断。
        assert_eq!(summarizer.process_tool_results(&msgs, 0)[0].content, big);
        // clear 不清理。
        assert_eq!(
            summarizer.clear_stale_tool_results(&msgs, 100)[0].content,
            big
        );
        // hint 恒 false。
        assert!(!summarizer.should_inject_summarize_hint(&msgs, 1));
        // 段选择器恒回基础段（不升级为紧急提示）。
        assert_eq!(
            summarizer.summarize_tool_results_section(&msgs, 1),
            SUMMARIZE_TOOL_RESULTS_SECTION
        );
        // summarize 原样返回。
        assert_eq!(summarizer.summarize_if_needed("R", "i", &big, 1), big);
        let _ = STALE_TURN_THRESHOLD;
    }
}
