//! 精确 Token 计数与四级回退链（Batch 0 P0-21）。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `engine/TokenCounter.java`：五个字符比常量、`estimateTokens(String)` 的
//!   精确层门控、`estimateTokensForModel` 的中文修正、`detectCharsPerToken` /
//!   `looksLikeCode` 的内容类型启发、`estimateTokens(List<Message>, String)`
//!   的消息路径公式；
//! - `engine/tokenizer/TokenizerService.java`：`POST /api/tokenizer/count`
//!   精确计数客户端（失败/空响应返回 `-1`，由调用方回退）；
//! - `python-service/src/routers/tokenizer.py`：侧车实现
//!   `tiktoken.get_encoding("cl100k_base")` + `len(enc.encode(text))`；
//! - `python-service/src/routers/token_estimator.py`：侧车 tiktoken 缺失时的
//!   字符兜底 `max(1, len(text) // 3)`；
//! - `config/FeatureFlagService`：`PRECISE_TOKENIZER` 出厂默认 `false`。
//!
//! # 四级回退链
//!
//! | 级 | 算法 | 触发条件 | 旧对照 |
//! |---|---|---|---|
//! | L1 | `tiktoken` `cl100k_base` 精确编码 | `PRECISE_TOKENIZER` 开启 **且** 编码器可用 | `TokenizerService.countExact` 返回 `>= 0` |
//! | L2 | 总字符 / 模型 `tokenCharRatio` | 模型已知（内置表或 `model.capabilities` 配置） | `ModelRegistry.getTokenCharRatio` |
//! | L3 | 总字符 / 3.5（文本路径按内容类型检测） | 模型名空白或未知（能力表落 `DEFAULT`） | `TokenCounter.DEFAULT_CHARS_PER_TOKEN` |
//! | L4 | `max(1, 总字符 / 3)` | 比率非有限或非正（防御级） | 侧车 `token_estimator` 的 `max(1, len // 3)` |
//!
//! 逐级触发条件与旧实现的对应关系：
//!
//! - **L1**：旧实现的精确层只挂在**单文本**入口 `estimateTokens(String)` 上，
//!   门控为 `tokenizerService != null && featureFlagService.isEnabled("PRECISE_TOKENIZER")`，
//!   且侧车返回负值（HTTP 失败 / 响应缺字段）时静默回退。本实现的等价门控是
//!   「flag 开启 **且** `cl100k_base()` 初始化成功」——本地进程内编码，无 HTTP
//!   失败面，故「侧车不可用」整类回退退化为「编码器构造失败」一条（见
//!   [`precise_encoder`]）。编码表与侧车逐字同源（`cl100k_base`），计数结果与
//!   Python `tiktoken` 逐 token 一致（黄金值断言见本模块单测）。
//! - **L2/L3**：旧实现的两条比率来自同一个 `ModelRegistry.getTokenCharRatio`：
//!   模型命中配置覆盖 / 内置表即 L2；空白或未命中落 `ModelCapabilities.DEFAULT`
//!   的 3.5 即 L3。**空白模型名**在旧实现另有分支：消息路径直接走
//!   `estimateTokens(messages)` 的常量 3.5，文本路径走 `detectCharsPerToken`
//!   的内容类型检测（JSON 2.0 / 中文加权 / 代码 3.5 / 缺省 3.5）——两者均归 L3，
//!   见 [`RatioSource::Detect`]。
//! - **L4**：旧 Java 侧无此级（`getTokenCharRatio` 恒返回合法比率）；`chars / 3`
//!   的字面来源是侧车 `token_estimator._count_tokens` 在 tiktoken 缺失时的兜底。
//!   本实现取其为**防御级**：比率非有限 / 非正时不得出现除零或 `NaN` 传染。
//!
//! # 消息路径的精确层（有意增强，留痕）
//!
//! 旧消息路径 `estimateTokens(List<Message>, String)` 无精确层——只有字符比。
//! 本实现在 L1 生效时对消息路径亦逐条精确编码（正文 + 工具名 + 工具入参），
//! 仍叠加旧 `size() * 4` 的消息边界开销，但**不再叠加** `ToolUseBlock` 的 +20 /
//! `ToolResultBlock` 的 +10 字符常量：这两个常量本就是「JSON 结构的字符级近似」，
//! 精确编码已覆盖同一部分，重复叠加会系统性高估。`PRECISE_TOKENIZER` 出厂
//! `false`，故默认路径与旧实现逐字一致；开启后为更准的上界估计。
//!
//! # 性能
//!
//! `cl100k_base` 词表内嵌于 crate，首次构造约数十毫秒，经 [`OnceLock`] 全进程
//! 共享一次；此后每次计数只做 BPE 编码，无锁、无分配复用问题。

use std::sync::{Arc, OnceLock};

use tiktoken_rs::CoreBPE;
use zk_core::FeatureFlags;
use zk_core::feature_flags::PRECISE_TOKENIZER;
use zk_llm::ChatMessage;

use super::{PER_MESSAGE_OVERHEAD_TOKENS, as_f64, char_count, message_chars, saturating_tokens};

/// 默认每 token 字符数（逐字对照旧 `TokenCounter.DEFAULT_CHARS_PER_TOKEN`）。
pub const DEFAULT_CHARS_PER_TOKEN: f64 = 3.5;

/// JSON 内容每 token 字符数（旧 `JSON_CHARS_PER_TOKEN`）。
pub const JSON_CHARS_PER_TOKEN: f64 = 2.0;

/// 代码内容每 token 字符数（旧 `CODE_CHARS_PER_TOKEN`）。
pub const CODE_CHARS_PER_TOKEN: f64 = 3.5;

/// 自然语言每 token 字符数（旧 `NATURAL_LANGUAGE_CHARS_PER_TOKEN`）。
pub const NATURAL_LANGUAGE_CHARS_PER_TOKEN: f64 = 4.0;

/// 中文内容每 token 字符数（旧 `CHINESE_CHARS_PER_TOKEN`）。
pub const CHINESE_CHARS_PER_TOKEN: f64 = 2.0;

/// L4 字符兜底除数（旧侧车 `token_estimator` 的 `len(text) // 3`）。
pub const CHAR_FALLBACK_DIVISOR: u64 = 3;

/// 中文占比判定阈值（旧 `chineseRatio > 0.3`）。
const CHINESE_RATIO_THRESHOLD: f64 = 0.3;

/// 中文修正系数（旧 `ratio * (1.0 - chineseRatio * 0.3)`）。
const CHINESE_ADJUST_FACTOR: f64 = 0.3;

/// 内容类型检测的最小长度（旧 `text.length() < 10 → DEFAULT`）。
const MIN_DETECT_CHARS: u64 = 10;

/// 代码特征检测采样长度（旧 `looksLikeCode` 的前 500 字符）。
const CODE_SAMPLE_CHARS: usize = 500;

/// 代码关键字集（顺序与旧 `looksLikeCode` 的 `contains` 链逐条一致）。
const CODE_KEYWORDS: [&str; 10] = [
    "import ",
    "function ",
    "class ",
    "def ",
    "public ",
    "private ",
    "const ",
    "let ",
    "var ",
    "return ",
];

/// 回退链落点（诊断与单测断言用；不参与旧行为语义）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenCountTier {
    /// L1：`tiktoken` `cl100k_base` 精确编码。
    Precise,
    /// L2：模型 `tokenCharRatio`。
    ModelRatio,
    /// L3：全局默认 3.5（含文本路径的内容类型检测）。
    GlobalRatio,
    /// L4：字符兜底 `max(1, chars / 3)`。
    CharFallback,
}

/// 非精确层的比率来源（由 [`resolve_char_ratio`] 判定；私有以便单测直接注入，
/// 从而令四级各自独立触发而不必伪造模型能力表）。
#[derive(Debug, Clone, Copy, PartialEq)]
enum RatioSource {
    /// L2 / L3：确定比率 + 落点。
    Ratio(f64, TokenCountTier),
    /// L3：模型名空白——文本路径按内容类型检测（旧 `detectCharsPerToken`），
    /// 消息路径取常量 3.5（旧 `estimateTokens(List<Message>)`）。
    Detect,
    /// L4：比率不可用（非有限 / 非正）。
    CharFallback,
}

/// `cl100k_base` 编码器（全进程一次构造；构造失败即整条 L1 不可用）。
///
/// 旧实现此处是 HTTP 客户端 + 侧车进程；本实现为进程内纯 Rust 编码，故
/// 「侧车未启动 / 超时 / 返回 `-1`」整类回退在此收敛为 `None` 一条。
fn precise_encoder() -> Option<&'static CoreBPE> {
    static ENCODER: OnceLock<Option<CoreBPE>> = OnceLock::new();
    ENCODER
        .get_or_init(|| match tiktoken_rs::cl100k_base() {
            Ok(encoder) => Some(encoder),
            Err(error) => {
                tracing::error!(
                    error = %error,
                    "cl100k_base encoder unavailable; precise token counting disabled"
                );
                None
            }
        })
        .as_ref()
}

/// 全局特性开关句柄（对照旧 Spring 单例 `FeatureFlagService`）。
static FEATURE_FLAGS: OnceLock<Arc<FeatureFlags>> = OnceLock::new();

/// 装配全局特性开关（zk-server 启动期以 `AppState` 的同一 `Arc` 调用一次，
/// 使运行时改写的 flag 对本模块即时可见）。
///
/// # Errors
///
/// 已装配（含已被首次计数惰性初始化）时把入参交回 `Err`，语义同 `OnceLock::set`。
pub fn install_feature_flags(flags: Arc<FeatureFlags>) -> Result<(), Arc<FeatureFlags>> {
    FEATURE_FLAGS.set(flags)
}

/// 生效的特性开关（未显式装配时惰性 [`FeatureFlags::from_env`]，与 zk-server
/// 的装配方式同源：出厂默认 + `ZK_FEATURE_` / `FEATURE_` 环境覆盖）。
fn feature_flags() -> &'static FeatureFlags {
    FEATURE_FLAGS
        .get_or_init(|| Arc::new(FeatureFlags::from_env()))
        .as_ref()
}

/// L1 是否生效（flag 开启 **且** 编码器可用；对照旧
/// `tokenizerService != null && featureFlagService.isEnabled("PRECISE_TOKENIZER")`）。
#[must_use]
pub fn precise_enabled() -> bool {
    active_encoder().is_some()
}

/// 门控后的编码器（flag 关闭即 `None`，与出厂默认一致）。
fn encoder_for(flags: &FeatureFlags) -> Option<&'static CoreBPE> {
    if flags.is_enabled(PRECISE_TOKENIZER) {
        precise_encoder()
    } else {
        None
    }
}

/// 生效编码器（走全局 flag 句柄）。
fn active_encoder() -> Option<&'static CoreBPE> {
    encoder_for(feature_flags())
}

/// 消息列表 token 数（四级回退链入口；旧
/// `TokenCounter.estimateTokens(List<Message>, String modelId)` 的等价式）。
#[must_use]
pub fn count(messages: &[ChatMessage], model: &str) -> u32 {
    count_with_tier(messages, model).0
}

/// 同 [`count`]，并回报落在哪一级（诊断 / 单测）。
#[must_use]
pub fn count_with_tier(messages: &[ChatMessage], model: &str) -> (u32, TokenCountTier) {
    count_messages_core(messages, active_encoder(), resolve_char_ratio(model))
}

/// 单文本 token 数（四级回退链入口；旧 `estimateTokens(String)` 与
/// `estimateTokensForModel(String, String)` 的合并等价式——模型名空白走前者的
/// 内容类型检测，非空走后者的模型比率 + 中文修正）。
#[must_use]
pub fn count_text(text: &str, model: &str) -> u32 {
    count_text_with_tier(text, model).0
}

/// 同 [`count_text`]，并回报落在哪一级（诊断 / 单测）。
#[must_use]
pub fn count_text_with_tier(text: &str, model: &str) -> (u32, TokenCountTier) {
    count_text_core(text, active_encoder(), resolve_char_ratio(model))
}

/// 比率解析（L2 → L3 → L4 的判定；对照旧 `ModelRegistry.getTokenCharRatio`
/// 与两个 `modelId == null || isBlank` 分支）。
fn resolve_char_ratio(model: &str) -> RatioSource {
    if model.trim().is_empty() {
        return RatioSource::Detect;
    }
    let ratio = zk_llm::capabilities_for(model).token_char_ratio;
    if !ratio.is_finite() || ratio <= 0.0 {
        return RatioSource::CharFallback;
    }
    let tier = if zk_llm::is_known_model(model) {
        TokenCountTier::ModelRatio
    } else {
        TokenCountTier::GlobalRatio
    };
    RatioSource::Ratio(ratio, tier)
}

/// 消息路径核心（`precise` / `ratio` 由调用方注入，令四级独立可测）。
fn count_messages_core(
    messages: &[ChatMessage],
    precise: Option<&CoreBPE>,
    ratio: RatioSource,
) -> (u32, TokenCountTier) {
    if messages.is_empty() {
        // 旧 `messages == null || messages.isEmpty()` 分支恒 0。
        return (0, tier_of(precise, ratio));
    }
    let count = u64::try_from(messages.len()).unwrap_or(u64::MAX);
    let overhead = count.saturating_mul(PER_MESSAGE_OVERHEAD_TOKENS);

    if let Some(encoder) = precise {
        // L1：逐条精确编码正文 + 工具名 + 工具入参（见模块文档「消息路径的精确层」）。
        let mut tokens: u64 = 0;
        for message in messages {
            tokens = tokens.saturating_add(encoded_len(encoder, &message.content));
            for call in &message.tool_calls {
                tokens = tokens
                    .saturating_add(encoded_len(encoder, &call.name))
                    .saturating_add(encoded_len(encoder, &call.arguments));
            }
        }
        return (
            u32::try_from(tokens.saturating_add(overhead)).unwrap_or(u32::MAX),
            TokenCountTier::Precise,
        );
    }

    let total_chars: u64 = messages.iter().map(message_chars).sum();
    match ratio {
        // 旧消息路径无内容类型检测：空白模型名直接取常量 3.5。
        RatioSource::Detect => (
            saturating_tokens(as_f64(total_chars) / DEFAULT_CHARS_PER_TOKEN + as_f64(overhead)),
            TokenCountTier::GlobalRatio,
        ),
        RatioSource::Ratio(ratio, tier) => (
            saturating_tokens(as_f64(total_chars) / ratio + as_f64(overhead)),
            tier,
        ),
        RatioSource::CharFallback => (
            saturating_tokens(as_f64(char_fallback_tokens(total_chars)) + as_f64(overhead)),
            TokenCountTier::CharFallback,
        ),
    }
}

/// 文本路径核心（`precise` / `ratio` 由调用方注入，令四级独立可测）。
fn count_text_core(
    text: &str,
    precise: Option<&CoreBPE>,
    ratio: RatioSource,
) -> (u32, TokenCountTier) {
    if let Some(encoder) = precise {
        // L1：旧 `estimateTokens(String)` 的精确分支（侧车 `len(enc.encode(text))`）。
        return (
            u32::try_from(encoded_len(encoder, text)).unwrap_or(u32::MAX),
            TokenCountTier::Precise,
        );
    }
    let chars = char_count(text);
    if chars == 0 {
        // 旧 `text == null || text.isEmpty()` 分支恒 0。
        return (0, tier_of(precise, ratio));
    }
    match ratio {
        // L3：旧 `estimateTokens(String)` 的 `detectCharsPerToken` 分支。
        RatioSource::Detect => (
            truncating_tokens(as_f64(chars) / detect_chars_per_token(text, chars)),
            TokenCountTier::GlobalRatio,
        ),
        // L2 / L3：旧 `estimateTokensForModel` 的模型比率 + 中文修正。
        RatioSource::Ratio(ratio, tier) => {
            let chinese_ratio = as_f64(han_count(text)) / as_f64(chars);
            let adjusted = if chinese_ratio > CHINESE_RATIO_THRESHOLD {
                ratio * (1.0 - chinese_ratio * CHINESE_ADJUST_FACTOR)
            } else {
                ratio
            };
            (truncating_tokens(as_f64(chars) / adjusted), tier)
        }
        // L4：侧车字符兜底 `max(1, len // 3)`。
        RatioSource::CharFallback => (
            u32::try_from(char_fallback_tokens(chars)).unwrap_or(u32::MAX),
            TokenCountTier::CharFallback,
        ),
    }
}

/// 空输入（0 token）时对外回报的落点。
fn tier_of(precise: Option<&CoreBPE>, ratio: RatioSource) -> TokenCountTier {
    if precise.is_some() {
        return TokenCountTier::Precise;
    }
    match ratio {
        RatioSource::Ratio(_, tier) => tier,
        RatioSource::Detect => TokenCountTier::GlobalRatio,
        RatioSource::CharFallback => TokenCountTier::CharFallback,
    }
}

/// 精确编码后的 token 数（`encode_ordinary` 不解释特殊 token，等价于侧车
/// `enc.encode(text)` 对普通文本的结果——特殊 token 串在侧车会抛异常并被
/// `countExact` 转成 `-1` 回退，本实现按普通字节序列计数，不引入失败面）。
fn encoded_len(encoder: &CoreBPE, text: &str) -> u64 {
    u64::try_from(encoder.encode_ordinary(text).len()).unwrap_or(u64::MAX)
}

/// L4 字符兜底（旧侧车 `max(1, len(text) // 3)`）。
fn char_fallback_tokens(chars: u64) -> u64 {
    (chars / CHAR_FALLBACK_DIVISOR).max(1)
}

/// 内容类型 → 字符比（逐条对照旧 `TokenCounter.detectCharsPerToken`）。
fn detect_chars_per_token(text: &str, chars: u64) -> f64 {
    if chars < MIN_DETECT_CHARS {
        return DEFAULT_CHARS_PER_TOKEN;
    }
    let trimmed = text.trim();
    if (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
    {
        return JSON_CHARS_PER_TOKEN;
    }
    let chinese_ratio = as_f64(han_count(text)) / as_f64(chars);
    if chinese_ratio > CHINESE_RATIO_THRESHOLD {
        // 混合内容按中文比例加权（旧公式，非中文修正公式）。
        return CHINESE_CHARS_PER_TOKEN * chinese_ratio
            + DEFAULT_CHARS_PER_TOKEN * (1.0 - chinese_ratio);
    }
    if looks_like_code(trimmed) {
        return CODE_CHARS_PER_TOKEN;
    }
    DEFAULT_CHARS_PER_TOKEN
}

/// 带内容类型提示的字符比（旧 `estimateTokens(String text, String contentType)`
/// 的 `switch`；`contentType` 大小写不敏感，未识别值回落自动检测）。
#[must_use]
pub fn chars_per_token_for_content_type(text: &str, content_type: &str) -> f64 {
    match content_type.to_ascii_lowercase().as_str() {
        "json" => JSON_CHARS_PER_TOKEN,
        "code" | "java" | "python" | "javascript" | "typescript" => CODE_CHARS_PER_TOKEN,
        "text" | "markdown" => NATURAL_LANGUAGE_CHARS_PER_TOKEN,
        _ => detect_chars_per_token(text, char_count(text)),
    }
}

/// 代码特征启发（逐条对照旧 `TokenCounter.looksLikeCode`：三类指标计数 >= 2）。
///
/// 一处实现差异：旧 `String.lines()` 亦把孤立 `\r` 视作行终止符，Rust
/// [`str::lines`] 只切 `\n` / `\r\n`——仅影响老式 Mac 换行文本的缩进行计数，
/// 对该启发的三值判定无实际影响。
fn looks_like_code(text: &str) -> bool {
    let sample: String = text.chars().take(CODE_SAMPLE_CHARS).collect();
    let mut indicators = 0_u32;

    // 花括号与分号密度。
    let braces = sample.chars().filter(|ch| *ch == '{' || *ch == '}').count();
    let semicolons = sample.chars().filter(|ch| *ch == ';').count();
    if braces > 2 || semicolons > 3 {
        indicators += 1;
    }
    // 常见代码关键字。
    if CODE_KEYWORDS.iter().any(|keyword| sample.contains(keyword)) {
        indicators += 1;
    }
    // 缩进模式（4 空格或 Tab 起头行）。
    let indented = sample
        .lines()
        .filter(|line| line.starts_with("    ") || line.starts_with('\t'))
        .count();
    if indented > 3 {
        indicators += 1;
    }
    indicators >= 2
}

/// 汉字（Han script）字符数（旧 `Character.UnicodeScript.of(cp) == HAN` 的等价式）。
fn han_count(text: &str) -> u64 {
    u64::try_from(text.chars().filter(|ch| is_han(*ch)).count()).unwrap_or(u64::MAX)
}

/// 码位是否属 Han script。
///
/// 旧实现依赖 JDK `Character.UnicodeScript`（无第三方依赖可用），此处按 Unicode
/// Han script 区间逐段判定：CJK 部首补充 / 康熙部首 / 表意文字符号中的 Han 项 /
/// 扩展 A / 基本区 / 兼容表意文字 / 扩展 B-H。**中日韩标点**（`，` `。` 等）在
/// Unicode 中属 Common script，旧实现同样不计入。
fn is_han(ch: char) -> bool {
    matches!(u32::from(ch),
        0x2E80..=0x2E99
        | 0x2E9B..=0x2EF3
        | 0x2F00..=0x2FD5
        | 0x3005
        | 0x3007
        | 0x3021..=0x3029
        | 0x3038..=0x303B
        | 0x3400..=0x4DBF
        | 0x4E00..=0x9FFF
        | 0xF900..=0xFA6D
        | 0xFA70..=0xFAD9
        | 0x16FE2..=0x16FE3
        | 0x16FF0..=0x16FF1
        | 0x20000..=0x2A6DF
        | 0x2A700..=0x2EBEF
        | 0x2F800..=0x2FA1D
        | 0x30000..=0x3134A
        | 0x31350..=0x323AF)
}

/// 向零截断并饱和到 [`u32::MAX`]（旧文本路径的 `(int) (length / ratio)`——
/// 注意与消息路径 `saturatingTokens` 的**向上取整**不同，两者均逐字保留）。
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "上界由 f64::from(u32::MAX) 比较夹紧、下界由 <= 0 分支拦截，转换无溢出"
)]
fn truncating_tokens(value: f64) -> u32 {
    if !value.is_finite() || value >= f64::from(u32::MAX) {
        return u32::MAX;
    }
    if value <= 0.0 {
        return 0;
    }
    value.trunc() as u32
}

#[cfg(test)]
mod tests {
    use super::{
        CHAR_FALLBACK_DIVISOR, DEFAULT_CHARS_PER_TOKEN, JSON_CHARS_PER_TOKEN, RatioSource,
        TokenCountTier, as_f64, char_fallback_tokens, chars_per_token_for_content_type, count,
        count_messages_core, count_text, count_text_core, detect_chars_per_token, encoder_for,
        han_count, looks_like_code, precise_encoder, resolve_char_ratio, truncating_tokens,
    };
    use zk_core::FeatureFlags;
    use zk_core::feature_flags::PRECISE_TOKENIZER;
    use zk_llm::{ChatMessage, ToolCallRequest};

    /// 与 Python `tiktoken` 交叉验证用的黄金值。
    ///
    /// 生成方式（一次性离线执行，不在测试中 spawn 进程）：用旧仓库侧车自带的
    /// 虚拟环境 `python-service/.venv/bin/python`（`tiktoken` 0.12.0）跑
    /// `enc = tiktoken.get_encoding("cl100k_base")`，对下列同一批字符串取
    /// `len(enc.encode(text))`；另以 `encode_ordinary` 复算一遍，两者结果完全
    /// 相同（这批文本不含特殊 token），故与 tiktoken-rs 的 `encode_ordinary`
    /// 可直接逐值比对。验收要求偏差 <5%，实测为**逐 token 精确相等**（0%）。
    const GOLDEN: &[(&str, &str, u32)] = &[
        (
            "pure_ascii_prose",
            "The quick brown fox jumps over the lazy dog near the riverbank at dawn.",
            16,
        ),
        (
            "pure_chinese",
            "上下文级联压缩的全部决策都建立在精确的令牌计数之上。",
            32,
        ),
        (
            "mixed_cn_en",
            "ZhikunCode 的 ContextCascade 六层压缩：Snip / MicroCompact / ContextCollapse 每轮无条件执行。",
            33,
        ),
        (
            "rust_code_block",
            "pub fn count(text: &str, model: &str) -> u32 {\n    let bpe = precise_encoder()?;\n    bpe.encode_ordinary(text).len() as u32\n}\n",
            40,
        ),
        (
            "json_payload",
            "{\"model\":\"kimi-k3\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"stream\":true}",
            23,
        ),
        ("emoji_only", "🚀🔥✅🎯🧠", 14),
        (
            "emoji_mixed_cn",
            "压缩完成 ✅ 节省 12000 tokens 🚀 继续推理中……",
            26,
        ),
        (
            "tool_result_snippet",
            "[tool_result] Read src/main.rs → 42 lines, 1.2KB, mtime=2026-08-18T09:00:00Z",
            35,
        ),
        (
            "markdown_doc",
            "# 标题\n\n- 第一项 first item\n- 第二项 second item\n\n> 引用 blockquote `inline_code()`\n",
            30,
        ),
    ];

    #[test]
    fn precise_layer_matches_python_tiktoken_golden_values() {
        let encoder = precise_encoder().expect("cl100k_base encoder");
        for (name, text, golden) in GOLDEN {
            let (tokens, tier) = count_text_core(
                text,
                Some(encoder),
                RatioSource::Ratio(3.5, TokenCountTier::ModelRatio),
            );
            assert_eq!(tier, TokenCountTier::Precise, "{name}");
            assert_eq!(tokens, *golden, "{name}: tiktoken-rs != Python tiktoken");
        }
        // 长重复 CJK（"令牌" × 64）单列：证明重复串上编码亦无漂移。
        let repeated = "令牌".repeat(64);
        let (tokens, _) = count_text_core(
            &repeated,
            Some(encoder),
            RatioSource::Ratio(3.5, TokenCountTier::ModelRatio),
        );
        assert_eq!(tokens, 256);
    }

    #[test]
    fn golden_values_stay_within_five_percent_band() {
        // 验收判据的显式表述：逐条偏差 < 5%（实测 0%，此断言守住回归带宽）。
        let encoder = precise_encoder().expect("cl100k_base encoder");
        for (name, text, golden) in GOLDEN {
            let (tokens, _) = count_text_core(text, Some(encoder), RatioSource::Detect);
            let deviation = (f64::from(tokens) - f64::from(*golden)).abs() / f64::from(*golden);
            assert!(deviation < 0.05, "{name}: deviation {deviation}");
        }
    }

    #[test]
    fn flag_gating_switches_precise_layer_off_and_on() {
        // 出厂默认 false（对照旧 application.yml `features.flags.PRECISE_TOKENIZER: false`）。
        let off = FeatureFlags::with_defaults();
        assert!(!off.is_enabled(PRECISE_TOKENIZER));
        assert!(encoder_for(&off).is_none());

        // 环境覆盖开启（`std::env::set_var` 在 unsafe_code = "forbid" 下不可用，
        // 故走 zk-core 的注入入口——与进程环境覆盖同一解析路径）。
        let on = FeatureFlags::with_env_overrides([("ZK_FEATURE_PRECISE_TOKENIZER", "true")]);
        assert!(on.is_enabled(PRECISE_TOKENIZER));
        assert!(encoder_for(&on).is_some());

        // 旧 `Boolean.parseBoolean` 语义：非 "true" 一律为假，精确层不激活。
        let bogus = FeatureFlags::with_env_overrides([("ZK_FEATURE_PRECISE_TOKENIZER", "1")]);
        assert!(encoder_for(&bogus).is_none());
    }

    #[test]
    fn tier_l1_precise_covers_messages_and_drops_structural_char_constants() {
        let encoder = precise_encoder().expect("cl100k_base encoder");
        let messages = vec![
            ChatMessage::user("hello world"),
            ChatMessage::tool("t1", "ok"),
        ];
        let (tokens, tier) = count_messages_core(
            &messages,
            Some(encoder),
            RatioSource::Ratio(3.5, TokenCountTier::ModelRatio),
        );
        assert_eq!(tier, TokenCountTier::Precise);
        // 正文精确编码之和 + 每条 4 token 边界开销（不含 +20 / +10 字符常量）。
        let expected = encoder.encode_ordinary("hello world").len()
            + encoder.encode_ordinary("ok").len()
            + 2 * 4;
        assert_eq!(u64::from(tokens), expected as u64);
    }

    #[test]
    fn tier_l1_precise_includes_tool_call_name_and_arguments() {
        let encoder = precise_encoder().expect("cl100k_base encoder");
        let assistant = ChatMessage::assistant_tool_calls(
            "run it",
            vec![ToolCallRequest {
                id: "t1".into(),
                name: "Read".into(),
                arguments: r#"{"file_path":"/tmp/a.rs"}"#.into(),
            }],
        );
        let (tokens, _) = count_messages_core(
            std::slice::from_ref(&assistant),
            Some(encoder),
            RatioSource::Detect,
        );
        let expected = encoder.encode_ordinary("run it").len()
            + encoder.encode_ordinary("Read").len()
            + encoder
                .encode_ordinary(r#"{"file_path":"/tmp/a.rs"}"#)
                .len()
            + 4;
        assert_eq!(u64::from(tokens), expected as u64);
    }

    #[test]
    fn tier_l2_model_ratio_reproduces_legacy_message_formula() {
        // 精确层不可用（flag off）→ 落 L2：ceil(700/3.5 + 2×4) = 208（旧公式）。
        let messages = vec![
            ChatMessage::user("x".repeat(350)),
            ChatMessage::assistant("y".repeat(350)),
        ];
        let (tokens, tier) = count_messages_core(
            &messages,
            None,
            RatioSource::Ratio(DEFAULT_CHARS_PER_TOKEN, TokenCountTier::ModelRatio),
        );
        assert_eq!(tier, TokenCountTier::ModelRatio);
        assert_eq!(tokens, 208);
        // 配置/内置表给出非 3.5 比率时按该比率计（此处 2.5：ceil(700/2.5 + 8) = 288）。
        let (tokens, _) = count_messages_core(
            &messages,
            None,
            RatioSource::Ratio(2.5, TokenCountTier::ModelRatio),
        );
        assert_eq!(tokens, 288);
        // 已知模型走 L2（kimi-k3 在内置表内）。
        assert_eq!(
            resolve_char_ratio("kimi-k3"),
            RatioSource::Ratio(3.5, TokenCountTier::ModelRatio)
        );
        // 端到端（flag 出厂 off）：公开入口与旧公式一致。
        assert_eq!(count(&messages, "kimi-k3"), 208);
    }

    #[test]
    fn tier_l3_global_ratio_covers_blank_and_unknown_models() {
        // 未知模型：能力表落 DEFAULT 的 3.5 → L3。
        assert_eq!(
            resolve_char_ratio("totally-unknown-model"),
            RatioSource::Ratio(3.5, TokenCountTier::GlobalRatio)
        );
        // 空白模型名：旧实现另走一条分支（消息路径常量 3.5，文本路径内容检测）。
        for blank in ["", "   "] {
            assert_eq!(resolve_char_ratio(blank), RatioSource::Detect);
        }
        let messages = vec![
            ChatMessage::user("x".repeat(350)),
            ChatMessage::assistant("y".repeat(350)),
        ];
        let (tokens, tier) = count_messages_core(&messages, None, RatioSource::Detect);
        assert_eq!(tier, TokenCountTier::GlobalRatio);
        assert_eq!(tokens, 208);
        assert_eq!(count(&messages, "totally-unknown-model"), 208);
    }

    #[test]
    fn tier_l4_char_fallback_guards_unusable_ratios() {
        // 比率不可用（非有限 / 非正）→ max(1, chars/3)（旧侧车 token_estimator 兜底）。
        let messages = vec![ChatMessage::user("x".repeat(300))];
        let (tokens, tier) = count_messages_core(&messages, None, RatioSource::CharFallback);
        assert_eq!(tier, TokenCountTier::CharFallback);
        assert_eq!(tokens, 300 / 3 + 4);
        let (tokens, tier) = count_text_core(&"x".repeat(300), None, RatioSource::CharFallback);
        assert_eq!(tier, TokenCountTier::CharFallback);
        assert_eq!(tokens, 100);
        // 下限 1（旧 `max(1, len // 3)`）。
        assert_eq!(char_fallback_tokens(0), 1);
        assert_eq!(char_fallback_tokens(2), 1);
        assert_eq!(char_fallback_tokens(3), 1);
        assert_eq!(char_fallback_tokens(9), 9 / CHAR_FALLBACK_DIVISOR);
    }

    #[test]
    fn empty_inputs_are_zero_on_every_tier() {
        for ratio in [
            RatioSource::Ratio(3.5, TokenCountTier::ModelRatio),
            RatioSource::Detect,
            RatioSource::CharFallback,
        ] {
            assert_eq!(count_messages_core(&[], None, ratio).0, 0);
            assert_eq!(count_text_core("", None, ratio).0, 0);
        }
        let encoder = precise_encoder().expect("cl100k_base encoder");
        assert_eq!(
            count_messages_core(&[], Some(encoder), RatioSource::Detect).0,
            0
        );
        assert_eq!(count_text_core("", Some(encoder), RatioSource::Detect).0, 0);
    }

    #[test]
    fn text_path_applies_chinese_correction_like_legacy() {
        // 旧 estimateTokensForModel：中文占比 > 0.3 时 ratio × (1 - chineseRatio × 0.3)。
        // 全中文 26 字（含句号，句号属 Common 不计入 Han）→ chineseRatio = 25/26。
        let text = "上下文级联压缩的全部决策都建立在精确的令牌计数之上。";
        let chars = u64::try_from(text.chars().count()).expect("char count");
        assert_eq!(han_count(text), chars - 1);
        let chinese_ratio = as_f64(chars - 1) / as_f64(chars);
        let adjusted = 3.5 * (1.0 - chinese_ratio * 0.3);
        let expected = truncating_tokens(as_f64(chars) / adjusted);
        let (tokens, tier) = count_text_core(
            text,
            None,
            RatioSource::Ratio(3.5, TokenCountTier::ModelRatio),
        );
        assert_eq!(tier, TokenCountTier::ModelRatio);
        assert_eq!(tokens, expected);
        // 纯英文（中文占比 0）不修正：截断除法，非向上取整。
        let ascii = "abcdefghij"; // 10 字符 / 3.5 = 2.857 → 2
        let (tokens, _) = count_text_core(
            ascii,
            None,
            RatioSource::Ratio(3.5, TokenCountTier::ModelRatio),
        );
        assert_eq!(tokens, 2);
    }

    #[test]
    fn text_path_detects_content_type_like_legacy() {
        // 短文本（< 10 字符）恒取 3.5。
        assert!((detect_chars_per_token("abc", 3) - DEFAULT_CHARS_PER_TOKEN).abs() < f64::EPSILON);
        // JSON（首尾成对）→ 2.0。
        let json = r#"{"a":1,"b":[2,3]}"#;
        let ratio = detect_chars_per_token(
            json,
            u64::try_from(json.chars().count()).expect("char count"),
        );
        assert!((ratio - JSON_CHARS_PER_TOKEN).abs() < f64::EPSILON);
        // 中文加权（2.0 × r + 3.5 × (1 - r)），与模型比率修正是两套不同公式。
        let cn = "全中文文本用于验证加权公式";
        let chars = u64::try_from(cn.chars().count()).expect("char count");
        let r = as_f64(han_count(cn)) / as_f64(chars);
        let expected = 2.0 * r + 3.5 * (1.0 - r);
        assert!((detect_chars_per_token(cn, chars) - expected).abs() < 1e-12);
        // 代码特征（花括号密度 + 关键字 + 缩进行）→ 3.5。
        let code =
            "pub fn main() {\n    let x = 1;\n    let y = 2;\n    let z = 3;\n    return x;\n}\n";
        assert!(looks_like_code(code));
        // 单一指标不足以判定为代码（仅关键字，无花括号密度/缩进）。
        assert!(!looks_like_code("please return the answer"));
        // 内容类型提示（旧 estimateTokens(text, contentType) 的 switch）。
        assert!((chars_per_token_for_content_type("x", "JSON") - 2.0).abs() < f64::EPSILON);
        assert!((chars_per_token_for_content_type("x", "typescript") - 3.5).abs() < f64::EPSILON);
        assert!((chars_per_token_for_content_type("x", "markdown") - 4.0).abs() < f64::EPSILON);
        // 未识别类型回落自动检测（此处短文本 → 3.5）。
        assert!((chars_per_token_for_content_type("x", "yaml") - 3.5).abs() < f64::EPSILON);
    }

    #[test]
    fn text_path_public_entry_defaults_to_flag_off_behaviour() {
        // 公开入口（flag 出厂 off）：走字符比而非精确层。
        let text = "abcdefghij";
        assert_eq!(count_text(text, "kimi-k3"), 2);
        assert_eq!(count_text("", "kimi-k3"), 0);
    }

    #[test]
    fn truncating_tokens_floors_and_clamps() {
        assert_eq!(truncating_tokens(2.99), 2);
        assert_eq!(truncating_tokens(0.9), 0);
        assert_eq!(truncating_tokens(-1.0), 0);
        assert_eq!(truncating_tokens(f64::NAN), u32::MAX);
        assert_eq!(truncating_tokens(f64::INFINITY), u32::MAX);
    }

    #[test]
    fn han_detection_counts_ideographs_only() {
        // 汉字计入；中日韩标点、假名、拉丁、emoji 不计入（Common / 其他 script）。
        assert_eq!(han_count("汉字"), 2);
        assert_eq!(han_count("，。！？"), 0);
        assert_eq!(han_count("ひらがな"), 0);
        assert_eq!(han_count("abc123"), 0);
        assert_eq!(han_count("🚀"), 0);
        // 扩展 B（U+20000 起）亦属 Han。
        assert_eq!(han_count("\u{20000}\u{2A6DF}"), 2);
    }
}
