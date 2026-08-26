//! 图片 / Base64 上下文预算守卫（旧 `engine/TokenBudgetGuard.java` 的等价移植）。
//!
//! 建模来源（旧仓库只读权威规格）：`backend/src/main/java/com/aicodeassistant/
//! engine/TokenBudgetGuard.java`（623 行）；调用点为 `QueryEngine.queryLoop()`
//! Step 3——Phase 1 在消息进入 API 转换前执行、Phase 2 在最终 payload 组装后
//! 执行。
//!
//! # 两阶段
//!
//! | 阶段 | 触发 | 作用 | 旧对照 |
//! |---|---|---|---|
//! | Phase 1 | 估算 token > `inputBudget` | 清理历史消息中的 Base64 载荷（两遍：先保护尾部 2 条，仍超限则只保护尾部 1 条） | `enforcePhase1` |
//! | Phase 2 | 最终 payload 估算 > `inputBudget` | 三级梯度降级：① 缩略图替换（640px / q=0.6）→ ② 图片换文本 → ③ 移除全部注入图片 | `enforcePhase2` |
//!
//! # 常量逐条对照（禁止臆造，全部取自旧源）
//!
//! | 本模块 | 旧常量 | 值 |
//! |---|---|---|
//! | [`TEXT_TOKEN_RATIO`] | `TEXT_TOKEN_RATIO` | `3.5` |
//! | [`BASE64_LENGTH_THRESHOLD`] | `BASE64_LENGTH_THRESHOLD` | `40_000` |
//! | [`MAX_DECODED_IMAGE_BYTES`] | `MAX_DECODED_IMAGE_BYTES` | `10 * 1024 * 1024` |
//! | [`MAX_IMAGE_PIXELS`] | `MAX_IMAGE_PIXELS` | `40_000_000` |
//! | [`BASE64_PREFIXES`] | `BASE64_PREFIXES` | `/9j/` `iVBOR` `R0lGOD` `UklGR` `PHN2` |
//! | [`THUMBNAIL_MAX_DIM`] | `replaceImagesWithThumbnails(.., 640, ..)` | `640` |
//! | [`THUMBNAIL_QUALITY`] | `setCompressionQuality(0.6f)` | `60`（见下「质量档映射」） |
//!
//! # 载体适配（旧 `Message` / `ContentBlock` → 新 [`ChatMessage`]）
//!
//! 旧 Phase 1 逐块处理 `ImageBlock` / `TextBlock` / `ToolResultBlock`；Rust 侧
//! [`ChatMessage`] 为纯文本（`D-S9-7`），媒体以内嵌
//! `data:<mime>;base64,<payload>` 形式出现（沿用 [`crate::recovery`] 已确立的
//! 同一载体约定）。故映射为：
//!
//! - 旧 `TextBlock` / `ToolResultBlock` 的**整段 Base64** 判定
//!   （`isBase64Content(text)`）→ 整条正文换占位；
//! - 旧 `ImageBlock.base64Data` → 正文内的 data URI 片段换占位，占位里的
//!   字符数取 **载荷** 长度（对应旧 `img.base64Data().length()`）；
//! - 旧 `SystemMessage` 不清理 → [`Role::System`] 原样透传；
//! - 旧 `ToolUseBlock` 不清理 → [`ChatMessage::tool_calls`] 原样透传。
//!
//! Phase 2 的旧载体是已转成 API 格式的 `List<Map<String, Object>>`，Rust 侧
//! 直接对应 `Vec<serde_json::Value>`（`{"type":"image","source":{"data":..}}`
//! 块结构逐字保留），故 [`enforce_phase2`] 的入参与旧 `apiMessages` 同形。
//!
//! # 质量档映射（唯一的语义换算，非臆造）
//!
//! 旧 `ImageWriteParam.setCompressionQuality(0.6f)` 取值域为 `[0.0, 1.0]`，JVM
//! JPEG 写出器将其线性映射到 libjpeg 的 `[0, 100]` 质量档；`image` crate 的
//! [`JpegEncoder::new_with_quality`] 直接吃 `[1, 100]`，故 `0.6f → 60`。
//!
//! # 去重指纹
//!
//! 旧 `hashImageBlock` = `Base64` 解码后取 `MessageDigest("SHA-256")` 再
//! `HexFormat.of().formatHex`（小写 hex 64 字符），`retainedImageHashes` 为
//! `Set<String>`。本模块逐字沿用（`sha2` + 小写 hex），**未**改用 xxhash——
//! 该集合会跨轮回传给 `ImageRefInjector` 的 `confirmedImageHashes`，哈希口径
//! 必须与旧实现逐字节一致，否则跨版本会话的图片去重状态互不认账。
//!
//! # Feature gate
//!
//! `image-budget` 关闭时：[`apply`]（级联热路径插桩点）空转返回 `false`、
//! Phase 2 降级链不编译——最小构建与 CI 无需 `image` crate 及其解码器传递
//! 依赖。Phase 1 为纯字符操作，无条件可用。
//!
//! # 未移植项（留痕）
//!
//! - `ImageRefInjector` / `ContextAttachmentResolver`（`@图片` 引用解析与临时
//!   注入）属 P0-13 附件上传批次，本步只交付预算守卫本体；[`collect_image_hashes`]
//!   与 [`image_block_hash`] 已按旧口径公开，供其后续复用；
//! - `estimateApiTokens` 对 `tool_use` / `thinking` 等「其他类型」块的估算，旧实现
//!   取 `Map.toString()` 的长度（`{k=v, k2=v2}` 的 JVM 专有格式）；本实现取紧凑
//!   JSON 序列化长度——二者都是粗略代理，无稳定字符级对应关系可抄。

use std::collections::HashSet;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
// `Map` 仅用于 Phase 2 的块重建（`text_block`），随 feature 一同门控。
#[cfg(feature = "image-budget")]
use serde_json::Map;
use serde_json::Value;
use sha2::{Digest, Sha256};
use zk_llm::{ChatMessage, Role};

// ==================== 常量（逐字对照旧常量池） ====================

/// 文本 token 估算的缺省字符/token 比（旧 `TEXT_TOKEN_RATIO`）。
pub const TEXT_TOKEN_RATIO: f64 = 3.5;

/// Base64 判定的长度阈值（旧 `BASE64_LENGTH_THRESHOLD`）：超过此长度才做字符集
/// 采样判定；未超过且无魔数前缀则一律不视为 Base64。
pub const BASE64_LENGTH_THRESHOLD: usize = 40_000;

/// 解码后字节数闸门（旧 `MAX_DECODED_IMAGE_BYTES`）：超过即安全省略，不解码。
pub const MAX_DECODED_IMAGE_BYTES: usize = 10 * 1024 * 1024;

/// 像素数闸门（旧 `MAX_IMAGE_PIXELS`）：由**只读头部**取到的宽高相乘判定，
/// 超过即安全省略——避免超大图全解码时 OOM。
pub const MAX_IMAGE_PIXELS: u64 = 40_000_000;

/// 五种图片 Base64 魔数前缀（旧 `BASE64_PREFIXES`，顺序一致）：
/// JPEG / PNG / GIF / WebP / SVG。
pub const BASE64_PREFIXES: [&str; 5] = ["/9j/", "iVBOR", "R0lGOD", "UklGR", "PHN2"];

/// 缩略图最长边（旧 `replaceImagesWithThumbnails(current, 640, 0.6f, ..)`）。
pub const THUMBNAIL_MAX_DIM: u32 = 640;

/// 缩略图 JPEG 质量档（旧 `0.6f` × 100，见模块文档「质量档映射」）。
pub const THUMBNAIL_QUALITY: u8 = 60;

/// Phase 1 首遍保护的尾部消息数（旧 `deepCleanBase64(messages, 2)`）。
pub const PHASE1_FIRST_PASS_PROTECT_TAIL: usize = 2;

/// Phase 1 次遍保护的尾部消息数（旧 `deepCleanBase64(messages, 1)`）。
///
/// 旧源注释明示取向：最新的请求 / 工具结果消息**永不静默删除**，历史清理不足
/// 时由后续级联/守卫显式失败，而不是改写当前用户意图。
pub const PHASE1_SECOND_PASS_PROTECT_TAIL: usize = 1;

/// 字符集采样判定的采样长度上界（旧 `Math.min(1000, content.length())`）。
const BASE64_SAMPLE_SIZE: usize = 1000;

/// 字符集采样判定的合法字符占比阈值（旧 `> 0.95`）。
const BASE64_VALID_CHAR_RATIO: f64 = 0.95;

/// 字符/token 比的合法区间下界（旧 `validateRatio` 的 `ratio <= 0.5` 拒绝）。
const MIN_TEXT_TOKEN_RATIO: f64 = 0.5;

/// 字符/token 比的合法区间上界（旧 `validateRatio` 的 `ratio > 16.0` 拒绝）。
const MAX_TEXT_TOKEN_RATIO: f64 = 16.0;

/// Phase 2 策略 2 的图片占位文案（旧 `"[图片已移除以减少上下文]"`）。
pub const IMAGE_REPLACED_TEXT: &str = "[图片已移除以减少上下文]";

/// Phase 2 未触发降级时的摘要（旧 `"无需降级"`）。
pub const SUMMARY_NO_REDUCTION: &str = "无需降级";

/// Phase 2 策略 1 摘要片段（旧 `summary.append("策略1-缩略图替换;")`）。
pub const SUMMARY_STRATEGY_THUMBNAIL: &str = "策略1-缩略图替换;";

/// Phase 2 策略 2 摘要片段（旧 `summary.append("策略2-图片替换为文本;")`）。
pub const SUMMARY_STRATEGY_TEXT: &str = "策略2-图片替换为文本;";

/// Phase 2 策略 3 摘要片段（旧 `summary.append("策略3-移除所有图片;")`）。
pub const SUMMARY_STRATEGY_REMOVE: &str = "策略3-移除所有图片;";

/// 安全省略原因：解码后字节超 10 MiB 闸门（旧文案逐字）。
pub const REASON_OVERSIZE: &str = "encoded image exceeds 10 MiB safety limit";

/// 安全省略原因：无法建立图片输入流（旧 `input == null` 分支文案逐字）。
pub const REASON_UNSUPPORTED_STREAM: &str = "unsupported image stream";

/// 安全省略原因：无可用解码器（旧 `!readers.hasNext()` 分支文案逐字）。
pub const REASON_UNSUPPORTED_FORMAT: &str = "unsupported image format";

/// 安全省略原因：宽高超像素闸门（旧文案逐字）。
pub const REASON_PIXEL_BUDGET: &str = "image dimensions exceed safe pixel budget";

/// 安全省略原因：全解码失败（旧 `ImageIO.read == null` 分支文案逐字）。
pub const REASON_DECODE_FAILED: &str = "image decode failed";

/// 安全省略原因：缩略图生成抛异常（旧 `catch (Exception)` 分支文案逐字）。
pub const REASON_THUMBNAIL_FAILED: &str = "thumbnail generation failed";

// ==================== 错误类型 ====================

/// 预算守卫的失败原因。
///
/// 旧实现在 `validateRatio` 里抛 `IllegalArgumentException`；Rust 侧不 panic，
/// 改为显式 `Result`——调用方（引擎接线层）据此决定跳过守卫而非静默篡改比率。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageBudgetError {
    /// 字符/token 比非有限值或落在 `(0.5, 16.0]` 之外（旧 `validateRatio`）。
    InvalidTextTokenRatio,
    /// 承载 CPU 密集解码/编码的 `spawn_blocking` 任务异常终止（panic 或运行时
    /// 关停）。旧实现全程同步，无此故障面。
    ThumbnailTaskFailed,
}

impl fmt::Display for ImageBudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTextTokenRatio => f.write_str("Invalid textTokenRatio"),
            Self::ThumbnailTaskFailed => f.write_str("thumbnail blocking task failed"),
        }
    }
}

impl std::error::Error for ImageBudgetError {}

/// 字符/token 比合法性校验（逐字对照旧 `validateRatio`）。
fn validate_ratio(ratio: f64) -> Result<(), ImageBudgetError> {
    if !ratio.is_finite() || ratio <= MIN_TEXT_TOKEN_RATIO || ratio > MAX_TEXT_TOKEN_RATIO {
        return Err(ImageBudgetError::InvalidTextTokenRatio);
    }
    Ok(())
}

// ==================== 返回类型（对照旧 records） ====================

/// Phase 1 结果（逐字段对照旧 `TokenBudgetGuard.GuardResult`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardResult {
    /// 清理后的消息列表（未触发清理时为入参的等值拷贝，对照旧
    /// `new ArrayList<>(messages)`）。
    pub messages: Vec<ChatMessage>,
    /// 是否执行了清理。
    pub trimmed: bool,
    /// 清理前估算 token。
    pub tokens_before: u32,
    /// 清理后估算 token。
    pub tokens_after: u32,
}

/// Phase 2 结果（逐字段对照旧 `TokenBudgetGuard.FinalBudgetResult`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalBudgetResult {
    /// 降级后的 API 格式消息。
    pub api_messages: Vec<Value>,
    /// 最终 payload 中仍然保留的图片指纹集合（跨轮回传给注入器做去重）。
    pub retained_image_hashes: HashSet<String>,
    /// 最终估算 token。
    pub estimated_tokens: u32,
    /// 输入预算。
    pub input_budget: i64,
    /// 是否已落入预算内。
    pub fits_budget: bool,
    /// 降级过程摘要（旧 `reductionSummary`）。
    pub reduction_summary: String,
}

// ==================== Base64 判定 ====================

/// 判定内容是否为 Base64 图片数据（逐条对照旧 `isBase64Content`）。
///
/// 判定顺序：① 命中 [`BASE64_PREFIXES`] 任一前缀即高置信度返回真；② 否则仅当
/// 长度超过 [`BASE64_LENGTH_THRESHOLD`] 时才做字符集采样判定。
#[must_use]
pub fn is_base64_content(content: &str) -> bool {
    if content.is_empty() {
        return false;
    }
    if BASE64_PREFIXES
        .iter()
        .any(|prefix| content.starts_with(prefix))
    {
        return true;
    }
    let length = char_count(content);
    if length > BASE64_LENGTH_THRESHOLD {
        return is_likely_base64(content, length);
    }
    false
}

/// 字符集采样判定（逐条对照旧 `isLikelyBase64`）。
///
/// 取前 1000 个字符，合法 Base64 字符占比 > 95% 即判定为 Base64。换行符不计入
/// 分子且**顺延采样窗口**——旧实现在循环内改写 `sampleSize` 的这一行为一并照搬，
/// 以保证判定边界逐字一致。
fn is_likely_base64(content: &str, total_chars: usize) -> bool {
    let mut sample_size = BASE64_SAMPLE_SIZE.min(total_chars);
    let mut valid_chars = 0usize;
    for (index, ch) in content.chars().enumerate() {
        if index >= sample_size {
            break;
        }
        if ch.is_ascii_alphanumeric() || matches!(ch, '+' | '/' | '=') {
            valid_chars += 1;
        } else if ch == '\n' || ch == '\r' {
            sample_size = (sample_size + 1).min(total_chars);
        }
    }
    sample_size > 0 && as_f64(valid_chars) / as_f64(sample_size) > BASE64_VALID_CHAR_RATIO
}

// ==================== token 估算（守卫自有口径） ====================

/// 消息列表的 token 估算（逐条对照旧 `estimateTokens(List<Message>, ratio)`）。
///
/// 注意这是**守卫自有**的估算口径（文本 `len / ratio` 向零截断、Base64 载荷
/// `1:1`），与 [`super::estimate_tokens`]（`ceil(总字符 / ratio) + 条数 × 4`）
/// 是两套独立算法——旧实现同样并存两者，各自服务不同判定，故不合并。
#[must_use]
pub fn estimate_tokens(messages: &[ChatMessage], text_token_ratio: f64) -> u32 {
    let mut total = 0u32;
    for message in messages {
        total = total.saturating_add(match message.role {
            // 旧 SystemMessage：只有 String content，一律按文本比率折算。
            Role::System => ratio_tokens(char_count(&message.content), text_token_ratio),
            // 旧 UserMessage.toolUseResult / ToolResultBlock：Base64 走 1:1。
            Role::Tool => estimate_string_tokens(&message.content, text_token_ratio),
            // 旧 TextBlock（按比率）+ ImageBlock（data URI 载荷按 1:1）。
            Role::User | Role::Assistant => {
                estimate_text_content_tokens(&message.content, text_token_ratio)
            }
        });
        // 旧 ToolUseBlock：仅计入 input 的序列化长度（不含工具名）。
        for call in &message.tool_calls {
            total =
                total.saturating_add(ratio_tokens(char_count(&call.arguments), text_token_ratio));
        }
    }
    total
}

/// 单段字符串估算（逐条对照旧 `estimateStringTokens`）：Base64 走 `1:1`，
/// 否则按比率折算。
fn estimate_string_tokens(content: &str, text_token_ratio: f64) -> u32 {
    if is_base64_content(content) {
        return one_to_one_tokens(char_count(content));
    }
    ratio_tokens(char_count(content), text_token_ratio)
}

/// 用户/助手正文估算：data URI 载荷段按 `1:1`（旧 `ImageBlock`），其余按比率
/// （旧 `TextBlock`）。
fn estimate_text_content_tokens(content: &str, text_token_ratio: f64) -> u32 {
    let mut total = 0u32;
    let mut text_chars = 0usize;
    for segment in split_data_uri_segments(content) {
        match segment {
            Segment::Text(text) => text_chars += char_count(text),
            Segment::ImagePayload(payload) => {
                total = total.saturating_add(one_to_one_tokens(char_count(payload)));
            }
        }
    }
    total.saturating_add(ratio_tokens(text_chars, text_token_ratio))
}

// ==================== 数值转换辅助 ====================

/// 字符数（Unicode 标量计数）。
///
/// 旧 Java `String.length()` 为 UTF-16 码元数，二者在 BMP（含全部 CJK 常用字与
/// Base64 的纯 ASCII 载荷）一致；此为 [`super`] 既有约定的延续。
fn char_count(text: &str) -> usize {
    text.chars().count()
}

/// `(int)(chars / ratio)`：向零截断并饱和到 [`u32::MAX`]（旧 int 强转语义）。
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "上界由 >= f64::from(u32::MAX) 夹紧、下界由 <= 0.0 分支拦截，转换无溢出"
)]
fn ratio_tokens(chars: usize, ratio: f64) -> u32 {
    let value = as_f64(chars) / ratio;
    if !value.is_finite() || value >= f64::from(u32::MAX) {
        return u32::MAX;
    }
    if value <= 0.0 {
        return 0;
    }
    value.trunc() as u32
}

/// Base64 载荷的 `1:1` 计数（旧 `total += data.length()`）。
fn one_to_one_tokens(chars: usize) -> u32 {
    u32::try_from(chars).unwrap_or(u32::MAX)
}

/// usize → f64（字符量级远低于 f64 精确整数上界；旧实现同为整型提升）。
#[expect(
    clippy::cast_precision_loss,
    reason = "字符计数量级远低于 f64 精确整数上界（2^53），旧实现同为 int→double 提升"
)]
fn as_f64(value: usize) -> f64 {
    value as f64
}

// ==================== data URI 分段（旧 ImageBlock 的载体适配） ====================

/// `data:` 前缀（与 [`crate::recovery`] 的 data URI 扫描约定同源）。
const DATA_URI_NEEDLE: &str = "data:";

/// Base64 载荷分隔符（同上）。
const DATA_URI_HEADER: &str = ";base64,";

/// `data:` 与 `;base64,` 之间的 MIME 段扫描窗口上界（同 [`crate::recovery`]）。
const DATA_URI_MIME_WINDOW: usize = 128;

/// 正文分段：普通文本 / data URI 图片载荷。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Segment<'a> {
    /// 非图片文本（旧 `TextBlock`）。
    Text(&'a str),
    /// data URI 图片（旧 `ImageBlock`）；载荷对应旧 `ImageBlock.base64Data()`。
    ImagePayload(&'a str),
}

/// 把正文切分为文本段与 data URI 图片段。
///
/// 扫描规则与 [`crate::recovery`] 的 `strip_data_uris` 逐字一致（同一终止符
/// 集合、同一 MIME 窗口），保证两处对「什么算一张内嵌图」的认定不产生分歧。
fn split_data_uri_segments(content: &str) -> Vec<Segment<'_>> {
    let mut segments = Vec::new();
    let mut search_from = 0usize;
    loop {
        let Some(relative) = content[search_from..].find(DATA_URI_NEEDLE) else {
            push_text(&mut segments, &content[search_from..]);
            break;
        };
        let start = search_from + relative;
        let mut window_end = (start + DATA_URI_MIME_WINDOW).min(content.len());
        while !content.is_char_boundary(window_end) {
            window_end -= 1;
        }
        if let Some(header_relative) = content[start..window_end].find(DATA_URI_HEADER) {
            push_text(&mut segments, &content[search_from..start]);
            let payload_start = start + header_relative + DATA_URI_HEADER.len();
            let payload_end = data_uri_payload_end(content, payload_start);
            segments.push(Segment::ImagePayload(&content[payload_start..payload_end]));
            search_from = payload_end;
        } else {
            // 非 data URI：原样保留 "data:" 后继续（旧扫描的同一让步）。
            let keep_end = start + DATA_URI_NEEDLE.len();
            push_text(&mut segments, &content[search_from..keep_end]);
            search_from = keep_end;
        }
    }
    segments
}

/// 定位 data URI 载荷终点（空白或结构分隔符处截断；同 [`crate::recovery`]）。
fn data_uri_payload_end(content: &str, payload_start: usize) -> usize {
    for (offset, ch) in content[payload_start..].char_indices() {
        if ch.is_whitespace() || matches!(ch, ')' | '"' | '\'' | ']' | '}' | '>' | ',') {
            return payload_start + offset;
        }
    }
    content.len()
}

/// 追加非空文本段（空段不入列，避免下游无意义遍历）。
fn push_text<'a>(segments: &mut Vec<Segment<'a>>, text: &'a str) {
    if !text.is_empty() {
        segments.push(Segment::Text(text));
    }
}

/// 清理占位文案（逐字对照旧 `"[image content removed - was %d chars]"`）。
fn removed_placeholder(chars: usize) -> String {
    format!("[image content removed - was {chars} chars]")
}

// ==================== Phase 1：历史 Base64 清理 ====================

/// Phase 1（缺省比率，逐字对照旧 `enforcePhase1(messages, inputBudget)`）。
///
/// `input_budget` 取 [`i64`] 而非 [`u32`]：旧 `inputBudget` 是 `int`，由
/// `contextWindow - maxTokens - systemPromptTokens - toolDefsTokens - buffer`
/// 相减得出，**可为负**（小窗口模型 + 大工具定义时），负预算意味着「无论如何都
/// 超限」并令守卫必然触发。用无符号会把该语义钳成 0，故保留符号。
#[must_use]
pub fn enforce_phase1(messages: &[ChatMessage], input_budget: i64) -> GuardResult {
    // [`TEXT_TOKEN_RATIO`] 恒在合法区间，直接进内部实现，无需过校验。
    phase1(messages, input_budget, TEXT_TOKEN_RATIO)
}

/// Phase 1（显式比率，逐条对照旧 `enforcePhase1(messages, inputBudget, ratio)`）。
///
/// 两遍策略：第一遍保护尾部 [`PHASE1_FIRST_PASS_PROTECT_TAIL`] 条；若仍超限则
/// **从原始列表重新清理**（非在第一遍结果上叠加），只保护尾部
/// [`PHASE1_SECOND_PASS_PROTECT_TAIL`] 条。
///
/// # Errors
///
/// `text_token_ratio` 非有限值或落在 `(0.5, 16.0]` 之外时返回
/// [`ImageBudgetError::InvalidTextTokenRatio`]（旧 `validateRatio` 抛
/// `IllegalArgumentException` 的等价位置）。
pub fn enforce_phase1_with_ratio(
    messages: &[ChatMessage],
    input_budget: i64,
    text_token_ratio: f64,
) -> Result<GuardResult, ImageBudgetError> {
    validate_ratio(text_token_ratio)?;
    Ok(phase1(messages, input_budget, text_token_ratio))
}

/// Phase 1 内部实现（比率已校验）。
fn phase1(messages: &[ChatMessage], input_budget: i64, text_token_ratio: f64) -> GuardResult {
    let tokens_before = estimate_tokens(messages, text_token_ratio);
    if i64::from(tokens_before) <= input_budget {
        return GuardResult {
            messages: messages.to_vec(),
            trimmed: false,
            tokens_before,
            tokens_after: tokens_before,
        };
    }

    // 第一遍：protectTail = 2。
    let cleaned = deep_clean_base64(messages, PHASE1_FIRST_PASS_PROTECT_TAIL);
    let tokens_after_first_pass = estimate_tokens(&cleaned, text_token_ratio);
    if i64::from(tokens_after_first_pass) <= input_budget {
        tracing::info!(
            tokens_before,
            tokens_after = tokens_after_first_pass,
            input_budget,
            "Phase1 first pass sufficient"
        );
        return GuardResult {
            messages: cleaned,
            trimmed: true,
            tokens_before,
            tokens_after: tokens_after_first_pass,
        };
    }

    // 第二遍：protectTail = 1。最新的请求/工具结果消息永不静默删除——历史清理
    // 不足时由后续级联/守卫显式失败，而不是改写当前用户意图（旧源注释）。
    let cleaned = deep_clean_base64(messages, PHASE1_SECOND_PASS_PROTECT_TAIL);
    let tokens_after_second_pass = estimate_tokens(&cleaned, text_token_ratio);
    tracing::info!(
        tokens_before,
        tokens_after = tokens_after_second_pass,
        input_budget,
        "Phase1 second pass"
    );
    GuardResult {
        messages: cleaned,
        trimmed: true,
        tokens_before,
        tokens_after: tokens_after_second_pass,
    }
}

/// 深拷贝并清理（逐条对照旧 `deepCleanBase64`）：`[protect_from, len)` 区间的
/// 尾部消息原样保留，其余逐条清理。
fn deep_clean_base64(messages: &[ChatMessage], protect_tail_count: usize) -> Vec<ChatMessage> {
    let protect = protect_tail_count.max(1);
    let protect_from = messages.len().saturating_sub(protect);
    messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            if index >= protect_from {
                message.clone()
            } else {
                clean_message_base64(message)
            }
        })
        .collect()
}

/// 单条消息清理（逐条对照旧 `cleanMessageBase64` + `cleanContentBlocks`）。
fn clean_message_base64(message: &ChatMessage) -> ChatMessage {
    // 旧 SystemMessage 只有 String content，通常不含 Base64，不清理。
    if message.role == Role::System {
        return message.clone();
    }
    let mut cleaned = message.clone();
    cleaned.content = clean_content_base64(&message.content);
    cleaned
}

/// 正文清理：整段 Base64（旧 `TextBlock` / `ToolResultBlock` 分支）优先，
/// 否则逐个替换内嵌 data URI（旧 `ImageBlock` 分支）。
fn clean_content_base64(content: &str) -> String {
    if content.is_empty() {
        return String::new();
    }
    if is_base64_content(content) {
        return removed_placeholder(char_count(content));
    }
    let segments = split_data_uri_segments(content);
    if !segments
        .iter()
        .any(|segment| matches!(segment, Segment::ImagePayload(_)))
    {
        return content.to_owned();
    }
    let mut out = String::with_capacity(content.len());
    for segment in segments {
        match segment {
            Segment::Text(text) => out.push_str(text),
            Segment::ImagePayload(payload) => {
                out.push_str(&removed_placeholder(char_count(payload)));
            }
        }
    }
    out
}

// ==================== 级联热路径插桩点 ====================

/// 预算缓冲比例（旧 `bufferTokens = (int)(effectiveContextWindow * 0.05)`）。
#[cfg(feature = "image-budget")]
const INPUT_BUDGET_BUFFER_RATIO: f64 = 0.05;

/// 级联前置守卫入口——在 [`super::ContextCascade::execute_pre_api_cascade`] 的
/// 最前面执行 Phase 1 Base64 清理，返回是否发生了清理。
///
/// # 与旧调用点的预算差异（留痕，非臆造）
///
/// 旧 `QueryEngine` Step 3 的输入预算算式为「上下文窗口 − 最大输出 − 系统提示 −
/// 工具定义 − 上下文窗口 × 0.05」。级联这一层拿不到最大输出 / 系统提示 / 工具定义
/// 三项（它们属请求组装层），**不臆造缺省值**，故此处只用本层已知的两项：
/// 「上下文窗口 − 上下文窗口 × 0.05」。该预算是旧预算的**上界**，即本插桩点
/// 比旧实现更少触发——它是兜底安全网，专治 P0-15 的核心威胁模型「粘一张大图即
/// 一次打穿上下文窗口」（一张 5 MB 图的 Base64 载荷按 `1:1` 计约 680 万 token，
/// 任何窗口都会击穿）。需要与旧实现逐字一致的预算时，由请求组装层直接调用
/// [`enforce_phase1_with_ratio`] 并传入完整算式的结果。
#[cfg(feature = "image-budget")]
pub fn apply(messages: &mut Vec<ChatMessage>, model: &str) -> bool {
    let budget = pre_cascade_input_budget(model);
    let ratio = super::token_char_ratio(model);
    let Ok(result) = enforce_phase1_with_ratio(messages, budget, ratio) else {
        tracing::warn!(
            model,
            ratio,
            "模型字符/token 比超出 (0.5, 16.0] 合法区间，跳过 Phase 1 预清理"
        );
        return false;
    };
    if result.trimmed {
        tracing::info!(
            tokens_before = result.tokens_before,
            tokens_after = result.tokens_after,
            "[ImageOpt] Phase1 cleanup"
        );
        *messages = result.messages;
    }
    result.trimmed
}

/// `image-budget` 关闭时的空转实现：不触碰消息、不引入 `image` 依赖，级联热路径
/// 与接入前逐字一致。
#[cfg(not(feature = "image-budget"))]
pub fn apply(messages: &mut Vec<ChatMessage>, model: &str) -> bool {
    let _ = (messages, model);
    false
}

/// 级联层可见的输入预算（见 [`apply`] 文档的差异说明）。
#[cfg(feature = "image-budget")]
fn pre_cascade_input_budget(model: &str) -> i64 {
    let context_window = super::context_window_for(model);
    let buffer = super::saturating_tokens(f64::from(context_window) * INPUT_BUDGET_BUFFER_RATIO);
    i64::from(context_window) - i64::from(buffer)
}

// ==================== 图片指纹（旧 hashImageBlock / collectImageHashes） ====================

/// API 块类型字段名（旧 `blockMap.get("type")`）。
const BLOCK_TYPE_KEY: &str = "type";

/// 图片块类型值（旧 `"image".equals(type)`）。
const BLOCK_TYPE_IMAGE: &str = "image";

/// 文本块类型值。
const BLOCK_TYPE_TEXT: &str = "text";

/// 工具结果块类型值。
const BLOCK_TYPE_TOOL_RESULT: &str = "tool_result";

/// 文本块正文字段名。
const BLOCK_TEXT_KEY: &str = "text";

/// 图片块来源字段名（旧 `blockMap.get("source")`）。
const BLOCK_SOURCE_KEY: &str = "source";

/// 图片来源的 Base64 数据字段名（旧 `sourceMap.get("data")`）。
const SOURCE_DATA_KEY: &str = "data";

/// 图片来源的媒体类型字段名（旧 `newSource.put("media_type", ..)`）。
#[cfg(feature = "image-budget")]
const SOURCE_MEDIA_TYPE_KEY: &str = "media_type";

/// 缩略图重编码后的媒体类型（旧 `"image/jpeg"`）。
#[cfg(feature = "image-budget")]
const JPEG_MEDIA_TYPE: &str = "image/jpeg";

/// 消息的内容字段名（旧 `msg.get("content")`）。
const MESSAGE_CONTENT_KEY: &str = "content";

/// 图片块指纹（逐条对照旧 `hashImageBlock`）：`source.data` 经 Base64 解码后取
/// 裸 SHA-256，输出小写 hex 64 字符（旧 `HexFormat.of().formatHex`）。
///
/// `source` 缺失 / `data` 非字符串或空白 / Base64 非法时返回 `None`（旧实现
/// 对应 `return null` 与 `catch (Exception invalid) { return null; }`）。
#[must_use]
pub fn image_block_hash(block: &Value) -> Option<String> {
    let encoded = block
        .get(BLOCK_SOURCE_KEY)?
        .get(SOURCE_DATA_KEY)?
        .as_str()?;
    if encoded.trim().is_empty() {
        return None;
    }
    let bytes = STANDARD.decode(encoded).ok()?;
    Some(hex(&Sha256::digest(&bytes)))
}

/// 小写 hex 编码，对照 `java.util.HexFormat.of().formatHex`。
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // 写入 String 不会失败。
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// 收集 API 消息中全部图片块的指纹（逐条对照旧 `collectImageHashes`）。
///
/// 结果即旧 `FinalBudgetResult.retainedImageHashes`：调用方跨轮回传给注入器的
/// `confirmedImageHashes`，从而让同一张图不再被重复全量注入。
#[must_use]
pub fn collect_image_hashes(api_messages: &[Value]) -> HashSet<String> {
    let mut hashes = HashSet::new();
    for message in api_messages {
        for block in content_blocks(message) {
            if is_image_block(block)
                && block.get(BLOCK_SOURCE_KEY).is_some_and(Value::is_object)
                && let Some(hash) = image_block_hash(block)
            {
                hashes.insert(hash);
            }
        }
    }
    hashes
}

/// 消息的内容块切片（`content` 非数组时为空——旧实现对应 `else` 分支的整条拷贝）。
fn content_blocks(message: &Value) -> &[Value] {
    message
        .get(MESSAGE_CONTENT_KEY)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// 是否为图片块（旧 `"image".equals(blockMap.get("type"))`）。
fn is_image_block(block: &Value) -> bool {
    block.get(BLOCK_TYPE_KEY).and_then(Value::as_str) == Some(BLOCK_TYPE_IMAGE)
}

/// 该图片块是否属于**本轮临时注入**（旧
/// `transientImageHashes.contains(hashImageBlock(blockMap))`）。
///
/// 这是「去重」的落点：只有临时注入的图片才允许被降级，已确认保留的历史图片
/// 一律原样透传——否则跨轮反复重编码同一张图会持续损失画质。
#[cfg(feature = "image-budget")]
fn is_transient_image(block: &Value, transient_image_hashes: &HashSet<String>) -> bool {
    is_image_block(block)
        && image_block_hash(block).is_some_and(|hash| transient_image_hashes.contains(&hash))
}

// ==================== Phase 2：最终 payload 校验与梯度降级 ====================

/// 对图片块逐个施加变换、非图片块原样保留，重建 API 消息列表。
///
/// 旧三个 `replaceImages*` / `removeAll*` 方法共享同一遍历骨架（含
/// 「`content` 非数组则整条拷贝」与「内容项非对象则丢弃」两处行为），此处提取为
/// 单一骨架 + 变换闭包，行为逐字保留。变换返回 `None` 表示丢弃该块（旧
/// `removeAllInjectedImages` 的语义）。
#[cfg(feature = "image-budget")]
fn map_image_blocks<F>(api_messages: &[Value], mut transform: F) -> Vec<Value>
where
    F: FnMut(&Value) -> Option<Value>,
{
    let mut result = Vec::with_capacity(api_messages.len());
    for message in api_messages {
        let Some(blocks) = message.get(MESSAGE_CONTENT_KEY).and_then(Value::as_array) else {
            result.push(message.clone());
            continue;
        };
        let new_content: Vec<Value> = blocks
            .iter()
            // 旧实现只处理 `item instanceof Map` 的项，非对象项被丢弃。
            .filter(|block| block.is_object())
            .filter_map(&mut transform)
            .collect();
        let mut new_message = message.clone();
        if let Some(object) = new_message.as_object_mut() {
            object.insert(MESSAGE_CONTENT_KEY.to_owned(), Value::Array(new_content));
        }
        result.push(new_message);
    }
    result
}

/// 策略 2：把临时注入的图片换成文本占位（逐条对照旧
/// `replaceImagesWithTextSummary`）。
#[cfg(feature = "image-budget")]
fn replace_images_with_text_summary(
    api_messages: &[Value],
    transient_image_hashes: &HashSet<String>,
) -> Vec<Value> {
    map_image_blocks(api_messages, |block| {
        if is_transient_image(block, transient_image_hashes) {
            Some(text_block(IMAGE_REPLACED_TEXT.to_owned()))
        } else {
            Some(block.clone())
        }
    })
}

/// 策略 3：移除全部临时注入的图片（逐条对照旧 `removeAllInjectedImages`）。
#[cfg(feature = "image-budget")]
fn remove_all_injected_images(
    api_messages: &[Value],
    transient_image_hashes: &HashSet<String>,
) -> Vec<Value> {
    map_image_blocks(api_messages, |block| {
        if is_transient_image(block, transient_image_hashes) {
            None
        } else {
            Some(block.clone())
        }
    })
}

/// 文本块构造（旧 `Map.of("type", "text", "text", ..)`）。
#[cfg(feature = "image-budget")]
fn text_block(text: String) -> Value {
    let mut object = Map::new();
    object.insert(BLOCK_TYPE_KEY.to_owned(), Value::from(BLOCK_TYPE_TEXT));
    object.insert(BLOCK_TEXT_KEY.to_owned(), Value::from(text));
    Value::Object(object)
}

/// 安全省略占位（逐字对照旧 `omittedImage(reason)` 的文案模板）。
#[cfg(feature = "image-budget")]
fn omitted_image(reason: &str) -> Value {
    text_block(format!("[Image omitted by media safety guard: {reason}]"))
}

/// API 格式消息的 token 估算（逐条对照旧 `estimateApiTokens`）。
#[must_use]
pub fn estimate_api_tokens(api_messages: &[Value], text_token_ratio: f64) -> u32 {
    let mut total = 0u32;
    for message in api_messages {
        let content = message.get(MESSAGE_CONTENT_KEY);
        if let Some(text) = content.and_then(Value::as_str) {
            total = total.saturating_add(ratio_tokens(char_count(text), text_token_ratio));
            continue;
        }
        for block in content_blocks(message) {
            if !block.is_object() {
                continue;
            }
            total = total.saturating_add(estimate_api_block_tokens(block, text_token_ratio));
        }
    }
    total
}

/// 单个 API 块的 token 估算（旧 `estimateApiTokens` 的块内分支树）。
fn estimate_api_block_tokens(block: &Value, text_token_ratio: f64) -> u32 {
    match block.get(BLOCK_TYPE_KEY).and_then(Value::as_str) {
        Some(BLOCK_TYPE_TEXT) => block
            .get(BLOCK_TEXT_KEY)
            .and_then(Value::as_str)
            .map_or(0, |text| ratio_tokens(char_count(text), text_token_ratio)),
        // 图片：Base64 载荷按 1:1。
        Some(BLOCK_TYPE_IMAGE) => block
            .get(BLOCK_SOURCE_KEY)
            .filter(|source| source.is_object())
            .and_then(|source| source.get(SOURCE_DATA_KEY))
            .and_then(Value::as_str)
            .map_or(0, |data| one_to_one_tokens(char_count(data))),
        // 工具结果：Base64 走 1:1，否则按比率。
        Some(BLOCK_TYPE_TOOL_RESULT) => block
            .get(MESSAGE_CONTENT_KEY)
            .and_then(Value::as_str)
            .map_or(0, |content| {
                estimate_string_tokens(content, text_token_ratio)
            }),
        // tool_use / thinking 等其他类型按序列化文本估算（见模块文档「未移植项」）。
        _ => ratio_tokens(char_count(&block.to_string()), text_token_ratio),
    }
}

/// Phase 2 策略 1：把临时注入的图片替换为缩略图（逐条对照旧
/// `replaceImagesWithThumbnails`）。
#[cfg(feature = "image-budget")]
fn replace_images_with_thumbnails(
    api_messages: &[Value],
    max_dim: u32,
    quality: u8,
    transient_image_hashes: &HashSet<String>,
) -> Vec<Value> {
    map_image_blocks(api_messages, |block| {
        if is_transient_image(block, transient_image_hashes) {
            Some(resize_image_block(block, max_dim, quality))
        } else {
            Some(block.clone())
        }
    })
}

/// 单张图片的缩略图重编码（逐条对照旧 `resizeImageBlock`）。
///
/// 分支与旧实现一一对应：
///
/// | 条件 | 旧行为 | 本实现 |
/// |---|---|---|
/// | `source` / `data` 缺失或空 | 整块原样拷贝 | 同 |
/// | Base64 解码失败 | 外层 `catch` → `thumbnail generation failed` | 同 |
/// | 解码字节 > 10 MiB | `encoded image exceeds 10 MiB safety limit` | 同 |
/// | 建不出图片输入流 | `unsupported image stream` | 猜格式失败 |
/// | 无可用解码器 | `unsupported image format` | 格式未识别 / 读头部失败 |
/// | 宽或高为 0，或 `宽 × 高 > 4000 万` | `image dimensions exceed safe pixel budget` | 同 |
/// | 全解码失败 | `image decode failed` | 同 |
/// | 宽高均 ≤ `max_dim` | 整块原样拷贝（已经够小） | 同 |
/// | 编码失败 | `thumbnail generation failed` | 同 |
///
/// 像素闸门**先于全解码**执行：旧实现用 `ImageReader.getWidth/getHeight(0)` 只读
/// 头部，本实现用 [`image::ImageReader::into_dimensions`]（同为只解析头部），
/// 从而让超大图在分配像素缓冲之前就被拦下。
#[cfg(feature = "image-budget")]
fn resize_image_block(block: &Value, max_dim: u32, quality: u8) -> Value {
    use std::io::Cursor;

    use image::ImageReader;
    use image::codecs::jpeg::JpegEncoder;
    use image::imageops::FilterType;

    let Some(source) = block.get(BLOCK_SOURCE_KEY).and_then(Value::as_object) else {
        return block.clone();
    };
    // 旧 `(String) sourceMap.get("data")`：缺失或非字符串即 null → 整块原样拷贝。
    let Some(data) = source.get(SOURCE_DATA_KEY).and_then(Value::as_str) else {
        return block.clone();
    };
    if data.is_empty() {
        return block.clone();
    }
    // 旧 `Base64.getDecoder().decode` 抛异常时落到方法级 catch。
    let Ok(bytes) = STANDARD.decode(data) else {
        return omitted_image(REASON_THUMBNAIL_FAILED);
    };
    if bytes.len() > MAX_DECODED_IMAGE_BYTES {
        return omitted_image(REASON_OVERSIZE);
    }

    // ===== 闸门 1/2：只读头部取宽高，超像素预算即省略（不全解码） =====
    let Ok(reader) = ImageReader::new(Cursor::new(&bytes)).with_guessed_format() else {
        return omitted_image(REASON_UNSUPPORTED_STREAM);
    };
    if reader.format().is_none() {
        return omitted_image(REASON_UNSUPPORTED_FORMAT);
    }
    let Ok((width, height)) = reader.into_dimensions() else {
        return omitted_image(REASON_UNSUPPORTED_FORMAT);
    };
    if width == 0 || height == 0 || u64::from(width) * u64::from(height) > MAX_IMAGE_PIXELS {
        return omitted_image(REASON_PIXEL_BUDGET);
    }

    let Ok(original) = image::load_from_memory(&bytes) else {
        return omitted_image(REASON_DECODE_FAILED);
    };
    if width <= max_dim && height <= max_dim {
        return block.clone(); // 已经够小
    }
    let (new_width, new_height) = thumbnail_dimensions(width, height, max_dim);
    // 旧 `BufferedImage.TYPE_INT_RGB` + `VALUE_INTERPOLATION_BILINEAR`：JPEG 无
    // alpha 通道，故先转 RGB 再双线性（`Triangle`）缩放到旧算式给出的确切尺寸。
    let rgb = original.to_rgb8();
    let resized = image::imageops::resize(&rgb, new_width, new_height, FilterType::Triangle);

    let mut jpeg_bytes = Vec::new();
    let mut encoder = JpegEncoder::new_with_quality(&mut jpeg_bytes, quality);
    if let Err(error) = encoder.encode_image(&resized) {
        tracing::warn!(%error, "缩略图生成失败，安全省略原图");
        return omitted_image(REASON_THUMBNAIL_FAILED);
    }

    let mut new_source = source.clone();
    new_source.insert(
        SOURCE_DATA_KEY.to_owned(),
        Value::from(STANDARD.encode(&jpeg_bytes)),
    );
    new_source.insert(
        SOURCE_MEDIA_TYPE_KEY.to_owned(),
        Value::from(JPEG_MEDIA_TYPE),
    );
    let mut new_block = block.clone();
    if let Some(object) = new_block.as_object_mut() {
        object.insert(BLOCK_SOURCE_KEY.to_owned(), Value::Object(new_source));
    }
    new_block
}

/// 缩放后尺寸（逐字对照旧 `scale = min(maxDim/w, maxDim/h)` +
/// `max(1, (int)(w * scale))`）。
#[cfg(feature = "image-budget")]
fn thumbnail_dimensions(width: u32, height: u32, max_dim: u32) -> (u32, u32) {
    let scale = f64::min(
        f64::from(max_dim) / f64::from(width),
        f64::from(max_dim) / f64::from(height),
    );
    (
        scaled_dimension(width, scale),
        scaled_dimension(height, scale),
    )
}

/// `max(1, (int)(value * scale))`：向零截断后取 1 为下限。
#[cfg(feature = "image-budget")]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "上界由 >= f64::from(u32::MAX) 夹紧、下界由 < 1.0 分支拦截，转换无溢出"
)]
fn scaled_dimension(value: u32, scale: f64) -> u32 {
    let scaled = f64::from(value) * scale;
    if !scaled.is_finite() {
        return 1;
    }
    let truncated = scaled.trunc();
    if truncated < 1.0 {
        return 1;
    }
    if truncated >= f64::from(u32::MAX) {
        return u32::MAX;
    }
    truncated as u32
}

/// Phase 2（缺省比率，对照旧 `enforcePhase2(apiMessages, inputBudget, hashes)`）。
///
/// CPU 密集的解码 / 缩放 / JPEG 重编码整段放在
/// [`tokio::task::spawn_blocking`] 中执行，不占用异步运行时的工作线程。
///
/// # Errors
///
/// 承载降级链的阻塞任务异常终止（panic 或运行时关停）时返回
/// [`ImageBudgetError::ThumbnailTaskFailed`]；旧实现全程同步，无此故障面。
#[cfg(feature = "image-budget")]
#[expect(
    clippy::implicit_hasher,
    reason = "指纹集合与 [`collect_image_hashes`] / [`FinalBudgetResult`] 的 `HashSet<String>` 互锁（跨轮回传给注入器），泛化哈希器无调用方受益"
)]
pub async fn enforce_phase2(
    api_messages: Vec<Value>,
    input_budget: i64,
    transient_image_hashes: HashSet<String>,
) -> Result<FinalBudgetResult, ImageBudgetError> {
    enforce_phase2_with_ratio(
        api_messages,
        input_budget,
        transient_image_hashes,
        TEXT_TOKEN_RATIO,
    )
    .await
}

/// Phase 2（显式比率，对照旧四参 `enforcePhase2`）。
///
/// # Errors
///
/// - [`ImageBudgetError::InvalidTextTokenRatio`]：比率非有限值或不在
///   `(0.5, 16.0]` 内（旧 `validateRatio`）；
/// - [`ImageBudgetError::ThumbnailTaskFailed`]：阻塞任务异常终止。
#[cfg(feature = "image-budget")]
#[expect(
    clippy::implicit_hasher,
    reason = "指纹集合与 [`collect_image_hashes`] / [`FinalBudgetResult`] 的 `HashSet<String>` 互锁（跨轮回传给注入器），泛化哈希器无调用方受益"
)]
pub async fn enforce_phase2_with_ratio(
    api_messages: Vec<Value>,
    input_budget: i64,
    transient_image_hashes: HashSet<String>,
    text_token_ratio: f64,
) -> Result<FinalBudgetResult, ImageBudgetError> {
    validate_ratio(text_token_ratio)?;
    tokio::task::spawn_blocking(move || {
        phase2(
            api_messages,
            input_budget,
            &transient_image_hashes,
            text_token_ratio,
        )
    })
    .await
    .map_err(|error| {
        tracing::error!(%error, "Phase2 梯度降级的阻塞任务异常终止");
        ImageBudgetError::ThumbnailTaskFailed
    })
}

/// Phase 2 内部实现（同步、比率已校验；由 `spawn_blocking` 承载）。
///
/// 三级策略**各自从原始 payload 出发**（旧实现对策略 2/3 都重新
/// `deepCopyApiMessages(apiMessages)`），而非在上一级结果上叠加。Rust 的值语义
/// 使「深拷贝」隐含于重建，故不需要显式的 `deepCopyApiMessages`。
#[cfg(feature = "image-budget")]
fn phase2(
    api_messages: Vec<Value>,
    input_budget: i64,
    transient_image_hashes: &HashSet<String>,
    text_token_ratio: f64,
) -> FinalBudgetResult {
    let estimated = estimate_api_tokens(&api_messages, text_token_ratio);
    if i64::from(estimated) <= input_budget {
        let retained_image_hashes = collect_image_hashes(&api_messages);
        return FinalBudgetResult {
            api_messages,
            retained_image_hashes,
            estimated_tokens: estimated,
            input_budget,
            fits_budget: true,
            reduction_summary: SUMMARY_NO_REDUCTION.to_owned(),
        };
    }

    tracing::warn!(estimated, input_budget, "Phase2 超限，开始梯度降级");
    let mut summary = String::new();

    // ===== 策略 1：缩略图替换（640px / q=0.6） =====
    let current = replace_images_with_thumbnails(
        &api_messages,
        THUMBNAIL_MAX_DIM,
        THUMBNAIL_QUALITY,
        transient_image_hashes,
    );
    summary.push_str(SUMMARY_STRATEGY_THUMBNAIL);
    if let Some(result) = settled(current, input_budget, text_token_ratio, &summary) {
        return result;
    }

    // ===== 策略 2：图片替换为文本 =====
    let current = replace_images_with_text_summary(&api_messages, transient_image_hashes);
    summary.push_str(SUMMARY_STRATEGY_TEXT);
    if let Some(result) = settled(current, input_budget, text_token_ratio, &summary) {
        return result;
    }

    // ===== 策略 3：移除所有注入的图片（终局，无论是否落入预算都返回） =====
    let current = remove_all_injected_images(&api_messages, transient_image_hashes);
    summary.push_str(SUMMARY_STRATEGY_REMOVE);
    let estimated = estimate_api_tokens(&current, text_token_ratio);
    let fits_budget = i64::from(estimated) <= input_budget;
    if !fits_budget {
        tracing::error!(estimated, input_budget, "Phase2 所有降级策略执行后仍超限");
    }
    FinalBudgetResult {
        retained_image_hashes: collect_image_hashes(&current),
        api_messages: current,
        estimated_tokens: estimated,
        input_budget,
        fits_budget,
        reduction_summary: summary,
    }
}

/// 某一级策略执行后若已落入预算则收敛为结果，否则返回 `None` 继续下一级。
#[cfg(feature = "image-budget")]
fn settled(
    api_messages: Vec<Value>,
    input_budget: i64,
    text_token_ratio: f64,
    summary: &str,
) -> Option<FinalBudgetResult> {
    let estimated = estimate_api_tokens(&api_messages, text_token_ratio);
    if i64::from(estimated) > input_budget {
        return None;
    }
    Some(FinalBudgetResult {
        retained_image_hashes: collect_image_hashes(&api_messages),
        api_messages,
        estimated_tokens: estimated,
        input_budget,
        fits_budget: true,
        reduction_summary: summary.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "image-budget")]
    use std::collections::HashSet;

    #[cfg(feature = "image-budget")]
    use base64::Engine as _;
    #[cfg(feature = "image-budget")]
    use base64::engine::general_purpose::STANDARD;
    use serde_json::{Value, json};
    use zk_llm::{ChatMessage, ToolCallRequest};

    use super::{
        BASE64_LENGTH_THRESHOLD, BASE64_PREFIXES, IMAGE_REPLACED_TEXT, ImageBudgetError,
        MAX_DECODED_IMAGE_BYTES, MAX_IMAGE_PIXELS, PHASE1_FIRST_PASS_PROTECT_TAIL,
        PHASE1_SECOND_PASS_PROTECT_TAIL, REASON_DECODE_FAILED, REASON_OVERSIZE,
        REASON_PIXEL_BUDGET, REASON_THUMBNAIL_FAILED, REASON_UNSUPPORTED_FORMAT,
        REASON_UNSUPPORTED_STREAM, SUMMARY_NO_REDUCTION, SUMMARY_STRATEGY_REMOVE,
        SUMMARY_STRATEGY_TEXT, SUMMARY_STRATEGY_THUMBNAIL, TEXT_TOKEN_RATIO, THUMBNAIL_MAX_DIM,
        THUMBNAIL_QUALITY, apply, char_count, collect_image_hashes, enforce_phase1,
        enforce_phase1_with_ratio, estimate_api_tokens, estimate_tokens, image_block_hash,
        is_base64_content,
    };
    #[cfg(feature = "image-budget")]
    use super::{
        enforce_phase2, enforce_phase2_with_ratio, omitted_image, replace_images_with_thumbnails,
        resize_image_block, scaled_dimension, thumbnail_dimensions,
    };

    /// 未知模型：走 [`zk_llm::DEFAULT_CAPABILITIES`]（窗口 8192 / 比率 3.5），
    /// 故级联层预算 = `8192 - ceil(8192 × 0.05)` = `8192 - 410` = 7782。
    const UNKNOWN_MODEL: &str = "model-not-in-registry";

    /// `30_000` 字符载荷的清理占位（[`super::removed_placeholder`] 的期望值）。
    const PLACEHOLDER_30K: &str = "[image content removed - was 30000 chars]";

    /// 整段 JPEG Base64 载荷：魔数前缀 + 填充，总长恰为 `chars`。
    fn jpeg_payload(chars: usize) -> String {
        let prefix = BASE64_PREFIXES[0];
        let mut payload = String::from(prefix);
        payload.push_str(&"A".repeat(chars - prefix.len()));
        payload
    }

    /// API 图片块（旧 `{type:image, source:{type:base64, media_type, data}}`）。
    fn image_block(data: &str) -> Value {
        json!({
            "type": "image",
            "source": { "type": "base64", "media_type": "image/png", "data": data },
        })
    }

    #[test]
    fn url_image_blocks_are_ignored_by_budget_and_hashing() {
        // url 型图片块无 base64 载荷：哈希缺席、token 贡献 0，绝不 panic。
        let url_block = json!({
            "type": "image",
            "source": {
                "type": "url",
                "url": "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png",
            },
        });
        assert_eq!(image_block_hash(&url_block), None);
        let messages = vec![json!({ "role": "user", "content": [url_block] })];
        assert_eq!(estimate_api_tokens(&messages, TEXT_TOKEN_RATIO), 0);
        assert!(collect_image_hashes(&messages).is_empty());
    }

    // ==================== Phase 1：魔数识别与阈值 ====================

    #[test]
    fn five_base64_magic_prefixes_each_match_once() {
        // 逐条对照旧 `BASE64_PREFIXES`：JPEG / PNG / GIF / WebP / SVG。
        assert!(is_base64_content("/9j/4AAQSkZJRgABAQAAAQ"), "JPEG");
        assert!(is_base64_content("iVBORw0KGgoAAAANSUhEUg"), "PNG");
        assert!(is_base64_content("R0lGODlhAQABAIAAAAAAAP"), "GIF");
        assert!(is_base64_content("UklGRiQAAABXRUJQVlA4IB"), "WebP");
        assert!(is_base64_content("PHN2ZyB4bWxucz0iaHR0cD"), "SVG");
        // 五种前缀全部命中，且顺序与旧常量池一致。
        for prefix in BASE64_PREFIXES {
            assert!(is_base64_content(prefix), "{prefix}");
        }
        // 无魔数且未超长度阈值 → 一律判否（旧 `isBase64Content` 的 else 分支）。
        assert!(!is_base64_content("hello world"));
        assert!(!is_base64_content(""));
    }

    #[test]
    fn sampling_judgement_requires_the_length_threshold() {
        // 恰等于阈值不做采样判定（旧 `length > BASE64_LENGTH_THRESHOLD` 为严格大于）。
        let at_threshold = "QUJD".repeat(BASE64_LENGTH_THRESHOLD / 4);
        assert_eq!(char_count(&at_threshold), BASE64_LENGTH_THRESHOLD);
        assert!(!is_base64_content(&at_threshold));
        // 超过阈值后纯 Base64 字符集 → 判真。
        assert!(is_base64_content(&format!("{at_threshold}QUJD")));
        // 合法字符占比 50% ≤ 95% → 判否。
        assert!(!is_base64_content(
            &"AB@#".repeat(BASE64_LENGTH_THRESHOLD / 2)
        ));
        // MIME 折行（每 76 字符一个换行）不计入分子且顺延采样窗口 → 仍判真。
        assert!(is_base64_content(
            &format!("{}\n", "A".repeat(76)).repeat(600)
        ));
    }

    #[test]
    fn constants_match_the_legacy_pool() {
        assert!((TEXT_TOKEN_RATIO - 3.5).abs() < f64::EPSILON);
        assert_eq!(BASE64_LENGTH_THRESHOLD, 40_000);
        assert_eq!(MAX_DECODED_IMAGE_BYTES, 10 * 1024 * 1024);
        assert_eq!(MAX_IMAGE_PIXELS, 40_000_000);
        assert_eq!(
            BASE64_PREFIXES,
            ["/9j/", "iVBOR", "R0lGOD", "UklGR", "PHN2"]
        );
        assert_eq!(THUMBNAIL_MAX_DIM, 640);
        assert_eq!(THUMBNAIL_QUALITY, 60);
        assert_eq!(PHASE1_FIRST_PASS_PROTECT_TAIL, 2);
        assert_eq!(PHASE1_SECOND_PASS_PROTECT_TAIL, 1);
        assert_eq!(IMAGE_REPLACED_TEXT, "[图片已移除以减少上下文]");
        assert_eq!(SUMMARY_NO_REDUCTION, "无需降级");
        assert_eq!(SUMMARY_STRATEGY_THUMBNAIL, "策略1-缩略图替换;");
        assert_eq!(SUMMARY_STRATEGY_TEXT, "策略2-图片替换为文本;");
        assert_eq!(SUMMARY_STRATEGY_REMOVE, "策略3-移除所有图片;");
        assert_eq!(REASON_OVERSIZE, "encoded image exceeds 10 MiB safety limit");
        assert_eq!(REASON_UNSUPPORTED_STREAM, "unsupported image stream");
        assert_eq!(REASON_UNSUPPORTED_FORMAT, "unsupported image format");
        assert_eq!(
            REASON_PIXEL_BUDGET,
            "image dimensions exceed safe pixel budget"
        );
        assert_eq!(REASON_DECODE_FAILED, "image decode failed");
        assert_eq!(REASON_THUMBNAIL_FAILED, "thumbnail generation failed");
    }

    // ==================== token 估算（守卫自有口径） ====================

    #[test]
    fn estimate_tokens_matches_the_legacy_rulers() {
        // 文本按 `(int)(len / ratio)` 向零截断：35 / 3.5 = 10。
        assert_eq!(
            estimate_tokens(&[ChatMessage::user("x".repeat(35))], TEXT_TOKEN_RATIO),
            10
        );
        // 工具结果整段 Base64 → 1:1（旧 `estimateStringTokens`）。
        assert_eq!(
            estimate_tokens(
                &[ChatMessage::tool("call_1", jpeg_payload(100))],
                TEXT_TOKEN_RATIO
            ),
            100
        );
        // 内嵌 data URI 载荷 → 1:1（旧 `ImageBlock`），其余文本按比率：100 + 6/3.5。
        let content = format!("看图 data:image/png;base64,{} 完毕", "A".repeat(100));
        assert_eq!(
            estimate_tokens(&[ChatMessage::user(content)], TEXT_TOKEN_RATIO),
            101
        );
        // 工具调用只计入 arguments 的序列化长度（旧 `ToolUseBlock`）：70 / 3.5 = 20。
        let call = ToolCallRequest {
            id: "call_2".to_owned(),
            name: "Read".to_owned(),
            arguments: "y".repeat(70),
        };
        assert_eq!(
            estimate_tokens(
                &[ChatMessage::assistant_tool_calls("", vec![call])],
                TEXT_TOKEN_RATIO
            ),
            20
        );
        assert_eq!(estimate_tokens(&[], TEXT_TOKEN_RATIO), 0);
    }

    #[test]
    fn estimate_api_tokens_walks_the_block_branch_tree() {
        let messages = vec![
            // `content` 为字符串 → 整体按比率：7 / 3.5 = 2。
            json!({ "role": "user", "content": "直接字符串正文" }),
            json!({
                "role": "user",
                "content": [
                    { "type": "text", "text": "x".repeat(35) },
                    image_block(&"A".repeat(64)),
                    { "type": "tool_result", "content": jpeg_payload(50) },
                ],
            }),
        ];
        // 2（字符串） + 10（文本） + 64（图片 1:1） + 50（Base64 工具结果 1:1）。
        assert_eq!(estimate_api_tokens(&messages, TEXT_TOKEN_RATIO), 126);
    }

    // ==================== Phase 1：两遍清理与尾部保护 ====================

    /// 四条消息：两张整段 Base64 图 + 两段普通文本（尾部保护的观测点）。
    fn phase1_history() -> Vec<ChatMessage> {
        vec![
            ChatMessage::user(jpeg_payload(30_000)),
            ChatMessage::user("中间的普通文本"),
            ChatMessage::user(jpeg_payload(30_000)),
            ChatMessage::user("最后一条请求"),
        ]
    }

    #[test]
    fn phase1_is_inert_within_budget() {
        let messages = phase1_history();
        let result = enforce_phase1(&messages, i64::from(u32::MAX));
        assert!(!result.trimmed);
        assert_eq!(result.messages, messages);
        assert_eq!(result.tokens_before, result.tokens_after);
    }

    #[test]
    fn phase1_first_pass_protects_the_last_two_messages() {
        let messages = phase1_history();
        let before = estimate_tokens(&messages, TEXT_TOKEN_RATIO);
        let result = enforce_phase1(&messages, i64::from(before) - 1);
        assert!(result.trimmed);
        assert_eq!(result.tokens_before, before);
        assert!(result.tokens_after < result.tokens_before);
        // 首遍 protectTail = 2：idx0 被清理，idx2 / idx3 原样。
        assert_eq!(result.messages[0].content, PLACEHOLDER_30K);
        assert_eq!(result.messages[2], messages[2]);
        assert_eq!(result.messages[3], messages[3]);
        // 非图片消息即使落在清理区间内也原样透传。
        assert_eq!(result.messages[1], messages[1]);
    }

    #[test]
    fn phase1_second_pass_protects_only_the_last_message() {
        let messages = phase1_history();
        // 预算 100：首遍（残留 idx2 的整图）仍超限 → 次遍 protectTail = 1。
        let result = enforce_phase1(&messages, 100);
        assert!(result.trimmed);
        assert_eq!(result.messages[0].content, PLACEHOLDER_30K);
        assert_eq!(result.messages[2].content, PLACEHOLDER_30K);
        assert_eq!(result.messages[3], messages[3]);
    }

    #[test]
    fn phase1_never_touches_system_messages() {
        let system = ChatMessage::system(jpeg_payload(30_000));
        let messages = vec![
            system.clone(),
            ChatMessage::user(jpeg_payload(30_000)),
            ChatMessage::user("倒数第二条"),
            ChatMessage::user("最后一条"),
        ];
        let result = enforce_phase1(&messages, 10);
        assert!(result.trimmed);
        assert_eq!(result.messages[0], system);
        assert_eq!(result.messages[1].content, PLACEHOLDER_30K);
    }

    #[test]
    fn phase1_passes_through_non_image_history_verbatim() {
        let messages = vec![
            ChatMessage::user("普通问题".repeat(10)),
            ChatMessage::assistant("普通回答".repeat(10)),
            ChatMessage::tool("call_1", "普通工具结果".repeat(10)),
        ];
        // 预算 0 强制进入清理流程：无 Base64 时正文一字不改。
        let result = enforce_phase1(&messages, 0);
        assert!(result.trimmed);
        assert_eq!(result.messages, messages);
        assert_eq!(result.tokens_after, result.tokens_before);
    }

    #[test]
    fn phase1_strips_embedded_data_uri_payloads_only() {
        let content = format!(
            "先看这张 data:image/png;base64,{} 再回答我的问题",
            "A".repeat(5_000)
        );
        let messages = vec![ChatMessage::user(content), ChatMessage::user("追问")];
        let result = enforce_phase1(&messages, 0);
        // 旧 `ImageBlock` 是独立内容块：命中的整段 data URI（含 MIME 头）被占位
        // 替换，字符数按载荷长度上报——扫描口径与 `recovery::strip_data_uris` 同源。
        assert_eq!(
            result.messages[0].content,
            "先看这张 [image content removed - was 5000 chars] 再回答我的问题"
        );
    }

    #[test]
    fn phase1_rejects_out_of_range_ratio() {
        for ratio in [0.5_f64, 16.1, f64::NAN, f64::INFINITY, -1.0] {
            assert_eq!(
                enforce_phase1_with_ratio(&[], 0, ratio).unwrap_err(),
                ImageBudgetError::InvalidTextTokenRatio,
                "{ratio}"
            );
        }
        // 区间为左开右闭 `(0.5, 16.0]`。
        assert!(enforce_phase1_with_ratio(&[], 0, 16.0).is_ok());
        assert!(enforce_phase1_with_ratio(&[], 0, 0.500_001).is_ok());
    }

    // ==================== 指纹与去重 ====================

    #[test]
    fn image_hash_is_sha256_hex_of_the_decoded_bytes() {
        // "AAAA" → 0x00 0x00 0x00；"AQID" → 0x01 0x02 0x03（离线核算的 SHA-256）。
        assert_eq!(
            image_block_hash(&image_block("AAAA")).as_deref(),
            Some("709e80c88487a2411e1ee4dfb9f22a861492d20c4765150c0c794abd70f8147c")
        );
        assert_eq!(
            image_block_hash(&image_block("AQID")).as_deref(),
            Some("039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81")
        );
        // source 缺失 / data 空白 / Base64 非法 → None（旧 `return null`）。
        assert!(image_block_hash(&json!({ "type": "image" })).is_none());
        assert!(image_block_hash(&image_block("   ")).is_none());
        assert!(image_block_hash(&image_block("!!!!")).is_none());
    }

    #[test]
    fn collect_image_hashes_dedupes_identical_images() {
        let messages = vec![
            json!({
                "role": "user",
                "content": [image_block("AAAA"), { "type": "text", "text": "同一张图" }],
            }),
            json!({ "role": "user", "content": [image_block("AAAA")] }),
            json!({ "role": "user", "content": [image_block("AQID")] }),
            // 非图片块与非数组 content 不入集合。
            json!({ "role": "assistant", "content": "纯文本" }),
        ];
        let hashes = collect_image_hashes(&messages);
        assert_eq!(hashes.len(), 2);
        assert!(
            hashes.contains("709e80c88487a2411e1ee4dfb9f22a861492d20c4765150c0c794abd70f8147c")
        );
    }

    // ==================== 级联插桩点（feature 开关行为） ====================

    #[cfg(feature = "image-budget")]
    #[test]
    fn apply_cleans_history_base64_over_the_cascade_budget() {
        let mut messages = vec![
            ChatMessage::user(jpeg_payload(30_000)),
            ChatMessage::user("这是什么"),
            ChatMessage::user("再看一次"),
        ];
        // 30_000 / 3.5 = 8571 token > 7782 预算 → 触发首遍清理（protectTail = 2）。
        assert!(apply(&mut messages, UNKNOWN_MODEL));
        assert_eq!(messages[0].content, PLACEHOLDER_30K);
        assert_eq!(messages[1].content, "这是什么");
        assert_eq!(messages[2].content, "再看一次");
    }

    #[cfg(feature = "image-budget")]
    #[test]
    fn apply_is_inert_within_the_cascade_budget() {
        let original = vec![ChatMessage::user("你好"), ChatMessage::assistant("在")];
        let mut messages = original.clone();
        assert!(!apply(&mut messages, UNKNOWN_MODEL));
        assert_eq!(messages, original);
    }

    #[cfg(not(feature = "image-budget"))]
    #[test]
    fn apply_is_a_noop_when_the_feature_is_disabled() {
        let original = vec![
            ChatMessage::user(jpeg_payload(30_000)),
            ChatMessage::user("这是什么"),
        ];
        let mut messages = original.clone();
        // 空转分支：即使远超任何预算也不改写消息、不引入 image 依赖。
        assert!(!apply(&mut messages, UNKNOWN_MODEL));
        assert_eq!(messages, original);
    }

    // ==================== Phase 2：双闸门与缩略图 ====================

    /// 噪声图 fixture（非纯色，保证 JPEG 重编码后体积仍可观，梯度判定才有意义）。
    #[cfg(feature = "image-budget")]
    fn png_fixture(width: u32, height: u32) -> Vec<u8> {
        use std::io::Cursor;

        let buffer = image::RgbImage::from_fn(width, height, |x, y| {
            image::Rgb([
                u8::try_from(x % 251).unwrap_or_default(),
                u8::try_from(y % 241).unwrap_or_default(),
                u8::try_from((x ^ y) % 239).unwrap_or_default(),
            ])
        });
        let mut encoded = Vec::new();
        image::DynamicImage::ImageRgb8(buffer)
            .write_to(&mut Cursor::new(&mut encoded), image::ImageFormat::Png)
            .expect("PNG fixture 应可编码");
        encoded
    }

    /// 缩略图化后的图片块（走缺省 640 / q=60 两个旧参数）。
    #[cfg(feature = "image-budget")]
    fn thumbnail_of(block: &Value) -> Value {
        resize_image_block(block, THUMBNAIL_MAX_DIM, THUMBNAIL_QUALITY)
    }

    #[cfg(feature = "image-budget")]
    #[test]
    fn byte_gate_omits_images_over_ten_mebibytes() {
        let over = STANDARD.encode(vec![0_u8; MAX_DECODED_IMAGE_BYTES + 1]);
        assert_eq!(
            thumbnail_of(&image_block(&over)),
            omitted_image(REASON_OVERSIZE)
        );
        // 恰等于闸门值不触发字节闸门（严格大于），转而落入格式识别分支。
        let at_limit = STANDARD.encode(vec![0_u8; MAX_DECODED_IMAGE_BYTES]);
        assert_eq!(
            thumbnail_of(&image_block(&at_limit)),
            omitted_image(REASON_UNSUPPORTED_FORMAT)
        );
    }

    /// 1 × 1 的合法 GIF（帧数据完整）。
    #[cfg(feature = "image-budget")]
    fn gif_one_pixel() -> Vec<u8> {
        let mut gif_bytes = Vec::new();
        {
            let mut encoder = image::codecs::gif::GifEncoder::new(&mut gif_bytes);
            encoder
                .encode(&[1_u8, 2, 3], 1, 1, image::ExtendedColorType::Rgb8)
                .expect("GIF fixture 应可编码");
        }
        gif_bytes
    }

    /// 逻辑屏尺寸被改写的合法 GIF：把逻辑屏描述符的宽高（偏移 `6..10`，小端
    /// `u16`）改成给定值。头部即可取到宽高，无需真造 42.9 亿像素的位图——这正是
    /// 「像素闸门先于全解码」的语义。
    #[cfg(feature = "image-budget")]
    fn gif_with_logical_screen(width: u16, height: u16) -> Vec<u8> {
        let mut encoded = gif_one_pixel();
        encoded[6..8].copy_from_slice(&width.to_le_bytes());
        encoded[8..10].copy_from_slice(&height.to_le_bytes());
        encoded
    }

    #[cfg(feature = "image-budget")]
    #[test]
    fn pixel_gate_omits_images_over_forty_million_pixels() {
        // 逻辑屏 65535 × 65535 ≈ 42.9 亿像素，远超 4000 万闸门。
        assert!(u64::from(u16::MAX) * u64::from(u16::MAX) > MAX_IMAGE_PIXELS);
        let payload = STANDARD.encode(gif_with_logical_screen(u16::MAX, u16::MAX));
        assert_eq!(
            thumbnail_of(&image_block(&payload)),
            omitted_image(REASON_PIXEL_BUDGET)
        );
    }

    #[cfg(feature = "image-budget")]
    #[test]
    fn unreadable_payloads_fall_into_the_omission_branches() {
        // 无任何已知魔数 → 格式识别失败。
        assert_eq!(
            thumbnail_of(&image_block(&STANDARD.encode(b"not-an-image-at-all"))),
            omitted_image(REASON_UNSUPPORTED_FORMAT)
        );
        // 头部完整（1 × 1，过双闸门）但帧数据被截断 → 全解码失败。
        let complete = gif_one_pixel();
        let truncated = &complete[..complete.len() - 4];
        assert_eq!(
            thumbnail_of(&image_block(&STANDARD.encode(truncated))),
            omitted_image(REASON_DECODE_FAILED)
        );
        // Base64 本身非法 → 旧方法级 catch 的 `thumbnail generation failed`。
        assert_eq!(
            thumbnail_of(&image_block("!!!!")),
            omitted_image(REASON_THUMBNAIL_FAILED)
        );
        // source / data 缺失时整块原样拷贝。
        let no_source = json!({ "type": "image" });
        assert_eq!(thumbnail_of(&no_source), no_source);
    }

    #[cfg(feature = "image-budget")]
    #[test]
    fn thumbnail_downscales_to_640_and_reencodes_as_jpeg() {
        let block = image_block(&STANDARD.encode(png_fixture(800, 700)));
        let thumbnail = thumbnail_of(&block);
        let source = thumbnail.get("source").expect("缩略图应保留 source 对象");
        assert_eq!(source.get("media_type"), Some(&json!("image/jpeg")));
        // 旧算式：scale = min(640/800, 640/700) = 0.8 → 640 × 560。
        assert_eq!(
            thumbnail_dimensions(800, 700, THUMBNAIL_MAX_DIM),
            (640, 560)
        );
        let bytes = STANDARD
            .decode(
                source
                    .get("data")
                    .and_then(Value::as_str)
                    .expect("data 应为字符串"),
            )
            .expect("缩略图应为合法 Base64");
        let decoded = image::load_from_memory(&bytes).expect("缩略图应可解码");
        assert_eq!((decoded.width(), decoded.height()), (640, 560));
        // 宽高均 ≤ maxDim → 旧「已经够小」分支，整块原样返回。
        let small = image_block(&STANDARD.encode(png_fixture(120, 90)));
        assert_eq!(thumbnail_of(&small), small);
        // `max(1, (int)(v * scale))` 的下限保护与截断语义。
        assert_eq!(scaled_dimension(3, 0.1), 1);
        assert_eq!(scaled_dimension(1_000, 0.5), 500);
    }

    #[cfg(feature = "image-budget")]
    #[test]
    fn only_transient_image_hashes_are_downgraded() {
        let transient_data = STANDARD.encode(png_fixture(800, 700));
        let retained_data = STANDARD.encode(png_fixture(801, 701));
        let messages = vec![json!({
            "role": "user",
            "content": [image_block(&transient_data), image_block(&retained_data)],
        })];
        let mut transient = HashSet::new();
        transient.insert(image_block_hash(&image_block(&transient_data)).expect("指纹应可计算"));

        let reduced = replace_images_with_thumbnails(
            &messages,
            THUMBNAIL_MAX_DIM,
            THUMBNAIL_QUALITY,
            &transient,
        );
        let blocks = reduced[0]
            .get("content")
            .and_then(Value::as_array)
            .expect("内容应为块数组");
        // 命中指纹的被重编码为 JPEG；未命中的（已确认保留的历史图）一字不动。
        assert_eq!(
            blocks[0].get("source").and_then(|s| s.get("media_type")),
            Some(&json!("image/jpeg"))
        );
        assert_eq!(blocks[1], image_block(&retained_data));
    }

    // ==================== Phase 2：梯度降级链 ====================

    /// 单张 800 × 700 噪声图的 payload + 其指纹集合。
    #[cfg(feature = "image-budget")]
    fn transient_image_payload(width: u32, height: u32) -> (Vec<Value>, HashSet<String>) {
        let block = image_block(&STANDARD.encode(png_fixture(width, height)));
        let mut transient = HashSet::new();
        transient.insert(image_block_hash(&block).expect("指纹应可计算"));
        (
            vec![json!({ "role": "user", "content": [block] })],
            transient,
        )
    }

    #[cfg(feature = "image-budget")]
    #[tokio::test]
    async fn phase2_reports_no_reduction_within_budget() {
        let messages =
            vec![json!({ "role": "user", "content": [{ "type": "text", "text": "你好" }] })];
        let result = enforce_phase2(messages.clone(), 1_000, HashSet::new())
            .await
            .expect("Phase2 应成功");
        assert!(result.fits_budget);
        assert_eq!(result.reduction_summary, SUMMARY_NO_REDUCTION);
        assert_eq!(result.api_messages, messages);
        assert!(result.retained_image_hashes.is_empty());
        assert_eq!(result.input_budget, 1_000);
    }

    #[cfg(feature = "image-budget")]
    #[tokio::test]
    async fn phase2_walks_down_to_the_text_summary_strategy() {
        let (messages, transient) = transient_image_payload(800, 700);
        // 预算 200：缩略图（数十 KB Base64）仍超限 → 落到策略 2 的文本占位。
        let result = enforce_phase2(messages, 200, transient)
            .await
            .expect("Phase2 应成功");
        assert!(result.fits_budget);
        assert_eq!(
            result.reduction_summary,
            format!("{SUMMARY_STRATEGY_THUMBNAIL}{SUMMARY_STRATEGY_TEXT}")
        );
        assert_eq!(
            result.api_messages[0]["content"][0],
            json!({ "type": "text", "text": IMAGE_REPLACED_TEXT })
        );
        assert!(result.retained_image_hashes.is_empty());
    }

    #[cfg(feature = "image-budget")]
    #[tokio::test]
    async fn phase2_removes_all_images_and_flags_over_budget() {
        let block = image_block(&STANDARD.encode(png_fixture(800, 700)));
        let mut transient = HashSet::new();
        transient.insert(image_block_hash(&block).expect("指纹应可计算"));
        // 一段无法降级的大文本（10_000 字符 → 2857 token）令三级策略全部落不进预算。
        let messages = vec![json!({
            "role": "user",
            "content": [{ "type": "text", "text": "文".repeat(10_000) }, block],
        })];

        let result = enforce_phase2(messages, 100, transient)
            .await
            .expect("Phase2 应成功");
        assert!(!result.fits_budget);
        assert_eq!(
            result.reduction_summary,
            format!("{SUMMARY_STRATEGY_THUMBNAIL}{SUMMARY_STRATEGY_TEXT}{SUMMARY_STRATEGY_REMOVE}")
        );
        // 图片整块移除，仅剩文本块；估算 = 10_000 / 3.5 = 2857。
        let blocks = result.api_messages[0]["content"]
            .as_array()
            .expect("内容应为块数组");
        assert_eq!(blocks.len(), 1);
        assert_eq!(result.estimated_tokens, 2_857);
        assert!(result.retained_image_hashes.is_empty());
    }

    #[cfg(feature = "image-budget")]
    #[tokio::test]
    async fn phase2_rejects_out_of_range_ratio() {
        let error = enforce_phase2_with_ratio(Vec::new(), 0, HashSet::new(), 0.5)
            .await
            .expect_err("比率越界应被拒绝");
        assert_eq!(error, ImageBudgetError::InvalidTextTokenRatio);
        assert!(
            enforce_phase2_with_ratio(Vec::new(), 0, HashSet::new(), 16.0)
                .await
                .is_ok()
        );
    }

    #[cfg(feature = "image-budget")]
    #[tokio::test(flavor = "current_thread")]
    async fn phase2_offloads_cpu_work_off_the_async_runtime() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Duration;

        let ticks = Arc::new(AtomicU64::new(0));
        let heartbeat = tokio::spawn({
            let ticks = Arc::clone(&ticks);
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    ticks.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        // 单线程运行时：若解码 / 缩放 / JPEG 重编码在异步线程内联执行，心跳定时器
        // 在 Phase2 返回前一次也不会推进。
        let (messages, transient) = transient_image_payload(1_200, 900);
        let result = enforce_phase2(messages, 1, transient)
            .await
            .expect("Phase2 应成功");
        let observed = ticks.load(Ordering::Relaxed);
        heartbeat.abort();

        assert!(result.fits_budget);
        assert!(
            observed > 0,
            "spawn_blocking 未生效：Phase2 期间异步运行时被阻塞（心跳 {observed} 次）"
        );
    }
}
