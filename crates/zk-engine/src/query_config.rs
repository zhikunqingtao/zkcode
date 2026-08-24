//! 查询预算与费用策略——输出 token 预算档位、截断恢复常量、用量成本换算。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `engine/QueryConfig.java`：`DEFAULT_MAX_TOKENS` / `ESCALATED_MAX_TOKENS`
//!   / `MAX_OUTPUT_TOKENS_RECOVERY_LIMIT` 与 `getRecommendedMaxTokens`；
//! - `engine/QueryEngine.java` L63-67：`MAX_TOKENS_RECOVERY_MESSAGE`；
//! - `service/CostTrackerService.java` L44-49：`recordUsage` 的费用公式。
//!
//! 层次归属：模型能力表在 zk-llm（[`zk_llm::capabilities_for`]），预算档位与
//! 费用策略属引擎决策，故落在 zk-engine——依赖方向仍为
//! `zk-engine → zk-llm`，不反向。

use zk_protocol::model::Usage;

/// 默认最大输出 token（逐字对照旧 `QueryConfig.DEFAULT_MAX_TOKENS`）。
///
/// 仅作为「模型未知 / 注册表无有效值」时的回落值；正常路径由
/// [`recommended_max_tokens`] 按模型能力表取值。
pub const DEFAULT_MAX_TOKENS: u32 = 8192;

/// 升级档最大输出 token（逐字对照旧 `QueryConfig.ESCALATED_MAX_TOKENS`）。
///
/// 双重语义（与旧实现一致）：既是 [`recommended_max_tokens`] 的上限夹紧值，
/// 也是截断恢复首次升级的目标值。
pub const ESCALATED_MAX_TOKENS: u32 = 65536;

/// `max_tokens` 截断恢复次数上限（逐字对照旧
/// `QueryConfig.MAX_OUTPUT_TOKENS_RECOVERY_LIMIT`）。
pub const MAX_OUTPUT_TOKENS_RECOVERY_LIMIT: u32 = 3;

/// 截断续写提示（逐字对照旧 `QueryEngine.MAX_TOKENS_RECOVERY_MESSAGE`
/// L63-67；分段拼接与旧字面量拼接一一对应）。
pub const MAX_TOKENS_RECOVERY_MESSAGE: &str = concat!(
    "Output token limit hit. Resume directly — no apology, ",
    "no recap of what you were doing. Pick up mid-thought if ",
    "that is where the cut happened. Break remaining work ",
    "into smaller pieces."
);

/// 按模型推荐的最大输出 token（逐条对照旧
/// `QueryConfig.getRecommendedMaxTokens(registry, model)`）。
///
/// 判定序：模型名空白 → [`DEFAULT_MAX_TOKENS`]（对应旧
/// `registry == null || model.isBlank()`）；能力表输出上限 `<= 0` →
/// [`DEFAULT_MAX_TOKENS`]；否则 `min(maxOutputTokens, ESCALATED_MAX_TOKENS)`。
///
/// 例：`kimi-k3` 能力表输出上限 131072 → 夹紧为 65536（旧同值）；未知模型
/// 走 [`zk_llm::DEFAULT_CAPABILITIES`] 的 4096（旧 `ModelCapabilities.DEFAULT`
/// 同值）。
#[must_use]
pub fn recommended_max_tokens(model: &str) -> u32 {
    if model.trim().is_empty() {
        return DEFAULT_MAX_TOKENS;
    }
    let max_output = zk_llm::max_output_tokens_for(model);
    if max_output == 0 {
        return DEFAULT_MAX_TOKENS;
    }
    max_output.min(ESCALATED_MAX_TOKENS)
}

/// 一次用量的美元成本（逐行对照旧 `CostTrackerService.recordUsage` L45-49）。
///
/// `inputCost + outputCost − cacheDiscount`，其中
/// `cacheDiscount = cacheReadInputTokens × costPer1kInput × 0.9 / 1000`
/// （缓存读 token 按 9 折折让，与旧实现同式同序）。费率取自
/// [`zk_llm::capabilities_for`]，未知模型费率为 0 → 成本 0。
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    reason = "token 计数量级远低于 f64 精确整数上界；旧实现同为 int×double 提升"
)]
pub fn usage_cost_usd(model: &str, usage: &Usage) -> f64 {
    let caps = zk_llm::capabilities_for(model);
    let input_cost = usage.input_tokens as f64 * caps.cost_per_1k_input / 1000.0;
    let output_cost = usage.output_tokens as f64 * caps.cost_per_1k_output / 1000.0;
    let cache_discount =
        usage.cache_read_input_tokens as f64 * caps.cost_per_1k_input * 0.9 / 1000.0;
    input_cost + output_cost - cache_discount
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommended_max_tokens_clamps_to_escalated_ceiling() {
        // kimi-k3 能力表 131072 → min(131072, 65536) = 65536（旧同值）。
        assert_eq!(recommended_max_tokens("kimi-k3"), ESCALATED_MAX_TOKENS);
        // 恰好等于上限的模型原值透传。
        assert_eq!(recommended_max_tokens("qwen3.7-max"), 65536);
        // 低于上限的模型原值透传（不再被 8192 默认档压低/抬高）。
        assert_eq!(recommended_max_tokens("claude-sonnet-4-6"), 16384);
        assert_eq!(recommended_max_tokens("moonshot-v1-128k"), 8192);
    }

    #[test]
    fn recommended_max_tokens_falls_back_for_blank_and_unknown() {
        // 旧 `registry == null || model.isBlank()` 分支 → DEFAULT_MAX_TOKENS。
        assert_eq!(recommended_max_tokens(""), DEFAULT_MAX_TOKENS);
        assert_eq!(recommended_max_tokens("   "), DEFAULT_MAX_TOKENS);
        // 未知模型走能力表默认值 4096（旧 ModelCapabilities.DEFAULT 同值）。
        assert_eq!(recommended_max_tokens("nope-9000"), 4096);
    }

    #[test]
    fn usage_cost_matches_legacy_formula() {
        // kimi-k3：0.002 / 1k 输入、0.012 / 1k 输出、缓存读 9 折折让。
        let usage = Usage {
            input_tokens: 10_000,
            output_tokens: 2_000,
            cache_read_input_tokens: 4_000,
            cache_creation_input_tokens: 0,
        };
        let expected =
            10_000.0 * 0.002 / 1000.0 + 2_000.0 * 0.012 / 1000.0 - 4_000.0 * 0.002 * 0.9 / 1000.0;
        let actual = usage_cost_usd("kimi-k3", &usage);
        assert!((actual - expected).abs() < 1e-12, "{actual} vs {expected}");
        // 未知模型费率为 0 → 成本 0。
        assert!(usage_cost_usd("nope-9000", &usage).abs() < f64::EPSILON);
    }

    #[test]
    fn recovery_message_matches_legacy_text() {
        assert!(MAX_TOKENS_RECOVERY_MESSAGE.starts_with("Output token limit hit."));
        assert!(MAX_TOKENS_RECOVERY_MESSAGE.ends_with("into smaller pieces."));
        assert!(MAX_TOKENS_RECOVERY_MESSAGE.contains("Pick up mid-thought"));
    }
}
