//! 敏感信息过滤器——逐字移植 `security/SensitiveDataFilter.java`（92 行）。
//!
//! 授权链在两处消费它：`OperationAnalyzerRegistry` 的 `redactedSummary`
//! （进 `permission_request` 的 `inputSummary`，用户可见）与 endpoints 脱敏
//! （进 `operationHash` 的事实集）。因此本模块的输出必须与旧源**逐字节**一致，
//! 否则同一操作在两代实现下的 `operationHash` 不同、免弹缓存失效。
//!
//! # Java 正则语义的精确保留
//!
//! - 16 条模式按**声明序**串行 `replaceAll`（后一条在前一条的输出上跑）；
//! - 替换体取 `group(1)`：缺组（PEM 无前缀分支）→ `***REDACTED***`；
//! - 前缀规则 `indexOf('_')`、`substring` 与 `length()` 全部是 **UTF-16 码元**
//!   语义（旧源 `token.length()` 进了替换文本，必须同长），故本实现在 `Vec<u16>`
//!   上做下标运算而非 Rust 的字节/字符下标。

use std::sync::LazyLock;

use regex::{Captures, Regex};

use crate::tool_facts::SensitiveDataFilterPort;

/// 旧源 `REDACTED`（L35）。
const REDACTED: &str = "***REDACTED***";

/// 旧源 `PATTERNS`（L37-66，16 条，声明序即执行序）。
///
/// 与旧源逐条对应；Java 的 `(?i)` 内联标志、非捕获组 `(?:...)`、可选组
/// `(RSA |EC |DSA |OPENSSH )?` 在 `regex` crate 中语义一致。
static PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        // OpenAI API Key: 新格式 sk-proj-xxx (含下划线和连字符)
        r"(sk-proj-[a-zA-Z0-9_-]{20,})",
        // OpenAI API Key: 通用格式 sk-xxx (字母数字+下划线+连字符, 20+字符)
        r"(sk-[a-zA-Z0-9_-]{20,})",
        // AWS Access Key ID
        r"(AKIA[0-9A-Z]{16})",
        // GitHub Personal Access Token
        r"(ghp_[a-zA-Z0-9]{36})",
        // GitLab Personal Access Token
        r"(glpat-[a-zA-Z0-9_-]{20,})",
        // Anthropic API Key
        r"(sk-ant-[a-zA-Z0-9_-]{20,})",
        // Slack Token
        r"(xox[bpsar]-[a-zA-Z0-9-]{10,})",
        // Generic key=value secrets — 字段名为非捕获组，让密钥成为 group(1)
        r#"(?i)(?:api[_-]?key|secret|password|token|auth|credential)\s*[=:]\s*['"]?([^\s'"]{8,})"#,
        // JWT Token
        r"(eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,})",
        // PEM Private Key header
        r"-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----",
        // Connection strings with passwords — 协议名为非捕获组，让密码成为 group(1)
        r"(?i)(?:mongodb|postgres|mysql|redis)://[^:]+:([^@]{4,})@",
        // HuggingFace Token
        r"(hf_[a-zA-Z0-9]{20,})",
        // GCP API Key
        r"(AIza[a-zA-Z0-9_\-]{35})",
        // Stripe Secret Key
        r"(sk_live_[a-zA-Z0-9]{24,})",
        // Vercel Token
        r"(vercel_[a-zA-Z0-9_]{24,})",
        // Supabase Service Key
        r"(sbp_[a-zA-Z0-9]{40,})",
    ]
    .into_iter()
    .map(|pattern| Regex::new(pattern).expect("static sensitive data pattern"))
    .collect()
});

/// 敏感信息过滤器（旧 `SensitiveDataFilter`）。
#[derive(Debug, Clone, Copy, Default)]
pub struct SensitiveDataFilter;

impl SensitiveDataFilter {
    /// 旧源 `filter(String)`（L73-91）。
    ///
    /// 旧源为每条模式单独 `try { ... } catch (IllegalArgumentException)`——那是
    /// `Matcher.quoteReplacement` 的替换语法异常防御；本实现的替换体是纯字符串
    /// 拼接、不存在替换语法，故无对应 catch（偏离 S-01，EQUIVALENT）。
    #[must_use]
    pub fn filter(content: &str) -> String {
        let mut result = content.to_owned();
        for pattern in PATTERNS.iter() {
            result = pattern
                .replace_all(&result, |caps: &Captures<'_>| Self::redact(caps.get(1)))
                .into_owned();
        }
        result
    }

    /// 旧源替换体（L78-86）：前缀保留 + 长度指纹。
    fn redact(token: Option<regex::Match<'_>>) -> String {
        // 旧源 L80：`if (token == null) return quoteReplacement(REDACTED);`
        let Some(token) = token else {
            return REDACTED.to_owned();
        };
        // 旧源 L81-85 全程是 UTF-16 下标运算（`indexOf` / `substring` /
        // `length`），故在码元序列上复刻。
        let units: Vec<u16> = token.as_str().encode_utf16().collect();
        let underscore_idx = units.iter().position(|unit| *unit == u16::from(b'_'));
        let prefix_len = match underscore_idx {
            // 旧源 L82-84：`underscoreIdx > 0 && underscoreIdx < 8`。
            Some(idx) if idx > 0 && idx < 8 => idx + 1,
            _ => units.len().min(4),
        };
        let prefix = String::from_utf16_lossy(&units[..prefix_len]);
        format!("{prefix}***REDACTED-{}***", units.len())
    }
}

impl SensitiveDataFilterPort for SensitiveDataFilter {
    fn filter(&self, value: &str) -> String {
        Self::filter(value)
    }
}

#[cfg(test)]
mod tests {
    use super::SensitiveDataFilter as F;

    /// 旧源 L39-40：`sk-` 通用格式 → 前 4 字符 + 长度指纹。
    #[test]
    fn openai_key_keeps_four_char_prefix() {
        let out = F::filter("export OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwx");
        assert!(out.contains("sk-a***REDACTED-"), "{out}");
        assert!(!out.contains("mnopqrstuvwx"), "{out}");
    }

    /// 旧源 L82-84：下划线在 (0, 8) 区间 → 前缀含下划线本身。
    #[test]
    fn underscore_prefix_is_kept_inclusive() {
        let token = format!("ghp_{}", "a".repeat(36));
        let out = F::filter(&token);
        assert_eq!(out, format!("ghp_***REDACTED-{}***", token.len()));
    }

    /// 旧源 L58 + L80：PEM 无前缀分支 `group(1) == null` → 整段 `***REDACTED***`。
    #[test]
    fn pem_header_without_algorithm_uses_plain_redaction() {
        assert_eq!(
            F::filter("-----BEGIN PRIVATE KEY-----"), // gitleaks:allow -- detector regression fixture
            "***REDACTED***",
            "group(1) 缺失走 L80 分支"
        );
    }

    /// 旧源 L58：`RSA ` 前缀被 `group(1)` 捕获 → 走长度指纹分支（含尾空格）。
    #[test]
    fn pem_header_with_algorithm_uses_length_fingerprint() {
        assert_eq!(
            F::filter("-----BEGIN RSA PRIVATE KEY-----"), // gitleaks:allow -- detector regression fixture
            "RSA ***REDACTED-4***"
        );
    }

    /// 旧源 L52-53 + L88：`key=value` 通用凭证。
    ///
    /// Java `Matcher#replaceAll` 替换的是**整个匹配**（字段名与分隔符一并吞掉），
    /// `group(1)` 只决定替换文本的前缀与长度指纹——此处逐字保留该行为。
    #[test]
    fn generic_key_value_consumes_whole_match() {
        assert_eq!(
            F::filter("password: hunter2hunter2"),
            "hunt***REDACTED-14***"
        );
    }

    /// 旧源 L64-65：连接串模式同样吞掉整个匹配（含 `协议://用户:` 与尾 `@`）。
    #[test]
    fn connection_string_consumes_whole_match() {
        assert_eq!(
            F::filter("postgres://admin:s3cretpass@db:5432/app"),
            "s3cr***REDACTED-10***db:5432/app"
        );
    }

    /// 无敏感内容原样返回（旧源逐模式 `replaceAll` 无匹配即恒等）。
    #[test]
    fn clean_text_is_returned_verbatim() {
        assert_eq!(F::filter("ls -la /tmp"), "ls -la /tmp");
    }
}
