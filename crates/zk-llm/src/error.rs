//! zk-llm 错误类型——可重试 / 致命二分与分类语义。

/// LLM 提供商调用错误。
///
/// 可重试分类对齐旧 `LlmApiException` 语义（`OpenAiCompatibleProvider.
/// handleErrorResponse`：`code == 429 || code >= 500` 为可重试；`IOException` /
/// 流读错误为可重试；解析与请求构造为致命）：
///
/// - **可重试**（[`ProviderError::is_retryable`] == `true`）：HTTP 429 / 5xx、
///   网络层失败（连接失败、连接/读超时，[`ProviderError::Network`]）。
/// - **致命**：HTTP 401 / 403 / 400 等请求侧错误、SSE chunk 解析失败
///   （[`ProviderError::Parse`]）、配置错误（[`ProviderError::Config`]）。
///
/// # 密钥安全不变量
///
/// 所有变体的 `Display` 输出**不得包含 API key 明文**：网络错误消息来自
/// reqwest 错误（只含 URL 与原因，key 在 `Authorization` 请求头中，不进入
/// 错误链）；HTTP 错误消息取自服务端响应体 `error.message`；其余变体消息
/// 均为本地静态描述。新增错误来源时必须维持该不变量。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProviderError {
    /// HTTP 响应状态非 2xx。
    ///
    /// `retryable` 由状态码推断（429 / 5xx 为 `true`，对齐旧
    /// `handleErrorResponse`）；`retry_after_ms` 仅 429 且响应携带数字型
    /// `Retry-After` 头时为 `Some`（HTTP-date 形式不解析，对齐旧实现）。
    #[error("provider http error ({status}): {message}")]
    Http {
        /// HTTP 状态码。
        status: u16,
        /// 服务端错误消息（响应体 `error.message`，缺失时为 `HTTP {status}`）。
        message: String,
        /// 重试等待毫秒（仅 429 且有数字型 `Retry-After` 时存在）。
        retry_after_ms: Option<u64>,
        /// 是否可重试（429 / 5xx 为 `true`）。
        retryable: bool,
    },
    /// 网络层错误：连接失败、连接超时、读超时、流中断。
    ///
    /// 恒为可重试（对齐旧 Java：`OkHttp` `IOException` → `retryable = true`）。
    #[error("provider network error: {message}")]
    Network {
        /// reqwest 错误描述（不含 API key——见模块文档密钥安全不变量）。
        message: String,
    },
    /// 请求在完成前被协作取消。
    ///
    /// 注意：取消路径由 [`crate::provider`] 的流层静默处理（直接终止流、
    /// 不产出 [`crate::provider::ProviderEvent::Error`]），本变体保留给
    /// 需要显式区分取消语义的调用方（如建立期失败诊断）。
    #[error("provider call cancelled")]
    Cancelled,
    /// SSE chunk 或响应体解析失败（致命——不重试）。
    #[error("provider parse error: {message}")]
    Parse {
        /// 解析失败描述（含 serde 错误信息，不含请求体全文）。
        message: String,
    },
    /// 提供商配置错误：`base_url` 非法、请求体无法构建等（致命）。
    #[error("provider configuration error: {message}")]
    Config {
        /// 配置问题描述（不含密钥）。
        message: String,
    },
}

impl ProviderError {
    /// 按旧 `handleErrorResponse` 语义从 HTTP 状态构造错误。
    ///
    /// `retryable` 规则：`status == 429 || status >= 500`；`retry_after_ms`
    /// 由调用方仅在 429 且能解析出数字型 `Retry-After` 时传入。
    #[must_use]
    pub fn http(status: u16, message: String, retry_after_ms: Option<u64>) -> Self {
        Self::Http {
            status,
            message,
            retry_after_ms,
            retryable: status == 429 || status >= 500,
        }
    }

    /// 是否可重试（超时 / 429 / 5xx / 连接失败 → `true`；401 / 403 / 400 /
    /// 解析错误 / 取消 / 配置错误 → `false`）。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http { retryable, .. } => *retryable,
            Self::Network { .. } => true,
            Self::Cancelled | Self::Parse { .. } | Self::Config { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_retryable_classification_matches_legacy() {
        // 对齐旧 handleErrorResponse：429 / 5xx 可重试；401 / 403 / 400 / 404 致命。
        for status in [429u16, 500, 502, 503, 529] {
            let err = ProviderError::http(status, format!("HTTP {status}"), None);
            assert!(err.is_retryable(), "status {status} should be retryable");
        }
        for status in [400u16, 401, 403, 404, 422] {
            let err = ProviderError::http(status, format!("HTTP {status}"), None);
            assert!(!err.is_retryable(), "status {status} should be fatal");
        }
    }

    #[test]
    fn category_retryable_flags() {
        assert!(
            ProviderError::Network {
                message: "connect timeout".into()
            }
            .is_retryable()
        );
        assert!(
            !ProviderError::Parse {
                message: "bad json".into()
            }
            .is_retryable()
        );
        assert!(
            !ProviderError::Config {
                message: "no api key".into()
            }
            .is_retryable()
        );
        assert!(!ProviderError::Cancelled.is_retryable());
    }

    #[test]
    fn display_carries_status_and_message() {
        let err = ProviderError::http(429, "Requests rate limit exceeded".into(), Some(1200));
        let rendered = err.to_string();
        assert!(rendered.contains("429"));
        assert!(rendered.contains("Requests rate limit exceeded"));
    }
}
