//! 真实连通验证（默认 `#[ignore]`，人工运行）。
//!
//! 密钥一律从环境变量注入（旧仓库 `.env` 的 `LLM_PROVIDER_*` 经 shell
//! `set -a; source .env; set +a` 注入），**不落代码 / 不打印明文**。缺失对应
//! `API_KEY` 时用例直接跳过（视为通过），因此 CI 无密钥环境不受影响。
//!
//! 运行示例（在 zkcode 根目录）：
//! ```bash
//! set -a; source /path/to/zhikuncode/.env; set +a
//! cargo test -p zk-llm --test real_connectivity -- --ignored --nocapture
//! ```
//!
//! 模型名可用 `ZK_REAL_<PROVIDER>_MODEL` 覆盖（目录默认为前向占位名，如
//! `kimi-k3`；如需对真实端点连通可覆盖为端点当前可用模型）。

use std::time::Duration;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use zk_llm::{
    ApiKey, ChatMessage, ChatProvider, ChatRequest, OpenAiCompatProvider, ProviderConfig,
    ProviderEvent,
};

/// 单次真实流式对话的事件统计（不含任何密钥 / 敏感字段）。
#[derive(Default, Debug)]
struct Tally {
    text_deltas: usize,
    text_len: usize,
    thinking_deltas: usize,
    tool_starts: usize,
    finish: Option<String>,
    error: Option<String>,
}

/// 驱动一次真实流式请求并汇总事件（30s 上限；不打印密钥）。
async fn drive(provider: &dyn ChatProvider, model: &str) -> Tally {
    let request = ChatRequest::new(model)
        .with_system_prompt(Some("You are terse.".to_owned()))
        .with_message(ChatMessage::user("Reply with exactly one word: pong"))
        .with_max_tokens(32);
    let cancel = CancellationToken::new();
    let mut tally = Tally::default();
    let stream = match provider.chat_stream(request, cancel) {
        Ok(stream) => stream,
        Err(err) => {
            tally.error = Some(format!("setup: {err}"));
            return tally;
        }
    };
    let mut stream = stream;
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            () = &mut deadline => {
                tally.error.get_or_insert_with(|| "timeout after 30s".to_owned());
                break;
            }
            next = stream.next() => {
                let Some(event) = next else { break };
                match event {
                    ProviderEvent::TextDelta { text } => {
                        tally.text_deltas += 1;
                        tally.text_len += text.len();
                    }
                    ProviderEvent::ThinkingDelta { .. } => tally.thinking_deltas += 1,
                    ProviderEvent::ToolUseStart { .. } => tally.tool_starts += 1,
                    ProviderEvent::ToolInputDelta { .. } | ProviderEvent::UsageUpdate { .. } => {}
                    ProviderEvent::Finish { finish_reason, .. } => {
                        tally.finish = Some(finish_reason.as_str().to_owned());
                    }
                    ProviderEvent::Error { error } => {
                        // ProviderError 的 Display 不含密钥（Secret 脱敏）。
                        tally.error = Some(error.to_string());
                    }
                }
            }
        }
    }
    tally
}

/// 通用连通探针：读 `key_env`，缺失则跳过；否则真实请求并断言到达终态。
async fn probe(name: &str, base_url: &str, key_env: &str, model_env: &str, default_model: &str) {
    let Ok(key) = std::env::var(key_env) else {
        eprintln!("[skip] {name}: {key_env} not set");
        return;
    };
    if key.trim().is_empty() {
        eprintln!("[skip] {name}: {key_env} is blank");
        return;
    }
    let model = std::env::var(model_env).unwrap_or_else(|_| default_model.to_owned());
    let config = ProviderConfig::new(
        name,
        base_url,
        ApiKey::new(key),
        model.clone(),
        vec![model.clone()],
    );
    let provider = OpenAiCompatProvider::new(config).expect("provider builds");
    let tally = drive(&provider, &model).await;
    eprintln!(
        "[{name}] model={model} text_deltas={} text_len={} thinking={} tool_starts={} finish={:?} error={:?}",
        tally.text_deltas,
        tally.text_len,
        tally.thinking_deltas,
        tally.tool_starts,
        tally.finish,
        tally.error
    );
    // 真实冒烟必须证明凭据、模型和流式响应均可用；provider 错误只能证明端点可达，
    // 不能作为成功结果。
    assert!(
        tally.error.is_none(),
        "{name}: provider returned an error: {:?}",
        tally.error
    );
    assert!(tally.text_deltas > 0, "{name}: response contained no text");
    assert!(
        tally.finish.is_some(),
        "{name}: stream ended without finish"
    );
}

#[tokio::test]
#[ignore = "requires real LLM_PROVIDER_MOONSHOT_API_KEY"]
async fn moonshot_kimi_live() {
    probe(
        "moonshot",
        zk_llm::config::MOONSHOT_BASE_URL,
        "LLM_PROVIDER_MOONSHOT_API_KEY",
        "ZK_REAL_MOONSHOT_MODEL",
        "kimi-k3",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires real LLM_PROVIDER_DEEPSEEK_API_KEY"]
async fn deepseek_live() {
    probe(
        "deepseek",
        zk_llm::config::DEEPSEEK_BASE_URL,
        "LLM_PROVIDER_DEEPSEEK_API_KEY",
        "ZK_REAL_DEEPSEEK_MODEL",
        "deepseek-v4-pro",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires real LLM_PROVIDER_DASHSCOPE_API_KEY"]
async fn dashscope_live() {
    probe(
        "dashscope",
        zk_llm::config::DASHSCOPE_BASE_URL,
        "LLM_PROVIDER_DASHSCOPE_API_KEY",
        "ZK_REAL_DASHSCOPE_MODEL",
        "qwen3.7-max",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires real LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY"]
async fn dashscope_token_plan_live() {
    probe(
        "dashscope-token-plan",
        zk_llm::config::DASHSCOPE_TOKEN_PLAN_BASE_URL,
        "LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY",
        "ZK_REAL_DASHSCOPE_TOKEN_PLAN_MODEL",
        "qwen3.8-max",
    )
    .await;
}
