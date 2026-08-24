//! SSE 流式解析 fixture 集成测试——字节流注入 [`sse_event_stream`]，零真实网络。
//!
//! 覆盖矩阵（任务规格第 7 节）：普通多块 delta 流、reasoning/content 混合流、
//! `finish_reason` 各取值归一化、`[DONE]` 终止、usage 尾块、任意粒度粘包切割、
//! 取消中途终止（无假 Finish）、HTTP 401/429/500 错误映射、流耗尽无 `[DONE]`
//! 容错、坏 chunk 宽容恢复。每条 fixture 断言事件序列**精确匹配**。

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use zk_llm::ProviderError;
use zk_llm::ProviderEvent;
use zk_llm::sse_event_stream;
use zk_protocol::model::Usage;

/// 将 SSE 文本按整块喂入，收集全部事件。
async fn run_fixture(fixture: &str) -> Vec<ProviderEvent> {
    let source = futures::stream::iter(vec![Ok::<_, ProviderError>(Bytes::from(
        fixture.to_owned(),
    ))]);
    sse_event_stream(source, CancellationToken::new())
        .collect::<Vec<_>>()
        .await
}

/// 将 SSE 文本按指定字节粒度切碎喂入（模拟 TCP/SSE 分块边界任意切割）。
async fn run_fixture_chunked(fixture: &str, chunk_size: usize) -> Vec<ProviderEvent> {
    let chunks: Vec<Result<Bytes, ProviderError>> = fixture
        .as_bytes()
        .chunks(chunk_size)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    sse_event_stream(futures::stream::iter(chunks), CancellationToken::new())
        .collect::<Vec<_>>()
        .await
}

fn usage(in_tokens: i64, out_tokens: i64) -> Usage {
    Usage {
        input_tokens: in_tokens,
        output_tokens: out_tokens,
        ..Usage::default()
    }
}

// ═══════════ Fixture 1：普通多块 delta 流 + finish + usage 尾块 + [DONE] ═══════════

const PLAIN_STREAM: &str = "\
data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"}}]}\n\
\n\
data: {\"choices\":[{\"delta\":{\"content\":\" wor\"}}]}\n\
\n\
data: {\"choices\":[{\"delta\":{\"content\":\"ld\"}}]}\n\
\n\
data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\
\n\
data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":5}}\n\
\n\
data: [DONE]\n\
\n";

#[tokio::test]
async fn plain_multichunk_delta_stream_matches_exactly() {
    let events = run_fixture(PLAIN_STREAM).await;
    assert_eq!(
        events,
        vec![
            ProviderEvent::TextDelta {
                text: "Hello".into()
            },
            ProviderEvent::TextDelta {
                text: " wor".into()
            },
            ProviderEvent::TextDelta { text: "ld".into() },
            ProviderEvent::Finish {
                finish_reason: zk_llm::FinishReason::EndTurn,
                usage: None,
            },
            ProviderEvent::UsageUpdate {
                usage: usage(12, 5)
            },
        ]
    );
    // 全零 usage 的尾块同样上报（对齐旧 Java：字段存在即发 MessageDelta）。
    let tail_only =
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0}}\n\n";
    assert_eq!(
        run_fixture(tail_only).await,
        vec![ProviderEvent::UsageUpdate {
            usage: Usage::default()
        }]
    );
}

// ═══════════ Fixture 2：reasoning_content 与 content 混合流 ═══════════

#[tokio::test]
async fn reasoning_and_content_mixed_stream_orders_thinking_first() {
    let fixture = concat!(
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"用户在问天气\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"，需要查表\",\"content\":\"正在\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"查询\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let events = run_fixture(fixture).await;
    assert_eq!(
        events,
        vec![
            ProviderEvent::ThinkingDelta {
                thinking: "用户在问天气".into()
            },
            // 同一 chunk 内先 Thinking 后 Text（对齐旧 processChunk 字段顺序）。
            ProviderEvent::ThinkingDelta {
                thinking: "，需要查表".into()
            },
            ProviderEvent::TextDelta {
                text: "正在".into()
            },
            ProviderEvent::TextDelta {
                text: "查询".into()
            },
            ProviderEvent::Finish {
                finish_reason: zk_llm::FinishReason::EndTurn,
                usage: None,
            },
        ]
    );
}

// ═══════════ Fixture 3：finish_reason 各取值归一化 ═══════════

#[tokio::test]
async fn finish_reason_values_normalize_to_unified_domain() {
    for (raw, expected) in [
        ("stop", zk_llm::FinishReason::EndTurn),
        ("length", zk_llm::FinishReason::MaxTokens),
        ("tool_calls", zk_llm::FinishReason::ToolUse),
        ("content_filter", zk_llm::FinishReason::ContentFilter),
        ("unknown_x", zk_llm::FinishReason::Other("unknown_x".into())),
    ] {
        let fixture = format!(
            "data: {{\"choices\":[{{\"delta\":{{}},\"finish_reason\":\"{raw}\"}}]}}\n\ndata: [DONE]\n\n"
        );
        let events = run_fixture(&fixture).await;
        assert_eq!(
            events,
            vec![ProviderEvent::Finish {
                finish_reason: expected.clone(),
                usage: None,
            }],
            "raw finish_reason `{raw}` must normalize to {expected:?}"
        );
    }
}

// ═══════════ Fixture 4：[DONE] 终止（含后续行不再消费）+ finish chunk 携带 usage ═══════════

#[tokio::test]
async fn done_marker_terminates_and_finish_chunk_may_carry_usage() {
    let fixture = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"NEVER\"}}]}\n\n",
    );
    let events = run_fixture(fixture).await;
    assert_eq!(
        events,
        vec![
            ProviderEvent::TextDelta { text: "a".into() },
            ProviderEvent::Finish {
                finish_reason: zk_llm::FinishReason::EndTurn,
                usage: Some(usage(3, 1)),
            },
        ]
    );
}

// ═══════════ Fixture 5：粘包 / 半行——任意粒度切割事件序列不变 ═══════════

#[tokio::test]
async fn arbitrary_byte_splitting_preserves_event_sequence() {
    let baseline = run_fixture(PLAIN_STREAM).await;
    // 覆盖 1 字节粒度（最细）、跨 JSON 边界粒度与大于整块粒度。
    for chunk_size in [1, 2, 3, 5, 8, 17, 64, 4096] {
        let events = run_fixture_chunked(PLAIN_STREAM, chunk_size).await;
        assert_eq!(
            events, baseline,
            "chunk_size = {chunk_size} must reassemble identically"
        );
    }
    // CRLF 行尾 + 跨块的孤立 \r\n 边界同样重组正确。
    let crlf_stream = PLAIN_STREAM.replace('\n', "\r\n");
    let events = run_fixture_chunked(&crlf_stream, 7).await;
    assert_eq!(events, baseline);
}

// ═══════════ Fixture 6：取消中途终止——不产 Finish、不产 Error、立即结束 ═══════════

#[tokio::test]
async fn cancellation_terminates_stream_without_finish_event() {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, ProviderError>>(16);
    let cancel = CancellationToken::new();
    let mut stream = Box::pin(sse_event_stream(tokio_stream_wrapper(rx), cancel.clone()));

    // 前两个 delta 正常消费。
    tx.send(Ok(Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"par\"}}]}\n\n",
    )))
    .await
    .unwrap();
    assert_eq!(
        stream.next().await,
        Some(ProviderEvent::TextDelta { text: "par".into() })
    );

    // 取消后：即使服务端继续推送 finish / [DONE]，消费端立即静默终止。
    cancel.cancel();
    tx.send(Ok(Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"tial\"}}]}\n\n",
    )))
    .await
    .unwrap();
    tx.send(Ok(Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    )))
    .await
    .unwrap();
    tx.send(Ok(Bytes::from_static(b"data: [DONE]\n\n")))
        .await
        .unwrap();

    assert_eq!(stream.next().await, None, "cancelled stream must end now");
    // 取消路径无 Finish / Error 事件进入序列：next() 直接返回 None 即证明
    // 积压事件被整体丢弃（futures::unfold 终止后不可重复 poll，不再追加
    // poll 断言）。
    drop(stream);
    drop(tx);
}

/// `mpsc::Receiver` → `Stream`（避免为测试引入 tokio-stream 依赖）。
fn tokio_stream_wrapper(
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, ProviderError>>,
) -> impl futures::Stream<Item = Result<Bytes, ProviderError>> {
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}

// ═══════════ Fixture 7：取消时积压的 Finish 绝不投递（防半条消息假成功） ═══════════

#[tokio::test]
async fn pending_finish_is_discarded_on_cancel() {
    // 同一 TCP 块携带 delta + finish：poll 出 delta 后立即取消，
    // 积压的 Finish 必须被丢弃。
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, ProviderError>>(16);
    let cancel = CancellationToken::new();
    let mut stream = Box::pin(sse_event_stream(tokio_stream_wrapper(rx), cancel.clone()));

    tx.send(Ok(Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\n",
    )))
    .await
    .unwrap();
    assert_eq!(
        stream.next().await,
        Some(ProviderEvent::TextDelta { text: "x".into() })
    );
    cancel.cancel();
    assert_eq!(stream.next().await, None, "pending Finish must be dropped");
}

// ═══════════ Fixture 8：HTTP 401 / 429 / 500 错误映射（字节源 Err 注入） ═══════════

#[tokio::test]
async fn http_error_status_maps_to_classified_error_and_terminates() {
    let cases = [
        (
            401u16,
            "Incorrect API key provided",
            None,
            false,
            "401 must be fatal",
        ),
        (
            429,
            "Rate limit reached",
            None,
            true,
            "429 must be retryable",
        ),
        (500, "internal", None, true, "5xx must be retryable"),
    ];
    for (status, message, retry_after, retryable, why) in cases {
        let source = futures::stream::iter(vec![Err(ProviderError::http(
            status,
            message.to_owned(),
            retry_after,
        ))]);
        let events = sse_event_stream(source, CancellationToken::new())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events.len(), 1, "{why}: exactly one Error event");
        match &events[0] {
            ProviderEvent::Error { error } => {
                assert_eq!(error.is_retryable(), retryable, "{why}");
                match error {
                    ProviderError::Http { status: s, .. } => assert_eq!(*s, status),
                    other => panic!("{why}: expected Http variant, got {other:?}"),
                }
            }
            other => panic!("{why}: expected Error event, got {other:?}"),
        }
    }
}

// ═══════════ Fixture 9：网络错误注入 → Error 事件 + 流终止 ═══════════

#[tokio::test]
async fn network_error_yields_single_retryable_error_event() {
    let source = futures::stream::iter(vec![
        Ok(Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"partial \"}}]}\n\n",
        )),
        Err(ProviderError::Network {
            message: "connection reset by peer".into(),
        }),
        Ok(Bytes::from_static(b"data: [DONE]\n\n")),
    ]);
    let events = sse_event_stream(source, CancellationToken::new())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        events,
        vec![
            ProviderEvent::TextDelta {
                text: "partial ".into()
            },
            ProviderEvent::Error {
                error: ProviderError::Network {
                    message: "connection reset by peer".into()
                },
            },
        ]
    );
    assert!(matches!(&events[1], ProviderEvent::Error { error } if error.is_retryable()));
}

// ═══════════ Fixture 10：流耗尽无 [DONE] 仍正常终止（旧 Java 容错语义） ═══════════

#[tokio::test]
async fn stream_end_without_done_marker_still_terminates_cleanly() {
    let fixture = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"tail\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n", // 末行无 [DONE]、无尾空行
    );
    let events = run_fixture(fixture).await;
    assert_eq!(
        events,
        vec![
            ProviderEvent::TextDelta {
                text: "tail".into()
            },
            ProviderEvent::Finish {
                finish_reason: zk_llm::FinishReason::EndTurn,
                usage: None,
            },
        ]
    );
}

// ═══════════ Fixture 11：坏 chunk 宽容恢复（对齐旧 processChunk） ═══════════

#[tokio::test]
async fn malformed_chunk_emits_error_then_recovers() {
    let fixture = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"ok-\"}}]}\n\n",
        ": keep-alive comment line\n\n",
        "data: {BROKEN json\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"recovered\"}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let events = run_fixture(fixture).await;
    assert_eq!(events.len(), 3, "exactly: TextDelta, Error, TextDelta");
    assert_eq!(events[0], ProviderEvent::TextDelta { text: "ok-".into() });
    assert!(matches!(&events[1], ProviderEvent::Error { error } if !error.is_retryable()));
    assert_eq!(
        events[2],
        ProviderEvent::TextDelta {
            text: "recovered".into()
        }
    );
}

// ═══════════ Fixture 12：惰性求值——构造后未 poll 不消费字节源 ═══════════

#[tokio::test]
async fn stream_is_lazy_until_first_poll() {
    let source = futures::stream::once(async {
        panic!("byte source must not be polled before the event stream is polled")
    })
    .map(|()| unreachable!());
    // 用 map 把 panic-once 流伪装成字节流；构造事件流本身不得触发它。
    let typed: futures::stream::Map<_, _> = source.map(|(): ()| Ok(Bytes::from_static(b"")));

    let _stream = sse_event_stream(typed, CancellationToken::new());
    // 构造完成即通过：惰性成立（poll panic 流则本测试失败）。
}

// ═══════════ Fixture 13：空字节流 → 零事件终止 ═══════════

#[tokio::test]
async fn empty_byte_stream_yields_no_events() {
    let events = run_fixture("").await;
    assert!(events.is_empty());
}

// ═══════════ 附：背压无害性——消费节奏远慢于生产，事件序列仍精确 ═══════════

#[tokio::test]
async fn slow_consumer_with_delay_preserves_sequence() {
    let source = futures::stream::iter(vec![
        Ok::<_, ProviderError>(Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n",
        )),
        Ok(Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n",
        )),
        Ok(Bytes::from_static(b"data: [DONE]\n\n")),
    ]);
    let mut stream = Box::pin(sse_event_stream(source, CancellationToken::new()));
    let mut collected = Vec::new();
    while let Some(event) = stream.next().await {
        collected.push(event);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        collected,
        vec![
            ProviderEvent::TextDelta { text: "a".into() },
            ProviderEvent::TextDelta { text: "b".into() },
        ]
    );
}
