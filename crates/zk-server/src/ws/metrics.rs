//! ws 打点辅助——metrics facade 的 WS 域序列注册（§17 可观测预埋）。
//!
//! 指标面（Prometheus 文本经 `/metrics` 暴露）：
//!
//! | 指标 | 类型 | 标签 | 语义 |
//! |---|---|---|---|
//! | `zk_ws_messages_total` | counter | `direction`（in/out）、`kind` | 每类消息收发计数 |
//! | `zk_ws_delta_dropped_total` | counter | — | delta 档丢弃数（背压满 / 无订阅者） |
//! | `zk_ws_critical_timeouts_total` | counter | — | critical 档 200ms 投递超时数 |
//! | `zk_ws_pending_replayed_total` | counter | — | bind 重放投递数 |
//! | `zk_ws_connections` | gauge | — | 当前连接数 |
//! | `zk_ws_pending_depth` | gauge | — | 全会话 critical pending 总深度 |
//!
//! 动态标签（direction/kind）与 S7 `record_request_metrics` 同模式：显式
//! `Key::from_parts` + `with_recorder`（`counter!` 宏的静态缓存臂不支持
//! 运行时标签值）。hub 内部另持原子计数器与打点同源（测试直读，避免对
//! 进程级 recorder 快照做脆弱断言）。

/// 消息收发计数（direction ∈ {in, out}；kind 为消息 type 字符串）。
pub(crate) fn count_message(direction: &str, kind: &str) {
    with_counter("zk_ws_messages_total", direction, kind, 1);
}

/// delta 档丢弃计数。
pub(crate) fn count_delta_dropped() {
    with_counter("zk_ws_delta_dropped_total", "", "", 1);
}

/// critical 档投递超时计数。
pub(crate) fn count_critical_timeout() {
    with_counter("zk_ws_critical_timeouts_total", "", "", 1);
}

/// pending 重放投递计数。
pub(crate) fn count_pending_replayed(count: u64) {
    with_counter("zk_ws_pending_replayed_total", "", "", count);
}

/// 连接数 gauge（absolute set）。
pub(crate) fn set_connections(value: u64) {
    let metadata = metrics::Metadata::new(
        env!("CARGO_CRATE_NAME"),
        metrics::Level::INFO,
        Some(module_path!()),
    );
    metrics::with_recorder(|recorder| {
        let key = metrics::Key::from_name("zk_ws_connections");
        recorder
            .register_gauge(&key, &metadata)
            .set(f64::from(u32::try_from(value).unwrap_or(u32::MAX)));
    });
}

/// pending 总深度 gauge（absolute set）。
pub(crate) fn set_pending_depth(value: u64) {
    let metadata = metrics::Metadata::new(
        env!("CARGO_CRATE_NAME"),
        metrics::Level::INFO,
        Some(module_path!()),
    );
    metrics::with_recorder(|recorder| {
        let key = metrics::Key::from_name("zk_ws_pending_depth");
        recorder
            .register_gauge(&key, &metadata)
            .set(f64::from(u32::try_from(value).unwrap_or(u32::MAX)));
    });
}

/// 计数器注册并递增（标签为空时走单序列）。
fn with_counter(name: &str, direction: &str, kind: &str, delta: u64) {
    let metadata = metrics::Metadata::new(
        env!("CARGO_CRATE_NAME"),
        metrics::Level::INFO,
        Some(module_path!()),
    );
    metrics::with_recorder(|recorder| {
        let key = if direction.is_empty() {
            metrics::Key::from_name(name.to_owned())
        } else {
            metrics::Key::from_parts(
                name.to_owned(),
                vec![
                    metrics::Label::new("direction", direction.to_owned()),
                    metrics::Label::new("kind", kind.to_owned()),
                ],
            )
        };
        recorder.register_counter(&key, &metadata).increment(delta);
    });
}
