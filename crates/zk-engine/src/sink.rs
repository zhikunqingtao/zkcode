//! 下行推送窄接口——引擎与 WS 通道解耦点（依赖方向铁律的实现载体）。

use futures::future::BoxFuture;
use zk_protocol::ServerMessage;

/// 会话级下行消息出口。
///
/// zk-server 侧由 `WsHub` 实现（`hub.push` 负责信封组装 / seq 分配 /
/// critical 暂存语义）；单测侧以录制桩实现。
///
/// # 形态裁决（D-S9-3）
///
/// `push` 返回 [`BoxFuture`] 而非同步签名：`WsHub::push` 是 async fn，
/// 若在适配层以 `tokio::spawn` 逐条转发，deltas 之间将失去顺序保证
/// （spawn 调度顺序不定）；引擎逐条 `await` 即可保序，trait 仍保持
/// object-safe。
pub trait MessageSink: Send + Sync {
    /// 向指定会话推送一条下行消息（推送失败由通道层自行吞吸，永不上抛）。
    fn push<'a>(&'a self, session_id: &'a str, message: ServerMessage) -> BoxFuture<'a, ()>;
}
