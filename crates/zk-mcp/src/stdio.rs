//! MCP STDIO 传输 — 子进程 stdin/stdout 的 JSON-RPC 行协议。
//!
//! 对照 Java 基线 `mcp/McpStdioTransport.java`（174 行）：`ProcessBuilder` →
//! [`tokio::process::Command`]；请求 id 自增（从 1 起）；stdin 写
//! `json + "\n"` 并 flush；stdout 逐行读；`isConnected` = 连接标志 ∧ 进程存活；
//! 关闭序列 destroy → 5s → destroyForcibly。
//!
//! STDIO **不支持重连**：进程死亡即不可恢复（`McpClientManager` 的健康检查对
//! `STDIO` 类型不触发退避重连，对照 Java `healthCheck` 的 `type != STDIO` 判定）。
//!
//! 偏离（长期正确方案，均为 Java 侧缺陷的修正）：
//! 1. **响应按 id 匹配**。Java `readResponseWithTimeout` 取到的**第一行**即视为
//!    本次请求的响应，不校验 id——服务端主动推送的通知（stdio MCP 服务器的
//!    `notifications/progress`、日志行）会被误当成响应返回，并让后续所有请求
//!    与响应整体错位一格。本实现起一个常驻读流任务：有 `id` 且无 `method`
//!    的行按 id 投递给等待者，其余行走通知分发。
//! 2. **通知实际分发**。Java 的 `notificationHandler` 字段被赋值但从未调用
//!    （stdio 服务器的 `roots/list` 反向请求因此永远得不到回复）。本实现把无
//!    `id` 的行、以及带 `method` 的反向请求交给处理器。
//! 3. **stderr 抽干**。Java `redirectErrorStream(false)` 后从不读 stderr，管道
//!    写满（64KB）时子进程阻塞在 write 上、整个服务器假死。本实现起抽干任务
//!    并以 `debug` 记录。
//! 4. **忙轮询消除**。Java 以 `reader.ready()` + `Thread.sleep(10)` 忙等
//!    （每请求最坏 3000 次唤醒），Rust 侧为 `tokio::time::timeout` + oneshot。
//! 5. **进程组终止**。Java `destroy()` 只终止直接子进程，`npx` 一类包装器
//!    留下孤儿孙进程；本实现与 `zk_tools::process` 同范式：
//!    `Command::process_group(0)` + `nix::killpg`（安全 API，符合
//!    `unsafe_code = "forbid"`）。
//! 6. **未连接时的通知写入**。Java `sendNotification` 不判连接状态，`stdinWriter`
//!    为 null 时抛 NPE（非 `IOException`，逃出 catch）；本实现降级为 warn。

use std::collections::{BTreeMap, HashMap};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::config::McpServerConfig;
use crate::error::McpProtocolError;
use crate::jsonrpc::{
    IncomingMessage, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId,
};
use crate::transport::{McpTransport, NotificationHandler, timeout_or_default};

/// SIGTERM → SIGKILL 宽限期（对照 Java `process.waitFor(5, SECONDS)`）。
const TERMINATE_GRACE: Duration = Duration::from_secs(5);

/// 等待响应的一次性通道。
type Pending = oneshot::Sender<Result<Option<Value>, McpProtocolError>>;

/// 读流任务与请求方共享的状态。
#[derive(Default)]
struct StdioShared {
    connected: AtomicBool,
    pending: Mutex<HashMap<String, Pending>>,
    notification_handler: RwLock<Option<NotificationHandler>>,
}

impl StdioShared {
    /// 摘出等待者（响应到达 / 超时清理共用）。
    fn take_pending(&self, key: &str) -> Option<Pending> {
        lock(&self.pending).remove(key)
    }

    /// 以同一错误失败掉全部等待者（流关闭 / 传输关闭）。
    fn fail_all_pending(&self, message: &str) {
        let drained: Vec<Pending> = lock(&self.pending).drain().map(|(_, tx)| tx).collect();
        for tx in drained {
            let _ = tx.send(Err(McpProtocolError::internal(message.to_owned())));
        }
    }

    fn notification_handler(&self) -> Option<NotificationHandler> {
        self.notification_handler
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// 锁获取（中毒后继续使用内部数据——本 crate 的状态均为可继续使用的集合，
/// 让传输因一次 panic 永久瘫痪反而放大故障面）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// MCP STDIO 传输实现。
pub struct StdioTransport {
    command: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    request_id: AtomicI64,
    shared: Arc<StdioShared>,
    child: Mutex<Option<Child>>,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for StdioTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioTransport")
            .field("command", &self.command)
            .field("args", &self.args)
            .field("connected", &self.shared.connected.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl StdioTransport {
    /// 由服务器配置构造（`command` 缺失时以空串占位，`connect` 必然失败并回报
    /// `"Failed to start STDIO process"`——对照 Java `config.command()` 为 null
    /// 时 `ProcessBuilder` 抛 `NullPointerException` 的等价失败路径）。
    #[must_use]
    pub fn new(config: &McpServerConfig) -> Self {
        Self {
            command: config.command.clone().unwrap_or_default(),
            args: config.args.clone(),
            env: config.env.clone(),
            request_id: AtomicI64::new(1),
            shared: Arc::new(StdioShared::default()),
            child: Mutex::new(None),
            stdin: tokio::sync::Mutex::new(None),
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// 写一行（`payload + "\n"` + flush，对照 Java 的写法）。
    async fn write_line(&self, payload: &str) -> std::io::Result<()> {
        let mut guard = self.stdin.lock().await;
        let stdin = guard.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "STDIO stdin unavailable")
        })?;
        stdin.write_all(payload.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await
    }

    fn child_slot(&self) -> MutexGuard<'_, Option<Child>> {
        lock(&self.child)
    }
}

impl McpTransport for StdioTransport {
    fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
        Box::pin(async move {
            let mut builder = Command::new(&self.command);
            builder
                .args(&self.args)
                .envs(&self.env)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            #[cfg(unix)]
            builder.process_group(0);
            let mut child = builder.spawn().map_err(|error| {
                McpProtocolError::wrapped(format!("Failed to start STDIO process: {error}"))
            })?;

            let stdout = child.stdout.take();
            let stderr = child.stderr.take();
            *self.stdin.lock().await = child.stdin.take();
            let pid = child.id();

            let shared = Arc::clone(&self.shared);
            let reader = tokio::spawn(async move { read_loop(stdout, shared).await });
            let command = self.command.clone();
            let drain = tokio::spawn(async move { drain_stderr(stderr, command).await });
            {
                let mut tasks = lock(&self.tasks);
                for task in tasks.drain(..) {
                    task.abort();
                }
                tasks.push(reader);
                tasks.push(drain);
            }
            *self.child_slot() = Some(child);
            self.shared.connected.store(true, Ordering::SeqCst);
            tracing::info!(?pid, command = %self.command, "STDIO transport connected");
            Ok(())
        })
    }

    fn send_request<'a>(
        &'a self,
        method: &'a str,
        params: Option<Value>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
        Box::pin(async move {
            if !self.shared.connected.load(Ordering::SeqCst) {
                return Err(McpProtocolError::not_initialized("STDIO not connected"));
            }
            let id = self.request_id.fetch_add(1, Ordering::SeqCst);
            let key = id.to_string();
            let request = JsonRpcRequest::new(RequestId::Number(id), method, params);
            let payload = serde_json::to_string(&request).map_err(|error| {
                McpProtocolError::wrapped(format!("STDIO communication error: {error}"))
            })?;

            let (tx, rx) = oneshot::channel();
            lock(&self.shared.pending).insert(key.clone(), tx);

            if let Err(error) = self.write_line(&payload).await {
                self.shared.take_pending(&key);
                return Err(McpProtocolError::wrapped(format!(
                    "STDIO communication error: {error}"
                )));
            }

            match tokio::time::timeout(timeout_or_default(timeout), rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(McpProtocolError::wrapped(
                    "STDIO communication error: response channel closed",
                )),
                Err(_) => {
                    self.shared.take_pending(&key);
                    Err(McpProtocolError::timeout(format!(
                        "STDIO timeout: {method}"
                    )))
                }
            }
        })
    }

    fn send_notification<'a>(
        &'a self,
        method: &'a str,
        params: Option<Value>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let notification = JsonRpcNotification::new(method, params);
            match serde_json::to_string(&notification) {
                Ok(payload) => {
                    if let Err(error) = self.write_line(&payload).await {
                        tracing::warn!(method, %error, "Failed to send STDIO notification");
                    }
                }
                Err(error) => {
                    tracing::warn!(method, %error, "Failed to send STDIO notification");
                }
            }
        })
    }

    fn send_response(&self, id: RequestId, result: Value) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if !self.shared.connected.load(Ordering::SeqCst) {
                tracing::warn!("Cannot send STDIO response — transport not connected");
                return;
            }
            let response = JsonRpcResponse::success(id.clone(), result);
            match serde_json::to_string(&response) {
                Ok(payload) => {
                    if let Err(error) = self.write_line(&payload).await {
                        tracing::warn!(%id, %error, "Failed to send STDIO response");
                    }
                }
                Err(error) => {
                    tracing::warn!(%id, %error, "Failed to send STDIO response");
                }
            }
        })
    }

    fn is_connected(&self) -> bool {
        if !self.shared.connected.load(Ordering::SeqCst) {
            return false;
        }
        match self.child.try_lock() {
            Ok(mut guard) => guard
                .as_mut()
                .is_some_and(|child| matches!(child.try_wait(), Ok(None))),
            // 锁被 close()/connect() 占用；连接标志仍为真则视为存活。
            Err(_) => true,
        }
    }

    fn set_notification_handler(&self, handler: NotificationHandler) {
        *self
            .shared
            .notification_handler
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(handler);
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.shared.connected.store(false, Ordering::SeqCst);
            let child = self.child_slot().take();
            if let Some(mut child) = child {
                terminate(&mut child).await;
            }
            drop(self.stdin.lock().await.take());
            for task in lock(&self.tasks).drain(..) {
                task.abort();
            }
            self.shared.fail_all_pending("Transport closed");
        })
    }
}

/// stdout 读流循环 — 逐行解码并分发（响应按 id 投递，其余走通知处理器）。
async fn read_loop(stdout: Option<ChildStdout>, shared: Arc<StdioShared>) {
    let Some(stdout) = stdout else {
        return;
    };
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                dispatch_line(&line, &shared);
            }
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(%error, "STDIO read error");
                break;
            }
        }
    }
    // stdout 关闭 = 再也收不到响应（进程退出或自行关流）：立刻置断连并失败掉
    // 全部等待者，而非让每个请求各等满自己的超时。
    shared.connected.store(false, Ordering::SeqCst);
    shared.fail_all_pending("STDIO stream closed");
}

/// 分发一行 JSON-RPC 文本。
fn dispatch_line(line: &str, shared: &StdioShared) {
    let Ok(raw) = serde_json::from_str::<Value>(line) else {
        // 非 JSON 行（服务器把日志写进了 stdout）——丢弃并记录，绝不当成响应。
        tracing::debug!(line, "Ignoring non-JSON line on MCP STDIO stdout");
        return;
    };
    let Ok(message) = serde_json::from_value::<IncomingMessage>(raw.clone()) else {
        tracing::debug!(
            line,
            "Ignoring malformed JSON-RPC message on MCP STDIO stdout"
        );
        return;
    };

    if let Some(id) = message.id.as_ref().filter(|_| message.method.is_none()) {
        let key = id.as_key();
        if let Some(tx) = shared.take_pending(&key) {
            let outcome = match message.error {
                Some(error) => Err(McpProtocolError::from_rpc(error)),
                None => Ok(message.result),
            };
            let _ = tx.send(outcome);
        } else {
            tracing::debug!(%id, "Unmatched STDIO response id, dropping");
        }
        return;
    }

    if message.method.is_some() {
        if let Some(handler) = shared.notification_handler() {
            handler(raw);
        }
        return;
    }
    tracing::debug!(line, "Unrecognized MCP STDIO message, dropping");
}

/// 抽干 stderr（防管道写满导致子进程阻塞）。
async fn drain_stderr(stderr: Option<ChildStderr>, command: String) {
    let Some(stderr) = stderr else {
        return;
    };
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(command = %command, line, "MCP STDIO server stderr");
    }
}

/// 终止子进程：SIGTERM → 宽限 [`TERMINATE_GRACE`] → SIGKILL（整进程组）。
async fn terminate(child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        signal_group(pid, nix::sys::signal::Signal::SIGTERM);
        if tokio::time::timeout(TERMINATE_GRACE, child.wait())
            .await
            .is_ok()
        {
            return;
        }
        signal_group(pid, nix::sys::signal::Signal::SIGKILL);
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

/// 对进程组发信号（`nix::killpg` 安全封装；组已消失时静默）。
#[cfg(unix)]
fn signal_group(pid: u32, signal: nix::sys::signal::Signal) {
    let Ok(raw) = i32::try_from(pid) else {
        return;
    };
    if let Err(error) = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(raw), signal) {
        tracing::debug!(pid, %error, ?signal, "killpg failed (process already gone?)");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use serde_json::json;

    use super::*;
    use crate::config::{McpConfigScope, McpTransportType};
    use crate::error::{REQUEST_TIMEOUT, SERVER_NOT_INITIALIZED};

    /// 以 `sh -c` 脚本充当 MCP 服务器（回显 id 的最小 JSON-RPC 实现）。
    fn script_config(script: &str) -> McpServerConfig {
        McpServerConfig {
            name: "test".to_owned(),
            transport: McpTransportType::Stdio,
            command: Some("/bin/sh".to_owned()),
            args: vec!["-c".to_owned(), script.to_owned()],
            env: BTreeMap::new(),
            url: None,
            headers: BTreeMap::new(),
            scope: McpConfigScope::Local,
        }
    }

    /// 逐行读请求、抽出数字 id、按模板回写（模板内 `%s` = id，模板作为运行时
    /// 数据插入，故写单层花括号）。
    fn echo_script(response_template: &str, preamble: &str) -> String {
        format!(
            "{preamble}while IFS= read -r line; do \
             id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9]*\\).*/\\1/p'); \
             if [ -n \"$id\" ]; then printf '{response_template}\\n' \"$id\"; fi; done"
        )
    }

    #[tokio::test]
    async fn round_trips_request_and_dispatches_notification() {
        let script = echo_script(
            r#"{"jsonrpc":"2.0","id":%s,"result":{"tools":[]}}"#,
            r#"printf '{"jsonrpc":"2.0","method":"notifications/progress","params":{"p":1}}\n'; "#,
        );
        let transport = StdioTransport::new(&script_config(&script));
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&seen);
        transport.set_notification_handler(Arc::new(move |value: Value| {
            assert_eq!(value["method"], json!("notifications/progress"));
            counter.fetch_add(1, Ordering::SeqCst);
        }));

        transport.connect().await.expect("connect");
        assert!(transport.is_connected());

        let result = transport
            .send_request("tools/list", Some(json!({})), Duration::from_secs(5))
            .await
            .expect("request");
        assert_eq!(result, Some(json!({"tools": []})));
        // 先于响应到达的通知必须走处理器，绝不被当成响应（Java 缺陷点）。
        assert_eq!(seen.load(Ordering::SeqCst), 1);

        // 第二次请求的 id 自增，仍能正确匹配。
        let second = transport
            .send_request("tools/list", None, Duration::from_secs(5))
            .await
            .expect("request");
        assert_eq!(second, Some(json!({"tools": []})));

        transport.close().await;
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn request_before_connect_is_server_not_initialized() {
        let transport = StdioTransport::new(&script_config("cat"));
        let error = transport
            .send_request("tools/list", None, Duration::from_secs(1))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), SERVER_NOT_INITIALIZED);
        assert_eq!(error.message(), "STDIO not connected");
    }

    #[tokio::test]
    async fn silent_server_yields_request_timeout() {
        let transport = StdioTransport::new(&script_config("cat > /dev/null"));
        transport.connect().await.expect("connect");
        let error = transport
            .send_request("tools/list", None, Duration::from_millis(150))
            .await
            .expect_err("must time out");
        assert_eq!(error.code(), REQUEST_TIMEOUT);
        assert_eq!(error.message(), "STDIO timeout: tools/list");
        transport.close().await;
    }

    #[tokio::test]
    async fn server_error_object_becomes_protocol_error() {
        let script = echo_script(
            r#"{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found: nope"}}"#,
            "",
        );
        let transport = StdioTransport::new(&script_config(&script));
        transport.connect().await.expect("connect");
        let error = transport
            .send_request("nope", None, Duration::from_secs(5))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), -32601);
        assert_eq!(error.message(), "Method not found: nope");
        transport.close().await;
    }

    #[tokio::test]
    async fn spawn_failure_is_reported() {
        let mut config = script_config("cat");
        config.command = Some("zk-mcp-no-such-binary".to_owned());
        let transport = StdioTransport::new(&config);
        let error = transport.connect().await.expect_err("must fail");
        assert!(
            error
                .message()
                .starts_with("Failed to start STDIO process: "),
            "unexpected message: {}",
            error.message()
        );
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn process_exit_marks_transport_disconnected() {
        let transport = StdioTransport::new(&script_config("exit 0"));
        transport.connect().await.expect("connect");
        // 等 stdout EOF 被读流任务观测到。
        for _ in 0..50 {
            if !transport.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!transport.is_connected());
        let error = transport
            .send_request("tools/list", None, Duration::from_secs(1))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), SERVER_NOT_INITIALIZED);
        transport.close().await;
    }

    #[tokio::test]
    async fn reverse_request_reaches_handler_and_response_is_writable() {
        // 服务器先发 roots/list 反向请求，再把客户端回写的响应原样打到 stderr
        // （不干扰 stdout 协议流）；这里只验证 handler 收到反向请求且写回不报错。
        let script = format!(
            "printf '{}\\n'; cat > /dev/null",
            r#"{"jsonrpc":"2.0","id":"r1","method":"roots/list"}"#
        );
        let transport = StdioTransport::new(&script_config(&script));
        let (tx, rx) = std::sync::mpsc::channel();
        transport.set_notification_handler(Arc::new(move |value: Value| {
            let _ = tx.send(value);
        }));
        transport.connect().await.expect("connect");
        let received = tokio::task::spawn_blocking(move || {
            rx.recv_timeout(Duration::from_secs(5))
                .expect("reverse request")
        })
        .await
        .expect("join");
        assert_eq!(received["method"], json!("roots/list"));
        assert_eq!(received["id"], json!("r1"));

        transport
            .send_response(RequestId::Text("r1".to_owned()), json!({"roots": []}))
            .await;
        transport.close().await;
    }

    #[tokio::test]
    async fn health_ping_defaults_to_connection_state() {
        let transport = StdioTransport::new(&script_config("cat > /dev/null"));
        assert!(!transport.send_health_ping().await);
        transport.connect().await.expect("connect");
        assert!(transport.send_health_ping().await);
        transport.close().await;
        assert!(!transport.send_health_ping().await);
    }
}
