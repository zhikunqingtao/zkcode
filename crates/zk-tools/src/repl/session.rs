//! 单个 REPL 会话——一个长驻解释器进程及其三条标准流。
//!
//! 对照旧 `tool/repl/ReplSession.java`（100L，只读权威规格）：字段
//! `id` / `language` / `process` / `stdinWriter` / `stdoutReader` /
//! `stderrReader` / `lastActive`（`AtomicReference<Instant>`）/ `createdAt`；
//! 方法 `writeStdin` / `readAvailableOutput` / `readAvailableStderr` /
//! `updateLastActive` / `destroy` / `isAlive`。
//!
//! 差异：旧靠 `BufferedReader.ready()` 做非阻塞探测，Rust 的 `AsyncBufRead`
//! 无等价物，故本实现在会话创建时即起两条读行任务把 stdout / stderr 持续
//! 抽进 [`std::sync::Mutex<String>`] 缓冲区，读取侧只做「取走并清空」。
//! 由此还额外获得旧实现没有的性质：解释器输出不会因无人读而把管道写满阻死。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::{Child, ChildStdin};
use tokio::task::JoinHandle;
use tracing::debug;

use super::QUIET_WINDOW;

/// 输出缓冲区——读行任务写入，[`ReplSession::drain`] 取走。
type Buffer = Arc<Mutex<String>>;

/// 长驻解释器会话（旧 `ReplSession`）。
pub struct ReplSession {
    /// 会话 ID（旧 `id`）。
    id: String,
    /// 语言（旧 `language`）。
    language: String,
    /// 子进程句柄；`start_kill` / `try_wait` 均需 `&mut`，故上锁持有。
    child: Mutex<Child>,
    /// 进程 stdin（旧 `stdinWriter`）；写入需 `await`，用异步锁。
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    /// stdout 缓冲区（旧 `stdoutReader` 的等价物）。
    stdout: Buffer,
    /// stderr 缓冲区（旧 `stderrReader` 的等价物）。
    stderr: Buffer,
    /// 两条读行任务，`Drop` 时一并中止。
    readers: Mutex<Vec<JoinHandle<()>>>,
    /// 最后活动时刻（旧 `AtomicReference<Instant> lastActive`），存 Unix 毫秒。
    last_active: AtomicU64,
    /// 创建时刻（旧 `createdAt`）。
    created_at: Instant,
}

/// 手写而非 `derive`：`child` / `readers` 是 [`Mutex`]，`derive` 的实现会在
/// 格式化时尝试上锁——诊断路径不应有阻塞或死锁风险，故只暴露不加锁即可取到的
/// 身份字段。
impl std::fmt::Debug for ReplSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplSession")
            .field("id", &self.id)
            .field("language", &self.language)
            .field("age", &self.created_at.elapsed())
            .finish_non_exhaustive()
    }
}

impl ReplSession {
    /// 接管已 spawn 的子进程并起两条读行任务（旧构造器）。
    ///
    /// # Panics
    /// `child` 的三条标准流未全部设为 `piped` 时 panic——调用方
    /// （[`super::ReplManager`]）保证这一点。
    pub fn new(id: impl Into<String>, language: impl Into<String>, mut child: Child) -> Self {
        let stdin = child.stdin.take().expect("stdin must be piped");
        let stdout_pipe = child.stdout.take().expect("stdout must be piped");
        let stderr_pipe = child.stderr.take().expect("stderr must be piped");

        let stdout: Buffer = Arc::default();
        let stderr: Buffer = Arc::default();
        let readers = vec![
            spawn_reader(stdout_pipe, Arc::clone(&stdout)),
            spawn_reader(stderr_pipe, Arc::clone(&stderr)),
        ];

        Self {
            id: id.into(),
            language: language.into(),
            child: Mutex::new(child),
            stdin: tokio::sync::Mutex::new(Some(stdin)),
            stdout,
            stderr,
            readers: Mutex::new(readers),
            last_active: AtomicU64::new(now_millis()),
            created_at: Instant::now(),
        }
    }

    /// 会话 ID（旧 `id()`）。
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 语言（旧 `language()`）。
    #[must_use]
    pub fn language(&self) -> &str {
        &self.language
    }

    /// 最后活动时刻（旧 `lastActive()`），Unix 毫秒。
    #[must_use]
    pub fn last_active_millis(&self) -> u64 {
        self.last_active.load(Ordering::Relaxed)
    }

    /// 已存活时长（旧 `createdAt()` 与 `SESSION_MAX_LIFETIME` 比较的等价物）。
    #[must_use]
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// 空闲时长（旧 `lastActive().isBefore(idleCutoff)` 的等价物）。
    #[must_use]
    pub fn idle_for(&self) -> Duration {
        Duration::from_millis(now_millis().saturating_sub(self.last_active_millis()))
    }

    /// 刷新最后活动时刻（旧 `updateLastActive`）。
    pub fn update_last_active(&self) {
        self.last_active.store(now_millis(), Ordering::Relaxed);
    }

    /// 进程是否仍活着（旧 `isAlive`）。
    ///
    /// `try_wait` 顺带回收僵尸进程，故必须调用而非只看 spawn 结果。
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.child
            .lock()
            .is_ok_and(|mut child| matches!(child.try_wait(), Ok(None)))
    }

    /// 向 stdin 写一行代码（旧 `writeStdin`：write + newLine + flush）。
    ///
    /// # Errors
    /// stdin 已关闭或管道写失败时返回 IO 错误。
    pub async fn write_stdin(&self, code: &str) -> std::io::Result<()> {
        let mut guard = self.stdin.lock().await;
        let stdin = guard.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "REPL stdin already closed")
        })?;
        stdin.write_all(code.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        self.update_last_active();
        Ok(())
    }

    /// 收敛读取 stdout（旧 `readAvailableOutput(timeoutMs)`）。
    ///
    /// 先等一个 [`QUIET_WINDOW`] 让输出产生（旧 `Thread.sleep(min(200, …))`），
    /// 随后每窗轮询一次：拿到字节则继续等下一窗，连续一窗无新字节即收工；
    /// 首字节迟迟不来则等到 `timeout` 为止（旧实现此时直接回空串）。
    pub async fn read_available_output(&self, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        let mut collected = String::new();
        loop {
            tokio::time::sleep(QUIET_WINDOW.min(timeout)).await;
            let chunk = drain(&self.stdout);
            let quiet = chunk.is_empty();
            collected.push_str(&chunk);
            // 有过输出且本窗静默 → 本轮收敛（旧 `!ready()` 退出循环）。
            if quiet && !collected.is_empty() {
                break;
            }
            // 进程已死且缓冲区抽干 → 无需再等（旧靠 `isAlive` 在上层发现）。
            if quiet && !self.is_alive() {
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        self.update_last_active();
        collected
    }

    /// 取走当前 stderr 缓冲（旧 `readAvailableStderr`）。
    #[must_use]
    pub fn read_available_stderr(&self) -> String {
        drain(&self.stderr)
    }

    /// 强杀进程并中止读行任务（旧 `destroy`：关 stdin + `destroyForcibly`）。
    pub fn destroy(&self) {
        // 先弃掉 stdin 让解释器看到 EOF（旧 `stdinWriter.close()`）。
        if let Ok(mut guard) = self.stdin.try_lock() {
            drop(guard.take());
        }
        if let Ok(mut child) = self.child.lock() {
            let _ = child.start_kill();
        }
        if let Ok(mut readers) = self.readers.lock() {
            for handle in readers.drain(..) {
                handle.abort();
            }
        }
        debug!(session_id = %self.id, "REPL session destroyed");
    }
}

impl Drop for ReplSession {
    /// 会话被移出进程池即释放进程——避免解释器变成孤儿。
    fn drop(&mut self) {
        self.destroy();
    }
}

/// 起一条读行任务，把管道内容持续追加进 `buffer`。
fn spawn_reader<R>(pipe: R, buffer: Buffer) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut reader = BufReader::new(pipe);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                // EOF——解释器退出。
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if let Ok(mut guard) = buffer.lock() {
                        guard.push_str(&line);
                    }
                }
            }
        }
    })
}

/// 取走缓冲区全部内容（锁被毒化时回空串，绝不 panic 在工具执行路径上）。
fn drain(buffer: &Buffer) -> String {
    buffer
        .lock()
        .map(|mut guard| std::mem::take(&mut *guard))
        .unwrap_or_default()
}

/// 当前 Unix 毫秒（`AtomicU64` 存储，替代旧 `AtomicReference<Instant>`）。
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;

    use super::*;

    /// spawn 一个 `cat` 当最小「解释器」——回显行为足够验证三条流接线。
    fn spawn_cat() -> Option<ReplSession> {
        let child = tokio::process::Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .ok()?;
        Some(ReplSession::new("s1", "python", child))
    }

    /// 写入 → 收敛读回：stdin/stdout 双向接线可用，活动时刻被刷新。
    #[tokio::test]
    async fn round_trips_through_the_child_process() {
        let Some(session) = spawn_cat() else {
            return; // 无 `cat` 的环境跳过（CI 容器最小镜像）。
        };
        assert!(session.is_alive());
        assert_eq!(session.id(), "s1");
        assert_eq!(session.language(), "python");

        session.write_stdin("ping").await.expect("write");
        let output = session.read_available_output(Duration::from_secs(5)).await;
        assert_eq!(output.trim(), "ping");
        // 缓冲区已被取走 → 再读为空（旧 `ready()` 转 false 的等价可观测）。
        assert!(session.read_available_stderr().is_empty());
        assert!(session.idle_for() < Duration::from_secs(5));
        assert!(session.age() < Duration::from_mins(1));
    }

    /// `destroy` 后进程不再存活，且写入落 `BrokenPipe`。
    #[tokio::test]
    async fn destroy_kills_the_process_and_closes_stdin() {
        let Some(session) = spawn_cat() else {
            return;
        };
        session.destroy();
        // 等 kill 生效（`start_kill` 是异步信号）。
        for _ in 0..50 {
            if !session.is_alive() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!session.is_alive());
        let error = session.write_stdin("x").await.expect_err("closed");
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }

    /// 进程已死时收敛读不会干等满超时预算。
    #[tokio::test]
    async fn dead_process_short_circuits_the_read_budget() {
        let Some(session) = spawn_cat() else {
            return;
        };
        session.destroy();
        let started = Instant::now();
        let output = session.read_available_output(Duration::from_secs(30)).await;
        assert!(output.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
    }
}
