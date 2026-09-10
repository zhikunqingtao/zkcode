//! 受控子进程基座——进程组隔离 + 优雅终止 + 输出采集上限。
//!
//! 对照旧 `tool/process/ManagedProcessRunner.java` 与
//! `tool/bash/ProcessTreeManager.java`（只读权威规格）：
//! - 采集上限 1 MiB（旧 `maxCaptureBytes = 1048576`）；
//! - 终止序列 SIGTERM → 宽限 → SIGKILL（旧 `destroy()` → `terminateGraceMs`
//!   → `destroyForcibly()`，逐进程后代逆序）；
//! - 超时退出码 137（旧 `BashTool` 超时分支逐字）。
//!
//! 差异（留痕 docs/compatibility.md §4）：
//! - 旧靠 JVM `ProcessHandle.descendants()` 逐个 destroy，本实现改为
//!   **进程组**语义：`Command::process_group(0)` 让子进程成为新进程组组长
//!   （等价 `setsid` 的可达效果），终止时对**整组**发信号
//!   （`nix::sys::signal::killpg`），孙进程不再逃逸。选此路线的硬原因：
//!   workspace lint `unsafe_code = "forbid"`，禁止 `pre_exec` +
//!   `libc::setsid` 裸调用；`process_group` / `killpg` 均为安全 API；
//! - 宽限期取 5s（任务判据）而非旧 1s。

use std::collections::VecDeque;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;

use crate::tool::{ExecutionResourceTerminal, ToolContext};

/// 单流采集上限（旧 `maxCaptureBytes = 1048576` 逐字对照）。
pub const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

/// SIGTERM → SIGKILL 宽限期（任务判据 5s；旧 `terminateGraceMs = 1000`）。
pub const TERMINATE_GRACE: Duration = Duration::from_secs(5);

/// Maximum time to prove absence after SIGKILL.
pub const KILL_CONFIRM_GRACE: Duration = Duration::from_secs(2);

/// Maximum time to reclaim stdout/stderr pumps after process-group cleanup.
pub const PIPE_RECLAIM_GRACE: Duration = Duration::from_secs(1);

/// 超时退出码（旧 `BashTool` 超时分支逐字 137）。
pub const TIMEOUT_EXIT_CODE: i32 = 137;

/// 子进程环境中必须无条件清除的敏感变量——与
/// `zk_authz::tool_safety::SENSITIVE_ENV_VARS` **同源**（逐字对照旧
/// `service/ToolSafetyGuard.java:200-206` 的 `SENSITIVE_ENV_VARS`）。
///
/// 此处内联而非引用 zk-authz：依赖方向铁律禁止 `zk-tools → zk-authz`
/// （zk-authz 依赖 zk-protocol / zk-db，会把仓储栈拖进工具执行面）。两处清单
/// 由 `crates/zk-server/tests/tool_safety_env_baseline.rs` 的跨 crate 相等断言
/// 锁死，永不分叉。
///
/// 清理在 [`spawn`] 里**无条件**执行，不经任何可选端口/组合根接线：旧源该守卫
/// 从未被接线（全仓零调用点），做成可选开关等于留后门。
pub const SENSITIVE_ENV_VARS: &[&str] = &[
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "NPM_TOKEN",
    "DOCKER_PASSWORD",
    "DATABASE_PASSWORD",
    "DB_PASSWORD",
    "PRIVATE_KEY",
    "SECRET_KEY",
];

/// 终止（信号）退出码基数（POSIX 约定 `128 + signal`）。
const SIGNAL_EXIT_BASE: i32 = 128;

/// Fixed stream-read chunk keeps memory independent of total child output.
const READ_CHUNK_BYTES: usize = 8 * 1024;

/// Kept inside the one-MiB capture budget so callers can distinguish the
/// retained head from the retained tail without relying only on metadata.
const TRUNCATION_MARKER: &[u8] = b"\n...[output truncated; tail follows]...\n";

/// SIGTERM 终止退出码（`128 + 15`；取消路径回报值）。
const SIGTERM_EXIT_CODE: i32 = SIGNAL_EXIT_BASE + 15;

/// 一次子进程执行的终态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessOutcome {
    /// 标准输出（上限内）。
    pub stdout: String,
    /// 标准错误（上限内）。
    pub stderr: String,
    /// 退出码（信号终止 → `128 + signal`；超时 → [`TIMEOUT_EXIT_CODE`]）。
    pub exit_code: i32,
    /// 是否因超时被终止。
    pub timed_out: bool,
    /// 是否因取消令牌被终止。
    pub cancelled: bool,
    /// 是否有任一流触达采集上限。
    pub truncated: bool,
}

/// 以 `bash -c` 执行命令行（对照旧 `BashTool` 的 `bash -c` 调用形状）。
///
/// stdout 增量按行经 [`ToolContext::report_progress`] 上报（映射下行
/// `tool_use_progress`）；取消令牌触发或超时 → 整进程组终止。
///
/// # Errors
/// 子进程 spawn 失败（可执行文件缺失 / 工作目录不存在等）时返回。
pub async fn run_shell(
    command: &str,
    working_dir: &Path,
    timeout: Duration,
    ctx: &ToolContext,
) -> std::io::Result<ProcessOutcome> {
    run_program(
        "bash",
        &["-c".to_owned(), command.to_owned()],
        working_dir,
        timeout,
        ctx,
    )
    .await
}

/// 直接以 argv 形式执行程序（**不经 shell**——Git 族工具用，入参不参与
/// shell 解析，天然无注入面）。
///
/// # Errors
/// 子进程 spawn 失败（程序不在 PATH / 工作目录不存在等）时返回。
pub async fn run_program(
    program: &str,
    args: &[String],
    working_dir: &Path,
    timeout: Duration,
    ctx: &ToolContext,
) -> std::io::Result<ProcessOutcome> {
    ctx.execution_owner_ready().map_err(std::io::Error::other)?;
    let program = program.to_owned();
    let args = args.to_vec();
    let working_dir = working_dir.to_path_buf();
    // Commit a stable ownership reservation before the process can exist. The
    // nested supervisor binds its PID immediately after spawn; if either spawn
    // or binding fails, the same owner closes the reservation conservatively.
    let lease = ctx
        .register_execution_resource(
            if cfg!(unix) {
                "processGroup"
            } else {
                "process"
            },
            None,
            serde_json::json!({
                "program": program,
                "workingDirectory": working_dir.display().to_string(),
                "phase": "reserved",
            }),
        )
        .await
        .map_err(std::io::Error::other)?;
    let supervisor_ctx = ctx.clone();
    let cleanup_ctx = ctx.clone();
    let (result_tx, result_rx) = oneshot::channel();
    let owned = async move {
        let result = match spawn(&program, &args, &working_dir) {
            Ok(child) => supervise_program(child, timeout, &supervisor_ctx, lease).await,
            Err(error) => {
                if let Some(lease) = lease {
                    let _ = supervisor_ctx
                        .finish_execution_resource(lease, ExecutionResourceTerminal::Released)
                        .await;
                }
                Err(error)
            }
        };
        let _ = result_tx.send(result);
    };
    if let Err(error) = ctx.spawn_owned_execution(Box::pin(owned)) {
        // Registration may have raced the supervisor intake close. No process
        // was spawned because physical creation lives inside the rejected
        // future, but durable cleanup is still closed conservatively.
        cleanup_ctx.force_unconfirmed_execution_resources().await;
        return Err(std::io::Error::other(error));
    }
    result_rx
        .await
        .map_err(|_| std::io::Error::other("process supervisor stopped without an outcome"))?
}

async fn supervise_program(
    mut child: Child,
    timeout: Duration,
    ctx: &ToolContext,
    lease: Option<crate::tool::ExecutionResourceLease>,
) -> std::io::Result<ProcessOutcome> {
    let Some(pid) = child.id() else {
        if let Some(lease) = lease {
            let _ = ctx
                .finish_execution_resource(lease, ExecutionResourceTerminal::Unconfirmed)
                .await;
        }
        return Err(std::io::Error::other("spawned process has no pid"));
    };
    if let Some(lease) = lease.as_ref()
        && let Err(error) = ctx
            .bind_execution_resource_external(lease, pid.to_string())
            .await
    {
        let _ = terminate(&mut child, pid).await;
        let _ = ctx
            .finish_execution_resource(lease.clone(), ExecutionResourceTerminal::Unconfirmed)
            .await;
        return Err(std::io::Error::other(error));
    }
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let progress = ctx.clone();
    let mut stdout_task = tokio::spawn(async move { pump(stdout, Some(progress)).await });
    let mut stderr_task = tokio::spawn(async move { pump(stderr, None).await });
    let started = Instant::now();
    let mut timed_out = false;
    let mut cancelled = false;
    let (exit_code, cleanup_confirmed) = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => {
            cancelled = true;
            (SIGTERM_EXIT_CODE, terminate(&mut child, pid).await)
        }
        status = tokio::time::timeout(timeout, child.wait()) => match status {
            Ok(Ok(status)) => {
                let code = exit_code_of(status);
                (code, cleanup_after_parent_exit(&mut child, pid).await)
            }
            Ok(Err(_)) => (SIGNAL_EXIT_BASE, terminate(&mut child, pid).await),
            Err(_) => {
                timed_out = true;
                (TIMEOUT_EXIT_CODE, terminate(&mut child, pid).await)
            }
        },
    };
    let ((stdout, out_truncated), (stderr, err_truncated)) =
        collect_pumps(&mut stdout_task, &mut stderr_task).await;
    if let Some(lease) = lease {
        let terminal = if cleanup_confirmed {
            ExecutionResourceTerminal::Released
        } else {
            ExecutionResourceTerminal::Unconfirmed
        };
        if let Err(error) = ctx.finish_execution_resource(lease, terminal).await {
            return Err(std::io::Error::other(error));
        }
    }
    tracing::debug!(
        pid,
        elapsed_ms = started.elapsed().as_millis(),
        exit_code,
        timed_out,
        cancelled,
        cleanup_confirmed,
        "managed process finished"
    );
    Ok(ProcessOutcome {
        stdout,
        stderr,
        exit_code,
        timed_out,
        cancelled,
        truncated: out_truncated || err_truncated,
    })
}

/// 装配并启动子进程（新进程组 + 三流重定向；stdin 关闭防交互挂起；
/// [`SENSITIVE_ENV_VARS`] 无条件剔除）。
///
/// 环境清理对照旧 `ToolSafetyGuard#sanitizeProcessEnvironment`：旧源作用于
/// `ProcessBuilder.environment()`，本实现用 [`Command::env_remove`] 达成等价
/// 效果。之所以落在这里——这是本 crate 子进程环境的**唯一物理构造点**，任何
/// 工具（Bash / Git / 未来新增）都必经此处；且 edition 2024 下
/// `std::env::remove_var` 为 `unsafe`，workspace `unsafe_code = "forbid"` 禁用，
/// `env_remove` 是唯一合规路径。
fn spawn(program: &str, args: &[String], working_dir: &Path) -> std::io::Result<Child> {
    let mut builder = Command::new(program);
    builder
        .args(args)
        .current_dir(working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for var in SENSITIVE_ENV_VARS {
        builder.env_remove(var);
    }
    #[cfg(unix)]
    builder.process_group(0);
    builder.spawn()
}

/// 按固定块读取一条流；短输出完整保留，长输出在同一个
/// [`MAX_CAPTURE_BYTES`] 预算内保留 head/tail。`progress` 非空时逐块按行上报。
async fn pump<R>(reader: Option<R>, progress: Option<ToolContext>) -> (String, bool)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return (String::new(), false);
    };
    let mut chunk = [0_u8; READ_CHUNK_BYTES];
    let head_limit = (MAX_CAPTURE_BYTES - TRUNCATION_MARKER.len()) / 2;
    let tail_limit = MAX_CAPTURE_BYTES - TRUNCATION_MARKER.len() - head_limit;
    let mut collected = Vec::with_capacity(MAX_CAPTURE_BYTES.min(64 * 1024));
    let mut tail = VecDeque::with_capacity(tail_limit);
    let mut truncated = false;
    loop {
        let read = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if let Some(ctx) = progress.as_ref() {
            // Progress is a bounded, lossy preview. Fixed-size chunks keep a
            // one-gigabyte stream without newlines from becoming a one-gigabyte
            // String before the capture limit can be applied.
            for part in chunk[..read].split_inclusive(|byte| *byte == b'\n') {
                let mut preview = String::from_utf8_lossy(part).into_owned();
                if preview.ends_with('\n') {
                    preview.pop();
                    if preview.ends_with('\r') {
                        preview.pop();
                    }
                }
                if !preview.is_empty() {
                    ctx.report_progress(preview);
                }
            }
        }
        if !truncated && collected.len().saturating_add(read) <= MAX_CAPTURE_BYTES {
            collected.extend_from_slice(&chunk[..read]);
            continue;
        }

        if !truncated {
            truncated = true;
            collected.extend_from_slice(&chunk[..read]);
            for byte in collected
                .iter()
                .skip(collected.len().saturating_sub(tail_limit))
            {
                tail.push_back(*byte);
            }
            collected.truncate(head_limit);
            continue;
        }

        for byte in &chunk[..read] {
            if tail.len() == tail_limit {
                tail.pop_front();
            }
            tail.push_back(*byte);
        }
    }
    if truncated {
        collected.extend_from_slice(TRUNCATION_MARKER);
        collected.extend(tail);
    }
    (String::from_utf8_lossy(&collected).into_owned(), truncated)
}

/// 终止整进程组：SIGTERM → 宽限 [`TERMINATE_GRACE`] → SIGKILL。
///
/// 组 ID = 子进程 pid（[`spawn`] 内 `process_group(0)` 使其成为组长）；
/// 非 unix 平台退化为直接 kill 子进程。
async fn terminate(child: &mut Child, pid: u32) -> bool {
    #[cfg(unix)]
    {
        signal_group(pid, nix::sys::signal::Signal::SIGTERM);
        if wait_until_process_group_gone(child, pid, TERMINATE_GRACE).await {
            return true;
        }
        signal_group(pid, nix::sys::signal::Signal::SIGKILL);
        return wait_until_process_group_gone(child, pid, KILL_CONFIRM_GRACE).await;
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
        tokio::time::timeout(KILL_CONFIRM_GRACE, child.wait())
            .await
            .is_ok_and(|status| status.is_ok())
    }
}

/// A shell may exit while a background descendant still owns its process group
/// and pipes. Such a run is not released until the whole group is absent.
async fn cleanup_after_parent_exit(child: &mut Child, pid: u32) -> bool {
    #[cfg(unix)]
    {
        if !process_group_exists(pid) {
            return true;
        }
        signal_group(pid, nix::sys::signal::Signal::SIGTERM);
        if wait_until_process_group_gone(child, pid, TERMINATE_GRACE).await {
            return true;
        }
        signal_group(pid, nix::sys::signal::Signal::SIGKILL);
        wait_until_process_group_gone(child, pid, KILL_CONFIRM_GRACE).await
    }
    #[cfg(not(unix))]
    {
        child.try_wait().is_ok_and(|status| status.is_some())
    }
}

#[cfg(unix)]
async fn wait_until_process_group_gone(child: &mut Child, pid: u32, grace: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        // Reap the group leader when possible. Failure is conservative: the
        // resource remains unconfirmed even if the subsequent liveness probe is
        // inconclusive.
        if child.try_wait().is_err() {
            return false;
        }
        if !process_group_exists(pid) {
            return true;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }
        tokio::time::sleep((deadline - now).min(Duration::from_millis(20))).await;
    }
}

#[cfg(unix)]
fn process_group_exists(pid: u32) -> bool {
    let Ok(raw) = i32::try_from(pid) else {
        return true;
    };
    match nix::sys::signal::killpg(nix::unistd::Pid::from_raw(raw), None) {
        Err(nix::errno::Errno::ESRCH) => false,
        // EPERM and unexpected kernel errors cannot prove absence.
        Ok(()) | Err(_) => true,
    }
}

async fn collect_pumps(
    stdout_task: &mut tokio::task::JoinHandle<(String, bool)>,
    stderr_task: &mut tokio::task::JoinHandle<(String, bool)>,
) -> ((String, bool), (String, bool)) {
    if let Ok(streams) = tokio::time::timeout(PIPE_RECLAIM_GRACE, async {
        let stdout = (&mut *stdout_task).await.unwrap_or_default();
        let stderr = (&mut *stderr_task).await.unwrap_or_default();
        (stdout, stderr)
    })
    .await
    {
        streams
    } else {
        stdout_task.abort();
        stderr_task.abort();
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        (
            (String::new(), true),
            (
                "pipe collection exceeded 1s cleanup window".to_owned(),
                true,
            ),
        )
    }
}

/// 对进程组发信号（`nix::killpg`，安全封装；组不存在时静默）。
#[cfg(unix)]
fn signal_group(pid: u32, signal: nix::sys::signal::Signal) {
    let Ok(raw) = i32::try_from(pid) else {
        return;
    };
    if let Err(error) = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(raw), signal) {
        tracing::debug!(pid, %error, ?signal, "killpg failed (process already gone?)");
    }
}

/// 退出码归一（信号终止 → `128 + signal`）。
fn exit_code_of(status: std::process::ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return SIGNAL_EXIT_BASE + signal;
        }
    }
    SIGNAL_EXIT_BASE
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::future::BoxFuture;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::tool::{
        ExecutionResourceAllocation, ExecutionResourceLease, ExecutionResourceObserver,
        ExecutionResourceOwner, ToolCleanupStatus,
    };

    #[derive(Default)]
    struct RecordingResourceObserver {
        allocations: Mutex<Vec<(ExecutionResourceOwner, ExecutionResourceAllocation)>>,
        reserved_external_ids: Mutex<Vec<Option<String>>>,
        terminals: Mutex<Vec<(String, ExecutionResourceTerminal)>>,
        registered: tokio::sync::Notify,
        finished: tokio::sync::Notify,
        reject_registration: bool,
    }

    impl ExecutionResourceObserver for RecordingResourceObserver {
        fn register(
            &self,
            owner: ExecutionResourceOwner,
            allocation: ExecutionResourceAllocation,
        ) -> BoxFuture<'static, Result<ExecutionResourceLease, String>> {
            let result = if self.reject_registration {
                Err("injected registration failure".to_owned())
            } else {
                self.reserved_external_ids
                    .lock()
                    .expect("reservation lock")
                    .push(allocation.external_id.clone());
                self.allocations
                    .lock()
                    .expect("allocations lock")
                    .push((owner, allocation.clone()));
                Ok(ExecutionResourceLease {
                    resource_id: allocation.resource_id,
                })
            };
            self.registered.notify_one();
            Box::pin(std::future::ready(result))
        }

        fn bind_external(
            &self,
            lease: ExecutionResourceLease,
            external_id: String,
        ) -> BoxFuture<'static, Result<(), String>> {
            let mut allocations = self.allocations.lock().expect("allocations lock");
            let Some((_, allocation)) = allocations
                .iter_mut()
                .find(|(_, allocation)| allocation.resource_id == lease.resource_id)
            else {
                return Box::pin(std::future::ready(Err(
                    "resource reservation not found".to_owned()
                )));
            };
            allocation.external_id = Some(external_id);
            Box::pin(std::future::ready(Ok(())))
        }

        fn finish(
            &self,
            lease: ExecutionResourceLease,
            terminal: ExecutionResourceTerminal,
        ) -> BoxFuture<'static, Result<(), String>> {
            self.terminals
                .lock()
                .expect("terminals lock")
                .push((lease.resource_id, terminal));
            self.finished.notify_one();
            Box::pin(std::future::ready(Ok(())))
        }
    }

    fn ctx() -> (ToolContext, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (ToolContext::new(CancellationToken::new(), tx), rx)
    }

    fn supervised_ctx(
        cancel: CancellationToken,
        observer: Arc<RecordingResourceObserver>,
    ) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(cancel, tx).with_execution_resources(
            ExecutionResourceOwner {
                task_id: uuid::Uuid::new_v4().to_string(),
                run_id: uuid::Uuid::new_v4().to_string(),
                invocation_id: uuid::Uuid::new_v4().to_string(),
            },
            observer,
        )
    }

    #[tokio::test]
    async fn process_group_is_registered_and_positively_released() {
        let observer = Arc::new(RecordingResourceObserver::default());
        let ctx = supervised_ctx(CancellationToken::new(), Arc::clone(&observer));
        let outcome = run_shell(
            "printf supervised",
            &std::env::temp_dir(),
            Duration::from_secs(10),
            &ctx,
        )
        .await
        .expect("supervised process");
        assert_eq!(outcome.stdout, "supervised");
        assert_eq!(ctx.execution_cleanup_status(), ToolCleanupStatus::Confirmed);

        let allocations = observer.allocations.lock().expect("allocations lock");
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0].1.resource_kind, "processGroup");
        assert!(allocations[0].1.external_id.is_some());
        assert_eq!(
            observer
                .reserved_external_ids
                .lock()
                .expect("reservation lock")
                .as_slice(),
            [None],
            "durable reservation must precede physical spawn and PID binding"
        );
        uuid::Uuid::parse_str(&allocations[0].1.resource_id).expect("full resource UUID");
        let resource_id = allocations[0].1.resource_id.clone();
        drop(allocations);
        assert_eq!(
            observer
                .terminals
                .lock()
                .expect("terminals lock")
                .as_slice(),
            [(resource_id, ExecutionResourceTerminal::Released)]
        );
    }

    #[tokio::test]
    async fn dropping_caller_future_does_not_drop_process_cleanup_responsibility() {
        let observer = Arc::new(RecordingResourceObserver::default());
        let cancel = CancellationToken::new();
        let ctx = supervised_ctx(cancel.clone(), Arc::clone(&observer));
        let caller = tokio::spawn(async move {
            run_shell(
                "sleep 30",
                &std::env::temp_dir(),
                Duration::from_mins(1),
                &ctx,
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(2), observer.registered.notified())
            .await
            .expect("resource registration");
        cancel.cancel();
        caller.abort();
        tokio::time::timeout(Duration::from_secs(3), observer.finished.notified())
            .await
            .expect("detached supervisor cleanup");

        assert!(matches!(
            observer
                .terminals
                .lock()
                .expect("terminals lock")
                .as_slice(),
            [(_, ExecutionResourceTerminal::Released)]
        ));
        #[cfg(unix)]
        {
            let pid = observer.allocations.lock().expect("allocations lock")[0]
                .1
                .external_id
                .as_deref()
                .expect("process group id")
                .parse::<u32>()
                .expect("numeric process group id");
            assert!(
                !process_group_exists(pid),
                "released process group is alive"
            );
        }
    }

    #[tokio::test]
    async fn registration_failure_fails_closed_and_marks_unconfirmed() {
        let observer = Arc::new(RecordingResourceObserver {
            reject_registration: true,
            ..RecordingResourceObserver::default()
        });
        let ctx = supervised_ctx(CancellationToken::new(), Arc::clone(&observer));
        let error = run_shell(
            "sleep 30",
            &std::env::temp_dir(),
            Duration::from_mins(1),
            &ctx,
        )
        .await
        .expect_err("unowned process must fail closed");
        assert!(
            error
                .to_string()
                .contains("EXECUTION_RESOURCE_REGISTER_FAILED")
        );
        assert_eq!(
            ctx.execution_cleanup_status(),
            ToolCleanupStatus::Unconfirmed
        );
        assert!(matches!(
            observer
                .terminals
                .lock()
                .expect("terminals lock")
                .as_slice(),
            [(_, ExecutionResourceTerminal::Unconfirmed)]
        ));
    }

    #[tokio::test]
    async fn captures_stdout_and_reports_progress() {
        let (ctx, mut rx) = ctx();
        let outcome = run_shell(
            "echo alpha; echo beta",
            &std::env::temp_dir(),
            Duration::from_secs(10),
            &ctx,
        )
        .await
        .expect("spawn");
        assert_eq!(outcome.exit_code, 0);
        assert_eq!(outcome.stdout, "alpha\nbeta\n");
        assert!(!outcome.timed_out);
        assert_eq!(rx.recv().await.as_deref(), Some("alpha"));
        assert_eq!(rx.recv().await.as_deref(), Some("beta"));
    }

    #[tokio::test]
    async fn non_zero_exit_and_stderr_are_preserved() {
        let (ctx, _rx) = ctx();
        let outcome = run_shell(
            "echo oops 1>&2; exit 3",
            &std::env::temp_dir(),
            Duration::from_secs(10),
            &ctx,
        )
        .await
        .expect("spawn");
        assert_eq!(outcome.exit_code, 3);
        assert_eq!(outcome.stderr, "oops\n");
    }

    #[tokio::test]
    async fn timeout_kills_the_process_group() {
        let (ctx, _rx) = ctx();
        let started = Instant::now();
        let outcome = run_shell(
            "sleep 30",
            &std::env::temp_dir(),
            Duration::from_millis(200),
            &ctx,
        )
        .await
        .expect("spawn");
        assert!(outcome.timed_out);
        assert_eq!(outcome.exit_code, TIMEOUT_EXIT_CODE);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn cancellation_terminates_immediately() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let ctx = ToolContext::new(cancel.clone(), tx);
        let handle = tokio::spawn(async move {
            run_shell(
                "sleep 30",
                &std::env::temp_dir(),
                Duration::from_mins(1),
                &ctx,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let outcome = handle.await.expect("join").expect("spawn");
        assert!(outcome.cancelled);
        assert!(!outcome.timed_out);
    }

    /// 标记变量：内层（被清理侧）复跑的判别键，避免无限自递归。
    const REEXEC_MARK: &str = "ZK_TSG_REEXEC";

    /// 非敏感对照变量：必须**存活**穿过 [`spawn`] 的清理。
    const KEEP_VAR: &str = "ZK_TSG_KEEP_ME";

    /// 敏感环境变量不进子进程；非敏感变量原样保留（对照旧
    /// `ToolSafetyGuard#sanitizeProcessEnvironment`）。
    ///
    /// 为什么要自我重执行：`std::env::set_var` 在 edition 2024 为 `unsafe`，
    /// workspace `unsafe_code = "forbid"` 禁用，测试进程无法自行注入敏感变量。
    /// 故外层以带敏感变量的环境重跑本测试自身（`current_exe` + 精确过滤），
    /// 内层再经 [`run_shell`] 观察孙进程实际拿到的环境。
    #[tokio::test]
    async fn sensitive_env_vars_are_stripped_from_child() {
        if std::env::var_os(REEXEC_MARK).is_none() {
            let exe = std::env::current_exe().expect("current exe");
            let mut outer = Command::new(exe);
            outer
                .args([
                    "process::tests::sensitive_env_vars_are_stripped_from_child",
                    "--exact",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(REEXEC_MARK, "1")
                .env(KEEP_VAR, "kept");
            for var in SENSITIVE_ENV_VARS {
                outer.env(var, "leaked-secret");
            }
            let status = outer.status().await.expect("re-exec test binary");
            assert!(status.success(), "inner (sanitized) assertions failed");
            return;
        }

        let (ctx, _rx) = ctx();
        let outcome = run_shell("env", &std::env::temp_dir(), Duration::from_secs(30), &ctx)
            .await
            .expect("spawn");
        assert_eq!(outcome.exit_code, 0);
        let names: Vec<&str> = outcome
            .stdout
            .lines()
            .filter_map(|line| line.split_once('=').map(|(name, _)| name))
            .collect();
        for var in SENSITIVE_ENV_VARS {
            assert!(
                !names.contains(var),
                "sensitive var {var} leaked into child environment"
            );
        }
        assert!(
            !outcome.stdout.contains("leaked-secret"),
            "sensitive value leaked into child environment"
        );
        assert!(
            names.contains(&KEEP_VAR),
            "non-sensitive var {KEEP_VAR} was wrongly stripped"
        );
        assert!(names.contains(&"PATH"), "PATH was wrongly stripped");
    }

    #[tokio::test]
    async fn capture_is_bounded_at_one_mib() {
        let (ctx, _rx) = ctx();
        let outcome = run_shell(
            "line=$(printf 'x%.0s' $(seq 1 64)); for i in $(seq 1 20000); do echo \"$line\"; done",
            &std::env::temp_dir(),
            Duration::from_mins(1),
            &ctx,
        )
        .await
        .expect("spawn");
        assert!(outcome.truncated);
        assert!(outcome.stdout.len() <= MAX_CAPTURE_BYTES);
        assert!(outcome.stdout.starts_with("xxxxxxxx"));
        assert!(outcome.stdout.contains("output truncated; tail follows"));
        assert!(
            outcome
                .stdout
                .ends_with("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n")
        );
    }

    #[tokio::test]
    async fn no_newline_output_keeps_bounded_head_and_tail() {
        let (ctx, _rx) = ctx();
        let outcome = run_shell(
            "printf HEAD; head -c 4194304 /dev/zero | tr '\\0' x; printf TAIL",
            &std::env::temp_dir(),
            Duration::from_mins(1),
            &ctx,
        )
        .await
        .expect("spawn");
        assert!(outcome.truncated);
        assert!(outcome.stdout.len() <= MAX_CAPTURE_BYTES);
        assert!(outcome.stdout.starts_with("HEAD"));
        assert!(outcome.stdout.ends_with("TAIL"));
    }
}
