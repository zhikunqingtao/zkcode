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

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use crate::tool::ToolContext;

/// 单流采集上限（旧 `maxCaptureBytes = 1048576` 逐字对照）。
pub const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

/// SIGTERM → SIGKILL 宽限期（任务判据 5s；旧 `terminateGraceMs = 1000`）。
pub const TERMINATE_GRACE: Duration = Duration::from_secs(5);

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
    let mut child = spawn(program, args, working_dir)?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let progress = ctx.clone();
    let stdout_task = tokio::spawn(async move { pump(stdout, Some(progress)).await });
    let stderr_task = tokio::spawn(async move { pump(stderr, None).await });
    let started = Instant::now();
    let mut timed_out = false;
    let mut cancelled = false;
    let exit_code = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => {
            cancelled = true;
            terminate(&mut child).await;
            SIGTERM_EXIT_CODE
        }
        status = tokio::time::timeout(timeout, child.wait()) => match status {
            Ok(Ok(status)) => exit_code_of(status),
            Ok(Err(_)) => SIGNAL_EXIT_BASE,
            Err(_) => {
                timed_out = true;
                terminate(&mut child).await;
                TIMEOUT_EXIT_CODE
            }
        },
    };
    let (stdout, out_truncated) = stdout_task.await.unwrap_or_default();
    let (stderr, err_truncated) = stderr_task.await.unwrap_or_default();
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        exit_code,
        timed_out,
        cancelled,
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

/// 按行读取一条流，累积至 [`MAX_CAPTURE_BYTES`]；`progress` 非空时逐行上报。
async fn pump<R>(reader: Option<R>, progress: Option<ToolContext>) -> (String, bool)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let Some(reader) = reader else {
        return (String::new(), false);
    };
    let mut lines = BufReader::new(reader).lines();
    let mut collected = String::new();
    let mut truncated = false;
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(ctx) = progress.as_ref() {
            ctx.report_progress(line.clone());
        }
        // 触达上限后**继续抽干**管道（不再累积）：否则子进程写满管道
        // 阻塞在 write 上，wait 永不返回，只能等超时兜底。
        if collected.len() + line.len() + 1 > MAX_CAPTURE_BYTES {
            truncated = true;
            continue;
        }
        collected.push_str(&line);
        collected.push('\n');
    }
    (collected, truncated)
}

/// 终止整进程组：SIGTERM → 宽限 [`TERMINATE_GRACE`] → SIGKILL。
///
/// 组 ID = 子进程 pid（[`spawn`] 内 `process_group(0)` 使其成为组长）；
/// 非 unix 平台退化为直接 kill 子进程。
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
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> (ToolContext, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (ToolContext::new(CancellationToken::new(), tx), rx)
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
    }
}
