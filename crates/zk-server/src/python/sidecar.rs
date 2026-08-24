//! Python 侧车进程生命周期管理——逐条对照旧 `PythonProcessManager.java`。
//!
//! 旧实现以 `ProcessBuilder("python","-m","uvicorn","src.main:app","--host",h,
//! "--port",p)` 起 TCP 监听；本实现按 D-P2-2 改走 Unix Domain Socket
//! （`uvicorn --uds <sock>`），HTTP 契约与 Python 本体零改动，仅传输层从
//! loopback TCP 换为 UDS（省端口占用 / 免端口冲突 / 文件权限即访问控制）。
//!
//! 逐条对齐的生命周期常量（旧源行号见 `docs/compatibility.md` §6 偏离表）：
//! - `MAX_RESTART_ATTEMPTS = 3`（`PythonProcessManager.java:39`）；
//! - `RESTART_DELAY = 5s`（同上 :42）；
//! - 健康轮询间隔 30s（`application.yml` `python.service.health-check-interval`）；
//! - `stop()` 宽限 10s 后强杀（同上 :139-142）；
//! - `start()` 后固定等 2s 再首探健康（同上 :105-107）。
//!
//! 两处受判据驱动的增强（`ENHANCED`，见 §6 偏离表）：
//! 1. 1s 粒度 liveness 探测：旧端仅 30s 健康轮询，`kill` 侧车后最坏
//!    30 + 5 + 2 ≈ 37s 才恢复，达不到「10s 内自动重启」判据；本实现在监督循环
//!    内以 1s 间隔 `try_wait()` 检出进程消失，≈7s 完成重启。
//! 2. 启动预算轮询：旧端 2s 后单次探测，Python 冷启（tree-sitter / playwright
//!    导入）慢于 2s 即永久 `FAILED`；本实现 2s 后按 500ms 间隔轮询至 30s 预算。

use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, Ordering};
use std::time::Duration;

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use super::client::PythonClient;
use crate::iso::now_millis;

/// 最大连续重启次数（`PythonProcessManager.java:39` `MAX_RESTART_ATTEMPTS = 3`）。
pub const MAX_RESTART_ATTEMPTS: u32 = 3;

/// 重启间隔（`PythonProcessManager.java:42` `RESTART_DELAY_MS = 5000`）。
pub const RESTART_DELAY: Duration = Duration::from_secs(5);

/// 启动后首探健康前的固定等待（`PythonProcessManager.java:105` `sleep(2000)`）。
pub const STARTUP_WAIT: Duration = Duration::from_secs(2);

/// 启动健康轮询预算上限（ENHANCED：旧端为 2s 后单次探测）。
pub const STARTUP_BUDGET: Duration = Duration::from_secs(30);

/// 启动健康轮询间隔（ENHANCED，同上）。
pub const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Interpreter version/import preflight timeout.
pub const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(10);

/// 默认健康轮询间隔（`application.yml` `health-check-interval: 30000`）。
pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// liveness 探测间隔（ENHANCED：支撑「kill 后 10s 内自动重启」判据）。
pub const LIVENESS_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// 优雅停止宽限（`PythonProcessManager.java:139` `waitFor(10, SECONDS)`）。
pub const STOP_GRACE: Duration = Duration::from_secs(10);

/// 停止阶段的 `try_wait` 轮询间隔（Java `Process.waitFor(timeout)` 的等价实现）。
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// 侧车进程状态（逐项对齐 `PythonProcessManager.ProcessState`，:62-69）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    /// 未启动 / 已停止。
    Stopped,
    /// 启动中（进程已 spawn，健康未确认）。
    Starting,
    /// 运行中且健康。
    Running,
    /// 进程在但健康检查失败。
    HealthCheckFailed,
    /// 正在重启。
    Restarting,
    /// 启动失败或重启次数耗尽，需人工介入。
    Failed,
}

impl ProcessState {
    /// 状态码（`AtomicU8` 承载用；顺序与枚举声明一致）。
    const fn code(self) -> u8 {
        match self {
            Self::Stopped => 0,
            Self::Starting => 1,
            Self::Running => 2,
            Self::HealthCheckFailed => 3,
            Self::Restarting => 4,
            Self::Failed => 5,
        }
    }

    /// 状态码 → 枚举（未知码兜底 `Stopped`，与 `AtomicU8` 初值一致）。
    const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::HealthCheckFailed,
            4 => Self::Restarting,
            5 => Self::Failed,
            _ => Self::Stopped,
        }
    }

    /// 对外线上表示（`/api/health` 聚合与日志共用，逐字沿用旧枚举名）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "STOPPED",
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
            Self::HealthCheckFailed => "HEALTH_CHECK_FAILED",
            Self::Restarting => "RESTARTING",
            Self::Failed => "FAILED",
        }
    }
}

/// 侧车启动参数（由 `Config` 装配，集成测试可直接构造）。
#[derive(Debug, Clone)]
pub struct SidecarConfig {
    /// UDS 套接字路径（`ZK_PYTHON_UDS`，默认 `~/.zkcode/python.sock`）。
    pub socket: PathBuf,
    /// `python-service` 目录（uvicorn 的工作目录，对齐旧 :92-95）。
    pub service_dir: PathBuf,
    /// Authorized project workspace exported to Python path-aware services.
    pub workspace_root: PathBuf,
    /// Python 解释器路径；`None` 时按 `start.sh:83-96` 顺序探测。
    pub python_command: Option<String>,
    /// 健康轮询间隔（`application.yml` 同名配置）。
    pub health_check_interval: Duration,
}

impl SidecarConfig {
    /// 解析实际使用的 Python 解释器。
    ///
    /// 旧 `PythonProcessManager.java:85` 硬编码 `"python"`——macOS 与多数
    /// Linux 发行版无该符号（仅 `python3`），故按 `start.sh:83-96` 的探测顺序
    /// 兜底：`{dir}/.venv/bin/python` → `{dir}/venv/bin/python` → `python3.11`
    /// → `python3.12`。不允许回退到版本不明的 `python3`。
    #[must_use]
    pub fn resolve_python_command(&self) -> Option<String> {
        if let Some(explicit) = &self.python_command {
            return Some(explicit.clone());
        }
        for venv in [".venv", "venv"] {
            let candidate = self.service_dir.join(venv).join("bin").join("python");
            if candidate.is_file() {
                return Some(candidate.to_string_lossy().into_owned());
            }
        }
        for binary in ["python3.11", "python3.12"] {
            if which(binary) {
                return Some(binary.to_owned());
            }
        }
        None
    }
}

/// `PATH` 内是否存在可执行文件（`command -v` 的最小等价实现）。
fn which(binary: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| dir.join(binary).is_file())
}

/// Python 侧车管理器。
///
/// 状态机与旧 `PythonProcessManager` 同构；`lifecycle` 互斥锁承担旧端
/// `synchronized start/stop/restart` 的串行化职责（Rust 侧改 async 锁，避免
/// 在 runtime 线程上阻塞）。
pub struct PythonSidecar {
    config: SidecarConfig,
    client: Arc<PythonClient>,
    /// 生命周期锁 + 子进程句柄（一体化：持锁即可独占操作进程）。
    lifecycle: Mutex<Option<Child>>,
    state: AtomicU8,
    restart_count: AtomicU32,
    /// 最近一次健康检查时刻（epoch 毫秒；0 表示尚未检查过）。
    last_health_check: AtomicI64,
}

impl PythonSidecar {
    /// 装配侧车管理器（不启动进程）。
    #[must_use]
    pub fn new(config: SidecarConfig, client: Arc<PythonClient>) -> Self {
        Self {
            config,
            client,
            lifecycle: Mutex::new(None),
            state: AtomicU8::new(ProcessState::Stopped.code()),
            restart_count: AtomicU32::new(0),
            last_health_check: AtomicI64::new(0),
        }
    }

    /// 当前状态（`PythonProcessManager.getState()`，:231-233）。
    #[must_use]
    pub fn state(&self) -> ProcessState {
        ProcessState::from_code(self.state.load(Ordering::Acquire))
    }

    /// 是否运行中（`isRunning()`，:235-237）。
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.state() == ProcessState::Running
    }

    /// 最近健康检查时刻的 epoch 毫秒（`getLastHealthCheck()`，:239-241；
    /// 0 表示从未检查）。
    #[must_use]
    pub fn last_health_check_millis(&self) -> i64 {
        self.last_health_check.load(Ordering::Acquire)
    }

    /// 连续重启计数（`getRestartCount()`，:243-245）。
    #[must_use]
    pub fn restart_count(&self) -> u32 {
        self.restart_count.load(Ordering::Acquire)
    }

    /// 服务地址（`getServiceUrl()`，:247-249；UDS 下形如 `unix:/path.sock`）。
    #[must_use]
    pub fn service_url(&self) -> String {
        format!("unix:{}", self.config.socket.display())
    }

    /// 套接字路径。
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.config.socket
    }

    fn set_state(&self, next: ProcessState) {
        self.state.store(next.code(), Ordering::Release);
    }

    /// 启动侧车（`start()`，:74-123）。返回是否启动成功。
    pub async fn start(&self) -> bool {
        let mut guard = self.lifecycle.lock().await;
        self.start_locked(&mut guard).await
    }

    /// 停止侧车（`stop()`，:128-151）。
    pub async fn stop(&self) {
        let mut guard = self.lifecycle.lock().await;
        self.stop_locked(&mut guard).await;
    }

    /// 重启侧车（`restart()`，:156-164：stop → sleep 5s → start）。
    pub async fn restart(&self) -> bool {
        let mut guard = self.lifecycle.lock().await;
        self.stop_locked(&mut guard).await;
        tokio::time::sleep(RESTART_DELAY).await;
        self.start_locked(&mut guard).await
    }

    /// 健康检查（`checkHealth()`，:169-195）。
    ///
    /// 与旧端唯一实质差异：旧端打 `/health`，而 python-service 只暴露
    /// `/api/health`（`main.py` 的 `health_router` 前缀 `/api`），旧端该检查
    /// 恒 404 → 恒不健康；此处修正为 `/api/health`（`MUST_FIX-fixed`）。
    pub async fn check_health(&self) -> bool {
        let healthy = self.client.is_healthy().await;
        self.last_health_check
            .store(now_millis(), Ordering::Release);
        if healthy && self.state() == ProcessState::HealthCheckFailed {
            self.set_state(ProcessState::Running);
            self.restart_count.store(0, Ordering::Release);
        }
        healthy
    }

    /// 监督循环：liveness（1s）+ 健康轮询（默认 30s）+ 受限自动重启。
    ///
    /// 健康分支逐条对齐 `scheduledHealthCheck()`（:200-222）：仅在
    /// `RUNNING` / `HEALTH_CHECK_FAILED` 生效；失败即置 `HEALTH_CHECK_FAILED`
    /// 并递增计数，`<= MAX_RESTART_ATTEMPTS` 则 `RESTARTING` + `restart()`，
    /// 否则 `FAILED` 停止重试。
    pub async fn supervise(self: Arc<Self>) {
        let mut since_health = Duration::ZERO;
        loop {
            tokio::time::sleep(LIVENESS_POLL_INTERVAL).await;
            since_health += LIVENESS_POLL_INTERVAL;

            let supervised = matches!(
                self.state(),
                ProcessState::Running | ProcessState::HealthCheckFailed
            );
            if !supervised {
                since_health = Duration::ZERO;
                continue;
            }

            // ENHANCED：进程已消失（外部 kill / 自身崩溃）时立刻走重启，不等
            // 下一个健康轮询窗口——判据要求 kill 后 10s 内自动重启。
            if self.process_exited().await {
                tracing::warn!("python sidecar process exited unexpectedly");
                self.handle_failure().await;
                since_health = Duration::ZERO;
                continue;
            }

            if since_health < self.config.health_check_interval {
                continue;
            }
            since_health = Duration::ZERO;
            if !self.check_health().await {
                self.handle_failure().await;
            }
        }
    }

    /// 进程是否已退出（`try_wait` 收敛僵尸；无句柄时视为已退出）。
    async fn process_exited(&self) -> bool {
        let mut guard = self.lifecycle.lock().await;
        match guard.as_mut() {
            None => true,
            Some(child) => matches!(child.try_wait(), Ok(Some(_)) | Err(_)),
        }
    }

    /// 失败处置（`scheduledHealthCheck()` :207-221 的重启配额逻辑）。
    async fn handle_failure(&self) {
        self.set_state(ProcessState::HealthCheckFailed);
        let attempts = self.restart_count.fetch_add(1, Ordering::AcqRel) + 1;
        if attempts <= MAX_RESTART_ATTEMPTS {
            tracing::warn!(
                attempts,
                max = MAX_RESTART_ATTEMPTS,
                "python sidecar health check failed, attempting restart"
            );
            self.set_state(ProcessState::Restarting);
            self.restart().await;
        } else {
            tracing::error!(
                max = MAX_RESTART_ATTEMPTS,
                "python sidecar restart limit reached, manual restart required"
            );
            self.set_state(ProcessState::Failed);
        }
    }

    /// 持锁启动实现。
    async fn start_locked(&self, guard: &mut Option<Child>) -> bool {
        if self.state() == ProcessState::Running {
            tracing::warn!("python sidecar is already running");
            return true;
        }
        self.set_state(ProcessState::Starting);
        let socket = &self.config.socket;
        tracing::info!(socket = %socket.display(), "starting python sidecar");

        if let Err(error) = self.preflight().await {
            self.set_state(ProcessState::Failed);
            tracing::error!(reason = %error, "python sidecar preflight failed");
            return false;
        }

        // 清理残留 socket 文件：uvicorn 对已存在的 UDS 路径会 bind 失败
        // （`Address already in use`），进程上次被 SIGKILL 时必然残留。
        if let Err(error) = prepare_socket_path(socket) {
            self.set_state(ProcessState::Failed);
            tracing::error!(%error, "failed to prepare python sidecar socket path");
            return false;
        }

        let child = match self.spawn(socket) {
            Ok(child) => child,
            Err(error) => {
                self.set_state(ProcessState::Failed);
                tracing::error!(%error, "failed to start python sidecar");
                return false;
            }
        };
        *guard = Some(child);
        if let Some(child) = guard.as_mut() {
            drain_output(child);
        }

        // 旧端：sleep 2000 后单次 checkHealth。此处保留 2s 起步等待，之后按
        // 500ms 轮询至 30s 预算（ENHANCED，容忍 Python 冷启动）。
        tokio::time::sleep(STARTUP_WAIT).await;
        let deadline = tokio::time::Instant::now() + STARTUP_BUDGET;
        loop {
            let alive = guard
                .as_mut()
                .is_some_and(|child| matches!(child.try_wait(), Ok(None)));
            if alive
                && socket.exists()
                && let Err(error) = enforce_socket_permissions(socket)
            {
                self.set_state(ProcessState::Failed);
                tracing::error!(%error, "python sidecar socket permission hardening failed");
                return false;
            }
            if alive && self.check_health().await {
                self.set_state(ProcessState::Running);
                // 能力缓存跨重启失效并**立即**重探：侧车此刻已确认健康，主动
                // 刷新既让 `/api/health` 的能力计数不失真（否则最长 5min TTL
                // 内恒显示 0/0），也免去首个工具调用付探测延迟。
                self.client.invalidate_capabilities();
                self.client.refresh_capabilities().await;
                tracing::info!(socket = %socket.display(), "python sidecar started");
                return true;
            }
            if !alive || tokio::time::Instant::now() >= deadline {
                self.set_state(ProcessState::Failed);
                tracing::error!("python sidecar failed to start");
                return false;
            }
            tokio::time::sleep(STARTUP_POLL_INTERVAL).await;
        }
    }

    /// spawn `uvicorn --uds`（对齐旧 :84-98，端口参数换为 `--uds`）。
    fn spawn(&self, socket: &Path) -> std::io::Result<Child> {
        let python = self.config.resolve_python_command().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Python 3.11 or 3.12 interpreter was not found",
            )
        })?;
        let mut command = Command::new(&python);
        command
            .arg("-m")
            .arg("uvicorn")
            .arg("src.main:app")
            .arg("--uds")
            .arg(socket)
            // 旧 :92-95 设 cwd=python-service；目录不存在时（打包分发未带
            // python-service）不设，等价于旧端「存在才设」的分支。
            .env("PYTHONPATH", "./src")
            .env("PYTHONPYCACHEPREFIX", self.pycache_dir())
            .env("WORKSPACE_ROOT", &self.config.workspace_root)
            .stdin(Stdio::null())
            // 旧 :97 `redirectErrorStream(true)`：stderr 并入 stdout 同一路
            // drain；Rust 侧无合流 API，故两路各起 drain 任务，日志形状等价。
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // 进程组独立：优雅停止走 killpg（uvicorn 的 reloader / worker 子进程
            // 一并收敛），对齐 Java `Process.destroy()` 的进程树语义。
            .process_group(0)
            .kill_on_drop(false);
        if self.config.service_dir.is_dir() {
            command.current_dir(&self.config.service_dir);
        }
        command.spawn()
    }

    async fn preflight(&self) -> Result<(), String> {
        let python = self
            .config
            .resolve_python_command()
            .ok_or_else(|| "Python 3.11 or 3.12 interpreter was not found".to_owned())?;
        let mut command = Command::new(&python);
        command
            .arg("-c")
            .arg(
                "import sys; assert sys.version_info[:2] in ((3,11),(3,12)); \
                 import uvicorn, fastapi, pydantic; import src.main; \
                 print(f'{sys.version_info.major}.{sys.version_info.minor}')",
            )
            .env("PYTHONPATH", "./src")
            .env("PYTHONPYCACHEPREFIX", self.pycache_dir())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if self.config.service_dir.is_dir() {
            command.current_dir(&self.config.service_dir);
        }
        let output = tokio::time::timeout(PREFLIGHT_TIMEOUT, command.output())
            .await
            .map_err(|_| "Python version/import preflight timed out".to_owned())?
            .map_err(|_| "Python interpreter could not be started".to_owned())?;
        if !output.status.success() {
            return Err(
                "Python must be 3.11/3.12 and import uvicorn, fastapi, pydantic, src.main"
                    .to_owned(),
            );
        }
        let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !matches!(version.as_str(), "3.11" | "3.12") {
            return Err(format!(
                "unsupported Python version reported by preflight: {version}"
            ));
        }
        tracing::info!(python_version = %version, "python sidecar preflight passed");
        Ok(())
    }

    fn pycache_dir(&self) -> PathBuf {
        self.config
            .socket
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("python-pycache")
    }

    /// 持锁停止实现（`stop()` :128-151 + UDS 残留清理）。
    async fn stop_locked(&self, guard: &mut Option<Child>) {
        let Some(mut child) = guard.take() else {
            self.set_state(ProcessState::Stopped);
            cleanup_socket(&self.config.socket);
            return;
        };
        if matches!(child.try_wait(), Ok(Some(_))) {
            self.set_state(ProcessState::Stopped);
            cleanup_socket(&self.config.socket);
            return;
        }

        tracing::info!("stopping python sidecar");
        // Graceful signal goes to uvicorn only. It must run FastAPI lifespan shutdown
        // before Playwright's driver child exits; the whole group is reserved for the
        // force-kill fallback below.
        signal_child(&child, Signal::SIGTERM);

        // Java `waitFor(10, SECONDS)` 的等价：轮询 try_wait 至宽限耗尽。
        let deadline = tokio::time::Instant::now() + STOP_GRACE;
        let mut exited = false;
        while tokio::time::Instant::now() < deadline {
            if matches!(child.try_wait(), Ok(Some(_)) | Err(_)) {
                exited = true;
                break;
            }
            tokio::time::sleep(STOP_POLL_INTERVAL).await;
        }
        if !exited {
            // Java `destroyForcibly()` = SIGKILL。
            signal_group(&child, Signal::SIGKILL);
            let _ = child.wait().await;
            tracing::warn!("python sidecar force-killed");
        }

        self.set_state(ProcessState::Stopped);
        self.client.invalidate_capabilities();
        cleanup_socket(&self.config.socket);
        tracing::info!("python sidecar stopped");
    }
}

/// 向子进程所在进程组发信号（`process_group(0)` 后 pid == pgid）。
fn signal_group(child: &Child, signal: Signal) {
    let Some(pid) = child.id() else {
        return;
    };
    let Ok(raw) = i32::try_from(pid) else {
        return;
    };
    if let Err(error) = killpg(Pid::from_raw(raw), signal) {
        tracing::debug!(%error, ?signal, "python sidecar signal delivery failed");
    }
}

fn signal_child(child: &Child, signal: Signal) {
    let Some(pid) = child.id() else {
        return;
    };
    let Ok(raw) = i32::try_from(pid) else {
        return;
    };
    if let Err(error) = kill(Pid::from_raw(raw), signal) {
        tracing::debug!(%error, ?signal, "python sidecar signal delivery failed");
    }
}

/// 建立父目录并清理残留 socket 文件。
fn prepare_socket_path(socket: &Path) -> std::io::Result<()> {
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Never unlink a live listener or follow an attacker-controlled symlink. A stale
    // Unix socket is removable only after connect proves no process is accepting it.
    if let Ok(metadata) = std::fs::symlink_metadata(socket) {
        if metadata.file_type().is_symlink() || !metadata.file_type().is_socket() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "python sidecar socket path is not a Unix socket",
            ));
        }
        match StdUnixStream::connect(socket) {
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    "python sidecar socket is owned by a live listener",
                ));
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) =>
            {
                std::fs::remove_file(socket)?;
            }
            // macOS reports EPROTOTYPE when the path is a live Unix socket of a
            // different type (for example a datagram socket). Preserve it and
            // fail closed instead of treating it as a removable stale stream.
            Err(error) if error.raw_os_error() == Some(Errno::EPROTOTYPE as i32) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    "python sidecar socket path is owned by a non-stream listener",
                ));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn enforce_socket_permissions(socket: &Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(socket)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_socket() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "python sidecar socket path is not a Unix socket",
        ));
    }
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
}

/// 停止后清理残留 socket 文件（失败仅记日志，不影响状态迁移）。
fn cleanup_socket(socket: &Path) {
    let Ok(metadata) = std::fs::symlink_metadata(socket) else {
        return;
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_socket() {
        tracing::warn!(socket = %socket.display(), "refusing to remove non-socket sidecar path");
        return;
    }
    if let Err(error) = std::fs::remove_file(socket) {
        tracing::debug!(%error, socket = %socket.display(), "stale python socket cleanup failed");
    }
}

/// 后台 drain 子进程输出（对齐旧 `drainOutput`，:253-263：逐行 `debug!`）。
fn drain_output(child: &mut Child) {
    if let Some(stdout) = child.stdout.take() {
        tokio::spawn(pump(BufReader::new(stdout)));
    }
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(pump(BufReader::new(stderr)));
    }
}

/// 逐行泵出（EOF / 读错即结束，等价旧端「进程结束时正常退出」）。
async fn pump<R: tokio::io::AsyncRead + Unpin>(reader: BufReader<R>) {
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!("[python] {line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(dir: PathBuf) -> SidecarConfig {
        SidecarConfig {
            socket: dir.join("python.sock"),
            service_dir: dir.clone(),
            workspace_root: dir,
            python_command: None,
            health_check_interval: HEALTH_CHECK_INTERVAL,
        }
    }

    /// 生命周期常量逐条对齐 `PythonProcessManager.java` / `application.yml`。
    #[test]
    fn lifecycle_constants_match_baseline() {
        assert_eq!(MAX_RESTART_ATTEMPTS, 3);
        // 旧端以毫秒字面量表达（5000 / 2000 / 30000），此处等值换算为秒。
        assert_eq!(RESTART_DELAY, Duration::from_secs(5));
        assert_eq!(STARTUP_WAIT, Duration::from_secs(2));
        assert_eq!(STOP_GRACE, Duration::from_secs(10));
        assert_eq!(HEALTH_CHECK_INTERVAL, Duration::from_secs(30));
    }

    /// 状态枚举与旧 `ProcessState` 名称逐字一致，且码值往返无损。
    #[test]
    fn process_state_round_trips_and_keeps_baseline_names() {
        let all = [
            ProcessState::Stopped,
            ProcessState::Starting,
            ProcessState::Running,
            ProcessState::HealthCheckFailed,
            ProcessState::Restarting,
            ProcessState::Failed,
        ];
        for state in all {
            assert_eq!(ProcessState::from_code(state.code()), state);
        }
        assert_eq!(
            all.map(ProcessState::as_str),
            [
                "STOPPED",
                "STARTING",
                "RUNNING",
                "HEALTH_CHECK_FAILED",
                "RESTARTING",
                "FAILED"
            ]
        );
        // 未知码兜底 Stopped（AtomicU8 初值语义）。
        assert_eq!(ProcessState::from_code(200), ProcessState::Stopped);
    }

    /// 显式 `ZK_PYTHON_CMD` 优先；否则优先 venv 内解释器（`start.sh:83-96`）。
    #[test]
    fn python_command_resolution_follows_start_sh_order() {
        let dir = std::env::temp_dir().join(format!("zk-sidecar-cmd-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("venv").join("bin")).expect("venv bin dir");
        let venv_python = dir.join("venv").join("bin").join("python");
        std::fs::write(&venv_python, b"#!/bin/sh\n").expect("venv python stub");

        let mut cfg = config(dir.clone());
        assert_eq!(
            cfg.resolve_python_command(),
            Some(venv_python.to_string_lossy().into_owned())
        );

        cfg.python_command = Some("/usr/bin/python3.12".to_owned());
        assert_eq!(
            cfg.resolve_python_command().as_deref(),
            Some("/usr/bin/python3.12")
        );

        let empty = std::env::temp_dir().join(format!("zk-sidecar-empty-{}", std::process::id()));
        std::fs::create_dir_all(&empty).expect("empty dir");
        let resolved = config(empty.clone()).resolve_python_command();
        assert!(matches!(
            resolved.as_deref(),
            Some("python3.11" | "python3.12") | None
        ));

        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&empty).ok();
    }

    /// 只清理确认无监听者的 stale socket；普通文件和活跃 listener 失败关闭。
    #[test]
    fn socket_preparation_is_type_safe_liveness_aware_and_mode_0600() {
        // macOS `sockaddr_un.sun_path` is only 104 bytes; use an intentionally short root.
        let dir = PathBuf::from(format!("/tmp/zk-sc-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        let socket = dir.join("nested").join("python.sock");

        prepare_socket_path(&socket).expect("parent dirs created");
        assert!(socket.parent().expect("parent").is_dir());

        std::fs::write(&socket, b"not a socket").expect("regular file");
        assert_eq!(
            prepare_socket_path(&socket)
                .expect_err("regular file rejected")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        cleanup_socket(&socket);
        assert_eq!(
            std::fs::read(&socket).expect("cleanup preserves regular file"),
            b"not a socket"
        );
        std::fs::remove_file(&socket).expect("remove regular file");

        // A dropped stream listener is the same socket type the Python sidecar
        // uses and deterministically models a crashed process's stale UDS inode.
        let stale = std::os::unix::net::UnixListener::bind(&socket).expect("stale socket");
        drop(stale);
        prepare_socket_path(&socket).expect("unowned stale socket removed");
        assert!(std::fs::symlink_metadata(&socket).is_err());

        #[cfg(target_os = "macos")]
        {
            // EPROTOTYPE means a different socket type is still live. Preserve
            // the path and normalize the platform error to fail closed.
            let datagram =
                std::os::unix::net::UnixDatagram::bind(&socket).expect("live datagram listener");
            assert_eq!(
                prepare_socket_path(&socket)
                    .expect_err("live non-stream socket preserved")
                    .kind(),
                std::io::ErrorKind::AddrInUse
            );
            assert!(std::fs::symlink_metadata(&socket).is_ok());
            drop(datagram);
            std::fs::remove_file(&socket).expect("remove datagram socket");
        }

        let live = std::os::unix::net::UnixListener::bind(&socket).expect("live listener");
        assert_eq!(
            prepare_socket_path(&socket)
                .expect_err("live socket preserved")
                .kind(),
            std::io::ErrorKind::AddrInUse
        );
        enforce_socket_permissions(&socket).expect("chmod");
        assert_eq!(
            std::fs::symlink_metadata(&socket)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        drop(live);
        cleanup_socket(&socket);
        assert!(std::fs::symlink_metadata(&socket).is_err());
        // 不存在时 cleanup 幂等静默。
        cleanup_socket(&socket);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// 无 Python 环境时 `start()` 落 `FAILED` 且不 panic；`stop()` 幂等。
    #[tokio::test]
    async fn start_without_python_service_fails_and_stop_is_idempotent() {
        let dir = std::env::temp_dir().join(format!("zk-sidecar-nopy-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let mut cfg = config(dir.clone());
        // 指向必然不存在的解释器：spawn 立即失败，无需等启动预算。
        cfg.python_command = Some(dir.join("no-such-python").to_string_lossy().into_owned());
        let client = Arc::new(PythonClient::new(cfg.socket.clone()));
        let sidecar = PythonSidecar::new(cfg, client);

        assert!(!sidecar.start().await);
        assert_eq!(sidecar.state(), ProcessState::Failed);
        assert!(!sidecar.is_running());

        sidecar.stop().await;
        assert_eq!(sidecar.state(), ProcessState::Stopped);
        sidecar.stop().await;
        assert_eq!(sidecar.state(), ProcessState::Stopped);
        assert_eq!(sidecar.restart_count(), 0);
        assert!(sidecar.service_url().starts_with("unix:"));

        std::fs::remove_dir_all(&dir).ok();
    }

    /// 未启动侧车时健康检查为 false 且记录检查时刻（降级路径不 panic）。
    #[tokio::test]
    async fn health_check_records_timestamp_even_when_down() {
        let dir = std::env::temp_dir().join(format!("zk-sidecar-hc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let cfg = config(dir.clone());
        let client = Arc::new(PythonClient::new(cfg.socket.clone()));
        let sidecar = PythonSidecar::new(cfg, client);

        assert_eq!(sidecar.last_health_check_millis(), 0);
        assert!(!sidecar.check_health().await);
        assert!(sidecar.last_health_check_millis() > 0);

        std::fs::remove_dir_all(&dir).ok();
    }
}
