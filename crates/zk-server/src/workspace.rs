//! 工作区服务层——Projects 域的路径校验 / 目录浏览 / 原生目录选择器（2.1）。
//!
//! 语义来源（旧仓库只读，2026-08-16 冻结）：`ProjectWorkspaceService.java`
//! （错误码 / HTTP 状态 / 消息文案 / 校验顺序逐条复刻）与
//! `NativeDirectoryPicker.java`（macOS osascript 实现；目标平台仅 macOS，
//! Windows PowerShell 分支不移植）。有意偏离（留痕 docs/compatibility.md
//! §2）：错误消息中的环境变量名换用本仓库前缀（`ZHIKUN_*` → `ZK_*`）；
//! picker 子进程仅采集 stdout（旧实现 `redirectErrorStream` 合并 stderr，
//! 输出上限判定含 stderr 噪声——AppleScript 结果只走 stdout，语义不变）。
//!
//! 分层：本模块为纯服务层（同步 fs 校验 + async picker 子进程），不持
//! axum 类型（`ApiError` 除外）；HTTP 侧（转发头判定 / header 守卫 /
//! `spawn_blocking` 包裹）见 `api::project`。

use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use serde::Serialize;

use crate::config::Config;
use crate::error::ApiError;

// ─── 错误构造（旧 WorkspaceException 三元组） ───

/// 构造带稳定错误码的 [`ApiError`]（旧 `WorkspaceException(status, code,
/// message)` 等价）。
pub(crate) fn failure(status: StatusCode, code: &str, message: &str) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: message.to_owned(),
    }
}

// ─── 线上形状（旧 service record 的 Jackson 序列化） ───

/// 目录浏览子项（旧 `DirectoryEntry` record：`{name,path}`）。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DirectoryEntry {
    /// 目录名（无文件名的极端路径回退全路径，对齐旧 `directoryEntry`）。
    pub name: String,
    /// 绝对路径。
    pub path: String,
}

/// 目录浏览响应（旧 `DirectoryListing` record；`parent` 为 null 时经
/// `NON_NULL` 剥离——current 即 owning root 时无上级）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DirectoryListing {
    /// 浏览根集合（allowed roots 或本地文件系统根）。
    pub roots: Vec<String>,
    /// 当前目录（canonical 绝对路径）。
    pub current: String,
    /// 上级目录（current == owning root 时剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// 可浏览子目录（名称大小写不敏感排序，再按路径）。
    pub directories: Vec<DirectoryEntry>,
    /// 本请求是否可打开原生目录选择器。
    pub native_picker_available: bool,
}

// ─── 名称与路径校验（旧 requireName / canonicalizeForCreate） ───

/// 项目名校验：trim 后 1..=80 字符（对齐旧 `requireName`；旧 Java 以
/// UTF-16 计长，此处以 Unicode 标量计，BMP 内完全一致）。
pub(crate) fn require_name(name: Option<&str>) -> Result<String, ApiError> {
    let trimmed = name.unwrap_or_default().trim();
    if trimmed.is_empty() || trimmed.chars().count() > 80 {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "PROJECT_NAME_INVALID",
            "Project name must contain 1 to 80 characters",
        ));
    }
    Ok(trimmed.to_owned())
}

/// 目录可读性判定（旧 `Files.isReadable` 在目录场景的等价：能否列举）。
fn is_readable_dir(path: &Path) -> bool {
    std::fs::read_dir(path).is_ok()
}

/// 词法归一化（旧 `Path.normalize()` 等价：消 `.`、弹出 `..`，不触盘）。
pub(crate) fn lexical_normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                // 根处 pop 无效（"/.." → "/"），与 Java normalize 一致。
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

/// 创建路径规范化（旧 `canonicalizeForCreate` 逐分支复刻）：空/非绝对/
/// 不存在/非目录/不可读/文件系统根/越 allowed roots 各回稳定错误码。
///
/// # Errors
/// 见旧实现映射表：400 `WORKSPACE_REQUIRED` / `WORKSPACE_ABSOLUTE_REQUIRED` /
/// `WORKSPACE_NOT_FOUND` / `WORKSPACE_PATH_INVALID` / `WORKSPACE_NOT_DIRECTORY`；
/// 403 `WORKSPACE_ACCESS_DENIED` / `WORKSPACE_ROOT_FORBIDDEN`。
pub(crate) fn canonicalize_for_create(
    config: &Config,
    value: Option<&str>,
) -> Result<PathBuf, ApiError> {
    let trimmed = value.unwrap_or_default().trim();
    if trimmed.is_empty() {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_REQUIRED",
            "workspaceRoot is required",
        ));
    }
    let path = Path::new(trimmed);
    if !path.is_absolute() {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_ABSOLUTE_REQUIRED",
            "workspaceRoot must be absolute",
        ));
    }
    let canonical = std::fs::canonicalize(path).map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_NOT_FOUND",
            "workspaceRoot does not exist",
        ),
        std::io::ErrorKind::PermissionDenied => failure(
            StatusCode::FORBIDDEN,
            "WORKSPACE_ACCESS_DENIED",
            "workspaceRoot is not accessible",
        ),
        _ => failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_PATH_INVALID",
            "workspaceRoot cannot be resolved",
        ),
    })?;
    if !canonical.is_dir() {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_NOT_DIRECTORY",
            "workspaceRoot must be a directory",
        ));
    }
    if !is_readable_dir(&canonical) {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "WORKSPACE_ACCESS_DENIED",
            "workspaceRoot is not readable",
        ));
    }
    if canonical.parent().is_none() {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "WORKSPACE_ROOT_FORBIDDEN",
            "The filesystem root cannot be a Project",
        ));
    }
    assert_within_allowed_roots(config, &canonical)?;
    Ok(canonical)
}

/// 存量绑定复核（旧 `requireCurrentBinding`）：库内保存的 workspace 此刻
/// 是否仍指向同一可用目录（消失 409 / 权限 403 / 重绑定 409）。
///
/// # Errors
/// 409 `WORKSPACE_UNAVAILABLE` / `WORKSPACE_REBOUND`；403
/// `WORKSPACE_ACCESS_DENIED`（含越 allowed roots）。
pub(crate) fn require_current_binding(
    config: &Config,
    saved_workspace_root: &str,
) -> Result<PathBuf, ApiError> {
    let saved = lexical_normalize(Path::new(saved_workspace_root.trim()));
    let current = std::fs::canonicalize(&saved).map_err(|err| match err.kind() {
        std::io::ErrorKind::PermissionDenied => failure(
            StatusCode::FORBIDDEN,
            "WORKSPACE_ACCESS_DENIED",
            "Workspace is not accessible",
        ),
        // NotFound 与其余故障同回 409（旧实现两 catch 同文案）。
        _ => failure(
            StatusCode::CONFLICT,
            "WORKSPACE_UNAVAILABLE",
            "Workspace is no longer available",
        ),
    })?;
    if !current.is_dir() || !is_readable_dir(&current) {
        return Err(failure(
            StatusCode::CONFLICT,
            "WORKSPACE_UNAVAILABLE",
            "Workspace is no longer an accessible directory",
        ));
    }
    if current != saved {
        return Err(failure(
            StatusCode::CONFLICT,
            "WORKSPACE_REBOUND",
            "Workspace path now resolves to a different directory",
        ));
    }
    assert_within_allowed_roots(config, &current)?;
    Ok(current)
}

/// allowed roots 边界（旧 `assertWithinAllowedRoots`：配置为空不设限）。
fn assert_within_allowed_roots(config: &Config, canonical: &Path) -> Result<(), ApiError> {
    if !config.workspace_allowed_roots.is_empty()
        && !config
            .workspace_allowed_roots
            .iter()
            .any(|root| canonical.starts_with(root))
    {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "WORKSPACE_ACCESS_DENIED",
            "Workspace is outside configured allowed roots",
        ));
    }
    Ok(())
}

// ─── 能力守卫（旧 assertCreateAllowed / assertBrowseAllowed 等） ───

/// 本地选择器开关守卫（旧 `assertLocalPickerEnabled`；消息中环境变量名
/// 换用 `ZK_*`，偏离留痕）。
fn assert_local_picker_enabled(config: &Config) -> Result<(), ApiError> {
    if config.local_picker_enabled {
        return Ok(());
    }
    Err(failure(
        StatusCode::FORBIDDEN,
        "LOCAL_PICKER_DISABLED",
        "Directory selection is disabled. For a direct local desktop server, \
         set ZK_LOCAL_PICKER_ENABLED=true; for remote or proxied deployments, \
         configure ZK_WORKSPACE_ALLOWED_ROOTS instead",
    ))
}

/// 创建守卫：allowed roots 非空直接放行；否则要求选择器开启 + 直连本机。
///
/// # Errors
/// 403 `LOCAL_PICKER_DISABLED` / `REMOTE_PROJECT_CREATE_FORBIDDEN`。
pub(crate) fn assert_create_allowed(
    config: &Config,
    caller_loopback: bool,
) -> Result<(), ApiError> {
    if !config.workspace_allowed_roots.is_empty() {
        return Ok(());
    }
    assert_local_picker_enabled(config)?;
    if !caller_loopback {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "REMOTE_PROJECT_CREATE_FORBIDDEN",
            "Remote Project creation requires ZK_WORKSPACE_ALLOWED_ROOTS",
        ));
    }
    Ok(())
}

/// 浏览守卫（与创建守卫同结构，远端错误码不同）。
///
/// # Errors
/// 403 `LOCAL_PICKER_DISABLED` / `REMOTE_DIRECTORY_BROWSE_FORBIDDEN`。
pub(crate) fn assert_browse_allowed(
    config: &Config,
    caller_loopback: bool,
) -> Result<(), ApiError> {
    if !config.workspace_allowed_roots.is_empty() {
        return Ok(());
    }
    assert_local_picker_enabled(config)?;
    if !caller_loopback {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "REMOTE_DIRECTORY_BROWSE_FORBIDDEN",
            "Remote directory browsing requires ZK_WORKSPACE_ALLOWED_ROOTS",
        ));
    }
    Ok(())
}

/// 原生选择器守卫（旧 `assertNativePickerAllowed`：三条件合一判 403，
/// 可执行文件缺失判 501）。
///
/// # Errors
/// 403 `NATIVE_PICKER_FORBIDDEN`；501 `NATIVE_PICKER_UNAVAILABLE`。
pub(crate) fn assert_native_picker_allowed(
    config: &Config,
    caller_loopback: bool,
) -> Result<(), ApiError> {
    if !config.local_picker_enabled
        || !config.workspace_allowed_roots.is_empty()
        || !caller_loopback
    {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "NATIVE_PICKER_FORBIDDEN",
            "Native folder selection is only available for direct local desktop access",
        ));
    }
    if !picker_available() {
        return Err(failure(
            StatusCode::NOT_IMPLEMENTED,
            "NATIVE_PICKER_UNAVAILABLE",
            "The native folder chooser is unavailable",
        ));
    }
    Ok(())
}

/// 本请求可否打开原生选择器（旧 `nativePickerAvailable`，listing 字段源）。
pub(crate) fn native_picker_available(config: &Config, caller_loopback: bool) -> bool {
    config.local_picker_enabled
        && config.workspace_allowed_roots.is_empty()
        && caller_loopback
        && picker_available()
}

/// 旧 `ProjectWorkspaceService.localDesktopAccessAllowed`（L171-172）：本地
/// 桌面动作（原生 reveal）准入——`localPickerEnabled && allowedRoots 为空 &&
/// 回环对端`。与 [`native_picker_available`] 的差异：不要求 `picker_available()`
/// （reveal 走 `open -R` 等系统命令，不依赖 osascript 目录选择器的可用性）。
pub(crate) fn local_desktop_access_allowed(config: &Config, caller_loopback: bool) -> bool {
    config.local_picker_enabled && config.workspace_allowed_roots.is_empty() && caller_loopback
}

// ─── 目录浏览（旧 browseDirectories / currentBrowseRoots / resolve…） ───

/// 当前浏览根快照（旧 `currentBrowseRoots`）：allowed roots 为空取本地
/// 文件系统根（macOS 恒 `/`），非空则逐根复核绑定、失效根剔除。
fn current_browse_roots(config: &Config) -> Result<Vec<PathBuf>, ApiError> {
    if config.workspace_allowed_roots.is_empty() {
        if let Ok(canonical) = std::fs::canonicalize(Path::new("/"))
            && canonical.is_dir()
            && is_readable_dir(&canonical)
        {
            return Ok(vec![canonical]);
        }
        return Err(failure(
            StatusCode::CONFLICT,
            "WORKSPACE_UNAVAILABLE",
            "No local filesystem roots are available",
        ));
    }
    let roots: Vec<PathBuf> = config
        .workspace_allowed_roots
        .iter()
        .filter_map(|root| require_current_binding(config, &root.to_string_lossy()).ok())
        .collect();
    if roots.is_empty() {
        return Err(failure(
            StatusCode::CONFLICT,
            "WORKSPACE_UNAVAILABLE",
            "Configured directory browser roots are unavailable",
        ));
    }
    Ok(roots)
}

/// 浏览目标目录解析（旧 `resolveBrowseDirectory`）：空取默认根/首根；
/// 显式路径须绝对、无相对段、canonical 与词法一致（symlink 拒绝）。
fn resolve_browse_directory(
    config: &Config,
    requested: Option<&str>,
    roots: &[PathBuf],
) -> Result<PathBuf, ApiError> {
    let trimmed = requested.unwrap_or_default().trim();
    if trimmed.is_empty() {
        return if config.workspace_allowed_roots.is_empty() {
            canonicalize_for_create(config, Some(&config.workspace_default_root))
        } else {
            Ok(roots[0].clone())
        };
    }
    let raw = Path::new(trimmed);
    if !raw.is_absolute() {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_ABSOLUTE_REQUIRED",
            "path must be absolute",
        ));
    }
    let lexical = lexical_normalize(raw);
    if lexical != raw {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "DIRECTORY_PATH_NOT_CANONICAL",
            "path must not contain relative segments",
        ));
    }
    let canonical = std::fs::canonicalize(&lexical).map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_NOT_FOUND",
            "Directory does not exist",
        ),
        std::io::ErrorKind::PermissionDenied => failure(
            StatusCode::FORBIDDEN,
            "WORKSPACE_ACCESS_DENIED",
            "Directory is not accessible",
        ),
        _ => failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_PATH_INVALID",
            "Directory cannot be resolved",
        ),
    })?;
    if canonical != lexical {
        return Err(failure(
            StatusCode::CONFLICT,
            "WORKSPACE_REBOUND",
            "Directory path resolves through an alias or symbolic link",
        ));
    }
    let is_dir_nofollow = std::fs::symlink_metadata(&canonical).is_ok_and(|meta| meta.is_dir());
    if !is_dir_nofollow || !is_readable_dir(&canonical) {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "WORKSPACE_ACCESS_DENIED",
            "Directory is not readable",
        ));
    }
    if !roots.iter().any(|root| canonical.starts_with(root)) {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "DIRECTORY_BROWSE_OUTSIDE_ROOTS",
            "Directory is outside the configured browser roots",
        ));
    }
    Ok(canonical)
}

/// 子项可浏览判定（旧 `isBrowsableDirectory`）：非 symlink、NOFOLLOW 目录、
/// 可读、canonical 与词法一致、且仍在 owning root 之内。
fn is_browsable_directory(child: &Path, owning_root: &Path) -> bool {
    let Ok(meta) = std::fs::symlink_metadata(child) else {
        return false;
    };
    if meta.file_type().is_symlink() || !meta.is_dir() || !is_readable_dir(child) {
        return false;
    }
    let Ok(canonical) = std::fs::canonicalize(child) else {
        return false;
    };
    canonical == lexical_normalize(child) && canonical.starts_with(owning_root)
}

/// 子项映射（旧 `directoryEntry`：无文件名的极端路径回退全路径）。
fn directory_entry(directory: &Path) -> DirectoryEntry {
    let path = directory.to_string_lossy().into_owned();
    DirectoryEntry {
        name: directory
            .file_name()
            .map_or_else(|| path.clone(), |name| name.to_string_lossy().into_owned()),
        path,
    }
}

/// 目录浏览主流程（旧 `browseDirectories`）：守卫 → 根快照 → 目标解析 →
/// owning root（`startsWith` 中最深者）→ 列举过滤排序 → listing。
///
/// 同步 fs 实现，HTTP 侧以 `spawn_blocking` 包裹（见 `api::project`）。
///
/// # Errors
/// 守卫 / 解析 / 列举各分支的稳定错误码见各私有函数文档。
pub(crate) fn browse_directories(
    config: &Config,
    requested: Option<&str>,
    caller_loopback: bool,
) -> Result<DirectoryListing, ApiError> {
    assert_browse_allowed(config, caller_loopback)?;
    let roots = current_browse_roots(config)?;
    let current = resolve_browse_directory(config, requested, &roots)?;
    let owning_root = roots
        .iter()
        .filter(|root| current.starts_with(root))
        .max_by_key(|root| root.components().count())
        .cloned()
        .ok_or_else(|| {
            failure(
                StatusCode::FORBIDDEN,
                "DIRECTORY_BROWSE_OUTSIDE_ROOTS",
                "Directory is outside the configured browser roots",
            )
        })?;
    let entries = std::fs::read_dir(&current).map_err(|err| {
        if err.kind() == std::io::ErrorKind::PermissionDenied {
            failure(
                StatusCode::FORBIDDEN,
                "WORKSPACE_ACCESS_DENIED",
                "Directory is not accessible",
            )
        } else {
            failure(
                StatusCode::CONFLICT,
                "WORKSPACE_UNAVAILABLE",
                "Directory is no longer available",
            )
        }
    })?;
    let mut directories: Vec<DirectoryEntry> = entries
        .filter_map(Result::ok) // 快照期间消失的子项直接略过
        .map(|entry| entry.path())
        .filter(|child| is_browsable_directory(child, &owning_root))
        .map(|child| directory_entry(&child))
        .collect();
    directories.sort_by(|a, b| {
        a.name
            .to_lowercase()
            .cmp(&b.name.to_lowercase())
            .then_with(|| a.path.cmp(&b.path))
    });
    let parent = if current == owning_root {
        None
    } else {
        current
            .parent()
            .map(|parent| parent.to_string_lossy().into_owned())
    };
    Ok(DirectoryListing {
        roots: roots
            .iter()
            .map(|root| root.to_string_lossy().into_owned())
            .collect(),
        current: current.to_string_lossy().into_owned(),
        parent,
        directories,
        native_picker_available: native_picker_available(config, caller_loopback),
    })
}

// ─── 原生目录选择器（旧 SystemNativeDirectoryPicker，macOS osascript） ───

/// macOS 系统选择器可执行文件（旧 `executableFor(MACOS)`）。
const OSASCRIPT: &str = "/usr/bin/osascript";
/// 取消哨兵（AppleScript error -128 分支的固定回显）。
const CANCELLED_SENTINEL: &str = "__ZHIKUN_CANCELLED__";
/// 子进程 stdout 采集上限（超限视为选择器异常）。
const MAX_OUTPUT_BYTES: usize = 16 * 1024;
/// 选择器等待上限（旧 `DEFAULT_TIMEOUT` 5 分钟）。
const PICKER_TIMEOUT: Duration = Duration::from_mins(5);

/// AppleScript（旧 `macScript()` 逐字照抄）：固定脚本，不含任何请求数据。
const MAC_SCRIPT: &str = "on run argv\n\
    set startFolder to POSIX file (item 1 of argv) as alias\n\
    try\n\
    set selectedFolder to choose folder with prompt \
    \"Select a zkcode workspace\" default location startFolder\n\
    return POSIX path of selectedFolder\n\
    on error number -128\n\
    return \"__ZHIKUN_CANCELLED__\"\n\
    end try\n\
    end run";

/// 选择器结果（`pick` 的成功域：选中路径或用户取消）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PickerOutcome {
    /// 用户选中的目录（POSIX 路径，尚未 canonicalize）。
    Selected(String),
    /// 用户取消 / 空输出（HTTP 侧回 204）。
    Cancelled,
}

/// 选择器故障（旧三个异常类的等价枚举）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PickerFailure {
    /// 已有选择器打开（进程内互斥）。
    Busy,
    /// 等待超时（子进程已终止）。
    Timeout,
    /// 不可用（可执行缺失 / 启动失败 / 非零退出 / 超长输出）。
    Unavailable,
}

impl From<PickerFailure> for ApiError {
    fn from(err: PickerFailure) -> Self {
        match err {
            PickerFailure::Busy => failure(
                StatusCode::CONFLICT,
                "NATIVE_PICKER_BUSY",
                "Another folder chooser is already open",
            ),
            PickerFailure::Timeout => failure(
                StatusCode::GATEWAY_TIMEOUT,
                "NATIVE_PICKER_TIMEOUT",
                "The folder chooser timed out",
            ),
            PickerFailure::Unavailable => failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "NATIVE_PICKER_UNAVAILABLE",
                "The native folder chooser is unavailable",
            ),
        }
    }
}

/// 进程内选择器互斥（旧 `AtomicBoolean active`；模块级 static 避免扩散
/// `AppState`——GUI 对话框天然单例，跨 `AppState` 实例互斥更严格无害）。
static PICKER_ACTIVE: AtomicBool = AtomicBool::new(false);

/// 互斥租约：`acquire` 抢占，Drop 归还（含 panic 路径，Java finally 等价）。
struct PickerLease;

impl PickerLease {
    fn acquire() -> Option<Self> {
        PICKER_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            .then_some(Self)
    }
}

impl Drop for PickerLease {
    fn drop(&mut self) {
        PICKER_ACTIVE.store(false, Ordering::Release);
    }
}

/// 可执行文件可用性（旧 `isAvailable`：常规文件 + 任一执行位）。
fn executable_available(executable: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(executable)
        .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

/// 系统选择器是否可用（守卫与 listing 字段共用）。
pub(crate) fn picker_available() -> bool {
    executable_available(Path::new(OSASCRIPT))
}

/// 选择器初始目录（旧 `defaultStartDirectory`：`~/Desktop` 存在否则 `~`）。
fn default_start_directory() -> PathBuf {
    let home = std::env::var("HOME").map_or_else(|_| PathBuf::from("."), PathBuf::from);
    let desktop = home.join("Desktop");
    if desktop.is_dir() { desktop } else { home }
}

/// 打开系统目录选择器并等待用户操作（旧 `pick()`）。
///
/// # Errors
/// [`PickerFailure`] 三分支（Busy / Timeout / Unavailable）。
pub(crate) async fn run_native_picker() -> Result<PickerOutcome, PickerFailure> {
    let _lease = PickerLease::acquire().ok_or(PickerFailure::Busy)?;
    let args = vec![
        "-e".to_owned(),
        MAC_SCRIPT.to_owned(),
        "--".to_owned(),
        default_start_directory().to_string_lossy().into_owned(),
    ];
    run_picker_command(Path::new(OSASCRIPT), &args, PICKER_TIMEOUT).await
}

/// 选择器子进程执行核心（参数化 executable/timeout 以便单测注入假脚本；
/// 生产调用点固定 osascript + 5 分钟）。
///
/// 超时经 `kill_on_drop` 终止子进程（旧 `terminate` 的 destroy 等价）。
async fn run_picker_command(
    executable: &Path,
    args: &[String],
    timeout: Duration,
) -> Result<PickerOutcome, PickerFailure> {
    if !executable_available(executable) {
        return Err(PickerFailure::Unavailable);
    }
    let child = tokio::process::Command::new(executable)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| PickerFailure::Unavailable)?;
    let output = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_err(|_| PickerFailure::Timeout)?
        .map_err(|_| PickerFailure::Unavailable)?;
    if output.stdout.len() > MAX_OUTPUT_BYTES || !output.status.success() {
        return Err(PickerFailure::Unavailable);
    }
    let selected = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if selected.is_empty() || selected == CANCELLED_SENTINEL {
        return Ok(PickerOutcome::Cancelled);
    }
    Ok(PickerOutcome::Selected(selected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// 独占临时目录（macOS `/tmp` 是 symlink，必须 canonicalize 再用）。
    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-ws-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::canonicalize(&dir).expect("canonicalize temp dir")
    }

    fn picker_config(enabled: bool) -> Config {
        let mut config = Config::test_config();
        config.local_picker_enabled = enabled;
        config
    }

    #[test]
    fn require_name_trims_and_bounds() {
        assert_eq!(require_name(Some("  Demo  ")).expect("valid"), "Demo");
        assert_eq!(
            require_name(None).expect_err("missing").code,
            "PROJECT_NAME_INVALID"
        );
        assert_eq!(
            require_name(Some("   ")).expect_err("blank").code,
            "PROJECT_NAME_INVALID"
        );
        let long = "x".repeat(81);
        assert_eq!(
            require_name(Some(&long)).expect_err("too long").code,
            "PROJECT_NAME_INVALID"
        );
        assert!(require_name(Some(&"x".repeat(80))).is_ok());
    }

    #[test]
    fn canonicalize_for_create_error_codes() {
        let config = picker_config(true);
        let cases = [
            (None, "WORKSPACE_REQUIRED"),
            (Some("  "), "WORKSPACE_REQUIRED"),
            (Some("relative/path"), "WORKSPACE_ABSOLUTE_REQUIRED"),
            (Some("/no/such/zk-dir-xyz"), "WORKSPACE_NOT_FOUND"),
            (Some("/"), "WORKSPACE_ROOT_FORBIDDEN"),
        ];
        for (input, code) in cases {
            assert_eq!(
                canonicalize_for_create(&config, input)
                    .expect_err("must fail")
                    .code,
                code,
                "input: {input:?}"
            );
        }
        // 常规文件 → WORKSPACE_NOT_DIRECTORY。
        let dir = temp_dir("file");
        let file = dir.join("plain.txt");
        std::fs::write(&file, "x").expect("write");
        assert_eq!(
            canonicalize_for_create(&config, Some(&file.to_string_lossy()))
                .expect_err("file is not dir")
                .code,
            "WORKSPACE_NOT_DIRECTORY"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn canonicalize_enforces_allowed_roots() {
        let inside = temp_dir("inside");
        let outside = temp_dir("outside");
        let mut config = Config::test_config();
        config.workspace_allowed_roots = vec![inside.clone()];
        assert_eq!(
            canonicalize_for_create(&config, Some(&inside.to_string_lossy())).expect("in root"),
            inside
        );
        assert_eq!(
            canonicalize_for_create(&config, Some(&outside.to_string_lossy()))
                .expect_err("outside root")
                .code,
            "WORKSPACE_ACCESS_DENIED"
        );
        let _ = std::fs::remove_dir_all(&inside);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn require_current_binding_missing_and_rebound() {
        let config = picker_config(true);
        assert_eq!(
            require_current_binding(&config, "/no/such/zk-dir-xyz")
                .expect_err("missing")
                .code,
            "WORKSPACE_UNAVAILABLE"
        );
        // symlink 保存路径：canonical ≠ saved → WORKSPACE_REBOUND。
        let dir = temp_dir("bind");
        let target = dir.join("real");
        std::fs::create_dir_all(&target).expect("mkdir");
        let link = dir.join("alias");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        assert_eq!(
            require_current_binding(&config, &link.to_string_lossy())
                .expect_err("rebound")
                .code,
            "WORKSPACE_REBOUND"
        );
        assert_eq!(
            require_current_binding(&config, &target.to_string_lossy()).expect("stable"),
            target
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn guards_follow_legacy_precedence() {
        // 选择器关闭：LOCAL_PICKER_DISABLED 先于 remote 判定。
        let disabled = picker_config(false);
        assert_eq!(
            assert_create_allowed(&disabled, false)
                .expect_err("disabled")
                .code,
            "LOCAL_PICKER_DISABLED"
        );
        // 开启 + 非直连：REMOTE_*。
        let enabled = picker_config(true);
        assert_eq!(
            assert_create_allowed(&enabled, false)
                .expect_err("remote")
                .code,
            "REMOTE_PROJECT_CREATE_FORBIDDEN"
        );
        assert_eq!(
            assert_browse_allowed(&enabled, false)
                .expect_err("remote")
                .code,
            "REMOTE_DIRECTORY_BROWSE_FORBIDDEN"
        );
        assert!(assert_create_allowed(&enabled, true).is_ok());
        // allowed roots 非空：无条件放行（远端亦可）。
        let root = temp_dir("guard");
        let mut rooted = picker_config(false);
        rooted.workspace_allowed_roots = vec![root.clone()];
        assert!(assert_create_allowed(&rooted, false).is_ok());
        assert!(assert_browse_allowed(&rooted, false).is_ok());
        // native picker：三条件缺一 → FORBIDDEN（先于可用性判定）。
        assert_eq!(
            assert_native_picker_allowed(&rooted, true)
                .expect_err("roots configured")
                .code,
            "NATIVE_PICKER_FORBIDDEN"
        );
        assert_eq!(
            assert_native_picker_allowed(&enabled, false)
                .expect_err("remote")
                .code,
            "NATIVE_PICKER_FORBIDDEN"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn browse_lists_sorted_children_and_parent() {
        let root = temp_dir("browse");
        for name in ["beta", "Alpha", "gamma"] {
            std::fs::create_dir_all(root.join(name)).expect("mkdir child");
        }
        std::fs::write(root.join("plain.txt"), "x").expect("file");
        std::os::unix::fs::symlink(root.join("beta"), root.join("link")).expect("symlink");
        let mut config = Config::test_config();
        config.workspace_allowed_roots = vec![root.clone()];
        let listing =
            browse_directories(&config, Some(&root.to_string_lossy()), true).expect("list");
        let names: Vec<&str> = listing
            .directories
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        // 大小写不敏感排序；文件与 symlink 均被过滤。
        assert_eq!(names, ["Alpha", "beta", "gamma"]);
        assert_eq!(listing.current, root.to_string_lossy());
        // current == owning root → parent 剥离（None）。
        assert_eq!(listing.parent, None);
        // 子目录浏览：parent 出现。
        let child = root.join("beta");
        let listing =
            browse_directories(&config, Some(&child.to_string_lossy()), true).expect("child");
        assert_eq!(listing.parent.as_deref(), Some(&*root.to_string_lossy()));
        // 相对段拒绝。
        let dotted = format!("{}/beta/..", root.to_string_lossy());
        assert_eq!(
            browse_directories(&config, Some(&dotted), true)
                .expect_err("dotted")
                .code,
            "DIRECTORY_PATH_NOT_CANONICAL"
        );
        // 越根拒绝。
        let outside = temp_dir("browse-outside");
        assert_eq!(
            browse_directories(&config, Some(&outside.to_string_lossy()), true)
                .expect_err("outside")
                .code,
            "DIRECTORY_BROWSE_OUTSIDE_ROOTS"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    /// 写一个 shell fixture；通过系统 `/bin/sh` 执行，避免 macOS 对临时目录内
    /// 新生成可执行文件的 provenance 扫描干扰进程时序。
    fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, body).expect("write script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        path
    }

    async fn run_shell_fixture(
        script: &Path,
        timeout: Duration,
    ) -> Result<PickerOutcome, PickerFailure> {
        run_picker_command(
            Path::new("/bin/sh"),
            &[script.to_string_lossy().into_owned()],
            timeout,
        )
        .await
    }

    #[tokio::test]
    async fn picker_command_outcomes() {
        let dir = temp_dir("picker");
        let secs = Duration::from_secs(5);
        // 选中：stdout 输出路径（含换行，trim 后返回）。
        let ok = write_script(&dir, "ok.sh", "#!/bin/sh\necho /Users/demo/ws\n");
        assert_eq!(
            run_shell_fixture(&ok, secs).await.expect("selected"),
            PickerOutcome::Selected("/Users/demo/ws".to_owned())
        );
        // 取消哨兵与空输出 → Cancelled。
        let cancel = write_script(&dir, "cancel.sh", "#!/bin/sh\necho __ZHIKUN_CANCELLED__\n");
        assert_eq!(
            run_shell_fixture(&cancel, secs).await.expect("cancelled"),
            PickerOutcome::Cancelled
        );
        let empty = write_script(&dir, "empty.sh", "#!/bin/sh\nexit 0\n");
        assert_eq!(
            run_shell_fixture(&empty, secs).await.expect("empty"),
            PickerOutcome::Cancelled
        );
        // 非零退出 → Unavailable。
        let fail = write_script(&dir, "fail.sh", "#!/bin/sh\nexit 1\n");
        assert_eq!(
            run_shell_fixture(&fail, secs).await.expect_err("exit 1"),
            PickerFailure::Unavailable
        );
        // 超时（子进程被 kill_on_drop 终止）→ Timeout。
        let slow = write_script(&dir, "slow.sh", "#!/bin/sh\nsleep 5\n");
        assert_eq!(
            run_shell_fixture(&slow, Duration::from_millis(100))
                .await
                .expect_err("timeout"),
            PickerFailure::Timeout
        );
        // 不可执行 / 不存在 → Unavailable。
        let plain = dir.join("plain.txt");
        std::fs::write(&plain, "x").expect("write");
        assert_eq!(
            run_picker_command(&plain, &[], secs)
                .await
                .expect_err("not executable"),
            PickerFailure::Unavailable
        );
        assert_eq!(
            run_picker_command(&dir.join("missing"), &[], secs)
                .await
                .expect_err("missing"),
            PickerFailure::Unavailable
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn real_macos_osascript_process_round_trips_output() {
        assert!(picker_available(), "macOS osascript must be available");
        let args = vec![
            "-e".to_owned(),
            "return \"/private/tmp/zkcode-picker-real\"".to_owned(),
        ];
        assert_eq!(
            run_picker_command(Path::new(OSASCRIPT), &args, Duration::from_mins(1))
                .await
                .expect("real osascript output"),
            PickerOutcome::Selected("/private/tmp/zkcode-picker-real".to_owned())
        );
    }

    #[test]
    fn picker_lease_is_exclusive_and_released() {
        let first = PickerLease::acquire().expect("free lease");
        assert!(PickerLease::acquire().is_none(), "second acquire busy");
        drop(first);
        let again = PickerLease::acquire().expect("released after drop");
        drop(again);
    }
}
