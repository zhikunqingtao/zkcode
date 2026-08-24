//! 统一路径安全验证服务 —— 8 层检查的唯一权威实现。
//!
//! 逐字对照 `backend/src/main/java/com/aicodeassistant/security/PathSecurityService.java`
//! （917 行）与 `security/SystemScratchpadPathPolicy.java`（L22-168）。硬编码敏感路径
//! 黑名单，不可通过配置修改（安全设计）。
//!
//! 层次索引（旧源行号）：
//!
//! | 层 | 语义 | 旧源 |
//! |---|---|---|
//! | Layer 1 | 硬编码设备路径阻止 | L42-49、L194-217 |
//! | Layer 2 | 危险文件 + 目录黑名单 | L51-92、L241-263、L406-426 |
//! | Layer 2.5 | 系统关键目录写入需确认 | L99-107、L428-433、L584-598 |
//! | Layer 3 | 符号链接写入检查 | L435-451 |
//! | Layer 4 | 危险删除检测 | L109-121、L460-485 |
//! | Layer 5 | Windows 路径绕过检测 | L487-507 |
//! | Layer 8 | 敏感系统文件读取检测 | L795-898 |
//!
//! Layer 6/7 在旧源中不存在编号（`PathValidator` 承担的 Bash 侧四层解析已由
//! `zk-tools::bash::path_validator` 移植），本模块保持与旧源同样的编号跳跃。
//!
//! 与 `zk-tools::bash::path_validator` 的关系：二者是**不同的旧类**。
//! `PathValidator`（335 行，Bash 命令内路径提取）已由 2.4 移植；本模块对应
//! `PathSecurityService`，是 `FileAnalyzer` 调用的授权门。不得互相替代。

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::LazyLock;

use crate::workspace::absolute_normalized;

// ===== Layer 1: 硬编码设备路径阻止 =====
// 对照 `PathSecurityService.java:43-49`。
const BLOCKED_DEVICE_PATHS: &[&str] = &[
    "/dev/zero",
    "/dev/random",
    "/dev/urandom",
    "/dev/full",
    "/dev/stdin",
    "/dev/tty",
    "/dev/console",
    "/dev/stdout",
    "/dev/stderr",
    "/dev/fd/0",
    "/dev/fd/1",
    "/dev/fd/2",
    "/proc/self/fd/0",
    "/proc/self/fd/1",
    "/proc/self/fd/2",
];

// ===== Layer 2: 危险文件黑名单 =====
// 对照 `PathSecurityService.java:52-65`。
const DANGEROUS_FILES: &[&str] = &[
    ".gitconfig",
    ".gitmodules",
    ".bashrc",
    ".bash_profile",
    ".bash_login",
    ".bash_logout",
    ".zshrc",
    ".zprofile",
    ".zshenv",
    ".zlogin",
    ".profile",
    ".login",
    ".ripgreprc",
    ".env",
    ".env.local",
    ".env.production",
    ".mcp.json",
    zk_core::paths::CONFIG_FILE_NAME,
    // 遗留保护面（#65）：迁移是**拷贝**而非移动，旧配置文件仍留在盘上，
    // 把它从名单里删掉等于让旧文件失去授权门禁——保护面只许单调扩张。
    zk_core::paths::LEGACY_CONFIG_FILE_NAME,
    ".npmrc",
    ".yarnrc",
    "id_rsa",
    "id_ed25519",
    "id_ecdsa",
    "known_hosts",
    "authorized_keys",
    ".pgpass",
    ".my.cnf",
    ".netrc",
    ".curlrc",
    "credentials",
    "token.json",
];

// ===== Layer 2: 危险目录黑名单 =====
// 对照 `PathSecurityService.java:68-74`。
const DANGEROUS_DIRECTORIES: &[&str] = &[
    ".git",
    ".vscode",
    ".idea",
    zk_core::paths::CONFIG_DIR_NAME,
    // 遗留保护面（#65），理由同 `DANGEROUS_FILES`。
    zk_core::paths::LEGACY_CONFIG_DIR_NAME,
    ".ai-code-assistant",
    ".ssh",
    ".gnupg",
    ".aws",
    ".config",
    ".local",
    ".kube",
    ".docker",
    "node_modules",
];

/// 直接读取这些目录会暴露凭据或仓库控制状态，因此永远需要一次新的批准。
///
/// 对照 `PathSecurityService.java:82-85`。刻意比 `DANGEROUS_DIRECTORIES` 更窄：
/// 依赖与 IDE 目录只从广域搜索中排除，本身不是秘密。
const SENSITIVE_READ_DIRECTORIES: &[&str] = &[
    ".git",
    ".ssh",
    ".gnupg",
    ".aws",
    ".kube",
    ".docker",
    ".ai-code-assistant",
];

/// 对控制/凭据目录的直接写入永远需要批准。对照 `PathSecurityService.java:88-92`。
const SENSITIVE_WRITE_DIRECTORIES: &[&str] = &[
    ".git",
    ".vscode",
    ".idea",
    zk_core::paths::CONFIG_DIR_NAME,
    // 遗留保护面（#65）：旧目录不再享受 scratchpad 放宽，只保留保护。
    zk_core::paths::LEGACY_CONFIG_DIR_NAME,
    ".ai-code-assistant",
    ".ssh",
    ".gnupg",
    ".aws",
    ".config",
    ".kube",
    ".docker",
];

/// 递归遍历这些系统根永远不是有界文件读取。对照 `PathSecurityService.java:95-97`。
const BLOCKED_RECURSIVE_ROOTS: &[&str] = &[
    "/",
    "/etc",
    "/private/etc",
    "/root",
    "/proc",
    "/sys",
    "/dev",
];

// ===== Layer 2.5: 系统关键目录 — 写入需确认（不硬拒绝）=====
// 对照 `PathSecurityService.java:100-107`。
const SYSTEM_CRITICAL_DIRS: &[&str] = &[
    "/etc",
    "/private/etc",
    "/usr",
    "/bin",
    "/sbin",
    "/boot",
    "/var",
    "/private/var",
    "/lib",
    "/lib64",
    "/opt",
    "/root",
    "/sys",
    "/proc",
    "/System",
    "/Applications",
    "C:/Windows",
    "C:/Program Files",
    "C:/Program Files (x86)",
    "C:/ProgramData",
];

// ===== Layer 8: 敏感系统文件黑名单（即使 BYPASS 模式也不允许通过 Bash 读取）=====
// 对照 `PathSecurityService.java:798-801`。
const SENSITIVE_SYSTEM_FILES: &[&str] = &[
    "/etc/shadow",
    "/etc/passwd",
    "/etc/sudoers",
    "/etc/sudoers.d",
    "/etc/master.passwd",
    "/etc/security/passwd",
];

/// 敏感用户文件前缀（`~` 在运行时展开）。对照 `PathSecurityService.java:804-810`。
const SENSITIVE_USER_PATHS: &[&str] = &[
    "~/.ssh/id_rsa",
    "~/.ssh/id_ed25519",
    "~/.ssh/id_ecdsa",
    "~/.ssh/id_dsa",
    "~/.ssh/config",
    "~/.aws/credentials",
    "~/.aws/config",
    "~/.gnupg/",
    "~/.kube/config",
    "~/.config/ai-code-assistant/keychain",
];

/// 敏感系统目录前缀。对照 `PathSecurityService.java:813-815`。
const SENSITIVE_SYSTEM_DIRS: &[&str] = &["/proc/", "/sys/"];

/// 安全的 `/proc` 路径白名单。对照 `PathSecurityService.java:818-822`。
const SAFE_PROC_PATHS: &[&str] = &[
    "/proc/self/cwd",
    "/proc/self/exe",
    "/proc/version",
    "/proc/cpuinfo",
    "/proc/meminfo",
    "/proc/loadavg",
    "/proc/uptime",
    "/proc/filesystems",
];

/// 读取类命令。对照 `PathSecurityService.java:825-830`。
const READ_COMMANDS: &[&str] = &[
    "cat", "less", "more", "head", "tail", "grep", "egrep", "fgrep", "strings", "xxd", "od",
    "file", "stat", "wc", "awk", "sed", "tac", "nl", "sort", "uniq", "cut", "paste", "tr", "fold",
    "hexdump", "base64",
];

/// Windows DOS 设备名。对照 `PathSecurityService.java:501-503`。
const DOS_DEVICE_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

// ===== Layer 4: 危险删除目标路径 =====
// 对照 `PathSecurityService.java:110-121`（静态块在类加载时并入 user.home）。
static DANGEROUS_REMOVAL_TARGETS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut targets: Vec<String> = [
        "/",
        "/*",
        "/etc",
        "/usr",
        "/var",
        "/bin",
        "/sbin",
        "/boot",
        "/lib",
        "/lib64",
        "/opt",
        "/root",
        "/System",
        "/Applications",
        "C:\\",
        "C:\\Windows",
        "C:\\Program Files",
    ]
    .iter()
    .map(|value| (*value).to_string())
    .collect();
    let home = user_home();
    targets.push(home.clone());
    targets.push(format!("{home}/*"));
    targets
});

/// `System.getProperty("user.home", "/root")` 的等价读取。
///
/// 对照 `PathSecurityService.java:721`、`L781-782`、`L843`。
fn user_home() -> String {
    std::env::var("HOME")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "/root".into())
}

/// `System.getProperty("os.name").toLowerCase().contains("win")`。
///
/// 对照 `PathSecurityService.java:785-787`。
const fn is_windows() -> bool {
    cfg!(windows)
}

/// 路径检查结果（三态）。对照 `PathSecurityService.java:905-915`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathCheckResult {
    /// 是否允许继续（`needs_confirmation` 为真时仍为 `true`）。
    pub is_allowed: bool,
    /// 是否需要用户确认（升级为交互）。
    pub needs_confirmation: bool,
    /// 拒绝或确认的展示文案（`allowed()` 为 `None`）。
    pub message: Option<String>,
}

impl PathCheckResult {
    /// 无条件放行。对照 `PathSecurityService.java:906-908`。
    #[must_use]
    pub const fn allowed() -> Self {
        Self {
            is_allowed: true,
            needs_confirmation: false,
            message: None,
        }
    }

    /// 硬拒绝。对照 `PathSecurityService.java:909-911`。
    #[must_use]
    pub fn denied(message: impl Into<String>) -> Self {
        Self {
            is_allowed: false,
            needs_confirmation: false,
            message: Some(message.into()),
        }
    }

    /// 需要用户确认。对照 `PathSecurityService.java:912-914`。
    #[must_use]
    pub fn needs_confirmation(message: impl Into<String>) -> Self {
        Self {
            is_allowed: true,
            needs_confirmation: true,
            message: Some(message.into()),
        }
    }
}

/// 执行期检查 + 被检查的精确目标。对照 `PathSecurityService.java:901-902`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedPathCheck {
    /// 被检查的规范目标（UNC 拒绝时为 `None`）。
    pub target: Option<PathBuf>,
    /// 检查结论。
    pub permission: PathCheckResult,
}

/// 协调者与 swarm run 使用的服务端自有 scratchpad 根解析策略。
///
/// 逐字对照 `security/SystemScratchpadPathPolicy.java`（L22-168）。配置路径在构造时
/// 通过最近的存在祖先解析一次；随后每次访问都重复该解析，若符号链接改绑导致有效根
/// 变化则失败关闭。
#[derive(Debug, Clone)]
pub struct SystemScratchpadPathPolicy {
    configured_root: PathBuf,
    canonical_root: PathBuf,
}

impl SystemScratchpadPathPolicy {
    /// 以显式服务端自有根构造。对照 `SystemScratchpadPathPolicy.java:35-46`。
    ///
    /// # Panics
    ///
    /// 配置根存在但不是目录时 panic，对照 L41-45 的 `IllegalArgumentException`。
    #[must_use]
    pub fn new(configured_root: &Path) -> Self {
        let configured = absolute_normalized(configured_root);
        let canonical = resolve_through_existing_ancestor_or(&configured, &configured);
        assert!(
            fs::symlink_metadata(&configured).is_err()
                || fs::metadata(&configured).is_ok_and(|meta| meta.is_dir()),
            "System scratchpad root is not a directory"
        );
        Self {
            configured_root: configured,
            canonical_root: canonical,
        }
    }

    /// 非 Spring 直接单元构造的默认策略：`{working-dir}/.zk/scratchpad`。
    ///
    /// 对照 `SystemScratchpadPathPolicy.java:51-61`（#65 起目录名经
    /// `zk_core::paths::scratchpad_dir` 解析，与 zk-server 缺省、系统提示同源）。
    #[must_use]
    pub fn default_policy() -> Self {
        let working = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Self::new(&zk_core::paths::scratchpad_dir(&working))
    }

    /// 固定的规范根（前提是其配置身份稳定）。
    ///
    /// 对照 `SystemScratchpadPathPolicy.java:64-67`。身份漂移时返回 `None`
    /// （旧实现抛 `SecurityException`；本 crate 以 `Option` 表达失败关闭）。
    #[must_use]
    pub fn system_root(&self) -> Option<&Path> {
        if self.is_root_stable() {
            Some(&self.canonical_root)
        } else {
            None
        }
    }

    /// 当前目标是否为 scratchpad 根或其后代。
    ///
    /// 对照 `SystemScratchpadPathPolicy.java:74-85`。目标中已存在的符号链接在
    /// 包含判定前先被解析；根不稳定时一律返回 `false`。
    #[must_use]
    pub fn contains(&self, target: &Path) -> bool {
        if !self.is_root_stable() {
            return false;
        }
        let absolute = absolute_normalized(target);
        let canonical_target = resolve_through_existing_ancestor_or(&absolute, &absolute);
        canonical_target.starts_with(&self.canonical_root) && self.is_root_stable()
    }

    /// 对照 `SystemScratchpadPathPolicy.java:126-132`。
    fn is_root_stable(&self) -> bool {
        resolve_through_existing_ancestor_or(&self.configured_root, &self.configured_root)
            == self.canonical_root
    }
}

impl Default for SystemScratchpadPathPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

/// 逐级上溯到存在的祖先后 `toRealPath`，再把缺失段逐段接回。
///
/// 对照 `PathSecurityService.java:563-582`（失败时返回 `fallback`，即旧源的
/// `return candidate`）。
fn resolve_through_existing_ancestor_or(candidate: &Path, fallback: &Path) -> PathBuf {
    let mut existing = Some(candidate.to_path_buf());
    let mut missing: Vec<std::ffi::OsString> = Vec::new();
    while let Some(current) = existing.clone() {
        if fs::symlink_metadata(&current).is_ok() {
            break;
        }
        if let Some(name) = current.file_name() {
            missing.push(name.to_os_string());
        }
        existing = current.parent().map(Path::to_path_buf);
    }
    let Some(existing) = existing else {
        return fallback.to_path_buf();
    };
    let Ok(mut resolved) = existing.canonicalize() else {
        return fallback.to_path_buf();
    };
    for name in missing.iter().rev() {
        resolved = resolved.join(name);
    }
    absolute_normalized(&resolved)
}

/// 对照 `PathSecurityService.java:635-642`：`\` → `/` 并去掉尾部 `/`（保留根 `/`）。
fn normalize_policy_path(path: &str) -> String {
    let mut normalized = path.replace('\\', "/");
    while normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }
    normalized
}

/// 对照 `PathSecurityService.java:628-633`。
fn is_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && bytes[2] == b'/'
}

/// 对照 `PathSecurityService.java:622-626`：Windows 盘符路径大小写不敏感。
fn paths_equal(first: &str, second: &str) -> bool {
    if is_windows_drive_path(first) || is_windows_drive_path(second) {
        first.eq_ignore_ascii_case(second)
    } else {
        first == second
    }
}

/// 对照 `PathSecurityService.java:611-620`。
fn is_same_or_descendant(path: &str, directory: &str) -> bool {
    if paths_equal(path, directory) {
        return true;
    }
    let prefix = if directory.ends_with('/') {
        directory.to_string()
    } else {
        format!("{directory}/")
    };
    if is_windows_drive_path(path) || is_windows_drive_path(directory) {
        path.to_lowercase().starts_with(&prefix.to_lowercase())
    } else {
        path.starts_with(&prefix)
    }
}

/// Layer 2.5 判定：写入系统关键目录需确认。
///
/// 对照 `PathSecurityService.java:584-598`。`macOS` 把 `/var` 映射到 `/private/var`，
/// 但其按用户的临时与缓存树是普通用户存储而非系统状态，故 `/private/var/folders/`
/// 前缀显式返回 `false`。
#[must_use]
pub fn is_system_critical_path(path: &str) -> bool {
    let normalized = normalize_policy_path(path);
    if normalized.starts_with("/private/var/folders/") {
        return false;
    }
    SYSTEM_CRITICAL_DIRS
        .iter()
        .any(|directory| is_same_or_descendant(&normalized, &normalize_policy_path(directory)))
}

/// 对照 `PathSecurityService.java:600-609`。
#[must_use]
pub fn is_blocked_recursive_root(path: &str) -> bool {
    let normalized = normalize_policy_path(path);
    BLOCKED_RECURSIVE_ROOTS
        .iter()
        .any(|root| paths_equal(&normalized, &normalize_policy_path(root)))
}

/// 对照 `PathSecurityService.java:789-793`。
fn is_unc_path(path: &str) -> bool {
    path.starts_with("//") || path.starts_with("\\\\")
}

/// 对照 `PathSecurityService.java:771-778`。
fn normalize_path(path: &str) -> String {
    let mut out = PathBuf::new();
    for component in Path::new(path).components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out.to_string_lossy().replace('\\', "/")
}

/// 对照 `PathSecurityService.java:780-783`。
fn resolve_path_variables(path: &str) -> String {
    let home = user_home();
    path.replace('~', &home).replace("$HOME", &home)
}

/// 词法绝对化 + 规范化（不触碰文件系统）。
///
/// 对照 `PathSecurityService.java:532-539`（`absoluteNormalizedPath`）。
fn absolute_normalized_path(file_path: &str, working_directory: &str) -> PathBuf {
    let path = Path::new(file_path);
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        Path::new(working_directory).join(path)
    };
    absolute_normalized(&joined)
}

/// 统一路径安全验证服务。
///
/// 对照 `PathSecurityService.java:26-40`：供文件工具与统一 Authorization Gateway
/// 共用，确保分析和执行前复检策略一致。
#[derive(Debug, Clone, Default)]
pub struct PathSecurityService {
    system_scratchpads: SystemScratchpadPathPolicy,
}

impl PathSecurityService {
    /// 以显式 scratchpad 策略构造。对照 `PathSecurityService.java:37-40`。
    #[must_use]
    pub fn new(system_scratchpads: SystemScratchpadPathPolicy) -> Self {
        Self { system_scratchpads }
    }

    // ==================== 读取权限检查 ====================

    /// 验证读取路径安全性（工具直调，不允许越出项目边界）。
    ///
    /// 对照 `PathSecurityService.java:128-130`。
    #[must_use]
    pub fn check_read_permission(
        &self,
        file_path: &str,
        working_directory: &str,
    ) -> PathCheckResult {
        self.read_permission(file_path, working_directory, false)
    }

    /// 授权门变体：普通区外路径可继续走交互/授权记录策略。
    ///
    /// 对照 `PathSecurityService.java:137-140`。设备路径与敏感路径仍保留其
    /// 硬拒绝 / 高风险处置。
    #[must_use]
    pub fn check_authorized_read_permission(
        &self,
        file_path: &str,
        working_directory: &str,
    ) -> PathCheckResult {
        self.read_permission(file_path, working_directory, true)
    }

    /// 执行期读取检查：任何重解析到不同目标的路径一律拒绝。
    ///
    /// 对照 `PathSecurityService.java:147-163`。
    #[must_use]
    pub fn inspect_authorized_execution_read_permission(
        &self,
        file_path: &str,
        working_directory: &str,
    ) -> AuthorizedPathCheck {
        let target = Self::inspect_authorized_target(file_path, working_directory);
        if !target.permission.is_allowed {
            return target;
        }
        let resolved = target.target.clone().unwrap_or_default();
        let permission =
            self.read_permission_resolved(&resolved, file_path, working_directory, true);
        AuthorizedPathCheck {
            target: target.target,
            permission,
        }
    }

    /// 对照 `PathSecurityService.java:165-177`。
    fn read_permission(
        &self,
        file_path: &str,
        working_directory: &str,
        allow_external: bool,
    ) -> PathCheckResult {
        if is_unc_path(file_path) {
            return PathCheckResult::denied(format!(
                "UNC path access denied (NTLM credential leak prevention): {file_path}"
            ));
        }
        let resolved = Self::resolve_path_unchecked(file_path, working_directory);
        self.read_permission_resolved(&resolved, file_path, working_directory, allow_external)
    }

    /// 读取检查主体（Layer 1 / Layer 2 / 项目边界 / Layer 8）。
    ///
    /// 逐段对照 `PathSecurityService.java:179-271`。
    #[allow(clippy::too_many_lines)]
    fn read_permission_resolved(
        &self,
        resolved: &Path,
        file_path: &str,
        working_directory: &str,
        allow_external: bool,
    ) -> PathCheckResult {
        let resolved_str = resolved.to_string_lossy().into_owned();
        let lexical_path = absolute_normalized_path(file_path, working_directory);

        // 1. 设备文件检查 — Layer 1（旧源 L193-196）
        if BLOCKED_DEVICE_PATHS.contains(&resolved_str.as_str()) {
            return PathCheckResult::denied(format!("Cannot read device file: {resolved_str}"));
        }

        // 2. /proc 特殊文件检查（旧源 L198-217）
        if resolved_str.starts_with("/proc/")
            && (resolved_str.ends_with("/fd/0")
                || resolved_str.ends_with("/fd/1")
                || resolved_str.ends_with("/fd/2")
                || resolved_str.ends_with("/environ"))
        {
            return PathCheckResult::denied(format!(
                "Cannot read process special file: {resolved_str}"
            ));
        }
        if (resolved_str == "/proc" || resolved_str.starts_with("/proc/"))
            && !SAFE_PROC_PATHS.contains(&resolved_str.as_str())
        {
            return PathCheckResult::denied(format!(
                "Cannot read process special path: {resolved_str}"
            ));
        }
        if resolved_str == "/sys"
            || resolved_str.starts_with("/sys/")
            || resolved_str == "/dev"
            || resolved_str.starts_with("/dev/")
        {
            return PathCheckResult::denied(format!(
                "Cannot read device or kernel path: {resolved_str}"
            ));
        }

        // 3. 项目边界检查（旧源 L219-239）
        let saved_project_root = absolute_normalized(Path::new(working_directory));
        let Ok(project_root) = saved_project_root.canonicalize() else {
            return PathCheckResult::denied("Access denied: project boundary is unavailable");
        };
        if project_root != saved_project_root {
            return PathCheckResult::denied("Access denied: project boundary has changed");
        }
        let outside_project = !resolved.starts_with(&project_root);
        if !allow_external && outside_project {
            return PathCheckResult::denied(format!(
                "Access denied: path '{file_path}' is outside project boundary. Allowed: {}",
                project_root.display()
            ));
        }

        // 4. 危险文件警告 — Layer 2（旧源 L241-247）
        if let Some(name) = protected_file_name(resolved, &lexical_path) {
            return PathCheckResult::needs_confirmation(format!("Reading sensitive file: {name}"));
        }

        // 旧源 L249-263：规范路径与词法路径都要过敏感目录检查。
        let mut sensitive_read_directory = self.matching_sensitive_directory(
            resolved,
            &project_root,
            outside_project,
            SENSITIVE_READ_DIRECTORIES,
        );
        if sensitive_read_directory.is_none() && lexical_path != resolved {
            sensitive_read_directory = self.matching_sensitive_directory(
                &lexical_path,
                &project_root,
                !lexical_path.starts_with(&project_root),
                SENSITIVE_READ_DIRECTORIES,
            );
        }
        if let Some(directory) = sensitive_read_directory {
            return PathCheckResult::needs_confirmation(format!(
                "Reading from sensitive directory: {directory}"
            ));
        }

        // 旧源 L265-268：Layer 8 的路径面（命令面见 `check_sensitive_file_read`）。
        if is_sensitive_system_or_user_path(resolved) {
            return PathCheckResult::needs_confirmation(format!(
                "Reading sensitive path: {resolved_str}"
            ));
        }

        PathCheckResult::allowed()
    }

    /// 递归读取（Glob / Grep）根检查。
    ///
    /// 对照 `PathSecurityService.java:279-283`。
    #[must_use]
    pub fn check_recursive_read_root_permission(
        &self,
        root_path: &str,
        working_directory: &str,
    ) -> PathCheckResult {
        self.recursive_read_root_permission(root_path, working_directory, false)
    }

    /// 授权门变体的递归根检查。对照 `PathSecurityService.java:286-290`。
    #[must_use]
    pub fn check_authorized_recursive_read_root_permission(
        &self,
        root_path: &str,
        working_directory: &str,
    ) -> PathCheckResult {
        self.recursive_read_root_permission(root_path, working_directory, true)
    }

    /// 执行期递归根检查。对照 `PathSecurityService.java:300-310`。
    #[must_use]
    pub fn inspect_authorized_execution_recursive_read_root_permission(
        &self,
        root_path: &str,
        working_directory: &str,
    ) -> AuthorizedPathCheck {
        let target = Self::inspect_authorized_target(root_path, working_directory);
        if !target.permission.is_allowed {
            return target;
        }
        let resolved = target.target.clone().unwrap_or_default();
        let permission =
            self.recursive_read_root_resolved(&resolved, root_path, working_directory, true);
        AuthorizedPathCheck {
            target: target.target,
            permission,
        }
    }

    /// 对照 `PathSecurityService.java:312-322`。
    fn recursive_read_root_permission(
        &self,
        root_path: &str,
        working_directory: &str,
        allow_external: bool,
    ) -> PathCheckResult {
        if is_unc_path(root_path) {
            return self.read_permission(root_path, working_directory, allow_external);
        }
        let resolved = Self::resolve_path_unchecked(root_path, working_directory);
        self.recursive_read_root_resolved(&resolved, root_path, working_directory, allow_external)
    }

    /// 对照 `PathSecurityService.java:324-346`。
    fn recursive_read_root_resolved(
        &self,
        resolved: &Path,
        root_path: &str,
        working_directory: &str,
        allow_external: bool,
    ) -> PathCheckResult {
        let read_check =
            self.read_permission_resolved(resolved, root_path, working_directory, allow_external);
        if !read_check.is_allowed || read_check.needs_confirmation {
            return read_check;
        }
        let resolved_path = resolved.to_string_lossy().into_owned();
        if resolved.parent().is_none()
            || is_blocked_recursive_root(&resolved_path)
            || resolved_path.starts_with("/proc/")
            || resolved_path.starts_with("/sys/")
            || resolved_path.starts_with("/dev/")
        {
            return PathCheckResult::denied(format!(
                "Recursive access to system root is denied: {resolved_path}"
            ));
        }
        read_check
    }

    // ==================== 写入权限检查 ====================

    /// 验证写入路径安全性 — 比读取更严格。对照 `PathSecurityService.java:353-355`。
    #[must_use]
    pub fn check_write_permission(
        &self,
        file_path: &str,
        working_directory: &str,
    ) -> PathCheckResult {
        self.write_permission(file_path, working_directory, false)
    }

    /// 授权门变体的写入检查。对照 `PathSecurityService.java:358-361`。
    #[must_use]
    pub fn check_authorized_write_permission(
        &self,
        file_path: &str,
        working_directory: &str,
    ) -> PathCheckResult {
        self.write_permission(file_path, working_directory, true)
    }

    /// 执行期写入检查。对照 `PathSecurityService.java:371-380`。
    #[must_use]
    pub fn inspect_authorized_execution_write_permission(
        &self,
        file_path: &str,
        working_directory: &str,
    ) -> AuthorizedPathCheck {
        let target = Self::inspect_authorized_target(file_path, working_directory);
        if !target.permission.is_allowed {
            return target;
        }
        let resolved = target.target.clone().unwrap_or_default();
        let permission =
            self.write_permission_resolved(&resolved, file_path, working_directory, true);
        AuthorizedPathCheck {
            target: target.target,
            permission,
        }
    }

    /// 对照 `PathSecurityService.java:382-393`。
    fn write_permission(
        &self,
        file_path: &str,
        working_directory: &str,
        allow_external: bool,
    ) -> PathCheckResult {
        if is_unc_path(file_path) {
            return self.read_permission(file_path, working_directory, allow_external);
        }
        let resolved = Self::resolve_path_unchecked(file_path, working_directory);
        self.write_permission_resolved(&resolved, file_path, working_directory, allow_external)
    }

    /// 写入检查主体（Layer 2 / Layer 2.5 / Layer 3 / Layer 5）。
    ///
    /// 逐段对照 `PathSecurityService.java:395-458`。
    fn write_permission_resolved(
        &self,
        resolved: &Path,
        file_path: &str,
        working_directory: &str,
        allow_external: bool,
    ) -> PathCheckResult {
        let read_check =
            self.read_permission_resolved(resolved, file_path, working_directory, allow_external);
        if !read_check.is_allowed && !read_check.needs_confirmation {
            return read_check;
        }

        // 5. 危险目录写入检查 — Layer 2（旧源 L406-426）
        // 注意：旧源此处用**词法** projectRoot（未 toRealPath），与读取路径的
        // canonicalize 版本不同；此处逐字保持该差异。
        let project_root = absolute_normalized(Path::new(working_directory));
        let outside_project = !resolved.starts_with(&project_root);
        let mut sensitive_write_directory = self.matching_sensitive_directory(
            resolved,
            &project_root,
            outside_project,
            SENSITIVE_WRITE_DIRECTORIES,
        );
        let lexical_path = absolute_normalized_path(file_path, working_directory);
        if sensitive_write_directory.is_none() && lexical_path != resolved {
            sensitive_write_directory = self.matching_sensitive_directory(
                &lexical_path,
                &project_root,
                !lexical_path.starts_with(&project_root),
                SENSITIVE_WRITE_DIRECTORIES,
            );
        }
        if let Some(directory) = sensitive_write_directory {
            return PathCheckResult::needs_confirmation(format!(
                "Writing to protected directory: {directory}"
            ));
        }

        // 5.5 系统关键目录写入检查 — Layer 2.5（旧源 L428-433）
        let resolved_str = resolved.to_string_lossy().into_owned();
        if is_system_critical_path(&resolved_str) {
            return PathCheckResult::needs_confirmation(format!(
                "Writing to system critical directory: {resolved_str}"
            ));
        }

        // 5.6 符号链接写入检查 — Layer 3（旧源 L435-451）
        if fs::symlink_metadata(resolved).is_ok_and(|meta| meta.file_type().is_symlink())
            && let Ok(real_path) = resolved.canonicalize()
        {
            let real_str = real_path.to_string_lossy().into_owned();
            if BLOCKED_DEVICE_PATHS.contains(&real_str.as_str()) {
                return PathCheckResult::denied(format!(
                    "Symlink targets device file: {file_path} -> {real_str}"
                ));
            }
            if real_path
                .file_name()
                .is_some_and(|name| is_protected_file_name(&name.to_string_lossy()))
            {
                return PathCheckResult::needs_confirmation(format!(
                    "Symlink targets sensitive file: {file_path} -> {real_str}"
                ));
            }
        }

        // 5.7 Windows 路径绕过检测 — Layer 5（旧源 L453-455）
        if let Some(win_check) = check_windows_bypass(file_path) {
            return win_check;
        }

        read_check
    }

    /// 解析路径（先词法规范化，再 `toRealPath`，失败则解析到存在祖先）。
    ///
    /// 对照 `PathSecurityService.java:514-530`：UNC 路径**先于**任何解析被拒绝
    /// （旧源抛 `IllegalArgumentException`，本实现回 `Err(message)`，消息逐字
    /// 相同）。调用方须把该错误映射为自身的失败关闭码——`FileAnalyzer#inspect`
    /// 映射为 `PROTECTED_PATH_DENIED`（旧源 L355-359 的 catch 分支）。
    ///
    /// # Errors
    ///
    /// `file_path` 是 UNC 路径（`//` 或 `\\` 前缀）时返回拒绝消息。
    pub fn resolve_path(
        &self,
        file_path: &str,
        working_directory: &str,
    ) -> Result<PathBuf, String> {
        if is_unc_path(file_path) {
            return Err(format!(
                "UNC path access denied (NTLM credential leak prevention): {file_path}"
            ));
        }
        Ok(Self::resolve_path_unchecked(file_path, working_directory))
    }

    /// [`Self::resolve_path`] 的解析主体（旧源 L519-529）。
    ///
    /// 仅供**已经**做过 `is_unc_path` 判定的内部调用点使用（旧源里那些调用点在
    /// 进入 `resolvePath` 前就已用 `isUncPath` 短路，异常分支不可达）。
    fn resolve_path_unchecked(file_path: &str, working_directory: &str) -> PathBuf {
        let resolved = absolute_normalized_path(file_path, working_directory);
        resolved
            .canonicalize()
            .unwrap_or_else(|_| resolve_through_existing_ancestor_or(&resolved, &resolved))
    }

    /// 授权目标绑定检查：执行前目标重解析必须与授权时完全一致。
    ///
    /// 对照 `PathSecurityService.java:541-561`。
    fn inspect_authorized_target(file_path: &str, working_directory: &str) -> AuthorizedPathCheck {
        if is_unc_path(file_path) {
            return AuthorizedPathCheck {
                target: None,
                permission: PathCheckResult::denied(format!(
                    "UNC path access denied (NTLM credential leak prevention): {file_path}"
                )),
            };
        }
        let authorized_target = absolute_normalized_path(file_path, working_directory);
        let current_target = Self::resolve_path_unchecked(file_path, working_directory);
        if current_target != authorized_target {
            return AuthorizedPathCheck {
                target: Some(authorized_target.clone()),
                permission: PathCheckResult::denied(format!(
                    "Authorized file target changed before execution: {} -> {}",
                    authorized_target.display(),
                    current_target.display()
                )),
            };
        }
        AuthorizedPathCheck {
            target: Some(current_target),
            permission: PathCheckResult::allowed(),
        }
    }

    /// 敏感目录匹配（含项目根自身命中与 scratchpad 放宽）。
    ///
    /// 逐条对照 `PathSecurityService.java:644-672`。
    fn matching_sensitive_directory(
        &self,
        resolved: &Path,
        project_root: &Path,
        outside_project: bool,
        sensitive_directories: &[&str],
    ) -> Option<String> {
        let mut path_to_inspect = resolved.to_path_buf();
        if !outside_project {
            let root_name = project_root
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
            for directory in sensitive_directories {
                if !root_name.eq_ignore_ascii_case(directory) {
                    continue;
                }
                if directory.eq_ignore_ascii_case(zk_core::paths::CONFIG_DIR_NAME)
                    && self.is_relaxed_scratchpad_marker(resolved, project_root, false)
                {
                    continue;
                }
                return Some((*directory).to_string());
            }
            path_to_inspect = resolved
                .strip_prefix(project_root)
                .unwrap_or(resolved)
                .to_path_buf();
        }
        for directory in sensitive_directories {
            if self.contains_unrelaxed_sensitive_component(
                &path_to_inspect,
                resolved,
                project_root,
                outside_project,
                directory,
            ) {
                return Some((*directory).to_string());
            }
        }
        None
    }

    /// 逐段扫描路径，遇到敏感段即命中（`.zk/scratchpad` 除外）。
    ///
    /// 逐条对照 `PathSecurityService.java:674-693`。
    fn contains_unrelaxed_sensitive_component(
        &self,
        path_to_inspect: &Path,
        resolved: &Path,
        project_root: &Path,
        outside_project: bool,
        sensitive_directory: &str,
    ) -> bool {
        let mut current = if path_to_inspect.is_absolute() {
            PathBuf::from("/")
        } else {
            project_root.to_path_buf()
        };
        for component in path_to_inspect.components() {
            let Component::Normal(name) = component else {
                continue;
            };
            current = current.join(name);
            if !name
                .to_string_lossy()
                .eq_ignore_ascii_case(sensitive_directory)
            {
                continue;
            }
            if sensitive_directory.eq_ignore_ascii_case(zk_core::paths::CONFIG_DIR_NAME)
                && self.is_relaxed_scratchpad_marker(resolved, &current, outside_project)
            {
                continue;
            }
            return true;
        }
        false
    }

    /// 仅精确的 `.zk/scratchpad` 标记被放宽；其他受保护后代（例如 `.ssh`）仍被检查。
    ///
    /// 放宽只认当前目录名：旧布局目录已不再是活动暂存区，故只保留保护、
    /// 不再放宽（#65）。
    ///
    /// 对照 `PathSecurityService.java:699-711`。
    fn is_relaxed_scratchpad_marker(
        &self,
        resolved: &Path,
        marker: &Path,
        outside_project: bool,
    ) -> bool {
        let scratchpad_root = marker.join("scratchpad");
        if !outside_project && resolved.starts_with(&scratchpad_root) {
            return true;
        }
        if !self.system_scratchpads.contains(resolved) {
            return false;
        }
        let Some(configured_root) = self.system_scratchpads.system_root() else {
            return false;
        };
        configured_root.starts_with(resolve_through_existing_ancestor_or(
            &scratchpad_root,
            &scratchpad_root,
        ))
    }

    // ==================== Layer 4: 危险删除检测 ====================

    /// 检测 Bash 命令中的危险删除操作。
    ///
    /// 对照 `PathSecurityService.java:468-485`。返回 `None` 表示安全。
    #[must_use]
    pub fn check_dangerous_removal(&self, command: &str) -> Option<String> {
        static PATTERN: LazyLock<regex::Regex> = LazyLock::new(|| {
            // 对照旧源 L470-471 的 `\b(rm|rmdir)\s+(?:-[a-zA-Z]{0,10}\s+){0,5}(\S+)`。
            // Rust regex 不支持 `\b`，改用等价的非单词字符边界断言组。
            regex::Regex::new(r"(?:^|[^0-9A-Za-z_])(rm|rmdir)\s+(?:-[a-zA-Z]{0,10}\s+){0,5}(\S+)")
                .expect("dangerous removal pattern is a literal")
        });
        for captures in PATTERN.captures_iter(command) {
            let target = captures.get(2)?.as_str();
            let resolved = resolve_path_variables(target);
            let norm_target = normalize_path(&resolved);
            for dangerous in DANGEROUS_REMOVAL_TARGETS.iter() {
                if norm_target == normalize_path(dangerous) {
                    return Some(format!(
                        "Dangerous removal denied: {command} (target: {norm_target})"
                    ));
                }
            }
            if target == "*" || target == "." || target == ".." {
                return Some(format!("Wildcard removal denied: {command}"));
            }
        }
        None
    }

    // ==================== Layer 8: 敏感系统文件读取检测 ====================

    /// 检测 Bash 命令中是否存在对敏感系统文件的读取操作。
    ///
    /// 逐条对照 `PathSecurityService.java:840-898`。此检查应在 BYPASS 模式之前执行，
    /// 确保不可绕过。返回 `None` 表示安全。
    #[must_use]
    pub fn check_sensitive_file_read(&self, command: &str) -> Option<String> {
        if command.trim().is_empty() {
            return None;
        }
        let home = user_home();
        for sub in command.split(['|', ';', '&']) {
            let trimmed = sub.trim();
            if trimmed.is_empty() {
                continue;
            }
            let tokens: Vec<&str> = trimmed.split_whitespace().collect();
            let Some(first) = tokens.first() else {
                continue;
            };
            // 去除路径前缀（如 /usr/bin/cat）。
            let cmd = first.rsplit('/').next().unwrap_or(first);
            if !READ_COMMANDS.contains(&cmd) {
                continue;
            }
            for arg in &tokens[1..] {
                // 跳过选项参数。
                if arg.starts_with('-') {
                    continue;
                }
                let expanded = arg.replace('~', &home).replace("$HOME", &home);

                // 敏感系统文件（精确匹配）。
                for sensitive in SENSITIVE_SYSTEM_FILES {
                    if expanded == *sensitive {
                        return Some(format!(
                            "Sensitive file access denied: {arg} (matches blocked path: {sensitive})"
                        ));
                    }
                }
                // 敏感用户文件（前缀匹配）。
                for user_path in SENSITIVE_USER_PATHS {
                    let expanded_user_path = user_path.replace('~', &home);
                    if expanded == expanded_user_path || expanded.starts_with(&expanded_user_path) {
                        return Some(format!(
                            "Sensitive file access denied: {arg} (matches blocked path: {user_path})"
                        ));
                    }
                }
                // 敏感系统目录（前缀匹配，排除安全白名单）。
                for dir in SENSITIVE_SYSTEM_DIRS {
                    if expanded.starts_with(dir) || expanded == dir[..dir.len() - 1] {
                        if SAFE_PROC_PATHS.iter().any(|safe| expanded == *safe) {
                            continue;
                        }
                        return Some(format!(
                            "Sensitive directory access denied: {arg} (within restricted area: {dir})"
                        ));
                    }
                }
            }
        }
        None
    }

    /// 递归内容读取器必须跳过的精确 basename。对照 `PathSecurityService.java:734-736`。
    #[must_use]
    pub fn protected_file_names(&self) -> BTreeSet<&'static str> {
        DANGEROUS_FILES.iter().copied().collect()
    }

    /// 递归进程型读取器必须排除的 basename glob。对照 `PathSecurityService.java:739-743`。
    #[must_use]
    pub fn protected_file_globs(&self) -> BTreeSet<&'static str> {
        let mut patterns: BTreeSet<&'static str> = DANGEROUS_FILES.iter().copied().collect();
        patterns.insert(".env*");
        patterns
    }

    /// 递归内容读取器必须跳过的精确目录名。对照 `PathSecurityService.java:765-767`。
    #[must_use]
    pub fn protected_directory_names(&self) -> BTreeSet<&'static str> {
        DANGEROUS_DIRECTORIES.iter().copied().collect()
    }
}

/// 直接读取与递归读取共用的大小写不敏感精确/前缀策略。
///
/// 对照 `PathSecurityService.java:746-751`。
#[must_use]
pub fn is_protected_file_name(file_name: &str) -> bool {
    let normalized = file_name.to_lowercase();
    DANGEROUS_FILES.contains(&normalized.as_str()) || normalized.starts_with(".env")
}

/// 规范路径与词法路径的 basename 任一命中即返回小写名。
///
/// 对照 `PathSecurityService.java:753-762`。
fn protected_file_name(canonical: &Path, lexical: &Path) -> Option<String> {
    for candidate in [canonical, lexical] {
        let Some(file_name) = candidate.file_name() else {
            continue;
        };
        let file_name = file_name.to_string_lossy();
        if is_protected_file_name(&file_name) {
            return Some(file_name.to_lowercase());
        }
    }
    None
}

/// Layer 8 的路径面：敏感系统文件 / 用户凭据路径 / sudoers 目录。
///
/// 逐条对照 `PathSecurityService.java:713-731`。
#[must_use]
pub fn is_sensitive_system_or_user_path(path: &Path) -> bool {
    let resolved = path.to_string_lossy().into_owned();
    // macOS 把 /etc 映射到 /private/etc，策略以 /etc 形态表达。
    let system_path = resolved
        .strip_prefix("/private")
        .filter(|_| resolved.starts_with("/private/etc/"))
        .unwrap_or(&resolved);
    if SENSITIVE_SYSTEM_FILES.contains(&system_path) || system_path.starts_with("/etc/sudoers.d/") {
        return true;
    }
    let home = user_home();
    SENSITIVE_USER_PATHS.iter().any(|user_path| {
        let expanded = user_path.replace('~', &home);
        if expanded.ends_with('/') {
            resolved.starts_with(&expanded)
        } else {
            resolved == expanded
        }
    })
}

/// Layer 5：Windows 路径绕过检测（NTFS ADS / 8.3 短名 / DOS 设备名）。
///
/// 对照 `PathSecurityService.java:489-507`。非 Windows 宿主直接返回 `None`。
#[must_use]
pub fn check_windows_bypass(raw_path: &str) -> Option<PathCheckResult> {
    static ADS: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^.*:[^/\\].*$").expect("literal pattern"));
    static SHORT_NAME: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^.*~\d.*$").expect("literal pattern"));
    if !is_windows() {
        return None;
    }
    if ADS.is_match(raw_path) {
        return Some(PathCheckResult::denied(format!(
            "NTFS ADS path detected: {raw_path}"
        )));
    }
    if SHORT_NAME.is_match(raw_path) {
        return Some(PathCheckResult::denied(format!(
            "8.3 short filename detected: {raw_path}"
        )));
    }
    if let Some(name) = Path::new(raw_path).file_name() {
        let name = name.to_string_lossy();
        let stem = name
            .rsplit_once('.')
            .map_or(name.as_ref(), |(head, _)| head)
            .to_uppercase();
        if DOS_DEVICE_NAMES.contains(&stem.as_str()) {
            return Some(PathCheckResult::denied(format!(
                "DOS device name detected: {raw_path}"
            )));
        }
    }
    None
}
