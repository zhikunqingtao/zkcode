//! Shell 状态管理器——逐字移植 `tool/bash/ShellStateManager.java`（202 行）。
//!
//! 旧源自述：**只**跨命令持久化 CWD，绝不持久化完整环境（`export -p` 快照既可能
//! 含凭据，也会因 Shell 序列化差异造成授权指纹漂移）。授权分析用的是「规范化后的
//! 实际继承环境」。
//!
//! 本模块被两条链路消费：
//! - **执行面**：[`BashTool`](crate::BashTool) 的 `wrap_command` + `resolve_working_directory`
//!   （`BashTool.java:344-346`）；
//! - **判定面**：`OperationAnalyzerRegistry` 经 `ShellStatePort` 取
//!   `authorization_environment_facts`（`OperationAnalyzerRegistry.java:272/290`）
//!   —— 其输出**直接进 `operationHash`**，故 `PATH` 规范化规则必须逐字一致，
//!   否则同一命令在两代实现下哈希不同、免弹缓存全部失效。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// 旧源 `SHELL_STATE_DIR`（L32-33：`java.io.tmpdir` / `ai-code-shells`）。
static SHELL_STATE_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| std::env::temp_dir().join("ai-code-shells"));

/// 旧源 `DIRECTORY_PERMISSIONS`（L34-35：`rwx------`）。
const DIRECTORY_PERMISSIONS: u32 = 0o700;
/// 旧源 `FILE_PERMISSIONS`（L36-37：`rw-------`）。
const FILE_PERMISSIONS: u32 = 0o600;

/// 旧源 `File.pathSeparator`（POSIX 恒 `:`）。
const PATH_SEPARATOR: char = ':';

/// 旧源 `wrapCommand` 的默认 heredoc 终止符（L81）。
const HEREDOC_DELIMITER: &str = "__ZHIKUN_EOF__";

/// Shell 状态管理器（旧 `ShellStateManager`）。
#[derive(Debug, Clone, Copy, Default)]
pub struct ShellStateManager;

impl ShellStateManager {
    /// 旧构造器（L39-47）：建目录 → 设 0700 → 清理历史 `*.env` 快照。
    ///
    /// 旧源失败只 `log.warn` 不抛，本实现同构（返回值恒为实例）。
    #[must_use]
    pub fn new() -> Self {
        let manager = Self;
        if let Err(error) = fs::create_dir_all(&*SHELL_STATE_DIR) {
            tracing::warn!(dir = %SHELL_STATE_DIR.display(), %error,
                "Failed to create shell state directory");
            return manager;
        }
        set_permissions_if_supported(&SHELL_STATE_DIR, DIRECTORY_PERMISSIONS);
        Self::delete_legacy_environment_snapshots();
        manager
    }

    /// 旧源 `getCwdTrackingPath(sessionId)`（L50-52）。
    #[must_use]
    pub fn cwd_tracking_path(session_id: &str) -> PathBuf {
        SHELL_STATE_DIR.join(format!("{session_id}.cwd"))
    }

    /// 旧源 `getTrackedCwd(sessionId, originalCwd)`（L57-70）：跟踪目录已不存在
    /// 时回退 `originalCwd`；读失败同样回退（旧源 `catch (IOException ignored)`）。
    #[must_use]
    pub fn tracked_cwd(session_id: &str, original_cwd: &str) -> String {
        let cwd_file = Self::cwd_tracking_path(session_id);
        if cwd_file.exists()
            && let Ok(tracked) = fs::read_to_string(&cwd_file)
        {
            let tracked = tracked.trim();
            if !tracked.is_empty() && Path::new(tracked).is_dir() {
                return tracked.to_owned();
            }
        }
        original_cwd.to_owned()
    }

    /// 旧源 `resolveWorkingDirectory(sessionId, originalCwd)`（L123-125）。
    #[must_use]
    pub fn resolve_working_directory(session_id: &str, original_cwd: &str) -> String {
        Self::tracked_cwd(session_id, original_cwd)
    }

    /// 旧源 `wrapCommand(userCommand, sessionId)`（L77-101）。
    ///
    /// heredoc + `source` 模式（而非 `eval`）避免二次展开；用户命令含默认终止符
    /// 时换用 `nanoTime` 后缀（旧源 L82-84）。
    #[must_use]
    pub fn wrap_command(user_command: &str, session_id: &str) -> String {
        let cwd_file = Self::cwd_tracking_path(session_id);
        let cwd_file = cwd_file.to_string_lossy();
        let delimiter = if user_command.contains(HEREDOC_DELIMITER) {
            format!("__ZHIKUN_EOF_{}__", nano_time())
        } else {
            HEREDOC_DELIMITER.to_owned()
        };
        format!(
            "umask 077\n\
             shopt -u extglob 2>/dev/null || true\n\
             __zhikun_cmd=$(mktemp)\n\
             __zhikun_cwd=$(mktemp '{cwd_file}.XXXXXX')\n\
             trap 'rm -f \"$__zhikun_cmd\" \"$__zhikun_cwd\"' EXIT\n\
             cat > \"$__zhikun_cmd\" <<'{delimiter}'\n\
             {user_command}\n\
             {delimiter}\n\
             source \"$__zhikun_cmd\"\n\
             __zhikun_exit=$?\n\
             rm -f \"$__zhikun_cmd\"\n\
             pwd > \"$__zhikun_cwd\"\n\
             chmod 600 \"$__zhikun_cwd\"\n\
             mv -f \"$__zhikun_cwd\" '{cwd_file}'\n\
             exit $__zhikun_exit"
        )
    }

    /// 旧源 `updateStateFromSnapshot(sessionId)`（L104-107）：包装脚本已原子替换
    /// CWD，此处仅记 debug（Shell 环境按产品约定不跨调用保存）。
    pub fn update_state_from_snapshot(session_id: &str) {
        tracing::debug!(session_id, "Shell state updated for session");
    }

    /// 旧源 `resetCwd(sessionId, originalCwd)`（L112-118）。
    pub fn reset_cwd(session_id: &str, original_cwd: &str) {
        if let Err(error) =
            write_private_file_atomically(&Self::cwd_tracking_path(session_id), original_cwd)
        {
            tracing::warn!(session_id, %error, "Failed to reset CWD for session");
        }
    }

    /// 旧源 `authorizationEnvironmentFacts(inheritedNames)`（L133-144）。
    ///
    /// 返回值只允许在内存中参与哈希，**不得**记录日志或持久化（旧源 L130）。
    /// `PATH` 保留空段代表当前目录的语义并去重；其余变量保持精确值，使真实环境
    /// 变化仍会使既有 Bash 授权失效。
    #[must_use]
    pub fn authorization_environment_facts(inherited_names: &[String]) -> BTreeMap<String, String> {
        // 旧源 L134-135：`LinkedHashSet` + 强制补 `PATH`。落点是 `TreeMap`，
        // 故插入序不可观测，此处直接用有序集合。
        let mut names: BTreeSet<&str> = inherited_names.iter().map(String::as_str).collect();
        names.insert("PATH");
        let mut facts = BTreeMap::new();
        for name in names {
            let value = std::env::var(name).ok();
            // 旧源 L140：只有 `PATH` 且有值时规范化。
            let value = match value {
                Some(value) if name == "PATH" => Some(normalize_path_for_authorization(&value)),
                other => other,
            };
            // 旧源 L141：缺失变量落 `<undefined>`（而非跳过）。
            facts.insert(
                name.to_owned(),
                value.unwrap_or_else(|| "<undefined>".to_owned()),
            );
        }
        facts
    }

    /// 旧源 `stateDirectory()`（L146-148，包可见，测试用）。
    #[doc(hidden)]
    #[must_use]
    pub fn state_directory() -> &'static Path {
        &SHELL_STATE_DIR
    }

    /// 旧源 `deleteLegacyEnvironmentSnapshots()`（L169-179）。
    fn delete_legacy_environment_snapshots() {
        let Ok(entries) = fs::read_dir(&*SHELL_STATE_DIR) else {
            return;
        };
        let mut deleted = 0_usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "env") && fs::remove_file(&path).is_ok() {
                deleted += 1;
            }
        }
        if deleted > 0 {
            tracing::info!(deleted, "Deleted legacy shell environment snapshot(s)");
        }
    }
}

/// 旧源 `normalizePathForAuthorization(value)`（L150-167）。
///
/// `split(quote(pathSeparator), -1)` 的 `-1` 保留尾部空段——空段与显式 `.` 都表示
/// 当前工作目录，不能当作无意义空白删除。`LinkedHashSet` 去重且保持首次出现序。
#[must_use]
pub fn normalize_path_for_authorization(value: &str) -> String {
    let mut normalized: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for segment in value.split(PATH_SEPARATOR) {
        // 旧源 L154-157
        let candidate = if segment.is_empty() {
            ".".to_owned()
        } else {
            // 旧源 L158-164：`Path.of(segment).normalize()`；规范化后为空落 `.`。
            // Rust 侧无「无法解析的路径」（`Path::new` 不校验），故旧源
            // `catch (RuntimeException)` 分支不可达（偏离 SS-01，EQUIVALENT）。
            let lexical = java_path_normalize(segment);
            if lexical.is_empty() {
                ".".to_owned()
            } else {
                lexical
            }
        };
        if seen.insert(candidate.clone()) {
            normalized.push(candidate);
        }
    }
    normalized.join(&PATH_SEPARATOR.to_string())
}

/// `java.nio.file.Path#normalize()` 的纯词法等价物。
///
/// 规则：丢弃 `.` 与空段；`name/..` 成对消除；**绝对**路径的前导 `..` 直接丢弃
/// （`/..` 即 `/`），**相对**路径的前导 `..` 保留。结果可能为空串（如输入 `.`）。
fn java_path_normalize(input: &str) -> String {
    let absolute = input.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for segment in input.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|last| *last != "..") {
                    parts.pop();
                } else if !absolute {
                    parts.push("..");
                }
            }
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    if absolute {
        format!("/{joined}")
    } else {
        joined
    }
}

/// 旧源 `writePrivateFileAtomically(target, content)`（L181-194）。
fn write_private_file_atomically(target: &Path, content: &str) -> std::io::Result<()> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    set_permissions_if_supported(parent, DIRECTORY_PERMISSIONS);
    let temporary = parent.join(format!(
        "{}{}.tmp",
        target.file_name().unwrap_or_default().to_string_lossy(),
        nano_time()
    ));
    let result = (|| {
        set_permissions_if_supported(&temporary, FILE_PERMISSIONS);
        fs::write(&temporary, content)?;
        fs::rename(&temporary, target)?;
        set_permissions_if_supported(target, FILE_PERMISSIONS);
        Ok(())
    })();
    // 旧源 finally：`Files.deleteIfExists(temporary)`。
    let _ = fs::remove_file(&temporary);
    result
}

/// 旧源 `setPosixPermissionsIfSupported(path, permissions)`（L196-201）。
///
/// 旧源以 `FileStore#supportsFileAttributeView("posix")` 探测；Rust 以
/// `cfg(unix)` 编译期区分，非 unix 目标为空实现。
fn set_permissions_if_supported(path: &Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(error) = fs::set_permissions(path, fs::Permissions::from_mode(mode)) {
            tracing::debug!(path = %path.display(), %error, "posix permissions not applied");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
}

/// 旧源 `System.nanoTime()` 的等价物（仅用于唯一后缀，不做时间语义）。
fn nano_time() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::{ShellStateManager, java_path_normalize, normalize_path_for_authorization};

    /// 旧源 L154-156：PATH 空段语义为「当前目录」，必须落 `.` 而非删除。
    #[test]
    fn empty_path_segments_become_current_directory() {
        assert_eq!(normalize_path_for_authorization(":/usr/bin"), ".:/usr/bin");
        assert_eq!(normalize_path_for_authorization("/usr/bin:"), "/usr/bin:.");
    }

    /// 旧源 L151 `LinkedHashSet`：语义相同的重复段去重，保持首次出现序。
    #[test]
    fn duplicate_segments_are_deduplicated_in_first_seen_order() {
        assert_eq!(
            normalize_path_for_authorization("/usr/bin:/usr/./bin:/opt:/usr/bin"),
            "/usr/bin:/opt"
        );
    }

    /// 旧源 L159：`Path.of(segment).normalize()` 的纯词法语义。
    #[test]
    fn lexical_normalization_matches_java_path_normalize() {
        assert_eq!(java_path_normalize("/usr/local/../bin"), "/usr/bin");
        assert_eq!(java_path_normalize("/.."), "/", "绝对路径前导 .. 被丢弃");
        assert_eq!(java_path_normalize("../a"), "../a", "相对路径前导 .. 保留");
        assert_eq!(java_path_normalize("a//b"), "a/b", "多重分隔符折叠");
        assert_eq!(java_path_normalize("."), "", "旧源随后回落为 .");
        assert_eq!(normalize_path_for_authorization("."), ".");
    }

    /// 旧源 L135 + L141：`PATH` 恒在结果内；缺失变量落 `<undefined>`。
    #[test]
    fn facts_always_include_path_and_mark_missing_as_undefined() {
        let facts = ShellStateManager::authorization_environment_facts(&[
            "ZK_DEFINITELY_ABSENT_VARIABLE".to_owned(),
        ]);
        assert!(facts.contains_key("PATH"), "PATH 强制补入");
        assert_eq!(facts["ZK_DEFINITELY_ABSENT_VARIABLE"], "<undefined>");
    }

    /// 旧源 L82-84：用户命令含默认终止符时换用 nanoTime 后缀。
    #[test]
    fn heredoc_delimiter_is_rotated_on_collision() {
        let wrapped = ShellStateManager::wrap_command("echo __ZHIKUN_EOF__", "s-1");
        assert!(wrapped.contains("__ZHIKUN_EOF_"), "{wrapped}");
        assert!(
            !wrapped.contains("<<'__ZHIKUN_EOF__'"),
            "冲突时不得沿用默认终止符: {wrapped}"
        );
    }

    /// 旧源 L86-100：包装脚本逐行骨架（umask / heredoc / source / cwd 原子替换）。
    #[test]
    fn wrapped_command_keeps_baseline_script_skeleton() {
        let wrapped = ShellStateManager::wrap_command("cd /tmp", "s-2");
        assert!(wrapped.starts_with("umask 077\nshopt -u extglob 2>/dev/null || true\n"));
        assert!(wrapped.contains("cat > \"$__zhikun_cmd\" <<'__ZHIKUN_EOF__'\ncd /tmp\n"));
        assert!(wrapped.contains("source \"$__zhikun_cmd\"\n__zhikun_exit=$?\n"));
        assert!(wrapped.contains("pwd > \"$__zhikun_cwd\"\nchmod 600 \"$__zhikun_cwd\"\n"));
        assert!(wrapped.ends_with("exit $__zhikun_exit"));
    }

    /// 旧源 L57-70：无跟踪文件 → 回退 `originalCwd`。
    #[test]
    fn tracked_cwd_falls_back_to_original() {
        let resolved = ShellStateManager::resolve_working_directory(
            "zk-session-without-tracking-file",
            "/tmp",
        );
        assert_eq!(resolved, "/tmp");
    }
}
