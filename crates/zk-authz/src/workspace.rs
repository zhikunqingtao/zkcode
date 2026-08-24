//! 规范授权根目录与稳定工作区身份的唯一解析权威。
//!
//! 逐字对照 `authorization/WorkspaceIdentityService.java`（L25-228）。资源边界始终是
//! 当前 worktree 根目录；关联 Git worktree 只通过共同 Git 目录共享 `workspace_key`。

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::hashing::workspace_key;
use crate::model::{AuthzError, AuthzResult};

/// Git 校验子进程超时。对照 `WorkspaceIdentityService.java:26`。
const GIT_TIMEOUT: Duration = Duration::from_secs(5);
/// Git 输出字节上限。对照 `WorkspaceIdentityService.java:27`。
const MAX_GIT_OUTPUT_BYTES: usize = 1024 * 1024;
/// `.git` / `commondir` 标记文件的合法大小上限。对照 `WorkspaceIdentityService.java:170`。
const MAX_MARKER_BYTES: u64 = 4096;

/// 解析出的工作区身份。对照 `WorkspaceIdentityService.java:29`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceIdentity {
    /// 授权根目录（当前 worktree 的规范路径）。
    pub authorization_root: PathBuf,
    /// 稳定工作区键：`SHA256("workspace-v2\0" + identityPath)`。
    pub workspace_key: String,
}

/// 工作区身份解析服务。
#[derive(Debug, Clone, Copy, Default)]
pub struct WorkspaceIdentityService;

impl WorkspaceIdentityService {
    /// 解析配置根目录为规范授权根 + 工作区键。
    ///
    /// 对照 `WorkspaceIdentityService.java:31-59`。`.git` 间接层不安全或未注册时
    /// **不使整个目录不可用**，而是回落到目录自身身份（L44-52 的显式裁定）。
    ///
    /// # Errors
    ///
    /// 目录缺失、非目录、或无法规范化时返回 `AUTHORIZATION_WORKSPACE_INVALID`。
    pub fn resolve(&self, configured_root: &Path) -> AuthzResult<WorkspaceIdentity> {
        if configured_root.as_os_str().is_empty() {
            return Err(invalid("Workspace root is missing"));
        }
        let absolute = absolute_normalized(configured_root);
        if !fs::symlink_metadata(&absolute).is_ok_and(|meta| meta.is_dir()) {
            return Err(invalid("Workspace root must be an existing directory"));
        }
        let root = absolute
            .canonicalize()
            .map_err(|_| invalid("Workspace root cannot be canonicalized"))?;
        // 畸形或未注册的 .git 间接层必须停用仓库级能力，但不得让一个本来有效的
        // 选定目录无法进行普通文件操作。
        let identity_path = git_common_directory(&root).unwrap_or_else(|| root.clone());
        Ok(WorkspaceIdentity {
            workspace_key: workspace_key(&identity_path.to_string_lossy()),
            authorization_root: root,
        })
    }

    /// 校验 `configured_root` 是否为间接层安全的真实 Git worktree 根。
    ///
    /// 对照 `WorkspaceIdentityService.java:66-80`。外部 Git 元数据仅在 Git 自身
    /// 把该规范路径列为 worktree 时被接受。
    #[must_use]
    pub fn is_validated_git_repository_root(&self, configured_root: &Path) -> bool {
        let Ok(root) = absolute_normalized(configured_root).canonicalize() else {
            return false;
        };
        if !fs::symlink_metadata(&root).is_ok_and(|meta| meta.is_dir())
            || fs::symlink_metadata(root.join(".git")).is_err()
        {
            return false;
        }
        if git_common_directory(&root).is_none() {
            return false;
        }
        run_git(&root, &["rev-parse", "--show-toplevel"])
            .and_then(|value| resolve_git_path(&root, &value))
            .is_some_and(|top| top == root)
    }
}

/// 解析共同 Git 目录（`None` = 间接层不安全，调用方回落到目录身份）。
///
/// 逐条对照 `WorkspaceIdentityService.java:82-129`。
fn git_common_directory(root: &Path) -> Option<PathBuf> {
    let marker = root.join(".git");
    let Ok(marker_meta) = fs::symlink_metadata(&marker) else {
        return Some(root.to_path_buf());
    };

    let git_directory = if marker_meta.is_dir() {
        marker.canonicalize().ok()?
    } else if marker_meta.is_file() {
        // `symlink_metadata().is_file()` 已排除符号链接，对齐 L89-90 的
        // `isRegularFile(NOFOLLOW_LINKS) && !isSymbolicLink`。
        let line = read_marker(&marker)?;
        let raw = line.strip_prefix("gitdir:")?.trim();
        if raw.is_empty() {
            return None;
        }
        let target = PathBuf::from(raw);
        let joined = if target.is_absolute() {
            target
        } else {
            root.join(target)
        };
        absolute_normalized(&joined).canonicalize().ok()?
    } else {
        // Unsafe .git marker（符号链接 / 设备 / FIFO 等）。
        return None;
    };
    if !fs::symlink_metadata(&git_directory).is_ok_and(|meta| meta.is_dir()) {
        return None;
    }

    let common_marker = git_directory.join("commondir");
    let mut common_directory = git_directory.clone();
    if let Ok(meta) = fs::symlink_metadata(&common_marker) {
        if !meta.is_file() {
            return None;
        }
        let raw = read_marker(&common_marker)?;
        if raw.is_empty() {
            return None;
        }
        let common = PathBuf::from(&raw);
        let joined = if common.is_absolute() {
            common
        } else {
            git_directory.join(common)
        };
        common_directory = absolute_normalized(&joined).canonicalize().ok()?;
    }
    if !fs::symlink_metadata(&common_directory).is_ok_and(|meta| meta.is_dir()) {
        return None;
    }

    let external_metadata = !git_directory.starts_with(root) || !common_directory.starts_with(root);
    if external_metadata && !validate_registered_worktree(root, &git_directory, &common_directory) {
        return None;
    }
    Some(common_directory)
}

/// 外部 Git 元数据的三重校验。
///
/// 对照 `WorkspaceIdentityService.java:131-150`：`rev-parse --absolute-git-dir` /
/// `rev-parse --git-common-dir` / `worktree list --porcelain` 全部一致才接受。
fn validate_registered_worktree(root: &Path, git_dir: &Path, common_dir: &Path) -> bool {
    let reported_git = run_git(root, &["rev-parse", "--absolute-git-dir"])
        .and_then(|value| resolve_git_path(root, &value));
    let reported_common = run_git(root, &["rev-parse", "--git-common-dir"])
        .and_then(|value| resolve_git_path(root, &value));
    if reported_git.as_deref() != Some(git_dir) || reported_common.as_deref() != Some(common_dir) {
        return false;
    }
    let Some(listing) = run_git(root, &["worktree", "list", "--porcelain"]) else {
        return false;
    };
    listing
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .any(|value| {
            PathBuf::from(value)
                .canonicalize()
                .is_ok_and(|path| path == root)
        })
}

/// 对照 `WorkspaceIdentityService.java:160-166`。
fn resolve_git_path(root: &Path, value: &str) -> Option<PathBuf> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let path = PathBuf::from(trimmed);
    let joined = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    absolute_normalized(&joined).canonicalize().ok()
}

/// 对照 `WorkspaceIdentityService.java:168-174`。
fn read_marker(marker: &Path) -> Option<String> {
    let size = fs::metadata(marker).ok()?.len();
    if size == 0 || size > MAX_MARKER_BYTES {
        return None;
    }
    Some(fs::read_to_string(marker).ok()?.trim().to_string())
}

/// 带超时与输出上限的 Git 子进程调用。
///
/// 对照 `WorkspaceIdentityService.java:176-212`。Rust 侧用 `wait_timeout` 语义的
/// 轮询实现（`std::process` 无原生超时 API），退出码非 0 或输出超限均返回 `None`。
fn run_git(root: &Path, arguments: &[&str]) -> Option<String> {
    use std::io::Read as _;
    use std::time::Instant;

    let mut child = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .ok()?;

    let deadline = Instant::now() + GIT_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut buffer = Vec::new();
                if let Some(mut stdout) = child.stdout.take() {
                    let _ = stdout
                        .by_ref()
                        .take(MAX_GIT_OUTPUT_BYTES as u64 + 1)
                        .read_to_end(&mut buffer);
                }
                if buffer.len() > MAX_GIT_OUTPUT_BYTES || !status.success() {
                    return None;
                }
                return Some(String::from_utf8_lossy(&buffer).trim().to_string());
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

/// `Path.toAbsolutePath().normalize()` 的等价实现（纯词法，不触碰文件系统）。
pub(crate) fn absolute_normalized(path: &Path) -> PathBuf {
    use std::path::Component;
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    };
    let mut out = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Java `normalize()` 在根之上的 `..` 会被丢弃，此处语义一致。
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from("/")
    } else {
        out
    }
}

fn invalid(message: &str) -> AuthzError {
    AuthzError::new("AUTHORIZATION_WORKSPACE_INVALID", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 旧源 `WorkspaceIdentityServiceTest.java`：普通目录解析出稳定 workspaceKey。
    #[test]
    fn ordinary_directory_resolves_to_a_stable_workspace_key() {
        let dir = std::env::temp_dir();
        let service = WorkspaceIdentityService;
        let first = service.resolve(&dir).unwrap();
        let second = service.resolve(&dir).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.workspace_key.len(), 64);
    }

    /// 旧源 `WorkspaceIdentityService.java:32-34`：缺失根目录被拒绝。
    #[test]
    fn missing_workspace_root_is_rejected() {
        let service = WorkspaceIdentityService;
        let error = service.resolve(Path::new("")).unwrap_err();
        assert_eq!(error.code, "AUTHORIZATION_WORKSPACE_INVALID");
    }

    /// 旧源 `WorkspaceIdentityService.java:37-39`：非目录根被拒绝。
    #[test]
    fn non_directory_workspace_root_is_rejected() {
        let service = WorkspaceIdentityService;
        let error = service.resolve(Path::new("/etc/hosts")).unwrap_err();
        assert_eq!(error.code, "AUTHORIZATION_WORKSPACE_INVALID");
    }

    /// 旧源 `WorkspaceIdentityService.java:44-52`：无 `.git` 时回落到目录身份。
    #[test]
    fn workspace_without_git_metadata_falls_back_to_the_folder_identity() {
        let dir = std::env::temp_dir().canonicalize().unwrap();
        let service = WorkspaceIdentityService;
        let identity = service.resolve(&dir).unwrap();
        assert_eq!(identity.authorization_root, dir);
    }
}
