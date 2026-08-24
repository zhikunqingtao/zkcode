//! `WorkspaceIdentityServiceTest.java`（97 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! 旧测试同时断言 `GitService.isGitRepositoryRoot` 与 `isGitRepository`；旧源
//! `GitService.java:92-127` 两者都最终委派到
//! `WorkspaceIdentityService.isValidatedGitRepositoryRoot`（`findRepositoryRoot`
//! 「刻意不上溯祖先」），入参已是规范目录时二者恒等价，故 Rust 侧两条断言都落到
//! [`WorkspaceIdentityService::is_validated_git_repository_root`]。

mod common;

use std::path::Path;
use std::process::Command;

use common::TempRoot;
use zk_authz::workspace::WorkspaceIdentityService;

fn git_gate_enabled() -> bool {
    std::env::var("ZK_RUN_GIT_TESTS").as_deref() == Ok("true")
}

/// 旧测试私有 `runGit`（`WorkspaceIdentityServiceTest.java:84-96`）。
fn run_git(directory: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(directory)
        .output()
        .expect("spawn git");
    assert!(
        output.status.success(),
        "git {} failed: {}{}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn create_dir(root: &Path, name: &str) -> std::path::PathBuf {
    let path = root.join(name);
    std::fs::create_dir(&path).expect("create dir");
    path.canonicalize().expect("canonical dir")
}

/// 旧源 `WorkspaceIdentityServiceTest.java:21-31` `ordinaryRepositoryIsAValidatedRoot`。
#[test]
fn ordinary_repository_is_a_validated_root() {
    if !git_gate_enabled() {
        return;
    }
    let temp = TempRoot::new("workspace-identity-ordinary");
    let identities = WorkspaceIdentityService;

    // L23-24
    let repository = create_dir(temp.path(), "repository");
    run_git(&repository, &["init"]);

    // L26-30
    let identity = identities.resolve(&repository).expect("resolve repository");
    assert_eq!(identity.authorization_root, repository);
    assert!(identities.is_validated_git_repository_root(&repository));
}

/// 旧源 `WorkspaceIdentityServiceTest.java:33-51`
/// `linkedWorktreeRemainsValidAndSharesWorkspaceIdentity`。
#[test]
fn linked_worktree_remains_valid_and_shares_workspace_identity() {
    if !git_gate_enabled() {
        return;
    }
    let temp = TempRoot::new("workspace-identity-worktree");
    let identities = WorkspaceIdentityService;

    // L35-42
    let repository = create_dir(temp.path(), "main");
    run_git(&repository, &["init"]);
    run_git(&repository, &["config", "user.email", "test@example.com"]);
    run_git(&repository, &["config", "user.name", "Test User"]);
    run_git(&repository, &["commit", "--allow-empty", "-m", "initial"]);
    let linked = temp.path().join("linked");
    run_git(
        &repository,
        &[
            "worktree",
            "add",
            "--detach",
            linked.to_string_lossy().as_ref(),
        ],
    );
    let linked = linked.canonicalize().expect("canonical linked worktree");

    // L44-50：链接 worktree 自身是合法根，且与主仓共享 workspaceKey。
    let main_identity = identities.resolve(&repository).expect("resolve main");
    let linked_identity = identities.resolve(&linked).expect("resolve linked");
    assert!(identities.is_validated_git_repository_root(&linked));
    assert_eq!(linked_identity.authorization_root, linked);
    assert_eq!(linked_identity.workspace_key, main_identity.workspace_key);
}

/// 旧源 `WorkspaceIdentityServiceTest.java:53-66`
/// `gitDirectorySymlinkToExternalRepositoryIsRejected`。
#[test]
fn git_directory_symlink_to_external_repository_is_rejected() {
    if !git_gate_enabled() {
        return;
    }
    let temp = TempRoot::new("workspace-identity-symlink");
    let identities = WorkspaceIdentityService;

    // L55-58
    let external = create_dir(temp.path(), "external-symlink-target");
    run_git(&external, &["init"]);
    let project = create_dir(temp.path(), "symlink-project");
    std::os::unix::fs::symlink(external.join(".git"), project.join(".git"))
        .expect("create .git symlink");

    // L60-65：外部 .git 软链不得授予仓库级能力，但普通文件操作仍可用；
    // workspaceKey 必须与外部仓库不同。
    assert!(!identities.is_validated_git_repository_root(&project));
    let identity = identities.resolve(&project).expect("resolve project");
    assert_eq!(identity.authorization_root, project);
    assert_ne!(
        identity.workspace_key,
        identities
            .resolve(&external)
            .expect("resolve external")
            .workspace_key
    );
}

/// 旧源 `WorkspaceIdentityServiceTest.java:68-82`
/// `unregisteredExternalGitDirectoryFileIsRejected`。
#[test]
fn unregistered_external_git_directory_file_is_rejected() {
    if !git_gate_enabled() {
        return;
    }
    let temp = TempRoot::new("workspace-identity-gitfile");
    let identities = WorkspaceIdentityService;

    // L70-74
    let external = create_dir(temp.path(), "external-gitfile-target");
    run_git(&external, &["init"]);
    let project = create_dir(temp.path(), "gitfile-project");
    std::fs::write(
        project.join(".git"),
        format!("gitdir: {}\n", external.join(".git").display()),
    )
    .expect("write .git file");

    // L76-81
    assert!(!identities.is_validated_git_repository_root(&project));
    let identity = identities.resolve(&project).expect("resolve project");
    assert_eq!(identity.authorization_root, project);
    assert_ne!(
        identity.workspace_key,
        identities
            .resolve(&external)
            .expect("resolve external")
            .workspace_key
    );
}
