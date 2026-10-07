//! Real Git fixtures cover command scope and failure semantics without touching a user checkout.

use crate::command::{CommandContext, CommandRegistry, CommandResult};
use crate::state::AppState;
use std::path::{Path, PathBuf};

use super::git_review::{run_git, run_git_raw};

struct Repository(PathBuf);

impl Repository {
    fn new() -> Self {
        let directory =
            std::env::temp_dir().join(format!("zk-git-regression-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let repository = Self(std::fs::canonicalize(directory).unwrap());
        for args in [
            vec!["init", "-q", "--initial-branch=main"],
            vec!["config", "user.email", "fixture@example.invalid"],
            vec!["config", "user.name", "fixture"],
            vec!["config", "commit.gpgsign", "false"],
        ] {
            git(&repository.0, &args);
        }
        repository
    }

    fn write(&self, path: &str, content: &str) {
        std::fs::write(self.0.join(path), content).unwrap();
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn git(directory: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

async fn execute(directory: &Path, name: &str, args: &str) -> CommandResult {
    let ctx = CommandContext::of(
        "git-fixture",
        directory.to_str().unwrap(),
        "kimi-k3",
        AppState::for_tests(),
    );
    CommandRegistry::with_builtin_commands()
        .find_command(name)
        .unwrap()
        .execute(args, &ctx)
        .await
}

#[tokio::test]
async fn diff_arguments_are_exact_and_read_failures_cannot_report_no_changes() {
    let repository = Repository::new();
    repository.write("tracked", "staged content\n");
    git(&repository.0, &["add", "tracked"]);
    for args in ["staged", "--staged", " STAGED "] {
        let CommandResult::Jsx(data) = execute(&repository.0, "diff", args).await else {
            panic!("staged preview");
        };
        assert_eq!(data["staged"], true);
        assert!(data["diff"].as_str().unwrap().contains("staged content"));
    }
    for args in [
        "staged extra",
        "not-staged",
        "--cached",
        "staged; echo unsafe",
    ] {
        assert!(matches!(
            execute(&repository.0, "diff", args).await,
            CommandResult::Error(_)
        ));
    }
    assert_eq!(
        execute(&repository.0, "diff", "unstaged").await,
        CommandResult::text("无差异")
    );
    repository.write(".git/index", "broken index");
    for name in ["diff", "commit", "review"] {
        assert!(
            matches!(
                execute(&repository.0, name, "").await,
                CommandResult::Error(_)
            ),
            "{name} swallowed a failed read"
        );
    }
}

#[tokio::test]
async fn complete_multi_megabyte_unstaged_and_staged_diffs_reach_the_preview() {
    let repository = Repository::new();
    repository.write("large.txt", "original\n");
    git(&repository.0, &["add", "large.txt"]);
    git(&repository.0, &["commit", "-qm", "baseline"]);
    let mut content = String::from("FIRST large diff marker\n");
    for line in 0..40_000 {
        use std::fmt::Write;
        writeln!(content, "{line:05} 中文 {}", "x".repeat(80)).unwrap();
    }
    content.push_str("LAST large diff marker\n");
    repository.write("large.txt", &content);
    let working_dir = repository.0.to_str().unwrap();
    for (args, preview_args) in [
        (vec!["diff"], "unstaged"),
        (vec!["diff", "--cached"], "staged"),
    ] {
        if preview_args == "staged" {
            git(&repository.0, &["add", "large.txt"]);
        }
        let expected = git(&repository.0, &args);
        assert!(expected.len() > 3 * 1024 * 1024);
        let captured = run_git_raw(working_dir, &args)
            .await
            .expect("complete diff within the human capture budget");
        assert_eq!(captured, expected);
        assert!(captured.contains("+LAST large diff marker\n"));
        let CommandResult::Jsx(preview) = execute(&repository.0, "diff", preview_args).await else {
            panic!("large {preview_args} diff must produce a preview");
        };
        let preview = preview["diff"].as_str().unwrap();
        assert!(preview.contains("FIRST large diff marker"));
        assert!(preview.ends_with("...(已截断)"));
        assert!(!preview.contains("LAST large diff marker"));
    }
    assert_eq!(
        run_git_raw(working_dir, &["diff"]).await,
        Some(String::new())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn raw_git_preserves_whitespace_crlf_nul_and_unicode_without_merging_stderr() {
    let repository = Repository::new();
    let args = [
        "-c",
        "alias.zk-raw=!printf ' \\t中文\\r\\nsecond\\n\\000tail \\t\\000'; printf 'diagnostic\\n' >&2",
        "zk-raw",
    ];
    let expected = " \t中文\r\nsecond\n\0tail \t\0";
    let working_dir = repository.0.to_str().unwrap();
    assert_eq!(git(&repository.0, &args), expected);
    assert_eq!(
        run_git_raw(working_dir, &args).await.as_deref(),
        Some(expected)
    );
    assert_eq!(
        run_git(working_dir, &args).await.as_deref(),
        Some(expected.trim())
    );
    assert_eq!(
        run_git_raw(working_dir, &["not-a-real-command"]).await,
        None
    );
}

#[tokio::test]
async fn commit_preview_contains_only_complete_staged_paths_and_preserves_message_arguments() {
    let repository = Repository::new();
    let filename = " leading\nfile ";
    repository.write(filename, "staged\n");
    repository.write("untracked", "never auto stage\n");
    git(&repository.0, &["add", "--", filename]);
    let CommandResult::Jsx(data) = execute(&repository.0, "commit", "").await else {
        panic!("preview");
    };
    assert_eq!(data["changedFiles"], serde_json::json!([filename]));
    assert_eq!(data["fileCount"], 1);
    let message = "fixture: quotes ' \" and literal $(touch forbidden)\n\nsecond paragraph";
    assert!(matches!(
        execute(&repository.0, "commit", message).await,
        CommandResult::Text(_)
    ));
    assert_eq!(
        git(&repository.0, &["log", "-1", "--format=%B"]).trim_end(),
        message
    );
    assert!(git(&repository.0, &["status", "--porcelain"]).contains("?? untracked"));
    assert!(!repository.0.join("forbidden").exists());
}

#[tokio::test]
async fn commit_does_not_attempt_unstaged_changes_but_accepts_empty_tree_linked_worktree_merge() {
    let repository = Repository::new();
    repository.write("untracked", "keep local\n");
    assert_eq!(
        execute(&repository.0, "commit", "should not commit").await,
        CommandResult::text("没有已暂存的变更；请先暂存需要提交的文件。")
    );
    git(&repository.0, &["commit", "--allow-empty", "-m", "base"]);
    git(&repository.0, &["checkout", "-b", "other"]);
    git(&repository.0, &["commit", "--allow-empty", "-m", "other"]);
    git(&repository.0, &["checkout", "main"]);
    git(&repository.0, &["commit", "--allow-empty", "-m", "main"]);
    let worktree = repository.0.join("linked");
    git(
        &repository.0,
        &[
            "worktree",
            "add",
            "-b",
            "linked",
            worktree.to_str().unwrap(),
        ],
    );
    git(&worktree, &["merge", "--no-ff", "--no-commit", "other"]);
    assert!(worktree.join(".git").is_file());
    let CommandResult::Jsx(data) = execute(&worktree, "commit", "").await else {
        panic!("empty merge preview");
    };
    assert_eq!(data["changedFiles"], serde_json::json!([]));
    assert!(matches!(
        execute(&worktree, "commit", "finish merge").await,
        CommandResult::Text(_)
    ));
    assert_eq!(
        git(&worktree, &["rev-list", "--parents", "-1", "HEAD"])
            .split_whitespace()
            .count(),
        3
    );
}

#[tokio::test]
async fn explicit_review_scope_never_prefetches_excluded_diff_and_default_discloses_coverage() {
    let repository = Repository::new();
    repository.write("sensitive", "private source\n");
    git(&repository.0, &["add", "sensitive"]);
    let CommandResult::Text(default) = execute(&repository.0, "review", "").await else {
        panic!("default review");
    };
    assert!(default.contains("未跟踪文件尚未核验"));
    assert!(default.contains("未暂存差异预览"));
    assert!(default.contains("已暂存差异预览"));
    assert!(default.contains("private source"));
    repository.write(".git/index", "unreadable, must not prefetch");
    let requested = "只比较 main...other，排除 sensitive；不要自动换范围";
    let CommandResult::Text(scoped) = execute(&repository.0, "review", requested).await else {
        panic!("explicit scope should not read index");
    };
    assert!(scoped.contains(requested));
    assert!(scoped.contains("只审查、不修改"));
    assert!(!scoped.contains("private source"));
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_commit_with_multi_megabyte_hook_output_succeeds_exactly_once() {
    use std::os::unix::fs::PermissionsExt;
    let repository = Repository::new();
    git(
        &repository.0,
        &["commit", "--allow-empty", "-qm", "baseline"],
    );
    let before: usize = git(&repository.0, &["rev-list", "--count", "HEAD"])
        .trim()
        .parse()
        .unwrap();
    repository.write("staged", "one explicit change\n");
    git(&repository.0, &["add", "staged"]);
    repository.write(".git/hook-output", &"hook-output\n".repeat(200_000));
    assert!(
        std::fs::metadata(repository.0.join(".git/hook-output"))
            .unwrap()
            .len()
            > 1024 * 1024
    );
    repository.write(
        ".git/hooks/post-commit",
        "#!/bin/sh\nprintf 'attempt\\n' >> .git/hook-attempts\ncat .git/hook-output || exit 1\nprintf 'complete' > .git/hook-complete\n",
    );
    std::fs::set_permissions(
        repository.0.join(".git/hooks/post-commit"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let CommandResult::Text(message) = execute(&repository.0, "commit", "large hook").await else {
        panic!("a complete commit report within the human capture budget must succeed");
    };
    assert!(message.starts_with("✅ 已提交:"));
    let after: usize = git(&repository.0, &["rev-list", "--count", "HEAD"])
        .trim()
        .parse()
        .unwrap();
    assert_eq!(after, before + 1);
    assert_eq!(
        git(&repository.0, &["log", "-1", "--format=%s"]).trim(),
        "large hook"
    );
    assert_eq!(
        std::fs::read_to_string(repository.0.join(".git/hook-attempts")).unwrap(),
        "attempt\n"
    );
    assert_eq!(
        std::fs::read_to_string(repository.0.join(".git/hook-complete")).unwrap(),
        "complete"
    );
    assert!(git(&repository.0, &["status", "--porcelain"]).is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_commit_reports_uncertainty_and_does_not_retry_a_hook() {
    use std::os::unix::fs::PermissionsExt;
    let repository = Repository::new();
    repository.write("staged", "data\n");
    git(&repository.0, &["add", "staged"]);
    repository.write(
        ".git/hooks/pre-commit",
        "#!/bin/sh\nprintf 'attempt\\n' >> .git/hook-attempts\nexit 1\n",
    );
    std::fs::set_permissions(
        repository.0.join(".git/hooks/pre-commit"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let CommandResult::Error(message) = execute(&repository.0, "commit", "rejected").await else {
        panic!("hook must fail");
    };
    assert!(message.contains("结果不确定"));
    assert!(message.contains("git log / git status"));
    assert_eq!(
        std::fs::read_to_string(repository.0.join(".git/hook-attempts")).unwrap(),
        "attempt\n"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn completed_commit_with_a_late_hook_is_uncertain_until_the_hook_really_finishes() {
    use std::os::unix::fs::PermissionsExt;
    let repository = Repository::new();
    repository.write("staged", "data\n");
    git(&repository.0, &["add", "staged"]);
    repository.write(
        ".git/hooks/post-commit",
        "#!/bin/sh\n(sleep 5.8; printf 'done' > .git/late-hook-done) >/dev/null 2>&1 &\n",
    );
    std::fs::set_permissions(
        repository.0.join(".git/hooks/post-commit"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let CommandResult::Error(message) = execute(&repository.0, "commit", "one commit").await else {
        panic!("live hook must remain uncertain");
    };
    assert!(message.contains("结果不确定"));
    assert_eq!(
        git(&repository.0, &["rev-list", "--count", "HEAD"]).trim(),
        "1"
    );
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while !repository.0.join(".git/late-hook-done").is_file() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("an already-completed Git command must not authorize killing its late hook");
}
