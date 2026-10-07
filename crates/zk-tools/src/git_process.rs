//! Git retains a live group anchor until hooks have actually stopped. Natural
//! Git completion never authorizes killing a hook that outlives Git itself.
use super::{
    Child, Command, Duration, ExecutionResourceTerminal, KILL_CONFIRM_GRACE, Path, ProcessOutcome,
    SENSITIVE_ENV_VARS, SIGTERM_EXIT_CODE, Stdio, TIMEOUT_EXIT_CODE, ToolContext, oneshot,
    pump_with_utf8, terminate,
};
use tokio::io::AsyncWriteExt;

const ANCHOR: &str = r#"IFS= read -r gate && [ "$gate" = start ] || exit 125
control=$1; shift
"$@" </dev/null &
command_pid=$!
printf '%s\n' "$command_pid" > "$control/pid"
wait "$command_pid"
command_status=$?
printf '%s\n' "$command_status" > "$control/exit"
IFS= read -r release
exit "$command_status"
"#;

/// Run raw Git with durable resource ownership and a retained process-group
/// identity. If a hook outlives the operation deadline, return uncertainty while
/// the owned supervisor continues observing it; never report clean completion.
///
/// # Errors
/// Returns an error when admission, spawning, ownership persistence or scope verification fails.
pub async fn run_git_program(
    args: &[String],
    cwd: &Path,
    timeout: Duration,
    ctx: &ToolContext,
) -> std::io::Result<ProcessOutcome> {
    run_with_capture_limit(args, cwd, timeout, ctx, super::MAX_CAPTURE_BYTES).await
}

/// Preserve multi-megabyte human-readable diffs and commit reports before applying a
/// display/prompt budget. Machine protocols retain the ordinary one-MiB limit.
/// The 16-MiB hard boundary prevents repository data from exhausting memory;
/// callers must reject `truncated` output instead of treating it as complete.
///
/// # Errors
/// Same admission, process and ownership errors as [`run_git_program`].
pub async fn run_git_human_program(
    args: &[String],
    cwd: &Path,
    timeout: Duration,
    ctx: &ToolContext,
) -> std::io::Result<ProcessOutcome> {
    run_with_capture_limit(args, cwd, timeout, ctx, 16 * 1024 * 1024).await
}

async fn run_with_capture_limit(
    args: &[String],
    cwd: &Path,
    timeout: Duration,
    ctx: &ToolContext,
    capture_limit: usize,
) -> std::io::Result<ProcessOutcome> {
    ctx.execution_owner_ready().map_err(std::io::Error::other)?;
    let lease = ctx
        .register_execution_resource(
            "processGroup",
            None,
            serde_json::json!({"program":"git","workingDirectory":cwd,"scope":"retainedGitAnchor"}),
        )
        .await
        .map_err(std::io::Error::other)?;
    let args = args.to_vec();
    let cwd = cwd.to_path_buf();
    let ctx = ctx.clone();
    let owned_ctx = ctx.clone();
    let (sender, receiver) = oneshot::channel();
    if let Err(error) = ctx.spawn_owned_execution(Box::pin(async move {
        let mut sender = Some(sender);
        let outcome = supervise(
            args,
            cwd,
            timeout,
            &owned_ctx,
            lease.clone(),
            &mut sender,
            capture_limit,
        )
        .await;
        if outcome.is_err()
            && let Some(lease) = lease
        {
            let _ = owned_ctx
                .finish_execution_resource(lease, ExecutionResourceTerminal::Unconfirmed)
                .await;
        }
        if let Some(sender) = sender {
            let _ = sender.send(outcome);
        }
    })) {
        ctx.force_unconfirmed_execution_resources().await;
        return Err(std::io::Error::other(error));
    }
    receiver
        .await
        .map_err(|_| std::io::Error::other("Git supervisor lost its result"))?
}

#[expect(
    clippy::too_many_lines,
    reason = "The retained anchor and resource lease must share one cancellation state machine."
)]
async fn supervise(
    args: Vec<String>,
    cwd: std::path::PathBuf,
    timeout: Duration,
    ctx: &ToolContext,
    lease: Option<crate::tool::ExecutionResourceLease>,
    sender: &mut Option<oneshot::Sender<std::io::Result<ProcessOutcome>>>,
    capture_limit: usize,
) -> std::io::Result<ProcessOutcome> {
    let control = std::env::temp_dir().join(format!("zk-git-control-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir(&control).await?;
    let mut command = Command::new("/bin/sh");
    command
        .args(["-p", "-c", ANCHOR, "zk-git-anchor"])
        .arg(&control)
        .arg("git")
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for var in SENSITIVE_ENV_VARS {
        command.env_remove(var);
    }
    #[cfg(unix)]
    command.process_group(0);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = tokio::fs::remove_dir(&control).await;
            return Err(error);
        }
    };
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other("Git anchor has no pid"))?;
    if let Some(lease) = &lease
        && let Err(error) = ctx
            .bind_execution_resource_external(lease, pid.to_string())
            .await
    {
        if terminate(&mut child, pid).await {
            let _ = tokio::fs::remove_dir(&control).await;
        }
        return Err(std::io::Error::other(error));
    }
    let mut input = child.stdin.take();
    let admission = if ctx.cancel.is_cancelled() {
        Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "PROCESS_CANCELLED_BEFORE_START",
        ))
    } else if let Some(input) = input.as_mut() {
        input.write_all(b"start\n").await
    } else {
        Err(std::io::Error::other("Git start gate unavailable"))
    };
    if let Err(error) = admission {
        let confirmed = terminate(&mut child, pid).await;
        let cancelled = error.kind() == std::io::ErrorKind::Interrupted;
        if cancelled && let Some(lease) = lease {
            ctx.finish_execution_resource(
                lease,
                if confirmed {
                    ExecutionResourceTerminal::Released
                } else {
                    ExecutionResourceTerminal::Unconfirmed
                },
            )
            .await
            .map_err(std::io::Error::other)?;
        }
        let _ = tokio::fs::remove_dir(&control).await;
        if cancelled {
            return Ok(ProcessOutcome {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: SIGTERM_EXIT_CODE,
                timed_out: false,
                cancelled: true,
                termination_confirmed: confirmed,
                truncated: false,
            });
        }
        return Err(error);
    }
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let progress = ctx.clone();
    let mut out_task =
        tokio::spawn(
            async move { pump_with_utf8(stdout, Some(progress), true, capture_limit).await },
        );
    let mut err_task =
        tokio::spawn(async move { pump_with_utf8(stderr, None, true, capture_limit).await });
    let deadline = tokio::time::Instant::now() + timeout;
    let mut natural_exit = None;
    let mut uncertain = false;
    let mut cancelled = false;
    let mut timed_out = false;
    let (exit_code, confirmed) = loop {
        if natural_exit.is_none()
            && let Ok(record) = tokio::fs::read_to_string(control.join("exit")).await
            && record.ends_with('\n')
            && record.len() <= 4
        {
            natural_exit = record
                .trim()
                .parse::<i32>()
                .ok()
                .filter(|code| (0..=255).contains(code));
        }
        if let Some(code) = natural_exit {
            match group_has_writers(pid, &mut child).await {
                Ok(false) => {
                    if let Some(mut input) = input.take() {
                        input.write_all(b"release\n").await?;
                    }
                    let ended = tokio::time::timeout(KILL_CONFIRM_GRACE, child.wait())
                        .await
                        .is_ok_and(|result| result.is_ok());
                    break (code, ended && !uncertain);
                }
                Ok(true) => {}
                Err(_) => break (-1, false),
            }
            if (tokio::time::Instant::now() >= deadline || ctx.cancel.is_cancelled())
                && sender.is_some()
            {
                uncertain = true;
                if let Some(lease) = &lease {
                    let _ = ctx
                        .finish_execution_resource(
                            lease.clone(),
                            ExecutionResourceTerminal::Unconfirmed,
                        )
                        .await;
                }
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err(std::io::Error::other("GIT_SCOPE_UNCONFIRMED: Git exited but hook termination is not confirmed; worktree retained")));
                }
            }
        } else if ctx.cancel.is_cancelled() || tokio::time::Instant::now() >= deadline {
            // Git may have exited between its wait() and publishing the exit
            // record. Never infer hook cancellation authority from the anchor.
            let Ok(record) = tokio::fs::read_to_string(control.join("pid")).await else {
                break (-1, false);
            };
            let Ok(foreground) = record.trim().parse::<u32>() else {
                break (-1, false);
            };
            let observed = tokio::time::timeout(
                Duration::from_secs(2),
                Command::new("ps")
                    .args(["-p", &foreground.to_string(), "-o", "pgid=,stat="])
                    .kill_on_drop(true)
                    .output(),
            )
            .await;
            let Ok(Ok(observed)) = observed else {
                break (-1, false);
            };
            let status = String::from_utf8_lossy(&observed.stdout);
            let mut fields = status.split_whitespace();
            let group = fields.next().and_then(|field| field.parse::<u32>().ok());
            let state = fields.next().unwrap_or("");
            if !observed.status.success() || group != Some(pid) || state.starts_with('Z') {
                natural_exit = Some(-1);
                uncertain = true;
                continue;
            }
            cancelled = ctx.cancel.is_cancelled();
            timed_out = !cancelled;
            break (
                if cancelled {
                    SIGTERM_EXIT_CODE
                } else {
                    TIMEOUT_EXIT_CODE
                },
                terminate(&mut child, pid).await,
            );
        } else if child.try_wait()?.is_some() {
            break (-1, false);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    // Natural Git exit never authorizes closing a pipe still owned by a hook.
    // Keep bounded pumps alive even when descendants changed their process group.
    let mut confirmed = confirmed;
    let joined = async { tokio::join!(&mut out_task, &mut err_task) };
    tokio::pin!(joined);
    let (out, err) = if let Ok(streams) =
        tokio::time::timeout(super::PIPE_RECLAIM_GRACE, &mut joined).await
    {
        streams
    } else {
        confirmed = false;
        if let Some(lease) = &lease {
            let _ = ctx
                .finish_execution_resource(lease.clone(), ExecutionResourceTerminal::Unconfirmed)
                .await;
        }
        if let Some(sender) = sender.take() {
            let _ = sender.send(Err(std::io::Error::other(
                "GIT_SCOPE_UNCONFIRMED: hook output remains supervised",
            )));
        }
        joined.await
    };
    let (stdout, out_truncated) = out.unwrap_or_else(|_| ("Git stdout reader failed".into(), true));
    let (stderr, err_truncated) = err.unwrap_or_else(|_| ("Git stderr reader failed".into(), true));
    if let Some(lease) = lease {
        let terminal = if confirmed {
            ExecutionResourceTerminal::Released
        } else {
            ExecutionResourceTerminal::Unconfirmed
        };
        ctx.finish_execution_resource(lease, terminal)
            .await
            .map_err(std::io::Error::other)?;
    }
    let _ = tokio::fs::remove_file(control.join("exit")).await;
    let _ = tokio::fs::remove_file(control.join("pid")).await;
    let _ = tokio::fs::remove_dir(control).await;
    if !confirmed {
        return Err(std::io::Error::other(
            "GIT_SCOPE_UNCONFIRMED: process cleanup could not be proven",
        ));
    }
    Ok(ProcessOutcome {
        stdout,
        stderr,
        exit_code,
        timed_out,
        cancelled,
        truncated: out_truncated || err_truncated,
        termination_confirmed: confirmed,
    })
}

async fn group_has_writers(pid: u32, anchor: &mut Child) -> std::io::Result<bool> {
    if anchor.try_wait()?.is_some() {
        return Err(std::io::Error::other("Git anchor exited prematurely"));
    }
    let mut ps = Command::new("ps");
    ps.args(["-axo", "pid=,pgid=,stat="]).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(2), ps.output())
        .await
        .map_err(|_| std::io::Error::other("Git scope inspection timeout"))??;
    if !output.status.success() || output.stdout.len() > 4 * 1024 * 1024 {
        return Err(std::io::Error::other("Git scope inspection incomplete"));
    }
    let mut anchor_seen = false;
    let mut writers = false;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let values = line.split_whitespace().collect::<Vec<_>>();
        if values.len() != 3 {
            return Err(std::io::Error::other("Invalid Git process table"));
        }
        let row_pid = values[0].parse::<u32>().map_err(std::io::Error::other)?;
        let group = values[1].parse::<u32>().map_err(std::io::Error::other)?;
        if group != pid {
            continue;
        }
        if row_pid == pid && !values[2].starts_with('Z') {
            anchor_seen = true;
        }
        if row_pid != pid && !values[2].starts_with('Z') {
            writers = true;
        }
    }
    if !anchor_seen || anchor.try_wait()?.is_some() {
        return Err(std::io::Error::other("Git anchor identity unavailable"));
    }
    Ok(writers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn repo() -> std::path::PathBuf {
        let repo = std::env::temp_dir().join(format!("zk-git-scope-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&repo).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.name", "Test"],
            vec!["config", "user.email", "test@example.invalid"],
        ] {
            assert!(
                std::process::Command::new("git")
                    .current_dir(&repo)
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        repo
    }
    fn ctx(path: &Path) -> ToolContext {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(tokio_util::sync::CancellationToken::new(), tx).with_working_dir(path)
    }
    fn hook(repo: &Path, body: &str) {
        let file = repo.join(".git/hooks/post-commit");
        std::fs::write(&file, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    #[tokio::test]
    async fn invalid_utf8_git_output_is_never_authoritative_complete_output() {
        let repo = repo();
        let output = run_git_program(
            &[
                "-c".into(),
                "alias.invalid=!printf '\\377'".into(),
                "invalid".into(),
            ],
            &repo,
            Duration::from_secs(5),
            &ctx(&repo),
        )
        .await
        .unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(
            output.truncated,
            "lossy UTF-8 must not be accepted by Git callers"
        );
    }
    #[tokio::test]
    async fn normal_git_exit_waits_for_background_hook_without_killing_it() {
        let repo = repo();
        hook(
            &repo,
            "(sleep 0.15; printf done > hook-finished) >/dev/null 2>&1 &",
        );
        let output = run_git_program(
            &[
                "commit".into(),
                "--allow-empty".into(),
                "-m".into(),
                "test".into(),
            ],
            &repo,
            Duration::from_secs(5),
            &ctx(&repo),
        )
        .await
        .unwrap();
        assert_eq!(output.exit_code, 0);
        assert_eq!(
            std::fs::read_to_string(repo.join("hook-finished")).unwrap(),
            "done"
        );
    }
    #[tokio::test]
    async fn late_natural_hook_reports_uncertain_but_survives_cancellation() {
        let repo = repo();
        let file = repo.join(".git/hooks/post-commit");
        // Publish Git's PID before forking, then keep the child in the same
        // group until the test observes the uncertainty acknowledgement.
        std::fs::write(&file,"#!/usr/bin/env python3\nimport os,time\nopen('git-parent','w').write(str(os.getppid()))\nif os.fork()==0:\n os.close(0);os.close(1);os.close(2)\n end=time.monotonic()+30\n while not os.path.exists('release-hook') and time.monotonic()<end: time.sleep(.02)\n open('hook-finished','w').write('done');os._exit(0)\nos._exit(0)\n").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
        let context = ctx(&repo);
        let cancel = context.cancel.clone();
        let running_repo = repo.clone();
        let call = tokio::spawn(async move {
            run_git_program(
                &[
                    "commit".into(),
                    "--allow-empty".into(),
                    "-m".into(),
                    "test".into(),
                ],
                &running_repo,
                Duration::from_secs(30),
                &context,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(pid) = std::fs::read_to_string(repo.join("git-parent")) {
                    let status = Command::new("ps")
                        .args(["-p", pid.trim(), "-o", "stat="])
                        .output()
                        .await
                        .unwrap();
                    if !status.status.success() {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        let result = call.await.unwrap();
        std::fs::write(repo.join("release-hook"), "release").unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("GIT_SCOPE_UNCONFIRMED")
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while !repo.join("hook-finished").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn cancellation_of_running_git_terminates_the_owned_process_group() {
        let repo = repo();
        let hookpath = repo.join(".git/hooks/pre-commit");
        std::fs::write(&hookpath,"#!/bin/sh\nprintf entered > hook-entered\nsleep 20\nprintf unexpected > hook-finished\n").unwrap();
        std::fs::set_permissions(&hookpath, std::fs::Permissions::from_mode(0o755)).unwrap();
        let context = ctx(&repo);
        let cancel = context.cancel.clone();
        let path = repo.clone();
        let startup_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut call = tokio::spawn(async move {
            run_git_program(
                &[
                    "commit".into(),
                    "--allow-empty".into(),
                    "-m".into(),
                    "test".into(),
                ],
                &path,
                Duration::from_secs(30),
                &context,
            )
            .await
        });
        // Startup latency is not the cancellation guarantee. Observe the real
        // hook handshake within the operation's existing total deadline, and
        // diagnose an early Git failure without abandoning a live process.
        let (started, finished) = tokio::select! {
            () = async {
                while !repo.join("hook-entered").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } => (true, None),
            result = &mut call => (false, Some(result)),
            () = tokio::time::sleep_until(startup_deadline) => (false, None),
        };
        // Cleanup also runs before reporting a failed startup assertion.
        cancel.cancel();
        let cleanup = if let Some(finished) = finished {
            Ok(finished)
        } else {
            tokio::time::timeout(Duration::from_secs(10), call).await
        };
        assert!(
            started,
            "Git ended or reached its total deadline before the hook handshake; cleanup: {cleanup:?}"
        );
        let output = cleanup
            .expect("owned process group cleanup exceeded ten seconds")
            .expect("Git supervisor task panicked")
            .expect("Git cancellation must confirm process group cleanup");
        assert!(output.cancelled, "{output:?}");
        assert!(output.termination_confirmed, "{output:?}");
        assert!(!repo.join("hook-finished").exists());
    }
}
