//! OS read/write/network boundary for language servers and every child process.
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

fn quoted(path: &Path) -> Result<String, String> {
    let value = path.to_str().ok_or("LSP_SANDBOX_PATH_INVALID")?;
    if value.chars().any(char::is_control) {
        return Err("LSP_SANDBOX_PATH_INVALID".into());
    }
    Ok(format!(
        "\"{}\"",
        value.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

/// Extra dependency access comes only from explicit host configuration, never tool JSON.
pub fn authorized_dependencies() -> Result<Vec<PathBuf>, String> {
    let Some(value) = std::env::var_os("ZK_LSP_READ_ROOTS") else {
        return Ok(Vec::new());
    };
    let roots: Vec<String> = serde_json::from_str(value.to_str().ok_or("LSP_READ_ROOTS_INVALID")?)
        .map_err(|_| "LSP_READ_ROOTS_INVALID")?;
    if roots.len() > 32 {
        return Err("LSP_READ_ROOTS_INVALID".into());
    }
    roots
        .into_iter()
        .map(|path| {
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err("LSP_READ_ROOTS_MUST_BE_ABSOLUTE".into());
            }
            path.canonicalize()
                .map_err(|_| "LSP_READ_ROOTS_UNAVAILABLE".into())
        })
        .collect()
}

pub async fn profile(
    workspace: &Path,
    bundle: &Path,
    rust: &Path,
    state: &Path,
) -> Result<PathBuf, String> {
    if !cfg!(target_os = "macos") || !Path::new("/usr/bin/sandbox-exec").is_file() {
        return Err("LSP_PLATFORM_SANDBOX_UNAVAILABLE".into());
    }
    let mut readable = vec![
        workspace.to_owned(),
        bundle.to_owned(),
        rust.to_owned(),
        state.to_owned(),
    ];
    readable.extend(authorized_dependencies()?);
    // Operating-system runtime data is read-only. No home-wide, network or
    // project write permission is granted to compiler helpers or plugins.
    readable.extend(
        [
            "/System",
            "/usr",
            "/bin",
            "/sbin",
            "/Library/Developer/CommandLineTools",
            "/Library/Apple",
            "/private/etc",
            "/private/var/db/timezone",
        ]
        .into_iter()
        .map(PathBuf::from),
    );
    let mut profile = String::from(
        "(version 1)\n(deny default)\n(allow process*)\n(allow signal (target same-sandbox))\n(allow sysctl-read)\n(allow file-read-metadata)\n(allow file-read-data (literal \"/\"))\n(allow file-read* (literal \"/dev/null\") (literal \"/dev/urandom\") (literal \"/dev/random\"))\n",
    );
    for path in readable {
        let _ = writeln!(profile, "(allow file-read* (subpath {}))", quoted(&path)?);
    }
    let _ = writeln!(
        profile,
        "(allow file-write* (subpath {}) (literal \"/dev/null\"))",
        quoted(state)?
    );
    let path = state.join("sandbox.sb");
    tokio::fs::write(&path, profile)
        .await
        .map_err(|_| "LSP_SANDBOX_PROFILE_FAILED")?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_paths_cannot_inject_rules() {
        assert_eq!(quoted(Path::new("/a/\"\\b")).unwrap(), "\"/a/\\\"\\\\b\"");
        assert!(quoted(Path::new("/a\n(allow network*)")).is_err());
    }
}

#[cfg(all(test, target_os = "macos"))]
mod native_tests {
    use super::*;

    /// Validate the actual OS boundary rather than only asserting profile text.
    #[tokio::test]
    async fn sandbox_denies_outside_content_workspace_writes_and_network() {
        let root = std::env::temp_dir().join(format!("zk-lsp-sandbox-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let workspace = root.join("workspace");
        let state = root.join("state");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(&state).unwrap();
        std::fs::write(root.join("secret"), "outside-content").unwrap();
        std::fs::write(workspace.join("source"), "authorized-source").unwrap();
        let policy = profile(
            &workspace,
            Path::new("/usr/bin"),
            Path::new("/usr/bin"),
            &state,
        )
        .await
        .unwrap();
        let run = |script: String| {
            let mut cmd = tokio::process::Command::new("/usr/bin/sandbox-exec");
            cmd.current_dir(&workspace)
                .arg("-f")
                .arg(&policy)
                .arg("/bin/sh")
                .arg("-c")
                .arg(script);
            cmd
        };
        let output = run(format!("cat '{}'", workspace.join("source").display()))
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"authorized-source");
        assert!(
            !run(format!("cat '{}'", root.join("secret").display()))
                .status()
                .await
                .unwrap()
                .success()
        );
        assert!(
            !run(format!(
                "echo denied > '{}'",
                workspace.join("modified").display()
            ))
            .status()
            .await
            .unwrap()
            .success()
        );
        assert!(!workspace.join("modified").exists());
        assert!(
            run(format!(
                "echo state > '{}'",
                state.join("allowed").display()
            ))
            .status()
            .await
            .unwrap()
            .success()
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let output = tokio::process::Command::new("/usr/bin/sandbox-exec")
            .arg("-f")
            .arg(&policy)
            .arg("/usr/bin/curl")
            .args([
                "--noproxy",
                "*",
                "--max-time",
                "2",
                "--silent",
                "--show-error",
            ])
            .arg(format!("http://{}/", listener.local_addr().unwrap()))
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(
            listener.accept().is_err(),
            "sandboxed process reached a network listener"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
