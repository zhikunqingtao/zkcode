//! Resources owned by one verification invocation, including cancellation cleanup.

use crate::python::client::{Correlation, PythonClient};
use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, Command},
};
use zk_tools::{ExecutionResourceLease, ExecutionResourceTerminal, ToolContext};

pub(super) struct JourneyResources {
    ctx: ToolContext,
    recording_db: Option<zk_db::Db>,
    client: Arc<PythonClient>,
    pub(super) browser_id: String,
    pub(super) recording_identity: Option<Value>,
    pub(super) recording_manifest: Option<Value>,
    pub(super) recording_error: Option<String>,
    pub(super) recording_resource_id: Option<String>,
    browser: Option<ExecutionResourceLease>,
    browser_reserved: bool,
    preview: Option<(Child, i32, Option<ExecutionResourceLease>)>,
}

impl JourneyResources {
    pub(super) fn new(ctx: ToolContext, client: Arc<PythonClient>) -> Self {
        Self {
            ctx,
            recording_db: None,
            client,
            browser_id: format!("rv-{}", uuid::Uuid::new_v4()),
            browser: None,
            recording_identity: None,
            recording_manifest: None,
            recording_error: None,
            recording_resource_id: None,
            browser_reserved: false,
            preview: None,
        }
    }

    pub(super) fn with_recording_store(mut self, db: zk_db::Db) -> Self {
        self.recording_db = Some(db);
        self
    }

    pub(super) async fn reserve_browser(&mut self, record: bool) -> Result<(), String> {
        let mut metadata = json!({"kind":"browserSession", "sidecarSessionId":self.browser_id});
        if record {
            let owner = self
                .ctx
                .execution_resource_owner()
                .ok_or("RECORDING_OWNER_REQUIRED")?;
            let identity = json!({"batch_id":uuid::Uuid::new_v4().to_string(),"session_id":self.ctx.session_id().ok_or("RECORDING_OWNER_REQUIRED")?,"run_id":owner.run_id,"invocation_id":owner.invocation_id});
            metadata["recordingFinalization"] =
                json!({"version":1,"phase":"reserved","identity":identity});
            self.recording_identity = Some(identity);
        }
        self.browser = self
            .ctx
            .register_execution_resource("stream", Some(self.browser_id.clone()), metadata)
            .await?;
        self.recording_resource_id = self.browser.as_ref().map(|lease| lease.resource_id.clone());
        self.browser_reserved = true;
        Ok(())
    }

    pub(super) async fn start_preview(&mut self, input: &Value) -> Result<String, String> {
        let supplied = input.get("base_url").and_then(Value::as_str);
        if supplied.is_some() && input.get("start_command").is_none() {
            return Ok(supplied.unwrap_or_default().to_owned());
        }
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").map_err(|_| "PREVIEW_PORT_UNAVAILABLE")?;
        let port = listener
            .local_addr()
            .map_err(|_| "PREVIEW_PORT_UNAVAILABLE")?
            .port();
        drop(listener);
        let base_url = supplied.map_or_else(|| format!("http://127.0.0.1:{port}"), str::to_owned);
        let url = reqwest::Url::parse(&base_url).map_err(|_| "VERIFY_BASE_URL_INVALID")?;
        if !matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1")) {
            return Err("PREVIEW_REQUIRES_LOOPBACK_URL".into());
        }
        let port = url
            .port_or_known_default()
            .ok_or("PREVIEW_PORT_UNAVAILABLE")?;
        // Never reuse or terminate a listener belonging to somebody else.
        if tokio::net::TcpStream::connect((url.host_str().unwrap_or("127.0.0.1"), port))
            .await
            .is_ok()
        {
            return Err("PREVIEW_PORT_IN_USE: provide base_url without start_command to verify an existing service".into());
        }
        let command = match input.get("start_command").and_then(Value::as_str) {
            Some(command) if !command.trim().is_empty() => command.to_owned(),
            Some(_) => return Err("PREVIEW_COMMAND_EMPTY".into()),
            None => detect_command(self.ctx.working_dir(), port)?,
        };
        let lease = self
            .ctx
            .register_execution_resource(
                "processGroup",
                None,
                json!({"kind":"verificationPreview", "baseUrl":base_url}),
            )
            .await?;
        let child = Command::new("/bin/sh")
            .args(["-p", "-c", "ulimit -c 0; IFS= read -r gate || exit 125; [ \"$gate\" = zk-start ] || exit 125; exec /bin/sh -p -c \"$1\"", "zk-preview", &command])
            .current_dir(self.ctx.working_dir())
            .env("PORT", port.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true)
            .spawn();
        let Ok(mut child) = child else {
            if let Some(lease) = lease {
                let _ = self
                    .ctx
                    .finish_execution_resource(lease, ExecutionResourceTerminal::Released)
                    .await;
            }
            return Err("PREVIEW_START_FAILED".into());
        };
        let pid = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .ok_or("PREVIEW_PROCESS_ID_MISSING")?;
        let mut start_gate = child.stdin.take().ok_or("PREVIEW_START_GATE_MISSING")?;
        self.preview = Some((child, pid, lease.clone()));
        if let Some(lease) = lease {
            self.ctx
                .bind_execution_resource_external(&lease, pid.to_string())
                .await?;
        }
        if self.ctx.cancel.is_cancelled() {
            return Err("VERIFY_CANCELLED".into());
        }
        start_gate
            .write_all(b"zk-start\n")
            .await
            .map_err(|_| "PREVIEW_START_GATE_FAILED")?;
        drop(start_gate);
        self.wait_until_ready(&base_url).await?;
        Ok(base_url)
    }

    async fn wait_until_ready(&mut self, base_url: &str) -> Result<(), String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|_| "PREVIEW_CLIENT_FAILED")?;
        let deadline = tokio::time::Instant::now() + Duration::from_mins(2);
        loop {
            if self.ctx.cancel.is_cancelled() {
                return Err("VERIFY_CANCELLED".into());
            }
            if self
                .preview
                .as_mut()
                .and_then(|(child, _, _)| child.try_wait().ok().flatten())
                .is_some()
            {
                return Err("PREVIEW_EXITED_BEFORE_READY".into());
            }
            if http
                .get(base_url)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("PREVIEW_START_TIMEOUT".into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub(super) async fn close(&mut self) {
        if std::mem::take(&mut self.browser_reserved) {
            let response: Option<Value> = self
                .client
                .call_if_available_with_timeout(
                    "BROWSER_AUTOMATION",
                    "/api/browser/close_session",
                    &json!({"session_id":self.browser_id,"recording":self.recording_identity}),
                    &Correlation::for_session(self.ctx.session_id()),
                    Duration::from_secs(7),
                )
                .await;
            self.recording_manifest = response
                .as_ref()
                .and_then(|value| value.get("data"))
                .and_then(|data| data.get("recording_manifest"))
                .cloned();
            let released = response
                .as_ref()
                .is_some_and(|value| value.get("success") == Some(&Value::Bool(true)));
            if let Some(lease) = self.browser.take() {
                let finished = self
                    .ctx
                    .finish_execution_resource(
                        lease,
                        if released {
                            ExecutionResourceTerminal::Released
                        } else {
                            ExecutionResourceTerminal::Unconfirmed
                        },
                    )
                    .await;
                if finished.is_err() && self.recording_identity.is_some() {
                    self.recording_error = Some("RECORDING_FINALIZATION_UNCONFIRMED".into());
                }
            }
            if self.recording_identity.is_some() {
                let persisted = match (
                    &self.recording_db,
                    &self.recording_resource_id,
                    response.as_ref(),
                ) {
                    (Some(db), Some(resource), Some(response)) if released => {
                        if let Some(manifest) = &self.recording_manifest {
                            db.seal_browser_recording(resource, manifest.clone(), json!([]))
                                .await
                        } else if let Some(proof) = response["data"].get("recording_finalization") {
                            db.acknowledge_uncreated_browser_recording(resource, proof.clone())
                                .await
                        } else {
                            Err(zk_db::DbError::Invalid("RECORDING_SEAL_UNCONFIRMED".into()))
                        }
                    }
                    _ => Err(zk_db::DbError::Invalid(
                        "RECORDING_CLEANUP_UNCONFIRMED".into(),
                    )),
                };
                if let Err(error) = persisted {
                    tracing::warn!(%error, "recording finalization retained for reconciliation");
                    self.recording_error = Some("RECORDING_FINALIZATION_UNCONFIRMED".into());
                }
            }
        }

        if let Some((mut child, pid, lease)) = self.preview.take() {
            let _ = killpg(Pid::from_raw(pid), Signal::SIGTERM);
            if tokio::time::timeout(Duration::from_secs(3), child.wait())
                .await
                .is_err()
            {
                let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
                let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
            }
            // The group can outlive the leader; do not report release based only on wait().
            if killpg(Pid::from_raw(pid), None).is_ok() {
                let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
            }
            let released = killpg(Pid::from_raw(pid), None) == Err(Errno::ESRCH);
            if let Some(lease) = lease {
                let _ = self
                    .ctx
                    .finish_execution_resource(
                        lease,
                        if released {
                            ExecutionResourceTerminal::Released
                        } else {
                            ExecutionResourceTerminal::Unconfirmed
                        },
                    )
                    .await;
            }
        }
    }
}

impl Drop for JourneyResources {
    fn drop(&mut self) {
        if !self.browser_reserved && self.preview.is_none() {
            return;
        }
        let mut remaining = Self {
            ctx: self.ctx.clone(),
            recording_db: self.recording_db.clone(),
            client: Arc::clone(&self.client),
            browser_id: self.browser_id.clone(),
            recording_identity: self.recording_identity.clone(),
            recording_manifest: self.recording_manifest.clone(),
            recording_error: self.recording_error.clone(),
            recording_resource_id: self.recording_resource_id.clone(),
            browser: self.browser.take(),
            browser_reserved: std::mem::take(&mut self.browser_reserved),
            preview: self.preview.take(),
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                remaining.close().await;
            });
        } else {
            if let Some((_, pid, _)) = remaining.preview.take() {
                let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
            }
            // No executor remains; the pending lease is recovered at restart.
            remaining.browser.take();
            remaining.browser_reserved = false;
        }
    }
}

fn detect_command(root: &Path, port: u16) -> Result<String, String> {
    let package = std::fs::read(root.join("package.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    if let Some(package) = package {
        let scripts = package
            .get("scripts")
            .ok_or("PREVIEW_START_COMMAND_REQUIRED")?;
        let script = if scripts.get("dev").is_some() {
            "dev"
        } else if scripts.get("start").is_some() {
            "start"
        } else {
            return Err("PREVIEW_START_COMMAND_REQUIRED".into());
        };
        let command = scripts
            .get(script)
            .and_then(Value::as_str)
            .unwrap_or_default();
        if command.contains("vite") {
            return Ok(format!(
                "npm run {script} -- --host 127.0.0.1 --port {port} --strictPort"
            ));
        }
        if command.contains("next") {
            return Ok(format!(
                "npm run {script} -- --hostname 127.0.0.1 --port {port}"
            ));
        }
        if command.trim() == "react-scripts start" {
            // CRA reads its bind address and port from the environment and must
            // never open another browser or interactively select another port.
            return Ok(format!(
                "HOST=127.0.0.1 PORT={port} BROWSER=none CI=true npm run {script}"
            ));
        }
        return Err(
            "PREVIEW_START_COMMAND_REQUIRED: only Vite/Next/CRA scripts are auto-started".into(),
        );
    }
    if root.join("index.html").is_file() {
        return Ok(format!("python3 -m http.server {port} --bind 127.0.0.1"));
    }
    Err("PREVIEW_START_COMMAND_REQUIRED".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preview_detection_is_bounded_and_explicit() {
        let root = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&root).unwrap();
        assert!(detect_command(&root, 4321).is_err());
        std::fs::write(root.join("index.html"), "hello").unwrap();
        assert!(
            detect_command(&root, 4321)
                .unwrap()
                .contains("--bind 127.0.0.1")
        );
        std::fs::write(root.join("package.json"), r#"{"scripts":{"dev":"vite"}}"#).unwrap();
        assert!(
            detect_command(&root, 4321)
                .unwrap()
                .contains("--strictPort")
        );
        std::fs::write(
            root.join("package.json"),
            r#"{"scripts":{"start":"react-scripts start"}}"#,
        )
        .unwrap();
        assert_eq!(
            detect_command(&root, 4321).unwrap(),
            "HOST=127.0.0.1 PORT=4321 BROWSER=none CI=true npm run start"
        );
        std::fs::write(
            root.join("package.json"),
            r#"{"scripts":{"start":"echo react-scripts start"}}"#,
        )
        .unwrap();
        assert!(detect_command(&root, 4321).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cra_launch_contract_keeps_the_owned_loopback_port() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
        std::fs::write(root.join("index.html"), "CRA protocol fixture").unwrap();
        std::fs::write(
            root.join("package.json"),
            r#"{"scripts":{"start":"react-scripts start"}}"#,
        )
        .unwrap();
        let executable = root.join("node_modules/.bin/react-scripts");
        // Exercise npm's real script launch and CRA's environment contract; this
        // fixture does not claim to test React's compiler or downloaded packages.
        std::fs::write(
            &executable,
            r#"#!/bin/sh
[ "$1" = start ] && [ "$HOST" = 127.0.0.1 ] && [ "$BROWSER" = none ] && [ "$CI" = true ] || exit 41
exec python3 -m http.server "$PORT" --bind "$HOST"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ToolContext::new(tokio_util::sync::CancellationToken::new(), progress)
            .with_working_dir(&root);
        let mut owner = JourneyResources::new(
            ctx,
            Arc::new(PythonClient::new("/tmp/absent-preview-sidecar.sock")),
        );
        let url = owner.start_preview(&json!({})).await.unwrap();
        assert!(
            reqwest::get(&url)
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
                .contains("CRA protocol fixture")
        );
        let port = reqwest::Url::parse(&url).unwrap().port().unwrap();
        owner.close().await;
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn managed_preview_releases_its_process_and_port() {
        let root = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("index.html"), "preview").unwrap();
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ToolContext::new(tokio_util::sync::CancellationToken::new(), progress)
            .with_working_dir(&root);
        let mut owner = JourneyResources::new(
            ctx,
            Arc::new(PythonClient::new("/tmp/absent-preview-sidecar.sock")),
        );
        let url = owner.start_preview(&json!({})).await.unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        let port = parsed.port().unwrap();
        assert!(reqwest::get(&url).await.unwrap().status().is_success());
        owner.close().await;
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn preview_bind_failure_never_executes_the_requested_script() {
        struct RejectBinding;
        impl zk_tools::ExecutionResourceObserver for RejectBinding {
            fn register(
                &self,
                _: zk_tools::ExecutionResourceOwner,
                allocation: zk_tools::ExecutionResourceAllocation,
            ) -> futures::future::BoxFuture<'static, Result<ExecutionResourceLease, String>>
            {
                Box::pin(async move {
                    Ok(ExecutionResourceLease {
                        resource_id: allocation.resource_id,
                    })
                })
            }
            fn bind_external(
                &self,
                _: ExecutionResourceLease,
                _: String,
            ) -> futures::future::BoxFuture<'static, Result<(), String>> {
                Box::pin(async { Err("injected bind failure".into()) })
            }
            fn finish(
                &self,
                _: ExecutionResourceLease,
                _: ExecutionResourceTerminal,
            ) -> futures::future::BoxFuture<'static, Result<(), String>> {
                Box::pin(async { Ok(()) })
            }
        }
        let root = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&root).unwrap();
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ToolContext::new(tokio_util::sync::CancellationToken::new(), progress)
            .with_working_dir(&root)
            .with_execution_resources(
                zk_tools::ExecutionResourceOwner {
                    task_id: "t".into(),
                    run_id: "r".into(),
                    invocation_id: "i".into(),
                },
                Arc::new(RejectBinding),
            );
        let mut owner = JourneyResources::new(
            ctx,
            Arc::new(PythonClient::new("/tmp/absent-preview-sidecar.sock")),
        );
        assert!(
            owner
                .start_preview(&json!({"start_command":"touch should-not-exist; sleep 60"}))
                .await
                .is_err()
        );
        let pid = owner.preview.as_ref().unwrap().1;
        owner.close().await;
        assert!(!root.join("should-not-exist").exists());
        assert_eq!(killpg(Pid::from_raw(pid), None), Err(Errno::ESRCH));
        std::fs::remove_dir(root).unwrap();
    }
}
