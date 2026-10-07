//! Local STDIO adapter. The server owns every Task, authorization and physical process.
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
    task::JoinSet,
};

const MAX_FRAME: usize = 1024 * 1024;
type Output = Arc<Mutex<tokio::io::Stdout>>;

/// Run `zk-server mcp-stdio --project-id ID [--server-url http://127.0.0.1:8082]`.
/// No credentials, project filesystem path or execution policies come from MCP messages.
/// # Errors
/// Returns a redacted error if local setup, transport or cleanup fails.
#[allow(clippy::too_many_lines)] // One bounded STDIO pump owns initialization, pending requests and EOF cleanup.
pub async fn run(arguments: &[OsString]) -> Result<(), String> {
    let mut project = None;
    let mut requested_capabilities = None;
    let port = std::env::var("ZK_PORT")
        .unwrap_or_else(|_| "8082".into())
        .parse::<u16>()
        .map_err(|_| "Invalid ZK_PORT")?;
    let mut endpoint = format!("http://127.0.0.1:{port}");
    let mut token_file = crate::access_token::default_token_path();
    let mut index = 0;
    while index < arguments.len() {
        let flag = arguments[index].to_str().ok_or("Invalid STDIO argument")?;
        let value = arguments
            .get(index + 1)
            .and_then(|v| v.to_str())
            .ok_or("STDIO argument requires a value")?;
        match flag {
            "--project-id" if project.is_none() => project = Some(value.to_owned()),
            "--server-url" => endpoint = value.to_owned(),
            "--token-file" => token_file = value.into(),
            "--request-capabilities" if requested_capabilities.is_none() => {
                requested_capabilities = Some(parse_capabilities(value)?);
            }
            _ => {
                return Err("Usage: zk-server mcp-stdio --project-id ID [--server-url URL] [--request-capabilities write,process,network]".into());
            }
        }
        index += 2;
    }
    let project = project
        .filter(|id| !id.is_empty())
        .ok_or("--project-id is required")?;
    let url = url::Url::parse(&endpoint).map_err(|_| "Invalid local server URL")?;
    if url.scheme() != "http"
        || !matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "::1"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err("STDIO server URL must be a literal loopback HTTP origin".into());
    }
    let origin = url.as_str().trim_end_matches('/');
    let token = {
        use std::io::Read;
        let mut bytes = Vec::new();
        zk_tools::safe_file::open_bound_regular(&token_file)
            .map_err(|_| "Local access token unavailable; start zk-server first")?
            .take(4097)
            .read_to_end(&mut bytes)
            .map_err(|_| "Local access token read failed")?;
        if bytes.len() > 4096 {
            return Err("Local access token file exceeds limit".into());
        }
        String::from_utf8(bytes).map_err(|_| "Local access token encoding invalid")?
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(610))
        .build()
        .map_err(|_| "HTTP client unavailable")?;
    let response = client
        .post(format!("{origin}/api/mcp/contexts"))
        .bearer_auth(token.trim())
        .json(&json!({"projectId":project}))
        .send()
        .await
        .map_err(|_| "Local MCP context creation failed")?;
    if response.status() != reqwest::StatusCode::CREATED {
        return Err("Local MCP context was denied or unavailable".into());
    }
    let context = read_json(response, MAX_FRAME).await?;
    let session = context["sessionId"]
        .as_str()
        .ok_or("Invalid MCP context response")?;
    let run = context["runId"]
        .as_str()
        .ok_or("Invalid MCP context response")?;
    let capability = context["contextToken"]
        .as_str()
        .ok_or("Invalid MCP context response")?;
    let headers = context_headers(token.trim(), session, run, capability)?;
    if let Some(requested) = requested_capabilities {
        let result = client
            .post(format!(
                "{origin}/api/mcp/contexts/{run}/capabilities/requests"
            ))
            .headers(headers.clone())
            .timeout(Duration::from_secs(30))
            .json(&requested)
            .send()
            .await;
        if !result.is_ok_and(|response| response.status() == reqwest::StatusCode::ACCEPTED) {
            let _ = client
                .delete(format!("{origin}/api/mcp/contexts/{run}"))
                .headers(headers.clone())
                .timeout(Duration::from_secs(5))
                .send()
                .await;
            return Err(
                "MCP capability request could not be created; no capabilities were granted".into(),
            );
        }
    }
    let capability_epoch = Arc::new(AtomicU64::new(0));
    let initialized = Arc::new(AtomicBool::new(false));
    let output = Arc::new(Mutex::new(tokio::io::stdout()));
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut pending = JoinSet::new();
    let mut transport_error = None;
    let mut frame = Vec::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    loop {
        let remaining = MAX_FRAME.saturating_add(1).saturating_sub(frame.len());
        let read = tokio::select! {
            result=async {(&mut reader).take(remaining as u64).read_until(b'\n',&mut frame).await}=>result,
            _=tokio::signal::ctrl_c()=>break,
            _=heartbeat.tick()=>{
                if pending.len() >= 20 { transport_error=Some("MCP connection saturated".into()); break; }
                let client=client.clone();let headers=headers.clone();let address=format!("{origin}/mcp");
                let output=output.clone();let epoch=capability_epoch.clone();let initialized=initialized.clone();
                pending.spawn(async move {
                    let response=client.post(address).headers(headers).timeout(Duration::from_secs(5)).json(&json!({"jsonrpc":"2.0","id":"zk-internal-heartbeat","method":"ping"})).send().await.map_err(|_|"MCP connection heartbeat failed".to_owned())?;
                    if !response.status().is_success(){return Err("MCP context expired".into());}
                    let value=read_json(response,MAX_FRAME).await?;
                    if value.get("error").is_some() {return Err("MCP context heartbeat rejected".into());}
                    let current=value["result"]["capabilityEpoch"].as_u64().ok_or("MCP heartbeat response invalid")?;
                    if initialized.load(Ordering::Acquire) && current>epoch.fetch_max(current,Ordering::AcqRel) {
                        write(&output,&json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"})).await?;
                    }
                    Ok(())
                });
                continue;
            },
            completed=pending.join_next(),if !pending.is_empty()=>{
                if matches!(completed,Some(Err(_) | Ok(Err(_)))) {transport_error=Some("MCP transport failed".to_owned());break;}
                continue;
            }
        };
        match read {
            Ok(0) => break,
            Err(_) => {
                transport_error = Some("MCP input failed".into());
                break;
            }
            _ => {}
        }
        if frame.len() > MAX_FRAME {
            transport_error = Some("MCP input exceeds 1 MiB".into());
            break;
        }
        let Ok(value) = serde_json::from_slice::<Value>(&std::mem::take(&mut frame)) else {
            if write(
                &output,
                &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}),
            )
            .await
            .is_err()
            {
                transport_error = Some("MCP output closed".into());
                break;
            }
            continue;
        };
        if value["jsonrpc"] == "2.0"
            && value["method"] == "notifications/initialized"
            && value.get("id").is_none()
        {
            initialized.store(true, Ordering::Release);
        }
        let id = value.get("id").cloned();
        if pending.len() >= 20
            || (pending.len() >= 16 && value["method"] != "notifications/cancelled")
        {
            if let Some(id)=id && write(&output,&json!({"jsonrpc":"2.0","id":id,"error":{"code":-32001,"message":"MCP concurrency limit"}})).await.is_err(){transport_error=Some("MCP output closed".into());break;}
            continue;
        }
        let client = client.clone();
        let headers = headers.clone();
        let address = format!("{origin}/mcp");
        let output = output.clone();
        pending.spawn(async move {
            let response = client
                .post(address)
                .headers(headers)
                .json(&value)
                .send()
                .await
                .map_err(|_| "MCP request transport failed".to_owned())?;
            if response.status() == reqwest::StatusCode::ACCEPTED && id.is_none() {
                return Ok(());
            }
            if !response.status().is_success() {
                return Err("MCP request denied or context expired".into());
            }
            let result = read_json(response, 8 * MAX_FRAME).await?;
            if id.is_some() {
                write(&output, &result).await?;
            }
            Ok::<(), String>(())
        });
    }
    // Dropping requests cancels their owned finalizers; explicit context cancellation
    // also reaches tools whose HTTP response has already disconnected.
    pending.abort_all();
    while pending.join_next().await.is_some() {}
    let close = client
        .delete(format!("{origin}/api/mcp/contexts/{run}"))
        .headers(headers.clone())
        .send()
        .await;
    if !close.is_ok_and(|response| response.status().is_success()) {
        return Err("MCP cleanup could not be requested; server deadline still applies".into());
    }
    // The adapter has the trusted local access token. Context credentials may
    // never be used on management routes, even when another credential is present.
    let mut management_headers = headers.clone();
    management_headers.remove("x-mcp-context-token");
    let stopped = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response = client
                .get(format!("{origin}/api/runs/{run}"))
                .headers(management_headers.clone())
                .send()
                .await
                .map_err(|_| "MCP cleanup status unavailable")?;
            let value = read_json(response, MAX_FRAME).await?;
            if matches!(
                value["status"].as_str(),
                Some("completed" | "succeeded" | "cancelled" | "failed" | "interrupted")
            ) {
                if !matches!(
                    value["cleanupStatus"].as_str(),
                    Some("confirmed" | "notRequired")
                ) {
                    return Err("MCP run ended with unconfirmed cleanup".into());
                }
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if !matches!(stopped, Ok(Ok(()))) {
        return Err("MCP cleanup remains unconfirmed; inspect the task in zkcode".into());
    }
    transport_error.map_or(Ok(()), Err)
}
fn parse_capabilities(value: &str) -> Result<Value, String> {
    let mut requested = json!({"write":false,"process":false,"network":false});
    for name in value.split(',') {
        if !matches!(name, "write" | "process" | "network") || requested[name] == true {
            return Err(
                "Capabilities must be unique comma-separated write,process,network values".into(),
            );
        }
        requested[name] = json!(true);
    }
    if requested["process"] == true && (requested["write"] != true || requested["network"] != true)
    {
        return Err("Native commands require explicit write,process,network candidates".into());
    }
    Ok(requested)
}
fn context_headers(
    token: &str,
    session: &str,
    run: &str,
    capability: &str,
) -> Result<reqwest::header::HeaderMap, String> {
    let mut headers = reqwest::header::HeaderMap::new();
    for (key, value) in [
        ("authorization", format!("Bearer {token}")),
        ("x-session-id", session.into()),
        ("x-run-id", run.into()),
        ("x-mcp-context-token", capability.into()),
    ] {
        let mut value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| "Invalid local MCP credential")?;
        value.set_sensitive(true);
        headers.insert(key, value);
    }
    Ok(headers)
}
async fn read_json(mut response: reqwest::Response, limit: usize) -> Result<Value, String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "Incomplete MCP HTTP response")?
    {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err("MCP response exceeds limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "Invalid MCP HTTP response".into())
}
async fn write(output: &Output, value: &Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| "MCP response encoding failed")?;
    bytes.push(b'\n');
    let mut stdout = output.lock().await;
    stdout
        .write_all(&bytes)
        .await
        .map_err(|_| "MCP output closed")?;
    stdout.flush().await.map_err(|_| "MCP output closed".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capabilities_are_requests_and_process_cannot_claim_to_be_isolated() {
        assert_eq!(
            parse_capabilities("write").unwrap(),
            json!({"write":true,"process":false,"network":false})
        );
        assert_eq!(
            parse_capabilities("write,process,network").unwrap(),
            json!({"write":true,"process":true,"network":true})
        );
        for input in [
            "",
            "process",
            "write,process",
            "write,write",
            "all",
            "write, network",
        ] {
            assert!(parse_capabilities(input).is_err(), "{input}");
        }
    }
}
