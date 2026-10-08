//! Browser journey verification tool backed by the Python Playwright capability.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use zk_authz::sensitive::SensitiveDataFilter;
use zk_db::Db;
use zk_tools::{
    ChildToolAccess, EVIDENCE_RECEIPT_SCHEMA_VERSION, EvidenceReceipt, EvidenceReceiptItem,
    EvidenceReceiptVerdict, Tool, ToolContext, ToolOutput,
};

use super::journey_resources::JourneyResources;
use super::{BROWSER_AUTOMATION, failure};
use crate::python::client::{Correlation, PythonClient};

/// Browser-semantic `VerifyJourney`; engineering compile/test checks live in
/// `VerifyPlanExecution` and `/api/verify/run-checks`.
pub struct BrowserVerifyJourneyTool {
    client: Arc<PythonClient>,
    db: Db,
}

impl BrowserVerifyJourneyTool {
    async fn capture_failure_snapshot(
        &self,
        response: &mut Option<Value>,
        browser_id: &str,
        correlation: &Correlation,
        ephemeral: bool,
    ) {
        if !response
            .as_ref()
            .is_some_and(|value| value["passed"] == false)
        {
            return;
        }
        let snapshot: Option<Value> = self
            .client
            .call_if_available_with_timeout(
                BROWSER_AUTOMATION,
                "/api/browser/snapshot-semantic",
                &json!({"session_id":browser_id, "include_screenshot":false,"strict_session":true,"ephemeral_content":ephemeral}),
                correlation,
                Duration::from_secs(2),
            )
            .await;
        if let Some(snapshot) = snapshot.filter(|value| value["success"] == true) {
            let filtered = SensitiveDataFilter::filter(&snapshot.to_string());
            if filtered.len() <= 64 * 1024
                && let Ok(snapshot) = serde_json::from_str::<Value>(&filtered)
                && let Some(response) = response.as_mut()
            {
                response["failure_snapshot"] = snapshot;
            }
        }
    }
    /// Build the browser journey bridge with the shared Python client.
    #[must_use]
    pub fn new(client: Arc<PythonClient>, db: Db) -> Self {
        Self { client, db }
    }
}

impl Tool for BrowserVerifyJourneyTool {
    fn produces_machine_evidence(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "VerifyJourney"
    }

    fn description(&self) -> &'static str {
        "Run a bounded browser or HTTP user journey through the Python sidecar and return \
         deterministic step evidence. Use VerifyPlanExecution for compile/test/lint checks."
    }

    fn child_access(&self) -> ChildToolAccess {
        ChildToolAccess::WriteGated
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "anyOf": [{"required":["steps"]}, {"required":["journey"]}],
            "properties": {
                "base_url": {"type":"string", "description":"HTTP(S) application base URL"},
                "steps": {
                    "type":"array", "minItems":1, "maxItems":50,
                    "items":{"type":"object"}
                },
                "journey": {"type":"array", "minItems":1, "maxItems":50, "items":{"type":"object"}},
                "start_command": {"type":"string"},
                "verification_mode": {"type":"string", "enum":["auto","browser","http_api"]},
                "record": {"oneOf":[{"type":"object"},{"type":"boolean"}]},
                "viewport": {
                    "type":"object",
                    "properties": {
                        "width":{"type":"integer","minimum":320,"maximum":4096},
                        "height":{"type":"integer","minimum":240,"maximum":4096}
                    }
                },
                "mode": {"type":"string","enum":["browser","http_api"]},
                "session_id": {"type":"string"},
                "claim": {
                    "type":"string",
                    "description":"Short acceptance claim this journey verifies"
                }
            }
        })
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(280)
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.run(input, ctx).await })
    }
}

impl BrowserVerifyJourneyTool {
    #[allow(clippy::too_many_lines)] // Keep the single resource lifetime visible around evidence persistence.
    async fn run(&self, input: Value, ctx: ToolContext) -> ToolOutput {
        let record_supplied = input.get("record").is_some();
        let mut body = match normalize_request(input) {
            Ok(body) => body,
            Err((code, message)) => return failure(code, message),
        };
        let Some(session_id) = ctx.session_id() else {
            return failure("VERIFY_CONTEXT_REQUIRED", "session context is required");
        };
        let Some(run_id) = ctx.run_id() else {
            return failure("VERIFY_CONTEXT_REQUIRED", "run context is required");
        };
        let ephemeral = match self.db.session_retention(session_id).await {
            Ok(retention) => retention == zk_db::content::ContentRetention::Ephemeral,
            Err(_) => {
                return failure(
                    "VERIFY_RETENTION_UNAVAILABLE",
                    "Session content policy unavailable",
                );
            }
        };
        if ephemeral {
            if !record_supplied {
                body["record"] = json!({"trace":false,"video":false,"har":false});
            }
            if body["record"]
                .as_object()
                .is_some_and(|options| options.values().any(|v| v != &Value::Bool(false)))
            {
                return failure(
                    "EPHEMERAL_RECORDING_UNSUPPORTED",
                    "Temporary sessions cannot record trace, video or HAR; set record=false",
                );
            }
            body["ephemeral_content"] = json!(true);
        } else {
            body["ephemeral_content"] = json!(false);
        }
        let http_mode = body["mode"] == "http_api";
        let mut resources = JourneyResources::new(ctx.clone(), Arc::clone(&self.client))
            .with_recording_store(self.db.clone());
        if !http_mode {
            let base_url = match resources.start_preview(&body).await {
                Ok(url) => url,
                Err(error) => {
                    resources.close().await;
                    return failure("VERIFY_PREVIEW_FAILED", error);
                }
            };
            body["base_url"] = json!(base_url);
            if let Err(error) = resources
                .reserve_browser(
                    body["record"]
                        .as_object()
                        .is_some_and(|record| record.values().any(|value| value == true)),
                )
                .await
            {
                resources.close().await;
                return failure("VERIFY_RESOURCE_RESERVATION_FAILED", error);
            }
        }
        if let Some(recording) = &resources.recording_identity {
            body["recording"] = recording.clone();
        }
        body["session_id"] = json!(resources.browser_id);
        let mut deadline = crate::iso::now_millis() + 120_000;
        if let Some(owner) = ctx.execution_resource_owner() {
            match self.db.read_task_budget(&owner.task_id).await {
                Ok(Some(budget)) => {
                    if let Some(root_deadline) = budget.deadline_at_ms {
                        deadline = deadline.min(root_deadline);
                    }
                }
                Ok(None) => {}
                Err(_) => {
                    resources.close().await;
                    return failure(
                        "VERIFY_BUDGET_UNAVAILABLE",
                        "Journey deadline cannot be verified",
                    );
                }
            }
        }
        body["deadline_epoch_ms"] = json!(deadline);
        let correlation = Correlation {
            run_id: Some(run_id.to_owned()),
            session_id: Some(session_id.to_owned()),
        };
        let (capability, endpoint) = if http_mode {
            ("HTTP_API", "/api/http/journey/run")
        } else {
            (BROWSER_AUTOMATION, "/api/browser/journey/run")
        };
        let response: Result<Option<Value>, &str> = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => Ok(None),
            result = self.client.call_journey_if_available(capability, endpoint, &body, &correlation, Duration::from_secs(130)) => result,
        };
        let mut response = match response {
            Ok(response) => response,
            Err(code) => {
                resources.close().await;
                let mut output = failure(
                    code,
                    "Journey refused or stopped; no automatic retry was performed",
                );
                output.metadata = Some(json!({"retryability":"NEVER","effectState":"UNKNOWN"}));
                return output;
            }
        };
        if !http_mode && !ctx.cancel.is_cancelled() {
            self.capture_failure_snapshot(
                &mut response,
                &resources.browser_id,
                &correlation,
                ephemeral,
            )
            .await;
        }
        resources.close().await;
        let Some(mut response) = response else {
            return failure(
                if ctx.cancel.is_cancelled() {
                    "VERIFY_CANCELLED"
                } else {
                    "VERIFY_CAPABILITY_UNAVAILABLE"
                },
                "Journey did not return a confirmed result; do not automatically retry side effects",
            );
        };
        if let Some(error) = &resources.recording_error {
            return failure(
                error,
                "Browser closed but recording finalization is unconfirmed; retained evidence must be reconciled before deletion. Do not replay journey actions.",
            );
        }
        if let Some(manifest) = &resources.recording_manifest {
            response["recording_manifest"] = manifest.clone();
        }
        if let Some(resource) = &resources.recording_resource_id {
            response["recording_resource_id"] = json!(resource);
        }
        response["verification_mode"] = body["mode"].clone();
        let passed = response
            .get("passed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let step_count = response
            .get("step_results")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let Ok(evidence) = build_journey_evidence_receipt(
            &self.db,
            session_id,
            body.get("claim").and_then(Value::as_str),
            &response,
            passed,
        )
        .await
        else {
            tracing::error!(
                code = "VERIFY_EVIDENCE_STORE_FAILED",
                "journey evidence receipt failed"
            );
            return failure(
                "VERIFY_EVIDENCE_STORE_FAILED",
                format!(
                    "Journey {} but evidence could not be stored. Do not rerun automatically; side effects may have occurred.",
                    if passed { "passed" } else { "failed" }
                ),
            );
        };
        let mut structured = sanitize_structured_response(&response);
        let Some(object) = structured.as_object_mut() else {
            return failure(
                "VERIFY_RESPONSE_INVALID",
                "Journey returned a non-object response",
            );
        };
        if let Some(steps) = object.get_mut("step_results").and_then(Value::as_array_mut) {
            for (step, item) in steps.iter_mut().zip(&evidence.items) {
                step["screenshot_stored_as_evidence"] = json!(item.blob_sha256.is_some());
                if let Some(hash) = &item.blob_sha256 {
                    step["screenshot_sha256"] = json!(hash);
                }
                if let Some(reason) = item
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.get("screenshot_archive_error"))
                {
                    step["screenshot_archive_error"] = reason.clone();
                }
            }
        }
        object.insert("evidence".into(), json!(evidence));
        object.insert("verification_mode".into(), body["mode"].clone());
        ToolOutput {
            content: format!(
                "Journey {} ({step_count} steps)",
                if passed { "passed" } else { "failed" }
            ),
            is_error: !passed,
            metadata: Some(json!({"structuredResult":structured})),
        }
    }
}

#[allow(clippy::too_many_lines)] // One validation pass over both supported DSL spellings.
fn normalize_request(mut input: Value) -> Result<Value, (&'static str, &'static str)> {
    const BROWSER: &[&str] = &[
        "navigate",
        "click",
        "type",
        "wait_for",
        "assert_text",
        "assert_url",
        "assert_no_console_error",
        "screenshot",
    ];
    const HTTP: &[&str] = &[
        "http_get",
        "http_post",
        "http_put",
        "http_delete",
        "assert_status",
        "assert_json",
        "assert_header",
        "set_variable",
    ];
    let steps = input
        .get("journey")
        .or_else(|| input.get("steps"))
        .and_then(Value::as_array)
        .ok_or(("VERIFY_STEPS_REQUIRED", "journey or steps must be an array"))?;
    if steps.is_empty() || steps.len() > 50 {
        return Err((
            "VERIFY_STEPS_INVALID",
            "steps must contain between 1 and 50 entries",
        ));
    }
    if input.get("journey").is_some()
        && input.get("steps").is_some()
        && input["journey"] != input["steps"]
    {
        return Err(("VERIFY_STEPS_CONFLICT", "journey and steps disagree"));
    }
    let mut browser = false;
    let mut http = false;
    for step in steps {
        let action = step
            .get("action")
            .and_then(Value::as_str)
            .ok_or(("VERIFY_STEP_INVALID", "Every step requires an action"))?;
        if BROWSER.contains(&action) {
            browser = true;
        } else if HTTP.contains(&action) {
            http = true;
        } else {
            return Err(("VERIFY_STEP_INVALID", "Unknown journey action"));
        }
    }
    if browser && http {
        return Err((
            "VERIFY_MIXED_MODES",
            "Browser and HTTP steps require separate journeys",
        ));
    }
    let mode = input
        .get("verification_mode")
        .or_else(|| input.get("mode"))
        .and_then(Value::as_str)
        .unwrap_or("auto");
    let mode = match mode {
        "auto" => {
            if http {
                "http_api"
            } else {
                "browser"
            }
        }
        "browser" if !http => "browser",
        "http_api" if !browser => "http_api",
        _ => return Err(("VERIFY_MODE_INVALID", "Mode must match the journey actions")),
    };
    if let Some(base) = input.get("base_url") {
        let url = base
            .as_str()
            .and_then(|url| reqwest::Url::parse(url).ok())
            .ok_or((
                "VERIFY_BASE_URL_INVALID",
                "base_url must be an absolute HTTP(S) URL",
            ))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(("VERIFY_BASE_URL_INVALID", "base_url must use http or https"));
        }
    } else if mode == "http_api" {
        return Err((
            "VERIFY_BASE_URL_REQUIRED",
            "HTTP verification requires a running base_url",
        ));
    }
    if mode == "http_api" && input.get("start_command").is_some() {
        return Err((
            "VERIFY_HTTP_START_FORBIDDEN",
            "HTTP verification uses an already running service",
        ));
    }
    if input.get("publication_path").is_some() || input.get("publication_runtime").is_some() {
        return Err((
            "VERIFY_PUBLICATION_UNSUPPORTED",
            "Publication binding is not supported",
        ));
    }
    let steps = steps.clone();
    let record = match input.get("record") {
        Some(Value::Bool(value)) => json!({"video":value,"trace":value,"har":value}),
        Some(Value::Object(value)) => Value::Object(value.clone()),
        None if input.get("journey").is_some() => json!({"video":true,"trace":true,"har":true}),
        None => json!({}),
        _ => {
            return Err((
                "VERIFY_RECORD_INVALID",
                "record must be a boolean or recording options",
            ));
        }
    };
    input["steps"] = json!(steps);
    input["mode"] = json!(mode);
    input["record"] = record;
    Ok(input)
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep one ordered evidence budget and receipt assembly path"
)]
async fn build_journey_evidence_receipt(
    db: &Db,
    session_id: &str,
    claim: Option<&str>,
    response: &Value,
    passed: bool,
) -> Result<EvidenceReceipt, Box<dyn std::error::Error + Send + Sync>> {
    let session = db
        .get_session(session_id)
        .await?
        .ok_or("session not found")?;
    let workspace = std::path::PathBuf::from(session.working_dir);
    let mut items = Vec::new();
    let mut screenshot_bytes = 0usize;
    for (sort_order, step) in response
        .get("step_results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let mut meta = step.clone();
        let blob_sha256 =
            archive_screenshot(db, session_id, &workspace, &mut meta, &mut screenshot_bytes)
                .await?;
        let action = step
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("journey_step");
        let status = if step.get("ok").and_then(Value::as_bool).unwrap_or(false) {
            "passed"
        } else {
            "failed"
        };
        let error = step.get("error").and_then(Value::as_str).unwrap_or("");
        items.push(EvidenceReceiptItem {
            item_type: if response["verification_mode"] == "http_api" {
                "http_journey_step"
            } else {
                "browser_journey_step"
            }
            .into(),
            summary: Some(bounded_text(
                &SensitiveDataFilter::filter(&format!("{action}: {status} {error}")),
                8192,
            )),
            blob_sha256,
            meta: Some(bounded_step_meta(meta)),
            sort_order: u32::try_from(sort_order).unwrap_or(u32::MAX),
        });
    }
    if db.session_retention(session_id).await? == zk_db::content::ContentRetention::Ephemeral {
        if response
            .get("artifacts")
            .and_then(Value::as_object)
            .is_some_and(|items| !items.is_empty())
        {
            return Err("EPHEMERAL_RECORDING_UNEXPECTED".into());
        }
    } else {
        super::browser_recordings::archive(db, session_id, &workspace, response, &mut items)
            .await?;
    }
    if let Some(snapshot) = response.get("failure_snapshot")
        && items.len() < zk_tools::MAX_EVIDENCE_RECEIPT_ITEMS
    {
        let snapshot = SensitiveDataFilter::filter(&snapshot.to_string());
        let digest = crate::api::evidence::store_blob(
            db,
            session_id,
            workspace.clone(),
            snapshot.into_bytes(),
        )
        .await?;
        items.push(EvidenceReceiptItem {
            item_type: "semantic_snapshot".into(),
            summary: Some("Semantic snapshot after the failed journey".into()),
            blob_sha256: Some(digest),
            meta: Some(json!({"format":"json"})),
            sort_order: u32::try_from(items.len()).unwrap_or(u32::MAX),
        });
    }
    let receipt = EvidenceReceipt {
        schema_version: EVIDENCE_RECEIPT_SCHEMA_VERSION,
        kind: if response["verification_mode"] == "http_api" {
            "http_journey"
        } else {
            "browser_journey"
        }
        .into(),
        claim: Some(bounded_text(
            &SensitiveDataFilter::filter(claim.unwrap_or("Journey verification")),
            4096,
        )),
        verdict: if passed {
            EvidenceReceiptVerdict::Verified
        } else {
            EvidenceReceiptVerdict::Failed
        },
        observed_at: crate::iso::format_rfc3339_micros(crate::iso::now_millis()),
        items,
    };
    if !receipt.is_valid() {
        return Err("browser journey produced an invalid evidence receipt".into());
    }
    Ok(receipt)
}

fn bounded_text(text: &str, max_bytes: usize) -> String {
    let mut end = max_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

fn bounded_step_meta(meta: Value) -> Value {
    let filtered = SensitiveDataFilter::filter(&meta.to_string());
    if filtered.len() <= 4096 {
        return serde_json::from_str(&filtered).unwrap_or(meta);
    }
    let mut bounded = json!({"details_truncated":true});
    for key in [
        "index",
        "action",
        "ok",
        "duration_ms",
        "method",
        "warning",
        "error",
        "screenshot_error",
        "screenshot_archive_error",
    ] {
        if let Some(value) = meta.get(key) {
            bounded[key] = if let Some(text) = value.as_str() {
                json!(bounded_text(&SensitiveDataFilter::filter(text), 512))
            } else {
                value.clone()
            };
        }
    }
    bounded
}

async fn archive_screenshot(
    db: &Db,
    session_id: &str,
    workspace: &std::path::Path,
    meta: &mut Value,
    screenshot_bytes: &mut usize,
) -> Result<Option<String>, crate::error::ApiError> {
    let screenshot = meta
        .as_object_mut()
        .and_then(|object| object.remove("screenshot_base64"))
        .and_then(|value| value.as_str().map(str::to_owned));
    let blob_sha256 = if let Some(encoded) = screenshot {
        let decoded = if encoded.len() > 4 * (5 * 1024 * 1024usize).div_ceil(3) {
            Err("Screenshot exceeds 5 MiB")
        } else {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| "Screenshot is not valid base64")
        };
        match decoded {
            Ok(bytes)
                if bytes.len() <= 5 * 1024 * 1024
                    && *screenshot_bytes + bytes.len() <= 20 * 1024 * 1024
                    && crate::api::evidence::image_mime(&bytes).is_some() =>
            {
                *screenshot_bytes += bytes.len();
                Some(
                    crate::api::evidence::store_blob(db, session_id, workspace.to_owned(), bytes)
                        .await?,
                )
            }
            result => {
                meta["screenshot_archive_error"] =
                    json!(result.err().unwrap_or(
                        "Screenshot is not PNG/JPEG or exceeds the journey image budget"
                    ));
                None
            }
        }
    } else {
        None
    };
    Ok(blob_sha256)
}

fn sanitize_structured_response(response: &Value) -> Value {
    let mut sanitized = response.clone();
    if let Some(object) = sanitized.as_object_mut() {
        object.remove("artifacts");
        object.remove("recording_manifest");
        object.remove("recording_resource_id");
    }
    if let Some(steps) = sanitized
        .get_mut("step_results")
        .and_then(Value::as_array_mut)
    {
        for step in steps {
            if let Some(object) = step.as_object_mut()
                && object.remove("screenshot_base64").is_some()
            {
                object.insert("screenshot_stored_as_evidence".into(), Value::Bool(true));
            }
        }
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn context() -> ToolContext {
        let (progress, _receiver) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), progress)
            .with_session_id("session")
            .with_run_id("run")
    }

    #[test]
    fn new_and_legacy_journey_dsl_preserve_modes_and_recording() {
        let new = normalize_request(json!({"journey":[{"action":"http_get","url":"/"}], "base_url":"http://localhost:80", "verification_mode":"auto", "record":false})).unwrap();
        assert_eq!(new["mode"], "http_api");
        assert_eq!(new["record"]["video"], false);
        let old = normalize_request(json!({"steps":[{"action":"screenshot"}], "base_url":"http://localhost", "mode":"browser", "record":{"trace":true}})).unwrap();
        assert_eq!(old["record"]["trace"], true);
        assert!(
            normalize_request(json!({"journey":[{"action":"http_get"},{"action":"click"}]}))
                .is_err()
        );
        assert!(
            normalize_request(json!({"journey":[{"action":"screenshot"}],"publication_path":"."}))
                .is_err()
        );
    }

    #[tokio::test]
    async fn receipt_limits_preserve_failed_verdict_and_fifty_steps() {
        let db = Db::open_in_memory().unwrap();
        let workspace = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&workspace).unwrap();
        let session = db
            .create_session("model", workspace.to_str().unwrap())
            .await
            .unwrap();
        let steps:Vec<Value> = (0..50).map(|index|json!({"index":index,"action":"screenshot","ok":false,"error":"錯".repeat(5000),"console_errors":["long".repeat(10000)]})).collect();
        let receipt = build_journey_evidence_receipt(&db,&session.id,Some(&"claim".repeat(1000)),&json!({"step_results":steps,"failure_snapshot":{"session_id":"untrusted-sidecar-id","tree":"details"},"artifacts":{"trace_path":"unused"}}),false).await.unwrap();
        assert!(receipt.is_valid());
        assert_eq!(receipt.items.len(), 50);
        assert_eq!(receipt.verdict, EvidenceReceiptVerdict::Failed);
        assert_eq!(
            receipt.items[0].meta.as_ref().unwrap()["details_truncated"],
            true
        );
        let receipt = build_journey_evidence_receipt(&db,&session.id,None,&json!({"step_results":[{"index":0,"action":"click","ok":false}],"failure_snapshot":{"session_id":"sidecar","tree":"details"}}),false).await.unwrap();
        assert!(receipt.is_valid());
        assert_eq!(receipt.items[1].item_type, "semantic_snapshot");
        assert!(receipt.items[1].blob_sha256.is_some());
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[tokio::test]
    async fn validates_before_python_io() {
        let tool = BrowserVerifyJourneyTool::new(
            Arc::new(PythonClient::new("/tmp/zkcode-missing-journey.sock")),
            Db::open_in_memory().expect("db"),
        );
        assert_eq!(tool.name(), "VerifyJourney");
        let output = tool
            .execute(
                json!({"steps": [{"action":"http_get","url":"/"}]}),
                context(),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.starts_with("VERIFY_BASE_URL_REQUIRED:"));

        let output = tool
            .execute(
                json!({"base_url":"file:///tmp/index.html","steps":[{"action":"screenshot"}]}),
                context(),
            )
            .await;
        assert!(output.content.starts_with("VERIFY_BASE_URL_INVALID:"));
    }

    #[tokio::test]
    async fn journey_steps_and_screenshots_become_a_bounded_uncommitted_receipt() {
        let db = Db::open_in_memory().expect("db");
        let workspace =
            std::env::temp_dir().join(format!("zkcode-browser-evidence-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&workspace).expect("workspace");
        let session = db
            .create_session("model", workspace.to_str().expect("utf8 path"))
            .await
            .expect("session");
        db.start_run("journey-run", &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        let screenshot = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==";
        let response = json!({
            "passed": true,
            "step_results": [{
                "index": 0,
                "action": "screenshot",
                "ok": true,
                "duration_ms": 4,
                "screenshot_base64": screenshot,
                "error": null
            }]
        });
        let receipt =
            build_journey_evidence_receipt(&db, &session.id, Some("page renders"), &response, true)
                .await
                .expect("receipt");
        assert_eq!(receipt.verdict, EvidenceReceiptVerdict::Verified);
        assert_eq!(receipt.items.len(), 1);
        assert!(receipt.items[0].blob_sha256.is_some());
        assert!(
            receipt.items[0]
                .meta
                .as_ref()
                .is_some_and(|meta| meta.get("screenshot_base64").is_none())
        );
        assert!(
            db.find_evidence_by_session(&session.id)
                .await
                .expect("query")
                .is_empty(),
            "the tool must not commit machine evidence before its invocation succeeds"
        );
        let sanitized = sanitize_structured_response(&response);
        assert_eq!(
            sanitized["step_results"][0]["screenshot_stored_as_evidence"],
            true
        );
        assert!(
            sanitized["step_results"][0]
                .get("screenshot_base64")
                .is_none()
        );
        std::fs::remove_dir_all(&workspace).expect("cleanup");
    }
}
