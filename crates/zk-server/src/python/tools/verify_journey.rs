//! Browser journey verification tool backed by the Python Playwright capability.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use zk_authz::sensitive::SensitiveDataFilter;
use zk_db::{Db, EvidenceBundleRecord, EvidenceItemRecord};
use zk_tools::{Tool, ToolContext, ToolOutput};

use super::{BROWSER_AUTOMATION, failure};
use crate::python::client::{Correlation, HEAVY_READ_TIMEOUT, PythonClient};

/// Browser-semantic `VerifyJourney`; engineering compile/test checks live in
/// `VerifyPlanExecution` and `/api/verify/run-checks`.
pub struct BrowserVerifyJourneyTool {
    client: Arc<PythonClient>,
    db: Db,
}

impl BrowserVerifyJourneyTool {
    /// Build the browser journey bridge with the shared Python client.
    #[must_use]
    pub fn new(client: Arc<PythonClient>, db: Db) -> Self {
        Self { client, db }
    }
}

impl Tool for BrowserVerifyJourneyTool {
    fn name(&self) -> &'static str {
        "VerifyJourney"
    }

    fn description(&self) -> &'static str {
        "Run a bounded browser or HTTP user journey through the Python sidecar and return \
         deterministic step evidence. Use VerifyPlanExecution for compile/test/lint checks."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "required": ["base_url", "steps"],
            "properties": {
                "base_url": {"type":"string", "description":"HTTP(S) application base URL"},
                "steps": {
                    "type":"array", "minItems":1, "maxItems":50,
                    "items":{"type":"object"}
                },
                "record": {"type":"object"},
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
        HEAVY_READ_TIMEOUT + Duration::from_secs(5)
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Some(base_url) = input.get("base_url").and_then(Value::as_str) else {
                return failure("VERIFY_BASE_URL_REQUIRED", "base_url is required");
            };
            if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
                return failure("VERIFY_BASE_URL_INVALID", "base_url must use http or https");
            }
            let Some(steps) = input.get("steps").and_then(Value::as_array) else {
                return failure("VERIFY_STEPS_REQUIRED", "steps must be an array");
            };
            if steps.is_empty() || steps.len() > 50 {
                return failure(
                    "VERIFY_STEPS_INVALID",
                    "steps must contain between 1 and 50 entries",
                );
            }
            let mut body = input;
            if let Some(object) = body.as_object_mut() {
                let session_id = object
                    .get("session_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| ctx.session_id().map(str::to_owned))
                    .unwrap_or_else(|| "verify-journey".to_owned());
                object.insert("session_id".into(), json!(session_id));
            }
            let Some(session_id) = ctx.session_id() else {
                return failure("VERIFY_CONTEXT_REQUIRED", "session context is required");
            };
            let Some(run_id) = ctx.run_id() else {
                return failure("VERIFY_CONTEXT_REQUIRED", "run context is required");
            };
            let correlation = Correlation {
                run_id: ctx.run_id().map(str::to_owned),
                session_id: ctx.session_id().map(str::to_owned),
            };
            let response: Option<Value> = self
                .client
                .call_if_available_with_timeout(
                    BROWSER_AUTOMATION,
                    "/api/browser/journey/run",
                    &body,
                    &correlation,
                    HEAVY_READ_TIMEOUT,
                )
                .await;
            let Some(response) = response else {
                return failure(
                    "VERIFY_BROWSER_UNAVAILABLE",
                    "Browser journey verification is unavailable",
                );
            };
            let passed = response
                .get("passed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let step_count = response
                .get("step_results")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let evidence = match persist_journey_evidence(
                &self.db,
                session_id,
                run_id,
                body.get("claim").and_then(Value::as_str),
                &response,
                passed,
            )
            .await
            {
                Ok(evidence) => evidence,
                Err(error) => {
                    tracing::error!(%error, "browser journey evidence persistence failed");
                    return failure(
                        "VERIFY_EVIDENCE_STORE_FAILED",
                        "Browser journey finished but evidence could not be persisted",
                    );
                }
            };
            let structured_result = sanitize_structured_response(&response);
            ToolOutput {
                content: format!(
                    "Browser journey {} ({step_count} steps)",
                    if passed { "passed" } else { "failed" }
                ),
                is_error: !passed,
                metadata: Some(json!({
                    "structuredResult": structured_result,
                    "evidenceBundleId": evidence.bundle_id,
                })),
            }
        })
    }
}

async fn persist_journey_evidence(
    db: &Db,
    session_id: &str,
    run_id: &str,
    claim: Option<&str>,
    response: &Value,
    passed: bool,
) -> Result<EvidenceBundleRecord, Box<dyn std::error::Error + Send + Sync>> {
    let session = db
        .get_session(session_id)
        .await?
        .ok_or("session not found")?;
    let workspace = std::path::PathBuf::from(session.working_dir);
    let mut items = Vec::new();
    for (sort_order, step) in response
        .get("step_results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let mut meta = step.clone();
        let screenshot = meta
            .as_object_mut()
            .and_then(|object| object.remove("screenshot_base64"))
            .and_then(|value| value.as_str().map(str::to_owned));
        let blob_sha256 = if let Some(encoded) = screenshot {
            let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
            Some(crate::api::evidence::store_blob(workspace.clone(), bytes).await?)
        } else {
            None
        };
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
        items.push(EvidenceItemRecord {
            id: uuid::Uuid::new_v4().to_string(),
            item_type: "browser_journey_step".into(),
            summary: Some(SensitiveDataFilter::filter(&format!(
                "{action}: {status} {error}"
            ))),
            blob_sha256,
            meta: Some(meta),
            sort_order: i64::try_from(sort_order).unwrap_or(i64::MAX),
        });
    }
    let bundle = EvidenceBundleRecord {
        bundle_id: uuid::Uuid::new_v4().to_string(),
        session_id: session_id.to_owned(),
        agent_id: None,
        kind: "browser_journey".into(),
        claim: Some(SensitiveDataFilter::filter(
            claim.unwrap_or("Browser journey verification"),
        )),
        verdict: if passed { "verified" } else { "failed" }.into(),
        created_at: crate::iso::format_rfc3339_micros(crate::iso::now_millis()),
        run_id: Some(run_id.to_owned()),
        items,
    };
    db.save_evidence_bundle(&bundle).await?;
    Ok(bundle)
}

fn sanitize_structured_response(response: &Value) -> Value {
    let mut sanitized = response.clone();
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

    #[tokio::test]
    async fn validates_before_python_io() {
        let tool = BrowserVerifyJourneyTool::new(
            Arc::new(PythonClient::new("/tmp/zkcode-missing-journey.sock")),
            Db::open_in_memory().expect("db"),
        );
        assert_eq!(tool.name(), "VerifyJourney");
        let output = tool.execute(json!({"steps": [{}]}), context()).await;
        assert!(output.is_error);
        assert!(output.content.starts_with("VERIFY_BASE_URL_REQUIRED:"));

        let output = tool
            .execute(
                json!({"base_url":"file:///tmp/index.html","steps":[{}]}),
                context(),
            )
            .await;
        assert!(output.content.starts_with("VERIFY_BASE_URL_INVALID:"));
    }

    #[tokio::test]
    async fn journey_steps_and_screenshots_become_durable_evidence() {
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
        let screenshot = base64::engine::general_purpose::STANDARD.encode(b"png bytes");
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
        let bundle = persist_journey_evidence(
            &db,
            &session.id,
            "journey-run",
            Some("page renders"),
            &response,
            true,
        )
        .await
        .expect("evidence");
        assert_eq!(bundle.verdict, "verified");
        assert_eq!(bundle.items.len(), 1);
        assert!(bundle.items[0].blob_sha256.is_some());
        assert!(
            bundle.items[0]
                .meta
                .as_ref()
                .is_some_and(|meta| meta.get("screenshot_base64").is_none())
        );
        assert!(
            db.find_evidence_bundle(&bundle.bundle_id)
                .await
                .expect("query")
                .is_some()
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
