//! `WebBrowser` 工具——Playwright 浏览器自动化桥（旧 `WebBrowserTool.java`）。
//!
//! 逐字对照旧源：工具名 `WebBrowser`、10 分钟超时、完整 description（含三段
//! Markdown 指引）、18 个 schema 字段、15 个 action 白名单、五条校验分支 +
//! 超时上限检查、截图落盘特化路径、四条错误码与文案。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use futures::future::BoxFuture;
use serde_json::json;
use zk_tools::{Tool, ToolContext, ToolOutput};

use super::{BROWSER_AUTOMATION, PythonEnvelope, allowed_list, failure, int_or, is_blank, opt_str};
use crate::api::browser_replay::BrowserReplayStore;
use crate::iso::now_millis;
use crate::python::client::{Correlation, PythonClient};

/// 允许的 action（旧 `ALLOWED_ACTIONS`，:34-38，共 15 个）。
pub const ALLOWED_ACTIONS: [&str; 15] = [
    "navigate",
    "screenshot",
    "click",
    "type",
    "evaluate",
    "extract_text",
    "extract_html",
    "wait_for",
    "select_option",
    "handle_dialog",
    "get_cookies",
    "set_cookie",
    "close_session",
    "get_js_errors",
    "snapshot-semantic",
];

/// 安全协议前缀（旧 `SAFE_PROTOCOLS`，:39）。
const SAFE_PROTOCOLS: [&str; 2] = ["http://", "https://"];

/// `evaluate` 脚本长度上限（旧 `MAX_SCRIPT_LENGTH = 10000`，:40）。
pub const MAX_SCRIPT_LENGTH: usize = 10_000;

/// 单 action 超时上限毫秒（旧 `MAX_TIMEOUT_MS = 600_000`，:41）。
pub const MAX_TIMEOUT_MS: i64 = 600_000;

/// 默认 action 超时毫秒（旧 :239 `input.getInt("timeout", 30000)`）。
const DEFAULT_TIMEOUT_MS: i64 = 30_000;

/// 工具执行超时（旧 `getMaxExecutionTimeMs() = 600_000`，:57-59）。
const BROWSER_TIMEOUT: Duration = Duration::from_mins(10);

/// 会话缺失时的兜底 session（Python `BrowserRequestBase.session_id` 默认值）。
const DEFAULT_SESSION_ID: &str = "default";

/// 工具描述（旧 `getDescription()` :62-95 的逐字搬运，含 `\n` 分段）。
const DESCRIPTION: &str = concat!(
    "Browser automation tool for navigating web pages, clicking elements, ",
    "filling forms, taking screenshots, and extracting content. ",
    "Supports JavaScript rendering with structured error capture (js_errors + collected_errors), ",
    "advanced wait conditions (wait_until: networkidle/load/domcontentloaded, text_contains), ",
    "session validation (strict_session), and interactive web operations.\n\n",
    "## Multi-step Browser Testing\n",
    "This tool supports chaining multiple browser actions for end-to-end testing. Typical workflow:\n",
    "1. navigate: Open the target URL\n",
    "2. screenshot: Capture initial page state\n",
    "3. type: Fill in form fields (use CSS selectors like 'input[name=q]', '#search-input', etc.)\n",
    "4. click: Click buttons or links (use CSS selectors like 'button[type=submit]', 'a.link-class', etc.)\n",
    "5. wait_for: Wait for page transitions or element appearance\n",
    "6. screenshot: Capture result page\n",
    "7. extract_text: Get page content for analysis\n",
    "8. close_session: Clean up when done\n\n",
    "## Common Selectors for Popular Sites\n",
    "- Baidu (2025+ new version): search box `#chat-textarea`, submit button `#chat-submit-button`\n",
    "- Baidu (legacy, hidden): `input[name=wd]`, `#kw`, `#su`\n",
    "- Search inputs: input[type=search], input[name=q]\n",
    "- Submit buttons: button[type=submit], input[type=submit]\n",
    "- Links: a[href*=\"keyword\"]\n\n",
    "## Important Notes\n",
    "- Always use navigate first to create a session\n",
    "- Use the same session_id across all actions in a test sequence\n",
    "- Use wait_for after click/navigate to ensure page loads completely\n",
    "- For AJAX pages (e.g. Baidu new search), use `no_wait_after: true` in click to avoid navigation timeout\n",
    "- If click times out due to element visibility issues, the tool automatically retries with JS click\n",
    "- Use click with `force: true` for elements that exist in DOM but are not visible (e.g. headless mode)\n",
    "- After AJAX click, use `wait_for(wait_until: 'networkidle')` to wait for content loading\n",
    "- Take screenshots at key checkpoints for visual evidence\n",
    "- Always close_session when testing is complete\n",
    "- Use get_js_errors to check for JavaScript errors on the page"
);

/// `WebBrowser` 工具（Python Playwright 桥）。
pub struct WebBrowserTool {
    client: Arc<PythonClient>,
    replay: Option<Arc<BrowserReplayStore>>,
}

impl WebBrowserTool {
    /// 装配工具（注入组装根持有的 [`PythonClient`]）。
    #[must_use]
    pub fn new(client: Arc<PythonClient>) -> Self {
        Self {
            client,
            replay: None,
        }
    }

    /// Production constructor: semantic snapshots are appended to the durable
    /// Browser Replay timeline before success is returned.
    #[must_use]
    pub(crate) fn with_replay_store(
        client: Arc<PythonClient>,
        replay: Arc<BrowserReplayStore>,
    ) -> Self {
        Self {
            client,
            replay: Some(replay),
        }
    }

    /// 入参校验（旧 `validateInput`，:181-246）。
    fn validate(input: &serde_json::Value) -> Result<&str, ToolOutput> {
        let Some(action) = opt_str(input, "action").filter(|value| ALLOWED_ACTIONS.contains(value))
        else {
            return Err(failure(
                "INVALID_ACTION",
                format!("Action must be one of: {}", allowed_list(&ALLOWED_ACTIONS)),
            ));
        };

        match action {
            "navigate" => {
                let url = opt_str(input, "url");
                if is_blank(url) {
                    return Err(failure(
                        "MISSING_URL",
                        "URL is required for navigate action",
                    ));
                }
                let url = url.unwrap_or_default();
                if !SAFE_PROTOCOLS.iter().any(|prefix| url.starts_with(prefix)) {
                    return Err(failure(
                        "UNSAFE_PROTOCOL",
                        "Only http:// and https:// protocols are allowed",
                    ));
                }
            }
            // 旧 :199-215：`wait_for` 允许 selector / wait_until 二选一，
            // `click` 则必须有 selector。
            "wait_for" => {
                if is_blank(opt_str(input, "selector")) && is_blank(opt_str(input, "wait_until")) {
                    return Err(failure(
                        "MISSING_PARAM",
                        "wait_for action requires either 'selector' or 'wait_until' parameter",
                    ));
                }
            }
            "click" => {
                if is_blank(opt_str(input, "selector")) {
                    return Err(failure(
                        "MISSING_SELECTOR",
                        format!("Selector is required for {action} action"),
                    ));
                }
            }
            "type" => {
                if is_blank(opt_str(input, "selector")) {
                    return Err(failure(
                        "MISSING_SELECTOR",
                        "Selector is required for type action",
                    ));
                }
                // 旧 :222 只判 `text == null`——空串合法（清空输入框）。
                if opt_str(input, "text").is_none() {
                    return Err(failure("MISSING_TEXT", "Text is required for type action"));
                }
            }
            "evaluate" => {
                let script = opt_str(input, "script");
                if is_blank(script) {
                    return Err(failure(
                        "MISSING_SCRIPT",
                        "Script is required for evaluate action",
                    ));
                }
                // 旧 :231 以 Java `String.length()`（UTF-16 code unit）计长；
                // 此处以 UTF-16 code unit 计长，逐字对齐边界行为。
                if script.unwrap_or_default().encode_utf16().count() > MAX_SCRIPT_LENGTH {
                    return Err(failure(
                        "SCRIPT_TOO_LONG",
                        format!("Script must be less than {MAX_SCRIPT_LENGTH} characters"),
                    ));
                }
            }
            _ => {}
        }

        // 旧 :238-243：超时上限检查（对所有 action 生效）。
        if int_or(input, "timeout", DEFAULT_TIMEOUT_MS) > MAX_TIMEOUT_MS {
            return Err(failure(
                "TIMEOUT_TOO_HIGH",
                format!("Timeout must be less than {MAX_TIMEOUT_MS}ms"),
            ));
        }
        Ok(action)
    }
}

impl Tool for WebBrowserTool {
    fn name(&self) -> &'static str {
        "WebBrowser"
    }

    fn description(&self) -> &'static str {
        DESCRIPTION
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ALLOWED_ACTIONS,
                    "description": "Browser action to perform"
                },
                "session_id": {
                    "type": "string",
                    "description": "Browser session ID (optional, defaults to current session)"
                },
                "url": { "type": "string", "description": "URL to navigate to" },
                "selector": { "type": "string", "description": "CSS selector for target element" },
                "text": { "type": "string", "description": "Text to type into an input field" },
                "script": { "type": "string", "description": "JavaScript expression to evaluate" },
                "full_page": {
                    "type": "boolean",
                    "description": "Whether to take a full page screenshot"
                },
                "wait_for": {
                    "type": "string",
                    "description": "Wait condition: load, domcontentloaded, or networkidle"
                },
                "wait_until": {
                    "type": "string",
                    "description": "Wait condition type for wait_for action: networkidle, load, domcontentloaded"
                },
                "state": {
                    "type": "string",
                    "description": "Element state for wait_for action: visible, hidden, attached, detached (default: visible)"
                },
                "text_contains": {
                    "type": "string",
                    "description": "Wait until element text contains this value (requires selector, used with wait_for action)"
                },
                "strict_session": {
                    "type": "boolean",
                    "description": "If true, fail when session does not exist instead of auto-creating (default: false)"
                },
                "timeout": {
                    "type": "integer",
                    "description": format!("Timeout in milliseconds (default: 30000, max: {MAX_TIMEOUT_MS})")
                },
                "accept": { "type": "boolean", "description": "Whether to accept a dialog" },
                "cookie": {
                    "type": "object",
                    "description": "Cookie object with name, value, domain, path"
                },
                "values": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Values to select in a dropdown"
                },
                "no_wait_after": {
                    "type": "boolean",
                    "description": "If true, skip waiting for navigation after click (useful for AJAX pages like Baidu new search)"
                },
                "force": {
                    "type": "boolean",
                    "description": "Force click, skip visibility/actionability checks. Useful for elements that exist in DOM but are not visible in headless mode (default: false). Click auto-falls back to JS click on visibility errors."
                }
            }
        })
    }

    fn timeout(&self) -> Duration {
        BROWSER_TIMEOUT
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let action = match Self::validate(&input) {
                Ok(action) => action.to_owned(),
                Err(output) => return output,
            };
            // 旧 :251 `input.getString("session_id", context.sessionId())`：
            // 入参优先，回落上下文会话；两者皆空时用 Python 侧默认 "default"
            // （旧端此时会写入 JSON null → Pydantic 422，此处兜底，见 §6）。
            let session_id = opt_str(&input, "session_id")
                .filter(|value| !value.trim().is_empty())
                .or_else(|| ctx.session_id())
                .unwrap_or(DEFAULT_SESSION_ID)
                .to_owned();

            // 旧 :252-253：整个入参作为请求体，并以有效 session 覆盖。
            let mut body = input.clone();
            if let Some(map) = body.as_object_mut() {
                map.insert("session_id".to_owned(), json!(session_id));
            } else {
                body = json!({ "session_id": session_id, "action": action });
            }

            let endpoint = format!("/api/browser/{action}");
            let correlation = Correlation::for_session(ctx.session_id());
            let response: Option<PythonEnvelope> = self
                .client
                .call_if_available(BROWSER_AUTOMATION, &endpoint, &body, &correlation)
                .await;

            let Some(envelope) = response else {
                return failure(
                    "BROWSER_AUTOMATION_UNAVAILABLE",
                    "Browser automation unavailable. Ensure playwright is installed: \
                     pip install playwright && playwright install chromium",
                );
            };
            if !envelope.success {
                return failure(envelope.code(), envelope.message());
            }
            let data = envelope.data.unwrap_or(serde_json::Value::Null);

            // 旧 :268-296 截图特化：base64 落盘，只回路径，避免撑爆上下文。
            if action == "screenshot"
                && let Some(base64_png) = data.get("screenshot_base64").and_then(|v| v.as_str())
            {
                let reported_size = data.get("size").and_then(serde_json::Value::as_i64);
                return persist_screenshot(
                    base64_png,
                    reported_size,
                    &session_id,
                    ctx.working_dir(),
                );
            }
            if action == "snapshot-semantic"
                && let Some(replay) = &self.replay
                && let Err(error) = replay.append_python_snapshot(&session_id, &data)
            {
                tracing::error!(
                    error_type = ?error.kind(),
                    "browser semantic snapshot persistence failed"
                );
                return failure(
                    "BROWSER_REPLAY_PERSIST_FAILED",
                    "Browser snapshot was captured but could not be persisted",
                );
            }
            ToolOutput::ok(data.to_string())
        })
    }
}

/// 截图落盘（旧 :271-295 的等价实现）。
///
/// 路径 `{workingDir}/screenshots/screenshot_{sessionId}_{millis}.png`；写入
/// 走「同目录临时文件 + `rename`」的原子替换（旧 `AtomicFileWriter` 的等价
/// 语义），失败按旧端分两档错误码。
fn persist_screenshot(
    base64_png: &str,
    reported_size: Option<i64>,
    session_id: &str,
    working_dir: &Path,
) -> ToolOutput {
    let bytes = match BASE64_STANDARD.decode(base64_png.as_bytes()) {
        Ok(bytes) => bytes,
        Err(error) => {
            // 旧端在 decode 失败时抛 IllegalArgumentException（RuntimeException）
            // → :290 catch → BROWSER_SCREENSHOT_PERSIST_FAILED。
            return failure(
                "BROWSER_SCREENSHOT_PERSIST_FAILED",
                format!(
                    "Screenshot taken ({} bytes) but failed to save: {error}",
                    reported_size.unwrap_or(0)
                ),
            );
        }
    };
    let filename = format!("screenshot_{session_id}_{}.png", now_millis());
    let filepath = working_dir.join("screenshots").join(filename);
    match atomic_write(&filepath, &bytes) {
        Ok(()) => {
            let absolute = std::fs::canonicalize(&filepath).unwrap_or_else(|_| filepath.clone());
            tracing::info!(
                path = %filepath.display(),
                bytes = bytes.len(),
                "screenshot saved"
            );
            let mut output = ToolOutput::ok(format!(
                "Screenshot saved to: {} (size: {} bytes)",
                absolute.display(),
                bytes.len()
            ));
            output.metadata = Some(json!({ "filePath": filepath.to_string_lossy() }));
            output
        }
        Err(error) => failure(
            "BROWSER_SCREENSHOT_WRITE_FAILED",
            format!(
                "Failed to write screenshot to {}: {error}",
                filepath.display()
            ),
        ),
    }
}

/// 原子写：同目录 `.tmp` 落盘后 `rename` 顶替（同一文件系统内为原子操作）。
fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut temp: PathBuf = path.to_path_buf();
    let stem = path.file_name().map_or_else(
        || "screenshot.png".to_owned(),
        |n| n.to_string_lossy().into_owned(),
    );
    temp.set_file_name(format!(".{stem}.{}.tmp", std::process::id()));
    std::fs::write(&temp, bytes)?;
    match std::fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            std::fs::remove_file(&temp).ok();
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    fn tool() -> WebBrowserTool {
        let socket = std::env::temp_dir().join("zk-web-browser-absent.sock");
        WebBrowserTool::new(Arc::new(PythonClient::new(socket)))
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    /// 名称 / 超时 / 常量 / schema 字段集逐字对齐旧 `WebBrowserTool`。
    #[test]
    fn spec_matches_baseline() {
        let tool = tool();
        assert_eq!(tool.name(), "WebBrowser");
        assert_eq!(tool.timeout(), Duration::from_mins(10)); // 旧 600_000ms
        assert_eq!(MAX_SCRIPT_LENGTH, 10_000);
        assert_eq!(MAX_TIMEOUT_MS, 600_000);
        assert_eq!(ALLOWED_ACTIONS.len(), 15);

        let description = tool.description();
        assert!(description.starts_with("Browser automation tool for navigating web pages"));
        assert!(description.contains("## Multi-step Browser Testing\n"));
        assert!(description.contains("## Common Selectors for Popular Sites\n"));
        assert!(description.contains("## Important Notes\n"));
        assert!(
            description.ends_with("- Use get_js_errors to check for JavaScript errors on the page")
        );

        let schema = tool.parameters();
        assert_eq!(schema["required"], json!(["action"]));
        let mut keys: Vec<&str> = schema["properties"]
            .as_object()
            .expect("properties object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "accept",
                "action",
                "cookie",
                "force",
                "full_page",
                "no_wait_after",
                "script",
                "selector",
                "session_id",
                "state",
                "strict_session",
                "text",
                "text_contains",
                "timeout",
                "url",
                "values",
                "wait_for",
                "wait_until"
            ]
        );
        // 旧 schema 恰 18 个属性（:102-159）。
        assert_eq!(keys.len(), 18);
    }

    /// action 白名单：15 个逐字一致，未知 action 报 `INVALID_ACTION`。
    #[test]
    fn action_allow_list_matches_baseline() {
        let mut sorted = ALLOWED_ACTIONS;
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            [
                "click",
                "close_session",
                "evaluate",
                "extract_html",
                "extract_text",
                "get_cookies",
                "get_js_errors",
                "handle_dialog",
                "navigate",
                "screenshot",
                "select_option",
                "set_cookie",
                "snapshot-semantic",
                "type",
                "wait_for"
            ]
        );
        let out =
            WebBrowserTool::validate(&json!({ "action": "download" })).expect_err("unknown action");
        assert!(
            out.content
                .starts_with("INVALID_ACTION: Action must be one of: [")
        );
    }

    /// navigate 分支：url 必填 + 仅 http/https。
    #[test]
    fn navigate_validation_matches_baseline() {
        let out =
            WebBrowserTool::validate(&json!({ "action": "navigate" })).expect_err("missing url");
        assert_eq!(
            out.content,
            "MISSING_URL: URL is required for navigate action"
        );

        let out = WebBrowserTool::validate(&json!({ "action": "navigate", "url": "  " }))
            .expect_err("blank url");
        assert!(out.content.starts_with("MISSING_URL: "));

        let out =
            WebBrowserTool::validate(&json!({ "action": "navigate", "url": "file:///etc/passwd" }))
                .expect_err("unsafe protocol");
        assert_eq!(
            out.content,
            "UNSAFE_PROTOCOL: Only http:// and https:// protocols are allowed"
        );

        for url in ["http://a.test/x", "https://a.test/x"] {
            assert_eq!(
                WebBrowserTool::validate(&json!({ "action": "navigate", "url": url }))
                    .expect("safe protocol"),
                "navigate"
            );
        }
    }

    /// `click` / `wait_for` / `type` / `evaluate` 分支逐条对齐旧 :199-235。
    #[test]
    fn per_action_validation_matches_baseline() {
        let out =
            WebBrowserTool::validate(&json!({ "action": "click" })).expect_err("click no selector");
        assert_eq!(
            out.content,
            "MISSING_SELECTOR: Selector is required for click action"
        );

        let out = WebBrowserTool::validate(&json!({ "action": "wait_for" }))
            .expect_err("wait_for without selector/wait_until");
        assert_eq!(
            out.content,
            "MISSING_PARAM: wait_for action requires either 'selector' or 'wait_until' parameter"
        );
        // selector 或 wait_until 二选一即可。
        assert_eq!(
            WebBrowserTool::validate(&json!({ "action": "wait_for", "selector": "#x" }))
                .expect("selector only"),
            "wait_for"
        );
        assert_eq!(
            WebBrowserTool::validate(&json!({ "action": "wait_for", "wait_until": "networkidle" }))
                .expect("wait_until only"),
            "wait_for"
        );

        let out = WebBrowserTool::validate(&json!({ "action": "type", "text": "x" }))
            .expect_err("type without selector");
        assert!(out.content.starts_with("MISSING_SELECTOR: "));
        let out = WebBrowserTool::validate(&json!({ "action": "type", "selector": "#x" }))
            .expect_err("type without text");
        assert_eq!(
            out.content,
            "MISSING_TEXT: Text is required for type action"
        );
        // 旧 :222 只判 null——空串是合法的「清空输入框」语义。
        assert_eq!(
            WebBrowserTool::validate(&json!({ "action": "type", "selector": "#x", "text": "" }))
                .expect("empty text is allowed"),
            "type"
        );

        let out = WebBrowserTool::validate(&json!({ "action": "evaluate" }))
            .expect_err("evaluate without script");
        assert_eq!(
            out.content,
            "MISSING_SCRIPT: Script is required for evaluate action"
        );
        let long_script = "a".repeat(MAX_SCRIPT_LENGTH + 1);
        let out = WebBrowserTool::validate(&json!({ "action": "evaluate", "script": long_script }))
            .expect_err("script too long");
        assert_eq!(
            out.content,
            "SCRIPT_TOO_LONG: Script must be less than 10000 characters"
        );
        let exact = "a".repeat(MAX_SCRIPT_LENGTH);
        assert_eq!(
            WebBrowserTool::validate(&json!({ "action": "evaluate", "script": exact }))
                .expect("exact length is allowed"),
            "evaluate"
        );
    }

    /// 超时上限对所有 action 生效（旧 :238-243），默认 30000 合法。
    #[test]
    fn timeout_ceiling_applies_to_every_action() {
        let out = WebBrowserTool::validate(&json!({ "action": "get_cookies", "timeout": 600_001 }))
            .expect_err("timeout too high");
        assert_eq!(
            out.content,
            "TIMEOUT_TOO_HIGH: Timeout must be less than 600000ms"
        );
        assert_eq!(
            WebBrowserTool::validate(&json!({ "action": "get_cookies", "timeout": 600_000 }))
                .expect("ceiling is inclusive"),
            "get_cookies"
        );
        assert_eq!(
            WebBrowserTool::validate(&json!({ "action": "get_cookies" })).expect("default timeout"),
            "get_cookies"
        );
    }

    /// 无 Python 侧车 → 降级文案逐字对齐旧 :259-261。
    #[tokio::test]
    async fn missing_sidecar_degrades_with_baseline_message() {
        let out = tool()
            .execute(
                json!({ "action": "navigate", "url": "https://example.test" }),
                ctx(),
            )
            .await;
        assert!(out.is_error);
        assert_eq!(
            out.content,
            "BROWSER_AUTOMATION_UNAVAILABLE: Browser automation unavailable. Ensure playwright \
             is installed: pip install playwright && playwright install chromium"
        );
    }

    /// 截图落盘：路径形状 / 输出文案 / metadata 对齐旧 :286-289。
    #[test]
    fn screenshot_persists_and_reports_absolute_path() {
        let dir = std::env::temp_dir().join(format!("zk-shot-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp working dir");
        let png = BASE64_STANDARD.encode(b"\x89PNG\r\n\x1a\nfake");

        let out = persist_screenshot(&png, Some(13), "sess-1", &dir);
        assert!(!out.is_error, "unexpected failure: {}", out.content);
        assert!(out.content.starts_with("Screenshot saved to: "));
        assert!(out.content.ends_with(" bytes)"));
        let metadata = out.metadata.expect("metadata present");
        let path = metadata["filePath"].as_str().expect("filePath");
        assert!(path.contains("/screenshots/screenshot_sess-1_"));
        assert_eq!(
            std::path::Path::new(path)
                .extension()
                .and_then(|e| e.to_str()),
            Some("png")
        );
        assert_eq!(
            std::fs::read(path).expect("screenshot written").len(),
            b"\x89PNG\r\n\x1a\nfake".len()
        );
        // 临时文件不残留。
        let leftovers: Vec<_> = std::fs::read_dir(dir.join("screenshots"))
            .expect("screenshots dir")
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp files must be renamed away");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// base64 非法 → `PERSIST_FAILED`（旧 :290-294 的 `RuntimeException` 分支）。
    #[test]
    fn invalid_base64_reports_persist_failure() {
        let dir = std::env::temp_dir().join(format!("zk-shot-bad-{}", std::process::id()));
        let out = persist_screenshot("!!!not-base64!!!", Some(42), "s", &dir);
        assert!(out.is_error);
        assert!(
            out.content
                .starts_with("BROWSER_SCREENSHOT_PERSIST_FAILED: ")
        );
        assert!(
            out.content
                .contains("Screenshot taken (42 bytes) but failed to save")
        );
    }
}
