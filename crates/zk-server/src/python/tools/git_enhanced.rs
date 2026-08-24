//! `Git` 增强工具——gitpython 语义 diff / 结构化 log / 逐行 blame 桥
//! （旧 `tool/impl/GitTool.java`）。
//!
//! 逐字对照旧源：工具名 `Git`、5 分钟超时、description + prompt 文案、
//! 8 个 schema 字段、`{diff,log,blame}` 白名单、三条校验分支与错误码、
//! `success==false` 的业务错误协议、两条 `*_UNAVAILABLE` / `*_FAILED` 文案。
//!
//! 与 2.3 已注册的 `GitDiff` / `GitLog` / `GitStatus`（本地 `git` CLI，
//! zk-tools）互补而非重复：那三件是旧 `GitTool` 在无 Python 时的 CLI 兜底，
//! 本件恢复旧端的**结构化**（gitpython）语义输出。

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::json;
use zk_tools::{Tool, ToolContext, ToolOutput};

use super::{GIT_ENHANCED, PythonEnvelope, allowed_list, failure, is_blank, opt_str};
use crate::python::client::{Correlation, PythonClient};

/// 允许的 action（旧 `ALLOWED_ACTIONS`，:34）。
pub const ALLOWED_ACTIONS: [&str; 3] = ["diff", "log", "blame"];

/// 执行超时（旧 `getMaxExecutionTimeMs() = 300_000`，:48-50）。
const GIT_ENHANCED_TIMEOUT: Duration = Duration::from_mins(5);

/// 工具描述——旧 `getDescription()`（:53-56）与 `prompt()`（:59-73）合并。
///
/// zk-tools 冻结的 `Tool` trait 只有单一 `description` 槽位（旧 `Tool.java`
/// 的 `prompt()` 无对应位），故两段文案以空行拼接后整体下发 LLM，信息量
/// 不丢失（EQUIVALENT，见 §6 偏离表）。
const DESCRIPTION: &str = "Git enhanced analysis tool for semantic diff, structured commit log, \
and line-by-line blame. Provides richer output than raw git commands.\n\n\
Git enhanced analysis tool powered by gitpython.\n\
Use this tool when you need structured Git analysis beyond raw git commands:\n\
- \"diff\": Semantic diff analysis with file-level change statistics\n\
- \"log\": Structured commit log with per-commit file lists\n\
- \"blame\": Line-by-line attribution for a specific file\n\
\n\
The repo_path parameter should be an absolute path to the git repository.\n\
For diff/log, you can specify git refs (branches, tags, commit SHAs).\n\
\n\
Prefer BashTool for simple git operations (status, add, commit, push).\n\
Use this tool only for analysis operations that benefit from structured output.\n";

/// `Git` 增强工具（Python `gitpython` 桥）。
pub struct GitEnhancedTool {
    client: Arc<PythonClient>,
}

impl GitEnhancedTool {
    /// 装配工具（注入组装根持有的 [`PythonClient`]）。
    #[must_use]
    pub fn new(client: Arc<PythonClient>) -> Self {
        Self { client }
    }

    /// 入参校验（旧 `validateInput`，:131-150）。
    fn validate(input: &serde_json::Value) -> Result<&str, ToolOutput> {
        let action = opt_str(input, "action");
        let Some(action) = action.filter(|value| ALLOWED_ACTIONS.contains(value)) else {
            return Err(failure(
                "INVALID_ACTION",
                format!("Action must be one of: {}", allowed_list(&ALLOWED_ACTIONS)),
            ));
        };
        if is_blank(opt_str(input, "repo_path")) {
            return Err(failure("MISSING_REPO_PATH", "repo_path is required"));
        }
        if action == "blame" && is_blank(opt_str(input, "file_path")) {
            return Err(failure(
                "MISSING_FILE_PATH",
                "file_path is required for blame action",
            ));
        }
        Ok(action)
    }
}

impl Tool for GitEnhancedTool {
    fn name(&self) -> &'static str {
        "Git"
    }

    fn description(&self) -> &'static str {
        DESCRIPTION
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["action", "repo_path"],
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ALLOWED_ACTIONS,
                    "description": "Git action: diff, log, or blame"
                },
                "repo_path": {
                    "type": "string",
                    "description": "Absolute path to the git repository"
                },
                "ref1": {
                    "type": "string",
                    "description": "Start reference for diff (default: HEAD~1)"
                },
                "ref2": {
                    "type": "string",
                    "description": "End reference for diff (default: HEAD)"
                },
                "file_path": {
                    "type": "string",
                    "description": "File path for blame (relative to repo root)"
                },
                "ref": {
                    "type": "string",
                    "description": "Git ref for blame (branch/tag/SHA, default: HEAD)"
                },
                "max_count": {
                    "type": "integer",
                    "description": "Max entries for log (default: 20, max: 100)"
                },
                "branch": {
                    "type": "string",
                    "description": "Branch name for log (default: current HEAD)"
                }
            }
        })
    }

    fn timeout(&self) -> Duration {
        GIT_ENHANCED_TIMEOUT
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let action = match Self::validate(&input) {
                Ok(action) => action,
                Err(output) => return output,
            };
            let endpoint = format!("/api/git/{action}");
            let correlation = Correlation::for_session(ctx.session_id());
            // 旧 :155 `new HashMap<>(input.getRawData())`——整个入参原样作为
            // 请求体（含 action 字段），Python 侧按需取字段。
            let response: Option<serde_json::Value> = self
                .client
                .call_if_available(GIT_ENHANCED, &endpoint, &input, &correlation)
                .await;
            let Some(node) = response else {
                return failure(
                    "GIT_ANALYSIS_UNAVAILABLE",
                    "Git enhanced analysis unavailable. Ensure gitpython is installed and \
                     GIT_ENHANCED capability is active.",
                );
            };
            // 旧 :168-173：`success` 缺失或非 false 视为成功（`asBoolean(true)`），
            // 显式 false 才走业务错误分支。
            let succeeded = node
                .get("success")
                .is_none_or(|flag| flag.as_bool().unwrap_or(true));
            if !succeeded {
                let envelope: PythonEnvelope =
                    serde_json::from_value(node).unwrap_or(PythonEnvelope {
                        success: false,
                        data: None,
                        error_code: None,
                        error_message: None,
                    });
                return failure(envelope.code(), envelope.message());
            }
            // 旧 :175-176：`data` 存在则输出 data，否则输出整个节点。
            let payload = node.get("data").unwrap_or(&node);
            ToolOutput::ok(payload.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    fn tool() -> GitEnhancedTool {
        let socket = std::env::temp_dir().join("zk-git-enhanced-absent.sock");
        GitEnhancedTool::new(Arc::new(PythonClient::new(socket)))
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    /// 名称 / 超时 / schema 字段集逐字对齐旧 `GitTool`。
    #[test]
    fn spec_matches_baseline() {
        let tool = tool();
        assert_eq!(tool.name(), "Git");
        assert_eq!(tool.timeout(), Duration::from_mins(5)); // 旧 300_000ms
        assert!(tool.description().contains("powered by gitpython"));
        assert!(tool.description().contains("Prefer BashTool"));

        let schema = tool.parameters();
        assert_eq!(schema["required"], json!(["action", "repo_path"]));
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!(ALLOWED_ACTIONS)
        );
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
                "action",
                "branch",
                "file_path",
                "max_count",
                "ref",
                "ref1",
                "ref2",
                "repo_path"
            ]
        );
    }

    /// 三条校验分支逐条对齐旧 :131-150（含顺序）。
    #[test]
    fn validation_branches_match_baseline() {
        let out = GitEnhancedTool::validate(&json!({})).expect_err("missing action");
        assert_eq!(
            out.content,
            "INVALID_ACTION: Action must be one of: [blame, diff, log]"
        );

        let out =
            GitEnhancedTool::validate(&json!({ "action": "push" })).expect_err("unknown action");
        assert!(out.content.starts_with("INVALID_ACTION: "));

        let out = GitEnhancedTool::validate(&json!({ "action": "diff", "repo_path": "  " }))
            .expect_err("blank repo path");
        assert_eq!(out.content, "MISSING_REPO_PATH: repo_path is required");

        let out = GitEnhancedTool::validate(&json!({ "action": "blame", "repo_path": "/repo" }))
            .expect_err("blame without file_path");
        assert_eq!(
            out.content,
            "MISSING_FILE_PATH: file_path is required for blame action"
        );

        assert_eq!(
            GitEnhancedTool::validate(&json!({ "action": "log", "repo_path": "/repo" }))
                .expect("valid log input"),
            "log"
        );
        assert_eq!(
            GitEnhancedTool::validate(
                &json!({ "action": "blame", "repo_path": "/repo", "file_path": "a.rs" })
            )
            .expect("valid blame input"),
            "blame"
        );
    }

    /// 无 Python 侧车 → 降级文案逐字对齐旧 :161-163。
    #[tokio::test]
    async fn missing_sidecar_degrades_with_baseline_message() {
        let out = tool()
            .execute(json!({ "action": "diff", "repo_path": "/repo" }), ctx())
            .await;
        assert!(out.is_error);
        assert_eq!(
            out.content,
            "GIT_ANALYSIS_UNAVAILABLE: Git enhanced analysis unavailable. Ensure gitpython is \
             installed and GIT_ENHANCED capability is active."
        );
    }
}
