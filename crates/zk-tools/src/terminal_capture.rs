//! `TerminalCapture` 工具——回读最近若干行终端输出。
//!
//! 对照旧 `tool/impl/TerminalCaptureTool.java`（39L，只读权威规格）：入参
//! `terminal_id`（默认活动终端）+ `lines`（默认 50），旧实现为**桩**——
//! 恒回 `"终端捕获功能需配合 IDE Bridge 实现"`，权限要求 `NONE`，
//! 分组 `execution`。
//!
//! 差异（留痕 docs/compatibility.md §4）：旧桩不读任何数据源；本实现在
//! 桩语义之上补一条**真实**可用路径——`~/.zk/terminal_history`（经
//! [`zk_core::paths::user_config_dir`] 单一事实源定位）存在时取其尾部
//! `lines` 行。文件缺失 → 逐字回旧桩文案，故旧行为是本实现的真子集。
//! `terminal_id` 入参保留（多终端历史分文件是后续能力），当前仅作为
//! `<id>` 后缀参与文件名解析：`terminal_history-<id>`，缺省取无后缀主文件。

use std::path::PathBuf;

use futures::future::BoxFuture;
use serde_json::json;

use crate::input::{failure, optional_str, optional_usize};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 默认回读行数（旧 `input.getInt("lines", 50)`）。
pub const DEFAULT_CAPTURE_LINES: usize = 50;

/// 回读行数上限（护栏；旧实现无上限因其为桩）。
pub const MAX_CAPTURE_LINES: usize = 2_000;

/// 历史文件名（`~/.zk/terminal_history`）。
pub const TERMINAL_HISTORY_FILE: &str = "terminal_history";

/// 历史文件缺失时的降级文案——**逐字**取自旧
/// `TerminalCaptureTool.java` L33。
pub const CAPTURE_UNAVAILABLE: &str = "终端捕获功能需配合 IDE Bridge 实现";

/// 回读上限（1 MiB；超限只取尾部，避免把巨大历史整体读进内存）。
const MAX_HISTORY_BYTES: u64 = 1024 * 1024;

/// 终端输出回读工具（旧 `TerminalCaptureTool`，名 `TerminalCapture`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct TerminalCaptureTool;

impl Tool for TerminalCaptureTool {
    fn name(&self) -> &'static str {
        "TerminalCapture"
    }

    fn description(&self) -> &'static str {
        "Capture the current contents of the terminal viewport (most recent output lines)."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "terminal_id": {
                    "type": "string",
                    "description": "Terminal id; defaults to the active terminal."
                },
                "lines": {
                    "type": "integer",
                    "description": "Number of trailing lines to capture (default 50, max 2000)."
                }
            }
        })
    }

    /// 只读工具（旧权限要求 `NONE`，无任何副作用）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let lines = optional_usize(&input, "lines")
                .unwrap_or(DEFAULT_CAPTURE_LINES)
                .clamp(1, MAX_CAPTURE_LINES);
            let path = history_path(optional_str(&input, "terminal_id"));
            capture(&path, lines).await
        })
    }
}

/// 历史文件路径：`~/.zk/terminal_history[-<terminal_id>]`。
///
/// `terminal_id` 参与文件名前先剥掉路径分隔符——入参来自模型，不能让它
/// 以 `../` 逃出 `~/.zk/`。
fn history_path(terminal_id: Option<&str>) -> PathBuf {
    let dir = zk_core::paths::user_config_dir();
    match terminal_id.map(sanitize_id).filter(|id| !id.is_empty()) {
        Some(id) => dir.join(format!("{TERMINAL_HISTORY_FILE}-{id}")),
        None => dir.join(TERMINAL_HISTORY_FILE),
    }
}

/// 终端 ID 卫生化：只留字母 / 数字 / `-` / `_`，其余一律丢弃。
fn sanitize_id(raw: &str) -> String {
    raw.chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .collect()
}

/// 取文件尾部 `lines` 行；文件缺失 → 旧桩文案，读失败 → 错误结果。
async fn capture(path: &std::path::Path, lines: usize) -> ToolOutput {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() => metadata,
        // 缺失（含被目录占位）→ 旧桩语义，且不是错误：调用方据文案改走 Bash。
        _ => return ToolOutput::ok(CAPTURE_UNAVAILABLE),
    };
    let raw = match read_tail(path, metadata.len()).await {
        Ok(raw) => raw,
        Err(error) => {
            return failure(
                "TERMINAL_CAPTURE_READ_FAILED",
                format!("{}: {error}", path.display()),
            );
        }
    };
    let tail: Vec<&str> = {
        let all: Vec<&str> = raw.lines().collect();
        all[all.len().saturating_sub(lines)..].to_vec()
    };
    if tail.is_empty() {
        return ToolOutput::ok(CAPTURE_UNAVAILABLE);
    }
    let mut output = ToolOutput::ok(tail.join("\n"));
    output.metadata = Some(json!({
        "structuredResult": {
            "source": path.display().to_string(),
            "lines": tail.len(),
        }
    }));
    output
}

/// 读尾部字节（>1 MiB 时只取末 1 MiB，并按 UTF-8 边界宽容解码）。
async fn read_tail(path: &std::path::Path, len: u64) -> std::io::Result<String> {
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

    let mut file = tokio::fs::File::open(path).await?;
    if len > MAX_HISTORY_BYTES {
        let offset = i64::try_from(MAX_HISTORY_BYTES).unwrap_or(i64::MAX);
        file.seek(std::io::SeekFrom::End(-offset)).await?;
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    // 截断可能切开一个多字节字符 → 宽容解码（`from_utf8_lossy`），
    // 不因一个替换符让整次回读失败。
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    fn temp_file(tag: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-capture-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join(tag);
        std::fs::write(&path, body).expect("write");
        path
    }

    /// 历史文件缺失 → 逐字回旧桩文案，且**不是**错误。
    #[tokio::test]
    async fn missing_history_falls_back_to_legacy_stub_text() {
        let path = std::env::temp_dir().join("zk-capture-definitely-absent-history");
        let _ = std::fs::remove_file(&path);
        let output = capture(&path, 50).await;
        assert!(!output.is_error);
        assert_eq!(output.content, CAPTURE_UNAVAILABLE);
    }

    /// 取尾部 N 行，且元数据回报实际行数与来源。
    #[tokio::test]
    async fn captures_trailing_lines_only() {
        let numbered: Vec<String> = (1..=120).map(|n| format!("line {n}")).collect();
        let body = format!("{}\n", numbered.join("\n"));
        let path = temp_file("tail", &body);

        let output = capture(&path, 3).await;
        assert!(!output.is_error);
        assert_eq!(output.content, "line 118\nline 119\nline 120");
        let meta = output.metadata.expect("metadata");
        assert_eq!(meta["structuredResult"]["lines"], 3);

        // 请求行数超过文件行数 → 全量返回，不 panic。
        let all = capture(&path, 10_000).await;
        assert_eq!(all.content.lines().count(), 120);
    }

    /// 空文件（无任何行）同样落降级文案。
    #[tokio::test]
    async fn empty_history_falls_back_to_stub_text() {
        let path = temp_file("empty", "");
        assert_eq!(capture(&path, 50).await.content, CAPTURE_UNAVAILABLE);
    }

    /// `lines` 入参钳制在 `[1, MAX_CAPTURE_LINES]`（缺省 50）。
    #[tokio::test]
    async fn lines_parameter_is_clamped() {
        // 走完整 execute 路径（真实 `~/.zk/` 可能无历史文件，故只断言不 panic
        // 且结果非错误——两种分支都合法）。
        let output = TerminalCaptureTool
            .execute(json!({ "lines": 999_999 }), ctx())
            .await;
        assert!(!output.is_error, "{}", output.content);
    }

    /// `terminal_id` 卫生化：路径分隔符与 `..` 不能逃出 `~/.zk/`。
    #[test]
    fn terminal_id_cannot_escape_the_config_dir() {
        let base = zk_core::paths::user_config_dir();
        assert_eq!(history_path(None), base.join(TERMINAL_HISTORY_FILE));
        assert_eq!(
            history_path(Some("../../etc/passwd")),
            base.join(format!("{TERMINAL_HISTORY_FILE}-etcpasswd"))
        );
        assert_eq!(
            history_path(Some("tty_1-a")),
            base.join(format!("{TERMINAL_HISTORY_FILE}-tty_1-a"))
        );
        // 卫生化后为空 → 回落主文件。
        assert_eq!(history_path(Some("///")), base.join(TERMINAL_HISTORY_FILE));
    }

    /// 工具规格：只读、名与旧一致。
    #[test]
    fn spec_matches_legacy_shape() {
        let spec = TerminalCaptureTool.spec();
        assert_eq!(spec.name, "TerminalCapture");
        assert!(TerminalCaptureTool.is_read_only(&json!({})));
        assert!(spec.parameters["properties"]["lines"].is_object());
        assert!(spec.parameters["properties"]["terminal_id"].is_object());
    }
}
