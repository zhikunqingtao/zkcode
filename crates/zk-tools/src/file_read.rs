//! `Read` 工具——按行范围读取文本文件，`cat -n` 风格行号输出。
//!
//! 对照旧 `tool/impl/FileReadTool.java`（只读权威规格）：工具名 `Read`、
//! 入参 `file_path` / `offset` / `limit`、行号列宽 6 右对齐 + 制表符分隔、
//! 输出行数上限 10 000、错误码 `FILE_NOT_FOUND` / `FILE_TOO_LARGE` /
//! `FILE_BINARY_UNSUPPORTED` / `FILE_READ_IO_FAILED`。
//!
//! 差异（留痕 docs/compatibility.md §4）：旧 `MAX_SIZE_BYTES` = 200 MiB 仅
//! 用于「拒读超大文件」，本实现保留该阈值，另按交付判据对**输出内容**加
//! 1 MiB 上限截断（执行器的 1 MiB 兜底之上的工具内早停，避免先读满内存）。

use futures::future::BoxFuture;
use serde_json::json;

use crate::file_state::{self, session_key};
use crate::input::{failure, optional_usize, required_str, resolve_path};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 拒读阈值（旧 `MAX_SIZE_BYTES = 200 * 1024 * 1024` 逐字对照）。
pub const MAX_READ_FILE_BYTES: u64 = 200 * 1024 * 1024;

/// 输出行数上限（旧 `MAX_OUTPUT_LINES = 10_000` 逐字对照）。
pub const MAX_READ_OUTPUT_LINES: usize = 10_000;

/// 输出内容字节上限（交付判据：1 MiB 截断）。
pub const MAX_READ_OUTPUT_BYTES: usize = 1024 * 1024;

/// 二进制嗅探窗口（首 8 KiB 内出现 NUL 即判定二进制）。
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

/// 行号列宽（旧 `String.format("%6d\t%s", …)`）。
const LINE_NUMBER_WIDTH: usize = 6;

/// 文本文件读取工具（名 `Read`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadFileTool;

/// 行范围切片结果。
struct Slice {
    /// 已渲染文本（含行号前缀）。
    body: String,
    /// 无行号前缀的原始正文（对照旧 `String.join("\n", selectedLines)`，
    /// 用作 [`file_state`] 台账的已读快照）。
    raw: String,
    /// 实际渲染行数。
    lines: usize,
    /// 是否因行数或字节上限提前停止。
    truncated: bool,
}

impl Tool for ReadFileTool {
    fn name(&self) -> &'static str {
        "Read"
    }

    fn description(&self) -> &'static str {
        "Read a text file from the local filesystem with line numbers. \
         Supports reading a line range via offset/limit for large files."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Absolute path, or path relative to the session workspace."
                },
                "offset": {
                    "type": "integer",
                    "description": "1-based line number to start reading from (default 1)."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of lines to read (default 10000)."
                }
            },
            "required": ["file_path"]
        })
    }

    /// 只读工具（旧 `FileReadTool.java:115` `isReadOnly` → `true`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

/// 执行主体（入参校验 → 元数据体检 → 读取 → 行切片 → 结果组装）。
async fn run(input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
    let raw_path = match required_str(&input, "file_path") {
        Ok(value) => value,
        Err(output) => return output,
    };
    let path = resolve_path(raw_path, &ctx);
    let display = path.display().to_string();
    let metadata = match tokio::fs::metadata(&path).await {
        Ok(metadata) => metadata,
        Err(error) => {
            return failure("FILE_NOT_FOUND", format!("{display}: {error}"));
        }
    };
    if metadata.is_dir() {
        return failure("FILE_NOT_FOUND", format!("{display} is a directory"));
    }
    if metadata.len() > MAX_READ_FILE_BYTES {
        return failure(
            "FILE_TOO_LARGE",
            format!(
                "{display} is {} bytes, exceeding the {MAX_READ_FILE_BYTES} byte limit",
                metadata.len()
            ),
        );
    }
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) => return failure("FILE_READ_IO_FAILED", format!("{display}: {error}")),
    };
    if bytes.iter().take(BINARY_SNIFF_BYTES).any(|byte| *byte == 0) {
        return failure(
            "FILE_BINARY_UNSUPPORTED",
            format!("{display} looks like a binary file"),
        );
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();
    finish(&display, &text, &input, &ctx)
}

/// 结果组装（行切片 + 已读台账记账 + `structuredResult` 元数据，对照旧
/// metadata 七元组）。
fn finish(display: &str, text: &str, input: &serde_json::Value, ctx: &ToolContext) -> ToolOutput {
    let total_lines = text.lines().count();
    let start_line = optional_usize(input, "offset").unwrap_or(1).max(1);
    let limit = optional_usize(input, "limit")
        .unwrap_or(MAX_READ_OUTPUT_LINES)
        .clamp(1, MAX_READ_OUTPUT_LINES);
    let slice = render(text, start_line, limit);
    // 已读台账（对照旧 `cache.markRead(filePath, content, offset > 0 ? offset :
    // null, limit > 0 ? limit : null, selectedLines.size() >= MAX_OUTPUT_LINES)`）；
    // 本实现额外把「1 MiB 字节截断」也计作截断视图（旧实现无字节截断面）。
    let is_partial = slice.truncated || slice.lines >= MAX_READ_OUTPUT_LINES;
    file_state::global().mark_read(
        session_key(ctx.session_id()),
        display,
        &slice.raw,
        Some(start_line),
        Some(limit),
        is_partial,
    );
    let next_offset = start_line + slice.lines;
    let has_more = next_offset <= total_lines;
    let mut output = ToolOutput::ok(slice.body);
    output.metadata = Some(json!({
        "structuredResult": {
            "filePath": display,
            "numLines": slice.lines,
            "startLine": start_line,
            "totalLines": total_lines,
            "encoding": "UTF-8",
            "truncated": slice.truncated,
            "hasMore": has_more,
            "nextOffset": if has_more { Some(next_offset) } else { None },
        }
    }));
    output
}

/// 渲染 `[start_line, start_line + limit)` 行，附 `cat -n` 风格行号。
fn render(text: &str, start_line: usize, limit: usize) -> Slice {
    let mut body = String::new();
    let mut raw: Vec<&str> = Vec::new();
    let mut lines = 0;
    let mut truncated = false;
    for (index, line) in text.lines().enumerate().skip(start_line - 1) {
        if lines >= limit {
            truncated = true;
            break;
        }
        let rendered = format!("{:>LINE_NUMBER_WIDTH$}\t{line}\n", index + 1);
        if body.len() + rendered.len() > MAX_READ_OUTPUT_BYTES {
            truncated = true;
            break;
        }
        body.push_str(&rendered);
        raw.push(line);
        lines += 1;
    }
    Slice {
        body,
        raw: raw.join("\n"),
        lines,
        truncated,
    }
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

    async fn read(input: serde_json::Value) -> ToolOutput {
        ReadFileTool.execute(input, ctx()).await
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-read-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[tokio::test]
    async fn reads_all_lines_with_line_numbers() {
        let path = temp_dir("all").join("a.txt");
        std::fs::write(&path, "alpha\nbeta\n").expect("write");
        let output = read(json!({ "file_path": path.to_str().expect("utf8") })).await;
        assert!(!output.is_error);
        assert_eq!(output.content, "     1\talpha\n     2\tbeta\n");
        let metadata = output.metadata.expect("metadata");
        let structured = &metadata["structuredResult"];
        assert_eq!(structured["numLines"], 2);
        assert_eq!(structured["totalLines"], 2);
        assert_eq!(structured["hasMore"], false);
    }

    #[tokio::test]
    async fn honours_offset_and_limit_window() {
        let path = temp_dir("window").join("b.txt");
        std::fs::write(&path, "l1\nl2\nl3\nl4\n").expect("write");
        let output = read(json!({
            "file_path": path.to_str().expect("utf8"),
            "offset": 2,
            "limit": 2,
        }))
        .await;
        assert_eq!(output.content, "     2\tl2\n     3\tl3\n");
        let metadata = output.metadata.expect("metadata");
        let structured = &metadata["structuredResult"];
        assert_eq!(structured["startLine"], 2);
        assert_eq!(structured["hasMore"], true);
        assert_eq!(structured["nextOffset"], 4);
    }

    #[tokio::test]
    async fn rejects_missing_path_and_binary_content() {
        let missing = read(json!({})).await;
        assert!(missing.is_error);
        assert!(missing.content.starts_with("MISSING_PARAMETER: "));

        let absent = read(json!({ "file_path": "/nope/zk-does-not-exist.txt" })).await;
        assert!(absent.content.starts_with("FILE_NOT_FOUND: "));

        let path = temp_dir("binary").join("c.bin");
        std::fs::write(&path, [0x00, 0x01, 0x02]).expect("write");
        let binary = read(json!({ "file_path": path.to_str().expect("utf8") })).await;
        assert!(binary.is_error);
        assert!(binary.content.starts_with("FILE_BINARY_UNSUPPORTED: "));
    }

    #[tokio::test]
    async fn truncates_output_at_one_mib() {
        let path = temp_dir("truncate").join("d.txt");
        let line = "x".repeat(1024);
        let content = std::iter::repeat_n(line.as_str(), 2048)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, content).expect("write");
        let output = read(json!({ "file_path": path.to_str().expect("utf8") })).await;
        assert!(!output.is_error);
        assert!(output.content.len() <= MAX_READ_OUTPUT_BYTES);
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["truncated"], true);
    }
}
