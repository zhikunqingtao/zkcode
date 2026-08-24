//! `Snip` 工具——提取文件中指定范围的代码片段（Batch 7）。
//!
//! 语义来源（旧仓库只读）：`SnipTool.java`（94L）。
//! 支持行号模式（`start_line` / `end_line` 1-based 切片）与符号模式
//! （`symbol` 包含搜索 + 花括号深度追踪定位结尾 + `context_lines` 上下文）。
//!
//! # 有意差异
//!
//! - Java `Files.readAllLines` → Rust `tokio::fs::read_to_string`（异步）；
//! - Java `PathSecurityService` 路径安全检查 → Rust 侧走 `input::resolve_path`
//!   解析（完整权限面归 zk-authz 2.5 管线，与旧实现留痕一致）。

use std::fmt::Write as _;
use std::path::PathBuf;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::{failure, optional_str, optional_usize, required_str, resolve_path};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 默认上下文行数（旧 `input.getInt("context_lines", 3)`）。
const DEFAULT_CONTEXT_LINES: usize = 3;

/// 符号扫描最大行数（旧 `findSymbolEnd` 的 `startLine + 30` 上限）。
const MAX_SYMBOL_SCAN_LINES: usize = 30;

/// `Snip` 工具（名 `Snip`）——提取文件中指定范围的代码片段。
#[derive(Clone, Copy, Debug, Default)]
pub struct SnipTool;

impl Tool for SnipTool {
    fn name(&self) -> &'static str {
        "Snip"
    }

    fn description(&self) -> &'static str {
        "提取文件中指定范围的代码片段，支持行号/符号定位"
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "required": ["file_path"],
            "properties": {
                "file_path": { "type": "string", "description": "文件路径" },
                "start_line": { "type": "integer", "description": "开始行号" },
                "end_line": { "type": "integer", "description": "结束行号" },
                "symbol": { "type": "string", "description": "符号名称（函数/类/方法）" },
                "context_lines": { "type": "integer", "description": "上下文行数，默认 3" }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

async fn run(input: Value, ctx: ToolContext) -> ToolOutput {
    let raw_path = match required_str(&input, "file_path") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let path: PathBuf = resolve_path(raw_path, &ctx);

    let content = match tokio::fs::read_to_string(&path).await {
        Ok(c) => c,
        Err(e) => return failure("SNIP_EXTRACTION_FAILED", format!("代码片段提取失败: {e}")),
    };
    let lines: Vec<&str> = content.lines().collect();
    let total = lines.len();

    let (start, end) = if optional_str(&input, "symbol").is_some() {
        // 符号模式
        let symbol = optional_str(&input, "symbol").unwrap();
        let ctx_lines = optional_usize(&input, "context_lines").unwrap_or(DEFAULT_CONTEXT_LINES);
        let Some(sym_line) = find_symbol_line(&lines, symbol) else {
            return failure("SNIP_SYMBOL_NOT_FOUND", format!("未找到符号: {symbol}"));
        };
        let sym_end = find_symbol_end(&lines, sym_line);
        let s = sym_line.saturating_sub(ctx_lines);
        let e = (sym_end + ctx_lines).min(total.saturating_sub(1));
        (s, e)
    } else {
        // 行号模式（1-based → 0-based）
        let s = optional_usize(&input, "start_line")
            .unwrap_or(1)
            .saturating_sub(1);
        let e = optional_usize(&input, "end_line")
            .unwrap_or(total)
            .saturating_sub(1);
        (s, e)
    };

    let start = start.min(total.saturating_sub(1));
    let end = end.min(total.saturating_sub(1));

    let mut output = String::new();
    for (idx, line) in lines.iter().enumerate().skip(start).take(end - start + 1) {
        let _ = writeln!(output, "{:>6}\u{2502} {}", idx + 1, line);
    }
    ToolOutput::ok(output)
}

/// 在全部行中搜索包含 `symbol` 的首行（0-based）。
fn find_symbol_line(lines: &[&str], symbol: &str) -> Option<usize> {
    lines.iter().position(|line| line.contains(symbol))
}

/// 从 `start_line` 开始做花括号深度追踪，返回符号结尾行（0-based）。
/// 最大扫描 `MAX_SYMBOL_SCAN_LINES` 行。
#[allow(clippy::needless_range_loop)]
fn find_symbol_end(lines: &[&str], start_line: usize) -> usize {
    let mut depth: i32 = 0;
    let limit = (start_line + MAX_SYMBOL_SCAN_LINES).min(lines.len());
    for i in start_line..limit {
        for ch in lines[i].chars() {
            if ch == '{' {
                depth += 1;
            } else if ch == '}' {
                depth -= 1;
            }
        }
        if depth <= 0 && i > start_line {
            return i;
        }
    }
    limit.saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx(working_dir: &str) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(working_dir)
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-snip-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[tokio::test]
    async fn line_range_mode() {
        let dir = temp_dir("range");
        let path = dir.join("a.rs");
        std::fs::write(&path, "line1\nline2\nline3\nline4\nline5\n").expect("write");
        let tool = SnipTool;
        let output = tool
            .execute(
                json!({
                    "file_path": path.to_str().unwrap(),
                    "start_line": 2,
                    "end_line": 4
                }),
                ctx(dir.to_str().unwrap()),
            )
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("line2"));
        assert!(output.content.contains("line4"));
        assert!(!output.content.contains("line1"));
    }

    #[tokio::test]
    async fn symbol_mode() {
        let dir = temp_dir("sym");
        let path = dir.join("b.rs");
        std::fs::write(
            &path,
            "// header\nfn hello() {\n    println!(\"hi\");\n}\n// footer\n",
        )
        .expect("write");
        let tool = SnipTool;
        let output = tool
            .execute(
                json!({
                    "file_path": path.to_str().unwrap(),
                    "symbol": "fn hello"
                }),
                ctx(dir.to_str().unwrap()),
            )
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("fn hello"));
    }

    #[tokio::test]
    async fn missing_file_yields_error() {
        let tool = SnipTool;
        let output = tool
            .execute(json!({ "file_path": "/nope/zk-missing.txt" }), ctx("/tmp"))
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("SNIP_EXTRACTION_FAILED"));
    }

    #[tokio::test]
    async fn symbol_not_found() {
        let dir = temp_dir("nosym");
        let path = dir.join("c.rs");
        std::fs::write(&path, "fn foo() {}\n").expect("write");
        let tool = SnipTool;
        let output = tool
            .execute(
                json!({
                    "file_path": path.to_str().unwrap(),
                    "symbol": "nonexistent_fn"
                }),
                ctx(dir.to_str().unwrap()),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("SNIP_SYMBOL_NOT_FOUND"));
    }
}
