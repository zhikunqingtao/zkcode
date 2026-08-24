//! `NotebookEdit` 工具——Jupyter Notebook（`.ipynb`）的 cell 级编辑（Batch 8E）。
//!
//! 语义来源（旧仓库只读）：`tool/notebook/NotebookEditTool.java`（318L）——
//! 五个命令（`add_cell` / `edit_cell` / `delete_cell` / `move_cell` /
//! `change_cell_type`）、`MAX_NOTEBOOK_BYTES = 100MiB`、
//! 「读全文 → 取 `expectedHash` → 改 `cells` → `writerWithDefaultPrettyPrinter`
//! → CAS 原子写」的流水线、`toSourceArray` 的 nbformat source 数组口径、
//! `createCell` 的 nbformat 4.5 字段集（含 8 位 `id`）、
//! `PermissionRequirement.ALWAYS_ASK` + `isReadOnly = false` +
//! `isConcurrencySafe = false`、成功文案 `"Notebook updated: <detail>"`、
//! 错误码 `NOTEBOOK_NOT_REGULAR_FILE` / `NOTEBOOK_SIZE_LIMIT` /
//! `NOTEBOOK_CELLS_MISSING` / `NOTEBOOK_INPUT_INVALID` /
//! `NOTEBOOK_EDIT_IO_FAILED` / `NOTEBOOK_ATOMIC_WRITE_FAILED`。
//!
//! # 有意差异
//!
//! - 旧入参名 `notebook_path` / `command` / `cell_index`；本移植**同时**接受
//!   `path` / `action` / `index` 别名，并把 `insert_cell` → `add_cell`、
//!   `replace_cell` → `edit_cell` 作为命令别名；`move_cell` 除旧
//!   `direction` ∈ {up, down} 外还接受 `target_index` 直接寻址。旧口径零回归。
//! - 旧 `ManagedWorkspacePathResolver.resolveAuthorizedExecutionProspective`
//!   的工作区准入 → 本 workspace 由 2.5 的权限管线在工具**之前**裁决，工具内
//!   仅走 [`crate::input::resolve_path`]（与 `Edit` / `Write` 同口径）。
//! - 旧 `AtomicFileWriter` 的双层锁（JVM 路径锁 + `FileChannel.tryLock`）→
//!   本移植复用 [`crate::atomic::write_checked`]（同路径互斥 + CAS + 回读校验），
//!   跨进程文件锁未移植（留痕：单进程服务端，旧 Layer 2 为多实例保留）。
//! - `serde_json` 未启用 `preserve_order`，故写回时对象键按字典序排列；nbformat
//!   的规范键序恰为字典序（`cell_type` / `execution_count` / `id` / `metadata`
//!   / `outputs` / `source`），且 JSON 对象无序，Jupyter 读取不受影响。

use std::path::Path;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Map, Value, json};

use crate::atomic::{ExpectedOldState, sha256_hex, write_checked};
use crate::input::{failure, optional_str, optional_usize, required_str, resolve_path};
use crate::snapshot::{MAX_SNAPSHOT_BYTES, SnapshotRequest, SnapshotSink};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// Notebook 体积上限（对照旧 `MAX_NOTEBOOK_BYTES = 100L * 1024 * 1024`）。
pub const MAX_NOTEBOOK_BYTES: u64 = 100 * 1024 * 1024;

/// 支持的 cell 类型（nbformat 的 `raw` 亦合法，故不做白名单硬拒——与旧
/// `changeCellType` 直接 `put` 的宽容口径一致）。
pub const CELL_TYPES: [&str; 3] = ["code", "markdown", "raw"];

/// `NotebookEdit` 工具（名 `NotebookEdit`）。
#[derive(Clone, Default)]
pub struct NotebookEditTool {
    sink: Option<Arc<dyn SnapshotSink>>,
}

impl std::fmt::Debug for NotebookEditTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NotebookEditTool")
            .field("snapshot_sink", &self.sink.is_some())
            .finish()
    }
}

impl NotebookEditTool {
    /// Constructs a `NotebookEdit` tool without persistence (primarily for isolated tests).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Injects the same durable pre-write snapshot sink used by Write and Edit.
    #[must_use]
    pub fn with_snapshot_sink(sink: Arc<dyn SnapshotSink>) -> Self {
        Self { sink: Some(sink) }
    }

    async fn capture(&self, ctx: &ToolContext, path: &Path, content: &str) -> bool {
        let (Some(sink), Some(session_id)) = (self.sink.as_ref(), ctx.session_id()) else {
            return false;
        };
        if content.len() > MAX_SNAPSHOT_BYTES {
            tracing::warn!(path = %path.display(), bytes = content.len(), "snapshot skipped: too large");
            return false;
        }
        let request = SnapshotRequest {
            session_id: session_id.to_owned(),
            message_id: ctx.tool_use_id().map(str::to_owned),
            file_path: path.to_string_lossy().into_owned(),
            content: content.to_owned(),
            operation: "notebook_edit".to_owned(),
        };
        match sink.capture(request).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "snapshot persist failed");
                false
            }
        }
    }
}

impl Tool for NotebookEditTool {
    fn name(&self) -> &'static str {
        "NotebookEdit"
    }

    fn description(&self) -> &'static str {
        "Edit Jupyter Notebook (.ipynb) files. Supports adding, editing, \
         deleting, moving cells and changing cell types."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "required": ["notebook_path", "command"],
            "properties": {
                "notebook_path": {
                    "type": "string",
                    "description": "Path to the .ipynb file（别名 path）"
                },
                "command": {
                    "type": "string",
                    "enum": [
                        "add_cell", "edit_cell", "delete_cell", "move_cell", "change_cell_type",
                        "insert_cell", "replace_cell"
                    ],
                    "description": "Operation to perform（别名 action；insert_cell/replace_cell 等价 add_cell/edit_cell）"
                },
                "cell_index": {
                    "type": "integer",
                    "description": "Index of the cell to operate on（0-based，别名 index）"
                },
                "content": {
                    "type": "string",
                    "description": "Content for add/edit operations"
                },
                "cell_type": {
                    "type": "string",
                    "enum": CELL_TYPES,
                    "description": "Cell 类型（add_cell 默认 code；change_cell_type 必填）"
                },
                "direction": {
                    "type": "string",
                    "enum": ["up", "down"],
                    "description": "move_cell 方向（与 target_index 二选一）"
                },
                "target_index": {
                    "type": "integer",
                    "description": "move_cell 目标位置（与 direction 二选一）"
                }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        false
    }

    fn path_of(&self, input: &Value) -> Option<String> {
        notebook_path(input).map(ToOwned::to_owned)
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.run(input, ctx).await })
    }
}

fn notebook_path(input: &Value) -> Option<&str> {
    optional_str(input, "notebook_path").or_else(|| optional_str(input, "path"))
}

/// 命令别名归一（`insert_cell` → `add_cell`、`replace_cell` → `edit_cell`）。
fn canonical_command(raw: &str) -> &str {
    match raw {
        "insert_cell" => "add_cell",
        "replace_cell" => "edit_cell",
        other => other,
    }
}

fn cell_index(input: &Value) -> Option<usize> {
    optional_usize(input, "cell_index").or_else(|| optional_usize(input, "index"))
}

/// 读盘 + 前置闸门 + 解析（对照旧 `call` 的 `isRegularFile` / `size` /
/// `readTree` 三步；失败即返回可直接下行的 [`ToolOutput`]）。
async fn load_notebook(path: &Path, raw_path: &str) -> Result<(Value, String, String), ToolOutput> {
    // 对照旧 `Files.isRegularFile(NOFOLLOW_LINKS) || isSymbolicLink` 双闸门。
    let Ok(metadata) = tokio::fs::symlink_metadata(path).await else {
        return Err(failure(
            "NOTEBOOK_NOT_REGULAR_FILE",
            format!("Notebook is not a regular file: {raw_path}"),
        ));
    };
    if !metadata.is_file() {
        return Err(failure(
            "NOTEBOOK_NOT_REGULAR_FILE",
            format!("Notebook is not a regular file: {raw_path}"),
        ));
    }
    if metadata.len() > MAX_NOTEBOOK_BYTES {
        return Err(failure(
            "NOTEBOOK_SIZE_LIMIT",
            format!("Notebook exceeds 100MiB limit: {raw_path}"),
        ));
    }
    let original = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(failure(
                "NOTEBOOK_EDIT_IO_FAILED",
                format!("Notebook edit failed: {error}"),
            ));
        }
    };
    let expected_hash = sha256_hex(&original);
    match serde_json::from_slice(&original) {
        Ok(value) => Ok((
            value,
            expected_hash,
            String::from_utf8(original).expect("valid JSON is valid UTF-8"),
        )),
        Err(error) => Err(failure(
            "NOTEBOOK_INPUT_INVALID",
            format!("Invalid notebook JSON: {error}"),
        )),
    }
}

impl NotebookEditTool {
    async fn run(&self, input: Value, ctx: ToolContext) -> ToolOutput {
        let Some(raw_path) = notebook_path(&input) else {
            return failure(
                "MISSING_PARAMETER",
                "Required parameter 'notebook_path' is missing or not a non-empty string",
            );
        };
        let Ok(raw_command) =
            required_str(&input, "command").or_else(|_| required_str(&input, "action"))
        else {
            return failure(
                "MISSING_PARAMETER",
                "Required parameter 'command' is missing or not a non-empty string",
            );
        };
        let command = canonical_command(raw_command);

        let path = resolve_path(raw_path, &ctx);
        let (mut notebook, expected_hash, original) = match load_notebook(&path, raw_path).await {
            Ok(loaded) => loaded,
            Err(rejection) => return rejection,
        };
        let Some(cells) = notebook.get_mut("cells").and_then(Value::as_array_mut) else {
            return failure(
                "NOTEBOOK_CELLS_MISSING",
                "Invalid notebook: no 'cells' array found",
            );
        };

        let detail = match apply(command, &input, cells) {
            Ok(detail) => detail,
            Err(message) => return failure("NOTEBOOK_INPUT_INVALID", message),
        };

        // 旧 `writerWithDefaultPrettyPrinter`（2 空格缩进）。
        let Ok(rendered) = serde_json::to_string_pretty(&notebook) else {
            return failure(
                "NOTEBOOK_EDIT_IO_FAILED",
                "Notebook edit failed: could not serialize notebook",
            );
        };
        // Capture the exact bytes read for the CAS edit before replacing them. Snapshot
        // persistence is best-effort, matching Write/Edit, while rewind remains durable
        // whenever a session-scoped sink is available.
        let snapshot_captured = self.capture(&ctx, &path, &original).await;
        let outcome =
            write_checked(&path, &rendered, &ExpectedOldState::sha256(&expected_hash)).await;
        if !outcome.success {
            return failure(
                "NOTEBOOK_ATOMIC_WRITE_FAILED",
                outcome
                    .error
                    .unwrap_or_else(|| "atomic write failed".to_owned()),
            );
        }

        ToolOutput {
            content: format!("Notebook updated: {detail}"),
            is_error: false,
            metadata: Some(json!({
                "sealedHash": outcome.new_hash,
                "command": command,
                "detail": detail,
                "snapshotCaptured": snapshot_captured,
            })),
        }
    }
}

/// 分派五个命令（错误以 `Err(String)` 表达旧 `IllegalArgumentException`）。
fn apply(command: &str, input: &Value, cells: &mut Vec<Value>) -> Result<String, String> {
    match command {
        "add_cell" => Ok(add_cell(input, cells)),
        "edit_cell" => edit_cell(input, cells),
        "delete_cell" => delete_cell(input, cells),
        "move_cell" => move_cell(input, cells),
        "change_cell_type" => change_cell_type(input, cells),
        other => Err(format!("Unknown command: {other}")),
    }
}

/// 对照旧 `addCell`：`cell_index` 缺省即追加到末尾，越界钳到末尾。
fn add_cell(input: &Value, cells: &mut Vec<Value>) -> String {
    let cell_type = optional_str(input, "cell_type").unwrap_or("code");
    let content = input
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let index = cell_index(input).unwrap_or(cells.len()).min(cells.len());
    cells.insert(index, create_cell(cell_type, content));
    format!("add_cell at index {index}")
}

/// 对照旧 `editCell`：替换 `source`，code cell 顺带清执行状态。
fn edit_cell(input: &Value, cells: &mut [Value]) -> Result<String, String> {
    let index = require_index(input, cells.len())?;
    let content = input
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| "'content' is required for edit_cell".to_owned())?;
    let cell = cells[index]
        .as_object_mut()
        .ok_or_else(|| format!("Cell {index} is not an object"))?;
    cell.insert("source".to_owned(), to_source_array(content));
    if cell.get("cell_type").and_then(Value::as_str) == Some("code") {
        cell.insert("execution_count".to_owned(), Value::Null);
        cell.insert("outputs".to_owned(), Value::Array(Vec::new()));
    }
    Ok(format!("edit_cell at index {index}"))
}

fn delete_cell(input: &Value, cells: &mut Vec<Value>) -> Result<String, String> {
    let index = require_index(input, cells.len())?;
    cells.remove(index);
    Ok(format!("delete_cell at index {index}"))
}

/// 对照旧 `moveCell`：`direction` 上下各一格；本移植额外支持 `target_index`。
fn move_cell(input: &Value, cells: &mut Vec<Value>) -> Result<String, String> {
    let from = require_index(input, cells.len())?;
    let to = if let Some(target) = optional_usize(input, "target_index") {
        target
    } else {
        let direction = optional_str(input, "direction")
            .ok_or_else(|| "'direction' or 'target_index' is required for move_cell".to_owned())?;
        match direction {
            "up" => from
                .checked_sub(1)
                .ok_or_else(|| "Cannot move cell up: target index -1 out of range".to_owned())?,
            "down" => from + 1,
            other => return Err(format!("Unknown direction: {other}. Use: up or down.")),
        }
    };
    if to >= cells.len() {
        return Err(format!("Cannot move cell: target index {to} out of range"));
    }
    let moving = cells.remove(from);
    cells.insert(to, moving);
    Ok(format!("move_cell from {from} to {to}"))
}

/// 对照旧 `changeCellType`：markdown → code 补 `outputs` / `execution_count`，
/// code → markdown 移除二者。
fn change_cell_type(input: &Value, cells: &mut [Value]) -> Result<String, String> {
    let index = require_index(input, cells.len())?;
    let new_type = optional_str(input, "cell_type")
        .ok_or_else(|| "'cell_type' is required for change_cell_type".to_owned())?
        .to_owned();
    let cell = cells[index]
        .as_object_mut()
        .ok_or_else(|| format!("Cell {index} is not an object"))?;
    cell.insert("cell_type".to_owned(), Value::String(new_type.clone()));
    if new_type == "code" && !cell.contains_key("outputs") {
        cell.insert("outputs".to_owned(), Value::Array(Vec::new()));
        cell.insert("execution_count".to_owned(), Value::Null);
    }
    if new_type == "markdown" {
        cell.remove("outputs");
        cell.remove("execution_count");
    }
    Ok(format!("change_cell_type at index {index} to {new_type}"))
}

/// 必填索引 + 越界拒绝（对照旧 `idx < 0 || idx >= cells.size()`；负数在 Rust
/// 侧由 [`optional_usize`] 的非负口径过滤，落同一句 out of range）。
fn require_index(input: &Value, len: usize) -> Result<usize, String> {
    let index = cell_index(input)
        .ok_or_else(|| "'cell_index' is required and must be a non-negative integer".to_owned())?;
    if index >= len {
        return Err(format!("Cell index out of range: {index}"));
    }
    Ok(index)
}

/// 新建 cell（对照旧 `createCell`：nbformat 4.5 字段集 + 8 位 `id`）。
fn create_cell(cell_type: &str, source: &str) -> Value {
    let mut cell = Map::new();
    cell.insert("cell_type".to_owned(), Value::String(cell_type.to_owned()));
    cell.insert("source".to_owned(), to_source_array(source));
    cell.insert("metadata".to_owned(), Value::Object(Map::new()));
    let id = uuid::Uuid::new_v4().to_string()[..8].to_owned();
    cell.insert("id".to_owned(), Value::String(id));
    if cell_type == "code" {
        cell.insert("outputs".to_owned(), Value::Array(Vec::new()));
        cell.insert("execution_count".to_owned(), Value::Null);
    }
    Value::Object(cell)
}

/// nbformat `source` 数组（对照旧 `toSourceArray`：按 `\n` 切分且非末行补回
/// 换行；空串 → 空数组）。
fn to_source_array(content: &str) -> Value {
    if content.is_empty() {
        return Value::Array(Vec::new());
    }
    let parts: Vec<&str> = content.split('\n').collect();
    let last = parts.len() - 1;
    Value::Array(
        parts
            .iter()
            .enumerate()
            .map(|(index, line)| {
                if index < last {
                    Value::String(format!("{line}\n"))
                } else {
                    Value::String((*line).to_owned())
                }
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx(working_dir: &str) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(working_dir)
    }

    #[derive(Default)]
    struct RecordingSink {
        seen: Mutex<Vec<SnapshotRequest>>,
    }

    impl SnapshotSink for RecordingSink {
        fn capture(&self, request: SnapshotRequest) -> BoxFuture<'_, Result<(), String>> {
            self.seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            Box::pin(futures::future::ready(Ok(())))
        }
    }

    fn temp_notebook(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-nb-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("book.ipynb");
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "cells": [
                    { "cell_type": "code", "source": ["print(1)\n"], "metadata": {},
                      "outputs": [{ "text": "1" }], "execution_count": 3 },
                    { "cell_type": "markdown", "source": ["# title"], "metadata": {} }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5
            }))
            .expect("seed json"),
        )
        .expect("seed notebook");
        path
    }

    fn cells_of(path: &PathBuf) -> Vec<Value> {
        let raw = std::fs::read_to_string(path).expect("read back");
        let value: Value = serde_json::from_str(&raw).expect("valid json");
        value["cells"].as_array().expect("cells").clone()
    }

    #[tokio::test]
    async fn insert_alias_adds_a_code_cell_with_nbformat_fields() {
        let path = temp_notebook("insert");
        let output = NotebookEditTool::new()
            .execute(
                json!({
                    "path": path.to_str().unwrap(),
                    "action": "insert_cell",
                    "index": 1,
                    "content": "a = 1\nb = 2"
                }),
                ctx("/tmp"),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "Notebook updated: add_cell at index 1");
        assert!(output.metadata.expect("metadata")["sealedHash"].is_string());

        let cells = cells_of(&path);
        assert_eq!(cells.len(), 3);
        assert_eq!(cells[1]["cell_type"], "code");
        assert_eq!(cells[1]["source"], json!(["a = 1\n", "b = 2"]));
        assert!(cells[1]["execution_count"].is_null());
        assert_eq!(cells[1]["outputs"], json!([]));
        assert_eq!(
            cells[1]["id"].as_str().expect("cell id").len(),
            8,
            "nbformat 4.5 cell id"
        );
    }

    #[tokio::test]
    async fn replace_alias_resets_code_execution_state() {
        let path = temp_notebook("replace");
        let output = NotebookEditTool::new()
            .execute(
                json!({
                    "notebook_path": path.to_str().unwrap(),
                    "command": "replace_cell",
                    "cell_index": 0,
                    "content": "print(2)"
                }),
                ctx("/tmp"),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        let cells = cells_of(&path);
        assert_eq!(cells[0]["source"], json!(["print(2)"]));
        assert_eq!(cells[0]["outputs"], json!([]));
        assert!(cells[0]["execution_count"].is_null());
    }

    #[tokio::test]
    async fn delete_and_move_reorder_cells() {
        let path = temp_notebook("delete-move");
        let deleted = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "delete_cell", "index": 1 }),
                ctx("/tmp"),
            )
            .await;
        assert_eq!(deleted.content, "Notebook updated: delete_cell at index 1");
        assert_eq!(cells_of(&path).len(), 1);

        let path = temp_notebook("move");
        let moved = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "move_cell",
                        "index": 0, "direction": "down" }),
                ctx("/tmp"),
            )
            .await;
        assert_eq!(moved.content, "Notebook updated: move_cell from 0 to 1");
        let cells = cells_of(&path);
        assert_eq!(cells[0]["cell_type"], "markdown");

        let path = temp_notebook("move-target");
        let targeted = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "move_cell",
                        "index": 1, "target_index": 0 }),
                ctx("/tmp"),
            )
            .await;
        assert_eq!(targeted.content, "Notebook updated: move_cell from 1 to 0");
    }

    #[tokio::test]
    async fn change_cell_type_toggles_output_fields() {
        let path = temp_notebook("retype");
        let to_markdown = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "change_cell_type",
                        "index": 0, "cell_type": "markdown" }),
                ctx("/tmp"),
            )
            .await;
        assert!(!to_markdown.is_error, "{}", to_markdown.content);
        let cells = cells_of(&path);
        assert_eq!(cells[0]["cell_type"], "markdown");
        assert!(cells[0].get("outputs").is_none());
        assert!(cells[0].get("execution_count").is_none());

        let to_code = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "change_cell_type",
                        "index": 1, "cell_type": "code" }),
                ctx("/tmp"),
            )
            .await;
        assert!(!to_code.is_error, "{}", to_code.content);
        let cells = cells_of(&path);
        assert_eq!(cells[1]["outputs"], json!([]));
        assert!(cells[1]["execution_count"].is_null());
    }

    #[tokio::test]
    async fn out_of_range_index_and_unknown_command_are_rejected() {
        let path = temp_notebook("reject");
        let out_of_range = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "delete_cell", "index": 9 }),
                ctx("/tmp"),
            )
            .await;
        assert!(out_of_range.is_error);
        assert_eq!(
            out_of_range.content,
            "NOTEBOOK_INPUT_INVALID: Cell index out of range: 9"
        );

        let unknown = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "burn_cell", "index": 0 }),
                ctx("/tmp"),
            )
            .await;
        assert!(unknown.is_error);
        assert!(
            unknown
                .content
                .starts_with("NOTEBOOK_INPUT_INVALID: Unknown command: burn_cell")
        );
    }

    #[tokio::test]
    async fn missing_file_and_missing_cells_are_rejected() {
        let missing = NotebookEditTool::new()
            .execute(
                json!({ "path": "/nope/zk-missing.ipynb", "action": "delete_cell", "index": 0 }),
                ctx("/tmp"),
            )
            .await;
        assert!(missing.is_error);
        assert!(missing.content.starts_with("NOTEBOOK_NOT_REGULAR_FILE: "));

        let dir = std::env::temp_dir().join(format!("zk-nb-nocells-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("bad.ipynb");
        std::fs::write(&path, "{\"metadata\": {}}").expect("seed");
        let no_cells = NotebookEditTool::new()
            .execute(
                json!({ "path": path.to_str().unwrap(), "action": "delete_cell", "index": 0 }),
                ctx("/tmp"),
            )
            .await;
        assert!(no_cells.is_error);
        assert!(no_cells.content.starts_with("NOTEBOOK_CELLS_MISSING: "));
    }

    #[tokio::test]
    async fn missing_parameters_are_rejected() {
        for input in [
            json!({ "action": "delete_cell", "index": 0 }),
            json!({ "path": "/tmp/x.ipynb" }),
        ] {
            let output = NotebookEditTool::new().execute(input, ctx("/tmp")).await;
            assert!(output.is_error);
            assert!(output.content.starts_with("MISSING_PARAMETER: "));
        }
    }

    #[test]
    fn source_array_follows_the_nbformat_convention() {
        assert_eq!(to_source_array(""), json!([]));
        assert_eq!(to_source_array("one"), json!(["one"]));
        assert_eq!(to_source_array("a\nb\n"), json!(["a\n", "b\n", ""]));
    }

    #[test]
    fn declares_mutating_nature_and_path_hint() {
        let input = json!({ "notebook_path": "/tmp/a.ipynb", "command": "delete_cell" });
        let tool = NotebookEditTool::new();
        assert!(!tool.is_read_only(&input));
        assert_eq!(tool.path_of(&input).as_deref(), Some("/tmp/a.ipynb"));
        assert_eq!(
            tool.path_of(&json!({ "path": "b.ipynb" })).as_deref(),
            Some("b.ipynb")
        );
    }

    #[tokio::test]
    async fn captures_exact_pre_edit_notebook_with_session_and_tool_use() {
        let path = temp_notebook("snapshot");
        let original = std::fs::read_to_string(&path).expect("original");
        let sink = Arc::new(RecordingSink::default());
        let tool = NotebookEditTool::with_snapshot_sink(Arc::clone(&sink) as Arc<dyn SnapshotSink>);
        let (tx, _rx) = mpsc::unbounded_channel();
        let context = ToolContext::new(CancellationToken::new(), tx)
            .with_working_dir("/tmp")
            .with_session_id("session-1")
            .with_tool_use_id("tool-use-1");

        let output = tool
            .execute(
                json!({ "path": path, "action": "delete_cell", "index": 1 }),
                context,
            )
            .await;

        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.metadata.expect("metadata")["snapshotCaptured"], true);
        let seen = sink.seen.lock().expect("seen");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].session_id, "session-1");
        assert_eq!(seen[0].message_id.as_deref(), Some("tool-use-1"));
        assert_eq!(seen[0].file_path, path.to_string_lossy());
        assert_eq!(seen[0].content, original);
        assert_eq!(seen[0].operation, "notebook_edit");
    }
}
