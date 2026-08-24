//! `Edit` 工具——精确字符串替换（唯一性校验 + 5 策略容错匹配 + CAS 原子写）。
//!
//! 逐字对照旧 `tool/impl/FileEditTool.java`（只读权威规格）：工具名 `Edit`、
//! 入参 `file_path` / `old_string` / `new_string` / `replace_all`、`1GB` 编辑
//! 上限、读前置门禁（`FILE_READ_REQUIRED` / `FILE_READ_STATE_STALE`）、
//! 「匹配数 > 1 且未开 `replace_all` 即拒」的唯一性校验、写前 SHA-256 冲突
//! 检测（Conflict-Abort）、经 [`crate::atomic`] 的 CAS 原子写、post-commit
//! 快照 + unified diff（3 行上下文）与 8 项结果元数据。
//!
//! 差异（留痕 docs/compatibility.md §9）：
//!
//! - 旧 `PathSecurityService.inspectAuthorizedExecutionWritePermission` 前置
//!   拒绝（`FILE_WRITE_PATH_DENIED`）未移植——路径准入由 2.5 的
//!   `ToolExecutionGateway` 在工具**之前**承担；同理 `outsideAuthorized` 分支
//!   与其 `OUTSIDE_AUTHORIZED_RESOURCE` 历史错误码不产生。
//! - 旧 `KeyFileTracker.trackFileReference` finally 埋点未移植（本系统尚无
//!   等价的关键文件追踪面）。
//! - 旧 `FileVersionTracker` 的进程级版本表未移植：`checkBeforeWrite` 在
//!   `FileEditTool` 内的调用序恒为「`recordRead`（写入本次读到的 hash，
//!   `lastEditor = null`）→ `checkBeforeWrite(expectedHash)`」，等价于
//!   「重读磁盘并与刚读到的内容比对」，故本实现自包含还原该语义；也因此
//!   `"\nLast editor: …"` 尾行在旧实现中恒不出现，本实现不产出该行。
//! - 旧 `countMatches` 在 `search` 为空串时 `idx += 0` 死循环（`old_string`
//!   为空且文件已存在即触发）。本实现按「空串在每个字符边界匹配一次」计数
//!   （非空文件 → 多匹配 → `FILE_EDIT_MATCH_AMBIGUOUS` 终止；空文件 → 单
//!   匹配 → 在开头插入），错误码同族、无新增码，且不会挂死。
//! - 容错匹配策略 2 的「归一化后反查原文子串」旧实现按 UTF-16 code unit
//!   取下标，本实现按 Unicode 标量取下标（弯引号均为 BMP 字符，两者对 BMP
//!   文本完全一致；仅当正文含星光平面字符时下标口径不同）。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;
use similar::TextDiff;

use crate::atomic::{ExpectedOldState, WriteOutcome, sha256_hex, write_checked};
use crate::file_state::{self, session_key};
use crate::input::{bool_or, failure, required_str, required_str_allow_empty, resolve_path};
use crate::snapshot::{MAX_SNAPSHOT_BYTES, SnapshotRequest, SnapshotSink};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 可编辑文件大小上限（旧 `MAX_EDIT_FILE_SIZE = 1024L * 1024 * 1024`）。
pub const MAX_EDIT_FILE_BYTES: u64 = 1024 * 1024 * 1024;

/// unified diff 上下文行数（旧 `generateUnifiedDiff(…, 3)`）。
const DIFF_CONTEXT_RADIUS: usize = 3;

/// 快照 operation 列写入值（旧调用点逐字传 `"edit"`）。
const SNAPSHOT_OPERATION: &str = "edit";

/// 匹配未命中时的多行提示（旧文案逐字，含策略链与三条建议）。
const MATCH_NOT_FOUND_HINT: &str = "\
Attempted strategies: exact → quote-normalization → trailing-whitespace → newline-normalization → tab/space-normalization
Suggestions:
  1. Use Read tool to re-read the file and verify the content
  2. Ensure old_string matches exactly (including whitespace/indentation)
  3. Try a smaller, more unique substring";

/// 定点编辑工具（名 `Edit`）。
#[derive(Clone, Default)]
pub struct EditFileTool {
    /// post-commit 快照出口；`None` = 不落快照（单测 / 无会话上下文场景）。
    sink: Option<Arc<dyn SnapshotSink>>,
}

impl std::fmt::Debug for EditFileTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EditFileTool")
            .field("snapshot_sink", &self.sink.is_some())
            .finish()
    }
}

impl EditFileTool {
    /// 装配（无快照出口）。
    #[must_use]
    pub fn new() -> Self {
        Self { sink: None }
    }

    /// 装配并注入快照出口（组合根提供 zk-db 实现）。
    #[must_use]
    pub fn with_snapshot_sink(sink: Arc<dyn SnapshotSink>) -> Self {
        Self { sink: Some(sink) }
    }
}

impl Tool for EditFileTool {
    fn name(&self) -> &'static str {
        "Edit"
    }

    fn description(&self) -> &'static str {
        "Make a targeted edit to a file by replacing a specific string with a new string. \
         Requires the file to have been read first."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": { "type": "string", "description": "Absolute path to the file" },
                "old_string": { "type": "string", "description": "The string to replace" },
                "new_string": { "type": "string", "description": "The replacement string" },
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace all occurrences (default false)"
                }
            },
            "required": ["file_path", "old_string", "new_string"]
        })
    }

    /// 自报路径入参（旧 `FileEditTool.java:122-124`）。
    fn path_of(&self, input: &serde_json::Value) -> Option<String> {
        input
            .get("file_path")
            .and_then(serde_json::Value::as_str)
            .map(std::borrow::ToOwned::to_owned)
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.run(input, ctx).await })
    }
}

impl EditFileTool {
    /// 执行主体（取参 → 基础校验 → 读前置门禁 → 新建 / 编辑分派）。
    async fn run(&self, input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
        // 取参序与旧实现一致：old_string / new_string / replace_all 先取，
        // 缺失即以校验失败短路（旧 `getString` 抛 `ToolInputValidationException`）。
        let old_string = match required_str_allow_empty(&input, "old_string") {
            Ok(value) => value,
            Err(output) => return output,
        };
        let new_string = match required_str_allow_empty(&input, "new_string") {
            Ok(value) => value,
            Err(output) => return output,
        };
        let replace_all = bool_or(&input, "replace_all", false);
        let raw_path = match required_str(&input, "file_path") {
            Ok(value) => value,
            Err(output) => return output,
        };
        let path = resolve_path(raw_path, &ctx);
        let file_path = path.display().to_string();

        if old_string == new_string {
            return failure(
                "FILE_EDIT_NO_CHANGE",
                "old_string and new_string are identical. No changes to make.",
            );
        }

        // Read-before-Edit + 外部改动过期检测（旧 §11.5.9 前置检查）。
        let session = session_key(ctx.session_id());
        if !old_string.is_empty() {
            let store = file_state::global();
            if !store.has_been_read(session, &file_path) {
                return failure("FILE_READ_REQUIRED", "请先使用 Read 工具读取文件内容");
            }
            if store.is_stale(session, &file_path) {
                return failure("FILE_READ_STATE_STALE", "文件已被外部修改，请重新 Read");
            }
        }

        if tokio::fs::symlink_metadata(&path).await.is_err() {
            if old_string.is_empty() {
                return create_file(&path, &file_path, new_string, session).await;
            }
            return failure(
                "FILE_NOT_FOUND",
                format!("File does not exist: {file_path}"),
            );
        }
        self.apply_edit(EditRequest {
            path: &path,
            file_path: &file_path,
            old_string,
            new_string,
            replace_all,
            session,
            ctx: &ctx,
        })
        .await
    }

    /// 已存在文件的编辑主体（读 → 匹配 → 唯一性 → 冲突检测 → 原子写 → 记账）。
    async fn apply_edit(&self, request: EditRequest<'_>) -> ToolOutput {
        let EditRequest {
            path,
            file_path,
            old_string,
            new_string,
            replace_all,
            session,
            ctx,
        } = request;
        match tokio::fs::metadata(path).await {
            Ok(metadata) if metadata.len() > MAX_EDIT_FILE_BYTES => {
                return failure(
                    "FILE_EDIT_SIZE_LIMIT",
                    "File too large (>1GB). Cannot edit.",
                );
            }
            Ok(_) => {}
            Err(error) => {
                return failure(
                    "FILE_EDIT_IO_FAILED",
                    format!("Failed to edit file: {error}"),
                );
            }
        }
        let file_content = match tokio::fs::read(path).await {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(error) => {
                return failure(
                    "FILE_EDIT_IO_FAILED",
                    format!("Failed to edit file: {error}"),
                );
            }
        };
        let expected_hash = sha256_hex(file_content.as_bytes());

        let Some(actual_old) = find_actual_string(&file_content, old_string) else {
            return failure(
                "FILE_EDIT_MATCH_NOT_FOUND",
                format!(
                    "No match found for the specified old_string in {file_path}.\n{MATCH_NOT_FOUND_HINT}"
                ),
            );
        };
        let match_count = count_matches(&file_content, &actual_old);
        if match_count > 1 && !replace_all {
            return failure(
                "FILE_EDIT_MATCH_AMBIGUOUS",
                format!(
                    "Found {match_count} matches. \
                     Set replace_all=true or provide more context to uniquely identify."
                ),
            );
        }
        let new_content = if replace_all {
            file_content.replace(&actual_old, new_string)
        } else {
            file_content.replacen(actual_old.as_str(), new_string, 1)
        };

        // 写前 SHA-256 冲突检测（Conflict-Abort：重读磁盘与刚读到的内容比对）。
        if let Some(current_hash) = current_disk_hash(path).await
            && current_hash != expected_hash
        {
            return failure(
                "FILE_CONFLICT",
                format!(
                    "文件自上次读取后已被修改，请重新读取文件后再编辑。\n\
                     Expected hash: {expected_hash}\n\
                     Current hash: {current_hash}"
                ),
            );
        }

        let outcome = write_checked(
            path,
            &new_content,
            &ExpectedOldState::sha256(&expected_hash),
        )
        .await;
        if !outcome.success {
            return write_failure(&outcome);
        }

        let (history_recorded, history_error) = self.capture(ctx, file_path, &file_content).await;
        file_state::global().mark_modified(session, file_path);
        let diff = unified_diff(file_path, &file_content, &new_content);
        let mut output = ToolOutput::ok(format!("Edited: {file_path}"));
        output.metadata = Some(json!({
            "structuredResult": {
                "type": "update",
                "filePath": file_path,
                "diff": diff,
                "sealedHash": outcome.new_hash,
                "historyRecorded": history_recorded,
                "historyErrorCode": history_error,
                "postCommitErrorCode": "",
                "matchCount": if replace_all { match_count } else { 1 },
            }
        }));
        output
    }

    /// post-commit 快照（旧 `trackAppliedEdit(filePath, fileContent, …, "edit")`
    /// 的错误码逐条对齐：无旧内容 / 超 10 MiB → `HISTORY_SNAPSHOT_NOT_APPLICABLE`，
    /// 落库异常 → `HISTORY_SNAPSHOT_PERSIST_FAILED`）。
    async fn capture(&self, ctx: &ToolContext, path: &str, original: &str) -> (bool, &'static str) {
        let (Some(sink), Some(session_id)) = (self.sink.as_ref(), ctx.session_id()) else {
            return (false, "HISTORY_SNAPSHOT_NOT_APPLICABLE");
        };
        if original.len() > MAX_SNAPSHOT_BYTES {
            tracing::warn!(path, bytes = original.len(), "snapshot skipped: too large");
            return (false, "HISTORY_SNAPSHOT_NOT_APPLICABLE");
        }
        let request = SnapshotRequest {
            session_id: session_id.to_owned(),
            message_id: ctx.tool_use_id().map(str::to_owned),
            file_path: path.to_owned(),
            content: original.to_owned(),
            operation: SNAPSHOT_OPERATION.to_owned(),
        };
        match sink.capture(request).await {
            Ok(()) => (true, ""),
            Err(error) => {
                tracing::warn!(path, %error, "snapshot persist failed");
                (false, "HISTORY_SNAPSHOT_PERSIST_FAILED")
            }
        }
    }
}

/// [`EditFileTool::apply_edit`] 的入参束（规避过长参数表 lint）。
struct EditRequest<'a> {
    /// 解析后的目标路径。
    path: &'a std::path::Path,
    /// 目标路径的展示串（元数据 / 台账 key）。
    file_path: &'a str,
    /// 待替换串。
    old_string: &'a str,
    /// 替换串。
    new_string: &'a str,
    /// 是否全量替换。
    replace_all: bool,
    /// 台账分桶 key。
    session: &'a str,
    /// 执行上下文。
    ctx: &'a ToolContext,
}

/// 「文件不存在 + `old_string` 为空」→ 新建文件（旧 `ExpectedOldState.absent()`）。
async fn create_file(
    path: &std::path::Path,
    file_path: &str,
    new_string: &str,
    session: &str,
) -> ToolOutput {
    let outcome = write_checked(path, new_string, &ExpectedOldState::Absent).await;
    if !outcome.success {
        return write_failure(&outcome);
    }
    file_state::global().mark_modified(session, file_path);
    let mut output = ToolOutput::ok(format!("Created: {file_path}"));
    output.metadata = Some(json!({
        "structuredResult": {
            "type": "create",
            "filePath": file_path,
            "sealedHash": outcome.new_hash,
            // 旧 `postCommitErrorCode` 仅在 markModified 抛异常时置
            // `POST_COMMIT_CACHE_UPDATE_FAILED`；Rust 侧台账更新不会失败。
            "postCommitErrorCode": "",
        }
    }));
    output
}

/// 原子写失败的统一出口（旧 `writeFailure`：`ATOMIC_WRITE_FAILED` +
/// `"原子写入失败: " + error`，元数据只带 `sealedHash`）。
fn write_failure(outcome: &WriteOutcome) -> ToolOutput {
    let reason = outcome.error.as_deref().unwrap_or_default();
    let mut output = failure("ATOMIC_WRITE_FAILED", format!("原子写入失败: {reason}"));
    output.metadata = Some(json!({
        "structuredResult": {
            "sealedHash": outcome.new_hash.clone().unwrap_or_default(),
        }
    }));
    output
}

/// 磁盘当前内容摘要；文件不存在或读失败 → `None`（旧
/// `checkBeforeWrite` 在这两种情况下均判「无冲突」）。
async fn current_disk_hash(path: &std::path::Path) -> Option<String> {
    tokio::fs::read(path)
        .await
        .ok()
        .map(|bytes| sha256_hex(&bytes))
}

/// unified diff（旧 `UnifiedDiffUtils.generateUnifiedDiff(filePath, filePath,
/// originalLines, patch, 3)` + `String.join("\n", …)`：两侧文件名同为目标
/// 路径、3 行上下文、无尾随换行）。
fn unified_diff(file_path: &str, original: &str, updated: &str) -> String {
    // 旧实现按 `split("\n", -1)` 切行（保留尾部空串），故用 `from_slices`
    // 而非 `from_lines`（后者保留 "\n" 结尾并触发 missing-newline 提示）。
    let original_lines: Vec<&str> = original.split('\n').collect();
    let updated_lines: Vec<&str> = updated.split('\n').collect();
    let diff = TextDiff::from_slices(&original_lines, &updated_lines);
    let mut formatter = diff.unified_diff();
    let rendered = formatter
        .context_radius(DIFF_CONTEXT_RADIUS)
        .missing_newline_hint(false)
        .header(file_path, file_path)
        .to_string();
    rendered
        .strip_suffix('\n')
        .map_or(rendered.clone(), std::borrow::ToOwned::to_owned)
}

/// 非重叠匹配计数（旧 `countMatches` 的 `idx += search.length()` 步进语义）。
fn count_matches(text: &str, search: &str) -> usize {
    if search.is_empty() {
        // 空串在每个字符边界各匹配一次（旧实现此处死循环，见模块文档）。
        return text.chars().count() + 1;
    }
    text.matches(search).count()
}

/// 5 策略容错匹配（旧 `findActualString`：精确 → 引号归一化 → 行尾空白
/// 归一化 → 换行符归一化 → Tab/空格归一化），命中即返回**原文中的**子串。
fn find_actual_string(file_content: &str, search: &str) -> Option<String> {
    // 策略 1：精确匹配。
    if file_content.contains(search) {
        return Some(search.to_owned());
    }

    // 策略 2：引号归一化——先归一化 search，再「归一化正文后反查原文子串」。
    let normalized_search = normalize_quotes(search);
    if normalized_search != search && file_content.contains(&normalized_search) {
        return Some(normalized_search);
    }
    let normalized_file = normalize_quotes(file_content);
    if let Some(byte_index) = normalized_file.find(&normalized_search) {
        // 归一化保字符数不变，故字符下标在原文与归一化文本间一一对应。
        let char_index = normalized_file[..byte_index].chars().count();
        let candidate: String = file_content
            .chars()
            .skip(char_index)
            .take(search.chars().count())
            .collect();
        if file_content.contains(&candidate) {
            return Some(candidate);
        }
    }

    // 策略 3：行尾空白归一化——逐行滑窗比对去尾空白后的行。
    let trimmed_search = trim_trailing_whitespace(search);
    if trimmed_search != search
        && let Some(candidate) = match_trailing_whitespace(file_content, &trimmed_search)
    {
        return Some(candidate);
    }

    // 策略 4：换行符归一化。
    let lf_search = search.replace("\r\n", "\n");
    if lf_search != search && file_content.contains(&lf_search) {
        return Some(lf_search);
    }

    // 策略 5：Tab/空格双向归一化。
    let spacified = search.replace('\t', "    ");
    if spacified != search && file_content.contains(&spacified) {
        return Some(spacified);
    }
    let tabbed = search.replace("    ", "\t");
    if tabbed != search && file_content.contains(&tabbed) {
        return Some(tabbed);
    }
    None
}

/// 策略 3 的滑窗匹配（旧实现逐行 `replaceAll("\\s+$", "")` 后比对，命中则
/// 用**原文行**拼回候选串）。
fn match_trailing_whitespace(file_content: &str, trimmed_search: &str) -> Option<String> {
    let file_lines: Vec<&str> = file_content.split('\n').collect();
    let search_lines: Vec<&str> = trimmed_search.split('\n').collect();
    let last_start = file_lines.len().checked_sub(search_lines.len())?;
    for start in 0..=last_start {
        let matched = search_lines
            .iter()
            .enumerate()
            .all(|(offset, expected)| trim_line_end(file_lines[start + offset]) == *expected);
        if matched {
            let candidate = file_lines[start..start + search_lines.len()].join("\n");
            if file_content.contains(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// 逐行去尾空白（旧 `trimTrailingWhitespace`）。
fn trim_trailing_whitespace(text: &str) -> String {
    text.split('\n')
        .map(trim_line_end)
        .collect::<Vec<_>>()
        .join("\n")
}

/// 去行尾空白（字符集与 Java 正则 `\s` 一致：空格 / 制表 / 换行 / 垂直制表 /
/// 换页 / 回车）。
fn trim_line_end(line: &str) -> &str {
    line.trim_end_matches([' ', '\t', '\n', '\u{0B}', '\u{0C}', '\r'])
}

/// 弯引号 → ASCII 引号（旧 `normalizeQuotes`，逐字符替换保字符数）。
fn normalize_quotes(text: &str) -> String {
    text.chars()
        .map(|ch| match ch {
            '\u{201C}' | '\u{201D}' => '"',
            '\u{2018}' | '\u{2019}' => '\'',
            other => other,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// 记录型快照出口。
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

    fn ctx(session: &str) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_session_id(session)
            .with_tool_use_id("call-1")
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-edit-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// 种一个文件并在台账登记「已读」（等价先跑一次 `Read`）。
    fn seed(tag: &str, name: &str, body: &str, session: &str) -> String {
        let path = temp_dir(tag).join(name);
        std::fs::write(&path, body).expect("seed");
        let display = path.display().to_string();
        file_state::global().mark_read(session, &display, body, None, None, false);
        display
    }

    #[tokio::test]
    async fn replaces_unique_occurrence_and_reports_diff() {
        let session = "edit-unique";
        let sink = Arc::new(RecordingSink::default());
        let tool = EditFileTool::with_snapshot_sink(Arc::clone(&sink) as Arc<dyn SnapshotSink>);
        let path = seed("unique", "a.txt", "alpha\nbeta\ngamma\n", session);
        let output = tool
            .execute(
                json!({ "file_path": path, "old_string": "beta", "new_string": "delta" }),
                ctx(session),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, format!("Edited: {path}"));
        assert_eq!(
            std::fs::read_to_string(&path).expect("read"),
            "alpha\ndelta\ngamma\n"
        );
        let structured = output.metadata.expect("metadata")["structuredResult"].clone();
        assert_eq!(structured["type"], "update");
        assert_eq!(structured["matchCount"], 1);
        assert_eq!(structured["historyRecorded"], true);
        assert_eq!(structured["historyErrorCode"], "");
        let diff = structured["diff"].as_str().expect("diff");
        assert!(
            diff.starts_with(&format!("--- {path}\n+++ {path}\n@@ ")),
            "{diff}"
        );
        assert!(diff.contains("-beta"), "{diff}");
        assert!(diff.contains("+delta"), "{diff}");
        assert!(!diff.ends_with('\n'), "diff 不带尾随换行");
        let seen = sink.seen.lock().expect("lock");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].operation, "edit");
        assert_eq!(seen[0].content, "alpha\nbeta\ngamma\n");
        assert_eq!(seen[0].message_id.as_deref(), Some("call-1"));
    }

    #[tokio::test]
    async fn requires_read_before_edit() {
        let tool = EditFileTool::new();
        let path = temp_dir("no-read").join("b.txt");
        std::fs::write(&path, "body").expect("seed");
        let output = tool
            .execute(
                json!({
                    "file_path": path.display().to_string(),
                    "old_string": "body",
                    "new_string": "next",
                }),
                ctx("edit-no-read"),
            )
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "FILE_READ_REQUIRED: 请先使用 Read 工具读取文件内容"
        );
    }

    #[tokio::test]
    async fn detects_external_modification_as_stale() {
        let session = "edit-stale";
        let tool = EditFileTool::new();
        let path = seed("stale", "c.txt", "body", session);
        let future = std::time::SystemTime::now() + std::time::Duration::from_mins(2);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .and_then(|file| file.set_modified(future))
            .expect("touch");
        let output = tool
            .execute(
                json!({ "file_path": path, "old_string": "body", "new_string": "next" }),
                ctx(session),
            )
            .await;
        assert_eq!(
            output.content,
            "FILE_READ_STATE_STALE: 文件已被外部修改，请重新 Read"
        );
    }

    #[tokio::test]
    async fn rejects_identical_strings_and_missing_parameters() {
        let tool = EditFileTool::new();
        let identical = tool
            .execute(
                json!({ "file_path": "/tmp/x.txt", "old_string": "a", "new_string": "a" }),
                ctx("edit-identical"),
            )
            .await;
        assert_eq!(
            identical.content,
            "FILE_EDIT_NO_CHANGE: old_string and new_string are identical. No changes to make."
        );

        let missing = tool
            .execute(json!({ "file_path": "/tmp/x.txt" }), ctx("edit-missing"))
            .await;
        assert!(missing.content.starts_with("MISSING_PARAMETER: "));
    }

    #[tokio::test]
    async fn rejects_ambiguous_match_unless_replace_all() {
        let session = "edit-ambiguous";
        let tool = EditFileTool::new();
        let path = seed("ambiguous", "d.txt", "x\nx\n", session);
        let ambiguous = tool
            .execute(
                json!({ "file_path": path.clone(), "old_string": "x", "new_string": "y" }),
                ctx(session),
            )
            .await;
        assert_eq!(
            ambiguous.content,
            "FILE_EDIT_MATCH_AMBIGUOUS: Found 2 matches. \
             Set replace_all=true or provide more context to uniquely identify."
        );

        let replaced = tool
            .execute(
                json!({
                    "file_path": path.clone(),
                    "old_string": "x",
                    "new_string": "y",
                    "replace_all": true,
                }),
                ctx(session),
            )
            .await;
        assert!(!replaced.is_error, "{}", replaced.content);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "y\ny\n");
        let structured = replaced.metadata.expect("metadata")["structuredResult"].clone();
        assert_eq!(structured["matchCount"], 2);
    }

    #[tokio::test]
    async fn reports_missing_match_with_strategy_hint() {
        let session = "edit-nomatch";
        let tool = EditFileTool::new();
        let path = seed("nomatch", "e.txt", "hello\n", session);
        let output = tool
            .execute(
                json!({ "file_path": path.clone(), "old_string": "nope", "new_string": "y" }),
                ctx(session),
            )
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            format!(
                "FILE_EDIT_MATCH_NOT_FOUND: No match found for the specified old_string in \
                 {path}.\n{MATCH_NOT_FOUND_HINT}"
            )
        );
    }

    #[tokio::test]
    async fn creates_file_when_old_string_is_empty() {
        let session = "edit-create";
        let tool = EditFileTool::new();
        let path = temp_dir("create").join("nested/f.txt");
        let _ = std::fs::remove_file(&path);
        let display = path.display().to_string();
        let output = tool
            .execute(
                json!({ "file_path": display.clone(), "old_string": "", "new_string": "fresh" }),
                ctx(session),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, format!("Created: {display}"));
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "fresh");
        let structured = output.metadata.expect("metadata")["structuredResult"].clone();
        assert_eq!(structured["type"], "create");
        assert_eq!(structured["postCommitErrorCode"], "");
    }

    #[tokio::test]
    async fn reports_missing_file_when_old_string_is_present() {
        let tool = EditFileTool::new();
        let path = temp_dir("absent").join("g.txt");
        let _ = std::fs::remove_file(&path);
        let display = path.display().to_string();
        let output = tool
            .execute(
                json!({ "file_path": display.clone(), "old_string": "a", "new_string": "b" }),
                ctx("edit-absent"),
            )
            .await;
        // 未读过 → 门禁先于存在性检查（与旧实现次序一致）。
        assert_eq!(
            output.content,
            "FILE_READ_REQUIRED: 请先使用 Read 工具读取文件内容"
        );

        file_state::global().mark_read("edit-absent", &display, "", None, None, false);
        let output = tool
            .execute(
                json!({ "file_path": display.clone(), "old_string": "a", "new_string": "b" }),
                ctx("edit-absent"),
            )
            .await;
        // 台账有记录但磁盘取不到 mtime → 判过期（旧 `isStale` 的 `IOException`
        // 分支逐字对齐 `return true`）。`FILE_NOT_FOUND` 分支仅在「门禁通过后
        // 文件被并发删除」的竞态下可达，与旧实现的检查次序完全一致。
        assert_eq!(
            output.content,
            "FILE_READ_STATE_STALE: 文件已被外部修改，请重新 Read"
        );
    }

    #[test]
    fn fuzzy_matching_covers_all_five_strategies() {
        assert_eq!(
            find_actual_string("let a = 1;", "a = 1").as_deref(),
            Some("a = 1")
        );
        // 策略 2：搜索串用弯引号，正文用 ASCII 引号。
        assert_eq!(
            find_actual_string("say \"hi\"", "say \u{201C}hi\u{201D}").as_deref(),
            Some("say \"hi\"")
        );
        // 策略 2 反向：正文用弯引号，搜索串用 ASCII 引号 → 反查回原文子串。
        assert_eq!(
            find_actual_string("say \u{201C}hi\u{201D}", "say \"hi\"").as_deref(),
            Some("say \u{201C}hi\u{201D}")
        );
        // 策略 3：**搜索串**行尾带空白（旧实现以 `!trimmedSearch.equals(
        // searchString)` 为进入条件，故正文带空白而搜索串不带时该策略不触发）。
        assert_eq!(
            find_actual_string("one\ntwo\n", "one   \ntwo").as_deref(),
            Some("one\ntwo")
        );
        // 策略 4：搜索串是 CRLF。
        assert_eq!(
            find_actual_string("a\nb", "a\r\nb").as_deref(),
            Some("a\nb")
        );
        // 策略 5：制表 ↔ 4 空格双向。
        assert_eq!(find_actual_string("    x", "\tx").as_deref(), Some("    x"));
        assert_eq!(find_actual_string("\tx", "    x").as_deref(), Some("\tx"));
        assert_eq!(find_actual_string("abc", "zzz"), None);
    }

    #[test]
    fn count_matches_is_non_overlapping_and_terminates_on_empty_needle() {
        assert_eq!(count_matches("aaaa", "aa"), 2);
        assert_eq!(count_matches("abc", "z"), 0);
        // 旧实现在此死循环；本实现按字符边界计数。
        assert_eq!(count_matches("ab", ""), 3);
        assert_eq!(count_matches("", ""), 1);
    }
}
