//! `Glob` 工具——按 glob 模式搜索文件路径。
//!
//! 对照旧 `tool/impl/GlobTool.java`（只读权威规格，逐条对齐）：工具名
//! `Glob`、入参 `pattern` / `path` / `max_results`（默认 200）、模式匹配
//! **相对路径**、VCS 元目录整枝跳过、结果字符上限 100 000、截断标记
//! `"\n[Results truncated]"`、`truncated = 命中数 >= max_results || 字符截断`、
//! 元数据 `filenames` / `numFiles` / `durationMs` / `truncated`、错误码
//! `GLOB_UNC_PATH_DENIED` / `GLOB_DIRECTORY_NOT_FOUND`。
//!
//! 差异（留痕 docs/compatibility.md §4）：
//! - 旧 `GLOB_PATH_DENIED`（`PathSecurityService` 递归读根鉴权）与受保护
//!   目录/文件名过滤属 2.5 权限管线，本阶段不实现；
//! - 旧描述声称「按修改时间排序」但实现未排序（遍历序），本实现改为
//!   **路径字典序**稳定输出（消除文件系统遍历序不确定性）；
//! - 旧遍历不看 `.gitignore`，本实现同样默认不看（`standard_filters(false)`），
//!   仅保留 VCS 元目录排除，与旧一致；
//! - 匹配语义对齐旧 Java `FileSystems.getPathMatcher("glob:…")`：Java 把
//!   `**` 译为 `.*`、`/` 为字面量，故 `**/*.rs` 形如 `.*/[^/]*\.rs`，**不**
//!   匹配顶层 `a.rs`；`globset` 则视前导 `**/` 为「零或多级目录」而匹配之。
//!   为逐字保真，`**/` 前导模式额外要求相对路径含分隔符（见
//!   [`requires_separator`]）。

use std::path::Path;
use std::time::Instant;

use futures::future::BoxFuture;
use serde_json::json;

use crate::input::{
    RESULTS_TRUNCATED, failure, optional_str, optional_usize, required_str, resolve_path,
    truncate_chars,
};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 默认命中上限（旧 `input.getInt("max_results", 200)` 逐字对照）。
pub const DEFAULT_GLOB_MAX_RESULTS: usize = 200;

/// 命中上限硬顶。
pub const MAX_GLOB_MAX_RESULTS: usize = 5_000;

/// 结果字符上限（旧 `MAX_RESULT_SIZE_CHARS = 100_000`）。
pub const MAX_GLOB_RESULT_CHARS: usize = 100_000;

/// VCS 元目录（旧 `VCS_EXCLUDE` 六项逐字对照）。
const VCS_EXCLUDE: [&str; 6] = [".git", ".svn", ".hg", ".bzr", ".jj", ".sl"];

/// glob 文件搜索工具（名 `Glob`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct GlobTool;

impl Tool for GlobTool {
    fn name(&self) -> &'static str {
        "Glob"
    }

    fn description(&self) -> &'static str {
        "Fast file pattern matching. Supports glob patterns like \"**/*.rs\" or \"src/**/*.ts\" \
         and returns matching file paths relative to the search directory."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern matched against paths relative to the search directory."
                },
                "path": {
                    "type": "string",
                    "description": "Directory to search in (default: the session working directory)."
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return (default: 200)."
                }
            },
            "required": ["pattern"]
        })
    }

    /// 只读工具（旧 `GlobTool.java:80` `isReadOnly` → `true`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

/// 执行主体（入参校验 → 模式编译 → `spawn_blocking` 遍历 → 结果组装）。
async fn run(input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
    let pattern = match required_str(&input, "pattern") {
        Ok(value) => value.to_owned(),
        Err(output) => return output,
    };
    let raw_path = optional_str(&input, "path");
    if raw_path.is_some_and(|path| path.starts_with("\\\\") || path.starts_with("//")) {
        return failure("GLOB_UNC_PATH_DENIED", "UNC paths are not allowed");
    }
    let root = match raw_path {
        Some(raw) => resolve_path(raw, &ctx),
        None => ctx.working_dir().to_path_buf(),
    };
    let display = root.display().to_string();
    if !tokio::fs::metadata(&root)
        .await
        .is_ok_and(|meta| meta.is_dir())
    {
        return failure(
            "GLOB_DIRECTORY_NOT_FOUND",
            format!("Directory does not exist: {display}"),
        );
    }
    let matcher = match globset::GlobBuilder::new(&pattern)
        .literal_separator(true)
        .build()
    {
        Ok(glob) => glob.compile_matcher(),
        Err(error) => return failure("GLOB_PATTERN_INVALID", format!("{pattern}: {error}")),
    };
    let need_separator = requires_separator(&pattern);
    let max_results = optional_usize(&input, "max_results")
        .unwrap_or(DEFAULT_GLOB_MAX_RESULTS)
        .clamp(1, MAX_GLOB_MAX_RESULTS);
    let started = Instant::now();
    let target = root.clone();
    let Ok(hits) =
        tokio::task::spawn_blocking(move || search(&target, &matcher, need_separator, max_results))
            .await
    else {
        return failure(
            "GLOB_SEARCH_IO_FAILED",
            format!("{display}: walk task failed"),
        );
    };
    finish(&hits, max_results, started)
}

/// 结果组装（字符上限截断 + 旧四元元数据）。
fn finish(hits: &[String], max_results: usize, started: Instant) -> ToolOutput {
    let hit_cap = hits.len() >= max_results;
    let (body, char_truncated) = truncate_chars(hits.join("\n"), MAX_GLOB_RESULT_CHARS);
    let truncated = hit_cap || char_truncated;
    let mut content = body;
    if truncated {
        content.push_str(RESULTS_TRUNCATED);
    }
    let mut output = ToolOutput::ok(content);
    output.metadata = Some(json!({
        "structuredResult": {
            "filenames": hits,
            "numFiles": hits.len(),
            "durationMs": started.elapsed().as_millis(),
            "truncated": truncated,
        }
    }));
    output
}

/// 前导 `**/` 判定（Java `PathMatcher` 要求此形状的模式至少跨一级目录）。
fn requires_separator(pattern: &str) -> bool {
    pattern.starts_with("**/")
}

/// 同步遍历 + 相对路径匹配（VCS 元目录整枝；不看 `.gitignore`，同旧）。
fn search(
    root: &Path,
    matcher: &globset::GlobMatcher,
    need_separator: bool,
    max_results: usize,
) -> Vec<String> {
    let mut builder = ignore::WalkBuilder::new(root);
    builder
        .standard_filters(false)
        .follow_links(false)
        .filter_entry(|entry| {
            entry
                .file_name()
                .to_str()
                .is_none_or(|name| !VCS_EXCLUDE.contains(&name))
        });
    let mut hits = Vec::new();
    for entry in builder.build().flatten() {
        if hits.len() >= max_results {
            break;
        }
        if !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        let Ok(relative) = entry.path().strip_prefix(root) else {
            continue;
        };
        let text = relative.to_string_lossy();
        if need_separator && !text.contains(std::path::MAIN_SEPARATOR) {
            continue;
        }
        if matcher.is_match(relative) {
            hits.push(text.into_owned());
        }
    }
    hits.sort_unstable();
    hits
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx(working_dir: &Path) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(working_dir)
    }

    /// 固定树：`a.rs` / `src/b.rs` / `src/deep/c.rs` / `notes.txt` / `.git/HEAD`。
    fn fixture(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("zk-glob-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("src/deep")).expect("mkdir");
        std::fs::create_dir_all(root.join(".git")).expect("mkdir .git");
        std::fs::write(root.join("a.rs"), "").expect("write");
        std::fs::write(root.join("src/b.rs"), "").expect("write");
        std::fs::write(root.join("src/deep/c.rs"), "").expect("write");
        std::fs::write(root.join("notes.txt"), "").expect("write");
        std::fs::write(root.join(".git/HEAD"), "").expect("write");
        root
    }

    #[test]
    fn leading_double_star_requires_a_directory_level() {
        assert!(requires_separator("**/*.rs"));
        assert!(!requires_separator("*.rs"));
        assert!(!requires_separator("src/**/*.rs"));
    }

    #[tokio::test]
    async fn matches_recursive_pattern_in_sorted_order() {
        let root = fixture("recursive");
        let output = GlobTool
            .execute(json!({ "pattern": "**/*.rs" }), ctx(&root))
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "src/b.rs\nsrc/deep/c.rs");
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["numFiles"], 2);
        assert_eq!(metadata["structuredResult"]["truncated"], false);
    }

    #[tokio::test]
    async fn literal_separator_keeps_single_level_patterns_shallow() {
        let root = fixture("shallow");
        let output = GlobTool
            .execute(
                json!({ "pattern": "*.rs", "path": root.to_str().expect("utf8") }),
                ctx(&root),
            )
            .await;
        assert_eq!(output.content, "a.rs");
    }

    #[tokio::test]
    async fn truncates_when_hitting_max_results() {
        let root = fixture("cap");
        let output = GlobTool
            .execute(
                json!({ "pattern": "**/*.rs", "max_results": 1 }),
                ctx(&root),
            )
            .await;
        assert!(
            output.content.ends_with(RESULTS_TRUNCATED),
            "{}",
            output.content
        );
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["truncated"], true);
        assert_eq!(metadata["structuredResult"]["numFiles"], 1);
    }

    #[tokio::test]
    async fn rejects_missing_pattern_unc_path_and_absent_directory() {
        let root = fixture("errors");
        let missing = GlobTool.execute(json!({}), ctx(&root)).await;
        assert!(missing.content.starts_with("MISSING_PARAMETER: "));

        let unc = GlobTool
            .execute(
                json!({ "pattern": "*", "path": "//host/share" }),
                ctx(&root),
            )
            .await;
        assert!(unc.content.starts_with("GLOB_UNC_PATH_DENIED: "));

        let absent = GlobTool
            .execute(json!({ "pattern": "*", "path": "nope" }), ctx(&root))
            .await;
        assert!(absent.content.starts_with("GLOB_DIRECTORY_NOT_FOUND: "));

        let bad = GlobTool
            .execute(json!({ "pattern": "a[" }), ctx(&root))
            .await;
        assert!(bad.content.starts_with("GLOB_PATTERN_INVALID: "));
    }
}
