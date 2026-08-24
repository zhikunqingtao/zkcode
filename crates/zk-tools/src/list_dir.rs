//! `ListDir` 工具——列目录条目（递归 / 非递归，尊重 `.gitignore`）。
//!
//! **无旧对照**：旧仓库 `tool/impl/` 无 `ListDirectory` / `LS` 类
//! （已 grep 确认），目录浏览在旧系统由 `Glob` + REST 文件树端点承担。
//! 本工具为 zkcode 新增能力，语义参数向旧 `GlobTool.java` 对齐：
//! 相对路径输出、`max_results` 默认 200、结果字符上限 100 000、
//! 截断标记 `"\n[Results truncated]"` 逐字复用。
//!
//! `.gitignore` 尊重由 [`ignore::WalkBuilder`] 提供（`require_git(false)`，
//! 无 git 仓库时同样生效）；隐藏文件默认列出（对齐旧 `Grep` / `Glob` 的
//! `--hidden`），VCS 元目录始终排除（旧 `VCS_EXCLUDE` 六项）。

use std::path::Path;

use futures::future::BoxFuture;
use serde_json::json;

use crate::input::{
    RESULTS_TRUNCATED, bool_or, failure, optional_str, optional_usize, resolve_path, truncate_chars,
};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 默认条目上限（对照旧 `GlobTool` 入参默认 `max_results = 200`）。
pub const DEFAULT_LIST_MAX_ENTRIES: usize = 200;

/// 条目上限硬顶。
pub const MAX_LIST_MAX_ENTRIES: usize = 5_000;

/// 结果字符上限（旧 `GlobTool.MAX_RESULT_SIZE_CHARS = 100_000`）。
pub const MAX_LIST_RESULT_CHARS: usize = 100_000;

/// VCS 元目录（旧 `GlobTool.VCS_EXCLUDE` 六项逐字对照）。
const VCS_EXCLUDE: [&str; 6] = [".git", ".svn", ".hg", ".bzr", ".jj", ".sl"];

/// 目录列举工具（名 `ListDir`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct ListDirectoryTool;

/// 遍历产出（条目 + 是否触达上限）。
struct Listing {
    /// 相对 root 的条目（目录带 `/` 后缀），已排序。
    entries: Vec<String>,
    /// 是否因条目上限提前停止。
    truncated: bool,
}

impl Tool for ListDirectoryTool {
    fn name(&self) -> &'static str {
        "ListDir"
    }

    fn description(&self) -> &'static str {
        "List directory entries (optionally recursive). Paths are returned relative to the \
         listed directory; directories carry a trailing slash. Respects .gitignore by default."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory to list (default: the session working directory)."
                },
                "recursive": {
                    "type": "boolean",
                    "description": "Walk sub-directories recursively (default false)."
                },
                "max_entries": {
                    "type": "integer",
                    "description": "Maximum entries to return (default 200, max 5000)."
                },
                "respect_gitignore": {
                    "type": "boolean",
                    "description": "Honour .gitignore / .ignore rules (default true)."
                }
            }
        })
    }

    /// 只读工具（目录枚举无副作用；旧仓库无对应 Java 工具，语义同
    /// `GlobTool.java:80`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

/// 执行主体（目录体检 → `spawn_blocking` 遍历 → 文本 + 元数据组装）。
async fn run(input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
    let root = match optional_str(&input, "path") {
        Some(raw) => resolve_path(raw, &ctx),
        None => ctx.working_dir().to_path_buf(),
    };
    let display = root.display().to_string();
    match tokio::fs::metadata(&root).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return failure("NOT_A_DIRECTORY", format!("{display} is not a directory")),
        Err(error) => return failure("DIRECTORY_NOT_FOUND", format!("{display}: {error}")),
    }
    let recursive = bool_or(&input, "recursive", false);
    let respect_gitignore = bool_or(&input, "respect_gitignore", true);
    let max_entries = optional_usize(&input, "max_entries")
        .unwrap_or(DEFAULT_LIST_MAX_ENTRIES)
        .clamp(1, MAX_LIST_MAX_ENTRIES);
    let target = root.clone();
    let listing = tokio::task::spawn_blocking(move || {
        collect(&target, recursive, respect_gitignore, max_entries)
    })
    .await;
    let Ok(listing) = listing else {
        return failure(
            "DIRECTORY_LIST_FAILED",
            format!("{display}: walk task failed"),
        );
    };
    finish(&display, &listing)
}

/// 结果组装（字符上限截断 + `structuredResult` 元数据）。
fn finish(display: &str, listing: &Listing) -> ToolOutput {
    let count = listing.entries.len();
    if count == 0 {
        let mut output = ToolOutput::ok(format!("No entries under {display}"));
        output.metadata = Some(json!({
            "structuredResult": { "path": display, "count": 0, "truncated": false }
        }));
        return output;
    }
    let (body, char_truncated) = truncate_chars(listing.entries.join("\n"), MAX_LIST_RESULT_CHARS);
    let truncated = listing.truncated || char_truncated;
    let mut content = body;
    if truncated {
        content.push_str(RESULTS_TRUNCATED);
    }
    let mut output = ToolOutput::ok(content);
    output.metadata = Some(json!({
        "structuredResult": { "path": display, "count": count, "truncated": truncated }
    }));
    output
}

/// 同步遍历（`ignore` crate：`.gitignore` 尊重 + VCS 元目录排除）。
fn collect(root: &Path, recursive: bool, respect_gitignore: bool, max_entries: usize) -> Listing {
    let mut builder = ignore::WalkBuilder::new(root);
    builder
        .hidden(false)
        .git_ignore(respect_gitignore)
        .git_global(respect_gitignore)
        .git_exclude(respect_gitignore)
        .ignore(respect_gitignore)
        .parents(respect_gitignore)
        .require_git(false)
        .follow_links(false)
        .filter_entry(|entry| {
            entry
                .file_name()
                .to_str()
                .is_none_or(|name| !VCS_EXCLUDE.contains(&name))
        });
    if !recursive {
        builder.max_depth(Some(1));
    }
    let mut entries = Vec::new();
    let mut truncated = false;
    for entry in builder.build().flatten() {
        let Ok(relative) = entry.path().strip_prefix(root) else {
            continue;
        };
        if relative.as_os_str().is_empty() {
            continue;
        }
        if entries.len() >= max_entries {
            truncated = true;
            break;
        }
        entries.push(render(relative, entry.path()));
    }
    entries.sort_unstable();
    Listing { entries, truncated }
}

/// 渲染单条目（目录追加 `/`）。
fn render(relative: &Path, full: &Path) -> String {
    let text = relative.to_string_lossy().into_owned();
    if full.is_dir() {
        format!("{text}/")
    } else {
        text
    }
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

    /// 布置固定目录树：`a.txt` / `sub/b.txt` / `ignored.log` + `.gitignore`。
    fn fixture(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("zk-ls-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("sub")).expect("mkdir");
        std::fs::create_dir_all(root.join(".git")).expect("mkdir .git");
        std::fs::write(root.join("a.txt"), "a").expect("write");
        std::fs::write(root.join("sub/b.txt"), "b").expect("write");
        std::fs::write(root.join("ignored.log"), "x").expect("write");
        std::fs::write(root.join(".gitignore"), "*.log\n").expect("write");
        std::fs::write(root.join(".git/HEAD"), "ref").expect("write");
        root
    }

    #[tokio::test]
    async fn lists_shallow_entries_and_excludes_vcs_and_ignored() {
        let root = fixture("shallow");
        let output = ListDirectoryTool.execute(json!({}), ctx(&root)).await;
        assert!(!output.is_error, "{}", output.content);
        let lines: Vec<&str> = output.content.lines().collect();
        assert!(lines.contains(&"a.txt"), "{lines:?}");
        assert!(lines.contains(&"sub/"), "{lines:?}");
        assert!(!lines.contains(&"ignored.log"), "{lines:?}");
        assert!(!lines.iter().any(|line| line.starts_with(".git/")));
        assert!(
            !lines.contains(&"sub/b.txt"),
            "shallow walk must stop at depth 1"
        );
    }

    #[tokio::test]
    async fn recursive_walk_reaches_nested_files_and_can_keep_ignored() {
        let root = fixture("deep");
        let output = ListDirectoryTool
            .execute(
                json!({ "recursive": true, "respect_gitignore": false }),
                ctx(&root),
            )
            .await;
        let lines: Vec<&str> = output.content.lines().collect();
        assert!(lines.contains(&"sub/b.txt"), "{lines:?}");
        assert!(lines.contains(&"ignored.log"), "{lines:?}");
    }

    #[tokio::test]
    async fn truncates_at_max_entries() {
        let root = fixture("cap");
        let output = ListDirectoryTool
            .execute(json!({ "recursive": true, "max_entries": 1 }), ctx(&root))
            .await;
        assert!(
            output.content.ends_with(RESULTS_TRUNCATED),
            "{}",
            output.content
        );
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["truncated"], true);
        assert_eq!(metadata["structuredResult"]["count"], 1);
    }

    #[tokio::test]
    async fn reports_missing_and_non_directory_targets() {
        let root = fixture("errors");
        let missing = ListDirectoryTool
            .execute(json!({ "path": "no-such-dir" }), ctx(&root))
            .await;
        assert!(missing.is_error);
        assert!(missing.content.starts_with("DIRECTORY_NOT_FOUND: "));

        let file = ListDirectoryTool
            .execute(json!({ "path": "a.txt" }), ctx(&root))
            .await;
        assert!(file.is_error);
        assert!(file.content.starts_with("NOT_A_DIRECTORY: "));
    }
}
