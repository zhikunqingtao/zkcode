//! `Grep` 工具——正则搜索文件内容（三种输出模式 + 行上下文）。
//!
//! 对照旧 `tool/impl/GrepTool.java`（只读权威规格）：工具名 `Grep`、入参
//! `pattern` / `path` / `glob` / `type` / `output_mode` / `-i` / `-A` / `-B` /
//! `-C` / `multiline` / `head_limit`（默认 250）/ `offset`、`output_mode`
//! 三态（`content` / `files_with_matches`（默认）/ `count`）、结果字符上限
//! 20 000、输出行上限 10 000、截断标记 `"\n[Results truncated]"`。
//!
//! 差异（留痕 docs/compatibility.md §4）：
//! - 旧实现 shell 外挂 `ripgrep`（`rg --hidden -n …`），本实现改为**进程内**
//!   `regex` + [`ignore`] 遍历——去掉对外部二进制的运行时依赖，也免去 shell
//!   注入面；`--hidden` 语义保留（隐藏文件参与搜索）、`.gitignore` 语义
//!   保留（rg 默认行为），VCS 元目录排除；
//! - 旧 `type` 直接透传 rg 的类型库，本实现内置常用类型 → 扩展名映射表，
//!   未识别的 `type` 退化为「按该扩展名过滤」；
//! - `content` 模式输出格式对齐 rg：命中行 `path:line:text`、上下文行
//!   `path-line-text`。

use std::path::Path;

use futures::future::BoxFuture;
use serde_json::json;

use crate::input::{
    RESULTS_TRUNCATED, bool_or, failure, optional_str, optional_usize, required_str, resolve_path,
    truncate_chars,
};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 默认输出行上限（旧 `DEFAULT_HEAD_LIMIT = 250` 逐字对照）。
pub const DEFAULT_GREP_HEAD_LIMIT: usize = 250;

/// 结果字符上限（旧 `MAX_RESULT_SIZE_CHARS = 20_000`）。
pub const MAX_GREP_RESULT_CHARS: usize = 20_000;

/// 输出行硬上限（旧 `MAX_OUTPUT_LINES = 10_000`）。
pub const MAX_GREP_OUTPUT_LINES: usize = 10_000;

/// 单文件读取上限（超限跳过，避免把巨型文件读进内存）。
const MAX_GREP_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// VCS 元目录（同 `Glob` 的 `VCS_EXCLUDE`）。
const VCS_EXCLUDE: [&str; 6] = [".git", ".svn", ".hg", ".bzr", ".jj", ".sl"];

/// 内置 `type` → 扩展名映射（未命中时退化为按 `type` 自身当扩展名）。
const TYPE_EXTENSIONS: [(&str, &[&str]); 10] = [
    ("rust", &["rs"]),
    ("js", &["js", "jsx", "mjs", "cjs"]),
    ("ts", &["ts", "tsx"]),
    ("py", &["py", "pyi"]),
    ("java", &["java"]),
    ("go", &["go"]),
    ("md", &["md", "markdown"]),
    ("json", &["json"]),
    ("toml", &["toml"]),
    ("yaml", &["yaml", "yml"]),
];

/// 输出模式（旧 `output_mode` 三态）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// 逐命中行输出（含上下文）。
    Content,
    /// 仅列命中文件（默认）。
    FilesWithMatches,
    /// 每文件命中计数。
    Count,
}

/// 一次搜索的全部选项（自入参解析而来）。
struct Options {
    /// 编译后的正则。
    regex: regex::Regex,
    /// 整文件多行匹配（旧 `multiline`）。
    multiline: bool,
    /// 输出模式。
    mode: Mode,
    /// 命中行前置上下文行数（旧 `-B` / `-C`）。
    before: usize,
    /// 命中行后置上下文行数（旧 `-A` / `-C`）。
    after: usize,
    /// 文件名包含模式（旧 `glob` / `include` 同义）。
    include: Option<globset::GlobMatcher>,
    /// 文件名排除模式（旧 `exclude`）。
    exclude: Option<globset::GlobMatcher>,
    /// 扩展名白名单（旧 `type`）。
    extensions: Option<Vec<String>>,
}

/// 内容正则搜索工具（名 `Grep`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct GrepTool;

impl Tool for GrepTool {
    fn name(&self) -> &'static str {
        "Grep"
    }

    fn description(&self) -> &'static str {
        "Search file contents with a regular expression. Supports output modes \
         (content / files_with_matches / count), context lines and path filters."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Regular expression to search for." },
                "path": { "type": "string", "description": "File or directory to search (default: working directory)." },
                "glob": { "type": "string", "description": "Only search files whose relative path matches this glob." },
                "include": { "type": "string", "description": "Alias of glob." },
                "exclude": { "type": "string", "description": "Skip files whose relative path matches this glob." },
                "type": { "type": "string", "description": "File type filter, e.g. rust, ts, py." },
                "output_mode": {
                    "type": "string",
                    "enum": ["content", "files_with_matches", "count"],
                    "description": "Output shape (default files_with_matches)."
                },
                "-i": { "type": "boolean", "description": "Case insensitive search." },
                "-A": { "type": "integer", "description": "Lines of trailing context (content mode)." },
                "-B": { "type": "integer", "description": "Lines of leading context (content mode)." },
                "-C": { "type": "integer", "description": "Lines of context on both sides (content mode)." },
                "multiline": { "type": "boolean", "description": "Let the pattern span line boundaries." },
                "head_limit": { "type": "integer", "description": "Maximum output lines (default 250)." },
                "offset": { "type": "integer", "description": "Skip this many output lines." }
            },
            "required": ["pattern"]
        })
    }

    /// 只读工具（旧 `GrepTool.java:155` `isReadOnly` → `true`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

/// 执行主体（选项解析 → 目标体检 → `spawn_blocking` 搜索 → 结果组装）。
async fn run(input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
    let options = match parse(&input) {
        Ok(options) => options,
        Err(output) => return output,
    };
    let root = match optional_str(&input, "path") {
        Some(raw) => resolve_path(raw, &ctx),
        None => ctx.working_dir().to_path_buf(),
    };
    let display = root.display().to_string();
    if tokio::fs::metadata(&root).await.is_err() {
        return failure(
            "GREP_PATH_NOT_FOUND",
            format!("Path does not exist: {display}"),
        );
    }
    let head_limit = optional_usize(&input, "head_limit")
        .unwrap_or(DEFAULT_GREP_HEAD_LIMIT)
        .clamp(1, MAX_GREP_OUTPUT_LINES);
    let offset = optional_usize(&input, "offset").unwrap_or(0);
    let target = root.clone();
    let Ok(found) = tokio::task::spawn_blocking(move || search(&target, &options)).await else {
        return failure(
            "GREP_SEARCH_FAILED",
            format!("{display}: search task failed"),
        );
    };
    finish(found, offset, head_limit)
}

/// 入参 → [`Options`]（正则编译失败 → `GREP_PATTERN_INVALID`）。
fn parse(input: &serde_json::Value) -> Result<Options, ToolOutput> {
    let pattern = required_str(input, "pattern")?;
    let multiline = bool_or(input, "multiline", false);
    let regex = regex::RegexBuilder::new(pattern)
        .case_insensitive(bool_or(input, "-i", false))
        .multi_line(multiline)
        .dot_matches_new_line(multiline)
        .build()
        .map_err(|error| failure("GREP_PATTERN_INVALID", format!("{pattern}: {error}")))?;
    let mode = match optional_str(input, "output_mode") {
        Some("content") => Mode::Content,
        Some("count") => Mode::Count,
        Some("files_with_matches") | None => Mode::FilesWithMatches,
        Some(other) => {
            return Err(failure(
                "GREP_OUTPUT_MODE_INVALID",
                format!("Unsupported output_mode: {other}"),
            ));
        }
    };
    let context = optional_usize(input, "-C").unwrap_or(0);
    Ok(Options {
        regex,
        multiline,
        mode,
        before: optional_usize(input, "-B").unwrap_or(context),
        after: optional_usize(input, "-A").unwrap_or(context),
        include: compile_glob(
            optional_str(input, "glob").or_else(|| optional_str(input, "include")),
        )?,
        exclude: compile_glob(optional_str(input, "exclude"))?,
        extensions: optional_str(input, "type").map(extensions_for),
    })
}

/// 编译可选 glob（失败 → `GREP_GLOB_INVALID`）。
fn compile_glob(pattern: Option<&str>) -> Result<Option<globset::GlobMatcher>, ToolOutput> {
    let Some(pattern) = pattern else {
        return Ok(None);
    };
    globset::Glob::new(pattern)
        .map(|glob| Some(glob.compile_matcher()))
        .map_err(|error| failure("GREP_GLOB_INVALID", format!("{pattern}: {error}")))
}

/// `type` → 扩展名集合（未识别时以 `type` 自身当扩展名）。
fn extensions_for(name: &str) -> Vec<String> {
    TYPE_EXTENSIONS
        .iter()
        .find(|(key, _)| *key == name)
        .map_or_else(
            || vec![name.to_owned()],
            |(_, extensions)| extensions.iter().map(|ext| (*ext).to_owned()).collect(),
        )
}

/// 结果组装（offset / `head_limit` 裁剪 + 字符上限截断 + 元数据）。
fn finish(found: Found, offset: usize, head_limit: usize) -> ToolOutput {
    if found.lines.is_empty() {
        let mut output = ToolOutput::ok("No matches found".to_owned());
        output.metadata = Some(json!({
            "structuredResult": { "numLines": 0, "numFiles": 0, "truncated": false }
        }));
        return output;
    }
    let total = found.lines.len();
    let window: Vec<String> = found
        .lines
        .into_iter()
        .skip(offset)
        .take(head_limit)
        .collect();
    let line_truncated = offset + window.len() < total;
    let (body, char_truncated) = truncate_chars(window.join("\n"), MAX_GREP_RESULT_CHARS);
    let truncated = line_truncated || char_truncated;
    let mut content = body;
    if truncated {
        content.push_str(RESULTS_TRUNCATED);
    }
    let mut output = ToolOutput::ok(content);
    output.metadata = Some(json!({
        "structuredResult": {
            "numLines": window.len(),
            "numFiles": found.files,
            "truncated": truncated,
        }
    }));
    output
}

/// 搜索产出（输出行 + 命中文件数）。
struct Found {
    /// 已渲染输出行（模式相关）。
    lines: Vec<String>,
    /// 命中文件数。
    files: usize,
}

/// 遍历候选文件并逐文件搜索（目录 → 递归遍历；单文件 → 直接搜）。
fn search(root: &Path, options: &Options) -> Found {
    let mut found = Found {
        lines: Vec::new(),
        files: 0,
    };
    if root.is_file() {
        scan_file(root, root, options, &mut found);
        return found;
    }
    let mut builder = ignore::WalkBuilder::new(root);
    builder
        .hidden(false)
        .follow_links(false)
        .filter_entry(|entry| {
            entry
                .file_name()
                .to_str()
                .is_none_or(|name| !VCS_EXCLUDE.contains(&name))
        });
    for entry in builder.build().flatten() {
        if found.lines.len() >= MAX_GREP_OUTPUT_LINES {
            break;
        }
        if !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        if accepts(root, entry.path(), options) {
            scan_file(root, entry.path(), options, &mut found);
        }
    }
    found
}

/// 路径过滤（glob 包含 / 排除 / 扩展名白名单）。
fn accepts(root: &Path, path: &Path, options: &Options) -> bool {
    let relative = path.strip_prefix(root).unwrap_or(path);
    if options
        .include
        .as_ref()
        .is_some_and(|matcher| !matcher.is_match(relative))
    {
        return false;
    }
    if options
        .exclude
        .as_ref()
        .is_some_and(|matcher| matcher.is_match(relative))
    {
        return false;
    }
    options.extensions.as_ref().is_none_or(|allowed| {
        path.extension()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|ext| allowed.iter().any(|candidate| candidate == ext))
    })
}

/// 单文件搜索（二进制 / 超大文件跳过；按模式渲染输出行）。
fn scan_file(root: &Path, path: &Path, options: &Options, found: &mut Found) {
    let Ok(metadata) = std::fs::metadata(path) else {
        return;
    };
    if metadata.len() > MAX_GREP_FILE_BYTES {
        return;
    }
    let Ok(bytes) = std::fs::read(path) else {
        return;
    };
    if bytes.iter().take(8 * 1024).any(|byte| *byte == 0) {
        return;
    }
    let text = String::from_utf8_lossy(&bytes);
    let label = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned();
    let hits = hit_lines(&text, options);
    if hits.is_empty() {
        return;
    }
    found.files += 1;
    match options.mode {
        Mode::FilesWithMatches => found.lines.push(label),
        Mode::Count => found.lines.push(format!("{label}:{}", hits.len())),
        Mode::Content => render_content(&text, &label, &hits, options, &mut found.lines),
    }
}

/// 命中行号集合（1-based；`multiline` 模式按匹配起点所在行归位）。
fn hit_lines(text: &str, options: &Options) -> Vec<usize> {
    if options.multiline {
        let mut lines: Vec<usize> = options
            .regex
            .find_iter(text)
            .map(|found| text[..found.start()].lines().count().max(1))
            .collect();
        lines.dedup();
        return lines;
    }
    text.lines()
        .enumerate()
        .filter(|(_, line)| options.regex.is_match(line))
        .map(|(index, _)| index + 1)
        .collect()
}

/// `content` 模式渲染（命中行 `path:line:text`、上下文行 `path-line-text`）。
fn render_content(
    text: &str,
    label: &str,
    hits: &[usize],
    options: &Options,
    out: &mut Vec<String>,
) {
    let lines: Vec<&str> = text.lines().collect();
    let mut emitted: Vec<usize> = Vec::new();
    for hit in hits {
        let from = hit.saturating_sub(options.before).max(1);
        let to = (hit + options.after).min(lines.len());
        for number in from..=to {
            if emitted.contains(&number) {
                continue;
            }
            emitted.push(number);
            let separator = if hits.contains(&number) { ':' } else { '-' };
            let body = lines.get(number - 1).copied().unwrap_or_default();
            out.push(format!("{label}{separator}{number}{separator}{body}"));
            if out.len() >= MAX_GREP_OUTPUT_LINES {
                return;
            }
        }
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

    /// 固定树：`a.rs`（两处命中）/ `b.txt`（一处命中）/ `c.rs`（无命中）。
    fn fixture(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("zk-grep-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir");
        std::fs::write(
            root.join("a.rs"),
            "fn alpha() {}\nlet x = 1;\nfn beta() {}\n",
        )
        .expect("w");
        std::fs::write(root.join("b.txt"), "prefix\nfn gamma() {}\nsuffix\n").expect("w");
        std::fs::write(root.join("c.rs"), "no hits here\n").expect("w");
        root
    }

    #[tokio::test]
    async fn lists_matching_files_by_default() {
        let root = fixture("files");
        let output = GrepTool
            .execute(json!({ "pattern": "^fn " }), ctx(&root))
            .await;
        assert!(!output.is_error, "{}", output.content);
        let mut lines: Vec<&str> = output.content.lines().collect();
        lines.sort_unstable();
        assert_eq!(lines, vec!["a.rs", "b.txt"]);
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["numFiles"], 2);
    }

    #[tokio::test]
    async fn content_mode_emits_line_numbers_and_context() {
        let root = fixture("content");
        let output = GrepTool
            .execute(
                json!({ "pattern": "beta", "output_mode": "content", "-B": 1, "type": "rust" }),
                ctx(&root),
            )
            .await;
        assert_eq!(output.content, "a.rs-2-let x = 1;\na.rs:3:fn beta() {}");
    }

    #[tokio::test]
    async fn count_mode_and_glob_filter_narrow_results() {
        let root = fixture("count");
        let output = GrepTool
            .execute(
                json!({ "pattern": "fn ", "output_mode": "count", "glob": "*.rs" }),
                ctx(&root),
            )
            .await;
        assert_eq!(output.content, "a.rs:2");
    }

    #[tokio::test]
    async fn head_limit_truncates_and_bad_inputs_are_rejected() {
        let root = fixture("limits");
        let capped = GrepTool
            .execute(
                json!({ "pattern": "fn ", "output_mode": "content", "head_limit": 1 }),
                ctx(&root),
            )
            .await;
        assert!(
            capped.content.ends_with(RESULTS_TRUNCATED),
            "{}",
            capped.content
        );

        let missing = GrepTool.execute(json!({}), ctx(&root)).await;
        assert!(missing.content.starts_with("MISSING_PARAMETER: "));

        let bad_regex = GrepTool
            .execute(json!({ "pattern": "a(" }), ctx(&root))
            .await;
        assert!(bad_regex.content.starts_with("GREP_PATTERN_INVALID: "));

        let bad_mode = GrepTool
            .execute(
                json!({ "pattern": "a", "output_mode": "weird" }),
                ctx(&root),
            )
            .await;
        assert!(bad_mode.content.starts_with("GREP_OUTPUT_MODE_INVALID: "));

        let absent = GrepTool
            .execute(json!({ "pattern": "a", "path": "nope" }), ctx(&root))
            .await;
        assert!(absent.content.starts_with("GREP_PATH_NOT_FOUND: "));
    }

    #[tokio::test]
    async fn case_insensitive_and_multiline_flags_apply() {
        let root = fixture("flags");
        let insensitive = GrepTool
            .execute(json!({ "pattern": "FN ALPHA", "-i": true }), ctx(&root))
            .await;
        assert_eq!(insensitive.content, "a.rs");

        let multiline = GrepTool
            .execute(
                json!({ "pattern": "alpha.*beta", "multiline": true, "glob": "a.rs" }),
                ctx(&root),
            )
            .await;
        assert_eq!(multiline.content, "a.rs");
    }
}
