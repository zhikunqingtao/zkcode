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
    RESULTS_TRUNCATED, bool_or, failure, optional_str, required_str, resolve_path, truncate_chars,
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
                "head_limit": { "type": "integer", "description": "32-bit integer maximum result lines (default 250); <=0 disables paging and ignores offset, total output caps still apply." },
                "offset": { "type": "integer", "description": "Non-negative 32-bit integer lines to skip; offset + head_limit + 1 must fit in 2147483647." }
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
    let (head_limit, offset) = match paging(&input) {
        Ok(paging) => paging,
        Err(output) => return output,
    };
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
    let context = if mode == Mode::Content {
        context_lines(input, "-C", 0)?
    } else {
        0
    };
    let before = if mode == Mode::Content {
        context_lines(input, "-B", context)?
    } else {
        0
    };
    let after = if mode == Mode::Content {
        context_lines(input, "-A", context)?
    } else {
        0
    };
    Ok(Options {
        regex,
        multiline,
        mode,
        before,
        after,
        include: compile_glob(
            optional_str(input, "glob").or_else(|| optional_str(input, "include")),
        )?,
        exclude: compile_glob(optional_str(input, "exclude"))?,
        extensions: optional_str(input, "type").map(extensions_for),
    })
}

fn exact_i32(input: &serde_json::Value, key: &str, default: i32) -> Result<i32, ToolOutput> {
    let Some(raw) = input.get(key).filter(|value| !value.is_null()) else {
        return Ok(default);
    };
    let parsed = raw
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .or_else(|| raw.as_str().and_then(|value| value.parse::<i32>().ok()))
        .or_else(|| {
            raw.as_f64()
                .filter(|value| value.fract() == 0.0)
                .and_then(|value| value.to_string().parse::<i32>().ok())
        });
    parsed.ok_or_else(|| {
        failure(
            "GREP_ARGUMENT_INVALID",
            format!("{key} must be a 32-bit integer"),
        )
    })
}

fn context_lines(
    input: &serde_json::Value,
    key: &str,
    default: usize,
) -> Result<usize, ToolOutput> {
    let value = exact_i32(input, key, i32::try_from(default).unwrap_or(i32::MAX))?;
    usize::try_from(value).map_err(|_| {
        failure(
            "GREP_ARGUMENT_INVALID",
            format!("{key} must be non-negative in content mode"),
        )
    })
}

fn paging(input: &serde_json::Value) -> Result<(usize, usize), ToolOutput> {
    let head = exact_i32(
        input,
        "head_limit",
        i32::try_from(DEFAULT_GREP_HEAD_LIMIT).unwrap_or(250),
    )?;
    if head <= 0 {
        return Ok((MAX_GREP_OUTPUT_LINES, 0));
    }
    let offset = exact_i32(input, "offset", 0)?;
    if offset < 0 || i64::from(offset) + i64::from(head) + 1 > i64::from(i32::MAX) {
        return Err(failure(
            "GREP_ARGUMENT_INVALID",
            "offset must be non-negative and offset + head_limit + 1 must not exceed 2147483647",
        ));
    }
    Ok((
        usize::try_from(head)
            .unwrap_or(MAX_GREP_OUTPUT_LINES)
            .min(MAX_GREP_OUTPUT_LINES),
        usize::try_from(offset).unwrap_or(0),
    ))
}

fn protected_descendant(name: &str, directory: bool) -> bool {
    use zk_core::protected_paths::{DANGEROUS_DIRECTORIES, DANGEROUS_FILES};
    if directory {
        DANGEROUS_DIRECTORIES
            .iter()
            .any(|entry| name.eq_ignore_ascii_case(entry))
    } else {
        name.to_ascii_lowercase().starts_with(".env")
            || DANGEROUS_FILES
                .iter()
                .any(|entry| name.eq_ignore_ascii_case(entry))
    }
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
    let truncated = found.truncated || line_truncated || char_truncated;
    let mut content = body;
    if truncated {
        content.push_str(RESULTS_TRUNCATED);
    }
    let mut output = ToolOutput::ok(content);
    output.metadata = Some(json!({
        "structuredResult": {
            "numLines": window.len(),
            "numFiles": found.files,
            "keyFileReferences": found.key_files,
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
    /// Bounded successful file matches for authorized context reload selection.
    key_files: Vec<String>,
    /// The global line cap stopped discovery before the search was exhausted.
    truncated: bool,
}

/// 遍历候选文件并逐文件搜索（目录 → 递归遍历；单文件 → 直接搜）。
fn search(root: &Path, options: &Options) -> Found {
    let mut found = Found {
        lines: Vec::new(),
        files: 0,
        key_files: Vec::new(),
        truncated: false,
    };
    if root.is_file() {
        scan_file(root, root, options, &mut found);
        apply_global_line_cap(&mut found);
        return found;
    }
    let mut builder = ignore::WalkBuilder::new(root);
    builder
        .hidden(false)
        .follow_links(false)
        .sort_by_file_path(std::cmp::Ord::cmp)
        .filter_entry(|entry| {
            // The explicit root has already passed the authorization gateway.
            // Exclusions apply only below it, including nested names matching it.
            entry.depth() == 0
                || entry.file_name().to_str().is_none_or(|name| {
                    !VCS_EXCLUDE.contains(&name)
                        && !protected_descendant(
                            name,
                            entry.file_type().is_some_and(|kind| kind.is_dir()),
                        )
                })
        });
    for entry in builder.build().flatten() {
        if found.lines.len() > MAX_GREP_OUTPUT_LINES {
            break;
        }
        if !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        if accepts(root, entry.path(), options) {
            scan_file(root, entry.path(), options, &mut found);
        }
    }
    apply_global_line_cap(&mut found);
    found
}

fn apply_global_line_cap(found: &mut Found) {
    if found.lines.len() > MAX_GREP_OUTPUT_LINES {
        found.lines.truncate(MAX_GREP_OUTPUT_LINES);
        found.truncated = true;
    }
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
    let relative = path.strip_prefix(root).unwrap_or(path);
    let label = if relative.as_os_str().is_empty() {
        path
    } else {
        relative
    }
    .to_string_lossy()
    .into_owned();
    let hits = hit_lines(&text, options);
    if hits.is_empty() {
        return;
    }
    found.files += 1;
    if found.key_files.len() < 20 {
        found.key_files.push(path.to_string_lossy().into_owned());
    }
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
        let to = hit.saturating_add(options.after).min(lines.len());
        for number in from..=to {
            if emitted.contains(&number) {
                continue;
            }
            emitted.push(number);
            let separator = if hits.contains(&number) { ':' } else { '-' };
            let body = lines.get(number - 1).copied().unwrap_or_default();
            out.push(format!("{label}{separator}{number}{separator}{body}"));
            if out.len() > MAX_GREP_OUTPUT_LINES {
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

    #[test]
    fn exact_paging_context_validation_and_disabled_offset() {
        assert_eq!(
            paging(&json!({"head_limit":0,"offset":"ignored"})).unwrap(),
            (MAX_GREP_OUTPUT_LINES, 0)
        );
        assert_eq!(
            paging(&json!({"head_limit":-1,"offset":-9})).unwrap(),
            (MAX_GREP_OUTPUT_LINES, 0)
        );
        assert_eq!(
            paging(&json!({"head_limit":"2","offset":1})).unwrap(),
            (2, 1)
        );
        for value in [
            json!({"head_limit":1.5}),
            json!({"offset":-1}),
            json!({"head_limit":2_147_483_647_i64}),
            json!({"offset":2_147_483_647_i64}),
        ] {
            assert!(
                paging(&value)
                    .unwrap_err()
                    .content
                    .starts_with("GREP_ARGUMENT_INVALID")
            );
        }
        assert!(parse(&json!({"pattern":"x", "output_mode":"content", "-A":-1})).is_err());
        assert!(parse(&json!({"pattern":"x", "output_mode":"count", "-A":-1})).is_ok());
    }

    #[tokio::test]
    async fn broad_search_excludes_protected_descendants_but_explicit_root_is_searchable() {
        let root = fixture("protected");
        let explicit = root.join("node_modules");
        std::fs::create_dir_all(explicit.join("node_modules")).unwrap();
        std::fs::write(explicit.join("visible.js"), "fn package").unwrap();
        std::fs::write(explicit.join("node_modules/hidden.js"), "fn nested").unwrap();
        std::fs::write(root.join(".ENV.production"), "fn secret").unwrap();
        let broad = GrepTool
            .execute(json!({"pattern":"fn", "glob":"**/*"}), ctx(&root))
            .await;
        assert!(
            !broad.content.contains("visible")
                && !broad.content.contains("hidden")
                && !broad.content.contains("ENV")
        );
        let narrow = GrepTool
            .execute(json!({"pattern":"fn", "path":explicit}), ctx(&root))
            .await;
        assert_eq!(narrow.content, "visible.js");
        let single = GrepTool
            .execute(
                json!({"pattern":"fn", "path":root.join("a.rs")}),
                ctx(&root),
            )
            .await;
        assert!(single.content.ends_with("a.rs"));
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
