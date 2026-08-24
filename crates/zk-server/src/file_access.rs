//! 会话文件访问服务层——搜索 / 预览 / 原生揭示（Batch 2 Step 2-4，旧
//! `service/FileSearchService.java`、`service/SessionFileAccessService.java`、
//! `security/ManagedWorkspacePathResolver.java` 的 `resolveProspective`
//! 只读分支）。
//!
//! # 层级归属
//!
//! 纯服务层：同步 fs 校验 + 原生子进程，不持 axum 类型（`ApiError` 除外）。
//! HTTP 侧（header/param 守卫、会话加载、`requireCurrentBinding`、
//! `spawn_blocking` 包裹、响应装配）见 `api::file`。会话加载与工作区绑定
//! 是 async DB / 服务调用，由 handler 先行完成后把已绑定的 `root` 传入本层。
//!
//! # 错误语义（逐字对照旧源）
//!
//! 旧源的 `ResponseStatusException(status, reason)` 经 `GlobalExceptionHandler
//! .handleResponseStatus` 映射为 `code == message == reason`；本层用
//! `workspace::failure(status, reason, reason)` 复刻。映射：
//!
//! | 场景 | HTTP | code=message |
//! |---|---|---|
//! | 空/空白 path | 400 | `FILE_PATH_REQUIRED` |
//! | 非常规文件 / 符号链接 / 越权解析 IO 失败 | 404 | `SESSION_FILE_NOT_FOUND` |
//! | 逃逸工作区 / 符号链接目标 / 非真实目录 | 403 | `SESSION_FILE_OUTSIDE_WORKSPACE` |
//! | 预览扩展名需原生打开 | 415 | `FILE_PREVIEW_REQUIRES_NATIVE_OPEN` |
//! | 预览超 50MB | 413 | `FILE_PREVIEW_TOO_LARGE` |
//! | 预览元数据不可读 | 409 | `FILE_PREVIEW_UNAVAILABLE` |
//! | 揭示非本地/回环 | 403 | `FILE_REVEAL_LOCAL_ONLY` |
//! | 揭示子进程失败 | 503 | `FILE_REVEAL_UNAVAILABLE` |
//!
//! # 偏离留痕
//!
//! - **F-01**：旧 `preview` 先 `Files.probeContentType`，null 再回退扩展名映射；
//!   Rust std 无 `probeContentType` 等价，且引入 mime 嗅探依赖不划算——直接用
//!   扩展名回退表（`fallback_content_type`），与旧回退分支逐条对齐。
//! - **F-02**：`resolveProspective` 的 `allowExternal=true` 分支（写工具的
//!   external 目标物化）不属本域，未移植；只保留 `preview`/`reveal` 用到的
//!   `allowExternal=false, requireBoundTarget=false` 只读分支。

use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::http::StatusCode;
use serde::Serialize;

use crate::error::ApiError;
use crate::workspace::{failure, lexical_normalize};

/// 预览体积上限（旧 `MAX_PREVIEW_BYTES = 50L * 1024 * 1024`）。
const MAX_PREVIEW_BYTES: u64 = 50 * 1024 * 1024;

/// 可内联预览的扩展名（旧 `INLINE_EXTENSIONS`，逐字照抄）。
const INLINE_EXTENSIONS: [&str; 42] = [
    "pdf",
    "png",
    "jpg",
    "jpeg",
    "gif",
    "webp",
    "bmp",
    "svg",
    "txt",
    "md",
    "markdown",
    "json",
    "yaml",
    "yml",
    "xml",
    "csv",
    "log",
    "js",
    "mjs",
    "cjs",
    "jsx",
    "ts",
    "tsx",
    "css",
    "scss",
    "less",
    "java",
    "kt",
    "kts",
    "py",
    "rb",
    "go",
    "rs",
    "c",
    "h",
    "cpp",
    "hpp",
    "sql",
    "properties",
    "toml",
    "ini",
    "conf",
];

/// 模糊搜索忽略目录（旧 `FileSearchService.IGNORED_DIRS`）。
const IGNORED_DIRS: [&str; 13] = [
    "node_modules",
    ".git",
    "target",
    "dist",
    "build",
    ".idea",
    "__pycache__",
    ".gradle",
    ".mvn",
    ".next",
    ".nuxt",
    "coverage",
    ".vscode",
];

/// 模糊搜索遍历深度（旧 `fuzzySearch(query, rootDir, limit)` → `..., 12`）。
const FUZZY_MAX_DEPTH: usize = 12;

/// 文件搜索结果（旧 `FileSearchService.FileSearchResult` record）。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FileSearchResult {
    /// 工作区相对路径。
    pub path: String,
    /// 文件名。
    pub name: String,
    /// `"file"` / `"directory"`。
    #[serde(rename = "type")]
    pub kind: String,
    /// 字节大小（目录/不可读为 0）。
    pub size: i64,
    /// 模糊匹配分（降序排序键）。
    pub score: f64,
}

/// 原生揭示结果（旧 `SessionFileAccessService.RevealResult` record）。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RevealResult {
    /// 恒 `true`（旧实现只在成功路径构造）。
    pub revealed: bool,
    /// 文件管理器标识（`FINDER` / `EXPLORER` / `FILE_MANAGER`）。
    pub application: String,
}

/// 预览目标（旧 `SessionFileAccessService.PreviewTarget` record；仅内部用于
/// 装配响应，不直接序列化）。
#[derive(Debug)]
pub(crate) struct PreviewTarget {
    /// 规范化后的绝对文件路径。
    pub path: PathBuf,
    /// 内容类型（用于 `Content-Type` 头）。
    pub content_type: String,
    /// 字节大小（用于 `Content-Length` 头）。
    pub size: u64,
}

/// `resolveProspective` 的两类异常（旧 `IllegalArgumentException` → 403、
/// `IOException` → 404）。
enum ProspectiveError {
    /// 逃逸工作区 / 符号链接目标 / 非真实目录（旧 `IllegalArgumentException`）。
    OutsideWorkspace,
    /// 工作区路径变动 / `toRealPath` IO 失败（旧 `IOException`）。
    NotFound,
}

/// 无跟随符号链接的存在性判定（旧 `Files.exists(p, NOFOLLOW)`）。
fn exists_no_follow(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

/// 无跟随符号链接的符号链接判定（旧 `Files.isSymbolicLink`）。
fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink())
}

/// 旧 `ManagedWorkspacePathResolver.assertRealDirectory`（L241-246）。
fn assert_real_directory(root: &Path, dir: &Path) -> Result<(), ProspectiveError> {
    let Ok(meta) = std::fs::symlink_metadata(dir) else {
        // 旧 `Files.isDirectory(dir, NOFOLLOW)` 遇 IO 失败返回 false → 取反成立
        // → IllegalArgumentException（403）。
        return Err(ProspectiveError::OutsideWorkspace);
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(ProspectiveError::OutsideWorkspace);
    }
    let real = std::fs::canonicalize(dir).map_err(|_| ProspectiveError::NotFound)?;
    if !real.starts_with(root) {
        return Err(ProspectiveError::OutsideWorkspace);
    }
    Ok(())
}

/// 旧 `validateExistingSegments`（L173-184）：逐段校验候选路径已存在的父级
/// 均为工作区内真实目录，且目标自身不是符号链接。
fn validate_existing_segments(root: &Path, candidate: &Path) -> Result<(), ProspectiveError> {
    if let Some(parent) = candidate.parent()
        && let Ok(relative) = parent.strip_prefix(root)
    {
        let mut current = root.to_path_buf();
        for segment in relative.components() {
            current = current.join(segment);
            if !exists_no_follow(&current) {
                break;
            }
            assert_real_directory(root, &current)?;
        }
    }
    if exists_no_follow(candidate) && is_symlink(candidate) {
        return Err(ProspectiveError::OutsideWorkspace);
    }
    Ok(())
}

/// 旧 `ManagedWorkspacePathResolver.resolveProspective(raw, workspaceRoot,
/// false, false)`（L88-124 的只读分支）。`root` 已是 `requireCurrentBinding`
/// 的 canonical 结果，故 `lexicalRoot == realRoot`。
fn resolve_prospective(raw: &Path, root: &Path) -> Result<PathBuf, ProspectiveError> {
    let lexical_root = lexical_normalize(root);
    let real_root = std::fs::canonicalize(&lexical_root).map_err(|_| ProspectiveError::NotFound)?;
    if real_root != lexical_root {
        // 旧 "workspace path changed"（IOException → 404）。
        return Err(ProspectiveError::NotFound);
    }
    let candidate = if raw.is_absolute() {
        let lexical_candidate = lexical_normalize(raw);
        if lexical_candidate.starts_with(&lexical_root) {
            // 旧 `root.resolve(lexicalRoot.relativize(lexicalCandidate)).normalize()`。
            match lexical_candidate.strip_prefix(&lexical_root) {
                Ok(rel) => lexical_normalize(&real_root.join(rel)),
                Err(_) => lexical_candidate,
            }
        } else {
            lexical_candidate
        }
    } else {
        lexical_normalize(&real_root.join(raw))
    };
    if candidate == real_root {
        // 旧 "target is the workspace root itself"（IllegalArgument → 403）。
        return Err(ProspectiveError::OutsideWorkspace);
    }
    if !candidate.starts_with(&real_root) {
        // 旧 "target escapes workspace"（IllegalArgument → 403）。
        return Err(ProspectiveError::OutsideWorkspace);
    }
    validate_existing_segments(&real_root, &candidate)?;
    Ok(candidate)
}

/// 空/空白 path 守卫（旧 `resolve` L114-116：`FILE_PATH_REQUIRED`）。
///
/// 单列成函数是因为旧 `resolve` 把此检查放在会话加载 / `requireCurrentBinding`
/// **之前**——handler 必须在这两步 async 调用前先调本函数以对齐错误顺序。
///
/// # Errors
/// path 为 null/空白时返回 400 `FILE_PATH_REQUIRED`。
pub(crate) fn require_path(raw_path: &str) -> Result<(), ApiError> {
    if raw_path.trim().is_empty() {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "FILE_PATH_REQUIRED",
            "FILE_PATH_REQUIRED",
        ));
    }
    Ok(())
}

/// 旧 `SessionFileAccessService.resolve` 的路径解析 + 文件校验部分（L116-152
/// 中会话加载 / `requireCurrentBinding` 之后的段落）。`root` 为已绑定工作区。
///
/// # Errors
/// 见模块级错误映射表（404 `SESSION_FILE_NOT_FOUND` / 403
/// `SESSION_FILE_OUTSIDE_WORKSPACE`）。
pub(crate) fn resolve_within(root: &Path, raw_path: &str) -> Result<PathBuf, ApiError> {
    let not_found = || {
        failure(
            StatusCode::NOT_FOUND,
            "SESSION_FILE_NOT_FOUND",
            "SESSION_FILE_NOT_FOUND",
        )
    };
    let outside = || {
        failure(
            StatusCode::FORBIDDEN,
            "SESSION_FILE_OUTSIDE_WORKSPACE",
            "SESSION_FILE_OUTSIDE_WORKSPACE",
        )
    };
    let prospective = resolve_prospective(Path::new(raw_path), root).map_err(|err| match err {
        ProspectiveError::OutsideWorkspace => outside(),
        ProspectiveError::NotFound => not_found(),
    })?;
    // 旧 `!isRegularFile(NOFOLLOW) || isSymbolicLink` → 404。
    let meta = std::fs::symlink_metadata(&prospective).map_err(|_| not_found())?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(not_found());
    }
    // 旧 `canonical = toRealPath()`（IO 失败 → 404），随后 canonical 必须等于
    // 词法归一且落在 root 内，否则 403。
    let canonical = std::fs::canonicalize(&prospective).map_err(|_| not_found())?;
    if canonical != lexical_normalize(&prospective) || !canonical.starts_with(root) {
        return Err(outside());
    }
    Ok(canonical)
}

/// 扩展名（旧 `extension`：最后一个 `.` 之后小写，无 `.` 为 ""）。
fn extension(path: &Path) -> String {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    match name.rfind('.') {
        Some(dot) => name[dot + 1..].to_lowercase(),
        None => String::new(),
    }
}

/// 扩展名 → 内容类型回退表（旧 `fallbackContentType`，逐条照抄）。
fn fallback_content_type(extension: &str) -> String {
    let mapped = match extension {
        "pdf" => "application/pdf",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "json" => "application/json;charset=UTF-8",
        "md" | "markdown" => "text/markdown;charset=UTF-8",
        _ => "text/plain;charset=UTF-8",
    };
    mapped.to_owned()
}

/// 旧 `preview` 的解析后段落（L52-70）：扩展名内联判定（415）→ 体积
/// （413）→ 内容类型。`canonical` 为 [`resolve_within`] 的结果。
///
/// # Errors
/// 415 `FILE_PREVIEW_REQUIRES_NATIVE_OPEN` / 413 `FILE_PREVIEW_TOO_LARGE` /
/// 409 `FILE_PREVIEW_UNAVAILABLE`。
pub(crate) fn preview_target(canonical: &Path) -> Result<PreviewTarget, ApiError> {
    let ext = extension(canonical);
    if !INLINE_EXTENSIONS.contains(&ext.as_str()) {
        return Err(failure(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "FILE_PREVIEW_REQUIRES_NATIVE_OPEN",
            "FILE_PREVIEW_REQUIRES_NATIVE_OPEN",
        ));
    }
    // 旧 try 块：Files.size 失败 → 409（IOException）。
    let size = std::fs::metadata(canonical)
        .map(|meta| meta.len())
        .map_err(|_| {
            failure(
                StatusCode::CONFLICT,
                "FILE_PREVIEW_UNAVAILABLE",
                "FILE_PREVIEW_UNAVAILABLE",
            )
        })?;
    if size > MAX_PREVIEW_BYTES {
        return Err(failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            "FILE_PREVIEW_TOO_LARGE",
            "FILE_PREVIEW_TOO_LARGE",
        ));
    }
    Ok(PreviewTarget {
        path: canonical.to_path_buf(),
        content_type: fallback_content_type(&ext),
        size,
    })
}

/// 旧 `SessionFileAccessService.revealInFileManager`（L88-113）：按 OS 分派
/// 系统命令揭示文件，5s 超时或非零退出 → 失败。
///
/// # Errors
/// 子进程启动/超时/非零退出时返回 503 `FILE_REVEAL_UNAVAILABLE`（旧
/// `reveal` 的 `catch Exception` 分支）。
pub(crate) fn reveal_path(canonical: &Path) -> Result<RevealResult, ApiError> {
    let unavailable = || {
        failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "FILE_REVEAL_UNAVAILABLE",
            "FILE_REVEAL_UNAVAILABLE",
        )
    };
    let os = std::env::consts::OS;
    let (mut command, application) = if os == "macos" {
        let mut cmd = std::process::Command::new("/usr/bin/open");
        cmd.arg("-R").arg(canonical);
        (cmd, "FINDER")
    } else if os == "windows" {
        let mut cmd = std::process::Command::new("explorer.exe");
        cmd.arg(format!("/select,{}", canonical.display()));
        (cmd, "EXPLORER")
    } else {
        let parent = canonical.parent().unwrap_or(canonical);
        let mut cmd = std::process::Command::new("xdg-open");
        cmd.arg(parent);
        (cmd, "FILE_MANAGER")
    };
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut child = command.spawn().map_err(|_| unavailable())?;
    match child.wait_timeout(Duration::from_secs(5)) {
        Ok(Some(status)) if status.success() => Ok(RevealResult {
            revealed: true,
            application: application.to_owned(),
        }),
        _ => {
            let _ = child.kill();
            Err(unavailable())
        }
    }
}

/// `std::process::Child` 的带超时等待（旧 `process.waitFor(5, SECONDS)`）。
/// std 无原生超时 API，用短轮询实现；`unsafe` 禁用，不引 wait-timeout 依赖。
trait WaitTimeout {
    fn wait_timeout(
        &mut self,
        timeout: Duration,
    ) -> std::io::Result<Option<std::process::ExitStatus>>;
}

impl WaitTimeout for std::process::Child {
    fn wait_timeout(
        &mut self,
        timeout: Duration,
    ) -> std::io::Result<Option<std::process::ExitStatus>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

/// 模糊匹配分（旧 `fuzzyScore`，逐字照抄；`score` 为 int 累加后转 double）。
fn fuzzy_score(query: &str, target: &str) -> f64 {
    let lq: Vec<char> = query.to_lowercase().chars().collect();
    let lt: Vec<char> = target.to_lowercase().chars().collect();
    let mut qi: usize = 0;
    let mut score: i32 = 0;
    let mut consecutive: i32 = 0;
    let mut ti: usize = 0;
    while ti < lt.len() && qi < lq.len() {
        if lt[ti] == lq[qi] {
            qi += 1;
            consecutive += 1;
            score += consecutive * 2;
            if ti == 0 || lt[ti - 1] == '/' || lt[ti - 1] == '.' {
                score += 5;
            }
        } else {
            consecutive = 0;
        }
        ti += 1;
    }
    if qi == lq.len() {
        f64::from(score)
    } else {
        0.0
    }
}

/// 相对路径逐段是否命中忽略目录（旧 `isIgnored`）。
fn is_ignored(path: &Path, root: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    relative
        .components()
        .any(|comp| IGNORED_DIRS.contains(&comp.as_os_str().to_string_lossy().as_ref()))
}

/// 安全取大小（旧 `safeSize`：常规文件取 size，否则/异常为 0）。
fn safe_size(path: &Path) -> i64 {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => i64::try_from(meta.len()).unwrap_or(i64::MAX),
        _ => 0,
    }
}

/// `Files.walk(root, maxDepth)` 的无跟随符号链接等价：收集 root 下 1..=maxDepth
/// 层的全部条目（含目录），不递归进符号链接目录。
fn walk_collect(dir: &Path, depth: usize, max_depth: usize, out: &mut Vec<PathBuf>) {
    if depth >= max_depth {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let file_type = entry.file_type();
        out.push(path.clone());
        if let Ok(ft) = file_type
            && ft.is_dir()
            && !ft.is_symlink()
        {
            walk_collect(&path, depth + 1, max_depth, out);
        }
    }
}

/// 旧 `FileSearchService.fuzzySearch(query, rootDir, limit)`（深度 12）：
/// 遍历工作区、模糊打分、按分降序截断。query 空白或 root 非目录 → 空列表。
pub(crate) fn fuzzy_search(query: &str, root: &Path, limit: usize) -> Vec<FileSearchResult> {
    if query.trim().is_empty() {
        return Vec::new();
    }
    if !root.is_dir() {
        return Vec::new();
    }
    let mut paths = Vec::new();
    walk_collect(root, 0, FUZZY_MAX_DEPTH, &mut paths);
    let mut results: Vec<FileSearchResult> = paths
        .into_iter()
        .filter(|p| !is_ignored(p, root) && !is_symlink(p))
        .filter_map(|p| {
            let relative = p.strip_prefix(root).ok()?.to_string_lossy().into_owned();
            if relative.is_empty() {
                return None;
            }
            let score = fuzzy_score(query, &relative);
            if score <= 0.0 {
                return None;
            }
            let name = p
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let kind = if p.is_dir() { "directory" } else { "file" };
            Some(FileSearchResult {
                path: relative,
                name,
                kind: kind.to_owned(),
                size: safe_size(&p),
                score,
            })
        })
        .collect();
    // 稳定降序（Java Stream.sorted 稳定；Rust sort_by 稳定）。
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(limit);
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn inline_extensions_cover_legacy_set() {
        // 旧集合 42 个去重成员必须全部命中。
        for ext in ["pdf", "rs", "toml", "conf", "markdown", "tsx"] {
            assert!(INLINE_EXTENSIONS.contains(&ext), "{ext}");
        }
        assert!(!INLINE_EXTENSIONS.contains(&"exe"));
    }

    #[test]
    fn extension_lowercases_after_last_dot() {
        assert_eq!(extension(Path::new("/a/b/Foo.MD")), "md");
        assert_eq!(extension(Path::new("/a/b/archive.tar.GZ")), "gz");
        assert_eq!(extension(Path::new("/a/b/README")), "");
    }

    #[test]
    fn fallback_content_type_matches_legacy_switch() {
        assert_eq!(fallback_content_type("pdf"), "application/pdf");
        assert_eq!(fallback_content_type("jpeg"), "image/jpeg");
        assert_eq!(
            fallback_content_type("json"),
            "application/json;charset=UTF-8"
        );
        assert_eq!(fallback_content_type("md"), "text/markdown;charset=UTF-8");
        assert_eq!(fallback_content_type("unknown"), "text/plain;charset=UTF-8");
    }

    #[test]
    fn fuzzy_score_prefix_and_boundary_bonus() {
        // 完整子序列命中 → 正分；boundary（起始/`/`/`.` 后）额外加分。
        assert!(fuzzy_score("ab", "ab") > 0.0);
        assert!(fuzzy_score("app", "src/App.tsx") > 0.0);
        // 非子序列 → 0。
        assert!(fuzzy_score("xyz", "abc").abs() < f64::EPSILON);
        // 空查询在 fuzzy_search 层短路；此处 lq 空 → qi==0==len → 返回 score 0。
        assert!(fuzzy_score("", "anything").abs() < f64::EPSILON);
    }

    fn tmp_root() -> PathBuf {
        let base = std::env::temp_dir().join(format!("zk-file-access-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&base).expect("mkroot");
        // canonicalize：对齐 requireCurrentBinding 传入的 canonical root。
        fs::canonicalize(&base).expect("canon")
    }

    #[test]
    fn resolve_within_accepts_in_tree_regular_file() {
        let root = tmp_root();
        fs::write(root.join("hello.txt"), b"hi").expect("write");
        let resolved = resolve_within(&root, "hello.txt").expect("resolve");
        assert!(resolved.starts_with(&root));
        assert!(resolved.ends_with("hello.txt"));
    }

    #[test]
    fn resolve_within_rejects_escape_as_forbidden() {
        let root = tmp_root();
        let err = resolve_within(&root, "../etc/passwd").expect_err("escape");
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert_eq!(err.code, "SESSION_FILE_OUTSIDE_WORKSPACE");
    }

    #[test]
    fn resolve_within_missing_file_is_not_found() {
        let root = tmp_root();
        let err = resolve_within(&root, "nope.txt").expect_err("missing");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
        assert_eq!(err.code, "SESSION_FILE_NOT_FOUND");
    }

    #[test]
    fn preview_target_rejects_non_inline_extension() {
        let root = tmp_root();
        fs::write(root.join("data.bin"), b"x").expect("write");
        let path = resolve_within(&root, "data.bin").expect("resolve");
        let err = preview_target(&path).expect_err("415");
        assert_eq!(err.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(err.code, "FILE_PREVIEW_REQUIRES_NATIVE_OPEN");
    }

    #[test]
    fn preview_target_ok_for_inline_extension() {
        let root = tmp_root();
        fs::write(root.join("note.md"), b"hello").expect("write");
        let path = resolve_within(&root, "note.md").expect("resolve");
        let target = preview_target(&path).expect("preview");
        assert_eq!(target.size, 5);
        assert_eq!(target.content_type, "text/markdown;charset=UTF-8");
    }

    #[test]
    fn require_path_rejects_blank() {
        let err = require_path("   ").expect_err("blank");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "FILE_PATH_REQUIRED");
        assert!(require_path("a.txt").is_ok());
    }

    #[test]
    fn fuzzy_search_finds_and_ignores() {
        let root = tmp_root();
        fs::write(root.join("main.rs"), b"x").expect("write");
        fs::create_dir_all(root.join("node_modules/pkg")).expect("mkdir");
        fs::write(root.join("node_modules/pkg/main.rs"), b"x").expect("write");
        let hits = fuzzy_search("main", &root, 20);
        assert!(hits.iter().any(|r| r.path == "main.rs"));
        assert!(
            !hits.iter().any(|r| r.path.contains("node_modules")),
            "ignored dir must be skipped"
        );
        // 空白查询短路。
        assert!(fuzzy_search("  ", &root, 20).is_empty());
    }
}
