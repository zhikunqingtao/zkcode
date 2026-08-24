//! 项目级记忆——项目根 `zhikun.md` / `zhikun.local.md` 的读写与提示段包裹
//! （Batch 5 Step 3）。
//!
//! 逐字对照旧 `service/ProjectMemoryService.java`（`loadMemory` / `writeMemory` /
//! `hasMemory`）与 `prompt/SystemPromptBuilder.java` L1102-1107 的
//! `loadMemoryPrompt`（`<project_memory>` 包裹）。
//!
//! # 与 [`crate::memdir`] 的分工
//!
//! 本模块是系统提示 `memory` 段的**唯一**数据源（旧 `SystemPromptBuilder` L179-180
//! 只调 `ProjectMemoryService.loadMemory`）；[`crate::memdir`] 的用户级
//! `~/.zk/MEMORY.md` 不参与提示注入（理由见该模块文档）。
//!
//! # 安全边界（旧实现逐条保留）
//!
//! 1. **根目录别名拒绝**：`工作目录的词法绝对化结果` 必须与其
//!    `canonicalize`（旧 `toRealPath`）结果**逐字相等**，且该路径在
//!    不跟随符号链接的意义上是目录；否则整体返回空（旧 `!projectRoot.equals(savedRoot)`）。
//!    这使「把项目根换绑到别处的符号链接」无法生效。
//! 2. **记忆文件符号链接拒绝**：`zhikun.md` 自身是符号链接、或不是常规文件
//!    （均按不跟随链接判定）时跳过。
//! 3. **越界二次校验**：文件的 `canonicalize` 结果必须仍在项目根之下，且与其
//!    词法绝对化路径相等（旧 `!realFile.startsWith(projectRoot) || !realFile.equals(...)`）。
//! 4. **体积截断**：单文件最多读 [`MAX_MEMORY_SIZE`] = 100 KiB（旧
//!    `input.readNBytes((int) MAX_MEMORY_SIZE)`——超限只截断 + 告警，不报错）。
//!
//! 写入侧额外拒绝路径穿越（`..` 逃出项目根）与「已存在但不是常规文件」，并在
//! `open` 时带 `O_NOFOLLOW`（对齐旧 `LinkOption.NOFOLLOW_LINKS` 写入选项，消除
//! 「检查后被换成符号链接」的 TOCTOU 窗口）。
//!
//! # Java 语义映射
//!
//! - `Path.toAbsolutePath()` → 相对路径以 [`std::env::current_dir`] 为基准拼接；
//! - `Path.normalize()` → [`zk_tools::file_state::normalize_path`]（纯词法，已对
//!   照 JDK `UnixPath.normalize` 逐字实现）；
//! - `Path.toRealPath()` → [`std::fs::canonicalize`]；
//! - `Files.isRegularFile(p, NOFOLLOW_LINKS)` → [`std::fs::symlink_metadata`] 后
//!   判 `is_file`（符号链接的 `symlink_metadata` 不是 file，与 NOFOLLOW 语义一致）；
//! - `new String(bytes, UTF_8)` → [`String::from_utf8_lossy`]（100 KiB 截断可能
//!   切断多字节字符，旧实现同样产出替换符）。

use std::io::Read;
use std::path::{Path, PathBuf};

/// 项目记忆文件名（旧 `MEMORY_FILES`，顺序即拼接顺序）。
pub const MEMORY_FILES: [&str; 2] = ["zhikun.md", "zhikun.local.md"];
/// 全局项目记忆文件名（旧 `isLocal == false` 分支）。
pub const PROJECT_MEMORY_FILE: &str = "zhikun.md";
/// 项目本地记忆文件名（旧 `isLocal == true` 分支，通常不入版本库）。
pub const PROJECT_MEMORY_LOCAL_FILE: &str = "zhikun.local.md";
/// 单文件读取上限（旧 `MAX_MEMORY_SIZE = 100 * 1024`）。
pub const MAX_MEMORY_SIZE: usize = 100 * 1024;
/// [`MAX_MEMORY_SIZE`] 的 `u64` 副本（`Read::take` 与文件大小比较用，避免 `as`
/// 转换）。两者一致性由单测 `max_memory_size_constants_agree` 互锁。
const MAX_MEMORY_SIZE_U64: u64 = 100 * 1024;
/// 多文件拼接分隔符（旧 `String.join("\n\n---\n\n", memories)`）。
const MEMORY_JOIN_SEPARATOR: &str = "\n\n---\n\n";

/// 加载项目记忆全文（旧 `loadMemory`）。
///
/// 返回按 [`MEMORY_FILES`] 顺序拼接的文本，每份前置 `<!-- {真实路径} -->` 行；
/// 无可用记忆、或根目录经别名解析（安全边界 1）时返回空串。任何 IO 失败都只告警
/// 并跳过该文件，**不**向上传播（旧实现同）。
#[must_use]
pub fn load_memory(working_dir: Option<&Path>) -> String {
    let Some(working_dir) = working_dir else {
        tracing::warn!("load_memory called with no working dir");
        return String::new();
    };
    let Some(project_root) = authorized_root(working_dir) else {
        return String::new();
    };

    let mut memories: Vec<String> = Vec::new();
    for file_name in MEMORY_FILES {
        let mem_file = project_root.join(file_name);
        if !is_regular_file_nofollow(&mem_file) {
            continue;
        }
        let Ok(real_file) = std::fs::canonicalize(&mem_file) else {
            tracing::warn!(file = %mem_file.display(), "failed to resolve memory file");
            continue;
        };
        // 旧 `!realFile.startsWith(projectRoot) || !realFile.equals(memFile.toAbsolutePath().normalize())`。
        if !real_file.starts_with(&project_root) || real_file != absolute_normalized(&mem_file) {
            tracing::warn!(
                file = %mem_file.display(),
                "ignoring project memory outside the authorized root"
            );
            continue;
        }
        match read_capped(&real_file) {
            Ok((bytes, size)) => {
                if size > MAX_MEMORY_SIZE_U64 {
                    tracing::warn!(
                        kib = size / 1024,
                        file = %real_file.display(),
                        "memory file too large, truncating"
                    );
                }
                let content = String::from_utf8_lossy(&bytes);
                memories.push(format!("<!-- {} -->\n{content}", real_file.display()));
                tracing::info!(file = %real_file.display(), bytes = size, "loaded memory file");
            }
            Err(error) => {
                tracing::warn!(file = %mem_file.display(), %error, "failed to read memory file");
            }
        }
    }

    if memories.is_empty() {
        return String::new();
    }
    memories.join(MEMORY_JOIN_SEPARATOR)
}

/// 系统提示 `memory` 段（旧 `SystemPromptBuilder.loadMemoryPrompt`，L1102-1107）。
///
/// 无记忆或全空白时返回 `None`（旧返回空串，随后被段过滤剔除）。产出串**保留旧
/// 仓的前导 `\n\n` 与尾随 `\n`**（旧 L1106 字面量），故与其余段以 `"\n\n"` 串接
/// 后的空行数与旧仓一致。
#[must_use]
pub fn memory_prompt_section(working_dir: Option<&Path>) -> Option<String> {
    let memory = load_memory(working_dir);
    if memory.trim().is_empty() {
        return None;
    }
    Some(format!(
        "\n\n<project_memory>\n{memory}\n</project_memory>\n"
    ))
}

/// 写入项目记忆（旧 `writeMemory`）。
///
/// `is_local` 为真写 [`PROJECT_MEMORY_LOCAL_FILE`]，否则写 [`PROJECT_MEMORY_FILE`]；
/// 恒截断覆盖（旧 `CREATE + TRUNCATE_EXISTING + WRITE`）。
///
/// # Errors
///
/// - 项目根不可用或经别名解析（旧 `Project memory root is unavailable or unsafe`）；
/// - 目标路径逃出项目根（旧 `Path traversal detected`）；
/// - 目标是符号链接（旧 `Refusing to write Project memory through a symbolic link`）；
/// - 目标已存在但不是常规文件（旧 `Project memory path is not a regular file`）；
/// - 底层写入失败。
pub fn write_memory(working_dir: &Path, content: &str, is_local: bool) -> std::io::Result<()> {
    let file_name = if is_local {
        PROJECT_MEMORY_LOCAL_FILE
    } else {
        PROJECT_MEMORY_FILE
    };
    let project_root = authorized_root(working_dir).ok_or_else(|| {
        std::io::Error::other("Project memory root is unavailable or unsafe".to_owned())
    })?;
    let mem_file = normalize(&project_root.join(file_name));

    if !mem_file.starts_with(&project_root) {
        return Err(std::io::Error::other(format!(
            "Path traversal detected: {}",
            mem_file.display()
        )));
    }
    match std::fs::symlink_metadata(&mem_file) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(std::io::Error::other(format!(
                "Refusing to write Project memory through a symbolic link: {}",
                mem_file.display()
            )));
        }
        // 旧 `Files.exists(NOFOLLOW) && !Files.isRegularFile(NOFOLLOW)`。
        Ok(metadata) if !metadata.is_file() => {
            return Err(std::io::Error::other(format!(
                "Project memory path is not a regular file: {}",
                mem_file.display()
            )));
        }
        _ => {}
    }

    write_nofollow(&mem_file, content.as_bytes())?;
    tracing::info!(file = %mem_file.display(), bytes = content.len(), "written memory file");
    Ok(())
}

/// 项目根是否存在任一记忆文件（旧 `hasMemory`）。
///
/// 旧实现在此**不**做根目录别名校验（直接 `workingDir.resolve`），本移植同——该
/// 方法只用于 UI 提示是否有记忆，不据此读取内容。
#[must_use]
pub fn has_memory(working_dir: &Path) -> bool {
    MEMORY_FILES
        .iter()
        .any(|file_name| is_regular_file_nofollow(&working_dir.join(file_name)))
}

// ==================== 内部辅助 ====================

/// 校验并返回授权项目根（旧 `loadMemory` / `writeMemory` 共同的前置四行）。
///
/// 词法绝对化路径必须与 `canonicalize` 结果逐字相等（拒绝符号链接别名），且不
/// 跟随链接地判定为目录；任一不满足返回 `None`。
fn authorized_root(working_dir: &Path) -> Option<PathBuf> {
    let saved_root = absolute_normalized(working_dir);
    let Ok(project_root) = std::fs::canonicalize(&saved_root) else {
        tracing::warn!(dir = %working_dir.display(), "project memory root is unavailable");
        return None;
    };
    if project_root != saved_root || !is_dir_nofollow(&project_root) {
        tracing::warn!(
            dir = %working_dir.display(),
            "project memory root is unavailable or resolves through an alias"
        );
        return None;
    }
    Some(project_root)
}

/// 旧 `path.toAbsolutePath().normalize()`。
fn absolute_normalized(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return normalize(path);
    }
    let base = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    normalize(&base.join(path))
}

/// 旧 `Path.normalize()`（纯词法，见模块文档的语义映射）。
fn normalize(path: &Path) -> PathBuf {
    zk_tools::file_state::normalize_path(path)
}

/// 旧 `Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS)`。
fn is_dir_nofollow(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir())
}

/// 旧 `Files.isSymbolicLink(p) || !Files.isRegularFile(p, NOFOLLOW_LINKS)` 的反面。
///
/// `symlink_metadata` 对符号链接返回链接自身的元数据（`is_file()` 为假），故单次
/// 系统调用即覆盖旧实现的两个判定。
fn is_regular_file_nofollow(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
}

/// 读取至多 [`MAX_MEMORY_SIZE`] 字节，同时回报文件真实大小（供越限告警）。
///
/// 对齐旧 `Files.size(realFile)` + `input.readNBytes((int) MAX_MEMORY_SIZE)`：
/// `open` 带 `O_NOFOLLOW`（旧 `Files.newInputStream(realFile, NOFOLLOW_LINKS)`）。
fn read_capped(path: &Path) -> std::io::Result<(Vec<u8>, u64)> {
    let mut file = open_nofollow(path)?;
    let size = file.metadata()?.len();
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_MEMORY_SIZE_U64)
        .read_to_end(&mut bytes)?;
    Ok((bytes, size))
}

/// 以 `O_NOFOLLOW` 只读打开（旧 `Files.newInputStream(p, NOFOLLOW_LINKS)`）。
fn open_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)
}

/// 以 `O_NOFOLLOW` 截断覆盖写（旧 `Files.writeString(..., CREATE, TRUNCATE_EXISTING,
/// WRITE, NOFOLLOW_LINKS)`）。
///
/// 非原子（不走 tmp + rename）——忠实还原旧行为：该文件是**用户手写**的项目记忆，
/// 旧仓的 `/memory init` 也直接覆盖写，改成原子替换会让用户在编辑器里打开的 inode
/// 与落盘文件脱钩。
fn write_nofollow(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 独占临时项目根（`canonicalize` 后返回，以通过别名校验——macOS 的
    /// `/tmp` 本身是 `/private/tmp` 的符号链接）。
    fn temp_project(tag: &str) -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("zk_projmem_{}_{tag}_{seq}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp project");
        std::fs::canonicalize(&dir).expect("canonicalize temp project")
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn load_memory_returns_empty_without_working_dir() {
        assert_eq!(load_memory(None), "");
        assert!(memory_prompt_section(None).is_none());
    }

    #[test]
    fn load_memory_returns_empty_for_missing_root() {
        let missing = Path::new("/nonexistent_zk_project_root_xyz");
        assert_eq!(load_memory(Some(missing)), "");
        assert!(!has_memory(missing));
    }

    #[test]
    fn load_memory_returns_empty_when_no_memory_files_exist() {
        let root = temp_project("empty");
        assert_eq!(load_memory(Some(&root)), "");
        assert!(!has_memory(&root));
        assert!(memory_prompt_section(Some(&root)).is_none());
        cleanup(&root);
    }

    #[test]
    fn load_memory_joins_both_files_in_declared_order() {
        let root = temp_project("both");
        std::fs::write(root.join(PROJECT_MEMORY_FILE), "GLOBAL BODY").expect("write global");
        std::fs::write(root.join(PROJECT_MEMORY_LOCAL_FILE), "LOCAL BODY").expect("write local");

        let loaded = load_memory(Some(&root));
        let global_marker = format!("<!-- {} -->", root.join(PROJECT_MEMORY_FILE).display());
        let local_marker = format!(
            "<!-- {} -->",
            root.join(PROJECT_MEMORY_LOCAL_FILE).display()
        );
        assert!(loaded.starts_with(&global_marker));
        assert!(loaded.contains(&local_marker));
        assert!(loaded.contains(MEMORY_JOIN_SEPARATOR));
        // 顺序即 MEMORY_FILES 顺序。
        assert!(loaded.find("GLOBAL BODY") < loaded.find("LOCAL BODY"));
        assert!(has_memory(&root));
        cleanup(&root);
    }

    #[test]
    fn memory_prompt_section_wraps_with_java_verbatim_padding() {
        let root = temp_project("wrap");
        std::fs::write(root.join(PROJECT_MEMORY_FILE), "BODY").expect("write");
        let section = memory_prompt_section(Some(&root)).expect("section");
        assert!(section.starts_with("\n\n<project_memory>\n"));
        assert!(section.ends_with("\n</project_memory>\n"));
        assert!(section.contains("BODY"));
        cleanup(&root);
    }

    #[test]
    fn memory_prompt_section_skips_blank_memory() {
        let root = temp_project("blank");
        // 文件存在但仅空白 → 拼出的正文只有注释头 + 空白，仍非空白，故会产出段。
        std::fs::write(root.join(PROJECT_MEMORY_FILE), "   \n").expect("write");
        assert!(memory_prompt_section(Some(&root)).is_some());
        cleanup(&root);
    }

    #[test]
    fn load_memory_truncates_at_max_size() {
        let root = temp_project("cap");
        // 填充字符必须不出现在临时目录路径里——注释头会内插真实路径。
        let oversized = "Q".repeat(MAX_MEMORY_SIZE + 4096);
        std::fs::write(root.join(PROJECT_MEMORY_FILE), &oversized).expect("write");
        let loaded = load_memory(Some(&root));
        let body_len = loaded.chars().filter(|c| *c == 'Q').count();
        assert_eq!(body_len, MAX_MEMORY_SIZE);
        cleanup(&root);
    }

    #[test]
    fn load_memory_skips_symlinked_memory_file() {
        let root = temp_project("symlink");
        let outside = root
            .parent()
            .expect("parent")
            .join(format!("zk_projmem_outside_{}.md", std::process::id()));
        std::fs::write(&outside, "SECRET").expect("write outside");
        std::os::unix::fs::symlink(&outside, root.join(PROJECT_MEMORY_FILE)).expect("symlink");

        // 符号链接不是常规文件（NOFOLLOW），整体跳过。
        assert_eq!(load_memory(Some(&root)), "");
        // `hasMemory` 用同一判定，故也为 false。
        assert!(!has_memory(&root));

        let _ = std::fs::remove_file(&outside);
        cleanup(&root);
    }

    #[test]
    fn load_memory_rejects_root_reached_through_symlink() {
        let root = temp_project("alias");
        std::fs::write(root.join(PROJECT_MEMORY_FILE), "BODY").expect("write");
        let alias = root
            .parent()
            .expect("parent")
            .join(format!("zk_projmem_alias_{}", std::process::id()));
        let _ = std::fs::remove_file(&alias);
        std::os::unix::fs::symlink(&root, &alias).expect("symlink root");

        // 经别名进入：词法绝对化 != canonicalize → 拒绝。
        assert_eq!(load_memory(Some(&alias)), "");
        assert!(write_memory(&alias, "x", false).is_err());

        let _ = std::fs::remove_file(&alias);
        cleanup(&root);
    }

    #[test]
    fn write_memory_creates_then_overwrites_global_file() {
        let root = temp_project("write");
        write_memory(&root, "first", false).expect("write");
        let path = root.join(PROJECT_MEMORY_FILE);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "first");

        // 截断覆盖（旧 TRUNCATE_EXISTING）：短内容不留旧尾巴。
        write_memory(&root, "v2", false).expect("rewrite");
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "v2");

        write_memory(&root, "local body", true).expect("write local");
        assert_eq!(
            std::fs::read_to_string(root.join(PROJECT_MEMORY_LOCAL_FILE)).expect("read"),
            "local body"
        );
        cleanup(&root);
    }

    #[test]
    fn write_memory_refuses_symlink_target() {
        let root = temp_project("write_symlink");
        let outside = root
            .parent()
            .expect("parent")
            .join(format!("zk_projmem_wsym_{}.md", std::process::id()));
        std::fs::write(&outside, "OLD").expect("write outside");
        std::os::unix::fs::symlink(&outside, root.join(PROJECT_MEMORY_FILE)).expect("symlink");

        let error = write_memory(&root, "pwned", false).expect_err("must refuse");
        assert!(error.to_string().contains("symbolic link"));
        // 链接目标未被改写。
        assert_eq!(std::fs::read_to_string(&outside).expect("read"), "OLD");

        let _ = std::fs::remove_file(&outside);
        cleanup(&root);
    }

    #[test]
    fn write_memory_refuses_non_regular_target() {
        let root = temp_project("write_dir");
        std::fs::create_dir(root.join(PROJECT_MEMORY_FILE)).expect("mkdir as memory path");
        let error = write_memory(&root, "x", false).expect_err("must refuse");
        assert!(error.to_string().contains("not a regular file"));
        cleanup(&root);
    }

    #[test]
    fn write_memory_rejects_unavailable_root() {
        let missing = Path::new("/nonexistent_zk_project_root_write");
        let error = write_memory(missing, "x", false).expect_err("must refuse");
        assert!(error.to_string().contains("unavailable or unsafe"));
    }

    #[test]
    fn has_memory_detects_either_file() {
        let root = temp_project("has");
        assert!(!has_memory(&root));
        std::fs::write(root.join(PROJECT_MEMORY_LOCAL_FILE), "x").expect("write");
        assert!(has_memory(&root));
        cleanup(&root);
    }

    #[test]
    fn max_memory_size_constants_agree() {
        assert_eq!(
            usize::try_from(MAX_MEMORY_SIZE_U64).expect("fits usize"),
            MAX_MEMORY_SIZE
        );
    }

    #[test]
    fn normalize_matches_jdk_lexical_semantics() {
        assert_eq!(normalize(Path::new("/a/./b/../c")), PathBuf::from("/a/c"));
        // 紧跟根之后的 `..` 被丢弃（JDK UnixPath.normalize）。
        assert_eq!(normalize(Path::new("/../x")), PathBuf::from("/x"));
    }
}
