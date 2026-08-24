//! 路径安全验证器——对照旧 `tool/bash/PathValidator.java`（全 334 行，只读权威规格）。
//!
//! 核心功能（旧源 L15-20）：
//! 1. 路径规范化 + 符号链接解析；
//! 2. 项目边界检查（禁止操作工作目录外的文件）；
//! 3. 危险路径删除检测；
//! 4. 输出重定向验证；
//! 5. 进程替换检测。
//!
//! **偏离登记**：旧源 `PathValidator` 虽被 `BashSecurityAnalyzer` 构造注入
//! （旧源 `BashSecurityAnalyzer.java` L155/L162），但该字段在 main@581d407b 中
//! **从未被调用**（死代码）；本移植仍逐字还原以保证实现不缩水。
//! 另：`check_project_boundary` 在旧源 L227-230 已被显式禁用（恒 `return null`），
//! 本移植原样保留该禁用语义，不得"修好"。留痕 docs/compatibility.md §5。

use std::path::{Component, Path, PathBuf};

/// 文件操作类型——对照旧源 `FileOperationType` L29。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileOperationType {
    /// 只读。
    Read,
    /// 只写。
    Write,
    /// 读写。
    ReadWrite,
    /// 不涉及文件。
    None,
}

/// 命令 → 操作类型映射（32 条）——对照旧源 `COMMAND_OPERATION_TYPE` L32-65。
const COMMAND_OPERATION_TYPE: &[(&str, FileOperationType)] = &[
    ("cd", FileOperationType::Read),
    ("ls", FileOperationType::Read),
    ("find", FileOperationType::Read),
    ("cat", FileOperationType::Read),
    ("head", FileOperationType::Read),
    ("tail", FileOperationType::Read),
    ("grep", FileOperationType::Read),
    ("rg", FileOperationType::Read),
    ("ag", FileOperationType::Read),
    ("diff", FileOperationType::Read),
    ("wc", FileOperationType::Read),
    ("file", FileOperationType::Read),
    ("stat", FileOperationType::Read),
    ("readlink", FileOperationType::Read),
    ("realpath", FileOperationType::Read),
    ("du", FileOperationType::Read),
    ("jq", FileOperationType::Read),
    ("mkdir", FileOperationType::Write),
    ("rm", FileOperationType::Write),
    ("rmdir", FileOperationType::Write),
    ("touch", FileOperationType::Write),
    ("mv", FileOperationType::Write),
    ("cp", FileOperationType::Write),
    ("ln", FileOperationType::Write),
    ("chmod", FileOperationType::Write),
    ("chown", FileOperationType::Write),
    ("sed", FileOperationType::ReadWrite),
    ("awk", FileOperationType::ReadWrite),
    ("tee", FileOperationType::ReadWrite),
    ("git", FileOperationType::ReadWrite),
    ("tar", FileOperationType::ReadWrite),
    ("zip", FileOperationType::ReadWrite),
];

/// 危险删除路径（17 条）——对照旧源 `DANGEROUS_REMOVAL_PATHS` L69-73。
const DANGEROUS_REMOVAL_PATHS: &[&str] = &[
    "/",
    "/bin",
    "/sbin",
    "/usr",
    "/usr/bin",
    "/usr/sbin",
    "/etc",
    "/var",
    "/tmp",
    "/opt",
    "/lib",
    "/lib64",
    "/boot",
    "/dev",
    "/proc",
    "/sys",
    "/run",
];

/// 受保护的隐藏目录（6 条）——对照旧源 `PROTECTED_HIDDEN_DIRS` L77-79。
const PROTECTED_HIDDEN_DIRS: &[&str] = &[".git", ".ssh", ".gnupg", ".aws", ".config", ".env"];

/// POSIX end-of-options 标记——对照旧源 `END_OF_OPTIONS` L82。
const END_OF_OPTIONS: &str = "--";

/// 查表取命令的操作类型——等价旧源 `COMMAND_OPERATION_TYPE.getOrDefault(cmd, NONE)`。
fn operation_type(command: &str) -> FileOperationType {
    COMMAND_OPERATION_TYPE
        .iter()
        .find(|(k, _)| *k == command)
        .map_or(FileOperationType::None, |(_, v)| *v)
}

/// 路径安全验证器——对照旧源 `PathValidator` L23-334。
#[derive(Clone, Copy, Debug, Default)]
pub struct PathValidator;

impl PathValidator {
    /// 构造验证器（旧源为无状态 `@Component`）。
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// 从命令参数中提取路径参数——对照旧源 `extractPaths` L89-107。
    #[must_use]
    pub fn extract_paths(&self, command: &str, args: &[String]) -> Vec<String> {
        let non_flag_args = Self::filter_out_flags(args);
        let op_type = operation_type(command);

        if op_type == FileOperationType::None {
            return Vec::new();
        }

        // 旧源 L95-106 为逐命令独立 `case` 分支，部分分支返回值相同（`nonFlagArgs`）；
        // 这里原样保留分支结构以对齐判定顺序与可审计性，故显式放行 `match_same_arms`。
        #[allow(clippy::match_same_arms)]
        match command {
            "cd" | "mkdir" | "rmdir" | "touch" => non_flag_args
                .first()
                .map_or_else(Vec::new, |first| vec![first.clone()]),
            // rm 的所有非 flag 参数都是路径；mv/cp/ln 源和目标都需要验证
            "rm" | "mv" | "cp" | "ln" => non_flag_args,
            "cat" | "head" | "tail" | "wc" | "stat" | "file" | "readlink" | "realpath" | "du" => {
                non_flag_args
            }
            // find 第一个参数是搜索路径
            "find" => non_flag_args
                .first()
                .map_or_else(|| vec![".".to_owned()], |first| vec![first.clone()]),
            "grep" | "rg" | "ag" | "chmod" | "chown" => {
                if non_flag_args.len() > 1 {
                    non_flag_args[1..].to_vec()
                } else {
                    Vec::new()
                }
            }
            "sed" => Self::extract_sed_paths(args),
            "git" => Self::extract_git_paths(args),
            _ => non_flag_args,
        }
    }

    /// 核心：验证命令路径安全性——对照旧源 `validateCommandPaths` L118-154。
    ///
    /// 返回 `None` 表示安全；`Some(reason)` 为拒绝原因。
    #[must_use]
    pub fn validate_command_paths(
        &self,
        command: &str,
        args: &[String],
        cwd: &Path,
        project_root: &Path,
    ) -> Option<String> {
        // 1. 提取路径参数
        let paths = self.extract_paths(command, args);
        if paths.is_empty() {
            return None;
        }

        let op_type = operation_type(command);

        for path_str in paths {
            if path_str.trim().is_empty() {
                continue;
            }

            // 2. 路径规范化
            let Some(resolved_path) = Self::resolve_path(&path_str, cwd) else {
                continue;
            };

            // 3. 符号链接解析
            let real_path = Self::resolve_symlink(&resolved_path);

            // 4. 项目边界检查（仅对写操作）
            if (op_type == FileOperationType::Write || op_type == FileOperationType::ReadWrite)
                && let Some(boundary_check) = Self::check_project_boundary(&real_path, project_root)
            {
                return Some(boundary_check);
            }

            // 5. 危险删除路径检查
            if command == "rm"
                && let Some(danger_check) =
                    Self::check_dangerous_removal_paths(command, args, &real_path)
            {
                return Some(danger_check);
            }

            // 6. 受保护隐藏目录检查
            if let Some(hidden_check) = Self::check_protected_hidden_dirs(&real_path, op_type) {
                return Some(hidden_check);
            }
        }

        None // 全部通过
    }

    /// 入口：路径约束检查——对照旧源 `checkPathConstraints` L159-174。
    #[must_use]
    pub fn check_path_constraints(
        &self,
        full_command: &str,
        cwd: &Path,
        project_root: Option<&Path>,
    ) -> Option<String> {
        // 1. 输出重定向检查
        if let Some(redirect_check) = Self::check_output_redirects(full_command, cwd, project_root)
        {
            return Some(redirect_check);
        }

        // 2. 进程替换检测
        if full_command.contains("<(") || full_command.contains(">(") {
            return Some("Process substitution detected".to_owned());
        }

        // 3. 复合命令 cd + 写操作检测
        if let Some(cd_write_check) = Self::check_cd_plus_write(full_command, cwd, project_root) {
            return Some(cd_write_check);
        }

        None
    }

    // ──── 内部方法 ────

    /// 过滤掉 flag 参数——对照旧源 `filterOutFlags` L179-191。
    #[must_use]
    pub fn filter_out_flags(args: &[String]) -> Vec<String> {
        let mut result = Vec::new();
        let mut end_of_options = false;
        for arg in args {
            if arg == END_OF_OPTIONS {
                end_of_options = true;
                continue;
            }
            if !end_of_options && arg.starts_with('-') {
                continue;
            }
            result.push(arg.clone());
        }
        result
    }

    /// 路径规范化——对照旧源 `resolvePath` L194-204。
    ///
    /// 旧源 `Path.of(pathStr)` 遇非法路径抛 `InvalidPathException` → `null`；
    /// Unix 下唯一非法输入是含 NUL 字节，故本移植以 NUL 检测等价复现。
    #[must_use]
    pub fn resolve_path(path_str: &str, cwd: &Path) -> Option<PathBuf> {
        if path_str.contains('\0') {
            return None;
        }
        let path = Path::new(path_str);
        let joined = if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd.join(path)
        };
        Some(lexical_normalize(&joined))
    }

    /// 符号链接解析——对照旧源 `resolveSymlink` L207-225。
    #[must_use]
    pub fn resolve_symlink(path: &Path) -> PathBuf {
        if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
            if let Ok(target) = std::fs::read_link(path) {
                let target = if target.is_absolute() {
                    target
                } else {
                    lexical_normalize(&path.parent().unwrap_or(Path::new("")).join(&target))
                };
                tracing::debug!(from = ?path, to = ?target, "Symlink resolved");
                return target;
            }
            return path.to_path_buf();
        }
        // 尝试 canonicalize 解析中间路径中的符号链接
        if path.exists() {
            if let Ok(real) = std::fs::canonicalize(path) {
                return real;
            }
            return path.to_path_buf();
        }
        path.to_path_buf()
    }

    /// 项目边界检查——**已禁用**，不限制任何目录，由用户授权控制。
    ///
    /// 对照旧源 L227-230：方法体恒 `return null`。本移植原样保留。
    #[must_use]
    pub fn check_project_boundary(_target_path: &Path, _project_root: &Path) -> Option<String> {
        None
    }

    /// 危险删除路径检测——对照旧源 `checkDangerousRemovalPaths` L233-255。
    #[must_use]
    pub fn check_dangerous_removal_paths(
        _command: &str,
        args: &[String],
        resolved_path: &Path,
    ) -> Option<String> {
        let path_str = resolved_path.to_string_lossy().into_owned();
        let has_force = args.iter().any(|a| a.starts_with('-') && a.contains('f'));
        let has_recursive = args
            .iter()
            .any(|a| a.starts_with('-') && (a.contains('r') || a.contains('R')));

        // 直接危险路径
        if DANGEROUS_REMOVAL_PATHS.contains(&path_str.as_str()) {
            return Some(format!(
                "Refusing to remove system-critical path: {path_str}"
            ));
        }

        // 用户主目录
        if let Ok(home) = std::env::var("HOME")
            && path_str == home
            && (has_force || has_recursive)
        {
            return Some(format!("Refusing to remove home directory: {path_str}"));
        }

        // 根目录的直接子目录 + recursive + force
        if name_count(resolved_path) <= 1 && has_recursive && has_force {
            return Some(format!(
                "Refusing recursive forced removal of top-level directory: {path_str}"
            ));
        }

        None
    }

    /// 受保护隐藏目录检查——对照旧源 `checkProtectedHiddenDirs` L258-267。
    #[must_use]
    pub fn check_protected_hidden_dirs(path: &Path, op_type: FileOperationType) -> Option<String> {
        if op_type != FileOperationType::Write && op_type != FileOperationType::ReadWrite {
            return None;
        }
        for component in path.components() {
            if let Component::Normal(os) = component {
                let name = os.to_string_lossy();
                if PROTECTED_HIDDEN_DIRS.contains(&name.as_ref()) {
                    return Some(format!(
                        "Write operation targets protected directory: {name}"
                    ));
                }
            }
        }
        None
    }

    /// 输出重定向检查——对照旧源 `checkOutputRedirects` L270-284。
    ///
    /// 旧源正则 `(?<!\\)[>]{1,2}\s*(\S+)` 含逆序环视，Rust `regex` 不支持，
    /// 故手写等价扫描（贪婪吃 1-2 个 `>`、`\s*`、`(\S+)`，Java `\s` 取 ASCII 集）。
    #[must_use]
    pub fn check_output_redirects(
        command: &str,
        cwd: &Path,
        project_root: Option<&Path>,
    ) -> Option<String> {
        for target in find_redirect_targets(command) {
            if target.starts_with("/dev/") {
                continue; // /dev/null 等允许
            }
            let resolved_target = Self::resolve_path(&target, cwd);
            if let Some(resolved_target) = resolved_target
                && let Some(root) = project_root
                && let Some(boundary) = Self::check_project_boundary(&resolved_target, root)
            {
                return Some(format!("Output redirect {boundary}"));
            }
        }
        None
    }

    /// cd + 写操作检测——对照旧源 `checkCdPlusWrite` L287-312。
    #[must_use]
    pub fn check_cd_plus_write(
        command: &str,
        cwd: &Path,
        project_root: Option<&Path>,
    ) -> Option<String> {
        // 检测 cd /somewhere && rm/mv/cp 模式
        if command.contains("cd ") && command.contains("&&") {
            let cd_target = find_cd_target(command)?;
            let new_cwd = Self::resolve_path(&cd_target, cwd);
            if let Some(new_cwd) = new_cwd
                && let Some(root) = project_root
                && let Some(boundary) = Self::check_project_boundary(&new_cwd, root)
            {
                // 检查 && 后是否有写命令
                let idx = command.find("&&").unwrap_or(0);
                let after_cd = command[idx + 2..].trim();
                let first_cmd = after_cd.split_whitespace().next().unwrap_or("");
                let op = operation_type(first_cmd);
                if op == FileOperationType::Write || op == FileOperationType::ReadWrite {
                    let _ = boundary;
                    return Some(format!(
                        "cd to outside project + write operation: cd {cd_target} && {first_cmd}"
                    ));
                }
            }
        }
        None
    }

    /// sed 路径提取——`sed -i` 的文件参数——对照旧源 `extractSedPaths` L315-327。
    fn extract_sed_paths(args: &[String]) -> Vec<String> {
        let mut paths = Vec::new();
        let mut has_in_place = false;
        let mut skip_next = false;
        for (i, arg) in args.iter().enumerate() {
            if skip_next {
                skip_next = false;
                continue;
            }
            if arg == "-i" || arg.starts_with("-i") {
                has_in_place = true;
            }
            if arg == "-e" || arg == "-f" {
                skip_next = true;
                continue;
            }
            if !arg.starts_with('-') && i > 0 {
                paths.push(arg.clone());
            }
        }
        // 只有 -i 模式才算写操作路径
        if has_in_place { paths } else { Vec::new() }
    }

    /// git 路径提取——对照旧源 `extractGitPaths` L330-333。
    ///
    /// git 大部分操作都在当前目录，只提取显式路径参数（旧源恒返回空列表）。
    fn extract_git_paths(_args: &[String]) -> Vec<String> {
        Vec::new()
    }
}

/// 词法路径规范化——等价 Java `Path.normalize()`（不触碰文件系统）。
fn lexical_normalize(path: &Path) -> PathBuf {
    let mut out: Vec<Component<'_>> = Vec::new();
    let mut has_root = false;
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) => {
                has_root = true;
                out.push(component);
            }
            Component::CurDir => {}
            Component::ParentDir => {
                let can_pop = matches!(out.last(), Some(Component::Normal(_)));
                if can_pop {
                    out.pop();
                } else if !has_root {
                    out.push(component);
                }
            }
            Component::Normal(_) => out.push(component),
        }
    }
    out.iter().collect()
}

/// 名称段数量——等价 Java `Path.getNameCount()`（不含根）。
fn name_count(path: &Path) -> usize {
    path.components()
        .filter(|c| matches!(c, Component::Normal(_)))
        .count()
}

/// Java `\s` 字符集（ASCII `[ \t\n\x0B\f\r]`）。
fn is_java_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{000B}' | '\u{000C}' | '\r')
}

/// 复现旧源 L272-273 正则 `(?<!\\)[>]{1,2}\s*(\S+)` 的全部 group(1)。
fn find_redirect_targets(command: &str) -> Vec<String> {
    let units: Vec<char> = command.chars().collect();
    let n = units.len();
    let mut targets = Vec::new();
    let mut i = 0;
    while i < n {
        if units[i] != '>' || (i > 0 && units[i - 1] == '\\') {
            i += 1;
            continue;
        }
        // `[>]{1,2}` 贪婪：能吃 2 个就吃 2 个
        let mut matched = false;
        for len in [2_usize, 1] {
            if len == 2 && !(i + 1 < n && units[i + 1] == '>') {
                continue;
            }
            let mut j = i + len;
            while j < n && is_java_space(units[j]) {
                j += 1;
            }
            let mut k = j;
            while k < n && !is_java_space(units[k]) {
                k += 1;
            }
            if k > j {
                targets.push(units[j..k].iter().collect::<String>());
                i = k;
                matched = true;
                break;
            }
        }
        if !matched {
            i += 1;
        }
    }
    targets
}

/// 复现旧源 L290-293 正则 `cd\s+(\S+)` 的首个 group(1)。
fn find_cd_target(command: &str) -> Option<String> {
    let units: Vec<char> = command.chars().collect();
    let n = units.len();
    let mut i = 0;
    while i + 2 <= n {
        if units[i] == 'c' && i + 1 < n && units[i + 1] == 'd' {
            let mut j = i + 2;
            let space_start = j;
            while j < n && is_java_space(units[j]) {
                j += 1;
            }
            if j > space_start {
                let mut k = j;
                while k < n && !is_java_space(units[k]) {
                    k += 1;
                }
                if k > j {
                    return Some(units[j..k].iter().collect());
                }
            }
        }
        i += 1;
    }
    None
}
