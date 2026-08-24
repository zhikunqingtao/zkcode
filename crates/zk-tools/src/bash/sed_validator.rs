//! `sed` 命令安全验证器——对照旧 `tool/bash/SedValidator.java`（全 204 行，
//! 只读权威规格）。
//!
//! 两种模式（旧源 L14-17）：
//! 1. Pattern 1：`sed -n 'Np'` —— 只读打印；
//! 2. Pattern 2：`sed 's/old/new/flags'` —— 替换命令。
//!
//! **偏离登记**：旧源 `SedValidator` 为 `@Component`，但 main@581d407b 全仓
//! **零调用点**（死代码）；本移植仍逐字还原以保证实现不缩水，留痕
//! docs/compatibility.md §5，分类 EQUIVALENT。

use std::sync::LazyLock;

use regex::Regex;

/// 允许的只读 flags（10 条）——对照旧源 `READONLY_FLAGS` L25-27。
pub const READONLY_FLAGS: &[&str] = &[
    "-n",
    "--quiet",
    "--silent",
    "-E",
    "--regexp-extended",
    "-r",
    "-z",
    "--zero-terminated",
    "--posix",
];

/// `-n` 等静默 flags——对照旧源 L47 `Set.of("-n", "--quiet", "--silent")`。
const QUIET_FLAGS: &[&str] = &["-n", "--quiet", "--silent"];

/// in-place 编辑 flags——对照旧源 `INPLACE_FLAGS` L37。
pub const INPLACE_FLAGS: &[&str] = &["-i", "--in-place"];

/// 打印命令正则——对照旧源 `PRINT_COMMAND_PATTERN` L30-31。
static PRINT_COMMAND_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:\d+|\d+,\d+)?p$").expect("static regex"));

/// 替换命令 flags 白名单——对照旧源 `SAFE_SUB_FLAGS` L34。
static SAFE_SUB_FLAGS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[gpiImM1-9]*$").expect("static regex"));

/// 全 ASCII（0x01-0x7F）检查——对照旧源 L172 `cmd.matches("[\\x01-\\x7F]+")`。
static ASCII_ONLY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[\x01-\x7F]+$").expect("static regex"));

/// `w`/`W` 写文件命令（裸地址）——对照旧源 L178 `^[wW]\s*\S+.*`。
static WRITE_BARE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[wW]\s*\S+.*$").expect("static regex"));

/// `w`/`W` 写文件命令（行号地址）——对照旧源 L179 `^\d+\s*[wW]\s*\S+.*`。
static WRITE_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d+\s*[wW]\s*\S+.*$").expect("static regex"));

/// `w`/`W` 写文件命令（正则地址）——对照旧源 L180 `^/[^/]*/[IMim]*\s*[wW]\s*\S+.*`。
static WRITE_REGEX_ADDR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^/[^/]*/[IMim]*\s*[wW]\s*\S+.*$").expect("static regex"));

/// `e` 执行命令（裸）——对照旧源 L182 `^e.*`。
static EXEC_BARE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^e.*$").expect("static regex"));

/// `e` 执行命令（行号地址）——对照旧源 L183 `^\d+\s*e.*`。
static EXEC_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d+\s*e.*$").expect("static regex"));

/// `sed` 命令安全分类结果——对照旧源 `SedClassification` L196-203。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SedClassification {
    /// 只读输出到 stdout。
    ReadonlyStdout,
    /// 写入文件但需要权限确认。
    WriteWithPermission,
    /// 需要用户权限确认。
    NeedsPermission,
}

/// `sed` 命令安全验证器——对照旧源 `SedValidator` L22-204。
#[derive(Clone, Copy, Debug, Default)]
pub struct SedValidator;

impl SedValidator {
    /// 构造验证器（旧源为无状态 `@Component`）。
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Pattern 1：`sed -n 'Np'` —— 只读打印——对照旧源 `isReadOnlyPrint` L46-53。
    ///
    /// `args` 为 sed 的 flag 参数列表（不含 `sed` 本身），`expressions` 为表达式列表。
    #[must_use]
    pub fn is_read_only_print(&self, args: &[String], expressions: &[String]) -> bool {
        if !Self::has_flag(args, QUIET_FLAGS) {
            return false;
        }
        if !Self::validate_flags_against(args, READONLY_FLAGS) {
            return false;
        }
        expressions.iter().all(|expr| {
            java_split_semicolon(expr)
                .iter()
                .map(|cmd| cmd.trim())
                .all(|cmd| PRINT_COMMAND_PATTERN.is_match(cmd))
        })
    }

    /// Pattern 2：`sed 's/old/new/flags'` —— 替换命令分类——对照旧源
    /// `classifySubstitution` L63-92。
    #[must_use]
    pub fn classify_substitution(
        &self,
        args: &[String],
        expressions: &[String],
        allow_file_writes: bool,
    ) -> SedClassification {
        // 不允许写入时，禁止 -i flag（旧源 L67-69）
        if !allow_file_writes && Self::has_flag(args, INPLACE_FLAGS) {
            return SedClassification::NeedsPermission;
        }
        // ★ 组合 flag 验证（旧源 L71-74）
        let flag_tokens: Vec<&str> = args
            .iter()
            .filter(|a| a.starts_with('-'))
            .map(String::as_str)
            .collect();
        if !Self::validate_sed_flags(&flag_tokens, READONLY_FLAGS) {
            return SedClassification::NeedsPermission;
        }
        if expressions.len() != 1 {
            return SedClassification::NeedsPermission;
        }

        let expr = expressions[0].trim();
        // ★ 支持任意分隔符：s/ s| s# 等（旧源 L79）
        if expr.chars().count() < 2 || !expr.starts_with('s') {
            return SedClassification::NeedsPermission;
        }

        // ★ 危险操作检测（旧源 L82）
        if Self::contains_dangerous_operations(expr) {
            return SedClassification::NeedsPermission;
        }

        // 提取 flags 部分并验证（旧源 L85-90）
        if let Some(flags) = Self::extract_substitution_flags(expr)
            && SAFE_SUB_FLAGS.is_match(&flags)
        {
            return if allow_file_writes {
                SedClassification::WriteWithPermission
            } else {
                SedClassification::ReadonlyStdout
            };
        }
        SedClassification::NeedsPermission
    }

    /// 从 `s<delim>pattern<delim>replacement<delim>flags` 中提取 flags——对照旧源
    /// `extractSubstitutionFlags` L100-126。
    ///
    /// 支持任意非字母数字非反斜杠字符作为分隔符（对齐 POSIX sed 规范）。
    /// 返回 `None` 表示格式无效。
    #[must_use]
    pub fn extract_substitution_flags(expr: &str) -> Option<String> {
        let units: Vec<char> = expr.chars().collect();
        if units.len() < 2 || units[0] != 's' {
            return None;
        }

        let delim = units[1]; // s 后面的第一个字符就是分隔符

        // POSIX: 分隔符不能是反斜杠、换行符、字母、数字
        if delim == '\\' || delim == '\n' || delim.is_alphanumeric() {
            return None;
        }

        let mut delim_count = 0_usize;
        let mut last_delim_pos: Option<usize> = None;
        let mut i = 2; // 从位置 2 开始（跳过 s 和首个分隔符）
        while i < units.len() {
            if units[i] == '\\' && i + 1 < units.len() {
                i += 2; // 跳过转义字符
                continue;
            }
            if units[i] == delim {
                delim_count += 1;
                last_delim_pos = Some(i);
                if delim_count == 2 {
                    break; // 找到 pattern/replacement 的两个分隔符
                }
            }
            i += 1;
        }
        if delim_count < 1 {
            return None; // 无效格式
        }
        if delim_count == 2 {
            let pos = last_delim_pos.unwrap_or(0);
            return Some(units[pos + 1..].iter().collect());
        }
        Some(String::new()) // 只有 1 个分隔符 → 没有 flags
    }

    /// 检查参数列表是否包含指定 flag 之一——对照旧源 `hasFlag` L131-133。
    fn has_flag(args: &[String], flags: &[&str]) -> bool {
        args.iter().any(|a| flags.contains(&a.as_str()))
    }

    /// 验证所有 flag 参数是否在允许列表内——对照旧源 `validateFlagsAgainst` L138-142。
    fn validate_flags_against(args: &[String], allowed_flags: &[&str]) -> bool {
        args.iter()
            .filter(|a| a.starts_with('-'))
            .all(|a| allowed_flags.contains(&a.as_str()))
    }

    /// 拆分组合 flag：`-nE` → `-n` / `-E` 逐个验证——对照旧源 `validateSedFlags`
    /// L147-161。
    fn validate_sed_flags(tokens: &[&str], allowed_flags: &[&str]) -> bool {
        for token in tokens {
            if !token.starts_with('-') || token.starts_with("--") {
                // 长 flag 直接检查
                if token.starts_with("--") && !allowed_flags.contains(token) {
                    return false;
                }
                continue;
            }
            // 短 flag 组合: "-nE" → 检查 "-n" 和 "-E"
            for c in token.chars().skip(1) {
                let single_flag = format!("-{c}");
                if !allowed_flags.contains(&single_flag.as_str()) {
                    return false;
                }
            }
        }
        true
    }

    /// 检测危险 sed 操作——对照旧源 `containsDangerousOperations` L167-191。
    ///
    /// 即使通过了白名单检查，也要拒绝包含危险操作的表达式。
    fn contains_dangerous_operations(expr: &str) -> bool {
        let cmd = expr.trim();
        if cmd.is_empty() {
            return false;
        }

        // 1. 拒绝非 ASCII 字符（Unicode 同形字攻击）
        if !ASCII_ONLY_RE.is_match(cmd) {
            return true;
        }
        // 2. 拒绝花括号块（太复杂无法安全解析）
        if cmd.contains('{') || cmd.contains('}') {
            return true;
        }
        // 3. 拒绝换行符（多行命令）
        if cmd.contains('\n') {
            return true;
        }
        // 4. 拒绝 w/W 写文件命令（各种地址格式）
        if WRITE_BARE_RE.is_match(cmd) || WRITE_LINE_RE.is_match(cmd) {
            return true;
        }
        if WRITE_REGEX_ADDR_RE.is_match(cmd) {
            return true;
        }
        // 5. 拒绝 e/E 执行命令
        if EXEC_BARE_RE.is_match(cmd) || EXEC_LINE_RE.is_match(cmd) {
            return true;
        }
        // 6. 拒绝替换命令中的 w/W/e/E flags
        //
        // 旧源 L185 用 `s([^\\\n]).*?\1.*?\1(.*?)$` —— Rust `regex` 不支持反向
        // 引用 `\1`，故手写等价扫描：最左起点 `s` + 分隔符（非 `\` 非 `\n`），随后
        // 取最近两个同分隔符，group(2) 即末段。走到本步时 cmd 已确保为单行纯
        // ASCII（步骤 1/3 已 fail-closed），故 `.` 不跨行的约束自动满足。
        if let Some(flags) = find_substitution_trailing_flags(cmd)
            && flags.contains(['w', 'W', 'e', 'E'])
        {
            return true;
        }
        false
    }
}

/// 复现旧源 L185-189 反向引用正则的匹配结果：返回 group(2)（末段 flags）。
fn find_substitution_trailing_flags(cmd: &str) -> Option<String> {
    let units: Vec<char> = cmd.chars().collect();
    let n = units.len();
    for i in 0..n {
        if units[i] != 's' || i + 1 >= n {
            continue;
        }
        let delim = units[i + 1];
        if delim == '\\' || delim == '\n' {
            continue;
        }
        // `.*?\1`：最近一个 delim（`.` 不跨行，走到此处已无换行）
        let Some(j) = (i + 2..n).find(|&k| units[k] == delim) else {
            continue;
        };
        let Some(k) = (j + 1..n).find(|&k| units[k] == delim) else {
            continue;
        };
        // `(.*?)$`：懒惰匹配 + `$` 锚定末尾 → 取 k 之后全部
        return Some(units[k + 1..].iter().collect());
    }
    None
}

/// 等价 Java `expr.split(";")`：丢弃尾部空串（limit == 0 语义）。
fn java_split_semicolon(expr: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = expr.split(';').collect();
    while parts.len() > 1 && parts.last().is_some_and(|s| s.is_empty()) {
        parts.pop();
    }
    parts
}
