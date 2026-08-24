//! P0 降级 Fallback：`BashCommandClassifier`——纯正则实现。
//!
//! 逐字对照旧源 `tool/bash/BashCommandClassifier.java`（1239 行，main@581d407b）。
//!
//! 仅在 [`super::security::BashSecurityAnalyzer::parse_for_security`] 返回
//! `ParseUnavailable` 时使用。
//!
//! 安全设计：fail-closed——无法分类时返回 `Unknown`（需要权限确认）。
//!
//! 三层验证架构：
//! 1. 层 1：`READONLY_COMMANDS`——纯只读命令，无需参数检查；
//! 2. 层 2：`READONLY_REGEXES`——正则匹配只读；
//! 3. 层 3：`COMMAND_ALLOWLIST`——flag 级别白名单验证（含 `validate_flags` 6 项安全加固）。
//!
//! 决策留痕见 `docs/compatibility.md` §5。

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;

use super::category::CommandCategory;
use super::javastr::{
    J_D, J_DOT, J_NS, J_S, J_W, java_is_blank, java_split, java_split_ws, java_substring, java_trim,
};

// ══════════════════════════════════════════════════════════════
// 层 1: 纯只读命令 (~60个，无需参数检查)
// ★ 安全修正: env/printenv 已移除(可泄露敏感环境变量); tput 移至 COMMAND_ALLOWLIST
// 对照旧源 L32-52
// ══════════════════════════════════════════════════════════════
static READONLY_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        // 系统信息
        "cal", "uptime", "id", "uname", "free", "df", "du", "locale", "groups", "nproc", "getconf",
        // 文件查看 (只读)
        "cat", "head", "tail", "wc", "stat", "strings", "hexdump", "od", "nl", "readlink",
        // 文本处理 (只读)
        "cut", "paste", "tr", "column", "tac", "rev", "fold", "expand", "unexpand", "fmt", "comm",
        "cmp", "numfmt", // 路径操作 (只读)
        "basename", "dirname", "realpath", // 其他只读
        "diff", "true", "false", "sleep", "which", "type", "expr", "test", "seq", "tsort", "pr",
        "getent", "ulimit", "umask", "stty", "tset", "infocmp", "toe", "ldd", "nm", "objdump",
        "readelf", "size", "openssl", "xxd", "md5sum", "sha1sum", "cksum", "look", "spell",
        "factor", "bc",
    ]
    .into_iter()
    .collect()
});

// ══════════════════════════════════════════════════════════════
// 层 2: 正则匹配只读命令 (有参数的只读命令) — 对照旧源 L57-67
// ══════════════════════════════════════════════════════════════
static READONLY_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        format!("^echo{J_S}"),
        format!("^uniq({J_S}|$)"),
        format!("^pwd({J_S}|$)"),
        format!("^whoami({J_S}|$)"),
        format!("^node{J_S}+(-v|--version)"),
        format!("^python3?{J_S}+--version"),
        format!("^java{J_S}+(-version|--version)"),
        format!("^mvn{J_S}+--version"),
        format!("^gradle{J_S}+--version"),
    ]
    .iter()
    .map(|p| Regex::new(p).expect("static readonly regex"))
    .collect()
});

// ══════════════════════════════════════════════════════════════
// 层 3: flag 级别验证 (带安全 flag 白名单)
// ══════════════════════════════════════════════════════════════

/// Flag 参数类型——对照旧源 `FlagArgType` L72。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlagArgType {
    /// 无参数。
    None,
    /// 带任意值参数。
    Value,
    /// 带数字值参数。
    Number,
}

/// 表内简写：无参数 flag。
const NO: FlagArgType = FlagArgType::None;
/// 表内简写：带任意值 flag。
const VA: FlagArgType = FlagArgType::Value;
/// 表内简写：带数字值 flag。
const NU: FlagArgType = FlagArgType::Number;

/// 额外危险检查回调类型——对照旧源 `BiPredicate<String, String[]>`。
pub type AdditionalDangerousCheck = fn(&str, &[&str]) -> bool;

/// Flag 配置——对照旧源 `record FlagConfig` L80-93。
#[derive(Clone)]
pub struct FlagConfig {
    safe_flags: HashMap<&'static str, FlagArgType>,
    respects_double_dash: bool,
    additional_dangerous_check: Option<AdditionalDangerousCheck>,
}

impl FlagConfig {
    /// 向后兼容构造器：仅 `safeFlags`（`respectsDoubleDash=true`，无回调）——旧源 L86-88。
    #[must_use]
    pub fn new(safe_flags: &[(&'static str, FlagArgType)]) -> Self {
        Self::with_options(safe_flags, true, None)
    }

    /// 向后兼容构造器：`safeFlags` + `respectsDoubleDash`（无回调）——旧源 L90-92。
    ///
    /// 旧源中该重载无调用点，为保持 1:1 还原一并移植。
    #[must_use]
    pub fn with_double_dash(
        safe_flags: &[(&'static str, FlagArgType)],
        respects_double_dash: bool,
    ) -> Self {
        Self::with_options(safe_flags, respects_double_dash, None)
    }

    /// 全参数构造器——旧源 L80-84 主构造器。
    #[must_use]
    pub fn with_options(
        safe_flags: &[(&'static str, FlagArgType)],
        respects_double_dash: bool,
        additional_dangerous_check: Option<AdditionalDangerousCheck>,
    ) -> Self {
        Self {
            safe_flags: safe_flags.iter().copied().collect(),
            respects_double_dash,
            additional_dangerous_check,
        }
    }

    /// 安全 flag 白名单。
    #[must_use]
    pub fn safe_flags(&self) -> &HashMap<&'static str, FlagArgType> {
        &self.safe_flags
    }

    /// 是否遵守 POSIX `--`。
    #[must_use]
    pub const fn respects_double_dash(&self) -> bool {
        self.respects_double_dash
    }

    /// 额外危险检查回调。
    #[must_use]
    pub const fn additional_dangerous_check(&self) -> Option<AdditionalDangerousCheck> {
        self.additional_dangerous_check
    }
}

/// flag 正则：以 `-` 开头，后跟字母/数字/`-`——对照旧源 L96。
static FLAG_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^-[a-zA-Z0-9-]").expect("static flag pattern"));

/// xargs 安全目标命令白名单——对照旧源 L100-102。
///
/// SECURITY：仅允许以下纯只读命令作为 xargs 目标。
static SAFE_TARGET_COMMANDS_FOR_XARGS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    ["echo", "printf", "wc", "grep", "head", "tail"]
        .into_iter()
        .collect()
});

/// find 危险参数黑名单正则——对照旧源 L105-107。
static FIND_DANGEROUS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"-(?:delete|exec|execdir|ok|okdir|fprint0?|fls|fprintf)\b")
        .expect("static find dangerous pattern")
});

/// fd/fdfind 共享安全 flag 白名单——对照旧源 L110-152。
const FD_SAFE_FLAGS: &[(&str, FlagArgType)] = &[
    ("-h", NO),
    ("--help", NO),
    ("-V", NO),
    ("--version", NO),
    ("-H", NO),
    ("--hidden", NO),
    ("-I", NO),
    ("--no-ignore", NO),
    ("--no-ignore-vcs", NO),
    ("--no-ignore-parent", NO),
    ("-s", NO),
    ("--case-sensitive", NO),
    ("-i", NO),
    ("--ignore-case", NO),
    ("-g", NO),
    ("--glob", NO),
    ("--regex", NO),
    ("-F", NO),
    ("--fixed-strings", NO),
    ("-a", NO),
    ("--absolute-path", NO),
    ("-L", NO),
    ("--follow", NO),
    ("-p", NO),
    ("--full-path", NO),
    ("-0", NO),
    ("--print0", NO),
    ("-d", NU),
    ("--max-depth", NU),
    ("--min-depth", NU),
    ("--exact-depth", NU),
    ("-t", VA),
    ("--type", VA),
    ("-e", VA),
    ("--extension", VA),
    ("-S", VA),
    ("--size", VA),
    ("--changed-within", VA),
    ("--changed-before", VA),
    ("-o", VA),
    ("--owner", VA),
    ("-E", VA),
    ("--exclude", VA),
    ("--ignore-file", VA),
    ("-c", VA),
    ("--color", VA),
    ("-j", NU),
    ("--threads", NU),
    ("--max-buffer-time", VA),
    ("--max-results", NU),
    ("-1", NO),
    ("-q", NO),
    ("--quiet", NO),
    ("--show-errors", NO),
    ("--strip-cwd-prefix", NO),
    ("--one-file-system", NO),
    ("--prune", NO),
    ("--search-path", VA),
    ("--base-directory", VA),
    ("--path-separator", VA),
    ("--batch-size", NU),
    ("--no-require-git", NO),
    ("--hyperlink", VA),
    ("--and", VA),
    ("--format", VA),
];

/// tput 危险 capability 回调——对照旧源 L250-255。
fn tput_dangerous(_cmd: &str, args: &[&str]) -> bool {
    for arg in args {
        if matches!(*arg, "init" | "reset" | "rmacs" | "smacs") {
            return true;
        }
    }
    false
}

/// 层 3 命令白名单——对照旧源 L154-256。
///
/// 旧源为 `Map.ofEntries`（无序 `HashMap`）；此处改用有序切片，遍历顺序确定，
/// 且键之间互不构成前缀关系，判定结果与旧源一致。
static COMMAND_ALLOWLIST: LazyLock<Vec<(&'static str, FlagConfig)>> = LazyLock::new(|| {
    vec![
        // xargs: +xargs目标命令检测在validateFlags中
        (
            "xargs",
            FlagConfig::new(&[
                ("-I", VA),
                ("-n", NU),
                ("-P", NU),
                ("-d", VA),
                ("-0", NO),
                ("--null", NO),
                ("-t", NO),
                ("--verbose", NO),
                ("-r", NO),
                ("--no-run-if-empty", NO),
                ("-E", VA),
                ("-L", NU),
                ("-s", NU),
                ("--max-chars", NU),
            ]),
        ),
        (
            "sort",
            FlagConfig::new(&[
                ("-r", NO),
                ("--reverse", NO),
                ("-n", NO),
                ("-u", NO),
                ("-k", VA),
                ("-t", VA),
                ("-f", NO),
            ]),
        ),
        (
            "man",
            FlagConfig::new(&[("-a", NO), ("-f", NO), ("-k", NO)]),
        ),
        (
            "ps",
            FlagConfig::new(&[("-e", NO), ("-A", NO), ("-f", NO), ("-u", VA)]),
        ),
        (
            "netstat",
            FlagConfig::new(&[("-a", NO), ("-n", NO), ("-t", NO), ("-l", NO)]),
        ),
        (
            "file",
            FlagConfig::new(&[
                ("-b", NO),
                ("--brief", NO),
                ("-i", NO),
                ("--mime", NO),
                ("-L", NO),
                ("--dereference", NO),
            ]),
        ),
        (
            "sed",
            FlagConfig::new(&[
                ("-n", NO),
                ("-e", VA),
                ("-E", NO),
                ("--regexp-extended", NO),
            ]),
        ),
        (
            "grep",
            FlagConfig::new(&[
                ("-r", NO),
                ("-R", NO),
                ("-l", NO),
                ("-L", NO),
                ("-c", NO),
                ("-n", NO),
                ("-i", NO),
                ("-v", NO),
                ("-w", NO),
                ("-x", NO),
                ("-E", NO),
                ("-P", NO),
                ("-F", NO),
                ("-o", NO),
                ("-h", NO),
                ("-H", NO),
                ("--include", VA),
                ("--exclude", VA),
                ("--exclude-dir", VA),
                ("-A", NU),
                ("-B", NU),
                ("-C", NU),
                ("-m", NU),
                ("--color", VA),
            ]),
        ),
        (
            "rg",
            FlagConfig::new(&[
                ("-i", NO),
                ("--ignore-case", NO),
                ("-S", NO),
                ("--smart-case", NO),
                ("-l", NO),
                ("--files-with-matches", NO),
                ("-c", NO),
                ("--count", NO),
                ("-n", NO),
                ("--line-number", NO),
                ("-w", NO),
                ("-v", NO),
                ("--invert-match", NO),
                ("-o", NO),
                ("--only-matching", NO),
                ("-t", VA),
                ("-T", VA),
                ("-g", VA),
                ("--glob", VA),
                ("-A", NU),
                ("-B", NU),
                ("-C", NU),
                ("-m", NU),
                ("--max-count", NU),
                ("--hidden", NO),
                ("--no-ignore", NO),
                ("-F", NO),
                ("--fixed-strings", NO),
                ("--heading", NO),
                ("--no-heading", NO),
                ("--column", NO),
                ("--type-list", NO),
                ("-u", NO),
                ("-a", NO),
                ("--text", NO),
                ("-z", NO),
                ("--json", NO),
                ("--stats", NO),
                ("--debug", NO),
                ("--color", VA),
                ("--colors", VA),
            ]),
        ),
        (
            "tree",
            FlagConfig::new(&[
                ("-L", NU),
                ("-d", NO),
                ("-a", NO),
                ("-I", VA),
                ("--gitignore", NO),
                ("-f", NO),
            ]),
        ),
        (
            "date",
            FlagConfig::new(&[("-u", NO), ("--utc", NO), ("-d", VA), ("--date", VA)]),
        ),
        ("hostname", FlagConfig::new(&[("-f", NO), ("-i", NO)])),
        (
            "lsof",
            FlagConfig::new(&[("-i", VA), ("-p", VA), ("-n", NO), ("-P", NO)]),
        ),
        (
            "pgrep",
            FlagConfig::new(&[("-l", NO), ("-a", NO), ("-f", NO), ("-u", VA)]),
        ),
        (
            "ss",
            FlagConfig::new(&[
                ("-t", NO),
                ("-u", NO),
                ("-l", NO),
                ("-n", NO),
                ("-a", NO),
                ("-p", NO),
            ]),
        ),
        (
            "base64",
            FlagConfig::new(&[("-d", NO), ("--decode", NO), ("-w", NU)]),
        ),
        ("sha256sum", FlagConfig::new(&[("-c", NO), ("--check", NO)])),
        // ★ fd/fdfind 白名单
        // SECURITY: -x/--exec, -X/--exec-batch, -l/--list-details 排除
        ("fd", FlagConfig::new(FD_SAFE_FLAGS)),
        ("fdfind", FlagConfig::new(FD_SAFE_FLAGS)),
        // ★ tput — 从READONLY_COMMANDS移至此处，带危险capability回调
        (
            "tput",
            FlagConfig::with_options(&[], true, Some(tput_dangerous)),
        ),
    ]
});

// ══════════════════════════════════════════════════════════════
// 外部只读命令前缀 (docker/kubectl/npm/yarn/pip) — 对照旧源 L261-267
// ══════════════════════════════════════════════════════════════
const EXTERNAL_READONLY_PREFIXES: &[&str] = &[
    "docker ps",
    "docker images",
    "kubectl get",
    "kubectl describe",
    "kubectl logs",
    "npm list",
    "npm info",
    "npm outdated",
    "npm audit",
    "yarn list",
    "yarn info",
    "yarn outdated",
    "pip list",
    "pip show",
    "pip freeze",
];

/// `git tag` 危险回调——对照旧源 L297-326。
///
/// SECURITY：`git tag v1.0` 创建标签 → not `readOnly`。
// 旧源 L310-316 / L358-364 为两段语义不同的独立分支（显式 `--list`/`-l` 与
// 组合短 flag `-li`/`-il`），赋值结果相同但判定含义不同；原样保留以对齐可审计性。
#[allow(clippy::if_same_then_else)]
fn git_tag_dangerous(_raw_cmd: &str, args: &[&str]) -> bool {
    const FLAGS_WITH_ARGS: &[&str] = &[
        "--contains",
        "--no-contains",
        "--merged",
        "--no-merged",
        "--points-at",
        "--sort",
        "--format",
        "-n",
    ];
    let mut idx = 0usize;
    let mut seen_list_flag = false;
    let mut seen_dash_dash = false;
    while idx < args.len() {
        let t = args[idx];
        if t.is_empty() {
            idx += 1;
            continue;
        }
        if t == "--" && !seen_dash_dash {
            seen_dash_dash = true;
            idx += 1;
            continue;
        }
        if !seen_dash_dash && t.starts_with('-') {
            if t == "--list" || t == "-l" {
                seen_list_flag = true;
            } else if !t.starts_with("--")
                && t.chars().count() > 2
                && !t.contains('=')
                && t.chars().skip(1).any(|c| c == 'l')
            {
                seen_list_flag = true; // bundle: -li, -il
            }
            if t.contains('=') {
                idx += 1;
            } else if FLAGS_WITH_ARGS.contains(&t) {
                idx += 2;
            } else {
                idx += 1;
            }
        } else {
            if !seen_list_flag {
                return true; // positional arg + no --list = create tag
            }
            idx += 1;
        }
    }
    false
}

/// `git branch` 危险回调——对照旧源 L345-377。
#[allow(clippy::if_same_then_else)]
fn git_branch_dangerous(_raw_cmd: &str, args: &[&str]) -> bool {
    const FLAGS_WITH_ARGS: &[&str] = &["--contains", "--no-contains", "--points-at", "--sort"];
    const FLAGS_WITH_OPTIONAL_ARGS: &[&str] = &["--merged", "--no-merged"];
    let mut idx = 0usize;
    let mut last_flag = "";
    let mut seen_list_flag = false;
    let mut seen_dash_dash = false;
    while idx < args.len() {
        let t = args[idx];
        if t.is_empty() {
            idx += 1;
            continue;
        }
        if t == "--" && !seen_dash_dash {
            seen_dash_dash = true;
            last_flag = "";
            idx += 1;
            continue;
        }
        if !seen_dash_dash && t.starts_with('-') {
            if t == "--list" || t == "-l" {
                seen_list_flag = true;
            } else if !t.starts_with("--")
                && t.chars().count() > 2
                && !t.contains('=')
                && t.chars().skip(1).any(|c| c == 'l')
            {
                seen_list_flag = true;
            }
            if t.contains('=') {
                last_flag = t.split('=').next().unwrap_or("");
                idx += 1;
            } else if FLAGS_WITH_ARGS.contains(&t) {
                last_flag = t;
                idx += 2;
            } else {
                last_flag = t;
                idx += 1;
            }
        } else {
            let last_flag_has_optional_arg = FLAGS_WITH_OPTIONAL_ARGS.contains(&last_flag);
            if !seen_list_flag && !last_flag_has_optional_arg {
                return true; // positional arg + no list/optional-arg = create branch
            }
            idx += 1;
        }
    }
    false
}

// ══════════════════════════════════════════════════════════════
// Git 只读命令 (带 flag 验证 + additionalDangerousCheck) — 对照旧源 L272-387
// ══════════════════════════════════════════════════════════════
static GIT_READONLY_COMMANDS: LazyLock<Vec<(&'static str, FlagConfig)>> = LazyLock::new(|| {
    vec![
        (
            "git diff",
            FlagConfig::new(&[
                ("--cached", NO),
                ("--staged", NO),
                ("--stat", NO),
                ("--name-only", NO),
                ("--name-status", NO),
                ("--no-color", NO),
            ]),
        ),
        (
            "git log",
            FlagConfig::new(&[
                ("--oneline", NO),
                ("-n", NU),
                ("--graph", NO),
                ("--stat", NO),
                ("--format", VA),
                ("--author", VA),
            ]),
        ),
        (
            "git show",
            FlagConfig::new(&[("--stat", NO), ("--format", VA)]),
        ),
        (
            "git status",
            FlagConfig::new(&[("-s", NO), ("--short", NO), ("--porcelain", NO)]),
        ),
        // ★ git tag
        (
            "git tag",
            FlagConfig::with_options(
                &[
                    ("-l", NO),
                    ("--list", NO),
                    ("--sort", VA),
                    ("-n", NU),
                    ("--contains", VA),
                    ("--no-contains", VA),
                    ("--points-at", VA),
                    ("--merged", VA),
                    ("--no-merged", VA),
                    ("--format", VA),
                    ("--column", NO),
                    ("--no-column", NO),
                    ("-i", NO),
                    ("--ignore-case", NO),
                ],
                true,
                Some(git_tag_dangerous),
            ),
        ),
        // ★ git branch
        (
            "git branch",
            FlagConfig::with_options(
                &[
                    ("-a", NO),
                    ("--all", NO),
                    ("-v", NO),
                    ("--verbose", NO),
                    ("-vv", NO),
                    ("-r", NO),
                    ("--remotes", NO),
                    ("-l", NO),
                    ("--list", NO),
                    ("--color", NO),
                    ("--no-color", NO),
                    ("--column", NO),
                    ("--no-column", NO),
                    ("--abbrev", NU),
                    ("--no-abbrev", NO),
                    ("--contains", VA),
                    ("--no-contains", VA),
                    ("--merged", NO),
                    ("--no-merged", NO),
                    ("--points-at", VA),
                    ("--sort", VA),
                    ("--show-current", NO),
                    ("-i", NO),
                    ("--ignore-case", NO),
                ],
                true,
                Some(git_branch_dangerous),
            ),
        ),
        ("git remote", FlagConfig::new(&[("-v", NO)])),
        ("git stash", FlagConfig::new(&[("list", NO)])),
        ("git ls-files", FlagConfig::new(&[])),
        ("git ls-tree", FlagConfig::new(&[("-r", NO)])),
        ("git rev-parse", FlagConfig::new(&[])),
        (
            "git config",
            FlagConfig::new(&[("--get", VA), ("-l", NO), ("--list", NO)]),
        ),
        ("git blame", FlagConfig::new(&[("-L", VA)])),
    ]
});

// ══════════════════════════════════════════════════════════════
// GH CLI 只读命令白名单 — 对照旧源 L393-504
// SECURITY: 每个子命令都排除了 --web/-w 和 --show-token/-t
// ══════════════════════════════════════════════════════════════
static GH_READONLY_COMMANDS: LazyLock<Vec<(&'static str, FlagConfig)>> = LazyLock::new(|| {
    vec![
        (
            "gh pr view",
            FlagConfig::new(&[
                ("--json", VA),
                ("--comments", NO),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh pr list",
            FlagConfig::new(&[
                ("--state", VA),
                ("-s", VA),
                ("--author", VA),
                ("--assignee", VA),
                ("--label", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--base", VA),
                ("--head", VA),
                ("--search", VA),
                ("--json", VA),
                ("--draft", NO),
                ("--app", VA),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh pr diff",
            FlagConfig::new(&[
                ("--color", VA),
                ("--name-only", NO),
                ("--patch", NO),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh pr checks",
            FlagConfig::new(&[
                ("--watch", NO),
                ("--required", NO),
                ("--fail-fast", NO),
                ("--json", VA),
                ("--interval", NU),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh pr status",
            FlagConfig::new(&[
                ("--conflict-status", NO),
                ("-c", NO),
                ("--json", VA),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh issue view",
            FlagConfig::new(&[
                ("--json", VA),
                ("--comments", NO),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh issue list",
            FlagConfig::new(&[
                ("--state", VA),
                ("-s", VA),
                ("--assignee", VA),
                ("--author", VA),
                ("--label", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--milestone", VA),
                ("--search", VA),
                ("--json", VA),
                ("--app", VA),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh issue status",
            FlagConfig::new(&[("--json", VA), ("--repo", VA), ("-R", VA)]),
        ),
        ("gh repo view", FlagConfig::new(&[("--json", VA)])),
        (
            "gh run list",
            FlagConfig::new(&[
                ("--branch", VA),
                ("-b", VA),
                ("--status", VA),
                ("-s", VA),
                ("--workflow", VA),
                ("-w", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--json", VA),
                ("--repo", VA),
                ("-R", VA),
                ("--event", VA),
                ("-e", VA),
                ("--user", VA),
                ("-u", VA),
                ("--created", VA),
                ("--commit", VA),
                ("-c", VA),
            ]),
        ),
        (
            "gh run view",
            FlagConfig::new(&[
                ("--log", NO),
                ("--log-failed", NO),
                ("--exit-status", NO),
                ("--verbose", NO),
                ("-v", NO),
                ("--json", VA),
                ("--repo", VA),
                ("-R", VA),
                ("--job", VA),
                ("-j", VA),
                ("--attempt", NU),
                ("-a", NU),
            ]),
        ),
        (
            "gh auth status",
            FlagConfig::new(&[
                ("--active", NO),
                ("-a", NO),
                ("--hostname", VA),
                ("-h", VA),
                ("--json", VA),
            ]),
        ),
        (
            "gh release list",
            FlagConfig::new(&[
                ("--exclude-drafts", NO),
                ("--exclude-pre-releases", NO),
                ("--json", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--order", VA),
                ("-O", VA),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh release view",
            FlagConfig::new(&[("--json", VA), ("--repo", VA), ("-R", VA)]),
        ),
        (
            "gh workflow list",
            FlagConfig::new(&[
                ("--all", NO),
                ("-a", NO),
                ("--json", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh workflow view",
            FlagConfig::new(&[
                ("--ref", VA),
                ("-r", VA),
                ("--yaml", NO),
                ("-y", NO),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh label list",
            FlagConfig::new(&[
                ("--json", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--order", VA),
                ("--search", VA),
                ("-S", VA),
                ("--sort", VA),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh search repos",
            FlagConfig::new(&[
                ("--archived", NO),
                ("--created", VA),
                ("--json", VA),
                ("--language", VA),
                ("--license", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--owner", VA),
                ("--sort", VA),
                ("--visibility", VA),
            ]),
        ),
        (
            "gh search issues",
            FlagConfig::new(&[
                ("--assignee", VA),
                ("--author", VA),
                ("--json", VA),
                ("--label", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--state", VA),
                ("--repo", VA),
                ("-R", VA),
            ]),
        ),
        (
            "gh search prs",
            FlagConfig::new(&[
                ("--assignee", VA),
                ("--author", VA),
                ("--json", VA),
                ("--label", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--state", VA),
                ("--repo", VA),
                ("-R", VA),
                ("--draft", NO),
            ]),
        ),
        (
            "gh search commits",
            FlagConfig::new(&[
                ("--author", VA),
                ("--json", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--owner", VA),
                ("--repo", VA),
                ("-R", VA),
                ("--sort", VA),
            ]),
        ),
        (
            "gh search code",
            FlagConfig::new(&[
                ("--extension", VA),
                ("--filename", VA),
                ("--json", VA),
                ("--language", VA),
                ("--limit", NU),
                ("-L", NU),
                ("--owner", VA),
                ("--repo", VA),
                ("-R", VA),
                ("--size", VA),
            ]),
        ),
    ]
});

// ══════════════════════════════════════════════════════════════
// Docker 只读命令 — 对照旧源 L509-519
// ══════════════════════════════════════════════════════════════
static DOCKER_READONLY_COMMANDS: LazyLock<Vec<(&'static str, FlagConfig)>> = LazyLock::new(|| {
    vec![
        (
            "docker logs",
            FlagConfig::new(&[
                ("--follow", NO),
                ("-f", NO),
                ("--tail", VA),
                ("-n", VA),
                ("--timestamps", NO),
                ("-t", NO),
                ("--since", VA),
                ("--until", VA),
                ("--details", NO),
            ]),
        ),
        (
            "docker inspect",
            FlagConfig::new(&[
                ("--format", VA),
                ("-f", VA),
                ("--type", VA),
                ("--size", NO),
                ("-s", NO),
            ]),
        ),
    ]
});

/// pyright 危险回调——对照旧源 L539-545。
fn pyright_dangerous(_cmd: &str, args: &[&str]) -> bool {
    for a in args {
        if *a == "--watch" || *a == "-w" {
            return true;
        }
        if a.starts_with("--createstub") {
            return true;
        }
    }
    false
}

// ══════════════════════════════════════════════════════════════
// Pyright 只读命令 — 对照旧源 L525-547
// SECURITY: --watch/-w 和 --createstub 通过回调检测; respectsDoubleDash=false
// ══════════════════════════════════════════════════════════════
static PYRIGHT_READONLY_COMMANDS: LazyLock<Vec<(&'static str, FlagConfig)>> = LazyLock::new(|| {
    vec![(
        "pyright",
        FlagConfig::with_options(
            &[
                ("--outputjson", NO),
                ("--project", VA),
                ("-p", VA),
                ("--pythonversion", VA),
                ("--pythonplatform", VA),
                ("--typeshedpath", VA),
                ("--venvpath", VA),
                ("--level", VA),
                ("--stats", NO),
                ("--verbose", NO),
                ("--version", NO),
                ("--dependencies", NO),
                ("--warnings", NO),
            ],
            false, // pyright 不遵守 POSIX --
            Some(pyright_dangerous),
        ),
    )]
});

// ══════════════════════════════════════════════════════════════
// GH 危险回调 — 对照旧源 L553-570
// 防止通过 --repo=HOST/OWNER/REPO 或 URL 参数进行 DNS exfiltration
// ══════════════════════════════════════════════════════════════
fn gh_is_dangerous_callback(_raw_command: &str, args: &[&str]) -> bool {
    for token in args {
        if token.is_empty() {
            continue;
        }
        let mut value: &str = token;
        if token.starts_with('-') {
            let Some(eq) = token.find('=') else {
                continue;
            };
            value = &token[eq + 1..];
            if value.is_empty() {
                continue;
            }
        }
        if !value.contains('/') && !value.contains("://") && !value.contains('@') {
            continue;
        }
        if value.contains("://") {
            return true;
        }
        if value.contains('@') {
            return true;
        }
        let slash_count = value.chars().filter(|c| *c == '/').count();
        if slash_count >= 2 {
            return true;
        }
    }
    false
}

// ══════════════════════════════════════════════════════════════
// 命令风险分级体系 (SAFE / MODERATE / DANGEROUS / BLOCKED) — 对照旧源 L572-694
// ══════════════════════════════════════════════════════════════

/// 命令风险级别——对照旧源 `RiskLevel` L585-590。
///
/// 变体声明顺序即 Java `ordinal()` 顺序，`PartialOrd` 派生自该顺序，
/// 用于旧源 L687 的 `segLevel.ordinal() > maxLevel.ordinal()` 比较。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RiskLevel {
    /// 只读命令，无副作用。
    Safe,
    /// 可能有副作用但风险可控。
    Moderate,
    /// 破坏性操作。
    Dangerous,
    /// 绝对禁止。
    Blocked,
}

/// 命令风险评估结果——对照旧源 `record RiskAssessment` L595-602。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RiskAssessment {
    /// 风险级别。
    pub level: RiskLevel,
    /// 判定理由。
    pub reason: String,
    /// 原始命令（旧源原样回传入参，可能为 `null`；此处用 `Option`）。
    pub command: Option<String>,
}

impl RiskAssessment {
    /// 是否被绝对禁止——对照旧源 L600。
    #[must_use]
    pub fn is_blocked(&self) -> bool {
        self.level == RiskLevel::Blocked
    }

    /// 是否安全——对照旧源 L601。
    #[must_use]
    pub fn is_safe(&self) -> bool {
        self.level == RiskLevel::Safe
    }
}

/// 绝对禁止命令 — 100% 拒绝执行——对照旧源 L605-606。
static BLOCKED_COMMAND_SET: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| ["sudo", "su", "doas"].into_iter().collect());

/// 破坏性命令 — 需严格权限确认——对照旧源 L609-614。
static DANGEROUS_COMMAND_SET: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "rm",
        "rmdir",
        "chmod",
        "chown",
        "mkfs",
        "dd",
        "shred",
        "truncate",
        "wipefs",
        "fdisk",
        "parted",
        "kill",
        "killall",
        "pkill",
        "reboot",
        "shutdown",
        "halt",
        "poweroff",
        "init",
        "systemctl",
        "cron",
        "crontab",
        "at",
        "atq",
        "atrm",
    ]
    .into_iter()
    .collect()
});

/// 中等风险命令 — 有副作用但风险可控——对照旧源 L617-622。
static MODERATE_COMMAND_SET: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "mv", "cp", "mkdir", "touch", "ln", "cd", "export", "unset", "tee", "install", "npm",
        "yarn", "pip", "brew", "apt", "apt-get", "git", "docker", "make", "cmake", "wget", "curl",
        "ssh", "scp",
    ]
    .into_iter()
    .collect()
});

/// 敏感信息泄露命令 — 需权限确认——对照旧源 L625-626。
static SENSITIVE_INFO_COMMANDS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| ["env", "printenv", "set"].into_iter().collect());

// ══════════════════════════════════════════════════════════════
// 原有分类表 (用于 classify 方法) — 对照旧源 L699-713
// ══════════════════════════════════════════════════════════════
static SEARCH_CMDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "find", "grep", "rg", "ag", "ack", "locate", "which", "whereis",
    ]
    .into_iter()
    .collect()
});
static READ_CMDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "cat", "head", "tail", "less", "more", "wc", "stat", "file", "strings", "jq", "awk", "cut",
        "sort", "uniq", "tr",
    ]
    .into_iter()
    .collect()
});
static LIST_CMDS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| ["ls", "tree", "du"].into_iter().collect());
static SHELL_BUILTINS_READONLY: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "cd", "pwd", "echo", "printf", "true", "false", "test", "[", "env", "printenv",
    ]
    .into_iter()
    .collect()
});
static SILENT_CMDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "mv", "cp", "rm", "mkdir", "rmdir", "chmod", "chown", "chgrp", "touch", "ln", "cd",
        "export", "unset", "wait",
    ]
    .into_iter()
    .collect()
});
static NEUTRAL_CMDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    ["echo", "printf", "true", "false", ":"]
        .into_iter()
        .collect()
});

/// 管道/链式命令拆分正则——对照旧源 L643 / L733 / L789 / L1067
/// `"\\s*(?:\\|\\||&&|[|;])\\s*"`。
static PIPELINE_SPLIT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"{J_S}*(?:\|\||&&|[|;]){J_S}*")).expect("static pipeline split")
});

/// 单个 Java `\s` 字符的分隔正则——对照旧源 L1072 `split("\\s")`。
static SINGLE_SPACE_SPLIT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(J_S).expect("static single space split"));

/// 环境变量赋值前缀正则——对照旧源 L652 / L1103 `"^(\\w+=\\S*\\s+)+"`。
static ASSIGNMENT_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^({J_W}+={J_NS}*{J_S}+)+")).expect("static assignment prefix")
});

/// 包装命令前缀正则——对照旧源 L1104 `"^(sudo|env|nice|nohup|time)\\s+"`。
static WRAPPER_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^(sudo|env|nice|nohup|time){J_S}+")).expect("static wrapper prefix")
});

/// 首 token 正则——对照旧源 L713 `"^([\\w./-]+)"`。
static FIRST_TOKEN_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([a-zA-Z_0-9./-]+)").expect("static first token pattern"));

/// 纯数字全匹配正则——对照旧源 L994 / L1020 / L1028 `"^\\d+$"`。
static NUMBER_ONLY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"^{J_D}+$")).expect("static number only"));

/// `git -<number>` 简写正则——对照旧源 L985 `"^-\\d+$"`。
static GIT_DASH_NUMBER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"^-{J_D}+$")).expect("static git dash number"));

/// `git -c` 全匹配正则——对照旧源 L1092 `"git\\s+-c\\s+.*"`。
static GIT_DASH_C: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^git{J_S}+-c{J_S}+{J_DOT}*$")).expect("static git dash c")
});

/// Git 反斜杠路径注入全匹配正则——对照旧源 L1095 `".*\\\\\\\\[^\\s]+\\\\.*"`。
static GIT_BACKSLASH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^{J_DOT}*\\\\{J_NS}+\\{J_DOT}*$")).expect("static git backslash")
});

/// 重定向目标剥离正则——对照旧源 L1072 `".*>>?\\s*"`。
static REDIRECT_STRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"{J_DOT}*>>?{J_S}*")).expect("static redirect strip"));

/// Java 行终止符判定。
fn is_java_line_terminator(c: char) -> bool {
    matches!(c, '\n' | '\r' | '\u{85}' | '\u{2028}' | '\u{2029}')
}

/// 判定 `.*(?<!\\)[<charset>].*`（Java `matches()` 全匹配语义）。
///
/// Rust `regex` 不支持逆序环视，改为手写字符扫描：整串不得含行终止符
/// （否则 `.*` 无法跨越，Java `matches()` 返回 false），且存在字符集内某字符
/// 其前一字符不是反斜杠。对照旧源 L814 / L815。
fn matches_unescaped_char_class(s: &str, set: &[char]) -> bool {
    if s.chars().any(is_java_line_terminator) {
        return false;
    }
    let units: Vec<char> = s.chars().collect();
    for (i, c) in units.iter().enumerate() {
        if set.contains(c) && (i == 0 || units[i - 1] != '\\') {
            return true;
        }
    }
    false
}

/// 判定 `.*(?<![>2&])>(?!>).*`（Java `matches()` 全匹配语义）——对照旧源 L1071。
fn matches_bare_redirect(s: &str) -> bool {
    if s.chars().any(is_java_line_terminator) {
        return false;
    }
    let units: Vec<char> = s.chars().collect();
    for i in 0..units.len() {
        if units[i] != '>' {
            continue;
        }
        let prev_ok = i == 0 || !matches!(units[i - 1], '>' | '2' | '&');
        let next_ok = i + 1 >= units.len() || units[i + 1] != '>';
        if prev_ok && next_ok {
            return true;
        }
    }
    false
}

/// 命令分类结果——对照旧源 `record Classification` L718-723。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Classification {
    /// 是否搜索类命令。
    pub is_search: bool,
    /// 是否读取类命令。
    pub is_read: bool,
    /// 是否列举类命令。
    pub is_list: bool,
}

impl Classification {
    /// 是否为只读命令——对照旧源 L720-722。
    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.is_search || self.is_read || self.is_list
    }
}

/// Bash 命令分类器（层 3 正则降级 Fallback）。
#[derive(Clone, Copy, Debug, Default)]
pub struct BashCommandClassifier;

impl BashCommandClassifier {
    /// 构造分类器（无状态）。
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// 评估命令的风险级别——对照旧源 `assessRisk` L636-694。
    ///
    /// 支持管道/链式命令，取所有子命令中的最高风险级别。
    #[must_use]
    pub fn assess_risk(&self, command: Option<&str>) -> RiskAssessment {
        let Some(command) = command.filter(|c| !java_is_blank(c)) else {
            return RiskAssessment {
                level: RiskLevel::Safe,
                reason: "Empty command".to_owned(),
                command: command.map(str::to_owned),
            };
        };
        let trimmed = java_trim(command);

        // 拆分管道/链式命令
        let segments = java_split(&PIPELINE_SPLIT, trimmed);
        let mut max_level = RiskLevel::Safe;
        let mut max_reason = "Read-only command".to_owned();

        for segment in segments {
            let seg = java_trim(segment);
            if seg.is_empty() {
                continue;
            }

            // 剥离环境变量赋值前缀
            let stripped = ASSIGNMENT_PREFIX.replace(seg, "");
            let first_token = Self::extract_first_token(&stripped);
            if first_token.is_empty() {
                continue;
            }

            let seg_level;
            let seg_reason;

            if BLOCKED_COMMAND_SET.contains(first_token.as_str()) {
                seg_level = RiskLevel::Blocked;
                seg_reason = format!("Privilege escalation command: {first_token}");
            } else if DANGEROUS_COMMAND_SET.contains(first_token.as_str()) {
                seg_level = RiskLevel::Dangerous;
                seg_reason = format!("Destructive command: {first_token}");
            } else if SENSITIVE_INFO_COMMANDS.contains(first_token.as_str())
                && (*stripped == first_token || stripped.starts_with(&format!("{first_token} ")))
            {
                // env 无参数或 printenv → 信息泄露风险
                // 但 env <cmd> 包装命令不算
                if *stripped == first_token || first_token == "printenv" || first_token == "set" {
                    seg_level = RiskLevel::Moderate;
                    seg_reason = format!("Sensitive info disclosure risk: {first_token}");
                } else {
                    seg_level = RiskLevel::Moderate;
                    seg_reason = format!("Command wrapper: {first_token}");
                }
            } else if MODERATE_COMMAND_SET.contains(first_token.as_str()) {
                seg_level = RiskLevel::Moderate;
                seg_reason = format!("Side-effect command: {first_token}");
            } else if self.is_search_or_read_command(Some(&first_token))
                || READONLY_COMMANDS.contains(first_token.as_str())
            {
                seg_level = RiskLevel::Safe;
                seg_reason = format!("Read-only command: {first_token}");
            } else {
                seg_level = RiskLevel::Moderate;
                seg_reason = format!("Unknown command: {first_token}");
            }

            if seg_level > max_level {
                max_level = seg_level;
                max_reason = seg_reason;
            }
        }

        RiskAssessment {
            level: max_level,
            reason: max_reason,
            command: Some(command.to_owned()),
        }
    }

    /// 正则分割 + 首 token 分类——对照旧源 `classify` L729-754。
    ///
    /// 管道/复合命令：所有子命令均为只读 → 整体只读。
    #[must_use]
    pub fn classify(&self, command: Option<&str>) -> Classification {
        let Some(command) = command.filter(|c| !java_is_blank(c)) else {
            return Classification {
                is_search: false,
                is_read: false,
                is_list: false,
            };
        };
        let parts = java_split(&PIPELINE_SPLIT, command);
        let mut all_search = true;
        let mut all_read = true;
        let mut all_list = true;
        let mut has_non_neutral = false;

        for part in parts {
            let cmd = Self::extract_first_token(java_trim(part));
            if cmd.is_empty() || NEUTRAL_CMDS.contains(cmd.as_str()) {
                continue;
            }
            has_non_neutral = true;
            if !SEARCH_CMDS.contains(cmd.as_str()) {
                all_search = false;
            }
            if !READ_CMDS.contains(cmd.as_str()) && !SEARCH_CMDS.contains(cmd.as_str()) {
                all_read = false;
            }
            if !LIST_CMDS.contains(cmd.as_str()) {
                all_list = false;
            }
            if !SEARCH_CMDS.contains(cmd.as_str())
                && !READ_CMDS.contains(cmd.as_str())
                && !LIST_CMDS.contains(cmd.as_str())
                && !SILENT_CMDS.contains(cmd.as_str())
                && !NEUTRAL_CMDS.contains(cmd.as_str())
            {
                return Classification {
                    is_search: false,
                    is_read: false,
                    is_list: false,
                };
            }
        }
        if !has_non_neutral {
            return Classification {
                is_search: false,
                is_read: false,
                is_list: false,
            };
        }
        Classification {
            is_search: all_search,
            is_read: all_read,
            is_list: all_list,
        }
    }

    /// 判断命令是否为搜索或读取命令——对照旧源 `isSearchOrReadCommand` L759-763。
    #[must_use]
    pub fn is_search_or_read_command(&self, argv0: Option<&str>) -> bool {
        let Some(argv0) = argv0.filter(|a| !java_is_blank(a)) else {
            return false;
        };
        SEARCH_CMDS.contains(argv0)
            || READ_CMDS.contains(argv0)
            || LIST_CMDS.contains(argv0)
            || SHELL_BUILTINS_READONLY.contains(argv0)
    }
}

// ══════════════════════════════════════════════════════════════
// isReadOnlyCommand — 三层只读验证 + 管道拆分 + 安全加固 — 对照旧源 L765-903
// ══════════════════════════════════════════════════════════════

impl BashCommandClassifier {
    /// 三层只读验证——对照旧源 `isReadOnlyCommand` L782-903。
    ///
    /// 安全加固（§11.5.6D + §11.5.7）：
    /// 1. 管道拆分：对每段递归调用；
    /// 2. `contains_unquoted_expansion` 前置检查：防止 `$变量` 和 glob 绕过；
    /// 3. `$token` 拒绝 + 花括号展开检测：在 ALLOWLIST 匹配前检查。
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn is_read_only_command(&self, command: Option<&str>) -> bool {
        let Some(command) = command.filter(|c| !java_is_blank(c)) else {
            return false;
        };
        let trimmed = java_trim(command);

        // ★ 管道拆分
        if trimmed.contains('|')
            || trimmed.contains("&&")
            || trimmed.contains("||")
            || trimmed.contains(';')
        {
            let segments = java_split(&PIPELINE_SPLIT, trimmed);
            if segments.len() > 1 {
                for segment in &segments {
                    if !java_is_blank(segment)
                        && !self.is_read_only_command(Some(java_trim(segment)))
                    {
                        return false;
                    }
                }
                return true;
            }
        }

        // ★ containsUnquotedExpansion 前置检查
        if self.contains_unquoted_expansion(trimmed) {
            return false;
        }

        // 层 1: 纯只读命令 — 首 token 匹配
        let first_token = Self::extract_first_token(trimmed);
        if READONLY_COMMANDS.contains(first_token.as_str()) {
            return true;
        }

        // 层 2: 正则匹配只读
        for p in READONLY_REGEXES.iter() {
            if p.is_match(trimmed) {
                return true;
            }
        }

        // ★ find 命令特殊处理: 正则黑名单而非 flag 白名单
        if first_token == "find" {
            if matches_unescaped_char_class(trimmed, &['<', '>', '$', '`', '|', '{', '}', '&']) {
                return false;
            }
            if matches_unescaped_char_class(trimmed, &['(', ')']) {
                return false;
            }
            return !FIND_DANGEROUS_PATTERN.is_match(trimmed);
        }

        // 层 3: flag 级别验证
        if let Some((_, config)) = COMMAND_ALLOWLIST
            .iter()
            .find(|(key, _)| *key == first_token.as_str())
        {
            let args_str = java_trim(java_substring(trimmed, first_token.len()));
            let arg_tokens: Vec<&str> = if args_str.is_empty() {
                Vec::new()
            } else {
                java_split_ws(args_str)
            };

            // ★ $token 拒绝 + 花括号展开检测
            for t in &arg_tokens {
                if t.contains('$') {
                    return false;
                }
                if t.contains('{') && (t.contains(',') || t.contains("..")) {
                    return false;
                }
            }

            if Self::validate_flags(&arg_tokens, 0, config, Some(first_token.as_str())) {
                if let Some(check) = config.additional_dangerous_check()
                    && check(trimmed, &arg_tokens)
                {
                    return false;
                }
                return true;
            }
        }

        // 外部只读命令前缀
        for prefix in EXTERNAL_READONLY_PREFIXES {
            if trimmed.starts_with(prefix) {
                return true;
            }
        }

        // Git 只读命令
        for (key, git_config) in GIT_READONLY_COMMANDS.iter() {
            if trimmed.starts_with(key) {
                let args_str = java_trim(java_substring(trimmed, key.len()));
                let arg_tokens: Vec<&str> = if args_str.is_empty() {
                    Vec::new()
                } else {
                    java_split_ws(args_str)
                };
                // $token 检查
                for t in &arg_tokens {
                    if t.contains('$') {
                        return false;
                    }
                }
                let command_name = key.split(' ').next().unwrap_or("");
                if Self::validate_flags(&arg_tokens, 0, git_config, Some(command_name)) {
                    if let Some(check) = git_config.additional_dangerous_check()
                        && check(trimmed, &arg_tokens)
                    {
                        return false;
                    }
                    return true;
                }
            }
        }

        // GH CLI 只读命令
        for (key, config) in GH_READONLY_COMMANDS.iter() {
            if trimmed.starts_with(key) {
                let args_str = java_trim(java_substring(trimmed, key.len()));
                let arg_tokens: Vec<&str> = if args_str.is_empty() {
                    Vec::new()
                } else {
                    java_split_ws(args_str)
                };
                if Self::validate_flags(&arg_tokens, 0, config, Some("gh"))
                    && !gh_is_dangerous_callback(trimmed, &arg_tokens)
                {
                    return true;
                }
            }
        }

        // Docker 只读命令
        for (key, config) in DOCKER_READONLY_COMMANDS.iter() {
            if trimmed.starts_with(key) {
                let args_str = java_trim(java_substring(trimmed, key.len()));
                let arg_tokens: Vec<&str> = if args_str.is_empty() {
                    Vec::new()
                } else {
                    java_split_ws(args_str)
                };
                if Self::validate_flags(&arg_tokens, 0, config, Some("docker")) {
                    return true;
                }
            }
        }

        // Pyright 只读命令
        for (key, pyright_config) in PYRIGHT_READONLY_COMMANDS.iter() {
            if trimmed.starts_with(key) {
                let args_str = java_trim(java_substring(trimmed, key.len()));
                let arg_tokens: Vec<&str> = if args_str.is_empty() {
                    Vec::new()
                } else {
                    java_split_ws(args_str)
                };
                if Self::validate_flags(&arg_tokens, 0, pyright_config, Some(key)) {
                    if let Some(check) = pyright_config.additional_dangerous_check()
                        && check(trimmed, &arg_tokens)
                    {
                        return false;
                    }
                    return true;
                }
            }
        }

        false
    }

    /// 检测未引用的变量展开和 glob——对照旧源 `containsUnquotedExpansion` L913-940。
    ///
    /// 使用字符级状态机精确跟踪引号状态。
    #[must_use]
    pub fn contains_unquoted_expansion(&self, command: &str) -> bool {
        let units: Vec<char> = command.chars().collect();
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut escaped = false;

        for (i, &c) in units.iter().enumerate() {
            if escaped {
                escaped = false;
                continue;
            }
            if c == '\\' && !in_single_quote {
                escaped = true;
                continue;
            }
            if c == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
                continue;
            }
            if c == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
                continue;
            }
            if in_single_quote {
                continue;
            }
            if c == '$' && i + 1 < units.len() {
                let next = units[i + 1];
                if next.is_alphabetic() || next == '_' || next == '{' || next == '(' {
                    return true;
                }
            }
            if !in_double_quote {
                if c == '*' || c == '?' {
                    return true;
                }
                // 旧源 L935-937：`comma > i && brace > comma`（`indexOf` 未找到即 -1，
                // 恒不满足严格大于，故 `None` 分支同样落到 false）。
                if c == '{'
                    && let Some(comma) = index_of_from(&units, ',', i)
                    && comma > i
                    && let Some(brace) = index_of_from(&units, '}', i)
                    && brace > comma
                {
                    return true;
                }
            }
        }
        false
    }

    /// 安全加固版 flag 验证——对照旧源 `validateFlags` L958-1050。
    ///
    /// 修复项：
    /// 1. `--flag=value` 内联值解析（`hasEquals` 安全语义）；
    /// 2. 组合短 flag 安全（要求所有 bundled flag 为 `NONE` 类型）；
    /// 3. `respectsDoubleDash` 支持；
    /// 4. `git -<number>` 简写支持；
    /// 5. `grep`/`rg` `-A20` 附着数字参数支持；
    /// 6. xargs 安全目标命令检测。
    #[allow(clippy::too_many_lines)]
    fn validate_flags(
        tokens: &[&str],
        start_index: usize,
        config: &FlagConfig,
        command_name: Option<&str>,
    ) -> bool {
        let mut i = start_index;
        while i < tokens.len() {
            let mut token = tokens[i];
            if token.is_empty() {
                i += 1;
                continue;
            }

            // -- 双横杠处理
            if token == "--" {
                if config.respects_double_dash() {
                    break; // -- 后面都是位置参数，停止验证
                }
                i += 1;
                continue;
            }

            if token.starts_with('-') && token.chars().count() > 1 && FLAG_PATTERN.is_match(token) {
                // ★ 修复1: --flag=value 格式解析
                let has_equals = token.contains('=');
                let eq_pos = token.find('=');
                let flag = match eq_pos {
                    Some(p) => &token[..p],
                    None => token,
                };
                let inline_value = eq_pos.map(|p| &token[p + 1..]);

                if flag.is_empty() {
                    return false;
                }

                let flag_arg_type = config.safe_flags().get(flag).copied();

                let Some(flag_arg_type) = flag_arg_type else {
                    // git -<number> 简写
                    if command_name == Some("git") && GIT_DASH_NUMBER.is_match(flag) {
                        i += 1;
                        continue;
                    }
                    // grep/rg -A20 附着数字参数
                    if (command_name == Some("grep") || command_name == Some("rg"))
                        && flag.starts_with('-')
                        && !flag.starts_with("--")
                        && flag.chars().count() > 2
                    {
                        let potential_flag = &flag[..2];
                        let potential_value = &flag[2..];
                        if config.safe_flags().contains_key(potential_flag)
                            && NUMBER_ONLY.is_match(potential_value)
                        {
                            i += 1;
                            continue;
                        }
                    }
                    // ★ 修复2: 组合短 flag 安全检查
                    if flag.starts_with('-') && !flag.starts_with("--") && flag.chars().count() > 2
                    {
                        let mut all_none = true;
                        for ch in flag.chars().skip(1) {
                            let single_flag = format!("-{ch}");
                            let Some(ty) = config.safe_flags().get(single_flag.as_str()).copied()
                            else {
                                return false;
                            };
                            if ty != FlagArgType::None {
                                all_none = false;
                                break;
                            }
                        }
                        if !all_none {
                            return false;
                        }
                        i += 1;
                        continue;
                    }
                    return false; // 未知 flag
                };

                // 验证 flag 参数
                if flag_arg_type == FlagArgType::None {
                    if has_equals {
                        return false;
                    }
                    i += 1;
                } else if has_equals {
                    if flag_arg_type == FlagArgType::Number
                        && inline_value.is_some_and(|v| !v.is_empty() && !NUMBER_ONLY.is_match(v))
                    {
                        return false;
                    }
                    i += 1;
                } else {
                    if i + 1 >= tokens.len() {
                        return false;
                    }
                    let arg_value = tokens[i + 1];
                    if flag_arg_type == FlagArgType::Number && !NUMBER_ONLY.is_match(arg_value) {
                        return false;
                    }
                    i += 2;
                }
            } else {
                // 非 flag（位置参数）— xargs 特殊处理
                if command_name == Some("xargs") {
                    if token == "--" && i + 1 < tokens.len() {
                        i += 1;
                        token = tokens[i];
                    }
                    if SAFE_TARGET_COMMANDS_FOR_XARGS.contains(token) {
                        break; // 安全目标命令 → 停止验证
                    }
                    return false; // 未知目标命令 → 拒绝
                }
                i += 1;
            }
        }
        true
    }

    /// 兼容旧签名的适配方法——对照旧源 L1053-1057（旧源无调用点，一并还原）。
    #[must_use]
    pub fn validate_flags_str(args_str: &str, config: &FlagConfig) -> bool {
        if java_is_blank(args_str) {
            return true;
        }
        let tokens = java_split_ws(args_str);
        Self::validate_flags(&tokens, 0, config, None)
    }

    /// 检查复合命令中是否包含写操作——对照旧源 `isCompoundCommandReadOnly` L1066-1085。
    #[must_use]
    pub fn is_compound_command_read_only(&self, command: &str) -> bool {
        let segments = java_split(&PIPELINE_SPLIT, command);
        for segment in segments {
            let trimmed = java_trim(segment);
            if trimmed.is_empty() {
                continue;
            }
            if matches_bare_redirect(trimmed) || trimmed.contains(">>") {
                let stripped = REDIRECT_STRIP.replace_all(trimmed, "");
                let target_src = java_trim(&stripped);
                let redirect_target = java_split(&SINGLE_SPACE_SPLIT, target_src)
                    .first()
                    .copied()
                    .unwrap_or("");
                if !redirect_target.starts_with("/dev/") {
                    return false;
                }
            }
            let first_token = Self::extract_first_token(trimmed);
            if !self.is_read_only_command(Some(trimmed))
                && !READONLY_COMMANDS.contains(first_token.as_str())
                && !first_token.is_empty()
                && matches!(
                    first_token.as_str(),
                    "rm" | "mv"
                        | "cp"
                        | "mkdir"
                        | "rmdir"
                        | "chmod"
                        | "chown"
                        | "touch"
                        | "ln"
                        | "tee"
                        | "dd"
                        | "mkfs"
                )
            {
                return false;
            }
        }
        true
    }

    /// Git 命令特有安全检查——对照旧源 `isGitCommandSafe` L1090-1097。
    #[must_use]
    pub fn is_git_command_safe(&self, command: &str) -> bool {
        if !command.starts_with("git ") {
            return true;
        }
        if GIT_DASH_C.is_match(command) {
            return false;
        }
        if command.contains("--exec-path=") {
            return false;
        }
        if command.contains("--config-env") {
            return false;
        }
        if GIT_BACKSLASH.is_match(command) {
            return false;
        }
        true
    }

    /// 提取首 token，跳过环境变量赋值（`KEY=VAL`）和 `sudo`/`env` 前缀
    /// ——对照旧源 `extractFirstToken` L1102-1107。
    fn extract_first_token(part: &str) -> String {
        let s = ASSIGNMENT_PREFIX.replace(part, "").into_owned();
        let s = WRAPPER_PREFIX.replace(&s, "").into_owned();
        FIRST_TOKEN_PATTERN
            .captures(&s)
            .map_or_else(String::new, |c| c[1].to_owned())
    }
}

/// `String.indexOf(char, int)` 的等价实现。
///
/// Java 以 `-1` 表示未找到，Rust 侧改用 `None`——语义等价，调用点已按
/// `> i` / `> comma` 的严格大于语义改写为 `Option` 比较。
fn index_of_from(units: &[char], needle: char, from: usize) -> Option<usize> {
    let mut i = from;
    while i < units.len() {
        if units[i] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

// ══════════════════════════════════════════════════════════════
// [第五层] 动态超时分类 — 基于命令内容推荐合适的超时时间
// 比 classifyForUI() 更细粒度，区分编译/测试/安装等长时间操作
// ══════════════════════════════════════════════════════════════

impl BashCommandClassifier {
    /// 分类命令以推荐合适的超时时间（旧源 `BashCommandClassifier.java:1121-1138`）。
    ///
    /// 比 [`Self::classify_for_ui`] 更细粒度，区分编译/测试/安装等长时间操作。
    #[must_use]
    pub fn classify_for_timeout(&self, command: Option<&str>) -> CommandCategory {
        let Some(command) = command else {
            return CommandCategory::Unknown;
        };
        if java_is_blank(command) {
            return CommandCategory::Unknown;
        }
        let trimmed = java_trim(command).to_lowercase();

        // 编译命令 — 300s
        if Self::is_compilation_command(&trimmed) {
            return CommandCategory::Compilation;
        }
        // 测试命令 — 600s
        if Self::is_test_command(&trimmed) {
            return CommandCategory::TestExecution;
        }
        // 包安装命令 — 300s
        if Self::is_package_install_command(&trimmed) {
            return CommandCategory::PackageInstall;
        }
        // Git操作 — 60s
        if Self::is_git_command(&trimmed) {
            return CommandCategory::GitOperation;
        }
        // 服务启动 — 120s
        if Self::is_server_start_command(&trimmed) {
            return CommandCategory::ServerStart;
        }

        // 退回到UI分类（READ_ONLY/SEARCH/MODIFICATION/SYSTEM_INFO/UNKNOWN）
        // 旧源 L1137 传入的是原始 `command` 而非 `trimmed`，此处逐字保留。
        self.classify_for_ui(Some(command))
    }

    /// 旧源 `BashCommandClassifier.java:1140-1150`。
    fn is_compilation_command(cmd: &str) -> bool {
        cmd.starts_with("mvn compile")
            || cmd.starts_with("mvn package")
            || cmd.starts_with("./mvnw compile")
            || cmd.starts_with("./mvnw package")
            || cmd.starts_with("mvn clean")
            || cmd.starts_with("./mvnw clean")
            || cmd.starts_with("npm run build")
            || cmd.starts_with("npx tsc")
            || cmd.starts_with("cargo build")
            || cmd.starts_with("go build")
            || cmd.starts_with("gcc ")
            || cmd.starts_with("g++ ")
            || cmd.starts_with("javac ")
            || cmd.starts_with("make")
            || cmd.starts_with("gradle build")
            || cmd.starts_with("./gradlew build")
            || cmd.starts_with("gradle compile")
            || cmd.starts_with("./gradlew compile")
    }

    /// 旧源 `BashCommandClassifier.java:1152-1160`。
    fn is_test_command(cmd: &str) -> bool {
        cmd.starts_with("mvn test")
            || cmd.starts_with("./mvnw test")
            || cmd.starts_with("mvn verify")
            || cmd.starts_with("./mvnw verify")
            || cmd.starts_with("npm test")
            || cmd.starts_with("npx jest")
            || cmd.starts_with("npx vitest")
            || cmd.starts_with("npx playwright")
            || cmd.starts_with("pytest")
            || cmd.starts_with("python -m pytest")
            || cmd.starts_with("cargo test")
            || cmd.starts_with("go test")
            || cmd.starts_with("gradle test")
            || cmd.starts_with("./gradlew test")
    }

    /// 旧源 `BashCommandClassifier.java:1162-1169`。
    fn is_package_install_command(cmd: &str) -> bool {
        cmd.starts_with("npm install")
            || cmd.starts_with("npm ci")
            || cmd.starts_with("yarn install")
            || cmd.starts_with("pnpm install")
            || cmd.starts_with("pip install")
            || cmd.starts_with("pip3 install")
            || cmd.starts_with("mvn dependency")
            || cmd.starts_with("./mvnw dependency")
            || cmd.starts_with("cargo fetch")
            || cmd.starts_with("go mod download")
            || cmd.starts_with("bundle install")
            || cmd.starts_with("composer install")
    }

    /// 旧源 `BashCommandClassifier.java:1171-1173`。
    fn is_git_command(cmd: &str) -> bool {
        cmd.starts_with("git ")
    }

    /// 旧源 `BashCommandClassifier.java:1175-1181`。
    fn is_server_start_command(cmd: &str) -> bool {
        cmd.starts_with("npm start")
            || cmd.starts_with("npm run dev")
            || cmd.starts_with("npm run serve")
            || cmd.starts_with("java -jar")
            || cmd.starts_with("python -m uvicorn")
            || cmd.starts_with("python manage.py runserver")
            || cmd.starts_with("./mvnw spring-boot:run")
    }
}

// ══════════════════════════════════════════════════════════════
// [第四层] UI 展示分类 — 与安全分类正交，仅用于日志/UI 标签
// 不影响 AST→正则→路径验证 三层安全架构
// ══════════════════════════════════════════════════════════════

/// 旧源 `BashCommandClassifier.java:1188-1189`。
static UI_SEARCH_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "grep", "find", "rg", "ag", "ack", "locate", "whereis", "which", "fd", "fdfind",
    ]
    .into_iter()
    .collect()
});

/// 旧源 `BashCommandClassifier.java:1191-1195`。
static UI_READ_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "cat", "head", "tail", "less", "more", "wc", "stat", "file", "strings", "jq", "awk", "cut",
        "sort", "uniq", "tr", "ls", "tree", "du", "df", "diff", "hexdump", "od", "nl", "readlink",
        "realpath", "basename", "dirname",
    ]
    .into_iter()
    .collect()
});

/// 旧源 `BashCommandClassifier.java:1197-1199`。
static UI_MODIFICATION_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "rm", "rmdir", "mkdir", "touch", "mv", "cp", "chmod", "chown", "ln", "tee", "install",
        "dd", "mkfs", "truncate", "shred",
    ]
    .into_iter()
    .collect()
});

/// 旧源 `BashCommandClassifier.java:1201-1204`。
static UI_SYSTEM_INFO_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "uname", "pwd", "whoami", "env", "printenv", "hostname", "id", "uptime", "free", "nproc",
        "locale", "groups", "date", "cal", "getconf", "ulimit", "umask",
    ]
    .into_iter()
    .collect()
});

impl BashCommandClassifier {
    /// UI 展示分类（旧源 `BashCommandClassifier.java:1215-1237`）——独立于安全分类，
    /// 仅用于日志和 UI 标签。
    ///
    /// 简单的命令前缀匹配，解析命令第一个 token 并匹配到已知命令集合。
    /// 不影响安全决策，独立于 AST→正则→路径验证 三层架构。
    #[must_use]
    pub fn classify_for_ui(&self, command: Option<&str>) -> CommandCategory {
        let Some(command) = command else {
            return CommandCategory::Unknown;
        };
        if java_is_blank(command) {
            return CommandCategory::Unknown;
        }
        let first_token = Self::extract_first_token(java_trim(command));
        if first_token.is_empty() {
            return CommandCategory::Unknown;
        }

        if UI_SEARCH_COMMANDS.contains(first_token.as_str()) {
            return CommandCategory::Search;
        }
        if UI_READ_COMMANDS.contains(first_token.as_str()) {
            return CommandCategory::ReadOnly;
        }
        if UI_MODIFICATION_COMMANDS.contains(first_token.as_str()) {
            return CommandCategory::Modification;
        }
        if UI_SYSTEM_INFO_COMMANDS.contains(first_token.as_str()) {
            return CommandCategory::SystemInfo;
        }
        CommandCategory::Unknown
    }
}
