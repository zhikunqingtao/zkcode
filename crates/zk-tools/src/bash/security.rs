//! Bash AST 安全分析器——对照旧 `tool/bash/BashSecurityAnalyzer.java`（1029 行）。
//!
//! 核心问题：「能否为此命令字符串中的每个简单命令生成可信的 argv[]？」
//!
//! - YES → [`ParseForSecurityResult::Simple`] —— 下游匹配 argv[0] 与权限规则；
//! - NO  → [`ParseForSecurityResult::TooComplex`] —— 需用户确认；
//! - 解析失败 → [`ParseForSecurityResult::ParseUnavailable`] —— 回退遗留路径。
//!
//! 这不是沙箱，不阻止危险命令执行。
//!
//! 留痕：`docs/compatibility.md` §5（子阶段 2.4）。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;

use super::ast::{BashAstNode, ParseForSecurityResult, ProgramNode, SimpleCommandNode};
use super::blacklist::{BlockLevel, CommandBlacklistService};
use super::heredoc::HeredocExtractor;
use super::javastr::{J_DOT, java_is_blank, java_split_ws, java_trim};
use super::lexer::SHELL_KEYWORDS;
use super::parser::parse;

// ──── 安全限制常量 ────

/// 命令最大长度——超过直接返回 parse-unavailable（旧源 L39）。
const MAX_COMMAND_LENGTH: usize = 10_000;

// ──── 预检查正则 ────

/// 控制字符（ASCII 0-31 除 `\t` `\n` `\r`）——旧源 L44-45。
static CONTROL_CHAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]").expect("static control char regex")
});

/// Unicode 空白（非 ASCII 空白字符）——旧源 L48-49。
static UNICODE_WHITESPACE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}]")
        .expect("static unicode whitespace regex")
});

// 旧源 L51-52 的 `BACKSLASH_WHITESPACE_RE` 为注释掉的预留位（当前禁用以避免误报），
// 移植后同样不启用，仅在此保留说明。

/// Zsh `~[...]` 动态目录——旧源 L55-56。
static ZSH_TILDE_BRACKET_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"~\[").expect("static zsh tilde bracket regex"));

/// Zsh `=cmd` 扩展（仅匹配行首/空白后的 `=word`，排除 `VAR=value` 赋值）——旧源 L59-60。
static ZSH_EQUALS_EXPANSION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|[ \t\n\x0B\x0C\r;&|])=[a-zA-Z]").expect("static zsh equals expansion regex")
});

/// 大括号展开混淆——仅匹配 `{xx,yy}` 含引号的模式，排除普通大括号分组 `{ cmd; }`（旧源 L63-64）。
static BRACE_WITH_QUOTE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\{[^}]*,[^}]*['"]\}"#).expect("static brace with quote regex"));

/// argv 中隐藏换行 `#` 模式——旧源 L67-68。
static NEWLINE_HASH_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\n#").expect("static newline hash regex"));

// ──── 安全环境变量 ────

/// `SAFE_ENV_VARS`——解析时可安全替换为占位符（旧源 L73-82）。
static SAFE_ENV_VARS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        // 路径
        "HOME",
        "PWD",
        "OLDPWD",
        "TMPDIR",
        "PATH",
        // 用户
        "USER",
        "LOGNAME",
        "UID",
        "EUID",
        "HOSTNAME",
        // Shell
        "SHELL",
        "BASH_VERSION",
        "BASHPID",
        "SHLVL",
        "HISTFILE",
        "IFS",
        // 系统
        "PPID",
        "RANDOM",
        "SECONDS",
        "LINENO",
    ]
    .into_iter()
    .collect()
});

/// 安全分析级特殊变量（不含 `@` 和 `*`）——旧源 L85-87。
static SPECIAL_VAR_NAMES: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| ["?", "$", "!", "#", "0", "-"].into_iter().collect());

/// 变量占位符——旧源 L90。
const VAR_PLACEHOLDER: &str = "__VAR_PLACEHOLDER__";
/// 命令替换占位符——旧源 L91。
const CMDSUB_PLACEHOLDER: &str = "__CMDSUB_OUTPUT__";

/// `DANGEROUS_TYPES`——这些节点类型在 AST 遍历中直接返回 too-complex（旧源 L98-105）。
///
/// 旧源该常量声明后**未被任何代码引用**（对应语义已由解析器直接产出
/// `TooComplexNode`、以及 `checkSemantics` 的 `$"` / `{a,b}` 检查覆盖）；
/// 移植保留常量以对齐可审计性，同样不引用。
#[allow(dead_code)]
static DANGEROUS_TYPES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "arithmetic_expansion",  // $(( expr ))
        "process_substitution",  // <() / >()
        "brace_expression",      // {a,b,c}
        "translated_string",     // $"..."
        "c_style_for_statement", // for((i=0;i<10;i++))
        "ternary_expression",    // ((a?b:c))
    ]
    .into_iter()
    .collect()
});

// ──── Eval 类内置命令 (直接拒绝) ────

/// 安全关键：遗漏任一项都可能导致 RCE 漏洞（旧源 L112-124）。
static EVAL_LIKE_BUILTINS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "eval",
        "source",
        ".",
        "exec",
        "fc",        // fc -e/-s 执行编辑器/命令
        "coproc",    // 以协进程方式执行任意命令
        "noglob",    // zsh precommand modifiers
        "nocorrect", // zsh precommand modifiers
        "trap",      // trap 'code' EXIT
        "enable",
        "hash",
        "mapfile",   // -C callback 回调执行
        "readarray", // -C callback 回调执行
        "bind",      // bind -x 执行 shell 命令
        "complete",  // -C/-F/-W 交互式回调
        "compgen",   // -C/-F/-W 交互式回调
        "alias",     // expand_aliases 风险
        "let",       // 算术求值 = $(()) 等价
    ]
    .into_iter()
    .collect()
});

/// Zsh 危险内置命令——旧源 L127-131。
static ZSH_DANGEROUS_BUILTINS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "zmodload",
        "autoload",
        "functions",
        "zle",
        "zstyle",
        "zformat",
        "zparseopts",
        "sched",
        "ztcp",
        "zsocket",
    ]
    .into_iter()
    .collect()
});

/// 包装命令（需递归剥离）——旧源 L134-137。
static WRAPPER_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "command", "builtin", "sudo", "nohup", "nice", "env", "stdbuf", "timeout", "xargs",
    ]
    .into_iter()
    .collect()
});

/// `[[` 算术比较操作符——旧源 L140-142。
static ARITHMETIC_COMPARE_OPS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    ["-eq", "-ne", "-lt", "-gt", "-le", "-ge"]
        .into_iter()
        .collect()
});

/// 裸变量展开不安全正则（空格/tab/换行/`*?[`）——旧源 L145-146。
///
/// 旧源该常量声明后**未被任何代码引用**；移植保留以对齐可审计性。
#[allow(dead_code)]
static BARE_VAR_UNSAFE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[ \t\n\x0B\x0C\r*?\[]").expect("static bare var unsafe regex"));

/// 数组下标含 `[`——旧源 L149-150。
static SUBSCRIPT_BRACKET_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[").expect("static subscript bracket regex"));

// ──── 危险子命令黑名单 ────

/// 旧源 L169-174（`Map.of` 无序，键唯一匹配 + 值集合全部映射到 `ASK`，
/// 故改用有序 `slice` 不影响判定结果）。
const DANGEROUS_SUBCOMMANDS: &[(&str, &[&str])] = &[
    (
        "git",
        &[
            "push --force",
            "push -f",
            "reset --hard",
            "clean -fd",
            "clean -fdx",
        ],
    ),
    ("docker", &["rm", "rmi", "system prune", "exec", "run"]),
    ("npm", &["publish", "unpublish"]),
    ("kubectl", &["delete", "apply", "exec", "edit"]),
];

/// 安全级别——旧源 L177。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecurityLevel {
    /// 安全，可直接执行。
    Safe,
    /// 需用户确认。
    Ask,
    /// 拒绝执行。
    Deny,
}

/// 受限 AST 环境变量解析状态——旧源 `EnvironmentParseStatus` L186。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EnvironmentParseStatus {
    /// 分析成功。
    #[default]
    Success,
    /// 含动态/不支持语义。
    TooComplex,
    /// 解析器不可用。
    Unavailable,
}

/// 基于受限 AST 的 Shell 环境变量引用分析结果——旧源 L180-197。
///
/// 旧源以 `LinkedHashSet` 累积、经 `Set.copyOf` 转为**无序**不可变集合返回，
/// 插入序在结果上不可观测；移植统一用 `BTreeSet`（确定性字典序），语义等价。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EnvironmentReferenceAnalysis {
    /// 命令内定义的局部变量。
    pub local_definitions: BTreeSet<String>,
    /// 引用的继承环境变量。
    pub inherited_references: BTreeSet<String>,
    /// 其中命中敏感命名规则的变量。
    pub sensitive_inherited_references: BTreeSet<String>,
    /// 解析状态。
    pub parse_status: EnvironmentParseStatus,
    /// 非成功状态下的原因。
    pub reason: Option<String>,
}

impl EnvironmentReferenceAnalysis {
    /// 旧源 L194-196：非 `SUCCESS` 一律保守询问，绝不隐式放行。
    #[must_use]
    pub fn requires_conservative_ask(&self) -> bool {
        self.parse_status != EnvironmentParseStatus::Success
    }
}

/// Shell 变量引用正则——旧源 L199-200。
static SHELL_VARIABLE_REFERENCE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{?([A-Za-z_][A-Za-z0-9_]*|[0-9]+|[?$!#@*_-])\}?")
        .expect("static shell variable reference regex")
});

/// 敏感环境变量命名正则（`CASE_INSENSITIVE` + `matches()` 全匹配）——旧源 L201-203。
static SENSITIVE_ENV_NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"(?i)^{J_DOT}*(TOKEN|API_KEY|SECRET|PASSWORD|PASSWD|PRIVATE_KEY|CREDENTIAL){J_DOT}*$"
    ))
    .expect("static sensitive env name regex")
});

/// 遍历中抛出的 too-complex 信号——旧源内部异常 `TooComplexException` L1018-1027。
struct TooComplexSignal {
    /// 拒绝原因。
    reason: String,
    /// 触发节点类型。
    node_type: String,
}

/// 环境变量累积器——旧源 `EnvironmentAccumulator` L520-524。
#[derive(Default)]
struct EnvironmentAccumulator {
    local_definitions: BTreeSet<String>,
    inherited_references: BTreeSet<String>,
    sensitive_inherited_references: BTreeSet<String>,
}

// ──── 分析器 ────

/// Bash AST 安全分析器——旧源 `BashSecurityAnalyzer` L32-1028。
///
/// 旧源构造器注入三个协作者：`PathValidator`（**字段声明后未被任何方法引用**，
/// 移植不持有）、`AppStateStore`（仅供无上下文重载解析 cwd/projectRoot，属会话
/// 状态层，移植时由调用方显式传入）、`CommandBlacklistService`（本 crate 已移植）。
pub struct BashSecurityAnalyzer {
    /// 系统级命令黑名单——旧源 L157 注入字段。
    blacklist: CommandBlacklistService,
}

impl Default for BashSecurityAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl BashSecurityAnalyzer {
    /// 以默认内建黑名单构造。
    #[must_use]
    pub fn new() -> Self {
        Self {
            blacklist: CommandBlacklistService::new(),
        }
    }

    /// 以外部配置好的黑名单构造——对齐旧源 L159-165 的构造器注入。
    #[must_use]
    pub fn with_blacklist(blacklist: CommandBlacklistService) -> Self {
        Self { blacklist }
    }

    /// 只读访问注入的黑名单服务。
    #[must_use]
    pub const fn blacklist(&self) -> &CommandBlacklistService {
        &self.blacklist
    }

    /// 授权流程使用的显式上下文入口——旧源 L224-277。
    ///
    /// 旧源形参 `cwd` / `projectRoot` 在方法体内**未被使用**（路径鉴权由
    /// `PathValidator` 在权限管线另行完成），此处保留签名以对齐调用契约。
    #[must_use]
    pub fn parse_for_security_with_context(
        &self,
        cmd: Option<&str>,
        _cwd: &std::path::Path,
        _project_root: &std::path::Path,
    ) -> ParseForSecurityResult {
        self.parse_for_security(cmd)
    }

    /// 安全解析入口——返回三态结果（旧源 L224-277）。
    #[must_use]
    pub fn parse_for_security(&self, cmd: Option<&str>) -> ParseForSecurityResult {
        // 旧源 L227-229：空字符串 → simple([])。
        let Some(cmd) = cmd else {
            return ParseForSecurityResult::Simple {
                commands: Vec::new(),
            };
        };
        if java_is_blank(cmd) {
            return ParseForSecurityResult::Simple {
                commands: Vec::new(),
            };
        }

        // 旧源 L231-234：命令过长 → parse-unavailable（`length()` 为 UTF-16 码元数）。
        if cmd.encode_utf16().count() > MAX_COMMAND_LENGTH {
            return ParseForSecurityResult::ParseUnavailable;
        }

        // 旧源 L236-241：★ 系统级命令黑名单前置检查（在预检查链之前）。
        let block_result = self.blacklist.check_command(cmd);
        if block_result.level == BlockLevel::AbsoluteDeny {
            return ParseForSecurityResult::TooComplex {
                reason: block_result.reason.unwrap_or_default(),
                node_type: "command-blacklist-deny".to_owned(),
            };
        }

        // 旧源 L243-247：── 预检查链 ──
        if let Some(pre_check_reason) = Self::run_pre_checks(cmd) {
            return ParseForSecurityResult::TooComplex {
                reason: pre_check_reason.to_owned(),
                node_type: "pre-check".to_owned(),
            };
        }

        // 旧源 L249-254：── 解析 ──（解析器超时/预算耗尽/命令过长 → null）
        let Some(root) = parse(cmd) else {
            return ParseForSecurityResult::ParseUnavailable;
        };

        // 旧源 L256-257：── AST 遍历 ──
        let result = Self::walk_program(cmd, &root);

        // 旧源 L259-274：★ Heredoc 安全分析
        if HeredocExtractor::contains_heredoc(cmd) {
            let extractor = HeredocExtractor::new();
            let heredoc_result = extractor.extract(cmd);
            for _entry in &heredoc_result.heredocs {
                let heredoc_pos = cmd.find("<<");
                let cmd_before = match heredoc_pos {
                    Some(pos) if pos > 0 => java_split_ws(java_trim(&cmd[..pos]))
                        .first()
                        .copied()
                        .unwrap_or(""),
                    _ => "",
                };
                // cat/echo/printf + heredoc → 只读
                if matches!(cmd_before, "cat" | "echo" | "printf") {
                    continue;
                }
                // python/bash/sh/node/ruby/perl + heredoc → 代码注入风险
                return ParseForSecurityResult::TooComplex {
                    reason: format!("Heredoc with {cmd_before} requires permission"),
                    node_type: "heredoc-security".to_owned(),
                };
            }
        }

        result
    }

    /// 预检查链——返回拒绝原因或 `None`（通过）。旧源 L531-548。
    fn run_pre_checks(cmd: &str) -> Option<&'static str> {
        if CONTROL_CHAR_RE.is_match(cmd) {
            return Some("Command contains control characters");
        }
        if UNICODE_WHITESPACE_RE.is_match(cmd) {
            return Some("Command contains Unicode whitespace");
        }
        if ZSH_TILDE_BRACKET_RE.is_match(cmd) {
            return Some("Command contains zsh ~[...] dynamic directory");
        }
        if ZSH_EQUALS_EXPANSION_RE.is_match(cmd) {
            return Some("Command contains zsh =cmd expansion");
        }
        if BRACE_WITH_QUOTE_RE.is_match(cmd) {
            return Some("Command contains brace-quote confusion");
        }
        None
    }

    /// `walkProgram`——从根节点开始遍历 AST，提取 `SimpleCommand` 列表（旧源 L555-574）。
    ///
    /// 旧源形参 `cmd` 在方法体内未被使用，此处保留以对齐签名。
    fn walk_program(_cmd: &str, root: &ProgramNode) -> ParseForSecurityResult {
        let mut commands: Vec<SimpleCommandNode> = Vec::new();
        let mut var_scope: HashMap<String, String> = HashMap::new();

        if let Err(signal) = Self::collect_commands_program(root, &mut commands, &mut var_scope) {
            return ParseForSecurityResult::TooComplex {
                reason: signal.reason,
                node_type: signal.node_type,
            };
        }

        // 旧源 L565-571：── 语义检查 ──
        for sc in &commands {
            if let Some(semantic_reason) = Self::check_semantics(sc) {
                return ParseForSecurityResult::TooComplex {
                    reason: semantic_reason,
                    node_type: "semantic-check".to_owned(),
                };
            }
        }

        ParseForSecurityResult::Simple { commands }
    }

    /// `ProgramNode` 分支——旧源 L585-589。
    fn collect_commands_program(
        prog: &ProgramNode,
        commands: &mut Vec<SimpleCommandNode>,
        var_scope: &mut HashMap<String, String>,
    ) -> Result<(), TooComplexSignal> {
        for stmt in &prog.statements {
            // 旧源 L591-592：`StatementNode` → 递归 body。
            Self::collect_commands(&stmt.body, commands, var_scope)?;
        }
        Ok(())
    }

    /// 递归收集 `SimpleCommandNode`——旧源 L579-697。
    #[allow(clippy::too_many_lines)]
    fn collect_commands(
        node: &BashAstNode,
        commands: &mut Vec<SimpleCommandNode>,
        var_scope: &mut HashMap<String, String>,
    ) -> Result<(), TooComplexSignal> {
        match node {
            BashAstNode::Program(prog) => {
                Self::collect_commands_program(prog, commands, var_scope)?;
            }

            BashAstNode::Statement(stmt) => {
                Self::collect_commands(&stmt.body, commands, var_scope)?;
            }

            BashAstNode::SimpleCommand(cmd) => commands.push(cmd.clone()),

            BashAstNode::Pipeline {
                commands: stages, ..
            } => {
                // 旧源 L597-603：管道各阶段 scope 副本（子 shell 语义）。
                for pipe_cmd in stages {
                    let mut pipe_scope = var_scope.clone();
                    Self::collect_commands(pipe_cmd, commands, &mut pipe_scope)?;
                }
            }

            BashAstNode::AndOr {
                left,
                operator,
                right,
                ..
            } => {
                // 旧源 L605-615：`&&` → scope 线性传递；`||` → scope 重置为入口快照。
                Self::collect_commands(left, commands, var_scope)?;
                if operator == "&&" {
                    Self::collect_commands(right, commands, var_scope)?;
                } else {
                    let mut reset_scope = var_scope.clone();
                    Self::collect_commands(right, commands, &mut reset_scope)?;
                }
            }

            BashAstNode::RedirectedStatement { body, .. } => {
                Self::collect_commands(body, commands, var_scope)?;
            }

            BashAstNode::Subshell { body, .. } => {
                // 旧源 L620-624：子 shell → scope 副本。
                let mut sub_scope = var_scope.clone();
                Self::collect_commands_program(body, commands, &mut sub_scope)?;
            }

            BashAstNode::BraceGroup { body, .. } => {
                Self::collect_commands_program(body, commands, var_scope)?;
            }

            BashAstNode::If {
                condition,
                then_body,
                else_body,
                ..
            } => {
                // 旧源 L629-638：条件用真实 scope，分支用 scope 副本。
                Self::collect_commands_program(condition, commands, var_scope)?;
                let mut then_scope = var_scope.clone();
                Self::collect_commands_program(then_body, commands, &mut then_scope)?;
                if let Some(else_body) = else_body {
                    let mut else_scope = var_scope.clone();
                    Self::collect_commands_program(else_body, commands, &mut else_scope)?;
                }
            }

            BashAstNode::For { var_name, body, .. } => {
                // 旧源 L640-645：循环变量 → VAR_PLACEHOLDER，body 用 scope 副本。
                let mut for_scope = var_scope.clone();
                for_scope.insert(var_name.clone(), VAR_PLACEHOLDER.to_owned());
                Self::collect_commands_program(body, commands, &mut for_scope)?;
            }

            BashAstNode::While {
                condition, body, ..
            } => {
                Self::collect_commands_program(condition, commands, var_scope)?;
                let mut body_scope = var_scope.clone();
                Self::collect_commands_program(body, commands, &mut body_scope)?;
            }

            BashAstNode::Case { items, .. } => {
                for item in items {
                    let mut item_scope = var_scope.clone();
                    Self::collect_commands_program(&item.body, commands, &mut item_scope)?;
                }
            }

            BashAstNode::FunctionDef { body, .. } => {
                // 旧源 L660-661（body 为 null 时 L582 提前返回）。
                if let Some(body) = body {
                    Self::collect_commands(body, commands, var_scope)?;
                }
            }

            BashAstNode::NegatedCommand { body, .. } => {
                if let Some(body) = body {
                    Self::collect_commands(body, commands, var_scope)?;
                }
            }

            BashAstNode::DeclarationCommand {
                argv,
                assignments,
                span,
                ..
            } => {
                // 旧源 L666-670：声明命令提取 argv。
                commands.push(SimpleCommandNode {
                    argv: argv.clone(),
                    env_vars: assignments.clone(),
                    redirects: Vec::new(),
                    span: span.clone(),
                });
            }

            BashAstNode::TestCommand { argv, span } => {
                // 旧源 L672-681：测试命令提取 argv，前缀补充命令名 `[` 或 `[[`。
                let cmd = if span.raw_text.starts_with("[[") {
                    "[["
                } else {
                    "["
                };
                let mut test_argv: Vec<String> = Vec::with_capacity(argv.len() + 1);
                test_argv.push(cmd.to_owned());
                test_argv.extend(argv.iter().cloned());
                commands.push(SimpleCommandNode {
                    argv: test_argv,
                    env_vars: Vec::new(),
                    redirects: Vec::new(),
                    span: span.clone(),
                });
            }

            BashAstNode::VariableAssignment { name, value, .. } => {
                // 旧源 L683-692：变量赋值追踪到 scope；PS4 / IFS → too-complex。
                if name == "PS4" || name == "IFS" {
                    return Err(TooComplexSignal {
                        reason: format!("Dangerous variable assignment: {name}"),
                        node_type: "variable_assignment".to_owned(),
                    });
                }
                var_scope.insert(name.clone(), value.clone());
            }

            BashAstNode::TooComplex { reason, .. } => {
                return Err(TooComplexSignal {
                    reason: reason.clone(),
                    node_type: "too-complex".to_owned(),
                });
            }
        }
        Ok(())
    }
}

// ──── 语义检查 (checkSemantics) ────

/// `read` 变量名全匹配正则——旧源 L427 `candidate.matches("[A-Za-z_][A-Za-z0-9_]*")`。
static PLAIN_VAR_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("static plain var name regex"));

impl BashSecurityAnalyzer {
    /// 对每个 `SimpleCommand` 执行后置语义验证——旧源 L706-793。
    ///
    /// 返回拒绝原因，或 `None`（通过）。
    #[allow(clippy::too_many_lines)]
    fn check_semantics(cmd: &SimpleCommandNode) -> Option<String> {
        if cmd.argv.is_empty() {
            return None;
        }

        // 1. 包装命令剥离
        let argv = Self::strip_wrappers(cmd.argv.clone());
        if argv.is_empty() {
            return None;
        }

        let argv0 = &argv[0];

        // 2. argv[0] 基本验证
        if argv0.is_empty() {
            return Some("Empty command name".to_owned());
        }
        if argv0.contains(VAR_PLACEHOLDER) || argv0.contains(CMDSUB_PLACEHOLDER) {
            return Some("Command name contains placeholder".to_owned());
        }
        if argv0.starts_with('-') || argv0.starts_with('|') || argv0.starts_with('&') {
            return Some(format!(
                "Command name starts with operator character: {argv0}"
            ));
        }

        // 3. Shell 关键字作为命令名 → 拒绝 (tree-sitter 误解析)
        if SHELL_KEYWORDS.contains(argv0.as_str()) {
            return Some(format!("Shell keyword as command name: {argv0}"));
        }

        // 4. Eval 类内置命令检查
        if EVAL_LIKE_BUILTINS.contains(argv0.as_str()) {
            return Some(format!("eval-like builtin: {argv0}"));
        }

        // 5. Zsh 危险内置命令
        if ZSH_DANGEROUS_BUILTINS.contains(argv0.as_str()) {
            return Some(format!("zsh dangerous builtin: {argv0}"));
        }

        // 6. 数组下标评估防护 (SUBSCRIPT_EVAL_FLAGS)
        if let Some(subscript_check) = Self::check_subscript_eval(&argv) {
            return Some(subscript_check);
        }

        // 7. [[ 算术比较防护: -eq/-ne/-lt 等两侧含 [ → 拒绝
        if let Some(arith_check) = Self::check_arithmetic_compare(&argv) {
            return Some(arith_check);
        }

        // 8. /proc/*/environ 访问检查 (泄露密钥)
        for arg in &argv {
            if arg.contains("/proc/") && arg.contains("/environ") {
                return Some("/proc/*/environ access detected".to_owned());
            }
        }

        // 9. jq system() 和 -f/-L/--from-file 检查
        if argv0 == "jq" {
            for arg in &argv {
                if arg.contains("system(") || arg.contains("system (") {
                    return Some("jq system() call detected".to_owned());
                }
            }
            for arg in &argv {
                if arg == "-f" || arg == "-L" || arg == "--from-file" {
                    return Some(format!("jq {arg} detected"));
                }
            }
        }

        // 10. argv 中 \n# 模式检查 (隐藏参数绕过路径验证)
        for arg in &argv {
            if NEWLINE_HASH_RE.is_match(arg) {
                return Some("Hidden argument after newline-hash pattern".to_owned());
            }
        }

        // 11. 翻译字符串检测: $"..." 是 locale-dependent 翻译，可能被滥用
        for arg in &argv {
            if arg.starts_with("$\"") || arg.contains("$\"") {
                return Some("translated_string detected in argument".to_owned());
            }
        }

        // 12. 大括号展开检测: {a,b,c} 模式 — shell 会展开为多个参数
        for arg in &argv {
            if arg.starts_with('{') && arg.ends_with('}') && arg.contains(',') {
                return Some("brace_expression detected in argument".to_owned());
            }
        }

        None
    }

    /// 参数级安全检查——分析 `rm`/`chmod` 等命令的参数组合（旧源 L804-840）。
    ///
    /// 旧源为实例方法但未读取任何字段，此处保留 `&self` 以对齐公共 API 形态。
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn check_arg_level_security(&self, command: &str, args: &[String]) -> SecurityLevel {
        if args.is_empty() {
            return SecurityLevel::Safe;
        }
        let cmd = &args[0];

        // rm 命令: -rf + 危险路径 → DENY（旧源 L809-816；注意 argv[0] 自身参与
        // `contains` 判定，`"rm"` 含 `r` 使 `hasRecursive` 恒真，此处逐字保留）
        if cmd == "rm" {
            let has_force = args.iter().any(|a| a.contains('f'));
            let has_recursive = args.iter().any(|a| a.contains('r') || a.contains('R'));
            let targets_dangerous_path = args
                .iter()
                .any(|a| a == "/" || a == "~" || a.starts_with("$HOME"));
            if (has_force || has_recursive) && targets_dangerous_path {
                return SecurityLevel::Deny;
            }
        }

        // chmod 命令: 777 -R / → DENY（旧源 L818-825）
        if cmd == "chmod" {
            let has777 = args.iter().any(|a| a == "777");
            let has_recursive = args.iter().any(|a| a == "-R");
            let targets_root = args.iter().any(|a| a == "/");
            if has777 && has_recursive && targets_root {
                return SecurityLevel::Deny;
            }
        }

        // 危险子命令检查（旧源 L827-837）
        for (key, dangerous_list) in DANGEROUS_SUBCOMMANDS {
            if cmd == key {
                // 旧源 L830：`command.substring(command.indexOf(cmd) + cmd.length()).trim()`；
                // `indexOf` 未命中时旧源为 `cmd.length() - 1`，此处等价复现。
                let start = command
                    .find(cmd.as_str())
                    .map_or_else(|| cmd.len().saturating_sub(1), |pos| pos + cmd.len());
                let rest = java_trim(command.get(start..).unwrap_or(""));
                for dangerous in *dangerous_list {
                    if rest.starts_with(dangerous) {
                        return SecurityLevel::Ask;
                    }
                }
            }
        }

        SecurityLevel::Safe
    }

    /// `[[` 算术比较防护——旧源 L847-861。
    ///
    /// `-eq`/`-ne`/`-lt`/`-gt`/`-le`/`-ge` 两侧操作数含 `[` → 拒绝；
    /// bash 在算术比较中执行 `$(...)` 嵌套在下标中的命令。
    fn check_arithmetic_compare(argv: &[String]) -> Option<String> {
        for i in 0..argv.len() {
            if ARITHMETIC_COMPARE_OPS.contains(argv[i].as_str()) {
                // 检查左操作数
                if i > 0 && SUBSCRIPT_BRACKET_RE.is_match(&argv[i - 1]) {
                    return Some(format!(
                        "Arithmetic comparison with array subscript on left: {}",
                        argv[i - 1]
                    ));
                }
                // 检查右操作数
                if i + 1 < argv.len() && SUBSCRIPT_BRACKET_RE.is_match(&argv[i + 1]) {
                    return Some(format!(
                        "Arithmetic comparison with array subscript on right: {}",
                        argv[i + 1]
                    ));
                }
            }
        }
        None
    }

    /// 递归剥离包装命令——旧源 L879-956。
    ///
    /// 包装命令：`command [-pvV]` / `builtin [-p]` / `sudo [-niuU...]` / `nohup` /
    /// `xargs -I` / `nice [-n N]` / `env [VAR=val] [-i]`（拒绝 `-S`/`-C`/`-P`）/
    /// `stdbuf [-oOeE MODE]` / `timeout [flags] duration`。
    fn strip_wrappers(argv: Vec<String>) -> Vec<String> {
        if argv.is_empty() {
            return argv;
        }

        let cmd = argv[0].clone();

        // command -v/-V 仅查询不执行 → 保留（旧源 L885-890）
        if cmd == "command" && argv.len() > 1 && (argv[1] == "-v" || argv[1] == "-V") {
            return argv;
        }

        // builtin -p 查询不执行 → 保留（旧源 L893-897）
        if cmd == "builtin" && argv.len() > 1 && argv[1] == "-p" {
            return argv;
        }

        if !WRAPPER_COMMANDS.contains(cmd.as_str()) {
            return argv;
        }

        // 特殊处理: env -S/-C/-P → 拒绝 (允许注入)（旧源 L903-911）
        if cmd == "env" {
            for arg in &argv[1..] {
                if arg == "-S" || arg == "-C" || arg == "-P" {
                    return argv; // 不剥离, 让 eval-like 检查处理
                }
            }
        }

        // 特殊处理: timeout 的 duration 参数不能含 $()（旧源 L913-921）
        if cmd == "timeout" && argv.len() > 1 {
            for arg in &argv[1..] {
                if !arg.starts_with('-') && (arg.contains("$(") || arg.contains('`')) {
                    return argv; // duration 含命令替换 → 不剥离
                }
            }
        }

        // 剥离第一层: 跳过标志和 VAR=val, 找到实际命令（旧源 L923-950）
        let mut remaining: Vec<String> = Vec::new();
        let mut found_cmd = false;
        let mut i = 1;
        while i < argv.len() {
            let arg = &argv[i];
            if !found_cmd && arg.starts_with('-') {
                // nice -n N: 跳过 -n 和下一个参数
                if cmd == "nice" && arg == "-n" && i + 1 < argv.len() {
                    i += 1; // 跳过 N
                }
                i += 1;
                continue;
            }
            if !found_cmd && arg.contains('=') {
                i += 1;
                continue; // 跳过 env VAR=val
            }
            // timeout: 第一个非标志参数是 duration, 跳过它
            if !found_cmd && cmd == "timeout" {
                // 旧源 L940 的 `foundCmd = false;` 在此分支中恒为无效赋值，故省略。
                remaining.clear();
                remaining.extend(argv[i + 1..].iter().cloned());
                break;
            }
            found_cmd = true;
            remaining.push(arg.clone());
            i += 1;
        }

        if remaining.is_empty() {
            return argv;
        }

        // 递归剥离下一层
        Self::strip_wrappers(remaining)
    }

    /// 数组下标评估防护——旧源 L962-1013。
    ///
    /// `printf -v` / `read -a` / `declare -n` 等的 NAME 参数含 `[` 则拒绝。
    fn check_subscript_eval(argv: &[String]) -> Option<String> {
        if argv.is_empty() {
            return None;
        }
        let cmd = &argv[0];

        // printf -v NAME（旧源 L967-976：注意上界为 `argv.size() - 1`）
        if cmd == "printf" {
            for i in 1..argv.len().saturating_sub(1) {
                if argv[i] == "-v" {
                    let name = if i + 1 < argv.len() { &argv[i + 1] } else { "" };
                    if SUBSCRIPT_BRACKET_RE.is_match(name) {
                        return Some(format!("printf -v with array subscript: {name}"));
                    }
                }
            }
        }

        // read -a NAME（旧源 L979-988）
        if cmd == "read" {
            for i in 1..argv.len() {
                if argv[i] == "-a" && i + 1 < argv.len() {
                    let name = &argv[i + 1];
                    if SUBSCRIPT_BRACKET_RE.is_match(name) {
                        return Some(format!("read -a with array subscript: {name}"));
                    }
                }
            }
        }

        // declare -n NAME（旧源 L991-1000）
        if cmd == "declare" || cmd == "typeset" {
            for i in 1..argv.len() {
                if argv[i] == "-n" && i + 1 < argv.len() {
                    let name = &argv[i + 1];
                    if SUBSCRIPT_BRACKET_RE.is_match(name) {
                        return Some(format!("declare -n with array subscript: {name}"));
                    }
                }
            }
        }

        // unset / read 裸名称含 [（旧源 L1003-1010）
        if cmd == "unset" || cmd == "read" {
            for arg in &argv[1..] {
                if !arg.starts_with('-') && SUBSCRIPT_BRACKET_RE.is_match(arg) {
                    return Some(format!("{cmd} with array subscript: {arg}"));
                }
            }
        }

        None
    }
}

// ──── 环境变量引用分析 ────

impl BashSecurityAnalyzer {
    /// 使用权限解析器共享的受限 Bash AST 分析继承环境变量引用——旧源 L283-305。
    ///
    /// 对不支持或动态 Shell 语义安全失败闭合：要求用户确认，绝不隐式放行。
    ///
    /// 旧源为实例方法（经 `this.parser` 解析），移植后解析器为自由函数，
    /// 保留 `&self` 以对齐公共 API 形态。
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn analyze_environment_references(
        &self,
        command: Option<&str>,
    ) -> EnvironmentReferenceAnalysis {
        // 旧源 L284-287
        let Some(command) = command else {
            return Self::environment_result(
                &EnvironmentAccumulator::default(),
                EnvironmentParseStatus::Success,
                None,
            );
        };
        if java_is_blank(command) {
            return Self::environment_result(
                &EnvironmentAccumulator::default(),
                EnvironmentParseStatus::Success,
                None,
            );
        }
        // 旧源 L288-292
        if command.encode_utf16().count() > MAX_COMMAND_LENGTH {
            return Self::environment_result(
                &EnvironmentAccumulator::default(),
                EnvironmentParseStatus::Unavailable,
                Some("command exceeds parser limit"),
            );
        }
        // 旧源 L293-298
        let Some(root) = parse(command) else {
            return Self::environment_result(
                &EnvironmentAccumulator::default(),
                EnvironmentParseStatus::Unavailable,
                Some("Bash parser unavailable"),
            );
        };
        // 旧源 L299-304
        let mut accumulator = EnvironmentAccumulator::default();
        let mut scope: BTreeSet<String> = BTreeSet::new();
        let status = Self::analyse_environment_program(&root, &mut scope, &mut accumulator);
        let reason = if status == EnvironmentParseStatus::Success {
            None
        } else {
            Some("dynamic or unsupported shell environment semantics")
        };
        Self::environment_result(&accumulator, status, reason)
    }

    /// 旧源 L307-309。
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn is_allowed_inherited_environment_reference(&self, variable: Option<&str>) -> bool {
        variable.is_some_and(|variable| SAFE_ENV_VARS.contains(variable))
    }

    /// `ProgramNode` 分支——旧源 L315-322。
    fn analyse_environment_program(
        program: &ProgramNode,
        scope: &mut BTreeSet<String>,
        accumulator: &mut EnvironmentAccumulator,
    ) -> EnvironmentParseStatus {
        let mut status = EnvironmentParseStatus::Success;
        for statement in &program.statements {
            // 旧源 L323：`StatementNode` → 递归 body。
            status = Self::merge_environment_status(
                status,
                Self::analyse_environment_node(&statement.body, scope, accumulator),
            );
        }
        status
    }

    /// 受限 AST 环境变量遍历——旧源 L311-401。
    #[allow(clippy::too_many_lines)]
    fn analyse_environment_node(
        node: &BashAstNode,
        scope: &mut BTreeSet<String>,
        accumulator: &mut EnvironmentAccumulator,
    ) -> EnvironmentParseStatus {
        match node {
            BashAstNode::Program(program) => {
                Self::analyse_environment_program(program, scope, accumulator)
            }
            BashAstNode::Statement(statement) => {
                Self::analyse_environment_node(&statement.body, scope, accumulator)
            }
            BashAstNode::Pipeline {
                commands: stages, ..
            } => {
                // 旧源 L324-331：每个管道阶段用 scope 副本。
                let mut status = EnvironmentParseStatus::Success;
                for command in stages {
                    let mut stage_scope = scope.clone();
                    status = Self::merge_environment_status(
                        status,
                        Self::analyse_environment_node(command, &mut stage_scope, accumulator),
                    );
                }
                status
            }
            BashAstNode::AndOr { left, right, .. } => {
                // 旧源 L332-334：Java 实参从左到右求值，右侧副本在左侧分析完成后拍摄。
                let left_status = Self::analyse_environment_node(left, scope, accumulator);
                let mut right_scope = scope.clone();
                let right_status =
                    Self::analyse_environment_node(right, &mut right_scope, accumulator);
                Self::merge_environment_status(left_status, right_status)
            }
            BashAstNode::RedirectedStatement {
                body, redirects, ..
            } => {
                // 旧源 L335-342
                let mut status = Self::analyse_environment_node(body, scope, accumulator);
                for redirect in redirects {
                    status = Self::merge_environment_status(
                        status,
                        Self::scan_environment_word(&redirect.target, scope, accumulator),
                    );
                }
                status
            }
            BashAstNode::Subshell { body, .. } => {
                // 旧源 L343-344
                let mut sub_scope = scope.clone();
                Self::analyse_environment_program(body, &mut sub_scope, accumulator)
            }
            BashAstNode::BraceGroup { body, .. } => {
                Self::analyse_environment_program(body, scope, accumulator)
            }
            BashAstNode::If {
                condition,
                then_body,
                else_body,
                ..
            } => {
                // 旧源 L346-353
                let mut status = Self::analyse_environment_program(condition, scope, accumulator);
                let mut then_scope = scope.clone();
                status = Self::merge_environment_status(
                    status,
                    Self::analyse_environment_program(then_body, &mut then_scope, accumulator),
                );
                // 旧源 L350-351：`elseBody()` 为 null 时 L313 提前返回 SUCCESS。
                if let Some(else_body) = else_body {
                    let mut else_scope = scope.clone();
                    status = Self::merge_environment_status(
                        status,
                        Self::analyse_environment_program(else_body, &mut else_scope, accumulator),
                    );
                }
                status
            }
            BashAstNode::For {
                var_name,
                words,
                body,
                ..
            } => {
                // 旧源 L354-364
                let mut status = EnvironmentParseStatus::Success;
                for word in words {
                    status = Self::merge_environment_status(
                        status,
                        Self::scan_environment_word(word, scope, accumulator),
                    );
                }
                let mut loop_scope = scope.clone();
                Self::define_local(var_name, &mut loop_scope, accumulator);
                Self::merge_environment_status(
                    status,
                    Self::analyse_environment_program(body, &mut loop_scope, accumulator),
                )
            }
            BashAstNode::While {
                condition, body, ..
            } => {
                // 旧源 L365-367
                let condition_status =
                    Self::analyse_environment_program(condition, scope, accumulator);
                let mut body_scope = scope.clone();
                let body_status =
                    Self::analyse_environment_program(body, &mut body_scope, accumulator);
                Self::merge_environment_status(condition_status, body_status)
            }
            BashAstNode::Case { word, items, .. } => {
                // 旧源 L368-375
                let mut status = Self::scan_environment_word(word, scope, accumulator);
                for item in items {
                    let mut item_scope = scope.clone();
                    status = Self::merge_environment_status(
                        status,
                        Self::analyse_environment_program(&item.body, &mut item_scope, accumulator),
                    );
                }
                status
            }
            BashAstNode::FunctionDef { body, .. } => {
                // 旧源 L376-377（body 为 null 时 L313 提前返回 SUCCESS）
                body.as_ref()
                    .map_or(EnvironmentParseStatus::Success, |body| {
                        let mut function_scope = scope.clone();
                        Self::analyse_environment_node(body, &mut function_scope, accumulator)
                    })
            }
            BashAstNode::NegatedCommand { body, .. } => body
                .as_ref()
                .map_or(EnvironmentParseStatus::Success, |body| {
                    Self::analyse_environment_node(body, scope, accumulator)
                }),
            BashAstNode::DeclarationCommand {
                argv, assignments, ..
            } => {
                // 旧源 L379-391
                let mut status = EnvironmentParseStatus::Success;
                for assignment in assignments {
                    status = Self::merge_environment_status(
                        status,
                        Self::scan_environment_word(&assignment.value, scope, accumulator),
                    );
                    Self::define_local(&assignment.name, scope, accumulator);
                }
                for argument in argv {
                    status = Self::merge_environment_status(
                        status,
                        Self::scan_environment_word(argument, scope, accumulator),
                    );
                }
                status
            }
            BashAstNode::SimpleCommand(simple) => {
                Self::analyse_simple_command_environment(simple, scope, accumulator)
            }
            BashAstNode::TestCommand { argv, .. } => {
                Self::scan_environment_words(argv, scope, accumulator)
            }
            BashAstNode::VariableAssignment { name, value, .. } => {
                // 旧源 L394-398
                let status = Self::scan_environment_word(value, scope, accumulator);
                Self::define_local(name, scope, accumulator);
                status
            }
            BashAstNode::TooComplex { .. } => EnvironmentParseStatus::TooComplex,
        }
    }

    /// 旧源 L403-434。
    fn analyse_simple_command_environment(
        command: &SimpleCommandNode,
        scope: &mut BTreeSet<String>,
        accumulator: &mut EnvironmentAccumulator,
    ) -> EnvironmentParseStatus {
        let mut command_scope = scope.clone();
        let mut status = EnvironmentParseStatus::Success;
        for assignment in &command.env_vars {
            status = Self::merge_environment_status(
                status,
                Self::scan_environment_word(&assignment.value, scope, accumulator),
            );
            Self::define_local(&assignment.name, &mut command_scope, accumulator);
            if command.argv.is_empty() {
                Self::define_local(&assignment.name, scope, accumulator);
            }
        }
        status = Self::merge_environment_status(
            status,
            Self::scan_environment_words(&command.argv, &command_scope, accumulator),
        );
        for redirect in &command.redirects {
            status = Self::merge_environment_status(
                status,
                Self::scan_environment_word(&redirect.target, &command_scope, accumulator),
            );
        }
        if !command.argv.is_empty() {
            let executable = Self::strip_shell_quotes(&command.argv[0]);
            if EVAL_LIKE_BUILTINS.contains(executable.as_str()) {
                return EnvironmentParseStatus::TooComplex;
            }
            if executable == "read" {
                for argument in &command.argv[1..] {
                    let candidate = Self::strip_shell_quotes(argument);
                    if PLAIN_VAR_NAME.is_match(&candidate) {
                        Self::define_local(&candidate, scope, accumulator);
                    }
                }
            }
        }
        status
    }

    /// 旧源 L436-444。
    fn scan_environment_words(
        words: &[String],
        scope: &BTreeSet<String>,
        accumulator: &mut EnvironmentAccumulator,
    ) -> EnvironmentParseStatus {
        let mut status = EnvironmentParseStatus::Success;
        for word in words {
            status = Self::merge_environment_status(
                status,
                Self::scan_environment_word(word, scope, accumulator),
            );
        }
        status
    }

    /// 旧源 L446-469。
    fn scan_environment_word(
        word: &str,
        scope: &BTreeSet<String>,
        accumulator: &mut EnvironmentAccumulator,
    ) -> EnvironmentParseStatus {
        if word.is_empty() {
            return EnvironmentParseStatus::Success;
        }
        if word.contains("${!") || word.contains("$(") || word.contains('`') {
            return EnvironmentParseStatus::TooComplex;
        }
        let expandable = Self::remove_single_quoted_segments(word);
        for captures in SHELL_VARIABLE_REFERENCE.captures_iter(&expandable) {
            let variable = captures.get(1).map_or("", |m| m.as_str());
            // 旧源 L458-461：纯数字或特殊变量 → 跳过。
            if !variable.is_empty() && variable.chars().all(|c| c.is_ascii_digit())
                || SPECIAL_VAR_NAMES.contains(variable)
            {
                continue;
            }
            if scope.contains(variable) {
                continue;
            }
            accumulator.inherited_references.insert(variable.to_owned());
            if SENSITIVE_ENV_NAME.is_match(variable) {
                accumulator
                    .sensitive_inherited_references
                    .insert(variable.to_owned());
            }
        }
        EnvironmentParseStatus::Success
    }

    /// 旧源 L471-483。
    fn remove_single_quoted_segments(value: &str) -> String {
        let units: Vec<char> = value.chars().collect();
        let mut result = String::with_capacity(value.len());
        let mut single_quoted = false;
        for (index, &ch) in units.iter().enumerate() {
            if ch == '\'' && (index == 0 || units[index - 1] != '\\') {
                single_quoted = !single_quoted;
                continue;
            }
            if !single_quoted {
                result.push(ch);
            }
        }
        result
    }

    /// 旧源 L485-488。
    fn strip_shell_quotes(value: &str) -> String {
        value.replace(['"', '\''], "")
    }

    /// 旧源 L490-495。
    fn define_local(
        variable: &str,
        scope: &mut BTreeSet<String>,
        accumulator: &mut EnvironmentAccumulator,
    ) {
        if java_is_blank(variable) {
            return;
        }
        scope.insert(variable.to_owned());
        accumulator.local_definitions.insert(variable.to_owned());
    }

    /// 旧源 L497-509。
    fn merge_environment_status(
        left: EnvironmentParseStatus,
        right: EnvironmentParseStatus,
    ) -> EnvironmentParseStatus {
        if left == EnvironmentParseStatus::Unavailable
            || right == EnvironmentParseStatus::Unavailable
        {
            return EnvironmentParseStatus::Unavailable;
        }
        if left == EnvironmentParseStatus::TooComplex || right == EnvironmentParseStatus::TooComplex
        {
            return EnvironmentParseStatus::TooComplex;
        }
        EnvironmentParseStatus::Success
    }

    /// 旧源 L511-518。
    fn environment_result(
        accumulator: &EnvironmentAccumulator,
        status: EnvironmentParseStatus,
        reason: Option<&str>,
    ) -> EnvironmentReferenceAnalysis {
        EnvironmentReferenceAnalysis {
            local_definitions: accumulator.local_definitions.clone(),
            inherited_references: accumulator.inherited_references.clone(),
            sensitive_inherited_references: accumulator.sensitive_inherited_references.clone(),
            parse_status: status,
            reason: reason.map(str::to_owned),
        }
    }
}
