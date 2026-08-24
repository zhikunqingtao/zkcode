//! `BashParser` 50 条黄金测试 —— 精选语料库。
//!
//! 逐条翻译旧源 `backend/src/test/java/com/aicodeassistant/tool/bash/
//! BashParserGoldenTest.java`（546 行 / 50 个 `@Test`），断言零改写。
//!
//! 旧源类注释 L17-26：覆盖 12 个核心语法类别——简单命令(5)/管道(4)/重定向(5)/
//! 变量展开(5)/命令替换(4)/引号转义(5)/控制流(6)/复合(4)/函数(2)/Glob(3)/
//! 声明(3)/安全边界(4)。安全结果验证：simple → `parseForSecurity` 返回 Simple，
//! too-complex → 返回 `TooComplex`，parse-unavailable → 返回 `ParseUnavailable`。

use zk_tools::bash::ast::{BashAstNode, ParseForSecurityResult, ProgramNode, SimpleCommandNode};
use zk_tools::bash::parser::parse;
use zk_tools::bash::security::BashSecurityAnalyzer;

/// 旧源 L32-41 `setUp()`：`BashParser` + `BashSecurityAnalyzer(PathValidator,
/// AppStateStore(workingDirectory=user.dir), CommandBlacklistService(null, ...))`。
fn analyzer() -> BashSecurityAnalyzer {
    BashSecurityAnalyzer::new()
}

/// 旧源 L47-53 `assertSimple` —— 断言解析成功且安全结果为 simple。
fn assert_simple(cmd: &str) -> Vec<SimpleCommandNode> {
    let result = analyzer().parse_for_security(Some(cmd));
    match result {
        ParseForSecurityResult::Simple { commands } => commands,
        other => panic!("Expected Simple for: {cmd}, got: {other:?}"),
    }
}

/// 旧源 L55-61 `assertTooComplex` —— 断言安全结果为 too-complex。
fn assert_too_complex(cmd: &str) -> (String, String) {
    let result = analyzer().parse_for_security(Some(cmd));
    match result {
        ParseForSecurityResult::TooComplex { reason, node_type } => (reason, node_type),
        other => panic!("Expected TooComplex for: {cmd}, got: {other:?}"),
    }
}

/// 旧源 L63-69 `assertParses` —— 断言解析成功（返回 `ProgramNode`）。
///
/// 旧源声明该私有辅助后未在任何 `@Test` 中调用；原样保留以对齐可审计性。
#[allow(dead_code)]
fn assert_parses(cmd: &str) -> ProgramNode {
    let node = parse(cmd).unwrap_or_else(|| panic!("Parse returned null for: {cmd}"));
    assert!(!node.statements.is_empty(), "Empty program for: {cmd}");
    node
}

/// 旧源 L71-74 `firstBody` —— 获取程序第一条语句的 body。
///
/// 旧源声明该私有辅助后未在任何 `@Test` 中调用；原样保留以对齐可审计性。
#[allow(dead_code)]
fn first_body(prog: &ProgramNode) -> &BashAstNode {
    &prog.statements.first().unwrap().body
}

// ═══════════════════════════════════════════════════════════════════
// 1. 简单命令 (5) —— 旧源 L76-124
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L84-90 `[1-01] ls → simple`。
#[test]
fn test_1_01() {
    let commands = assert_simple("ls");
    assert_eq!(1, commands.len());
    assert_eq!("ls", commands.first().unwrap().argv.first().unwrap());
}

/// 旧源 L92-100 `[1-02] echo hello world → simple`。
#[test]
fn test_1_02() {
    let commands = assert_simple("echo hello world");
    assert_eq!(1, commands.len());
    let cmd = commands.first().unwrap();
    assert_eq!("echo", cmd.argv.first().unwrap());
    assert!(cmd.argv.len() >= 2);
}

/// 旧源 L102-108 `[1-03] git commit -m 'fix bug' → simple`。
#[test]
fn test_1_03() {
    let commands = assert_simple("git commit -m 'fix bug'");
    assert_eq!(1, commands.len());
    assert_eq!("git", commands.first().unwrap().argv.first().unwrap());
}

/// 旧源 L110-115 `[1-04] ENV=value command arg → simple`。
#[test]
fn test_1_04() {
    let commands = assert_simple("ENV=value command arg");
    assert!(!commands.is_empty());
}

/// 旧源 L117-123 `[1-05] A=1 B=2 → simple`（纯赋值，commands 可能为空）。
#[test]
fn test_1_05() {
    let commands = assert_simple("A=1 B=2");
    // 旧源 assertNotNull(result)：等价于"能得到 Simple 结果"。
    let _ = commands;
}

// ═══════════════════════════════════════════════════════════════════
// 2. 管道与序列 (4) —— 旧源 L126-161
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L134-139 `[2-01] cat file | grep pattern → simple`（单管道）。
#[test]
fn test_2_01() {
    let commands = assert_simple("cat file | grep pattern");
    assert!(commands.len() >= 2);
}

/// 旧源 L141-146 `[2-02] ps aux | grep java | head -5 → simple`（多级管道）。
#[test]
fn test_2_02() {
    let commands = assert_simple("ps aux | grep java | head -5");
    assert!(commands.len() >= 3);
}

/// 旧源 L148-153 `[2-03] make && make install → simple`（逻辑与）。
#[test]
fn test_2_03() {
    let commands = assert_simple("make && make install");
    assert!(commands.len() >= 2);
}

/// 旧源 L155-160 `[2-04] cmd1 || cmd2 && cmd3 → simple`（混合逻辑）。
#[test]
fn test_2_04() {
    let commands = assert_simple("cmd1 || cmd2 && cmd3");
    assert!(commands.len() >= 2);
}

// ═══════════════════════════════════════════════════════════════════
// 3. 重定向 (5) —— 旧源 L163-206
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L171-177 `[3-01] echo hello > output.txt → simple`。
#[test]
fn test_3_01() {
    let commands = assert_simple("echo hello > output.txt");
    assert_eq!(1, commands.len());
    assert_eq!("echo", commands.first().unwrap().argv.first().unwrap());
}

/// 旧源 L179-184 `[3-02] sort < input.txt >> result.txt → simple`。
#[test]
fn test_3_02() {
    let commands = assert_simple("sort < input.txt >> result.txt");
    assert_eq!(1, commands.len());
}

/// 旧源 L186-191 `[3-03] cmd 2>&1 → simple`。
#[test]
fn test_3_03() {
    let commands = assert_simple("cmd 2>&1");
    assert_eq!(1, commands.len());
}

/// 旧源 L193-198 `[3-04] cmd &> /dev/null → simple`。
#[test]
fn test_3_04() {
    let commands = assert_simple("cmd &> /dev/null");
    assert!(!commands.is_empty());
}

/// 旧源 L200-205 `[3-05] cat <<'EOF'\nhello world\nEOF → simple`（heredoc）。
#[test]
fn test_3_05() {
    let commands = assert_simple("cat <<'EOF'\nhello world\nEOF");
    assert!(!commands.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// 4. 变量展开 (5) —— 旧源 L208-250
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L216-221 `[4-01] echo $HOME → simple`。
#[test]
fn test_4_01() {
    let commands = assert_simple("echo $HOME");
    assert_eq!(1, commands.len());
}

/// 旧源 L223-228 `[4-02] echo ${VAR:-default} → simple`。
#[test]
fn test_4_02() {
    let commands = assert_simple("echo ${VAR:-default}");
    assert_eq!(1, commands.len());
}

/// 旧源 L230-235 `[4-03] echo ${#array[@]} → simple`。
#[test]
fn test_4_03() {
    let commands = assert_simple("echo ${#array[@]}");
    assert_eq!(1, commands.len());
}

/// 旧源 L237-242 `[4-04] echo $? → simple`。
#[test]
fn test_4_04() {
    let commands = assert_simple("echo $?");
    assert_eq!(1, commands.len());
}

/// 旧源 L244-249 `[4-05] echo ${PATH//:/\n} → simple`。
#[test]
fn test_4_05() {
    let commands = assert_simple("echo ${PATH//:/\\n}");
    assert_eq!(1, commands.len());
}

// ═══════════════════════════════════════════════════════════════════
// 5. 命令替换 (4) —— 旧源 L252-286
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L260-265 `[5-01] echo $(date +%Y) → simple`。
#[test]
fn test_5_01() {
    let commands = assert_simple("echo $(date +%Y)");
    assert!(!commands.is_empty());
}

/// 旧源 L267-272 `[5-02] echo `uname` → simple`。
#[test]
fn test_5_02() {
    let commands = assert_simple("echo `uname`");
    assert!(!commands.is_empty());
}

/// 旧源 L274-279 `[5-03] dir=$(pwd) → simple`。
#[test]
fn test_5_03() {
    let commands = assert_simple("dir=$(pwd)");
    // 旧源 assertNotNull(result)。
    let _ = commands;
}

/// 旧源 L281-285 `[5-04] echo $((1 + 2)) → too-complex`（算术展开）。
#[test]
fn test_5_04() {
    assert_too_complex("echo $((1 + 2))");
}

// ═══════════════════════════════════════════════════════════════════
// 6. 引号与转义 (5) —— 旧源 L288-330
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L296-301 `[6-01] echo 'single quoted' → simple`。
#[test]
fn test_6_01() {
    let commands = assert_simple("echo 'single quoted'");
    assert_eq!(1, commands.len());
}

/// 旧源 L303-308 `[6-02] echo "hello $USER" → simple`。
#[test]
fn test_6_02() {
    let commands = assert_simple("echo \"hello $USER\"");
    assert_eq!(1, commands.len());
}

/// 旧源 L310-315 `[6-03] echo $'\t\n\\' → simple`（ANSI-C）。
#[test]
fn test_6_03() {
    let commands = assert_simple("echo $'\\t\\n\\\\'");
    assert_eq!(1, commands.len());
}

/// 旧源 L317-322 `[6-04] echo hello\ world → simple`（反斜杠转义）。
#[test]
fn test_6_04() {
    let commands = assert_simple("echo hello\\ world");
    assert_eq!(1, commands.len());
}

/// 旧源 L324-329 `[6-05] echo "pre"'mid'"$suf" → simple`（拼接）。
#[test]
fn test_6_05() {
    let commands = assert_simple("echo \"pre\"'mid'\"$suf\"");
    assert_eq!(1, commands.len());
}

// ═══════════════════════════════════════════════════════════════════
// 7. 控制流 (6) —— 旧源 L332-380
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L340-345 `[7-01] if [ -f file ]; then echo yes; fi → simple`。
#[test]
fn test_7_01() {
    let commands = assert_simple("if [ -f file ]; then echo yes; fi");
    assert!(!commands.is_empty());
}

/// 旧源 L347-352 `[7-02] if-elif-else 完整链 → simple`。
#[test]
fn test_7_02() {
    let commands = assert_simple("if cmd; then a; elif cmd2; then b; else c; fi");
    assert!(!commands.is_empty());
}

/// 旧源 L354-359 `[7-03] for f in *.txt; do echo "$f"; done → simple`。
#[test]
fn test_7_03() {
    let commands = assert_simple("for f in *.txt; do echo \"$f\"; done");
    assert!(!commands.is_empty());
}

/// 旧源 L361-365 `[7-04] for ((i=0; i<10; i++)); do echo $i; done → too-complex`。
#[test]
fn test_7_04() {
    assert_too_complex("for ((i=0; i<10; i++)); do echo $i; done");
}

/// 旧源 L367-372 `[7-05] while read -r line; do echo "$line"; done → simple`。
#[test]
fn test_7_05() {
    let commands = assert_simple("while read -r line; do echo \"$line\"; done");
    assert!(!commands.is_empty());
}

/// 旧源 L374-379 `[7-06] case "$1" in start) run;; stop) halt;; *) usage;; esac → simple`。
#[test]
fn test_7_06() {
    let commands = assert_simple("case \"$1\" in start) run;; stop) halt;; *) usage;; esac");
    assert!(!commands.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// 8. 复合结构 (4) —— 旧源 L382-417
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L390-395 `[8-01] (cd /tmp && ls) → simple`（子 shell）。
#[test]
fn test_8_01() {
    let commands = assert_simple("(cd /tmp && ls)");
    assert!(!commands.is_empty());
}

/// 旧源 L397-402 `[8-02] { echo a; echo b; } → simple`（大括号分组）。
#[test]
fn test_8_02() {
    let commands = assert_simple("{ echo a; echo b; }");
    assert!(!commands.is_empty());
}

/// 旧源 L404-409 `[8-03] [[ -n "$var" && -f "$file" ]] → simple`（测试命令）。
#[test]
fn test_8_03() {
    let commands = assert_simple("[[ -n \"$var\" && -f \"$file\" ]]");
    assert!(!commands.is_empty());
}

/// 旧源 L411-416 `[8-04] ! grep -q pattern file → simple`（否定）。
#[test]
fn test_8_04() {
    let commands = assert_simple("! grep -q pattern file");
    assert!(!commands.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// 9. 函数定义 (2) —— 旧源 L419-440
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L427-432 `[9-01] greet() { echo "hello $1"; } → simple`。
#[test]
fn test_9_01() {
    let commands = assert_simple("greet() { echo \"hello $1\"; }");
    assert!(!commands.is_empty());
}

/// 旧源 L434-439 `[9-02] log() { echo "$@"; } 2>/dev/null → simple`。
#[test]
fn test_9_02() {
    let commands = assert_simple("log() { echo \"$@\"; } 2>/dev/null");
    assert!(!commands.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// 10. Glob 与大括号展开 (3) —— 旧源 L442-470
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L450-456 `[10-01] ls *.txt → simple`。
#[test]
fn test_10_01() {
    let commands = assert_simple("ls *.txt");
    assert_eq!(1, commands.len());
    assert_eq!("ls", commands.first().unwrap().argv.first().unwrap());
}

/// 旧源 L458-463 `[10-02] echo file[0-9].log → simple`。
#[test]
fn test_10_02() {
    let commands = assert_simple("echo file[0-9].log");
    assert_eq!(1, commands.len());
}

/// 旧源 L465-469 `[10-03] echo {a,b,c} → too-complex`（大括号展开）。
#[test]
fn test_10_03() {
    assert_too_complex("echo {a,b,c}");
}

// ═══════════════════════════════════════════════════════════════════
// 11. 声明命令 (3) —— 旧源 L472-500
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L480-485 `[11-01] export PATH="/usr/bin:$PATH" → simple`。
#[test]
fn test_11_01() {
    let commands = assert_simple("export PATH=\"/usr/bin:$PATH\"");
    assert!(!commands.is_empty());
}

/// 旧源 L487-492 `[11-02] declare -a arr=(1 2 3) → simple`。
#[test]
fn test_11_02() {
    let commands = assert_simple("declare -a arr=(1 2 3)");
    assert!(!commands.is_empty());
}

/// 旧源 L494-499 `[11-03] local var="value" → simple`。
#[test]
fn test_11_03() {
    let commands = assert_simple("local var=\"value\"");
    assert!(!commands.is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// 12. 安全边界 (4) —— 旧源 L502-544
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L510-514 `[12-01] echo <(cat /etc/passwd) → too-complex`（进程替换）。
#[test]
fn test_12_01() {
    assert_too_complex("echo <(cat /etc/passwd)");
}

/// 旧源 L516-520 `[12-02] echo $"hello" → too-complex`（翻译字符串）。
#[test]
fn test_12_02() {
    assert_too_complex("echo $\"hello\"");
}

/// 旧源 L522-532 `[12-03] trap 'rm -rf /' EXIT → TooComplex`。
///
/// 旧源注释：trap 是 `EVAL_LIKE_BUILTINS`，安全分析在 `checkSemantics` 层拦截，
/// `parseForSecurity` → `checkSemantics` → "eval-like builtin: trap"。
#[test]
fn test_12_03() {
    let result = analyzer().parse_for_security(Some("trap 'rm -rf /' EXIT"));
    assert!(
        matches!(result, ParseForSecurityResult::TooComplex { .. }),
        "trap should be caught by EVAL_LIKE_BUILTINS check"
    );
}

/// 旧源 L534-543 `[12-04] 超长深嵌套 → parse-unavailable`（> 10000 字符）。
#[test]
fn test_12_04() {
    let long_cmd = format!("echo {}", "a".repeat(10001));
    let result = analyzer().parse_for_security(Some(&long_cmd));
    assert_eq!(
        ParseForSecurityResult::ParseUnavailable,
        result,
        "Command > 10000 chars should return ParseUnavailable"
    );
}
