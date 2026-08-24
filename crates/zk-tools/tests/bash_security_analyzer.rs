//! `BashSecurityAnalyzer` 8 层安全检查完整性测试。
//!
//! 逐条翻译旧源 `backend/src/test/java/com/aicodeassistant/tool/bash/
//! BashSecurityAnalyzerTest.java`（540 行 / 64 个 `@Test`），断言零改写。
//!
//! 旧源类注释 L16-21：覆盖预检查链、AST 遍历、语义检查、路径验证、Heredoc 安全、
//! 包装命令剥离、参数级安全、与原版 28 项检查对照。

use zk_tools::bash::ast::{ParseForSecurityResult, SimpleCommandNode};
use zk_tools::bash::security::{BashSecurityAnalyzer, SecurityLevel};

/// 旧源 L26-34 `setUp()`：`AppStateStore` 的 `workingDirectory` / `projectRoot`
/// 均为 `System.getProperty("user.dir")`；`CommandBlacklistService(null, ...)`
/// 等价于 zkcode 的默认内置规则集。
fn analyzer() -> BashSecurityAnalyzer {
    BashSecurityAnalyzer::new()
}

/// 旧源 L36-41 `assertSimple(String cmd)`。
fn assert_simple(cmd: &str) -> Vec<SimpleCommandNode> {
    let result = analyzer().parse_for_security(Some(cmd));
    match result {
        ParseForSecurityResult::Simple { commands } => commands,
        other => panic!("Expected Simple for: {cmd}, got: {other:?}"),
    }
}

/// 旧源 L43-48 `assertTooComplex(String cmd)`。
fn assert_too_complex(cmd: &str) -> (String, String) {
    let result = analyzer().parse_for_security(Some(cmd));
    match result {
        ParseForSecurityResult::TooComplex { reason, node_type } => (reason, node_type),
        other => panic!("Expected TooComplex for: {cmd}, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// BS-01: 安全命令放行 —— 旧源 L50-87
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L57-62 `test_ls_la`：`ls -la` → simple。
#[test]
fn bs01_test_ls_la() {
    let commands = assert_simple("ls -la");
    assert_eq!(1, commands.len());
    assert_eq!("ls", commands.first().unwrap().argv.first().unwrap());
}

/// 旧源 L64-68 `test_ls_la_tmp`：`ls -la /tmp` → simple。
#[test]
fn bs01_test_ls_la_tmp() {
    let commands = assert_simple("ls -la /tmp");
    assert!(!commands.is_empty());
}

/// 旧源 L70-74 `test_cat`：`cat README.md` → simple。
#[test]
fn bs01_test_cat() {
    let commands = assert_simple("cat README.md");
    assert_eq!("cat", commands.first().unwrap().argv.first().unwrap());
}

/// 旧源 L76-80 `test_echo`：`echo hello` → simple。
#[test]
fn bs01_test_echo() {
    let commands = assert_simple("echo hello");
    assert_eq!("echo", commands.first().unwrap().argv.first().unwrap());
}

/// 旧源 L82-86 `test_pwd`：`pwd` → simple。
#[test]
fn bs01_test_pwd() {
    let commands = assert_simple("pwd");
    assert_eq!("pwd", commands.first().unwrap().argv.first().unwrap());
}

// ═══════════════════════════════════════════════════════════════════
// BS-02: 破坏性命令阻断（通过 checkArgLevelSecurity）—— 旧源 L89-137
// ═══════════════════════════════════════════════════════════════════

fn argv(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

/// 旧源 L96-101 `test_rm_rf_root`：`rm -rf /` → DENY。
#[test]
fn bs02_test_rm_rf_root() {
    let level = analyzer().check_arg_level_security("rm -rf /", &argv(&["rm", "-rf", "/"]));
    assert_eq!(SecurityLevel::Deny, level);
}

/// 旧源 L103-108 `test_rm_rf_home`：`rm -rf ~` → DENY。
#[test]
fn bs02_test_rm_rf_home() {
    let level = analyzer().check_arg_level_security("rm -rf ~", &argv(&["rm", "-rf", "~"]));
    assert_eq!(SecurityLevel::Deny, level);
}

/// 旧源 L110-115 `test_chmod_777`：`chmod 777 -R /` → DENY。
#[test]
fn bs02_test_chmod_777() {
    let level =
        analyzer().check_arg_level_security("chmod 777 -R /", &argv(&["chmod", "777", "-R", "/"]));
    assert_eq!(SecurityLevel::Deny, level);
}

/// 旧源 L117-122 `test_rm_safe`：`rm safe-file` → SAFE（非递归非根）。
#[test]
fn bs02_test_rm_safe() {
    let level = analyzer().check_arg_level_security("rm safe-file", &argv(&["rm", "safe-file"]));
    assert_eq!(SecurityLevel::Safe, level);
}

/// 旧源 L124-129 `test_git_push_force`：`git push --force` → ASK（危险子命令）。
#[test]
fn bs02_test_git_push_force() {
    let level =
        analyzer().check_arg_level_security("git push --force", &argv(&["git", "push", "--force"]));
    assert_eq!(SecurityLevel::Ask, level);
}

/// 旧源 L131-136 `test_docker_rm`：`docker rm container` → ASK（危险子命令）。
#[test]
fn bs02_test_docker_rm() {
    let level = analyzer()
        .check_arg_level_security("docker rm container", &argv(&["docker", "rm", "container"]));
    assert_eq!(SecurityLevel::Ask, level);
}

// ═══════════════════════════════════════════════════════════════════
// BS-03: 命令包装剥离 —— 旧源 L139-177
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L146-152 `test_sudo_rm`：sudo 剥离后 `argv[0]=rm`，rm 不在
/// `EVAL_LIKE_BUILTINS`；
/// `parseForSecurity` 不直接拒绝 rm——那是权限层的事，故仅断言非 null。
#[test]
fn bs03_test_sudo_rm() {
    let result = analyzer().parse_for_security(Some("sudo rm -rf /"));
    // 旧源 assertNotNull(result)：Rust 侧返回值非 Option，等价断言为"能得到结果"。
    let _ = result;
}

/// 旧源 L154-158 `test_sudo_eval`：`sudo eval 'dangerous'` → eval-like 拦截。
#[test]
fn bs03_test_sudo_eval() {
    let (reason, _) = assert_too_complex("sudo eval 'dangerous'");
    assert!(reason.contains("eval-like"), "reason={reason}");
}

/// 旧源 L160-164 `test_command_v`：`command -v ls` → 保留（-v 仅查询）。
#[test]
fn bs03_test_command_v() {
    let commands = assert_simple("command -v ls");
    assert!(!commands.is_empty());
}

/// 旧源 L166-170 `test_nohup_eval`：`nohup eval 'code'` → eval-like 拦截。
#[test]
fn bs03_test_nohup_eval() {
    let (reason, _) = assert_too_complex("nohup eval 'code'");
    assert!(reason.contains("eval-like"), "reason={reason}");
}

/// 旧源 L172-176 `test_env_eval`：`env eval 'code'` → eval-like 拦截。
#[test]
fn bs03_test_env_eval() {
    let (reason, _) = assert_too_complex("env eval 'code'");
    assert!(reason.contains("eval-like"), "reason={reason}");
}

// ═══════════════════════════════════════════════════════════════════
// BS-04: 命令替换检查 —— 旧源 L179-202
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L186-190 `test_dollar_paren`：`echo $(date)` → simple。
#[test]
fn bs04_test_dollar_paren() {
    let commands = assert_simple("echo $(date)");
    assert!(!commands.is_empty());
}

/// 旧源 L192-196 `test_backtick`：``echo `uname` `` → simple。
#[test]
fn bs04_test_backtick() {
    let commands = assert_simple("echo `uname`");
    assert!(!commands.is_empty());
}

/// 旧源 L198-201 `test_arithmetic`：`echo $((1+2))` → too-complex（算术展开）。
#[test]
fn bs04_test_arithmetic() {
    assert_too_complex("echo $((1+2))");
}

// ═══════════════════════════════════════════════════════════════════
// BS-05: 敏感路径检查 —— 旧源 L204-223
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L211-216 `test_proc_environ`：`cat /proc/self/environ` → too-complex。
#[test]
fn bs05_test_proc_environ() {
    let (reason, _) = assert_too_complex("cat /proc/self/environ");
    assert!(
        reason.contains("/proc/") || reason.contains("environ"),
        "reason={reason}"
    );
}

/// 旧源 L218-222 `test_proc_pid_environ`：`cat /proc/1/environ` → too-complex。
#[test]
fn bs05_test_proc_pid_environ() {
    let (reason, _) = assert_too_complex("cat /proc/1/environ");
    assert!(reason.contains("/proc/"), "reason={reason}");
}

// ═══════════════════════════════════════════════════════════════════
// BS-06: 危险变量检查 —— 旧源 L225-249
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L232-236 `test_ifs_assignment`：`IFS=x` → too-complex（危险变量赋值）。
#[test]
fn bs06_test_ifs_assignment() {
    let (reason, _) = assert_too_complex("IFS=x");
    assert!(reason.contains("Dangerous variable"), "reason={reason}");
}

/// 旧源 L238-242 `test_ps4_assignment`：`PS4='$(cmd)'` → too-complex（PS4 注入）。
#[test]
fn bs06_test_ps4_assignment() {
    let (reason, _) = assert_too_complex("PS4='$(cmd)'");
    assert!(reason.contains("Dangerous variable"), "reason={reason}");
}

/// 旧源 L244-248 `test_eval`：`eval 'echo hello'` → too-complex。
#[test]
fn bs06_test_eval() {
    let (reason, _) = assert_too_complex("eval 'echo hello'");
    assert!(reason.contains("eval-like"), "reason={reason}");
}

// ═══════════════════════════════════════════════════════════════════
// BS-07: Zsh 危险命令 —— 旧源 L251-287
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L258-262 `test_zmodload`：`zmodload zsh/system` → too-complex。
#[test]
fn bs07_test_zmodload() {
    let (reason, _) = assert_too_complex("zmodload zsh/system");
    assert!(reason.contains("zsh dangerous"), "reason={reason}");
}

/// 旧源 L264-268 `test_autoload`：`autoload func` → too-complex。
#[test]
fn bs07_test_autoload() {
    let (reason, _) = assert_too_complex("autoload func");
    assert!(reason.contains("zsh dangerous"), "reason={reason}");
}

/// 旧源 L270-274 `test_zle`：`zle -N my-widget` → too-complex。
#[test]
fn bs07_test_zle() {
    let (reason, _) = assert_too_complex("zle -N my-widget");
    assert!(reason.contains("zsh dangerous"), "reason={reason}");
}

/// 旧源 L276-280 `test_ztcp`：`ztcp host 8080` → too-complex。
#[test]
fn bs07_test_ztcp() {
    let (reason, _) = assert_too_complex("ztcp host 8080");
    assert!(reason.contains("zsh dangerous"), "reason={reason}");
}

/// 旧源 L282-286 `test_zsocket`：`zsocket /tmp/sock` → too-complex。
#[test]
fn bs07_test_zsocket() {
    let (reason, _) = assert_too_complex("zsocket /tmp/sock");
    assert!(reason.contains("zsh dangerous"), "reason={reason}");
}

// ═══════════════════════════════════════════════════════════════════
// BS-08: 控制字符检查 —— 旧源 L289-319
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L296-300 `test_nul`：含 NUL 字符 → too-complex。
#[test]
fn bs08_test_nul() {
    let (reason, _) = assert_too_complex("echo \u{0}hello");
    assert!(reason.contains("control characters"), "reason={reason}");
}

/// 旧源 L302-306 `test_bel`：含 BEL 字符 → too-complex。
#[test]
fn bs08_test_bel() {
    let (reason, _) = assert_too_complex("echo \u{7}hello");
    assert!(reason.contains("control characters"), "reason={reason}");
}

/// 旧源 L308-312 `test_backspace`：含 BS 字符 → too-complex。
#[test]
fn bs08_test_backspace() {
    let (reason, _) = assert_too_complex("echo \u{8}hello");
    assert!(reason.contains("control characters"), "reason={reason}");
}

/// 旧源 L314-318 `test_del`：含 DEL 字符 → too-complex。
#[test]
fn bs08_test_del() {
    let (reason, _) = assert_too_complex("echo \u{7f}hello");
    assert!(reason.contains("control characters"), "reason={reason}");
}

// ═══════════════════════════════════════════════════════════════════
// BS-09: 花括号展开检查 —— 旧源 L321-338
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L328-331 `test_brace_expansion`：`echo {a,b,c}` → too-complex。
#[test]
fn bs09_test_brace_expansion() {
    assert_too_complex("echo {a,b,c}");
}

/// 旧源 L333-337 `test_brace_quote_confusion`：`{a,b,'c'}` → too-complex 含 "brace"。
#[test]
fn bs09_test_brace_quote_confusion() {
    let (reason, _) = assert_too_complex("{a,b,'c'}");
    assert!(reason.contains("brace"), "reason={reason}");
}

// ═══════════════════════════════════════════════════════════════════
// BS-10: 安全开发命令白名单 —— 旧源 L340-371
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L347-350 `test_git_status`：`git status` → simple。
#[test]
fn bs10_test_git_status() {
    assert_simple("git status");
}

/// 旧源 L352-355 `test_npm_install`：`npm install` → simple。
#[test]
fn bs10_test_npm_install() {
    assert_simple("npm install");
}

/// 旧源 L357-360 `test_mvn_clean`：`mvn clean` → simple。
#[test]
fn bs10_test_mvn_clean() {
    assert_simple("mvn clean");
}

/// 旧源 L362-365 `test_pip`：`pip install -r requirements.txt` → simple。
#[test]
fn bs10_test_pip() {
    assert_simple("pip install -r requirements.txt");
}

/// 旧源 L367-370 `test_grep`：`grep -r pattern .` → simple。
#[test]
fn bs10_test_grep() {
    assert_simple("grep -r pattern .");
}

// ═══════════════════════════════════════════════════════════════════
// 预检查链补充 —— 旧源 L373-410
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L380-384 `test_unicode_whitespace`：Unicode 空白 → too-complex。
#[test]
fn precheck_test_unicode_whitespace() {
    let (reason, _) = assert_too_complex("echo\u{a0}hello");
    assert!(reason.contains("Unicode whitespace"), "reason={reason}");
}

/// 旧源 L386-390 `test_zsh_tilde_bracket`：`cd ~[some]` → too-complex。
#[test]
fn precheck_test_zsh_tilde_bracket() {
    let (reason, _) = assert_too_complex("cd ~[some]");
    assert!(reason.contains("zsh ~[...]"), "reason={reason}");
}

/// 旧源 L392-396 `test_zsh_equals_expansion`：` =ls` → too-complex。
#[test]
fn precheck_test_zsh_equals_expansion() {
    let (reason, _) = assert_too_complex(" =ls");
    assert!(reason.contains("zsh =cmd"), "reason={reason}");
}

/// 旧源 L398-402 `test_empty`：空命令 → simple（空列表）。
#[test]
fn precheck_test_empty() {
    let commands = assert_simple("");
    assert!(commands.is_empty());
}

/// 旧源 L404-409 `test_overlong`：超长命令 → parse-unavailable。
#[test]
fn precheck_test_overlong() {
    let long_cmd = format!("echo {}", "x".repeat(10001));
    let result = analyzer().parse_for_security(Some(&long_cmd));
    assert_eq!(ParseForSecurityResult::ParseUnavailable, result);
}

// ═══════════════════════════════════════════════════════════════════
// 语义检查补充 —— 旧源 L412-501
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L419-422 `test_eval`：`eval echo hi` → too-complex。
#[test]
fn semantic_test_eval() {
    assert_too_complex("eval echo hi");
}

/// 旧源 L424-427 `test_source`：`source ~/.bashrc` → too-complex。
#[test]
fn semantic_test_source() {
    assert_too_complex("source ~/.bashrc");
}

/// 旧源 L429-432 `test_exec`：`exec bash` → too-complex。
#[test]
fn semantic_test_exec() {
    assert_too_complex("exec bash");
}

/// 旧源 L434-437 `test_trap`：`trap 'echo bye' EXIT` → too-complex。
#[test]
fn semantic_test_trap() {
    assert_too_complex("trap 'echo bye' EXIT");
}

/// 旧源 L439-442 `test_alias`：`alias ll='ls -la'` → too-complex。
#[test]
fn semantic_test_alias() {
    assert_too_complex("alias ll='ls -la'");
}

/// 旧源 L444-447 `test_bind`：`bind -x '"\C-l": clear'` → too-complex。
#[test]
fn semantic_test_bind() {
    assert_too_complex("bind -x '\"\\C-l\": clear'");
}

/// 旧源 L449-452 `test_jq_system`：`jq 'system("id")'` → too-complex。
#[test]
fn semantic_test_jq_system() {
    assert_too_complex("jq 'system(\"id\")'");
}

/// 旧源 L454-457 `test_jq_file`：`jq -f script.jq data.json` → too-complex。
#[test]
fn semantic_test_jq_file() {
    assert_too_complex("jq -f script.jq data.json");
}

/// 旧源 L459-462 `test_process_substitution`：`diff <(cmd1) <(cmd2)` → too-complex。
#[test]
fn semantic_test_process_substitution() {
    assert_too_complex("diff <(cmd1) <(cmd2)");
}

/// 旧源 L464-467 `test_output_process_substitution`：`tee >(sha256sum)` → too-complex。
#[test]
fn semantic_test_output_process_substitution() {
    assert_too_complex("tee >(sha256sum)");
}

/// 旧源 L469-472 `test_translated_string`：`echo $"hello"` → too-complex。
#[test]
fn semantic_test_translated_string() {
    assert_too_complex("echo $\"hello\"");
}

/// 旧源 L474-480 `test_newline_hash`：`\n#` 在 argv 中，根据具体解析情况判定，
/// 旧源仅 `assertNotNull(result)`。
#[test]
fn semantic_test_newline_hash() {
    let result = analyzer().parse_for_security(Some("echo 'arg\n#hidden'"));
    let _ = result;
}

/// 旧源 L482-485 `test_printf_v_subscript`：`printf -v 'arr[0]' '%s' val` → too-complex。
#[test]
fn semantic_test_printf_v_subscript() {
    assert_too_complex("printf -v 'arr[0]' '%s' val");
}

/// 旧源 L487-490 `test_read_a_subscript`：`read -a 'arr[0]'` → too-complex。
#[test]
fn semantic_test_read_a_subscript() {
    assert_too_complex("read -a 'arr[0]'");
}

/// 旧源 L492-495 `test_declare_n_subscript`：`declare -n 'ref[0]'` → too-complex。
#[test]
fn semantic_test_declare_n_subscript() {
    assert_too_complex("declare -n 'ref[0]'");
}

/// 旧源 L497-500 `test_arithmetic_compare_subscript`：`[[ arr[0] -eq 1 ]]` → too-complex。
#[test]
fn semantic_test_arithmetic_compare_subscript() {
    assert_too_complex("[[ arr[0] -eq 1 ]]");
}

// ═══════════════════════════════════════════════════════════════════
// Heredoc 安全 —— 旧源 L503-525
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L510-514 `test_cat_heredoc`：`cat <<EOF\nhello\nEOF` → simple（只读安全）。
#[test]
fn heredoc_test_cat_heredoc() {
    let commands = assert_simple("cat <<EOF\nhello\nEOF");
    assert!(!commands.is_empty());
}

/// 旧源 L516-524 `test_python_heredoc`：python + heredoc 应触发 heredoc-security；
/// 若结果为 `TooComplex` 则 reason 含 "Heredoc" 或 "heredoc"。
#[test]
fn heredoc_test_python_heredoc() {
    let result = analyzer().parse_for_security(Some("python <<EOF\nprint('hi')\nEOF"));
    if let ParseForSecurityResult::TooComplex { reason, .. } = &result {
        assert!(
            reason.contains("Heredoc") || reason.contains("heredoc"),
            "reason={reason}"
        );
    }
    // 旧源注释：如果 parseForSecurity 不拦截 python heredoc，至少应为 Simple。
}

// ═══════════════════════════════════════════════════════════════════
// 路径安全验证 —— 旧源 L527-538
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L534-537 `test_process_sub_path`：`cat <(ls /etc)` → too-complex。
#[test]
fn pathsec_test_process_sub_path() {
    assert_too_complex("cat <(ls /etc)");
}
