//! `BashCommandClassifier` 独立单元测试。
//!
//! 逐条翻译旧源 `backend/src/test/java/com/aicodeassistant/tool/bash/
//! BashCommandClassifierTest.java`（790 行 / 77 个 `@Test`），断言零改写。
//!
//! 旧源类注释 L10-15：覆盖三层只读验证、管道/复合命令拆分、安全加固
//! （变量展开/花括号检测）、Git/GH/Docker/外部命令分类、`classify()` 分类逻辑、
//! 边界情况。

use zk_tools::bash::classifier::BashCommandClassifier;

/// 旧源 L20-23 `setUp()`：`new BashCommandClassifier()`。
fn c() -> BashCommandClassifier {
    BashCommandClassifier::new()
}

/// `classifier.isReadOnlyCommand(cmd)` 的简写。
fn ro(cmd: &str) -> bool {
    c().is_read_only_command(Some(cmd))
}

// ══════════════════════════════════════════════════════════════
// 层1: 纯只读命令（READONLY_COMMANDS）—— 旧源 L25-96
// ══════════════════════════════════════════════════════════════

/// 旧源 L33-44 `systemInfoCommandsAreReadOnly`。
#[test]
fn system_info_commands_are_read_only() {
    assert!(ro("cal"));
    assert!(ro("uptime"));
    assert!(ro("id"));
    assert!(ro("uname"));
    assert!(ro("free"));
    assert!(ro("df"));
    assert!(ro("du"));
    assert!(ro("nproc"));
}

/// 旧源 L46-57 `fileViewCommandsAreReadOnly`。
#[test]
fn file_view_commands_are_read_only() {
    assert!(ro("cat"));
    assert!(ro("head"));
    assert!(ro("tail"));
    assert!(ro("wc"));
    assert!(ro("stat"));
    assert!(ro("strings"));
    assert!(ro("nl"));
    assert!(ro("readlink"));
}

/// 旧源 L59-69 `textProcessingCommandsAreReadOnly`。
#[test]
fn text_processing_commands_are_read_only() {
    assert!(ro("cut"));
    assert!(ro("paste"));
    assert!(ro("tr"));
    assert!(ro("column"));
    assert!(ro("tac"));
    assert!(ro("rev"));
    assert!(ro("diff"));
}

/// 旧源 L71-77 `pathCommandsAreReadOnly`。
#[test]
fn path_commands_are_read_only() {
    assert!(ro("basename"));
    assert!(ro("dirname"));
    assert!(ro("realpath"));
}

/// 旧源 L79-86 `securityToolsAreReadOnly`。
#[test]
fn security_tools_are_read_only() {
    assert!(ro("md5sum"));
    assert!(ro("sha1sum"));
    assert!(ro("openssl"));
    assert!(ro("xxd"));
}

/// 旧源 L88-95 `readOnlyCommandsWithArgsStillReadOnly`。
#[test]
fn read_only_commands_with_args_still_read_only() {
    assert!(ro("cat file.txt"));
    assert!(ro("head -n 10 file.txt"));
    assert!(ro("wc -l file.txt"));
    assert!(ro("diff file1 file2"));
}

// ══════════════════════════════════════════════════════════════
// 层2: 正则匹配只读（READONLY_REGEXES）—— 旧源 L98-133
// ══════════════════════════════════════════════════════════════

/// 旧源 L102-107 `echoIsReadOnly`。
#[test]
fn echo_is_read_only() {
    assert!(ro("echo hello"));
    assert!(ro("echo 'some text'"));
}

/// 旧源 L109-114 `pwdWhoamiAreReadOnly`。
#[test]
fn pwd_whoami_are_read_only() {
    assert!(ro("pwd"));
    assert!(ro("whoami"));
}

/// 旧源 L116-125 `versionCommandsAreReadOnly`。
#[test]
fn version_commands_are_read_only() {
    assert!(ro("node -v"));
    assert!(ro("node --version"));
    assert!(ro("python3 --version"));
    assert!(ro("java --version"));
    assert!(ro("mvn --version"));
    assert!(ro("gradle --version"));
}

/// 旧源 L127-132 `uniqIsReadOnly`。
#[test]
fn uniq_is_read_only() {
    assert!(ro("uniq"));
    assert!(ro("uniq -c"));
}

// ══════════════════════════════════════════════════════════════
// 层3: flag 级别白名单验证（COMMAND_ALLOWLIST）—— 旧源 L135-240
// ══════════════════════════════════════════════════════════════

/// 旧源 L139-146 `sortWithSafeFlagsIsReadOnly`。
#[test]
fn sort_with_safe_flags_is_read_only() {
    assert!(ro("sort -r"));
    assert!(ro("sort -n"));
    assert!(ro("sort -u"));
    assert!(ro("sort -k 2"));
}

/// 旧源 L148-155 `grepWithSafeFlagsIsReadOnly`。
#[test]
fn grep_with_safe_flags_is_read_only() {
    assert!(ro("grep -r pattern ."));
    assert!(ro("grep -i pattern file.txt"));
    assert!(ro("grep -n pattern file.txt"));
    assert!(ro("grep -A 5 pattern file.txt"));
}

/// 旧源 L157-163 `grepCombinedShortFlagsIsReadOnly`：
/// `-rn` 是 `-r` + `-n` 的组合，两者均为 NONE 类型。
#[test]
fn grep_combined_short_flags_is_read_only() {
    assert!(
        ro("grep -rn pattern ."),
        "grep -rn should be read-only (combined -r + -n)"
    );
}

/// 旧源 L165-170 `grepIncludeGlobIsReadOnly`：
/// `--include=*.java` 中的 `*` 在引号内，不应触发 `containsUnquotedExpansion`。
#[test]
fn grep_include_glob_is_read_only() {
    assert!(ro("grep --include='*.java' pattern ."));
}

/// 旧源 L172-179 `rgWithSafeFlagsIsReadOnly`。
#[test]
fn rg_with_safe_flags_is_read_only() {
    assert!(ro("rg -i pattern"));
    assert!(ro("rg --hidden pattern"));
    assert!(ro("rg -t java pattern"));
    assert!(ro("rg -C 3 pattern"));
}

/// 旧源 L181-187 `grepAttachedNumberArgIsReadOnly`。
#[test]
fn grep_attached_number_arg_is_read_only() {
    assert!(ro("grep -A20 pattern file"));
    assert!(ro("grep -B5 pattern file"));
    assert!(ro("rg -C10 pattern"));
}

/// 旧源 L189-195 `treeWithSafeFlagsIsReadOnly`。
#[test]
fn tree_with_safe_flags_is_read_only() {
    assert!(ro("tree -L 3"));
    assert!(ro("tree -d"));
    assert!(ro("tree -a"));
}

/// 旧源 L197-202 `psWithSafeFlagsIsReadOnly`。
#[test]
fn ps_with_safe_flags_is_read_only() {
    assert!(ro("ps -ef"));
    assert!(ro("ps -A"));
}

/// 旧源 L204-208 `sedReadOnlyModeIsReadOnly`。
#[test]
fn sed_read_only_mode_is_read_only() {
    assert!(ro("sed -n '1,10p'"));
}

/// 旧源 L210-215 `unknownFlagsCauseRejection`。
#[test]
fn unknown_flags_cause_rejection() {
    assert!(!ro("sort --dangerous-flag"));
    assert!(!ro("grep --unknown-flag pattern"));
}

/// 旧源 L217-223 `xargsWithSafeTargetIsReadOnly`。
#[test]
fn xargs_with_safe_target_is_read_only() {
    assert!(ro("xargs echo"));
    assert!(ro("xargs grep pattern"));
    assert!(ro("xargs -I {} echo {}"));
}

/// 旧源 L225-230 `xargsWithUnsafeTargetIsRejected`。
#[test]
fn xargs_with_unsafe_target_is_rejected() {
    assert!(!ro("xargs rm"));
    assert!(!ro("xargs mv"));
}

/// 旧源 L232-239 `tputSafeAndDangerousCapabilities`。
#[test]
fn tput_safe_and_dangerous_capabilities() {
    assert!(ro("tput cols"));
    assert!(ro("tput lines"));
    assert!(!ro("tput init"));
    assert!(!ro("tput reset"));
}

// ══════════════════════════════════════════════════════════════
// find 命令特殊处理 —— 旧源 L242-261
// ══════════════════════════════════════════════════════════════

/// 旧源 L246-252 `safeFindIsReadOnly`。
#[test]
fn safe_find_is_read_only() {
    assert!(ro("find . -name '*.java'"));
    assert!(ro("find /tmp -type f"));
    assert!(ro("find . -maxdepth 2 -name '*.txt'"));
}

/// 旧源 L254-260 `dangerousFindIsRejected`。
#[test]
fn dangerous_find_is_rejected() {
    assert!(!ro("find . -delete"));
    assert!(!ro("find . -exec rm {} ;"));
    assert!(!ro("find . -execdir mv {} /tmp ;"));
}

// ══════════════════════════════════════════════════════════════
// 管道命令处理 —— 旧源 L267-284
// ══════════════════════════════════════════════════════════════

/// 旧源 L271-277 `allReadOnlyPipeIsReadOnly`。
#[test]
fn all_read_only_pipe_is_read_only() {
    assert!(ro("cat file.txt | grep pattern"));
    assert!(ro("cat file | sort | head"));
    assert!(ro("echo hello | wc -l"));
}

/// 旧源 L279-283 `pipeWithDangerousCommandIsRejected`。
#[test]
fn pipe_with_dangerous_command_is_rejected() {
    assert!(!ro("cat file | rm something"));
}

// ══════════════════════════════════════════════════════════════
// 复合命令处理（&&、||、;）—— 旧源 L286-304
// ══════════════════════════════════════════════════════════════

/// 旧源 L290-295 `allReadOnlyCompoundIsReadOnly`。
#[test]
fn all_read_only_compound_is_read_only() {
    assert!(ro("pwd && whoami"));
    assert!(ro("echo hello; pwd"));
}

/// 旧源 L297-303 `compoundWithDangerousIsRejected`。
#[test]
fn compound_with_dangerous_is_rejected() {
    assert!(!ro("pwd && rm file"));
    assert!(!ro("echo hello; rm -rf /"));
    assert!(!ro("cat file || rm file"));
}

// ══════════════════════════════════════════════════════════════
// 安全加固: 变量展开/花括号检测 —— 旧源 L310-367
// ══════════════════════════════════════════════════════════════

/// 旧源 L314-320 `detectsUnquotedVariableExpansion`。
#[test]
fn detects_unquoted_variable_expansion() {
    assert!(c().contains_unquoted_expansion("echo $HOME"));
    assert!(c().contains_unquoted_expansion("cat ${file}"));
    assert!(c().contains_unquoted_expansion("echo $(whoami)"));
}

/// 旧源 L322-327 `singleQuotedVariableNotDetected`。
#[test]
fn single_quoted_variable_not_detected() {
    assert!(!c().contains_unquoted_expansion("echo '$HOME'"));
    assert!(!c().contains_unquoted_expansion("echo '${file}'"));
}

/// 旧源 L329-334 `detectsUnquotedGlob`。
#[test]
fn detects_unquoted_glob() {
    assert!(c().contains_unquoted_expansion("ls *.txt"));
    assert!(c().contains_unquoted_expansion("cat file?.txt"));
}

/// 旧源 L336-340 `detectsBraceExpansion`。
#[test]
fn detects_brace_expansion() {
    assert!(c().contains_unquoted_expansion("echo {a,b,c}"));
}

/// 旧源 L342-346 `escapedCharactersNotDetected`。
#[test]
fn escaped_characters_not_detected() {
    assert!(!c().contains_unquoted_expansion("echo \\$HOME"));
}

/// 旧源 L348-353 `readOnlyRejectsUnquotedExpansion`。
#[test]
fn read_only_rejects_unquoted_expansion() {
    assert!(!ro("sort $HOME/file"));
    assert!(!ro("grep pattern ${file}"));
}

/// 旧源 L355-360 `dollarInFlagTokenRejected`。
#[test]
fn dollar_in_flag_token_rejected() {
    assert!(!ro("sort $FILE"));
    assert!(!ro("grep -r $PATTERN ."));
}

/// 旧源 L362-366 `braceExpansionInFlagTokenRejected`。
#[test]
fn brace_expansion_in_flag_token_rejected() {
    assert!(!ro("sort {a,b}"));
}

// ══════════════════════════════════════════════════════════════
// Git 命令分类 —— 旧源 L369-456
// ══════════════════════════════════════════════════════════════

/// 旧源 L377-390 `gitReadOnlySubcommandsAreReadOnly`。
#[test]
fn git_read_only_subcommands_are_read_only() {
    assert!(ro("git status"));
    assert!(ro("git log"));
    assert!(ro("git diff"));
    assert!(ro("git show"));
    assert!(ro("git branch"));
    assert!(ro("git remote -v"));
    assert!(ro("git blame file.java"));
    assert!(ro("git ls-files"));
    assert!(ro("git rev-parse HEAD"));
    assert!(ro("git config --get user.name"));
}

/// 旧源 L392-399 `gitLogWithSafeFlagsIsReadOnly`。
#[test]
fn git_log_with_safe_flags_is_read_only() {
    assert!(ro("git log --oneline"));
    assert!(ro("git log -n 10"));
    assert!(ro("git log --graph --stat"));
    assert!(ro("git log --format=oneline"));
}

/// 旧源 L401-407 `gitDiffWithSafeFlagsIsReadOnly`。
#[test]
fn git_diff_with_safe_flags_is_read_only() {
    assert!(ro("git diff --cached"));
    assert!(ro("git diff --stat"));
    assert!(ro("git diff --name-only"));
}

/// 旧源 L409-414 `gitNumberShortcutIsReadOnly`。
#[test]
fn git_number_shortcut_is_read_only() {
    assert!(ro("git log -5"));
    assert!(ro("git log -20"));
}

/// 旧源 L416-423 `gitBranchReadOnlyVsCreate`。
#[test]
fn git_branch_read_only_vs_create() {
    assert!(ro("git branch"));
    assert!(ro("git branch -a"));
    assert!(ro("git branch --list"));
    assert!(!ro("git branch new-feature"));
}

/// 旧源 L425-431 `gitTagListVsCreate`。
#[test]
fn git_tag_list_vs_create() {
    assert!(ro("git tag -l"));
    assert!(ro("git tag --list"));
    assert!(!ro("git tag v1.0"));
}

/// 旧源 L433-441 `gitWriteCommandsAreRejected`。
#[test]
fn git_write_commands_are_rejected() {
    assert!(!ro("git push"));
    assert!(!ro("git commit -m 'msg'"));
    assert!(!ro("git merge feature"));
    assert!(!ro("git rebase main"));
    assert!(!ro("git reset HEAD~1"));
}

/// 旧源 L443-449 `gitConfigInjectionDetected`。
#[test]
fn git_config_injection_detected() {
    assert!(!c().is_git_command_safe("git -c core.pager=less log"));
    assert!(!c().is_git_command_safe("git --exec-path=/tmp log"));
    assert!(!c().is_git_command_safe("git --config-env=X=Y log"));
}

/// 旧源 L451-455 `gitCommandWithDollarRejected`。
#[test]
fn git_command_with_dollar_rejected() {
    assert!(!ro("git log $BRANCH"));
}

// ══════════════════════════════════════════════════════════════
// 外部只读命令前缀 —— 旧源 L462-483
// ══════════════════════════════════════════════════════════════

/// 旧源 L466-482 `externalReadOnlyPrefixesAreReadOnly`。
#[test]
fn external_read_only_prefixes_are_read_only() {
    assert!(ro("docker ps"));
    assert!(ro("docker images"));
    assert!(ro("kubectl get pods"));
    assert!(ro("kubectl describe pod my-pod"));
    assert!(ro("kubectl logs my-pod"));
    assert!(ro("npm list"));
    assert!(ro("npm info express"));
    assert!(ro("npm outdated"));
    assert!(ro("npm audit"));
    assert!(ro("yarn list"));
    assert!(ro("pip list"));
    assert!(ro("pip show flask"));
    assert!(ro("pip freeze"));
}

// ══════════════════════════════════════════════════════════════
// Docker 只读命令（flag 验证）—— 旧源 L489-506
// ══════════════════════════════════════════════════════════════

/// 旧源 L493-498 `dockerLogsWithSafeFlagsIsReadOnly`。
#[test]
fn docker_logs_with_safe_flags_is_read_only() {
    assert!(ro("docker logs -f container"));
    assert!(ro("docker logs --tail 100 container"));
}

/// 旧源 L500-505 `dockerInspectIsReadOnly`。
#[test]
fn docker_inspect_is_read_only() {
    assert!(ro("docker inspect container"));
    assert!(ro("docker inspect --format '{{.State}}' container"));
}

// ══════════════════════════════════════════════════════════════
// GH CLI 只读命令 —— 旧源 L512-533
// ══════════════════════════════════════════════════════════════

/// 旧源 L516-525 `ghPrIssueReadOnlySubcommands`。
#[test]
fn gh_pr_issue_read_only_subcommands() {
    assert!(ro("gh pr list"));
    assert!(ro("gh pr view 123"));
    assert!(ro("gh pr diff 123"));
    assert!(ro("gh pr status"));
    assert!(ro("gh issue list"));
    assert!(ro("gh issue view 456"));
}

/// 旧源 L527-532 `ghWithUrlArgIsRejected`（DNS exfiltration 防护）。
#[test]
fn gh_with_url_arg_is_rejected() {
    assert!(!ro("gh pr list --repo=http://evil.com/owner/repo"));
    assert!(!ro("gh pr list --repo=user@host:repo"));
}

// ══════════════════════════════════════════════════════════════
// Pyright 只读命令 —— 旧源 L539-563
// ══════════════════════════════════════════════════════════════

/// 旧源 L543-549 `pyrightWithSafeFlagsIsReadOnly`。
#[test]
fn pyright_with_safe_flags_is_read_only() {
    assert!(ro("pyright --version"));
    assert!(ro("pyright --outputjson src/"));
    assert!(ro("pyright --stats"));
}

/// 旧源 L551-556 `pyrightWatchIsRejected`。
#[test]
fn pyright_watch_is_rejected() {
    assert!(!ro("pyright --watch"));
    assert!(!ro("pyright -w"));
}

/// 旧源 L558-562 `pyrightCreateStubIsRejected`。
#[test]
fn pyright_create_stub_is_rejected() {
    assert!(!ro("pyright --createstub numpy"));
}

// ══════════════════════════════════════════════════════════════
// classify() 分类逻辑 —— 旧源 L569-647
// ══════════════════════════════════════════════════════════════

/// 旧源 L573-579 `searchCommandsClassifiedAsSearch`。
#[test]
fn search_commands_classified_as_search() {
    let result = c().classify(Some("find . -name '*.java'"));
    assert!(result.is_search);
    assert!(result.is_read_only());
}

/// 旧源 L581-587 `readCommandsClassifiedAsRead`。
#[test]
fn read_commands_classified_as_read() {
    let result = c().classify(Some("cat file.txt"));
    assert!(result.is_read);
    assert!(result.is_read_only());
}

/// 旧源 L589-598 `listCommandsClassifiedAsList`。
#[test]
fn list_commands_classified_as_list() {
    let result = c().classify(Some("ls -la"));
    assert!(result.is_list);
    assert!(result.is_read_only());

    let tree_result = c().classify(Some("tree"));
    assert!(tree_result.is_list);
}

/// 旧源 L600-608 `dangerousCommandsNotReadOnly`。
#[test]
fn dangerous_commands_not_read_only() {
    let rm_result = c().classify(Some("rm file.txt"));
    assert!(!rm_result.is_read_only());

    let mv_result = c().classify(Some("mv a b"));
    assert!(!mv_result.is_read_only());
}

/// 旧源 L610-615 `pipeAllReadOnlySubcommands`。
#[test]
fn pipe_all_read_only_subcommands() {
    let result = c().classify(Some("cat file | grep pattern | sort"));
    assert!(result.is_read_only());
}

/// 旧源 L617-622 `pipeWithDangerousCommand`。
#[test]
fn pipe_with_dangerous_command() {
    let result = c().classify(Some("cat file | rm something"));
    assert!(!result.is_read_only());
}

/// 旧源 L624-629 `compoundCommandAllReadOnly`。
#[test]
fn compound_command_all_read_only() {
    let result = c().classify(Some("grep pattern file && wc -l file"));
    assert!(result.is_read_only());
}

/// 旧源 L631-636 `compoundCommandWithDangerous`。
#[test]
fn compound_command_with_dangerous() {
    let result = c().classify(Some("cat file; rm file"));
    assert!(!result.is_read_only());
}

/// 旧源 L638-646 `neutralCommandsAloneNotClassified`：
/// echo/printf 是 `NEUTRAL_CMDS`，单独使用无 non-neutral → (false,false,false)。
#[test]
fn neutral_commands_alone_not_classified() {
    let result = c().classify(Some("echo hello"));
    assert!(!result.is_search);
    assert!(!result.is_read);
    assert!(!result.is_list);
}

// ══════════════════════════════════════════════════════════════
// isSearchOrReadCommand —— 旧源 L653-683
// ══════════════════════════════════════════════════════════════

/// 旧源 L657-666 `searchReadListCommandsReturnTrue`。
#[test]
fn search_read_list_commands_return_true() {
    assert!(c().is_search_or_read_command(Some("grep")));
    assert!(c().is_search_or_read_command(Some("find")));
    assert!(c().is_search_or_read_command(Some("cat")));
    assert!(c().is_search_or_read_command(Some("ls")));
    assert!(c().is_search_or_read_command(Some("tree")));
    assert!(c().is_search_or_read_command(Some("sort")));
}

/// 旧源 L668-674 `dangerousCommandsReturnFalse`。
#[test]
fn dangerous_commands_return_false() {
    assert!(!c().is_search_or_read_command(Some("rm")));
    assert!(!c().is_search_or_read_command(Some("mv")));
    assert!(!c().is_search_or_read_command(Some("chmod")));
}

/// 旧源 L676-682 `emptyOrNullReturnsFalse`。
#[test]
fn empty_or_null_returns_false() {
    assert!(!c().is_search_or_read_command(None));
    assert!(!c().is_search_or_read_command(Some("")));
    assert!(!c().is_search_or_read_command(Some("   ")));
}

// ══════════════════════════════════════════════════════════════
// isCompoundCommandReadOnly —— 旧源 L689-711
// ══════════════════════════════════════════════════════════════

/// 旧源 L693-697 `allReadOnlyCompoundReturnsTrue`。
#[test]
fn all_read_only_compound_returns_true() {
    assert!(c().is_compound_command_read_only("cat file && head file"));
}

/// 旧源 L699-704 `compoundWithDangerousReturnsFalse`。
#[test]
fn compound_with_dangerous_returns_false() {
    assert!(!c().is_compound_command_read_only("cat file; rm file"));
    assert!(!c().is_compound_command_read_only("ls && mv a b"));
}

/// 旧源 L706-710 `redirectToNonDevReturnsFalse`。
#[test]
fn redirect_to_non_dev_returns_false() {
    assert!(!c().is_compound_command_read_only("echo hello > output.txt"));
}

// ══════════════════════════════════════════════════════════════
// 边界情况 —— 旧源 L717-766
// ══════════════════════════════════════════════════════════════

/// 旧源 L721-727 `nullInputDoesNotCrash`。
#[test]
fn null_input_does_not_crash() {
    assert!(!c().is_read_only_command(None));
    let result = c().classify(None);
    assert!(!result.is_read_only());
}

/// 旧源 L729-735 `emptyStringInput`。
#[test]
fn empty_string_input() {
    assert!(!ro(""));
    let result = c().classify(Some(""));
    assert!(!result.is_read_only());
}

/// 旧源 L737-744 `whitespaceOnlyInput`。
#[test]
fn whitespace_only_input() {
    assert!(!ro("   "));
    assert!(!ro("\t\n"));
    let result = c().classify(Some("   "));
    assert!(!result.is_read_only());
}

/// 旧源 L746-751 `unknownCommandsRejected`。
#[test]
fn unknown_commands_rejected() {
    assert!(!ro("someRandomCommand"));
    assert!(!ro("customtool --flag"));
}

/// 旧源 L753-758 `envPrintenvNotReadOnly`（安全移除）。
#[test]
fn env_printenv_not_read_only() {
    assert!(!ro("env"));
    assert!(!ro("printenv"));
}

/// 旧源 L760-765 `nonGitCommandIsSafe`。
#[test]
fn non_git_command_is_safe() {
    assert!(c().is_git_command_safe("ls -la"));
    assert!(c().is_git_command_safe("cat file"));
}

// ══════════════════════════════════════════════════════════════
// --flag=value 内联值解析 —— 旧源 L772-788
// ══════════════════════════════════════════════════════════════

/// 旧源 L776-781 `flagEqualsValueParsedCorrectly`。
#[test]
fn flag_equals_value_parsed_correctly() {
    assert!(ro("grep --color=auto pattern file"));
    assert!(ro("rg --color=always pattern"));
}

/// 旧源 L783-787 `flagEqualsNonNumericRejectedForNumber`。
#[test]
fn flag_equals_non_numeric_rejected_for_number() {
    assert!(!ro("rg --max-count=abc pattern"));
}
