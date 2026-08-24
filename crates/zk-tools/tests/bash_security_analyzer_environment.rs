//! Shell 环境变量引用分析测试。
//!
//! 逐条翻译旧源 `backend/src/test/java/com/aicodeassistant/tool/bash/
//! BashSecurityAnalyzerEnvironmentTest.java`（56 行 / 3 个 `@Test`），断言零改写。

use zk_tools::bash::security::BashSecurityAnalyzer;

/// 旧源 L11-13：`new BashSecurityAnalyzer(mock(PathValidator), mock(AppStateStore),
/// mock(CommandBlacklistService))`。环境变量分析路径不触达这三个协作者，
/// zkcode 侧用默认构造等价替代（详见偏离表）。
fn analyzer() -> BashSecurityAnalyzer {
    BashSecurityAnalyzer::new()
}

/// 旧源 L50-54 `assertLocal(String command)`。
fn assert_local(command: &str) {
    let result = analyzer().analyze_environment_references(Some(command));
    assert!(!result.requires_conservative_ask(), "{command}");
    assert!(result.inherited_references.is_empty(), "{command}");
}

/// 旧源 L15-22 `astScopeRecognizesShellLocalDefinitions`。
#[test]
fn ast_scope_recognizes_shell_local_definitions() {
    assert_local("LOG=build.log; grep error \"$LOG\""); // 旧源 L17
    assert_local("for p in a b; do echo \"$p\"; done"); // 旧源 L18
    assert_local("read value; printf '%s' \"$value\""); // 旧源 L19
    assert_local("printf '%s' \"$1\""); // 旧源 L20
    assert_local("f(){ local item=ok; echo \"$item\"; }; f"); // 旧源 L21
}

/// 旧源 L24-36 `inheritedUnknownAndSensitiveVariablesAreClassified`。
#[test]
fn inherited_unknown_and_sensitive_variables_are_classified() {
    // 旧源 L26-28
    let unknown = analyzer().analyze_environment_references(Some("echo \"$CUSTOM_HOME\""));
    assert_eq!(
        vec!["CUSTOM_HOME"],
        unknown.inherited_references.iter().collect::<Vec<_>>()
    );
    assert!(unknown.sensitive_inherited_references.is_empty());

    // 旧源 L30-31
    let sensitive = analyzer().analyze_environment_references(Some("echo \"$OPENAI_API_KEY\""));
    assert_eq!(
        vec!["OPENAI_API_KEY"],
        sensitive
            .sensitive_inherited_references
            .iter()
            .collect::<Vec<_>>()
    );

    // 旧源 L33-35
    let allowed = analyzer().analyze_environment_references(Some("echo \"$PATH\""));
    assert_eq!(
        vec!["PATH"],
        allowed.inherited_references.iter().collect::<Vec<_>>()
    );
    assert!(analyzer().is_allowed_inherited_environment_reference(Some("PATH")));
}

/// 旧源 L38-48 `assignmentRightHandSideAndDynamicExpansionFailClosed`。
#[test]
fn assignment_right_hand_side_and_dynamic_expansion_fail_closed() {
    // 旧源 L40-42
    let inherited =
        analyzer().analyze_environment_references(Some("SECRET=$OPENAI_API_KEY; echo \"$SECRET\""));
    assert!(
        inherited
            .sensitive_inherited_references
            .contains("OPENAI_API_KEY")
    );
    assert!(!inherited.inherited_references.contains("SECRET"));

    // 旧源 L44-45
    assert!(
        analyzer()
            .analyze_environment_references(Some("echo \"${!name}\""))
            .requires_conservative_ask()
    );
    // 旧源 L46-47
    assert!(
        analyzer()
            .analyze_environment_references(Some("eval \"$COMMAND\""))
            .requires_conservative_ask()
    );
}
