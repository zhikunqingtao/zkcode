//! `OperationAnalyzerRegistryTest.java`（975 行，21 测试）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - **OAR-01**：旧 `registry(bash, shellState)`（L930-949）把 `PathSecurityService`
//!   整体 mock 成「一律 allowed」。Rust 侧 `PathSecurityService` 是具体类型（8 层
//!   检查是硬安全不变量，不做成可替换端口），故统一注入真实实现；本文件所有 bash
//!   素材的 cwd 都在授权根内，真实实现的判定结果与旧 mock 一致。
//! - **OAR-02**：旧 `safeBash()` 里 `isAllowedInheritedEnvironmentReference` 的白名单
//!   过滤在 Rust 侧属 `BashSecurityPort` 实现方（zk-tools）职责，端口只回**已过滤**
//!   的继承变量名列表，故 `FakeBashSecurity::inherit(&["HOME"])` 等价于旧两个 stub。
//! - **OAR-03**：旧 L195-259 把「工作区外文件」放在 `JUnit` `@TempDir` 的**父目录**
//!   （系统 temp 根）。Rust 侧改为 `temp/ws` 作授权根、`temp/outside.txt` 作外部
//!   文件——同为「授权根之外」，但不向系统 temp 根写文件。
//! - **OAR-04 (DEFERRED)**：旧 L675-731 `executionCallRejectsSensitiveReboundAfterFinalDynamicRecheck`
//!   与 L733-775 `executionCallAllowsUnchangedTargetOriginallyAuthorizedAsHigh` 断言
//!   的是**工具内部**第二次路径校验（`FileReadTool#call` 自己调
//!   `PathSecurityService#resolvePath`，失败码 `PATH_OUTSIDE_WORKSPACE`）。zkcode 的
//!   `zk-tools` 按 Phase 2 依赖铁律不依赖 `zk-authz`，其 `file_read` 目前没有接
//!   `PathSecurityService`，故这两格无法在本 crate 内表达。已在 §8 列为
//!   `MUST_FIX`/`DEFERRED`（工具侧二次校验缺口），随 `engine_bridge` 集成任务在
//!   `zk-tools` 侧补齐后翻译。

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::{FakeBashSecurity, FakeTool, TempRoot};
use serde_json::{Value, json};
use zk_authz::analyzer::OperationAnalyzerRegistry;
use zk_authz::frozen::{FrozenToolInput, FrozenToolInputFactory};
use zk_authz::model::{
    AuthorizationSubject, EffectClass, OperationDescriptor, ResourceRef, RiskClass,
};
use zk_authz::path_security::{PathSecurityService, SystemScratchpadPathPolicy};
use zk_authz::tool_facts::{
    BashParseOutcome, PassthroughFilter, StatelessShellState, ToolUseContext,
};

/// 旧 `registry(bash)` / `registry(bash, shellState)`（L902-904、L930-949）与
/// `new OperationAnalyzerRegistry(mapper, safeBash(), filter, new PathSecurityService(), ...)`
/// 的统一等价物（OAR-01）。
fn registry(bash: &Arc<FakeBashSecurity>) -> OperationAnalyzerRegistry {
    OperationAnalyzerRegistry::new(
        Some(bash.clone()),
        Arc::new(PassthroughFilter),
        Arc::new(PathSecurityService::new(
            SystemScratchpadPathPolicy::default_policy(),
        )),
        Arc::new(StatelessShellState),
    )
}

/// 旧 `safeBash()`（L951-957）：`Parsed` + 空环境引用分析。
fn safe_bash() -> Arc<FakeBashSecurity> {
    Arc::new(FakeBashSecurity::default())
}

/// 旧 `bashTool()`（L964-969）：名字 `Bash`、`isReadOnly == true`。
fn bash_tool() -> FakeTool {
    FakeTool::new("Bash").read_only()
}

/// 旧 `frozen(input)`（L971-974）：1 MiB / 4 MiB 上限，工具名恒 `Bash`。
fn frozen(input: &Value) -> FrozenToolInput {
    FrozenToolInputFactory::with_max_bytes(1024 * 1024)
        .freeze("Bash", input)
        .expect("freeze")
}

/// 旧 `new AuthorizationSubject(session, rootRun, currentRun, "wk", root)`。
fn subject(session: &str, root_run: &str, current_run: &str, root: &Path) -> AuthorizationSubject {
    AuthorizationSubject {
        root_session_id: session.to_owned(),
        root_run_id: root_run.to_owned(),
        current_run_id: current_run.to_owned(),
        workspace_key: "wk".to_owned(),
        authorization_root: root.to_path_buf(),
    }
}

/// 旧 `ToolUseContext.of(workingDirectory, sessionId)`。
fn ctx(working_directory: &str, session: &str) -> ToolUseContext {
    ToolUseContext::new(None, None, Some(session.to_owned()))
        .with_shell(Some(session.to_owned()), Some(working_directory.to_owned()))
}

/// 建一个已存在的目录并返回其规范路径（旧 `Files.createDirectory(..).toRealPath()`）。
fn dir(parent: &Path, name: &str) -> PathBuf {
    let path = parent.join(name);
    std::fs::create_dir_all(&path).expect("create dir");
    path.canonicalize().expect("canonical dir")
}

/// 写文件并返回其规范路径（旧 `Files.writeString(..).toRealPath()`）。
fn file(path: &Path, content: &str) -> PathBuf {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent dir");
    }
    std::fs::write(path, content).expect("write file");
    path.canonicalize().expect("canonical file")
}

/// 分析一次（旧 `registry.analyzerFor(tool).analyze(...)`）。
fn analyze(
    registry: &OperationAnalyzerRegistry,
    tool: &FakeTool,
    input: &Value,
    context: &ToolUseContext,
    subject: &AuthorizationSubject,
) -> zk_authz::model::AuthzResult<OperationDescriptor> {
    let kind = registry.analyzer_for(tool);
    let frozen = frozen(input);
    registry.analyze(kind, tool, &frozen, input, context, subject)
}

/// 复检一次（旧 `registry.analyzerFor(tool).recheck(...)`）。
fn recheck(
    registry: &OperationAnalyzerRegistry,
    tool: &FakeTool,
    descriptor: &OperationDescriptor,
    input: &Value,
    context: &ToolUseContext,
    subject: &AuthorizationSubject,
) -> zk_authz::model::AuthzResult<()> {
    let kind = registry.analyzer_for(tool);
    registry.recheck(kind, tool, descriptor, input, context, subject)
}

/// 旧源 `OperationAnalyzerRegistryTest.java:43-57`
/// `mcpToolsUseDedicatedAnalyzerWithoutExpandingGenericControlTools`。
#[test]
fn mcp_tools_use_dedicated_analyzer_without_expanding_generic_control_tools() {
    // L45-52
    let registry = registry(&safe_bash());
    let mcp = FakeTool::new("mcp__search__query").mcp("search");
    let control = FakeTool::new("Agent");
    let spoofed_mcp = FakeTool::new("mcp__spoofed__tool");

    // L54-56
    assert_eq!(registry.analyzer_for(&mcp).id(), "mcp-v1");
    assert_eq!(
        registry.analyzer_for(&spoofed_mcp).id(),
        "static-or-remote-v1"
    );
    assert_eq!(registry.analyzer_for(&control).id(), "static-or-remote-v1");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:59-78`
/// `unchangedBashFactsPassStrictFinalRecheck`。
#[test]
fn unchanged_bash_facts_pass_strict_final_recheck() {
    // L61-67
    let temp = TempRoot::new("analyzer-bash-recheck");
    let registry = registry(&safe_bash());
    let tool = bash_tool();
    let input = json!({"command": "pwd && ls -la"});
    let subject = subject("s", "root", "root", temp.path());
    let context = ctx(&temp.path().to_string_lossy(), "s");

    // L70-76
    let approved = analyze(&registry, &tool, &input, &context, &subject).expect("analyze");
    recheck(&registry, &tool, &approved, &input, &context, &subject).expect("recheck passes");
    assert_eq!(
        approved.effects,
        vec![EffectClass::Process, EffectClass::ReadResource]
    );
}

/// 旧源 `OperationAnalyzerRegistryTest.java:80-97`
/// `bashExactIdentityIncludesWorkingDirectory`。
#[test]
fn bash_exact_identity_includes_working_directory() {
    // L82-89
    let temp = TempRoot::new("analyzer-bash-cwd");
    let registry = registry(&safe_bash());
    let tool = bash_tool();
    let input = json!({"command": "ls -la"});
    let first_directory = dir(temp.path(), "a");
    let second_directory = dir(temp.path(), "b");
    let subject = subject("s", "root", "child", temp.path());

    // L90-93
    let first = analyze(
        &registry,
        &tool,
        &input,
        &ctx(&first_directory.to_string_lossy(), "child"),
        &subject,
    )
    .expect("analyze first");
    let second = analyze(
        &registry,
        &tool,
        &input,
        &ctx(&second_directory.to_string_lossy(), "child"),
        &subject,
    )
    .expect("analyze second");

    // L95-96
    assert_ne!(first.operation_hash, second.operation_hash);
    assert_eq!(first.resources, vec![ResourceRef::new("cwd", "a", false)]);
}

/// 旧源 `OperationAnalyzerRegistryTest.java:99-112`
/// `relativeWorkingDirectoryIsResolvedAgainstAuthorizationRoot`。
#[test]
fn relative_working_directory_is_resolved_against_authorization_root() {
    // L101-106
    let temp = TempRoot::new("analyzer-relative-cwd");
    let registry = registry(&safe_bash());
    dir(temp.path(), "module");
    let subject = subject("s", "root", "child", temp.path());
    let input = json!({"command": "ls"});

    // L108-109
    let operation = analyze(
        &registry,
        &bash_tool(),
        &input,
        &ctx("module", "s"),
        &subject,
    )
    .expect("analyze");

    // L111
    assert_eq!(
        operation.resources,
        vec![ResourceRef::new("cwd", "module", false)]
    );
}

/// 旧源 `OperationAnalyzerRegistryTest.java:114-133`
/// `bashExactIdentityIgnoresDisplayDescriptionAndTimeout`。
#[test]
fn bash_exact_identity_ignores_display_description_and_timeout() {
    // L116-123
    let temp = TempRoot::new("analyzer-bash-display");
    let registry = registry(&safe_bash());
    let tool = bash_tool();
    let first_input =
        json!({"command": "ls -la", "description": "first wording", "timeout": 1_000});
    let second_input =
        json!({"command": "ls -la", "description": "different wording", "timeout": 30_000});
    let subject = subject("s", "root", "child", temp.path());
    let context = ctx(&temp.path().to_string_lossy(), "child");

    // L125-128
    let first = analyze(&registry, &tool, &first_input, &context, &subject).expect("first");
    let second = analyze(&registry, &tool, &second_input, &context, &subject).expect("second");

    // L130-132
    assert_ne!(first.input_hash, second.input_hash);
    assert_eq!(first.operation_hash, second.operation_hash);
    assert_eq!(first.analyzer_id, "bash-v2");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:135-150`
/// `bashExactIdentityIncludesBackgroundExecutionMode`。
#[test]
fn bash_exact_identity_includes_background_execution_mode() {
    // L137-142
    let temp = TempRoot::new("analyzer-bash-background");
    let registry = registry(&safe_bash());
    let tool = bash_tool();
    let foreground = json!({"command": "ls -la"});
    let background = json!({"command": "ls -la", "is_background": true});
    let subject = subject("s", "root", "child", temp.path());
    let context = ctx(&temp.path().to_string_lossy(), "child");

    // L144-149
    let first = analyze(&registry, &tool, &foreground, &context, &subject).expect("foreground");
    let second = analyze(&registry, &tool, &background, &context, &subject).expect("background");
    assert_ne!(first.operation_hash, second.operation_hash);
}

/// 旧源 `OperationAnalyzerRegistryTest.java:152-175`
/// `reusableBashIdentityIsStableAcrossRepeatedAnalysis`。
#[test]
fn reusable_bash_identity_is_stable_across_repeated_analysis() {
    // L154-164（OAR-02：端口只回已过滤的继承变量名）
    let temp = TempRoot::new("analyzer-bash-env");
    let bash = safe_bash();
    bash.inherit(&["HOME"]);
    let registry = registry(&bash);
    let session = format!("auth-env-{}", uuid::Uuid::new_v4());
    let tool = bash_tool();
    let input = json!({"command": "printf '%s' \"$HOME\""});
    let subject = subject(&session, "root", "root", temp.path());
    let context = ctx(&temp.path().to_string_lossy(), &session);

    // L166-169
    let first = analyze(&registry, &tool, &input, &context, &subject).expect("first");
    let second = analyze(&registry, &tool, &input, &context, &subject).expect("second");

    // L171-174
    assert_eq!(second.operation_hash, first.operation_hash);
    recheck(&registry, &tool, &first, &input, &context, &subject).expect("recheck passes");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:177-192`
/// `absoluteCommandBlacklistCannotBeOverriddenByOnceInteraction`。
#[test]
fn absolute_command_blacklist_cannot_be_overridden_by_once_interaction() {
    // L179-183：`command-blacklist-deny` 即 ABSOLUTE_DENY 入口。
    let temp = TempRoot::new("analyzer-blacklist");
    let bash = safe_bash();
    bash.set(BashParseOutcome::BlacklistDeny {
        reason: "disk destruction".to_owned(),
    });
    let registry = registry(&bash);
    let input = json!({"command": "dd of=/dev/disk0"});

    // L185-191
    let denied = analyze(
        &registry,
        &bash_tool(),
        &input,
        &ctx(&temp.path().to_string_lossy(), "s"),
        &subject("s", "r", "r", temp.path()),
    )
    .expect_err("blacklisted command must be denied during analysis");
    assert!(
        denied.message.contains("disk destruction"),
        "message carries the blacklist reason: {}",
        denied.message
    );
    assert_eq!(denied.code, "COMMAND_ABSOLUTELY_DENIED");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:194-259`
/// `protectedFileIsHighRiskAndOutsideFileIsGuarded`。
#[test]
fn protected_file_is_high_risk_and_outside_file_is_guarded() {
    // L197-212（OAR-03：授权根改用 `temp/ws`，外部文件放 `temp/outside.txt`）
    let temp = TempRoot::new("analyzer-protected");
    let workspace = dir(temp.path(), "ws");
    let protected_file = file(&workspace.join(".env.production"), "TOKEN=secret");
    let outside = file(&temp.path().join("outside.txt"), "outside");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");

    // L213-234：受保护文件 HIGH，且复检通过（事实未变）。
    let read = FakeTool::new("Read").path_from("file_path");
    let protected_input = json!({"file_path": protected_file.to_string_lossy()});
    let protected_operation =
        analyze(&registry, &read, &protected_input, &context, &subject).expect("analyze protected");
    assert_eq!(protected_operation.risk, RiskClass::High);
    recheck(
        &registry,
        &read,
        &protected_operation,
        &protected_input,
        &context,
        &subject,
    )
    .expect("unchanged protected file passes recheck");

    // L236-258：工作区外文件 GUARDED，资源用规范绝对路径且标记 outside。
    let outside_input = json!({"file_path": outside.to_string_lossy()});
    let outside_operation =
        analyze(&registry, &read, &outside_input, &context, &subject).expect("analyze outside");
    assert_eq!(outside_operation.risk, RiskClass::Guarded);
    assert_eq!(
        outside_operation.resources,
        vec![ResourceRef::new(
            "path",
            outside.to_string_lossy().as_ref(),
            true
        )]
    );
    assert!(
        outside_operation
            .redacted_summary
            .contains("outside Project"),
        "summary marks the outside target: {}",
        outside_operation.redacted_summary
    );
    assert!(
        outside_operation
            .redacted_summary
            .contains(outside.to_string_lossy().as_ref()),
        "summary carries the canonical path: {}",
        outside_operation.redacted_summary
    );
}

/// 旧源 `OperationAnalyzerRegistryTest.java:260-325`
/// `externalFileGrantIdentityIgnoresOffsetsAndContent`。
#[test]
fn external_file_grant_identity_ignores_offsets_and_content() {
    // L263-279
    let temp = TempRoot::new("analyzer-identity");
    let workspace = dir(temp.path(), "identity-workspace");
    let outside = file(&temp.path().join("identity-outside.txt"), "old");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");

    // L281-295
    let read = FakeTool::new("Read").path_from("file_path");
    let first_read = json!({"file_path": outside.to_string_lossy(), "offset": 0});
    let second_read = json!({"file_path": outside.to_string_lossy(), "offset": 500});
    let write = FakeTool::new("Write").path_from("file_path");
    let first_write = json!({"file_path": outside.to_string_lossy(), "content": "first"});
    let second_write = json!({"file_path": outside.to_string_lossy(), "content": "second"});

    // L301-312
    let read_one = analyze(&registry, &read, &first_read, &context, &subject).expect("read one");
    let read_two = analyze(&registry, &read, &second_read, &context, &subject).expect("read two");
    let write_one =
        analyze(&registry, &write, &first_write, &context, &subject).expect("write one");
    let write_two =
        analyze(&registry, &write, &second_write, &context, &subject).expect("write two");

    // L314-323
    assert_ne!(read_one.input_hash, read_two.input_hash);
    assert_eq!(read_one.operation_hash, read_two.operation_hash);
    assert_ne!(write_one.input_hash, write_two.input_hash);
    assert_eq!(write_one.operation_hash, write_two.operation_hash);
    assert_eq!(read_one.risk, RiskClass::Guarded);
    assert_eq!(write_one.risk, RiskClass::Guarded);
}

/// 旧源 `OperationAnalyzerRegistryTest.java:327-376`
/// `recursiveProtectedRootIsHighRiskWithoutUpgradingProjectRoot`。
#[test]
fn recursive_protected_root_is_high_risk_without_upgrading_project_root() {
    // L330-341
    let temp = TempRoot::new("analyzer-recursive");
    let workspace = dir(temp.path(), "recursive-risk-workspace");
    let protected_directory = dir(&workspace, ".ssh");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");

    // L342-351：Grep 自身不报路径，靠 `path` 入参（旧 mock 未 stub `getPath`）。
    let grep = FakeTool::new("Grep");
    let broad_input = json!({"pattern": "secret", "path": workspace.to_string_lossy()});
    let protected_input =
        json!({"pattern": "secret", "path": protected_directory.to_string_lossy()});

    // L356-374
    let broad = analyze(&registry, &grep, &broad_input, &context, &subject).expect("broad");
    let protected_root =
        analyze(&registry, &grep, &protected_input, &context, &subject).expect("protected root");
    assert_eq!(broad.risk, RiskClass::Safe);
    assert_eq!(protected_root.risk, RiskClass::High);
    recheck(
        &registry,
        &grep,
        &protected_root,
        &protected_input,
        &context,
        &subject,
    )
    .expect("unchanged recursive root passes recheck");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:378-437`
/// `directCredentialDirectoriesAreHighButDevelopmentDirectoriesAreOrdinary`。
#[test]
fn direct_credential_directories_are_high_but_development_directories_are_ordinary() {
    // L381-393
    let temp = TempRoot::new("analyzer-directories");
    let workspace = dir(temp.path(), "directory-risk-workspace");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");
    let read = FakeTool::new("Read").path_from("file_path");

    // L395-415：凭据目录直接命中 HIGH。
    for directory in [
        ".git",
        ".ssh",
        ".aws",
        ".kube",
        ".docker",
        ".gnupg",
        ".ai-code-assistant",
    ] {
        let target = file(&dir(&workspace, directory).join("ordinary-name"), "secret");
        let input = json!({"file_path": target.to_string_lossy()});
        let operation = analyze(&registry, &read, &input, &context, &subject).expect(directory);
        assert_eq!(operation.risk, RiskClass::High, "{directory}");
    }

    // L417-436：开发目录保持普通（SAFE）。
    for directory in [".vscode", ".idea", "node_modules", ".local"] {
        let target = file(
            &dir(&workspace, directory).join("ordinary-name"),
            "ordinary",
        );
        let input = json!({"file_path": target.to_string_lossy()});
        let operation = analyze(&registry, &read, &input, &context, &subject).expect(directory);
        assert_eq!(operation.risk, RiskClass::Safe, "{directory}");
    }
}

/// 旧源 `OperationAnalyzerRegistryTest.java:439-473`
/// `sensitiveAncestorOutsideSelectedProjectDoesNotUpgradeOrdinaryFile`。
#[test]
fn sensitive_ancestor_outside_selected_project_does_not_upgrade_ordinary_file() {
    // L442-461：`.ssh` 是被选中项目的**祖先**，不应升级项目内普通文件。
    let temp = TempRoot::new("analyzer-ancestor");
    let sensitive_ancestor = dir(temp.path(), ".ssh");
    let workspace = dir(&sensitive_ancestor, "selected-project");
    let target = file(&workspace.join("ordinary.txt"), "ordinary");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");
    let read = FakeTool::new("Read").path_from("file_path");
    let input = json!({"file_path": target.to_string_lossy()});

    // L465-472
    let operation = analyze(&registry, &read, &input, &context, &subject).expect("analyze");
    assert_eq!(operation.risk, RiskClass::Safe);
}

/// 旧源 `OperationAnalyzerRegistryTest.java:474-522`
/// `finalFileRecheckRejectsSymlinkReplacement`。
#[test]
fn final_file_recheck_rejects_symlink_replacement() {
    // L477-497
    let temp = TempRoot::new("analyzer-symlink-recheck");
    let workspace = dir(temp.path(), "recheck-workspace");
    let outside = dir(temp.path(), "recheck-outside");
    let secret = file(&outside.join("secret.txt"), "secret");
    let target = workspace.join("target.txt");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");
    let read = FakeTool::new("Read").path_from("file_path");
    let input = json!({"file_path": target.to_string_lossy()});

    // L499-520：批准后把目标改绑到工作区外的机密文件 → 复检必须拒绝。
    let descriptor = analyze(&registry, &read, &input, &context, &subject).expect("analyze");
    std::os::unix::fs::symlink(&secret, &target).expect("symlink swap");
    let denied = recheck(&registry, &read, &descriptor, &input, &context, &subject)
        .expect_err("swapped target must fail the recheck");
    assert_eq!(denied.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:523-580`
/// `boundExecutionInputCannotBeRedirectedByAliasRebound`。
#[test]
fn bound_execution_input_cannot_be_redirected_by_alias_rebound() {
    // L526-550
    let temp = TempRoot::new("analyzer-bound-input");
    let workspace = dir(temp.path(), "bound-input-workspace");
    let first = file(&workspace.join("first.txt"), "first");
    let second = file(&workspace.join("second.txt"), "second");
    let alias = workspace.join("alias.txt");
    std::os::unix::fs::symlink(&first, &alias).expect("alias symlink");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");
    let read = FakeTool::new("Read").path_from("file_path");
    let input = json!({"file_path": alias.to_string_lossy(), "offset": 7});

    // L552-563：绑定后执行输入指向已批准的规范目标，其余参数逐字保留。
    let descriptor = analyze(&registry, &read, &input, &context, &subject).expect("analyze");
    let bound =
        OperationAnalyzerRegistry::bind_execution_input(&read, &descriptor, &input, &subject)
            .expect("file operations bind their canonical target");
    assert_eq!(
        bound.get("file_path").and_then(Value::as_str),
        Some(first.to_string_lossy().as_ref())
    );
    assert_eq!(bound.get("offset").and_then(Value::as_i64), Some(7));

    // L565-577：把别名改绑到另一个文件后，已绑定输入仍指向原目标，复检通过。
    std::fs::remove_file(&alias).expect("delete alias");
    std::os::unix::fs::symlink(&second, &alias).expect("rebound alias");
    recheck(&registry, &read, &descriptor, &bound, &context, &subject)
        .expect("bound input is immune to the alias rebound");
    let bound_path = bound
        .get("file_path")
        .and_then(Value::as_str)
        .expect("bound path");
    assert_eq!(
        std::fs::read_to_string(Path::new(bound_path)).expect("read bound target"),
        "first"
    );
}

/// 旧源 `OperationAnalyzerRegistryTest.java:581-622`
/// `fileAnalyzerRejectsUncBeforePathCanonicalization`。
#[test]
fn file_analyzer_rejects_unc_before_path_canonicalization() {
    // L584-598
    let temp = TempRoot::new("analyzer-unc");
    let workspace = dir(temp.path(), "unc-analyzer-workspace");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");
    let read = FakeTool::new("Read").path_from("file_path");
    let input = json!({"file_path": "//attacker.invalid/share/secret.txt"});

    // L600-620：UNC 必须在任何规范化之前被拒（NTLM 凭据外泄防护）。
    let denied = analyze(&registry, &read, &input, &context, &subject)
        .expect_err("UNC path must be denied during analysis");
    assert_eq!(denied.code, "PROTECTED_PATH_DENIED");
    assert!(
        denied.message.contains("UNC path access denied"),
        "message states the UNC rejection: {}",
        denied.message
    );
}

/// 旧源 `OperationAnalyzerRegistryTest.java:623-673`
/// `finalFileRecheckRejectsProtectedSymlinkReplacement`。
#[test]
fn final_file_recheck_rejects_protected_symlink_replacement() {
    // L626-647
    let temp = TempRoot::new("analyzer-protected-recheck");
    let workspace = dir(temp.path(), "protected-recheck-workspace");
    let protected_file = file(&workspace.join(".env"), "TOKEN=secret");
    let target = file(&workspace.join("target.txt"), "ordinary");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");
    let read = FakeTool::new("Read").path_from("file_path");
    let input = json!({"file_path": target.to_string_lossy()});

    // L649-671：批准的是普通文件（SAFE），改绑到受保护文件后复检必须拒绝。
    let descriptor = analyze(&registry, &read, &input, &context, &subject).expect("analyze");
    assert_eq!(descriptor.risk, RiskClass::Safe);
    std::fs::remove_file(&target).expect("delete target");
    std::os::unix::fs::symlink(&protected_file, &target).expect("symlink swap");
    let denied = recheck(&registry, &read, &descriptor, &input, &context, &subject)
        .expect_err("protected swap must fail the recheck");
    assert_eq!(denied.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:776-835`
/// `finalFileRecheckBindsApprovedCanonicalTargetEvenWhenRiskDecreases`。
#[test]
fn final_file_recheck_binds_approved_canonical_target_even_when_risk_decreases() {
    // L779-800
    let temp = TempRoot::new("analyzer-decreased-recheck");
    let workspace = dir(temp.path(), "decreased-recheck-workspace");
    let protected_file = file(&workspace.join(".env"), "TOKEN=secret");
    let target = workspace.join("target.txt");
    std::os::unix::fs::symlink(&protected_file, &target).expect("alias to protected file");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");
    let read = FakeTool::new("Read").path_from("file_path");
    let input = json!({"file_path": target.to_string_lossy()});

    // L802-811：批准的规范目标是那个受保护文件（HIGH），事实未变时复检通过。
    let descriptor = analyze(&registry, &read, &input, &context, &subject).expect("analyze");
    assert_eq!(descriptor.risk, RiskClass::High);
    recheck(&registry, &read, &descriptor, &input, &context, &subject)
        .expect("unchanged binding passes recheck");

    // L813-833：风险**下降**（改成普通文件）也算身份漂移，必须拒绝。
    std::fs::remove_file(&target).expect("delete alias");
    std::fs::write(&target, "ordinary").expect("write ordinary target");
    let denied = recheck(&registry, &read, &descriptor, &input, &context, &subject)
        .expect_err("identity drift must fail even when risk decreases");
    assert_eq!(denied.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}

/// 旧源 `OperationAnalyzerRegistryTest.java:836-900`
/// `scratchpadSensitiveSymlinkAliasesRemainHighRisk`。
#[test]
fn scratchpad_sensitive_symlink_aliases_remain_high_risk() {
    // L838-861
    let temp = TempRoot::new("analyzer-scratchpad-alias");
    let workspace = dir(temp.path(), "scratchpad-alias-workspace");
    let scratchpad = dir(&workspace, ".zk/scratchpad");
    let ordinary_directory = dir(&scratchpad, "ordinary");
    let ordinary_file = file(&ordinary_directory.join("notes.txt"), "notes");
    let env_alias = scratchpad.join(".envrc");
    std::os::unix::fs::symlink(&ordinary_file, &env_alias).expect("env alias");
    let ssh_alias = scratchpad.join(".ssh");
    std::os::unix::fs::symlink(&ordinary_directory, &ssh_alias).expect("ssh alias");
    let registry = registry(&safe_bash());
    let subject = subject("s", "run", "run", &workspace);
    let context = ctx(&workspace.to_string_lossy(), "s");

    // L863-885：敏感**名字**的别名（无论读写）一律 HIGH。
    for tool_name in ["Read", "Write"] {
        let tool = FakeTool::new(tool_name).path_from("file_path");
        for requested in [env_alias.clone(), ssh_alias.join("notes.txt")] {
            let input = json!({"file_path": requested.to_string_lossy()});
            let descriptor =
                analyze(&registry, &tool, &input, &context, &subject).expect("analyze alias");
            assert_eq!(
                descriptor.risk,
                RiskClass::High,
                "{tool_name} {}",
                requested.display()
            );
        }
    }

    // L887-899：普通名字的别名仍是 SAFE。
    let ordinary_alias = scratchpad.join("notes-link.txt");
    std::os::unix::fs::symlink(&ordinary_file, &ordinary_alias).expect("ordinary alias");
    let read = FakeTool::new("Read").path_from("file_path");
    let ordinary_input = json!({"file_path": ordinary_alias.to_string_lossy()});
    let descriptor =
        analyze(&registry, &read, &ordinary_input, &context, &subject).expect("analyze ordinary");
    assert_eq!(descriptor.risk, RiskClass::Safe);
}
