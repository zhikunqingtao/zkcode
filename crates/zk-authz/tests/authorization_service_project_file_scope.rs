//! `AuthorizationServiceProjectFileScopeTest.java`（326 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - **PF-01**：旧测试全用 Mockito，`interactions` 是 mock，`createAuthorization`
//!   抛 `PermissionPromptExpected` 表达「本应弹窗」。Rust 侧 `FakeGateway` 默认
//!   脚本是 `PENDING`（没人回答），故「本应弹窗」等价观测为
//!   `authorize_prepared` 返回 `Err` 且 `gateway.prompt_count() == 1`；
//!   「不应弹窗」等价观测为 `prompt_count() == 0`（旧 `verify(never())`）。
//! - **PF-02**：`verify(projects, never()).isTrustedFileScope(any())` 由
//!   `FakeWorkspaceTrust::probe_count()` 等价观测。
//! - **PF-03**：旧 fixture 无库；Rust 侧 `FakeGateway` 真落 `interaction_requests`
//!   行，故补一行 `sessions`/`run_envelopes` 种子（不改变任何判定输入）。
//! - **PF-04**：旧 `Fixture.context` 不带 `currentRunId`（mock 的
//!   `createAuthorization` 第三参标注 `nullable(String.class)`）。两侧 schema 的
//!   `interaction_requests.run_id` 都是 `NOT NULL`（旧
//!   `V015_CreateInteractionSchema.java:26`、新
//!   `V2__init_session_message.sql:304`），真落库时 `NULL` 必然违约，故本文件的
//!   context 补 `currentRunId = "run"`（与 `subject.currentRunId` 一致）。该字段
//!   只参与交互行的 `run_id`/相关键查找，不进入任何判定分支。

mod common;

use common::{FakeTool, Harness};
use serde_json::json;
use zk_authz::model::{
    AuthorizationSubject, AuthorizedOperation, AuthzError, DiagnosticSource, EffectClass,
    OperationDescriptor, PermissionMode, PreparedOperation, ResourceRef, RiskClass,
};
use zk_authz::tool_facts::ToolUseContext;

/// 旧测试私有 `descriptor(...)`（`AuthorizationServiceProjectFileScopeTest.java:281-292`）。
fn descriptor(
    tool_name: &str,
    analyzer: &str,
    effects: Vec<EffectClass>,
    risk: RiskClass,
    resources: Vec<ResourceRef>,
) -> OperationDescriptor {
    OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: tool_name.to_owned(),
        action: "invoke".to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: analyzer.to_owned(),
        effects,
        resources,
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "summary".to_owned(),
    }
}

/// 旧测试私有 `fixture(mode, trusted)`（L248-279）。
async fn fixture(mode: PermissionMode, trusted: bool) -> Harness {
    let harness = Harness::new();
    harness.seed_run("session", "run").await; // PF-03
    harness.modes.set(mode);
    if trusted {
        harness.trust.trust(harness.workspace.clone());
    }
    harness
}

/// 旧 `Fixture.subject`（L264-267）：`("session","run","run","workspace", workspace)`。
fn subject(harness: &Harness) -> AuthorizationSubject {
    AuthorizationSubject {
        root_session_id: "session".to_owned(),
        root_run_id: "run".to_owned(),
        current_run_id: "run".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: harness.workspace.clone(),
    }
}

/// 旧 `Fixture.context`（L314-316）：`ToolUseContext.of(authorizationRoot, rootSessionId)`
/// ——**不带** `currentRunId`/`toolUseId`（PF-04：Rust 侧补 `currentRunId`）。
fn context(harness: &Harness) -> ToolUseContext {
    ToolUseContext::new(Some("run".to_owned()), None, Some("session".to_owned())).with_shell(
        Some("session".to_owned()),
        Some(harness.workspace.to_string_lossy().to_string()),
    )
}

/// 旧 `Fixture.authorize(descriptor)`（L300-320）。
async fn authorize(
    harness: &Harness,
    operation: OperationDescriptor,
) -> Result<AuthorizedOperation, AuthzError> {
    let tool = FakeTool::new(&operation.tool_name);
    let input = json!({});
    let frozen = harness.freeze(&operation.tool_name, &input);
    let prepared = PreparedOperation {
        subject: subject(harness),
        descriptor: operation,
        execution_attempt_id: "attempt".to_owned(),
    };
    harness
        .service
        .authorize_prepared(&tool, &frozen, input.clone(), &context(harness), prepared)
        .await
}

/// 工作区内普通写操作素材（旧测试反复出现的 `descriptor("Write"/"Edit", ...)`）。
fn ordinary_write(tool_name: &str) -> OperationDescriptor {
    descriptor(
        tool_name,
        "file-v1",
        vec![EffectClass::WriteResource],
        RiskClass::Guarded,
        vec![ResourceRef::new("path", "src/App.java", false)],
    )
}

/// 旧源 `...ProjectFileScopeTest.java:35-57`
/// `defaultModeAutoAllowsOnlyTrustedOrdinaryFileWrites`。
#[tokio::test]
async fn default_mode_auto_allows_only_trusted_ordinary_file_writes() {
    // L37-43
    let harness = fixture(PermissionMode::Default, true).await;

    // L45-46
    let authorized = authorize(&harness, ordinary_write("Write"))
        .await
        .expect("trusted ordinary write is auto allowed");

    // L48-56
    assert_eq!(authorized.reason_code, "PROJECT_FILE_SCOPE");
    assert_eq!(authorized.source, DiagnosticSource::Policy);
    assert_eq!(harness.gateway.prompt_count(), 0);
}

/// 旧源 `...ProjectFileScopeTest.java:59-74` `planStillDeniesTrustedFileWrites`。
#[tokio::test]
async fn plan_still_denies_trusted_file_writes() {
    // L61-67
    let harness = fixture(PermissionMode::Plan, true).await;

    // L69-73
    let denied = authorize(&harness, ordinary_write("Edit"))
        .await
        .expect_err("plan mode denies writes");
    assert_eq!(denied.code, "PLAN_MODE_EFFECT_DENIED");
}

/// 旧源 `...ProjectFileScopeTest.java:76-95`
/// `dontAskAllowsTrustedProjectFileWritesWithoutInteraction`。
#[tokio::test]
async fn dont_ask_allows_trusted_project_file_writes_without_interaction() {
    // L78-84
    let harness = fixture(PermissionMode::DontAsk, true).await;

    // L86
    let authorized = authorize(&harness, ordinary_write("Edit"))
        .await
        .expect("dont-ask keeps the persisted project scope");

    // L88-94
    assert_eq!(authorized.reason_code, "PROJECT_FILE_SCOPE");
    assert_eq!(harness.gateway.prompt_count(), 0);
}

/// 旧源 `...ProjectFileScopeTest.java:97-113`
/// `dontAskDeniesOrdinaryWritesWhenRootIsNotTrustedProject`。
#[tokio::test]
async fn dont_ask_denies_ordinary_writes_when_root_is_not_trusted_project() {
    // L99-105
    let harness = fixture(PermissionMode::DontAsk, false).await;

    // L107-112
    let denied = authorize(&harness, ordinary_write("Write"))
        .await
        .expect_err("untrusted root cannot auto allow");
    assert_eq!(denied.code, "PERMISSION_INTERACTION_REQUIRED");
}

/// 旧源 `...ProjectFileScopeTest.java:115-145`
/// `planAndDontAskDenyProtectedHighRiskWritesWithoutPrompt`。
#[tokio::test]
async fn plan_and_dont_ask_deny_protected_high_risk_writes_without_prompt() {
    // L117-119
    for mode in [PermissionMode::Plan, PermissionMode::DontAsk] {
        // L120-128
        let harness = fixture(mode, true).await;
        let protected_file = descriptor(
            "Write",
            "file-v1",
            vec![EffectClass::WriteResource],
            RiskClass::High,
            vec![ResourceRef::new(
                "path",
                ".ai-code-assistant/data.db",
                false,
            )],
        );

        // L130-143
        let denied = authorize(&harness, protected_file)
            .await
            .expect_err("protected high risk write is denied");
        assert_eq!(
            denied.code,
            if mode == PermissionMode::Plan {
                "PLAN_MODE_EFFECT_DENIED"
            } else {
                "PERMISSION_INTERACTION_REQUIRED"
            },
            "mode {mode:?}"
        );
        assert_eq!(harness.gateway.prompt_count(), 0, "mode {mode:?}");
    }
}

/// 旧源 `...ProjectFileScopeTest.java:147-175`
/// `planAndDontAskDenyEveryHighRiskAnalyzerWithoutPrompt`。
#[tokio::test]
async fn plan_and_dont_ask_deny_every_high_risk_analyzer_without_prompt() {
    // L149-151
    for mode in [PermissionMode::Plan, PermissionMode::DontAsk] {
        // L152-158
        let harness = fixture(mode, true).await;
        let high_risk_bash = descriptor(
            "Bash",
            "bash-v2",
            vec![EffectClass::Process],
            RiskClass::High,
            vec![ResourceRef::new("cwd", ".", false)],
        );

        // L160-173
        let denied = authorize(&harness, high_risk_bash)
            .await
            .expect_err("high risk bash is denied");
        assert_eq!(
            denied.code,
            if mode == PermissionMode::Plan {
                "PLAN_MODE_EFFECT_DENIED"
            } else {
                "PERMISSION_INTERACTION_REQUIRED"
            },
            "mode {mode:?}"
        );
        assert_eq!(harness.gateway.prompt_count(), 0, "mode {mode:?}");
    }
}

/// 旧源 `...ProjectFileScopeTest.java:177-197` `protectedFilesNeverUseProjectAutoAllow`。
#[tokio::test]
async fn protected_files_never_use_project_auto_allow() {
    // L179-184
    let harness = fixture(PermissionMode::Default, true).await;

    // L186-194：HIGH 风险受保护文件必须走交互（PF-01）。
    let protected_file = descriptor(
        "Write",
        "file-v1",
        vec![EffectClass::WriteResource],
        RiskClass::High,
        vec![ResourceRef::new("path", ".git/config", false)],
    );
    authorize(&harness, protected_file)
        .await
        .expect_err("protected file must prompt, and nobody answers");
    assert_eq!(harness.gateway.prompt_count(), 1);

    // L195-196：Project 信任探测根本不该被问（PF-02）。
    assert_eq!(harness.trust.probe_count(), 0);
}

/// 旧源 `...ProjectFileScopeTest.java:199-219` `nonFileToolsNeverUseProjectAutoAllow`。
#[tokio::test]
async fn non_file_tools_never_use_project_auto_allow() {
    // L201-206
    let harness = fixture(PermissionMode::Default, true).await;

    // L207-216
    let bash = descriptor(
        "Bash",
        "bash-v2",
        vec![EffectClass::Process, EffectClass::WriteResource],
        RiskClass::Guarded,
        vec![ResourceRef::new("cwd", ".", false)],
    );
    authorize(&harness, bash)
        .await
        .expect_err("bash must prompt, and nobody answers");
    assert_eq!(harness.gateway.prompt_count(), 1);

    // L217-218
    assert_eq!(harness.trust.probe_count(), 0);
}

/// 旧源 `...ProjectFileScopeTest.java:221-246` `revokedProjectIsDeniedAtFinalAdmission`。
#[tokio::test]
async fn revoked_project_is_denied_at_final_admission() {
    // L223-231：信任先真后假（旧 `thenReturn(true, false)`）。
    let harness = fixture(PermissionMode::Default, true).await;
    harness
        .trust
        .script(harness.workspace.clone(), &[true, false]);

    // L233
    let authorized = authorize(&harness, ordinary_write("Write"))
        .await
        .expect("first probe still trusts the project");
    assert_eq!(authorized.reason_code, "PROJECT_FILE_SCOPE");

    // L235-245：执行前的最终准入复检必须拒绝已撤销的 Project。
    let service = harness.service.clone();
    let context = context(&harness);
    let denied = harness
        .db
        .with_writer(move |conn| {
            Ok(service.final_grant_recheck_in_current_transaction(conn, &authorized, &context))
        })
        .await
        .expect("writer")
        .expect_err("revoked project must be denied at final admission");
    assert_eq!(denied.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}
