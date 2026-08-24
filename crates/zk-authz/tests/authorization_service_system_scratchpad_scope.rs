//! `AuthorizationServiceSystemScratchpadScopeTest.java`（425 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - **SP-01**：旧 fixture 的 `interactions` 是 mock，`createAuthorization`
//!   抛 `PermissionPromptExpected` 表达「本应弹窗」。Rust 侧 `FakeGateway` 默认
//!   脚本是 `PENDING`（没人回答），故「本应弹窗」等价观测为 `authorize_prepared`
//!   返回 `Err` 且 `gateway.prompt_count() == 1`；旧 `verifyNoPrompt` 等价观测为
//!   `prompt_count() == 0`。
//! - **SP-02**：旧 `new SystemScratchpadPathPolicy(scratchpad)` 每个 fixture 换根，
//!   Rust 侧由 `Harness::with_scratchpad(relative)` 提供同一能力。
//! - **SP-03**：旧 fixture 无库；Rust 侧 `FakeGateway` 真落 `interaction_requests`
//!   行，故补一行 `sessions`/`run_envelopes` 种子，并按 PF-04 同因给 context 带上
//!   `currentRunId = "run"`（`interaction_requests.run_id` 两侧都是 `NOT NULL`）。
//! - **SP-04**：`finalGrantRecheckInCurrentTransaction(authorized, context)` 在
//!   Rust 侧显式收 `&Connection`（Spring 的 `@Transactional` 无等价物），故经
//!   `db.with_writer` 调用。

mod common;

use std::path::{Path, PathBuf};

use common::{FakeTool, Harness};
use serde_json::json;
use zk_authz::model::{
    AuthorizationSubject, AuthorizedOperation, AuthzError, DiagnosticSource, EffectClass,
    OperationDescriptor, PermissionMode, PreparedOperation, ResourceRef, RiskClass,
    TypedFileOperation,
};
use zk_authz::tool_facts::ToolUseContext;

/// 旧测试 `createRoots`（L41-47）里的 scratchpad 布局（目录名按 #65 统一为
/// `.zk`，放宽逻辑只认当前目录名）。
const SCRATCHPAD_LAYOUT: &str = "state/.zk/scratchpad";

/// 旧测试反复出现的 `/private/tmp` 目标根（L217-218、L245-246）。
const PRIVATE_TMP: &str = "/private/tmp/zhikuncode-authorization-test";

/// 旧测试私有 `record ToolAction(toolName, action, effect)`（L389-392）。
struct ToolAction(&'static str, TypedFileOperation, EffectClass);

/// 旧测试私有 `fixture(mode, trusted)`（L305-337）。
async fn fixture(mode: PermissionMode, trusted: bool) -> Harness {
    fixture_with_scratchpad(SCRATCHPAD_LAYOUT, mode, trusted).await
}

/// 同上，但换 scratchpad 根（旧 L180 把 scratchpad 改到工作区内后再建 fixture）。
async fn fixture_with_scratchpad(layout: &str, mode: PermissionMode, trusted: bool) -> Harness {
    let harness = Harness::with_scratchpad(layout);
    harness.seed_run("session", "run").await; // SP-03
    harness.modes.set(mode);
    if trusted {
        harness.trust.trust(harness.workspace.clone());
    }
    harness
}

/// 旧 fixture 的 `subject`（L320-321）：`("session","run","run","workspace", workspace)`。
fn subject(harness: &Harness) -> AuthorizationSubject {
    AuthorizationSubject {
        root_session_id: "session".to_owned(),
        root_run_id: "run".to_owned(),
        current_run_id: "run".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: harness.workspace.clone(),
    }
}

/// 旧 `Fixture.authorize` 的 `ToolUseContext.of(authorizationRoot, rootSessionId)`
///（L413-415），按 SP-03 补 `currentRunId`。
fn context(harness: &Harness) -> ToolUseContext {
    ToolUseContext::new(Some("run".to_owned()), None, Some("session".to_owned())).with_shell(
        Some("session".to_owned()),
        Some(harness.workspace.to_string_lossy().to_string()),
    )
}

/// 旧 `descriptor(toolName, action, effect, risk, resources)`（L346-366）。
fn descriptor(
    harness: &Harness,
    tool_name: &str,
    action: TypedFileOperation,
    effect: EffectClass,
    risk: RiskClass,
    resources: &[PathBuf],
) -> OperationDescriptor {
    let refs = resources
        .iter()
        .map(|path| {
            let outside = !path.starts_with(&harness.workspace);
            let value = if outside {
                path.to_string_lossy().to_string()
            } else {
                path.strip_prefix(&harness.workspace)
                    .expect("inside workspace")
                    .to_string_lossy()
                    .to_string()
            };
            ResourceRef::new("path", value, outside)
        })
        .collect();
    OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: tool_name.to_owned(),
        action: action.as_str().to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: "file-v1".to_owned(),
        effects: vec![effect],
        resources: refs,
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "summary".to_owned(),
    }
}

/// 旧 `nonFileDescriptor(toolName, analyzer, effects)`（L368-379）。
fn non_file_descriptor(
    harness: &Harness,
    tool_name: &str,
    analyzer: &str,
    effects: Vec<EffectClass>,
) -> OperationDescriptor {
    OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: tool_name.to_owned(),
        action: "invoke".to_owned(),
        input_hash: "input-hash".to_owned(),
        analyzer_id: analyzer.to_owned(),
        effects,
        resources: vec![ResourceRef::new(
            "path",
            harness
                .scratchpad_root
                .join("session")
                .to_string_lossy()
                .as_ref(),
            true,
        )],
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk: RiskClass::Guarded,
        operation_hash: "operation-hash".to_owned(),
        redacted_summary: "summary".to_owned(),
    }
}

/// 旧 `Fixture.authorize(descriptor)`（L400-419）。
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

/// 旧 `verifyNoPrompt(fixture)`（L381-387）。
fn verify_no_prompt(harness: &Harness) {
    assert_eq!(harness.gateway.prompt_count(), 0);
}

/// 旧 `Fixture.service().finalGrantRecheckInCurrentTransaction(...)`（L203-207）。
async fn final_recheck(harness: &Harness, authorized: AuthorizedOperation) -> AuthzError {
    let service = harness.service.clone();
    let context = context(harness);
    harness
        .db
        .with_writer(move |conn| {
            Ok(service.final_grant_recheck_in_current_transaction(conn, &authorized, &context))
        })
        .await
        .expect("writer")
        .expect_err("revoked project must fail the final admission recheck")
}

/// 旧源 `...SystemScratchpadScopeTest.java:49-74`
/// `ordinaryFileOperationsUseExplicitSystemScratchpadPolicy`。
#[tokio::test]
async fn ordinary_file_operations_use_explicit_system_scratchpad_policy() {
    // L51-59
    for ToolAction(tool_name, action, effect) in [
        ToolAction(
            "Read",
            TypedFileOperation::ReadFile,
            EffectClass::ReadResource,
        ),
        ToolAction(
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
        ),
        ToolAction(
            "Edit",
            TypedFileOperation::PatchFile,
            EffectClass::WriteResource,
        ),
        ToolAction(
            "NotebookEdit",
            TypedFileOperation::PatchFile,
            EffectClass::WriteResource,
        ),
    ] {
        // L60-64
        let harness = fixture(PermissionMode::Default, true).await;
        let note = harness.scratchpad_root.join("session/note.md");
        let operation = descriptor(
            &harness,
            tool_name,
            action,
            effect,
            RiskClass::Guarded,
            &[note],
        );

        // L66-72
        let authorized = authorize(&harness, operation)
            .await
            .expect("scratchpad file operation is policy allowed");
        assert_eq!(authorized.reason_code, "SYSTEM_SCRATCHPAD_SCOPE");
        assert_eq!(authorized.source, DiagnosticSource::Policy);
        verify_no_prompt(&harness);
    }
}

/// 旧源 `...SystemScratchpadScopeTest.java:76-97`
/// `planAllowsScratchpadReadButStillDeniesScratchpadWrite`。
#[tokio::test]
async fn plan_allows_scratchpad_read_but_still_denies_scratchpad_write() {
    // L78-85：PLAN 仍放行 scratchpad 读。
    let read_harness = fixture(PermissionMode::Plan, true).await;
    let note = read_harness.scratchpad_root.join("session/note.md");
    let read = authorize(
        &read_harness,
        descriptor(
            &read_harness,
            "Read",
            TypedFileOperation::ReadFile,
            EffectClass::ReadResource,
            RiskClass::Guarded,
            &[note],
        ),
    )
    .await
    .expect("plan mode still allows scratchpad reads");
    assert_eq!(read.reason_code, "SYSTEM_SCRATCHPAD_SCOPE");
    verify_no_prompt(&read_harness);

    // L87-96：PLAN 拒绝 scratchpad 写。
    let write_harness = fixture(PermissionMode::Plan, true).await;
    let note = write_harness.scratchpad_root.join("session/note.md");
    let denied = authorize(
        &write_harness,
        descriptor(
            &write_harness,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            &[note],
        ),
    )
    .await
    .expect_err("plan mode denies scratchpad writes");
    assert_eq!(denied.code, "PLAN_MODE_EFFECT_DENIED");
    verify_no_prompt(&write_harness);
}

/// 旧源 `...SystemScratchpadScopeTest.java:99-111`
/// `dontAskAllowsOrdinaryScratchpadWriteForTrustedProject`。
#[tokio::test]
async fn dont_ask_allows_ordinary_scratchpad_write_for_trusted_project() {
    // L101
    let harness = fixture(PermissionMode::DontAsk, true).await;

    // L103-106
    let note = harness.scratchpad_root.join("session/note.md");
    let authorized = authorize(
        &harness,
        descriptor(
            &harness,
            "Edit",
            TypedFileOperation::PatchFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            &[note],
        ),
    )
    .await
    .expect("dont-ask still allows trusted scratchpad writes");

    // L108-110
    assert_eq!(authorized.reason_code, "SYSTEM_SCRATCHPAD_SCOPE");
    verify_no_prompt(&harness);
}

/// 旧源 `...SystemScratchpadScopeTest.java:113-127`
/// `dontAskDoesNotTrustScratchpadForUntrustedProjectSubject`。
#[tokio::test]
async fn dont_ask_does_not_trust_scratchpad_for_untrusted_project_subject() {
    // L115：项目未被信任。
    let harness = fixture(PermissionMode::DontAsk, false).await;

    // L117-125
    let note = harness.scratchpad_root.join("session/note.md");
    let denied = authorize(
        &harness,
        descriptor(
            &harness,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            &[note],
        ),
    )
    .await
    .expect_err("untrusted project gets no scratchpad auto-allow");
    assert_eq!(denied.code, "PERMISSION_INTERACTION_REQUIRED");

    // L126
    verify_no_prompt(&harness);
}

/// 旧源 `...SystemScratchpadScopeTest.java:129-138`
/// `highRiskScratchpadFilesStillRequireAOneTimeInteraction`。
#[tokio::test]
async fn high_risk_scratchpad_files_still_require_a_one_time_interaction() {
    // L131
    let harness = fixture(PermissionMode::Default, true).await;

    // L133-137：scratchpad 里的 `.env` 是 HIGH，必须每次弹窗（SP-01）。
    let dotenv = harness.scratchpad_root.join("session/.env");
    authorize(
        &harness,
        descriptor(
            &harness,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::High,
            &[dotenv],
        ),
    )
    .await
    .expect_err("high risk scratchpad write must prompt, and nobody answers");
    assert_eq!(harness.gateway.prompt_count(), 1);
}

/// 旧源 `...SystemScratchpadScopeTest.java:140-163`
/// `lookalikesAndNonOrdinaryFileToolsDoNotUseScratchpadPolicy`。
#[tokio::test]
async fn lookalikes_and_non_ordinary_file_tools_do_not_use_scratchpad_policy() {
    // 旧 L142-157 的四个素材各自建一个 fixture（旧循环体 L158 同样每轮新建）。
    for index in 0..4 {
        let harness = fixture(PermissionMode::Default, true).await;
        let operation = match index {
            // L143-146：`scratchpad-evil` 同名前缀骗不过策略。
            0 => {
                let evil = harness
                    .scratchpad_root
                    .parent()
                    .expect("scratchpad parent")
                    .join("scratchpad-evil/note.md");
                descriptor(
                    &harness,
                    "Read",
                    TypedFileOperation::ReadFile,
                    EffectClass::ReadResource,
                    RiskClass::Guarded,
                    &[evil],
                )
            }
            // L147-149：Glob 的 `LIST_DIRECTORY` 不是「普通文件操作」。
            1 => {
                let session = harness.scratchpad_root.join("session");
                descriptor(
                    &harness,
                    "Glob",
                    TypedFileOperation::ListDirectory,
                    EffectClass::ReadResource,
                    RiskClass::Guarded,
                    &[session],
                )
            }
            // L150-152：Grep 同理。
            2 => {
                let session = harness.scratchpad_root.join("session");
                descriptor(
                    &harness,
                    "Grep",
                    TypedFileOperation::ListDirectory,
                    EffectClass::ReadResource,
                    RiskClass::Guarded,
                    &[session],
                )
            }
            // L153-155：非 file-v1 分析器（Bash）。
            3 => non_file_descriptor(
                &harness,
                "Bash",
                "bash-v2",
                vec![EffectClass::Process, EffectClass::WriteResource],
            ),
            _ => unreachable!(),
        };

        // L160-161
        authorize(&harness, operation)
            .await
            .expect_err("no scratchpad auto-allow, so it must prompt");
        assert_eq!(harness.gateway.prompt_count(), 1);
    }
}

/// 旧源 `...SystemScratchpadScopeTest.java:153-162` 的
/// `nonFileDescriptor("Worktree","generic-v1",[CONTROL_PLANE])` 分支。
///
/// 与上一个测试同属旧 `lookalikesAndNonOrdinaryFileToolsDoNotUseScratchpadPolicy`
/// 循环，拆开只为让每格断言独立可见（旧 L156-157）。
#[tokio::test]
async fn control_plane_tools_do_not_use_scratchpad_policy() {
    let harness = fixture(PermissionMode::Default, true).await;
    let operation = non_file_descriptor(
        &harness,
        "Worktree",
        "generic-v1",
        vec![EffectClass::ControlPlane],
    );

    authorize(&harness, operation)
        .await
        .expect_err("control plane tool must prompt");
    assert_eq!(harness.gateway.prompt_count(), 1);
}

/// 旧源 `...SystemScratchpadScopeTest.java:165-176`
/// `everyResourceMustRemainInsideSystemScratchpad`。
#[tokio::test]
async fn every_resource_must_remain_inside_system_scratchpad() {
    // L167
    let harness = fixture(PermissionMode::Default, true).await;

    // L168-172：一条资源在 scratchpad 内、一条在外 → 整体不适用策略。
    let inside = harness.scratchpad_root.join("session/note.md");
    let outside = harness
        .workspace
        .parent()
        .expect("temp root")
        .join("outside.txt");
    let operation = descriptor(
        &harness,
        "Write",
        TypedFileOperation::ReplaceFile,
        EffectClass::WriteResource,
        RiskClass::Guarded,
        &[inside, outside],
    );

    // L174-175
    authorize(&harness, operation)
        .await
        .expect_err("mixed resources must prompt");
    assert_eq!(harness.gateway.prompt_count(), 1);
}

/// 旧源 `...SystemScratchpadScopeTest.java:178-191`
/// `relativeProjectResourceCanStillMatchConfiguredSystemRoot`。
#[tokio::test]
async fn relative_project_resource_can_still_match_configured_system_root() {
    // L180-181：scratchpad 根改到工作区内（资源因此是相对路径）。
    let harness =
        fixture_with_scratchpad("workspace/.zk/scratchpad", PermissionMode::Default, true).await;
    let note = harness.scratchpad_root.join("session/note.md");
    assert!(note.starts_with(&harness.workspace), "resource is relative");

    // L183-186
    let authorized = authorize(
        &harness,
        descriptor(
            &harness,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            &[note],
        ),
    )
    .await
    .expect("relative in-project scratchpad resource still matches");

    // L188-190
    assert_eq!(authorized.reason_code, "SYSTEM_SCRATCHPAD_SCOPE");
    verify_no_prompt(&harness);
}

/// 旧源 `...SystemScratchpadScopeTest.java:193-213`
/// `finalAdmissionRevalidatesSystemScratchpadPolicy`。
#[tokio::test]
async fn final_admission_revalidates_system_scratchpad_policy() {
    // L195-197：信任先真后假。
    let harness = fixture(PermissionMode::Default, true).await;
    harness
        .trust
        .script(harness.workspace.clone(), &[true, false]);

    // L198-201
    let note = harness.scratchpad_root.join("session/note.md");
    let authorized = authorize(
        &harness,
        descriptor(
            &harness,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            &[note],
        ),
    )
    .await
    .expect("first probe still trusts the project");
    assert_eq!(authorized.reason_code, "SYSTEM_SCRATCHPAD_SCOPE");

    // L203-212（SP-04）
    let denied = final_recheck(&harness, authorized).await;
    assert_eq!(denied.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}

/// 旧源 `...SystemScratchpadScopeTest.java:215-241`
/// `ordinaryFileOperationsUsePrivateTmpPolicy`。
#[tokio::test]
async fn ordinary_file_operations_use_private_tmp_policy() {
    // L217-218
    let private_tmp = Path::new(PRIVATE_TMP);
    // L219-227
    for ToolAction(tool_name, action, effect) in [
        ToolAction(
            "Read",
            TypedFileOperation::ReadFile,
            EffectClass::ReadResource,
        ),
        ToolAction(
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
        ),
        ToolAction(
            "Edit",
            TypedFileOperation::PatchFile,
            EffectClass::WriteResource,
        ),
        ToolAction(
            "NotebookEdit",
            TypedFileOperation::PatchFile,
            EffectClass::WriteResource,
        ),
    ] {
        // L228
        let harness = fixture(PermissionMode::Default, true).await;

        // L230-233
        let target = private_tmp.join(format!("{tool_name}.txt"));
        let authorized = authorize(
            &harness,
            descriptor(
                &harness,
                tool_name,
                action,
                effect,
                RiskClass::Guarded,
                &[target],
            ),
        )
        .await
        .expect("private tmp file operation is policy allowed");

        // L235-239
        assert_eq!(authorized.reason_code, "PRIVATE_TMP_FILE_SCOPE");
        assert_eq!(authorized.source, DiagnosticSource::Policy);
        verify_no_prompt(&harness);
    }
}

/// 旧源 `...SystemScratchpadScopeTest.java:243-270`
/// `privateTmpPolicyPreservesPlanAndHighRiskGates`。
#[tokio::test]
async fn private_tmp_policy_preserves_plan_and_high_risk_gates() {
    // L245-246
    let target = PathBuf::from(PRIVATE_TMP).join("note.md");

    // L247-253：PLAN 仍放行 `/private/tmp` 读。
    let plan_read = fixture(PermissionMode::Plan, true).await;
    let read = authorize(
        &plan_read,
        descriptor(
            &plan_read,
            "Read",
            TypedFileOperation::ReadFile,
            EffectClass::ReadResource,
            RiskClass::Guarded,
            std::slice::from_ref(&target),
        ),
    )
    .await
    .expect("plan mode still allows private tmp reads");
    assert_eq!(read.reason_code, "PRIVATE_TMP_FILE_SCOPE");
    verify_no_prompt(&plan_read);

    // L255-263：PLAN 拒绝写。
    let plan_write = fixture(PermissionMode::Plan, true).await;
    let denied = authorize(
        &plan_write,
        descriptor(
            &plan_write,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            std::slice::from_ref(&target),
        ),
    )
    .await
    .expect_err("plan mode denies private tmp writes");
    assert_eq!(denied.code, "PLAN_MODE_EFFECT_DENIED");
    verify_no_prompt(&plan_write);

    // L265-269：HIGH 风险仍必须弹窗。
    let high_risk = fixture(PermissionMode::Default, true).await;
    authorize(
        &high_risk,
        descriptor(
            &high_risk,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::High,
            &[target],
        ),
    )
    .await
    .expect_err("high risk private tmp write must prompt");
    assert_eq!(high_risk.gateway.prompt_count(), 1);
}

/// 旧源 `...SystemScratchpadScopeTest.java:272-281`
/// `privateTmpLookalikeDoesNotUsePrivateTmpPolicy`。
#[tokio::test]
async fn private_tmp_lookalike_does_not_use_private_tmp_policy() {
    // L274
    let harness = fixture(PermissionMode::Default, true).await;

    // L276-280：`/private/tmp-evil` 只是同名前缀。
    authorize(
        &harness,
        descriptor(
            &harness,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            &[PathBuf::from("/private/tmp-evil/note.md")],
        ),
    )
    .await
    .expect_err("lookalike must prompt");
    assert_eq!(harness.gateway.prompt_count(), 1);
}

/// 旧源 `...SystemScratchpadScopeTest.java:283-303`
/// `finalAdmissionRevalidatesPrivateTmpPolicy`。
#[tokio::test]
async fn final_admission_revalidates_private_tmp_policy() {
    // L285-287：信任先真后假。
    let harness = fixture(PermissionMode::Default, true).await;
    harness
        .trust
        .script(harness.workspace.clone(), &[true, false]);

    // L288-291
    let authorized = authorize(
        &harness,
        descriptor(
            &harness,
            "Write",
            TypedFileOperation::ReplaceFile,
            EffectClass::WriteResource,
            RiskClass::Guarded,
            &[PathBuf::from(PRIVATE_TMP).join("note.md")],
        ),
    )
    .await
    .expect("first probe still trusts the project");
    assert_eq!(authorized.reason_code, "PRIVATE_TMP_FILE_SCOPE");

    // L293-302（SP-04）
    let denied = final_recheck(&harness, authorized).await;
    assert_eq!(denied.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}
