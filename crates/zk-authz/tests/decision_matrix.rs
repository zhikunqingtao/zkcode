//! §10.1 授权判定矩阵逐格测试（方案文档 v1.4 L448-455）。
//!
//! 矩阵 6 行 × 1 测试，一一对应，行序即本文件测试序：
//!
//! | # | riskLevel | 授权 scope | operationHash 缓存 | 期望行为 | 测试 |
//! |---|---|---|---|---|---|
//! | 1 | safe | — | — | 不弹窗，直接执行 | [`row1_safe_executes_without_prompt`] |
//! | 2 | guarded | once | 未命中 | 弹窗；仅本次生效，不写缓存 | [`row2_guarded_once_miss_prompts_and_writes_no_grant`] |
//! | 3 | guarded | once | 命中（session 级历史授权） | 不弹窗，留痕执行 | [`row3_guarded_once_hit_skips_prompt_via_session_grant`] |
//! | 4 | guarded | session | 未命中 | 弹窗；授权后写缓存 | [`row4_guarded_session_miss_prompts_then_caches`] |
//! | 5 | guarded | session | 命中 | 不弹窗，留痕执行 | [`row5_guarded_session_hit_skips_prompt`] |
//! | 6 | deny | — | — | 直接拒绝不弹窗，不走交互生命周期 | [`row6_deny_never_enters_interaction_lifecycle`] |
//!
//! 「是否弹窗」的观测点恒为 [`common::FakeGateway::prompt_count`]（等价旧系统
//! `DurableInteractionService.createAuthorization` 调用次数）；「缓存命中」的观测
//! 点恒为 `AuthorizedOperation.source == GRANT` 且 `reason_code == GRANT_MATCH`
//! （旧 `AuthorizationService.java:120-128` 第 4 步）。

mod common;

use common::{FakeTool, Harness};
use serde_json::json;
use zk_authz::model::{DiagnosticSource, PermissionScope, RiskClass};
use zk_authz::tool_facts::{BashParseOutcome, ToolFacts};

/// 建一个「读工作区内文件」的 SAFE 操作素材。
fn safe_read(harness: &Harness) -> (FakeTool, serde_json::Value) {
    let target = harness.workspace.join("inside.txt");
    std::fs::write(&target, b"x").expect("write target");
    (
        FakeTool::new("Read"),
        json!({ "file_path": target.to_string_lossy() }),
    )
}

/// 建一个「读工作区外文件」的 GUARDED 操作素材。
///
/// 工作区外 → `FileAnalyzer#fileRisk` 回 `GUARDED`（`analyzer.rs:969`）；
/// 同时 `capabilityConstraint` 因 `outsideWorkspace` 回 `None`，故 `plan(SESSION)`
/// 落 `EXACT_GUARDED` + `Exact{operationHash}` 约束——即矩阵所述
/// 「operationHash 缓存」的真实形态（`grants.rs:625-640`）。
fn guarded_read(harness: &Harness) -> (FakeTool, serde_json::Value) {
    let outside = harness
        .scratchpad_root
        .parent()
        .expect("temp root")
        .join("outside");
    std::fs::create_dir_all(&outside).expect("outside dir");
    let target = outside.join("target.txt");
    std::fs::write(&target, b"x").expect("write target");
    (
        FakeTool::new("Read"),
        json!({ "file_path": target.to_string_lossy() }),
    )
}

// ── 矩阵第 1 行：safe / — / — → 不弹窗直接执行 ────────────────────────────
#[tokio::test]
async fn row1_safe_executes_without_prompt() {
    let harness = Harness::new();
    harness.seed_run("s-row1", "r-row1").await;
    let (tool, input) = safe_read(&harness);
    let frozen = harness.freeze(tool.name(), &input);
    let context = harness.context("r-row1", "tu-row1", "s-row1");

    let prepared = harness
        .service
        .prepare(&tool, &frozen, &input, &context)
        .await
        .expect("prepare");
    assert_eq!(prepared.descriptor.risk, RiskClass::Safe, "素材必须是 SAFE");

    let authorized = harness
        .service
        .authorize_prepared(&tool, &frozen, input, &context, prepared)
        .await
        .expect("safe read is auto-allowed");

    assert_eq!(harness.gateway.prompt_count(), 0, "SAFE 读不得弹窗");
    assert_eq!(authorized.source, DiagnosticSource::Mode);
    assert_eq!(authorized.reason_code, "SAFE_READ_AUTO");
    assert!(authorized.grant_id.is_none(), "自动放行不写授权记录");
}

// ── 矩阵第 2 行：guarded / once / 未命中 → 弹窗；仅本次生效，不写缓存 ──────
#[tokio::test]
async fn row2_guarded_once_miss_prompts_and_writes_no_grant() {
    let harness = Harness::new();
    harness.seed_run("s-row2", "r-row2").await;
    let (tool, input) = guarded_read(&harness);
    let frozen = harness.freeze(tool.name(), &input);
    let context = harness.context("r-row2", "tu-row2", "s-row2");

    let prepared = harness
        .service
        .prepare(&tool, &frozen, &input, &context)
        .await
        .expect("prepare");
    assert_eq!(
        prepared.descriptor.risk,
        RiskClass::Guarded,
        "素材必须是 GUARDED"
    );
    assert!(
        harness
            .grants
            .find_match(&prepared.subject, &prepared.descriptor)
            .await
            .expect("find_match")
            .is_none(),
        "前置：缓存未命中"
    );
    let subject = prepared.subject.clone();
    let descriptor = prepared.descriptor.clone();
    harness.gateway.allow_once(&descriptor.operation_hash);

    let authorized = harness
        .service
        .authorize_prepared(&tool, &frozen, input, &context, prepared)
        .await
        .expect("user allows once");

    assert_eq!(harness.gateway.prompt_count(), 1, "未命中缓存必须弹窗一次");
    assert_eq!(authorized.source, DiagnosticSource::UserOnce);
    assert_eq!(authorized.reason_code, "USER_APPROVED_ONCE");
    assert!(authorized.grant_id.is_none(), "once 决策不落授权记录");
    assert!(
        harness
            .grants
            .find_match(&subject, &descriptor)
            .await
            .expect("find_match")
            .is_none(),
        "once 决策不得写缓存"
    );
}

// ── 矩阵第 3 行：guarded / once / 命中（session 级历史授权）→ 不弹窗留痕 ────
#[tokio::test]
async fn row3_guarded_once_hit_skips_prompt_via_session_grant() {
    let harness = Harness::new();
    harness.seed_run("s-row3", "r-row3").await;
    let (tool, input) = guarded_read(&harness);
    let frozen = harness.freeze(tool.name(), &input);
    let context = harness.context("r-row3", "tu-row3", "s-row3");

    let prepared = harness
        .service
        .prepare(&tool, &frozen, &input, &context)
        .await
        .expect("prepare");
    // 历史 SESSION 级授权（同 operationHash）先落库。
    let grant_id = harness
        .grants
        .create(
            &prepared.subject,
            &prepared.descriptor,
            Some(PermissionScope::Session),
            None,
        )
        .await
        .expect("seed session grant")
        .expect("grant created");
    // 本次用户若被问会选 once——脚本留在这里，用于证明它根本没被问到。
    harness
        .gateway
        .allow_once(&prepared.descriptor.operation_hash);

    let authorized = harness
        .service
        .authorize_prepared(&tool, &frozen, input, &context, prepared)
        .await
        .expect("grant match allows");

    assert_eq!(harness.gateway.prompt_count(), 0, "同 hash 已授权不得弹窗");
    assert_eq!(authorized.source, DiagnosticSource::Grant);
    assert_eq!(authorized.reason_code, "GRANT_MATCH");
    assert_eq!(authorized.grant_id.as_deref(), Some(grant_id.as_str()));
    assert_eq!(authorized.grant_scope, Some(PermissionScope::Session));
}

// ── 矩阵第 4 行：guarded / session / 未命中 → 弹窗；授权后写缓存 ───────────
#[tokio::test]
async fn row4_guarded_session_miss_prompts_then_caches() {
    let harness = Harness::new();
    harness.seed_run("s-row4", "r-row4").await;
    let (tool, input) = guarded_read(&harness);
    let frozen = harness.freeze(tool.name(), &input);
    let context = harness.context("r-row4", "tu-row4", "s-row4");

    let prepared = harness
        .service
        .prepare(&tool, &frozen, &input, &context)
        .await
        .expect("prepare");
    let subject = prepared.subject.clone();
    let descriptor = prepared.descriptor.clone();
    assert!(
        harness
            .grants
            .find_match(&subject, &descriptor)
            .await
            .expect("find_match")
            .is_none(),
        "前置：缓存未命中"
    );
    harness.gateway.allow_remember(
        &harness.grants,
        &subject,
        &descriptor,
        PermissionScope::Session,
    );

    let authorized = harness
        .service
        .authorize_prepared(&tool, &frozen, input.clone(), &context, prepared)
        .await
        .expect("user allows and remembers");

    assert_eq!(harness.gateway.prompt_count(), 1, "未命中缓存必须弹窗一次");
    assert_eq!(authorized.source, DiagnosticSource::Grant);
    assert_eq!(authorized.reason_code, "USER_REMEMBERED_GRANT");
    assert_eq!(authorized.grant_scope, Some(PermissionScope::Session));
    // 缓存已写入：同 operationHash 再次匹配即命中。
    let cached = harness
        .grants
        .find_match(&subject, &descriptor)
        .await
        .expect("find_match")
        .expect("session grant cached");
    assert_eq!(cached.scope, PermissionScope::Session);
    assert_eq!(
        authorized.grant_id.as_deref(),
        Some(cached.grant_id.as_str())
    );
}

// ── 矩阵第 5 行：guarded / session / 命中 → 不弹窗留痕 ─────────────────────
#[tokio::test]
async fn row5_guarded_session_hit_skips_prompt() {
    let harness = Harness::new();
    harness.seed_run("s-row5", "r-row5").await;
    let (tool, input) = guarded_read(&harness);
    let frozen = harness.freeze(tool.name(), &input);
    let context = harness.context("r-row5", "tu-row5", "s-row5");

    // 第一轮：弹窗 + 记住（SESSION）。
    let first = harness
        .service
        .prepare(&tool, &frozen, &input, &context)
        .await
        .expect("prepare");
    harness.gateway.allow_remember(
        &harness.grants,
        &first.subject,
        &first.descriptor,
        PermissionScope::Session,
    );
    harness
        .service
        .authorize_prepared(&tool, &frozen, input.clone(), &context, first)
        .await
        .expect("first round remembers");
    assert_eq!(harness.gateway.prompt_count(), 1, "第一轮弹窗一次");

    // 第二轮：同 operationHash → 直接命中缓存，弹窗次数不再增长。
    let authorized = harness
        .service
        .authorize(&tool, &frozen, input, &context)
        .await
        .expect("second round hits the cache");

    assert_eq!(
        harness.gateway.prompt_count(),
        1,
        "同 hash 免弹：弹窗次数不增"
    );
    assert_eq!(authorized.source, DiagnosticSource::Grant);
    assert_eq!(authorized.reason_code, "GRANT_MATCH");
    assert_eq!(authorized.grant_scope, Some(PermissionScope::Session));
}

// ── 矩阵第 6 行：deny（ABSOLUTE_DENY / PROTECTED_PATH）→ 直接拒绝不弹窗 ────
#[tokio::test]
async fn row6_deny_never_enters_interaction_lifecycle() {
    let harness = Harness::new();
    harness.seed_run("s-row6", "r-row6").await;
    let context = harness.context("r-row6", "tu-row6", "s-row6");

    // (a) ABSOLUTE_DENY：命令黑名单绝对拒绝，先于风险分级与授权匹配。
    harness.bash.set(BashParseOutcome::BlacklistDeny {
        reason: "Command is absolutely denied".to_owned(),
    });
    let bash = FakeTool::new("Bash");
    let bash_input = json!({ "command": "rm -rf /" });
    let bash_frozen = harness.freeze(bash.name(), &bash_input);
    let denied = harness
        .service
        .authorize(&bash, &bash_frozen, bash_input, &context)
        .await
        .expect_err("ABSOLUTE_DENY is unreachable to bypass");
    assert_eq!(denied.code, "COMMAND_ABSOLUTELY_DENIED");
    assert_eq!(
        harness.gateway.prompt_count(),
        0,
        "绝对拒绝不走交互生命周期"
    );

    // (b) PROTECTED_PATH：设备/内核路径读取直接拒绝。
    let read = FakeTool::new("Read");
    let read_input = json!({ "file_path": "/dev/null" });
    let read_frozen = harness.freeze(read.name(), &read_input);
    let denied = harness
        .service
        .authorize(&read, &read_frozen, read_input, &context)
        .await
        .expect_err("protected path is denied");
    assert_eq!(denied.code, "PROTECTED_PATH_DENIED");
    assert_eq!(
        harness.gateway.prompt_count(),
        0,
        "受保护路径不走交互生命周期"
    );
}

// ── 硬安全不变量：AUTO_APPROVE 也无法绕过 ABSOLUTE_DENY ────────────────────
#[tokio::test]
async fn absolute_deny_outranks_auto_approve_mode() {
    let harness = Harness::new();
    harness.seed_run("s-inv", "r-inv").await;
    harness
        .modes
        .set(zk_authz::model::PermissionMode::AutoApprove);
    harness.bash.set(BashParseOutcome::BlacklistDeny {
        reason: "Command is absolutely denied".to_owned(),
    });
    let bash = FakeTool::new("Bash");
    let input = json!({ "command": "rm -rf /" });
    let frozen = harness.freeze(bash.name(), &input);

    let denied = harness
        .service
        .authorize(&bash, &frozen, input, &context_of(&harness))
        .await
        .expect_err("AUTO_APPROVE must not bypass ABSOLUTE_DENY");

    assert_eq!(denied.code, "COMMAND_ABSOLUTELY_DENIED");
    assert_eq!(harness.gateway.prompt_count(), 0);
}

/// 上一测试的上下文（`s-inv` / `r-inv`）。
fn context_of(harness: &Harness) -> zk_authz::tool_facts::ToolUseContext {
    harness.context("r-inv", "tu-inv", "s-inv")
}

// ── 硬门禁：用户 deny 100% 拦截 ───────────────────────────────────────────
#[tokio::test]
async fn user_denial_blocks_execution() {
    let harness = Harness::new();
    harness.seed_run("s-deny", "r-deny").await;
    let (tool, input) = guarded_read(&harness);
    let frozen = harness.freeze(tool.name(), &input);
    let context = harness.context("r-deny", "tu-deny", "s-deny");
    harness.gateway.deny();

    let denied = harness
        .service
        .authorize(&tool, &frozen, input, &context)
        .await
        .expect_err("user denial blocks the tool");

    assert_eq!(denied.code, "PERMISSION_USER_DENIED");
    assert_eq!(harness.gateway.prompt_count(), 1, "拒绝前必然弹过一次窗");
}
