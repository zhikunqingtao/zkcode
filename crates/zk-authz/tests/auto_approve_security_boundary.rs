//! `AutoApproveSecurityBoundaryTest.java`（168 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - **AB-01**：旧第 3 个测试 `webFetchLoopbackIsDeniedInsideToolAfterAutoApproval`
//!   （L87-108）依赖 `WebFetchTool` 与 `ToolExecutionGateway.execute(...)` 返回
//!   `ToolResult{failureCode="WEB_FETCH_URL_DENIED"}`。zkcode 当前工具族尚无
//!   `WebFetch`（`crates/zk-tools/src` 仅 `file/bash/git/glob/grep/list_dir`），且
//!   网关按 GW-01 只表达准入不执行工具 → 记 **DEFERRED**，随 `WebFetch` 工具迁移
//!   时在 `zk-tools` 侧补测（工具内网禁令属工具自身职责，不属授权链）。
//! - 其余两个测试为「`AUTO_APPROVE` 模式不得绕过硬边界」的核心安全断言，逐字翻译。

mod common;

use common::{FakeTool, Harness};
use serde_json::json;
use zk_authz::model::{DiagnosticSource, PermissionMode};
use zk_authz::tool_facts::ToolFacts;

/// 旧测试私有 `readTool()`（`AutoApproveSecurityBoundaryTest.java:138-144`）：
/// `getPath(input)` 取 `file_path`。
fn read_tool() -> FakeTool {
    FakeTool::new("Read").path_from("file_path")
}

/// 旧测试私有 `fixture()`（L110-136）：AUTO_APPROVE 模式 + 安全 bash 替身。
///
/// `safeBash()`（L146-156）等价物是 `FakeBashSecurity` 的缺省脚本（简单解析、
/// 无环境引用）；本文件只走 file 分析器，故不需额外脚本。
async fn fixture() -> Harness {
    let harness = Harness::new();
    harness.seed_run("session", "run").await;
    harness.modes.set(PermissionMode::AutoApprove);
    harness
}

/// 旧源 `AutoApproveSecurityBoundaryTest.java:44-58`
/// `hardDeniedUncPathIsRejectedBeforeAutoApproval`。
#[tokio::test]
async fn hard_denied_unc_path_is_rejected_before_auto_approval() {
    // L46-49
    let harness = fixture().await;
    let read = read_tool();
    let input = json!({ "file_path": "//attacker.invalid/share/secret.txt" });

    // L51-57：AUTO_APPROVE 模式也不得越过 `prepare` 的硬拒绝层。
    let frozen = harness.freeze(read.name(), &input);
    let denied = harness
        .service
        .prepare(
            &read,
            &frozen,
            &input,
            &harness.context("run", "tool-use", "session"),
        )
        .await
        .expect_err("UNC path must be hard denied");
    assert_eq!(denied.code, "PROTECTED_PATH_DENIED");
}

/// 旧源 `AutoApproveSecurityBoundaryTest.java:60-85`
/// `finalDynamicRecheckRejectsChangedFileTargetAfterAutoApproval`。
#[tokio::test]
async fn final_dynamic_recheck_rejects_changed_file_target_after_auto_approval() {
    // L62-66
    let harness = fixture().await;
    let approved_path = harness.workspace.join("approved.txt");
    std::fs::write(&approved_path, b"approved").expect("write approved");
    let replacement = harness.workspace.join("replacement.txt");
    std::fs::write(&replacement, b"replacement").expect("write replacement");
    let read = read_tool();
    let input = json!({ "file_path": approved_path.to_string_lossy() });
    let context = harness.context("run", "tool-use", "session");

    // L68-74：AUTO_APPROVE 放行。
    let frozen = harness.freeze(read.name(), &input);
    let prepared = harness
        .service
        .prepare(&read, &frozen, &input, &context)
        .await
        .expect("prepare");
    let allowed = harness
        .service
        .authorize_prepared(&read, &frozen, input, &context, prepared)
        .await
        .expect("auto approve authorizes");
    assert_eq!(allowed.source, DiagnosticSource::Mode);
    assert_eq!(allowed.reason_code, "AUTO_APPROVE");

    // L76-77：批准后把目标换成指向别处的软链。
    std::fs::remove_file(&approved_path).expect("delete approved");
    std::os::unix::fs::symlink(&replacement, &approved_path).expect("symlink swap");

    // L79-83：执行前复检必须拒绝。
    let denied = harness
        .service
        .final_dynamic_recheck(&read, &allowed, &context)
        .expect_err("swapped target must fail the final recheck");
    assert_eq!(denied.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}
