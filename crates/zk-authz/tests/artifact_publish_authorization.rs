//! `ArtifactPublishAuthorizationTest.java`（98 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录，编号 AP-01）：
//! 旧 `ArtifactPublicationPolicy.Snapshot` 有 14 个字段，其中 `artifactId` /
//! `manifestId` / `runId` / `absolutePath` / `objectKey` / `contentType` 等只在
//! `authorizationFacts` 里参与哈希；Rust [`PublicationSnapshot`] 把它们收敛进
//! `authorization_facts: Value` 一个字段（由 zk-server 适配器负责填充），进入
//! `operationHash` 的字节集合与摘要文本完全一致。

mod common;

use std::sync::Arc;

use common::{FakePublicationPolicy, FakeTool, TempRoot};
use serde_json::json;
use zk_authz::analyzer::OperationAnalyzerRegistry;
use zk_authz::frozen::FrozenToolInputFactory;
use zk_authz::model::{AuthorizationSubject, RiskClass};
use zk_authz::path_security::{PathSecurityService, SystemScratchpadPathPolicy};
use zk_authz::tool_facts::{
    PassthroughFilter, PublicationSnapshot, StatelessShellState, ToolUseContext,
};

/// 旧测试私有 `snapshot(hash, objectKey)`（`ArtifactPublishAuthorizationTest.java:85-91`）。
fn snapshot(hash: &str, object_key: &str) -> PublicationSnapshot {
    PublicationSnapshot {
        relative_path: "report.html".to_owned(),
        size: 42,
        sha256: hash.to_owned(),
        bucket: "test-artifacts".to_owned(),
        public_url: format!("https://test-artifacts.oss-cn-beijing.aliyuncs.com/{object_key}"),
        endpoint: "https://oss-cn-beijing.aliyuncs.com".to_owned(),
        authorization_facts: json!({
            "artifactId": "artifact-1",
            "manifestId": "manifest-1",
            "runId": "run-1",
            "relativePath": "report.html",
            "size": 42,
            "sha256": hash,
            "contentType": "text/html; charset=utf-8",
            "objectKey": object_key,
            "bucket": "test-artifacts",
        }),
    }
}

/// 旧测试私有 `registry(policy)`（`ArtifactPublishAuthorizationTest.java:78-83`）。
fn registry(
    scratchpad_root: &std::path::Path,
    policy: Arc<FakePublicationPolicy>,
) -> OperationAnalyzerRegistry {
    let scratchpads = SystemScratchpadPathPolicy::new(scratchpad_root);
    OperationAnalyzerRegistry::new(
        None,
        Arc::new(PassthroughFilter),
        Arc::new(PathSecurityService::new(scratchpads)),
        Arc::new(StatelessShellState),
    )
    .with_artifact_publication(policy)
}

/// 旧源 `ArtifactPublishAuthorizationTest.java:26-51`
/// `permissionDescriptorFreezesPermanentPublicDestinationAndIntegrityFacts`。
#[test]
fn permission_descriptor_freezes_permanent_public_destination_and_integrity_facts() {
    let temp = TempRoot::new("artifact-publish-freeze");
    let workspace = temp.path().to_path_buf();

    // L28-31：策略只被脚本化为一份「已批准」的事实。
    let policy = Arc::new(FakePublicationPolicy::default());
    policy.script(vec![
        snapshot("hash-a", "object-a"),
        snapshot("hash-a", "object-a"),
    ]);
    let registry = registry(temp.path(), policy);

    // L32-37
    let tool = FakeTool::new("PublishArtifact");
    let input = json!({"file_path": "report.html"});
    let context = ToolUseContext::new(Some("run-1".to_owned()), None, Some("session-1".to_owned()))
        .with_shell(
            Some("session-1".to_owned()),
            Some(workspace.to_string_lossy().to_string()),
        );
    let subject = AuthorizationSubject {
        root_session_id: "session-1".to_owned(),
        root_run_id: "run-1".to_owned(),
        current_run_id: "run-1".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: workspace.clone(),
    };

    // L39-42
    let frozen = FrozenToolInputFactory::with_max_bytes(1024 * 1024)
        .freeze("PublishArtifact", &input)
        .expect("freeze input");
    let kind = registry.analyzer_for(&tool);
    let descriptor = registry
        .analyze(kind, &tool, &frozen, &input, &context, &subject)
        .expect("analyze publication");

    // L44-47
    assert_eq!(descriptor.analyzer_id, "artifact-publish-v1");
    assert_eq!(descriptor.risk, RiskClass::High);
    for fragment in [
        "PERMANENT PUBLIC OSS upload",
        "report.html",
        "test-artifacts",
        "hash-a",
    ] {
        assert!(
            descriptor.redacted_summary.contains(fragment),
            "summary missing {fragment}: {}",
            descriptor.redacted_summary
        );
    }

    // L48-49：事实未变时终局复检放行。
    registry
        .recheck(kind, &tool, &descriptor, &input, &context, &subject)
        .expect("unchanged facts pass final recheck");
}

/// 旧源 `ArtifactPublishAuthorizationTest.java:53-76`
/// `finalRecheckRejectsChangedHashOrObjectDestination`。
#[test]
fn final_recheck_rejects_changed_hash_or_object_destination() {
    let temp = TempRoot::new("artifact-publish-drift");
    let workspace = temp.path().to_path_buf();

    // L55-58：第一次 inspect 回 hash-a/object-a，第二次回 hash-b/object-b。
    let policy = Arc::new(FakePublicationPolicy::default());
    policy.script(vec![
        snapshot("hash-a", "object-a"),
        snapshot("hash-b", "object-b"),
    ]);
    let registry = registry(temp.path(), policy);

    // L59-64
    let tool = FakeTool::new("PublishArtifact");
    let input = json!({"file_path": "report.html"});
    let context = ToolUseContext::new(Some("run-1".to_owned()), None, Some("session-1".to_owned()))
        .with_shell(
            Some("session-1".to_owned()),
            Some(workspace.to_string_lossy().to_string()),
        );
    let subject = AuthorizationSubject {
        root_session_id: "session-1".to_owned(),
        root_run_id: "run-1".to_owned(),
        current_run_id: "run-1".to_owned(),
        workspace_key: "workspace".to_owned(),
        authorization_root: workspace.clone(),
    };

    // L66-69
    let frozen = FrozenToolInputFactory::with_max_bytes(1024 * 1024)
        .freeze("PublishArtifact", &input)
        .expect("freeze input");
    let kind = registry.analyzer_for(&tool);
    let descriptor = registry
        .analyze(kind, &tool, &frozen, &input, &context, &subject)
        .expect("analyze publication");

    // L71-74：哈希/目标漂移必须终局拒绝。
    let failure = registry
        .recheck(kind, &tool, &descriptor, &input, &context, &subject)
        .expect_err("changed publication facts must be denied");
    assert_eq!(failure.code, "AUTHORIZATION_FINAL_RECHECK_DENIED");
}
