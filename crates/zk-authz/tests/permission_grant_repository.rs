//! `PermissionGrantRepositoryTest.java`（290 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - **PG-T01**：旧 `fixture()`（L262-280）自建两张极简表再跑 V015/V019 迁移；
//!   zkcode 直接用 27 表基线库（`common::Harness`），种子行内容与旧测试逐行一致
//!   （`s-root`/`s-other` 两会话，`r-root`/`r-child`/`r-sibling`/`r-other` 四 Run，
//!   父子关系与 `s-child` 合成会话原样保留）。
//! - **PG-T02**：旧 `repository.supportedScopes(op)` 是实例方法；Rust 侧同名逻辑
//!   为自由函数 `zk_authz::grants::supported_scopes`（`grants.rs:569`），断言等价。

mod common;

use common::Harness;
use zk_authz::grants::supported_scopes;
use zk_authz::model::{
    AuthorizationSubject, EffectClass, OperationDescriptor, PermissionScope, ResourceRef,
    RiskClass, TypedFileOperation,
};
use zk_authz::subject::AuthorizationSubjectResolver;

/// 旧测试私有 `operation(...)`（`PermissionGrantRepositoryTest.java:282-287`）。
fn operation(
    tool: &str,
    action: &str,
    analyzer: &str,
    risk: RiskClass,
    effects: Vec<EffectClass>,
    resources: Vec<ResourceRef>,
    operation_hash: &str,
) -> OperationDescriptor {
    OperationDescriptor {
        authorization_schema_version: 1,
        tool_name: tool.to_owned(),
        action: action.to_owned(),
        input_hash: format!("input-{operation_hash}"),
        analyzer_id: analyzer.to_owned(),
        effects,
        resources,
        inherited_environment_names: Vec::new(),
        endpoints: Vec::new(),
        risk,
        operation_hash: operation_hash.to_owned(),
        redacted_summary: format!("{tool} operation"),
    }
}

/// 旧 `fixture()` 的种子数据（L267-276）。
async fn fixture() -> Harness {
    let harness = Harness::new();
    let workspace = harness.workspace.to_string_lossy().to_string();
    harness
        .db
        .with_writer(move |conn| {
            let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
            for session in ["s-root", "s-other"] {
                conn.execute(
                    "INSERT INTO sessions(id,model,working_dir,created_at,updated_at) \
                     VALUES(?1,'test-model',?2,?3,?3)",
                    rusqlite::params![session, workspace, now],
                )?;
            }
            for (run, session, parent) in [
                ("r-root", "s-root", None),
                ("r-child", "s-child", Some("r-root")),
                ("r-sibling", "s-child", Some("r-root")),
                ("r-other", "s-other", None),
            ] {
                conn.execute(
                    "INSERT INTO run_envelopes(id,session_id,parent_run_id,status,model,\
                       started_at,created_at,updated_at) \
                     VALUES(?1,?2,?3,'running','test-model',?4,?4,?4)",
                    rusqlite::params![run, session, parent, now],
                )?;
            }
            Ok(())
        })
        .await
        .expect("seed fixture");
    harness
}

/// 旧 `AuthorizationSubject` 构造（`(rootSession, rootRun, currentRun, workspaceKey, root)`）。
fn subject(
    harness: &Harness,
    root_session: &str,
    root_run: &str,
    current_run: &str,
    workspace_key: &str,
) -> AuthorizationSubject {
    AuthorizationSubject {
        root_session_id: root_session.to_owned(),
        root_run_id: root_run.to_owned(),
        current_run_id: current_run.to_owned(),
        workspace_key: workspace_key.to_owned(),
        authorization_root: harness.workspace.clone(),
    }
}

/// 旧 `jdbc.queryForObject("SELECT COUNT(*) FROM permission_grants", Integer.class)`。
async fn grant_count(harness: &Harness) -> i64 {
    harness
        .db
        .with_reader(|conn| {
            Ok(
                conn.query_row("SELECT COUNT(*) FROM permission_grants", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
        .expect("count grants")
}

/// 旧源 `PermissionGrantRepositoryTest.java:27-65`
/// `networkAndMcpSessionGrantsIgnoreInputsButRemainToolSpecific`。
// 逐字翻译旧单个 JUnit 用例（L27-65）：拆函数会破坏「一测 ↔ 一旧用例」映射，
// 故此处按 §8 留痕定点放行行数上限。
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn network_and_mcp_session_grants_ignore_inputs_but_remain_tool_specific() {
    let harness = fixture().await;
    // L30-33
    let child = subject(&harness, "s-root", "r-root", "r-child", "workspace");
    let sibling = subject(&harness, "s-root", "r-root", "r-sibling", "workspace");

    // L35-39：network SESSION 授权。
    let web_fetch = operation(
        "WebFetch",
        "network",
        "network-v1",
        RiskClass::Guarded,
        vec![EffectClass::Network],
        Vec::new(),
        "fetch-url-a",
    );
    assert_eq!(
        supported_scopes(&web_fetch),
        vec![PermissionScope::Run, PermissionScope::Session]
    );
    let network_grant = harness
        .grants
        .create(&child, &web_fetch, Some(PermissionScope::Session), None)
        .await
        .expect("create network grant")
        .expect("network grant persisted");

    // L41-46：同工具换输入命中；换工具不命中。
    let other_web_fetch_input = operation(
        "WebFetch",
        "network",
        "network-v1",
        RiskClass::Guarded,
        vec![EffectClass::Network],
        Vec::new(),
        "fetch-url-b",
    );
    assert_eq!(
        harness
            .grants
            .find_match(&sibling, &other_web_fetch_input)
            .await
            .expect("find match")
            .expect("network grant matches other input")
            .grant_id,
        network_grant
    );
    let web_browser = operation(
        "WebBrowser",
        "network",
        "network-v1",
        RiskClass::Guarded,
        vec![EffectClass::Network],
        Vec::new(),
        "browser-url-a",
    );
    assert!(
        harness
            .grants
            .find_match(&sibling, &web_browser)
            .await
            .expect("find match")
            .is_none()
    );

    // L48-53：MCP SESSION 授权。
    let first_mcp = operation(
        "mcp__search__query",
        "invoke",
        "mcp-v1",
        RiskClass::Guarded,
        vec![EffectClass::Unknown],
        Vec::new(),
        "mcp-query-a",
    );
    assert!(
        harness
            .grants
            .find_match(&child, &first_mcp)
            .await
            .expect("find match")
            .is_none()
    );
    assert_eq!(
        supported_scopes(&first_mcp),
        vec![PermissionScope::Run, PermissionScope::Session]
    );
    let mcp_grant = harness
        .grants
        .create(&child, &first_mcp, Some(PermissionScope::Session), None)
        .await
        .expect("create mcp grant")
        .expect("mcp grant persisted");

    // L55-60：同 MCP 工具换输入命中；换 MCP 工具不命中。
    let other_mcp_input = operation(
        "mcp__search__query",
        "invoke",
        "mcp-v1",
        RiskClass::Guarded,
        vec![EffectClass::Unknown],
        Vec::new(),
        "mcp-query-b",
    );
    assert_eq!(
        harness
            .grants
            .find_match(&sibling, &other_mcp_input)
            .await
            .expect("find match")
            .expect("mcp grant matches other input")
            .grant_id,
        mcp_grant
    );
    let other_mcp_tool = operation(
        "mcp__browser__open",
        "invoke",
        "mcp-v1",
        RiskClass::Guarded,
        vec![EffectClass::Unknown],
        Vec::new(),
        "different-mcp-tool",
    );
    assert!(
        harness
            .grants
            .find_match(&sibling, &other_mcp_tool)
            .await
            .expect("find match")
            .is_none()
    );

    // L62-64：非远程分析器不受这两条授权影响。
    let control = operation(
        "Agent",
        "invoke",
        "static-or-remote-v1",
        RiskClass::Guarded,
        vec![EffectClass::ControlPlane],
        Vec::new(),
        "control",
    );
    assert!(
        harness
            .grants
            .find_match(&sibling, &control)
            .await
            .expect("find match")
            .is_none()
    );
}

/// 旧源 `PermissionGrantRepositoryTest.java:67-83`
/// `remoteHighRiskOperationsDoNotOfferOrPersistRememberedGrants`。
#[tokio::test]
async fn remote_high_risk_operations_do_not_offer_or_persist_remembered_grants() {
    let harness = fixture().await;
    // L70-71
    let subject = subject(&harness, "s-root", "r-root", "r-root", "workspace");

    // L73-80
    for operation in [
        operation(
            "WebFetch",
            "network",
            "network-v1",
            RiskClass::High,
            vec![EffectClass::Network],
            Vec::new(),
            "high-network",
        ),
        operation(
            "mcp__admin__delete",
            "invoke",
            "mcp-v1",
            RiskClass::High,
            vec![EffectClass::Unknown],
            Vec::new(),
            "high-mcp",
        ),
    ] {
        assert!(supported_scopes(&operation).is_empty());
        assert!(
            harness
                .grants
                .create(&subject, &operation, Some(PermissionScope::Session), None)
                .await
                .expect("create high risk grant")
                .is_none()
        );
    }

    // L82
    assert_eq!(grant_count(&harness).await, 0);
}

/// 旧源 `PermissionGrantRepositoryTest.java:85-102`
/// `remoteWorkspaceGrantsAreNotSupportedOrPersisted`。
#[tokio::test]
async fn remote_workspace_grants_are_not_supported_or_persisted() {
    let harness = fixture().await;
    // L88-89
    let subject = subject(&harness, "s-root", "r-root", "r-root", "workspace");

    // L91-99
    for operation in [
        operation(
            "WebFetch",
            "network",
            "network-v1",
            RiskClass::Guarded,
            vec![EffectClass::Network],
            Vec::new(),
            "network-workspace",
        ),
        operation(
            "mcp__search__query",
            "invoke",
            "mcp-v1",
            RiskClass::Guarded,
            vec![EffectClass::Unknown],
            Vec::new(),
            "mcp-workspace",
        ),
    ] {
        assert!(!supported_scopes(&operation).contains(&PermissionScope::Workspace));
        assert!(
            harness
                .grants
                .create(&subject, &operation, Some(PermissionScope::Workspace), None)
                .await
                .expect("create workspace grant")
                .is_none()
        );
    }

    // L101
    assert_eq!(grant_count(&harness).await, 0);
}

/// 旧源 `PermissionGrantRepositoryTest.java:104-122`
/// `sessionGrantIsInheritedByDescendantsButDirectRunGrantIsNot`。
#[tokio::test]
async fn session_grant_is_inherited_by_descendants_but_direct_run_grant_is_not() {
    let harness = fixture().await;
    // L107-110
    let bash = operation(
        "Bash",
        "execute",
        "bash-v2",
        RiskClass::Guarded,
        vec![EffectClass::Process, EffectClass::ReadResource],
        Vec::new(),
        "bash-op",
    );
    let child = subject(&harness, "s-root", "r-root", "r-child", "workspace");
    let session_grant = harness
        .grants
        .create(&child, &bash, Some(PermissionScope::Session), None)
        .await
        .expect("create session grant")
        .expect("session grant persisted");

    // L112-113：兄弟 Run 继承同会话授权。
    let sibling = subject(&harness, "s-root", "r-root", "r-sibling", "workspace");
    assert_eq!(
        harness
            .grants
            .find_match(&sibling, &bash)
            .await
            .expect("find match")
            .expect("sibling inherits session grant")
            .grant_id,
        session_grant
    );

    // L115-121：RUN 级授权只对同一 Run 生效。
    let exact_file = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "README.md", false)],
        "root-file",
    );
    let root = subject(&harness, "s-root", "r-root", "r-root", "workspace");
    let direct = harness
        .grants
        .create(&root, &exact_file, Some(PermissionScope::Run), None)
        .await
        .expect("create run grant")
        .expect("run grant persisted");
    assert_eq!(
        harness
            .grants
            .find_match(&root, &exact_file)
            .await
            .expect("find match")
            .expect("same run matches")
            .grant_id,
        direct
    );
    assert!(
        harness
            .grants
            .find_match(&child, &exact_file)
            .await
            .expect("find match")
            .is_none()
    );
}

/// 旧源 `PermissionGrantRepositoryTest.java:124-138`
/// `persistedChildRunWithSyntheticSessionInheritsRootSessionGrant`。
#[tokio::test]
async fn persisted_child_run_with_synthetic_session_inherits_root_session_grant() {
    let harness = fixture().await;
    // L127-132
    let resolver = AuthorizationSubjectResolver::new(harness.db.clone());
    let root = resolver
        .resolve(Some("r-root"))
        .await
        .expect("root subject");
    let child = resolver
        .resolve(Some("r-child"))
        .await
        .expect("child subject");
    assert_eq!(child.root_session_id, root.root_session_id);
    assert_eq!(child.root_run_id, root.root_run_id);
    assert_eq!(child.current_run_id, "r-child");

    // L134-137
    let bash = operation(
        "Bash",
        "execute",
        "bash-v2",
        RiskClass::Guarded,
        vec![EffectClass::Process, EffectClass::ReadResource],
        Vec::new(),
        "inherited-bash",
    );
    let grant_id = harness
        .grants
        .create(&root, &bash, Some(PermissionScope::Session), None)
        .await
        .expect("create session grant")
        .expect("session grant persisted");
    assert_eq!(
        harness
            .grants
            .find_match(&child, &bash)
            .await
            .expect("find match")
            .expect("child inherits root session grant")
            .grant_id,
        grant_id
    );
}

/// 旧源 `PermissionGrantRepositoryTest.java:140-154`
/// `createReturnsTheExactInsertedGrantRatherThanAnotherMatchingScope`。
#[tokio::test]
async fn create_returns_the_exact_inserted_grant_rather_than_another_matching_scope() {
    let harness = fixture().await;
    // L143-148
    let root = subject(&harness, "s-root", "r-root", "r-root", "workspace");
    let bash = operation(
        "Bash",
        "execute",
        "bash-v2",
        RiskClass::Guarded,
        vec![EffectClass::Process, EffectClass::ReadResource],
        Vec::new(),
        "same-bash",
    );
    let run_grant = harness
        .grants
        .create(&root, &bash, Some(PermissionScope::Run), None)
        .await
        .expect("create run grant")
        .expect("run grant persisted");
    let session_grant = harness
        .grants
        .create(&root, &bash, Some(PermissionScope::Session), None)
        .await
        .expect("create session grant")
        .expect("session grant persisted");

    // L150-153
    assert_ne!(session_grant, run_grant);
    let rows: i64 = harness
        .db
        .with_reader(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM (SELECT grant_id,scope FROM permission_grants \
                 WHERE grant_id IN (?1,?2) ORDER BY scope)",
                rusqlite::params![run_grant, session_grant],
                |row| row.get(0),
            )?)
        })
        .await
        .expect("query grants");
    assert_eq!(rows, 2);
}

/// 旧源 `PermissionGrantRepositoryTest.java:156-195`
/// `externalFileGrantsAreExactAndLimitedToRunOrSession`。
#[tokio::test]
async fn external_file_grants_are_exact_and_limited_to_run_or_session() {
    let harness = fixture().await;
    // L159-167
    let subject = subject(&harness, "s-root", "r-root", "r-root", "workspace");
    let outside_path = harness
        .workspace
        .parent()
        .expect("temp root")
        .join("outside.txt");
    let outside = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Guarded,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new(
            "path",
            outside_path.to_string_lossy().as_ref(),
            true,
        )],
        "outside-exact",
    );

    // L169-179：工作区外只允许 RUN/SESSION；WORKSPACE 不落库。
    assert_eq!(
        supported_scopes(&outside),
        vec![PermissionScope::Run, PermissionScope::Session]
    );
    assert!(
        harness
            .grants
            .create(&subject, &outside, Some(PermissionScope::Workspace), None)
            .await
            .expect("create workspace grant")
            .is_none()
    );
    let grant = harness
        .grants
        .create(&subject, &outside, Some(PermissionScope::Session), None)
        .await
        .expect("create session grant")
        .expect("session grant persisted");
    assert_eq!(
        harness
            .grants
            .find_match(&subject, &outside)
            .await
            .expect("find match")
            .expect("exact target matches")
            .grant_id,
        grant
    );

    // L181-188：换目标不命中（EXACT 约束）。
    let other_path = harness
        .workspace
        .parent()
        .expect("temp root")
        .join("other.txt");
    let other_target = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Guarded,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new(
            "path",
            other_path.to_string_lossy().as_ref(),
            true,
        )],
        "outside-other",
    );
    assert!(
        harness
            .grants
            .find_match(&subject, &other_target)
            .await
            .expect("find match")
            .is_none()
    );

    // L190-194：同目标升到 HIGH 后不再提供任何可记住范围。
    let sensitive = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::High,
        vec![EffectClass::ReadResource],
        outside.resources.clone(),
        "outside-sensitive",
    );
    assert!(supported_scopes(&sensitive).is_empty());
}

/// 旧源 `PermissionGrantRepositoryTest.java:197-218`
/// `workspaceCapabilityCrossesSessionsOnlyForSameWorkspaceAndSegment`。
#[tokio::test]
async fn workspace_capability_crosses_sessions_only_for_same_workspace_and_segment() {
    let harness = fixture().await;
    // L200-204
    let read = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "src/main/App.java", false)],
        "read-one",
    );
    let first = subject(&harness, "s-root", "r-root", "r-root", "workspace");
    let grant = harness
        .grants
        .create(&first, &read, Some(PermissionScope::Workspace), None)
        .await
        .expect("create workspace grant")
        .expect("workspace grant persisted");

    // L206-212：同工作区同目录跨会话命中；换 workspaceKey 不命中。
    let same_directory = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "src/main/Other.java", false)],
        "read-two",
    );
    let other_session = subject(&harness, "s-other", "r-other", "r-other", "workspace");
    assert_eq!(
        harness
            .grants
            .find_match(&other_session, &same_directory)
            .await
            .expect("find match")
            .expect("same workspace + segment matches")
            .grant_id,
        grant
    );
    let foreign_workspace = subject(&harness, "s-other", "r-other", "r-other", "other");
    assert!(
        harness
            .grants
            .find_match(&foreign_workspace, &same_directory)
            .await
            .expect("find match")
            .is_none()
    );

    // L214-217：前缀相邻目录不命中（`src/main2` 不是 `src/main` 的子段）。
    let sibling_prefix = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "src/main2/Other.java", false)],
        "read-three",
    );
    assert!(
        harness
            .grants
            .find_match(&other_session, &sibling_prefix)
            .await
            .expect("find match")
            .is_none()
    );
}

/// 旧源 `PermissionGrantRepositoryTest.java:220-240`
/// `rootLevelFileExpandsOnlyToWorkspaceRootDirectory`。
#[tokio::test]
async fn root_level_file_expands_only_to_workspace_root_directory() {
    let harness = fixture().await;
    // L223-230
    let subject = subject(&harness, "s-root", "r-root", "r-root", "workspace");
    let root_file = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "README.md", false)],
        "root-file",
    );
    assert_eq!(
        supported_scopes(&root_file),
        vec![
            PermissionScope::Run,
            PermissionScope::Session,
            PermissionScope::Workspace
        ]
    );
    let grant = harness
        .grants
        .create(&subject, &root_file, Some(PermissionScope::Workspace), None)
        .await
        .expect("create workspace grant")
        .expect("workspace grant persisted");

    // L232-239
    let sibling_root_file = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "LICENSE", false)],
        "root-sibling",
    );
    let nested_file = operation(
        "Read",
        TypedFileOperation::ReadFile.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "src/App.java", false)],
        "nested-file",
    );
    assert_eq!(
        harness
            .grants
            .find_match(&subject, &sibling_root_file)
            .await
            .expect("find match")
            .expect("root sibling matches")
            .grant_id,
        grant
    );
    assert_eq!(
        harness
            .grants
            .find_match(&subject, &nested_file)
            .await
            .expect("find match")
            .expect("nested file matches")
            .grant_id,
        grant
    );
}

/// 旧源 `PermissionGrantRepositoryTest.java:242-260`
/// `directoryListingCapabilityDoesNotExpandToParentDirectory`。
#[tokio::test]
async fn directory_listing_capability_does_not_expand_to_parent_directory() {
    let harness = fixture().await;
    // L245-250
    let subject = subject(&harness, "s-root", "r-root", "r-root", "workspace");
    let listed = operation(
        "Glob",
        TypedFileOperation::ListDirectory.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "src/main", false)],
        "list-main",
    );
    let grant = harness
        .grants
        .create(&subject, &listed, Some(PermissionScope::Workspace), None)
        .await
        .expect("create workspace grant")
        .expect("workspace grant persisted");

    // L252-259
    let descendant = operation(
        "Glob",
        TypedFileOperation::ListDirectory.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "src/main/java", false)],
        "list-child",
    );
    let sibling = operation(
        "Glob",
        TypedFileOperation::ListDirectory.as_str(),
        "file-v1",
        RiskClass::Safe,
        vec![EffectClass::ReadResource],
        vec![ResourceRef::new("path", "src/test", false)],
        "list-sibling",
    );
    assert_eq!(
        harness
            .grants
            .find_match(&subject, &descendant)
            .await
            .expect("find match")
            .expect("descendant directory matches")
            .grant_id,
        grant
    );
    assert!(
        harness
            .grants
            .find_match(&subject, &sibling)
            .await
            .expect("find match")
            .is_none()
    );
}
