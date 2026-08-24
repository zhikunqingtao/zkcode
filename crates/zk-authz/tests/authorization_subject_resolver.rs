//! `AuthorizationSubjectResolverTest.java`（61 行）逐条翻译。
//!
//! 旧测试私有 `database()`（L53-59）自建两张极简表；Rust 侧直接用
//! [`common::Harness`] 的内存库（27 表基线），行内容与旧测试一致。

mod common;

use common::Harness;
use zk_authz::subject::AuthorizationSubjectResolver;

/// 插入一条 `run_envelopes` 行（旧测试 `jdbc.update(...)` 的等价物）。
async fn insert_run(harness: &Harness, run_id: &str, session_id: &str, parent: Option<&str>) {
    let (run_id, session_id) = (run_id.to_owned(), session_id.to_owned());
    let parent = parent.map(str::to_owned);
    harness
        .db
        .with_writer(move |conn| {
            let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
            conn.execute(
                "INSERT INTO run_envelopes(id,session_id,parent_run_id,status,model,\
                   started_at,created_at,updated_at) \
                 VALUES(?1,?2,?3,'running','test-model',?4,?4,?4)",
                rusqlite::params![run_id, session_id, parent, now],
            )?;
            Ok(())
        })
        .await
        .expect("insert run");
}

/// 插入一条 `sessions` 行，`working_dir` 指向 harness 工作区。
async fn insert_session(harness: &Harness, session_id: &str) {
    let session_id = session_id.to_owned();
    let workspace = harness.workspace.to_string_lossy().to_string();
    harness
        .db
        .with_writer(move |conn| {
            let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
            conn.execute(
                "INSERT INTO sessions(id,model,working_dir,created_at,updated_at) \
                 VALUES(?1,'test-model',?2,?3,?3)",
                rusqlite::params![session_id, workspace, now],
            )?;
            Ok(())
        })
        .await
        .expect("insert session");
}

/// 把 `run_id` 的父指向 `parent`（两行都已存在，绕过外键的插入顺序限制）。
async fn link_parent(harness: &Harness, run_id: &str, parent: &str) {
    let (run_id, parent) = (run_id.to_owned(), parent.to_owned());
    harness
        .db
        .with_writer(move |conn| {
            conn.execute(
                "UPDATE run_envelopes SET parent_run_id=?2 WHERE id=?1",
                rusqlite::params![run_id, parent],
            )?;
            Ok(())
        })
        .await
        .expect("link parent");
}

/// 旧源 `AuthorizationSubjectResolverTest.java:16-28`
/// `childUsesPersistedRootSessionAndWorkspace`。
#[tokio::test]
async fn child_uses_persisted_root_session_and_workspace() {
    let harness = Harness::new();
    let resolver = AuthorizationSubjectResolver::new(harness.db.clone());

    // L18-22：sessions('root-session', temp)；run_envelopes root(NULL parent) + child(parent=root)。
    insert_session(&harness, "root-session").await;
    insert_run(&harness, "root", "root-session", None).await;
    insert_run(&harness, "child", "child-session", Some("root")).await;

    // L24-27：子代理主体上溯到根 Run/根会话，工作区取根会话的真实路径。
    let subject = resolver
        .resolve(Some("child"))
        .await
        .expect("child subject resolves");
    assert_eq!(subject.root_run_id, "root");
    assert_eq!(subject.root_session_id, "root-session");
    assert_eq!(subject.current_run_id, "child");
    assert_eq!(subject.authorization_root, harness.workspace);
}

/// 旧源 `AuthorizationSubjectResolverTest.java:30-38`
/// `missingRootSessionFailsClosedEvenWhenRunChainExists`。
#[tokio::test]
async fn missing_root_session_fails_closed_even_when_run_chain_exists() {
    let harness = Harness::new();
    let resolver = AuthorizationSubjectResolver::new(harness.db.clone());

    // L32-33：Run 链完整，但引用的会话不存在。
    insert_run(&harness, "root", "missing", None).await;

    // L35-37：失败关闭，消息含 "Root session"（旧源 L67）。
    let failure = resolver
        .resolve(Some("root"))
        .await
        .expect_err("missing root session must fail closed");
    assert_eq!(failure.code, "AUTHORIZATION_ANCESTRY_INVALID");
    assert!(
        failure.message.contains("Root session"),
        "unexpected message: {}",
        failure.message
    );
}

/// 旧源 `AuthorizationSubjectResolverTest.java:40-51`
/// `missingAndCyclicParentChainsFailClosed`。
#[tokio::test]
async fn missing_and_cyclic_parent_chains_fail_closed() {
    let harness = Harness::new();
    let resolver = AuthorizationSubjectResolver::new(harness.db.clone());

    // L42-43：a→b、b→a 构成环。
    // 旧测试自建的极简表无外键，可直接插入指向尚不存在父 Run 的行；zkcode 用的是
    // 27 表基线（`run_envelopes.parent_run_id`/`session_id` 均有外键），故先各插
    // NULL 父再互指成环——落库后的行内容与旧测试完全一致（偏离表 T-05）。
    insert_session(&harness, "session-a").await;
    insert_session(&harness, "session-b").await;
    insert_run(&harness, "a", "session-a", None).await;
    insert_run(&harness, "b", "session-b", None).await;
    link_parent(&harness, "a", "b").await;
    link_parent(&harness, "b", "a").await;

    // L45-47：环 → "cycle"（旧源 L54）。
    let cyclic = resolver
        .resolve(Some("a"))
        .await
        .expect_err("cyclic chain must fail closed");
    assert_eq!(cyclic.code, "AUTHORIZATION_ANCESTRY_INVALID");
    assert!(
        cyclic.message.contains("cycle"),
        "unexpected message: {}",
        cyclic.message
    );

    // L49-50：Run 不存在 → "missing parent"（旧源 L60）。
    let missing = resolver
        .resolve(Some("missing"))
        .await
        .expect_err("missing run must fail closed");
    assert_eq!(missing.code, "AUTHORIZATION_ANCESTRY_INVALID");
    assert!(
        missing.message.contains("missing parent"),
        "unexpected message: {}",
        missing.message
    );
}
