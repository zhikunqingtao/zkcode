//! `AuthorizationSubjectResolverTest.java`（61 行）逐条翻译。
//!
//! 旧测试私有 `database()`（L53-59）自建两张极简表；Rust 侧直接用
//! [`common::Harness`] 的最终版内存库，并为每个 Run 创建合法 Task 与 Session
//! 所有权关系。数据库强约束不允许构造悬空 Session 的 Run。

mod common;

use common::Harness;
use zk_authz::subject::AuthorizationSubjectResolver;

/// 插入一条完整、合法的 Task/Run 记录。
///
/// 根 Run 归属于调用方预先创建的根 Session；子 Run 则创建 attached 子 Task
/// 和 internal Session，保持最终版 schema 的 Task/Run/Session 不变量。
async fn insert_run(harness: &Harness, run_id: &str, session_id: &str, parent: Option<&str>) {
    let (run_id, session_id) = (run_id.to_owned(), session_id.to_owned());
    let parent = parent.map(str::to_owned);
    let task_id = uuid::Uuid::new_v4().to_string();
    harness
        .db
        .with_writer(move |conn| {
            let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
            let (task_session_id, parent_task_id, root_task_id) = if let Some(parent_run_id) = &parent
            {
                let (parent_task_id, parent_session_id, root_task_id): (String, String, String) =
                    conn.query_row(
                        "SELECT t.id,t.session_id,t.root_task_id \
                         FROM run_envelopes r JOIN tasks t ON t.id=r.task_id \
                         WHERE r.id=?1",
                        rusqlite::params![parent_run_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )?;
                (parent_session_id, Some(parent_task_id), root_task_id)
            } else {
                (session_id.clone(), None, task_id.clone())
            };

            conn.execute(
                "INSERT INTO tasks(\
                   id,session_id,parent_task_id,root_task_id,current_run_id,description,status,\
                   created_at,updated_at) \
                 VALUES(?1,?2,?3,?4,NULL,'authorization subject test task','running',?5,?5)",
                rusqlite::params![
                    task_id,
                    task_session_id,
                    parent_task_id,
                    root_task_id,
                    now
                ],
            )?;

            if parent.is_some() {
                conn.execute(
                    "INSERT INTO sessions(\
                       id,kind,parent_session_id,parent_task_id,model,working_dir,created_at,updated_at) \
                     SELECT ?1,'internal',?2,?3,model,working_dir,?4,?4 \
                     FROM sessions WHERE id=?2",
                    rusqlite::params![session_id, task_session_id, task_id, now],
                )?;
            }

            conn.execute(
                "INSERT INTO run_envelopes(\
                   id,session_id,task_id,parent_run_id,status,model,started_at,created_at,updated_at) \
                 VALUES(?1,?2,?3,?4,'running','test-model',?5,?5,?5)",
                rusqlite::params![run_id, session_id, task_id, parent, now],
            )?;
            conn.execute(
                "UPDATE tasks SET current_run_id=?2 WHERE id=?1",
                rusqlite::params![task_id, run_id],
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
/// 最终版 schema 对 Session 删除执行级联收敛，Resolver 对已经消失的 Run
/// 仍必须失败关闭。
#[tokio::test]
async fn deleted_root_session_cascades_run_and_resolver_fails_closed() {
    let harness = Harness::new();
    let resolver = AuthorizationSubjectResolver::new(harness.db.clone());

    insert_session(&harness, "root-session").await;
    insert_run(&harness, "root", "root-session", None).await;
    harness
        .db
        .with_writer(|conn| {
            conn.execute(
                "DELETE FROM sessions WHERE id='root-session'",
                rusqlite::params![],
            )?;
            Ok(())
        })
        .await
        .expect("delete root session");

    // Run/Task 已被数据库级联删除，Resolver 不得使用旧身份或放行。
    let failure = resolver
        .resolve(Some("root"))
        .await
        .expect_err("deleted root run must fail closed");
    assert_eq!(failure.code, "AUTHORIZATION_ANCESTRY_INVALID");
    assert!(
        failure.message.contains("missing parent"),
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
    // 最终版 schema 先创建两棵各自合法的根 Task/Run，再仅修改父 Run 指针构造
    // 语义损坏。生产约束保持开启，Resolver 仍须对环失败关闭。
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
