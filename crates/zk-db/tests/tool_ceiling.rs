//! Request tool constraints stay authoritative across child creation, recovery and temporary sessions.
use serde_json::json;
use std::collections::BTreeSet;
use zk_db::{CreateTaskWithRun, Db, tool_ceiling::ToolCeiling};

fn policy(allowed: Option<&[&str]>, denied: &[&str]) -> ToolCeiling {
    ToolCeiling {
        allowed: allowed.map(|names| names.iter().map(|name| (*name).into()).collect()),
        denied: denied.iter().map(|name| (*name).into()).collect(),
    }
}
async fn root(db: &Db, session: &str, ceiling: &ToolCeiling) -> String {
    let run = uuid::Uuid::new_v4().to_string();
    db.start_conversation_run_with_policy(
        &run,
        session,
        "fixture",
        Some(&zk_db::TaskBudgetLimits {
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
            ..zk_db::TaskBudgetLimits::default()
        }),
        1,
        ceiling,
    )
    .await
    .unwrap();
    run
}
fn child(session: &str, parent: &str, lifecycle: &str) -> CreateTaskWithRun {
    CreateTaskWithRun {
        task_id: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        root_session_id: session.into(), transcript_session_id: uuid::Uuid::new_v4().to_string(),
        parent_task_id: Some(parent.into()), parent_run_id: Some(parent.into()),
        creator_tool_use_id: Some(uuid::Uuid::new_v4().to_string()), ordinal: 0,
        description: "child".into(), prompt: Some("task".into()), task_type: "agent".into(),
        model: "fixture".into(), working_dir: "/tmp".into(), startup_epoch: 1,
        execution_config_json: json!({"lifecycle":lifecycle,"allowedTools":["Read","Bash","Write"],"disallowedTools":["Write"]}).to_string(),
    }
}
#[test]
fn explicit_empty_invalid_and_intersection_are_not_unrestricted() {
    let restricted = policy(Some(&[]), &[]);
    assert!(!restricted.unrestricted());
    assert!(!restricted.allows("Read"));
    let combined = policy(Some(&["Read", "Bash"]), &["Bash"])
        .intersect(&policy(Some(&["Read", "Write"]), &["Write"]));
    assert_eq!(combined.allowed, Some(BTreeSet::from(["Read".into()])));
    assert!(combined.allows("Read"));
    assert!(!combined.allows("Bash"));
    for invalid in [
        json!({"allowedTools":"Read"}),
        json!({"toolCeiling":{"allowed":[""]}}),
        json!({"disallowedTools":[3]}),
        json!([]),
    ] {
        assert!(ToolCeiling::from_config(&invalid).is_err());
    }
}
#[tokio::test]
async fn atomic_parent_policy_limits_attached_detached_and_replayed_children() {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap().id;
    let run = root(
        &db,
        &session,
        &policy(Some(&["Read", "Bash", "TaskCreate"]), &["Bash"]),
    )
    .await;
    assert!(!db.run_tool_ceiling(&run).await.unwrap().allows("Write"));
    for lifecycle in ["attached", "detached"] {
        let request = child(&session, &run, lifecycle);
        let created = db.create_task_with_run(&request).await.unwrap();
        let frozen = db.run_tool_ceiling(&created.run_id).await.unwrap();
        assert!(frozen.allows("Read"));
        assert!(!frozen.allows("Bash"));
        assert!(!frozen.allows("Write"));
        assert!(!frozen.allows("TaskCreate"));
        let replay = db.create_task_with_run(&request).await.unwrap();
        assert!(!replay.created);
        assert_eq!(created.run_id, replay.run_id);
    }
}
#[tokio::test]
async fn narrowing_is_monotonic_and_survives_reopen() {
    let directory = std::env::temp_dir().join(format!("zk-ceiling-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("db.sqlite");
    let db = Db::open(&path).unwrap();
    let session = db.create_session("fixture", "/tmp").await.unwrap().id;
    let run = root(&db, &session, &policy(None, &["Bash"])).await;
    let narrowed = db
        .narrow_run_tool_ceiling(&run, &policy(Some(&["Read", "Bash"]), &[]))
        .await
        .unwrap();
    assert!(narrowed.allows("Read"));
    assert!(!narrowed.allows("Bash"));
    assert_eq!(
        db.narrow_run_tool_ceiling(&run, &ToolCeiling::default())
            .await
            .unwrap(),
        narrowed
    );
    drop(db);
    let reopened = Db::open(&path).unwrap();
    assert_eq!(reopened.run_tool_ceiling(&run).await.unwrap(), narrowed);
    let derived = reopened
        .create_task_with_run(&child(&session, &run, "attached"))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .run_tool_ceiling(&derived.run_id)
            .await
            .unwrap()
            .allowed,
        Some(BTreeSet::from(["Read".into(), "Bash".into()]))
    );
    assert!(
        !reopened
            .run_tool_ceiling(&derived.run_id)
            .await
            .unwrap()
            .allows("Bash")
    );
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}
#[tokio::test]
async fn temporary_policy_uses_content_store_and_expired_parent_fails_closed() {
    let db = Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    let run = root(
        &db,
        &session,
        &policy(Some(&["Read", "TaskCreate"]), &["Bash"]),
    )
    .await;
    let derived = db
        .create_task_with_run(&child(&session, &run, "attached"))
        .await
        .unwrap();
    assert!(
        db.run_tool_ceiling(&derived.run_id)
            .await
            .unwrap()
            .allows("Read")
    );
    assert!(
        !db.run_tool_ceiling(&derived.run_id)
            .await
            .unwrap()
            .allows("Bash")
    );
    let raw: Vec<String> = db
        .with_reader(|conn| {
            let mut stmt = conn.prepare("SELECT execution_config_json FROM tasks")?;
            Ok(stmt
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()?)
        })
        .await
        .unwrap();
    assert!(
        raw.iter().all(|row| !row.contains("Read")
            && !row.contains("Bash")
            && !row.contains("toolCeiling"))
    );
    drop(lease);
    assert!(db.run_tool_ceiling(&run).await.is_err());
    assert!(
        db.create_task_with_run(&child(&session, &run, "attached"))
            .await
            .is_err()
    );
}
