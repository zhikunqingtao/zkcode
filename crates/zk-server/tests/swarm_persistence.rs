//! Real file-SQLite + production Router restart evidence for durable Swarm history.

mod common;

use axum::http::StatusCode;
use common::{call, json_body, local_get};
use zk_db::{Db, SwarmRecord};
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;

#[tokio::test]
async fn router_reads_interrupted_swarm_history_after_database_reopen() {
    let path = std::env::temp_dir().join(format!(
        "zk-server-swarm-restart-real-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let db = Db::open(&path).expect("open file SQLite");
    let session = db
        .create_session("model", "/tmp/zk-server-swarm-restart")
        .await
        .expect("session");
    let mut record = SwarmRecord::created("restart-history", &session.id, 2);
    record.phase = String::from("RUNNING");
    record.total_tasks = 2;
    record.active_workers = 2;
    db.save_swarm(&record).await.expect("persist running Swarm");
    drop(db);

    let reopened = Db::open(&path).expect("reopen same file SQLite");
    assert_eq!(
        reopened
            .interrupt_active_swarms()
            .await
            .expect("startup recovery"),
        1
    );
    let mut config = Config::test_config();
    config.swarm_enabled = true;
    let state = AppState::new(reopened, config);
    let mut router = build_router(state);

    let (status, _, body) = call(&mut router, local_get("/api/swarm/restart-history")).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    let value = json_body(&body);
    assert_eq!(value["swarmId"], "restart-history");
    assert_eq!(value["phase"], "INTERRUPTED");
    assert_eq!(value["activeWorkers"], 0);
    assert_eq!(value["totalTasks"], 2);
}
