//! Composition of shell tasks through the existing Engine tool boundary.
use crate::{authz::EngineAdmission, state::AppState};
use futures::future::BoxFuture;
use std::sync::Arc;
use zk_engine::{Engine, MessageSink, ToolAdmission};
use zk_protocol::ServerMessage;
use zk_tools::{BashTool, ToolRegistry};

struct ShellSink {
    db: zk_db::Db,
    hub: crate::ws::WsHub,
}
impl MessageSink for ShellSink {
    fn push<'a>(&'a self, source: &'a str, message: ServerMessage) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let session = source.to_owned();
            let route = self
                .db
                .with_reader(move |conn| {
                    conn.query_row(
                        "SELECT COALESCE(parent_session_id,id) FROM sessions WHERE id=?1",
                        [session],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(zk_db::DbError::from)
                })
                .await;
            match route {
                Ok(route) => {
                    self.hub
                        .push_runtime_event(&self.db, &route, source, message)
                        .await;
                }
                Err(error) => {
                    tracing::error!(%error,"shell task event has no durable session route");
                }
            }
        })
    }
}

pub(super) fn build_engine(state: &AppState) -> Arc<Engine> {
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(BashTool));
    let tools = Arc::new(tools);
    let admission: Arc<dyn ToolAdmission> = Arc::new(EngineAdmission::new(
        state.authz.clone(),
        Arc::clone(&tools),
    ));
    Arc::new(
        Engine::sub_session(
            state.db.clone(),
            state.providers.clone(),
            Arc::new(ShellSink {
                db: state.db.clone(),
                hub: state.hub.clone(),
            }),
            tools,
            admission,
            state.costs.clone(),
            state.file_history.clone(),
            state.hooks.clone(),
        )
        .with_run_tool_scopes(Arc::clone(&state.run_tool_scopes))
        .with_task_runtime(Arc::clone(&state.task_runtime))
        .with_execution_supervisor(&state.execution_supervisor)
        .with_observability(Arc::clone(&state.observability)),
    )
}
