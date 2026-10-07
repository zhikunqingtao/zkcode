//! Non-LLM MCP roots share `TaskRuntime` lifecycle, budgets and terminal authority.
use futures::future::BoxFuture;
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;
use zk_db::{Db, ResultStatus, TaskBudgetLimits};
use zk_engine::{
    ExternalRootSubmission, MessageSink, TaskExecutionResult, TaskOutputRequest, TaskRuntime,
};
use zk_protocol::ServerMessage;
struct Sink;
impl MessageSink for Sink {
    fn push<'a>(&'a self, _: &'a str, _: ServerMessage) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}
async fn setup(timeout: Duration) -> (TaskRuntime, ExternalRootSubmission) {
    let db = Db::open_in_memory().unwrap();
    let epoch = db.begin_runtime_startup_epoch().await.unwrap();
    let session = db.create_session("qwen3.8-max-0902", "/tmp").await.unwrap();
    let request = ExternalRootSubmission {
        session_id: session.id,
        startup_epoch: epoch,
        timeout,
        budget: TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(2_000_000_000),
            deadline_at_ms: None,
        },
    };
    (TaskRuntime::new(db, Arc::new(Sink)), request)
}
async fn result(runtime: &TaskRuntime, session: &str, task: &str) -> zk_db::TaskResultRecord {
    let result = runtime
        .read_output(TaskOutputRequest {
            root_session_id: session.into(),
            task_id: task.into(),
            wait_ms: 5000,
            result_version: None,
            cursor: 0,
            max_bytes: 4096,
        })
        .await
        .unwrap();
    result.result.expect("durable terminal result").result
}
#[tokio::test]
async fn external_root_is_claimed_before_execution_and_has_one_immutable_result() {
    let (runtime, request) = setup(Duration::from_secs(30)).await;
    let db = runtime.db().clone();
    let session = request.session_id.clone();
    let (tx, rx) = oneshot::channel();
    let receipt = runtime
        .submit_external_root(request, move |context| async move {
            let task = db
                .find_runtime_task_by_id(&context.task_id)
                .await
                .unwrap()
                .unwrap();
            let run = db.find_run_by_id(&context.run_id).await.unwrap().unwrap();
            assert_eq!(task.status.as_db(), "running");
            assert_eq!(run.status, "running");
            assert_eq!(task.task_type, "mcp");
            assert!(task.prompt.is_none());
            assert_eq!(context.budget.cost_limit_nanos_usd, Some(2_000_000_000));
            tx.send(context.run_id).unwrap();
            TaskExecutionResult::complete("connection closed")
        })
        .await
        .unwrap();
    assert_eq!(rx.await.unwrap(), receipt.run_id);
    let first = result(&runtime, &session, &receipt.task.id).await;
    assert_eq!(first.status, ResultStatus::Complete);
    assert_eq!(result(&runtime, &session, &receipt.task.id).await, first);
}
#[tokio::test]
async fn stop_waits_for_the_owning_executor_and_remains_session_scoped() {
    let (runtime, request) = setup(Duration::from_secs(30)).await;
    let session = request.session_id.clone();
    let (tx, rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let receipt = runtime
        .submit_external_root(request, move |context| async move {
            tx.send(()).unwrap();
            context.cancel.cancelled().await;
            let _ = finish_rx.await;
            TaskExecutionResult::Cancelled {
                message: "resource drain finished".into(),
            }
        })
        .await
        .unwrap();
    rx.await.unwrap();
    assert!(
        runtime
            .cancel_owned("foreign-session", &receipt.task.id, "stop")
            .await
            .is_err()
    );
    runtime
        .cancel_owned(&session, &receipt.task.id, "stop")
        .await
        .unwrap();
    assert!(
        runtime
            .db()
            .read_task_result(&receipt.task.id, None, 0, 4096)
            .await
            .unwrap()
            .is_none()
    );
    finish_tx.send(()).unwrap();
    let result = result(&runtime, &session, &receipt.task.id).await;
    assert_eq!(result.status, ResultStatus::Cancelled);
}
#[tokio::test]
async fn deadline_and_invalid_admission_do_not_bypass_the_driver() {
    let (runtime, mut request) = setup(Duration::from_millis(100)).await;
    let session = request.session_id.clone();
    request.startup_epoch = 0;
    assert!(
        runtime
            .submit_external_root(request.clone(), |_| async {
                panic!("invalid request cannot run")
            })
            .await
            .is_err()
    );
    request.startup_epoch = 1;
    let receipt = runtime
        .submit_external_root(request, |context| async move {
            context.cancel.cancelled().await;
            TaskExecutionResult::Cancelled {
                message: "deadline observed".into(),
            }
        })
        .await
        .unwrap();
    let result = result(&runtime, &session, &receipt.task.id).await;
    assert_eq!(result.error_code.as_deref(), Some("TIMEOUT"));
}

#[tokio::test]
async fn repl_service_has_internal_transcript_and_can_coexist_with_conversation_runs() {
    let (runtime, request) = setup(Duration::from_hours(1)).await;
    let session = request.session_id.clone();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let receipt = runtime
        .submit_repl_service(request.clone(), move |context| async move {
            assert_ne!(context.root_session_id, context.transcript_session_id);
            ready_tx.send(context.transcript_session_id).unwrap();
            let _ = finish_rx.await;
            TaskExecutionResult::complete("interpreter stopped")
        })
        .await
        .unwrap();
    let transcript = ready_rx.await.unwrap();
    assert_eq!(receipt.task.task_type, "repl");
    assert_eq!(receipt.transcript_session_id, transcript);
    assert!(
        runtime
            .submit_repl_service(request, |_| async {
                panic!("duplicate service must not start")
            })
            .await
            .is_err()
    );
    let query = uuid::Uuid::new_v4().to_string();
    runtime
        .db()
        .start_run(&query, &session, None, Some("query"), "fixture")
        .await
        .unwrap();
    assert_eq!(
        runtime
            .db()
            .find_latest_root_run_by_session(&session)
            .await
            .unwrap()
            .unwrap()
            .id,
        query
    );
    assert!(
        runtime
            .db()
            .get_session(&session)
            .await
            .unwrap()
            .unwrap()
            .messages
            .is_empty()
    );
    assert!(
        runtime
            .db()
            .list_sessions(None, 100)
            .await
            .unwrap()
            .sessions
            .iter()
            .all(|entry| entry.id != transcript)
    );
    finish_tx.send(()).unwrap();
    assert_eq!(
        result(&runtime, &session, &receipt.task.id).await.status,
        ResultStatus::Complete
    );
    assert!(
        runtime
            .db()
            .get_session(&session)
            .await
            .unwrap()
            .unwrap()
            .messages
            .is_empty()
    );
    assert!(
        !runtime
            .db()
            .get_session(&transcript)
            .await
            .unwrap()
            .unwrap()
            .messages
            .is_empty()
    );
}

#[tokio::test]
async fn ephemeral_session_cannot_create_a_cross_turn_repl_service() {
    let (runtime, mut request) = setup(Duration::from_secs(30)).await;
    let (session, _lease) = runtime
        .db()
        .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    request.session_id = session;
    let error = runtime
        .submit_repl_service(request, |_| async {
            panic!("temporary service must never start")
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "EPHEMERAL_OPERATION_UNSUPPORTED");
}
