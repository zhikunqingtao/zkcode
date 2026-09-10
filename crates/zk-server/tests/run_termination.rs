//! Unified Run cancellation integration tests.
//!
//! Cancellation is deliberately two-phase: the server coordinator durably
//! requests Task/Run cancellation and signals the registered execution token;
//! only the execution owner may publish the immutable result and terminal state.

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use zk_authz::interaction::{InteractionRecord, InteractionStatus, InteractionType};
use zk_db::{
    CleanupStatus, CommitTaskResult, Db, ResultStatus, TaskStatus, VerificationStatus, time,
};
use zk_engine::{MessageSink, TaskExecutionLease, TaskRuntime};
use zk_protocol::ServerMessage;
use zk_server::interaction::service::{
    DurableInteractionService, InteractionCreateSpec, NoopInteractionPublisher,
};
use zk_server::interaction::{MAX_WAITING, TransitionResult, runs};
use zk_server::run_termination::{RunTerminationCoordinator, assemble_with_runtime};

#[derive(Debug)]
struct NoopMessageSink;

impl MessageSink for NoopMessageSink {
    fn push<'a>(&'a self, _session_id: &'a str, _message: ServerMessage) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

struct Fixture {
    db: Db,
    tasks: Arc<TaskRuntime>,
    interactions: Arc<DurableInteractionService>,
    terminations: Arc<RunTerminationCoordinator>,
    run_id: String,
    cancel: CancellationToken,
    _execution: TaskExecutionLease,
}

async fn fixture() -> Fixture {
    let db = Db::open_in_memory().expect("in-memory db boots with migrations");
    let run_id = uuid::Uuid::new_v4().to_string();
    {
        let run = run_id.clone();
        db.with_writer(move |conn| {
            let now = time::format_rfc3339_micros(time::now_millis());
            conn.execute(
                "INSERT INTO sessions(id,model,working_dir,created_at,updated_at) \
                 VALUES('s1','known','/tmp',?1,?1)",
                rusqlite::params![now],
            )?;
            runs::start_in_current_write(conn, &run, "s1", None, Some("main"), "known")
        })
        .await
        .expect("run starts");
    }
    let tasks = Arc::new(TaskRuntime::new(db.clone(), Arc::new(NoopMessageSink)));
    let cancel = CancellationToken::new();
    let execution = tasks
        .attach_existing_execution("s1", &run_id, &run_id, cancel.clone())
        .await
        .expect("root execution is attached");
    let (interactions, terminations) =
        assemble_with_runtime(db.clone(), Arc::new(NoopInteractionPublisher), &tasks);
    Fixture {
        db,
        tasks,
        interactions,
        terminations,
        run_id,
        cancel,
        _execution: execution,
    }
}

#[tokio::test]
async fn terminal_commit_race_still_closes_pending_interactions() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "terminal-race"))
        .await
        .expect("interaction");

    fixture
        .tasks
        .cancel_run_with_cause(&fixture.run_id, "userCancelled", "first request")
        .await
        .expect("durable cancellation request");
    commit_execution_result(
        &fixture,
        ResultStatus::Cancelled,
        "execution won the terminal race",
        Some("USER_CANCELLED"),
        CleanupStatus::Confirmed,
    )
    .await;

    let transition = fixture
        .terminations
        .cancel_by_user(&fixture.run_id, Some("idempotent transport retry"))
        .await
        .expect("terminal retry also performs interaction cleanup");
    assert_eq!(transition, TransitionResult::AlreadyTerminal);
    assert_eq!(
        find(&fixture.interactions, &request.interaction_id)
            .await
            .status,
        InteractionStatus::Cancelled
    );
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING);
}

fn elicitation(run_id: &str, correlation_key: &str) -> InteractionCreateSpec {
    InteractionCreateSpec {
        correlation_key: correlation_key.to_owned(),
        session_id: "s1".to_owned(),
        run_id: Some(run_id.to_owned()),
        kind: InteractionType::Elicitation,
        prompt: json!({ "tool": "Bash" }),
        allowed_decisions: vec!["allow".to_owned(), "deny".to_owned()],
        scope_options: vec!["session".to_owned()],
        source: Some("direct".to_owned()),
        child_session_id: None,
    }
}

async fn find(interactions: &DurableInteractionService, id: &str) -> InteractionRecord {
    interactions
        .find_by_id(id)
        .await
        .expect("read interaction")
        .expect("interaction exists")
}

async fn expire_delivery_window(db: &Db, interaction_id: &str) {
    let interaction_id = interaction_id.to_owned();
    db.with_writer(move |conn| {
        conn.execute(
            "UPDATE interaction_requests SET delivery_window_ends_at=?1 \
             WHERE interaction_id=?2",
            rusqlite::params![time::format_rfc3339_micros(0), interaction_id],
        )?;
        Ok(())
    })
    .await
    .expect("rewind delivery window");
}

async fn assert_cancelling(fixture: &Fixture, requested_exit_reason: &str) {
    let run = fixture
        .db
        .find_run_by_id(&fixture.run_id)
        .await
        .expect("read run")
        .expect("run exists");
    let task = fixture
        .db
        .find_runtime_task_by_id(&fixture.run_id)
        .await
        .expect("read task")
        .expect("task exists");
    assert_eq!(run.status, "cancelling");
    assert_eq!(
        run.requested_exit_reason.as_deref(),
        Some(requested_exit_reason)
    );
    assert_eq!(run.cleanup_status, "pending");
    assert_eq!(task.status, TaskStatus::Cancelling);
    assert_eq!(task.cleanup_status, CleanupStatus::Pending);
    assert!(
        fixture.cancel.is_cancelled(),
        "execution token must be signalled"
    );
    assert!(
        fixture
            .db
            .read_task_result(&fixture.run_id, None, 0, 65_536)
            .await
            .expect("result query")
            .is_none(),
        "the cancellation coordinator must not manufacture a terminal result"
    );
}

async fn commit_execution_result(
    fixture: &Fixture,
    status: ResultStatus,
    content: &str,
    error_code: Option<&str>,
    cleanup_status: CleanupStatus,
) {
    let task = fixture
        .db
        .find_runtime_task_by_id(&fixture.run_id)
        .await
        .expect("read task")
        .expect("task exists");
    fixture
        .db
        .commit_task_result(&CommitTaskResult {
            task_id: fixture.run_id.clone(),
            run_id: fixture.run_id.clone(),
            expected_task_version: task.version,
            status,
            content: content.to_owned(),
            media_type: "text/markdown".to_owned(),
            error_code: error_code.map(str::to_owned),
            cleanup_status,
            verification_status: VerificationStatus::NotRequested,
        })
        .await
        .expect("execution result transaction");
}

#[tokio::test]
async fn user_cancel_signals_runtime_then_executor_commits_cancelled_result() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-user-cancel"))
        .await
        .expect("interaction");

    let transition = fixture
        .terminations
        .cancel_by_user(&fixture.run_id, Some("user pressed stop"))
        .await
        .expect("cancel by user");
    assert_eq!(transition, TransitionResult::Applied);
    assert_cancelling(&fixture, "userCancelled").await;

    let interaction = find(&fixture.interactions, &request.interaction_id).await;
    assert_eq!(interaction.status, InteractionStatus::Cancelled);
    assert_eq!(
        interaction.terminal_reason.as_deref(),
        Some("user pressed stop")
    );
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING);

    commit_execution_result(
        &fixture,
        ResultStatus::Cancelled,
        "cancelled at a safe boundary",
        Some("USER_CANCELLED"),
        CleanupStatus::NotRequired,
    )
    .await;
    let run = fixture
        .db
        .find_run_by_id(&fixture.run_id)
        .await
        .expect("run query")
        .expect("run");
    let task = fixture
        .db
        .find_runtime_task_by_id(&fixture.run_id)
        .await
        .expect("task query")
        .expect("task");
    let result = fixture
        .db
        .read_task_result(&fixture.run_id, None, 0, 65_536)
        .await
        .expect("result query")
        .expect("immutable result");
    assert_eq!(run.status, "cancelled");
    assert_eq!(run.exit_reason.as_deref(), Some("userCancelled"));
    assert_eq!(task.status, TaskStatus::Cancelled);
    assert_eq!(task.cleanup_status, CleanupStatus::NotRequired);
    assert_eq!(result.result.status, ResultStatus::Cancelled);
}

#[tokio::test]
async fn interaction_expiry_cancels_siblings_and_finishes_only_after_error_result() {
    let fixture = fixture().await;
    let doomed = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-doomed"))
        .await
        .expect("first interaction");
    let sibling = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-sibling"))
        .await
        .expect("second interaction");
    expire_delivery_window(&fixture.db, &doomed.interaction_id).await;

    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("deadline sweep");
    assert_cancelling(&fixture, "timeout").await;
    assert_eq!(
        find(&fixture.interactions, &doomed.interaction_id)
            .await
            .status,
        InteractionStatus::Undeliverable
    );
    assert_eq!(
        find(&fixture.interactions, &sibling.interaction_id)
            .await
            .status,
        InteractionStatus::Cancelled
    );
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING);

    commit_execution_result(
        &fixture,
        ResultStatus::Error,
        "delivery_not_acknowledged",
        Some("TIMEOUT"),
        CleanupStatus::NotRequired,
    )
    .await;
    let run = fixture
        .db
        .find_run_by_id(&fixture.run_id)
        .await
        .expect("run query")
        .expect("run");
    let task = fixture
        .db
        .find_runtime_task_by_id(&fixture.run_id)
        .await
        .expect("task query")
        .expect("task");
    let result = fixture
        .db
        .read_task_result(&fixture.run_id, None, 0, 65_536)
        .await
        .expect("result query")
        .expect("result");
    assert_eq!(run.status, "failed");
    assert_eq!(run.exit_reason.as_deref(), Some("timeout"));
    assert_eq!(task.status, TaskStatus::Failed);
    assert_eq!(result.result.status, ResultStatus::Error);
}

#[tokio::test]
async fn unconfirmed_cleanup_is_partial_never_cancelled() {
    let fixture = fixture().await;
    fixture
        .terminations
        .cancel_by_user(&fixture.run_id, Some("stop"))
        .await
        .expect("cancel request");
    assert_cancelling(&fixture, "userCancelled").await;

    commit_execution_result(
        &fixture,
        ResultStatus::Partial,
        "process termination could not be confirmed",
        Some("CLEANUP_UNCONFIRMED"),
        CleanupStatus::Unconfirmed,
    )
    .await;
    let run = fixture
        .db
        .find_run_by_id(&fixture.run_id)
        .await
        .expect("run query")
        .expect("run");
    let task = fixture
        .db
        .find_runtime_task_by_id(&fixture.run_id)
        .await
        .expect("task query")
        .expect("task");
    let result = fixture
        .db
        .read_task_result(&fixture.run_id, None, 0, 65_536)
        .await
        .expect("result query")
        .expect("result");
    assert_eq!(run.status, "completed");
    assert_eq!(run.exit_reason.as_deref(), Some("userCancelled"));
    assert_eq!(task.status, TaskStatus::Partial);
    assert_eq!(task.cleanup_status, CleanupStatus::Unconfirmed);
    assert_eq!(result.result.status, ResultStatus::Partial);

    let retry = fixture
        .terminations
        .cancel_by_user(&fixture.run_id, Some("late retry"))
        .await
        .expect("terminal retry");
    assert_eq!(retry, TransitionResult::AlreadyTerminal);
}
