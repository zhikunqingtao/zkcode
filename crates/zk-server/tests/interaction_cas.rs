//! `DurableInteractionServiceCasTest.java`（301 行，10 测试）的零改写翻译。
//!
//! 旧源用 `@TempDir` + `SqliteConfig` 建真项目库、`V015_CreateInteractionSchema` +
//! `V019_CreateAuthorizationSchema` 建表、`RunControlService.start` 起 Run；此处用
//! [`zk_db::Db::open_in_memory`]（27 表基线迁移已含 `interaction_requests` /
//! `permission_grants` / `run_envelopes` / `run_event_log`）+
//! [`zk_server::interaction::runs::start_in_current_write`]。
//!
//! # 与旧测试的形状偏离（详见 `docs/compatibility.md` §8）
//!
//! - **CAS-01**：旧 `Executors.newVirtualThreadPerTaskExecutor()` / `newFixedThreadPool(2)`
//!   + `CountDownLatch` → `tokio::spawn` + [`tokio::sync::Barrier`]。并发语义
//!     （同时起跑、各自独占写事务）一致。
//! - **CAS-02**：旧 `ApplicationEventPublisher` 收集 `RunTerminationRequestedEvent`
//!   → [`RecordingTermination`] 收集 [`RunTerminationRequest::request`] 三元组
//!   （见 §8 I-02，旧事件的唯一消费者是 Run 终止编排）。
//! - **CAS-03**：旧 `Mockito.mock(PermissionGrantRepository.class)` 注入；Rust 的
//!   `decide_request` 在同一写事务内直接调 `zk_authz` 的授权落库函数，无需替身
//!   ——本文件全部交互都是 `ELICITATION`（`authorization_context_json IS NULL`），
//!   不触达授权落库分支。
//! - **CAS-04**：`claim_redelivery(id, transport_id, expected_attempts)` /
//!   `acknowledge_received(id, transport_id, delivery_generation)` 的入参顺序与旧
//!   `(id, expectedAttempts, transportId)` / `(id, deliveryGeneration, transportId)`
//!   不同；`redelivery_candidates()` 不收 `now`（内部取 `time::now_millis()`）。
//!   纯签名形状，判定逻辑逐格一致。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use zk_authz::interaction::{InteractionRecord, InteractionStatus, InteractionType};
use zk_db::{Db, time};
use zk_server::interaction::service::{
    DurableInteractionService, InteractionCreateSpec, NoopInteractionPublisher,
    RunTerminationRequest,
};
use zk_server::interaction::{TransitionResult, runs};

/// 旧 `List<Object> publishedEvents` + `ApplicationEventPublisher events`（L283-284）。
#[derive(Debug, Default)]
struct RecordingTermination {
    events: Mutex<Vec<(String, String, String)>>,
}

impl RecordingTermination {
    fn events(&self) -> Vec<(String, String, String)> {
        self.events
            .lock()
            .expect("termination events mutex")
            .clone()
    }
}

impl RunTerminationRequest for RecordingTermination {
    fn request<'a>(
        &'a self,
        run_id: &'a str,
        exit_reason: &'a str,
        detail: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        self.events.lock().expect("termination events mutex").push((
            run_id.to_owned(),
            exit_reason.to_owned(),
            detail.unwrap_or_default().to_owned(),
        ));
        Box::pin(std::future::ready(()))
    }
}

/// 旧 `record Fixture(jdbc, runs, interactions, run, events)`（L298-300）。
struct Fixture {
    db: Db,
    interactions: Arc<DurableInteractionService>,
    terminations: Arc<RecordingTermination>,
    run_id: String,
}

/// 旧 `fixture()`（L271-290）。
async fn fixture() -> Fixture {
    let db = Db::open_in_memory().expect("in-memory db boots with migrations");
    let run_id = uuid::Uuid::new_v4().to_string();
    {
        // 旧 L279 `INSERT INTO sessions(id) VALUES('s1')` + L282
        // `runs.start("s1", null, "main", "known")`。
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
    let terminations = Arc::new(RecordingTermination::default());
    let interactions = Arc::new(DurableInteractionService::new(
        db.clone(),
        Arc::new(NoopInteractionPublisher),
        terminations.clone(),
    ));
    // 旧 L288 `interactions.reconcileCapacityAfterRestart()`。
    interactions
        .reconcile_capacity_after_restart()
        .await
        .expect("capacity reconciles");
    Fixture {
        db,
        interactions,
        terminations,
        run_id,
    }
}

/// 旧 `fixture.interactions.create(key, "s1", run, ELICITATION, prompt, decisions, scopes,
/// "direct", null)`（L43-45 等处的九参调用）。
fn elicitation(
    run_id: &str,
    correlation_key: &str,
    prompt: Value,
    decisions: &[&str],
    scopes: &[&str],
) -> InteractionCreateSpec {
    InteractionCreateSpec {
        correlation_key: correlation_key.to_owned(),
        session_id: "s1".to_owned(),
        run_id: Some(run_id.to_owned()),
        kind: InteractionType::Elicitation,
        prompt,
        allowed_decisions: decisions.iter().map(|value| (*value).to_owned()).collect(),
        scope_options: scopes.iter().map(|value| (*value).to_owned()).collect(),
        source: Some("direct".to_owned()),
        child_session_id: None,
    }
}

/// 旧 `fixture.jdbc.queryForObject("SELECT status FROM run_envelopes WHERE id=?", ...)`。
async fn run_status(db: &Db, run_id: &str) -> String {
    let run_id = run_id.to_owned();
    db.with_reader(move |conn| {
        Ok(conn.query_row(
            "SELECT status FROM run_envelopes WHERE id=?1",
            rusqlite::params![run_id],
            |row| row.get(0),
        )?)
    })
    .await
    .expect("run status")
}

/// 旧 `fixture.jdbc.queryForObject("SELECT COUNT(*) ...", Integer.class, ...)`。
async fn count(db: &Db, sql: &'static str, args: Vec<String>) -> i64 {
    db.with_reader(move |conn| {
        let params: Vec<&dyn rusqlite::ToSql> = args
            .iter()
            .map(|value| value as &dyn rusqlite::ToSql)
            .collect();
        Ok(conn.query_row(sql, params.as_slice(), |row| row.get(0))?)
    })
    .await
    .expect("count")
}

/// 旧 `fixture.jdbc.update(sql, args...)`：测试直接改期限以驱动过期/重投分支。
async fn update(db: &Db, sql: &'static str, args: Vec<String>) -> usize {
    db.with_writer(move |conn| {
        let params: Vec<&dyn rusqlite::ToSql> = args
            .iter()
            .map(|value| value as &dyn rusqlite::ToSql)
            .collect();
        Ok(conn.execute(sql, params.as_slice())?)
    })
    .await
    .expect("update")
}

/// 旧 `fixture.interactions.findById(id)`（行不存在旧源抛异常，此处 `expect`）。
async fn find(service: &DurableInteractionService, id: &str) -> InteractionRecord {
    service
        .find_by_id(id)
        .await
        .expect("interaction lookup")
        .expect("interaction row exists")
}

/// 旧 `concurrentDifferentInteractionsShareOneProjectWriterWithoutBusyFailures`（L36-54）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_different_interactions_share_one_project_writer_without_busy_failures() {
    let fixture = fixture().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(20));
    let mut handles = Vec::new();
    for index in 0..20 {
        let service = fixture.interactions.clone();
        let run_id = fixture.run_id.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            service
                .create(elicitation(
                    &run_id,
                    &format!("parallel-{index}"),
                    json!({ "question": index }),
                    &["answer", "cancel"],
                    &[],
                ))
                .await
        }));
    }
    for handle in handles {
        let created = handle.await.expect("task joins").expect("create succeeds");
        assert_eq!(created.status, InteractionStatus::Pending);
    }

    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM interaction_requests WHERE run_id=?1",
            vec![fixture.run_id.clone()],
        )
        .await,
        20
    );
}

/// 旧 `runRemainsWaitingUntilAllConcurrentInteractionsResolve`（L56-75）。
#[tokio::test]
async fn run_remains_waiting_until_all_concurrent_interactions_resolve() {
    let fixture = fixture().await;
    let first = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "first",
            json!({ "question": "first" }),
            &["answer", "cancel"],
            &[],
        ))
        .await
        .expect("first interaction");
    let second = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "second",
            json!({ "question": "second" }),
            &["answer", "cancel"],
            &[],
        ))
        .await
        .expect("second interaction");

    fixture
        .interactions
        .decide(
            &first.interaction_id,
            first.version,
            InteractionStatus::Answered,
            Some(json!({ "decision": "answer" })),
            Some("user_rest"),
        )
        .await
        .expect("first decision");
    assert_eq!(
        run_status(&fixture.db, &fixture.run_id).await,
        "waitingInteraction"
    );

    fixture
        .interactions
        .decide(
            &second.interaction_id,
            second.version,
            InteractionStatus::Answered,
            Some(json!({ "decision": "answer" })),
            Some("user_rest"),
        )
        .await
        .expect("second decision");
    assert_eq!(run_status(&fixture.db, &fixture.run_id).await, "running");
}

/// 旧 `concurrentDecisionsHaveOneDatabaseAuthorityAndResumeRun`（L77-112）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_decisions_have_one_database_authority_and_resume_run() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "tool-1",
            json!({ "tool": "Bash" }),
            &["allow", "deny"],
            &["session"],
        ))
        .await
        .expect("interaction");
    let decision_version = request.version;
    assert!(
        fixture
            .interactions
            .mark_dispatched(&request.interaction_id, "transport-1")
            .await
            .expect("dispatch")
    );
    let delivered = find(&fixture.interactions, &request.interaction_id).await;
    assert!(
        fixture
            .interactions
            .acknowledge_received(
                &delivered.interaction_id,
                Some("transport-1"),
                delivered.delivery_generation,
            )
            .await
            .expect("ack")
    );
    let version = find(&fixture.interactions, &request.interaction_id)
        .await
        .version;
    assert_eq!(
        version, decision_version,
        "delivery metadata must not invalidate a displayed decision version"
    );

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let allow = {
        let (service, id, barrier) = (
            fixture.interactions.clone(),
            request.interaction_id.clone(),
            barrier.clone(),
        );
        tokio::spawn(async move {
            barrier.wait().await;
            service
                .decide(
                    &id,
                    version,
                    InteractionStatus::Answered,
                    Some(json!({ "decision": "allow" })),
                    Some("USER_WS"),
                )
                .await
        })
    };
    let deny = {
        let (service, id) = (fixture.interactions.clone(), request.interaction_id.clone());
        tokio::spawn(async move {
            barrier.wait().await;
            service
                .decide(
                    &id,
                    version,
                    InteractionStatus::Denied,
                    Some(json!({ "decision": "deny" })),
                    Some("USER_WS"),
                )
                .await
        })
    };
    for outcome in [
        allow.await.expect("allow task joins"),
        deny.await.expect("deny task joins"),
    ] {
        let status = outcome.expect("both callers observe the database authority");
        assert!(
            matches!(
                status,
                InteractionStatus::Answered | InteractionStatus::Denied
            ),
            "unexpected terminal status {status}"
        );
    }

    let terminal = find(&fixture.interactions, &request.interaction_id).await;
    assert!(matches!(
        terminal.status,
        InteractionStatus::Answered | InteractionStatus::Denied
    ));
    assert_eq!(run_status(&fixture.db, &fixture.run_id).await, "running");
}

/// 旧 `concurrentCreationWithSameCorrelationJoinsSingleDatabaseInteraction`（L114-137）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creation_with_same_correlation_joins_single_database_interaction() {
    let fixture = fixture().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let service = fixture.interactions.clone();
        let run_id = fixture.run_id.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            service
                .create(elicitation(
                    &run_id,
                    "same-logical-tool",
                    json!({ "question": "same" }),
                    &["answer", "cancel"],
                    &[],
                ))
                .await
        }));
    }
    let mut ids = Vec::new();
    for handle in handles {
        ids.push(
            handle
                .await
                .expect("task joins")
                .expect("both callers join one interaction")
                .interaction_id,
        );
    }
    assert_eq!(ids[0], ids[1]);
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM interaction_requests WHERE run_id=?1 AND correlation_key=?2",
            vec![fixture.run_id.clone(), "same-logical-tool".to_owned()],
        )
        .await,
        1
    );
}

/// 旧 `unacknowledgedDeliveryExpiresBeforeRequestingRunTermination`（L139-158）。
#[tokio::test]
async fn unacknowledged_delivery_expires_before_requesting_run_termination() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "tool-2",
            json!({ "tool": "Bash" }),
            &["allow", "deny"],
            &["session"],
        ))
        .await
        .expect("interaction");
    update(
        &fixture.db,
        "UPDATE interaction_requests SET delivery_window_ends_at=?1 WHERE interaction_id=?2",
        vec![
            time::format_rfc3339_micros(0),
            request.interaction_id.clone(),
        ],
    )
    .await;

    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("sweep");

    assert_eq!(
        find(&fixture.interactions, &request.interaction_id)
            .await
            .status,
        InteractionStatus::Undeliverable
    );
    // 旧测试注入的是收集用 `ApplicationEventPublisher`（无
    // `RunTerminationCoordinator` 监听该事件），故 Run 停在 `waitingInteraction`
    // ——本文件断言的是「事件已发」这一交互域责任边界。真协调器接线后的终态链见
    // `crates/zk-server/tests/run_termination.rs`。
    assert_eq!(
        run_status(&fixture.db, &fixture.run_id).await,
        "waitingInteraction"
    );
    assert!(
        fixture
            .terminations
            .events()
            .iter()
            .any(|(run_id, exit_reason, _)| {
                run_id == &fixture.run_id && exit_reason == "timeout"
            }),
        "expiry must request run termination: {:?}",
        fixture.terminations.events()
    );
}

/// 旧 `shortAckTimeoutRemainsRecoverableUntilFinalDeliveryWindow`（L160-183）。
#[tokio::test]
async fn short_ack_timeout_remains_recoverable_until_final_delivery_window() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "tool-recoverable-ack",
            json!({ "tool": "Agent" }),
            &["allow", "deny"],
            &["session"],
        ))
        .await
        .expect("interaction");
    assert!(
        fixture
            .interactions
            .mark_dispatched(&request.interaction_id, "transport-1")
            .await
            .expect("dispatch")
    );
    let now_millis = time::now_millis();
    update(
        &fixture.db,
        "UPDATE interaction_requests SET first_dispatched_at=?1,delivery_ack_deadline_at=?2,\
           delivery_window_ends_at=?3 WHERE interaction_id=?4",
        vec![
            time::format_rfc3339_micros(now_millis - 6_000),
            time::format_rfc3339_micros(0),
            time::format_rfc3339_micros(now_millis + 20_000),
            request.interaction_id.clone(),
        ],
    )
    .await;

    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("sweep");

    let recoverable = find(&fixture.interactions, &request.interaction_id).await;
    assert_eq!(recoverable.status, InteractionStatus::Pending);
    assert!(
        fixture
            .interactions
            .redelivery_candidates()
            .await
            .expect("candidates")
            .iter()
            .any(|candidate| candidate.interaction_id == request.interaction_id)
    );
    assert!(
        fixture
            .interactions
            .acknowledge_received(
                &recoverable.interaction_id,
                Some("transport-reconnected"),
                recoverable.delivery_generation,
            )
            .await
            .expect("ack")
    );
    assert!(
        find(&fixture.interactions, &request.interaction_id)
            .await
            .received_at
            .is_some()
    );
}

/// 旧 `redeliveryClaimsAreAtomicAndBounded`（L185-201）。
#[tokio::test]
async fn redelivery_claims_are_atomic_and_bounded() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "tool-3",
            json!({ "tool": "Bash" }),
            &["allow", "deny"],
            &["session"],
        ))
        .await
        .expect("interaction");
    assert!(
        fixture
            .interactions
            .mark_dispatched(&request.interaction_id, "transport-1")
            .await
            .expect("dispatch")
    );
    let now_millis = time::now_millis();
    update(
        &fixture.db,
        "UPDATE interaction_requests SET first_dispatched_at=?1,delivery_ack_deadline_at=?2 \
         WHERE interaction_id=?3",
        vec![
            time::format_rfc3339_micros(now_millis - 2_000),
            time::format_rfc3339_micros(now_millis + 10_000),
            request.interaction_id.clone(),
        ],
    )
    .await;

    assert!(
        fixture
            .interactions
            .redelivery_candidates()
            .await
            .expect("candidates")
            .iter()
            .any(|candidate| candidate.interaction_id == request.interaction_id)
    );
    assert!(
        fixture
            .interactions
            .claim_redelivery(&request.interaction_id, "transport-2", 1)
            .await
            .expect("first claim")
    );
    assert!(
        !fixture
            .interactions
            .claim_redelivery(&request.interaction_id, "transport-3", 1)
            .await
            .expect("second claim")
    );
    assert_eq!(
        find(&fixture.interactions, &request.interaction_id)
            .await
            .dispatch_attempts,
        2
    );
}

/// 旧 `anyBoundSessionTransportMayAcknowledgeABroadcastDelivery`（L203-218）。
#[tokio::test]
async fn any_bound_session_transport_may_acknowledge_a_broadcast_delivery() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "tool-secondary-tab",
            json!({ "tool": "Read" }),
            &["allow", "deny"],
            &["session"],
        ))
        .await
        .expect("interaction");
    assert!(
        fixture
            .interactions
            .mark_dispatched(&request.interaction_id, "transport-a")
            .await
            .expect("dispatch")
    );
    let dispatched = find(&fixture.interactions, &request.interaction_id).await;

    assert!(
        fixture
            .interactions
            .acknowledge_received(
                &dispatched.interaction_id,
                Some("transport-b"),
                dispatched.delivery_generation,
            )
            .await
            .expect("ack")
    );
    let acknowledged = find(&fixture.interactions, &request.interaction_id).await;
    assert!(acknowledged.received_at.is_some());
    assert_eq!(
        acknowledged.last_transport_id.as_deref(),
        Some("transport-b")
    );
    assert_eq!(acknowledged.version, request.version);
}

/// 旧 `acknowledgementReplacesDeliveryDeadlineWithoutCompletingTerminalWait`（L220-247）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledgement_replaces_delivery_deadline_without_completing_terminal_wait() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "tool-ack-deadline",
            json!({ "tool": "Bash" }),
            &["answer", "cancel"],
            &[],
        ))
        .await
        .expect("interaction");
    assert!(
        fixture
            .interactions
            .mark_dispatched(&request.interaction_id, "transport-1")
            .await
            .expect("dispatch")
    );

    // 旧 L228 注释：缩短首次投递 ACK 窗口，使测试稳定覆盖「等待已按旧窗口调度、
    // 随后 ACK 延长决策期限」的竞态。
    update(
        &fixture.db,
        "UPDATE interaction_requests SET delivery_ack_deadline_at=?1 WHERE interaction_id=?2",
        vec![
            time::format_rfc3339_micros(time::now_millis() + 250),
            request.interaction_id.clone(),
        ],
    )
    .await;
    let mut terminal = {
        let (service, id) = (fixture.interactions.clone(), request.interaction_id.clone());
        tokio::spawn(async move { service.await_terminal(&id).await })
    };

    let dispatched = find(&fixture.interactions, &request.interaction_id).await;
    assert!(
        fixture
            .interactions
            .acknowledge_received(
                &dispatched.interaction_id,
                Some("transport-1"),
                dispatched.delivery_generation,
            )
            .await
            .expect("ack")
    );
    let acknowledged = find(&fixture.interactions, &request.interaction_id).await;
    let deadline = acknowledged
        .decision_deadline_at
        .as_deref()
        .and_then(time::parse_rfc3339_millis)
        .expect("ack opens the decision window");
    assert!(
        deadline > time::now_millis() + 290_000,
        "decision deadline must be pushed to +300s, got {deadline}"
    );

    assert!(
        tokio::time::timeout(Duration::from_secs(3), &mut terminal)
            .await
            .is_err(),
        "acknowledgement must not complete the terminal wait"
    );

    fixture
        .interactions
        .decide(
            &request.interaction_id,
            acknowledged.version,
            InteractionStatus::Answered,
            Some(json!({ "decision": "answer" })),
            Some("user_rest"),
        )
        .await
        .expect("decision");
    let status = tokio::time::timeout(Duration::from_secs(2), terminal)
        .await
        .expect("terminal wait wakes on decision")
        .expect("task joins")
        .expect("terminal status");
    assert_eq!(status, InteractionStatus::Answered);
}

/// 旧 `decisionRollsBackWhenRunCannotResume`（L249-269）。
#[tokio::test]
async fn decision_rolls_back_when_run_cannot_resume() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(
            &fixture.run_id,
            "tool-4",
            json!({ "tool": "Bash" }),
            &["allow", "deny"],
            &["session"],
        ))
        .await
        .expect("interaction");
    let cancelled = {
        let run_id = fixture.run_id.clone();
        fixture
            .db
            .with_writer(move |conn| {
                runs::request_cancel_in_current_write(conn, &run_id, runs::EXIT_USER_CANCELLED)
            })
            .await
            .expect("cancel write")
    };
    assert_eq!(cancelled, TransitionResult::Applied);

    let rejected = fixture
        .interactions
        .decide(
            &request.interaction_id,
            request.version,
            InteractionStatus::Answered,
            Some(json!({ "decision": "allow" })),
            Some("USER_REST"),
        )
        .await
        .expect_err("a cancelling run cannot absorb a decision");
    assert!(
        rejected
            .message
            .contains("INTERACTION_RUN_TRANSITION_FAILED"),
        "unexpected rejection: {rejected:?}"
    );

    assert_eq!(
        find(&fixture.interactions, &request.interaction_id)
            .await
            .status,
        InteractionStatus::Pending
    );
    assert_eq!(run_status(&fixture.db, &fixture.run_id).await, "cancelling");
}

#[tokio::test]
async fn staged_deadlines_start_at_ten_minutes_and_obey_root_deadline() {
    let f = fixture().await;
    let now = time::now_millis();
    let request = f
        .interactions
        .create(elicitation(
            &f.run_id,
            "staged",
            json!({"multiSelect":true}),
            &["answer", "cancel"],
            &[],
        ))
        .await
        .unwrap();
    let first = time::parse_rfc3339_millis(&request.delivery_window_ends_at).unwrap();
    assert!(first >= now + 599_000 && first <= time::now_millis() + 601_000);
    let root_deadline = time::now_millis() + 12_000;
    update(&f.db,"UPDATE tasks SET deadline_at_ms=?1 WHERE id=(SELECT task_id FROM run_envelopes WHERE id=?2)",vec![root_deadline.to_string(),f.run_id.clone()]).await;
    assert!(
        f.interactions
            .mark_dispatched(&request.interaction_id, "client")
            .await
            .unwrap()
    );
    let sent = find(&f.interactions, &request.interaction_id).await;
    assert_eq!(
        time::parse_rfc3339_millis(&sent.delivery_window_ends_at),
        Some(root_deadline)
    );
    assert!(
        f.interactions
            .acknowledge_received(
                &request.interaction_id,
                Some("client"),
                sent.delivery_generation
            )
            .await
            .unwrap()
    );
    let ack = find(&f.interactions, &request.interaction_id).await;
    assert_eq!(
        ack.decision_deadline_at
            .as_deref()
            .and_then(time::parse_rfc3339_millis),
        Some(root_deadline)
    );
    for invalid in [
        json!("single"),
        json!([]),
        json!(["same", "same"]),
        json!([""]),
    ] {
        assert!(
            f.interactions
                .decide(
                    &request.interaction_id,
                    ack.version,
                    InteractionStatus::Answered,
                    Some(invalid),
                    Some("user_rest")
                )
                .await
                .is_err()
        );
        assert_eq!(
            find(&f.interactions, &request.interaction_id).await.status,
            InteractionStatus::Pending
        );
    }
    let accepted = f
        .interactions
        .decide(
            &request.interaction_id,
            ack.version,
            InteractionStatus::Answered,
            Some(json!(["one", "two"])),
            Some("user_rest"),
        )
        .await
        .unwrap();
    assert_eq!(accepted, InteractionStatus::Answered);
    assert_eq!(
        find(&f.interactions, &request.interaction_id)
            .await
            .response_json
            .as_deref(),
        Some("[\"one\",\"two\"]")
    );
}

#[tokio::test]
async fn expired_decision_cannot_win_before_background_sweeper() {
    let f = fixture().await;
    let request = f
        .interactions
        .create(elicitation(
            &f.run_id,
            "late",
            json!({"multiSelect":false}),
            &["answer", "cancel"],
            &[],
        ))
        .await
        .unwrap();
    update(
        &f.db,
        "UPDATE interaction_requests SET delivery_window_ends_at=?1 WHERE interaction_id=?2",
        vec![
            time::format_rfc3339_micros(time::now_millis() - 1000),
            request.interaction_id.clone(),
        ],
    )
    .await;
    assert!(
        f.interactions
            .decide(
                &request.interaction_id,
                request.version,
                InteractionStatus::Answered,
                Some(json!("late answer")),
                Some("user_rest")
            )
            .await
            .is_err()
    );
    assert_eq!(
        find(&f.interactions, &request.interaction_id).await.status,
        InteractionStatus::Pending
    );
}

/// Source regression: a transport failure must not claim the user ignored a question.
#[tokio::test]
async fn question_delivery_failures_remain_distinct_from_user_answer_expiry() {
    use tokio_util::sync::CancellationToken;
    use zk_server::interaction::DurableElicitationSink;
    use zk_tools::{AskUserQuestionTool, Tool, ToolContext};

    for stage in 0..3 {
        let fixture = fixture().await;
        let tool = AskUserQuestionTool::with_elicitation_sink(Arc::new(
            DurableElicitationSink::new(fixture.interactions.clone()),
        ));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let context = ToolContext::new(CancellationToken::new(), tx)
            .with_session_id("s1")
            .with_run_id(fixture.run_id.clone());
        let running = tokio::spawn(async move {
            tool.execute(
                json!({"questions":[{"question":"Continue?","multiSelect":true,
                "options":[{"label":"Yes"},{"label":"No"}]}]}),
                context,
            )
            .await
        });
        let record = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(record) = fixture.interactions.pending("s1").await.unwrap().pop() {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&record.prompt_json).unwrap()["multiSelect"],
            true
        );
        if stage > 0 {
            fixture
                .interactions
                .mark_dispatched(&record.interaction_id, "transport")
                .await
                .unwrap();
        }
        if stage == 2 {
            assert!(
                fixture
                    .interactions
                    .acknowledge_received(&record.interaction_id, Some("transport"), 1)
                    .await
                    .unwrap()
            );
        }
        let id = record.interaction_id.clone();
        fixture.db.with_writer(move |conn| {
            let column = if stage == 2 { "decision_deadline_at" } else { "delivery_window_ends_at" };
            conn.execute(&format!("UPDATE interaction_requests SET {column}='1970-01-01T00:00:00.000000Z' WHERE interaction_id=?1"), [id])?;
            Ok(())
        }).await.unwrap();
        fixture.interactions.expire_deadlines().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_error);
        let (code, detail, reason) = match stage {
            0 => (
                "ELICITATION_UNDELIVERABLE",
                "could not be dispatched",
                "delivery_not_dispatched",
            ),
            1 => (
                "ELICITATION_UNDELIVERABLE",
                "did not acknowledge",
                "delivery_not_acknowledged",
            ),
            _ => (
                "ELICITATION_EXPIRED",
                "no answer was received",
                "decision_deadline_exceeded",
            ),
        };
        assert!(result.content.contains(code), "{result:?}");
        assert!(result.content.contains(detail), "{result:?}");
        assert!(!result.content.contains("User did not respond"));
        let persisted = fixture
            .interactions
            .find_by_id(&record.interaction_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.terminal_reason.as_deref(), Some(reason));
        assert!(persisted.status.is_terminal());
        let recovered = fixture
            .interactions
            .prepare_recovery_delivery(&record.interaction_id, "late")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.status, persisted.status);
        assert_eq!(recovered.delivery_generation, persisted.delivery_generation);
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One live lease, WAL privacy proof and expired-lease cancellation form a single interaction lifecycle"
)]
async fn ephemeral_interaction_deduplicates_without_storing_prompt_or_operation_hash() {
    let root =
        std::env::temp_dir().join(format!("zk-private-interaction-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let db = Db::open(root.join("interaction.sqlite")).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", root.to_str().unwrap(), "DEFAULT")
        .await
        .unwrap();
    let run = uuid::Uuid::new_v4().to_string();
    let sid = session.clone();
    let rid = run.clone();
    db.with_writer(move |conn| {
        runs::start_in_current_write(conn, &rid, &sid, None, Some("main"), "fixture")
    })
    .await
    .unwrap();
    let service = Arc::new(DurableInteractionService::new(
        db.clone(),
        Arc::new(NoopInteractionPublisher),
        Arc::new(RecordingTermination::default()),
    ));
    let secret = format!("private-operation-and-hash-{}", uuid::Uuid::new_v4());
    let mut spec = elicitation(
        &run,
        &secret,
        json!({"question":secret,"multiSelect":false}),
        &["answer"],
        &[],
    );
    spec.session_id = session.clone();
    let (one, two) = tokio::join!(service.create(spec.clone()), service.create(spec));
    let one = one.unwrap();
    let two = two.unwrap();
    assert_eq!(one.interaction_id, two.interaction_id);
    assert!(one.prompt_json.contains(&secret));
    assert_eq!(
        service
            .find_by_correlation_key(&run, &secret)
            .await
            .unwrap()
            .unwrap()
            .interaction_id,
        one.interaction_id
    );
    db.with_writer(|conn| {
        assert!(
            conn.execute("UPDATE interaction_requests SET prompt_json='raw'", [])
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
    let answered = service
        .decide_request(
            &one.interaction_id,
            one.version,
            InteractionStatus::Answered,
            Some(Value::String(secret.clone())),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        answered.response_json.as_deref(),
        Some(serde_json::to_string(&secret).unwrap().as_str())
    );
    for file in std::fs::read_dir(&root).unwrap() {
        let file = file.unwrap().path();
        if file.is_file() {
            let bytes = std::fs::read(file).unwrap();
            assert!(
                !bytes
                    .windows(secret.len())
                    .any(|part| part == secret.as_bytes())
            );
        }
    }
    let mut pending = elicitation(
        &run,
        "pending-cleanup",
        json!({"question":secret}),
        &["answer"],
        &[],
    );
    pending.session_id = session.clone();
    let pending = service.create(pending).await.unwrap();
    // Terminal metadata remains available for cleanup, while old answers disappear.
    drop(lease);
    db.request_runtime_shutdown().await.unwrap();
    let cancelled = service
        .complete_runtime_cancellation(&run, "private cancellation reason")
        .await
        .unwrap();
    assert_eq!(cancelled.interactions_cancelled, 1);
    assert_eq!(
        service
            .find_by_id(&pending.interaction_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        InteractionStatus::Cancelled
    );
    let expired = service
        .find_by_id(&one.interaction_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.status, InteractionStatus::Answered);
    assert_eq!(expired.prompt_json, r#"{"contentUnavailable":true}"#);
    assert_eq!(expired.response_json.as_deref(), Some("null"));
    drop(service);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
