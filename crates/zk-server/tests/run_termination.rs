//! `RunTerminationCoordinator` 接线后的终止链——交互域事件 → 终止端口 → Run 终态。
//!
//! 旧源无独立的 `RunTerminationCoordinatorTest`：其行为分布在
//! `DurableInteractionServiceCasTest`（注入收集用 `ApplicationEventPublisher`，
//! 只断言事件已发，见 `interaction_cas.rs` CAS-02）与 Spring 全上下文测试之间。
//! zkcode 的端口化装配（§8 I-02）使「事件已发」与「Run 真被终止」可分开断言，
//! 本文件补齐后半段：**真协调器**在位时 Run 必须离开 `waiting_interaction` 并落到
//! 旧源 `RunTerminationCoordinator.terminate` L69-73 指定的终态。
//!
//! 逐值对照锚点（`main@581d407b`）：
//!
//! | 断言 | 旧源 |
//! |---|---|
//! | 未 ACK → `terminal_reason=delivery_not_acknowledged` | `expireIfDue` L605-609 |
//! | 决策超期 → `terminal_reason=decision_deadline_exceeded` | `expireIfDue` L610-614 |
//! | 两条路径 `exit_reason` 同为 `interaction_expired` | `expire` L721-723 |
//! | `waiting_interaction → cancelling` 记 `requested_exit_reason` | `beginRunTermination` L629 → `requestCancel` L118-122 |
//! | 同 Run 兄弟待决交互一并 `cancelled` + 配额归还 | `beginRunTermination` L634-647 + `completeCancelled` L653-660 |
//! | `cancelling → failed`，`error_summary = detail` | `terminate` L72 `runs.fail(runId, reason, detail)` |
//! | `user_cancelled → cancelled` + `abort_reason` | `terminate` L70 `runs.cancel(runId)` |
//! | 非 `APPLIED` 即返回、不落终态 | `terminate` L43-46 |

use std::sync::Arc;

use serde_json::{Value, json};
use zk_authz::interaction::{InteractionRecord, InteractionStatus, InteractionType};
use zk_db::{Db, time};
use zk_server::interaction::service::{
    DurableInteractionService, InteractionCreateSpec, NoopInteractionPublisher,
};
use zk_server::interaction::{MAX_WAITING, TransitionResult, runs};
use zk_server::run_termination::{RunTerminationCoordinator, assemble};

/// 夹具：内存库（27 表基线迁移）+ 已 `running` 的根 Run + 生产装配的交互服务。
struct Fixture {
    db: Db,
    interactions: Arc<DurableInteractionService>,
    terminations: Arc<RunTerminationCoordinator>,
    run_id: String,
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
    // 生产装配同一入口：终止端口恒为真 `RunTerminationCoordinator`。
    let (interactions, terminations) = assemble(db.clone(), Arc::new(NoopInteractionPublisher));
    Fixture {
        db,
        interactions,
        terminations,
        run_id,
    }
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

/// `run_envelopes` 终态读回视图。
struct RunRow {
    status: String,
    exit_reason: Option<String>,
    requested_exit_reason: Option<String>,
    abort_reason: Option<String>,
    error_summary: Option<String>,
    terminal_at: Option<String>,
}

async fn run_row(db: &Db, run_id: &str) -> RunRow {
    let run_id = run_id.to_owned();
    db.with_reader(move |conn| {
        Ok(conn
            .query_row(
                "SELECT status,exit_reason,requested_exit_reason,abort_reason,error_summary,\
                        terminal_at FROM run_envelopes WHERE id=?1",
                rusqlite::params![run_id],
                |row| {
                    Ok(RunRow {
                        status: row.get(0)?,
                        exit_reason: row.get(1)?,
                        requested_exit_reason: row.get(2)?,
                        abort_reason: row.get(3)?,
                        error_summary: row.get(4)?,
                        terminal_at: row.get(5)?,
                    })
                },
            )
            .expect("run row"))
    })
    .await
    .expect("read run row")
}

/// `run_event_log` 的 `(event_type, 事件信封)` 序列（按 `seq`）。
async fn run_events(db: &Db, run_id: &str) -> Vec<(String, Value)> {
    let run_id = run_id.to_owned();
    db.with_reader(move |conn| {
        let mut stmt = conn.prepare(
            "SELECT event_type,event_data FROM run_event_log WHERE run_id=?1 ORDER BY seq",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![run_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .map(|(event_type, payload)| {
                (
                    event_type,
                    serde_json::from_str::<Value>(&payload).expect("event json"),
                )
            })
            .collect())
    })
    .await
    .expect("read run events")
}

async fn find(interactions: &DurableInteractionService, id: &str) -> InteractionRecord {
    interactions
        .find_by_id(id)
        .await
        .expect("read interaction")
        .expect("interaction exists")
}

async fn rewind(db: &Db, sql: &'static str, interaction_id: &str) {
    let args = [time::format_rfc3339_micros(0), interaction_id.to_owned()];
    db.with_writer(move |conn| {
        conn.execute(sql, rusqlite::params_from_iter(args.iter()))?;
        Ok(())
    })
    .await
    .expect("rewind deadline");
}

/// 把投递窗口回拨到 epoch 0（旧测试同法：直接改期限列，不等真实时钟）。
async fn expire_delivery_window(db: &Db, interaction_id: &str) {
    rewind(
        db,
        "UPDATE interaction_requests SET delivery_window_ends_at=?1 WHERE interaction_id=?2",
        interaction_id,
    )
    .await;
}

/// 把决策期限回拨到 epoch 0（前置：已 ACK，`received_at` 非空）。
async fn expire_decision_deadline(db: &Db, interaction_id: &str) {
    rewind(
        db,
        "UPDATE interaction_requests SET decision_deadline_at=?1 WHERE interaction_id=?2",
        interaction_id,
    )
    .await;
}

// ══════════════════ 路径一：投递未 ACK（undeliverable） ══════════════════

/// 旧 `expireIfDue` L605-609（`UNDELIVERABLE` / `delivery_not_acknowledged`）
/// → `expire` L721-724（`INTERACTION_EXPIRED`）→ `terminate` L72（`runs.fail`）。
#[tokio::test]
async fn undeliverable_delivery_terminates_run_as_interaction_expired() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-undeliverable"))
        .await
        .expect("interaction");
    // 权限未决：交互已落库、Run 已切 `waiting_interaction`，前端始终未 ACK。
    assert_eq!(
        run_row(&fixture.db, &fixture.run_id).await.status,
        "waiting_interaction"
    );
    expire_delivery_window(&fixture.db, &request.interaction_id).await;

    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("sweep");

    let interaction = find(&fixture.interactions, &request.interaction_id).await;
    assert_eq!(interaction.status, InteractionStatus::Undeliverable);
    assert_eq!(
        interaction.terminal_reason.as_deref(),
        Some("delivery_not_acknowledged")
    );

    let run = run_row(&fixture.db, &fixture.run_id).await;
    // 修复的核心：Run 不再卡在 `waiting_interaction`。
    assert_eq!(run.status, "failed", "expiry must terminate the run");
    assert_eq!(run.exit_reason.as_deref(), Some("interaction_expired"));
    assert_eq!(
        run.requested_exit_reason.as_deref(),
        Some("interaction_expired"),
        "requestCancel 记 requested_exit_reason（旧 L118-122）"
    );
    // 旧 L72 `runs.fail(runId, reason, detail)`：detail 原样落 error_summary。
    assert_eq!(
        run.error_summary.as_deref(),
        Some("delivery_not_acknowledged")
    );
    assert!(run.terminal_at.is_some(), "终态必须有 terminal_at");
    assert!(
        run.abort_reason.is_none(),
        "旧 fail 不写 abort_reason（仅 cancel / interrupt 写）"
    );
    // 配额已归还（旧 `finish` L727 `capacity.release()`）。
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING);
}

// ══════════════════ 路径二：ACK 后决策超期（expired） ══════════════════

/// 旧 `expireIfDue` L610-614（`EXPIRED` / `decision_deadline_exceeded`）。
/// 终止原因与路径一**同为** `INTERACTION_EXPIRED`，仅 detail 不同。
#[tokio::test]
async fn decision_deadline_expiry_terminates_run_as_interaction_expired() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-expired"))
        .await
        .expect("interaction");
    assert!(
        fixture
            .interactions
            .mark_dispatched(&request.interaction_id, "transport-1")
            .await
            .expect("dispatch")
    );
    let dispatched = find(&fixture.interactions, &request.interaction_id).await;
    assert!(
        fixture
            .interactions
            .acknowledge_received(
                &request.interaction_id,
                Some("transport-1"),
                dispatched.delivery_generation,
            )
            .await
            .expect("ack")
    );
    expire_decision_deadline(&fixture.db, &request.interaction_id).await;

    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("sweep");

    let interaction = find(&fixture.interactions, &request.interaction_id).await;
    assert_eq!(interaction.status, InteractionStatus::Expired);
    assert_eq!(
        interaction.terminal_reason.as_deref(),
        Some("decision_deadline_exceeded")
    );

    let run = run_row(&fixture.db, &fixture.run_id).await;
    assert_eq!(run.status, "failed");
    assert_eq!(run.exit_reason.as_deref(), Some("interaction_expired"));
    assert_eq!(
        run.error_summary.as_deref(),
        Some("decision_deadline_exceeded"),
        "两条路径 exit_reason 同值，detail 逐路径不同"
    );
    assert!(run.terminal_at.is_some());
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING);
}

// ══════════════════ 事件序列 + 兄弟交互级联 ══════════════════

/// 旧 `beginRunTermination` L634-647：同事务级联取消该 Run 的**全部** pending
/// 交互；`completeCancelled` L653-660 归还配额。这正是终止不能简化为
/// `Db::terminate_run`（只做 `requestCancel`）的原因。
#[tokio::test]
async fn expiry_cascades_sibling_pending_interactions_and_releases_capacity() {
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
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING - 2);
    expire_delivery_window(&fixture.db, &doomed.interaction_id).await;

    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("sweep");

    assert_eq!(
        find(&fixture.interactions, &doomed.interaction_id)
            .await
            .status,
        InteractionStatus::Undeliverable
    );
    let sibling = find(&fixture.interactions, &sibling.interaction_id).await;
    assert_eq!(
        sibling.status,
        InteractionStatus::Cancelled,
        "兄弟待决交互必须随 Run 终止一并取消，否则容量配额泄漏"
    );
    // 旧 L640：级联取消写入的 terminal_reason 即 terminate 的 detail
    // （`detail == null ? reason.dbValue() : detail`，旧 L42）。
    assert_eq!(
        sibling.terminal_reason.as_deref(),
        Some("delivery_not_acknowledged")
    );
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING);

    let events = run_events(&fixture.db, &fixture.run_id).await;
    let types: Vec<&str> = events
        .iter()
        .map(|(event_type, _)| event_type.as_str())
        .collect();
    assert_eq!(types.first(), Some(&"run_started"));
    let transitions: Vec<(String, String)> = events
        .iter()
        .filter(|(event_type, _)| event_type == "run_status_changed")
        .map(|(_, envelope)| {
            let data = &envelope["data"];
            (
                data["from"].as_str().unwrap_or_default().to_owned(),
                data["to"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        transitions,
        vec![
            ("running".to_owned(), "waiting_interaction".to_owned()),
            ("waiting_interaction".to_owned(), "cancelling".to_owned()),
            ("cancelling".to_owned(), "failed".to_owned()),
        ],
        "旧源终止链：waiting_interaction → cancelling → failed"
    );
    assert_eq!(
        types
            .iter()
            .filter(|event_type| **event_type == "interaction_terminal")
            .count(),
        2,
        "过期的与被级联取消的各一条 interaction_terminal"
    );
}

// ══════════════════ 竞态：终止后引擎回写不再 INVALID_TRANSITION ══════════════════

/// 缺陷复现的反向断言：交互过期终止后，引擎侧的 `complete_run` 只会拿到
/// `ALREADY_TERMINAL`（旧 `transition` 的终态短路），而不是修复前
/// 「Run 卡在 `waiting_interaction` → `complete[running]` → `INVALID_TRANSITION`」。
#[tokio::test]
async fn engine_completion_after_expiry_is_already_terminal_not_invalid_transition() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-race"))
        .await
        .expect("interaction");
    expire_delivery_window(&fixture.db, &request.interaction_id).await;
    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("sweep");

    let late = fixture
        .db
        .complete_run(&fixture.run_id, 128, 0.0, 2)
        .await
        .expect("late completion is a no-op, not an error");
    assert_eq!(late, TransitionResult::AlreadyTerminal);
    // 终态不被迟到的完成回写篡改。
    let run = run_row(&fixture.db, &fixture.run_id).await;
    assert_eq!(run.status, "failed");
    assert_eq!(run.exit_reason.as_deref(), Some("interaction_expired"));
}

// ══════════════════ 协调器直调：user_cancelled 与幂等 ══════════════════

/// 旧 `cancelByUser` L37-39 → `terminate` L70 `runs.cancel(runId)`：终态是
/// `cancelled`（而非 `failed`），`abort_reason` 逐字为 `user_cancelled`
/// （旧 `RunControlService.cancel` L136-138）。
#[tokio::test]
async fn cancel_by_user_lands_cancelled_with_abort_reason() {
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
    let run = run_row(&fixture.db, &fixture.run_id).await;
    assert_eq!(run.status, "cancelled");
    assert_eq!(run.exit_reason.as_deref(), Some("user_cancelled"));
    assert_eq!(run.abort_reason.as_deref(), Some("user_cancelled"));
    assert!(
        run.error_summary.is_none(),
        "旧 cancel 的 error 位为 null（旧 L136-138）"
    );
    // 待决交互随之取消，配额归还。
    let cancelled = find(&fixture.interactions, &request.interaction_id).await;
    assert_eq!(cancelled.status, InteractionStatus::Cancelled);
    assert_eq!(
        cancelled.terminal_reason.as_deref(),
        Some("user pressed stop")
    );
    assert_eq!(fixture.interactions.available_permits(), MAX_WAITING);
}

/// 旧 `terminate` L43-46：`beginRunTermination` 非 `APPLIED` 即原样返回、
/// **不落终态**（不重复终止、不覆盖已有终态原因）。
#[tokio::test]
async fn terminating_an_already_terminal_run_is_a_no_op() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-twice"))
        .await
        .expect("interaction");
    expire_delivery_window(&fixture.db, &request.interaction_id).await;
    fixture
        .interactions
        .expire_deadlines()
        .await
        .expect("sweep");
    let before = run_row(&fixture.db, &fixture.run_id).await;

    let transition = fixture
        .terminations
        .terminate(
            &fixture.run_id,
            runs::EXIT_USER_CANCELLED,
            Some("late cancel"),
        )
        .await
        .expect("second termination is rejected, not an error");

    assert_eq!(transition, TransitionResult::AlreadyTerminal);
    let after = run_row(&fixture.db, &fixture.run_id).await;
    assert_eq!(after.status, before.status);
    assert_eq!(after.exit_reason, before.exit_reason);
    assert_eq!(after.error_summary, before.error_summary);
    assert_eq!(after.terminal_at, before.terminal_at);
}

/// 旧 `terminate` L41-42：`detail == null` 时以 `reason.dbValue()` 兜底喂给交互
/// `terminal_reason`，而落终态一步（L72）**不兜底**——`error_summary` 保持 `NULL`。
#[tokio::test]
async fn null_detail_falls_back_only_for_interaction_terminal_reason() {
    let fixture = fixture().await;
    let request = fixture
        .interactions
        .create(elicitation(&fixture.run_id, "tool-null-detail"))
        .await
        .expect("interaction");

    let transition = fixture
        .terminations
        .terminate(&fixture.run_id, runs::EXIT_INTERACTION_EXPIRED, None)
        .await
        .expect("terminate");

    assert_eq!(transition, TransitionResult::Applied);
    assert_eq!(
        find(&fixture.interactions, &request.interaction_id)
            .await
            .terminal_reason
            .as_deref(),
        Some("interaction_expired"),
        "旧 L42：detail 为空时以 reason.dbValue() 兜底"
    );
    let run = run_row(&fixture.db, &fixture.run_id).await;
    assert_eq!(run.status, "failed");
    assert_eq!(run.exit_reason.as_deref(), Some("interaction_expired"));
    assert!(
        run.error_summary.is_none(),
        "旧 L72 原样传 detail，null 即 error_summary NULL"
    );
}
