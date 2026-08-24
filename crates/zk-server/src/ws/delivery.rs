//! 交互投递闸门（旧 `WebSocketController.deliverInteraction`，L285-315 +
//! `onInteractionCreated` L266-274 + `redeliverUnacknowledgedInteractions`
//! L317-330）。
//!
//! # 为什么必须有这道闸门
//!
//! `decide_request` 第 3 层校验要求「决策所引用的投递代次 == 当前代次且
//! `received_at IS NOT NULL`」。而 `delivery_generation` 只在库内 claim
//! （`mark_dispatched` / `claim_redelivery` / `prepare_recovery_delivery`）时自增，
//! `received_at` 只在客户端 `interaction_ack` 上行时落。若创建后直推视图而不 claim，
//! 交互恒停在 `delivery_generation=0`，前端 ACK 一律被丢弃、用户决策 100% 被拒
//! （`PERMISSION_DELIVERY_STALE`）——即权限闭环整体不可用。
//!
//! # 三档投递
//!
//! | 档 | 触发 | claim 动作 | 实现位置 |
//! |---|---|---|---|
//! | INITIAL | `interaction_created` 事件 | `mark_dispatched` | 本模块 |
//! | RETRY | 250ms 定时扫描未 ACK 交互 | `claim_redelivery` | 本模块 |
//! | RECOVERY | bind 重连后 pending 重投 | `prepare_recovery_delivery` | [`super::inbound`] |
//!
//! 三档共用同一前置条件：会话必须有已绑定 transport（无 transport 只记日志、
//! 不落任何投递痕迹，等重连走 RECOVERY），claim 成功后回查权威行再推
//! `interaction_created`（旧 L306-307 硬编码事件类型）。

use std::sync::{Arc, Weak};
use std::time::Duration;

use zk_authz::interaction::InteractionRecord;
use zk_protocol::ServerMessage;

use super::WsHub;
use crate::interaction::DurableInteractionService;

/// 未 ACK 交互的重投扫描间隔（旧 `@Scheduled(fixedDelay = 250)`，L318）。
const REDELIVERY_SCAN_INTERVAL: Duration = Duration::from_millis(250);

/// 旧 `private enum InteractionDelivery { INITIAL, RETRY, RECOVERY }`（L285）。
///
/// RECOVERY 档不在此列举：它的投递目标恒为刚 bind 的那条连接，claim 与推送在
/// `inbound::replay_pending_interactions` 内完成（见 §8 偏离表 D-02）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionDelivery {
    /// 创建后首投（`mark_dispatched`：首投时刻 / ACK 窗口 / 代次 +1）。
    Initial,
    /// 退避重投（`claim_redelivery`：`dispatch_attempts` CAS，1..3 次）。
    Retry,
}

/// 旧 `deliverInteraction(request, delivery)`（L286-308）。
///
/// 无绑定 transport、claim 未命中、回查行消失时静默返回（旧源同为「只记日志、
/// 不回滚交互」的 best-effort 语义）。
pub async fn deliver_interaction(
    hub: &WsHub,
    service: &DurableInteractionService,
    record: &InteractionRecord,
    delivery: InteractionDelivery,
) {
    // 旧 L288-294：会话无已绑定 transport → info 日志后返回（交互留在库里等重连）。
    let Some(transport) = hub.first_transport_for_session(&record.session_id) else {
        tracing::info!(
            interaction_id = %record.interaction_id,
            session_id = %record.session_id,
            "Interaction pending without bound transport"
        );
        return;
    };
    // 旧 L296-305：INITIAL 走 `markInteractionDispatched`，RETRY 走 `claimRedelivery`。
    let claimed = match delivery {
        InteractionDelivery::Initial => {
            service
                .mark_dispatched(&record.interaction_id, &transport)
                .await
        }
        InteractionDelivery::Retry => {
            service
                .claim_redelivery(&record.interaction_id, &transport, record.dispatch_attempts)
                .await
        }
    };
    match claimed {
        // 旧 L306：`if (!claimed) return;`。
        Ok(false) => return,
        Ok(true) => {}
        Err(error) => {
            tracing::warn!(
                interaction_id = %record.interaction_id,
                ?delivery,
                error = %error,
                "interaction delivery claim failed"
            );
            return;
        }
    }
    // 旧 L307：`pushInteractionView(findInteraction(id), "interaction_created")`
    //——claim 已改写代次与 ACK 截止，必须回查权威行再投。
    match service.find_by_id(&record.interaction_id).await {
        Ok(Some(current)) => push_interaction_view(hub, &current, "interaction_created").await,
        Ok(None) => tracing::warn!(
            interaction_id = %record.interaction_id,
            "interaction row vanished before delivery"
        ),
        Err(error) => tracing::warn!(
            interaction_id = %record.interaction_id,
            error = %error,
            "interaction reload before delivery failed"
        ),
    }
}

/// 旧 `pushInteractionView(request, type)`（L310-315）：视图不可投时只记告警。
pub(crate) async fn push_interaction_view(
    hub: &WsHub,
    record: &InteractionRecord,
    event_type: &str,
) {
    let view = match DurableInteractionService::view(record) {
        Ok(view) => view,
        Err(error) => {
            tracing::warn!(
                session_id = %record.session_id,
                event_type,
                interaction_id = %record.interaction_id,
                code = %error.code,
                "interaction view is not publishable (push skipped)"
            );
            return;
        }
    };
    let message = match event_type {
        "interaction_created" => ServerMessage::InteractionCreated { view },
        "interaction_terminal" => ServerMessage::InteractionTerminal { view },
        _ => ServerMessage::InteractionUpdated { view },
    };
    hub.push(&record.session_id, message).await;
}

/// 旧 `@Scheduled(fixedDelay = 250) redeliverUnacknowledgedInteractions()`
///（L317-330）：对已投递但未 ACK 的交互按 1/2/4 秒退避重投。
///
/// 持 [`Weak`] 而非 [`Arc`]：本任务随进程生命周期常驻，不得延长交互服务的存活期
/// （服务已释放即退出循环）。
#[must_use]
pub fn spawn_interaction_redelivery(
    hub: WsHub,
    service: Weak<DurableInteractionService>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(REDELIVERY_SCAN_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let Some(service) = service.upgrade() else {
                return;
            };
            let candidates = match service.redelivery_candidates().await {
                Ok(candidates) => candidates,
                Err(error) => {
                    tracing::warn!(error = %error, "interaction redelivery scan failed");
                    continue;
                }
            };
            for record in candidates {
                deliver_interaction(&hub, &service, &record, InteractionDelivery::Retry).await;
            }
        }
    })
}

/// 启动期一次性对账 + 两个常驻定时任务（旧 `@PostConstruct
/// reconcileCapacityAfterRestart` L669-686、`@Scheduled(fixedRate = 1000)
/// expireDeadlines` L688、`@Scheduled(fixedDelay = 250)` L318）。
///
/// 返回两个 join handle（进程关停时 `abort`）。
pub async fn spawn_interaction_lifecycle(
    hub: WsHub,
    service: &Arc<DurableInteractionService>,
) -> [tokio::task::JoinHandle<()>; 2] {
    match service.reconcile_capacity_after_restart().await {
        Ok(pending) => tracing::info!(pending, "interaction capacity reconciled after restart"),
        Err(error) => tracing::error!(
            code = %error.code,
            error = %error.message,
            "interaction capacity reconciliation failed"
        ),
    }
    [
        service.spawn_deadline_timer(),
        spawn_interaction_redelivery(hub, Arc::downgrade(service)),
    ]
}

#[cfg(test)]
mod tests {
    use super::{InteractionDelivery, deliver_interaction};
    use crate::interaction::runs;
    use crate::interaction::service::{
        DurableInteractionService, InteractionCreateSpec, NoopInteractionPublisher,
    };
    use crate::ws::WsConfig;
    use crate::ws::hub::{OutboundFrame, WsHub};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use zk_authz::interaction::{
        AuthorizationInteractionContext, AuthorizationInteractionSpec, InteractionRecord,
        InteractionType,
    };
    use zk_db::{Db, time};

    /// 建库 + 起 Run + 装配交互服务（下行出口在测试里由 hub 直接观察）。
    async fn fixture() -> (Db, Arc<DurableInteractionService>, String) {
        let db = Db::open_in_memory().expect("in-memory db boots with migrations");
        let run_id = uuid::Uuid::new_v4().to_string();
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
        let (service, _terminations) =
            crate::run_termination::assemble(db.clone(), Arc::new(NoopInteractionPublisher));
        (db, service, run_id)
    }

    async fn elicitation(
        service: &DurableInteractionService,
        run_id: &str,
        key: &str,
    ) -> InteractionRecord {
        service
            .create(InteractionCreateSpec {
                correlation_key: key.to_owned(),
                session_id: "s1".to_owned(),
                run_id: Some(run_id.to_owned()),
                kind: InteractionType::Elicitation,
                prompt: json!({ "question": "?" }),
                allowed_decisions: vec!["answer".to_owned()],
                scope_options: Vec::new(),
                source: None,
                child_session_id: None,
            })
            .await
            .expect("interaction is created")
    }

    /// 任务 #47 现场原样取用的 v3 授权上下文（`authorization_context_json`）。
    ///
    /// 权限档下行载荷由 `view()` 从该上下文推导（protocolVersion / operationHash /
    /// actorType / options 四项），因此固定真实样本而非手搓最小结构。
    const PERMISSION_CONTEXT_V3: &str = r#"{"protocolVersion":3,"toolUseId":"Bash_0","executionAttemptId":"29be1f9e-01c5-4222-9a22-8c96f1223bbd","inputHash":"f3da9e90a5392ea2bfb160bd111e4dbabafa64ad480b12643edeaf2388b0c5b4","operationHash":"36cbfcd90c336b3eea9fa5ee8be24cb20ed7f5b9a5ee3e9fa56746bd9bfad7c2","subject":{"rootSessionId":"7a7aa3bd-ff91-4b2b-b541-19e00b99803b","rootRunId":"c84c8a0f-1b4f-412f-b726-bd34f32a4101","currentRunId":"c84c8a0f-1b4f-412f-b726-bd34f32a4101","workspaceKey":"afe4cc091eaf3fecdacc8b8297f71d6e950c8682b18e56244184ef8f578fd03c","authorizationRoot":"/Users/example/projects/zkcode"},"operation":{"authorizationSchemaVersion":1,"toolName":"Bash","action":"execute","inputHash":"f3da9e90a5392ea2bfb160bd111e4dbabafa64ad480b12643edeaf2388b0c5b4","analyzerId":"bash-v2","effects":["PROCESS","READ_RESOURCE"],"resources":[{"kind":"cwd","value":".","outsideWorkspace":false}],"inheritedEnvironmentNames":[],"endpoints":[],"risk":"SAFE","operationHash":"36cbfcd90c336b3eea9fa5ee8be24cb20ed7f5b9a5ee3e9fa56746bd9bfad7c2","redactedSummary":"pwd"},"options":[{"optionId":"allow_once","decision":"allow","scope":"once"},{"optionId":"allow_run","decision":"allow","scope":"run"},{"optionId":"allow_session","decision":"allow","scope":"session"},{"optionId":"deny","decision":"deny","scope":"once"}]}"#; // gitleaks:allow -- fixed protocol fixture with non-secret hashes

    /// 权限档交互（`create_authorization`：唯一带协议校验的 PERMISSION 入口）。
    async fn permission(
        service: &DurableInteractionService,
        run_id: &str,
        key: &str,
    ) -> InteractionRecord {
        let context: AuthorizationInteractionContext =
            serde_json::from_str(PERMISSION_CONTEXT_V3).expect("v3 authorization context");
        let prompt = json!({
            "toolUseId": "Bash_0",
            "toolName": "Bash",
            "inputSummary": "pwd",
            "riskLevel": "safe",
            "reason": "Read access requires confirmation",
            "operationHash": context.operation_hash,
        });
        service
            .create_authorization(AuthorizationInteractionSpec {
                correlation_key: key.to_owned(),
                root_session_id: "s1".to_owned(),
                run_id: Some(run_id.to_owned()),
                prompt: prompt.as_object().expect("prompt object").clone(),
                allowed_decisions: vec!["allow".to_owned(), "deny".to_owned()],
                scope_options: vec!["run".to_owned(), "session".to_owned()],
                source: "direct".to_owned(),
                authorization_context: context,
            })
            .await
            .expect("permission interaction is created")
    }

    /// 旧 L288-294：会话无绑定 transport → 不 claim、不推送（交互留库等重连）。
    #[tokio::test]
    async fn delivery_without_bound_transport_leaves_interaction_undispatched() {
        let (_db, service, run_id) = fixture().await;
        let hub = WsHub::new(WsConfig::default());
        let record = elicitation(&service, &run_id, "no-transport").await;

        deliver_interaction(&hub, &service, &record, InteractionDelivery::Initial).await;

        let current = service
            .find_by_id(&record.interaction_id)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(current.dispatch_attempts, 0, "无 transport 不得计投递次数");
        assert_eq!(current.delivery_generation, 0, "无 transport 不得自增代次");
        assert!(current.first_dispatched_at.is_none());
    }

    /// 旧 L296-307 的 INITIAL 档：claim 成功 → 代次 +1、开 ACK 窗口 → 推
    /// `interaction_created`（前端据此 ACK，ACK 是决策的前置条件）。
    #[tokio::test]
    async fn initial_delivery_claims_dispatch_and_pushes_created_view() {
        let (_db, service, run_id) = fixture().await;
        let hub = WsHub::new(WsConfig::default());
        let (tx, mut rx) = mpsc::channel(8);
        hub.register("t-1", tx);
        hub.bind("t-1", "s1", 1).expect("bind");
        let record = elicitation(&service, &run_id, "initial").await;

        deliver_interaction(&hub, &service, &record, InteractionDelivery::Initial).await;

        let current = service
            .find_by_id(&record.interaction_id)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(current.dispatch_attempts, 1);
        assert_eq!(current.delivery_generation, 1, "首投必须自增到代次 1");
        assert!(current.first_dispatched_at.is_some());
        assert!(
            current.delivery_ack_deadline_at.is_some(),
            "首投开 ACK 窗口"
        );
        assert_eq!(current.last_transport_id.as_deref(), Some("t-1"));

        let OutboundFrame::Text(text) = rx.try_recv().expect("created view is pushed") else {
            panic!("interaction delivery must push a text frame");
        };
        let envelope: serde_json::Value = serde_json::from_str(&text).expect("json frame");
        assert_eq!(envelope["type"], "interaction_created");
        assert_eq!(envelope["interactionId"], record.interaction_id);
        assert_eq!(envelope["deliveryGeneration"], 1);
    }

    /// 旧 L302-303 的 RETRY 档：`claimRedelivery` 以 `dispatch_attempts` 做 CAS
    ///——过期快照（陈旧 attempts）不得重复投递。
    #[tokio::test]
    async fn retry_delivery_is_bounded_by_dispatch_attempts_cas() {
        let (_db, service, run_id) = fixture().await;
        let hub = WsHub::new(WsConfig::default());
        let (tx, mut rx) = mpsc::channel(8);
        hub.register("t-1", tx);
        hub.bind("t-1", "s1", 1).expect("bind");
        let record = elicitation(&service, &run_id, "retry").await;
        deliver_interaction(&hub, &service, &record, InteractionDelivery::Initial).await;
        let dispatched = service
            .find_by_id(&record.interaction_id)
            .await
            .expect("read")
            .expect("row");
        let _ = rx.try_recv();

        // 快照 attempts=1 → claim 命中，代次 +1。
        deliver_interaction(&hub, &service, &dispatched, InteractionDelivery::Retry).await;
        let retried = service
            .find_by_id(&record.interaction_id)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(retried.dispatch_attempts, 2);
        assert_eq!(retried.delivery_generation, 2);
        assert!(rx.try_recv().is_ok(), "重投同样推 interaction_created");

        // 用同一份陈旧快照再投 → CAS 失败，库内不变、不推送。
        deliver_interaction(&hub, &service, &dispatched, InteractionDelivery::Retry).await;
        let unchanged = service
            .find_by_id(&record.interaction_id)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(unchanged.dispatch_attempts, 2, "陈旧快照不得再次 claim");
        assert_eq!(unchanged.delivery_generation, 2);
        assert!(rx.try_recv().is_err(), "CAS 失败不得推送");
    }

    /// 权限档（DEFAULT 模式主链路）首投必须真正落帧，且帧内字段满足前端
    /// `handleInteractionCreated` 的三重守卫（`dispatch.ts` L117-121）。
    ///
    /// 覆盖动机：`out` 指标在逐订阅者序列化之前计数，「指标说发了」并不等于
    /// 「帧进了通道」——本用例直接断言 `mpsc` 收到帧。
    #[tokio::test]
    async fn permission_initial_delivery_pushes_created_view_with_v3_payload() {
        let (_db, service, run_id) = fixture().await;
        let hub = WsHub::new(WsConfig::default());
        let (tx, mut rx) = mpsc::channel(8);
        hub.register("t-1", tx);
        hub.bind("t-1", "s1", 1).expect("bind");
        let record = permission(&service, &run_id, "permission-v3:Bash_0:36cbfcd9").await;

        deliver_interaction(&hub, &service, &record, InteractionDelivery::Initial).await;

        let OutboundFrame::Text(text) = rx.try_recv().expect("permission created view is pushed")
        else {
            panic!("permission delivery must push a text frame");
        };
        let envelope: serde_json::Value = serde_json::from_str(&text).expect("json frame");
        assert_eq!(envelope["type"], "interaction_created");
        assert_eq!(
            envelope["interactionType"], "permission",
            "前端按小写 permission 判分支"
        );
        assert_eq!(envelope["protocolVersion"], 3, "permission 期望协议版本 3");
        assert_eq!(envelope["status"], "pending", "非 pending 会被前端守卫丢弃");
        assert_eq!(
            envelope["deliveryGeneration"], 1,
            "ACK 幂等键必须随首投下发"
        );
        assert_eq!(envelope["_sessionId"], "s1");
        assert_eq!(envelope["_bindingEpoch"], 1);
        assert_eq!(
            envelope["options"].as_array().expect("options array").len(),
            4,
            "四个决策选项必须随视图下发"
        );
    }
}
