//! Run 终止协调器（旧 `run/RunTerminationCoordinator.java`，115 行）。
//!
//! 旧源类文档：*“The only coordinator allowed to move an active Run through
//! cancellation to a terminal state.”*——把活动 Run 经 `cancelling` 推向终态的
//! **唯一**入口。旧源以 `@EventListener onTerminationRequested` 订阅
//! `RunTerminationRequestedEvent`，而该事件全仓只有两个发布点，都在
//! `DurableInteractionService`：
//!
//! | 发布点 | `reason` | `detail` |
//! |---|---|---|
//! | `createInternal` L94-102（待决交互超 `MAX_WAITING`） | `INTERACTION_CAPACITY_EXCEEDED` | `"Maximum pending interactions reached"` |
//! | `expire` L721-724（投递窗口 / 决策期限耗尽） | `INTERACTION_EXPIRED` | `request.terminalReason()` |
//!
//! 故 zkcode 把「事件总线 + 唯一监听者」收敛为
//! [`crate::interaction::RunTerminationRequest`] 端口 +
//! [`RunTerminationCoordinator`] 实现（见 `docs/compatibility.md` §8 I-02）。
//!
//! # 逐行对照
//!
//! | 本模块 | 旧源 |
//! |---|---|
//! | [`RunTerminationCoordinator::terminate`] | `terminate(runId, reason, detail)` L40-75 |
//! | [`RunTerminationCoordinator::cancel_by_user`] | `cancelByUser(runId, detail)` L37-39 |
//! | `impl RunTerminationRequest for ...` | `@EventListener onTerminationRequested` L78-81 |
//! | [`assemble`] | Spring 容器对 `DurableInteractionService` ↔ `RunTerminationCoordinator` 的循环装配 |
//!
//! # 环的处理
//!
//! 旧源两者互不直接引用：服务往事件总线 `publishEvent`，协调器从总线收，
//! 循环由 Spring 的事件总线天然打断。Rust 侧协调器是服务的构造入参，服务又是
//! 协调器的回调目标，故 [`assemble`] 用 [`Arc::new_cyclic`] 在服务构造完成前
//! 就把 [`Weak`] 句柄交给协调器——**服务只能经 [`assemble`] 存在**，因此不存在
//! 「端口未接线」的中间状态（对比 [`crate::authz::WsInteractionPublisher`] 的
//! `OnceLock` + `bind` 方案：那里的 publisher 可以独立于服务先建）。
//!
//! # 偏离留痕
//!
//! - **R-04**（承接 [`zk_db::run`]）：旧源 L48-67 在两步之间关闭准入并停子系统
//!   （`executions.beginTermination` / `abortRun` / `tools.cancelRunDetailed` /
//!   `processes.cancelRunDetailed` / `awaitQuiescence(2s)`），未静默则把终态原因
//!   改判为 `TOOL_TERMINATION_UNCONFIRMED` / `PROCESS_TERMINATION_UNCONFIRMED`。
//!   `RunExecutionRegistry` / `ManagedProcessRunner` 的静默确认属 **M-RUN
//!   里程碑**，本模块取旧源「已静默」路径（`quiescent && allTerminated`），
//!   终止实际由引擎侧三层取消令牌树驱动。DEFERRED。
//! - **R-06**：旧源 `recordSummary` 经 `BestEffortObservabilityRecorder.record`
//!   写 `run_termination_summary`。该记录器（`observability/BestEffortObservabilityRecorder.java`）
//!   写的是独立的 `observability-events` **日志文件**、不入 `run_event_log`，且
//!   `@Autowired(required = false)`（缺省即整段跳过）。zkcode 尚未移植可观测
//!   记录器（**M-OBS**），此处不代偿——若改写 `run_event_log` 反而会多出旧源
//!   不产生的行。DEFERRED。
//! - **R-07**：旧 `Result(transition, processes, terminationConfirmed)` 的后两个
//!   字段来自 R-04 未移植的子系统摘要，故本模块只返回
//!   [`TransitionResult`]。旧源两个发布点都忽略返回值（`@EventListener` 无返回
//!   通道），语义无损。

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use zk_authz::model::{AuthzError, AuthzResult};
use zk_db::Db;

use crate::interaction::service::{InteractionPublisher, RunTerminationRequest};
use crate::interaction::{DurableInteractionService, TransitionResult, runs};

/// 装配交互服务与其终止协调器（旧 Spring 容器的循环 Bean 装配）。
///
/// 组装根唯一入口：`AuthzStack::build` 与测试夹具都经此函数建服务，保证终止端口
/// **恒为**真协调器（不存在「未接线」形态）。返回的协调器句柄供需要主动终止 Run
/// 的调用方使用（旧源里 `RunTerminationCoordinator` 同样是可注入的独立 Bean）；
/// 交互过期 / 容量耗尽两条路径无需它——服务已内部持有同一实例。
///
/// # Panics
///
/// 理论不可达：[`Arc::new_cyclic`] 契约保证初始化闭包恰好执行一次并同步返回，
/// 故句柄必已写入。此处不做静默兜底（造第二个协调器实例会偏离旧源的单 Bean
/// 语义），宁可让契约被破坏时立刻炸出。
#[must_use]
pub fn assemble(
    db: Db,
    publisher: Arc<dyn InteractionPublisher>,
) -> (
    Arc<DurableInteractionService>,
    Arc<RunTerminationCoordinator>,
) {
    let mut handle = None;
    let interactions = Arc::new_cyclic(|weak| {
        let coordinator = Arc::new(RunTerminationCoordinator::new(db.clone(), weak.clone()));
        handle = Some(Arc::clone(&coordinator));
        DurableInteractionService::new(db, publisher, coordinator)
    });
    let coordinator = handle.expect("Arc::new_cyclic runs its initializer exactly once");
    (interactions, coordinator)
}

/// 把活动 Run 经 `cancelling` 推向终态的唯一协调器（旧
/// `RunTerminationCoordinator`）。
pub struct RunTerminationCoordinator {
    /// Run 生命周期 SQL 的唯一权威（旧注入的 `RunControlService runs`）。
    db: Db,
    /// 交互权威回指（旧注入的 `DurableInteractionService interactions`）。
    ///
    /// [`Weak`] 而非 [`Arc`]：协调器由服务持有，反向持强引用即成环泄漏。
    interactions: Weak<DurableInteractionService>,
}

impl std::fmt::Debug for RunTerminationCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunTerminationCoordinator")
            .field("bound", &self.interactions.strong_count().gt(&0))
            .finish_non_exhaustive()
    }
}

impl RunTerminationCoordinator {
    /// 装配（仅 [`assemble`] 调用——`interactions` 须是同一 [`Arc`] 的弱句柄）。
    #[must_use]
    fn new(db: Db, interactions: Weak<DurableInteractionService>) -> Self {
        Self { db, interactions }
    }

    /// 旧 `cancelByUser(runId, detail)`（L37-39）。
    ///
    /// # Errors
    /// 见 [`Self::terminate`]。
    pub async fn cancel_by_user(
        &self,
        run_id: &str,
        detail: Option<&str>,
    ) -> AuthzResult<TransitionResult> {
        self.terminate(run_id, runs::EXIT_USER_CANCELLED, detail)
            .await
    }

    /// 旧 `terminate(runId, reason, detail)`（L40-75）。
    ///
    /// 两步语义逐值对齐旧源：
    ///
    /// 1. **L41-42**：`interactions.beginRunTermination(runId, reason,
    ///    detail == null ? reason.dbValue() : detail)`——同一写事务内把 Run 切到
    ///    `cancelling` 并级联终结其全部待决交互（归还容量配额 + 唤醒等待者 +
    ///    推 `interaction_terminal`）。**不能**简化为
    ///    [`zk_db::run::terminate_in_current_write`]：那条路径只做
    ///    `requestCancel`，会漏掉交互级联，导致容量信号量泄漏。
    /// 2. **L43-46**：迁移结果非 `APPLIED` 即原样返回、不落终态（Run 已在终态，
    ///    或已有其它终止流程在跑）。
    /// 3. **L69-73**：`user_cancelled` → `runs.cancel`，其余 →
    ///    `runs.fail(runId, reason, detail)`（`detail` 原样传，不做 L42 的兜底），
    ///    经 [`Db::finish_run`] 落库。
    ///
    /// 旧源 L48-67 的子系统停止与静默确认见模块级 R-04。
    ///
    /// # Errors
    /// 交互级联取消（`INTERACTION_CANCEL_COUNT_MISMATCH` / 写库失败）或终态写库
    /// 失败时返回错误。
    pub async fn terminate(
        &self,
        run_id: &str,
        exit_reason: &str,
        detail: Option<&str>,
    ) -> AuthzResult<TransitionResult> {
        let Some(interactions) = self.interactions.upgrade() else {
            // 不可达：服务只能经 `assemble` 的 `Arc::new_cyclic` 存在，upgrade
            // 失败仅出现在该 Arc 析构期间——那时不可能有终止请求在飞。
            tracing::error!(
                run_id,
                exit_reason,
                "run termination skipped: interaction service already dropped"
            );
            return Ok(TransitionResult::NotFound);
        };
        // 旧源 L41-42。
        let requested = interactions
            .begin_run_termination(run_id, exit_reason, detail.unwrap_or(exit_reason))
            .await?;
        // 旧源 L43-46（`recordSummary` 见 R-06）。
        if requested.run_transition != TransitionResult::Applied {
            tracing::debug!(
                run_id,
                exit_reason,
                transition = requested.run_transition.as_str(),
                "run termination not applicable"
            );
            return Ok(requested.run_transition);
        }
        // 旧源 L69-73。
        let terminal = self
            .db
            .finish_run(run_id, exit_reason, detail)
            .await
            .map_err(|error| AuthzError::new("RUN_TERMINATION_FAILED", error.to_string()))?;
        tracing::info!(
            run_id,
            exit_reason,
            detail,
            interactions_cancelled = requested.interactions_cancelled,
            transition = terminal.as_str(),
            "run terminated"
        );
        Ok(terminal)
    }
}

impl RunTerminationRequest for RunTerminationCoordinator {
    /// 旧 `@EventListener onTerminationRequested(event)`（L78-81）。
    ///
    /// 失败只记日志：旧源的发布方 `safePublish`
    /// （`DurableInteractionService.java:753-760`）捕获监听者抛出的
    /// `RuntimeException` 并 `log.error`，已提交的交互事务不回滚。
    fn request<'a>(
        &'a self,
        run_id: &'a str,
        exit_reason: &'a str,
        detail: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Err(error) = self.terminate(run_id, exit_reason, detail).await {
                tracing::error!(
                    run_id,
                    exit_reason,
                    detail,
                    code = %error.code,
                    %error,
                    "run termination request failed"
                );
            }
        })
    }
}
