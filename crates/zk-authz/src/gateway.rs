//! 工具执行准入网关。
//!
//! 逐字移植 `authorization/ToolExecutionGateway.java`（71 行）——旧源自述
//! 「生产代码中唯一允许调用 `Tool.call()` 的入口」。
//!
//! # 唯一入口不变量在 Rust 侧的落点（偏离 GW-01，EQUIVALENT）
//!
//! 旧 `execute(...)` 在完成准入后**直接**调用 `tool.call(...)` 并返回
//! `ToolResult`。zk-authz 受依赖方向铁律约束（`zk-authz → (zk-protocol, zk-db)`）
//! 不得依赖 zk-tools，因此本模块只承载准入语义、由 [`ToolExecutionGateway::admit`]
//! 表达；真正的 `tool.call()` 由 zk-engine 的 `ToolExecutor` 在 `admit` 返回
//! `Ok` **之后**执行。旧源「事件事务先提交、工具再跑」的时序被完整保留：
//! `admit` 返回即代表 `tool_started` 事务已提交（这也顶替了旧
//! `executionStartedAction` 回调 —— Rust 的 `await` 返回天然是 happens-after，
//! 见偏离 GW-02）。唯一入口不变量由 zk-engine 侧「`ToolExecutor` 只经 `admit`
//! 放行」这一结构约束保证，不弱于旧源的 `@Service` 单入口约定。
//!
//! # 三段式时序（与旧源逐格一致）
//!
//! 1. `context.currentRunId == null` → `AUTHORIZATION_ANCESTRY_INVALID`
//!    （在 `try` 之外，**不**记录终局拒绝）；
//! 2. `finalDynamicRecheck`（动态环境/资源漂移复检，无 DB 写）；
//! 3. 一个有界写事务内串行：`finalGrantRecheckInCurrentTransaction` →
//!    可选 `admissionAction` → `tool_started` 事件 → commit。
//!
//! 步 2/3 抛出的**授权异常**统一走 `recordFinalDenial` 后原样上抛；准入动作
//! 自身的拒绝（旧 `AdmissionException`）与 DB 故障**不**记录终局拒绝——旧源
//! `catch` 只捕获 `AuthorizationException`，此语义原样保留。

use std::sync::Arc;

use zk_db::{Db, DbError};

use crate::diagnostic;
use crate::model::{AuthorizedOperation, AuthzError, DiagnosticOutcome, EvaluationStage};
use crate::service::AuthorizationService;
use crate::tool_facts::{RunEventSink, ToolFacts, ToolUseContext};

/// 准入动作自身的拒绝（旧 `ToolExecutionGateway.AdmissionException`，L14-19）。
///
/// 旧源它是 `RuntimeException` 而非 `AuthorizationException`，故**不**被
/// `execute` 的 `catch` 捕获、**不**触发 `recordFinalDenial`；本类型保留该区分。
#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct AdmissionRejection {
    /// 稳定错误码（旧 `AdmissionException#code()`）。
    pub code: String,
    /// 人读消息（旧源以 `cause` 承载，本实现扁平化为字符串）。
    pub message: String,
}

impl AdmissionRejection {
    /// 构造准入拒绝。
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// 与准入事件同事务执行的短声明动作（旧 `Runnable admissionAction`，L34）。
///
/// 旧源注释：「仅允许有界元数据检查，不得产生文件副作用或网络 I/O」。本别名把
/// 旧源隐含的「在同一事务里」显式化为参数 —— 实现方必须复用传入连接，禁止另开
/// 事务或另取连接。
pub type AdmissionAction =
    Arc<dyn Fn(&rusqlite::Connection) -> Result<(), AdmissionRejection> + Send + Sync>;

/// 准入失败分类。
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// 授权拒绝（旧 `AuthorizationException`）；抛出前已 `recordFinalDenial`。
    #[error(transparent)]
    Denied(#[from] AuthzError),
    /// 准入动作拒绝（旧 `AdmissionException`）；**不**记录终局拒绝。
    #[error(transparent)]
    Admission(#[from] AdmissionRejection),
    /// 有界写事务自身故障（旧源为 `DataAccessException`）；**不**记录终局拒绝。
    #[error(transparent)]
    Db(#[from] DbError),
}

/// 唯一工具执行准入入口（旧 `ToolExecutionGateway`，L13）。
pub struct ToolExecutionGateway {
    authorization: Arc<AuthorizationService>,
    runs: Arc<dyn RunEventSink>,
    db: Db,
}

impl std::fmt::Debug for ToolExecutionGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolExecutionGateway")
            .finish_non_exhaustive()
    }
}

impl ToolExecutionGateway {
    /// 装配网关（旧构造器，`ToolExecutionGateway.java:22-24`）。
    #[must_use]
    pub fn new(
        authorization: Arc<AuthorizationService>,
        runs: Arc<dyn RunEventSink>,
        db: Db,
    ) -> Self {
        Self {
            authorization,
            runs,
            db,
        }
    }

    /// 无准入动作的准入（旧一参重载 `execute(tool, allowed, context)`，L26-28）。
    ///
    /// # Errors
    /// 见 [`Self::admit_with`]。
    pub async fn admit(
        &self,
        tool: &dyn ToolFacts,
        allowed: &AuthorizedOperation,
        context: &ToolUseContext,
    ) -> Result<(), GatewayError> {
        self.admit_with(tool, allowed, context, None).await
    }

    /// 完成动态环境与授权记录复检、提交执行准入事件（旧 `execute(...)` 五参主体，
    /// `ToolExecutionGateway.java:44-70`）。
    ///
    /// 返回 `Ok(())` 即代表 `tool_started` 事务**已提交**，调用方此后方可执行
    /// `allowed.execution_input`。数据库短事务在工具真正执行前结束，避免长任务
    /// 占用 `SQLite` 写锁（旧源同注释，L30-31）。
    ///
    /// # Errors
    /// - [`GatewayError::Denied`]：`current_run_id` 缺失
    ///   （`AUTHORIZATION_ANCESTRY_INVALID`）、动态复检失败、事务内授权记录复检
    ///   失败。后两者抛出前已写入终局拒绝诊断。
    /// - [`GatewayError::Admission`]：`admission` 动作拒绝。
    /// - [`GatewayError::Db`]：有界写事务故障。
    pub async fn admit_with(
        &self,
        tool: &dyn ToolFacts,
        allowed: &AuthorizedOperation,
        context: &ToolUseContext,
        admission: Option<AdmissionAction>,
    ) -> Result<(), GatewayError> {
        // 旧源 L47-48：祖先校验在 try 之外 —— 无 Run 即无处记录诊断。
        let Some(run_id) = context.current_run_id.clone() else {
            return Err(GatewayError::Denied(AuthzError::new(
                "AUTHORIZATION_ANCESTRY_INVALID",
                "Authorized tool execution requires a persisted Run",
            )));
        };
        match self
            .admit_inner(tool, allowed, context, admission, run_id)
            .await
        {
            Err(GatewayError::Denied(denied)) => {
                // 旧源 L66-69：catch (AuthorizationException denied)。
                self.authorization
                    .record_final_denial(allowed, context, &denied.code);
                Err(GatewayError::Denied(denied))
            }
            other => other,
        }
    }

    /// 旧源 `try` 块主体（L49-65）。
    async fn admit_inner(
        &self,
        tool: &dyn ToolFacts,
        allowed: &AuthorizedOperation,
        context: &ToolUseContext,
        admission: Option<AdmissionAction>,
        run_id: String,
    ) -> Result<(), GatewayError> {
        // 旧源 L50。
        self.authorization
            .final_dynamic_recheck(tool, allowed, context)?;
        // 旧源 L51-57：ALLOW / FINAL_RECHECK 诊断载荷。
        let event = diagnostic::payload(
            &allowed.subject,
            &allowed.descriptor,
            context,
            &allowed.execution_attempt_id,
            DiagnosticOutcome::Allow,
            EvaluationStage::FinalRecheck,
            allowed.source,
            &allowed.reason_code,
            allowed.grant_id.as_deref(),
            allowed.grant_scope,
            allowed.interaction_id.as_deref(),
        );
        let authorization = Arc::clone(&self.authorization);
        let runs = Arc::clone(&self.runs);
        let allowed = allowed.clone();
        let context = context.clone();
        let tool_use_id = context.tool_use_id.clone();
        // 旧源 L58-64：runs.executeBoundedWrite(...)。zk-db 的单 writer Mutex +
        // busy_timeout=5s 等价于旧 executeWriteBounded 的 5s 上限（偏离 GW-03）。
        //
        // 业务拒绝以 Ok(Err(..)) 回传而非 Err(DbError)：既能让事务在闭包退出时
        // 自然回滚（未 commit 即回滚），又不把授权语义压扁进 DB 错误分类。
        self.db
            .with_writer(move |conn| {
                let tx = conn.transaction()?;
                if let Err(denied) = authorization
                    .final_grant_recheck_in_current_transaction(&tx, &allowed, &context)
                {
                    return Ok(Err(GatewayError::Denied(denied)));
                }
                if let Some(action) = admission
                    && let Err(rejected) = action(&tx)
                {
                    return Ok(Err(GatewayError::Admission(rejected)));
                }
                runs.append_event_in_current_write(
                    &tx,
                    &run_id,
                    "tool_started",
                    tool_use_id.as_deref(),
                    &event,
                );
                tx.commit()?;
                Ok(Ok(()))
            })
            .await?
    }
}
