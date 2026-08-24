//! 持久交互请求模型 + 授权侧交互网关端口。
//!
//! 逐字移植 `interaction/InteractionRequest.java`（17 行）、
//! `interaction/InteractionView.java`（36 行）、
//! `authorization/AuthorizationInteractionContext.java`（29 行），以及
//! `AuthorizationService` 对 `DurableInteractionService` 的**全部**调用面
//!（`findByCorrelationKey` / `createAuthorization` / `awaitTerminal` /
//! `findById` / `requireAnsweredOnce`）。
//!
//! # 为什么是 trait 端口
//!
//! `DurableInteractionService` 需要 WS 投递、重投退避定时器与进程内唤醒 Future，
//! 属 zk-server 关注点。依赖方向铁律要求 `zk-authz → (zk-protocol, zk-db)`，故此处
//! 只冻结**协议与调用契约**，实现落在 `zk-server::interaction`。
//!
//! # 时间表示
//!
//! 旧 record 用 `Instant`；本 crate 与 `interaction_requests` 表一致，全程用
//! 6 位微秒 ISO 文本（`zk_db::time::format_rfc3339_micros`），需要做时限运算时
//! 用 `zk_db::time::parse_rfc3339_millis` 还原毫秒。这样序列化边界零转换、
//! DB 列与内存态同构。

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::model::{AuthorizationSubject, AuthzResult, OperationDescriptor};

/// 端口方法返回的装箱 Future。
///
/// 不引入 `async-trait` 依赖：授权链只有 5 个端口方法，手写装箱可保持依赖池不变
///（新增外部 crate 需过 cargo-deny 许可证面审计）。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 交互协议版本（旧 `AuthorizationInteractionContext.PROTOCOL_VERSION`，L18）。
pub const PROTOCOL_VERSION: i64 = 3;

macro_rules! db_enum {
    (
        $(#[$meta:meta])*
        $name:ident { $( $(#[$vmeta:meta])* $variant:ident = $text:literal ),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(
                $(#[$vmeta])*
                #[doc = concat!("旧 Java 枚举常量 `", $text, "`。")]
                #[serde(rename = $text)]
                $variant,
            )+
        }

        impl $name {
            /// 旧 `Enum::name()`（诊断/日志用大写形态）。
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $( Self::$variant => $text, )+ }
            }

            /// 旧 `InteractionRequest.db(Enum)`（L15）：`name().toLowerCase(ROOT)`，
            /// 即 `interaction_requests.type` / `.status` 的落库形态。
            #[must_use]
            pub fn db(self) -> String {
                self.as_str().to_ascii_lowercase()
            }

            /// 从落库小写形态还原；未知字面量返回 `None`（调用方失败关闭）。
            #[must_use]
            pub fn from_db(value: &str) -> Option<Self> {
                match value { $( v if v.eq_ignore_ascii_case($text) => Some(Self::$variant), )+ _ => None }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

db_enum! {
    /// 交互类型（旧 `InteractionRequest.Type`，L14）。
    InteractionType {
        Permission = "PERMISSION",
        Elicitation = "ELICITATION",
        PlanApproval = "PLAN_APPROVAL",
    }
}

db_enum! {
    /// 交互状态（旧 `InteractionRequest.Status`，L15）。
    ///
    /// `PENDING` 之外的 5 个状态均为终态；`AuthorizationService.interact`
    ///（L404-412）把它们映射为 5 个稳定拒绝码。
    InteractionStatus {
        Pending = "PENDING",
        Answered = "ANSWERED",
        Denied = "DENIED",
        Expired = "EXPIRED",
        Cancelled = "CANCELLED",
        Undeliverable = "UNDELIVERABLE",
    }
}

impl InteractionStatus {
    /// 是否终态（非 `PENDING`）。
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }

    /// 旧 `AuthorizationService.interact` 的状态 → 拒绝码映射（L404-412）。
    ///
    /// `ANSWERED` 不参与该映射（调用方只在 `status != ANSWERED` 时进入）；
    /// 为保持 `switch` 的 `default` 分支语义，此处返回 `PERMISSION_NOT_GRANTED`。
    #[must_use]
    pub const fn denial_code(self) -> &'static str {
        match self {
            Self::Denied => "PERMISSION_USER_DENIED",
            Self::Expired => "INTERACTION_EXPIRED",
            Self::Cancelled => "INTERACTION_CANCELLED",
            Self::Undeliverable => "PERMISSION_UNDELIVERABLE",
            Self::Pending | Self::Answered => "PERMISSION_NOT_GRANTED",
        }
    }
}

/// 持久交互请求行（旧 `InteractionRequest` record 全 25 个组件，L6-13）。
///
/// `Serialize` 即旧 Jackson 对该 record 的默认形状：组件名 camelCase、两个枚举
/// 写 `name()` 大写字面量、`Instant` 写 ISO 文本。`InteractionController.decide`
/// 直接把本行作为 200/409 响应体回给前端（`InteractionController.java:104-105,
/// 169-171`），故序列化面即契约面。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InteractionRecord {
    /// 主键。
    pub interaction_id: String,
    /// 关联键（授权链恒为 `permission-v3:{toolUseId}:{operationHash}`）。
    pub correlation_key: String,
    /// 根会话 id。
    pub session_id: String,
    /// 发起 run id。
    pub run_id: String,
    /// 交互类型（旧组件名 `type`）。
    #[serde(rename = "type")]
    pub kind: InteractionType,
    /// 交互状态。
    pub status: InteractionStatus,
    /// 下行 prompt JSON。
    pub prompt_json: String,
    /// 允许的决策字面量 JSON（授权链恒 `["allow","deny"]`）。
    pub allowed_decisions_json: String,
    /// 可记住范围 JSON（小写 scope 名）。
    pub scope_options_json: String,
    /// 用户响应 JSON（未决时为 `None`）。
    pub response_json: Option<String>,
    /// 创建时刻（6 位微秒 ISO）。
    pub created_at: String,
    /// 投递窗口截止（`DELIVERY_WINDOW_SECONDS`）。
    pub delivery_window_ends_at: String,
    /// 首次投递时刻。
    pub first_dispatched_at: Option<String>,
    /// 投递确认截止（`ACK_WINDOW_SECONDS`）。
    pub delivery_ack_deadline_at: Option<String>,
    /// 前端确认收到时刻。
    pub received_at: Option<String>,
    /// 决策截止（`DECISION_SECONDS`，自 `received_at` 起算）。
    pub decision_deadline_at: Option<String>,
    /// 决策落库时刻。
    pub decided_at: Option<String>,
    /// 终态原因。
    pub terminal_reason: Option<String>,
    /// 来源（授权链为 `direct` / `descendant`）。
    pub source: String,
    /// 子会话 id（子代理交互透传）。
    pub child_session_id: Option<String>,
    /// 投递世代（断线重连自增，用于丢弃过期 ack）。
    pub delivery_generation: i64,
    /// 已投递次数（`BETWEEN 1 AND 3` 时才允许重投）。
    pub dispatch_attempts: i64,
    /// 最近一次投递的传输通道 id。
    pub last_transport_id: Option<String>,
    /// 授权上下文 JSON（`authorization_context_json` 列）。
    ///
    /// 旧 record **无**此组件（授权链另经 `findById` + 单独读列取用），故不进
    /// REST 响应体，以保持旧 25 组件形状逐键一致。
    #[serde(skip_serializing)]
    pub authorization_context_json: Option<String>,
    /// 更新时刻。
    pub updated_at: String,
    /// 乐观锁版本（CAS 权威）。
    pub version: i64,
}

/// 单个决策选项（旧 `AuthorizationInteractionContext.DecisionOption`，L19）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionOption {
    /// 选项 id（`allow_once` / `allow_run` / `allow_session` / `allow_workspace` / `deny`）。
    #[serde(rename = "optionId")]
    pub option_id: String,
    /// 决策（`allow` / `deny`）。
    pub decision: String,
    /// 范围小写名（`once` / `run` / `session` / `workspace`）。
    pub scope: String,
}

/// 交互上下文中的主体快照（旧 `AuthorizationSubjectData`，L20-28）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationSubjectData {
    /// 根会话 id。
    #[serde(rename = "rootSessionId")]
    pub root_session_id: String,
    /// 根 run id。
    #[serde(rename = "rootRunId")]
    pub root_run_id: String,
    /// 当前 run id。
    #[serde(rename = "currentRunId")]
    pub current_run_id: String,
    /// 工作区身份键。
    #[serde(rename = "workspaceKey")]
    pub workspace_key: String,
    /// 授权根（字符串形态，旧 `authorizationRoot().toString()`）。
    #[serde(rename = "authorizationRoot")]
    pub authorization_root: String,
}

impl AuthorizationSubjectData {
    /// 旧 `AuthorizationSubjectData.from(AuthorizationSubject)`（L21-24）。
    #[must_use]
    pub fn from_subject(value: &AuthorizationSubject) -> Self {
        Self {
            root_session_id: value.root_session_id.clone(),
            root_run_id: value.root_run_id.clone(),
            current_run_id: value.current_run_id.clone(),
            workspace_key: value.workspace_key.clone(),
            authorization_root: value.authorization_root.to_string_lossy().into_owned(),
        }
    }

    /// 旧 `AuthorizationSubjectData.toSubject()`（L25-27）。
    #[must_use]
    pub fn to_subject(&self) -> AuthorizationSubject {
        AuthorizationSubject {
            root_session_id: self.root_session_id.clone(),
            root_run_id: self.root_run_id.clone(),
            current_run_id: self.current_run_id.clone(),
            workspace_key: self.workspace_key.clone(),
            authorization_root: std::path::PathBuf::from(&self.authorization_root),
        }
    }
}

/// 校验 v3 决策并原子创建授权所需的持久权限事实
///（旧 `AuthorizationInteractionContext` record，L7-15）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationInteractionContext {
    /// 协议版本（恒 [`PROTOCOL_VERSION`]）。
    #[serde(rename = "protocolVersion")]
    pub protocol_version: i64,
    /// 工具调用 id。
    #[serde(rename = "toolUseId")]
    pub tool_use_id: String,
    /// 本次执行尝试 id。
    #[serde(rename = "executionAttemptId")]
    pub execution_attempt_id: String,
    /// 冻结输入哈希。
    #[serde(rename = "inputHash")]
    pub input_hash: String,
    /// 授权身份哈希。
    #[serde(rename = "operationHash")]
    pub operation_hash: String,
    /// 主体快照。
    pub subject: AuthorizationSubjectData,
    /// 操作描述符。
    pub operation: OperationDescriptor,
    /// 决策选项。
    pub options: Vec<DecisionOption>,
}

/// 创建授权交互的入参（旧 `interactions.createAuthorization(...)` 的 9 个实参，
/// `AuthorizationService.java` L392-397）。
#[derive(Debug, Clone)]
pub struct AuthorizationInteractionSpec {
    /// 关联键。
    pub correlation_key: String,
    /// 根会话 id。
    pub root_session_id: String,
    /// 发起 run id（`context.currentRunId()`）。
    pub run_id: Option<String>,
    /// 下行 prompt。
    pub prompt: Map<String, Value>,
    /// 允许的决策（恒 `["allow","deny"]`）。
    pub allowed_decisions: Vec<String>,
    /// 可记住范围（小写）。
    pub scope_options: Vec<String>,
    /// 来源（`direct` / `descendant`）。
    pub source: String,
    /// 授权上下文。
    pub authorization_context: AuthorizationInteractionContext,
}

/// 授权链对持久交互服务的**全部**依赖面。
///
/// 实现见 `zk-server::interaction::DurableInteractionService`。
pub trait InteractionGateway: Send + Sync {
    /// 旧 `findByCorrelationKey(runId, correlationKey)`：同 run 内幂等复用。
    ///
    /// 这是「断线重连不重复弹窗」的关键——`(run_id, correlation_key)` 上有
    /// `UNIQUE` 约束，重放的同一工具调用会命中既有行而不是新建交互。
    fn find_by_correlation_key<'a>(
        &'a self,
        run_id: Option<&'a str>,
        correlation_key: &'a str,
    ) -> BoxFuture<'a, AuthzResult<Option<InteractionRecord>>>;

    /// 旧 `createAuthorization(...)`：落库 + 发布 `InteractionCreatedEvent`（提交后推 WS）。
    fn create_authorization(
        &self,
        spec: AuthorizationInteractionSpec,
    ) -> BoxFuture<'_, AuthzResult<InteractionRecord>>;

    /// 旧 `awaitTerminal(interactionId).join()`：阻塞到终态。
    ///
    /// DB 行是唯一权威；本 Future 只负责唤醒，超时与过期由服务端定时器写库驱动。
    fn await_terminal<'a>(
        &'a self,
        interaction_id: &'a str,
    ) -> BoxFuture<'a, AuthzResult<InteractionStatus>>;

    /// 旧 `findById(interactionId)`。
    fn find_by_id<'a>(
        &'a self,
        interaction_id: &'a str,
    ) -> BoxFuture<'a, AuthzResult<Option<InteractionRecord>>>;

    /// 旧 `requireAnsweredOnce(interactionId, subject, descriptor, toolUseId)`。
    ///
    /// **同步**且接受调用方的连接：旧源注释「必须在调用方持有的项目库有界事务内
    /// 执行」是硬要求 —— `USER_ONCE` 授权在最终复检时必须与 `tool_started` 事件
    /// 落在同一写事务里重新确认该交互仍是 `ANSWERED`、且主体/操作/toolUseId 完全
    /// 一致，否则一次性批准可在检查与执行之间被并发复用。故此方法刻意不做成
    /// `async`：它只能在 `zk_db::Db::with_writer` 闭包内被调用。
    ///
    /// 旧源抛 `IllegalStateException`；此处返回 `AuthzError`，码由实现给出。
    ///
    /// # Errors
    /// 交互不存在、状态已非 `ANSWERED`、或主体 / 操作 / `toolUseId` 与批准时不一致
    /// 时返回拒绝（失败关闭，一次性批准不得被复用）。
    fn require_answered_once_in_tx(
        &self,
        conn: &rusqlite::Connection,
        interaction_id: Option<&str>,
        subject: &AuthorizationSubject,
        descriptor: &OperationDescriptor,
        tool_use_id: &str,
    ) -> AuthzResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 旧 `InteractionRequest.db(Enum)`（`InteractionRequest.java:15`）。
    #[test]
    fn db_form_is_lowercase() {
        assert_eq!(InteractionStatus::Pending.db(), "pending");
        assert_eq!(InteractionStatus::Undeliverable.db(), "undeliverable");
        assert_eq!(InteractionType::PlanApproval.db(), "plan_approval");
    }

    /// 旧 `AuthorizationService.interact` 状态映射（`AuthorizationService.java:404-412`）。
    #[test]
    fn denial_codes_match_baseline_switch() {
        assert_eq!(
            InteractionStatus::Denied.denial_code(),
            "PERMISSION_USER_DENIED"
        );
        assert_eq!(
            InteractionStatus::Expired.denial_code(),
            "INTERACTION_EXPIRED"
        );
        assert_eq!(
            InteractionStatus::Cancelled.denial_code(),
            "INTERACTION_CANCELLED"
        );
        assert_eq!(
            InteractionStatus::Undeliverable.denial_code(),
            "PERMISSION_UNDELIVERABLE"
        );
        assert_eq!(
            InteractionStatus::Pending.denial_code(),
            "PERMISSION_NOT_GRANTED"
        );
    }

    /// 旧 `AuthorizationSubjectData.from` / `toSubject`
    ///（`AuthorizationInteractionContext.java:21-27`）。
    #[test]
    fn subject_data_round_trips() {
        let subject = AuthorizationSubject {
            root_session_id: "s1".into(),
            root_run_id: "r1".into(),
            current_run_id: "r2".into(),
            workspace_key: "wk".into(),
            authorization_root: std::path::PathBuf::from("/tmp/ws"),
        };
        let data = AuthorizationSubjectData::from_subject(&subject);
        assert_eq!(data.authorization_root, "/tmp/ws");
        assert_eq!(data.to_subject(), subject);
    }

    /// `PROTOCOL_VERSION` 冻结值（`AuthorizationInteractionContext.java:18`）。
    #[test]
    fn protocol_version_is_three() {
        assert_eq!(PROTOCOL_VERSION, 3);
    }

    /// `from_db` 接受落库小写形态（`InteractionRequest.java:15` 的逆运算）。
    #[test]
    fn from_db_parses_lowercase() {
        assert_eq!(
            InteractionStatus::from_db("answered"),
            Some(InteractionStatus::Answered)
        );
        assert_eq!(
            InteractionType::from_db("permission"),
            Some(InteractionType::Permission)
        );
        assert_eq!(InteractionStatus::from_db("bogus"), None);
    }
}
