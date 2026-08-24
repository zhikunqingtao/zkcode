//! 交互式取值端口（`Elicitation`）——工具侧发问、等待用户终态决策。
//!
//! 对照旧 `engine/ElicitationService.java`（只读权威规格）：一次 `requestAndWait`
//! = 建一条 `ELICITATION` 持久交互（`allowed_decisions = ["answer","cancel"]`、
//! `source = "direct"`）→ 阻塞等待终态 → 按终态映射四类结局
//! （`ANSWERED → Success`、`CANCELLED`/`DENIED → Cancelled`、
//! `EXPIRED`/`UNDELIVERABLE → Timeout`、其余 → `Error`）。
//!
//! 依赖方向铁律禁止 `zk-tools → zk-server`，故此处只定义端口
//! [`ElicitationSink`]，生产实现（持久交互 + WS 下行）落 zk-server 组合根——
//! 范式与 [`crate::snapshot::SnapshotSink`] / [`crate::executor::ToolSafetyGuard`]
//! 一致。
//!
//! 差异（留痕 docs/compatibility.md §9）：旧 `requestAndWait` 的
//! `timeoutMs` 形参**被忽略**（形参名即 `ignoredTimeoutMs`），超时权归数据库
//! 侧的交互过期；本端口同样不接收超时参数，工具侧另加一层
//! 5 分钟本地看门狗（见 [`crate::ask_user_question`]）以保证进程内不会
//! 无限期挂起。

use futures::future::BoxFuture;

/// 一个可选项（旧 `record ElicitationOption(String label, String value,
/// String description)`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElicitationOption {
    /// 展示标签。
    pub label: String,
    /// 回传值（旧调用点逐字传 `new ElicitationOption(label, label, desc)`，
    /// 即 `value` 与 `label` 同值）。
    pub value: String,
    /// 补充说明（缺省空串）。
    pub description: String,
}

/// 一次发问（`run_id` 缺失时持久交互侧应以 `INTERACTION_REQUIRES_RUN` 拒绝，
/// 与旧 `@Deprecated requestAndWait` 的 `RUN_ID_REQUIRED` 同义）。
#[derive(Clone, Debug)]
pub struct ElicitationRequest {
    /// 归属会话。
    pub session_id: String,
    /// 归属 Run。
    pub run_id: Option<String>,
    /// 问题正文。
    pub question: String,
    /// 可选项列表。
    pub options: Vec<ElicitationOption>,
}

/// 发问结局（旧 `ElicitationResponse.Status` 四态）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ElicitationOutcome {
    /// 用户已作答（`None` = 终态无 `response_json`，旧亦为合法的 null 值）。
    Success(Option<serde_json::Value>),
    /// 用户取消 / 拒绝。
    Cancelled,
    /// 过期或不可投递。
    Timeout,
    /// 其他失败（携原因文案）。
    Error(String),
}

/// 交互式取值出口（生产实现在 zk-server 组合根）。
pub trait ElicitationSink: Send + Sync {
    /// 发问并等待用户终态决策。
    ///
    /// 实现不得 panic：内部失败一律映射为 [`ElicitationOutcome::Error`]
    /// （旧实现以 `catch (Exception e) → ElicitationResponse.error` 达到同效）。
    fn request_and_wait(&self, request: ElicitationRequest) -> BoxFuture<'_, ElicitationOutcome>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定回答的桩实现——验证端口对象安全。
    struct StubSink(ElicitationOutcome);

    impl ElicitationSink for StubSink {
        fn request_and_wait(
            &self,
            _request: ElicitationRequest,
        ) -> BoxFuture<'_, ElicitationOutcome> {
            Box::pin(async move { self.0.clone() })
        }
    }

    #[tokio::test]
    async fn sink_is_object_safe_and_returns_outcome() {
        let sink: std::sync::Arc<dyn ElicitationSink> =
            std::sync::Arc::new(StubSink(ElicitationOutcome::Cancelled));
        let outcome = sink
            .request_and_wait(ElicitationRequest {
                session_id: "s".to_owned(),
                run_id: Some("r".to_owned()),
                question: "pick".to_owned(),
                options: vec![ElicitationOption {
                    label: "a".to_owned(),
                    value: "a".to_owned(),
                    description: String::new(),
                }],
            })
            .await;
        assert_eq!(outcome, ElicitationOutcome::Cancelled);
    }
}
