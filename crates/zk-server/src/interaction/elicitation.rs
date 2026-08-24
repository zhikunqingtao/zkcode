//! `ELICITATION` 交互适配——[`zk_tools::ElicitationSink`] 的持久实现。
//!
//! 逐字对照旧 `engine/ElicitationService.java`（只读权威规格）：一次发问 =
//! 随机 `correlation_key` + 建一条 `ELICITATION` 交互（`prompt` 为
//! `{question, options}`、`allowed_decisions = ["answer","cancel"]`、
//! `scope_options` 空、`source = "direct"`、无子会话）→ [`DurableInteractionService::await_terminal`]
//! 阻塞等待 → 重读终态行取 `response_json` → 按终态四路映射
//! （`ANSWERED → Success`、`CANCELLED`/`DENIED → Cancelled`、
//! `EXPIRED`/`UNDELIVERABLE → Timeout`、其余 → `Error`）。
//! 数据库是唯一决策权威——本适配不持任何进程内待决表。
//!
//! 差异（留痕 docs/compatibility.md §9）：旧 `catch (Exception e)` 回传
//! `e.getMessage()`，本实现回传 [`zk_authz::AuthzError`] 的 `Display`
//! （`"{code}: {message}"`）——多带稳定错误码（如 `INTERACTION_REQUIRES_RUN`），
//! 便于工具结果直接定位失败原因。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Value, json};
use zk_authz::interaction::{InteractionStatus, InteractionType};
use zk_tools::{ElicitationOption, ElicitationOutcome, ElicitationRequest, ElicitationSink};

use super::service::{DurableInteractionService, InteractionCreateSpec};

/// 持久交互支撑的发问端口实现（组合根注入 `AskUserQuestion` 工具）。
pub struct DurableElicitationSink {
    /// 交互权威（落库 / 投递 / CAS 决策 / 期限过期）。
    interactions: Arc<DurableInteractionService>,
}

impl std::fmt::Debug for DurableElicitationSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableElicitationSink")
            .finish_non_exhaustive()
    }
}

impl DurableElicitationSink {
    /// 装配。
    #[must_use]
    pub fn new(interactions: Arc<DurableInteractionService>) -> Self {
        Self { interactions }
    }

    /// 建交互 → 等终态 → 映射结局。
    async fn request(&self, request: ElicitationRequest) -> ElicitationOutcome {
        let spec = InteractionCreateSpec {
            correlation_key: uuid::Uuid::new_v4().to_string(),
            session_id: request.session_id,
            run_id: request.run_id,
            kind: InteractionType::Elicitation,
            prompt: json!({
                "question": request.question,
                "options": options_json(&request.options),
            }),
            allowed_decisions: vec!["answer".to_owned(), "cancel".to_owned()],
            scope_options: Vec::new(),
            source: Some("direct".to_owned()),
            child_session_id: None,
        };
        let record = match self.interactions.create(spec).await {
            Ok(record) => record,
            Err(error) => return failed(&error),
        };
        let status = match self
            .interactions
            .await_terminal(&record.interaction_id)
            .await
        {
            Ok(status) => status,
            Err(error) => return failed(&error),
        };
        match status {
            InteractionStatus::Answered => self.answer_of(&record.interaction_id).await,
            InteractionStatus::Cancelled | InteractionStatus::Denied => {
                ElicitationOutcome::Cancelled
            }
            InteractionStatus::Expired | InteractionStatus::Undeliverable => {
                ElicitationOutcome::Timeout
            }
            InteractionStatus::Pending => {
                ElicitationOutcome::Error(format!("Unexpected interaction state: {status}"))
            }
        }
    }

    /// 重读终态行取 `response_json`（旧 `findById` + `readValue`；缺列即
    /// `Success(None)`，与旧 `responseJson == null ? null : …` 一致）。
    async fn answer_of(&self, interaction_id: &str) -> ElicitationOutcome {
        let record = match self.interactions.find_by_id(interaction_id).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                return ElicitationOutcome::Error(format!(
                    "interaction {interaction_id} vanished after reaching a terminal state"
                ));
            }
            Err(error) => return ElicitationOutcome::Error(error.to_string()),
        };
        let Some(response) = record.response_json else {
            return ElicitationOutcome::Success(None);
        };
        match serde_json::from_str::<Value>(&response) {
            Ok(value) => ElicitationOutcome::Success(Some(value)),
            Err(error) => ElicitationOutcome::Error(error.to_string()),
        }
    }
}

impl ElicitationSink for DurableElicitationSink {
    fn request_and_wait(&self, request: ElicitationRequest) -> BoxFuture<'_, ElicitationOutcome> {
        Box::pin(async move { self.request(request).await })
    }
}

/// 选项序列化（旧 `record ElicitationOption` 的 Jackson 默认形状）。
fn options_json(options: &[ElicitationOption]) -> Value {
    Value::Array(
        options
            .iter()
            .map(|option| {
                json!({
                    "label": option.label,
                    "value": option.value,
                    "description": option.description,
                })
            })
            .collect(),
    )
}

/// 失败结局（旧 `log.warn("Durable elicitation failed: …")` + `error(msg)`）。
fn failed(error: &zk_authz::AuthzError) -> ElicitationOutcome {
    tracing::warn!(code = %error.code, message = %error.message, "durable elicitation failed");
    ElicitationOutcome::Error(error.to_string())
}

#[cfg(test)]
mod tests {
    use zk_db::{Db, time};

    use super::*;
    use crate::interaction::{NoopInteractionPublisher, runs};

    /// 建库 + 一条会话与 Run（交互需归属 Run）。
    async fn fixture() -> (Arc<DurableInteractionService>, String) {
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
            crate::run_termination::assemble(db, Arc::new(NoopInteractionPublisher));
        (service, run_id)
    }

    fn request(run_id: &str) -> ElicitationRequest {
        ElicitationRequest {
            session_id: "s1".to_owned(),
            run_id: Some(run_id.to_owned()),
            question: "Which language?".to_owned(),
            options: vec![
                ElicitationOption {
                    label: "rust".to_owned(),
                    value: "rust".to_owned(),
                    description: String::new(),
                },
                ElicitationOption {
                    label: "java".to_owned(),
                    value: "java".to_owned(),
                    description: String::new(),
                },
            ],
        }
    }

    /// 用户作答 → `Success(answer)`，且 prompt 落库形状为 `{question, options}`。
    #[tokio::test]
    async fn answered_interaction_yields_the_stored_response() {
        let (service, run_id) = fixture().await;
        let sink = DurableElicitationSink::new(service.clone());
        let waiting = tokio::spawn({
            let sink = Arc::new(sink);
            let request = request(&run_id);
            async move { sink.request(request).await }
        });

        // 等交互落库后再作答（`create` 是唯一写入点，轮询到即可）。
        let record = loop {
            let pending = service.pending("s1").await.expect("pending query");
            if let Some(record) = pending.into_iter().next() {
                break record;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        let prompt: Value = serde_json::from_str(&record.prompt_json).expect("prompt json");
        assert_eq!(prompt["question"], "Which language?");
        assert_eq!(prompt["options"][0]["label"], "rust");
        assert_eq!(prompt["options"][1]["value"], "java");
        assert_eq!(record.kind, InteractionType::Elicitation);

        service
            .decide(
                &record.interaction_id,
                record.version,
                InteractionStatus::Answered,
                Some(json!("rust")),
                Some("user_answered"),
            )
            .await
            .expect("decision lands");

        let outcome = waiting.await.expect("waiter joins");
        assert_eq!(outcome, ElicitationOutcome::Success(Some(json!("rust"))));
    }

    /// 用户取消 → `Cancelled`。
    #[tokio::test]
    async fn cancelled_interaction_maps_to_cancelled() {
        let (service, run_id) = fixture().await;
        let sink = Arc::new(DurableElicitationSink::new(service.clone()));
        let waiting = tokio::spawn({
            let sink = sink.clone();
            let request = request(&run_id);
            async move { sink.request(request).await }
        });
        let record = loop {
            let pending = service.pending("s1").await.expect("pending query");
            if let Some(record) = pending.into_iter().next() {
                break record;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        service
            .decide(
                &record.interaction_id,
                record.version,
                InteractionStatus::Cancelled,
                None,
                Some("user_cancelled"),
            )
            .await
            .expect("decision lands");
        assert_eq!(
            waiting.await.expect("waiter joins"),
            ElicitationOutcome::Cancelled
        );
    }

    /// 缺 Run → 交互服务拒绝，结局为携错误码的 `Error`（旧 `RUN_ID_REQUIRED` 同义）。
    #[tokio::test]
    async fn missing_run_is_rejected_with_a_stable_code() {
        let (service, _run_id) = fixture().await;
        let sink = DurableElicitationSink::new(service);
        let mut request = request("");
        request.run_id = None;
        match sink.request(request).await {
            ElicitationOutcome::Error(message) => {
                assert!(
                    message.starts_with("INTERACTION_REQUIRES_RUN"),
                    "message: {message}"
                );
            }
            other => panic!("expected an error outcome, got {other:?}"),
        }
    }
}
