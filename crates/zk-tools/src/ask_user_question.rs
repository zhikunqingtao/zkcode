//! `AskUserQuestion` 工具——向用户提多选问题并阻塞等待作答。
//!
//! 逐字对照旧 `tool/interaction/AskUserQuestionTool.java`（只读权威规格）：
//! 工具名 `AskUserQuestion`、入参 `questions`、`1-4` 问 × 每问 `2-4` 选项的
//! 数量校验、逐题串行发问、`QUESTION_TIMEOUT_MS = 5 * 60 * 1000`、答案键
//! `"q" + (i + 1)`、结果 JSON `{questions, answers}`，以及四条结局错误码
//! （`ELICITATION_CANCELLED` / `ELICITATION_EXPIRED` / `ELICITATION_FAILED` /
//! `ELICITATION_RESULT_SERIALIZATION_FAILED`）。
//!
//! 三路竞态（本任务判据）：`tokio::select!` 在
//! ① [`ElicitationSink::request_and_wait`] 的完成、
//! ② [`QUESTION_TIMEOUT`] 本地看门狗、
//! ③ [`ToolContext::cancel`] 取消令牌 三者之间取先到者。
//!
//! 差异（留痕 docs/compatibility.md §9）：
//!
//! - 旧 `ElicitationService.requestAndWait` **忽略** `timeoutMs`（超时权在
//!   数据库侧交互过期），故旧实现在数据库过期机制失效时会无限期挂住工具
//!   线程；本实现保留数据库权威的同时另加进程内 5 分钟看门狗，超时按旧
//!   `TIMEOUT` 分支产出 `ELICITATION_EXPIRED`，语义一致且不会挂死。
//! - 取消令牌命中时按旧 `CANCELLED` 分支产出 `ELICITATION_CANCELLED`——旧
//!   实现无工具级取消面（取消经数据库交互级联到 `CANCELLED` 状态），本实现
//!   多一条更快的本地路径，终态错误码相同。
//! - 未接线 [`ElicitationSink`] 时按旧 `ERROR` 分支产出 `ELICITATION_FAILED`
//!   （旧实现的服务由容器保证非空，本实现的 `None` 只出现在单测 / 未装配的
//!   降级部署）。
//! - `questions` 缺失 / 非数组时旧实现走同一条 `null || isEmpty` 判断，本实现
//!   与之逐字一致地返回 `ELICITATION_QUESTION_COUNT_INVALID`。
//! - 旧 schema 声明的 `multiSelect` 未被 `call` 消费（发问只下发
//!   `question` + `options`），本实现保留 schema 字段、同样不消费。

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::elicitation::{
    ElicitationOption, ElicitationOutcome, ElicitationRequest, ElicitationSink,
};
use crate::file_state::session_key;
use crate::input::failure;
use crate::tool::{MAX_TOOL_TIMEOUT, Tool, ToolContext, ToolOutput};

/// 单问等待上限（旧 `QUESTION_TIMEOUT_MS = 5 * 60 * 1000L`）。
pub const QUESTION_TIMEOUT: Duration = Duration::from_mins(5);

/// 问题数上下限（旧 `questions.isEmpty() || questions.size() > 4`）。
const MAX_QUESTIONS: usize = 4;

/// 每问选项数下限（旧 `options.size() < 2`）。
const MIN_OPTIONS: usize = 2;

/// 每问选项数上限（旧 `options.size() > 4`）。
const MAX_OPTIONS: usize = 4;

/// 多选问答工具（名 `AskUserQuestion`）。
#[derive(Clone, Default)]
pub struct AskUserQuestionTool {
    /// 交互出口；`None` = 未装配（走旧 `ERROR` 分支）。
    sink: Option<Arc<dyn ElicitationSink>>,
}

impl std::fmt::Debug for AskUserQuestionTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AskUserQuestionTool")
            .field("elicitation_sink", &self.sink.is_some())
            .finish()
    }
}

impl AskUserQuestionTool {
    /// 装配（无交互出口）。
    #[must_use]
    pub fn new() -> Self {
        Self { sink: None }
    }

    /// 装配并注入交互出口（组合根提供持久交互实现）。
    #[must_use]
    pub fn with_elicitation_sink(sink: Arc<dyn ElicitationSink>) -> Self {
        Self { sink: Some(sink) }
    }
}

impl Tool for AskUserQuestionTool {
    fn name(&self) -> &'static str {
        "AskUserQuestion"
    }

    fn description(&self) -> &'static str {
        "Ask the user a multiple-choice question. \
         Supports 1-4 questions, each with 2-4 options. \
         The tool blocks until the user responds or times out."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "questions": {
                    "type": "array",
                    "description": "List of questions to ask (1-4)",
                    "items": {
                        "type": "object",
                        "properties": {
                            "question": { "type": "string" },
                            "options": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "label": { "type": "string" },
                                        "description": { "type": "string" }
                                    }
                                }
                            },
                            "multiSelect": { "type": "boolean" }
                        }
                    }
                }
            },
            "required": ["questions"]
        })
    }

    /// 只读调用（旧 `isReadOnly(ToolInput) → true`）。
    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    /// 最坏情形 4 问 × 5 分钟 = 20 分钟，超出执行器的
    /// [`MAX_TOOL_TIMEOUT`]（10 分钟）钳制上限，故直接返回上限值：
    /// 单问看门狗由本工具内部保证，执行器层只需不早于它触发。
    fn timeout(&self) -> Duration {
        MAX_TOOL_TIMEOUT
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.run(input, ctx).await })
    }
}

impl AskUserQuestionTool {
    /// 执行主体（数量校验 → 逐题发问 → 组装 `{questions, answers}`）。
    async fn run(&self, input: Value, ctx: ToolContext) -> ToolOutput {
        let questions = match validate(&input) {
            Ok(questions) => questions,
            Err(output) => return output,
        };
        let session = session_key(ctx.session_id()).to_owned();
        let mut answers = serde_json::Map::new();
        for (index, question) in questions.iter().enumerate() {
            let text = question
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let options = to_options(question);
            tracing::info!(
                question = %text,
                position = index + 1,
                total = questions.len(),
                "AskUserQuestion: sending question"
            );
            let request = ElicitationRequest {
                session_id: session.clone(),
                run_id: ctx.run_id().map(str::to_owned),
                question: text,
                options,
            };
            match self.ask(request, &ctx).await {
                ElicitationOutcome::Success(value) => {
                    answers.insert(format!("q{}", index + 1), value.unwrap_or(Value::Null));
                }
                ElicitationOutcome::Cancelled => {
                    return failure("ELICITATION_CANCELLED", "User cancelled the question.");
                }
                ElicitationOutcome::Timeout => {
                    return failure(
                        "ELICITATION_EXPIRED",
                        "User did not respond within 5 minutes.",
                    );
                }
                ElicitationOutcome::Error(error) => {
                    return failure("ELICITATION_FAILED", format!("Error: {error}"));
                }
            }
        }

        let mut result = serde_json::Map::new();
        result.insert("questions".to_owned(), Value::Array(questions));
        result.insert("answers".to_owned(), Value::Object(answers.clone()));
        match serde_json::to_string(&Value::Object(result)) {
            Ok(text) => {
                let mut output = ToolOutput::ok(text);
                output.metadata = Some(json!({
                    "structuredResult": { "answers": Value::Object(answers) }
                }));
                output
            }
            Err(error) => failure(
                "ELICITATION_RESULT_SERIALIZATION_FAILED",
                format!("Error serializing result: {error}"),
            ),
        }
    }

    /// 单问的三路竞态：交互终态 / 5 分钟看门狗 / 取消令牌。
    async fn ask(&self, request: ElicitationRequest, ctx: &ToolContext) -> ElicitationOutcome {
        let Some(sink) = self.sink.as_ref() else {
            return ElicitationOutcome::Error("elicitation sink is not configured".to_owned());
        };
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => ElicitationOutcome::Cancelled,
            outcome = sink.request_and_wait(request) => outcome,
            () = tokio::time::sleep(QUESTION_TIMEOUT) => ElicitationOutcome::Timeout,
        }
    }
}

/// 数量校验（旧顺序：先问题数、再逐问选项数）。
fn validate(input: &Value) -> Result<Vec<Value>, ToolOutput> {
    let questions = input.get("questions").and_then(Value::as_array);
    let Some(questions) = questions.filter(|list| !list.is_empty() && list.len() <= MAX_QUESTIONS)
    else {
        return Err(failure(
            "ELICITATION_QUESTION_COUNT_INVALID",
            "Must provide 1-4 questions.",
        ));
    };
    for question in questions {
        let count = question
            .get("options")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        if !(MIN_OPTIONS..=MAX_OPTIONS).contains(&count) {
            return Err(failure(
                "ELICITATION_OPTION_COUNT_INVALID",
                format!("Each question must have 2-4 options. Got: {count}"),
            ));
        }
    }
    Ok(questions.clone())
}

/// 选项转换（旧 `new ElicitationOption(label, label, desc)`，缺省空串）。
fn to_options(question: &Value) -> Vec<ElicitationOption> {
    question
        .get("options")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .map(|option| {
                    let label = option
                        .get("label")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    ElicitationOption {
                        value: label.clone(),
                        label,
                        description: option
                            .get("description")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// 按调用序回放结局的桩出口（并记录收到的发问）。
    #[derive(Default)]
    struct ScriptedSink {
        script: Mutex<std::collections::VecDeque<ElicitationOutcome>>,
        seen: Mutex<Vec<ElicitationRequest>>,
    }

    impl ScriptedSink {
        fn new(outcomes: Vec<ElicitationOutcome>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(outcomes.into()),
                seen: Mutex::new(Vec::new()),
            })
        }
    }

    impl ElicitationSink for ScriptedSink {
        fn request_and_wait(
            &self,
            request: ElicitationRequest,
        ) -> BoxFuture<'_, ElicitationOutcome> {
            self.seen.lock().expect("seen").push(request);
            let next = self
                .script
                .lock()
                .expect("script")
                .pop_front()
                .unwrap_or(ElicitationOutcome::Timeout);
            Box::pin(async move { next })
        }
    }

    /// 永不返回的桩出口——用于验证看门狗与取消路径。
    struct PendingSink;

    impl ElicitationSink for PendingSink {
        fn request_and_wait(
            &self,
            _request: ElicitationRequest,
        ) -> BoxFuture<'_, ElicitationOutcome> {
            Box::pin(std::future::pending())
        }
    }

    fn ctx(cancel: CancellationToken) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(cancel, tx)
            .with_session_id("ask-session")
            .with_run_id("ask-run")
    }

    fn question(text: &str, labels: &[&str]) -> Value {
        json!({
            "question": text,
            "options": labels
                .iter()
                .map(|label| json!({ "label": label, "description": "" }))
                .collect::<Vec<_>>(),
        })
    }

    #[tokio::test]
    async fn collects_answers_keyed_by_question_position() {
        let sink = ScriptedSink::new(vec![
            ElicitationOutcome::Success(Some(json!("yes"))),
            ElicitationOutcome::Success(Some(json!("no"))),
        ]);
        let tool = AskUserQuestionTool::with_elicitation_sink(sink.clone());
        let output = tool
            .execute(
                json!({ "questions": [
                    question("first?", &["yes", "no"]),
                    question("second?", &["yes", "no"]),
                ] }),
                ctx(CancellationToken::new()),
            )
            .await;
        assert!(!output.is_error);
        let result: Value = serde_json::from_str(&output.content).expect("json");
        assert_eq!(result["answers"]["q1"], json!("yes"));
        assert_eq!(result["answers"]["q2"], json!("no"));
        assert_eq!(result["questions"].as_array().expect("questions").len(), 2);

        let seen = sink.seen.lock().expect("seen");
        assert_eq!(seen.len(), 2, "逐题串行发问");
        assert_eq!(seen[0].session_id, "ask-session");
        assert_eq!(seen[0].run_id.as_deref(), Some("ask-run"));
        assert_eq!(
            seen[0].options[0],
            ElicitationOption {
                label: "yes".to_owned(),
                value: "yes".to_owned(),
                description: String::new(),
            },
            "value 与 label 同值（旧 new ElicitationOption(label, label, desc)）"
        );
    }

    #[tokio::test]
    async fn maps_every_terminal_outcome_to_the_legacy_code() {
        for (outcome, expected) in [
            (
                ElicitationOutcome::Cancelled,
                "ELICITATION_CANCELLED: User cancelled the question.",
            ),
            (
                ElicitationOutcome::Timeout,
                "ELICITATION_EXPIRED: User did not respond within 5 minutes.",
            ),
            (
                ElicitationOutcome::Error("boom".to_owned()),
                "ELICITATION_FAILED: Error: boom",
            ),
        ] {
            let tool = AskUserQuestionTool::with_elicitation_sink(ScriptedSink::new(vec![outcome]));
            let output = tool
                .execute(
                    json!({ "questions": [question("q?", &["a", "b"])] }),
                    ctx(CancellationToken::new()),
                )
                .await;
            assert!(output.is_error);
            assert_eq!(output.content, expected);
        }
    }

    #[tokio::test]
    async fn rejects_out_of_range_question_and_option_counts() {
        let tool = AskUserQuestionTool::with_elicitation_sink(ScriptedSink::new(Vec::new()));
        let cancel = CancellationToken::new();

        let empty = tool
            .execute(json!({ "questions": [] }), ctx(cancel.clone()))
            .await;
        assert_eq!(
            empty.content,
            "ELICITATION_QUESTION_COUNT_INVALID: Must provide 1-4 questions."
        );

        let missing = tool.execute(json!({}), ctx(cancel.clone())).await;
        assert!(
            missing
                .content
                .starts_with("ELICITATION_QUESTION_COUNT_INVALID: ")
        );

        let too_many = tool
            .execute(
                json!({ "questions": (0..5).map(|_| question("q?", &["a", "b"])).collect::<Vec<_>>() }),
                ctx(cancel.clone()),
            )
            .await;
        assert!(
            too_many
                .content
                .starts_with("ELICITATION_QUESTION_COUNT_INVALID: ")
        );

        let one_option = tool
            .execute(
                json!({ "questions": [question("q?", &["only"])] }),
                ctx(cancel.clone()),
            )
            .await;
        assert_eq!(
            one_option.content,
            "ELICITATION_OPTION_COUNT_INVALID: Each question must have 2-4 options. Got: 1"
        );

        let no_options = tool
            .execute(json!({ "questions": [{ "question": "q?" }] }), ctx(cancel))
            .await;
        assert_eq!(
            no_options.content,
            "ELICITATION_OPTION_COUNT_INVALID: Each question must have 2-4 options. Got: 0"
        );
    }

    #[tokio::test]
    async fn cancel_token_wins_over_a_pending_elicitation() {
        let tool = AskUserQuestionTool::with_elicitation_sink(Arc::new(PendingSink));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let output = tool
            .execute(
                json!({ "questions": [question("q?", &["a", "b"])] }),
                ctx(cancel),
            )
            .await;
        assert_eq!(
            output.content,
            "ELICITATION_CANCELLED: User cancelled the question."
        );
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_expires_a_pending_elicitation_after_five_minutes() {
        let tool = AskUserQuestionTool::with_elicitation_sink(Arc::new(PendingSink));
        let output = tool
            .execute(
                json!({ "questions": [question("q?", &["a", "b"])] }),
                ctx(CancellationToken::new()),
            )
            .await;
        assert_eq!(
            output.content,
            "ELICITATION_EXPIRED: User did not respond within 5 minutes."
        );
    }

    #[tokio::test]
    async fn unconfigured_sink_reports_the_legacy_error_code() {
        let output = AskUserQuestionTool::new()
            .execute(
                json!({ "questions": [question("q?", &["a", "b"])] }),
                ctx(CancellationToken::new()),
            )
            .await;
        assert_eq!(
            output.content,
            "ELICITATION_FAILED: Error: elicitation sink is not configured"
        );
    }

    #[test]
    fn spec_and_flags_match_the_legacy_contract() {
        let tool = AskUserQuestionTool::new();
        assert_eq!(tool.name(), "AskUserQuestion");
        assert!(tool.is_read_only(&json!({})));
        assert_eq!(tool.timeout(), MAX_TOOL_TIMEOUT);
        assert_eq!(tool.spec().parameters["required"], json!(["questions"]));
    }
}
