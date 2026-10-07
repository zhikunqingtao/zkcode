//! Bind optional suggestions to the original request's visible native capability.
use super::{
    CallEnv, CancellationToken, ChatMessage, ChatRequest, Engine, FlushedCall, MessageRecord,
    MessageRole, NewMessage, StoredBlock, ToolCallTracker, json, run_message_attribution,
    to_tool_call_requests,
};
use crate::auto_visualization::RunVisualizationState;
use crate::auxiliary_query::AuxiliaryExecution;

impl Engine {
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "One durable suggestion and the ordinary exact-owner tool pipeline form a single boundary"
    )]
    pub(super) async fn route_visualization_intent(
        &self,
        session_id: &str,
        request: &mut ChatRequest,
        env: &CallEnv,
        cancel: &CancellationToken,
        state: &mut RunVisualizationState,
        mut projected: Option<&mut Vec<MessageRecord>>,
    ) -> Result<(), String> {
        if !self.visualization_router.enabled()
            || state.presented
            || cancel.is_cancelled()
            || !request
                .tools
                .iter()
                .any(|tool| tool.name == "Visualization")
        {
            return Ok(());
        }
        let Some(mut attribution) = request.execution.clone() else {
            return Ok(());
        };
        if !self
            .tools_for_run(&attribution.run_id)
            .get("Visualization")
            .is_some_and(|tool| tool.produces_visualizations())
        {
            return Ok(());
        }
        if !state.replay_checked {
            let task_id = attribution.task_id.clone();
            state.presented = self.db.with_reader(move |conn| {
                // Across attempts of the same logical Task, a committed runtime ToolUse is the durable once-only fence,
                // including a crash before the invocation or its result commits.
                let mut statement = conn.prepare("SELECT session_id, metadata_json FROM messages WHERE task_id=?1 AND origin='runtime' AND role='assistant' AND metadata_json IS NOT NULL")?;
                let mut rows = statement.query([&task_id])?;
                while let Some(row) = rows.next()? {
                    let session:String=row.get(0)?;
                    let raw = zk_db::content::load_row_text(conn,&session,row.get(1)?)?;
                    if serde_json::from_str::<serde_json::Value>(&raw).ok().is_some_and(|meta| meta["autoVisualization"] == true) { return Ok(true); }
                }
                Ok(false)
            }).await.map_err(|_| "VISUALIZATION_REPLAY_CHECK_FAILED".to_owned())?;
            state.replay_checked = true;
            if state.presented {
                return Ok(());
            }
        }
        let budget = self
            .db
            .read_task_budget(&attribution.task_id)
            .await
            .map_err(|_| "VISUALIZATION_BUDGET_READ_FAILED".to_owned())?
            .ok_or("VISUALIZATION_TASK_NOT_FOUND")?;
        attribution.kind = "visualization_intent".into();
        let input = self
            .visualization_router
            .classify(
                &request.messages,
                state,
                AuxiliaryExecution {
                    db: &self.db,
                    attribution: attribution.clone(),
                    limits: zk_db::TaskBudgetLimits {
                        token_limit: budget.token_limit,
                        cost_limit_nanos_usd: budget.cost_limit_nanos_usd,
                        deadline_at_ms: budget.deadline_at_ms,
                    },
                    cancel,
                },
            )
            .await;
        super::ensure_post_turn_budget_integrity(
            &self.db,
            &attribution.task_id,
            &attribution.run_id,
        )
        .await
        .map_err(|failure| match failure {
            super::LlmAdmissionError::Runtime(code) => code.code().to_owned(),
            super::LlmAdmissionError::Internal(_) => "VISUALIZATION_USAGE_CHECK_FAILED".into(),
        })?;
        let Some(input) = input else {
            return Ok(());
        };
        // Never restore a revoked/removed capability after a slow classification.
        if cancel.is_cancelled()
            || !self
                .tools_for_run(&attribution.run_id)
                .get("Visualization")
                .is_some_and(|tool| tool.produces_visualizations())
        {
            return Ok(());
        }
        let call = FlushedCall {
            id: format!("auto_visualization_{}", uuid::Uuid::new_v4()),
            name: "Visualization".into(),
            arguments: input.to_string(),
            input,
        };
        let meta = Some(json!({"autoVisualization":true}));
        let text = "[Runtime visualization suggestion: inferred presentation only; no analysis has run and no new permission is granted.]";
        let record = self
            .db
            .append_attributed_message(
                session_id,
                NewMessage {
                    meta: meta.clone(),
                    role: MessageRole::Assistant,
                    content: vec![
                        StoredBlock::Text { text: text.into() },
                        StoredBlock::ToolUse {
                            id: call.id.clone(),
                            name: call.name.clone(),
                            input: call.input.clone(),
                        },
                    ],
                    stop_reason: Some("tool_use".into()),
                    input_tokens: 0,
                    output_tokens: 0,
                },
                run_message_attribution(&attribution.task_id, &attribution.run_id, "runtime"),
            )
            .await
            .map_err(|_| "VISUALIZATION_TOOL_USE_STORE_FAILED".to_owned())?;
        state.presented = true;
        if let Some(projected) = projected.as_deref_mut() {
            projected.push(record);
        }
        request.messages.push(
            ChatMessage::assistant_tool_calls(
                text,
                to_tool_call_requests(std::slice::from_ref(&call)),
            )
            .with_metadata(meta),
        );
        let Some(messages) = self
            .run_sub_agent_tools(
                session_id,
                &attribution.task_id,
                &[call],
                env,
                cancel,
                &mut ToolCallTracker::new(),
                projected,
            )
            .await
        else {
            return Err("VISUALIZATION_TOOL_RESULT_UNCONFIRMED".into());
        };
        request.messages.extend(messages);
        Ok(())
    }
}
