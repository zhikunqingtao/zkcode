//! Shared factual context projection for the tool and the slash command.
use futures::future::BoxFuture;
use zk_tools::{ContextInfo, ContextInfoPort};

pub(crate) struct DbContextInfo(pub(crate) zk_db::Db);
impl ContextInfoPort for DbContextInfo {
    fn get_context_info<'a>(
        &'a self,
        session_id: &'a str,
        run_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ContextInfo, String>> {
        Box::pin(async move {
            let detail = self
                .0
                .get_session(session_id)
                .await
                .map_err(|error| code(&error))?
                .ok_or("CONTEXT_SESSION_NOT_FOUND")?;
            let mut depth = 0u32;
            if let Some(run_id) = run_id {
                let mut run = self
                    .0
                    .find_run_by_id(run_id)
                    .await
                    .map_err(|error| code(&error))?
                    .ok_or("CONTEXT_RUN_NOT_FOUND")?;
                if run.session_id != session_id {
                    return Err("CONTEXT_RUN_SESSION_MISMATCH".into());
                }
                let mut visited = std::collections::HashSet::from([run.id.clone()]);
                while let Some(parent) = run.parent_run_id.as_deref() {
                    if depth >= 128 || !visited.insert(parent.to_owned()) {
                        return Err("CONTEXT_RUN_ANCESTRY_INVALID".into());
                    }
                    run = self
                        .0
                        .find_run_by_id(parent)
                        .await
                        .map_err(|error| code(&error))?
                        .ok_or("CONTEXT_PARENT_RUN_NOT_FOUND")?;
                    depth += 1;
                }
            }
            Ok(ContextInfo {
                session_id: detail.session_id,
                message_count: u32::try_from(detail.messages.len())
                    .map_err(|_| "CONTEXT_MESSAGE_COUNT_OVERFLOW")?,
                total_input_tokens: u64::try_from(detail.total_usage.input_tokens)
                    .map_err(|_| "CONTEXT_USAGE_INVALID")?,
                total_output_tokens: u64::try_from(detail.total_usage.output_tokens)
                    .map_err(|_| "CONTEXT_USAGE_INVALID")?,
                nesting_depth: depth,
                working_directory: detail.working_dir,
            })
        })
    }
}
fn code(error: &zk_db::DbError) -> String {
    error.diagnostic_code().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zk_tools::Tool;

    #[tokio::test]
    async fn real_context_counts_and_missing_or_foreign_owner_are_not_fabricated() {
        let state = crate::state::AppState::for_tests();
        let session = state.db.create_session("test-model", "/tmp").await.unwrap();
        state
            .db
            .add_session_usage(
                &session.id,
                &zk_protocol::Usage {
                    input_tokens: 37,
                    output_tokens: 11,
                    ..Default::default()
                },
                0.0,
            )
            .await
            .unwrap();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let tool = zk_tools::CtxInspectTool::new(Some(std::sync::Arc::new(DbContextInfo(
            state.db.clone(),
        ))));
        let result = tool
            .execute(
                serde_json::json!({}),
                zk_tools::ToolContext::new(tokio_util::sync::CancellationToken::new(), tx)
                    .with_session_id(&session.id),
            )
            .await;
        assert!(!result.is_error, "{}", result.content);
        assert!(
            result.content.contains("累计输入 Token: 37")
                && result.content.contains("累计输出 Token: 11")
        );
        assert!(result.content.contains("不是当前模型上下文窗口占用"));
        let port = DbContextInfo(state.db.clone());
        assert_eq!(
            port.get_context_info("missing", None).await.unwrap_err(),
            "CONTEXT_SESSION_NOT_FOUND"
        );
        let other = state.db.create_session("test-model", "/tmp").await.unwrap();
        state
            .db
            .start_run("foreign-run", &other.id, None, None, "test-model")
            .await
            .unwrap();
        assert_eq!(
            port.get_context_info(&session.id, Some("foreign-run"))
                .await
                .unwrap_err(),
            "CONTEXT_RUN_SESSION_MISMATCH"
        );
    }
}
