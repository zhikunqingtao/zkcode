//! External tools reuse the same immutable tool pipeline without making a model call.
use super::{
    Arc, BTreeSet, CallEnv, CancellationToken, CleanupStatus, DurableTaskStatus, Engine,
    FlushedCall, MessageRole, NewMessage, StoredBlock, ToolAdmission, ToolCallTracker,
    ToolResultContent, json, run_message_attribution,
};

/// A host-authorized operation bound to one live MCP service, never a client cwd.
pub struct ExternalToolCall {
    /// Authorized dedicated service session.
    pub session_id: String,
    /// Exact service Run; never follows a later Run in the session.
    pub run_id: String,
    /// Explicit stable operation identity, separate from reusable JSON-RPC ids.
    pub operation_id: String,
    /// Tool from the current bound registry.
    pub name: String,
    /// Original tool input before PRE Hook transformations.
    pub input: serde_json::Value,
    /// Call cancellation linked by the host to both transport and service lifetime.
    pub cancel: CancellationToken,
    /// Current allowed catalog. Admission must also enforce actual operation capabilities.
    pub allowed_tools: BTreeSet<String>,
    /// Host-approved ceiling for Hook command and network effects.
    pub hook_policy: crate::hook::ExternalHookPolicy,
}

fn external_store_error(error: &zk_db::DbError) -> String {
    let detail = match error {
        zk_db::DbError::Invalid(detail)
        | zk_db::DbError::Validation(detail)
        | zk_db::DbError::Conflict(detail) => detail.as_str(),
        _ => "EXTERNAL_TOOL_STORE_UNAVAILABLE",
    };
    match detail {
        "EXTERNAL_OPERATION_ID_INVALID"
        | "EXTERNAL_OPERATION_OWNER_MISMATCH"
        | "EXTERNAL_OPERATION_CONFLICT"
        | "EXTERNAL_OPERATION_RUN_CLOSED"
        | "EXTERNAL_OPERATION_FACTS_MISSING"
        | "EXTERNAL_OPERATION_UNCONFIRMED"
        | "EXTERNAL_OPERATION_RESULT_MISSING"
        | "EXTERNAL_OPERATION_RESULT_MISMATCH"
        | "EXTERNAL_OPERATION_PROJECTION_PENDING"
        | "EXTERNAL_OPERATION_RESULT_CONFLICT" => detail.into(),
        _ => "EXTERNAL_TOOL_STORE_UNAVAILABLE".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessageSink;
    use futures::{future::BoxFuture, stream::BoxStream};
    use zk_db::{CasOutcome, Db};
    use zk_llm::{ChatProvider, ChatRequest, ProviderError, ProviderEvent};
    use zk_protocol::ServerMessage;
    use zk_tools::{RunToolScope, RunToolScopeFactory, ToolContext, ToolRegistry, WriteFileTool};

    struct NoModel;
    impl ChatProvider for NoModel {
        fn provider_name(&self) -> &'static str {
            "external-fixture"
        }
        fn chat_stream(
            &self,
            _: ChatRequest,
            _: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            panic!("external tools cannot dispatch model requests")
        }
    }
    struct Sink;
    impl MessageSink for Sink {
        fn push<'a>(&'a self, _: &'a str, _: ServerMessage) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }
    #[derive(Debug)]
    struct LocalScopeFactory;
    struct LocalScope(Arc<ToolRegistry>);
    impl RunToolScopeFactory for LocalScopeFactory {
        fn prepare(
            &self,
            _: ToolContext,
            base: Arc<ToolRegistry>,
        ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
            Box::pin(async move { Ok(Arc::new(LocalScope(base)) as Arc<dyn RunToolScope>) })
        }
    }
    impl RunToolScope for LocalScope {
        fn registry(&self) -> Arc<ToolRegistry> {
            self.0.clone()
        }
        fn cleanup(&self) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
    }
    struct Fixture {
        engine: Arc<Engine>,
        session: String,
        run: String,
        directory: std::path::PathBuf,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }
    async fn fixture() -> Fixture {
        let directory =
            std::env::temp_dir().join(format!("zk-external-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let directory = directory.canonicalize().unwrap();
        let db = Db::open_in_memory().unwrap();
        let session = db
            .create_session("fixture", directory.to_str().unwrap())
            .await
            .unwrap()
            .id;
        let created=db.create_task_with_run(&zk_db::CreateTaskWithRun {
            task_id:uuid::Uuid::new_v4().to_string(),run_id:uuid::Uuid::new_v4().to_string(),
            root_session_id:session.clone(),transcript_session_id:session.clone(),
            parent_task_id:None,parent_run_id:None,creator_tool_use_id:None,ordinal:0,
            description:"external fixture".into(),prompt:None,task_type:"mcp".into(),
            model:"fixture".into(),working_dir:directory.to_string_lossy().into_owned(),
            execution_config_json:json!({"executor":"localMcp","budget":{"tokenLimit":1000,"costLimitNanosUsd":1_000_000,"deadlineAtMs":zk_db::time::now_millis()+60_000}}).to_string(),startup_epoch:0,
        }).await.unwrap();
        assert_eq!(
            db.claim_task_run_cas(&created.task.id, &created.run_id, created.task.version)
                .await
                .unwrap(),
            CasOutcome::Applied
        );
        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(WriteFileTool::new()));
        let registry = Arc::new(registry);
        let scopes = Arc::new(crate::run_tool_scopes::RunToolScopes::new(vec![Arc::new(
            LocalScopeFactory,
        )]));
        let engine = Arc::new(
            Engine::with_tools(db, Arc::new(NoModel), Arc::new(Sink), registry.clone())
                .with_run_tool_scopes(scopes.clone()),
        );
        scopes
            .prepare(
                &engine.db,
                &engine.executor,
                &created.run_id,
                CallEnv::new()
                    .with_session_id(&session)
                    .with_run_id(&created.run_id)
                    .with_working_dir(&directory),
                CancellationToken::new(),
                registry,
                scopes.factories(None, None),
            )
            .await
            .unwrap();
        Fixture {
            engine,
            session,
            run: created.run_id,
            directory,
        }
    }
    fn call(fixture: &Fixture, operation: &str, content: &str) -> ExternalToolCall {
        ExternalToolCall {
            session_id: fixture.session.clone(),
            run_id: fixture.run.clone(),
            operation_id: operation.into(),
            name: "Write".into(),
            input: json!({"file_path":fixture.directory.join("result.txt"),"content":content}),
            cancel: CancellationToken::new(),
            allowed_tools: BTreeSet::from(["Write".into()]),
            hook_policy: crate::hook::ExternalHookPolicy::default(),
        }
    }

    #[tokio::test]
    async fn external_write_uses_canonical_artifact_pipeline_and_replays_without_rewriting() {
        let f = fixture().await;
        let (first, second) = tokio::join!(
            f.engine
                .execute_external_bound_tool(call(&f, "write-1", "initial"), crate::allow_all()),
            f.engine
                .execute_external_bound_tool(call(&f, "write-1", "initial"), crate::allow_all())
        );
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_ne!(first.replayed, second.replayed);
        assert!(!first.result.is_error);
        assert_eq!(first.status, "succeeded");
        assert_eq!(first.result_message_id, second.result_message_id);
        let manifest = f
            .engine
            .db
            .find_artifact_manifest_by_run(&f.run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(
            manifest.entries[0].producer_invocation_id.as_deref(),
            Some(first.invocation_id.as_str())
        );
        std::fs::write(f.directory.join("result.txt"), "later user edit").unwrap();
        let replay = f
            .engine
            .execute_external_bound_tool(call(&f, "write-1", "initial"), crate::allow_all())
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(
            std::fs::read_to_string(f.directory.join("result.txt")).unwrap(),
            "later user edit"
        );
        assert!(
            f.engine
                .execute_external_bound_tool(
                    call(&f, "write-1", "changed input"),
                    crate::allow_all()
                )
                .await
                .is_err()
        );
        let mut foreign = call(&f, "write-2", "foreign");
        foreign.session_id = "foreign".into();
        assert!(
            f.engine
                .execute_external_bound_tool(foreign, crate::allow_all())
                .await
                .is_err()
        );
        let count = f
            .engine
            .db
            .with_reader(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM llm_calls", [], |row| {
                    row.get::<_, i64>(0)
                })?)
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn failed_required_projection_does_not_repeat_an_already_applied_write() {
        let f = fixture().await;
        f.engine.db.with_writer(|conn| {conn.execute_batch("CREATE TRIGGER fail_external_artifact BEFORE INSERT ON artifact_entries BEGIN SELECT RAISE(ABORT,'injected failure'); END;")?;Ok(())}).await.unwrap();
        assert!(
            f.engine
                .execute_external_bound_tool(
                    call(&f, "effect", "written before projection"),
                    crate::allow_all()
                )
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(f.directory.join("result.txt")).unwrap(),
            "written before projection"
        );
        std::fs::write(f.directory.join("result.txt"), "retained user edit").unwrap();
        assert!(
            f.engine
                .execute_external_bound_tool(
                    call(&f, "effect", "written before projection"),
                    crate::allow_all()
                )
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(f.directory.join("result.txt")).unwrap(),
            "retained user edit"
        );
        let input = call(&f, "effect", "written before projection").input;
        assert!(matches!(
            f.engine
                .db
                .admit_external_tool_operation(&f.session, &f.run, "effect", "Write", &input)
                .await
                .unwrap(),
            zk_db::ExternalToolAdmission::Unconfirmed
        ));
    }
}

/// Canonical output after all required immutable facts and derived projections exist.
#[derive(Clone, Debug)]
pub struct ExternalToolResult {
    /// Stable host operation identity for explicit retry.
    pub operation_id: String,
    /// Durable tool-use identity, reused only for the same operation.
    pub tool_use_id: String,
    /// Physical invocation identity.
    pub invocation_id: String,
    /// Original persisted result message.
    pub result_message_id: String,
    /// Real lifecycle status, independent of tool result's error flag.
    pub status: String,
    /// Actual physical cleanup, never inferred from a successful RPC response.
    pub cleanup_status: CleanupStatus,
    /// Immutable tool output; POST presentation cannot replace it.
    pub result: ToolResultContent,
    /// True means the original result was returned without repeating side effects.
    pub replayed: bool,
}

impl Engine {
    /// Execute a host-bound MCP call through the existing tool transaction pipeline.
    /// The caller must retain a supervisor-owned finalizer until this future ends.
    /// It supplies the normal authorization service plus a revocable capability ceiling.
    ///
    /// # Errors
    /// Ownership, current capability, storage and unconfirmed effects fail explicitly.
    #[allow(clippy::too_many_lines)] // one ordered ownership → admission → immutable result boundary
    pub async fn execute_external_bound_tool(
        &self,
        call: ExternalToolCall,
        admission: Arc<dyn ToolAdmission>,
    ) -> Result<ExternalToolResult, String> {
        if !call.allowed_tools.contains(&call.name) {
            return Err("EXTERNAL_TOOL_CAPABILITY_DENIED".into());
        }
        let lock = {
            let mut locks = self
                .external_tool_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks
                .get(&call.session_id)
                .and_then(std::sync::Weak::upgrade)
            {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(call.session_id.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let _serial = tokio::select! {
            biased;
            ()=call.cancel.cancelled()=>return Err("EXTERNAL_TOOL_CANCELLED".into()),
            guard=lock.lock()=>guard,
        };
        let run = self
            .db
            .find_run_by_id(&call.run_id)
            .await
            .map_err(|_| "EXTERNAL_RUN_UNAVAILABLE")?
            .ok_or("EXTERNAL_RUN_NOT_FOUND")?;
        let task = self
            .db
            .find_runtime_task_by_id(&run.task_id)
            .await
            .map_err(|_| "EXTERNAL_TASK_UNAVAILABLE")?
            .ok_or("EXTERNAL_TASK_NOT_FOUND")?;
        let session = self
            .db
            .get_session(&call.session_id)
            .await
            .map_err(|_| "EXTERNAL_SESSION_UNAVAILABLE")?
            .ok_or("EXTERNAL_SESSION_NOT_FOUND")?;
        if run.session_id != call.session_id
            || task.session_id != call.session_id
            || task.task_type != "mcp"
            || task.current_run_id.as_deref() != Some(call.run_id.as_str())
            || task.status != DurableTaskStatus::Running
            || run.status != "running"
            || run.requested_exit_reason.is_some()
            || session.status != "active"
        {
            return Err("EXTERNAL_TOOL_OWNER_INACTIVE".into());
        }
        if task
            .deadline_at_ms
            .is_none_or(|deadline| deadline <= zk_db::time::now_millis())
        {
            return Err("EXTERNAL_TOOL_DEADLINE_EXCEEDED".into());
        }
        if self.run_tool_scopes.directory(&call.run_id).is_none() {
            return Err("EXTERNAL_TOOL_RUN_SCOPE_REQUIRED".into());
        }
        let ephemeral = self
            .db
            .session_retention(&call.session_id)
            .await
            .map_err(|_| "EXTERNAL_CONTENT_POLICY_UNAVAILABLE")?
            == zk_db::content::ContentRetention::Ephemeral;
        let reserved = self
            .reserve_conversation(&call.session_id)
            .ok_or("EXTERNAL_SESSION_BUSY")?;
        // The shared reservation excludes an ordinary chat tool transaction in this session.
        let _ = reserved.run.run_id.set(call.run_id.clone());
        reserved.run.run_ready.notify_waiters();
        let request_cancel = call.cancel.clone();
        let reservation_cancel = reserved.run.cancel.clone();
        let watch = tokio::spawn(async move {
            tokio::select! {()=reservation_cancel.cancelled()=>request_cancel.cancel(),()=request_cancel.cancelled()=>{}}
        });
        let _watch = Watch(watch);
        let decision = self
            .db
            .admit_external_tool_operation(
                &call.session_id,
                &call.run_id,
                &call.operation_id,
                &call.name,
                &call.input,
            )
            .await
            .map_err(|error| external_store_error(&error))?;
        let tool_use_id = match decision {
            zk_db::ExternalToolAdmission::Accepted { tool_use_id } => tool_use_id,
            zk_db::ExternalToolAdmission::Completed {
                tool_use_id,
                message_id,
            } => {
                return self
                    .external_tool_result(&call, &tool_use_id, &message_id, true)
                    .await;
            }
            zk_db::ExternalToolAdmission::Unconfirmed => {
                return Err("EXTERNAL_OPERATION_IN_FLIGHT_OR_UNCONFIRMED".into());
            }
        };
        let record = self
            .db
            .append_attributed_message(
                &call.session_id,
                NewMessage {
                    role: MessageRole::Assistant,
                    content: vec![StoredBlock::ToolUse {
                        id: tool_use_id.clone(),
                        name: call.name.clone(),
                        input: call.input.clone(),
                    }],
                    meta: Some(
                        json!({"runtimeProjection":"external_mcp","operationId":call.operation_id}),
                    ),
                    stop_reason: Some("tool_use".into()),
                    input_tokens: 0,
                    output_tokens: 0,
                },
                run_message_attribution(&task.id, &call.run_id, "runtime"),
            )
            .await
            .map_err(|_| "EXTERNAL_TOOL_INPUT_STORE_FAILED")?;
        if let Some(history) = &self.file_history {
            history.begin_transaction(
                &call.session_id,
                &record.id,
                usize::try_from(record.seq_num).unwrap_or(usize::MAX),
            );
        }
        let env = CallEnv::new()
            .with_ephemeral_content(ephemeral)
            .with_working_dir(&session.working_dir)
            .with_session_id(&call.session_id)
            .with_run_id(&call.run_id);
        let flushed = FlushedCall {
            id: tool_use_id.clone(),
            name: call.name.clone(),
            arguments: call.input.to_string(),
            input: call.input.clone(),
        };
        let mut records = Vec::new();
        let _outcome = self
            .run_bound_tools(
                &call.session_id,
                &task.id,
                &[flushed],
                &env,
                &call.cancel,
                &mut ToolCallTracker::new(),
                Some(&mut records),
                admission.as_ref(),
                Some(&call.allowed_tools),
                Some(call.hook_policy),
            )
            .await;
        self.commit_file_history(&call.session_id);
        let message=records.iter().find(|record|record.content.iter().any(|block|matches!(block,StoredBlock::ToolResult {tool_use_id:id,..} if id==&tool_use_id))).ok_or("EXTERNAL_TOOL_RESULT_UNCONFIRMED")?;
        self.external_tool_result(&call, &tool_use_id, &message.id, false)
            .await
    }

    async fn external_tool_result(
        &self,
        call: &ExternalToolCall,
        tool_use_id: &str,
        message_id: &str,
        replayed: bool,
    ) -> Result<ExternalToolResult, String> {
        let facts = self
            .db
            .complete_external_tool_operation(
                &call.session_id,
                &call.run_id,
                &call.operation_id,
                message_id,
            )
            .await
            .map_err(|error| external_store_error(&error))?;
        let message = self
            .db
            .get_message_by_id(message_id)
            .await
            .map_err(|_| "EXTERNAL_TOOL_RESULT_UNAVAILABLE")?
            .ok_or("EXTERNAL_TOOL_RESULT_MISSING")?;
        if message.session_id != call.session_id {
            return Err("EXTERNAL_TOOL_RESULT_OWNER_MISMATCH".into());
        }
        let result = message
            .content
            .into_iter()
            .find_map(|block| match block {
                StoredBlock::ToolResult {
                    tool_use_id: id,
                    content,
                    is_error,
                    metadata,
                } if id == tool_use_id => Some(ToolResultContent {
                    content,
                    is_error,
                    metadata,
                }),
                _ => None,
            })
            .ok_or("EXTERNAL_TOOL_RESULT_INVALID")?;
        Ok(ExternalToolResult {
            operation_id: call.operation_id.clone(),
            tool_use_id: tool_use_id.into(),
            invocation_id: facts.invocation_id,
            result_message_id: facts.message_id,
            status: facts.status,
            cleanup_status: facts.cleanup_status,
            result,
            replayed,
        })
    }
}

struct Watch(tokio::task::JoinHandle<()>);
impl Drop for Watch {
    fn drop(&mut self) {
        self.0.abort();
    }
}
