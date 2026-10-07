//! Non-model root execution uses the same durable driver as every child task.
use super::{
    ActiveExecution, Arc, CancellationToken, CreateTaskWithRun, DEFAULT_TASK_TIMEOUT, Duration,
    ExecutionClass, Future, Mutex, TaskBudgetLimits, TaskExecutionContext, TaskExecutionResult,
    TaskRuntime, TaskRuntimeError, TaskSubmissionReceipt, Uuid, drive_task, record_task_event,
};

/// A local MCP context. The caller first authorizes and binds its dedicated Session.
/// No client-supplied command, prompt, model, cwd or arbitrary execution policy is stored here.
#[derive(Clone, Debug)]
pub struct ExternalRootSubmission {
    pub session_id: String,
    pub startup_epoch: i64,
    pub timeout: Duration,
    pub budget: TaskBudgetLimits,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LocalExecution {
    Mcp,
    Repl,
    Hook,
}

impl TaskRuntime {
    /// Admit a non-LLM root to the ordinary queue, cancellation tree and terminal writer.
    /// The closure starts only after durable claim. Returning means physical work has
    /// stopped; the driver still verifies the resource ledger before reporting success.
    pub async fn submit_external_root<F, Fut>(
        &self,
        request: ExternalRootSubmission,
        build: F,
    ) -> Result<TaskSubmissionReceipt, TaskRuntimeError>
    where
        F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskExecutionResult> + Send + 'static,
    {
        self.submit_local_service(request, LocalExecution::Mcp, build)
            .await
    }

    /// Start a Session-owned REPL service that can outlive one conversation Run.
    /// Its internal transcript cannot pollute user/model history. The host must
    /// authorize every call and stop the service at its idle/overall deadline.
    /// Temporary sessions use attached Run scopes instead of this entry point.
    pub async fn submit_repl_service<F, Fut>(
        &self,
        request: ExternalRootSubmission,
        build: F,
    ) -> Result<TaskSubmissionReceipt, TaskRuntimeError>
    where
        F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskExecutionResult> + Send + 'static,
    {
        self.submit_local_service(request, LocalExecution::Repl, build)
            .await
    }

    /// A bounded host lifecycle operation. It has no model request or model-tool
    /// invocation and uses the ordinary global Task capacity and cleanup driver.
    /// The existing shell storage kind names the non-model host executor; the
    /// explicit `localHook` executor distinguishes it from a user Shell command.
    ///
    /// # Errors
    /// Rejects unavailable intake, invalid owner/deadline or failed durable admission.
    pub async fn submit_hook_operation<F, Fut>(
        &self,
        request: ExternalRootSubmission,
        build: F,
    ) -> Result<TaskSubmissionReceipt, TaskRuntimeError>
    where
        F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskExecutionResult> + Send + 'static,
    {
        self.submit_local_service(request, LocalExecution::Hook, build)
            .await
    }

    async fn submit_local_service<F, Fut>(
        &self,
        request: ExternalRootSubmission,
        kind: LocalExecution,
        build: F,
    ) -> Result<TaskSubmissionReceipt, TaskRuntimeError>
    where
        F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskExecutionResult> + Send + 'static,
    {
        let _intake = self.execution_intake().await?;
        let repl = kind == LocalExecution::Repl;
        let maximum = if repl {
            Duration::from_hours(1)
        } else {
            DEFAULT_TASK_TIMEOUT
        };
        if request.startup_epoch <= 0 || request.timeout.is_zero() || request.timeout > maximum {
            return Err(TaskRuntimeError::new(
                "EXTERNAL_ROOT_LIMITS_INVALID",
                "A startup epoch and bounded positive deadline are required",
                false,
            ));
        }
        let session = self
            .inner
            .db
            .get_session(&request.session_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new(
                    "SESSION_NOT_FOUND",
                    "Authorized service session no longer exists",
                    false,
                )
            })?;
        if repl
            && self
                .inner
                .db
                .session_retention(&request.session_id)
                .await
                .map_err(TaskRuntimeError::storage)?
                != zk_db::content::ContentRetention::Persistent
        {
            return Err(TaskRuntimeError::new(
                "EPHEMERAL_OPERATION_UNSUPPORTED",
                "Temporary REPL interpreters must remain attached to their Run",
                false,
            ));
        }
        let timeout_ms = i64::try_from(request.timeout.as_millis()).map_err(|_| {
            TaskRuntimeError::new(
                "EXTERNAL_ROOT_LIMITS_INVALID",
                "Deadline is not representable",
                false,
            )
        })?;
        let deadline = zk_db::time::now_millis().saturating_add(timeout_ms);
        let deadline = request
            .budget
            .deadline_at_ms
            .map_or(deadline, |existing| existing.min(deadline));
        let timeout = Duration::from_millis(
            u64::try_from(deadline.saturating_sub(zk_db::time::now_millis()).max(0)).unwrap_or(0),
        );
        let config = serde_json::json!({
            "executor":match kind { LocalExecution::Repl => "localRepl", LocalExecution::Mcp => "localMcp", LocalExecution::Hook => "localHook" }, "lifecycle":"attached", "isolation":if kind == LocalExecution::Mcp {"readOnly"} else {"sharedDirectory"},
            "taskTimeoutMs":timeout_ms,
            "budget":{"tokenLimit":request.budget.token_limit,"costLimitNanosUsd":request.budget.cost_limit_nanos_usd,"deadlineAtMs":deadline}
        });
        let durable = self
            .inner
            .db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: Uuid::new_v4().to_string(),
                run_id: Uuid::new_v4().to_string(),
                root_session_id: request.session_id.clone(),
                transcript_session_id: if repl {
                    Uuid::new_v4().to_string()
                } else {
                    request.session_id.clone()
                },
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: match kind {
                    LocalExecution::Repl => "Session REPL service",
                    LocalExecution::Mcp => "Local MCP connection",
                    LocalExecution::Hook => "Local session lifecycle hooks",
                }
                .into(),
                prompt: None,
                task_type: match kind {
                    LocalExecution::Repl => "repl",
                    LocalExecution::Mcp => "mcp",
                    LocalExecution::Hook => "shell",
                }
                .into(),
                model: session.model,
                working_dir: session.working_dir,
                execution_config_json: config.to_string(),
                startup_epoch: request.startup_epoch,
            })
            .await
            .map_err(TaskRuntimeError::submission_storage)?;
        let receipt = TaskSubmissionReceipt {
            task: durable.task.clone(),
            run_id: durable.run_id.clone(),
            transcript_session_id: durable.transcript_session_id.clone(),
            created: durable.created,
        };
        let cancel = CancellationToken::new();
        let active = Arc::new(ActiveExecution {
            task: durable.task.clone(),
            run_id: durable.run_id.clone(),
            cancel: cancel.clone(),
            driver: Mutex::new(None),
            hook_notifications: Arc::new(tokio::sync::Mutex::new(true)),
        });
        self.inner
            .active
            .insert(durable.task.id.clone(), Arc::clone(&active));
        record_task_event(
            &self.inner.observability,
            &request.session_id,
            &durable.task.id,
            "submit",
            "queued",
        );
        let inner = Arc::clone(&self.inner);
        let driver = tokio::spawn(async move {
            drive_task(
                inner,
                durable.task,
                durable.run_id,
                durable.transcript_session_id,
                request.session_id,
                timeout,
                cancel,
                match kind {
                    LocalExecution::Repl => ExecutionClass::ReplService,
                    LocalExecution::Mcp => ExecutionClass::McpService,
                    LocalExecution::Hook => ExecutionClass::Task,
                },
                build,
            )
            .await;
        });
        *active.driver.lock().expect("task driver lock poisoned") = Some(driver);
        Ok(receipt)
    }
}
