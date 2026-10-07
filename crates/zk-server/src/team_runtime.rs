//! Process-local queue pumps; durable Tasks own all worker execution and termination.
use crate::engine_bridge::AgentRuntime;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use zk_db::{Db, TeamDefinition, TeamWorkItem};
use zk_engine::agent::PersistedChildExecution;
use zk_engine::{
    AgentRequest, ChildExecutionContext, ChildTaskSubmission, IsolationMode, TaskExecutionResult,
    TaskRuntime,
};

struct TeamWorker {
    db: Db,
    executor: Arc<zk_engine::SubAgentExecutor>,
    stopped: Arc<Mutex<HashMap<String, bool>>>,
    team_id: String,
    policy_valid: bool,
    request: AgentRequest,
    context: ChildExecutionContext,
}
impl TeamWorker {
    async fn execute(mut self, execution: zk_engine::TaskExecutionContext) -> TaskExecutionResult {
        if !self.policy_valid {
            return TaskExecutionResult::Failed {
                message: "The requested team worktree policy is not enabled".into(),
                code: "TEAM_WORKTREE_POLICY_DISABLED".into(),
            };
        }
        if self
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&self.team_id)
        {
            return TaskExecutionResult::Cancelled {
                message: "Team stopped before worker execution".into(),
            };
        }
        match self.db.find_team(&self.team_id).await {
            Ok(Some(team)) if team.status == "open" => {}
            Ok(_) => {
                return TaskExecutionResult::Cancelled {
                    message: "Team intake closed before worker execution".into(),
                };
            }
            Err(error) => {
                return TaskExecutionResult::Failed {
                    message: error.to_string(),
                    code: "TEAM_POLICY_UNAVAILABLE".into(),
                };
            }
        }
        // The queue identity is only a placeholder until the runtime commits the
        // authoritative Task. Never execute with a client- or queue-chosen Task ID.
        self.request.agent_id.clone_from(&execution.task_id);
        let persisted = match PersistedChildExecution::try_new(
            execution.task_id,
            execution.run_id,
            execution.transcript_session_id,
        ) {
            Ok(value) => value,
            Err(error) => return TaskExecutionResult::failed(error),
        };
        let result = self
            .executor
            .execute_precreated_with_cancel(
                &self.request,
                &self.context,
                &persisted,
                execution.budget,
                execution.cancel,
            )
            .await;
        crate::engine_bridge::agent_result_to_task_result(result)
    }
}

pub(crate) struct TeamRuntime {
    db: Db,
    tasks: Arc<TaskRuntime>,
    executor: Arc<zk_engine::SubAgentExecutor>,
    startup_epoch: i64,
    worktree_writes_enabled: bool,
    events: Arc<zk_engine::CoordinatorEventBus>,
    // Scheduling wakeups only; never a task status or cancellation authority.
    pumps: Mutex<HashMap<String, u64>>,
    stopped: Arc<Mutex<HashMap<String, bool>>>,
}
impl TeamRuntime {
    pub(crate) fn new(
        db: Db,
        agent: &AgentRuntime,
        startup_epoch: i64,
        worktree_writes_enabled: bool,
        events: Arc<zk_engine::CoordinatorEventBus>,
    ) -> Self {
        Self {
            db,
            tasks: Arc::clone(&agent.tasks),
            executor: Arc::clone(&agent.executor),
            startup_epoch,
            worktree_writes_enabled,
            events,
            pumps: Mutex::new(HashMap::new()),
            stopped: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub(crate) fn stop_intake(&self, id: &str, shutdown: bool) {
        let mut stopped = self
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stopped
            .entry(id.to_owned())
            .and_modify(|existing| *existing &= shutdown)
            .or_insert(shutdown);
    }
    pub(crate) fn reset_created(&self, id: &str) {
        self.stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }
    pub(crate) fn kick(self: &Arc<Self>, team_id: &str) {
        let mut pumps = self
            .pumps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(generation) = pumps.get_mut(team_id) {
            *generation = generation.wrapping_add(1);
            return;
        }
        pumps.insert(team_id.to_owned(), 0);
        let this = Arc::clone(self);
        let team_id = team_id.to_owned();
        drop(tokio::spawn(async move {
            this.pump(team_id).await;
        }));
    }
    async fn pump(self: Arc<Self>, team_id: String) {
        let mut last_projection = None;
        loop {
            let generation = *self
                .pumps
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&team_id)
                .unwrap_or(&0);
            let step = self.step(&team_id).await;
            if let Ok(Some(team)) = self.db.find_team(&team_id).await {
                match self.projection(&team).await {
                    Ok(projection) if last_projection.as_ref() != Some(&projection) => {
                        self.publish_projection(&team, &projection);
                        last_projection = Some(projection);
                    }
                    Err(error) => tracing::warn!(
                        team_id,
                        error_type = std::any::type_name_of_val(&error),
                        "team projection unavailable"
                    ),
                    Ok(_) => {}
                }
            }
            match step {
                Ok(false) => {
                    let mut pumps = self
                        .pumps
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if pumps.get(&team_id) == Some(&generation) {
                        pumps.remove(&team_id);
                        return;
                    }
                }
                Ok(true) => tokio::time::sleep(Duration::from_millis(250)).await,
                Err(error) => {
                    tracing::error!(
                        team_id,
                        error_type = std::any::type_name_of_val(&error),
                        "team scheduler retained for reconciliation"
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    async fn step(&self, team_id: &str) -> Result<bool, String> {
        let Some(mut team) = self
            .db
            .find_team(team_id)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(false);
        };
        let local_stop = self
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(team_id)
            .copied();
        let mut stop_error = None;
        if let Some(shutdown) = local_stop {
            team.status = if shutdown { "shutdown" } else { "stopping" }.into();
            if let Err(error) = self.db.stop_team(team_id, shutdown).await {
                stop_error = Some(error.to_string());
            }
        }
        self.db
            .reconcile_team_queue(self.startup_epoch)
            .await
            .map_err(|e| e.to_string())?;
        // Retry only our current-process claims. The creator identity makes
        // submission idempotent across acknowledgement failures; older claims
        // were interrupted by reconciliation and are never automatically rerun.
        for item in self
            .db
            .team_work_items(team_id)
            .await
            .map_err(|e| e.to_string())?
        {
            if item.status == "claimed" {
                if team.status == "open" {
                    self.submit(&team, &item).await?;
                } else if let Some(claim) = item.claim_id {
                    self.db
                        .reject_team_work(&item.id, &claim, "TEAM_STOPPED_BEFORE_SUBMISSION")
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        if team.status == "open" {
            while let Some(item) = self
                .db
                .claim_team_work(team_id, self.startup_epoch)
                .await
                .map_err(|e| e.to_string())?
            {
                if self
                    .stopped
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains_key(team_id)
                {
                    self.db
                        .reject_team_work(
                            &item.id,
                            item.claim_id.as_deref().unwrap_or_default(),
                            "TEAM_STOPPED_BEFORE_SUBMISSION",
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    break;
                }
                self.submit(&team, &item).await?;
            }
        }
        let pending = self.pending_workers(&team).await?;
        if let Some(error) = stop_error {
            return Err(error);
        }
        Ok(pending)
    }
    async fn pending_workers(&self, team: &TeamDefinition) -> Result<bool, String> {
        let items = self
            .db
            .team_work_items(&team.id)
            .await
            .map_err(|e| e.to_string())?;
        let mut pending = false;
        for item in items {
            if matches!(item.status.as_str(), "queued" | "claimed") {
                pending = true;
            }
            if let Some(task_id) = item.task_id {
                let Some(task) = self
                    .tasks
                    .get_owned(&team.session_id, &task_id)
                    .await
                    .map_err(|e| e.to_string())?
                else {
                    continue;
                };
                if !task.status.is_terminal() {
                    pending = true;
                    if team.status == "stopping" {
                        // Persistence failures still locally stop owned executions; the
                        // retained pump retries, and never claims confirmed cleanup.
                        self.tasks
                            .cancel_owned(&team.session_id, &task_id, "Team stopped")
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
        }
        Ok(pending)
    }
    async fn submit(&self, team: &TeamDefinition, item: &TeamWorkItem) -> Result<(), String> {
        let claim = item.claim_id.clone().ok_or("TEAM_CLAIM_MISSING")?;
        let session = self
            .db
            .get_session(&team.session_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("TEAM_SESSION_MISSING")?;
        let prompt = item.payload["prompt"]
            .as_str()
            .ok_or("TEAM_PROMPT_CORRUPT")?
            .to_owned();
        let model = item.payload["model"]
            .as_str()
            .ok_or("TEAM_MODEL_CORRUPT")?
            .to_owned();
        let agent_type = item.payload["agentType"].as_str().map(str::to_owned);
        let isolation = team.config["workerIsolation"]
            .as_str()
            .unwrap_or("readOnly")
            .to_owned();
        let write_requested = isolation == "worktree";
        let write_allowed = write_requested && self.worktree_writes_enabled;

        let allowed_tools = team.config["workerToolAllowList"].as_array().map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<std::collections::BTreeSet<_>>()
        });
        let mut submission = ChildTaskSubmission::attached(
            &team.session_id,
            &item.parent_task_id,
            &item.parent_run_id,
            &item.id,
            "Team worker",
            &prompt,
            &model,
            &session.working_dir,
        );
        submission.task_type = "agent".into();
        submission.startup_epoch = self.startup_epoch;
        submission.execution_config_json=json!({"isolation":isolation,"allowWriteTools":write_allowed,"lifecycle":"attached","teamId":team.id,"allowedTools":allowed_tools}).to_string();
        let worker = TeamWorker {
            executor: Arc::clone(&self.executor),
            db: self.db.clone(),
            stopped: Arc::clone(&self.stopped),
            team_id: team.id.clone(),
            policy_valid: matches!(isolation.as_str(), "readOnly" | "worktree")
                && (!write_requested || write_allowed),
            request: AgentRequest::new(
                &item.id,
                prompt,
                agent_type,
                Some(model),
                if write_requested {
                    IsolationMode::Worktree
                } else {
                    IsolationMode::None
                },
                true,
            ),
            context: ChildExecutionContext {
                parent_session_id: team.session_id.clone(),
                parent_run_id: item.parent_run_id.clone(),
                working_directory: session.working_dir.into(),
                tool_use_id: item.id.clone(),
                allowed_tools,
                allow_write_tools: write_allowed,
                write_tool_allowlist: None,
                include_project_prompt: true,
            },
        };
        let result = self
            .tasks
            .submit_child(submission, move |execution| worker.execute(execution))
            .await;
        self.settle_submission(team, item, &claim, result).await
    }
    async fn settle_submission(
        &self,
        team: &TeamDefinition,
        item: &TeamWorkItem,
        claim: &str,
        result: Result<zk_engine::TaskSubmissionReceipt, zk_engine::TaskRuntimeError>,
    ) -> Result<(), String> {
        match result {
            Ok(receipt) => {
                self.db
                    .bind_team_work(&item.id, claim, &receipt.task.id)
                    .await
                    .map_err(|e| e.to_string())?;
                if self
                    .db
                    .find_team(&team.id)
                    .await
                    .map_err(|e| e.to_string())?
                    .is_none_or(|team| team.status != "open")
                {
                    self.tasks
                        .cancel_owned(
                            &team.session_id,
                            &receipt.task.id,
                            "Team closed during dispatch",
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                }
                Ok(())
            }
            Err(error) => {
                // A failed submission may have committed before losing its
                // acknowledgement. Recover that binding instead of declaring
                // that a possibly running child never existed.
                self.db
                    .reconcile_team_queue(self.startup_epoch)
                    .await
                    .map_err(|e| e.to_string())?;
                let persisted = self
                    .db
                    .team_work_items(&team.id)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(task_id) = persisted
                    .iter()
                    .find(|row| row.id == item.id)
                    .and_then(|row| row.task_id.as_deref())
                {
                    self.tasks
                        .cancel_owned(
                            &team.session_id,
                            task_id,
                            "Team submission acknowledgement failed",
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(())
                } else {
                    self.db
                        .reject_team_work(&item.id, claim, &error.code)
                        .await
                        .map_err(|e| e.to_string())
                }
            }
        }
    }
    fn publish_projection(&self, team: &TeamDefinition, projection: &Value) {
        let workers = projection["workers"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|worker| {
                let id = worker["workerId"].as_str().unwrap_or_default().to_owned();
                let status = match worker["status"].as_str().unwrap_or_default() {
                    "queued" | "claimed" => "STARTING",
                    "running" | "waitingDependencies" | "waitingInteraction" | "cancelling" => {
                        "WORKING"
                    }
                    _ => "TERMINATED",
                };
                (
                    id.clone(),
                    zk_protocol::WorkerSnapshot {
                        worker_id: id,
                        status: status.into(),
                        current_task: worker["task"]["id"].as_str().map(str::to_owned),
                        tool_call_count: worker["toolCallCount"].as_i64().unwrap_or(0),
                        token_consumed: worker["reportedTokens"].as_i64().unwrap_or(0),
                    },
                )
            })
            .collect();
        let phase = match projection["phase"].as_str().unwrap_or_default() {
            "CREATED" => "INITIALIZING",
            "RUNNING" | "QUEUED" => "RUNNING",
            "ABORTING" => "SHUTTING_DOWN",
            _ => "TERMINATED",
        };
        let _ = self
            .events
            .publish(zk_engine::CoordinatorEvent::SwarmStateUpdate {
                session_id: team.session_id.clone(),
                swarm_id: team.id.clone(),
                phase: phase.into(),
                active_workers: projection["activeWorkers"].as_i64().unwrap_or(0),
                total_workers: team.config["maxWorkers"].as_i64().unwrap_or(0),
                completed_tasks: projection["completedTasks"].as_i64().unwrap_or(0),
                total_tasks: projection["totalTasks"].as_i64().unwrap_or(0),
                workers,
            });
    }
    pub(crate) async fn projection(&self, team: &TeamDefinition) -> Result<Value, String> {
        let items = self
            .db
            .team_work_items(&team.id)
            .await
            .map_err(|e| e.to_string())?;
        let mut workers = Vec::new();
        let mut active = 0;
        let mut completed = 0;
        let mut queued = 0;
        for item in items {
            let task = match item.task_id.as_deref() {
                Some(id) => self
                    .tasks
                    .get_owned(&team.session_id, id)
                    .await
                    .map_err(|e| e.to_string())?,
                None => None,
            };
            if let Some(task) = &task {
                if !task.status.is_terminal() {
                    active += 1;
                }
                if task.status == zk_db::TaskStatus::Succeeded {
                    completed += 1;
                }
            } else if matches!(item.status.as_str(), "queued" | "claimed") {
                queued += 1;
            }
            let (tool_count, reported_tokens) = match item.task_id.as_deref() {
                Some(id) => self
                    .db
                    .team_worker_activity(id)
                    .await
                    .map_err(|error| error.to_string())?,
                None => (0, 0),
            };
            workers.push(json!({"workerId":item.task_id.as_deref().unwrap_or(&item.id),"queueId":item.id,"toolCallCount":tool_count,"reportedTokens":reported_tokens,"queueStatus":item.status,"status":task.as_ref().map_or(item.status.as_str(),|task|task.status.as_db()),"task":task,"errorCode":item.error_code}));
        }
        let phase = if active > 0 {
            if team.status == "stopping" {
                "ABORTING"
            } else {
                "RUNNING"
            }
        } else if queued > 0 {
            "QUEUED"
        } else if team.status != "open" {
            "ABORTED"
        } else if workers.is_empty() {
            "CREATED"
        } else if completed == workers.len() {
            "COMPLETED"
        } else {
            "FAILED"
        };
        Ok(
            json!({"swarmId":team.id,"teamName":team.id,"sessionId":team.session_id,"phase":phase,"intakeStatus":team.status,"maxWorkers":team.config["maxWorkers"],"activeWorkers":active,"completedTasks":completed,"totalTasks":workers.len(),"queuedTasks":queued,"workers":workers,"config":team.config}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, state::AppState};
    use futures::{StreamExt, stream};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use zk_llm::{
        ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry,
    };

    #[derive(Default)]
    struct Provider {
        calls: AtomicUsize,
        wait: AtomicBool,
        requests: Mutex<Vec<ChatRequest>>,
        hold_first_root: AtomicBool,
        hold_workers: AtomicBool,
        write_worker: AtomicBool,
        attempt_denied_write: AtomicBool,
        root_gate: Arc<tokio::sync::Notify>,
        worker_gate: Arc<tokio::sync::Notify>,
    }
    impl ChatProvider for Provider {
        fn provider_name(&self) -> &'static str {
            "deepseek"
        }
        fn chat_stream(
            &self,
            request: ChatRequest,
            cancel: tokio_util::sync::CancellationToken,
        ) -> Result<futures::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let is_root = request
                .execution
                .as_ref()
                .is_some_and(|owner| owner.kind == "conversation");
            let gate = if is_root && self.hold_first_root.swap(false, Ordering::AcqRel) {
                Some(Arc::clone(&self.root_gate))
            } else if !is_root && self.hold_workers.load(Ordering::Acquire) {
                Some(Arc::clone(&self.worker_gate))
            } else {
                None
            };
            let denied_attempt =
                !is_root && self.attempt_denied_write.swap(false, Ordering::AcqRel);
            let write_first =
                !is_root && self.write_worker.swap(false, Ordering::AcqRel) || denied_attempt;
            if write_first {
                assert_eq!(
                    request.tools.iter().any(|tool| tool.name == "Write"),
                    !denied_attempt
                );
            }
            self.requests.lock().unwrap().push(request);
            if write_first {
                return Ok(stream::iter([
                    ProviderEvent::ToolUseStart{id:"team-write".into(),name:"Write".into()},
                    ProviderEvent::ToolInputDelta{id:"team-write".into(),delta:json!({"file_path":"team-delivery.txt","content":"isolated team result\n"}).to_string()},
                    ProviderEvent::Finish{finish_reason:FinishReason::ToolUse,usage:Some(zk_protocol::Usage{input_tokens:3,output_tokens:2,..Default::default()})},
                ]).boxed());
            }
            if let Some(gate) = gate {
                return Ok(stream::once(async move {
                    tokio::select! {
                        () = gate.notified() => ProviderEvent::TextDelta { text: if is_root { "provisional root answer" } else { "verified worker output" }.into() },
                        () = cancel.cancelled() => ProviderEvent::Error { error: ProviderError::Cancelled },
                    }
                }).chain(stream::iter([ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(zk_protocol::Usage {input_tokens:3, output_tokens:2, ..zk_protocol::Usage::default()}),
                }])).boxed());
            }
            if self.wait.load(Ordering::Acquire) {
                return Ok(stream::once(async move {
                    cancel.cancelled().await;
                    ProviderEvent::Error {
                        error: ProviderError::Cancelled,
                    }
                })
                .boxed());
            }
            Ok(stream::iter([
                ProviderEvent::TextDelta {
                    text: "verified worker output".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(zk_protocol::Usage {
                        input_tokens: 3,
                        output_tokens: 2,
                        ..zk_protocol::Usage::default()
                    }),
                },
            ])
            .boxed())
        }
    }
    async fn fixture() -> (
        AppState,
        Arc<Provider>,
        zk_db::CreateTaskWithRunOutcome,
        zk_db::TeamDefinition,
        std::path::PathBuf,
    ) {
        fixture_with_writes(false).await
    }
    async fn fixture_with_writes(
        writes: bool,
    ) -> (
        AppState,
        Arc<Provider>,
        zk_db::CreateTaskWithRunOutcome,
        zk_db::TeamDefinition,
        std::path::PathBuf,
    ) {
        fixture_with_policy(writes, false).await
    }
    async fn fixture_with_policy(
        writes: bool,
        swarm: bool,
    ) -> (
        AppState,
        Arc<Provider>,
        zk_db::CreateTaskWithRunOutcome,
        zk_db::TeamDefinition,
        std::path::PathBuf,
    ) {
        let path = std::env::temp_dir().join(format!("zk-team-runtime-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let mut config = Config::test_config();
        config.agent_enabled = true;
        config.swarm_enabled = swarm;
        config.agent_write_enabled = writes;
        config.worktree_enabled = writes;
        config.workspace_default_root = path.to_str().unwrap().into();
        config.scratchpad_system_root = path.join("scratch");
        config.snapshot_dir = Some(path.join("snapshots"));
        config.mcp_registry_path = path.join("mcp.json");
        let provider = Arc::new(Provider::default());
        let mut providers = ProviderRegistry::new();
        providers.register("deepseek", provider.clone(), vec!["deepseek-flash".into()]);
        let db = Db::open_in_memory().unwrap();
        let state = AppState::new(db.clone(), config).with_providers(providers);
        state.set_startup_epoch(1).unwrap();
        let session = db
            .create_session("deepseek-flash", path.to_str().unwrap())
            .await
            .unwrap()
            .id;
        state
            .authz
            .modes
            .set_mode(&session, zk_authz::PermissionMode::DontAsk)
            .await
            .unwrap();
        let parent = db
            .create_task_with_run(&zk_db::CreateTaskWithRun {
                task_id: uuid::Uuid::new_v4().to_string(),
                run_id: uuid::Uuid::new_v4().to_string(),
                root_session_id: session.clone(),
                transcript_session_id: session.clone(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "user root".into(),
                prompt: Some("user task".into()),
                task_type: "agent".into(),
                model: "deepseek-flash".into(),
                working_dir: path.to_str().unwrap().into(),
                execution_config_json:
                    json!({"budget":{"deadlineAtMs":zk_db::time::now_millis()+60000}}).to_string(),
                startup_epoch: 1,
            })
            .await
            .unwrap();
        db.claim_task_run_cas(&parent.task.id, &parent.run_id, parent.task.version)
            .await
            .unwrap();
        let team = db
            .create_team(
                "team",
                &session,
                json!({"maxWorkers":1,"taskQueueSize":5,"workerToolAllowList":[]}),
            )
            .await
            .unwrap();
        (state, provider, parent, team, path)
    }
    async fn enqueue(state: &AppState, parent: &zk_db::CreateTaskWithRunOutcome, count: usize) {
        state.db.enqueue_team_work("team",&parent.task.session_id,&parent.task.id,&parent.run_id,"request",(0..count).map(|i|json!({"prompt":format!("work {i}"),"model":"deepseek-flash","agentType":"explore"})).collect()).await.unwrap();
    }
    #[tokio::test]
    async fn durable_queue_runs_real_child_engines_serially_and_preserves_results() {
        let (state, provider, parent, team, path) = fixture().await;
        enqueue(&state, &parent, 2).await;
        let runtime = state.team_runtime().unwrap();
        runtime.kick("team");
        runtime.kick("team");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if runtime.projection(&team).await.unwrap()["completedTasks"] == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(provider.calls.load(Ordering::Acquire), 2);
        assert!(
            provider
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request.tools.is_empty())
        );
        let items = state.db.team_work_items("team").await.unwrap();
        assert_eq!(items.len(), 2);
        for item in items {
            let task = state
                .db
                .find_runtime_task_by_id(item.task_id.as_ref().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                task.parent_task_id.as_deref(),
                Some(parent.task.id.as_str())
            );
            assert_eq!(task.status, zk_db::TaskStatus::Succeeded);
            assert_eq!(task.lifecycle_policy, "attached");
            assert!(
                state
                    .db
                    .read_task_result(&task.id, None, 0, 4096)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert!(
            !state.swarm_executable(),
            "core proof does not bypass the release gate or enable user flags"
        );
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn stopping_team_cancels_owned_worker_and_never_dispatches_queued_work() {
        let (state, provider, parent, _, path) = fixture().await;
        provider.wait.store(true, Ordering::Release);
        enqueue(&state, &parent, 2).await;
        let runtime = state.team_runtime().unwrap();
        runtime.kick("team");
        tokio::time::timeout(Duration::from_secs(10), async {
            while provider.calls.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        runtime.stop_intake("team", false);
        state.db.stop_team("team", false).await.unwrap();
        runtime.kick("team");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let items = state.db.team_work_items("team").await.unwrap();
                let mut active = false;
                for item in items {
                    if let Some(id) = item.task_id {
                        active |= !state
                            .db
                            .find_runtime_task_by_id(&id)
                            .await
                            .unwrap()
                            .unwrap()
                            .status
                            .is_terminal();
                    }
                }
                if !active {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(provider.calls.load(Ordering::Acquire), 1);
        let items = state.db.team_work_items("team").await.unwrap();
        assert!(
            items
                .iter()
                .any(|item| item.status == "cancelled" && item.task_id.is_none())
        );
        let id = items.iter().find_map(|item| item.task_id.as_ref()).unwrap();
        assert_eq!(
            state
                .db
                .find_runtime_task_by_id(id)
                .await
                .unwrap()
                .unwrap()
                .status,
            zk_db::TaskStatus::Cancelled
        );
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn broadcast_reaches_real_worker_once_without_mailbox_delivery() {
        let (state, provider, parent, team, path) = fixture().await;
        provider.hold_workers.store(true, Ordering::Release);
        state.db.enqueue_team_work(&team.id,&team.session_id,&parent.task.id,&parent.run_id,"broadcast-task",vec![json!({
            "prompt":"Report after coordination","model":"deepseek-flash","agentType":"explore"
        })]).await.unwrap();
        state.team_runtime().unwrap().kick(&team.id);
        let child = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let rows = state.db.team_work_items(&team.id).await.unwrap();
                if provider.calls.load(Ordering::Acquire) == 1
                    && let Some(id) = rows[0].task_id.clone()
                {
                    break id;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let receivers = state
            .db
            .broadcast_team(
                &team.id,
                &team.session_id,
                "broadcast-once",
                "Verify the requested detail",
            )
            .await
            .unwrap();
        assert_eq!(receivers, vec![child.clone()]);
        assert_eq!(
            state
                .db
                .broadcast_team(
                    &team.id,
                    &team.session_id,
                    "broadcast-once",
                    "Verify the requested detail"
                )
                .await
                .unwrap(),
            receivers
        );
        provider.hold_workers.store(false, Ordering::Release);
        provider.worker_gate.notify_one();
        assert_eq!(await_team_child(&state, &team.id).await, child);
        assert_eq!(provider.calls.load(Ordering::Acquire), 2);
        {
            let requests = provider.requests.lock().unwrap();
            assert_eq!(
                requests[1]
                    .messages
                    .iter()
                    .filter(|message| message.content.contains("Verify the requested detail"))
                    .count(),
                1
            );
            assert!(
                requests[1]
                    .messages
                    .iter()
                    .any(|message| message.content.contains("not new user authorization"))
            );
        }
        let messages = state.db.read_task_inbox(&child, &[], 100).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].status, zk_db::InboxStatus::Consumed);
        std::fs::remove_dir_all(path).unwrap();
    }

    async fn await_team_child(state: &AppState, team: &str) -> String {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let rows = state.db.team_work_items(team).await.unwrap();
                if let Some(id) = rows[0].task_id.as_deref()
                    && state
                        .db
                        .find_runtime_task_by_id(id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        .is_terminal()
                {
                    return id.to_owned();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn persisted_team_write_request_cannot_bypass_existing_write_gates() {
        let (state, provider, parent, _, path) = fixture().await;
        state
            .db
            .create_team(
                "disabled-writers",
                &parent.task.session_id,
                json!({
                    "backend":"IN_PROCESS","maxWorkers":1,"taskQueueSize":2,
                    "workerIsolation":"worktree","workerToolAllowList":["Write"]
                }),
            )
            .await
            .unwrap();
        state.db.enqueue_team_work("disabled-writers",&parent.task.session_id,&parent.task.id,&parent.run_id,"write",vec![json!({
            "prompt":"This saved configuration must not enable writes","model":"deepseek-flash","agentType":"general-purpose"
        })]).await.unwrap();
        state.team_runtime().unwrap().kick("disabled-writers");
        let child = await_team_child(&state, "disabled-writers").await;
        let result = state
            .db
            .read_task_result(&child, None, 0, 4096)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.result.status, zk_db::ResultStatus::Error);
        assert_eq!(
            result.result.error_code.as_deref(),
            Some("TEAM_WORKTREE_POLICY_DISABLED")
        );
        assert_eq!(provider.calls.load(Ordering::Acquire), 0);
        let count: i64 = state
            .db
            .with_reader(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM managed_worktrees", [], |row| {
                        row.get(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn git(path: &std::path::Path, args: &[&str]) -> String {
        let result = std::process::Command::new("git")
            .current_dir(path)
            .args(args)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        String::from_utf8(result.stdout).unwrap().trim().to_owned()
    }
    #[tokio::test]
    async fn team_worker_cannot_expand_parent_request_tool_ceiling() {
        let (state, provider, parent, _, path) = fixture_with_writes(true).await;
        git(&path, &["init", "-q"]);
        git(&path, &["config", "user.name", "Team Test"]);
        git(&path, &["config", "user.email", "team@example.invalid"]);
        git(&path, &["commit", "--allow-empty", "-qm", "base"]);
        state
            .authz
            .modes
            .set_mode(
                &parent.task.session_id,
                zk_authz::PermissionMode::AcceptEdits,
            )
            .await
            .unwrap();
        state
            .db
            .narrow_run_tool_ceiling(
                &parent.run_id,
                &zk_db::tool_ceiling::ToolCeiling {
                    allowed: None,
                    denied: std::collections::BTreeSet::from(["Write".into()]),
                },
            )
            .await
            .unwrap();
        state.db.create_team("writers", &parent.task.session_id, json!({"maxWorkers":1,"taskQueueSize":2,"workerIsolation":"worktree","workerToolAllowList":["Read","Write"]})).await.unwrap();
        provider.attempt_denied_write.store(true, Ordering::Release);
        state.db.enqueue_team_work("writers",&parent.task.session_id,&parent.task.id,&parent.run_id,"denied-write",vec![json!({"prompt":"Attempt a denied write","model":"deepseek-flash","agentType":"general-purpose"})]).await.unwrap();
        state.team_runtime().unwrap().kick("writers");
        let child = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let rows = state.db.team_work_items("writers").await.unwrap();
                if let Some(id) = rows[0].task_id.as_ref()
                    && let Some(task) = state.db.find_runtime_task_by_id(id).await.unwrap()
                    && task.status.is_terminal()
                {
                    break task;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let run = child.current_run_id.unwrap();
        assert!(
            !state
                .db
                .run_tool_ceiling(&run)
                .await
                .unwrap()
                .allows("Write")
        );
        assert_eq!(provider.calls.load(Ordering::Acquire), 2);
        assert!(
            provider
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request.tools.iter().all(|tool| tool.name != "Write"))
        );
        let attempts:Vec<(String,String)>=state.db.with_reader(move|conn| {
            let mut query=conn.prepare("SELECT tool_name,status FROM tool_invocations WHERE run_id=?1 AND invocation_kind='tool'")?;
            Ok(query.query_map([run],|row|Ok((row.get(0)?,row.get(1)?)))?.collect::<Result<_,_>>()?)
        }).await.unwrap();
        assert_eq!(attempts, vec![("Write".into(), "failed".into())]);
        assert!(!path.join("team-delivery.txt").exists());
        let records: Vec<String> = state
            .db
            .with_reader(|conn| {
                let mut query = conn.prepare("SELECT record_json FROM managed_worktrees")?;
                Ok(query
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<_, _>>()?)
            })
            .await
            .unwrap();
        for raw in records {
            let record: Value = serde_json::from_str(&raw).unwrap();
            let worktree = std::path::Path::new(record["path"].as_str().unwrap());
            assert!(!worktree.join("team-delivery.txt").exists());
            if worktree.exists() {
                git(
                    &path,
                    &["worktree", "remove", "--force", worktree.to_str().unwrap()],
                );
            }
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "This real owned-process and durable parent/child lifecycle regression keeps setup, terminal assertions and cleanup together"
    )]
    async fn team_write_worker_uses_managed_worktree_and_retains_delivery_without_commit() {
        let (state, provider, parent, _, path) = fixture_with_writes(true).await;
        git(&path, &["init", "-q"]);
        git(&path, &["config", "user.name", "Team Test"]);
        git(&path, &["config", "user.email", "team@example.invalid"]);
        git(&path, &["commit", "--allow-empty", "-qm", "base"]);
        let baseline = git(&path, &["rev-parse", "HEAD"]);
        state
            .authz
            .modes
            .set_mode(
                &parent.task.session_id,
                zk_authz::PermissionMode::AcceptEdits,
            )
            .await
            .unwrap();
        state.db.create_team("writers",&parent.task.session_id,json!({"maxWorkers":1,"taskQueueSize":2,"workerIsolation":"worktree","workerToolAllowList":["Read","Write"]})).await.unwrap();
        provider.write_worker.store(true, Ordering::Release);
        state.db.enqueue_team_work("writers",&parent.task.session_id,&parent.task.id,&parent.run_id,"write",vec![json!({"prompt":"Create a retained isolated delivery","model":"deepseek-flash","agentType":"general-purpose"})]).await.unwrap();
        state.team_runtime().unwrap().kick("writers");
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let rows = state.db.team_work_items("writers").await.unwrap();
                if let Some(id) = rows[0].task_id.as_deref()
                    && state
                        .db
                        .find_runtime_task_by_id(id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        .is_terminal()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let child = state.db.team_work_items("writers").await.unwrap()[0]
            .task_id
            .clone()
            .unwrap();
        let result = state
            .db
            .read_task_result(&child, None, 0, 65536)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.result.status,
            zk_db::ResultStatus::Complete,
            "{}",
            result.content
        );
        assert_eq!(provider.calls.load(Ordering::Acquire), 2);
        assert_eq!(git(&path, &["rev-parse", "HEAD"]), baseline);
        assert!(!path.join("team-delivery.txt").exists());
        let record: String = state
            .db
            .with_reader(|conn| {
                Ok(
                    conn.query_row("SELECT record_json FROM managed_worktrees", [], |row| {
                        row.get(0)
                    })?,
                )
            })
            .await
            .unwrap();
        let record: Value = serde_json::from_str(&record).unwrap();
        let worktree = std::path::Path::new(record["path"].as_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(worktree.join("team-delivery.txt")).unwrap(),
            "isolated team result\n"
        );
        assert_eq!(
            git(worktree, &["rev-parse", "HEAD"]),
            baseline,
            "no automatic commit"
        );
        assert!(
            result.content.contains("team-delivery")
                || result.content.contains(worktree.to_str().unwrap())
        );
        let child_task = state
            .db
            .find_runtime_task_by_id(&child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record["owner_run"],
            child_task.current_run_id.as_ref().unwrap().as_str()
        );
        assert_eq!(record["worker_active"], false);
        assert_eq!(
            state
                .db
                .run_cleanup_status(child_task.current_run_id.as_ref().unwrap())
                .await
                .unwrap(),
            zk_db::CleanupStatus::Confirmed
        );
        git(
            &path,
            &["worktree", "remove", "--force", worktree.to_str().unwrap()],
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "This real owned-process and durable parent/child lifecycle regression keeps setup, terminal assertions and cleanup together"
    )]
    async fn natural_root_completion_waits_for_queue_and_incorporates_real_worker_result() {
        let (state, provider, _, _, path) = fixture().await;
        let session = state
            .db
            .create_session("deepseek-flash", path.to_str().unwrap())
            .await
            .unwrap()
            .id;
        state
            .authz
            .modes
            .set_mode(&session, zk_authz::PermissionMode::DontAsk)
            .await
            .unwrap();
        state
            .db
            .create_team(
                "live",
                &session,
                json!({"maxWorkers":1,"taskQueueSize":3,"workerToolAllowList":[]}),
            )
            .await
            .unwrap();
        provider.hold_first_root.store(true, Ordering::Release);
        provider.hold_workers.store(true, Ordering::Release);
        let engine = crate::engine_bridge::wire_engine(&state);
        let mut root =
            engine.spawn_user_message(&session, "complete with the team's findings".into());
        tokio::time::timeout(Duration::from_secs(5), async {
            while provider.calls.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let run = state
            .db
            .find_latest_root_run_by_session(&session)
            .await
            .unwrap()
            .unwrap();
        state.db.enqueue_team_work("live", &session, &run.task_id, &run.id, "dispatch", vec![json!({"prompt":"inspect facts","model":"deepseek-flash","agentType":"explore"})]).await.unwrap();
        // The parent must also wait while the accepted queue has no Task binding.
        provider.root_gate.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(80), &mut root)
                .await
                .is_err()
        );
        assert_eq!(provider.calls.load(Ordering::Acquire), 1);
        let runtime = state.team_runtime().unwrap();
        runtime.kick("live");
        tokio::time::timeout(Duration::from_secs(5), async {
            while provider.calls.load(Ordering::Acquire) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(80), &mut root)
                .await
                .is_err()
        );
        provider.worker_gate.notify_one();
        if tokio::time::timeout(Duration::from_secs(10), &mut root)
            .await
            .is_err()
        {
            let rows = state.db.team_work_items("live").await.unwrap();
            let parent = state
                .db
                .find_runtime_task_by_id(&run.task_id)
                .await
                .unwrap();
            let mut children = Vec::new();
            for row in &rows {
                if let Some(id) = &row.task_id {
                    children.push(state.db.find_runtime_task_by_id(id).await.unwrap());
                }
            }
            let kinds: Vec<_> = provider
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|request| request.execution.as_ref().map(|owner| owner.kind.clone()))
                .collect();
            panic!(
                "parent stalled: {parent:?}; queue={rows:?}; children={children:?}; kinds={kinds:?}"
            );
        }
        {
            let requests = provider.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                3,
                "one worker request and two accounted root requests"
            );
            assert!(
                format!("{:?}", requests.last().unwrap().messages)
                    .contains("verified worker output")
            );
        }
        assert!(!state.db.pending_team_work(&run.id).await.unwrap());
        assert!(
            state
                .db
                .enqueue_team_work(
                    "live",
                    &session,
                    &run.task_id,
                    &run.id,
                    "late",
                    vec![json!({"prompt":"too late"})]
                )
                .await
                .is_err()
        );
        assert_eq!(
            state
                .db
                .find_runtime_task_by_id(&run.task_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            zk_db::TaskStatus::Succeeded
        );
        std::fs::remove_dir_all(path).unwrap();
    }
    async fn api_request(
        state: &AppState,
        path: &str,
        body: Option<Value>,
    ) -> (axum::http::StatusCode, Value) {
        use tower::ServiceExt;
        let method = if body.is_some() { "POST" } else { "GET" };
        let request = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("Origin", "http://127.0.0.1:5273")
            .header("Sec-Fetch-Site", "same-origin")
            .header("Content-Type", "application/json")
            .extension(axum::extract::ConnectInfo(
                "127.0.0.1:51717".parse::<std::net::SocketAddr>().unwrap(),
            ))
            .body(axum::body::Body::from(
                body.map_or_else(String::new, |value| value.to_string()),
            ))
            .unwrap();
        let response = crate::routes::build_router(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn configured_team_rest_uses_saved_policy_and_real_runtime_workers() {
        let (state, provider, parent, _, path) = fixture_with_policy(false, true).await;
        state
            .db
            .put_config_value(
                "user_config",
                &json!({"swarm":{"maxWorkers":2,"taskQueueSize":7,"workerToolAllowList":[]}})
                    .to_string(),
            )
            .await
            .unwrap();
        let (status, health) = api_request(&state, "/api/health", None).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(health["capabilities"]["swarm"]["executable"], true);
        let body = json!({"teamName":"api-team","sessionId":parent.task.session_id});
        let (status, created) = api_request(&state, "/api/swarm", Some(body.clone())).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{created}");
        assert_eq!(created["config"]["maxWorkers"], 2);
        assert_eq!(created["config"]["taskQueueSize"], 7);
        let mut bad = body.clone();
        bad["teamName"] = json!("external");
        bad["backend"] = json!("EXTERNAL_PROCESS");
        assert_eq!(
            api_request(&state, "/api/swarm", Some(bad)).await.0,
            axum::http::StatusCode::BAD_REQUEST
        );
        let mut bad = body;
        bad["teamName"] = json!("writes");
        bad["workerIsolation"] = json!("worktree");
        assert_eq!(
            api_request(&state, "/api/swarm", Some(bad)).await.0,
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        let payload = json!({"runId":parent.run_id,"requestId":"rest-once","tasks":[{"prompt":"Report the checked result"}]});
        let (status, first) = api_request(
            &state,
            "/api/swarm/api-team/dispatch",
            Some(payload.clone()),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{first}");
        let (status, replayed) =
            api_request(&state, "/api/swarm/api-team/dispatch", Some(payload)).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{replayed}");
        assert_eq!(first["queueIds"], replayed["queueIds"]);
        let child = await_team_child(&state, "api-team").await;
        assert_eq!(provider.calls.load(Ordering::Acquire), 1);
        assert!(
            state
                .db
                .read_task_result(&child, None, 0, 4096)
                .await
                .unwrap()
                .unwrap()
                .content
                .contains("verified worker output")
        );
        let (status, view) = api_request(&state, "/api/swarm/api-team", None).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(view["completedTasks"], 1);
        let (status, stopped) =
            api_request(&state, "/api/swarm/api-team/shutdown", Some(json!({}))).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{stopped}");
        assert_eq!(
            state
                .db
                .find_team("api-team")
                .await
                .unwrap()
                .unwrap()
                .status,
            "shutdown"
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}
