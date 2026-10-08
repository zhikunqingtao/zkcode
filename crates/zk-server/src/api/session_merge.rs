//! Session merge HTTP orchestration over the durable `SQLite` coordinator.
use crate::{error::ApiError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use zk_db::{SessionMergeOperation, SessionMergeRequest};

type WorkerKey = (String, i64);
#[derive(Clone, Copy)]
enum CancellationProgress {
    Waiting,
    Pending,
    Confirmed,
    StateChanged,
}
static CONTROL_SLOTS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
> = std::sync::OnceLock::new();
fn control_slot(id: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    let mut slots = CONTROL_SLOTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    slots.retain(|_, slot| slot.strong_count() > 0);
    if let Some(slot) = slots.get(id).and_then(std::sync::Weak::upgrade) {
        return slot;
    }
    let slot = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    slots.insert(id.into(), std::sync::Arc::downgrade(&slot));
    slot
}
struct WorkerState {
    epoch: i64,
    cancel_signal: tokio_util::sync::CancellationToken,
    cancel_requested: std::sync::atomic::AtomicBool,
    reconciling: std::sync::atomic::AtomicBool,
    cancellation_result: tokio::sync::watch::Sender<CancellationProgress>,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    finished: std::sync::atomic::AtomicBool,
    done: tokio::sync::Notify,
}
static WORKERS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<WorkerKey, std::sync::Arc<WorkerState>>>,
> = std::sync::OnceLock::new();
static CAPTURE_SLOTS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Weak<tokio::sync::Semaphore>>>,
> = std::sync::OnceLock::new();
#[cfg(test)]
type CaptureTestGate = (
    tokio::sync::mpsc::UnboundedSender<&'static str>,
    std::sync::Arc<tokio::sync::Notify>,
);
#[cfg(test)]
static CAPTURE_TEST_GATES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, CaptureTestGate>>,
> = std::sync::OnceLock::new();
fn capture_slot(state: &AppState) -> std::sync::Arc<tokio::sync::Semaphore> {
    let key = if state.config.db_path == std::path::Path::new(":memory:") {
        format!("memory:{:p}", std::sync::Arc::as_ptr(&state.task_runtime))
    } else {
        std::fs::canonicalize(&state.config.db_path)
            .unwrap_or_else(|_| state.config.db_path.clone())
            .to_string_lossy()
            .into_owned()
    };
    let mut slots = CAPTURE_SLOTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    slots.retain(|_, slot| slot.strong_count() > 0);
    if let Some(slot) = slots.get(&key).and_then(std::sync::Weak::upgrade) {
        return slot;
    }
    let slot = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    slots.insert(key, std::sync::Arc::downgrade(&slot));
    slot
}
struct WorkerGuard {
    key: WorkerKey,
    state: std::sync::Arc<WorkerState>,
}
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.state
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
        self.state.done.notify_waiters();
        if let Some(workers) = WORKERS.get() {
            let mut workers = workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Stopping execution is not proof that its cancellation was saved.
            if !self
                .state
                .cancel_requested
                .load(std::sync::atomic::Ordering::Acquire)
                && workers
                    .get(&self.key)
                    .is_some_and(|current| std::sync::Arc::ptr_eq(current, &self.state))
            {
                workers.remove(&self.key);
            }
        }
    }
}
fn new_worker(epoch: i64, finished: bool) -> std::sync::Arc<WorkerState> {
    std::sync::Arc::new(WorkerState {
        epoch,
        cancel_signal: tokio_util::sync::CancellationToken::new(),
        cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        cancel_requested: std::sync::atomic::AtomicBool::new(false),
        reconciling: std::sync::atomic::AtomicBool::new(false),
        cancellation_result: tokio::sync::watch::channel(CancellationProgress::Waiting).0,
        finished: std::sync::atomic::AtomicBool::new(finished),
        done: tokio::sync::Notify::new(),
    })
}
fn cancellation_pending(id: &str) -> bool {
    WORKERS.get().is_some_and(|workers| {
        workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|((operation, _), worker)| {
                operation == id
                    && worker
                        .cancel_requested
                        .load(std::sync::atomic::Ordering::Acquire)
            })
    })
}
fn project_local_stop(mut operation: SessionMergeOperation) -> SessionMergeOperation {
    if cancellation_pending(&operation.operation_id) {
        operation.can_resume = false;
        operation.can_cancel = true;
        operation.error = Some("MERGE_CANCELLATION_PENDING：本地执行已请求停止，取消状态或来源锁释放仍待确认，请勿重新创建合并。".into());
    }
    operation
}
fn cancellation_pending_error() -> ApiError {
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "MERGE_CANCELLATION_PENDING".into(),
        message: "本地执行已请求停止；取消状态或来源锁释放仍待确认，停止屏障会保留。".into(),
    }
}
async fn wait_worker(worker: &WorkerState) {
    loop {
        let notified = worker.done.notified();
        if worker.finished.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}
fn retry_cancellation(error: &zk_db::DbError) -> bool {
    match error {
        zk_db::DbError::Sqlite(rusqlite::Error::SqliteFailure(error, _)) => {
            matches!(error.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                | rusqlite::ErrorCode::SystemIoFailure | rusqlite::ErrorCode::DiskFull
                | rusqlite::ErrorCode::CannotOpen | rusqlite::ErrorCode::ReadOnly)
                // A host-owned trigger can temporarily reject writes during maintenance.
                || error.extended_code == 1811
        }
        zk_db::DbError::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::TimedOut
        ),
        _ => false,
    }
}
fn reconcile_cancel(state: AppState, id: String, worker: std::sync::Arc<WorkerState>) {
    if worker
        .reconciling
        .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        return;
    }
    worker
        .cancellation_result
        .send_replace(CancellationProgress::Waiting);
    tokio::spawn(async move {
        // The original worker remains the only owner of capture/provider cleanup.
        wait_worker(&worker).await;
        let mut failures = 0_u32;
        loop {
            let outcome = async {
                let operation = state
                    .db
                    .session_merge(&id)
                    .await?
                    .ok_or_else(|| zk_db::DbError::Invalid("merge disappeared".into()))?;
                // A worker may have persisted its own failure pause just as stop arrived.
                let epoch =
                    if operation.status == "paused" && operation.run_epoch == worker.epoch + 1 {
                        operation.run_epoch
                    } else {
                        worker.epoch
                    };
                state
                    .db
                    .transition_session_merge(&id, Some(epoch), None, true)
                    .await?;
                state.db.release_stopped_merge_sources(&id).await
            }
            .await;
            match outcome {
                Ok(()) => {
                    let key = (id.clone(), worker.epoch);
                    let mut workers = WORKERS
                        .get()
                        .unwrap()
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if workers
                        .get(&key)
                        .is_some_and(|current| std::sync::Arc::ptr_eq(current, &worker))
                    {
                        workers.remove(&key);
                    }
                    worker
                        .cancellation_result
                        .send_replace(CancellationProgress::Confirmed);
                    return;
                }
                Err(error) => {
                    if matches!(&error, zk_db::DbError::Conflict(_))
                        && state
                            .db
                            .session_merge(&id)
                            .await
                            .ok()
                            .flatten()
                            .is_some_and(|operation| {
                                operation.status == "completed"
                                    || operation.run_epoch > worker.epoch + 1
                            })
                    {
                        // Publication or a newer explicit generation won before this stop.
                        WORKERS
                            .get()
                            .unwrap()
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&(id.clone(), worker.epoch));
                        worker
                            .cancellation_result
                            .send_replace(CancellationProgress::StateChanged);
                        return;
                    }
                    worker
                        .cancellation_result
                        .send_replace(CancellationProgress::Pending);
                    if failures == 0 || failures.is_power_of_two() {
                        tracing::warn!(operation_id=%id, epoch=worker.epoch, code=error.diagnostic_code(), "merge stopped; cancellation reconciliation pending");
                    }
                    if !retry_cancellation(&error) {
                        // Keep the fence visible; an explicit retry can restart reconciliation.
                        worker
                            .reconciling
                            .store(false, std::sync::atomic::Ordering::Release);
                        return;
                    }
                    failures = failures.saturating_add(1);
                    // Only retry the idempotent fence/release, never capture or paid work.
                    let delay = if failures > 8 {
                        30_000
                    } else {
                        50_u64 << failures.min(6)
                    };
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
            }
        }
    });
}
fn signal_workers(id: &str) -> Vec<std::sync::Arc<WorkerState>> {
    let workers = WORKERS
        .get()
        .map(|workers| {
            workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|((operation, _), _)| operation == id)
                .map(|(_, worker)| worker.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for worker in &workers {
        worker.cancel_signal.cancel();
        worker
            .cancelled
            .store(true, std::sync::atomic::Ordering::Release);
    }
    workers
}
async fn stop_workers(id: &str) {
    for worker in signal_workers(id) {
        wait_worker(&worker).await;
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep worker ownership, its epoch/cancellation fences and capture-to-publication error handling in one auditable lifecycle"
)]
fn start_worker(state: AppState, operation: &SessionMergeOperation) {
    if operation.status != "preparing" {
        return;
    }
    let (id, epoch) = (operation.operation_id.clone(), operation.run_epoch);
    let worker = new_worker(epoch, false);
    {
        let mut workers = WORKERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if workers.contains_key(&(id.clone(), epoch))
            || workers.iter().any(|((operation, _), worker)| {
                operation == &id
                    && worker
                        .cancel_requested
                        .load(std::sync::atomic::Ordering::Acquire)
            })
        {
            return;
        }
        workers.insert((id.clone(), epoch), worker.clone());
    }
    let guard = WorkerGuard {
        key: (id.clone(), epoch),
        state: worker.clone(),
    };
    let capture_slot = capture_slot(&state);
    tokio::spawn(async move {
        let _guard = guard;
        let outcome = async {
            let operation = state
                .db
                .session_merge(&id)
                .await?
                .ok_or_else(|| zk_db::DbError::Invalid("merge disappeared".into()))?;
            if operation.run_epoch != epoch || operation.status != "preparing" {
                return Err(zk_db::DbError::Conflict(
                    "merge worker has been fenced".into(),
                ));
            }
            if !operation.snapshot_sealed {
                #[cfg(test)]
                let test_gate=CAPTURE_TEST_GATES.get_or_init(Default::default).lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&id);
                #[cfg(test)]
                if let Some((events,_))=&test_gate { let _=events.send("waiting"); }
                let _permit = tokio::select! {
                    permit = capture_slot.acquire() => permit.map_err(|_| zk_db::DbError::Invalid("MERGE_CAPTURE_UNAVAILABLE".into()))?,
                    () = worker.cancel_signal.cancelled() => return Err(zk_db::DbError::Conflict("MERGE_CAPTURE_CANCELLED".into())),
                };
                if worker.cancelled.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(zk_db::DbError::Conflict("MERGE_CAPTURE_CANCELLED".into()));
                }
                #[cfg(test)]
                if let Some((events,release))=test_gate {
                    let _=events.send("entered");
                    tokio::select! {
                        ()=release.notified()=>{},
                        ()=worker.cancel_signal.cancelled()=>return Err(zk_db::DbError::Conflict("MERGE_CAPTURE_CANCELLED".into())),
                    }
                }
                for source in &operation.locked_source_session_ids {
                    crate::python::tools::close_session_browser_contexts(
                        &state.db,
                        &state.python,
                        source,
                    )
                    .await
                    .map_err(zk_db::DbError::Invalid)?;
                }
                state
                    .db
                    .prepare_session_merge_capture(
                        &id,
                        epoch,
                        Some(state.config.scratchpad_system_root.clone()),
                        worker.cancelled.clone(),
                    )
                    .await?;
            }
            let operation = state
                .db
                .session_merge(&id)
                .await?
                .ok_or_else(|| zk_db::DbError::Invalid("merge disappeared".into()))?;
            crate::session_merge_summary::prepare_with_cancel(
                &state,
                &operation,
                worker.cancel_signal.clone(),
            )
            .await?;
            state
                .db
                .complete_session_merge_with_cancel(&id, epoch, worker.cancelled.clone())
                .await
        }
        .await;
        match outcome {
            Ok(operation) => {
                // The database target already inherited the sealed primary mode.
                // Publish the same value into the synchronous authorization cache.
                if let Ok(modes) = state.db.permission_modes_at_startup()
                    && let Some(mode) = modes
                        .get(&operation.target_session_id)
                        .and_then(|m| zk_authz::model::PermissionMode::parse(m))
                {
                    state
                        .authz
                        .modes
                        .set_ephemeral_mode(&operation.target_session_id, mode);
                }
            }
            Err(_)
                if worker
                    .cancel_requested
                    .load(std::sync::atomic::Ordering::Acquire) =>
            {
                // The cancellation coordinator owns persistence and source release.
            }
            Err(error) => {
                // Only a proven newer durable state makes an old worker obsolete.
                if matches!(&error, zk_db::DbError::Conflict(_))
                    && state
                        .db
                        .session_merge(&id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|operation| {
                            operation.run_epoch != epoch || operation.status != "preparing"
                        })
                {
                    return;
                }
                tracing::error!(operation_id=%id,%error,"session merge paused");
                if let Err(persist) = state
                    .db
                    .pause_session_merge(
                        id,
                        epoch,
                        match &error {
                            zk_db::DbError::Invalid(code)
                                if code.starts_with("MERGE_") || code.starts_with("BUDGET_") =>
                            {
                                format!("{code}：合并进度已保留，可检查配置后恢复或取消。")
                            }
                            _ => "合并未完成，进度已保留；请检查配置后恢复或取消。".into(),
                        },
                    )
                    .await
                {
                    tracing::error!(%persist,"could not persist merge failure");
                }
            }
        }
    });
}

pub(crate) async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<SessionMergeRequest>,
) -> Result<(StatusCode, Json<SessionMergeOperation>), ApiError> {
    let key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::validation("Idempotency-Key is required"))?
        .to_owned();
    if let Some(model) = request.model.as_deref().filter(|m| !m.is_empty()) {
        request.model = Some(super::session::resolve_model(&state, Some(model))?);
    } else {
        request.model = None;
    }
    let operation = state
        .db
        .reserve_session_merge(key, request)
        .await
        .map_err(merge_start_error)?;
    start_worker(state, &operation);
    Ok((StatusCode::ACCEPTED, Json(project_local_stop(operation))))
}

fn merge_start_error(error: zk_db::DbError) -> ApiError {
    match &error {
        zk_db::DbError::Invalid(code) if code == "MERGE_DISK_SPACE_LOW" => ApiError {
            status: StatusCode::INSUFFICIENT_STORAGE,
            code: code.clone(),
            message: "数据库所在磁盘空间不足；合并写入后必须至少保留 1 GiB 可用空间。此次创建已回滚，请释放空间后重试。".into(),
        },
        zk_db::DbError::Invalid(code) if code.starts_with("MERGE_DISK_SPACE_CHECK_FAILED") => {
            tracing::error!(%error, "could not verify merge disk reserve");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "MERGE_DISK_SPACE_CHECK_FAILED".into(),
                message: "无法确认数据库所在磁盘的可用空间，此次合并创建已回滚，请检查磁盘后重试。".into(),
            }
        }
        _ => error.into(),
    }
}

pub(crate) async fn get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionMergeOperation>, ApiError> {
    Ok(Json(project_local_stop(
        state
            .db
            .session_merge(&id)
            .await?
            .ok_or_else(|| ApiError::not_found("MERGE_NOT_FOUND", "Merge operation not found"))?,
    )))
}

pub(crate) async fn active(State(state): State<AppState>) -> Result<Response, ApiError> {
    // A durable cancelled row can still own unconfirmed source release locally.
    let pending: Vec<_> = WORKERS
        .get()
        .map(|workers| {
            workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|(_, worker)| {
                    worker
                        .cancel_requested
                        .load(std::sync::atomic::Ordering::Acquire)
                })
                .map(|((id, _), _)| id.clone())
                .collect()
        })
        .unwrap_or_default();
    for id in pending {
        if let Some(operation) = state.db.session_merge(&id).await? {
            return Ok(Json(project_local_stop(operation)).into_response());
        }
    }
    Ok(match state.db.active_session_merge().await? {
        Some(operation) => Json(project_local_stop(operation)).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ResumeRequest {
    expected_epoch: i64,
    model: Option<String>,
}

pub(crate) async fn resume(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<ResumeRequest>,
) -> Result<(StatusCode, Json<SessionMergeOperation>), ApiError> {
    let control = control_slot(&id);
    let _control = control.lock().await;
    if cancellation_pending(&id) {
        return Err(cancellation_pending_error());
    }
    let model = request
        .model
        .as_deref()
        .map(|model| super::session::resolve_model(&state, Some(model)))
        .transpose()?;
    let previous = state
        .db
        .session_merge(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("MERGE_NOT_FOUND", "Merge operation not found"))?;
    if !previous.can_resume || previous.run_epoch != request.expected_epoch {
        return Err(zk_db::DbError::Conflict("merge state or epoch changed".into()).into());
    }
    stop_workers(&id).await;
    state
        .db
        .cleanup_stopped_merge_staging(&id, request.expected_epoch)
        .await?;
    let operation = state
        .db
        .transition_session_merge(&id, Some(request.expected_epoch), model, false)
        .await?;
    start_worker(state, &operation);
    Ok((StatusCode::ACCEPTED, Json(project_local_stop(operation))))
}

pub(crate) async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionMergeOperation>, ApiError> {
    let control = control_slot(&id);
    let _control = control.lock().await;
    // Install the local fence under the same lock used by WorkerGuard/start_worker.
    let mut owners = {
        let workers = WORKERS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owners: Vec<_> = workers
            .iter()
            .filter(|((operation, _), _)| operation == &id)
            .map(|(_, worker)| worker.clone())
            .collect();
        for worker in &owners {
            worker
                .cancel_requested
                .store(true, std::sync::atomic::Ordering::Release);
            worker
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
            worker.cancel_signal.cancel();
            reconcile_cancel(state.clone(), id.clone(), worker.clone());
        }
        owners
    };
    if owners.is_empty() {
        let operation =
            state.db.session_merge(&id).await?.ok_or_else(|| {
                ApiError::not_found("MERGE_NOT_FOUND", "Merge operation not found")
            })?;
        if operation.status == "completed" {
            return Err(zk_db::DbError::Conflict("merge already completed".into()).into());
        }
        let mut workers = WORKERS
            .get()
            .unwrap()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let worker = workers
            .entry((id.clone(), operation.run_epoch))
            .or_insert_with(|| new_worker(operation.run_epoch, true))
            .clone();
        worker
            .cancel_requested
            .store(true, std::sync::atomic::Ordering::Release);
        worker
            .cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        worker.cancel_signal.cancel();
        reconcile_cancel(state.clone(), id.clone(), worker.clone());
        owners.push(worker);
    }
    for worker in owners {
        let mut progress = worker.cancellation_result.subscribe();
        loop {
            let result = *progress.borrow_and_update();
            match result {
                CancellationProgress::Confirmed => break,
                CancellationProgress::Pending => return Err(cancellation_pending_error()),
                CancellationProgress::StateChanged => {
                    return Err(
                        zk_db::DbError::Conflict("merge state or epoch changed".into()).into(),
                    );
                }
                CancellationProgress::Waiting => {
                    progress
                        .changed()
                        .await
                        .map_err(|_| cancellation_pending_error())?;
                }
            }
        }
    }
    Ok(Json(state.db.session_merge(&id).await?.ok_or_else(
        || ApiError::not_found("MERGE_NOT_FOUND", "Merge operation not found"),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn capture_event(
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<&'static str>,
    ) -> &'static str {
        tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn same_database_workers_capture_one_at_a_time_and_cancel_releases_the_permit() {
        let state = AppState::for_tests();
        let mut operations = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..2 {
            let first = state.db.create_session("fixture", "/tmp").await.unwrap();
            let second = state.db.create_session("fixture", "/tmp").await.unwrap();
            let operation = state
                .db
                .reserve_session_merge(
                    uuid::Uuid::new_v4().to_string(),
                    SessionMergeRequest {
                        source_session_ids: vec![first.id.clone(), second.id],
                        primary_session_id: first.id,
                        title: None,
                        model: None,
                    },
                )
                .await
                .unwrap();
            let (events, receiver) = tokio::sync::mpsc::unbounded_channel();
            CAPTURE_TEST_GATES
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(
                    operation.operation_id.clone(),
                    (events, std::sync::Arc::new(tokio::sync::Notify::new())),
                );
            operations.push(operation);
            receivers.push(receiver);
        }
        start_worker(state.clone(), &operations[0]);
        assert_eq!(capture_event(&mut receivers[0]).await, "waiting");
        assert_eq!(capture_event(&mut receivers[0]).await, "entered");
        start_worker(state.clone(), &operations[1]);
        assert_eq!(capture_event(&mut receivers[1]).await, "waiting");
        assert!(
            matches!(
                receivers[1].try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "second worker entered while first owns capture permit"
        );
        let first = cancel(
            State(state.clone()),
            Path(operations[0].operation_id.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(first.status, "cancelled");
        assert!(first.locked_source_session_ids.is_empty());
        assert_eq!(capture_event(&mut receivers[1]).await, "entered");
        let second = cancel(
            State(state.clone()),
            Path(operations[1].operation_id.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(second.status, "cancelled");
        assert!(second.locked_source_session_ids.is_empty());
        for operation in operations {
            assert!(
                !state
                    .db
                    .session_merge(&operation.operation_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .snapshot_sealed
            );
        }
        state
            .execution_supervisor
            .shutdown(std::time::Duration::from_secs(5))
            .await;
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "Keep the Router/SQLite outage, stop fence, and eventual recovery assertions in one auditable scenario"
    )]
    async fn router_cancel_write_outage_retains_stop_and_recovers_without_a_second_cancel() {
        use axum::{body::Body, extract::ConnectInfo, http::Request};
        use tower::ServiceExt;
        let state = AppState::for_tests();
        let first = state.db.create_session("fixture", "/tmp").await.unwrap();
        let second = state.db.create_session("fixture", "/tmp").await.unwrap();
        let key = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({"sourceSessionIds":[first.id,second.id],"primarySessionId":first.id});
        let router = crate::routes::build_router(state.clone());
        let request = |uri: &str, body: String| {
            Request::builder()
                .method("POST")
                .uri(uri)
                .extension(ConnectInfo(
                    "127.0.0.1:51717".parse::<std::net::SocketAddr>().unwrap(),
                ))
                .header("Origin", "http://127.0.0.1:5273")
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", &key)
                .body(Body::from(body))
                .unwrap()
        };
        // Block capture admission, so no fixture touches live browser resources or model providers.
        let slot = capture_slot(&state);
        let permit = slot.acquire().await.unwrap();
        let created = router
            .clone()
            .oneshot(request("/api/sessions/merge", body.to_string()))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::ACCEPTED);
        let created: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(created.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let created = state
            .db
            .session_merge(created["operationId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        state.db.with_writer(|conn| {
            conn.execute_batch("CREATE TRIGGER reject_merge_cancel BEFORE UPDATE OF status ON session_merges WHEN NEW.status='cancelled' BEGIN SELECT RAISE(ABORT,'fixture cancellation save failure'); END;")?;
            Ok(())
        }).await.unwrap();
        let response = router
            .clone()
            .oneshot(request(
                &format!("/api/session-merges/{}/cancel", created.operation_id),
                String::new(),
            ))
            .await
            .unwrap();
        assert!(response.status().is_server_error());
        let retry = router
            .clone()
            .oneshot(request("/api/sessions/merge", body.to_string()))
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::ACCEPTED);
        let retry: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(retry.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(retry["operationId"], created.operation_id);
        assert_eq!(retry["canResume"], false);
        assert!(
            retry["error"]
                .as_str()
                .is_some_and(|error| error.starts_with("MERGE_CANCELLATION_PENDING"))
        );
        let retained = WORKERS
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .get(&(created.operation_id.clone(), created.run_epoch))
            .cloned();
        let stop_retained = retained
            .as_ref()
            .is_some_and(|worker| worker.cancelled.load(std::sync::atomic::Ordering::Acquire));
        state
            .db
            .with_writer(|conn| {
                conn.execute_batch("DROP TRIGGER reject_merge_cancel;")?;
                Ok(())
            })
            .await
            .unwrap();
        // Release all local fixtures even if the pre-fix assertion fails.
        if !stop_retained {
            stop_workers(&created.operation_id).await;
        }
        drop(permit);
        assert!(
            stop_retained,
            "same-key create replaced the stopped owner after a cancellation write failure"
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let op = state
                    .db
                    .session_merge(&created.operation_id)
                    .await
                    .unwrap()
                    .unwrap();
                if op.status == "cancelled" && op.locked_source_session_ids.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("original cancellation owner must reconcile after storage recovers");
        assert!(
            state
                .db
                .get_session(&created.target_session_id)
                .await
                .unwrap()
                .is_none()
        );
        state
            .execution_supervisor
            .shutdown(std::time::Duration::from_secs(5))
            .await;
    }

    #[tokio::test]
    async fn cancelled_merge_keeps_source_release_pending_visible_and_retries() {
        let state = AppState::for_tests();
        let first = state.db.create_session("fixture", "/tmp").await.unwrap();
        let second = state.db.create_session("fixture", "/tmp").await.unwrap();
        let operation = state
            .db
            .reserve_session_merge(
                uuid::Uuid::new_v4().to_string(),
                SessionMergeRequest {
                    source_session_ids: vec![first.id.clone(), second.id],
                    primary_session_id: first.id,
                    title: None,
                    model: None,
                },
            )
            .await
            .unwrap();
        state.db.with_writer(|conn| {
            conn.execute_batch("CREATE TRIGGER reject_merge_unlock BEFORE DELETE ON session_merge_locks BEGIN SELECT RAISE(ABORT,'fixture unlock failure'); END;")?; Ok(())
        }).await.unwrap();
        let error = cancel(State(state.clone()), Path(operation.operation_id.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.code, "MERGE_CANCELLATION_PENDING");
        let current = state
            .db
            .session_merge(&operation.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "cancelled");
        assert!(!current.locked_source_session_ids.is_empty());
        let response = active(State(state.clone())).await.unwrap();
        let visible: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(visible["operationId"], operation.operation_id);
        assert_eq!(visible["canCancel"], true);
        assert!(
            visible["error"]
                .as_str()
                .unwrap()
                .starts_with("MERGE_CANCELLATION_PENDING")
        );
        state
            .db
            .with_writer(|conn| {
                conn.execute_batch("DROP TRIGGER reject_merge_unlock;")?;
                Ok(())
            })
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while cancellation_pending(&operation.operation_id) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            state
                .db
                .session_merge(&operation.operation_id)
                .await
                .unwrap()
                .unwrap()
                .locked_source_session_ids
                .is_empty()
        );
        state
            .execution_supervisor
            .shutdown(std::time::Duration::from_secs(5))
            .await;
    }

    #[test]
    fn permanent_merge_cancellation_errors_do_not_spin() {
        assert!(!retry_cancellation(&zk_db::DbError::Invalid(
            "identity changed".into()
        )));
        assert!(!retry_cancellation(&zk_db::DbError::Conflict(
            "old epoch".into()
        )));
        assert!(!retry_cancellation(&zk_db::DbError::Sqlite(
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
                None
            )
        )));
        assert!(retry_cancellation(&zk_db::DbError::Sqlite(
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                None
            )
        )));
    }

    #[test]
    fn snapshot_space_failures_report_rollback_without_exposing_probe_details() {
        let low = merge_start_error(zk_db::DbError::Invalid("MERGE_DISK_SPACE_LOW".into()));
        assert_eq!(low.status, StatusCode::INSUFFICIENT_STORAGE);
        assert_eq!(low.code, "MERGE_DISK_SPACE_LOW");
        assert!(low.message.contains("已回滚"));
        let probe = merge_start_error(zk_db::DbError::Invalid(
            "MERGE_DISK_SPACE_CHECK_FAILED: /private/database/path".into(),
        ));
        assert_eq!(probe.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(probe.code, "MERGE_DISK_SPACE_CHECK_FAILED");
        assert!(!probe.message.contains("/private"));
    }
}
