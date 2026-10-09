//! Session snapshot REST adapters backed by the singleton snapshot service.

use axum::Json;
use axum::extract::{Path, State};
use serde_json::{Value, json};
use zk_db::SnapshotRestoreOutcome;
use zk_engine::{SessionSnapshot, SessionSnapshotError, SessionSnapshotSummary};

use crate::error::ApiError;
use crate::state::AppState;

fn invalid_snapshot_id() -> ApiError {
    ApiError::validation_with_code("SNAPSHOT_ID_INVALID", "Snapshot session id is invalid")
}

fn snapshot_error(error: SessionSnapshotError) -> ApiError {
    match error {
        SessionSnapshotError::InvalidId(_) => invalid_snapshot_id(),
        SessionSnapshotError::Content(error) => error.into(),
        SessionSnapshotError::Write(error) => {
            tracing::error!(%error, "snapshot save failed before replacement");
            ApiError {
                status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                code: "SNAPSHOT_WRITE_FAILED".into(),
                message: "Snapshot could not be saved; any previous snapshot was retained".into(),
            }
        }
        SessionSnapshotError::Serialize(error) => {
            tracing::error!(%error, "snapshot serialization failed");
            ApiError {
                status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                code: "SNAPSHOT_WRITE_FAILED".into(),
                message: "Snapshot could not be serialized".into(),
            }
        }
        SessionSnapshotError::PersistenceUnconfirmed(error) => {
            tracing::error!(%error, "snapshot rename succeeded but durability is unconfirmed");
            ApiError {
                status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
                code: "SNAPSHOT_PERSISTENCE_UNCONFIRMED".into(),
                message: "Snapshot was replaced, but durable storage could not be confirmed".into(),
            }
        }
    }
}

async fn require_durable_snapshot(state: &AppState, session_id: &str) -> Result<(), ApiError> {
    if state.db.session_retention(session_id).await? == zk_db::content::ContentRetention::Ephemeral
    {
        return Err(ApiError::validation_with_code(
            "EPHEMERAL_OPERATION_UNSUPPORTED",
            "Temporary conversations cannot save or resume durable snapshots",
        ));
    }
    Ok(())
}

fn snapshot_not_found(session_id: &str) -> ApiError {
    ApiError::not_found(
        "SNAPSHOT_NOT_FOUND",
        &format!("Snapshot not found for session: {session_id}"),
    )
}

/// `GET /api/sessions/snapshots`.
pub(crate) async fn list(State(state): State<AppState>) -> Json<Vec<SessionSnapshotSummary>> {
    Json(state.session_snapshots.list_snapshots().await)
}

/// `POST /api/sessions/{sessionId}/snapshot`.
pub(crate) async fn save(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionSnapshotSummary>, ApiError> {
    require_durable_snapshot(&state, &session_id).await?;
    let detail = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let snapshot = SessionSnapshot::from_session_detail(&detail);
    state
        .session_snapshots
        .save_snapshot(&session_id, &snapshot)
        .await
        .map_err(snapshot_error)?;
    Ok(Json(snapshot.summary()))
}

/// `POST /api/sessions/{sessionId}/snapshot/resume`.
pub(crate) async fn resume(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionSnapshotSummary>, ApiError> {
    require_durable_snapshot(&state, &session_id).await?;
    let snapshot = state
        .session_snapshots
        .load_snapshot(&session_id)
        .await
        .map_err(snapshot_error)?
        .ok_or_else(|| snapshot_not_found(&session_id))?;
    if snapshot.session_id.as_deref() != Some(session_id.as_str()) {
        return Err(ApiError::validation_with_code(
            "SNAPSHOT_SESSION_MISMATCH",
            "Snapshot session id does not match the requested session",
        ));
    }
    // Queries reserve this same slot before reading history, before their Run
    // exists in SQLite. Keep it through the restore commit; the DB transaction
    // separately protects durable/background work and postprocessing facts.
    let reservation = if let Some(conversation) = state.conversation() {
        Some(
            conversation
                .try_reserve_session_mutation(&session_id)
                .ok_or_else(|| ApiError {
                    status: axum::http::StatusCode::CONFLICT,
                    code: "SESSION_BUSY".into(),
                    message: "Wait for the current session operation before restoring a snapshot"
                        .into(),
                })?,
        )
    } else {
        None
    };
    let current = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let snapshot_workspace = metadata_string(&snapshot, "workingDir").ok_or_else(|| {
        ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_MISSING",
            "Snapshot has no authorized workspace",
        )
    })?;
    let current_canonical = std::fs::canonicalize(&current.working_dir).map_err(|_| {
        ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_UNAVAILABLE",
            "Session workspace is unavailable",
        )
    })?;
    let snapshot_canonical = std::fs::canonicalize(snapshot_workspace).map_err(|_| {
        ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_UNAVAILABLE",
            "Snapshot workspace is unavailable",
        )
    })?;
    if current_canonical != snapshot_canonical {
        return Err(ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_MISMATCH",
            "Snapshot belongs to a different workspace",
        ));
    }

    start_reserved_restore(
        state.db,
        session_id,
        current.working_dir,
        snapshot,
        reservation,
    )
    .await
    .map_err(|error| {
        tracing::error!(%error, "snapshot restore task failed");
        ApiError::internal()
    })?
}

// SQLite writes use spawn_blocking and may outlive a cancelled HTTP request.
// The owned task retains the query reservation until that write actually ends.
fn start_reserved_restore(
    db: zk_db::Db,
    session_id: String,
    working_dir: String,
    snapshot: SessionSnapshot,
    reservation: Option<impl Send + 'static>,
) -> tokio::task::JoinHandle<Result<Json<SessionSnapshotSummary>, ApiError>> {
    tokio::spawn(async move {
        let _reservation = reservation;
        let model = snapshot.model.as_deref().ok_or_else(|| {
            ApiError::validation_with_code("SNAPSHOT_MODEL_MISSING", "Snapshot has no model")
        })?;
        let status = metadata_string(&snapshot, "status").unwrap_or("active");
        let title = snapshot.metadata.get("title").and_then(Value::as_str);
        match db
            .restore_session_snapshot(
                &session_id,
                &working_dir,
                model,
                status,
                title,
                snapshot.messages.clone(),
            )
            .await?
        {
            SnapshotRestoreOutcome::Applied => Ok(Json(snapshot.summary())),
            SnapshotRestoreOutcome::NotFound => Err(ApiError::session_not_found(&session_id)),
            SnapshotRestoreOutcome::WorkspaceMismatch => Err(ApiError::validation_with_code(
                "SNAPSHOT_WORKSPACE_MISMATCH",
                "Snapshot belongs to a different workspace",
            )),
            SnapshotRestoreOutcome::HistoryConflict => Err(ApiError {
                status: axum::http::StatusCode::CONFLICT,
                code: "SNAPSHOT_HISTORY_CONFLICT".into(),
                message:
                    "Snapshot history conflicts with the current session; no changes were applied"
                        .into(),
            }),
            SnapshotRestoreOutcome::InvalidMessages => Err(ApiError::validation_with_code(
                "SNAPSHOT_MESSAGES_INVALID",
                "Snapshot messages are invalid; no changes were applied",
            )),
        }
    })
}

/// `DELETE /api/sessions/snapshots/{sessionId}`.
pub(crate) async fn delete(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let deleted = state
        .session_snapshots
        .delete_snapshot(&session_id)
        .await
        .map_err(snapshot_error)?;
    if !deleted {
        return Err(snapshot_not_found(&session_id));
    }
    Ok(Json(json!({ "sessionId": session_id, "deleted": true })))
}

fn metadata_string<'a>(snapshot: &'a SessionSnapshot, key: &str) -> Option<&'a str> {
    snapshot.metadata.get(key).and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use zk_db::{MessageRole, NewMessage, StoredBlock};

    use super::*;

    struct Fixture {
        root: PathBuf,
        state: AppState,
        session_id: String,
        snapshot: SessionSnapshot,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    async fn fixture() -> Fixture {
        let root =
            std::env::temp_dir().join(format!("zkcode-reserved-snapshot-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let mut config = crate::config::Config::test_config();
        config.snapshot_dir = Some(root.join("snapshots"));
        config.workspace_default_root = root.to_string_lossy().into_owned();
        config.workspace_allowed_roots = vec![root.clone()];
        config.scratchpad_system_root = root.join("scratchpad");
        config.mcp_registry_path = root.join("mcp.json");
        config.python_uds_path = root.join("disabled-python.sock");
        let state = AppState::new(zk_db::Db::open_in_memory().unwrap(), config);
        let session = state
            .db
            .create_session("snapshot-model", &root.to_string_lossy())
            .await
            .unwrap();
        for text in ["saved", "unsaved"] {
            state
                .db
                .append_message(
                    &session.id,
                    NewMessage {
                        meta: None,
                        role: MessageRole::User,
                        content: vec![StoredBlock::Text { text: text.into() }],
                        stop_reason: None,
                        input_tokens: 0,
                        output_tokens: 0,
                    },
                )
                .await
                .unwrap();
        }
        let mut detail = state.db.get_session(&session.id).await.unwrap().unwrap();
        detail.messages.truncate(1);
        let snapshot = SessionSnapshot::from_session_detail(&detail);
        state
            .db
            .update_session_model(&session.id, "current-model")
            .await
            .unwrap();
        let _engine = crate::engine_bridge::wire_engine(&state);
        Fixture {
            root,
            state,
            session_id: session.id,
            snapshot,
        }
    }

    // This wrapper only observes release; the reservation itself is the real
    // ConversationService/Engine guard used by query admission.
    struct ObservedReservation<T> {
        guard: Option<T>,
        released: Option<oneshot::Sender<()>>,
    }

    impl<T> Drop for ObservedReservation<T> {
        fn drop(&mut self) {
            drop(self.guard.take());
            if let Some(released) = self.released.take() {
                let _ = released.send(());
            }
        }
    }

    async fn hold_writer(
        db: &zk_db::Db,
    ) -> (
        mpsc::Sender<()>,
        tokio::task::JoinHandle<Result<(), zk_db::DbError>>,
    ) {
        let (entered, acquired) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let db = db.clone();
        let holder = tokio::spawn(async move {
            db.with_writer(move |_| {
                entered.send(()).unwrap();
                // The sender is dropped on assertion failure, so the blocking
                // writer cannot strand the test runtime during unwinding.
                let _ = wait.recv();
                Ok(())
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(10), acquired)
            .await
            .unwrap()
            .unwrap();
        (release, holder)
    }

    #[tokio::test]
    async fn dropped_restore_waiter_retains_query_slot_until_writer_completion() {
        let fixture = fixture().await;
        let conversation = fixture.state.conversation().unwrap();
        let guard = conversation
            .try_reserve_session_mutation(&fixture.session_id)
            .unwrap();
        let (released, finished) = oneshot::channel();
        let (release_writer, holder) = hold_writer(&fixture.state.db).await;
        let restoring = start_reserved_restore(
            fixture.state.db.clone(),
            fixture.session_id.clone(),
            fixture.root.to_string_lossy().into_owned(),
            fixture.snapshot.clone(),
            Some(ObservedReservation {
                guard: Some(guard),
                released: Some(released),
            }),
        );

        // Cancelling the HTTP future drops this JoinHandle. The writer is
        // definitely occupied, so the restore cannot already have committed.
        drop(restoring);
        assert!(conversation.reserve(&fixture.session_id).is_none());
        assert!(conversation.reserve("unrelated-session").is_some());
        release_writer.send(()).unwrap();
        holder.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(10), finished)
            .await
            .unwrap()
            .unwrap();

        assert!(conversation.reserve(&fixture.session_id).is_some());
        let restored = fixture
            .state
            .db
            .get_session(&fixture.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.model, "snapshot-model");
        assert_eq!(restored.messages.len(), 1);
        assert_eq!(restored.messages[0].id, fixture.snapshot.messages[0].id);
    }

    #[tokio::test]
    async fn rejected_restore_releases_query_slot_after_writer_completion() {
        let fixture = fixture().await;
        fixture
            .state
            .db
            .start_run(
                "snapshot-blocking-run",
                &fixture.session_id,
                None,
                None,
                "current-model",
            )
            .await
            .unwrap();
        let conversation = fixture.state.conversation().unwrap();
        let guard = conversation
            .try_reserve_session_mutation(&fixture.session_id)
            .unwrap();
        let (release_writer, holder) = hold_writer(&fixture.state.db).await;
        let restoring = start_reserved_restore(
            fixture.state.db.clone(),
            fixture.session_id.clone(),
            fixture.root.to_string_lossy().into_owned(),
            fixture.snapshot.clone(),
            Some(guard),
        );
        assert!(conversation.reserve(&fixture.session_id).is_none());
        release_writer.send(()).unwrap();
        holder.await.unwrap().unwrap();
        let error = tokio::time::timeout(Duration::from_secs(10), restoring)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert_eq!(error.status, axum::http::StatusCode::CONFLICT);
        assert!(conversation.reserve(&fixture.session_id).is_some());
        let unchanged = fixture
            .state
            .db
            .get_session(&fixture.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.model, "current-model");
        assert_eq!(unchanged.messages.len(), 2);
        assert_eq!(
            fixture
                .state
                .db
                .find_run_by_id("snapshot-blocking-run")
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
    }
}
