//! Content retention is chosen before accepting a session's first input.
//!
//! Execution identities, ownership and accounting are durable for both policies.
//! Ephemeral bodies belong to a bounded memory store and may never fall back to disk.

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::{Db, DbError};

mod memory;
pub use memory::{ContentRef, EphemeralContentLease, MemoryContentStore};

use std::sync::Arc;

/// Register explicit content codecs on every connection, including readers.
/// `SQLite` sees only opaque references for temporary bodies; the callbacks never
/// write files, log input or substitute an empty value when a scope has expired.
pub(crate) fn install(conn: &Connection, store: &Arc<MemoryContentStore>) -> Result<(), DbError> {
    let flags = rusqlite::functions::FunctionFlags::SQLITE_UTF8;
    let memory = Arc::clone(store);
    conn.create_scalar_function("zk_ephemeral_put", 2, flags, move |ctx| {
        let session = ctx.get::<String>(0)?;
        let text = ctx.get::<Option<String>>(1)?;
        text.map(|text| {
            memory
                .put(&session, text.as_bytes())
                .and_then(|reference| Ok(serde_json::to_string(&reference)?))
                .map_err(codec_error)
        })
        .transpose()
    })?;
    let memory = Arc::clone(store);
    conn.create_scalar_function("zk_ephemeral_get", 2, flags, move |ctx| {
        let session = ctx.get::<String>(0)?;
        let reference = ctx.get::<Option<String>>(1)?;
        reference
            .map(|reference| {
                let reference = serde_json::from_str::<ContentRef>(&reference).map_err(|_| {
                    codec_error(DbError::Invalid(
                        "EPHEMERAL_CONTENT_REFERENCE_INVALID".into(),
                    ))
                })?;
                let bytes = memory.get(&session, &reference).map_err(codec_error)?;
                String::from_utf8(bytes.to_vec()).map_err(|_| {
                    codec_error(DbError::Invalid(
                        "EPHEMERAL_CONTENT_ENCODING_INVALID".into(),
                    ))
                })
            })
            .transpose()
    })?;
    let memory = Arc::clone(store);
    conn.create_scalar_function("zk_ephemeral_attach", 2, flags, move |ctx| {
        memory
            .attach(&ctx.get::<String>(0)?, &ctx.get::<String>(1)?)
            .map_err(codec_error)?;
        Ok(true)
    })?;
    let memory = Arc::clone(store);
    conn.create_scalar_function("zk_ephemeral_ref_valid", 2, flags, move |ctx| {
        let session = ctx.get::<String>(0)?;
        let raw = ctx.get::<Option<String>>(1)?;
        Ok(raw.is_none_or(|raw| {
            serde_json::from_str::<ContentRef>(&raw)
                .is_ok_and(|reference| memory.get(&session, &reference).is_ok())
        }))
    })?;
    let memory = Arc::clone(store);
    conn.create_scalar_function("zk_ephemeral_diagnostic_valid", 2, flags, move |ctx| {
        let session = ctx.get::<String>(0)?;
        let raw = ctx.get::<Option<String>>(1)?;
        Ok(raw.is_none_or(|raw| {
            raw == UNAVAILABLE_DIAGNOSTIC
                || serde_json::from_str::<ContentRef>(&raw)
                    .is_ok_and(|reference| memory.get(&session, &reference).is_ok())
        }))
    })?;
    let memory = Arc::clone(store);
    conn.create_scalar_function("zk_ephemeral_reason_valid", 2, flags, move |ctx| {
        let session = ctx.get::<String>(0)?;
        let raw = ctx.get::<Option<String>>(1)?;
        Ok(raw.is_none_or(|raw| {
            is_lifecycle_reason(&raw)
                || raw == UNAVAILABLE_DIAGNOSTIC
                || serde_json::from_str::<ContentRef>(&raw)
                    .is_ok_and(|reference| memory.get(&session, &reference).is_ok())
        }))
    })?;
    Ok(())
}

fn codec_error(error: DbError) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(error))
}

/// Content-free marker used only in diagnostic fields, never conversation bodies.
pub const UNAVAILABLE_DIAGNOSTIC: &str = r#"{"$zkEphemeralUnavailable":true}"#;

fn lifetime_unavailable(error: &DbError) -> bool {
    match error {
        DbError::Invalid(code) | DbError::Validation(code) => matches!(
            code.as_str(),
            "EPHEMERAL_CONTENT_EXPIRED" | "EPHEMERAL_CONTENT_CAPACITY"
        ),
        DbError::Sqlite(rusqlite::Error::UserFunctionError(inner)) => inner
            .downcast_ref::<DbError>()
            .is_some_and(lifetime_unavailable),
        // SQLite scalar callbacks flatten their Rust error into SQLITE_ERROR.
        // Match only our two exact content-free codes, never arbitrary messages.
        DbError::Sqlite(rusqlite::Error::SqliteFailure(code, Some(message)))
            if code.extended_code == rusqlite::ffi::SQLITE_ERROR =>
        {
            matches!(
                message.as_str(),
                "invalid state: EPHEMERAL_CONTENT_EXPIRED"
                    | "invalid input: EPHEMERAL_CONTENT_CAPACITY"
            )
        }
        _ => false,
    }
}

/// Store optional diagnostics without making cancellation or usage settlement
/// depend on the lifetime/capacity of their explanatory text. Other failures,
/// including database and reference ownership failures, still propagate.
///
/// # Errors
/// Unknown owners and database failures are not downgraded.
pub fn store_diagnostic(
    conn: &Connection,
    session: &str,
    value: Option<&str>,
) -> Result<Option<String>, DbError> {
    value
        .map(|value| match store_text(conn, session, value) {
            Ok(stored) => Ok(stored),
            Err(error) if lifetime_unavailable(&error) => Ok(UNAVAILABLE_DIAGNOSTIC.to_owned()),
            Err(error) => Err(error),
        })
        .transpose()
}

/// Decode diagnostic text while keeping durable status and cost readable after
/// an ephemeral scope ends. An explicit unavailable code replaces expired text;
/// this helper must not be used for messages, inputs, results or evidence.
///
/// # Errors
/// Corrupt and foreign references remain errors.
pub fn load_diagnostic(
    conn: &Connection,
    session: &str,
    value: Option<String>,
) -> rusqlite::Result<Option<String>> {
    value
        .map(|value| {
            if session_retention(conn, session).map_err(codec_error)? == ContentRetention::Ephemeral
                && value == UNAVAILABLE_DIAGNOSTIC
            {
                return Ok("EPHEMERAL_CONTENT_UNAVAILABLE".to_owned());
            }
            match load_text(conn, session, &value) {
                Ok(text) => Ok(text),
                Err(error) if lifetime_unavailable(&error) => {
                    Ok("EPHEMERAL_CONTENT_UNAVAILABLE".to_owned())
                }
                Err(error) => Err(codec_error(error)),
            }
        })
        .transpose()
}

fn is_lifecycle_reason(value: &str) -> bool {
    crate::task_runtime::ExitReason::parse(value).is_ok()
        || matches!(
            value,
            "attachedChildrenPending" | "attachedChildrenResolved"
        )
}

/// Decode a reason column that also accepts a closed set of lifecycle codes.
///
/// # Errors
/// Free-form diagnostics follow the same ownership checks as [`load_diagnostic`].
pub fn load_reason(
    conn: &Connection,
    session: &str,
    value: Option<String>,
) -> rusqlite::Result<Option<String>> {
    if value.as_deref().is_some_and(is_lifecycle_reason) {
        Ok(value)
    } else {
        load_diagnostic(conn, session, value)
    }
}

/// Resolve a Run's content owner in the current `SQLite` snapshot.
///
/// # Errors
/// A missing Run has no authority to allocate or read conversation content.
pub fn run_session(conn: &Connection, run: &str) -> rusqlite::Result<String> {
    conn.query_row(
        "SELECT session_id FROM run_envelopes WHERE id=?1",
        [run],
        |row| row.get(0),
    )
}

/// Resolve a logical Task's root content owner.
///
/// # Errors
/// Missing tasks are rejected.
pub fn task_session(conn: &Connection, task: &str) -> rusqlite::Result<String> {
    conn.query_row("SELECT session_id FROM tasks WHERE id=?1", [task], |row| {
        row.get(0)
    })
}

/// Encode a Run-owned field without accepting a caller-selected content scope.
///
/// # Errors
/// Missing owners, expired scopes and capacity limits fail closed.
pub fn store_run_text(conn: &Connection, run: &str, value: &str) -> Result<String, DbError> {
    store_text(conn, &run_session(conn, run)?, value)
}

/// Decode a Run-owned field for live execution or event replay.
///
/// # Errors
/// Expired ephemeral replay is explicitly unavailable.
pub fn load_run_text(conn: &Connection, run: &str, value: String) -> rusqlite::Result<String> {
    let session = run_session(conn, run)?;
    if value == UNAVAILABLE_DIAGNOSTIC
        && session_retention(conn, &session).map_err(codec_error)? == ContentRetention::Ephemeral
    {
        return Err(codec_error(DbError::Invalid(
            "EPHEMERAL_CONTENT_UNAVAILABLE".into(),
        )));
    }
    load_row_text(conn, &session, value)
}

/// Seal a text field according to the immutable policy inside the current write.
///
/// # Errors
/// Unknown or expired scopes and exhausted memory capacity are explicit failures.
pub fn store_text(conn: &Connection, session: &str, value: &str) -> Result<String, DbError> {
    match session_retention(conn, session)? {
        ContentRetention::Persistent => Ok(value.to_owned()),
        ContentRetention::Ephemeral => Ok(conn.query_row(
            "SELECT zk_ephemeral_put(?1,?2)",
            rusqlite::params![session, value],
            |row| row.get(0),
        )?),
    }
}

/// Materialize a text field, failing closed if ephemeral content no longer exists.
///
/// # Errors
/// Missing, expired or foreign content references never become empty history.
pub fn load_text(conn: &Connection, session: &str, value: &str) -> Result<String, DbError> {
    match session_retention(conn, session)? {
        ContentRetention::Persistent => Ok(value.to_owned()),
        ContentRetention::Ephemeral => Ok(conn.query_row(
            "SELECT zk_ephemeral_get(?1,?2)",
            rusqlite::params![session, value],
            |row| row.get(0),
        )?),
    }
}

/// Optional variant used by nullable metadata and summary columns.
///
/// # Errors
/// The same strict policy and capacity checks as [`store_text`] apply.
pub fn store_optional(
    conn: &Connection,
    session: &str,
    value: Option<&str>,
) -> Result<Option<String>, DbError> {
    value
        .map(|value| store_text(conn, session, value))
        .transpose()
}

/// Optional field decoder suitable for repository row mappers.
///
/// # Errors
/// The same strict reference ownership checks as [`load_text`] apply.
pub fn load_optional(
    conn: &Connection,
    session: &str,
    value: Option<String>,
) -> rusqlite::Result<Option<String>> {
    value
        .map(|value| load_row_text(conn, session, value))
        .transpose()
}

/// Non-null field decoder for a repository row mapper.
///
/// # Errors
/// Propagates invalid references as a row error rather than discarding content.
pub fn load_row_text(conn: &Connection, session: &str, value: String) -> rusqlite::Result<String> {
    match session_retention(conn, session).map_err(codec_error)? {
        ContentRetention::Persistent => Ok(value),
        ContentRetention::Ephemeral => conn.query_row(
            "SELECT zk_ephemeral_get(?1,?2)",
            rusqlite::params![session, value],
            |row| row.get(0),
        ),
    }
}

/// Inherit a live parent's body scope when creating an attached transcript.
///
/// # Errors
/// An expired parent cannot create new content-bearing child sessions.
pub fn attach_session(
    conn: &Connection,
    parent: &str,
    session: &str,
) -> Result<ContentRetention, DbError> {
    let retention = session_retention(conn, parent)?;
    if retention == ContentRetention::Ephemeral {
        conn.query_row(
            "SELECT zk_ephemeral_attach(?1,?2)",
            rusqlite::params![parent, session],
            |row| row.get::<_, bool>(0),
        )?;
    }
    Ok(retention)
}

/// Immutable retention policy for a conversation and its attached descendants.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ContentRetention {
    /// Normal persisted conversation content.
    Persistent,
    /// Content exists only for the lifetime of its in-memory execution scope.
    Ephemeral,
}

impl ContentRetention {
    /// Canonical persisted policy name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::Ephemeral => "ephemeral",
        }
    }
}

/// Read the policy inside the caller's existing transaction.
///
/// # Errors
/// Unknown sessions and malformed state are rejected rather than defaulted.
pub fn session_retention(conn: &Connection, session_id: &str) -> Result<ContentRetention, DbError> {
    let value: Option<String> = conn
        .query_row(
            "SELECT content_retention FROM sessions WHERE id=?1",
            [session_id],
            |row| row.get(0),
        )
        .optional()?;
    match value.as_deref() {
        Some("persistent") => Ok(ContentRetention::Persistent),
        Some("ephemeral") => Ok(ContentRetention::Ephemeral),
        None => Err(DbError::SessionNotFound(session_id.to_owned())),
        Some(_) => Err(DbError::Invalid("CONTENT_RETENTION_INVALID".into())),
    }
}

/// Guard operations whose result requires durable conversation content.
///
/// # Errors
/// Ephemeral or unknown sessions cannot be forked, merged or detached.
pub fn require_persistent_session(conn: &Connection, session_id: &str) -> Result<(), DbError> {
    match session_retention(conn, session_id)? {
        ContentRetention::Persistent => Ok(()),
        ContentRetention::Ephemeral => Err(DbError::Validation(
            "EPHEMERAL_OPERATION_UNSUPPORTED".into(),
        )),
    }
}

impl Db {
    /// Share the bounded content store with execution-scoped binary-body adapters.
    #[must_use]
    pub fn memory_content_store(&self) -> Arc<MemoryContentStore> {
        Arc::clone(&self.content)
    }

    /// Create only metadata after reserving the temporary content lifetime.
    /// Retention is immutable from this point onward, including on failed execution.
    ///
    /// # Errors
    /// Identity, storage and scope-capacity errors do not create persistent bodies.
    pub async fn create_ephemeral_session(
        &self,
        model: &str,
        workspace: &str,
        permission_mode: &str,
    ) -> Result<(String, EphemeralContentLease), DbError> {
        let id = uuid::Uuid::new_v4().to_string();
        let lease = self.content.begin(&id)?;
        let (session, model, workspace, mode) = (
            id.clone(),
            model.to_owned(),
            workspace.to_owned(),
            permission_mode.to_owned(),
        );
        self.with_writer(move |conn| {
            let now=crate::time::format_rfc3339_micros(crate::time::now_millis());
            conn.execute("INSERT INTO sessions(id,model,working_dir,permission_mode,content_retention,created_at,updated_at) VALUES(?1,?2,?3,?4,'ephemeral',?5,?5)",
                rusqlite::params![session,model,workspace,mode,now])?;
            Ok(())
        }).await?;
        Ok((id, lease))
    }

    /// Read the immutable content policy without loading any conversation body.
    ///
    /// # Errors
    /// Storage failures and unknown sessions propagate to the caller.
    pub async fn session_retention(&self, session_id: &str) -> Result<ContentRetention, DbError> {
        let id = session_id.to_owned();
        self.with_reader(move |conn| session_retention(conn, &id))
            .await
    }
}
