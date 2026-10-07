//! Transactional user-wide skill preferences, independent of discovery.
use crate::{Db, DbError};
use std::collections::HashMap;

impl Db {
    /// Synchronous narrow lookup for the synchronous authorization mode port.
    /// # Errors
    /// Missing sessions and unreadable state must never retain cached authority.
    pub fn session_permission_mode(&self, session_id: &str) -> Result<Option<String>, DbError> {
        use rusqlite::OptionalExtension;
        let conn = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        conn.query_row(
            "SELECT permission_mode FROM sessions WHERE id=?1",
            [session_id],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()?
        .ok_or_else(|| DbError::SessionNotFound(session_id.to_owned()))
    }
    /// Load persisted session permissions before accepting any request.
    /// # Errors
    /// Propagates unreadable authoritative state.
    pub fn permission_modes_at_startup(&self) -> Result<HashMap<String, String>, DbError> {
        let conn = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = conn
            .prepare("SELECT id,permission_mode FROM sessions WHERE permission_mode IS NOT NULL")?;
        Ok(stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?)
    }

    /// Save a permission transition without changing message ordering.
    /// # Errors
    /// Missing sessions and persistence failures prevent mode publication.
    pub async fn set_session_permission_mode(
        &self,
        session_id: String,
        mode: String,
    ) -> Result<(), DbError> {
        self.with_writer(move |conn| {
            if conn.execute(
                "UPDATE sessions SET permission_mode=?1 WHERE id=?2",
                rusqlite::params![mode, session_id],
            )? == 0
            {
                return Err(DbError::SessionNotFound(session_id));
            }
            Ok(())
        })
        .await
    }
    /// Load preferences during synchronous server assembly, before requests.
    /// # Errors
    /// Corrupt/unreadable state must not be interpreted as all skills enabled.
    pub fn skill_states_at_startup(&self) -> Result<HashMap<String, bool>, DbError> {
        let conn = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = conn.prepare("SELECT name,enabled FROM skill_states")?;
        Ok(stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?)
    }

    /// Persist a switch before publishing it to the registry cache.
    /// # Errors
    /// A failed transaction leaves the last committed preference intact.
    pub async fn set_skill_enabled(&self, name: String, enabled: bool) -> Result<(), DbError> {
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            tx.execute("INSERT INTO skill_states(name,enabled,updated_at) VALUES(?1,?2,strftime('%Y-%m-%dT%H:%M:%fZ','now')) ON CONFLICT(name) DO UPDATE SET enabled=excluded.enabled,updated_at=excluded.updated_at", rusqlite::params![name, enabled])?;
            tx.commit()?;
            Ok(())
        }).await
    }
}
