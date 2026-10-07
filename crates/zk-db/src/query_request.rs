//! Content-free request tombstones prevent transport retries from repeating effects.

use crate::{Db, DbError};
use rusqlite::{OptionalExtension, params};

impl Db {
    /// Claim before creating a Session, running Hooks or accepting user content.
    /// A crashed process leaves a tombstone, never authority to replay its query.
    ///
    /// # Errors
    /// Reused/cancelled identities and database failures are explicit failures.
    pub async fn claim_query_request(&self, id: &str) -> Result<(), DbError> {
        let id = uuid::Uuid::parse_str(id)
            .map_err(|_| DbError::Validation("QUERY_REQUEST_ID_INVALID".into()))?
            .to_string();
        self.with_writer(move |conn| {
            let inserted=conn.execute("INSERT INTO query_requests(request_id,status,created_at_ms) VALUES(?1,'claimed',?2) ON CONFLICT(request_id) DO NOTHING",
                params![id,crate::time::now_millis()])?;
            if inserted==1 {return Ok(());}
            let status:String=conn.query_row("SELECT status FROM query_requests WHERE request_id=?1",[id],|row|row.get(0))?;
            Err(DbError::Conflict(if status=="cancelled" {"QUERY_REQUEST_CANCELLED"}else{"QUERY_REQUEST_ALREADY_ACCEPTED"}.into()))
        }).await
    }

    /// Save an early stop even when the corresponding POST has not arrived.
    /// This records intent only; resource cleanup remains owned by `TaskRuntime`.
    ///
    /// # Errors
    /// Invalid identities and persistence failures propagate.
    pub async fn cancel_query_request(&self, id: &str) -> Result<(), DbError> {
        let id = uuid::Uuid::parse_str(id)
            .map_err(|_| DbError::Validation("QUERY_REQUEST_ID_INVALID".into()))?
            .to_string();
        self.with_writer(move |conn| {
            conn.execute("INSERT INTO query_requests(request_id,status,created_at_ms) VALUES(?1,'cancelled',?2)
                ON CONFLICT(request_id) DO UPDATE SET status='cancelled'",params![id,crate::time::now_millis()])?;
            Ok(())
        }).await
    }

    /// Bind the claimed identity to its Session once and check cancellation in
    /// the same write. No prompt, hash, configuration or credential is recorded.
    ///
    /// # Errors
    /// Missing claims, rebinding and concurrent cancellation fail closed.
    pub async fn bind_query_request(&self, id: &str, session: &str) -> Result<(), DbError> {
        let (id, session) = (id.to_owned(), session.to_owned());
        self.with_writer(move |conn| {
            let current: Option<(String, Option<String>)> = conn
                .query_row(
                    "SELECT status,session_id FROM query_requests WHERE request_id=?1",
                    [&id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            match current {
                Some((status, _)) if status == "cancelled" => {
                    return Err(DbError::Conflict("QUERY_REQUEST_CANCELLED".into()));
                }
                Some((_, Some(bound))) if bound != session => {
                    return Err(DbError::Conflict("QUERY_REQUEST_OWNER_CONFLICT".into()));
                }
                None => return Err(DbError::Conflict("QUERY_REQUEST_NOT_RESERVED".into())),
                _ => {}
            }
            conn.execute(
                "UPDATE query_requests SET session_id=?1 WHERE request_id=?2",
                params![session, id],
            )?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn claims_survive_restart_and_early_cancel_fences_binding() {
        let path =
            std::env::temp_dir().join(format!("zk-query-claims-{}.sqlite", uuid::Uuid::new_v4()));
        let db = Db::open(&path).unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        db.claim_query_request(&id).await.unwrap();
        assert!(db.claim_query_request(&id).await.is_err());
        let session = db.create_session("fixture", "/tmp").await.unwrap();
        db.bind_query_request(&id, &session.id).await.unwrap();
        db.cancel_query_request(&id).await.unwrap();
        assert!(db.bind_query_request(&id, &session.id).await.is_err());
        drop(db);
        let db = Db::open(&path).unwrap();
        assert!(db.claim_query_request(&id).await.is_err());
        let early = uuid::Uuid::new_v4().to_string();
        db.cancel_query_request(&early).await.unwrap();
        assert!(db.claim_query_request(&early).await.is_err());
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}
