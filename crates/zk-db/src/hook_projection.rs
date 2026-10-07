//! Optional presentation notes are separate from immutable execution facts.
use crate::{Db, DbError};
use rusqlite::params;
use serde::Serialize;

/// A trusted UI-only projection of a completed physical tool invocation.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HookPresentation {
    /// Original run; tool-use identities are only unique within this scope.
    pub run_id: String,
    /// Original tool call identity.
    pub tool_use_id: String,
    /// Exact assistant message UUID for restored history; absent if unavailable.
    pub assistant_message_id: Option<String>,
    /// Bounded plain text, never tool output or execution evidence.
    pub text: String,
    /// Stable pagination identity.
    pub sequence: i64,
}

impl Db {
    /// Save only after the corresponding tool result has durably terminated.
    ///
    /// # Errors
    /// Invalid text, ownership mismatches or storage failures reject the projection.
    pub async fn save_hook_presentation(
        &self,
        session: &str,
        run: &str,
        tool_use: &str,
        text: &str,
    ) -> Result<(), DbError> {
        if text.chars().count() > 2000 {
            return Err(DbError::Validation("HOOK_PRESENTATION_TOO_LARGE".into()));
        }
        let (session, run, tool_use, text) = (
            session.to_owned(),
            run.to_owned(),
            tool_use.to_owned(),
            text.to_owned(),
        );
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let invocation: String = tx.query_row("SELECT i.invocation_id FROM tool_invocations i JOIN run_envelopes r ON r.id=i.run_id WHERE r.session_id=?1 AND i.run_id=?2 AND i.tool_use_id=?3 AND i.status IN ('succeeded','failed','cancelled','interrupted')", params![session,run,tool_use], |row|row.get(0))?;
            let text = crate::content::store_text(&tx, &session, &text)?;
            tx.execute("INSERT INTO hook_result_presentations(invocation_id,session_id,run_id,tool_use_id,text) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(invocation_id) DO UPDATE SET text=excluded.text", params![invocation,session,run,tool_use,text])?;
            tx.commit()?;
            Ok(())
        }).await
    }

    /// Read a bounded page in the explicitly authorized conversation scope.
    ///
    /// # Errors
    /// Invalid cursors, unavailable ephemeral bodies and storage failures propagate.
    pub async fn hook_presentations(
        &self,
        session: &str,
        after: i64,
        limit: u32,
    ) -> Result<Vec<HookPresentation>, DbError> {
        if after < 0 || !(1..=200).contains(&limit) {
            return Err(DbError::Validation("HOOK_PRESENTATION_PAGE_INVALID".into()));
        }
        let session = session.to_owned();
        self.with_reader(move |conn| {
            let mut query = conn.prepare("SELECT run_id,tool_use_id,text,sequence FROM hook_result_presentations WHERE session_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3")?;
            query.query_map(params![session,after,limit], |row| Ok(HookPresentation {
                run_id:row.get(0)?, tool_use_id:row.get(1)?,
                assistant_message_id: assistant_message(conn, &session, &row.get::<_, String>(0)?, &row.get::<_, String>(1)?)?,
                text:crate::content::load_row_text(conn, &session, row.get(2)?)?, sequence:row.get(3)?,
            }))?.collect::<Result<Vec<_>,_>>().map_err(Into::into)
        }).await
    }
}

fn assistant_message(
    conn: &rusqlite::Connection,
    session: &str,
    run: &str,
    tool_use: &str,
) -> rusqlite::Result<Option<String>> {
    let mut query=conn.prepare("SELECT id,content_json FROM messages WHERE session_id=?1 AND run_id=?2 AND role='assistant' ORDER BY seq_num DESC")?;
    let mut rows = query.query(params![session, run])?;
    while let Some(row) = rows.next()? {
        let content = crate::content::load_row_text(conn, session, row.get(1)?)?;
        let blocks: Vec<crate::StoredBlock> = serde_json::from_str(&content).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        if blocks
            .iter()
            .any(|block| matches!(block,crate::StoredBlock::ToolUse{id,..} if id==tool_use))
        {
            return row.get(0).map(Some);
        }
    }
    Ok(None)
}
