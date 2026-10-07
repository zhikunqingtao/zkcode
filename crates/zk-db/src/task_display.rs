//! The narrow child self-report boundary; display data never mutates execution facts.
use crate::{
    Db, DbError, RuntimeTaskRecord,
    task_runtime::{RUNTIME_TASK_COLUMNS, map_runtime_task},
};
use rusqlite::{OptionalExtension, params};

impl Db {
    /// Replace only a child task's display note, bound to the caller's current Run.
    /// # Errors
    /// Rejects root/sibling tasks, stale Runs, missing identity and storage failures.
    pub async fn update_own_task_display(
        &self,
        caller_session: &str,
        caller_run: &str,
        requested_task: Option<&str>,
        output: String,
    ) -> Result<RuntimeTaskRecord, DbError> {
        let (session, run, requested) = (
            caller_session.to_owned(),
            caller_run.to_owned(),
            requested_task.map(str::to_owned),
        );
        self.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let owned: Option<String> = tx.query_row(
                "SELECT t.id FROM tasks t JOIN run_envelopes r ON r.task_id=t.id WHERE r.id=?1 AND r.session_id=?2 AND t.current_run_id=r.id AND t.parent_task_id IS NOT NULL",
                (&run, &session), |row| row.get(0),
            ).optional()?;
            let Some(task) = owned.filter(|id| requested.as_ref().is_none_or(|requested| requested == id)) else {
                return Err(DbError::Validation("TASK_SELF_OUTPUT_ACCESS_DENIED".into()));
            };
            let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
            let output=crate::content::store_text(&tx,&session,&output)?;
            tx.execute("UPDATE tasks SET display_output=?1,updated_at=?2,version=version+1 WHERE id=?3", params![output, now, task])?;
            let record = tx.query_row(&format!("SELECT {RUNTIME_TASK_COLUMNS} FROM tasks WHERE id=?1"), [&task], |row|map_runtime_task(&tx,row))?;
            tx.commit()?;
            Ok(record)
        }).await
    }
}
