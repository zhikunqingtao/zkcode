//! Preflight merge writes on the database filesystem, including buffered WAL growth.
use crate::DbError;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

const MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;
// Account conservatively for the database and WAL copies plus row/index pages.
const WRITE_OVERHEAD: u64 = 64 * 1024;

pub(crate) struct MergeWriteBudget {
    directory: Option<PathBuf>,
    initial_available: Option<u64>,
    reserved: u64,
}

impl MergeWriteBudget {
    pub(crate) fn new(conn: &Connection) -> Self {
        Self {
            directory: conn.path().filter(|p| !p.is_empty()).map(|p| {
                Path::new(p)
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."))
                    .to_owned()
            }),
            initial_available: None,
            reserved: 0,
        }
    }

    pub(crate) fn reserve(&mut self, payload: usize) -> Result<(), DbError> {
        self.reserve_with(payload, available_bytes)
    }

    fn reserve_with(
        &mut self,
        payload: usize,
        available: impl FnOnce(&Path) -> Result<u64, DbError>,
    ) -> Result<(), DbError> {
        let Some(directory) = &self.directory else {
            // In-memory SQLite has no backing filesystem and must not depend on host space.
            return Ok(());
        };
        let current = available(directory)?;
        let initial = *self.initial_available.get_or_insert(current);
        let bytes = u64::try_from(payload)
            .ok()
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(WRITE_OVERHEAD))
            .ok_or_else(|| DbError::Invalid("MERGE_DISK_SPACE_LOW".into()))?;
        // SQLite may buffer a transaction's earlier writes in memory; do not reuse
        // their apparent free space. Also observe unrelated writers' latest usage.
        let available = current.min(initial.saturating_sub(self.reserved));
        if available
            .checked_sub(bytes)
            .is_none_or(|left| left < MIN_FREE_BYTES)
        {
            return Err(DbError::Invalid("MERGE_DISK_SPACE_LOW".into()));
        }
        self.reserved = self
            .reserved
            .checked_add(bytes)
            .ok_or_else(|| DbError::Invalid("MERGE_DISK_SPACE_LOW".into()))?;
        Ok(())
    }
}

#[cfg(unix)]
fn available_bytes(directory: &Path) -> Result<u64, DbError> {
    let stat = nix::sys::statvfs::statvfs(directory)
        .map_err(|error| DbError::Invalid(format!("MERGE_DISK_SPACE_CHECK_FAILED: {error}")))?;
    u64::try_from(u128::from(stat.blocks_available()) * u128::from(stat.fragment_size()))
        .map_err(|_| DbError::Invalid("MERGE_DISK_SPACE_CHECK_FAILED: capacity overflow".into()))
}

#[cfg(not(unix))]
fn available_bytes(_directory: &Path) -> Result<u64, DbError> {
    Err(DbError::Invalid(
        "MERGE_DISK_SPACE_CHECK_FAILED: unsupported filesystem capacity API".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn on_disk() -> MergeWriteBudget {
        MergeWriteBudget {
            directory: Some(PathBuf::from("/not-the-host-filesystem")),
            initial_available: None,
            reserved: 0,
        }
    }

    #[test]
    fn reserve_counts_buffered_writes_and_live_space_without_spending_the_floor() {
        let mut budget = on_disk();
        let available = MIN_FREE_BYTES + 2 * WRITE_OVERHEAD + 4;
        budget.reserve_with(1, |_| Ok(available)).unwrap();
        budget.reserve_with(1, |_| Ok(available)).unwrap();
        assert!(
            budget
                .reserve_with(0, |_| Ok(available))
                .unwrap_err()
                .to_string()
                .contains("MERGE_DISK_SPACE_LOW")
        );
        let mut live = on_disk();
        live.reserve_with(0, |_| Ok(u64::MAX)).unwrap();
        assert!(live.reserve_with(0, |_| Ok(MIN_FREE_BYTES)).is_err());
        assert!(
            on_disk()
                .reserve_with(usize::MAX, |_| Ok(u64::MAX))
                .is_err()
        );
    }

    #[test]
    fn memory_skips_the_host_probe_and_disk_probe_failures_are_not_success() {
        let conn = Connection::open_in_memory().unwrap();
        MergeWriteBudget::new(&conn)
            .reserve_with(usize::MAX, |_| panic!("must not probe host"))
            .unwrap();
        assert!(
            on_disk()
                .reserve(0)
                .unwrap_err()
                .to_string()
                .contains("MERGE_DISK_SPACE_CHECK_FAILED")
        );
    }

    #[test]
    fn rejected_transaction_does_not_leave_a_partial_snapshot() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE snapshots(value TEXT);")
            .unwrap();
        {
            let tx = conn.transaction().unwrap();
            tx.execute("INSERT INTO snapshots VALUES('first')", [])
                .unwrap();
            assert!(on_disk().reserve_with(100, |_| Ok(MIN_FREE_BYTES)).is_err());
        }
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM snapshots", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[cfg(unix)]
    #[test]
    fn real_probe_uses_the_database_parent_directory() {
        let dir = std::env::temp_dir().join(format!("zk-merge-space-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let conn = Connection::open(dir.join("data.sqlite")).unwrap();
        let budget = MergeWriteBudget::new(&conn);
        assert_eq!(budget.directory, Some(std::fs::canonicalize(&dir).unwrap()));
        available_bytes(budget.directory.as_deref().unwrap()).unwrap();
        drop(conn);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
