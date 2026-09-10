//! Composition adapter from the `Memory` tool port to authoritative `SQLite`.

use futures::future::BoxFuture;
use zk_db::{MemoryTarget as DbMemoryTarget, MemoryUpsert};
use zk_tools::{MemoryScope, MemoryStore, MemoryTarget};

/// `SQLite` implementation used by the production tool registry.
#[derive(Clone)]
pub(crate) struct DbMemoryStore {
    db: zk_db::Db,
}

impl DbMemoryStore {
    /// Bind the adapter to the application's single database handle.
    #[must_use]
    pub(crate) const fn new(db: zk_db::Db) -> Self {
        Self { db }
    }
}

impl MemoryStore for DbMemoryStore {
    fn read_memories(&self, target: MemoryTarget) -> BoxFuture<'_, Result<String, String>> {
        Box::pin(async move {
            let target = db_target(&target)?;
            let rows = self
                .db
                .list_memories(target)
                .await
                .map_err(|error| error.to_string())?;
            Ok(rows
                .into_iter()
                .map(|row| format!("[{}] {}\n{}", row.category, row.title, row.content))
                .collect::<Vec<_>>()
                .join("\n\n"))
        })
    }

    fn write_tool_memory(
        &self,
        target: MemoryTarget,
        content: String,
    ) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let target = db_target(&target)?;
            let title = content
                .lines()
                .find(|line| !line.trim().is_empty())
                .map_or("Memory", str::trim)
                .chars()
                .take(80)
                .collect();
            self.db
                .create_memory(
                    target,
                    MemoryUpsert {
                        id: None,
                        category: "SEMANTIC".to_owned(),
                        title,
                        content,
                        keywords: None,
                        source: Some("TOOL".to_owned()),
                    },
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }

    fn delete_memory(
        &self,
        target: MemoryTarget,
        pattern: String,
    ) -> BoxFuture<'_, Result<bool, String>> {
        Box::pin(async move {
            let target = db_target(&target)?;
            self.db
                .delete_memories_matching(target, &pattern)
                .await
                .map(|deleted| deleted > 0)
                .map_err(|error| error.to_string())
        })
    }
}

fn db_target(target: &MemoryTarget) -> Result<DbMemoryTarget, String> {
    match target.scope() {
        MemoryScope::Global => Ok(DbMemoryTarget::global()),
        MemoryScope::Project => {
            let path = target
                .project_path()
                .ok_or_else(|| "project memory target is missing a project path".to_owned())?;
            let path = path
                .to_str()
                .ok_or_else(|| "project memory path is not valid UTF-8".to_owned())?;
            DbMemoryTarget::project(path).map_err(|error| error.to_string())
        }
    }
}
