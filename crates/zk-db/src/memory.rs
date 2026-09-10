//! SQLite-backed long-term memory repository.
//!
//! The `memories` table is the sole authority. Every operation receives an
//! explicit [`MemoryTarget`], which makes project isolation part of the
//! repository contract instead of a convention at HTTP or tool call sites.

use rusqlite::params;

use crate::error::DbError;
use crate::time::{format_rfc3339_micros, now_millis};

/// Supported memory scopes. Project is the product default; callers must
/// construct [`MemoryTarget::global`] explicitly to access global memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryScope {
    /// Memory isolated to one project path.
    Project,
    /// User-wide memory, selected explicitly.
    Global,
}

impl MemoryScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::Global => "global",
        }
    }
}

/// Scope plus its required project identity.
///
/// Fields are private so an invalid combination (`project` without a path or
/// `global` with one) cannot reach a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryTarget {
    scope: MemoryScope,
    project_path: Option<String>,
}

impl MemoryTarget {
    /// Select one project. Blank paths are rejected rather than silently
    /// widening the query to global memory.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Invalid`] when `project_path` is blank.
    pub fn project(project_path: impl Into<String>) -> Result<Self, DbError> {
        let project_path = project_path.into();
        if project_path.trim().is_empty() {
            return Err(DbError::Invalid(
                "project memory requires a non-blank project path".to_owned(),
            ));
        }
        Ok(Self {
            scope: MemoryScope::Project,
            project_path: Some(project_path),
        })
    }

    /// Select user-wide memory. This constructor is intentionally explicit.
    #[must_use]
    pub const fn global() -> Self {
        Self {
            scope: MemoryScope::Global,
            project_path: None,
        }
    }

    /// Selected scope.
    #[must_use]
    pub const fn scope(&self) -> MemoryScope {
        self.scope
    }

    /// Project identity, present only for project-scoped targets.
    #[must_use]
    pub fn project_path(&self) -> Option<&str> {
        self.project_path.as_deref()
    }

    fn into_sql_parts(self) -> (&'static str, Option<String>) {
        (self.scope.as_str(), self.project_path)
    }
}

/// Stored memory row.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRecord {
    /// Stable row id.
    pub id: String,
    /// Free-form category used for presentation and retrieval.
    pub category: String,
    /// Short display title.
    pub title: String,
    /// Memory text.
    pub content: String,
    /// Optional comma-separated retrieval hints.
    pub keywords: Option<String>,
    /// Persisted scope.
    pub scope: MemoryScope,
    /// Project identity for project rows; absent for global rows.
    pub project_path: Option<String>,
    /// Writer provenance such as `USER` or `TOOL`.
    pub source: String,
    /// Server-assigned RFC 3339 creation time.
    pub created_at: String,
    /// Server-assigned RFC 3339 update time.
    pub updated_at: String,
}

/// Full memory payload for create/upsert. Scope is deliberately absent: it is
/// supplied separately as [`MemoryTarget`] and cannot be smuggled in a row.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryUpsert {
    /// Optional caller-supplied id; a UUID is generated when omitted.
    #[serde(default)]
    pub id: Option<String>,
    /// Free-form category.
    pub category: String,
    /// Short display title.
    pub title: String,
    /// Memory text.
    pub content: String,
    /// Optional comma-separated retrieval hints.
    #[serde(default)]
    pub keywords: Option<String>,
    /// Optional provenance; defaults to `USER`.
    #[serde(default)]
    pub source: Option<String>,
}

const DEFAULT_SOURCE: &str = "USER";
const MEMORY_SELECT: &str = "SELECT id, category, title, content, keywords, scope, \
                             project_path, source, created_at, updated_at FROM memories";
const TARGET_PREDICATE: &str =
    "scope = ?1 AND ((?2 IS NULL AND project_path IS NULL) OR project_path = ?2)";

fn map_memory_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryRecord> {
    let raw_scope: String = row.get("scope")?;
    let scope = match raw_scope.as_str() {
        "project" => MemoryScope::Project,
        "global" => MemoryScope::Global,
        other => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid memory scope {other:?}"),
                )),
            ));
        }
    };
    Ok(MemoryRecord {
        id: row.get("id")?,
        category: row.get("category")?,
        title: row.get("title")?,
        content: row.get("content")?,
        keywords: row.get("keywords")?,
        scope,
        project_path: row.get("project_path")?,
        source: row.get("source")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

impl crate::Db {
    /// Insert a memory into exactly one target scope.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when the database write fails.
    pub async fn create_memory(
        &self,
        target: MemoryTarget,
        entry: MemoryUpsert,
    ) -> Result<String, DbError> {
        self.with_writer(move |conn| {
            let (scope, project_path) = target.into_sql_parts();
            let id = entry.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let source = entry.source.unwrap_or_else(|| DEFAULT_SOURCE.to_owned());
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO memories \
                 (id, category, title, content, keywords, scope, project_path, source, \
                  created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
                params![
                    id,
                    entry.category,
                    entry.title,
                    entry.content,
                    entry.keywords,
                    scope,
                    project_path,
                    source,
                    now
                ],
            )?;
            Ok(id)
        })
        .await
    }

    /// List one target only, newest first.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when the database query fails.
    pub async fn list_memories(&self, target: MemoryTarget) -> Result<Vec<MemoryRecord>, DbError> {
        self.with_reader(move |conn| {
            let (scope, project_path) = target.into_sql_parts();
            let sql = format!(
                "{MEMORY_SELECT} WHERE {TARGET_PREDICATE} ORDER BY updated_at DESC, id DESC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![scope, project_path], map_memory_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    /// Update a row in one target, inserting it there when absent.
    ///
    /// If the same id belongs to another target, the scoped update misses and
    /// the insert fails on the primary key. A caller therefore cannot move or
    /// overwrite memory across scopes by guessing an id.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when the scoped update or insert fails.
    pub async fn update_memory(
        &self,
        target: MemoryTarget,
        entry: MemoryUpsert,
    ) -> Result<(String, bool), DbError> {
        self.with_writer(move |conn| {
            let (scope, project_path) = target.into_sql_parts();
            let id = entry.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let source = entry.source.unwrap_or_else(|| DEFAULT_SOURCE.to_owned());
            let now = format_rfc3339_micros(now_millis());
            let updated = conn.execute(
                "UPDATE memories SET category = ?1, title = ?2, content = ?3, keywords = ?4, \
                 source = ?5, updated_at = ?6 WHERE id = ?7 AND scope = ?8 AND \
                 ((?9 IS NULL AND project_path IS NULL) OR project_path = ?9)",
                params![
                    &entry.category,
                    &entry.title,
                    &entry.content,
                    &entry.keywords,
                    &source,
                    &now,
                    &id,
                    scope,
                    &project_path
                ],
            )?;
            if updated > 0 {
                return Ok((id, false));
            }
            conn.execute(
                "INSERT INTO memories \
                 (id, category, title, content, keywords, scope, project_path, source, \
                  created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
                params![
                    &id,
                    &entry.category,
                    &entry.title,
                    &entry.content,
                    &entry.keywords,
                    scope,
                    &project_path,
                    &source,
                    &now
                ],
            )?;
            Ok((id, true))
        })
        .await
    }

    /// Delete an id only from the selected target.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when the database write fails.
    pub async fn delete_memory(&self, target: MemoryTarget, id: &str) -> Result<bool, DbError> {
        let id = id.to_owned();
        self.with_writer(move |conn| {
            let (scope, project_path) = target.into_sql_parts();
            let deleted = conn.execute(
                "DELETE FROM memories WHERE id = ?3 AND scope = ?1 AND \
                 ((?2 IS NULL AND project_path IS NULL) OR project_path = ?2)",
                params![scope, project_path, id],
            )?;
            Ok(deleted > 0)
        })
        .await
    }

    /// Delete rows whose content contains a literal pattern (case-insensitive)
    /// in the selected target only.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Invalid`] for a blank pattern or [`DbError`] when the
    /// database write fails.
    pub async fn delete_memories_matching(
        &self,
        target: MemoryTarget,
        pattern: &str,
    ) -> Result<usize, DbError> {
        if pattern.trim().is_empty() {
            return Err(DbError::Invalid(
                "memory delete pattern must not be blank".to_owned(),
            ));
        }
        let pattern = pattern.to_owned();
        self.with_writer(move |conn| {
            let (scope, project_path) = target.into_sql_parts();
            let deleted = conn.execute(
                "DELETE FROM memories WHERE scope = ?1 AND \
                 ((?2 IS NULL AND project_path IS NULL) OR project_path = ?2) AND \
                 instr(lower(content), lower(?3)) > 0",
                params![scope, project_path, pattern],
            )?;
            Ok(deleted)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryScope, MemoryTarget, MemoryUpsert};

    fn entry(id: &str, title: &str) -> MemoryUpsert {
        MemoryUpsert {
            id: Some(id.to_owned()),
            category: "SEMANTIC".to_owned(),
            title: title.to_owned(),
            content: format!("body of {title}"),
            keywords: None,
            source: None,
        }
    }

    #[tokio::test]
    async fn scopes_are_isolated_and_global_is_explicit() {
        let db = crate::Db::open_in_memory().expect("boot");
        db.create_memory(
            MemoryTarget::project("/work/a").unwrap(),
            entry("a", "alpha"),
        )
        .await
        .expect("project a");
        db.create_memory(
            MemoryTarget::project("/work/b").unwrap(),
            entry("b", "beta"),
        )
        .await
        .expect("project b");
        db.create_memory(MemoryTarget::global(), entry("g", "global"))
            .await
            .expect("global");

        let a = db
            .list_memories(MemoryTarget::project("/work/a").unwrap())
            .await
            .expect("list a");
        assert_eq!(
            a.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["a"]
        );
        assert_eq!(a[0].scope, MemoryScope::Project);
        assert_eq!(a[0].project_path.as_deref(), Some("/work/a"));

        let global = db
            .list_memories(MemoryTarget::global())
            .await
            .expect("list global");
        assert_eq!(
            global.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["g"]
        );
        assert_eq!(global[0].project_path, None);
    }

    #[tokio::test]
    async fn cross_scope_update_and_delete_fail_closed() {
        let db = crate::Db::open_in_memory().expect("boot");
        let project_a = MemoryTarget::project("/work/a").unwrap();
        let project_b = MemoryTarget::project("/work/b").unwrap();
        db.create_memory(project_a.clone(), entry("same-id", "original"))
            .await
            .expect("create");

        let err = db
            .update_memory(project_b.clone(), entry("same-id", "overwrite"))
            .await
            .expect_err("primary key prevents cross-scope move");
        assert!(matches!(err, crate::DbError::Sqlite(_)));
        assert!(
            !db.delete_memory(project_b, "same-id")
                .await
                .expect("scoped delete")
        );
        assert_eq!(
            db.list_memories(project_a).await.expect("list")[0].title,
            "original"
        );
    }

    #[tokio::test]
    async fn update_and_literal_pattern_delete_are_scoped() {
        let db = crate::Db::open_in_memory().expect("boot");
        let target = MemoryTarget::project("/work/a").unwrap();
        db.update_memory(target.clone(), entry("one", "100% Rust"))
            .await
            .expect("insert through upsert");
        let (_, inserted) = db
            .update_memory(target.clone(), entry("one", "updated"))
            .await
            .expect("update");
        assert!(!inserted);
        assert_eq!(
            db.delete_memories_matching(target.clone(), "%")
                .await
                .expect("literal percent"),
            0,
            "SQL wildcard characters must be treated literally"
        );
        assert_eq!(
            db.delete_memories_matching(target.clone(), "UPDATED")
                .await
                .expect("case insensitive"),
            1
        );
        assert!(db.list_memories(target).await.expect("list").is_empty());
    }

    #[test]
    fn blank_project_path_is_rejected() {
        assert!(MemoryTarget::project("  ").is_err());
    }
}
