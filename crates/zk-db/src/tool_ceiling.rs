//! Request-local tool constraints inherited through the authoritative Task config.
use crate::{Db, DbError};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

/// A deny always wins. An absent allowlist preserves the host's existing directory.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolCeiling {
    /// Optional explicit allowlist; an empty set permits no tool.
    pub allowed: Option<BTreeSet<String>>,
    /// Explicitly excluded tool names.
    #[serde(default)]
    pub denied: BTreeSet<String>,
}
impl ToolCeiling {
    /// Check a tool name without granting permission to its implementation.
    #[must_use]
    pub fn allows(&self, name: &str) -> bool {
        self.allowed
            .as_ref()
            .is_none_or(|names| names.contains(name))
            && !self.denied.contains(name)
    }
    /// Whether this policy leaves the existing host directory unchanged.
    #[must_use]
    pub fn unrestricted(&self) -> bool {
        self.allowed.is_none() && self.denied.is_empty()
    }
    /// Intersection cannot add any capability to either input policy.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let allowed = match (&self.allowed, &other.allowed) {
            (Some(a), Some(b)) => Some(a.intersection(b).cloned().collect()),
            (Some(a), None) | (None, Some(a)) => Some(a.clone()),
            (None, None) => None,
        };
        Self {
            allowed,
            denied: self.denied.union(&other.denied).cloned().collect(),
        }
    }
    /// Decode only validated metadata, never interpreting absent values as malformed constraints.
    /// # Errors
    /// Rejects malformed lists or an invalid typed ceiling.
    pub fn from_config(config: &Value) -> Result<Self, DbError> {
        if !config.is_object() {
            return Err(DbError::Invalid("TOOL_CEILING_INVALID".into()));
        }
        let ceiling: Self = config.get("toolCeiling").map_or_else(
            || Ok(Self::default()),
            |value| {
                serde_json::from_value(value.clone())
                    .map_err(|_| DbError::Invalid("TOOL_CEILING_INVALID".into()))
            },
        )?;
        if ceiling
            .allowed
            .iter()
            .flatten()
            .chain(&ceiling.denied)
            .any(|name| name.is_empty() || name.len() > 256)
        {
            return Err(DbError::Invalid("TOOL_CEILING_INVALID".into()));
        }
        let allowed = names(config.get("allowedTools"))?;
        let denied = names(config.get("disallowedTools"))?.unwrap_or_default();
        Ok(ceiling.intersect(&Self { allowed, denied }))
    }
}
fn names(value: Option<&Value>) -> Result<Option<BTreeSet<String>>, DbError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let array = value
        .as_array()
        .ok_or_else(|| DbError::Invalid("TOOL_CEILING_INVALID".into()))?;
    let values = array
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 256)
                .map(str::to_owned)
                .ok_or_else(|| DbError::Invalid("TOOL_CEILING_INVALID".into()))
        })
        .collect::<Result<_, _>>()?;
    Ok(Some(values))
}

pub(crate) fn run_ceiling(conn: &Connection, run_id: &str) -> Result<ToolCeiling, DbError> {
    let (session,raw):(String,String)=conn.query_row("SELECT t.session_id,t.execution_config_json FROM tasks t JOIN run_envelopes r ON r.task_id=t.id WHERE r.id=?1",[run_id],|row|Ok((row.get(0)?,row.get(1)?)))?;
    let raw = crate::content::load_text(conn, &session, &raw)?;
    ToolCeiling::from_config(&serde_json::from_str::<Value>(&raw)?)
}

pub(crate) fn inherit(
    conn: &Connection,
    parent_run: Option<&str>,
    raw: &str,
) -> Result<String, DbError> {
    let mut config: Value = serde_json::from_str(raw)?;
    let mut ceiling = ToolCeiling::from_config(&config)?;
    if let Some(parent) = parent_run {
        ceiling = ceiling.intersect(&run_ceiling(conn, parent)?);
    }
    if !ceiling.unrestricted() || config.get("toolCeiling").is_some() {
        config
            .as_object_mut()
            .ok_or_else(|| DbError::Invalid("TOOL_CEILING_INVALID".into()))?
            .insert("toolCeiling".into(), serde_json::to_value(ceiling)?);
        Ok(config.to_string())
    } else {
        Ok(raw.into())
    }
}
impl Db {
    /// Read the frozen Task policy for the exact physical Run.
    /// # Errors
    /// Missing ownership, unavailable temporary content or invalid policy fails closed.
    pub async fn run_tool_ceiling(&self, run_id: &str) -> Result<ToolCeiling, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| run_ceiling(conn, &run_id))
            .await
    }
    /// Persist a runtime directive only as an intersection with existing authority.
    /// # Errors
    /// Rejects stale/closed Runs, invalid policy or any content-store/database failure.
    pub async fn narrow_run_tool_ceiling(
        &self,
        run_id: &str,
        proposed: &ToolCeiling,
    ) -> Result<ToolCeiling, DbError> {
        let run_id = run_id.to_owned();
        let proposed = proposed.clone();
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let (task,session,raw):(String,String,String)=tx.query_row("SELECT t.id,t.session_id,t.execution_config_json FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id WHERE r.id=?1 AND r.status IN ('running','waitingDependencies','waitingInteraction') AND r.requested_exit_reason IS NULL",[&run_id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
            let raw=crate::content::load_text(&tx,&session,&raw)?;
            let mut config:Value=serde_json::from_str(&raw)?;
            let ceiling=ToolCeiling::from_config(&config)?.intersect(&proposed);
            config["toolCeiling"]=serde_json::to_value(&ceiling)?;
            let encoded=crate::content::store_text(&tx,&session,&config.to_string())?;
            tx.execute("UPDATE tasks SET execution_config_json=?2 WHERE id=?1",rusqlite::params![task,encoded])?;
            tx.commit()?;Ok(ceiling)
        }).await
    }
}
