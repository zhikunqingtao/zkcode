//! Durable, invocation-attributed research quality ledger.
#![allow(missing_docs, clippy::missing_errors_doc, clippy::too_many_lines)]

use std::fmt::Write as _;

use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{Db, DbError};

pub const MAX_RESEARCH_URL_BYTES: usize = 4_096;
pub const MAX_RESEARCH_TITLE_BYTES: usize = 4_096;
pub const MAX_RESEARCH_PROVIDER_BYTES: usize = 512;
pub const MAX_RESEARCH_EXCERPT_BYTES: usize = 8_192;
pub const MAX_RESEARCH_QUERY_BYTES: usize = 4_096;
pub const MAX_RESEARCH_RECEIPT_ENTRIES: usize = 10;
/// Per-category Workbench projection cap; source text is bounded separately.
pub const MAX_RESEARCH_PROJECTION_ROWS: usize = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ProducedResearchKind {
    WebSearch,
    WebFetch,
}

impl ProducedResearchKind {
    const fn as_db(self) -> &'static str {
        match self {
            Self::WebSearch => "webSearch",
            Self::WebFetch => "webFetch",
        }
    }

    const fn tool_name(self) -> &'static str {
        match self {
            Self::WebSearch => "WebSearch",
            Self::WebFetch => "WebFetch",
        }
    }

    const fn source_kind(self) -> &'static str {
        match self {
            Self::WebSearch => "searchResult",
            Self::WebFetch => "fetchedPage",
        }
    }

    const fn finding_kind(self) -> &'static str {
        match self {
            Self::WebSearch => "searchSnippet",
            Self::WebFetch => "fetchExcerpt",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducedResearchEntry {
    pub url: String,
    pub title: Option<String>,
    pub provider: Option<String>,
    pub excerpt: Option<String>,
    pub rank: Option<i64>,
    pub http_status: Option<i64>,
    pub content_type: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducedResearchCapture {
    pub task_id: String,
    pub run_id: String,
    pub producer_invocation_id: String,
    pub kind: ProducedResearchKind,
    pub query: Option<String>,
    pub fetched_at: String,
    pub entries: Vec<ProducedResearchEntry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchCaptureRecord {
    pub producer_invocation_id: String,
    pub root_task_id: String,
    pub task_id: String,
    pub run_id: String,
    pub capture_kind: String,
    pub query: Option<String>,
    pub fetched_at: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchSourceRecord {
    pub source_id: String,
    pub root_task_id: String,
    pub task_id: String,
    pub run_id: String,
    pub producer_invocation_id: String,
    pub ordinal: i64,
    pub source_kind: String,
    pub url: String,
    pub title: Option<String>,
    pub provider: Option<String>,
    pub fetched_at: String,
    pub http_status: Option<i64>,
    pub content_type: Option<String>,
    pub truncated: bool,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchFindingRecord {
    pub finding_id: String,
    pub source_id: String,
    pub root_task_id: String,
    pub task_id: String,
    pub run_id: String,
    pub producer_invocation_id: String,
    pub ordinal: i64,
    pub finding_kind: String,
    pub excerpt: String,
    pub rank: Option<i64>,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchConflictRecord {
    pub conflict_id: String,
    pub root_task_id: String,
    pub left_finding_id: Option<String>,
    pub right_finding_id: Option<String>,
    pub summary: String,
    pub status: String,
    pub resolution: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchOpenQuestionRecord {
    pub question_id: String,
    pub root_task_id: String,
    pub task_id: Option<String>,
    pub run_id: Option<String>,
    pub question: String,
    pub status: String,
    pub resolution: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchRequirementCoverageRecord {
    pub coverage_id: String,
    pub root_task_id: String,
    pub requirement_key: String,
    pub requirement_text: String,
    pub status: String,
    pub supporting_finding_id: Option<String>,
    pub notes: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Safe Workbench projection. It contains bounded excerpts and provenance, but
/// never the complete `WebFetch` body or raw invocation input/output.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResearchProjection {
    pub root_task_id: String,
    /// True when at least one category exceeded the bounded Workbench view.
    pub truncated: bool,
    pub captures: Vec<ResearchCaptureRecord>,
    pub sources: Vec<ResearchSourceRecord>,
    pub findings: Vec<ResearchFindingRecord>,
    pub conflicts: Vec<ResearchConflictRecord>,
    pub open_questions: Vec<ResearchOpenQuestionRecord>,
    pub requirement_coverage: Vec<ResearchRequirementCoverageRecord>,
}

impl Db {
    /// Commit a structured receipt after the physical web invocation is already
    /// durably succeeded. Exact retries are no-ops; changed retries fail closed.
    pub async fn record_research_capture(
        &self,
        capture: &ProducedResearchCapture,
    ) -> Result<(), DbError> {
        validate_capture(capture)?;
        let capture = capture.clone();
        let receipt_sha256 = sha256_hex(&serde_json::to_vec(&capture)?);
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let owner = tx
                .query_row(
                    "SELECT invocation.task_id,invocation.run_id,invocation.tool_name,task.root_task_id \
                     FROM tool_invocations invocation \
                     JOIN run_envelopes run ON run.id=invocation.run_id \
                     JOIN tasks task ON task.id=invocation.task_id \
                     WHERE invocation.invocation_id=?1 \
                       AND invocation.status='succeeded' \
                       AND invocation.side_effect_class='read' \
                       AND invocation.tool_name IN ('WebSearch','WebFetch') \
                       AND run.task_id=invocation.task_id",
                    [&capture.producer_invocation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .optional()?;
            let Some((task_id, run_id, tool_name, root_task_id)) = owner else {
                return Err(DbError::Invalid(
                    "RESEARCH_PRODUCER_INVOCATION_MISMATCH".to_owned(),
                ));
            };
            if task_id != capture.task_id
                || run_id != capture.run_id
                || tool_name != capture.kind.tool_name()
            {
                return Err(DbError::Invalid(
                    "RESEARCH_PRODUCER_INVOCATION_MISMATCH".to_owned(),
                ));
            }

            let existing = tx
                .query_row(
                    "SELECT receipt_sha256 FROM research_captures \
                     WHERE producer_invocation_id=?1",
                    [&capture.producer_invocation_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(existing_hash) = existing {
                if existing_hash == receipt_sha256 {
                    tx.commit()?;
                    return Ok(());
                }
                return Err(DbError::Invalid(
                    "RESEARCH_CAPTURE_IMMUTABLE".to_owned(),
                ));
            }

            let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
            tx.execute(
                "INSERT INTO research_captures \
                 (producer_invocation_id,root_task_id,task_id,run_id,capture_kind,query, \
                  fetched_at,receipt_sha256,created_at) \
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    &capture.producer_invocation_id,
                    &root_task_id,
                    &capture.task_id,
                    &capture.run_id,
                    capture.kind.as_db(),
                    &capture.query,
                    &capture.fetched_at,
                    &receipt_sha256,
                    &now,
                ],
            )?;
            for (ordinal, entry) in capture.entries.iter().enumerate() {
                let ordinal = i64::try_from(ordinal)
                    .map_err(|_| DbError::Invalid("RESEARCH_ORDINAL_INVALID".to_owned()))?;
                let source_id = uuid::Uuid::new_v4().to_string();
                tx.execute(
                    "INSERT INTO research_sources \
                     (source_id,root_task_id,task_id,run_id,producer_invocation_id,ordinal, \
                      source_kind,url,title,provider,fetched_at,http_status,content_type, \
                      truncated,created_at) \
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
                    params![
                        &source_id,
                        &root_task_id,
                        &capture.task_id,
                        &capture.run_id,
                        &capture.producer_invocation_id,
                        ordinal,
                        capture.kind.source_kind(),
                        &entry.url,
                        &entry.title,
                        &entry.provider,
                        &capture.fetched_at,
                        entry.http_status,
                        &entry.content_type,
                        i64::from(entry.truncated),
                        &now,
                    ],
                )?;
                if let Some(excerpt) = entry.excerpt.as_deref().filter(|value| !value.is_empty()) {
                    tx.execute(
                        "INSERT INTO research_findings \
                         (finding_id,source_id,root_task_id,task_id,run_id, \
                          producer_invocation_id,ordinal,finding_kind,excerpt,rank,created_at) \
                         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                        params![
                            uuid::Uuid::new_v4().to_string(),
                            &source_id,
                            &root_task_id,
                            &capture.task_id,
                            &capture.run_id,
                            &capture.producer_invocation_id,
                            ordinal,
                            capture.kind.finding_kind(),
                            excerpt,
                            entry.rank,
                            &now,
                        ],
                    )?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Read a safe projection for an owned root Task.
    pub async fn find_research_projection_by_root_task(
        &self,
        root_task_id: &str,
        asserted_root_session_id: &str,
    ) -> Result<Option<ResearchProjection>, DbError> {
        let root_task_id = root_task_id.to_owned();
        let asserted_root_session_id = asserted_root_session_id.to_owned();
        self.with_reader(move |conn| {
            let owned = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM tasks task \
                 WHERE task.id=?1 AND task.root_task_id=task.id \
                   AND task.parent_task_id IS NULL AND task.session_id=?2)",
                params![root_task_id, asserted_root_session_id],
                |row| row.get::<_, i64>(0),
            )? != 0;
            if !owned {
                return Ok(None);
            }
            load_projection(conn, &root_task_id).map(Some)
        })
        .await
    }

    /// Read a safe projection for an owned root Run attempt.
    pub async fn find_research_projection_by_root_run(
        &self,
        root_run_id: &str,
        asserted_root_session_id: &str,
    ) -> Result<Option<ResearchProjection>, DbError> {
        let root_run_id = root_run_id.to_owned();
        let asserted_root_session_id = asserted_root_session_id.to_owned();
        self.with_reader(move |conn| {
            let root_task_id = conn
                .query_row(
                    "SELECT task.id FROM run_envelopes run \
                     JOIN tasks task ON task.id=run.task_id \
                     WHERE run.id=?1 AND run.parent_run_id IS NULL \
                       AND task.parent_task_id IS NULL AND task.root_task_id=task.id \
                       AND task.session_id=?2",
                    params![root_run_id, asserted_root_session_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            root_task_id.map_or(Ok(None), |task_id| {
                load_projection(conn, &task_id).map(Some)
            })
        })
        .await
    }
}

fn validate_capture(capture: &ProducedResearchCapture) -> Result<(), DbError> {
    let producer_id = uuid::Uuid::parse_str(&capture.producer_invocation_id)
        .map_err(|_| DbError::Invalid("RESEARCH_INVOCATION_ID_MUST_BE_UUID_V4".to_owned()))?;
    if producer_id.get_version() != Some(uuid::Version::Random)
        || producer_id.hyphenated().to_string() != capture.producer_invocation_id
    {
        return Err(DbError::Invalid(
            "RESEARCH_INVOCATION_ID_MUST_BE_UUID_V4".to_owned(),
        ));
    }
    if capture.fetched_at.is_empty() || capture.fetched_at.len() > 64 {
        return Err(DbError::Invalid("RESEARCH_FETCHED_AT_INVALID".to_owned()));
    }
    if capture.entries.len() > MAX_RESEARCH_RECEIPT_ENTRIES
        || matches!(capture.kind, ProducedResearchKind::WebFetch) && capture.entries.len() != 1
    {
        return Err(DbError::Invalid("RESEARCH_ENTRY_COUNT_INVALID".to_owned()));
    }
    match capture.kind {
        ProducedResearchKind::WebSearch => {
            capture
                .query
                .as_deref()
                .filter(|query| !query.is_empty() && query.len() <= MAX_RESEARCH_QUERY_BYTES)
                .ok_or_else(|| DbError::Invalid("RESEARCH_QUERY_INVALID".to_owned()))?;
        }
        ProducedResearchKind::WebFetch if capture.query.is_some() => {
            return Err(DbError::Invalid("RESEARCH_QUERY_INVALID".to_owned()));
        }
        ProducedResearchKind::WebFetch => {}
    }
    for (index, entry) in capture.entries.iter().enumerate() {
        let parsed = Url::parse(&entry.url)
            .map_err(|_| DbError::Invalid("RESEARCH_SOURCE_URL_INVALID".to_owned()))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || entry.url.len() > MAX_RESEARCH_URL_BYTES
            || entry
                .title
                .as_ref()
                .is_some_and(|value| value.len() > MAX_RESEARCH_TITLE_BYTES)
            || entry
                .provider
                .as_ref()
                .is_some_and(|value| value.len() > MAX_RESEARCH_PROVIDER_BYTES)
            || entry
                .excerpt
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > MAX_RESEARCH_EXCERPT_BYTES)
            || entry
                .content_type
                .as_ref()
                .is_some_and(|value| value.len() > 256)
        {
            return Err(DbError::Invalid("RESEARCH_ENTRY_INVALID".to_owned()));
        }
        match capture.kind {
            ProducedResearchKind::WebSearch
                if entry.rank != i64::try_from(index + 1).ok()
                    || entry.http_status.is_some()
                    || entry.content_type.is_some() =>
            {
                return Err(DbError::Invalid("RESEARCH_SEARCH_ENTRY_INVALID".to_owned()));
            }
            ProducedResearchKind::WebFetch
                if entry.rank.is_some()
                    || !entry
                        .http_status
                        .is_some_and(|status| (100..=599).contains(&status)) =>
            {
                return Err(DbError::Invalid("RESEARCH_FETCH_ENTRY_INVALID".to_owned()));
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn load_projection(
    conn: &rusqlite::Connection,
    root_task_id: &str,
) -> Result<ResearchProjection, DbError> {
    let mut captures = {
        let mut statement = conn.prepare(
            "SELECT producer_invocation_id,root_task_id,task_id,run_id,capture_kind,query, \
                    fetched_at,created_at FROM research_captures \
             WHERE root_task_id=?1 ORDER BY fetched_at,producer_invocation_id LIMIT 1001",
        )?;
        statement
            .query_map([root_task_id], |row| {
                Ok(ResearchCaptureRecord {
                    producer_invocation_id: row.get(0)?,
                    root_task_id: row.get(1)?,
                    task_id: row.get(2)?,
                    run_id: row.get(3)?,
                    capture_kind: row.get(4)?,
                    query: row.get(5)?,
                    fetched_at: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut sources = {
        let mut statement = conn.prepare(
            "SELECT source_id,root_task_id,task_id,run_id,producer_invocation_id,ordinal, \
                    source_kind,url,title,provider,fetched_at,http_status,content_type, \
                    truncated,created_at FROM research_sources \
             WHERE root_task_id=?1 ORDER BY fetched_at,producer_invocation_id,ordinal LIMIT 1001",
        )?;
        statement
            .query_map([root_task_id], |row| {
                Ok(ResearchSourceRecord {
                    source_id: row.get(0)?,
                    root_task_id: row.get(1)?,
                    task_id: row.get(2)?,
                    run_id: row.get(3)?,
                    producer_invocation_id: row.get(4)?,
                    ordinal: row.get(5)?,
                    source_kind: row.get(6)?,
                    url: row.get(7)?,
                    title: row.get(8)?,
                    provider: row.get(9)?,
                    fetched_at: row.get(10)?,
                    http_status: row.get(11)?,
                    content_type: row.get(12)?,
                    truncated: row.get::<_, i64>(13)? != 0,
                    created_at: row.get(14)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut findings = {
        let mut statement = conn.prepare(
            "SELECT finding_id,source_id,root_task_id,task_id,run_id, \
                    producer_invocation_id,ordinal,finding_kind,excerpt,rank,created_at \
             FROM research_findings WHERE root_task_id=?1 \
             ORDER BY created_at,producer_invocation_id,ordinal,finding_id LIMIT 1001",
        )?;
        statement
            .query_map([root_task_id], |row| {
                Ok(ResearchFindingRecord {
                    finding_id: row.get(0)?,
                    source_id: row.get(1)?,
                    root_task_id: row.get(2)?,
                    task_id: row.get(3)?,
                    run_id: row.get(4)?,
                    producer_invocation_id: row.get(5)?,
                    ordinal: row.get(6)?,
                    finding_kind: row.get(7)?,
                    excerpt: row.get(8)?,
                    rank: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut conflicts = {
        let mut statement = conn.prepare(
            "SELECT conflict_id,root_task_id,left_finding_id,right_finding_id,summary, \
                    status,resolution,created_at,updated_at FROM research_conflicts \
             WHERE root_task_id=?1 ORDER BY created_at,conflict_id LIMIT 1001",
        )?;
        statement
            .query_map([root_task_id], |row| {
                Ok(ResearchConflictRecord {
                    conflict_id: row.get(0)?,
                    root_task_id: row.get(1)?,
                    left_finding_id: row.get(2)?,
                    right_finding_id: row.get(3)?,
                    summary: row.get(4)?,
                    status: row.get(5)?,
                    resolution: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut open_questions = {
        let mut statement = conn.prepare(
            "SELECT question_id,root_task_id,task_id,run_id,question,status,resolution, \
                    created_at,updated_at FROM research_open_questions \
             WHERE root_task_id=?1 ORDER BY created_at,question_id LIMIT 1001",
        )?;
        statement
            .query_map([root_task_id], |row| {
                Ok(ResearchOpenQuestionRecord {
                    question_id: row.get(0)?,
                    root_task_id: row.get(1)?,
                    task_id: row.get(2)?,
                    run_id: row.get(3)?,
                    question: row.get(4)?,
                    status: row.get(5)?,
                    resolution: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut requirement_coverage = {
        let mut statement = conn.prepare(
            "SELECT coverage_id,root_task_id,requirement_key,requirement_text,status, \
                    supporting_finding_id,notes,created_at,updated_at \
             FROM research_requirement_coverage WHERE root_task_id=?1 \
             ORDER BY requirement_key,coverage_id LIMIT 1001",
        )?;
        statement
            .query_map([root_task_id], |row| {
                Ok(ResearchRequirementCoverageRecord {
                    coverage_id: row.get(0)?,
                    root_task_id: row.get(1)?,
                    requirement_key: row.get(2)?,
                    requirement_text: row.get(3)?,
                    status: row.get(4)?,
                    supporting_finding_id: row.get(5)?,
                    notes: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let truncated = captures.len() > MAX_RESEARCH_PROJECTION_ROWS
        || sources.len() > MAX_RESEARCH_PROJECTION_ROWS
        || findings.len() > MAX_RESEARCH_PROJECTION_ROWS
        || conflicts.len() > MAX_RESEARCH_PROJECTION_ROWS
        || open_questions.len() > MAX_RESEARCH_PROJECTION_ROWS
        || requirement_coverage.len() > MAX_RESEARCH_PROJECTION_ROWS;
    captures.truncate(MAX_RESEARCH_PROJECTION_ROWS);
    sources.truncate(MAX_RESEARCH_PROJECTION_ROWS);
    findings.truncate(MAX_RESEARCH_PROJECTION_ROWS);
    conflicts.truncate(MAX_RESEARCH_PROJECTION_ROWS);
    open_questions.truncate(MAX_RESEARCH_PROJECTION_ROWS);
    requirement_coverage.truncate(MAX_RESEARCH_PROJECTION_ROWS);
    Ok(ResearchProjection {
        root_task_id: root_task_id.to_owned(),
        truncated,
        captures,
        sources,
        findings,
        conflicts,
        open_questions,
        requirement_coverage,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CleanupStatus, NewToolInvocation, ToolInvocationStatus};

    async fn seed_invocation(
        db: &Db,
        run_id: &str,
        tool_use_id: &str,
        tool_name: &str,
        status: ToolInvocationStatus,
    ) -> (String, String) {
        let run = db
            .find_run_by_id(run_id)
            .await
            .expect("run lookup")
            .expect("run");
        let invocation_id = uuid::Uuid::new_v4().to_string();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation_id.clone(),
            task_id: run.task_id.clone(),
            run_id: run_id.to_owned(),
            tool_use_id: tool_use_id.to_owned(),
            tool_name: tool_name.to_owned(),
            input_json: Some("{}".to_owned()),
            side_effect_class: "read".to_owned(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .expect("create invocation");
        assert_eq!(
            db.transition_tool_invocation_cas(
                &invocation_id,
                0,
                status,
                Some("{}"),
                Some("toolResult:test"),
                None,
                CleanupStatus::NotRequired,
            )
            .await
            .expect("terminal invocation"),
            crate::CasOutcome::Applied
        );
        (invocation_id, run.task_id)
    }

    fn search_capture(task_id: &str, run_id: &str, invocation_id: &str) -> ProducedResearchCapture {
        ProducedResearchCapture {
            task_id: task_id.to_owned(),
            run_id: run_id.to_owned(),
            producer_invocation_id: invocation_id.to_owned(),
            kind: ProducedResearchKind::WebSearch,
            query: Some("durable agents".to_owned()),
            fetched_at: "2026-09-09T00:00:00.000000Z".to_owned(),
            entries: vec![ProducedResearchEntry {
                url: "https://example.com/agents".to_owned(),
                title: Some("Durable agents".to_owned()),
                provider: Some("fixture".to_owned()),
                excerpt: Some("A bounded attributable finding.".to_owned()),
                rank: Some(1),
                http_status: None,
                content_type: None,
                truncated: false,
            }],
        }
    }

    #[tokio::test]
    async fn succeeded_web_invocation_records_bounded_projection_and_enforces_root_access() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/research")
            .await
            .expect("session");
        let run_id = uuid::Uuid::new_v4().to_string();
        db.start_run(&run_id, &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        let (invocation_id, task_id) = seed_invocation(
            &db,
            &run_id,
            "search-ok",
            "WebSearch",
            ToolInvocationStatus::Succeeded,
        )
        .await;
        let capture = search_capture(&task_id, &run_id, &invocation_id);
        db.record_research_capture(&capture).await.expect("capture");
        db.record_research_capture(&capture)
            .await
            .expect("idempotent capture");

        let projection = db
            .find_research_projection_by_root_run(&run_id, &session.id)
            .await
            .expect("projection")
            .expect("owned root");
        assert_eq!(projection.captures.len(), 1);
        assert_eq!(projection.sources.len(), 1);
        assert_eq!(projection.findings.len(), 1);
        assert_eq!(
            projection.findings[0].excerpt,
            "A bounded attributable finding."
        );
        assert!(
            db.find_research_projection_by_root_task(&task_id, "another-root-session")
                .await
                .expect("denied projection")
                .is_none()
        );
    }

    #[tokio::test]
    async fn failed_or_wrong_web_producer_is_rejected_without_partial_rows() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/research")
            .await
            .expect("session");
        let run_id = uuid::Uuid::new_v4().to_string();
        db.start_run(&run_id, &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        let (failed_id, task_id) = seed_invocation(
            &db,
            &run_id,
            "search-failed",
            "WebSearch",
            ToolInvocationStatus::Failed,
        )
        .await;
        let error = db
            .record_research_capture(&search_capture(&task_id, &run_id, &failed_id))
            .await
            .expect_err("failed producer");
        assert!(
            error
                .to_string()
                .contains("RESEARCH_PRODUCER_INVOCATION_MISMATCH")
        );

        let (wrong_id, task_id) = seed_invocation(
            &db,
            &run_id,
            "echo-ok",
            "Echo",
            ToolInvocationStatus::Succeeded,
        )
        .await;
        let error = db
            .record_research_capture(&search_capture(&task_id, &run_id, &wrong_id))
            .await
            .expect_err("wrong producer");
        assert!(
            error
                .to_string()
                .contains("RESEARCH_PRODUCER_INVOCATION_MISMATCH")
        );

        let projection = db
            .find_research_projection_by_root_run(&run_id, &session.id)
            .await
            .expect("projection")
            .expect("owned root");
        assert!(projection.captures.is_empty());
        assert!(projection.sources.is_empty());
    }
}
