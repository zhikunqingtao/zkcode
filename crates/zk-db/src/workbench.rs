//! Durable workbench bindings and acceptance criteria repository.

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::activity::load_activities_by_session_paged_in_snapshot;
use crate::artifact::load_manifest;
use crate::evidence::load_bundle;
use crate::message::load_message_rows;
use crate::research::load_projection as load_research_projection;
use crate::run::{RunEnvelopeView, map_envelope_row};
use crate::task_runtime::{RUNTIME_TASK_COLUMNS, map_runtime_task};
use crate::{
    ArtifactManifestRecord, Db, DbError, EvidenceBundleRecord, MessageRecord, ResearchProjection,
    RuntimeTaskRecord,
};

/// Root-run message binding.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbenchBindingRecord {
    /// Root run identifier.
    pub root_run_id: String,
    /// User request message.
    pub request_message_id: String,
    /// Final deliverable assistant message.
    pub result_message_id: Option<String>,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// RFC 3339 update time.
    pub updated_at: String,
}

/// One explicit business acceptance criterion.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcceptanceCriterionRecord {
    /// Criterion identifier.
    pub criterion_id: String,
    /// Owning root run.
    pub root_run_id: String,
    /// Stable request order.
    pub ordinal: i64,
    /// Currently always `business`.
    pub criterion_type: String,
    /// Original criterion text.
    pub source_text: String,
    /// `passed`, `failed`, `partial` or `not_verified`.
    pub status: String,
    /// Explicit supporting evidence bundle.
    pub evidence_bundle_id: Option<String>,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// RFC 3339 update time.
    pub updated_at: String,
}

/// Complete durable workbench association for one root run.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbenchRecord {
    /// Message binding.
    pub binding: WorkbenchBindingRecord,
    /// Ordered acceptance criteria.
    pub criteria: Vec<AcceptanceCriterionRecord>,
}

/// Exact subtree usage from the physical model-call ledger.
///
/// A started/failed call with missing usage contributes zero to numeric totals
/// and flips `complete` to false. Zero is therefore never used to pretend an
/// unknown provider response was fully accounted for.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbenchSubtreeUsage {
    /// Billed input tokens with known usage.
    pub input_tokens: i64,
    /// Billed output tokens with known usage.
    pub output_tokens: i64,
    /// Cache-read tokens with known usage.
    pub cache_read_tokens: i64,
    /// Cache-create tokens with known usage.
    pub cache_create_tokens: i64,
    /// Exact billionths of a USD with known pricing.
    pub cost_nanos_usd: i64,
    /// Whether every physical call has authoritative usage and pricing.
    pub complete: bool,
}

/// One non-terminal tool invocation in the current root Run tree.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbenchActiveTool {
    /// Physical invocation identity.
    pub invocation_id: String,
    /// Logical owner Task.
    pub task_id: String,
    /// Physical owner Run.
    pub run_id: String,
    /// Provider tool-use identity.
    pub tool_use_id: String,
    /// Registered tool name.
    pub tool_name: String,
    /// Persisted lifecycle state.
    pub status: String,
    /// Complete input when it has been assembled; absent while preparing.
    pub input: Option<Value>,
    /// Persisted side-effect classification.
    pub side_effect_class: String,
    /// Orthogonal cleanup state.
    pub cleanup_status: String,
    /// Admission time, when execution has started.
    pub started_at: Option<String>,
    /// Durable creation time.
    pub created_at: String,
}

/// Last successful delivery shown when the latest root Run failed.
#[derive(Clone, Debug)]
pub struct PreviousWorkbenchDelivery {
    /// Previous completed root Run.
    pub root_run: RunEnvelopeView,
    /// Explicitly bound result message, when present.
    pub result_message: Option<MessageRecord>,
    /// Artifact manifests across that Run tree.
    pub manifests: Vec<ArtifactManifestRecord>,
}

/// One immutable `SQLite` read snapshot for the complete current Workbench.
///
/// The public API deliberately returns empty collections/options for a root
/// Session that has not executed yet. This makes an empty snapshot
/// authoritative and lets clients clear stale state after switching sessions.
#[derive(Clone, Debug)]
pub struct CurrentWorkbenchProjection {
    /// Root logical Task selected by the current root Run.
    pub root_task: Option<RuntimeTaskRecord>,
    /// Complete logical Task tree, including internal child Sessions.
    pub task_tree: Vec<RuntimeTaskRecord>,
    /// Latest root Run for the visible Session.
    pub root_run: Option<RunEnvelopeView>,
    /// Complete physical Run tree, including internal child Sessions.
    pub run_tree: Vec<RunEnvelopeView>,
    /// Explicit request/result binding and acceptance criteria.
    pub workbench: Option<WorkbenchRecord>,
    /// Bound root request message.
    pub request_message: Option<MessageRecord>,
    /// Bound final Assistant message.
    pub result_message: Option<MessageRecord>,
    /// Artifact manifests across the current Run tree.
    pub manifests: Vec<ArtifactManifestRecord>,
    /// Evidence bundles across the current Run tree.
    pub evidence: Vec<EvidenceBundleRecord>,
    /// Pending durable interactions across the current Run tree.
    pub pending_actions: Vec<zk_protocol::InteractionView>,
    /// Run-attributed activity rows in the bounded current view.
    pub activities: Vec<Value>,
    /// Bounded research provenance for the root Task.
    pub research: ResearchProjection,
    /// Exact subtree model-call ledger projection.
    pub usage: WorkbenchSubtreeUsage,
    /// Global `run_event_log.id` high-water for the selected root Run tree.
    pub event_high_water: i64,
    /// Non-terminal invocations across the current Run tree.
    pub active_tools: Vec<WorkbenchActiveTool>,
    /// Most recent completed artifact-bearing delivery after current failure.
    pub previous_delivery: Option<PreviousWorkbenchDelivery>,
}

impl CurrentWorkbenchProjection {
    fn empty() -> Self {
        Self {
            root_task: None,
            task_tree: Vec::new(),
            root_run: None,
            run_tree: Vec::new(),
            workbench: None,
            request_message: None,
            result_message: None,
            manifests: Vec::new(),
            evidence: Vec::new(),
            pending_actions: Vec::new(),
            activities: Vec::new(),
            research: ResearchProjection::default(),
            usage: WorkbenchSubtreeUsage {
                complete: true,
                ..WorkbenchSubtreeUsage::default()
            },
            event_high_water: 0,
            active_tools: Vec::new(),
            previous_delivery: None,
        }
    }
}

impl Db {
    /// Create the root-run binding and initial criteria in one transaction.
    ///
    /// # Errors
    /// Returns [`DbError`] when a criterion belongs to another run or the `SQLite`
    /// transaction fails.
    pub async fn initialize_workbench(
        &self,
        binding: &WorkbenchBindingRecord,
        criteria: &[AcceptanceCriterionRecord],
    ) -> Result<(), DbError> {
        let binding = binding.clone();
        let criteria = criteria.to_vec();
        self.with_writer(move |conn| {
            if criteria
                .iter()
                .any(|criterion| criterion.root_run_id != binding.root_run_id)
            {
                return Err(DbError::Invalid(
                    "acceptance criterion root run mismatch".into(),
                ));
            }
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO run_workbench_bindings \
                 (root_run_id,request_message_id,result_message_id,created_at,updated_at) \
                 VALUES (?1,?2,?3,?4,?5) ON CONFLICT(root_run_id) DO UPDATE SET \
                 request_message_id=excluded.request_message_id, \
                 result_message_id=excluded.result_message_id,updated_at=excluded.updated_at",
                rusqlite::params![
                    binding.root_run_id,
                    binding.request_message_id,
                    binding.result_message_id,
                    binding.created_at,
                    binding.updated_at,
                ],
            )?;
            tx.execute(
                "DELETE FROM run_acceptance_criteria WHERE root_run_id=?1",
                [&binding.root_run_id],
            )?;
            for criterion in criteria {
                tx.execute(
                    "INSERT INTO run_acceptance_criteria \
                     (criterion_id,root_run_id,ordinal,criterion_type,source_text,status, \
                      evidence_bundle_id,created_at,updated_at) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    rusqlite::params![
                        criterion.criterion_id,
                        criterion.root_run_id,
                        criterion.ordinal,
                        criterion.criterion_type,
                        criterion.source_text,
                        criterion.status,
                        criterion.evidence_bundle_id,
                        criterion.created_at,
                        criterion.updated_at,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Upsert the request/result binding.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` write fails.
    pub async fn save_workbench_binding(
        &self,
        binding: &WorkbenchBindingRecord,
    ) -> Result<(), DbError> {
        let binding = binding.clone();
        self.with_writer(move |conn| {
            conn.execute(
                "INSERT INTO run_workbench_bindings \
                 (root_run_id,request_message_id,result_message_id,created_at,updated_at) \
                 VALUES (?1,?2,?3,?4,?5) ON CONFLICT(root_run_id) DO UPDATE SET \
                 request_message_id=excluded.request_message_id, \
                 result_message_id=excluded.result_message_id,updated_at=excluded.updated_at",
                rusqlite::params![
                    binding.root_run_id,
                    binding.request_message_id,
                    binding.result_message_id,
                    binding.created_at,
                    binding.updated_at,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Attach the final assistant deliverable without replacing the request binding.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` update fails.
    pub async fn bind_workbench_result(
        &self,
        root_run_id: &str,
        result_message_id: &str,
    ) -> Result<bool, DbError> {
        let root_run_id = root_run_id.to_owned();
        let result_message_id = result_message_id.to_owned();
        self.with_writer(move |conn| {
            let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
            Ok(conn.execute(
                "UPDATE run_workbench_bindings SET result_message_id=?1,updated_at=?2 \
                 WHERE root_run_id=?3",
                rusqlite::params![result_message_id, now, root_run_id],
            )? > 0)
        })
        .await
    }

    /// Replace all criteria for a root run atomically.
    ///
    /// # Errors
    /// Returns [`DbError`] when a criterion belongs to another run or the `SQLite`
    /// transaction fails.
    pub async fn replace_acceptance_criteria(
        &self,
        root_run_id: &str,
        criteria: &[AcceptanceCriterionRecord],
    ) -> Result<(), DbError> {
        let root_run_id = root_run_id.to_owned();
        let criteria = criteria.to_vec();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "DELETE FROM run_acceptance_criteria WHERE root_run_id=?1",
                [&root_run_id],
            )?;
            for criterion in criteria {
                if criterion.root_run_id != root_run_id {
                    return Err(DbError::Invalid(
                        "acceptance criterion root run mismatch".into(),
                    ));
                }
                tx.execute(
                    "INSERT INTO run_acceptance_criteria \
                     (criterion_id,root_run_id,ordinal,criterion_type,source_text,status, \
                      evidence_bundle_id,created_at,updated_at) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    rusqlite::params![
                        criterion.criterion_id,
                        criterion.root_run_id,
                        criterion.ordinal,
                        criterion.criterion_type,
                        criterion.source_text,
                        criterion.status,
                        criterion.evidence_bundle_id,
                        criterion.created_at,
                        criterion.updated_at,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Update a criterion only when an explicit evidence bundle is supplied.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` update fails.
    pub async fn bind_criterion_evidence(
        &self,
        criterion_id: &str,
        evidence_bundle_id: &str,
        status: &str,
    ) -> Result<bool, DbError> {
        let criterion_id = criterion_id.to_owned();
        let evidence_bundle_id = evidence_bundle_id.to_owned();
        let status = status.to_owned();
        self.with_writer(move |conn| {
            let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
            Ok(conn.execute(
                "UPDATE run_acceptance_criteria SET evidence_bundle_id=?1,status=?2,updated_at=?3 \
                 WHERE criterion_id=?4",
                rusqlite::params![evidence_bundle_id, status, now, criterion_id],
            )? > 0)
        })
        .await
    }

    /// Read one root-run workbench projection.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query fails.
    pub async fn find_workbench(
        &self,
        root_run_id: &str,
    ) -> Result<Option<WorkbenchRecord>, DbError> {
        let root_run_id = root_run_id.to_owned();
        self.with_reader(move |conn| load_workbench(conn, &root_run_id))
            .await
    }

    /// Read the latest bound root run for a session; no run returns `None`.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query fails.
    pub async fn find_current_workbench_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<WorkbenchRecord>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let root_run_id = conn
                .query_row(
                    "SELECT b.root_run_id FROM run_workbench_bindings b \
                     JOIN run_envelopes r ON r.id=b.root_run_id \
                     WHERE r.session_id=?1 AND r.parent_run_id IS NULL \
                     ORDER BY r.created_at DESC,r.id DESC LIMIT 1",
                    [session_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            root_run_id.map_or(Ok(None), |id| load_workbench(conn, &id))
        })
        .await
    }

    /// Read the complete current Workbench from one deferred `SQLite` transaction.
    ///
    /// Root Task/Run identity, internal child Runs, bound messages, active tools,
    /// model usage, artifacts, evidence, research, pending interactions, activity
    /// rows, and the event high-water all come from the same WAL snapshot. The
    /// authorization decision intentionally stays in the server layer; this
    /// repository still verifies that `session_id` names a root Session.
    ///
    /// # Errors
    /// Returns [`DbError`] when any constituent row is malformed or the snapshot
    /// cannot be read. A missing Session returns `None`; a Session without a Run
    /// returns an authoritative empty projection.
    pub async fn find_current_workbench_projection(
        &self,
        session_id: &str,
    ) -> Result<Option<CurrentWorkbenchProjection>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let tx = conn.transaction()?;
            let projection =
                load_current_workbench_projection_in_snapshot(&tx, &session_id, || {})?;
            tx.commit()?;
            Ok(projection)
        })
        .await
    }

    /// Determine whether a completed root run has a user-visible result.
    ///
    /// A result is reviewable when it has an explicit result-message binding,
    /// an artifact anywhere in the root/child run tree, or a legacy assistant
    /// text message inside the run time window. The query mirrors the reference
    /// workbench task service while keeping the decision in one read snapshot.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query fails.
    pub async fn has_reviewable_run_result(
        &self,
        session_id: &str,
        root_run_id: &str,
        started_at: &str,
        upper_bound: &str,
    ) -> Result<bool, DbError> {
        let session_id = session_id.to_owned();
        let root_run_id = root_run_id.to_owned();
        let started_at = started_at.to_owned();
        let upper_bound = upper_bound.to_owned();
        self.with_reader(move |conn| {
            conn.query_row(
                "WITH RECURSIVE run_tree(id) AS ( \
                   SELECT id FROM run_envelopes WHERE id=?1 \
                   UNION ALL \
                   SELECT child.id FROM run_envelopes child \
                   JOIN run_tree parent ON child.parent_run_id=parent.id \
                 ) \
                 SELECT CASE WHEN \
                   EXISTS(SELECT 1 FROM run_workbench_bindings \
                          WHERE root_run_id=?1 AND result_message_id IS NOT NULL) \
                   OR EXISTS(SELECT 1 FROM artifact_manifests manifest \
                             JOIN run_tree tree ON tree.id=manifest.run_id) \
                   OR EXISTS(SELECT 1 FROM messages \
                             WHERE session_id=?2 AND role='assistant' \
                               AND created_at>=?3 AND created_at<=?4 \
                               AND content_json LIKE '%\"type\":\"text\"%') \
                   THEN 1 ELSE 0 END",
                rusqlite::params![root_run_id, session_id, started_at, upper_bound],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(Into::into)
        })
        .await
    }
}

fn load_workbench(
    conn: &rusqlite::Connection,
    root_run_id: &str,
) -> Result<Option<WorkbenchRecord>, DbError> {
    let binding = conn
        .query_row(
            "SELECT root_run_id,request_message_id,result_message_id,created_at,updated_at \
             FROM run_workbench_bindings WHERE root_run_id=?1",
            [root_run_id],
            |row| {
                Ok(WorkbenchBindingRecord {
                    root_run_id: row.get(0)?,
                    request_message_id: row.get(1)?,
                    result_message_id: row.get(2)?,
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(binding) = binding else {
        return Ok(None);
    };
    let mut stmt = conn.prepare(
        "SELECT criterion_id,root_run_id,ordinal,criterion_type,source_text,status, \
         evidence_bundle_id,created_at,updated_at FROM run_acceptance_criteria \
         WHERE root_run_id=?1 ORDER BY ordinal ASC,criterion_id ASC",
    )?;
    let criteria = stmt
        .query_map([root_run_id], |row| {
            Ok(AcceptanceCriterionRecord {
                criterion_id: row.get(0)?,
                root_run_id: row.get(1)?,
                ordinal: row.get(2)?,
                criterion_type: row.get(3)?,
                source_text: row.get(4)?,
                status: row.get(5)?,
                evidence_bundle_id: row.get(6)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(WorkbenchRecord { binding, criteria }))
}

#[allow(clippy::too_many_lines)] // the point is one cohesive, non-tearing read boundary
fn load_current_workbench_projection_in_snapshot<F>(
    conn: &Connection,
    session_id: &str,
    after_snapshot_anchor: F,
) -> Result<Option<CurrentWorkbenchProjection>, DbError>
where
    F: FnOnce(),
{
    let is_root_session = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1 AND kind='root')",
        [session_id],
        |row| row.get::<_, i64>(0),
    )? != 0;
    if !is_root_session {
        return Ok(None);
    }

    let root_run = conn
        .query_row(
            "SELECT * FROM run_envelopes
             WHERE session_id=?1 AND parent_run_id IS NULL
             ORDER BY started_at DESC,id DESC LIMIT 1",
            [session_id],
            map_envelope_row,
        )
        .optional()?;
    // The first SELECT above anchors the deferred transaction. Tests use this
    // boundary to commit a complete newer state concurrently and prove that no
    // field below leaks across WAL snapshots.
    after_snapshot_anchor();
    let Some(root_run) = root_run else {
        return Ok(Some(CurrentWorkbenchProjection::empty()));
    };

    let run_tree = load_run_tree(conn, &root_run.id)?;
    let task_tree = load_task_tree(conn, &root_run.task_id)?;
    let root_task = task_tree
        .iter()
        .find(|task| task.id == root_run.task_id)
        .cloned();
    if root_task.is_none() {
        return Err(DbError::Invalid("WORKBENCH_ROOT_TASK_NOT_FOUND".to_owned()));
    }

    let messages = load_message_rows(conn, session_id)?;
    let workbench = load_workbench(conn, &root_run.id)?;
    let request_message = workbench
        .as_ref()
        .and_then(|record| {
            messages
                .iter()
                .find(|message| message.id == record.binding.request_message_id)
        })
        .cloned();
    let result_message = workbench
        .as_ref()
        .and_then(|record| record.binding.result_message_id.as_deref())
        .and_then(|id| messages.iter().find(|message| message.id == id))
        .cloned();

    let manifests = load_manifests_for_run_tree(conn, &root_run.id)?;
    let evidence = load_evidence_for_run_tree(conn, &root_run.id)?;
    let pending_actions = load_pending_actions_for_run_tree(conn, &root_run.id)?;
    let run_ids = run_tree
        .iter()
        .map(|run| run.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let activities = load_activities_by_session_paged_in_snapshot(conn, session_id, 0, 200)?
        .into_iter()
        .filter(|activity| {
            activity
                .get("run_id")
                .and_then(Value::as_str)
                .is_some_and(|run_id| run_ids.contains(run_id))
        })
        .collect();
    let research = load_research_projection(conn, &root_run.task_id)?;
    let usage = load_subtree_usage(conn, &root_run.id)?;
    let event_high_water = load_event_high_water(conn, &root_run.id)?;
    let active_tools = load_active_tools(conn, &root_run.id)?;
    let previous_delivery = if root_run.is_terminal() && root_run.status != "completed" {
        load_previous_delivery(conn, session_id, &root_run.id, &messages)?
    } else {
        None
    };

    Ok(Some(CurrentWorkbenchProjection {
        root_task,
        task_tree,
        root_run: Some(root_run),
        run_tree,
        workbench,
        request_message,
        result_message,
        manifests,
        evidence,
        pending_actions,
        activities,
        research,
        usage,
        event_high_water,
        active_tools,
        previous_delivery,
    }))
}

fn load_run_tree(conn: &Connection, root_run_id: &str) -> Result<Vec<RunEnvelopeView>, DbError> {
    let mut statement = conn.prepare(
        "WITH RECURSIVE run_tree(id,depth) AS (
             SELECT id,0 FROM run_envelopes WHERE id=?1
             UNION ALL
             SELECT child.id,parent.depth+1 FROM run_envelopes child
             JOIN run_tree parent ON child.parent_run_id=parent.id
         )
         SELECT run.* FROM run_envelopes run
         JOIN run_tree tree ON tree.id=run.id
         ORDER BY tree.depth,run.started_at,run.id",
    )?;
    statement
        .query_map([root_run_id], map_envelope_row)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn load_task_tree(
    conn: &Connection,
    root_task_id: &str,
) -> Result<Vec<RuntimeTaskRecord>, DbError> {
    let sql = format!(
        "SELECT {RUNTIME_TASK_COLUMNS} FROM tasks WHERE root_task_id=?1
         ORDER BY CASE WHEN parent_task_id IS NULL THEN 0 ELSE 1 END,
                  created_at,ordinal,id"
    );
    let mut statement = conn.prepare(&sql)?;
    statement
        .query_map([root_task_id], map_runtime_task)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn load_manifests_for_run_tree(
    conn: &Connection,
    root_run_id: &str,
) -> Result<Vec<ArtifactManifestRecord>, DbError> {
    let ids = {
        let mut statement = conn.prepare(
            "WITH RECURSIVE run_tree(id) AS (
                 SELECT id FROM run_envelopes WHERE id=?1
                 UNION ALL
                 SELECT child.id FROM run_envelopes child
                 JOIN run_tree parent ON child.parent_run_id=parent.id
             )
             SELECT manifest.manifest_id FROM artifact_manifests manifest
             JOIN run_tree tree ON tree.id=manifest.run_id
             ORDER BY manifest.created_at,manifest.manifest_id",
        )?;
        statement
            .query_map([root_run_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    ids.into_iter()
        .map(|id| {
            load_manifest(conn, &id)?
                .ok_or_else(|| DbError::Invalid(format!("WORKBENCH_MANIFEST_DISAPPEARED:{id}")))
        })
        .collect()
}

fn load_evidence_for_run_tree(
    conn: &Connection,
    root_run_id: &str,
) -> Result<Vec<EvidenceBundleRecord>, DbError> {
    let ids = {
        let mut statement = conn.prepare(
            "WITH RECURSIVE run_tree(id) AS (
                 SELECT id FROM run_envelopes WHERE id=?1
                 UNION ALL
                 SELECT child.id FROM run_envelopes child
                 JOIN run_tree parent ON child.parent_run_id=parent.id
             )
             SELECT evidence.bundle_id FROM evidence_bundles evidence
             JOIN run_tree tree ON tree.id=evidence.run_id
             ORDER BY evidence.created_at DESC,evidence.bundle_id DESC",
        )?;
        statement
            .query_map([root_run_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    ids.into_iter()
        .map(|id| {
            load_bundle(conn, &id)?
                .ok_or_else(|| DbError::Invalid(format!("WORKBENCH_EVIDENCE_DISAPPEARED:{id}")))
        })
        .collect()
}

fn load_subtree_usage(
    conn: &Connection,
    root_run_id: &str,
) -> Result<WorkbenchSubtreeUsage, DbError> {
    conn.query_row(
        "WITH RECURSIVE run_tree(id) AS (
             SELECT id FROM run_envelopes WHERE id=?1
             UNION ALL
             SELECT child.id FROM run_envelopes child
             JOIN run_tree parent ON child.parent_run_id=parent.id
         )
         SELECT COALESCE(SUM(COALESCE(call.input_tokens,0)),0),
                COALESCE(SUM(COALESCE(call.output_tokens,0)),0),
                COALESCE(SUM(COALESCE(call.cache_read_tokens,0)),0),
                COALESCE(SUM(COALESCE(call.cache_create_tokens,0)),0),
                COALESCE(SUM(COALESCE(call.cost_nanos_usd,0)),0),
                CASE WHEN COUNT(call.call_id)=0 THEN 1 ELSE MIN(call.usage_complete) END
         FROM run_tree tree LEFT JOIN llm_calls call ON call.run_id=tree.id",
        [root_run_id],
        |row| {
            Ok(WorkbenchSubtreeUsage {
                input_tokens: row.get(0)?,
                output_tokens: row.get(1)?,
                cache_read_tokens: row.get(2)?,
                cache_create_tokens: row.get(3)?,
                cost_nanos_usd: row.get(4)?,
                complete: row.get::<_, i64>(5)? != 0,
            })
        },
    )
    .map_err(Into::into)
}

fn load_event_high_water(conn: &Connection, root_run_id: &str) -> Result<i64, DbError> {
    conn.query_row(
        "WITH RECURSIVE run_tree(id) AS (
             SELECT id FROM run_envelopes WHERE id=?1
             UNION ALL
             SELECT child.id FROM run_envelopes child
             JOIN run_tree parent ON child.parent_run_id=parent.id
         )
         SELECT COALESCE(MAX(event.id),0) FROM run_event_log event
         JOIN run_tree tree ON tree.id=event.run_id",
        [root_run_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn load_active_tools(
    conn: &Connection,
    root_run_id: &str,
) -> Result<Vec<WorkbenchActiveTool>, DbError> {
    let mut statement = conn.prepare(
        "WITH RECURSIVE run_tree(id) AS (
             SELECT id FROM run_envelopes WHERE id=?1
             UNION ALL
             SELECT child.id FROM run_envelopes child
             JOIN run_tree parent ON child.parent_run_id=parent.id
         )
         SELECT invocation.invocation_id,invocation.task_id,invocation.run_id,
                invocation.tool_use_id,invocation.tool_name,invocation.status,
                invocation.input_json,invocation.side_effect_class,
                invocation.cleanup_status,invocation.started_at,invocation.created_at
         FROM tool_invocations invocation
         JOIN run_tree tree ON tree.id=invocation.run_id
         WHERE invocation.status IN ('preparing','queued','running')
         ORDER BY invocation.created_at,invocation.invocation_id",
    )?;
    let rows = statement
        .query_map([root_run_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(
            |(
                invocation_id,
                task_id,
                run_id,
                tool_use_id,
                tool_name,
                status,
                input_json,
                side_effect_class,
                cleanup_status,
                started_at,
                created_at,
            )| {
                let input = input_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?;
                Ok(WorkbenchActiveTool {
                    invocation_id,
                    task_id,
                    run_id,
                    tool_use_id,
                    tool_name,
                    status,
                    input,
                    side_effect_class,
                    cleanup_status,
                    started_at,
                    created_at,
                })
            },
        )
        .collect()
}

#[derive(Debug)]
struct PendingInteractionRow {
    interaction_id: String,
    correlation_key: String,
    session_id: String,
    run_id: String,
    kind: String,
    status: String,
    prompt_json: String,
    allowed_decisions_json: String,
    scope_options_json: String,
    response_json: Option<String>,
    created_at: String,
    delivery_window_ends_at: String,
    received_at: Option<String>,
    decision_deadline_at: Option<String>,
    decided_at: Option<String>,
    terminal_reason: Option<String>,
    source: String,
    child_session_id: Option<String>,
    delivery_generation: i64,
    dispatch_attempts: i64,
    authorization_context_json: Option<String>,
    version: i64,
}

fn load_pending_actions_for_run_tree(
    conn: &Connection,
    root_run_id: &str,
) -> Result<Vec<zk_protocol::InteractionView>, DbError> {
    let rows = {
        let mut statement = conn.prepare(
            "WITH RECURSIVE run_tree(id) AS (
                 SELECT id FROM run_envelopes WHERE id=?1
                 UNION ALL
                 SELECT child.id FROM run_envelopes child
                 JOIN run_tree parent ON child.parent_run_id=parent.id
             )
             SELECT interaction.interaction_id,interaction.correlation_key,
                    interaction.session_id,interaction.run_id,interaction.type,
                    interaction.status,interaction.prompt_json,
                    interaction.allowed_decisions_json,interaction.scope_options_json,
                    interaction.response_json,interaction.created_at,
                    interaction.delivery_window_ends_at,interaction.received_at,
                    interaction.decision_deadline_at,interaction.decided_at,
                    interaction.terminal_reason,interaction.source,
                    interaction.child_session_id,interaction.delivery_generation,
                    interaction.dispatch_attempts,interaction.authorization_context_json,
                    interaction.version
             FROM interaction_requests interaction
             JOIN run_tree tree ON tree.id=interaction.run_id
             WHERE interaction.status='pending'
             ORDER BY interaction.created_at,interaction.interaction_id",
        )?;
        statement
            .query_map([root_run_id], |row| {
                Ok(PendingInteractionRow {
                    interaction_id: row.get(0)?,
                    correlation_key: row.get(1)?,
                    session_id: row.get(2)?,
                    run_id: row.get(3)?,
                    kind: row.get(4)?,
                    status: row.get(5)?,
                    prompt_json: row.get(6)?,
                    allowed_decisions_json: row.get(7)?,
                    scope_options_json: row.get(8)?,
                    response_json: row.get(9)?,
                    created_at: row.get(10)?,
                    delivery_window_ends_at: row.get(11)?,
                    received_at: row.get(12)?,
                    decision_deadline_at: row.get(13)?,
                    decided_at: row.get(14)?,
                    terminal_reason: row.get(15)?,
                    source: row.get(16)?,
                    child_session_id: row.get(17)?,
                    delivery_generation: row.get(18)?,
                    dispatch_attempts: row.get(19)?,
                    authorization_context_json: row.get(20)?,
                    version: row.get(21)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    rows.into_iter().map(pending_interaction_view).collect()
}

fn pending_interaction_view(
    row: PendingInteractionRow,
) -> Result<zk_protocol::InteractionView, DbError> {
    let prompt = serde_json::from_str(&row.prompt_json)?;
    let decisions = serde_json::from_str(&row.allowed_decisions_json)?;
    let scopes = serde_json::from_str(&row.scope_options_json)?;
    let response = row
        .response_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?;
    let mut protocol_version = 2;
    let mut operation_hash = None;
    let mut actor_run_id = None;
    let mut actor_type = None;
    let mut options = Vec::new();
    if row.kind == "permission" {
        let context: Value = row
            .authorization_context_json
            .as_deref()
            .ok_or_else(|| DbError::Invalid("PERMISSION_PROTOCOL_MISMATCH".to_owned()))
            .and_then(|raw| serde_json::from_str(raw).map_err(Into::into))?;
        if context.get("protocolVersion").and_then(Value::as_i64) != Some(3) {
            return Err(DbError::Invalid("PERMISSION_PROTOCOL_MISMATCH".to_owned()));
        }
        protocol_version = 3;
        operation_hash = context
            .get("operationHash")
            .and_then(Value::as_str)
            .map(str::to_owned);
        actor_run_id = context
            .pointer("/subject/currentRunId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let root_actor = context
            .pointer("/subject/rootRunId")
            .and_then(Value::as_str);
        actor_type = actor_run_id.as_deref().map(|current| {
            if root_actor == Some(current) {
                "direct"
            } else {
                "descendant"
            }
            .to_owned()
        });
        options = context
            .get("options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
    }
    let source = actor_type.clone().unwrap_or(row.source);
    Ok(zk_protocol::InteractionView {
        interaction_id: row.interaction_id,
        protocol_version: Some(protocol_version),
        correlation_key: Some(row.correlation_key),
        session_id: Some(row.session_id),
        run_id: Some(row.run_id),
        interaction_type: Some(row.kind),
        status: Some(row.status),
        prompt: Some(prompt),
        allowed_decisions: Some(decisions),
        scope_options: Some(scopes),
        response,
        source: Some(source),
        child_session_id: row.child_session_id,
        actor_run_id,
        actor_type,
        delivery_generation: Some(row.delivery_generation),
        dispatch_attempts: Some(row.dispatch_attempts),
        created_at: flex_epoch(&row.created_at),
        received_at: row.received_at.as_deref().and_then(flex_epoch),
        decision_deadline_at: row.decision_deadline_at.as_deref().and_then(flex_epoch),
        delivery_window_ends_at: flex_epoch(&row.delivery_window_ends_at),
        decided_at: row.decided_at.as_deref().and_then(flex_epoch),
        terminal_reason: row.terminal_reason,
        version: Some(row.version),
        server_now: Some(crate::time::now_millis()),
        operation_hash,
        options: Some(options),
    })
}

fn flex_epoch(value: &str) -> Option<zk_protocol::FlexEpoch> {
    crate::time::parse_rfc3339_millis(value).map(zk_protocol::FlexEpoch::from_millis)
}

fn load_previous_delivery(
    conn: &Connection,
    session_id: &str,
    current_root_run_id: &str,
    messages: &[MessageRecord],
) -> Result<Option<PreviousWorkbenchDelivery>, DbError> {
    let candidates = {
        let mut statement = conn.prepare(
            "SELECT * FROM run_envelopes
             WHERE session_id=?1 AND parent_run_id IS NULL AND id<>?2 AND status='completed'
             ORDER BY started_at DESC,id DESC LIMIT 200",
        )?;
        statement
            .query_map(params![session_id, current_root_run_id], map_envelope_row)?
            .collect::<Result<Vec<_>, _>>()?
    };
    for candidate in candidates {
        let manifests = load_manifests_for_run_tree(conn, &candidate.id)?;
        if !manifests.iter().any(|manifest| {
            manifest
                .entries
                .iter()
                .any(|entry| entry.operation != "deleted")
        }) {
            continue;
        }
        let result_message = load_workbench(conn, &candidate.id)?
            .and_then(|record| record.binding.result_message_id)
            .and_then(|id| messages.iter().find(|message| message.id == id).cloned());
        return Ok(Some(PreviousWorkbenchDelivery {
            root_run: candidate,
            result_message,
            manifests,
        }));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactEntryRecord, ArtifactManifestRecord, CreateTaskWithRun, EvidenceBundleRecord,
        EvidenceItemRecord, EvidenceOrigin, NewToolInvocation,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::too_many_lines)] // complete WAL snapshot race fixture is intentionally cohesive
    async fn compound_projection_does_not_tear_across_a_concurrent_commit() {
        let directory = std::env::temp_dir().join(format!(
            "zkcode-workbench-snapshot-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).expect("create fixture directory");
        let database_path = directory.join("data.db");
        let db = Db::open(&database_path).expect("open file database with WAL readers");
        let session = db
            .create_session("model", directory.to_str().expect("utf8 directory"))
            .await
            .expect("root session");
        let root = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: uuid::Uuid::new_v4().to_string(),
                run_id: uuid::Uuid::new_v4().to_string(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id.clone(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "snapshot fixture".to_owned(),
                prompt: Some("prove a stable read".to_owned()),
                task_type: "agent".to_owned(),
                model: "model".to_owned(),
                working_dir: directory.to_string_lossy().into_owned(),
                execution_config_json: r#"{"isolation":"readOnly"}"#.to_owned(),
                startup_epoch: 1,
            })
            .await
            .expect("root task and run");

        let snapshot_arrived = std::sync::Arc::new(std::sync::Barrier::new(2));
        let release_snapshot = std::sync::Arc::new(std::sync::Barrier::new(2));
        let reader_db = db.clone();
        let reader_session_id = session.id.clone();
        let reader_arrived = std::sync::Arc::clone(&snapshot_arrived);
        let reader_release = std::sync::Arc::clone(&release_snapshot);
        let reader = tokio::spawn(async move {
            reader_db
                .with_reader(move |conn| {
                    let transaction = conn.transaction()?;
                    let projection = load_current_workbench_projection_in_snapshot(
                        &transaction,
                        &reader_session_id,
                        || {
                            reader_arrived.wait();
                            reader_release.wait();
                        },
                    )?;
                    transaction.commit()?;
                    Ok(projection)
                })
                .await
        });

        let main_arrived = std::sync::Arc::clone(&snapshot_arrived);
        tokio::task::spawn_blocking(move || main_arrived.wait())
            .await
            .expect("snapshot arrival barrier");

        let request = db
            .append_message(
                &session.id,
                crate::NewMessage {
                    role: crate::MessageRole::User,
                    content: vec![crate::StoredBlock::Text {
                        text: "committed after snapshot".to_owned(),
                    }],
                    stop_reason: None,
                    input_tokens: 0,
                    output_tokens: 0,
                },
            )
            .await
            .expect("concurrent request message");
        let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
        db.save_workbench_binding(&WorkbenchBindingRecord {
            root_run_id: root.run_id.clone(),
            request_message_id: request.id,
            result_message_id: None,
            created_at: now.clone(),
            updated_at: now,
        })
        .await
        .expect("concurrent workbench binding");
        let invocation_id = uuid::Uuid::new_v4().to_string();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation_id.clone(),
            task_id: root.task.id.clone(),
            run_id: root.run_id.clone(),
            tool_use_id: "snapshot-tool".to_owned(),
            tool_name: "Read".to_owned(),
            input_json: Some(r#"{"filePath":"README.md"}"#.to_owned()),
            side_effect_class: "read".to_owned(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .expect("concurrent active tool");
        db.append_run_event(
            &root.run_id,
            "snapshot_concurrent_commit",
            Some("snapshot-tool"),
            &serde_json::json!({"committed": true}),
        )
        .await
        .expect("concurrent run event");

        let main_release = std::sync::Arc::clone(&release_snapshot);
        tokio::task::spawn_blocking(move || main_release.wait())
            .await
            .expect("snapshot release barrier");
        let frozen = reader
            .await
            .expect("reader task")
            .expect("snapshot query")
            .expect("root session projection");
        assert!(frozen.workbench.is_none());
        assert!(frozen.request_message.is_none());
        assert!(frozen.active_tools.is_empty());

        let latest = db
            .find_current_workbench_projection(&session.id)
            .await
            .expect("latest projection")
            .expect("root session projection");
        assert!(latest.workbench.is_some());
        assert!(latest.request_message.is_some());
        assert_eq!(latest.active_tools.len(), 1);
        assert_eq!(latest.active_tools[0].invocation_id, invocation_id);
        assert!(latest.event_high_water > frozen.event_high_water);

        drop(db);
        std::fs::remove_dir_all(directory).expect("remove fixture directory");
    }

    #[tokio::test]
    async fn workbench_binding_and_criteria_round_trip() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/workbench")
            .await
            .expect("session");
        let user = db
            .append_message(
                &session.id,
                crate::NewMessage {
                    role: crate::MessageRole::User,
                    content: vec![crate::StoredBlock::Text {
                        text: "ship".into(),
                    }],
                    stop_reason: None,
                    input_tokens: 0,
                    output_tokens: 0,
                },
            )
            .await
            .expect("message");
        db.start_run("root-run", &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        let now = "2026-08-21T00:00:00.000000Z".to_owned();
        db.save_workbench_binding(&WorkbenchBindingRecord {
            root_run_id: "root-run".into(),
            request_message_id: user.id,
            result_message_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        })
        .await
        .expect("binding");
        db.replace_acceptance_criteria(
            "root-run",
            &[AcceptanceCriterionRecord {
                criterion_id: "criterion-1".into(),
                root_run_id: "root-run".into(),
                ordinal: 0,
                criterion_type: "business".into(),
                source_text: "tests pass".into(),
                status: "not_verified".into(),
                evidence_bundle_id: None,
                created_at: now.clone(),
                updated_at: now,
            }],
        )
        .await
        .expect("criteria");
        let loaded = db
            .find_current_workbench_for_session(&session.id)
            .await
            .expect("current")
            .expect("workbench");
        assert_eq!(loaded.binding.root_run_id, "root-run");
        assert_eq!(loaded.criteria[0].status, "not_verified");
        let assistant = db
            .append_message(
                &session.id,
                crate::NewMessage {
                    role: crate::MessageRole::Assistant,
                    content: vec![crate::StoredBlock::Text {
                        text: "shipped".into(),
                    }],
                    stop_reason: Some("end_turn".into()),
                    input_tokens: 0,
                    output_tokens: 0,
                },
            )
            .await
            .expect("assistant message");
        assert!(
            db.bind_workbench_result("root-run", &assistant.id)
                .await
                .expect("result binding")
        );
        let loaded = db
            .find_workbench("root-run")
            .await
            .expect("load")
            .expect("workbench");
        assert_eq!(
            loaded.binding.result_message_id.as_deref(),
            Some(assistant.id.as_str())
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one end-to-end reopen scenario spans all three durable records
    async fn evidence_artifact_and_workbench_survive_database_reopen() {
        let dir =
            std::env::temp_dir().join(format!("zkcode-wp06-restart-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("data.db");
        let session_id;
        {
            let db = Db::open(&path).expect("open database");
            let session = db
                .create_session("model", dir.to_str().expect("utf8 path"))
                .await
                .expect("session");
            session_id = session.id.clone();
            let user = db
                .append_message(
                    &session.id,
                    crate::NewMessage {
                        role: crate::MessageRole::User,
                        content: vec![crate::StoredBlock::Text {
                            text: "persist delivery".into(),
                        }],
                        stop_reason: None,
                        input_tokens: 0,
                        output_tokens: 0,
                    },
                )
                .await
                .expect("message");
            db.start_run("restart-run", &session.id, None, Some("query"), "model")
                .await
                .expect("run");
            let now = "2026-08-22T00:00:00.000000Z".to_owned();
            db.initialize_workbench(
                &WorkbenchBindingRecord {
                    root_run_id: "restart-run".into(),
                    request_message_id: user.id,
                    result_message_id: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                &[AcceptanceCriterionRecord {
                    criterion_id: "restart-criterion".into(),
                    root_run_id: "restart-run".into(),
                    ordinal: 0,
                    criterion_type: "business".into(),
                    source_text: "persist delivery".into(),
                    status: "not_verified".into(),
                    evidence_bundle_id: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                }],
            )
            .await
            .expect("workbench");
            db.save_evidence_bundle(&EvidenceBundleRecord {
                bundle_id: "restart-evidence".into(),
                session_id: session.id.clone(),
                agent_id: None,
                kind: "test".into(),
                claim: Some("persist delivery".into()),
                origin: EvidenceOrigin::Human,
                producer_invocation_id: None,
                verdict: "verified".into(),
                created_at: now.clone(),
                run_id: Some("restart-run".into()),
                items: vec![EvidenceItemRecord {
                    id: "restart-item".into(),
                    producer_invocation_id: None,
                    item_type: "log".into(),
                    summary: Some("ok".into()),
                    blob_sha256: None,
                    meta: None,
                    sort_order: 0,
                }],
            })
            .await
            .expect("evidence");
            db.save_artifact_manifest(&ArtifactManifestRecord {
                manifest_id: "restart-manifest".into(),
                run_id: "restart-run".into(),
                session_id: session.id,
                workspace_root: dir.to_string_lossy().into_owned(),
                state: "verified".into(),
                created_at: now.clone(),
                updated_at: now.clone(),
                entries: vec![ArtifactEntryRecord {
                    artifact_id: "restart-artifact".into(),
                    tool_use_id: "restart-tool".into(),
                    producer_invocation_id: None,
                    canonical_path: dir.join("result.txt").to_string_lossy().into_owned(),
                    operation: "created".into(),
                    state: "integrity_verified".into(),
                    sealed_hash: Some("abc".into()),
                    actual_hash: Some("abc".into()),
                    file_size: Some(3),
                    required_validator_id: None,
                    validator_result: None,
                    failure_code: None,
                    created_at: now.clone(),
                    updated_at: now,
                }],
            })
            .await
            .expect("artifact");
        }

        let reopened = Db::open(&path).expect("reopen database");
        assert!(
            reopened
                .find_current_workbench_for_session(&session_id)
                .await
                .expect("workbench query")
                .is_some()
        );
        assert_eq!(
            reopened
                .find_evidence_by_run("restart-run")
                .await
                .expect("evidence query")
                .len(),
            1
        );
        assert_eq!(
            reopened
                .find_artifact_manifest_by_run("restart-run")
                .await
                .expect("artifact query")
                .expect("manifest")
                .state,
            "verified"
        );
        drop(reopened);
        std::fs::remove_dir_all(&dir).expect("remove isolated restart directory");
    }
}
