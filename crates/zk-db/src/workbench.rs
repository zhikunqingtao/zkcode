//! Durable workbench bindings and acceptance criteria repository.

use serde::{Deserialize, Serialize};

use crate::{Db, DbError};

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

use rusqlite::OptionalExtension;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactEntryRecord, ArtifactManifestRecord, EvidenceBundleRecord, EvidenceItemRecord,
    };

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
                verdict: "verified".into(),
                created_at: now.clone(),
                run_id: Some("restart-run".into()),
                items: vec![EvidenceItemRecord {
                    id: "restart-item".into(),
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
