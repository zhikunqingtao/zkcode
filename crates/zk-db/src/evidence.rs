//! Evidence bundle repository backed by `evidence_bundles` and `evidence_items`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Db, DbError};

use rusqlite::OptionalExtension;

/// Provenance of an evidence conclusion.
///
/// A model assertion is retained for traceability but is never accepted as a
/// passing verification. Machine evidence is produced by an executed verifier;
/// human evidence is the result of an explicit review decision.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EvidenceOrigin {
    /// Deterministic output from an executed verifier or test harness.
    Machine,
    /// A claim emitted by a language model; never sufficient for a passing verdict.
    ModelAssertion,
    /// An explicit reviewer decision.
    Human,
}

impl EvidenceOrigin {
    /// Database representation shared with the final schema CHECK constraint.
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Machine => "machine",
            Self::ModelAssertion => "modelAssertion",
            Self::Human => "human",
        }
    }

    pub(crate) fn from_db(value: &str) -> Result<Self, DbError> {
        match value {
            "machine" => Ok(Self::Machine),
            "modelAssertion" => Ok(Self::ModelAssertion),
            "human" => Ok(Self::Human),
            _ => Err(DbError::Invalid(format!(
                "unknown evidence origin: {value}"
            ))),
        }
    }

    /// Whether this origin may satisfy an acceptance criterion.
    #[must_use]
    pub const fn can_verify(self) -> bool {
        matches!(self, Self::Machine | Self::Human)
    }
}

/// One ordered evidence item.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceItemRecord {
    /// Item identifier.
    pub id: String,
    /// Physical invocation that produced this exact observation.
    pub producer_invocation_id: Option<String>,
    /// Item kind such as log, screenshot or assertion.
    #[serde(rename = "type")]
    pub item_type: String,
    /// Redacted summary.
    pub summary: Option<String>,
    /// Optional content-addressed blob digest.
    pub blob_sha256: Option<String>,
    /// Additional structured metadata.
    pub meta: Option<Value>,
    /// Stable display order.
    pub sort_order: i64,
}

/// Evidence bundle and its ordered items.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceBundleRecord {
    /// Bundle identifier.
    pub bundle_id: String,
    /// Owning session.
    pub session_id: String,
    /// Optional producer agent.
    pub agent_id: Option<String>,
    /// Evidence category.
    pub kind: String,
    /// Redacted claim.
    pub claim: Option<String>,
    /// Whether the conclusion came from a machine check, a model assertion or a human review.
    pub origin: EvidenceOrigin,
    /// Physical tool invocation that produced the evidence, when applicable.
    pub producer_invocation_id: Option<String>,
    /// Verification verdict.
    pub verdict: String,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// Associated run.
    pub run_id: Option<String>,
    /// Ordered evidence items.
    pub items: Vec<EvidenceItemRecord>,
}

/// One append-only change to the effective verdict of an immutable bundle.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceVerdictEventRecord {
    /// Event identifier.
    pub event_id: String,
    /// Immutable bundle whose read projection changes.
    pub bundle_id: String,
    /// Bundle-local monotonic version, starting at one.
    pub version: i64,
    /// Immediately preceding event, if this is not the first override.
    pub supersedes_event_id: Option<String>,
    /// Effective verdict after this event.
    pub verdict: String,
    /// Actor that created the override (`human`, `artifactIntegrity`, ...).
    pub origin: String,
    /// Evidence origin exposed by the current read projection.
    pub effective_origin: EvidenceOrigin,
    /// Stable, non-secret reason code.
    pub reason: String,
    /// RFC 3339 event time.
    pub created_at: String,
}

impl Db {
    /// Insert one immutable bundle in a transaction.
    ///
    /// Retrying the exact same request is idempotent. Reusing a bundle id with
    /// different metadata or items fails with `EVIDENCE_IMMUTABLE_MISMATCH`.
    ///
    /// # Errors
    /// Returns [`DbError`] when metadata serialization or the `SQLite` transaction fails.
    pub async fn save_evidence_bundle(&self, bundle: &EvidenceBundleRecord) -> Result<(), DbError> {
        let mut bundle = bundle.clone();
        canonicalize_items(&mut bundle.items);
        validate_bundle(&bundle)?;
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            if let Some(existing) = load_bundle_base(&tx, &bundle.bundle_id)? {
                if existing == bundle {
                    tx.commit()?;
                    return Ok(());
                }
                return Err(DbError::Invalid(
                    "EVIDENCE_IMMUTABLE_MISMATCH".to_owned(),
                ));
            }
            tx.execute(
                "INSERT INTO evidence_bundles \
                 (bundle_id,session_id,agent_id,kind,claim,origin,producer_invocation_id,verdict,created_at,run_id) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                rusqlite::params![
                    &bundle.bundle_id,
                    &bundle.session_id,
                    &bundle.agent_id,
                    &bundle.kind,
                    &bundle.claim,
                    bundle.origin.as_db(),
                    &bundle.producer_invocation_id,
                    &bundle.verdict,
                    &bundle.created_at,
                    &bundle.run_id,
                ],
            )?;
            for item in &bundle.items {
                let meta_json = item.meta.as_ref().map(serde_json::to_string).transpose()?;
                tx.execute(
                    "INSERT INTO evidence_items \
                     (id,bundle_id,producer_invocation_id,type,summary,blob_sha256,meta_json,sort_order) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    rusqlite::params![
                        &item.id,
                        &bundle.bundle_id,
                        &item.producer_invocation_id,
                        &item.item_type,
                        &item.summary,
                        &item.blob_sha256,
                        meta_json,
                        item.sort_order,
                    ],
                )?;
            }
            project_machine_verification_in_current_write(&tx, &bundle)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Read a bundle and its items by primary key.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query or stored metadata decoding fails.
    pub async fn find_evidence_bundle(
        &self,
        bundle_id: &str,
    ) -> Result<Option<EvidenceBundleRecord>, DbError> {
        let bundle_id = bundle_id.to_owned();
        self.with_reader(move |conn| load_bundle(conn, &bundle_id))
            .await
    }

    /// List all bundles owned by a session, newest first.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query or stored metadata decoding fails.
    pub async fn find_evidence_by_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<EvidenceBundleRecord>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT bundle_id FROM evidence_bundles WHERE session_id=?1 \
                 ORDER BY created_at DESC, bundle_id DESC",
            )?;
            let ids = stmt
                .query_map([session_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            ids.into_iter()
                .map(|id| {
                    load_bundle(conn, &id)?.ok_or_else(|| {
                        DbError::Invalid(format!("evidence bundle disappeared: {id}"))
                    })
                })
                .collect()
        })
        .await
    }

    /// List bundles associated with one run.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query or stored metadata decoding fails.
    pub async fn find_evidence_by_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<EvidenceBundleRecord>, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT bundle_id FROM evidence_bundles WHERE run_id=?1 \
                 ORDER BY created_at DESC, bundle_id DESC",
            )?;
            let ids = stmt
                .query_map([run_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            ids.into_iter()
                .map(|id| {
                    load_bundle(conn, &id)?.ok_or_else(|| {
                        DbError::Invalid(format!("evidence bundle disappeared: {id}"))
                    })
                })
                .collect()
        })
        .await
    }

    /// Read the complete append-only verdict history for a bundle.
    ///
    /// # Errors
    /// Returns [`DbError`] when the query or stored origin is invalid.
    pub async fn find_evidence_verdict_events(
        &self,
        bundle_id: &str,
    ) -> Result<Vec<EvidenceVerdictEventRecord>, DbError> {
        let bundle_id = bundle_id.to_owned();
        self.with_reader(move |conn| load_verdict_events(conn, &bundle_id))
            .await
    }

    /// Record an explicit human review verdict.
    ///
    /// # Errors
    /// Returns [`DbError`] when the append-only review write fails.
    pub async fn update_evidence_verdict(
        &self,
        bundle_id: &str,
        verdict: &str,
    ) -> Result<bool, DbError> {
        let bundle_id = bundle_id.to_owned();
        let verdict = verdict.to_owned();
        if !matches!(verdict.as_str(), "verified" | "failed" | "inconclusive") {
            return Err(DbError::Invalid(
                "EVIDENCE_REVIEW_VERDICT_INVALID".to_owned(),
            ));
        }
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            if load_bundle_base(&tx, &bundle_id)?.is_none() {
                tx.commit()?;
                return Ok(false);
            }
            append_evidence_verdict_event_in_current_write(
                &tx,
                &bundle_id,
                &verdict,
                "human",
                EvidenceOrigin::Human,
                "human_review",
                &crate::time::format_rfc3339_micros(crate::time::now_millis()),
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
    }
}

pub(crate) fn load_bundle(
    conn: &rusqlite::Connection,
    bundle_id: &str,
) -> Result<Option<EvidenceBundleRecord>, DbError> {
    let Some(mut bundle) = load_bundle_base(conn, bundle_id)? else {
        return Ok(None);
    };
    if let Some(event) = load_latest_verdict_event(conn, bundle_id)? {
        bundle.verdict = event.verdict;
        bundle.origin = event.effective_origin;
    }
    Ok(Some(bundle))
}

fn load_bundle_base(
    conn: &rusqlite::Connection,
    bundle_id: &str,
) -> Result<Option<EvidenceBundleRecord>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT bundle_id,session_id,agent_id,kind,claim,origin,producer_invocation_id,verdict,created_at,run_id \
         FROM evidence_bundles WHERE bundle_id=?1",
    )?;
    let mut rows = stmt.query([bundle_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let mut bundle = EvidenceBundleRecord {
        bundle_id: row.get(0)?,
        session_id: row.get(1)?,
        agent_id: row.get(2)?,
        kind: row.get(3)?,
        claim: row.get(4)?,
        origin: EvidenceOrigin::from_db(&row.get::<_, String>(5)?)?,
        producer_invocation_id: row.get(6)?,
        verdict: row.get(7)?,
        created_at: row.get(8)?,
        run_id: row.get(9)?,
        items: Vec::new(),
    };
    drop(rows);
    drop(stmt);
    let mut item_stmt = conn.prepare(
        "SELECT id,producer_invocation_id,type,summary,blob_sha256,meta_json,sort_order FROM evidence_items \
         WHERE bundle_id=?1 ORDER BY sort_order ASC,id ASC",
    )?;
    let rows = item_stmt.query_map([bundle_id], |row| {
        let meta_json: Option<String> = row.get(5)?;
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            meta_json,
            row.get::<_, i64>(6)?,
        ))
    })?;
    for row in rows {
        let (id, producer_invocation_id, item_type, summary, blob_sha256, meta_json, sort_order) =
            row?;
        bundle.items.push(EvidenceItemRecord {
            id,
            producer_invocation_id,
            item_type,
            summary,
            blob_sha256,
            meta: meta_json.as_deref().map(serde_json::from_str).transpose()?,
            sort_order,
        });
    }
    canonicalize_items(&mut bundle.items);
    Ok(Some(bundle))
}

fn canonicalize_items(items: &mut [EvidenceItemRecord]) {
    items.sort_by(|left, right| {
        left.sort_order
            .cmp(&right.sort_order)
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn load_latest_verdict_event(
    conn: &rusqlite::Connection,
    bundle_id: &str,
) -> Result<Option<EvidenceVerdictEventRecord>, DbError> {
    conn.query_row(
        "SELECT event_id,bundle_id,version,supersedes_event_id,verdict,origin, \
                effective_origin,reason,created_at \
         FROM evidence_verdict_events WHERE bundle_id=?1 \
         ORDER BY version DESC LIMIT 1",
        [bundle_id],
        map_verdict_event,
    )
    .optional()
    .map_err(Into::into)
}

fn load_verdict_events(
    conn: &rusqlite::Connection,
    bundle_id: &str,
) -> Result<Vec<EvidenceVerdictEventRecord>, DbError> {
    let mut statement = conn.prepare(
        "SELECT event_id,bundle_id,version,supersedes_event_id,verdict,origin, \
                effective_origin,reason,created_at \
         FROM evidence_verdict_events WHERE bundle_id=?1 ORDER BY version ASC",
    )?;
    statement
        .query_map([bundle_id], map_verdict_event)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn map_verdict_event(
    row: &rusqlite::Row<'_>,
) -> Result<EvidenceVerdictEventRecord, rusqlite::Error> {
    let effective_origin = row.get::<_, String>(6)?;
    let effective_origin = EvidenceOrigin::from_db(&effective_origin).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(EvidenceVerdictEventRecord {
        event_id: row.get(0)?,
        bundle_id: row.get(1)?,
        version: row.get(2)?,
        supersedes_event_id: row.get(3)?,
        verdict: row.get(4)?,
        origin: row.get(5)?,
        effective_origin,
        reason: row.get(7)?,
        created_at: row.get(8)?,
    })
}

pub(crate) fn append_evidence_verdict_event_in_current_write(
    conn: &rusqlite::Connection,
    bundle_id: &str,
    verdict: &str,
    origin: &str,
    effective_origin: EvidenceOrigin,
    reason: &str,
    created_at: &str,
) -> Result<EvidenceVerdictEventRecord, DbError> {
    validate_verdict(verdict)?;
    if !matches!(origin, "human" | "artifactIntegrity" | "machine" | "system") {
        return Err(DbError::Invalid(
            "EVIDENCE_VERDICT_EVENT_ORIGIN_INVALID".to_owned(),
        ));
    }
    let latest = load_latest_verdict_event(conn, bundle_id)?;
    let version = latest.as_ref().map_or(1, |event| event.version + 1);
    let event = EvidenceVerdictEventRecord {
        event_id: uuid::Uuid::new_v4().to_string(),
        bundle_id: bundle_id.to_owned(),
        version,
        supersedes_event_id: latest.map(|event| event.event_id),
        verdict: verdict.to_owned(),
        origin: origin.to_owned(),
        effective_origin,
        reason: reason.to_owned(),
        created_at: created_at.to_owned(),
    };
    conn.execute(
        "INSERT INTO evidence_verdict_events \
         (event_id,bundle_id,version,supersedes_event_id,verdict,origin,effective_origin,reason,created_at) \
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        rusqlite::params![
            &event.event_id,
            &event.bundle_id,
            event.version,
            &event.supersedes_event_id,
            &event.verdict,
            &event.origin,
            event.effective_origin.as_db(),
            &event.reason,
            &event.created_at,
        ],
    )?;
    Ok(event)
}

fn project_machine_verification_in_current_write(
    conn: &rusqlite::Connection,
    bundle: &EvidenceBundleRecord,
) -> Result<(), DbError> {
    if bundle.origin != EvidenceOrigin::Machine
        || !matches!(bundle.verdict.as_str(), "verified" | "failed")
    {
        return Ok(());
    }
    let run_id = bundle.run_id.as_deref().ok_or_else(|| {
        DbError::Invalid("MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION".to_owned())
    })?;
    let invocation_id = bundle.producer_invocation_id.as_deref().ok_or_else(|| {
        DbError::Invalid("MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION".to_owned())
    })?;
    let task_id = conn
        .query_row(
            "SELECT run.task_id FROM run_envelopes run \
             JOIN tool_invocations invocation ON invocation.run_id=run.id \
             WHERE run.id=?1 AND run.session_id=?2 AND invocation.invocation_id=?3 \
               AND invocation.status='succeeded'",
            rusqlite::params![run_id, &bundle.session_id, invocation_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| {
            DbError::Invalid("MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION".to_owned())
        })?;
    let (machine_count, failed_count, verified_count) = conn.query_row(
        "SELECT COUNT(*), \
                COALESCE(SUM(CASE WHEN effective_verdict='failed' THEN 1 ELSE 0 END),0), \
                COALESCE(SUM(CASE WHEN effective_verdict='verified' THEN 1 ELSE 0 END),0) \
         FROM ( \
             SELECT COALESCE((SELECT event.verdict FROM evidence_verdict_events event \
                              WHERE event.bundle_id=bundle.bundle_id \
                              ORDER BY event.version DESC LIMIT 1),bundle.verdict) AS effective_verdict \
             FROM evidence_bundles bundle \
             WHERE bundle.run_id=?1 AND bundle.origin='machine' \
         )",
        [run_id],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )?;
    let status = if failed_count > 0 {
        "failed"
    } else if machine_count > 0 && verified_count == machine_count {
        "passed"
    } else {
        "pending"
    };
    let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
    conn.execute(
        "UPDATE run_envelopes SET verification_status=?1,updated_at=?2,version=version+1 \
         WHERE id=?3 AND verification_status<>?1",
        rusqlite::params![status, &now, run_id],
    )?;
    let task_count = conn.execute(
        "UPDATE tasks SET verification_status=?1,updated_at=?2,version=version+1 \
         WHERE id=?3 AND verification_status<>?1",
        rusqlite::params![status, &now, &task_id],
    )?;
    if task_count == 0 {
        let task_exists = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
            [&task_id],
            |row| row.get::<_, bool>(0),
        )?;
        if !task_exists {
            return Err(DbError::Invalid(
                "EVIDENCE_PRODUCER_TASK_NOT_FOUND".to_owned(),
            ));
        }
    }
    crate::run::append_event_in_current_write(
        conn,
        run_id,
        "verification_updated",
        None,
        &serde_json::json!({
            "bundleId": bundle.bundle_id,
            "origin": "machine",
            "verdict": bundle.verdict,
            "verificationStatus": status,
        }),
    )?;
    Ok(())
}

fn validate_bundle(bundle: &EvidenceBundleRecord) -> Result<(), DbError> {
    validate_verdict(&bundle.verdict)?;
    if bundle.origin == EvidenceOrigin::ModelAssertion
        && !matches!(bundle.verdict.as_str(), "pending" | "inconclusive")
    {
        return Err(DbError::Invalid("MODEL_ASSERTION_CANNOT_VERIFY".to_owned()));
    }
    if bundle.producer_invocation_id.is_some() && bundle.run_id.is_none() {
        return Err(DbError::Invalid(
            "EVIDENCE_PRODUCER_RUN_REQUIRED".to_owned(),
        ));
    }
    if bundle.origin == EvidenceOrigin::Machine
        && matches!(bundle.verdict.as_str(), "verified" | "failed")
        && (bundle.run_id.is_none()
            || bundle.producer_invocation_id.is_none()
            || bundle
                .items
                .iter()
                .any(|item| item.producer_invocation_id.is_none()))
    {
        return Err(DbError::Invalid(
            "MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION".to_owned(),
        ));
    }
    Ok(())
}

fn validate_verdict(verdict: &str) -> Result<(), DbError> {
    if matches!(
        verdict,
        "pending" | "verified" | "failed" | "inconclusive" | "unavailable" | "stale"
    ) {
        Ok(())
    } else {
        Err(DbError::Invalid("EVIDENCE_VERDICT_INVALID".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one audit-chain scenario verifies idempotency, immutability and supersession"
    )]
    async fn bundle_is_immutable_exact_retries_are_idempotent_and_reviews_append() {
        let db = Db::open_in_memory().expect("db");
        let bundle = EvidenceBundleRecord {
            bundle_id: "bundle-1".into(),
            session_id: "session-1".into(),
            agent_id: None,
            kind: "verify".into(),
            claim: Some("tests pass".into()),
            origin: EvidenceOrigin::ModelAssertion,
            producer_invocation_id: None,
            verdict: "pending".into(),
            created_at: "2026-08-21T00:00:00.000000Z".into(),
            run_id: None,
            items: vec![
                EvidenceItemRecord {
                    id: "later".into(),
                    producer_invocation_id: None,
                    item_type: "log".into(),
                    summary: None,
                    blob_sha256: None,
                    meta: Some(json!({"exitCode": 0})),
                    sort_order: 2,
                },
                EvidenceItemRecord {
                    id: "first".into(),
                    producer_invocation_id: None,
                    item_type: "assertion".into(),
                    summary: Some("ok".into()),
                    blob_sha256: None,
                    meta: None,
                    sort_order: 1,
                },
            ],
        };
        db.save_evidence_bundle(&bundle).await.expect("save");
        db.save_evidence_bundle(&bundle)
            .await
            .expect("exact retry is idempotent");
        let mut mismatch = bundle.clone();
        mismatch.claim = Some("different claim".into());
        let error = db
            .save_evidence_bundle(&mismatch)
            .await
            .expect_err("bundle id cannot be reused for different content");
        assert!(error.to_string().contains("EVIDENCE_IMMUTABLE_MISMATCH"));
        let loaded = db
            .find_evidence_bundle("bundle-1")
            .await
            .expect("query")
            .expect("bundle");
        assert_eq!(loaded.items[0].id, "first");
        assert!(
            db.update_evidence_verdict("bundle-1", "passed")
                .await
                .is_err(),
            "unknown verdicts must fail closed"
        );
        assert!(
            db.update_evidence_verdict("bundle-1", "verified")
                .await
                .expect("human review")
        );
        let reviewed = db
            .find_evidence_bundle("bundle-1")
            .await
            .expect("query reviewed")
            .expect("reviewed bundle");
        assert_eq!(reviewed.origin, EvidenceOrigin::Human);
        assert_eq!(reviewed.verdict, "verified");
        db.save_evidence_bundle(&bundle)
            .await
            .expect("review projection does not change immutable retry identity");
        let events = db
            .find_evidence_verdict_events("bundle-1")
            .await
            .expect("review history");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].version, 1);
        assert_eq!(events[0].origin, "human");
        assert_eq!(events[0].effective_origin, EvidenceOrigin::Human);
        assert_eq!(events[0].reason, "human_review");
        assert!(events[0].supersedes_event_id.is_none());
        assert_eq!(
            db.find_evidence_by_session("session-1")
                .await
                .expect("list")[0]
                .verdict,
            "verified"
        );

        let (raw_origin, raw_verdict, bundle_count, item_count) = db
            .with_conn_blocking(|conn| {
                let (origin, verdict) = conn.query_row(
                    "SELECT origin,verdict FROM evidence_bundles WHERE bundle_id='bundle-1'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )?;
                let bundle_count = conn.query_row(
                    "SELECT COUNT(*) FROM evidence_bundles WHERE bundle_id='bundle-1'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                let item_count = conn.query_row(
                    "SELECT COUNT(*) FROM evidence_items WHERE bundle_id='bundle-1'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok((origin, verdict, bundle_count, item_count))
            })
            .expect("raw immutable state");
        assert_eq!(raw_origin, "modelAssertion");
        assert_eq!(raw_verdict, "pending");
        assert_eq!(bundle_count, 1);
        assert_eq!(item_count, 2);
    }

    #[tokio::test]
    async fn evidence_tables_reject_update_and_delete() {
        let db = Db::open_in_memory().expect("db");
        let bundle = EvidenceBundleRecord {
            bundle_id: "immutable-bundle".into(),
            session_id: "session-1".into(),
            agent_id: None,
            kind: "claim".into(),
            claim: Some("immutable".into()),
            origin: EvidenceOrigin::ModelAssertion,
            producer_invocation_id: None,
            verdict: "pending".into(),
            created_at: "2026-09-09T00:00:00.000000Z".into(),
            run_id: None,
            items: vec![EvidenceItemRecord {
                id: "immutable-item".into(),
                producer_invocation_id: None,
                item_type: "assertion".into(),
                summary: Some("original".into()),
                blob_sha256: None,
                meta: None,
                sort_order: 0,
            }],
        };
        db.save_evidence_bundle(&bundle).await.expect("save");
        assert!(
            db.update_evidence_verdict("immutable-bundle", "verified")
                .await
                .expect("review")
        );

        for sql in [
            "UPDATE evidence_bundles SET claim='changed' WHERE bundle_id='immutable-bundle'",
            "DELETE FROM evidence_bundles WHERE bundle_id='immutable-bundle'",
            "UPDATE evidence_items SET summary='changed' WHERE id='immutable-item'",
            "DELETE FROM evidence_items WHERE id='immutable-item'",
            "UPDATE evidence_verdict_events SET reason='changed' WHERE bundle_id='immutable-bundle'",
            "DELETE FROM evidence_verdict_events WHERE bundle_id='immutable-bundle'",
        ] {
            let error = db
                .with_conn_blocking(|conn| {
                    conn.execute(sql, [])?;
                    Ok(())
                })
                .expect_err("append-only tables reject mutation");
            assert!(error.to_string().contains("IMMUTABLE"), "{error}");
        }
    }

    #[tokio::test]
    async fn model_assertion_cannot_be_persisted_as_machine_verified() {
        let db = Db::open_in_memory().expect("db");
        let bundle = EvidenceBundleRecord {
            bundle_id: "model-claim".into(),
            session_id: "session-1".into(),
            agent_id: Some("agent-1".into()),
            kind: "claim".into(),
            claim: Some("I ran the tests".into()),
            origin: EvidenceOrigin::ModelAssertion,
            producer_invocation_id: None,
            verdict: "verified".into(),
            created_at: "2026-09-08T00:00:00.000000Z".into(),
            run_id: None,
            items: Vec::new(),
        };
        let error = db
            .save_evidence_bundle(&bundle)
            .await
            .expect_err("model assertion must not verify");
        assert!(error.to_string().contains("MODEL_ASSERTION_CANNOT_VERIFY"));
    }

    async fn seed_succeeded_verifier(db: &Db, run_id: &str, tool_use_id: &str) -> String {
        use crate::{CleanupStatus, NewToolInvocation, ToolInvocationStatus};

        let run = db
            .find_run_by_id(run_id)
            .await
            .expect("run query")
            .expect("run");
        let invocation_id = uuid::Uuid::new_v4().to_string();
        let invocation = db
            .create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: run.task_id,
                run_id: run_id.to_owned(),
                tool_use_id: tool_use_id.to_owned(),
                tool_name: "VerifyJourney".into(),
                input_json: Some("{}".into()),
                side_effect_class: "read".into(),
                directory_generation: Some(1),
                connection_generation: None,
            })
            .await
            .expect("invocation");
        assert_eq!(
            db.transition_tool_invocation_cas(
                &invocation_id,
                invocation.version,
                ToolInvocationStatus::Succeeded,
                Some("{}"),
                Some("toolResult:verify"),
                None,
                CleanupStatus::Confirmed,
            )
            .await
            .expect("transition"),
            crate::CasOutcome::Applied
        );
        invocation_id
    }

    fn machine_bundle(
        session_id: &str,
        run_id: &str,
        invocation_id: &str,
        suffix: &str,
        verdict: &str,
    ) -> EvidenceBundleRecord {
        EvidenceBundleRecord {
            bundle_id: format!("machine-{suffix}"),
            session_id: session_id.to_owned(),
            agent_id: None,
            kind: "verify".into(),
            claim: Some(format!("check {suffix}")),
            origin: EvidenceOrigin::Machine,
            producer_invocation_id: Some(invocation_id.to_owned()),
            verdict: verdict.to_owned(),
            created_at: "2026-09-09T00:00:00.000000Z".into(),
            run_id: Some(run_id.to_owned()),
            items: vec![EvidenceItemRecord {
                id: format!("machine-item-{suffix}"),
                producer_invocation_id: Some(invocation_id.to_owned()),
                item_type: "test_result".into(),
                summary: Some(verdict.to_owned()),
                blob_sha256: None,
                meta: Some(json!({"verdict": verdict})),
                sort_order: 0,
            }],
        }
    }

    #[tokio::test]
    async fn machine_verdicts_project_conservatively_to_run_and_task() {
        use crate::VerificationStatus;

        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/evidence-projection")
            .await
            .expect("session");
        db.start_run(
            "evidence-projection-run",
            &session.id,
            None,
            Some("query"),
            "model",
        )
        .await
        .expect("run");

        let passed_invocation =
            seed_succeeded_verifier(&db, "evidence-projection-run", "verify-pass").await;
        db.save_evidence_bundle(&machine_bundle(
            &session.id,
            "evidence-projection-run",
            &passed_invocation,
            "pass",
            "verified",
        ))
        .await
        .expect("passing evidence");
        assert_eq!(
            db.find_run_by_id("evidence-projection-run")
                .await
                .expect("run query")
                .expect("run")
                .verification_status,
            "passed"
        );
        assert_eq!(
            db.find_runtime_task_by_id("evidence-projection-run")
                .await
                .expect("task query")
                .expect("task")
                .verification_status,
            VerificationStatus::Passed
        );

        let failed_invocation =
            seed_succeeded_verifier(&db, "evidence-projection-run", "verify-fail").await;
        db.save_evidence_bundle(&machine_bundle(
            &session.id,
            "evidence-projection-run",
            &failed_invocation,
            "fail",
            "failed",
        ))
        .await
        .expect("failing evidence");
        let later_pass =
            seed_succeeded_verifier(&db, "evidence-projection-run", "verify-later-pass").await;
        db.save_evidence_bundle(&machine_bundle(
            &session.id,
            "evidence-projection-run",
            &later_pass,
            "later-pass",
            "verified",
        ))
        .await
        .expect("later passing evidence");

        assert_eq!(
            db.find_run_by_id("evidence-projection-run")
                .await
                .expect("run query")
                .expect("run")
                .verification_status,
            "failed",
            "a later pass cannot erase an earlier effective machine failure"
        );
        assert_eq!(
            db.find_runtime_task_by_id("evidence-projection-run")
                .await
                .expect("task query")
                .expect("task")
                .verification_status,
            VerificationStatus::Failed
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one transaction-level producer/item provenance scenario
    async fn terminal_machine_evidence_requires_the_same_succeeded_invocation_per_item() {
        use crate::{CleanupStatus, NewToolInvocation, ToolInvocationStatus};

        let db = Db::open_in_memory().expect("db");
        let workspace =
            std::env::temp_dir().join(format!("zk-evidence-commit-gate-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&workspace).expect("workspace");
        let session = db
            .create_session("model", workspace.to_str().expect("utf8"))
            .await
            .expect("session");
        db.start_run("evidence-run", &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        let invocation_id = uuid::Uuid::new_v4().to_string();
        let invocation = db
            .create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: "evidence-run".into(),
                run_id: "evidence-run".into(),
                tool_use_id: "verify-call".into(),
                tool_name: "VerifyJourney".into(),
                input_json: Some(r#"{"steps":[{}]}"#.into()),
                side_effect_class: "read".into(),
                directory_generation: Some(1),
                connection_generation: None,
            })
            .await
            .expect("invocation");
        let base = EvidenceBundleRecord {
            bundle_id: "machine-proof".into(),
            session_id: session.id.clone(),
            agent_id: None,
            kind: "browser_journey".into(),
            claim: Some("page renders".into()),
            origin: EvidenceOrigin::Machine,
            producer_invocation_id: Some(invocation_id.clone()),
            verdict: "verified".into(),
            created_at: "2026-09-09T00:00:00.000000Z".into(),
            run_id: Some("evidence-run".into()),
            items: vec![EvidenceItemRecord {
                id: "machine-step".into(),
                producer_invocation_id: Some(invocation_id.clone()),
                item_type: "browser_journey_step".into(),
                summary: Some("navigate: passed".into()),
                blob_sha256: None,
                meta: Some(json!({"ok":true})),
                sort_order: 0,
            }],
        };

        let running_error = db
            .save_evidence_bundle(&base)
            .await
            .expect_err("preparing invocation cannot certify evidence");
        assert!(
            running_error
                .to_string()
                .contains("EVIDENCE_PRODUCER_INVOCATION_MISMATCH")
                || running_error
                    .to_string()
                    .contains("MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION")
        );

        assert_eq!(
            db.transition_tool_invocation_cas(
                &invocation_id,
                invocation.version,
                ToolInvocationStatus::Succeeded,
                Some(r#"{"steps":[{}]}"#),
                Some("toolResult:verify-call"),
                None,
                CleanupStatus::Confirmed,
            )
            .await
            .expect("transition"),
            crate::CasOutcome::Applied
        );

        let mut forged = base.clone();
        forged.bundle_id = "metadata-forgery".into();
        forged.items[0].id = "metadata-forgery-step".into();
        forged.items[0].producer_invocation_id = None;
        forged.items[0].meta = Some(json!({"producerInvocationId": invocation_id}));
        let forged_error = db
            .save_evidence_bundle(&forged)
            .await
            .expect_err("metadata cannot replace typed producer identity");
        assert!(
            forged_error
                .to_string()
                .contains("MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION")
        );

        db.save_evidence_bundle(&base)
            .await
            .expect("succeeded invocation commits evidence");
        let saved = db
            .find_evidence_bundle("machine-proof")
            .await
            .expect("query")
            .expect("bundle");
        assert_eq!(
            saved.items[0].producer_invocation_id.as_deref(),
            Some(invocation_id.as_str())
        );
        std::fs::remove_dir_all(workspace).expect("cleanup");
    }
}
