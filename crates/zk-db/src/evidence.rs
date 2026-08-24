//! Evidence bundle repository backed by `evidence_bundles` and `evidence_items`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Db, DbError};

/// One ordered evidence item.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceItemRecord {
    /// Item identifier.
    pub id: String,
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
    /// Verification verdict.
    pub verdict: String,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// Associated run.
    pub run_id: Option<String>,
    /// Ordered evidence items.
    pub items: Vec<EvidenceItemRecord>,
}

impl Db {
    /// Upsert a complete bundle in one transaction.
    ///
    /// # Errors
    /// Returns [`DbError`] when metadata serialization or the `SQLite` transaction fails.
    pub async fn save_evidence_bundle(&self, bundle: &EvidenceBundleRecord) -> Result<(), DbError> {
        let bundle = bundle.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO evidence_bundles \
                 (bundle_id,session_id,agent_id,kind,claim,verdict,created_at,run_id) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8) \
                 ON CONFLICT(bundle_id) DO UPDATE SET session_id=excluded.session_id, \
                 agent_id=excluded.agent_id, kind=excluded.kind, claim=excluded.claim, \
                 verdict=excluded.verdict, created_at=excluded.created_at, run_id=excluded.run_id",
                rusqlite::params![
                    bundle.bundle_id,
                    bundle.session_id,
                    bundle.agent_id,
                    bundle.kind,
                    bundle.claim,
                    bundle.verdict,
                    bundle.created_at,
                    bundle.run_id,
                ],
            )?;
            tx.execute(
                "DELETE FROM evidence_items WHERE bundle_id=?1",
                [&bundle.bundle_id],
            )?;
            for item in bundle.items {
                let meta_json = item.meta.as_ref().map(serde_json::to_string).transpose()?;
                tx.execute(
                    "INSERT INTO evidence_items \
                     (id,bundle_id,type,summary,blob_sha256,meta_json,sort_order) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    rusqlite::params![
                        item.id,
                        bundle.bundle_id,
                        item.item_type,
                        item.summary,
                        item.blob_sha256,
                        meta_json,
                        item.sort_order,
                    ],
                )?;
            }
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

    /// Update only the bundle verdict.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` update fails.
    pub async fn update_evidence_verdict(
        &self,
        bundle_id: &str,
        verdict: &str,
    ) -> Result<bool, DbError> {
        let bundle_id = bundle_id.to_owned();
        let verdict = verdict.to_owned();
        self.with_writer(move |conn| {
            Ok(conn.execute(
                "UPDATE evidence_bundles SET verdict=?1 WHERE bundle_id=?2",
                [&verdict, &bundle_id],
            )? > 0)
        })
        .await
    }
}

fn load_bundle(
    conn: &rusqlite::Connection,
    bundle_id: &str,
) -> Result<Option<EvidenceBundleRecord>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT bundle_id,session_id,agent_id,kind,claim,verdict,created_at,run_id \
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
        verdict: row.get(5)?,
        created_at: row.get(6)?,
        run_id: row.get(7)?,
        items: Vec::new(),
    };
    drop(rows);
    drop(stmt);
    let mut item_stmt = conn.prepare(
        "SELECT id,type,summary,blob_sha256,meta_json,sort_order FROM evidence_items \
         WHERE bundle_id=?1 ORDER BY sort_order ASC,id ASC",
    )?;
    let rows = item_stmt.query_map([bundle_id], |row| {
        let meta_json: Option<String> = row.get(4)?;
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            meta_json,
            row.get::<_, i64>(5)?,
        ))
    })?;
    for row in rows {
        let (id, item_type, summary, blob_sha256, meta_json, sort_order) = row?;
        bundle.items.push(EvidenceItemRecord {
            id,
            item_type,
            summary,
            blob_sha256,
            meta: meta_json.as_deref().map(serde_json::from_str).transpose()?,
            sort_order,
        });
    }
    Ok(Some(bundle))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn bundle_round_trip_keeps_stable_item_order_and_updates_atomically() {
        let db = Db::open_in_memory().expect("db");
        let bundle = EvidenceBundleRecord {
            bundle_id: "bundle-1".into(),
            session_id: "session-1".into(),
            agent_id: None,
            kind: "verify".into(),
            claim: Some("tests pass".into()),
            verdict: "pending".into(),
            created_at: "2026-08-21T00:00:00.000000Z".into(),
            run_id: None,
            items: vec![
                EvidenceItemRecord {
                    id: "later".into(),
                    item_type: "log".into(),
                    summary: None,
                    blob_sha256: None,
                    meta: Some(json!({"exitCode": 0})),
                    sort_order: 2,
                },
                EvidenceItemRecord {
                    id: "first".into(),
                    item_type: "assertion".into(),
                    summary: Some("ok".into()),
                    blob_sha256: None,
                    meta: None,
                    sort_order: 1,
                },
            ],
        };
        db.save_evidence_bundle(&bundle).await.expect("save");
        let loaded = db
            .find_evidence_bundle("bundle-1")
            .await
            .expect("query")
            .expect("bundle");
        assert_eq!(loaded.items[0].id, "first");
        assert!(
            db.update_evidence_verdict("bundle-1", "passed")
                .await
                .expect("update")
        );
        assert_eq!(
            db.find_evidence_by_session("session-1")
                .await
                .expect("list")[0]
                .verdict,
            "passed"
        );
    }
}
