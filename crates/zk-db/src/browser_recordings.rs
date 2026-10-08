//! Narrow, host-owned recording consumption state on existing browser resources.
//! Physical resource release and evidence consumption are independent lifecycles.
use crate::{Db, DbError};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// A bounded snapshot of recording consumption state on its physical resource.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserRecordingFinalization {
    /// Existing execution-resource identity.
    pub resource_id: String,
    /// CAS version shared with the resource row; status remains orthogonal.
    pub version: i64,
    /// Host-only versioned batch identity, manifest and consumption phase.
    pub state: Value,
    /// Failed/cancelled/interrupted producer whose browser actions cannot resume.
    pub recoverable: bool,
    /// Stable private browser context identity, never a caller supplied alias.
    pub external_id: Option<String>,
    /// Physical cleanup remains independent from recording consumption.
    pub physical_status: String,
}

/// Host-owned replay snapshot for a persistent, recorded `VerifyJourney`.
/// The private fields bind the exact journal/result/resource facts rechecked at commit.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordedJourneyPostprocessing {
    /// Immutable physical producer.
    pub invocation_id: String,
    /// Content owner resolved from the Run.
    pub session_id: String,
    /// Owning Run identity.
    pub run_id: String,
    /// Receipt matched against the immutable tool result.
    pub receipt: Value,
    /// Original tool-result error bit; recovery never rewrites it.
    pub output_is_error: bool,
    resource_id: String,
    workspace: String,
    payload: Value,
    result: Value,
    recording: Value,
    version: i64,
    completed: bool,
}

impl Db {
    /// Load only the durable evidence-only obligation created by a trusted verifier.
    /// Ephemeral sessions are excluded before materializing any retained body.
    ///
    /// # Errors
    /// Rejects malformed receipts and mismatched ownership/result facts.
    pub async fn recorded_journey_postprocessing(
        &self,
        invocation: &str,
    ) -> Result<Option<RecordedJourneyPostprocessing>, DbError> {
        let invocation = invocation.to_owned();
        self.with_reader(move |conn| load_recorded_journey_postprocessing(conn, &invocation))
            .await
    }

    /// Atomically register immutable evidence and complete its exact journal obligation.
    /// Retrying a proven completed obligation succeeds without duplicating events.
    ///
    /// # Errors
    /// A changed obligation, conflicting old evidence or corrupt recording fails closed.
    pub async fn complete_recorded_journey_postprocessing(
        &self,
        expected: &RecordedJourneyPostprocessing,
        bundle: &crate::EvidenceBundleRecord,
    ) -> Result<(), DbError> {
        let expected = expected.clone();
        let bundle = bundle.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let current = load_recorded_journey_postprocessing(&tx, &expected.invocation_id)?
                .ok_or_else(|| DbError::Invalid("RECORDING_POSTPROCESSING_OWNER_CHANGED".into()))?;
            let mut comparable = current.clone();
            comparable.version = expected.version;
            comparable.completed = expected.completed;
            if comparable != expected || (!current.completed && current.version != expected.version) {
                return Err(DbError::Invalid("RECORDING_POSTPROCESSING_CHANGED".into()));
            }
            validate_recorded_journey_bundle(&current, &bundle)?;
            // This is the same bounded archive validation used for aborted recordings.
            validate_recovery_dispositions(&current.recording, &current.workspace)?;
            let ids = {
                let mut statement = tx.prepare("SELECT bundle_id FROM evidence_bundles WHERE producer_invocation_id=?1 AND origin='machine' ORDER BY bundle_id")?;
                statement.query_map([&current.invocation_id], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?
            };
            if ids.is_empty() {
                if current.completed {
                    return Err(DbError::Invalid("RECORDING_POSTPROCESSING_EVIDENCE_MISSING".into()));
                }
                crate::evidence::save_evidence_bundle_in_current_write(&tx, &bundle)?;
            } else {
                // Old releases generated random bundle/item IDs before completing the
                // journal. Compare base facts (never human verdict projections), and
                // retain all original IDs/events. A conflicting bundle is not overwritten.
                for id in ids {
                    let existing = crate::evidence::load_bundle_base(&tx, &id)?
                        .ok_or_else(|| DbError::Invalid("RECORDING_POSTPROCESSING_EVIDENCE_MISSING".into()))?;
                    validate_recorded_journey_bundle(&current, &existing)?;
                }
            }
            if !current.completed {
                let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
                if tx.execute("UPDATE tool_result_postprocessing SET status='completed',version=version+1,updated_at=?1,completed_at=?1 WHERE invocation_id=?2 AND status='pending' AND version=?3", params![now,current.invocation_id,current.version])? != 1 {
                    return Err(DbError::Invalid("RECORDING_POSTPROCESSING_CHANGED".into()));
                }
            }
            tx.commit()?;
            Ok(())
        }).await
    }

    /// Protect every unsettled batch, including reserved allocations whose reply
    /// was lost. A malformed store is an error, never permission to delete.
    ///
    /// # Errors
    /// Propagates database read or malformed JSON errors; failure never permits pruning.
    pub async fn browser_recording_batch_protected(&self, batch: &str) -> Result<bool, DbError> {
        let batch = batch.to_owned();
        self.with_reader(move |conn| Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM execution_resources WHERE json_extract(metadata_json,'$.recordingFinalization.identity.batch_id')=?1 AND COALESCE(json_extract(metadata_json,'$.recordingFinalization.phase'),'reserved')<>'acknowledged')", [&batch], |row| row.get(0))?)).await
    }

    /// Whether a Session has ever registered a managed browser context lease.
    ///
    /// # Errors
    /// Propagates database read errors; failure never permits Session cleanup.
    pub async fn session_has_browser_resources(&self, session: &str) -> Result<bool, DbError> {
        let session = session.to_owned();
        self.with_reader(move |conn| Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM execution_resources r JOIN run_envelopes e ON e.id=r.run_id WHERE e.session_id=?1 AND json_extract(r.metadata_json,'$.kind')='browserUsageLease')", [&session], |row| row.get(0))?)).await
    }

    /// Browser owners in the exact Session subtree deleted by the FK cascade.
    ///
    /// # Errors
    /// Propagates read errors; callers must retain their deletion admission guard.
    pub async fn session_browser_owner_ids_for_deletion(
        &self,
        session: &str,
    ) -> Result<Vec<String>, DbError> {
        let session = session.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare("WITH RECURSIVE tree(id) AS (SELECT id FROM sessions WHERE id=?1 UNION SELECT s.id FROM sessions s JOIN tree ON s.parent_session_id=tree.id) SELECT DISTINCT e.session_id FROM execution_resources r JOIN run_envelopes e ON e.id=r.run_id JOIN tree ON tree.id=e.session_id WHERE json_extract(r.metadata_json,'$.kind')='browserUsageLease' ORDER BY e.session_id")?;
            Ok(stmt.query_map([session], |row| row.get(0))?.collect::<Result<Vec<_>, _>>()?)
        }).await
    }

    /// Bind the sealed private-spool manifest to its already registered owner.
    ///
    /// # Errors
    /// Rejects owner, manifest, immutability or size violations and propagates persistence errors.
    pub async fn seal_browser_recording(
        &self,
        resource_id: &str,
        manifest: Value,
        dispositions: Value,
    ) -> Result<(), DbError> {
        let resource_id = resource_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let (run, invocation, mut metadata): (String, String, String) = tx.query_row(
                "SELECT run_id,invocation_id,metadata_json FROM execution_resources WHERE resource_id=?1", [&resource_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            let mut value: Value = serde_json::from_str(&metadata)?;
            let state = &mut value["recordingFinalization"];
            if state["version"] != 1 || !matches!(state["phase"].as_str(), Some("reserved" | "sealed")) || state["identity"] != manifest["identity"]
                || manifest["identity"]["run_id"] != run || manifest["identity"]["invocation_id"] != invocation {
                return Err(DbError::Invalid("RECORDING_OWNER_MISMATCH".into()));
            }
            if manifest["manifest_sha256"].as_str().is_none_or(|digest| digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())) {
                return Err(DbError::Invalid("RECORDING_MANIFEST_INVALID".into()));
            }
            if state["phase"] == "sealed" && (state["manifest"] != manifest || (state["dispositions"] != json!([]) && state["dispositions"] != dispositions)) {
                return Err(DbError::Invalid("RECORDING_MANIFEST_IMMUTABLE".into()));
            }
            state["phase"] = json!("sealed");
            state["manifest"] = manifest;
            state["dispositions"] = dispositions;
            metadata = serde_json::to_string(&value)?;
            if metadata.len() > 128 * 1024 { return Err(DbError::Invalid("RECORDING_MANIFEST_LIMIT".into())); }
            tx.execute("UPDATE execution_resources SET metadata_json=?2,version=version+1,updated_at=?3 WHERE resource_id=?1", params![resource_id, metadata, crate::time::format_rfc3339_micros(crate::time::now_millis())])?;
            tx.commit()?;
            Ok(())
        }).await
    }

    /// Finish a reservation only after the private sidecar positively confirms
    /// context, retained creation and batch are all absent. This consumes no
    /// existing file; unknown transport outcomes must never call this method.
    ///
    /// # Errors
    /// Rejects missing release proof, mismatched owner or phase, and propagates persistence errors.
    pub async fn acknowledge_uncreated_browser_recording(
        &self,
        resource_id: &str,
        proof: Value,
    ) -> Result<(), DbError> {
        let resource_id = resource_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let (status, raw, run, invocation, session): (String,String,String,Option<String>,String) = tx.query_row("SELECT r.status,r.metadata_json,r.run_id,r.invocation_id,e.session_id FROM execution_resources r JOIN run_envelopes e ON e.id=r.run_id WHERE r.resource_id=?1", [&resource_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?;
            let mut metadata: Value = serde_json::from_str(&raw)?;
            let state = &mut metadata["recordingFinalization"];
            if status != "released" || proof["phase"] != "not_created" || state["version"] != 1 || state["identity"] != proof["identity"] || proof["identity"]["run_id"] != run || proof["identity"]["session_id"] != session || proof["identity"]["invocation_id"].as_str() != invocation.as_deref() || invocation.is_none() || !(state["phase"] == "reserved" || (state["phase"] == "acknowledged" && state["disposition"] == "not_created")) {
                return Err(DbError::Invalid("RECORDING_ABSENCE_PROOF_INVALID".into()));
            }
            state["phase"] = json!("acknowledged");
            state["disposition"] = json!("not_created");
            tx.execute("UPDATE execution_resources SET metadata_json=?2,version=version+1,updated_at=?3 WHERE resource_id=?1",params![resource_id,metadata.to_string(),crate::time::format_rfc3339_micros(crate::time::now_millis())])?;
            tx.commit()?;
            Ok(())
        }).await
    }

    /// Bounded maintenance batch, including reservations whose producer stopped.
    /// Active reservations are excluded: scanning must never cancel a live Journey.
    ///
    /// # Errors
    /// Propagates database read and metadata decoding errors.
    pub async fn pending_browser_recordings(
        &self,
    ) -> Result<Vec<BrowserRecordingFinalization>, DbError> {
        self.with_reader(|conn| {
            let mut statement = conn.prepare(&format!("{RECORDING_QUERY} WHERE json_extract(r.metadata_json,'$.recordingFinalization.phase') IN ('sealed','ackEligible') OR (json_extract(r.metadata_json,'$.recordingFinalization.phase')='reserved' AND i.status IN ('failed','cancelled','interrupted')) ORDER BY r.updated_at,r.resource_id LIMIT 50"))?;
            let rows = statement.query_map([], recording_row)?;
            rows.map(|row| decode_recording(row?)).collect()
        }).await
    }

    /// Refresh a recording after cleanup/archival changed its shared CAS version.
    ///
    /// # Errors
    /// Propagates SQL and metadata decoding failures.
    pub async fn browser_recording_finalization(
        &self,
        resource: &str,
    ) -> Result<Option<BrowserRecordingFinalization>, DbError> {
        let resource = resource.to_owned();
        self.with_reader(move |conn| {
            conn.query_row(
                &format!("{RECORDING_QUERY} WHERE r.resource_id=?1"),
                [resource],
                recording_row,
            )
            .optional()?
            .map(decode_recording)
            .transpose()
        })
        .await
    }

    /// Commit recording-only evidence for an aborted verifier. The original tool
    /// result and its postprocessing are immutable and are never promoted to success.
    /// All ownership and evidence fields are constructed from the durable resource.
    ///
    /// # Errors
    /// Rejects active/successful producers, unconfirmed cleanup, incomplete file
    /// dispositions and missing/corrupt archive blobs; storage failure rolls back.
    pub async fn complete_failed_browser_recording(&self, resource: &str) -> Result<(), DbError> {
        let resource = resource.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let (raw, status, invocation, outcome, run, session, workspace, created): RecoveryOwnerRow = tx.query_row(
                "SELECT r.metadata_json,r.status,i.invocation_id,i.status,r.run_id,e.session_id,s.working_dir,r.created_at FROM execution_resources r JOIN tool_invocations i ON i.invocation_id=r.invocation_id AND i.run_id=r.run_id JOIN run_envelopes e ON e.id=r.run_id JOIN sessions s ON s.id=e.session_id WHERE r.resource_id=?1 AND i.tool_name='VerifyJourney' AND s.content_retention='persistent'", [&resource],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)))?;
            let mut metadata: Value = serde_json::from_str(&raw)?;
            let state = &metadata["recordingFinalization"];
            if status != "released" || !matches!(outcome.as_str(), "failed"|"cancelled"|"interrupted") || metadata["kind"] != "browserSession" || state["version"] != 1 || !matches!(state["phase"].as_str(), Some("sealed"|"ackEligible"|"acknowledged")) || state["identity"]["session_id"] != session || state["identity"]["run_id"] != run || state["identity"]["invocation_id"] != invocation || state["manifest"]["identity"] != state["identity"] {
                return Err(DbError::Invalid("RECORDING_RECOVERY_OWNER_INVALID".into()));
            }
            validate_recovery_dispositions(state, &workspace)?;
            let bundle = format!("recording-finalization:{resource}");
            let meta = recovery_evidence_meta(&resource, &outcome, state);
            let saved: Option<String> = tx.query_row("SELECT meta_json FROM evidence_items WHERE id=?1", [format!("{bundle}:manifest")], |row| row.get(0)).optional()?;
            if let Some(saved) = saved {
                if serde_json::from_str::<Value>(&saved)? != meta || state["recoveryReceipt"]["bundle_id"] != bundle {
                    return Err(DbError::Invalid("RECORDING_RECOVERY_IMMUTABLE".into()));
                }
                return Ok(());
            }
            if state["phase"] != "sealed" { return Err(DbError::Invalid("RECORDING_RECOVERY_PHASE_INVALID".into())); }
            // The ordinary producer fields certify succeeded invocations.
            // This aborted producer instead retains its exact ownership in
            // immutable, DB-derived source_identity; it asserts no Journey verdict.
            tx.execute("INSERT INTO evidence_bundles(bundle_id,session_id,kind,origin,verdict,created_at) VALUES(?1,?2,'browser_recording_finalization','machine','inconclusive',?3)", params![bundle,session,created])?;
            tx.execute("INSERT INTO evidence_items(id,bundle_id,type,summary,meta_json,sort_order) VALUES(?1,?2,'recording_finalization','Recording archived after an aborted verifier; no Journey verdict is asserted',?3,0)", params![format!("{bundle}:manifest"),bundle,meta.to_string()])?;
            let dispositions = state["dispositions"].as_array().ok_or_else(|| DbError::Invalid("RECORDING_RECOVERY_DISPOSITIONS_INVALID".into()))?;
            for (index, disposition) in dispositions.iter().enumerate() {
                tx.execute("INSERT INTO evidence_items(id,bundle_id,type,summary,blob_sha256,meta_json,sort_order) VALUES(?1,?2,'journey_recording',?3,?4,?5,?6)", params![format!("{bundle}:{index}"),bundle,format!("Recording disposition: {}",disposition["status"].as_str().unwrap_or("unknown")),disposition["blob_sha256"].as_str(),disposition.to_string(),i64::try_from(index).unwrap_or(50)+1])?;
            }
            metadata["recordingFinalization"]["recoveryReceipt"] = json!({"bundle_id":bundle,"postprocessing":"completed"});
            tx.execute("UPDATE execution_resources SET metadata_json=?2,version=version+1,updated_at=?3 WHERE resource_id=?1", params![resource,metadata.to_string(),crate::time::format_rfc3339_micros(crate::time::now_millis())])?;
            tx.commit()?;
            Ok(())
        }).await
    }

    /// A versioned CAS with database-verified evidence. Does not change physical
    /// resource status or tool cleanup projections and therefore cannot deadlock
    /// the evidence commit which must happen before an acknowledgement.
    ///
    /// # Errors
    /// Propagates invalid manifest and persistence errors. A lost CAS or absent evidence returns false.
    pub async fn advance_browser_recording(
        &self,
        resource_id: &str,
        expected_version: i64,
        acknowledged: bool,
    ) -> Result<bool, DbError> {
        let resource_id = resource_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let current: Option<(i64, String, String, String)> = tx.query_row(
                "SELECT version,status,invocation_id,metadata_json FROM execution_resources WHERE resource_id=?1", [&resource_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional()?;
            let Some((version, status, invocation, metadata)) = current else { return Ok(false); };
            if version != expected_version || status != "released" { return Ok(false); }
            let mut metadata: Value = serde_json::from_str(&metadata)?;
            let state = &mut metadata["recordingFinalization"];
            let source = if acknowledged { "ackEligible" } else { "sealed" };
            if state["phase"] != source { return Ok(false); }
            if !acknowledged {
                let digest = state["manifest"]["manifest_sha256"].as_str().ok_or_else(|| DbError::Invalid("RECORDING_MANIFEST_INVALID".into()))?;
                let expected = serde_json::to_string(&state["dispositions"])?;
                let proven: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM tool_result_postprocessing p JOIN evidence_bundles b ON b.producer_invocation_id=p.invocation_id JOIN evidence_items i ON i.bundle_id=b.bundle_id JOIN execution_resources r ON r.invocation_id=p.invocation_id WHERE r.resource_id=?1 AND p.status='completed' AND b.origin='machine' AND b.run_id=r.run_id AND b.session_id=(SELECT session_id FROM run_envelopes WHERE id=r.run_id) AND i.producer_invocation_id=?2 AND json_extract(i.meta_json,'$.recording_manifest_sha256')=?3 AND json_extract(i.meta_json,'$.recording_dispositions')=json(?4))",
                    params![resource_id, invocation, digest, expected], |row| row.get(0))?;
                if !proven && !recovery_evidence_proven(&tx, &resource_id, state)? { return Ok(false); }
            }
            state["phase"] = json!(if acknowledged { "acknowledged" } else { "ackEligible" });
            let changed = tx.execute("UPDATE execution_resources SET metadata_json=?1,version=version+1,updated_at=?4 WHERE resource_id=?2 AND version=?3", params![serde_json::to_string(&metadata)?, resource_id, expected_version, crate::time::format_rfc3339_micros(crate::time::now_millis())])?;
            tx.commit()?;
            Ok(changed == 1)
        }).await
    }
}

fn load_recorded_journey_postprocessing(
    conn: &rusqlite::Connection,
    invocation: &str,
) -> Result<Option<RecordedJourneyPostprocessing>, DbError> {
    let mut statement = conn.prepare(
        "SELECT i.invocation_id,e.session_id,r.run_id,r.resource_id,s.working_dir,p.payload_json,m.content_json,r.metadata_json,p.version,p.status,i.tool_use_id
         FROM execution_resources r JOIN tool_invocations i ON i.invocation_id=r.invocation_id AND i.run_id=r.run_id AND i.task_id=r.task_id
         JOIN run_envelopes e ON e.id=r.run_id AND e.task_id=i.task_id
         JOIN sessions s ON s.id=e.session_id
         JOIN tool_result_postprocessing p ON p.invocation_id=i.invocation_id AND p.task_id=i.task_id AND p.run_id=i.run_id
         JOIN messages m ON m.id=p.result_message_id AND m.session_id=e.session_id AND m.task_id=i.task_id AND m.run_id=i.run_id AND m.origin='tool_result'
         WHERE i.invocation_id=?1 AND i.tool_name='VerifyJourney' AND i.status='succeeded'
           AND s.content_retention='persistent' AND r.status='released'
           AND json_extract(r.metadata_json,'$.kind')='browserSession'
           AND json_extract(r.metadata_json,'$.recordingFinalization.phase') IN ('sealed','ackEligible','acknowledged') LIMIT 2")?;
    let mut rows = statement.query([invocation])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let session: String = row.get(1)?;
    let payload: Value = serde_json::from_str(&crate::content::load_text(
        conn,
        &session,
        &row.get::<_, String>(5)?,
    )?)?;
    if payload["schemaVersion"] != 1
        || payload["toolName"] != "VerifyJourney"
        || payload["requiredKinds"] != json!(["evidence"])
    {
        return Ok(None);
    }
    let result: Value = serde_json::from_str(&crate::content::load_text(
        conn,
        &session,
        &row.get::<_, String>(6)?,
    )?)?;
    let blocks: Vec<crate::model::StoredBlock> = serde_json::from_value(result.clone())?;
    let [
        crate::model::StoredBlock::ToolResult {
            tool_use_id,
            is_error,
            metadata,
            ..
        },
    ] = blocks.as_slice()
    else {
        return Err(DbError::Invalid(
            "RECORDING_POSTPROCESSING_RESULT_INVALID".into(),
        ));
    };
    let receipt = payload["metadata"]["structuredResult"]["evidence"].clone();
    if tool_use_id != &row.get::<_, String>(10)?
        || receipt.is_null()
        || metadata
            .as_ref()
            .map(|m| &m["structuredResult"]["evidence"])
            != Some(&receipt)
    {
        return Err(DbError::Invalid(
            "RECORDING_POSTPROCESSING_RECEIPT_MISMATCH".into(),
        ));
    }
    let metadata: Value = serde_json::from_str(&row.get::<_, String>(7)?)?;
    let state = &metadata["recordingFinalization"];
    let run: String = row.get(2)?;
    if state["version"] != 1
        || state["identity"]["session_id"] != session
        || state["identity"]["run_id"] != run
        || state["identity"]["invocation_id"] != invocation
        || state["manifest"]["identity"] != state["identity"]
    {
        return Err(DbError::Invalid(
            "RECORDING_POSTPROCESSING_OWNER_INVALID".into(),
        ));
    }
    let snapshot = RecordedJourneyPostprocessing {
        invocation_id: row.get(0)?,
        session_id: session,
        run_id: run,
        receipt,
        output_is_error: *is_error,
        resource_id: row.get(3)?,
        workspace: row.get(4)?,
        payload,
        result,
        recording: json!({"identity":state["identity"],"manifest":state["manifest"],"dispositions":state["dispositions"]}),
        version: row.get(8)?,
        completed: row.get::<_, String>(9)? == "completed",
    };
    if rows.next()?.is_some() {
        return Err(DbError::Invalid(
            "RECORDING_POSTPROCESSING_OWNER_AMBIGUOUS".into(),
        ));
    }
    Ok(Some(snapshot))
}

fn validate_recorded_journey_bundle(
    snapshot: &RecordedJourneyPostprocessing,
    bundle: &crate::EvidenceBundleRecord,
) -> Result<(), DbError> {
    let items: Vec<Value> = bundle
        .items
        .iter()
        .map(|item| {
            json!({
                "type":item.item_type,"summary":item.summary,"blobSha256":item.blob_sha256,
                "meta":item.meta,"sortOrder":item.sort_order,
            })
        })
        .collect();
    let receipt = json!({"schemaVersion":1,"kind":bundle.kind,"claim":bundle.claim,
        "verdict":bundle.verdict,"observedAt":bundle.created_at,"items":items});
    if bundle.session_id != snapshot.session_id
        || bundle.run_id.as_deref() != Some(&snapshot.run_id)
        || bundle.producer_invocation_id.as_deref() != Some(&snapshot.invocation_id)
        || bundle.origin != crate::EvidenceOrigin::Machine
        || bundle.agent_id.is_some()
        || bundle
            .items
            .iter()
            .any(|item| item.producer_invocation_id.as_deref() != Some(&snapshot.invocation_id))
        || receipt != snapshot.receipt
        || (bundle.verdict == "failed") != snapshot.output_is_error
        || !bundle.items.iter().any(|item| {
            item.meta.as_ref().is_some_and(|meta| {
                meta["recording_manifest_sha256"]
                    == snapshot.recording["manifest"]["manifest_sha256"]
                    && meta["recording_dispositions"] == snapshot.recording["dispositions"]
            })
        })
    {
        return Err(DbError::Invalid(
            "RECORDING_POSTPROCESSING_EVIDENCE_MISMATCH".into(),
        ));
    }
    Ok(())
}

const RECORDING_QUERY: &str = "SELECT r.resource_id,r.version,json_extract(r.metadata_json,'$.recordingFinalization'),COALESCE(i.status IN ('failed','cancelled','interrupted') AND i.tool_name='VerifyJourney' AND json_extract(r.metadata_json,'$.kind')='browserSession',0),r.external_id,r.status FROM execution_resources r JOIN tool_invocations i ON i.invocation_id=r.invocation_id AND i.run_id=r.run_id";
type RecoveryOwnerRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);
type RecordingRow = (String, i64, String, bool, Option<String>, String);
fn recording_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordingRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}
fn decode_recording(row: RecordingRow) -> Result<BrowserRecordingFinalization, DbError> {
    let (resource_id, version, raw, recoverable, external_id, physical_status) = row;
    Ok(BrowserRecordingFinalization {
        resource_id,
        version,
        state: serde_json::from_str(&raw)?,
        recoverable,
        external_id,
        physical_status,
    })
}
fn recovery_evidence_meta(resource: &str, outcome: &str, state: &Value) -> Value {
    use sha2::{Digest, Sha256};
    let manifest_metadata_hash = format!(
        "{:x}",
        Sha256::digest(state["manifest"].to_string().as_bytes())
    );
    json!({"resource_id":resource,"source_identity":state["identity"],"source_outcome":outcome,"recording_manifest_sha256":state["manifest"]["manifest_sha256"],"recording_manifest_metadata_sha256":manifest_metadata_hash,"recording_dispositions":state["dispositions"]})
}
fn recovery_evidence_proven(
    conn: &rusqlite::Connection,
    resource: &str,
    state: &Value,
) -> Result<bool, DbError> {
    let bundle = format!("recording-finalization:{resource}");
    if state["recoveryReceipt"] != json!({"bundle_id":bundle,"postprocessing":"completed"}) {
        return Ok(false);
    }
    let found: Option<(String,String)> = conn.query_row(
        "SELECT i.status,item.meta_json FROM execution_resources r JOIN tool_invocations i ON i.invocation_id=r.invocation_id AND i.run_id=r.run_id JOIN run_envelopes e ON e.id=r.run_id JOIN evidence_bundles b ON b.bundle_id=?2 AND b.session_id=e.session_id AND b.run_id IS NULL JOIN evidence_items item ON item.bundle_id=b.bundle_id AND item.id=?3 WHERE r.resource_id=?1 AND i.tool_name='VerifyJourney' AND i.status IN ('failed','cancelled','interrupted') AND b.kind='browser_recording_finalization' AND b.origin='machine' AND b.verdict='inconclusive' AND b.producer_invocation_id IS NULL AND item.type='recording_finalization' AND item.producer_invocation_id IS NULL AND json_extract(r.metadata_json,'$.kind')='browserSession' AND json_extract(r.metadata_json,'$.recordingFinalization.identity.session_id')=e.session_id AND json_extract(r.metadata_json,'$.recordingFinalization.identity.run_id')=r.run_id AND json_extract(r.metadata_json,'$.recordingFinalization.identity.invocation_id')=i.invocation_id",
        params![resource,bundle,format!("{bundle}:manifest")], |row| Ok((row.get(0)?,row.get(1)?))).optional()?;
    Ok(found.is_some_and(|(outcome, meta)| {
        serde_json::from_str::<Value>(&meta)
            .is_ok_and(|meta| meta == recovery_evidence_meta(resource, &outcome, state))
    }))
}
fn validate_recovery_dispositions(state: &Value, workspace: &str) -> Result<(), DbError> {
    let invalid = || DbError::Invalid("RECORDING_RECOVERY_DISPOSITIONS_INVALID".into());
    let files = state["manifest"]["files"].as_array().ok_or_else(invalid)?;
    let dispositions = state["dispositions"].as_array().ok_or_else(invalid)?;
    if files.len() > 50 || files.len() != dispositions.len() {
        return Err(invalid());
    }
    let mut remaining = 20 * 1024 * 1024u64;
    for (file, disposition) in files.iter().zip(dispositions) {
        if file["path"] != disposition["path"]
            || file["kind"] != disposition["kind"]
            || file["size"] != disposition["size"]
        {
            return Err(invalid());
        }
        match disposition["status"].as_str() {
            Some("archived")
                if file["status"] == "available"
                    && disposition["blob_sha256"] == file["sha256"] =>
            {
                remaining = remaining
                    .checked_sub(file["size"].as_u64().ok_or_else(invalid)?)
                    .ok_or_else(invalid)?;
                verify_recovery_blob(workspace, file)?;
            }
            Some("missing")
                if file["status"] == "missing"
                    && disposition["error_code"] == file["error_code"] => {}
            Some("omitted_budget")
                if matches!(
                    disposition["error_code"].as_str(),
                    Some("RECORDING_BYTE_BUDGET_EXCEEDED" | "RECORDING_ITEM_BUDGET_EXCEEDED")
                ) => {}
            _ => return Err(invalid()),
        }
    }
    Ok(())
}
fn verify_recovery_blob(workspace: &str, item: &Value) -> Result<(), DbError> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let invalid = || DbError::Invalid("RECORDING_RECOVERY_BLOB_INVALID".into());
    let digest = item["sha256"].as_str().ok_or_else(invalid)?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid());
    }
    let workspace = std::fs::canonicalize(workspace).map_err(|_| invalid())?;
    let parent = std::fs::canonicalize(workspace.join(".zk/blobs").join(&digest[..2]))
        .map_err(|_| invalid())?;
    if !parent.starts_with(&workspace) {
        return Err(invalid());
    }
    let fd = nix::fcntl::open(
        &parent.join(digest),
        nix::fcntl::OFlag::O_RDONLY
            | nix::fcntl::OFlag::O_NOFOLLOW
            | nix::fcntl::OFlag::O_NONBLOCK
            | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(|_| invalid())?;
    let file = std::fs::File::from(fd);
    let metadata = file.metadata().map_err(|_| invalid())?;
    if !metadata.is_file()
        || metadata.len() > 10 * 1024 * 1024
        || Some(metadata.len()) != item["size"].as_u64()
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take(10 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    if format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(invalid());
    }
    Ok(())
}

/// Must run in the deletion transaction; protects resource metadata against FK
/// cascades until the matching private-spool batch was actually consumed.
pub(crate) fn ensure_recordings_consumed(
    conn: &rusqlite::Connection,
    session: &str,
) -> Result<(), DbError> {
    let pending: bool = conn.query_row("WITH RECURSIVE tree(id) AS (SELECT id FROM sessions WHERE id=?1 UNION SELECT s.id FROM sessions s JOIN tree ON s.parent_session_id=tree.id) SELECT EXISTS(SELECT 1 FROM execution_resources r JOIN run_envelopes e ON e.id=r.run_id JOIN tree ON tree.id=e.session_id WHERE json_extract(r.metadata_json,'$.recordingFinalization.phase') IS NOT NULL AND json_extract(r.metadata_json,'$.recordingFinalization.phase')<>'acknowledged')", [session], |row| row.get(0))?;
    if pending {
        return Err(DbError::Conflict(
            "SESSION_RECORDING_FINALIZATION_PENDING".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CleanupStatus, CommitToolInvocationResult, EvidenceBundleRecord, EvidenceItemRecord,
        EvidenceOrigin, ExecutionResourceStatus, NewExecutionResource, NewToolInvocation,
        ToolInvocationStatus,
    };
    #[tokio::test]
    async fn deleting_parent_protects_child_recordings_after_physical_release() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn_blocking(|conn| {
            conn.execute_batch(r#"INSERT INTO sessions(id,model,working_dir,created_at,updated_at) VALUES('parent','m','/tmp','now','now');
                INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at) VALUES('root','parent','root','fixture','running','now','now');
                INSERT INTO tasks(id,session_id,parent_task_id,root_task_id,description,status,created_at,updated_at) VALUES('child-task','parent','root','root','fixture','running','now','now');
                INSERT INTO sessions(id,kind,parent_session_id,parent_task_id,model,working_dir,created_at,updated_at) VALUES('child','internal','parent','child-task','m','/tmp','now','now');
                INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at) VALUES('nested-root','child','nested-root','fixture','running','now','now');
                INSERT INTO sessions(id,kind,parent_session_id,parent_task_id,model,working_dir,created_at,updated_at) VALUES('grandchild','internal','child','nested-root','m','/tmp','now','now');
                INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,created_at,updated_at) VALUES('root-run','parent','root','running','m','now','now','now');
                INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,created_at,updated_at) VALUES('child-run','child','child-task','running','m','now','now','now');
                INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,created_at,updated_at) VALUES('deep-run','grandchild','nested-root','running','m','now','now','now');
                UPDATE tasks SET current_run_id=CASE id WHEN 'root' THEN 'root-run' WHEN 'child-task' THEN 'child-run' ELSE 'deep-run' END;
                INSERT INTO task_results(result_id,task_id,run_id,result_version,status,inline_text,byte_len,content_sha256,created_at) SELECT id||'-result',id,current_run_id,1,'cancelled','',0,'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855','now' FROM tasks;
                UPDATE run_envelopes SET status='cancelled',terminal_at='now',finished_at='now',cleanup_status='confirmed';
                UPDATE tasks SET status='cancelled',terminal_at='now',cleanup_status='confirmed';
                INSERT INTO tool_invocations(invocation_id,task_id,run_id,tool_use_id,tool_name,status,terminal_at,cleanup_status,created_at,updated_at) VALUES('invoke','nested-root','deep-run','tool','VerifyJourney','cancelled','now','confirmed','now','now');
                INSERT INTO execution_resources(resource_id,task_id,run_id,invocation_id,resource_kind,status,metadata_json,created_at,updated_at,released_at) VALUES('recording','nested-root','deep-run','invoke','stream','released','{"recordingFinalization":{"version":1,"phase":"sealed"}}','now','now','now');
                INSERT INTO execution_resources(resource_id,task_id,run_id,invocation_id,resource_kind,status,metadata_json,created_at,updated_at,released_at) VALUES('browser-lease','nested-root','deep-run','invoke','stream','released','{"kind":"browserUsageLease"}','now','now','now');"#)?;
            Ok(())
        }).unwrap();
        assert_eq!(
            db.session_browser_owner_ids_for_deletion("parent")
                .await
                .unwrap(),
            ["grandchild"]
        );
        for phase in ["reserved", "sealed", "ackEligible"] {
            db.with_conn_blocking(move |conn| { conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.phase',?1) WHERE resource_id='recording'",[phase])?;Ok(()) }).unwrap();
            let error = db.delete_session("parent").await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("SESSION_RECORDING_FINALIZATION_PENDING"),
                "{error}"
            );
        }
        db.with_conn_blocking(|conn| {conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.phase','acknowledged') WHERE resource_id='recording'",[])?;Ok(())}).unwrap();
        assert!(db.delete_session("parent").await.unwrap());
        let count: i64 = db
            .with_conn_blocking(|conn| {
                Ok(
                    conn.query_row("SELECT count(*) FROM execution_resources", [], |row| {
                        row.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn terminal_reserved_recordings_are_recoverable_but_running_ones_are_not() {
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session("m", "/tmp").await.unwrap();
        db.start_run("recovery-run", &session.id, None, Some("query"), "m")
            .await
            .unwrap();
        let run = db.find_run_by_id("recovery-run").await.unwrap().unwrap();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: "recovery-invocation".into(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            tool_use_id: "tool".into(),
            tool_name: "VerifyJourney".into(),
            input_json: Some("{}".into()),
            side_effect_class: "read".into(),
            directory_generation: None,
            connection_generation: None,
        })
        .await
        .unwrap();
        db.register_execution_resource(&NewExecutionResource {
            resource_id: "recovery-resource".into(), task_id: run.task_id, run_id: run.id, invocation_id: Some("recovery-invocation".into()), resource_kind: "stream".into(), external_id: Some("rv-recovery".into()), metadata_json: json!({"kind":"browserSession","recordingFinalization":{"version":1,"phase":"reserved","identity":{"batch_id":"00000000-0000-4000-8000-000000000001","session_id":session.id,"run_id":"recovery-run","invocation_id":"recovery-invocation"}}}).to_string(),
        }).await.unwrap();
        assert!(db.pending_browser_recordings().await.unwrap().is_empty());
        db.commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: "recovery-invocation".into(),
            expected_version: 0,
            session_id: session.id,
            target: ToolInvocationStatus::Cancelled,
            input_json: None,
            content: "cancelled".into(),
            is_error: true,
            metadata: None,
            output_sha256: None,
            error_code: Some("CANCELLED".into()),
            cleanup_status: CleanupStatus::Unconfirmed,
            postprocessing: None,
        })
        .await
        .unwrap();
        assert_eq!(
            db.pending_browser_recordings().await.unwrap().len(),
            1,
            "terminal reserved batch needs a recovery owner"
        );
    }

    async fn recovery_fixture(outcome: ToolInvocationStatus, workspace: &str) -> (Db, String) {
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session("m", workspace).await.unwrap().id;
        db.start_run("recovery-run", &session, None, Some("query"), "m")
            .await
            .unwrap();
        let run = db.find_run_by_id("recovery-run").await.unwrap().unwrap();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: "recovery-invocation".into(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            tool_use_id: "tool".into(),
            tool_name: "VerifyJourney".into(),
            input_json: Some("{}".into()),
            side_effect_class: "read".into(),
            directory_generation: None,
            connection_generation: None,
        })
        .await
        .unwrap();
        let identity = json!({"batch_id":"00000000-0000-4000-8000-000000000001","session_id":session,"run_id":run.id,"invocation_id":"recovery-invocation"});
        db.register_execution_resource(&NewExecutionResource {resource_id:"recovery-resource".into(),task_id:run.task_id,run_id:run.id,invocation_id:Some("recovery-invocation".into()),resource_kind:"stream".into(),external_id:Some("rv-recovery".into()),metadata_json:json!({"kind":"browserSession","recordingFinalization":{"version":1,"phase":"reserved","identity":identity}}).to_string()}).await.unwrap();
        db.finalize_execution_resource("recovery-resource", ExecutionResourceStatus::Released)
            .await
            .unwrap();
        db.commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: "recovery-invocation".into(),
            expected_version: 0,
            session_id: session.clone(),
            target: outcome,
            input_json: Some("{}".into()),
            content: "original result must remain unchanged".into(),
            is_error: true,
            metadata: None,
            output_sha256: None,
            error_code: Some("ORIGINAL_OUTCOME".into()),
            cleanup_status: CleanupStatus::Confirmed,
            postprocessing: None,
        })
        .await
        .unwrap();
        let file = json!({"path":"network.har","kind":"har","status":"missing","error_code":"RECORDING_NOT_CREATED"});
        db.seal_browser_recording(
            "recovery-resource",
            json!({"identity":identity,"manifest_sha256":"a".repeat(64),"files":[file]}),
            json!([file]),
        )
        .await
        .unwrap();
        (db, session)
    }

    #[tokio::test]
    async fn failed_recording_receipt_is_atomic_idempotent_and_does_not_rewrite_tool_result() {
        for outcome in [
            ToolInvocationStatus::Failed,
            ToolInvocationStatus::Cancelled,
            ToolInvocationStatus::Interrupted,
        ] {
            let (db, session) = recovery_fixture(outcome, "/tmp").await;
            let entry = db
                .browser_recording_finalization("recovery-resource")
                .await
                .unwrap()
                .unwrap();
            assert!(
                !db.advance_browser_recording(&entry.resource_id, entry.version, false)
                    .await
                    .unwrap()
            );
            db.with_conn_blocking(|conn| { conn.execute_batch("CREATE TEMP TRIGGER block_recording_receipt BEFORE INSERT ON evidence_items BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END;")?; Ok(()) }).unwrap();
            assert!(
                db.complete_failed_browser_recording("recovery-resource")
                    .await
                    .is_err()
            );
            assert!(
                db.find_evidence_by_session(&session)
                    .await
                    .unwrap()
                    .is_empty()
            );
            db.with_conn_blocking(|conn| {
                conn.execute_batch("DROP TRIGGER block_recording_receipt;")?;
                Ok(())
            })
            .unwrap();
            let (first, second) = tokio::join!(
                db.complete_failed_browser_recording("recovery-resource"),
                db.complete_failed_browser_recording("recovery-resource")
            );
            first.unwrap();
            second.unwrap();
            let bundles = db.find_evidence_by_session(&session).await.unwrap();
            assert_eq!(bundles.len(), 1);
            assert_eq!(bundles[0].verdict, "inconclusive");
            assert!(bundles[0].producer_invocation_id.is_none());
            let entry = db
                .browser_recording_finalization("recovery-resource")
                .await
                .unwrap()
                .unwrap();
            assert!(
                db.advance_browser_recording(&entry.resource_id, entry.version, false)
                    .await
                    .unwrap()
            );
            let observed = db.with_conn_blocking(|conn| Ok(conn.query_row("SELECT status,error_code,(SELECT count(*) FROM tool_result_postprocessing) FROM tool_invocations WHERE invocation_id='recovery-invocation'",[],|row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?)))?)).unwrap();
            assert_eq!(
                observed,
                (outcome.as_db().into(), "ORIGINAL_OUTCOME".into(), 0)
            );
        }
    }

    #[tokio::test]
    async fn recovery_receipt_rejects_successful_foreign_and_forged_owners() {
        let (db, _) = recovery_fixture(ToolInvocationStatus::Succeeded, "/tmp").await;
        assert!(
            db.complete_failed_browser_recording("recovery-resource")
                .await
                .is_err()
        );
        let (db, _) = recovery_fixture(ToolInvocationStatus::Cancelled, "/tmp").await;
        db.with_conn_blocking(|conn| {
            conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.identity.session_id','other-session') WHERE resource_id='recovery-resource'",[])?; Ok(())
        }).unwrap();
        assert!(
            db.complete_failed_browser_recording("recovery-resource")
                .await
                .is_err()
        );
        let (db, _) = recovery_fixture(ToolInvocationStatus::Cancelled, "/tmp").await;
        db.with_conn_blocking(|conn| { conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.recoveryReceipt',json('{\"bundle_id\":\"recording-finalization:recovery-resource\",\"postprocessing\":\"completed\"}')) WHERE resource_id='recovery-resource'",[])?; Ok(()) }).unwrap();
        let entry = db
            .browser_recording_finalization("recovery-resource")
            .await
            .unwrap()
            .unwrap();
        assert!(
            !db.advance_browser_recording(&entry.resource_id, entry.version, false)
                .await
                .unwrap(),
            "an asserted receipt without exact durable evidence cannot ACK"
        );
        db.complete_failed_browser_recording("recovery-resource")
            .await
            .unwrap();
        db.with_conn_blocking(|conn| { conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.manifest.manifest_sha256',?1)", ["b".repeat(64)])?; Ok(()) }).unwrap();
        let entry = db
            .browser_recording_finalization("recovery-resource")
            .await
            .unwrap()
            .unwrap();
        assert!(
            !db.advance_browser_recording(&entry.resource_id, entry.version, false)
                .await
                .unwrap(),
            "changing a manifest invalidates the immutable consumption proof"
        );
        assert!(
            db.complete_failed_browser_recording("recovery-resource")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn recovery_receipt_requires_actual_matching_archive_blob() {
        use sha2::{Digest, Sha256};
        let workspace =
            std::env::temp_dir().join(format!("zk-recording-db-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&workspace).unwrap();
        let (db, _) =
            recovery_fixture(ToolInvocationStatus::Failed, workspace.to_str().unwrap()).await;
        let bytes = b"archived recording";
        let digest = format!("{:x}", Sha256::digest(bytes));
        let file = json!({"path":"network.har","kind":"har","status":"available","size":bytes.len(),"sha256":digest});
        let disposition = json!({"path":"network.har","kind":"har","status":"archived","size":bytes.len(),"blob_sha256":digest});
        db.with_conn_blocking(move |conn| { conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.manifest.files',json(?1),'$.recordingFinalization.dispositions',json(?2))",params![json!([file]).to_string(),json!([disposition]).to_string()])?; Ok(()) }).unwrap();
        assert!(
            db.complete_failed_browser_recording("recovery-resource")
                .await
                .is_err()
        );
        let parent = workspace.join(".zk/blobs").join(&digest[..2]);
        std::fs::create_dir_all(&parent).unwrap();
        std::fs::write(parent.join(&digest), b"corrupt").unwrap();
        assert!(
            db.complete_failed_browser_recording("recovery-resource")
                .await
                .is_err()
        );
        std::fs::write(parent.join(&digest), bytes).unwrap();
        db.complete_failed_browser_recording("recovery-resource")
            .await
            .unwrap();
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "Single real transaction/evidence lifecycle fixture"
    )]
    async fn recording_ack_requires_real_evidence_and_completed_postprocessing() {
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session("m", "/tmp").await.unwrap();
        db.start_run("record-run", &session.id, None, Some("query"), "m")
            .await
            .unwrap();
        let run = db.find_run_by_id("record-run").await.unwrap().unwrap();
        let invocation = "record-invocation";
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation.into(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            tool_use_id: "record-tool".into(),
            tool_name: "VerifyJourney".into(),
            input_json: Some("{}".into()),
            side_effect_class: "read".into(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .unwrap();
        db.transition_tool_invocation_cas(
            invocation,
            0,
            ToolInvocationStatus::Running,
            Some("{}"),
            None,
            None,
            CleanupStatus::Pending,
        )
        .await
        .unwrap();
        let identity = json!({"batch_id":"00000000-0000-4000-8000-000000000001","session_id":session.id,"run_id":run.id,"invocation_id":invocation});
        db.register_execution_resource(&NewExecutionResource {resource_id:"record-resource".into(),task_id:run.task_id.clone(),run_id:run.id.clone(),invocation_id:Some(invocation.into()),resource_kind:"stream".into(),external_id:Some("browser".into()),metadata_json:json!({"recordingFinalization":{"version":1,"phase":"reserved","identity":identity}}).to_string()}).await.unwrap();
        db.finalize_execution_resource("record-resource", ExecutionResourceStatus::Released)
            .await
            .unwrap();
        let digest = "a".repeat(64);
        let dispositions = json!([{"path":"network.har","status":"omitted_budget","error_code":"RECORDING_BYTE_BUDGET_EXCEEDED"}]);
        db.seal_browser_recording(
            "record-resource",
            json!({"identity":identity,"manifest_sha256":digest,"files":[]}),
            dispositions.clone(),
        )
        .await
        .unwrap();
        let entry = db.pending_browser_recordings().await.unwrap().remove(0);
        assert!(
            !db.advance_browser_recording(&entry.resource_id, entry.version, false)
                .await
                .unwrap()
        );
        db.commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: invocation.into(),
            expected_version: 1,
            session_id: session.id.clone(),
            target: ToolInvocationStatus::Succeeded,
            input_json: Some("{}".into()),
            content: "Journey completed, recording omitted by budget".into(),
            is_error: false,
            metadata: None,
            output_sha256: None,
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            postprocessing: Some(json!({"evidence":true})),
        })
        .await
        .unwrap();
        db.save_evidence_bundle(&EvidenceBundleRecord {bundle_id:"record-evidence".into(),session_id:session.id.clone(),agent_id:None,kind:"browser_journey".into(),claim:None,origin:EvidenceOrigin::Machine,producer_invocation_id:Some(invocation.into()),verdict:"verified".into(),created_at:"2026-10-08T00:00:00.000000Z".into(),run_id:Some(run.id),items:vec![EvidenceItemRecord{id:"record-item".into(),producer_invocation_id:Some(invocation.into()),item_type:"journey_recording".into(),summary:Some("Recording omitted by budget".into()),blob_sha256:None,meta:Some(json!({"recording_manifest_sha256":digest,"recording_dispositions":dispositions})),sort_order:0}]}).await.unwrap();
        assert!(
            !db.advance_browser_recording(&entry.resource_id, entry.version, false)
                .await
                .unwrap()
        );
        db.complete_tool_result_postprocessing_cas(invocation, 0)
            .await
            .unwrap();
        assert!(
            db.advance_browser_recording(&entry.resource_id, entry.version, false)
                .await
                .unwrap()
        );
        assert!(
            !db.advance_browser_recording(&entry.resource_id, entry.version, true)
                .await
                .unwrap(),
            "stale version cannot consume another phase"
        );
        let entry = db.pending_browser_recordings().await.unwrap().remove(0);
        let guarded = db.with_conn_blocking({
            let session = session.id.clone();
            move |conn| ensure_recordings_consumed(conn, &session)
        });
        assert!(
            guarded.is_err(),
            "released browser does not mean recording consumed"
        );
        assert!(
            db.advance_browser_recording(&entry.resource_id, entry.version, true)
                .await
                .unwrap()
        );
        assert!(db.pending_browser_recordings().await.unwrap().is_empty());
        db.with_conn_blocking(move |conn| ensure_recordings_consumed(conn, &session.id))
            .unwrap();
    }
    #[tokio::test]
    async fn not_created_requires_matching_owner_and_confirmed_physical_release() {
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session("m", "/tmp").await.unwrap();
        db.start_run("absent-run", &session.id, None, Some("query"), "m")
            .await
            .unwrap();
        let run = db.find_run_by_id("absent-run").await.unwrap().unwrap();
        let identity = json!({"batch_id":"00000000-0000-4000-8000-000000000002","session_id":session.id,"run_id":run.id,"invocation_id":"absent-invocation"});
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: "absent-invocation".into(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            tool_use_id: "absent-tool".into(),
            tool_name: "VerifyJourney".into(),
            input_json: Some("{}".into()),
            side_effect_class: "read".into(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .unwrap();
        db.register_execution_resource(&NewExecutionResource {resource_id:"absent-resource".into(),task_id:run.task_id,run_id:run.id,invocation_id:Some("absent-invocation".into()),resource_kind:"stream".into(),external_id:Some("browser".into()),metadata_json:json!({"recordingFinalization":{"version":1,"phase":"reserved","identity":identity}}).to_string()}).await.unwrap();
        let proof = json!({"phase":"not_created","identity":identity});
        assert!(
            db.acknowledge_uncreated_browser_recording("absent-resource", proof.clone())
                .await
                .is_err()
        );
        db.finalize_execution_resource("absent-resource", ExecutionResourceStatus::Released)
            .await
            .unwrap();
        let mut wrong = proof.clone();
        wrong["identity"]["run_id"] = json!("other-run");
        assert!(
            db.acknowledge_uncreated_browser_recording("absent-resource", wrong)
                .await
                .is_err()
        );
        // Matching JSON alone is insufficient: the durable row is the owner.
        db.with_conn_blocking(|conn| {
            conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.identity.run_id','other-run') WHERE resource_id='absent-resource'", [])?;
            Ok(())
        }).unwrap();
        let mut forged = proof.clone();
        forged["identity"]["run_id"] = json!("other-run");
        assert!(
            db.acknowledge_uncreated_browser_recording("absent-resource", forged)
                .await
                .is_err()
        );
        db.with_conn_blocking(|conn| {
            conn.execute("UPDATE execution_resources SET metadata_json=json_set(metadata_json,'$.recordingFinalization.identity.run_id','absent-run') WHERE resource_id='absent-resource'", [])?;
            Ok(())
        }).unwrap();
        assert!(
            db.browser_recording_batch_protected(identity["batch_id"].as_str().unwrap())
                .await
                .unwrap()
        );
        db.acknowledge_uncreated_browser_recording("absent-resource", proof.clone())
            .await
            .unwrap();
        db.acknowledge_uncreated_browser_recording("absent-resource", proof)
            .await
            .unwrap();
        assert!(
            !db.browser_recording_batch_protected(identity["batch_id"].as_str().unwrap())
                .await
                .unwrap()
        );
    }
}
