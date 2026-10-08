//! Replay only a persistent recorded verifier's evidence obligation. No tool runs here.
use zk_db::{Db, EvidenceBundleRecord, EvidenceItemRecord, EvidenceOrigin};
use zk_tools::{EvidenceReceipt, EvidenceReceiptVerdict};

/// Complete a durable, evidence-only recorded `VerifyJourney` obligation.
/// `false` leaves all other tools/retention policies on their existing path.
///
/// # Errors
/// Invalid receipts, conflicting evidence and persistence failures retain the journal.
pub async fn complete_recorded_verify_journey_evidence(
    db: &Db,
    invocation_id: &str,
) -> Result<bool, String> {
    let Some(snapshot) = db
        .recorded_journey_postprocessing(invocation_id)
        .await
        .map_err(|error| format!("RECORDING_POSTPROCESSING_READ_FAILED: {error}"))?
    else {
        return Ok(false);
    };
    let receipt: EvidenceReceipt = serde_json::from_value(snapshot.receipt.clone())
        .map_err(|_| "RECORDING_POSTPROCESSING_RECEIPT_INVALID")?;
    if !receipt.is_valid()
        || (receipt.verdict == EvidenceReceiptVerdict::Failed) != snapshot.output_is_error
    {
        return Err("RECORDING_POSTPROCESSING_RECEIPT_INVALID".into());
    }
    let bundle_id = format!("recorded-journey:{invocation_id}");
    let bundle = EvidenceBundleRecord {
        bundle_id: bundle_id.clone(),
        session_id: snapshot.session_id.clone(),
        agent_id: None,
        kind: receipt.kind,
        claim: receipt.claim,
        origin: EvidenceOrigin::Machine,
        producer_invocation_id: Some(snapshot.invocation_id.clone()),
        verdict: receipt.verdict.as_db().into(),
        created_at: receipt.observed_at,
        run_id: Some(snapshot.run_id.clone()),
        items: receipt
            .items
            .into_iter()
            .map(|item| EvidenceItemRecord {
                id: format!("{bundle_id}:{}", item.sort_order),
                producer_invocation_id: Some(snapshot.invocation_id.clone()),
                item_type: item.item_type,
                summary: item.summary,
                blob_sha256: item.blob_sha256,
                meta: item.meta,
                sort_order: i64::from(item.sort_order),
            })
            .collect(),
    };
    db.complete_recorded_journey_postprocessing(&snapshot, &bundle)
        .await
        .map_err(|error| format!("RECORDING_POSTPROCESSING_COMMIT_FAILED: {error}"))?;
    Ok(true)
}
