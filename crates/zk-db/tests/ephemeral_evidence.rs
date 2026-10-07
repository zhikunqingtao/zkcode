//! Evidence bodies and content hashes are in memory, with unchanged ownership and immutability.
use serde_json::json;
use sha2::{Digest, Sha256};
use zk_db::{Db, EvidenceBundleRecord, EvidenceItemRecord, EvidenceOrigin};

#[tokio::test]
#[allow(clippy::too_many_lines)] // One immutable evidence lifetime and actual DB/WAL scan.
async fn ephemeral_evidence_is_immutable_session_owned_and_absent_from_sqlite_and_wal() {
    let root = std::env::temp_dir().join(format!("zk-evidence-memory-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("evidence.sqlite");
    let db = Db::open(&path).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", root.to_str().unwrap(), "DEFAULT")
        .await
        .unwrap();
    let (other, _other) = db
        .create_ephemeral_session("fixture", root.to_str().unwrap(), "DEFAULT")
        .await
        .unwrap();
    let marker = format!("evidence-private-{}", uuid::Uuid::new_v4());
    let digest = format!("{:x}", Sha256::digest(marker.as_bytes()));
    let content = db.memory_content_store();
    content
        .put_named_bytes(&session, &format!("evidence:{digest}"), marker.as_bytes())
        .unwrap();
    assert!(
        content
            .get_named_bytes(&other, &format!("evidence:{digest}"))
            .is_err()
    );
    let bundle = EvidenceBundleRecord {
        bundle_id: uuid::Uuid::new_v4().to_string(),
        session_id: session.clone(),
        agent_id: None,
        kind: marker.clone(),
        claim: Some(marker.clone()),
        origin: EvidenceOrigin::ModelAssertion,
        producer_invocation_id: None,
        verdict: "pending".into(),
        created_at: "2026-10-07T00:00:00Z".into(),
        run_id: None,
        items: vec![EvidenceItemRecord {
            id: uuid::Uuid::new_v4().to_string(),
            producer_invocation_id: None,
            item_type: marker.clone(),
            summary: Some(marker.clone()),
            blob_sha256: Some(digest.clone()),
            meta: Some(json!({"body":marker,"hash":digest})),
            sort_order: 0,
        }],
    };
    db.save_evidence_bundle(&bundle).await.unwrap();
    let retained_before = content.retained_bytes();
    db.save_evidence_bundle(&bundle).await.unwrap();
    assert_eq!(
        retained_before,
        content.retained_bytes(),
        "idempotent retries must not consume memory again"
    );
    assert_eq!(
        db.find_evidence_bundle(&bundle.bundle_id).await.unwrap(),
        Some(bundle.clone())
    );
    assert!(db.evidence_owns_blob(&session, &digest).await.unwrap());
    assert!(!db.evidence_owns_blob(&other, &digest).await.unwrap());
    let mut changed = bundle.clone();
    changed.claim = Some("replacement".into());
    assert!(db.save_evidence_bundle(&changed).await.is_err());
    let (sid, secret) = (session.clone(), marker.clone());
    assert!(db.with_writer(move |conn| {
        conn.execute("INSERT INTO evidence_bundles(bundle_id,session_id,kind,origin,verdict,created_at) VALUES('raw-evidence',?1,?2,'modelAssertion','pending','now')",rusqlite::params![sid,secret])?;Ok(())
    }).await.is_err());
    db.update_evidence_verdict(&bundle.bundle_id, "verified")
        .await
        .unwrap();
    assert_eq!(
        db.find_evidence_verdict_events(&bundle.bundle_id)
            .await
            .unwrap()[0]
            .reason,
        "human_review"
    );
    for entry in std::fs::read_dir(&root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let bytes = std::fs::read(&path).unwrap();
            for needle in [&marker, &digest] {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|part| part == needle.as_bytes()),
                    "body or hash leaked: {}",
                    path.display()
                );
            }
        }
    }
    let reopened = Db::open(&path).unwrap();
    assert!(
        reopened
            .find_evidence_bundle(&bundle.bundle_id)
            .await
            .is_err()
    );
    drop(lease);
    assert!(db.find_evidence_bundle(&bundle.bundle_id).await.is_err());
    assert!(
        content
            .get_named_bytes(&session, &format!("evidence:{digest}"))
            .is_err()
    );
    assert_eq!(
        db.find_evidence_verdict_events(&bundle.bundle_id)
            .await
            .unwrap()[0]
            .reason,
        "EPHEMERAL_CONTENT_UNAVAILABLE"
    );
    drop(reopened);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
