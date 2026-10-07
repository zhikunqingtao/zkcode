//! The snapshot side channels respect retention without deleting user artifacts.

use zk_db::Db;
use zk_engine::{FileHistoryService, SessionSnapshot, SessionSnapshotService};

#[tokio::test]
async fn undo_restores_exact_encoded_bytes_for_persistent_and_temporary_snapshots() {
    use zk_tools::text_encoding::{TextEncoding, TextFormat};
    let root = std::env::temp_dir().join(format!("zk-encoded-undo-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    for temporary in [false, true] {
        for (encoding, bom) in [
            (TextEncoding::Utf8, true),
            (TextEncoding::Utf16Le, true),
            (TextEncoding::Utf16Be, true),
            (TextEncoding::Gb18030, false),
            (TextEncoding::Latin1, false),
        ] {
            let db = Db::open_in_memory().unwrap();
            let (session, lease) = if temporary {
                let (session, lease) = db
                    .create_ephemeral_session("fixture", root.to_str().unwrap(), "DONT_ASK")
                    .await
                    .unwrap();
                (session, Some(lease))
            } else {
                (
                    db.create_session("fixture", root.to_str().unwrap())
                        .await
                        .unwrap()
                        .id,
                    None,
                )
            };
            let format = TextFormat { encoding, bom };
            let old = "café\r\nold\rlast\n";
            let bytes = format.encode(old).unwrap();
            let path = root.join(format!("{session}.txt"));
            db.insert_file_snapshot_with_bytes(
                &session,
                Some("turn"),
                path.to_str().unwrap(),
                old,
                "edit",
                Some(&bytes),
            )
            .await
            .unwrap();
            let after = format.encode("é\r\nnew\n").unwrap();
            std::fs::write(&path, &after).unwrap();
            let result = FileHistoryService::new(db.clone())
                .rewind_files(&session, "turn", None)
                .await;
            assert!(result.success, "{encoding:?}: {:?}", result.errors);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            let snapshots = db.list_file_snapshots(&session).await.unwrap();
            let reverse = snapshots
                .iter()
                .find(|entry| entry.operation == "rewind")
                .unwrap();
            assert_eq!(reverse.original_bytes.as_deref(), Some(after.as_slice()));
            drop(lease);
            assert_eq!(
                std::fs::read(&path).unwrap(),
                bytes,
                "scope cleanup must preserve the restored user file"
            );
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn temporary_session_snapshot_never_creates_a_file_and_expires_with_scope() {
    let root = std::env::temp_dir().join(format!("zk-memory-snapshot-{}", uuid::Uuid::new_v4()));
    let db = Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    let service = SessionSnapshotService::with_dir(root.clone()).with_database(db.clone());
    let mut snapshot = SessionSnapshot {
        session_id: Some(session.clone()),
        ..SessionSnapshot::default()
    };
    snapshot.metadata.insert(
        "privateBody".into(),
        serde_json::json!("temporary-snapshot-marker"),
    );
    service.save_snapshot(&session, &snapshot).await.unwrap();
    assert_eq!(
        service.load_snapshot(&session).await.unwrap(),
        Some(snapshot.clone())
    );
    assert!(std::fs::read_dir(&root).unwrap().next().is_none());
    assert!(service.list_snapshots().await.is_empty());

    let normal = db.create_session("fixture", "/tmp").await.unwrap();
    let persistent = SessionSnapshot {
        session_id: Some(normal.id.clone()),
        ..SessionSnapshot::default()
    };
    service
        .save_snapshot(&normal.id, &persistent)
        .await
        .unwrap();
    assert!(root.join(format!("{}.json", normal.id)).is_file());
    assert_eq!(service.list_snapshots().await.len(), 1);
    assert_eq!(
        service.load_snapshot(&normal.id).await.unwrap(),
        Some(persistent)
    );
    drop(lease);
    assert_eq!(db.memory_content_store().retained_bytes(), 0);
    assert!(service.load_snapshot(&session).await.is_err());
    assert!(service.save_snapshot(&session, &snapshot).await.is_err());
    assert!(!root.join(format!("{session}.json")).exists());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    assert!(
        service
            .save_snapshot("unknown-session", &snapshot)
            .await
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn temporary_file_history_supports_live_undo_and_preserves_authorized_artifacts() {
    let root =
        std::env::temp_dir().join(format!("zk-memory-file-history-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let db = Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", root.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let history = FileHistoryService::new(db.clone());
    let path = root.join("authorized-output.txt");
    std::fs::write(&path, "before authorized edit").unwrap();
    history
        .track_edit(path.to_str().unwrap(), &session, Some("turn"), "edit")
        .await
        .unwrap();
    std::fs::write(&path, "authorized edited output").unwrap();
    let result = history.rewind_files(&session, "turn", None).await;
    assert!(result.success, "{:?}", result.errors);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "before authorized edit"
    );
    assert!(
        db.list_file_snapshots(&session)
            .await
            .unwrap()
            .iter()
            .any(|snapshot| snapshot.content == "authorized edited output")
    );
    std::fs::write(&path, "final user artifact stays").unwrap();
    drop(lease);
    assert_eq!(db.memory_content_store().retained_bytes(), 0);
    assert!(db.list_file_snapshots(&session).await.is_err());
    assert!(!history.rewind_files(&session, "turn", None).await.success);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "final user artifact stays"
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    std::fs::remove_dir_all(root).unwrap();
}
