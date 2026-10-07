//! Checkpoint and write-before-image bodies never enter `SQLite`, its WAL, or hashes.

use serde_json::json;
use sha2::{Digest, Sha256};
use zk_db::{Db, new_agent_checkpoint};

async fn assert_raw_sql_cannot_bypass_retention(db: &Db, session: &str, marker: &str) {
    for sql in [
        "UPDATE agent_checkpoints SET messages_json=?1 WHERE session_id=?2",
        "UPDATE agent_checkpoints SET file_state_json=?1 WHERE session_id=?2",
        "UPDATE file_snapshots SET content=CAST(?1 AS BLOB) WHERE session_id=?2",
        "UPDATE file_snapshots SET original_bytes=CAST(?1 AS BLOB) WHERE session_id=?2",
        "INSERT INTO agent_checkpoints(id,run_id,session_id,agent_id,seq,messages_json,created_at) VALUES('raw-checkpoint','snapshot-run',?2,'fixture-agent',9,?1,'now')",
        "INSERT INTO file_snapshots(id,session_id,file_path,content,created_at) VALUES('raw-file',?2,'/tmp/file',CAST(?1 AS BLOB),'now')",
    ] {
        let (secret, session) = (marker.to_owned(), session.to_owned());
        assert!(
            db.with_writer(move |conn| {
                conn.execute(sql, rusqlite::params![secret, session])?;
                Ok(())
            })
            .await
            .is_err(),
            "raw SQL must not bypass retention: {sql}"
        );
    }
}

fn assert_disk_has_no_snapshot_content(root: &std::path::Path, binary: &[u8], needles: &[&str]) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let bytes = std::fs::read(&path).unwrap();
            assert!(
                !bytes.windows(binary.len()).any(|part| part == binary),
                "original file bytes leaked to {}",
                path.display()
            );
            for needle in needles {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|part| part == needle.as_bytes()),
                    "snapshot body or content hash leaked to {}",
                    path.display()
                );
            }
        }
    }
}

#[tokio::test]
async fn ephemeral_checkpoints_and_file_images_are_live_only_and_sql_guarded() {
    let root = std::env::temp_dir().join(format!("zk-snapshot-content-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let database = root.join("snapshots.sqlite");
    let db = Db::open(&database).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", "/tmp", "DONT_ASK")
        .await
        .unwrap();
    let marker = format!("snapshot-private-body-{}", uuid::Uuid::new_v4());
    let hash = format!("{:x}", Sha256::digest(marker.as_bytes()));
    let binary = [
        vec![0xff, 0xfe],
        marker.encode_utf16().flat_map(u16::to_le_bytes).collect(),
    ]
    .concat();
    let binary_hash = format!("{:x}", Sha256::digest(&binary));
    db.start_run("snapshot-run", &session, None, None, "fixture")
        .await
        .unwrap();
    let mut checkpoint = new_agent_checkpoint(
        "snapshot-run",
        &session,
        "fixture-agent",
        1,
        json!({"kind":"contextCheckpoint","messages":[{"text":marker}]}),
    );
    checkpoint.file_state = Some(json!({"content": marker, "sha256": hash}));
    db.save_agent_checkpoint(&checkpoint).await.unwrap();
    db.insert_file_snapshot_with_bytes(
        &session,
        Some("turn"),
        "/tmp/user-artifact.txt",
        &marker,
        "edit",
        Some(&binary),
    )
    .await
    .unwrap();
    let loaded = db
        .latest_agent_checkpoint("snapshot-run")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.messages, checkpoint.messages);
    assert_eq!(loaded.file_state, checkpoint.file_state);
    assert!(loaded.messages.get("recoveryProof").is_none());
    assert_eq!(
        db.list_file_snapshots(&session).await.unwrap()[0]
            .original_bytes
            .as_deref(),
        Some(binary.as_slice())
    );
    assert_eq!(
        db.list_by_message_id(&session, "turn").await.unwrap()[0].content,
        marker
    );
    assert_eq!(
        db.latest_file_snapshot(&session, "/tmp/user-artifact.txt")
            .await
            .unwrap()
            .unwrap()
            .content,
        marker
    );

    assert_raw_sql_cannot_bypass_retention(&db, &session, &marker).await;
    assert_disk_has_no_snapshot_content(&root, &binary, &[&marker, &hash, &binary_hash]);
    let reopened = Db::open(&database).unwrap();
    assert!(
        reopened
            .latest_agent_checkpoint("snapshot-run")
            .await
            .is_err()
    );
    assert!(reopened.list_file_snapshots(&session).await.is_err());
    drop(lease);
    assert_eq!(db.memory_content_store().retained_bytes(), 0);
    assert!(db.latest_agent_checkpoint("snapshot-run").await.is_err());
    assert!(db.list_file_snapshots(&session).await.is_err());
    assert!(db.save_agent_checkpoint(&checkpoint).await.is_err());
    drop(reopened);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
