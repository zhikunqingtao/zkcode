//! D-P2-4 读写分离并发压测——16 并发读 + 1 写循环（文件库 + WAL）。
//!
//! 验证目标：
//! 1. 无 `SQLITE_BUSY`：全程任一读/写操作不得返回错误（busy 会以
//!    `DbError::Sqlite` 浮出）；
//! 2. 数据一致：写终态 200 条消息 `seq_num` 恰为 1..=200 稠密序列；
//!    读侧每一页内 `seq_num` 严格递增（WAL 快照读不见半程状态）。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use zk_db::{MessageRole, NewMessage, StoredBlock};

/// 写循环总量（单 writer 串行追加）。
const WRITE_TOTAL: i64 = 200;
/// 并发读任务数（任务书判据：16 并发读）。
const READERS: usize = 16;

/// user 文本消息便捷构造。
fn user_msg(content: &str) -> NewMessage {
    NewMessage {
        role: MessageRole::User,
        content: vec![StoredBlock::Text {
            text: content.to_owned(),
        }],
        stop_reason: None,
        input_tokens: 0,
        output_tokens: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sixteen_readers_one_writer_no_busy_and_consistent() {
    let dir = std::env::temp_dir().join(format!("zk-db-conc-{}", uuid::Uuid::new_v4()));
    let db_path = dir.join("data.db");
    let db = zk_db::Db::open(&db_path).expect("open file db");
    let session = db
        .create_session("test-model", "/tmp/conc")
        .await
        .expect("create session");
    let sid = session.id.clone();

    let done = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));

    // 16 个读任务：写尚未收尾时持续轮询三条只读路径，任一 Err 即失败
    // （SQLITE_BUSY 必经 DbError 浮出，此处即为「无 BUSY」断言）。
    let mut readers = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let db = db.clone();
        let sid = sid.clone();
        let done = Arc::clone(&done);
        let total_reads = Arc::clone(&total_reads);
        readers.push(tokio::spawn(async move {
            while !done.load(Ordering::Acquire) {
                let page = db
                    .list_messages(&sid, None, 50)
                    .await
                    .expect("read must not hit SQLITE_BUSY")
                    .expect("session exists");
                // 页内一致性：seq 严格递增（快照读不见撕裂中间态）。
                for pair in page.messages.windows(2) {
                    assert!(
                        pair[0].seq_num < pair[1].seq_num,
                        "seq must be strictly increasing within a page"
                    );
                }
                db.list_sessions(None, 20)
                    .await
                    .expect("list_sessions must not hit SQLITE_BUSY");
                db.get_session(&sid)
                    .await
                    .expect("get_session must not hit SQLITE_BUSY")
                    .expect("session exists");
                total_reads.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // 单写循环：200 次追加（每次含 seq 原子分配 INSERT...SELECT
    // COALESCE(MAX)+1...RETURNING + touch 会话，全部走 writer）。
    for i in 0..WRITE_TOTAL {
        db.append_message(&sid, user_msg(&format!("msg-{i}")))
            .await
            .expect("write must not hit SQLITE_BUSY");
    }
    done.store(true, Ordering::Release);
    for handle in readers {
        handle.await.expect("reader task must not panic");
    }

    // 终态一致性：恰 200 条消息，seq_num 为 1..=200 稠密序列。
    let detail = db
        .get_session(&sid)
        .await
        .expect("final read")
        .expect("session exists");
    let seqs: Vec<i64> = detail.messages.iter().map(|m| m.seq_num).collect();
    assert_eq!(seqs, (1..=WRITE_TOTAL).collect::<Vec<_>>());
    // 读侧确实与写并发发生过（16 任务全程轮询，至少各完成一轮）。
    assert!(
        total_reads.load(Ordering::Relaxed) >= READERS as u64,
        "readers must have overlapped with the write loop"
    );

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
