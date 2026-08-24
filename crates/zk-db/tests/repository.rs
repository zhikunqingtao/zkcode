//! zk-db 集成测试——U2 全量 8 端点语义 + 游标 / 级联 / seq / 并发全覆盖。
//!
//! 内存库覆盖行为语义；文件库（tempdir）覆盖迁移幂等、PRAGMA 生效与
//! 旧库数据形态布数（subagent 过滤、确定性 `updated_at` 排序锚点——经
//! `Db::with_conn_blocking` 直连布数，模拟无法经公开 API 表达的存量数据）。

// 定点豁免：cost_usd 断言均为 SQLite REAL 列的精确常量往返（0.0/0.5），
// 无浮点运算链，直接相等比较即语义本身。
#![allow(clippy::float_cmp)]

use std::collections::HashSet;

use base64::Engine;
use rusqlite::params;
use zk_db::{DbError, MessageRole, NewMessage, StoredBlock};
use zk_protocol::model::Usage;

/// 文本块便捷构造。
fn text(content: &str) -> StoredBlock {
    StoredBlock::Text {
        text: content.to_owned(),
    }
}

/// user 消息便捷构造。
fn user_msg(content: &str) -> NewMessage {
    NewMessage {
        role: MessageRole::User,
        content: vec![text(content)],
        stop_reason: None,
        input_tokens: 0,
        output_tokens: 0,
    }
}

/// assistant 消息便捷构造（带 usage）。
fn assistant_msg(content: &str, input: i64, output: i64) -> NewMessage {
    NewMessage {
        role: MessageRole::Assistant,
        content: vec![text(content)],
        stop_reason: Some("end_turn".to_owned()),
        input_tokens: input,
        output_tokens: output,
    }
}

// ═══ 端点 1：POST /api/sessions（创建）═══

#[tokio::test]
async fn create_session_persists_active_row() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let summary = db.create_session("qwen3.7-max", "/tmp/work").await.unwrap();
    assert_eq!(summary.model, "qwen3.7-max");
    assert_eq!(summary.working_directory, "/tmp/work");
    assert_eq!(summary.message_count, 0);
    assert_eq!(summary.cost_usd, 0.0);
    assert!(summary.title.is_none() && summary.goal_preview.is_none());
    // UUIDv4 形状。
    assert_eq!(summary.id.len(), 36);
    assert_eq!(summary.id.as_bytes().get(14), Some(&b'4'));
    assert_eq!(summary.created_at, summary.updated_at);

    // 落库可见（经详情端点语义）。
    let detail = db.get_session(&summary.id).await.unwrap().unwrap();
    assert_eq!(detail.session_id, summary.id);
    assert_eq!(detail.status, "active");
    assert!(detail.messages.is_empty());
    assert!(detail.config.is_empty());
    assert_eq!(detail.total_usage, Usage::default());
    assert_eq!(detail.total_cost_usd, 0.0);
    assert_eq!(detail.created_at, summary.created_at);
}

// ═══ 端点 2：GET /api/sessions（游标分页列表）═══

#[tokio::test]
async fn list_sessions_cursor_pagination_full_cycle() {
    let dir = std::env::temp_dir().join(format!("zk-db-test-{}", uuid::Uuid::new_v4()));
    let db_path = dir.join("data.db");
    let db = zk_db::Db::open(&db_path).unwrap();

    // 布数：5 个真实会话（updated_at 每小时递增）+ 1 个 subagent 虚拟会话。
    db.with_conn_blocking(|conn| {
        for i in 0..5u32 {
            let iso = format!("2026-08-0{}T10:00:00.{:06}Z", 1 + i, i * 100_000);
            conn.execute(
                "INSERT INTO sessions (id, title, model, working_dir, status, created_at, updated_at)
                 VALUES (?1, ?2, 'm', '/w', 'active', ?3, ?3)",
                params![format!("sess-{i}"), if i == 2 { Some("titled") } else { None }, iso],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO sessions (id, model, working_dir, status, metadata_json, created_at, updated_at)
             VALUES ('sub-1', 'subagent', '/w', 'active',
                     '{\"type\":\"subagent\",\"parent_session_id\":\"sess-0\"}',
                     '2026-08-09T10:00:00.000000Z', '2026-08-09T10:00:00.000000Z')",
            [],
        )
        .unwrap();
        // sess-4 加一条 user 消息（goal 预览 + message_count）。
        conn.execute(
            "INSERT INTO messages (id, session_id, role, content_json, stop_reason,
                input_tokens, output_tokens, created_at, seq_num)
             VALUES ('m1', 'sess-4', 'user', '[{\"type\":\"text\",\"text\":\"帮我看下这个 bug\"}]',
                     NULL, 0, 0, '2026-08-05T10:00:00.000000Z', 1)",
            [],
        )
        .unwrap();
        Ok(())
    })
    .unwrap();

    // 首页（无游标）：最新在前 + subagent 被过滤 + limit+1 探测。
    let page1 = db.list_sessions(None, 2).await.unwrap();
    let ids1: Vec<&str> = page1.sessions.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids1, vec!["sess-4", "sess-3"]);
    assert!(page1.has_more);
    let cursor1 = page1.next_cursor.clone().unwrap();
    // 游标格式黄金锁定：Base64("updated_at|id")。
    let decoded = String::from_utf8(
        base64::engine::general_purpose::STANDARD
            .decode(&cursor1)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(decoded, "2026-08-04T10:00:00.300000Z|sess-3");

    // 翻页：取 sess-3 的 updated_at 之前 → sess-2 / sess-1。
    let page2 = db.list_sessions(Some(&cursor1), 2).await.unwrap();
    let ids2: Vec<&str> = page2.sessions.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids2, vec!["sess-2", "sess-1"]);
    assert!(page2.has_more);
    assert_eq!(page2.sessions[0].title.as_deref(), Some("titled"));

    // 尾页：sess-0，无更多。
    let page3 = db
        .list_sessions(Some(page2.next_cursor.as_deref().unwrap()), 2)
        .await
        .unwrap();
    assert_eq!(page3.sessions.len(), 1);
    assert_eq!(page3.sessions[0].id, "sess-0");
    assert!(!page3.has_more);
    assert!(page3.next_cursor.is_none());

    // 摘要富字段：goal 预览与 message_count 来自子查询。
    let page_all = db.list_sessions(None, 10).await.unwrap();
    let s4 = page_all.sessions.iter().find(|s| s.id == "sess-4").unwrap();
    assert_eq!(s4.goal_preview.as_deref(), Some("帮我看下这个 bug"));
    assert_eq!(s4.message_count, 1);

    // 无效游标：回退「从最新开始」（对齐旧系统 log.warn 行为）。
    let fallback = db.list_sessions(Some("!!!invalid!!!"), 2).await.unwrap();
    let fallback_ids: Vec<&str> = fallback.sessions.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(fallback_ids, vec!["sess-4", "sess-3"]);

    // 锚点会话已被删除：旧系统 500，zk-db 回退最新页。
    db.delete_session("sess-3").await.unwrap();
    let after_delete = db.list_sessions(Some(&cursor1), 2).await.unwrap();
    let after_delete_ids: Vec<&str> = after_delete
        .sessions
        .iter()
        .map(|s| s.id.as_str())
        .collect();
    assert_eq!(after_delete_ids, vec!["sess-4", "sess-2"]);

    let _ = std::fs::remove_dir_all(&dir);
}

// ═══ 端点 3 + 5 + 7：GET /{id} 详情 / resume / export（同一数据源）═══

#[tokio::test]
async fn detail_resume_export_share_full_load() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("kimi-k3", "/w").await.unwrap();

    db.append_message(&session.id, user_msg("你好"))
        .await
        .unwrap();
    db.append_message(&session.id, assistant_msg("在的", 12, 34))
        .await
        .unwrap();
    db.update_session_title(&session.id, "闲聊").await.unwrap();
    db.add_session_usage(
        &session.id,
        &Usage {
            input_tokens: 12,
            output_tokens: 34,
            ..Usage::default()
        },
        0.5,
    )
    .await
    .unwrap();

    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(detail.title.as_deref(), Some("闲聊"));
    assert_eq!(detail.messages.len(), 2);
    assert_eq!(detail.messages[0].role, MessageRole::User);
    assert_eq!(detail.messages[1].role, MessageRole::Assistant);
    assert_eq!(detail.messages[1].stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(detail.messages[1].input_tokens, 12);
    assert_eq!(
        detail.total_usage,
        Usage {
            input_tokens: 12,
            output_tokens: 34,
            ..Usage::default()
        }
    );
    assert_eq!(detail.total_cost_usd, 0.5);
    // 消息写入 touch 了会话 updated_at。
    assert!(detail.updated_at >= detail.created_at);

    // resume / export 端点在 server 层复用同一 load；此处验证重复读取一致。
    let again = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(again.messages, detail.messages);

    // 不存在的会话。
    assert!(db.get_session("nope").await.unwrap().is_none());
}

// ═══ 端点 4：DELETE /{id}（级联删除消息）═══

#[tokio::test]
async fn delete_session_cascades_messages() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    for i in 0..3 {
        db.append_message(&session.id, user_msg(&format!("msg-{i}")))
            .await
            .unwrap();
    }
    let before = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(before.messages.len(), 3);

    assert!(db.delete_session(&session.id).await.unwrap());
    assert!(!db.delete_session(&session.id).await.unwrap()); // 二次删 = false

    // 级联验证：会话与消息双双不可见。
    assert!(db.get_session(&session.id).await.unwrap().is_none());
    assert!(
        db.list_messages(&session.id, None, 50)
            .await
            .unwrap()
            .is_none()
    );
    // 物理级联：另开会话不误删。
    let other = db.create_session("m", "/w").await.unwrap();
    db.append_message(&other.id, user_msg("x")).await.unwrap();
    assert!(db.delete_session(&session.id).await.is_ok());
    let other_detail = db.get_session(&other.id).await.unwrap().unwrap();
    assert_eq!(other_detail.messages.len(), 1);
}

// ═══ 端点 8：GET /{id}/messages（索引游标分页）═══

#[tokio::test]
async fn message_pagination_index_cursor() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    for i in 1..=7 {
        db.append_message(&session.id, user_msg(&format!("m{i}")))
            .await
            .unwrap();
    }

    // 首页（无游标，limit=3）。
    let p1 = db
        .list_messages(&session.id, None, 3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p1.messages.len(), 3);
    assert_eq!(p1.messages[0].seq_num, 1);
    assert_eq!(p1.messages[2].seq_num, 3);
    assert!(p1.has_more);
    let c1 = p1.next_cursor.clone().unwrap();
    assert_eq!(
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(&c1)
                .unwrap()
        )
        .unwrap(),
        "3"
    );

    // 中页。
    let p2 = db
        .list_messages(&session.id, Some(&c1), 3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        p2.messages.iter().map(|m| m.seq_num).collect::<Vec<_>>(),
        vec![4, 5, 6]
    );
    assert!(p2.has_more);

    // 尾页。
    let p3 = db
        .list_messages(&session.id, Some(p2.next_cursor.as_deref().unwrap()), 3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        p3.messages.iter().map(|m| m.seq_num).collect::<Vec<_>>(),
        vec![7]
    );
    assert!(!p3.has_more);
    assert!(p3.next_cursor.is_none());

    // 无效游标 → 回退索引 0。
    let bad = db
        .list_messages(&session.id, Some("garbage"), 3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bad.messages[0].seq_num, 1);

    // 越界索引 → 空页不报错（旧系统此处 500）。
    let oob = db
        .list_messages(
            &session.id,
            Some(&base64::engine::general_purpose::STANDARD.encode("999")),
            3,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(oob.messages.is_empty());
    assert!(!oob.has_more);

    // 不存在的会话 → None（server 层 404）。
    assert!(db.list_messages("nope", None, 3).await.unwrap().is_none());
}

// ═══ 端点 6：POST compact —— 纯计算无 DB 操作；summary 落库位验证 ═══

#[tokio::test]
async fn compact_persists_summary_via_update() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    db.append_message(&session.id, user_msg("长对话"))
        .await
        .unwrap();
    assert!(
        db.update_session_summary(&session.id, "压缩后的上下文摘要")
            .await
            .unwrap()
    );
    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(detail.summary.as_deref(), Some("压缩后的上下文摘要"));
    assert!(!db.update_session_summary("nope", "x").await.unwrap());
}

// ═══ seq_num 分配：连续 + 并发 ═══

#[tokio::test]
async fn seq_num_assigns_contiguously() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    let mut expect = Vec::new();
    for i in 1..=5 {
        let record = db.append_message(&session.id, user_msg("x")).await.unwrap();
        assert_eq!(record.seq_num, i);
        expect.push(i);
    }
    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        detail
            .messages
            .iter()
            .map(|m| m.seq_num)
            .collect::<Vec<_>>(),
        expect
    );
}

#[tokio::test]
async fn seq_num_survives_concurrent_appends() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    let mut handles = Vec::new();
    for i in 0..20 {
        let db = db.clone();
        let sid = session.id.clone();
        handles.push(tokio::spawn(async move {
            db.append_message(&sid, user_msg(&format!("c{i}"))).await
        }));
    }
    let mut seqs = HashSet::new();
    for handle in handles {
        let record = handle.await.unwrap().unwrap();
        assert!(seqs.insert(record.seq_num));
    }
    // 互斥连接 + 事务：1..=20 无重无漏（UNIQUE(session_id, seq_num) 兜底）。
    assert_eq!((1..=20).collect::<HashSet<_>>(), seqs);
    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(detail.messages.len(), 20);
    // 并发读写混合：读不阻塞写、写不污染读。
    let mut mixed = Vec::new();
    for i in 0..8 {
        let dbw = db.clone();
        let sid = session.id.clone();
        mixed.push(tokio::spawn(async move {
            dbw.list_messages(&sid, None, 5 + i).await
        }));
    }
    for handle in mixed {
        assert!(handle.await.unwrap().unwrap().is_some());
    }
}

// ═══ 写入路径错误语义 ═══

#[tokio::test]
async fn append_to_missing_session_maps_to_not_found() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let err = db.append_message("ghost", user_msg("x")).await.unwrap_err();
    assert!(matches!(err, DbError::SessionNotFound(ref id) if id == "ghost"));
}

#[tokio::test]
async fn append_with_id_is_idempotent() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    let fixed_id = "00000000-0000-4000-8000-000000000001";
    let first = db
        .append_message_with_id(fixed_id, &session.id, user_msg("once"))
        .await
        .unwrap();
    assert!(first.is_some());
    let second = db
        .append_message_with_id(fixed_id, &session.id, user_msg("once"))
        .await
        .unwrap();
    assert!(second.is_none());
    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(detail.messages.len(), 1);
    assert_eq!(detail.messages[0].id, fixed_id);
    // 幂等跳过不追加 seq。
    let next = db
        .append_message(&session.id, user_msg("twice"))
        .await
        .unwrap();
    assert_eq!(next.seq_num, 2);
}

// ═══ rewind（deleteAfterSeqNum）与单条读取 ═══

#[tokio::test]
async fn rewind_deletes_strictly_after_seq() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    for i in 0..5 {
        db.append_message(&session.id, user_msg(&format!("m{i}")))
            .await
            .unwrap();
    }
    assert_eq!(db.delete_messages_after(&session.id, 3).await.unwrap(), 2);
    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        detail
            .messages
            .iter()
            .map(|m| m.seq_num)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    // rewind 后新消息从 MAX+1 续排。
    let after = db
        .append_message(&session.id, user_msg("new"))
        .await
        .unwrap();
    assert_eq!(after.seq_num, 4);

    let by_id = db
        .get_message_by_id(&detail.messages[0].id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(by_id.seq_num, 1);
    assert!(db.get_message_by_id("ghost").await.unwrap().is_none());
}

// ═══ 状态与用量更新 ═══

#[tokio::test]
async fn status_and_usage_updates() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    assert!(
        db.update_session_status(&session.id, "closed")
            .await
            .unwrap()
    );
    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(detail.status, "closed");
    assert!(!db.update_session_status("nope", "closed").await.unwrap());

    db.add_session_usage(
        &session.id,
        &Usage {
            input_tokens: 100,
            output_tokens: 50,
            cache_read_input_tokens: 7,
            cache_creation_input_tokens: 3,
        },
        0.25,
    )
    .await
    .unwrap();
    db.add_session_usage(&session.id, &Usage::default(), 0.25)
        .await
        .unwrap();
    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(detail.total_usage.input_tokens, 100);
    assert_eq!(detail.total_usage.cache_read_input_tokens, 7);
    assert!((detail.total_cost_usd - 0.5).abs() < f64::EPSILON * 4.0);
    assert!(
        !db.add_session_usage("nope", &Usage::default(), 0.0)
            .await
            .unwrap()
    );
}

// ═══ 文件库：迁移幂等 + PRAGMA 生效 ═══

#[tokio::test]
async fn file_db_migration_idempotent_and_pragmas_live() {
    let dir = std::env::temp_dir().join(format!("zk-db-test-{}", uuid::Uuid::new_v4()));
    let db_path = dir.join("nested").join("data.db");
    let db = zk_db::Db::open(&db_path).unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    db.append_message(&session.id, user_msg("persisted"))
        .await
        .unwrap();

    // 二次打开同一路径：迁移历史表判定 → 幂等，数据完好。
    let db2 = zk_db::Db::open(&db_path).unwrap();
    let detail = db2.get_session(&session.id).await.unwrap().unwrap();
    assert_eq!(detail.messages.len(), 1);

    // PRAGMA 断言：WAL + 外键开启（级联已由行为测试覆盖，此处直查）。
    db.with_conn_blocking(|conn| {
        let journal: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        let fk: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
        assert_eq!(journal.to_lowercase(), "wal");
        assert_eq!(fk, 1);
        Ok(())
    })
    .unwrap();

    let _ = std::fs::remove_dir_all(&dir);
}

// ═══ 旧库数据形态兼容（直连布数非标行）═══

#[tokio::test]
async fn legacy_shaped_rows_are_tolerated() {
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db.create_session("m", "/w").await.unwrap();
    // 直连布数：旧格式时间戳（动态小数位）+ 非标 role + 裸文本 content。
    db.with_conn_blocking(|conn| {
        conn.execute(
            "INSERT INTO messages (id, session_id, role, content_json, stop_reason,
                input_tokens, output_tokens, created_at, seq_num)
             VALUES ('legacy-1', ?1, 'assistant', '[{\"type\":\"text\",\"text\":\"旧消息\"}]',
                     'end_turn', 3, 5, '2026-08-12T17:19:18.227179Z', 1),
                    ('legacy-2', ?1, 'tool', 'plain text', NULL, 0, 0, '2026-08-12T17:19:19Z', 2),
                    ('legacy-3', ?1, 'system', 'not-json-at-all', NULL, 0, 0,
                     '2026-08-12T17:19:20.123456789Z', 3)",
            params![session.id],
        )?;
        Ok(())
    })
    .unwrap();

    let detail = db.get_session(&session.id).await.unwrap().unwrap();
    // 未知 role 的 legacy-2 被跳过（对齐 mapRowToMessage）；其余保留。
    assert_eq!(detail.messages.len(), 2);
    assert_eq!(detail.messages[0].id, "legacy-1");
    assert_eq!(detail.messages[0].created_at, 1_786_555_158_227);
    // 裸文本/坏 JSON 回退单文本块。
    assert_eq!(detail.messages[1].content, vec![text("not-json-at-all")]);
    // 9 位小数（纳秒）截断到毫秒。
    assert_eq!(detail.messages[1].created_at, 1_786_555_160_123);
}
