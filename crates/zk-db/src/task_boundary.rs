//! Todo boundaries share the successful tool result's transaction and Run owner.
use crate::message::{MessageAttribution, insert_message_in_current_write};
use crate::{DbError, MessageRecord, MessageRole, NewMessage, StoredBlock};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

pub(crate) fn append_todo_boundaries(
    conn: &Connection,
    session: &str,
    task: &str,
    run: &str,
    content: &str,
) -> Result<Vec<MessageRecord>, DbError> {
    let Ok(payload) = serde_json::from_str::<Value>(content) else {
        return Ok(Vec::new());
    };
    let Some(todos) = payload.get("newTodos").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let old = payload.get("oldTodos").and_then(Value::as_array);
    // The source transcript owns its turn count, including an agent's independent
    // prompt. Tool-result rows and steering never create instruction turns.
    let turn_index: i64 = conn.query_row(
        "SELECT COUNT(*) FROM messages WHERE session_id=?1 AND role='user' \
          AND COALESCE(json_type(CASE WHEN (SELECT content_retention FROM sessions WHERE id=messages.session_id)='ephemeral' THEN zk_ephemeral_get(messages.session_id,metadata_json) ELSE metadata_json END,'$.steering'),'null')!='true' \
          AND EXISTS(SELECT 1 FROM json_each(CASE WHEN (SELECT content_retention FROM sessions WHERE id=messages.session_id)='ephemeral' THEN zk_ephemeral_get(messages.session_id,content_json) ELSE content_json END) block \
            WHERE json_extract(block.value,'$.type') IN ('text','image'))",
        [session],
        |row| row.get(0),
    )?;
    let mut seq: i64 = conn.query_row(
        "SELECT COALESCE(MAX(json_extract(CASE WHEN (SELECT content_retention FROM sessions WHERE id=messages.session_id)='ephemeral' THEN zk_ephemeral_get(messages.session_id,metadata_json) ELSE metadata_json END,'$.boundary_seq')),0) FROM messages WHERE run_id=?1 AND json_extract(CASE WHEN (SELECT content_retention FROM sessions WHERE id=messages.session_id)='ephemeral' THEN zk_ephemeral_get(messages.session_id,metadata_json) ELSE metadata_json END,'$.boundary_kind')='todo'",
        [run], |row| row.get(0),
    )?;
    let mut messages = Vec::new();
    for todo in todos {
        let Some(id) = todo
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
        else {
            continue;
        };
        if todo.get("status").and_then(Value::as_str) != Some("IN_PROGRESS")
            || old.is_some_and(|old| {
                old.iter().any(|prior| {
                    prior.get("id").and_then(Value::as_str) == Some(id)
                        && prior.get("status").and_then(Value::as_str) == Some("IN_PROGRESS")
                })
            })
        {
            continue;
        }
        let seen: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE run_id=?1 AND json_extract(CASE WHEN (SELECT content_retention FROM sessions WHERE id=messages.session_id)='ephemeral' THEN zk_ephemeral_get(messages.session_id,metadata_json) ELSE metadata_json END,'$.boundary_kind')='todo' AND json_extract(CASE WHEN (SELECT content_retention FROM sessions WHERE id=messages.session_id)='ephemeral' THEN zk_ephemeral_get(messages.session_id,metadata_json) ELSE metadata_json END,'$.task_id')=?2)",
            params![run,id], |row| row.get(0),
        )?;
        if seen {
            continue;
        }
        seq += 1;
        let title = todo.get("content").and_then(Value::as_str).unwrap_or(id);
        let message = NewMessage {
            role: MessageRole::System,
            content: vec![StoredBlock::Text {
                text: title.to_owned(),
            }],
            meta: Some(
                json!({"subtype":"task_boundary","boundary_kind":"todo","task_id":id,"title":title,"seq":seq,"boundary_seq":seq,"turn_index":turn_index}),
            ),
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        };
        let record = insert_message_in_current_write(
            conn,
            &uuid::Uuid::new_v4().to_string(),
            session,
            &message,
            &MessageAttribution {
                task_id: Some(task.to_owned()),
                run_id: Some(run.to_owned()),
                origin: "runtime".to_owned(),
                source_task_id: None,
            },
        )?
        .ok_or_else(|| DbError::Invalid("TASK_BOUNDARY_ID_COLLISION".to_owned()))?;
        messages.push(record);
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::append_todo_boundaries;
    use crate::Db;
    use serde_json::json;

    #[tokio::test]
    async fn boundary_turn_index_counts_only_source_session_instructions() {
        let db = Db::open_in_memory().unwrap();
        for session in ["parent", "child"] {
            db.create_session_with_id(session, "model", "/tmp")
                .await
                .unwrap();
        }
        db.start_run("r", "parent", None, None, "model")
            .await
            .unwrap();
        db.start_run("c", "child", None, None, "model")
            .await
            .unwrap();
        db.with_writer(|conn| {
            let tx = conn.transaction()?;
            let rows = [
                ("parent", "user", json!([{"type":"text","text":"first"}]), None),
                ("parent", "user", json!([{"type":"text","text":"steer"}]), Some(json!({"steering":true}))),
                ("parent", "user", json!([{"type":"tool_result","tool_use_id":"tool","content":"result"}]), None),
                ("parent", "system", json!([{"type":"text","text":"history"}]), None),
                ("parent", "user", json!([{"type":"image","source":{"type":"base64","media_type":"image/png","data":""}}]), None),
                ("child", "user", json!([{"type":"text","text":"child first"}]), None),
            ];
            for (ordinal, (session, role, content, meta)) in rows.into_iter().enumerate() {
                tx.execute("INSERT INTO messages(id,session_id,role,content_json,created_at,seq_num,metadata_json) VALUES(?1,?2,?3,?4,'now',?5,?6)",
                    rusqlite::params![format!("m{ordinal}"), session, role, content.to_string(),i64::try_from(ordinal).unwrap()+1,meta.map(|v|v.to_string())])?;
            }
            let payload = json!({"newTodos":[{"id":"a","content":"Start","status":"IN_PROGRESS"}]}).to_string();
            let parent = append_todo_boundaries(&tx,"parent","r","r",&payload)?;
            let child = append_todo_boundaries(&tx,"child","c","c",&payload)?;
            assert_eq!(parent[0].meta.as_ref().unwrap()["turn_index"], 2);
            assert_eq!(child[0].meta.as_ref().unwrap()["turn_index"], 1);
            tx.commit()?;
            Ok(())
        }).await.unwrap();
    }

    #[tokio::test]
    async fn first_in_progress_is_durable_ordered_and_once_per_run() {
        let db = Db::open_in_memory().unwrap();
        db.create_session_with_id("s", "model", "/tmp")
            .await
            .unwrap();
        db.start_run("r", "s", None, None, "model").await.unwrap();
        db.with_writer(|conn| {
            let tx = conn.transaction()?;
            let input = json!({"oldTodos":[{"id":"already","status":"IN_PROGRESS"}],"newTodos":[{"id":"already","status":"IN_PROGRESS"},{"id":"a","content":"First","status":"IN_PROGRESS"},{"id":"b","content":"Second","status":"IN_PROGRESS"}]}).to_string();
            let first = append_todo_boundaries(&tx,"s","r","r",&input)?;
            assert_eq!(first.len(), 2);
            assert_eq!(first[0].meta.as_ref().unwrap()["seq"], 1);
            assert_eq!(first[1].meta.as_ref().unwrap()["seq"], 2);
            tx.commit()?;
            Ok(())
        }).await.unwrap();
        db.with_writer(|conn| {
            let tx = conn.transaction()?;
            let input = json!({"newTodos":[{"id":"a","status":"IN_PROGRESS"}]}).to_string();
            assert!(append_todo_boundaries(&tx, "s", "r", "r", &input)?.is_empty());
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            db.get_session("s").await.unwrap().unwrap().messages.len(),
            2
        );
        let message = db.get_session("s").await.unwrap().unwrap().messages[0]
            .id
            .clone();
        db.complete_run("r", 0, 0.0, 1).await.unwrap();
        db.start_run("new-run", "s", None, None, "model")
            .await
            .unwrap();
        let event = db
            .append_ws_outbox_event(
                "s",
                "s",
                None,
                None,
                "ws_task_boundary",
                None,
                &json!({"messageId":message,"taskId":"a"}),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            event.source_run_id, "r",
            "late boundary remains owned by original run"
        );
        assert!(
            db.append_ws_outbox_event(
                "s",
                "s",
                None,
                None,
                "ws_task_boundary",
                None,
                &json!({"messageId":"unowned"})
            )
            .await
            .is_err()
        );
    }
}
