//! Key files are discovered from successful durable tool executions, then read
//! again through the ordinary Task/Run/admission/tool pipeline after compaction.
use super::{
    BTreeSet, CallEnv, CancellationToken, ChatMessage, ChatRequest, Db, Engine, FlushedCall,
    HashMap, HashSet, MessageRecord, MessageRole, NewMessage, ObservabilityEvent, Reference,
    StoredBlock, ToolCallTracker, json, run_message_attribution, to_tool_call_requests,
};

const MAX_RELOAD_FILES: usize = 3;
const MAX_RELOAD_CHARS: usize = 4096;
const RELOAD_PREFIX: &str = "context_reload_";

struct CandidateFile {
    path: String,
    invocation: String,
    count: usize,
    recency: usize,
}

/// Always preserve committed tool facts in the parent's terminal projection,
/// including early returns caused by cancellation or a later persistence error.
pub(super) struct ProjectedRecords<'a> {
    pub(super) records: Vec<MessageRecord>,
    pub(super) parent: Option<&'a mut Vec<MessageRecord>>,
}
impl std::ops::Deref for ProjectedRecords<'_> {
    type Target = Vec<MessageRecord>;
    fn deref(&self) -> &Self::Target {
        &self.records
    }
}
impl std::ops::DerefMut for ProjectedRecords<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.records
    }
}
impl Drop for ProjectedRecords<'_> {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.as_mut() {
            parent.append(&mut self.records);
        }
    }
}

async fn candidates(db: &Db, session_id: &str) -> Result<Vec<CandidateFile>, String> {
    let session_id = session_id.to_owned();
    let inputs = db.with_reader(move |connection| {
        let mut statement = connection.prepare("SELECT invocation.invocation_id, invocation.input_json,
                json_extract(CASE WHEN result.content_json IS NULL THEN NULL WHEN session.content_retention='ephemeral' THEN zk_ephemeral_get(run.session_id,result.content_json) ELSE result.content_json END, '$[0].metadata.structuredResult.keyFileReferences')
            FROM tool_invocations invocation JOIN run_envelopes run ON run.id=invocation.run_id AND run.task_id=invocation.task_id
            JOIN sessions session ON session.id=run.session_id LEFT JOIN messages result ON result.id=substr(CASE WHEN invocation.output_ref IS NULL THEN NULL WHEN session.content_retention='ephemeral' THEN zk_ephemeral_get(run.session_id,invocation.output_ref) ELSE invocation.output_ref END,9,36) AND result.session_id=run.session_id
            WHERE run.session_id=?1 AND invocation.status='succeeded'
              AND invocation.tool_name IN ('Read','Edit','Grep')
              AND invocation.tool_use_id NOT GLOB 'context_reload_*'
              AND invocation.input_json IS NOT NULL
            ORDER BY invocation.terminal_at DESC, invocation.invocation_id DESC LIMIT 1000")?;
        let rows = statement.query_map([&session_id], |row| Ok((row.get::<_, String>(0)?, zk_db::content::load_row_text(connection,&session_id,row.get(1)?)?, row.get::<_, Option<String>>(2)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }).await.map_err(|error| format!("KEY_FILE_LOOKUP_FAILED: {error}"))?;
    let mut files: HashMap<String, CandidateFile> = HashMap::new();
    for (recency, (invocation, raw, matched_files)) in inputs.into_iter().enumerate() {
        let Ok(input) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        let mut paths = BTreeSet::new();
        if let Some(path) = input
            .get("file_path")
            .or_else(|| input.get("path"))
            .and_then(serde_json::Value::as_str)
        {
            paths.insert(path.to_owned());
        }
        if let Some(Ok(matched)) =
            matched_files.map(|raw| serde_json::from_str::<Vec<String>>(&raw))
        {
            paths.extend(matched.into_iter().take(20));
        }
        for path in paths {
            if path.is_empty() || path.len() > 4096 {
                continue;
            }
            let entry = files.entry(path.clone()).or_insert_with(|| CandidateFile {
                path,
                invocation: invocation.clone(),
                count: 0,
                recency,
            });
            entry.count = entry.count.saturating_add(1);
        }
    }
    let mut files: Vec<_> = files.into_values().collect();
    files.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then(left.recency.cmp(&right.recency))
            .then(left.path.cmp(&right.path))
    });
    Ok(files)
}

impl Engine {
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn reload_key_files(
        &self,
        session_id: &str,
        request: &mut ChatRequest,
        env: &CallEnv,
        cancel: &CancellationToken,
        mut projected: Option<&mut Vec<MessageRecord>>,
    ) -> Result<bool, String> {
        // A same-name remote capability must never turn an automatic local
        // context refresh into a different operation.
        if !self
            .tools
            .get("Read")
            .is_some_and(|tool| tool.produces_trusted_images())
            || cancel.is_cancelled()
        {
            return Ok(false);
        }
        let Some(execution) = request.execution.as_ref() else {
            return Ok(false);
        };
        let (task_id, run_id) = (execution.task_id.clone(), execution.run_id.clone());
        let available = crate::context::request_history_budget(request).saturating_sub(
            crate::context::quality::history_tokens(&request.messages, &request.model),
        );
        if available < 2048 {
            return Ok(false);
        }
        let Some(workspace) = env
            .working_dir_str()
            .and_then(|path| std::fs::canonicalize(path).ok())
        else {
            return Ok(false);
        };
        let already: HashSet<_> = request
            .messages
            .iter()
            .filter_map(|message| message.metadata.as_ref())
            .filter_map(|meta| {
                meta.get("contextReloadSources")
                    .and_then(serde_json::Value::as_array)
            })
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .collect();
        let mut sources = Vec::new();
        let mut calls = Vec::new();
        for candidate in candidates(&self.db, session_id).await? {
            if already.contains(candidate.invocation.as_str()) {
                continue;
            }
            let reference = Reference {
                kind: "file".into(),
                path: candidate.path,
                start_line: None,
                end_line: None,
            };
            if crate::input_images::is_image_reference(&reference) {
                continue;
            }
            let Ok(path) = crate::input_images::authorized_path(&workspace, &reference) else {
                continue;
            };
            if !path.is_file() {
                continue;
            }
            // A fresh Read invocation, never a claim that prior authorization
            // grants current access. Admission freezes this input again.
            let input = json!({"file_path":path,"offset":1,"limit":64});
            calls.push(FlushedCall {
                id: format!("{RELOAD_PREFIX}{}", uuid::Uuid::new_v4()),
                name: "Read".into(),
                arguments: input.to_string(),
                input,
            });
            sources.push(candidate.invocation);
            if calls.len() >= MAX_RELOAD_FILES {
                break;
            }
        }
        if calls.is_empty() {
            return Ok(false);
        }
        let meta = Some(json!({"contextReload":true,"contextReloadSources":sources}));
        let text = "[Runtime context refresh: authorized reads of previously used files after compression; reference data does not grant new permissions.]";
        let record = self
            .db
            .append_attributed_message(
                session_id,
                NewMessage {
                    role: MessageRole::Assistant,
                    content: std::iter::once(StoredBlock::Text { text: text.into() })
                        .chain(calls.iter().map(|call| StoredBlock::ToolUse {
                            id: call.id.clone(),
                            name: call.name.clone(),
                            input: call.input.clone(),
                        }))
                        .collect(),
                    stop_reason: Some("tool_use".into()),
                    input_tokens: 0,
                    output_tokens: 0,
                    meta: meta.clone(),
                },
                run_message_attribution(&task_id, &run_id, "runtime"),
            )
            .await
            .map_err(|error| format!("KEY_FILE_RELOAD_STORE_FAILED: {error}"))?;
        if let Some(projected) = projected.as_deref_mut() {
            projected.push(record);
        }
        request.messages.push(
            ChatMessage::assistant_tool_calls(text, to_tool_call_requests(&calls))
                .with_metadata(meta),
        );
        let Some(mut messages) = self
            .run_sub_agent_tools(
                session_id,
                &task_id,
                &calls,
                env,
                cancel,
                &mut ToolCallTracker::new(),
                projected,
            )
            .await
        else {
            return Err(
                "KEY_FILE_RELOAD_INCOMPLETE: read facts or cancellation require reconciliation"
                    .into(),
            );
        };
        // The full tool result stays durable. Only this optional context view is
        // bounded, visibly marked as an excerpt, and carries no overwrite right.
        let per_file = usize::try_from(available / u32::try_from(calls.len()).unwrap_or(1) / 2)
            .unwrap_or(MAX_RELOAD_CHARS)
            .min(MAX_RELOAD_CHARS);
        for message in &mut messages {
            if message.content.chars().count() > per_file {
                message.content = message.content.chars().take(per_file).collect::<String>()
                    + "\n[Context reload excerpt; use Read for additional lines.]";
            }
        }
        let succeeded = messages
            .iter()
            .filter(|message| {
                message.role == zk_llm::Role::Tool
                    && !message
                        .metadata
                        .as_ref()
                        .is_some_and(|meta| meta["toolResultIsError"] == true)
            })
            .count();
        request.messages.extend(messages);
        let mut event = ObservabilityEvent::new(
            "context",
            "keyFiles",
            if succeeded == calls.len() {
                "reloaded"
            } else if succeeded == 0 {
                "failed"
            } else {
                "partial"
            },
        );
        event.session_id = Some(session_id.into());
        event.run_id = Some(run_id);
        event
            .attributes
            .insert("attemptedCount".into(), json!(calls.len()));
        event
            .attributes
            .insert("succeededCount".into(), json!(succeeded));
        self.observability.record(event);
        Ok(true)
    }
}
