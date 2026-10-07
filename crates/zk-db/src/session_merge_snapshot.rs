//! Immutable relational projections and owned file copies for session handoff.
use super::{DbError, MergeSummaryInput, Snapshot, digest};
use crate::session_merge_budget::MergeWriteBudget;
use base64::Engine as _;
use rusqlite::{Connection, params, types::ValueRef};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};

const MAX_COPY_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = MAX_COPY_BYTES;

pub(super) fn descendants(conn: &Connection, root: &str) -> Result<Vec<String>, DbError> {
    let mut stmt=conn.prepare("WITH RECURSIVE tree(id) AS (SELECT id FROM sessions WHERE id=?1 UNION SELECT s.id FROM sessions s JOIN tree ON s.parent_session_id=tree.id WHERE s.kind='internal') SELECT id FROM tree ORDER BY id")?;
    Ok(stmt
        .query_map([root], |r| r.get(0))?
        .collect::<Result<Vec<_>, _>>()?)
}

fn rows(conn: &Connection, table: &str, filter: &str, source: &str) -> Result<Vec<Value>, DbError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT * FROM {table} WHERE {filter} ORDER BY rowid"
    ))?;
    let names = stmt
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let result=stmt.query_map([source],|row|{
        let mut value=serde_json::Map::new();
        for (index,name) in names.iter().enumerate(){
            let field=match row.get_ref(index)? {
                ValueRef::Null=>Value::Null, ValueRef::Integer(n)=>json!(n),ValueRef::Real(n)=>json!(n),
                ValueRef::Text(text)=>Value::String(String::from_utf8_lossy(text).into_owned()),
                ValueRef::Blob(bytes)=>json!({"encoding":"base64","data":base64::engine::general_purpose::STANDARD.encode(bytes)}),
            };
            value.insert(name.clone(),field);
        }
        Ok(Value::Object(value))
    })?.collect::<Result<Vec<_>,_>>()?;
    Ok(result)
}

pub(super) fn records(conn: &Connection, source: &str) -> Result<Vec<MergeSummaryInput>, DbError> {
    let mut records = Vec::new();
    // SQL identifiers are constants, never supplied by a handoff request.
    let tables = [
        ("activities", "session_id=?1"),
        ("tasks", "session_id=?1"),
        ("run_envelopes", "session_id=?1"),
        (
            "run_event_log",
            "run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1)",
        ),
        (
            "tool_invocations",
            "run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1)",
        ),
        (
            "task_results",
            "run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1)",
        ),
        (
            "task_result_blobs",
            "sha256 IN (SELECT blob_sha256 FROM task_results WHERE run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1))",
        ),
        ("artifact_manifests", "session_id=?1"),
        (
            "artifact_entries",
            "manifest_id IN (SELECT manifest_id FROM artifact_manifests WHERE session_id=?1)",
        ),
        ("evidence_bundles", "session_id=?1"),
        (
            "evidence_items",
            "bundle_id IN (SELECT bundle_id FROM evidence_bundles WHERE session_id=?1)",
        ),
        (
            "evidence_verdict_events",
            "bundle_id IN (SELECT bundle_id FROM evidence_bundles WHERE session_id=?1)",
        ),
        ("agent_checkpoints", "session_id=?1"),
        ("interaction_requests", "session_id=?1"),
        (
            "run_workbench_bindings",
            "root_run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1)",
        ),
        (
            "run_acceptance_criteria",
            "root_run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1)",
        ),
        (
            "research_sources",
            "run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1)",
        ),
        (
            "research_findings",
            "run_id IN (SELECT id FROM run_envelopes WHERE session_id=?1)",
        ),
        (
            "research_conflicts",
            "root_task_id IN (SELECT id FROM tasks WHERE session_id=?1)",
        ),
        (
            "research_open_questions",
            "root_task_id IN (SELECT id FROM tasks WHERE session_id=?1)",
        ),
    ];
    for (table, filter) in tables {
        for (index, row) in rows(conn, table, filter, source)?.into_iter().enumerate() {
            let text = serde_json::to_string(&row)?;
            records.push(MergeSummaryInput {
                reference: format!("record:{source}:{table}:{index}"),
                source_id: source.into(),
                sha256: digest(text.as_bytes()),
                text,
            });
        }
    }
    records.extend(inherited_records(conn, source)?);
    Ok(records)
}

fn collect_tree(root: &Path, current: &Path, paths: &mut BTreeSet<PathBuf>) -> Result<(), DbError> {
    if !current.exists() {
        return Ok(());
    }
    let metadata = std::fs::symlink_metadata(current)
        .map_err(|e| DbError::Invalid(format!("MERGE_ASSET_READ: {e}")))?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        for entry in std::fs::read_dir(current)
            .map_err(|e| DbError::Invalid(format!("MERGE_ASSET_READ: {e}")))?
        {
            let entry = entry.map_err(|e| DbError::Invalid(format!("MERGE_ASSET_READ: {e}")))?;
            let path = entry.path();
            if path.starts_with(root) {
                collect_tree(root, &path, paths)?;
            }
        }
    } else {
        paths.insert(current.to_owned());
    }
    Ok(())
}

pub(super) fn copy_assets(
    conn: &Connection,
    operation: &str,
    snapshot: &Snapshot,
    scratchpad: Option<&Path>,
    used: &mut u64,
    disk: &mut MergeWriteBudget,
) -> Result<(), DbError> {
    let source = &snapshot.session_id;
    copy_inline_images(conn, operation, snapshot, used, disk)?;
    copy_inherited_assets(conn, operation, source, used, disk)?;
    let workspace = Path::new(&snapshot.working_directory);
    let mut paths = BTreeSet::new();
    let mut expected = std::collections::BTreeMap::new();
    let mut stmt=conn.prepare("SELECT e.canonical_path,e.sealed_hash FROM artifact_entries e JOIN artifact_manifests m ON m.manifest_id=e.manifest_id WHERE m.session_id=?1 AND e.operation!='deleted'")?;
    for row in stmt.query_map([source], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
    })? {
        let (path, hash) = row?;
        paths.insert(PathBuf::from(&path));
        if let Some(hash) = hash {
            expected.insert(PathBuf::from(path), hash);
        }
    }
    let mut stmt=conn.prepare("SELECT DISTINCT i.blob_sha256 FROM evidence_items i JOIN evidence_bundles b ON b.bundle_id=i.bundle_id WHERE b.session_id=?1 AND i.blob_sha256 IS NOT NULL")?;
    for hash in stmt.query_map([source], |r| r.get::<_, String>(0))? {
        let hash = hash?;
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(DbError::Invalid("MERGE_BLOB_ID_INVALID".into()));
        }
        let path = workspace.join(".zk/blobs").join(&hash[..2]).join(&hash);
        paths.insert(path.clone());
        expected.insert(path, hash);
    }
    let own = scratchpad
        .map_or_else(|| workspace.join(".zk/scratchpad"), Path::to_owned)
        .join(source);
    collect_tree(&own, &own, &mut paths)?;
    let references = snapshot_references(snapshot)?;
    for reference in &references {
        let path = Path::new(reference);
        // Only explicit references in the source-owned scratchpad can expand copying.
        if path.starts_with(&own)
            && !path
                .components()
                .any(|p| matches!(p, std::path::Component::ParentDir))
            && !path.is_dir()
        {
            paths.insert(path.to_owned());
        }
    }
    let workspace = std::fs::canonicalize(workspace).ok();
    let own = std::fs::canonicalize(&own).ok();
    for path in paths {
        let original = path.to_string_lossy().into_owned();
        let reference = format!(
            "asset:{}",
            digest(format!("{source}\0{original}").as_bytes())
        );
        let copy = copy_owned_file(
            &path,
            workspace.as_deref(),
            own.as_deref(),
            expected.get(&path).map(String::as_str),
            *used,
        );
        let (status, reason, hash, data) = match copy {
            Ok(bytes) => {
                *used += bytes.len() as u64;
                ("copied", None, Some(digest(&bytes)), Some(bytes))
            }
            Err(reason) => {
                if matches!(
                    reason.as_str(),
                    "copy_budget_exceeded" | "copy_failed" | "changed_during_copy"
                ) {
                    return Err(DbError::Invalid(format!("MERGE_COPY_INCOMPLETE: {reason}")));
                }
                ("unavailable", Some(reason), None, None)
            }
        };
        disk.reserve(
            data.as_ref()
                .map_or(0, Vec::len)
                .saturating_add(original.len())
                .saturating_add(1024),
        )?;
        conn.execute("INSERT INTO session_merge_assets(operation_id,reference,source_session_id,original_path,status,reason,sha256,size,content) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![operation,reference,source,original,status,reason,hash,i64::try_from(data.as_ref().map_or(0,Vec::len)).map_err(|_|DbError::Invalid("asset too large".into()))?,data])?;
    }
    for reference in references {
        let key = format!(
            "asset:{}",
            digest(format!("{source}\0{reference}").as_bytes())
        );
        disk.reserve(reference.len().saturating_add(1024))?;
        conn.execute("INSERT OR IGNORE INTO session_merge_assets(operation_id,reference,source_session_id,original_path,status,reason) VALUES(?1,?2,?3,?4,'external_reference','Historical reference only; not a file snapshot or permission to access')",params![operation,key,source,reference])?;
    }
    Ok(())
}

fn snapshot_references(snapshot: &Snapshot) -> Result<BTreeSet<String>, DbError> {
    fn collect(value: &Value, references: &mut BTreeSet<String>) {
        match value {
            Value::String(text) => {
                if text.len() < 4096
                    && !text.contains('\n')
                    && (text.starts_with('/')
                        || text.starts_with("https://")
                        || text.starts_with("http://"))
                {
                    references.insert(text.clone());
                }
            }
            Value::Array(items) => {
                for item in items {
                    collect(item, references);
                }
            }
            Value::Object(fields) => {
                if fields
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| {
                        matches!(
                            kind,
                            "provider_response_state"
                                | "providerResponseState"
                                | "redacted_thinking"
                        )
                    })
                {
                    return;
                }
                for (key, value) in fields {
                    if key.ends_with("_json")
                        && let Some(text) = value.as_str()
                        && let Ok(parsed) = serde_json::from_str::<Value>(text)
                    {
                        collect(&parsed, references);
                    } else {
                        collect(value, references);
                    }
                }
            }
            _ => {}
        }
    }
    let mut references = BTreeSet::new();
    collect(&serde_json::to_value(&snapshot.messages)?, &mut references);
    for record in &snapshot.records {
        collect(
            &serde_json::from_str::<Value>(&record.text)?,
            &mut references,
        );
    }
    Ok(references)
}

fn copy_owned_file(
    path: &Path,
    workspace: Option<&Path>,
    own: Option<&Path>,
    expected_hash: Option<&str>,
    used: u64,
) -> Result<Vec<u8>, String> {
    let before = std::fs::symlink_metadata(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            "missing"
        } else {
            "copy_failed"
        }
    })?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err("symlink_or_non_regular".into());
    }
    let canonical = std::fs::canonicalize(path).map_err(|_| "copy_failed")?;
    if !workspace
        .as_ref()
        .is_some_and(|root| canonical.starts_with(root))
        && !own.as_ref().is_some_and(|root| canonical.starts_with(root))
    {
        return Err("ownership_unknown".into());
    }
    if before.len() > MAX_FILE_BYTES || used.saturating_add(before.len()) > MAX_COPY_BYTES {
        return Err("copy_budget_exceeded".into());
    }
    let mut file = std::fs::File::open(&canonical).map_err(|_| "copy_failed")?;
    let opened = file.metadata().map_err(|_| "copy_failed")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err("changed_during_copy".into());
        }
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "copy_failed")?;
    let after = file.metadata().map_err(|_| "copy_failed")?;
    if bytes.len() as u64 != before.len()
        || after.len() != before.len()
        || after.modified().ok() != before.modified().ok()
    {
        return Err("changed_during_copy".into());
    }
    if expected_hash.is_some_and(|hash| hash != digest(&bytes)) {
        return Err("hash_mismatch".into());
    }
    Ok(bytes)
}

struct InlineCopy<'a> {
    original: String,
    media_type: Option<&'a str>,
    data: &'a str,
    expected_hash: Option<&'a str>,
}

fn save_inline_copy(
    conn: &Connection,
    operation: &str,
    source: &str,
    image: InlineCopy<'_>,
    used: &mut u64,
    disk: &mut MergeWriteBudget,
) -> Result<(), DbError> {
    if image.data.len() as u64 > MAX_FILE_BYTES.div_ceil(3) * 4 {
        return Err(DbError::Invalid("MERGE_INLINE_IMAGE_TOO_LARGE".into()));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(image.data)
        .map_err(|_| DbError::Invalid("MERGE_INLINE_IMAGE_INVALID".into()))?;
    if bytes.len() as u64 > MAX_FILE_BYTES
        || used.saturating_add(bytes.len() as u64) > MAX_COPY_BYTES
    {
        return Err(DbError::Invalid("MERGE_COPY_BUDGET_EXCEEDED".into()));
    }
    let hash = digest(&bytes);
    if image.expected_hash.is_some_and(|expected| expected != hash) {
        return Err(DbError::Invalid("MERGE_INLINE_IMAGE_HASH_MISMATCH".into()));
    }
    *used += bytes.len() as u64;
    let original = image.original;
    let reference = format!(
        "asset:{}",
        digest(format!("{source}\0{original}").as_bytes())
    );
    disk.reserve(
        bytes
            .len()
            .saturating_add(original.len())
            .saturating_add(1024),
    )?;
    conn.execute("INSERT INTO session_merge_assets(operation_id,reference,source_session_id,original_path,status,sha256,mime_type,size,content) VALUES(?1,?2,?3,?4,'copied',?5,?6,?7,?8)",params![operation,reference,source,original,hash,image.media_type,i64::try_from(bytes.len()).map_err(|_|DbError::Invalid("asset too large".into()))?,bytes])?;
    Ok(())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sealed_tool_images<'a>(
    conn: &Connection,
    source: &str,
    message: &crate::MessageRecord,
    tool_use_id: &str,
    metadata: &'a Value,
) -> Result<Option<&'a Vec<Value>>, DbError> {
    let Some(producer) = metadata["__zkTrustedImageProducer"]
        .as_str()
        .filter(|name| matches!(*name, "Read" | "HandoffRead"))
    else {
        return Ok(None);
    };
    let output = format!("message:{}", message.id);
    let receipt: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages m JOIN tool_invocations i ON i.run_id=m.run_id AND i.task_id=m.task_id WHERE m.id=?1 AND m.session_id=?2 AND m.origin='tool_result' AND i.tool_use_id=?3 AND i.tool_name=?4 AND i.status='succeeded' AND (i.output_ref=?5 OR i.output_ref LIKE ?6))",
        params![message.id,source,tool_use_id,producer,output,format!("{output}#sha256:%")], |row| row.get(0))?;
    if !receipt {
        return Ok(None);
    }
    let Some(images) = metadata["inlineImages"].as_array() else {
        return Ok(None);
    };
    for image in images {
        let source_hash = image["sourceDigest"]
            .as_str()
            .filter(|value| valid_digest(value));
        let payload_hash = image["payloadDigest"]
            .as_str()
            .or(source_hash)
            .filter(|value| valid_digest(value));
        if source_hash.is_none()
            || payload_hash.is_none()
            || (producer == "Read"
                && metadata["structuredResult"]["contentSha256"].as_str() != source_hash)
            || image["data"]
                .as_str()
                .is_none_or(|data| data.len() > (10 * 1024 * 1024usize).div_ceil(3) * 4)
            || !matches!(
                image["mediaType"].as_str(),
                Some("image/png" | "image/jpeg" | "image/gif" | "image/webp")
            )
        {
            return Err(DbError::Invalid(
                "MERGE_TRUSTED_IMAGE_METADATA_INVALID".into(),
            ));
        }
    }
    Ok(Some(images))
}

fn copy_inline_images(
    conn: &Connection,
    operation: &str,
    snapshot: &Snapshot,
    used: &mut u64,
    disk: &mut MergeWriteBudget,
) -> Result<(), DbError> {
    let source = &snapshot.session_id;
    for message in &snapshot.messages {
        for (index, block) in message.content.iter().enumerate() {
            if let crate::StoredBlock::Image { source: image, .. } = block
                && let Some(data) = &image.data
            {
                save_inline_copy(
                    conn,
                    operation,
                    source,
                    InlineCopy {
                        original: format!("inline:{}:{index}", message.id),
                        media_type: image.media_type.as_deref(),
                        data,
                        expected_hash: None,
                    },
                    used,
                    disk,
                )?;
            }
            if let crate::StoredBlock::ToolResult {
                tool_use_id,
                is_error: false,
                metadata: Some(metadata),
                ..
            } = block
                && let Some(images) =
                    sealed_tool_images(conn, source, message, tool_use_id, metadata)?
            {
                for (ordinal, image) in images.iter().enumerate() {
                    let source_hash = image["sourceDigest"].as_str().unwrap_or_default();
                    save_inline_copy(
                        conn,
                        operation,
                        source,
                        InlineCopy {
                            original: format!(
                                "inline-tool:{}:{index}:{ordinal}:source:{source_hash}",
                                message.id
                            ),
                            media_type: image["mediaType"].as_str(),
                            data: image["data"].as_str().unwrap_or_default(),
                            expected_hash: Some(
                                image["payloadDigest"].as_str().unwrap_or(source_hash),
                            ),
                        },
                        used,
                        disk,
                    )?;
                }
            }
        }
    }
    Ok(())
}

// A merged session can itself become a source. Seal its previous handoff again,
// so the new owner never needs authority over an older target or live source.
fn prior_operation(conn: &Connection, source: &str) -> Result<Option<String>, DbError> {
    use rusqlite::OptionalExtension as _;
    Ok(conn
        .query_row(
            "SELECT id FROM session_merges WHERE target_session_id=?1 AND status='completed'",
            [source],
            |r| r.get(0),
        )
        .optional()?)
}

fn inherited_records(conn: &Connection, source: &str) -> Result<Vec<MergeSummaryInput>, DbError> {
    let Some(prior) = prior_operation(conn, source)? else {
        return Ok(Vec::new());
    };
    let mut records = Vec::new();
    for snapshot in super::load_snapshots(conn, &prior)? {
        let text = serde_json::to_string(&snapshot)?;
        records.push(MergeSummaryInput {
            reference: format!(
                "record:{source}:prior_handoff:{prior}:{}",
                snapshot.session_id
            ),
            source_id: source.into(),
            sha256: digest(text.as_bytes()),
            text,
        });
    }
    let mut stmt=conn.prepare("SELECT unit_id,result_json,result_hash FROM session_merge_units WHERE operation_id=?1 AND state='completed' ORDER BY ordinal,unit_id")?;
    for row in stmt.query_map([&prior], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })? {
        let (unit, text, sha256) = row?;
        if digest(text.as_bytes()) != sha256 {
            return Err(DbError::Invalid("MERGE_DETAIL_HASH_MISMATCH".into()));
        }
        records.push(MergeSummaryInput {
            reference: format!("record:{source}:prior_detail:{prior}:{unit}"),
            source_id: source.into(),
            text,
            sha256,
        });
    }
    Ok(records)
}

fn copy_inherited_assets(
    conn: &Connection,
    operation: &str,
    source: &str,
    used: &mut u64,
    disk: &mut MergeWriteBudget,
) -> Result<(), DbError> {
    let Some(prior) = prior_operation(conn, source)? else {
        return Ok(());
    };
    let mut stmt=conn.prepare("SELECT reference,original_path,status,reason,sha256,mime_type,size,content FROM session_merge_assets WHERE operation_id=?1 ORDER BY reference")?;
    for row in stmt.query_map([&prior], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, i64>(6)?,
            r.get::<_, Option<Vec<u8>>>(7)?,
        ))
    })? {
        let (reference, path, status, reason, hash, mime, size, content) = row?;
        if status == "copied" {
            let data = content
                .as_deref()
                .ok_or_else(|| DbError::Invalid("MERGE_ASSET_MISSING".into()))?;
            if hash.as_deref() != Some(digest(data).as_str())
                || size
                    != i64::try_from(data.len())
                        .map_err(|_| DbError::Invalid("asset too large".into()))?
            {
                return Err(DbError::Invalid("MERGE_ASSET_HASH_MISMATCH".into()));
            }
            if used.saturating_add(data.len() as u64) > MAX_COPY_BYTES {
                return Err(DbError::Invalid("MERGE_COPY_BUDGET_EXCEEDED".into()));
            }
            *used += data.len() as u64;
        }
        let new_ref = format!(
            "asset:{}",
            digest(format!("{source}\0{prior}\0{reference}").as_bytes())
        );
        disk.reserve(
            content
                .as_ref()
                .map_or(0, Vec::len)
                .saturating_add(path.len())
                .saturating_add(1024),
        )?;
        conn.execute("INSERT INTO session_merge_assets(operation_id,reference,source_session_id,original_path,status,reason,sha256,mime_type,size,content) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",params![operation,new_ref,source,format!("handoff:{prior}:{path}"),status,reason,hash,mime,size,content])?;
    }
    Ok(())
}
