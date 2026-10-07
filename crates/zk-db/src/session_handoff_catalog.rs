//! Immutable projected text index: bounded reads never materialize an entire handoff.
use super::{DbError, HandoffQuery, MergeSummaryInput, digest};
use base64::Engine as _;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const CHUNK_BYTES: usize = 8192;
const RESPONSE_BYTES: usize = 16_384;
const SCAN_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn seal(
    conn: &Connection,
    operation: &str,
    inputs: Vec<MergeSummaryInput>,
) -> Result<(), DbError> {
    let mut disk = crate::session_merge_budget::MergeWriteBudget::new(conn);
    for (ordinal, input) in inputs.into_iter().enumerate() {
        if digest(input.text.as_bytes()) != input.sha256 {
            return Err(DbError::Invalid("HANDOFF_TEXT_HASH_MISMATCH".into()));
        }
        let sections: Vec<String> = if input.reference.starts_with("detail:") {
            serde_json::from_str::<Value>(&input.text)
                .ok()
                .and_then(|v| v.get("items").and_then(Value::as_array).cloned())
                .unwrap_or_default()
                .iter()
                .filter_map(|item| {
                    item.get("section")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .collect()
        } else {
            vec![]
        };
        let sections = serde_json::to_string(&sections)?;
        let bytes = i64::try_from(input.text.len())
            .map_err(|_| DbError::Invalid("handoff text too large".into()))?;
        let ordinal = i64::try_from(ordinal)
            .map_err(|_| DbError::Invalid("handoff catalog too large".into()))?;
        let manifest = digest(
            json!([
                input.reference,
                input.source_id,
                input.sha256,
                bytes,
                sections,
                ordinal
            ])
            .to_string()
            .as_bytes(),
        );
        disk.reserve(input.text.len().saturating_mul(2).saturating_add(4096))?;
        conn.execute("INSERT INTO session_handoff_catalog(operation_id,reference,ordinal,source_id,sha256,total_bytes,sections_json,manifest_hash) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![operation,input.reference,ordinal,input.source_id,input.sha256,bytes,sections,manifest])?;
        // Byte chunks deliberately need not end on Unicode boundaries; reads assemble first.
        for (index, chunk) in input.text.as_bytes().chunks(CHUNK_BYTES).enumerate() {
            let start = i64::try_from(index * CHUNK_BYTES)
                .map_err(|_| DbError::Invalid("handoff offset too large".into()))?;
            conn.execute("INSERT INTO session_handoff_chunks(operation_id,reference,start_byte,content,sha256) VALUES(?1,?2,?3,?4,?5)",params![operation,input.reference,start,chunk,digest(chunk)])?;
        }
    }
    Ok(())
}

struct Entry {
    reference: String,
    source: String,
    sha: String,
    bytes: usize,
    ordinal: i64,
}
fn entry(
    conn: &Connection,
    op: &str,
    query: &HandoffQuery,
    ordinal: i64,
    reference: Option<&str>,
) -> Result<Option<Entry>, DbError> {
    let row: Option<(String,String,String,i64,i64,String,String)> = conn.query_row(
        "SELECT reference,source_id,sha256,total_bytes,ordinal,sections_json,manifest_hash FROM session_handoff_catalog WHERE operation_id=?1 AND ordinal>=?2 AND (?3 IS NULL OR source_id=?3) AND (?4 IS NULL OR EXISTS(SELECT 1 FROM json_each(sections_json) WHERE value=?4)) AND (?5 IS NULL OR reference=?5) ORDER BY ordinal LIMIT 1",
        params![op,ordinal,query.source_id,query.section,reference], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    row.map(
        |(reference, source, sha, bytes, ordinal, sections, manifest)| {
            if digest(
                json!([reference, source, sha, bytes, sections, ordinal])
                    .to_string()
                    .as_bytes(),
            ) != manifest
            {
                return Err(DbError::Invalid("HANDOFF_CATALOG_HASH_MISMATCH".into()));
            }
            Ok(Entry {
                reference,
                source,
                sha,
                bytes: usize::try_from(bytes)
                    .map_err(|_| DbError::Invalid("invalid handoff size".into()))?,
                ordinal,
            })
        },
    )
    .transpose()
}

struct Budget {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    scanned: usize,
}
impl Budget {
    fn check(&self) -> Result<(), DbError> {
        if self.cancelled.load(Ordering::Relaxed) || Instant::now() >= self.deadline {
            Err(DbError::Conflict(
                "HANDOFF_READ_TIMEOUT_OR_CANCELLED".into(),
            ))
        } else {
            Ok(())
        }
    }
}
fn bytes(
    conn: &Connection,
    op: &str,
    item: &Entry,
    start: usize,
    end: usize,
    budget: &Budget,
) -> Result<Vec<u8>, DbError> {
    budget.check()?;
    if start > end || end > item.bytes {
        return Err(DbError::Validation("invalid handoff byte range".into()));
    }
    if start == end {
        return Ok(vec![]);
    }
    let mut result = Vec::with_capacity(end - start);
    let first = (start / CHUNK_BYTES) * CHUNK_BYTES;
    let mut stmt=conn.prepare("SELECT start_byte,content,sha256 FROM session_handoff_chunks WHERE operation_id=?1 AND reference=?2 AND start_byte>=?3 AND start_byte<?4 ORDER BY start_byte")?;
    let rows = stmt.query_map(
        params![
            op,
            item.reference,
            i64::try_from(first).unwrap_or(i64::MAX),
            i64::try_from(end).unwrap_or(i64::MAX)
        ],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
            ))
        },
    )?;
    let mut expected = first;
    for row in rows {
        budget.check()?;
        let (offset, data, hash) = row?;
        let offset = usize::try_from(offset)
            .map_err(|_| DbError::Invalid("invalid handoff chunk offset".into()))?;
        if offset != expected
            || digest(&data) != hash
            || data.len() != (item.bytes - offset).min(CHUNK_BYTES)
        {
            return Err(DbError::Invalid("HANDOFF_CHUNK_HASH_MISMATCH".into()));
        }
        let from = start.saturating_sub(offset);
        let to = (end - offset).min(data.len());
        result.extend_from_slice(&data[from..to]);
        expected = offset + data.len();
    }
    if result.len() != end - start {
        return Err(DbError::Invalid("HANDOFF_CHUNK_MISSING".into()));
    }
    Ok(result)
}
fn utf8_prefix(bytes: &[u8]) -> Result<&str, DbError> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(text),
        Err(error) if error.error_len().is_none() => {
            std::str::from_utf8(&bytes[..error.valid_up_to()])
                .map_err(|_| DbError::Validation("invalid UTF-8 boundary".into()))
        }
        Err(_) => Err(DbError::Validation("invalid UTF-8 boundary".into())),
    }
}
fn envelope(op: &str, binding: &str, result: Value, next: Option<(i64, usize)>) -> Value {
    let cursor = next.map(|(row, offset)| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(json!([binding, row, offset]).to_string())
    });
    let mut response = json!({"operationId":op,"referenceOnly":true,"complete":cursor.is_none(),"nextCursor":cursor});
    response["result"] = result;
    response
}
fn descriptor(item: &Entry) -> Value {
    json!({"ref":item.reference,"sourceId":item.source,"sha256":item.sha,"bytes":item.bytes})
}
type ReferenceSpan<'a> = (&'a str, Option<(usize, usize)>);
fn reference_span(reference: &str) -> Result<ReferenceSpan<'_>, DbError> {
    let Some((base, suffix)) = reference.rsplit_once('@') else {
        return Ok((reference, None));
    };
    let Some((start, end)) = suffix.split_once(':') else {
        return Err(DbError::Validation("invalid evidence span".into()));
    };
    let start = start
        .parse::<usize>()
        .map_err(|_| DbError::Validation("invalid evidence span".into()))?;
    let end = end
        .parse::<usize>()
        .map_err(|_| DbError::Validation("invalid evidence span".into()))?;
    if start > end {
        return Err(DbError::Validation("invalid evidence span".into()));
    }
    Ok((base, Some((start, end))))
}
fn read(
    conn: &Connection,
    op: &str,
    query: &HandoffQuery,
    binding: &str,
    cursor: Option<(i64, usize)>,
    budget: &Budget,
) -> Result<Value, DbError> {
    let requested = query
        .reference
        .as_deref()
        .ok_or_else(|| DbError::Validation("ref is required".into()))?;
    let (reference, span) = reference_span(requested)?;
    let item = entry(conn, op, query, 0, Some(reference))?
        .ok_or_else(|| DbError::Validation("ref is not in this handoff".into()))?;
    let (begin, end) = span.unwrap_or((0, item.bytes));
    let start = cursor.map_or(begin, |(_, offset)| offset);
    if start < begin
        || start > end
        || end > item.bytes
        || cursor.is_some_and(|(row, _)| row != item.ordinal)
    {
        return Err(DbError::Validation("invalid handoff read cursor".into()));
    }
    // Validate both span boundaries, even if only its first page is requested.
    for boundary in [begin, end, start] {
        if boundary < item.bytes {
            let first = bytes(conn, op, &item, boundary, boundary + 1, budget)?[0];
            if first & 0xc0 == 0x80 {
                return Err(DbError::Validation("invalid UTF-8 boundary".into()));
            }
        }
    }
    let data = bytes(
        conn,
        op,
        &item,
        start,
        (start + CHUNK_BYTES).min(end),
        budget,
    )?;
    let mut text = utf8_prefix(&data)?;
    loop {
        let next = start + text.len();
        let result = envelope(
            op,
            binding,
            json!({"ref":requested,"sourceId":item.source,"sha256":item.sha,"text":text,"start":start,"end":next,"totalBytes":end-begin,"rangeStart":begin,"rangeEnd":end}),
            (next < end).then_some((item.ordinal, next)),
        );
        if result.to_string().len() <= RESPONSE_BYTES {
            return Ok(result);
        }
        if text.len() < 2 {
            return Err(DbError::Invalid("HANDOFF_RESPONSE_LIMIT".into()));
        }
        let mut split = text.len() / 2;
        while !text.is_char_boundary(split) {
            split -= 1;
        }
        text = &text[..split];
    }
}

fn page_result(
    op: &str,
    binding: &str,
    rows: &[Value],
    next: Option<(i64, usize)>,
    scanned: usize,
) -> Value {
    envelope(
        op,
        binding,
        json!({"entries":rows,"scannedBytes":scanned,"scanLimitBytes":SCAN_BYTES}),
        next,
    )
}
enum SearchRow {
    Found(Value),
    Skip,
    Pause,
}
fn search_row(
    conn: &Connection,
    op: &str,
    item: &Entry,
    needle: &[u8],
    position: &mut (i64, usize),
    budget: &mut Budget,
) -> Result<SearchRow, DbError> {
    let mut row = descriptor(item);
    if position.1 == item.bytes {
        *position = (item.ordinal + 1, 0);
        return Ok(SearchRow::Skip);
    }
    let remaining = SCAN_BYTES - budget.scanned;
    if remaining < needle.len() {
        return Ok(SearchRow::Pause);
    }
    let scan_end = (position.1 + CHUNK_BYTES.min(remaining - needle.len() + 1)).min(item.bytes);
    let data = bytes(
        conn,
        op,
        item,
        position.1,
        (scan_end + needle.len() - 1).min(item.bytes),
        budget,
    )?;
    budget.scanned += data.len();
    let found = data
        .windows(needle.len())
        .position(|candidate| candidate == needle)
        .filter(|index| position.1 + index < scan_end);
    if let Some(index) = found {
        let start = position.1 + index;
        let excerpt = bytes(
            conn,
            op,
            item,
            start,
            (start + 1024.min(SCAN_BYTES - budget.scanned)).min(item.bytes),
            budget,
        )?;
        budget.scanned += excerpt.len();
        row["excerpt"] = json!(utf8_prefix(&excerpt)?);
        row["start"] = json!(start);
        row["end"] = json!(start + needle.len());
        *position = (item.ordinal, start + needle.len());
    } else {
        *position = (item.ordinal, scan_end);
        return Ok(SearchRow::Skip);
    }
    Ok(SearchRow::Found(row))
}

fn scan(
    conn: &Connection,
    op: &str,
    query: &HandoffQuery,
    binding: &str,
    mut position: (i64, usize),
    budget: &mut Budget,
) -> Result<Value, DbError> {
    let mut rows = Vec::new();
    let limit = query.limit.unwrap_or(20);
    let needle = query.query.as_deref().unwrap_or("").as_bytes();
    if query.action == "search" && (needle.is_empty() || needle.len() > CHUNK_BYTES) {
        return Err(DbError::Validation(
            "search requires 1–8192 bytes of literal text".into(),
        ));
    }
    loop {
        budget.check()?;
        if budget.scanned >= SCAN_BYTES {
            return Ok(page_result(
                op,
                binding,
                &rows,
                Some(position),
                budget.scanned,
            ));
        }
        let Some(item) = entry(conn, op, query, position.0, None)? else {
            return Ok(page_result(op, binding, &rows, None, budget.scanned));
        };
        if item.ordinal != position.0 {
            position = (item.ordinal, 0);
        }
        if position.1 > item.bytes {
            return Err(DbError::Validation("invalid handoff scan cursor".into()));
        }
        let before = position;
        let mut row = descriptor(&item);
        if query.action == "list" {
            if item.reference.starts_with("asset:") {
                let data = bytes(conn, op, &item, 0, item.bytes.min(CHUNK_BYTES), budget)?;
                row["asset"] = serde_json::from_slice(&data)
                    .map_err(|_| DbError::Invalid("HANDOFF_ASSET_METADATA_LIMIT".into()))?;
            }
            position = (item.ordinal + 1, 0);
        } else {
            match search_row(conn, op, &item, needle, &mut position, budget)? {
                SearchRow::Found(found) => row = found,
                SearchRow::Skip => continue,
                SearchRow::Pause => {
                    return Ok(page_result(
                        op,
                        binding,
                        &rows,
                        Some(position),
                        budget.scanned,
                    ));
                }
            }
        }
        rows.push(row);
        if page_result(op, binding, &rows, Some(position), budget.scanned)
            .to_string()
            .len()
            > RESPONSE_BYTES
        {
            rows.pop();
            if rows.is_empty() {
                return Err(DbError::Invalid("HANDOFF_RESPONSE_LIMIT".into()));
            }
            return Ok(page_result(
                op,
                binding,
                &rows,
                Some(before),
                budget.scanned,
            ));
        }
        if rows.len() >= limit {
            return Ok(page_result(
                op,
                binding,
                &rows,
                Some(position),
                budget.scanned,
            ));
        }
    }
}

pub(super) fn query(
    conn: &Connection,
    op: &str,
    query: &HandoffQuery,
    cancelled: Arc<AtomicBool>,
) -> Result<Value, DbError> {
    if !matches!(query.action.as_str(), "list" | "search" | "read") {
        return Err(DbError::Validation("unknown handoff action".into()));
    }
    if query.limit.is_some_and(|limit| !(1..=20).contains(&limit)) {
        return Err(DbError::Validation("handoff limit must be 1–20".into()));
    }
    let binding = digest(
        json!([
            op,
            query.action,
            query.reference,
            query.query,
            query.source_id,
            query.section
        ])
        .to_string()
        .as_bytes(),
    );
    let cursor = if let Some(raw) = &query.cursor {
        if raw.len() > 1024 {
            return Err(DbError::Validation("invalid handoff cursor".into()));
        }
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| DbError::Validation("invalid handoff cursor".into()))?;
        let (owner, row, offset): (String, i64, usize) = serde_json::from_slice(&raw)?;
        if owner != binding || row < 0 {
            return Err(DbError::Validation(
                "cursor belongs to another handoff query".into(),
            ));
        }
        Some((row, offset))
    } else {
        None
    };
    let mut budget = Budget {
        deadline: Instant::now() + Duration::from_secs(15),
        cancelled,
        scanned: 0,
    };
    if query.action == "read" {
        read(conn, op, query, &binding, cursor, &budget)
    } else {
        scan(
            conn,
            op,
            query,
            &binding,
            cursor.unwrap_or((0, 0)),
            &mut budget,
        )
    }
}
