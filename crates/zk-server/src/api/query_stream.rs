//! Bounded, memory-only Query event replay. Reconnection never re-executes a Query.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{
    IntoResponse, Response, Sse,
    sse::{Event, KeepAlive},
};
use futures::stream;
use serde_json::json;
use tokio::sync::watch;
use zk_engine::ConversationCancellation;
use zk_protocol::ServerMessage;

use super::{PreparedQuery, QueryExecution};
use crate::error::ApiError;
use crate::ws::hub::SessionEvents;

const RUN_BYTES: usize = 8 * 1024 * 1024;
const TOTAL_BYTES: usize = 64 * 1024 * 1024;
const MAX_EVENTS: usize = 4096;
const MAX_EXECUTIONS: usize = 32;
const MAX_READERS: usize = 8;
const DISCONNECT_GRACE: Duration = Duration::from_secs(15);

#[derive(Default)]
pub(crate) struct QueryStreams {
    active: Mutex<HashMap<String, Arc<Replay>>>,
    bytes: Arc<AtomicUsize>,
}

struct Frame {
    seq: u64,
    name: String,
    data: Arc<str>,
    charge: usize,
}

struct Buffer {
    frames: VecDeque<Frame>,
    bytes: usize,
    next: u64,
    terminal: bool,
    readers: usize,
    generation: u64,
    global_bytes: Arc<AtomicUsize>,
}

impl Buffer {
    fn evict(&mut self) -> bool {
        let Some(frame) = self.frames.pop_front() else {
            return false;
        };
        self.bytes -= frame.charge;
        self.global_bytes.fetch_sub(frame.charge, Ordering::AcqRel);
        true
    }

    fn push(&mut self, name: &str, data: String) -> Result<(), &'static str> {
        let charge = data.len().saturating_add(name.len()).saturating_add(128);
        if charge > RUN_BYTES {
            return Err("QUERY_EVENT_TOO_LARGE");
        }
        while self.bytes.saturating_add(charge) > RUN_BYTES || self.frames.len() >= MAX_EVENTS {
            self.evict();
        }
        loop {
            if self
                .global_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |total| {
                    total
                        .checked_add(charge)
                        .filter(|next| *next <= TOTAL_BYTES)
                })
                .is_ok()
            {
                break;
            }
            if !self.evict() {
                return Err("QUERY_REPLAY_CAPACITY");
            }
        }
        let seq = self.next;
        self.next += 1;
        self.bytes += charge;
        self.frames.push_back(Frame {
            seq,
            name: name.to_owned(),
            data: data.into(),
            charge,
        });
        Ok(())
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.global_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Replay {
    request: String,
    session: String,
    cancellation: ConversationCancellation,
    buffer: Mutex<Buffer>,
    changed: watch::Sender<u64>,
}

impl Replay {
    fn push(&self, name: &str, data: String) -> Result<(), &'static str> {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        buffer.push(name, data)?;
        self.changed.send_replace(buffer.next);
        Ok(())
    }

    fn error(&self, code: &str) {
        let _ = self.push(
            "error",
            json!({"type":"error", "code":code,
            "message":"Query streaming could not continue; execution is stopping"})
            .to_string(),
        );
    }

    fn close(&self, success: bool) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buffer.terminal {
            return;
        }
        // Evict our own old frames if necessary; a terminal marker is small and
        // must never be displaced by an oversized model/tool result.
        let data = json!({"requestId":self.request,"sessionId":self.session,"success":success})
            .to_string();
        if buffer.push("complete", data.clone()).is_err() {
            while buffer.evict() {}
            let _ = buffer.push("complete", data);
        }
        buffer.terminal = true;
        self.changed.send_replace(buffer.next);
    }

    fn reader(self: &Arc<Self>, after: u64) -> Result<Reader, ApiError> {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buffer.terminal {
            return Err(unavailable());
        }
        let first = buffer.frames.front().map_or(buffer.next, |frame| frame.seq);
        if after < first.saturating_sub(1) {
            return Err(cursor_expired());
        }
        if after >= buffer.next {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                "QUERY_CURSOR_INVALID",
                "Cursor is ahead of this execution",
            ));
        }
        if buffer.readers >= MAX_READERS {
            return Err(api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "QUERY_READER_LIMIT",
                "Too many readers for this query",
            ));
        }
        buffer.readers += 1;
        buffer.generation += 1;
        Ok(Reader {
            replay: self.clone(),
            changed: self.changed.subscribe(),
            after,
            done: false,
        })
    }
}

impl QueryStreams {
    pub(super) fn start(
        self: &Arc<Self>,
        prepared: PreparedQuery,
        events: SessionEvents,
        include_partial: bool,
    ) -> Result<Response, ApiError> {
        let (changed, _) = watch::channel(0);
        let replay = Arc::new(Replay {
            request: prepared.request_id.clone(),
            session: prepared.lease.session_id().to_owned(),
            cancellation: prepared.service.cancellation(&prepared.lease),
            changed,
            buffer: Mutex::new(Buffer {
                frames: VecDeque::new(),
                bytes: 0,
                next: 1,
                terminal: false,
                readers: 0,
                generation: 0,
                global_bytes: self.bytes.clone(),
            }),
        });
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active.len() >= MAX_EXECUTIONS {
            return Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "QUERY_STREAM_LIMIT",
                "Too many active query streams",
            ));
        }
        if active.contains_key(&replay.request) {
            return Err(api_error(
                StatusCode::CONFLICT,
                "QUERY_REQUEST_ACTIVE",
                "This query already has a stream",
            ));
        }
        replay.push("query_started", json!({"type":"query_started","requestId":replay.request,"sessionId":replay.session}).to_string())
            .map_err(|code| api_error(StatusCode::SERVICE_UNAVAILABLE,code,"Query replay memory is full"))?;
        let reader = replay.reader(0)?;
        active.insert(replay.request.clone(), replay.clone());
        drop(active);
        let owner = Producer {
            registry: Arc::downgrade(self),
            replay,
        };
        tokio::spawn(pump(prepared.start(), events, include_partial, owner));
        Ok(response(reader))
    }

    pub(super) fn resume(&self, request: &str, cursor: &str) -> Result<Response, ApiError> {
        let replay = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(request)
            .cloned()
            .ok_or_else(unavailable)?;
        let after = cursor
            .strip_prefix(&format!("{request}:"))
            .and_then(|sequence| sequence.parse::<u64>().ok())
            .ok_or_else(|| {
                api_error(
                    StatusCode::BAD_REQUEST,
                    "QUERY_CURSOR_INVALID",
                    "Cursor must belong to this requestId",
                )
            })?;
        Ok(response(replay.reader(after)?))
    }
}

struct Producer {
    registry: Weak<QueryStreams>,
    replay: Arc<Replay>,
}

impl Drop for Producer {
    fn drop(&mut self) {
        // Also protects against an aborted/panicked projection task. The owned
        // QueryExecution cancels the engine while its worker retains cleanup.
        if !self
            .replay
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .terminal
        {
            self.replay.error("QUERY_EXECUTION_LOST");
            self.replay.close(false);
        }
        if let Some(registry) = self.registry.upgrade() {
            registry
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.replay.request);
        }
    }
}

fn emit(
    replay: &Replay,
    envelope: &zk_protocol::ServerEnvelope,
    include_partial: bool,
) -> Result<(), &'static str> {
    if !include_partial
        && matches!(
            envelope.msg.kind(),
            "stream_delta" | "thinking_delta" | "tool_input_delta"
        )
    {
        return Ok(());
    }
    let name = match &envelope.msg {
        ServerMessage::StreamDelta { .. } => "text",
        ServerMessage::ThinkingDelta { .. } => "thinking",
        ServerMessage::MessageComplete { .. } => "assistant_message",
        message => message.kind(),
    };
    let data = serde_json::to_string(&envelope).map_err(|_| "QUERY_STREAM_SERIALIZATION_FAILED")?;
    // Original Task/Run/Invocation/eventId and native recovery cursor stay in
    // the payload. The SSE ID identifies this ordered transport projection.
    replay.push(name, data)
}

async fn pump(
    mut execution: QueryExecution,
    mut events: SessionEvents,
    include_partial: bool,
    owner: Producer,
) {
    let replay = &owner.replay;
    let mut failure = None;
    let completion = loop {
        if events.overflowed() && failure.is_none() {
            failure = Some("QUERY_STREAM_OVERFLOW");
            replay.error("QUERY_STREAM_OVERFLOW");
            execution.cancellation.cancel("QUERY_STREAM_OVERFLOW");
        }
        tokio::select! {
            biased;
            envelope = events.receiver.recv(), if failure.is_none() => {
                if let Some(envelope) = envelope {
                    if let Err(code) = emit(replay, &envelope, include_partial) {
                        failure = Some(code); replay.error(code); execution.cancellation.cancel(code);
                    }
                } else {
                    failure = Some("QUERY_EVENT_SOURCE_CLOSED");
                    replay.error("QUERY_EVENT_SOURCE_CLOSED");
                    execution.cancellation.cancel("QUERY_EVENT_SOURCE_CLOSED");
                }
            },
            outcome = &mut execution.completion => break outcome,
        }
    };
    // Finish only after previously published events have been projected.
    if failure.is_none() {
        while let Ok(envelope) = events.receiver.try_recv() {
            if let Err(code) = emit(replay, &envelope, include_partial) {
                failure = Some(code);
                replay.error(code);
                break;
            }
        }
    }
    execution.finished = true;
    if let Ok(mut outcome) = completion {
        if outcome.error.is_none() {
            outcome.error = failure.map(str::to_owned);
        }
        let success = outcome.error.is_none();
        match serde_json::to_string(&outcome)
            .map_err(|_| "QUERY_RESULT_SERIALIZATION_FAILED")
            .and_then(|data| replay.push("result", data))
        {
            Ok(()) => replay.close(success),
            Err(code) => {
                replay.error(code);
                replay.close(false);
            }
        }
    } else {
        replay.error("QUERY_EXECUTION_LOST");
        replay.close(false);
    }
}

struct Reader {
    replay: Arc<Replay>,
    changed: watch::Receiver<u64>,
    after: u64,
    done: bool,
}

impl Reader {
    async fn next(&mut self) -> Option<Result<Event, Infallible>> {
        loop {
            if self.done {
                return None;
            }
            {
                let buffer = self
                    .replay
                    .buffer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let first = buffer.frames.front().map_or(buffer.next, |frame| frame.seq);
                if self.after < first.saturating_sub(1) {
                    self.done = true;
                    return Some(Ok(Event::default().event("error").data(
                        json!({"type":"error","code":"QUERY_CURSOR_EXPIRED",
                        "message":"Fetch a current session snapshot; do not resend the Query"})
                        .to_string(),
                    )));
                }
                if let Some(frame) = buffer.frames.iter().find(|frame| frame.seq > self.after) {
                    self.after = frame.seq;
                    self.done = frame.name == "complete";
                    return Some(Ok(Event::default()
                        .event(&frame.name)
                        .id(format!("{}:{}", self.replay.request, frame.seq))
                        .data(frame.data.as_ref())));
                }
                if buffer.terminal {
                    self.done = true;
                    return None;
                }
            }
            if self.changed.changed().await.is_err() {
                self.done = true;
                return None;
            }
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        let generation = {
            let mut buffer = self
                .replay
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            buffer.readers = buffer.readers.saturating_sub(1);
            buffer.generation += 1;
            if buffer.readers != 0 || buffer.terminal {
                return;
            }
            buffer.generation
        };
        let replay = Arc::downgrade(&self.replay);
        tokio::spawn(async move {
            tokio::time::sleep(DISCONNECT_GRACE).await;
            if let Some(replay) = replay.upgrade() {
                let buffer = replay
                    .buffer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let cancel =
                    buffer.readers == 0 && !buffer.terminal && buffer.generation == generation;
                drop(buffer);
                if cancel {
                    replay.cancellation.cancel("QUERY_TRANSPORT_CLOSED");
                }
            }
        });
    }
}

fn response(reader: Reader) -> Response {
    let events = stream::unfold(reader, |mut reader| async move {
        reader.next().await.map(|event| (event, reader))
    });
    Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(10))
                .text("keep-alive"),
        )
        .into_response()
}

fn api_error(status: StatusCode, code: &str, message: &str) -> ApiError {
    ApiError {
        status,
        code: code.into(),
        message: message.into(),
    }
}
fn unavailable() -> ApiError {
    api_error(
        StatusCode::GONE,
        "QUERY_STREAM_UNAVAILABLE",
        "Only a still-running in-memory execution can be reconnected; do not resend the Query",
    )
}
fn cursor_expired() -> ApiError {
    api_error(
        StatusCode::CONFLICT,
        "QUERY_CURSOR_EXPIRED",
        "Fetch a current session snapshot; do not resend the Query",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoEvents;

    impl zk_engine::MessageSink for NoEvents {
        fn push<'a>(&'a self, _: &'a str, _: ServerMessage) -> futures::future::BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    fn replay() -> Arc<Replay> {
        let engine = Arc::new(zk_engine::Engine::new(
            zk_db::Db::open_in_memory().unwrap(),
            Arc::new(zk_llm::ProviderRegistry::new()),
            Arc::new(NoEvents),
        ));
        let lease = engine.reserve_conversation("stream-limits").unwrap();
        let cancellation = lease.cancellation(engine);
        let (changed, _) = watch::channel(0);
        Arc::new(Replay {
            request: "bounded-request".into(),
            session: "stream-limits".into(),
            cancellation,
            buffer: Mutex::new(buffer(Arc::new(AtomicUsize::new(0)))),
            changed,
        })
    }

    async fn event_text(event: Result<Event, Infallible>) -> String {
        let response = Sse::new(stream::iter([event])).into_response();
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn buffer(global: Arc<AtomicUsize>) -> Buffer {
        Buffer {
            frames: VecDeque::new(),
            bytes: 0,
            next: 1,
            terminal: false,
            readers: 0,
            generation: 0,
            global_bytes: global,
        }
    }

    #[test]
    fn replay_capacity_is_bounded_across_readers_and_expiry_releases_accounting() {
        let global = Arc::new(AtomicUsize::new(0));
        let mut buffers = Vec::new();
        for _ in 0..9 {
            let mut replay = buffer(global.clone());
            // Each ring must evict old projections without recycling a cursor.
            for _ in 0..12 {
                replay.push("text", "x".repeat(1024 * 1024)).unwrap();
            }
            assert!(replay.bytes <= RUN_BYTES);
            assert!(replay.frames.front().unwrap().seq > 1);
            assert_eq!(replay.next, 13);
            buffers.push(replay);
        }
        assert!(global.load(Ordering::Acquire) <= TOTAL_BYTES);
        let before = global.load(Ordering::Acquire);
        let mut full = buffer(global.clone());
        assert_eq!(
            full.push("text", "x".repeat(RUN_BYTES - 132)),
            Err("QUERY_REPLAY_CAPACITY")
        );
        assert_eq!(global.load(Ordering::Acquire), before);
        assert_eq!(
            full.push("text", "x".repeat(RUN_BYTES)),
            Err("QUERY_EVENT_TOO_LARGE")
        );
        assert_eq!(global.load(Ordering::Acquire), before);
        drop(buffers);
        assert_eq!(global.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn slow_readers_fail_explicitly_and_terminal_cannot_restart_a_stream() {
        let replay = replay();
        replay.push("text", "first".into()).unwrap();
        let mut slow = replay.reader(0).unwrap();
        for _ in 0..MAX_EVENTS {
            replay.push("text", "later".into()).unwrap();
        }
        assert_eq!(replay.reader(0).err().unwrap().code, "QUERY_CURSOR_EXPIRED");
        // A reader already holding a connection also receives a terminal error
        // instead of silently skipping events which have left the memory ring.
        assert!(
            event_text(slow.next().await.unwrap())
                .await
                .contains("QUERY_CURSOR_EXPIRED")
        );
        assert!(slow.next().await.is_none());
        assert_eq!(
            replay.reader(u64::MAX).err().unwrap().code,
            "QUERY_CURSOR_INVALID"
        );
        let latest = replay.buffer.lock().unwrap().next - 1;
        let mut current = replay.reader(latest).unwrap();
        replay.close(false);
        replay.close(true);
        let terminal = event_text(current.next().await.unwrap()).await;
        assert!(terminal.contains("event: complete"));
        assert!(terminal.contains("\"success\":false"));
        assert!(current.next().await.is_none());
        assert_eq!(
            replay.reader(latest).err().unwrap().code,
            "QUERY_STREAM_UNAVAILABLE"
        );
    }

    #[tokio::test]
    async fn simultaneous_readers_share_a_bounded_ring_without_duplicating_execution() {
        let replay = replay();
        replay.push("text", "one projection".into()).unwrap();
        let mut readers = (0..MAX_READERS)
            .map(|_| replay.reader(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(replay.reader(0).err().unwrap().code, "QUERY_READER_LIMIT");
        let charged = replay.buffer.lock().unwrap().bytes;
        let events = futures::future::join_all(readers.iter_mut().map(Reader::next)).await;
        for event in events {
            assert!(event_text(event.unwrap()).await.contains("one projection"));
        }
        assert_eq!(replay.buffer.lock().unwrap().bytes, charged);
        readers.pop();
        readers.push(replay.reader(0).unwrap());
        replay.close(true);
        assert_eq!(replay.buffer.lock().unwrap().readers, MAX_READERS);
        drop(readers);
        assert_eq!(replay.buffer.lock().unwrap().readers, 0);
    }
}
