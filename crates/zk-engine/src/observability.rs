//! Best-effort structured operations telemetry, separate from durable Run events.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use serde::Serialize;
use serde_json::Value;

/// One structured operations event. High-cardinality identities are fields,
/// never metric labels.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservabilityEvent {
    /// Epoch milliseconds.
    pub timestamp_ms: i64,
    /// Low-cardinality domain (`llm`, `tool`, `hook`, ...).
    pub domain: String,
    /// Low-cardinality action (`start`, `complete`, `denied`, ...).
    pub action: String,
    /// Low-cardinality outcome.
    pub outcome: String,
    /// Optional session identity, stored only in the structured event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Optional Run identity, stored only in the structured event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Optional tool-use identity, stored only in the structured event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
    /// Optional elapsed milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Bounded, secret-free diagnostic attributes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
    /// Security audit events additionally increment the audit failure counter
    /// when the queue or sink is unavailable.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub security_audit: bool,
}

impl ObservabilityEvent {
    /// Construct a low-cardinality event with the current timestamp.
    #[must_use]
    pub fn new(
        domain: impl Into<String>,
        action: impl Into<String>,
        outcome: impl Into<String>,
    ) -> Self {
        Self {
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |duration| {
                    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
                }),
            domain: domain.into(),
            action: action.into(),
            outcome: outcome.into(),
            session_id: None,
            run_id: None,
            tool_use_id: None,
            duration_ms: None,
            attributes: BTreeMap::new(),
            security_audit: false,
        }
    }
}

/// Recorder health counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObservabilityHealth {
    /// Events accepted by the bounded queue.
    pub accepted: u64,
    /// Events dropped because the bounded queue was full.
    pub dropped: u64,
    /// Queue disconnects or sink write failures.
    pub write_failures: u64,
    /// Failed security audit records (also included in `write_failures` or `dropped`).
    pub audit_failures: u64,
}

/// Narrow recording port shared by Engine, tools, Python and MCP bridges.
pub trait ObservabilityRecorder: Send + Sync {
    /// Non-blocking best-effort record.
    fn record(&self, event: ObservabilityEvent);
    /// Current health counters.
    fn health(&self) -> ObservabilityHealth;
}

/// Event destination used by the background writer.
pub trait ObservabilitySink: Send + Sync + 'static {
    /// Persist one event without changing business state.
    ///
    /// # Errors
    /// Returns a bounded diagnostic when the destination cannot persist the event.
    fn write(&self, event: &ObservabilityEvent) -> Result<(), String>;
}

/// Append-only JSONL sink kept separate from `SQLite` Run recovery facts.
pub struct JsonlObservabilitySink {
    path: PathBuf,
    lock: Mutex<()>,
}

impl JsonlObservabilitySink {
    /// Construct for a fixed log path.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            lock: Mutex::new(()),
        }
    }
}

impl ObservabilitySink for JsonlObservabilitySink {
    fn write(&self, event: &ObservabilityEvent) -> Result<(), String> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| "sink lock poisoned".to_owned())?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut encoded = serde_json::to_vec(event).map_err(|error| error.to_string())?;
        encoded.push(b'\n');
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| error.to_string())?;
        file.write_all(&encoded).map_err(|error| error.to_string())
    }
}

struct Counters {
    accepted: AtomicU64,
    dropped: AtomicU64,
    write_failures: AtomicU64,
    audit_failures: AtomicU64,
}

/// Production bounded recorder. `record` never waits for disk or queue space.
pub struct BoundedObservabilityRecorder {
    sender: mpsc::SyncSender<ObservabilityEvent>,
    counters: Arc<Counters>,
}

impl BoundedObservabilityRecorder {
    /// Start one background writer with a strictly positive queue capacity.
    ///
    /// # Panics
    /// Panics only when the operating system cannot create the writer thread.
    #[must_use]
    pub fn new(capacity: usize, sink: Arc<dyn ObservabilitySink>) -> Self {
        let (sender, receiver) = mpsc::sync_channel(capacity.max(1));
        let counters = Arc::new(Counters {
            accepted: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            audit_failures: AtomicU64::new(0),
        });
        let worker_counters = Arc::clone(&counters);
        std::thread::Builder::new()
            .name("zk-observability-writer".to_owned())
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    if sink.write(&event).is_err() {
                        worker_counters
                            .write_failures
                            .fetch_add(1, Ordering::Relaxed);
                        if event.security_audit {
                            worker_counters
                                .audit_failures
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
            .expect("observability writer thread");
        Self { sender, counters }
    }
}

impl ObservabilityRecorder for BoundedObservabilityRecorder {
    fn record(&self, event: ObservabilityEvent) {
        let audit = event.security_audit;
        match self.sender.try_send(event) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::TrySendError::Full(_)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                if audit {
                    self.counters.audit_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.counters.write_failures.fetch_add(1, Ordering::Relaxed);
                if audit {
                    self.counters.audit_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn health(&self) -> ObservabilityHealth {
        ObservabilityHealth {
            accepted: self.counters.accepted.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            write_failures: self.counters.write_failures.load(Ordering::Relaxed),
            audit_failures: self.counters.audit_failures.load(Ordering::Relaxed),
        }
    }
}

/// Disabled recorder used in isolated library tests.
#[derive(Default)]
pub struct NoopObservabilityRecorder;

impl ObservabilityRecorder for NoopObservabilityRecorder {
    fn record(&self, _event: ObservabilityEvent) {}

    fn health(&self) -> ObservabilityHealth {
        ObservabilityHealth::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    #[test]
    fn jsonl_sink_persists_parseable_events_to_disk() {
        let root =
            std::env::temp_dir().join(format!("zk-observability-jsonl-{}", uuid::Uuid::new_v4()));
        let path = root.join("events/operations.jsonl");
        let recorder = BoundedObservabilityRecorder::new(
            8,
            Arc::new(JsonlObservabilitySink::new(path.clone())),
        );

        let mut first = ObservabilityEvent::new("tool", "start", "ok");
        first.session_id = Some("session-real-jsonl".to_owned());
        first.run_id = Some("run-real-jsonl".to_owned());
        first
            .attributes
            .insert("tool".to_owned(), Value::String("Read".to_owned()));
        recorder.record(first);

        let mut second = ObservabilityEvent::new("hook", "pre", "denied");
        second.security_audit = true;
        recorder.record(second);

        let mut contents = String::new();
        for _ in 0..100 {
            contents = std::fs::read_to_string(&path).unwrap_or_default();
            if contents.lines().count() == 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        let events: Vec<Value> = contents
            .lines()
            .map(|line| serde_json::from_str(line).expect("each JSONL line must parse"))
            .collect();
        assert_eq!(
            events.len(),
            2,
            "both accepted events must reach the real file"
        );
        assert_eq!(events[0]["domain"], "tool");
        assert_eq!(events[0]["sessionId"], "session-real-jsonl");
        assert_eq!(events[0]["attributes"]["tool"], "Read");
        assert_eq!(events[1]["securityAudit"], true);
        assert_eq!(recorder.health().accepted, 2);
        assert_eq!(recorder.health().dropped, 0);
        assert_eq!(recorder.health().write_failures, 0);

        std::fs::remove_dir_all(root).ok();
    }

    struct BlockingSink {
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl ObservabilitySink for BlockingSink {
        fn write(&self, _event: &ObservabilityEvent) -> Result<(), String> {
            let (lock, condition) = &*self.gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = condition.wait(open).unwrap();
            }
            Ok(())
        }
    }

    #[test]
    fn full_queue_drops_without_blocking_business() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let recorder = BoundedObservabilityRecorder::new(
            1,
            Arc::new(BlockingSink {
                gate: Arc::clone(&gate),
            }),
        );
        recorder.record(ObservabilityEvent::new("tool", "start", "ok"));
        std::thread::sleep(Duration::from_millis(20));
        recorder.record(ObservabilityEvent::new("tool", "end", "ok"));
        recorder.record(ObservabilityEvent::new("tool", "end", "ok"));
        assert!(recorder.health().dropped >= 1);
        let (lock, condition) = &*gate;
        *lock.lock().unwrap() = true;
        condition.notify_all();
    }

    struct FailingSink;

    impl ObservabilitySink for FailingSink {
        fn write(&self, _event: &ObservabilityEvent) -> Result<(), String> {
            Err("disk unavailable".to_owned())
        }
    }

    #[test]
    fn sink_failure_counts_security_audit_failure() {
        let recorder = BoundedObservabilityRecorder::new(2, Arc::new(FailingSink));
        let mut event = ObservabilityEvent::new("hook", "denied", "error");
        event.security_audit = true;
        recorder.record(event);
        for _ in 0..50 {
            if recorder.health().write_failures > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(recorder.health().write_failures, 1);
        assert_eq!(recorder.health().audit_failures, 1);
    }
}
