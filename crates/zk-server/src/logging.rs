//! Secure structured-log writer with bounded, restart-safe size rotation.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing_subscriber::fmt::MakeWriter;

/// Maximum size of the active production log file.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 100 * 1024 * 1024;
/// Number of completed production files retained beside the active file.
pub const DEFAULT_MAX_ARCHIVES: usize = 5;
/// Replacement used for values whose field name is classified as sensitive.
pub const REDACTED: &str = "[REDACTED]";

/// File/stderr destinations and rotation limits for the structured log writer.
#[derive(Clone, Debug)]
pub struct LogWriterConfig {
    file_path: Option<PathBuf>,
    mirror_stderr: bool,
    max_file_bytes: u64,
    max_archives: usize,
}

impl LogWriterConfig {
    /// Build the production configuration.
    ///
    /// The default file is `logs/zk-server.jsonl` beside the database. Setting
    /// `ZK_LOG_FILE` to an explicit path overrides it; `off` disables only file
    /// output and retains structured stderr logging.
    #[must_use]
    pub fn production(database_path: &Path) -> Self {
        let default_parent = database_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let default_path = default_parent.join("logs").join("zk-server.jsonl");
        let file_path = match std::env::var("ZK_LOG_FILE") {
            Ok(value) if value.trim().eq_ignore_ascii_case("off") => None,
            Ok(value) if !value.trim().is_empty() => Some(PathBuf::from(value.trim())),
            _ => Some(default_path),
        };
        Self {
            file_path,
            mirror_stderr: true,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_archives: DEFAULT_MAX_ARCHIVES,
        }
    }

    /// Disable every destination. This is the default test assembly when logs
    /// are irrelevant and guarantees that no file or terminal output is made.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            file_path: None,
            mirror_stderr: false,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_archives: DEFAULT_MAX_ARCHIVES,
        }
    }

    /// Build an isolated file-only writer configuration for tests. Production
    /// code should use [`Self::production`] so stderr remains available.
    #[doc(hidden)]
    #[must_use]
    pub fn file_only_for_test(path: PathBuf, max_file_bytes: u64, max_archives: usize) -> Self {
        Self {
            file_path: Some(path),
            mirror_stderr: false,
            max_file_bytes,
            max_archives,
        }
    }
}

/// Cloneable `tracing_subscriber` writer factory. Each emitted event is buffered
/// until a complete JSON line is available, sanitized, then written to every
/// configured destination.
#[derive(Clone)]
pub struct SecureRollingMakeWriter {
    file: Option<Arc<Mutex<RollingFile>>>,
    mirror_stderr: bool,
}

impl std::fmt::Debug for SecureRollingMakeWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecureRollingMakeWriter")
            .field("file_enabled", &self.file.is_some())
            .field("mirror_stderr", &self.mirror_stderr)
            .finish()
    }
}

impl SecureRollingMakeWriter {
    /// Open and validate all configured file destinations before installing the
    /// tracing subscriber.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the directory or active log file cannot be
    /// created, inspected, or opened.
    pub fn new(config: LogWriterConfig) -> io::Result<Self> {
        let file = config
            .file_path
            .map(|path| {
                RollingFile::open(path, config.max_file_bytes, config.max_archives)
                    .map(|file| Arc::new(Mutex::new(file)))
            })
            .transpose()?;
        Ok(Self {
            file,
            mirror_stderr: config.mirror_stderr,
        })
    }

    fn lock_file(&self) -> Option<MutexGuard<'_, RollingFile>> {
        self.file.as_ref().map(|file| match file.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        })
    }
}

impl<'writer> MakeWriter<'writer> for SecureRollingMakeWriter {
    type Writer = SecureEventWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        SecureEventWriter {
            destination: self.clone(),
            pending: Vec::with_capacity(512),
        }
    }
}

/// Per-event writer returned to `tracing_subscriber`.
pub struct SecureEventWriter {
    destination: SecureRollingMakeWriter,
    pending: Vec<u8>,
}

impl SecureEventWriter {
    fn drain_complete_lines(&mut self) -> io::Result<()> {
        while let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
            let remainder = self.pending.split_off(newline + 1);
            let line = std::mem::replace(&mut self.pending, remainder);
            self.emit(&line)?;
        }
        Ok(())
    }

    fn emit_pending(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let line = std::mem::take(&mut self.pending);
        self.emit(&line)
    }

    fn emit(&self, input: &[u8]) -> io::Result<()> {
        let sanitized = sanitize_json_line(input);
        let mut first_error = None;
        if self.destination.mirror_stderr {
            let mut stderr = io::stderr().lock();
            if let Err(error) = stderr.write_all(&sanitized).and_then(|()| stderr.flush()) {
                first_error = Some(error);
            }
        }
        if let Some(mut file) = self.destination.lock_file()
            && let Err(error) = file.write_record(&sanitized)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Write for SecureEventWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(buffer);
        self.drain_complete_lines()?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit_pending()?;
        if let Some(mut file) = self.destination.lock_file() {
            file.flush()?;
        }
        if self.destination.mirror_stderr {
            io::stderr().flush()?;
        }
        Ok(())
    }
}

impl Drop for SecureEventWriter {
    fn drop(&mut self) {
        if let Err(error) = self.emit_pending() {
            eprintln!("zk-server: structured log write failed: {error}");
        }
    }
}

struct RollingFile {
    path: PathBuf,
    file: Option<File>,
    current_bytes: u64,
    max_bytes: u64,
    max_archives: usize,
}

impl RollingFile {
    fn open(path: PathBuf, max_bytes: u64, max_archives: usize) -> io::Result<Self> {
        if max_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log file size limit must be positive",
            ));
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        prune_excess_archives(&path, max_archives)?;
        let file = open_log_file(&path, false)?;
        let current_bytes = file.metadata()?.len();
        Ok(Self {
            path,
            file: Some(file),
            current_bytes,
            max_bytes,
            max_archives,
        })
    }

    fn write_record(&mut self, input: &[u8]) -> io::Result<()> {
        let bounded = bound_oversized_record(input, self.max_bytes);
        let input_bytes = u64::try_from(bounded.len()).unwrap_or(u64::MAX);
        if self.current_bytes > 0 && self.current_bytes.saturating_add(input_bytes) > self.max_bytes
        {
            self.rotate()?;
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("active log file is unavailable"))?;
        file.write_all(&bounded)?;
        file.flush()?;
        self.current_bytes = self.current_bytes.saturating_add(input_bytes);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.as_mut().map_or(Ok(()), Write::flush)
    }

    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush()?;
        }
        if self.max_archives == 0 {
            self.file = Some(open_log_file(&self.path, true)?);
            self.current_bytes = 0;
            return Ok(());
        }

        let oldest = archive_path(&self.path, self.max_archives);
        remove_if_present(&oldest)?;
        for index in (1..self.max_archives).rev() {
            let source = archive_path(&self.path, index);
            if source.exists() {
                let destination = archive_path(&self.path, index + 1);
                remove_if_present(&destination)?;
                fs::rename(source, destination)?;
            }
        }
        if self.path.exists() {
            let first = archive_path(&self.path, 1);
            remove_if_present(&first)?;
            fs::rename(&self.path, first)?;
        }
        self.file = Some(open_log_file(&self.path, true)?);
        self.current_bytes = 0;
        Ok(())
    }
}

fn archive_path(path: &Path, index: usize) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn prune_excess_archives(path: &Path, max_archives: usize) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let Some(file_name) = path.file_name() else {
        return Ok(());
    };
    let prefix = format!("{}.", file_name.to_string_lossy());
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(suffix) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Ok(index) = suffix.parse::<usize>() else {
            continue;
        };
        if index > max_archives {
            remove_if_present(&entry.path())?;
        }
    }
    Ok(())
}

fn open_log_file(path: &Path, truncate: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).write(true);
    if truncate {
        options.truncate(true);
    } else {
        options.append(true);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let file = options.mode(0o600).open(path)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        options.open(path)
    }
}

fn sanitize_json_line(input: &[u8]) -> Vec<u8> {
    let trimmed = input.strip_suffix(b"\n").unwrap_or(input);
    let trimmed = trimmed.strip_suffix(b"\r").unwrap_or(trimmed);
    let Ok(mut value) = serde_json::from_slice::<Value>(trimmed) else {
        return rejected_record("nonJsonRecord", input);
    };
    redact_value(&mut value);
    let mut output = serde_json::to_vec(&value)
        .unwrap_or_else(|_| rejected_record("serializationFailed", input));
    if !output.ends_with(b"\n") {
        output.push(b'\n');
    }
    output
}

fn redact_value(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if is_sensitive_key(key) {
                    *value = Value::String(REDACTED.to_owned());
                } else {
                    redact_value(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(redact_value),
        Value::String(text) if contains_inline_secret(text) => REDACTED.clone_into(text),
        _ => {}
    }
}

fn contains_inline_secret(text: &str) -> bool {
    const MARKERS: [&str; 9] = [
        "authorization: bearer ",
        "authorization=bearer ",
        "api_key=",
        "apikey=",
        "access_token=",
        "refresh_token=",
        "token=",
        "password=",
        "secret=",
    ];
    let lowercase = text.to_ascii_lowercase();
    MARKERS.iter().any(|marker| lowercase.contains(marker))
        || lowercase
            .split(|character: char| {
                !(character.is_ascii_alphanumeric() || character == '-' || character == '_')
            })
            .any(|word| word.starts_with("sk-") && word.len() > 8)
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    matches!(
        normalized.as_str(),
        "prompt"
            | "systemprompt"
            | "appendsystemprompt"
            | "body"
            | "requestbody"
            | "responsebody"
            | "content"
            | "messagecontent"
            | "apikey"
            | "token"
            | "accesstoken"
            | "refreshtoken"
            | "authorization"
            | "cookie"
            | "secret"
            | "password"
            | "credential"
            | "credentials"
            | "base64data"
    )
}

fn bound_oversized_record(input: &[u8], max_bytes: u64) -> Vec<u8> {
    let input_bytes = u64::try_from(input.len()).unwrap_or(u64::MAX);
    if input_bytes <= max_bytes {
        return input.to_vec();
    }
    let mut notice = rejected_record("recordExceedsFileLimit", input);
    if u64::try_from(notice.len()).unwrap_or(u64::MAX) <= max_bytes {
        return notice;
    }
    notice.clear();
    if max_bytes >= 3 {
        notice.extend_from_slice(b"{}\n");
    } else {
        notice.resize(usize::try_from(max_bytes).unwrap_or(0), b'\n');
    }
    notice
}

fn rejected_record(reason: &str, input: &[u8]) -> Vec<u8> {
    let digest = Sha256::digest(input);
    let record = serde_json::json!({
        "event": "logRecordRejected",
        "reason": reason,
        "originalBytes": input.len(),
        "sha256": format!("{digest:x}"),
    });
    let mut output = serde_json::to_vec(&record).unwrap_or_else(|_| b"{}".to_vec());
    output.push(b'\n');
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("zk-log-{name}-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&path).expect("test log directory");
            Self(path)
        }

        fn log(&self) -> PathBuf {
            self.0.join("server.jsonl")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_event(writer: &SecureRollingMakeWriter, value: &Value) {
        let mut event = writer.make_writer();
        writeln!(event, "{value}").expect("write event");
    }

    #[test]
    fn exact_boundary_does_not_rotate_until_the_next_record() {
        let directory = TestDir::new("boundary");
        let path = directory.log();
        let mut file = RollingFile::open(path.clone(), 10, 2).expect("rolling file");
        file.write_record(b"12345").expect("first record");
        file.write_record(b"67890").expect("exact boundary");
        assert!(!archive_path(&path, 1).exists());
        file.write_record(b"x").expect("rotated record");
        assert_eq!(fs::read(&path).expect("active"), b"x");
        assert_eq!(
            fs::read(archive_path(&path, 1)).expect("archive"),
            b"1234567890"
        );
    }

    #[test]
    fn archive_count_never_exceeds_configured_limit() {
        let directory = TestDir::new("archives");
        let path = directory.log();
        fs::write(archive_path(&path, 4), b"stale archive").expect("stale archive");
        let mut file = RollingFile::open(path.clone(), 8, 3).expect("rolling file");
        assert!(!archive_path(&path, 4).exists());
        for index in 0..9 {
            file.write_record(format!("entry{index}").as_bytes())
                .expect("record");
        }
        assert!(path.exists());
        for index in 1..=3 {
            assert!(archive_path(&path, index).exists(), "archive {index}");
        }
        assert!(!archive_path(&path, 4).exists());
    }

    #[test]
    fn reopening_uses_existing_size_and_rotates_on_the_next_write() {
        let directory = TestDir::new("restart");
        let path = directory.log();
        {
            let mut file = RollingFile::open(path.clone(), 10, 2).expect("first process");
            file.write_record(b"1234567890").expect("fill active");
        }
        {
            let mut file = RollingFile::open(path.clone(), 10, 2).expect("restarted process");
            file.write_record(b"x").expect("post-restart record");
        }
        assert_eq!(fs::read(&path).expect("active"), b"x");
        assert_eq!(
            fs::read(archive_path(&path, 1)).expect("archive"),
            b"1234567890"
        );
    }

    #[test]
    fn structured_sensitive_fields_are_recursively_redacted() {
        let directory = TestDir::new("redaction");
        let path = directory.log();
        let writer = SecureRollingMakeWriter::new(LogWriterConfig::file_only_for_test(
            path.clone(),
            1024 * 1024,
            2,
        ))
        .expect("writer");
        write_event(
            &writer,
            &serde_json::json!({
                "level": "INFO",
                "prompt": "do not persist",
                "fields": {
                    "body": "request body",
                    "content": "assistant response",
                    "apiKey": "sk-secret",
                    "token": "bearer-secret",
                    "input_tokens": 42,
                    "promptHash": "safe-digest"
                }
            }),
        );
        drop(writer);

        let line = fs::read_to_string(path).expect("log line");
        assert!(!line.contains("do not persist"));
        assert!(!line.contains("request body"));
        assert!(!line.contains("assistant response"));
        assert!(!line.contains("sk-secret"));
        assert!(!line.contains("bearer-secret"));
        let value: Value = serde_json::from_str(line.trim()).expect("sanitized json");
        assert_eq!(value["prompt"], REDACTED);
        assert_eq!(value["fields"]["body"], REDACTED);
        assert_eq!(value["fields"]["content"], REDACTED);
        assert_eq!(value["fields"]["apiKey"], REDACTED);
        assert_eq!(value["fields"]["token"], REDACTED);
        assert_eq!(value["fields"]["input_tokens"], 42);
        assert_eq!(value["fields"]["promptHash"], "safe-digest");
    }

    #[test]
    fn malformed_input_is_rejected_without_echoing_the_original() {
        let line = sanitize_json_line(b"not-json apiKey=secret\n");
        let text = String::from_utf8(line).expect("utf8");
        assert!(text.contains("logRecordRejected"));
        assert!(!text.contains("apiKey=secret"));
    }

    #[test]
    fn tracing_json_is_sanitized_before_reaching_the_file() {
        let directory = TestDir::new("tracing-redaction");
        let path = directory.log();
        let writer = SecureRollingMakeWriter::new(LogWriterConfig::file_only_for_test(
            path.clone(),
            1024 * 1024,
            2,
        ))
        .expect("writer");
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_writer(writer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                prompt = "never-store-this",
                message_content = "nor-this-body",
                safe_label = "Authorization: Bearer raw-secret",
                input_tokens = 42_u64,
                "safe event"
            );
        });
        drop(writer);

        let line = fs::read_to_string(path).expect("tracing output");
        assert!(!line.contains("never-store-this"));
        assert!(!line.contains("nor-this-body"));
        assert!(!line.contains("raw-secret"));
        let value: Value = serde_json::from_str(line.trim()).expect("sanitized tracing json");
        assert_eq!(value["fields"]["prompt"], REDACTED);
        assert_eq!(value["fields"]["message_content"], REDACTED);
        assert_eq!(value["fields"]["safe_label"], REDACTED);
        assert_eq!(value["fields"]["input_tokens"], 42);
        assert_eq!(value["fields"]["message"], "safe event");
    }

    #[test]
    fn disabled_configuration_has_no_destinations() {
        let writer = SecureRollingMakeWriter::new(LogWriterConfig::disabled()).expect("disabled");
        assert!(writer.file.is_none());
        assert!(!writer.mirror_stderr);
        write_event(&writer, &serde_json::json!({"event": "discarded"}));
    }
}
