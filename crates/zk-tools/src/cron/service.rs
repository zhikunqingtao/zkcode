//! Cron tool contracts and schedule helpers.
//!
//! `zk-tools` intentionally owns no job state. The server composition root
//! supplies one [`CronTaskPort`] backed by `SQLite`, which keeps this crate below
//! `zk-db` in the dependency graph and prevents a second JSON/in-memory
//! authority from appearing.
#![allow(missing_docs)]

use std::str::FromStr as _;

use chrono::{DateTime, SecondsFormat, Utc};
use chrono_tz::Tz;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

/// Maximum non-deleted jobs owned by one root Session.
pub const MAX_JOBS: usize = 50;
/// Default IANA timezone when the caller does not supply one.
pub const DEFAULT_TIMEZONE: &str = "UTC";
/// V1 overlap behavior. Later policies require their own safety gate.
pub const DEFAULT_OVERLAP_POLICY: &str = "skip";
/// V1 downtime behavior. Missed executions are recorded, never replayed.
pub const DEFAULT_MISSED_POLICY: &str = "skip";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronCreateRequest {
    pub owner_session_id: String,
    pub cron_expression: String,
    pub timezone: String,
    pub prompt: String,
    pub recurring: bool,
    pub overlap_policy: String,
    pub missed_policy: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronTask {
    pub job_id: String,
    pub cron: String,
    pub timezone: String,
    pub prompt: String,
    pub recurring: bool,
    pub overlap_policy: String,
    pub missed_policy: String,
    pub status: String,
    pub next_scheduled_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronDeleteReceipt {
    pub task: CronTask,
    pub remaining: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct CronPortError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl CronPortError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }
}

/// Reverse dependency port implemented by the server-owned `SQLite` service.
pub trait CronTaskPort: Send + Sync {
    fn create(&self, request: CronCreateRequest) -> BoxFuture<'_, Result<CronTask, CronPortError>>;

    fn list(&self, owner_session_id: String)
    -> BoxFuture<'_, Result<Vec<CronTask>, CronPortError>>;

    fn delete(
        &self,
        owner_session_id: String,
        job_id: String,
    ) -> BoxFuture<'_, Result<Option<CronDeleteReceipt>, CronPortError>>;
}

/// Parse the public 5-field Unix form, while accepting the cron crate's 6/7
/// field extended form. Five fields receive a zero-second prefix.
///
/// # Errors
///
/// Returns an error when the field count or cron expression is invalid.
pub fn parse_schedule(expression: &str) -> Result<::cron::Schedule, String> {
    let trimmed = expression.trim();
    let fields = trimmed.split_whitespace().count();
    let normalized = match fields {
        5 => format!("0 {trimmed}"),
        6 | 7 => trimmed.to_owned(),
        other => {
            return Err(format!(
                "expected a 5-field Unix cron expression (minute hour day-of-month month day-of-week), got {other} field(s)"
            ));
        }
    };
    ::cron::Schedule::from_str(&normalized).map_err(|error| error.to_string())
}

/// Validate and canonicalize an IANA timezone name.
///
/// # Errors
///
/// Returns an error when the timezone is empty or is not a known IANA name.
pub fn parse_timezone(value: &str) -> Result<Tz, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("timezone must not be empty".to_owned());
    }
    Tz::from_str(trimmed).map_err(|_| format!("unknown IANA timezone: {trimmed}"))
}

/// Compute the next occurrence strictly after `after_ms` in the job timezone.
///
/// # Errors
///
/// Returns an error for invalid schedules, timezones, timestamps, or schedules
/// without a future occurrence.
pub fn next_run_after_ms(expression: &str, timezone: &str, after_ms: i64) -> Result<i64, String> {
    let schedule = parse_schedule(expression)?;
    let timezone = parse_timezone(timezone)?;
    let after = DateTime::<Utc>::from_timestamp_millis(after_ms)
        .ok_or_else(|| "schedule reference time is outside the supported range".to_owned())?
        .with_timezone(&timezone);
    schedule
        .after(&after)
        .next()
        .map(|next| next.timestamp_millis())
        .ok_or_else(|| "cron expression has no future occurrence".to_owned())
}

/// RFC-3339 UTC representation used by lower-camel tool responses.
#[must_use]
pub fn format_timestamp_ms(timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms).map_or_else(
        || timestamp_ms.to_string(),
        |value| value.to_rfc3339_opts(SecondsFormat::Millis, true),
    )
}

/// Character-boundary clipping for prompt previews.
#[must_use]
pub fn clip(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    let head: String = text.chars().take(max_chars).collect();
    format!("{head}...")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_cron_and_iana_timezone_are_validated() {
        assert!(parse_schedule("*/5 * * * *").is_ok());
        assert!(parse_schedule("0 30 9 1 5 Mon").is_ok());
        assert!(parse_schedule("* *").is_err());
        assert_eq!(parse_timezone("UTC").expect("UTC").to_string(), "UTC");
        assert_eq!(
            parse_timezone("Asia/Shanghai")
                .expect("IANA timezone")
                .to_string(),
            "Asia/Shanghai"
        );
        assert!(parse_timezone("Mars/Olympus").is_err());
    }

    #[test]
    fn next_run_uses_the_job_timezone() {
        // 2026-01-01 00:30 UTC = 08:30 Asia/Shanghai. A local 09:00 job is
        // therefore due at 01:00 UTC, not 09:00 UTC.
        let after = DateTime::parse_from_rfc3339("2026-01-01T00:30:00Z")
            .expect("time")
            .timestamp_millis();
        let next = next_run_after_ms("0 9 * * *", "Asia/Shanghai", after).expect("next");
        assert_eq!(format_timestamp_ms(next), "2026-01-01T01:00:00.000Z");
    }

    #[test]
    fn clip_respects_character_boundaries() {
        assert_eq!(clip("short", 80), "short");
        assert_eq!(clip("中文很长的一段提示", 4), "中文很长...");
    }
}
