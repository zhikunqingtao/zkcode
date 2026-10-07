//! Pure diagnostics: suggestions never execute commands or authorize retries.
use super::super::process::ProcessOutcome;

pub(super) struct Recovery {
    pub category: &'static str,
    pub code: &'static str,
    pub suggestion: &'static str,
}

pub(super) fn classify(outcome: &ProcessOutcome) -> Recovery {
    let text = format!("{}\n{}", outcome.stderr, outcome.stdout).to_ascii_lowercase();
    let (category, code, suggestion) = if !outcome.termination_confirmed {
        (
            "NEEDS_HUMAN",
            "PROCESS_TERMINATION_UNCONFIRMED",
            "Inspect the owned process and actual side effects before any retry; termination has not been confirmed.",
        )
    } else if outcome.cancelled {
        (
            "CANCELLED",
            "BASH_CANCELLED",
            "The command was cancelled. Inspect partial effects before explicitly resuming.",
        )
    } else if outcome.timed_out {
        (
            "TIMEOUT",
            "BASH_TIMEOUT",
            "Inspect partial effects, then adjust the command or explicitly choose a longer timeout within the task deadline.",
        )
    } else if outcome.exit_code == 127 || text.contains("command not found") {
        (
            "NON_RETRYABLE",
            "BASH_COMMAND_NOT_FOUND",
            "Verify the executable name and the configured PATH; install or select an available executable explicitly.",
        )
    } else if text.contains("no space left on device") {
        (
            "NEEDS_HUMAN",
            "BASH_DISK_FULL",
            "Free space safely before another attempt; inspect partially written files first.",
        )
    } else if outcome.exit_code == 126
        || text.contains("permission denied")
        || text.contains("operation not permitted")
    {
        (
            "NEEDS_HUMAN",
            "BASH_PERMISSION_DENIED",
            "Check the authorized scope, file ownership and executable permissions; do not bypass access controls.",
        )
    } else if [
        "connection refused",
        "connection timed out",
        "econnreset",
        "econnrefused",
        "network is unreachable",
        "temporary failure in name resolution",
    ]
    .iter()
    .any(|needle| text.contains(needle))
    {
        (
            "RETRYABLE",
            "BASH_NETWORK_ERROR",
            "Check network availability and whether the previous request already had effects; retry only through a new explicit invocation.",
        )
    } else if [
        "resource temporarily unavailable",
        "lock file",
        "could not get lock",
    ]
    .iter()
    .any(|needle| text.contains(needle))
    {
        (
            "RETRYABLE",
            "BASH_RESOURCE_LOCKED",
            "Inspect the lock owner and command effects before a new invocation; do not delete locks or loop automatically.",
        )
    } else if text.contains("error[e") || text.contains(": error:") || text.contains(": error ts") {
        (
            "NON_RETRYABLE",
            "BASH_COMPILATION_ERROR",
            "Correct the reported source errors before another build.",
        )
    } else if matches!(outcome.exit_code, 137 | 143) {
        // A signal alone is not proof that our deadline expired.
        (
            "NON_RETRYABLE",
            "BASH_SIGNAL_TERMINATED",
            "The process was terminated by a signal. Inspect its owner and partial effects before another invocation.",
        )
    } else {
        (
            "NON_RETRYABLE",
            "BASH_COMMAND_FAILED",
            "Review the actual output and side effects before changing the command or trying again.",
        )
    };
    Recovery {
        category,
        code,
        suggestion,
    }
}
