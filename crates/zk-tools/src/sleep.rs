//! `Sleep` 工具——暂停执行指定时长（Batch 8E）。
//!
//! 语义来源（旧仓库只读）：`tool/interaction/SleepTool.java`（105L）——
//! `MAX_SLEEP_SECONDS = 300`、非法时长即 `SLEEP_DURATION_INVALID`、
//! 中断返回 `"Sleep interrupted after partial wait."`、正常返回
//! `"Slept for N seconds."`、`isReadOnly` 与 `isConcurrencySafe` 皆 `true`。
//!
//! # 有意差异
//!
//! - 旧入参是 `integer`（1..=300），**越界即拒**；本移植接受 `number`（小数秒）
//!   且 `> 300` 时**钳制到 300 并在结果里带警告**——理由：调用方常给
//!   「等一会儿」的粗略大数，直接拒绝会让模型多耗一轮重试；`<= 0` 与
//!   非数值仍逐字落 `SLEEP_DURATION_INVALID`（与旧一致）。
//! - 旧靠 `Thread.sleep` + `InterruptedException`；本移植以
//!   [`ToolContext::cancel`] 的取消令牌抢占（`tokio::select!`），中断文案逐字
//!   保留。
//!
//! [`ToolContext::cancel`]: crate::tool::ToolContext#structfield.cancel

use std::fmt::Write as _;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::failure;
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 最大休眠秒数（对照旧 `MAX_SLEEP_SECONDS = 300`）。
pub const MAX_SLEEP_SECONDS: f64 = 300.0;

/// 中断文案（对照旧 `"Sleep interrupted after partial wait."`）。
pub const SLEEP_INTERRUPTED: &str = "Sleep interrupted after partial wait.";

/// `Sleep` 工具（名 `Sleep`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct SleepTool;

impl Tool for SleepTool {
    fn name(&self) -> &'static str {
        "Sleep"
    }

    fn description(&self) -> &'static str {
        "Pause execution for a specified number of seconds (1-300). \
         Useful for waiting on external processes or rate limiting."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "required": ["seconds"],
            "properties": {
                "seconds": {
                    "type": "number",
                    "description": "Number of seconds to sleep (1-300)",
                    "minimum": 0,
                    "maximum": MAX_SLEEP_SECONDS
                }
            }
        })
    }

    fn timeout(&self) -> Duration {
        // 休眠上限 + 余量：否则 300s 的合法休眠会被执行器超时截断。
        Duration::from_secs(330)
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(&input, &ctx).await })
    }
}

async fn run(input: &Value, ctx: &ToolContext) -> ToolOutput {
    let Some(requested) = input.get("seconds").and_then(Value::as_f64) else {
        return failure(
            "SLEEP_DURATION_INVALID",
            format!("seconds must be a number between 0 and {MAX_SLEEP_SECONDS}"),
        );
    };
    if !requested.is_finite() || requested <= 0.0 {
        return failure(
            "SLEEP_DURATION_INVALID",
            format!("seconds must be between 0 and {MAX_SLEEP_SECONDS}, got: {requested}"),
        );
    }

    let clamped = requested.min(MAX_SLEEP_SECONDS);
    let interrupted = tokio::select! {
        () = ctx.cancel.cancelled() => true,
        () = tokio::time::sleep(Duration::from_secs_f64(clamped)) => false,
    };

    let mut content = if interrupted {
        SLEEP_INTERRUPTED.to_owned()
    } else {
        format!("Slept for {} seconds.", format_seconds(clamped))
    };
    if clamped < requested {
        let _ = write!(
            content,
            "\n[warning] requested {} s exceeds the {} s cap and was clamped.",
            format_seconds(requested),
            format_seconds(MAX_SLEEP_SECONDS)
        );
    }
    ToolOutput {
        content,
        is_error: false,
        metadata: Some(json!({
            "requestedSeconds": requested,
            "sleptSeconds": clamped,
            "clamped": clamped < requested,
            "interrupted": interrupted,
        })),
    }
}

/// 整秒去掉小数尾（保证 `"Slept for 2 seconds."` 与旧文案逐字一致）。
fn format_seconds(value: f64) -> String {
    if (value.fract()).abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> (ToolContext, CancellationToken) {
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        (ToolContext::new(cancel.clone(), tx), cancel)
    }

    #[tokio::test(start_paused = true)]
    async fn sleeps_and_reports_whole_seconds() {
        let (ctx, _cancel) = ctx();
        let output = SleepTool.execute(json!({ "seconds": 2 }), ctx).await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "Slept for 2 seconds.");
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["clamped"], json!(false));
        assert_eq!(metadata["interrupted"], json!(false));
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_request_is_clamped_with_a_warning() {
        let (ctx, _cancel) = ctx();
        let output = SleepTool.execute(json!({ "seconds": 900 }), ctx).await;
        assert!(!output.is_error);
        assert!(output.content.starts_with("Slept for 300 seconds."));
        assert!(output.content.contains("[warning] requested 900 s exceeds"));
        assert_eq!(output.metadata.expect("metadata")["clamped"], json!(true));
    }

    #[tokio::test]
    async fn cancellation_wakes_the_sleep_early() {
        let (ctx, cancel) = ctx();
        cancel.cancel();
        let output = SleepTool.execute(json!({ "seconds": 300 }), ctx).await;
        assert!(!output.is_error);
        assert_eq!(output.content, SLEEP_INTERRUPTED);
        assert_eq!(
            output.metadata.expect("metadata")["interrupted"],
            json!(true)
        );
    }

    #[tokio::test]
    async fn invalid_durations_are_rejected() {
        for input in [json!({}), json!({ "seconds": 0 }), json!({ "seconds": -5 })] {
            let (ctx, _cancel) = ctx();
            let output = SleepTool.execute(input, ctx).await;
            assert!(output.is_error);
            assert!(output.content.starts_with("SLEEP_DURATION_INVALID: "));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fractional_seconds_are_supported() {
        let (ctx, _cancel) = ctx();
        let output = SleepTool.execute(json!({ "seconds": 0.25 }), ctx).await;
        assert_eq!(output.content, "Slept for 0.25 seconds.");
    }

    #[test]
    fn declares_read_only_and_a_timeout_above_the_cap() {
        assert!(SleepTool.is_read_only(&json!({})));
        assert!(!SleepTool.is_destructive(&json!({})));
        assert!(SleepTool.timeout() > Duration::from_secs_f64(MAX_SLEEP_SECONDS));
    }
}
