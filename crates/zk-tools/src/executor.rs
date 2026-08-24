//! 受控工具执行器——并发上限 / 超时 / 取消 / 输出截断。
//!
//! 对照旧 `ToolExecutionPipeline.java` 语义骨架：每工具调用一
//! `tokio::spawn`；全局 `Semaphore(16)`（对照旧
//! `process.runner.max-concurrent`，`ManagedProcessRunner.java` L43）；
//! 超时默认 120s / 上限 600s（`BashTool.java` L51-54）；输出上限 1 MiB。
//!
//! # 取消语义（对齐 D-S6-5 的静默终止族）
//!
//! 取消（run 令牌 → 本调用 child 令牌）后任务直接退出、**不产出**
//! [`ToolEvent::Finished`]——事件通道随之关闭，消费方以「通道关闭且无
//! Finished」判定中断，由引擎按旧 FIX-02 语义合成
//! `<tool_use_error>Interrupted by user</tool_use_error>` 结果。

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::tool::{MAX_TOOL_TIMEOUT, Tool, ToolContext, ToolOutput};

/// 全局并发上限（对照旧 `process.runner.max-concurrent` 默认 16）。
pub const MAX_CONCURRENT_TOOLS: usize = 16;

/// 单工具输出采集上限（1 MiB；超限截断并追加标记）。
pub const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;

/// 截断标记（追加于截断点之后）。
const TRUNCATION_MARKER: &str = "\n... [output truncated at 1MB]";

/// 工具执行事件（每调用一事件通道；Progress 零到多次 + Finished 恰一次；
/// 取消路径通道直接关闭、无 Finished）。
#[derive(Clone, Debug, PartialEq)]
pub enum ToolEvent {
    /// 执行进度（stdout 增量语义 → 下行 `tool_use_progress`）。
    Progress {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 进度文本。
        text: String,
    },
    /// 执行完成（含超时合成的错误结果）。
    Finished {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 执行结果（输出已按上限截断）。
        output: ToolOutput,
    },
}

/// 单次调用的环境注入（2.3 追加）——工作目录 / 会话 ID。
///
/// 引擎侧按 run 维度构造（`session_id` 恒有值，`working_dir` 缺省时沿用
/// 进程当前目录）；[`ToolExecutor::spawn_call`] 等价于全默认环境，故
/// Phase 1/2.2 既有调用方零改动。`tool_use_id` 不在此列——执行器已持有
/// 该参数，直接注入上下文。
#[derive(Clone, Debug, Default)]
pub struct CallEnv {
    working_dir: Option<PathBuf>,
    session_id: Option<String>,
    run_id: Option<String>,
}

impl CallEnv {
    /// 空环境（等价 [`Default`]）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 指定工作目录（相对路径入参的解析基准）。
    #[must_use]
    pub fn with_working_dir(mut self, working_dir: impl Into<PathBuf>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    /// 指定会话 ID（写前快照等会话维度副作用的归属键）。
    #[must_use]
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// 工作目录字符串视图（2.5 授权链的 `workingDirectory` 事实来源；
    /// 非 UTF-8 路径返回 `None`——授权链据此退回配置默认值）。
    #[must_use]
    pub fn working_dir_str(&self) -> Option<&str> {
        self.working_dir
            .as_deref()
            .and_then(std::path::Path::to_str)
    }

    /// 会话 ID 视图（2.5 授权链的 root session 事实来源）。
    #[must_use]
    pub fn session_id_str(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// 指定 Run ID（持久交互的归属 Run；见
    /// [`ToolContext::with_run_id`]）。
    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Run ID 视图。
    #[must_use]
    pub fn run_id_str(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// 施加到上下文（缺省项保持 [`ToolContext::new`] 的默认值）。
    fn apply(self, mut ctx: ToolContext) -> ToolContext {
        if let Some(working_dir) = self.working_dir {
            ctx = ctx.with_working_dir(working_dir);
        }
        if let Some(session_id) = self.session_id {
            ctx = ctx.with_session_id(session_id);
        }
        if let Some(run_id) = self.run_id {
            ctx = ctx.with_run_id(run_id);
        }
        ctx
    }
}

/// 工具参数安全守卫端口（旧 `service/ToolSafetyGuard.java`）。
///
/// 与授权系统**正交**：权限系统回答「用户允不允许」，本守卫回答「调用**参数
/// 本身**是否安全」。权威策略实现位于 zk-authz `tool_safety`（含 scratchpad
/// 写入边界），由 zk-server 组合根接线——依赖方向铁律禁止 `zk-tools → zk-authz`，
/// 故此处以 trait 反转（范式同 zk-authz `tool_facts` 的 `ToolFacts`）。
///
/// 注意：旧类的**环境安全层**不走本端口。子进程敏感环境变量清理在
/// [`crate::process`] 的 spawn 处**无条件**执行——旧源该守卫全仓零调用点，
/// 若把它做成可选接线就会留后门。
pub trait ToolSafetyGuard: Send + Sync {
    /// 检查一次工具调用的参数安全性；拒绝时返回可展示的原因文案。
    ///
    /// # Errors
    ///
    /// 参数越界（例如写入目标越出 scratchpad 边界、路径命中敏感黑名单）时
    /// 返回拒绝原因，执行器据此直接产出 `is_error` 结果、不调用工具。
    fn check_tool_call(
        &self,
        tool: &dyn Tool,
        input: &serde_json::Value,
        env: &CallEnv,
    ) -> Result<(), String>;
}

/// 受控执行器（可克隆共享；全部调用共享同一全局许可池）。
#[derive(Clone)]
pub struct ToolExecutor {
    semaphore: Arc<Semaphore>,
    /// 参数安全守卫（未接线时为 `None`——旧源默认形态，见
    /// [`ToolSafetyGuard`] 关于环境安全层不依赖接线的说明）。
    safety_guard: Option<Arc<dyn ToolSafetyGuard>>,
}

impl Default for ToolExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolExecutor {
    /// 以默认并发上限（[`MAX_CONCURRENT_TOOLS`]）构造。
    #[must_use]
    pub fn new() -> Self {
        Self::with_concurrency(MAX_CONCURRENT_TOOLS)
    }

    /// 以指定并发上限构造（测试用）。
    #[must_use]
    pub fn with_concurrency(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            safety_guard: None,
        }
    }

    /// 接入参数安全守卫（zk-server 组合根注入 zk-authz 实现）。
    #[must_use]
    pub fn with_safety_guard(mut self, guard: Arc<dyn ToolSafetyGuard>) -> Self {
        self.safety_guard = Some(guard);
        self
    }

    /// 派发一次工具调用（每调用一 `tokio::spawn`），返回事件接收端。
    ///
    /// `parent_cancel` 为 run 层令牌；内部派生 `tool_call` 层 child 令牌
    /// （三层树第三层），排队 / 执行期间取消均即时退出（见模块文档取消语义）。
    #[must_use]
    pub fn spawn_call(
        &self,
        tool: Arc<dyn Tool>,
        tool_use_id: String,
        input: serde_json::Value,
        parent_cancel: &CancellationToken,
    ) -> mpsc::UnboundedReceiver<ToolEvent> {
        self.spawn_call_in(tool, tool_use_id, input, parent_cancel, CallEnv::new())
    }

    /// 派发一次工具调用并注入调用环境（2.3 追加；语义同
    /// [`Self::spawn_call`]，额外把 [`CallEnv`] 与 `tool_use_id` 落入
    /// [`ToolContext`]）。
    #[must_use]
    pub fn spawn_call_in(
        &self,
        tool: Arc<dyn Tool>,
        tool_use_id: String,
        input: serde_json::Value,
        parent_cancel: &CancellationToken,
        env: CallEnv,
    ) -> mpsc::UnboundedReceiver<ToolEvent> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let cancel = parent_cancel.child_token();
        let semaphore = Arc::clone(&self.semaphore);
        let safety_guard = self.safety_guard.clone();
        tokio::spawn(async move {
            // 并发上限：许可获取本身可取消（排队期间中断即退出，无 Finished）。
            let permit = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                permit = semaphore.acquire_owned() => match permit {
                    Ok(permit) => permit,
                    // Semaphore 关闭（进程关停路径），静默退出。
                    Err(_) => return,
                },
            };
            // 参数安全守卫（旧 `ToolSafetyGuard`）：先于工具执行判定「参数
            // 本身是否安全」，拒绝则直接产出 is_error 结果、不进 execute。
            if let Some(guard) = safety_guard.as_ref()
                && let Err(reason) = guard.check_tool_call(tool.as_ref(), &input, &env)
            {
                tracing::warn!(tool = tool.name(), %reason, "tool call denied by safety guard");
                drop(permit);
                let _ = event_tx.send(ToolEvent::Finished {
                    tool_use_id,
                    output: ToolOutput::error(reason),
                });
                return;
            }
            let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
            let ctx = env.apply(
                ToolContext::new(cancel.clone(), progress_tx).with_tool_use_id(tool_use_id.clone()),
            );
            let timeout = tool.timeout().min(MAX_TOOL_TIMEOUT);
            let mut work = tool.execute(input, ctx);
            let deadline = tokio::time::sleep(timeout);
            tokio::pin!(deadline);
            let mut progress_open = true;
            let output = loop {
                tokio::select! {
                    biased;
                    // 取消优先：不产出 Finished（通道关闭即中断信号）。
                    () = cancel.cancelled() => {
                        drop(permit);
                        return;
                    }
                    () = &mut deadline => {
                        break ToolOutput::error(format!(
                            "Tool execution timed out after {}ms",
                            timeout.as_millis()
                        ));
                    }
                    progress = progress_rx.recv(), if progress_open => match progress {
                        Some(text) => {
                            let _ = event_tx.send(ToolEvent::Progress {
                                tool_use_id: tool_use_id.clone(),
                                text,
                            });
                        }
                        None => progress_open = false,
                    },
                    output = &mut work => break output,
                }
            };
            drop(permit);
            // 排干残余进度（保证 Progress 先于 Finished 的事件序）。
            while let Ok(text) = progress_rx.try_recv() {
                let _ = event_tx.send(ToolEvent::Progress {
                    tool_use_id: tool_use_id.clone(),
                    text,
                });
            }
            let _ = event_tx.send(ToolEvent::Finished {
                tool_use_id,
                output: truncate_output(output),
            });
        });
        event_rx
    }
}

/// 输出截断（按 char 边界回退，避免切碎多字节 UTF-8）。
fn truncate_output(mut output: ToolOutput) -> ToolOutput {
    if output.content.len() <= MAX_TOOL_OUTPUT_BYTES {
        return output;
    }
    let mut cut = MAX_TOOL_OUTPUT_BYTES;
    while !output.content.is_char_boundary(cut) {
        cut -= 1;
    }
    output.content.truncate(cut);
    output.content.push_str(TRUNCATION_MARKER);
    output
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use serde_json::json;

    use super::*;

    /// 并发追踪桩：进入时 current+1 并刷新 max，短暂驻留后退出。
    struct GateTool {
        current: Arc<AtomicUsize>,
        max_seen: Arc<AtomicUsize>,
    }

    impl Tool for GateTool {
        fn name(&self) -> &'static str {
            "Gate"
        }

        fn description(&self) -> &'static str {
            "concurrency probe"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let current = Arc::clone(&self.current);
            let max_seen = Arc::clone(&self.max_seen);
            Box::pin(async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                ToolOutput::ok("done")
            })
        }
    }

    /// 永不完成桩（短自定义超时，供超时/取消测试）。
    struct HangTool {
        timeout: Duration,
    }

    impl Tool for HangTool {
        fn name(&self) -> &'static str {
            "Hang"
        }

        fn description(&self) -> &'static str {
            "never completes"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn timeout(&self) -> Duration {
            self.timeout
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            Box::pin(std::future::pending())
        }
    }

    /// 进度桩：两条进度后成功返回。
    struct ProgressTool;

    impl Tool for ProgressTool {
        fn name(&self) -> &'static str {
            "Progress"
        }

        fn description(&self) -> &'static str {
            "emits progress"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            Box::pin(async move {
                ctx.report_progress("step 1");
                ctx.report_progress("step 2");
                ToolOutput::ok("finished")
            })
        }
    }

    /// 执行计数桩：验证守卫拒绝时工具**未被调用**。
    struct CountingTool {
        calls: Arc<AtomicUsize>,
    }

    impl Tool for CountingTool {
        fn name(&self) -> &'static str {
            "Counting"
        }

        fn description(&self) -> &'static str {
            "counts executions"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                ToolOutput::ok("executed")
            })
        }
    }

    /// 守卫桩：记录被检查的（工具名, 会话 ID），按 `deny` 决定放行/拒绝。
    struct RecordingGuard {
        deny: bool,
        seen: std::sync::Mutex<Vec<(String, Option<String>)>>,
    }

    impl ToolSafetyGuard for RecordingGuard {
        fn check_tool_call(
            &self,
            tool: &dyn Tool,
            _input: &serde_json::Value,
            env: &CallEnv,
        ) -> Result<(), String> {
            self.seen.lock().expect("guard lock").push((
                tool.name().to_owned(),
                env.session_id_str().map(str::to_owned),
            ));
            if self.deny {
                Err(
                    "scratchpad boundary violation: /etc/passwd is outside the scratchpad root"
                        .to_owned(),
                )
            } else {
                Ok(())
            }
        }
    }

    /// 守卫拒绝 → 直接产出 `is_error` 结果，且工具 execute 从未被调用。
    #[tokio::test]
    async fn safety_guard_denial_short_circuits_execution() {
        let guard = Arc::new(RecordingGuard {
            deny: true,
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let executor = ToolExecutor::with_concurrency(1)
            .with_safety_guard(Arc::clone(&guard) as Arc<dyn ToolSafetyGuard>);
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(CountingTool {
            calls: Arc::clone(&calls),
        });
        let events = collect(executor.spawn_call_in(
            tool,
            "call-1".to_owned(),
            json!({ "file_path": "/etc/passwd" }),
            &CancellationToken::new(),
            CallEnv::new().with_session_id("s-1"),
        ))
        .await;

        assert_eq!(events.len(), 1);
        match &events[0] {
            ToolEvent::Finished {
                tool_use_id,
                output,
            } => {
                assert_eq!(tool_use_id, "call-1");
                assert!(output.is_error);
                assert!(output.content.contains("scratchpad boundary violation"));
            }
            other @ ToolEvent::Progress { .. } => panic!("unexpected event: {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0, "tool must not execute");
        let seen = guard.seen.lock().expect("guard lock");
        assert_eq!(
            seen.as_slice(),
            [("Counting".to_owned(), Some("s-1".to_owned()))]
        );
    }

    /// 守卫放行 → 正常执行；未接线守卫时行为与放行一致（既有测试覆盖）。
    #[tokio::test]
    async fn safety_guard_allow_lets_execution_through() {
        let guard = Arc::new(RecordingGuard {
            deny: false,
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let executor = ToolExecutor::with_concurrency(1)
            .with_safety_guard(Arc::clone(&guard) as Arc<dyn ToolSafetyGuard>);
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(CountingTool {
            calls: Arc::clone(&calls),
        });
        let events = collect(executor.spawn_call(
            tool,
            "call-2".to_owned(),
            json!({}),
            &CancellationToken::new(),
        ))
        .await;

        assert_eq!(events.len(), 1);
        match &events[0] {
            ToolEvent::Finished { output, .. } => {
                assert!(!output.is_error);
                assert_eq!(output.content, "executed");
            }
            other @ ToolEvent::Progress { .. } => panic!("unexpected event: {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(guard.seen.lock().expect("guard lock").len(), 1);
    }

    async fn collect(mut rx: mpsc::UnboundedReceiver<ToolEvent>) -> Vec<ToolEvent> {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn concurrency_never_exceeds_limit() {
        let executor = ToolExecutor::with_concurrency(4);
        let current = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let receivers: Vec<_> = (0..16)
            .map(|i| {
                let tool: Arc<dyn Tool> = Arc::new(GateTool {
                    current: Arc::clone(&current),
                    max_seen: Arc::clone(&max_seen),
                });
                executor.spawn_call(tool, format!("call-{i}"), json!({}), &cancel)
            })
            .collect();
        for rx in receivers {
            let events = collect(rx).await;
            assert!(matches!(
                events.last(),
                Some(ToolEvent::Finished { output, .. }) if !output.is_error
            ));
        }
        assert!(
            max_seen.load(Ordering::SeqCst) <= 4,
            "observed concurrency {} exceeds limit 4",
            max_seen.load(Ordering::SeqCst)
        );
        assert_eq!(current.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn timeout_yields_error_finished() {
        let executor = ToolExecutor::new();
        let cancel = CancellationToken::new();
        let tool: Arc<dyn Tool> = Arc::new(HangTool {
            timeout: Duration::from_millis(30),
        });
        let events = collect(executor.spawn_call(tool, "t1".into(), json!({}), &cancel)).await;
        assert_eq!(events.len(), 1);
        let ToolEvent::Finished {
            tool_use_id,
            output,
        } = &events[0]
        else {
            panic!("expected Finished, got {events:?}");
        };
        assert_eq!(tool_use_id, "t1");
        assert!(output.is_error);
        assert!(output.content.contains("timed out after 30ms"));
    }

    #[tokio::test]
    async fn cancel_closes_channel_without_finished() {
        let executor = ToolExecutor::new();
        let cancel = CancellationToken::new();
        let tool: Arc<dyn Tool> = Arc::new(HangTool {
            timeout: Duration::from_mins(1),
        });
        let rx = executor.spawn_call(tool, "t2".into(), json!({}), &cancel);
        // 执行已开始后再取消（parent → child 树传播）。
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        let events = collect(rx).await;
        assert!(
            events.is_empty(),
            "cancelled call must not emit events: {events:?}"
        );
    }

    #[tokio::test]
    async fn progress_events_precede_finished() {
        let executor = ToolExecutor::new();
        let cancel = CancellationToken::new();
        let events =
            collect(executor.spawn_call(Arc::new(ProgressTool), "t3".into(), json!({}), &cancel))
                .await;
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0],
            ToolEvent::Progress {
                tool_use_id: "t3".into(),
                text: "step 1".into()
            }
        );
        assert_eq!(
            events[1],
            ToolEvent::Progress {
                tool_use_id: "t3".into(),
                text: "step 2".into()
            }
        );
        assert!(
            matches!(&events[2], ToolEvent::Finished { output, .. } if output.content == "finished")
        );
    }

    #[test]
    fn truncate_output_respects_char_boundary() {
        // 未超限原样返回。
        let small = truncate_output(ToolOutput::ok("short"));
        assert_eq!(small.content, "short");
        // 超限：多字节字符跨界时回退到 char 边界再追加标记。
        let mut content = "x".repeat(MAX_TOOL_OUTPUT_BYTES - 1);
        content.push('你'); // 3 字节，跨越 1MiB 边界
        content.push_str("tail");
        let truncated = truncate_output(ToolOutput::ok(content));
        assert!(truncated.content.ends_with(TRUNCATION_MARKER));
        let kept = &truncated.content[..truncated.content.len() - TRUNCATION_MARKER.len()];
        assert_eq!(kept.len(), MAX_TOOL_OUTPUT_BYTES - 1); // '你' 整字符被回退丢弃
        assert!(kept.chars().all(|c| c == 'x'));
    }
}
