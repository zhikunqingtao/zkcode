//! 2.3 基础工具族集成测试——经 `ToolExecutor` 走完整调用链（许可 / 取消
//! 树 / 超时 / 1 MiB 截断 / `ToolEvent` 通道），而非直调 `Tool::execute`。
//!
//! 覆盖判据：`Bash` 简单命令 / 超时→kill→错误结果 / 大输出截断；文件族
//! 写-读-搜索链路（`Write` → `Read` → `Grep` → `Glob` → `ListDir`）。
//!
//! 2.4 追加：`Edit`（读前置门禁 → 唯一匹配替换 → 原子落盘）、
//! `TodoWrite`（`.zk/todos.md` 镜像）、`AskUserQuestion`（经端口阻塞发问）、
//! `Config` 与 `SyntheticOutput`（均经执行器走完整调用链）。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use zk_tools::{
    AskUserQuestionTool, BashTool, CallEnv, ConfigTool, EditFileTool, ElicitationOutcome,
    ElicitationRequest, ElicitationSink, GlobTool, GrepTool, ListDirectoryTool, ReadFileTool,
    SyntheticOutputTool, TodoWriteTool, Tool, ToolEvent, ToolExecutor, ToolOutput, WriteFileTool,
};

/// 独占测试目录（同名冲突下先清空重建）。
fn workspace(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("zk-tool-family-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("mkdir workspace");
    root
}

/// 经执行器跑一次调用，收敛为最终 `ToolOutput`（丢弃 progress 事件）。
async fn call(tool: Arc<dyn Tool>, input: serde_json::Value, working_dir: &Path) -> ToolOutput {
    call_as(tool, input, working_dir, "session-it").await
}

/// 同 [`call`]，但指定会话 ID（`Edit` 的读前置台账按会话分桶，用以隔离
/// 同一测试二进程内共享进程级单例的各用例）。
async fn call_as(
    tool: Arc<dyn Tool>,
    input: serde_json::Value,
    working_dir: &Path,
    session_id: &str,
) -> ToolOutput {
    let executor = ToolExecutor::new();
    let cancel = CancellationToken::new();
    let mut rx = executor.spawn_call_in(
        tool,
        "toolu_it".to_owned(),
        input,
        &cancel,
        CallEnv::new()
            .with_session_id(session_id)
            .with_run_id("run-it")
            .with_working_dir(working_dir),
    );
    while let Some(event) = rx.recv().await {
        if let ToolEvent::Finished { output, .. } = event {
            return output;
        }
    }
    panic!("executor closed without Finished");
}

/// `Bash` 简单命令：退出 0、stdout 原样返回、结构化元数据带 `exitCode`。
#[tokio::test]
async fn bash_runs_a_simple_command() {
    let root = workspace("bash-ok");
    let output = call(
        Arc::new(BashTool),
        json!({ "command": "echo hello-zkcode" }),
        &root,
    )
    .await;
    assert!(!output.is_error, "unexpected error: {}", output.content);
    assert!(
        output.content.contains("hello-zkcode"),
        "content: {}",
        output.content
    );
    let meta = output.metadata.expect("metadata");
    assert_eq!(meta["structuredResult"]["exitCode"], 0);
    assert_eq!(meta["structuredResult"]["timedOut"], false);
    let _ = std::fs::remove_dir_all(&root);
}

/// `Bash` 超时：per-call `timeout`（毫秒，旧入参名）到点即杀进程树，
/// 且实际耗时远小于命令自身的 30s（证明是被 kill 而非等其自然结束）。
#[tokio::test]
async fn bash_timeout_kills_and_reports_137() {
    let root = workspace("bash-timeout");
    let started = Instant::now();
    let output = call(
        Arc::new(BashTool),
        json!({ "command": "sleep 30", "timeout": 400 }),
        &root,
    )
    .await;
    let elapsed = started.elapsed();
    assert!(output.is_error, "timeout must surface as error result");
    assert!(
        output.content.contains("Command timed out after 400 ms"),
        "content: {}",
        output.content
    );
    assert!(
        output.content.contains("Exit code: 137"),
        "content: {}",
        output.content
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "should be killed early, took {elapsed:?}"
    );
    let meta = output.metadata.expect("metadata");
    assert_eq!(meta["structuredResult"]["timedOut"], true);
    let _ = std::fs::remove_dir_all(&root);
}

/// `Bash` 大输出：30 000 字符预览上限 + `[Output truncated]` 尾注。
#[tokio::test]
async fn bash_truncates_large_output() {
    let root = workspace("bash-truncate");
    let output = call(
        Arc::new(BashTool),
        json!({
            "command": "line=$(printf 'y%.0s' $(seq 1 80)); for i in $(seq 1 2000); do echo \"$line\"; done"
        }),
        &root,
    )
    .await;
    assert!(!output.is_error, "content: {}", output.content);
    assert!(
        output.content.ends_with("[Output truncated]"),
        "tail: {:?}",
        &output.content[output.content.len().saturating_sub(40)..]
    );
    assert!(
        output.content.chars().count() <= 30_000 + "\n[Output truncated]".chars().count(),
        "chars: {}",
        output.content.chars().count()
    );
    let meta = output.metadata.expect("metadata");
    assert_eq!(meta["structuredResult"]["truncated"], true);
    let _ = std::fs::remove_dir_all(&root);
}

/// 文件族链路：相对路径经 `working_dir` 解析——写入（含建目录）→ 带行号
/// 读回 → `Grep` 命中 → `Glob` 命中 → `ListDir` 列出。
#[tokio::test]
async fn file_family_write_read_and_search() {
    let root = workspace("file-family");

    let written = call(
        Arc::new(WriteFileTool::new()),
        json!({ "file_path": "src/demo.rs", "content": "fn alpha() {}\nfn beta() {}\n" }),
        &root,
    )
    .await;
    assert!(!written.is_error, "content: {}", written.content);
    assert!(
        written.content.starts_with("create:"),
        "{}",
        written.content
    );
    assert!(root.join("src/demo.rs").is_file(), "file must exist");

    let read = call(
        Arc::new(ReadFileTool),
        json!({ "file_path": "src/demo.rs" }),
        &root,
    )
    .await;
    assert!(!read.is_error, "content: {}", read.content);
    assert!(
        read.content.contains("     1\tfn alpha() {}"),
        "content: {:?}",
        read.content
    );

    let grep = call(
        Arc::new(GrepTool),
        json!({ "pattern": "fn beta", "path": ".", "output_mode": "content" }),
        &root,
    )
    .await;
    assert!(!grep.is_error, "content: {}", grep.content);
    assert!(grep.content.contains("fn beta() {}"), "{}", grep.content);

    let globbed = call(Arc::new(GlobTool), json!({ "pattern": "**/*.rs" }), &root).await;
    assert!(!globbed.is_error, "content: {}", globbed.content);
    assert!(globbed.content.contains("demo.rs"), "{}", globbed.content);

    let listed = call(
        Arc::new(ListDirectoryTool),
        json!({ "path": ".", "recursive": true }),
        &root,
    )
    .await;
    assert!(!listed.is_error, "content: {}", listed.content);
    assert!(listed.content.contains("demo.rs"), "{}", listed.content);

    let _ = std::fs::remove_dir_all(&root);
}

/// 覆盖写：第二次写同一路径为 `update:`，字节数与新内容一致。
#[tokio::test]
async fn write_reports_update_on_existing_file() {
    let root = workspace("file-update");
    std::fs::write(root.join("a.txt"), "old").expect("seed");
    let output = call(
        Arc::new(WriteFileTool::new()),
        json!({ "file_path": "a.txt", "content": "brand-new" }),
        &root,
    )
    .await;
    assert!(!output.is_error, "content: {}", output.content);
    assert!(output.content.starts_with("update:"), "{}", output.content);
    let meta = output.metadata.expect("metadata");
    assert_eq!(meta["structuredResult"]["type"], "update");
    assert_eq!(meta["structuredResult"]["bytesWritten"], 9);
    assert_eq!(
        std::fs::read_to_string(root.join("a.txt")).expect("read"),
        "brand-new"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// `Edit`：未读先编辑被读前置门禁拦住；`Read` 后同一编辑唯一匹配
/// 落盘，元数据带 diff 与封存杂凑。
#[tokio::test]
async fn edit_gates_on_a_prior_read_then_replaces_uniquely() {
    let root = workspace("edit-family");
    std::fs::write(root.join("code.rs"), "fn alpha() {}\nfn beta() {}\n").expect("seed");
    let edit = json!({
        "file_path": "code.rs",
        "old_string": "fn beta() {}",
        "new_string": "fn gamma() {}"
    });

    let ungated = call_as(
        Arc::new(EditFileTool::new()),
        edit.clone(),
        &root,
        "session-edit",
    )
    .await;
    assert!(ungated.is_error, "content: {}", ungated.content);
    assert!(
        ungated.content.starts_with("FILE_READ_REQUIRED: "),
        "content: {}",
        ungated.content
    );

    let read = call_as(
        Arc::new(ReadFileTool),
        json!({ "file_path": "code.rs" }),
        &root,
        "session-edit",
    )
    .await;
    assert!(!read.is_error, "content: {}", read.content);

    let edited = call_as(Arc::new(EditFileTool::new()), edit, &root, "session-edit").await;
    assert!(!edited.is_error, "content: {}", edited.content);
    // 展示路径为解析后的绝对路径（同旧实现的 `Edited: {resolvedPath}`）。
    assert!(
        edited.content.starts_with("Edited: ") && edited.content.ends_with("code.rs"),
        "content: {}",
        edited.content
    );
    assert_eq!(
        std::fs::read_to_string(root.join("code.rs")).expect("read"),
        "fn alpha() {}\nfn gamma() {}\n"
    );
    let structured = edited.metadata.expect("metadata")["structuredResult"].clone();
    assert_eq!(structured["type"], "update");
    assert_eq!(structured["matchCount"], 1);
    assert!(
        structured["diff"]
            .as_str()
            .expect("diff")
            .contains("fn gamma() {}"),
        "diff: {}",
        structured["diff"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// `TodoWrite`：首次写入镖像到 `.zk/todos.md`，merge 模式按 `id` 原位覆盖。
#[tokio::test]
async fn todo_write_mirrors_into_the_project_config_dir() {
    let root = workspace("todo-family");
    let first = call_as(
        Arc::new(TodoWriteTool),
        json!({ "todos": [
            { "id": "t1", "content": "draft plan", "status": "IN_PROGRESS" },
            { "id": "t2", "content": "ship it", "status": "PENDING" }
        ] }),
        &root,
        "session-todo",
    )
    .await;
    assert!(!first.is_error, "content: {}", first.content);

    let mirror = root.join(".zk").join("todos.md");
    let rendered = std::fs::read_to_string(&mirror).expect("mirror written");
    assert!(
        rendered.contains("- [~] IN_PROGRESS — draft plan `t1`"),
        "{rendered}"
    );
    assert!(
        rendered.contains("- [ ] PENDING — ship it `t2`"),
        "{rendered}"
    );

    let merged = call_as(
        Arc::new(TodoWriteTool),
        json!({
            "merge": true,
            "todos": [{ "id": "t2", "content": "ship it", "status": "IN_PROGRESS" }]
        }),
        &root,
        "session-todo",
    )
    .await;
    assert!(!merged.is_error, "content: {}", merged.content);
    let rendered = std::fs::read_to_string(&mirror).expect("mirror rewritten");
    assert!(
        rendered.contains("- [~] IN_PROGRESS — ship it `t2`"),
        "{rendered}"
    );
    assert!(!rendered.contains("PENDING"), "{rendered}");
    let _ = std::fs::remove_dir_all(&root);
}

/// 逐题回答的桩端口（记录每次请求的会话 / Run / 选项数供断言）。
struct ScriptedSink {
    answers: Vec<&'static str>,
    seen: std::sync::Mutex<Vec<(String, Option<String>, usize)>>,
}

impl ElicitationSink for ScriptedSink {
    fn request_and_wait(&self, request: ElicitationRequest) -> BoxFuture<'_, ElicitationOutcome> {
        let mut seen = self.seen.lock().expect("lock");
        let index = seen.len();
        seen.push((request.session_id, request.run_id, request.options.len()));
        drop(seen);
        let answer = self.answers.get(index).copied().unwrap_or_default();
        Box::pin(async move { ElicitationOutcome::Success(Some(json!(answer))) })
    }
}

/// `AskUserQuestion`：两题逐个发出（携会话 / Run），答案按 `q1` / `q2` 回填。
#[tokio::test]
async fn ask_user_question_blocks_per_question_and_collects_answers() {
    let root = workspace("ask-family");
    let sink = Arc::new(ScriptedSink {
        answers: vec!["rust", "yes"],
        seen: std::sync::Mutex::new(Vec::new()),
    });
    let output = call_as(
        Arc::new(AskUserQuestionTool::with_elicitation_sink(sink.clone())),
        json!({ "questions": [
            { "question": "Language?", "options": ["rust", "java"] },
            { "question": "Proceed?", "options": ["yes", "no"] }
        ] }),
        &root,
        "session-ask",
    )
    .await;
    assert!(!output.is_error, "content: {}", output.content);
    let parsed: serde_json::Value = serde_json::from_str(&output.content).expect("json result");
    assert_eq!(parsed["answers"]["q1"], "rust");
    assert_eq!(parsed["answers"]["q2"], "yes");
    assert_eq!(parsed["questions"].as_array().expect("questions").len(), 2);
    let structured = output.metadata.expect("metadata")["structuredResult"].clone();
    assert_eq!(structured["answers"]["q1"], "rust");

    let seen = sink.seen.lock().expect("lock").clone();
    assert_eq!(
        seen,
        vec![
            ("session-ask".to_owned(), Some("run-it".to_owned()), 2),
            ("session-ask".to_owned(), Some("run-it".to_owned()), 2),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// `Config` 与 `SyntheticOutput`：经执行器走通读写与结构化回填。
#[tokio::test]
async fn config_and_synthetic_output_round_trip_through_the_executor() {
    let root = workspace("config-family");

    let listed = call(
        Arc::new(ConfigTool::new()),
        json!({ "action": "list" }),
        &root,
    )
    .await;
    assert!(!listed.is_error, "content: {}", listed.content);
    assert!(
        listed.content.starts_with("Available settings:\n"),
        "content: {}",
        listed.content
    );
    assert!(listed.content.contains("  theme = "), "{}", listed.content);

    let rejected = call(
        Arc::new(ConfigTool::new()),
        json!({ "action": "set", "key": "theme", "value": "neon" }),
        &root,
    )
    .await;
    assert!(rejected.is_error, "content: {}", rejected.content);
    assert_eq!(
        rejected.content,
        "CONFIG_VALUE_INVALID: Invalid value for 'theme'. Options: [system, light, dark]"
    );

    let tool = SyntheticOutputTool::new();
    tool.set_schema(json!({
        "type": "object",
        "properties": { "subject": { "type": "string" } },
        "required": ["subject"]
    }));
    let structured = call(
        Arc::new(tool.clone()),
        json!({ "subject": "feat: batch 2 tools" }),
        &root,
    )
    .await;
    assert!(!structured.is_error, "content: {}", structured.content);
    assert_eq!(
        structured.content,
        "Structured output provided successfully."
    );
    let meta = structured.metadata.expect("metadata");
    assert_eq!(
        meta["structuredResult"]["structured_output"]["subject"],
        "feat: batch 2 tools"
    );

    let empty = call(Arc::new(tool), json!({}), &root).await;
    assert!(empty.is_error, "content: {}", empty.content);
    assert_eq!(
        empty.content,
        "STRUCTURED_OUTPUT_EMPTY: Empty structured output."
    );
    let _ = std::fs::remove_dir_all(&root);
}
