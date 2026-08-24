//! `TodoWrite` 工具——任务清单的 merge / replace 双模式管理。
//!
//! 逐字对照旧 `tool/interaction/TodoWriteTool.java`（只读权威规格）：工具名
//! `TodoWrite`、入参 `todos` / `merge`、进程级按会话隔离的内存清单存储、
//! merge 模式按 `id` 覆盖且**保留旧条目的插入位置**（旧 `LinkedHashMap.put`
//! 语义）、「全部 `COMPLETE` / `CANCELLED` 即自动清空」、「本次输入中
//! `COMPLETE` 计数 ≥ 3 且结果清单无 `verif` 任务 → `verificationNudgeNeeded`」、
//! 返回 JSON `{oldTodos, newTodos[, verificationNudgeNeeded]}`。
//!
//! 差异（留痕 docs/compatibility.md §9）：
//!
//! - 旧实现向 `/topic/session/{id}` 推 `{"type":"todos_update","todos":…}`，
//!   但两端前端均无该消息的消费方（本仓前端仅消费 `task_update`，形状不
//!   兼容），属旧仓死推送；本实现改为 write-through 镜像
//!   `{cwd}/.zk/todos.md`（best-effort：失败仅告警、不回读、不阻断），
//!   既落地「`.zk/todos.md` 持久化」判据，又不新增无人消费的下行消息。
//! - `todos` 缺失 / 非数组时旧实现在后续 `forEach` / 拷贝构造上抛 NPE
//!   （HTTP 500），本实现按框架约定返回 `MISSING_PARAMETER` 校验失败。
//! - 旧存储为 `ConcurrentHashMap`，键为可空的 `context.sessionId()`；本实现
//!   经 [`session_key`] 把 `None` 归入匿名桶（`ConcurrentHashMap` 不接受
//!   null 键，旧实现在无会话上下文时会抛 NPE）。

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::atomic::{ExpectedOldState, sha256_hex, write_checked};
use crate::file_state::session_key;
use crate::input::{bool_or, failure};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 触发验证提醒的最小完成数（旧 `completedCount >= 3`）。
const VERIFICATION_NUDGE_THRESHOLD: usize = 3;

/// 验证任务的内容关键字（旧 `content.toLowerCase().contains("verif")`）。
const VERIFY_KEYWORD: &str = "verif";

/// 镜像文件名（相对 `{cwd}/.zk/`）。
const MIRROR_FILE_NAME: &str = "todos.md";

/// 任务清单工具（名 `TodoWrite`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct TodoWriteTool;

/// 进程级任务清单存储（等价旧单例 bean 持有的 `ConcurrentMap<scopeKey, List>`）。
fn store() -> &'static Mutex<HashMap<String, Vec<Value>>> {
    static STORE: LazyLock<Mutex<HashMap<String, Vec<Value>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    &STORE
}

/// 取存储锁（毒化后继续用内层值——清单是尽力而为的记账面）。
fn lock() -> std::sync::MutexGuard<'static, HashMap<String, Vec<Value>>> {
    store()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Tool for TodoWriteTool {
    fn name(&self) -> &'static str {
        "TodoWrite"
    }

    fn description(&self) -> &'static str {
        "Create and manage a task/todo list. \
         Supports merge mode (update by id) and replace mode (full replacement). \
         Auto-clears when all items are COMPLETE or CANCELLED."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "todos": {
                    "type": "array",
                    "description": "List of todo items",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" },
                            "content": { "type": "string" },
                            "status": {
                                "type": "string",
                                "enum": ["PENDING", "IN_PROGRESS", "COMPLETE", "CANCELLED"]
                            }
                        }
                    }
                },
                "merge": {
                    "type": "boolean",
                    "description": "true=merge by id, false=replace all"
                }
            },
            "required": ["todos"]
        })
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

/// 执行主体（取参 → merge/replace → 清空检测 → 验证提醒 → 落存储 + 镜像）。
async fn run(input: Value, ctx: ToolContext) -> ToolOutput {
    let Some(new_todos) = input.get("todos").and_then(Value::as_array) else {
        return failure(
            "MISSING_PARAMETER",
            "Required parameter 'todos' is missing or not an array",
        );
    };
    let merge = bool_or(&input, "merge", false);
    let scope = session_key(ctx.session_id());

    let old_todos = lock().get(scope).cloned().unwrap_or_default();
    let mut result_todos = if merge {
        merge_by_id(&old_todos, new_todos)
    } else {
        new_todos.clone()
    };

    // 全部完成 / 取消 → 自动清空（旧 `allComplete` 分支）。
    let all_settled = !result_todos.is_empty() && result_todos.iter().all(is_settled);
    if all_settled {
        result_todos.clear();
    }

    // 验证提醒：完成数取自**本次输入**，验证任务检测取自结果清单（旧同序）。
    let completed_count = new_todos
        .iter()
        .filter(|todo| status_of(todo) == Some("COMPLETE"))
        .count();
    let has_verify_task = result_todos.iter().any(|todo| {
        todo.get("content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_lowercase()
            .contains(VERIFY_KEYWORD)
    });
    let verification_nudge_needed =
        completed_count >= VERIFICATION_NUDGE_THRESHOLD && !has_verify_task;

    lock().insert(scope.to_owned(), result_todos.clone());
    mirror(&ctx, &result_todos).await;

    let mut result = serde_json::Map::new();
    result.insert("oldTodos".to_owned(), Value::Array(old_todos));
    result.insert("newTodos".to_owned(), Value::Array(result_todos.clone()));
    if verification_nudge_needed {
        result.insert("verificationNudgeNeeded".to_owned(), Value::Bool(true));
    }
    let text = serde_json::to_string(&Value::Object(result)).unwrap_or_else(|error| {
        // 旧实现的 `JsonProcessingException` 回落分支（本实现的入参已是合法
        // JSON，理论不可达，保留以对齐行为面）。
        tracing::warn!(%error, "todo result serialization failed");
        format!("Todos updated. Count: {}", result_todos.len())
    });
    let mut output = ToolOutput::ok(text);
    output.metadata = Some(json!({
        "structuredResult": {
            "todos": result_todos,
            "verificationNudgeNeeded": verification_nudge_needed,
        }
    }));
    output
}

/// 按 `id` 合并：旧条目保序在前，同 `id` 原位覆盖，新 `id` 追加
/// （旧 `LinkedHashMap` 先 put 旧、后 put 新的等价语义；`id` 缺失的条目
/// 与旧实现的 null 键一致地共用同一槽位）。
fn merge_by_id(old_todos: &[Value], new_todos: &[Value]) -> Vec<Value> {
    let mut keys: Vec<Option<&str>> = Vec::new();
    let mut merged: Vec<Value> = Vec::new();
    for todo in old_todos.iter().chain(new_todos) {
        let key = todo.get("id").and_then(Value::as_str);
        if let Some(position) = keys.iter().position(|existing| *existing == key) {
            merged[position] = todo.clone();
        } else {
            keys.push(key);
            merged.push(todo.clone());
        }
    }
    merged
}

/// 条目状态（缺失 / 非字符串 → `None`）。
fn status_of(todo: &Value) -> Option<&str> {
    todo.get("status").and_then(Value::as_str)
}

/// 是否为终态（旧 `"COMPLETE".equals(status) || "CANCELLED".equals(status)`）。
fn is_settled(todo: &Value) -> bool {
    matches!(status_of(todo), Some("COMPLETE" | "CANCELLED"))
}

/// write-through 镜像 `{cwd}/.zk/todos.md`（best-effort，失败仅告警）。
async fn mirror(ctx: &ToolContext, todos: &[Value]) {
    let dir = zk_core::paths::project_config_dir(ctx.working_dir());
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        tracing::warn!(dir = %dir.display(), %error, "todo mirror directory unavailable");
        return;
    }
    let path = dir.join(MIRROR_FILE_NAME);
    // 取当前内容推导 CAS 前置态：并发改写时镜像写入放弃而非覆盖未知内容。
    let expected = match tokio::fs::read(&path).await {
        Ok(bytes) => ExpectedOldState::Sha256(sha256_hex(&bytes)),
        Err(_) => ExpectedOldState::Absent,
    };
    let outcome = write_checked(&path, &render_markdown(todos), &expected).await;
    if !outcome.success {
        tracing::warn!(
            path = %path.display(),
            error = outcome.error.as_deref().unwrap_or_default(),
            "todo mirror write failed"
        );
    }
}

/// 渲染镜像 Markdown（复选框状态 → `PENDING` / `IN_PROGRESS` / `COMPLETE` /
/// `CANCELLED` 四态；清单为空时输出显式空标记）。
fn render_markdown(todos: &[Value]) -> String {
    use std::fmt::Write as _;

    let mut text = String::from("# Todos\n\n");
    if todos.is_empty() {
        text.push_str("_(empty)_\n");
        return text;
    }
    for todo in todos {
        let status = status_of(todo).unwrap_or("PENDING");
        let mark = match status {
            "COMPLETE" => "x",
            "IN_PROGRESS" => "~",
            "CANCELLED" => "-",
            _ => " ",
        };
        let content = todo
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let id = todo.get("id").and_then(Value::as_str).unwrap_or_default();
        // 写入 String 永不失败。
        let _ = write!(text, "- [{mark}] {status} — {content}");
        if !id.is_empty() {
            let _ = write!(text, " `{id}`");
        }
        text.push('\n');
    }
    text
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx(session: &str, dir: &std::path::Path) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_working_dir(dir)
            .with_session_id(session)
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-todo-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn todo(id: &str, content: &str, status: &str) -> Value {
        json!({ "id": id, "content": content, "status": status })
    }

    async fn call(session: &str, dir: &std::path::Path, input: Value) -> ToolOutput {
        TodoWriteTool.execute(input, ctx(session, dir)).await
    }

    fn parse(output: &ToolOutput) -> Value {
        serde_json::from_str(&output.content).expect("json result")
    }

    #[tokio::test]
    async fn replace_mode_overwrites_the_whole_list() {
        let dir = temp_dir("replace");
        let session = "todo-replace";
        let first = call(
            session,
            &dir,
            json!({ "todos": [todo("1", "step one", "PENDING")] }),
        )
        .await;
        assert!(!first.is_error);
        assert_eq!(parse(&first)["oldTodos"], json!([]));

        let second = call(
            session,
            &dir,
            json!({ "todos": [todo("9", "other", "PENDING")] }),
        )
        .await;
        let result = parse(&second);
        assert_eq!(
            result["oldTodos"],
            json!([todo("1", "step one", "PENDING")])
        );
        assert_eq!(result["newTodos"], json!([todo("9", "other", "PENDING")]));
    }

    #[tokio::test]
    async fn merge_mode_overwrites_by_id_and_keeps_original_order() {
        let dir = temp_dir("merge");
        let session = "todo-merge";
        call(
            session,
            &dir,
            json!({ "todos": [
                todo("a", "first", "PENDING"),
                todo("b", "second", "PENDING"),
            ] }),
        )
        .await;
        let merged = call(
            session,
            &dir,
            json!({
                "merge": true,
                "todos": [
                    todo("b", "second", "IN_PROGRESS"),
                    todo("c", "third", "PENDING"),
                ]
            }),
        )
        .await;
        assert_eq!(
            parse(&merged)["newTodos"],
            json!([
                todo("a", "first", "PENDING"),
                todo("b", "second", "IN_PROGRESS"),
                todo("c", "third", "PENDING"),
            ]),
            "同 id 原位覆盖、新 id 追加"
        );
    }

    #[tokio::test]
    async fn clears_the_list_when_every_item_is_settled() {
        let dir = temp_dir("clear");
        let session = "todo-clear";
        let output = call(
            session,
            &dir,
            json!({ "todos": [
                todo("a", "done", "COMPLETE"),
                todo("b", "dropped", "CANCELLED"),
            ] }),
        )
        .await;
        assert_eq!(parse(&output)["newTodos"], json!([]));
        assert!(lock().get(session).expect("bucket").is_empty());
    }

    #[tokio::test]
    async fn nudges_verification_after_three_completions_without_a_verify_task() {
        let dir = temp_dir("nudge");
        let session = "todo-nudge";
        // 三条 COMPLETE + 一条未完成 → 不触发自动清空，且无 verif 任务。
        let output = call(
            session,
            &dir,
            json!({ "todos": [
                todo("a", "one", "COMPLETE"),
                todo("b", "two", "COMPLETE"),
                todo("c", "three", "COMPLETE"),
                todo("d", "four", "PENDING"),
            ] }),
        )
        .await;
        assert_eq!(parse(&output)["verificationNudgeNeeded"], json!(true));

        let with_verify = call(
            session,
            &dir,
            json!({ "todos": [
                todo("a", "one", "COMPLETE"),
                todo("b", "two", "COMPLETE"),
                todo("c", "three", "COMPLETE"),
                todo("d", "Verify the build", "PENDING"),
            ] }),
        )
        .await;
        assert_eq!(parse(&with_verify)["verificationNudgeNeeded"], Value::Null);
    }

    #[tokio::test]
    async fn mirrors_the_list_into_the_project_config_dir() {
        let dir = temp_dir("mirror");
        let session = "todo-mirror";
        call(
            session,
            &dir,
            json!({ "todos": [
                todo("a", "write code", "IN_PROGRESS"),
                todo("b", "ship it", "PENDING"),
            ] }),
        )
        .await;
        let mirror_path = zk_core::paths::project_config_dir(&dir).join(MIRROR_FILE_NAME);
        let text = std::fs::read_to_string(&mirror_path).expect("mirror file");
        assert!(text.starts_with("# Todos\n\n"));
        assert!(text.contains("- [~] IN_PROGRESS — write code `a`"));
        assert!(text.contains("- [ ] PENDING — ship it `b`"));

        // 自动清空后镜像同步为空标记。
        call(
            session,
            &dir,
            json!({ "todos": [todo("a", "write code", "COMPLETE")] }),
        )
        .await;
        let cleared = std::fs::read_to_string(&mirror_path).expect("mirror file");
        assert_eq!(cleared, "# Todos\n\n_(empty)_\n");
    }

    #[tokio::test]
    async fn rejects_missing_or_non_array_todos() {
        let dir = temp_dir("bad");
        let missing = call("todo-bad", &dir, json!({})).await;
        assert!(missing.is_error);
        assert_eq!(
            missing.content,
            "MISSING_PARAMETER: Required parameter 'todos' is missing or not an array"
        );

        let wrong_type = call("todo-bad", &dir, json!({ "todos": "nope" })).await;
        assert!(wrong_type.is_error);
    }

    #[test]
    fn spec_matches_the_legacy_contract() {
        let spec = TodoWriteTool.spec();
        assert_eq!(spec.name, "TodoWrite");
        assert_eq!(spec.parameters["required"], json!(["todos"]));
        assert_eq!(
            spec.parameters["properties"]["todos"]["items"]["properties"]["status"]["enum"],
            json!(["PENDING", "IN_PROGRESS", "COMPLETE", "CANCELLED"])
        );
    }
}
