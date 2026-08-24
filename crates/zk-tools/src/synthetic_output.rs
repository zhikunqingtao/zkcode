//! `SyntheticOutput` 工具——模型按动态 JSON Schema 回填结构化数据。
//!
//! 逐字对照旧 `tool/config/SyntheticOutputTool.java`（只读权威规格）：
//! 工具名 `SyntheticOutput`、只读、无权限要求、入参 Schema 由引擎在发起
//! 结构化输出查询前经 [`SyntheticOutputTool::set_schema`] 动态注入
//! （未注入时回落 `{"type":"object","additionalProperties":true}` 即接受任意
//! JSON，并留一条 `SyntheticOutput called without schema` 告警）、空入参
//! 拒绝（`STRUCTURED_OUTPUT_EMPTY`）、成功文案
//! `Structured output provided successfully.`、原始数据随元数据回传。
//!
//! 差异（留痕 docs/compatibility.md §9）：
//!
//! - 旧实现把原始数据挂在 `metadata.structured_output`；本框架的引擎侧只透传
//!   `metadata.structuredResult` 一个键（[`ToolOutput::metadata`] 契约），故落
//!   在 `structuredResult.structured_output`——键名保留旧拼写，便于比对。
//! - 旧 `getInputSchema()` 直接返回 `volatile currentSchema` 引用；本实现以
//!   `RwLock<Option<Value>>` 持有并克隆返回（Rust 无共享可变引用）。
//! - 旧 Schema 校验为 P1 占位（注释掉的 `currentSchema.validate`），本实现
//!   同样不做校验以保持行为一致；引入校验器属后续增量，不在本批次。
//! - 旧 `isConcurrencySafe(input) = true` 无 Rust 对应面（并发安全由执行器的
//!   信号量 + 取消树统一承担），未移植。

use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::failure;
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 结构化输出工具（名 `SyntheticOutput`）。
///
/// Schema 由引擎在查询前注入、查询后清除；克隆共享同一份 Schema
/// （等价旧单例 bean 的 `volatile` 字段）。
#[derive(Clone, Debug, Default)]
pub struct SyntheticOutputTool {
    /// 当前 Schema；`None` = 接受任意 JSON（旧 `currentSchema == null`）。
    schema: Arc<RwLock<Option<Value>>>,
}

impl SyntheticOutputTool {
    /// 装配（初始无 Schema）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 注入 Schema（旧 `setSchema`；引擎在发起结构化输出查询时调用）。
    pub fn set_schema(&self, schema: Value) {
        *self.write() = Some(schema);
    }

    /// 清除 Schema（旧 `clearSchema`；查询结束后调用）。
    pub fn clear_schema(&self) {
        *self.write() = None;
    }

    /// 当前 Schema（旧包级 `getCurrentSchema()`，测试与引擎自检用）。
    #[must_use]
    pub fn current_schema(&self) -> Option<Value> {
        self.read().clone()
    }

    /// 取读锁（毒化后继续用内层值——Schema 是纯数据，无破损不变式）。
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Option<Value>> {
        self.schema
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 取写锁（毒化后继续用内层值）。
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Option<Value>> {
        self.schema
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// 无 Schema 时的兜底入参定义（旧 `Map.of("type","object","additionalProperties",true)`）。
fn permissive_schema() -> Value {
    json!({ "type": "object", "additionalProperties": true })
}

impl Tool for SyntheticOutputTool {
    fn name(&self) -> &'static str {
        "SyntheticOutput"
    }

    fn description(&self) -> &'static str {
        "Provide structured output conforming to a specified JSON schema. \
         Used for commit messages, classifier results, and other structured data."
    }

    /// 动态入参定义（旧 `getInputSchema`：有注入用注入值，否则接受任意 JSON）。
    fn parameters(&self) -> Value {
        self.read().clone().unwrap_or_else(permissive_schema)
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.run(&input) })
    }
}

impl SyntheticOutputTool {
    /// 执行主体（无 Schema 告警 → 空入参拒绝 → 原样回传）。
    fn run(&self, input: &Value) -> ToolOutput {
        if self.read().is_none() {
            tracing::warn!("SyntheticOutput called without schema — accepting raw data");
        }
        let is_empty = input
            .as_object()
            .is_none_or(serde_json::Map::<String, Value>::is_empty);
        if is_empty {
            return failure("STRUCTURED_OUTPUT_EMPTY", "Empty structured output.");
        }
        // 旧实现在此序列化一次并记录长度（同时是 catch 分支的唯一触发源）。
        let rendered = match serde_json::to_string(input) {
            Ok(rendered) => rendered,
            Err(error) => {
                return failure(
                    "STRUCTURED_OUTPUT_PROCESSING_FAILED",
                    format!("Failed to process structured output: {error}"),
                );
            }
        };
        tracing::info!(chars = rendered.len(), "SyntheticOutput received");
        ToolOutput {
            content: "Structured output provided successfully.".to_owned(),
            is_error: false,
            metadata: Some(json!({ "structuredResult": { "structured_output": input } })),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    async fn call(tool: &SyntheticOutputTool, input: Value) -> ToolOutput {
        tool.execute(input, ctx()).await
    }

    #[tokio::test]
    async fn accepts_any_payload_without_schema_and_echoes_it_in_metadata() {
        let tool = SyntheticOutputTool::new();
        assert_eq!(tool.parameters(), permissive_schema());

        let output = call(&tool, json!({ "subject": "fix: guard", "body": "" })).await;
        assert!(!output.is_error);
        assert_eq!(output.content, "Structured output provided successfully.");
        let structured = output.metadata.expect("metadata")["structuredResult"].clone();
        assert_eq!(structured["structured_output"]["subject"], "fix: guard");
        assert_eq!(structured["structured_output"]["body"], "");
    }

    #[tokio::test]
    async fn rejects_empty_and_non_object_payloads() {
        let tool = SyntheticOutputTool::new();
        for input in [json!({}), json!(null), json!("text"), json!([1, 2])] {
            let output = call(&tool, input).await;
            assert!(output.is_error);
            assert_eq!(
                output.content,
                "STRUCTURED_OUTPUT_EMPTY: Empty structured output."
            );
        }
    }

    #[test]
    fn schema_injection_is_visible_through_parameters_and_reversible() {
        let tool = SyntheticOutputTool::new();
        let schema = json!({
            "type": "object",
            "properties": { "subject": { "type": "string" } },
            "required": ["subject"]
        });
        tool.set_schema(schema.clone());
        assert_eq!(tool.current_schema(), Some(schema.clone()));
        assert_eq!(tool.parameters(), schema);

        // 克隆共享同一份 Schema（等价旧单例 bean 的 volatile 字段）。
        let shared = tool.clone();
        shared.clear_schema();
        assert_eq!(tool.current_schema(), None);
        assert_eq!(tool.parameters(), permissive_schema());
    }

    #[test]
    fn spec_is_stable_and_read_only() {
        let tool = SyntheticOutputTool::new();
        assert_eq!(tool.spec().name, "SyntheticOutput");
        assert!(tool.is_read_only(&json!({})));
        assert!(!tool.is_destructive(&json!({})));
    }
}
