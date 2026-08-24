//! 消息序列五步标准化管线。
//!
//! 在压缩（cascade）之后、LLM API 请求构造之前执行，确保消息序列满足所有
//! LLM Provider 的约束条件，避免回放非法序列触发 400。
//!
//! 对照 Java 基线 `MessageNormalizer.java` 的五阶段管线，按本仓库
//! [`zk_llm::ChatMessage`] 的**扁平文本模型**（role + `content: String` +
//! `tool_calls` + `tool_call_id`）等价落地：
//!
//! 1. **过滤系统消息**——`SystemMessage` 由 [`zk_llm::ChatRequest::system`]
//!    独立字段承载，不应出现在对话序列中（对照 Java Phase 1
//!    `filterMessages`）。
//! 2. **合并连续同角色消息**——部分模型不允许连续同 role；相邻 `Assistant`
//!    消息合并文本内容与工具调用（对照 Java Phase 2 `convertAndMerge`；本
//!    仓库扁平文本模型不合并 user 以保留语义边界，详见
//!    [`merge_consecutive_same_role`]）。
//! 3. **thinking 块处理**——本仓库 `ChatMessage.content` 为纯文本，思考内容
//!    未在消息模型中单独建模（见 `provider.rs`：assistant 历史
//!    `reasoning_content` 回传为待办），故 Java 的 orphan-thinking /
//!    尾部 thinking 剥离在扁平模型下无对应载体，此步为结构占位、当前不改动
//!    序列。若后续消息模型引入思考块，在此按目标模型能力剥离。
//! 4. **`tool_use` / `tool_result` 配对保证**——API 要求每个 `tool_use`
//!    必须有对应 `tool_result`；对孤儿 `tool_use` 在其所属 assistant 之后
//!    补一条 error `tool_result`（对照 Java Phase 4 `ensureToolResultPairing`
//!    与本仓库 `ORPHAN_TOOL_RESULT` 兜底语义）。
//! 5. **空内容过滤**——移除文本空白且无工具调用的 assistant 消息（对照
//!    Java Phase 5 `filterEmptyAssistantMessages`）。`Tool` 消息即便内容为空
//!    也承载配对关系，绝不移除，否则会重新制造孤儿。

use std::collections::HashSet;

use zk_llm::{ChatMessage, Role};

/// 孤儿 `tool_use` 的合成 error `tool_result` 文案。
///
/// 对照 Java `ensureToolResultPairing` 的 `<tool_use_error>No result
/// received</tool_use_error>` 与本仓库 `engine::ORPHAN_TOOL_RESULT`：回放到
/// provider 时以显式错误占位，避免「`tool_use` 后必须紧跟 tool 消息」的
/// 400 令会话永久损坏。
const ORPHAN_TOOL_RESULT: &str =
    "<tool_use_error>Tool execution was interrupted before completion</tool_use_error>";

/// 原地标准化消息列表。
///
/// 见模块级文档的五步管线说明。原地修改 `&mut Vec<ChatMessage>`，仅在孤儿
/// `tool_use` 存在时新增合成 `tool_result`（正常序列零结构变更）。
pub fn normalize(messages: &mut Vec<ChatMessage>) {
    filter_system_messages(messages);
    merge_consecutive_same_role(messages);
    // Step 3 thinking：扁平文本模型无思考块载体，当前为无操作（见模块文档）。
    ensure_tool_result_pairing(messages);
    filter_empty_assistant_messages(messages);
}

/// Step 1：移除 `System` 角色消息（系统提示经独立参数传入）。
fn filter_system_messages(messages: &mut Vec<ChatMessage>) {
    messages.retain(|msg| msg.role != Role::System);
}

/// Step 2：合并相邻且 role 相同的 `Assistant` 消息。
///
/// 仅 assistant 消息合并——连续 assistant 是病态历史（正常流程一轮只有一条
/// assistant），合并文本 + 延展 `tool_calls` 恢复序列合法性。
///
/// `User` 消息不合并：Rust 扁平文本模型无法做到 Java 的「content block 列表
/// 追加」（非破坏性结构合并），字符串拼接会丢失语义边界（如恢复消息独立于
/// 原始用户输入）。`Tool` 消息各自携带 `tool_call_id` 配对锚点，不可合并。
fn merge_consecutive_same_role(messages: &mut Vec<ChatMessage>) {
    let mut merged: Vec<ChatMessage> = Vec::with_capacity(messages.len());
    for msg in messages.drain(..) {
        let should_merge = if let Some(last) = merged.last() {
            last.role == msg.role && msg.role == Role::Assistant
        } else {
            false
        };
        if should_merge {
            let last = merged.last_mut().unwrap();
            if !msg.content.is_empty() {
                if last.content.is_empty() {
                    last.content = msg.content;
                } else {
                    last.content.push('\n');
                    last.content.push_str(&msg.content);
                }
            }
            last.tool_calls.extend(msg.tool_calls);
        } else {
            merged.push(msg);
        }
    }
    *messages = merged;
}

/// Step 4：为孤儿 `tool_use` 补一条 error `tool_result`。
///
/// 先全量收集已存在的 `tool_result`（`Tool` 消息的 `tool_call_id`），再按序
/// 扫描 assistant 消息的 `tool_calls`，对无匹配结果者在其所属 assistant 之后
/// 插入合成 `Tool` 消息。插入按升序位置 + 偏移累加，保持工具调用顺序稳定。
fn ensure_tool_result_pairing(messages: &mut Vec<ChatMessage>) {
    let have_results: HashSet<&str> = messages
        .iter()
        .filter_map(|msg| msg.tool_call_id.as_deref())
        .collect();

    let mut seen: HashSet<String> = HashSet::new();
    // (插入位置, 孤儿 tool_use id)——按 assistant 出现顺序天然升序。
    let mut insertions: Vec<(usize, String)> = Vec::new();
    for (idx, msg) in messages.iter().enumerate() {
        if msg.role != Role::Assistant {
            continue;
        }
        for call in &msg.tool_calls {
            if !have_results.contains(call.id.as_str()) && seen.insert(call.id.clone()) {
                insertions.push((idx + 1, call.id.clone()));
            }
        }
    }

    for (offset, (idx, id)) in insertions.into_iter().enumerate() {
        messages.insert(
            idx + offset,
            ChatMessage::tool(id, ORPHAN_TOOL_RESULT.to_owned()),
        );
    }
}

/// Step 5：移除文本空白且无工具调用的 assistant 消息。
///
/// `User` / `Tool` 消息一律保留：`Tool` 承载 `tool_call_id` 配对关系，移除会
/// 重新制造孤儿。
fn filter_empty_assistant_messages(messages: &mut Vec<ChatMessage>) {
    messages.retain(|msg| {
        if msg.role != Role::Assistant {
            return true;
        }
        !msg.tool_calls.is_empty() || !msg.content.trim().is_empty()
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use zk_llm::ToolCallRequest;

    fn tool_call(id: &str) -> ToolCallRequest {
        ToolCallRequest {
            id: id.to_owned(),
            name: "Read".to_owned(),
            arguments: "{}".to_owned(),
        }
    }

    #[test]
    fn removes_system_messages() {
        let mut messages = vec![
            ChatMessage::system("you are helpful"),
            ChatMessage::user("hi"),
        ];
        normalize(&mut messages);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, Role::User);
        assert_eq!(messages[0].content, "hi");
    }

    #[test]
    fn does_not_merge_consecutive_user_messages() {
        // Rust 扁平文本模型不合并 user 消息（避免丢失语义边界）。
        let mut messages = vec![
            ChatMessage::user("first"),
            ChatMessage::user("second"),
            ChatMessage::assistant("reply"),
        ];
        normalize(&mut messages);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role, Role::User);
        assert_eq!(messages[0].content, "first");
        assert_eq!(messages[1].role, Role::User);
        assert_eq!(messages[1].content, "second");
        assert_eq!(messages[2].role, Role::Assistant);
    }

    #[test]
    fn merges_consecutive_assistant_and_keeps_tool_calls() {
        let mut messages = vec![
            ChatMessage::user("go"),
            ChatMessage::assistant("thinking out loud"),
            ChatMessage::assistant_tool_calls("", vec![tool_call("call_1")]),
            ChatMessage::tool("call_1", "done"),
        ];
        normalize(&mut messages);
        // 两条 assistant 合并为一条，携带工具调用。
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].role, Role::Assistant);
        assert_eq!(messages[1].content, "thinking out loud");
        assert_eq!(messages[1].tool_calls.len(), 1);
        assert_eq!(messages[1].tool_calls[0].id, "call_1");
        assert_eq!(messages[2].role, Role::Tool);
    }

    #[test]
    fn injects_synthetic_result_for_orphan_tool_use() {
        let mut messages = vec![
            ChatMessage::user("run it"),
            ChatMessage::assistant_tool_calls("", vec![tool_call("call_orphan")]),
            ChatMessage::user("next question"),
        ];
        normalize(&mut messages);
        // 孤儿 tool_use 之后补一条 Tool error 结果。
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1].role, Role::Assistant);
        assert_eq!(messages[2].role, Role::Tool);
        assert_eq!(messages[2].tool_call_id.as_deref(), Some("call_orphan"));
        assert_eq!(messages[2].content, ORPHAN_TOOL_RESULT);
        assert_eq!(messages[3].role, Role::User);
    }

    #[test]
    fn injects_synthetic_results_for_multiple_orphans_in_order() {
        let mut messages = vec![
            ChatMessage::assistant_tool_calls("", vec![tool_call("call_a"), tool_call("call_b")]),
            ChatMessage::tool("call_a", "a done"),
        ];
        normalize(&mut messages);
        // call_a 已配对；仅 call_b 孤儿，插入到 assistant 之后、既有结果之前。
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role, Role::Assistant);
        assert_eq!(messages[1].role, Role::Tool);
        assert_eq!(messages[1].tool_call_id.as_deref(), Some("call_b"));
        assert_eq!(messages[1].content, ORPHAN_TOOL_RESULT);
        assert_eq!(messages[2].tool_call_id.as_deref(), Some("call_a"));
    }

    #[test]
    fn filters_empty_assistant_messages() {
        // 空白 assistant 被 step 5 移除，剩余消息保持原序。
        let mut messages = vec![
            ChatMessage::user("hi"),
            ChatMessage::assistant("   "),
            ChatMessage::user("another"),
            ChatMessage::assistant("real reply"),
        ];
        normalize(&mut messages);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role, Role::User);
        assert_eq!(messages[0].content, "hi");
        assert_eq!(messages[1].role, Role::User);
        assert_eq!(messages[1].content, "another");
        assert_eq!(messages[2].role, Role::Assistant);
        assert_eq!(messages[2].content, "real reply");
    }

    #[test]
    fn keeps_empty_tool_result_message() {
        // 空内容 Tool 消息仍承载配对，绝不移除。
        let mut messages = vec![
            ChatMessage::assistant_tool_calls("", vec![tool_call("call_1")]),
            ChatMessage::tool("call_1", ""),
        ];
        normalize(&mut messages);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1].role, Role::Tool);
        assert_eq!(messages[1].tool_call_id.as_deref(), Some("call_1"));
    }

    #[test]
    fn leaves_well_formed_sequence_unchanged() {
        let original = vec![
            ChatMessage::user("请读取文件"),
            ChatMessage::assistant_tool_calls("好的", vec![tool_call("call_1")]),
            ChatMessage::tool("call_1", "file contents"),
            ChatMessage::assistant("这是内容摘要"),
        ];
        let mut messages = original.clone();
        normalize(&mut messages);
        assert_eq!(messages, original);
    }
}
