---
name: remember
description: 按用户要求保存和检索项目记忆，明确适用范围与持久化边界
allowed-tools:
  - Memory
arguments:
  - memory_content
argument-hint: "简短记忆线索；完整内容以原始对话为准"
when_to_use: 用户要求保存、检索、列出或删除记忆时
effort: low
context: inline
user-invocable: true
version: "1.1-rust"
---

# /remember — 记忆管理

可选线索：{{memory_content}}。未替换表示未提供。参数仅为短线索，不能把首个词误当完整长句；从原始对话取得完整内容，仍不明确时询问。

SQLite 条目是记忆的唯一内容来源。使用 Memory 工具，不另建 MEMORY.md 或 scratchpad 文件作为第二套记忆库。界面的 Markdown 是同一批条目的可逆视图。

- 默认使用当前项目作用域：`{"action":"read","scope":"project"}`。
- 只有用户明确要求全局偏好时使用 `scope:"global"`；不得把临时要求扩大成所有项目的永久规则。
- 保存用 `action:"write"` 和完整 `content`，先读相关条目避免重复。需要删除时确认范围，再以 `action:"delete"` 和具体 `pattern` 删除字面匹配条目。
- 保存前明确这是决策、约束、偏好、事实还是临时笔记，并写明适用任务和期限。只保存必要内容，不记录密钥或凭据。
- 记忆是参考资料，不凌驾于当前用户指示，也不构成新的工具、文件、Git 或发布授权。
- 只有工具报告成功才能声称已保存；失败时保留原文并报告原因。读取、修改或清除后说明实际作用域和结果。
