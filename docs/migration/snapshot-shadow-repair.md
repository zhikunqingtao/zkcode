# 快照恢复保护与开放 Shadow DOM 观察

日期：2026-10-08。接续 [N1–N5 修复记录](n1-n5-repair.md) 中的快照缺口，以及安全语义投影的覆盖取舍。

后续更新：快照恢复的当前契约见[保留执行事实的安全恢复](session-snapshot-safe-restore.md)。下文保留本轮历史证据；其中清空 ackEligible 消息及 journal 的旧行为已被后续修复收紧。

## 快照恢复

恢复允许舍弃快照之后的普通历史，但不能删除活动执行和后处理仍需消费的消息、journal。没有新增数据库表、修改外键或重放浏览器动作。

- REST 恢复复用查询的会话执行槽位，在读取当前历史前取得保护；持久状态仍由数据库事务验证。
- 实际恢复由持有该槽位的独立异步任务完成。HTTP 等待取消不会释放仍在排队或执行的 SQLite 写入的保护；提交或失败后释放。
- 同一恢复事务在删除消息前检查活动任务、交互、未释放资源、合并占用，以及目标会话消息上的 pending 后处理。
- 成功录制处于 `sealed` 时，即使 journal 已 completed，仍需它进入 `ackEligible`，因此保留消息和 journal。
- `ackEligible` 的后续 ACK 不再依赖 journal，不仅因为未 ACK 而拒绝恢复。只保护本次替换的消息，不递归套用删除父会话时的整树录制门控。
- 冲突返回 HTTP 409；原消息、journal、资源和会话元数据不变。既有外键及其他合法恢复约束继续生效。

测试覆盖真实 Router、未持久化 Run 的真实查询 lease、Running/NeedsAttention、独立后处理依赖、sealed/completed、ACK 正向路径、无关子会话、合并锁及正常幂等恢复。取消测试通过真实 writer barrier 保证时序，不依赖 sleep 推测请求进度。

## 安全语义投影

本次补充开放 Shadow DOM 中的基本观察能力；`complete` 仍表示已声明的采集范围成功完成，不表示完整无障碍树或旅程验证通过。

- 在既有采集根中有界发现嵌套 open shadow roots，复用共享元素投影；密码和隐藏输入值不读取，普通值不重复读取。
- `aria-labelledby` 在元素所属的 document/shadow root 中解析。
- 保留普通 light DOM 原有的独立前 200 项交互查询，安全树截断不会让原本可见的后方按钮从交互清单消失；shadow 控件使用剩余容量。
- 安全树和 shadow 发现共用原有 2000 节点、64 KiB UTF-8 树文本边界。保留隐藏/排除子树与 textarea 原始子文本的跳过规则。
- `node_count` 沿用当前文档 light DOM 元素统计，包含隐藏后代，不扩成跨 shadow 的全量统计。
- 不扩展 selector 语法，不重排 slot，不进入 iframe 或 closed roots，不恢复原生 ARIA。

真实 Chromium 回归包含控件实时状态、嵌套作用域、标签归属、密码 getter、限额，以及大量普通节点/隐藏子树之后的控件与原计数兼容性。

## 验证边界

在私有源码副本、数据库、快照、spool 和浏览器上下文中验证；未重启开发服务或访问现有对话数据。最终执行结果和源码摘要保存在本轮修复的外部验收报告中。

| 最终源码上的检查 | 结果 |
|---|---|
| `cargo test -p zk-db`，含 7 项新增恢复专项 | 270 passed |
| `zk-server` 的 `session_snapshot_api` | 7 passed |
| `api::session_snapshot::tests`，writer barrier 与取消保护 | 2 passed |
| `python::tools::browser_recordings::tests`，既有录制恢复 | 11 passed |
| Python 投影、生命周期、真实浏览器、临时证据四个模块 | 124 passed，含 31 项投影测试 |
| `zk-db` / `zk-server` 全目标 Clippy，`-D warnings` | 通过 |
| Cargo fmt / Git diff 空白检查 | 通过 |

Rust 使用 `--offline --locked` 与私有 target；Python 使用私有源码、spool 和 pytest 目录。先前迭代的 201/123 项 Python 记录不重复计入最终 124 项。

既有 `task_results.final_message_id` 的 `ON DELETE RESTRICT` 保持不变；被已发布终态结果引用的消息仍不能经此恢复删除。本次不承诺任意完整任务历史都能回滚，也不通过削弱外键让正向夹具通过。ACK 正向测试使用成功工具后后续 Run 合法失败、无最终助手消息引用的状态，以独立验证本次录制阶段门控。

本记录只覆盖这两项修复，不把此前其他工作区改动、真实外部服务或生产部署算作本次重新验收。没有提交或推送 GitHub。
