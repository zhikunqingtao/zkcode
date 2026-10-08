# 未知价格与终态诊断修复记录

日期：2026-10-08。起点：`main` / `298d2b30b3d310420c12d953bb4382ec8064d1bb` 加工作区既有改动。

后续：用户已授权在 N1–N5 中纳入本文末尾的原子金额边界；该修复及新的验证记录见 [N1–N5 修复记录](./n1-n5-repair.md)。下文保留当时的范围、失败和通过记录，不作为后续源码的验收结果。

本次只处理未知价格拒绝调用、未知费用误作零费用、失败诊断被完成快照覆盖三项问题。没有修改费用累加算法、订阅计费体系、数据库 schema 或 Python；没有提交、推送、切换分支或重启用户开发服务。

## 起始保护与范围

起始内容、SHA256 清单、139 条工作区状态和完整 Git 差异保存在 `/tmp/zk-price-fix-20261008-154305/`（本机临时验证目录，不进入仓库）。`before/` 是本次增量比较基准，不能以 `git diff HEAD` 将此前的其他本地改动归入本修复。

批准范围为 25 个文件，下面列出本次涉及位置。

| 类别 | 文件 | 必要性 |
|---|---|---|
| 生产 | `crates/zk-engine/src/engine.rs` | 根预算与请求准入；完成快照诊断投影；终态费用状态刷新 |
| 生产 | `crates/zk-engine/src/llm_ledger.rs` | 每次真实请求的未知价格预留与 NULL 结算；用量完整性 |
| 生产 | `crates/zk-db/src/message.rs` | 展示消息共享诊断投影，保持原始消息和分页身份 |
| 生产 | `crates/zk-db/src/session.rs` | 展示详情／恢复与会话、全局计价状态只读查询 |
| 生产 | `crates/zk-server/src/api/session.rs` | REST 展示入口使用同一投影 |
| 生产 | `crates/zk-protocol/src/server_message.rs` | 费用状态及仅刷新状态时省略 usage 的协议 |
| 生产 | `crates/zk-server/src/command/builtin/info.rs` | `/cost` 显示未知或未确认费用 |
| 生产 | `frontend/src/types/index.ts` | 费用状态类型与可选 usage |
| 生产 | `frontend/src/api/dispatch.ts` | 恢复计价状态及同 Run 错误去重身份 |
| 生产 | `frontend/src/store/costStore.ts` | 独立计价状态；状态刷新不覆盖既有用量 |
| 生产 | `frontend/src/components/layout/Header.tsx` | 未知／未确认费用可见提示 |
| 生产 | `frontend/src/store/selectors/turnSections.ts` | 根边界诊断生成稳定、去重展示尾部 |
| 生产 | `frontend/src/components/message/turn/turnUtils.ts` | 权威失败或中断诊断决定轮次状态 |
| 测试 | `crates/zk-engine/tests/engine_flow.rs` | 连续轮次、工具调用、子 Agent 及兼容 provider |
| 测试 | `crates/zk-engine/src/llm_summarizer_ledger_tests.rs` | 真实 SQLite 摘要、重试、缺 usage 和金额边界 |
| 测试 | `crates/zk-protocol/tests/roundtrip.rs` | 协议往返、旧载荷缺省未确认 |
| 测试／夹具 | `crates/zk-server/src/ws/restore.rs` | WS 恢复载荷诊断与独立计价状态 |
| 测试 | `crates/zk-server/tests/production_runtime_roundtrip.rs` | 真实 Rust／SQLite 完成及再次绑定；子任务完整性 |
| 测试 | `crates/zk-server/tests/rest_api.rs` | 详情、恢复和分页诊断一致性 |
| 测试 | `frontend/src/__tests__/api/dispatchRecovery.test.ts` | 完成／恢复、归属、费用状态和用量保留 |
| 测试 | `frontend/src/components/layout/Header.test.tsx` | 费用未知与未确认展示 |
| 测试 | `frontend/src/store/selectors/turnSections.test.ts` | 部分内容、诊断去重及轮次隔离 |
| 测试 | `frontend/src/components/message/turn/__tests__/turnUtils.test.ts` | 失败状态及权威中断覆盖同 Run 瞬时错误 |
| E2E | `frontend/e2e/production-backend.spec.ts` | 真实错误、完成快照、REST、刷新与页面状态 |
| 记录 | `docs/migration/unknown-pricing-and-run-errors.md` | 范围、行为、证据和未完成项 |

## 行为与边界

### 价格和用量分离

- 未登记模型和零费率内置模型均视为价格未知，允许执行；不猜测供应商单价或把订阅模型宣称为免费。
- 未知价格不做美元估算和按金额缩减输出。物理调用金额预留 0 表示没有可预留的已知金额，最终 `llm_calls.cost_nanos_usd` 保持 `NULL`。
- `usage_complete` 由实际 usage 决定。缺 usage、无效用量、保存失败继续阻止不安全的后续调用；Token、期限、取消和权限检查保持原链路。
- 独立摘要、合法重试和子任务复用现有 observer；没有新增请求执行路径或伪造物理调用记录。
- `sessionPricingStatus`／`totalPricingStatus` 为 `known`、`unknown`、`unavailable`。只查询已结束物理调用是否有未知金额，不新增精确次数。
- `known` 只表示未发现未知金额，不代表现有数字已全面对账；`unavailable` 不得当作已知零费用。现有进程内费用累加器及恢复时数字范围均未重构。
- 正常 `cost_update` 保留原响应用量；终态仅刷新费用和完整性时省略 `usage`，客户端保留原数字。用量完整性独立读取 Run／Task／根 Task，避免子任务缺 usage 时仅凭根 Run 宣称完整。

### 诊断只用于显示

- 以数据库消息归属连接同会话根 Run／TaskResult，不信任消息 metadata 中自称的 Run 或 Task 身份。
- `runtimeDiagnostic` 加到原有根任务边界的展示副本；不追加数据库消息，不更改正文、工具结果、消息 ID、计数或分页游标。
- 完成快照、REST 详情／恢复／分页及 WS 恢复共用投影；模型历史和原始导出保持不变。
- 前端生成 `runtime-diagnostic:<runId>` 展示身份，同 Run 瞬时错误被权威诊断去重；不同轮次、不同会话保持隔离，部分答复仍展示。
- 临时内容经原有 ContentStore 读取；本修复没有增加终态写正文依赖。诊断读取失败如实报告，不发送缺失诊断且看似成功的权威替换快照。
- 没有扩展所有终态样式或修改耗时计算。

## 红灯证据

日志位于上述临时目录，保留失败记录，不以编译失败代替问题复现。

- `ledger-red.log`：3 个真实失败，价格未知导致 usage 不完整／摘要未调用。
- `engine-red2.log`：根任务与子任务均因价格未知在请求前失败。`engine-red.log` 为测试编写阶段的编译错误，不计反例。
- `db-red-2.log`：2 个真实失败，恢复缺诊断与计价状态。
- `ui-red.log`：10 个失败；另有 `ui-terminal-precedence-red.log`、`ui-usage-preserve-red.log` 和 `ui-terminal-usage-red.log`，分别验证终态优先级、完整性保留和状态刷新不覆盖用量。

## 验收状态

目前不能声明全部完成：金额恰好耗尽的原子边界仍有一项真实失败，原因见下一节。没有忽略或删除该回归。

| 检查 | 实际结果 | 日志 |
|---|---|---|
| Cargo 格式 | 通过 | `cargo-fmt-final.log` |
| 相关四 crate Clippy `--all-targets -- -D warnings` | 最终测试断言修改后再次通过 | `four-crates-clippy-after-flow-tests.log` |
| DB／协议完整库测试与集成测试 | 275 通过 | `db-protocol-full.log` |
| Engine 库完整测试 | 601 通过、1 失败；为待确认范围的金额边界 | `engine-lib-final.log` |
| Engine flow 完整测试 | 66 通过；精确检查新增状态事件不重复提供 usage 或计费 | `engine-flow-status-adapted.log`；`engine-flow-final.log` 保留此前旧协议断言失败 |
| Server 库完整测试 | 593 通过、1 明确跳过 | `server-lib.log` |
| REST API 全部用例 | 8 通过 | `rest-all.log` |
| 真实 Runtime 往返 | 新增诊断与缺 usage 检查通过；并行执行 8 通过、1 失败；串行复验 9 通过 | `runtime-roundtrip-final.log`、`runtime-roundtrip-serial.log` |
| Rust 构建 `cargo build -p zk-server --locked` | 通过 | `server-build.log` |
| 前端 lint／构建 | 通过；构建保留大 chunk 警告 | `frontend-lint-final.log`、`frontend-build-final.log` |
| 前端完整单测 | 151 文件、1,350 项通过 | `frontend-tests-final.log` |
| 隔离真实后端 E2E | 11 项通过，包含新增失败持久展示 | `production-e2e.log` |
| 机器契约、公开凭据库检查、TaskRuntime 生成一致性 | 通过 | `contracts.log`、`generated-contracts.log` |

Server 跳过项为 `mcp_search::tests::dashscope_web_search_live`：需要真实 Key 和供应商网络请求，本次未执行，不计完成。Rust 测试链接器报告 `__eh_frame` 较大警告，构建及测试完成，未为消除警告扩大改动。

额外单独运行 `cargo clippy -p zk-engine --tests -- -D warnings` 时，在未启用生产服务所用图片预算特性的既有 stub `context/image_budget.rs` 报出 `missing_errors_doc`／`unused_async`。该文件未被本次修改；单 crate `--all-features` 和相关四 crate 的生产特性组合检查通过，不据此宣称所有特性组合均已通过。

Runtime 并行失败为 `production_incident_four_terminal_and_four_background_agents_are_durable_and_partitioned` 在完成前读到 1 个 Task 而期望 2 个；同源码串行复验通过。没有修改其调度实现或断言，保留失败证据，不能仅凭一次串行通过宣称并发稳定性已解决。

前端首次完整测试撞上新增终态 usage 回归的红灯阶段，并发现错误 metadata 对无归属事件增加了空身份字段；已修复为空身份不附加字段，再完整运行得到上述 1,350 项通过。首次日志 `frontend-tests.log` 保留。

E2E 使用现有 `npm run test:e2e:production` 隔离机制：独立临时工作区、SQLite、随机端口及本地 HTTP fixture，`reuseExistingServer=false`，测试截图输出到上述临时目录。未连接 5273／8082 开发服务；未调用付费模型。新增截图 `e2e-artifacts/production-backend-real-ba-382bc-mmitted-snapshot-and-reload/failed-run-durable.png` 已人工读取核对：刷新后红色失败状态、错误正文及费用未知提示均可见。

本次 E2E 实际 Rust 二进制 SHA256 为 `773829653d63f25d500e63fb144e611a211312729deb70916fa84b0d3a42b138`，测试前后身份相同。增量补丁及源码 SHA256 另存临时目录 `implementation-only.patch`、`implementation-sha256.json`；对起始 Git 差异进行逐文件比较，清单外 tracked 文件没有新增变化。

## 必须明确的金额边界

真实 SQLite 回归发现：物理请求的现有事务仅判断 `已知消耗 + 活跃预留 + 本次预留 > 上限`。本次未知价格预留为 0 后，当已知金额恰好达到上限时，摘要或重试可能仍被放行。根请求预检查不能取代该原子事务。

完整修复需要在 `crates/zk-db/src/runtime_ledger.rs` 同一事务中拒绝“现有消耗及预留已达到上限”。此文件不在原 25 文件清单内，已单独向用户确认是否纳入；批准前不修改。对应红灯回归 `unknown_pricing_cannot_bypass_already_exhausted_known_cost_budget` 已加入既定测试文件，未解决前不能宣称全部验收完成。

## 未并入的独立事项

费用数字仍沿用现有算法，包括进程重启、辅助请求和恢复范围的既有口径差异。本次只提供价格覆盖提示，未将这些数字重命名为全面校正的真实总费用。上述事项不在本次修复内。
