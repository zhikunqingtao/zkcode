# N1–N5 最小完整修复记录

日期：2026-10-08。基线：`main` / `298d2b30b3d310420c12d953bb4382ec8064d1bb` 及实施前全部本地改动。本记录只评估这五项修复，不代替整个迁移项目的发布审计。

## 工作区与范围

起始 1,451 个源码文件副本、逐文件 SHA256、Git 工作区/暂存区差异及 HEAD 保存在 `/tmp/zk-n1-n5-fix-20261008-165540/`。本轮增量以其中 `before/` 比较，不能将 `git diff HEAD` 中此前的改动归入本轮。

不修改数据库 schema，不重建数据库，不切换分支，不重启现有开发服务，不提交或推送。真实测试使用独立 SQLite、目录、端口、UDS、浏览器和本地供应商 fixture；不调用付费模型。

## 修复行为

| 项目 | 生产行为与必要边界 |
|---|---|
| N1 | 仅恢复普通持久会话中成功录制的 `VerifyJourney`，且 journal 义务精确为 `evidence`。从原工具结果、数据库归属和录制凭据重建证据；证据核验/保存与 journal 完成在同一事务。新 ID 稳定派生；旧随机 ID 完整等价时复用，保留人工判定。根/子任务与现有录制维护共用完成函数；恢复不执行 Journey、不自动恢复 Run、不解除故障隔离。 |
| N2 | 在现有 writer transaction 中计算已知消耗与有效预留；`charged >= limit` 或 `charged + reservation > limit` 时拒绝。有余额时未知价格仍可执行，金额预留为 0、结算为 NULL；合法请求允许恰好用完余额。 |
| N3 | 合并 `usage.pricingStatus` 独立于 `usageComplete`，在原聚合查询中读取所有 attempt 的已结束调用是否含 NULL 金额；原有 token/金额小计算法不变。未知费用与读取未确认分别显示，明确保存的零金额仍可视为 known。 |
| N4 | interactive 与 safe_dom 共用同步、缓存的安全元素投影，包含作用域根、普通实时值、原生/ARIA 控件状态及多选项。密码/隐藏输入值不读取；保留数量、长度和 partial 边界。Replay 沿用现有 safe_dom 展示。 |
| N5 | 目录身份判断与清理/注册在同一 directory 临界区；等待保留锁外。旧 ping、重连完成、OAuth 延迟返回及同 Arc 新 generation 不得清理新目录、修改新连接或调度。范围止于后端目录、binding、搜索目录和同步发布顺序，不包含独立异步 WS 帧排序协议。 |

N5 的 OAuth 用户授权完成回调与自动重连直接共享同一个 TOCTOU；因此必要修改包含 `manager/oauth.rs`。延迟重连调度及同 Arc 的正在重连标记同样携带现有 generation，并在原锁内核验；没有新增持久结构或外部 owner 协议。

## 反例和历史失败

日志均位于上述本机验证目录，失败保留，不以编译失败代替缺陷反例。

- N1：`n1-red.log`，成功 Invocation 与 pending journal 无法补齐证据、无法进入 ACK。
- N2：`n2-red.log`，已消费达到上限和活跃预留达到上限时，未知价格的零预留均被错误放行。
- N3：`n3-db-red-assertion.log` 缺失计价状态；`n3-ui-red.log` 5 项费用文字反例。`n3-db-red.log` 是早期夹具/编译阶段失败，不计为有效缺陷复现。
- N4：`n4-red.log` 与冻结基线复验 `n4-frozen-red.log`，11 失败、2 通过。
- N5：`n5-red.log`，旧 ping 返回后新连接被降级并清空目录。
- 首轮前端全量 `front-tests.log`：1,356 通过、1 项既有停止按钮测试触发 5 秒超时。独立复验 `front-timeout-recheck.log` 中该文件 34 项通过；保留首轮失败，不修改超时或产品实现掩盖它。

## 最终验收

N1–N5 的实现、反例回归及相关生产链路验证已完成。完整相关门禁不是首轮全绿：前端首轮有一项超时，Rust 首轮有一项既有夹具竞态；保留失败及未经修改的默认并发复验记录。另有下文列出的独立快照缺陷，因此本记录不判定整个仓库已达到可提交水准。

| 检查 | 实际结果 | 日志 |
|---|---|---|
| Cargo 格式 | 最终源码 `cargo fmt --all -- --check` 通过 | `cargo-fmt-final.log` |
| Clippy | DB / Protocol / Engine / Server / MCP 五个相关 crate，全部 target，通过；早期新增代码 lint 已修正 | `clippy-verified-final.log`；保留 `clippy-iteration.log`、`clippy-final.log` |
| 五 crate 默认并发测试（启用真实 Git 测试） | 首轮 74 个测试可执行文件：2,037 通过、1 失败、2 ignored；失败为下文的既有并发夹具 | `cargo-tests-final.log` |
| 首轮失败后未执行的集成测试 | 补跑剩余 22 个可执行文件：141 通过、4 ignored；4 项原生浏览器测试另行显式执行 | `cargo-tests-remaining.log` |
| Runtime 默认并发复验 | 同一 `production_runtime_roundtrip` 9/9 通过；未改源码、未改为串行 | `runtime-parallel-recheck.log` |
| 最终 Engine flow | 67/67 通过，覆盖最后一次仅测试分支顺序的 Clippy 修正 | `engine-flow-lint-final.log` |
| Doc tests | 五 crate 执行成功，实际 0 个文档用例；不计作功能覆盖 | `cargo-doc-tests.log` |
| Rust Server 构建 | `cargo build -p zk-server --bin zk-server --locked` 通过 | `server-build.log`、`server-build-final.log` |
| 前端 lint / 构建 | 通过；构建仍有现有大 chunk 提示，未借本轮调整打包 | `front-lint.log`、`front-build.log` |
| 前端完整单测复验 | 151 个文件、1,357 项通过；首轮超时如前述保留 | `front-tests-final.log` |
| 契约 / 生成一致性 | parity、公开演示凭据数据库、TaskRuntime 生成器检查通过 | `contracts.log`、`contracts-generator.log` |
| 隔离真实 Rust 后端 E2E | 12/12 通过，包含真实合并已知/未知计价状态、组件文字及刷新恢复；前后端使用私有测试端口，未访问开发服务 | `e2e-production.log`、`e2e-artifacts/` |
| 受影响 Python / Chromium 回归 | 浏览器、Journey、录制等相关模块共 192 项通过，包含 N4 新增的 13 项真实 Chromium 用例 | `python-browser-all.log` |
| 原生 Rust → Python UDS → Chromium 故障回归 | 4/4 显式 ignored 测试通过。子进程及外层日志会重复汇总，不能算成 8 项 | `n1-native-verified-final.log` |

本轮的 N1 SQLite 专项为 11/11；真实 Engine 回归覆盖根/子执行、可信工具限制、证据插入和 journal 更新失败。原生回归要求 Journey 本身 `verified`，证明真实浏览器副作用只发生一次、数据库重新打开后恢复、ACK 已消费但回复丢失后再次确认、容量释放。最初夹具误用断言字段 `text` 而非 `expected`，强断言揭示后仅修正测试夹具；失败保留在 `n1-native-final.log`，不能将原来没有检查业务 verdict 的旧通过结果当最终证据。

N2/N3 的 DB 回归包含根/子、活跃预留并发、余额边界、全部合并 attempt、NULL/明确零金额与 usage 独立性；N3 组件文件 30 项通过。N5 最终 9 项受控并发回归和 MCP 库 236 项通过、1 项 ignored；真实 Server registry、调用 binding 和搜索目录回归也在五 crate 测试中通过。

上述重复执行的用例不累加成唯一覆盖数量。仍未执行 macOS Keychain 写入回归、真实 DashScope 外部调用（常规测试的 2 项 ignored）；没有付费模型、真实外部 OAuth 服务验证。此次也未重跑完整工作区所有 crate、无关 Python 模块、Office / 五类 LSP 全套原生门禁或依赖安装/安全全量审计，不能沿用以前的通过结果将这些记作本轮通过。

### 源码与构建身份

- 起点 HEAD 及最终 HEAD 均为 `298d2b30b3d310420c12d953bb4382ec8064d1bb`，分支保持 `main`。
- `final-full-source.json`：1,374 个非 docs 的 Git 已跟踪/未忽略文件及内容 SHA256，包含 `.mjs` 测试夹具；该 JSON 的 SHA256 为 `73bf88fe75b7954ab4da55d7461767fad98ff7cba821d9e22bbc653dbd731188`。
- 仓库原有脚本生成的 `final-source.sha256` 清单自身 SHA256 为 `ebaeb11d9c40af8f7295492a682d92ba941abf0649635318987a563baf83a4d4`。它与上面的完整文件清单并存，不将其未纳入的扩展名假定为已覆盖。
- 首次冻结清单 `frozen-full-source.json` 的 SHA256 为 `671be2679a5fac8ad68b67d9fe9da94c44bde4e51c1d41a85e0e7e39b5ec0974`。最终源码仅比它多一处 `engine_flow.rs` 测试分支顺序调整，以通过 Clippy；逻辑等价并已重新执行 67 项测试，生产源码未再变化。
- E2E 使用、最终标准构建恢复的 `target/debug/zk-server` SHA256：`3d620a68d63c6c8604788cbf387f1b73269684488302f777c9865c43c2a2d338`。E2E 明确检查运行前后该身份相同。多 crate 测试过程生成过不同 feature 组合的二进制，最终重新运行标准构建并核对为上述 E2E 身份。
- `implementation-only.patch` 与 `implementation-files.json` 保存相对实施前快照的本轮差异及逐文件摘要，不混入此前本地改动。
- 既有后端 PID 2579（10 月 7 日 19:09:01 启动）与 Vite PID 93669（10 月 7 日 18:37:38 启动）未重启；编译生成的新二进制未替换正在运行的进程。

## 增量文件清单

本轮共 24 个文件：15 个生产文件（部分含同模块回归）、7 个测试/夹具文件、2 个记录文件。新增生产文件只有窄的 Engine 录制完成模块；新增独立测试文件只有浏览器安全投影回归。

| 对应项 | 文件 | 必要性 |
|---|---|---|
| N1 | `crates/zk-db/src/browser_recordings.rs` | 受限结果读取、完整凭据核验、事务完成 |
| N1 | `crates/zk-db/src/evidence.rs` | 复用事务内证据保存与原始证据读取 |
| N1 | `crates/zk-db/src/lib.rs` | 导出受限快照类型 |
| N1 | `crates/zk-engine/src/verify_journey_postprocessing.rs`（新增） | 根/子/维护共用的 typed receipt 完成入口 |
| N1 | `crates/zk-engine/src/lib.rs` | 导出窄完成入口 |
| N1 | `crates/zk-engine/src/engine.rs` | 两处可信工具路径接线，避免第二次 CAS |
| N1 | `crates/zk-server/src/python/tools/browser_recordings.rs` | 原维护中先补证据再 ACK；持久化故障/重启/并发测试 |
| N1 | `crates/zk-engine/tests/engine_flow.rs` | 真实 Engine 根/子、信任检查及事务失败回归 |
| N1 | `crates/zk-server/tests/verify_journey_native.rs` | 真实 Chromium 成功、副作用一次、ACK 丢回复和容量回归 |
| N2 | `crates/zk-db/src/runtime_ledger.rs` | 原子金额边界；根/子及并发预留回归 |
| N3 | `crates/zk-db/src/session_merge.rs` | 原聚合查询增加独立计价状态与跨 attempt 回归 |
| N3 | `frontend/src/store/sessionMergeStore.ts` | 复用 PricingStatus 类型，允许旧载荷缺失 |
| N3 | `frontend/src/components/session/SessionMergePanel.tsx` | 真实费用状态展示，不把未知当零 |
| N3 | `frontend/src/components/session/SessionMergePanel.test.tsx` | 未知/混合/零/缺失状态与 usage 独立性 |
| N3 | `frontend/e2e/production-backend.spec.ts` | 已知与未知模型真实合并 API/组件/刷新验收 |
| N3 | `frontend/e2e/support/scripted-openai-provider.mjs` | 本地 fixture 接受内置零价模型 |
| N3 | `scripts/testing/start-production-e2e-server.sh` | 仅隔离 E2E 注册该模型，沿用现有隔离机制 |
| N4 | `python-service/src/services/browser_service.py` | 一次同步共享安全投影 |
| N4 | `python-service/tests/test_browser_semantic_projection.py`（新增） | 13 项真实 Chromium 状态/密码/截断回归 |
| N5 | `crates/zk-mcp/src/manager.rs` | 目录锁与两层代际核验、调度归属及可控竞态测试 |
| N5 | `crates/zk-mcp/src/manager/services.rs` | 原开关路径复用持锁清理 helper |
| N5 | `crates/zk-mcp/src/manager/oauth.rs` | 延迟 consent 不越过当前 owner；原合法空连接授权继续可用 |
| 记录 | `docs/migration/n1-n5-repair.md`（新增） | 本轮范围、源码身份、证据和边界 |
| 记录 | `docs/migration/unknown-pricing-and-run-errors.md` | 追加后续修复链接，保留历史失败 |

## 适用边界与未并入事项

- N1 的真实故障会留下 Task=`needsAttention`、Run=`interrupted`。恢复后录制 batch 保护消除、pending 录制为空、容量释放；任务隔离保持。通用会话删除仍可能被未处理任务拦截，不能声称整个会话自动可删除。本轮不增加 abandon 功能。
- N1 不恢复其他工具的 Artifact/Research 后处理，不重放有副作用动作，不把临时正文转为持久内容。
- N1 恢复要求原始工具结果与 journal 尚存在。下述快照删除会破坏这一前提，不能声称本次恢复能够重新构造已经删除的原始事实。
- N2/N3 不重构全局费用算法，未知价格仍未知，`known` 仅表示未发现已结束调用的未知金额。
- N4 不实现完整 ARIA 引擎，不恢复会泄露密码的原生 ARIA 快照。
- N5 不保证独立异步 WebSocket 帧最终到达顺序；执行目录、工具 binding 与搜索目录由后端本次锁/代际检查保证。

## 独立发现：只记录，未并入实现

### 快照恢复缺少运行/录制门控（已真实复现）

> 后续状态：此处保留 N1–N5 当轮的发现与证据。该缺口已在后续[快照恢复保护修复](snapshot-shadow-repair.md)中处理；修复限定实际消息依赖，不一律阻止所有未 ACK 或子会话录制。

生产 `POST /snapshot/resume` 在 Task 为 `Running` 和 `NeedsAttention` 时均可返回 200。`Db::restore_session_snapshot` 删除当前会话消息，外键继而删除 tool result journal；成功 Invocation 与 released / sealed 录制仍在。此后 N1 受限读取返回 `Ok(None)`，原结果不再可供验证和恢复。普通会话删除在相同状态下会被 idle 门控拒绝，但快照恢复入口没有对应门控。

证据：`n1-snapshot-cascade-probe.rs` / `.log`。使用生产 Router 的 HTTP 请求处理、内存 SQLite、公开运行时 DB API 和私有工作区构造两个状态；恢复前消息/journal 各 1 条，恢复后均 0。未篡改 SQL，未调用开发服务。该微复现使用自有简化 recording 元数据，没有执行浏览器或重新验证 blob；它证明的是入口与删除行为，不替代上述真实浏览器回归。

相关 `crates/zk-server/src/api/session_snapshot.rs` 与 `crates/zk-db/src/session.rs` 与本轮起始快照完全相同。建议另行在快照恢复的现有事务链加入运行及未完成录制保护，并测试并发，而不是让 N1 猜测已经删除的内容。本轮未修改这些文件，也未将此问题标为已解决。

### 八子任务测试的请求计数依赖未保证的时序

首轮 `production_incident_four_terminal_and_four_background_agents_are_durable_and_partitioned` 在 `child_calls` 断言得到 7，期望 8。此前八组任务成功、归属、usage 和预算断言均已通过。

测试只等待 4 个 provider 调用的 barrier，却据此向后台子任务发送 follow-up；Running 状态发生在 executor 启动之前，不能证明八个子任务都已发出首次请求。较晚启动的子任务允许在首次模型请求前消费 inbox。夹具按最后一条 User 分类请求，此时会把该首次请求计入 follow-up，跳过 `child_calls`；这是可达的测试计数竞态，并非八个任务中有一个没有执行。该次失败没有完整请求轨迹，因此不能声称已确定具体是哪一个子任务。

测试文件、inbox consumer、provider registry 与本轮起始快照字节一致；本轮 Engine 差异不涉及该调用链。原默认并发的 9 项复验通过，但不抹去首轮门禁失败。建议另行按每个后台任务的首次请求边界同步、在 4 个并发槽内分批放行；不能直接改成 8 人 barrier，否则可能被四个占槽请求阻塞。此次未放宽断言、未调整生产调度或改为串行。
