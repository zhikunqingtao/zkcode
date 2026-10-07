# Query 与 CLI 实施记录

> **2026-10-07 F1–F5 修复更新：** F5 将 Query 与根任务的同一绝对期限贯通到停止首因、SQLite 和终态投影，取消不再被展示为正常 end_turn；已提交原始模型事实不改写。 最新验证见 [修复记录](f1-f5-fixes.md)、[回归定位表](f1-f5-regression-map.md) 和 [门禁](full-alignment-gates.json)。下方较早的通过数量、状态和构建身份保留为历史，不代表本轮验证。

固定源 `053adf90`，沿用当前 Rust Engine / TaskRuntime / SQLite / 物理请求费用账本。

> 历史追加状态（F1–F5 修复前）：修复了供应商适配器静默覆盖 `thinking=disabled`。修复后严格 Clippy、9 项实际 wire、全工作区 **145 个独立 harness / 3143 passed / 0 failed / 9 ignored**、无默认 features Engine **16 个 harness / 696 passed / 0 failed / 0 ignored** 已通过；新 release 与唯一真实收费探针也已通过。最终工作区日志 `/tmp/zk-alignment-thinking-workspace-final.txt`，SHA-256 `e65109d7113495aded4bfe36014dfc575a2717160d8c99776a6a792d7deed7eb`。详见 [运行时追加记录](full-alignment-runtime.md#2026-10-07-追加修复显式思考开关修复后回归已通过)。下方 3138 项是修复前快照，保留历史。 最终二进制 SHA-256 `cca0d0a64e2ab3bad9bf95fdc22fcb02f278d89c8b9e78286d4c0960a89e3251`，同二进制 E2E 6+3 项及 doctor 37 项检查通过。官方 DeepSeek 唯一实际请求的 32 输入/2 输出 token 与物理调用、Run、Query 三层一致，账本费用 0.000012 美元；增量先于终态、无 thinking、清理确认、进程退出和凭证无落盘均验证。完整结果及零转发准备失败见运行时顶部索引。 持久证据：[deepseek-flash-release-probe.json](deepseek-flash-release-probe.json)，SHA-256 `1c3140fbfed1850e604f92ed77d8e17750378907d128b4cd82861191ae653fea`；仅白名单元数据，不包含凭证或模型正文。

## 修复前验证索引（历史快照，2026-10-07）

Query/CLI 请求选项、SSE 活跃恢复、原子 JSONL、fork、运行专用 MCP 配置、结构化输出和临时正文策略已接到生产执行链。最新 Rust 全工作区 **145 个独立 harness、3138 passed、0 failed、9 ignored**；日志 `/tmp/zk-alignment-workspace-tests-final.txt`，SHA-256 `d7e0c4cc106318b9d13f2b82b5730d6211d3bc4d052372327130028c96c8dc98`。最新严格 Clippy 通过，日志 `/tmp/zk-alignment-clippy-final4.txt`，SHA-256 `8c78a6cb8c4dad5a388de0bee810a2412abcabeb71b94ca219807c182ab3cd49`。

原始日志含 146 条 test result / 3139 passed，其中一条为工具库 self-reexec 子测试，已在外层 394 项中计入，以上已去重。

该结果包含 Query/取消/恢复/临时存储相关实际 HTTP→Engine 测试；九项环境或真实远端专项跳过不计为通过，Python、原生专项、release 构建、真实前后端 E2E 和 doctor 等按主任务统一门禁分别记账，当前不据此宣称完整发布验收通过。

临时模式在输入持久化之前绑定有界 RAM scope，生命周期覆盖执行、清理和结果投影。费用、usage 与归属仍持久；根/attached Agent/Shell、工具/交互/快照/证据各域采用同一保留策略。临时会话不可续接/fork，不创建持久团队队列、detached 任务或长期服务，不写持久记忆；用户明确创建的普通输出文件仍保留。未知费用、清理失败和过期正文分别保持真实状态。

下文按实施时间保留记录。早期“尚未完成”“待复跑”及阶段性失败属于历史快照；对应修复后的当前结果以本索引及后续明确日志为准，不能把旧通过计数或旧待办当作当前整体结论。

## 已接生产路径

- `api/query.rs` 的三种传输共用 `ConversationService`。先取得会话执行锁，再应用运行参数，直至结果投影结束释放。忙碌请求在发送 SSE 前返回 409。
- `query_stream.rs` 订阅现有 WsHub 的归属事件，保留 V4 身份和事件 ID；有界队列溢出如实报错并停止执行。实时文本、思考、工具、压缩及 usage 事件先行，最后由不可变 TaskResult 和 Run 费用账本生成 `result` / 唯一 `complete`。
- Query API 默认 1024 轮、系统根任务期限；CLI 默认 99 轮、300 秒。金额限制按纳美元安全转换，继续取请求和系统约束中的严格者。
- 请求专属 lease/cancellation 避免旧停止打断新执行。取消接口只确认停止请求和阻止准入，不声称清理已完成。内存中的有界 requestId 索引及 SQLite `query_requests` 墓碑不包含正文或正文哈希，覆盖取消先于 Query POST 到达及重启后重复 POST；先占请求身份再新建/fork，会话接线后再开始 Hook/执行，客户端不自动重发 Query。
- 新建 CLI 绑定当前目录 Project；远程连接须显式 Project。新会话 DONT_ASK，续接读取持久权限，显式冲突失败。
- 工具候选集与显式 allowlist 相交再扣除 denylist；显式空集合保持空。未知选项、非法工具和矛盾模型配置不被静默吞掉。
- JSONL 仅接受用户文本消息，拒绝伪造角色和控制帧；EOF 后执行一次，按原顺序原子写入同一 Task/Run 下的独立消息。单次输入上限 1 MiB / 256 条消息。
- Python SSE 解码支持增量 UTF-8、CR/LF/CRLF、注释和多行 data，在线路分帧前限制缓存；缺终态、坏 JSON、截断及终态后的数据都失败。Ctrl+C 发送精确 requestId 取消并退出 130。
- `thinking` / `effort`、本次 fallback 链和 stop sequences 已接统一请求选项事实源。`includePartialMessages` 只影响流展示。标题与当前会话模型变更沿用既有持久配置入口。
- `jsonSchema` 使用有界本地 JSON Schema 校验，拒绝远程引用；首次最终答案无效时在原期限、轮次和金额预算内允许一次禁用工具的格式修复，原答案保留，每次实际请求计费。第二次无效、截断或尝试调用工具均如实失败。
- `mcpConfig` 支持 CLI 内联 JSON 或有大小上限的 UTF-8 配置文件。实际 STDIO 连接属于 RunToolScope invocation，复用资源准入和清理链；配置与秘密只存在内存。attached 子任务继承工厂并建立自己的资源归属，detached 任务采用宿主默认目录。
- `forkSession` 接通封存快照事务，保留权限、实际原文与图片身份，生成独立消息 ID，不复制活跃执行和一次性授权。发送给模型的历史用户消息有明确历史引用前缀，持久原文不改写。
- 临时 ContentStore 基座使用有容量上限的内存正文和随机引用，选择策略后不可改为持久或临时。已适配消息、标题/摘要、Task 配置、结果/receipt、Inbox、Run 事件及取消诊断；工具/交互/快照/工作台/证据等其它域继续分组验证。SQLite BEFORE guard 拒绝未适配的原始正文写入。**Query 已接收 `noSession`，在接收/保存输入前创建有界 RAM 内容所属范围，并持有至执行、清理和结果投影结束；域级验收继续进行，不能把一次纯文本通过当作完整隐私门禁。**

## 历史阶段验证

| 检查 | 本轮结果 | 范围和限制 |
|---|---|---|
| Python CLI 项目/权限/SSE/JSONL/MCP 配置专项 | 67 passed | `/tmp/zk-cli-latest-tests.txt`；真实分帧、CLI调用与本地HTTP fixture，不包含真实供应商 |
| Rust Query API 专项 | 5 passed | `/tmp/zk-query-runtime-tests.txt`；后续新增字段仍须重跑 |
| Rust Query SSE/Fork 生产组合 | 5 passed | `/tmp/zk-query-stream-current.txt`；文件 SQLite + 真实 HTTP Router + Engine + ProviderRegistry 本地 fixture；增量先于终态、并发拒绝、真实费用投影、取消及迟到停止、JSONL身份、fork权限/历史投影/独立执行 |
| 运行专用 MCP 真实 STDIO | 1 passed | `/tmp/zk-query-mcp-scope-current.txt`；Python STDIO fixture真实调用，Run/Invocation/资源归属、物理清理、独立目录和SQLite/WAL无配置秘密；其后临时存储改动仍须全量回归 |
| RunTermination 专项 | 5 passed | 同上日志；含取消保存失败后的停止和对账 |
| JSONL 原子批次 | 2 passed | `/tmp/zk-ephemeral-foundation-tests.txt`；完整回滚、独立顺序身份和伪造归属拒绝 |
| JSON Schema 校验器 | 2 passed | `/tmp/zk-structured-output-tests.txt`；真正 schema 约束和本地引用 |
| JSON Schema 实际引擎 | 4 passed | `/tmp/zk-structured-output-engine-tests.txt`；两次物理费用、预算/轮次约束、无工具修复和原始答复不改写 |
| Fork 封存事务 | 5 passed | `/tmp/zk-fork-db-focused.txt`；权限/图像/独立身份、幂等、失败回滚和删除后不复活 |
| 内存正文、TaskResult/Inbox/事件、真实 DB/WAL 扫描 | 4 passed | `/tmp/zk-ephemeral-runtime-boundary3.txt`；正文和SHA不落库、原有持久行为、过期后取消/重启/费用仍可查询；之后新增artifact/research专项待跑，不代表完整隐私验收 |
| SQLite 层整体回归 | 154 passed | `/tmp/zk-db-retention-regression.txt`；含 MemoryContentStore、持久请求重启防重、fork、合并、预算和不可变证据。后续正文域改动需各自重跑 |
| JSONL 原子输入复测 | 2 passed | `/tmp/zk-ephemeral-runtime-boundary3.txt` |

## 历史阶段待办（后续接线及回归已更新）

- 真正临时执行的所有正文域接线、无正文/正文哈希持久审计、并发容量、工具/交互/子任务及文件缓存扫描。
- 外部 MCP context 真实创建/读取/权限/取消已通过工具工作线专项；STDIO CLI最后fixture断言修正后仍待复跑，详见工具记录。
- SSE 活跃内存恢复与 HTTP 重复新建拒绝已通过；无人重连停止已在生产组合复测通过；容量、旧游标与并发读者专项及完整发布门禁单列记录。
- 请求级输出选项与实际模型能力的扩展回归；全量 Cargo/前端/Python/安装/依赖门禁。

以上是当时待办快照；对应生产接线与 Rust 回归已由顶部索引更新。完整发布门禁仍单独记录，不由阶段用例替代。


## 2026-10-07 本次执行记录

新增生产路径：

- `GET /api/query/{requestId}/stream` + `Last-Event-ID`/`after` 仅恢复仍在运行的执行。每执行 8 MiB/4096 条事件，全局 64 MiB、32 执行、每执行 8 个读者；序号不复用，原 Task/Run/Invocation 身份保留。过期游标明确 409，结束或重启后 410；绝不重新 POST Query。
- 最后一个流读者断开后保留 15 秒重连宽限，再停止精确所属执行；网络断开探测还受 10 秒 SSE 心跳周期影响。停止接口与 Ctrl+C 仍立即请求停止。原 25 秒测试等待未覆盖探测+宽限+清理；等待上限改为 40 秒后，在真实网络断开/Engine 停止链路中复测通过，没有增加生产宽限或取消延迟。
- Python CLI 在线路中断/无终态时只允许有总期限的两次 GET 恢复，不会重发输入；坏 JSON、错误终态及 410 如实失败。
- 临时 Query 拒绝指定已有会话、fork/续接；普通用户授权生成的文件保留。费用元数据可在 RAM 正文过期后读取。
- 清除内置 `publish-oss` 和专用引用，同名用户 Skill 的正常注册仍保留（新增测试待运行）。

实际运行：

| 命令/范围 | 结果 | 证据与限制 |
|---|---|---|
| Python CLI 项目/流完整性/恢复专项 | 67 passed | `/tmp/zk-query-cli-reconnect-tests.txt`，POST 恰好一次、GET 游标恢复、过期不重发 |
| Rust Query API | 5 passed | `/tmp/zk-query-sse-reconnect.txt` |
| Rust Query SSE | 8 passed，1 failed | 同日志；活跃重连、重复 POST、新建/fork、取消、JSONL、临时 Query 都通过；无人重连停止计时仍待修复/确认 |
| 临时 Query 真实 HTTP/Engine/SQLite | 上述 8 项中的 1 项通过 | 实际执行期间内容可读，结束后失效、费用保持，扫描 DB/WAL/自动快照/fixture目录无私有正文及正文 SHA；只覆盖该 fixture 已执行操作 |
| 独立 MCP 上下文、STDIO、运行私有配置 | 各 1 passed | 同日志，真实 Python STDIO 子进程及真实 Rust Router，EOF 等待清理，未访问付费供应商 |
| DB 临时内容扩展 | 5 passed | `/tmp/zk-db-content-final.txt`，含文件产物与研究来源 |
| Engine 临时 Shell | 2 passed | `/tmp/zk-temporary-shell-engine.txt`，根执行与 attached Shell 子任务，cwd 复用、命令不写临时文件、正常用户文件保留 |
| Engine MCP/REPL service root | 5 passed | `/tmp/zk-engine-repl-root-service.txt`，普通内部服务与聊天并行、独立 transcript，临时模式拒绝长存服务 |

此后还有 REPL/MCP、产物终态、团队 Inbox 和调用入口改动；完整合并回归仍需重新执行。


## 历史组合验证（2026-10-07，最终回归见顶部索引）

`/tmp/zk-server-alignment-combined3.txt` 中 Query API **5 通过**、运行私有 MCP scope **1 通过**、Query SSE/普通 REPL **10 通过**。日志同时包含其他能力的真实失败，不能据此称整个服务端门禁通过。

新增普通 REPL 整链使用真实 HTTP Query → Rust Engine → Python 解释器：两轮调用分别得到 `42`、`43`，使用同一受管服务 Run；首轮完成后状态保留。活跃时删除/合并均返回 409，另一个会话停止被拒绝，停止后进程资源实际释放并可正常删除。普通模型请求费用仍归属各自 Query，内部 REPL transcript 不混入用户聊天。

该组还验证首增量先于模型完成、活跃 GET 恢复与终态 410、重复 POST 不创建第二个执行、无人重连停止、取消先于请求、JSONL 消息边界、fork 和临时正文/正文哈希不出现在 SQLite/WAL/自动快照。恢复不重新 POST Query。纯文本隐私测试覆盖其实际操作范围，完整工具/交互/证据隐私门禁另列。
