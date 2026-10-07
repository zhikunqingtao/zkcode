# 完整能力对齐：运行时实施记录

> **2026-10-07 F1–F5 修复更新：** 摘要最多重试一次必须以上一物理请求可结算且预算/期限允许为前提；未知 usage 的 429 保持未知费用并阻止后续付费调用。本地回退不表示账本或整个聊天已恢复。F5 的超时/用户取消首因及持久终态投影见修复记录。 最新验证见 [修复记录](f1-f5-fixes.md)、[回归定位表](f1-f5-regression-map.md) 和 [门禁](full-alignment-gates.json)。下方较早的通过数量、状态和构建身份保留为历史，不代表本轮验证。

本记录区分已接线实现、测试实际结果和未完成工作。不以模块、类型或测试存在作为能力完成证明。

## F1–F5 修复前运行时验证索引（历史，2026-10-07）

本次修复后的严格 Clippy、全工作区 **145 个独立 harness / 3143 passed / 0 failed / 9 ignored**、Engine 无默认 features **16 个 harness / 696 passed / 0 failed / 0 ignored**、provider wire **9 passed** 均已通过。下方追加记录给出各自日志与 SHA；九项跳过仍未计入通过数，不借本次单模型探针宣称其它供应商通过。

最终 release 构建通过，`target/release/zk-server` SHA-256 为 `cca0d0a64e2ab3bad9bf95fdc22fcb02f278d89c8b9e78286d4c0960a89e3251`。主任务在同一 SHA 上完成真实前后端 E2E **6+3 项通过**、正式同步与 doctor **37 项检查通过**；本工作线随后完成唯一一次真实 DeepSeek Flash 公开请求，未再改 Rust 源码。

真实收费探针结果：

- 固定公开 system/user `pong` 提示，无用户文件或历史；`tools=[]`、thinking disabled、无 fallback/辅助模型。90 秒期限、1024 token 总预算、0.05 美元上限。
- 本机透明闸门仅将 **1 次**原始请求转发至官方 `https://api.deepseek.com/v1/chat/completions`，没有改写请求正文/SSE、重试或第二次物理请求；这是本机闸门加官方 TLS 上游验证，不宣称直接客户端 TLS 链路测试。
- 官方 SSE model=`deepseek-flash`，输入 **32 tokens**、输出 **2 tokens**、cache 0，与 `llm_calls`、Run 和 Query usage 完全一致且 usage complete；对应本地金额账本 **12000 nanos USD = 0.000012 美元**。这是实际请求的账本费用，未声称对账供应商发票。
- 首个文本增量 404 ms，终态 562 ms，`text → result → complete` 顺序正确；结果为 `pong`，无 thinking 增量。Run `completed/modelFinished`、cleanup `confirmed`、无未释放资源、实际工具调用 0。
- 服务优雅退出码 0，所属进程组消失，二进制 SHA 前后不变，probe 自有文件扫描未发现凭证明文。

证据：已脱敏报告原样保存为 [`deepseek-flash-release-probe.json`](deepseek-flash-release-probe.json)，临时原报告 `/private/var/folders/g_/cgkxr_w91xg7tx8n84hjt9zm0000gn/T/zk-deepseek-release-c7naj5q2/probe-report.json`，SHA-256 `1c3140fbfed1850e604f92ed77d8e17750378907d128b4cd82861191ae653fea`；执行日志 `/tmp/zk-deepseek-release-probe-execution4.txt`，SHA-256 `28c23faedd57dab12913c65d66732d7656157973d6b6856ee99c575e70ab5be4`；可重复探针脚本 `/tmp/zk-deepseek-release-probe.py`，SHA-256 `c2af5a56be20a9cd8e9539944e196847564438d4b6e651b580261f6982afdfca`（执行需显式付费标志，未再次运行）。

零收费的准备失败保留在 `/tmp/zk-deepseek-release-probe-execution.txt`、`execution2.txt`、`execution3.txt`：首次把 health 的 text/plain 当 JSON；随后缺可信本机 Origin，被生产保护明确拒绝。三次均 gate forwarded=0、物理账本调用=0，优雅退出且无凭证落盘；只修探针 fixture，未放松生产权限。第一次真实转发即通过。

未提交、推送、切分支或创建 PR。已有配置、默认开关与明确排除项保持原约定。

## 2026-10-07 追加修复：显式思考开关（修复后回归已通过）

DeepSeek 发布探针的出站契约核对发现：适配器会把显式 `thinking=disabled` 改成 `enabled/max`。尚未向收费端点发送请求；本轮修正实际协议映射，不放宽探针条件。下列新改动不包含在后文较早的 3138 项结果中；修复后的严格 Clippy、wire、workspace 与无默认 features Engine 已重新通过，release 与唯一真实收费探针随后也通过，详见顶部最终索引。

- DeepSeek 文本与 Kimi（含 Kimi Code `k3`、`kimi-for-coding`）按本次开关发送 `thinking.type`；Adaptive 保留原 max 默认，显式 effort 与独立摘要设置仍分别验证。依据 [DeepSeek 思考模式](https://api-docs.deepseek.com/guides/thinking_mode/) 和 [Kimi Code 模型说明](https://www.kimi.com/code/docs/kimi-code/models.html)。
- 已有 Qwen 3.6/3.7/3.8 路由在 Disabled 时明确发送 `enable_thinking=false`；已知强制思考的 GLM 5.3 / 5.3 Flash，以及百炼 GLM 5.3 对关闭请求在发 HTTP 前拒绝。依据 [百炼思考模式](https://www.alibabacloud.com/help/en/model-studio/deep-thinking) 和 [百炼 GLM](https://www.alibabacloud.com/help/tc/model-studio/glm-zhipu)。
- OpenRouter 显式 Disabled 发送 `reasoning.enabled=false`，保留 `require_parameters`，不把隐藏思考输出误作关闭。依据 [OpenRouter reasoning 参数](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens)。该本机 wire 验收不证明每个聚合提供商都支持关闭，真实拒绝继续向上返回。
- 直连 OpenAI `gpt-5.6-sol` 明确发送 `reasoning_effort=none`；官方已明确不支持关闭的直连 `gpt-6-astra` 在 preflight 拒绝。保留 Adaptive 和既有 namespaced/Responses 行为，Responses 已发送 `reasoning.effort=none`，本次仅补 wire 断言。依据 [Chat Completions 参数](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)、[GPT-5.6 Sol](https://developers.openai.com/api/docs/models/gpt-5.6-sol) 和 [OpenAI reasoning 支持范围](https://developers.openai.com/api/docs/guides/reasoning?api-mode=chat)。未扩大旧版/自定义模型的能力菜单。

新增实际 loopback HTTP 测试同时覆盖启用/关闭/摘要 override、强制模型零连接拒绝、原始工具/opaque 历史，以及原 max 默认；尚未将文件存在计为测试通过。唯一真实收费探针待新 release SHA 后才执行，且最多转发一次公开 pong 请求，不发送用户文件；实际结果见顶部。

首轮严格 Clippy 通过；首轮新 workspace 仅新增 `provider_routing_wire` 两项失败（该 target 7 passed / 2 failed）。失败原因是 fixture 的两个适配器 ID 没有内置能力记录，保守 context 与默认输出额度相等，正确触发 `INVALID_MODEL_BUDGET_CONFIGURATION`。仅将该两项合成请求的输出额度明确设为 512，未改生产上下文保护或猜测型号价格；修复后的统一回归待执行。日志 `/tmp/zk-alignment-thinking-clippy.txt` 与 `/tmp/zk-alignment-thinking-workspace.txt`。

修复后实际 loopback wire 专项 **9 passed / 0 failed / 0 ignored**（0.03 秒），日志 `/tmp/zk-alignment-thinking-wire-final.txt`，SHA-256 `dac7806e1c89d06792ca460904685f038197cef944811742769bd7f8b960cde8`。这包含所有本次新增用例和 Kimi / OpenRouter 历史重放、Responses `none` 原有用例的加强断言；仍不等于远端供应商实测。修复后全工作区 **145 个独立 harness、3143 passed、0 failed、9 ignored**，日志 `/tmp/zk-alignment-thinking-workspace-final.txt`，SHA-256 `e65109d7113495aded4bfe36014dfc575a2717160d8c99776a6a792d7deed7eb`。Engine 无默认 features **16 个 harness、696 passed、0 failed、0 ignored**，日志 `/tmp/zk-alignment-thinking-engine-no-default.txt`，SHA-256 `f845d33b649b9dee312887244c22db0efba8a0b6f6c5b68a334ba1b83f3039d4`。最新严格 Clippy 日志 `/tmp/zk-alignment-thinking-clippy-final.txt`，SHA-256 `2792530b7b1a1f84d2b6c263c980b0e8fc7240d1dbd0a8703b65e893b1cabd78`。新 release 与真实公开请求已在顶部最终索引记录。

## 功能状态索引（思考开关追加修复之前的回归快照）

**当前 Rust 全工作区测试与严格 Clippy 已通过；完整发布验收仍须等待 release 构建、真实前后端 E2E、doctor 等统一门禁。** 下文按实施时间追加，早期“待实施”“待执行”“gate 保持关闭”均是当时快照；不能作为当前实现状态。用户开关及全部排除项继续不变；本轮没有提交、推送或创建 PR。

最新整套测试日志 `/tmp/zk-alignment-workspace-tests-final.txt` 实际为 **145 个独立 harness、3138 passed、0 failed、9 ignored**，SHA-256：`d7e0c4cc106318b9d13f2b82b5730d6211d3bc4d052372327130028c96c8dc98`。严格 Clippy 日志 `/tmp/zk-alignment-clippy-final4.txt` 通过，SHA-256：`8c78a6cb8c4dad5a388de0bee810a2412abcabeb71b94ca219807c182ab3cd49`。原始日志含 146 条 test result / 3139 passed，其中一条为工具库 self-reexec 子测试，已在外层 394 项中计入，以上已去重。

九项跳过是四个真实 LLM、DashScope 搜索、原生 Keychain、两个原生 Journey 和原生 LSP；不计入上述通过数，原生专项是否已独立运行需参照对应工作线的独立日志。

| 能力 | 当前实际入口与状态 | 最新已知验证边界 |
|---|---|---|
| 上下文、图片引用、重载、辅助请求、记忆精排 | 根/子 Engine、原工具权限与物理费用账本已接线；可选收费路由仍须显式配置 | 最新 workspace 覆盖相应单元与实际引擎测试；不代表真实付费供应商验收 |
| Team 队列、广播、隔离写 worker | 同一 TaskRuntime、原 Agent/Worktree gates、事务认领与精确父 Run 约束；默认 Swarm 仍关闭 | 最新 workspace 覆盖真实 Team/Worktree/广播、REST、父 deny Write 与 Hook 通知边界 |
| Skill 来源、路径、作用域 | 三路径兼容、全局事务开关；项目/插件正文按 DB workspace 独立视图 | 最新 workspace 覆盖独立 catalog、路径热重载与原生 Skill directive 正反向用例 |
| 外部 MCP、长期本地服务 | 共用原工具流水线；独立操作身份与不可重复未知副作用；有界服务准入不占 Agent 名额 | 最新 workspace 覆盖真实外部 Write/后处理失败不重复和服务饱和；外部目录受只读默认及显式能力批准约束，不等于全部本地工具对外开放 |
| AutoVis | 默认关闭；真实辅助账本、原权限、可信原生卡片与同逻辑 Task 恢复栅栏 | 最新 workspace 真实引擎 7 PASS / 0 FAIL，包含 unknown usage、根/子归属与恢复不重复 |
| 请求工具上限 | 根原子保存，普通 Agent/Shell/Team 与 attached/detached 继承交集 | 最新 workspace DB 四项及 Engine 四项全 PASS，包含临时 Shell 真 scope；Team 父 deny 用例 PASS |
| 超时与后台 Shell 停止 | 按 durable timeout cause 统一终态；物理未知 usage 保持未知，预算规则不放宽 | 最新 workspace 后台 stop 与 processGroup released 断言、production runtime 9/9 PASS；真实 checkpoint partial 与未知费用均保留 |
| TaskCompleted / TeammateIdle Hook | 父 Run 共享 supervisor；child commit 前 admission 与 parent seal/drain 互斥；Hook 资源登记及 start gate 前二次 DB 活跃检查 | 最新 workspace 与 Engine 无默认 features 均覆盖真实取消 DB fence/通知/seal/timeout 账本；DB Hook 三项全部 PASS |

首轮全工作区日志 `/tmp/zk-alignment-workspace-tests-first.txt` 存在失败，SHA-256 为 `c2fa640ada392ef0e994af50dba2f767ea3c227707e85f95f47488e0c0ad6704`。较早 strict Clippy20 通过仅覆盖当时快照（`/tmp/zk-alignment-clippy-twenty.txt`，SHA-256 `04a4f19e94af1c175050c2dee61599d5f046dbd33c4ad88b7ada0b6907e30314`）；第二轮前 strict Clippy 通过（`/tmp/zk-alignment-clippy-final.txt`，SHA-256 `52736bd0b5c510f9f733e3dba74734160257eb6c53b992c0c5774311006fa8da`），但第二轮 workspace 仍有六个失败 target（日志 `/tmp/zk-alignment-workspace-tests-second.txt`，SHA-256 `5ecaca36733dda8609d0116f0eb26fec997d46f4c395a40681fa4e7bef1ec9c3`）。随后修复不能沿用此前 Clippy/测试结果；尚未记为完整发布通过。

## 当前行为边界

- 临时会话支持根执行和 attached Agent/Shell，正文保留在有界 RAM，金额、usage、执行归属等必要元数据照常持久。不能续接/fork 临时会话，也不创建持久团队队列、detached 任务或长期 MCP/REPL 服务，不写持久记忆；这些是已确认的保留策略边界。普通会话原能力保留。
- Team 使用同一个 TaskRuntime，恢复只修复已创建 Task 的绑定窗口；原执行进程消失后的未派送工作中断，不自动重放未知副作用。写 worker 仍须既有 Agent/Worktree 开关、当前权限和父请求工具上限；提交与合入始终由显式操作触发。
- 清理确认依赖保留所有者的真实关闭证明及身份/version CAS，不修改不可变 TaskResult 来假报成功。未报告 usage 保持未知；只允许原有限定的无金额/token 上限、已清理超时后代例外继续父任务，不把费用写成零。
- 本次只读完成条件核对未发现上述范围仍缺少的必需生产接点；这不是对未执行的真机/发布检查作出通过声明。

## 历史实施快照阅读规则

以下记录保留发现问题、修复与实测的先后关系。早期待办已被后续实现接替时，参照上面的索引和后续明确日志；未实际执行、跳过、失败或受阻的检查始终不算完成。

## 历史阶段：取消状态写库失败时停止本地执行

用户已明确批准：取消持久化失败仍尽力停止当前 Run 及 attached 子树；不假报 durable cancellation 或 confirmed cleanup。

当前实现改动：

- `task/cancellation.rs` 管理按 Run 绑定的进程内停止屏障；仅使用已注册执行的所属会话/Task/Run 身份，在数据库故障时停止已拥有的执行，不扩大授权。
- attached 后代按父子边选择，detached 边切断级联。新提交、迟到登记和执行 claim 检查停止屏障及父任务状态。
- 写库失败返回 `TASK_CANCELLATION_PERSISTENCE_PENDING`；后台对账保留原 Run 和退出原因，禁止取消请求落到新的重试 Run。
- child 终态提交先对账取消意图；清理监督、不可变结果、未确认清理的 partial/unconfirmed 机制继续使用现有 TaskRuntime。
- 根会话终态对账由 engine 生产入口接入。停止按钮/期限到达的本地 token 在持久化错误时仍触发；界面收到明确的保存失败提示。
- 服务端取消协调器独立重试待决交互清理，不把本地 token 信号当作交互已落库或资源已清理。

验证状态（2026-10-07）：

- 已新增/调整定向测试：写入故障下 attached 停止、detached 隔离、跨会话拒绝、新任务准入关闭、恢复后原因保真、timeout/parent-stop 先本地停止、quarantine 不伪造结果。
- 已新增真实 SQLite trigger 故障的服务端集成测试，覆盖取消 API 错误反馈和恢复后交互释放。
- `cargo test -p zk-engine --lib task::runtime::tests -- --test-threads=2`：38 passed，0 failed/ignored；日志 `/tmp/zk-runtime-cancellation-alignment-tests.txt`。
- 首轮取消/终态相关 `runtime_migration`：13 passed，0 failed/ignored；日志 `/tmp/zk-runtime-migration-cancel-tests.txt`。
- 服务端 `run_termination`（真实 SQLite trigger 故障）：5 passed；父任务执行日志 `/tmp/zk-query-runtime-tests.txt`。
- 后续补充早期 reservation 同步取消和截止期限先本地停止，取消原因在终态分类前经 TaskRuntime 对账。新增专项 `engine::early_cancellation_tests` 1 passed；日志 `/tmp/zk-early-cancellation-tests.txt`。
- 尚未计为完整验收通过；Clippy、全工作区和服务端新增专项待实际执行。

## 历史阶段：请求选项与模型过载

- 新增请求级 `reasoning_effort`、`stop_sequences`、`fallback_models`；默认缺省保持原行为，`Some([])` 仅关闭当前请求降级。
- 显式降级候选必须已有模型归属，不允许 typo 经首提供商模糊路由。候选能力重新校验，原物理调用账本继续记录各次实际尝试。
- 思考档位按仓库已确认的具体模型/传输规则验证，不因“支持思考”而猜测支持任意档位。Responses 明确拒绝停止序列；Chat Completions / Anthropic 使用各自真实字段。
- 529 冷却按提供商和实际模型共享（包括固定辅助路由），保留前台有界重试和全部候选不可用时的既有 fail-open。429 密钥冷却及预算不变；不运行收费的后台健康探测。
- `cargo test -p zk-llm --lib --offline`：212 passed，0 failed/ignored；日志 `/tmp/zk-llm-request-options-tests.txt`（包含请求链隔离、未注册拒绝、逐实际调用账本、跨模型冷却隔离，以及自定义 Anthropic 传输不能凭模型名冒认 effort 支持）。

## 历史阶段：上下文质量与用户图片引用

- 根/子执行在最终出站图片准备后按真实 system/tools/output 开销评估上下文。70% 为软目标，硬超预算先执行一次现有有界恢复，再明确失败；原用户文本和必需图片不为满足目标而删除。
- 质量事件包含预算、前后估算、释放 token 与分类，不含用户文本、路径、图片或密钥。压缩 checkpoint 成功后才发布完成事件。
- `context::quality`：3 passed，0 failed；日志 `/tmp/zk-context-quality-tests.txt`。首次测试暴露估算器对损坏图片保守返回字符数，已补完整图片校验后通过。
- 新 `@path` / `@"含空格路径"` 图片引用只解析当前用户输入；不解析历史、邮箱、URL、转义或代码块。结构化 `type=image` / 图片文件引用也支持。
- 引用限制于当前会话工作目录，以逐目录 FD 的 no-follow 读取复用 native Read 完整解码与尺寸限制；封存原始摘要和实际 payload 摘要，BMP 转 PNG，持久化图片字节参与原重放、身份与预算链。
- 图片解码在阻塞工作线程执行；整链、越界/损坏、删除后重放和取消/超限的 `runtime_migration` 最新结果 16 passed、0 failed/ignored，日志 `/tmp/zk-runtime-full-alignment-tests.txt`。native Read 快照 4 passed，日志 `/tmp/zk-image-snapshot-tests.txt`。

## 历史阶段：关键文件重载

- 从当前会话成功且已持久化的 Read/Edit/Grep invocation 选择最多三个文件；Grep 只记录真实命中的有界路径。合并或历史文本不生成新的读权限。
- 只重载工作目录内非符号链接的普通文本文件。通过现有 Read 准入、Task/Run invocation、执行和 ToolResult 提交链执行；用户撤销当前读取权限会收到真实拒绝。
- 轮末工具摘要与下一轮级联压缩都记录实际压缩，并先保存 checkpoint。可选重载视图有上限，完整工具事实仍持久化；根轮末消息投影包含这些记录。
- 实际 `engine_flow` 两条集成测试通过（2 passed、0 failed/ignored）：压缩后重载、原有读权限撤销后不泄露内容；日志 `/tmp/zk-key-file-tests.txt`。

## 历史阶段：当前会话执行偏好

- `GET/PATCH /api/sessions/{id}/execution-preferences` 使用会话 metadata 命名空间和 revision/CAS，不覆盖其他 metadata。effort/fast 组合经实际 provider 适配器验证。
- `/fast` 仅使用已有显式 ZK_FAST_MODEL/LLM_FAST_MODEL 配置且有严格模型归属；运行时模型选择不回写 session.model。图片路由仍在其后按能力选择。
- 普通聊天通过 ConversationPreferenceSource 读取偏好；显式 Query options 保持隔离。无配置保留现有默认。
- 真实服务端集成 `execution_preferences` 1 passed、0 failed/ignored；日志 `/tmp/zk-execution-preferences-tests.txt`。新增 fast 配置与 DB CAS 专项待跑。

## 历史阶段待办（以文末最新状态为准）

1. 上下文质量/指标、关键文件重载与会话运行偏好需完整引擎和全工作区回归。
2. 统一辅助请求执行/费用账本与 SQLite 记忆精排。
3. 用户图片引用的前端入口和全工作区回归。
4. 连续失败的一次策略提示由主任务工作线接入；仍需跨根/子执行验收。
5. 基于 TaskRuntime 的进程内团队 CAS 队列、认领、广播；用户配置优先，默认功能开关不变，不实现外部进程 worker。

以上是早期尚未实现时的待办快照，后来完成的生产接线与实测详见当前索引及后续记录；本段不代表当前仍未实施。

## 历史阶段：进程内团队初期验证

- 新 `team_definitions` / `team_work_items` / `team_broadcasts` 保存逻辑配置、CAS 认领和冻结广播收件人；执行、预算、取消、输出、不可变结果继续由唯一 TaskRuntime 负责。
- 队列冻结原 parent Task/Run；重复 requestId 需内容一致。重启只修复已创建 Task 的绑定确认窗口，旧进程尚未执行的认领标记中断，不重放副作用。
- `team_run_closures` 在自然最终答复边界与入队共用事务互斥。尚未绑定 Task 的队列也是父 Run 的依赖；父引擎等待现有 TaskResult receipt 后以普通计数请求综合结果。
- create / enqueue 的事务拒绝 ephemeral 会话；采用 IN_PROCESS、现有只读子任务准入和用户已配置 worker/queue 参数。未改变配置默认或 ENABLE_AGENT_SWARMS。
- 原 REST 路由接持久队列；dispatch 要求当前所属 runId，支持 requestId。投影经原 CoordinatorEventBus/WS 桥发布，但不恢复旧 Coordinator 的第二套执行生命周期。
- 已真实通过：DB 队列 CAS/幂等和广播/重启绑定 2 项（`/tmp/zk-team-db-tests.txt`）；真实生产子引擎串行执行、停止后的剩余队列不派送 2 项（`/tmp/zk-team-runtime-tests.txt`）。后续新增原子关闭竞争、临时会话拒绝、原始父引擎等待专项尚未执行，不计验收通过。

## 历史阶段：Hooks 初期验证

- 有界且逐目录 no-follow 读取 `.zk/hooks.toml`；坏配置不等价空配置，热更新失败保留最近有效版本并记录错误。PRE transform 必须重走工具准入，security 只能拒绝，不能改写输入。
- POST presentation 只产生受信任 `hookPresentation.text` 展示备注；实际工具正文、错误状态、模型后续输入和证据保留原值。前端由另一工作线接入。
- Stop 只在自然 EndTurn 评估，至多一轮普通计数纠正；金额、次数、总期限、用户取消仍优先。UserPromptSubmit 原文保留，修改文本只形成显式不可信运行时投影。
- 本地命令有 64 KiB 输出/256 KiB 输入上限，deadline 覆盖写入/读取/退出；退出后确认整个进程组消失，调用者取消由保留所有权的 supervisor 清理。HTTP 保留原 SSRF/绑定地址策略，关闭环境代理绕过，DNS 查找也受期限约束。
- 外部 hook 不发送 ephemeral 正文；相关 mandatory security hook 显式拒绝不兼容操作。根/子工具上下文继承 CallEnv retention 和 Run 取消令牌。
- 新增实际 shell/引擎测试覆盖配置损坏、security 输入不变、临时模式、无界输出、stdin 阻塞、孙进程清理、Stop 次数/费用/取消、POST 只展示；这些新增测试当前待执行。仍需核实全部事件入口与最终发布门禁，不能将枚举或接口存在计为已验收。

### 历史待办：团队写入能力（后续已接线并完成六项专项）

当前验证只读 worker 仅是阶段边界，不能代表最终团队能力完成。仍需把显式写 worker 配置、受管隔离 Worktree、当前授权的写工具集合、安全交付/清理接到同一 CAS 队列；提交与合入保持显式操作，不能借团队后台执行自动触发。完成专项验收后才能解除可执行性门禁，配置默认和已有用户开关保持原值。

Hook 展示备注单独存入 `hook_result_presentations`，按已终态 invocation 绑定 session/run/toolUseId，不写入 canonical message、模型输入或证据。`GET /api/sessions/{id}/tool-presentations` 要求 `X-Session-Id`，按 `after` sequence 分页（每页 200，返回 `presentations` 与 `nextCursor`）；内存会话采用相同 ContentStore 与 SQL 防原文触发器。REST 恢复、跨会话拒绝及临时内容失效测试已添加，尚待执行。

2026-10-07 补充真实验证：`/tmp/zk-hook-hardening-tests.txt` 的 Hook 单测 27 项、`/tmp/zk-hook-engine-flow-tests.txt` 的真实引擎 Stop/POST 链路 3 项、`/tmp/zk-hook-admission-hardening-tests.txt` 的改写后重新授权 1 项均通过。新增 UserPromptSubmit 与独立备注恢复测试在此后添加，不能沿用旧通过结果。团队原队列与停止 2 项通过；新增自然根任务等待队列的用例暴露最终等待超时，仍在诊断，功能 gate 保持关闭。DB 队列封存/并发与临时会话拒绝测试已由主任务本轮全库测试验证。

临时会话增量边界（实现已落盘，专项待执行）：工具输入、结果引用（包含内容 SHA 的引用）及后处理 payload 入同会话 RAM；LLM 路由诊断和 requestId 入 RAM，usage/费用/归属继续入 SQLite，scope 结束后诊断只返回固定 unavailable。交互 prompt、回答、权限上下文和包含 operationHash 的 correlationKey 都不写原文；同 Run 并发去重在同 writer 事务中比较 RAM 原值。新授权只提供一次批准并拒绝 remember，已有用户配置许可仍可读取。Activity 正文采用同 scope；SQL guards 拒绝绕过 codec 的原文写入，只校验实际改变列以免过期正文阻断账本收尾。关键文件重载已同步适配 RAM 工具输入/引用/消息。

本轮问题修复与验证：团队父任务等待超时的根因是长期 `RunToolScope` 被当作尚未完成的对话工具；现使用不可改写的 `invocation_kind=tool|runtimeScope`，内部专用创建方法设置类型，普通同名模型工具仍必须完成配对。资源终止仍检查真实 cleanup，临时正文过期后的专用 scope 终态接口不重写输入。`/tmp/zk-team-runtime-final-boundary-tests.txt` 的生产队列/停止/父结果吸收 3 项 PASS；`/tmp/zk-scope-safe-boundary-tests.txt` 的真实配对与同名伪造防绕过 2 项 PASS。团队写 worker/Worktree 接线与验收仍未完成，功能 gate 保持关闭。

`/tmp/zk-hook-engine-final-tests.txt` 4 项真实引擎回归 PASS，覆盖 UserPrompt 保原文/拒绝、Stop 单次纠正/计费/预算/取消及 POST 独立展示。`/tmp/zk-hook-interaction-private-tests.txt` 中 Hook REST 2 项 PASS、既有 interaction CAS 13 项 PASS；新增临时交互过期取消首先暴露 `interaction_terminal` 仍试写严格正文，补固定诊断事件后 `/tmp/zk-ephemeral-interaction-retry-tests.txt` 1 项 PASS。`/tmp/zk-ephemeral-ledger-final-tests.txt` 1 项 PASS，实际检查 SQLite/WAL 无工具正文及 SHA、scope 过期后仍入真实 usage/费用、专用 scope 关闭、原文 SQL 防旁路和重启收尾。一次权限完整准入新增用例尚待执行：上一轮编译被并行 Query 传输接口中间态阻断，不计完成。

本轮后续实现（尚待专项，不沿用先前 PASS）：团队 `workerIsolation=worktree` 通过既有 Agent write + Worktree 双配置门控，执行仍由唯一 TaskRuntime 和受管 Worktree 生命周期负责；未配置维持 readOnly，后台不提交/合入。新增真实 Git 隔离交付与保存配置不能绕过写门控测试。child Stop 仅自然完成且有剩余轮次时可作一次计数纠正；反应式 413/质量恢复补 Pre/PostCompact，Post 在 checkpoint 成功之后；child POST 备注补独立持久化。

临时日志：provider 响应与非法工具 JSON 不再通过 tracing 原样输出，改固定错误码；引擎持久化故障日志保留静态错误类型、固定操作描述与归属，真实诊断继续走受保护的响应/ContentStore。LLM observer 后台写入失败仅记录固定码与 callId；DB API 500 只记录 DbError 枚举类别。新增 TRACE 级真实临时会话反射错误测试及 ContentStore 容量耗尽仍结算真实费用测试，当前未执行。

团队广播接线核对发现只写持久 inbox、worker 仅读内存邮箱的遗漏，现已增加 `consume_task_inbox_at_boundary`：同 Task/current Run 与 sender 同 root 校验，消息正文/metadata、消费状态、事件一次事务提交；内存 mailbox 仅提示，不再直接执行其 payload。新消息明确标注协作上下文、不授予新权限。正常、错误、取消结果提交与 needsAttention 关闭该 Run 的晚到消息，不能复用到下一 Run。新增真实 worker 广播去重、并发消费/失败回滚、临时范围过期与终态晚到拒绝测试，均待执行。

2026-10-07 后续真实验收（取代上文对应“待执行”状态，完整发布门禁仍未完成）：

| 范围 | 实际结果与日志 | SHA-256 |
|---|---|---|
| 真实 Team worker / Worktree 写入 / 广播 / 原父队列边界 | 6 PASS、0 FAIL；`/tmp/zk-runtime-team-write-broadcast-tests6.txt` | `c81b6ab86893b302183de082c0c2d3d526b1f19aa2a882d2f2719fca19249328` |
| 全部 engine_flow（含 child Stop、413 Hooks、临时 TRACE 正文不泄漏） | 62 PASS、0 FAIL；`/tmp/zk-runtime-engine-flow-final3.txt` | `bb5edda74fca635dc0641be19f3d3e8d4005dc68e28f9b4e4e7c0cef2753c8a6` |
| Root SendMessage 安全边界真实模型请求 | 1 PASS；`/tmp/zk-root-inbox-engine-tests.txt` | `0ab1958601fafdd7bc108ec082b52d18ed727eb2e82e84e34ddfdb3f0f0949f4` |
| Inbox 事务/幂等/过期/晚到关闭 | 3 PASS；`/tmp/zk-runtime-inbox-tests3.txt` | `c846e9c9e069c8cd420ec72615d385274da3df124a704027df28df664caba32d` |
| Retained owner 清理对账 | 2 PASS；`/tmp/zk-runtime-cleanup-reconciliation-tests2.txt` | `70877a65a6facc79bc8b04bdac271676f9beea405cf5362a931fe4c54a458b8f` |
| 临时物理账本 + 容量耗尽后的 usage/金额 | 此日志的 ephemeral_ledger harness 2 PASS；后续 inbox harness 因夹具缺期限失败，已由独立日志修正，不能把整条命令称为通过。`/tmp/zk-runtime-inbox-ledger-tests2.txt` | `740f9c51378f90902f5e0d6fc6ffa530631b847cb9dcc653522e9d3eb18a31ef` |

引擎回归暴露立即取消先于异步取消落库的竞态，已在工具中断收尾前安装同一 scoped intent 并等待对账；实际停止不等待该写入，持久原因未确认时也不会假称普通成功。根与子引擎现在都从同一事务 inbox 消费协作上下文；原始发送来源、当前 Run 和内容范围仍须验证。

团队实际执行与隔离写入通过后，`swarm_executable` 改为原 Swarm/Agent/feature flag + 已初始化 epoch + 唯一 Agent runtime 门控，没有修改任何默认开关。新增真实 REST 创建/保存配置优先/派送幂等/输出/停止测试仍待执行，不能将未跑入口算通过。

新增窄清理对账接口仅在保留的资源所有者确认实际关闭后，凭原 Task/Run/invocation/resource/external identity/version CAS 将 unconfirmed 更新为 released；普通 finalize 仍禁止这一跃迁。已终态内部 runtimeScope 只更新 cleanup，TaskResult 内容/状态保持不可变；Run 汇总仍受其他资源、调用与 attached 子树约束。终态只读 observer 统一在已提交结果的发布之前调用，有十秒上限，失败不回写结果正文。

新发现长期 REPL/MCP 服务占用普通 Agent 并发名额，已增加 TaskRuntime 内部显式执行类别：普通 Task 仍全局 8 / 单 root 4，MCP 与 REPL 分别有 16 个服务名额。只有原 typed service 提交口可以选类别，用户配置不能伪造；等待仍受原绝对期限/取消/账本驱动。生产接线与饱和/排队取消回归已保存但尚未执行，后续必须补验收。外部 MCP 完整工具后处理窄接口随后已保存，但实际门禁尚未通过，当前受限过滤不放宽。

外部 MCP 复用 `run_bound_tools` 原工具事务链，绑定独立 service Session/Run、同 Engine 会话串行租约、原 admission 与 revocable ceiling，保留 PRE/POST、Invocation/Supervisor、文件产物、research 与 machine evidence。`external_tool_requests` 使用独立 operation identity（允许 JSON-RPC id 正常复用），先 writer 事务认领再执行；同 identity 的不同输入拒绝、未知副作用不重做，只有原始结果及所需后处理完整后允许重放。临时输入沿同域 RAM codec，不能落盘正文 SHA。新增并发、真实 SQLite 关闭重开、Hook 投影恢复、临时范围、真实 Write/产物及落库故障回归代码，尚未执行。

后台 Shell 实际 stop 回归发现取消分支过早封存 `cleanupUnconfirmed`，即使物理进程随后正常退出也无法形成正确取消结果。修复仅在子任务/外部工具取消时有界排干该批执行 streams，再按 exact invocation/run 的非空且全 released 资源证据确认；未排干或无正证据保持 unconfirmed，根会话 Ctrl+C 快速取消不额外等待。本轮修复后真实 stop 尚待重测。

Skill 路径兼容与热重载（实现已保存，专项待执行）：用户与项目分别按 `LEGACY_CONFIG_DIR_NAME/skills`、`CONFIG_DIR_NAME/skills`、既有 `.zkcode/skills` 加载，同来源当前路径优先，原 Managed/User/Project/Plugin/Bundled/MCP 优先级不变。热重载对完整、确定顺序的文件候选在单次注册表写锁内发布，删除胜者可逐层回退现有文件；读取失败保留上一完整视图并重试。首次 watcher 对账关闭启动加载与基线之间的删除窗口；全局禁用状态和事务存储不受候选替换影响。新增四项真实目录/重载用例连同原七项待执行，没有移动、删除或自动启用用户技能。

外部 MCP 验收夹具调整：本轮政策禁止临时会话创建长期 MCP 服务，不能为测试 RAM codec 人工放开服务准入。现改为 persistent MCP 测后处理/原结果重放，并独立验证 ephemeral 创建被拒绝且没有 Task/operation 正文落盘；同域 RAM codec 仍作为纵深保护。新增 SQL owner trigger 禁止以另一个合法 Session 绕过原 Run 的内容策略。全部四项等待本轮复跑。

最新 DB 实测：`/tmp/zk-db-alignment-all-targets3.txt` 中 external_tool_request 四项全部 PASS，覆盖并发/冲突/直接 SQL 跨会话拒绝、后处理完成才重放、真实关闭重开与独立 Hook 备注恢复、临时 MCP 准入拒绝无落盘；同轮 cleanup_reconciliation 两项也 PASS。日志 SHA-256：`720768fe1e71424dc046ca53c47bd41fd6f226b97ef6ef083eaf3f7bf8ba31fe`。Engine 外部生产链、独立服务饱和与新增 Skill/Team/Shell 回归仍等待下一轮，不由 DB 结果代替。

2026-10-07 当前专项更新：`/tmp/zk-engine-alignment-all.txt` 实际通过 `external_write_uses_canonical_artifact_pipeline_and_replays_without_rewriting`、`failed_required_projection_does_not_repeat_an_already_applied_write` 及 `saturated_local_services_never_take_agent_slots_and_queued_cancel_never_starts`，证明外部工具原事务链/未知副作用不重复及独立服务有界准入。整条 engine 命令仍有其他 rewind 夹具失败，不计全量通过。日志 SHA-256：`a5b9dfb2fe64f1c8e786772ec095ae01ea30882f2b98c5bd9c06dc7b958e5007`。

`/tmp/zk-server-alignment-combined3.txt` 已通过后台 Bash 三项（含实际进程停止后再回终态）、团队保存配置/真实派送 REST 一项及 Skill loader 十一项。该批仍有 production_runtime_roundtrip 三项失败：旧计数把内部 runtimeScope 当模型工具；旧并发 fixture 未处理新增 Inbox 实际消费后的二次请求；限时子任务的取消费用例外仅认 STREAM_DROPPED，未覆盖正式 PROVIDER_CANCELLED。已更新前两项为更严格的类型/消息归属/额外真实费用断言；第三项只扩展已清理 timeout 子任务的正式取消码等价类，根有金额或 token 限额仍禁止继续。新增物理 usage 不完整、token/金额保持 NULL 及 reservation 保持 incomplete 的断言。修复后尚待重测，不能将它们计通过。

新增必需修复：原 startup 全局 Skill 表把启动目录的 Project/Plugin 正文泄露给其他工作区。现使用 `SkillCatalog`，只从 DB Session/Project 的工作区读取项目视图；全局来源与事务开关共享，项目与插件内容不写入全局表。REST、WS `/skill` 与 fix/stuck/别名、模型实际执行、Run ToolSearch 目录均用同一作用域；无上下文只给全局目录。项目缓存有容量与活跃视图租约，500ms watcher 刷新现有视图，任何读取失败保留上次完整定义并报告固定诊断；文件胜者删除按原来源/兼容路径规则回退，禁用不丢失。新增三项 catalog 测试与一项 REST 集成测试已保存（多项目并发、热重载、伪 CWD/跨 Run 防绕过、共享开关与无上下文隔离），尚未执行。此次更新不将先前的 loader PASS 作为新隔离代码已验收。


## 当前实现核查：辅助能力与真实入口

- 工具摘要通过 `LlmSummarizer` → `collect_auxiliary` → `SummaryExecution` 接既有真实费用账本；记忆精排仅显式 `ZK_MEMORY_RERANK_MODEL`，`prepare_run` 调用 SQLite revision/候选 ID 校验及三秒本地回退。默认未额外选择收费路由。
- 根/子工具连续三次失败各产生一次策略调整提示，保留连续十次失败、总期限和预算停止条件；提示不授予新能力。Hooks 的 PRE/POST、自然 Stop 一次纠正、Session/Run/Compact/Task 生命周期已有生产调用方，正文只在授权内容策略内传递；POST 展示为独立投影。先前“尚未执行”的条目均为当时状态，实际专项与总门禁以最新日志列为准。
- 团队工作队列已允许受原写 Agent/Worktree gates 约束的隔离写 worker。Git commit/merge/remove 仅显式动作；队列旧 epoch 未执行认领中断，已创建任务仅按同 creator 身份恢复绑定，不重放未知副作用。原 ENABLE_AGENT_SWARMS 默认关闭不变。

## 可视化意图自动路由（已接线，实测问题修复待复验）

固定源 `053adf90` 的 `VisualizationIntentClassifier` / `VisualizationAutoRouter` 真实循环接线已重新核对；源按变化上下文分类并直接调用工具，不能由此前免费 `/visualize` 模板助手冒充迁移完成。现新增 `auto_visualization` 与引擎窄接点，根会话、真实子引擎均接入。默认关闭；仅 `ZK_VISUALIZATION_AUTO_ROUTING_ENABLED=true|1` 且显式配置 `ZK_VISUALIZATION_MODEL`（或既有 `ZK_FAST_MODEL` / `LLM_FAST_MODEL`）才允许发请求。配置失效不猜测默认模型或新收费 Key。

每次分类最多 256 输出 token、15 秒且受所属 Task 剩余期限/真实预算限制；所有物理请求仍入 SQLite 费用账本。无可见原生 Visualization（包括 Query `tools:[]`、allowlist/denylist、Run 目录和用户禁用）时先返回，不发送收费分类。Run 内最多保留 32 个已分类上下文的有界 RAM 副本，负向结果在用户/工具摘要变化时可重判，原上下文不重复；正向后该 Run 不重复自动卡片。

正向分类只接受源七个 viewType 与短字符串建议，拒绝命令、任意目录、伪 schema/查询数据。保留原 `diagram_type/content` 参数，新 `viewType/props` 兼容原生工具；自动 `intentOnly=true` 明确尚未执行分析，前端不因建议 mount 自动查询组件。真正派送使用已保存的 runtime ToolUse → 原 ToolAdmission/PRE/Executor/Invocation/POST/结果事实链。自动建议前先保存同逻辑 Task 的消息栅栏，同一 Task 的 Run 恢复/新尝试见栅栏不重复执行；只有实际成功的可信原生绑定可发布可视化事件，远端同名工具/metadata 不构成信任。

新增真实引擎专项覆盖默认/无工具/显式禁用零辅助调用、正向计费与唯一工具事实、负向后变化重判、相同结果去重、权限拒绝无卡片、缺少 usage 不冒充免费、子 Run 独立归属。首轮 workspace 已执行：当前索引列出 5 PASS / 2 FAIL 与修复状态。Skill 三项作用域用例通过；production runtime 已由三处失败收敛为一处 timeout race，不能据此称整体通过。

## 请求工具限制的派生任务边界（实现已保存，分层验收中）

独立复核发现根请求的 `allowed_tools/disallowed_tools` 原先只筛选根模型目录，团队/普通 Agent/Shell 子任务的注册目录仍可能扩大范围。现将 typed `toolCeiling` 与根 Task/Run/预算在同一 writer 事务保存；每个子任务创建在同一事务读取精确父 Run 的 Task 配置并取交集，attached/detached 均适用。Team 本地 allowlist、写 worker/Worktree 开关仍进一步收窄，不能补回父拒绝的工具。Skill directive 也只在持久收窄成功后更新实际目录，落库失败停止运行。

根、Agent、Shell、恢复使用同一 Task 配置解码和 Run 目录过滤；无 scope factory 的 SDK/测试构造也不会跳过约束。没有请求限制时保留原目录；显式空列表表示无工具；非法/失效临时正文读取失败时拒绝派生调用，不推断为无限制。临时配置仍经原 ContentStore，只在 RAM 保留内容。

`/tmp/zk-db-tool-ceiling-final2.txt` 实际 4 PASS（SHA-256 `5efd5b6a7a20f694e868b322aac0707724ad060416aaa61a49620810291190a3`）：空列表/非法策略、attached/detached/幂等交集、单调收窄及 DB 关闭重开、临时 RAM 与过期拒绝。最初夹具缺少生产期限导致失败，已为夹具加真实期限，未放宽 `ROOT_BUDGET_NOT_CONFIGURED`。同阶段 DB lib 164 PASS，原整条命令含旧夹具失败，因此不计整轮通过。后续首轮 workspace 中 Query deny 与可信 Skill 两项通过、Agent/Shell 两项夹具失败；Team Worktree/Write 拒绝专项通过。Agent deadline、临时 ShellMemoryScopeFactory 夹具已补，仍待复测，不以这四项 DB 测试代替完整验收。

相同信任边界核查还修复了 Skill directive 仅按工具名识别的问题：Tool trait 默认不提供可信指令能力，只有原生 Skill 绑定明确提供，Engine 仅消费实际 invocation 绑定声明的指令。远端同名工具或 metadata 不能改模型/持久工具策略；新增正反向真实引擎用例在首轮 workspace 已通过；实际绑定仍为唯一可信指令入口。


## 第二轮全工作区后的本域修复记录

第二轮 AutoVis 七项、Engine 工具上限四项、Team 父 deny Write、后台 Shell stop（有界等待后所有真实 processGroup 已 released）通过。第一轮失败的未配置 child deadline 与临时 ShellScope 夹具均已按生产约束修复，没有放宽费用、权限或清理要求。

TaskCompleted/TeammateIdle 正常实际命令、父 seal 等待 child commit admission、失败 guard 释放和 timeout cause/未知物理费用断言通过。取消竞态用例起初忽略旧 Run helper 返回的 InvalidTransition，父 Run 实际仍 waitingDependencies；已改为真实 TaskRuntime 持久取消并断言成功，故意不 signal 内存 token，以单独验证 DB 的两道 Hook 物理派送屏障。旧兼容 terminate helper 也补 waitingDependencies→cancelling，并增加状态/资源封锁测试；这些后加修复待复验。

生产 timeout 集成仍严格要求已输出 partial 可恢复。第二轮揭示恢复函数只接受旧顶层消息数组，而真实 ContextCheckpointState 写入版本化对象；现只解析 `schemaVersion=1` / `kind=contextCheckpoint` 的 `messages`，保留旧数组，未知版本不猜测。只提取最后有效 assistant 文本，tool/thinking 不进入成果。已加实际 envelope 单测；原强 partial、两份子结果、物理未知 usage 和预算断言全部保留，等待新一轮真实执行。


### 最新 Engine 无默认 features 验收

`/tmp/zk-alignment-engine-no-default-current.txt` 实际 16 个 harness、696 PASS、0 FAIL、0 IGNORE，SHA-256 `1be8ba3d7ec3ecf5bacf0af21c92166cb7e86a6eb7d212e4178c77f70908831f`。日志明确包含修正后的 `parent_cancel_after_child_admission_prevents_actual_hook_side_effects`、全部四项 terminal_hooks、`timeout_recovery_uses_last_assistant_text_only`（包含真实版本化 envelope）、AutoVis 七项、工具上限四项和隔离日志回归。该结果验证最新 Engine 代码；仍不能代替 server 的 production timeout 完整真实链与最终全 features/workspace 门禁。新 DB waitingDependencies helper 专项亦等待相应 DB 命令实际结果。


### 最新服务端与 DB 取消/超时闭环

`/tmp/zk-alignment-hook-db-current.txt` 实际三项 PASS（包括新 `legacy_run_termination_accepts_a_parent_waiting_for_dependencies`），SHA-256 `fe089b01dea501fdb71a44d9378dcb368712e8775d0ca558997c800a487e7db8`。`/tmp/zk-alignment-server-focused-current2.txt` 中 production_runtime_roundtrip 九项全部 PASS，含真实 timeout partial 汇总和不伪造 unknown usage/费用；该组合另有 Bash 一项、MCP 五项、Query 十一项通过，SHA-256 `1d10085fd7b5ae9c97fff981720cd23dfa516f7d4f4a1573031ccc38aa114136`。这取代上文对应的待复验状态，最终发布门禁仍由主任务统一执行。

新增只读 Git UI 的真实 REST 专项由本工作线负责测试文件 `crates/zk-server/tests/git_read_api.rs`，生产实现由前端工作线负责。六项真实临时 Git 仓库测试覆盖固定 SHA 分页、HEAD 变化、根提交 diff、中文/空格/TAB 路径 blame、会话/项目边界、历史 symlink 拒绝、预取消及跨 owner 隔离。主任务实测 `/tmp/zk-alignment-git-read-api-current.txt` 六项全部 PASS（0 fail/ignore），SHA-256 `d1abca7db82d54438a953e60486e8358e3816f5b20d0caa7ca7bc0481f24fe73`。在途进程取消/清理单测由生产模块工作线另行记录，本六项结果不冒充该项验证。
