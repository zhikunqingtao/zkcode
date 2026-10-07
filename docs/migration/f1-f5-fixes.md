# F1–F5 修复与真实账本验收

> **后续范围变更（2026-10-07）：** 用户明确删除简洁工作台，仅保留开发工作台，不处理简洁模式历史数据。最新前端验证为 145 文件／1289 项及 9 项真实后端 E2E；见 [单一工作台记录](development-workbench-only.md)。本页此前 F1–F5 全量计数与源码身份属于该次冻结，不能代替后续前端验证。


范围：在 `main` / `08cbdc45` 及既有未提交迁移上修复本轮确认的问题。源目标仍为 `zhikuncode 053adf90`。不重建数据库，不改变有效权限偏好，不提交、推送或创建 PR。测试使用独立 SQLite、临时工作目录与本地协议夹具；本轮不调用付费供应商。

**当前状态：F1–F5 修复完成，真实账本集成通过，37 项必需本地发布门禁已通过；具备本轮范围内的提交条件，适用边界见下文。** 当前机器记录为 [full-alignment-gates.json](full-alignment-gates.json)。修复前的通过记录已保存在 [历史门禁快照](full-alignment-gates-before-f1-f5-20261007.json)，不能代替修后验证。

## 实现与边界

### F1：Hook 先授权，再进入原有执行监管

- 引擎增加宿主 `HookAdmission` 端口，服务端使用现有权限服务、交互、`permission_grants` 和执行准入事务。`hook-v1` 事实仅由宿主构造，未注册为模型工具。Bash 批准不能变成 Hook 批准，模型空工具集与宿主 Hook 授权分别处理。
- 精确事实包括物理工作根、配置来源、已解析声明和固定执行环境。TOML 注释、排版不改变事实；命令、URL 或执行语义变化会重新评估。单次批准在准入时消耗，只支持单次、Run、Session；不同 Worktree 不共享批准。
- PLAN 始终禁止副作用 Hook；DEFAULT/ACCEPT_EDITS 按既有风险和精确授权检查；DONT_ASK 无授权时拒绝且不弹确认；AUTO_APPROVE 保留硬性限制。HTTP 保留 SSRF 防护，外部 MCP 能力上限不等于执行批准。
- `hook_admitted` 仅表示获准启动，审计写入失败即不启动；不冒充模型工具调用或执行成功。配置保存说明和既有权限弹窗显示实际含义。
- 准入产生不可克隆的单次启动凭据。资源登记后、命令 start gate 或 HTTP send 前，再核对原授权、真实模式、配置身份、所属任务、取消及期限；不二次提示、不二次消耗单次批准。真实 SQLite 触发器验证资源绑定阶段撤权或切到 PLAN 时不能启动命令。持久会话读取 SQLite 权限，不以临时模式覆盖遮蔽后续变化。
- 必要 security Hook 拒绝会阻止对应操作；普通通知、展示与可选转换失败隔离并记录。POST 不改原始工具事实，Stop 不覆盖用户取消、预算和期限。
- 删除与手动压缩使用同一个 TaskRuntime 的受管宿主执行。无 Hook 时不创建空任务。SessionEnd 是“请求删除”阶段的检查，仍可查询原会话授权；清理确认后才删除。PreCompact、实际保存和 PostCompact 共享执行归属，POST 失败不撤销已保存摘要。
- 为兼容本轮不重建数据库，宿主任务复用现有 `shell` 存储类型，以 `executor=localHook` 和明确描述区分；不新增模型 Bash 调用。读取终态失败时保留任务、取消和清理责任，不能因 HTTP Future 消失释放仍活跃的修改租约。
- 正常路径回归还暴露了原有 stdin EOF 等待问题：写入 Hook 输入后显式关闭管道，避免 `cat` 等待 EOF 而超时。

入口：`crates/zk-server/src/hook_admission.rs`、`crates/zk-authz/src/`、`crates/zk-engine/src/hook/`、`crates/zk-server/src/api/session_hooks.rs`、`crates/zk-engine/src/task/external_root.rs`。

批准声明不代表为任意脚本依赖建立了沙箱。普通 Shell 的既有能力边界仍然适用。

### F2：Skill 词法身份与读取授权分离

保留来源优先级、缓存键、旧路径和热重载。PROJECT/PLUGIN 绑定其自身项目授权根；USER/MANAGED 使用明确配置根。以目录描述符打开与扫描，核对实际路径和目录身份，拒绝根、祖先或文件置换导致的越界。保留授权范围内的合法来源根别名，继续不扫描子文件符号链接。

首次非法来源不加载；刷新读取失败保留最近已验证的内容快照并报告错误；结构性来源失效后，列表、详情、发现、命令和模型 Skill 调用均不能继续使用旧快照。首次加载前替换已登记工作根也被现有工作区绑定检查拒绝。整个授权根被替换后不会自动重新授权，需要重新建立有效来源身份。根变成普通文件或循环链接属于结构性授权失效，不能被错误归类为可沿用快照的普通 I/O 故障；原根暂时不可用及原身份恢复则继续保留既有回退行为。

入口：`crates/zk-server/src/skill/filesystem.rs`、`catalog.rs`、`loader.rs`、`registry.rs`、`tool.rs` 及 REST/WS 入口；安全目录访问复用 `zk-tools::safe_file`。

### F3：仅明确新建操作可转移首页草稿

首页发送或显式新建捕获 `draft.id`，由内部绑定上下文带到匹配的 generation/requestId/epoch 提交点，先比较后转移，再展示目标会话。历史恢复、重连、普通选择和已有会话中新建均不推断转移首页草稿。

目标已有其他草稿或源已换代时保留现有记录并提示；相同身份重复操作幂等且不能删除后来创建的首页草稿。异步图片读取、本地附件及提交清理继续追踪原草稿身份。创建 HTTP 响应和 WebSocket 恢复两阶段都检查用户是否已经切换选择，迟到结果不得夺回当前会话。

入口：`frontend/src/App.tsx`、`services/sessionActivation.ts`、`api/dispatch.ts`、`store/promptDraftStore.ts`、`components/input/PromptInput/usePromptDraftKey.ts`。

### F4：复杂度缓存只使用实际参与分析的输入

复杂度分析器与指纹计算共用文件选择函数，包含语言、规范化目标、过滤规则、相对路径和实际内容，覆盖 JS/JSX。内容等长、mtime 不变、增删或重命名都能正确失效；任务前后使用同一指纹，运行期间版本改变不缓存混合结果。保留 500 文件上限、owner 隔离、有界缓存、可取消 worker 和其他分析类型的原指纹。

入口：`python-service/src/services/complexity_analyzer.py`、`python-service/src/analysis_jobs.py`。

### F5：期限原因贯通持久化和终态投影

Query 和根任务使用同一绝对截止时间，内部 `RunStopCause` 竞争同一个首个停止事实。期限不会再调用硬编码的用户取消路径；迟到计时器不覆盖已完成结果，旧请求取消能力不影响后续同会话执行。

本地停止先阻断执行，再传播所属 Run/attached 子树；UI ACK 背压不再阻挡停止传播。取消保存失败仍保留原有对账责任。终态投影读取已提交 Run：包括“模型已答完，但 RunEnd Hook 尚未清理完时超时”的边界，REST、SSE、`message_complete` 和数据库保持一致。原始 assistant 消息和模型 finish 事实不被改写。

入口：`crates/zk-engine/src/engine.rs`、`conversation_service.rs`、`crates/zk-server/src/api/query.rs`。

## 摘要重试与账本的准确承诺

摘要最多重试一次的前提是：上一物理请求能完整结算，而且金额、token 和期限仍允许。未知 usage 的 HTTP 429 不视为免费；账本保持未知值并以 `BUDGET_USAGE_INCOMPLETE` 阻止后续付费请求。

本地摘要回退可以完成纯本地压缩，但不修复未知账单，也不表示聊天已经恢复为可继续付费。普通请求与摘要请求均使用生产 SQLite observer 验证物理请求次数、真实用量/金额、拒绝后的零新增请求，以及保存失败不伪造成功。

活跃请求用量先计入 Run；Task 的已结算消费在真实终态事务更新。验证同时检查这两个阶段和再次提交不双记，并以实际准入验证超剩余 token/金额时拒绝启动。未重构费用系统，未放松未知费用门禁。

测试：`crates/zk-engine/src/llm_summarizer_ledger_tests.rs`，11 项。9 项使用本地 provider 事件协议和真实 SQLite observer；另 2 项通过生产 OpenAI 兼容适配器访问随机 loopback HTTP 端口，验证普通请求及完整摘要压缩收到真实 HTTP 429 后，账本保留未知费用，HTTP 请求数停在 1。可结算失败重试仍以事件协议覆盖；不据此宣称远端供应商的 HTTP 429 一定带 usage。

## 验收记录

本轮 **37 项必需本地门禁通过**；1 项可选付费供应商门禁未运行，不计为通过。详见 [机器门禁](full-alignment-gates.json) 与 [回归定位及验证边界](f1-f5-regression-map.md)。专项用例已纳入下方完整套件，总数不能再与专项数量相加。

| 验证范围 | 本次修后结果 |
|---|---|
| Cargo 工作区 | 148 个独立 harness，3199 通过、0 失败、9 默认忽略；原生日志另列，避免重复计数 |
| Engine 无默认 features | 716 通过、0 失败、0 忽略 |
| Rust 格式／Clippy／release | 全部通过；Clippy 覆盖所有 targets/features，拒绝 warning |
| React | 157 文件、1321 通过、0 跳过；lint、类型和构建通过 |
| Python | 342 通过；父进程覆盖率 75.85%，高于 70% 门槛；9 条依赖／弃用 warning |
| 真实 release 页面 E2E | 6 项产品 + 3 项代码分析／Git，全通过；真实 Rust、SQLite、Python UDS、本地模型协议 fixture |
| 浏览器样式 | 20 主题场景／12728 断言，Jelly 42 项通过 |
| 原生能力 | 五类 LSP、macOS Keychain、两项浏览器／HTTP Journey 单独显式通过 |
| Office／安装 | Office 41 项、安装脚本 87 项通过；官方 Rust/build repair 与 deep doctor 40 项通过 |
| 契约／依赖／敏感信息 | 契约生成、Cargo deny、npm audit、pip check、源码秘密扫描及 diff 格式检查通过 |
| 摘要及普通请求账本 | 11 项真实 SQLite observer 集成通过，其中 2 项经过真实 loopback HTTP 429 |

源码冻结覆盖 1387 个路径（1384 文件、3 删除项），前后内容及逐路径身份一致。源码 SHA-256：`01407de2a65016f55704b4e6e1f25ce14f58dab800b0e196ec24d99634d32a37`。只排除文档和 Git 忽略内容，不以 Git HEAD 代替未提交源码身份。

实际 release：`zk-server 0.1.0 (git 08cbdc45e12cc28f3f13aaafbd0b056b980dfb1a, built 1791332366, schema 2, ws 4)`，37773888 字节，SHA-256：`3ff461b276879e19fb9898b639af54c9cd979f2db5ce64cd3741d073e4bf476e`。两套真实页面 E2E 前后都核对该摘要。前端与 Python 完整测试执行后各自源码未变化，最终冻结再次逐文件核对。

当前分支与 HEAD 仍为 `main` / `08cbdc45`。没有提交、推送、PR、数据库重建或付费调用。现有后台服务 PID、健康状态保持不变；运行中的 Rust 进程早于新构建，**需要后续显式重启才能加载修复**。本次使用官方 `dev repair rust`、`dev repair build` 及 `dev doctor --deep --json`，没有执行可能触发用户服务恢复／重启的全量 `dev sync`，也不冒称完成全新机器安装。

### 失败与修正记录

- 五项缺陷均先保留反例，再修生产路径。11 份预期失败日志和修后日志已归档，失败没有被重标为通过。
- 第一次完整 Rust 回归失败于 4 个 target：旧 Hook 夹具缺少新宿主授权端口，以及取消用例仍期待成功终态。夹具改为显式提供授权／真实执行归属，取消断言加强为持久化 `userCancelled`；没有给生产环境增加默认放行。
- 第二次仅 Git 取消用例失败：繁忙机器上 Hook 实际启动需约 3.61 秒，超过测试自设的 3 秒等待。测试改为等待真实启动信号或已有总期限，并在任何失败路径先取消、确认清理。生产 Git 逻辑未改，10 秒清理要求未放宽。
- 第三次完整工作区通过 3199 项。严格 Clippy 的中间失败及修复后运行均保留。
- E2E 启动准备首次使用系统 Python 3.9，缺少 `hashlib.file_digest`，在启动测试前失败；改用项目既有 Python 3.11 后两套 E2E 均通过，没有因此修改产品源码或调用供应商。
- Python 最新 342 项包含真实 spawn 补测。覆盖率仅统计父进程，子进程不计入该覆盖率；不把更早的 339 项／76.50% 作为最终证据。前端构建现有 chunk-size 提示及 Python warning 如实保留。

### 验证边界与结论

F1–F5 的已确认反例及账本验收缺口已关闭；正常授权 Hook、合法 Skill 来源、明确新建草稿、有效缓存及用户取消路径均有正向回归。**在约定的本地范围内，目前具备提交 GitHub 的条件，未发现本轮仍待修复的提交阻断项。** 这是基于当前实现和明确测试范围的判断，不是“所有行为、所有项目、所有调度均无缺陷”的保证。

- 9 项默认忽略中，Keychain 1 项、LSP 1 项、原生 Journey 2 项已单独通过；其余 5 项付费凭证测试未运行。此前 DeepSeek 探针只证明其历史源码／构建，不能算作本轮新构建通过。
- Hook 覆盖五模式、命令／HTTP 授权和各生命周期入口，未穷举全部交叉组合；请求消失用取消 Router Future 验证，未冒称真实 TCP 断网。批准不是脚本全部依赖的沙箱。
- Skill WS 用真实传输和 RecordingEngine；加载后撤销在共用校验、发现和模型入口验证，未另做 WS 撤销专项。草稿完整竞态／附件／导航矩阵是组件与 Store 测试，不是完整浏览器矩阵。
- 缓存变化由真实 worker／spawn 子进程验证；浏览器分析 fixture 为 Python。双计时器先后在内部确定性控制，取消保存故障使用 failpoint；没有把它描述为全部 REST、真实 SQLite I/O 故障和任务树组合已验证。
- 可结算摘要重试使用 ProviderEvent 协议 fixture 和生产 SQLite observer；未知 429 另经真实 HTTP 验证。没有远端供应商互操作、真实磁盘写满或全部崩溃恢复认证。本地回退不解除 `BUDGET_USAGE_INCOMPLETE`。
- 原生 Office 实际使用 LibreOfficeDev 26.8 alpha；远端 OAuth 仅本地协议验证。临时 Java LSP 与外部 MCP 的既定能力限制保持，未迁移任何明确排除项。
