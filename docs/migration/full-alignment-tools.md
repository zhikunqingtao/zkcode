# 本地开发工具能力补全记录

> **2026-10-07 F1–F5 修复更新：** F1 已将 Hook 接入宿主授权及实际启动前复核，必要安全 Hook 拒绝与可选 Hook 失败分流；F2 的 Skill 来源以真实授权身份和目录描述符访问约束。普通 REPL、工具权限与现有来源优先级继续保留。 最新验证见 [修复记录](f1-f5-fixes.md)、[回归定位表](f1-f5-regression-map.md) 和 [门禁](full-alignment-gates.json)。下方较早的通过数量、状态和构建身份保留为历史，不代表本轮验证。

固定来源：zhikuncode `053adf90`；在 zkcode 当前工作区增量实现。本文区分已验证实现与后续工作，不将来源占位实现算作已迁移完成。

## F1–F5 修复前状态索引（历史，2026-10-07）

**本节及后文为 F1–F5 修复前各阶段的历史记录；当前状态见页首修复链接。** 单次真实模型探针准备中发现 DeepSeek 适配器忽略显式 `thinking=disabled`，无条件写入 enabled/max，发现时尚未调用收费 API。适配器已修复，9 项真实 loopback wire 回归、最终格式 / 严格 Clippy / 机器契约已通过；修复后整工作区 3143 项、无默认特性 16 个 harness / 696 项，以及新发布链均已通过。下表和发布产物均已更新为修复后实际结果；未受影响的前端、Python、LSP、Office、Keychain与本机浏览器专项证据继续保留。 后文“尚未开放 no-session”“临时 WebBrowser / REPL 拒绝”“STDIO 待验”“普通 REPL 服务待实现”等叙述属于早期中间态，已经由后续受管实现和以下真实验证替代，不能作为当前缺口。统一可核验日志、SHA-256、历史失败和待完成门禁见 [完整对齐门禁台账](full-alignment-gates.json)。

| 当前能力 / 检查 | 冻结版本真实结果 |
|---|---|
| Rust 全工作区（thinking 修复后） | 145 个独立 harness，3143 通过、0 失败、9 默认忽略（原始日志 146 条结果 / 3144 次通过包含一次子进程自重跑，已去重）；日志 `/tmp/zk-alignment-thinking-workspace-final.txt`，SHA-256 `e65109d7113495aded4bfe36014dfc575a2717160d8c99776a6a792d7deed7eb`。忽略项中 Keychain 1、五语言 LSP 1、原生 Journey 2 已显式另跑通过；4 个收费 LLM 和 1 个远端搜索测试未运行，不算通过。 |
| MCP 服务、外部 STDIO、候选上限和统一 Engine 管线 | MCP lib 210、服务 REST 19、Context 2、STDIO 2、外部能力 5、reverse 3、WS 6 均通过。包含真实本机批准、默认 DEFAULT、Hook 间接扩权拒绝、快照与操作重放；不是仅有只读过滤。 |
| SessionStart / PRE / POST / Stop / 通知 Hook | Session Hook 归属 6、诊断日志 2、DB 原子资源门控 3，以及父通知与封口专项通过。SessionStart 先于主模型和辅助记忆精排；异步结束通知在真实 owner 终态提交前排干，取消不被 Stop 推翻。 |
| 普通与临时 REPL | Python / Node / Ruby scope 3、普通服务所有权 / 清理重试 2、REST 1 通过；真实两轮 Query 保留 42→43，并验证未批准不建解释器、停止、跨会话拒绝、删除 / 合并门控。普通服务跨轮次保留，临时 Run 结束清理。 |
| 临时证据、WebBrowser 与 VerifyJourney | DB / WAL 与跨会话资源回归通过；实际 Chromium / HTTP 普通和临时场景 2 通过（4.93 秒），包含多次真实交互、RAM 截图、错误 Run 拒绝及资源清理。no-session 已接通，录制 trace / video / HAR 仍在执行前明确拒绝。 |
| LSP 与本机 Office | 五类私有固定语言服务器真实协议 / 进程清理通过；Keychain 真实写读删通过。Office 41 通过，保存真实工具 / 字体 / 浏览器身份；使用实际 LibreOfficeDev alpha，不冒称稳定版。 |
| 前端和 Python | 冻结前后源指纹一致：前端 156 文件、1295 通过、0 跳过，lint / build 通过；Python 335 通过、覆盖率 75.83%。主题真实 Chrome 20 场景 / 12728 断言、Jelly 42 场景通过。 |
| 静态与契约门禁（thinking 修复后） | 严格 workspace / all-targets / all-features Clippy、格式、diff、机器契约及最终秘密扫描通过。 |

修复前发布构建通过（6 分 41 秒），旧二进制 SHA-256 `822adfed182e047c0cc31ac4b9e1c41a14d474f19d249f331f27aa83a92a6315`；该产物的 production 6 / analysis 3 E2E、正式同步和 37 项 doctor 当时均通过，保留其实际记录和首轮夹具失败历史。thinking 控制修复、完整 Rust / 无默认特性回归和严格检查已通过；修复后 release 构建已通过（5 分 56 秒），当前产物 SHA-256 `cca0d0a64e2ab3bad9bf95fdc22fcb02f278d89c8b9e78286d4c0960a89e3251`。对应 production 6 / analysis 3 E2E 已通过（29.1 / 30.6 秒），前后产物摘要一致；正式同步与 37 项深度 doctor 也已通过；单次限额 DeepSeek 实测与最终源码指纹比较也已通过；旧 `822ad…` 产物不能作为修复后最终证明。

源码冻结已复核相等：排除 `docs/**` 后，1374 个普通文件与 3 个删除路径、21,776,379 原始字节，路径 / 内容聚合 SHA-256 `37ee9ab3456b0953469042a52fd1216a0929e408bbeed2331bd380cf12785f5c`。清单和算法记录在统一门禁台账；未提交的源码身份不以旧 Git HEAD 单独代替。

仍然有效的边界：

- 临时会话 Java LSP 因 JDT 日志无法保证请求正文不写盘而明确拒绝；普通 Java LSP 正常可用。其余四类临时 LSP 保持受限独占状态。
- 外部 MCP 尚不发布 REPL / WebBrowser，需 connection-owned 适配后才能开放；普通 Query 的这两项能力正常可用。外部写入、Shell、网络与 VerifyJourney 已通过本机明确能力上限和逐次授权的统一管线。
- 唯一获授权的真实 `deepseek-flash` 官方 chat-completions 请求已通过：32 输入 / 2 输出 token、账本 $0.000012、thinking 关闭、无辅助 / 降级 / 工具，资源清理确认。它不覆盖其它供应商、模式或接口；5 个既有收费 LLM / 搜索测试仍未运行。真实远端 OAuth 提供商互操作仍未验证，本机 HTTP / PKCE / 刷新夹具与 Keychain 成功不等于远端全部通过。
- OSS / Meoo 发布、flyai、Java 插件、云桥接及其它用户明确排除项继续排除。保留原有可信 OSS 图片 URL 和正常语音数据链路，不将它们误删为发布功能。

## MCP 服务级开关（第一里程碑）

已实现：

- SQLite `config` 表保存全局服务偏好；未配置时维持原有默认值。读取异常失败关闭，保存异常保留最近有效状态，不改各能力的独立启用偏好。
- `GET /api/mcp/services`、`PATCH /api/mcp/services/{name}`、来源兼容 `PATCH /api/mcp/services/{name}/toggle?enabled=...`，同一管理器执行实际操作。列表区分启用意图与连接状态，不返回凭据、启动参数或远端 URL。
- 关闭时撤销目录、推进连接代际、取消在途建连/重连并关闭本地连接。禁止通过添加、重启、能力启用或诊断连接绕过开关；晚到发现回调不重新发布工具。
- 发现、目录注册与持有旧适配器的调用方均检查服务/能力权限；同服务其他能力仍启用时，关闭单项能力同步刷新目录。
- MCP 页面增加真实服务列表与开关；持久化成功前不乐观翻转，后端异常会重新读取实际状态并显示错误。
- 未实现的 `SDK` / `ZHIKUN_AI_PROXY` 传输返回失败，不再伪报已连接。这不引入已排除的 Java 插件或云服务。

实际验证：

| 项目 | 结果 | 证据 |
|---|---|---|
| `cargo test -p zk-mcp --lib --locked` | 196 通过，0 失败，0 跳过 | `/tmp/zk-mcp-services-tests-final.txt`，SHA256 `583ace1673534860de2a2b703fb59ec9835ccb3425e667f7439b95593e6ef8b2` |
| 前端服务 store / panel 专项 | 2 文件、7 项通过 | `/tmp/zk-mcp-services-frontend-tests.txt`，SHA256 `7d7a20a9a29b24a8ce67a32055a0456a6a17830484b02b8fb9d6580c085034c8` |
| 专属 ESLint / 前端 `tsc --noEmit` | 通过，exit 0 | `/tmp/zk-mcp-services-frontend-lint.txt`、`/tmp/zk-mcp-services-frontend-typecheck.txt`；均为空日志 |
| 新增 SQLite 宿主单测、服务 REST 集成用例 | 已编写，待统一跨 crate 测试 | `crates/zk-server/src/mcp.rs`、`crates/zk-server/tests/mcp_api.rs` |

首轮 Rust 失败为新测试 channel 类型及测试夹具仍依赖原 SDK 假成功状态，已修复并全 crate 复跑通过。测试没有调用付费远端 API；建连取消用真实 loopback HTTP listener 验证。

## OAuth、Schema 与单次运行 MCP（第二里程碑）

已实现并验证核心模块：

- 按 [MCP Authorization 规范](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization) 进行资源/issuer 绑定、HTTPS 元数据发现、显式客户端注册或 DCR、PKCE S256、随机单次 loopback 回调、令牌轮换及撤销。macOS Keychain 保存秘密，SQLite 仅保存非敏感绑定；失败不回退明文文件。
- 回调校验路径、Host、state、issuer 与重复参数；HTTP 授权请求验证 DNS 地址并固定解析结果，禁止带凭据重定向。取消在落密钥/落绑定阶段保持补偿顺序，保存失败不替换旧有效授权。
- 每次 MCP HTTP 请求发送前刷新即将过期令牌，串行合并并发刷新；真实工具调用不因刷新被重复发送。服务被关闭后，旧授权适配器不可继续发请求。
- MCP 服务页包含显式浏览器授权入口、非敏感状态与退出授权操作；输入客户端秘密仅存组件内存。
- Schema 瘦身仅删除 schema 位置的 title/examples/$comment；保留 description、enum、const、default、约束、引用和组合结构。
- `RunMcpConfig` 是严格解析、Debug 脱敏且不实现 Serialize 的内存承载类型。最多 16 个服务、256KiB；错误条目整体拒绝。只启动显式配置，不读取其他用户/工程/env 配置、不写全局信任或全局工具目录。
- `RunToolScopeFactory` 使用已落库的真实 setup invocation；STDIO 先登记 processGroup、再启动并绑定 PID，连接 cwd/roots 绑定会话工作区。目录独立叠加，拒绝全局同名服务/工具，执行前复检代际和实时权限。相同 Run 的临时缓存独立。
- prepare future 被取消时仍保留受控 startup 清理所有者；退出时整进程组确认消失后才记录 Released。STDIO stderr 和畸形协议行不记录原文，单帧限制 8MiB，避免泄密及无界累积。

| 实际验证 | 结果 | 证据 |
|---|---|---|
| `cargo test -p zk-mcp --lib --locked` | 209 通过、0 失败、1 默认跳过 | `/tmp/zk-mcp-run-scope-tests.txt`，SHA256 `689ec0d339a0d847de0c0218271e7120f67cbf82456e5bf7dc889a666ebb5229` |
| native Keychain 显式 `--ignored` | 1 通过，真实写入/读取/删除测试凭据 | `/tmp/zk-mcp-keychain-test.txt`，SHA256 `e8f0f85c3f5b2c9ca3a9d9113989676d3e83e9eee9dd5497ba942843ee0bd8dd` |
| MCP 前端专项 | 3 文件、10 通过；专属 lint 通过 | `/tmp/zk-mcp-oauth-ui-final.txt`，SHA256 `8f03b60e5b838e4478cd79732f012db261048f1e6caef7dccd7ea589bdd616ea` |

OAuth 使用真实本机 HTTP 授权/令牌服务器测试 PKCE、错误 state、资源不匹配、持久化失败、并发刷新和撤销；没有调用付费 API，不视为对每一家远端提供商都完成互操作验证。默认跳过的 Keychain 项已单独执行。单次运行 scope 测试使用真实 Python STDIO 服务和被取消的 sleep 进程；Query 根/附属子运行接线与整体门禁仍由后续跨 crate 验收确认。

## 继续实施项

- 外部 STDIO MCP：规范入口 `zk-server mcp-stdio --project-id …`；复用 TaskRuntime、工程工作区、会话/运行归属、资源监督及授权链；默认独立 DEFAULT 会话和只读能力上限，不继承普通 CLI 的 DONT_ASK。
- 真实 LSP：TS/JS、Python、Rust、Go、Java 五类私有固定工具链默认安装；不覆盖用户全局工具链，不在首次执行时自动下载。实现真实协议能力、权限、进程监管与专项验证。

以上后续项未完成前，不宣称全部本地工具对齐完成。

### 2026-10-07：外部 MCP 与私有语言服务器（实施中）

- 新增 `zk-server mcp-stdio --project-id ID`，通过现有本机认证 HTTP 入口创建独立 `DEFAULT` Session、`mcp` 类型 Task/Run；上下文具有随机能力令牌、30 秒失联租约、总期限和费用/Token 上限。工具调用继续通过真实绑定授权、持久 invocation 与原执行监督器；默认仅开放本地 Read/Grep/Glob/CodeIntel/LSP/ToolSearch，最终执行输入再次检查只读性，不能使用 CLI 的权限偏好扩大外部权限。EOF 请求取消后核验真实 Run 终态。这一新增链路尚待专项验证，不计完成。
- 五类语言服务器已安装到 `.runtime/lsp` 私有目录，命令不依赖执行时 `npx` 下载，不修改用户全局 Node/Go/Java 默认。固定版本：Node 22.23.3、TypeScript Language Server 6.0.1／TypeScript 6.0.3、Pyright 1.1.414、rust-analyzer 2026-10-05、Go 1.27.1／gopls v0.23.0、Temurin 21.0.12.1+1／Eclipse JDT LS 1.61.0。安装器使用官方 SHA-256、npm lock 与 Go 校验数据库，失败不替换最近有效 manifest。Go 官方下载多次截断，镜像取得的文件经 Go 官方摘要独立核验后进入缓存，未接受摘要不符的文件。
- 安装日志 `/tmp/zk-lsp-install-fifth.txt` SHA-256 `19cc6995c7d74b2709597e51ba862bd752b6d26f8a5b93f34c013822ca1d3274`；安装 manifest `.runtime/lsp/current.json` SHA-256 `f6061d13bd3ddec25bebcb04ec3190a5685b2304e2280ad228bbace8c40044be`；随后只读 probe 为 `ok:true`。安装成功不代表五语言功能测试已通过，真实验收新增于 `crates/zk-tools/tests/lsp_native.rs`，需显式执行。
- 默认 LSP scope 只注册工具，不在普通聊天时启动进程；第一次调用按语言／工作区懒启动真实语言服务器。每个 scope 使用独立资源跟踪器，真实 setup invocation／TaskRuntime 仍是持久归属。进程组、清理落库失败保留 Peer／transport 和 lease 支持重试，不通过丢弃句柄来伪造释放。未带版本的推送诊断明确标记 `unverified-unversioned`，不冒称与当前文档匹配。

### 五语言 LSP：最终隔离配置的真实验收

- 已完成真实 TypeScript/JavaScript、Python、Rust、Go、Java 协议实现与固定私有安装。Rust 使用已有具名 `1.97.1` 工具链，显式安装缺少的 `rust-src` 组件；不更改用户全局默认，运行时核验 rustc/cargo 文件摘要。运行过程不下载工具或依赖。
- `sandbox-exec` 限制语言服务器和子进程：工作区、固定工具链及 Run 独占状态可读，只有 Run 状态可写，网络禁止。只允许同 sandbox 的子进程信号；不允许任意 Mach 服务访问。macOS 动态加载器所需根目录权限仅限 `/` 本身，不递归开放内容。
- 宿主可以通过 `ZK_LSP_READ_ROOTS` 的绝对路径 JSON 数组显式授权额外依赖目录，模型参数不能扩大范围。返回的源码位置也必须属于工作区、明确授权依赖或固定工具链。Rust build scripts/proc macros/check 自动构建禁用，Cargo `--locked`/offline；Go readonly/offline、CGO 关闭。受限依赖不能静默下载。
- 每 Run 的语言服务进程、索引、JDT 可写 OSGi 配置均独占，清理失败保留真实句柄与 lease，重试确认后才删除状态目录。临时会话下 Java LSP 明确拒绝：JDT 的工作区错误日志目前无法保证正文不写盘；此边界不影响普通会话 Java 能力。
- 方法包括定义、引用、hover、文档/工作区符号、诊断、实现、调用层级；兼容来源 `operation/filePath/character` 和 Rust `action/file_path/column`。外部 URI 过滤，UTF-16 坐标转换，单文档 10 MiB、协议消息/响应有界。无版本的 push 诊断明确标为未验证版本。

| 最终实际检查 | 结果 | 日志 SHA-256 |
|---|---|---|
| 私有工具链安装及 probe | 成功；manifest `e789c506977fdb30ffec3c14d467b60940c65b3af704923ea11103ab21f12cfd` | `/tmp/zk-lsp-install-sandbox-final2.txt`：`a3343195734bf50e2d1ee27ec3aaf0ec7a6a59b59738a5a1c94ae631a2c31888` |
| LSP 单测与真实 OS 拒绝测试 | 4 通过；工作区外内容、项目写入、实际 loopback 连接均拒绝 | `/tmp/zk-lsp-sandbox-unit-final3.txt`：`b1d9b89582ed8254768ea7f501d2e0d78ca39921cfd6621736e18d4d5b19652d` |
| 五语言真实服务器，显式 `--ignored` | 1 通过，17.76 秒；五类函数符号、TS 多方法、5 个进程组消失、状态目录删除、重复清理幂等 | `/tmp/zk-lsp-native-sandbox-final2.txt`：`7f1ea1536376316a52065ef3f9af19f9c25b5fd8e10a50a1aa0c4221edd6e158` |
| MCP scope 清理落库失败重试 | 4 通过；同一资源被确认前不删除 transport/lease，不重启进程 | `/tmp/zk-mcp-cleanup-retry-final.txt`：`a66d2c7ff16d0bddbf4ddd42a89233d005bfa467f1f57aac1e510bec84302bbd` |
| 外部 MCP Context | 1 通过；独立 DEFAULT、Read 可用、写能力/伪造令牌拒绝、持久取消 | `/tmp/zk-mcp-external-final2.txt`：`f4abb29d9a4397440b8af22f82152ae2756f3bb3582fe62ec2f9a2f79a3991cb` |

该日志中的 STDIO 项整体仍失败：真实进程已完成 initialize/list/Read、EOF 清理并 exit 0，但测试末端误查询不存在的 `runs` 表；已改为 `run_envelopes`，等待正式复跑，不能按前段通过宣称整项通过。此前取消原因错误字符串已修为正式常量 `EXIT_USER_CANCELLED`，没有放宽数据库约束。

### 临时会话证据与浏览器（实施中，尚未开放 no-session）

- 新增有容量收费的 exact-Session named bytes 索引；名称/内容/hash 都仅在 RAM，随机引用入库，附属 Session 共用容量但不共享命名 Blob 的隐式访问。
- Evidence 的 kind/claim/type/summary/hash/meta 使用严格内容 codec；immutable/idempotency/producer 约束保持。verdict reason 用诊断 codec，正文过期后仍能保留明确的状态变化。
- Blob GET 先证明该 Session 有对应证据，再读受限工作区或 RAM；同一工作区及已知摘要不形成跨会话授权。
- 临时 VerifyJourney 默认关闭录制，显式 trace/video/HAR 在执行前拒绝。截图和失败语义快照使用 RAM；Python 禁止临时浏览器下载、保持截图响应在内存，并抑制该请求及其子任务的正文日志。共享 Playwright driver 若开启 DEBUG/PWDEBUG，则执行前拒绝该临时浏览器请求，避免原始协议日志泄漏。
- Python 定向 **103 项通过**，`/tmp/zk-ephemeral-python-evidence-final.txt` SHA-256 `a745b34bd2c12e2bdc931774e55b133438cef1b461b4292f68f2a9de57db02ca`。新增 DB/WAL 扫描、Blob 跨会话、真实临时 Chromium 验证待执行，未计通过。
- 临时 `WebBrowser` 和 REPL 目前执行前明确拒绝。后续必须接 Run-owned browser/interpreter scope，复用原监督器、取消与费用边界，并保持 Python/Node/Ruby 参数兼容；这是待实现项，拒绝不代表完成。正常持久会话入口继续保留。

### 临时证据实际回归与受管交互工具（后续增量）

- 实际 DB/WAL 隐私与不可变回归 1 通过，`/tmp/zk-ephemeral-evidence-db.txt` SHA-256 `13eb7127ddbb508e53bb3359f7c8215dfa4fdf893778176044c89758e620688a`；named RAM 索引容量/精确归属/幂等 1 通过，`/tmp/zk-ephemeral-named-memory.txt` SHA-256 `b160ae0d47cf0a4023ef07818f236d5141f90040303b5d11cca8a5717853e0cc`。
- 证据 API 全部 5 通过，包括同工作区跨 Session 拒绝、临时 Blob 不建磁盘目录、正文/hash 不出现在真实 DB/WAL。日志 `/tmp/zk-ephemeral-evidence-api.txt` SHA-256 `34e327e070388c974c9200d4eb97a637b5aab447404b48830d4b7f98bc4146c9`。
- Rust→UDS→实际 Chromium/HTTP 的普通和临时 VerifyJourney **2 项通过**；临时截图 RAM 保存、禁录制、预览进程和浏览器释放。日志 `/tmp/zk-ephemeral-journey-native.txt` SHA-256 `747df7fa6134d9055cda783887ddad8336f962e91d50478475bba997cf5d4474`。此记录早于后来增加的多次交互 WebBrowser 断言，后者仍待新一轮实际运行。
- Python 全套在证据阶段 **324 通过**，`/tmp/zk-ephemeral-python-full.txt` SHA-256 `d13d191602ee45976d51ed0a6ee9ae1835e7e15454e4bda4a9ab2360d81bca60`；后续 owned Browser 的 identity/严格存在性/关闭竞态与日志 10 项通过，`/tmp/zk-owned-browser-python.txt` SHA-256 `9f39cddcde6d5038bc8db373a34293a3515b328cecc2daba891c67c410dc2cb2`。后续 Python 变更仍需整套复测。
- `BrowserRunScopeFactory` 已新增并接默认 scope：仅临时 Run 且浏览器已启用才适配；真实 setup invocation 预留资源，模型别名映射成 Run 私有随机 sidecar ID，串行操作，截图/语义内容作为 RAM observation receipt（inconclusive，不把截图冒充验收通过）。清理使用 scope 子取消令牌，不取消父 Run；失败保留资源重试。新增实际交互/取消/身份专项待跑，不能计完成。
- `ToolRegistry::adapt_bound` 仅供可信宿主包装已有精确 binding；保留 live 可见性、child 投影与撤销，源绑定变更后不可自动重新绑定。普通 MCP overlay 仍不能覆盖任何现有/隐藏同名工具。相关 registry **16 项通过**，`/tmp/zk-temporary-shell-registry.txt` SHA-256 `900d9199d7c0761c7f48ddf0a79968889b5f2d2afcf286b26ac3e02a61c31e16`。
- Reverse MCP 的 tools/list 与 tools/call 共用需 Engine 投影的能力过滤；真实可信机器证据 producer 也必须走 Engine，远端同名/任意 metadata 不获得证据身份。带 Run 的目录需匹配持久 Session 归属，临时 scope 丢失明确拒绝。最新 server 定向待跑。
- 用户最终决定：**普通 REPL 保留跨轮次状态，采用 Session 所属受管解释器服务，提供停止入口，并参与删除/合并门控；临时 REPL 在 Run 结束清理。** 不将普通 REPL 悄然改为每轮销毁。当前先完成三语言临时 scope/固定内存驱动与普通 Session 别名隔离；普通服务端口/生命周期、停止入口及实际三语言监督测试仍在实现，不计完成。


### 本机 Office／Python 最新验证与 REPL 服务收敛

- owned Browser 最终 Python 全套 **329 通过，0 失败**，37.58 秒；`/tmp/zk-owned-browser-python-full.txt` SHA-256 `f333de460b1ba78e30c85bb9cdf7114c57fbf4f09f409bc6477ffcb518a853ea`。9 条既有依赖/弃用警告不视作失败。
- `./dev test office` 实际原生 **41 通过**，23.32 秒。证据根 `/var/folders/g_/cgkxr_w91xg7tx8n84hjt9zm0000gn/T/zkcode-office-zs0ua3iq`；`pytest.log` SHA-256 `b864319e365220c0024222c1efa68858210b3ade9bdf70dbce5ffcbc4cb5b8d5`，`run-identity.json` SHA-256 `416fc21714516deb2f33920bc897545e92d071d908384cc242e019c82e1ff62d`。
- 该次 Office 的 `evidence/preflight-toolchain.json` SHA-256 `1f7dd7f399283a37e287036ac3a311655084a314986ca41962aeac7a23cff49c`，含所有执行文件、Noto CJK 字体、两种中文 OCR 模型的实际路径及摘要；`evidence/native-browser-identity.json` SHA-256 `a3f4ff039b36a33d29a1ef3f6cdebb2a4024c31d99658d9c5fb6ee0570224fca`（私有 Chromium Headless Shell 145.0.7632.6）；`evidence/preflight-python-packages.json` SHA-256 `f933e1c620db78602549dacdd83865b8ae73b815a49881e93ccbfef274ec937c`。工具使用实际可用的 LibreOfficeDev 26.8 alpha、Poppler 26.10、qpdf 12.4.2、FFmpeg 9.0.2、ImageMagick 7.1.2-32、Graphviz 16.1、Pandoc 3.12、Tesseract 5.5.3；不冒称稳定版 LibreOffice 已安装。工具维护继续使用既有安装器、doctor、原生 runner，更新后重新保存身份并执行此套件。
- 普通 REPL 服务已接原 TaskRuntime 独立内部 transcript、10 分钟空闲／1 小时最长生存、取消与物理资源账本，用户聊天跨轮次复用解释器。GET/DELETE `/api/sessions/{id}/repl-service` 要求精确 `x-session-id`，UI 停止需确认，只有实际 cleanup confirmed 才显示停止。真实 Rust 三语言、服务跨轮次与停止专项尚待执行；先前清理不明后的重试使用独立精确归属 reconciliation，不改不可变 TaskResult。
- CRA 自动预览识别标准 `react-scripts start`，显式绑定 loopback、固定 PORT、关闭浏览器自动打开和交互改端口；复用原 VerifyJourney 授权及预览租约。预览脚本在真实进程 PID 归属落库后才通过 stdin gate 启动。新增真实 npm 启动协议 fixture；它验证环境／端口／清理契约，不冒称已下载并测试 CRA 的 React 编译器。该 Rust 专项待执行。


#### 外部 MCP 早期中间态（已由后续统一管线替代）

此处保留早期状态的原因记录：当时外部 STDIO/HTTP context 仅开放只读工具，写入、Shell、网络与 VerifyJourney 因缺少统一后处理暂时隐藏。该阶段已由下文的真实 Engine 窄口、显式候选授权和完整管线专项替代；不再把长期过滤当作功能完成方式，也不另建第二执行器。


#### 已执行的服务与预览专项（待最终源码统一复跑）

复用同一实际编译的 `zk-server` lib test binary，SHA-256 `a5989eaa0875467b0ae73d1d5f55c9bc21b98341719b6bea34f824ecab9f67fa`：

- REPL 服务 **2 通过**，0.69 秒，`/tmp/zk-repl-service-native-first.txt` SHA-256 `9f72370fc9d54043c1b9a1f4fe0f8d3e09680a9cee9c5fc9936651f1506dd6ef`。真实 Python 跨 Query 状态 42→43、取消与进程组消失；SQLite trigger 注入 Released 写失败，先如实显示 cleanupUnconfirmed，再保留 owner 完成重试；不可变 TaskResult 的 ID、状态和摘要不变。重载管理器也从持久元数据恢复真实停止状态。
- 预览 **4 通过**，1.65 秒，`/tmp/zk-preview-cra-first.txt` SHA-256 `8a218b4b707935dbb9784e0904470faf811d73fc7ba87bf01efd42ca4803f1f1`。覆盖静态 HTTP、CRA 标准脚本及真实 npm 环境协议、进程/端口释放、PID 归属写失败时不执行请求脚本。
- 此后 startup guard 的已排队 Run 取消、逐解释器 LRU 兼容及 OpenAPI 注册等增量，仍需最终 Cargo 专项和发布门禁复跑。它们不能仅凭上述旧二进制记录计为全部通过。


#### REPL／预览最终源码专项补验

- `CARGO_INCREMENTAL=0 cargo test -p zk-tools --test repl_scope --locked -- --nocapture`：真实 Python／Node／Ruby **3 通过，0 失败**，包含 top-level await、异常后继续、输出有界、三解释器 LRU、取消、登记失败不执行与释放重试。
- `cargo test -p zk-server --lib repl_service::tests --locked -- --nocapture`：**2 通过，0 失败**；包含跨用户 Run 的普通服务状态和不可变结果保持下的真实清理重试。
- `cargo test -p zk-server --lib python::tools::journey_resources::tests --locked -- --nocapture`：**4 通过，0 失败**，标准 CRA 命令协议、已有预览租约和启动 gate。
- 上述均在最新服务 startup guard 与 LRU 实现后重新编译执行；完整 Query 两轮入口与 REST/native Browser 继续单列验收，不由原生管理器测试替代。

证据 `/tmp/zk-repl-scope-final.txt`，SHA-256 `bb17804c8de6b99cd5367a908fb4688c49fba24a671d56289529f9f225dee52b`。

证据 `/tmp/zk-repl-service-final.txt`，SHA-256 `c0cae12e02aaddcb14cd9b0dc3eaab70be937ef8d8d968d0982d5074e97e22a0`。

证据 `/tmp/zk-preview-cra-final.txt`，SHA-256 `14ecf7aa561a9b819a07f71a9a9d3d3bf029490e3bf5f3cbdaa4e7040ad3a8fe`。


#### 外部 MCP 原生管线接线与候选能力

- 先前只读链路的最终已执行组合：MCP context **2 通过**、REPL REST **1 通过**、原生普通/临时 Journey 与 owned Browser **2 通过**（6.42 秒）。后者包含真实多次 JavaScript 计数、RAM 截图、错误 Run 拒绝、重复清理、父 Run 不被子 scope 清理取消。日志 `/tmp/zk-mcp-candidates-repl-browser-final.txt`，SHA-256 `78ee644c40b94580e2f8d7fffa1103f716670cce035343e627999a45b7654228`。候选申请测试通过真实固定选择答复；任意文本不授予，context token 无法自己批准。
- 新增 `zk-server mcp-stdio --project-id ID --request-capabilities write,process,network`；参数只创建本机界面待确认申请，不直接授予能力。独立会话保持 DEFAULT。可以只申请 `write` 或 `network`；任意本机程序能写入及联网，因此 `process` 必须同时明确申请另两项。
- 外部工具已接 `ConversationService::execute_external_bound_tool`，使用现有 Engine 的 Hook、精确绑定 Admission、Supervisor、不可变结果、文件快照与产物/证据后处理。候选上限在原始、PRE 后和最终授权输入复查；VerifyJourney 启动预览命令或自动检测命令同样要求三项。尚未接 connection-owned 解释器/浏览器交互的 REPL/WebBrowser 不对外发布。
- `_meta.operationId`（兼容 `_meta.toolUseId`）是独立于 JSON-RPC `id` 的操作身份。缺省由服务器生成并在结果返回；完成后同输入重放原结果，不重复副作用；不同输入冲突、未知效果不自动重做。客户端不能在没有稳定操作身份时把传输重试视作幂等。
- STDIO 心跳核验 RPC 错误并发送候选目录变更通知；16 个等待权限的调用不会挤占心跳/取消控制面。EOF 通过 TaskRuntime 请求取消并检查真实清理状态，管理查询仅由已认证本机 adapter 使用本地 Bearer，context token 无管理权。
- 该增量的真实统一执行与 STDIO 测试已于下文所列最终专项通过；更早的只读和候选层记录不能代替此门禁。


#### 临时正文日志与安装器专项

- Write/Edit/Notebook 的快照与产物诊断、原子写的 fsync/临时清理/非法旧摘要诊断，以及 VerifyJourney 证据失败诊断仅保留固定错误 code、工具名、字节数量或 `io::ErrorKind`。不把 sink 的任意错误字符串、输入路径、文件正文或原始摘要写入日志；普通模式也保留同样安全的诊断。
- `zk-tools --test ephemeral_tool_logs` **1 通过**：真实三种工具在普通/临时两种模式共六次命中含私有正文/路径的恶意错误 sink；tracing 捕获六条可诊断失败记录，无私有路径、正文、错误回显。
- 正式 managed Python **3.11.15** 运行安装器测试：LSP **3 通过**（损坏缓存、逃逸归档链接、原子 manifest 回退），OCR **4 通过**（保留有效现有模型、拒绝摘要错误与 symlink、原子安装）。未联网。首次直接使用系统 Xcode Python 3.9 因缺少 `tomllib`/安全 tar API 失败；正式 dev 入口使用 `DEV_PYTHON`，因此该旧解释器结果不伪记为通过。

证据 `/tmp/zk-ephemeral-file-tool-logs-final.txt`，SHA-256 `ea7843fbc12a39e0efdfb5fb17a648d33233fdaeace8c1e3ca5473db6c5afbc0`。

证据 `/tmp/zk-lsp-installer-managed-final.txt`，SHA-256 `e6db6f5128a08613a4f836bad2a420f4c12e8d24613cfd816134208809c72705`。

证据 `/tmp/zk-ocr-installer-managed-final.txt`，SHA-256 `da00793b7c0aab8122cf5032bd2caca4a537bb9059fc2acbb7aa2e26f7b2bc84`。


#### 外部 MCP 完整管线专项与可信会话用途

- `CARGO_INCREMENTAL=0 cargo test -p zk-server --test mcp_external_capabilities --test mcp_stdio --locked -- --nocapture`：**4 通过，0 失败**。外部 Engine 用例 2 项（0.58 秒）：真实文件快照、写入、稳定操作身份重放不重复覆盖、冲突拒绝及恢复原文件；普通 Session 被设为 AUTO_APPROVE 后，外部调用仍走固定 DEFAULT，真实 requestId/派发 generation/ACK 后本机 allow_once 才执行 Bash。STDIO 原生子进程用例 2 项（5.01 秒）：initialize/list/Read、CLI 候选申请、审批前不开能力、真实本机答复后目录更新，以及 EOF durable cleanup。
- 初次 Bash 测试未模拟真实派发与 ACK，被正确拒绝 PERMISSION_DELIVERY_STALE；补齐握手后通过，没有放宽生产授权。候选 ELICITATION 等待时 Run 属于 waiting_user，测试也确认工具执行必须等待回答，不能把此状态当作仍可执行。
- 日志 `/tmp/zk-mcp-external-engine-stdio-final2.txt`，SHA-256 `43680ad758d4a48a8b41d1096b2fdd6ebb42212df823a839938b3d0230187a31`。未调用真实收费服务；本机网络候选允许配置的第三方服务，不声明其免费。
- Session summary/detail/REST export/WS metadata 新增 `purpose: chat | mcp`。只从同一数据库快照中真实 MCP 根任务派生，终态后仍保留用途，客户端 title/config 不可伪造。MCP Session 保持列表可见，用于本机 Activity/权限确认；WS 显示固定 DEFAULT。合并 writer 在事务内拒绝此类源会话，普通 Query 继续由既有 DB guard 拒绝。
- 上述用途字段新增的 REST/WS 回归及 resource observer 日志注入第二用例尚待最终统一执行，**不计通过**。资源错误日志已改固定 `EXECUTION_RESOURCE_UNCONFIRMED_PERSIST_FAILED`，失败仍保留 Unconfirmed，未把不明清理冒充成功。


#### 默认安装独立核对

- 直接读取固定源 `053adf90:Dockerfile` 的适用安装项，逐项核对当前 `configuration/dev-toolchain.toml`、`scripts/dev/documents.sh` 与 `scripts/dev/sync.sh`：默认同步包含 LibreOffice、FFmpeg、ImageMagick、Poppler（渲染/提取/信息）、qpdf、Graphviz、Pandoc、Tesseract 和简/繁中文模型、CJK 字体。macOS 使用 Noto CJK / 系统 PingFang 替代 Linux 文泉驿组合；未引入 Docker 或 Linux 运行要求。
- LSP 默认同步五类服务，固定下载 SHA 与 npm lock，私有 `.runtime/lsp` 目录、阶段目录原子替换；Rust 只安装具名固定工具链的 rust-src，保留用户默认工具链。执行工具时不调用 npx 安装。实际独立 probe 再次通过（下列日志），不能用 probe 替代五语言协议/监督原生测试；后者在清理 reconciliation 改动后仍需最终显式运行。
- 已检索真实生产工具/REST/安装/依赖入口，没有 OSS/Meoo/flyai 发布调用或安装项。残留 `oss_trust`、`ZK_OSS_*` 只读 URL 信任校验与 `ArtifactPublish` 风险判定在 `08cbdc45` 已存在，无发布实现；语音服务返回的 OSS URL 是正常云 ASR/TTS 数据链路。保留这些边界以免破坏已有可信图片或语音能力，不把文本命中等同于发布能力。

实际检查 `/tmp/zk-lsp-probe-alignment-final.json`，SHA-256 `19c681aa6210c3ca32497d9b49447fdf1e57560639f6128b64f036cac3d1f1e5`。

实际检查 `/tmp/zk-office-probe-alignment-final.json`，SHA-256 `9135e1e026db011a943e8b53a839e381d07ebf50db21b86cb8be8154b591435b`。

实际检查 `/tmp/zk-purpose-machine-contract.txt`，SHA-256 `dd7dca9f28ae775b866302044d05a2c4700f78b16d8efd9ce8ec2ee7f9173de5`。


#### 静态检查收敛补充

- 严格 Clippy 分层检查期间修复本工作线的 LSP/REPL、MCP/OAuth、Run scopes 与 Hook 诊断规范问题：公开失败路径补充错误契约、避免无用分配/大栈数组、显式资源状态分支；不以全 crate lint allow 代替修复。少量必须保持同一资源锁/启动事务的大函数使用逐函数理由说明。
- Hook PRE/POST/Stop/通知和 matcher 失败日志不再回显不可信错误字符串，保留固定 code；实际安全拒绝、结果不可变和取消规则不变。新增 `zk-engine/tests/hook_diagnostic_logs.rs` 使用真实本地命令生成含正文的非法 decision，验证 fail-closed 与日志边界。该新增测试尚待统一门禁，不计通过。
- 严格 Clippy 和所有最终发布门禁的最终结果由统一发布记录给出，本处不把分层修复或编译成功记为完整通过。


#### 外部 MCP 的 Hook 能力上限补强（待统一执行新增回归）

- 发现并修复一条间接扩权路径：仅获 `write` 的外部连接可以修改工作区 Hook 配置；如果后续只读工具照常运行该配置中的命令，就会越过 `process/network` 上限。现在外部调用从真实本机已批准的原子 capability 状态生成宿主 typed policy，沿同一 Engine 管线进入 HookContext；不从 JSON 工具参数或 `.zk/hooks.toml` 声明中推导授权。
- 命令 Hook 必须同时获得 `write/process/network`，HTTP Hook 必须获得 `network`。PRE 在配置匹配后明确拒绝；POST 记录安全拒绝且保留已经提交的真实工具结果；Stop/通知也在实际执行边界检查。取消后的 Hook 不启动，Stop 不推翻用户取消。普通会话已配置 Hook 没有外部上限覆盖，保持既有行为；临时正文仍禁止经外部 Hook 输出。
- 新增真实服务器用例：write-only 写入 Hook 配置后 Read 不产生命令副作用，后续本机批准三项能力后同一配置可执行，操作重放不重复执行；已有 HTTP Hook 在未批准网络时没有建立连接。新增 Engine 用例覆盖 PRE、POST、Stop、通知及取消；另有真实 Read 失败用例验证禁止或允许 POST Hook 均不能改变原始错误结果或把展示文字塞入工具正文。以上新增断言尚未运行，待最终编译与专项日志确认后记录，不沿用之前四项 MCP 测试通过证明本次修复。


#### 会话启动与通知 Hook 的执行归属

- SessionStart 从无 Task 的 Query/REST 准备阶段移到 Engine 的真实根 Run 建立后；WS 首次执行同样生效。数据库中的首次根 Run 决定是否触发，续接不重复，fork 的新会话独立触发。SessionStart、UserPromptSubmit、RunStart 均先于主模型和可选语义记忆精排请求。
- Query 在入口冻结绝对总期限，准备、Hook、模型共用剩余时间；Hook 不重新取得完整期限。现有 Supervisor 负责命令进程组/HTTP 请求及取消，命令先登记、绑定后开闸，正文不进入资源账本。异步通知保持运行期异步，并在所属 Run 终态提交前排干。
- RunEnd 在主体结束、结果拟定但 Run 仍活跃时执行；MessageSent 在同阶段仅表示消息已持久化，不表示客户端完成帧已发送。随后确认资源释放再封存 Task/Run 终态并发送完成帧。取消或期限已到不会启动新的外部结束通知，安全 Hook 失败明确留为失败且不改写已发生的工具效果、原始模型消息或费用。
- 新增 `zk-engine --test session_hook_ownership` 真实 Shell/SQLite/脚本模型场景：首次/续接/fork、阻塞 Hook 取消、准备时间计入绝对期限、异步 RunEnd/MessageSent 在终态前排干，以及启动/结束安全 Hook 失败。所有场景启用语义记忆精排；失败/取消断言主模型和辅助模型均零请求。此节生产接线和测试源码已完成，专项尚待统一执行，未标记通过。

- 统一回归首轮 `session_hook_ownership` 已实际 **6 通过，0 失败（2.72 秒）**，日志 `/tmp/zk-alignment-workspace-tests-first.txt` 对应此 harness；该整批另有失败，不能记作完整门禁通过。随后补上 Hook 专用资源注册/物理启动的原子 owner 校验与父通知封口，因此此新原子边界仍须再次验证。
- 同一轮架构检查正确发现普通 REPL bridge 嵌套调用服务 `Tool::execute`。已改为服务 scope 准备后提供专用 `ReplServiceHandle`：解释器操作继续属于原服务 Run，外层 REPL 工具仍只经过一次 Engine 授权/执行器/结果管线；没有第二执行引擎，也未给架构测试增加 `Tool::execute` 例外。额外约束该物理端口只能由私有服务 bridge 调用；普通跨轮次变量、停止、删除/合并门控仍需在最终当前源码上复跑。
- MCP 新 Hook 能力用例首轮因真实 scope 已创建 `.zk` 而在 fixture 再次 `create_dir` 处失败，改为幂等目录准备；未修改生产授权逻辑。旧 reverse MCP 目录测试已改为未授权默认只读，显式写能力继续由真实本机批准的独立集成场景验证。三项新增 Hook 用例尚未计为通过。

- 首轮完整日志 SHA-256：`c2fa640ada392ef0e994af50dba2f767ea3c227707e85f95f47488e0c0ad6704`。其中 `hook_diagnostic_logs` **2 通过**（包括外部上限 PRE/POST/Stop/通知及取消），`ephemeral_tool_logs` **2 通过**（包括 resource observer 恶意错误注入），`repl_scope` 三语言与取消/清理 **3 通过**。本轮 LSP 原生测试明确 ignored，未计为通过；REPL typed port 与 Hook 原子 owner/seal 改动的后续验证仍待执行。

#### 可视化入口实际接线补齐

- 代码路径面板增加文件、函数及深度输入，非 API 函数可明确请求追踪；卡片只预填参数，须用户点击才通过既有 `AnalysisRequest` 和 Rust 工作区绑定进入 Python。切会话清空表单与结果，编辑参数取消旧请求，无自动分析。
- API 序列图卡片改用独立、会话所属的视图状态，仅筛选或定位当前已有工具调用；不再误写 API 契约 store。找不到匹配时明确提示，不用卡片 props 生成调用或结果。切会话清理筛选和详情；详情每次从当前真实记录推导，不保留旧会话的原始结果对象。
- 序列图读取正常恢复链已投影的 `tool_use.result`，保留真实失败；尚无结果的调用标为“结果待确认”，不显示成功。普通卡片点击同时接通移动主区导航。
- `vitest` 专项 **4 文件、24 通过（7.64 秒）**，`/tmp/zk-visualization-entry-focused-final.txt` SHA-256 `199d9ded5e18ab37d8a0350eb6167dcf75e7798a70f75131a44004b309fe9fb3`。专属 ESLint `--max-warnings 0`、全前端 `tsc --noEmit` 均 exit 0，日志 `/tmp/zk-visualization-entry-lint-final.txt`、`/tmp/zk-visualization-entry-typecheck-final.txt` 均为空，SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`。当前整体前端/真实后端 E2E 仍由统一门禁重跑，不用本专项替代。
