# 298d2b3 必要缺陷修复与验收

本记录保留原 16 组修复的范围与历史验收。其后经用户授权的 schema 修复及验证单列于[含证据会话删除修复](evidence-session-deletion-2026-10-08.md)，不沿用本文的旧源码身份和测试数量作为新版验证。

基线：`298d2b30b3d310420c12d953bb4382ec8064d1bb`。当前工作仍在原分支；本轮不提交、不推送、不创建 PR、不重启用户服务。数据库主 schema 未变更，未重建用户数据库。未知价格和计费策略改造不在本轮范围。

本记录补充四份外部审查的采纳依据；它不把外部报告、旧门禁数量或仅前端 mock 当作完成证据。此前 2026-10-08 曾据已有门禁判定可提交；后续独立复核确认 R1–R7 及慢清理提示覆盖缺口，此前提交结论已撤回。**这七项及慢清理提示缺口现已修复，并完成新的适用发布门禁，详见 [后续修复报告](review-followup-2026-10-08.md)。**既有成功证据删除约束另行披露，不据门禁通过宣称整个仓库无缺陷。这不代表已部署到正在运行的服务，也不代表已完成真实付费供应商验证或本轮明确排除的计费改造。

## 范围与生产入口

| 组 | 报告依据 / 采纳理由 | 局部实现与保留边界 | 验证入口 |
|---|---|---|---|
| 01 Grep HTML | DSH E1、Kimi P0；工具结果来自不可信文件 | `SearchResultRenderer.tsx` 使用文本节点和 mark；不改变 Markdown | 同名前端回归：标签、实体、事件属性、Unicode、查询特殊字符 |
| 02 文件权限 | DSH G1；原子替换确实重建 inode | `zk-tools/atomic.rs` 保留访问/执行位，同一独占 FD 创建即 0600，正文 flush 后才设置最终权限；新文件通过无正文探针保留 umask；复核 inode/mode/content | `atomic::tests`，含 0600/0644/0755、并发 chmod、同内容替换、chmod 失败、创建权限及 umask 探针 |
| 03 密码快照 | DSH F2；原生 ARIA 在真实 Chromium 中读取密码 | 经用户确认，统一采用同步安全 DOM 语义投影，`tree.source=safe_dom_v1`；原生 ARIA 标为未请求。不读取密码 value、长度、摘要，不改网页 DOM | Python 真实浏览器 getter canary、动态 text→password、普通输入；Rust 原生 Journey 联调 |
| 04 最终权限复核 | Kimi P2；批准与启动之间模式可变 | `zk-authz/service.rs` 在既有准入事务读取当前模式；旧授权不得穿越 PLAN；保留安全读取/内部操作 | `decision_matrix` 模式切换、批准单次/记忆授权；完整 authz suite |
| 05 会话恢复 | Kimi P1-7/P2；重复 REST 等待和队列溢出可卡绑定 | `dispatch.ts` WS 提交后完成绑定；独立 REST 8 秒覆盖 body，取消和版本防迟到；5,000 帧首次溢出仅重绑一次，原期限不延长 | `dispatchSafety`、`dispatchRecovery`、真实 Rust E2E |
| 06 合并 | Kimi P1-8、GLM D1/D2；writer 内慢 I/O 与累计预算缺口 | 显式 reserve 短事务→一致读/私有暂存→有界导入→封存→发布；单库一个 worker；32 MiB 单记录、64 MiB 总元数据、20k 条（含继承）；120 秒捕获导入期限 | `session_merge::capture` 真 WAL writer 并发、预算、封存、恢复；server merge summary 真实 SQLite 故障 |
| 07 浏览器生命周期 | GLM P1、Kimi P2；分配等待与迟到 ready/空闲清理 | 30 秒绝对创建期限；保留迟到资源回收主体；Session 浏览器+Run 使用租约（15 秒续租/60 秒失效），临时模式 Run 结束释放 | Python lifecycle 与真实 Chromium；Rust owner/resource 回归 |
| 08 取消 / REPL | DSH B2、Kimi P2；取消保存失败与启动槽位竞态 | 3 秒后一次非终态通知，唯此通知可绕 outbox；原 worker/lease 保留。同步 Query 返回 503 和原身份。REPL Starting/Running/Stopping+generation 保留普通跨轮状态 | 真实 SQLite trigger + HTTP/SSE/WS；REPL 并发启动、等待方丢失、迟到 receipt/超时 first cause |
| 09 快照 | GLM E7；保存失败后旧对象可冒充成功 | `session_snapshot.rs` 唯一同目录临时文件、文件与目录同步；替换后同步失败明确未确认；成功直接返回本次保存对象 | engine snapshot 和 `session_snapshot_api` 的 I/O 故障、并发保存、内存临时模式 |
| 10 浏览器证据 | DSH F1/F2、GLM P3/P5；语义失败/录制归档状态缺失 | complete/partial/failed 分组件报告；拒绝空 Journey。私有暂存→封存→Rust 校验/证据持久化/后处理→ACK；ACK 不重跑 Journey。复用 execution_resources 元数据/CAS | Python 真录制；DB 状态回归；`verify_journey_native` 真实 UDS/DB/监督器 |
| 11 协议诊断 | DSH D4、GLM D5；无限行/错误体与诊断泄密 | 增量 SSE 行/事件 16 MiB，HTTP 错误体 256 KiB；保留先行 usage/终态与 HTTP status/Retry-After；URL 凭据移除；Hook 显示 Authorization/Bearer 脱敏；Responses Stream 独立类型 | zk-llm 全套、同字节不同切块、错误体本地 TCP；authz 脱敏回归 |
| 12 MCP / 本机 API | DSH C2/G3、Kimi P1-6；目录碰撞/关闭责任/跨站访问 | merge/Query 恢复加入现有 Origin/Bearer；命名规范化/前缀/工具碰撞拒绝；关闭未确认保留 transport/PGID/lease；OAuth 先持久停用再 Keychain 清理 | MCP 进程树、close abort、重启/Keychain fixture；真实 Router；原生 Keychain 另验 |
| 13 Hook 校验 | Kimi P2、GLM E5；保存与加载接受无效条目 | 共用 validate；拒绝非法 matcher/角色，整份配置不部分接纳，热重载保留最近有效配置；REST 提供脱敏命名诊断 | hook registry/service、`hooks_api`、HooksEditor |
| 14 安装边界 | DSH H1/H2、Kimi Python；sync 隐式系统安装与全局 rust-src | 缺 brew 要求 bootstrap；OCR 验证 prefix 成功且绝对；私有 rust-src 固定版本/校验/完整树身份，cargo.sysrootSrc 显式设置；安装 Python 下限 3.11.4 | 本地 shell stub、私有安装测试、五类真实 LSP；不改变 Python 服务支持范围 |
| 15 前端体验 | 四报告；未知图片能力和主题/布局接线 | unknown 不显示 0/0、不删附件；可重试且阻止新增/发送未确认图片；system Monaco、OAuth 颜色、侧栏宽度、停止说明；Starting/Hook 诊断接线 | 前端单元、lint/build、真实后端桌面/移动/可访问性 E2E |
| 16 门禁台账 | 四报告；旧证据不能代表修后源码 | 本记录和机器可读门禁记录关联源码/构建身份，保留红测及失败/跳过 | Cargo、前端、Python、契约、依赖、适用原生门禁 |

## 明确保留和未采纳项

- 保留单 BLOB 附件，不引入分块表，不声称解决极端大 BLOB 的单次写锁延迟。
- 不修改 ToolRunBlock 位置 key 回退组件：当前正常 Rust 生产路径可达性未证实，且无需历史会话兼容。
- MCP 专用会话的禁止输入提示原已接线，保留而非重复开发。
- 保留 Chromium sandbox、UDS、导航限制、ordinary Session 浏览器与 REPL 跨轮状态；未自动启用 Swarm/Cron。
- 私有录制最多 10 个待 ACK 批次属于批次数限制，不是录制写盘峰值上限。正常有归属/引用批次不能按年龄强删。
- `not_created` 只在 Python 确认 context、creation、batch 全不存在时成立；超时、未知结果、网络失败不构成不存在证明。
- 取消待确认通知只表示局部停止与收尾责任，不能作为权威终态、工具结果、费用或证据。Query 503 不授权客户端重发原请求。

## 验收方法与证据限制

测试使用隔离临时目录、SQLite、随机端口和本地协议 fixture。未调用付费供应商；不以 fixture 证明真实供应商可用性。SQLite trigger 模拟保存失败，不冒称真实磁盘损坏。

完整既定门禁入口为 `scripts/parity/run-local-gates.sh`。本次逐项执行其中检查，并使用隔离候选根运行 doctor、私有 LSP manifest 运行原生专项，未在用户原目录一次性运行整段脚本。可复现命令、结果及源码/构建身份记录在同目录 [necessary-fixes-gates.json](necessary-fixes-gates.json)。历史失败保留，后续成功注明对应关系；未运行/忽略/受阻项目不计通过。

已实测的红→绿样例包括：原文件 0600→0644、普通授权后切 PLAN 仍启动、原生 ARIA 密码 getter 访问、取消保存失败时 HTTP 永久等待/SSE 无提示、MCP close 中断丢失所有权、OAuth logout 失败后重启仍认证、无限 SSE 缓冲、同 chunk 超限吞掉先行 usage、REPL 启动超时误记 userCancelled、继承历史绕过合并条数预算。各项最终状态以修后门禁记录为准。

## 历史冻结源码验收（独立审查修复前）

以下指纹及数量保留为历史执行记录，不代表 R1–R7 修后的最终状态。本轮后续修复、既有边界与新门禁见 [review-followup-2026-10-08.md](review-followup-2026-10-08.md)。

源码基于上述 Git HEAD 的未提交工作区。指纹对 tracked 与未忽略的新文件排序，排除 `docs/` 后累计 `UTF-8 相对路径 + NUL + SHA-256(文件内容)`，共 1,367 个文件。

- 构建时源码 SHA-256：`b402af935f36ba8552f8dfd8b4b92d26204abbb85aabb5cbd78b763b269d52d2`。
- 最终源码 SHA-256：`20331dc0e52035229c3df5f3b66294f0380953f7ec0f7380a7581359bd3a37a5`。
- 实际 release 后端 SHA-256：`3d72352480df1fb53e3fd5939dfb7f5e4b1638d5963dde40ab9f01e03decead6`；页面 E2E 与 doctor 均核对前后相同。

构建后仅修正两个测试文件：`frontend/e2e/production-backend.spec.ts` 的真实 Origin / 202 断言，以及 `scripts/testing/start-production-e2e-server.sh` 的隔离 UDS 地址。逐文件比较确认生产源码未变；修改后的类型检查、lint、shell 语法与 13 项 E2E 均通过。没有把二进制声称为由之后的测试修改重新构建，也不以原 HEAD 冒充已包含本轮修复的提交。

| 检查 | 此前结果 | 说明 |
|---|---:|---|
| Cargo fmt / Clippy | 通过 | Clippy workspace/all-targets/all-features，warnings 为错误 |
| Cargo workspace | 3,262 通过，9 ignored | 包含真实 Git 专项；不重复累计嵌套测试进程输出 |
| Engine no-default-features | 722 通过 | 与 workspace 部分重叠，不相加为独立用例总数 |
| 五类原生 LSP | 1 组通过 | TS/JS、Python、Rust、Go、Java 实际语义请求及进程组清理 |
| 原生浏览器 / HTTP / 临时证据 | 2 通过 | 实际 UDS、Chromium、SQLite；含 ACK 断连与重启恢复、密码 canary 全链路检查 |
| 原生 macOS Keychain | 1 通过 | 测试专用凭据创建、读取、删除；不是远程供应商 OAuth 验收 |
| React | 149 文件，1,317 项通过 | lint/build 通过；20 个主题场景、12,688 个断言；42 个 Jelly 场景 |
| Python | 362 通过 | 覆盖率 76.52%；依赖弃用警告保留 |
| 原生 Office | 41 通过 | 实际重算、结构、中文 PDF、HTML 键盘检查；记录工具、字体、浏览器身份 |
| 安装边界 | 11 通过 | Homebrew/OCR 路径 stub、私有 LSP 安装单元测试与源码身份算法；另有真实私有离线安装、probe 及五类原生验证 |
| 契约、密钥、依赖 | 通过 | 契约一致；gitleaks 无命中；cargo deny 通过；npm audit 零漏洞 |
| release 构建 | 通过 | workspace / release / locked，二进制身份见上 |
| 真实后端页面 E2E | 13 通过，0 跳过 | production 10、analysis 3；包含移动端、导航和可访问性，使用本次 release 及受管 Python UDS |
| 隔离候选 doctor | deep 37/37 通过 | 保留先前普通 doctor 的 29/29 记录，不相加为独立检查总数 |

workspace 默认忽略的 9 项中，五类 LSP、两个浏览器原生场景、Keychain 共 4 项已显式实跑通过。余下 5 项为 DashScope、订阅 DashScope、DeepSeek、Kimi 和 DashScope WebSearch 的真实外部调用，按本轮本地 fixture 验收范围未运行，不计通过。

## 失败记录与验证关系

- 合并旧集成测试原先断言失败后操作表为空。新流程在短事务先登记操作；已改为精确验证 `paused`、未封存、无目标、无附件以及来源锁释放，并保留损坏图片身份拒绝断言。数据库全套 244 项通过；包含中途导入失败后重开数据库恢复、同键长度/摘要/身份冲突不覆盖。
- 快照新增受控的 write、文件 fsync、rename、目录 fsync 故障：替换前旧字节不变，替换后同步失败明确未确认。单库合并 worker 的竞争/取消 permit 回归通过。
- 初始原生 LSP 验证暴露 Python 与 Rust 对相对路径排序不一致；统一为 POSIX UTF-8 字节排序，增加前缀目录回归，重新私有安装后五类真实服务通过。
- 原生浏览器新测试曾有 fixture 问题：非法 Run 终态、访问私有模块，以及把 evaluate 的字符串 `"0"` 当作数字。修正测试接线与精确返回类型后，两个完整场景通过；没有放宽生产密码、证据、归属或 ACK 判断。
- 首轮安装 stub 的 5 秒进程期限在并发门禁下超时，测试自身上限改为 30 秒后 11 项通过；生产安装期限未变。
- 首轮 production E2E 为 9/10：直接 HTTP 重放缺少可信 Origin，按新防护正确返回 403。测试补入实际页面 Origin 并断言既有 202 Accepted，随后 10/10 通过；未放松生产认证。初始禁用 Python 的 runner 曾探测旧 UDS 并得到 404，修为隔离不存在 socket 后重跑，未重启旧服务。
- Clippy、格式和测试 fixture 的历史失败保留在机器记录中。密钥扫描两处假阳性来自展示脱敏测试的假命令前缀，改用明确 fixture 命令后通过，没有改扫描规则或 allowlist。
- 一次全量编译在空间下降时主动中断；确认编译器退出后只清理可重建的增量缓存及重复旧测试产物，随后以 `CARGO_INCREMENTAL=0` 重新完整通过。未清理用户数据或正在运行的后端文件。

## 使用边界与运行环境

安全语义投影标注 `safe_dom_v1`，覆盖当前 document，不冒称原生 ARIA 或 frame/shadow 的完整可访问性树。录制 ACK 失败与保存失败保留收尾责任及删除门控；恢复只消费已封存批次，不重做浏览器副作用。

故障测试使用 SQLite trigger、协议断连、进程重启、受控 I/O 和期限；未进行真实断电或大型磁盘耗尽试验。1 GiB 单 BLOB 专项明确不在范围内。REPL 此次原生跨轮回归使用 Python，Node/Ruby 共用受管槽位与清理链，未另做长时间原生状态保留试验。

Office 实测使用 LibreOfficeDev 26.8.0.0.alpha0；这是记录到身份中的 alpha 工具，不是稳定版 Office 验证。前端构建仍有大 chunk、静态/动态混合导入及空 chunk 提示；Python 有依赖弃用警告，cargo deny 有重复版本提示，均不伪称零警告。真实供应商 OAuth 尚未执行；本地协议 fixture 与 macOS Keychain 原生测试已通过。

当前活跃服务未重启。原目录 doctor 的旧构建指纹和旧 LSP manifest 状态单独记录，不冒称已切换到修后环境。新候选使用隔离私有 LSP manifest，实际 `doctor --deep --json` 为 37/37；其固定 5273 健康检查会读现有前端，不作为新版本运行证明，新版本由隔离 13 项 E2E 验证。原目录 manifest / dev-state 在检查前后摘要相同。启用新版本时需要在允许停止旧服务后同步工具链、构建并启动。旧有效配置、密钥和用户文件保持原状。
