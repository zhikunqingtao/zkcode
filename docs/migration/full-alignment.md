# 固定目标全量能力对齐

> **后续范围变更（2026-10-07）：** 用户明确删除简洁工作台，仅保留开发工作台，不处理简洁模式历史数据。最新前端验证为 145 文件／1289 项及 9 项真实后端 E2E；见 [单一工作台记录](development-workbench-only.md)。本页此前 F1–F5 全量计数与源码身份属于该次冻结，不能代替后续前端验证。


> **2026-10-07 修复闭环：** F1–F5 及摘要账本验收缺口已关闭，修后冻结源码上的 37 项必需本地门禁通过；可选付费验证未运行。以 [修复报告](f1-f5-fixes.md)、[回归定位](f1-f5-regression-map.md) 和 [当前门禁](full-alignment-gates.json) 为准，历史门禁保留在独立快照。

源目标：`zhikuncode 053adf90`；目标基线：`zkcode 08cbdc45`。本轮在已有本地改动上增量实施，不切分支，不自动提交或推送。

## 范围与证据规则

覆盖 114 次增量提交、适用旧差距及已批准的适用原型补全。每项必须记录生产入口、验证结果和边界。类名、文件覆盖数和此前通过数量不构成完成证据。状态只使用 `pending`、`in_progress`、`verified`、`excluded`；未运行、失败或受阻不得标记 verified。

明确排除 Docker/Linux/Windows 部署、Meoo、OSS 发布、flyai、Java 插件系统、历史评测资料、装裱和仪式动画、固定删除验证码、三个 TaskCreate 兼容别名、新远程 Bridge/设备配对/云账号体系、外部进程 Agent worker。保留本地访问控制、已有配置与正常功能；历史会话数据无需升级兼容。

## 实施台账

| 能力组 | 状态 | 记录 |
|---|---|---|
| React/Rust/Python 代码分析及 OpenAPI | verified | [前端记录](full-alignment-frontend.md)；包含真实复杂度、Git、调用路径与聚合规范，最终 release 页面 E2E 另列 |
| Query/CLI 实时 SSE、参数、默认及取消 | verified | [Query 记录](full-alignment-query.md)；实际流式、恢复、取消和权限专项纳入最终 Rust/Python 回归 |
| 临时 ContentStore 与无正文审计 | verified | [Query 记录](full-alignment-query.md)、[工具记录](full-alignment-tools.md)；覆盖实际 DB/WAL、日志、快照及工具证据操作；适用限制见下文 |
| 模型能力/冷却/辅助查询/真实费用 | verified | [运行时记录](full-alignment-runtime.md)；实际物理请求账本、未知费用和可选分类入口已验证；远端供应商范围独立记录 |
| 压缩质量/指标/授权文件重载/图片引用 | verified | [运行时记录](full-alignment-runtime.md) |
| 取消保存失败、本地停止、清理对账 | verified | [运行时记录](full-alignment-runtime.md)；真实超时 partial、落库失败、清理不明及终态竞态回归 |
| MCP 服务/OAuth/STDIO 与 LSP | verified | [工具记录](full-alignment-tools.md)；本机 OAuth fixture、真实 Keychain、五类真实 LSP；对外目录边界见下文 |
| Hooks、通用工具、编码及恢复建议 | verified | [工具记录](full-alignment-tools.md)；原始结果、错误、授权和证据不可被 Hook 投影改写 |
| 团队队列/认领/广播及 Worktree | verified | [运行时记录](full-alignment-runtime.md)；真实受管 worker、Git 冲突、取消和广播，保留功能开关，不自动提交或合入 |
| Tailwind 4/交互/命令/快捷键/Vim | verified | [前端记录](full-alignment-frontend.md)；157 文件、1321 项，20 个主题场景及 Jelly 回归 |
| 会话/合并/Skill/SQLite 记忆回归 | verified | [运行时记录](full-alignment-runtime.md)；最终 DB 232 项及真实服务端回归，项目 Skill 隔离与全局禁用同源 |
| 浏览器/CRA/产物验证/diff 行号 | verified | [证据记录](full-alignment-evidence.md)；普通及临时真实浏览器/HTTP 验证另行显式通过 |
| 安装/Office/路径/维护及排除残留 | verified | Office 41 项、安装脚本 87 项及维护回归已通过；本轮官方 Rust/build repair 与 deep doctor 40 项通过；未执行全量 sync 或全新安装 |
| 完整发布门禁 | verified | [机器可读门禁](full-alignment-gates.json)；37 项必需本地门禁通过，修后 release、9 项真实页面 E2E 和 doctor 40 项通过；可选付费探针未运行，源码冻结身份一致 |

**本轮新增修复**：宿主 Hook 精确授权、Skill 实际来源绑定、明确新建草稿迁移、复杂度自身内容指纹、期限首因与权威终态，以及普通／摘要请求真实 SQLite 账本回归。保留此前思考参数修复。完整生产入口、反例、修正及边界见 [F1–F5 修复记录](f1-f5-fixes.md)。

## 锁定默认

- CLI：新建 DONT_ASK；续接继承权限；99 轮、300 秒。
- Query API：缺省使用引擎 1024 轮和根任务期限（当前 1800 秒）；有效请求预算与系统预算取更严格者。
- 独立摘要：deepseek/deepseek-flash，thinking=max，8192 输出、4096 摘要、90 秒；失败本地回退；物理请求逐次入账。
- 按量 Key 必须显式许可；现有辅助路由及 Swarm/Cron/递归默认不变。
- balanced 轮次、强动效关闭、ASR 聊天上下文关闭；有效已保存偏好优先。
- 客户端首次交互等待 600 秒，受任务剩余期限约束；按钮停止确认、Ctrl+C 立即停止。
- Task 默认 attached；Git 提交/合入必须显式触发；Grep 保留非敏感范围。
- 五类 LSP（TS/JS、Python、Rust、Go、Java）全部默认安装私有固定工具链。
- `--no-session` 真正不保留对话，保留无正文费用及必要运行审计；不允许自动落盘回退。

## 验证记录

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

早期结果仅见 [修复前门禁快照](full-alignment-gates-before-f1-f5-20261007.json) 和原始审查报告；它们不能替代本轮运行。完整失败保留和未覆盖说明见 [修复报告](f1-f5-fixes.md)。

## 适用边界

- 临时 Java LSP 因 JDT 日志可能保存正文而执行前拒绝；普通会话五类 LSP 均已真实启动验证。临时浏览器不允许 trace/video/HAR 录制。
- 外部 MCP 仅发布其已实现且经过本机授权的能力；REPL/WebBrowser 不向外部连接发布。普通聊天/Query 的这两项能力保持可用。不能把外部 MCP 上限当作普通聊天权限，也不能通过同名远端工具获得原生信任。
- OAuth 以本机真实协议 fixture 和 Keychain 验证，不宣称每个远端供应商都完成互操作。本轮没有付费供应商调用；[此前 DeepSeek 探针](deepseek-flash-release-probe.json) 仅保留为旧源码和旧构建的历史证据。费用按注册价格与实际 usage 计算，不冒称供应商账单核对。
- Office 如实使用本机 LibreOfficeDev 26.8 alpha，并记录全部工具、字体和浏览器身份；不冒称已验证稳定版 LibreOffice。
- `upstream-path-coverage.json` 的 1203 个路径（880 项能力映射、323 项排除）及 114 次提交已逐项归档；路径覆盖和源码关联本身不等于真实供应商或任意用户项目验证。


## 普通 REPL 生命周期决定

2026-10-07 用户明确选择保留普通会话的跨轮次解释器变量/状态。使用会话所属的受管服务，提供真实状态和确认停止入口；删除/合并继续检查活跃服务与未确认清理。临时解释器仅属于当前任务及 attached 子树，任务结束清理。普通服务内部 transcript 不进入用户聊天历史，也不占用普通模型 worker 的并发额度（独立有界准入仍由 TaskRuntime 管理）。真实两次 Query 保留状态、三种解释器、未授权不启动、停止及删除/合并门控已通过最终工作区回归。
