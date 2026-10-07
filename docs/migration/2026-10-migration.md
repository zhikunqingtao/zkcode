# zhikuncode → zkcode 迁移记录

固定源版本 `053adf90`，源基线 `3e536438`，目标基线 `08cbdc45`。
逐提交清单位于 `2026-10-upstream.json`，共 114 次提交。`pending` 不表示完成。

## 约束

增量迁移能力，保留 Rust 架构、正常工作的产品能力和有效用户配置。
排除 OSS、Meoo、flyai、主题装裱和仪式、固定删除验证码、TaskCreate 三个兼容别名、历史评测资料、Linux/Docker 与 Windows PowerShell 部署。
继续支持 Apple Silicon macOS；不升级旧库、不导入 Java 数据。用户进一步确认没有用户、无需考虑任何 zkcode 历史会话数据，按全新数据库设计，不做历史备份或兼容流程。新版本产生的数据仍须正确持久化、重启恢复和隔离。
不自动提交、推送、创建 PR。

已确认的默认：CLI 新建 DONT_ASK、续接沿用权限；合并继承主会话权限；按量密钥回退显式允许且受金额预算约束；保留现有辅助路由；保留旧型号兼容；ASR 上下文默认关闭；轮次 balanced；停止按钮确认、Ctrl+C 立即停止；会话模型和新会话默认分离；强动效默认关闭；交互首次客户端等待 600 秒且受总期限约束；Skill 全局事务开关；Grep 保护敏感目录但保留其他搜索范围。

## 验证状态

**以下门禁为扩展全量方案实施前的历史快照，不代表当前工作树通过。** 当前方案已包含 Tailwind 4、取消失败立即本地停止、临时 ContentStore、LSP/MCP/团队等新增工作；不再存在下文历史记录中的待用户决策。当前状态以 [全量对齐台账](full-alignment.md) 及其分组记录为准，完成后重新生成发布结果。

- 迁移前契约检查：通过。
- 迁移前前端：48 个测试文件，268 项通过，16 项跳过。
- 迁移后：已完成范围内主体实现及下列本机回归；发布门禁尚未全部通过，不能标记迁移完成。任何跳过、未运行或阻塞项均不得计为完成。

固定范围的 114 次提交共触及 2,367 个路径记录、1,203 个唯一源路径；其中 880 项已有逐路径能力或测试断言映射，323 项按约定排除。净改动路径为 1,185 个，其余中间增删路径仍保留在提交台账。`upstream-path-coverage.json` 的 `documented_coverage` 表示已核对并记录处理方式，不代表执行测试通过。187 个后端测试源路径的原断言、Rust 适配和实际回归位置分别记录在 `*-backend-test-audit.json`。

完整发布状态以 [release-gates.json](release-gates.json) 和各能力台账的最新验证记录为准。2026-10-07 已修复完整测试发现的问题，并重新执行默认及全部 feature 的 workspace 测试，均通过。两次都使用 `ZK_RUN_GIT_TESTS=true`，确保 Git 环境开关内的测试也真实执行。本机 HTTP 协议回归采用无代理客户端连接临时 loopback 服务，避免环境代理预连接干扰，生产客户端仍沿用用户代理配置。这些本机协议测试不构成远端付费服务可用性的证明。

| 检查 | 最新结果 | 范围与限制 |
| --- | --- | --- |
| Cargo 格式、严格 Clippy | 通过 | Clippy 包含 workspace、所有 target、全部 feature，警告视为错误 |
| Rust 全 workspace 默认配置 | 2,852 通过、0 失败、8 忽略 | 106 个顶层测试 harness；不重复计入子进程自执行输出 |
| Rust 全 workspace 全部 feature | 2,852 通过、0 失败、8 忽略 | 与默认配置分别执行，不将两次相加作为唯一用例数 |
| Engine 无可选 feature | 616 通过、0 失败 | 单独验证未启用图片缩略功能的分支；workspace 会由 server 启用该 feature |
| release 构建与版本启动 | 通过 | workspace 锁定依赖、优化构建；版本为 0.1.0，schema 2、WebSocket 4 |
| 原生 VerifyJourney | 1 通过 | 显式运行默认忽略项；实际 Rust→UDS→Python→Chromium、HTTP 预览、证据及资源释放 |
| 前端 lint、构建 | 通过 | 当前锁定依赖与 TypeScript 编译 |
| 前端单元测试 | 135 个文件，1,190 通过、16 跳过 | 已补充停止确认、Ctrl+C、复制、输入法及自然结束回归；跳过项未计入通过 |
| 真实 Rust 后端浏览器 E2E | release 产物 4 通过（19.3 秒） | 独立 SQLite、真实 Chrome、本机脚本模型；执行前后校验同一优化二进制身份 |
| 主题浏览器专项 | 主题 20 场景 / 12,508 项断言；Jelly 42 场景通过 | Chrome 154.0.8037.98，桌面及移动宽度；真实组件、CSS、图片与复制交互，业务请求和剪贴板隔离 |
| Python | 279 通过，覆盖率 76.49% | 正式同步后重建的 Python 3.11 环境；pip check 通过，9 条警告保留 |
| 原生 Office 回归 | 41 通过 | 工具、字体、浏览器身份均记录；当前 LibreOfficeDev 身份不冒充稳定版 |
| dev 脚本、正式同步、深度 doctor | 84 项通过；同步及 doctor 通过 | 支持的 Python 3.11；离线缓存缺失的早期失败保留在历史记录 |
| 契约 | 通过 | 60 种下行事件、56 张表、34 个默认工具 |
| Cargo 依赖及发布秘密扫描 | 通过 | 当前结果与日志摘要见门禁台账 |
| npm 安全审计 | **失败：6 high** | Tailwind 3 的 braces 依赖链；待用户选择主版本升级 |

上述 Rust 默认忽略项中的原生 VerifyJourney 已显式通过；剩余 4 个真实付费 LLM、1 个真实搜索服务和 2 个保留的旧 Swarm 测试未执行。前端 16 个跳过项、源 Java 测试及源浏览器参考未被冒充为执行通过；原断言与 Rust/前端等价测试的对应关系保留在台账中。

[本地产品验收矩阵](product-acceptance-matrix.json) 按新旧工作台、主题、模型、停止、草稿、语音、图片/附件、归属等能力列出已执行断言及实际覆盖边界。16 个前端跳过项已与目标基线核对，均为已有的空测试占位；其中相关行为的其他有效测试和仍未单独验证的旧组件布局细节分别列出，不能把空占位视为回归证据。

为避免生成的调试缓存耗尽磁盘，本机 Cargo 验证使用 `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`；断言、feature 和测试逻辑保持启用。运行环境为 Apple Silicon、macOS 26.5.2、Rust/Cargo 1.97.1、Python 3.11.15。直接执行前端命令的 shell 使用 Node 22.14.0 / npm 10.9.2；正式 dev 入口解析到 Node 22.23.2 / npm 10.9.8，已通过深度 doctor，二者均在项目要求的 Node 22 范围内。

## 历史取舍（已由全量方案明确批准）

- Tailwind 3 的开发依赖链仍有未修补的 braces 高危通告。升级 Tailwind 4 需要主版本样式迁移及视觉回归；当前保留 Tailwind 3，依赖安全门禁如实记为失败，不隐去通告。
- 显式停止时若数据库无法保存取消意图：是否仍立即停止本地归属进程并报告持久化失败。当前保留 Rust 原有“先持久化取消意图，再发送停止信号”行为，尚未将不同语义视为已获批准。

以上是旧阶段的待确认记录；全量方案已批准 Tailwind 4 和取消保存失败时立即停止本地归属执行，不再等待重复确认。

## 已采用的 Rust 适配补充

- 多服务商注册同一模型时保留原有先注册者优先的契约；同一服务商内部模型别名和重复 Key 去重，共享凭证冷却状态。
- Shell/Git 在归属预约和 PID 绑定后才释放内部启动门控；绑定失败或在绑定期间取消不得执行用户命令。
- Git 机器协议保持每流 1 MiB 采集上限；面向用户的 diff 和显式 commit 报告支持每流 16 MiB。超过上限、读取失败或清理未确认均不可当作完整结果，显式 Git 操作不会自动重试。
- 消息的 text/tool/text 原始块顺序进入持久化投影；一次补答前先保存真实原文，落库失败不发送下一次请求。

## 实现与验证入口

| 方案批次 | 已实现的主要内容 | 逐能力记录 |
| --- | --- | --- |
| 契约与兼容基础 | TaskRuntime V4、消息及系统事件、事件归属/恢复、前端和 Python 增量合并 | [root](root-capabilities.json)、[task-display](task-display-capabilities.json) |
| 模型、上下文与执行可靠性 | 新服务商和路由、Key 优先级、完整工具批次、上下文压缩、独立摘要、一次恢复、严格落库及真实费用 | [runtime](runtime-capabilities.json) |
| 会话、交互与协作 | 会话查询/权限、多选、合并事务和封存快照、HandoffRead、attached/detached、进程归属、Worktree 显式交付 | [merge](merge-capabilities.json)、[git](git-capabilities.json)、[runtime](runtime-capabilities.json) |
| 页面、Skill、记忆与通用工具 | 轮次和主题、模型操作分离、草稿/语音、全局 Skill 事务、SQLite 记忆及 CAS、工具和附件身份 | [frontend](frontend-capabilities.json)、[root](root-capabilities.json)、[general-tools](general-tools-capabilities.json) |
| 浏览器、证据与本机环境 | 浏览器生命周期、原生 VerifyJourney、Blob/图片完整性、授权预览、macOS 文档工具链 | [python-evidence](python-evidence-capabilities.json)、[native-adaptations](native-adaptations-capabilities.json) |

[逐提交台账](2026-10-upstream.json)、[逐路径覆盖](upstream-path-coverage.json) 和 `*-backend-test-audit.json` 用于追溯源改动。每次执行的命令、输出日志及 SHA-256 见发布门禁台账；`previousValidation` / `priorAttempts` 中的失败是保留的历史证据，不覆盖最新结果。日志位于本机临时目录，仓库内摘要不保证临时日志永久存在。

完整发布入口为 `scripts/parity/run-local-gates.sh`，其中真实后端 E2E 显式使用本次 release 构建产物，避免误选旧 debug 二进制。CI 的 production-runtime 作业也显式构建并验证 release 程序；主题及 Jelly 的 Chrome 回归已加入本地和 CI 门禁。原生 Office 和 VerifyJourney 另有专项门禁；本次逐项执行结果如上。当前 npm 审计失败，因此不能将整个发布入口描述为全绿。

## 实施分工

- 前端：三方合并、轮次、主题、会话、Skill、记忆和交互。
- 模型与引擎：服务商、密钥、摘要、压缩、回放、执行可靠性与费用。
- Python 与本机环境：浏览器、VerifyJourney、证据、附件、CLI 和 macOS 工具链。
- Rust 集成：协议、数据库、会话合并、权限、Skill、记忆、交互、任务和 Worktree。
