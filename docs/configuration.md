# 配置参考

zkcode 0.1.x 仅支持 macOS Apple Silicon 本地运行。`./dev` 会把仓库根目录中被忽略
提交的 `.env` 按数据解析，并强制后端绑定 `127.0.0.1`；不会用 shell 执行配置内容。
修改配置后执行 `./dev restart` 生效。

## 配置文件规则

首次执行 `./dev bootstrap --start` 会从 [`.env.example`](../.env.example)
创建 `.env`，已有文件不会被覆盖。正式语法只允许空行、`#` 注释和 `KEY=VALUE`；值可
不加引号、使用单引号或双引号。不支持 `export`、变量插值、命令替换、多行 shell、重定向
或续行。配置始终作为字符串传给子进程，不会执行。不要提交、截图或粘贴真实密钥。

## 模型与首次启动凭据

仓库包含用于发行体验的公开引导数据库。`ZK_DEV_ALLOW_DEMO_CREDENTIAL`
只接受 `0` 或 `1`，源码开发默认为 `0`：不导入公开凭据，并在重启时从运行库中
持久移除能通过来源标记或当前/历史公开 seed 指纹精确证明的旧 demo key。相同 provider 下
值不同的用户密钥会保留。只有维护者显式设置 `ZK_DEV_ALLOW_DEMO_CREDENTIAL=1`
时，源码入口才允许验证发行体验。公开凭据对所有下载者可提取且可能随时失效，
不能当作秘密或用于敏感内容。

建议启动后在 **设置 → API Keys** 替换为自己的凭据；也可以在 `.env` 配置。
以普通 DashScope 和默认模型 `qwen3.8-max-0902` 为例：

```dotenv
LLM_PROVIDER_DASHSCOPE_API_KEY=在本机填写真实密钥
LLM_PROVIDER_DASHSCOPE_MODELS=qwen3.8-max-0902,qwen3.7-plus
ZK_DEFAULT_MODEL=qwen3.8-max-0902
```

模型清单必须包含默认模型。多个 provider 可以同时配置；只有 API key 非空的
provider 会被注册。通用变量规则如下：

| 变量 | 说明 |
|---|---|
| `LLM_PROVIDER_<NAME>_API_KEY` | provider 密钥；同一 provider 多把密钥可用逗号分隔 |
| `LLM_PROVIDER_<NAME>_MODELS` | 逗号分隔模型清单 |
| `LLM_PROVIDER_<NAME>_BASE_URL` | 可选的兼容端点覆盖 |
| `LLM_PROVIDER_<NAME>_DEFAULT_MODEL` | 可选的 provider 默认模型覆盖 |
| `ZK_DEFAULT_MODEL` | 新建 Session 的默认模型 |
| `ZK_MODEL_FALLBACK_CHAIN` | 冒号分隔的模型降级链 |
| `ZK_ROOT_TASK_TOKEN_BUDGET` | 可选的根任务 token 硬上限；默认留空、不限制 |
| `ZK_ROOT_TASK_COST_BUDGET_USD` | 可选的根任务费用硬上限；默认留空、不限制；配置时精确到 nano-dollar 入账 |
| `ZK_ROOT_TASK_DEADLINE_SECONDS` | 根任务 wall-clock 硬截止；默认 1800 秒 |

默认运行不会因为 token 或费用达到固定阈值而终止任务，但仍会持久记录每次模型调用的
真实 usage 和费用。Deadline、最大轮数、取消和 usage 完整性检查继续生效。只有显式配置
上述两个可选变量或在 API 请求中传入 `maxBudgetUsd` 时，才启用相应硬上限。

`<NAME>` 支持 `DASHSCOPE`、`DASHSCOPE_TOKEN_PLAN`、`DEEPSEEK`、`MOONSHOT`、
`KIMI_CODE`、`OPENROUTER`、`ZHIPU`、`MINIMAX`、`ZENMUX`、`ANTHROPIC` 和 `OPENAI`。服务内置这些
provider 的官方端点；只有使用兼容代理或私有网关时才需要覆盖 `BASE_URL`。
`OPENAI` 默认固定使用 `https://api.openai.com/v1`，不会把 OpenAI key 发送到
DashScope；只有用户显式设置 `LLM_PROVIDER_OPENAI_BASE_URL` 才会改写该端点。

如果没有配置任何 `LLM_PROVIDER_*_API_KEY`，服务会回退到旧的单 provider
变量 `ZK_LLM_API_KEY`、`ZK_LLM_BASE_URL` 和 `ZK_DEFAULT_MODEL`。

语音输入与朗读只使用普通 DashScope 凭据（设置项 `dashscope` 或
`LLM_PROVIDER_DASHSCOPE_API_KEY`），不使用 Token Plan 或旧单 provider 凭据。
TTS 固定使用 `qwen3-tts-flash` 的 `Cherry` 音色；麦克风录音需要 HTTPS 或 localhost
安全上下文。

## 服务与工作区

| 变量 | 支持配置中的默认值 | 说明 |
|---|---|---|
| `ZK_HOST` | `127.0.0.1` | 只允许 loopback；`./dev` 启动器会强制覆盖 |
| `ZK_PORT` | `8082` | 本地后端端口 |
| `ZK_AUTH_MODE` | `localhost` | 当前唯一支持的鉴权模式；启动时强制覆盖 |
| `ZK_DB_PATH` | `.zk/data.db` | 单库 SQLite 路径，相对仓库根目录 |
| `ZK_DEMO_CREDENTIAL_DB` | `configuration/bootstrap/demo-credentials.db` | 公开、只读的首次启动种子库；不应指向用户运行库 |
| `ZK_DEV_ALLOW_DEMO_CREDENTIAL` | `0` | 源码开发公开 demo 门控；仅接受 `0/1`，修改后需重启 |
| `ZK_SNAPSHOT_DIR` | `~/.zk/snapshots` | Session 快照目录 |
| `ZK_WORKSPACE_DEFAULT_ROOT` | 当前启动目录 | 目录选择器的初始根 |
| `ZK_WORKSPACE_ALLOWED_ROOTS` | 空 | 可选的逗号分隔绝对路径白名单；空表示本机路径不设限 |
| `ZK_LOCAL_PICKER_ENABLED` | `true` | 启用 macOS 本机目录和文件选择器 |
| `ZK_STATIC_DIR` | 自动探测 | 后端静态资源目录 |
| `ZK_CORS_ALLOWED_ORIGINS` | 空 | 额外 loopback 开发源；不用于远程部署 |
| `ZK_LOG` / `RUST_LOG` | `info` | 服务日志级别 |

`ZK_WORKSPACE_ALLOWED_ROOTS` 为空时，Rust 和 Python 都不施加全局路径白名单，适合
本机单用户安装；`WORKSPACE_ROOT` 只作为相对路径的解析起点。Project 选择仍要求
本机直连和本地选择器授权，路径规范化、敏感路径检查与每次操作的 Admission 也仍会
执行。需要额外隔离时，再显式配置一个或多个允许根目录。

## Python 与浏览器

| 变量 | 默认值 | 说明 |
|---|---|---|
| `ZK_PYTHON_ENABLED` | `true` | 启动 Python sidecar 并注册动态能力 |
| `ZK_PYTHON_UDS` | `./dev` 使用 `.runtime/python.sock` | 权限为 `0600`；`.env.example` 与开发脚本显式覆盖。直接启动 Rust 且未设置该变量时使用 `~/.zkcode/python.sock` |
| `ZK_PYTHON_SERVICE_DIR` | `python-service` | sidecar 源目录；`./dev` 会设为绝对路径 |
| `ZK_PYTHON_CMD` | 自动探测 | `./dev` 会固定使用项目 `.venv` |
| `ZK_PYTHON_HEALTH_CHECK_INTERVAL_MS` | `30000` | 健康检查间隔 |
| `BROWSER_TYPE` | `chromium` | Playwright 浏览器类型 |
| `BROWSER_CHANNEL` | 空 | 空值使用锁定的 Playwright Chromium；`chrome` 使用系统 Chrome |

`./dev sync` 会执行锁定依赖安装和
`python -m playwright install --only-shell chromium`，并把 Headless Shell 与 FFmpeg
放在 `.runtime/playwright`。浏览器下载或真实启动冒烟失败都会让同步失败，不会伪装成
浏览器能力可用。

## 生产能力门

| 变量 | 默认值 | 说明 |
|---|---|---|
| `ZK_AGENT_ENABLED` | `true` | 启用 Agent 生产装配；子任务默认 30 分钟，超时收尾最多 30 秒 |
| `ZHIKUN_COORDINATOR_MODE` | `0` | 进程级顶层 Coordinator 模式；仅接受 `0` / `1`，修改后必须重启 |
| `ZK_AGENT_WRITE_ENABLED` | `true` | 允许已授权的子 Agent 写工具；共享工作区仍受独立开关与 lease 门控 |
| `ZK_SHARED_WORKSPACE_ENABLED` | `false` | sharedWorkspace 独立门禁；还必须同时启用 Agent、子 Agent 写工具并装配进程级 workspace lease |
| `ZK_AUTO_RESUME_SAFE_TASKS` | `false` | 安全自动恢复请求开关；当前没有完整父链恢复入口，开启时进程会先事实中断旧 Run，再明确启动失败 |
| `ZK_CRON_ENABLED` | `false` | 启用 SQLite 持久调度；还需统一 Agent TaskRuntime 真实装配 |
| `ZK_SWARM_ENABLED` | `false` | 显式启用进程内团队；还需 Agent、`ENABLE_AGENT_SWARMS` 功能开关及已初始化的统一 TaskRuntime |
| `ZK_WORKTREE_ENABLED` | `true` | 隔离工作树；完成后保留交付，提交、合入与移除均需显式操作 |
| `MCP_REGISTRY_PATH` | `configuration/mcp/mcp_capability_registry.json` | MCP 身份与能力授权注册表 |

关闭生产能力门会返回稳定的不可用结果，而不是装配宽松或空实现。健康接口分别
报告每项能力的 `configured` 与 `executable`；sharedWorkspace 只有四项条件全部
满足才可执行。Swarm 的 `configured` 忠实反映环境开关；同时满足 Agent、
`ENABLE_AGENT_SWARMS`、已初始化的启动 epoch 与统一 Agent runtime 后，
`executable` 才为 `true`。显式开启 Swarm 而前置条件不足时，API 返回
`FEATURE_NOT_READY`，readiness 返回 `NOT_READY`。自动恢复的 `executable` 仍为
`false`；开启自动恢复时服务会在完成 restart reconciliation 后明确启动失败，
且不会创建恢复 attempt。

团队仅支持 `IN_PROCESS` worker。创建请求未显式覆盖时，优先采用已有的
`user_config.swarm` 设置；完全未配置时 `maxWorkers=5`、`taskQueueSize=50`、
`workerIsolation=readOnly`。队列认领、输出、广播、停止与预算统一归属 TaskRuntime。
写 worker 需显式选择 `workerIsolation=worktree`，并同时通过 Agent 写工具与 Worktree
开关及每次实际工具授权；完成后保留隔离交付，Git 提交和合入仍由显式操作触发。
迁移不改变 Swarm 默认关闭状态或已有用户设置。

## 功能开关

原生开关使用 `ZK_FEATURE_<NAME>`；兼容旧配置的 `FEATURE_<NAME>` 优先级更低。
发布配置显式开启 `THINKING_MODE`、`COORDINATOR_MODE`、`WEB_BROWSER_TOOL`、
`GIT_ENHANCED_TOOL` 和 `RUNTIME_VERIFICATION`。`AGENT_TRIGGERS`、
`RESOURCE_MONITOR` 与 `SELF_CORRECTION_LOOP` 默认关闭。

`ZHIKUN_COORDINATOR_MODE` 是启动期读取并冻结的进程级开关，默认 `0`，只接受精确的
`0` 或 `1`；空串、`true`、带空格的值等均会使启动失败。有效模式还要求
`COORDINATOR_MODE` feature flag 与 `ZK_AGENT_ENABLED` 同时开启；若有效模式下关闭
Agent runtime，服务会 fail-fast。修改任一相关配置后必须重启，不会按消息关键词自动
开启，也不会因恢复旧 Session 改写进程模式。它不能绕过显式 Swarm API 的硬性门禁。
除非正在开发相应功能，不建议修改未列在 [`.env.example`](../.env.example) 中的内部开关。

## 管理员端点

`ZK_ADMIN_PASSWORD` 为空时管理员端点关闭。若本地调试需要开启，应只在 `.env`
中设置强密码，且仍不得把服务暴露到 loopback 之外。

安全边界与数据去向分别见 [安全策略](../SECURITY.md) 和
[数据与隐私](data-and-privacy.md)。


## 2026 年 10 月新增配置

新配置仅在未设置时采用默认值，已有有效模型、辅助路由与界面偏好保留。
前端的“当前会话模型”只更改当前会话，“新会话默认模型”只影响之后创建的会话。

| 配置 | 默认值与用途 |
|---|---|
| `LLM_PROVIDER_KIMI_CODE_API_KEY` | 空，不注册 Kimi Code；模型 `k3`、`kimi-for-coding` |
| `LLM_PROVIDER_OPENROUTER_API_KEY` | 空，不注册 OpenRouter；同名跨渠道型号用 `openrouter/` 前缀隔离 |
| `LLM_PROVIDER_<NAME>_KEY_SELECTION_STRATEGY` | ZenMux 为 `PRIORITY_FAILOVER`，其余为 `ROUND_ROBIN` |
| `LLM_PROVIDER_ZENMUX_ALLOW_PAID_FAILOVER` | `false`；同时配置订阅与按量 Key 时，只有显式 true 才允许切换到按量 Key |
| `LLM_COMPACT_PROVIDER` / `LLM_COMPACT_MODEL` | `deepseek` / `deepseek-flash`，独立压缩摘要通道 |
| `LLM_COMPACT_THINKING_MODE` | `max` |
| `LLM_COMPACT_MAX_COMPLETION_TOKENS` / `LLM_COMPACT_MAX_SUMMARY_TOKENS` | `8192` / `4096` |
| `LLM_COMPACT_TIMEOUT_MS` | `90000` |
| `ZK_VISION_FALLBACK_MODEL` | 未配置时沿用视觉路由选择；显式目标必须具备图片能力 |
| `ZK_FAST_MODEL` / `LLM_FAST_MODEL` | 可选快模型路由，前者优先；显式 `ZK_LIGHTWEIGHT_MODEL` 继续优先于快模型。快模型缺省或无效时保留已有轻量/默认路由 |
| `ZK_INTERACTION_FIRST_CLIENT_WAIT_SECONDS` | `600`，仍受所属任务总期限约束 |
| `ASR_CORRECTIONS` | `规范词:变体1,变体2;另一规范词:变体`；未设置时包含产品名热词 |

摘要通道不可用时使用本地压缩回退。摘要、重试、降级和会话合并的物理调用均记入费用账本，
受真实金额及 token 预算约束；未知费用不会显示为已确认的零费用。

Skill 内容按当前会话保存的工作区隔离；未创建会话时，管理页面可选择已保存项目。
Managed、User、Bundled、MCP 来源全局共享，Project 与 Plugin 只对所属工作区可见。
无会话、无项目上下文的目录只展示全局来源，不使用服务进程启动目录的项目技能。
REST 在所有 Skill 目录、详情和开关请求中使用 `X-Session-Id` 或 `projectId` 查询参数，
二者不能同时提交；客户端不能以任意路径指定技能根目录。

项目与用户 Skill 同时读取以下兼容路径；不移动、删除或自动启用已有文件。

| 来源 | 同来源目录优先级（从低到高） |
|---|---|
| 项目 | `<workspace>/.zhikun/skills` → `<workspace>/.zk/skills` → `<workspace>/.zkcode/skills` |
| 用户 | `~/.zhikun/skills` → `~/.zk/skills` → `~/.zkcode/skills` |

同名技能仍按 managed > user > project > plugin > bundled > MCP 选择来源；例如用户旧目录
中的技能也优先于项目当前目录的同名技能。热重载保持确定顺序，删除胜者后回退到仍存在的
下一候选；读取失败时保留最近有效内容并重试。管理页面开关是全局开关，按技能规范名称
事务存储，同名覆盖、删除回退和热重载都不会绕过禁用状态。
记忆文档整体保存携带 scope revision，冲突返回 409；Markdown 只是数据库条目的可逆视图。


### 可选可视化意图建议

`ZK_VISUALIZATION_AUTO_ROUTING_ENABLED` 默认关闭，仅 `true` 或 `1` 开启。开启时还需显式配置 `ZK_VISUALIZATION_MODEL`；未单独指定时只使用已有 `ZK_FAST_MODEL` / `LLM_FAST_MODEL`，不自动选择主模型或新按量 Key。分类请求进入当前 Task 的真实费用、token 和期限账本，最多 256 输出 token、15 秒。无允许的原生 Visualization 工具时不发分类请求。Run 内上下文去重有界为 32 条，正向建议只生成一次；用户的显式 Visualization 调用不受此自动建议次数限制。

自动结果是标为“尚未执行分析”的建议卡片。点击分析入口后仍使用原项目/会话授权，不把意图分类当作已查询数据或新的工具许可。既有免费 `/visualize` 命令和图表参数保持兼容。
