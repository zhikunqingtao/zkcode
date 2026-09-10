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
`ZHIPU`、`MINIMAX`、`ZENMUX`、`ANTHROPIC` 和 `OPENAI`。服务内置这些
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
| `ZK_PYTHON_UDS` | `.runtime/python.sock` | 权限为 `0600` 的仓库本地 Unix socket |
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
| `ZK_AGENT_WRITE_ENABLED` | `false` | 子 Agent 默认只读；共享写与写工具在独立门禁通过后才可显式开启 |
| `ZK_SHARED_WORKSPACE_ENABLED` | `false` | sharedWorkspace 独立门禁；还必须同时启用 Agent、子 Agent 写工具并装配进程级 workspace lease |
| `ZK_AUTO_RESUME_SAFE_TASKS` | `false` | 安全自动恢复请求开关；当前没有完整父链恢复入口，开启时进程会先事实中断旧 Run，再明确启动失败 |
| `ZK_CRON_ENABLED` | `false` | 启用 SQLite 持久调度；还需统一 Agent TaskRuntime 真实装配 |
| `ZK_SWARM_ENABLED` | `false` | 仅表达 Swarm 部署意图；当前执行适配器未认证，设为 `true` 会使 API 返回 `FEATURE_NOT_READY`、readiness 返回 `NOT_READY` |
| `ZK_WORKTREE_ENABLED` | `false` | Worktree 尚未完成真实 Git E2E，必须保持关闭 |
| `MCP_REGISTRY_PATH` | `configuration/mcp/mcp_capability_registry.json` | MCP 身份与能力授权注册表 |

关闭生产能力门会返回稳定的不可用结果，而不是装配宽松或空实现。健康接口分别
报告每项能力的 `configured` 与 `executable`；sharedWorkspace 只有四项条件全部
满足才可执行。Swarm 与自动恢复的 `configured` 忠实反映环境开关，当前两者的
`executable` 始终为 `false`；误设 Swarm 时 readiness 会 fail-closed，误设自动恢复
时服务会在完成 restart reconciliation 后明确启动失败，且不会创建恢复 attempt。

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
