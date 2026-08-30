# zkcode

面向 macOS 本地开发的 Rust-native AI 编码助手，从 [ZhikunCode](https://github.com/zhikunqingtao/zhikuncode)（Java 后端）演进而来。

当前版本 **0.1.0 Beta**，仅验收于 Apple Silicon macOS 环境。

[快速开始](#快速开始源码开发-beta) · [配置说明](#配置说明) · [开发者指南](#开发者指南) · [故障排查](docs/troubleshooting.md) · [安全说明](#安全说明) · [更新日志](CHANGELOG.md)

## 核心能力

- REST、原生 WebSocket、SSE 和 CLI 连续对话
- 内置工具：Read、Edit、Write、Bash、Notebook、搜索、可视化、验证
- 统一授权准入：路径、命令、敏感数据、PRE Hook
- 持久化：Session、Run、Task、Snapshot、Evidence、Artifact、Workbench（单库 SQLite）
- 多 Agent 协作：子 Agent、Team、只读 Swarm
- MCP Client/Server、Python UDS sidecar、Playwright 浏览器回放
- 多 LLM Provider：DashScope、DeepSeek、Moonshot、Zhipu、MiniMax、Anthropic、OpenAI 等
- 默认模型：`qwen3.8-max`（百炼订阅）

## 技术栈

| 层 | 技术 | 版本 |
|---|---|---|
| 后端 | Rust（Axum + Tokio + rusqlite） | Rust 1.97.1，Edition 2024 |
| 前端 | React + TypeScript + Vite | Node 22、npm 10、React 18 |
| Python 服务 | FastAPI + tree-sitter + Playwright | Python 3.11 |

Rust workspace 成员：`zk-core`、`zk-db`、`zk-llm`、`zk-protocol`、`zk-engine`、`zk-tools`、`zk-mcp`、`zk-authz`、`zk-server`。

## 快速开始（源码开发 Beta）

> 这是面向 Apple Silicon macOS 15+ 的源码开发 Beta 入口。它会在 CI 中走同一套依赖同步、
> 构建和 Headless Browser 诊断；“完全没有 CLT/Homebrew/语言工具链”的干净机器安装仍属于
> 正式公开保证前的独立验收矩阵。

**系统要求**：Apple Silicon Mac、macOS 15+、可访问 Homebrew/npm/PyPI/Rust/Playwright
官方源或受信代理的网络。若未安装 Homebrew，当前 macOS 账户还必须具有管理员权限。
具体工具版本以 [`configuration/dev-toolchain.toml`](configuration/dev-toolchain.toml) 为准。

### 1. 获取代码

已有 Git 时推荐克隆，这也是后续提交代码必需的方式：

```bash
git clone https://github.com/zhikunqingtao/zkcode.git
cd zkcode
```

完全干净的 Mac 若尚无 Git/Command Line Tools，可先在 GitHub 选择 **Code → Download ZIP**，
解压并进入 `zkcode-main` 目录。如果解压工具丢失了可执行位，执行：

```bash
chmod +x dev install-zkcode.command start.sh stop.sh scripts/*.sh scripts/dev/*.sh scripts/parity/*.sh
```

### 2. 首次构建并启动

```bash
./dev bootstrap --start
```

`./dev` 会复用兼容工具链，以 side-by-side 方式补齐缺失版本，严格消费 lock，安装项目本地
Headless Shell，构建当前源码并验证 Rust Core、Python UDS sidecar 与 Vite。它不会 pull、
reset、checkout、修改 shell rc 或覆盖已有 `.env`。

缺少 Command Line Tools 时，macOS 会弹出 Apple 安装界面。缺少 Homebrew 时，普通
`bootstrap` 会在当前 Terminal 请求一次管理员授权；密码不回显是 `sudo` 的正常行为，
zkcode 不会读取或保存密码。**不要执行 `sudo ./dev ...`。**

`--yes` 只跳过项目的依赖安装确认，不绕过 Apple 安装界面或管理员权限，也不会在
终端询问 sudo 密码。无人值守环境只能在 Homebrew 已存在，或管理员已安全配置
sudo 缓存、免密授权或 `SUDO_ASKPASS` 时使用。

### 3. 验证首次启动

```bash
./dev status
./dev doctor --deep
```

默认配置下，`status` 应显示 backend、frontend 和 python 均为 `healthy`，然后打开
<http://127.0.0.1:5273/>。如果失败，不要删除 lock 或 PID 文件；按
[故障排查](docs/troubleshooting.md) 检查首个错误和 `.runtime/backend.log`、
`.runtime/frontend.log`。

没有配置 API Key 时服务和页面仍会启动，但模型对话不可用。启动后请在
**设置 → API Keys** 保存自己的 provider 密钥，或按 [配置参考](docs/configuration.md) 编辑 `.env`。
完全没有 Command Line Tools、Homebrew 或语言运行时的干净机器仍属于独立的发行验收场景。

### ⚠️ 关于演示凭据的重要说明

仓库中的演示凭据是供发行体验使用的公开测试凭据，不应被视为秘密，也不适合处理敏感
内容。源码开发入口默认不导入该凭据；对升级用户，下次重启还会从运行库中移除能与
公开 seed 来源或指纹精确匹配的旧演示密钥，不会按 provider 名删除用户自己的密钥。
请通过 **设置 → API Keys** 配置自己的密钥，保存后立即生效。也可以编辑 `.env`，
然后执行 `./dev restart`。若维护者需要验证发行体验，可在本机 `.env` 显式设置
`ZK_DEV_ALLOW_DEMO_CREDENTIAL=1`。

公开演示凭据存在以下限制：

- 每日有模型调用次数限额
- 密钥可能随时被平台封禁/吊销

申请并配置独立密钥：

1. 前往 [阿里云百炼平台](https://bailian.console.aliyun.com/) 申请百炼订阅密钥；
2. 在浏览器中打开 **设置 → API Keys**，在对应 Provider 输入自己的密钥并保存；保存后立即生效，无需重启服务。

若维护者显式启用了演示凭据，调用时出现 401/403 通常表示共享额度已用完或凭据已停用，
请按上述步骤改用自己的密钥。

## 安装完成后的日常使用

| 命令 | 用途 |
|---|---|
| `./dev up` | 同步变更、增量构建，并恢复所有已启用服务 |
| `./dev restart` | 构建成功后安全重启全部服务 |
| `./dev restart backend` | 只重启 Rust 后端和 Python sidecar |
| `./dev restart frontend` | 只重启 Vite 前端 |
| `./dev stop` | 精确停止本仓库进程 |
| `./dev status --json` | 输出 PID、URL、健康状态和依赖状态 |
| `./dev doctor --deep --json` | 执行完整环境与能力诊断 |
| `./dev repair browser` | 重新安装并验证项目本地浏览器 |
| `./dev logs backend` | 跟踪后端日志；也支持 `frontend` 和 `python` |

启动后访问：<http://127.0.0.1:5273/>

## 端口分配

| 服务 | 地址 |
|---|---|
| 后端（zk-server） | 127.0.0.1:8082 |
| 前端（Vite） | 127.0.0.1:5273 |
| Python sidecar | Unix Domain Socket |

## 配置说明

所有配置通过根目录 `.env` 文件管理。关键配置分类：

`.env` 只支持空行、`#` 注释和 `KEY=VALUE`；值可以不加引号、使用单引号或双引号。
不支持 `export`、变量插值、命令替换、多行 shell、重定向或续行，配置内容不会由 shell 执行。

| 分类 | 关键变量 |
|---|---|
| 服务器 | `ZK_HOST`、`ZK_PORT`、`ZK_LOG` |
| LLM Provider | `LLM_PROVIDER_{NAME}_API_KEY`（DashScope / DeepSeek / Moonshot / Zhipu / MiniMax / Anthropic / OpenAI） |
| 默认模型 | `ZK_DEFAULT_MODEL`（默认 `qwen3.8-max`） |
| Python 侧车 | `ZK_PYTHON_ENABLED`、`BROWSER_TYPE` |
| 功能门控 | `ZK_AGENT_ENABLED`、`ZK_SWARM_ENABLED` |
| Feature Flags | `ZK_FEATURE_THINKING_MODE`、`ZK_FEATURE_COORDINATOR_MODE` 等 |

完整变量与注释见 [`.env.example`](.env.example)，详细说明见 [配置参考](docs/configuration.md)。

> 不要把 `.env`、真实密钥或访问令牌提交到版本库。

通过 **设置 → API Keys** 保存的 LLM 密钥会热替换并立即生效。手动修改 `.env` 后则需要重启服务加载新配置：

```bash
./dev restart
```

## 开发者指南

### 测试

```bash
# 日常回归：诊断 + 前端 + Python + zk-server 单元测试
./dev test quick

# 完整本地门禁（需要 cargo-deny 和 gitleaks）
./dev test full

# 真实 Playwright 浏览器测试
./dev test browser
```

需要定向调试时，可分别执行 `cargo test --workspace --locked`、
`cd frontend && npm run lint && npm run test:run && npm run build`，或
`cd python-service && .venv/bin/python -m pytest --cov=src --cov-fail-under=70`。

### 代码质量

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

更多本地门禁与开发流程见 [本地开发](docs/dev-run.md) 和 [贡献指南](CONTRIBUTING.md)。

## 项目结构

| 目录 | 说明 |
|---|---|
| `crates/` | Rust workspace（9 个 crate） |
| `frontend/` | React + Vite 浏览器界面 |
| `python-service/` | FastAPI 能力服务 + CLI |
| `scripts/` | 安装、诊断、契约检查脚本 |
| `docs/` | 架构、安全、配置等文档 |
| `configuration/bootstrap/` | 公开、只读的首次启动演示凭据库（不是用户库） |
| `configuration/mcp/` | MCP 能力注册表 |
| `.env` | 环境配置（本地，不提交到版本库） |
| `dev` | 源码开发统一安装、诊断与生命周期入口 |
| `install-zkcode.command`、`start.sh` / `stop.sh` | 转发到 `./dev` 的兼容入口 |

## 安全说明

- 仅监听 `127.0.0.1`，不支持远程/LAN 访问
- 无容器/VM 沙箱，工具以启动用户权限运行
- 不支持多用户/Docker 部署
- 新会话默认使用 `AUTO_APPROVE`（界面显示为“完全访问权限”）：文件写入、Shell、网络访问及其他工具请求不会逐次弹窗确认，但仍受路径、命令、敏感数据、Hook 和部署边界等系统安全检查约束
- 如需逐项确认，可先创建或选择会话，再打开 **设置 → 常规 → 权限模式**，切换为“默认模式”；只希望先规划、暂不修改文件时可切换为“计划模式”

完整说明见 [安全策略](SECURITY.md)、[安全模型](docs/security-model.md) 和 [数据与隐私](docs/data-and-privacy.md)。

## 已知限制

- 仅支持 Apple Silicon Mac
- 完全干净机器的工具链安装仍需独立发行验收
- Beta 阶段，API 可能变化
- Worktree 功能默认关闭

## 许可证

Apache License 2.0 — 详见 [LICENSE](LICENSE)。

第三方声明见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
