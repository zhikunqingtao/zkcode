# zkcode

面向 macOS 本地开发的 Rust-native AI 编码助手，从 [ZhikunCode](https://github.com/zhikunqingtao/zhikuncode)（Java 后端）演进而来。

当前版本 **0.1.0 Beta**，仅验收于 Apple Silicon macOS 环境。

[快速开始](#快速开始零配置) · [配置说明](#配置说明) · [开发者指南](#开发者指南) · [安全说明](#安全说明) · [更新日志](CHANGELOG.md)

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
| 后端 | Rust（Axum + Tokio + rusqlite） | 1.97+，Edition 2024 |
| 前端 | React + TypeScript + Vite | Node 22，React 18 |
| Python 服务 | FastAPI + tree-sitter + Playwright | Python 3.11–3.12 |

Rust workspace 成员：`zk-core`、`zk-db`、`zk-llm`、`zk-protocol`、`zk-engine`、`zk-tools`、`zk-mcp`、`zk-authz`、`zk-server`。

## 快速开始（零配置）

**系统要求**：Apple Silicon Mac，macOS 15+

**一键安装启动**：

```bash
# 双击 install-zkcode.command，或在终端执行：
./install-zkcode.command
```

脚本全自动完成：Homebrew → Node 22 → Python 3.11 → Rust 1.97 → 依赖安装 → 编译 → 启动服务 → 打开浏览器。

发行包包含一个用于首次启动的引导数据库模板。系统会从模板导入一个公开、限额的千问百炼（DashScope）测试密钥，**无需手动配置即可完成首次体验**。该模板受版本控制，但不会作为用户日常运行时数据库；Session、配置和用户自己的密钥写入本机运行时数据库。

### ⚠️ 关于内置密钥的重要说明

内置密钥是供所有下载者共用的公开测试凭据，不应被视为秘密，也不适合处理敏感内容。它存在以下限制：

- 每日有模型调用次数限额
- 密钥可能随时被平台封禁/吊销

**强烈建议**启动成功后申请自己的独立密钥：

1. 前往 [阿里云百炼平台](https://bailian.console.aliyun.com/) 申请百炼订阅密钥；
2. 在浏览器中打开 **设置 → API Keys**，在对应 Provider 输入自己的密钥并保存；保存后立即生效，无需重启服务。

**如果内置密钥失效**：共享额度可能已用完，或测试密钥可能已被平台停用。若调用时出现 401/403，请按上述步骤使用自己的密钥替换。

## 安装完成后的日常使用

```bash
./start.sh   # 启动服务
./stop.sh    # 停止服务
```

启动后访问：<http://127.0.0.1:5273/>

## 端口分配

| 服务 | 地址 |
|---|---|
| 后端（zk-server） | 127.0.0.1:8082 |
| 前端（Vite） | 127.0.0.1:5273 |
| Python sidecar | Unix Domain Socket |

## 配置说明

所有配置通过根目录 `.env` 文件管理。关键配置分类：

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
./stop.sh
./start.sh
```

## 开发者指南

### 测试

```bash
# Rust
cargo test --workspace --locked

# 前端
cd frontend && npm run lint && npm run test:run && npm run build

# Python
cd python-service && .venv/bin/python -m pytest --cov=src --cov-fail-under=70
```

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
| `install-zkcode.command` | 一键安装器 |
| `start.sh` / `stop.sh` | 服务启停脚本 |

## 安全说明

- 仅监听 `127.0.0.1`，不支持远程/LAN 访问
- 无容器/VM 沙箱，工具以启动用户权限运行
- 不支持多用户/Docker 部署
- 新会话默认使用 `AUTO_APPROVE`（界面显示为“完全访问权限”）：文件写入、Shell、网络访问及其他工具请求不会逐次弹窗确认，但仍受路径、命令、敏感数据、Hook 和部署边界等系统安全检查约束
- 如需逐项确认，可先创建或选择会话，再打开 **设置 → 常规 → 权限模式**，切换为“默认模式”；只希望先规划、暂不修改文件时可切换为“计划模式”

完整说明见 [安全策略](SECURITY.md)、[安全模型](docs/security-model.md) 和 [数据与隐私](docs/data-and-privacy.md)。

## 已知限制

- 仅支持 Apple Silicon Mac
- Beta 阶段，API 可能变化
- Worktree 功能默认关闭

## 许可证

Apache License 2.0 — 详见 [LICENSE](LICENSE)。

第三方声明见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
