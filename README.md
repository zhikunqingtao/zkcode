# zkcode

面向 macOS 本地开发的 Rust-native AI 编码助手。

zkcode 从 [ZhikunCode](https://github.com/zhikunqingtao/zhikuncode) 演进而来，
保留 React 操作面和 Python 能力服务，以 Rust 实现会话引擎、原生 WebSocket、
安全准入、工具编排、多 Agent、MCP 和单库 SQLite 持久化。

当前版本：**0.1.0 Beta**。仅在维护者的 `arm64 / macOS 26.5.2` 环境完成真实
验收；其他 Apple Silicon Mac 可能可用，但尚不承诺。版本变化见
[更新日志](CHANGELOG.md)。

[快速安装](#快速安装) · [完成第一个任务](#完成第一个任务) ·
[配置](docs/configuration.md) · [数据与隐私](docs/data-and-privacy.md) ·
[安全](SECURITY.md) · [故障排查](docs/troubleshooting.md) ·
[参与贡献](CONTRIBUTING.md)

## 能力概览

- REST、原生 WebSocket、SSE 和 CLI 连续对话
- Read、Edit、Write、Bash、Notebook、搜索、可视化和验证工具
- 路径、命令、敏感数据、PRE Hook 与统一授权准入
- Session、Run、Task、Snapshot、Evidence、Artifact 和 Workbench 持久化
- 子 Agent、Team、只读 Swarm 与定向取消/重启恢复
- MCP Client/Server、Python UDS sidecar 和真实浏览器回放
- OpenAI-compatible、Anthropic 原生协议及多 provider 模型配置
- REST、WebSocket、Tool 和 SQLite DDL 机器契约门禁

Worktree 默认关闭。它不会在未完成真实 Git E2E 验收时被描述为可用能力。

## 先了解安全边界

zkcode **没有**容器、虚拟机或操作系统级沙箱。工具、Shell、Hook、Skill、MCP
和 Agent 以启动 zkcode 的 macOS 用户权限运行。授权提示和路径检查可以降低风险，
但不能隔离恶意代码。

本 Beta 只支持 `127.0.0.1` 本地访问，不支持 Docker、LAN/公网暴露、反向代理、
远程部署或多用户共享。不要用 zkcode 打开不可信仓库。完整说明见
[安全策略](SECURITY.md) 和 [数据与隐私](docs/data-and-privacy.md)。

## 环境要求

| 组件 | 要求 | 当前实测 |
|---|---|---|
| Mac | Apple Silicon | macOS 26.5.2 arm64 |
| Rust | 1.97+ | 1.97.1 |
| Node.js / npm | Node.js 22 / npm 10 | 22.14.0 / 10.9.2 |
| Python | 3.11 或 3.12 | 3.11.15 |
| 系统工具 | Xcode Command Line Tools、curl | macOS 自带/官方安装 |

依赖必须通过 `Cargo.lock`、`frontend/package-lock.json` 和
`python-service/requirements.lock` 安装。

## 快速安装

### 1. 获取源码

一键安装器在源码已经位于本机后运行。真正全新的 Mac 尚没有 Git 时，先在 GitHub
仓库页面选择 **Code → Download ZIP**，解压并在终端进入 `zkcode` 目录。这样不依赖
Command Line Tools。已经安装 Git 的机器也可以使用：

```bash
git clone https://github.com/zhikunqingtao/zkcode.git
cd zkcode
```

### 2. 全新 Mac 一键安装并启动

```bash
./install-zkcode.command
```

这一个命令会完成：

1. 请求安装 Apple Xcode Command Line Tools（首次运行需在系统对话框确认）；
2. 从官方入口安装 Homebrew、Node.js 22、Python 3.11 和 Rust stable 1.97+；
3. 优先使用项目支持的版本，不卸载或覆盖 Mac 上已有的其他语言版本；
4. 使用三份 lock 文件安装依赖，并安装真实 Playwright Chromium；
5. 构建后端、运行诊断、启动全部本地服务并打开默认浏览器。

首次安装需要访问 Cargo、npm、PyPI 和 Playwright 下载源。任何依赖或 Chromium
下载失败都会让脚本明确失败；所有下载、安装和健康等待都有最大时限，不会无限
重试或永久卡住。请恢复正确的下载条件后重新执行同一命令，不要手工绕过。代理、
可信镜像和超时处理见 [故障排查](docs/troubleshooting.md#一键安装命令失败或超时)。

如果所需工具已经存在，命令会校验并复用；它不会执行 Docker，也不会创建沙箱。
需要分步排查时，可以改用手动安装路径：

```bash
./scripts/setup-macos.sh
./scripts/doctor.sh
./start.sh
```

### 3. 配置模型

编辑仓库根目录 `.env`，至少配置一个 provider。推荐的短任务配置示例：

```dotenv
LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY=在本机填写真实密钥
LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_MODELS=qwen3.8-max
ZK_DEFAULT_MODEL=qwen3.8-max
```

不要把 `.env`、真实密钥或访问令牌提交到版本库。其他 provider、模型降级链和
功能开关见 [配置参考](docs/configuration.md)。如果一键安装时还没有密钥，它已经用空
配置启动了服务；保存 `.env` 后必须重启一次以加载新配置：

```bash
./stop.sh
./start.sh
```

### 4. 后续手动诊断与启动

```bash
./scripts/doctor.sh
./start.sh
```

一键命令会自动执行这一步并打开浏览器。以后手动启动时，看到 `zkcode is ready` 后打开
[http://127.0.0.1:5273](http://127.0.0.1:5273)。后端默认使用
`127.0.0.1:8081`（可通过 `ZK_PORT` 修改），Python 服务只监听权限为 `0600` 的本地
Unix socket。

## 完成第一个任务

1. 在输入框发送一条消息；首次创建 Session 时会打开“选择文件夹授权”。
2. 选择一个专用、可信的本地项目目录，阅读授权说明后创建或选择 Project。
3. 先发送一个只读小任务，例如“读取 README 并用三点总结项目结构”。
4. 确认模型回复和工具记录正常，再尝试一次范围明确的小编辑。
5. 对工作区外路径、敏感文件、Shell、Hook 或第三方 MCP 请求逐项检查后再批准。

不要把仓库级长 Agent、长 Swarm 或基准评测作为首次验证。发生问题先看
[故障排查](docs/troubleshooting.md)。

## 启动、停止与日志

```bash
./start.sh
./stop.sh
```

前后端在独立的本地进程会话中运行，因此关闭安装终端后仍会继续运行；它们不会被
注册为开机启动项，也不会在异常退出后无限重启。进程 PID 和日志保存在忽略提交的
`.runtime/`。停止脚本会核对 PID 对应命令，不使用宽泛进程匹配。常用检查：

```bash
curl --fail http://127.0.0.1:8081/api/health
tail -n 100 .runtime/backend.log
tail -n 100 .runtime/frontend.log
```

## CLI

完成安装并启动服务后，可以从项目 Python 环境调用：

```bash
python-service/.venv/bin/zkcode --help
python-service/.venv/bin/zkcode "解释当前项目结构"
```

CLI 默认连接 `http://127.0.0.1:8081`，与 Web UI 共用同一会话和授权模型。

## 配置与本地数据

- [配置参考](docs/configuration.md)：模型 provider、端口、工作区、Python、MCP
  和功能开关。
- [数据与隐私](docs/data-and-privacy.md)：SQLite、日志、快照、上传、外部数据流、
  备份和重置。
- [安全策略](SECURITY.md)：本地执行边界、支持版本和漏洞报告。
- [故障排查](docs/troubleshooting.md)：安装、Chromium、UDS、端口和模型错误。

## 开发与测试

快速门禁命令：

```bash
./scripts/parity/check-contracts.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cd frontend && npm run lint && npm run test:run && npm run build
cd ../python-service && .venv/bin/python -m pytest --cov=src --cov-fail-under=70
```

完整发布门禁使用 `./scripts/parity/run-local-gates.sh`。其中还包含 release build、
依赖审计、secret scanning 和供应链策略检查。真实模型与浏览器测试是独立的短时
opt-in 门禁：只从本地环境注入密钥，不得回显密钥，也不对来自 fork 的代码运行。

完整开发说明见 [本地开发](docs/dev-run.md) 和 [贡献指南](CONTRIBUTING.md)。

## 项目结构与设计资料

| 目录 | 说明 |
|---|---|
| `crates/` | Rust workspace：协议、数据库、模型、工具、授权、引擎、MCP 和服务 |
| `frontend/` | React + TypeScript + Vite 浏览器界面 |
| `python-service/` | Python 能力服务和 `zkcode` CLI |
| `scripts/` | macOS 安装、诊断、契约与真实短冒烟脚本 |
| `docs/` | 架构、兼容性、安全与公开契约资料 |

进一步阅读：

- [架构](docs/architecture.md)
- [兼容性与有意差异](docs/compatibility.md)
- [安全模型](docs/security-model.md)
- [功能对齐证据](docs/parity/README.md)
- [更新日志](CHANGELOG.md)

## 已知限制

- 仅在维护者当前 Apple Silicon Mac 上验证，不承诺 Intel Mac 或通用 macOS 版本。
- 不支持 Docker、远程部署、LAN 访问、多用户模式或 OS 级沙箱。
- Worktree 保持关闭；远端 MCP `wss://` 全矩阵仍需扩展验证。
- 普通发布门禁不运行仓库级长 Agent、长 Swarm、SWE-bench 或类似复杂任务。

## 贡献、行为准则与许可证

提交变更前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 和
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。漏洞请按 [SECURITY.md](SECURITY.md)
私密报告。

zkcode 使用 [MIT License](LICENSE)。来源和第三方声明见
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
