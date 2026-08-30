# 故障排查

本页适用于 zkcode 0.1.x 的 macOS Apple Silicon 本地安装。不支持 Docker、Linux、
Windows、LAN/公网暴露或远程服务器部署。

## 先运行诊断

在仓库根目录执行：

```bash
./dev doctor
```

诊断会检查 Rust、Node、npm、curl、项目 Python 环境、`.env` 和本地服务状态，
不会打印密钥。服务启动失败时查看：

```bash
tail -n 100 .runtime/backend.log
tail -n 100 .runtime/frontend.log
```

分享日志前仍应人工检查工作区路径、提示词、文件内容和上游响应是否需要脱敏。

## 安装失败

### 一键安装命令失败或超时

`./dev bootstrap --start` 对缺失工具链、依赖、构建和启动分阶段执行；失败后保留已经验证
的组件和恢复备份，网络恢复后可安全重跑同一命令。兼容入口
`./install-zkcode.command` 会转发到该命令。Apple Command Line Tools 最多等待 30 分钟；
Homebrew 与 rustup 主安装最多 20 分钟；单个 Homebrew formula、npm、pip 或 Playwright
步骤最多 30 分钟；Cargo fetch 最多 20 分钟；首次 `zk-server` 构建最多 45 分钟；服务
readiness 最多约 4 分钟（包括冷启动 Python sidecar 的 90 秒窗口）。达到时限后脚本会终止
当前进程树、回滚正在替换的项目环境并返回非零状态；网络恢复后可安全地重新执行同一命令。

不要把官方安装脚本替换成来源不明的脚本。公司网络、地区网络或代理导致官方源不可达
时，优先配置组织提供且你信任的 HTTPS 代理，再重试：

```bash
export HTTPS_PROXY=http://127.0.0.1:你的代理端口
export HTTP_PROXY="$HTTPS_PROXY"
export ALL_PROXY="$HTTPS_PROXY"
./dev bootstrap --start
```

如果组织提供经过审核的镜像，可使用对应工具的标准环境变量，而不修改项目脚本：

```bash
export HOMEBREW_BOTTLE_DOMAIN=https://受信任的镜像
export NPM_CONFIG_REGISTRY=https://受信任的镜像
export PIP_INDEX_URL=https://受信任的镜像/simple
export PLAYWRIGHT_DOWNLOAD_HOST=https://受信任的镜像
export RUSTUP_DIST_SERVER=https://受信任的镜像
export RUSTUP_UPDATE_ROOT=https://受信任的镜像/rustup
./dev bootstrap --start
```

只设置你实际拥有并信任的镜像；变量值因服务方而异。失败后以终端中最早出现的
`error:` 为根因，不要通过删除 lock 文件、跳过 Chromium 或伪造依赖来绕过。

一键命令不会修改 shell profile，也不会卸载已有 Node/Python/Rust。现有版本冲突时，
项目脚本会明确优先选择 Homebrew `node@22`、`python@3.11` 与 `$HOME/.cargo/bin`；
这只影响 zkcode 的安装和启动进程。Apple Silicon 安装器只接受原生
`/opt/homebrew`，不会误用迁移遗留的 Intel `/usr/local` Homebrew。

### Homebrew 管理员授权失败

只有未安装原生 `/opt/homebrew` 时才需要 sudo。普通 `./dev bootstrap --start`
会在当前 Terminal 请求授权，最多等待 5 分钟。输入密码时没有字符或光标反馈是
macOS `sudo` 的正常行为；密码直接由 sudo 处理，zkcode 不会读取或保存。

取消、密码错误、当前账户不是管理员，或组织策略不允许 `mkdir` 权限时，命令以
退出码 13 停止，不会运行 Homebrew 安装器。修正账户或策略后可安全重试。

`--yes` 是严格的无人值守模式，不会从终端询问 sudo 密码。如果管理员允许使用临时
sudo 缓存，可在同一个受信 Terminal 中执行：

```bash
sudo -v
./dev bootstrap --start --yes
```

自动化环境应由管理员预先配置最小免密规则或受信的 `SUDO_ASKPASS`。不要使用
`sudo ./dev ...`，不要用 `sudo -S` 或管道传递密码，也不要修改脚本跳过权限检查。

### 工具版本不符合要求

源码开发入口要求 Rust 1.97.1、Node.js 22、npm 10 和 Python 3.11。
先安装 Xcode Command Line Tools，再按各项目官方方式安装对应版本：

```bash
xcode-select --install
rustc --version
node --version
npm --version
python3.11 --version
```

Intel Mac 和其他 macOS 版本可能工作，但 0.1.x 尚未承诺支持。

### npm 或 Python 依赖无法下载

不要手工复制单个依赖，也不要把缺失依赖改成可选项。确认能访问 npm、PyPI 后
重新执行 `./dev sync`；前端必须使用 `npm ci`，Python 必须使用
`python-service/requirements.lock`。

如果依赖替换失败，脚本会恢复旧依赖目录；修复下载问题后执行 `./dev up` 恢复服务。

### Playwright Chromium 下载失败

安装脚本会执行真实的浏览器安装。网络恢复后可单独重试：

```bash
./dev repair browser
```

不要通过伪造可执行文件或跳过能力探测来绕过。若明确不需要浏览器工具，可在
`.env` 中设置 `ZK_FEATURE_WEB_BROWSER_TOOL=false`，但这与默认完整功能配置不同。

## 启动失败

### 端口被占用

```bash
lsof -nP -iTCP:8082 -sTCP:LISTEN
lsof -nP -iTCP:5273 -sTCP:LISTEN
```

先确认占用者身份，再正常停止对应应用。不要使用宽泛的 `killall`。zkcode 自身应
使用 `./dev stop` 停止。

前后端使用新的本地进程会话与安装终端分离；终端退出不会终止服务，也不会注册开机
启动或在崩溃后自动无限重启。如果 PID 文件存在但服务异常，先运行 `./dev stop` 精确
停止并清理。停止脚本也会核验并停止由当前后端创建的 Python sidecar，防止 UDS
监听进程遗留；不要手动编辑 PID 文件或直接删除仍有监听者的 socket。

### Python sidecar 或 UDS 失败

确认 `.env` 的 `ZK_PYTHON_UDS` 位于当前用户可写目录、没有残留为其他进程使用，
并检查后端日志。正常 socket 权限是 `0600`。`./dev up` 会检查 Python
子系统；如果已记录后端的 sidecar 不再为 `UP`，它会只重启 backend/sidecar 并保留
健康的 Vite。若项目 `.venv` 本身损坏，执行 `./dev repair python` 后再运行 `./dev up`；
这些命令不会改写已有 `.env`。

### 后端健康检查超时

`./dev up` 最多等待后端 90 秒。检查 `.runtime/backend.log` 中最早的错误；常见原因
包括 SQLite 路径不可写、MCP 注册表路径错误、`.env` shell 语法错误或 Python
sidecar 启动失败。

## 页面能打开但不能对话

1. 确认 `./dev status` 同时显示前端和后端健康。
2. 检查 `.env` 至少有一个非空 `LLM_PROVIDER_*_API_KEY`，且模型清单包含
   `ZK_DEFAULT_MODEL`。
3. 执行 `./dev restart`，确保配置已重新加载。
4. 在 UI 中重新选择已授权 Project，确认 WebSocket 已连接。
5. 对 401/403 检查本地访问令牌和授权；对 429 等待 provider 限流恢复；对 5xx
   检查 provider 状态和端点配置。

配置详情见 [配置参考](configuration.md)。

## 文件或命令被拒绝

拒绝通常是安全准入的正常结果。确认当前 Project 对应正确的 canonical 工作区，
不要用符号链接、`..` 或任意 `workingDirectory` 绕过边界。敏感文件、高风险命令、
工作区外路径和修改后的 Hook 参数都可能需要再次批准或被拒绝。

Worktree 在 0.1.x 中固定关闭，因为真实 Git E2E 尚未验收；不要通过环境变量把它
当作已支持能力。

## 提交问题前

请附上 zkcode 版本、macOS/芯片、`./dev doctor --json` 输出、最小复现步骤和已脱敏日志。
不要附上 `.env`、数据库、访问令牌、私有源代码、绝对个人路径或完整模型请求。
安全问题请按 [SECURITY.md](../SECURITY.md) 私密报告。
