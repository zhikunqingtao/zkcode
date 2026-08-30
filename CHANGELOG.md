# 更新日志

本文件记录 zkcode 的重要变更。格式参考
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

### 新增

- 增加全新 Apple Silicon Mac 的一键安装命令：安装受支持工具链与锁定依赖、处理
  本机版本冲突、限时启动全部服务并自动打开浏览器。
- 增加 GFM 与工作区 Markdown 图片渲染、DashScope ASR/TTS 语音交互。
- 增加 `qwen3.8-flash`，并将内置 GLM 视觉模型升级为 `glm-5.3-flash`。

### 变更

- 项目许可证由 MIT 迁移至 Apache License 2.0：根目录 LICENSE 已更新为官方
  文本，上游 ZhikunCode 的 MIT 声明完整保留于 THIRD_PARTY_NOTICES.md，
  贡献条款（CONTRIBUTING.md）同步更新。

### 修复

- 缺少 Homebrew 时在运行官方非交互安装器前安全完成 sudo 授权，
  并为 `--yes` 保留严格的无人值守语义。
- `./dev up` 不再复用 Python sidecar 已失效的后端，而会安全重启
  backend/sidecar 并重新验证 readiness。
- 源码开发禁用公开 demo 凭据时，持久移除旧版本已导入且来源可证明的
  demo key，同时保留用户自己的 provider 密钥。

### 安全

- 所有一键安装网络步骤使用有限重试、连接/总超时和失败关闭；不执行无限循环，
  不自动信任第三方镜像，也不卸载用户已有语言运行时。

## [0.1.0] - 2026-08-24

### 新增

- 面向 macOS Apple Silicon 的本地三进程安装与启动流程。
- Rust 后端、React 前端和 Python sidecar 的统一开发与质量门禁。
- 原生 WebSocket、REST、SSE、CLI 和 MCP 接口。
- 会话、Run、任务、快照、证据、产物、Workbench 与 Swarm 的 SQLite 持久化。
- Agent、工具、Hook、Python 浏览器能力、MCP 与可观测性链路。
- 机器可校验的 REST、WebSocket、Tool 和 DDL 契约。

### 安全

- 默认只监听 `127.0.0.1`，并对 REST、WebSocket、SSE 和 MCP 使用本地访问令牌。
- 默认关闭尚未完成真实 Git 验收的 Worktree 能力。
- 对文件路径、命令、敏感数据和 MCP 能力执行统一准入检查。

### 已知限制

- 当前仅支持 macOS Apple Silicon 本地安装；不支持 Docker、Linux、Windows 或远程部署。
- zkcode 是本地进程级安全边界，不是操作系统级沙箱。
- Worktree 真实 Git E2E 尚未验收，因此保持关闭。

[未发布]: https://github.com/zhikunqingtao/zkcode/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/zhikunqingtao/zkcode/releases/tag/v0.1.0
