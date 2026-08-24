# 更新日志

本文件记录 zkcode 的重要变更。格式参考
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

### 新增

- 增加全新 Apple Silicon Mac 的一键安装命令：安装受支持工具链与锁定依赖、处理
  本机版本冲突、限时启动全部服务并自动打开浏览器。

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
