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
- 增加统一 TaskRuntime V4、根任务 token/费用/时限硬预算、持久化 Cron/Research
  工具和任务诊断接口。

### 变更

- TaskOutput 模型文本去除与 content 完全相同的 output 别名，结构化响应和分页契约保持不变；
  Coordinator 综合提示明确保留逐项证据等级、来源和不确定性，禁止把未核验数字提升为确定结论。

- 搜索供应商无链接摘要保留为明确标注的未核验线索，不再误报空搜索，也不生成来源证据。
  静态网页仅有标题时明确返回正文不可读；子 Agent 在 50 轮上限内预留最后一轮无工具总结，
  结果保持 partial/MAX_TURNS，不延长轮次或恢复费用预算。

- Agent 的 30 分钟期限由 TaskRuntime 管理，取消通用工具执行器的 10 分钟钳制；
  超时提供 30 秒收尾并恢复部分正文和产物引用，父任务可综合已完成内容。
  无 token/费用上限时，仅清理已确认的超时子调用允许未知 usage 继续保留；其他用量缺失仍拒绝准入。

- 项目许可证由 MIT 迁移至 Apache License 2.0：根目录 LICENSE 已更新为官方
  文本，上游 ZhikunCode 的 MIT 声明完整保留于 THIRD_PARTY_NOTICES.md，
  贡献条款（CONTRIBUTING.md）同步更新。
- WebSocket 协议从 v3 一次性切换至 v4，旧版客户端不再兼容。
- `ZK_AGENT_WRITE_ENABLED` 与 `ZK_SWARM_ENABLED` 默认关闭；新增根任务预算、
  shared workspace、自动恢复和 Cron 的显式配置项。
- 顶层 Coordinator 改为默认关闭的进程级显式模式；
  `ZHIKUN_COORDINATOR_MODE` 仅接受 `0` / `1`，配置变更需重启，并与
  `COORDINATOR_MODE` feature flag、Agent runtime 共同门控。
- 根任务 token 与费用硬上限改为显式可选；默认不再因固定额度终止根任务或子 Agent，
  但继续持久记录真实 usage/费用，并保留 Deadline、最大轮数和 usage 完整性门禁。
- 数据库采用 greenfield 最终 schema，不对旧 `.zk/data.db` 原地迁移或回填。

### 修复

- Coordinator 系统提示改用 TaskRuntime V4 的真实工具名、lowerCamelCase 字段、
  attached barrier、SQLite result receipt 与预算语义，移除旧版伪协议示例。
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
