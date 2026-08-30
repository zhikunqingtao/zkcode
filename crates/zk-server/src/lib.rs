//! zk-server：zkcode 组装根（composition root）与网络入口——HTTP REST、
//! WebSocket 会话通道与服务生命周期管理。
//!
//! 9-crate 拓扑定位：最顶层 crate，依赖 zk-protocol / zk-db / zk-llm /
//! zk-engine 并完成装配。遵循 U2（SessionController 全部 8 端点）、U3（开发态
//! 8082 / 验收态 8080 双轨端口）、D7（localhost 鉴权模式）。
//!
//! # Phase 1（S7）范围
//!
//! REST 骨架 + 可观测预埋：
//! - 8 个会话域端点（`POST/GET /api/sessions`、`GET/DELETE /api/sessions/{id}`、
//!   `resume` / `compact` / `export` / `messages`）+ `GET /api/health` +
//!   鉴权三元组端点（`/api/auth/status`、`/api/auth/token`）+ `GET /metrics`；
//! - 响应形状以 `docs/baseline/samples/` 实采样例为逐键权威，samples 缺失的
//!   端点（messages 非空元素 / compact / markdown export）以旧仓库源码为准
//!   （`SessionController.java` / `Message.java` / `ContentBlock.java`）；
//! - 公共错误响应统一为 `{code,message,requestId}`，并在 `x-request-id` 响应头
//!   回传同一标识；全局序列化对齐旧 Jackson `NON_NULL`（null 字段
//!   剥离），export 端点对齐其独立 `ObjectMapper`（null 保留 + epoch 浮点秒）；
//! - 鉴权：D7 localhost 模式——loopback 直信，非 loopback 403（Phase 1 不做
//!   Cookie/Bearer/URL token 层级 2/3）；
//! - 可观测：tracing 结构化 JSON 日志（method/path/status/耗时/request-id）、
//!   metrics facade 请求计数与延迟直方图（自持轻量 Prometheus 文本输出）、
//!   `CatchPanic` 恢复、请求/响应体不落日志（§19 脱敏）。
//!
//! # 模块导航
//!
//! - [`access_token`]：局域网 access token 生成 / 持久化与会话 Cookie 签发
//!   （Batch 2b，旧 `RemoteAccessSecurityFilter` 的凭证半边）。
//! - [`config`]：环境变量装配（端口 / DB 路径 / 默认模型 / CORS 白名单）。
//! - [`state`]：`AppState`（`Db` 句柄 + 配置 + 启动时刻）。
//! - [`error`]：`ApiError` → HTTP 映射与统一错误响应。
//! - [`iso`]：epoch 毫秒 → RFC 3339（zk-db time 模块为 crate 私有，此处自持
//!   副本并以相同黄金值互锁）。
//! - [`routes`]：Router 组装与中间件栈（CatchPanic → 鉴权 → CORS → 观测）。
//! - [`middleware`]：请求观测（日志 + metrics）与三层递进准入中间件
//!   （Batch 2b：localhost 免认证 → 私有网段 token 认证 → 其余 403 / 401）。
//! - [`network`]：局域网 IPv4 探测与私有网段判定（Batch 2b，旧
//!   `RemoteAccessSecurityFilter.getLocalIp` / `SecurityConfig.getLocalIps`）。
//! - [`metrics_recorder`]：metrics facade 的轻量 in-process recorder。
//! - [`api`]：handler 层（DTO / 存储形状映射 / 端点实现）。
//! - [`workspace`]：Projects 域服务层（路径校验 / 目录浏览 / 原生选择器，2.1）。
//! - [`file_access`]：会话文件搜索 / 预览 / 原生揭示服务层（Batch 2 Step 2-4，旧
//!   `FileSearchService` / `SessionFileAccessService`）。
//! - [`ws`]：S8 原生 WebSocket 通道（hub 路由 / 背压双档 / 心跳双轨）。
//! - [`engine_bridge`]：S9 引擎接线（`EngineHook` 桥 + `WsHub` sink 适配）+
//!   2.3 工具注册表装配（基础工具族 9 件）。
//! - [`snapshot_sink`]：写前文件快照落库（zk-tools `SnapshotSink` × zk-db
//!   `file_snapshots`，2.3）。
//! - [`python`]：Python 侧车集成（2.6；UDS 传输 + `PythonSidecar` 生命周期 +
//!   `PythonClient` 能力缓存 + 桥接工具三件）。
//! - [`interaction`]：持久交互闭环 + Run 生命周期子集（2.5；旧
//!   `DurableInteractionService` / `RunControlService`）。
//! - [`skill`]：Skill 系统（3B.7；frontmatter 解析 + 六级来源注册表 +
//!   轮询热重载，旧 `skill` 包 + `SkillController`）。
//! - [`tool_catalog`]：工具目录展示面元数据（分组 / 权限枚举）与会话级
//!   启用位覆盖表（Batch 1 Step 1-5，旧 `Tool.getGroup` /
//!   `getPermissionRequirement` + `ToolSessionState`）。
//! - [`command`]：斜杠命令域（Batch 3；命令契约 / 注册表 / 11 个内建
//!   命令，旧 `command` 包 + `WebSocketController.handleSlashCommand`）。

pub mod access_token;
pub mod api;
pub mod authz;
pub mod command;
pub mod config;
pub mod cost;
mod demo_credentials;
pub mod engine_bridge;
pub mod error;
pub mod file_access;
pub mod http_fetch;
pub mod http_search;
pub mod interaction;
pub mod iso;
pub mod mcp;
pub mod mcp_search;
pub mod mcp_tools;
pub mod metrics_recorder;
pub mod middleware;
pub mod network;
pub mod oss_trust;
pub mod python;
pub mod routes;
pub mod run_termination;
pub mod session_access;
pub mod skill;
pub mod snapshot_sink;
pub(crate) mod speech;
pub mod state;
pub mod tool_catalog;
pub mod workspace;
pub mod ws;

/// 打印 zk-server 版本号（取 `CARGO_PKG_VERSION`，随 workspace 版本统一）。
pub fn print_version() {
    println!("zk-server {}", env!("CARGO_PKG_VERSION"));
}

/// 占位冒烟：验证 crate 编译、workspace lints 接线与 test harness 加载。
#[test]
fn crate_boots() {
    let skeleton_ready = true;
    assert!(skeleton_ready);
}
