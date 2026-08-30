//! REST handler 层——DTO、存储形状 → 线上形状映射、端点实现。
//!
//! - [`dto`]：REST 线上 DTO（旧 Controller DTO records + `Message` /
//!   `ContentBlock` 的 REST 通道序列化形状）。
//! - [`mapping`]：zk-db 存储形状（`StoredBlock` / `MessageRecord` /
//!   `SessionDetail`）→ REST 线上 JSON 的转换，及 compact 确定性压缩、
//!   markdown 导出、token 估算（对齐旧 `TokenCounter`）。
//! - [`session`]：U2 的 8 个会话域端点。
//! - [`system`]：健康检查（含 live/ready 双探针）、鉴权三元组、Prometheus
//!   指标端点。
//! - [`models`]：模型目录端点（S7b，Phase 1 静态目录）。
//! - [`config`]：用户全局配置与项目级配置端点（S7b + 2.1，zk-db 双表持久化）。
//! - [`project`]：Projects 域 5 端点（2.1，服务层见 [`crate::workspace`]）。
//! - [`interaction`]：交互 pending 查询与 CAS 决策 2 端点（2.5，旧
//!   `InteractionController`）。
//! - [`grant`]：权限授权列表与撤销 2 端点（2.5，旧 `PermissionGrantController`）。
//! - [`skill`]：技能目录与详情 2 端点（3B.7，旧 `SkillController`）。
//! - [`tool`]：工具目录 / 详情 / 会话级开关 3 端点（Batch 1 Step 1-5，
//!   旧 `ToolController`）。
//! - [`doctor`]：环境诊断端点（Batch 1 Step 1-6，旧
//!   `HealthController.doctor`）。
//! - [`openapi`]：`OpenAPI` 文档聚合与 `GET /api/openapi.json`（S7b）。
//! - [`run`]：Run 域 3 读端点（Batch 2 Step 2-5，旧 `RunController`）。
//! - [`file`]：File 域 3 端点（Batch 2 Step 2-4，旧 `FileController`）。
//! - [`activity`]：Activity 域 1 端点（Batch 2 Step 2-6，旧 `ActivityController`）。
//! - [`attachment`]：Attachment 域 2 端点（Batch 2 Step 2-7，旧 `AttachmentController`）。
//! - [`http_params`]：Spring `@RequestParam` 绑定/失败语义共享助手。
//! - [`remote`]：远程控制 2 端点（Batch 2b Step 2b-6，旧
//!   `RemoteControlController`）。
//! - [`history`]：文件历史域 3 端点（Batch 5 Step 6，旧
//!   `FileHistoryController`）。
//! - [`memory`]：记忆域 5 端点（Batch 5 Step 6，旧 `MemoryController`）。

pub(crate) mod config;
pub(crate) mod doctor;
// ── Task 4 Step 5：LLM 密钥管理端点（GET/PUT /api/llm-keys）──
pub(crate) mod dto;
pub(crate) mod evidence;
pub(crate) mod grant;
pub(crate) mod interaction;
pub(crate) mod llm_keys;
pub(crate) mod mapping;
pub(crate) mod mcp;
pub(crate) mod mcp_capability;
pub(crate) mod mcp_server;
pub(crate) mod models;
pub(crate) mod openapi;
pub(crate) mod project;
pub(crate) mod query;
pub(crate) mod session;
pub(crate) mod session_snapshot;
pub(crate) mod skill;
pub(crate) mod speech;
pub(crate) mod system;
pub(crate) mod tool;
pub(crate) mod verify;
pub(crate) mod workbench;
// ── Batch 2 端点域（Step 2-4~2-7）──
pub(crate) mod activity;
pub(crate) mod artifact;
pub(crate) mod attachment;
pub(crate) mod file;
pub(crate) mod http_params;
pub(crate) mod run;
// ── Batch 2b 远程控制域（Step 2b-6）──
pub(crate) mod remote;
// ── Batch 5 记忆与历史域（Step 6）──
pub(crate) mod history;
pub(crate) mod memory;
// ── Batch 6 Phase C：Swarm 多代理协作端点（Step 6-16）──
pub(crate) mod swarm;
// ── Batch 8B：Admin 认证 + Dialog 决策端点（Step 7/8）──
pub(crate) mod admin;
pub(crate) mod dialog;
// ── Batch 8G：Plugin / BrowserReplay / CodeAnalysis 端点（Step 3-5）──
pub(crate) mod browser_replay;
pub(crate) mod code_analysis;
pub(crate) mod command;
pub(crate) mod plugin;
