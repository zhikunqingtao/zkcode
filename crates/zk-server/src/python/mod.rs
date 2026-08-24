//! Python 侧车集成（2.6 / M15）——进程生命周期 + 能力感知客户端 + 桥接工具。
//!
//! # 架构定位
//!
//! 旧系统为 `Java → HTTP(loopback TCP :8000) → Python FastAPI → {tree-sitter,
//! gitpython, playwright, …}` 三段式。本移植按方案 v1.4 决策 **D-P2-2** 将
//! IPC 传输改为 **Unix Domain Socket**（`uvicorn --uds ~/.zkcode/python.sock`，
//! `ZK_PYTHON_UDS` 可覆盖）：HTTP 报文与路由契约完全不变，**python-service
//! Python 服务保留原拓扑，并补上 macOS 版本/import preflight、离线 tokenizer
//! 和有界浏览器启动探测。
//!
//! # 模块构成
//!
//! - [`uds`]：HTTP over UDS 传输层（hyper http1 + `TokioIo` + `UnixStream`）。
//! - [`client`]：[`PythonClient`]——旧 `PythonCapabilityAwareClient` 的等价物
//!   （三档超时 / 指数退避 / 能力缓存双 TTL / 只读端点白名单）。
//! - [`sidecar`]：[`PythonSidecar`]——旧 `PythonProcessManager` 的等价物
//!   （启动 / 30s 健康轮询 / 崩溃自动重启 ≤3 次 / 优雅停止 / socket 清理）。
//! - [`tools`]：桥接工具族 `WebBrowser` / `CodeIntel` / `Git`。
//! - [`proxy`]：前端六组分析面板 → 侧车的 UDS 白名单反向代理。
//!
//! # 为何桥接工具放在 zk-server 而非 zk-tools
//!
//! 桥接工具需要 [`PythonClient`]（进程生命周期的同伴对象，由组装根持有），
//! 而 zk-tools 位于 zk-server **下游**——依赖方向铁律禁止 zk-tools 反向依赖
//! zk-server。故三件工具在此实现 zk-tools 的 `Tool` trait，由
//! [`crate::engine_bridge::build_tool_registry`] 注册进同一注册表；对引擎与
//! LLM 而言与原生工具完全同形。
//!
//! # 能力降级矩阵
//!
//! | 场景 | 核心对话 | `CodeIntel` / `Git` / `WebBrowser` |
//! |---|---|---|
//! | 侧车 `RUNNING` 且能力就绪 | 正常 | 正常执行 |
//! | 侧车 `RUNNING`、某能力缺依赖 | 正常 | 返回该能力的 `*_UNAVAILABLE` 提示 |
//! | 侧车未起 / `FAILED` | **正常** | 同上（不 panic、不阻塞、不重试风暴） |
//! | `ZK_PYTHON_ENABLED=false` | **正常** | 工具不注册（LLM 不可见） |
//!
//! 全链路无 `unwrap` on 外部输入，所有 Python 调用失败均折叠为 `None` →
//! 工具层给出旧端逐字一致的降级文案，主对话链路（zk-engine / zk-llm）
//! 完全不感知 Python 状态。

pub mod client;
pub(crate) mod proxy;
pub mod sidecar;
pub mod tools;
mod uds;

pub use client::{CapabilityStatus, Correlation, PythonClient};
pub use sidecar::{ProcessState, PythonSidecar, SidecarConfig};
pub use tools::{BrowserVerifyJourneyTool, CodeIntelTool, GitEnhancedTool, WebBrowserTool};
