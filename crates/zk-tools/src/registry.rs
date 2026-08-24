//! 工具注册表——注册 / 查找 / 全量规格导出（对照旧 `ToolRegistry.java`）。
//!
//! # 内部可变（Batch 4B）
//!
//! MCP 服务器的工具是**运行时**发现的：连接握手成功后 `tools/list` 才拿到清单，
//! 断开时又要摘除。zk-mcp 以 `McpToolSink { fn register_dynamic(&self, ..) }`
//! 端口表达这一需求（`&self`，因为管理器持有的是 `Arc<dyn McpToolSink>`），而
//! 组合根装配完注册表后即以 `Arc<ToolRegistry>` 共享给 REST 目录端点与引擎，
//! 拿不到 `&mut`。故内表改 `RwLock<BTreeMap<..>>`：
//!
//! - `register(&mut self, ..)` 保留原签名（装配期批量注册的既有调用零改动），
//!   内部走 `get_mut` 无锁竞争路径；
//! - `register_dynamic(&self, ..)` / `unregister(&self, ..)` /
//!   `unregister_by_prefix(&self, ..)` 为运行时增删入口。
//!
//! 读路径（`get` / `specs` / `names` / `len`）改为持读锁快照——`Arc<dyn Tool>`
//! 克隆出锁，故不会把锁带进 `execute` 的 await 点。

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::tool::{Tool, ToolSpec};

/// 工具注册表（名字 → 工具实例；`BTreeMap` 保证 specs / names 输出稳定有序，
/// 下发 LLM 的 tools 列表与未知工具引导文案跨次运行确定）。
#[derive(Default)]
pub struct ToolRegistry {
    tools: RwLock<BTreeMap<String, Arc<dyn Tool>>>,
}

impl ToolRegistry {
    /// 构造空注册表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册工具；同名重复注册以后者覆盖并告警（对照旧 register 的
    /// put 覆盖语义 + 日志）。
    ///
    /// 装配期入口——独占借用故直接走 `get_mut`（无锁获取失败面；此路径只在
    /// 注册表被共享**之前**跑，毒化不可能已发生，故直接取内值）。
    pub fn register(&mut self, tool: Arc<dyn Tool>) {
        let name = tool.name().to_owned();
        let table = self
            .tools
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if table.insert(name.clone(), tool).is_some() {
            tracing::warn!(tool = %name, "duplicate tool registration overwrites previous");
        }
    }

    /// 运行时注册工具（MCP 工具发现入口，对照 Java
    /// `ToolRegistry.registerTool` 被 `McpClientManager` 在连接就绪后调用）。
    ///
    /// 语义与 [`Self::register`] 完全一致（同名覆盖 + 告警），区别仅在于以
    /// `&self` 接收：调用方持的是 `Arc<ToolRegistry>`。
    pub fn register_dynamic(&self, tool: Arc<dyn Tool>) {
        let name = tool.name().to_owned();
        if self.write_table().insert(name.clone(), tool).is_some() {
            tracing::warn!(tool = %name, "duplicate tool registration overwrites previous");
        }
    }

    /// Replace a runtime-derived tool snapshot without treating the expected
    /// refresh as a duplicate registration.
    ///
    /// This is intentionally separate from [`Self::register_dynamic`]: MCP
    /// discovery still warns when two independent registrations collide,
    /// while derived tools such as `ToolSearch` can atomically refresh their
    /// immutable catalog without emitting a warning on every capability poll.
    pub fn replace_dynamic(&self, tool: Arc<dyn Tool>) {
        let name = tool.name().to_owned();
        self.write_table().insert(name, tool);
    }

    /// 按精确名字摘除一个工具，返回被摘除的实例。
    ///
    /// Python 能力清单会在侧车启动、依赖变化或重启后改变；精确摘除避免用
    /// 前缀误伤同族的原生工具。读侧拿到的旧 `Arc` 可安全完成已开始的调用，
    /// 新的 REST/LLM 目录快照则立即不再暴露该能力。
    pub fn unregister(&self, name: &str) -> Option<Arc<dyn Tool>> {
        let removed = self.write_table().remove(name);
        if removed.is_some() {
            tracing::info!(tool = %name, "unregistered dynamic tool");
        }
        removed
    }

    /// 摘除名字以 `prefix` 起头的全部工具，返回摘除条数（MCP 服务器下线时按
    /// `mcp__{server}__` 前缀批量清理，对照 Java
    /// `ToolRegistry.unregisterByPrefix`）。
    pub fn unregister_by_prefix(&self, prefix: &str) -> usize {
        let mut table = self.write_table();
        let doomed: Vec<String> = table
            .keys()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect();
        for name in &doomed {
            table.remove(name);
        }
        if !doomed.is_empty() {
            tracing::info!(%prefix, removed = doomed.len(), "unregistered tools by prefix");
        }
        doomed.len()
    }

    /// 按名查找工具。
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.read_table().get(name).cloned()
    }

    /// 导出全量规格（名字典序，供 LLM tools 参数）。
    #[must_use]
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.read_table().values().map(|tool| tool.spec()).collect()
    }

    /// 全量工具名（名字典序，供未知工具引导文案的可用工具列表）。
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.read_table().keys().cloned().collect()
    }

    /// 注册数量。
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_table().len()
    }

    /// 是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read_table().is_empty()
    }

    /// 取读锁（毒化即清毒后续用——工具表只是句柄映射，持锁期不 panic 亦无
    /// 跨字段不变式，丢弃整张表远比拒绝服务更糟）。
    fn read_table(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Arc<dyn Tool>>> {
        self.tools.read().unwrap_or_else(|poisoned| {
            self.tools.clear_poison();
            poisoned.into_inner()
        })
    }

    /// 取写锁（毒化处理同 [`Self::read_table`]）。
    fn write_table(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Arc<dyn Tool>>> {
        self.tools.write().unwrap_or_else(|poisoned| {
            self.tools.clear_poison();
            poisoned.into_inner()
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::future::BoxFuture;
    use serde_json::json;

    use super::*;
    use crate::tool::{ToolContext, ToolOutput};

    /// 极简桩工具（可配置 name/description，返回固定文本）。
    struct StubTool {
        name: &'static str,
        description: &'static str,
    }

    impl Tool for StubTool {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self) -> &str {
            self.description
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object", "properties": {} })
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            Box::pin(futures::future::ready(ToolOutput::ok("stub")))
        }
    }

    fn stub(name: &'static str, description: &'static str) -> Arc<dyn Tool> {
        Arc::new(StubTool { name, description })
    }

    #[test]
    fn register_and_get_round_trip() {
        let mut registry = ToolRegistry::new();
        assert!(registry.is_empty());
        registry.register(stub("Echo", "echo tool"));
        registry.register(stub("Clock", "clock tool"));
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert_eq!(registry.get("Echo").expect("echo").name(), "Echo");
        assert!(registry.get("Missing").is_none());
    }

    #[test]
    fn specs_export_full_tuple_in_name_order() {
        let mut registry = ToolRegistry::new();
        // 逆字典序注册，导出必须按名有序（BTreeMap 语义）。
        registry.register(stub("Zeta", "z tool"));
        registry.register(stub("Alpha", "a tool"));
        let specs = registry.specs();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "Alpha");
        assert_eq!(specs[0].description, "a tool");
        assert_eq!(specs[0].parameters["type"], "object");
        assert_eq!(specs[1].name, "Zeta");
        assert_eq!(registry.names(), vec!["Alpha", "Zeta"]);
    }

    #[test]
    fn duplicate_registration_overwrites() {
        let mut registry = ToolRegistry::new();
        registry.register(stub("Echo", "first"));
        registry.register(stub("Echo", "second"));
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.get("Echo").expect("echo").description(), "second");
    }

    /// 共享句柄上的运行时注册对全部读路径立即可见（MCP 工具发现的核心不变
    /// 式：REST 目录端点与引擎看到的是同一张表）。
    #[test]
    fn dynamic_registration_is_visible_through_shared_handle() {
        let registry = Arc::new(ToolRegistry::new());
        let reader = Arc::clone(&registry);
        registry.register_dynamic(stub("mcp__weather__forecast", "mcp tool"));
        assert_eq!(reader.len(), 1);
        assert!(reader.get("mcp__weather__forecast").is_some());
        assert_eq!(reader.names(), vec!["mcp__weather__forecast"]);
    }

    /// 前缀摘除只清同前缀条目，且返回摘除条数（服务器下线的批量清理语义）。
    #[test]
    fn unregister_by_prefix_removes_only_matching_tools() {
        let mut registry = ToolRegistry::new();
        registry.register(stub("Read", "builtin"));
        registry.register(stub("mcp__weather__forecast", "mcp a"));
        registry.register(stub("mcp__weather__alerts", "mcp b"));
        registry.register(stub("mcp__github__issues", "mcp c"));

        assert_eq!(registry.unregister_by_prefix("mcp__weather__"), 2);
        assert_eq!(registry.names(), vec!["Read", "mcp__github__issues"]);
        // 无匹配时为零操作。
        assert_eq!(registry.unregister_by_prefix("mcp__weather__"), 0);
    }

    #[test]
    fn replace_dynamic_refreshes_without_changing_registry_size() {
        let registry = ToolRegistry::new();
        registry.register_dynamic(stub("Echo", "old description"));

        registry.replace_dynamic(stub("Echo", "new description"));

        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry
                .get("Echo")
                .expect("refreshed tool")
                .spec()
                .description,
            "new description"
        );
    }

    #[test]
    fn unregister_removes_only_the_exact_name() {
        let mut registry = ToolRegistry::new();
        registry.register(stub("Git", "python bridge"));
        registry.register(stub("GitStatus", "native"));

        assert_eq!(registry.unregister("Git").expect("removed").name(), "Git");
        assert!(registry.get("Git").is_none());
        assert!(registry.get("GitStatus").is_some());
        assert!(registry.unregister("Git").is_none());
    }
}
