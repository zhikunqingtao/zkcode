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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::tool::{Tool, ToolSpec};

/// 工具注册表（名字 → 工具实例；`BTreeMap` 保证 specs / names 输出稳定有序，
/// 下发 LLM 的 tools 列表与未知工具引导文案跨次运行确定）。
#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: Arc<RwLock<BTreeMap<String, Arc<dyn Tool>>>>,
    generation: Arc<AtomicU64>,
    revocations: Arc<RwLock<BTreeMap<String, CancellationToken>>>,
    visibility: Option<Arc<ToolVisibility>>,
}

/// A live visibility policy evaluated against the currently registered tool
/// instance. Unlike a frozen name snapshot, this also governs tools discovered
/// after a child registry view was created (for example MCP/Python tools).
type ToolVisibility = dyn Fn(&str, &dyn Tool) -> bool + Send + Sync;

/// Atomic directory resolution used by the execution path.  The binding keeps
/// the exact tool instance and both revocation generations observed while the
/// registry read lock was held.
#[derive(Clone)]
pub struct ToolBinding {
    tool: Arc<dyn Tool>,
    directory_generation: u64,
    connection_generation: Option<u64>,
    revocation: CancellationToken,
}

impl ToolBinding {
    /// Exact tool instance resolved from the directory.
    #[must_use]
    pub fn tool(&self) -> Arc<dyn Tool> {
        Arc::clone(&self.tool)
    }

    /// Monotonic directory generation captured with the tool instance.
    #[must_use]
    pub const fn directory_generation(&self) -> u64 {
        self.directory_generation
    }

    /// Reconnectable transport generation, if any.
    #[must_use]
    pub const fn connection_generation(&self) -> Option<u64> {
        self.connection_generation
    }

    /// Token cancelled when this exact directory binding is replaced or removed.
    #[must_use]
    pub fn revocation_token(&self) -> CancellationToken {
        self.revocation.clone()
    }
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
        let mut table = self.write_table();
        if table.insert(name.clone(), tool).is_some() {
            tracing::warn!(tool = %name, "duplicate tool registration overwrites previous");
        }
        self.rotate_revocation(&name);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// 运行时注册工具（MCP 工具发现入口，对照 Java
    /// `ToolRegistry.registerTool` 被 `McpClientManager` 在连接就绪后调用）。
    ///
    /// 语义与 [`Self::register`] 完全一致（同名覆盖 + 告警），区别仅在于以
    /// `&self` 接收：调用方持的是 `Arc<ToolRegistry>`。
    pub fn register_dynamic(&self, tool: Arc<dyn Tool>) {
        let name = tool.name().to_owned();
        let mut table = self.write_table();
        if table.insert(name.clone(), tool).is_some() {
            tracing::warn!(tool = %name, "duplicate tool registration overwrites previous");
        }
        self.rotate_revocation(&name);
        self.generation.fetch_add(1, Ordering::AcqRel);
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
        let mut table = self.write_table();
        table.insert(name.clone(), tool);
        self.rotate_revocation(&name);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// 按精确名字摘除一个工具，返回被摘除的实例。
    ///
    /// Python 能力清单会在侧车启动、依赖变化或重启后改变；精确摘除避免用
    /// 前缀误伤同族的原生工具。读侧拿到的旧 `Arc` 可安全完成已开始的调用，
    /// 新的 REST/LLM 目录快照则立即不再暴露该能力。
    pub fn unregister(&self, name: &str) -> Option<Arc<dyn Tool>> {
        let mut table = self.write_table();
        let removed = table.remove(name);
        if removed.is_some() {
            self.revoke_name(name);
            self.generation.fetch_add(1, Ordering::AcqRel);
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
            self.revoke_name(name);
        }
        if !doomed.is_empty() {
            self.generation.fetch_add(1, Ordering::AcqRel);
            tracing::info!(%prefix, removed = doomed.len(), "unregistered tools by prefix");
        }
        doomed.len()
    }

    /// 按名查找工具。
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.resolve(name).map(|binding| binding.tool())
    }

    /// Resolve a name, exact instance and both generations as one read-locked
    /// snapshot.  Callers must revalidate after asynchronous admission and
    /// before starting side effects.
    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<ToolBinding> {
        let table = self.read_table();
        let tool = table.get(name)?.clone();
        if !self.is_visible(name, tool.as_ref()) {
            return None;
        }
        let revocation = self
            .revocations
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)?
            .clone();
        Some(ToolBinding {
            connection_generation: tool.connection_generation(),
            tool,
            directory_generation: self.generation.load(Ordering::Acquire),
            revocation,
        })
    }

    /// Whether a previously resolved tool is still the currently authorized
    /// instance in exactly the same directory and connection generation.
    #[must_use]
    pub fn is_binding_current(&self, binding: &ToolBinding) -> bool {
        if !self.is_visible(binding.tool.name(), binding.tool.as_ref()) {
            return false;
        }
        let table = self.read_table();
        self.generation.load(Ordering::Acquire) == binding.directory_generation
            && !binding.revocation.is_cancelled()
            && table
                .get(binding.tool.name())
                .is_some_and(|current| Arc::ptr_eq(current, &binding.tool))
            && binding
                .connection_generation
                .is_none_or(|generation| binding.tool.is_connection_generation_current(generation))
    }

    /// 导出全量规格（名字典序，供 LLM tools 参数）。
    #[must_use]
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.read_table()
            .iter()
            .filter(|(name, tool)| self.is_visible(name, tool.as_ref()))
            .map(|(_, tool)| tool.spec())
            .collect()
    }

    /// 全量工具名（名字典序，供未知工具引导文案的可用工具列表）。
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.read_table()
            .iter()
            .filter(|(name, tool)| self.is_visible(name, tool.as_ref()))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// 注册数量。
    #[must_use]
    pub fn len(&self) -> usize {
        self.names().len()
    }

    /// 是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names().is_empty()
    }

    /// Monotonic generation shared by every filtered view of this directory.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Create a live, read-only visibility view over the same directory. Dynamic
    /// replacement and revocation are immediately observed; nested views intersect
    /// their allowlists and can never widen authority.
    #[must_use]
    pub fn filtered(&self, names: BTreeSet<String>) -> Self {
        let names = Arc::new(names);
        self.filtered_by(move |name, _| names.contains(name))
    }

    /// Create a live, read-only policy view over this directory.
    ///
    /// The predicate is evaluated for every lookup/snapshot, so a matching tool
    /// registered after view construction becomes visible immediately. Nested
    /// views intersect predicates and therefore cannot widen their parent.
    #[must_use]
    pub fn filtered_by(
        &self,
        predicate: impl Fn(&str, &dyn Tool) -> bool + Send + Sync + 'static,
    ) -> Self {
        let predicate: Arc<ToolVisibility> = Arc::new(predicate);
        let visibility = match self.visibility.as_ref() {
            None => predicate,
            Some(current) => {
                let current = Arc::clone(current);
                Arc::new(move |name: &str, tool: &dyn Tool| {
                    current(name, tool) && predicate(name, tool)
                }) as Arc<ToolVisibility>
            }
        };
        Self {
            tools: Arc::clone(&self.tools),
            generation: Arc::clone(&self.generation),
            revocations: Arc::clone(&self.revocations),
            visibility: Some(visibility),
        }
    }

    fn rotate_revocation(&self, name: &str) {
        let mut revocations = self
            .revocations
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = revocations.insert(name.to_owned(), CancellationToken::new()) {
            previous.cancel();
        }
    }

    fn revoke_name(&self, name: &str) {
        if let Some(token) = self
            .revocations
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name)
        {
            token.cancel();
        }
    }

    fn is_visible(&self, name: &str, tool: &dyn Tool) -> bool {
        self.visibility
            .as_ref()
            .is_none_or(|visibility| visibility(name, tool))
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

    #[test]
    fn replacing_or_removing_a_binding_cancels_its_revocation_token() {
        let registry = ToolRegistry::new();
        registry.register_dynamic(stub("Echo", "first"));
        let first = registry.resolve("Echo").expect("first binding");
        assert!(!first.revocation_token().is_cancelled());

        registry.replace_dynamic(stub("Echo", "second"));
        assert!(first.revocation_token().is_cancelled());
        let second = registry.resolve("Echo").expect("second binding");
        assert!(!second.revocation_token().is_cancelled());

        registry.unregister("Echo");
        assert!(second.revocation_token().is_cancelled());
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

    #[test]
    fn live_policy_view_admits_matching_tools_registered_after_creation() {
        let registry = ToolRegistry::new();
        let child = registry.filtered_by(|name, _| name.starts_with("safe__"));
        assert!(child.is_empty());

        registry.register_dynamic(stub("safe__late", "late safe tool"));
        registry.register_dynamic(stub("unsafe__late", "late unsafe tool"));

        assert_eq!(child.names(), vec!["safe__late"]);
        assert!(child.get("safe__late").is_some());
        assert!(child.get("unsafe__late").is_none());
    }

    #[test]
    fn nested_live_policy_views_only_narrow_authority() {
        let registry = ToolRegistry::new();
        let broad = registry.filtered_by(|name, _| name.starts_with("safe__"));
        let exact = broad.filtered(BTreeSet::from(["safe__one".to_owned()]));

        registry.register_dynamic(stub("safe__one", "one"));
        registry.register_dynamic(stub("safe__two", "two"));
        registry.register_dynamic(stub("unsafe__one", "unsafe"));

        assert_eq!(broad.names(), vec!["safe__one", "safe__two"]);
        assert_eq!(exact.names(), vec!["safe__one"]);
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

    #[test]
    fn filtered_views_are_live_and_never_widen_nested_authority() {
        let mut registry = ToolRegistry::new();
        registry.register(stub("Read", "read v1"));
        registry.register(stub("Write", "write"));
        let start_generation = registry.generation();
        let view = registry.filtered(BTreeSet::from(["Read".to_owned()]));
        assert_eq!(view.names(), vec!["Read"]);
        assert!(view.get("Write").is_none());

        registry.replace_dynamic(stub("Read", "read v2"));
        assert!(view.generation() > start_generation);
        assert_eq!(view.get("Read").expect("read").description(), "read v2");

        let nested = view.filtered(BTreeSet::from(["Read".to_owned(), "Write".to_owned()]));
        assert_eq!(nested.names(), vec!["Read"]);
        registry.unregister("Read");
        assert!(view.get("Read").is_none());
        assert!(nested.is_empty());
    }

    #[test]
    fn resolved_binding_is_invalidated_by_directory_replacement_or_removal() {
        let registry = ToolRegistry::new();
        registry.register_dynamic(stub("Echo", "v1"));
        let first = registry.resolve("Echo").expect("first binding");
        assert!(registry.is_binding_current(&first));

        registry.replace_dynamic(stub("Echo", "v2"));
        assert!(!registry.is_binding_current(&first));
        let second = registry.resolve("Echo").expect("second binding");
        assert!(registry.is_binding_current(&second));

        registry.unregister("Echo");
        assert!(!registry.is_binding_current(&second));
    }
}
