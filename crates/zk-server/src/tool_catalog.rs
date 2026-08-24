//! 工具目录元数据与会话级开关状态（Batch 1 Step 1-5，旧 `ToolController` 的
//! 两个注入依赖之一 + `Tool` 接口的展示面元数据）。
//!
//! # 为何需要本模块
//!
//! 旧 `ToolController` 的响应字段来自三处：
//!
//! | 字段 | 旧来源 | Rust 来源 |
//! |---|---|---|
//! | `name` / `description` / `inputSchema` | `Tool.getName/getDescription/getInputSchema` | zk-tools `Tool::name/description/parameters` |
//! | `category` | `Tool.getGroup()`（默认 `"general"`，旧 `Tool.java:46`） | [`group_of`]（本模块表） |
//! | `permissionLevel` | `Tool.getPermissionRequirement().name()`（默认 `NONE`，旧 `Tool.java:62`） | [`permission_level_of`]（本模块表） |
//! | `enabled` | `Tool.isEnabled()` × [`ToolSessionState`] 覆盖 | 注册即启用 × [`ToolSessionState`] 覆盖 |
//!
//! zk-tools 的 `Tool` trait 只承载「执行面」四元组（name / description /
//! parameters / execute）与授权事实三件（`is_destructive` / `is_read_only` /
//! `path_of`），**不含** `group` / `permission_requirement` 这两个纯展示面
//! 字段。它们在 Rust 侧的权威消费者只有本 REST 目录端点（引擎与授权链走
//! zk-authz 的风险分级，不读这两个值），故以 zk-server 侧的**薄壳表**承载，
//! 不为一个展示字段反向修改下游 crate 的 trait 契约。
//!
//! # 表的权威源（旧仓库只读逐字对照）
//!
//! 每行注明旧文件与行号；未登记者按旧 `Tool` 接口默认值回落
//! （`"general"` / `"NONE"`）。
//!
//! # 会话级开关
//!
//! [`ToolSessionState`] 逐字对照 `tool/ToolSessionState.java`：
//! `sessionId → (toolName → enabled)` 双层映射，会话级值**优先于**工具全局
//! 启用位。旧实现的 `clearSession` 在旧仓库全局 grep **无调用方**（仅
//! `ToolController` 用 get/set），本移植同样只提供能力、不接线到会话删除。

use std::collections::HashMap;
use std::sync::Mutex;

/// 工具展示面元数据表——`(工具名, category, permissionLevel)`。
///
/// 逐字对照旧仓库（`backend/src/main/java/com/aicodeassistant/tool/`）：
///
/// - `Read` → `impl/FileReadTool.java:110` `getGroup()="read"`；无
///   `getPermissionRequirement` 覆写 → `Tool.java:62` 默认 `NONE`；
/// - `Write` → `impl/FileWriteTool.java:91` `"edit"` / `:96` `ALWAYS_ASK`；
/// - `Edit` → `impl/FileEditTool.java:112` `"edit"` / `:117` `ALWAYS_ASK`；
/// - `Glob` → `impl/GlobTool.java:75` `"read"` / 默认 `NONE`；
/// - `Grep` → `impl/GrepTool.java:150` `"read"` / 默认 `NONE`；
/// - `Bash` → `impl/BashTool.java:259` `"bash"` / `:264` `CONDITIONAL`；
/// - `Git` → `impl/GitTool.java:111` `"read"` / `:126` `NONE`；
/// - `CodeIntel` → `impl/CodeIntelTool.java:30` `"code_intelligence"` /
///   `:32` `NONE`；
/// - `WebBrowser` → `impl/WebBrowserTool.java` 无 `getGroup` 覆写 → 默认
///   `"general"`；`:176` `ALWAYS_ASK`。
/// - `TodoWrite` → `interaction/TodoWriteTool.java:117` `"interaction"` /
///   `:122` `NONE`；
/// - `AskUserQuestion` → `interaction/AskUserQuestionTool.java:103`
///   `"interaction"` / `:108` `NONE`；
/// - `Config` → `config/ConfigTool.java:128` `"config"` / `:138` `NONE`；
/// - `SyntheticOutput` → `config/SyntheticOutputTool.java:77` `"config"` /
///   `:82` `NONE`；
/// - `Memory` → `memdir/MemoryTool.java:114` `"general"` / `:109` `NONE`
///   （两者均与接口默认值同值，仍显式登记以留下旧源锚点）。
///
/// `ListDir` / `GitDiff` / `GitLog` / `GitStatus` 是 Rust 侧原生只读工具，
/// 旧仓库无同名 bean（旧 git 能力集中在单个 `Git` 工具，目录列举归 `Bash`），
/// 故按旧仓库同族只读工具的取值归类为 `"read"` / `NONE`——与
/// `FileReadTool` / `GitTool` 完全一致。
const TOOL_METADATA: &[(&str, &str, &str)] = &[
    ("AskUserQuestion", "interaction", "NONE"),
    ("Bash", "bash", "CONDITIONAL"),
    ("CodeIntel", "code_intelligence", "NONE"),
    ("Config", "config", "NONE"),
    ("Edit", "edit", "ALWAYS_ASK"),
    ("Git", "read", "NONE"),
    ("GitDiff", "read", "NONE"),
    ("GitLog", "read", "NONE"),
    ("GitStatus", "read", "NONE"),
    ("Glob", "read", "NONE"),
    ("Grep", "read", "NONE"),
    ("ListDir", "read", "NONE"),
    ("Memory", "general", "NONE"),
    ("Read", "read", "NONE"),
    ("SyntheticOutput", "config", "NONE"),
    ("TodoWrite", "interaction", "NONE"),
    ("WebBrowser", "general", "ALWAYS_ASK"),
    ("Write", "edit", "ALWAYS_ASK"),
];

/// 旧 `Tool.getGroup()` 的默认返回值（`Tool.java:46`）。
const DEFAULT_GROUP: &str = "general";

/// 旧 `Tool.getPermissionRequirement()` 的默认返回值枚举名
/// （`Tool.java:62` → `PermissionRequirement.NONE`）。
const DEFAULT_PERMISSION_LEVEL: &str = "NONE";

/// 工具分组（旧 `Tool.getGroup()`；表内未登记者回落 `"general"`——与旧接口
/// 默认实现同义，MCP 动态工具在旧端亦走此默认）。
#[must_use]
pub fn group_of(tool_name: &str) -> &'static str {
    TOOL_METADATA
        .iter()
        .find(|(name, _, _)| *name == tool_name)
        .map_or(DEFAULT_GROUP, |(_, group, _)| *group)
}

/// 权限需求枚举名（旧 `Tool.getPermissionRequirement().name()`；表内未登记者
/// 回落 `"NONE"`——与旧接口默认实现同义）。
#[must_use]
pub fn permission_level_of(tool_name: &str) -> &'static str {
    TOOL_METADATA
        .iter()
        .find(|(name, _, _)| *name == tool_name)
        .map_or(DEFAULT_PERMISSION_LEVEL, |(_, _, level)| *level)
}

/// 工具会话级启用状态覆盖表——逐字对照 `tool/ToolSessionState.java`。
///
/// 旧实现为 `ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>>`；
/// 此处以单把 `Mutex` 守护双层 `HashMap`：临界区只做映射读写（无 await、
/// 无 IO），争用可忽略，且不引入新依赖。
#[derive(Debug, Default)]
pub struct ToolSessionState {
    /// `sessionId → (toolName → enabled)`。
    state: Mutex<HashMap<String, HashMap<String, bool>>>,
}

impl ToolSessionState {
    /// 构造空状态表（旧 Spring `@Component` 单例的等价物，装配在
    /// [`crate::state::AppState`]）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 读取会话中某工具的覆盖状态（未设置 → `None`，旧 `getToolState`
    /// 返回 `null` 的等价物）。
    ///
    /// # Panics
    ///
    /// 仅当持锁线程 panic 导致锁中毒时——临界区无 panic 路径，实际不可达。
    #[must_use]
    pub fn tool_state(&self, session_id: &str, tool_name: &str) -> Option<bool> {
        self.state
            .lock()
            .expect("tool session state mutex is never poisoned")
            .get(session_id)
            .and_then(|tools| tools.get(tool_name))
            .copied()
    }

    /// 设置会话中某工具的覆盖状态（旧 `setToolState` 的
    /// `computeIfAbsent(...).put(...)`）。
    ///
    /// # Panics
    ///
    /// 同 [`Self::tool_state`]（锁中毒，实际不可达）。
    pub fn set_tool_state(&self, session_id: &str, tool_name: &str, enabled: bool) {
        self.state
            .lock()
            .expect("tool session state mutex is never poisoned")
            .entry(session_id.to_owned())
            .or_default()
            .insert(tool_name.to_owned(), enabled);
    }

    /// 清除指定会话的全部覆盖（旧 `clearSession`；旧仓库无调用方，本移植
    /// 同样保留能力而不接线）。
    ///
    /// # Panics
    ///
    /// 同 [`Self::tool_state`]（锁中毒，实际不可达）。
    pub fn clear_session(&self, session_id: &str) {
        self.state
            .lock()
            .expect("tool session state mutex is never poisoned")
            .remove(session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{ToolSessionState, group_of, permission_level_of};

    /// 表内取值逐条对照旧仓库（写工具 `edit`/`ALWAYS_ASK`、Bash
    /// `bash`/`CONDITIONAL`、只读族 `read`/`NONE`）。
    #[test]
    fn metadata_matches_java_tool_declarations() {
        assert_eq!(group_of("Read"), "read");
        assert_eq!(permission_level_of("Read"), "NONE");
        assert_eq!(group_of("Write"), "edit");
        assert_eq!(permission_level_of("Write"), "ALWAYS_ASK");
        assert_eq!(group_of("Bash"), "bash");
        assert_eq!(permission_level_of("Bash"), "CONDITIONAL");
        assert_eq!(group_of("CodeIntel"), "code_intelligence");
        assert_eq!(permission_level_of("CodeIntel"), "NONE");
        assert_eq!(group_of("Memory"), "general");
        assert_eq!(permission_level_of("Memory"), "NONE");
        assert_eq!(group_of("WebBrowser"), "general");
        assert_eq!(permission_level_of("WebBrowser"), "ALWAYS_ASK");
        assert_eq!(group_of("Git"), "read");
        assert_eq!(permission_level_of("Git"), "NONE");
    }

    /// 未登记工具走旧接口默认值（`general` / `NONE`）。
    #[test]
    fn unknown_tool_falls_back_to_interface_defaults() {
        assert_eq!(group_of("mcp__server__thing"), "general");
        assert_eq!(permission_level_of("mcp__server__thing"), "NONE");
    }

    /// 会话级覆盖按 `(sessionId, toolName)` 隔离，`clearSession` 只清本会话。
    #[test]
    fn session_overrides_are_scoped_per_session_and_tool() {
        let state = ToolSessionState::new();
        assert_eq!(state.tool_state("s1", "Bash"), None);

        state.set_tool_state("s1", "Bash", false);
        state.set_tool_state("s2", "Bash", true);
        assert_eq!(state.tool_state("s1", "Bash"), Some(false));
        assert_eq!(state.tool_state("s2", "Bash"), Some(true));
        assert_eq!(state.tool_state("s1", "Read"), None);

        // 覆盖同键以后写为准（旧 `put` 语义）。
        state.set_tool_state("s1", "Bash", true);
        assert_eq!(state.tool_state("s1", "Bash"), Some(true));

        state.clear_session("s1");
        assert_eq!(state.tool_state("s1", "Bash"), None);
        assert_eq!(state.tool_state("s2", "Bash"), Some(true));
    }
}
