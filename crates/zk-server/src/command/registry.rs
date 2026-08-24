//! 命令注册表（旧 `command/CommandRegistry.java`）。
//!
//! 逐字对照旧实现的四件事：
//! 1. `register`——`name.toLowerCase()` 主索引 + 别名索引，覆盖时 `warn`；
//! 2. `unregister`——按名移除并连带清掉该命令的全部别名；
//! 3. `findCommand`——先主索引再别名索引，空白名直接 miss；
//! 4. `suggestCommands`——Levenshtein ≤ 3 或双向 `contains`，按距离升序取 3。
//!
//! # 并发形态
//!
//! 旧侧是两张 `ConcurrentHashMap`（`commandsByName` / `commandsByAlias`），
//! `register` 对两张表的写入不是原子的；此处收敛为**单把** `RwLock` 保护的
//! [`Index`]——注册/注销对外原子可见，读路径（`find` / `visible`）取共享锁。
//! 可观察行为等价，并发窗口更窄。
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! | 旧成员 | 处置 |
//! |---|---|
//! | `dynamicSources` / `getVisibleCommandsWithDynamic` | 未移植——动态命令源专供 MCP 命令族（后续 Batch），本批无生产者 |
//! | `REMOTE_SAFE_COMMANDS` / `BRIDGE_SAFE_COMMANDS` / `isRemoteSafe` / `isBridgeSafe` | 未移植——WS 通道无「远程/桥接模式」判定点（[`CommandContext`] 的两个布尔位恒 false），移植即死代码 |
//! | `getVisibleCommands` 的 `filter(Command::isUserInvocable)` | 未移植该过滤位——本批 11 个命令无一覆写 `isUserInvocable`（旧默认 `true`），过滤结果完全相同 |
//!
//! 另：旧 `commandsByName.values().stream()` 的迭代序由 `HashMap` 决定（不定），
//! 排序后平票项顺序不稳定；本实现底层为 `BTreeMap`，平票按命令名字典序——
//! 属确定性增强（旧行为的严格化）。

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use super::traits::{Command, CommandType};

/// 名/别名双索引（单锁保护）。
#[derive(Default)]
struct Index {
    /// 旧 `commandsByName`。
    by_name: BTreeMap<String, Arc<dyn Command>>,
    /// 旧 `commandsByAlias`。
    by_alias: BTreeMap<String, Arc<dyn Command>>,
}

/// 命令未找到（旧 `CommandRegistry.CommandNotFoundException`）。
///
/// `message` 即旧异常消息，逐字为
/// `"Unknown command: /" + name + ". " + suggestions`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandNotFound {
    /// 请求的命令名（原样，未小写化）。
    pub command: String,
    /// 完整失败文案（含模糊建议）。
    pub message: String,
}

impl std::fmt::Display for CommandNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CommandNotFound {}

/// 斜杠命令注册表（旧 `CommandRegistry` `@Service` 单例的等价物）。
pub struct CommandRegistry {
    index: RwLock<Index>,
}

impl std::fmt::Debug for CommandRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandRegistry")
            .field("size", &self.len())
            .finish()
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandRegistry {
    /// 空注册表。
    #[must_use]
    pub fn new() -> Self {
        Self {
            index: RwLock::new(Index::default()),
        }
    }

    /// 装配全部内建命令（旧构造器 `CommandRegistry(List<Command>)` 的等价
    /// 物——Spring 自动收集所有 `Command` Bean，此处由
    /// [`super::builtin::register_builtin_commands`] 显式枚举）。
    #[must_use]
    pub fn with_builtin_commands() -> Self {
        let registry = Self::new();
        super::builtin::register_builtin_commands(&registry);
        tracing::info!(
            commands = registry.len(),
            "CommandRegistry initialized: {} commands registered",
            registry.len()
        );
        registry
    }

    /// 旧 `register(Command)`：主索引覆盖时 `warn`，别名同步入表。
    pub fn register(&self, command: Arc<dyn Command>) {
        let name = command.name().to_lowercase();
        let aliases = command.aliases();
        let command_type = command.command_type().as_str();
        let mut index = self.write();
        for alias in aliases {
            index
                .by_alias
                .insert(alias.to_lowercase(), Arc::clone(&command));
        }
        // 主索引最后写入并直接移交 `Arc`（别名先行只影响写入次序，覆盖语义不变）。
        if index.by_name.insert(name.clone(), command).is_some() {
            tracing::warn!(command = %name, "Command overridden");
        }
        tracing::debug!(
            command = %name,
            command_type,
            aliases = ?aliases,
            "Registered command"
        );
    }

    /// 旧 `unregister(String)`：命中时连带清掉该命令的全部别名。
    pub fn unregister(&self, name: &str) {
        let mut index = self.write();
        let Some(removed) = index.by_name.remove(&name.to_lowercase()) else {
            return;
        };
        for alias in removed.aliases() {
            index.by_alias.remove(&alias.to_lowercase());
        }
        tracing::debug!(command = %name, "Unregistered command");
    }

    /// 旧 `findCommand(String)`：空白名直接 miss，先主索引再别名索引。
    #[must_use]
    pub fn find_command(&self, name: &str) -> Option<Arc<dyn Command>> {
        if name.trim().is_empty() {
            return None;
        }
        let lower = name.to_lowercase();
        let index = self.read();
        index
            .by_name
            .get(&lower)
            .or_else(|| index.by_alias.get(&lower))
            .map(Arc::clone)
    }

    /// 旧 `getCommand(String)`：未找到时携模糊建议失败。
    ///
    /// # Errors
    /// [`CommandNotFound`]——`message` 与旧异常消息逐字一致。
    pub fn get_command(&self, name: &str) -> Result<Arc<dyn Command>, CommandNotFound> {
        self.find_command(name).ok_or_else(|| CommandNotFound {
            command: name.to_owned(),
            message: format!(
                "Unknown command: /{name}. {suggestions}",
                suggestions = self.suggest_commands(name)
            ),
        })
    }

    /// 旧 `suggestCommands(String)`：距离 ≤ 3 或双向包含，按距离升序取前 3。
    #[must_use]
    pub fn suggest_commands(&self, input: &str) -> String {
        const FALLBACK: &str = "Type /help for available commands.";
        if input.trim().is_empty() {
            return FALLBACK.to_owned();
        }
        let lower = input.to_lowercase();
        let mut candidates: Vec<(usize, String)> = self
            .read()
            .by_name
            .keys()
            .filter(|name| {
                levenshtein_distance(&lower, name) <= 3
                    || name.contains(&lower)
                    || lower.contains(name.as_str())
            })
            .map(|name| (levenshtein_distance(&lower, name), name.clone()))
            .collect();
        // 旧 `sorted(Comparator.comparingInt(...))` 是稳定排序；底层 `BTreeMap`
        // 已按名字典序产出，故平票项顺序确定（见模块文档）。
        candidates.sort_by_key(|(distance, _)| *distance);
        candidates.truncate(3);
        if candidates.is_empty() {
            return FALLBACK.to_owned();
        }
        let joined = candidates
            .iter()
            .map(|(_, name)| format!("/{name}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("Did you mean: {joined}?")
    }

    /// 旧 `getVisibleCommands()`：排除隐藏命令，按名升序。
    #[must_use]
    pub fn visible_commands(&self) -> Vec<Arc<dyn Command>> {
        self.read()
            .by_name
            .values()
            .filter(|command| !command.is_hidden())
            .map(Arc::clone)
            .collect()
    }

    /// 旧 `getCommandsByType(CommandType)`：按名升序。
    #[must_use]
    pub fn commands_by_type(&self, command_type: CommandType) -> Vec<Arc<dyn Command>> {
        self.read()
            .by_name
            .values()
            .filter(|command| command.command_type() == command_type)
            .map(Arc::clone)
            .collect()
    }

    /// 旧 `size()`。
    #[must_use]
    pub fn len(&self) -> usize {
        self.read().by_name.len()
    }

    /// 注册表是否为空（clippy `len_without_is_empty`）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read().by_name.is_empty()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Index> {
        self.index
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Index> {
        self.index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Levenshtein 距离（旧 `CommandRegistry.levenshteinDistance` 二维 DP 逐字移植）。
///
/// 旧实现按 `charAt`（UTF-16 码元）比较；此处按 `char`（Unicode 标量）比较——
/// 命令名域为 ASCII 小写字母与 `-`，两者结果相同。滚动两行替代整张 `int[][]`：
/// DP 递推只依赖上一行，结果逐位一致。
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let b_chars: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b_chars.len()).collect();
    let mut current = vec![0_usize; b_chars.len() + 1];
    for (i, a_char) in a.chars().enumerate() {
        current[0] = i + 1;
        for (j, &b_char) in b_chars.iter().enumerate() {
            let cost = usize::from(a_char != b_char);
            current[j + 1] = (previous[j + 1] + 1)
                .min(current[j] + 1)
                .min(previous[j] + cost);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b_chars.len()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::future::BoxFuture;

    use super::{CommandRegistry, levenshtein_distance};
    use crate::command::context::CommandContext;
    use crate::command::traits::{Command, CommandResult, CommandType};

    struct Stub {
        name: &'static str,
        aliases: &'static [&'static str],
        command_type: CommandType,
        hidden: bool,
    }

    impl Command for Stub {
        fn name(&self) -> &'static str {
            self.name
        }
        fn aliases(&self) -> &'static [&'static str] {
            self.aliases
        }
        fn description(&self) -> &'static str {
            "stub"
        }
        fn command_type(&self) -> CommandType {
            self.command_type
        }
        fn execute<'a>(
            &'a self,
            _args: &'a str,
            _ctx: &'a CommandContext,
        ) -> BoxFuture<'a, CommandResult> {
            Box::pin(async { CommandResult::text("stub") })
        }
        fn is_hidden(&self) -> bool {
            self.hidden
        }
    }

    fn stub(
        name: &'static str,
        aliases: &'static [&'static str],
        command_type: CommandType,
        hidden: bool,
    ) -> Arc<dyn Command> {
        Arc::new(Stub {
            name,
            aliases,
            command_type,
            hidden,
        })
    }

    /// 主索引 + 别名索引双命中，大小写无关（旧 `toLowerCase` 双端）。
    #[test]
    fn find_command_resolves_names_and_aliases_case_insensitively() {
        let registry = CommandRegistry::new();
        registry.register(stub("clear", &["reset", "new"], CommandType::Local, false));
        assert_eq!(registry.len(), 1);
        for probe in ["clear", "CLEAR", "reset", "New"] {
            assert!(
                registry.find_command(probe).is_some(),
                "must resolve {probe}"
            );
        }
        assert!(registry.find_command("nope").is_none());
        // 旧 `findCommand` 对空白名直接 miss（不进索引）。
        assert!(registry.find_command("").is_none());
        assert!(registry.find_command("   ").is_none());
    }

    /// 注销连带清别名（旧 `unregister` 的 `removed.getAliases()` 循环）。
    #[test]
    fn unregister_drops_aliases_too() {
        let registry = CommandRegistry::new();
        registry.register(stub("clear", &["reset"], CommandType::Local, false));
        registry.unregister("CLEAR");
        assert!(registry.is_empty());
        assert!(registry.find_command("reset").is_none());
        // 未注册名注销为无操作（旧 removed==null 分支）。
        registry.unregister("ghost");
    }

    /// 可见清单排除 hidden 且按名升序；按类型筛选同样按名升序。
    #[test]
    fn visible_and_typed_listings_are_sorted_by_name() {
        let registry = CommandRegistry::new();
        registry.register(stub("status", &[], CommandType::Local, false));
        registry.register(stub("clear", &[], CommandType::Local, false));
        registry.register(stub("model", &[], CommandType::LocalJsx, false));
        registry.register(stub("secret", &[], CommandType::Local, true));

        let visible: Vec<&str> = registry
            .visible_commands()
            .iter()
            .map(|command| command.name())
            .collect();
        assert_eq!(visible, ["clear", "model", "status"]);

        let local: Vec<&str> = registry
            .commands_by_type(CommandType::Local)
            .iter()
            .map(|command| command.name())
            .collect();
        assert_eq!(local, ["clear", "secret", "status"], "按类型不过滤 hidden");
    }

    /// 旧 `getCommand` 未找到时的消息逐字（含建议句）。
    #[test]
    fn get_command_error_message_matches_the_legacy_text() {
        let registry = CommandRegistry::new();
        registry.register(stub("clear", &[], CommandType::Local, false));
        let Err(err) = registry.get_command("claer") else {
            panic!("未注册命令必须失败");
        };
        assert_eq!(err.command, "claer");
        assert_eq!(
            err.message,
            "Unknown command: /claer. Did you mean: /clear?"
        );
    }

    /// 建议规则：距离 ≤ 3 / 双向包含命中，最多 3 条，按距离升序。
    #[test]
    fn suggestions_follow_the_legacy_filter_and_ordering() {
        let registry = CommandRegistry::new();
        for name in ["compact", "config", "cost", "clear", "doctor"] {
            registry.register(stub(name, &[], CommandType::Local, false));
        }
        // "cost" 距离 1 入选；"config"(4) / "compact"(5) / "clear"(4) 既超距离
        // 阈值又不满足双向包含 → 全不入选。
        assert_eq!(registry.suggest_commands("cos"), "Did you mean: /cost?");
        // 前缀 "co" 靠 `name.contains(lower)` 命中三条，按距离升序（cost 2 <
        // config 4 < compact 5）取满 3 条。
        assert_eq!(
            registry.suggest_commands("co"),
            "Did you mean: /cost, /config, /compact?"
        );
        // 无任何候选 → 旧兜底句。
        assert_eq!(
            registry.suggest_commands("zzzzzzzzzz"),
            "Type /help for available commands."
        );
        // 空白输入 → 同一兜底句（旧 `isBlank()` 分支）。
        assert_eq!(
            registry.suggest_commands("  "),
            "Type /help for available commands."
        );
    }

    /// DP 结果与旧 `levenshteinDistance` 逐例一致。
    #[test]
    fn levenshtein_matches_the_legacy_dp() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("clear", "clear"), 0);
        assert_eq!(levenshtein_distance("", "clear"), 5);
        assert_eq!(levenshtein_distance("clear", ""), 5);
        assert_eq!(levenshtein_distance("claer", "clear"), 2);
        assert_eq!(levenshtein_distance("cos", "cost"), 1);
        assert_eq!(levenshtein_distance("cos", "config"), 4);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
    }
}
