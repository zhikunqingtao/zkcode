//! 特性标志——应用级特性开关的唯一事实源（旧 `config/FeatureFlagService.java`）。
//!
//! # 优先级链（逐条对齐旧 `getFeatureValue`）
//!
//! 1. **环境变量覆盖**——旧实现为 `FEATURE_<KEY>`（`"FEATURE_" + key.toUpperCase()`）。
//!    本实现在其之上再认 zkcode 原生前缀 `ZK_FEATURE_<KEY>`（优先级更高）：该前缀
//!    在 zk-server 侧已是既有约定（`ZK_FEATURE_WEB_BROWSER_TOOL` /
//!    `ZK_FEATURE_GIT_ENHANCED_TOOL`），两者并存既保住本仓既有文档与部署脚本，也
//!    补齐旧仓的 `FEATURE_` 覆盖能力，且是纯增量（任一前缀单独存在时结果不变）。
//! 2. **裸名环境变量**——仅 [`PLACEHOLDER_ENV_FLAGS`] 三项。旧 `application.yml` 把
//!    它们写成 `${GIT_ENHANCED_TOOL:true}` 形态，Spring 在占位符解析期读同名裸环境
//!    变量，故必须复刻（旧仓 SWE-bench 评测就靠裸 `SELF_CORRECTION_LOOP` 开自纠错）。
//! 3. **出厂默认值**——见 [`factory_default`]，逐字抄自旧
//!    `backend/src/main/resources/application.yml` 的 `features.flags` 节（L144-173）。
//!    旧实现第 2 层是 YAML 绑定表、第 3 层是调用方传入的默认值；本实现把 YAML 层
//!    固化为出厂表（Rust 侧无 YAML 装配），运行时可经 [`FeatureFlags::set_value`]
//!    改写（旧 `setFeatureValue`，`/config` 域端点的数据入口）。
//!
//! # 与旧实现的差异（有意为之，均已核对）
//!
//! - 旧 `getFeatureValue` 有一段「期望 `Boolean` 实得 `String`」的强转兜底，那是
//!   Spring 把 `${...}` 占位符一律绑成 `String` 的产物；本实现出厂表是强类型的
//!   （占位符三项直接是 [`FlagValue::Bool`]），该兜底路径不存在。
//! - 旧实现的默认值由调用方逐点传入（`isEnabled` 传 `false`、
//!   `getFeatureValue("FRC_KEEP_RECENT", 3)` 传 `3`）。本实现默认值来自出厂表，两者
//!   在旧仓全部调用点上取值一致（已逐点核对：`FRC_KEEP_RECENT` 调用方默认 3 = YAML
//!   3；`FRC_SUPPORTED_MODELS` 调用方默认 `"light,standard"` = YAML 值）；未登记的
//!   flag（旧仓有 7 个只在代码里读、YAML 未声明，见下）取类型零值，即 `false` /
//!   `0` / 空表，与旧仓这些调用点的 `false` 默认同义。
//! - 类型不匹配（如以 [`FeatureFlags::is_enabled`] 读整型 flag）返回类型零值；旧
//!   实现该路径抛 `ClassCastException`（调用方错误），不复刻异常。
//! - 环境变量值**不做 trim**：旧实现直接 `Boolean.parseBoolean` / `Integer.parseInt`
//!   原值，`" true"` 在旧仓即为 `false`，此处保持一致。
//!
//! # YAML 未声明、仅代码内读取的 flag（旧仓现状，效值恒 `false`）
//!
//! `EMBEDDED_SEARCH_TOOLS`、`FORK_SUBAGENT`、`INTERNAL_USER_MODE`、
//! `NUMERIC_LENGTH_ANCHORS`、`PROMPT_CACHE_GLOBAL_SCOPE`、`REPL_MODE`、
//! `SKILL_DISCOVERY`。它们**刻意不入出厂表**——旧 `getAllFlags` 只暴露 YAML 绑定
//! 表，登记进来会让 [`FeatureFlags::all_flags`] 比旧实现多吐 7 条。仍可经环境变量
//! 打开（未登记名走 `ZK_FEATURE_` / `FEATURE_` 两层，与旧实现同）。

use std::collections::BTreeMap;
use std::sync::{PoisonError, RwLock};

/// flag 名——`THINKING_MODE`（旧 `application.yml` L144）。
pub const THINKING_MODE: &str = "THINKING_MODE";
/// flag 名——`TOOL_SEARCH`（旧 `application.yml` L145）。
pub const TOOL_SEARCH: &str = "TOOL_SEARCH";
/// flag 名——`CLASSIFIER_V2`（旧 `application.yml` L147）。
pub const CLASSIFIER_V2: &str = "CLASSIFIER_V2";
/// flag 名——`ENABLE_AGENT_SWARMS`（旧 `application.yml` L148）。
pub const ENABLE_AGENT_SWARMS: &str = "ENABLE_AGENT_SWARMS";
/// flag 名——`MCP_SKILLS`（旧 `application.yml` L149）。
pub const MCP_SKILLS: &str = "MCP_SKILLS";
/// flag 名——`COORDINATOR_MODE`（旧 `application.yml` L150）。
pub const COORDINATOR_MODE: &str = "COORDINATOR_MODE";
/// flag 名——`TOKEN_BUDGET`（旧 `application.yml` L152，A1 段）。
pub const TOKEN_BUDGET: &str = "TOKEN_BUDGET";
/// flag 名——`SCRATCHPAD`（旧 `application.yml` L153，A3 段）。
pub const SCRATCHPAD: &str = "SCRATCHPAD";
/// flag 名——`CACHED_MICROCOMPACT`（旧 `application.yml` L154，A4 FRC 总开关）。
pub const CACHED_MICROCOMPACT: &str = "CACHED_MICROCOMPACT";
/// flag 名——`FRC_SUPPORTED_MODELS`（旧 `application.yml` L155）。
pub const FRC_SUPPORTED_MODELS: &str = "FRC_SUPPORTED_MODELS";
/// flag 名——`FRC_KEEP_RECENT`（旧 `application.yml` L156）。
pub const FRC_KEEP_RECENT: &str = "FRC_KEEP_RECENT";
/// flag 名——`AGENT_TRIGGERS`（旧 `application.yml` L158，`Cron*` 工具门控）。
pub const AGENT_TRIGGERS: &str = "AGENT_TRIGGERS";
/// flag 名——`WEB_BROWSER_TOOL`（旧 `application.yml` L159）。
pub const WEB_BROWSER_TOOL: &str = "WEB_BROWSER_TOOL";
/// flag 名——`RUNTIME_VERIFICATION`（旧 `application.yml` L160）。
pub const RUNTIME_VERIFICATION: &str = "RUNTIME_VERIFICATION";
/// flag 名——`GIT_ENHANCED_TOOL`（旧 `application.yml` L161，占位符形态）。
pub const GIT_ENHANCED_TOOL: &str = "GIT_ENHANCED_TOOL";
/// flag 名——`RESOURCE_MONITOR`（旧 `application.yml` L162，占位符形态）。
pub const RESOURCE_MONITOR: &str = "RESOURCE_MONITOR";
/// flag 名——`BACKGROUND_AGENT_WAIT`（旧 `application.yml` L164）。
pub const BACKGROUND_AGENT_WAIT: &str = "BACKGROUND_AGENT_WAIT";
/// flag 名——`AGENT_ABORT_CASCADE`（旧 `application.yml` L165）。
pub const AGENT_ABORT_CASCADE: &str = "AGENT_ABORT_CASCADE";
/// flag 名——`SELF_CORRECTION_LOOP`（旧 `application.yml` L167，占位符形态）。
pub const SELF_CORRECTION_LOOP: &str = "SELF_CORRECTION_LOOP";
/// flag 名——`PRECISE_TOKENIZER`（旧 `application.yml` L169）。
pub const PRECISE_TOKENIZER: &str = "PRECISE_TOKENIZER";
/// flag 名——`GIT_DIFF_TRACKER`（旧 `application.yml` L171）。
pub const GIT_DIFF_TRACKER: &str = "GIT_DIFF_TRACKER";
/// flag 名——`SEARCH_STRATEGY_ROUTER`（旧 `application.yml` L173）。
pub const SEARCH_STRATEGY_ROUTER: &str = "SEARCH_STRATEGY_ROUTER";

/// zkcode 原生环境变量前缀（本仓既有约定，优先级最高）。
pub const ENV_PREFIX_NATIVE: &str = "ZK_FEATURE_";

/// 旧实现环境变量前缀（`"FEATURE_" + key.toUpperCase()`）。
pub const ENV_PREFIX_LEGACY: &str = "FEATURE_";

/// 以裸名环境变量兜底的 flag——旧 `application.yml` 把这三项写成
/// `${<NAME>:<default>}`，Spring 在占位符解析期读同名裸环境变量。
pub const PLACEHOLDER_ENV_FLAGS: &[&str] =
    &[GIT_ENHANCED_TOOL, RESOURCE_MONITOR, SELF_CORRECTION_LOOP];

/// flag 值——旧 `features.flags` 节出现过的三种 YAML 标量形态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlagValue {
    /// 布尔开关（22 个出厂 flag 中的 20 个）。
    Bool(bool),
    /// 整型阈值（旧 YAML 为 `Integer`，此处统一 `i64`）。
    Int(i64),
    /// 逗号分隔串的展开形态。
    ///
    /// 旧 YAML 存单个字符串（`"light,standard"`），消费点 `SystemPromptBuilder`
    /// 才 `split(",")` + `trim`；此处在装配期就切好，消费语义等价。
    StringList(Vec<String>),
}

/// 出厂默认值的 const 形态。
///
/// [`FlagValue::StringList`] 持 `Vec` 故 `FlagValue` 不能进常量表，这里用
/// `&'static [&'static str]` 表达列表、经 `FlagDefault::to_value` 转换。
#[derive(Clone, Copy, Debug)]
enum FlagDefault {
    /// 见 [`FlagValue::Bool`]。
    Bool(bool),
    /// 见 [`FlagValue::Int`]。
    Int(i64),
    /// 见 [`FlagValue::StringList`]。
    StringList(&'static [&'static str]),
}

impl FlagDefault {
    /// 转为运行时值。
    fn to_value(self) -> FlagValue {
        match self {
            Self::Bool(value) => FlagValue::Bool(value),
            Self::Int(value) => FlagValue::Int(value),
            Self::StringList(items) => {
                FlagValue::StringList(items.iter().map(|item| (*item).to_owned()).collect())
            }
        }
    }
}

/// 22 个受支持 flag 的出厂默认值。旧端的 `SANDBOX_DEFAULT_ON` 被有意排除：
/// macOS 本地 Beta 不提供或声明操作系统沙箱能力。其余值对齐旧
/// `backend/src/main/resources/application.yml` 的 `features.flags` 节（L144-173），
/// 顺序与分组注释亦与 YAML 一致，便于逐行对照。
///
/// 注意：旧 `FeatureFlagService` 类注释里那张 5 行表格已过期（它写 `THINKING_MODE`
/// 默认 `false`，而 YAML 是 `true`）。生效值由 YAML 决定，故本表以 YAML 为准。
const FACTORY_DEFAULTS: &[(&str, FlagDefault)] = &[
    (THINKING_MODE, FlagDefault::Bool(true)),
    (TOOL_SEARCH, FlagDefault::Bool(false)),
    (CLASSIFIER_V2, FlagDefault::Bool(false)),
    (ENABLE_AGENT_SWARMS, FlagDefault::Bool(true)),
    (MCP_SKILLS, FlagDefault::Bool(false)),
    (COORDINATOR_MODE, FlagDefault::Bool(true)),
    // ── A 组：SystemPrompt 动态段 ──
    (TOKEN_BUDGET, FlagDefault::Bool(false)),
    (SCRATCHPAD, FlagDefault::Bool(true)),
    (CACHED_MICROCOMPACT, FlagDefault::Bool(true)),
    (
        FRC_SUPPORTED_MODELS,
        FlagDefault::StringList(&["light", "standard"]),
    ),
    (FRC_KEEP_RECENT, FlagDefault::Int(3)),
    // ── B 组：P2 实验性工具 ──
    (AGENT_TRIGGERS, FlagDefault::Bool(false)),
    (WEB_BROWSER_TOOL, FlagDefault::Bool(true)),
    (RUNTIME_VERIFICATION, FlagDefault::Bool(true)),
    (GIT_ENHANCED_TOOL, FlagDefault::Bool(true)),
    (RESOURCE_MONITOR, FlagDefault::Bool(false)),
    // ── C 组：后台代理生命周期 ──
    (BACKGROUND_AGENT_WAIT, FlagDefault::Bool(false)),
    (AGENT_ABORT_CASCADE, FlagDefault::Bool(true)),
    // ── D 组：自纠错循环 ──
    (SELF_CORRECTION_LOOP, FlagDefault::Bool(false)),
    // ── E 组：精确 Tokenizer ──
    (PRECISE_TOKENIZER, FlagDefault::Bool(false)),
    // ── F 组：变更追踪 / 搜索策略 ──
    (GIT_DIFF_TRACKER, FlagDefault::Bool(false)),
    (SEARCH_STRATEGY_ROUTER, FlagDefault::Bool(false)),
];

/// 环境变量层的取值来源。
///
/// 进程环境在 Rust 2024 下不可安全改写（`set_var` 是 `unsafe`，workspace
/// `unsafe_code = "forbid"`），故覆盖语义的可测性靠这层抽象：生产走
/// `EnvLayer::Process`，测试走 `EnvLayer::Fixed`。
#[derive(Clone, Debug)]
enum EnvLayer {
    /// 真实进程环境。
    Process,
    /// 固定表（测试注入；空表 = 无任何覆盖）。
    Fixed(BTreeMap<String, String>),
}

impl EnvLayer {
    /// 取环境变量原值。
    ///
    /// 与旧 `System.getenv` 同义：**存在即覆盖**，空串也算存在（旧实现判
    /// `!= null`，空串会被 `Boolean.parseBoolean` 解成 `false`）。
    fn get(&self, key: &str) -> Option<String> {
        match self {
            Self::Process => std::env::var(key).ok(),
            Self::Fixed(map) => map.get(key).cloned(),
        }
    }
}

/// 特性标志表（旧 `FeatureFlagService`）。
///
/// 读多写少：`RwLock` + 锁内克隆出参，锁作用域恒为常数级；poison 一律
/// `into_inner` 恢复（flag 表是可重建的配置状态，无需以 panic 传播）。
/// 以 `Arc<FeatureFlags>` 跨层共享——旧实现是单例 Spring Bean，全进程一张表，
/// [`FeatureFlags::set_value`] 的运行时改写对所有持有者立即可见。
#[derive(Debug)]
pub struct FeatureFlags {
    /// 生效 flag 表（出厂默认 + 运行时改写；旧 `flags` `ConcurrentHashMap`）。
    flags: RwLock<BTreeMap<String, FlagValue>>,
    /// 环境变量覆盖层（优先级高于 `flags`，旧实现亦然）。
    env: EnvLayer,
}

impl FeatureFlags {
    /// 出厂默认值 + 进程环境变量覆盖（生产装配入口）。
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            flags: RwLock::new(factory_table()),
            env: EnvLayer::Process,
        }
    }

    /// 仅出厂默认值，不读环境变量（确定性装配：测试与集成测试用）。
    #[must_use]
    pub fn with_defaults() -> Self {
        Self {
            flags: RwLock::new(factory_table()),
            env: EnvLayer::Fixed(BTreeMap::new()),
        }
    }

    /// 出厂默认值 + 指定环境变量表（覆盖语义的可测入口）。
    ///
    /// `#[doc(hidden)]`：仅供测试注入，生产代码用 [`FeatureFlags::from_env`]。
    /// 之所以需要注入而非直接改写进程环境——`std::env::set_var` 在 Rust 2024 是
    /// `unsafe`，而 workspace 声明了 `unsafe_code = "forbid"`。
    #[doc(hidden)]
    #[must_use]
    pub fn with_env_overrides<I, K, V>(overrides: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            flags: RwLock::new(factory_table()),
            env: EnvLayer::Fixed(
                overrides
                    .into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
        }
    }

    /// 特性是否启用（旧 `isEnabled`：默认值 `false` 的布尔特化）。
    ///
    /// 环境变量覆盖按旧 `Boolean.parseBoolean` 语义解析——仅 `true`（忽略大小写）
    /// 为真，其余一切（空串 / `1` / `yes` / 乱值）为假，且**不回落**到下层。
    #[must_use]
    pub fn is_enabled(&self, name: &str) -> bool {
        if let Some(raw) = self.env_raw(name) {
            return raw.eq_ignore_ascii_case("true");
        }
        match self.lookup(name) {
            Some(FlagValue::Bool(value)) => value,
            _ => false,
        }
    }

    /// 门控检查（旧 `checkGate`）。
    ///
    /// 与 [`FeatureFlags::is_enabled`] 同语义——旧仓两个方法并存且实现一致，此处
    /// 保留同名入口，使迁移调用点可逐字对照而不必判断该用哪个。
    #[must_use]
    pub fn check_gate(&self, name: &str) -> bool {
        self.is_enabled(name)
    }

    /// 取整型 flag（旧 `getFeatureValue(key, <int>)`）。
    ///
    /// 环境变量解析失败时按旧 `convertEnvValue` 的 `NumberFormatException` 分支
    /// 回落出厂默认值；未登记的 flag 回落 `0`。
    #[must_use]
    pub fn get_int(&self, name: &str) -> i64 {
        if let Some(raw) = self.env_raw(name) {
            if let Ok(parsed) = raw.parse::<i64>() {
                return parsed;
            }
            return match factory_default(name) {
                Some(FlagValue::Int(value)) => value,
                _ => 0,
            };
        }
        match self.lookup(name) {
            Some(FlagValue::Int(value)) => value,
            _ => 0,
        }
    }

    /// 取字符串列表 flag（旧存单串、消费点 `split(",")` + `trim`）。
    ///
    /// 环境变量覆盖同样逗号切分 + 逐段 `trim`，含「空串切出单个空段」这一退化形态
    /// （旧仓 `"".split(",")` 亦得 `[""]`，语义保持一致）。
    #[must_use]
    pub fn get_string_list(&self, name: &str) -> Vec<String> {
        if let Some(raw) = self.env_raw(name) {
            return split_list(&raw);
        }
        match self.lookup(name) {
            Some(FlagValue::StringList(items)) => items,
            _ => Vec::new(),
        }
    }

    /// 取 flag 表中的生效值（**不含**环境变量层）。
    ///
    /// 环境变量原值需要类型上下文才能解释，故本方法只看 flag 表；要吃到环境变量
    /// 覆盖请用类型化 getter（[`FeatureFlags::is_enabled`] /
    /// [`FeatureFlags::get_int`] / [`FeatureFlags::get_string_list`]）。
    #[must_use]
    pub fn get_value(&self, name: &str) -> Option<FlagValue> {
        self.lookup(name)
    }

    /// 运行时改写 flag（旧 `setFeatureValue`，`/config` 域端点的数据入口）。
    ///
    /// 注意优先级：环境变量层在其之上，故被环境变量钉住的 flag 改写后读回不变
    /// ——旧实现同样如此（`getFeatureValue` 先查 env 再查表）。
    pub fn set_value(&self, name: &str, value: FlagValue) {
        // 先记日志再入表：`value` 随即被 `insert` 消费（不留克隆），而两步同处一个
        // 无 await 的同步函数内，外部观察不到先后差异。
        tracing::info!(flag = name, ?value, "feature flag updated");
        self.flags
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned(), value);
    }

    /// 全部在册 flag 的快照（旧 `getAllFlags` 只读视图的等价物）。
    #[must_use]
    pub fn all_flags(&self) -> BTreeMap<String, FlagValue> {
        self.flags
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// flag 表查询（锁内克隆，锁作用域常数级）。
    fn lookup(&self, name: &str) -> Option<FlagValue> {
        self.flags
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    /// 环境变量层原值：`ZK_FEATURE_<NAME>` → `FEATURE_<NAME>` → 裸 `<NAME>`
    /// （末层仅 [`PLACEHOLDER_ENV_FLAGS`] 三项）。
    ///
    /// 名称一律大写后拼装，对齐旧 `"FEATURE_" + featureKey.toUpperCase()`。
    fn env_raw(&self, name: &str) -> Option<String> {
        let upper = name.to_ascii_uppercase();
        self.env
            .get(&format!("{ENV_PREFIX_NATIVE}{upper}"))
            .or_else(|| self.env.get(&format!("{ENV_PREFIX_LEGACY}{upper}")))
            .or_else(|| {
                PLACEHOLDER_ENV_FLAGS
                    .contains(&upper.as_str())
                    .then(|| self.env.get(&upper))
                    .flatten()
            })
    }
}

impl Default for FeatureFlags {
    /// 等价 [`FeatureFlags::with_defaults`]（不读环境变量的确定性装配）。
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// 出厂默认值查询（环境变量解析失败时的回落源）。
///
/// 未登记的 flag 返回 `None`——旧仓那 7 个只在代码里读的 flag 走这条路。
#[must_use]
pub fn factory_default(name: &str) -> Option<FlagValue> {
    FACTORY_DEFAULTS
        .iter()
        .find(|(flag, _)| *flag == name)
        .map(|(_, default)| default.to_value())
}

/// 全部出厂登记的 flag 名（顺序同旧 YAML 声明序）。
pub fn factory_flag_names() -> impl Iterator<Item = &'static str> {
    FACTORY_DEFAULTS.iter().map(|(name, _)| *name)
}

/// 出厂表实例化。
fn factory_table() -> BTreeMap<String, FlagValue> {
    FACTORY_DEFAULTS
        .iter()
        .map(|(name, default)| ((*name).to_owned(), default.to_value()))
        .collect()
}

/// 逗号分隔串切分——逐段 `trim`，对齐旧消费点
/// `Arrays.stream(s.split(",")).map(String::trim)`。
fn split_list(raw: &str) -> Vec<String> {
    raw.split(',').map(|item| item.trim().to_owned()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 出厂表排除不受支持的 sandbox flag，保留 22 条。
    #[test]
    fn factory_table_matches_legacy_yaml_size() {
        assert_eq!(FACTORY_DEFAULTS.len(), 22);
        assert_eq!(factory_flag_names().count(), 22);
        assert_eq!(FeatureFlags::with_defaults().all_flags().len(), 22);
    }

    /// flag 名无重复——出厂表是手抄的，重复项会让后写者静默覆盖前者。
    #[test]
    fn factory_table_has_no_duplicate_names() {
        let mut names: Vec<&str> = factory_flag_names().collect();
        names.sort_unstable();
        let total = names.len();
        names.dedup();
        assert_eq!(
            names.len(),
            total,
            "duplicate flag name in FACTORY_DEFAULTS"
        );
    }

    /// 20 个受支持布尔 flag 的默认值对齐旧 `application.yml` L144-173。
    #[test]
    fn boolean_defaults_match_legacy_yaml() {
        let flags = FeatureFlags::with_defaults();
        for (name, expected) in [
            (THINKING_MODE, true),
            (TOOL_SEARCH, false),
            (CLASSIFIER_V2, false),
            (ENABLE_AGENT_SWARMS, true),
            (MCP_SKILLS, false),
            (COORDINATOR_MODE, true),
            (TOKEN_BUDGET, false),
            (SCRATCHPAD, true),
            (CACHED_MICROCOMPACT, true),
            (AGENT_TRIGGERS, false),
            (WEB_BROWSER_TOOL, true),
            (RUNTIME_VERIFICATION, true),
            (GIT_ENHANCED_TOOL, true),
            (RESOURCE_MONITOR, false),
            (BACKGROUND_AGENT_WAIT, false),
            (AGENT_ABORT_CASCADE, true),
            (SELF_CORRECTION_LOOP, false),
            (PRECISE_TOKENIZER, false),
            (GIT_DIFF_TRACKER, false),
            (SEARCH_STRATEGY_ROUTER, false),
        ] {
            assert_eq!(flags.is_enabled(name), expected, "flag {name}");
            assert_eq!(flags.check_gate(name), expected, "gate {name}");
            assert_eq!(
                factory_default(name),
                Some(FlagValue::Bool(expected)),
                "factory {name}"
            );
        }
    }

    /// 非布尔 flag 的默认值：`FRC_KEEP_RECENT: 3` / `FRC_SUPPORTED_MODELS`。
    #[test]
    fn typed_defaults_match_legacy_yaml() {
        let flags = FeatureFlags::with_defaults();
        assert_eq!(flags.get_int(FRC_KEEP_RECENT), 3);
        assert_eq!(
            flags.get_string_list(FRC_SUPPORTED_MODELS),
            vec!["light".to_owned(), "standard".to_owned()]
        );
        assert_eq!(
            flags.get_value(FRC_KEEP_RECENT),
            Some(FlagValue::Int(3)),
            "get_value 直读 flag 表"
        );
    }

    /// 未登记 flag 取类型零值（旧仓 7 个只在代码里读的 flag 走这条路）。
    #[test]
    fn unregistered_flag_falls_back_to_zero_value() {
        let flags = FeatureFlags::with_defaults();
        for name in [
            "EMBEDDED_SEARCH_TOOLS",
            "FORK_SUBAGENT",
            "INTERNAL_USER_MODE",
            "NUMERIC_LENGTH_ANCHORS",
            "PROMPT_CACHE_GLOBAL_SCOPE",
            "REPL_MODE",
            "SKILL_DISCOVERY",
        ] {
            assert!(!flags.is_enabled(name), "flag {name} must default off");
            assert_eq!(flags.get_value(name), None, "flag {name} not in table");
            assert_eq!(factory_default(name), None, "flag {name} not in factory");
        }
        assert_eq!(flags.get_int("UNKNOWN_INT"), 0);
        assert!(flags.get_string_list("UNKNOWN_LIST").is_empty());
    }

    /// `ZK_FEATURE_<NAME>` 覆盖布尔 flag（本仓原生前缀，优先级最高）。
    #[test]
    fn native_prefix_overrides_boolean_default() {
        let flags = FeatureFlags::with_env_overrides([("ZK_FEATURE_TOOL_SEARCH", "true")]);
        assert!(flags.is_enabled(TOOL_SEARCH), "默认 false → 环境变量开启");

        let flags = FeatureFlags::with_env_overrides([("ZK_FEATURE_THINKING_MODE", "false")]);
        assert!(!flags.is_enabled(THINKING_MODE), "默认 true → 环境变量关闭");
    }

    /// `FEATURE_<NAME>` 覆盖（旧实现前缀，未登记 flag 亦生效）。
    #[test]
    fn legacy_prefix_overrides_boolean_default() {
        let flags = FeatureFlags::with_env_overrides([
            ("FEATURE_WEB_BROWSER_TOOL", "false"),
            ("FEATURE_INTERNAL_USER_MODE", "TRUE"),
        ]);
        assert!(!flags.is_enabled(WEB_BROWSER_TOOL));
        assert!(
            flags.is_enabled("INTERNAL_USER_MODE"),
            "大小写不敏感 + 未登记 flag 可开"
        );
    }

    /// 原生前缀优先于旧前缀（两者同时存在时）。
    #[test]
    fn native_prefix_wins_over_legacy_prefix() {
        let flags = FeatureFlags::with_env_overrides([
            ("ZK_FEATURE_TOOL_SEARCH", "true"),
            ("FEATURE_TOOL_SEARCH", "false"),
        ]);
        assert!(flags.is_enabled(TOOL_SEARCH));
    }

    /// 裸名环境变量只对 YAML 占位符三项生效（旧 `${NAME:default}` 形态）。
    #[test]
    fn bare_env_name_applies_to_placeholder_flags_only() {
        let flags = FeatureFlags::with_env_overrides([
            ("SELF_CORRECTION_LOOP", "true"),
            ("GIT_ENHANCED_TOOL", "false"),
            ("RESOURCE_MONITOR", "true"),
            // 非占位符 flag：裸名必须被忽略，否则任意同名环境变量都会误触开关。
            ("TOOL_SEARCH", "true"),
            ("THINKING_MODE", "false"),
        ]);
        assert!(flags.is_enabled(SELF_CORRECTION_LOOP));
        assert!(!flags.is_enabled(GIT_ENHANCED_TOOL));
        assert!(flags.is_enabled(RESOURCE_MONITOR));
        assert!(!flags.is_enabled(TOOL_SEARCH), "裸名对非占位符 flag 无效");
        assert!(flags.is_enabled(THINKING_MODE), "裸名对非占位符 flag 无效");
    }

    /// 前缀形态优先于裸名（占位符 flag 上三层同时存在）。
    #[test]
    fn prefixed_env_wins_over_bare_name() {
        let flags = FeatureFlags::with_env_overrides([
            ("FEATURE_SELF_CORRECTION_LOOP", "false"),
            ("SELF_CORRECTION_LOOP", "true"),
        ]);
        assert!(!flags.is_enabled(SELF_CORRECTION_LOOP));
    }

    /// 布尔覆盖的旧 `Boolean.parseBoolean` 语义：非 `true` 一律为假、不回落、不 trim。
    #[test]
    fn boolean_env_parsing_matches_java_parse_boolean() {
        for raw in ["false", "", "1", "yes", "TRUE ", " true", "garbage", "True"] {
            let expected = raw.eq_ignore_ascii_case("true");
            let flags = FeatureFlags::with_env_overrides([("ZK_FEATURE_THINKING_MODE", raw)]);
            assert_eq!(
                flags.is_enabled(THINKING_MODE),
                expected,
                "raw {raw:?} 覆盖默认 true"
            );
        }
    }

    /// 整型覆盖生效；非法值按旧 `NumberFormatException` 分支回落出厂默认。
    #[test]
    fn int_env_override_and_parse_failure_fallback() {
        let flags = FeatureFlags::with_env_overrides([("ZK_FEATURE_FRC_KEEP_RECENT", "7")]);
        assert_eq!(flags.get_int(FRC_KEEP_RECENT), 7);

        let flags = FeatureFlags::with_env_overrides([("FEATURE_FRC_KEEP_RECENT", "-2")]);
        assert_eq!(
            flags.get_int(FRC_KEEP_RECENT),
            -2,
            "负值不做业务校验，同旧实现"
        );

        for raw in ["abc", "", " 3"] {
            let flags = FeatureFlags::with_env_overrides([("ZK_FEATURE_FRC_KEEP_RECENT", raw)]);
            assert_eq!(
                flags.get_int(FRC_KEEP_RECENT),
                3,
                "raw {raw:?} 回落出厂默认"
            );
        }
        let flags = FeatureFlags::with_env_overrides([("FEATURE_UNKNOWN_INT", "abc")]);
        assert_eq!(flags.get_int("UNKNOWN_INT"), 0, "未登记 flag 回落 0");
    }

    /// 字符串列表覆盖：逗号切分 + 逐段 trim；空串保留单个空段（旧退化形态）。
    #[test]
    fn string_list_env_override_splits_and_trims() {
        let flags = FeatureFlags::with_env_overrides([(
            "ZK_FEATURE_FRC_SUPPORTED_MODELS",
            "light, premium",
        )]);
        assert_eq!(
            flags.get_string_list(FRC_SUPPORTED_MODELS),
            vec!["light".to_owned(), "premium".to_owned()]
        );

        let flags = FeatureFlags::with_env_overrides([("ZK_FEATURE_FRC_SUPPORTED_MODELS", "")]);
        assert_eq!(
            flags.get_string_list(FRC_SUPPORTED_MODELS),
            vec![String::new()],
            "旧 \"\".split(\",\") 亦得单个空段"
        );
    }

    /// 类型不匹配读取取类型零值（旧实现该路径抛 `ClassCastException`）。
    #[test]
    fn type_mismatch_reads_zero_value() {
        let flags = FeatureFlags::with_defaults();
        assert!(!flags.is_enabled(FRC_KEEP_RECENT), "整型 flag 当布尔读");
        assert_eq!(flags.get_int(THINKING_MODE), 0, "布尔 flag 当整型读");
        assert!(
            flags.get_string_list(THINKING_MODE).is_empty(),
            "布尔 flag 当列表读"
        );
    }

    /// 运行时改写生效（旧 `setFeatureValue`），且对 `all_flags` 快照可见。
    #[test]
    fn set_value_updates_table() {
        let flags = FeatureFlags::with_defaults();
        flags.set_value(TOOL_SEARCH, FlagValue::Bool(true));
        flags.set_value(FRC_KEEP_RECENT, FlagValue::Int(9));

        assert!(flags.is_enabled(TOOL_SEARCH));
        assert_eq!(flags.get_int(FRC_KEEP_RECENT), 9);
        assert_eq!(
            flags.all_flags().get(TOOL_SEARCH),
            Some(&FlagValue::Bool(true))
        );
        assert_eq!(flags.all_flags().len(), 22, "改写不新增条目");

        flags.set_value("BRAND_NEW_FLAG", FlagValue::Bool(true));
        assert_eq!(flags.all_flags().len(), 23, "新键入表，同旧 Map.put");
    }

    /// 环境变量层压过运行时改写——旧 `getFeatureValue` 先查 env 再查表。
    #[test]
    fn env_layer_outranks_runtime_set_value() {
        let flags = FeatureFlags::with_env_overrides([("ZK_FEATURE_TOOL_SEARCH", "true")]);
        flags.set_value(TOOL_SEARCH, FlagValue::Bool(false));
        assert!(
            flags.is_enabled(TOOL_SEARCH),
            "env 覆盖不可被运行时改写掀翻"
        );
        assert_eq!(
            flags.get_value(TOOL_SEARCH),
            Some(FlagValue::Bool(false)),
            "get_value 只看 flag 表，如实反映改写"
        );
    }

    /// `Arc` 共享下改写对所有持有者可见（旧单例 Bean 语义）。
    #[test]
    fn shared_handle_sees_updates() {
        let flags = std::sync::Arc::new(FeatureFlags::with_defaults());
        let handle = std::sync::Arc::clone(&flags);
        handle.set_value(TOOL_SEARCH, FlagValue::Bool(true));
        assert!(flags.is_enabled(TOOL_SEARCH));
    }

    /// `Default` 与 `with_defaults` 等价。
    #[test]
    fn default_impl_equals_with_defaults() {
        assert_eq!(
            FeatureFlags::default().all_flags(),
            FeatureFlags::with_defaults().all_flags()
        );
    }
}
