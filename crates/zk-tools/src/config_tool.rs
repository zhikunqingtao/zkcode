//! `Config` 工具——运行时配置的 `get` / `set` / `list`。
//!
//! 逐字对照旧 `tool/config/ConfigTool.java`（只读权威规格）：工具名 `Config`、
//! 七项配置默认值、三项枚举可选值、`model` 键的可选值动态取自提供商注册表
//! （内置别名 `light` / `standard` / `premium` 打头，再追加实际可用模型）、
//! `"default"` 值重置语义、布尔 / 整数类型强制，以及三条错误码
//! （`CONFIG_SETTING_UNKNOWN` / `CONFIG_VALUE_INVALID` / `CONFIG_ACTION_INVALID`）
//! 与三段成功文案。
//!
//! 差异（留痕 docs/compatibility.md §9）：
//!
//! - 旧存储为 `ConcurrentHashMap`，`list` 的遍历序不确定；本实现用
//!   [`BTreeMap`] 保证键序稳定（同一组配置每次输出同序），便于工具结果进入
//!   上下文后被缓存断点复用。
//! - 旧 `isConcurrencySafe(input)` 无 Rust 对应面（本框架的并发安全由执行器
//!   统一的信号量 + 取消树承担），仅移植 `isReadOnly(input) = action != "set"`。
//! - 旧 `shouldDefer() = true`（结果延后进上下文）属旧管线的调度标记，本框架
//!   无该面，未移植。
//! - `get` / `set` 缺 `key`（或 `set` 缺 `value`）时旧实现由 `ToolInput.getString`
//!   抛校验异常，本实现按框架约定返回 `MISSING_PARAMETER`。
//! - 旧 `getModelOptions` 在提供商查询抛异常时只 `log.debug` 并返回内置别名；
//!   本实现的 [`ModelCatalog`] 端口以「返回空列表」表达同一降级（端口不返回
//!   错误，实现内部自吞）。

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::{failure, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 内置模型别名（旧 `LlmProviderRegistry.getBuiltinAliases()` 逐字返回值）。
pub const BUILTIN_MODEL_ALIASES: [&str; 3] = ["light", "standard", "premium"];

/// 重置为默认值的哨兵入参（旧 `"default".equals(value)`）。
const RESET_SENTINEL: &str = "default";

/// 可用模型目录端口（旧 `LlmProviderRegistry.listAvailableModels()`）。
///
/// 依赖方向铁律禁止 `zk-tools → zk-llm`，故以端口反转，生产实现由组合根注入。
pub trait ModelCatalog: Send + Sync {
    /// 当前可用模型名（默认模型置首，与旧实现同序；查询失败返回空列表）。
    fn available_models(&self) -> Vec<String>;
}

/// 支持的配置项及默认值（旧 `DEFAULTS`）。
fn defaults() -> &'static BTreeMap<&'static str, Value> {
    static DEFAULTS: LazyLock<BTreeMap<&'static str, Value>> = LazyLock::new(|| {
        BTreeMap::from([
            ("theme", json!("system")),
            ("model", json!("standard")),
            ("maxTokens", json!(8192)),
            ("autoCompact", json!(true)),
            ("verboseLogging", json!(false)),
            ("maxTurns", json!(100)),
            ("language", json!("auto")),
        ])
    });
    &DEFAULTS
}

/// 静态可选值（旧 `OPTIONS`；`model` 键的可选值动态取自 [`ModelCatalog`]）。
fn static_options(key: &str) -> Option<Vec<String>> {
    let values: &[&str] = match key {
        "theme" => &["system", "light", "dark"],
        "language" => &["auto", "en", "zh", "ja", "ko", "fr", "de", "es"],
        _ => return None,
    };
    Some(values.iter().map(|value| (*value).to_owned()).collect())
}

/// 进程级运行时配置存储（等价旧单例 bean 的 `ConcurrentMap`，以 `DEFAULTS` 播种）。
fn store() -> &'static Mutex<BTreeMap<String, Value>> {
    static STORE: LazyLock<Mutex<BTreeMap<String, Value>>> = LazyLock::new(|| {
        Mutex::new(
            defaults()
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
        )
    });
    &STORE
}

/// 取存储锁（毒化后继续用内层值）。
fn lock() -> std::sync::MutexGuard<'static, BTreeMap<String, Value>> {
    store()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// 运行时配置工具（名 `Config`）。
#[derive(Clone, Default)]
pub struct ConfigTool {
    /// 模型目录；`None` = 只用内置别名（旧查询失败时的等价降级）。
    catalog: Option<Arc<dyn ModelCatalog>>,
}

impl std::fmt::Debug for ConfigTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigTool")
            .field("model_catalog", &self.catalog.is_some())
            .finish()
    }
}

impl ConfigTool {
    /// 装配（无模型目录）。
    #[must_use]
    pub fn new() -> Self {
        Self { catalog: None }
    }

    /// 装配并注入模型目录（组合根提供 zk-llm 实现）。
    #[must_use]
    pub fn with_model_catalog(catalog: Arc<dyn ModelCatalog>) -> Self {
        Self {
            catalog: Some(catalog),
        }
    }

    /// `model` 键的可选值：内置别名在前，再追加未重复的可用模型
    /// （旧 `getModelOptions`）。
    fn model_options(&self) -> Vec<String> {
        let mut options: Vec<String> = BUILTIN_MODEL_ALIASES
            .iter()
            .map(|alias| (*alias).to_owned())
            .collect();
        if let Some(catalog) = self.catalog.as_ref() {
            for model in catalog.available_models() {
                if !options.contains(&model) {
                    options.push(model);
                }
            }
        }
        options
    }
}

impl Tool for ConfigTool {
    fn name(&self) -> &'static str {
        "Config"
    }

    fn description(&self) -> &'static str {
        "Read and modify runtime configuration settings. \
         Supports get, set, and list actions."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["get", "set", "list"],
                    "description": "Action to perform (default: get)"
                },
                "key": {
                    "type": "string",
                    "description": "Configuration key (required for get/set)"
                },
                "value": {
                    "type": "string",
                    "description": "Value to set (required for set action)"
                }
            }
        })
    }

    /// 只读判定（旧 `isReadOnly(input) = !"set".equals(action)`，缺省 `get`）。
    fn is_read_only(&self, input: &Value) -> bool {
        action_of(input) != "set"
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.run(&input) })
    }
}

impl ConfigTool {
    /// 执行主体（三动作分派，未知动作走旧 `default` 分支）。
    fn run(&self, input: &Value) -> ToolOutput {
        match action_of(input) {
            "list" => list(),
            "get" => get(input),
            "set" => self.set(input),
            other => failure(
                "CONFIG_ACTION_INVALID",
                format!("Unknown action: {other}. Expected: get, set, list."),
            ),
        }
    }

    /// `set`：未知键拒绝 → `"default"` 重置 → 可选值校验 → 类型强制 → 落存储。
    fn set(&self, input: &Value) -> ToolOutput {
        let key = match required_str(input, "key") {
            Ok(value) => value,
            Err(output) => return output,
        };
        let value = match required_str(input, "value") {
            Ok(value) => value,
            Err(output) => return output,
        };
        let Some(default_value) = defaults().get(key) else {
            return failure("CONFIG_SETTING_UNKNOWN", format!("Unknown setting: {key}"));
        };
        if value == RESET_SENTINEL {
            lock().insert(key.to_owned(), default_value.clone());
            return ToolOutput::ok(format!(
                "Setting '{key}' reset to default: {}",
                render(default_value)
            ));
        }
        let options = if key == "model" {
            Some(self.model_options())
        } else {
            static_options(key)
        };
        if let Some(options) = options
            && !options.iter().any(|option| option == value)
        {
            return failure(
                "CONFIG_VALUE_INVALID",
                format!(
                    "Invalid value for '{key}'. Options: {}",
                    render_list(&options)
                ),
            );
        }
        let typed = coerce(default_value, value);
        let previous = lock().insert(key.to_owned(), typed.clone());
        let previous = previous.unwrap_or_else(|| default_value.clone());
        tracing::info!(
            key,
            previous = %render(&previous),
            current = %render(&typed),
            "Config updated"
        );
        ToolOutput::ok(format!(
            "Setting '{key}' updated: {} → {}",
            render(&previous),
            render(&typed)
        ))
    }
}

/// 动作入参（缺省 `get`，旧 `input.getString("action", "get")`）。
fn action_of(input: &Value) -> &str {
    input.get("action").and_then(Value::as_str).unwrap_or("get")
}

/// `list`：逐项输出 `"  {key} = {value}"`。
fn list() -> ToolOutput {
    use std::fmt::Write as _;

    let mut text = String::from("Available settings:\n");
    for (key, value) in lock().iter() {
        // 写入 String 永不失败。
        let _ = writeln!(text, "  {key} = {}", render(value));
    }
    ToolOutput::ok(text)
}

/// `get`：未知键拒绝，否则输出当前值（缺项回落默认值）。
fn get(input: &Value) -> ToolOutput {
    let key = match required_str(input, "key") {
        Ok(value) => value,
        Err(output) => return output,
    };
    let Some(default_value) = defaults().get(key) else {
        return failure("CONFIG_SETTING_UNKNOWN", format!("Unknown setting: {key}"));
    };
    let value = lock()
        .get(key)
        .cloned()
        .unwrap_or_else(|| default_value.clone());
    ToolOutput::ok(format!("Setting '{key}' = {}", render(&value)))
}

/// 类型强制（旧 `coerceType`：默认值为布尔 → `Boolean.parseBoolean`；为整数 →
/// `Integer.parseInt`，失败保留原字符串；其余原样）。
fn coerce(default_value: &Value, value: &str) -> Value {
    if default_value.is_boolean() {
        return Value::Bool(value.eq_ignore_ascii_case("true"));
    }
    if default_value.is_i64() || default_value.is_u64() {
        return value
            .parse::<i64>()
            .map_or_else(|_| Value::String(value.to_owned()), Value::from);
    }
    Value::String(value.to_owned())
}

/// 渲染单值（等价 Java `String.format("%s", value)`：字符串不带引号）。
fn render(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), std::borrow::ToOwned::to_owned)
}

/// 渲染可选值列表（等价 Java `List.toString()` 的 `[a, b, c]`）。
fn render_list(options: &[String]) -> String {
    format!("[{}]", options.join(", "))
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// 固定模型列表的桩目录。
    struct StubCatalog(Vec<&'static str>);

    impl ModelCatalog for StubCatalog {
        fn available_models(&self) -> Vec<String> {
            self.0.iter().map(|name| (*name).to_owned()).collect()
        }
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    async fn call(tool: &ConfigTool, input: Value) -> ToolOutput {
        tool.execute(input, ctx()).await
    }

    /// 每个用例前把存储复位到默认值——进程级单例在测试间共享。
    fn reset_store() {
        let mut store = lock();
        store.clear();
        for (key, value) in defaults() {
            store.insert((*key).to_owned(), value.clone());
        }
    }

    #[tokio::test]
    async fn list_prints_every_setting_in_stable_key_order() {
        reset_store();
        let output = call(&ConfigTool::new(), json!({ "action": "list" })).await;
        assert!(!output.is_error);
        assert_eq!(
            output.content,
            "Available settings:\n\
             \x20 autoCompact = true\n\
             \x20 language = auto\n\
             \x20 maxTokens = 8192\n\
             \x20 maxTurns = 100\n\
             \x20 model = standard\n\
             \x20 theme = system\n\
             \x20 verboseLogging = false\n"
        );
    }

    #[tokio::test]
    async fn get_defaults_to_the_get_action_and_renders_scalars_bare() {
        reset_store();
        let tool = ConfigTool::new();
        let theme = call(&tool, json!({ "key": "theme" })).await;
        assert_eq!(theme.content, "Setting 'theme' = system");
        let tokens = call(&tool, json!({ "action": "get", "key": "maxTokens" })).await;
        assert_eq!(tokens.content, "Setting 'maxTokens' = 8192");
        let compact = call(&tool, json!({ "action": "get", "key": "autoCompact" })).await;
        assert_eq!(compact.content, "Setting 'autoCompact' = true");
    }

    #[tokio::test]
    async fn set_coerces_booleans_and_integers_and_reports_the_transition() {
        reset_store();
        let tool = ConfigTool::new();
        let tokens = call(
            &tool,
            json!({ "action": "set", "key": "maxTokens", "value": "4096" }),
        )
        .await;
        assert_eq!(tokens.content, "Setting 'maxTokens' updated: 8192 → 4096");

        let compact = call(
            &tool,
            json!({ "action": "set", "key": "autoCompact", "value": "false" }),
        )
        .await;
        assert_eq!(
            compact.content,
            "Setting 'autoCompact' updated: true → false"
        );

        // 整数解析失败保留原字符串（旧 `NumberFormatException → return value`）。
        let bad_number = call(
            &tool,
            json!({ "action": "set", "key": "maxTurns", "value": "many" }),
        )
        .await;
        assert_eq!(bad_number.content, "Setting 'maxTurns' updated: 100 → many");
    }

    #[tokio::test]
    async fn set_default_resets_to_the_builtin_value() {
        reset_store();
        let tool = ConfigTool::new();
        call(
            &tool,
            json!({ "action": "set", "key": "theme", "value": "dark" }),
        )
        .await;
        let reset = call(
            &tool,
            json!({ "action": "set", "key": "theme", "value": "default" }),
        )
        .await;
        assert_eq!(reset.content, "Setting 'theme' reset to default: system");
        assert_eq!(
            call(&tool, json!({ "key": "theme" })).await.content,
            "Setting 'theme' = system"
        );
    }

    #[tokio::test]
    async fn set_validates_enumerated_values_and_dynamic_model_options() {
        reset_store();
        let tool = ConfigTool::new();
        let bad_theme = call(
            &tool,
            json!({ "action": "set", "key": "theme", "value": "neon" }),
        )
        .await;
        assert!(bad_theme.is_error);
        assert_eq!(
            bad_theme.content,
            "CONFIG_VALUE_INVALID: Invalid value for 'theme'. Options: [system, light, dark]"
        );

        // 无目录时只认内置别名。
        let unknown_model = call(
            &tool,
            json!({ "action": "set", "key": "model", "value": "gpt-9" }),
        )
        .await;
        assert_eq!(
            unknown_model.content,
            "CONFIG_VALUE_INVALID: Invalid value for 'model'. \
             Options: [light, standard, premium]"
        );

        // 注入目录后追加可用模型，且不重复内置别名。
        let with_catalog = ConfigTool::with_model_catalog(std::sync::Arc::new(StubCatalog(vec![
            "gpt-9", "standard",
        ])));
        let accepted = call(
            &with_catalog,
            json!({ "action": "set", "key": "model", "value": "gpt-9" }),
        )
        .await;
        assert_eq!(
            accepted.content,
            "Setting 'model' updated: standard → gpt-9"
        );
        let still_invalid = call(
            &with_catalog,
            json!({ "action": "set", "key": "model", "value": "nope" }),
        )
        .await;
        assert_eq!(
            still_invalid.content,
            "CONFIG_VALUE_INVALID: Invalid value for 'model'. \
             Options: [light, standard, premium, gpt-9]"
        );
    }

    #[tokio::test]
    async fn rejects_unknown_settings_actions_and_missing_parameters() {
        reset_store();
        let tool = ConfigTool::new();
        let unknown_get = call(&tool, json!({ "key": "nope" })).await;
        assert_eq!(
            unknown_get.content,
            "CONFIG_SETTING_UNKNOWN: Unknown setting: nope"
        );
        let unknown_set = call(
            &tool,
            json!({ "action": "set", "key": "nope", "value": "x" }),
        )
        .await;
        assert_eq!(
            unknown_set.content,
            "CONFIG_SETTING_UNKNOWN: Unknown setting: nope"
        );
        let unknown_action = call(&tool, json!({ "action": "purge" })).await;
        assert_eq!(
            unknown_action.content,
            "CONFIG_ACTION_INVALID: Unknown action: purge. Expected: get, set, list."
        );
        let no_key = call(&tool, json!({ "action": "get" })).await;
        assert!(no_key.content.starts_with("MISSING_PARAMETER: "));
        let no_value = call(&tool, json!({ "action": "set", "key": "theme" })).await;
        assert!(no_value.content.starts_with("MISSING_PARAMETER: "));
    }

    #[test]
    fn read_only_flag_tracks_the_action() {
        let tool = ConfigTool::new();
        assert!(tool.is_read_only(&json!({})));
        assert!(tool.is_read_only(&json!({ "action": "list" })));
        assert!(!tool.is_read_only(&json!({ "action": "set" })));
        assert_eq!(tool.spec().name, "Config");
    }
}
