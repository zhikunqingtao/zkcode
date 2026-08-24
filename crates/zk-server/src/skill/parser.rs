//! 技能 Markdown 解析——YAML frontmatter 提取、正文切分与模板参数替换。
//!
//! 语义来源（旧仓库只读，`581d407b`）：
//! `backend/src/main/java/com/aicodeassistant/skill/FrontmatterParser.java`
//! （解析四步：`---` 起始分隔符 → `\n---` 结束分隔符 → 简化 YAML → 正文
//! 首段落作 description 兜底）、`FrontmatterData.java`（16 字段 record 与
//! 默认值）、`ArgumentSubstitution.java`（`{{arg}}` 模板变量三算法）。
//!
//! 逐分支复刻要点：
//! - 起始分隔符必须是 trim 后的第一个字符序列，否则整篇作正文 + 首段落兜底；
//! - 结束分隔符从下标 3 起搜 `\n---`，缺失同样退化为「无 frontmatter」；
//! - YAML 只支持 `key: value` 与缩进 `- item` 两种形态（旧实现即如此，
//!   非完整 YAML 子集），注释行 `#` 跳过，值两侧同种引号剥离；
//! - key 统一 `lowercase` + `_` → `-` 归一，读取时再回试 snake/kebab 双写法；
//! - `allowed-tools` 支持逗号串或列表，`arguments` / `paths` 支持单值或列表。
//!
//! 与旧实现的刻意差异：
//! - 正则用显式 ASCII 字符类（`[A-Za-z0-9_]`）替代 `\w`——Rust `regex` 的
//!   `\w` 默认 Unicode 感知，而 Java `Pattern` 的 `\w` 为纯 ASCII，若直用
//!   `\w` 会让中文 key 意外通过校验，故按 Java 语义写死 ASCII；
//! - `hooks` 字段不建模：旧实现 `buildFrontmatterData` 恒填 `Map.of()`
//!   （注释「hooks: 简化处理」），无任何读取点，故 Rust 侧不引入死字段。

use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;

use regex::{Captures, Regex};
use serde::Serialize;

/// 模板变量：`{{arg_name}}`（旧 `TEMPLATE_VAR`）。
static TEMPLATE_VAR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\{\{[ \t]*([A-Za-z0-9_]+)[ \t]*\}\}").expect("static template var regex")
});

/// YAML `key: value` 行（旧 `YAML_LINE`，作用于 trim 后的整行）。
static YAML_LINE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([A-Za-z0-9_][A-Za-z0-9_\-]*):[ \t]*(.*)$").expect("static yaml line regex")
});

/// YAML 列表项（旧 `YAML_LIST_ITEM`，要求行首缩进，作用于原始行）。
static YAML_LIST_ITEM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[ \t]+-[ \t]+(.+)$").expect("static yaml list item regex"));

/// 简化 YAML 的值形态——标量或字符串列表（旧 `Map<String,Object>` 的两种
/// 实际取值：`String` 与 `List<String>`）。
#[derive(Debug, Clone, PartialEq, Eq)]
enum YamlValue {
    /// 标量（已去引号）。
    Scalar(String),
    /// 缩进列表项聚合。
    List(Vec<String>),
}

/// 技能 frontmatter 元数据（旧 record `FrontmatterData` 15/16 字段，
/// `hooks` 见模块文档的刻意差异说明）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FrontmatterData {
    /// 技能描述（缺省时由正文首段落兜底）。
    pub description: Option<String>,
    /// 显示名称（覆盖文件名）。
    pub name: Option<String>,
    /// 允许的工具列表（`allowed-tools`）。
    pub allowed_tools: Vec<String>,
    /// 参数提示文本（`argument-hint`）。
    pub argument_hint: Option<String>,
    /// 参数定义列表（位置参数按此顺序匹配）。
    pub arguments: Vec<String>,
    /// 模型自动调用的条件描述（`when_to_use`）。
    pub when_to_use: Option<String>,
    /// 版本号。
    pub version: Option<String>,
    /// 指定模型（`inherit` 表示沿用父模型）。
    pub model: Option<String>,
    /// 禁止模型自动调用（`disable_model_invocation`）。
    pub disable_model_invocation: bool,
    /// 用户可手动调用（`user-invocable`，默认 true）。
    pub user_invocable: bool,
    /// 推理努力等级。
    pub effort: Option<String>,
    /// 执行上下文：`inline`（默认）或 `fork`。
    pub context: String,
    /// 关联子代理名称（仅 `fork` 上下文有效）。
    pub agent: Option<String>,
    /// 文件路径 glob 模式列表。
    pub paths: Vec<String>,
    /// Shell 类型：`bash`（默认）或 `powershell`。
    pub shell: String,
}

impl Default for FrontmatterData {
    /// 旧 `FrontmatterData.defaults()`：`context=inline`、`shell=bash`、
    /// `user-invocable=true`，其余空。
    fn default() -> Self {
        Self {
            description: None,
            name: None,
            allowed_tools: Vec::new(),
            argument_hint: None,
            arguments: Vec::new(),
            when_to_use: None,
            version: None,
            model: None,
            disable_model_invocation: false,
            user_invocable: true,
            effort: None,
            context: "inline".to_owned(),
            agent: None,
            paths: Vec::new(),
            shell: "bash".to_owned(),
        }
    }
}

impl FrontmatterData {
    /// 是否 fork 执行模式（旧 `isFork`，大小写不敏感）。
    #[must_use]
    pub fn is_fork(&self) -> bool {
        self.context.eq_ignore_ascii_case("fork")
    }

    /// 有效模型（旧 `resolvedModel`：`inherit` → `None` 表示沿用父模型）。
    #[must_use]
    pub fn resolved_model(&self) -> Option<&str> {
        self.model
            .as_deref()
            .filter(|model| !model.eq_ignore_ascii_case("inherit"))
    }
}

/// 解析结果（旧 record `ParsedMarkdown`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMarkdown {
    /// frontmatter 元数据。
    pub frontmatter: FrontmatterData,
    /// Markdown 正文（模板渲染输入）。
    pub content: String,
}

/// 解析 Markdown 文件内容（旧 `FrontmatterParser.parse`，逐分支复刻）。
#[must_use]
pub fn parse(raw_content: &str) -> ParsedMarkdown {
    let content = raw_content.trim();
    if content.is_empty() {
        return ParsedMarkdown {
            frontmatter: FrontmatterData::default(),
            content: String::new(),
        };
    }
    // 起始分隔符缺失 → 整篇作正文，description 走首段落兜底。
    if !content.starts_with("---") {
        return ParsedMarkdown {
            frontmatter: fallback_description(content),
            content: content.to_owned(),
        };
    }
    // 结束分隔符：从下标 3 起搜 `\n---`（旧 `indexOf("\n---", 3)`）。
    let Some(offset) = content[3..].find("\n---") else {
        return ParsedMarkdown {
            frontmatter: fallback_description(content),
            content: content.to_owned(),
        };
    };
    let end_index = 3 + offset;
    let yaml_text = content[3..end_index].trim();
    let body = content[end_index + 4..].trim();

    let mut frontmatter = parse_yaml(yaml_text);
    // description 兜底：frontmatter 缺省/空白时取正文首段落。
    if frontmatter
        .description
        .as_ref()
        .is_none_or(|value| value.trim().is_empty())
        && let Some(fallback) = extract_first_paragraph(body)
    {
        frontmatter.description = Some(fallback);
    }
    ParsedMarkdown {
        frontmatter,
        content: body.to_owned(),
    }
}

/// 解析简化 YAML（旧 `parseYaml`：`key: value` + 缩进 `- item` 两形态）。
#[must_use]
pub fn parse_yaml(yaml_text: &str) -> FrontmatterData {
    let mut map: HashMap<String, YamlValue> = HashMap::new();
    let mut current_key: Option<String> = None;

    for line in yaml_text.split('\n') {
        // 注释行跳过。
        if line.trim().starts_with('#') {
            continue;
        }
        // 列表项：归属最近一次出现的 key。
        if let Some(caps) = YAML_LIST_ITEM.captures(line)
            && let Some(key) = current_key.clone()
        {
            let item = unquote(caps[1].trim());
            match map.get_mut(&key) {
                Some(YamlValue::List(items)) => items.push(item),
                _ => {
                    map.insert(key, YamlValue::List(vec![item]));
                }
            }
            continue;
        }
        // `key: value` 行（旧实现对 trim 后整行匹配）。
        if let Some(caps) = YAML_LINE.captures(line.trim()) {
            let key = normalize_key(&caps[1]);
            let value = caps[2].trim();
            // 空值可能是列表头，先占位标量空串（旧 `map.put(key, "")`）。
            map.insert(key.clone(), YamlValue::Scalar(unquote(value)));
            current_key = Some(key);
        }
    }
    build_frontmatter_data(&map)
}

/// 由归一后的 YAML map 构建 [`FrontmatterData`]（旧 `buildFrontmatterData`）。
fn build_frontmatter_data(map: &HashMap<String, YamlValue>) -> FrontmatterData {
    FrontmatterData {
        description: get_str(map, "description"),
        name: get_str(map, "name"),
        allowed_tools: parse_tools_list(map.get("allowed-tools")),
        argument_hint: get_str(map, "argument-hint"),
        arguments: parse_string_list(map.get("arguments")),
        when_to_use: get_str(map, "when_to_use"),
        version: get_str(map, "version"),
        model: get_str(map, "model"),
        disable_model_invocation: get_str(map, "disable_model_invocation")
            .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        user_invocable: !get_str(map, "user-invocable")
            .is_some_and(|value| value.eq_ignore_ascii_case("false")),
        effort: get_str(map, "effort"),
        context: get_str(map, "context").unwrap_or_else(|| "inline".to_owned()),
        agent: get_str(map, "agent"),
        paths: parse_string_list(map.get("paths")),
        shell: get_str(map, "shell").unwrap_or_else(|| "bash".to_owned()),
    }
}

/// key 归一（旧 `normalizeKey`：小写 + `_` → `-`）。
fn normalize_key(key: &str) -> String {
    key.to_lowercase().replace('_', "-")
}

/// 标量读取（旧 `getStr`：原名 → snake → kebab 三试，空串视作缺省）。
fn get_str(map: &HashMap<String, YamlValue>, key: &str) -> Option<String> {
    let candidates = [key.to_owned(), key.replace('-', "_"), key.replace('_', "-")];
    candidates
        .iter()
        .find_map(|candidate| match map.get(candidate) {
            Some(YamlValue::Scalar(value)) if !value.is_empty() => Some(value.clone()),
            _ => None,
        })
}

/// 工具列表（旧 `parseToolsList`：逗号串或 YAML 列表）。
fn parse_tools_list(value: Option<&YamlValue>) -> Vec<String> {
    match value {
        None => Vec::new(),
        Some(YamlValue::List(items)) => items.clone(),
        Some(YamlValue::Scalar(raw)) => raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect(),
    }
}

/// 字符串列表（旧 `parseStringList`：单标量视作一元列表）。
fn parse_string_list(value: Option<&YamlValue>) -> Vec<String> {
    match value {
        None => Vec::new(),
        Some(YamlValue::List(items)) => items.clone(),
        Some(YamlValue::Scalar(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Vec::new()
            } else {
                vec![trimmed.to_owned()]
            }
        }
    }
}

/// 去引号（旧 `unquote`：同种成对单/双引号才剥离）。
fn unquote(value: &str) -> String {
    let bytes = value.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return value[1..value.len() - 1].to_owned();
        }
    }
    value.to_owned()
}

/// 正文首段落（旧 `extractFirstParagraph`：跳标题行，空行终止，空格拼接）。
fn extract_first_paragraph(content: &str) -> Option<String> {
    if content.trim().is_empty() {
        return None;
    }
    let mut buffer = String::new();
    for line in content.split('\n') {
        let trimmed = line.trim();
        if trimmed.is_empty() && !buffer.is_empty() {
            break;
        }
        if trimmed.starts_with('#') {
            continue;
        }
        if !trimmed.is_empty() {
            if !buffer.is_empty() {
                buffer.push(' ');
            }
            buffer.push_str(trimmed);
        }
    }
    let result = buffer.trim().to_owned();
    (!result.is_empty()).then_some(result)
}

/// 无 frontmatter 时的兜底（旧 `fallbackDescription`）。
fn fallback_description(content: &str) -> FrontmatterData {
    FrontmatterData {
        description: extract_first_paragraph(content),
        ..FrontmatterData::default()
    }
}

// ── 模板参数替换（旧 ArgumentSubstitution） ─────────────────────────────

/// 提取模板全部参数名（旧 `parseArgumentNames`：去重保序）。
#[must_use]
pub fn parse_argument_names(content: &str) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for caps in TEMPLATE_VAR.captures_iter(content) {
        let name = caps[1].to_owned();
        if !seen.contains(&name) {
            seen.push(name);
        }
    }
    seen
}

/// 解析参数字符串（旧 `parseArgs`：`key=value` 命名参数 + 位置参数按
/// `argDefs` 顺序匹配 + 无定义时整串归入 `args`）。
///
/// 容器取 `BTreeMap` 而非旧 `LinkedHashMap`：参数顺序对替换与渲染无语义，
/// 有序 map 让断言与日志确定化。
#[must_use]
pub fn parse_args(args_string: &str, arg_defs: &[String]) -> BTreeMap<String, String> {
    let mut result: BTreeMap<String, String> = BTreeMap::new();
    if args_string.trim().is_empty() {
        return result;
    }
    let mut positional: Vec<&str> = Vec::new();
    for part in args_string.split_whitespace() {
        match part.find('=') {
            // `=` 在首位不算命名参数（旧 `eqIdx > 0`）。
            Some(idx) if idx > 0 => {
                result.insert(part[..idx].to_owned(), part[idx + 1..].to_owned());
            }
            _ => positional.push(part),
        }
    }
    for (name, value) in arg_defs.iter().zip(positional.iter()) {
        result
            .entry(name.clone())
            .or_insert_with(|| (*value).to_owned());
    }
    if arg_defs.is_empty() && !positional.is_empty() {
        result.insert("args".to_owned(), positional.join(" "));
    }
    result
}

/// 替换模板变量（旧 `substitute`：未提供的参数保留 `{{name}}` 占位符）。
#[must_use]
pub fn substitute(content: &str, params: &BTreeMap<String, String>) -> String {
    if params.is_empty() {
        return content.to_owned();
    }
    TEMPLATE_VAR
        .replace_all(content, |caps: &Captures<'_>| {
            params
                .get(&caps[1])
                .cloned()
                .unwrap_or_else(|| caps[0].to_owned())
        })
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    /// 完整 frontmatter：键归一（`allowed-tools` / `argument_hint` 混写）、
    /// 列表两形态（逗号串 + 缩进列表）、引号剥离、注释行跳过。
    #[test]
    fn parses_frontmatter_keys_lists_and_comments() {
        let raw = "---\n\
                   # 注释行整行跳过\n\
                   name: \"Git Commit\"\n\
                   description: '生成提交信息'\n\
                   allowed-tools: Bash, Read , \n\
                   argument_hint: <message>\n\
                   arguments:\n\
                   \x20 - message\n\
                   \x20 - scope\n\
                   model: inherit\n\
                   user-invocable: false\n\
                   disable_model_invocation: true\n\
                   context: fork\n\
                   ---\n\
                   # 标题\n\n\
                   正文首段。\n";
        let parsed = parse(raw);
        let fm = &parsed.frontmatter;
        assert_eq!(fm.name.as_deref(), Some("Git Commit"));
        assert_eq!(fm.description.as_deref(), Some("生成提交信息"));
        assert_eq!(fm.allowed_tools, vec!["Bash".to_owned(), "Read".to_owned()]);
        assert_eq!(fm.argument_hint.as_deref(), Some("<message>"));
        assert_eq!(fm.arguments, vec!["message".to_owned(), "scope".to_owned()]);
        // `inherit` 保留原值，仅 `resolved_model` 归一为 None。
        assert_eq!(fm.model.as_deref(), Some("inherit"));
        assert_eq!(fm.resolved_model(), None);
        assert!(!fm.user_invocable);
        assert!(fm.disable_model_invocation);
        assert!(fm.is_fork());
        assert_eq!(parsed.content, "# 标题\n\n正文首段。");
    }

    /// 无 frontmatter / 缺结束分隔符：整篇作正文，description 走首段落兜底
    /// （标题行跳过、空行终止、多行空格拼接）。
    #[test]
    fn falls_back_to_first_paragraph_without_frontmatter() {
        let no_fm = parse("# 标题\n第一行\n第二行\n\n第二段");
        assert_eq!(
            no_fm.frontmatter.description.as_deref(),
            Some("第一行 第二行")
        );
        assert_eq!(no_fm.content, "# 标题\n第一行\n第二行\n\n第二段");

        // 起始 `---` 但无结束分隔符 → 同样退化（旧 `indexOf("\n---", 3) == -1`）。
        let unterminated = parse("---\nname: x\n正文");
        assert_eq!(
            unterminated.frontmatter.description.as_deref(),
            Some("--- name: x 正文")
        );
        assert_eq!(unterminated.frontmatter.name, None);

        // frontmatter 有但 description 缺失 → 正文首段落补位。
        let missing_desc = parse("---\nname: x\n---\n只有一段正文。\n");
        assert_eq!(
            missing_desc.frontmatter.description.as_deref(),
            Some("只有一段正文。")
        );
    }

    /// 空/空白输入：默认值（`context=inline`、`shell=bash`、可被用户调用）。
    #[test]
    fn blank_input_yields_defaults() {
        for raw in ["", "   \n\t "] {
            let parsed = parse(raw);
            assert_eq!(parsed.content, "");
            assert_eq!(parsed.frontmatter, FrontmatterData::default());
            assert_eq!(parsed.frontmatter.context, "inline");
            assert_eq!(parsed.frontmatter.shell, "bash");
            assert!(parsed.frontmatter.user_invocable);
            assert!(!parsed.frontmatter.is_fork());
        }
    }

    /// 非 ASCII key 不入表（旧 Java `\w` 为纯 ASCII，见模块文档刻意差异）；
    /// 单标量 `paths` 视作一元列表；不成对引号不剥离。
    #[test]
    fn yaml_edge_cases_match_java_semantics() {
        let fm = parse_yaml("描述: 中文键\npaths: src/**\nversion: \"1.0'");
        assert_eq!(fm.description, None);
        assert_eq!(fm.paths, vec!["src/**".to_owned()]);
        assert_eq!(fm.version.as_deref(), Some("\"1.0'"));
    }

    /// `{{arg}}` 提取去重保序；未提供的参数保留占位符；空参数表原样返回。
    #[test]
    fn substitutes_template_variables() {
        let template = "{{ message }} / {{scope}} / {{message}} / {{missing}}";
        assert_eq!(
            parse_argument_names(template),
            vec![
                "message".to_owned(),
                "scope".to_owned(),
                "missing".to_owned()
            ]
        );
        let rendered = substitute(template, &params(&[("message", "fix"), ("scope", "api")]));
        assert_eq!(rendered, "fix / api / fix / {{missing}}");
        assert_eq!(substitute(template, &BTreeMap::new()), template);
    }

    /// `parseArgs` 三算法：命名参数（`=` 不在首位）、位置参数按 `argDefs`
    /// 顺序且不覆盖已命名、无 `argDefs` 时整串归入 `args`。
    #[test]
    fn parses_named_and_positional_arguments() {
        let defs = vec!["message".to_owned(), "scope".to_owned()];
        // 命名参数优先占位，位置参数补剩余定义。
        let parsed = parse_args("scope=api fix-typo", &defs);
        assert_eq!(parsed.get("scope").map(String::as_str), Some("api"));
        assert_eq!(parsed.get("message").map(String::as_str), Some("fix-typo"));

        // `=` 在首位不算命名参数（旧 `eqIdx > 0`）。
        let leading_eq = parse_args("=weird", &defs);
        assert_eq!(
            leading_eq.get("message").map(String::as_str),
            Some("=weird")
        );

        // 无参数定义 → 整串（空白归一后）进 `args`。
        let joined = parse_args("  多个   词  ", &[]);
        assert_eq!(joined.get("args").map(String::as_str), Some("多个 词"));

        // 空参数串 → 空表。
        assert!(parse_args("   ", &defs).is_empty());
    }
}
