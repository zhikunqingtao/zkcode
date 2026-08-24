//! Heredoc 提取器——对照旧 `tool/bash/HeredocExtractor.java`（全 197 行，只读权威规格）。
//!
//! 从命令中提取 heredoc 并替换为占位符，供 [`crate::bash::security`] 安全分析。
//!
//! 安全要点（逐条对齐旧源类注释 L12-21）：
//! 1. 跳过引号内的 `<<`；
//! 2. 跳过注释内的 `<<`；
//! 3. 跳过转义的 `<<`；
//! 4. 跳过算术上下文中的 `<<`；
//! 5. 跳过 `$'` 和 `$"` 特殊引用；
//! 6. 行继续符检测；
//! 7. `PST_EOFTOKEN` 提前关闭检测。
//!
//! **索引口径**：旧源用 Java UTF-16 `char` 索引；本移植用 `Vec<char>`（Unicode
//! 标量）索引。二者仅在非 BMP 字符（代理对）上有偏移差，而所有结构字符
//! （`<` `'` `"` `#` `\` `\n`）与分隔符字符集 `\w` 均为 ASCII，判定结果一致
//! （留痕 docs/compatibility.md §5，分类 EQUIVALENT）。

use std::sync::LazyLock;

use regex::Regex;

/// ANSI-C quoting 前置检查正则——对照旧源 L66 `command.matches(".*\\$['\"].*")`。
///
/// 旧源用 `String.matches`（整串匹配）且 `.` 不跨换行，本移植以 `^...$` 锚定
/// 复现同一语义。
static ANSI_C_QUOTING_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^.*\$['"].*$"#).expect("static regex"));

/// Heredoc 信息记录——对照旧源 `HeredocInfo` L29-34。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeredocInfo {
    /// 分隔符词（不含引号）。
    pub delimiter: String,
    /// heredoc 内容。
    pub content: String,
    /// `<<-` 变体（strip leading tabs）。
    pub is_dash: bool,
    /// 引号/转义分隔符（禁止变量展开）。
    pub is_quoted_or_escaped: bool,
}

/// Heredoc 提取结果——对照旧源 `HeredocExtractionResult` L36-39。
///
/// `heredocs` 用 `Vec` 承载有序键值对，复现旧源 `LinkedHashMap` 的插入序
/// （旧源 L89 `new LinkedHashMap<>()`）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeredocExtractionResult {
    /// 占位符替换后的命令（旧源 L168 原样回传 `command`，未做替换）。
    pub processed_command: String,
    /// 占位符 → heredoc 信息。
    pub heredocs: Vec<(String, HeredocInfo)>,
}

/// Heredoc 起始的一次匹配——复现旧源 `HEREDOC_START` L53-55 的分组语义。
///
/// 旧源正则 `(?<!<)<<(?!<)(-)?[ \t]*(?:(['"])(\w+)\2|\\?(\w+))` 同时用到
/// 逆序环视与反向引用 `\2`，Rust `regex` 均不支持，故手写等价字符扫描。
struct HeredocMatch {
    /// 匹配起始（`<<` 的首字符下标）。
    start: usize,
    /// 匹配结束（末字符下一位，等价 `Matcher.end()`）。
    end: usize,
    /// group(1) == `"-"`。
    is_dash: bool,
    /// group(2) 是否存在（引号分隔符）。
    quoted: bool,
    /// group(3) 或 group(4)（分隔符词）。
    delimiter: String,
    /// group(0) 是否含反斜杠。
    has_backslash: bool,
}

/// Java `\w` 语义（ASCII `[A-Za-z0-9_]`，旧源未开 `UNICODE_CHARACTER_CLASS`）。
fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// 在 `units[from..]` 中找下一个 heredoc 起始——等价旧源 `Matcher.find()`。
///
/// 复现 Java 正则的最左匹配语义：逐个候选起点尝试整体匹配，失败则起点 +1。
fn find_heredoc_start(units: &[char], from: usize) -> Option<HeredocMatch> {
    let n = units.len();
    let mut p = from;
    while p < n {
        // `(?<!<)<<(?!<)`
        let two_lt = units[p] == '<' && p + 1 < n && units[p + 1] == '<';
        let prev_ok = p == 0 || units[p - 1] != '<';
        let next_ok = p + 2 >= n || units[p + 2] != '<';
        if two_lt
            && prev_ok
            && next_ok
            && let Some(m) = try_match_at(units, p)
        {
            return Some(m);
        }
        p += 1;
    }
    None
}

/// 在起点 `p` 尝试完整匹配 `HEREDOC_START`。
fn try_match_at(units: &[char], p: usize) -> Option<HeredocMatch> {
    let n = units.len();
    let mut i = p + 2;

    // `(-)?`
    let is_dash = i < n && units[i] == '-';
    if is_dash {
        i += 1;
    }

    // `[ \t]*`
    while i < n && (units[i] == ' ' || units[i] == '\t') {
        i += 1;
    }

    // 分支 1：`(['"])(\w+)\2`
    if i < n && (units[i] == '\'' || units[i] == '"') {
        let quote = units[i];
        let mut j = i + 1;
        while j < n && is_word_char(units[j]) {
            j += 1;
        }
        if j > i + 1 && j < n && units[j] == quote {
            let delimiter: String = units[i + 1..j].iter().collect();
            let end = j + 1;
            return Some(HeredocMatch {
                start: p,
                end,
                is_dash,
                quoted: true,
                delimiter,
                has_backslash: units[p..end].contains(&'\\'),
            });
        }
        // 分支 1 失败：引号字符不属于 `\w`，也不是 `\\`，故分支 2 必然失败。
        return None;
    }

    // 分支 2：`\\?(\w+)`
    let mut j = i;
    if j < n && units[j] == '\\' {
        j += 1;
    }
    let word_start = j;
    while j < n && is_word_char(units[j]) {
        j += 1;
    }
    if j == word_start {
        return None;
    }
    let delimiter: String = units[word_start..j].iter().collect();
    Some(HeredocMatch {
        start: p,
        end: j,
        is_dash,
        quoted: false,
        delimiter,
        has_backslash: units[p..j].contains(&'\\'),
    })
}

/// Heredoc 提取器——对照旧源 `HeredocExtractor` L24-196。
#[derive(Clone, Copy, Debug, Default)]
pub struct HeredocExtractor;

impl HeredocExtractor {
    /// 构造提取器（旧源为无状态 `@Component`，Rust 侧零字段）。
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// 从 heredoc 操作符位置提取所属命令名——对照旧源 `getCommandForHeredoc` L45-49。
    ///
    /// 取 `<<` 前子串的第一个 token（命令名）。
    #[must_use]
    pub fn command_for_heredoc(command: &str, heredoc_start_index: usize) -> String {
        let units: Vec<char> = command.chars().collect();
        let cut = heredoc_start_index.min(units.len());
        let before: String = units[..cut].iter().collect();
        let before = before.trim();
        // Java `"".split("\\s+")` → `[""]`，故空串取首元素得 ""。
        java_split_whitespace(before)
            .first()
            .map_or_else(String::new, |s| (*s).to_owned())
    }

    /// 从命令中提取 heredoc 并替换为占位符——对照旧源 `extract` L60-169。
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn extract(&self, command: &str) -> HeredocExtractionResult {
        if !command.contains("<<") {
            return HeredocExtractionResult {
                processed_command: command.to_owned(),
                heredocs: Vec::new(),
            };
        }

        // 安全前置检查（旧源 L66-72）
        if ANSI_C_QUOTING_RE.is_match(command) {
            // bail: ANSI-C quoting
            return HeredocExtractionResult {
                processed_command: command.to_owned(),
                heredocs: Vec::new(),
            };
        }
        let units: Vec<char> = command.chars().collect();
        let first_heredoc_pos = char_index_of(&units, "<<").unwrap_or(usize::MAX);
        if first_heredoc_pos != usize::MAX
            && first_heredoc_pos > 0
            && units[..first_heredoc_pos].contains(&'`')
        {
            // bail: backtick
            return HeredocExtractionResult {
                processed_command: command.to_owned(),
                heredocs: Vec::new(),
            };
        }

        // ═══ 增量式引号/注释状态扫描器（旧源 L74-115）═══
        let mut in_single_q = false;
        let mut in_double_q = false;
        let mut in_comment = false;
        let mut dq_escape_next = false;
        let mut pending_backslashes: usize = 0;
        let mut scan_pos: usize = 0;

        // ═══ 主循环（旧源 L88-166）═══
        let mut heredocs: Vec<(String, HeredocInfo)> = Vec::new();
        let mut search_from: usize = 0;

        while let Some(m) = find_heredoc_start(&units, search_from) {
            search_from = m.end.max(m.start + 1);
            let start_index = m.start;

            // 推进扫描器到匹配位置（旧源 L96-114）
            let mut i = scan_pos;
            while i < start_index {
                let ch = units[i];
                i += 1;
                if ch == '\n' {
                    in_comment = false;
                }

                if in_single_q {
                    if ch == '\'' {
                        in_single_q = false;
                    }
                    continue;
                }
                if in_double_q {
                    if dq_escape_next {
                        dq_escape_next = false;
                        continue;
                    }
                    if ch == '\\' {
                        dq_escape_next = true;
                        continue;
                    }
                    if ch == '"' {
                        in_double_q = false;
                    }
                    continue;
                }
                if ch == '\\' {
                    pending_backslashes += 1;
                    continue;
                }
                let escaped = pending_backslashes % 2 == 1;
                pending_backslashes = 0;
                if escaped {
                    continue;
                }
                if ch == '\'' {
                    in_single_q = true;
                } else if ch == '"' {
                    in_double_q = true;
                } else if !in_comment && ch == '#' {
                    in_comment = true;
                }
            }
            scan_pos = start_index;

            // 跳过: 引号内/注释内/转义的 `<<`（旧源 L117-120）
            if in_single_q || in_double_q {
                continue;
            }
            if in_comment {
                continue;
            }
            if pending_backslashes % 2 == 1 {
                continue;
            }

            let is_dash = m.is_dash;
            let delimiter = m.delimiter;
            let is_quoted_or_escaped = m.quoted || m.has_backslash;
            let operator_end_index = m.end;

            // 找到逻辑行结尾（跳过引号内的换行）（旧源 L128-129）
            let Some(first_newline_offset) = find_unquoted_newline(&units, operator_end_index)
            else {
                continue;
            };

            // 行继续符检测（`\` + 换行）（旧源 L132-134）
            let same_line = &units[operator_end_index..operator_end_index + first_newline_offset];
            let trailing_bs = count_trailing_backslashes(same_line);
            if trailing_bs % 2 == 1 {
                continue; // 行继续符 → bail
            }

            // 提取 heredoc 内容直到关闭分隔符（旧源 L137-160）
            let content_start = operator_end_index + first_newline_offset + 1;
            if content_start >= units.len() {
                continue;
            }
            let after_newline: String = units[content_start..].iter().collect();
            let content_lines: Vec<&str> = java_split_keep_trailing(&after_newline, '\n');

            let mut closing_line_index: Option<usize> = None;
            for (i, line) in content_lines.iter().enumerate() {
                let check_line: &str = if is_dash {
                    line.trim_start_matches('\t')
                } else {
                    line
                };
                if check_line == delimiter {
                    closing_line_index = Some(i);
                    break;
                }
                // PST_EOFTOKEN 检测（旧源 L151-157）
                if check_line.chars().count() > delimiter.chars().count()
                    && check_line.starts_with(&delimiter)
                {
                    let after_delim = check_line
                        .chars()
                        .nth(delimiter.chars().count())
                        .unwrap_or('\0');
                    if ")}`|&;(<>".contains(after_delim) {
                        closing_line_index = None;
                        break; // bail: 可能的 shell 元字符
                    }
                }
            }

            let Some(closing_line_index) = closing_line_index else {
                continue; // 未找到关闭分隔符
            };

            let content = content_lines[..closing_line_index].join("\n");
            heredocs.push((
                format!("heredoc_{}", heredocs.len()),
                HeredocInfo {
                    delimiter,
                    content,
                    is_dash,
                    is_quoted_or_escaped,
                },
            ));
        }

        HeredocExtractionResult {
            processed_command: command.to_owned(),
            heredocs,
        }
    }

    /// 快速检测命令是否包含 heredoc——对照旧源 `containsHeredoc` L193-195。
    #[must_use]
    pub fn contains_heredoc(command: &str) -> bool {
        let units: Vec<char> = command.chars().collect();
        find_heredoc_start(&units, 0).is_some()
    }
}

/// 找到第一个非引号内的换行符偏移量——对照旧源 `findUnquotedNewline` L172-183。
fn find_unquoted_newline(units: &[char], from: usize) -> Option<usize> {
    let mut inside_single_quote = false;
    let mut inside_double_quote = false;
    let mut k = from;
    while k < units.len() {
        let ch = units[k];
        if inside_single_quote {
            if ch == '\'' {
                inside_single_quote = false;
            }
            k += 1;
            continue;
        }
        if inside_double_quote {
            if ch == '\\' {
                k += 2;
                continue;
            }
            if ch == '"' {
                inside_double_quote = false;
            }
            k += 1;
            continue;
        }
        if ch == '\n' {
            return Some(k - from);
        }
        if ch == '\'' {
            inside_single_quote = true;
        } else if ch == '"' {
            inside_double_quote = true;
        }
        k += 1;
    }
    None
}

/// 计算末尾连续反斜杠数量——对照旧源 `countTrailingBackslashes` L186-190。
fn count_trailing_backslashes(units: &[char]) -> usize {
    let mut count = 0;
    for &c in units.iter().rev() {
        if c == '\\' {
            count += 1;
        } else {
            break;
        }
    }
    count
}

/// 在字符序列中查找子串首次出现的字符下标（等价 Java `String.indexOf`）。
fn char_index_of(units: &[char], needle: &str) -> Option<usize> {
    let pat: Vec<char> = needle.chars().collect();
    if pat.is_empty() {
        return Some(0);
    }
    if units.len() < pat.len() {
        return None;
    }
    (0..=units.len() - pat.len()).find(|&i| units[i..i + pat.len()] == pat[..])
}

/// 等价 Java `s.split("\n", -1)`：保留尾部空串。
fn java_split_keep_trailing(s: &str, sep: char) -> Vec<&str> {
    s.split(sep).collect()
}

/// 等价 Java `s.split("\\s+")`：连续空白视为单个分隔符；串首空白产生前导空元素。
///
/// 调用点均先 `trim()`，故此处仅需处理常规切分（对照旧源 L47）。
fn java_split_whitespace(s: &str) -> Vec<&str> {
    if s.is_empty() {
        return vec![""];
    }
    s.split_whitespace().collect()
}
