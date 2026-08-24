//! Java 字符串 / 正则语义等价工具（内部）。
//!
//! 旧仓库 main@581d407b 为 Java 实现，`String.trim()` / `String.isBlank()` /
//! `String.split()` 与 `java.util.regex` 的 `\s` `\w` `\d` 语义均与 Rust
//! 标准库、`regex` crate 的默认语义存在差异。本模块把差异集中封装，使移植后的
//! 判定分支能够与 Java 源码逐字对齐（留痕 `docs/compatibility.md` §5）。

use regex::Regex;

/// Java 正则 `\s` 的等价字符类（ASCII 语义：`[ \t\n\x0B\f\r]`）。
///
/// Rust `regex` 的 `\s` 为 Unicode 语义，直接使用会放宽匹配面，故所有移植正则
/// 一律改写为本字面量。
pub(crate) const J_S: &str = r"[ \t\n\x0B\x0C\r]";

/// Java 正则 `\S` 的等价字符类。
pub(crate) const J_NS: &str = r"[^ \t\n\x0B\x0C\r]";

/// Java 正则 `\w` 的等价字符类（ASCII 语义：`[a-zA-Z_0-9]`）。
pub(crate) const J_W: &str = r"[a-zA-Z_0-9]";

/// Java 正则 `\d` 的等价字符类（ASCII 语义：`[0-9]`）。
pub(crate) const J_D: &str = r"[0-9]";

/// Java 正则默认模式下 `.` 的等价字符类。
///
/// Java `Pattern` 的 `.` 排除 5 个行终止符（`\n` / `\r` / `\u0085` /
/// `\u2028` / `\u2029`），Rust `regex` 的 `.` 仅排除 `\n`；显式改写以对齐。
pub(crate) const J_DOT: &str = r"[^\n\r\x{85}\x{2028}\x{2029}]";

/// Java 正则 `\s` 所匹配的字符集合。
pub(crate) const fn is_java_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r')
}

/// `Character.isWhitespace(char)` 的等价判定。
///
/// 与 `char::is_whitespace` 的差异：Java 排除不换行空格 `U+00A0` / `U+2007` /
/// `U+202F`，但额外把 `U+001C..U+001F`（文件/组/记录/单元分隔符）视为空白。
pub(crate) const fn is_java_whitespace(c: char) -> bool {
    matches!(
        c,
        '\u{09}'..='\u{0D}'
            | '\u{1C}'..='\u{1F}'
            | '\u{20}'
            | '\u{1680}'
            | '\u{2000}'..='\u{2006}'
            | '\u{2008}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{205F}'
            | '\u{3000}'
    )
}

/// `String.trim()` 的等价实现：剥离首尾所有码位 `<= U+0020` 的字符。
pub(crate) fn java_trim(s: &str) -> &str {
    s.trim_matches(|c: char| c <= '\u{20}')
}

/// `String.isBlank()` 的等价实现（空串为真）。
pub(crate) fn java_is_blank(s: &str) -> bool {
    s.chars().all(is_java_whitespace)
}

/// `String.split(Pattern, 0)` 的等价实现。
///
/// Java 语义要点：
/// 1. 完全不匹配时返回单元素数组 `[s]`（即使 `s` 为空串）；
/// 2. 结果数组尾部的全部空串被移除（`";".split(";")` → 长度 0）；
/// 3. 首部空串保留（`";a".split(";")` → `["", "a"]`）。
pub(crate) fn java_split<'a>(re: &Regex, s: &'a str) -> Vec<&'a str> {
    let mut out: Vec<&str> = Vec::new();
    let mut last = 0usize;
    let mut matched = false;
    for m in re.find_iter(s) {
        if m.start() == m.end() {
            continue; // 本模块不使用零宽分隔模式
        }
        matched = true;
        out.push(&s[last..m.start()]);
        last = m.end();
    }
    if !matched {
        return vec![s];
    }
    out.push(&s[last..]);
    while out.last().is_some_and(|t| t.is_empty()) {
        out.pop();
    }
    out
}

/// `String.split("\\s+")` 的等价实现（Java `\s` 为 ASCII 语义）。
pub(crate) fn java_split_ws(s: &str) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    let mut last = 0usize;
    let mut idx = 0usize;
    let mut matched = false;
    let bytes = s.as_bytes();
    while idx < bytes.len() {
        let c = bytes[idx] as char;
        if bytes[idx] < 0x80 && is_java_space(c) {
            let start = idx;
            while idx < bytes.len() && bytes[idx] < 0x80 && is_java_space(bytes[idx] as char) {
                idx += 1;
            }
            matched = true;
            out.push(&s[last..start]);
            last = idx;
        } else {
            idx += 1;
        }
    }
    if !matched {
        return vec![s];
    }
    out.push(&s[last..]);
    while out.last().is_some_and(|t| t.is_empty()) {
        out.pop();
    }
    out
}

/// `String.substring(int)` 的等价实现（Java 以 UTF-16 码元计数）。
///
/// 旧源多处以 `firstToken.length()` 做偏移；此处按 UTF-16 码元推进以对齐语义。
/// Java 在 `beginIndex > length()` 时抛 `StringIndexOutOfBoundsException`，
/// 移植后退化为返回空串（旧源调用点保证偏移不越界）。
pub(crate) fn java_substring(s: &str, begin_index: usize) -> &str {
    if begin_index == 0 {
        return s;
    }
    let mut units = 0usize;
    for (byte_idx, ch) in s.char_indices() {
        if units >= begin_index {
            return &s[byte_idx..];
        }
        units += ch.len_utf16();
    }
    ""
}
