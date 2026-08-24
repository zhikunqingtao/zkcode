//! Bash 词法分析器——手写实现，逐行对照旧 `tool/bash/parser/BashLexer.java`
//! （825 行）。
//!
//! 核心设计（旧源 L8-19 原样保留）：
//! - 双索引追踪：`i`（Java `char` 索引）+ `b`（UTF-8 字节偏移）；
//! - 最长匹配：3 字符 → 2 字符 → 1 字符操作符；
//! - 上下文敏感：`[[ [ { } !` 仅在命令位置识别为操作符；
//! - 引号状态机：单引号 / 双引号 / ANSI-C 引号 / `$()` 嵌套。
//!
//! **Java `char` 语义对齐**：旧源 `source.charAt(i)` 是 UTF-16 code unit，
//! `Character.isHighSurrogate` 分支使 `i += 2`。本移植以 `Vec<u16>`
//! （UTF-16 code unit 序列）承载源码，使索引推进与字节偏移计算与旧实现
//! **逐位等价**；若改用 `Vec<char>` 则代理对分支不可复现。

use std::collections::HashSet;
use std::sync::LazyLock;

use super::ast::{BashToken, BashTokenType};

/// 解析中止原因——对照旧源三个 `RuntimeException` 子类：
/// `BashLexer.LexerBudgetExceededException`（L819）、
/// `BashParserCore.ParserTimeoutException` / `ParserBudgetExceededException`。
///
/// Java 用异常做非局部跳转；Rust 无异常，统一为 `Result` 的 `Err` 变体
/// （语义等价，留痕 §5）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseAbort {
    /// 词法节点预算超出。
    LexerBudgetExceeded(String),
    /// 语法分析超时。
    ParserTimeout(String),
    /// 语法节点预算超出。
    ParserBudgetExceeded(String),
}

/// Shell 关键字——对照旧源 L25-29。
pub static SHELL_KEYWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        "if", "then", "elif", "else", "fi", "while", "until", "for", "in", "do", "done", "case",
        "esac", "function", "select",
    ])
});

/// 声明关键字——对照旧源 L32-34。
pub static DECL_KEYWORDS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| HashSet::from(["export", "declare", "typeset", "readonly", "local"]));

/// 命令起始关键字——对照旧源 L40-44。
static CMD_START_KEYWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        "if", "elif", "then", "else", "while", "until", "do", "fi", "done", "esac",
    ])
});

/// Lexer 级特殊变量（含 `@` 与 `*`）——对照旧源 L47-49。
pub static SPECIAL_VARS: LazyLock<HashSet<char>> =
    LazyLock::new(|| HashSet::from(['?', '$', '@', '*', '#', '-', '!', '_']));

/// 参数扩展变体类型——对照旧源 L766-793（13 个枚举值，顺序一致）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterExpansionType {
    /// 简单变量引用 `${var}`。
    Simple,
    /// 字符串长度 `${#var}`。
    Length,
    /// 默认值 `${var:-default}`。
    DefaultValue,
    /// 赋值默认值 `${var:=default}`。
    AssignDefault,
    /// 替代值 `${var:+alternate}`。
    Alternate,
    /// 错误消息 `${var:?error}`。
    Error,
    /// 最短前缀删除 `${var#pattern}`。
    PrefixShort,
    /// 最长前缀删除 `${var##pattern}`。
    PrefixLong,
    /// 最短后缀删除 `${var%pattern}`。
    SuffixShort,
    /// 最长后缀删除 `${var%%pattern}`。
    SuffixLong,
    /// 首次替换 `${var/pat/repl}`。
    ReplaceFirst,
    /// 全局替换 `${var//pat/repl}`。
    ReplaceAll,
    /// 子串提取 `${var:off:len}`。
    Substring,
}

/// 分类参数扩展类型——对照旧源 `classifyParameterExpansion` L801-814。
///
/// 判定顺序严格照抄（`##` 必须在 `#` 之前、`%%` 在 `%` 之前、`//` 在 `/` 之前）。
/// 旧源 `SUBSTRING` 分支不可达（`${var:off:len}` 落到 `SIMPLE`），本移植原样保留。
#[must_use]
pub fn classify_parameter_expansion(content: &str) -> ParameterExpansionType {
    if content.starts_with('#') {
        return ParameterExpansionType::Length;
    }
    if content.contains(":-") {
        return ParameterExpansionType::DefaultValue;
    }
    if content.contains(":=") {
        return ParameterExpansionType::AssignDefault;
    }
    if content.contains(":+") {
        return ParameterExpansionType::Alternate;
    }
    if content.contains(":?") {
        return ParameterExpansionType::Error;
    }
    if content.contains("##") {
        return ParameterExpansionType::PrefixLong;
    }
    if content.contains('#') {
        return ParameterExpansionType::PrefixShort;
    }
    if content.contains("%%") {
        return ParameterExpansionType::SuffixLong;
    }
    if content.contains('%') {
        return ParameterExpansionType::SuffixShort;
    }
    if content.contains("//") {
        return ParameterExpansionType::ReplaceAll;
    }
    if content.contains('/') {
        return ParameterExpansionType::ReplaceFirst;
    }
    ParameterExpansionType::Simple
}

/// 计算字符串 UTF-8 字节长度——对照旧源 `utf8ByteLength` L621-623。
#[must_use]
pub fn utf8_byte_length(s: &str) -> usize {
    s.len()
}

/// 词法分析器状态快照——对照旧源 `saveLex` L100-102 的 `long` 打包。
///
/// Java 用 `((long) b << 32) | (i & 0xFFFFFFFFL)` 规避溢出；Rust 直接用
/// 二元组承载，语义等价（留痕 §5）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LexState {
    byte_offset: usize,
    index: usize,
}

/// Bash 词法分析器——对照旧源 `BashLexer` L20-824。
pub struct BashLexer {
    /// 源码（UTF-16 code unit 序列，对齐 Java `String.charAt`）。
    units: Vec<u16>,
    /// 源码原文（`getSource()` 用）。
    source: String,
    /// code unit 总数（对照旧源 `length`）。
    length: usize,
    /// Java 字符索引。
    i: usize,
    /// UTF-8 字节偏移。
    b: usize,
    /// 是否在命令起始位置。
    at_cmd_start: bool,
    /// 上一次 `next_token` 前是否跳过了空白。
    had_whitespace_before: bool,
    /// 节点计数（预算控制）。
    node_count: usize,
    /// 节点预算上限。
    max_nodes: usize,
}

/// 非 ASCII code unit 的占位字符——保证与任何 ASCII 比较均不相等。
const NON_ASCII: char = '\u{FFFD}';

impl BashLexer {
    /// 构造 Lexer——对照旧源 L80-87。
    #[must_use]
    pub fn new(source: &str, max_nodes: usize) -> Self {
        let units: Vec<u16> = source.encode_utf16().collect();
        Self {
            length: units.len(),
            units,
            source: source.to_owned(),
            i: 0,
            b: 0,
            at_cmd_start: true,
            had_whitespace_before: true,
            node_count: 0,
            max_nodes,
        }
    }

    /// 默认预算 50000 的构造——对照旧源 L89-91。
    #[must_use]
    pub fn with_default_budget(source: &str) -> Self {
        Self::new(source, 50_000)
    }

    // ──── 状态保存/恢复（回溯机制），旧源 L100-108 ────

    /// 保存当前状态。
    #[must_use]
    pub fn save_lex(&self) -> LexState {
        LexState {
            byte_offset: self.b,
            index: self.i,
        }
    }

    /// 恢复状态。
    pub fn restore_lex(&mut self, state: LexState) {
        self.b = state.byte_offset;
        self.i = state.index;
    }

    // ──── 核心方法 ────

    /// 获取下一个 Token——对照旧源 `nextToken` L118-183。
    ///
    /// # Errors
    ///
    /// 超出节点预算时返回 [`ParseAbort::LexerBudgetExceeded`]（对照旧源
    /// L128-130 抛 `LexerBudgetExceededException`）。
    pub fn next_token(&mut self) -> Result<BashToken, ParseAbort> {
        let before_i = self.i;
        self.skip_blanks();
        self.had_whitespace_before = self.i > before_i;

        if self.i >= self.length {
            return Ok(self.make_token(BashTokenType::Eof, String::new(), self.i, self.i, self.b));
        }

        self.node_count += 1;
        if self.node_count > self.max_nodes {
            return Err(ParseAbort::LexerBudgetExceeded(format!(
                "Node budget exceeded: {}",
                self.max_nodes
            )));
        }

        let start_i = self.i;
        let start_b = self.b;
        let ch = self.char_at(self.i);

        // ──── 换行 ────
        if ch == '\n' {
            self.advance();
            self.at_cmd_start = true;
            return Ok(self.make_token(
                BashTokenType::Newline,
                "\n".to_owned(),
                start_i,
                self.i,
                start_b,
            ));
        }

        // ──── 注释 ────
        if ch == '#' {
            return Ok(self.scan_comment(start_i, start_b));
        }

        // ──── 单引号 ────
        if ch == '\'' {
            return Ok(self.scan_single_quote(start_i, start_b));
        }

        // ──── 双引号 ────
        if ch == '"' {
            return Ok(self.scan_double_quote(start_i, start_b));
        }

        // ──── ANSI-C 引号 $'...' ────
        if ch == '$' && self.peek(1) == '\'' {
            return Ok(self.scan_ansi_c_quote(start_i, start_b));
        }

        // ──── Dollar 展开 ────
        if ch == '$' {
            return Ok(self.scan_dollar(start_i, start_b));
        }

        // ──── 反引号 ────
        if ch == '`' {
            self.advance();
            self.at_cmd_start = false;
            return Ok(self.make_token(
                BashTokenType::Backtick,
                "`".to_owned(),
                start_i,
                self.i,
                start_b,
            ));
        }

        // ──── 操作符（最长匹配） ────
        if let Some(op_token) = self.try_scan_operator(start_i, start_b) {
            return Ok(op_token);
        }

        // ──── 普通单词 ────
        Ok(self.scan_word(start_i, start_b))
    }

    /// 查看当前位置后 `offset` 个字符——对照旧源 `peek` L188-191。
    #[must_use]
    pub fn peek(&self, offset: usize) -> char {
        let idx = self.i + offset;
        if idx < self.length {
            self.char_at(idx)
        } else {
            '\0'
        }
    }

    /// 当前字符——对照旧源 `current` L194-196。
    #[must_use]
    pub fn current(&self) -> char {
        if self.i < self.length {
            self.char_at(self.i)
        } else {
            '\0'
        }
    }

    /// 是否已到末尾——对照旧源 `isAtEnd` L199-201。
    #[must_use]
    pub fn is_at_end(&self) -> bool {
        self.i >= self.length
    }

    /// 当前 Java 字符索引——对照旧源 `getIndex` L204-206。
    #[must_use]
    pub fn index(&self) -> usize {
        self.i
    }

    /// 当前 UTF-8 字节偏移——对照旧源 `getByteOffset` L209-211。
    #[must_use]
    pub fn byte_offset(&self) -> usize {
        self.b
    }

    /// 上一个 Token 之前是否存在空白分隔——对照旧源 L214-216。
    #[must_use]
    pub fn had_whitespace_before(&self) -> bool {
        self.had_whitespace_before
    }

    /// 源码原文——对照旧源 `getSource` L219-221。
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// 源码 code unit 数——对应旧源 `getSource().length()`。
    #[must_use]
    pub fn source_length(&self) -> usize {
        self.length
    }

    /// 子串（按 UTF-16 索引）——对照旧源 `substring` L224-226。
    #[must_use]
    pub fn substring(&self, start: usize, end: usize) -> String {
        let start = start.min(self.length);
        let end = end.min(self.length).max(start);
        String::from_utf16_lossy(&self.units[start..end])
    }

    // ──── 前进方法（UTF-8 双索引），旧源 L241-264 ────

    fn advance(&mut self) {
        if self.i >= self.length {
            return;
        }
        let unit = self.units[self.i];
        if unit < 0x80 {
            self.b += 1;
            self.i += 1;
        } else if unit < 0x800 {
            self.b += 2;
            self.i += 1;
        } else if (0xD800..=0xDBFF).contains(&unit) {
            self.b += 4;
            self.i += 2; // 跳过代理对的两个 char
        } else {
            self.b += 3;
            self.i += 1;
        }
    }

    fn advance_n(&mut self, n: usize) {
        for _ in 0..n {
            self.advance();
        }
    }

    // ──── 跳过空白，旧源 L271-284 ────

    fn skip_blanks(&mut self) {
        while self.i < self.length {
            let ch = self.char_at(self.i);
            if ch == ' ' || ch == '\t' || ch == '\r' {
                self.advance();
            } else if ch == '\\' && self.peek(1) == '\n' {
                // 行继续: \<newline> → 跳过两个字符
                self.advance();
                self.advance();
            } else {
                break;
            }
        }
    }

    // ──── 注释扫描，旧源 L288-295 ────

    fn scan_comment(&mut self, start_i: usize, start_b: usize) -> BashToken {
        while self.i < self.length && self.char_at(self.i) != '\n' {
            self.advance();
        }
        let text = self.substring(start_i, self.i);
        self.at_cmd_start = true;
        self.make_token(BashTokenType::Comment, text, start_i, self.i, start_b)
    }

    // ──── 单引号扫描，旧源 L302-313 ────

    fn scan_single_quote(&mut self, start_i: usize, start_b: usize) -> BashToken {
        self.advance(); // 跳过开引号 '
        while self.i < self.length && self.char_at(self.i) != '\'' {
            self.advance();
        }
        if self.i < self.length {
            self.advance(); // 跳过闭引号 '
        }
        let text = self.substring(start_i, self.i);
        self.at_cmd_start = false;
        self.make_token(BashTokenType::SQuote, text, start_i, self.i, start_b)
    }

    // ──── 双引号扫描，旧源 L322-352 ────

    fn scan_double_quote(&mut self, start_i: usize, start_b: usize) -> BashToken {
        self.advance(); // 跳过开引号 "
        let mut depth = 0_i32; // 嵌套 $() 深度
        while self.i < self.length {
            let ch = self.char_at(self.i);
            if ch == '\\' {
                self.advance();
                if self.i < self.length {
                    self.advance();
                }
                continue;
            }
            if ch == '$' && self.peek(1) == '(' {
                depth += 1;
                self.advance();
                self.advance();
                continue;
            }
            if ch == ')' && depth > 0 {
                depth -= 1;
                self.advance();
                continue;
            }
            if ch == '"' && depth == 0 {
                self.advance(); // 跳过闭引号 "
                break;
            }
            self.advance();
        }
        let text = self.substring(start_i, self.i);
        self.at_cmd_start = false;
        self.make_token(BashTokenType::DQuote, text, start_i, self.i, start_b)
    }

    // ──── ANSI-C 引号扫描，旧源 L359-378 ────

    fn scan_ansi_c_quote(&mut self, start_i: usize, start_b: usize) -> BashToken {
        self.advance(); // 跳过 $
        self.advance(); // 跳过 '
        while self.i < self.length {
            let ch = self.char_at(self.i);
            if ch == '\\' {
                self.advance();
                if self.i < self.length {
                    self.advance();
                }
                continue;
            }
            if ch == '\'' {
                self.advance();
                break;
            }
            self.advance();
        }
        let text = self.substring(start_i, self.i);
        self.at_cmd_start = false;
        self.make_token(BashTokenType::AnsiC, text, start_i, self.i, start_b)
    }

    // ──── Dollar 展开扫描，旧源 L392-440 ────

    fn scan_dollar(&mut self, start_i: usize, start_b: usize) -> BashToken {
        self.advance(); // 跳过 $

        if self.i >= self.length {
            self.at_cmd_start = false;
            return self.make_token(
                BashTokenType::Dollar,
                "$".to_owned(),
                start_i,
                self.i,
                start_b,
            );
        }

        let next = self.char_at(self.i);

        // $(( — 算术扩展（必须在 $( 之前检测）
        if next == '(' && self.peek(1) == '(' {
            return self.lex_arithmetic_expansion(start_i, start_b);
        }

        // $( — 命令替换
        if next == '(' {
            self.advance();
            self.at_cmd_start = true;
            return self.make_token(
                BashTokenType::DollarParen,
                "$(".to_owned(),
                start_i,
                self.i,
                start_b,
            );
        }

        // ${ — 参数扩展变体（完整内容）
        if next == '{' {
            return self.lex_parameter_expansion(start_i, start_b);
        }

        // $SPECIAL_VAR — 特殊变量
        if SPECIAL_VARS.contains(&next) {
            self.advance();
            let text = self.substring(start_i, self.i);
            self.at_cmd_start = false;
            return self.make_token(BashTokenType::Dollar, text, start_i, self.i, start_b);
        }

        // $NAME — 普通变量名
        if is_name_start(next) {
            while self.i < self.length && is_name_char(self.char_at(self.i)) {
                self.advance();
            }
            let text = self.substring(start_i, self.i);
            self.at_cmd_start = false;
            return self.make_token(BashTokenType::Dollar, text, start_i, self.i, start_b);
        }

        // 孤立 $（不跟变量名）
        self.at_cmd_start = false;
        self.make_token(
            BashTokenType::Dollar,
            "$".to_owned(),
            start_i,
            self.i,
            start_b,
        )
    }

    // ──── 操作符扫描（最长匹配优先），旧源 L449-496 ────

    fn try_scan_operator(&mut self, start_i: usize, start_b: usize) -> Option<BashToken> {
        if self.i >= self.length {
            return None;
        }
        let c1 = self.char_at(self.i);
        let c2 = self.peek(1);
        let c3 = self.peek(2);

        // 3 字符操作符
        let op3 = if c2 != '\0' && c3 != '\0' {
            format!("{c1}{c2}{c3}")
        } else {
            String::new()
        };
        if !op3.is_empty() && is_3_char_op(&op3) {
            self.advance_n(3);
            self.update_cmd_start_after_op(&op3);
            return Some(self.make_token(BashTokenType::Op, op3, start_i, self.i, start_b));
        }

        // 2 字符操作符
        let op2 = if c2 == '\0' {
            String::new()
        } else {
            format!("{c1}{c2}")
        };
        if !op2.is_empty() && is_2_char_op(&op2) {
            // 上下文敏感: [[ 仅在命令位置
            if op2 == "[[" && !self.at_cmd_start {
                // 不作为操作符处理，落到下方 1 字符分支
            } else {
                self.advance_n(2);
                self.update_cmd_start_after_op(&op2);
                // <( 和 >( 是进程替换 — 使用完整内容词法分析
                if op2 == "<(" || op2 == ">(" {
                    // 回退 advance_n(2)，由 lex_process_substitution 统一处理
                    self.i = start_i;
                    self.b = start_b;
                    return Some(self.lex_process_substitution(start_i, start_b));
                }
                return Some(self.make_token(BashTokenType::Op, op2, start_i, self.i, start_b));
            }
        }

        // 1 字符操作符
        if is_1_char_op(c1) {
            // 上下文敏感: [ { } ! 仅在命令位置
            if (c1 == '[' || c1 == '{' || c1 == '}' || c1 == '!') && !self.at_cmd_start {
                return None; // 作为 WORD 处理
            }
            self.advance();
            let op1 = c1.to_string();
            self.update_cmd_start_after_op(&op1);
            return Some(self.make_token(BashTokenType::Op, op1, start_i, self.i, start_b));
        }

        None
    }

    /// 操作符后更新命令起始状态——对照旧源 `updateCmdStartAfterOp` L526-534。
    fn update_cmd_start_after_op(&mut self, op: &str) {
        self.at_cmd_start = matches!(
            op,
            "|" | "&&"
                | "||"
                | ";"
                | ";&"
                | ";;&"
                | "|&"
                | "("
                | ")"
                | "))"
                | "{"
                | "!"
                | "[["
                | ";;"
        );
    }

    // ──── 普通单词扫描，旧源 L544-583 ────

    fn scan_word(&mut self, start_i: usize, start_b: usize) -> BashToken {
        while self.i < self.length {
            let ch = self.char_at(self.i);

            // 反斜杠转义
            if ch == '\\' {
                self.advance();
                if self.i < self.length && self.char_at(self.i) != '\n' {
                    self.advance();
                }
                continue;
            }

            // 遇到空白/换行 → 单词结束
            if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
                break;
            }

            // 遇到引号/Dollar/反引号 → 单词结束（拼接由 Parser 层处理）
            if ch == '\'' || ch == '"' || ch == '$' || ch == '`' {
                break;
            }

            // 遇到操作符字符 → 单词结束
            if is_operator_char(ch) {
                break;
            }

            self.advance();
        }

        let text = self.substring(start_i, self.i);

        // 判断是否为纯数字（文件描述符）
        let token_type = if is_all_digits(&text) {
            BashTokenType::Number
        } else {
            BashTokenType::Word
        };

        // Shell 命令起始关键字后面跟的是新命令位置
        self.at_cmd_start = CMD_START_KEYWORDS.contains(text.as_str());
        self.make_token(token_type, text, start_i, self.i, start_b)
    }

    // ──── 扩展语法词法分析，旧源 L638-761 ────

    /// 算术扩展 `$((...))`——对照旧源 `lexArithmeticExpansion` L638-668。
    fn lex_arithmetic_expansion(&mut self, start_i: usize, start_b: usize) -> BashToken {
        self.advance(); // 跳过 (
        self.advance(); // 跳过 (

        let mut depth = 1_i32;

        while self.i < self.length && depth > 0 {
            let ch = self.char_at(self.i);
            if ch == ')' && self.peek(1) == ')' {
                depth -= 1;
                if depth == 0 {
                    self.advance();
                    self.advance();
                    break;
                }
                self.advance();
                self.advance();
            } else if ch == '(' && self.peek(1) == '(' {
                depth += 1;
                self.advance();
                self.advance();
            } else {
                self.advance();
            }
        }

        let text = self.substring(start_i, self.i);
        self.at_cmd_start = false;
        self.make_token(
            BashTokenType::ArithmeticExpansion,
            text,
            start_i,
            self.i,
            start_b,
        )
    }

    /// 参数扩展 `${...}`——对照旧源 `lexParameterExpansion` L693-721。
    fn lex_parameter_expansion(&mut self, start_i: usize, start_b: usize) -> BashToken {
        self.advance(); // 跳过 {

        let mut brace_depth = 1_i32;

        while self.i < self.length && brace_depth > 0 {
            let ch = self.char_at(self.i);
            if ch == '\\' {
                self.advance();
                if self.i < self.length {
                    self.advance();
                }
                continue;
            }
            if ch == '{' {
                brace_depth += 1;
            } else if ch == '}' {
                brace_depth -= 1;
                if brace_depth == 0 {
                    self.advance(); // 跳过闭合 }
                    break;
                }
            }
            self.advance();
        }

        let text = self.substring(start_i, self.i);
        self.at_cmd_start = false;
        self.make_token(
            BashTokenType::ParameterExpansion,
            text,
            start_i,
            self.i,
            start_b,
        )
    }

    /// 进程替换 `<(...)` / `>(...)`——对照旧源 `lexProcessSubstitution` L734-761。
    fn lex_process_substitution(&mut self, start_i: usize, start_b: usize) -> BashToken {
        let prefix = self.char_at(self.i); // < 或 >
        self.advance(); // 跳过 < 或 >
        self.advance(); // 跳过 (

        let mut paren_depth = 1_i32;

        while self.i < self.length && paren_depth > 0 {
            let ch = self.char_at(self.i);
            if ch == '(' {
                paren_depth += 1;
            } else if ch == ')' {
                paren_depth -= 1;
                if paren_depth == 0 {
                    self.advance(); // 跳过闭合 )
                    break;
                }
            }
            self.advance();
        }

        let text = self.substring(start_i, self.i);
        let token_type = if prefix == '<' {
            BashTokenType::ProcessSubstitutionIn
        } else {
            BashTokenType::ProcessSubstitutionOut
        };
        self.at_cmd_start = false;
        self.make_token(token_type, text, start_i, self.i, start_b)
    }

    // ──── 内部辅助 ────

    /// 取指定索引的 code unit 并映射为字符；非 ASCII 一律映射为占位符，
    /// 保证与旧源全部 ASCII 字符比较的结果一致。
    fn char_at(&self, idx: usize) -> char {
        let unit = self.units[idx];
        if unit < 0x80 {
            char::from(u8::try_from(unit).unwrap_or(0))
        } else {
            NON_ASCII
        }
    }

    fn make_token(
        &self,
        token_type: BashTokenType,
        text: String,
        start_i: usize,
        end_i: usize,
        start_b: usize,
    ) -> BashToken {
        BashToken {
            token_type,
            text,
            start_byte: start_b,
            end_byte: self.b,
            start_index: start_i,
            end_index: end_i,
        }
    }
}

/// 3 字符操作符集合——对照旧源 `is3CharOp` L499-504。
fn is_3_char_op(op: &str) -> bool {
    matches!(op, ";;&" | "<<-" | "<<<" | ">&-" | "<&-" | "&>>" | "$((")
}

/// 2 字符操作符集合——对照旧源 `is2CharOp` L507-515。
fn is_2_char_op(op: &str) -> bool {
    matches!(
        op,
        "&&" | "||"
            | "|&"
            | ";;"
            | ";&"
            | ">>"
            | ">&"
            | ">|"
            | "&>"
            | "<<"
            | "<&"
            | "<("
            | ">("
            | "(("
            | "))"
            | "$("
            | "${"
            | "[["
    )
}

/// 1 字符操作符集合——对照旧源 `is1CharOp` L518-523。
fn is_1_char_op(ch: char) -> bool {
    matches!(
        ch,
        '|' | '&' | ';' | '>' | '<' | '(' | ')' | '[' | '{' | '}' | '!'
    )
}

/// 单词断词用操作符字符——对照旧源 `isOperatorChar` L587-595。
///
/// 旧源注释明确：`[ ] { }` 已移除（glob `file[0-9].log` 与大括号展开
/// `{a,b,c}` 需并入 WORD），命令位置的识别交由 `is1CharOp` + `atCmdStart`。
fn is_operator_char(ch: char) -> bool {
    matches!(ch, '|' | '&' | ';' | '>' | '<' | '(' | ')')
}

/// 名称首字符——对照旧源 `isNameStart` L597-599。
#[must_use]
pub fn is_name_start(ch: char) -> bool {
    ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
}

/// 名称后续字符——对照旧源 `isNameChar` L601-603。
#[must_use]
pub fn is_name_char(ch: char) -> bool {
    is_name_start(ch) || ch.is_ascii_digit()
}

/// 是否全为数字——对照旧源 `isAllDigits` L605-611。
fn is_all_digits(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}
