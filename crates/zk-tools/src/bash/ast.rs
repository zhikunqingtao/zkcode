//! Bash AST 节点层级——唯一权威定义。
//!
//! 逐字对照旧 `tool/bash/ast/BashAstNode.java`（sealed interface + 18 个
//! record）、`ast/BashTokenType.java`（20 个 token 类型）、
//! `parser/BashToken.java`、`ast/ParseForSecurityResult.java`（三态安全结果）。
//!
//! Java `sealed interface` + pattern matching 的穷举性由 Rust `enum` + `match`
//! 天然等价；各 record 尾部重复声明的 `startByte / endByte / rawText` 三件套
//! 统一收敛为 [`Span`]（语义完全一致，留痕 docs/compatibility.md §5）。

use std::fmt;

/// Token 类型——对照旧 `ast/BashTokenType.java` L1-83（20 个枚举值，顺序一致）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BashTokenType {
    /// 普通单词（命令名、参数、路径）。
    Word,
    /// 纯数字（文件描述符）。
    Number,
    /// 操作符。
    Op,
    /// 换行。
    Newline,
    /// 注释。
    Comment,
    /// 双引号串。
    DQuote,
    /// 单引号串。
    SQuote,
    /// ANSI-C 引号 `$'...'`。
    AnsiC,
    /// 简单变量展开 `$VAR` / `$?`。
    Dollar,
    /// 命令替换起始 `$(`。
    DollarParen,
    /// 参数展开起始 `${`。
    DollarBrace,
    /// 算术展开起始 `$((`。
    DollarDParen,
    /// 反引号。
    Backtick,
    /// 进程替换起始 `<(`。
    LtParen,
    /// 进程替换起始 `>(`。
    GtParen,
    /// 完整算术展开 `$((...))`。
    ArithmeticExpansion,
    /// 完整参数展开 `${...}`。
    ParameterExpansion,
    /// 完整输入进程替换 `<(...)`。
    ProcessSubstitutionIn,
    /// 完整输出进程替换 `>(...)`。
    ProcessSubstitutionOut,
    /// 输入结束。
    Eof,
}

/// 词法 Token——对照旧 `parser/BashToken.java` L19-48。
#[derive(Debug, Clone)]
pub struct BashToken {
    /// Token 类型。
    pub token_type: BashTokenType,
    /// 原文。
    pub text: String,
    /// UTF-8 起始字节偏移。
    pub start_byte: usize,
    /// UTF-8 结束字节偏移。
    pub end_byte: usize,
    /// UTF-16 起始索引（对照旧 Java char 索引）。
    pub start_index: usize,
    /// UTF-16 结束索引。
    pub end_index: usize,
}

impl BashToken {
    /// 是否为指定操作符——对照旧 `BashToken.isOp` L29。
    #[must_use]
    pub fn is_op(&self, op: &str) -> bool {
        self.token_type == BashTokenType::Op && self.text == op
    }

    /// 是否为指定单词——对照旧 `BashToken.isWord` L34。
    #[must_use]
    pub fn is_word(&self, word: &str) -> bool {
        self.token_type == BashTokenType::Word && self.text == word
    }

    /// 是否为 EOF——对照旧 `BashToken.isEof` L39。
    #[must_use]
    pub fn is_eof(&self) -> bool {
        self.token_type == BashTokenType::Eof
    }
}

impl fmt::Display for BashToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}('{}')@{}..{}",
            self.token_type, self.text, self.start_byte, self.end_byte
        )
    }
}

/// 节点位置与原文——旧源每个 record 尾部的
/// `int startByte, int endByte, String rawText` 三件套。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Span {
    /// UTF-8 起始字节偏移。
    pub start_byte: usize,
    /// UTF-8 结束字节偏移。
    pub end_byte: usize,
    /// 节点原文。
    pub raw_text: String,
}

impl Span {
    /// 构造 span。
    #[must_use]
    pub fn new(start_byte: usize, end_byte: usize, raw_text: impl Into<String>) -> Self {
        Self {
            start_byte,
            end_byte,
            raw_text: raw_text.into(),
        }
    }
}

/// 变量赋值——对照旧 `BashAstNode.VarAssignment` L84（非 AST 节点）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarAssignment {
    /// 变量名。
    pub name: String,
    /// 变量值。
    pub value: String,
}

/// 重定向——对照旧 `BashAstNode.RedirectNode` L87（非 AST 节点）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedirectNode {
    /// 操作符原文。
    pub operator: String,
    /// 重定向目标。
    pub target: String,
    /// 文件描述符（无显式 fd 时为 -1，对照旧源 `int fd`）。
    pub fd: i32,
}

/// 程序节点——对照旧 `BashAstNode.ProgramNode` L32-36。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgramNode {
    /// 语句列表。
    pub statements: Vec<StatementNode>,
    /// 位置与原文。
    pub span: Span,
}

/// 语句节点——对照旧 `BashAstNode.StatementNode` L38-42。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementNode {
    /// 语句体。
    pub body: Box<BashAstNode>,
    /// 是否后台执行（`&` 结尾）。
    pub is_background: bool,
    /// 位置与原文。
    pub span: Span,
}

/// `case` 分支项——对照旧 `BashAstNode.CaseItem` L123（非 AST 节点）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseItem {
    /// 匹配模式列表。
    pub patterns: Vec<String>,
    /// 分支体。
    pub body: ProgramNode,
}

/// 简单命令节点——对照旧 `BashAstNode.SimpleCommandNode` L76-81。
///
/// 独立成 struct 是因为 [`ParseForSecurityResult::Simple`] 需持有
/// `List<SimpleCommandNode>`（与旧源同构）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleCommandNode {
    /// 参数向量（含 argv[0]）。
    pub argv: Vec<String>,
    /// 前置环境变量赋值。
    pub env_vars: Vec<VarAssignment>,
    /// 重定向列表。
    pub redirects: Vec<RedirectNode>,
    /// 位置与原文。
    pub span: Span,
}

/// Bash AST 节点——对照旧 `BashAstNode.java` sealed interface 的 18 个 record。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BashAstNode {
    /// 程序。
    Program(ProgramNode),
    /// 语句。
    Statement(StatementNode),
    /// 管道——旧 `PipelineNode` L47-51。
    Pipeline {
        /// 各管道阶段。
        commands: Vec<BashAstNode>,
        /// 是否被 `!` 取反。
        negated: bool,
        /// 位置与原文。
        span: Span,
    },
    /// 逻辑与/或——旧 `AndOrNode` L54-59。
    AndOr {
        /// 左操作数。
        left: Box<BashAstNode>,
        /// 操作符（`&&` 或 `||`）。
        operator: String,
        /// 右操作数。
        right: Box<BashAstNode>,
        /// 位置与原文。
        span: Span,
    },
    /// 子 shell `( ... )`——旧 `SubshellNode` L62-65。
    Subshell {
        /// 子程序体。
        body: ProgramNode,
        /// 位置与原文。
        span: Span,
    },
    /// 大括号分组 `{ ... ; }`——旧 `BraceGroupNode` L68-71。
    BraceGroup {
        /// 分组体。
        body: ProgramNode,
        /// 位置与原文。
        span: Span,
    },
    /// 简单命令——旧 `SimpleCommandNode` L76-81。
    SimpleCommand(SimpleCommandNode),
    /// `if` 语句——旧 `IfNode` L92-97。
    If {
        /// 条件。
        condition: ProgramNode,
        /// then 分支。
        then_body: ProgramNode,
        /// else 分支（可空，对照旧源 nullable）。
        else_body: Option<ProgramNode>,
        /// 位置与原文。
        span: Span,
    },
    /// `for` 语句——旧 `ForNode` L100-105。
    For {
        /// 循环变量名。
        var_name: String,
        /// 迭代词表。
        words: Vec<String>,
        /// 循环体。
        body: ProgramNode,
        /// 位置与原文。
        span: Span,
    },
    /// `while` / `until` 语句——旧 `WhileNode` L108-113。
    While {
        /// 条件。
        condition: ProgramNode,
        /// 循环体。
        body: ProgramNode,
        /// 是否为 `until`。
        is_until: bool,
        /// 位置与原文。
        span: Span,
    },
    /// `case` 语句——旧 `CaseNode` L116-120。
    Case {
        /// 被匹配的词。
        word: String,
        /// 分支项。
        items: Vec<CaseItem>,
        /// 位置与原文。
        span: Span,
    },
    /// 函数定义——旧 `FunctionDefNode` L126-130。
    FunctionDef {
        /// 函数名。
        name: String,
        /// 函数体（旧源 `parseCommand()` 可返回 null，故为 `Option`）。
        body: Option<Box<BashAstNode>>,
        /// 位置与原文。
        span: Span,
    },
    /// 声明命令（`export` / `declare` / ...）——旧 `DeclarationCommandNode` L135-140。
    DeclarationCommand {
        /// 声明关键字。
        keyword: String,
        /// 参数向量。
        argv: Vec<String>,
        /// 赋值列表。
        assignments: Vec<VarAssignment>,
        /// 位置与原文。
        span: Span,
    },
    /// 带重定向的复合语句——旧 `RedirectedStatementNode` L145-149。
    RedirectedStatement {
        /// 被包裹的语句体。
        body: Box<BashAstNode>,
        /// 重定向列表。
        redirects: Vec<RedirectNode>,
        /// 位置与原文。
        span: Span,
    },
    /// `!` 否定命令——旧 `NegatedCommandNode` L154-157。
    NegatedCommand {
        /// 被否定的语句体（旧源 `parseCommand()` 可返回 null，故为 `Option`）。
        body: Option<Box<BashAstNode>>,
        /// 位置与原文。
        span: Span,
    },
    /// 测试命令 `[ ... ]` / `[[ ... ]]`——旧 `TestCommandNode` L162-165。
    TestCommand {
        /// 参数向量。
        argv: Vec<String>,
        /// 位置与原文。
        span: Span,
    },
    /// 独立变量赋值——旧 `VariableAssignmentNode` L170-175。
    VariableAssignment {
        /// 变量名。
        name: String,
        /// 变量值。
        value: String,
        /// 是否为 `+=` 追加。
        is_append: bool,
        /// 位置与原文。
        span: Span,
    },
    /// 过于复杂节点——旧 `TooComplexNode` L185-188。
    TooComplex {
        /// 原因。
        reason: String,
        /// 位置与原文。
        span: Span,
    },
}

impl BashAstNode {
    /// 节点 span——统一实现旧 interface 的 `startByte()` / `endByte()` /
    /// `rawText()`（L21-27）。
    #[must_use]
    pub fn span(&self) -> &Span {
        match self {
            Self::Program(node) => &node.span,
            Self::Statement(node) => &node.span,
            Self::SimpleCommand(node) => &node.span,
            Self::Pipeline { span, .. }
            | Self::AndOr { span, .. }
            | Self::Subshell { span, .. }
            | Self::BraceGroup { span, .. }
            | Self::If { span, .. }
            | Self::For { span, .. }
            | Self::While { span, .. }
            | Self::Case { span, .. }
            | Self::FunctionDef { span, .. }
            | Self::DeclarationCommand { span, .. }
            | Self::RedirectedStatement { span, .. }
            | Self::NegatedCommand { span, .. }
            | Self::TestCommand { span, .. }
            | Self::VariableAssignment { span, .. }
            | Self::TooComplex { span, .. } => span,
        }
    }

    /// UTF-8 起始字节偏移。
    #[must_use]
    pub fn start_byte(&self) -> usize {
        self.span().start_byte
    }

    /// UTF-8 结束字节偏移。
    #[must_use]
    pub fn end_byte(&self) -> usize {
        self.span().end_byte
    }

    /// 节点原文。
    #[must_use]
    pub fn raw_text(&self) -> &str {
        &self.span().raw_text
    }
}

/// 安全分析结果三态——对照旧 `ast/ParseForSecurityResult.java` L20-47。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseForSecurityResult {
    /// 解析成功且安全分析通过，可提取可信 argv。
    Simple {
        /// 提取的简单命令列表（叶子节点）。
        commands: Vec<SimpleCommandNode>,
    },
    /// 含危险/复杂结构，需用户确认。
    TooComplex {
        /// 拒绝原因。
        reason: String,
        /// 触发 too-complex 的节点类型。
        node_type: String,
    },
    /// 解析器不可用（超时 / 预算耗尽 / 命令过长）。
    ParseUnavailable,
}
