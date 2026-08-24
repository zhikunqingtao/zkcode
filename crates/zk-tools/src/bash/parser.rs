//! Bash 递归下降解析器核心——对照旧源
//! `tool/bash/parser/BashParserCore.java`（1119 行）与
//! `tool/bash/parser/BashParser.java`（51 行）逐行移植。
//!
//! 解析层次（旧源 L13-24 注释）：
//!
//! ```text
//! parse_program()
//!   parse_statements()          // 分号/& /换行分隔的语句序列
//!     parse_and_or()            // && / || 链（左结合）
//!       parse_pipeline()        // | / |& 管道
//!         parse_command()       // 单个命令分派
//!           parse_simple_command()
//!           parse_if / parse_while / parse_for / parse_case
//!           parse_function / parse_declaration
//!           subshell / brace_group / test_command
//! ```
//!
//! Java 用未检查异常（`ParserTimeoutException` / `ParserBudgetExceededException`）
//! 做非局部跳转，Rust 用 [`Result<_, ParseAbort>`] 逐层上抛，语义等价。

use std::time::{Duration, Instant};

use super::ast::{
    BashAstNode, BashToken, BashTokenType, CaseItem, ProgramNode, RedirectNode, SimpleCommandNode,
    Span, StatementNode, VarAssignment,
};
use super::lexer::{BashLexer, ParseAbort, is_name_char, is_name_start};

/// 解析超时（毫秒）——旧 `BashParser.PARSE_TIMEOUT_MS` L15。
pub const PARSE_TIMEOUT_MS: u64 = 50;

/// AST 节点预算上限——旧 `BashParser.MAX_NODES` L18。
pub const MAX_NODES: usize = 50_000;

/// 命令最大长度（字符）——旧 `BashParser.MAX_COMMAND_LENGTH` L21。
pub const MAX_COMMAND_LENGTH: usize = 10_000;

/// 递归下降最大嵌套深度。
///
/// 旧源无此守卫：Java 深递归抛 `StackOverflowError`（`Error` 而非
/// `Exception`，未被 `BashParser.parse` 的 catch 覆盖）。Rust 栈溢出为
/// 进程级 abort，不可恢复，故新增深度守卫并映射到
/// [`ParseAbort::ParserBudgetExceeded`]——与超预算走同一 fail-closed 路径
/// （`parse` 返回 `None` → `ParseUnavailable` → 需用户授权）。
/// 留痕 `docs/compatibility.md` §5 偏离表。
const MAX_DEPTH: usize = 256;

/// 解析 Bash 命令字符串，返回 AST 根节点——对照旧 `BashParser.parse` L29-50。
///
/// 超时 / 预算耗尽 / 命令过长返回 `None`（旧源同为 `null` →
/// `PARSE_ABORTED`）。
#[must_use]
pub fn parse(source: &str) -> Option<ProgramNode> {
    // 旧源 L30-33：null 或空串 → 空 ProgramNode。
    if source.is_empty() {
        return Some(ProgramNode {
            statements: Vec::new(),
            span: Span::new(0, 0, ""),
        });
    }

    // 旧源 L35-37：`source.length()` 为 UTF-16 code unit 数。
    if source.encode_utf16().count() > MAX_COMMAND_LENGTH {
        return None;
    }

    // 旧源 L39。
    let deadline = Instant::now() + Duration::from_millis(PARSE_TIMEOUT_MS);

    // 旧源 L41-49：三类中止异常统一降级为 null。
    let lexer = BashLexer::new(source, MAX_NODES);
    let mut core = BashParserCore::new(lexer, deadline, MAX_NODES).ok()?;
    core.parse_program().ok()
}

/// Bash 递归下降解析器核心——对照旧源 `BashParserCore` L27-1118。
pub struct BashParserCore {
    lexer: BashLexer,
    /// 超时截止（旧源 `long deadline`，`System.nanoTime()` 基准）。
    deadline: Instant,
    max_nodes: usize,
    node_count: usize,
    /// 当前 Token（预读一个）——旧源 L35。
    current: BashToken,
    /// 当前递归深度（Rust 侧栈保护，见 [`MAX_DEPTH`]）。
    depth: usize,
}

impl BashParserCore {
    /// 构造解析器并预读首个 Token——对照旧源 L37-43。
    ///
    /// # Errors
    ///
    /// 首个 Token 词法预算超限时返回 [`ParseAbort::LexerBudgetExceeded`]。
    pub fn new(
        mut lexer: BashLexer,
        deadline: Instant,
        max_nodes: usize,
    ) -> Result<Self, ParseAbort> {
        let current = lexer.next_token()?;
        Ok(Self {
            lexer,
            deadline,
            max_nodes,
            node_count: 0,
            current,
            depth: 0,
        })
    }

    // ──── 超时与预算检查，旧源 L47-55 ────

    fn check_budget(&mut self) -> Result<(), ParseAbort> {
        if Instant::now() > self.deadline {
            return Err(ParseAbort::ParserTimeout(
                "Parser timeout exceeded".to_owned(),
            ));
        }
        self.node_count += 1;
        if self.node_count > self.max_nodes {
            return Err(ParseAbort::ParserBudgetExceeded(format!(
                "Node budget exceeded: {}",
                self.max_nodes
            )));
        }
        Ok(())
    }

    /// 进入一层递归（见 [`MAX_DEPTH`]）。
    fn enter(&mut self) -> Result<(), ParseAbort> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(ParseAbort::ParserBudgetExceeded(format!(
                "Recursion depth exceeded: {MAX_DEPTH}"
            )));
        }
        Ok(())
    }

    /// 退出一层递归。
    fn leave(&mut self) {
        self.depth -= 1;
    }

    // ──── Token 操作，旧源 L60-96 ────

    /// 消费当前 Token 并前进——旧源 L60-64。
    fn consume(&mut self) -> Result<BashToken, ParseAbort> {
        let next = self.lexer.next_token()?;
        Ok(std::mem::replace(&mut self.current, next))
    }

    /// 消费指定操作符，不匹配则返回 `None`——旧源 L75-80。
    fn consume_op(&mut self, op: &str) -> Result<Option<BashToken>, ParseAbort> {
        if self.current.is_op(op) {
            Ok(Some(self.consume()?))
        } else {
            Ok(None)
        }
    }

    /// 消费指定关键字，不匹配则返回 `None`——旧源 L83-88。
    fn consume_keyword(&mut self, keyword: &str) -> Result<Option<BashToken>, ParseAbort> {
        if self.current.is_word(keyword) {
            Ok(Some(self.consume()?))
        } else {
            Ok(None)
        }
    }

    /// 跳过换行和注释——旧源 L91-96。
    fn skip_newlines(&mut self) -> Result<(), ParseAbort> {
        while self.current.token_type == BashTokenType::Newline
            || self.current.token_type == BashTokenType::Comment
        {
            self.consume()?;
        }
        Ok(())
    }

    /// 是否为语句终止符——旧源 L99-110。
    fn is_statement_terminator(&self) -> bool {
        if self.current.is_eof() {
            return true;
        }
        if self.current.is_op(")") {
            return true;
        }
        if self.current.is_op("}") {
            return true;
        }
        matches!(
            self.current.text.as_str(),
            "then" | "else" | "elif" | "fi" | "do" | "done" | "esac" | ";;" | ";&" | ";;&"
        )
    }

    // ──── 层 1: parse_program，旧源 L117-129 ────

    /// 解析顶层程序。
    ///
    /// # Errors
    ///
    /// 超时 / 节点预算 / 词法预算超限时返回对应 [`ParseAbort`]。
    pub fn parse_program(&mut self) -> Result<ProgramNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        let stmts = self.parse_statements()?;

        let end_b = self.current.start_byte;
        let end_i = self.current.start_index;
        let raw = self.lexer.substring(start_i, end_i);

        Ok(ProgramNode {
            statements: stmts,
            span: Span::new(start_b, end_b, raw),
        })
    }

    // ──── 层 2: parse_statements，旧源 L137-165 ────

    fn parse_statements(&mut self) -> Result<Vec<StatementNode>, ParseAbort> {
        let mut stmts = Vec::new();

        self.skip_newlines()?;

        while !self.current.is_eof() && !self.is_statement_terminator() {
            self.check_budget()?;

            if let Some(stmt) = self.parse_statement()? {
                stmts.push(stmt);
            }

            // 消费分隔符: ; & \n（旧源 L151-157）
            let mut consumed = false;
            while self.current.is_op(";")
                || self.current.is_op("&")
                || self.current.token_type == BashTokenType::Newline
                || self.current.token_type == BashTokenType::Comment
            {
                consumed = true;
                self.consume()?;
            }

            if !consumed && !self.current.is_eof() && !self.is_statement_terminator() {
                break; // 无分隔符且未结束 → 停止（旧源 L159-161）
            }
        }

        Ok(stmts)
    }

    /// 解析单个语句——旧源 L170-193。
    fn parse_statement(&mut self) -> Result<Option<StatementNode>, ParseAbort> {
        self.check_budget()?;
        self.enter()?;
        let result = self.parse_statement_inner();
        self.leave();
        result
    }

    fn parse_statement_inner(&mut self) -> Result<Option<StatementNode>, ParseAbort> {
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        let Some(body) = self.parse_and_or()? else {
            return Ok(None);
        };

        // 检查是否为后台执行 (&)——旧源 L179-186
        let mut is_background = false;
        if self.current.is_op("&") && !self.current.is_op("&&") && self.current.text == "&" {
            self.consume()?;
            is_background = true;
        }

        let end_b = self.current.start_byte;
        let end_i = self.current.start_index;
        // 旧源 L190：Math.min(endI, lexer.getSource().length())
        let raw = self
            .lexer
            .substring(start_i, end_i.min(self.lexer.source_length()));

        Ok(Some(StatementNode {
            body: Box::new(body),
            is_background,
            span: Span::new(start_b, end_b, raw),
        }))
    }

    // ──── 层 3: parse_and_or，旧源 L201-229 ────

    fn parse_and_or(&mut self) -> Result<Option<BashAstNode>, ParseAbort> {
        self.check_budget()?;

        let Some(mut left) = self.parse_pipeline()? else {
            return Ok(None);
        };

        while self.current.is_op("&&") || self.current.is_op("||") {
            self.check_budget()?;
            let operator = self.current.text.clone();
            self.consume()?;
            self.skip_newlines()?; // && / || 后允许换行（旧源 L211）

            let Some(right) = self.parse_pipeline()? else {
                break;
            };

            let start_byte = left.start_byte();
            let end_byte = right.end_byte();
            // 旧源 L221-224
            let raw = self.lexer.substring(
                self.find_char_index(start_byte),
                self.find_char_index(end_byte)
                    .min(self.lexer.source_length()),
            );
            left = BashAstNode::AndOr {
                left: Box::new(left),
                operator,
                right: Box::new(right),
                span: Span::new(start_byte, end_byte, raw),
            };
        }

        Ok(Some(left))
    }

    // ──── 层 4: parse_pipeline，旧源 L236-282 ────

    fn parse_pipeline(&mut self) -> Result<Option<BashAstNode>, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        // 检查否定 !（旧源 L242-246）
        let mut negated = false;
        if self.current.is_op("!") {
            negated = true;
            self.consume()?;
        }

        let Some(first) = self.parse_command()? else {
            return Ok(None);
        };

        // 检查管道操作符（旧源 L252-261）
        if !self.current.is_op("|") && !self.current.is_op("|&") {
            if negated {
                let end_b = first.end_byte();
                let raw = self.lexer.substring(
                    start_i,
                    self.find_char_index(end_b).min(self.lexer.source_length()),
                );
                return Ok(Some(BashAstNode::NegatedCommand {
                    body: Some(Box::new(first)),
                    span: Span::new(start_b, end_b, raw),
                }));
            }
            return Ok(Some(first));
        }

        // 构建管道（旧源 L264-281）
        let mut commands = vec![first];

        while self.current.is_op("|") || self.current.is_op("|&") {
            self.check_budget()?;
            self.consume()?;
            self.skip_newlines()?; // 管道符后允许换行

            let Some(cmd) = self.parse_command()? else {
                break;
            };
            commands.push(cmd);
        }

        let end_b = commands.last().map_or(0, BashAstNode::end_byte);
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(Some(BashAstNode::Pipeline {
            commands,
            negated,
            span: Span::new(start_b, end_b, raw),
        }))
    }

    // ──── 层 5: parse_command，旧源 L292-356 ────

    fn parse_command(&mut self) -> Result<Option<BashAstNode>, ParseAbort> {
        self.check_budget()?;
        self.enter()?;
        let result = self.parse_command_inner();
        self.leave();
        result
    }

    fn parse_command_inner(&mut self) -> Result<Option<BashAstNode>, ParseAbort> {
        if self.current.is_eof() || self.is_statement_terminator() {
            return Ok(None);
        }

        let text = self.current.text.clone();
        let mut result: Option<BashAstNode> = None;
        let mut is_compound = false;

        // ── 控制流关键字（旧源 L304-320）──
        if self.current.token_type == BashTokenType::Word {
            result = match text.as_str() {
                "if" => {
                    is_compound = true;
                    Some(self.parse_if()?)
                }
                "while" | "until" => {
                    is_compound = true;
                    Some(self.parse_while()?)
                }
                "for" => {
                    is_compound = true;
                    Some(self.parse_for()?)
                }
                "case" => {
                    is_compound = true;
                    Some(self.parse_case()?)
                }
                "select" => {
                    is_compound = true;
                    Some(self.parse_select()?)
                }
                "function" => {
                    is_compound = true;
                    Some(self.parse_function()?)
                }
                "export" | "declare" | "typeset" | "readonly" | "local" => {
                    Some(self.parse_declaration()?)
                }
                _ => {
                    let r = self.parse_simple_command_or_function()?;
                    if matches!(r, Some(BashAstNode::FunctionDef { .. })) {
                        is_compound = true;
                    }
                    r
                }
            };
        }

        // ── 子 shell: ( stmts )（旧源 L323-326）──
        if result.is_none() && self.current.is_op("(") {
            result = Some(self.parse_subshell()?);
            is_compound = true;
        }

        // ── 大括号分组: { stmts }（旧源 L329-332）──
        if result.is_none() && self.current.is_op("{") {
            result = Some(self.parse_brace_group()?);
            is_compound = true;
        }

        // ── 条件测试: [[ expr ]] 或 [ expr ]（旧源 L335-338）──
        if result.is_none() && (self.current.is_op("[[") || self.current.is_op("[")) {
            result = Some(self.parse_test_command()?);
            is_compound = true;
        }

        // ── 否定: !（旧源 L341-343）──
        if result.is_none() && self.current.is_op("!") {
            result = Some(self.parse_negated()?);
        }

        // 默认: 尝试简单命令（旧源 L346-348）
        if result.is_none() {
            result = self.parse_simple_command()?;
        }

        // ── 复合命令/函数定义的尾部重定向（旧源 L351-353）──
        if let Some(node) = result {
            if is_compound && (self.is_redirect_operator() || self.is_fd_redirect_start()) {
                return Ok(Some(self.wrap_with_redirects(node)?));
            }
            return Ok(Some(node));
        }

        Ok(None)
    }

    /// 将复合命令包装为 `RedirectedStatement`——旧源 L363-376。
    fn wrap_with_redirects(&mut self, body: BashAstNode) -> Result<BashAstNode, ParseAbort> {
        let mut redirects = Vec::new();
        while self.is_redirect_operator() || self.is_fd_redirect_start() {
            if let Some(redir) = self.parse_redirect()? {
                redirects.push(redir);
            }
        }
        if redirects.is_empty() {
            return Ok(body);
        }

        let start_byte = body.start_byte();
        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            self.find_char_index(start_byte),
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );
        Ok(BashAstNode::RedirectedStatement {
            body: Box::new(body),
            redirects,
            span: Span::new(start_byte, end_b, raw),
        })
    }

    // ──── 简单命令解析，旧源 L383-455 ────

    fn parse_simple_command(&mut self) -> Result<Option<BashAstNode>, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        let mut env_vars: Vec<VarAssignment> = Vec::new();
        let mut argv: Vec<String> = Vec::new();
        let mut redirects: Vec<RedirectNode> = Vec::new();

        // 1. 前置变量赋值 (VAR=value)（旧源 L393-396）
        while self.is_var_assignment() {
            if let Some(va) = self.parse_var_assignment()? {
                env_vars.push(va);
            }
        }

        // 2. 命令名 + 参数（旧源 L399-423）
        while !self.current.is_eof()
            && !self.is_statement_terminator()
            && !self.is_operator_token()
            && !self.is_redirect_operator()
        {
            if self.is_var_assignment() && argv.is_empty() {
                // 额外的变量赋值
                if let Some(va) = self.parse_var_assignment()? {
                    env_vars.push(va);
                }
                continue;
            }

            // 危险 token 类型 → 直接返回 TooComplex（旧源 L409-418）
            if self.current.token_type == BashTokenType::DollarDParen
                || self.current.token_type == BashTokenType::ArithmeticExpansion
            {
                return Ok(Some(self.parse_too_complex(
                    "arithmetic_expansion",
                    start_b,
                    start_i,
                )?));
            }
            if self.current.token_type == BashTokenType::LtParen
                || self.current.token_type == BashTokenType::GtParen
                || self.current.token_type == BashTokenType::ProcessSubstitutionIn
                || self.current.token_type == BashTokenType::ProcessSubstitutionOut
            {
                return Ok(Some(self.parse_too_complex(
                    "process_substitution",
                    start_b,
                    start_i,
                )?));
            }

            let Some(word) = self.consume_word()? else {
                break;
            };
            argv.push(word);
        }

        // 3. 重定向（旧源 L426-429）
        while self.is_redirect_operator() {
            if let Some(redir) = self.parse_redirect()? {
                redirects.push(redir);
            }
        }

        // 纯变量赋值 (无命令): A=1 B=2（旧源 L432-444）
        if argv.is_empty() && !env_vars.is_empty() {
            let end_b = self.current.start_byte;
            let raw = self.lexer.substring(
                start_i,
                self.find_char_index(end_b).min(self.lexer.source_length()),
            );
            if env_vars.len() == 1 {
                let first = &env_vars[0];
                return Ok(Some(BashAstNode::VariableAssignment {
                    name: first.name.clone(),
                    value: first.value.clone(),
                    is_append: false,
                    span: Span::new(start_b, end_b, raw),
                }));
            }
            // 多个赋值 → 也包装为 SimpleCommand (空 argv)
            return Ok(Some(BashAstNode::SimpleCommand(SimpleCommandNode {
                argv,
                env_vars,
                redirects,
                span: Span::new(start_b, end_b, raw),
            })));
        }

        if argv.is_empty() && env_vars.is_empty() {
            return Ok(None);
        }

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(Some(BashAstNode::SimpleCommand(SimpleCommandNode {
            argv,
            env_vars,
            redirects,
            span: Span::new(start_b, end_b, raw),
        })))
    }

    /// 解析简单命令或函数定义 (`NAME() { ... }`)——旧源 L460-490。
    fn parse_simple_command_or_function(&mut self) -> Result<Option<BashAstNode>, ParseAbort> {
        let saved = self.lexer.save_lex();
        let saved_current = self.current.clone();
        let saved_node_count = self.node_count;

        if self.current.token_type == BashTokenType::Word {
            let name = self.current.text.clone();
            self.consume()?;
            if self.current.is_op("(") {
                self.consume()?;
                if self.current.is_op(")") {
                    self.consume()?;
                    // 确认为函数定义
                    let start_b = saved_current.start_byte;
                    let start_i = saved_current.start_index;
                    let body = self.parse_command()?;
                    let end_b = self.current.start_byte;
                    let raw = self.lexer.substring(
                        start_i,
                        self.find_char_index(end_b).min(self.lexer.source_length()),
                    );
                    return Ok(Some(BashAstNode::FunctionDef {
                        name,
                        body: body.map(Box::new),
                        span: Span::new(start_b, end_b, raw),
                    }));
                }
            }
        }

        // 不是函数定义 → 恢复并解析为简单命令（旧源 L486-489）
        self.lexer.restore_lex(saved);
        self.current = saved_current;
        self.node_count = saved_node_count;
        self.parse_simple_command()
    }

    // ──── 重定向解析，旧源 L497-540 ────

    /// 判断当前是否为重定向操作符——旧源 L497-513。
    fn is_redirect_operator(&self) -> bool {
        if self.current.token_type == BashTokenType::Op {
            return matches!(
                self.current.text.as_str(),
                ">" | ">>"
                    | "<"
                    | "<<"
                    | "<<<"
                    | ">&"
                    | "<&"
                    | "&>"
                    | "&>>"
                    | ">|"
                    | "<<-"
                    | ">&-"
                    | "<&-"
            );
        }
        // 旧源 L507-511：NUMBER 分支为空实现（注释「暂简化处理」），恒 false。
        false
    }

    /// 解析单个重定向——旧源 L518-540。
    fn parse_redirect(&mut self) -> Result<Option<RedirectNode>, ParseAbort> {
        let mut fd: i32 = -1;

        // 可选文件描述符号（旧源 L522-525）
        if self.current.token_type == BashTokenType::Number {
            // 旧源用 Integer.parseInt，溢出抛 NumberFormatException（未被
            // BashParser.parse 捕获）；Rust 溢出降级为 fd = -1（fail-open 到
            // 「无显式 fd」，与旧源无 fd 分支一致），留痕 §5。
            fd = self.current.text.parse::<i32>().unwrap_or(-1);
            self.consume()?;
        }

        // 重定向操作符（旧源 L528-530）
        if !self.is_redirect_operator() {
            return Ok(None);
        }
        let operator = self.current.text.clone();
        self.consume()?;

        // 目标（旧源 L533-537）
        let mut target = String::new();
        if !self.current.is_eof() && self.current.token_type != BashTokenType::Newline {
            target = self.consume_word()?.unwrap_or_default();
        }

        Ok(Some(RedirectNode {
            operator,
            target,
            fd,
        }))
    }

    // ──── 控制流解析，旧源 L547-834 ────

    /// `if` 语句——旧源 L547-584。
    fn parse_if(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.consume_keyword("if")?;
        self.skip_newlines()?;

        let condition = self.parse_inline_program(&["then"])?;
        self.consume_keyword("then")?;
        self.skip_newlines()?;

        let then_body = self.parse_inline_program(&["elif", "else", "fi"])?;

        let mut else_body: Option<ProgramNode> = None;
        if self.consume_keyword("elif")?.is_some() {
            // elif → 递归为嵌套 if（旧源 L563-571）
            let nested_if = self.parse_if_elif_branch()?;
            else_body = Some(wrap_single_statement(nested_if));
        } else if self.consume_keyword("else")?.is_some() {
            self.skip_newlines()?;
            else_body = Some(self.parse_inline_program(&["fi"])?);
        }

        self.consume_keyword("fi")?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::If {
            condition,
            then_body,
            else_body,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// `elif` 分支解析（不消费 `elif` 关键字）——旧源 L587-621。
    fn parse_if_elif_branch(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        self.enter()?;
        let result = self.parse_if_elif_branch_inner();
        self.leave();
        result
    }

    fn parse_if_elif_branch_inner(&mut self) -> Result<BashAstNode, ParseAbort> {
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.skip_newlines()?;
        let condition = self.parse_inline_program(&["then"])?;
        self.consume_keyword("then")?;
        self.skip_newlines()?;

        let then_body = self.parse_inline_program(&["elif", "else", "fi"])?;

        let mut else_body: Option<ProgramNode> = None;
        if self.consume_keyword("elif")?.is_some() {
            let nested = self.parse_if_elif_branch()?;
            else_body = Some(wrap_single_statement(nested));
        } else if self.consume_keyword("else")?.is_some() {
            self.skip_newlines()?;
            else_body = Some(self.parse_inline_program(&["fi"])?);
        }

        self.consume_keyword("fi")?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::If {
            condition,
            then_body,
            else_body,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// `select` 语句——旧源 L627-631：一律标记 `TooComplex`。
    fn parse_select(&mut self) -> Result<BashAstNode, ParseAbort> {
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;
        self.parse_too_complex("select_statement", start_b, start_i)
    }

    /// `while` / `until` 循环——旧源 L636-657。
    fn parse_while(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        let is_until = self.current.text == "until";
        self.consume()?; // while/until
        self.skip_newlines()?;

        let condition = self.parse_inline_program(&["do"])?;
        self.consume_keyword("do")?;
        self.skip_newlines()?;

        let body = self.parse_inline_program(&["done"])?;
        self.consume_keyword("done")?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::While {
            condition,
            body,
            is_until,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// `for` 循环——旧源 L662-707。
    fn parse_for(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.consume_keyword("for")?;
        self.skip_newlines()?;

        // C 风格 for: for (( ... ))（旧源 L671-673）
        if self.current.is_op("((") {
            return self.parse_too_complex("c_style_for_statement", start_b, start_i);
        }

        // 变量名（旧源 L676-677）
        let var_name = self.current.text.clone();
        self.consume()?;

        // 可选: in word+（旧源 L680-690）
        let mut words: Vec<String> = Vec::new();
        self.skip_newlines()?;
        if self.consume_keyword("in")?.is_some() {
            while !self.current.is_eof()
                && !self.current.is_op(";")
                && self.current.token_type != BashTokenType::Newline
                && !self.current.is_word("do")
            {
                let Some(w) = self.consume_word()? else { break };
                words.push(w);
            }
        }

        // 分隔符: ; 或 \n（旧源 L693-694）
        if self.current.is_op(";") {
            self.consume()?;
        }
        self.skip_newlines()?;

        self.consume_keyword("do")?;
        self.skip_newlines()?;

        let body = self.parse_inline_program(&["done"])?;
        self.consume_keyword("done")?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::For {
            var_name,
            words,
            body,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// `case` 语句——旧源 L712-738。
    fn parse_case(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.consume_keyword("case")?;
        let word = self.consume_word()?.unwrap_or_default();
        self.consume_keyword("in")?;
        self.skip_newlines()?;

        let mut items: Vec<CaseItem> = Vec::new();
        while !self.current.is_eof() && !self.current.is_word("esac") {
            self.check_budget()?;
            let item = self.parse_case_item()?;
            items.push(item);
            self.skip_newlines()?;
        }

        self.consume_keyword("esac")?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::Case {
            word,
            items,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// `case` 分支项——旧源 L740-771（旧源恒返回非 null）。
    fn parse_case_item(&mut self) -> Result<CaseItem, ParseAbort> {
        self.check_budget()?;
        self.skip_newlines()?;

        // 可选: ( 前缀
        self.consume_op("(")?;

        // patterns: pattern { '|' pattern }
        let mut patterns: Vec<String> = Vec::new();
        if let Some(p) = self.consume_word()? {
            patterns.push(p);
        }

        while self.current.is_op("|") {
            self.consume()?;
            if let Some(next) = self.consume_word()? {
                patterns.push(next);
            }
        }

        // )
        self.consume_op(")")?;
        self.skip_newlines()?;

        // body
        let body = self.parse_inline_program(&[";;", ";&", ";;&", "esac"])?;

        // 终止符: ;; 或 ;& 或 ;;&
        if self.current.is_op(";;") || self.current.is_op(";&") || self.current.is_op(";;&") {
            self.consume()?;
        }

        Ok(CaseItem { patterns, body })
    }

    /// 函数定义 `function NAME { ... }`——旧源 L776-799。
    fn parse_function(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.consume_keyword("function")?;
        let name = self.current.text.clone();
        self.consume()?;

        // 可选 ()
        if self.current.is_op("(") {
            self.consume()?;
            self.consume_op(")")?;
        }
        self.skip_newlines()?;

        let body = self.parse_command()?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::FunctionDef {
            name,
            body: body.map(Box::new),
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// 声明命令 `export` / `declare` / `local` / `readonly` / `typeset`——旧源 L804-834。
    fn parse_declaration(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        let keyword = self.current.text.clone();
        self.consume()?;

        let mut argv: Vec<String> = vec![keyword.clone()];
        let mut assignments: Vec<VarAssignment> = Vec::new();

        while !self.current.is_eof()
            && !self.is_statement_terminator()
            && !self.is_operator_token()
            && !self.is_redirect_operator()
        {
            if self.is_var_assignment() {
                if let Some(va) = self.parse_var_assignment()? {
                    assignments.push(va);
                }
            } else {
                let Some(w) = self.consume_word()? else { break };
                argv.push(w);
            }
        }

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::DeclarationCommand {
            keyword,
            argv,
            assignments,
            span: Span::new(start_b, end_b, raw),
        })
    }

    // ──── 复合结构解析，旧源 L841-941 ────

    /// 子 shell `( statement_list )`——旧源 L841-858。
    fn parse_subshell(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.consume_op("(")?;
        self.skip_newlines()?;

        let body = self.parse_inline_program(&[")"])?;

        self.consume_op(")")?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::Subshell {
            body,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// 大括号分组 `{ statement_list }`——旧源 L863-880。
    fn parse_brace_group(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.consume_op("{")?;
        self.skip_newlines()?;

        let body = self.parse_inline_program(&["}"])?;

        self.consume_op("}")?;

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::BraceGroup {
            body,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// 条件测试 `[[ expr ]]` / `[ expr ]`——旧源 L885-922。
    fn parse_test_command(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        let is_double = self.current.is_op("[[");
        self.consume()?; // [[ 或 [

        let closer = if is_double { "]]" } else { "]" };
        let mut argv: Vec<String> = Vec::new();

        // 使用 text 匹配而非 isOp 匹配（旧源 L896-910）
        while !self.current.is_eof() && self.current.text != closer {
            if let Some(w) = self.consume_word()? {
                argv.push(w);
            } else if self.current.token_type == BashTokenType::Op {
                argv.push(self.current.text.clone());
                self.consume()?;
            } else {
                break;
            }
        }

        // 消费闭合符（可能是 WORD 或 OP 类型）
        if self.current.text == closer {
            self.consume()?;
        }

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::TestCommand {
            argv,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// 否定命令 `! command`——旧源 L927-941。
    fn parse_negated(&mut self) -> Result<BashAstNode, ParseAbort> {
        self.check_budget()?;
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        self.consume_op("!")?;

        let body = self.parse_command()?;

        let end_b = body
            .as_ref()
            .map_or(self.current.start_byte, BashAstNode::end_byte);
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(BashAstNode::NegatedCommand {
            body: body.map(Box::new),
            span: Span::new(start_b, end_b, raw),
        })
    }

    // ──── 辅助解析方法，旧源 L948-1101 ────

    /// 解析内联 `ProgramNode`——读取语句直到遇到指定终止关键字（旧源 L948-980）。
    fn parse_inline_program(&mut self, terminators: &[&str]) -> Result<ProgramNode, ParseAbort> {
        let start_b = self.current.start_byte;
        let start_i = self.current.start_index;

        let mut stmts: Vec<StatementNode> = Vec::new();

        self.skip_newlines()?;

        while !self.current.is_eof() && !self.is_terminator(terminators) {
            self.check_budget()?;
            if let Some(stmt) = self.parse_statement()? {
                stmts.push(stmt);
            }

            // 消费分隔符
            let mut consumed = false;
            while self.current.is_op(";")
                || self.current.is_op("&")
                || self.current.token_type == BashTokenType::Newline
                || self.current.token_type == BashTokenType::Comment
            {
                consumed = true;
                self.consume()?;
            }

            if !consumed && !self.current.is_eof() && !self.is_terminator(terminators) {
                break;
            }
        }

        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );

        Ok(ProgramNode {
            statements: stmts,
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// 旧源 L982-987。
    fn is_terminator(&self, terminators: &[&str]) -> bool {
        terminators.iter().any(|t| self.current.text == *t)
    }

    /// 消费一个「word」——相邻的引号/变量展开会拼接为一个完整单词（旧源 L993-1005）。
    fn consume_word(&mut self) -> Result<Option<String>, ParseAbort> {
        if !self.is_word_token() {
            return Ok(None);
        }

        let mut sb = String::new();
        sb.push_str(&self.current.text);
        self.consume()?;
        // 继续拼接直接相邻的 Token（无空白分隔）
        while self.is_word_token() && !self.lexer.had_whitespace_before() {
            sb.push_str(&self.current.text);
            self.consume()?;
        }
        Ok(Some(sb))
    }

    /// 判断当前 Token 是否可以构成单词的一部分——旧源 L1008-1014。
    fn is_word_token(&self) -> bool {
        matches!(
            self.current.token_type,
            BashTokenType::Word
                | BashTokenType::Number
                | BashTokenType::SQuote
                | BashTokenType::DQuote
                | BashTokenType::AnsiC
                | BashTokenType::Dollar
                | BashTokenType::DollarParen
                | BashTokenType::DollarBrace
                | BashTokenType::Backtick
                | BashTokenType::ArithmeticExpansion
        )
    }

    /// 判断当前是否为文件描述符 + 重定向（如 `2>`）——旧源 L1020-1024。
    fn is_fd_redirect_start(&self) -> bool {
        if self.current.token_type != BashTokenType::Number {
            return false;
        }
        let ch = self.lexer.current();
        ch == '>' || ch == '<'
    }

    /// 判断当前是否为管道/逻辑操作符 Token——旧源 L1027-1035。
    fn is_operator_token(&self) -> bool {
        if self.current.token_type != BashTokenType::Op {
            return false;
        }
        matches!(
            self.current.text.as_str(),
            "|" | "||"
                | "&&"
                | ";"
                | "&"
                | "|&"
                | "("
                | ")"
                | "{"
                | "}"
                | "[["
                | "]]"
                | ";;"
                | ";&"
                | ";;&"
        )
    }

    /// 判断当前 Token 是否为变量赋值 (`NAME=value`)——旧源 L1038-1050。
    fn is_var_assignment(&self) -> bool {
        if self.current.token_type != BashTokenType::Word {
            return false;
        }
        let text = &self.current.text;
        let Some(eq) = text.find('=') else {
            return false;
        };
        if eq == 0 {
            return false;
        }
        for (k, ch) in text[..eq].chars().enumerate() {
            if k == 0 && !is_name_start(ch) {
                return false;
            }
            if k > 0 && !is_name_char(ch) {
                return false;
            }
        }
        true
    }

    /// 解析变量赋值——旧源 L1053-1070。
    fn parse_var_assignment(&mut self) -> Result<Option<VarAssignment>, ParseAbort> {
        let text = self.current.text.clone();
        let Some(eq) = text.find('=') else {
            return Ok(None);
        };
        let name = text[..eq].to_owned();
        let mut value = text[eq + 1..].to_owned();
        self.consume()?;

        // 值可能跟着引号 Token（旧源 L1061-1067）
        if value.is_empty()
            && self.is_word_token()
            && (self.current.token_type == BashTokenType::SQuote
                || self.current.token_type == BashTokenType::DQuote
                || self.current.token_type == BashTokenType::Dollar)
        {
            value = self.consume_word()?.unwrap_or_default();
        }

        Ok(Some(VarAssignment { name, value }))
    }

    /// `TooComplex` 兜底——消费到语句终止（旧源 L1073-1084）。
    fn parse_too_complex(
        &mut self,
        reason: &str,
        start_b: usize,
        start_i: usize,
    ) -> Result<BashAstNode, ParseAbort> {
        while !self.current.is_eof()
            && !self.is_statement_terminator()
            && self.current.token_type != BashTokenType::Newline
            && !self.current.is_op(";")
        {
            self.consume()?;
        }
        let end_b = self.current.start_byte;
        let raw = self.lexer.substring(
            start_i,
            self.find_char_index(end_b).min(self.lexer.source_length()),
        );
        Ok(BashAstNode::TooComplex {
            reason: reason.to_owned(),
            span: Span::new(start_b, end_b, raw),
        })
    }

    /// 从字节偏移推算字符索引——旧源 L1098-1101 的保守估计（纯 ASCII 时相等）。
    fn find_char_index(&self, byte_offset: usize) -> usize {
        byte_offset.min(self.lexer.source_length())
    }
}

/// 将单个节点包装成只含一条语句的 `ProgramNode`——旧源 L566-570 / L603-607。
fn wrap_single_statement(node: BashAstNode) -> ProgramNode {
    let span = node.span().clone();
    ProgramNode {
        statements: vec![StatementNode {
            body: Box::new(node),
            is_background: false,
            span: span.clone(),
        }],
        span,
    }
}
