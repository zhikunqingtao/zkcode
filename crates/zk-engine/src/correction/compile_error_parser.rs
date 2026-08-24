//! 编译错误解析器——Java / TypeScript / Python / Rust 编译错误输出。
//!
//! 对齐旧 `engine/correction/CompileErrorParser.java`（149 行）。正则逐字移植
//! （`regex` crate），每种语言一个 golden test。
//!
//! # 与 Java 基线的有意差异（已核对，本 Batch 决策）
//!
//! 新增 **Rust 编译错误**解析（旧仓仅 Java/TS/Python 三种）。Rust 端自身产物
//! 即 `cargo` 输出，故补一条 `error[Ennnn]: <msg>\n  --> <file>:<line>:<col>` 模式；
//! 语言标识为 `"rust"`。其余三种与旧正则逐字一致。

use std::sync::LazyLock;

use regex::Regex;

use super::ParsedError;

/// 最大返回错误数量（对齐旧 `MAX_ERRORS`）。
pub const MAX_ERRORS: usize = 5;

/// Java 编译错误：`file.java:line: error: message`（旧 `JAVA_ERROR_PATTERN`）。
static JAVA_ERROR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(.+\.java):(\d+): error: (.+)").expect("static java error regex")
});

/// TypeScript 编译错误：`file.ts(line,col): error TSxxxx: message`（旧 `TYPESCRIPT_ERROR_PATTERN`）。
static TYPESCRIPT_ERROR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(.+\.tsx?)\((\d+),\d+\): error TS\d+: (.+)")
        .expect("static typescript error regex")
});

/// Python 语法错误首行：`File "file.py", line N`（旧 `PYTHON_ERROR_PATTERN`）。
static PYTHON_ERROR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"File "(.+\.py)", line (\d+)"#).expect("static python error regex")
});

/// Python 错误类型行：`ErrorType: ...` / `SomeException: ...`（旧 `extractPythonErrorMessage`）。
static PYTHON_ERROR_TYPE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[A-Z]\w*(?:Error|Exception):").expect("static python error-type regex")
});

/// Rust 编译错误（本 Batch 新增）：`error[Ennnn]: msg` + 次行 `--> file:line:col`。
static RUST_ERROR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"error\[E\d+\]: (.+)\n\s+--> (.+):(\d+):\d+").expect("static rust error regex")
});

/// 解析工具输出中的编译错误。
///
/// 对齐旧 `parse`：Java → TypeScript → Python → Rust 依次追加，最多 [`MAX_ERRORS`]
/// 条；空白输入或全无匹配返回 `None`。
#[must_use]
pub fn parse(tool_output: &str) -> Option<Vec<ParsedError>> {
    if tool_output.trim().is_empty() {
        return None;
    }

    let mut errors: Vec<ParsedError> = Vec::new();
    parse_java(tool_output, &mut errors);
    parse_typescript(tool_output, &mut errors);
    parse_python(tool_output, &mut errors);
    parse_rust(tool_output, &mut errors);

    if errors.is_empty() {
        return None;
    }
    errors.truncate(MAX_ERRORS);
    Some(errors)
}

/// 解析 Java 编译错误（旧 `parseJavaErrors`）。
fn parse_java(output: &str, errors: &mut Vec<ParsedError>) {
    for caps in JAVA_ERROR.captures_iter(output) {
        if errors.len() >= MAX_ERRORS {
            break;
        }
        let Some(line_number) = caps[2].parse::<u32>().ok() else {
            continue;
        };
        errors.push(ParsedError {
            file_name: caps[1].to_owned(),
            line_number,
            error_message: caps[3].to_owned(),
            language: "java".to_owned(),
        });
    }
}

/// 解析 TypeScript 编译错误（旧 `parseTypeScriptErrors`）。
fn parse_typescript(output: &str, errors: &mut Vec<ParsedError>) {
    for caps in TYPESCRIPT_ERROR.captures_iter(output) {
        if errors.len() >= MAX_ERRORS {
            break;
        }
        let Some(line_number) = caps[2].parse::<u32>().ok() else {
            continue;
        };
        errors.push(ParsedError {
            file_name: caps[1].to_owned(),
            line_number,
            error_message: caps[3].to_owned(),
            language: "typescript".to_owned(),
        });
    }
}

/// 解析 Python 编译/语法错误（旧 `parsePythonErrors` + `extractPythonErrorMessage`）。
fn parse_python(output: &str, errors: &mut Vec<ParsedError>) {
    let lines: Vec<&str> = output.split('\n').collect();
    for (i, line) in lines.iter().enumerate() {
        if errors.len() >= MAX_ERRORS {
            break;
        }
        let Some(caps) = PYTHON_ERROR.captures(line) else {
            continue;
        };
        let Some(line_number) = caps[2].parse::<u32>().ok() else {
            continue;
        };
        if let Some(message) = extract_python_error_message(&lines, i)
            && !message.trim().is_empty()
        {
            errors.push(ParsedError {
                file_name: caps[1].to_owned(),
                line_number,
                error_message: message,
                language: "python".to_owned(),
            });
        }
    }
}

/// 从 Python 错误输出的后续行提取错误消息（旧 `extractPythonErrorMessage`）。
///
/// 先在 `i+1..=i+4` 找首个以大写字母开头且形如 `XxxError:` / `XxxException:` 的
/// 行；否则退回紧随的非空行。
fn extract_python_error_message(lines: &[&str], file_line_index: usize) -> Option<String> {
    let upper = (file_line_index + 4).min(lines.len().saturating_sub(1));
    for line in lines.iter().take(upper + 1).skip(file_line_index + 1) {
        let trimmed = line.trim();
        if PYTHON_ERROR_TYPE.is_match(trimmed) {
            return Some(trimmed.to_owned());
        }
    }
    if let Some(next) = lines.get(file_line_index + 1) {
        let trimmed = next.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }
    None
}

/// 解析 Rust 编译错误（本 Batch 新增，见模块文档）。
fn parse_rust(output: &str, errors: &mut Vec<ParsedError>) {
    for caps in RUST_ERROR.captures_iter(output) {
        if errors.len() >= MAX_ERRORS {
            break;
        }
        let Some(line_number) = caps[3].parse::<u32>().ok() else {
            continue;
        };
        errors.push(ParsedError {
            file_name: caps[2].to_owned(),
            line_number,
            error_message: caps[1].to_owned(),
            language: "rust".to_owned(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;

    #[test]
    fn blank_output_returns_none() {
        assert!(parse("   \n  ").is_none());
        assert!(parse("no errors here, all green").is_none());
    }

    #[test]
    fn parses_java_error() {
        let out = "src/Main.java:42: error: cannot find symbol\n  symbol: variable foo";
        let errors = parse(out).expect("java error");
        assert_eq!(errors[0].file_name, "src/Main.java");
        assert_eq!(errors[0].line_number, 42);
        assert_eq!(errors[0].error_message, "cannot find symbol");
        assert_eq!(errors[0].language, "java");
    }

    #[test]
    fn parses_typescript_error() {
        let out = "src/app.ts(10,5): error TS2345: Argument of type 'string'.";
        let errors = parse(out).expect("ts error");
        assert_eq!(errors[0].file_name, "src/app.ts");
        assert_eq!(errors[0].line_number, 10);
        assert_eq!(errors[0].language, "typescript");
    }

    #[test]
    fn parses_python_error() {
        let out = "File \"main.py\", line 3\n    print(x\n         ^\nSyntaxError: unexpected EOF while parsing";
        let errors = parse(out).expect("python error");
        assert_eq!(errors[0].file_name, "main.py");
        assert_eq!(errors[0].line_number, 3);
        assert_eq!(
            errors[0].error_message,
            "SyntaxError: unexpected EOF while parsing"
        );
        assert_eq!(errors[0].language, "python");
    }

    #[test]
    fn parses_rust_error() {
        let out = "error[E0308]: mismatched types\n  --> src/main.rs:10:5\n   |\n10 |     let x: u32 = \"s\";";
        let errors = parse(out).expect("rust error");
        assert_eq!(errors[0].file_name, "src/main.rs");
        assert_eq!(errors[0].line_number, 10);
        assert_eq!(errors[0].error_message, "mismatched types");
        assert_eq!(errors[0].language, "rust");
    }

    #[test]
    fn truncates_to_max_errors() {
        let mut out = String::new();
        for i in 0..8 {
            let _ = writeln!(out, "src/F{i}.java:{i}: error: boom {i}");
        }
        let errors = parse(&out).expect("many errors");
        assert_eq!(errors.len(), MAX_ERRORS);
    }
}
