//! 测试失败解析器——JUnit / Jest / pytest 测试失败输出。
//!
//! 对齐旧 `engine/correction/TestFailureParser.java`（286 行）。正则逐字移植
//! （`regex` crate），每种框架一个 golden test。pytest 采用旧「主正则 +
//! fallback」双正则策略（方案 D2）。

use std::sync::LazyLock;

use regex::Regex;

use super::ParsedTestFailure;

/// 最大返回失败数量（对齐旧 `MAX_FAILURES`）。
pub const MAX_FAILURES: usize = 5;

// ===== JUnit =====
/// `JUnit` 总结行：`Tests run: N, Failures: N`（旧 `JUNIT_SUMMARY_PATTERN`）。
static JUNIT_SUMMARY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"Tests run: (\d+), Failures: (\d+)").expect("junit summary regex")
});
/// `JUnit` 失败方法名：`methodName(com.pkg.Class)`（旧 `JUNIT_FAILURE_METHOD_PATTERN`）。
static JUNIT_METHOD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\w+)\([\w.]+\)").expect("junit method regex"));
/// `JUnit` 断言：`expected:<xxx> but was:<yyy>`（旧 `JUNIT_ASSERTION_PATTERN`）。
static JUNIT_ASSERTION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"expected:<(.+?)> but was:<(.+?)>").expect("junit assertion regex")
});

// ===== Jest =====
/// Jest 失败标记：`FAIL path/to/file.ts`（旧 `JEST_FAIL_PATTERN`）。
static JEST_FAIL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"FAIL (.+\.tsx?)").expect("jest fail regex"));
/// Jest 测试名：`● Suite > name`（旧 `JEST_TEST_NAME_PATTERN`）。
static JEST_TEST_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"● (.+)").expect("jest test-name regex"));
/// Jest 期望值：`Expected: xxx`（旧 `JEST_EXPECTED_PATTERN`）。
static JEST_EXPECTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"Expected: (.+)").expect("jest expected regex"));
/// Jest 实际值：`Received: xxx`（旧 `JEST_RECEIVED_PATTERN`）。
static JEST_RECEIVED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"Received: (.+)").expect("jest received regex"));

// ===== pytest =====
/// pytest 失败主正则：`^FAILED file::test - `（MULTILINE，旧 `PYTEST_FAILED_PATTERN`）。
static PYTEST_FAILED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^FAILED (.+?) - ").expect("pytest failed regex"));
/// pytest fallback 正则：`FAILED file::test -`（旧 `PYTEST_FAILED_PATTERN_FALLBACK`）。
static PYTEST_FAILED_FALLBACK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"FAILED (.+?)::(.+?) -").expect("pytest fallback regex"));
/// pytest 断言：`AssertionError: message`（旧 `PYTEST_ASSERTION_PATTERN`）。
static PYTEST_ASSERTION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"AssertionError: (.+)").expect("pytest assertion regex"));
/// pytest assert 语句：`assert xxx == yyy`（旧 `PYTEST_ASSERT_EQ_PATTERN`）。
static PYTEST_ASSERT_EQ: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"assert (.+) == (.+)").expect("pytest assert-eq regex"));

/// 解析工具输出中的测试失败。
///
/// 对齐旧 `parse`：`JUnit` → Jest → pytest 依次追加，最多 [`MAX_FAILURES`] 条；
/// 空白输入或全无匹配返回 `None`。
#[must_use]
pub fn parse(tool_output: &str) -> Option<Vec<ParsedTestFailure>> {
    if tool_output.trim().is_empty() {
        return None;
    }

    let mut failures: Vec<ParsedTestFailure> = Vec::new();
    parse_junit(tool_output, &mut failures);
    parse_jest(tool_output, &mut failures);
    parse_pytest(tool_output, &mut failures);

    if failures.is_empty() {
        return None;
    }
    failures.truncate(MAX_FAILURES);
    Some(failures)
}

/// 解析 `JUnit` 测试失败（旧 `parseJUnitFailures`）。
fn parse_junit(output: &str, failures: &mut Vec<ParsedTestFailure>) {
    let Some(summary) = JUNIT_SUMMARY.captures(output) else {
        return;
    };
    let failure_count: u32 = summary[2].parse().unwrap_or(0);
    if failure_count == 0 {
        return;
    }

    let lines: Vec<&str> = output.split('\n').collect();
    for i in 0..lines.len() {
        if failures.len() >= MAX_FAILURES {
            break;
        }
        let Some(method) = JUNIT_METHOD.captures(lines[i]) else {
            continue;
        };
        let test_name = method[1].to_owned();
        let mut expected: Option<String> = None;
        let mut actual: Option<String> = None;
        let mut stack_trace = String::new();

        let upper = (i + 10).min(lines.len().saturating_sub(1));
        for line in lines.iter().take(upper + 1).skip(i) {
            if expected.is_none()
                && let Some(assertion) = JUNIT_ASSERTION.captures(line)
            {
                expected = Some(assertion[1].to_owned());
                actual = Some(assertion[2].to_owned());
            }
            if line.trim().starts_with("at ") {
                stack_trace.push_str(line.trim());
                stack_trace.push('\n');
            }
        }

        if expected.is_some() || !stack_trace.is_empty() {
            failures.push(ParsedTestFailure {
                test_name,
                expected,
                actual,
                stack_trace: stack_trace.trim().to_owned(),
                framework: "junit".to_owned(),
            });
        }
    }
}

/// 解析 Jest 测试失败（旧 `parseJestFailures`）。
fn parse_jest(output: &str, failures: &mut Vec<ParsedTestFailure>) {
    if !JEST_FAIL.is_match(output) {
        return;
    }

    let lines: Vec<&str> = output.split('\n').collect();
    for i in 0..lines.len() {
        if failures.len() >= MAX_FAILURES {
            break;
        }
        let Some(name) = JEST_TEST_NAME.captures(lines[i]) else {
            continue;
        };
        let test_name = name[1].trim().to_owned();
        let mut expected: Option<String> = None;
        let mut actual: Option<String> = None;
        let mut stack_trace = String::new();

        let upper = (i + 15).min(lines.len().saturating_sub(1));
        for line in lines.iter().take(upper + 1).skip(i + 1) {
            if let Some(exp) = JEST_EXPECTED.captures(line) {
                expected = Some(exp[1].trim().to_owned());
            }
            if let Some(rec) = JEST_RECEIVED.captures(line) {
                actual = Some(rec[1].trim().to_owned());
            }
            if line.trim().starts_with("at ") {
                stack_trace.push_str(line.trim());
                stack_trace.push('\n');
            }
        }

        if expected.is_some() || actual.is_some() {
            failures.push(ParsedTestFailure {
                test_name,
                expected,
                actual,
                stack_trace: stack_trace.trim().to_owned(),
                framework: "jest".to_owned(),
            });
        }
    }
}

/// 解析 pytest 测试失败（旧 `parsePytestFailures`，方案 D2 主正则 + fallback）。
fn parse_pytest(output: &str, failures: &mut Vec<ParsedTestFailure>) {
    let before = failures.len();

    for caps in PYTEST_FAILED.captures_iter(output) {
        if failures.len() >= MAX_FAILURES {
            break;
        }
        let (file, test) = split_pytest_line(&caps[1]);
        add_pytest_failure(output, &file, &test, failures);
    }

    // Fallback：主正则无任何新增匹配时退回原正则（§9 风险回滚项）。
    if failures.len() == before {
        for caps in PYTEST_FAILED_FALLBACK.captures_iter(output) {
            if failures.len() >= MAX_FAILURES {
                break;
            }
            add_pytest_failure(output, &caps[1], &caps[2], failures);
        }
    }
}

/// 二次拆分主正则捕获的 `file::test`（旧 `parsePytestLine`）。
///
/// 用首个 `::` 定位分隔符，参数化测试名 `[param]` 中的 `::` 留在 test 部分。
fn split_pytest_line(matched: &str) -> (String, String) {
    match matched.find("::") {
        Some(idx) if idx > 0 => (matched[..idx].to_owned(), matched[idx + 2..].to_owned()),
        _ => (matched.to_owned(), String::new()),
    }
}

/// 查找 pytest 失败对应的断言信息并追加（旧 `addPytestFailure`）。
fn add_pytest_failure(
    output: &str,
    file_name: &str,
    test_name: &str,
    failures: &mut Vec<ParsedTestFailure>,
) {
    let mut expected: Option<String> = None;
    let mut actual: Option<String> = None;
    let mut assertion_message: Option<String> = None;

    for line in output.split('\n') {
        if let Some(assert_eq) = PYTEST_ASSERT_EQ.captures(line) {
            // 旧语义：assert lhs == rhs → expected=rhs, actual=lhs。
            expected = Some(assert_eq[2].trim().to_owned());
            actual = Some(assert_eq[1].trim().to_owned());
            break;
        }
        if let Some(assertion) = PYTEST_ASSERTION.captures(line) {
            assertion_message = Some(assertion[1].trim().to_owned());
        }
    }

    let stack_info = assertion_message.unwrap_or_default();
    failures.push(ParsedTestFailure {
        test_name: format!("{file_name}::{test_name}"),
        expected,
        actual,
        stack_trace: stack_info,
        framework: "pytest".to_owned(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;

    #[test]
    fn blank_output_returns_none() {
        assert!(parse("  \n ").is_none());
        assert!(parse("all tests passed").is_none());
    }

    #[test]
    fn parses_junit_failure() {
        let out = "Tests run: 3, Failures: 1\n\
                   shouldAdd(com.example.CalcTest)\n\
                   expected:<4> but was:<5>\n\
                   \tat com.example.CalcTest.shouldAdd(CalcTest.java:10)";
        let failures = parse(out).expect("junit failure");
        assert_eq!(failures[0].test_name, "shouldAdd");
        assert_eq!(failures[0].expected.as_deref(), Some("4"));
        assert_eq!(failures[0].actual.as_deref(), Some("5"));
        assert_eq!(failures[0].framework, "junit");
    }

    #[test]
    fn junit_zero_failures_ignored() {
        let out = "Tests run: 3, Failures: 0\nshouldAdd(com.example.CalcTest)";
        assert!(parse(out).is_none());
    }

    #[test]
    fn parses_jest_failure() {
        let out = "FAIL src/app.test.ts\n\
                   ● Calculator > adds numbers\n\
                   Expected: 4\n\
                   Received: 5\n\
                   at Object.<anonymous> (src/app.test.ts:10:20)";
        let failures = parse(out).expect("jest failure");
        assert_eq!(failures[0].test_name, "Calculator > adds numbers");
        assert_eq!(failures[0].expected.as_deref(), Some("4"));
        assert_eq!(failures[0].actual.as_deref(), Some("5"));
        assert_eq!(failures[0].framework, "jest");
    }

    #[test]
    fn parses_pytest_failure_primary() {
        let out = "FAILED tests/test_calc.py::test_add[param-1] - assert 5 == 4\n\
                   some other line";
        let failures = parse(out).expect("pytest failure");
        assert_eq!(
            failures[0].test_name,
            "tests/test_calc.py::test_add[param-1]"
        );
        // assert lhs == rhs → expected=rhs(4), actual=lhs(5)。
        assert_eq!(failures[0].expected.as_deref(), Some("4"));
        assert_eq!(failures[0].actual.as_deref(), Some("5"));
        assert_eq!(failures[0].framework, "pytest");
    }

    #[test]
    fn pytest_fallback_regex_used_when_primary_misses() {
        // 无 " - reason" 结尾结构使主正则不命中，触发 fallback。
        let out = "some FAILED tests/test_x.py::test_y - AssertionError: boom";
        let failures = parse(out).expect("pytest fallback");
        assert_eq!(failures[0].test_name, "tests/test_x.py::test_y");
        assert_eq!(failures[0].framework, "pytest");
    }

    #[test]
    fn truncates_to_max_failures() {
        let mut out = String::from("Tests run: 20, Failures: 20\n");
        for i in 0..8 {
            let _ = writeln!(
                out,
                "testCase{i}(com.example.T)\nexpected:<{i}> but was:<{}>",
                i + 1
            );
        }
        let failures = parse(&out).expect("many failures");
        assert_eq!(failures.len(), MAX_FAILURES);
    }
}
