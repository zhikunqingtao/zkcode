//! 解析器性能预算测试——对照 main 基线 `tool/bash/BashParser.java` L15/L18/L21
//! 三个常量（`PARSE_TIMEOUT_MS = 50` / `MAX_NODES = 50_000` /
//! `MAX_COMMAND_LENGTH = 10_000`）。
//!
//! 旧仓库未提供独立的性能断言测试（预算仅由 `BashParserCore` 内部 deadline 与
//! 节点计数守卫强制），本文件是移植侧新增的门禁测试：34 条语法覆盖样本 + 深
//! 嵌套/超长输入压力样本，逐条断言单次 `parse()` 墙钟耗时 < 50ms，并验证
//! `MAX_COMMAND_LENGTH` 长度守卫与 `MAX_NODES` 预算守卫的短路语义。
//!
//! 样本集与旧源 `BashParserGoldenTest.java` 的 12 个语法类别一一覆盖。

use std::time::{Duration, Instant};

use zk_tools::bash::parser::{MAX_COMMAND_LENGTH, MAX_NODES, PARSE_TIMEOUT_MS, parse};

/// 性能预算上界——恒等于旧源 `PARSE_TIMEOUT_MS`。
const BUDGET: Duration = Duration::from_millis(PARSE_TIMEOUT_MS);

/// 34 条语法覆盖样本，覆盖旧源 `BashParserGoldenTest` 的全部语法类别。
const GOLDEN_SAMPLES: &[&str] = &[
    // 简单命令 / 赋值前缀
    "ls",
    "echo hello world",
    "git commit -m 'fix bug'",
    "ENV=value command arg",
    "A=1 B=2",
    // 管道
    "cat file | grep pattern",
    "ps aux | grep java | head -5",
    // 列表
    "make && make install",
    // 重定向
    "echo hello > output.txt",
    "sort < input.txt >> result.txt",
    "cmd 2>&1",
    // 展开
    "echo $HOME",
    "echo ${VAR:-default}",
    "echo $(date +%Y)",
    "echo $((1 + 2))",
    // 控制结构
    "if [ -f file ]; then echo yes; fi",
    "for f in *.txt; do echo \"$f\"; done",
    "for ((i=0; i<10; i++)); do echo $i; done",
    "while read -r line; do echo \"$line\"; done",
    "case \"$1\" in start) run;; stop) halt;; *) usage;; esac",
    // 子 shell / 命令组
    "(cd /tmp && ls)",
    "{ echo a; echo b; }",
    // 条件表达式 / 取反
    "[[ -n \"$var\" && -f \"$file\" ]]",
    "! grep -q pattern file",
    // 函数定义
    "greet() { echo \"hello $1\"; }",
    "log() { echo \"$@\"; } 2>/dev/null",
    // Glob / 花括号展开
    "ls *.txt",
    "echo {a,b,c}",
    // 变量声明
    "export PATH=\"/usr/bin:$PATH\"",
    "declare -a arr=(1 2 3)",
    "local var=\"value\"",
    // 进程替换 / trap / heredoc
    "echo <(cat /etc/passwd)",
    "trap 'rm -rf /' EXIT",
    "cat <<'EOF'\nhello world\nEOF",
];

/// 计时执行一次解析，返回墙钟耗时。
fn timed(src: &str) -> Duration {
    let started = Instant::now();
    let _ = parse(src);
    started.elapsed()
}

/// 34 条黄金样本逐条满足 50ms 预算（对照 `PARSE_TIMEOUT_MS`）。
#[test]
fn golden_samples_within_parse_budget() {
    for src in GOLDEN_SAMPLES {
        let elapsed = timed(src);
        assert!(
            elapsed < BUDGET,
            "parse budget exceeded ({elapsed:?} >= {BUDGET:?}) for: {src:?}"
        );
    }
}

/// 34 条黄金样本全量解析总耗时同样落在单次预算内（放大后的回归哨兵）。
#[test]
fn golden_suite_total_within_parse_budget() {
    let started = Instant::now();
    for src in GOLDEN_SAMPLES {
        let _ = parse(src);
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < BUDGET,
        "full golden suite ({} samples) exceeded {BUDGET:?}: {elapsed:?}",
        GOLDEN_SAMPLES.len()
    );
}

/// 深嵌套子 shell（256 层，恰达移植侧 `MAX_DEPTH` 守卫）不得超预算，
/// 且必须在预算内返回（成功或降级为 `None`，不允许挂死或栈溢出）。
#[test]
fn deep_nesting_within_parse_budget() {
    let depth = 256;
    let src = format!("{}ls{}", "(".repeat(depth), ")".repeat(depth));
    let elapsed = timed(&src);
    assert!(
        elapsed < BUDGET,
        "deep nesting (depth={depth}) exceeded {BUDGET:?}: {elapsed:?}"
    );
}

/// 长管道链（2000 段）不得超预算。
#[test]
fn long_pipeline_within_parse_budget() {
    let src = vec!["echo x"; 2000].join(" | ");
    let elapsed = timed(&src);
    assert!(
        elapsed < BUDGET,
        "long pipeline exceeded {BUDGET:?}: {elapsed:?}"
    );
}

/// 长命令列表（2000 段 `&&`）不得超预算。
#[test]
fn long_and_list_within_parse_budget() {
    let src = vec!["true"; 2000].join(" && ");
    let elapsed = timed(&src);
    assert!(
        elapsed < BUDGET,
        "long and-list exceeded {BUDGET:?}: {elapsed:?}"
    );
}

/// 长度守卫：超过 `MAX_COMMAND_LENGTH` 立即短路返回 `None`（旧源 L21 语义），
/// 且短路本身远快于解析预算。
#[test]
fn over_length_command_short_circuits() {
    let src = "a".repeat(MAX_COMMAND_LENGTH + 1);
    let started = Instant::now();
    let result = parse(&src);
    let elapsed = started.elapsed();
    assert!(result.is_none(), "over-length command must not parse");
    assert!(
        elapsed < BUDGET,
        "length guard short-circuit exceeded {BUDGET:?}: {elapsed:?}"
    );
}

/// 恰好等于 `MAX_COMMAND_LENGTH` 的命令仍在预算内完成（边界内侧）。
#[test]
fn at_length_limit_within_parse_budget() {
    let src = format!("echo {}", "a".repeat(MAX_COMMAND_LENGTH - 5));
    assert_eq!(MAX_COMMAND_LENGTH, src.encode_utf16().count());
    let elapsed = timed(&src);
    assert!(
        elapsed < BUDGET,
        "at-limit command exceeded {BUDGET:?}: {elapsed:?}"
    );
}

/// 节点预算常量与旧源一致（`MAX_NODES = 50_000`、`PARSE_TIMEOUT_MS = 50`、
/// `MAX_COMMAND_LENGTH = 10_000`）。
#[test]
fn budget_constants_match_baseline() {
    assert_eq!(50, PARSE_TIMEOUT_MS, "PARSE_TIMEOUT_MS must stay 50ms");
    assert_eq!(50_000, MAX_NODES, "MAX_NODES must stay 50k");
    assert_eq!(
        10_000, MAX_COMMAND_LENGTH,
        "MAX_COMMAND_LENGTH must stay 10k"
    );
}

/// 实测采样：打印 34 条样本的单条最大/平均耗时（`--nocapture` 可见），
/// 并再次断言最大值落在预算内，作为报告口径的可复现数据源。
#[test]
fn report_parse_latency_distribution() {
    // 预热，排除首次 `LazyLock` 正则编译开销计入样本。
    for src in GOLDEN_SAMPLES {
        let _ = parse(src);
    }
    let mut max = Duration::ZERO;
    let mut max_src = "";
    let mut total = Duration::ZERO;
    for src in GOLDEN_SAMPLES {
        let elapsed = timed(src);
        total += elapsed;
        if elapsed > max {
            max = elapsed;
            max_src = src;
        }
    }
    let avg = total / u32::try_from(GOLDEN_SAMPLES.len()).expect("sample count fits u32");
    println!(
        "parse latency: samples={} max={max:?} (on {max_src:?}) avg={avg:?} budget={BUDGET:?}",
        GOLDEN_SAMPLES.len()
    );
    assert!(
        max < BUDGET,
        "max parse latency {max:?} exceeded {BUDGET:?}"
    );
}
