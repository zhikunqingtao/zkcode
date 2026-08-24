//! 漂移锁：敏感环境变量清单跨 crate 恒等断言（Step 0-7 / P0-19）。
//!
//! 旧 `service/ToolSafetyGuard.java:200-206` 的 `SENSITIVE_ENV_VARS` 在 Rust 侧
//! 存在两份物理副本，原因是依赖方向铁律禁止 `zk-tools → zk-authz`：
//!
//! - `zk_authz::tool_safety::SENSITIVE_ENV_VARS`：**权威策略源**（纯策略 crate，
//!   供授权/诊断侧读取）；
//! - `zk_tools::process::SENSITIVE_ENV_VARS`：**物理强制点**（子进程 `Command`
//!   构造处无条件 `env_remove`）。
//!
//! zk-server 是唯一同时依赖两者的 crate，故漂移锁落在这里：任一侧增删项都会
//! 让本测试失败，二者永不分叉。

use std::collections::BTreeSet;

/// Java 基线逐字副本（`ToolSafetyGuard.java:200-206`，10 项）。
///
/// 第三份独立抄录是**故意**的：若只比对两份 Rust 副本，两边同时被改错仍会通过。
/// 本表是与 Java 源文本的锚点。
const JAVA_BASELINE: &[&str] = &[
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "NPM_TOKEN",
    "DOCKER_PASSWORD",
    "DATABASE_PASSWORD",
    "DB_PASSWORD",
    "PRIVATE_KEY",
    "SECRET_KEY",
];

fn as_set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

#[test]
fn authz_policy_source_equals_tools_enforcement_point() {
    let policy = as_set(zk_authz::tool_safety::SENSITIVE_ENV_VARS);
    let enforcement = as_set(zk_tools::process::SENSITIVE_ENV_VARS);
    assert_eq!(
        policy, enforcement,
        "SENSITIVE_ENV_VARS drifted between zk-authz (policy) and zk-tools (enforcement)"
    );
}

#[test]
fn both_copies_match_the_java_baseline() {
    let baseline = as_set(JAVA_BASELINE);
    assert_eq!(
        baseline.len(),
        10,
        "Java baseline must hold exactly 10 vars"
    );
    assert_eq!(
        baseline,
        as_set(zk_authz::tool_safety::SENSITIVE_ENV_VARS),
        "zk-authz copy diverged from ToolSafetyGuard.java:200-206"
    );
    assert_eq!(
        baseline,
        as_set(zk_tools::process::SENSITIVE_ENV_VARS),
        "zk-tools copy diverged from ToolSafetyGuard.java:200-206"
    );
}

/// 清单内无重复项（`Set.of` 在重复键上抛异常，Rust slice 不会——显式锁死）。
#[test]
fn copies_contain_no_duplicates() {
    for (label, items) in [
        ("zk-authz", zk_authz::tool_safety::SENSITIVE_ENV_VARS),
        ("zk-tools", zk_tools::process::SENSITIVE_ENV_VARS),
    ] {
        assert_eq!(
            as_set(items).len(),
            items.len(),
            "{label} SENSITIVE_ENV_VARS contains duplicates"
        );
    }
}
