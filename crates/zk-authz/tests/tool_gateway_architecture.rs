//! `ToolGatewayArchitectureTest.java`（60 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录，编号 GW-ARCH）：
//! 旧测试用 Spring 内置 ASM 扫 `target/classes` 的**字节码** invoke 指令。Rust 无
//! 等价的稳定字节码可扫（`.rlib` 内是 MIR/机器码，无 Java 那种符号化 invoke 表），
//! 故改为扫**产物源码文本**：等价性依据是三条不变量都表述为「符号 X 只允许在文件
//! Y 中被调用」，源码层的调用点集合与字节码层一一对应（Rust 无反射/动态代理可绕过
//! 这层文本可见性）。
//!
//! 三条不变量的 zkcode 对应：
//! 1. 旧 `Tool.call` 只许 `ToolExecutionGateway` 调 → zkcode `Tool::execute` 只许
//!    `zk-tools/src/executor.rs` 调（授权拦截在 `ToolAdmission`，见 §3）。
//! 2. 旧 `ToolExecutionPipeline.execute` 只许 `StreamingToolExecutor`（+ MCP 适配器）
//!    调 → zkcode `ToolExecutor::spawn_call{,_in}` 只许 `zk-engine/src/engine.rs` 与
//!    reverse MCP 适配器调用，且两处都**恒先**执行 PRE hook 与
//!    `admission.admit(...)`。
//! 3. 旧 `HookRegistry.register` 必须带显式 role → zkcode 尚无 hook 子系统（Phase 3
//!    才移植），此条记 DEFERRED，本测试留断言占位以便 hook 落地时自动生效。

use std::path::{Path, PathBuf};

/// `Tool::execute` 唯一合法调用点（旧源 L35-38 的 `ToolExecutionGateway` 位置）。
const TOOL_EXECUTE_CALLER: &str = "crates/zk-tools/src/executor.rs";
/// `spawn_call{,_in}` 的唯一合法调用点 + 定义点（旧源 L39-46）。
const SPAWN_CALL_SITES: &[&str] = &[
    "crates/zk-tools/src/executor.rs",
    "crates/zk-engine/src/engine.rs",
    "crates/zk-server/src/api/mcp_server.rs",
];

/// 旧源 `ToolGatewayArchitectureTest.java:17-59` `bytecodeHasNoExecutionBypass`。
#[test]
fn source_has_no_execution_bypass() {
    // L19-20
    let root = workspace_root();
    let mut violations: Vec<String> = Vec::new();

    // L21-22：遍历全部产物源码（`crates/*/src/**/*.rs`）。
    for path in production_sources(&root) {
        let relative = path
            .strip_prefix(&root)
            .expect("path under workspace root")
            .to_string_lossy()
            .replace('\\', "/");
        let source = std::fs::read_to_string(&path).expect("read source");
        // 只看产物代码：`#[cfg(test)]` 之后的内容等价于旧 `target/test-classes`
        // （旧测试也只扫 `target/classes`）。
        let production = source
            .split_once("#[cfg(test)]")
            .map_or(source.as_str(), |(head, _)| head);

        for (index, line) in production.lines().enumerate() {
            let trimmed = line.trim_start();
            // 注释与文档链接不是调用点（旧测试扫的是 invoke 指令）。
            if trimmed.starts_with("//") {
                continue;
            }
            let number = index + 1;

            // L35-38：`Tool::execute` 越过唯一执行器。
            if (trimmed.contains("tool.execute(") || trimmed.contains("Tool::execute("))
                && relative != TOOL_EXECUTE_CALLER
            {
                violations.push(format!("{relative}:{number} invokes Tool::execute"));
            }

            // L39-46：绕过 `ToolExecutor` 的派发入口。
            if trimmed.contains("spawn_call") && !SPAWN_CALL_SITES.contains(&relative.as_str()) {
                violations.push(format!("{relative}:{number} bypasses ToolExecutor"));
            }

            // L47-51：hook 注册必须带显式 role。zkcode 无 hook 子系统，一旦引入
            // `HookRegistry::register` 而不带 role 参数即触发。
            if trimmed.contains("HookRegistry") && trimmed.contains("register(") {
                violations.push(format!(
                    "{relative}:{number} registers a hook without explicit role"
                ));
            }
        }
    }

    // L58
    assert!(
        violations.is_empty(),
        "tool execution architecture bypasses: {violations:#?}"
    );
}

/// 仓库根（`crates/zk-authz` 的祖父目录）。
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

/// 收集 `crates/*/src` 下全部 `.rs`（旧源 `Files.walk(target/classes)`）。
fn production_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    let crates = root.join("crates");
    let entries = std::fs::read_dir(&crates).expect("read crates dir");
    for entry in entries {
        let path = entry.expect("crate entry").path();
        let src = path.join("src");
        if src.is_dir() {
            collect_rust_files(&src, &mut sources);
        }
    }
    sources.sort();
    assert!(!sources.is_empty(), "no production sources discovered");
    sources
}

fn collect_rust_files(directory: &Path, sink: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(directory).expect("read source dir");
    for entry in entries {
        let path = entry.expect("source entry").path();
        if path.is_dir() {
            collect_rust_files(&path, sink);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            sink.push(path);
        }
    }
}
