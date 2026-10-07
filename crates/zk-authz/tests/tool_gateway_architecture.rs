//! `ToolGatewayArchitectureTest.java` 的 Rust 源码边界检查适配。
//!
//! 旧测试使用 Spring ASM 检查字节码调用指令；这里扫描 `crates/*/src/**/*.rs`
//! 的已知调用形状和允许文件，防止执行入口在重构中意外绕过既有网关。
//! 这不是 Rust AST、类型或调用图证明：别名、跨行表达式和其他调用写法可能不匹配，
//! 并且按当前文件布局截取首个 `#[cfg(test)]` 之前的文本。真实授权与生命周期
//! 仍由运行时不变量和集成测试验证，不能从本检查通过推出不存在所有执行旁路。
//!
//! 当前检查范围：
//! 1. 已知 `Tool::execute` 调用形状只能出现在 `zk-tools/src/executor.rs`；
//!    已授权 REPL 工具内部的物理解释器端口只允许会话服务 bridge 调用。
//! 2. 原始 `ToolExecutor::spawn_call{,_in}`、宿主 `ExecutionSupervisor` 入口与
//!    server 内独立执行器构造的已知形状受文件边界限制。检查不证明调用点已经
//!    完成 PRE、admission 或持久 invocation 装配，这些由真实管线测试覆盖。
//! 3. Hook 子系统已经实现。保留的源码规则会拒绝同一行同时出现
//!    `HookRegistry` 和 `register(` 的直接注册形状；它不解析 `HookConfig.role`。
//!    当前注册通过完整 `HookConfig`，角色/事件/异步兼容性由 `HookRegistry::register`
//!    校验；反序列化缺省角色为 notification。角色语义、安全拒绝及外部副作用
//!    边界由 Hook 单测与真实执行回归验证，本检查不能替代它们。

use std::path::{Path, PathBuf};

/// `Tool::execute` 唯一合法调用点（旧源 L35-38 的 `ToolExecutionGateway` 位置）。
const TOOL_EXECUTE_CALLER: &str = "crates/zk-tools/src/executor.rs";
/// Raw `ToolExecutor::spawn_call{,_in}` 的唯一合法调用点 + 定义点。
const RAW_SPAWN_CALL_SITES: &[&str] = &[
    "crates/zk-tools/src/executor.rs",
    "crates/zk-engine/src/engine.rs",
    "crates/zk-engine/src/execution_resources.rs",
];
/// Production surfaces allowed to enter the process-wide `ExecutionSupervisor`.
const SUPERVISOR_CALL_SITES: &[&str] = &[
    "crates/zk-server/src/api/mcp_server.rs",
    "crates/zk-server/src/api/verify.rs",
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
        // 按现有文件布局跳过首个测试模块及后续文本；这只是文本范围约定，
        // 不会展开 cfg 或识别测试模块之后可能出现的生产项。
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

            // The native REPL bridge may borrow an admitted Session interpreter,
            // but no API or alternate Tool implementation may invoke that physical port.
            // It never calls Tool::execute a second time or creates another gateway.
            if trimmed.contains(".execute_authorized(")
                && relative != "crates/zk-server/src/repl_service.rs"
            {
                violations.push(format!(
                    "{relative}:{number} bypasses the admitted REPL bridge"
                ));
            }

            // L39-46：原始 ToolExecutor 只能被 Engine 或唯一 Supervisor 封装调用。
            if trimmed.contains("spawn_call")
                && !trimmed.contains("execution_supervisor.spawn_call_in")
                && !RAW_SPAWN_CALL_SITES.contains(&relative.as_str())
            {
                violations.push(format!("{relative}:{number} bypasses ExecutionSupervisor"));
            }

            // API surface 只能调用 AppState 中的 process-wide Supervisor；这里按
            // 精确接收者扫描，避免把任意名为 spawn_call 的旁路整体加入白名单。
            if trimmed.contains("execution_supervisor.spawn_call_in")
                && !SUPERVISOR_CALL_SITES.contains(&relative.as_str())
            {
                violations.push(format!(
                    "{relative}:{number} is not an approved ExecutionSupervisor surface"
                ));
            }

            // server 组装根之外不得再构造独立 ToolExecutor；否则即使调用点经过
            // hook/admission，也会绕过全局并发、资源 owner 和 cleanup 台账。
            if relative.starts_with("crates/zk-server/src/")
                && (trimmed.contains("ToolExecutor::new(")
                    || trimmed.contains("static TOOL_EXECUTOR"))
            {
                violations.push(format!(
                    "{relative}:{number} constructs a process-local ToolExecutor"
                ));
            }

            // 保留旧注册形状哨兵：拒绝同一行直接注册，不解析角色实参。
            // 真实 HookConfig 角色校验和执行行为另由 Hook 系统及其回归负责。
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
