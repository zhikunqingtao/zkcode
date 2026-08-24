//! `Monitor` 工具——系统资源监控（CPU / 内存 / 磁盘 / 进程）。
//!
//! 对照旧 `tool/impl/MonitorTool.java`（230L，只读权威规格）：`category` ∈
//! {all, cpu, memory, disk, jvm} 分派，只读 + 并发安全，经 feature flag
//! `RESOURCE_MONITOR` **执行期**门控（关闭时逐字回
//! `RESOURCE_MONITOR_DISABLED` 校验错误，见 `MonitorTool.java` L83-86）。
//!
//! 差异（留痕 docs/compatibility.md §4）：
//! - 旧 `jvm` 段读 `java.version` / `ManagementFactory.getRuntimeMXBean()` 等
//!   JVM 专属指标，Rust 侧无对应物 → 改为 `process` 段（本进程 PID / 常驻集 +
//!   宿主 OS / 内核版本 / 运行时长）；入参仍**接受** `"jvm"` 值并映射到该段，
//!   旧调用点无需改字；
//! - 旧 `memory` 段的 Heap / Non-Heap 三分（JVM 托管堆）无等价物 → 改为
//!   物理内存 + Swap 两分；
//! - 旧 `disk` 段遍历 `FileSystems.getDefault().getFileStores()`，本实现走
//!   `sysinfo::Disks`（同为「已挂载卷」口径，额外带出挂载点路径）；
//! - 输出体裁由旧 `- key: value` 项目符号改为 Markdown 表格（本批判据要求），
//!   `## <段名>` 标题层级与旧一致。

use std::fmt::Write as _;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;
use sysinfo::{Disks, System};
use zk_core::feature_flags::{self, FeatureFlags};

use crate::input::{bool_or, failure, optional_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// `verbose` 模式下列出的进程条数（按常驻集降序取前 N）。
pub const VERBOSE_PROCESS_LIMIT: usize = 10;

/// 可选的监控分区（旧 `category` 入参枚举）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Category {
    /// 全部分区（旧 `default -> getAllInfo()`）。
    All,
    /// CPU 负载与核数。
    Cpu,
    /// 物理内存与 Swap。
    Memory,
    /// 已挂载卷用量。
    Disk,
    /// 本进程 + 宿主运行时（旧 `jvm` 段的等价物）。
    Process,
}

impl Category {
    /// 解析入参值；未知值按旧 `switch` 的 `default` 分支落 [`Self::All`]。
    fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some("cpu") => Self::Cpu,
            Some("memory") => Self::Memory,
            Some("disk") => Self::Disk,
            // `jvm` 为旧入参字面量，`process` 为本实现的语义名，二者同段。
            Some("jvm" | "process") => Self::Process,
            _ => Self::All,
        }
    }
}

/// 系统资源监控工具（旧 `MonitorTool`，名 `Monitor`）。
///
/// 持 [`FeatureFlags`] 句柄而非布尔快照：旧 `isEnabled("RESOURCE_MONITOR")`
/// 是**每次调用**都问的执行期门（运行时 `setFeatureValue` 立即生效），与
/// `WebBrowser` 那类注册期门语义不同。
pub struct MonitorTool {
    /// 门控标志表（与组合根共享同一实例）。
    flags: Arc<FeatureFlags>,
}

impl MonitorTool {
    /// 以共享标志表装配（生产入口；组合根传 `AppState::feature_flags`）。
    #[must_use]
    pub fn new(flags: Arc<FeatureFlags>) -> Self {
        Self { flags }
    }
}

impl Tool for MonitorTool {
    fn name(&self) -> &'static str {
        "Monitor"
    }

    fn description(&self) -> &'static str {
        "Monitor system resources including CPU, memory, and disk usage."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Resource category to monitor: 'all', 'cpu', 'memory', 'disk', 'process'",
                    "enum": ["all", "cpu", "memory", "disk", "process", "jvm"]
                },
                "verbose": {
                    "type": "boolean",
                    "description": "Append the top 10 processes by resident memory."
                }
            },
            "required": []
        })
    }

    /// 只读工具（旧 `MonitorTool.java:71` `isReadOnly` → `true`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            if !self.flags.is_enabled(feature_flags::RESOURCE_MONITOR) {
                // 文案对齐旧 `MonitorTool.java` L84-85（环境变量名同步为本仓
                // `FeatureFlags` 实际认的两种形态，旧文案的 `application.yml`
                // 一支在 Rust 侧无对应装配面故不复刻）。
                return failure(
                    "RESOURCE_MONITOR_DISABLED",
                    "MonitorTool is disabled. Set environment variable RESOURCE_MONITOR=true \
                     or ZK_FEATURE_RESOURCE_MONITOR=true to enable.",
                );
            }
            let category = Category::parse(optional_str(&input, "category"));
            let verbose = bool_or(&input, "verbose", false);
            ToolOutput::ok(report(category, verbose).await)
        })
    }
}

/// 组装报告正文（分区分派 + `verbose` 追加进程表）。
async fn report(category: Category, verbose: bool) -> String {
    let mut body = String::new();
    if matches!(category, Category::All | Category::Cpu) {
        body.push_str(&cpu_section().await);
    }
    if matches!(category, Category::All | Category::Memory) {
        body.push_str(&memory_section());
    }
    if matches!(category, Category::All | Category::Disk) {
        body.push_str(&disk_section());
    }
    if matches!(category, Category::All | Category::Process) {
        body.push_str(&process_section());
    }
    if verbose {
        body.push_str(&top_processes_section());
    }
    body
}

/// `## CPU` 段——全局占用率需两次采样（`sysinfo` 以差分计算占用）。
async fn cpu_section() -> String {
    let mut sys = System::new();
    sys.refresh_cpu_usage();
    tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
    sys.refresh_cpu_usage();
    let load = System::load_average();
    let logical = sys.cpus().len();

    let mut out = String::from("## CPU\n\n| Metric | Value |\n| --- | --- |\n");
    let _ = writeln!(out, "| Logical cores | {logical} |");
    let _ = writeln!(out, "| Architecture | {} |", std::env::consts::ARCH);
    let _ = writeln!(out, "| Usage | {:.1}% |", sys.global_cpu_usage());
    let _ = writeln!(
        out,
        "| Load average (1 / 5 / 15 min) | {:.2} / {:.2} / {:.2} |",
        load.one, load.five, load.fifteen
    );
    out.push('\n');
    out
}

/// `## Memory` 段——物理内存 + Swap（旧 Heap / Non-Heap 三分的等价物）。
fn memory_section() -> String {
    let mut sys = System::new();
    sys.refresh_memory();
    let total = sys.total_memory();
    let used = sys.used_memory();

    let mut out = String::from("## Memory\n\n| Metric | Value |\n| --- | --- |\n");
    let _ = writeln!(out, "| RAM used | {} |", format_bytes(used));
    let _ = writeln!(out, "| RAM total | {} |", format_bytes(total));
    let _ = writeln!(out, "| RAM usage | {} |", percent(used, total));
    let _ = writeln!(
        out,
        "| RAM available | {} |",
        format_bytes(sys.available_memory())
    );
    let _ = writeln!(out, "| Swap used | {} |", format_bytes(sys.used_swap()));
    let _ = writeln!(out, "| Swap total | {} |", format_bytes(sys.total_swap()));
    out.push('\n');
    out
}

/// `## Disk` 段——逐卷 used / total / 占比 / 挂载点（旧 `FileStore` 遍历）。
fn disk_section() -> String {
    let mut out = String::from(
        "## Disk\n\n| Volume | Used | Total | Usage | Mount |\n| --- | --- | --- | --- | --- |\n",
    );
    let disks = Disks::new_with_refreshed_list();
    let mut rows = 0_usize;
    for disk in &disks {
        let total = disk.total_space();
        // 旧实现 `if (total <= 0) continue;`——伪文件系统（devfs 等）总量为 0。
        if total == 0 {
            continue;
        }
        let used = total.saturating_sub(disk.available_space());
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} |",
            disk.name().to_string_lossy(),
            format_bytes(used),
            format_bytes(total),
            percent(used, total),
            disk.mount_point().display()
        );
        rows += 1;
    }
    if rows == 0 {
        out.push_str("| (none) | - | - | - | - |\n");
    }
    out.push('\n');
    out
}

/// `## Process` 段——本进程 + 宿主运行时（旧 `## JVM` 段的等价物）。
fn process_section() -> String {
    let mut out = String::from("## Process\n\n| Metric | Value |\n| --- | --- |\n");
    let _ = writeln!(out, "| PID | {} |", std::process::id());
    let _ = writeln!(
        out,
        "| OS | {} {} |",
        System::name().unwrap_or_else(|| "unknown".to_owned()),
        System::os_version().unwrap_or_else(|| "unknown".to_owned())
    );
    let _ = writeln!(
        out,
        "| Kernel | {} |",
        System::kernel_version().unwrap_or_else(|| "unknown".to_owned())
    );
    let _ = writeln!(
        out,
        "| Host uptime | {} |",
        format_duration(System::uptime())
    );
    if let Some(usage) = current_process_memory() {
        let _ = writeln!(out, "| Resident set | {} |", format_bytes(usage));
    }
    out.push('\n');
    out
}

/// `## Top Processes` 段（`verbose = true`）——按常驻集降序取前
/// [`VERBOSE_PROCESS_LIMIT`] 条。
fn top_processes_section() -> String {
    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
    let mut ranked: Vec<(String, u32, u64)> = sys
        .processes()
        .values()
        .map(|process| {
            (
                process.name().to_string_lossy().into_owned(),
                process.pid().as_u32(),
                process.memory(),
            )
        })
        .collect();
    // 常驻集降序；同量级时以 PID 升序定序，保证输出跨次运行稳定。
    ranked.sort_unstable_by(|left, right| right.2.cmp(&left.2).then(left.1.cmp(&right.1)));
    ranked.truncate(VERBOSE_PROCESS_LIMIT);

    let mut out =
        String::from("## Top Processes\n\n| PID | Name | Resident set |\n| --- | --- | --- |\n");
    for (name, pid, memory) in ranked {
        let _ = writeln!(out, "| {pid} | {name} | {} |", format_bytes(memory));
    }
    out.push('\n');
    out
}

/// 本进程常驻集（取不到 PID / 进程表缺失时 `None`）。
fn current_process_memory() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    sys.process(pid).map(sysinfo::Process::memory)
}

/// 字节数人类可读化——阈值与小数位**逐字**对齐旧
/// `MonitorTool.formatBytes`（B / KB.1 / MB.1 / GB.2）。
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    reason = "字节量级远低于 f64 精确整数上界；旧实现同为 long / double 提升"
)]
pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes < KIB {
        return format!("{bytes} B");
    }
    if bytes < MIB {
        return format!("{:.1} KB", bytes as f64 / KIB as f64);
    }
    if bytes < GIB {
        return format!("{:.1} MB", bytes as f64 / MIB as f64);
    }
    format!("{:.2} GB", bytes as f64 / GIB as f64)
}

/// 占比文本（分母 0 → `n/a`，避免旧实现 `used / max` 在 `max = -1` 时的
/// `-0.0%` 观感问题）。
#[expect(
    clippy::cast_precision_loss,
    reason = "字节量级远低于 f64 精确整数上界"
)]
fn percent(part: u64, whole: u64) -> String {
    if whole == 0 {
        return "n/a".to_owned();
    }
    format!("{:.1}%", part as f64 / whole as f64 * 100.0)
}

/// 秒数 → `Nd Nh Nm` 形态（旧 `getUptime() / 1000 + " seconds"` 的可读化）。
fn format_duration(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use zk_core::feature_flags::FlagValue;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    /// 出厂 `RESOURCE_MONITOR = false` → 执行期门拒绝（旧 L83-86）。
    #[tokio::test]
    async fn disabled_flag_rejects_with_legacy_error_code() {
        let tool = MonitorTool::new(Arc::new(FeatureFlags::with_defaults()));
        let output = tool.execute(json!({}), ctx()).await;
        assert!(output.is_error);
        assert!(
            output
                .content
                .starts_with("RESOURCE_MONITOR_DISABLED: MonitorTool is disabled."),
            "{}",
            output.content
        );
    }

    /// 运行时打开 flag → 立即放行（旧执行期门语义，非注册期）。
    #[tokio::test]
    async fn runtime_flag_flip_opens_the_gate() {
        let flags = Arc::new(FeatureFlags::with_defaults());
        let tool = MonitorTool::new(Arc::clone(&flags));
        assert!(tool.execute(json!({}), ctx()).await.is_error);

        flags.set_value(feature_flags::RESOURCE_MONITOR, FlagValue::Bool(true));
        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error, "{}", output.content);
        for section in ["## CPU", "## Memory", "## Disk", "## Process"] {
            assert!(output.content.contains(section), "missing {section}");
        }
        assert!(!output.content.contains("## Top Processes"));
    }

    /// `category` 只渲染对应段；`verbose` 追加进程表。
    #[tokio::test]
    async fn category_selects_a_single_section() {
        let flags = Arc::new(FeatureFlags::with_defaults());
        flags.set_value(feature_flags::RESOURCE_MONITOR, FlagValue::Bool(true));
        let tool = MonitorTool::new(flags);

        let output = tool.execute(json!({ "category": "memory" }), ctx()).await;
        assert!(output.content.starts_with("## Memory"));
        assert!(!output.content.contains("## CPU"));
        assert!(output.content.contains("| RAM total |"));

        let verbose = tool
            .execute(json!({ "category": "disk", "verbose": true }), ctx())
            .await;
        assert!(verbose.content.starts_with("## Disk"));
        assert!(verbose.content.contains("## Top Processes"));
    }

    /// 旧 `jvm` 字面量映射到 `process` 段；未知值落 `all`（旧 `default` 分支）。
    #[test]
    fn category_parse_matches_legacy_switch() {
        assert_eq!(Category::parse(Some("cpu")), Category::Cpu);
        assert_eq!(Category::parse(Some("jvm")), Category::Process);
        assert_eq!(Category::parse(Some("process")), Category::Process);
        assert_eq!(Category::parse(Some("bogus")), Category::All);
        assert_eq!(Category::parse(None), Category::All);
    }

    /// 阈值与小数位逐字对齐旧 `formatBytes`。
    #[test]
    fn format_bytes_matches_legacy_thresholds() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(
            format_bytes(3 * 1024 * 1024 * 1024 + 512 * 1024 * 1024),
            "3.50 GB"
        );
    }

    /// 占比与运行时长格式化的边界。
    #[test]
    fn percent_and_duration_formatting() {
        assert_eq!(percent(1, 4), "25.0%");
        assert_eq!(percent(0, 0), "n/a");
        assert_eq!(format_duration(59), "0m");
        assert_eq!(format_duration(3_720), "1h 2m");
        assert_eq!(format_duration(90_061), "1d 1h 1m");
    }
}
