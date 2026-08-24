//! metrics facade 的轻量 in-process recorder（§17 预埋，§3.2 Micrometer 的
//! facade 半边）。
//!
//! 设计：打点侧只依赖 `metrics::` facade API（`counter!` / `histogram!`），
//! 本模块提供 `Recorder` 实现并渲染 Prometheus 文本——Phase 2 换正式
//! exporter（如 `metrics-exporter-prometheus`）时打点零改动，仅替换 install。
//!
//! 存储：进程内三张 `Vec<MetricSeries>` 表（`Arc<Mutex>` 共享），注册期经
//! `Key` 建档、句柄原地更新；直方图保留全量样本，渲染时按标准桶累积。
//! 无后台线程、无预聚合——轻量预埋，非高基数生产方案。

use std::fmt::Write as _;
use std::sync::{Arc, Mutex, OnceLock};

use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn};
use metrics::{Key, KeyName, Metadata, SharedString, Unit};

/// Prometheus 标准累积桶（与 `client_golang` 默认 `DefBuckets` 一致，单位秒）。
const BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// 全局共享三张表（`install_once` 装配，句柄与 `render_snapshot` 共享）。
static SHARED: OnceLock<Tables> = OnceLock::new();
/// install 只执行一次的守卫。
static INSTALL: OnceLock<()> = OnceLock::new();

/// 计数器 / 直方图 / 量规三张序列表。
#[derive(Default, Clone)]
struct Tables {
    /// 单调计数器。
    counters: Arc<Mutex<Vec<MetricSeries<u64>>>>,
    /// 样本全量直方图。
    histograms: Arc<Mutex<Vec<MetricSeries<Vec<f64>>>>>,
    /// 即时量规（当前无打点方，预留完整性）。
    gauges: Arc<Mutex<Vec<MetricSeries<f64>>>>,
}

/// 一条时间序列（`name` + 渲染好的标签段）。
struct MetricSeries<V> {
    /// 指标名（无标签部分，`.` 已规约为 `_`）。
    name: String,
    /// 标签段（形如 `method="GET",route="/api/health"`，可为空）。
    labels: String,
    /// 当前值。
    value: V,
}

/// upsert：命中返回既有值引用，未命中以 `initial` 建档。
fn upsert<'a, V>(
    table: &'a mut Vec<MetricSeries<V>>,
    name: &str,
    labels: &str,
    initial: V,
) -> &'a mut V {
    if let Some(index) = table
        .iter()
        .position(|series| series.name == name && series.labels == labels)
    {
        return &mut table[index].value;
    }
    table.push(MetricSeries {
        name: name.to_owned(),
        labels: labels.to_owned(),
        value: initial,
    });
    &mut table
        .last_mut()
        .expect("series just pushed, table non-empty")
        .value
}

/// 轻量 recorder（实现 `metrics::Recorder`）。
struct MetricsRecorder {
    /// 三张共享表。
    tables: Tables,
}

/// 幂等安装（main 与各集成测试二进制进程内各执行一次）。
pub fn install_once() {
    INSTALL.get_or_init(|| {
        if let Err(_err) = metrics::set_global_recorder(MetricsRecorder { tables: tables() }) {
            tracing::debug!("metrics global recorder already installed");
        }
    });
}

/// 取共享表（首访建档）。
fn tables() -> Tables {
    SHARED.get_or_init(Tables::default).clone()
}

/// 渲染当前指标快照（Prometheus 文本格式 v0.0.4）。
#[must_use]
pub fn render_snapshot() -> String {
    let tables = tables();
    // 恒定注释头：无序列时输出仍非空（占位可见性），且为合法文本格式。
    let mut out = String::from("# zk_server metrics snapshot (in-process recorder)\n");
    render_counters(&tables, &mut out);
    render_gauges(&tables, &mut out);
    render_histograms(&tables, &mut out);
    out
}

fn render_counters(tables: &Tables, out: &mut String) {
    let Ok(series) = tables.counters.lock() else {
        return;
    };
    let mut sorted: Vec<&MetricSeries<u64>> = series.iter().collect();
    sorted.sort_by(|a, b| (&a.name, &a.labels).cmp(&(&b.name, &b.labels)));
    let mut current = String::new();
    for entry in sorted {
        if entry.name != current {
            current.clone_from(&entry.name);
            let _ = writeln!(out, "# TYPE {current} counter");
        }
        out.push_str(&series_line(
            &entry.name,
            &entry.labels,
            &entry.value.to_string(),
        ));
    }
}

fn render_gauges(tables: &Tables, out: &mut String) {
    let Ok(series) = tables.gauges.lock() else {
        return;
    };
    let mut sorted: Vec<&MetricSeries<f64>> = series.iter().collect();
    sorted.sort_by(|a, b| (&a.name, &a.labels).cmp(&(&b.name, &b.labels)));
    let mut current = String::new();
    for entry in sorted {
        if entry.name != current {
            current.clone_from(&entry.name);
            let _ = writeln!(out, "# TYPE {current} gauge");
        }
        out.push_str(&series_line(
            &entry.name,
            &entry.labels,
            &format_f64(entry.value),
        ));
    }
}

fn render_histograms(tables: &Tables, out: &mut String) {
    let Ok(series) = tables.histograms.lock() else {
        return;
    };
    let mut sorted: Vec<&MetricSeries<Vec<f64>>> = series.iter().collect();
    sorted.sort_by(|a, b| (&a.name, &a.labels).cmp(&(&b.name, &b.labels)));
    let mut current = String::new();
    for entry in sorted {
        if entry.name != current {
            current.clone_from(&entry.name);
            let _ = writeln!(out, "# TYPE {current} histogram");
        }
        let samples = &entry.value;
        let count = samples.len();
        let sum: f64 = samples.iter().sum();
        for bucket in BUCKETS {
            let in_bucket = samples.iter().filter(|sample| **sample <= bucket).count();
            out.push_str(&series_line(
                &format!("{}_bucket", entry.name),
                &format!("{},le=\"{bucket}\"", entry.labels),
                &in_bucket.to_string(),
            ));
        }
        out.push_str(&series_line(
            &format!("{}_bucket", entry.name),
            &format!("{},le=\"+Inf\"", entry.labels),
            &count.to_string(),
        ));
        out.push_str(&series_line(
            &format!("{}_sum", entry.name),
            &entry.labels,
            &format_f64(sum),
        ));
        out.push_str(&series_line(
            &format!("{}_count", entry.name),
            &entry.labels,
            &count.to_string(),
        ));
    }
}

/// 单序列输出行（`name{labels} value`；标签为空时省略花括号）。
fn series_line(name: &str, labels: &str, value: &str) -> String {
    if labels.is_empty() {
        format!("{name} {value}\n")
    } else {
        format!("{name}{{{labels}}} {value}\n")
    }
}

/// f64 的 Prometheus 文本表示（NaN/Inf 走其字面量）。
fn format_f64(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "+Inf".to_owned()
        } else {
            "-Inf".to_owned()
        }
    } else {
        format!("{value}")
    }
}

/// 指标名（`Key` 的 Display 截取名称段，`.` → `_` 规约）。
fn key_name(key: &Key) -> String {
    key.name().replace('.', "_")
}

/// 标签段（`k="v"` 逗号连接；值按 Prometheus 文本格式转义）。
fn key_labels(key: &Key) -> String {
    key.labels()
        .map(|label| {
            format!(
                "{}=\"{}\"",
                label.key().replace('.', "_"),
                escape_label_value(label.value())
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Prometheus 标签值转义：反斜杠、双引号与换行（文本格式的三个转义点）。
fn escape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out
}

impl metrics::Recorder for MetricsRecorder {
    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(CounterHandle {
            name: key_name(key),
            labels: key_labels(key),
            table: self.tables.counters.clone(),
        }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(GaugeHandle {
            name: key_name(key),
            labels: key_labels(key),
            table: self.tables.gauges.clone(),
        }))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(HistogramHandle {
            name: key_name(key),
            labels: key_labels(key),
            table: self.tables.histograms.clone(),
        }))
    }

    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
}

struct CounterHandle {
    name: String,
    labels: String,
    table: Arc<Mutex<Vec<MetricSeries<u64>>>>,
}

impl CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        if let Ok(mut table) = self.table.lock() {
            let current = upsert(&mut table, &self.name, &self.labels, 0_u64);
            *current = current.saturating_add(value);
        }
    }

    fn absolute(&self, value: u64) {
        if let Ok(mut table) = self.table.lock() {
            *upsert(&mut table, &self.name, &self.labels, 0_u64) = value;
        }
    }
}

struct GaugeHandle {
    name: String,
    labels: String,
    table: Arc<Mutex<Vec<MetricSeries<f64>>>>,
}

impl GaugeFn for GaugeHandle {
    fn set(&self, value: f64) {
        if let Ok(mut table) = self.table.lock() {
            *upsert(&mut table, &self.name, &self.labels, 0.0) = value;
        }
    }

    fn increment(&self, value: f64) {
        if let Ok(mut table) = self.table.lock() {
            let current = upsert(&mut table, &self.name, &self.labels, 0.0);
            *current += value;
        }
    }

    fn decrement(&self, value: f64) {
        if let Ok(mut table) = self.table.lock() {
            let current = upsert(&mut table, &self.name, &self.labels, 0.0);
            *current -= value;
        }
    }
}

struct HistogramHandle {
    name: String,
    labels: String,
    table: Arc<Mutex<Vec<MetricSeries<Vec<f64>>>>>,
}

impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        if let Ok(mut table) = self.table.lock() {
            upsert(&mut table, &self.name, &self.labels, Vec::new()).push(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facade_counters_render_prometheus_text() {
        install_once();
        metrics::counter!("zk_server_test_total", "kind" => "unit").increment(2);
        metrics::counter!("zk_server_test_total", "kind" => "unit").increment(1);
        metrics::histogram!("zk_server_test_seconds").record(0.010);
        let text = render_snapshot();
        assert!(text.contains("# TYPE zk_server_test_total counter"));
        assert!(text.contains("zk_server_test_total{kind=\"unit\"} 3"));
        assert!(text.contains("# TYPE zk_server_test_seconds histogram"));
        assert!(text.contains("zk_server_test_seconds_count 1"));
        assert!(text.contains("zk_server_test_seconds_sum 0.01"));
        assert!(text.contains("le=\"+Inf\""));
    }

    /// 未安装时渲染不 panic（防御路径）。
    #[test]
    fn snapshot_before_install_renders_placeholder() {
        // 渲染恒有注释占位头（未安装/无序列均非 panic、非空）。
        assert!(!render_snapshot().is_empty());
    }
}
