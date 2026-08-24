//! epoch 毫秒 → RFC 3339（UTC，恒 6 位微秒）——REST 出口时间格式化。
//!
//! zk-db 的 `time` 模块为 crate 私有（提升可见性属上游 API 变更，超出 S7
//! 范围），此处自持同算法副本：写出恒 `.{6}Z` 微秒、`civil_from_days`
//! 纯函数换算，以相同黄金值（S2 实采 export 样例 `1786792040.564532` ⇔
//! `2026-08-15T11:07:20.564532Z`）与 zk-db / zk-protocol 互锁。
//!
//! REST 线上时间形状：旧 Jackson `write-dates-as-timestamps: false` 输出
//! ISO 字符串（`createdAt` / `timestamp` / 错误信封 `timestamp`），唯一例外
//! 是 export 端点的独立 `ObjectMapper`（epoch 浮点秒，见 `api` 模块）。

/// 当前时刻的 epoch 毫秒（时钟早于 `UNIX_EPOCH` 时取 0，防御性兜底）。
#[must_use]
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// epoch 毫秒 → `YYYY-MM-DDTHH:MM:SS.ffffffZ`。
#[must_use]
pub fn format_rfc3339_micros(millis: i64) -> String {
    let secs = millis.div_euclid(1000);
    let micros = millis.rem_euclid(1000) * 1000;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}Z")
}

/// 自 1970-01-01 的天数 → civil 日期（Howard Hinnant `civil_from_days`）。
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 黄金值与 zk-db `time.rs` / 实采样例互锁。
    #[test]
    fn golden_sample_roundtrip() {
        const GOLDEN_MILLIS: i64 = 1_786_792_040_564;
        assert_eq!(
            format_rfc3339_micros(GOLDEN_MILLIS),
            "2026-08-15T11:07:20.564000Z"
        );
        assert_eq!(format_rfc3339_micros(-1), "1969-12-31T23:59:59.999000Z");
    }
}
