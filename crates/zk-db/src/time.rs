//! epoch 毫秒 ↔ RFC 3339（UTC）双向转换，及领域 struct 时间字段的 serde 适配。
//!
//! # 为什么 DB 里存 ISO 字符串而不是整数毫秒
//!
//! 旧系统以 `Instant.now().toString()` 落库（RFC 3339 字符串），且
//! `ORDER BY updated_at DESC` 排序、`updated_at < ?` 游标锚点、
//! `Base64("updated_at|session_id")` 游标内容**全部依赖该字符串**。zk-db
//! 照抄 TEXT 存储形状（迁移逐列照抄，见 `migrations/`），使 zkcode 与旧
//! 系统 data.db 双表二进制互读（U3 对照调试 8082/8080 并行依赖此性质）。
//!
//! # 写出恒定 6 位微秒（对旧格式的有意收紧）
//!
//! Java `Instant.toString()` 小数位动态取 0/3/6/9 位：整秒时输出**无小数**
//!（如 `...T10:38:15Z`）。`Z`(0x5A) 大于 `.`(0x2E)，导致 `15Z` 在字典序上
//! 排在 `15.413839Z` **之后**——即旧库中混合精度时间戳的整秒条目会被
//! `ORDER BY updated_at` 判为「最新」，这是旧系统排序的边界隐患（实际触发
//! 概率极低，见 tests::whole_second_lexicographic_hazard）。zk-db 写出恒定
//! `.{6}Z` 微秒（同毫秒内单调、与旧 6 位格式同序），读取侧宽容解析旧库
//! 任意 0/3/6/9 位变体。
//!
//! # serde 形状
//!
//! [`serde_iso_ms`] 将领域 struct 的 `i64` 毫秒字段序列化为 RFC 3339 字符串
//!（对齐实采样例 `GET /api/sessions` 的 `createdAt` 形状），反序列化同时
//! 接受毫秒数字与 RFC 3339 字符串（输入域与 `zk_protocol::FlexEpoch` 一致）。

/// 当前时刻的 epoch 毫秒（时钟早于 `UNIX_EPOCH` 时取 0，防御性兜底）。
#[must_use]
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| {
            // u128 毫秒 → i64：时间戳正值域远小于 i64 上限，饱和转换足够。
            i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
        })
}

/// epoch 毫秒 → `YYYY-MM-DDTHH:MM:SS.ffffffZ`（UTC，恒 6 位微秒）。
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

/// 解析 RFC 3339（`YYYY-MM-DDTHH:MM:SS[.1-9 位小数](Z|±HH:MM|±HHMM)`）
/// 为 epoch 毫秒；小数秒截断到毫秒。
///
/// 与 `zk_protocol::model::parse_rfc3339_millis` 算法一致（该函数为契约层
/// 私有实现，跨 crate 提升其可见性属协议层 API 变更，超出 S5 范围，故此处
/// 自持副本；两处算法以相同的黄金测试值互锁）。
#[must_use]
pub fn parse_rfc3339_millis(input: &str) -> Option<i64> {
    let b = input.as_bytes();
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let mut n: i64 = 0;
        for &c in b.get(range)? {
            n = n * 10 + i64::from((c as char).to_digit(10)?);
        }
        Some(n)
    };
    if b.len() < 20 {
        return None;
    }
    let year = digits(0..4)?;
    let month = digits(5..7)?;
    let day = digits(8..10)?;
    if b[4] != b'-' || b[7] != b'-' || (b[10] != b'T' && b[10] != b't') {
        return None;
    }
    let hour = digits(11..13)?;
    let minute = digits(14..16)?;
    let second = digits(17..19)?;
    if b[13] != b':' || b[16] != b':' {
        return None;
    }
    // 可选小数秒：取毫秒（截断到 3 位，余下位数忽略）。
    let mut millis = 0_i64;
    let mut idx = 19;
    if b.get(idx) == Some(&b'.') {
        idx += 1;
        let start = idx;
        while idx < b.len() && b[idx].is_ascii_digit() {
            if idx - start < 3 {
                millis = millis * 10 + i64::from(b[idx] - b'0');
            }
            idx += 1;
        }
        for _ in (idx - start)..3 {
            millis *= 10;
        }
        if idx == start {
            return None;
        }
    }
    // 时区：Z / ±HH:MM / ±HHMM。
    let offset_secs: i64 = match b.get(idx) {
        Some(&b'Z' | &b'z') => {
            idx += 1;
            0
        }
        Some(&sign @ (b'+' | b'-')) => {
            idx += 1;
            let oh = digits(idx..idx + 2)?;
            let om = if b.get(idx + 2) == Some(&b':') {
                let m = digits(idx + 3..idx + 5)?;
                idx += 5;
                m
            } else {
                let m = digits(idx + 2..idx + 4)?;
                idx += 4;
                m
            };
            let mag = oh * 3600 + om * 60;
            if sign == b'+' { mag } else { -mag }
        }
        _ => return None,
    };
    if idx != b.len() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(
        days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second
            - offset_secs,
    )
    .map(|secs| secs * 1000 + millis)
}

/// civil 日期 → 自 1970-01-01 的天数（Howard Hinnant `days_from_civil`）。
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
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

/// 领域 struct 时间字段（epoch 毫秒 `i64`）的 serde 适配：
/// 序列化 → RFC 3339 字符串（对齐实采样例 `createdAt` 形状）；
/// 反序列化 ← 毫秒数字或 RFC 3339 字符串（对齐 `FlexEpoch` 输入域）。
pub(crate) mod serde_iso_ms {
    use super::{format_rfc3339_micros, parse_rfc3339_millis};
    use serde::{Deserialize, Deserializer, Serializer};

    /// 序列化为 RFC 3339（UTC，6 位微秒）字符串。
    // serde `with` 约定签名为 `&i64`，无法改传值——定点豁免拷贝传参 lint。
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(crate) fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_rfc3339_micros(*value))
    }

    /// 反序列化：毫秒数字或 RFC 3339 字符串。
    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(n) => n
                .as_i64()
                .ok_or_else(|| serde::de::Error::custom("epoch milliseconds must be an integer")),
            serde_json::Value::String(s) => parse_rfc3339_millis(&s).ok_or_else(|| {
                serde::de::Error::custom(format!("invalid RFC 3339 timestamp: {s}"))
            }),
            other => Err(serde::de::Error::custom(format!(
                "expected number or RFC 3339 string, got: {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 黄金值来自实采样例（GET /api/sessions-id-export-json）：
    /// `createdAt: 1786792040.564532`（epoch 秒）⇔ `2026-08-15T11:07:20.564532Z`。
    const GOLDEN_MILLIS: i64 = 1_786_792_040_564;
    const GOLDEN_ISO: &str = "2026-08-15T11:07:20.564532Z";

    #[test]
    fn format_matches_baseline_sample() {
        // 领域时间为毫秒：样例的微秒尾（532）在解析时已截断，写出恒 6 位补零。
        assert_eq!(
            format_rfc3339_micros(GOLDEN_MILLIS),
            "2026-08-15T11:07:20.564000Z"
        );
    }

    #[test]
    fn parse_roundtrip() {
        assert_eq!(parse_rfc3339_millis(GOLDEN_ISO), Some(GOLDEN_MILLIS));
        // 旧库动态小数位：0 / 3 / 9 位变体全部宽容解析。
        assert_eq!(
            parse_rfc3339_millis("2026-08-15T11:07:20Z"),
            Some(1_786_792_040_000)
        );
        assert_eq!(
            parse_rfc3339_millis("2026-08-15T11:07:20.564Z"),
            Some(GOLDEN_MILLIS)
        );
        assert_eq!(
            parse_rfc3339_millis("2026-08-15T11:07:20.564532119Z"),
            Some(GOLDEN_MILLIS)
        );
        // 非零时区偏移换算。
        assert_eq!(
            parse_rfc3339_millis("2026-08-15T19:07:20.564+08:00"),
            Some(GOLDEN_MILLIS)
        );
        assert_eq!(
            parse_rfc3339_millis("2026-08-15T07:07:20.564-0400"),
            Some(GOLDEN_MILLIS)
        );
        // 负 epoch（1970 前）走 div_euclid/rem_euclid 分支不 panic。
        assert_eq!(format_rfc3339_micros(-1), "1969-12-31T23:59:59.999000Z");
        assert_eq!(
            parse_rfc3339_millis("1969-12-31T23:59:59.999000Z"),
            Some(-1)
        );
        // 非法输入。
        assert_eq!(parse_rfc3339_millis("not-a-time"), None);
        assert_eq!(parse_rfc3339_millis("2026-13-01T00:00:00Z"), None);
    }

    /// 留痕旧系统混合精度时间戳的字典序隐患（模块文档所述），
    /// zk-db 恒 6 位微秒写出使整秒条目不再劣后。
    #[test]
    fn whole_second_lexicographic_hazard() {
        let whole_second = format_rfc3339_micros(1_786_792_040_000);
        let with_micros = format_rfc3339_micros(GOLDEN_MILLIS);
        // 旧格式（"…15Z" vs "…15.564532Z"）字典序会把整秒判为更大；
        // zk-db 格式（"…15.000000Z"）字典序与时间序一致。
        assert!(whole_second < with_micros);
    }

    #[test]
    fn now_millis_is_sane() {
        let a = now_millis();
        let b = now_millis();
        assert!(b >= a);
        assert!(a > 1_700_000_000_000); // 2023-11 之后
    }
}
