//! REST query-param 解析助手——复刻 Spring `@RequestParam` 的绑定与失败语义
//! （Batch 2 端点域；`RunController` / `FileController` / `ActivityController`
//! 的 `int` 与必填 `String` 参数）。
//!
//! # 语义对照（旧 `GlobalExceptionHandler`）
//!
//! - `@RequestParam(defaultValue = "N") int x`：缺省 → `N`；提供且可解析为
//!   `i32`（Java `int`）→ 值；提供但**不可解析**（如 `limit=abc` / 越界）→
//!   Spring 抛 `MethodArgumentTypeMismatchException`，该类**无**专门
//!   `@ExceptionHandler`（它非 `IllegalArgumentException` 子类），落到
//!   `@ExceptionHandler(Exception.class)` → **500 `INTERNAL_ERROR`**。故
//!   [`parse_spring_int`] 对非法值返回 [`ApiError::internal`]。
//! - 必填 `@RequestParam String x`（无 `defaultValue`）：缺省 →
//!   `MissingServletRequestParameterException` → **400 `MISSING_PARAMETER`**，
//!   文案 `Required parameter 'x' is missing`（见 [`require_param`]）。空串
//!   （`?x=`）视为**已提供**，原样返回（消费方自行处理空值）。

use std::collections::HashMap;

use axum::http::StatusCode;

use crate::error::ApiError;

/// 复刻 Spring `@RequestParam(defaultValue) int` 绑定：缺省取默认，非法值 500。
///
/// # Errors
/// 参数存在但无法解析为 `i32` 时返回 500 `INTERNAL_ERROR`（对齐旧
/// `MethodArgumentTypeMismatchException` 落 `handleGeneric` 的行为）。
pub(crate) fn parse_spring_int(raw: Option<&str>, default: i32) -> Result<i32, ApiError> {
    match raw {
        None => Ok(default),
        // Spring 的 `String`→`Number` 转换（`NumberUtils.parseNumber`）先
        // `trimAllWhitespace`，再 `Integer.parseInt`；此处 trim 后解析对齐。
        Some(text) => text.trim().parse::<i32>().map_err(|_| ApiError::internal()),
    }
}

/// 复刻必填 `@RequestParam String`：缺省 → 400 `MISSING_PARAMETER`；空串视为
/// 已提供并原样返回。
///
/// # Errors
/// 参数缺省时返回 400 `MISSING_PARAMETER`，文案
/// `Required parameter '<name>' is missing`。
pub(crate) fn require_param<'a>(
    params: &'a HashMap<String, String>,
    name: &str,
) -> Result<&'a str, ApiError> {
    params
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "MISSING_PARAMETER".to_owned(),
            message: format!("Required parameter '{name}' is missing"),
        })
}

/// Spring `StringToBooleanConverter` 的真值集合（比较前 `trim` + 小写化）。
const SPRING_TRUE_VALUES: [&str; 4] = ["true", "on", "yes", "1"];

/// Spring `StringToBooleanConverter` 的假值集合。
const SPRING_FALSE_VALUES: [&str; 4] = ["false", "off", "no", "0"];

/// 复刻 Spring `StringToBooleanConverter`：`trim` 后小写化，空串 → `None`
/// （转换器对空输入返回 `null`），不在两个集合内 → `Err(())`。
fn convert_spring_bool(raw: &str) -> Result<Option<bool>, ()> {
    let value = raw.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Ok(None);
    }
    if SPRING_TRUE_VALUES.contains(&value.as_str()) {
        return Ok(Some(true));
    }
    if SPRING_FALSE_VALUES.contains(&value.as_str()) {
        return Ok(Some(false));
    }
    Err(())
}

/// 复刻必填 `@RequestParam boolean`（**基元**类型）绑定。
///
/// # Errors
/// - 参数缺省 → 400 `MISSING_PARAMETER`（`MissingServletRequestParameterException`）；
/// - 参数为空串 → 500 `INTERNAL_ERROR`（转换器回 `null`，基元形参无法接收 →
///   `IllegalStateException`，落 `handleGeneric`）；
/// - 参数不可识别（如 `enabled=maybe`）→ 500 `INTERNAL_ERROR`
///   （`MethodArgumentTypeMismatchException`，同 [`parse_spring_int`]）。
pub(crate) fn require_spring_bool(
    params: &HashMap<String, String>,
    name: &str,
) -> Result<bool, ApiError> {
    let raw = require_param(params, name)?;
    match convert_spring_bool(raw) {
        Ok(Some(value)) => Ok(value),
        Ok(None) | Err(()) => Err(ApiError::internal()),
    }
}

/// 复刻可选 `@RequestParam(required = false) Boolean`（**包装**类型）绑定：
/// 缺省与空串同为 `None`。
///
/// # Errors
/// 参数不可识别时返回 500 `INTERNAL_ERROR`（`ConversionFailedException`）。
pub(crate) fn optional_spring_bool(
    params: &HashMap<String, String>,
    name: &str,
) -> Result<Option<bool>, ApiError> {
    match params.get(name) {
        None => Ok(None),
        Some(raw) => convert_spring_bool(raw).map_err(|()| ApiError::internal()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_spring_int_defaults_when_absent() {
        assert_eq!(parse_spring_int(None, 20).expect("default"), 20);
    }

    #[test]
    fn parse_spring_int_parses_valid_including_negative() {
        assert_eq!(parse_spring_int(Some("50"), 20).expect("valid"), 50);
        assert_eq!(parse_spring_int(Some(" 7 "), 20).expect("trimmed"), 7);
        assert_eq!(parse_spring_int(Some("-3"), 20).expect("negative"), -3);
    }

    #[test]
    fn parse_spring_int_malformed_maps_500() {
        let err = parse_spring_int(Some("abc"), 20).expect_err("malformed");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.code, "INTERNAL_ERROR");
        // i32 溢出同样走非法分支。
        let overflow = parse_spring_int(Some("99999999999"), 20).expect_err("overflow");
        assert_eq!(overflow.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn require_param_missing_maps_400_missing_parameter() {
        let params = HashMap::new();
        let err = require_param(&params, "query").expect_err("missing");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "MISSING_PARAMETER");
        assert_eq!(err.message, "Required parameter 'query' is missing");
    }

    #[test]
    fn require_param_present_returns_value_including_empty() {
        let mut params = HashMap::new();
        params.insert("query".to_owned(), String::new());
        // 空串视为已提供（Spring 语义）。
        assert_eq!(require_param(&params, "query").expect("present"), "");
        params.insert("sessionId".to_owned(), "s-1".to_owned());
        assert_eq!(require_param(&params, "sessionId").expect("present"), "s-1");
    }

    #[test]
    fn spring_bool_accepts_all_four_synonym_pairs() {
        let mut params = HashMap::new();
        for raw in ["true", "TRUE", " on ", "Yes", "1"] {
            params.insert("enabled".to_owned(), raw.to_owned());
            assert!(require_spring_bool(&params, "enabled").expect(raw));
        }
        for raw in ["false", "OFF", "no", "0"] {
            params.insert("enabled".to_owned(), raw.to_owned());
            assert!(!require_spring_bool(&params, "enabled").expect(raw));
        }
    }

    #[test]
    fn require_spring_bool_missing_is_400_but_malformed_is_500() {
        let mut params = HashMap::new();
        let missing = require_spring_bool(&params, "enabled").expect_err("missing");
        assert_eq!(missing.status, StatusCode::BAD_REQUEST);
        assert_eq!(missing.code, "MISSING_PARAMETER");
        params.insert("enabled".to_owned(), "maybe".to_owned());
        assert_eq!(
            require_spring_bool(&params, "enabled")
                .expect_err("malformed")
                .status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        // 空串：转换器回 null，基元形参无法接收 → 500。
        params.insert("enabled".to_owned(), "  ".to_owned());
        assert_eq!(
            require_spring_bool(&params, "enabled")
                .expect_err("blank")
                .status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn optional_spring_bool_treats_absent_and_blank_as_none() {
        let mut params = HashMap::new();
        assert_eq!(
            optional_spring_bool(&params, "enabled").expect("absent"),
            None
        );
        params.insert("enabled".to_owned(), String::new());
        assert_eq!(
            optional_spring_bool(&params, "enabled").expect("blank"),
            None
        );
        params.insert("enabled".to_owned(), "true".to_owned());
        assert_eq!(
            optional_spring_bool(&params, "enabled").expect("true"),
            Some(true)
        );
        params.insert("enabled".to_owned(), "nope".to_owned());
        assert_eq!(
            optional_spring_bool(&params, "enabled")
                .expect_err("malformed")
                .status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
