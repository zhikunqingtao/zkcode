//! 授权事实的唯一规范化入口。
//!
//! 逐字对照 `authorization/AuthorizationFactCanonicalizer.java`（L12-31）。授权哈希与
//! 最终描述符必须使用完全相同的有序事实，否则同一操作会因集合构造顺序不同，在执行前
//! 复检时被误判为安全事实发生变化。

use crate::model::{EffectClass, ResourceRef};

/// `effects`：去重后按枚举 **名称** 升序。
///
/// 对照 `AuthorizationFactCanonicalizer.java:20-22`
/// （`distinct().sorted(Comparator.comparing(Enum::name))`）。注意排序键是 `name()`
/// 字符串而不是 `ordinal()`——例如 `NETWORK` 排在 `PROCESS` 之前。
#[must_use]
pub fn effects(values: &[EffectClass]) -> Vec<EffectClass> {
    let mut out: Vec<EffectClass> = Vec::with_capacity(values.len());
    for value in values {
        if !out.contains(value) {
            out.push(*value);
        }
    }
    out.sort_unstable_by_key(|effect| effect.as_str());
    out
}

/// `resources`：去重后按 `(kind, value, outsideWorkspace)` 三元组升序。
///
/// 对照 `AuthorizationFactCanonicalizer.java:13-16, 24-26`。`outsideWorkspace` 为
/// `Boolean` 自然序，即 `false < true`——Rust `bool` 的 `Ord` 完全一致。
#[must_use]
pub fn resources(values: &[ResourceRef]) -> Vec<ResourceRef> {
    let mut out: Vec<ResourceRef> = Vec::with_capacity(values.len());
    for value in values {
        if !out.contains(value) {
            out.push(value.clone());
        }
    }
    out.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.value.cmp(&right.value))
            .then_with(|| left.outside_workspace.cmp(&right.outside_workspace))
    });
    out
}

/// `strings`：去重后自然升序（用于 `environment` 与 `endpoints`）。
///
/// 对照 `AuthorizationFactCanonicalizer.java:28-30`。
#[must_use]
pub fn strings(values: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(values.len());
    for value in values {
        if !out.contains(value) {
            out.push(value.clone());
        }
    }
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 旧源 `AuthorizationFactCanonicalizer.java:20-22`：按 `name()` 而非 `ordinal()`。
    #[test]
    fn effects_sort_by_enum_name_not_declaration_order() {
        let sorted = effects(&[
            EffectClass::Process,
            EffectClass::Network,
            EffectClass::Process,
        ]);
        assert_eq!(sorted, vec![EffectClass::Network, EffectClass::Process]);
    }

    /// 旧源 `AuthorizationFactCanonicalizer.java:13-16`：kind → value → outsideWorkspace。
    #[test]
    fn resources_sort_by_kind_then_value_then_outside_flag() {
        let sorted = resources(&[
            ResourceRef::new("path", "b", false),
            ResourceRef::new("cwd", "z", false),
            ResourceRef::new("path", "a", true),
            ResourceRef::new("path", "a", false),
        ]);
        assert_eq!(
            sorted,
            vec![
                ResourceRef::new("cwd", "z", false),
                ResourceRef::new("path", "a", false),
                ResourceRef::new("path", "a", true),
                ResourceRef::new("path", "b", false),
            ]
        );
    }

    /// 旧源 `AuthorizationFactCanonicalizer.java:28-30`：去重 + 自然升序。
    #[test]
    fn strings_are_deduplicated_and_sorted() {
        assert_eq!(
            strings(&["b".to_string(), "a".to_string(), "b".to_string()]),
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
