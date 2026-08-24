//! `FrozenToolInputFactoryTest.java`（61 行）逐条翻译。
//!
//! 形状差异（已在 `docs/compatibility.md` §8 记录）：
//! - 旧源工厂签名 `(ObjectMapper, maxBytes, maxInflightBytes)`；Rust 侧只保留
//!   `maxBytes`（F-01：inflight 堆配额会计 DEFERRED），故第三个测试只翻译
//!   「单次字节上限」半段，`inflightBytes()==0` 的断言在 Rust 无对应观测量。
//! - 旧源 `AuthorizationException` → Rust `AuthzError { code, message }`。

use serde_json::{Value, json};
use zk_authz::frozen::FrozenToolInputFactory;

/// 旧源 `FrozenToolInputFactoryTest.java:17-32`
/// `canonicalHashIsOrderStableAndExecutionUsesFrozenBytes`。
#[test]
fn canonical_hash_is_order_stable_and_execution_uses_frozen_bytes() {
    let factory = FrozenToolInputFactory::with_max_bytes(1024);

    // L19-24：键序不同、内容相同的两份输入必须得到同一 inputHash。
    let first = json!({"z": 1, "a": {"b": 2, "a": 1}});
    let second = json!({"a": {"a": 1, "b": 2}, "z": 1});
    let one = factory.freeze("Tool", &first).expect("freeze first");
    let two = factory.freeze("Tool", &second).expect("freeze second");
    assert_eq!(one.input_hash(), two.input_hash());

    // L26-29：冻结后改写源 map 不得影响执行用输入。
    // Rust 侧 `freeze(&Value)` 拷出规范字节，源值的别名突变结构上不可能影响
    // 冻结体；这里显式改写源值再断言冻结体不变，与旧源观察点一致。
    let mut mutated = first.clone();
    mutated["z"] = json!(999);
    let executed = one.to_tool_input().expect("frozen input to tool input");
    assert_eq!(executed.get("z"), Some(&json!(1)));
    assert_eq!(mutated.get("z"), Some(&json!(999)));

    // L31：旧源断言 `factory.inflightBytes() == 0`（F-01 DEFERRED，无对应量）；
    // 等价可观测：冻结体字节数就是规范 JSON 长度，不随源值改写变化。
    assert_eq!(one.byte_size(), one.canonical_json_bytes().len());
}

/// 旧源 `FrozenToolInputFactoryTest.java:34-44`
/// `conservativeCanonicalizationDoesNotMergeNumbersOrUnicodeForms`。
#[test]
fn conservative_canonicalization_does_not_merge_numbers_or_unicode_forms() {
    let factory = FrozenToolInputFactory::with_max_bytes(1024);

    // L36-39：整数 1 与浮点 1.0 是不同的规范形态，哈希不得合并。
    let integral = factory
        .freeze("Tool", &json!({"n": 1}))
        .expect("freeze integral");
    let decimal = factory
        .freeze("Tool", &json!({"n": 1.0}))
        .expect("freeze decimal");
    assert_ne!(integral.input_hash(), decimal.input_hash());

    // L41-43：NFC 与 NFD 的 "é" 不做 Unicode 归一化，哈希不得合并。
    let composed: Value = json!({"s": "\u{00e9}"});
    let decomposed: Value = json!({"s": "e\u{0301}"});
    let nfc = factory.freeze("Tool", &composed).expect("freeze nfc");
    let nfd = factory.freeze("Tool", &decomposed).expect("freeze nfd");
    assert_ne!(nfc.input_hash(), nfd.input_hash());
}

/// 旧源 `FrozenToolInputFactoryTest.java:46-60`
/// `sizeAndInflightBudgetsFailWithoutLeakingPermits`（只翻译 size 半段）。
#[test]
fn size_budget_fails_with_stable_error_code() {
    // L48-53：上限 32 字节的工厂冻结 100 字符输入必须失败，且消息含
    // "Canonical tool input exceeds"。
    let tiny = FrozenToolInputFactory::with_max_bytes(32);
    let oversized = json!({"s": "x".repeat(100)});
    let failure = tiny
        .freeze("Tool", &oversized)
        .expect_err("oversized input must be rejected");
    assert_eq!(failure.code, "TOOL_INPUT_TOO_LARGE");
    assert!(
        failure.message.contains("Canonical tool input exceeds"),
        "unexpected message: {}",
        failure.message
    );

    // L55-59：旧源用第二个工厂验证 inflight 许可不泄漏。Rust 无 inflight 会计
    // （F-01 DEFERRED），等价不变量为：失败后工厂仍可正常冻结合法输入。
    let ok = tiny
        .freeze("Tool", &json!({"s": "x"}))
        .expect("factory stays usable after rejection");
    assert!(ok.byte_size() <= 32);
}
