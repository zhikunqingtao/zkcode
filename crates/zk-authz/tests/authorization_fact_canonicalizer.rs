//! 旧 `authorization/AuthorizationFactCanonicalizerTest.java`（26 行）逐条翻译。
//!
//! | 旧 `@Test` | 旧源行号 | 本文件 |
//! |---|---|---|
//! | `canonicalizesEverySetLikeFactWithStableTotalOrdering` | L10-25 | [`canonicalizes_every_set_like_fact_with_stable_total_ordering`] |

use zk_authz::canonicalizer;
use zk_authz::model::{EffectClass, ResourceRef};

/// 旧源 `AuthorizationFactCanonicalizerTest.java:10-25`。
#[test]
fn canonicalizes_every_set_like_fact_with_stable_total_ordering() {
    // 旧源 L12-14
    assert_eq!(
        canonicalizer::effects(&[
            EffectClass::Process,
            EffectClass::ReadResource,
            EffectClass::Process,
        ]),
        vec![EffectClass::Process, EffectClass::ReadResource]
    );
    // 旧源 L15-22
    assert_eq!(
        canonicalizer::resources(&[
            ResourceRef::new("path", "b", false),
            ResourceRef::new("path", "a", true),
            ResourceRef::new("path", "a", false),
        ]),
        vec![
            ResourceRef::new("path", "a", false),
            ResourceRef::new("path", "a", true),
            ResourceRef::new("path", "b", false),
        ]
    );
    // 旧源 L23-24
    assert_eq!(
        canonicalizer::strings(&["z".to_owned(), "a".to_owned(), "z".to_owned()]),
        vec!["a".to_owned(), "z".to_owned()]
    );
}
