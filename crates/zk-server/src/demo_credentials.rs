//! Public first-run demo credential seed.
//!
//! The repository tracks a deliberately public, minimal `SQLite` database under
//! `configuration/bootstrap/`.  It is **not** the user's runtime database: on a
//! clean first launch we open it read-only, validate its fixed schema and copy
//! the single allowed credential into the private runtime database.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// `SQLite` application id (`"ZKDD"`) for the public demo credential seed.
const APPLICATION_ID: i64 = 1_514_885_956;
const SCHEMA_VERSION: i64 = 1;
const EXPECTED_PROVIDER: &str = "dashscope-token-plan";
const EXPECTED_PURPOSE: &str = "public-first-run-demo";

/// Existing runtime key map. Its public JSON shape is intentionally unchanged.
pub(crate) const KEYS_DB_KEY: &str = "llm_provider_keys";
/// Private companion row proving which entries came from the public demo seed.
pub(crate) const PROVENANCE_DB_KEY: &str = "llm_provider_key_provenance";
pub(crate) const PUBLIC_DEMO_SOURCE: &str = "public_demo_seed";

/// Append-only fingerprints of every public demo value approved for release.
///
/// Add the tracked seed digest here before each release and never remove an
/// entry. Runtime catalog loading also derives the current digest from the
/// strictly validated asset; keeping it in this table locks review history and
/// preserves recognition after a later seed rotation.
const HISTORICAL_PUBLIC_DEMO_KEY_SHA256: &[(&str, &str)] = &[(
    "dashscope-token-plan",
    "04f610b848356f68380f534627a2d1b8ad3975a973c4cdfc523c94a6c2529ce8",
)];

/// Private provenance marker. Only proven public-demo entries are recorded;
/// user credentials never get a fingerprint companion row.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyProvenance {
    pub source: String,
    pub sha256: String,
}

pub(crate) type ProvenanceMap = BTreeMap<String, KeyProvenance>;

/// Strictly loaded current credentials plus the append-only historical digest
/// catalog used to identify legacy runtime values without retaining them twice.
pub(crate) struct Catalog {
    credentials: BTreeMap<String, String>,
    known_sha256: BTreeMap<String, BTreeSet<String>>,
}

pub(crate) struct ReconcileOutcome {
    pub changed: bool,
    pub removed_public: usize,
    pub cleared_stale_markers: usize,
}

impl Catalog {
    #[cfg(test)]
    #[must_use]
    pub fn credentials(&self) -> &BTreeMap<String, String> {
        &self.credentials
    }

    #[must_use]
    pub fn is_public_value(&self, provider: &str, api_key: &str) -> bool {
        self.known_sha256
            .get(provider)
            .is_some_and(|known| known.contains(&sha256_hex(api_key)))
    }

    /// Filter public-demo members regardless of their original provider. This
    /// is used only for the provider-less legacy `ZK_LLM_API_KEY` fallback.
    #[must_use]
    pub fn retain_values_not_public_for_any_provider(&self, api_keys: &str) -> Option<String> {
        Self::filter_key_ring(api_keys, |api_key| {
            self.is_public_value_for_any_provider(api_key)
        })
        .0
    }

    /// Whether any effective member of a comma-separated key ring is a known
    /// public demo value, regardless of the provider label under which it was
    /// supplied. Runtime/API policy uses this form so relabelling a public key
    /// cannot bypass opt-out; durable upgrade migration remains provider-bound.
    #[must_use]
    pub fn contains_public_value_for_any_provider(&self, api_keys: &str) -> bool {
        effective_key_items(api_keys).any(|api_key| self.is_public_value_for_any_provider(api_key))
    }

    fn is_public_value_for_any_provider(&self, api_key: &str) -> bool {
        let digest = sha256_hex(api_key);
        self.known_sha256
            .values()
            .any(|known| known.contains(&digest))
    }

    fn filter_key_ring(
        api_keys: &str,
        is_public: impl Fn(&str) -> bool,
    ) -> (Option<String>, usize) {
        let mut retained = Vec::new();
        let mut removed = 0usize;
        for api_key in effective_key_items(api_keys) {
            if is_public(api_key) {
                removed += 1;
            } else {
                retained.push(api_key);
            }
        }
        ((!retained.is_empty()).then(|| retained.join(",")), removed)
    }

    #[must_use]
    pub fn marker_for(&self, provider: &str, api_keys: &str) -> Option<KeyProvenance> {
        let mut items = effective_key_items(api_keys);
        let api_key = items.next()?;
        if items.next().is_some() || !self.is_public_value(provider, api_key) {
            return None;
        }
        Some(KeyProvenance {
            source: PUBLIC_DEMO_SOURCE.to_owned(),
            sha256: sha256_hex(api_key),
        })
    }

    /// A marker independently proves public-demo provenance when its source and
    /// well-formed digest match the current stored value. It need not remain in
    /// the append-only catalog: provenance supports values shipped before the
    /// digest catalog existed. A single marker never proves a multi-key ring:
    /// doing so could make an unrelated user key removable with the public one.
    #[must_use]
    pub fn marker_proves_public(api_keys: &str, marker: &KeyProvenance) -> bool {
        let mut items = effective_key_items(api_keys);
        let Some(api_key) = items.next() else {
            return false;
        };
        marker.source == PUBLIC_DEMO_SOURCE
            && is_sha256_hex(&marker.sha256)
            && items.next().is_none()
            && marker.sha256 == sha256_hex(api_key)
    }

    /// Reconcile an upgraded runtime database against the trusted public-demo
    /// catalog. Unknown/user values are never removed. A stale marker is cleared
    /// without touching its key; an exact current or historical public value is
    /// either marked (opt-in) or durably removed (opt-out).
    pub fn reconcile(
        &self,
        keys: &mut BTreeMap<String, String>,
        provenance: &mut ProvenanceMap,
        allowed: bool,
    ) -> ReconcileOutcome {
        let proven_by_marker: BTreeSet<String> = provenance
            .iter()
            .filter_map(|(provider, marker)| match keys.get(provider) {
                Some(api_key) if Self::marker_proves_public(api_key, marker) => {
                    Some(provider.clone())
                }
                _ => None,
            })
            .collect();
        let stale: Vec<String> = provenance
            .keys()
            .filter(|provider| !proven_by_marker.contains(*provider))
            .cloned()
            .collect();
        for provider in &stale {
            provenance.remove(provider);
        }

        let mut changed = !stale.is_empty();
        let mut removed_public = 0usize;
        if allowed {
            for (provider, api_keys) in keys.iter() {
                // A provider-level marker is safe only when the persisted value
                // resolves to exactly one known public key. Mixed rings remain
                // unmarked so a later opt-out cannot delete their user members.
                let Some(marker) = self.marker_for(provider, api_keys) else {
                    continue;
                };
                if provenance.get(provider) != Some(&marker) {
                    provenance.insert(provider.clone(), marker);
                    changed = true;
                }
            }
        } else {
            // A matching provenance marker proves the complete persisted value,
            // which by construction contains exactly one effective key.
            for provider in &proven_by_marker {
                keys.remove(provider);
                provenance.remove(provider);
                removed_public += 1;
            }
            changed |= !proven_by_marker.is_empty();

            // Legacy DB values and API values can contain a comma-separated key
            // ring. Remove each catalog-proven public member while preserving all
            // other effective members for the same provider.
            let remaining_providers: Vec<String> = keys.keys().cloned().collect();
            for provider in remaining_providers {
                let Some(api_keys) = keys.get(&provider) else {
                    continue;
                };
                let (retained, removed_here) = Self::filter_key_ring(api_keys, |api_key| {
                    self.is_public_value(&provider, api_key)
                });
                if removed_here == 0 {
                    continue;
                }
                removed_public += removed_here;
                changed = true;
                match retained {
                    Some(retained) => {
                        keys.insert(provider, retained);
                    }
                    None => {
                        keys.remove(&provider);
                    }
                }
            }
        }

        ReconcileOutcome {
            changed,
            removed_public,
            cleared_stale_markers: stale.len(),
        }
    }

    /// Add the strictly validated current seed and its proof markers.
    pub fn copy_current_seed_into(
        &self,
        keys: &mut BTreeMap<String, String>,
        provenance: &mut ProvenanceMap,
    ) {
        for (provider, api_key) in &self.credentials {
            keys.insert(provider.clone(), api_key.clone());
            if let Some(marker) = self.marker_for(provider, api_key) {
                provenance.insert(provider.clone(), marker);
            }
        }
    }
}

/// Load and strictly validate the public demo credential seed database.
///
/// Only one `dashscope-token-plan` row is accepted.  Opening is read-only, so a
/// normal application run can never mutate the repository asset.
pub(crate) fn load(path: &Path) -> Result<BTreeMap<String, String>, String> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("cannot open public demo credential database: {error}"))?;

    let application_id: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(|error| format!("cannot read public demo database application id: {error}"))?;
    if application_id != APPLICATION_ID {
        return Err("public demo credential database has an unexpected application id".to_owned());
    }
    let schema_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| format!("cannot read public demo database schema version: {error}"))?;
    if schema_version != SCHEMA_VERSION {
        return Err("public demo credential database has an unsupported schema version".to_owned());
    }

    let mut statement = connection
        .prepare(
            "SELECT provider, api_key, purpose
             FROM public_demo_credentials
             ORDER BY provider",
        )
        .map_err(|error| format!("public demo credential table is unavailable: {error}"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|error| format!("cannot query public demo credentials: {error}"))?;

    let mut credentials = BTreeMap::new();
    for row in rows {
        let (provider, api_key, purpose) =
            row.map_err(|error| format!("invalid public demo credential row: {error}"))?;
        if provider != EXPECTED_PROVIDER || purpose != EXPECTED_PURPOSE {
            return Err("public demo credential database contains an unexpected row".to_owned());
        }
        let api_key = api_key.trim();
        if !api_key.starts_with("sk-sp-") || !(40..=512).contains(&api_key.len()) {
            return Err("public demo credential has an invalid shape".to_owned());
        }
        if credentials.insert(provider, api_key.to_owned()).is_some() {
            return Err("public demo credential database contains a duplicate provider".to_owned());
        }
    }
    if credentials.len() != 1 {
        return Err("public demo credential database must contain exactly one row".to_owned());
    }
    Ok(credentials)
}

/// Load the current seed and combine it with the append-only historical digest
/// catalog. Neither keys nor fingerprints are ever included in errors or logs.
pub(crate) fn load_catalog(path: &Path) -> Result<Catalog, String> {
    let credentials = load(path)?;
    let mut known_sha256: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (provider, api_key) in &credentials {
        known_sha256
            .entry(provider.clone())
            .or_default()
            .insert(sha256_hex(api_key));
    }
    for (provider, digest) in HISTORICAL_PUBLIC_DEMO_KEY_SHA256 {
        if !is_sha256_hex(digest) {
            return Err("historical public demo credential fingerprint is invalid".to_owned());
        }
        known_sha256
            .entry((*provider).to_owned())
            .or_default()
            .insert((*digest).to_owned());
    }
    Ok(Catalog {
        credentials,
        known_sha256,
    })
}

fn sha256_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn effective_key_items(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_seed_is_readable_and_contains_only_the_public_demo_provider() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let credentials = load(&path).expect("tracked public demo database is valid");
        assert_eq!(credentials.len(), 1);
        assert!(credentials.contains_key(EXPECTED_PROVIDER));
    }

    #[test]
    fn tracked_seed_catalog_contains_the_expected_current_fingerprint() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let api_key = catalog
            .credentials()
            .get(EXPECTED_PROVIDER)
            .expect("tracked provider");
        let digest = sha256_hex(api_key);
        assert!(catalog.is_public_value(EXPECTED_PROVIDER, api_key));
        assert_eq!(
            digest,
            "04f610b848356f68380f534627a2d1b8ad3975a973c4cdfc523c94a6c2529ce8"
        );

        let marker = catalog
            .marker_for(EXPECTED_PROVIDER, api_key)
            .expect("current public value has provenance");
        assert!(Catalog::marker_proves_public(api_key, &marker));
        assert!(
            HISTORICAL_PUBLIC_DEMO_KEY_SHA256
                .iter()
                .any(|(provider, known_digest)| {
                    *provider == EXPECTED_PROVIDER && *known_digest == digest.as_str()
                })
        );
    }

    #[test]
    fn opt_out_recognizes_a_historical_fingerprint_after_seed_rotation() {
        let historical_value = "retired-public-demo-value-for-test";
        let catalog = Catalog {
            credentials: BTreeMap::new(),
            known_sha256: BTreeMap::from([(
                EXPECTED_PROVIDER.to_owned(),
                BTreeSet::from([sha256_hex(historical_value)]),
            )]),
        };
        let mut keys =
            BTreeMap::from([(EXPECTED_PROVIDER.to_owned(), historical_value.to_owned())]);
        let mut provenance = ProvenanceMap::new();

        let outcome = catalog.reconcile(&mut keys, &mut provenance, false);

        assert!(keys.is_empty());
        assert_eq!(outcome.removed_public, 1);
        assert!(outcome.changed);
    }

    #[test]
    fn stale_or_untrusted_markers_do_not_prove_a_key() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let api_key = catalog
            .credentials()
            .get(EXPECTED_PROVIDER)
            .expect("tracked provider");
        let wrong_source = KeyProvenance {
            source: "unexpected".to_owned(),
            sha256: sha256_hex(api_key),
        };
        assert!(!Catalog::marker_proves_public(api_key, &wrong_source));
        let stale_digest = KeyProvenance {
            source: PUBLIC_DEMO_SOURCE.to_owned(),
            sha256: "0".repeat(64),
        };
        assert!(!Catalog::marker_proves_public(api_key, &stale_digest));
    }

    #[test]
    fn opt_out_removes_only_exact_public_values_and_clears_stale_markers() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let public_key = catalog
            .credentials()
            .get(EXPECTED_PROVIDER)
            .expect("tracked provider")
            .clone();
        let mut keys = BTreeMap::from([
            (EXPECTED_PROVIDER.to_owned(), public_key),
            ("moonshot".to_owned(), "user-owned-key".to_owned()),
        ]);
        let mut provenance = BTreeMap::from([
            (
                EXPECTED_PROVIDER.to_owned(),
                catalog
                    .marker_for(
                        EXPECTED_PROVIDER,
                        keys.get(EXPECTED_PROVIDER).expect("public key"),
                    )
                    .expect("marker"),
            ),
            (
                "moonshot".to_owned(),
                KeyProvenance {
                    source: PUBLIC_DEMO_SOURCE.to_owned(),
                    sha256: "0".repeat(64),
                },
            ),
        ]);

        let outcome = catalog.reconcile(&mut keys, &mut provenance, false);
        assert!(outcome.changed);
        assert_eq!(outcome.removed_public, 1);
        assert_eq!(outcome.cleared_stale_markers, 1);
        assert!(!keys.contains_key(EXPECTED_PROVIDER));
        assert_eq!(
            keys.get("moonshot").map(String::as_str),
            Some("user-owned-key")
        );
        assert!(provenance.is_empty());
    }

    #[test]
    fn opt_out_filters_public_members_from_a_mixed_key_ring() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let public_key = catalog
            .credentials()
            .get(EXPECTED_PROVIDER)
            .expect("tracked provider");
        let mut keys = BTreeMap::from([(
            EXPECTED_PROVIDER.to_owned(),
            format!("  {public_key}, user-one ,, {public_key}, user-two  "),
        )]);
        let mut provenance = ProvenanceMap::new();

        let outcome = catalog.reconcile(&mut keys, &mut provenance, false);

        assert!(outcome.changed);
        assert_eq!(outcome.removed_public, 2);
        assert_eq!(
            keys.get(EXPECTED_PROVIDER).map(String::as_str),
            Some("user-one,user-two")
        );
        assert!(provenance.is_empty());
    }

    #[test]
    fn opt_out_removes_provider_when_key_ring_contains_only_public_and_empty_members() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let public_key = catalog
            .credentials()
            .get(EXPECTED_PROVIDER)
            .expect("tracked provider");
        let mut keys = BTreeMap::from([(
            EXPECTED_PROVIDER.to_owned(),
            format!(", {public_key},, {public_key},   "),
        )]);
        let mut provenance = ProvenanceMap::new();

        let outcome = catalog.reconcile(&mut keys, &mut provenance, false);

        assert_eq!(outcome.removed_public, 2);
        assert!(keys.is_empty());
        assert!(provenance.is_empty());
    }

    #[test]
    fn opt_in_backfills_a_marker_for_a_legacy_public_value() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let mut keys = catalog.credentials().clone();
        let mut provenance = ProvenanceMap::new();

        let outcome = catalog.reconcile(&mut keys, &mut provenance, true);
        assert!(outcome.changed);
        assert_eq!(outcome.removed_public, 0);
        assert_eq!(provenance.len(), 1);
    }

    #[test]
    fn matching_public_demo_marker_proves_a_value_outside_the_digest_catalog() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let provider = "moonshot".to_owned();
        let api_key = "previously-marked-public-demo-value".to_owned();
        assert!(!catalog.is_public_value(&provider, &api_key));
        let mut keys = BTreeMap::from([(provider.clone(), api_key.clone())]);
        let mut provenance = ProvenanceMap::from([(
            provider.clone(),
            KeyProvenance {
                source: PUBLIC_DEMO_SOURCE.to_owned(),
                sha256: sha256_hex(&api_key),
            },
        )]);

        let outcome = catalog.reconcile(&mut keys, &mut provenance, false);
        assert_eq!(outcome.removed_public, 1);
        assert!(keys.is_empty());
        assert!(provenance.is_empty());
    }

    #[test]
    fn a_single_key_marker_never_proves_or_deletes_a_mixed_key_ring() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let public_key = catalog
            .credentials()
            .get(EXPECTED_PROVIDER)
            .expect("tracked provider");
        let marker = catalog
            .marker_for(EXPECTED_PROVIDER, public_key)
            .expect("single public key marker");
        let mixed = format!("{public_key},user-owned-key");
        assert!(!Catalog::marker_proves_public(&mixed, &marker));
        assert!(catalog.marker_for(EXPECTED_PROVIDER, &mixed).is_none());

        let mut keys = BTreeMap::from([(EXPECTED_PROVIDER.to_owned(), mixed)]);
        let mut provenance = ProvenanceMap::from([(EXPECTED_PROVIDER.to_owned(), marker)]);
        let outcome = catalog.reconcile(&mut keys, &mut provenance, false);

        assert_eq!(outcome.cleared_stale_markers, 1);
        assert_eq!(outcome.removed_public, 1);
        assert_eq!(
            keys.get(EXPECTED_PROVIDER).map(String::as_str),
            Some("user-owned-key")
        );
        assert!(provenance.is_empty());
    }

    #[test]
    fn mismatched_public_demo_marker_is_cleared_without_removing_its_key() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let catalog = load_catalog(&path).expect("tracked public demo database is valid");
        let provider = "moonshot".to_owned();
        let mut keys = BTreeMap::from([(provider.clone(), "user-replacement-value".to_owned())]);
        let mut provenance = ProvenanceMap::from([(
            provider.clone(),
            KeyProvenance {
                source: PUBLIC_DEMO_SOURCE.to_owned(),
                sha256: "0".repeat(64),
            },
        )]);

        let outcome = catalog.reconcile(&mut keys, &mut provenance, false);
        assert_eq!(outcome.removed_public, 0);
        assert_eq!(outcome.cleared_stale_markers, 1);
        assert!(keys.contains_key(&provider));
        assert!(provenance.is_empty());
    }

    #[test]
    fn missing_seed_fails_closed() {
        let path = std::env::temp_dir().join(format!(
            "zkcode-missing-demo-credentials-{}.db",
            uuid::Uuid::new_v4()
        ));
        assert!(load(&path).is_err());
    }
}
