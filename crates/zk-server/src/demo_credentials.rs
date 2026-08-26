//! Public first-run demo credential seed.
//!
//! The repository tracks a deliberately public, minimal `SQLite` database under
//! `configuration/bootstrap/`.  It is **not** the user's runtime database: on a
//! clean first launch we open it read-only, validate its fixed schema and copy
//! the single allowed credential into the private runtime database.

use std::collections::BTreeMap;
use std::path::Path;

use rusqlite::{Connection, OpenFlags};

/// `SQLite` application id (`"ZKDD"`) for the public demo credential seed.
const APPLICATION_ID: i64 = 1_514_885_956;
const SCHEMA_VERSION: i64 = 1;
const EXPECTED_PROVIDER: &str = "dashscope-token-plan";
const EXPECTED_PURPOSE: &str = "public-first-run-demo";

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
    fn missing_seed_fails_closed() {
        let path = std::env::temp_dir().join(format!(
            "zkcode-missing-demo-credentials-{}.db",
            uuid::Uuid::new_v4()
        ));
        assert!(load(&path).is_err());
    }
}
