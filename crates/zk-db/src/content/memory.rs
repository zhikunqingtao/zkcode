//! Bounded, session-owned body storage. There is deliberately no filesystem API.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::DbError;

const ENTRY_OVERHEAD: usize = 128;
const MAX_SCOPES: usize = 128;
const MAX_ATTACHED_SESSIONS: usize = 256;

/// Random identity of an immutable in-memory body. It is not a content hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentRef {
    #[serde(rename = "$zkEphemeralContent")]
    id: String,
}

struct Scope {
    sessions: HashSet<String>,
    entries: HashMap<ContentRef, Arc<[u8]>>,
    named: HashMap<(String, String), ContentRef>,
    charged_bytes: usize,
}

#[derive(Default)]
struct State {
    scopes: HashMap<String, Scope>,
    owners: HashMap<String, String>,
    charged_bytes: usize,
}

/// Content capacity is enforced before allocation; exhaustion never spills to disk.
/// The budgets include a fixed charge for each entry so empty values are bounded too.
pub struct MemoryContentStore {
    per_scope_limit: usize,
    global_limit: usize,
    state: Mutex<State>,
}

impl Default for MemoryContentStore {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024, 256 * 1024 * 1024)
    }
}

impl std::fmt::Debug for MemoryContentStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryContentStore")
            .field("per_scope_limit", &self.per_scope_limit)
            .field("global_limit", &self.global_limit)
            .finish_non_exhaustive()
    }
}

/// Keeps a root execution's content alive through result projection and cleanup.
/// Dropping the root lease expires every attached session's references together.
pub struct EphemeralContentLease {
    store: Arc<MemoryContentStore>,
    root: String,
}

impl Drop for EphemeralContentLease {
    fn drop(&mut self) {
        let mut state = self
            .store
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(scope) = state.scopes.remove(&self.root) {
            state.charged_bytes -= scope.charged_bytes;
            for session in scope.sessions {
                state.owners.remove(&session);
            }
        }
    }
}

impl MemoryContentStore {
    /// Configure retained-body limits. Zero capacity is a valid deny-all policy.
    #[must_use]
    pub fn new(per_scope_limit: usize, global_limit: usize) -> Self {
        Self {
            per_scope_limit,
            global_limit,
            state: Mutex::default(),
        }
    }

    /// Reserve a content scope before creating its ephemeral Session metadata.
    ///
    /// # Errors
    /// Duplicate identities and scope capacity exhaustion are explicit failures.
    pub fn begin(self: &Arc<Self>, root: &str) -> Result<EphemeralContentLease, DbError> {
        if uuid::Uuid::parse_str(root).is_err() {
            return Err(DbError::Validation("EPHEMERAL_SESSION_ID_INVALID".into()));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.owners.contains_key(root) {
            return Err(DbError::Conflict("EPHEMERAL_SESSION_EXISTS".into()));
        }
        if state.scopes.len() >= MAX_SCOPES {
            return Err(DbError::Validation("EPHEMERAL_SCOPE_CAPACITY".into()));
        }
        state.scopes.insert(
            root.to_owned(),
            Scope {
                sessions: HashSet::from([root.to_owned()]),
                entries: HashMap::new(),
                named: HashMap::new(),
                charged_bytes: 0,
            },
        );
        state.owners.insert(root.to_owned(), root.to_owned());
        Ok(EphemeralContentLease {
            store: Arc::clone(self),
            root: root.to_owned(),
        })
    }

    /// Attach a child transcript to the exact live parent's lifetime and capacity.
    ///
    /// # Errors
    /// Expired parents, reassignment and attachment capacity exhaustion are rejected.
    pub fn attach(&self, parent: &str, session: &str) -> Result<(), DbError> {
        if uuid::Uuid::parse_str(session).is_err() {
            return Err(DbError::Validation("EPHEMERAL_SESSION_ID_INVALID".into()));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = state
            .owners
            .get(parent)
            .cloned()
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        if let Some(existing) = state.owners.get(session) {
            return if existing == &root {
                Ok(())
            } else {
                Err(DbError::Conflict("EPHEMERAL_OWNER_MISMATCH".into()))
            };
        }
        let scope = state
            .scopes
            .get_mut(&root)
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        if scope.sessions.len() >= MAX_ATTACHED_SESSIONS {
            return Err(DbError::Validation("EPHEMERAL_ATTACHMENT_CAPACITY".into()));
        }
        scope.sessions.insert(session.to_owned());
        state.owners.insert(session.to_owned(), root);
        Ok(())
    }

    /// Seal bytes and return an opaque, non-content-derived reference.
    ///
    /// # Errors
    /// Expired sessions and either memory budget limit fail before accepting bytes.
    pub fn put(&self, session: &str, bytes: &[u8]) -> Result<ContentRef, DbError> {
        let charge = bytes
            .len()
            .checked_add(ENTRY_OVERHEAD)
            .ok_or_else(|| DbError::Validation("EPHEMERAL_CONTENT_CAPACITY".into()))?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = state
            .owners
            .get(session)
            .cloned()
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        if charge > self.global_limit.saturating_sub(state.charged_bytes) {
            return Err(DbError::Validation("EPHEMERAL_CONTENT_CAPACITY".into()));
        }
        let scope = state
            .scopes
            .get_mut(&root)
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        if charge > self.per_scope_limit.saturating_sub(scope.charged_bytes) {
            return Err(DbError::Validation("EPHEMERAL_CONTENT_CAPACITY".into()));
        }
        let reference = ContentRef {
            id: uuid::Uuid::new_v4().to_string(),
        };
        scope.entries.insert(reference.clone(), Arc::from(bytes));
        scope.charged_bytes += charge;
        state.charged_bytes += charge;
        Ok(reference)
    }

    /// Store immutable named bytes without persisting either their name or body.
    /// Names are scoped to the exact Session; attached sessions share capacity,
    /// not implicit access to each other's named evidence.
    ///
    /// # Errors
    /// Expired owners, mismatched replacements and capacity exhaustion fail closed.
    pub fn put_named_bytes(
        &self,
        session: &str,
        name: &str,
        bytes: &[u8],
    ) -> Result<ContentRef, DbError> {
        if name.is_empty() || name.len() > 256 {
            return Err(DbError::Validation("EPHEMERAL_CONTENT_NAME_INVALID".into()));
        }
        let charge = bytes
            .len()
            .checked_add(ENTRY_OVERHEAD * 2)
            .and_then(|n| n.checked_add(name.len()))
            .and_then(|n| n.checked_add(session.len()))
            .ok_or_else(|| DbError::Validation("EPHEMERAL_CONTENT_CAPACITY".into()))?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = state
            .owners
            .get(session)
            .cloned()
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        let key = (session.to_owned(), name.to_owned());
        let scope = state
            .scopes
            .get(&root)
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        if let Some(reference) = scope.named.get(&key) {
            return if scope
                .entries
                .get(reference)
                .is_some_and(|existing| existing.as_ref() == bytes)
            {
                Ok(reference.clone())
            } else {
                Err(DbError::Conflict(
                    "EPHEMERAL_CONTENT_IMMUTABLE_MISMATCH".into(),
                ))
            };
        }
        if charge > self.global_limit.saturating_sub(state.charged_bytes)
            || charge > self.per_scope_limit.saturating_sub(scope.charged_bytes)
        {
            return Err(DbError::Validation("EPHEMERAL_CONTENT_CAPACITY".into()));
        }
        let scope = state
            .scopes
            .get_mut(&root)
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        let reference = ContentRef {
            id: uuid::Uuid::new_v4().to_string(),
        };
        scope.entries.insert(reference.clone(), Arc::from(bytes));
        scope.named.insert(key, reference.clone());
        scope.charged_bytes += charge;
        state.charged_bytes += charge;
        Ok(reference)
    }

    /// Resolve an exact Session's named bytes; names and digests never become credentials.
    ///
    /// # Errors
    /// Missing, expired and foreign names are rejected.
    pub fn get_named_bytes(&self, session: &str, name: &str) -> Result<Arc<[u8]>, DbError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scope = state
            .owners
            .get(session)
            .and_then(|root| state.scopes.get(root))
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        scope
            .named
            .get(&(session.to_owned(), name.to_owned()))
            .and_then(|reference| scope.entries.get(reference))
            .cloned()
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_NOT_OWNED".into()))
    }

    /// Resolve a reference only within its authorized root/attached scope.
    ///
    /// # Errors
    /// Missing or cross-scope references fail without exposing other bodies.
    pub fn get(&self, session: &str, reference: &ContentRef) -> Result<Arc<[u8]>, DbError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scope = state
            .owners
            .get(session)
            .and_then(|root| state.scopes.get(root))
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_EXPIRED".into()))?;
        scope
            .entries
            .get(reference)
            .cloned()
            .ok_or_else(|| DbError::Invalid("EPHEMERAL_CONTENT_NOT_OWNED".into()))
    }

    /// Whether a Session still has its authorized in-memory content owner.
    #[must_use]
    pub fn has_live_scope(&self, session: &str) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .owners
            .get(session)
            .is_some_and(|root| state.scopes.contains_key(root))
    }

    /// Content-free diagnostics; no bodies or body hashes are emitted.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .charged_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attached_scope_is_shared_bounded_and_expires_atomically() {
        let store = Arc::new(MemoryContentStore::new(260, 520));
        let root = uuid::Uuid::new_v4().to_string();
        let child = uuid::Uuid::new_v4().to_string();
        let lease = store.begin(&root).unwrap();
        store.attach(&root, &child).unwrap();
        let reference = store.put(&child, b"abc").unwrap();
        assert_eq!(&*store.get(&root, &reference).unwrap(), b"abc");
        store.put(&root, b"x").unwrap();
        assert!(store.put(&child, b"").is_err());
        assert_eq!(store.retained_bytes(), 260);
        drop(lease);
        assert_eq!(store.retained_bytes(), 0);
        assert!(store.get(&child, &reference).is_err());
        assert!(store.put(&root, b"late").is_err());
    }

    #[test]
    fn unrelated_scope_cannot_read_references_and_global_budget_is_atomic() {
        let store = Arc::new(MemoryContentStore::new(512, 260));
        let a = uuid::Uuid::new_v4().to_string();
        let b = uuid::Uuid::new_v4().to_string();
        let _a = store.begin(&a).unwrap();
        let b_lease = store.begin(&b).unwrap();
        let reference = store.put(&a, b"a").unwrap();
        assert!(store.get(&b, &reference).is_err());
        assert!(store.attach(&a, &b).is_err());
        store.put(&b, b"b").unwrap();
        assert!(store.put(&a, b"").is_err());
        drop(b_lease);
        store.put(&a, b"c").unwrap();
    }
}

#[cfg(test)]
mod named_tests {
    use super::*;
    #[test]
    fn named_binary_storage_is_immutable_bounded_and_exactly_owned() {
        let store = Arc::new(MemoryContentStore::new(600, 600));
        let session = uuid::Uuid::new_v4().to_string();
        let child = uuid::Uuid::new_v4().to_string();
        let lease = store.begin(&session).unwrap();
        store.attach(&session, &child).unwrap();
        let reference = store
            .put_named_bytes(&session, "evidence:hash", b"body")
            .unwrap();
        let charged = store.retained_bytes();
        assert!(charged >= b"body".len() + "evidence:hash".len() + session.len());
        assert_eq!(
            store
                .put_named_bytes(&session, "evidence:hash", b"body")
                .unwrap(),
            reference
        );
        assert_eq!(store.retained_bytes(), charged);
        assert!(
            store
                .put_named_bytes(&session, "evidence:hash", b"changed")
                .is_err()
        );
        assert!(store.get_named_bytes(&child, "evidence:hash").is_err());
        assert!(
            store
                .put_named_bytes(&session, "evidence:other", &[1; 500])
                .is_err()
        );
        assert_eq!(store.retained_bytes(), charged);
        drop(lease);
        assert_eq!(store.retained_bytes(), 0);
        assert!(store.get_named_bytes(&session, "evidence:hash").is_err());
    }
}
