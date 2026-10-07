//! Capacity overload is scoped to a provider's actual requested model, not the
//! provider circuit. The configured fallback chain remains the only route set.
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use crate::ProviderError;

const DEFAULT_COOLDOWN_MS: u64 = 30 * 60 * 1000;
const MIN_COOLDOWN_MS: u64 = 10 * 60 * 1000;
const SHORT_RETRY_MS: u64 = 20 * 1000;
const MAX_ENTRIES: usize = 4096;

#[derive(Debug, Default)]
pub(crate) struct ModelCooldowns(Mutex<HashMap<String, u64>>);

impl ModelCooldowns {
    pub(crate) fn record(&self, model: &str, error: &ProviderError) {
        if let ProviderError::Http {
            status: 529,
            retry_after_ms,
            ..
        } = error
        {
            self.record_at(model, *retry_after_ms, crate::clock::mono_millis());
        }
    }

    fn record_at(&self, model: &str, retry_after: Option<u64>, now: u64) {
        if retry_after.is_some_and(|delay| delay > 0 && delay < SHORT_RETRY_MS) {
            return;
        }
        let duration = retry_after
            .filter(|delay| *delay > 0)
            .map_or(DEFAULT_COOLDOWN_MS, |delay| delay.max(MIN_COOLDOWN_MS));
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        entries.retain(|_, expiry| *expiry > now);
        if entries.len() >= MAX_ENTRIES
            && !entries.contains_key(model)
            && let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, expiry)| **expiry)
                .map(|(model, _)| model.clone())
        {
            entries.remove(&oldest);
        }
        entries.insert(model.to_owned(), now.saturating_add(duration));
    }

    pub(crate) fn available(&self, model: &str) -> bool {
        self.available_at(model, crate::clock::mono_millis())
    }

    fn available_at(&self, model: &str, now: u64) -> bool {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let available = entries.get(model).is_none_or(|expiry| *expiry <= now);
        if available {
            entries.remove(model);
        }
        available
    }

    pub(crate) fn success(&self, model: &str) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(model);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overload_is_model_local_bounded_and_recovers_without_background_calls() {
        let state = ModelCooldowns::default();
        state.record_at("overloaded", Some(10_000), 100);
        assert!(state.available_at("overloaded", 101));
        state.record_at("overloaded", Some(20_000), 100);
        assert!(!state.available_at("overloaded", 100 + MIN_COOLDOWN_MS - 1));
        assert!(state.available_at("unaffected", 101));
        assert!(ModelCooldowns::default().available_at("overloaded", 101));
        assert!(state.available_at("overloaded", 100 + MIN_COOLDOWN_MS));
        state.record_at("overloaded", None, 100);
        assert!(!state.available_at("overloaded", 100 + MIN_COOLDOWN_MS));
        state.success("overloaded");
        assert!(state.available_at("overloaded", 101));
    }
}
