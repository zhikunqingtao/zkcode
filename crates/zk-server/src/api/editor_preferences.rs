//! Local editor preferences contain configuration only; key events stay in React.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::ApiError;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub(crate) struct EditorPreferences {
    pub vim_enabled: bool,
    /// Missing action uses the default. Empty string explicitly disables it.
    pub keybindings: BTreeMap<String, String>,
}

pub(crate) const ACTION_DEFAULTS: [(&str, &[&str]); 5] = [
    ("chat:submit", &["enter"]),
    ("chat:commandPalette", &["ctrl+k", "meta+k"]),
    ("chat:focus", &["ctrl+shift+i", "meta+shift+i"]),
    ("app:settings", &["ctrl+comma", "meta+comma"]),
    ("app:keybindings", &["ctrl+slash", "meta+slash"]),
];

impl EditorPreferences {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        let invalid = || ApiError::validation("Invalid or conflicting editor keybinding");
        if self
            .keybindings
            .keys()
            .any(|key| !ACTION_DEFAULTS.iter().any(|(action, _)| key == action))
        {
            return Err(invalid());
        }
        let mut resolved: Vec<Vec<String>> = Vec::new();
        for (action, defaults) in ACTION_DEFAULTS {
            let candidates = self.keybindings.get(action).map_or_else(
                || {
                    defaults
                        .iter()
                        .map(|key| (*key).to_owned())
                        .collect::<Vec<_>>()
                },
                |key| {
                    if key.is_empty() {
                        vec![]
                    } else {
                        vec![key.clone()]
                    }
                },
            );
            for candidate in candidates {
                if candidate.len() > 96 {
                    return Err(invalid());
                }
                let steps = candidate
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                if steps.is_empty() || steps.len() > 2 {
                    return Err(invalid());
                }
                let mut normalized = Vec::new();
                for step in steps {
                    normalized.push(normalize_step(action, &step)?);
                }
                if resolved.iter().any(|existing| {
                    existing.starts_with(&normalized) || normalized.starts_with(existing)
                }) {
                    return Err(invalid());
                }
                resolved.push(normalized);
            }
        }
        Ok(())
    }
}

fn normalize_step(action: &str, step: &str) -> Result<String, ApiError> {
    let invalid = || ApiError::validation("Invalid or reserved editor keybinding");
    let mut tokens = step.split('+').collect::<Vec<_>>();
    let key = tokens.pop().ok_or_else(invalid)?;
    let valid_key = (key.len() == 1 && key.as_bytes()[0].is_ascii_alphanumeric())
        || [
            "enter",
            "space",
            "up",
            "down",
            "left",
            "right",
            "home",
            "end",
            "pageup",
            "pagedown",
            "comma",
            "slash",
            "backspace",
            "delete",
        ]
        .contains(&key);
    if !valid_key || key.chars().any(char::is_uppercase) {
        return Err(invalid());
    }
    let modifiers = tokens
        .iter()
        .map(|modifier| {
            if *modifier == "mod" {
                "meta"
            } else {
                *modifier
            }
        })
        .collect::<Vec<_>>();
    if modifiers
        .iter()
        .any(|modifier| !["ctrl", "alt", "shift", "meta"].contains(modifier))
        || modifiers
            .iter()
            .enumerate()
            .any(|(index, modifier)| modifiers[..index].contains(modifier))
    {
        return Err(invalid());
    }
    if !(modifiers
        .iter()
        .any(|modifier| ["ctrl", "alt", "meta"].contains(modifier))
        || action == "chat:submit" && key == "enter" && modifiers.is_empty())
    {
        return Err(invalid());
    }
    // Keep native copy/paste/cut/undo and the immediate interrupt
    // path available independently of user remapping.
    if ["c", "v", "x", "z"].contains(&key)
        && modifiers
            .iter()
            .any(|modifier| ["ctrl", "meta"].contains(modifier))
    {
        return Err(invalid());
    }
    let mut parts = ["ctrl", "alt", "shift", "meta"]
        .into_iter()
        .filter(|modifier| modifiers.contains(modifier))
        .collect::<Vec<_>>();
    parts.push(key);
    Ok(parts.join("+"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_nonconflicting_chords_are_valid() {
        EditorPreferences::default().validate().unwrap();
        let mut preferences = EditorPreferences::default();
        preferences
            .keybindings
            .insert("chat:commandPalette".into(), "ctrl+k ctrl+g".into());
        preferences.validate().unwrap();
    }

    #[test]
    fn copy_interrupt_prefix_collisions_and_unknown_actions_are_rejected() {
        for (action, shortcut) in [
            ("chat:focus", "ctrl+c"),
            ("chat:focus", "ctrl+k ctrl+g"),
            ("chat:focus", "meta+k"),
            ("chat:submit", "j"),
            ("app:unknown", "ctrl+j"),
        ] {
            let mut preferences = EditorPreferences::default();
            preferences
                .keybindings
                .insert(action.into(), shortcut.into());
            assert!(preferences.validate().is_err(), "{action}: {shortcut}");
        }
    }
}
