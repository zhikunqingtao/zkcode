//! Line locations from the exact displayed Git patch, never test/verification evidence.
use serde::Serialize;
use std::collections::BTreeSet;

const MAX_LOCATIONS: usize = 20_000;
const MAX_FILES: usize = 1_000;

/// Locations use the new revision for additions and the old revision for removals.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChangedFile {
    old_path: Option<String>,
    file_path: Option<String>,
    changed_lines: BTreeSet<u64>,
    removed_lines: BTreeSet<u64>,
    /// Neighbouring new-revision positions, not claims that a removed line still exists.
    deletion_anchors: BTreeSet<u64>,
    binary: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChangedLocations {
    pub(crate) files: Vec<ChangedFile>,
    pub(crate) complete: bool,
    pub(crate) reason: Option<&'static str>,
    analysis_kind: &'static str,
    is_verification_evidence: bool,
}

/// Truncation/malformed paths never appear as a complete, empty change set.
pub(crate) fn parse(diff: &str, captured_completely: bool) -> ChangedLocations {
    let mut result = ChangedLocations {
        files: Vec::new(),
        complete: captured_completely,
        reason: (!captured_completely).then_some("patch_truncated"),
        analysis_kind: "advisory",
        is_verification_evidence: false,
    };
    let mut file = None::<ChangedFile>;
    let mut old = 0_u64;
    let mut new = 0_u64;
    let mut hunk = false;
    let mut count = 0;
    for line in diff.lines() {
        if line.starts_with("diff --git ") {
            if let Some(current) = file.take() {
                result.files.push(current);
            }
            if result.files.len() >= MAX_FILES {
                result.complete = false;
                result.reason = Some("location_limit");
                break;
            }
            file = Some(ChangedFile::default());
            hunk = false;
            continue;
        }
        let Some(current) = file.as_mut() else {
            continue;
        };
        if line.starts_with("@@ ") {
            if let Some((a, b)) = hunk_start(line) {
                old = a;
                new = b;
                hunk = true;
            } else {
                hunk = false;
                result.complete = false;
                result.reason = Some("invalid_hunk");
            }
            continue;
        }
        if hunk {
            hunk = advance_hunk(current, line, &mut old, &mut new, &mut count);
            if count >= MAX_LOCATIONS {
                result.complete = false;
                result.reason = Some("location_limit");
                break;
            }
            continue;
        }
        if let Some(raw) = line.strip_prefix("--- ") {
            if let Ok(decoded) = path(raw, "a/") {
                current.old_path = decoded;
            } else {
                result.complete = false;
                result.reason = Some("path_encoding_unsupported");
            }
        } else if let Some(raw) = line.strip_prefix("+++ ") {
            if let Ok(decoded) = path(raw, "b/") {
                current.file_path = decoded;
            } else {
                result.complete = false;
                result.reason = Some("path_encoding_unsupported");
            }
        } else if line.starts_with("Binary files ") || line == "GIT binary patch" {
            current.binary = true;
            result.complete = false;
            result.reason = Some("binary_line_locations_unavailable");
        } else if let Some(raw) = line.strip_prefix("rename from ") {
            current.old_path = path(raw, "").ok().flatten();
        } else if let Some(raw) = line.strip_prefix("rename to ") {
            current.file_path = path(raw, "").ok().flatten();
        }
    }
    if let Some(current) = file {
        result.files.push(current);
    }
    for file in &mut result.files {
        if file.file_path.is_none() {
            file.deletion_anchors.clear();
        }
        if file.old_path.is_none() && file.file_path.is_none() {
            result.complete = false;
            result.reason.get_or_insert("path_unavailable");
        }
    }
    result
}

fn advance_hunk(
    file: &mut ChangedFile,
    line: &str,
    old: &mut u64,
    new: &mut u64,
    count: &mut usize,
) -> bool {
    match line.as_bytes().first() {
        Some(b'+') => {
            file.changed_lines.insert(*new);
            *new += 1;
            *count += 1;
        }
        Some(b'-') => {
            file.removed_lines.insert(*old);
            file.deletion_anchors.insert((*new).max(1));
            *old += 1;
            *count += 1;
        }
        Some(b' ') => {
            *old += 1;
            *new += 1;
        }
        Some(b'\\') => {}
        _ => return false,
    }
    true
}

fn hunk_start(line: &str) -> Option<(u64, u64)> {
    let mut parts = line.strip_prefix("@@ ")?.split_ascii_whitespace();
    let old = parts
        .next()?
        .strip_prefix('-')?
        .split(',')
        .next()?
        .parse()
        .ok()?;
    let new = parts
        .next()?
        .strip_prefix('+')?
        .split(',')
        .next()?
        .parse()
        .ok()?;
    (parts.next()? == "@@").then_some((old, new))
}

fn path(raw: &str, prefix: &str) -> Result<Option<String>, ()> {
    // Git terminates ambiguous patch-header paths with a literal TAB, including
    // after a C-quoted name. A TAB inside a filename is escaped inside quotes.
    let raw = raw.strip_suffix('\t').unwrap_or(raw);
    if raw == "/dev/null" {
        return Ok(None);
    }
    let bytes = if let Some(quoted) = raw
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        let mut source = quoted.as_bytes().iter().copied().peekable();
        let mut result = Vec::new();
        while let Some(ch) = source.next() {
            if ch != b'\\' {
                result.push(ch);
                continue;
            }
            let next = source.next().ok_or(())?;
            let escaped = match next {
                b'\\' | b'"' => next,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'b' => 8,
                b'f' => 12,
                b'v' => 11,
                b'a' => 7,
                b'0'..=b'7' => {
                    let mut value = u16::from(next - b'0');
                    for _ in 0..2 {
                        match source.peek() {
                            Some(b'0'..=b'7') => {
                                value = value * 8 + u16::from(source.next().ok_or(())? - b'0');
                            }
                            _ => break,
                        }
                    }
                    u8::try_from(value).map_err(|_| ())?
                }
                _ => return Err(()),
            };
            result.push(escaped);
        }
        result
    } else {
        raw.as_bytes().to_vec()
    };
    let value = String::from_utf8(bytes).map_err(|_| ())?;
    let value = value.strip_prefix(prefix).ok_or(())?;
    if value.is_empty()
        || value.starts_with('/')
        || value.split('/').any(|part| part == "..")
        || value.contains('\0')
    {
        return Err(());
    }
    Ok(Some(value.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_additions_removals_context_and_git_quoted_unicode_paths() {
        let patch = "diff --git a/a b/a\n--- \"a/\\344\\270\\255 file.py\"\t\n+++ \"b/\\344\\270\\255 file.py\"\t\n@@ -10,4 +10,4 @@ function\n keep\n-old\n+new\n--- is source text, not a filename\n+++ is source text\n keep\n";
        let result = parse(patch, true);
        assert!(result.complete);
        assert_eq!(result.files[0].file_path.as_deref(), Some("中 file.py"));
        assert_eq!(result.files[0].changed_lines, BTreeSet::from([11, 12]));
        assert_eq!(result.files[0].removed_lines, BTreeSet::from([11, 12]));
        assert!(!result.is_verification_evidence);
    }
    #[test]
    fn deleted_and_binary_files_never_invent_new_revision_lines() {
        let patch = "diff --git a/a.py b/a.py\n--- a/a.py\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-old\n-gone\ndiff --git a/a.png b/a.png\nBinary files a/a.png and b/a.png differ\n";
        let result = parse(patch, true);
        assert!(!result.complete);
        assert!(result.files[0].changed_lines.is_empty());
        assert!(result.files[0].deletion_anchors.is_empty());
        assert!(result.files[1].binary);
        assert!(!parse("", false).complete);
    }
}
