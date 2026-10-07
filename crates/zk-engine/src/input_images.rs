//! Explicit current-user image references. Historical text is never parsed.
use std::path::{Path, PathBuf};

use zk_protocol::Reference;

pub(crate) fn is_image_reference(reference: &Reference) -> bool {
    reference.kind == "image" || reference.kind == "file" && is_image_path(&reference.path)
}

fn is_image_path(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp"
            )
        })
}

/// Only standalone @path or @"path with spaces" tokens outside Markdown code.
/// Emails, URLs, escaped @, inline code and fenced examples grant no file access.
pub(crate) fn append_explicit_images(text: &str, references: &mut Vec<Reference>) {
    let mut fence: Option<char> = None;
    let mut inline_ticks = 0;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            let marker = trimmed.chars().next().unwrap_or('`');
            if fence == Some(marker) {
                fence = None;
            } else if fence.is_none() {
                fence = Some(marker);
            }
            continue;
        }
        if fence.is_some() {
            continue;
        }
        let chars: Vec<_> = line.char_indices().collect();
        let mut cursor = 0;
        while cursor < chars.len() {
            let (_, ch) = chars[cursor];
            if ch == '`' {
                let start = cursor;
                while cursor < chars.len() && chars[cursor].1 == '`' {
                    cursor += 1;
                }
                let count = cursor - start;
                if inline_ticks == 0 {
                    inline_ticks = count;
                } else if inline_ticks == count {
                    inline_ticks = 0;
                }
                continue;
            }
            let boundary = cursor == 0
                || chars[cursor - 1].1.is_whitespace()
                || matches!(chars[cursor - 1].1, '(' | '[' | '{');
            if ch != '@' || inline_ticks != 0 || !boundary {
                cursor += 1;
                continue;
            }
            cursor += 1;
            let Some(&(start, next)) = chars.get(cursor) else {
                break;
            };
            let (path_start, end, next_cursor) = if matches!(next, '"' | '\'') {
                let Some(relative_end) = chars[cursor + 1..].iter().position(|(_, ch)| *ch == next)
                else {
                    break;
                };
                let end_cursor = cursor + 1 + relative_end;
                (start + next.len_utf8(), chars[end_cursor].0, end_cursor + 1)
            } else {
                let mut end_cursor = cursor;
                while end_cursor < chars.len()
                    && !chars[end_cursor].1.is_whitespace()
                    && !matches!(chars[end_cursor].1, ')' | ']' | '}' | ',' | ';' | '`')
                {
                    end_cursor += 1;
                }
                (
                    start,
                    chars
                        .get(end_cursor)
                        .map_or(line.len(), |(index, _)| *index),
                    end_cursor,
                )
            };
            cursor = next_cursor;
            let path = &line[path_start..end];
            if path.len() <= 4096
                && is_image_path(path)
                && !references.iter().any(|reference| reference.path == path)
            {
                if references.len() > 20 {
                    return;
                }
                references.push(Reference {
                    kind: "image".into(),
                    path: path.into(),
                    start_line: None,
                    end_line: None,
                });
            }
        }
    }
}

pub(crate) fn authorized_path(
    workspace: &Path,
    reference: &Reference,
) -> Result<PathBuf, (&'static str, String)> {
    if reference.start_line.is_some() || reference.end_line.is_some() {
        return Err((
            "IMAGE_REFERENCE_RANGE_INVALID",
            "Image references cannot specify text line ranges".into(),
        ));
    }
    let path = Path::new(&reference.path);
    let lexical = zk_tools::file_state::normalize_path(&if path.is_absolute() {
        path.to_owned()
    } else {
        workspace.join(path)
    });
    let canonical = lexical.canonicalize().map_err(|_| {
        (
            "REFERENCE_NOT_FOUND",
            "Referenced image was not found".into(),
        )
    })?;
    if !canonical.starts_with(workspace) || canonical != lexical {
        return Err(("REFERENCE_PATH_FORBIDDEN", "Image references must stay inside the authorized session workspace without symbolic links".into()));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_image_tokens_preserve_text_and_ignore_non_authorizing_examples() {
        let text = "inspect @assets/demo.png and @\"assets/中文 图.webp\"\nuser@host.png https://host/@image.png \\@escaped.jpg `@code.jpg`\n```text\n@fenced.gif\n```\n[@last.bmp] @assets/demo.png";
        let mut references = Vec::new();
        append_explicit_images(text, &mut references);
        assert_eq!(
            references
                .iter()
                .map(|reference| reference.path.as_str())
                .collect::<Vec<_>>(),
            ["assets/demo.png", "assets/中文 图.webp", "last.bmp"]
        );
        assert!(references.iter().all(|reference| reference.kind == "image"));
    }
}
