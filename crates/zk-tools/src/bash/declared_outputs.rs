//! Bounded pre-execution declarations and descriptor-bound post-execution seals.
use crate::input::failure;
use crate::tool::ToolOutput;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::Read,
    path::{Path, PathBuf},
};

const MAX_OUTPUTS: usize = 32;
const MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 100 * 1024 * 1024;

/// Native receipt derived from descriptor-bound before/after file bytes.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeclaredOutputReceipt {
    /// Original requested declaration path.
    pub requested_path: String,
    /// Descriptor-verified path within the owning workspace.
    pub canonical_path: String,
    /// Canonical created, modified, or deleted operation.
    pub operation: String,
    /// Actual original byte identity, when previously present.
    pub previous_hash: Option<String>,
    /// Actual final byte identity, absent for deletion.
    pub sealed_hash: Option<String>,
    /// Actual final bytes, absent for deletion.
    pub file_size: Option<u64>,
    /// Pending validator requirement; not validation evidence.
    pub required_validator_id: Option<String>,
}

pub(super) struct FrozenOutput {
    receipt: DeclaredOutputReceipt,
    anchor: PathBuf,
    anchor_identity: (u64, u64),
}

fn operation(value: &str) -> Option<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "created" | "create" => Some("created"),
        "modified" | "update" => Some("modified"),
        "deleted" | "delete" => Some("deleted"),
        _ => None,
    }
}
fn error(code: &str) -> ToolOutput {
    failure(
        code,
        "Declared output could not be safely verified; no artifact is claimed and command effects must not be retried automatically",
    )
}
fn identity(path: &Path) -> Result<(u64, u64), ToolOutput> {
    use std::os::unix::fs::MetadataExt;
    let meta =
        std::fs::symlink_metadata(path).map_err(|_| error("BASH_OUTPUT_PARENT_UNAVAILABLE"))?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(error("BASH_OUTPUT_PATH_UNSAFE"));
    }
    Ok((meta.dev(), meta.ino()))
}
fn reject_symlinks(path: &Path) -> Result<(), ToolOutput> {
    let mut prefix = PathBuf::from("/");
    for component in path.components().skip(1) {
        let std::path::Component::Normal(name) = component else {
            return Err(error("BASH_OUTPUT_PATH_UNSAFE"));
        };
        prefix.push(name);
        match std::fs::symlink_metadata(&prefix) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(error("BASH_OUTPUT_PATH_UNSAFE"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(_) => return Err(error("BASH_OUTPUT_PATH_UNAVAILABLE")),
        }
    }
    Ok(())
}
fn digest(path: &Path) -> Result<(String, u64), ToolOutput> {
    use std::os::unix::fs::MetadataExt;
    let mut file =
        crate::safe_file::open_bound_regular(path).map_err(|_| error("BASH_OUTPUT_FILE_UNSAFE"))?;
    let before = file
        .metadata()
        .map_err(|_| error("BASH_OUTPUT_READ_FAILED"))?;
    if before.len() > MAX_FILE_BYTES {
        return Err(error("BASH_OUTPUT_SIZE_LIMIT"));
    }
    let mut hash = Sha256::new();
    let mut buf = vec![0_u8; 65536];
    let mut total = 0_u64;
    loop {
        let count = file
            .read(&mut buf)
            .map_err(|_| error("BASH_OUTPUT_READ_FAILED"))?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > MAX_FILE_BYTES {
            return Err(error("BASH_OUTPUT_SIZE_LIMIT"));
        }
        hash.update(&buf[..count]);
    }
    let after = file
        .metadata()
        .map_err(|_| error("BASH_OUTPUT_READ_FAILED"))?;
    let current = std::fs::symlink_metadata(path).map_err(|_| error("BASH_OUTPUT_CHANGED"))?;
    if before.len() != total
        || after.len() != total
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
        || after.dev() != current.dev()
        || after.ino() != current.ino()
    {
        return Err(error("BASH_OUTPUT_CHANGED"));
    }
    Ok((format!("{:x}", hash.finalize()), total))
}

pub(super) fn freeze(
    input: &Value,
    cwd: &Path,
    workspace: &Path,
) -> Result<Vec<FrozenOutput>, ToolOutput> {
    let Some(value) = input.get("declared_outputs") else {
        return Ok(Vec::new());
    };
    let items = value
        .as_array()
        .ok_or_else(|| error("BASH_OUTPUT_DECLARATION_INVALID"))?;
    if items.len() > MAX_OUTPUTS {
        return Err(error("BASH_OUTPUT_COUNT_LIMIT"));
    }
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let workspace = workspace
        .canonicalize()
        .map_err(|_| error("BASH_OUTPUT_WORKSPACE_UNAVAILABLE"))?;
    let mut seen = BTreeSet::new();
    let mut total = 0_u64;
    let mut result = Vec::new();
    for item in items {
        let raw = item
            .get("path")
            .and_then(Value::as_str)
            .filter(|path| !path.trim().is_empty() && !path.contains('\0'))
            .ok_or_else(|| error("BASH_OUTPUT_DECLARATION_INVALID"))?;
        let operation = item
            .get("operation")
            .and_then(Value::as_str)
            .and_then(operation)
            .ok_or_else(|| error("BASH_OUTPUT_OPERATION_INVALID"))?;
        let requested = Path::new(raw);
        let path = crate::file_state::normalize_path(&if requested.is_absolute() {
            requested.to_owned()
        } else {
            cwd.join(requested)
        });
        // Resolve an alias for the authorized workspace itself (not symlinks
        // within it), e.g. macOS /tmp -> /private/tmp. Descendants are still
        // descriptor-checked with O_NOFOLLOW below.
        let path = if path.starts_with(&workspace) {
            path
        } else {
            let alias = path
                .ancestors()
                .find(|ancestor| ancestor.canonicalize().ok().as_ref() == Some(&workspace))
                .ok_or_else(|| error("BASH_OUTPUT_PATH_INVALID"))?;
            workspace.join(
                path.strip_prefix(alias)
                    .map_err(|_| error("BASH_OUTPUT_PATH_INVALID"))?,
            )
        };
        if !path.starts_with(&workspace) || path == workspace || !seen.insert(path.clone()) {
            return Err(error("BASH_OUTPUT_PATH_INVALID"));
        }
        reject_symlinks(&path)?;
        let mut anchor = path
            .parent()
            .ok_or_else(|| error("BASH_OUTPUT_PATH_INVALID"))?
            .to_owned();
        while !anchor.exists() {
            if !anchor.pop() {
                return Err(error("BASH_OUTPUT_PATH_INVALID"));
            }
        }
        let anchor_identity = identity(&anchor)?;
        let prior = match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                let (hash, size) = digest(&path)?;
                total = total.saturating_add(size);
                if total > MAX_TOTAL_BYTES {
                    return Err(error("BASH_OUTPUT_TOTAL_SIZE_LIMIT"));
                }
                Some(hash)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return Err(error("BASH_OUTPUT_PATH_UNAVAILABLE")),
        };
        if (operation == "created") != prior.is_none() {
            return Err(error("BASH_OUTPUT_PRECONDITION_FAILED"));
        }
        let validator = match item.get("requiredValidatorId") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if !value.trim().is_empty() && value.len() <= 256 => {
                Some(value.clone())
            }
            _ => return Err(error("BASH_OUTPUT_VALIDATOR_INVALID")),
        };
        result.push(FrozenOutput {
            receipt: DeclaredOutputReceipt {
                requested_path: raw.into(),
                canonical_path: path.to_string_lossy().into_owned(),
                operation: operation.into(),
                previous_hash: prior,
                sealed_hash: None,
                file_size: None,
                required_validator_id: validator,
            },
            anchor,
            anchor_identity,
        });
    }
    Ok(result)
}

pub(super) fn seal(outputs: Vec<FrozenOutput>, mut output: ToolOutput) -> ToolOutput {
    if outputs.is_empty() || output.is_error {
        return output;
    }
    let sealed = (|| -> Result<Vec<DeclaredOutputReceipt>, ToolOutput> {
        let mut result = Vec::new();
        let mut total = 0_u64;
        for mut frozen in outputs {
            let path = Path::new(&frozen.receipt.canonical_path);
            if identity(&frozen.anchor)? != frozen.anchor_identity {
                return Err(error("BASH_OUTPUT_PARENT_CHANGED"));
            }
            reject_symlinks(path)?;
            if frozen.receipt.operation == "deleted" {
                if !matches!(std::fs::symlink_metadata(path),Err(error) if error.kind()==std::io::ErrorKind::NotFound)
                {
                    return Err(error("BASH_OUTPUT_NOT_DELETED"));
                }
            } else {
                let (hash, size) = digest(path)?;
                total = total.saturating_add(size);
                if total > MAX_TOTAL_BYTES {
                    return Err(error("BASH_OUTPUT_TOTAL_SIZE_LIMIT"));
                }
                if frozen.receipt.previous_hash.as_ref() == Some(&hash) {
                    return Err(error("BASH_OUTPUT_UNCHANGED"));
                }
                frozen.receipt.sealed_hash = Some(hash);
                frozen.receipt.file_size = Some(size);
            }
            result.push(frozen.receipt);
        }
        Ok(result)
    })();
    match sealed {
        Ok(receipts) => {
            output.metadata.get_or_insert_with(|| serde_json::json!({}))["structuredResult"]["declaredOutputs"] =
                serde_json::to_value(receipts).expect("plain receipt");
        }
        Err(error) => {
            output.is_error = true;
            output.content.push('\n');
            output.content.push_str(&error.content);
            let meta = output.metadata.get_or_insert_with(|| serde_json::json!({}));
            meta["structuredResult"]["code"] = serde_json::json!("BASH_OUTPUT_SEAL_FAILED");
            meta["structuredResult"]["retryability"] = serde_json::json!("NEVER");
            meta["structuredResult"]["effectState"] = serde_json::json!("UNKNOWN");
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BashTool, Tool, ToolContext};
    use serde_json::json;
    fn fixture() -> (PathBuf, ToolContext) {
        let root = std::env::temp_dir().join(format!("zk-bash-outputs-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ToolContext::new(tokio_util::sync::CancellationToken::new(), tx)
            .with_session_id(uuid::Uuid::new_v4().to_string())
            .with_working_dir(&root);
        (root, ctx)
    }
    #[tokio::test]
    async fn explicit_create_modify_delete_capture_only_real_requested_effects() {
        let (root, ctx) = fixture();
        std::fs::write(root.join("old"), b"old").unwrap();
        std::fs::write(root.join("remove"), b"removed").unwrap();
        let output=BashTool.execute(json!({"command":"printf new > new; printf changed > old; rm remove; printf incidental > incidental","declared_outputs":[{"path":"new","operation":"created","requiredValidatorId":"document-structure"},{"path":"old","operation":"modified"},{"path":"remove","operation":"deleted"}]}),ctx).await;
        assert!(!output.is_error, "{}", output.content);
        let receipts: Vec<DeclaredOutputReceipt> = serde_json::from_value(
            output.metadata.unwrap()["structuredResult"]["declaredOutputs"].clone(),
        )
        .unwrap();
        assert_eq!(receipts.len(), 3);
        assert_eq!(
            receipts[0].sealed_hash.as_deref(),
            Some(crate::atomic::sha256_hex(b"new").as_str())
        );
        assert!(receipts[0].previous_hash.is_none());
        assert_eq!(
            receipts[0].required_validator_id.as_deref(),
            Some("document-structure")
        );
        assert_eq!(
            receipts[1].previous_hash.as_deref(),
            Some(crate::atomic::sha256_hex(b"old").as_str())
        );
        assert!(receipts[2].sealed_hash.is_none() && receipts[2].file_size.is_none());
        assert!(root.join("incidental").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn invalid_before_state_refuses_process_and_unchanged_after_state_is_not_sealed() {
        let (root, ctx) = fixture();
        std::fs::write(root.join("already"), b"original").unwrap();
        let output=BashTool.execute(json!({"command":"touch must-not-run","declared_outputs":[{"path":"already","operation":"created"}]}),ctx.clone()).await;
        assert!(output.is_error && output.content.contains("PRECONDITION"));
        assert!(!root.join("must-not-run").exists());
        let output=BashTool.execute(json!({"command":"printf executed; touch did-run","declared_outputs":[{"path":"already","operation":"modified"}]}),ctx).await;
        assert!(output.is_error && output.content.contains("BASH_OUTPUT_UNCHANGED"));
        assert!(root.join("did-run").exists());
        let meta = output.metadata.unwrap();
        assert_eq!(meta["structuredResult"]["retryability"], "NEVER");
        assert!(meta["structuredResult"].get("declaredOutputs").is_none());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn replaced_parent_or_symlink_output_cannot_be_sealed() {
        let (root, ctx) = fixture();
        std::fs::create_dir(root.join("child")).unwrap();
        let output=BashTool.execute(json!({"command":"mv child previous; mkdir child; printf bytes > child/out","declared_outputs":[{"path":"child/out","operation":"created"}]}),ctx.clone()).await;
        assert!(output.is_error && output.content.contains("PARENT_CHANGED"));
        let output=BashTool.execute(json!({"command":"ln -s ../child/out linked","declared_outputs":[{"path":"linked","operation":"created"}]}),ctx).await;
        assert!(output.is_error && output.content.contains("PATH_UNSAFE"));
        std::fs::remove_dir_all(root).unwrap();
    }
}
