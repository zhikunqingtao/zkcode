use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
pub(super) struct ServerCommand {
    pub program: String,
    pub args: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    schema_version: u32,
    bundle: String,
    identities: BTreeMap<String, String>,
    servers: BTreeMap<String, ServerCommand>,
    versions: Value,
    rust_toolchain_root: PathBuf,
    rust_compiler_sha256: String,
    cargo_sha256: String,
}
pub(super) struct Installation {
    pub bundle: PathBuf,
    pub rust_toolchain_root: PathBuf,
    pub commands: BTreeMap<String, ServerCommand>,
    pub versions: Value,
}
impl Installation {
    pub async fn load(path: &Path) -> Result<Self, String> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut bytes = Vec::new();
            std::fs::File::open(&path)
                .map_err(|_| "LSP_TOOLCHAINS_MISSING: run ./dev bootstrap")?
                .take(256 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| "LSP_MANIFEST_READ_FAILED")?;
            if bytes.len() > 256 * 1024 {
                return Err("LSP_MANIFEST_TOO_LARGE".into());
            }
            let manifest: Manifest =
                serde_json::from_slice(&bytes).map_err(|_| "LSP_MANIFEST_INVALID")?;
            let home = path
                .parent()
                .ok_or("LSP_MANIFEST_PATH_INVALID")?
                .canonicalize()
                .map_err(|_| "LSP_MANIFEST_PATH_INVALID")?;
            if manifest.schema_version != 1
                || manifest.bundle.contains('/')
                || manifest.bundle.starts_with('.')
            {
                return Err("LSP_MANIFEST_INVALID".into());
            }
            let bundle = home
                .join(&manifest.bundle)
                .canonicalize()
                .map_err(|_| "LSP_BUNDLE_MISSING")?;
            if !bundle.starts_with(home) {
                return Err("LSP_BUNDLE_PATH_INVALID".into());
            }
            for (relative, expected) in &manifest.identities {
                let file = bundle
                    .join(relative)
                    .canonicalize()
                    .map_err(|_| "LSP_IDENTITY_FILE_MISSING")?;
                if !file.starts_with(&bundle) {
                    return Err("LSP_IDENTITY_PATH_INVALID".into());
                }
                let mut stream =
                    std::fs::File::open(file).map_err(|_| "LSP_IDENTITY_READ_FAILED")?;
                let mut hash = Sha256::new();
                let mut block = vec![0_u8; 65536].into_boxed_slice();
                loop {
                    let count = stream
                        .read(&mut block)
                        .map_err(|_| "LSP_IDENTITY_READ_FAILED")?;
                    if count == 0 {
                        break;
                    }
                    hash.update(&block[..count]);
                }
                if format!("{:x}", hash.finalize()) != *expected {
                    return Err("LSP_TOOLCHAIN_IDENTITY_MISMATCH: run ./dev bootstrap".into());
                }
            }
            for command in manifest.servers.values() {
                if !manifest.identities.contains_key(&command.program) {
                    return Err("LSP_EXECUTABLE_IDENTITY_REQUIRED".into());
                }
            }
            let rust_toolchain_root = manifest
                .rust_toolchain_root
                .canonicalize()
                .map_err(|_| "LSP_RUST_TOOLCHAIN_MISSING")?;
            for (name, expected) in [
                ("rustc", manifest.rust_compiler_sha256),
                ("cargo", manifest.cargo_sha256),
            ] {
                let mut file = std::fs::File::open(rust_toolchain_root.join("bin").join(name))
                    .map_err(|_| "LSP_RUST_TOOLCHAIN_MISSING")?;
                let mut hash = Sha256::new();
                let mut block = vec![0_u8; 65536].into_boxed_slice();
                loop {
                    let count = file
                        .read(&mut block)
                        .map_err(|_| "LSP_RUST_IDENTITY_READ_FAILED")?;
                    if count == 0 {
                        break;
                    }
                    hash.update(&block[..count]);
                }
                if format!("{:x}", hash.finalize()) != expected {
                    return Err("LSP_RUST_TOOLCHAIN_IDENTITY_MISMATCH".into());
                }
            }
            Ok(Self {
                bundle,
                rust_toolchain_root,
                commands: manifest.servers,
                versions: manifest.versions,
            })
        })
        .await
        .map_err(|_| "LSP_MANIFEST_READ_FAILED")?
    }
}
