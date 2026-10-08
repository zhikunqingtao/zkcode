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
    rust_source_root: PathBuf,
    rust_source_sha256: String,
}
pub(super) struct Installation {
    pub bundle: PathBuf,
    pub rust_toolchain_root: PathBuf,
    pub rust_source_root: PathBuf,
    pub rust_compiler_version: String,
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
            if manifest.schema_version != 2
                || manifest.bundle.contains('/')
                || manifest.bundle.starts_with('.')
            {
                return Err("LSP_MANIFEST_INVALID".into());
            }
            let rust_compiler_version = manifest
                .versions
                .get("rustCompiler")
                .and_then(Value::as_str)
                .filter(|version| !version.trim().is_empty())
                .ok_or("LSP_RUST_VERSION_REQUIRED")?
                .to_owned();
            if manifest.versions.get("rust-src").and_then(Value::as_str)
                != Some(rust_compiler_version.as_str())
            {
                return Err("LSP_RUST_SOURCE_VERSION_MISMATCH".into());
            }
            let bundle = home
                .join(&manifest.bundle)
                .canonicalize()
                .map_err(|_| "LSP_BUNDLE_MISSING")?;
            if !bundle.starts_with(home) {
                return Err("LSP_BUNDLE_PATH_INVALID".into());
            }
            let rust_source_root = bundle
                .join(&manifest.rust_source_root)
                .canonicalize()
                .map_err(|_| "LSP_RUST_SOURCE_MISSING")?;
            if !rust_source_root.starts_with(&bundle) {
                return Err("LSP_RUST_SOURCE_PATH_INVALID".into());
            }
            if source_tree_digest(&rust_source_root)? != manifest.rust_source_sha256 {
                return Err("LSP_RUST_SOURCE_IDENTITY_MISMATCH".into());
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
            let rust_toolchain_root = validate_rust_toolchain(&manifest)?;
            Ok(Self {
                bundle,
                rust_toolchain_root,
                rust_source_root,
                rust_compiler_version,
                commands: manifest.servers,
                versions: manifest.versions,
            })
        })
        .await
        .map_err(|_| "LSP_MANIFEST_READ_FAILED")?
    }
}

fn validate_rust_toolchain(manifest: &Manifest) -> Result<PathBuf, String> {
    let rust_toolchain_root = manifest
        .rust_toolchain_root
        .canonicalize()
        .map_err(|_| "LSP_RUST_TOOLCHAIN_MISSING")?;
    for (name, expected) in [
        ("rustc", &manifest.rust_compiler_sha256),
        ("cargo", &manifest.cargo_sha256),
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
        if format!("{:x}", hash.finalize()) != *expected {
            return Err("LSP_RUST_TOOLCHAIN_IDENTITY_MISMATCH".into());
        }
    }
    Ok(rust_toolchain_root)
}

/// Same path + NUL + content-digest + newline framing as the private installer.
fn source_tree_digest(root: &Path) -> Result<String, &'static str> {
    let mut files = BTreeMap::new();
    let mut directories = vec![root.to_owned()];
    let mut entries = 0;
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).map_err(|_| "LSP_RUST_SOURCE_READ_FAILED")? {
            entries += 1;
            if entries > 100_000 {
                return Err("LSP_RUST_SOURCE_TOO_LARGE");
            }
            let entry = entry.map_err(|_| "LSP_RUST_SOURCE_READ_FAILED")?;
            let kind = entry
                .file_type()
                .map_err(|_| "LSP_RUST_SOURCE_READ_FAILED")?;
            let path = entry.path();
            if kind.is_dir() {
                directories.push(path);
            } else if kind.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|_| "LSP_RUST_SOURCE_PATH_INVALID")?
                    .to_str()
                    .ok_or("LSP_RUST_SOURCE_PATH_INVALID")?
                    .to_owned();
                files.insert(relative, path);
            } else {
                return Err("LSP_RUST_SOURCE_LINK_OR_TYPE_INVALID");
            }
        }
    }
    if files.is_empty() {
        return Err("LSP_RUST_SOURCE_EMPTY");
    }
    let mut tree = Sha256::new();
    let mut block = vec![0_u8; 65536].into_boxed_slice();
    for (relative, path) in files {
        let mut file = std::fs::File::open(path).map_err(|_| "LSP_RUST_SOURCE_READ_FAILED")?;
        let mut content = Sha256::new();
        loop {
            let count = file
                .read(&mut block)
                .map_err(|_| "LSP_RUST_SOURCE_READ_FAILED")?;
            if count == 0 {
                break;
            }
            content.update(&block[..count]);
        }
        tree.update(relative.as_bytes());
        tree.update(b"\0");
        tree.update(format!("{:x}\n", content.finalize()).as_bytes());
    }
    Ok(format!("{:x}", tree.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn private_fixture() -> (Fixture, Value) {
        let root = std::env::temp_dir().join(format!("zk-private-lsp-{}", uuid::Uuid::new_v4()));
        for directory in ["bundle/source/core", "bundle/source/std", "compiler/bin"] {
            std::fs::create_dir_all(root.join(directory)).unwrap();
        }
        for (path, bytes) in [
            ("bundle/source/core/lib.rs", b"core".as_slice()),
            ("bundle/source/std/lib.rs", b"std"),
            ("compiler/bin/rustc", b"rustc"),
            ("compiler/bin/cargo", b"cargo"),
        ] {
            std::fs::write(root.join(path), bytes).unwrap();
        }
        let record = serde_json::json!({"schemaVersion":2,"bundle":"bundle","identities":{},"servers":{},
            "versions":{"rustCompiler":"1.97.1","rust-src":"1.97.1"},"rustToolchainRoot":root.join("compiler"),
            "rustCompilerSha256":format!("{:x}",Sha256::digest(b"rustc")),"cargoSha256":format!("{:x}",Sha256::digest(b"cargo")),
            "rustSourceRoot":"source","rustSourceSha256":"497c27d4b48df7fe32f8276b04c85a12f9b37e0a9e3cb498945f0def2df1bba0"});
        (Fixture(root), record)
    }
    #[tokio::test]
    async fn private_source_identity_matches_installer_and_binds_all_bytes() {
        let (fixture, record) = private_fixture();
        let path = fixture.0.join("current.json");
        std::fs::write(&path, record.to_string()).unwrap();
        let installed = Installation::load(&path)
            .await
            .expect("valid private manifest");
        assert_eq!(installed.rust_compiler_version, "1.97.1");
        assert!(installed.rust_source_root.starts_with(&installed.bundle));
        std::fs::write(fixture.0.join("bundle/source/std/lib.rs"), b"mutated").unwrap();
        assert_eq!(
            Installation::load(&path).await.err().as_deref(),
            Some("LSP_RUST_SOURCE_IDENTITY_MISMATCH")
        );
    }
    #[tokio::test]
    async fn private_manifest_requires_nonempty_matching_rust_versions() {
        let (fixture, mut record) = private_fixture();
        let path = fixture.0.join("current.json");
        for version in [
            Value::Null,
            Value::String(String::new()),
            Value::String("  ".into()),
            Value::String("1.0.0".into()),
        ] {
            record["versions"]["rustCompiler"] = version;
            std::fs::write(&path, record.to_string()).unwrap();
            assert!(Installation::load(&path).await.is_err());
        }
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn private_source_rejects_links_and_escaping_roots() {
        let (fixture, mut record) = private_fixture();
        let path = fixture.0.join("current.json");
        std::fs::remove_file(fixture.0.join("bundle/source/std/lib.rs")).unwrap();
        std::os::unix::fs::symlink(
            fixture.0.join("compiler/bin/rustc"),
            fixture.0.join("bundle/source/std/lib.rs"),
        )
        .unwrap();
        std::fs::write(&path, record.to_string()).unwrap();
        assert_eq!(
            Installation::load(&path).await.err().as_deref(),
            Some("LSP_RUST_SOURCE_LINK_OR_TYPE_INVALID")
        );
        record["rustSourceRoot"] = Value::String("../compiler".into());
        std::fs::write(&path, record.to_string()).unwrap();
        assert_eq!(
            Installation::load(&path).await.err().as_deref(),
            Some("LSP_RUST_SOURCE_PATH_INVALID")
        );
    }
    #[test]
    fn source_identity_sorts_whole_relative_names_in_posix_byte_order() {
        let (fixture, _) = private_fixture();
        let source = fixture.0.join("prefix-sources");
        std::fs::create_dir_all(source.join("a")).unwrap();
        std::fs::write(source.join("a/lib.rs"), b"nested").unwrap();
        std::fs::write(source.join("a.rs"), b"flat").unwrap();
        assert_eq!(
            source_tree_digest(&source).unwrap(),
            "88f8acf7a2d56929bdb269b7fe9c4b3033d62837300ce705b0fa98cb992a65ae"
        );
    }

    #[tokio::test]
    async fn legacy_manifest_cannot_select_a_global_rust_source_tree() {
        let root = std::env::temp_dir().join(format!("zk-lsp-manifest-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("bundle")).unwrap();
        std::fs::create_dir_all(root.join("global/bin")).unwrap();
        std::fs::write(root.join("global/bin/rustc"), b"rustc").unwrap();
        std::fs::write(root.join("global/bin/cargo"), b"cargo").unwrap();
        let record = serde_json::json!({"schemaVersion":1,"bundle":"bundle","identities":{},"servers":{},
            "versions":{"rustCompiler":"1.97.1"},"rustToolchainRoot":root.join("global"),
            "rustCompilerSha256":format!("{:x}",Sha256::digest(b"rustc")),"cargoSha256":format!("{:x}",Sha256::digest(b"cargo"))});
        let path = root.join("current.json");
        std::fs::write(&path, record.to_string()).unwrap();
        let rejected = Installation::load(&path).await.is_err();
        std::fs::remove_dir_all(root).unwrap();
        assert!(rejected, "legacy global-source manifest was still accepted");
    }
}
