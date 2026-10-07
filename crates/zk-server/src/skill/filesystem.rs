//! Filesystem Skills keep lexical identities for reloads and a separate, pinned
//! physical source authority. All reads walk owned directory descriptors.

use std::fs::File;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::SystemTime;

use nix::dir::{Dir, Type};
use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;

const SOURCE_UNAUTHORIZED: &str = "SKILL_SOURCE_UNAUTHORIZED";

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
}

/// Explicit configured source root; first successful lookup pins its identity.
/// Project/plugin sources share the workspace root, user/managed sources use
/// their individually configured Skill roots (including intentional aliases).
#[derive(Debug, Clone)]
pub(super) struct SourceRoot {
    lexical: PathBuf,
    identity: Arc<RwLock<Option<RootIdentity>>>,
    persisted_workspace: bool,
}

impl PartialEq for SourceRoot {
    fn eq(&self, other: &Self) -> bool {
        self.lexical == other.lexical
    }
}

impl Eq for SourceRoot {}

impl SourceRoot {
    pub(super) fn new(path: &Path) -> Self {
        let root = Self {
            lexical: std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf()),
            identity: Arc::default(),
            persisted_workspace: false,
        };
        // Freeze existing sources now, rather than silently adopting a replaced
        // root the first time a cached view is used.
        let _ = root.open();
        root
    }

    /// A persisted workspace was canonicalized when selected. It must not adopt
    /// a new symlink target even before its first Skill lookup in this process.
    pub(super) fn persisted(path: &Path) -> Self {
        let root = Self {
            lexical: path.to_path_buf(),
            identity: Arc::default(),
            persisted_workspace: true,
        };
        let _ = root.open();
        root
    }

    fn open(&self) -> io::Result<(RootIdentity, File)> {
        let (physical, file) = if self.persisted_workspace {
            // A saved workspace is already a physical path. Open that exact
            // directory before the API-facing binding helper coalesces OS errors:
            // a file/ancestor replacement or symlink loop revokes authority,
            // while temporary absence/read failure retains the verified snapshot.
            let file =
                zk_tools::safe_file::open_bound_directory(&self.lexical).map_err(|error| {
                    if is_replacement(&error) {
                        unauthorized()
                    } else {
                        io::Error::other("Skill workspace is unavailable")
                    }
                })?;
            let physical =
                crate::workspace::require_current_binding_identity(&self.lexical.to_string_lossy())
                    .map_err(|error| {
                        if error.code == "WORKSPACE_REBOUND" {
                            unauthorized()
                        } else {
                            io::Error::other("Skill workspace is unavailable")
                        }
                    })?;
            (physical, file)
        } else {
            let physical = self.lexical.canonicalize()?;
            let file = zk_tools::safe_file::open_bound_directory(&physical)?;
            (physical, file)
        };
        let metadata = file.metadata()?;
        let identity = RootIdentity {
            path: physical,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let mut pinned = self
            .identity
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(previous) = pinned.as_ref() {
            if previous != &identity {
                return Err(unauthorized());
            }
        } else {
            *pinned = Some(identity.clone());
        }
        Ok((identity, file))
    }

    pub(super) fn source(&self, lexical: &Path) -> io::Result<BoundSource> {
        let (identity, root) = self.open()?;
        let physical = lexical.canonicalize()?;
        let relative = physical
            .strip_prefix(&identity.path)
            .map_err(|_| unauthorized())?;
        let directory = Arc::new(open_relative(&root, relative, true)?);
        let result = BoundSource {
            authority: self.clone(),
            lexical: std::path::absolute(lexical)?,
            physical,
            directory,
        };
        result.revalidate()?;
        Ok(result)
    }
}

fn unauthorized() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, SOURCE_UNAUTHORIZED)
}

pub(super) fn diagnostic(error: &io::Error) -> &'static str {
    if error.to_string() == SOURCE_UNAUTHORIZED {
        SOURCE_UNAUTHORIZED
    } else {
        "SKILL_SCAN_FAILED"
    }
}

/// Immutable provenance attached to a validated content snapshot. Rechecking
/// authorization does not reload bytes or discard a good snapshot on I/O errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadAuthority {
    root: SourceRoot,
    source: PathBuf,
    file: PathBuf,
}

impl ReadAuthority {
    pub(super) fn permits_snapshot(&self) -> bool {
        match self.root.source(&self.source) {
            Ok(source) => {
                let Ok(relative) = self.file.strip_prefix(&self.source) else {
                    return false;
                };
                // Child symlinks have never been a supported Skill source. A
                // cached body must not survive a change to that authority.
                match open_relative(&source.directory, relative, false) {
                    Ok(_) => true,
                    Err(error) => !is_replacement(&error),
                }
            }
            Err(error) => !is_replacement(&error),
        }
    }
}

fn is_replacement(error: &io::Error) -> bool {
    error.to_string() == SOURCE_UNAUTHORIZED
        || matches!(
            error.raw_os_error(),
            Some(nix::libc::ELOOP | nix::libc::ENOTDIR)
        )
}

#[derive(Debug)]
pub(super) struct BoundSource {
    authority: SourceRoot,
    lexical: PathBuf,
    physical: PathBuf,
    directory: Arc<File>,
}

impl BoundSource {
    fn revalidate(&self) -> io::Result<()> {
        let (root, root_file) = self.authority.open()?;
        if self.lexical.canonicalize()? != self.physical {
            return Err(unauthorized());
        }
        let relative = self
            .physical
            .strip_prefix(&root.path)
            .map_err(|_| unauthorized())?;
        let current = open_relative(&root_file, relative, true)?.metadata()?;
        let pinned = self.directory.metadata()?;
        if current.dev() != pinned.dev() || current.ino() != pinned.ino() {
            return Err(unauthorized());
        }
        Ok(())
    }

    pub(super) fn scan(&self, max_depth: usize) -> io::Result<Vec<BoundSkillFile>> {
        let mut files = Vec::new();
        self.walk(&self.directory, Path::new(""), 0, max_depth, &mut files)?;
        self.revalidate()?;
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    }

    fn walk(
        &self,
        directory: &File,
        relative: &Path,
        depth: usize,
        max_depth: usize,
        files: &mut Vec<BoundSkillFile>,
    ) -> io::Result<()> {
        if depth > max_depth {
            return Ok(());
        }
        let mut entries = Dir::from_fd(directory.try_clone()?.into())?;
        for entry in entries.iter() {
            let entry = entry?;
            let name = std::ffi::OsStr::from_bytes(entry.file_name().to_bytes());
            if name.as_bytes().starts_with(b".") || entry.file_type() == Some(Type::Symlink) {
                continue;
            }
            // DT_UNKNOWN is valid on some filesystems. Inspect the fd, never
            // following a link or opening a FIFO in blocking mode.
            let file = match open_child(directory, name, false) {
                Ok(file) => file,
                Err(error) if error.raw_os_error() == Some(nix::libc::ELOOP) => continue,
                Err(error) => return Err(error),
            };
            let metadata = file.metadata()?;
            let child = relative.join(name);
            if metadata.is_dir() {
                self.walk(&file, &child, depth + 1, max_depth, files)?;
            } else if metadata.is_file() && child.extension().is_some_and(|ext| ext == "md") {
                files.push(BoundSkillFile {
                    path: self.lexical.join(&child),
                    relative: child,
                    directory: self.directory.clone(),
                    stamp: FileStamp::from_metadata(&metadata),
                    authority: ReadAuthority {
                        root: self.authority.clone(),
                        source: self.lexical.clone(),
                        file: self.lexical.join(relative).join(name),
                    },
                });
            }
        }
        Ok(())
    }

    pub(super) fn child_directories(&self) -> io::Result<Vec<PathBuf>> {
        let mut result = Vec::new();
        let mut entries = Dir::from_fd(self.directory.try_clone()?.into())?;
        for entry in entries.iter() {
            let entry = entry?;
            let name = std::ffi::OsStr::from_bytes(entry.file_name().to_bytes());
            if name.as_bytes().starts_with(b".") || entry.file_type() == Some(Type::Symlink) {
                continue;
            }
            match open_child(&self.directory, name, true) {
                Ok(_) => result.push(self.lexical.join(name)),
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(nix::libc::ELOOP | nix::libc::ENOTDIR)
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        self.revalidate()?;
        result.sort();
        Ok(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FileStamp {
    modified: Option<SystemTime>,
    changed: (i64, i64),
    len: u64,
    device: u64,
    inode: u64,
}

impl FileStamp {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            modified: metadata.modified().ok(),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
            len: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[derive(Debug)]
pub(super) struct BoundSkillFile {
    pub(super) path: PathBuf,
    pub(super) stamp: FileStamp,
    pub(super) authority: ReadAuthority,
    relative: PathBuf,
    directory: Arc<File>,
}

impl BoundSkillFile {
    fn revalidate_source(&self) -> io::Result<()> {
        let source = self.authority.root.source(&self.authority.source)?;
        let current = source.directory.metadata()?;
        let scanned = self.directory.metadata()?;
        if current.dev() != scanned.dev() || current.ino() != scanned.ino() {
            return Err(unauthorized());
        }
        Ok(())
    }

    pub(super) fn read(&self) -> io::Result<String> {
        // Revalidation pins the original source; a concurrent lexical alias
        // change cannot redirect this read because it uses the stored fd.
        self.revalidate_source()?;
        let mut file = open_relative(&self.directory, &self.relative, false)?;
        if FileStamp::from_metadata(&file.metadata()?) != self.stamp {
            return Err(io::Error::other("Skill changed during scan"));
        }
        let mut raw = String::new();
        file.read_to_string(&mut raw)?;
        if FileStamp::from_metadata(&file.metadata()?) != self.stamp {
            return Err(io::Error::other("Skill changed during read"));
        }
        self.revalidate_source()?;
        Ok(raw)
    }
}

fn open_child(directory: &File, name: &std::ffi::OsStr, is_dir: bool) -> io::Result<File> {
    let mut flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK;
    if is_dir {
        flags |= OFlag::O_DIRECTORY;
    }
    Ok(File::from(openat(directory, name, flags, Mode::empty())?))
}

fn open_relative(root: &File, relative: &Path, is_dir: bool) -> io::Result<File> {
    let mut current = root.try_clone()?;
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(unauthorized());
        };
        current = open_child(&current, name, components.peek().is_some() || is_dir)?;
    }
    let metadata = current.metadata()?;
    if (is_dir && !metadata.is_dir()) || (!is_dir && !metadata.is_file()) {
        return Err(io::Error::other("invalid Skill file type"));
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    struct Workspace(PathBuf);
    impl Workspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("zk-skill-fd-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn write(&self, name: &str, text: &str) {
            let path = self.0.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
    }
    impl Drop for Workspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn source_replacement_after_scan_never_reads_the_new_target() {
        let inside = Workspace::new();
        let outside = Workspace::new();
        inside.write("skills/one.md", "validated content");
        outside.write("skills/one.md", "not authorized");
        let root = SourceRoot::new(&inside.0);
        let source = root.source(&inside.0.join("skills")).unwrap();
        let files = source.scan(8).unwrap();
        std::fs::rename(inside.0.join("skills"), inside.0.join("previous")).unwrap();
        symlink(outside.0.join("skills"), inside.0.join("skills")).unwrap();
        assert_eq!(
            diagnostic(&files[0].read().unwrap_err()),
            SOURCE_UNAUTHORIZED
        );
    }

    #[test]
    fn ancestor_replacement_after_scan_cannot_redirect_fd_walk() {
        let inside = Workspace::new();
        let outside = Workspace::new();
        inside.write("skills/nested/one.md", "validated content");
        outside.write("one.md", "not authorized");
        let root = SourceRoot::new(&inside.0);
        let source = root.source(&inside.0.join("skills")).unwrap();
        let files = source.scan(8).unwrap();
        std::fs::rename(inside.0.join("skills/nested"), inside.0.join("previous")).unwrap();
        symlink(&outside.0, inside.0.join("skills/nested")).unwrap();
        assert!(files[0].read().is_err());
    }

    #[test]
    fn valid_root_alias_is_supported_but_child_symlinks_stay_ignored() {
        let workspace = Workspace::new();
        workspace.write("real/one.md", "one");
        workspace.write("other/two.md", "two");
        symlink(workspace.0.join("real"), workspace.0.join("alias")).unwrap();
        symlink(
            workspace.0.join("other/two.md"),
            workspace.0.join("real/link.md"),
        )
        .unwrap();
        symlink(workspace.0.join("other"), workspace.0.join("real/link-dir")).unwrap();
        let root = SourceRoot::new(&workspace.0);
        let files = root
            .source(&workspace.0.join("alias"))
            .unwrap()
            .scan(8)
            .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].read().unwrap(), "one");
        assert_eq!(files[0].path, workspace.0.join("alias/one.md"));
    }

    #[test]
    fn replaced_workspace_is_not_implicitly_reauthorized() {
        let workspace = Workspace::new();
        workspace.write("skills/one.md", "first");
        let root = SourceRoot::new(&workspace.0);
        let previous = workspace.0.with_extension("previous");
        std::fs::rename(&workspace.0, &previous).unwrap();
        workspace.write("skills/one.md", "second");
        assert_eq!(
            diagnostic(&root.source(&workspace.0.join("skills")).unwrap_err()),
            SOURCE_UNAUTHORIZED
        );
        std::fs::remove_dir_all(previous).unwrap();
    }

    #[test]
    fn configured_external_source_is_pinned_and_missing_roots_can_appear_later() {
        let home = Workspace::new();
        let external = Workspace::new();
        external.write("skills/one.md", "external configured source");
        symlink(external.0.join("skills"), home.0.join("user-skills")).unwrap();
        let root = SourceRoot::new(&home.0.join("user-skills"));
        assert_eq!(
            root.source(&home.0.join("user-skills"))
                .unwrap()
                .scan(8)
                .unwrap()[0]
                .read()
                .unwrap(),
            "external configured source"
        );
        let late = SourceRoot::new(&home.0.join("late"));
        home.write("late/new.md", "new source");
        assert_eq!(
            late.source(&home.0.join("late"))
                .unwrap()
                .scan(8)
                .unwrap()
                .len(),
            1
        );
    }
}
