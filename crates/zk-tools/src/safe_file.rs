//! Descriptor-anchored reads; callers must separately authorize the requested path.

use std::path::Path;

/// Open each directory from an owned root descriptor. Frozen authorization has
/// already resolved aliases; a subsequent symlink replacement must fail closed.
/// # Errors
/// Rejects non-absolute paths, symlinks, non-regular files and failed OS reads.
#[cfg(unix)]
pub fn open_bound_regular(path: &Path) -> std::io::Result<std::fs::File> {
    let file = open_bound(path, false)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("bound path is not a regular file"));
    }
    Ok(file)
}

/// Open a frozen physical directory without following any replaced ancestor.
/// The caller is responsible for the authorization of this root. Descendant
/// operations can then remain relative to the returned owned descriptor.
/// # Errors
/// Rejects relative/non-normalized paths, symlinks and non-directories.
#[cfg(unix)]
pub fn open_bound_directory(path: &Path) -> std::io::Result<std::fs::File> {
    open_bound(path, true)
}

#[cfg(unix)]
fn open_bound(path: &Path, is_directory: bool) -> std::io::Result<std::fs::File> {
    use nix::fcntl::{OFlag, open, openat};
    use nix::sys::stat::Mode;
    use std::path::Component;
    let flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW;
    let mut directory = open("/", flags | OFlag::O_DIRECTORY, Mode::empty())?;
    let mut parts = path.components().peekable();
    if parts.next() != Some(Component::RootDir) {
        return Err(std::io::Error::other("path must be bound and absolute"));
    }
    while let Some(component) = parts.next() {
        let Component::Normal(name) = component else {
            return Err(std::io::Error::other("path must be normalized"));
        };
        let options = if parts.peek().is_none() && !is_directory {
            flags | OFlag::O_NONBLOCK
        } else {
            flags | OFlag::O_DIRECTORY
        };
        directory = openat(&directory, name, options, Mode::empty())?;
    }
    let file = std::fs::File::from(directory);
    Ok(file)
}

#[cfg(not(unix))]
pub fn open_bound_regular(_path: &Path) -> std::io::Result<std::fs::File> {
    Err(std::io::Error::other(
        "descriptor-anchored reads require a Unix host",
    ))
}

#[cfg(not(unix))]
pub fn open_bound_directory(_path: &Path) -> std::io::Result<std::fs::File> {
    Err(std::io::Error::other(
        "descriptor-anchored reads require a Unix host",
    ))
}
