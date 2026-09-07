"""Canonical workspace path validation shared by public Python routes."""

from __future__ import annotations

import os
from pathlib import Path


class WorkspacePathError(ValueError):
    """Raised when a requested path cannot be safely resolved in the workspace."""


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def workspace_root() -> Path:
    """Return the default relative-path root as a canonical directory."""
    raw_root = os.getenv("WORKSPACE_ROOT", os.getcwd())
    try:
        root = Path(raw_root).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise WorkspacePathError("Configured workspace root is unavailable") from error
    if not root.is_dir():
        raise WorkspacePathError("Configured workspace root is not a directory")
    return root


def _allowed_roots() -> tuple[Path, ...] | None:
    """Return explicitly configured roots, or no global path restriction.

    ``WORKSPACE_ROOT`` remains the anchor for relative requests. The sidecar is
    part of a local, single-user application, so absolute requests are
    unrestricted by default. Deployments that need containment can opt in with
    ``ZK_WORKSPACE_ALLOWED_ROOTS``.
    """
    configured = os.getenv("ZK_WORKSPACE_ALLOWED_ROOTS", "")
    values = [value.strip() for value in configured.split(",")
              if value.strip()]
    if values:
        roots: list[Path] = []
        for value in values:
            try:
                root = Path(value).resolve(strict=True)
            except (OSError, RuntimeError) as error:
                raise WorkspacePathError(
                    "Configured workspace allowed root is unavailable") from error
            if not root.is_dir():
                raise WorkspacePathError(
                    "Configured workspace allowed root is not a directory")
            if root not in roots:
                roots.append(root)
        return tuple(roots)
    return None


def _is_allowed(path: Path, roots: tuple[Path, ...] | None) -> bool:
    return roots is None or any(_is_within(path, root) for root in roots)


def resolve_workspace_path(
    raw_path: str,
    *,
    base: Path | None = None,
    require_directory: bool = False,
    require_file: bool = False,
) -> Path:
    """Resolve an absolute or relative path without allowing symlink escape.

    Relative paths are anchored at ``base`` when supplied, otherwise at
    ``WORKSPACE_ROOT``. Explicit allowed roots are always enforced; without
    them, absolute paths may address any local file or directory. A supplied
    ``base`` still contains relative requests to that project directory.
    """
    if not raw_path or "\x00" in raw_path:
        raise WorkspacePathError("Path is empty or invalid")
    if require_directory and require_file:
        raise WorkspacePathError("Path cannot be both a directory and a file")

    root = workspace_root()
    allowed_roots = _allowed_roots()
    try:
        anchor = root if base is None else Path(base).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise WorkspacePathError("Base path is unavailable") from error
    if base is not None:
        if not anchor.is_dir():
            raise WorkspacePathError("Base path is not a directory")
        if not _is_allowed(anchor, allowed_roots):
            raise WorkspacePathError("Base path is outside the workspace")

    # User-provided `~` is a workspace-relative name, not permission to inspect
    # the service account's home directory. Only the trusted root setting above
    # supports expanduser().
    requested = Path(raw_path)
    if not requested.is_absolute():
        requested = anchor / requested
    try:
        resolved = requested.resolve(strict=True)
        if base is not None:
            resolved.relative_to(anchor)
    except (ValueError, OSError, RuntimeError) as error:
        raise WorkspacePathError("Path is outside the workspace or does not exist") from error

    if base is None and not _is_allowed(resolved, allowed_roots):
        raise WorkspacePathError("Path is outside the configured workspace")

    if require_directory and not resolved.is_dir():
        raise WorkspacePathError("Path is not a directory")
    if require_file and not resolved.is_file():
        raise WorkspacePathError("Path is not a file")
    return resolved
