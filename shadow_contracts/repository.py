"""Repository identity helpers shared by release-facing commands."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


class RepositoryStateError(RuntimeError):
    """Raised when the running code cannot be bound to an exact commit."""


def _validate_sha1(value: str, *, source: str) -> str:
    candidate = value.strip().lower()
    if len(candidate) != 40 or any(char not in "0123456789abcdef" for char in candidate):
        raise RepositoryStateError(f"{source} did not resolve to a 40-character SHA-1 commit")
    return candidate


def _run_git(command: list[str], *, source: str) -> str:
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RepositoryStateError(f"{source} is unavailable") from exc
    return _validate_sha1(completed.stdout, source=source)


def resolve_code_commit(repository_root: Path) -> str:
    """Resolve code identity in the mandated fail-closed order.

    Order: ``APP_CODE_COMMIT``, an ordinary Git checkout, then the legacy
    split ``.version-history`` directory.  A missing or malformed identity is
    a release error; callers must not substitute ``unknown`` or a workspace
    label.
    """

    configured = os.environ.get("APP_CODE_COMMIT", "").strip()
    if configured:
        return _validate_sha1(configured, source="APP_CODE_COMMIT")

    root = Path(repository_root).resolve()
    try:
        return _run_git(
            ["git", "-C", str(root), "rev-parse", "--verify", "HEAD^{commit}"],
            source="git rev-parse HEAD",
        )
    except RepositoryStateError as ordinary_error:
        legacy = root / ".version-history"
        if legacy.is_dir() and (legacy / "HEAD").is_file():
            try:
                return _run_git(
                    [
                        "git",
                        f"--git-dir={legacy}",
                        f"--work-tree={root}",
                        "rev-parse",
                        "--verify",
                        "HEAD^{commit}",
                    ],
                    source=".version-history/HEAD",
                )
            except RepositoryStateError as legacy_error:
                raise RepositoryStateError(
                    "cannot resolve code commit from APP_CODE_COMMIT, ordinary Git, "
                    "or .version-history/HEAD"
                ) from legacy_error
        raise RepositoryStateError(
            "cannot resolve code commit from APP_CODE_COMMIT, ordinary Git, "
            "or .version-history/HEAD"
        ) from ordinary_error
