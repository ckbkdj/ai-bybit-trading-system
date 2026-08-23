from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = (
    ROOT / "ai_bot3" / "ai_bot3" / "requirements.txt",
    ROOT / "ai_bot3" / "ai_bot3" / "requirements-dev.txt",
    ROOT / "BybitContractBotV4" / "requirements.txt",
)
LOCKFILES = (ROOT / "ai_bot3" / "ai_bot3" / "package-lock.json",)
SECRET_PATTERNS = {
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "aws_access_key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "hardcoded_secret": re.compile(
        r"(?i)(?:api[_-]?key|api[_-]?secret|secret[_-]?key|access[_-]?token|"
        r"bearer[_-]?token|webhook(?:[_-]?url)?)"
        r"\s*[:=]\s*['\"][A-Za-z0-9_./+=:-]{16,}['\"]"
    ),
}
MAX_HISTORY_BLOB_BYTES = 5 * 1024 * 1024
MAX_HISTORY_BLOBS = 25_000


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def dependency_pinning() -> dict[str, Any]:
    unpinned = []
    files = []
    for path in REQUIREMENTS:
        lines = path.read_text(encoding="utf-8").splitlines()
        for number, raw in enumerate(lines, 1):
            line = raw.split("#", 1)[0].strip()
            if not line or line.startswith("-r "):
                continue
            if "==" not in line:
                unpinned.append({"path": str(path.relative_to(ROOT)), "line": number})
        files.append({"path": str(path.relative_to(ROOT)), "sha256": sha256(path)})
    return {"files": files, "unpinned_entries": unpinned}


def node_lock_integrity() -> dict[str, Any]:
    results = []
    missing_integrity = []
    for path in LOCKFILES:
        payload = json.loads(path.read_text(encoding="utf-8"))
        packages = payload.get("packages") or {}
        for name, metadata in packages.items():
            if name and isinstance(metadata, dict) and metadata.get("resolved") and not metadata.get("integrity"):
                missing_integrity.append(name)
        results.append(
            {
                "path": str(path.relative_to(ROOT)),
                "sha256": sha256(path),
                "lockfile_version": payload.get("lockfileVersion"),
            }
        )
    return {"files": results, "missing_integrity_entries": missing_integrity}


def _git_command(*args: str) -> list[str]:
    split_git_dir = ROOT / ".version-history"
    if split_git_dir.exists():
        return [
            "git",
            f"--git-dir={split_git_dir}",
            f"--work-tree={ROOT}",
            *args,
        ]
    return ["git", "-C", str(ROOT), *args]


def _git_text(*args: str) -> str:
    completed = subprocess.run(
        _git_command(*args),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout


def tracked_files() -> list[Path]:
    """Return tracked paths in both the local split-Git layout and normal clones."""

    return [
        ROOT / line
        for line in _git_text("ls-files").splitlines()
        if line.strip()
    ]


def _scan_text(text: str, *, path: str, blob_sha: str | None = None) -> list[dict[str, Any]]:
    findings = []
    for kind, pattern in SECRET_PATTERNS.items():
        for match in pattern.finditer(text):
            item: dict[str, Any] = {
                "path": path,
                "line": text.count("\n", 0, match.start()) + 1,
                "kind": kind,
            }
            if blob_sha:
                item["blob_sha"] = blob_sha
            findings.append(item)
    return findings


def secret_scan() -> dict[str, Any]:
    findings = []
    sensitive_paths = []
    for path in tracked_files():
        relative = str(path.relative_to(ROOT)).replace("\\", "/")
        lowered = relative.lower()
        if ".env" in Path(relative).name.lower() and not lowered.endswith(".env.example"):
            sensitive_paths.append(relative)
        if not path.is_file() or path.suffix.lower() in {".png", ".jpg", ".pdf", ".sqlite3"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        findings.extend(_scan_text(text, path=relative))
    return {
        "finding_count": len(findings),
        "findings_without_values": findings,
        "tracked_sensitive_paths": sensitive_paths,
    }


def _history_blob_paths() -> dict[str, str]:
    """Map every reachable object id to its first recorded path."""

    mapping: dict[str, str] = {}
    for raw in _git_text("rev-list", "--objects", "--all").splitlines():
        object_id, _, path = raw.partition(" ")
        if object_id and object_id not in mapping:
            mapping[object_id] = path or "<unpathed-object>"
    return mapping


def _batch_object_metadata(object_ids: Iterable[str]) -> dict[str, tuple[str, int]]:
    values = list(object_ids)
    if not values:
        return {}
    completed = subprocess.run(
        _git_command("cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"),
        input="\n".join(values) + "\n",
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    result: dict[str, tuple[str, int]] = {}
    for line in completed.stdout.splitlines():
        object_id, object_type, raw_size = line.split(" ", 2)
        result[object_id] = (object_type, int(raw_size))
    return result


def history_secret_scan() -> dict[str, Any]:
    """Scan all reachable Git blobs, not only the current checkout.

    Values are intentionally omitted from the report.  Full-history scanning is
    necessary because deleting a credential from HEAD does not remove it from a
    previously pushed commit.
    """

    paths = _history_blob_paths()
    metadata = _batch_object_metadata(paths)
    findings = []
    sensitive_paths = []
    scanned = 0
    skipped_large = 0
    skipped_binary = 0
    for object_id, path in paths.items():
        object_type, size = metadata.get(object_id, ("unknown", 0))
        if object_type != "blob":
            continue
        if scanned >= MAX_HISTORY_BLOBS:
            break
        if size > MAX_HISTORY_BLOB_BYTES:
            skipped_large += 1
            continue
        normalized = path.replace("\\", "/")
        if ".env" in Path(normalized).name.lower() and not normalized.lower().endswith(".env.example"):
            sensitive_paths.append({"path": normalized, "blob_sha": object_id})
        completed = subprocess.run(
            _git_command("cat-file", "blob", object_id),
            check=True,
            capture_output=True,
        )
        try:
            text = completed.stdout.decode("utf-8")
        except UnicodeDecodeError:
            skipped_binary += 1
            continue
        scanned += 1
        findings.extend(_scan_text(text, path=normalized, blob_sha=object_id))
    return {
        "finding_count": len(findings),
        "findings_without_values": findings,
        "historical_sensitive_paths": sensitive_paths,
        "scanned_blob_count": scanned,
        "skipped_large_blob_count": skipped_large,
        "skipped_binary_blob_count": skipped_binary,
        "scan_limit_reached": scanned >= MAX_HISTORY_BLOBS,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline supply-chain and secret gate")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    pinning = dependency_pinning()
    node = node_lock_integrity()
    secrets = secret_scan()
    history = history_secret_scan()
    blockers = []
    if pinning["unpinned_entries"]:
        blockers.append("python dependencies are not fully locked")
    if node["missing_integrity_entries"]:
        blockers.append("node lock entries are missing integrity hashes")
    if secrets["finding_count"] or secrets["tracked_sensitive_paths"]:
        blockers.append("tracked repository may contain credentials")
    if history["finding_count"] or history["historical_sensitive_paths"]:
        blockers.append("reachable Git history may contain credentials")
    if history["scan_limit_reached"]:
        blockers.append("Git history scan limit was reached")
    # An offline source/config scan cannot establish that dependencies have no
    # currently disclosed vulnerabilities.  The release pipeline must attach
    # pip-audit/npm-audit or an equivalent signed report.
    blockers.append("current vulnerability scan attestation is not attached")
    payload = {
        "status": "BLOCKED" if blockers else "PASS",
        "blockers": blockers,
        "python_dependency_pinning": pinning,
        "node_lock_integrity": node,
        "tracked_secret_scan": secrets,
        "git_history_secret_scan": history,
        "vulnerability_scan": "NOT_RUN_ATTESTATION_REQUIRED",
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
