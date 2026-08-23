from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any


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
        r"(?i)(?:api[_-]?key|secret[_-]?key|access[_-]?token|bearer[_-]?token)"
        r"\s*[:=]\s*['\"][A-Za-z0-9_./+=:-]{16,}['\"]"
    ),
}


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


def tracked_files() -> list[Path]:
    completed = subprocess.run(
        [
            "git",
            f"--git-dir={ROOT / '.version-history'}",
            f"--work-tree={ROOT}",
            "ls-files",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return [ROOT / line for line in completed.stdout.splitlines() if line.strip()]


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
        for kind, pattern in SECRET_PATTERNS.items():
            for match in pattern.finditer(text):
                findings.append(
                    {
                        "path": relative,
                        "line": text.count("\n", 0, match.start()) + 1,
                        "kind": kind,
                    }
                )
    return {
        "finding_count": len(findings),
        "findings_without_values": findings,
        "tracked_sensitive_paths": sensitive_paths,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline supply-chain and secret gate")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    pinning = dependency_pinning()
    node = node_lock_integrity()
    secrets = secret_scan()
    blockers = []
    if pinning["unpinned_entries"]:
        blockers.append("python dependencies are not fully locked")
    if node["missing_integrity_entries"]:
        blockers.append("node lock entries are missing integrity hashes")
    if secrets["finding_count"] or secrets["tracked_sensitive_paths"]:
        blockers.append("tracked repository may contain credentials")
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
