"""Build physically separated predictor/executor production-paper bundles.

Run this script on a trusted build/operations machine from an exact Git checkout.
It never includes databases, model artifacts, credentials, logs or caches.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

PREDICTOR_PATHS = (
    "ai_bot3/ai_bot3",
    "shadow_contracts",
    "requirements",
    "deploy/predictor-production-paper",
    "runtime-data-manifest.json",
    "runtime-data-manifest.v1.json",
    "schemas/runtime-data-manifest.v1.schema.json",
    "README.md",
)

EXECUTOR_PATHS = (
    "BybitContractBotV4",
    "shadow_contracts",
    "requirements",
    "deploy/executor-production-paper",
    "runtime-data-manifest.json",
    "runtime-data-manifest.v1.json",
    "schemas/runtime-data-manifest.v1.schema.json",
    "README.md",
)

FORBIDDEN_NAMES = {
    ".env",
    ".env.local",
    "__pycache__",
    ".pytest_cache",
    ".codex-pytest",
    ".git",
    ".version-history",
}
FORBIDDEN_SUFFIXES = {
    ".sqlite",
    ".sqlite3",
    ".db",
    ".log",
    ".key",
    ".pem",
    ".p12",
    ".pfx",
    ".keras",
    ".h5",
    ".joblib",
    ".pkl",
    ".pt",
    ".safetensors",
    ".zip",
    ".tar",
    ".gz",
}
FORBIDDEN_PARTS = {
    "data",
    "models",
    "model_results",
    "logs",
    "cache",
    "caches",
    "backups",
}


def _resolve_commit() -> str:
    configured = os.environ.get("APP_CODE_COMMIT", "").strip()
    if configured:
        return configured
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _forbidden(relative: Path) -> bool:
    lowered_parts = {part.lower() for part in relative.parts}
    if lowered_parts & FORBIDDEN_PARTS:
        return True
    if any(part.lower() in FORBIDDEN_NAMES for part in relative.parts):
        return True
    name = relative.name.lower()
    if name.endswith(("-wal", "-shm")):
        return True
    return relative.suffix.lower() in FORBIDDEN_SUFFIXES


def _copy_selected(destination: Path, selected: tuple[str, ...]) -> list[str]:
    copied: list[str] = []
    for raw in selected:
        source = ROOT / raw
        if not source.exists():
            raise FileNotFoundError(source)
        if source.is_file():
            relative = source.relative_to(ROOT)
            if _forbidden(relative):
                raise RuntimeError(f"selected deployment file is forbidden: {relative}")
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            copied.append(relative.as_posix())
            continue
        for item in source.rglob("*"):
            if not item.is_file():
                continue
            relative = item.relative_to(ROOT)
            if _forbidden(relative):
                continue
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)
            copied.append(relative.as_posix())
    return sorted(set(copied))


def _archive(role: str, selected: tuple[str, ...], output: Path, commit: str) -> dict:
    with tempfile.TemporaryDirectory(prefix=f"ai-bybit-{role}-bundle-") as directory:
        root = Path(directory) / "ai-bybit"
        root.mkdir(parents=True)
        files = _copy_selected(root, selected)
        release = {
            "schema_version": "two-node-deployment-bundle.v1",
            "role": role,
            "execution_mode": "paper",
            "code_commit": commit,
            "mainnet_allowed": False,
            "files": files,
        }
        (root / "RELEASE_MANIFEST.json").write_text(
            json.dumps(release, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(output, "w:gz") as archive:
            archive.add(root, arcname="ai-bybit")
    return {
        "role": role,
        "path": str(output.resolve()),
        "sha256": _sha256(output),
        "size_bytes": output.stat().st_size,
        "file_count": len(files) + 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    commit = _resolve_commit()
    short = commit[:12]
    predictor = _archive(
        "predictor",
        PREDICTOR_PATHS,
        args.output_dir / f"ai-bybit-predictor-paper-{short}.tar.gz",
        commit,
    )
    executor = _archive(
        "executor",
        EXECUTOR_PATHS,
        args.output_dir / f"ai-bybit-executor-paper-{short}.tar.gz",
        commit,
    )
    report = {
        "status": "PASS",
        "code_commit": commit,
        "execution_mode": "paper",
        "mainnet_allowed": False,
        "bundles": [predictor, executor],
        "root_scripts_runtime_service": False,
    }
    manifest = args.output_dir / f"two-node-bundles-{short}.json"
    manifest.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
