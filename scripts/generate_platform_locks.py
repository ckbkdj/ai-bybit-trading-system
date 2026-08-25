"""Resolve and hash exact wheel closures for the supported CI platforms."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, parse_sdist_filename, parse_wheel_filename


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Target:
    output: str
    platforms: tuple[str, ...]
    python_version: str
    abi: str
    seed: str


TARGETS = (
    Target(
        output="requirements/windows-py312.lock",
        platforms=("win_amd64",),
        python_version="312",
        abi="cp312",
        seed="declared",
    ),
    Target(
        output="requirements/windows-py311.lock",
        platforms=("win_amd64",),
        python_version="311",
        abi="cp311",
        seed="declared",
    ),
    Target(
        output="requirements/linux-py311.lock",
        platforms=("manylinux_2_28_x86_64", "manylinux2014_x86_64"),
        python_version="311",
        abi="cp311",
        seed="declared",
    ),
    Target(
        output="requirements/audit.lock",
        platforms=("manylinux_2_28_x86_64", "manylinux2014_x86_64"),
        python_version="311",
        abi="cp311",
        seed="requirements/audit.lock",
    ),
    Target(
        output="requirements/audit-windows-py312.lock",
        platforms=("win_amd64",),
        python_version="312",
        abi="cp312",
        seed="requirements/audit.lock",
    ),
)


def _seed_requirements(path: Path) -> list[str]:
    requirements = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        value = raw.strip()
        if not value or value.startswith("#"):
            continue
        requirements.append(value.split(" --hash=", 1)[0])
    return requirements


def _declared_requirements() -> list[str]:
    inputs = (
        ROOT / "ai_bot3" / "ai_bot3" / "requirements.txt",
        ROOT / "ai_bot3" / "ai_bot3" / "requirements-dev.txt",
        ROOT / "BybitContractBotV4" / "requirements.txt",
    )
    requirements = []
    for path in inputs:
        for raw in path.read_text(encoding="utf-8").splitlines():
            value = raw.split("#", 1)[0].strip()
            if value and not value.startswith("-r ") and value not in requirements:
                requirements.append(value)
    return requirements


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def resolve(target: Target) -> None:
    seed = (
        _declared_requirements()
        if target.seed == "declared"
        else _seed_requirements(ROOT / target.seed)
    )
    source_only = [
        "PyExecJS==1.5.1"
        for item in seed
        if canonicalize_name(Requirement(item).name) == "pyexecjs"
    ]
    wheel_seed = [
        item
        for item in seed
        if canonicalize_name(Requirement(item).name) != "pyexecjs"
    ]
    with tempfile.TemporaryDirectory(prefix="shadow-lock-") as directory:
        destination = Path(directory)
        platform_options = [
            option
            for platform in target.platforms
            for option in ("--platform", platform)
        ]
        command = [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--disable-pip-version-check",
            "--only-binary=:all:",
            *platform_options,
            "--python-version",
            target.python_version,
            "--implementation",
            "cp",
            "--abi",
            target.abi,
            "--dest",
            str(destination),
            *wheel_seed,
        ]
        platform_label = "+".join(target.platforms)
        print(f"resolving {target.output} ({platform_label}, cp{target.python_version})", flush=True)
        subprocess.run(command, cwd=ROOT, check=True)
        if source_only:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "download",
                    "--disable-pip-version-check",
                    "--no-deps",
                    "--no-binary=:all:",
                    "--dest",
                    str(destination),
                    *source_only,
                ],
                cwd=ROOT,
                check=True,
            )
        rows: list[tuple[str, str, str]] = []
        for artifact in destination.iterdir():
            if artifact.suffix == ".whl":
                name, version, _, _ = parse_wheel_filename(artifact.name)
            elif artifact.name.endswith((".tar.gz", ".zip")):
                name, version = parse_sdist_filename(artifact.name)
            else:
                raise RuntimeError(f"unexpected resolver artifact: {artifact.name}")
            rows.append((canonicalize_name(name), str(version), _sha256(artifact)))
        if not rows:
            raise RuntimeError(f"resolver produced no wheels for {target.output}")
        names = [name for name, _, _ in rows]
        if len(names) != len(set(names)):
            raise RuntimeError(f"resolver produced duplicate distributions for {target.output}")
        rendered = [
            "# Fully resolved wheel dependency closure.",
            f"# Target: {platform_label}, CPython {target.python_version[:1]}.{target.python_version[1:]}.",
            "# Generated by scripts/generate_platform_locks.py; do not hand-edit.",
            f"# Install with: python -m pip install --require-hashes -r {target.output}",
            "",
        ]
        rendered.extend(
            f"{name}=={version} --hash=sha256:{digest}"
            for name, version, digest in sorted(rows)
        )
        (ROOT / target.output).write_text("\n".join(rendered) + "\n", encoding="utf-8")
        print(f"wrote {target.output}: {len(rows)} distributions", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("targets", nargs="*", choices=[item.output for item in TARGETS])
    args = parser.parse_args()
    selected = set(args.targets)
    for target in TARGETS:
        if not selected or target.output in selected:
            resolve(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
