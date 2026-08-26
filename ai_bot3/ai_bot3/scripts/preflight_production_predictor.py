"""Offline production-paper preflight for the predictor node."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(REPOSITORY_ROOT))

from core.deployment_bootstrap import resolve_publication_outbox_path  # noqa: E402
from core.service_runtime import load_predictor_runtime  # noqa: E402


def _placeholder(value: str) -> bool:
    normalized = value.strip().lower()
    return (
        not normalized
        or normalized.startswith("<")
        or "change-me" in normalized
        or "replace-me" in normalized
    )


def _path_setting(name: str) -> Path | None:
    raw = os.environ.get(name, "").strip()
    return Path(raw).expanduser().resolve() if raw else None


def main() -> int:
    identity = load_predictor_runtime()
    errors: list[str] = []

    if identity.execution_mode.value != "paper":
        errors.append("predictor production release must run EXECUTION_MODE=paper")
    if os.environ.get("MAINNET_ALLOWED", "false").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }:
        errors.append("MAINNET_ALLOWED must remain false")

    global_token = os.environ.get("CONTROL_PLANE_API_TOKEN", "")
    if _placeholder(global_token):
        errors.append("CONTROL_PLANE_API_TOKEN is missing or still a placeholder")

    for name in (
        "CONTROL_PLANE_EXECUTOR_TOKENS",
        "CONTROL_PLANE_CONSUMER_IDENTITIES",
    ):
        raw = os.environ.get(name, "").strip()
        if _placeholder(raw):
            errors.append(f"{name} is missing or still a placeholder")
        else:
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict) or not payload:
                    raise ValueError
                if any(_placeholder(str(value)) for value in payload.values()):
                    errors.append(f"{name} contains a placeholder value")
            except (json.JSONDecodeError, ValueError):
                errors.append(f"{name} must be a non-empty JSON object")

    bind_host = os.environ.get("CONTROL_PLANE_BIND_HOST", "127.0.0.1").strip()
    if bind_host not in {"127.0.0.1", "::1", "localhost"}:
        errors.append("production control plane application must bind loopback only")

    outbox = resolve_publication_outbox_path()
    if outbox is None:
        errors.append("FORECAST_PUBLICATION_OUTBOX_DB is required")

    paths = {
        name: _path_setting(name)
        for name in (
            "PREDICTOR_DATA_DIR",
            "CONTROL_PLANE_DB",
            "RESEARCH_JOB_DB",
            "BYBIT_PUBLIC_PIT_STORE",
        )
    }
    paths["FORECAST_PUBLICATION_OUTBOX_DB"] = outbox
    for name, path in paths.items():
        if path is None:
            errors.append(f"{name} is required")
            continue
        parent = path if name == "PREDICTOR_DATA_DIR" else path.parent
        if not parent.exists():
            errors.append(f"parent directory does not exist for {name}: {parent}")
        elif not parent.is_dir():
            errors.append(f"parent path is not a directory for {name}: {parent}")

    resolved_paths = [str(path) for path in paths.values() if path is not None]
    if len(resolved_paths) != len(set(resolved_paths)):
        errors.append("predictor/control/outbox/collector paths must be distinct")

    if errors:
        print(
            json.dumps(
                {"status": "FAIL", "errors": errors},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2

    print(
        json.dumps(
            {
                "status": "PASS",
                "deployment_environment": identity.app_environment.value,
                "service_role": identity.service_role.value,
                "execution_mode": identity.execution_mode.value,
                "host_id": identity.host_id,
                "cluster_id": identity.cluster_id,
                "deployment_id": identity.deployment_id,
                "publication_outbox": str(outbox),
                "mainnet_allowed": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
