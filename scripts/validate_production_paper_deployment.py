"""Validate that the production-paper artifacts can actually start.

This is a no-network, no-exchange preflight. It validates entrypoints, role
boundaries, the canonical publication outbox and the production control-plane
composition from an isolated temporary data root.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PREDICTOR_ROOT = ROOT / "ai_bot3" / "ai_bot3"
EXECUTOR_ROOT = ROOT / "BybitContractBotV4"


def _read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        raise RuntimeError(f"required deployment artifact is missing: {relative}")
    return path.read_text(encoding="utf-8")


def _run(command: list[str], *, cwd: Path, env: dict[str, str]) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"preflight command failed ({completed.returncode}): "
            f"{' '.join(command)}\n{completed.stdout}\n{completed.stderr}"
        )
    return completed.stdout.strip()


def _base_environment(role: str) -> dict[str, str]:
    environment = dict(os.environ)
    for name in (
        "BYBIT_API_KEY",
        "BYBIT_SECRET_KEY",
        "EXECUTION_DB_PATH",
        "AI_BOT_FEATURE_STORE_PATH",
        "AI_BOT_KLINE_FEATURE_STORE_PATH",
        "BYBIT_PUBLIC_PIT_STORE",
        "LOCKBOX_BYBIT_PIT_STORE",
        "MACRO_PIT_STORE",
        "FLOW_PIT_STORE",
        "AI_BOT_MODEL_WRITE_DIR",
        "MODEL_OUTPUT_DIR",
        "RESEARCH_JOB_DB",
        "TRAINING_ENABLED",
        "BACKFILL_ENABLED",
    ):
        environment.pop(name, None)
    environment.update(
        {
            "APP_ENV": "production",
            "SERVICE_ROLE": role,
            "EXECUTION_MODE": "paper",
            "BYBIT_TRADING_MODE": "shadow",
            "HOST_ID": f"{role}-paper-01",
            "CLUSTER_ID": "two-node-paper",
            "DEPLOYMENT_ID": "two-node-paper-v1",
            "BYBIT_ENABLE_LIVE": "false",
            "MAINNET_ALLOWED": "false",
        }
    )
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT), str(PREDICTOR_ROOT), str(EXECUTOR_ROOT)]
    )
    return environment


def _validate_static_artifacts() -> dict[str, Any]:
    executor_unit = _read(
        "deploy/executor-production-paper/systemd/executor.service"
    )
    control_unit = _read(
        "deploy/predictor-production-paper/systemd/control-plane-api.service"
    )
    predictor_unit = _read(
        "deploy/predictor-production-paper/systemd/predictor-realtime.service"
    )
    worker_unit = _read(
        "deploy/predictor-production-paper/systemd/publication-worker.service"
    )
    predictor_env = _read(
        "deploy/predictor-production-paper/.env.production-paper.example"
    )
    windows_predictor = _read(
        "deploy/predictor-production-paper/windows/install-services.ps1"
    )
    windows_executor = _read(
        "deploy/executor-production-paper/windows/install-service.ps1"
    )
    predictor_entry = _read("ai_bot3/ai_bot3/main_forecast.py")

    checks = {
        "executor_entrypoint_exists": (EXECUTOR_ROOT / "main.py").is_file(),
        "executor_systemd_uses_main": "python main.py" in executor_unit,
        "executor_systemd_has_repo_pythonpath": (
            "Environment=PYTHONPATH=/opt/ai-bybit:" in executor_unit
        ),
        "control_plane_uses_versioned_app": (
            "api.control_plane_main:app" in control_unit
            and "api/api_server.py" not in control_unit
        ),
        "predictor_systemd_has_repo_pythonpath": (
            "Environment=PYTHONPATH=/opt/ai-bybit:" in predictor_unit
        ),
        "worker_systemd_has_repo_pythonpath": (
            "Environment=PYTHONPATH=/opt/ai-bybit:" in worker_unit
        ),
        "canonical_outbox_variable": (
            "FORECAST_PUBLICATION_OUTBOX_DB=" in predictor_env
            and "\nFORECAST_PUBLICATION_OUTBOX=" not in predictor_env
        ),
        "production_predictor_has_no_research_db": (
            "\nRESEARCH_JOB_DB=" not in predictor_env
        ),
        "windows_predictor_uses_versioned_app": (
            "api.control_plane_main:app" in windows_predictor
            and "api\\api_server.py" not in windows_predictor
        ),
        "windows_executor_uses_main": '"main.py"' in windows_executor,
        "predictor_uses_external_runtime_root": (
            "PREDICTOR_DATA_DIR" in predictor_entry
            and "os.chdir(root)" in predictor_entry
            and 'PROJECT_ROOT / "config.yml"' in predictor_entry
        ),
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise RuntimeError("deployment artifact checks failed: " + ", ".join(failed))
    return checks


def _validate_executor_preflight(temp_root: Path) -> dict[str, Any]:
    environment = _base_environment("executor")
    # The checked-in entrypoint must be usable from its service directory even
    # when an operator does not manually export PYTHONPATH.
    environment.pop("PYTHONPATH", None)
    environment.update(
        {
            "EXECUTION_DB_PATH": str(temp_root / "executor" / "execution.sqlite3"),
            "TICKET_API_BASE_URL": "https://predictor-paper.internal:8443",
            "CONTROL_PLANE_API_TOKEN": "executor-specific-token",
            "CONTROL_PLANE_MTLS_CERT": str(temp_root / "pki" / "executor.crt"),
            "CONTROL_PLANE_MTLS_KEY": str(temp_root / "pki" / "executor.key"),
            "CONTROL_PLANE_CERT_IDENTITY": "executor-paper-01",
            "PREDICTION_CA_BUNDLE": str(temp_root / "pki" / "control-plane-ca.crt"),
            "TICKET_CONSUMER_ID": "executor-paper-01",
            "POSITION_OWNER_ID": "paper-owner-executor-01",
            "EXECUTOR_VERSION": "1.0.0",
        }
    )
    output = _run(
        [sys.executable, "main.py", "--preflight"],
        cwd=EXECUTOR_ROOT,
        env=environment,
    )
    payload = json.loads(output)
    if (
        payload.get("status") != "PASS"
        or payload.get("private_trading_api_enabled") is not False
        or payload.get("mainnet_order_submission_enabled") is not False
        or payload.get("real_capital_at_risk") is not False
    ):
        raise RuntimeError("executor preflight did not remain paper-only")
    return payload


def _validate_predictor_composition(temp_root: Path) -> dict[str, Any]:
    environment = _base_environment("predictor")
    outbox_path = temp_root / "publication" / "forecast-outbox.sqlite3"
    research_path = temp_root / "forbidden-research" / "research.sqlite3"
    environment.update(
        {
            "PREDICTOR_DATA_DIR": str(temp_root / "predictor"),
            "FORECAST_PUBLICATION_OUTBOX_DB": str(outbox_path),
            "CONTROL_PLANE_DB": str(temp_root / "control" / "control.sqlite3"),
            "RESEARCH_JOB_DB": str(research_path),
            "CONTROL_PLANE_API_TOKEN": "global-control-token",
            "CONTROL_PLANE_EXECUTOR_TOKENS": (
                '{"executor-paper-01":"executor-token"}'
            ),
            "CONTROL_PLANE_CONSUMER_IDENTITIES": (
                '{"executor-paper-01":"executor-paper-01"}'
            ),
            "VALIDATION_RESULTS_DIR": str(temp_root / "results"),
        }
    )
    code = """
import json
import os
from pathlib import Path
from api.control_plane_main import app
from core.result_manager import ResultManager

expected = Path(os.environ["FORECAST_PUBLICATION_OUTBOX_DB"]).resolve()
manager = ResultManager(
    Path(os.environ["VALIDATION_RESULTS_DIR"]),
    tickets_enabled=False,
)
payload = {
    "route_count": len(app.routes),
    "publication_outbox": str(manager.publication_outbox_db),
    "canonical_outbox_match": manager.publication_outbox_db == expected,
}
print(json.dumps(payload, sort_keys=True))
"""
    output = _run(
        [sys.executable, "-c", code],
        cwd=PREDICTOR_ROOT,
        env=environment,
    )
    payload = json.loads(output)
    if not payload.get("canonical_outbox_match"):
        raise RuntimeError("predictor and publication worker outbox paths diverged")
    if research_path.exists():
        raise RuntimeError("production control plane created a research database")
    return payload


def _code_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unknown"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        with tempfile.TemporaryDirectory(
            prefix="production-paper-deployment-"
        ) as directory:
            temp_root = Path(directory)
            report = {
                "schema_version": "production-paper-deployment-preflight.v1",
                "status": "PASS",
                "code_commit": _code_commit(),
                "execution_mode": "paper",
                "mainnet_allowed": False,
                "real_capital_at_risk": False,
                "static_artifacts": _validate_static_artifacts(),
                "executor": _validate_executor_preflight(temp_root),
                "predictor": _validate_predictor_composition(temp_root),
            }
    except Exception as exc:
        report = {
            "schema_version": "production-paper-deployment-preflight.v1",
            "status": "FAIL",
            "code_commit": _code_commit(),
            "execution_mode": "paper",
            "mainnet_allowed": False,
            "real_capital_at_risk": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
