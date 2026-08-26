from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREDICTOR_ROOT = ROOT / "ai_bot3" / "ai_bot3"
EXECUTOR_ROOT = ROOT / "BybitContractBotV4"


def _python_path(*paths: Path) -> str:
    return os.pathsep.join(str(path.resolve()) for path in paths)


def _assert_entrypoints() -> list[str]:
    required = [
        EXECUTOR_ROOT / "main.py",
        EXECUTOR_ROOT / "bot_threshold_super_v4_1.py",
        PREDICTOR_ROOT / "main_forecast.py",
        PREDICTOR_ROOT / "api" / "api_server.py",
        PREDICTOR_ROOT / "scripts" / "run_publication_worker.py",
        PREDICTOR_ROOT / "scripts" / "run_bybit_public_pit_collector.py",
        ROOT / "shadow_contracts" / "runtime.py",
    ]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise RuntimeError("deployment entrypoints missing: " + ", ".join(missing))

    executor_unit = (
        ROOT
        / "deploy"
        / "executor-production-paper"
        / "systemd"
        / "executor.service"
    ).read_text(encoding="utf-8")
    if "python main.py" not in executor_unit:
        raise RuntimeError("executor systemd unit does not use canonical main.py")
    if "main.py --preflight" not in executor_unit:
        raise RuntimeError("executor systemd unit has no offline preflight")
    if "PYTHONPATH=/opt/ai-bybit" not in executor_unit:
        raise RuntimeError("executor systemd unit cannot import shared contracts")

    predictor_env = (
        ROOT
        / "deploy"
        / "predictor-production-paper"
        / ".env.production-paper.example"
    ).read_text(encoding="utf-8")
    if "FORECAST_PUBLICATION_OUTBOX_DB=" not in predictor_env:
        raise RuntimeError("canonical publication outbox setting is absent")
    legacy_lines = [
        line
        for line in predictor_env.splitlines()
        if line.startswith("FORECAST_PUBLICATION_OUTBOX=")
    ]
    if legacy_lines:
        raise RuntimeError("legacy publication outbox variable remains active")
    return [str(path.relative_to(ROOT)) for path in required]


def _run_executor_preflight(temp_root: Path) -> dict:
    pki = temp_root / "pki"
    pki.mkdir(parents=True)
    for name in ("client.crt", "client.key", "ca.crt"):
        (pki / name).write_text("deployment-smoke-placeholder\n", encoding="utf-8")

    environment = dict(os.environ)
    environment.update(
        {
            "APP_ENV": "production",
            "SERVICE_ROLE": "executor",
            "EXECUTION_MODE": "paper",
            "BYBIT_TRADING_MODE": "shadow",
            "HOST_ID": "executor-smoke-01",
            "CLUSTER_ID": "deployment-smoke",
            "DEPLOYMENT_ID": "deployment-smoke-v1",
            "PYTHONPATH": _python_path(ROOT, EXECUTOR_ROOT),
            "EXECUTION_DB_PATH": str(temp_root / "executor.sqlite3"),
            "TICKET_API_BASE_URL": "https://predictor-smoke.internal:8443",
            "CONTROL_PLANE_API_TOKEN": "smoke-token-not-a-secret",
            "CONTROL_PLANE_MTLS_CERT": str(pki / "client.crt"),
            "CONTROL_PLANE_MTLS_KEY": str(pki / "client.key"),
            "CONTROL_PLANE_CERT_IDENTITY": "executor-smoke-01",
            "PREDICTION_CA_BUNDLE": str(pki / "ca.crt"),
            "TICKET_CONSUMER_ID": "executor-smoke-01",
            "POSITION_OWNER_ID": "paper-owner-smoke-01",
            "BYBIT_ENABLE_LIVE": "false",
            "MAINNET_ALLOWED": "false",
        }
    )
    for forbidden in ("BYBIT_API_KEY", "BYBIT_SECRET_KEY"):
        environment.pop(forbidden, None)
    completed = subprocess.run(
        [sys.executable, "main.py", "--preflight"],
        cwd=EXECUTOR_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "executor preflight failed: " + completed.stdout + completed.stderr
        )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    if payload.get("execution_mode") != "paper" or payload.get("mainnet_enabled"):
        raise RuntimeError("executor preflight did not remain production-paper")
    return payload


def _run_predictor_path_and_control_plane_smoke(temp_root: Path) -> dict:
    data_root = temp_root / "predictor"
    data_root.mkdir(parents=True)
    outbox_path = data_root / "publication.sqlite3"
    control_path = data_root / "control.sqlite3"
    research_path = data_root / "research-disabled.sqlite3"

    environment = dict(os.environ)
    environment.update(
        {
            "APP_ENV": "production",
            "SERVICE_ROLE": "predictor",
            "EXECUTION_MODE": "paper",
            "BYBIT_TRADING_MODE": "shadow",
            "HOST_ID": "predictor-smoke-01",
            "CLUSTER_ID": "deployment-smoke",
            "DEPLOYMENT_ID": "deployment-smoke-v1",
            "PYTHONPATH": _python_path(ROOT, PREDICTOR_ROOT),
            "PREDICTOR_DATA_DIR": str(data_root),
            "FORECAST_PUBLICATION_OUTBOX_DB": str(outbox_path),
            "CONTROL_PLANE_DB": str(control_path),
            "RESEARCH_JOB_DB": str(research_path),
            "CONTROL_PLANE_API_TOKEN": "global-smoke-token",
            "CONTROL_PLANE_EXECUTOR_TOKENS": json.dumps(
                {"executor-smoke-01": "executor-smoke-token"}
            ),
            "CONTROL_PLANE_CONSUMER_IDENTITIES": json.dumps(
                {"executor-smoke-01": "executor-smoke-01"}
            ),
            "CONTROL_PLANE_TRUSTED_REVERSE_PROXY": "true",
            "MAINNET_ALLOWED": "false",
        }
    )
    for forbidden in ("BYBIT_API_KEY", "BYBIT_SECRET_KEY", "EXECUTION_DB_PATH"):
        environment.pop(forbidden, None)

    code = r'''
import json
import os
import tempfile
from pathlib import Path
from core.deployment_bootstrap import configure_predictor_runtime_paths

expected = Path(os.environ["FORECAST_PUBLICATION_OUTBOX_DB"]).resolve()
configured = configure_predictor_runtime_paths()
from core.result_manager import ResultManager
from scripts.run_publication_worker import build_worker
from api.control_plane_api import create_control_plane_router

root = Path(os.environ["PREDICTOR_DATA_DIR"]).resolve()
manager = ResultManager(root / "model_results", tickets_enabled=False)
worker, worker_outbox = build_worker()
router = create_control_plane_router(Path.cwd())
actual = manager.publication_outbox.db_path.resolve()
worker_path = worker_outbox.db_path.resolve()
assert configured == expected
assert actual == expected
assert worker_path == expected
assert len(router.routes) > 0
print(json.dumps({
    "expected_outbox": str(expected),
    "result_manager_outbox": str(actual),
    "publication_worker_outbox": str(worker_path),
    "control_plane_routes": len(router.routes),
    "research_store_path": os.environ["RESEARCH_JOB_DB"],
}, sort_keys=True))
'''
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PREDICTOR_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "predictor deployment smoke failed: "
            + completed.stdout
            + completed.stderr
        )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def main() -> int:
    entrypoints = _assert_entrypoints()
    with tempfile.TemporaryDirectory(prefix="ai-bybit-deployment-smoke-") as directory:
        temp_root = Path(directory)
        executor = _run_executor_preflight(temp_root)
        predictor = _run_predictor_path_and_control_plane_smoke(temp_root)
    report = {
        "status": "PASS",
        "deployment_mode": "production-paper",
        "entrypoints": entrypoints,
        "executor_preflight": executor,
        "predictor": predictor,
        "private_exchange_api_used": False,
        "mainnet_allowed": False,
        "background_processes_remaining": 0,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
