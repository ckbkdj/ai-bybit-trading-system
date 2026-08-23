from __future__ import annotations

import asyncio
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen


WORKSPACE = Path(__file__).resolve().parents[1]
AI_ROOT = WORKSPACE / "ai_bot3" / "ai_bot3"
sys.path.insert(0, str(AI_ROOT))

from contracts.strategy_release_v1 import StrategyReleaseBundle
from core.release.strategy_bundle import canonical_bundle_hash
from core.result_manager import ResultManager


RELEASE_ID = "sr_shadow_e2e_fixture_001"
ARTIFACT_KEYS = (
    "brain_model_sha256",
    "lstm_model_sha256",
    "scaler_sha256",
    "calibration_sha256",
    "feature_schema_sha256",
    "factor_weights_sha256",
    "cost_policy_sha256",
    "ticket_policy_sha256",
    "execution_policy_sha256",
    "training_snapshot_sha256",
    "evidence_bundle_sha256",
)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_healthy(base_url: str, process: subprocess.Popen, timeout: float = 15) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"control plane exited early: {stdout}\n{stderr}")
        try:
            with urlopen(f"{base_url}/v1/health", timeout=1) as response:
                if json.load(response).get("status") == "ok":
                    return
        except Exception:
            time.sleep(0.1)
    raise TimeoutError("control plane did not become healthy")


def release_bundle(now: datetime) -> StrategyReleaseBundle:
    payload = {
        "strategy_release_id": RELEASE_ID,
        "release_stage": "live",
        "created_at": (now - timedelta(hours=1)).isoformat(),
        "code_commit": "shadow-e2e-commit",
        "artifacts": {key: "0" * 64 for key in ARTIFACT_KEYS},
        "approval_id": "shadow-e2e-approval",
        "approved_by": "automated-shadow-regression",
    }
    payload["bundle_sha256"] = canonical_bundle_hash(payload)
    return StrategyReleaseBundle.model_validate(payload)


def prediction(now: datetime, *, mode: str, expected_return: float) -> dict:
    generated_at = now.isoformat()
    return {
        "generated_at": generated_at,
        "latest_kline_ts": (now - timedelta(seconds=5)).isoformat(),
        "trend": "up",
        "calibrated_trend": "up",
        "confidence": 0.9,
        "direction_confidence": 0.9,
        "predicted_return": expected_return,
        "calibrated_predicted_return": expected_return,
        "current_price": 100_000.0,
        "current_price_age_seconds": 5,
        "range_guard_score": 0.1,
        "calibration_status": "valid",
        "market_regime": "risk_on",
        "data_source_status": "ok",
        "data_source_reliable": True,
        "context_completeness": {"score": 0.96},
        "model_version": f"shadow-e2e-{mode}-v1",
        "strategy_release_id": RELEASE_ID,
        "brain_prediction": {
            "version": f"brain-shadow-e2e-{mode}",
            "status": "ok",
            "direction": "long",
            "actionable": True,
            "release_stage": "live",
            "strategy_release_id": RELEASE_ID,
            "confidence": 0.9,
            "expected_return": expected_return,
        },
    }


def publish_release_gated_ticket(temp: Path, control_db: Path, now: datetime) -> dict:
    manager = ResultManager(
        temp / "results",
        control_plane_db=control_db,
        tickets_enabled=True,
        required_brain_release_stage="live",
        strategy_release_bundle=release_bundle(now),
    )
    # One horizon is deliberately insufficient.  The second prediction must pass
    # the real release gate, SignalBook and PortfolioIntent path before a ticket
    # becomes visible to the execution service.
    asyncio.run(
        manager.save_result(
            "BTCUSDT", "scalping", prediction(now, mode="scalping", expected_return=0.010)
        )
    )
    with closing(sqlite3.connect(control_db)) as connection:
        first_ticket_count = int(
            connection.execute("SELECT COUNT(*) FROM operation_tickets").fetchone()[0]
        )
    if first_ticket_count != 0:
        raise RuntimeError("one horizon unexpectedly generated an executable ticket")

    asyncio.run(
        manager.save_result(
            "BTCUSDT", "mid_short", prediction(now, mode="mid_short", expected_return=0.009)
        )
    )
    with closing(sqlite3.connect(control_db)) as connection:
        forecast_count = int(connection.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0])
        ticket_count = int(
            connection.execute("SELECT COUNT(*) FROM operation_tickets").fetchone()[0]
        )
        intent_count = int(
            connection.execute("SELECT COUNT(*) FROM portfolio_intents").fetchone()[0]
        )
    if (forecast_count, intent_count, ticket_count) != (2, 1, 1):
        raise RuntimeError(
            "release-gated prediction path did not produce exactly "
            f"2 forecasts / 1 intent / 1 ticket: "
            f"{forecast_count}/{intent_count}/{ticket_count}"
        )
    return {
        "forecast_count": forecast_count,
        "portfolio_intent_count": intent_count,
        "ticket_count": ticket_count,
        "strategy_release_id": RELEASE_ID,
    }


def main() -> int:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        temp = Path(directory)
        control_db = temp / "control.sqlite3"
        execution_db = temp / "execution.sqlite3"
        now = datetime.now(timezone.utc).replace(microsecond=0)
        generation_evidence = publish_release_gated_ticket(temp, control_db, now)

        port = free_port()
        base_url = f"http://127.0.0.1:{port}"
        environment = dict(os.environ)
        environment.update(
            {
                "CONTROL_PLANE_DB": str(control_db),
                "RESEARCH_JOB_DB": str(temp / "research.sqlite3"),
                "PYTHONPATH": str(AI_ROOT),
                "BYBIT_TRADING_MODE": "shadow",
                "BYBIT_ENABLE_LIVE": "false",
            }
        )
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "api.control_plane_main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            cwd=AI_ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=flags,
        )
        try:
            wait_healthy(base_url, server)
            worker = subprocess.run(
                [
                    sys.executable,
                    str(WORKSPACE / "scripts" / "shadow_e2e_worker.py"),
                    base_url,
                    str(execution_db),
                ],
                cwd=WORKSPACE,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
                creationflags=flags,
            )
            if worker.returncode:
                raise RuntimeError(f"shadow worker failed: {worker.stdout}\n{worker.stderr}")
            result = json.loads(worker.stdout.strip().splitlines()[-1])
            with closing(sqlite3.connect(control_db)) as connection:
                receipts = int(
                    connection.execute("SELECT COUNT(*) FROM execution_receipts").fetchone()[0]
                )
            result.update(generation_evidence)
            result["control_plane_receipt_count"] = receipts
            result["path"] = (
                "ResultManager -> verified live release -> ForecastEnvelope[2] -> "
                "PortfolioIntent -> OperationTicket -> HTTP claim -> active shadow executor -> "
                "ExecutionReceipt"
            )
            if receipts != 1:
                raise RuntimeError("control plane did not persist the execution receipt")
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
            # Windows may release SQLite/WAL handles a moment after process exit.
            time.sleep(0.5)


if __name__ == "__main__":
    raise SystemExit(main())