from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen


WORKSPACE = Path(__file__).resolve().parents[1]
AI_ROOT = WORKSPACE / "ai_bot3" / "ai_bot3"
sys.path.insert(0, str(AI_ROOT))

from adapters.legacy_forecast_adapter import LegacyForecastAdapter
from core.control_plane import ControlPlaneRepository
from core.decision.ticket_builder import TicketBuilder


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


def main() -> int:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        temp = Path(directory)
        control_db = temp / "control.sqlite3"
        execution_db = temp / "execution.sqlite3"
        now = datetime.now(timezone.utc).replace(microsecond=0)
        forecast = LegacyForecastAdapter().adapt(
            "BTCUSDT",
            "scalping",
            {
                "generated_at": now.isoformat(),
                "latest_kline_ts": now.isoformat(),
                "trend": "up",
                "calibrated_trend": "up",
                "confidence": 0.8,
                "predicted_return": 0.008,
                "calibrated_predicted_return": 0.008,
                "out_of_distribution_score": 0.1,
                "calibration_status": "valid",
                "market_regime": "risk_on",
                "data_source_status": "ok",
                "data_source_reliable": True,
                "current_price_age_seconds": 0,
                "context_completeness": {"score": 0.96},
                "model_version": "shadow-e2e-v1",
            },
        )
        ticket = TicketBuilder().build_open_ticket(
            forecast, reference_price=100000, required_position_version=0
        )
        if ticket is None:
            raise RuntimeError("forecast did not clear the ticket policy")
        ControlPlaneRepository(control_db).publish(forecast, ticket)

        port = free_port()
        base_url = f"http://127.0.0.1:{port}"
        environment = dict(os.environ)
        environment.update(
            {
                "CONTROL_PLANE_DB": str(control_db),
                "RESEARCH_JOB_DB": str(temp / "research.sqlite3"),
                "PYTHONPATH": str(AI_ROOT),
            }
        )
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        server = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "api.control_plane_main:app",
                "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
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
                [sys.executable, str(WORKSPACE / "scripts" / "shadow_e2e_worker.py"), base_url, str(execution_db)],
                cwd=WORKSPACE,
                capture_output=True,
                text=True,
                timeout=30,
                creationflags=flags,
            )
            if worker.returncode:
                raise RuntimeError(f"shadow worker failed: {worker.stdout}\n{worker.stderr}")
            result = json.loads(worker.stdout.strip().splitlines()[-1])
            with sqlite3.connect(control_db) as connection:
                receipts = connection.execute("SELECT COUNT(*) FROM execution_receipts").fetchone()[0]
            result["control_plane_receipt_count"] = receipts
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
