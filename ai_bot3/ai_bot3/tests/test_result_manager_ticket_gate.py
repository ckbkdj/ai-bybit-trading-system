from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.result_manager import ResultManager


def _prediction(stage: str) -> dict:
    return {
        "generated_at": "2026-08-21T08:00:00Z",
        "latest_kline_ts": "2026-08-21T07:59:55Z",
        "trend": "up",
        "calibrated_trend": "up",
        "calibration_status": "valid",
        "confidence": 0.9,
        "predicted_return": 0.01,
        "current_price": 100_000.0,
        "current_price_age_seconds": 5,
        "data_source_status": "ok",
        "data_source_reliable": True,
        "context_completeness": {"score": 0.96},
        "out_of_distribution_score": 0.1,
        "market_regime": "risk_on",
        "model_version": "lstm-test",
        "brain_prediction": {
            "version": "brain-test",
            "status": "ok",
            "direction": "long",
            "actionable": True,
            "release_stage": stage,
            "confidence": 0.9,
            "expected_return": 0.01,
        },
    }


def _counts(db_path: Path) -> tuple[int, int]:
    with closing(sqlite3.connect(db_path)) as connection:
        forecasts = connection.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
        tickets = connection.execute("SELECT COUNT(*) FROM operation_tickets").fetchone()[0]
    return int(forecasts), int(tickets)


def test_default_gate_publishes_forecast_but_only_live_brain_can_emit_ticket():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / "control.sqlite3"
        manager = ResultManager(root / "results", control_plane_db=db, tickets_enabled=True)
        asyncio.run(manager.save_result("BTCUSDT", "scalping", _prediction("candidate")))
        assert _counts(db) == (1, 0)
        asyncio.run(manager.save_result("ETHUSDT", "scalping", _prediction("live")))
        assert _counts(db) == (2, 1)


def test_candidate_stage_requires_explicit_testnet_policy():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / "control.sqlite3"
        manager = ResultManager(
            root / "results",
            control_plane_db=db,
            tickets_enabled=True,
            required_brain_release_stage="candidate",
        )
        asyncio.run(manager.save_result("BTCUSDT", "scalping", _prediction("candidate")))
        assert _counts(db) == (1, 1)
