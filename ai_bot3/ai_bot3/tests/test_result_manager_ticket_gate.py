from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.result_manager import ResultManager
from contracts.strategy_release_v1 import StrategyReleaseBundle
from core.release.strategy_bundle import canonical_bundle_hash


def _iso(point: datetime) -> str:
    return point.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _prediction(stage: str, release_id: str = "sr_result_gate_test_001") -> dict:
    # Ticket generation is intentionally time-gated.  Tests must provide a
    # currently valid forecast instead of relying on a historical wall-clock
    # literal that eventually expires on CI.
    generated_at = datetime.now(timezone.utc)
    return {
        "generated_at": _iso(generated_at),
        "latest_kline_ts": _iso(generated_at - timedelta(seconds=5)),
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
        "strategy_release_id": release_id,
        "model_version": "lstm-test",
        "brain_prediction": {
            "version": "brain-test",
            "status": "ok",
            "direction": "long",
            "actionable": True,
            "release_stage": stage,
            "strategy_release_id": release_id,
            "confidence": 0.9,
            "expected_return": 0.01,
        },
    }


def _bundle(stage: str, release_id: str = "sr_result_gate_test_001") -> StrategyReleaseBundle:
    hashes = {
        key: "0" * 64
        for key in (
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
    }
    payload = {
        "strategy_release_id": release_id,
        "release_stage": stage,
        "created_at": _iso(datetime.now(timezone.utc) - timedelta(hours=1)),
        "code_commit": "1234567",
        "artifacts": hashes,
        "approval_id": "approval-test-001",
        "approved_by": "test-reviewer",
    }
    payload["bundle_sha256"] = canonical_bundle_hash(payload)
    return StrategyReleaseBundle.model_validate(payload)


def _counts(db_path: Path) -> tuple[int, int]:
    with closing(sqlite3.connect(db_path)) as connection:
        forecasts = connection.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
        tickets = connection.execute("SELECT COUNT(*) FROM operation_tickets").fetchone()[0]
    return int(forecasts), int(tickets)


def test_default_gate_requires_verified_release_and_two_horizons_before_ticket():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / "control.sqlite3"
        manager = ResultManager(root / "results", control_plane_db=db, tickets_enabled=True)
        asyncio.run(manager.save_result("BTCUSDT", "scalping", _prediction("live")))
        assert _counts(db) == (1, 0)

        manager = ResultManager(
            root / "results",
            control_plane_db=db,
            tickets_enabled=True,
            strategy_release_bundle=_bundle("live"),
        )
        asyncio.run(manager.save_result("BTCUSDT", "mid_short", _prediction("live")))
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
            strategy_release_bundle=_bundle("candidate"),
        )
        asyncio.run(manager.save_result("BTCUSDT", "scalping", _prediction("candidate")))
        assert _counts(db) == (1, 0)
        asyncio.run(manager.save_result("BTCUSDT", "mid_short", _prediction("candidate")))
        assert _counts(db) == (2, 1)


def test_prediction_file_is_atomic_and_read_does_not_refresh_its_age():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        manager = ResultManager(
            root / "results",
            control_plane_db=root / "control.sqlite3",
            tickets_enabled=False,
        )
        asyncio.run(manager.save_result("BTCUSDT", "scalping", _prediction("shadow")))
        path = root / "results" / "BTCUSDT_scalping.json"
        first = json.loads(path.read_text(encoding="utf-8"))
        loaded = manager.get_latest_results()["BTCUSDT"]["details"]["scalping"]
        assert loaded["saved_at"] == first["saved_at"]
        assert loaded["updated_at"] == first["updated_at"]
        assert list((root / "results").glob("*.tmp")) == []
