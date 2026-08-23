from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.legacy_forecast_adapter import LegacyForecastAdapter
from core.decision.portfolio_intent import PortfolioIntentBuilder
from core.decision.ticket_builder import TicketBuilder
from core.release.strategy_bundle import (
    StrategyReleaseLoader,
    StrategyReleaseVerificationError,
    canonical_bundle_hash,
)


RELEASE_ID = "sr_portfolio_release_test_001"


def _forecast(mode: str, trend: str, expected_return: float):
    return LegacyForecastAdapter().adapt(
        "BTCUSDT",
        mode,
        {
            "generated_at": "2026-08-21T08:00:00Z",
            "latest_kline_ts": "2026-08-21T07:59:55Z",
            "trend": trend,
            "confidence": 0.9,
            "predicted_return": expected_return,
            "current_price_age_seconds": 5,
            "data_source_status": "ok",
            "data_source_reliable": True,
            "context_completeness": {"score": 0.96},
            "calibration_status": "valid",
            "range_guard_score": 0.1,
            "strategy_release_id": RELEASE_ID,
            "model_version": f"model-{mode}",
        },
    )


def test_multi_horizon_signal_book_nets_once_and_ticket_references_decision():
    near = _forecast("scalping", "up", 0.01)
    far = _forecast("swing", "down", -0.008)
    intent = PortfolioIntentBuilder().build(
        [far, near],
        strategy_release_id=RELEASE_ID,
        decision_version=1,
        now=datetime(2026, 8, 21, 8, 1, tzinfo=timezone.utc),
    )
    assert intent is not None
    assert len(intent.contributions) == 2
    assert intent.target_net_exposure_pct > 0
    assert intent.target_short_exposure_pct == 0

    ticket = TicketBuilder().build_portfolio_ticket(
        intent,
        [near, far],
        reference_price=100_000,
        required_position_version=3,
    )
    assert ticket is not None
    assert ticket.portfolio_decision_id == intent.portfolio_decision_id
    assert ticket.strategy_release_id == RELEASE_ID
    assert ticket.intent.side == "BUY"
    assert ticket.guards.execution_market == "bybit"
    assert ticket.guards.forecast_market == "binance"
    assert ticket.created_at == intent.created_at
    assert ticket.expires_at <= intent.valid_until


def test_one_horizon_event_block_vetoes_the_whole_portfolio():
    near = _forecast("scalping", "up", 0.01)
    far = _forecast("swing", "up", 0.008)
    for event_regime in ("blackout", "reduce_only"):
        blocked = far.model_copy(
            update={
                "regime": far.regime.model_copy(
                    update={"event_regime": event_regime}
                )
            }
        )
        intent = PortfolioIntentBuilder().build(
            [near, blocked],
            strategy_release_id=RELEASE_ID,
            decision_version=2,
            now=datetime(2026, 8, 21, 8, 1, tzinfo=timezone.utc),
        )
        assert intent is None


def test_expired_horizons_cannot_create_a_new_portfolio_intent():
    scalping = _forecast("scalping", "up", 0.01)
    mid_short = _forecast("mid_short", "up", 0.009)
    intent = PortfolioIntentBuilder().build(
        [scalping, mid_short],
        strategy_release_id=RELEASE_ID,
        decision_version=3,
        now=datetime(2026, 8, 21, 8, 20, tzinfo=timezone.utc),
    )
    assert intent is None


def test_strategy_release_loader_hashes_manifest_and_every_artifact():
    artifact_names = (
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
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        hashes = {}
        paths = {}
        for index, name in enumerate(artifact_names):
            path = root / f"artifact-{index}.bin"
            content = f"immutable-{name}".encode()
            path.write_bytes(content)
            hashes[name] = hashlib.sha256(content).hexdigest()
            paths[name] = path.name
        payload = {
            "strategy_release_id": RELEASE_ID,
            "release_stage": "candidate",
            "created_at": "2026-08-21T07:00:00Z",
            "code_commit": "1234567",
            "artifacts": hashes,
            "artifact_paths": paths,
            "immutable_limits": {"max_daily_loss_pct": 0.02},
            "approval_id": "approval-release-test",
            "approved_by": "quant-risk-reviewer",
        }
        payload["bundle_sha256"] = canonical_bundle_hash(payload)
        manifest = root / "strategy-release.json"
        manifest.write_text(json.dumps(payload), encoding="utf-8")

        loaded = StrategyReleaseLoader.load(manifest)
        assert loaded.strategy_release_id == RELEASE_ID
        assert StrategyReleaseLoader.effective_limits(
            loaded, {"max_daily_loss_pct": 0.01}
        )["max_daily_loss_pct"] == 0.01

        (root / "artifact-0.bin").write_bytes(b"tampered")
        try:
            StrategyReleaseLoader.load(manifest)
        except StrategyReleaseVerificationError as exc:
            assert "hash mismatch" in str(exc)
        else:
            raise AssertionError("tampered strategy artifact was accepted")
