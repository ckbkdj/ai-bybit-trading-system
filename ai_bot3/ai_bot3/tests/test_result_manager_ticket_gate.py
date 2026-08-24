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
from core.evaluation.profitability_gate import ProfitabilityGateResult, write_profitability_report
from core.release.profitability_release import create_candidate_manifest
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


def _authorized_alpha(manifest: dict, release_id: str = "sr_result_gate_test_001") -> dict:
    return {
        "model_family": "profitability_two_stage",
        "model_bundle_id": "profitability-two-stage-test",
        "strategy_release_id": release_id,
        "release_id": manifest["release_id"],
        "model_artifact_sha256": manifest["model_artifact_sha256"],
        "lockbox_fingerprint": manifest["lockbox_fingerprint"],
        "profitability_gate": "PASSED",
        "release_stage": "candidate",
        "decision": "TRADE",
        "actionable": True,
        "direction": "long",
        "p_up": 0.8,
        "p_flat": 0.1,
        "p_down": 0.1,
        "expected_net_return_bps": 70.0,
        "expected_mae_bps": 50.0,
        "expected_mfe_bps": 120.0,
        "lower_bound_net_edge_bps": 40.0,
        "return_quantiles_bps": {
            "p10": 80.0,
            "p25": 90.0,
            "p50": 100.0,
            "p75": 115.0,
            "p90": 130.0,
        },
    }


def _profitability_release(root: Path) -> tuple[Path, Path, dict]:
    gate = ProfitabilityGateResult(
        profitability_gate="PASSED",
        stage="candidate",
        candidate_count=1,
        live_count=0,
        checks={"fixture": {"passed": True}},
        metrics={"net_return": 0.01},
        blockers=(),
    )
    report = root / "profitability_report.json"
    artifact = root / "two-stage-model.json"
    artifact.write_text('{"release_stage":"rejected"}', encoding="utf-8")
    evidence_paths = {}
    for name in (
        "walk_forward_report.json",
        "lockbox_report.json",
        "factor_ablation_report.json",
        "execution_cost_report.json",
        "capital_preservation_report.json",
    ):
        evidence_path = root / name
        evidence_path.write_text(
            json.dumps({"fixture": name}, sort_keys=True), encoding="utf-8"
        )
        evidence_paths[name] = evidence_path
    write_profitability_report(report, gate)
    manifest_path = root / "candidate_release_manifest.json"
    manifest = create_candidate_manifest(
        manifest_path,
        gate=gate,
        profitability_report_path=report,
        model_artifact_path=artifact,
        lockbox_fingerprint="c" * 64,
        code_commit="1234567",
        evidence_report_paths=evidence_paths,
    ).to_dict()
    return report, manifest_path, manifest


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


def test_brain_never_authorizes_ticket_even_with_stale_live_release():
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
        assert _counts(db) == (2, 0)


def test_candidate_stage_without_profitability_evidence_fails_closed():
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
        assert _counts(db) == (2, 0)


def test_verified_profitability_release_can_create_candidate_ticket_only():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / "control.sqlite3"
        report, manifest_path, manifest = _profitability_release(root)
        manager = ResultManager(
            root / "results",
            control_plane_db=db,
            tickets_enabled=True,
            required_brain_release_stage="candidate",
            strategy_release_bundle=_bundle("candidate"),
            profitability_report_path=report,
            candidate_release_manifest_path=manifest_path,
        )
        near = _prediction("rejected")
        near["alpha_prediction"] = _authorized_alpha(manifest)
        far = _prediction("rejected")
        far["alpha_prediction"] = _authorized_alpha(manifest)
        asyncio.run(manager.save_result("BTCUSDT", "scalping", near))
        assert _counts(db) == (1, 0)
        asyncio.run(manager.save_result("BTCUSDT", "mid_short", far))
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
