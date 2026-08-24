from __future__ import annotations

import asyncio
import hashlib
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
from contracts.horizons import MAX_CANDIDATE_KLINE_AGE_SEC
from contracts.strategy_release_v1 import StrategyReleaseBundle
from core.evaluation.profitability_gate import ProfitabilityGateResult, write_profitability_report
from core.release.profitability_release import (
    REQUIRED_EVIDENCE_REPORTS,
    create_candidate_manifest,
)
from core.release.strategy_bundle import canonical_bundle_hash


def _iso(point: datetime) -> str:
    return point.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _release_evidence_fixture(
    name: str, model_sha256: str = "f" * 64
) -> dict[str, object]:
    if name == "walk_forward_report.json":
        horizons = ("180", "900", "7200", "14400", "86400")
        return {
            "folds": [{"fold_id": "fixture"}],
            "outer_oos_used_for_tuning": False,
            "development_eligible_horizons": [int(value) for value in horizons],
            "direct_execution_release_datasets": {
                value: {"release_walk_forward_ready": True} for value in horizons
            },
            "positive_fold_ratio": 0.60,
        }
    if name == "lockbox_report.json":
        horizons = ("180", "900", "7200", "14400", "86400")
        return {
            "status": "EVALUATED_ONCE",
            "used_for_parameter_selection": False,
            "lockbox_labels_materialized": True,
            "lockbox_fingerprint": "c" * 64,
            "development_eligible_horizons": [int(value) for value in horizons],
            "result": {"trades": [{} for _ in range(100)]},
            "horizon_results": {value: {"gate": "fixture"} for value in horizons},
        }
    if name == "factor_ablation_report.json":
        definitions = {
            "legacy_brain_technical": (180, 900, 7200, 14400, 86400),
            "bybit_orderbook": (180, 900),
            "public_trades": (180, 900),
            "basis_funding_oi": (180, 900),
            "liquidations": (180, 900),
            "execution_quality": (180, 900),
            "us_risk": (7200, 14400, 86400),
            "rates_usd": (7200, 14400, 86400),
            "commodities": (7200, 14400, 86400),
            "healthcare": (7200, 14400, 86400),
            "china": (7200, 14400, 86400),
            "crypto_equities": (7200, 14400, 86400),
            "stablecoin_flows": (7200, 14400, 86400),
            "fund_flows": (7200, 14400, 86400),
            "macro_vintage": (7200, 14400, 86400),
            "tier_a_events": (7200, 14400, 86400),
        }
        return {
            "all_required_groups_evaluated": True,
            "groups": [
                {
                    "factor_group": group,
                    "oos_ablation_status": "EVALUATED_OOS",
                    "all_applicable_horizons_evaluated": True,
                    "applicable_horizons": list(horizons),
                    "horizon_results": {
                        str(horizon): {"oos_ablation_status": "EVALUATED_OOS"}
                        for horizon in horizons
                    },
                }
                for group, horizons in definitions.items()
            ],
        }
    if name == "execution_cost_report.json":
        return {
            "evaluation_scope": "lockbox",
            "execution_evidence_complete": True,
            "candidate_backtest_execution_evidence_complete": True,
            "execution_evidence": {
                "official_pit_cost_inputs_complete": True,
                "simulation_complete": True,
                "risk_policy_compliant": True,
                "candidate_backtest_execution_evidence_complete": True,
                "proxy_execution_cost_trade_count": 0,
                "direct_execution_cost_trade_count": 100,
            },
            "normal_cost": {"mark_to_market_used": True},
            "two_x_cost": {"net_return": 0.0},
        }
    if name == "capital_preservation_report.json":
        return {
            "fail_closed": True,
            "no_averaging_down": True,
            "no_martingale": True,
            "no_trade_without_stop": True,
            "no_trade_when_lower_bound_net_edge_lte_zero": True,
            "policy": {
                "risk_per_trade": 0.0025,
                "daily_loss_limit": 0.005,
                "weekly_loss_limit": 0.015,
                "equity_drawdown_limit": 0.03,
                "leverage_cap": 2.0,
            },
        }
    if name == "statistical_overfit_report.json":
        horizons = ("180", "900", "7200", "14400", "86400")
        evidence = {
            "complete": True,
            "deflated_sharpe_probability": 0.95,
            "probability_of_backtest_overfitting": 0.05,
        }
        return {
            "development_eligible_horizons": [int(value) for value in horizons],
            "development": {
                "portfolio": evidence,
                "horizons": {value: evidence for value in horizons},
            },
            "lockbox": {
                "portfolio": evidence,
                "horizons": {value: evidence for value in horizons},
                "alternative_variants_scored_on_lockbox": False,
            },
        }

    if name == "data_coverage_report.json":
        return {
            "status": "PASSED",
            "complete": True,
            "expected_series_count": 25,
            "passed_series_count": 25,
        }
    if name == "missing_intervals_report.json":
        return {
            "status": "PASSED",
            "complete": True,
            "total_discontinuity_count": 0,
        }
    if name == "independent_timestamp_count_report.json":
        return {
            "status": "PASSED",
            "raw_source_complete": True,
            "outer_oos_complete": True,
        }
    if name == "calibration_coverage_report.json":
        return {
            "status": "PASSED",
            "complete": True,
            "development": {"portfolio": {"passed": True}},
            "lockbox": {
                "portfolio": {"passed": True},
                "used_for_calibration_or_tuning": False,
                "alternative_models_scored": False,
            },
        }
    if name == "nested_cv_report.json":
        return {
            "status": "PASSED",
            "complete": True,
            "outer_oos_used_for_tuning": False,
        }
    if name == "signal_funnel_report.json":
        return {
            "status": "PASSED",
            "complete": True,
            "development": {
                "status": "PASSED",
                "zero_signal_or_trade_result_accepted": False,
            },
            "lockbox": {
                "status": "PASSED",
                "zero_signal_or_trade_result_accepted": False,
            },
        }
    if name == "intratrade_drawdown_report.json":
        scope = {
            "status": "PASSED",
            "mark_to_market_used": True,
            "equity_observation_count": 10,
        }
        return {
            "status": "PASSED",
            "complete": True,
            "development": scope,
            "lockbox": scope,
        }
    if name == "production_replay_report.json":
        return {
            "status": "PASSED",
            "complete": True,
            "lockbox_used": False,
            "alternative_models_scored": False,
            "expected_sample_count": 25,
            "observed_sample_count": 25,
            "failed_sample_count": 0,
            "final_bundle_models_match_replayed": True,
            "final_model_bundle_sha256": model_sha256,
        }
    return {"fixture": name}


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


def _authorized_alpha(
    manifest: dict,
    release_id: str = "sr_result_gate_test_001",
    *,
    horizon_sec: int,
) -> dict:
    last_observed_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    return {
        "model_family": "profitability_two_stage",
        "model_bundle_id": "profitability-two-stage-test",
        "strategy_release_id": release_id,
        "release_id": manifest["release_id"],
        "model_artifact_sha256": manifest["model_artifact_sha256"],
        "lockbox_fingerprint": manifest["lockbox_fingerprint"],
        "profitability_gate": "PASSED",
        "release_stage": "candidate",
        "horizon_sec": horizon_sec,
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
        "range_guard_score": 0.1,
        "range_guard_details": {
            "method": "standardized_3_5_sigma",
            "violation_fraction": 0.0,
            "maximum_excess": 0.0,
        },
        "market_regime": "risk_on",
        "feature_evidence": {
            "price_path": {
                "status": "verified",
                "training_kline_source": "bybit",
                "runtime_price_source": "bybit_linear_last_trade_kline",
                "same_venue": True,
                "continuous": True,
                "ohlcv_contract_valid": True,
                "observed_bar_count": 100,
                "interval_sec": horizon_sec,
                "first_observed_at": _iso(
                    last_observed_at - timedelta(seconds=99 * horizon_sec)
                ),
                "last_observed_at": _iso(last_observed_at),
                "last_price": 100_000.0,
                "candidate_freshness_verified": True,
                "age_seconds": 5.0,
                "maximum_age_seconds": float(
                    MAX_CANDIDATE_KLINE_AGE_SEC[horizon_sec]
                ),
            }
        },
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
    model_sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()
    for name in REQUIRED_EVIDENCE_REPORTS:
        evidence_path = root / name
        evidence_path.write_text(
            json.dumps(
                _release_evidence_fixture(name, model_sha256), sort_keys=True
            ),
            encoding="utf-8",
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
        near["alpha_prediction"] = _authorized_alpha(manifest, horizon_sec=180)
        near["current_price"] = 1.0
        near["data_source_status"] = "error"
        near["data_source_reliable"] = False
        near["context_completeness"] = {"score": 0.0}
        near["calibration_status"] = "invalid"
        near["range_guard_score"] = 1.0
        far = _prediction("rejected")
        far["alpha_prediction"] = _authorized_alpha(manifest, horizon_sec=900)
        far["current_price"] = 1.0
        far["data_source_status"] = "error"
        far["data_source_reliable"] = False
        far["context_completeness"] = {"score": 0.0}
        far["calibration_status"] = "invalid"
        far["range_guard_score"] = 1.0
        asyncio.run(manager.save_result("BTCUSDT", "scalping", near))
        assert _counts(db) == (1, 0)
        asyncio.run(manager.save_result("BTCUSDT", "mid_short", far))
        assert _counts(db) == (2, 1)
        active = manager.control_plane.active_forecasts(
            "BTCUSDT", strategy_release_id=manager.strategy_release_bundle.strategy_release_id
        )
        assert len(active) == 2
        assert all(item.instrument.exchange == "bybit" for item in active)
        assert all(item.quality.source_status == "ok" for item in active)
        assert all(item.quality.data_quality == 1.0 for item in active)
        assert all(item.quality.calibration_status == "valid" for item in active)
        assert all(item.quality.range_guard_score == 0.1 for item in active)
        tickets, _ = manager.control_plane.list_tickets()
        assert len(tickets) == 1
        assert tickets[0].entry is not None
        assert tickets[0].entry.reference_price == 100_000.0
        assert tickets[0].guards.forecast_market == "bybit"


def test_candidate_ticket_rejects_malformed_edge_or_runtime_price_contract():
    for defect in (
        "wrong_horizon",
        "infinite_horizon",
        "unverified_grid",
        "nan_edge",
        "invalid_bar_count",
        "infinite_interval",
        "invalid_generated_at",
        "future_observation",
        "delayed_result",
        "broken_observation_span",
        "nonfinite_last_price",
        "invalid_range_guard",
        "excessive_range_guard",
        "stale_age",
        "inflated_maximum_age",
    ):
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
            near["alpha_prediction"] = _authorized_alpha(
                manifest, horizon_sec=180
            )
            far = _prediction("rejected")
            far_alpha = _authorized_alpha(
                manifest,
                horizon_sec=180 if defect == "wrong_horizon" else 900,
            )
            if defect == "infinite_horizon":
                far_alpha["horizon_sec"] = float("inf")
            elif defect == "unverified_grid":
                far_alpha["feature_evidence"]["price_path"]["continuous"] = False
            elif defect == "nan_edge":
                far_alpha["lower_bound_net_edge_bps"] = float("nan")
            elif defect == "invalid_bar_count":
                far_alpha["feature_evidence"]["price_path"][
                    "observed_bar_count"
                ] = {"not": "an integer"}
            elif defect == "infinite_interval":
                far_alpha["feature_evidence"]["price_path"]["interval_sec"] = float(
                    "inf"
                )
            elif defect == "invalid_generated_at":
                far["generated_at"] = "not-a-timestamp"
            elif defect == "future_observation":
                far_alpha["feature_evidence"]["price_path"][
                    "last_observed_at"
                ] = _iso(datetime.now(timezone.utc) + timedelta(days=1))
            elif defect == "delayed_result":
                last_observed_at = datetime.now(timezone.utc) - timedelta(
                    minutes=5
                )
                price_path = far_alpha["feature_evidence"]["price_path"]
                price_path["last_observed_at"] = _iso(last_observed_at)
                price_path["first_observed_at"] = _iso(
                    last_observed_at - timedelta(seconds=99 * 900)
                )
                # This is the age captured before an artificial queue delay.
                # Authorization must use saved_at - last_observed_at instead.
                price_path["age_seconds"] = 5.0
            elif defect == "broken_observation_span":
                far_alpha["feature_evidence"]["price_path"][
                    "first_observed_at"
                ] = far_alpha["feature_evidence"]["price_path"]["last_observed_at"]
            elif defect == "nonfinite_last_price":
                far_alpha["feature_evidence"]["price_path"]["last_price"] = float(
                    "inf"
                )
            elif defect == "invalid_range_guard":
                far_alpha["range_guard_score"] = float("nan")
            elif defect == "excessive_range_guard":
                far_alpha["range_guard_score"] = 0.36
            elif defect == "stale_age":
                far_alpha["feature_evidence"]["price_path"]["age_seconds"] = 2_701
            elif defect == "inflated_maximum_age":
                far_alpha["feature_evidence"]["price_path"][
                    "maximum_age_seconds"
                ] = 999_999
            far["alpha_prediction"] = far_alpha

            asyncio.run(manager.save_result("BTCUSDT", "scalping", near))
            asyncio.run(manager.save_result("BTCUSDT", "mid_short", far))

            assert _counts(db) == (2, 0)
            active = manager.control_plane.active_forecasts(
                "BTCUSDT",
                strategy_release_id=manager.strategy_release_bundle.strategy_release_id,
            )
            assert len(active) == 1


def test_candidate_ticket_revalidates_release_evidence_after_startup():
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
        assert manager.profitability_authorized is True

        changed = json.loads(report.read_text(encoding="utf-8"))
        changed["candidate_count"] = 0
        report.write_text(json.dumps(changed), encoding="utf-8")

        near = _prediction("rejected")
        near["alpha_prediction"] = _authorized_alpha(manifest, horizon_sec=180)
        far = _prediction("rejected")
        far["alpha_prediction"] = _authorized_alpha(manifest, horizon_sec=900)
        asyncio.run(manager.save_result("BTCUSDT", "scalping", near))
        asyncio.run(manager.save_result("BTCUSDT", "mid_short", far))

        assert _counts(db) == (2, 0)
        assert manager.profitability_authorized is False
        assert (
            manager.profitability_authorization_reason
            == "profitability_candidate_counts_invalid"
        )
        assert manager.control_plane.active_forecasts(
            "BTCUSDT",
            strategy_release_id=manager.strategy_release_bundle.strategy_release_id,
        ) == []


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
