from __future__ import annotations

import asyncio
import hashlib
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
from core.evaluation.profitability_gate import ProfitabilityGateResult, write_profitability_report
from core.release.profitability_release import (
    REQUIRED_EVIDENCE_REPORTS,
    create_candidate_manifest,
)
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
        "release_stage": "candidate",
        "created_at": (now - timedelta(hours=1)).isoformat(),
        "code_commit": "shadow-e2e-commit",
        "artifacts": {key: "0" * 64 for key in ARTIFACT_KEYS},
        "approval_id": "shadow-e2e-approval",
        "approved_by": "automated-shadow-regression",
    }
    payload["bundle_sha256"] = canonical_bundle_hash(payload)
    return StrategyReleaseBundle.model_validate(payload)


def prediction(
    now: datetime,
    *,
    mode: str,
    expected_return: float,
    alpha_prediction: dict,
) -> dict:
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
        "alpha_prediction": alpha_prediction,
        "brain_prediction": {
            "version": f"brain-shadow-e2e-{mode}",
            "status": "rejected_baseline",
            "direction": "long",
            "actionable": False,
            "release_stage": "rejected",
            "strategy_release_id": RELEASE_ID,
            "confidence": 0.9,
            "expected_return": expected_return,
        },
    }


def shadow_authenticity_evidence(
    name: str, *, model_sha256: str
) -> dict[str, object]:
    """Build semantically complete, explicitly non-research E2E fixtures.

    These short-lived files exercise release verification and the real shadow
    transport boundary.  They are never written to the repository, trial
    ledger, model registry, or a live environment and are not profitability
    evidence.
    """

    common = {
        "fixture_scope": "shadow_authenticity_only",
        "not_profitability_evidence": True,
    }
    if name == "walk_forward_report.json":
        horizons = ("180", "900", "7200", "14400", "86400")
        return {
            **common,
            "folds": [{"fold_id": "shadow-authenticity-fixture"}],
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
            **common,
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
            **common,
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
            **common,
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
            **common,
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
            **common,
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
            **common,
            "status": "PASSED",
            "complete": True,
            "expected_series_count": 25,
            "passed_series_count": 25,
        }
    if name == "missing_intervals_report.json":
        return {
            **common,
            "status": "PASSED",
            "complete": True,
            "total_discontinuity_count": 0,
        }
    if name == "independent_timestamp_count_report.json":
        return {
            **common,
            "status": "PASSED",
            "raw_source_complete": True,
            "outer_oos_complete": True,
        }
    if name == "calibration_coverage_report.json":
        return {
            **common,
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
            **common,
            "status": "PASSED",
            "complete": True,
            "outer_oos_used_for_tuning": False,
        }
    if name == "signal_funnel_report.json":
        scope = {
            "status": "PASSED",
            "zero_signal_or_trade_result_accepted": False,
        }
        return {
            **common,
            "status": "PASSED",
            "complete": True,
            "development": scope,
            "lockbox": scope,
        }
    if name == "intratrade_drawdown_report.json":
        scope = {
            "status": "PASSED",
            "mark_to_market_used": True,
            "equity_observation_count": 10,
        }
        return {
            **common,
            "status": "PASSED",
            "complete": True,
            "development": scope,
            "lockbox": scope,
        }
    if name == "production_replay_report.json":
        return {
            **common,
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
    return {**common, "report": name}


def publish_release_gated_ticket(temp: Path, control_db: Path, now: datetime) -> dict:
    # This is an authenticity fixture, not profitability evidence.  It exercises
    # the complete candidate-gated production path without opening live trading.
    gate = ProfitabilityGateResult(
        profitability_gate="PASSED",
        stage="candidate",
        candidate_count=1,
        live_count=0,
        checks={"shadow_e2e_fixture": {"passed": True}},
        metrics={"net_return": 0.01},
        blockers=(),
    )
    report_path = temp / "profitability_report.json"
    model_artifact_path = temp / "two-stage-model.json"
    manifest_path = temp / "candidate_release_manifest.json"
    model_artifact_path.write_text(
        '{"fixture":true,"release_stage":"candidate"}', encoding="utf-8"
    )
    write_profitability_report(report_path, gate)
    model_sha256 = hashlib.sha256(model_artifact_path.read_bytes()).hexdigest()
    evidence_report_paths: dict[str, Path] = {}
    for name in REQUIRED_EVIDENCE_REPORTS:
        evidence_path = temp / name
        evidence_path.write_text(
            json.dumps(
                shadow_authenticity_evidence(name, model_sha256=model_sha256),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        evidence_report_paths[name] = evidence_path
    manifest = create_candidate_manifest(
        manifest_path,
        gate=gate,
        profitability_report_path=report_path,
        model_artifact_path=model_artifact_path,
        lockbox_fingerprint="c" * 64,
        code_commit="shadow-e2e-commit",
        evidence_report_paths=evidence_report_paths,
    ).to_dict()
    def alpha_prediction_for(horizon_sec: int) -> dict:
        return {
            "model_family": "profitability_two_stage",
            "model_bundle_id": "profitability-two-stage-shadow-e2e",
            "strategy_release_id": RELEASE_ID,
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
                    "candidate_freshness_verified": True,
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
    manager = ResultManager(
        temp / "results",
        control_plane_db=control_db,
        tickets_enabled=True,
        required_brain_release_stage="candidate",
        strategy_release_bundle=release_bundle(now),
        profitability_report_path=report_path,
        candidate_release_manifest_path=manifest_path,
    )
    # One horizon is deliberately insufficient.  The second prediction must pass
    # the real release gate, SignalBook and PortfolioIntent path before a ticket
    # becomes visible to the execution service.
    asyncio.run(
        manager.save_result(
            "BTCUSDT",
            "scalping",
            prediction(
                now,
                mode="scalping",
                expected_return=0.010,
                alpha_prediction=alpha_prediction_for(180),
            ),
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
            "BTCUSDT",
            "mid_short",
            prediction(
                now,
                mode="mid_short",
                expected_return=0.009,
                alpha_prediction=alpha_prediction_for(900),
            ),
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
        existing_pythonpath = environment.get("PYTHONPATH", "")
        environment.update(
            {
                "CONTROL_PLANE_DB": str(control_db),
                "RESEARCH_JOB_DB": str(temp / "research.sqlite3"),
                "BYBIT_TRADING_MODE": "shadow",
                "BYBIT_ENABLE_LIVE": "false",
            }
        )
        environment["PYTHONPATH"] = str(AI_ROOT) + (
            os.pathsep + existing_pythonpath if existing_pythonpath else ""
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
                "ResultManager -> verified profitability candidate -> ForecastEnvelope[2] -> "
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
