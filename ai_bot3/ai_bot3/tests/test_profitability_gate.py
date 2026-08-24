from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_gate import (
    ProfitabilityThresholds,
    _concentration,
    evaluate_development_gate,
    evaluate_profitability_gate,
    write_profitability_report,
)
from core.evaluation.profitability_rebuild import (
    _precommitted_statistical_trial_count,
    _require_precommitted_horizon_gates,
    write_failed_outputs,
)
from core.evaluation.statistical_governance import TrialLedger
from core.release.profitability_release import (
    REQUIRED_EVIDENCE_REPORTS,
    create_candidate_manifest,
    verify_candidate_authorization,
)
from core.risk.capital_preservation import CapitalState, TradeProposal, evaluate_trade_proposal


def _profitable_trades() -> list[dict[str, object]]:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    months = ("2026-01", "2026-02", "2026-03")
    regimes = ("normal", "high_volatility", "risk_off")
    started_at = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    output = []
    for index in range(120):
        value = -0.001 if index % 5 == 0 else 0.002
        output.append(
            {
                "net_return": value,
                "net_pnl": value * 1_000,
                "symbol": symbols[index % 3],
                "month": months[(index // 3) % 3],
                "regime": regimes[(index // 9) % 3],
                "exit_at": started_at + timedelta(days=index),
            }
        )
    return output


def _statistical_evidence() -> dict[str, object]:
    return {
        "complete": True,
        "deflated_sharpe_probability": 0.97,
        "probability_of_backtest_overfitting": 0.01,
        "number_of_trials": 25,
        "strategy_count": 4,
        "combination_count": 70,
        "return_unit": "utc_calendar_day_portfolio_net_return",
    }


def _calibration_evidence() -> dict[str, object]:
    return {
        "status": "PASSED",
        "passed": True,
        "complete": True,
        "failed_group_count": 0,
        "record_count": 500,
        "unique_decision_timestamp_count": 100,
        "method": "outer_oos_fixture",
    }


def _release_evidence_fixture(name: str) -> dict[str, object]:
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
    return {"report": name}


def test_profitability_gate_passes_only_complete_stable_evidence():
    gate = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": value} for value in (0.01, 0.02, -0.001, 0.015, 0.005)],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )
    assert gate.profitability_gate == "PASSED"
    assert gate.stage == "candidate"
    assert gate.candidate_count == 1 and gate.live_count == 0
    assert gate.checks["independent_return_clusters"]["actual"] == 120
    assert gate.checks["minimum_trades"] == {
        "passed": True,
        "actual": 120,
        "threshold": 100,
        "scope": "portfolio",
    }
    assert gate.checks["fee_adjusted_win_rate"]["actual"] == pytest.approx(0.8)
    assert gate.checks["bootstrap_lower_expectancy"]["unit"] == (
        "utc_calendar_day_portfolio_net_return"
    )


def test_profitability_thresholds_cannot_be_relaxed():
    unsafe = (
        ("minimum_net_return", -0.0001),
        ("minimum_profit_factor", 1.1999),
        ("minimum_fee_adjusted_win_rate", 0.5199),
        ("minimum_deflated_sharpe_probability", 0.9499),
        ("maximum_cscv_pbo", 0.0501),
        ("maximum_drawdown", 0.030001),
        ("minimum_bootstrap_expectancy", -0.0001),
        ("minimum_two_x_cost_net_return", -0.000001),
        ("minimum_positive_fold_ratio", 0.5999),
        ("maximum_concentration_share", 0.5001),
        ("minimum_portfolio_trades", 99),
        ("minimum_horizon_trades", 29),
        ("minimum_independent_return_clusters", 19),
        ("bootstrap_samples", 1999),
    )
    for field, value in unsafe:
        with pytest.raises(ValueError):
            ProfitabilityThresholds(**{field: value})


def test_statistical_trial_count_includes_every_factor_arm_and_prior_pipeline():
    audit = _precommitted_statistical_trial_count(2, 1)

    assert audit["final_model_variant_count"] == 10
    assert audit["ablation_horizon_arm_count"] == 90
    assert audit["current_pipeline_variant_count"] == 190
    assert audit["number_of_trials"] == 380


def test_profitability_gate_defaults_missing_release_evidence_to_failed():
    gate = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": value} for value in (0.01, 0.02, -0.001, 0.015, 0.005)],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
    )

    assert gate.profitability_gate == "FAILED"
    assert "execution_evidence" in gate.blockers
    assert "factor_ablation" in gate.blockers
    assert "deflated_sharpe_ratio" in gate.blockers
    assert "cscv_probability_of_backtest_overfitting" in gate.blockers


def test_profitability_gate_enforces_net_win_rate_and_nonnegative_cost_stress():
    trades = _profitable_trades()
    for index, trade in enumerate(trades):
        trade["net_pnl"] = 7.0 if index % 2 == 0 else -1.0
    gate = evaluate_profitability_gate(
        trades,
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=-0.000001,
        mark_to_market_max_drawdown=0.001,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )

    assert gate.profitability_gate == "FAILED"
    assert gate.checks["fee_adjusted_win_rate"]["actual"] == pytest.approx(0.50)
    assert "fee_adjusted_win_rate" in gate.blockers
    assert "two_x_cost_stress" in gate.blockers


def test_horizon_scope_requires_30_trades_while_portfolio_requires_100():
    trades = _profitable_trades()[:30]
    horizon = evaluate_profitability_gate(
        trades,
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.001,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
        gate_scope="horizon",
    )
    portfolio = evaluate_profitability_gate(
        trades,
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.001,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )

    assert horizon.checks["minimum_trades"]["passed"] is True
    assert horizon.checks["minimum_trades"]["threshold"] == 30
    assert portfolio.checks["minimum_trades"]["passed"] is False
    assert portfolio.checks["minimum_trades"]["threshold"] == 100


def test_portfolio_profit_cannot_authorize_a_failed_precommitted_horizon():
    portfolio = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": value} for value in (0.01, 0.02, -0.001, 0.015, 0.005)],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )
    failed_horizon = evaluate_profitability_gate(
        [],
        [{"net_return": -0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=-0.01,
        mark_to_market_max_drawdown=0.0,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=False,
        factor_ablation_complete=True,
    )

    scoped = _require_precommitted_horizon_gates(
        portfolio, {180: portfolio, 900: failed_horizon}
    )

    assert portfolio.passed is True
    assert scoped.passed is False
    assert scoped.candidate_count == 0
    assert "precommitted_horizon_profitability" in scoped.blockers
    assert scoped.checks["precommitted_horizon_profitability"][
        "passed_horizons"
    ] == [180]


def test_profitability_gate_does_not_treat_correlated_trades_as_independent():
    trades = _profitable_trades()
    same_day = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    for trade in trades:
        trade["exit_at"] = same_day
    gate = evaluate_profitability_gate(
        trades,
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )
    assert gate.profitability_gate == "FAILED"
    assert "independent_return_clusters" in gate.blockers
    assert "bootstrap_lower_expectancy" in gate.blockers
    assert gate.checks["independent_return_clusters"]["actual"] == 1


def test_sparse_calendar_gaps_do_not_count_as_independent_trading_evidence():
    trades = _profitable_trades()[:30]
    active_days = (
        datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
        datetime(2026, 2, 1, 12, tzinfo=timezone.utc),
        datetime(2026, 3, 1, 12, tzinfo=timezone.utc),
    )
    for index, trade in enumerate(trades):
        trade["exit_at"] = active_days[index % len(active_days)]
    gate = evaluate_profitability_gate(
        trades,
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
    )

    assert gate.profitability_gate == "FAILED"
    assert gate.checks["independent_return_clusters"]["actual"] == 3
    evidence = gate.checks["independent_return_clusters"]["evidence"]
    assert evidence["cluster_count"] == 60
    assert evidence["active_cluster_count"] == 3


def test_return_concentration_uses_net_group_contribution():
    trades = [
        {"symbol": "A", "net_pnl": 100.0},
        {"symbol": "A", "net_pnl": -100.0},
        {"symbol": "B", "net_pnl": 60.0},
        {"symbol": "C", "net_pnl": 40.0},
    ]

    share, group = _concentration(trades, "symbol")

    assert group == "B"
    assert share == pytest.approx(0.60)


def test_profitability_gate_fails_closed_without_utc_trade_timestamps():
    trades = _profitable_trades()
    for trade in trades:
        trade.pop("exit_at")
    gate = evaluate_profitability_gate(
        trades,
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
    )
    assert gate.profitability_gate == "FAILED"
    assert gate.checks["independent_return_clusters"]["evidence"]["reason"] == (
        "missing_or_non_utc_trade_timestamps"
    )


def test_failed_gate_has_zero_candidates_and_cannot_create_manifest(tmp_path):
    gate = evaluate_profitability_gate(
        [],
        [],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=-0.02,
        execution_evidence_complete=False,
        factor_ablation_complete=False,
    )
    assert gate.profitability_gate == "FAILED"
    assert gate.stage == "rejected"
    assert gate.candidate_count == 0 and gate.live_count == 0
    report = tmp_path / "profitability_report.json"
    model = tmp_path / "model.json"
    model.write_text("{}", encoding="utf-8")
    write_profitability_report(report, gate)
    with pytest.raises(ValueError, match="forbidden"):
        create_candidate_manifest(
            tmp_path / "candidate_release_manifest.json",
            gate=gate,
            profitability_report_path=report,
            model_artifact_path=model,
            lockbox_fingerprint="a" * 64,
            code_commit="1234567",
        )


def test_candidate_manifest_binds_every_final_evidence_report(tmp_path):
    gate = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": value} for value in (0.01, 0.02, -0.001, 0.015, 0.005)],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )
    profitability = tmp_path / "profitability_report.json"
    model = tmp_path / "model.json"
    write_profitability_report(profitability, gate)
    model.write_text("{}", encoding="utf-8")
    evidence = {}
    for name in REQUIRED_EVIDENCE_REPORTS:
        path = tmp_path / name
        path.write_text(
            json.dumps(_release_evidence_fixture(name), sort_keys=True),
            encoding="utf-8",
        )
        evidence[name] = path
    manifest = tmp_path / "candidate_release_manifest.json"
    create_candidate_manifest(
        manifest,
        gate=gate,
        profitability_report_path=profitability,
        model_artifact_path=model,
        lockbox_fingerprint="d" * 64,
        code_commit="1234567",
        evidence_report_paths=evidence,
    )
    assert verify_candidate_authorization(profitability, manifest) == (
        True,
        "verified_profitability_candidate",
    )

    evidence["execution_cost_report.json"].write_text(
        '{"tampered":true}', encoding="utf-8"
    )
    authorized, reason = verify_candidate_authorization(profitability, manifest)
    assert authorized is False
    assert reason == "profitability_evidence_hash_mismatch:execution_cost_report.json"


def test_candidate_manifest_release_id_is_derived_from_bound_evidence(tmp_path):
    gate = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": value} for value in (0.01, 0.02, -0.001, 0.015, 0.005)],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )
    profitability = tmp_path / "profitability_report.json"
    model = tmp_path / "model.json"
    write_profitability_report(profitability, gate)
    model.write_text("{}", encoding="utf-8")
    evidence = {}
    for name in REQUIRED_EVIDENCE_REPORTS:
        evidence_path = tmp_path / name
        evidence_path.write_text(
            json.dumps(_release_evidence_fixture(name), sort_keys=True),
            encoding="utf-8",
        )
        evidence[name] = evidence_path
    manifest_path = tmp_path / "candidate_release_manifest.json"
    create_candidate_manifest(
        manifest_path,
        gate=gate,
        profitability_report_path=profitability,
        model_artifact_path=model,
        lockbox_fingerprint="e" * 64,
        code_commit="1234567",
        evidence_report_paths=evidence,
    )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["release_id"] = "pr_tampered_identity"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    assert verify_candidate_authorization(profitability, manifest_path) == (
        False,
        "profitability_release_id_mismatch",
    )


def test_profitability_gate_rejects_realized_only_or_excess_mtm_drawdown():
    missing = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
    )
    assert missing.profitability_gate == "FAILED"
    assert "mark_to_market_drawdown" in missing.blockers

    excessive = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": 0.01}] * 5,
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.031,
        mark_to_market_evidence_complete=True,
    )
    assert excessive.profitability_gate == "FAILED"
    assert excessive.checks["mark_to_market_drawdown"]["actual"] == 0.031


def test_development_gate_can_pass_without_creating_a_candidate_or_opening_lockbox():
    development = evaluate_development_gate(
        _profitable_trades(),
        [{"net_return": value} for value in (0.01, 0.02, -0.001, 0.015, 0.005)],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
        execution_evidence_complete=True,
        factor_ablation_complete=True,
        statistical_overfit_evidence=_statistical_evidence(),
        calibration_coverage_evidence=_calibration_evidence(),
    )
    assert development.profitability_gate == "PASSED"
    assert development.stage == "development_validated"
    assert development.candidate_count == 0 and development.live_count == 0
    assert "development_oos_net_return" in development.checks
    assert "lockbox_net_return" not in development.checks


def test_pipeline_failure_archives_stale_candidate_manifest(tmp_path):
    stale = tmp_path / "candidate_release_manifest.json"
    stale.write_text('{"stage":"candidate"}', encoding="utf-8")
    result = write_failed_outputs(tmp_path, reason="synthetic pipeline fault")
    assert result.profitability_gate == "FAILED"
    assert result.candidate_count == 0 and result.live_count == 0
    assert not stale.exists()
    archived = list((tmp_path / "archive").glob("candidate_release_manifest.pipeline_failed*.json"))
    assert len(archived) == 1
    assert archived[0].read_text(encoding="utf-8") == '{"stage":"candidate"}'


def test_lockbox_cannot_be_reused_by_another_trial(tmp_path):
    ledger = TrialLedger(tmp_path / "trials.sqlite3")
    fingerprint = "b" * 64
    assert ledger.claim_lockbox(fingerprint, "trial-one") is True
    assert ledger.claim_lockbox(fingerprint, "trial-one") is False
    with pytest.raises(ValueError, match="already consumed"):
        ledger.claim_lockbox(fingerprint, "trial-two")
    with pytest.raises(ValueError, match="trial already claimed"):
        ledger.claim_lockbox("c" * 64, "trial-one")
    assert ledger.lockbox_claim_count() == 1


def test_capital_preservation_rejects_missing_stop_and_martingale():
    decision = evaluate_trade_proposal(
        CapitalState(
            equity=99_000,
            peak_equity=100_000,
            daily_pnl=-100,
            weekly_pnl=-100,
            gross_exposure=0.1,
            previous_trade_risk=0.001,
            previous_trade_was_loss=True,
        ),
        TradeProposal(
            symbol="BTCUSDT",
            risk_fraction=0.002,
            leverage=1.0,
            target_exposure=0.1,
            stop_price=None,
            lower_bound_net_edge=0.0,
        ),
    )
    assert decision.allowed is False
    assert "missing_stop" in decision.reasons
    assert "non_positive_lower_bound_net_edge" in decision.reasons
    assert "martingale_forbidden" in decision.reasons
