from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_gate import (
    evaluate_development_gate,
    evaluate_profitability_gate,
    write_profitability_report,
)
from core.evaluation.profitability_rebuild import write_failed_outputs
from core.evaluation.statistical_governance import TrialLedger
from core.release.profitability_release import create_candidate_manifest
from core.risk.capital_preservation import CapitalState, TradeProposal, evaluate_trade_proposal


def _profitable_trades() -> list[dict[str, object]]:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    months = ("2026-01", "2026-02", "2026-03")
    regimes = ("normal", "high_volatility", "risk_off")
    started_at = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    output = []
    for index in range(60):
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


def test_profitability_gate_passes_only_complete_stable_evidence():
    gate = evaluate_profitability_gate(
        _profitable_trades(),
        [{"net_return": value} for value in (0.01, 0.02, -0.001, 0.015, 0.005)],
        initial_equity_usdt=100_000,
        two_x_cost_net_return=0.01,
        mark_to_market_max_drawdown=0.02,
        mark_to_market_evidence_complete=True,
    )
    assert gate.profitability_gate == "PASSED"
    assert gate.stage == "candidate"
    assert gate.candidate_count == 1 and gate.live_count == 0
    assert gate.checks["independent_return_clusters"]["actual"] == 60
    assert gate.checks["bootstrap_lower_expectancy"]["unit"] == (
        "utc_calendar_day_portfolio_net_return"
    )


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
    )
    assert gate.profitability_gate == "FAILED"
    assert "independent_return_clusters" in gate.blockers
    assert "bootstrap_lower_expectancy" in gate.blockers
    assert gate.checks["independent_return_clusters"]["actual"] == 1


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
