from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.release_evidence import (
    intratrade_drawdown_evidence,
    nested_cv_evidence,
    production_replay_evidence,
    signal_funnel_evidence,
)


def test_nested_cv_report_proves_inner_only_selection_and_purge():
    validation_start = datetime(2026, 1, 2, tzinfo=timezone.utc)
    inner = {
        "fold": 1,
        "train_rows": 100,
        "inner_oos_rows": 20,
        "train_decision_end": validation_start - timedelta(seconds=180),
        "train_label_available_max": validation_start - timedelta(seconds=1),
        "validation_start": validation_start,
        "purge_sec": 180,
        "embargo_sec": 45,
    }
    outer = {
        "horizon_sec": 180,
        "fold_id": "outer_01",
        "train_rows": 200,
        "test_rows": 50,
        "outer_oos_used_for_tuning": False,
        "nested_selection": {
            "selection_data": "inner_walk_forward_oos_only",
            "outer_oos_used_for_selection": False,
            "inner_fold_count": 2,
        },
        "inner_candidate_results": [
            {
                "config_id": "config-a",
                "outer_oos_rows_seen": 0,
                "inner_folds": [inner, {**inner, "fold": 2}],
            }
        ],
    }

    report = nested_cv_evidence([outer])

    assert report["status"] == "PASSED"
    assert report["outer_oos_used_for_tuning"] is False


def test_signal_funnel_rejects_zero_signal_evidence():
    gate = {
        "paired_action_rows": 200,
        "candidate_decisions": 100,
        "meta_pass_rows": 10,
        "positive_expectancy_lcb_rows": 10,
        "direction_consistent_rows": 100,
        "all_gate_pass_rows": 0,
        "selected_decisions": 0,
    }
    report = signal_funnel_evidence(
        [{"prediction_gate": gate, "signals": 0, "trades": 0}],
        SimpleNamespace(trades=(), rejected_signals={}),
        scope="development_outer_oos",
    )

    assert report["status"] == "FAILED"
    assert report["zero_signal_or_trade_result_accepted"] is False


def test_intratrade_drawdown_recomputes_the_full_mtm_curve():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    curve = (
        SimpleNamespace(
            observed_at=start,
            equity_usdt=101_000.0,
            active_positions=1,
            gross_exposure_usdt=10_000.0,
        ),
        SimpleNamespace(
            observed_at=start + timedelta(minutes=1),
            equity_usdt=98_980.0,
            active_positions=1,
            gross_exposure_usdt=10_000.0,
        ),
    )
    trade = SimpleNamespace(
        signal_id="s1",
        symbol="BTCUSDT",
        entry_at=start,
        exit_at=start + timedelta(minutes=1),
        mae=0.02,
        mfe=0.01,
        exit_reason="STOP_LOSS",
    )
    report = SimpleNamespace(
        equity_curve=curve,
        trades=(trade,),
        initial_equity_usdt=100_000.0,
        max_drawdown=0.02,
        mark_to_market_used=True,
        intrabar_path_used=True,
        simulation_complete=True,
    )

    evidence = intratrade_drawdown_evidence(report, scope="lockbox")

    assert evidence["status"] == "PASSED"
    assert evidence["recomputed_max_drawdown"] == 0.02
    assert evidence["trade_mae"]["maximum"] == 0.02


def test_production_replay_requires_one_passing_sample_per_contract_key():
    evidence = production_replay_evidence(
        [
            {"horizon_sec": 180, "symbol": "BTCUSDT", "passed": True},
            {"horizon_sec": 180, "symbol": "ETHUSDT", "passed": True},
        ],
        expected_horizons=[180],
        expected_symbols=["BTCUSDT", "ETHUSDT"],
    )
    assert evidence["status"] == "PASSED"

    failed = production_replay_evidence(
        [{"horizon_sec": 180, "symbol": "BTCUSDT", "passed": True}],
        expected_horizons=[180],
        expected_symbols=["BTCUSDT", "ETHUSDT"],
    )
    assert failed["status"] == "FAILED"
    assert failed["missing_sample_keys"] == [
        {"horizon_sec": 180, "symbol": "ETHUSDT"}
    ]
