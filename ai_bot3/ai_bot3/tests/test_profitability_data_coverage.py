from __future__ import annotations

import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import (
    BYBIT_EXECUTION_EVIDENCE_FEATURES,
    MINIMUM_COVERAGE_DAYS,
    ProfitabilityRebuild,
    ProfitabilityRebuildConfig,
    _build_direct_release_dataset,
    _bybit_names_for_horizon,
    _emit_ablation_progress,
    _engineer_features,
    _market_bars,
    _maximum_execution_window_observed,
    _panel_rows,
    validate_source_coverage,
)
from core.training.pooled_panel import PooledPanelBuilder
from core.labels.triple_barrier import MarketBar


def _frame(days: int, interval_sec: int) -> pd.DataFrame:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    periods = int(days * 86_400 / interval_sec) + 1
    opens = pd.date_range(
        start,
        periods=periods,
        freq=pd.Timedelta(seconds=interval_sec),
    )
    return pd.DataFrame(
        {
            "open_at": opens,
            "close_at": opens + pd.Timedelta(seconds=interval_sec),
        }
    )


def test_short_horizon_coverage_cannot_silently_collapse_to_six_or_31_days(tmp_path):
    with pytest.raises(ValueError, match="coverage"):
        validate_source_coverage(_frame(6, 180), "3m")
    with pytest.raises(ValueError, match="coverage"):
        validate_source_coverage(_frame(31, 900), "15m")
    evidence = validate_source_coverage(_frame(181, 180), "3m")
    assert evidence["coverage_days"] >= MINIMUM_COVERAGE_DAYS["3m"]
    assert evidence["continuity_gate"] == "PASSED"

    config = ProfitabilityRebuildConfig(
        feature_store_path=tmp_path / "features.sqlite3",
        output_dir=tmp_path / "reports",
        trial_ledger_path=tmp_path / "trials.sqlite3",
        model_output_dir=tmp_path / "models",
        code_commit="1" * 40,
    )
    assert config.max_bars_per_symbol >= 175_200


def test_nominal_history_span_cannot_hide_a_missing_kline():
    frame = _frame(181, 180).drop(index=100).reset_index(drop=True)

    with pytest.raises(ValueError, match="discontinuous"):
        validate_source_coverage(frame, "3m")


def test_long_horizons_load_real_execution_evidence_without_using_short_factors():
    short_factor_names = (
        "bybit_orderbook_delta_l5",
        "orderbook_spread_bps",
        "orderbook_depth_usdt_l5",
        "funding_rate",
        "open_interest_change_1h",
    )
    assert _bybit_names_for_horizon(180, short_factor_names) == short_factor_names
    assert _bybit_names_for_horizon(900, short_factor_names) == short_factor_names
    for horizon in (7200, 14400, 86400):
        requested = _bybit_names_for_horizon(horizon, short_factor_names)
        assert requested == BYBIT_EXECUTION_EVIDENCE_FEATURES
        assert "orderbook_spread_bps" in requested
        assert "orderbook_depth_usdt_l5" in requested
        assert "funding_rate" in requested
        assert "open_interest_change_1h" not in requested


def test_separate_lockbox_bybit_store_is_not_opened_during_initialization(tmp_path):
    feature_store = tmp_path / "features.sqlite3"
    feature_store.touch()
    sealed_store = tmp_path / "must-not-be-read-before-development-passes.sqlite3"
    config = ProfitabilityRebuildConfig(
        feature_store_path=feature_store,
        output_dir=tmp_path / "reports",
        trial_ledger_path=tmp_path / "trials.sqlite3",
        model_output_dir=tmp_path / "models",
        code_commit="1" * 40,
        lockbox_bybit_pit_store_path=sealed_store,
    )

    # Construction freezes development sources only.  A missing final store
    # must not be observed until run() has independently passed development.
    runner = ProfitabilityRebuild(config)
    assert runner.config.lockbox_bybit_pit_store_path == sealed_store

    alternate = ProfitabilityRebuildConfig(
        feature_store_path=feature_store,
        output_dir=tmp_path / "reports",
        trial_ledger_path=tmp_path / "trials.sqlite3",
        model_output_dir=tmp_path / "models",
        code_commit="1" * 40,
        lockbox_bybit_pit_store_path=tmp_path / "another-sealed-store.sqlite3",
    )
    assert ProfitabilityRebuild(alternate).trial_id == runner.trial_id


def test_trial_identity_binds_feature_store_content_not_only_size_and_mtime(tmp_path):
    feature_store = tmp_path / "same-metadata-different-content.sqlite3"
    feature_store.write_bytes(b"AAAA")
    original_stat = feature_store.stat()

    def config() -> ProfitabilityRebuildConfig:
        return ProfitabilityRebuildConfig(
            feature_store_path=feature_store,
            output_dir=tmp_path / "reports",
            trial_ledger_path=tmp_path / "trials.sqlite3",
            model_output_dir=tmp_path / "models",
            code_commit="1" * 40,
        )

    first = ProfitabilityRebuild(config())
    feature_store.write_bytes(b"BBBB")
    os.utime(
        feature_store,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    second = ProfitabilityRebuild(config())

    assert feature_store.stat().st_size == original_stat.st_size
    assert feature_store.stat().st_mtime_ns == original_stat.st_mtime_ns
    assert first.feature_store_identity["sha256"] != second.feature_store_identity[
        "sha256"
    ]
    assert first.trial_id != second.trial_id


def test_label_decisions_do_not_treat_overlapping_execution_windows_as_independent():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(120):
        open_at = start + timedelta(seconds=180 * index)
        price = 100.0 + index * 0.01
        rows.append(
            {
                "symbol": "BTCUSDT",
                "open_at": open_at,
                "close_at": open_at + timedelta(seconds=179),
                "open": price,
                "high": price * 1.002,
                "low": price * 0.998,
                "close": price * 1.0002,
                "volume": 1_000.0 + index,
            }
        )
    frame = pd.DataFrame(rows)
    bars = _market_bars(_engineer_features(frame))
    labels = _panel_rows(frame, 180, bars)
    decision_times = sorted({row["decision_at"] for row in labels})

    assert decision_times
    assert all(
        later - earlier >= timedelta(seconds=270)
        for earlier, later in zip(decision_times, decision_times[1:])
    )
    for decision_at in decision_times:
        alternatives = [row for row in labels if row["decision_at"] == decision_at]
        assert {row["side"] for row in alternatives} == {"BUY", "SELL"}
        assert {
            row["execution_window_evidence_complete"] for row in alternatives
        } == {False}


def test_direct_execution_window_is_bound_before_buy_sell_outcomes_are_inspected():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "open_at": start + timedelta(seconds=180 * index),
                "close_at": start + timedelta(seconds=180 * index + 179),
                "open": 100.0 + index * 0.01,
                "high": (100.0 + index * 0.01) * 1.002,
                "low": (100.0 + index * 0.01) * 0.998,
                "close": (100.0 + index * 0.01) * 1.0001,
                "volume": 1_000.0,
            }
            for index in range(120)
        ]
    )
    enriched = _engineer_features(frame)
    direct_bars = tuple(
        replace(
            bar,
            spread_bps=1.5,
            depth_usdt=1_000_000.0,
            funding_bps=0.01,
            spread_source="bybit_orderbook_pit",
            depth_source="bybit_orderbook_pit",
            funding_source="bybit_funding_pit",
            spread_observed=True,
            depth_observed=True,
            funding_observed=True,
        )
        for bar in _market_bars(enriched)
    )
    labels = _panel_rows(enriched, 180, direct_bars)

    assert labels
    assert {row["execution_window_evidence_complete"] for row in labels} == {True}
    assert {row["execution_cost_evidence_complete"] for row in labels} == {True}
    for decision_at in {row["decision_at"] for row in labels}:
        alternatives = [row for row in labels if row["decision_at"] == decision_at]
        assert {row["side"] for row in alternatives} == {"BUY", "SELL"}
        assert {
            row["execution_window_evidence_complete"] for row in alternatives
        } == {True}


def test_late_funding_availability_cannot_extend_the_observed_price_path():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    first = MarketBar(
        symbol="BTCUSDT",
        open_time=start,
        close_time=start + timedelta(minutes=2),
        available_at=start + timedelta(days=1),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
    )
    second = replace(
        first,
        open_time=start + timedelta(minutes=2),
        close_time=start + timedelta(minutes=4),
        available_at=start + timedelta(days=1, minutes=1),
    )

    assert not _maximum_execution_window_observed(
        (first,),
        window_start=start,
        window_end=start + timedelta(minutes=3),
    )
    assert _maximum_execution_window_observed(
        (first, second),
        window_start=start,
        window_end=start + timedelta(minutes=3),
    )


def test_release_walk_forward_excludes_proxy_rows_before_splitting():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(160):
        decision_at = start + timedelta(minutes=5 * index)
        for symbol in ("BTCUSDT", "ETHUSDT"):
            rows.append(
                {
                    "symbol": symbol,
                    "horizon_sec": 180,
                    "decision_at": decision_at,
                    "available_at": decision_at,
                    "label_available_at": decision_at + timedelta(seconds=180),
                    "liquidity": 1_000_000.0,
                    "volatility": 0.01,
                    "session": "asia",
                    "regime": "normal",
                    "net_return": 0.001 if index % 3 else -0.002,
                    "mae": 0.001,
                    "mfe": 0.002,
                    "execution_window_evidence_complete": index % 2 == 0,
                }
            )
    panel = pd.DataFrame(rows)
    dataset, evidence = _build_direct_release_dataset(
        PooledPanelBuilder(
            minimum_train_rows=40,
            minimum_test_rows=10,
            maximum_folds=2,
        ),
        panel,
        180,
        lockbox_start=start + timedelta(days=1),
    )

    assert dataset is not None
    assert dataset.development["execution_window_evidence_complete"].all()
    assert set(dataset.development["net_return"] > 0) == {False, True}
    assert evidence["selection_columns"] == [
        "execution_window_evidence_complete"
    ]
    assert evidence["outcome_dependent_selection"] is False
    assert evidence["direct_window_rows"] == 160

    corrupted = panel.copy()
    corrupted.loc[0, "available_at"] = (
        pd.Timestamp(corrupted.loc[0, "decision_at"]) + timedelta(seconds=1)
    )
    with pytest.raises(ValueError, match="PIT violation"):
        _build_direct_release_dataset(
            PooledPanelBuilder(
                minimum_train_rows=40,
                minimum_test_rows=10,
                maximum_folds=2,
            ),
            corrupted,
            180,
            lockbox_start=start + timedelta(days=1),
        )


def test_long_running_ablation_emits_auditable_fold_heartbeat():
    events = []
    _emit_ablation_progress(
        events.append,
        factor_group="legacy_brain_technical",
        horizon_sec=180,
        fold_id="outer_01",
        status="STARTED",
        train_rows=100_000,
        test_rows=20_000,
    )

    assert events == [
        {
            "factor_group": "legacy_brain_technical",
            "horizon_sec": 180,
            "fold_id": "outer_01",
            "status": "STARTED",
            "train_rows": 100_000,
            "test_rows": 20_000,
        }
    ]


def test_development_label_materialization_stops_before_sealed_lockbox_path():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(160):
        open_at = start + timedelta(seconds=180 * index)
        rows.append(
            {
                "symbol": "BTCUSDT",
                "open_at": open_at,
                "close_at": open_at + timedelta(seconds=179),
                "open": 100.0,
                "high": 100.2,
                "low": 99.8,
                "close": 100.01,
                "volume": 1_000.0,
            }
        )
    frame = pd.DataFrame(rows)
    enriched = _engineer_features(frame)
    bars = _market_bars(enriched)
    lockbox_start = rows[130]["open_at"]
    development_end = lockbox_start - timedelta(seconds=270)
    labels = _panel_rows(
        enriched,
        180,
        bars,
        decision_before=development_end,
    )

    assert labels
    assert max(row["decision_at"] for row in labels) < development_end
    assert max(row["label_available_at"] for row in labels) < lockbox_start
