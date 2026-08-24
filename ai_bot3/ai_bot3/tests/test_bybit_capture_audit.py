from __future__ import annotations

import sys
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import _evaluate_bybit_pit_ablation
from core.providers.bybit_capture_audit import (
    audit_live_capture,
    merge_audited_liquidation_capture,
)
from core.providers.bybit_public_pit import BybitPublicPITStore
from core.training.bybit_pit_panel import (
    BybitPITFeatureSource,
    _completed_day_continuity,
)


SYMBOLS = ("BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "1000PEPEUSDT")


def test_daily_manifest_continuity_does_not_hide_missing_calendar_days():
    evidence = _completed_day_continuity(
        ("2026-01-01", "2026-01-02", "2026-01-04", "2026-01-05", "2026-01-06")
    )
    assert evidence["completed_source_day_count"] == 5
    assert evidence["longest_consecutive_completed_days"] == 3
    assert evidence["missing_source_day_count"] == 1
    assert evidence["completed_source_day_ratio"] == 5 / 6


def _captured_day(path: Path) -> BybitPublicPITStore:
    store = BybitPublicPITStore(path)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.start_session(
        "sealed-session",
        endpoint="wss://stream.bybit.com/v5/public/linear",
        symbols=SYMBOLS,
        started_at=start - timedelta(seconds=1),
    )
    for index in range(1_441):
        received = start + timedelta(minutes=index)
        symbol = SYMBOLS[index % len(SYMBOLS)]
        store.append_raw(
            event_id=f"raw-{index}",
            session_id="sealed-session",
            topic=f"allLiquidation.{symbol}",
            symbol=symbol,
            event_type="liquidation",
            exchange_time=received - timedelta(milliseconds=100),
            received_at=received,
            payload={"index": index, "symbol": symbol},
        )
        if index < len(SYMBOLS) or index >= 1_441 - len(SYMBOLS):
            store.append_feature(
                event_id=f"feature-{index}",
                symbol=symbol,
                name="liquidation_imbalance_5m",
                value=0.25,
                unit="ratio",
                event_time=received - timedelta(milliseconds=100),
                received_at=received,
                source="bybit.public.liquidations.v2",
                quality=0.95,
            )
    store.end_session(
        "sealed-session",
        ended_at=start + timedelta(days=1, seconds=1),
        status="completed",
    )
    return store


def test_running_capture_cannot_be_sealed(tmp_path):
    store = BybitPublicPITStore(tmp_path / "running.sqlite3")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.start_session(
        "running",
        endpoint="wss://stream.bybit.com/v5/public/linear",
        symbols=SYMBOLS,
        started_at=now,
    )
    store.append_raw(
        event_id="running-event",
        session_id="running",
        topic="publicTrade.BTCUSDT",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_time=now,
        received_at=now,
        payload={"trade": 1},
    )
    try:
        audit_live_capture(store)
    except RuntimeError as exc:
        assert "running capture sessions" in str(exc)
    else:
        raise AssertionError("a running capture session was accepted as sealed evidence")


def test_audited_liquidations_merge_append_only_and_unlock_continuity(tmp_path):
    source_path = tmp_path / "live.sqlite3"
    destination_path = tmp_path / "development.sqlite3"
    source = _captured_day(source_path)
    audit = audit_live_capture(source)
    source.close()

    first = merge_audited_liquidation_capture(source_path, destination_path)
    second = merge_audited_liquidation_capture(source_path, destination_path)
    assert first.source_audit_id == audit.audit_id
    assert first.source_counts["raw_events"] == 1_441
    assert first.inserted_counts["raw_events"] == 1_441
    assert all(count == 0 for count in second.inserted_counts.values())

    history, evidence = BybitPITFeatureSource(destination_path).load(
        ["liquidation_imbalance_5m"], symbols=SYMBOLS
    )
    assert len(history) == 10
    assert evidence["live_capture_audit_count"] == 1
    assert evidence["pit_import_count"] == 1
    report = _evaluate_bybit_pit_ablation(
        {},
        {},
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        evidence,
        factor_groups={"liquidations": ("liquidation_imbalance_5m",)},
        minimum_history_days=0.99,
    )["liquidations"]
    assert report["oos_ablation_status"] == "FAILED_INSUFFICIENT_PIT_ROWS"
    assert report["live_capture_continuity_complete"] is True
    assert report["qualifying_live_capture_audit_ids"] == [audit.audit_id]

    nonoverlapping = deepcopy(evidence)
    for item in nonoverlapping["feature_coverage"].values():
        item["start"] = "2027-01-01T00:00:00Z"
        item["end"] = "2027-01-02T00:00:00Z"
    rejected = _evaluate_bybit_pit_ablation(
        {},
        {},
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        nonoverlapping,
        factor_groups={"liquidations": ("liquidation_imbalance_5m",)},
        minimum_history_days=0.99,
    )["liquidations"]
    assert rejected["oos_ablation_status"] == "COLLECTING_INSUFFICIENT_PIT_HISTORY"
    assert rejected["live_capture_continuity_complete"] is False


def test_first_to_last_span_without_daily_or_capture_receipts_stays_collecting():
    coverage = {
        f"{symbol}:liquidation_imbalance_5m": {
            "observations": 2,
            "coverage_days": 181.0,
            "longest_consecutive_completed_days": 0,
        }
        for symbol in SYMBOLS
    }
    result = _evaluate_bybit_pit_ablation(
        {},
        {},
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        {"feature_coverage": coverage, "live_capture_audits": []},
        factor_groups={"liquidations": ("liquidation_imbalance_5m",)},
    )["liquidations"]
    assert result["oos_ablation_status"] == "COLLECTING_INSUFFICIENT_PIT_HISTORY"
    assert result["minimum_observed_history_days"] == 181.0
    assert result["live_capture_continuity_complete"] is False
