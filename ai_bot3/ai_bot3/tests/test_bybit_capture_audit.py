from __future__ import annotations

import sys
from copy import deepcopy
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import _evaluate_bybit_pit_ablation
from core.providers.bybit_capture_audit import (
    audit_live_capture,
    merge_audited_liquidation_capture,
)
from core.providers.bybit_public_pit import (
    BYBIT_PUBLIC_LINEAR_WS,
    BybitPublicPITStore,
)
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
                event_id=f"raw-{index}:bybit-liquidation-side-v2",
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


def test_capture_audit_rejects_non_official_endpoint(tmp_path):
    store = BybitPublicPITStore(tmp_path / "wrong-endpoint.sqlite3")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.start_session(
        "wrong-endpoint",
        endpoint="wss://example.com/not-bybit",
        symbols=("BTCUSDT",),
        started_at=now,
    )
    store.append_raw(
        event_id="wrong-endpoint-event",
        session_id="wrong-endpoint",
        topic="publicTrade.BTCUSDT",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_time=now,
        received_at=now,
        payload={"trade": 1},
    )
    store.end_session(
        "wrong-endpoint", ended_at=now + timedelta(seconds=1), status="completed"
    )
    try:
        audit_live_capture(store)
    except RuntimeError as exc:
        assert "non-official" in str(exc)
    else:
        raise AssertionError("a non-official endpoint was accepted as capture evidence")


def test_capture_audit_rejects_orphan_and_mistyped_raw_events(tmp_path):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    orphan = BybitPublicPITStore(tmp_path / "orphan.sqlite3")
    orphan.append_raw(
        event_id="orphan",
        session_id="missing-session",
        topic="publicTrade.BTCUSDT",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_time=now,
        received_at=now,
        payload={"trade": 1},
    )
    try:
        audit_live_capture(orphan)
    except RuntimeError as exc:
        assert "without a session contract" in str(exc)
    else:
        raise AssertionError("an orphan raw event was accepted as capture evidence")

    mistyped = BybitPublicPITStore(tmp_path / "mistyped.sqlite3")
    mistyped.start_session(
        "mistyped",
        endpoint=BYBIT_PUBLIC_LINEAR_WS,
        symbols=("BTCUSDT",),
        started_at=now,
    )
    mistyped.append_raw(
        event_id="mistyped-event",
        session_id="mistyped",
        topic="publicTrade.BTCUSDT",
        symbol="BTCUSDT",
        event_type="ticker",
        exchange_time=now,
        received_at=now,
        payload={"trade": 1},
    )
    mistyped.end_session(
        "mistyped", ended_at=now + timedelta(seconds=1), status="completed"
    )
    try:
        audit_live_capture(mistyped)
    except RuntimeError as exc:
        assert "topic/type contract" in str(exc)
    else:
        raise AssertionError("a mistyped raw event was accepted as capture evidence")


def test_capture_audit_rejects_stale_raw_event(tmp_path):
    store = BybitPublicPITStore(tmp_path / "stale.sqlite3")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.start_session(
        "stale",
        endpoint=BYBIT_PUBLIC_LINEAR_WS,
        symbols=("BTCUSDT",),
        started_at=now - timedelta(seconds=20),
    )
    store.append_raw(
        event_id="stale-event",
        session_id="stale",
        topic="publicTrade.BTCUSDT",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_time=now - timedelta(seconds=11),
        received_at=now,
        payload={"trade": 1},
    )
    store.end_session(
        "stale", ended_at=now + timedelta(seconds=1), status="completed"
    )
    try:
        audit_live_capture(store)
    except RuntimeError as exc:
        assert "lag contract" in str(exc)
    else:
        raise AssertionError("a stale raw event was accepted as capture evidence")


def test_capture_audit_rejects_liquidation_feature_without_raw_link(tmp_path):
    store = BybitPublicPITStore(tmp_path / "unlinked-feature.sqlite3")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.start_session(
        "unlinked",
        endpoint=BYBIT_PUBLIC_LINEAR_WS,
        symbols=("BTCUSDT",),
        started_at=now - timedelta(seconds=1),
    )
    store.append_raw(
        event_id="raw-liquidation",
        session_id="unlinked",
        topic="allLiquidation.BTCUSDT",
        symbol="BTCUSDT",
        event_type="liquidation",
        exchange_time=now,
        received_at=now,
        payload={"liquidation": 1},
    )
    store.append_feature(
        event_id="not-the-raw-event:bybit-liquidation-side-v2",
        symbol="BTCUSDT",
        name="liquidation_imbalance_5m",
        value=1.0,
        unit="ratio",
        event_time=now,
        received_at=now,
        source="bybit.public.liquidations.v2",
        quality=1.0,
    )
    store.end_session(
        "unlinked", ended_at=now + timedelta(seconds=1), status="completed"
    )
    try:
        BybitPITFeatureSource(store.path).load(["liquidation_imbalance_5m"])
    except RuntimeError as exc:
        assert "no deterministic raw-event link" in str(exc)
    else:
        raise AssertionError("training accepted an unlinked liquidation feature")
    try:
        audit_live_capture(store)
    except RuntimeError as exc:
        assert "no deterministic raw-event link" in str(exc)
    else:
        raise AssertionError("an unlinked liquidation feature was accepted")


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
    frozen_source = BybitPITFeatureSource(destination_path)
    frozen_sequence, frozen_invalidation = frozen_source.snapshot_watermarks()
    frozen_audit, frozen_import = frozen_source.evidence_watermarks()
    destination_store = BybitPublicPITStore(destination_path)
    second_audit = audit_live_capture(destination_store, maximum_gap_sec=89.0)
    _, frozen_evidence = frozen_source.load(
        ["liquidation_imbalance_5m"],
        maximum_sequence=frozen_sequence,
        maximum_invalidation_rowid=frozen_invalidation,
        maximum_capture_audit_rowid=frozen_audit,
        maximum_pit_import_rowid=frozen_import,
        symbols=SYMBOLS,
    )
    _, current_evidence = frozen_source.load(
        ["liquidation_imbalance_5m"], symbols=SYMBOLS
    )
    assert frozen_evidence["live_capture_audit_count"] == 1
    assert current_evidence["live_capture_audit_count"] == 2
    assert second_audit.audit_id not in {
        item["audit_id"] for item in frozen_evidence["live_capture_audits"]
    }
    assert frozen_evidence["snapshot_sha256"] != current_evidence["snapshot_sha256"]

    for statement in (
        "UPDATE bybit_live_capture_audits SET status='failed'",
        "DELETE FROM bybit_raw_public_events WHERE event_id='raw-0'",
        "UPDATE bybit_feature_observations SET value=0",
        "DELETE FROM bybit_pit_imports",
    ):
        try:
            with destination_store.connect() as connection:
                connection.execute(statement)
        except sqlite3.IntegrityError as exc:
            assert "append-only" in str(exc) or "immutable" in str(exc)
        else:
            raise AssertionError(f"immutable Bybit evidence accepted: {statement}")
    destination_store.close()
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

    missing_import = deepcopy(evidence)
    missing_import["historical_archive_file_count"] = 1
    missing_import["pit_imports"] = []
    unreceipted = _evaluate_bybit_pit_ablation(
        {},
        {},
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        missing_import,
        factor_groups={"liquidations": ("liquidation_imbalance_5m",)},
        minimum_history_days=0.99,
    )["liquidations"]
    assert unreceipted["oos_ablation_status"] == (
        "COLLECTING_INSUFFICIENT_PIT_HISTORY"
    )
    assert unreceipted["historical_store_requires_import_receipt"] is True
    assert unreceipted["live_capture_continuity_complete"] is False

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

    destination_store = BybitPublicPITStore(destination_path)
    forged_audit_id = "bca_" + ("0" * 48)
    with destination_store.connect() as connection:
        connection.execute(
            """INSERT INTO bybit_live_capture_audits(
                   audit_id,created_at,snapshot_maximum_raw_sequence,
                   snapshot_maximum_feature_sequence,snapshot_maximum_invalidation_rowid,
                   first_received_at,last_received_at,maximum_gap_sec,raw_event_count,
                   liquidation_feature_count,symbols_json,topic_counts_json,
                   event_type_counts_json,interval_count,longest_interval_sec,
                   manifest_sha256,status,error
               )
               SELECT ?,created_at,snapshot_maximum_raw_sequence,
                      snapshot_maximum_feature_sequence,
                      snapshot_maximum_invalidation_rowid,first_received_at,
                      last_received_at,maximum_gap_sec,raw_event_count,
                      liquidation_feature_count,symbols_json,topic_counts_json,
                      event_type_counts_json,0,longest_interval_sec,
                      manifest_sha256,status,error
                 FROM bybit_live_capture_audits ORDER BY rowid LIMIT 1""",
            (forged_audit_id,),
        )
        connection.commit()
    destination_store.close()
    try:
        BybitPITFeatureSource(destination_path).load(
            ["liquidation_imbalance_5m"], symbols=SYMBOLS
        )
    except RuntimeError as exc:
        assert "interval indices" in str(exc)
    else:
        raise AssertionError("training accepted a forged capture audit receipt")


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
