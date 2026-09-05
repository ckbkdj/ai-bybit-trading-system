from __future__ import annotations

import hashlib
import json
import sqlite3
from types import SimpleNamespace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytest

from core.providers import bybit_historical_derivatives as derivatives
from core.providers.bybit_historical_derivatives import (
    HTTPPayload,
    audit_historical_derivative_window,
    record_historical_api_failure,
    replay_basis_day,
    replay_funding_day,
    replay_open_interest_day,
)
from core.providers.bybit_public_pit import BybitPublicPITStore
from core.training.bybit_pit_panel import BybitPITFeatureSource
from scripts import backfill_bybit_historical_derivatives as backfill_script


DAY = date(2026, 8, 1)
START = datetime(2026, 8, 1, tzinfo=timezone.utc)
FETCHED = datetime(2026, 8, 22, tzinfo=timezone.utc)
SYMBOL = "1000PEPEUSDT"


def _payload(result: dict[str, object]) -> bytes:
    return json.dumps(
        {"retCode": 0, "retMsg": "OK", "result": result, "time": 1787356800000},
        separators=(",", ":"),
    ).encode()


def _fake_requester(url: str, _timeout_sec: float) -> HTTPPayload:
    parsed = urlparse(url)
    query = {key: values[0] for key, values in parse_qs(parsed.query).items()}
    if parsed.path == "/v5/market/funding/history":
        rows = [
            {
                "symbol": SYMBOL,
                "fundingRate": str(value),
                "fundingRateTimestamp": str(int((START + timedelta(hours=hour)).timestamp() * 1000)),
            }
            for hour, value in ((0, 0.0001), (8, -0.0002), (16, 0.0003))
        ]
        result = {"category": "linear", "list": list(reversed(rows))}
    elif parsed.path == "/v5/market/open-interest":
        window_start = datetime.fromtimestamp(int(query["startTime"]) / 1000, timezone.utc)
        window_end = datetime.fromtimestamp(int(query["endTime"]) / 1000, timezone.utc)
        points = []
        event_time = START - timedelta(hours=1)
        while event_time < START + timedelta(days=1):
            if window_start <= event_time <= window_end:
                periods = int(
                    (event_time - (START - timedelta(hours=1))).total_seconds()
                    / 300
                )
                points.append(
                    {
                        "openInterest": str(1000 + periods),
                        "timestamp": str(int(event_time.timestamp() * 1000)),
                    }
                )
            event_time += timedelta(minutes=5)
        result = {"category": "linear", "symbol": SYMBOL, "list": list(reversed(points))}
    elif parsed.path in {
        "/v5/market/mark-price-kline",
        "/v5/market/index-price-kline",
    }:
        window_start = datetime.fromtimestamp(int(query["start"]) / 1000, timezone.utc)
        window_end = datetime.fromtimestamp(int(query["end"]) / 1000, timezone.utc)
        index_price = 0.01
        mark_price = 0.01001
        close = mark_price if "mark-price" in parsed.path else index_price
        points = []
        event_time = window_start
        while event_time < window_end + timedelta(milliseconds=1):
            if window_start <= event_time <= window_end:
                stamp = str(int(event_time.timestamp() * 1000))
                points.append([stamp, str(close), str(close), str(close), str(close)])
            event_time += timedelta(minutes=1)
        result = {"category": "linear", "symbol": SYMBOL, "list": list(reversed(points))}
    else:  # pragma: no cover - makes unexpected endpoint changes obvious
        raise AssertionError(parsed.path)
    return HTTPPayload(
        body=_payload(result),
        requested_at=FETCHED - timedelta(seconds=1),
        received_at=FETCHED,
        http_status=200,
    )


def test_official_derivative_history_is_hashed_and_pit_joinable(tmp_path: Path) -> None:
    database = tmp_path / "bybit-pit.sqlite3"
    store = BybitPublicPITStore(database)
    funding = replay_funding_day(
        store, symbol=SYMBOL, trading_date=DAY, requester=_fake_requester
    )
    open_interest = replay_open_interest_day(
        store, symbol=SYMBOL, trading_date=DAY, requester=_fake_requester
    )
    basis = replay_basis_day(
        store, symbol=SYMBOL, trading_date=DAY, requester=_fake_requester
    )

    assert funding.feature_observation_count == 3
    assert open_interest.feature_observation_count == 288
    assert basis.feature_observation_count == 1440
    assert funding.response_count == 1
    assert open_interest.response_count == 2
    assert basis.response_count == 4

    history, evidence = BybitPITFeatureSource(database).load(
        ["funding_rate", "open_interest_change_1h", "perpetual_basis_bps"]
    )
    assert len(history) == 1731
    assert set(history["source"]) == {
        "bybit.public.funding_history",
        "bybit.public.open_interest_history",
        "bybit.public.mark_index_kline",
    }
    assert history["provenance_kind"].unique().tolist() == ["historical_api_replay"]
    assert evidence["historical_api_batch_count"] == 3
    assert evidence["historical_api_response_count"] == 7
    assert all(
        len(item["content_sha256"]) == 64
        for item in evidence["historical_api_responses"]
    )
    with store.connect() as connection:
        raw_responses = connection.execute(
            """SELECT content_blob,content_length,content_sha256
                 FROM bybit_historical_api_responses"""
        ).fetchall()
    assert len(raw_responses) == 7
    assert all(
        len(row["content_blob"]) == row["content_length"]
        and hashlib.sha256(row["content_blob"]).hexdigest()
        == row["content_sha256"]
        for row in raw_responses
    )
    assert not (
        (history["event_time"] > history["available_at"])
        | (history["available_at"] > history["ingested_at"])
    ).any()

    decisions = pd.DataFrame(
        {
            "symbol": [SYMBOL, SYMBOL],
            "decision_at": [
                START + timedelta(seconds=61),
                START + timedelta(seconds=63),
            ],
        }
    )
    joined = BybitPITFeatureSource(database).join(
        decisions,
        names=["perpetual_basis_bps"],
        history=history,
    )
    assert pd.isna(joined.loc[0, "perpetual_basis_bps"])
    assert joined.loc[1, "perpetual_basis_bps"] == pytest.approx(10.0)


def test_incomplete_basis_grid_fails_atomically(tmp_path: Path) -> None:
    def incomplete_requester(url: str, timeout_sec: float) -> HTTPPayload:
        response = _fake_requester(url, timeout_sec)
        parsed = urlparse(url)
        if parsed.path not in {
            "/v5/market/mark-price-kline",
            "/v5/market/index-price-kline",
        }:
            return response
        payload = json.loads(response.body)
        payload["result"]["list"] = payload["result"]["list"][1:]
        return HTTPPayload(
            body=json.dumps(payload, separators=(",", ":")).encode(),
            requested_at=response.requested_at,
            received_at=response.received_at,
            http_status=response.http_status,
        )

    database = tmp_path / "incomplete-grid.sqlite3"
    store = BybitPublicPITStore(database)

    with pytest.raises(ValueError, match="grid is incomplete"):
        replay_basis_day(
            store,
            symbol=SYMBOL,
            trading_date=DAY,
            requester=incomplete_requester,
        )
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_historical_api_batches"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_historical_api_responses"
        ).fetchone()[0] == 0


def test_response_category_mismatch_fails_atomically(tmp_path: Path) -> None:
    def wrong_category_requester(url: str, timeout_sec: float) -> HTTPPayload:
        response = _fake_requester(url, timeout_sec)
        payload = json.loads(response.body)
        payload["result"]["category"] = "spot"
        return HTTPPayload(
            body=json.dumps(payload, separators=(",", ":")).encode(),
            requested_at=response.requested_at,
            received_at=response.received_at,
            http_status=response.http_status,
        )

    store = BybitPublicPITStore(tmp_path / "wrong-category.sqlite3")
    with pytest.raises(ValueError, match="category contract"):
        replay_funding_day(
            store,
            symbol=SYMBOL,
            trading_date=DAY,
            requester=wrong_category_requester,
        )
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_historical_api_batches"
        ).fetchone()[0] == 0


def test_derivative_window_audit_rehashes_raw_evidence_and_requires_every_day(
    tmp_path: Path,
) -> None:
    store = BybitPublicPITStore(tmp_path / "audit.sqlite3")
    replay_funding_day(
        store, symbol=SYMBOL, trading_date=DAY, requester=_fake_requester
    )

    complete = audit_historical_derivative_window(
        store,
        start=DAY,
        end=DAY,
        symbols=[SYMBOL],
        data_kinds=["funding"],
    )
    assert complete["status"] == "VERIFIED_COMPLETE"
    assert complete["expected_batches"] == 1
    assert complete["completed_batches"] == 1

    incomplete = audit_historical_derivative_window(
        store,
        start=DAY,
        end=DAY + timedelta(days=1),
        symbols=[SYMBOL],
        data_kinds=["funding"],
    )
    assert incomplete["status"] == "FAILED"
    assert incomplete["missing_batches"] == 1
    assert incomplete["series"][0]["missing_date_ranges"] == [
        {"start": "2026-08-02", "end": "2026-08-02", "days": 1}
    ]

    # Simulate an offline SQLite-file modification after append-only controls
    # were bypassed. The retained body must be independently re-hashed.
    with store.connect() as connection:
        connection.execute("DROP TRIGGER reject_bybit_api_response_update")
        connection.execute(
            "UPDATE bybit_historical_api_responses SET content_blob=?",
            (b"{}",),
        )
        connection.commit()
    corrupted = audit_historical_derivative_window(
        store,
        start=DAY,
        end=DAY,
        symbols=[SYMBOL],
        data_kinds=["funding"],
    )
    assert corrupted["status"] == "FAILED"
    assert corrupted["integrity_violation_count"] > 0
    assert "response_body_hash_mismatch" in {
        item["reason"] for item in corrupted["integrity_violations"]
    }


def test_max_batches_cannot_report_partial_window_as_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = tmp_path / "report.json"
    database = tmp_path / "partial.sqlite3"
    monkeypatch.setattr(
        backfill_script,
        "parse_args",
        lambda: SimpleNamespace(
            start=DAY,
            end=DAY + timedelta(days=1),
            symbols=[SYMBOL],
            kinds=["funding"],
            database=database,
            report=report,
            timeout_sec=1.0,
            request_pause_sec=0.0,
            max_batches=1,
        ),
    )

    def replay_with_fixture(store, *, data_kind, symbol, trading_date, timeout_sec):
        assert data_kind == "funding"
        return replay_funding_day(
            store,
            symbol=symbol,
            trading_date=trading_date,
            requester=_fake_requester,
            timeout_sec=timeout_sec,
        )

    monkeypatch.setattr(
        backfill_script, "replay_derivative_day", replay_with_fixture
    )
    assert backfill_script.main() == 2
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["status"] == "FAILED_INCOMPLETE"
    assert payload["max_batches_reached"] is True
    assert payload["coverage_audit"]["missing_batches"] == 1


def test_historical_api_batch_validation_is_atomic(tmp_path: Path) -> None:
    store = BybitPublicPITStore(tmp_path / "bybit-pit.sqlite3")
    batch = {
        "batch_id": "bh_test",
        "data_kind": "funding",
        "market": "linear",
        "symbol": "BTCUSDT",
        "trading_date": "2026-08-01",
        "endpoint_group": "/v5/market/funding/history",
        "requested_at": "2026-08-22T00:00:00Z",
        "completed_at": "2026-08-22T00:00:01Z",
        "first_event_time": "2026-08-01T00:00:00Z",
        "last_event_time": "2026-08-01T00:00:00Z",
        "response_count": 2,
        "rows_read": 1,
        "feature_observation_count": 1,
        "request_manifest_sha256": "a" * 64,
    }
    observation = {
        "event_id": "funding-test",
        "symbol": "BTCUSDT",
        "name": "funding_rate",
        "value": 0.0001,
        "unit": "ratio",
        "event_time": START,
        "available_at": START + timedelta(minutes=1),
        "ingested_at": FETCHED,
        "source": "bybit.public.funding_history",
        "quality": 1.0,
    }
    with pytest.raises(ValueError, match="count"):
        store.append_feature_batch(
            [observation], api_batch_record=batch, api_response_records=[]
        )
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_historical_api_batches"
        ).fetchone()[0] == 0


def test_historical_api_rejects_non_official_hosts() -> None:
    with pytest.raises(ValueError, match="allow-list"):
        derivatives._validate_url(
            "https://example.com/v5/market/open-interest?symbol=BTCUSDT"
        )


def test_completed_historical_api_evidence_cannot_be_downgraded_by_late_failure(
    tmp_path: Path,
) -> None:
    database = tmp_path / "bybit-pit.sqlite3"
    store = BybitPublicPITStore(database)
    completed = replay_funding_day(
        store, symbol=SYMBOL, trading_date=DAY, requester=_fake_requester
    )

    record_historical_api_failure(
        store,
        data_kind="funding",
        symbol=SYMBOL,
        trading_date=DAY,
        error="a concurrent retry failed after the completed commit",
    )

    with store.connect() as connection:
        row = connection.execute(
            """SELECT batch_id,status,error,request_manifest_sha256,rows_read,
                      feature_observation_count,response_count
                 FROM bybit_historical_api_batches
                WHERE data_kind='funding' AND symbol=? AND trading_date=?""",
            (SYMBOL, DAY.isoformat()),
        ).fetchone()
    assert dict(row) == {
        "batch_id": completed.batch_id,
        "status": "completed",
        "error": None,
        "request_manifest_sha256": completed.request_manifest_sha256,
        "rows_read": completed.rows_read,
        "feature_observation_count": completed.feature_observation_count,
        "response_count": completed.response_count,
    }
    repeated = replay_funding_day(
        store, symbol=SYMBOL, trading_date=DAY, requester=_fake_requester
    )
    assert repeated.feature_observation_count == 0
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with store.connect() as connection:
            connection.execute(
                "UPDATE bybit_historical_api_batches SET status='failed'"
            )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with store.connect() as connection:
            connection.execute(
                "UPDATE bybit_historical_api_responses SET rows_read=0"
            )
