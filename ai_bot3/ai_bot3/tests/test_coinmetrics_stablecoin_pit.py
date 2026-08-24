from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from core.evaluation.profitability_rebuild import LONG_FACTOR_GROUPS
from core.providers.coinmetrics_stablecoin_pit import (
    FEATURE_NAMES,
    CoinMetricsStablecoinPITStore,
    HTTPPayload,
    backfill_coinmetrics_stablecoin_pit,
)
from core.training.flow_pit_panel import FlowPITFeatureSource


UTC = timezone.utc
FETCHED = datetime(2026, 1, 20, tzinfo=UTC)


def _body(*, conflict: bool = False) -> bytes:
    rows: list[dict[str, str]] = []
    for offset in range(12):
        observed = date(2026, 1, 1) + timedelta(days=offset)
        rows.extend(
            (
                {
                    "asset": "usdc",
                    "time": observed.isoformat() + "T00:00:00.000000000Z",
                    "SplyCur": str(50_000_000_000 + offset * 10_000_000),
                },
                {
                    "asset": "usdt",
                    "time": observed.isoformat() + "T00:00:00.000000000Z",
                    "SplyCur": str(
                        100_000_000_000
                        + offset * 20_000_000
                        + (1 if conflict and offset == 10 else 0)
                    ),
                },
            )
        )
    return json.dumps({"data": rows}, separators=(",", ":")).encode()


def _requester(url: str, timeout_sec: float) -> HTTPPayload:
    assert "api_key" not in url
    assert "status=reviewed" in url
    assert timeout_sec > 0
    return HTTPPayload(
        body=_body(),
        requested_at=FETCHED - timedelta(seconds=1),
        received_at=FETCHED,
        http_status=200,
    )


def test_stablecoin_backfill_is_hashed_idempotent_semantic_and_pit(tmp_path: Path):
    database = tmp_path / "flows.sqlite3"
    cache = tmp_path / "raw"
    store = CoinMetricsStablecoinPITStore(database)
    report = backfill_coinmetrics_stablecoin_pit(
        store,
        cache_dir=cache,
        observation_start=date(2026, 1, 1),
        observation_end=date(2026, 1, 12),
        requester=_requester,
    )
    backfill_coinmetrics_stablecoin_pit(
        store,
        cache_dir=cache,
        observation_start=date(2026, 1, 1),
        observation_end=date(2026, 1, 12),
        requester=_requester,
    )

    assert report["status"] == "PASS"
    assert report["continuous_through_requested_end"] is True
    assert report["missing_complete_date_count"] == 0
    assert set(report["feature_names"]) == set(FEATURE_NAMES)
    assert report["api_key_required"] is False
    assert "not exchange netflow" in report["semantic_scope"]
    with store.connect() as connection:
        evidence = connection.execute(
            "SELECT * FROM coinmetrics_responses"
        ).fetchall()
        observations = connection.execute(
            "SELECT * FROM flow_pit_observations"
        ).fetchall()
    assert len(evidence) == 1
    assert len(evidence[0]["content_sha256"]) == 64
    raw_path = Path(evidence[0]["raw_response_path"])
    assert raw_path.is_file()
    assert hashlib.sha256(raw_path.read_bytes()).hexdigest() == evidence[0][
        "content_sha256"
    ]
    assert observations
    assert all(
        row["event_time"] <= row["available_at"] <= row["ingested_at"]
        for row in observations
    )


def test_stablecoin_backfill_rejects_an_internal_complete_date_gap(tmp_path: Path):
    payload = json.loads(_body())
    payload["data"] = [
        row
        for row in payload["data"]
        if not str(row["time"]).startswith("2026-01-06")
    ]

    def missing_day_requester(_url: str, _timeout_sec: float) -> HTTPPayload:
        return HTTPPayload(
            body=json.dumps(payload, separators=(",", ":")).encode(),
            requested_at=FETCHED - timedelta(seconds=1),
            received_at=FETCHED,
            http_status=200,
        )

    with pytest.raises(ValueError, match="continuously cover"):
        backfill_coinmetrics_stablecoin_pit(
            CoinMetricsStablecoinPITStore(tmp_path / "flows.sqlite3"),
            cache_dir=tmp_path / "raw",
            observation_start=date(2026, 1, 1),
            observation_end=date(2026, 1, 12),
            requester=missing_day_requester,
        )


def test_stablecoin_history_conflict_fails_closed(tmp_path: Path):
    store = CoinMetricsStablecoinPITStore(tmp_path / "flows.sqlite3")
    backfill_coinmetrics_stablecoin_pit(
        store,
        cache_dir=tmp_path / "raw",
        observation_start=date(2026, 1, 1),
        observation_end=date(2026, 1, 12),
        requester=_requester,
    )

    def conflicting(_url: str, _timeout_sec: float) -> HTTPPayload:
        return HTTPPayload(
            body=_body(conflict=True),
            requested_at=FETCHED,
            received_at=FETCHED + timedelta(days=1),
            http_status=200,
        )

    with pytest.raises(ValueError, match="historical ledger value changed"):
        backfill_coinmetrics_stablecoin_pit(
            store,
            cache_dir=tmp_path / "raw",
            observation_start=date(2026, 1, 1),
            observation_end=date(2026, 1, 12),
            requester=conflicting,
        )


def test_flow_snapshot_hash_raw_evidence_and_asof_join(tmp_path: Path):
    database = tmp_path / "flows.sqlite3"
    cache = tmp_path / "raw"
    store = CoinMetricsStablecoinPITStore(database)
    backfill_coinmetrics_stablecoin_pit(
        store,
        cache_dir=cache,
        observation_start=date(2026, 1, 1),
        observation_end=date(2026, 1, 12),
        requester=_requester,
    )
    source = FlowPITFeatureSource(database)
    maximum_sequence, maximum_invalidation_rowid = source.snapshot_watermarks()
    history, evidence = source.load(
        FEATURE_NAMES,
        maximum_sequence=maximum_sequence,
        maximum_invalidation_rowid=maximum_invalidation_rowid,
    )
    assert evidence["response_count"] == 1
    assert evidence["raw_response_hashes_verified"] is True
    assert len(evidence["snapshot_sha256"]) == 64
    decisions = pd.DataFrame(
        {
            "decision_at": [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 10, 12, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
            ]
        }
    )
    joined = source.join(
        decisions,
        names=["stablecoin_net_issuance_1d_usd"],
        history=history,
    )
    assert pd.isna(joined.loc[0, "stablecoin_net_issuance_1d_usd"])
    assert joined.loc[1, "stablecoin_net_issuance_1d_usd"] == 30_000_000
    assert pd.isna(joined.loc[2, "stablecoin_net_issuance_1d_usd"])
    assert (
        joined.loc[1, "stablecoin_net_issuance_1d_usd__available_at"]
        <= joined.loc[1, "decision_at"]
    )

    raw_path = next(cache.glob("*.json"))
    raw_path.write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="length|hash"):
        source.load(FEATURE_NAMES)


def test_long_factor_registry_does_not_mislabel_stablecoin_supply_as_exchange_flow():
    factors = LONG_FACTOR_GROUPS["stablecoin_flows"]
    assert set(factors) == set(FEATURE_NAMES)
    assert "stablecoin_exchange_netflow_1h" not in factors
    assert LONG_FACTOR_GROUPS["fund_flows"] == (
        "digital_asset_fund_flow_weekly_usd",
    )
