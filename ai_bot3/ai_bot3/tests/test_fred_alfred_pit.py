from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from core.providers.fred_alfred_pit import (
    FEATURE_NAMES,
    FredAlfredPITStore,
    HTTPPayload,
    backfill_fred_alfred_pit,
)
from core.providers import fred_alfred_pit


FETCHED = datetime(2026, 8, 22, tzinfo=timezone.utc)


def _initial(series: str) -> list[dict[str, str]]:
    if series == "CPIAUCSL":
        rows = []
        for offset in range(14):
            year = 2024 + offset // 12
            month = offset % 12 + 1
            observed = date(year, month, 1)
            released = observed + timedelta(days=42)
            rows.append(
                {
                    "realtime_start": released.isoformat(),
                    "realtime_end": "2026-08-20",
                    "date": observed.isoformat(),
                    "value": str(300 + offset),
                }
            )
        return rows
    values = {
        "VIXCLS": [("2025-01-02", "17.5"), ("2025-01-03", "18.0")],
        "DFII10": [("2025-01-02", "2.10"), ("2025-01-03", "2.12")],
        "PAYEMS": [("2025-01-01", "159000"), ("2025-02-01", "159250")],
        "UNRATE": [("2025-01-01", "4.1"), ("2025-02-01", "4.0")],
    }[series]
    rows = []
    for observed, value in values:
        observation_date = date.fromisoformat(observed)
        release_lag = (
            -1
            if series == "VIXCLS" and observation_date == date(2025, 1, 3)
            else 35
        )
        rows.append(
            {
                "realtime_start": (
                    observation_date + timedelta(days=release_lag)
                ).isoformat(),
                "realtime_end": "2026-08-20",
                "date": observed,
                "value": value,
            }
        )
    return rows


def _fake_requester(url: str, _timeout_sec: float) -> HTTPPayload:
    query = {key: values[0] for key, values in parse_qs(urlparse(url).query).items()}
    series = query["series_id"]
    output_type = int(query["output_type"])
    assert query["api_key"] == "a" * 32
    if output_type == 4:
        observations = _initial(series)
    elif series == "CPIAUCSL":
        observations = [
            {
                "date": "2025-01-01",
                "CPIAUCSL_20250212": "312.0",
                "CPIAUCSL_20260213": "311.8",
            }
        ]
    else:
        observations = [
            {
                "date": "2025-01-01",
                "PAYEMS_20250205": "159000",
                "PAYEMS_20250307": "158950",
            }
        ]
    body = json.dumps(
        {"output_type": output_type, "observations": observations},
        separators=(",", ":"),
    ).encode()
    return HTTPPayload(
        body=body,
        requested_at=FETCHED - timedelta(seconds=1),
        received_at=FETCHED,
        http_status=200,
    )


def test_fred_alfred_backfill_is_key_redacted_hashed_append_only_and_pit(tmp_path: Path):
    database = tmp_path / "macro.sqlite3"
    cache = tmp_path / "raw"
    store = FredAlfredPITStore(database)
    report = backfill_fred_alfred_pit(
        store,
        cache_dir=cache,
        api_key="a" * 32,
        observation_start=date(2024, 1, 1),
        observation_end=date(2026, 8, 20),
        requester=_fake_requester,
    )
    # An unchanged replay is idempotent and must not fabricate more history.
    backfill_fred_alfred_pit(
        store,
        cache_dir=cache,
        api_key="a" * 32,
        observation_start=date(2024, 1, 1),
        observation_end=date(2026, 8, 20),
        requester=_fake_requester,
    )

    assert report["status"] == "PASS"
    assert report["response_count"] == 7
    assert report["api_key_recorded"] is False
    assert set(report["feature_names"]) == set(FEATURE_NAMES)
    with store.connect() as connection:
        responses = connection.execute(
            "SELECT request_descriptor,content_sha256,raw_response_path FROM fred_alfred_responses"
        ).fetchall()
        observations = connection.execute(
            """SELECT name,event_time,available_at,ingested_at
                 FROM macro_pit_observations"""
        ).fetchall()
        assert connection.execute(
            "SELECT COUNT(*) FROM fred_alfred_vintages"
        ).fetchone()[0] == report["vintage_row_count"]
    assert len(responses) == 7
    assert observations
    assert all("api_key" not in row["request_descriptor"] for row in responses)
    assert all(len(row["content_sha256"]) == 64 for row in responses)
    assert all(Path(row["raw_response_path"]).is_file() for row in responses)
    assert all(
        row["event_time"] <= row["available_at"] <= row["ingested_at"]
        for row in observations
    )
    assert ("a" * 32).encode() not in database.read_bytes()


def test_fred_request_rejects_invalid_key_and_redacts_url_failures(monkeypatch):
    with pytest.raises(ValueError, match="32-character"):
        fred_alfred_pit._request_url(
            series_id="VIXCLS",
            api_key="not-a-key",
            output_type=4,
            observation_start=date(2025, 1, 1),
            observation_end=date(2025, 1, 2),
        )

    secret = "b" * 32

    def fail_with_url(_request, timeout):
        raise RuntimeError(
            f"failed URL containing api_key={secret} with timeout={timeout}"
        )

    monkeypatch.setattr(fred_alfred_pit.urllib.request, "urlopen", fail_with_url)
    url, descriptor = fred_alfred_pit._request_url(
        series_id="VIXCLS",
        api_key=secret,
        output_type=4,
        observation_start=date(2025, 1, 1),
        observation_end=date(2025, 1, 2),
    )
    assert secret not in descriptor
    with pytest.raises(RuntimeError) as failure:
        fred_alfred_pit._default_request(url, 1.0)
    assert secret not in str(failure.value)


def test_daily_series_are_split_below_official_vintage_limit(tmp_path: Path):
    store = FredAlfredPITStore(tmp_path / "macro.sqlite3")
    report = backfill_fred_alfred_pit(
        store,
        cache_dir=tmp_path / "raw",
        api_key="a" * 32,
        observation_start=date(2018, 1, 1),
        observation_end=date(2026, 8, 20),
        requester=_fake_requester,
    )

    # VIXCLS and DFII10 each need three realtime windows; the other five
    # series/output-type pairs remain a single request.
    assert report["response_count"] == 11
    assert len({item["response_id"] for item in report["responses"]}) == 11
    daily_descriptors = [
        json.loads(item["request_descriptor"])
        for item in report["responses"]
        if item["series_id"] in {"VIXCLS", "DFII10"}
    ]
    assert len(daily_descriptors) == 6
    assert all(
        (
            date.fromisoformat(item["realtime_end"])
            - date.fromisoformat(item["realtime_start"])
        ).days
        < fred_alfred_pit.MAX_DAILY_REALTIME_WINDOW_DAYS
        for item in daily_descriptors
    )
