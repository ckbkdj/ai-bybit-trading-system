from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from core.evaluation.profitability_rebuild import LONG_FACTOR_GROUPS
from core.providers import fomc_pit
from core.providers.fomc_pit import FOMCPITStore, HTTPPayload, backfill_fomc_pit
from core.training.macro_pit_panel import MacroPITFeatureSource


UTC = timezone.utc
RECEIVED_AT = datetime(2026, 8, 24, 2, tzinfo=UTC)


INDEX = b"""
<html><body>
  <a href="/newsevents/pressreleases/monetary20240131a.htm">
    Federal Reserve issues FOMC statement
  </a>
  <a href="/newsevents/pressreleases/monetary20240221a.htm">
    Minutes of the Federal Open Market Committee
  </a>
  <a href="/newsevents/pressreleases/monetary20240612a.htm">
    Federal Reserve issues FOMC statement
  </a>
  <a href="/newsevents/pressreleases/monetary20240612a1.htm">
    Implementation Note
  </a>
</body></html>
"""

JANUARY_STATEMENT = b"""
<html><head><title>Federal Reserve issues FOMC statement</title></head>
<body><p>January 31, 2024</p><p>For release at 2:00 p.m. EST</p></body></html>
"""

JUNE_STATEMENT = b"""
<html><head><title>Federal Reserve issues FOMC statement</title></head>
<body><p>June 12, 2024</p><p>For release at 2:00 p.m. EDT</p></body></html>
"""


def _requester(url: str, _timeout: float) -> HTTPPayload:
    if url.endswith("2024-press-fomc.htm"):
        body = INDEX
    elif url.endswith("monetary20240131a.htm"):
        body = JANUARY_STATEMENT
    elif url.endswith("monetary20240612a.htm"):
        body = JUNE_STATEMENT
    else:
        raise AssertionError(f"unexpected URL {url}")
    return HTTPPayload(
        body=body,
        requested_at=RECEIVED_AT,
        received_at=RECEIVED_AT,
        http_status=200,
    )


def test_fomc_backfill_uses_explicit_official_release_times_and_hashes(tmp_path: Path):
    database = tmp_path / "macro.sqlite3"
    cache = tmp_path / "raw"
    store = FOMCPITStore(database)
    report = backfill_fomc_pit(
        store,
        cache_dir=cache,
        observation_start=date(2024, 1, 1),
        observation_end=date(2024, 12, 31),
        requester=_requester,
    )
    # Replaying an unchanged official snapshot is idempotent.
    backfill_fomc_pit(
        store,
        cache_dir=cache,
        observation_start=date(2024, 1, 1),
        observation_end=date(2024, 12, 31),
        requester=_requester,
    )

    assert report["status"] == "PASS"
    assert report["response_count"] == 3
    assert report["statement_count"] == 2
    assert report["feature_observation_count"] == 4
    assert report["guessed_release_time"] is False
    with sqlite3.connect(database) as connection:
        responses = connection.execute(
            """SELECT request_url,content_sha256,raw_response_path
                 FROM official_macro_responses ORDER BY request_url"""
        ).fetchall()
        events = connection.execute(
            "SELECT released_at FROM fomc_statement_events ORDER BY released_at"
        ).fetchall()
        states = connection.execute(
            """SELECT value,available_at FROM macro_pit_observations
                WHERE name='fomc_statement_event_state' ORDER BY available_at"""
        ).fetchall()
    assert len(responses) == 3
    assert all(len(row[1]) == 64 and Path(row[2]).is_file() for row in responses)
    assert [row[0] for row in events] == [
        "2024-01-31T19:00:00Z",
        "2024-06-12T18:00:00Z",
    ]
    assert states == [
        (1.0, "2024-01-31T19:00:00Z"),
        (0.0, "2024-02-01T19:00:00Z"),
        (1.0, "2024-06-12T18:00:00Z"),
        (0.0, "2024-06-13T18:00:00Z"),
    ]


def test_macro_source_verifies_fomc_evidence_and_joins_strictly_asof(tmp_path: Path):
    database = tmp_path / "macro.sqlite3"
    store = FOMCPITStore(database)
    backfill_fomc_pit(
        store,
        cache_dir=tmp_path / "raw",
        observation_start=date(2024, 1, 1),
        observation_end=date(2024, 12, 31),
        requester=_requester,
    )
    source = MacroPITFeatureSource(database)
    history, evidence = source.load(["fomc_statement_event_state"])
    joined = source.join(
        pd.DataFrame(
            {
                "decision_at": [
                    datetime(2024, 1, 31, 18, 59, tzinfo=UTC),
                    datetime(2024, 1, 31, 19, 0, tzinfo=UTC),
                    datetime(2024, 2, 1, 19, 0, tzinfo=UTC),
                ]
            }
        ),
        names=["fomc_statement_event_state"],
        history=history,
    )

    assert evidence["response_count"] == 3
    assert evidence["raw_response_hashes_verified"] is True
    assert pd.isna(joined.loc[0, "fomc_statement_event_state"])
    assert joined.loc[1, "fomc_statement_event_state"] == 1.0
    assert joined.loc[2, "fomc_statement_event_state"] == 0.0
    assert joined.loc[1, "fomc_statement_event_state__available_at"] <= joined.loc[1, "decision_at"]
    assert "fomc_statement_event_state" in LONG_FACTOR_GROUPS["tier_a_events"]


def test_fomc_parser_fails_closed_when_official_page_has_no_release_time():
    url = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20240131a.htm"
    with pytest.raises(ValueError, match="no explicit release time"):
        fomc_pit._release_datetime(url, b"<html><body>January 31, 2024</body></html>")


def test_fomc_backfill_falls_back_to_legacy_general_press_index(tmp_path: Path):
    calls: list[str] = []

    def requester(url: str, timeout: float) -> HTTPPayload:
        calls.append(url)
        if url.endswith("2018-press-fomc.htm"):
            return HTTPPayload(b"", RECEIVED_AT, RECEIVED_AT, 404)
        if url.endswith("2018-press.htm"):
            index = INDEX.replace(b"2024", b"2018")
            return HTTPPayload(index, RECEIVED_AT, RECEIVED_AT, 200)
        if url.endswith("monetary20180131a.htm"):
            body = JANUARY_STATEMENT.replace(b"2024", b"2018")
            return HTTPPayload(body, RECEIVED_AT, RECEIVED_AT, 200)
        if url.endswith("monetary20180612a.htm"):
            body = JUNE_STATEMENT.replace(b"2024", b"2018")
            return HTTPPayload(body, RECEIVED_AT, RECEIVED_AT, 200)
        raise AssertionError(f"unexpected URL {url} with timeout {timeout}")

    report = backfill_fomc_pit(
        FOMCPITStore(tmp_path / "macro.sqlite3"),
        cache_dir=tmp_path / "raw",
        observation_start=date(2018, 1, 1),
        observation_end=date(2018, 12, 31),
        requester=requester,
    )

    assert report["statement_count"] == 2
    assert calls[:2] == [
        fomc_pit.FOMC_INDEX_URL.format(year=2018),
        fomc_pit.GENERAL_INDEX_URL.format(year=2018),
    ]
