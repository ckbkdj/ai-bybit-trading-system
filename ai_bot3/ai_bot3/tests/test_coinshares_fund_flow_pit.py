from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from core.providers.coinshares_fund_flow_pit import (
    FEATURE_NAME,
    SITEMAP_URL,
    CoinSharesFundFlowPITStore,
    HTTPPayload,
    backfill_coinshares_fund_flow_pit,
    _weekly_flow,
)
from core.training.flow_pit_panel import FlowPITFeatureSource
from scripts import backfill_coinshares_fund_flow_pit as backfill_script


UTC = timezone.utc
FETCHED = datetime(2026, 8, 24, tzinfo=UTC)
START = date(2025, 1, 6)


def _ordinal(day: int) -> str:
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _article(index: int, *, parsable: bool = True) -> bytes:
    published = START + timedelta(days=index * 7)
    published_text = f"{published.strftime('%b')} {_ordinal(published.day)}, {published.year}"
    if parsable:
        direction = "inflows" if index % 2 == 0 else "outflows"
        flow = f"US${100 + index}m"
        paragraph = (
            f"Digital asset investment products saw {direction} totalling {flow} "
            "last week."
        )
    else:
        paragraph = "Digital asset investment products had an uneventful week."
    return (
        '<html><body><div class="article-content__main"><p>'
        + paragraph
        + '</p></div><p class="published-on">Published on<span>'
        + published_text
        + "</span></p></body></html>"
    ).encode()


def _sitemap() -> bytes:
    urls = "".join(
        (
            "<url><loc>https://coinshares.test/insights/research-data/"
            f"fund-flows-{index:03d}/</loc></url>"
        )
        for index in range(60)
    )
    return (
        '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        + urls
        + "</urlset>"
    ).encode()


def _requester(url: str, timeout_sec: float) -> HTTPPayload:
    assert timeout_sec > 0
    body = _sitemap() if url == SITEMAP_URL else _article(int(url.rstrip("/").rsplit("-", 1)[-1]))
    return HTTPPayload(
        body=body,
        requested_at=FETCHED - timedelta(seconds=1),
        received_at=FETCHED,
        http_status=200,
    )


def test_coinshares_backfill_uses_published_date_raw_hashes_and_signed_weekly_flow(
    tmp_path: Path,
):
    database = tmp_path / "flows.sqlite3"
    cache = tmp_path / "raw"
    report = backfill_coinshares_fund_flow_pit(
        CoinSharesFundFlowPITStore(database),
        cache_dir=cache,
        publication_start=START,
        publication_end=START + timedelta(days=59 * 7),
        workers=2,
        requester=_requester,
    )

    assert report["status"] == "PASS"
    assert report["feature_observation_count"] == 60
    assert report["excluded_count"] == 0
    assert "not daily issuer-level" in report["semantic_scope"]
    with CoinSharesFundFlowPITStore(database).connect() as connection:
        responses = connection.execute(
            "SELECT * FROM coinshares_responses"
        ).fetchall()
        observations = connection.execute(
            "SELECT * FROM flow_pit_observations ORDER BY available_at"
        ).fetchall()
    assert len(responses) == 60
    assert len(observations) == 60
    assert observations[0]["value"] == 100_000_000
    assert observations[1]["value"] == -101_000_000
    assert all(row["event_time"] <= row["available_at"] <= row["ingested_at"] for row in observations)
    for response in responses:
        raw_path = Path(response["raw_response_path"])
        assert hashlib.sha256(raw_path.read_bytes()).hexdigest() == response[
            "content_sha256"
        ]

    source = FlowPITFeatureSource(database)
    maximum_sequence, maximum_invalidation_rowid = source.snapshot_watermarks()
    history, evidence = source.load(
        [FEATURE_NAME],
        maximum_sequence=maximum_sequence,
        maximum_invalidation_rowid=maximum_invalidation_rowid,
    )
    assert evidence["response_count"] == 60
    decisions = pd.DataFrame(
        {
            "decision_at": [
                datetime.combine(START, datetime.min.time(), UTC),
                datetime.combine(START + timedelta(days=1), datetime.min.time(), UTC),
            ]
        }
    )
    joined = source.join(decisions, names=[FEATURE_NAME], history=history)
    assert pd.isna(joined.loc[0, FEATURE_NAME])
    assert joined.loc[1, FEATURE_NAME] == 100_000_000
    assert joined.loc[1, f"{FEATURE_NAME}__available_at"] <= joined.loc[1, "decision_at"]


def test_coinshares_unparsable_article_is_explicitly_excluded(tmp_path: Path):
    def requester(url: str, timeout_sec: float) -> HTTPPayload:
        if url == SITEMAP_URL:
            body = _sitemap()
        else:
            index = int(url.rstrip("/").rsplit("-", 1)[-1])
            body = _article(index, parsable=index != 7)
        return HTTPPayload(
            body=body,
            requested_at=FETCHED - timedelta(seconds=1),
            received_at=FETCHED,
            http_status=200,
        )

    report = backfill_coinshares_fund_flow_pit(
        CoinSharesFundFlowPITStore(tmp_path / "flows.sqlite3"),
        cache_dir=tmp_path / "raw",
        publication_start=START,
        publication_end=START + timedelta(days=59 * 7),
        requester=requester,
    )
    assert report["status"] == "PASS_WITH_EXCLUSIONS"
    assert report["feature_observation_count"] == 59
    assert report["excluded_count"] == 1
    assert "no parsable global weekly flow" in report["exclusions"][0]["reason"]


def test_coinshares_backfill_fails_closed_when_requested_tail_is_stale(tmp_path: Path):
    report = backfill_coinshares_fund_flow_pit(
        CoinSharesFundFlowPITStore(tmp_path / "flows.sqlite3"),
        cache_dir=tmp_path / "raw",
        publication_start=START,
        publication_end=START + timedelta(days=59 * 7 + 30),
        requester=_requester,
    )
    assert report["status"] == "FAILED_INCOMPLETE_COVERAGE"
    assert report["coverage_complete"] is False
    assert report["trailing_gap_days"] == 30


def test_coinshares_cli_returns_nonzero_for_incomplete_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    report_path = tmp_path / "report.json"
    monkeypatch.setattr(
        backfill_script,
        "backfill_coinshares_fund_flow_pit",
        lambda *_args, **_kwargs: {
            "status": "FAILED_INCOMPLETE_COVERAGE",
            "sitemap_article_count": 10,
            "feature_observation_count": 8,
            "excluded_count": 2,
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backfill_coinshares_fund_flow_pit.py",
            "--start",
            START.isoformat(),
            "--end",
            (START + timedelta(days=70)).isoformat(),
            "--database",
            str(tmp_path / "flows.sqlite3"),
            "--cache-dir",
            str(tmp_path / "raw"),
            "--report",
            str(report_path),
        ],
    )
    assert backfill_script.main() == 2
    assert json.loads(report_path.read_text(encoding="utf-8"))["status"] == (
        "FAILED_INCOMPLETE_COVERAGE"
    )


def test_equivalent_duplicate_publication_urls_are_canonical_and_idempotent(
    tmp_path: Path,
):
    duplicate_url = (
        "https://coinshares.test/insights/research-data/fund-flows-1000/"
    )

    def requester(url: str, timeout_sec: float) -> HTTPPayload:
        if url == SITEMAP_URL:
            body = _sitemap().replace(
                b"</urlset>",
                f"<url><loc>{duplicate_url}</loc></url></urlset>".encode(),
            )
        else:
            index = int(url.rstrip("/").rsplit("-", 1)[-1])
            body = _article(0 if index == 1000 else index)
        return HTTPPayload(
            body=body,
            requested_at=FETCHED - timedelta(seconds=1),
            received_at=FETCHED,
            http_status=200,
        )

    database = tmp_path / "flows.sqlite3"
    store = CoinSharesFundFlowPITStore(database)
    for _ in range(2):
        report = backfill_coinshares_fund_flow_pit(
            store,
            cache_dir=tmp_path / "raw",
            publication_start=START,
            publication_end=START + timedelta(days=59 * 7),
            workers=2,
            requester=requester,
        )
        assert report["article_response_count"] == 61
        assert report["feature_observation_count"] == 60
    history, _ = FlowPITFeatureSource(database).load([FEATURE_NAME])
    assert len(history) == 60


def test_flow_source_rejects_conflicting_active_parser_outputs(tmp_path: Path):
    database = tmp_path / "flows.sqlite3"
    store = CoinSharesFundFlowPITStore(database)
    backfill_coinshares_fund_flow_pit(
        store,
        cache_dir=tmp_path / "raw",
        publication_start=START,
        publication_end=START + timedelta(days=59 * 7),
        workers=2,
        requester=_requester,
    )
    with store.connect() as connection:
        original = connection.execute(
            "SELECT * FROM flow_pit_observations ORDER BY sequence LIMIT 1"
        ).fetchone()
        conflicting_id = "conflicting-active-parser-output"
        connection.execute(
            """INSERT INTO flow_pit_observations(
                   observation_id,name,value,unit,event_time,available_at,
                   ingested_at,source,series_id,observation_date,response_id
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                conflicting_id,
                original["name"],
                float(original["value"]) + 1_000_000,
                original["unit"],
                original["event_time"],
                original["available_at"],
                original["ingested_at"],
                original["source"],
                original["series_id"],
                original["observation_date"],
                original["response_id"],
            ),
        )
        connection.commit()

    source = FlowPITFeatureSource(database)
    with pytest.raises(RuntimeError, match="ambiguous active releases"):
        source.load([FEATURE_NAME])
    frozen_sequence, frozen_invalidation_rowid = source.snapshot_watermarks()

    backfill_coinshares_fund_flow_pit(
        store,
        cache_dir=tmp_path / "raw",
        publication_start=START,
        publication_end=START + timedelta(days=59 * 7),
        workers=2,
        requester=_requester,
    )
    with pytest.raises(RuntimeError, match="ambiguous active releases"):
        source.load(
            [FEATURE_NAME],
            maximum_sequence=frozen_sequence,
            maximum_invalidation_rowid=frozen_invalidation_rowid,
        )
    history, evidence = source.load([FEATURE_NAME])
    assert len(history) == 60
    assert evidence["equivalent_duplicate_count"] == 0
    assert evidence["snapshot_maximum_invalidation_rowid"] > frozen_invalidation_rowid
    with store.connect() as connection:
        correction = connection.execute(
            """SELECT reason,parser_version
                 FROM flow_pit_observation_invalidations
                WHERE observation_id=?""",
            (conflicting_id,),
        ).fetchone()
    assert correction["reason"].startswith("SUPERSEDED_BY:")
    assert correction["parser_version"].startswith("coinshares-weekly-flow-parser.")

    # Repeating the same parser backfill is idempotent: the canonical current
    # observation remains active and is not allowed to invalidate itself.
    backfill_coinshares_fund_flow_pit(
        store,
        cache_dir=tmp_path / "raw",
        publication_start=START,
        publication_end=START + timedelta(days=59 * 7),
        workers=2,
        requester=_requester,
    )
    repeated, _ = source.load([FEATURE_NAME])
    assert len(repeated) == 60


def test_coinshares_parser_separates_headline_cumulative_and_nearest_direction():
    body = b"""
        <html><body>
        <h2>US ETFs reached US$62.9bn in cumulative net inflows</h2>
        <p>Digital asset investment products rebounded from the previous week's
        outflows, recording inflows of US$882m last week and bringing YTD
        inflows to US$6.7bn.</p>
        </body></html>
    """
    value, sentence = _weekly_flow(body)
    assert value == 882_000_000
    assert "US$882m" in sentence

    body = b"""
        <html><body>
        <h2>Record outflows of US$3.8bn over three weeks</h2>
        <p>Digital asset investment products saw the largest weekly outflows
        on record at US$2.9bn, bringing the three-week total to US$3.8bn.</p>
        </body></html>
    """
    value, _ = _weekly_flow(body)
    assert value == -2_900_000_000

    value, _ = _weekly_flow(
        b"<p>Digital asset investment products experienced weekly outflows "
        b"totalling US$725,7m.</p>"
    )
    assert value == -725_700_000

    with pytest.raises(ValueError, match="annual aggregate"):
        _weekly_flow(
            b"<p>Digital asset investment products finished 2025 with global "
            b"inflows totalling US$47.2B, below the 2024 record.</p>"
        )
