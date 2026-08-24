from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import (
    KlinePanelSource,
    MINIMUM_COVERAGE_DAYS,
    SYMBOLS,
    audit_source_coverage,
)
from core.providers.bybit_kline_history import (
    BybitHTTPReceipt,
    BybitKlineHistoryStore,
    TIMEFRAME_INTERVAL_MS,
    instrument_launch_time_ms,
    instrument_url,
    kline_url,
)
from core.training.pooled_panel import HORIZON_TIMEFRAME


USER_AGENT = "ai-bybit-profitability-research/3.0"


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("cached receipt timestamp is not timezone-aware")
    return parsed.astimezone(timezone.utc)


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def _copy_database(source: Path, target: Path) -> None:
    source = source.resolve()
    target = target.resolve()
    if source == target:
        raise ValueError("source and target kline databases must differ")
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".copying")
    if temporary.exists():
        temporary.unlink()
    with closing(sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)) as src:
        with closing(sqlite3.connect(temporary)) as dst:
            src.backup(dst, pages=16_384, sleep=0.05)
            dst.commit()
    temporary.replace(target)


def _cache_paths(cache_dir: Path, url: str) -> tuple[Path, Path]:
    key = hashlib.sha256(url.encode()).hexdigest()
    return cache_dir / f"{key}.body", cache_dir / f"{key}.receipt.json"


def _cached_receipt(cache_dir: Path, url: str) -> BybitHTTPReceipt | None:
    body_path, metadata_path = _cache_paths(cache_dir, url)
    if body_path.exists() != metadata_path.exists():
        raise RuntimeError(f"incomplete cached Bybit receipt: {url}")
    if not body_path.exists():
        return None
    body = body_path.read_bytes()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if (
        metadata.get("request_url") != url
        or int(metadata.get("http_status", 0)) != 200
        or int(metadata.get("content_length", -1)) != len(body)
        or metadata.get("content_sha256") != hashlib.sha256(body).hexdigest()
    ):
        raise RuntimeError(f"cached Bybit receipt integrity failed: {url}")
    return BybitHTTPReceipt(
        request_url=url,
        requested_at=_timestamp(str(metadata["requested_at"])),
        received_at=_timestamp(str(metadata["received_at"])),
        http_status=200,
        body=body,
    )


def _download_receipt(
    cache_dir: Path,
    url: str,
    *,
    attempts: int = 5,
) -> BybitHTTPReceipt:
    cached = _cached_receipt(cache_dir, url)
    if cached is not None:
        return cached
    cache_dir.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, attempts + 1):
        requested_at = datetime.now(timezone.utc)
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                status = int(response.status)
                body = response.read()
            received_at = datetime.now(timezone.utc)
            receipt = BybitHTTPReceipt(
                request_url=url,
                requested_at=requested_at,
                received_at=received_at,
                http_status=status,
                body=body,
            )
            body_path, metadata_path = _cache_paths(cache_dir, url)
            body_temporary = body_path.with_suffix(body_path.suffix + ".tmp")
            body_temporary.write_bytes(body)
            body_temporary.replace(body_path)
            _atomic_json(
                metadata_path,
                {
                    "schema_version": "bybit-http-receipt.v1",
                    "request_url": url,
                    "requested_at": _iso(requested_at),
                    "received_at": _iso(received_at),
                    "http_status": status,
                    "content_length": len(body),
                    "content_sha256": hashlib.sha256(body).hexdigest(),
                },
            )
            return receipt
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError, ValueError):
            if attempt == attempts:
                raise
            time.sleep(min(30, 2**attempt))
    raise RuntimeError("unreachable Bybit retry state")


def _series_batches(database: Path, symbol: str, timeframe: str) -> int:
    with closing(
        sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    ) as connection:
        exists = connection.execute(
            """SELECT 1 FROM sqlite_master
                 WHERE type='table' AND name='bybit_kline_batches'"""
        ).fetchone()
        if not exists:
            return 0
        return int(
            connection.execute(
                """SELECT COUNT(*) FROM bybit_kline_batches
                    WHERE symbol=? AND timeframe=? AND source='bybit'""",
                (symbol, timeframe),
            ).fetchone()[0]
        )


def _coverage_report(
    database: Path,
    *,
    completed_end: pd.Timestamp,
    symbols: tuple[str, ...] = SYMBOLS,
    timeframes: tuple[str, ...] | None = None,
) -> dict[str, object]:
    source = KlinePanelSource(database, source="bybit")
    selected_timeframes = timeframes or tuple(
        HORIZON_TIMEFRAME[horizon] for horizon in HORIZON_TIMEFRAME
    )
    series: list[dict[str, object]] = []
    for symbol in symbols:
        for timeframe in selected_timeframes:
            try:
                frame = source.load(symbol, timeframe, 200_000)
                audit = audit_source_coverage(
                    frame,
                    timeframe,
                    listing_evidence=source.listing_evidence(symbol, timeframe),
                )
                observed_end = pd.Timestamp(frame["close_at"].max())
                if observed_end != completed_end:
                    audit = {
                        **audit,
                        "status": "FAILED",
                        "coverage_gate": "FAILED",
                        "failure_reasons": [
                            *list(audit.get("failure_reasons", [])),
                            "does_not_reach_precommitted_completed_end",
                        ],
                    }
            except Exception as exc:
                audit = {
                    "status": "FAILED",
                    "coverage_gate": "FAILED",
                    "continuity_gate": "FAILED",
                    "failure_reasons": [f"{type(exc).__name__}:{exc}"],
                }
            series.append({"symbol": symbol, "timeframe": timeframe, **audit})
    passed = sum(item.get("status") == "PASSED" for item in series)
    return {
        "schema_version": "bybit-kline-history-backfill.v1",
        "database": str(Path(database).resolve()),
        "source": "bybit",
        "venue_price_definition": "official Bybit last-trade kline OHLCV",
        "completed_end": completed_end.isoformat().replace("+00:00", "Z"),
        "status": "PASSED" if passed == len(series) else "FAILED",
        "complete": passed == len(series),
        "expected_series_count": len(series),
        "passed_series_count": passed,
        "series": series,
    }


def _completed_end(value: str | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC").floor("D")
    parsed = pd.to_datetime(value, utc=True, errors="raise")
    timestamp = pd.Timestamp(parsed)
    if timestamp != timestamp.floor("D"):
        raise ValueError("--completed-end must be a UTC day boundary")
    return timestamp


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a new versioned database of official Bybit last-trade klines. "
            "Every REST response and request boundary is retained and reverified."
        )
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=ROOT / "data" / "kline_feature_store.profitability-v2.sqlite3",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "data" / "kline_feature_store.profitability-v3-bybit.sqlite3",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=ROOT / "data" / "raw" / "bybit-kline-history",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT
        / "model_results"
        / "evaluation"
        / "bybit_kline_history_backfill_report.json",
    )
    parser.add_argument(
        "--completed-end",
        help="exclusive UTC day boundary; defaults to the current UTC day start",
    )
    parser.add_argument("--request-pause-sec", type=float, default=0.10)
    args = parser.parse_args()
    if args.request_pause_sec < 0:
        raise SystemExit("--request-pause-sec cannot be negative")
    completed_end = _completed_end(args.completed_end)
    _copy_database(args.source, args.output)
    store = BybitKlineHistoryStore(args.output)
    timeframes = tuple(HORIZON_TIMEFRAME[horizon] for horizon in HORIZON_TIMEFRAME)
    completed_end_ms = int(completed_end.timestamp() * 1_000)
    for symbol in SYMBOLS:
        instrument_receipt = _download_receipt(
            args.cache_dir / symbol / "instrument",
            instrument_url(symbol),
        )
        instrument_receipt_id = store.record_instrument(symbol, instrument_receipt)
        launch_time_ms = instrument_launch_time_ms(instrument_receipt, symbol)
        for timeframe in timeframes:
            interval_ms = TIMEFRAME_INTERVAL_MS[timeframe]
            launch_floor = launch_time_ms - launch_time_ms % interval_ms
            fixed_start = completed_end - pd.Timedelta(
                days=float(MINIMUM_COVERAGE_DAYS[timeframe])
            )
            fixed_start_ms = int(fixed_start.timestamp() * 1_000)
            window_start_ms = max(fixed_start_ms, launch_floor)
            if window_start_ms >= completed_end_ms:
                raise RuntimeError(f"{symbol} launched after the completed data boundary")
            if store.completed(
                symbol,
                timeframe,
                window_start_ms,
                completed_end_ms,
            ):
                print(f"already completed {symbol} {timeframe}", flush=True)
                continue
            if _series_batches(args.output, symbol, timeframe):
                raise RuntimeError(
                    f"{symbol} {timeframe} has a different immutable Bybit window; "
                    "use a new versioned --output"
                )
            responses: list[BybitHTTPReceipt] = []
            cursor = window_start_ms
            while cursor < completed_end_ms:
                request_end = min(
                    completed_end_ms,
                    cursor + interval_ms * 1_000,
                )
                url = kline_url(
                    symbol,
                    timeframe,
                    start_ms=cursor,
                    end_ms=request_end,
                    limit=1_000,
                )
                responses.append(
                    _download_receipt(
                        args.cache_dir / symbol / timeframe,
                        url,
                    )
                )
                cursor = request_end
                time.sleep(args.request_pause_sec)
            imported = store.import_window(
                symbol=symbol,
                timeframe=timeframe,
                window_start_ms=window_start_ms,
                window_end_ms=completed_end_ms,
                instrument_receipt_id=instrument_receipt_id,
                responses=responses,
            )
            print(
                f"completed {symbol} {timeframe} rows={imported.row_count} "
                f"responses={imported.response_count}",
                flush=True,
            )
    report = _coverage_report(
        args.output,
        completed_end=completed_end,
        timeframes=timeframes,
    )
    _atomic_json(args.report, report)
    if report["status"] != "PASSED":
        print(
            "versioned Bybit kline coverage FAILED: "
            f"{report['passed_series_count']}/{report['expected_series_count']} "
            f"report={args.report.resolve()}",
            flush=True,
        )
        return 2
    print(
        f"versioned Bybit kline store ready: {args.output.resolve()} "
        f"coverage={report['passed_series_count']}/{report['expected_series_count']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
