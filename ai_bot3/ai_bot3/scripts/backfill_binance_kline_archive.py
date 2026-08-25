from __future__ import annotations

import argparse
import json
import shutil
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
from core.providers.binance_kline_archive import (
    BinanceKlineArchiveStore,
    archive_url,
    checksum_url,
)
from core.training.pooled_panel import HORIZON_TIMEFRAME


USER_AGENT = "ai-bybit-profitability-research/2.0"


def _month(value: pd.Timestamp) -> str:
    return value.strftime("%Y-%m")


def _previous_month(value: str) -> str:
    return _month(pd.Timestamp(f"{value}-01", tz="UTC") - pd.offsets.MonthBegin())


def _download(url: str, *, attempts: int = 4) -> tuple[int, bytes]:
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return int(response.status), response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return 404, b""
            if attempt == attempts:
                raise
        except (TimeoutError, urllib.error.URLError):
            if attempt == attempts:
                raise
        time.sleep(min(30, 2**attempt))
    raise RuntimeError("unreachable archive retry state")


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


def _latest_close(database: Path, symbol: str, timeframe: str) -> pd.Timestamp:
    with closing(sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)) as connection:
        row = connection.execute(
            """SELECT MAX(close_time) FROM raw_kline
                WHERE symbol=? AND timeframe=? AND source='binance'""",
            (symbol, timeframe),
        ).fetchone()
    if row is None or row[0] is None:
        raise ValueError(f"no existing kline anchor for {symbol} {timeframe}")
    return pd.Timestamp(int(row[0]), unit="ms", tz="UTC")


def _cache_paths(cache: Path, symbol: str, timeframe: str, year_month: str) -> tuple[Path, Path]:
    folder = cache / symbol / timeframe
    folder.mkdir(parents=True, exist_ok=True)
    filename = f"{symbol}-{timeframe}-{year_month}.zip"
    return folder / filename, folder / f"{filename}.CHECKSUM"


def _payload(path: Path, url: str) -> tuple[int, bytes]:
    if path.is_file() and path.stat().st_size > 0:
        return 200, path.read_bytes()
    status, body = _download(url)
    if status == 200:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(body)
        temporary.replace(path)
    return status, body


def _coverage_report(
    database: Path,
    *,
    symbols: tuple[str, ...] = SYMBOLS,
    timeframes: tuple[str, ...] | None = None,
) -> dict[str, object]:
    source = KlinePanelSource(database)
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
        "schema_version": "binance-kline-archive-backfill.v1",
        "database": str(Path(database).resolve()),
        "status": "PASSED" if passed == len(series) else "FAILED",
        "complete": passed == len(series),
        "expected_series_count": len(series),
        "passed_series_count": passed,
        "series": series,
    }


def _atomic_json(path: Path, payload: object) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a new versioned kline database from checksum-verified official "
            "Binance USD-M monthly archives. The source snapshot is never modified."
        )
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=ROOT / "data" / "kline_feature_store.rebuilt.20260822.sqlite3",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "data" / "kline_feature_store.profitability-v2.sqlite3",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=ROOT / "data" / "raw" / "binance-kline-archive",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT
        / "model_results"
        / "evaluation"
        / "kline_archive_backfill_report.json",
    )
    parser.add_argument("--request-pause-sec", type=float, default=0.05)
    args = parser.parse_args()
    if args.request_pause_sec < 0:
        raise SystemExit("--request-pause-sec cannot be negative")
    _copy_database(args.source, args.output)
    store = BinanceKlineArchiveStore(args.output)
    timeframes = tuple(HORIZON_TIMEFRAME[horizon] for horizon in HORIZON_TIMEFRAME)
    for symbol in SYMBOLS:
        for timeframe in timeframes:
            latest = _latest_close(args.output, symbol, timeframe)
            fixed_days = float(MINIMUM_COVERAGE_DAYS[timeframe])
            target = latest - pd.Timedelta(days=fixed_days)
            current_month = pd.Timestamp(
                year=latest.year, month=latest.month, day=1, tz="UTC"
            ) - pd.offsets.MonthBegin()
            target_month = pd.Timestamp(
                year=target.year, month=target.month, day=1, tz="UTC"
            )
            year_month = _month(current_month)
            completed_months: list[str] = []
            prior_not_found: str | None = None
            while True:
                archive_path, checksum_path = _cache_paths(
                    args.cache_dir, symbol, timeframe, year_month
                )
                if store.completed(symbol, timeframe, year_month):
                    completed_months.append(year_month)
                elif store.not_found(symbol, timeframe, year_month):
                    prior_not_found = year_month
                    break
                else:
                    checksum_status, checksum_body = _payload(
                        checksum_path,
                        checksum_url(symbol, timeframe, year_month),
                    )
                    if checksum_status == 404:
                        archive_status, _ = _payload(
                            archive_path,
                            archive_url(symbol, timeframe, year_month),
                        )
                        if archive_status != 404:
                            raise RuntimeError(
                                "checksum is missing but the Binance archive exists"
                            )
                        store.record_not_found(
                            symbol=symbol,
                            timeframe=timeframe,
                            year_month=year_month,
                            archive_http_status=archive_status,
                            checksum_http_status=checksum_status,
                        )
                        prior_not_found = year_month
                        break
                    archive_status, archive_body = _payload(
                        archive_path,
                        archive_url(symbol, timeframe, year_month),
                    )
                    if archive_status == 404:
                        if checksum_status != 404:
                            raise RuntimeError(
                                "Binance archive is missing but its checksum exists"
                            )
                        store.record_not_found(
                            symbol=symbol,
                            timeframe=timeframe,
                            year_month=year_month,
                            archive_http_status=archive_status,
                            checksum_http_status=checksum_status,
                        )
                        prior_not_found = year_month
                        break
                    imported = store.import_month(
                        symbol=symbol,
                        timeframe=timeframe,
                        year_month=year_month,
                        archive_body=archive_body,
                        checksum_body=checksum_body,
                        archive_path=archive_path,
                        checksum_path=checksum_path,
                    )
                    completed_months.append(year_month)
                    print(
                        f"completed {symbol} {timeframe} {year_month} "
                        f"rows={imported.row_count}",
                        flush=True,
                    )
                    time.sleep(args.request_pause_sec)
                if pd.Timestamp(f"{year_month}-01", tz="UTC") <= target_month:
                    break
                year_month = _previous_month(year_month)
            if prior_not_found is not None:
                if not completed_months:
                    raise RuntimeError(
                        f"no Binance archive month exists for {symbol} {timeframe}"
                    )
                first_archive_month = min(completed_months)
                if _previous_month(first_archive_month) != prior_not_found:
                    raise RuntimeError(
                        "listing evidence is not adjacent to the first completed month"
                    )
                store.finalize_listing_evidence(
                    symbol=symbol,
                    timeframe=timeframe,
                    first_archive_year_month=first_archive_month,
                    prior_month_year_month=prior_not_found,
                )
                print(
                    f"verified since-listing boundary {symbol} {timeframe}: "
                    f"404={prior_not_found}, first={first_archive_month}",
                    flush=True,
                )
    report = _coverage_report(args.output, timeframes=timeframes)
    _atomic_json(args.report, report)
    if report["status"] != "PASSED":
        print(
            "versioned kline store coverage FAILED: "
            f"{report['passed_series_count']}/{report['expected_series_count']} "
            f"report={args.report.resolve()}",
            flush=True,
        )
        return 2
    print(
        f"versioned kline store ready: {args.output.resolve()} "
        f"coverage={report['passed_series_count']}/{report['expected_series_count']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
