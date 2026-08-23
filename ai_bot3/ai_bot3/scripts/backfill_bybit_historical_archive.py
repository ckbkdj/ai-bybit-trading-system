from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_historical_archive import (
    archive_already_completed,
    download_official_archive,
    orderbook_archive_url,
    record_archive_failure,
    replay_orderbook_archive,
    replay_trade_archive,
    trade_archive_url,
)
from core.providers.bybit_public_pit import BybitPublicPITStore
DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "1000PEPEUSDT",
)


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    temporary.replace(path)


def _dates(start: date, end: date) -> tuple[date, ...]:
    if end < start:
        raise ValueError("end date precedes start date")
    return tuple(start + timedelta(days=offset) for offset in range((end - start).days + 1))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill strict-PIT features from official Bybit public archives"
    )
    parser.add_argument("--start", type=_date, required=True)
    parser.add_argument("--end", type=_date, required=True)
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument(
        "--kinds",
        nargs="+",
        choices=("orderbook", "trades"),
        default=["orderbook", "trades"],
    )
    parser.add_argument(
        "--database", type=Path, default=ROOT / "data" / "bybit_public_pit.sqlite3"
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=ROOT / "data" / "bybit_archive_cache"
    )
    parser.add_argument(
        "--report", type=Path, default=ROOT / "model_results" / "evaluation" / "bybit_archive_backfill_report.json"
    )
    parser.add_argument("--emit-interval-sec", type=float, default=15.0)
    parser.add_argument("--assumed-feed-latency-ms", type=int, default=1_000)
    parser.add_argument("--maximum-download-bytes", type=int, default=2_000_000_000)
    parser.add_argument("--keep-archives", action="store_true")
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="bounded validation mode; omit for the complete requested range",
    )
    args = parser.parse_args()
    if args.emit_interval_sec <= 0 or args.assumed_feed_latency_ms < 0:
        parser.error("invalid emit interval or assumed latency")
    today_utc = datetime.now(timezone.utc).date()
    if args.end >= today_utc:
        parser.error("end date must be a completed UTC day")
    symbols = tuple(dict.fromkeys(str(value).strip().upper() for value in args.symbols))
    if not symbols or any(
        re.fullmatch(r"[A-Z0-9]{2,24}USDT", value) is None for value in symbols
    ):
        parser.error("symbols must be explicit uppercase USDT contracts")
    work = [
        (kind, symbol, day)
        for day in _dates(args.start, args.end)
        for symbol in symbols
        for kind in args.kinds
    ]
    if args.max_files is not None:
        if args.max_files <= 0:
            parser.error("max-files must be positive")
        work = work[: args.max_files]

    store = BybitPublicPITStore(
        args.database,
        batch_writes=True,
        batch_max_operations=160,
        batch_max_interval_sec=0.10,
        busy_timeout_sec=300.0,
    )
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc)
    results: list[dict[str, object]] = []
    failures = 0
    try:
        for kind, symbol, day in work:
            if archive_already_completed(
                store,
                data_kind=kind,
                symbol=symbol,
                trading_date=day,
            ):
                results.append(
                    {
                        "data_kind": kind,
                        "symbol": symbol,
                        "trading_date": day.isoformat(),
                        "status": "skipped_already_completed",
                    }
                )
                continue
            url = (
                orderbook_archive_url(symbol, day)
                if kind == "orderbook"
                else trade_archive_url(symbol, day)
            )
            filename = url.rsplit("/", 1)[-1]
            target = args.cache_dir / filename
            fetched_at = datetime.now(timezone.utc)
            content_length = 0
            content_sha256 = ""
            try:
                content_length, content_sha256, fetched_at = download_official_archive(
                    url,
                    target,
                    maximum_bytes=args.maximum_download_bytes,
                )
                common = {
                    "symbol": symbol,
                    "trading_date": day,
                    "source_url": url,
                    "fetched_at": fetched_at,
                    "content_sha256": content_sha256,
                    "feature_emit_interval_sec": args.emit_interval_sec,
                    "assumed_feed_latency_ms": args.assumed_feed_latency_ms,
                }
                evidence = (
                    replay_orderbook_archive(store, target, **common)
                    if kind == "orderbook"
                    else replay_trade_archive(store, target, **common)
                )
                results.append(evidence.to_dict())
                if not args.keep_archives:
                    target.unlink()
            except Exception as exc:
                failures += 1
                evidence = record_archive_failure(
                    store,
                    data_kind=kind,
                    symbol=symbol,
                    trading_date=day,
                    source_url=url,
                    fetched_at=fetched_at,
                    error=f"{type(exc).__name__}: {exc}",
                    content_length=content_length,
                    content_sha256=content_sha256,
                )
                results.append(evidence.to_dict())
    finally:
        store.close()

    report = {
        "schema_version": "bybit-historical-archive-backfill.v1",
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "database": str(args.database.resolve()),
        "symbols": list(symbols),
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "requested_file_count": len(work),
        "failure_count": failures,
        "status": "PASS" if failures == 0 else "FAILED_PARTIAL",
        "report": str(args.report.resolve()),
        "pit_semantics": {
            "event_time": "exchange cts/ts from the official archive",
            "available_at": f"exchange event time plus {args.assumed_feed_latency_ms} ms conservative replay latency",
            "ingested_at": "actual archive fetch time",
            "claim": "historical_archive_replay_not_live_capture",
        },
        "derived_factor_semantics": {
            "fill_probability": "two-sided USD probe completion fraction against observed top-five depth; not queue-position or realized fill evidence",
            "expected_slippage_bps": "two-sided USD probe VWAP displacement against the observed midpoint",
            "ofi_1m": "rolling Cont-style best-level order-flow imbalance from every reconstructed L2 delta",
            "aggressive_cvd_1m": "rolling one-minute aggressor buy volume minus sell volume from official public trades",
            "execution_evidence_complete": False,
            "execution_evidence_blocker": "requires OOS shadow/testnet order receipts and queue/latency calibration",
        },
        "feature_sampling_interval_sec": args.emit_interval_sec,
        "results": results,
    }
    _atomic_json(args.report, report)
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "requested_file_count",
                    "failure_count",
                    "report",
                )
            },
            indent=2,
        )
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
