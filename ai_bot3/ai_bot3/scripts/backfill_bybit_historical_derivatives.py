from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.providers.bybit_historical_derivatives import (  # noqa: E402
    DATA_KINDS,
    audit_historical_derivative_window,
    historical_api_batch_completed,
    record_historical_api_failure,
    replay_derivative_day,
)
from core.providers.bybit_public_pit_store import BybitPublicPITStore  # noqa: E402


DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "1000PEPEUSDT",
)
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{2,30}USDT$")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def _days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill auditable Bybit funding, open-interest and mark/index basis "
            "from official public REST history. This does not provide liquidation "
            "or realised execution evidence."
        )
    )
    parser.add_argument("--start", required=True, type=_date)
    parser.add_argument("--end", required=True, type=_date)
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument("--kinds", nargs="+", choices=DATA_KINDS, default=list(DATA_KINDS))
    parser.add_argument(
        "--database", type=Path, default=ROOT / "data" / "bybit_public_pit.sqlite3"
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "model_results" / "evaluation" / "bybit_derivatives_backfill_report.json",
    )
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument(
        "--request-pause-sec",
        type=float,
        default=0.5,
        help="Minimum pause between day batches to protect the public REST rate budget.",
    )
    parser.add_argument("--max-batches", type=int)
    return parser.parse_args()


def _write_json_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temporary, path)


def main() -> int:
    args = parse_args()
    if args.start > args.end:
        raise SystemExit("--start must not be after --end")
    if args.end >= datetime.now(timezone.utc).date():
        raise SystemExit("--end must be a completed UTC trading day")
    if args.timeout_sec <= 0:
        raise SystemExit("--timeout-sec must be positive")
    if args.request_pause_sec < 0:
        raise SystemExit("--request-pause-sec must not be negative")
    if args.max_batches is not None and args.max_batches <= 0:
        raise SystemExit("--max-batches must be positive")
    symbols = tuple(dict.fromkeys(str(value).strip().upper() for value in args.symbols))
    if not symbols or any(not SYMBOL_PATTERN.fullmatch(symbol) for symbol in symbols):
        raise SystemExit("--symbols contains an invalid linear USDT symbol")
    kinds = tuple(dict.fromkeys(args.kinds))
    store = BybitPublicPITStore(args.database, busy_timeout_sec=300.0)
    records: list[dict[str, object]] = []
    attempted = 0
    completed = 0
    skipped = 0
    failed = 0
    stop = False
    for trading_date in _days(args.start, args.end):
        for symbol in symbols:
            for data_kind in kinds:
                if args.max_batches is not None and attempted >= args.max_batches:
                    stop = True
                    break
                if historical_api_batch_completed(
                    store,
                    data_kind=data_kind,
                    symbol=symbol,
                    trading_date=trading_date,
                ):
                    skipped += 1
                    records.append(
                        {
                            "data_kind": data_kind,
                            "symbol": symbol,
                            "trading_date": trading_date.isoformat(),
                            "status": "skipped_completed",
                        }
                    )
                    continue
                attempted += 1
                try:
                    evidence = replay_derivative_day(
                        store,
                        data_kind=data_kind,
                        symbol=symbol,
                        trading_date=trading_date,
                        timeout_sec=args.timeout_sec,
                    )
                except Exception as exc:
                    failed += 1
                    record_historical_api_failure(
                        store,
                        data_kind=data_kind,
                        symbol=symbol,
                        trading_date=trading_date,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    records.append(
                        {
                            "data_kind": data_kind,
                            "symbol": symbol,
                            "trading_date": trading_date.isoformat(),
                            "status": "failed",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                else:
                    completed += 1
                    records.append(evidence.to_dict())
                if args.request_pause_sec:
                    time.sleep(args.request_pause_sec)
            if stop:
                break
        if stop:
            break
    audit = audit_historical_derivative_window(
        store,
        start=args.start,
        end=args.end,
        symbols=symbols,
        data_kinds=kinds,
    )
    store.close()
    complete = bool(audit["complete"]) and failed == 0
    if complete:
        status = "PASS"
    elif int(audit["integrity_violation_count"]) > 0:
        status = "FAILED_INTEGRITY"
    else:
        status = "FAILED_INCOMPLETE"
    report = {
        "schema_version": "bybit-historical-derivatives-backfill.v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "database": str(args.database.resolve()),
        "requested_start": args.start.isoformat(),
        "requested_end": args.end.isoformat(),
        "symbols": list(symbols),
        "data_kinds": list(kinds),
        "attempted_batches": attempted,
        "completed_batches": completed,
        "skipped_completed_batches": skipped,
        "failed_batches": failed,
        "max_batches_reached": stop,
        "coverage_audit": audit,
        "status": status,
        "records": records,
        "pit_semantics": (
            "Official historical REST responses are hashed and replayed at exchange event "
            "time with conservative availability lag; ingested_at remains the actual fetch time."
        ),
        "limitations": {
            "liquidation_history_complete": False,
            "execution_evidence_complete": False,
            "candidate_authorized": False,
        },
    }
    _write_json_atomic(args.report, report)
    print(
        json.dumps(
            {
                "status": status,
                "attempted_batches": attempted,
                "completed_batches": completed,
                "skipped_completed_batches": skipped,
                "failed_batches": failed,
                "report": str(args.report.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
