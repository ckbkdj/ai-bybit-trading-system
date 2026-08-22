"""Non-destructively rebuild the versioned K-line feature store.

The damaged database is opened read-only.  Raw candles are copied in bounded
batches, recent legacy caches are merged, derived features are recomputed from
the current feature definition, and the new database must pass ``quick_check``.
This tool never replaces the configured production path automatically.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pandas as pd
import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AI_ROOT = REPOSITORY_ROOT / "ai_bot3" / "ai_bot3"
if str(AI_ROOT) not in sys.path:
    sys.path.insert(0, str(AI_ROOT))

from core.kline_feature_store import KlineFeatureStore, timeframe_ms  # noqa: E402


@contextmanager
def read_only_connection(path: Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        yield connection
    finally:
        connection.close()


def valid_candles(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Reject impossible rows instead of carrying corruption into the rebuild."""

    if frame.empty:
        return frame, 0
    numeric = ["open", "high", "low", "close", "volume"]
    out = frame.copy()
    for column in numeric:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    finite = np.isfinite(out[numeric].to_numpy()).all(axis=1)
    valid = (
        finite
        & (out[["open", "high", "low", "close"]] > 0).all(axis=1)
        & (out["volume"] >= 0)
        & (out["high"] >= out[["open", "close", "low"]].max(axis=1))
        & (out["low"] <= out[["open", "close", "high"]].min(axis=1))
    )
    rejected = int((~valid).sum())
    return out.loc[valid].reset_index(drop=True), rejected


def copy_raw_store(
    source: Path,
    target: KlineFeatureStore,
    *,
    batch_size: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    copied: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with read_only_connection(source) as connection:
        groups = connection.execute(
            "SELECT DISTINCT symbol,timeframe,source FROM raw_kline ORDER BY symbol,timeframe,source"
        ).fetchall()
        for group in groups:
            symbol, timeframe, source_name = map(str, group)
            if source_name != target.source:
                failures.append(
                    {
                        "group": f"{symbol}-{timeframe}-{source_name}",
                        "error": f"source mismatch; target source is {target.source}",
                    }
                )
                continue
            last_open_time = -1
            group_rows = 0
            rejected_rows = 0
            try:
                while True:
                    rows = connection.execute(
                        """SELECT open_time,close_time,open,high,low,close,volume
                           FROM raw_kline
                           WHERE symbol=? AND timeframe=? AND source=? AND open_time>?
                           ORDER BY open_time LIMIT ?""",
                        (symbol, timeframe, source_name, last_open_time, batch_size),
                    ).fetchall()
                    if not rows:
                        break
                    frame, rejected = valid_candles(pd.DataFrame([dict(row) for row in rows]))
                    rejected_rows += rejected
                    if not frame.empty:
                        group_rows += target.upsert_raw_frame(symbol, timeframe, frame)
                    last_open_time = int(rows[-1]["open_time"])
                copied.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "source": source_name,
                        "rows_written": group_rows,
                        "rows_rejected": rejected_rows,
                    }
                )
            except (sqlite3.DatabaseError, ValueError, OSError) as exc:
                failures.append(
                    {
                        "group": f"{symbol}-{timeframe}-{source_name}",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    return copied, failures


def merge_legacy_caches(
    legacy_dir: Path,
    target: KlineFeatureStore,
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    symbols = [str(item) for item in (cfg.get("general") or {}).get("symbols") or []]
    timeframes = sorted({str(values[0]) for values in (cfg.get("modes") or {}).values()})
    for symbol in symbols:
        path = legacy_dir / f"{symbol}.sqlite"
        if not path.is_file():
            merged.append({"symbol": symbol, "status": "missing_legacy_cache"})
            continue
        with read_only_connection(path) as connection:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            for timeframe in timeframes:
                table = f"k_{timeframe}"
                if table not in tables:
                    continue
                frame = pd.read_sql_query(
                    f'SELECT ts,open,high,low,close,volume FROM "{table}" ORDER BY ts',
                    connection,
                )
                frame, rejected = valid_candles(frame)
                if not frame.empty:
                    timestamp = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
                    frame = frame.loc[timestamp.notna()].copy()
                    timestamp = timestamp.loc[timestamp.notna()]
                    frame["open_time"] = timestamp.map(lambda value: int(value.timestamp() * 1000))
                    frame["close_time"] = frame["open_time"] + timeframe_ms(timeframe)
                written = target.upsert_raw_frame(symbol, timeframe, frame)
                merged.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "rows_seen": int(len(frame)),
                        "rows_written": int(written),
                        "rows_rejected": rejected,
                    }
                )
    return merged


def recompute_features(target: KlineFeatureStore) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for spec in target.load_mode_specs():
        key = (spec.symbol, spec.base_timeframe, spec.feature_version, spec.config_hash)
        if key in seen:
            continue
        seen.add(key)
        rows = target.update_enhanced_kline(spec.symbol, spec.base_timeframe, spec)
        results.append(
            {"symbol": spec.symbol, "timeframe": spec.base_timeframe, "rows_written": rows}
        )
    return results


def quick_check(path: Path) -> list[str]:
    with read_only_connection(path) as connection:
        return [str(row[0]) for row in connection.execute("PRAGMA quick_check")]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--legacy-dir", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=25_000)
    parser.add_argument("--raw-only", action="store_true")
    args = parser.parse_args()

    if not args.source.is_file() or not args.config.is_file():
        parser.error("source database and config must exist")
    if args.target.exists():
        parser.error("target already exists; choose a new path so no evidence is overwritten")
    if args.target.resolve() == args.source.resolve():
        parser.error("source and target must differ")
    if args.batch_size <= 0:
        parser.error("batch-size must be positive")

    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8")) or {}
    target = KlineFeatureStore(args.target, cfg, source="binance")
    copied, failures = copy_raw_store(args.source, target, batch_size=args.batch_size)
    merged = (
        merge_legacy_caches(args.legacy_dir, target, cfg)
        if args.legacy_dir is not None
        else []
    )
    enhanced = [] if args.raw_only or failures else recompute_features(target)
    checks = quick_check(args.target)
    payload = {
        "schema_version": "kline-feature-store-rebuild.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_opened_read_only": True,
        "source": str(args.source.resolve()),
        "target": str(args.target.resolve()),
        "raw_groups": copied,
        "legacy_merge": merged,
        "copy_failures": failures,
        "enhanced_groups": enhanced,
        "target_quick_check": checks,
        "production_path_changed": False,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0 if checks == ["ok"] and not failures and (args.raw_only or enhanced) else 2


if __name__ == "__main__":
    raise SystemExit(main())
