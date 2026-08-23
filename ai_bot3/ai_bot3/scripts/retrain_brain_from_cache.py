from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.brain_model import train_brain_from_df
from core.kline_feature_store import KlineFeatureStore


def _load_frame(db_path: Path, timeframe: str) -> pd.DataFrame:
    table = f"k_{timeframe}"
    uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if table not in tables:
            raise ValueError(f"cache table is missing: {table}")
        frame = pd.read_sql_query(f'SELECT * FROM "{table}" ORDER BY ts', connection)
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["ts"]).drop_duplicates(subset=["ts"], keep="last")
    frame = frame.set_index("ts").sort_index()
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description="Retrain governed Brain candidates from local OHLCV caches")
    parser.add_argument("--config", type=Path, default=ROOT / "config.yml")
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--modes", nargs="*")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--feature-store",
        type=Path,
        help="Versioned KlineFeatureStore path; defaults to general.kline_feature_store_path",
    )
    parser.add_argument(
        "--legacy-cache-diagnostics",
        action="store_true",
        help="Inspect old per-symbol cache rows without training or promotion",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "model_results" / "evaluation" / "brain_retrain_report.json",
    )
    args = parser.parse_args()
    config = yaml.safe_load(args.config.read_text(encoding="utf-8")) or {}
    symbols = args.symbols or list((config.get("general") or {}).get("symbols") or [])
    modes = args.modes or list((config.get("modes") or {}).keys())
    db_dir = Path((config.get("general") or {}).get("db_dir") or ROOT / "data")
    if not db_dir.is_absolute():
        db_dir = ROOT / db_dir
    feature_store_path = args.feature_store or Path(
        (config.get("general") or {}).get("kline_feature_store_path")
        or ROOT / "data" / "kline_feature_store.sqlite3"
    )
    if not feature_store_path.is_absolute():
        feature_store_path = ROOT / feature_store_path
    feature_store = None
    if not args.legacy_cache_diagnostics:
        # Corruption, missing schema, or feature-version mismatch must abort the run.
        feature_store = KlineFeatureStore(feature_store_path, config, read_only=True)

    results: list[dict] = []
    for symbol in symbols:
        db_path = db_dir / f"{symbol}.sqlite"
        for mode in modes:
            definition = (config.get("modes") or {}).get(mode)
            if not definition:
                results.append({"symbol": symbol, "mode": mode, "status": "missing_mode_config"})
                continue
            timeframe = str(definition[0])
            try:
                if args.legacy_cache_diagnostics:
                    frame = _load_frame(db_path, timeframe)
                    results.append(
                        {
                            "symbol": symbol,
                            "mode": mode,
                            "status": "legacy_cache_diagnostic_only",
                            "promote_decision": "shadow",
                            "rows": int(len(frame)),
                            "source_contract": "legacy_symbol_cache_non_deployable",
                        }
                    )
                    continue
                spec = feature_store.spec_for(symbol, mode)
                attrition = feature_store.dataset_attrition(spec)
                built = feature_store.build_mode_dataset(spec)
                if attrition.split_status != "ready" or built.signature.row_count <= 0:
                    raise RuntimeError(
                        f"versioned dataset gate failed: {attrition.split_status} "
                        f"rows={built.signature.row_count}"
                    )
                frame = built.df
                metadata = train_brain_from_df(
                    frame,
                    symbol,
                    timeframe,
                    mode,
                    config,
                    force=args.force,
                )
                results.append(
                    {
                        "symbol": symbol,
                        "mode": mode,
                        "source_contract": "versioned_kline_feature_store",
                        "feature_store": str(feature_store_path.resolve()),
                        "feature_store_signature": built.signature.digest(),
                        "dataset_attrition": attrition.__dict__,
                        **metadata,
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "symbol": symbol,
                        "mode": mode,
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "force": bool(args.force),
        "legacy_cache_diagnostics": bool(args.legacy_cache_diagnostics),
        "results": results,
        "counts": {
            status: sum(1 for item in results if item.get("promote_decision") == status)
            for status in ("candidate", "shadow", "rejected")
        },
        "failures": sum(1 for item in results if item.get("status") == "failed"),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps(payload["counts"], ensure_ascii=False))
    print(f"failures={payload['failures']} report={args.output}")
    return 1 if payload["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
