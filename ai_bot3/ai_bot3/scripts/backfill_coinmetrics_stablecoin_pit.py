from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.coinmetrics_stablecoin_pit import (
    CoinMetricsStablecoinPITStore,
    backfill_coinmetrics_stablecoin_pit,
)


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill PIT USDC/USDT net issuance from Coin Metrics"
    )
    parser.add_argument("--start", type=_date, required=True)
    parser.add_argument("--end", type=_date, required=True)
    parser.add_argument(
        "--database", type=Path, default=ROOT / "data" / "flow_pit.sqlite3"
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=ROOT / "data" / "coinmetrics_cache"
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=(
            ROOT
            / "model_results"
            / "evaluation"
            / "coinmetrics_stablecoin_backfill_report.json"
        ),
    )
    args = parser.parse_args()
    report = backfill_coinmetrics_stablecoin_pit(
        CoinMetricsStablecoinPITStore(args.database),
        cache_dir=args.cache_dir,
        observation_start=args.start,
        observation_end=args.end,
    )
    report["database"] = str(args.database.resolve())
    report["cache_dir"] = str(args.cache_dir.resolve())
    _atomic_json(args.report, report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "source_row_count": report["source_row_count"],
                "feature_observation_count": report["feature_observation_count"],
                "report": str(args.report.resolve()),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
