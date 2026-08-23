from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.fred_alfred_pit import FredAlfredPITStore, backfill_fred_alfred_pit


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def _key_from_env_file(path: Path) -> str:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        name, value = text.split("=", 1)
        values[name.strip()] = value.strip().strip("\"'")
    return values.get("TRAD_FRED_API_KEY") or values.get("FRED_API_KEY") or ""


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill strict-PIT FRED/ALFRED factors")
    parser.add_argument("--start", type=_date, required=True)
    parser.add_argument("--end", type=_date, required=True)
    parser.add_argument("--database", type=Path, default=ROOT / "data" / "macro_pit.sqlite3")
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "data" / "fred_alfred_cache")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "model_results" / "evaluation" / "fred_alfred_backfill_report.json",
    )
    args = parser.parse_args()
    key = os.environ.get("TRAD_FRED_API_KEY") or os.environ.get("FRED_API_KEY") or ""
    secret_source = "process_environment"
    if not key and args.env_file is not None:
        key = _key_from_env_file(args.env_file)
        secret_source = "operator_env_file"
    if not key:
        parser.error("TRAD_FRED_API_KEY/FRED_API_KEY is not configured")
    report = backfill_fred_alfred_pit(
        FredAlfredPITStore(args.database),
        cache_dir=args.cache_dir,
        api_key=key,
        observation_start=args.start,
        observation_end=args.end,
    )
    report["database"] = str(args.database.resolve())
    report["cache_dir"] = str(args.cache_dir.resolve())
    report["secret_source"] = secret_source
    _atomic_json(args.report, report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "response_count": report["response_count"],
                "vintage_row_count": report["vintage_row_count"],
                "feature_observation_count": report["feature_observation_count"],
                "report": str(args.report.resolve()),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
