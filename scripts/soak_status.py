from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT_ROOT = ROOT / "BybitContractBotV4"
if str(BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(BOT_ROOT))

from soak_monitor import evaluate_soak  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the 30-day execution soak SLO")
    parser.add_argument("--db", required=True, type=Path)
    args = parser.parse_args()
    result = evaluate_soak(args.db)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
