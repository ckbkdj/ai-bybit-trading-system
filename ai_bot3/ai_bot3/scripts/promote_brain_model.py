from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main() -> int:
    parser = argparse.ArgumentParser(description="Disabled Brain promotion command")
    parser.add_argument("symbol")
    parser.add_argument("mode")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--model-dir", type=Path, default=ROOT / "models" / "brain")
    args = parser.parse_args()

    raise SystemExit(
        "Brain promotion is disabled: all Brain direction classifiers are baseline-only "
        "and rejected until a separate profitability_two_stage release passes the untouched lockbox gate"
    )


if __name__ == "__main__":
    raise SystemExit(main())
