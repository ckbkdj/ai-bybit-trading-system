from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import (
    ProfitabilityRebuild,
    ProfitabilityRebuildConfig,
    write_failed_outputs,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Profitability-first pooled alpha rebuild")
    parser.add_argument(
        "--feature-store",
        type=Path,
        default=ROOT / "data" / "kline_feature_store.rebuilt.20260822.sqlite3",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=ROOT / "model_results" / "evaluation"
    )
    parser.add_argument(
        "--trial-ledger", type=Path, default=ROOT / "data" / "research_trials.sqlite3"
    )
    parser.add_argument(
        "--model-output-dir", type=Path, default=ROOT / "models" / "profitability"
    )
    parser.add_argument("--code-commit", default="unknown")
    parser.add_argument("--max-bars-per-symbol", type=int, default=3000)
    parser.add_argument("--walk-forward-folds", type=int, default=3)
    args = parser.parse_args()
    config = ProfitabilityRebuildConfig(
        feature_store_path=args.feature_store,
        output_dir=args.output_dir,
        trial_ledger_path=args.trial_ledger,
        model_output_dir=args.model_output_dir,
        code_commit=args.code_commit,
        max_bars_per_symbol=args.max_bars_per_symbol,
        walk_forward_folds=args.walk_forward_folds,
    )
    runner = None
    try:
        runner = ProfitabilityRebuild(config)
        result = runner.run()
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        if runner is not None:
            runner.record_failure(reason)
        result = write_failed_outputs(args.output_dir, reason=reason)
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False, default=str))
        return 1
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False, default=str))
    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
