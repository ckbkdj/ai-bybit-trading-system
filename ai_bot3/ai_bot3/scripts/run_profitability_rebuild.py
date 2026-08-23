from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import (
    ProfitabilityRebuild,
    ProfitabilityRebuildConfig,
    write_failed_outputs,
)


def _local_head_commit() -> str:
    git_dir = WORKSPACE / ".version-history"
    head_file = git_dir / "HEAD"
    if not head_file.exists():
        raise RuntimeError("local version-history HEAD is missing")
    value = head_file.read_text(encoding="utf-8").strip()
    if value.startswith("ref: "):
        reference = git_dir / value.removeprefix("ref: ")
        if not reference.exists():
            raise RuntimeError(f"local version-history reference is missing: {reference}")
        value = reference.read_text(encoding="utf-8").strip()
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value.lower()):
        raise RuntimeError("local version-history HEAD is not a SHA-1 commit")
    return value.lower()


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
    parser.add_argument("--code-commit")
    configured_trad_root = os.environ.get("TRAD_DATA_SERVICE_ROOT", "").strip()
    parser.add_argument(
        "--trad-panel-root",
        type=Path,
        default=Path(configured_trad_root) if configured_trad_root else None,
    )
    parser.add_argument("--max-bars-per-symbol", type=int, default=200_000)
    parser.add_argument("--walk-forward-folds", type=int, default=3)
    args = parser.parse_args()
    head_commit = _local_head_commit()
    if args.code_commit and args.code_commit.lower() != head_commit:
        raise SystemExit(
            f"--code-commit does not match the running repository HEAD: {head_commit}"
        )
    config = ProfitabilityRebuildConfig(
        feature_store_path=args.feature_store,
        output_dir=args.output_dir,
        trial_ledger_path=args.trial_ledger,
        model_output_dir=args.model_output_dir,
        code_commit=head_commit,
        trad_panel_root=args.trad_panel_root,
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
