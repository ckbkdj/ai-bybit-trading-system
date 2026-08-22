from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.historical_strategy_audit import (
    PortfolioAuditConfig,
    audit_portfolio,
    load_settled_predictions,
)
from core.evaluation.statistical_governance import TrialLedger


def _legacy_run_count(path: Path) -> int:
    if not path.exists():
        return 0
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM brain_training_runs").fetchone()[0])
    except sqlite3.Error:
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Cost-aware historical strategy evidence audit")
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "online_learning.sqlite3")
    parser.add_argument("--trial-db", type=Path, default=ROOT / "data" / "research_trials.sqlite3")
    parser.add_argument(
        "--legacy-history-db",
        type=Path,
        default=ROOT / "data" / "brain_training_history.sqlite3",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "model_results" / "evaluation" / "strategy_audit.json")
    args = parser.parse_args()

    rows = load_settled_predictions(args.db)
    governed_trial_count = TrialLedger(args.trial_db).trial_count() if args.trial_db.exists() else 0
    legacy_run_count = _legacy_run_count(args.legacy_history_db)
    # Legacy rows mix real fits, skipped signatures and scheduler repeats, so
    # they are not independent trials. Treating all as selection events is a
    # deliberately conservative sensitivity bound, never a precise PBO input.
    trial_count = max(1, governed_trial_count, legacy_run_count)
    strict = audit_portfolio(rows, PortfolioAuditConfig(), trial_count=trial_count)
    diagnostic = audit_portfolio(
        rows,
        PortfolioAuditConfig(
            minimum_signal_return=0.0,
            require_recorded_direction=False,
            require_confidence=False,
            require_model_version=False,
        ),
        trial_count=trial_count,
    )
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_db": str(args.db.resolve()),
        "selection_bias_counts": {
            "governed_actual_trials": governed_trial_count,
            "legacy_training_run_events": legacy_run_count,
            "conservative_count_used_for_deflation": trial_count,
            "independent_trial_count_known": False,
        },
        "strict_live_gate_replay": strict,
        "legacy_direction_inference_diagnostic_only": diagnostic,
        "conclusion": (
            "profitability_not_demonstrated"
            if strict["eligible_trades"] == 0
            else "requires_independent_review"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
