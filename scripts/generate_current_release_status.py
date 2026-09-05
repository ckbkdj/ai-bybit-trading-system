"""Generate the fail-closed current release status for the exact checkout."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shadow_contracts.repository import resolve_code_commit  # noqa: E402


DEFAULT_OUTPUT = (
    ROOT
    / "ai_bot3"
    / "ai_bot3"
    / "model_results"
    / "evaluation"
    / "current_release_status.json"
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    payload = {
        "schema_version": "current-release-status.v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "code_commit": resolve_code_commit(ROOT),
        "profitability_evidence": "STALE_NOT_REGENERATED",
        "profitability_gate": "FAILED",
        "candidate_count": 0,
        "live_count": 0,
        "mainnet_allowed": False,
        "success_claim": False,
        "note": (
            "No current profitability evaluation was run. Zero candidates and zero live "
            "releases are a fail-closed state, not a profitability success."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
