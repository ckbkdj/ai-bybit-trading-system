from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_capture_audit import merge_audited_liquidation_capture


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append a sealed Bybit liquidation snapshot to a historical development PIT store."
        )
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--audit-id")
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    evidence = merge_audited_liquidation_capture(
        args.source,
        args.destination,
        audit_id=args.audit_id,
    )
    report = {
        "schema_version": "bybit-liquidation-capture-import.v1",
        "evidence": evidence.to_dict(),
        "copy_policy": (
            "append-only audited liquidation raw events, corrected PIT observations, "
            "invalidations, sessions and continuity receipts; conflicts fail closed"
        ),
        "candidate_authorized": False,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
