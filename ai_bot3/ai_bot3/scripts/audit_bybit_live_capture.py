from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_capture_audit import audit_live_capture
from core.providers.bybit_public_pit_store import BybitPublicPITStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seal stopped Bybit public-WebSocket journals into continuity evidence."
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--maximum-gap-sec", type=float, default=90.0)
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    store = BybitPublicPITStore(args.database, busy_timeout_sec=120.0)
    try:
        evidence = audit_live_capture(store, maximum_gap_sec=args.maximum_gap_sec)
    finally:
        store.close()
    report = {
        "schema_version": "bybit-live-capture-audit.v1",
        "database": str(args.database.resolve()),
        "evidence": evidence.to_dict(),
        "release_claim": (
            "hashed stopped public-WebSocket activity intervals; profitability and "
            "execution evidence remain separate gates"
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
