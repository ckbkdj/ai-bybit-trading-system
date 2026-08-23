from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_public_pit import BybitPublicPITCollector, BybitPublicPITStore


async def _run(args: argparse.Namespace) -> None:
    store = BybitPublicPITStore(args.database)
    collector = BybitPublicPITCollector(
        store,
        args.symbol,
        orderbook_depth=args.orderbook_depth,
    )
    print(
        json.dumps(
            {
                "mode": "public_market_data_capture_only",
                "authentication": False,
                "trading": False,
                "database": str(args.database.resolve()),
                "symbols": list(collector.symbols),
                "topics": collector.topics,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if args.run_seconds is None:
        await collector.run_forever()
    else:
        await asyncio.wait_for(collector.run_forever(), timeout=args.run_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture unauthenticated Bybit public PIT market data; never place orders"
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=ROOT / "data" / "bybit_public_pit.sqlite3",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Bybit linear symbol; repeat for multiple symbols",
    )
    parser.add_argument("--orderbook-depth", type=int, default=50)
    parser.add_argument("--run-seconds", type=float)
    args = parser.parse_args()
    if not args.symbol:
        args.symbol = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "1000PEPEUSDT"]
    try:
        asyncio.run(_run(args))
    except (asyncio.TimeoutError, KeyboardInterrupt):
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
