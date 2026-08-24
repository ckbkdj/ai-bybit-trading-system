from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Deque


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.providers.bybit_public_pit import BybitPublicPITStore  # noqa: E402


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("stored liquidation timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Append corrected Bybit allLiquidation side-v2 features from retained raw "
            "events. V1 observations remain stored but are invalidated, never deleted."
        )
    )
    parser.add_argument(
        "--database", type=Path, default=ROOT / "data" / "bybit_public_pit.sqlite3"
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT
        / "model_results"
        / "evaluation"
        / "bybit_liquidation_semantics_rebuild_report.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    store = BybitPublicPITStore(args.database)
    with store.connect() as connection:
        rows = connection.execute(
            """SELECT event_id,symbol,exchange_time,received_at,payload_json
                 FROM bybit_raw_public_events
                WHERE event_type='liquidation' AND topic LIKE 'allLiquidation.%'
                ORDER BY received_at,sequence"""
        ).fetchall()
        invalidated = int(
            connection.execute(
                """SELECT COUNT(*) FROM bybit_feature_invalidations
                    WHERE correction_version='bybit-liquidation-side-v2'"""
            ).fetchone()[0]
        )
    rolling: dict[str, Deque[tuple[datetime, float, str]]] = defaultdict(deque)
    inserted = 0
    duplicates = 0
    ingested_at = datetime.now(timezone.utc)
    first_event: datetime | None = None
    last_event: datetime | None = None
    for row in rows:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise ValueError("stored liquidation payload is not an object")
        symbol = str(row["symbol"]).upper()
        side = str(payload.get("S"))
        if side not in {"Buy", "Sell"}:
            raise ValueError("stored liquidation side is outside the Bybit contract")
        price = float(payload["p"])
        volume = float(payload["v"])
        if not math.isfinite(price) or not math.isfinite(volume) or price <= 0 or volume <= 0:
            raise ValueError("stored liquidation price/volume is invalid")
        event_time = _datetime(str(row["exchange_time"]))
        received_at = _datetime(str(row["received_at"]))
        if event_time > received_at:
            raise ValueError("stored liquidation chronology is invalid")
        position = "long" if side == "Buy" else "short"
        history = rolling[symbol]
        history.append((received_at, price * volume, position))
        cutoff = received_at - timedelta(minutes=5)
        while history and history[0][0] < cutoff:
            history.popleft()
        long_value = sum(value for _, value, item_side in history if item_side == "long")
        short_value = sum(value for _, value, item_side in history if item_side == "short")
        total = long_value + short_value
        imbalance = (short_value - long_value) / total if total else 0.0
        created = store.append_feature(
            event_id=f"{row['event_id']}:bybit-liquidation-side-v2",
            symbol=symbol,
            name="liquidation_imbalance_5m",
            value=imbalance,
            unit="ratio",
            event_time=event_time,
            received_at=received_at,
            ingested_at=ingested_at,
            source="bybit.public.liquidations.v2",
            quality=1.0,
        )
        inserted += int(created)
        duplicates += int(not created)
        first_event = min(first_event or event_time, event_time)
        last_event = max(last_event or event_time, event_time)
    store.close()
    report = {
        "schema_version": "bybit-liquidation-side-correction.v2",
        "generated_at": ingested_at.isoformat().replace("+00:00", "Z"),
        "database": str(args.database.resolve()),
        "official_side_contract": {
            "Buy": "long_position_liquidated",
            "Sell": "short_position_liquidated",
        },
        "old_observations_deleted": False,
        "invalidated_v1_observations": invalidated,
        "raw_events_read": len(rows),
        "v2_observations_inserted": inserted,
        "v2_observations_already_present": duplicates,
        "first_event_time": (
            first_event.isoformat().replace("+00:00", "Z") if first_event else None
        ),
        "last_event_time": (
            last_event.isoformat().replace("+00:00", "Z") if last_event else None
        ),
        "candidate_authorized": False,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
