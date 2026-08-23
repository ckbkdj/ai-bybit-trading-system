from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


TRADE_ROOT = Path(__file__).resolve().parents[1] / "BybitContractBotV4"
sys.path.insert(0, str(TRADE_ROOT))

from bybit import BybitClient
from bybit_executor import BybitExecutor
from execution_reporter import ExecutionReporter
from risk_guard import AccountSnapshot, MarketSnapshot, PortfolioSnapshot, SystemHealth
from sizing import InstrumentRules
from ticket_client import TicketHttpClient, deterministic_lease_token
from ticket_consumer import TicketConsumer
from ticket_store import ExecutionStore


class ShadowContext:
    def market(self, ticket):
        return MarketSnapshot(
            ticket.instrument.symbol, 100000, 99995, 100005,
            ticket.guards.observed_market_regime, datetime.now(timezone.utc),
            cross_exchange_basis_bps=0,
        )

    def account(self, ticket):
        return AccountSnapshot(10000, 10000, 0)

    def portfolio(self, ticket):
        return PortfolioSnapshot(0, 0, ticket.guards.required_position_version, 0)

    def health(self, ticket):
        return SystemHealth("shadow", False, False, True, 0)

    def instrument_rules(self, ticket):
        return InstrumentRules(
            ticket.instrument.symbol, Decimal("0.001"), Decimal("0.001"),
            Decimal("0.1"), Decimal("5"),
        )


def main(base_url: str, db_path: str) -> int:
    client = TicketHttpClient(base_url, timeout_seconds=5)
    items = client.fetch(0, "shadow-e2e", 10)
    if len(items) != 1:
        raise RuntimeError(f"expected one ticket, received {len(items)}")
    item = items[0]
    lease = deterministic_lease_token("shadow-e2e", item.ticket.ticket_id)
    claim_epoch = client.claim(item.ticket.ticket_id, "shadow-e2e", lease)
    if claim_epoch is None:
        raise RuntimeError("remote claim failed")
    store = ExecutionStore(Path(db_path))
    exchange = BybitClient(mode="shadow")
    consumer = TicketConsumer(
        consumer_id="shadow-e2e",
        store=store,
        context=ShadowContext(),
        executor=BybitExecutor(exchange, store),
    )
    state = consumer.process(
        item.ticket, claim_epoch=claim_epoch, lease_token=lease
    )
    receipt = ExecutionReporter(store, "shadow-e2e", "shadow").build(item.ticket.ticket_id)
    if not client.post_receipt(receipt):
        raise RuntimeError("receipt delivery failed")
    output = {
        "ticket_id": item.ticket.ticket_id,
        "state": state.value,
        "shadow_order_count": len(exchange.exchange.orders),
        "cursor": item.cursor,
        "receipt_id": receipt.receipt_id,
        "reason_code": store.ticket_record(item.ticket.ticket_id).get("reason_code"),
        "reason_detail": store.ticket_record(item.ticket.ticket_id).get("reason_detail"),
    }
    print(json.dumps(output, sort_keys=True))
    return 0 if output["state"] == "SUBMITTED" and output["shadow_order_count"] == 1 else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
