from __future__ import annotations

import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(WORKSPACE))

from exchange_gateway import ExchangeGateway
from bybit_executor import BybitExecutor
from contracts.common import order_link_id
from contracts.operation_ticket_v1 import OperationTicket
from execution_reconciler import ExecutionReconciler
from execution_reporter import ExecutionReporter
from execution_state import ExecutionState
from private_stream import PrivateStreamHandler
from rate_limiter import EndpointRateLimiter, RateLimitBlocked
from risk_guard import (
    AccountSnapshot,
    MarketSnapshot,
    PortfolioSnapshot,
    RiskGuard,
    SystemHealth,
)
from sizing import InstrumentRules, PositionSizer
from runtime_context import BybitRuntimeContext
from service_main import TradingExecutionService
from ticket_consumer import TicketConsumer
from ticket_store import ExecutionStore


NOW = datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)


def ticket_payload(ticket_id="tk_execution_test_001", *, position_version=3, reference_price=100000):
    return {
        "ticket_id": ticket_id,
        "forecast_id": "fc_execution_test_001",
        "forecast_revision": 1,
        "created_at": NOW,
        "valid_from": NOW,
        "expires_at": NOW + timedelta(minutes=5),
        "instrument": {"symbol": "BTCUSDT"},
        "intent": {
            "action": "OPEN",
            "side": "BUY",
            "position_effect": "OPEN_OR_INCREASE",
            "target_exposure_pct": 0.08,
            "risk_budget_pct": 0.003,
            "max_notional_usdt": 5000,
            "leverage_cap": 3,
        },
        "entry": {
            "order_type": "LIMIT",
            "reference_price": reference_price,
            "limit_price": reference_price - 10,
            "price_band_bps": 12,
            "max_slippage_bps": 6,
            "max_wait_sec": 90,
        },
        "protection": {
            "stop_loss": {"type": "MARK_PRICE", "price": 99000, "max_loss_bps": 100},
            "take_profit": [{"price": 102000, "close_fraction": 0.8}],
            "max_holding_sec": 14400,
        },
        "economics": {
            "expected_return_bps": 70,
            "estimated_fee_bps": 10,
            "estimated_slippage_bps": 5,
            "estimated_funding_bps": 2,
            "model_error_buffer_bps": 13,
            "expected_return_after_cost_bps": 40,
        },
        "guards": {
            "min_data_quality": 0.9,
            "observed_data_quality": 0.96,
            "max_feature_age_sec": 120,
            "observed_feature_age_sec": 10,
            "max_live_spread_bps": 10,
            "max_live_price_deviation_bps": 18,
            "required_market_regime": ["risk_on"],
            "observed_market_regime": "risk_on",
            "required_position_version": position_version,
            "require_flat_position": True,
        },
        "reason": {"regime": "risk_on"},
    }


class StaticContext:
    def __init__(self):
        self.market_snapshot = MarketSnapshot(
            symbol="BTCUSDT",
            last_price=100000,
            bid_price=99995,
            ask_price=100005,
            market_regime="risk_on",
            captured_at=NOW,
        )
        self.account_snapshot = AccountSnapshot(10000, 9000, 100)
        self.portfolio_snapshot = PortfolioSnapshot(0, 0, 3, 0)
        self.health_snapshot = SystemHealth("shadow", False, False, True, 0.1)
        self.rules = InstrumentRules(
            "BTCUSDT",
            min_qty=Decimal("0.001"),
            qty_step=Decimal("0.001"),
            tick_size=Decimal("0.1"),
            min_notional_usdt=Decimal("5"),
        )

    def market(self, ticket):
        return self.market_snapshot

    def account(self, ticket):
        return self.account_snapshot

    def portfolio(self, ticket):
        return self.portfolio_snapshot

    def health(self, ticket):
        return self.health_snapshot

    def instrument_rules(self, ticket):
        return self.rules


class FakeReconciliationGateway:
    def __init__(self, remote=None, fills=None):
        self.remote = remote
        self.fills = fills or []
        self.cancelled = []

    def find_order(self, symbol, order_link_id):
        return self.remote

    def fetch_executions(self, symbol, order_link_id):
        return list(self.fills)

    def cancel_order(self, symbol, bybit_order_id):
        self.cancelled.append((symbol, bybit_order_id))
        return {"id": bybit_order_id, "status": "canceled"}


class FakeClock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value


class ExecutionEngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = ExecutionStore(Path(self.temp.name) / "execution.sqlite3")
        self.client = ExchangeGateway(mode="shadow")
        self.context = StaticContext()
        self.executor = BybitExecutor(self.client, self.store)
        self.consumer = TicketConsumer(
            consumer_id="consumer-a",
            store=self.store,
            context=self.context,
            executor=self.executor,
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_duplicate_ticket_never_creates_second_order(self):
        ticket = OperationTicket.model_validate(ticket_payload())
        self.assertEqual(self.consumer.process(ticket, now=NOW), ExecutionState.SUBMITTED)
        self.assertEqual(self.consumer.process(ticket, now=NOW), ExecutionState.SUBMITTED)
        self.assertEqual(len(self.client.exchange.orders), 1)
        self.assertEqual(len(self.store.orders_for_ticket(ticket.ticket_id)), 1)
        params = self.client.exchange.orders[0]["params"]
        self.assertEqual(Decimal(params["stopLoss"]), Decimal("99000"))
        self.assertEqual(params["slTriggerBy"], "MarkPrice")

    def test_equity_high_water_drawdown_blocks_new_risk(self):
        ticket = OperationTicket.model_validate(ticket_payload("tk_drawdown_guard_001"))
        self.context.account_snapshot = replace(
            self.context.account_snapshot,
            equity_high_water_usdt=12_000,
        )
        state = self.consumer.process(ticket, now=NOW)
        self.assertEqual(state, ExecutionState.RISK_BLOCKED)
        self.assertEqual(
            self.store.ticket_record(ticket.ticket_id)["reason_code"],
            "EQUITY_DRAWDOWN_LIMIT",
        )

    def test_equity_high_water_is_monotonic_and_persistent(self):
        self.assertEqual(self.store.observe_equity(10_000), 10_000)
        self.assertEqual(self.store.observe_equity(9_500), 10_000)
        reopened = ExecutionStore(Path(self.temp.name) / "execution.sqlite3")
        self.assertEqual(reopened.observe_equity(9_000), 10_000)

    def test_rest_success_does_not_mark_filled_and_fill_events_are_idempotent(self):
        ticket = OperationTicket.model_validate(ticket_payload())
        self.consumer.process(ticket, now=NOW)
        self.assertEqual(self.store.state(ticket.ticket_id), ExecutionState.SUBMITTED)
        link = order_link_id(ticket.ticket_id)
        order = self.store.order(link)
        half = order["quantity"] / 2
        stream = PrivateStreamHandler(self.store)
        stream.on_order({"data": [{"orderLinkId": link, "orderStatus": "New"}]})
        self.assertEqual(self.store.state(ticket.ticket_id), ExecutionState.ACKNOWLEDGED)
        event = {
            "execId": "exec-1",
            "orderLinkId": link,
            "orderId": order["bybit_order_id"],
            "execQty": half,
            "execPrice": 99990,
            "execFee": 0.1,
            "execTime": int(NOW.timestamp() * 1000),
        }
        stream.on_execution({"data": [event]})
        stream.on_execution({"data": [event]})
        self.assertEqual(self.store.state(ticket.ticket_id), ExecutionState.PARTIALLY_FILLED)
        self.assertAlmostEqual(self.store.order(link)["cum_exec_qty"], half)
        second = dict(event, execId="exec-2", execQty=order["quantity"] - half)
        stream.on_execution({"data": [second]})
        self.assertEqual(self.store.state(ticket.ticket_id), ExecutionState.FILLED)
        self.assertAlmostEqual(self.store.order(link)["cum_exec_qty"], order["quantity"])
        self.assertEqual(len(self.store.fills_for_ticket(ticket.ticket_id)), 2)
        stream.on_order({"data": [{"orderLinkId": link, "orderStatus": "New"}]})
        self.assertEqual(self.store.state(ticket.ticket_id), ExecutionState.FILLED)
        self.assertEqual(self.store.order(link)["order_status"], "FILLED")

    def test_private_stream_ignores_manual_orders_and_requires_live_probe(self):
        stream = PrivateStreamHandler(self.store)
        stream.mark_connected()
        self.assertFalse(stream.health_confirmed())
        stream.set_connection_probe(lambda: True)
        stream.mark_connected()
        self.assertTrue(stream.health_confirmed())
        stream.on_order({"data": [{"orderLinkId": "manual-order-123", "orderStatus": "New"}]})
        stream.on_execution(
            {"data": [{"execId": "manual-exec", "orderLinkId": "manual-order-123"}]}
        )
        self.assertEqual(stream.ignored_records, 2)
        self.assertFalse(self.store.kill_switch_enabled())

    def test_cancel_fill_race_promotes_actual_fill(self):
        ticket = OperationTicket.model_validate(ticket_payload())
        self.consumer.process(ticket, now=NOW)
        self.store.transition(ticket.ticket_id, ExecutionState.CANCELLED, "cancel_confirmed")
        link = order_link_id(ticket.ticket_id)
        order = self.store.order(link)
        self.store.record_fill(
            exec_id="race-fill",
            order_link_id=link,
            quantity=order["quantity"],
            price=99990,
            fee=0.1,
            executed_at=NOW,
        )
        self.assertEqual(self.store.state(ticket.ticket_id), ExecutionState.FILLED)

    def test_filled_entry_installs_idempotent_reduce_only_take_profit(self):
        ticket = OperationTicket.model_validate(ticket_payload("tk_take_profit_001"))
        self.consumer.process(ticket, now=NOW)
        entry_link = order_link_id(ticket.ticket_id)
        entry = self.store.order(entry_link)
        self.store.record_fill(
            exec_id="tp-entry-fill",
            order_link_id=entry_link,
            quantity=entry["quantity"],
            price=99990,
            fee=0.1,
            executed_at=NOW,
        )
        self.executor.rate_limiter = EndpointRateLimiter(minimum_interval_seconds=0)
        first = self.executor.submit_take_profits(ticket, self.context.rules)
        second = self.executor.submit_take_profits(ticket, self.context.rules)
        self.assertEqual(len(first), 1)
        self.assertTrue(first[0].newly_submitted)
        self.assertFalse(second[0].newly_submitted)
        self.assertEqual(len(self.client.exchange.orders), 2)
        take_profit = self.store.order(order_link_id(ticket.ticket_id, "take_profit_1"))
        self.assertEqual(take_profit["role"], "take_profit_1")
        self.assertTrue(self.client.exchange.orders[1]["params"]["reduceOnly"])
        self.assertEqual(self.client.exchange.orders[1]["params"]["positionIdx"], 1)
        self.store.record_fill(
            exec_id="tp-exit-fill",
            order_link_id=take_profit["order_link_id"],
            quantity=take_profit["quantity"],
            price=102000,
            fee=0.1,
            executed_at=NOW + timedelta(minutes=1),
        )
        self.assertEqual(self.store.state(ticket.ticket_id), ExecutionState.FILLED)

    def test_max_holding_submits_one_reduce_only_market_exit_and_stops_new_risk(self):
        payload = ticket_payload("tk_time_exit_001")
        payload["protection"]["max_holding_sec"] = 60
        ticket = OperationTicket.model_validate(payload)
        self.consumer.process(ticket, now=NOW)
        entry_link = order_link_id(ticket.ticket_id)
        entry = self.store.order(entry_link)
        quantity = float(entry["quantity"])
        self.store.record_fill(
            exec_id="time-exit-entry-fill",
            order_link_id=entry_link,
            quantity=quantity,
            price=99990,
            fee=0.1,
            executed_at=NOW,
        )
        self.client.exchange.positions = [
            {
                "symbol": "BTCUSDT",
                "side": "long",
                "contracts": quantity,
                "markPrice": 100000,
                "entryPrice": 99990,
                "notional": quantity * 100000,
                "info": {"symbol": "BTCUSDT", "size": str(quantity)},
            }
        ]
        runtime_context = BybitRuntimeContext(
            public_exchange=object(),
            account_client=self.client,
            store=self.store,
            mode="shadow",
            correlated_symbols={"BTCUSDT"},
        )
        service = TradingExecutionService.__new__(TradingExecutionService)
        service.store = self.store
        service.context = runtime_context
        service.executor = self.executor
        self.executor.rate_limiter = EndpointRateLimiter(minimum_interval_seconds=0)

        service._enforce_max_holding(now=NOW + timedelta(seconds=61))
        service._enforce_max_holding(now=NOW + timedelta(seconds=90))

        self.assertTrue(self.store.kill_switch_enabled())
        self.assertEqual(len(self.client.exchange.orders), 2)
        exit_order = self.client.exchange.orders[-1]
        self.assertEqual(exit_order["type"], "market")
        self.assertTrue(exit_order["params"]["reduceOnly"])

    def test_restart_recovery_uses_order_link_id_without_resubmit(self):
        ticket = OperationTicket.model_validate(ticket_payload())
        self.store.receive(ticket)
        self.store.transition(ticket.ticket_id, ExecutionState.VALIDATED, "validated")
        self.store.claim(ticket.ticket_id, "consumer-a", "lease", 60)
        self.store.transition(ticket.ticket_id, ExecutionState.RISK_APPROVED, "risk_approved")
        plan = PositionSizer().calculate(
            ticket,
            self.context.account_snapshot,
            self.context.portfolio_snapshot,
            self.context.rules,
        )
        link = order_link_id(ticket.ticket_id)
        self.store.reserve_order(
            ticket.ticket_id,
            link,
            role="entry",
            side=plan.side,
            order_type=plan.order_type,
            quantity=float(plan.quantity),
            price=float(plan.price),
        )
        remote = {"id": "remote-order-1", "status": "open", "info": {"orderStatus": "New"}}
        reconciler = ExecutionReconciler(self.store, FakeReconciliationGateway(remote=remote))
        recovered = reconciler.recover_all(now=NOW + timedelta(seconds=10))
        self.assertEqual(recovered[ticket.ticket_id], ExecutionState.ACKNOWLEDGED)
        self.assertEqual(len(self.client.exchange.orders), 0)

    def test_entry_wait_timeout_cancels_gtc_order_and_confirms_state(self):
        ticket = OperationTicket.model_validate(ticket_payload("tk_wait_timeout_001"))
        self.consumer.process(ticket, now=NOW)
        gateway = FakeReconciliationGateway()
        reconciler = ExecutionReconciler(self.store, gateway)
        state = reconciler.reconcile_ticket(
            ticket.ticket_id, now=ticket.expires_at + timedelta(seconds=1)
        )
        self.assertEqual(state, ExecutionState.CANCELLED)
        self.assertEqual(len(gateway.cancelled), 1)
        self.assertEqual(
            self.store.order(order_link_id(ticket.ticket_id))["order_status"],
            "CANCELLED",
        )

    def test_filled_ticket_with_active_exit_remains_recoverable(self):
        ticket = OperationTicket.model_validate(ticket_payload("tk_exit_recovery_001"))
        self.consumer.process(ticket, now=NOW)
        entry_link = order_link_id(ticket.ticket_id)
        entry = self.store.order(entry_link)
        self.store.record_fill(
            exec_id="entry-before-restart",
            order_link_id=entry_link,
            quantity=entry["quantity"],
            price=99990,
            fee=0.1,
            executed_at=NOW,
        )
        self.executor.rate_limiter = EndpointRateLimiter(minimum_interval_seconds=0)
        self.executor.submit_take_profits(ticket, self.context.rules)
        exit_link = order_link_id(ticket.ticket_id, "take_profit_1")
        remote = {"id": "remote-tp", "status": "open", "info": {"orderStatus": "New"}}
        recovered = ExecutionReconciler(
            self.store, FakeReconciliationGateway(remote=remote)
        ).recover_all(now=NOW + timedelta(minutes=1))
        self.assertIn(ticket.ticket_id, recovered)
        self.assertEqual(self.store.order(exit_link)["order_status"], "OPEN")

    def test_position_version_price_and_kill_switch_are_rejected(self):
        conflict = OperationTicket.model_validate(ticket_payload("tk_position_conflict", position_version=99))
        self.assertEqual(self.consumer.process(conflict, now=NOW), ExecutionState.RISK_BLOCKED)
        self.assertEqual(self.store.ticket_record(conflict.ticket_id)["reason_code"], "POSITION_VERSION_CONFLICT")

        self.context.market_snapshot = replace(self.context.market_snapshot, last_price=101000)
        deviation = OperationTicket.model_validate(ticket_payload("tk_price_deviation"))
        self.assertEqual(self.consumer.process(deviation, now=NOW), ExecutionState.RISK_BLOCKED)
        self.assertEqual(self.store.ticket_record(deviation.ticket_id)["reason_code"], "PRICE_DEVIATION")

        self.context.market_snapshot = replace(self.context.market_snapshot, last_price=100000)
        self.store.set_kill_switch(True)
        killed = OperationTicket.model_validate(ticket_payload("tk_kill_switch"))
        self.assertEqual(self.consumer.process(killed, now=NOW), ExecutionState.RISK_BLOCKED)
        self.assertEqual(self.store.ticket_record(killed.ticket_id)["reason_code"], "KILL_SWITCH")

    def test_precision_is_normalized_down_without_exceeding_limits(self):
        ticket = OperationTicket.model_validate(ticket_payload())
        plan = PositionSizer().calculate(
            ticket,
            self.context.account_snapshot,
            self.context.portfolio_snapshot,
            self.context.rules,
        )
        self.assertEqual(plan.quantity % Decimal("0.001"), 0)
        self.assertEqual(plan.price % Decimal("0.1"), 0)
        self.assertLessEqual(plan.notional_usdt, Decimal("5000"))

    def test_position_size_includes_proposed_correlated_exposure(self):
        ticket = OperationTicket.model_validate(ticket_payload("tk_corr_capacity_001"))
        portfolio = replace(
            self.context.portfolio_snapshot,
            same_direction_correlated_notional_usdt=3000,
        )
        plan = PositionSizer().calculate(
            ticket, self.context.account_snapshot, portfolio, self.context.rules
        )
        self.assertLessEqual(plan.notional_usdt, Decimal("500"))

    def test_reduce_and_close_size_from_current_position_only(self):
        portfolio = replace(self.context.portfolio_snapshot, current_position_qty=0.021)
        reduce_payload = ticket_payload("tk_reduce_test_001")
        reduce_payload["intent"].update(
            action="REDUCE",
            side="SELL",
            position_effect="REDUCE_ONLY",
            target_exposure_pct=0,
            risk_budget_pct=0,
            reduce_fraction=0.5,
        )
        reduce_payload["entry"].update(order_type="MARKET", limit_price=None)
        reduce_payload["protection"] = None
        reduce_payload["economics"] = {
            "expected_return_bps": 0,
            "estimated_fee_bps": 0,
            "estimated_slippage_bps": 0,
            "estimated_funding_bps": 0,
            "model_error_buffer_bps": 0,
            "expected_return_after_cost_bps": 0,
        }
        reduce_ticket = OperationTicket.model_validate(reduce_payload)
        reduce_plan = PositionSizer().calculate(
            reduce_ticket, self.context.account_snapshot, portfolio, self.context.rules
        )
        self.assertEqual(reduce_plan.quantity, Decimal("0.010"))
        self.assertEqual(reduce_plan.risk_amount_usdt, Decimal("0"))

        close_payload = ticket_payload("tk_close_test_001")
        close_payload["intent"].update(
            action="CLOSE",
            side="SELL",
            position_effect="CLOSE_ONLY",
            target_exposure_pct=0,
            risk_budget_pct=0,
        )
        close_payload["entry"].update(order_type="MARKET", limit_price=None)
        close_payload["protection"] = None
        close_payload["economics"] = dict(reduce_payload["economics"])
        close_ticket = OperationTicket.model_validate(close_payload)
        close_plan = PositionSizer().calculate(
            close_ticket, self.context.account_snapshot, portfolio, self.context.rules
        )
        self.assertEqual(close_plan.quantity, Decimal("0.021"))

    def test_cancel_ticket_cancels_target_without_creating_another_order(self):
        original = OperationTicket.model_validate(ticket_payload("tk_cancel_target_001"))
        self.assertEqual(self.consumer.process(original, now=NOW), ExecutionState.SUBMITTED)
        target_link_id = order_link_id(original.ticket_id)
        cancel_payload = ticket_payload("tk_cancel_command_001")
        cancel_payload["intent"].update(
            action="CANCEL",
            side="BUY",
            position_effect="CANCEL_ONLY",
            target_exposure_pct=0,
            risk_budget_pct=0,
            target_order_link_id=target_link_id,
        )
        cancel_payload["entry"] = None
        cancel_payload["protection"] = None
        cancel_payload["economics"] = {
            "expected_return_bps": 0,
            "estimated_fee_bps": 0,
            "estimated_slippage_bps": 0,
            "estimated_funding_bps": 0,
            "model_error_buffer_bps": 0,
            "expected_return_after_cost_bps": 0,
        }
        cancel = OperationTicket.model_validate(cancel_payload)
        self.assertEqual(self.consumer.process(cancel, now=NOW), ExecutionState.CANCELLED)
        self.assertEqual(self.store.state(original.ticket_id), ExecutionState.CANCELLED)
        self.assertEqual(self.store.order(target_link_id)["order_status"], "CANCELLED")
        self.assertEqual(len(self.client.exchange.orders), 1)
        self.assertEqual(self.client.exchange.orders[0]["status"], "canceled")

    def test_expired_bad_quality_and_unconfirmed_testnet_are_fail_closed(self):
        expired_payload = ticket_payload("tk_expired_test_001")
        expired_payload["created_at"] = NOW - timedelta(minutes=10)
        expired_payload["valid_from"] = NOW - timedelta(minutes=9)
        expired_payload["expires_at"] = NOW - timedelta(seconds=1)
        expired = OperationTicket.model_validate(expired_payload)
        self.assertEqual(self.consumer.process(expired, now=NOW), ExecutionState.EXPIRED)

        bad_quality_payload = ticket_payload("tk_bad_quality_001")
        bad_quality_payload["guards"]["observed_data_quality"] = 0.5
        with self.assertRaises(ValueError):
            OperationTicket.model_validate(bad_quality_payload)

        self.context.health_snapshot = replace(
            self.context.health_snapshot, mode="testnet", websocket_confirmed=False
        )
        unconfirmed = OperationTicket.model_validate(ticket_payload("tk_ws_unconfirmed_001"))
        self.assertEqual(self.consumer.process(unconfirmed, now=NOW), ExecutionState.RISK_BLOCKED)
        self.assertEqual(
            self.store.ticket_record(unconfirmed.ticket_id)["reason_code"],
            "WEBSOCKET_UNCONFIRMED",
        )

    def test_rate_limit_headers_block_without_sleep_or_retry(self):
        clock = FakeClock()
        limiter = EndpointRateLimiter(minimum_interval_seconds=0, clock=clock)
        limiter.update_from_headers(
            "uid", "create", {"X-Bapi-Limit-Status": "0", "Retry-After": "2"}
        )
        with self.assertRaises(RateLimitBlocked):
            limiter.acquire("uid", "create")
        clock.value += 2.1
        limiter.acquire("uid", "create")

    def test_execution_receipt_is_immutable_and_queued_once(self):
        ticket = OperationTicket.model_validate(ticket_payload())
        self.consumer.process(ticket, now=NOW)
        link = order_link_id(ticket.ticket_id)
        order = self.store.order(link)
        self.store.record_fill(
            exec_id="receipt-fill",
            order_link_id=link,
            quantity=order["quantity"],
            price=99990,
            fee=0.2,
            executed_at=NOW,
        )
        version = self.store.save_position(
            "BTCUSDT",
            side="BUY",
            quantity=order["quantity"],
            avg_price=99990,
            notional_usdt=order["quantity"] * 99990,
            source="test-fill",
        )
        reporter = ExecutionReporter(self.store, "consumer-a", "shadow")
        first = reporter.build(ticket.ticket_id)
        second = reporter.build(ticket.ticket_id)
        self.assertEqual(first.receipt_id, second.receipt_id)
        self.assertEqual(first.status, "FILLED")
        self.assertEqual(first.position_version_after, version)
        self.assertEqual(len(self.store.pending_receipts()), 1)

    def test_account_risk_snapshot_replay_replaces_stale_runtime_values(self):
        loss_at = datetime.now(timezone.utc) - timedelta(minutes=5)
        self.store.synchronize_risk_runtime(
            realised_pnl=-125,
            unrealised_pnl=-25,
            consecutive_losses=3,
            last_loss_at=loss_at,
        )
        snapshot = self.store.risk_runtime()
        self.assertEqual(snapshot["realised_pnl"], -125)
        self.assertEqual(snapshot["unrealised_pnl"], -25)
        self.assertEqual(snapshot["consecutive_losses"], 3)
        self.assertIsNotNone(snapshot["cooldown_until"])

    def test_superseded_ticket_cannot_continue(self):
        old = OperationTicket.model_validate(ticket_payload("tk_old_ticket_001"))
        replacement_payload = ticket_payload("tk_new_ticket_001")
        replacement_payload["supersedes_ticket_id"] = old.ticket_id
        replacement_payload["intent"]["action"] = "REPLACE"
        replacement_payload["intent"]["position_effect"] = "REPLACE_ONLY"
        replacement = OperationTicket.model_validate(replacement_payload)
        self.store.receive(old)
        self.store.transition(old.ticket_id, ExecutionState.VALIDATED, "validated")
        self.store.receive(replacement)
        self.assertEqual(self.store.state(old.ticket_id), ExecutionState.SUPERSEDED)
        self.assertEqual(self.consumer.process(old, now=NOW), ExecutionState.SUPERSEDED)
        self.assertEqual(len(self.client.exchange.orders), 0)


class BybitEvidenceTests(unittest.TestCase):
    def test_kill_switch_blocks_open_but_keeps_close_path_available(self):
        close_payload = ticket_payload("tk_kill_switch_close_001")
        close_payload["intent"].update(
            action="CLOSE",
            side="SELL",
            position_effect="CLOSE_ONLY",
            target_exposure_pct=0,
            risk_budget_pct=0,
        )
        close_payload["entry"].update(order_type="MARKET", limit_price=None)
        close_payload["protection"] = None
        close_payload["economics"] = {
            "expected_return_bps": 0,
            "estimated_fee_bps": 0,
            "estimated_slippage_bps": 0,
            "estimated_funding_bps": 0,
            "model_error_buffer_bps": 0,
            "expected_return_after_cost_bps": 0,
        }
        ticket = OperationTicket.model_validate(close_payload)
        decision = RiskGuard().evaluate(
            ticket,
            MarketSnapshot("BTCUSDT", 100000, 99990, 100010, "normal", NOW),
            AccountSnapshot(0, 0, 0, risk_metrics_healthy=False),
            PortfolioSnapshot(2000, 2000, 3, 0.02),
            SystemHealth("live", True, False, False, float("inf")),
            now=NOW,
        )
        self.assertTrue(decision.approved)

    def test_ccxt_unified_position_symbol_is_included_in_portfolio_risk(self):
        class Account:
            def get_all_open_positions(self):
                return [
                    {
                        "symbol": "BTC/USDT:USDT",
                        "side": "long",
                        "contracts": 0.02,
                        "markPrice": 100000,
                        "entryPrice": 99000,
                        "notional": 2000,
                        "info": {"symbol": "BTCUSDT", "size": "0.02"},
                    }
                ]

        with tempfile.TemporaryDirectory() as td:
            store = ExecutionStore(Path(td) / "execution.sqlite3")
            context = BybitRuntimeContext(
                public_exchange=object(),
                account_client=Account(),
                store=store,
                mode="shadow",
                correlated_symbols={"BTCUSDT", "ETHUSDT"},
            )
            ticket = OperationTicket.model_validate(ticket_payload("tk_symbol_normalize_001"))
            portfolio = context.portfolio(ticket)
            self.assertEqual(portfolio.current_position_qty, 0.02)
            self.assertEqual(portfolio.gross_notional_usdt, 2000)
            self.assertEqual(portfolio.same_direction_correlated_notional_usdt, 2000)

    def test_closed_order_recovery_uses_history_after_realtime_cache_miss(self):
        class Exchange:
            def fetch_open_orders(self, symbol):
                return []

            def private_get_v5_order_realtime(self, params):
                return {"result": {"list": []}}

            def private_get_v5_order_history(self, params):
                return {
                    "result": {
                        "list": [{"orderId": "remote-1", "orderStatus": "Filled"}]
                    }
                }

        result = ExchangeGateway(mode="live", exchange=Exchange()).find_order_by_link_id(
            "BTCUSDT", "tk-history-entry"
        )
        self.assertEqual(result["orderId"], "remote-1")

    def test_daily_risk_replays_only_trading_ledger_and_loss_streak(self):
        now_ms = int(NOW.timestamp() * 1000)

        class Exchange:
            def private_get_v5_account_transaction_log(self, params):
                return {
                    "result": {
                        "list": [
                            {"type": "TRADE", "orderId": "loss-1", "change": "-11", "cashFlow": "-10", "transactionTime": now_ms - 4},
                            {"type": "SETTLEMENT", "change": "-2", "cashFlow": "0", "transactionTime": now_ms - 3},
                            {"type": "TRANSFER_IN", "change": "100", "cashFlow": "100", "transactionTime": now_ms - 2},
                            {"type": "TRADE", "orderId": "win-1", "change": "3.5", "cashFlow": "4", "transactionTime": now_ms - 1},
                            {"type": "TRADE", "orderId": "loss-2", "change": "-2.5", "cashFlow": "-2", "transactionTime": now_ms},
                        ],
                        "nextPageCursor": "",
                    }
                }

        metrics = ExchangeGateway(mode="live", exchange=Exchange()).get_daily_risk_metrics(NOW)
        self.assertTrue(metrics["healthy"])
        self.assertEqual(metrics["realised_pnl"], -12.0)
        self.assertEqual(metrics["consecutive_losses"], 1)
        self.assertEqual(metrics["record_count"], 5)

    def test_daily_risk_fails_closed_when_page_cap_is_exceeded(self):
        class Exchange:
            calls = 0

            def private_get_v5_account_transaction_log(self, params):
                self.calls += 1
                return {
                    "result": {
                        "list": [{"type": "TRADE", "change": "0", "cashFlow": "0"}],
                        "nextPageCursor": f"cursor-{self.calls}",
                    }
                }

        metrics = ExchangeGateway(mode="live", exchange=Exchange()).get_daily_risk_metrics(NOW)
        self.assertFalse(metrics["healthy"])
        self.assertEqual(metrics["reason"], "transaction_log_pagination_limit_exceeded")


if __name__ == "__main__":
    unittest.main()
