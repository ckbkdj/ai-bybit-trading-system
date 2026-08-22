from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from bybit_executor import BybitExecutor
from exchange_gateway import build_exchange_gateway
from execution_reconciler import BybitReconciliationGateway, ExecutionReconciler
from execution_reporter import ExecutionReporter
from execution_service import TicketConsumerService
from health_server import HealthServer
from logger import logger
from private_stream import BybitPrivateWebSocket, PrivateStreamHandler
from risk_guard import RiskGuard, RiskLimits
from runtime_config import TradingMode, TradingSettings
from runtime_context import BybitRuntimeContext
from sizing import PositionSizer
from ticket_client import TicketHttpClient
from ticket_consumer import TicketConsumer
from ticket_store import ExecutionStore


def _public_exchange(settings: TradingSettings):
    import ccxt

    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    if settings.mode is TradingMode.TESTNET:
        exchange.set_sandbox_mode(True)
    exchange.load_markets()
    return exchange


class TradingExecutionService:
    def __init__(self, settings: TradingSettings):
        self.settings = settings
        db_path = Path(settings.execution_db_path)
        if not db_path.is_absolute():
            db_path = settings.root / db_path
        self.store = ExecutionStore(db_path.resolve())
        self.account_client = build_exchange_gateway(settings)
        self.public_exchange = _public_exchange(settings)
        self.ticket_client = TicketHttpClient(
            settings.ticket_api_base_url,
            token=settings.ticket_api_token,
            timeout_seconds=settings.prediction_timeout_seconds,
            verify=settings.requests_verify,
        )
        self.stream_handler = PrivateStreamHandler(self.store)
        self.websocket = None
        limits = RiskLimits(
            max_daily_loss_pct=settings.max_daily_loss_pct,
            max_equity_drawdown_pct=settings.max_equity_drawdown_pct,
            max_gross_leverage=settings.max_gross_leverage,
            max_correlated_exposure_pct=settings.max_correlated_exposure_pct,
            max_margin_utilization=settings.max_margin_utilization,
            max_consecutive_losses=settings.max_consecutive_losses,
            max_exchange_clock_drift_sec=settings.max_exchange_clock_drift_seconds,
            require_websocket_confirmation=settings.require_websocket_confirmation,
        )
        self.context = BybitRuntimeContext(
            public_exchange=self.public_exchange,
            account_client=self.account_client,
            store=self.store,
            mode=settings.mode.value,
            private_stream=self.stream_handler,
            regime_provider=self.ticket_client.latest_market_regime,
            data_health_provider=self.ticket_client.health,
            correlated_symbols=set(settings.correlated_symbols),
        )
        self.executor = BybitExecutor(self.account_client, self.store, uid=settings.ticket_consumer_id)
        self.stream_handler.on_entry_filled = self._install_take_profits
        consumer = TicketConsumer(
            consumer_id=settings.ticket_consumer_id,
            store=self.store,
            context=self.context,
            executor=self.executor,
            risk_guard=RiskGuard(limits),
            sizer=PositionSizer(limits),
        )
        self.reporter = ExecutionReporter(
            self.store, settings.ticket_consumer_id, settings.mode.value
        )
        self.consumer_service = TicketConsumerService(
            consumer_id=settings.ticket_consumer_id,
            client=self.ticket_client,
            consumer=consumer,
            reporter=self.reporter,
            store=self.store,
        )
        self.reconciler = ExecutionReconciler(
            self.store, BybitReconciliationGateway(self.account_client)
        )
        self.last_poll_at = None
        self.last_poll_result = None
        self.last_error = None
        self.health_server = HealthServer(
            settings.health_host, settings.health_port, self.health_snapshot
        )

    def _install_take_profits(self, ticket_id: str, *, recover: bool = False) -> None:
        ticket = self.store.get_ticket(ticket_id)
        if not ticket:
            self.store.set_kill_switch(True)
            logger.error("Protection install failed: ticket %s is absent", ticket_id)
            return
        if any(
            str(order.get("role") or "").startswith("time_exit_")
            for order in self.store.orders_for_ticket(ticket_id)
        ):
            # Once the maximum-holding close has started, adding new profit
            # targets only creates an avoidable cancel/fill race.
            return
        try:
            rules = self.context.instrument_rules(ticket)
            position_quantity = None
            if recover:
                portfolio = self.context.portfolio(ticket)
                expected_long = ticket.intent.side == "BUY"
                live_quantity = float(portfolio.current_position_qty)
                if abs(live_quantity) <= 1e-12:
                    return
                if (live_quantity > 0) != expected_long:
                    self.store.set_kill_switch(True)
                    logger.error(
                        "Protection recovery found opposite live position for ticket %s",
                        ticket_id,
                    )
                    return
                position_quantity = abs(live_quantity)
            self.executor.submit_take_profits(
                ticket, rules, position_quantity=position_quantity
            )
        except Exception:
            # The entry stop is attached atomically. Failure to add profit-taking
            # children still escalates to the kill switch for operator review.
            self.store.set_kill_switch(True)
            logger.exception("Take-profit installation failed for ticket %s", ticket_id)

    def _recover_take_profits(self) -> None:
        # Only the newest filled/part-filled entry per symbol can own the current
        # position because opening tickets require a flat position.
        for ticket_id in self.store.latest_position_origin_ticket_ids():
            self._install_take_profits(ticket_id, recover=True)

    def _enforce_max_holding(self, *, now: datetime | None = None) -> None:
        """Submit one auditable reduce-only close when a ticket's hold clock expires."""

        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        for ticket_id in self.store.latest_position_origin_ticket_ids():
            ticket = self.store.get_ticket(ticket_id)
            if not ticket or not ticket.protection:
                continue
            first_fill = self.store.first_entry_fill_at(ticket_id)
            if first_fill is None:
                continue
            age_seconds = (current - first_fill).total_seconds()
            if age_seconds < ticket.protection.max_holding_sec:
                continue
            portfolio = self.context.portfolio(ticket)
            live_quantity = float(portfolio.current_position_qty)
            if abs(live_quantity) <= 1e-12:
                continue
            expected_long = ticket.intent.side == "BUY"
            if (live_quantity > 0) != expected_long:
                self.store.set_kill_switch(True)
                raise RuntimeError(
                    f"max-holding exit found opposite position for {ticket_id}"
                )
            # Stop all new risk before an automatic risk-reducing action.  The
            # deterministic child id makes retries safe across restarts.
            self.store.set_kill_switch(True)
            self.executor.submit_time_exit(
                ticket, position_quantity=Decimal(str(abs(live_quantity)))
            )
            logger.warning(
                "Maximum holding time reached; reduce-only exit submitted for %s age_sec=%s",
                ticket_id,
                int(age_seconds),
            )

    def health_snapshot(self) -> dict:
        return {
            "status": "degraded" if self.last_error else "ok",
            "mode": self.settings.mode.value,
            "kill_switch": self.store.kill_switch_enabled(),
            "private_websocket_connected": self.stream_handler.health_confirmed(),
            "private_websocket_last_message_at": (
                self.stream_handler.last_message_at.isoformat()
                if self.stream_handler.last_message_at else None
            ),
            "private_stream_ignored_records": self.stream_handler.ignored_records,
            "incomplete_ticket_count": len(self.store.incomplete_ticket_ids()),
            "last_poll_at": self.last_poll_at,
            "last_poll_result": self.last_poll_result,
            "last_error": self.last_error,
        }

    def start(self) -> None:
        logger.warning("Starting versioned ticket consumer in %s mode", self.settings.mode.value)
        self.health_server.start()
        if self.settings.mode in {TradingMode.TESTNET, TradingMode.LIVE}:
            self.websocket = BybitPrivateWebSocket(
                self.settings.api_key,
                self.settings.secret_key,
                testnet=self.settings.mode is TradingMode.TESTNET,
                handler=self.stream_handler,
            )
            self.websocket.start()
        self.reconciler.recover_all()
        self._enforce_max_holding()
        self._recover_take_profits()

    def run_once(self) -> None:
        try:
            self.reconciler.recover_all()
            self._enforce_max_holding()
            self._recover_take_profits()
            result = self.consumer_service.run_once()
            self.last_poll_at = datetime.now(timezone.utc).isoformat()
            self.last_poll_result = result.__dict__
            self.last_error = None
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.exception("ticket consumer iteration failed")

    def run_forever(self) -> None:
        self.start()
        try:
            while True:
                self.run_once()
                time.sleep(self.settings.ticket_poll_seconds)
        finally:
            self.stop()

    def stop(self) -> None:
        if self.websocket is not None:
            self.websocket.stop()
            self.websocket = None
        self.ticket_client.close()
        self.health_server.stop()


def run_service(settings: TradingSettings | None = None) -> None:
    runtime = settings or TradingSettings.load()
    TradingExecutionService(runtime).run_forever()
