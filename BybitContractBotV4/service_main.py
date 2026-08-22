from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from bybit import build_bybit_client
from bybit_executor import BybitExecutor
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
        self.account_client = build_bybit_client(settings)
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

    def _install_take_profits(self, ticket_id: str) -> None:
        ticket = self.store.get_ticket(ticket_id)
        if not ticket:
            self.store.set_kill_switch(True)
            logger.error("Protection install failed: ticket %s is absent", ticket_id)
            return
        try:
            rules = self.context.instrument_rules(ticket)
            self.executor.submit_take_profits(ticket, rules)
        except Exception:
            # The entry stop is attached atomically. Failure to add profit-taking
            # children still escalates to the kill switch for operator review.
            self.store.set_kill_switch(True)
            logger.exception("Take-profit installation failed for ticket %s", ticket_id)

    def health_snapshot(self) -> dict:
        return {
            "status": "degraded" if self.last_error else "ok",
            "mode": self.settings.mode.value,
            "kill_switch": self.store.kill_switch_enabled(),
            "private_websocket_connected": self.stream_handler.connected,
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

    def run_once(self) -> None:
        try:
            self.reconciler.recover_all()
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
