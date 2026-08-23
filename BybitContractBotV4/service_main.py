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
from incident_modes import IncidentMode
from logger import logger
from private_stream import BybitPrivateWebSocket, PrivateStreamHandler
from risk_guard import RiskGuard, RiskLimits
from runtime_config import TradingMode, TradingSettings
from runtime_context import BybitRuntimeContext
from sizing import PositionSizer
from soak_monitor import SoakMonitor
from ticket_client import TicketHttpClient
from ticket_consumer import TicketConsumer
from ticket_store import ExecutionStore
from ticket_validator import TicketValidator


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
        self.store = ExecutionStore(
            db_path.resolve(), code_commit=settings.app_code_commit or None
        )
        log_paths = [
            Path(handler.baseFilename)
            for handler in logger.handlers
            if getattr(handler, "baseFilename", None)
        ]
        self.soak_monitor = SoakMonitor(self.store, log_paths=log_paths)
        self._last_soak_sample_monotonic = 0.0
        self._reconcile_inconsistencies = 0
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
            position_owner_id=settings.position_owner_id,
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
            validator=TicketValidator(settings.approved_strategy_release_id),
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
        self.last_ownership_audit = None
        self.health_server = HealthServer(
            settings.health_host, settings.health_port, self.health_snapshot
        )

    @staticmethod
    def _position_identity(position: dict) -> tuple[str, float]:
        info = position.get("info") or {}
        symbol = BybitRuntimeContext._linear_symbol_id(
            info.get("symbol") or position.get("symbol")
        )
        quantity = abs(float(position.get("contracts") or info.get("size") or 0))
        side = str(position.get("side") or info.get("side") or "").lower()
        return symbol, -quantity if side in {"short", "sell"} else quantity

    def _open_private_websocket(self) -> None:
        if self.websocket is not None:
            self.websocket.stop()
        self.websocket = BybitPrivateWebSocket(
            self.settings.api_key,
            self.settings.secret_key,
            testnet=self.settings.mode is TradingMode.TESTNET,
            handler=self.stream_handler,
        )
        self.websocket.start()

    def _verify_account_ownership(self) -> dict:
        """Fail closed when a dedicated account contains state not owned by this service."""

        if self.settings.mode is TradingMode.SHADOW:
            result = {"status": "shadow", "unknown_orders": [], "unknown_positions": []}
            self.last_ownership_audit = result
            return result
        known_links = self.store.known_order_link_ids()
        unknown_orders = []
        for order in self.account_client.get_all_open_orders() or []:
            info = order.get("info") or {}
            link_id = str(info.get("orderLinkId") or order.get("clientOrderId") or "")
            if not link_id or link_id not in known_links:
                unknown_orders.append(
                    {
                        "order_id": str(order.get("id") or info.get("orderId") or "unknown"),
                        "order_link_id": link_id or None,
                        "symbol": str(info.get("symbol") or order.get("symbol") or ""),
                    }
                )
        unknown_positions = []
        for position in self.account_client.get_all_open_positions() or []:
            symbol, remote_quantity = self._position_identity(position)
            if abs(remote_quantity) <= 1e-12:
                continue
            local_quantity = self.store.local_position_quantity(symbol)
            latest = self.store.latest_position(symbol)
            snapshot_quantity = float(latest.get("quantity") or 0)
            snapshot_owned = (
                latest.get("position_owner_id") == self.settings.position_owner_id
                and abs(snapshot_quantity - remote_quantity)
                <= max(1e-9, abs(remote_quantity) * 1e-6)
            )
            ledger_owned = abs(local_quantity - remote_quantity) <= max(
                1e-9, abs(remote_quantity) * 1e-6
            )
            if not (snapshot_owned or ledger_owned):
                unknown_positions.append(
                    {
                        "symbol": symbol,
                        "remote_quantity": remote_quantity,
                        "local_quantity": local_quantity,
                        "snapshot_owner": latest.get("position_owner_id"),
                    }
                )
        result = {
            "status": "blocked" if unknown_orders or unknown_positions else "owned",
            "unknown_orders": unknown_orders,
            "unknown_positions": unknown_positions,
            "position_owner_id": self.settings.position_owner_id,
        }
        self.last_ownership_audit = result
        if unknown_orders or unknown_positions:
            self.store.set_reconciliation_complete(False, "unknown account state")
            self.store.set_incident_mode(
                IncidentMode.MANUAL_HANDOVER,
                "unknown position/order requires explicit adoption or operator cleanup",
            )
            self.store.set_kill_switch(True)
            raise RuntimeError(
                f"dedicated-account ownership check failed: "
                f"unknown_orders={len(unknown_orders)} "
                f"unknown_positions={len(unknown_positions)}"
            )
        return result

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
        runtime = self.store.system_runtime()
        websocket_healthy = self.stream_handler.health_confirmed()
        degraded = bool(
            self.last_error
            or not runtime.get("reconciliation_complete")
            or runtime.get("incident_mode") != IncidentMode.NORMAL.value
            or (
                self.settings.mode in {TradingMode.TESTNET, TradingMode.LIVE}
                and not websocket_healthy
            )
        )
        return {
            "status": "degraded" if degraded else "ok",
            "mode": self.settings.mode.value,
            "kill_switch": self.store.kill_switch_enabled(),
            "incident_mode": runtime.get("incident_mode"),
            "reconciliation_complete": bool(runtime.get("reconciliation_complete")),
            "account_ownership": self.last_ownership_audit,
            "private_websocket_connected": websocket_healthy,
            "private_websocket_last_message_at": (
                self.stream_handler.last_message_at.isoformat()
                if self.stream_handler.last_message_at else None
            ),
            "private_stream_ignored_records": self.stream_handler.ignored_records,
            "incomplete_ticket_count": len(self.store.incomplete_ticket_ids()),
            "last_poll_at": self.last_poll_at,
            "last_poll_result": self.last_poll_result,
            "last_error": self.last_error,
            "soak_run_id": self.soak_monitor.run_id,
        }

    def _sample_soak(self, *, force: bool = False, external_checks: bool = True) -> None:
        now_monotonic = time.monotonic()
        if not force and now_monotonic - self._last_soak_sample_monotonic < 60:
            return
        ownership = self.last_ownership_audit or {}
        if external_checks:
            try:
                self._last_exchange_clock_drift = (
                    float(self.public_exchange.fetch_time()) / 1000 - time.time()
                )
            except Exception:
                self._last_exchange_clock_drift = 1_000_000_000.0
            try:
                self._last_data_healthy = bool(self.ticket_client.health())
            except Exception:
                self._last_data_healthy = False
        drift = getattr(self, "_last_exchange_clock_drift", 1_000_000_000.0)
        data_healthy = getattr(self, "_last_data_healthy", False)
        self.soak_monitor.sample(
            websocket_reconnects=max(0, self.stream_handler.connection_count - 1),
            reconcile_inconsistencies=self._reconcile_inconsistencies,
            unknown_orders=len(ownership.get("unknown_orders") or []),
            unknown_positions=len(ownership.get("unknown_positions") or []),
            stale_sources=0 if data_healthy else 1,
            exchange_clock_drift_sec=drift,
        )
        self._last_soak_sample_monotonic = now_monotonic

    def start(self) -> None:
        logger.warning("Starting versioned ticket consumer in %s mode", self.settings.mode.value)
        unexpected_restart = self.soak_monitor.start()
        if unexpected_restart:
            logger.error("Previous service run did not record a clean shutdown")
        self.store.set_reconciliation_complete(False, "startup reconciliation in progress")
        try:
            if self.settings.mode in {TradingMode.TESTNET, TradingMode.LIVE}:
                self._open_private_websocket()
            self._verify_account_ownership()
            self.reconciler.recover_all()
            self._verify_account_ownership()
            self._enforce_max_holding()
            self._recover_take_profits()
            self.store.set_reconciliation_complete(True, "startup reconciliation complete")
            self.health_server.start()
        except Exception:
            if self.websocket is not None:
                self.websocket.stop()
                self.websocket = None
            self.health_server.stop()
            self.soak_monitor.stop()
            raise

    def run_once(self) -> None:
        try:
            self.store.set_reconciliation_complete(False, "periodic reconciliation in progress")
            if self.settings.mode in {TradingMode.TESTNET, TradingMode.LIVE}:
                if not self.stream_handler.health_confirmed():
                    logger.warning("Private WebSocket unhealthy; reconnecting before reconcile")
                    self._open_private_websocket()
            self.reconciler.recover_all()
            self._verify_account_ownership()
            self._enforce_max_holding()
            self._recover_take_profits()
            self.store.set_reconciliation_complete(True, "periodic reconciliation complete")
            result = self.consumer_service.run_once()
            self.last_poll_at = datetime.now(timezone.utc).isoformat()
            self.last_poll_result = result.__dict__
            self.last_error = None
        except Exception as exc:
            self._reconcile_inconsistencies += 1
            self.store.set_reconciliation_complete(False, "periodic reconciliation failed")
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.exception("ticket consumer iteration failed")
        finally:
            self._sample_soak()

    def run_forever(self) -> None:
        self.start()
        try:
            while True:
                self.run_once()
                time.sleep(self.settings.ticket_poll_seconds)
        finally:
            self.stop()

    def stop(self) -> None:
        if self.soak_monitor.started:
            self._sample_soak(force=True, external_checks=False)
            self.soak_monitor.stop()
        if self.websocket is not None:
            self.websocket.stop()
            self.websocket = None
        self.ticket_client.close()
        self.health_server.stop()


def run_service(settings: TradingSettings | None = None) -> None:
    runtime = settings or TradingSettings.load()
    TradingExecutionService(runtime).run_forever()
