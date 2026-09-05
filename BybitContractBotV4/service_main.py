from __future__ import annotations

import time
import shutil
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from bybit_executor import BybitExecutor
from durable_execution_store import DurableExecutionStore
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
        self.store = DurableExecutionStore(
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
        self.instance_id = (
            f"{settings.deployment_id or 'local'}:{settings.host_id or 'host'}:"
            f"{uuid.uuid4().hex[:16]}"
        )
        self.account_client = build_exchange_gateway(settings)
        self.public_exchange = _public_exchange(settings)
        self.ticket_client = TicketHttpClient(
            settings.ticket_api_base_url,
            token=settings.ticket_api_token,
            timeout_seconds=settings.prediction_timeout_seconds,
            verify=settings.requests_verify,
            client_cert=settings.requests_client_cert,
            consumer_id=settings.ticket_consumer_id,
        )
        self.stream_handler = PrivateStreamHandler(self.store)
        self.websocket = None
        limits = RiskLimits(
            max_risk_per_trade_pct=settings.max_risk_per_trade_pct,
            max_daily_loss_pct=settings.max_daily_loss_pct,
            max_weekly_loss_pct=settings.max_weekly_loss_pct,
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
            service_session_id=self.instance_id,
        )
        self.reconciler = ExecutionReconciler(
            self.store, BybitReconciliationGateway(self.account_client)
        )
        self.last_poll_at = None
        self.last_poll_result = None
        self.last_error = None
        self.last_reconciliation_error = None
        self.last_control_error = None
        self.last_control_handshake = None
        self.control_plane_ready = False
        self._control_freeze_owned = False
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

    def _freeze_for_control_plane(self, reason: str) -> None:
        runtime = self.store.system_runtime()
        if runtime.get("incident_mode") == IncidentMode.NORMAL.value:
            self.store.set_incident_mode(IncidentMode.FREEZE_NEW_RISK, reason)
            self._control_freeze_owned = True

    def _refresh_control_plane_handshake(self):
        result = self.ticket_client.handshake(
            consumer_id=self.settings.ticket_consumer_id,
            instance_id=self.instance_id,
            account_id=self.settings.position_owner_id,
            executor_version=self.settings.executor_version,
            expected_cluster_id=self.settings.cluster_id,
            expected_deployment_id=self.settings.deployment_id,
            max_clock_skew_seconds=self.settings.max_control_plane_clock_drift_seconds,
        )
        self.last_control_handshake = result
        self.control_plane_ready = bool(result.ready)
        if result.ready:
            self.last_control_error = None
            if (
                self._control_freeze_owned
                and self.store.system_runtime().get("incident_mode")
                == IncidentMode.FREEZE_NEW_RISK.value
            ):
                self.store.set_incident_mode(
                    IncidentMode.NORMAL, "control-plane handshake recovered"
                )
            self._control_freeze_owned = False
        else:
            self.last_control_error = result.reason
            self._freeze_for_control_plane(
                f"control-plane handshake blocked ticket intake: {result.reason}"
            )
        return result

    def health_snapshot(self) -> dict:
        runtime = self.store.system_runtime()
        websocket_healthy = self.stream_handler.health_confirmed()
        control_ready = bool(getattr(self, "control_plane_ready", True))
        counts = self.store.operational_counts()
        ownership = self.last_ownership_audit or {}
        reconciliation_ready = bool(runtime.get("reconciliation_complete"))
        live_websocket_required = self.settings.mode in {
            TradingMode.TESTNET,
            TradingMode.LIVE,
        }
        handshake = getattr(self, "last_control_handshake", None)
        latest_forecast_age = (
            (handshake.capabilities or {}).get("latest_forecast_age_seconds")
            if handshake is not None
            else None
        )
        market_freshness_required = hasattr(
            self.settings, "prediction_max_age_seconds"
        )
        market_data_ready = bool(
            latest_forecast_age is not None
            and float(latest_forecast_age)
            <= float(getattr(self.settings, "prediction_max_age_seconds", 600))
        )
        setting_value = lambda name, default: getattr(
            getattr(self.settings, name, default), "value", getattr(self.settings, name, default)
        )
        degraded = bool(
            self.last_error
            or not reconciliation_ready
            or not control_ready
            or (market_freshness_required and not market_data_ready)
            or runtime.get("incident_mode") != IncidentMode.NORMAL.value
            or (live_websocket_required and not websocket_healthy)
        )
        ready = not degraded and not self.store.kill_switch_enabled()
        return {
            "status": "degraded" if degraded else "ok",
            "ready": ready,
            "mode": self.settings.mode.value,
            "deployment_environment": setting_value("app_environment", "development"),
            "service_role": setting_value("service_role", "executor"),
            "execution_mode": setting_value("execution_mode", self.settings.mode.value),
            "kill_switch": self.store.kill_switch_enabled(),
            "incident_mode": runtime.get("incident_mode"),
            "prediction_node_connected": control_ready,
            "control_plane_ready": control_ready,
            "ticket_intake": "ready" if ready else "frozen",
            "receipt_delivery": {
                "status": "blocked" if counts["receipt_dead_letter_count"] else "ready",
                "outbox_depth": counts["receipt_outbox_backlog"],
                "dead_letter_count": counts["receipt_dead_letter_count"],
            },
            "exchange_reconciliation": {
                "ready": reconciliation_ready,
                "last_error": getattr(self, "last_reconciliation_error", None),
            },
            "reconciliation_complete": reconciliation_ready,
            "account_ownership": self.last_ownership_audit,
            "position_protection": "ready" if reconciliation_ready else "degraded",
            "private_websocket": {
                "required": live_websocket_required,
                "connected": websocket_healthy,
            },
            "private_websocket_connected": websocket_healthy,
            "private_websocket_last_message_at": (
                self.stream_handler.last_message_at.isoformat()
                if self.stream_handler.last_message_at else None
            ),
            "private_stream_ignored_records": self.stream_handler.ignored_records,
            "market_data": (
                "ready"
                if market_data_ready
                else ("stale" if latest_forecast_age is not None else "unknown")
            ),
            "clock_skew": {
                "control_plane_seconds": (
                    handshake.clock_skew_seconds if handshake is not None else None
                ),
                "exchange_seconds": getattr(self, "_last_exchange_clock_drift", None),
            },
            "outbox_depth": counts["receipt_outbox_backlog"],
            "dead_letter_count": counts["receipt_dead_letter_count"],
            "disk_free": shutil.disk_usage(self.store.db_path.parent).free,
            "model_loaded": False,
            "latest_forecast_age": latest_forecast_age,
            "incomplete_ticket_count": len(self.store.incomplete_ticket_ids()),
            "last_poll_at": self.last_poll_at,
            "last_poll_result": self.last_poll_result,
            "last_error": self.last_error,
            "last_control_error": getattr(self, "last_control_error", None),
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
            self.last_reconciliation_error = None
            self._refresh_control_plane_handshake()
            self.last_error = self.last_control_error
            self.health_server.start()
        except Exception:
            if self.websocket is not None:
                self.websocket.stop()
                self.websocket = None
            self.health_server.stop()
            self.soak_monitor.stop()
            raise

    def run_once(self) -> None:
        exchange_ready = False
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
            self.last_reconciliation_error = None
            exchange_ready = True
        except Exception as exc:
            self._reconcile_inconsistencies += 1
            self.store.set_reconciliation_complete(False, "periodic reconciliation failed")
            self.last_reconciliation_error = f"{type(exc).__name__}: {exc}"
            logger.exception("exchange reconciliation/protection iteration failed")
        try:
            handshake = self._refresh_control_plane_handshake()
            if handshake.ready and exchange_ready:
                result = self.consumer_service.run_once()
                self.last_poll_at = datetime.now(timezone.utc).isoformat()
                self.last_poll_result = result.__dict__
                self.last_control_error = None
            elif handshake.ready:
                self.last_control_error = "exchange reconciliation is not ready"
        except Exception as exc:
            self.control_plane_ready = False
            self.last_control_error = f"{type(exc).__name__}: {exc}"
            self._freeze_for_control_plane(
                f"ticket intake failed closed: {self.last_control_error}"
            )
            logger.exception("control-plane ticket intake iteration failed")
        finally:
            self.last_error = self.last_reconciliation_error or self.last_control_error
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
