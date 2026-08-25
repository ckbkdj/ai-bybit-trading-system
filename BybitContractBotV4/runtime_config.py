from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping

from shadow_contracts.runtime import (
    AppEnvironment,
    ExecutionMode,
    RuntimeConfigurationError,
    RuntimeIdentity,
    ServiceRole,
)


class SettingsError(RuntimeError):
    """Raised when runtime configuration could permit an unsafe launch."""


class TradingMode(str, Enum):
    SHADOW = "shadow"
    TESTNET = "testnet"
    LIVE = "live"


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class TradingSettings:
    root: Path
    app_environment: AppEnvironment
    service_role: ServiceRole
    execution_mode: ExecutionMode
    host_id: str
    cluster_id: str
    deployment_id: str
    mode: TradingMode
    api_key: str
    secret_key: str
    enable_live: bool
    live_approval_id: str
    shadow_account_equity_usdt: float
    prediction_api_base_url: str
    prediction_api_route: str
    prediction_api_token: str
    prediction_timeout_seconds: float
    prediction_max_age_seconds: int
    prediction_tls_verify: bool
    prediction_ca_bundle: str
    lark_webhook_url: str
    ticket_api_base_url: str
    ticket_api_token: str
    ticket_consumer_id: str
    control_plane_client_cert: str
    control_plane_client_key: str
    control_plane_cert_identity: str
    executor_version: str
    max_control_plane_clock_drift_seconds: float
    ticket_poll_seconds: float
    execution_db_path: str
    max_risk_per_trade_pct: float
    max_daily_loss_pct: float
    max_weekly_loss_pct: float
    max_equity_drawdown_pct: float
    max_gross_leverage: float
    max_correlated_exposure_pct: float
    correlated_symbols: frozenset[str]
    max_margin_utilization: float
    max_consecutive_losses: int
    max_exchange_clock_drift_seconds: float
    require_websocket_confirmation: bool
    health_host: str
    health_port: int
    position_mode: str
    dedicated_subaccount: bool
    position_owner_id: str
    allow_manual_orders: bool
    approved_strategy_release_id: str
    app_code_commit: str

    @classmethod
    def load(
        cls,
        root: Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "TradingSettings":
        service_root = (root or Path(__file__).resolve().parent).resolve()
        file_values = _read_env_file(service_root / ".env.local")
        process_values = dict(os.environ if environ is None else environ)
        merged_values = {**file_values, **process_values}

        def value(key: str, default: str = "") -> str:
            # Process-level values intentionally override the local env file.
            return str(process_values.get(key, file_values.get(key, default))).strip()

        try:
            identity = RuntimeIdentity.load(
                merged_values,
                expected_role=ServiceRole.EXECUTOR,
            )
        except RuntimeConfigurationError as exc:
            raise SettingsError(str(exc)) from exc
        mode = {
            ExecutionMode.PAPER: TradingMode.SHADOW,
            ExecutionMode.TESTNET: TradingMode.TESTNET,
            ExecutionMode.LIVE: TradingMode.LIVE,
        }[identity.execution_mode]

        api_key = value("BYBIT_API_KEY")
        secret_key = value("BYBIT_SECRET_KEY")
        enable_live = _truthy(value("BYBIT_ENABLE_LIVE", "false"))
        live_approval_id = value("BYBIT_LIVE_APPROVAL_ID")

        if mode is TradingMode.LIVE and (not enable_live or not live_approval_id):
            raise SettingsError(
                "live mode is blocked: BYBIT_ENABLE_LIVE=true and a non-empty "
                "BYBIT_LIVE_APPROVAL_ID are both required for an approved launch"
            )
        if mode in {TradingMode.TESTNET, TradingMode.LIVE} and not (api_key and secret_key):
            raise SettingsError(f"{mode.value} mode requires BYBIT_API_KEY and BYBIT_SECRET_KEY")

        try:
            shadow_equity = float(value("SHADOW_ACCOUNT_EQUITY_USDT", "10000"))
            timeout = float(value("PREDICTION_TIMEOUT_SECONDS", "10"))
            max_age = int(value("PREDICTION_MAX_AGE_SECONDS", "600"))
            ticket_poll = float(value("TICKET_POLL_SECONDS", "2"))
            max_risk_per_trade = float(value("MAX_RISK_PER_TRADE_PCT", "0.0025"))
            max_daily_loss = float(value("MAX_DAILY_LOSS_PCT", "0.005"))
            max_weekly_loss = float(value("MAX_WEEKLY_LOSS_PCT", "0.015"))
            max_equity_drawdown = float(value("MAX_EQUITY_DRAWDOWN_PCT", "0.03"))
            max_gross_leverage = float(value("MAX_GROSS_LEVERAGE", "2"))
            max_correlated = float(value("MAX_CORRELATED_EXPOSURE_PCT", "0.35"))
            max_margin_utilization = float(value("MAX_MARGIN_UTILIZATION", "0.70"))
            max_consecutive_losses = int(value("MAX_CONSECUTIVE_LOSSES", "4"))
            max_clock_drift = float(value("MAX_EXCHANGE_CLOCK_DRIFT_SECONDS", "2"))
            max_control_plane_clock_drift = float(
                value(
                    "MAX_CONTROL_PLANE_CLOCK_DRIFT_SECONDS",
                    "5" if identity.execution_mode is ExecutionMode.PAPER else "2",
                )
            )
            health_port = int(value("HEALTH_PORT", "8787"))
        except ValueError as exc:
            raise SettingsError("numeric runtime settings contain an invalid value") from exc
        if shadow_equity <= 0 or timeout <= 0 or max_age <= 0 or ticket_poll <= 0:
            raise SettingsError("shadow equity, prediction timeout and max age must be positive")
        if not 0 < max_risk_per_trade <= 0.0025:
            raise SettingsError("MAX_RISK_PER_TRADE_PCT cannot exceed 0.0025")
        if not 0 < max_daily_loss <= 0.005:
            raise SettingsError("MAX_DAILY_LOSS_PCT cannot exceed 0.005")
        if not 0 < max_weekly_loss <= 0.015:
            raise SettingsError("MAX_WEEKLY_LOSS_PCT cannot exceed 0.015")
        if not 0 < max_equity_drawdown <= 0.03:
            raise SettingsError("MAX_EQUITY_DRAWDOWN_PCT cannot exceed 0.03")
        if not 0 < max_gross_leverage <= 2:
            raise SettingsError("MAX_GROSS_LEVERAGE cannot exceed 2")
        if not 0 < max_correlated <= 1 or not 0 < max_margin_utilization <= 1:
            raise SettingsError("risk exposure/margin limits must be in (0, 1]")
        if (
            max_consecutive_losses <= 0
            or max_clock_drift <= 0
            or max_control_plane_clock_drift <= 0
        ):
            raise SettingsError("loss streak and clock drift limits must be positive")
        if not 1 <= health_port <= 65535:
            raise SettingsError("HEALTH_PORT is invalid")

        route = value("PREDICTION_API_ROUTE", "/results/{symbol}")
        if "{symbol}" not in route:
            raise SettingsError("PREDICTION_API_ROUTE must contain {symbol}")
        # The active ledger is intentionally net-position based.  Until orders,
        # fills, snapshots and ownership checks are keyed by positionIdx/leg,
        # two simultaneous hedge legs cannot be represented without ambiguity.
        position_mode = value("BYBIT_POSITION_MODE", "one_way").lower()
        if position_mode not in {"hedge", "one_way"}:
            raise SettingsError("BYBIT_POSITION_MODE must be hedge or one_way")
        if mode in {TradingMode.TESTNET, TradingMode.LIVE} and position_mode == "hedge":
            raise SettingsError(
                f"{mode.value} hedge mode is blocked until the execution ledger is positionIdx-aware"
            )
        dedicated_subaccount = _truthy(value("BYBIT_DEDICATED_SUBACCOUNT", "false"))
        position_owner_id = value("POSITION_OWNER_ID", "shadow-primary-owner")
        allow_manual_orders = _truthy(value("BYBIT_ALLOW_MANUAL_ORDERS", "false"))
        approved_strategy_release_id = value("APPROVED_STRATEGY_RELEASE_ID")
        app_code_commit = value("APP_CODE_COMMIT")
        ticket_consumer_id = value("TICKET_CONSUMER_ID", "bybit-v4-primary")
        ticket_api_base_url = value(
            "TICKET_API_BASE_URL",
            value("PREDICTION_API_BASE_URL", "https://crypto_api.hk.ie520.com"),
        ).rstrip("/")
        ticket_api_token = value(
            "CONTROL_PLANE_API_TOKEN",
            value("TICKET_API_TOKEN", value("PREDICTION_API_TOKEN")),
        )
        control_plane_client_cert = value("CONTROL_PLANE_MTLS_CERT")
        control_plane_client_key = value("CONTROL_PLANE_MTLS_KEY")
        control_plane_cert_identity = value(
            "CONTROL_PLANE_CERT_IDENTITY", ticket_consumer_id
        )
        executor_version = value("EXECUTOR_VERSION", "1.0.0")
        if identity.app_environment is AppEnvironment.PRODUCTION:
            missing_security = [
                name
                for name, configured in (
                    ("CONTROL_PLANE_API_TOKEN", ticket_api_token),
                    ("CONTROL_PLANE_MTLS_CERT", control_plane_client_cert),
                    ("CONTROL_PLANE_MTLS_KEY", control_plane_client_key),
                    ("PREDICTION_CA_BUNDLE", value("PREDICTION_CA_BUNDLE")),
                )
                if not configured
            ]
            if missing_security:
                raise SettingsError(
                    "production executor security is incomplete: "
                    + ", ".join(missing_security)
                )
            if not ticket_api_base_url.lower().startswith("https://"):
                raise SettingsError("production executor requires an HTTPS control plane")
            if control_plane_cert_identity != ticket_consumer_id:
                raise SettingsError(
                    "CONTROL_PLANE_CERT_IDENTITY must equal TICKET_CONSUMER_ID"
                )
        if mode in {TradingMode.TESTNET, TradingMode.LIVE}:
            if not dedicated_subaccount:
                raise SettingsError(
                    f"{mode.value} requires BYBIT_DEDICATED_SUBACCOUNT=true"
                )
            if len(position_owner_id) < 8:
                raise SettingsError(f"{mode.value} requires a stable POSITION_OWNER_ID")
            if allow_manual_orders:
                raise SettingsError(
                    f"{mode.value} dedicated execution account forbids manual orders"
                )
            if len(approved_strategy_release_id) < 8:
                raise SettingsError(
                    f"{mode.value} requires APPROVED_STRATEGY_RELEASE_ID"
                )
            if len(app_code_commit) < 7 or app_code_commit == "workspace-uncommitted":
                raise SettingsError(f"{mode.value} requires a deployed APP_CODE_COMMIT")

        return cls(
            root=service_root,
            app_environment=identity.app_environment,
            service_role=identity.service_role,
            execution_mode=identity.execution_mode,
            host_id=identity.host_id,
            cluster_id=identity.cluster_id,
            deployment_id=identity.deployment_id,
            mode=mode,
            api_key=api_key,
            secret_key=secret_key,
            enable_live=enable_live,
            live_approval_id=live_approval_id,
            shadow_account_equity_usdt=shadow_equity,
            prediction_api_base_url=value(
                "PREDICTION_API_BASE_URL", "https://crypto_api.hk.ie520.com"
            ).rstrip("/"),
            prediction_api_route=route,
            prediction_api_token=value("PREDICTION_API_TOKEN"),
            prediction_timeout_seconds=timeout,
            prediction_max_age_seconds=max_age,
            prediction_tls_verify=_truthy(value("PREDICTION_TLS_VERIFY", "true")),
            prediction_ca_bundle=value("PREDICTION_CA_BUNDLE"),
            lark_webhook_url=value("LARK_WEBHOOK_URL"),
            ticket_api_base_url=ticket_api_base_url,
            ticket_api_token=ticket_api_token,
            ticket_consumer_id=ticket_consumer_id,
            control_plane_client_cert=control_plane_client_cert,
            control_plane_client_key=control_plane_client_key,
            control_plane_cert_identity=control_plane_cert_identity,
            executor_version=executor_version,
            max_control_plane_clock_drift_seconds=max_control_plane_clock_drift,
            ticket_poll_seconds=ticket_poll,
            execution_db_path=value(
                "EXECUTION_DB_PATH", str(service_root / "execution_state.sqlite3")
            ),
            max_risk_per_trade_pct=max_risk_per_trade,
            max_daily_loss_pct=max_daily_loss,
            max_weekly_loss_pct=max_weekly_loss,
            max_equity_drawdown_pct=max_equity_drawdown,
            max_gross_leverage=max_gross_leverage,
            max_correlated_exposure_pct=max_correlated,
            correlated_symbols=frozenset(
                item.strip().upper()
                for item in value(
                    "CORRELATED_SYMBOLS",
                    "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,1000PEPEUSDT",
                ).split(",")
                if item.strip()
            ),
            max_margin_utilization=max_margin_utilization,
            max_consecutive_losses=max_consecutive_losses,
            max_exchange_clock_drift_seconds=max_clock_drift,
            require_websocket_confirmation=_truthy(
                value("REQUIRE_WEBSOCKET_CONFIRMATION", "true")
            ),
            health_host=value("HEALTH_HOST", "127.0.0.1"),
            health_port=health_port,
            position_mode=position_mode,
            dedicated_subaccount=dedicated_subaccount,
            position_owner_id=position_owner_id,
            allow_manual_orders=allow_manual_orders,
            approved_strategy_release_id=approved_strategy_release_id,
            app_code_commit=app_code_commit,
        )

    @property
    def requests_verify(self) -> bool | str:
        if self.prediction_ca_bundle:
            return self.prediction_ca_bundle
        return self.prediction_tls_verify

    @property
    def requests_client_cert(self) -> tuple[str, str] | None:
        if self.control_plane_client_cert and self.control_plane_client_key:
            return self.control_plane_client_cert, self.control_plane_client_key
        return None

    def prediction_url(self, symbol: str) -> str:
        normalized = symbol.strip().upper()
        route = self.prediction_api_route.format(symbol=normalized)
        return f"{self.prediction_api_base_url}/{route.lstrip('/')}"
