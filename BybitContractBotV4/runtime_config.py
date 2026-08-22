from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping


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
    mode: TradingMode
    api_key: str
    secret_key: str
    enable_live: bool
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
    ticket_poll_seconds: float
    execution_db_path: str
    max_daily_loss_pct: float
    max_gross_leverage: float
    max_correlated_exposure_pct: float
    max_margin_utilization: float
    max_consecutive_losses: int
    max_exchange_clock_drift_seconds: float
    require_websocket_confirmation: bool
    health_host: str
    health_port: int
    position_mode: str

    @classmethod
    def load(
        cls,
        root: Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "TradingSettings":
        service_root = (root or Path(__file__).resolve().parent).resolve()
        file_values = _read_env_file(service_root / ".env.local")
        process_values = dict(os.environ if environ is None else environ)

        def value(key: str, default: str = "") -> str:
            # Process-level values intentionally override the local env file.
            return str(process_values.get(key, file_values.get(key, default))).strip()

        raw_mode = value("BYBIT_TRADING_MODE", TradingMode.SHADOW.value).lower()
        try:
            mode = TradingMode(raw_mode)
        except ValueError as exc:
            raise SettingsError(
                f"BYBIT_TRADING_MODE must be one of: {', '.join(m.value for m in TradingMode)}"
            ) from exc

        api_key = value("BYBIT_API_KEY")
        secret_key = value("BYBIT_SECRET_KEY")
        enable_live = _truthy(value("BYBIT_ENABLE_LIVE", "false"))

        if mode is TradingMode.LIVE and not enable_live:
            raise SettingsError(
                "live mode is blocked: set BYBIT_ENABLE_LIVE=true only during an approved launch"
            )
        if mode in {TradingMode.TESTNET, TradingMode.LIVE} and not (api_key and secret_key):
            raise SettingsError(f"{mode.value} mode requires BYBIT_API_KEY and BYBIT_SECRET_KEY")

        try:
            shadow_equity = float(value("SHADOW_ACCOUNT_EQUITY_USDT", "10000"))
            timeout = float(value("PREDICTION_TIMEOUT_SECONDS", "10"))
            max_age = int(value("PREDICTION_MAX_AGE_SECONDS", "600"))
            ticket_poll = float(value("TICKET_POLL_SECONDS", "2"))
            max_daily_loss = float(value("MAX_DAILY_LOSS_PCT", "0.02"))
            max_gross_leverage = float(value("MAX_GROSS_LEVERAGE", "2"))
            max_correlated = float(value("MAX_CORRELATED_EXPOSURE_PCT", "0.35"))
            max_margin_utilization = float(value("MAX_MARGIN_UTILIZATION", "0.70"))
            max_consecutive_losses = int(value("MAX_CONSECUTIVE_LOSSES", "4"))
            max_clock_drift = float(value("MAX_EXCHANGE_CLOCK_DRIFT_SECONDS", "2"))
            health_port = int(value("HEALTH_PORT", "8787"))
        except ValueError as exc:
            raise SettingsError("numeric runtime settings contain an invalid value") from exc
        if shadow_equity <= 0 or timeout <= 0 or max_age <= 0 or ticket_poll <= 0:
            raise SettingsError("shadow equity, prediction timeout and max age must be positive")
        if not 0 < max_daily_loss <= 1 or max_gross_leverage <= 0:
            raise SettingsError("risk loss/leverage limits contain invalid values")
        if not 0 < max_correlated <= 1 or not 0 < max_margin_utilization <= 1:
            raise SettingsError("risk exposure/margin limits must be in (0, 1]")
        if max_consecutive_losses <= 0 or max_clock_drift <= 0:
            raise SettingsError("loss streak and clock drift limits must be positive")
        if not 1 <= health_port <= 65535:
            raise SettingsError("HEALTH_PORT is invalid")

        route = value("PREDICTION_API_ROUTE", "/results/{symbol}")
        if "{symbol}" not in route:
            raise SettingsError("PREDICTION_API_ROUTE must contain {symbol}")
        position_mode = value("BYBIT_POSITION_MODE", "hedge").lower()
        if position_mode not in {"hedge", "one_way"}:
            raise SettingsError("BYBIT_POSITION_MODE must be hedge or one_way")

        return cls(
            root=service_root,
            mode=mode,
            api_key=api_key,
            secret_key=secret_key,
            enable_live=enable_live,
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
            ticket_api_base_url=value(
                "TICKET_API_BASE_URL",
                value("PREDICTION_API_BASE_URL", "https://crypto_api.hk.ie520.com"),
            ).rstrip("/"),
            ticket_api_token=value("TICKET_API_TOKEN", value("PREDICTION_API_TOKEN")),
            ticket_consumer_id=value("TICKET_CONSUMER_ID", "bybit-v4-primary"),
            ticket_poll_seconds=ticket_poll,
            execution_db_path=value(
                "EXECUTION_DB_PATH", str(service_root / "execution_state.sqlite3")
            ),
            max_daily_loss_pct=max_daily_loss,
            max_gross_leverage=max_gross_leverage,
            max_correlated_exposure_pct=max_correlated,
            max_margin_utilization=max_margin_utilization,
            max_consecutive_losses=max_consecutive_losses,
            max_exchange_clock_drift_seconds=max_clock_drift,
            require_websocket_confirmation=_truthy(
                value("REQUIRE_WEBSOCKET_CONFIRMATION", "true")
            ),
            health_host=value("HEALTH_HOST", "127.0.0.1"),
            health_port=health_port,
            position_mode=position_mode,
        )

    @property
    def requests_verify(self) -> bool | str:
        if self.prediction_ca_bundle:
            return self.prediction_ca_bundle
        return self.prediction_tls_verify

    def prediction_url(self, symbol: str) -> str:
        normalized = symbol.strip().upper()
        route = self.prediction_api_route.format(symbol=normalized)
        return f"{self.prediction_api_base_url}/{route.lstrip('/')}"
