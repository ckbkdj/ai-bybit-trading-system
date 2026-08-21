from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from runtime_config import TradingSettings


class PredictionUnavailable(RuntimeError):
    """The prediction is missing, stale, malformed or explicitly unreliable."""


def _timestamp(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            return float(text)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def parse_prediction_payload(
    payload: dict[str, Any],
    symbol: str,
    mode: str,
    max_age_seconds: int,
    *,
    now: float | None = None,
) -> dict[str, Any]:
    """Accept both the current `/predict` and legacy `/results` response shapes."""
    normalized_symbol = symbol.strip().upper()
    candidate: Any = None
    container_symbol: Any = None

    modes = payload.get("modes") if isinstance(payload, dict) else None
    if isinstance(modes, dict):
        container_symbol = payload.get("symbol")
        mode_bundle = modes.get(mode)
        if isinstance(mode_bundle, dict):
            candidate = mode_bundle.get("local_prediction", mode_bundle)
    elif isinstance(payload, dict):
        symbol_bundle = payload.get(normalized_symbol)
        if isinstance(symbol_bundle, dict):
            candidate = symbol_bundle.get(mode)

    if not isinstance(candidate, dict):
        raise PredictionUnavailable(f"missing prediction for {normalized_symbol}/{mode}")

    result = dict(candidate)
    result_symbol = str(result.get("symbol") or container_symbol or normalized_symbol).upper()
    if result_symbol != normalized_symbol:
        raise PredictionUnavailable(
            f"prediction symbol mismatch: expected {normalized_symbol}, got {result_symbol}"
        )

    trend = str(result.get("trend") or "").lower()
    if trend not in {"up", "down", "flat"}:
        raise PredictionUnavailable(f"invalid prediction trend: {trend or '<missing>'}")

    source_status = result.get("data_source_status")
    if source_status not in (None, "ok"):
        raise PredictionUnavailable(f"prediction data source is not ready: {source_status}")
    if result.get("data_source_reliable") is False:
        raise PredictionUnavailable(
            str(result.get("data_source_warning") or "prediction data source is unreliable")
        )

    generated_at = _timestamp(result.get("generated_at"))
    if generated_at is None:
        raise PredictionUnavailable("prediction has no parseable generated_at")
    current = datetime.now(timezone.utc).timestamp() if now is None else float(now)
    age = current - generated_at
    if age < -60:
        raise PredictionUnavailable(f"prediction timestamp is in the future: age={age:.1f}s")
    if age > max_age_seconds:
        raise PredictionUnavailable(
            f"prediction is stale: age={age:.1f}s, max_age={max_age_seconds}s"
        )

    result["trend"] = trend
    result["_prediction_age_seconds"] = max(0.0, age)
    result["_prediction_contract"] = "legacy-compatible-v1"
    return result


class PredictionClient:
    def __init__(self, settings: TradingSettings, session: Any = None):
        self.settings = settings
        self._session = session
        self._owns_session = session is None

    def _get_session(self):
        if self._session is None:
            # Import lazily so parsing and safety tests remain fully offline.
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3 import Retry

            retry = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
                raise_on_status=False,
            )
            self._session = requests.Session()
            self._session.mount("https://", HTTPAdapter(max_retries=retry))
            self._session.mount("http://", HTTPAdapter(max_retries=retry))
        return self._session

    def fetch(
        self,
        symbol: str,
        mode: str = "scalping",
        *,
        api_url: str | None = None,
    ) -> dict[str, Any]:
        url = api_url or self.settings.prediction_url(symbol)
        headers = {"Accept": "application/json", "User-Agent": "BybitContractBotV4/0.5"}
        if self.settings.prediction_api_token:
            headers["Authorization"] = f"Bearer {self.settings.prediction_api_token}"
        response = self._get_session().get(
            url,
            headers=headers,
            verify=self.settings.requests_verify,
            timeout=self.settings.prediction_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise PredictionUnavailable("prediction response is not a JSON object")
        return parse_prediction_payload(
            payload,
            symbol,
            mode,
            self.settings.prediction_max_age_seconds,
        )

    def close(self) -> None:
        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None
