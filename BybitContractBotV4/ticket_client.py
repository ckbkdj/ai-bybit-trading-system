from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Optional

from contracts.execution_receipt_v1 import ExecutionReceipt
from contracts.operation_ticket_v1 import OperationTicket


class TicketApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class TicketPageItem:
    cursor: int
    ticket: OperationTicket


class TicketHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str = "",
        timeout_seconds: float = 10,
        verify: bool | str = True,
        session: Any = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self._session = session
        self._owns_session = session is None

    def _get_session(self):
        if self._session is None:
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
            adapter = HTTPAdapter(max_retries=retry)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)
        return self._session

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": "BybitTicketConsumer/1.0"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def fetch(self, after_cursor: int, consumer_id: str, limit: int = 100) -> list[TicketPageItem]:
        response = self._get_session().get(
            f"{self.base_url}/v1/tickets",
            params={"after_cursor": after_cursor, "limit": limit, "consumer_id": consumer_id},
            headers=self._headers(),
            timeout=self.timeout_seconds,
            verify=self.verify,
        )
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise TicketApiError("ticket page has no items array")
        result = []
        for item in items:
            if not isinstance(item, dict):
                raise TicketApiError("ticket page item is invalid")
            result.append(
                TicketPageItem(int(item["cursor"]), OperationTicket.model_validate(item["ticket"]))
            )
        return result

    def claim(self, ticket_id: str, consumer_id: str, lease_token: str, lease_sec: int = 60) -> bool:
        response = self._get_session().post(
            f"{self.base_url}/v1/tickets/{ticket_id}/claim",
            json={"consumer_id": consumer_id, "lease_token": lease_token, "lease_sec": lease_sec},
            headers=self._headers(),
            timeout=self.timeout_seconds,
            verify=self.verify,
        )
        if response.status_code == 409:
            return False
        response.raise_for_status()
        return bool(response.json().get("claimed"))

    def health(self) -> bool:
        try:
            response = self._get_session().get(
                f"{self.base_url}/v1/health",
                headers=self._headers(),
                timeout=self.timeout_seconds,
                verify=self.verify,
            )
            response.raise_for_status()
            return response.json().get("status") == "ok"
        except Exception:
            return False

    def latest_market_regime(self, symbol: str) -> str:
        try:
            response = self._get_session().get(
                f"{self.base_url}/v1/forecasts/latest",
                params={"symbol": symbol},
                headers=self._headers(),
                timeout=self.timeout_seconds,
                verify=self.verify,
            )
            response.raise_for_status()
            payload = response.json()
            return str((payload.get("regime") or {}).get("market_regime") or "unknown")
        except Exception:
            return "unknown"

    def post_receipt(self, receipt: ExecutionReceipt | dict[str, Any]) -> bool:
        payload = receipt.model_dump(mode="json") if isinstance(receipt, ExecutionReceipt) else receipt
        response = self._get_session().post(
            f"{self.base_url}/v1/executions",
            json=payload,
            headers=self._headers(),
            timeout=self.timeout_seconds,
            verify=self.verify,
        )
        response.raise_for_status()
        body = response.json()
        return bool(body.get("accepted") or body.get("receipt_id") == payload.get("receipt_id"))

    def close(self) -> None:
        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None


def deterministic_lease_token(consumer_id: str, ticket_id: str) -> str:
    digest = hashlib.sha256(f"{consumer_id}:{ticket_id}".encode()).hexdigest()
    return f"lease_{digest[:40]}"
