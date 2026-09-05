from __future__ import annotations

import hashlib
import time
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


@dataclass(frozen=True)
class TicketPage:
    items: list[TicketPageItem]
    next_cursor: int
    backlog: dict[str, Any]


@dataclass(frozen=True)
class ReceiptDeliveryResult:
    outcome: str
    http_status: int | None = None
    error_code: str = ""
    detail: str = ""

    @property
    def delivered(self) -> bool:
        return self.outcome == "delivered"


@dataclass(frozen=True)
class HandshakeResult:
    ready: bool
    reason: str
    clock_skew_seconds: float
    capabilities: dict[str, Any]
    ownership_epoch: int | None = None


class TicketHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str = "",
        timeout_seconds: float = 10,
        verify: bool | str = True,
        client_cert: tuple[str, str] | None = None,
        consumer_id: str = "",
        session: Any = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.client_cert = client_cert
        self.consumer_id = consumer_id
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
        if self.consumer_id:
            headers["X-Executor-Consumer-ID"] = self.consumer_id
        return headers

    def _transport(self) -> dict[str, Any]:
        options: dict[str, Any] = {
            "headers": self._headers(),
            "timeout": self.timeout_seconds,
            "verify": self.verify,
        }
        if self.client_cert:
            options["cert"] = self.client_cert
        return options

    def fetch_page(
        self, after_cursor: int, consumer_id: str, limit: int = 100
    ) -> TicketPage:
        response = self._get_session().get(
            f"{self.base_url}/v1/tickets",
            params={"after_cursor": after_cursor, "limit": limit, "consumer_id": consumer_id},
            **self._transport(),
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
        next_cursor = int(payload.get("next_cursor", after_cursor))
        if next_cursor < after_cursor:
            raise TicketApiError("control-plane cursor moved backwards")
        backlog = payload.get("backlog") if isinstance(payload.get("backlog"), dict) else {}
        return TicketPage(result, next_cursor, backlog)

    def fetch(self, after_cursor: int, consumer_id: str, limit: int = 100) -> list[TicketPageItem]:
        return self.fetch_page(after_cursor, consumer_id, limit).items

    def claim(
        self, ticket_id: str, consumer_id: str, lease_token: str, lease_sec: int = 60
    ) -> Optional[int]:
        response = self._get_session().post(
            f"{self.base_url}/v1/tickets/{ticket_id}/claim",
            json={"consumer_id": consumer_id, "lease_token": lease_token, "lease_sec": lease_sec},
            **self._transport(),
        )
        if response.status_code == 409:
            return None
        response.raise_for_status()
        payload = response.json()
        if not payload.get("claimed"):
            return None
        claim_epoch = payload.get("claim_epoch")
        if not isinstance(claim_epoch, int) or claim_epoch < 1:
            raise TicketApiError("claim response has no valid fencing epoch")
        return claim_epoch

    def health(self) -> bool:
        try:
            response = self._get_session().get(
                f"{self.base_url}/v1/health",
                **self._transport(),
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
                **self._transport(),
            )
            response.raise_for_status()
            payload = response.json()
            return str((payload.get("regime") or {}).get("market_regime") or "unknown")
        except Exception:
            return "unknown"

    def deliver_receipt(
        self, receipt: ExecutionReceipt | dict[str, Any]
    ) -> ReceiptDeliveryResult:
        payload = receipt.model_dump(mode="json") if isinstance(receipt, ExecutionReceipt) else receipt
        try:
            response = self._get_session().post(
                f"{self.base_url}/v1/executions",
                json=payload,
                **self._transport(),
            )
        except Exception as exc:
            return ReceiptDeliveryResult(
                "retry", None, "TRANSPORT_ERROR", f"{type(exc).__name__}: {exc}"
            )
        status = int(response.status_code)
        try:
            body = response.json()
        except Exception:
            body = {}
        if 200 <= status < 300:
            if body.get("accepted") or body.get("receipt_id") == payload.get("receipt_id"):
                return ReceiptDeliveryResult("delivered", status, "DELIVERED")
            return ReceiptDeliveryResult(
                "retry", status, "INVALID_SUCCESS_RESPONSE", "receipt acknowledgement missing"
            )
        if status in {401, 403}:
            return ReceiptDeliveryResult("security", status, "AUTHORIZATION_FAILURE")
        if status == 409:
            content_match = bool(
                body.get("idempotent")
                or body.get("content_match")
                or (
                    body.get("receipt_id") == payload.get("receipt_id")
                    and body.get("conflict") == "identical"
                )
            )
            return ReceiptDeliveryResult(
                "delivered" if content_match else "conflict",
                status,
                "IDEMPOTENT_CONFLICT" if content_match else "CONTENT_CONFLICT",
            )
        if status in {404, 422}:
            return ReceiptDeliveryResult("dead_letter", status, f"HTTP_{status}")
        if status == 429 or status >= 500:
            return ReceiptDeliveryResult("retry", status, f"HTTP_{status}")
        return ReceiptDeliveryResult("dead_letter", status, f"HTTP_{status}")

    def post_receipt(self, receipt: ExecutionReceipt | dict[str, Any]) -> bool:
        return self.deliver_receipt(receipt).delivered

    def capabilities(self) -> dict[str, Any]:
        response = self._get_session().get(
            f"{self.base_url}/v1/capabilities", **self._transport()
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise TicketApiError("capabilities response is invalid")
        return payload

    def server_time(self) -> float:
        started = time.time()
        response = self._get_session().get(
            f"{self.base_url}/v1/time", **self._transport()
        )
        finished = time.time()
        response.raise_for_status()
        payload = response.json()
        server = float(payload["unix_time"])
        midpoint = (started + finished) / 2
        return server - midpoint

    @staticmethod
    def _version_tuple(value: str) -> tuple[int, ...]:
        try:
            return tuple(int(part) for part in value.split(".")[:3])
        except ValueError as exc:
            raise TicketApiError(f"invalid semantic version: {value}") from exc

    def activate_consumer(
        self, consumer_id: str, instance_id: str, account_id: str
    ) -> int | None:
        response = self._get_session().post(
            f"{self.base_url}/v1/consumers/activate",
            json={
                "consumer_id": consumer_id,
                "instance_id": instance_id,
                "account_id": account_id,
                "lease_sec": 60,
            },
            **self._transport(),
        )
        if response.status_code == 409:
            return None
        response.raise_for_status()
        payload = response.json()
        return int(payload["ownership_epoch"])

    def handshake(
        self,
        *,
        consumer_id: str,
        instance_id: str,
        account_id: str,
        executor_version: str,
        expected_cluster_id: str,
        expected_deployment_id: str,
        max_clock_skew_seconds: float,
    ) -> HandshakeResult:
        try:
            capabilities = self.capabilities()
            ticket_schemas = set(capabilities.get("supported_ticket_schemas") or [])
            receipt_schemas = set(capabilities.get("supported_receipt_schemas") or [])
            minimum = str(capabilities.get("minimum_executor_version") or "")
            if "operation-ticket.v1" not in ticket_schemas:
                return HandshakeResult(False, "ticket_schema_incompatible", float("inf"), capabilities)
            if "execution-receipt.v1" not in receipt_schemas:
                return HandshakeResult(False, "receipt_schema_incompatible", float("inf"), capabilities)
            if self._version_tuple(executor_version) < self._version_tuple(minimum):
                return HandshakeResult(False, "executor_version_too_old", float("inf"), capabilities)
            if expected_cluster_id and capabilities.get("cluster_id") != expected_cluster_id:
                return HandshakeResult(False, "cluster_id_mismatch", float("inf"), capabilities)
            if (
                expected_deployment_id
                and capabilities.get("deployment_id") != expected_deployment_id
            ):
                return HandshakeResult(False, "deployment_id_mismatch", float("inf"), capabilities)
            skew = self.server_time()
            if abs(skew) > max_clock_skew_seconds:
                return HandshakeResult(False, "clock_skew", skew, capabilities)
            ownership_epoch = self.activate_consumer(consumer_id, instance_id, account_id)
            if ownership_epoch is None:
                return HandshakeResult(False, "consumer_account_already_active", skew, capabilities)
            return HandshakeResult(True, "ready", skew, capabilities, ownership_epoch)
        except Exception as exc:
            return HandshakeResult(
                False,
                f"{type(exc).__name__}: {exc}",
                float("inf"),
                {},
            )

    def close(self) -> None:
        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None


def deterministic_lease_token(
    consumer_id: str, ticket_id: str, service_session_id: str = "legacy-session"
) -> str:
    """Return a retry-stable token that changes whenever the process restarts."""

    digest = hashlib.sha256(
        f"{consumer_id}:{service_session_id}:{ticket_id}".encode()
    ).hexdigest()
    return f"lease_{digest[:40]}"
