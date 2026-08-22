from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

from ticket_store import ExecutionStore


def _records(message: dict[str, Any]) -> Iterable[dict[str, Any]]:
    data = message.get("data") or []
    return data if isinstance(data, list) else [data]


def _timestamp_ms(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(float(value) / 1000, timezone.utc)
    except (TypeError, ValueError, OSError):
        return datetime.now(timezone.utc)


class PrivateStreamHandler:
    def __init__(
        self,
        store: ExecutionStore,
        on_entry_filled: Optional[Callable[[str], None]] = None,
    ):
        self.store = store
        self.on_entry_filled = on_entry_filled
        self.connected = False
        self.last_message_at: Optional[datetime] = None

    def mark_connected(self) -> None:
        self.connected = True
        self.last_message_at = datetime.now(timezone.utc)

    def mark_disconnected(self) -> None:
        self.connected = False

    def on_order(self, message: dict[str, Any]) -> None:
        self.last_message_at = datetime.now(timezone.utc)
        for record in _records(message):
            link_id = str(record.get("orderLinkId") or "")
            if not link_id:
                continue
            status = str(record.get("orderStatus") or "UNKNOWN")
            self.store.acknowledge_order(link_id, status, record)

    def on_execution(self, message: dict[str, Any]) -> None:
        self.last_message_at = datetime.now(timezone.utc)
        for record in _records(message):
            exec_id = str(record.get("execId") or "")
            link_id = str(record.get("orderLinkId") or "")
            if not exec_id or not link_id:
                continue
            inserted = self.store.record_fill(
                exec_id=exec_id,
                order_link_id=link_id,
                bybit_order_id=str(record.get("orderId") or "") or None,
                quantity=float(record.get("execQty") or 0),
                price=float(record.get("execPrice") or 0),
                fee=abs(float(record.get("execFee") or 0)),
                executed_at=_timestamp_ms(record.get("execTime")),
                raw=record,
            )
            order = self.store.order(link_id)
            if (
                inserted
                and order
                and order.get("role") == "entry"
                and str(order.get("order_status")).upper() == "FILLED"
                and self.on_entry_filled
            ):
                self.on_entry_filled(order["ticket_id"])


class BybitPrivateWebSocket:
    """Optional pybit connector. Construction and network start are always explicit."""

    def __init__(self, api_key: str, secret_key: str, *, testnet: bool, handler: PrivateStreamHandler):
        self.api_key = api_key
        self.secret_key = secret_key
        self.testnet = testnet
        self.handler = handler
        self._socket = None

    def start(self) -> None:
        from pybit.unified_trading import WebSocket

        self._socket = WebSocket(
            testnet=self.testnet,
            channel_type="private",
            api_key=self.api_key,
            api_secret=self.secret_key,
        )
        self._socket.order_stream(callback=self.handler.on_order)
        self._socket.execution_stream(callback=self.handler.on_execution)
        self.handler.mark_connected()

    def stop(self) -> None:
        if self._socket is not None:
            try:
                self._socket.exit()
            finally:
                self._socket = None
        self.handler.mark_disconnected()
