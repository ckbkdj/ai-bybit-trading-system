"""Canonical transport contracts shared by prediction and execution."""

from .execution_receipt_v1 import ExecutionReceipt
from .operation_ticket_v1 import OperationTicket
from .runtime import AppEnvironment, ExecutionMode, RuntimeIdentity, ServiceRole

__all__ = (
    "AppEnvironment",
    "ExecutionMode",
    "ExecutionReceipt",
    "OperationTicket",
    "RuntimeIdentity",
    "ServiceRole",
)
