"""Canonical transport contracts shared by prediction and execution.

Contract models are imported lazily so repository/audit utilities do not need the
Pydantic runtime merely to resolve a commit SHA.
"""

from .runtime import AppEnvironment, ExecutionMode, RuntimeIdentity, ServiceRole

__all__ = (
    "AppEnvironment",
    "ExecutionMode",
    "ExecutionReceipt",
    "OperationTicket",
    "RuntimeIdentity",
    "ServiceRole",
)


def __getattr__(name: str):
    if name == "ExecutionReceipt":
        from .execution_receipt_v1 import ExecutionReceipt

        return ExecutionReceipt
    if name == "OperationTicket":
        from .operation_ticket_v1 import OperationTicket

        return OperationTicket
    raise AttributeError(name)
