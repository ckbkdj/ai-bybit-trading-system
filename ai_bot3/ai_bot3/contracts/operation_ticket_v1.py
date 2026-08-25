"""Compatibility facade for the canonical OperationTicket contract."""

from ._shared import ensure_shared_contracts_path

ensure_shared_contracts_path()

from shadow_contracts.operation_ticket_v1 import *  # noqa: E402,F401,F403
