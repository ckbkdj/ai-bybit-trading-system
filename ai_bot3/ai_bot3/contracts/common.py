"""Compatibility facade for the canonical shared contract helpers."""

from ._shared import ensure_shared_contracts_path

ensure_shared_contracts_path()

from shadow_contracts.common import *  # noqa: E402,F401,F403
from shadow_contracts.common import __all__  # noqa: E402,F401
