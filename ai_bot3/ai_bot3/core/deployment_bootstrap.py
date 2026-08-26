"""Production bootstrap helpers that run before predictor modules are imported.

The legacy predictor constructs its global ``ResultManager`` during module import.
This bootstrap keeps that compatibility path while making the durable publication
outbox location explicit and identical to the standalone publisher process.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


_PATCH_MARKER = "_ai_bybit_environment_outbox_patch"


def resolve_publication_outbox_path() -> Path | None:
    modern = os.environ.get("FORECAST_PUBLICATION_OUTBOX_DB", "").strip()
    legacy = os.environ.get("FORECAST_PUBLICATION_OUTBOX", "").strip()
    if modern and legacy:
        modern_path = Path(modern).expanduser().resolve()
        legacy_path = Path(legacy).expanduser().resolve()
        if modern_path != legacy_path:
            raise RuntimeError(
                "FORECAST_PUBLICATION_OUTBOX_DB and legacy "
                "FORECAST_PUBLICATION_OUTBOX point to different files"
            )
    configured = modern or legacy
    if not configured:
        return None
    resolved = Path(configured).expanduser().resolve()
    os.environ["FORECAST_PUBLICATION_OUTBOX_DB"] = str(resolved)
    # One release of read-only compatibility for older host configuration.
    os.environ["FORECAST_PUBLICATION_OUTBOX"] = str(resolved)
    return resolved


def configure_predictor_runtime_paths() -> Path | None:
    """Patch the import-time ResultManager constructor before portfolio import.

    The change is deliberately narrow: explicit constructor arguments retain
    priority, while an omitted ``publication_outbox_db`` uses the canonical
    environment path.  The standalone publication worker reads the same setting.
    """

    configured = resolve_publication_outbox_path()
    if configured is None:
        return None

    from core import result_manager as result_manager_module

    manager_type = result_manager_module.ResultManager
    if getattr(manager_type, _PATCH_MARKER, False):
        return configured

    original_init = manager_type.__init__

    def environment_aware_init(
        self: Any,
        *args: Any,
        publication_outbox_db: Path | None = None,
        **kwargs: Any,
    ) -> None:
        selected = publication_outbox_db or configured
        original_init(
            self,
            *args,
            publication_outbox_db=selected,
            **kwargs,
        )

    manager_type.__init__ = environment_aware_init  # type: ignore[method-assign]
    setattr(manager_type, _PATCH_MARKER, True)
    return configured
