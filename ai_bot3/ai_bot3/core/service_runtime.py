"""Prediction-node runtime identity facade."""

from __future__ import annotations

import os
from typing import Mapping

from shadow_contracts.runtime import (
    RuntimeIdentity,
    ServiceRole,
    assert_loaded_module_boundary,
)


def load_predictor_runtime(
    environ: Mapping[str, str] | None = None,
    *,
    check_imports: bool = True,
) -> RuntimeIdentity:
    identity = RuntimeIdentity.load(
        os.environ if environ is None else environ,
        expected_role=ServiceRole.PREDICTOR,
    )
    if check_imports:
        assert_loaded_module_boundary(identity)
    return identity
