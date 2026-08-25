from __future__ import annotations

from types import ModuleType

import pytest

from core.service_runtime import load_predictor_runtime
from shadow_contracts.runtime import (
    RuntimeConfigurationError,
    assert_loaded_module_boundary,
)


def test_predictor_rejects_private_bybit_credentials_and_execution_db():
    for setting in ("BYBIT_API_KEY", "BYBIT_SECRET_KEY", "EXECUTION_DB_PATH"):
        with pytest.raises(RuntimeConfigurationError, match="SERVICE_ROLE=predictor forbids"):
            load_predictor_runtime(
                {"SERVICE_ROLE": "predictor", setting: "must-not-be-present"},
                check_imports=False,
            )


def test_predictor_production_identity_is_complete_and_paper_only_fixture():
    identity = load_predictor_runtime(
        {
            "APP_ENV": "production",
            "SERVICE_ROLE": "predictor",
            "EXECUTION_MODE": "paper",
            "BYBIT_TRADING_MODE": "shadow",
            "HOST_ID": "predictor-01",
            "CLUSTER_ID": "two-node-primary",
            "DEPLOYMENT_ID": "paper-20260825",
        },
        check_imports=False,
    )
    assert identity.execution_mode.value == "paper"
    assert identity.host_id == "predictor-01"


def test_predictor_refuses_loaded_executor_implementation_module():
    identity = load_predictor_runtime(
        {"SERVICE_ROLE": "predictor"}, check_imports=False
    )
    module = ModuleType("forbidden_executor")
    module.__file__ = "C:/repo/BybitContractBotV4/service_main.py"
    with pytest.raises(RuntimeConfigurationError, match="forbidden role modules"):
        assert_loaded_module_boundary(identity, {"forbidden_executor": module})
