"""Shared fail-closed runtime identity and service-role boundaries."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum
from types import ModuleType
from typing import Mapping


class RuntimeConfigurationError(RuntimeError):
    """Raised when a process could start with an ambiguous or unsafe role."""


class AppEnvironment(str, Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class ServiceRole(str, Enum):
    PREDICTOR = "predictor"
    EXECUTOR = "executor"
    RESEARCH = "research"


class ExecutionMode(str, Enum):
    PAPER = "paper"
    TESTNET = "testnet"
    LIVE = "live"


LEGACY_EXECUTION_MODES = {
    "shadow": ExecutionMode.PAPER,
    "testnet": ExecutionMode.TESTNET,
    "live": ExecutionMode.LIVE,
}

PREDICTOR_FORBIDDEN_SETTINGS = (
    "BYBIT_API_KEY",
    "BYBIT_SECRET_KEY",
    "EXECUTION_DB_PATH",
)

EXECUTOR_FORBIDDEN_SETTINGS = (
    "AI_BOT_FEATURE_STORE_PATH",
    "AI_BOT_KLINE_FEATURE_STORE_PATH",
    "BYBIT_PUBLIC_PIT_STORE",
    "LOCKBOX_BYBIT_PIT_STORE",
    "MACRO_PIT_STORE",
    "FLOW_PIT_STORE",
    "AI_BOT_MODEL_WRITE_DIR",
    "MODEL_OUTPUT_DIR",
    "RESEARCH_JOB_DB",
    "TRAINING_ENABLED",
    "BACKFILL_ENABLED",
)


def _enum_value(enum_type, raw: str, setting: str):
    try:
        return enum_type(raw.strip().lower())
    except ValueError as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise RuntimeConfigurationError(f"{setting} must be one of: {allowed}") from exc


@dataclass(frozen=True)
class RuntimeIdentity:
    app_environment: AppEnvironment
    service_role: ServiceRole
    execution_mode: ExecutionMode
    host_id: str
    cluster_id: str
    deployment_id: str

    @classmethod
    def load(
        cls,
        environ: Mapping[str, str],
        *,
        expected_role: ServiceRole | str | None = None,
    ) -> "RuntimeIdentity":
        values = {str(key): str(value).strip() for key, value in environ.items()}
        environment = _enum_value(
            AppEnvironment,
            values.get("APP_ENV", AppEnvironment.DEVELOPMENT.value),
            "APP_ENV",
        )
        expected = ServiceRole(expected_role) if expected_role is not None else None
        default_role = expected.value if expected is not None else ServiceRole.RESEARCH.value
        role = _enum_value(
            ServiceRole,
            values.get("SERVICE_ROLE", default_role),
            "SERVICE_ROLE",
        )
        if expected is not None and role is not expected:
            raise RuntimeConfigurationError(
                f"SERVICE_ROLE={role.value} cannot start the {expected.value} service"
            )

        modern_raw = values.get("EXECUTION_MODE", "").lower()
        legacy_raw = values.get("BYBIT_TRADING_MODE", "").lower()
        modern = (
            _enum_value(ExecutionMode, modern_raw, "EXECUTION_MODE")
            if modern_raw
            else None
        )
        if legacy_raw and legacy_raw not in LEGACY_EXECUTION_MODES:
            raise RuntimeConfigurationError(
                "BYBIT_TRADING_MODE must be one of: shadow, testnet, live"
            )
        legacy = LEGACY_EXECUTION_MODES.get(legacy_raw) if legacy_raw else None
        if modern is not None and legacy is not None and modern is not legacy:
            raise RuntimeConfigurationError(
                "EXECUTION_MODE and BYBIT_TRADING_MODE describe different modes"
            )
        execution_mode = modern or legacy or ExecutionMode.PAPER

        host_id = values.get("HOST_ID", "")
        cluster_id = values.get("CLUSTER_ID", "")
        deployment_id = values.get("DEPLOYMENT_ID", "")
        if environment is AppEnvironment.PRODUCTION:
            missing = [
                name
                for name, value in (
                    ("HOST_ID", host_id),
                    ("CLUSTER_ID", cluster_id),
                    ("DEPLOYMENT_ID", deployment_id),
                )
                if not value
            ]
            if missing:
                raise RuntimeConfigurationError(
                    "production runtime identity is incomplete: " + ", ".join(missing)
                )

        forbidden = ()
        if role is ServiceRole.PREDICTOR:
            forbidden = PREDICTOR_FORBIDDEN_SETTINGS
        elif role is ServiceRole.EXECUTOR:
            forbidden = EXECUTOR_FORBIDDEN_SETTINGS
        populated = [name for name in forbidden if values.get(name)]
        if populated:
            raise RuntimeConfigurationError(
                f"SERVICE_ROLE={role.value} forbids settings: {', '.join(populated)}"
            )

        return cls(
            app_environment=environment,
            service_role=role,
            execution_mode=execution_mode,
            host_id=host_id,
            cluster_id=cluster_id,
            deployment_id=deployment_id,
        )


def assert_loaded_module_boundary(
    identity: RuntimeIdentity,
    modules: Mapping[str, ModuleType] | None = None,
) -> None:
    """Reject cross-role implementation imports before a service becomes ready."""

    loaded = modules or sys.modules
    violations: list[str] = []
    for name, module in loaded.items():
        path = str(getattr(module, "__file__", "") or "").replace("\\", "/").lower()
        if identity.service_role is ServiceRole.PREDICTOR:
            if "/bybitcontractbotv4/" in path:
                violations.append(name)
        elif identity.service_role is ServiceRole.EXECUTOR:
            if "/ai_bot3/ai_bot3/" in path:
                violations.append(name)
    if violations:
        sample = ", ".join(sorted(set(violations))[:8])
        raise RuntimeConfigurationError(
            f"{identity.service_role.value} process imported forbidden role modules: {sample}"
        )
