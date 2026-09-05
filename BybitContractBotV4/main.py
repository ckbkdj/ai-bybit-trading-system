"""Deployable entrypoint for the versioned Bybit execution service.

The ``--preflight`` path validates the production-paper configuration without
constructing an exchange client or opening any network connection. Normal
startup delegates to the existing active service implementation.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parent
REPOSITORY_ROOT = SERVICE_ROOT.parent
for candidate in (REPOSITORY_ROOT, SERVICE_ROOT):
    text = str(candidate)
    if text not in sys.path:
        sys.path.insert(0, text)

from runtime_config import TradingMode, TradingSettings


def run_preflight(settings: TradingSettings | None = None) -> dict[str, object]:
    runtime = settings or TradingSettings.load()
    private_api_enabled = runtime.mode in {TradingMode.TESTNET, TradingMode.LIVE}
    mainnet_enabled = runtime.mode is TradingMode.LIVE and runtime.enable_live
    payload: dict[str, object] = {
        "status": "PASS",
        "deployment_environment": runtime.app_environment.value,
        "service_role": runtime.service_role.value,
        "execution_mode": runtime.execution_mode.value,
        "legacy_trading_mode": runtime.mode.value,
        "host_id": runtime.host_id,
        "cluster_id": runtime.cluster_id,
        "deployment_id": runtime.deployment_id,
        "ticket_consumer_id": runtime.ticket_consumer_id,
        "control_plane_https": runtime.ticket_api_base_url.lower().startswith("https://"),
        "private_trading_api_enabled": private_api_enabled,
        "mainnet_order_submission_enabled": mainnet_enabled,
        "real_capital_at_risk": mainnet_enabled,
    }
    if runtime.execution_mode.value == "paper" and private_api_enabled:
        raise RuntimeError("paper execution unexpectedly enabled a private exchange API")
    if runtime.execution_mode.value == "paper" and mainnet_enabled:
        raise RuntimeError("paper execution unexpectedly enabled mainnet submission")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AI-Bybit execution service")
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="validate configuration without connecting to Bybit or the control plane",
    )
    args = parser.parse_args(argv)
    if args.preflight:
        print(json.dumps(run_preflight(), ensure_ascii=False, sort_keys=True))
        return 0

    # Import only after configuration parsing so that preflight remains a strict
    # no-network/no-exchange operation.
    from service_main import run_service

    run_service()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
