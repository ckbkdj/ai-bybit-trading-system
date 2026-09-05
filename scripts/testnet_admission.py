"""Read-only Bybit testnet admission check.

This command validates the executor's fail-closed testnet configuration and makes
read-only testnet API calls. It never creates, amends, cancels, or closes an order.
Actual execution scenarios remain a separate explicitly-authorized acceptance run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXECUTOR_ROOT = ROOT / "BybitContractBotV4"
for candidate in (ROOT, EXECUTOR_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from runtime_config import TradingMode, TradingSettings  # noqa: E402


def truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Bybit testnet admission")
    parser.add_argument("--output", type=Path, default=Path("testnet-admission.json"))
    parser.add_argument("--require-flat-account", action="store_true", default=True)
    parser.add_argument("--allow-existing-state", dest="require_flat_account", action="store_false")
    args = parser.parse_args()

    required_attestations = {
        "explicit_permission": truthy("BYBIT_TESTNET_EXPLICIT_PERMISSION"),
        "no_withdrawal_permission": truthy("BYBIT_TESTNET_NO_WITHDRAWAL_CONFIRMED"),
        "fixed_ip": truthy("BYBIT_TESTNET_FIXED_IP_CONFIRMED"),
    }
    settings = TradingSettings.load(root=EXECUTOR_ROOT)
    if settings.mode is not TradingMode.TESTNET:
        raise RuntimeError("testnet admission requires EXECUTION_MODE=testnet")
    if settings.enable_live:
        raise RuntimeError("testnet admission refuses BYBIT_ENABLE_LIVE=true")

    import ccxt  # imported only after fail-closed configuration validation

    exchange = ccxt.bybit(
        {
            "apiKey": settings.api_key,
            "secret": settings.secret_key,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    exchange.set_sandbox_mode(True)
    started = time.monotonic()
    exchange.load_markets()
    server_time = float(exchange.fetch_time()) / 1000.0
    balance = exchange.fetch_balance()
    positions = exchange.fetch_positions()
    open_orders = exchange.fetch_open_orders()
    active_positions = [
        item
        for item in positions
        if abs(float(item.get("contracts") or (item.get("info") or {}).get("size") or 0)) > 0
    ]
    checks = {
        **required_attestations,
        "testnet_mode": settings.mode is TradingMode.TESTNET,
        "one_way_mode": settings.position_mode == "one_way",
        "dedicated_subaccount": settings.dedicated_subaccount,
        "manual_orders_forbidden": not settings.allow_manual_orders,
        "mainnet_disabled": not settings.enable_live,
        "api_authenticated": bool(balance),
        "clock_skew_ok": abs(server_time - time.time()) <= settings.max_exchange_clock_drift_seconds,
        "flat_account": (not active_positions and not open_orders) if args.require_flat_account else True,
    }
    report = {
        "schema_version": "testnet-admission.v1",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "generated_at": utc_now(),
        "execution_mode": "testnet",
        "mainnet_allowed": False,
        "read_only_api_check": True,
        "orders_created": 0,
        "orders_cancelled": 0,
        "active_position_count": len(active_positions),
        "open_order_count": len(open_orders),
        "clock_skew_seconds": abs(server_time - time.time()),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "checks": checks,
        "next_gate": "explicit testnet execution scenario approval",
        "background_processes": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
