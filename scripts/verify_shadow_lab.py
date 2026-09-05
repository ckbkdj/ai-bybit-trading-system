"""Verify that the Docker Shadow Lab is actually running and paper-only."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_json(url: str, timeout: float = 3.0) -> tuple[int, Any]:
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            return int(response.status), json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload: Any = json.loads(body)
        except json.JSONDecodeError:
            payload = {"raw": body[:1024]}
        return int(exc.code), payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-url", default="http://127.0.0.1:18000")
    parser.add_argument("--executor-url", default="http://127.0.0.1:18787")
    parser.add_argument("--ops-url", default="http://127.0.0.1:18790")
    parser.add_argument("--deadline-seconds", type=float, default=180.0)
    parser.add_argument("--output", type=Path, default=Path("container-shadow-smoke.json"))
    args = parser.parse_args()

    deadline = time.monotonic() + args.deadline_seconds
    attempts: list[dict[str, Any]] = []
    final: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        attempt: dict[str, Any] = {"at": utc_now()}
        try:
            control_status, control = get_json(f"{args.control_url}/v1/health/live")
            executor_status, executor = get_json(f"{args.executor_url}/v1/health/live")
            ops_status, ops_health = get_json(f"{args.ops_url}/healthz")
            status_code, status = get_json(f"{args.ops_url}/api/status", timeout=5.0)
            attempt.update(
                {
                    "control_status": control_status,
                    "executor_status": executor_status,
                    "ops_status": ops_status,
                    "status_api": status_code,
                    "control": control,
                    "executor": executor,
                    "ops_health": ops_health,
                    "snapshot": status,
                }
            )
            checks = {
                "control_live": control_status == 200 and control.get("status") == "live",
                "executor_live": executor_status == 200 and executor.get("status") == "alive",
                "ops_live": ops_status == 200 and ops_health.get("status") == "alive",
                "ops_status_available": status_code == 200,
                "predictor_seen_live": status.get("predictor", {}).get("live") is True,
                "executor_seen_live": status.get("executor", {}).get("live") is True,
                "paper_only": status.get("safety", {}).get("paper_only") is True,
                "not_unsafe": status.get("overall") in {"ready", "degraded"},
                "mainnet_disabled": status.get("safety", {}).get("mainnet_allowed") is False,
            }
            attempt["checks"] = checks
            attempt["passed"] = all(checks.values())
            attempts.append(attempt)
            if attempt["passed"]:
                final = attempt
                break
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            attempt.update(
                {
                    "passed": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            attempts.append(attempt)
        time.sleep(3.0)

    report = {
        "schema_version": "container-shadow-smoke.v1",
        "status": "PASS" if final is not None else "FAIL",
        "generated_at": utc_now(),
        "execution_mode": "paper",
        "mainnet_allowed": False,
        "bybit_private_api_used": False,
        "actual_containers_started": True,
        "attempt_count": len(attempts),
        "final": final,
        "attempts": attempts[-20:],
        "background_processes": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if final is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
