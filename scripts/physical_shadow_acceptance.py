"""Verify a real two-host Shadow/Paper deployment over its network boundary.

This command is read-only. It samples the predictor control plane and executor
health APIs, verifies the shared deployment identity, checks clock drift, and
writes machine-readable acceptance evidence. It never creates a ticket or order.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def code_commit(root: Path) -> str:
    configured = os.environ.get("APP_CODE_COMMIT", "").strip()
    if configured:
        return configured
    try:
        return subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def request_json(
    session: requests.Session,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    cert: tuple[str, str] | str | None = None,
    verify: str | bool = True,
    timeout: float = 5.0,
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        response = session.get(
            url,
            headers=headers or {},
            cert=cert,
            verify=verify,
            timeout=timeout,
        )
        try:
            payload: Any = response.json()
        except ValueError:
            payload = {"raw": response.text[:1024]}
        return {
            "ok": 200 <= response.status_code < 300,
            "status_code": response.status_code,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
            "payload": payload,
            "error": None,
        }
    except requests.RequestException as exc:
        return {
            "ok": False,
            "status_code": 0,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
            "payload": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def payload(result: dict[str, Any]) -> dict[str, Any]:
    value = result.get("payload")
    return value if isinstance(value, dict) else {}


def build_control_auth(args: argparse.Namespace) -> tuple[dict[str, str], Any, Any]:
    token = args.control_token or os.environ.get(args.control_token_env, "").strip()
    if not token:
        raise RuntimeError(
            f"control-plane token is required via --control-token or {args.control_token_env}"
        )
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Executor-Consumer-ID": args.consumer_id,
        "X-Client-Certificate-Identity": args.certificate_identity,
        "Accept": "application/json",
    }
    cert: Any = None
    if args.client_cert:
        cert = (args.client_cert, args.client_key) if args.client_key else args.client_cert
    verify: Any = args.ca_bundle or (not args.insecure)
    return headers, cert, verify


def sample_once(
    args: argparse.Namespace,
    control_session: requests.Session,
    executor_session: requests.Session,
    headers: dict[str, str],
    cert: Any,
    verify: Any,
) -> dict[str, Any]:
    control_base = args.control_plane_url.rstrip("/")
    executor_base = args.executor_url.rstrip("/")
    control = {
        name: request_json(
            control_session,
            f"{control_base}{path}",
            headers=headers,
            cert=cert,
            verify=verify,
            timeout=args.timeout_seconds,
        )
        for name, path in {
            "live": "/v1/health/live",
            "ready": "/v1/health/ready",
            "dependencies": "/v1/health/dependencies",
            "capabilities": "/v1/capabilities",
            "time": "/v1/time",
        }.items()
    }
    executor = {
        name: request_json(
            executor_session,
            f"{executor_base}{path}",
            verify=not args.executor_insecure,
            timeout=args.timeout_seconds,
        )
        for name, path in {
            "live": "/v1/health/live",
            "ready": "/v1/health/ready",
            "dependencies": "/v1/health/dependencies",
            "health": "/health",
        }.items()
    }

    ready_control = control["ready"]["ok"] and payload(control["ready"]).get("status") == "ready"
    ready_executor = executor["ready"]["ok"] and payload(executor["ready"]).get("status") == "ready"
    capabilities = payload(control["capabilities"])
    control_ready = payload(control["ready"])
    executor_health = payload(executor["health"])
    server_unix = payload(control["time"]).get("unix_time")
    clock_skew = abs(float(server_unix) - time.time()) if server_unix is not None else None

    checks = {
        "control_live": bool(control["live"]["ok"]),
        "control_ready": bool(ready_control),
        "executor_live": bool(executor["live"]["ok"]),
        "executor_ready": bool(ready_executor),
        "paper_control": control_ready.get("execution_mode") == "paper",
        "paper_executor": executor_health.get("execution_mode") == "paper",
        "shadow_executor": executor_health.get("mode") == "shadow",
        "kill_switch_clear": executor_health.get("kill_switch") is False,
        "incident_normal": executor_health.get("incident_mode") == "NORMAL",
        "dead_letter_clear": int(executor_health.get("dead_letter_count") or 0) == 0,
        "clock_skew_ok": clock_skew is not None and clock_skew <= args.max_clock_skew_seconds,
        "cluster_match": (
            not args.expected_cluster_id
            or capabilities.get("cluster_id") == args.expected_cluster_id
        ),
        "deployment_match": (
            not args.expected_deployment_id
            or capabilities.get("deployment_id") == args.expected_deployment_id
        ),
    }
    if args.require_market_data:
        checks["market_data_ready"] = executor_health.get("market_data") == "ready"
    if args.require_zero_incomplete:
        checks["no_incomplete_tickets"] = int(
            executor_health.get("incomplete_ticket_count") or 0
        ) == 0
    return {
        "sampled_at": utc_now(),
        "passed": all(checks.values()),
        "checks": checks,
        "clock_skew_seconds": clock_skew,
        "control": control,
        "executor": executor,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only physical two-node Shadow acceptance"
    )
    parser.add_argument("--control-plane-url", required=True)
    parser.add_argument("--executor-url", required=True)
    parser.add_argument("--control-token", default="")
    parser.add_argument("--control-token-env", default="CONTROL_PLANE_API_TOKEN")
    parser.add_argument("--consumer-id", default="executor-paper-01")
    parser.add_argument("--certificate-identity", default="executor-paper-01")
    parser.add_argument("--client-cert", default="")
    parser.add_argument("--client-key", default="")
    parser.add_argument("--ca-bundle", default="")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--executor-insecure", action="store_true")
    parser.add_argument("--expected-cluster-id", default="")
    parser.add_argument("--expected-deployment-id", default="")
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--max-clock-skew-seconds", type=float, default=5.0)
    parser.add_argument("--min-healthy-ratio", type=float, default=1.0)
    parser.add_argument("--require-market-data", action="store_true", default=True)
    parser.add_argument("--no-require-market-data", dest="require_market_data", action="store_false")
    parser.add_argument("--require-zero-incomplete", action="store_true", default=True)
    parser.add_argument("--allow-incomplete", dest="require_zero_incomplete", action="store_false")
    parser.add_argument("--output", type=Path, default=Path("physical-shadow-acceptance.json"))
    args = parser.parse_args()
    if not 0 < args.min_healthy_ratio <= 1:
        raise SystemExit("--min-healthy-ratio must be in (0, 1]")
    if args.insecure and os.environ.get("APP_ENV", "").lower() == "production":
        raise SystemExit("--insecure is forbidden when APP_ENV=production")

    headers, cert, verify = build_control_auth(args)
    control_session = requests.Session()
    executor_session = requests.Session()
    samples: list[dict[str, Any]] = []
    deadline = time.monotonic() + max(0.0, args.duration_seconds)
    while True:
        samples.append(
            sample_once(args, control_session, executor_session, headers, cert, verify)
        )
        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.25, args.interval_seconds))

    passed_count = sum(1 for item in samples if item["passed"])
    healthy_ratio = passed_count / len(samples)
    status = "PASS" if healthy_ratio >= args.min_healthy_ratio else "FAIL"
    report = {
        "schema_version": "physical-shadow-acceptance.v1",
        "status": status,
        "generated_at": utc_now(),
        "code_commit": code_commit(Path(__file__).resolve().parents[1]),
        "execution_mode": "paper",
        "mainnet_allowed": False,
        "bybit_private_api_used": False,
        "control_plane_url": args.control_plane_url,
        "executor_url": args.executor_url,
        "sample_count": len(samples),
        "passed_sample_count": passed_count,
        "healthy_ratio": healthy_ratio,
        "minimum_healthy_ratio": args.min_healthy_ratio,
        "samples": samples,
        "background_processes": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
