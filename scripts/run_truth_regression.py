from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shadow_contracts.repository import resolve_code_commit  # noqa: E402


NETWORK_GUARD = ROOT / "scripts" / "truth_network_guard"
SECRET_ENV_KEYS = (
    "BYBIT_API_KEY",
    "BYBIT_SECRET_KEY",
    "CONTROL_PLANE_API_TOKEN",
    "TICKET_API_TOKEN",
    "PREDICTION_API_TOKEN",
)
LIVE_ENV_KEYS = (
    "BYBIT_ENABLE_LIVE",
    "LIVE_TRADING",
    "ENABLE_LIVE_TRADING",
    "MAINNET_ENABLED",
)


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "live"}


def _safe_tail(text: str, limit: int = 12_000) -> str:
    if len(text) <= limit:
        return text
    return "...<truncated>...\n" + text[-limit:]


def _run(name: str, command: list[str], *, cwd: Path = ROOT, env: dict[str, str]) -> dict[str, Any]:
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return {
        "name": name,
        "status": "PASS" if completed.returncode == 0 else "FAIL",
        "return_code": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
        "command": command,
        "cwd": str(cwd.relative_to(ROOT) if cwd != ROOT else Path(".")),
        "stdout_tail": _safe_tail(completed.stdout),
        "stderr_tail": _safe_tail(completed.stderr),
    }


def _git_commit() -> str:
    return resolve_code_commit(ROOT)


def _environment_gate() -> dict[str, Any]:
    present_secrets = [key for key in SECRET_ENV_KEYS if str(os.environ.get(key) or "").strip()]
    live_flags = [key for key in LIVE_ENV_KEYS if _truthy(os.environ.get(key))]
    mode = str(os.environ.get("BYBIT_TRADING_MODE") or "shadow").strip().lower()
    violations = []
    if present_secrets:
        violations.append("real/private credentials are present in the regression environment")
    if live_flags or mode == "live":
        violations.append("a live/mainnet flag is enabled")
    return {
        "status": "PASS" if not violations else "FAIL",
        "bybit_mode": mode,
        "present_secret_variable_names": present_secrets,
        "enabled_live_flags": live_flags,
        "violations": violations,
    }


def _audit_evidence(
    env: dict[str, str],
    vulnerability_report: Path | None = None,
) -> dict[str, Any]:
    output = ROOT / "truth-supply-chain-audit.json"
    command = [sys.executable, "scripts/supply_chain_audit.py", "--output", str(output)]
    if vulnerability_report is not None:
        command.extend(["--vulnerability-report", str(vulnerability_report.resolve())])
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    payload: dict[str, Any] = {}
    parse_error = None
    try:
        payload = json.loads(output.read_text(encoding="utf-8"))
    except Exception as exc:
        parse_error = f"{type(exc).__name__}: {exc}"
    finally:
        output.unlink(missing_ok=True)

    tracked = payload.get("tracked_secret_scan") or {}
    history = payload.get("git_history_secret_scan") or {}
    node = payload.get("node_lock_integrity") or {}
    credential_safe = bool(payload) and not (
        tracked.get("finding_count")
        or tracked.get("tracked_sensitive_paths")
        or history.get("finding_count")
        or history.get("historical_sensitive_paths")
        or history.get("scan_limit_reached")
        or node.get("missing_integrity_entries")
    )
    return {
        "name": "reachable-history credential and lock-integrity truth",
        "status": "PASS" if credential_safe and parse_error is None else "FAIL",
        "release_gate_status": payload.get("status", "UNKNOWN"),
        "return_code": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
        "parse_error": parse_error,
        "tracked_secret_finding_count": tracked.get("finding_count"),
        "historical_secret_finding_count": history.get("finding_count"),
        "historical_blob_count": history.get("scanned_blob_count"),
        "history_scan_limit_reached": history.get("scan_limit_reached"),
        "node_missing_integrity_count": len(node.get("missing_integrity_entries") or []),
        "release_blockers": payload.get("blockers") or [],
        "stdout_tail": _safe_tail(completed.stdout),
        "stderr_tail": _safe_tail(completed.stderr),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run shadow-only authenticity regression and emit machine evidence"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "truth-regression.json")
    parser.add_argument(
        "--vulnerability-report",
        type=Path,
        help="pip-audit JSON covering the exact deployment lock",
    )
    args = parser.parse_args()

    environment_gate = _environment_gate()
    safe_env = dict(os.environ)
    safe_env.update(
        {
            "BYBIT_TRADING_MODE": "shadow",
            "BYBIT_ENABLE_LIVE": "false",
            "LIVE_TRADING": "false",
            "ENABLE_LIVE_TRADING": "false",
            "MAINNET_ENABLED": "false",
            "AI_BOT_TICKETS_ENABLED": "true",
            "AI_BOT_REQUIRED_BRAIN_RELEASE_STAGE": "live",
            "TRUTH_REGRESSION_BLOCK_EXTERNAL_NETWORK": "1",
        }
    )
    for key in SECRET_ENV_KEYS:
        safe_env.pop(key, None)
    existing_pythonpath = safe_env.get("PYTHONPATH", "")
    safe_env["PYTHONPATH"] = str(NETWORK_GUARD) + (
        os.pathsep + existing_pythonpath if existing_pythonpath else ""
    )

    checks: list[dict[str, Any]] = []
    if environment_gate["status"] == "PASS":
        checks.append(
            _run(
                "loopback-only network guard",
                [
                    sys.executable,
                    "-c",
                    (
                        "import socket,sys; "
                        "\ntry: socket.create_connection(('1.1.1.1',443), timeout=0.2)"
                        "\nexcept OSError as exc:"
                        "\n    print(type(exc).__name__ + ': ' + str(exc))"
                        "\n    sys.exit(0 if 'truth regression blocked' in str(exc) else 2)"
                        "\nraise SystemExit('external network guard did not block connection')"
                    ),
                ],
                env=safe_env,
            )
        )
        checks.append(
            _run(
                "exchange journal, async acknowledgement, duplicate event and recovery truth",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "BybitContractBotV4/tests/test_async_cancel_confirmation.py",
                    "BybitContractBotV4/tests/test_timeout_cancel_confirmation.py",
                    "BybitContractBotV4/tests/test_child_cancel_semantics.py",
                    "BybitContractBotV4/tests/test_durable_kill_switch.py",
                    "BybitContractBotV4/tests/test_position_mode_gate.py",
                    "BybitContractBotV4/tests/test_replace_fail_closed.py",
                    "BybitContractBotV4/tests/test_execution_engine.py::ExecutionEngineTests::test_rest_success_does_not_mark_filled_and_fill_events_are_idempotent",
                    "BybitContractBotV4/tests/test_execution_engine.py::ExecutionEngineTests::test_cancel_fill_race_promotes_actual_fill",
                    "BybitContractBotV4/tests/test_execution_engine.py::ExecutionEngineTests::test_restart_recovery_uses_order_link_id_without_resubmit",
                    "BybitContractBotV4/tests/test_execution_engine.py::ExecutionEngineTests::test_expired_worker_fence_cannot_reserve_exchange_order",
                    "BybitContractBotV4/tests/test_execution_engine.py::ExecutionEngineTests::test_execution_receipt_is_immutable_and_queued_once",
                ],
                env=safe_env,
            )
        )
        checks.append(
            _run(
                "forecast horizon, event veto, ticket lifetime and anti-leakage truth",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "tests/test_mode_horizon_truth.py",
                    "tests/test_portfolio_release.py",
                    "tests/test_kline_only_training_anti_leakage.py",
                ],
                cwd=ROOT / "ai_bot3" / "ai_bot3",
                env=safe_env,
            )
        )
        checks.append(
            _run(
                "cross-process release-gated HTTP ticket-order-receipt shadow truth",
                [sys.executable, "scripts/run_shadow_e2e.py"],
                env=safe_env,
            )
        )
        checks.append(_audit_evidence(safe_env, args.vulnerability_report))

    external_gates = [
        {
            "status": "GATED",
            "scenario": "real Bybit testnet partial fills and duplicate private execution delivery",
            "reason": "requires a dedicated testnet account and exchange-generated execId/order events",
        },
        {
            "status": "GATED",
            "scenario": "real cancel/fill race, REST timeout and private WebSocket reconnect",
            "reason": "a deterministic simulator proves invariants but cannot prove exchange timing/distribution",
        },
        {
            "status": "GATED",
            "scenario": "real instrument precision, rate-limit headers and maintenance/network partition",
            "reason": "must be observed against current Bybit testnet behavior",
        },
        {
            "status": "GATED",
            "scenario": "30-day continuous shadow soak and strategy profitability",
            "reason": "cannot be manufactured by a short regression run",
        },
    ]

    required_pass = environment_gate["status"] == "PASS" and all(
        item.get("status") == "PASS" for item in checks
    )
    report = {
        "schema_version": "truth-regression.v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "repository_commit": _git_commit(),
        "overall_status": "PASS" if required_pass else "FAIL",
        "release_conclusion": "SHADOW_ONLY",
        "environment_gate": environment_gate,
        "child_process_network_policy": {
            "external_tcp": "BLOCKED",
            "allowed": ["127.0.0.0/8", "::1", "local/unix sockets"],
            "guard": str(NETWORK_GUARD.relative_to(ROOT) / "sitecustomize.py"),
        },
        "checks": checks,
        "external_testnet_and_time_gates": external_gates,
        "interpretation": (
            "PASS proves reproducible software invariants in a clean, loopback-only shadow environment. "
            "It does not prove real-exchange behavior, continuous reliability, or profitability."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if required_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
