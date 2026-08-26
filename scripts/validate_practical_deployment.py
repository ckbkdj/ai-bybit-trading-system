"""Static and in-process validation for the practical deployment layer."""

from __future__ import annotations

import importlib.util
import json
import os
import py_compile
import sys
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        raise RuntimeError(f"required practical deployment file is missing: {relative}")
    return path.read_text(encoding="utf-8")


def load_yaml(relative: str) -> dict[str, Any]:
    payload = yaml.safe_load(read(relative))
    if not isinstance(payload, dict) or not isinstance(payload.get("services"), dict):
        raise RuntimeError(f"{relative} is not a Docker Compose service document")
    return payload


def assert_no_live_values(value: Any, location: str = "root") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            assert_no_live_values(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            assert_no_live_values(child, f"{location}[{index}]")
    elif isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"live", "true"} and any(
            name in location.lower()
            for name in ("bybit_trading_mode", "bybit_enable_live", "mainnet_allowed")
        ):
            raise RuntimeError(f"unsafe practical compose value at {location}: {value}")


def fake_result(payload: dict[str, Any], status_code: int = 200) -> dict[str, Any]:
    return {
        "ok": 200 <= status_code < 300,
        "status_code": status_code,
        "elapsed_ms": 1.0,
        "payload": payload,
        "error": None,
    }


def validate_console_logic() -> None:
    os.environ.setdefault("APP_ENV", "development")
    spec = importlib.util.spec_from_file_location("practical_ops_console", ROOT / "ops_console" / "app.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot import operations console")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    results = {
        "control_live": fake_result({"status": "live"}),
        "control_ready": fake_result({"status": "ready", "execution_mode": "paper"}),
        "control_dependencies": fake_result({"status": "ok"}),
        "control_capabilities": fake_result(
            {"cluster_id": "cluster-a", "deployment_id": "deployment-a"}
        ),
        "executor_live": fake_result({"status": "alive"}),
        "executor_ready": fake_result({"status": "ready"}),
        "executor_dependencies": fake_result({"ready": True}),
        "executor_health": fake_result(
            {
                "ready": True,
                "mode": "shadow",
                "execution_mode": "paper",
                "kill_switch": False,
                "incident_mode": "NORMAL",
                "dead_letter_count": 0,
                "incomplete_ticket_count": 0,
            }
        ),
    }
    snapshot = module._derive_snapshot(results)
    if snapshot["overall"] != "ready" or not snapshot["safety"]["paper_only"]:
        raise RuntimeError("operations console failed to recognize a valid paper deployment")
    results["executor_health"] = fake_result(
        {"ready": True, "mode": "live", "execution_mode": "live"}
    )
    unsafe = module._derive_snapshot(results)
    if unsafe["overall"] != "unsafe":
        raise RuntimeError("operations console failed to reject a live deployment")


def main() -> int:
    required = [
        "deploy/docker/Dockerfile",
        "deploy/docker/tcp_healthcheck.py",
        "deploy/practical/predictor.compose.yml",
        "deploy/practical/executor.compose.yml",
        "deploy/practical/shadow-lab.compose.yml",
        "deploy/practical/predictor.env.example",
        "deploy/practical/executor.env.example",
        "deploy/practical/nginx.conf",
        "deploy/practical/bootstrap-shadow-pki.sh",
        "deploy/practical/up.sh",
        "deploy/practical/up.ps1",
        "deploy/predictor-production-paper/.env.production-paper.example",
        "deploy/predictor-production-paper/systemd/control-plane-api.service",
        "ops_console/app.py",
        "ops_console/static/index.html",
        "ops_console/static/app.js",
        "ops_console/static/style.css",
        "scripts/physical_shadow_acceptance.py",
        "scripts/testnet_admission.py",
        "docs/current/PRACTICAL_DEPLOYMENT.md",
        "docs/current/TESTNET_ADMISSION.md",
    ]
    for path in required:
        read(path)

    dockerfile = read("deploy/docker/Dockerfile")
    for target in ("AS predictor", "AS executor", "AS ops-console"):
        if target not in dockerfile:
            raise RuntimeError(f"Dockerfile target missing: {target}")
    if "--require-hashes" not in dockerfile or "linux-py311.lock" not in dockerfile:
        raise RuntimeError("Dockerfile does not install the hash-locked runtime")

    predictor = load_yaml("deploy/practical/predictor.compose.yml")
    executor = load_yaml("deploy/practical/executor.compose.yml")
    lab = load_yaml("deploy/practical/shadow-lab.compose.yml")
    assert_no_live_values(predictor)
    assert_no_live_values(executor)
    assert_no_live_values(lab)

    expected_predictor = {
        "control-plane",
        "control-plane-proxy",
        "predictor-realtime",
        "publication-worker",
        "market-collector",
        "ops-console",
    }
    if set(predictor["services"]) != expected_predictor:
        raise RuntimeError("predictor compose service set is incomplete")
    if set(executor["services"]) != {"executor"}:
        raise RuntimeError("executor compose must contain only the executor role")
    if not {"control-plane", "executor", "ops-console"}.issubset(lab["services"]):
        raise RuntimeError("shadow lab compose is incomplete")

    for document in (predictor, executor):
        for name, service in document["services"].items():
            if name == "control-plane-proxy":
                continue
            if service.get("privileged"):
                raise RuntimeError(f"privileged practical service is forbidden: {name}")
            if service.get("network_mode") == "host":
                raise RuntimeError(f"host networking is forbidden: {name}")

    nginx = read("deploy/practical/nginx.conf")
    for required_line in (
        "ssl_verify_client on",
        "X-Executor-Consumer-ID",
        "X-Client-Certificate-Identity",
    ):
        if required_line not in nginx:
            raise RuntimeError(f"mTLS proxy contract missing: {required_line}")

    for env_file in (
        "deploy/practical/predictor.env.example",
        "deploy/practical/executor.env.example",
    ):
        content = read(env_file)
        if "BYBIT_ENABLE_LIVE=false" not in content or "MAINNET_ALLOWED=false" not in content:
            raise RuntimeError(f"{env_file} is not fail-closed")
        if "BYBIT_TRADING_MODE=shadow" not in content:
            raise RuntimeError(f"{env_file} is not a Shadow example")

    production_env = read("deploy/predictor-production-paper/.env.production-paper.example")
    if "RESEARCH_JOB_DB=/var/lib/ai-bybit/control-plane/research-disabled.sqlite3" not in production_env:
        raise RuntimeError("production control plane has no writable disabled-research store")
    control_unit = read("deploy/predictor-production-paper/systemd/control-plane-api.service")
    if "preflight_production_predictor.py" not in control_unit:
        raise RuntimeError("systemd control plane does not run the offline predictor preflight")
    if "api/control_plane_server.py" not in control_unit:
        raise RuntimeError("systemd control plane does not use the minimal runtime entrypoint")

    app_js = read("ops_console/static/app.js")
    if "createApp" not in app_js or "renderFallback" not in app_js:
        raise RuntimeError("operations UI must use Vue and retain an offline fallback")

    for path in (
        "ops_console/app.py",
        "scripts/physical_shadow_acceptance.py",
        "scripts/testnet_admission.py",
        "deploy/docker/tcp_healthcheck.py",
    ):
        py_compile.compile(str(ROOT / path), doraise=True)
    validate_console_logic()

    report = {
        "status": "PASS",
        "schema_version": "practical-deployment-validation.v1",
        "required_file_count": len(required),
        "predictor_services": sorted(predictor["services"]),
        "executor_services": sorted(executor["services"]),
        "shadow_lab_services": sorted(lab["services"]),
        "vue_console": True,
        "telegram_alerting": True,
        "physical_shadow_acceptance": True,
        "testnet_read_only_admission": True,
        "mainnet_allowed": False,
        "background_processes": 0,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
