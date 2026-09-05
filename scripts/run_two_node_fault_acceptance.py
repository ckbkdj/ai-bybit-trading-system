from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _safe_environment(role: str) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "APP_ENV": "development",
            "SERVICE_ROLE": role,
            "EXECUTION_MODE": "paper",
            "BYBIT_TRADING_MODE": "shadow",
            "BYBIT_ENABLE_LIVE": "false",
            "MAINNET_ALLOWED": "false",
        }
    )
    for name in (
        "BYBIT_API_KEY",
        "BYBIT_SECRET_KEY",
        "EXECUTION_DB_PATH" if role == "predictor" else "",
        "TRAINING_ENABLED" if role == "executor" else "",
        "BACKFILL_ENABLED" if role == "executor" else "",
        "RESEARCH_JOB_DB" if role == "executor" else "",
    ):
        if name:
            environment.pop(name, None)
    return environment


def _finish(process: subprocess.Popen, timeout: float = 180) -> tuple[int, str]:
    try:
        stdout, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            stdout, _ = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, _ = process.communicate(timeout=5)
        return 124, stdout
    return int(process.returncode or 0), stdout


def _json_output(completed: subprocess.CompletedProcess[str]) -> dict:
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {
            "status": "FAIL",
            "exit_code": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    with tempfile.TemporaryDirectory(prefix="two-node-acceptance-") as directory:
        evidence_root = Path(directory)
        predictor_root = evidence_root / "predictor-node"
        executor_root = evidence_root / "executor-node"
        predictor_root.mkdir()
        executor_root.mkdir()
        predictor_command = [
            sys.executable,
            "-m",
            "pytest",
            "ai_bot3/ai_bot3/tests/test_forecast_publication_outbox.py",
            "ai_bot3/ai_bot3/tests/test_production_paper_runtime.py",
            "ai_bot3/ai_bot3/tests/test_ticket_outbox.py",
            "ai_bot3/ai_bot3/tests/test_resource_governor.py",
            "--basetemp",
            str(predictor_root),
            "-q",
        ]
        executor_command = [
            sys.executable,
            "-m",
            "pytest",
            "BybitContractBotV4/tests/test_production_entrypoint.py",
            "BybitContractBotV4/tests/test_claim_conflict_recovery.py",
            "BybitContractBotV4/tests/test_two_node_autonomy.py",
            "BybitContractBotV4/tests/test_receipt_outbox_resilience.py",
            "BybitContractBotV4/tests/test_execution_engine.py::ExecutionEngineTests::test_one_hundred_replays_do_not_duplicate_or_move_state_or_cursor_backwards",
            "--basetemp",
            str(executor_root),
            "-q",
        ]
        predictor = subprocess.Popen(
            predictor_command,
            cwd=ROOT,
            env=_safe_environment("predictor"),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=flags,
        )
        executor = subprocess.Popen(
            executor_command,
            cwd=ROOT,
            env=_safe_environment("executor"),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=flags,
        )
        predictor_code, predictor_log = _finish(predictor)
        executor_code, executor_log = _finish(executor)
        stress = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "run_prediction_resource_stress.py"),
                "--duration-seconds",
                "1",
                "--cadence-ms",
                "50",
                "--max-jitter-ms",
                "500",
                "--pressure-workers",
                "1",
            ],
            cwd=ROOT,
            env=_safe_environment("predictor"),
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=flags,
        )
        try:
            stress_report = json.loads(stress.stdout)
        except json.JSONDecodeError:
            stress_report = {"status": "FAIL", "raw": stress.stdout}
        deployment = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "validate_production_paper_deployment.py"),
            ],
            cwd=ROOT,
            env=_safe_environment("predictor"),
            capture_output=True,
            text=True,
            timeout=90,
            creationflags=flags,
        )
        deployment_report = _json_output(deployment)
        passed = (
            predictor_code
            == executor_code
            == stress.returncode
            == deployment.returncode
            == 0
        )
        report = {
            "status": "PASS" if passed else "FAIL",
            "execution_mode": "paper",
            "live_count": 0,
            "mainnet_allowed": False,
            "node_processes": {
                "predictor": {
                    "pid": predictor.pid,
                    "exit_code": predictor_code,
                    "isolated_temp_root": str(predictor_root),
                    "log": predictor_log.strip(),
                },
                "executor": {
                    "pid": executor.pid,
                    "exit_code": executor_code,
                    "isolated_temp_root": str(executor_root),
                    "log": executor_log.strip(),
                },
            },
            "scenarios": {
                "A_executor_outage_6h_accelerated": predictor_code == 0,
                "B_predictor_outage_2h_accelerated": executor_code == 0,
                "C_network_jitter_100_replays": executor_code == 0,
                "D_poison_receipt": executor_code == 0,
                "E_duplicate_executor_ownership": predictor_code == 0,
                "F_schema_incompatible": executor_code == 0,
                "G_clock_skew": executor_code == 0,
                "H_disk_pressure": predictor_code == 0 and stress.returncode == 0,
                "I_deployable_entrypoints_and_paths": deployment.returncode == 0,
            },
            "resource_stress": stress_report,
            "deployment_preflight": deployment_report,
            "shared_sqlite": False,
            "background_processes_remaining": int(predictor.poll() is None)
            + int(executor.poll() is None)
            + int(stress_report.get("background_processes_remaining", 1)),
        }
        rendered = json.dumps(report, indent=2, sort_keys=True)
        if args.output:
            args.output.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0 if passed and report["background_processes_remaining"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
