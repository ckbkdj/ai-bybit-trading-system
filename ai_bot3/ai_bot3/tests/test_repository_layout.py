from __future__ import annotations

import hashlib
import json
from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[3]
BASELINE_COMMIT = "f4a424fc06643e7af40478be5fc2c4d935a8491b"


def _env_keys(path: Path) -> set[str]:
    return {
        line.split("=", 1)[0].strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#") and "=" in line
    }


def _locked_versions(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        requirement = value.split(" --hash=", 1)[0]
        name, version = requirement.split("==", 1)
        result[name.lower().replace("_", "-")] = version
    return result


def test_baseline_and_runtime_manifests_are_consistent():
    head = json.loads(
        (WORKSPACE / "docs" / "current_head_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    runtime = json.loads(
        (WORKSPACE / "runtime-data-manifest.json").read_text(encoding="utf-8")
    )
    assert head["baseline"]["commit"] == BASELINE_COMMIT
    assert head["baseline"]["frozen"] is True
    assert head["assessment"]["pull_request_4_merge_as_is"] is False
    assert runtime["baseline_commit"] == BASELINE_COMMIT
    ids = [item["id"] for item in runtime["artifacts"]]
    assert len(ids) == len(set(ids))
    assert runtime["policy"]["tracked_in_git"] is False


def test_archived_machine_report_manifest_matches_files():
    head = json.loads(
        (WORKSPACE / "docs" / "current_head_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    archive = WORKSPACE / head["machine_report_archive"]["path"]
    for item in head["machine_report_archive"]["files"]:
        path = archive / item["name"]
        content = path.read_bytes()
        assert len(content) == item["size_bytes"]
        assert hashlib.sha256(content).hexdigest() == item["sha256"]


def test_three_environment_templates_cover_the_service_boundaries():
    shared = _env_keys(WORKSPACE / ".env.example")
    prediction = _env_keys(WORKSPACE / "ai_bot3" / "ai_bot3" / ".env.example")
    execution = _env_keys(WORKSPACE / "BybitContractBotV4" / ".env.example")
    assert {"APP_CODE_COMMIT", "BYBIT_TRADING_MODE", "BYBIT_ENABLE_LIVE"} <= shared
    assert {
        "AI_BOT_KLINE_FEATURE_STORE_PATH",
        "BYBIT_PUBLIC_PIT_STORE",
        "MACRO_PIT_STORE",
        "FLOW_PIT_STORE",
    } <= prediction
    assert {
        "BYBIT_TRADING_MODE",
        "BYBIT_ENABLE_LIVE",
        "EXECUTION_DB_PATH",
        "CORRELATED_SYMBOLS",
    } <= execution


def test_platform_locks_share_the_same_application_versions():
    windows = _locked_versions(WORKSPACE / "requirements" / "windows-py312.lock")
    ubuntu = _locked_versions(WORKSPACE / "requirements" / "ubuntu-py311.lock")
    assert set(windows) - {"winloop"} == set(ubuntu)
    for name, version in ubuntu.items():
        assert windows[name] == version


def test_ci_uses_locks_and_has_windows_python312_prediction_regression():
    workflow = (WORKSPACE / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    assert "requirements/ubuntu-py311.lock" in workflow
    assert "requirements/windows-py312.lock" in workflow
    prediction = workflow.split("  prediction-tests:", 1)[1].split(
        "  shadow-e2e:", 1
    )[0]
    assert "os: windows-latest" in prediction
    assert 'python-version: "3.12"' in prediction
