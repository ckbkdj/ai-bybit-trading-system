from __future__ import annotations

import json
import re
from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[3]


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


def test_versioned_runtime_manifest_has_complete_access_contracts():
    runtime = json.loads(
        (WORKSPACE / "runtime-data-manifest.v1.json").read_text(encoding="utf-8")
    )
    assert runtime["schema_version"] == "runtime-data-manifest.v1"
    names = [item["logical_name"] for item in runtime["artifacts"]]
    assert len(names) == len(set(names))
    required = {
        "logical_name",
        "path_env",
        "required_for",
        "schema_version",
        "read_only",
        "read_write",
        "backup_required",
        "contains_secrets",
        "minimum_coverage",
        "health_check",
    }
    for item in runtime["artifacts"]:
        assert required <= set(item)
        assert item["read_only"] is not item["read_write"]
    assert (WORKSPACE / "schemas" / "runtime-data-manifest.v1.schema.json").is_file()


def test_old_evaluation_reports_are_historical_and_current_path_fails_closed():
    archive = (
        WORKSPACE
        / "docs"
        / "evidence"
        / "history"
        / "7579fb63f93f0e77cf311ec73777de0291b361f8"
    )
    archived = {path.name for path in archive.glob("*.json")}
    assert {
        "profitability_report.json",
        "walk_forward_report.json",
        "lockbox_report.json",
    } <= archived
    active = WORKSPACE / "ai_bot3" / "ai_bot3" / "model_results" / "evaluation"
    assert {path.name for path in active.glob("*.json")} == {
        "current_release_status.json"
    }
    status = json.loads((active / "current_release_status.json").read_text(encoding="utf-8"))
    assert status["profitability_evidence"] == "STALE_NOT_REGENERATED"
    assert status["profitability_gate"] == "FAILED"
    assert status["candidate_count"] == status["live_count"] == 0
    assert status["mainnet_allowed"] is False


def test_four_environment_profiles_are_fail_closed():
    paths = {
        "prediction_shadow": WORKSPACE / "ai_bot3" / "ai_bot3" / ".env.shadow.example",
        "prediction_research": WORKSPACE / "ai_bot3" / "ai_bot3" / ".env.research.example",
        "execution_shadow": WORKSPACE / "BybitContractBotV4" / ".env.shadow.example",
        "execution_testnet": WORKSPACE / "BybitContractBotV4" / ".env.testnet.example",
    }
    for path in paths.values():
        keys = _env_keys(path)
        assert {"APP_CODE_COMMIT", "BYBIT_TRADING_MODE", "BYBIT_ENABLE_LIVE"} <= keys
        text = path.read_text(encoding="utf-8")
        assert "BYBIT_ENABLE_LIVE=false" in text
        assert "BYBIT_LIVE_APPROVAL_ID=" in text
        assert "BYBIT_TRADING_MODE=live" not in text
    testnet = paths["execution_testnet"].read_text(encoding="utf-8")
    assert "BYBIT_TRADING_MODE=testnet" in testnet
    assert "BYBIT_DEDICATED_SUBACCOUNT=true" in testnet
    assert "BYBIT_ALLOW_MANUAL_ORDERS=false" in testnet
    assert "BYBIT_POSITION_MODE=one_way" in testnet


def test_platform_locks_are_complete_and_hashed():
    locks = (
        WORKSPACE / "requirements" / "windows-py312.lock",
        WORKSPACE / "requirements" / "windows-py311.lock",
        WORKSPACE / "requirements" / "linux-py311.lock",
    )
    for path in locks:
        versions = _locked_versions(path)
        assert {"pytest", "pydantic", "tensorflow", "pybit"} <= set(versions)
        entries = [
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert entries
        assert all(re.search(r" --hash=sha256:[0-9a-f]{64}$", line) for line in entries)


def test_ci_uses_locks_and_has_windows_python312_prediction_regression():
    workflow = (WORKSPACE / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    assert "requirements/linux-py311.lock" in workflow
    assert "requirements/windows-py312.lock" in workflow
    assert "--require-hashes" in workflow
    assert "npm audit" in workflow
    assert "cyclonedx-json" in workflow
    prediction = workflow.split("  prediction-tests:", 1)[1].split(
        "  shadow-e2e:", 1
    )[0]
    assert "os: windows-latest" in prediction
    assert 'python-version: "3.12"' in prediction


def test_root_readme_links_only_current_documentation():
    readme = (WORKSPACE / "README.md").read_text(encoding="utf-8")
    links = re.findall(r"\[[^]]+\]\(([^)]+)\)", readme)
    assert links
    assert all(link.startswith("docs/current/") for link in links)
