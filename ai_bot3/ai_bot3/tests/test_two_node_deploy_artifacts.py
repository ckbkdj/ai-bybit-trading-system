from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_predictor_artifact_has_four_limited_services_and_no_execution_secret():
    environment = _read(
        "deploy/predictor-production-paper/.env.production-paper.example"
    )
    assert "SERVICE_ROLE=predictor" in environment
    assert "EXECUTION_MODE=paper" in environment
    keys = {
        line.split("=", 1)[0]
        for line in environment.splitlines()
        if line and not line.startswith("#") and "=" in line
    }
    assert not {"BYBIT_API_KEY", "BYBIT_SECRET_KEY", "EXECUTION_DB_PATH"} & keys
    units = (
        "predictor-realtime.service",
        "control-plane-api.service",
        "market-collector.service",
        "publication-worker.service",
    )
    paths = set()
    for unit in units:
        content = _read(f"deploy/predictor-production-paper/systemd/{unit}")
        for setting in ("CPUQuota=", "MemoryMax=", "Nice=", "IOWeight="):
            assert setting in content
        path_line = next(
            line for line in content.splitlines() if line.startswith("ReadWritePaths=")
        )
        paths.add(path_line)
    assert len(paths) == 4


def test_executor_paper_artifact_has_no_predictor_internal_dependency():
    environment = _read(
        "deploy/executor-production-paper/.env.production-paper.example"
    )
    assert "SERVICE_ROLE=executor" in environment
    assert "EXECUTION_MODE=paper" in environment
    assert "BYBIT_ENABLE_LIVE=false" in environment
    for forbidden in (
        "MODEL_OUTPUT_DIR=",
        "AI_BOT_MODEL_WRITE_DIR=",
        "BYBIT_PUBLIC_PIT_STORE=",
        "RESEARCH_JOB_DB=",
        "TRAINING_ENABLED=",
        "BACKFILL_ENABLED=",
    ):
        assert forbidden not in environment


def test_mtls_proxy_and_inactive_testnet_gate_are_explicit():
    proxy = _read(
        "deploy/predictor-production-paper/nginx/control-plane-mtls.conf"
    )
    assert "listen 10.70.0.1:8443 ssl" in proxy
    assert "ssl_verify_client on" in proxy
    assert "X-Client-Certificate-Identity $ssl_client_s_dn_cn" in proxy
    testnet_unit = _read("deploy/executor-testnet/systemd/executor-testnet.service")
    testnet_environment = _read("deploy/executor-testnet/.env.testnet.example")
    assert "ConditionPathExists=/etc/ai-bybit/TESTNET_HUMAN_APPROVED" in testnet_unit
    assert "EXECUTION_MODE=testnet" in testnet_environment
    assert "BYBIT_ENABLE_LIVE=false" in testnet_environment
    assert "MAINNET_ALLOWED=false" in testnet_environment
