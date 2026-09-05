from __future__ import annotations

from pathlib import Path

from main import run_preflight
from runtime_config import TradingSettings


def _paper_environment(tmp_path: Path) -> dict[str, str]:
    return {
        "APP_ENV": "production",
        "SERVICE_ROLE": "executor",
        "EXECUTION_MODE": "paper",
        "BYBIT_TRADING_MODE": "shadow",
        "HOST_ID": "executor-paper-01",
        "CLUSTER_ID": "two-node-paper",
        "DEPLOYMENT_ID": "two-node-paper-v1",
        "EXECUTION_DB_PATH": str(tmp_path / "execution.sqlite3"),
        "TICKET_API_BASE_URL": "https://predictor-paper.internal:8443",
        "CONTROL_PLANE_API_TOKEN": "executor-specific-token",
        "CONTROL_PLANE_MTLS_CERT": str(tmp_path / "executor.crt"),
        "CONTROL_PLANE_MTLS_KEY": str(tmp_path / "executor.key"),
        "CONTROL_PLANE_CERT_IDENTITY": "executor-paper-01",
        "PREDICTION_CA_BUNDLE": str(tmp_path / "control-plane-ca.crt"),
        "TICKET_CONSUMER_ID": "executor-paper-01",
        "POSITION_OWNER_ID": "paper-owner-executor-01",
        "EXECUTOR_VERSION": "1.0.0",
        "BYBIT_ENABLE_LIVE": "false",
        "MAINNET_ALLOWED": "false",
    }


def test_production_paper_preflight_is_no_private_api_and_no_capital(tmp_path: Path):
    settings = TradingSettings.load(
        root=tmp_path,
        environ=_paper_environment(tmp_path),
    )

    result = run_preflight(settings)

    assert result["status"] == "PASS"
    assert result["execution_mode"] == "paper"
    assert result["legacy_trading_mode"] == "shadow"
    assert result["private_trading_api_enabled"] is False
    assert result["mainnet_order_submission_enabled"] is False
    assert result["real_capital_at_risk"] is False
    assert result["control_plane_https"] is True
