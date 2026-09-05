from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime_config import SettingsError, TradingSettings


def _external_env(mode: str) -> dict[str, str]:
    values = {
        "BYBIT_TRADING_MODE": mode,
        "BYBIT_API_KEY": "placeholder-key",
        "BYBIT_SECRET_KEY": "placeholder-secret",
        "BYBIT_DEDICATED_SUBACCOUNT": "true",
        "POSITION_OWNER_ID": "owner-test-primary",
        "APPROVED_STRATEGY_RELEASE_ID": "sr_test_approved_001",
        "APP_CODE_COMMIT": "abcdef1234567890",
    }
    if mode == "live":
        values.update(
            BYBIT_ENABLE_LIVE="true",
            BYBIT_LIVE_APPROVAL_ID="change-test-001",
        )
    return values


def test_default_position_mode_is_one_way():
    with tempfile.TemporaryDirectory() as directory:
        settings = TradingSettings.load(Path(directory), environ={})
    assert settings.position_mode == "one_way"


@pytest.mark.parametrize("mode", ["testnet", "live"])
def test_external_modes_block_hedge_until_position_ledger_is_leg_aware(mode: str):
    with tempfile.TemporaryDirectory() as directory:
        with pytest.raises(SettingsError, match="positionIdx-aware"):
            TradingSettings.load(
                Path(directory),
                environ={**_external_env(mode), "BYBIT_POSITION_MODE": "hedge"},
            )
