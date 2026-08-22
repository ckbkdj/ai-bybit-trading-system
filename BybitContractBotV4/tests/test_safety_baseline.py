from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


BOT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BOT_ROOT))

from exchange_gateway import (
    ExchangeGateway,
    LazyExchangeGateway,
    ShadowExchange,
    build_exchange_gateway,
)
from prediction_client import PredictionUnavailable, parse_prediction_payload
from runtime_config import SettingsError, TradingMode, TradingSettings


class RuntimeConfigTests(unittest.TestCase):
    def load(self, values=None):
        with tempfile.TemporaryDirectory() as directory:
            return TradingSettings.load(Path(directory), environ=values or {})

    def test_default_mode_is_shadow_and_needs_no_credentials(self):
        settings = self.load()
        self.assertIs(settings.mode, TradingMode.SHADOW)
        self.assertFalse(settings.enable_live)
        self.assertEqual(settings.live_approval_id, "")
        self.assertEqual(settings.api_key, "")
        self.assertIn("BTCUSDT", settings.correlated_symbols)

    def test_correlated_symbol_group_is_explicit_and_normalized(self):
        settings = self.load({"CORRELATED_SYMBOLS": "btcusdt, ethusdt"})
        self.assertEqual(settings.correlated_symbols, frozenset({"BTCUSDT", "ETHUSDT"}))

    def test_live_mode_is_fail_closed_without_explicit_gate(self):
        with self.assertRaisesRegex(SettingsError, "live mode is blocked"):
            self.load(
                {
                    "BYBIT_TRADING_MODE": "live",
                    "BYBIT_ENABLE_LIVE": "true",
                    "BYBIT_API_KEY": "placeholder-key",
                    "BYBIT_SECRET_KEY": "placeholder-secret",
                }
            )

    def test_testnet_and_live_require_credentials(self):
        with self.assertRaisesRegex(SettingsError, "requires BYBIT_API_KEY"):
            self.load({"BYBIT_TRADING_MODE": "testnet"})
        with self.assertRaisesRegex(SettingsError, "requires BYBIT_API_KEY"):
            self.load(
                {
                    "BYBIT_TRADING_MODE": "live",
                    "BYBIT_ENABLE_LIVE": "true",
                    "BYBIT_LIVE_APPROVAL_ID": "change-20260822-001",
                }
            )

    def test_live_mode_requires_both_gate_and_credentials(self):
        settings = self.load(
            {
                "BYBIT_TRADING_MODE": "live",
                "BYBIT_ENABLE_LIVE": "true",
                "BYBIT_LIVE_APPROVAL_ID": "change-20260822-001",
                "BYBIT_API_KEY": "placeholder-key",
                "BYBIT_SECRET_KEY": "placeholder-secret",
            }
        )
        self.assertIs(settings.mode, TradingMode.LIVE)


class ShadowExchangeTests(unittest.TestCase):
    def test_factory_is_lazy(self):
        calls = []
        lazy = LazyExchangeGateway(
            lambda: calls.append("created") or ExchangeGateway(mode="shadow")
        )
        self.assertFalse(lazy.initialized)
        self.assertEqual(calls, [])
        self.assertEqual(lazy.get_all_open_positions(), [])
        self.assertTrue(lazy.initialized)
        self.assertEqual(calls, ["created"])

    def test_shadow_order_never_uses_ccxt_or_private_network(self):
        settings = TradingSettings.load(BOT_ROOT, environ={})
        client = build_exchange_gateway(settings)
        self.assertIsInstance(client.exchange, ShadowExchange)

        order = client.create_ticket_order(
            symbol="ETHUSDT",
            side="BUY",
            order_type="MARKET",
            amount=0.025,
            price=None,
            leverage=5,
            order_link_id="shadow-test-entry",
        )

        self.assertTrue(order["shadow"])
        self.assertTrue(order["id"].startswith("shadow-"))
        self.assertEqual(len(client.exchange.orders), 1)
        self.assertEqual(client.exchange.orders[0]["symbol"], "ETHUSDT")


class PredictionContractTests(unittest.TestCase):
    def test_legacy_payload_uses_requested_symbol_not_xrp(self):
        payload = {
            "ETHUSDT": {
                "scalping": {
                    "symbol": "ETHUSDT",
                    "trend": "up",
                    "generated_at": 995.0,
                    "data_source_status": "ok",
                    "data_source_reliable": True,
                }
            },
            "XRPUSDT": {
                "scalping": {
                    "symbol": "XRPUSDT",
                    "trend": "down",
                    "generated_at": 995.0,
                }
            },
        }
        result = parse_prediction_payload(payload, "ETHUSDT", "scalping", 30, now=1000)
        self.assertEqual(result["trend"], "up")

    def test_new_predict_payload_is_supported(self):
        payload = {
            "modes": {
                "scalping": {
                    "local_prediction": {
                        "symbol": "BTCUSDT",
                        "trend": "flat",
                        "generated_at": 999.0,
                        "data_source_status": "ok",
                    }
                }
            }
        }
        result = parse_prediction_payload(payload, "BTCUSDT", "scalping", 30, now=1000)
        self.assertEqual(result["trend"], "flat")

    def test_new_predict_payload_rejects_top_level_symbol_mismatch(self):
        payload = {
            "symbol": "XRPUSDT",
            "modes": {
                "scalping": {
                    "local_prediction": {
                        "trend": "up",
                        "generated_at": 999.0,
                        "data_source_status": "ok",
                    }
                }
            },
        }
        with self.assertRaisesRegex(PredictionUnavailable, "symbol mismatch"):
            parse_prediction_payload(payload, "ETHUSDT", "scalping", 30, now=1000)

    def test_stale_or_unreliable_prediction_is_rejected(self):
        stale = {
            "BTCUSDT": {
                "scalping": {"trend": "up", "generated_at": 900.0}
            }
        }
        with self.assertRaisesRegex(PredictionUnavailable, "stale"):
            parse_prediction_payload(stale, "BTCUSDT", "scalping", 30, now=1000)

        unreliable = {
            "BTCUSDT": {
                "scalping": {
                    "trend": "up",
                    "generated_at": 999.0,
                    "data_source_reliable": False,
                }
            }
        }
        with self.assertRaisesRegex(PredictionUnavailable, "unreliable"):
            parse_prediction_payload(unreliable, "BTCUSDT", "scalping", 30, now=1000)


if __name__ == "__main__":
    unittest.main()
