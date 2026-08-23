from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pydantic import ValidationError


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.legacy_forecast_adapter import LegacyForecastAdapter
from contracts.execution_receipt_v1 import ExecutionReceipt
from contracts.generate_schemas import generate
from contracts.operation_ticket_v1 import OperationTicket
from contracts.schema_validation import SchemaValidationError, validate_schema_file


NOW = datetime(2026, 8, 21, 8, 0, tzinfo=timezone.utc)


def operation_payload():
    return {
        "ticket_id": "tk_01k2contracttest",
        "forecast_id": "fc_01k2contracttest",
        "forecast_revision": 1,
        "portfolio_decision_id": "pd_01k2contracttest",
        "strategy_release_id": "sr_01k2contracttest",
        "created_at": NOW,
        "valid_from": NOW,
        "expires_at": NOW + timedelta(minutes=5),
        "instrument": {"symbol": "BTCUSDT"},
        "intent": {
            "action": "OPEN",
            "side": "BUY",
            "position_effect": "OPEN_OR_INCREASE",
            "target_exposure_pct": 0.08,
            "risk_budget_pct": 0.003,
            "max_notional_usdt": 5000,
            "leverage_cap": 3,
        },
        "entry": {
            "order_type": "LIMIT",
            "reference_price": 100000,
            "limit_price": 99990,
            "price_band_bps": 12,
            "max_slippage_bps": 6,
            "max_wait_sec": 90,
        },
        "protection": {
            "stop_loss": {"type": "MARK_PRICE", "price": 99000, "max_loss_bps": 100},
            "take_profit": [{"price": 102000, "close_fraction": 0.5}],
            "max_holding_sec": 14400,
        },
        "economics": {
            "expected_return_bps": 70,
            "estimated_fee_bps": 10,
            "estimated_slippage_bps": 5,
            "estimated_funding_bps": 2,
            "model_error_buffer_bps": 13,
            "expected_return_after_cost_bps": 40,
        },
        "guards": {
            "min_data_quality": 0.9,
            "observed_data_quality": 0.95,
            "max_feature_age_sec": 120,
            "observed_feature_age_sec": 10,
            "max_live_spread_bps": 10,
            "max_live_price_deviation_bps": 18,
            "required_market_regime": ["risk_on"],
            "observed_market_regime": "risk_on",
            "required_position_version": 3,
        },
        "reason": {"regime": "risk_on"},
    }


class ContractTests(unittest.TestCase):
    def test_forecast_passes_pydantic_and_json_schema(self):
        forecast = LegacyForecastAdapter().adapt(
            "BTCUSDT",
            "scalping",
            {
                "generated_at": "2026-08-21T08:00:00Z",
                "latest_kline_ts": "2026-08-21T07:59:55Z",
                "trend": "up",
                "confidence": 0.8,
                "predicted_return": 0.006,
                "data_source_status": "ok",
                "data_source_reliable": True,
                "context_completeness": {"score": 0.94},
                "model_version": "legacy-golden-v1",
            },
        )
        with tempfile.TemporaryDirectory() as directory:
            schema_dir = Path(directory)
            generate(schema_dir)
            validate_schema_file(
                forecast.model_dump(mode="json"), schema_dir / "forecast-envelope.v1.json"
            )
        self.assertEqual(forecast.forecast_id, "fc_uf6ztxfcijxdsl6tb5xc7mohw2")
        self.assertEqual(forecast.instrument.symbol, "BTCUSDT")

    def test_operation_and_receipt_pass_both_validators(self):
        ticket = OperationTicket.model_validate(operation_payload())
        receipt = ExecutionReceipt.model_validate(
            {
                "receipt_id": "rc_01k2contracttest",
                "ticket_id": ticket.ticket_id,
                "consumer_id": "shadow-a",
                "mode": "shadow",
                "status": "VALIDATED",
                "created_at": NOW,
                "updated_at": NOW,
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            schema_dir = Path(directory)
            generate(schema_dir)
            validate_schema_file(ticket.model_dump(mode="json"), schema_dir / "operation-ticket.v1.json")
            validate_schema_file(receipt.model_dump(mode="json"), schema_dir / "execution-receipt.v1.json")

    def test_contract_rejects_unknown_fields_and_is_frozen(self):
        payload = operation_payload()
        payload["unknown"] = True
        with self.assertRaises(ValidationError):
            OperationTicket.model_validate(payload)
        ticket = OperationTicket.model_validate(operation_payload())
        with self.assertRaises(ValidationError):
            ticket.ticket_id = "mutated"

    def test_hold_and_missing_risk_fields_are_rejected(self):
        payload = operation_payload()
        payload["intent"]["action"] = "HOLD"
        with self.assertRaises(ValidationError):
            OperationTicket.model_validate(payload)

        payload = operation_payload()
        payload["protection"]["stop_loss"] = None
        with self.assertRaises(ValidationError):
            OperationTicket.model_validate(payload)

    def test_schema_validation_is_independent(self):
        ticket = OperationTicket.model_validate(operation_payload()).model_dump(mode="json")
        del ticket["guards"]
        with tempfile.TemporaryDirectory() as directory:
            schema_dir = Path(directory)
            generate(schema_dir)
            with self.assertRaises(SchemaValidationError):
                validate_schema_file(ticket, schema_dir / "operation-ticket.v1.json")


if __name__ == "__main__":
    unittest.main()
