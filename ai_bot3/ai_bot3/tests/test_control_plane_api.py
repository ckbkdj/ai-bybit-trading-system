from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import fastapi  # noqa: F401
except ImportError:
    fastapi = None


@unittest.skipIf(fastapi is None, "FastAPI runtime is not installed in this test interpreter")
class ControlPlaneApiTests(unittest.TestCase):
    def setUp(self):
        from adapters.legacy_forecast_adapter import LegacyForecastAdapter
        from api.control_plane_api import create_control_plane_router
        from core.decision.ticket_builder import TicketBuilder

        self.temp = tempfile.TemporaryDirectory()
        control_db = str(Path(self.temp.name) / "control.sqlite3")
        research_db = str(Path(self.temp.name) / "research.sqlite3")
        self.environment = patch.dict(
            os.environ,
            {"CONTROL_PLANE_DB": control_db, "RESEARCH_JOB_DB": research_db},
        )
        self.environment.start()
        self.router = create_control_plane_router(ROOT)
        self.repository = self.router.control_repository
        generated_at = datetime.now(timezone.utc)
        self.forecast = LegacyForecastAdapter().adapt(
            "BTCUSDT",
            "scalping",
            {
                "generated_at": generated_at.isoformat(),
                "latest_kline_ts": (generated_at - timedelta(seconds=5)).isoformat(),
                "trend": "up",
                "calibrated_trend": "up",
                "confidence": 0.9,
                "predicted_return": 0.01,
                "return_quantiles_bps": {
                    "p10": 70.0,
                    "p25": 85.0,
                    "p50": 100.0,
                    "p75": 115.0,
                    "p90": 130.0,
                },
                "data_source_status": "ok",
                "data_source_reliable": True,
                "context_completeness": {"score": 0.96},
                "out_of_distribution_score": 0.1,
                "calibration_status": "valid",
                "current_price_age_seconds": 5,
                "market_regime": "risk_on",
                "model_version": "api-test-model",
            },
        )
        self.ticket = TicketBuilder().build_open_ticket(
            self.forecast, reference_price=100000, required_position_version=0
        )
        self.repository.publish(self.forecast, self.ticket)

    def tearDown(self):
        self.environment.stop()
        self.temp.cleanup()

    def endpoint(self, path, method="GET"):
        for route in self.router.routes:
            if route.path == path and method in (route.methods or set()):
                return route.endpoint
        raise AssertionError(f"route not found: {method} {path}")

    def test_schema_forecast_ticket_claim_and_receipt_endpoints(self):
        from api.control_plane_api import ClaimRequest
        from contracts.execution_receipt_v1 import ExecutionReceipt

        health = self.endpoint("/v1/health")()
        self.assertEqual(health["status"], "ok")
        self.assertEqual(self.endpoint("/v1/health/live")()["status"], "live")
        self.assertEqual(self.endpoint("/v1/health/ready")()["status"], "ready")
        capabilities = self.endpoint("/v1/capabilities")()
        self.assertIn("operation-ticket.v1", capabilities["supported_ticket_schemas"])
        self.assertGreaterEqual(capabilities["latest_forecast_age_seconds"], 0)
        self.assertIn("unix_time", self.endpoint("/v1/time")())
        schema_response = self.endpoint("/v1/schema/{schema_name}")("operation-ticket")
        self.assertIn(b"operation-ticket.v1", schema_response.body)
        latest = self.endpoint("/v1/forecasts/latest")(symbol="BTCUSDT")
        self.assertEqual(latest["forecast_id"], self.forecast.forecast_id)
        page = self.endpoint("/v1/tickets")(
            after_cursor=0, limit=100, consumer_id="test-consumer"
        )
        self.assertEqual(len(page["items"]), 1)
        claim = self.endpoint("/v1/tickets/{ticket_id}/claim", "POST")(
            self.ticket.ticket_id,
            ClaimRequest(consumer_id="test-consumer", lease_token="lease-token-001", lease_sec=60),
        )
        self.assertTrue(claim["claimed"])
        now = datetime.now(timezone.utc)
        receipt = ExecutionReceipt.model_validate(
            {
                "receipt_id": "rc_api_endpoint_001",
                "ticket_id": self.ticket.ticket_id,
                "consumer_id": "test-consumer",
                "mode": "shadow",
                "status": "VALIDATED",
                "created_at": now,
                "updated_at": now,
            }
        )
        accepted = self.endpoint("/v1/executions", "POST")(receipt)
        self.assertTrue(accepted["accepted"])


if __name__ == "__main__":
    unittest.main()
