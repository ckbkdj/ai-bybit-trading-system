from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.legacy_forecast_adapter import LegacyForecastAdapter
from contracts.operation_ticket_v1 import OperationTicket
from core.control_plane import ControlPlaneRepository, ImmutableConflict
from core.decision.ticket_builder import TicketBuilder


def actionable_forecast(symbol="BTCUSDT", generated_at="2026-08-21T08:00:00Z", trend="up"):
    signed_return = 0.01 if trend == "up" else -0.01
    return LegacyForecastAdapter().adapt(
        symbol,
        "scalping",
        {
            "generated_at": generated_at,
            "latest_kline_ts": "2026-08-21T07:59:55Z",
            "trend": trend,
            "calibrated_trend": trend,
            "confidence": 0.9,
            "predicted_return": signed_return,
            "data_source_status": "ok",
            "data_source_reliable": True,
            "context_completeness": {"score": 0.96},
            "out_of_distribution_score": 0.1,
            "calibration_status": "valid",
            "current_price_age_seconds": 5,
            "market_regime": "risk_on",
            "model_version": "test-model-v1",
            "factor_scores": {"crypto_microstructure": 0.7},
        },
    )


class TicketAndOutboxTests(unittest.TestCase):
    def test_ticket_builder_emits_only_positive_after_cost_edge(self):
        ticket = TicketBuilder().build_open_ticket(
            actionable_forecast(), reference_price=100000, required_position_version=7
        )
        self.assertIsNotNone(ticket)
        self.assertEqual(ticket.intent.side, "BUY")
        self.assertGreater(ticket.economics.expected_return_after_cost_bps, 0)
        self.assertEqual(ticket.guards.required_position_version, 7)

        weak = actionable_forecast().model_copy(
            update={
                "distribution": actionable_forecast().distribution.model_copy(
                    update={"expected_return_bps": 1.0}
                )
            }
        )
        self.assertIsNone(
            TicketBuilder().build_open_ticket(weak, reference_price=100000, required_position_version=7)
        )

    def test_publish_is_immutable_idempotent_and_cursor_based(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = ControlPlaneRepository(Path(directory) / "control.sqlite3")
            forecast = actionable_forecast()
            ticket = TicketBuilder().build_open_ticket(
                forecast, reference_price=100000, required_position_version=1
            )
            self.assertTrue(repository.publish(forecast, ticket))
            self.assertFalse(repository.publish(forecast, ticket))

            second_forecast = actionable_forecast(
                "ETHUSDT", generated_at="2026-08-21T08:00:00Z", trend="down"
            )
            second_ticket = TicketBuilder().build_open_ticket(
                second_forecast, reference_price=4000, required_position_version=2
            )
            self.assertTrue(repository.publish(second_forecast, second_ticket))

            first_page, first_cursor = repository.list_tickets(limit=1)
            second_page, second_cursor = repository.list_tickets(after_cursor=first_cursor, limit=1)
            self.assertEqual([item.ticket_id for item in first_page + second_page], [ticket.ticket_id, second_ticket.ticket_id])
            self.assertGreater(second_cursor, first_cursor)

            changed = ticket.model_dump(mode="json")
            changed["reason"]["warnings"] = ["mutated"]
            with self.assertRaises(ImmutableConflict):
                repository.publish(forecast, OperationTicket.model_validate(changed))

    def test_claim_lease_has_single_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = ControlPlaneRepository(Path(directory) / "control.sqlite3")
            forecast = actionable_forecast()
            ticket = TicketBuilder().build_open_ticket(
                forecast, reference_price=100000, required_position_version=1
            )
            repository.publish(forecast, ticket)
            first_epoch = repository.claim(ticket.ticket_id, "consumer-a", "lease-a", 60)
            self.assertEqual(first_epoch, 1)
            self.assertIsNone(repository.claim(ticket.ticket_id, "consumer-b", "lease-b", 60))
            self.assertEqual(
                repository.claim(ticket.ticket_id, "consumer-a", "lease-a", 60),
                first_epoch,
            )

    def test_replacement_is_a_new_immutable_ticket(self):
        forecast = actionable_forecast()
        original = TicketBuilder().build_open_ticket(
            forecast, reference_price=100000, required_position_version=1
        )
        replacement = TicketBuilder().build_open_ticket(
            forecast,
            reference_price=100100,
            required_position_version=1,
            supersedes_ticket_id=original.ticket_id,
        )
        self.assertNotEqual(original.ticket_id, replacement.ticket_id)
        self.assertEqual(replacement.supersedes_ticket_id, original.ticket_id)
        self.assertEqual(replacement.intent.action, "REPLACE")


if __name__ == "__main__":
    unittest.main()
