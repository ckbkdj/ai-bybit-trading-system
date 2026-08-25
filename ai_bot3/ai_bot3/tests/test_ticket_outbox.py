from __future__ import annotations

import sys
import json
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone


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
            "return_quantiles_bps": (
                {"p10": 70.0, "p25": 85.0, "p50": 100.0, "p75": 115.0, "p90": 130.0}
                if trend == "up"
                else {"p10": -130.0, "p25": -115.0, "p50": -100.0, "p75": -85.0, "p90": -70.0}
            ),
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

    def test_ten_thousand_expired_tickets_fast_forward_without_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = ControlPlaneRepository(Path(directory) / "control.sqlite3")
            forecast = actionable_forecast(
                generated_at=datetime.now(timezone.utc).isoformat()
            )
            repository.publish(forecast, None)
            template = TicketBuilder().build_open_ticket(
                forecast, reference_price=100_000, required_position_version=0
            ).model_dump(mode="json")
            expired_at = datetime.now(timezone.utc) - timedelta(hours=1)
            rows = []
            for index in range(10_000):
                payload = dict(template)
                payload["ticket_id"] = f"tk_expired_{index:05d}"
                payload["created_at"] = (expired_at - timedelta(minutes=2)).isoformat()
                payload["valid_from"] = (expired_at - timedelta(minutes=1)).isoformat()
                payload["expires_at"] = expired_at.isoformat()
                payload_json = json.dumps(payload, sort_keys=True)
                rows.append(
                    (
                        payload["ticket_id"],
                        forecast.forecast_id,
                        forecast.revision,
                        None,
                        "BTCUSDT",
                        index + 1,
                        None,
                        payload["created_at"],
                        payload["expires_at"],
                        payload_json,
                        "0" * 64,
                    )
                )
            with repository.transaction(immediate=True) as connection:
                connection.executemany(
                    """INSERT INTO operation_tickets(
                        ticket_id,forecast_id,forecast_revision,supersedes_ticket_id,symbol,
                        decision_version,allowed_consumer_id,created_at,expires_at,
                        payload_json,payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    rows,
                )
            live = TicketBuilder().build_open_ticket(
                forecast, reference_price=100_100, required_position_version=0
            )
            repository.publish(forecast, live)
            page, cursor, skipped = repository.eligible_ticket_page(
                0, "executor-a", scan_limit=20_000
            )
            self.assertEqual([item.ticket_id for _, item in page], [live.ticket_id])
            self.assertEqual(skipped["expired_skipped"], 10_000)
            self.assertEqual(cursor, 10_001)

    def test_second_executor_cannot_take_active_consumer_account(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = ControlPlaneRepository(Path(directory) / "control.sqlite3")
            now = datetime.now(timezone.utc)
            first = repository.activate_consumer(
                "executor-a", "instance-one", "account-001", now=now
            )
            second = repository.activate_consumer(
                "executor-a", "instance-two", "account-001", now=now
            )
            renewed = repository.activate_consumer(
                "executor-a", "instance-one", "account-001", now=now
            )
            self.assertEqual(first, 1)
            self.assertIsNone(second)
            self.assertEqual(renewed, 1)

    def test_highest_decision_version_wins_even_if_published_earlier(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = ControlPlaneRepository(Path(directory) / "control.sqlite3")
            now = datetime.now(timezone.utc)
            forecast = actionable_forecast(generated_at=now.isoformat())
            first = TicketBuilder().build_open_ticket(
                forecast, reference_price=100_000, required_position_version=0
            )
            second = TicketBuilder().build_open_ticket(
                forecast,
                reference_price=100_100,
                required_position_version=0,
                supersedes_ticket_id=first.ticket_id,
            )
            repository.publish(forecast, first)
            repository.publish(forecast, second)
            with repository.transaction(immediate=True) as connection:
                connection.execute(
                    "UPDATE operation_tickets SET decision_version=10 WHERE ticket_id=?",
                    (first.ticket_id,),
                )
                connection.execute(
                    "UPDATE operation_tickets SET decision_version=9 WHERE ticket_id=?",
                    (second.ticket_id,),
                )
            page, cursor, skipped = repository.eligible_ticket_page(0, "executor-a")
            self.assertEqual([ticket.ticket_id for _, ticket in page], [first.ticket_id])
            self.assertEqual(cursor, 2)
            self.assertEqual(skipped["superseded_skipped"], 1)


if __name__ == "__main__":
    unittest.main()
