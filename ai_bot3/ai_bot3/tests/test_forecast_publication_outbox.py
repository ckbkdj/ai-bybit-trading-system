from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core.control_plane import ControlPlaneRepository
from core.publication_outbox import (
    ForecastPublicationOutbox,
    OutboxLimits,
    PublicationCapacityError,
    PublicationWorker,
)
from core.result_manager import ResultManager
from tests.test_result_manager_ticket_gate import _prediction
from tests.test_ticket_outbox import actionable_forecast


def test_control_plane_failure_does_not_block_prediction_save(tmp_path):
    manager = ResultManager(
        tmp_path / "results",
        control_plane_db=tmp_path / "unavailable" / "control.sqlite3",
        publication_outbox_db=tmp_path / "predictor-outbox.sqlite3",
        tickets_enabled=False,
    )
    asyncio.run(manager.save_result("BTCUSDT", "scalping", _prediction("shadow")))
    assert (tmp_path / "results" / "BTCUSDT_scalping.json").is_file()
    assert manager.publication_outbox.metrics()["pending"] == 1


def test_six_hour_executor_outage_replays_forecasts_idempotently(tmp_path):
    outbox = ForecastPublicationOutbox(
        tmp_path / "outbox.sqlite3",
        limits=OutboxLimits(min_disk_free_bytes=0),
    )
    start = datetime.now(timezone.utc) - timedelta(hours=6)
    for hour in range(7):
        forecast = actionable_forecast(
            generated_at=(start + timedelta(hours=hour)).isoformat().replace("+00:00", "Z")
        )
        assert outbox.enqueue(forecast, now=start + timedelta(hours=hour))
    assert outbox.metrics()["pending"] == 7

    def unavailable():
        raise OSError("control plane unavailable")

    failed = PublicationWorker(outbox, unavailable).run_once(now=datetime.now(timezone.utc))
    assert failed["retried"] == 7
    assert outbox.metrics()["pending"] == 7

    control = ControlPlaneRepository(tmp_path / "control.sqlite3")
    recovered = PublicationWorker(outbox, control).run_once(
        now=datetime.now(timezone.utc) + timedelta(hours=2)
    )
    assert recovered["acknowledged"] == 7
    assert outbox.metrics()["pending"] == 0
    assert len(control.list_tickets()[0]) == 0
    assert control.latest_forecast("BTCUSDT") is not None


def test_expired_ticket_is_archived_and_never_becomes_executable(tmp_path):
    from core.decision.ticket_builder import TicketBuilder

    outbox = ForecastPublicationOutbox(
        tmp_path / "outbox.sqlite3", limits=OutboxLimits(min_disk_free_bytes=0)
    )
    forecast = actionable_forecast(generated_at="2026-08-21T08:00:00Z")
    ticket = TicketBuilder().build_open_ticket(
        forecast, reference_price=100_000, required_position_version=0
    )
    assert ticket is not None
    outbox.enqueue(forecast, ticket, now=forecast.time.created_at)
    control = ControlPlaneRepository(tmp_path / "control.sqlite3")
    result = PublicationWorker(outbox, control).run_once(
        now=ticket.expires_at + timedelta(seconds=1)
    )
    assert result["archived_expired"] == 1
    assert control.list_tickets()[0] == []
    assert outbox.metrics()["archived"] == 1


def test_outbox_capacity_gate_raises_instead_of_dropping(tmp_path):
    outbox = ForecastPublicationOutbox(
        tmp_path / "outbox.sqlite3",
        limits=OutboxLimits(max_pending=1, min_disk_free_bytes=0),
    )
    outbox.enqueue(actionable_forecast("BTCUSDT"))
    with pytest.raises(PublicationCapacityError, match="pending-count"):
        outbox.enqueue(actionable_forecast("ETHUSDT"))
    assert outbox.metrics()["pending"] == 1
