from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.legacy_forecast_adapter import LegacyForecastAdapter
from contracts.execution_receipt_v1 import ExecutionReceipt
from core.decision.receipt_cost_model import (
    ExecutionCostObservation,
    ReceiptCalibratedCostModel,
)
from core.decision.ticket_builder import TicketBuilder
from core.evaluation.execution_labels import ExecutionLabelBuilder, PriceObservation


NOW = datetime(2026, 8, 21, 8, 0, tzinfo=timezone.utc)


def _ticket():
    forecast = LegacyForecastAdapter().adapt(
        "BTCUSDT",
        "scalping",
        {
            "generated_at": NOW,
            "latest_kline_ts": NOW - timedelta(seconds=5),
            "trend": "up",
            "confidence": 0.9,
            "predicted_return": 0.01,
            "return_quantiles_bps": {
                "p10": 70.0,
                "p25": 85.0,
                "p50": 100.0,
                "p75": 115.0,
                "p90": 130.0,
            },
            "current_price_age_seconds": 5,
            "data_source_status": "ok",
            "data_source_reliable": True,
            "context_completeness": {"score": 0.96},
            "calibration_status": "valid",
            "range_guard_score": 0.1,
            "strategy_release_id": "sr_execution_label_test_001",
        },
    )
    return TicketBuilder().build_open_ticket(
        forecast, reference_price=100_000, required_position_version=1
    )


def test_execution_label_tracks_fill_cost_path_and_first_barrier():
    ticket = _ticket()
    receipt = ExecutionReceipt.model_validate(
        {
            "receipt_id": "rc_execution_label_test_001",
            "ticket_id": ticket.ticket_id,
            "consumer_id": "shadow-a",
            "mode": "shadow",
            "status": "FILLED",
            "orders": [
                {
                    "order_link_id": "qt_execution_label_entry",
                    "role": "entry",
                    "order_status": "FILLED",
                    "side": "BUY",
                    "order_type": "LIMIT",
                    "quantity": 0.1,
                    "price": 100_010,
                    "cum_exec_qty": 0.1,
                    "avg_exec_price": 100_010,
                }
            ],
            "fills": [
                {
                    "exec_id": "exec-label-1",
                    "order_link_id": "qt_execution_label_entry",
                    "quantity": 0.1,
                    "price": 100_010,
                    "exec_fee": 2.0,
                    "executed_at": NOW + timedelta(seconds=10),
                }
            ],
            "total_exec_fee": 2.0,
            "created_at": NOW,
            "updated_at": NOW + timedelta(minutes=2),
        }
    )
    target = ticket.protection.take_profit[0].price
    label = ExecutionLabelBuilder().build(
        ticket,
        receipt,
        price_path=[
            PriceObservation(NOW + timedelta(seconds=20), 99_500),
            PriceObservation(NOW + timedelta(seconds=30), target + 1),
        ],
        funding_bps=0.4,
    )
    assert label.entry_fill_fraction == 1
    assert label.time_to_first_fill_sec == 10
    assert label.first_barrier == "TAKE_PROFIT"
    assert label.mfe_bps > 0
    assert label.mae_bps > 0
    assert label.realised_cost_bps > 0

    observations = [
        ExecutionCostObservation(
            label=label,
            order_type="LIMIT",
            notional_bucket="small",
            spread_bucket="tight",
            depth_bucket="deep",
            session="asia",
            regime="risk_on",
            fee_tier="vip0",
        )
        for _ in range(30)
    ]
    estimate = ReceiptCalibratedCostModel(observations).estimate(
        50,
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        notional_bucket="small",
        spread_bucket="tight",
        depth_bucket="deep",
        session="asia",
        regime="risk_on",
        fee_tier="vip0",
    )
    assert estimate.fee_bps == label.fee_bps
    assert estimate.slippage_bps == label.slippage_bps
