from __future__ import annotations

from datetime import datetime, timedelta, timezone

from adapters.legacy_forecast_adapter import LegacyForecastAdapter, MODE_HORIZONS


EXPECTED_MODE_HORIZONS = {
    "scalping": 180,
    "mid_short": 900,
    "trend": 7200,
    "trend_swing": 14400,
    "swing": 86400,
}


def _legacy_payload() -> dict:
    return {
        "generated_at": "2026-08-21T08:00:00Z",
        "latest_kline_ts": "2026-08-21T07:59:55Z",
        "trend": "up",
        "confidence": 0.9,
        "predicted_return": 0.01,
        "current_price_age_seconds": 5,
        "data_source_status": "ok",
        "data_source_reliable": True,
        "context_completeness": {"score": 0.96},
        "calibration_status": "valid",
        "range_guard_score": 0.1,
        "strategy_release_id": "sr_horizon_truth_001",
        "model_version": "horizon-truth-v1",
    }


def test_registry_matches_actual_prediction_settlement_horizons():
    assert MODE_HORIZONS == EXPECTED_MODE_HORIZONS


def test_adapter_target_time_uses_mode_settlement_horizon_not_training_context():
    adapter = LegacyForecastAdapter()
    cutoff = datetime(2026, 8, 21, 7, 59, 55, tzinfo=timezone.utc)

    for mode, expected_seconds in EXPECTED_MODE_HORIZONS.items():
        forecast = adapter.adapt("BTCUSDT", mode, _legacy_payload())
        assert forecast.time.horizon_sec == expected_seconds
        assert forecast.time.forecast_target_at == cutoff + timedelta(seconds=expected_seconds)


def test_explicit_horizon_override_remains_auditable():
    payload = _legacy_payload()
    payload["horizon_sec"] = 600
    forecast = LegacyForecastAdapter().adapt("BTCUSDT", "scalping", payload)
    assert forecast.time.horizon_sec == 600
