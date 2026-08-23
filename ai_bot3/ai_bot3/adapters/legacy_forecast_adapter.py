from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from contracts.common import deterministic_id
from contracts.forecast_v1 import ForecastEnvelope


# These horizons are the actual settlement/decision horizons used by the live
# prediction loop, not the amount of historical context loaded by each mode.
# Confusing the two made tickets and event gates outlive their source forecast.
MODE_HORIZONS = {
    "scalping": 3 * 60,
    "mid_short": 15 * 60,
    "trend": 2 * 60 * 60,
    "trend_swing": 4 * 60 * 60,
    "swing": 24 * 60 * 60,
}


def _timestamp(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(float(value), timezone.utc)
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            parsed = fallback
    else:
        parsed = fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class LegacyForecastAdapter:
    """Deterministically converts the existing result JSON into v1 without hiding inference."""

    def adapt(self, symbol: str, mode: str, legacy: Mapping[str, Any]) -> ForecastEnvelope:
        normalized_symbol = symbol.strip().upper()
        generated_at = _timestamp(legacy.get("generated_at"), datetime.now(timezone.utc))
        data_cutoff = _timestamp(
            legacy.get("latest_kline_ts") or legacy.get("data_cutoff") or generated_at,
            generated_at,
        )
        if data_cutoff > generated_at:
            data_cutoff = generated_at
        horizon = int(legacy.get("horizon_sec") or MODE_HORIZONS.get(mode, 3600))

        brain = legacy.get("brain_prediction") if isinstance(legacy.get("brain_prediction"), Mapping) else {}
        brain_direction = str(brain.get("direction") or "flat").lower()
        brain_stage = str(brain.get("release_stage") or "unreviewed").lower()
        brain_qualified = bool(brain.get("actionable")) and brain_stage in {"candidate", "live"}
        brain_trend = "up" if brain_direction == "long" else "down" if brain_direction == "short" else "flat"
        trend = str(
            brain_trend
            if brain_qualified
            else legacy.get("calibrated_direction") or legacy.get("calibrated_trend") or legacy.get("trend") or "flat"
        ).lower()
        if trend not in {"up", "down", "flat"}:
            trend = "flat"
        confidence = max(
            0.0,
            min(
                1.0,
                _float(
                    brain.get("confidence")
                    if brain_qualified
                    else legacy.get("direction_confidence") or legacy.get("confidence"),
                    0.5,
                ),
            ),
        )
        directional = 0.34 + 0.46 * confidence
        flat_probability = max(0.05, 0.25 * (1 - confidence))
        opposite = 1.0 - directional - flat_probability
        if trend == "up":
            p_up, p_flat, p_down = directional, flat_probability, opposite
        elif trend == "down":
            p_up, p_flat, p_down = opposite, flat_probability, directional
        else:
            p_flat = max(0.5, directional)
            p_up = p_down = (1 - p_flat) / 2

        predicted_return = legacy.get("calibrated_predicted_return")
        if predicted_return is None:
            predicted_return = legacy.get("predicted_return")
        if brain_qualified:
            brain_return = abs(_float(brain.get("expected_return"), 0.0))
            predicted_return = brain_return if brain_direction == "long" else -brain_return
        expected_return_bps = _float(predicted_return) * 10_000 if predicted_return is not None else None
        source_status = str(legacy.get("data_source_status") or "degraded").lower()
        if source_status not in {"ok", "degraded", "missing", "error"}:
            source_status = "degraded"
        reliable = legacy.get("data_source_reliable") is True and source_status == "ok"
        quality = _float((legacy.get("context_completeness") or {}).get("score") if isinstance(legacy.get("context_completeness"), dict) else legacy.get("context_completeness"), 0.75 if reliable else 0.5)
        quality = max(0.0, min(1.0, quality))
        warnings = ["legacy_inferred_direction_distribution"]
        if brain_qualified:
            warnings.append(f"brain_model_signal:{brain_stage}")
        if not reliable:
            warnings.append(str(legacy.get("data_source_warning") or "legacy_source_not_verified"))

        forecast_id = deterministic_id(
            "fc",
            normalized_symbol,
            mode,
            generated_at.isoformat(),
            legacy.get("model_version"),
        )
        model_bundle = str(
            brain.get("version") if brain_qualified else legacy.get("model_version") or f"legacy-{mode}"
        )
        calibration_status = str(legacy.get("calibration_status") or "unknown").lower()
        if calibration_status not in {"valid", "degraded", "invalid", "unknown"}:
            calibration_status = "degraded"
        feature_age = legacy.get("current_price_age_seconds")
        feature_age_sec = int(_float(feature_age, 2_147_483_647)) if feature_age is not None else 2_147_483_647
        return ForecastEnvelope.model_validate(
            {
                "forecast_id": forecast_id,
                "revision": int(legacy.get("revision") or 1),
                "instrument": {
                    "symbol": normalized_symbol,
                    "exchange": str(legacy.get("forecast_market") or "binance").lower(),
                },
                "time": {
                    "created_at": generated_at,
                    "data_cutoff": data_cutoff,
                    "horizon_sec": horizon,
                    "forecast_target_at": data_cutoff + timedelta(seconds=horizon),
                },
                "distribution": {
                    "p_up": p_up,
                    "p_flat": p_flat,
                    "p_down": p_down,
                    "expected_return_bps": expected_return_bps,
                    "expected_volatility_bps": None,
                    "expected_mae_bps": None,
                    "expected_mfe_bps": None,
                },
                "regime": {
                    "market_regime": str(legacy.get("market_regime") or "unknown"),
                    "liquidity_regime": str(legacy.get("liquidity_regime") or "unknown"),
                    "event_regime": str(legacy.get("event_regime") or "normal"),
                },
                "quality": {
                    "data_coverage": quality,
                    "data_quality": quality,
                    "calibration_status": calibration_status,
                    "range_guard_score": max(
                        0.0,
                        min(
                            1.0,
                            _float(
                                legacy.get("range_guard_score")
                                if legacy.get("range_guard_score") is not None
                                else legacy.get("out_of_distribution_score"),
                                1.0,
                            ),
                        ),
                    ),
                    "max_feature_age_sec": max(0, feature_age_sec),
                    "source_status": source_status,
                    "data_health_status": (
                        "valid"
                        if reliable and quality >= 0.9
                        else "degraded" if source_status in {"ok", "degraded"} else "invalid"
                    ),
                    "predictive_health_status": (
                        "valid"
                        if calibration_status == "valid"
                        else "degraded" if calibration_status in {"degraded", "unknown"} else "invalid"
                    ),
                },
                "factor_scores": dict(legacy.get("factor_scores") or {}),
                "evidence": {"warnings": warnings},
                "lineage": {
                    "strategy_release_id": str(
                        legacy.get("strategy_release_id")
                        or brain.get("strategy_release_id")
                        or f"legacy-{model_bundle}"
                    ),
                    "model_bundle_id": model_bundle,
                    "feature_set_id": str(legacy.get("feature_set_id") or "legacy-feature-set"),
                    "calibration_model_id": str(legacy.get("calibration_model_id") or "legacy-calibration"),
                    "code_commit": str(legacy.get("code_commit") or "unknown"),
                },
            }
        )