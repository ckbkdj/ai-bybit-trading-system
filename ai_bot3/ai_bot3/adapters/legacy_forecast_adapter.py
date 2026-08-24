from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from contracts.common import deterministic_id
from contracts.forecast_v1 import ForecastEnvelope
from contracts.horizons import MODE_HORIZONS


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

    def adapt(
        self,
        symbol: str,
        mode: str,
        legacy: Mapping[str, Any],
        *,
        execution_authorized: bool = False,
    ) -> ForecastEnvelope:
        normalized_symbol = symbol.strip().upper()
        generated_at = _timestamp(legacy.get("generated_at"), datetime.now(timezone.utc))
        alpha = legacy.get("alpha_prediction") if isinstance(legacy.get("alpha_prediction"), Mapping) else {}
        alpha_stage = str(alpha.get("release_stage") or "rejected").lower()
        alpha_claims_candidate = (
            str(alpha.get("model_family") or "") == "profitability_two_stage"
            and bool(alpha.get("actionable"))
            and str(alpha.get("decision") or "").upper() == "TRADE"
            and alpha_stage == "candidate"
            and str(alpha.get("profitability_gate") or "").upper() == "PASSED"
        )
        alpha_qualified = alpha_claims_candidate and execution_authorized
        feature_evidence = (
            alpha.get("feature_evidence")
            if isinstance(alpha.get("feature_evidence"), Mapping)
            else {}
        )
        price_path = (
            feature_evidence.get("price_path")
            if isinstance(feature_evidence.get("price_path"), Mapping)
            else {}
        )
        if alpha_qualified:
            data_cutoff = _timestamp(price_path.get("last_observed_at"), generated_at)
            horizon = int(alpha.get("horizon_sec"))
        else:
            data_cutoff = _timestamp(
                legacy.get("latest_kline_ts") or legacy.get("data_cutoff") or generated_at,
                generated_at,
            )
            horizon = int(legacy.get("horizon_sec") or MODE_HORIZONS.get(mode, 3600))
        if data_cutoff > generated_at:
            data_cutoff = generated_at
        brain = legacy.get("brain_prediction") if isinstance(legacy.get("brain_prediction"), Mapping) else {}
        brain_direction = str(brain.get("direction") or "flat").lower()
        brain_stage = str(brain.get("release_stage") or "unreviewed").lower()
        # Brain is retained as a comparison baseline only.  Even stale files
        # claiming candidate/live cannot influence the execution forecast.
        brain_qualified = False
        brain_trend = "up" if brain_direction == "long" else "down" if brain_direction == "short" else "flat"
        trend = str(
            str(alpha.get("direction") or "flat").lower()
            if alpha_qualified
            else legacy.get("calibrated_direction") or legacy.get("calibrated_trend") or legacy.get("trend") or "flat"
        ).lower()
        if trend in {"long", "buy"}:
            trend = "up"
        elif trend in {"short", "sell"}:
            trend = "down"
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
        if alpha_qualified:
            raw_probabilities = [
                max(0.0, _float(alpha.get("p_up"), 0.0)),
                max(0.0, _float(alpha.get("p_flat"), 0.0)),
                max(0.0, _float(alpha.get("p_down"), 0.0)),
            ]
            probability_total = sum(raw_probabilities)
            if probability_total <= 0:
                p_up = p_flat = p_down = 1.0 / 3.0
            else:
                p_up, p_flat, p_down = [value / probability_total for value in raw_probabilities]
        else:
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
        if alpha_qualified:
            if alpha.get("expected_net_return_bps") is not None:
                expected_return_bps = _float(alpha.get("expected_net_return_bps"))
            else:
                expected_return_bps = _float(alpha.get("expected_net_return")) * 10_000
        else:
            expected_return_bps = _float(predicted_return) * 10_000 if predicted_return is not None else None
        quantiles = (
            alpha.get("return_quantiles_bps")
            if alpha_qualified
            else legacy.get("return_quantiles_bps")
        )
        if not isinstance(quantiles, Mapping):
            quantiles = None
        source_status = str(
            "ok"
            if alpha_qualified
            else legacy.get("data_source_status") or "degraded"
        ).lower()
        if source_status not in {"ok", "degraded", "missing", "error"}:
            source_status = "degraded"
        reliable = bool(
            alpha_qualified
            or (
                legacy.get("data_source_reliable") is True
                and source_status == "ok"
            )
        )
        quality = (
            1.0
            if alpha_qualified
            else _float(
                (legacy.get("context_completeness") or {}).get("score")
                if isinstance(legacy.get("context_completeness"), dict)
                else legacy.get("context_completeness"),
                0.75 if reliable else 0.5,
            )
        )
        quality = max(0.0, min(1.0, quality))
        warnings = ["legacy_inferred_direction_distribution"]
        if alpha_qualified:
            warnings.append(f"profitability_two_stage_signal:{alpha_stage}")
        elif alpha_claims_candidate:
            warnings.append("profitability_candidate_execution_authorization_denied")
        if brain_stage in {"candidate", "live"}:
            warnings.append("stale_brain_release_ignored")
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
            (
                alpha.get("model_bundle_id")
                if alpha_qualified
                else legacy.get("model_version")
            )
            or f"legacy-{mode}"
        )
        calibration_status = str(
            "valid"
            if alpha_qualified
            else legacy.get("calibration_status") or "unknown"
        ).lower()
        if calibration_status not in {"valid", "degraded", "invalid", "unknown"}:
            calibration_status = "degraded"
        feature_age = (
            price_path.get("age_seconds")
            if alpha_qualified
            else legacy.get("current_price_age_seconds")
        )
        feature_age_sec = int(_float(feature_age, 2_147_483_647)) if feature_age is not None else 2_147_483_647
        range_guard_value = (
            alpha.get("range_guard_score")
            if alpha_qualified
            else (
                legacy.get("range_guard_score")
                if legacy.get("range_guard_score") is not None
                else legacy.get("out_of_distribution_score")
            )
        )
        factor_scores = dict(
            (alpha.get("factor_scores") or {})
            if alpha_qualified
            else (legacy.get("factor_scores") or {})
        )
        return ForecastEnvelope.model_validate(
            {
                "forecast_id": forecast_id,
                "revision": int(legacy.get("revision") or 1),
                "instrument": {
                    "symbol": normalized_symbol,
                    "exchange": (
                        "bybit"
                        if alpha_qualified
                        else str(legacy.get("forecast_market") or "binance").lower()
                    ),
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
                    "return_quantiles_bps": dict(quantiles) if quantiles else None,
                    "expected_return_bps": expected_return_bps,
                    "expected_volatility_bps": None,
                    "expected_mae_bps": (
                        _float(alpha.get("expected_mae_bps")) if alpha_qualified else None
                    ),
                    "expected_mfe_bps": (
                        _float(alpha.get("expected_mfe_bps")) if alpha_qualified else None
                    ),
                },
                "regime": {
                    "market_regime": str(
                        (
                            alpha.get("market_regime")
                            if alpha_qualified
                            else legacy.get("market_regime")
                        )
                        or "unknown"
                    ),
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
                                range_guard_value,
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
                "factor_scores": factor_scores,
                "evidence": {"warnings": warnings},
                "lineage": {
                    "strategy_release_id": str(
                        (
                            alpha.get("strategy_release_id")
                            if alpha_qualified
                            else (
                                f"rejected-alpha-{normalized_symbol}-{mode}"
                                if alpha_claims_candidate
                                else legacy.get("strategy_release_id")
                            )
                        )
                        or f"legacy-{model_bundle}"
                    ),
                    "model_bundle_id": model_bundle,
                    "feature_set_id": str(legacy.get("feature_set_id") or "legacy-feature-set"),
                    "calibration_model_id": str(legacy.get("calibration_model_id") or "legacy-calibration"),
                    "code_commit": str(legacy.get("code_commit") or "unknown"),
                },
            }
        )
