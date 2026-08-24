from __future__ import annotations

import math
from collections import Counter
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


def nested_cv_evidence(
    outer_folds: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    fold_evidence: list[dict[str, object]] = []
    for fold in outer_folds:
        audit = dict(fold.get("nested_selection") or {})
        candidates = list(fold.get("inner_candidate_results") or [])
        inner_checks: list[dict[str, object]] = []
        for candidate in candidates:
            for inner in list(candidate.get("inner_folds") or []):
                train_end = pd.to_datetime(
                    inner.get("train_decision_end"), utc=True, errors="coerce"
                )
                label_max = pd.to_datetime(
                    inner.get("train_label_available_max"),
                    utc=True,
                    errors="coerce",
                )
                validation_start = pd.to_datetime(
                    inner.get("validation_start"), utc=True, errors="coerce"
                )
                purge_sec = int(inner.get("purge_sec", 0))
                timestamps_valid = not any(
                    pd.isna(value)
                    for value in (train_end, label_max, validation_start)
                )
                inner_checks.append(
                    {
                        "config_id": candidate.get("config_id"),
                        "inner_fold": inner.get("fold"),
                        "timestamps_valid": timestamps_valid,
                        "train_label_available_before_validation": bool(
                            timestamps_valid and label_max < validation_start
                        ),
                        "purge_respected": bool(
                            timestamps_valid
                            and validation_start - train_end
                            >= pd.Timedelta(seconds=purge_sec)
                        ),
                        "embargo_sec": int(inner.get("embargo_sec", 0)),
                        "train_rows": int(inner.get("train_rows", 0)),
                        "inner_oos_rows": int(inner.get("inner_oos_rows", 0)),
                    }
                )
        candidates_never_saw_outer_oos = bool(candidates) and all(
            int(candidate.get("outer_oos_rows_seen", -1)) == 0
            for candidate in candidates
        )
        passed = bool(
            fold.get("outer_oos_used_for_tuning") is False
            and audit.get("outer_oos_used_for_selection") is False
            and audit.get("selection_data") == "inner_walk_forward_oos_only"
            and int(audit.get("inner_fold_count", 0)) >= 2
            and candidates_never_saw_outer_oos
            and inner_checks
            and all(
                bool(item["train_label_available_before_validation"])
                and bool(item["purge_respected"])
                and int(item["train_rows"]) > 0
                and int(item["inner_oos_rows"]) > 0
                for item in inner_checks
            )
        )
        fold_evidence.append(
            {
                "horizon_sec": fold.get("horizon_sec"),
                "fold_id": fold.get("fold_id"),
                "outer_train_rows": fold.get("train_rows"),
                "outer_oos_rows": fold.get("test_rows"),
                "outer_oos_used_for_tuning": fold.get(
                    "outer_oos_used_for_tuning"
                ),
                "selection_audit": audit,
                "candidate_count": len(candidates),
                "candidates_never_saw_outer_oos": candidates_never_saw_outer_oos,
                "inner_fold_checks": inner_checks,
                "passed": passed,
            }
        )
    passed = bool(fold_evidence) and all(
        bool(item["passed"]) for item in fold_evidence
    )
    return {
        "schema_version": "profitability-nested-cv.v1",
        "status": "PASSED" if passed else "FAILED",
        "complete": passed,
        "outer_oos_used_for_tuning": False,
        "selection_source": "inner_walk_forward_oos_only",
        "outer_fold_count": len(fold_evidence),
        "failed_outer_fold_count": sum(
            not bool(item["passed"]) for item in fold_evidence
        ),
        "folds": fold_evidence,
    }


def signal_funnel_evidence(
    fold_results: Sequence[Mapping[str, object]],
    report: object,
    *,
    scope: str,
) -> dict[str, object]:
    gate_fields = (
        "paired_action_rows",
        "candidate_decisions",
        "meta_pass_rows",
        "positive_expectancy_lcb_rows",
        "direction_consistent_rows",
        "all_gate_pass_rows",
        "selected_decisions",
    )
    totals = {field: 0 for field in gate_fields}
    folds: list[dict[str, object]] = []
    complete = bool(fold_results)
    for fold in fold_results:
        gate = dict(fold.get("prediction_gate") or {})
        if any(field not in gate for field in gate_fields):
            complete = False
        for field in gate_fields:
            totals[field] += int(gate.get(field, 0))
        folds.append(
            {
                "horizon_sec": fold.get("horizon_sec"),
                "fold_id": fold.get("fold_id"),
                "prediction_gate": gate,
                "signals": int(fold.get("signals", 0)),
                "trades": int(fold.get("trades", 0)),
            }
        )
    signal_count = sum(int(fold.get("signals", 0)) for fold in fold_results)
    trades = tuple(getattr(report, "trades", ()))
    trade_count = len(trades)
    rejected = {
        str(key): int(value)
        for key, value in dict(getattr(report, "rejected_signals", {})).items()
    }
    complete = bool(
        complete
        and totals["candidate_decisions"] > 0
        and totals["paired_action_rows"] >= totals["candidate_decisions"]
        and totals["all_gate_pass_rows"] >= totals["selected_decisions"]
    )
    passed = bool(complete and signal_count > 0 and trade_count > 0)
    return {
        "scope": scope,
        "status": "PASSED" if passed else "FAILED",
        "complete": complete,
        "zero_signal_or_trade_result_accepted": False,
        "totals": {
            **totals,
            "signals": signal_count,
            "executed_trades": trade_count,
            "rejected_signals": sum(rejected.values()),
        },
        "conversion": {
            "candidate_to_selected": (
                totals["selected_decisions"] / totals["candidate_decisions"]
                if totals["candidate_decisions"]
                else 0.0
            ),
            "signal_to_trade": trade_count / signal_count if signal_count else 0.0,
        },
        "rejection_reasons": rejected,
        "folds": folds,
    }


def intratrade_drawdown_evidence(report: object, *, scope: str) -> dict[str, object]:
    curve = tuple(getattr(report, "equity_curve", ()))
    trades = tuple(getattr(report, "trades", ()))
    initial = float(getattr(report, "initial_equity_usdt", 0.0))
    values = [initial, *[float(point.equity_usdt) for point in curve]]
    peak = values[0] if values else 0.0
    recomputed_drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        if peak > 0:
            recomputed_drawdown = max(recomputed_drawdown, (peak - value) / peak)
    reported_drawdown = float(getattr(report, "max_drawdown", math.nan))
    timestamps = [point.observed_at for point in curve]
    timestamps_monotonic = timestamps == sorted(timestamps)
    mae = np.asarray([float(trade.mae) for trade in trades], dtype=float)
    mfe = np.asarray([float(trade.mfe) for trade in trades], dtype=float)
    finite_excursions = bool(
        len(mae) and np.isfinite(mae).all() and np.isfinite(mfe).all()
    )
    drawdown_matches = bool(
        np.isfinite(reported_drawdown)
        and abs(recomputed_drawdown - reported_drawdown) <= 1e-12
    )
    passed = bool(
        trades
        and curve
        and getattr(report, "mark_to_market_used", False)
        and getattr(report, "intrabar_path_used", False)
        and getattr(report, "simulation_complete", False)
        and timestamps_monotonic
        and finite_excursions
        and drawdown_matches
    )
    exit_reasons = Counter(str(trade.exit_reason) for trade in trades)
    return {
        "scope": scope,
        "status": "PASSED" if passed else "FAILED",
        "complete": passed,
        "mark_to_market_used": bool(getattr(report, "mark_to_market_used", False)),
        "intrabar_path_used": bool(getattr(report, "intrabar_path_used", False)),
        "simulation_complete": bool(getattr(report, "simulation_complete", False)),
        "equity_observation_count": len(curve),
        "timestamps_monotonic": timestamps_monotonic,
        "reported_max_drawdown": reported_drawdown,
        "recomputed_max_drawdown": recomputed_drawdown,
        "drawdown_matches_equity_curve": drawdown_matches,
        "maximum_active_positions": max(
            (int(point.active_positions) for point in curve), default=0
        ),
        "maximum_gross_exposure_usdt": max(
            (float(point.gross_exposure_usdt) for point in curve), default=0.0
        ),
        "trade_count": len(trades),
        "trade_mae": {
            "maximum": float(mae.max()) if len(mae) else None,
            "median": float(np.median(mae)) if len(mae) else None,
            "p95": float(np.quantile(mae, 0.95)) if len(mae) else None,
        },
        "trade_mfe": {
            "maximum": float(mfe.max()) if len(mfe) else None,
            "median": float(np.median(mfe)) if len(mfe) else None,
            "p05": float(np.quantile(mfe, 0.05)) if len(mfe) else None,
        },
        "exit_reasons": dict(sorted(exit_reasons.items())),
        "trades": [
            {
                "signal_id": trade.signal_id,
                "symbol": trade.symbol,
                "entry_at": trade.entry_at,
                "exit_at": trade.exit_at,
                "mae": float(trade.mae),
                "mfe": float(trade.mfe),
                "exit_reason": trade.exit_reason,
            }
            for trade in trades
        ],
    }


def production_replay_evidence(
    samples: Sequence[Mapping[str, object]],
    *,
    expected_horizons: Sequence[int],
    expected_symbols: Sequence[str],
) -> dict[str, object]:
    expected_keys = {
        (int(horizon), str(symbol).upper())
        for horizon in expected_horizons
        for symbol in expected_symbols
    }
    observed_keys = [
        (int(sample.get("horizon_sec", -1)), str(sample.get("symbol", "")).upper())
        for sample in samples
    ]
    duplicates = len(observed_keys) - len(set(observed_keys))
    missing = sorted(expected_keys.difference(observed_keys))
    unexpected = sorted(set(observed_keys).difference(expected_keys))
    failed = [sample for sample in samples if not bool(sample.get("passed"))]
    complete = bool(
        not missing
        and not unexpected
        and duplicates == 0
        and len(samples) == len(expected_keys)
    )
    passed = complete and not failed
    return {
        "schema_version": "profitability-production-replay.v1",
        "status": "PASSED" if passed else "FAILED",
        "passed": passed,
        "complete": complete,
        "scope": "development_outer_oos_feature_and_prediction_replay",
        "lockbox_used": False,
        "alternative_models_scored": False,
        "sample_selection": (
            "earliest outer-OOS paired decision per preregistered horizon and symbol; "
            "outcomes never used for selection"
        ),
        "numeric_tolerance": 1e-10,
        "expected_sample_count": len(expected_keys),
        "observed_sample_count": len(samples),
        "failed_sample_count": len(failed),
        "duplicate_sample_key_count": duplicates,
        "missing_sample_keys": [
            {"horizon_sec": horizon, "symbol": symbol}
            for horizon, symbol in missing
        ],
        "unexpected_sample_keys": [
            {"horizon_sec": horizon, "symbol": symbol}
            for horizon, symbol in unexpected
        ],
        "samples": [dict(sample) for sample in samples],
    }


__all__ = (
    "intratrade_drawdown_evidence",
    "nested_cv_evidence",
    "production_replay_evidence",
    "signal_funnel_evidence",
)
