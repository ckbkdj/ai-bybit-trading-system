from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from core.backtest.event_driven import BacktestConfig, EventDrivenBacktest, SignalEvent
from core.evaluation.profitability_gate import (
    ProfitabilityGateResult,
    ProfitabilityThresholds,
    evaluate_development_gate,
    evaluate_profitability_gate,
    write_profitability_report,
)
from core.evaluation.ablation import compare_factor_groups
from core.evaluation.statistical_governance import TrialLedger, TrialRecord
from core.features.profitability_technical import (
    LEGACY_BRAIN_FEATURE_COLUMNS,
    TECHNICAL_FEATURE_COLUMNS,
    engineer_profitability_features,
)
from core.labels.triple_barrier import EntrySpec, MarketBar, TripleBarrierConfig, build_triple_barrier_label
from core.models.two_stage import (
    TwoStageAlphaModel,
    TwoStageConfig,
    TwoStagePrediction,
    prediction_gate_diagnostics,
)
from core.release.profitability_release import create_candidate_manifest
from core.risk.capital_preservation import CapitalPreservationConfig, policy_report
from core.training.pooled_panel import (
    HORIZON_TIMEFRAME,
    HORIZONS_SEC,
    PooledPanelBuilder,
    causal_regime_labels,
    dataset_manifest,
)
from core.training.nested_walk_forward import NestedWalkForwardSelector
from core.training.bybit_pit_panel import BybitPITFeatureSource
from core.training.pit_factor_panel import (
    TRAD_PANEL_FACTOR_GROUPS,
    TradPanelHistorySource,
    TRAD_PANEL_MISSING_REQUIRED_FACTORS,
)


SYMBOLS: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "1000PEPEUSDT")
MINIMUM_COVERAGE_DAYS: Mapping[str, float] = {
    "3m": 180.0,
    "15m": 180.0,
    "2h": 730.0,
    "4h": 730.0,
    "1d": 1095.0,
}
SHORT_FACTOR_GROUPS: Mapping[str, tuple[str, ...]] = {
    "bybit_orderbook": ("bybit_orderbook_delta_l5", "orderbook_spread_bps", "orderbook_depth_usdt_l5", "microprice_deviation_bps"),
    "public_trades": ("public_trade_imbalance_1m", "ofi_1m", "aggressive_cvd_1m"),
    "crypto_derivatives": ("perpetual_basis_bps", "funding_rate", "open_interest_change_1h", "liquidation_imbalance_5m"),
    "execution_quality": ("fill_probability", "expected_slippage_bps"),
}
LEGACY_FACTOR_GROUPS: Mapping[str, tuple[str, ...]] = {
    "legacy_brain_technical": LEGACY_BRAIN_FEATURE_COLUMNS,
}
LONG_FACTOR_GROUPS: Mapping[str, tuple[str, ...]] = {
    "us_risk": ("spy_return", "qqq_return", "vix_level"),
    "rates_usd": ("tlt_return", "real_yield_10y", "uup_return"),
    "commodities": ("gld_return", "uso_return"),
    "healthcare": ("xlv_return", "ibb_return"),
    "china": ("fxi_return", "kweb_return"),
    "crypto_equities": ("coin_return", "mstr_return"),
    "flows": ("crypto_etf_netflow_daily", "stablecoin_exchange_netflow_1h"),
    "macro_vintage": ("fred_vintage_surprise", "alfred_revision_surprise"),
    "tier_a_events": ("tier_a_event_state",),
}


@dataclass(frozen=True)
class ProfitabilityRebuildConfig:
    feature_store_path: Path
    output_dir: Path
    trial_ledger_path: Path
    model_output_dir: Path
    code_commit: str
    trad_panel_root: Path | None = None
    verify_trad_panel_sha256: bool = True
    bybit_pit_store_path: Path | None = None
    max_bars_per_symbol: int = 200_000
    walk_forward_folds: int = 3
    lockbox_fraction: float = 0.15
    random_seed: int = 20260823

    def __post_init__(self) -> None:
        if self.max_bars_per_symbol < 20_000:
            raise ValueError("max_bars_per_symbol is too small for required short-horizon coverage")
        if not 2 <= self.walk_forward_folds <= 8:
            raise ValueError("walk_forward_folds must be between 2 and 8")


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    temporary.replace(path)


def _utc_ms(value: int) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000.0, timezone.utc)


def _hash_payload(payload: object) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _archive_candidate_manifest(output_dir: Path, run_id: str) -> str | None:
    """Preserve, but deactivate, a manifest left by an older successful run."""

    candidate = output_dir / "candidate_release_manifest.json"
    if not candidate.exists():
        return None
    archive_dir = output_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived = archive_dir / f"candidate_release_manifest.{run_id}.json"
    suffix = 1
    while archived.exists():
        archived = archive_dir / f"candidate_release_manifest.{run_id}.{suffix}.json"
        suffix += 1
    candidate.replace(archived)
    return str(archived)


class KlinePanelSource:
    """Read immutable exchange bars without mutating the production feature store."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(self.path)

    def load(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        uri = f"file:{self.path.resolve().as_posix()}?mode=ro"
        query = """
            SELECT symbol,timeframe,source,open_time,close_time,open,high,low,close,volume,fetched_at
              FROM (
                    SELECT symbol,timeframe,source,open_time,close_time,open,high,low,close,volume,fetched_at
                      FROM raw_kline
                     WHERE symbol=? AND timeframe=? AND source='binance'
                     ORDER BY open_time DESC
                     LIMIT ?
              )
             ORDER BY open_time
        """
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            frame = pd.read_sql_query(query, connection, params=(symbol, timeframe, int(limit)))
        if frame.empty:
            raise ValueError(f"no bars for {symbol} {timeframe}")
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(subset=["open", "high", "low", "close", "volume"])
        frame["open_at"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True)
        frame["close_at"] = pd.to_datetime(frame["close_time"], unit="ms", utc=True)
        return frame.reset_index(drop=True)


def validate_source_coverage(frame: pd.DataFrame, timeframe: str) -> dict[str, object]:
    if timeframe not in MINIMUM_COVERAGE_DAYS:
        raise ValueError(f"unsupported coverage timeframe: {timeframe}")
    if frame.empty or "open_at" not in frame or "close_at" not in frame:
        raise ValueError("source coverage requires open_at and close_at")
    first = pd.to_datetime(frame["open_at"], utc=True, errors="coerce").min()
    last = pd.to_datetime(frame["close_at"], utc=True, errors="coerce").max()
    if pd.isna(first) or pd.isna(last) or last <= first:
        raise ValueError("source coverage timestamps are invalid")
    coverage_days = float((last - first).total_seconds() / 86_400.0)
    minimum = float(MINIMUM_COVERAGE_DAYS[timeframe])
    if coverage_days < minimum:
        raise ValueError(
            f"{timeframe} coverage {coverage_days:.2f} days is below required {minimum:.2f} days"
        )
    return {
        "bars": len(frame),
        "start": first.isoformat().replace("+00:00", "Z"),
        "end": last.isoformat().replace("+00:00", "Z"),
        "coverage_days": coverage_days,
        "minimum_coverage_days": minimum,
        "coverage_gate": "PASSED",
    }


_engineer_features = engineer_profitability_features


def _market_bars(frame: pd.DataFrame) -> list[MarketBar]:
    bars: list[MarketBar] = []
    for row in frame.itertuples(index=False):
        range_bps = max(1.0, float((row.high - row.low) / row.close * 10_000.0))
        # These are deliberately conservative OHLCV-derived proxies.  They are
        # never marked as direct execution evidence in the profitability gate.
        spread_bps = min(25.0, max(2.0, range_bps * 0.03))
        depth_usdt = max(1_000.0, float(row.close * row.volume) * 0.02)
        raw_volatility = float(getattr(row, "volatility", 0.0) or 0.0)
        volatility_bps = max(0.0, raw_volatility * 10_000.0) if np.isfinite(raw_volatility) else 0.0
        bars.append(
            MarketBar(
                symbol=str(row.symbol),
                open_time=row.open_at.to_pydatetime(),
                close_time=row.close_at.to_pydatetime(),
                available_at=row.close_at.to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                spread_bps=spread_bps,
                depth_usdt=depth_usdt,
                volatility_bps=volatility_bps,
                funding_bps=0.0,
            )
        )
    return bars


def _panel_rows(frame: pd.DataFrame, horizon_sec: int, bars: Sequence[MarketBar]) -> list[dict[str, object]]:
    features = _engineer_features(frame)
    features["regime"] = causal_regime_labels(
        features.rename(columns={"close_at": "decision_at"})
    ).to_numpy()
    output: list[dict[str, object]] = []
    feature_names = TECHNICAL_FEATURE_COLUMNS + LEGACY_BRAIN_FEATURE_COLUMNS
    next_allowed_decision_at: datetime | None = None
    for index in range(48, len(features) - 1):
        row = features.iloc[index]
        if row[list(feature_names) + ["volatility", "liquidity"]].isna().any():
            continue
        signal_at = row["close_at"].to_pydatetime()
        if next_allowed_decision_at is not None and signal_at < next_allowed_decision_at:
            continue
        reference = float(row["close"])
        volatility_bps = max(20.0, min(300.0, float(row["volatility"]) * 10_000.0))
        stop_bps = max(25.0, volatility_bps * 1.25)
        take_profit_bps = stop_bps * 1.50
        # Include every bar that can participate from entry timeout through the
        # complete holding window.  The bound is time-based, never a fixed bar
        # count, so finer event streams retain their full path without O(n^2)
        # copies of the rest of history.
        max_wait_sec = max(30, min(300, horizon_sec // 2))
        path_end_time = signal_at + timedelta(seconds=max_wait_sec + horizon_sec)
        # Decisions for the same symbol/horizon are scheduled on disjoint
        # maximum execution windows.  BUY/SELL rows below are paired action
        # alternatives at one decision, not independent opportunities.
        next_allowed_decision_at = path_end_time
        path_end = index + 1
        while path_end < len(bars) and bars[path_end].open_time <= path_end_time:
            path_end += 1
        path = bars[index + 1 : path_end]
        labels = {}
        for side in ("BUY", "SELL"):
            labels[side] = build_triple_barrier_label(
                EntrySpec(
                    symbol=str(row["symbol"]),
                    side=side,
                    signal_at=signal_at,
                    reference_price=reference,
                    quantity=max(1e-12, 1_000.0 / reference),
                    take_profit_bps=take_profit_bps,
                    stop_loss_bps=stop_bps,
                    max_holding_sec=horizon_sec,
                    feature_available_at=(signal_at,),
                    max_wait_sec=max_wait_sec,
                ),
                path,
                TripleBarrierConfig(),
            )
        long_net = labels["BUY"].net_return
        short_net = labels["SELL"].net_return
        if max(long_net, short_net) <= 0:
            direction_label = "flat"
        else:
            direction_label = "up" if long_net >= short_net else "down"
        hour = signal_at.hour
        session = "asia" if hour < 8 else "europe" if hour < 16 else "americas"
        regime = str(row["regime"])
        for side, label in labels.items():
            if label.label_available_at <= signal_at:
                raise ValueError("label PIT invariant failed")
            payload = {
                "symbol": str(row["symbol"]),
                "horizon_sec": horizon_sec,
                "side": side,
                "decision_at": signal_at,
                "available_at": signal_at,
                "label_available_at": label.label_available_at,
                "reference_price": reference,
                "liquidity": float(row["liquidity"]),
                "volatility": float(row["volatility"]),
                "session": session,
                "regime": regime,
                "direction_label": direction_label,
                "gross_return": label.gross_return,
                "net_return": label.net_return,
                "fee_return": label.fee_return,
                "slippage_return": label.slippage_return,
                "funding_return": label.funding_return,
                "mae": label.mae,
                "mfe": label.mfe,
                "fill_probability": label.fill_probability,
                "fill_fraction": label.fill_fraction,
                "partial_fill": label.partial_fill,
                "exit_reason": label.exit_reason,
                "path_observations": label.path_observations,
                "stop_loss_bps": stop_bps,
                "take_profit_bps": take_profit_bps,
            }
            payload.update({name: float(row[name]) for name in feature_names})
            output.append(payload)
    return output


FEATURE_COLUMNS: tuple[str, ...] = (
    "symbol", "horizon_sec", "side", "liquidity", "volatility", "session", "regime",
    "ret_1", "ret_2", "ret_3", "ret_6", "ret_12", "ret_24",
    "range_pct", "body_pct", "volume_zscore", "momentum_vol_ratio", "ma_gap_8_24",
)


def _signals_from_predictions(
    frame: pd.DataFrame,
    predictions: Sequence[TwoStagePrediction],
    horizon_sec: int,
) -> list[SignalEvent]:
    if len(frame) != len(predictions):
        raise ValueError("prediction and frame lengths differ")
    candidates = frame.copy().reset_index(drop=True)
    candidates["_prediction"] = list(predictions)
    signals: list[SignalEvent] = []
    for (symbol, decision_at), group in candidates.groupby(["symbol", "decision_at"], sort=True):
        qualified: list[tuple[float, pd.Series, TwoStagePrediction]] = []
        for _, row in group.iterrows():
            prediction = row["_prediction"]
            direction_ok = (
                row["side"] == "BUY" and prediction.p_up >= prediction.p_down
            ) or (
                row["side"] == "SELL" and prediction.p_down >= prediction.p_up
            )
            if prediction.decision == "TRADE" and direction_ok and prediction.lower_bound_net_edge > 0:
                qualified.append((prediction.meta_trade_probability, row, prediction))
        if not qualified:
            continue
        _, row, prediction = max(qualified, key=lambda item: (item[0], item[2].lower_bound_net_edge))
        token = hashlib.sha256(
            f"{symbol}|{decision_at}|{horizon_sec}|{row['side']}".encode()
        ).hexdigest()[:20]
        signals.append(
            SignalEvent(
                signal_id=f"alpha_{token}",
                symbol=str(symbol),
                side=str(row["side"]),
                decision_at=pd.Timestamp(decision_at).to_pydatetime(),
                reference_price=float(row["reference_price"]),
                lower_bound_net_edge=float(prediction.lower_bound_net_edge),
                take_profit_bps=float(row["take_profit_bps"]),
                stop_loss_bps=float(row["stop_loss_bps"]),
                max_holding_sec=horizon_sec,
                feature_available_at=(pd.Timestamp(row["available_at"]).to_pydatetime(),),
                max_wait_sec=max(30, min(300, horizon_sec // 2)),
                regime=str(row["regime"]),
                market_key=f"{symbol}:{horizon_sec}",
            )
        )
    return signals


def _factor_ablation_report(
    evaluated_groups: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    evaluated = dict(evaluated_groups or {})
    groups = []
    for cadence, definitions in (
        ("all_horizons", LEGACY_FACTOR_GROUPS),
        ("short", SHORT_FACTOR_GROUPS),
        ("medium_long", LONG_FACTOR_GROUPS),
    ):
        for group, factors in definitions.items():
            if group in evaluated:
                groups.append(dict(evaluated[group]))
                continue
            groups.append(
                {
                    "cadence": cadence,
                    "factor_group": group,
                    "factors": list(factors),
                    "oos_ablation_status": "FAILED_DATA_UNAVAILABLE",
                    "evaluated": False,
                    "pit_observation_count": 0,
                    "oos_fold_count": 0,
                    "retained": False,
                    "formal_feature_set": False,
                }
            )
    return {
        "method": "identical purged walk-forward folds; fee-adjusted event-driven net return",
        "baseline": "price_technical_only",
        "all_required_groups_evaluated": all(
            item.get("oos_ablation_status") == "EVALUATED_OOS" for item in groups
        ),
        "retained_factor_groups": [
            str(item["factor_group"]) for item in groups if bool(item.get("retained"))
        ],
        "groups": groups,
        "blocker": "current local store has OHLCV and snapshots, but not complete PIT histories for required orderbook/trades/macro-vintage groups",
    }


def _evaluate_legacy_technical_ablation(
    datasets: Mapping[int, object],
    market: Mapping[str, Sequence[MarketBar]],
    selector: NestedWalkForwardSelector,
    backtest: EventDrivenBacktest,
) -> dict[str, dict[str, object]]:
    """Evaluate reusable legacy Brain indicators on untouched outer folds."""

    group = "legacy_brain_technical"
    columns = LEGACY_FACTOR_GROUPS[group]
    baseline_folds: list[dict[str, float]] = []
    augmented_folds: list[dict[str, float]] = []
    fold_evidence: list[dict[str, object]] = []
    for horizon, dataset in datasets.items():
        for fold in dataset.folds:
            train = dataset.development.iloc[list(fold.train_indices)]
            test = dataset.development.iloc[list(fold.test_indices)]
            eligible_train = train.loc[
                train[list(columns)].notna().all(axis=1)
            ].reset_index(drop=True)
            eligible_test = test.loc[
                test[list(columns)].notna().all(axis=1)
            ].reset_index(drop=True)
            if len(eligible_train) < 100 or len(eligible_test) < 30:
                fold_evidence.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "status": "FAILED_INSUFFICIENT_PIT_ROWS",
                        "train_rows": len(eligible_train),
                        "test_rows": len(eligible_test),
                    }
                )
                continue
            baseline_selection = selector.select_and_fit(
                eligible_train, FEATURE_COLUMNS
            )
            augmented_selection = selector.select_and_fit(
                eligible_train, FEATURE_COLUMNS + tuple(columns)
            )
            baseline_predictions = baseline_selection.model.predict(eligible_test)
            augmented_predictions = augmented_selection.model.predict(eligible_test)
            baseline_signals = _signals_from_predictions(
                eligible_test, baseline_predictions, horizon
            )
            augmented_signals = _signals_from_predictions(
                eligible_test, augmented_predictions, horizon
            )
            baseline_report = backtest.run(baseline_signals, market)
            augmented_report = backtest.run(augmented_signals, market)
            baseline_folds.append({"net_return": baseline_report.net_return})
            augmented_folds.append({"net_return": augmented_report.net_return})
            fold_evidence.append(
                {
                    "horizon_sec": horizon,
                    "fold_id": fold.fold_id,
                    "status": "EVALUATED_OOS",
                    "train_rows": len(eligible_train),
                    "test_rows": len(eligible_test),
                    "baseline_signals": len(baseline_signals),
                    "baseline_trades": len(baseline_report.trades),
                    "baseline_net_return": baseline_report.net_return,
                    "baseline_prediction_gate": prediction_gate_diagnostics(
                        eligible_test,
                        baseline_predictions,
                        meta_threshold=baseline_selection.selected_config.meta_trade_probability,
                    ),
                    "augmented_signals": len(augmented_signals),
                    "augmented_trades": len(augmented_report.trades),
                    "augmented_net_return": augmented_report.net_return,
                    "augmented_prediction_gate": prediction_gate_diagnostics(
                        eligible_test,
                        augmented_predictions,
                        meta_threshold=augmented_selection.selected_config.meta_trade_probability,
                    ),
                }
            )
    if len(baseline_folds) < 2:
        return {
            group: {
                "cadence": "all_horizons",
                "factor_group": group,
                "factors": list(columns),
                "oos_ablation_status": "FAILED_INSUFFICIENT_PIT_ROWS",
                "evaluated": False,
                "pit_observation_count": sum(
                    int(item.get("test_rows", 0)) for item in fold_evidence
                ),
                "oos_fold_count": len(baseline_folds),
                "retained": False,
                "formal_feature_set": False,
                "folds": fold_evidence,
            }
        }
    comparison = compare_factor_groups(
        baseline_folds,
        {group: augmented_folds},
        primary_metric="net_return",
        higher_is_better=True,
        minimum_mean_improvement=0.0,
        minimum_improved_fold_ratio=0.60,
        minimum_worst_fold_improvement=-0.002,
    )[0]
    return {
        group: {
            "cadence": "all_horizons",
            "factor_group": group,
            "factors": list(columns),
            "source": "legacy Brain causal OHLCV logic without current-snapshot broadcast",
            "oos_ablation_status": "EVALUATED_OOS",
            "evaluated": True,
            "pit_observation_count": sum(
                int(item.get("test_rows", 0)) for item in fold_evidence
            ),
            "oos_fold_count": len(baseline_folds),
            "metric": comparison.metric,
            "baseline_mean": comparison.baseline_mean,
            "augmented_mean": comparison.augmented_mean,
            "mean_improvement": comparison.mean_improvement,
            "improved_fold_ratio": comparison.improved_fold_ratio,
            "worst_fold_improvement": comparison.worst_fold_improvement,
            "retained": comparison.retained,
            "formal_feature_set": comparison.retained,
            "folds": fold_evidence,
        }
    }


def _evaluate_trad_panel_ablation(
    datasets: Mapping[int, object],
    market: Mapping[str, Sequence[MarketBar]],
    selector: NestedWalkForwardSelector,
    backtest: EventDrivenBacktest,
) -> dict[str, dict[str, object]]:
    results: dict[str, dict[str, object]] = {}
    for group, columns in TRAD_PANEL_FACTOR_GROUPS.items():
        baseline_folds: list[dict[str, float]] = []
        augmented_folds: list[dict[str, float]] = []
        fold_evidence: list[dict[str, object]] = []
        for horizon, dataset in datasets.items():
            if horizon < 7200:
                continue
            for fold in dataset.folds:
                train = dataset.development.iloc[list(fold.train_indices)]
                test = dataset.development.iloc[list(fold.test_indices)]
                train_mask = train[list(columns)].notna().all(axis=1)
                test_mask = test[list(columns)].notna().all(axis=1)
                eligible_train = train.loc[train_mask].reset_index(drop=True)
                eligible_test = test.loc[test_mask].reset_index(drop=True)
                if len(eligible_train) < 100 or len(eligible_test) < 30:
                    fold_evidence.append(
                        {
                            "horizon_sec": horizon,
                            "fold_id": fold.fold_id,
                            "status": "FAILED_INSUFFICIENT_PIT_ROWS",
                            "train_rows": len(eligible_train),
                            "test_rows": len(eligible_test),
                        }
                    )
                    continue
                baseline_selection = selector.select_and_fit(
                    eligible_train, FEATURE_COLUMNS
                )
                augmented_selection = selector.select_and_fit(
                    eligible_train, FEATURE_COLUMNS + tuple(columns)
                )
                baseline_predictions = baseline_selection.model.predict(eligible_test)
                augmented_predictions = augmented_selection.model.predict(eligible_test)
                baseline_signals = _signals_from_predictions(
                    eligible_test, baseline_predictions, horizon
                )
                augmented_signals = _signals_from_predictions(
                    eligible_test, augmented_predictions, horizon
                )
                baseline_report = backtest.run(baseline_signals, market)
                augmented_report = backtest.run(augmented_signals, market)
                baseline_gate_diagnostics = prediction_gate_diagnostics(
                    eligible_test,
                    baseline_predictions,
                    meta_threshold=baseline_selection.selected_config.meta_trade_probability,
                )
                augmented_gate_diagnostics = prediction_gate_diagnostics(
                    eligible_test,
                    augmented_predictions,
                    meta_threshold=augmented_selection.selected_config.meta_trade_probability,
                )
                baseline_folds.append({"net_return": baseline_report.net_return})
                augmented_folds.append({"net_return": augmented_report.net_return})
                fold_evidence.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "status": "EVALUATED_OOS",
                        "train_rows": len(eligible_train),
                        "test_rows": len(eligible_test),
                        "baseline_signals": len(baseline_signals),
                        "baseline_trades": len(baseline_report.trades),
                        "baseline_net_return": baseline_report.net_return,
                        "baseline_prediction_gate": baseline_gate_diagnostics,
                        "augmented_signals": len(augmented_signals),
                        "augmented_trades": len(augmented_report.trades),
                        "augmented_net_return": augmented_report.net_return,
                        "augmented_prediction_gate": augmented_gate_diagnostics,
                    }
                )
        if len(baseline_folds) < 2:
            results[group] = {
                "cadence": "medium_long",
                "factor_group": group,
                "factors": list(columns),
                "oos_ablation_status": "FAILED_INSUFFICIENT_PIT_ROWS",
                "evaluated": False,
                "pit_observation_count": sum(
                    int(item.get("test_rows", 0)) for item in fold_evidence
                ),
                "oos_fold_count": len(baseline_folds),
                "retained": False,
                "formal_feature_set": False,
                "folds": fold_evidence,
            }
            continue
        comparison = compare_factor_groups(
            baseline_folds,
            {group: augmented_folds},
            primary_metric="net_return",
            higher_is_better=True,
            minimum_mean_improvement=0.0,
            minimum_improved_fold_ratio=0.60,
            minimum_worst_fold_improvement=-0.002,
        )[0]
        missing_required = TRAD_PANEL_MISSING_REQUIRED_FACTORS.get(group, ())
        complete_group = not missing_required
        results[group] = {
            "cadence": "medium_long",
            "factor_group": group,
            "factors": list(columns + tuple(missing_required)),
            "evaluated_factors": list(columns),
            "missing_required_factors": list(missing_required),
            "oos_ablation_status": (
                "EVALUATED_OOS"
                if complete_group
                else "EVALUATED_PARTIAL_OOS_MISSING_REQUIRED_FACTORS"
            ),
            "evaluated": True,
            "pit_observation_count": sum(
                int(item.get("test_rows", 0)) for item in fold_evidence
            ),
            "oos_fold_count": len(baseline_folds),
            "metric": comparison.metric,
            "baseline_mean": comparison.baseline_mean,
            "augmented_mean": comparison.augmented_mean,
            "mean_improvement": comparison.mean_improvement,
            "improved_fold_ratio": comparison.improved_fold_ratio,
            "worst_fold_improvement": comparison.worst_fold_improvement,
            "measured_subset_would_retain": comparison.retained,
            "retained": bool(complete_group and comparison.retained),
            "formal_feature_set": bool(complete_group and comparison.retained),
            "folds": fold_evidence,
        }
    return results


def _evaluate_bybit_pit_ablation(
    datasets: Mapping[int, object],
    market: Mapping[str, Sequence[MarketBar]],
    selector: NestedWalkForwardSelector,
    backtest: EventDrivenBacktest,
    source_evidence: Mapping[str, object],
    *,
    minimum_history_days: float = 30.0,
) -> dict[str, dict[str, object]]:
    """Ablate real short-horizon features only after sufficient PIT history exists."""

    results: dict[str, dict[str, object]] = {}
    coverage = dict(source_evidence.get("feature_coverage") or {})
    for group, columns in SHORT_FACTOR_GROUPS.items():
        required_coverage = [
            coverage.get(f"{symbol}:{column}")
            for symbol in SYMBOLS
            for column in columns
        ]
        missing_contracts = [
            f"{symbol}:{column}"
            for symbol in SYMBOLS
            for column in columns
            if coverage.get(f"{symbol}:{column}") is None
        ]
        observed_days = [
            float(item.get("coverage_days", 0.0))
            for item in required_coverage
            if isinstance(item, Mapping)
        ]
        minimum_observed_days = min(observed_days) if observed_days else 0.0
        if missing_contracts or minimum_observed_days < minimum_history_days:
            results[group] = {
                "cadence": "short",
                "factor_group": group,
                "factors": list(columns),
                "oos_ablation_status": "COLLECTING_INSUFFICIENT_PIT_HISTORY",
                "evaluated": False,
                "pit_observation_count": sum(
                    int(item.get("observations", 0))
                    for item in required_coverage
                    if isinstance(item, Mapping)
                ),
                "oos_fold_count": 0,
                "retained": False,
                "formal_feature_set": False,
                "minimum_required_history_days": minimum_history_days,
                "minimum_observed_history_days": minimum_observed_days,
                "missing_symbol_feature_contracts": missing_contracts,
            }
            continue

        baseline_folds: list[dict[str, float]] = []
        augmented_folds: list[dict[str, float]] = []
        fold_evidence: list[dict[str, object]] = []
        for horizon, dataset in datasets.items():
            if horizon not in {180, 900}:
                continue
            for fold in dataset.folds:
                train = dataset.development.iloc[list(fold.train_indices)]
                test = dataset.development.iloc[list(fold.test_indices)]
                train_mask = train[list(columns)].notna().all(axis=1)
                test_mask = test[list(columns)].notna().all(axis=1)
                eligible_train = train.loc[train_mask].reset_index(drop=True)
                eligible_test = test.loc[test_mask].reset_index(drop=True)
                if len(eligible_train) < 1_000 or len(eligible_test) < 200:
                    fold_evidence.append(
                        {
                            "horizon_sec": horizon,
                            "fold_id": fold.fold_id,
                            "status": "FAILED_INSUFFICIENT_PIT_ROWS",
                            "train_rows": len(eligible_train),
                            "test_rows": len(eligible_test),
                        }
                    )
                    continue
                baseline_selection = selector.select_and_fit(
                    eligible_train, FEATURE_COLUMNS
                )
                augmented_selection = selector.select_and_fit(
                    eligible_train, FEATURE_COLUMNS + tuple(columns)
                )
                baseline_predictions = baseline_selection.model.predict(eligible_test)
                augmented_predictions = augmented_selection.model.predict(eligible_test)
                baseline_signals = _signals_from_predictions(
                    eligible_test, baseline_predictions, horizon
                )
                augmented_signals = _signals_from_predictions(
                    eligible_test, augmented_predictions, horizon
                )
                baseline_report = backtest.run(baseline_signals, market)
                augmented_report = backtest.run(augmented_signals, market)
                baseline_folds.append({"net_return": baseline_report.net_return})
                augmented_folds.append({"net_return": augmented_report.net_return})
                fold_evidence.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "status": "EVALUATED_OOS",
                        "train_rows": len(eligible_train),
                        "test_rows": len(eligible_test),
                        "baseline_signals": len(baseline_signals),
                        "baseline_trades": len(baseline_report.trades),
                        "baseline_net_return": baseline_report.net_return,
                        "baseline_prediction_gate": prediction_gate_diagnostics(
                            eligible_test,
                            baseline_predictions,
                            meta_threshold=baseline_selection.selected_config.meta_trade_probability,
                        ),
                        "augmented_signals": len(augmented_signals),
                        "augmented_trades": len(augmented_report.trades),
                        "augmented_net_return": augmented_report.net_return,
                        "augmented_prediction_gate": prediction_gate_diagnostics(
                            eligible_test,
                            augmented_predictions,
                            meta_threshold=augmented_selection.selected_config.meta_trade_probability,
                        ),
                    }
                )
        if len(baseline_folds) < 2:
            results[group] = {
                "cadence": "short",
                "factor_group": group,
                "factors": list(columns),
                "oos_ablation_status": "FAILED_INSUFFICIENT_PIT_ROWS",
                "evaluated": False,
                "pit_observation_count": sum(
                    int(item.get("test_rows", 0)) for item in fold_evidence
                ),
                "oos_fold_count": len(baseline_folds),
                "retained": False,
                "formal_feature_set": False,
                "folds": fold_evidence,
            }
            continue
        comparison = compare_factor_groups(
            baseline_folds,
            {group: augmented_folds},
            primary_metric="net_return",
            higher_is_better=True,
            minimum_mean_improvement=0.0,
            minimum_improved_fold_ratio=0.60,
            minimum_worst_fold_improvement=-0.002,
        )[0]
        results[group] = {
            "cadence": "short",
            "factor_group": group,
            "factors": list(columns),
            "oos_ablation_status": "EVALUATED_OOS",
            "evaluated": True,
            "pit_observation_count": sum(
                int(item.get("test_rows", 0)) for item in fold_evidence
            ),
            "oos_fold_count": len(baseline_folds),
            "metric": comparison.metric,
            "baseline_mean": comparison.baseline_mean,
            "augmented_mean": comparison.augmented_mean,
            "mean_improvement": comparison.mean_improvement,
            "improved_fold_ratio": comparison.improved_fold_ratio,
            "worst_fold_improvement": comparison.worst_fold_improvement,
            "retained": comparison.retained,
            "formal_feature_set": comparison.retained,
            "folds": fold_evidence,
        }
    return results


class ProfitabilityRebuild:
    def __init__(self, config: ProfitabilityRebuildConfig) -> None:
        self.config = config
        self.source = KlinePanelSource(config.feature_store_path)
        self.ledger = TrialLedger(config.trial_ledger_path)
        self.bybit_pit_snapshot_maximum_sequence = None
        if config.bybit_pit_store_path is not None:
            self.bybit_pit_snapshot_maximum_sequence = BybitPITFeatureSource(
                config.bybit_pit_store_path
            ).maximum_sequence()
        feature_store_stat = config.feature_store_path.stat()
        run_payload = {
            "code_commit": config.code_commit,
            "feature_store": str(config.feature_store_path.resolve()),
            "feature_store_size_bytes": feature_store_stat.st_size,
            "feature_store_modified_ns": feature_store_stat.st_mtime_ns,
            "trad_panel_root": (
                str(config.trad_panel_root.resolve()) if config.trad_panel_root else None
            ),
            "verify_trad_panel_sha256": config.verify_trad_panel_sha256,
            "bybit_pit_store": (
                str(config.bybit_pit_store_path.resolve())
                if config.bybit_pit_store_path
                else None
            ),
            "bybit_pit_snapshot_maximum_sequence": self.bybit_pit_snapshot_maximum_sequence,
            "max_bars_per_symbol": config.max_bars_per_symbol,
            "horizons": HORIZONS_SEC,
            "symbols": SYMBOLS,
        }
        self.trial_id = f"profitability_{_hash_payload(run_payload)[:24]}"

    def run(self) -> ProfitabilityGateResult:
        self.ledger.append_event(self.trial_id, "running", {"phase": "load_and_label"})
        panels: dict[int, pd.DataFrame] = {}
        market: dict[str, Sequence[MarketBar]] = {}
        source_evidence: dict[str, object] = {}
        for horizon in HORIZONS_SEC:
            timeframe = HORIZON_TIMEFRAME[horizon]
            rows: list[dict[str, object]] = []
            for symbol in SYMBOLS:
                frame = self.source.load(symbol, timeframe, self.config.max_bars_per_symbol)
                coverage = validate_source_coverage(frame, timeframe)
                enriched = _engineer_features(frame)
                bars = _market_bars(enriched)
                market[f"{symbol}:{horizon}"] = bars
                rows.extend(_panel_rows(enriched, horizon, bars))
                source_evidence[f"{symbol}:{horizon}"] = {
                    "timeframe": timeframe,
                    "decision_sampling": "non_overlapping_max_execution_windows",
                    "paired_side_alternatives": True,
                    **coverage,
                }
            panels[horizon] = pd.DataFrame(rows)

        trad_panel_evidence: dict[str, object] | None = None
        if self.config.trad_panel_root is not None:
            trad_source = TradPanelHistorySource(
                self.config.trad_panel_root,
                verify_sha256=self.config.verify_trad_panel_sha256,
            )
            trad_history, trad_panel_evidence = trad_source.load()
            for horizon in HORIZONS_SEC:
                panels[horizon] = trad_source.join(
                    panels[horizon], history=trad_history
                )
            source_evidence["trad_data_service"] = trad_panel_evidence

        bybit_pit_evidence: dict[str, object] | None = None
        if self.config.bybit_pit_store_path is not None:
            bybit_names = tuple(
                dict.fromkeys(
                    name
                    for columns in SHORT_FACTOR_GROUPS.values()
                    for name in columns
                )
            )
            bybit_source = BybitPITFeatureSource(self.config.bybit_pit_store_path)
            bybit_history, bybit_pit_evidence = bybit_source.load(
                bybit_names,
                maximum_sequence=self.bybit_pit_snapshot_maximum_sequence,
            )
            for horizon in (180, 900):
                panels[horizon] = bybit_source.join(
                    panels[horizon], names=bybit_names, history=bybit_history
                )
            source_evidence["bybit_public_pit"] = bybit_pit_evidence

        splitter = PooledPanelBuilder(
            lockbox_fraction=self.config.lockbox_fraction,
            minimum_train_rows=300,
            minimum_test_rows=80,
            maximum_folds=self.config.walk_forward_folds,
        )
        datasets = splitter.build(panels)
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "labels_and_pooled_panels_ready",
                "datasets": {
                    str(horizon): dataset_manifest(dataset)
                    for horizon, dataset in datasets.items()
                },
            },
        )
        walk_forward: list[dict[str, object]] = []
        development_signals: list[SignalEvent] = []
        candidate_configs = (
            TwoStageConfig(
                direction_iterations=80,
                meta_iterations=80,
                learning_rate=0.03,
                l2=0.03,
                ridge=1.0,
                tail_penalty=0.75,
                meta_trade_probability=0.58,
            ),
            TwoStageConfig(
                direction_iterations=80,
                meta_iterations=80,
                learning_rate=0.03,
                l2=0.03,
                ridge=2.0,
                tail_penalty=0.75,
                meta_trade_probability=0.62,
            ),
        )
        selector = NestedWalkForwardSelector(candidate_configs, inner_folds=3)
        backtest = EventDrivenBacktest(BacktestConfig())
        evaluated_factor_groups = _evaluate_legacy_technical_ablation(
            datasets, market, selector, backtest
        )
        if trad_panel_evidence is not None:
            evaluated_factor_groups.update(
                _evaluate_trad_panel_ablation(datasets, market, selector, backtest)
            )
        if bybit_pit_evidence is not None:
            evaluated_factor_groups.update(
                _evaluate_bybit_pit_ablation(
                    datasets,
                    market,
                    selector,
                    backtest,
                    bybit_pit_evidence,
                )
            )
        factor_report = _factor_ablation_report(evaluated_factor_groups)
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "factor_ablation_ready",
                "all_required_groups_evaluated": factor_report[
                    "all_required_groups_evaluated"
                ],
                "retained_factor_groups": factor_report[
                    "retained_factor_groups"
                ],
            },
        )
        retained_groups = tuple(
            str(group) for group in factor_report["retained_factor_groups"]
        )
        model_feature_columns_by_horizon: dict[int, tuple[str, ...]] = {}
        for horizon in HORIZONS_SEC:
            retained_factor_columns: list[str] = []
            for group in retained_groups:
                if group in LEGACY_FACTOR_GROUPS:
                    retained_factor_columns.extend(LEGACY_FACTOR_GROUPS[group])
                if horizon in {180, 900} and group in SHORT_FACTOR_GROUPS:
                    retained_factor_columns.extend(SHORT_FACTOR_GROUPS[group])
                if horizon >= 7200 and group in TRAD_PANEL_FACTOR_GROUPS:
                    retained_factor_columns.extend(TRAD_PANEL_FACTOR_GROUPS[group])
            model_feature_columns_by_horizon[horizon] = FEATURE_COLUMNS + tuple(
                dict.fromkeys(retained_factor_columns)
            )
        for horizon, dataset in datasets.items():
            model_feature_columns = model_feature_columns_by_horizon[horizon]
            for fold in dataset.folds:
                train = dataset.development.iloc[list(fold.train_indices)]
                test = dataset.development.iloc[list(fold.test_indices)]
                selection = selector.select_and_fit(train, model_feature_columns)
                predictions = selection.model.predict(test)
                signals = _signals_from_predictions(test, predictions, horizon)
                prediction_gate = prediction_gate_diagnostics(
                    test,
                    predictions,
                    meta_threshold=selection.selected_config.meta_trade_probability,
                )
                development_signals.extend(signals)
                report = backtest.run(signals, market)
                self.ledger.append_event(
                    self.trial_id,
                    "running",
                    {
                        "phase": "outer_walk_forward_fold_scored",
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "signals": len(signals),
                        "trades": len(report.trades),
                        "net_return": report.net_return,
                    },
                )
                walk_forward.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "train_rows": len(train),
                        "test_rows": len(test),
                        "signals": len(signals),
                        "prediction_gate": prediction_gate,
                        "trades": len(report.trades),
                        "net_return": report.net_return,
                        "max_drawdown": report.max_drawdown,
                        "profit_factor": report.profit_factor,
                        "purge_sec": fold.purge_sec,
                        "embargo_sec": fold.embargo_sec,
                        "nested_selection": dict(selection.audit),
                        "inner_candidate_results": list(selection.candidate_results),
                        "outer_oos_used_for_tuning": False,
                        "formal_feature_columns": list(model_feature_columns),
                    }
                )

        development_report = backtest.run(development_signals, market)
        development_stress = backtest.run(
            development_signals, market, cost_multiplier=2.0
        )
        execution_evidence_complete = False
        development_gate = evaluate_development_gate(
            development_report.trades,
            walk_forward,
            initial_equity_usdt=development_report.initial_equity_usdt,
            two_x_cost_net_return=development_stress.net_return,
            mark_to_market_max_drawdown=development_report.max_drawdown,
            mark_to_market_evidence_complete=development_report.mark_to_market_used,
            execution_evidence_complete=execution_evidence_complete,
            factor_ablation_complete=bool(factor_report["all_required_groups_evaluated"]),
            thresholds=ProfitabilityThresholds(),
        )
        output = self.config.output_dir
        _archive_candidate_manifest(output, self.trial_id)
        write_profitability_report(output / "profitability_report.json", development_gate)
        _atomic_json(
            output / "walk_forward_report.json",
            {
                "trial_id": self.trial_id,
                "method": "nested pooled-panel walk-forward; inner OOS selects parameters, outer OOS scores once",
                "outer_oos_used_for_tuning": False,
                "folds": walk_forward,
                "positive_fold_ratio": development_gate.metrics[
                    "positive_walk_forward_fold_ratio"
                ],
                "development_portfolio": development_report.to_dict(include_trades=True),
                "datasets": {str(h): dataset_manifest(ds) for h, ds in datasets.items()},
            },
        )
        _atomic_json(output / "factor_ablation_report.json", factor_report)
        _atomic_json(
            output / "execution_cost_report.json",
            {
                "evaluation_scope": "development_oos",
                "execution_evidence_complete": execution_evidence_complete,
                "normal_cost": development_report.to_dict(include_trades=False),
                "two_x_cost": development_stress.to_dict(include_trades=False),
                "limitations": [
                    "historical spread/depth/fill inputs are conservative OHLCV-derived proxies",
                    "PIT Bybit orderbook and public-trade histories are not present in the current store",
                    "therefore execution evidence cannot authorize opening a new lockbox",
                ],
            },
        )
        _atomic_json(
            output / "capital_preservation_report.json",
            policy_report(CapitalPreservationConfig()),
        )

        # A rejected model is still useful for auditable shadow collection.
        # It is fitted on development only and cannot promote itself.  Saving
        # it here does not inspect or consume the sealed lockbox.
        model_dir = self.config.model_output_dir / self.trial_id
        model_paths: dict[str, str] = {}
        model_sha256: dict[str, str] = {}
        final_selection: dict[str, object] = {}
        final_models: dict[int, TwoStageAlphaModel] = {}
        for horizon, dataset in datasets.items():
            model_feature_columns = model_feature_columns_by_horizon[horizon]
            selection = selector.select_and_fit(
                dataset.development, model_feature_columns
            )
            path = model_dir / f"horizon_{horizon}.json"
            selection.model.save(path)
            final_models[horizon] = selection.model
            model_paths[str(horizon)] = path.name
            model_sha256[str(horizon)] = _sha256_file(path)
            final_selection[str(horizon)] = {
                "audit": dict(selection.audit),
                "candidate_results": list(selection.candidate_results),
            }
        bundle_path = model_dir / "model_bundle.json"
        rejected_bundle = {
            "schema_version": "profitability-model-bundle.v2",
            "trial_id": self.trial_id,
            "model_family": "profitability_two_stage",
            "release_stage": "rejected",
            "profitability_gate": "FAILED",
            "models": model_paths,
            "model_sha256": model_sha256,
            "formal_feature_columns": {
                str(horizon): list(columns)
                for horizon, columns in model_feature_columns_by_horizon.items()
            },
            "retained_factor_groups": list(retained_groups),
            "lockbox_fingerprint": None,
            "lockbox_consumed": False,
            "code_commit": self.config.code_commit,
        }
        _atomic_json(bundle_path, rejected_bundle)

        if not development_gate.passed:
            _atomic_json(
                output / "lockbox_report.json",
                {
                    "trial_id": self.trial_id,
                    "status": "SEALED_NOT_OPENED",
                    "lockbox_evaluated": False,
                    "used_for_parameter_selection": False,
                    "reason": "development profitability, factor, or execution gate failed",
                    "source_evidence": source_evidence,
                    "rejected_shadow_model_bundle": str(bundle_path),
                },
            )
            record = TrialRecord(
                trial_id=self.trial_id,
                model_family="profitability_two_stage",
                data_signature=_hash_payload(source_evidence)[:24],
                parameter_hash=TrialLedger.parameter_hash(
                    {
                        "candidate_configs": [asdict(config) for config in candidate_configs],
                        "features_by_horizon": model_feature_columns_by_horizon,
                        "nested_walk_forward": True,
                    }
                ),
                code_commit=self.config.code_commit,
                status="rejected",
                metrics=development_gate.to_dict(),
            )
            self.ledger.append(record)
            self.ledger.append_event(self.trial_id, "rejected", development_gate.to_dict())
            return development_gate

        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "open_new_lockbox_after_development_pass",
                "walk_forward_folds": len(walk_forward),
            },
        )
        lockbox_fingerprint = _hash_payload(
            {horizon: dataset.lockbox_fingerprint for horizon, dataset in datasets.items()}
        )
        self.ledger.claim_lockbox(
            lockbox_fingerprint, self.trial_id, purpose="final_evaluation"
        )
        lockbox_signals: list[SignalEvent] = []
        for horizon, dataset in datasets.items():
            lockbox_signals.extend(
                _signals_from_predictions(
                    dataset.lockbox,
                    final_models[horizon].predict(dataset.lockbox),
                    horizon,
                )
            )

        lockbox_report = backtest.run(lockbox_signals, market)
        stressed_report = backtest.run(lockbox_signals, market, cost_multiplier=2.0)
        gate = evaluate_profitability_gate(
            lockbox_report.trades,
            walk_forward,
            initial_equity_usdt=lockbox_report.initial_equity_usdt,
            two_x_cost_net_return=stressed_report.net_return,
            mark_to_market_max_drawdown=lockbox_report.max_drawdown,
            mark_to_market_evidence_complete=lockbox_report.mark_to_market_used,
            execution_evidence_complete=execution_evidence_complete,
            factor_ablation_complete=bool(factor_report["all_required_groups_evaluated"]),
            thresholds=ProfitabilityThresholds(),
        )
        if not gate.passed:
            _archive_candidate_manifest(output, self.trial_id)
        write_profitability_report(output / "profitability_report.json", gate)
        _atomic_json(
            output / "lockbox_report.json",
            {
                "trial_id": self.trial_id,
                "status": "EVALUATED_ONCE",
                "lockbox_fingerprint": lockbox_fingerprint,
                "used_for_parameter_selection": False,
                "source_evidence": source_evidence,
                "final_development_selection": final_selection,
                "result": lockbox_report.to_dict(include_trades=True),
            },
        )
        _atomic_json(
            bundle_path,
            {
                "schema_version": "profitability-model-bundle.v2",
                "trial_id": self.trial_id,
                "model_family": "profitability_two_stage",
                "release_stage": "candidate" if gate.passed else "rejected",
                "profitability_gate": gate.profitability_gate,
                "models": model_paths,
                "model_sha256": model_sha256,
                "formal_feature_columns": {
                    str(horizon): list(columns)
                    for horizon, columns in model_feature_columns_by_horizon.items()
                },
                "retained_factor_groups": list(retained_groups),
                "lockbox_fingerprint": lockbox_fingerprint,
                "lockbox_consumed": True,
                "code_commit": self.config.code_commit,
            },
        )
        if gate.passed:
            create_candidate_manifest(
                output / "candidate_release_manifest.json",
                gate=gate,
                profitability_report_path=output / "profitability_report.json",
                model_artifact_path=bundle_path,
                lockbox_fingerprint=lockbox_fingerprint,
                code_commit=self.config.code_commit,
            )
        record = TrialRecord(
            trial_id=self.trial_id,
            model_family="profitability_two_stage",
            data_signature=_hash_payload(source_evidence)[:24],
            parameter_hash=TrialLedger.parameter_hash(
                {
                    "candidate_configs": [asdict(config) for config in candidate_configs],
                    "features_by_horizon": model_feature_columns_by_horizon,
                    "nested_walk_forward": True,
                }
            ),
            code_commit=self.config.code_commit,
            status="completed" if gate.passed else "rejected",
            metrics=gate.to_dict(),
        )
        self.ledger.append(record)
        self.ledger.append_event(
            self.trial_id, "completed" if gate.passed else "rejected", gate.to_dict()
        )
        return gate

    def record_failure(self, reason: str) -> None:
        """Persist an incomplete experiment without pretending it reached lockbox."""

        metrics = {
            "profitability_gate": "FAILED",
            "stage": "rejected",
            "candidate_count": 0,
            "live_count": 0,
            "pipeline_error": reason,
        }
        self.ledger.append_event(self.trial_id, "failed", metrics)
        record = TrialRecord(
            trial_id=self.trial_id,
            model_family="profitability_two_stage",
            data_signature="pipeline_incomplete",
            parameter_hash=TrialLedger.parameter_hash(
                {
                    "max_bars_per_symbol": self.config.max_bars_per_symbol,
                    "walk_forward_folds": self.config.walk_forward_folds,
                    "horizons": HORIZONS_SEC,
                    "symbols": SYMBOLS,
                }
            ),
            code_commit=self.config.code_commit,
            status="failed",
            metrics=metrics,
        )
        try:
            self.ledger.append(record)
        except ValueError:
            # An exact run may already have a terminal record.  The append-only
            # event above still preserves this failed invocation.
            pass


def write_failed_outputs(output_dir: Path, *, reason: str) -> ProfitabilityGateResult:
    _archive_candidate_manifest(output_dir, "pipeline_failed")
    result = ProfitabilityGateResult(
        profitability_gate="FAILED",
        stage="rejected",
        candidate_count=0,
        live_count=0,
        checks={"pipeline_completed": {"passed": False, "reason": reason}},
        metrics={"trade_count": 0, "net_return": None, "max_drawdown": None},
        blockers=("pipeline_completed",),
    )
    write_profitability_report(output_dir / "profitability_report.json", result)
    for name, payload in (
        ("walk_forward_report.json", {"status": "FAILED", "reason": reason, "folds": []}),
        ("lockbox_report.json", {"status": "FAILED", "reason": reason, "used_for_parameter_selection": False}),
        ("factor_ablation_report.json", _factor_ablation_report()),
        ("execution_cost_report.json", {"status": "FAILED", "reason": reason, "execution_evidence_complete": False}),
        ("capital_preservation_report.json", policy_report(CapitalPreservationConfig())),
    ):
        _atomic_json(output_dir / name, payload)
    return result


__all__: Sequence[str] = (
    "ProfitabilityRebuild",
    "ProfitabilityRebuildConfig",
    "MINIMUM_COVERAGE_DAYS",
    "SYMBOLS",
    "validate_source_coverage",
    "write_failed_outputs",
)
