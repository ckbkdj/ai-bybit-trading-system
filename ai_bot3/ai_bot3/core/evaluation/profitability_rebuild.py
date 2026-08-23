from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
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
from core.evaluation.statistical_governance import TrialLedger, TrialRecord
from core.labels.triple_barrier import EntrySpec, MarketBar, TripleBarrierConfig, build_triple_barrier_label
from core.models.two_stage import TwoStageConfig, TwoStagePrediction
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


def _engineer_features(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    close = data["close"].replace(0, np.nan)
    volume = data["volume"].clip(lower=0.0)
    for window in (1, 2, 3, 6, 12, 24):
        data[f"ret_{window}"] = close.pct_change(window)
    data["range_pct"] = (data["high"] - data["low"]) / close
    data["body_pct"] = (data["close"] - data["open"]) / data["open"].replace(0, np.nan)
    data["volume_zscore"] = (
        (volume - volume.rolling(48, min_periods=8).mean())
        / (volume.rolling(48, min_periods=8).std() + 1e-12)
    )
    data["volatility"] = close.pct_change().rolling(24, min_periods=8).std()
    data["liquidity"] = close * volume
    data["momentum_vol_ratio"] = data["ret_12"] / (data["volatility"] + 1e-12)
    data["ma_gap_8_24"] = close.rolling(8, min_periods=4).mean() / (close.rolling(24, min_periods=8).mean() + 1e-12) - 1.0
    return data.replace([np.inf, -np.inf], np.nan)


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
    feature_names = (
        "ret_1", "ret_2", "ret_3", "ret_6", "ret_12", "ret_24",
        "range_pct", "body_pct", "volume_zscore", "momentum_vol_ratio", "ma_gap_8_24",
    )
    for index in range(48, len(features) - 1):
        row = features.iloc[index]
        if row[list(feature_names) + ["volatility", "liquidity"]].isna().any():
            continue
        signal_at = row["close_at"].to_pydatetime()
        reference = float(row["close"])
        volatility_bps = max(20.0, min(300.0, float(row["volatility"]) * 10_000.0))
        stop_bps = max(25.0, volatility_bps * 1.25)
        take_profit_bps = stop_bps * 1.50
        # Give the label builder the complete future stream.  It performs its
        # own max-holding cutoff; a fixed two-bar slice is not a holding path.
        path = bars[index + 1 :]
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
                    max_wait_sec=max(30, min(300, horizon_sec // 2)),
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


def _factor_ablation_report() -> dict[str, object]:
    groups = []
    for cadence, definitions in (("short", SHORT_FACTOR_GROUPS), ("medium_long", LONG_FACTOR_GROUPS)):
        for group, factors in definitions.items():
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
        "all_required_groups_evaluated": False,
        "retained_factor_groups": [],
        "groups": groups,
        "blocker": "current local store has OHLCV and snapshots, but not complete PIT histories for required orderbook/trades/macro-vintage groups",
    }


class ProfitabilityRebuild:
    def __init__(self, config: ProfitabilityRebuildConfig) -> None:
        self.config = config
        self.source = KlinePanelSource(config.feature_store_path)
        self.ledger = TrialLedger(config.trial_ledger_path)
        run_payload = {
            "code_commit": config.code_commit,
            "feature_store": str(config.feature_store_path.resolve()),
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
                    **coverage,
                }
            panels[horizon] = pd.DataFrame(rows)

        splitter = PooledPanelBuilder(
            lockbox_fraction=self.config.lockbox_fraction,
            minimum_train_rows=300,
            minimum_test_rows=80,
            maximum_folds=self.config.walk_forward_folds,
        )
        datasets = splitter.build(panels)
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
        for horizon, dataset in datasets.items():
            for fold in dataset.folds:
                train = dataset.development.iloc[list(fold.train_indices)]
                test = dataset.development.iloc[list(fold.test_indices)]
                selection = selector.select_and_fit(train, FEATURE_COLUMNS)
                predictions = selection.model.predict(test)
                signals = _signals_from_predictions(test, predictions, horizon)
                development_signals.extend(signals)
                report = backtest.run(signals, market)
                walk_forward.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "train_rows": len(train),
                        "test_rows": len(test),
                        "signals": len(signals),
                        "trades": len(report.trades),
                        "net_return": report.net_return,
                        "max_drawdown": report.max_drawdown,
                        "profit_factor": report.profit_factor,
                        "purge_sec": fold.purge_sec,
                        "embargo_sec": fold.embargo_sec,
                        "nested_selection": dict(selection.audit),
                        "inner_candidate_results": list(selection.candidate_results),
                        "outer_oos_used_for_tuning": False,
                    }
                )

        development_report = backtest.run(development_signals, market)
        development_stress = backtest.run(
            development_signals, market, cost_multiplier=2.0
        )
        factor_report = _factor_ablation_report()
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
                },
            )
            record = TrialRecord(
                trial_id=self.trial_id,
                model_family="profitability_two_stage",
                data_signature=_hash_payload(source_evidence)[:24],
                parameter_hash=TrialLedger.parameter_hash(
                    {
                        "candidate_configs": [asdict(config) for config in candidate_configs],
                        "features": FEATURE_COLUMNS,
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
        model_paths: dict[str, str] = {}
        final_selection: dict[str, object] = {}
        for horizon, dataset in datasets.items():
            selection = selector.select_and_fit(dataset.development, FEATURE_COLUMNS)
            path = self.config.model_output_dir / self.trial_id / f"horizon_{horizon}.json"
            selection.model.save(path)
            model_paths[str(horizon)] = str(path)
            final_selection[str(horizon)] = {
                "audit": dict(selection.audit),
                "candidate_results": list(selection.candidate_results),
            }
            lockbox_signals.extend(
                _signals_from_predictions(
                    dataset.lockbox,
                    selection.model.predict(dataset.lockbox),
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
        bundle_path = self.config.model_output_dir / self.trial_id / "model_bundle.json"
        _atomic_json(
            bundle_path,
            {
                "trial_id": self.trial_id,
                "model_family": "profitability_two_stage",
                "release_stage": "candidate" if gate.passed else "rejected",
                "models": model_paths,
                "lockbox_fingerprint": lockbox_fingerprint,
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
                    "features": FEATURE_COLUMNS,
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
