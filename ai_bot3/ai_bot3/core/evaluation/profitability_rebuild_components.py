from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from contracts.horizons import MODE_HORIZONS
from core.backtest.event_driven import BacktestConfig, EventDrivenBacktest, SignalEvent
from core.evaluation.profitability_gate import (
    ProfitabilityGateResult,
    ProfitabilityThresholds,
    evaluate_development_gate,
    evaluate_profitability_gate,
    write_profitability_report,
)
from core.evaluation.ablation import compare_factor_groups
from core.evaluation.calibration_coverage import (
    directional_calibration_rows,
    evaluate_quantile_coverage,
)
from core.evaluation.release_evidence import (
    intratrade_drawdown_evidence,
    nested_cv_evidence,
    production_replay_evidence,
    signal_funnel_evidence,
)
from core.evaluation.statistical_governance import (
    TrialLedger,
    TrialRecord,
    final_evaluation_statistical_evidence,
    statistical_overfit_evidence,
)
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
from core.models.profitability_runtime import (
    EXTERNAL_FEATURE_ALIASES,
    build_current_feature_rows,
    generate_profitability_alpha_prediction,
    select_directional_prediction,
)
from core.release.profitability_release import create_candidate_manifest
from core.risk.capital_preservation import CapitalPreservationConfig, policy_report
from core.training.pooled_panel import (
    HORIZON_TIMEFRAME,
    HORIZONS_SEC,
    HorizonDataset,
    PooledPanelBuilder,
    causal_regime_labels,
    dataset_manifest,
    split_factor_research_and_evaluation,
)
from core.training.nested_walk_forward import NestedWalkForwardSelector
from core.training.bybit_execution_bars import (
    ORDERBOOK_EXECUTION_FEATURES,
    enrich_market_bars_with_bybit_execution_pit,
)
from core.training.bybit_pit_panel import BybitPITFeatureSource
from core.training.macro_pit_panel import (
    MACRO_FEATURE_CONTRACTS,
    MacroPITFeatureSource,
)
from core.training.flow_pit_panel import (
    FLOW_FEATURE_CONTRACTS,
    FlowPITFeatureSource,
)
from core.training.pit_factor_panel import TradPanelHistorySource
from core.providers.bybit_kline_history import verify_bybit_listing_evidence


SYMBOLS: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "1000PEPEUSDT")
MINIMUM_COVERAGE_DAYS: Mapping[str, float] = {
    "3m": 180.0,
    "15m": 365.0,
    "2h": 1095.0,
    "4h": 1095.0,
    "1d": 1825.0,
}
TIMEFRAME_INTERVAL_SEC: Mapping[str, int] = {
    timeframe: horizon for horizon, timeframe in HORIZON_TIMEFRAME.items()
}
SHORT_FACTOR_GROUPS: Mapping[str, tuple[str, ...]] = {
    "bybit_orderbook": ("bybit_orderbook_delta_l5", "orderbook_spread_bps", "orderbook_depth_usdt_l5", "microprice_deviation_bps"),
    "public_trades": ("public_trade_imbalance_1m", "ofi_1m", "aggressive_cvd_1m"),
    "basis_funding_oi": (
        "perpetual_basis_bps",
        "funding_rate",
        "open_interest_change_1h",
    ),
    "liquidations": ("liquidation_imbalance_5m",),
    "execution_quality": ("fill_probability", "expected_slippage_bps"),
}
SHORT_FACTOR_COLLECTION_EVIDENCE: Mapping[str, Mapping[str, object]] = {
    "liquidations": {
        "data_mode": "forward_only_public_websocket",
        "source_topic": "allLiquidation.{symbol}",
        "push_frequency_ms": 500,
        "historical_backfill_supported": False,
        "official_rest_history_endpoint": None,
        "source_contract_url": (
            "https://bybit-exchange.github.io/docs/v5/websocket/public/"
            "all-liquidation"
        ),
        "release_policy": (
            "collect until the precommitted minimum PIT history is reached; "
            "never infer liquidations from OHLCV"
        ),
    }
}
SHORT_FACTOR_LIVE_TOPIC_PREFIXES: Mapping[str, tuple[str, ...]] = {
    "bybit_orderbook": ("orderbook.",),
    "public_trades": ("publicTrade.", "orderbook."),
    "basis_funding_oi": ("tickers.",),
    "liquidations": ("allLiquidation.",),
    "execution_quality": ("orderbook.",),
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
    "stablecoin_flows": tuple(
        name for name in FLOW_FEATURE_CONTRACTS if name.startswith("stablecoin_")
    ),
    "fund_flows": ("digital_asset_fund_flow_weekly_usd",),
    "macro_vintage": (
        "fred_cpi_first_release_yoy_ratio",
        "fred_payrolls_first_release_change_thousands",
        "fred_unemployment_first_release_pct",
        "alfred_cpi_mean_revision_delta",
        "alfred_payrolls_mean_revision_delta",
    ),
    "tier_a_events": (
        "tier_a_event_state",
        "fomc_statement_event_state",
    ),
}
MINIMUM_ABLATION_OOS_TRADES = 30
MINIMUM_ABLATION_TRADED_FOLDS = 2
MINIMUM_SHORT_FACTOR_HISTORY_DAYS = 180.0
ABLATION_RESEARCH_SELECTION_FRACTION = 0.02
ABLATION_RESEARCH_TAIL_PENALTY = 0.50
BYBIT_EXECUTION_EVIDENCE_FEATURES = tuple(
    dict.fromkeys((*ORDERBOOK_EXECUTION_FEATURES, "funding_rate"))
)


def _precommitted_statistical_trial_count(
    candidate_config_count: int,
    historical_pipeline_trial_count: int,
) -> dict[str, int]:
    """Count every pre-registered model/factor arm before OOS is scored."""

    if candidate_config_count < 2 or historical_pipeline_trial_count < 0:
        raise ValueError("invalid statistical trial-count inputs")
    final_model_variants = candidate_config_count * len(HORIZONS_SEC)
    ablation_horizon_arms = 2 * (
        len(LEGACY_FACTOR_GROUPS) * len(HORIZONS_SEC)
        + len(SHORT_FACTOR_GROUPS) * 2
        + len(LONG_FACTOR_GROUPS) * 3
    )
    current_pipeline_variants = (
        final_model_variants + candidate_config_count * ablation_horizon_arms
    )
    total = current_pipeline_variants * (historical_pipeline_trial_count + 1)
    return {
        "candidate_config_count": candidate_config_count,
        "final_model_variant_count": final_model_variants,
        "ablation_horizon_arm_count": ablation_horizon_arms,
        "current_pipeline_variant_count": current_pipeline_variants,
        "historical_pipeline_trial_count": historical_pipeline_trial_count,
        "number_of_trials": total,
    }


def _bybit_names_for_horizon(
    horizon_sec: int,
    short_factor_names: Sequence[str],
) -> tuple[str, ...]:
    if horizon_sec not in HORIZONS_SEC:
        raise ValueError(f"unsupported horizon: {horizon_sec}")
    return (
        tuple(short_factor_names)
        if horizon_sec in {180, 900}
        else BYBIT_EXECUTION_EVIDENCE_FEATURES
    )


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
    lockbox_bybit_pit_store_path: Path | None = None
    macro_pit_store_path: Path | None = None
    verify_macro_raw_hashes: bool = True
    flow_pit_store_path: Path | None = None
    verify_flow_raw_hashes: bool = True
    kline_source: str = "binance"
    max_bars_per_symbol: int = 200_000
    walk_forward_folds: int = 6
    lockbox_fraction: float = 0.15
    random_seed: int = 20260823

    def __post_init__(self) -> None:
        if self.kline_source not in {"binance", "bybit"}:
            raise ValueError("kline_source must be 'binance' or 'bybit'")
        if self.max_bars_per_symbol < 20_000:
            raise ValueError("max_bars_per_symbol is too small for required short-horizon coverage")
        if not 4 <= self.walk_forward_folds <= 8:
            raise ValueError(
                "walk_forward_folds must be between 4 and 8 so factor research "
                "and frozen-feature evaluation each have at least two OOS folds"
            )


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    temporary.replace(path)


def _require_precommitted_horizon_gates(
    portfolio_gate: ProfitabilityGateResult,
    horizon_gates: Mapping[int, ProfitabilityGateResult],
) -> ProfitabilityGateResult:
    """Prevent a profitable portfolio from authorizing a failed horizon model."""

    approved = sorted(
        int(horizon) for horizon, result in horizon_gates.items() if result.passed
    )
    all_passed = bool(horizon_gates) and len(approved) == len(horizon_gates)
    checks = dict(portfolio_gate.checks)
    checks["precommitted_horizon_profitability"] = {
        "passed": all_passed,
        "required_horizons": sorted(int(value) for value in horizon_gates),
        "passed_horizons": approved,
        "policy": "every development-precommitted horizon must independently pass lockbox",
    }
    blocker_items = list(portfolio_gate.blockers)
    if not all_passed:
        blocker_items.append("precommitted_horizon_profitability")
    blockers = tuple(dict.fromkeys(blocker_items))
    passed = portfolio_gate.passed and all_passed
    return ProfitabilityGateResult(
        profitability_gate="PASSED" if passed else "FAILED",
        stage="candidate" if passed else "rejected",
        candidate_count=1 if passed else 0,
        live_count=0,
        checks=checks,
        metrics={
            **portfolio_gate.metrics,
            "precommitted_horizons": sorted(int(value) for value in horizon_gates),
            "individually_passed_horizons": approved,
        },
        blockers=blockers,
    )


def _ablation_ledger_summary(result: Mapping[str, object]) -> dict[str, object]:
    summary = {
        key: result.get(key)
        for key in (
            "oos_ablation_status",
            "evaluated",
            "pit_observation_count",
            "oos_fold_count",
            "execution_evidence",
            "mean_improvement",
            "bootstrap_lower_mean_improvement",
            "bootstrap_confidence",
            "bootstrap_samples",
            "improved_fold_ratio",
            "worst_fold_improvement",
            "retained",
            "formal_feature_set",
            "all_applicable_horizons_evaluated",
            "retained_horizons",
        )
        if key in result
    }
    summary["horizon_results"] = {
        str(horizon): {
            key: item.get(key)
            for key in (
                "oos_ablation_status",
                "evaluated",
                "scheduled_oos_fold_count",
                "oos_fold_count",
                "unevaluated_oos_fold_count",
                "execution_evidence",
                "mean_improvement",
                "bootstrap_lower_mean_improvement",
                "improved_fold_ratio",
                "worst_fold_improvement",
                "retained",
            )
            if key in item
        }
        for horizon, item in dict(result.get("horizon_results") or {}).items()
        if isinstance(item, Mapping)
    }
    return summary


AblationProgressCallback = Callable[[Mapping[str, object]], None]


def _emit_ablation_progress(
    callback: AblationProgressCallback | None,
    *,
    factor_group: str,
    horizon_sec: int,
    fold_id: str,
    status: str,
    train_rows: int,
    test_rows: int,
) -> None:
    if callback is None:
        return
    callback(
        {
            "factor_group": factor_group,
            "horizon_sec": horizon_sec,
            "fold_id": fold_id,
            "status": status,
            "train_rows": train_rows,
            "test_rows": test_rows,
        }
    )


def _factor_fold_coverage(
    train: pd.DataFrame,
    test: pd.DataFrame,
    columns: Sequence[str],
) -> tuple[pd.Series, pd.Series, dict[str, object]]:
    """Require the ablation sample to match final train/serve availability."""

    train_mask = train[list(columns)].notna().all(axis=1)
    test_mask = test[list(columns)].notna().all(axis=1)
    train_complete = int(train_mask.sum())
    test_complete = int(test_mask.sum())
    evidence = {
        "required_factor_columns": list(columns),
        "train_total_rows": len(train),
        "train_complete_rows": train_complete,
        "train_missing_rows": len(train) - train_complete,
        "train_complete_ratio": train_complete / len(train) if len(train) else 0.0,
        "test_total_rows": len(test),
        "test_complete_rows": test_complete,
        "test_missing_rows": len(test) - test_complete,
        "test_complete_ratio": test_complete / len(test) if len(test) else 0.0,
        "complete": bool(train_mask.all() and test_mask.all()),
        "policy": (
            "no complete-case row selection: every scheduled train/OOS row must "
            "have every retained factor because production inference fails closed"
        ),
    }
    return train_mask, test_mask, evidence


def _execution_release_evidence(
    report: object,
    *,
    shadow_or_testnet_fill_receipt_count: int = 0,
    queue_position_and_latency_calibration_complete: bool = False,
) -> dict[str, object]:
    """Separate candidate backtest evidence from the stricter live gate."""

    official_pit_cost_inputs_complete = bool(
        getattr(report, "execution_cost_evidence_complete", False)
    )
    observed_price_path_complete = bool(
        getattr(
            report,
            "price_path_evidence_complete",
            official_pit_cost_inputs_complete,
        )
    )
    simulation_complete = bool(getattr(report, "simulation_complete", False))
    risk_policy_compliant = bool(getattr(report, "risk_policy_compliant", False))
    receipts_complete = shadow_or_testnet_fill_receipt_count > 0
    candidate_complete = bool(
        official_pit_cost_inputs_complete
        and observed_price_path_complete
        and simulation_complete
        and risk_policy_compliant
    )
    live_complete = bool(
        candidate_complete
        and receipts_complete
        and queue_position_and_latency_calibration_complete
    )
    return {
        "official_pit_cost_inputs_complete": official_pit_cost_inputs_complete,
        "observed_price_path_complete": observed_price_path_complete,
        "observed_price_path_trade_count": int(
            getattr(report, "observed_price_path_trade_count", 0)
        ),
        "proxy_price_path_trade_count": int(
            getattr(report, "proxy_price_path_trade_count", 0)
        ),
        "simulation_complete": simulation_complete,
        "unresolved_position_count": int(
            getattr(report, "unresolved_position_count", 0)
        ),
        "risk_policy_compliant": risk_policy_compliant,
        "risk_budget_breach_count": int(
            getattr(report, "risk_budget_breach_count", 0)
        ),
        "capital_preservation_breaches": {
            "daily_loss_limit": int(
                getattr(report, "daily_loss_limit_breach_count", 0)
            ),
            "weekly_loss_limit": int(
                getattr(report, "weekly_loss_limit_breach_count", 0)
            ),
            "equity_drawdown_limit": bool(
                getattr(report, "equity_drawdown_limit_breached", False)
            ),
            "leverage_limit": int(
                getattr(report, "leverage_limit_breach_count", 0)
            ),
        },
        "direct_execution_cost_trade_count": int(
            getattr(report, "direct_execution_cost_trade_count", 0)
        ),
        "proxy_execution_cost_trade_count": int(
            getattr(report, "proxy_execution_cost_trade_count", 0)
        ),
        "candidate_backtest_execution_evidence_complete": candidate_complete,
        "shadow_or_testnet_fill_receipts_complete": receipts_complete,
        "shadow_or_testnet_fill_receipt_count": int(
            shadow_or_testnet_fill_receipt_count
        ),
        "queue_position_and_latency_calibration_complete": bool(
            queue_position_and_latency_calibration_complete
        ),
        "live_execution_evidence_complete": live_complete,
        "historical_archive_claim": (
            "official PIT spread/depth/funding inputs; not realized own-order fills"
        ),
        "candidate_scope": (
            "may open a sealed lockbox and produce candidate/shadow evidence only"
        ),
        "candidate_blockers": [
            name
            for name, passed in (
                ("official_pit_cost_inputs_incomplete", official_pit_cost_inputs_complete),
                ("observed_price_path_incomplete", observed_price_path_complete),
                ("execution_simulation_incomplete", simulation_complete),
                ("capital_risk_budget_breached", risk_policy_compliant),
            )
            if not passed
        ],
        "live_blocker": (
            None
            if live_complete
            else "requires immutable OOS shadow/testnet fill receipts and queue/latency calibration"
        ),
    }


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


def _stable_file_identity(path: Path) -> dict[str, object]:
    """Hash an artifact only when its filesystem identity stays unchanged."""

    resolved = Path(path).resolve()
    before = resolved.stat()
    sha256 = _sha256_file(resolved)
    after = resolved.stat()
    before_identity = (before.st_size, before.st_mtime_ns)
    after_identity = (after.st_size, after.st_mtime_ns)
    if after_identity != before_identity:
        raise RuntimeError(f"artifact changed while hashing: {resolved}")
    return {
        "database": str(resolved),
        "size_bytes": after.st_size,
        "modified_ns": after.st_mtime_ns,
        "sha256": sha256,
    }


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

    def __init__(self, path: Path, *, source: str = "binance") -> None:
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        normalized_source = str(source).strip().lower()
        if normalized_source not in {"binance", "bybit"}:
            raise ValueError("kline source must be 'binance' or 'bybit'")
        self.source = normalized_source

    @staticmethod
    def _attach_times(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Normalize inclusive exchange close milliseconds to availability time."""

        interval_ms = int(TIMEFRAME_INTERVAL_SEC[timeframe] * 1_000)
        open_ms = pd.to_numeric(frame["open_time"], errors="raise").astype("int64")
        close_ms = pd.to_numeric(frame["close_time"], errors="raise").astype("int64")
        inclusive_close = (close_ms - open_ms + 1) == interval_ms
        normalized_close_ms = close_ms + inclusive_close.astype("int64")
        frame["open_at"] = pd.to_datetime(open_ms, unit="ms", utc=True)
        frame["close_at"] = pd.to_datetime(
            normalized_close_ms, unit="ms", utc=True
        )
        return frame

    def load(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        uri = f"file:{self.path.resolve().as_posix()}?mode=ro"
        query = """
            SELECT symbol,timeframe,source,open_time,close_time,open,high,low,close,volume,fetched_at
              FROM (
                    SELECT symbol,timeframe,source,open_time,close_time,open,high,low,close,volume,fetched_at
                      FROM raw_kline
                     WHERE symbol=? AND timeframe=? AND source=?
                     ORDER BY open_time DESC
                     LIMIT ?
              )
             ORDER BY open_time
        """
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            frame = pd.read_sql_query(
                query,
                connection,
                params=(symbol, timeframe, self.source, int(limit)),
            )
        if frame.empty:
            raise ValueError(f"no {self.source} bars for {symbol} {timeframe}")
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(subset=["open", "high", "low", "close", "volume"])
        return self._attach_times(frame, timeframe).reset_index(drop=True)

    def load_timestamps(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> pd.DataFrame:
        """Read only the immutable time grid used to pre-register boundaries."""

        uri = f"file:{self.path.resolve().as_posix()}?mode=ro"
        query = """
            SELECT symbol,timeframe,source,open_time,close_time
              FROM (
                    SELECT symbol,timeframe,source,open_time,close_time
                      FROM raw_kline
                     WHERE symbol=? AND timeframe=? AND source=?
                     ORDER BY open_time DESC
                     LIMIT ?
              )
             ORDER BY open_time
        """
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            frame = pd.read_sql_query(
                query,
                connection,
                params=(symbol, timeframe, self.source, int(limit)),
            )
        if frame.empty:
            raise ValueError(f"no {self.source} bars for {symbol} {timeframe}")
        return self._attach_times(frame, timeframe).reset_index(drop=True)

    def load_before(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        *,
        close_at_or_before: object,
        include_boundary: bool = True,
    ) -> pd.DataFrame:
        """Read OHLCV through, or strictly before, a pre-registered boundary."""

        cutoff = pd.to_datetime(close_at_or_before, utc=True, errors="coerce")
        if pd.isna(cutoff):
            raise ValueError("kline close boundary is invalid")
        cutoff_ms = int(pd.Timestamp(cutoff).value // 1_000_000)
        interval_ms = int(TIMEFRAME_INTERVAL_SEC[timeframe] * 1_000)
        comparison = "<=" if include_boundary else "<"
        uri = f"file:{self.path.resolve().as_posix()}?mode=ro"
        query = f"""
            SELECT symbol,timeframe,source,open_time,close_time,open,high,low,close,volume,fetched_at
              FROM (
                    SELECT symbol,timeframe,source,open_time,close_time,open,high,low,close,volume,fetched_at
                      FROM raw_kline
                     WHERE symbol=? AND timeframe=? AND source=?
                       AND (
                            close_time
                            + CASE
                                WHEN close_time-open_time+1=? THEN 1
                                ELSE 0
                              END
                       ) {comparison} ?
                     ORDER BY open_time DESC
                     LIMIT ?
              )
             ORDER BY open_time
        """
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            frame = pd.read_sql_query(
                query,
                connection,
                params=(
                    symbol,
                    timeframe,
                    self.source,
                    interval_ms,
                    cutoff_ms,
                    int(limit),
                ),
            )
        if frame.empty:
            raise ValueError(
                f"no {self.source} bars for {symbol} {timeframe} before the PIT boundary"
            )
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(subset=["open", "high", "low", "close", "volume"])
        return self._attach_times(frame, timeframe).reset_index(drop=True)

    def listing_evidence(
        self, symbol: str, timeframe: str
    ) -> dict[str, object] | None:
        if self.source == "bybit":
            return verify_bybit_listing_evidence(self.path, symbol, timeframe)
        uri = f"file:{self.path.resolve().as_posix()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            exists = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='kline_listing_evidence'"""
            ).fetchone()
            if not exists:
                return None
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                """SELECT evidence.*,
                          batch.archive_path AS retained_archive_path,
                          batch.checksum_path AS retained_checksum_path,
                          batch.archive_url AS batch_archive_url,
                          batch.archive_sha256 AS batch_archive_sha256,
                          batch.checksum_sha256 AS batch_checksum_sha256,
                          batch.checksum_verified AS batch_checksum_verified
                     FROM kline_listing_evidence AS evidence
                LEFT JOIN kline_archive_batches AS batch
                       ON batch.symbol=evidence.symbol
                      AND batch.timeframe=evidence.timeframe
                      AND batch.source=evidence.source
                      AND batch.year_month=evidence.first_archive_year_month
                    WHERE evidence.symbol=? AND evidence.timeframe=?
                      AND evidence.source='binance'
                    ORDER BY evidence.verified_at DESC LIMIT 1""",
                (symbol, timeframe),
            ).fetchone()
        if row is None:
            return None
        evidence = dict(row)
        failures: list[str] = []
        archive_path = Path(str(evidence.pop("retained_archive_path") or ""))
        checksum_path = Path(str(evidence.pop("retained_checksum_path") or ""))
        batch_archive_url = evidence.pop("batch_archive_url")
        batch_archive_sha256 = evidence.pop("batch_archive_sha256")
        batch_checksum_sha256 = evidence.pop("batch_checksum_sha256")
        batch_checksum_verified = evidence.pop("batch_checksum_verified")
        if batch_archive_url != evidence.get("first_archive_url"):
            failures.append("archive_url_receipt_mismatch")
        if batch_archive_sha256 != evidence.get("first_archive_sha256"):
            failures.append("archive_sha256_receipt_mismatch")
        if batch_checksum_verified not in {1, True}:
            failures.append("batch_checksum_not_verified")
        for label, path, expected_sha256 in (
            ("archive", archive_path, batch_archive_sha256),
            ("checksum", checksum_path, batch_checksum_sha256),
        ):
            if not path.is_file():
                failures.append(f"retained_{label}_missing")
                continue
            try:
                actual_sha256 = _sha256_file(path)
            except OSError:
                failures.append(f"retained_{label}_unreadable")
                continue
            if not expected_sha256 or actual_sha256 != expected_sha256:
                failures.append(f"retained_{label}_sha256_mismatch")
        evidence["raw_receipt_reverified"] = not failures
        evidence["raw_receipt_reverification_failures"] = failures
        return evidence


def audit_source_coverage(
    frame: pd.DataFrame,
    timeframe: str,
    *,
    listing_evidence: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Describe the complete, actually observed kline grid without hiding failures."""

    if timeframe not in MINIMUM_COVERAGE_DAYS:
        raise ValueError(f"unsupported coverage timeframe: {timeframe}")
    expected_interval_sec = TIMEFRAME_INTERVAL_SEC[timeframe]
    fixed_minimum = float(MINIMUM_COVERAGE_DAYS[timeframe])
    base: dict[str, object] = {
        "bars": int(len(frame)),
        "start": None,
        "end": None,
        "coverage_days": 0.0,
        "minimum_coverage_days": fixed_minimum,
        "fixed_minimum_coverage_days": fixed_minimum,
        "coverage_policy": "fixed_history_floor",
        "listing_exception_applied": False,
        "listing_evidence": dict(listing_evidence or {}),
        "coverage_gate": "FAILED",
        "continuity_gate": "FAILED",
        "status": "FAILED",
        "expected_interval_sec": expected_interval_sec,
        "maximum_open_gap_sec": None,
        "independent_open_timestamp_count": 0,
        "invalid_timestamp_count": 0,
        "duplicate_open_count": 0,
        "invalid_bar_duration_count": 0,
        "discontinuity_count": 0,
        "missing_interval_count": 0,
        "missing_bar_count": 0,
        "overlap_or_off_grid_count": 0,
        "missing_intervals": [],
        "missing_intervals_truncated": False,
        "missing_intervals_sha256": _hash_payload([]),
        "failure_reasons": [],
    }
    if frame.empty or "open_at" not in frame or "close_at" not in frame:
        base["failure_reasons"] = ["open_at_and_close_at_required"]
        return base
    ordered = frame[["open_at", "close_at"]].copy()
    ordered["open_at"] = pd.to_datetime(
        ordered["open_at"], utc=True, errors="coerce"
    )
    ordered["close_at"] = pd.to_datetime(
        ordered["close_at"], utc=True, errors="coerce"
    )
    invalid_timestamp_count = int(ordered.isna().any(axis=1).sum())
    base["invalid_timestamp_count"] = invalid_timestamp_count
    ordered = ordered.dropna().sort_values("open_at").reset_index(drop=True)
    if ordered.empty:
        base["failure_reasons"] = ["timestamps_invalid"]
        return base
    first = ordered["open_at"].min()
    last = ordered["close_at"].max()
    if pd.isna(first) or pd.isna(last) or last <= first:
        base["failure_reasons"] = ["timestamps_invalid"]
        return base
    duplicate_open_count = int(ordered["open_at"].duplicated().sum())
    base["duplicate_open_count"] = duplicate_open_count
    base["independent_open_timestamp_count"] = int(ordered["open_at"].nunique())
    base["start"] = pd.Timestamp(first).isoformat().replace("+00:00", "Z")
    base["end"] = pd.Timestamp(last).isoformat().replace("+00:00", "Z")
    coverage_days = float((last - first).total_seconds() / 86_400.0)
    base["coverage_days"] = coverage_days
    effective_minimum = fixed_minimum
    if listing_evidence:
        listing_start = pd.to_datetime(
            listing_evidence.get("listing_start_utc"), utc=True, errors="coerce"
        )
        earliest_open_time_ms = listing_evidence.get("earliest_open_time_ms")
        official_binance_archive = str(
            listing_evidence.get("first_archive_url") or ""
        ).startswith(
            "https://data.binance.vision/data/futures/um/monthly/klines/"
        )
        binance_listing_verified = bool(
            listing_evidence.get("status") == "VERIFIED_SINCE_LISTING"
            and int(listing_evidence.get("prior_month_http_status", 0)) == 404
            and listing_evidence.get("first_archive_checksum_verified") in {1, True}
            and listing_evidence.get("raw_receipt_reverified") is True
            and official_binance_archive
            and not pd.isna(listing_start)
            and earliest_open_time_ms is not None
            and abs(
                float(earliest_open_time_ms)
                - pd.Timestamp(first).timestamp() * 1000.0
            )
            <= 1_000.0
            and abs((pd.Timestamp(first) - listing_start).total_seconds()) <= 1.0
        )
        official_bybit_instrument = str(
            listing_evidence.get("official_instrument_url") or ""
        ).startswith(
            "https://api.bybit.com/v5/market/instruments-info?category=linear&symbol="
        )
        bybit_launch_verified = bool(
            listing_evidence.get("status") == "VERIFIED_SINCE_LAUNCH"
            and listing_evidence.get("source") == "bybit"
            and listing_evidence.get("raw_receipt_reverified") is True
            and int(listing_evidence.get("immutable_trigger_count", 0)) == 8
            and official_bybit_instrument
            and listing_evidence.get("launch_time_ms") is not None
            and not pd.isna(listing_start)
            and earliest_open_time_ms is not None
            and abs(
                float(earliest_open_time_ms)
                - pd.Timestamp(first).timestamp() * 1000.0
            )
            <= 1_000.0
            and abs((pd.Timestamp(first) - listing_start).total_seconds()) <= 1.0
        )
        if binance_listing_verified or bybit_launch_verified:
            since_listing_days = max(
                0.0,
                float((last - listing_start).total_seconds() / 86_400.0),
            )
            effective_minimum = min(fixed_minimum, since_listing_days)
            base["minimum_coverage_days"] = effective_minimum
            base["coverage_policy"] = (
                "fixed_floor_or_verified_since_launch"
                if bybit_launch_verified
                else "fixed_floor_or_verified_since_listing"
            )
            base["listing_exception_applied"] = effective_minimum < fixed_minimum
    ordered = ordered.sort_values("open_at").reset_index(drop=True)
    durations = (
        ordered["close_at"] - ordered["open_at"]
    ).dt.total_seconds()
    invalid_bar_duration_count = int(
        ((durations - expected_interval_sec).abs() > 1.0).sum()
    )
    base["invalid_bar_duration_count"] = invalid_bar_duration_count
    gaps = ordered["open_at"].diff().dt.total_seconds().iloc[1:]
    discontinuity_mask = (gaps - expected_interval_sec).abs() > 1.0
    base["discontinuity_count"] = int(discontinuity_mask.sum())
    base["maximum_open_gap_sec"] = float(gaps.max()) if len(gaps) else None
    missing_records: list[dict[str, object]] = []
    missing_bar_count = 0
    for current_index in gaps[gaps > expected_interval_sec + 1.0].index:
        previous_open = pd.Timestamp(ordered.loc[current_index - 1, "open_at"])
        current_open = pd.Timestamp(ordered.loc[current_index, "open_at"])
        gap_sec = float((current_open - previous_open).total_seconds())
        estimated_missing = max(1, int(math.floor(gap_sec / expected_interval_sec)) - 1)
        missing_bar_count += estimated_missing
        missing_records.append(
            {
                "after_open_at": previous_open.isoformat().replace("+00:00", "Z"),
                "next_open_at": current_open.isoformat().replace("+00:00", "Z"),
                "gap_sec": gap_sec,
                "estimated_missing_bars": estimated_missing,
            }
        )
    base["missing_interval_count"] = len(missing_records)
    base["missing_bar_count"] = int(missing_bar_count)
    base["overlap_or_off_grid_count"] = int(
        (gaps < expected_interval_sec - 1.0).sum()
    )
    base["missing_intervals"] = missing_records[:1000]
    base["missing_intervals_truncated"] = len(missing_records) > 1000
    base["missing_intervals_sha256"] = _hash_payload(missing_records)
    failure_reasons: list[str] = []
    if invalid_timestamp_count:
        failure_reasons.append("timestamps_invalid")
    if duplicate_open_count:
        failure_reasons.append("duplicate_bar_opens")
    if invalid_bar_duration_count:
        failure_reasons.append("invalid_bar_durations")
    if int(base["discontinuity_count"]):
        failure_reasons.append("discontinuous_bar_grid")
    if coverage_days < effective_minimum:
        failure_reasons.append("insufficient_history")
    base["failure_reasons"] = failure_reasons
    base["coverage_gate"] = (
        "PASSED"
        if coverage_days >= effective_minimum
        else "FAILED"
    )
    base["continuity_gate"] = (
        "PASSED"
        if not any(
            (
                invalid_timestamp_count,
                duplicate_open_count,
                invalid_bar_duration_count,
                int(base["discontinuity_count"]),
            )
        )
        else "FAILED"
    )
    base["status"] = "PASSED" if not failure_reasons else "FAILED"
    return base


def validate_source_coverage(
    frame: pd.DataFrame,
    timeframe: str,
    *,
    listing_evidence: Mapping[str, object] | None = None,
) -> dict[str, object]:
    evidence = audit_source_coverage(
        frame, timeframe, listing_evidence=listing_evidence
    )
    if evidence["failure_reasons"] == ["open_at_and_close_at_required"]:
        raise ValueError("source coverage requires open_at and close_at")
    if int(evidence["invalid_timestamp_count"]):
        raise ValueError("source coverage timestamps are invalid")
    if int(evidence["duplicate_open_count"]):
        raise ValueError("source coverage contains duplicate bar opens")
    if int(evidence["invalid_bar_duration_count"]):
        raise ValueError("source coverage contains invalid bar durations")
    if int(evidence["discontinuity_count"]):
        raise ValueError("source coverage is discontinuous")
    if evidence["coverage_gate"] != "PASSED":
        raise ValueError(
            f"{timeframe} coverage {float(evidence['coverage_days']):.2f} days is "
            f"below required {float(evidence['minimum_coverage_days']):.2f} days"
        )
    return evidence


def _write_kline_data_evidence(
    output_dir: Path,
    *,
    trial_id: str,
    code_commit: str,
    feature_store_identity: Mapping[str, object],
    series_audits: Mapping[str, Mapping[str, object]],
    source_timestamp_counts_by_horizon: Mapping[int, int],
    oos_timestamp_evidence: Mapping[int, Mapping[str, object]] | None = None,
) -> None:
    expected_series_count = len(SYMBOLS) * len(HORIZONS_SEC)
    ordered_audits = {
        key: dict(series_audits[key]) for key in sorted(series_audits)
    }
    passed_series_count = sum(
        1 for audit in ordered_audits.values() if audit.get("status") == "PASSED"
    )
    complete = len(ordered_audits) == expected_series_count
    coverage_passed = complete and passed_series_count == expected_series_count
    _atomic_json(
        output_dir / "data_coverage_report.json",
        {
            "schema_version": "profitability-data-coverage.v1",
            "trial_id": trial_id,
            "code_commit": code_commit,
            "feature_store": dict(feature_store_identity),
            "status": "PASSED" if coverage_passed else "FAILED",
            "complete": complete,
            "expected_series_count": expected_series_count,
            "audited_series_count": len(ordered_audits),
            "passed_series_count": passed_series_count,
            "minimum_coverage_days": dict(MINIMUM_COVERAGE_DAYS),
            "continuity_tolerance_sec": 1.0,
            "series": ordered_audits,
        },
    )
    missing_series = {
        key: {
            "timeframe": audit.get("timeframe"),
            "horizon_sec": audit.get("horizon_sec"),
            "discontinuity_count": audit.get("discontinuity_count"),
            "missing_interval_count": audit.get("missing_interval_count"),
            "missing_bar_count": audit.get("missing_bar_count"),
            "overlap_or_off_grid_count": audit.get("overlap_or_off_grid_count"),
            "missing_intervals": audit.get("missing_intervals"),
            "missing_intervals_truncated": audit.get(
                "missing_intervals_truncated"
            ),
            "missing_intervals_sha256": audit.get("missing_intervals_sha256"),
        }
        for key, audit in ordered_audits.items()
        if int(audit.get("discontinuity_count", 0)) > 0
    }
    total_discontinuities = sum(
        int(audit.get("discontinuity_count", 0))
        for audit in ordered_audits.values()
    )
    _atomic_json(
        output_dir / "missing_intervals_report.json",
        {
            "schema_version": "profitability-missing-intervals.v1",
            "trial_id": trial_id,
            "code_commit": code_commit,
            "feature_store_sha256": feature_store_identity.get("sha256"),
            "status": (
                "PASSED" if complete and total_discontinuities == 0 else "FAILED"
            ),
            "complete": complete,
            "audited_series_count": len(ordered_audits),
            "total_discontinuity_count": total_discontinuities,
            "total_missing_interval_count": sum(
                int(audit.get("missing_interval_count", 0))
                for audit in ordered_audits.values()
            ),
            "total_missing_bar_count": sum(
                int(audit.get("missing_bar_count", 0))
                for audit in ordered_audits.values()
            ),
            "series_with_discontinuities": missing_series,
        },
    )
    oos_payload = {
        str(horizon): dict(evidence)
        for horizon, evidence in sorted((oos_timestamp_evidence or {}).items())
    }
    oos_complete = bool(oos_timestamp_evidence) and set(oos_timestamp_evidence) == set(
        HORIZONS_SEC
    ) and all(
        int(evidence.get("unique_decision_timestamp_count", 0)) > 0
        for evidence in oos_timestamp_evidence.values()
    )
    _atomic_json(
        output_dir / "independent_timestamp_count_report.json",
        {
            "schema_version": "profitability-independent-timestamps.v1",
            "trial_id": trial_id,
            "code_commit": code_commit,
            "status": "PASSED" if coverage_passed and oos_complete else "INCOMPLETE",
            "raw_source_complete": coverage_passed,
            "outer_oos_complete": oos_complete,
            "counting_policy": (
                "paired BUY/SELL alternatives and simultaneous symbols are not counted "
                "as independent decision timestamps"
            ),
            "raw_source_unique_timestamps_by_horizon": {
                str(horizon): int(count)
                for horizon, count in sorted(
                    source_timestamp_counts_by_horizon.items()
                )
            },
            "outer_oos_by_horizon": oos_payload,
        },
    )


def _external_replay_context(
    row: pd.Series,
    *,
    model_feature_columns: Sequence[str],
    trad_panel_evidence: Mapping[str, object] | None,
) -> Mapping[str, object] | None:
    required = [
        name for name in model_feature_columns if name in EXTERNAL_FEATURE_ALIASES
    ]
    if not required:
        return None
    if trad_panel_evidence is None:
        raise ValueError("production replay requires verified trad panel evidence")
    available_at = pd.to_datetime(
        row.get("factor_available_at"), utc=True, errors="coerce"
    )
    if pd.isna(available_at):
        raise ValueError("production replay external factor availability is missing")
    revision_control = trad_panel_evidence.get("revision_control")
    if not isinstance(revision_control, Mapping):
        raise ValueError("production replay external revision evidence is missing")
    features = {
        EXTERNAL_FEATURE_ALIASES[name]: float(row[name]) for name in required
    }
    if not all(np.isfinite(value) for value in features.values()):
        raise ValueError("production replay external features are not finite")
    return {
        "status": "ok",
        "source": trad_panel_evidence.get("source"),
        "data": {
            "available_at": pd.Timestamp(available_at).isoformat().replace(
                "+00:00", "Z"
            ),
            "hash_verified": trad_panel_evidence.get("hash_verified"),
            "latest_pass_run_id": trad_panel_evidence.get("latest_pass_run_id"),
            "canonical_sha_from_receipt": trad_panel_evidence.get(
                "canonical_sha256"
            ),
            "revision_control": dict(revision_control),
            "features": features,
        },
    }


def _prediction_values(prediction: object) -> dict[str, float | str | bool]:
    return {
        "decision": str(prediction.decision),
        "p_down": float(prediction.p_down),
        "p_flat": float(prediction.p_flat),
        "p_up": float(prediction.p_up),
        "expected_net_return": float(prediction.expected_net_return),
        "return_p10": float(prediction.return_p10),
        "return_p50": float(prediction.return_p50),
        "return_p90": float(prediction.return_p90),
        "expected_mae": float(prediction.expected_mae),
        "expected_mfe": float(prediction.expected_mfe),
        "uncertainty": float(prediction.uncertainty),
        "meta_trade_probability": float(prediction.meta_trade_probability),
        "lower_bound_net_edge": float(prediction.lower_bound_net_edge),
        "shadow_actionable": bool(
            prediction.decision == "TRADE"
            and float(prediction.lower_bound_net_edge) > 0
        ),
    }


def _require_development_evidence(
    gate: ProfitabilityGateResult,
    *,
    check_name: str,
    evidence: Mapping[str, object],
) -> ProfitabilityGateResult:
    passed_check = bool(evidence.get("passed"))
    checks = {
        **gate.checks,
        check_name: {
            "passed": passed_check,
            "status": evidence.get("status", "MISSING"),
            "complete": bool(evidence.get("complete")),
            "failed_sample_count": evidence.get("failed_sample_count"),
            "observed_sample_count": evidence.get("observed_sample_count"),
            "expected_sample_count": evidence.get("expected_sample_count"),
        },
    }
    blockers = tuple(
        dict.fromkeys([*gate.blockers, *(() if passed_check else (check_name,))])
    )
    passed = gate.passed and passed_check
    return ProfitabilityGateResult(
        profitability_gate="PASSED" if passed else "FAILED",
        stage="development_validated" if passed else "rejected",
        candidate_count=0,
        live_count=0,
        checks=checks,
        metrics={
            **gate.metrics,
            f"{check_name}_status": evidence.get("status", "MISSING"),
        },
        blockers=blockers,
    )


def _require_candidate_evidence(
    gate: ProfitabilityGateResult,
    *,
    check_name: str,
    passed_check: bool,
) -> ProfitabilityGateResult:
    checks = {
        **gate.checks,
        check_name: {"passed": bool(passed_check), "required": True},
    }
    blockers = tuple(
        dict.fromkeys([*gate.blockers, *(() if passed_check else (check_name,))])
    )
    passed = gate.passed and passed_check
    return ProfitabilityGateResult(
        profitability_gate="PASSED" if passed else "FAILED",
        stage="candidate" if passed else "rejected",
        candidate_count=1 if passed else 0,
        live_count=0,
        checks=checks,
        metrics={**gate.metrics, f"{check_name}_passed": bool(passed_check)},
        blockers=blockers,
    )


def _run_production_replay(
    *,
    source: KlinePanelSource,
    max_bars_per_symbol: int,
    release_datasets: Mapping[int, HorizonDataset],
    final_models: Mapping[int, TwoStageAlphaModel],
    model_feature_columns_by_horizon: Mapping[int, Sequence[str]],
    model_bundle_path: Path,
    trad_panel_evidence: Mapping[str, object] | None,
    bybit_pit_store_path: Path | None,
    macro_pit_store_path: Path | None,
    flow_pit_store_path: Path | None,
    pit_snapshot_watermarks: Mapping[str, Mapping[str, int | None]],
) -> dict[str, object]:
    mode_by_horizon = {horizon: mode for mode, horizon in MODE_HORIZONS.items()}
    samples: list[dict[str, object]] = []
    categorical = {"symbol", "side", "session", "regime"}
    for horizon in HORIZONS_SEC:
        dataset = release_datasets.get(horizon)
        if dataset is None:
            for symbol in SYMBOLS:
                samples.append(
                    {
                        "horizon_sec": horizon,
                        "symbol": symbol,
                        "passed": False,
                        "reason": "direct_execution_outer_oos_dataset_missing",
                    }
                )
            continue
        outer_positions = sorted(
            {
                int(position)
                for fold in dataset.folds
                for position in fold.test_indices
            }
        )
        outer_oos = dataset.development.iloc[outer_positions]
        features = list(model_feature_columns_by_horizon[horizon])
        model = final_models[horizon]
        for symbol in SYMBOLS:
            sample_result: dict[str, object] = {
                "horizon_sec": horizon,
                "symbol": symbol,
                "passed": False,
            }
            try:
                symbol_oos = outer_oos[
                    outer_oos["symbol"].astype(str).str.upper() == symbol
                ].sort_values("decision_at")
                selected_group: pd.DataFrame | None = None
                for _, group in symbol_oos.groupby("decision_at", sort=True):
                    if set(group["side"].astype(str).str.upper()) == {"BUY", "SELL"}:
                        selected_group = group.copy()
                        break
                if selected_group is None:
                    raise ValueError("no paired outer-OOS replay decision")
                side_order = pd.Categorical(
                    selected_group["side"].astype(str).str.upper(),
                    categories=["BUY", "SELL"],
                    ordered=True,
                )
                offline_rows = (
                    selected_group.assign(__side_order=side_order)
                    .sort_values("__side_order")
                    .drop(columns="__side_order")
                    .reset_index(drop=True)
                )
                decision_at = pd.to_datetime(
                    offline_rows.loc[0, "decision_at"], utc=True, errors="raise"
                ).to_pydatetime()
                raw = source.load_before(
                    symbol,
                    HORIZON_TIMEFRAME[horizon],
                    max_bars_per_symbol,
                    close_at_or_before=decision_at,
                )
                external_context = _external_replay_context(
                    offline_rows.iloc[0],
                    model_feature_columns=features,
                    trad_panel_evidence=trad_panel_evidence,
                )
                runtime_rows, feature_evidence = build_current_feature_rows(
                    raw,
                    symbol=symbol,
                    horizon_sec=horizon,
                    model_feature_columns=features,
                    latest_decision_at=decision_at,
                    external_panel_context=external_context,
                    bybit_pit_store_path=bybit_pit_store_path,
                    macro_pit_store_path=macro_pit_store_path,
                    flow_pit_store_path=flow_pit_store_path,
                    pit_snapshot_watermarks=pit_snapshot_watermarks,
                )
                feature_mismatches: dict[str, object] = {}
                maximum_numeric_difference = 0.0
                for column in features:
                    if column in categorical:
                        offline_values = offline_rows[column].astype(str).tolist()
                        runtime_values = runtime_rows[column].astype(str).tolist()
                        if offline_values != runtime_values:
                            feature_mismatches[column] = {
                                "offline": offline_values,
                                "runtime": runtime_values,
                            }
                    else:
                        offline_values = pd.to_numeric(
                            offline_rows[column], errors="coerce"
                        ).to_numpy(float)
                        runtime_values = pd.to_numeric(
                            runtime_rows[column], errors="coerce"
                        ).to_numpy(float)
                        difference = float(
                            np.max(np.abs(offline_values - runtime_values))
                        )
                        maximum_numeric_difference = max(
                            maximum_numeric_difference, difference
                        )
                        if not np.isfinite(difference) or difference > 1e-10:
                            feature_mismatches[column] = {
                                "maximum_absolute_difference": difference
                            }
                offline_side, offline_prediction = select_directional_prediction(
                    model.predict(offline_rows)
                )
                runtime_side, runtime_prediction = select_directional_prediction(
                    model.predict(runtime_rows)
                )
                offline_values = _prediction_values(offline_prediction)
                runtime_values = _prediction_values(runtime_prediction)
                prediction_mismatches: dict[str, object] = {}
                for name, expected in offline_values.items():
                    observed = runtime_values[name]
                    if isinstance(expected, float):
                        if abs(expected - float(observed)) > 1e-10:
                            prediction_mismatches[name] = {
                                "offline": expected,
                                "runtime": observed,
                            }
                    elif observed != expected:
                        prediction_mismatches[name] = {
                            "offline": expected,
                            "runtime": observed,
                        }
                runtime_output = generate_profitability_alpha_prediction(
                    raw,
                    symbol=symbol,
                    mode=mode_by_horizon[horizon],
                    input_price_source=(
                        "bybit_linear_last_trade_kline"
                        if source.source == "bybit"
                        else "binance_futures"
                    ),
                    latest_decision_at=decision_at,
                    external_panel_context=external_context,
                    bybit_pit_store_path=bybit_pit_store_path,
                    macro_pit_store_path=macro_pit_store_path,
                    flow_pit_store_path=flow_pit_store_path,
                    model_bundle_path=model_bundle_path,
                    pit_snapshot_watermarks=pit_snapshot_watermarks,
                )
                output_mismatches: dict[str, object] = {}
                expected_output = {
                    "status": "ok",
                    "release_stage": "rejected",
                    "profitability_gate": "FAILED",
                    "actionable": False,
                    "side": offline_side,
                    "decision": offline_values["decision"],
                    "shadow_actionable": offline_values["shadow_actionable"],
                    "p_down": offline_values["p_down"],
                    "p_flat": offline_values["p_flat"],
                    "p_up": offline_values["p_up"],
                    "expected_net_return": offline_values[
                        "expected_net_return"
                    ],
                    "expected_net_return_bps": float(
                        offline_values["expected_net_return"]
                    )
                    * 10_000,
                    "expected_mae_bps": float(offline_values["expected_mae"])
                    * 10_000,
                    "expected_mfe_bps": float(offline_values["expected_mfe"])
                    * 10_000,
                    "lower_bound_net_edge_bps": float(
                        offline_values["lower_bound_net_edge"]
                    )
                    * 10_000,
                    "meta_trade_probability": offline_values[
                        "meta_trade_probability"
                    ],
                    "uncertainty": offline_values["uncertainty"],
                }
                for name, expected in expected_output.items():
                    observed = runtime_output.get(name)
                    if isinstance(expected, float):
                        matches = observed is not None and abs(
                            expected - float(observed)
                        ) <= 1e-10
                    else:
                        matches = observed == expected
                    if not matches:
                        output_mismatches[name] = {
                            "offline": expected,
                            "production": observed,
                        }
                production_quantiles = runtime_output.get("return_quantiles_bps")
                if not isinstance(production_quantiles, Mapping):
                    output_mismatches["return_quantiles_bps"] = {
                        "offline": "p10/p50/p90",
                        "production": production_quantiles,
                    }
                else:
                    for quantile in ("p10", "p50", "p90"):
                        expected = float(offline_values[f"return_{quantile}"]) * 10_000
                        observed = production_quantiles.get(quantile)
                        if observed is None or abs(expected - float(observed)) > 1e-10:
                            output_mismatches[f"return_quantiles_bps.{quantile}"] = {
                                "offline": expected,
                                "production": observed,
                            }
                production_feature_evidence = runtime_output.get("feature_evidence")
                production_feature_hash = (
                    production_feature_evidence.get("feature_snapshot_sha256")
                    if isinstance(production_feature_evidence, Mapping)
                    else None
                )
                if production_feature_hash != feature_evidence.get(
                    "feature_snapshot_sha256"
                ):
                    output_mismatches["feature_snapshot_sha256"] = {
                        "offline_replay": feature_evidence.get(
                            "feature_snapshot_sha256"
                        ),
                        "production": production_feature_hash,
                    }
                sample_result.update(
                    {
                        "decision_at": decision_at,
                        "feature_snapshot_sha256": feature_evidence.get(
                            "feature_snapshot_sha256"
                        ),
                        "maximum_numeric_feature_difference": (
                            maximum_numeric_difference
                        ),
                        "feature_mismatches": feature_mismatches,
                        "offline_selected_side": offline_side,
                        "runtime_selected_side": runtime_side,
                        "prediction_mismatches": prediction_mismatches,
                        "production_output_mismatches": output_mismatches,
                        "production_authorization_expected": False,
                        "passed": not (
                            feature_mismatches
                            or prediction_mismatches
                            or output_mismatches
                            or runtime_side != offline_side
                        ),
                    }
                )
            except Exception as exc:
                sample_result["reason"] = f"{type(exc).__name__}: {exc}"
            samples.append(sample_result)
    return production_replay_evidence(
        samples,
        expected_horizons=HORIZONS_SEC,
        expected_symbols=SYMBOLS,
    )


_engineer_features = engineer_profitability_features


def _market_bars(frame: pd.DataFrame) -> list[MarketBar]:
    bars: list[MarketBar] = []
    for row in frame.itertuples(index=False):
        raw_price_source = str(getattr(row, "source", "unknown_proxy")).lower()
        price_source = (
            f"{raw_price_source}_last_trade_kline"
            if raw_price_source in {"bybit", "binance"}
            else raw_price_source
        )
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
                price_source=price_source,
                price_observed=raw_price_source in {"bybit", "binance"},
            )
        )
    return bars


def _maximum_execution_window_observed(
    path: Sequence[MarketBar],
    *,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    """Return whether continuous OHLC observations cover the whole window.

    ``available_at`` can legitimately be later than a bar close when settled
    funding provenance arrives later. It controls PIT label availability, but
    must never be used to pretend that the OHLC price path extends further.
    """

    path_cursor = window_start
    for path_bar in path:
        # Consecutive candles may meet at the same instant or have a
        # sub-second exchange boundary. A larger hole is incomplete history.
        if path_bar.open_time > path_cursor + timedelta(seconds=1):
            return False
        path_cursor = max(path_cursor, path_bar.close_time)
        if path_cursor >= window_end:
            return True
    return False


def _panel_frame(
    frame: pd.DataFrame,
    horizon_sec: int,
    bars: Sequence[MarketBar],
    *,
    decision_at_or_after: datetime | None = None,
    decision_before: datetime | None = None,
) -> pd.DataFrame:
    feature_names = TECHNICAL_FEATURE_COLUMNS + LEGACY_BRAIN_FEATURE_COLUMNS
    required_engineered = set(feature_names) | {"volatility", "liquidity"}
    # The rebuild already engineers each symbol before creating execution bars.
    # Reuse that immutable frame instead of duplicating every numeric column and
    # recalculating all rolling indicators at peak memory.  Direct callers that
    # supply raw OHLCV retain the same public behaviour.
    features = (
        frame
        if required_engineered.issubset(frame.columns)
        else _engineer_features(frame)
    )
    regimes = causal_regime_labels(
        features.rename(columns={"close_at": "decision_at"})
    )
    output: dict[str, list[object]] = {}

    def append_payload(payload: Mapping[str, object]) -> None:
        if not output:
            output.update({name: [] for name in payload})
        if len(output) != len(payload):
            raise RuntimeError("panel payload schema changed within one horizon")
        for name, values in output.items():
            values.append(payload[name])

    next_allowed_decision_at: datetime | None = None
    for index in range(48, len(features) - 1):
        row = features.iloc[index]
        if row[list(feature_names) + ["volatility", "liquidity"]].isna().any():
            continue
        signal_at = row["close_at"].to_pydatetime()
        if decision_at_or_after is not None and signal_at < decision_at_or_after:
            continue
        if decision_before is not None and signal_at >= decision_before:
            break
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
        maximum_execution_window_observed = _maximum_execution_window_observed(
            path,
            window_start=signal_at,
            window_end=path_end_time,
        )
        # Release evidence is selected before labels/outcomes are inspected.
        # Requiring the entire maximum execution window prevents a profitable
        # early exit from being more likely to enter the evidence-complete
        # sample than a losing/max-hold path.
        execution_window_evidence_complete = (
            maximum_execution_window_observed
            and all(
                bar.spread_observed
                and bar.depth_observed
                and bar.close_spread_observed is True
                and bar.close_depth_observed is True
                and bar.funding_observed
                for bar in path
            )
        )
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
        # An incomplete holding path is not a zero-return observation.  It has
        # no valid target and must not contaminate either level of the model.
        if any(label.exit_reason == "NO_EXIT_OBSERVATION" for label in labels.values()):
            continue
        long_net = labels["BUY"].net_return
        short_net = labels["SELL"].net_return
        if max(long_net, short_net) <= 0:
            direction_label = "flat"
        else:
            direction_label = "up" if long_net >= short_net else "down"
        hour = signal_at.hour
        session = "asia" if hour < 8 else "europe" if hour < 16 else "americas"
        regime = str(regimes.iloc[index])
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
                "maximum_execution_window_observed": (
                    maximum_execution_window_observed
                ),
                "execution_window_evidence_complete": (
                    execution_window_evidence_complete
                ),
                "execution_cost_evidence_complete": (
                    label.execution_cost_evidence_complete
                ),
                "entry_spread_source": label.entry_spread_source,
                "entry_depth_source": label.entry_depth_source,
                "exit_spread_source": label.exit_spread_source,
                "exit_depth_source": label.exit_depth_source,
                "funding_source": label.funding_source,
                "stop_loss_bps": stop_bps,
                "take_profit_bps": take_profit_bps,
            }
            payload.update({name: float(row[name]) for name in feature_names})
            append_payload(payload)
    return pd.DataFrame(output)


def _panel_rows(
    frame: pd.DataFrame,
    horizon_sec: int,
    bars: Sequence[MarketBar],
    *,
    decision_at_or_after: datetime | None = None,
    decision_before: datetime | None = None,
) -> list[dict[str, object]]:
    """Compatibility wrapper for diagnostics and small focused tests."""

    return _panel_frame(
        frame,
        horizon_sec,
        bars,
        decision_at_or_after=decision_at_or_after,
        decision_before=decision_before,
    ).to_dict(orient="records")


def _build_direct_release_dataset(
    splitter: PooledPanelBuilder,
    panel: pd.DataFrame,
    horizon_sec: int,
    *,
    lockbox_start: object,
) -> tuple[HorizonDataset | None, dict[str, object]]:
    """Build release folds from outcome-independent direct-cost evidence only.

    ``execution_window_evidence_complete`` is computed from the full maximum
    execution window before Triple Barrier outcomes are inspected. Keeping the
    filtering here prevents later callers from selecting rows by realized
    exit, return, MAE or MFE.
    """

    evidence_column = "execution_window_evidence_complete"
    if evidence_column not in panel.columns:
        raise ValueError(f"release panel is missing {evidence_column}")
    direct_mask = panel[evidence_column].fillna(False).astype(bool)
    direct_panel = panel.loc[direct_mask].reset_index(drop=True)
    evidence: dict[str, object] = {
        "selection_policy": (
            "full_maximum_execution_window_direct_before_outcome_filtering"
        ),
        "selection_columns": [evidence_column],
        "outcome_dependent_selection": False,
        "all_panel_rows": len(panel),
        "direct_window_rows": len(direct_panel),
        "direct_window_ratio": (
            len(direct_panel) / len(panel) if len(panel) else 0.0
        ),
        "release_walk_forward_ready": False,
    }
    try:
        dataset = splitter.build_sealed_development(
            direct_panel,
            horizon_sec,
            lockbox_start=lockbox_start,
        )
    except ValueError as exc:
        expected_evidence_blockers = {
            "pooled panel requires at least two symbols",
            "sealed development panel is too small",
            "no valid sealed-development walk-forward folds",
        }
        if str(exc) not in expected_evidence_blockers:
            raise
        evidence["blocker"] = f"{type(exc).__name__}: {exc}"
        return None, evidence
    evidence.update(
        {
            "release_walk_forward_ready": True,
            "release_development_rows": len(dataset.development),
            "release_fold_count": len(dataset.folds),
            "release_development_fingerprint": dataset.development_fingerprint,
        }
    )
    return dataset, evidence


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


def _ablation_signals_from_predictions(
    frame: pd.DataFrame,
    predictions: Sequence[TwoStagePrediction],
    horizon_sec: int,
    *,
    minimum_score: float,
) -> list[SignalEvent]:
    """Apply a training-calibrated research threshold without relaxing release gates.

    A factor can improve ranking while every candidate still has a negative
    deployable expectancy lower bound.  Reusing the production TRADE gate in
    that situation makes the ablation degenerate to zero observations.  This
    The caller must calibrate ``minimum_score`` before the outer OOS interval.
    The OOS score distribution is never used to decide how many signals to
    retain. Signals keep their true (possibly negative) lower bound and remain
    ineligible for release or ticketing.
    """

    if len(frame) != len(predictions):
        raise ValueError("prediction and frame lengths differ")
    candidates = frame.copy().reset_index(drop=True)
    candidates["_prediction"] = list(predictions)
    ranked: list[tuple[float, str, pd.Series, TwoStagePrediction]] = []
    for (symbol, decision_at), group in candidates.groupby(
        ["symbol", "decision_at"], sort=True
    ):
        paired: list[tuple[float, str, pd.Series, TwoStagePrediction]] = []
        for _, row in group.iterrows():
            prediction = row["_prediction"]
            direction_ok = (
                row["side"] == "BUY" and prediction.p_up >= prediction.p_down
            ) or (
                row["side"] == "SELL" and prediction.p_down >= prediction.p_up
            )
            if not direction_ok:
                continue
            score = float(
                prediction.expected_net_return
                - ABLATION_RESEARCH_TAIL_PENALTY * prediction.expected_mae
            )
            stable_key = f"{symbol}|{pd.Timestamp(decision_at).isoformat()}|{row['side']}"
            paired.append((score, stable_key, row, prediction))
        if paired:
            ranked.append(max(paired, key=lambda item: (item[0], item[1])))
    if not ranked:
        return []
    selected = [item for item in ranked if item[0] >= minimum_score]
    signals: list[SignalEvent] = []
    for score, stable_key, row, prediction in selected:
        token = hashlib.sha256(
            f"ablation|{horizon_sec}|{stable_key}".encode()
        ).hexdigest()[:20]
        signals.append(
            SignalEvent(
                signal_id=f"ablation_{token}",
                symbol=str(row["symbol"]),
                side=str(row["side"]),
                decision_at=pd.Timestamp(row["decision_at"]).to_pydatetime(),
                reference_price=float(row["reference_price"]),
                lower_bound_net_edge=float(prediction.lower_bound_net_edge),
                take_profit_bps=float(row["take_profit_bps"]),
                stop_loss_bps=float(row["stop_loss_bps"]),
                max_holding_sec=horizon_sec,
                feature_available_at=(
                    pd.Timestamp(row["available_at"]).to_pydatetime(),
                ),
                max_wait_sec=max(30, min(300, horizon_sec // 2)),
                regime=str(row["regime"]),
                market_key=f"{row['symbol']}:{horizon_sec}",
            )
        )
    return signals


def _ablation_oof_threshold(selection: object) -> float:
    """Require a cutoff calibrated exclusively from inner walk-forward OOS."""

    threshold = getattr(selection, "oof_score_threshold", None)
    if threshold is None or not np.isfinite(float(threshold)):
        raise ValueError("ablation is missing an inner OOS score threshold")
    return float(threshold)


def _horizon_scoped_ablation_result(
    result: Mapping[str, object],
    applicable_horizons: Sequence[int],
) -> dict[str, object]:
    """Prevent a factor that worked at one horizon leaking into another."""

    output = dict(result)
    fold_evidence = [
        dict(item)
        for item in list(output.get("folds") or [])
        if isinstance(item, Mapping)
    ]
    horizon_results: dict[str, dict[str, object]] = {}
    for horizon in applicable_horizons:
        horizon_folds = [
            item
            for item in fold_evidence
            if int(item.get("horizon_sec", -1)) == horizon
        ]
        evaluated_folds = [
            item
            for item in horizon_folds
            if item.get("status") == "EVALUATED_OOS"
            and isinstance(item.get("baseline_execution"), Mapping)
            and isinstance(item.get("augmented_execution"), Mapping)
        ]
        baseline = [dict(item["baseline_execution"]) for item in evaluated_folds]
        augmented = [dict(item["augmented_execution"]) for item in evaluated_folds]
        common = {
            "horizon_sec": horizon,
            "factor_group": str(output["factor_group"]),
            "factors": list(output.get("factors") or []),
            "pit_observation_count": sum(
                int(item.get("test_rows", 0)) for item in horizon_folds
            ),
            "scheduled_oos_fold_count": len(horizon_folds),
            "oos_fold_count": len(evaluated_folds),
            "unevaluated_oos_fold_count": (
                len(horizon_folds) - len(evaluated_folds)
            ),
            "folds": horizon_folds,
        }
        if len(horizon_folds) < MINIMUM_ABLATION_TRADED_FOLDS:
            horizon_results[str(horizon)] = {
                **common,
                "oos_ablation_status": "FAILED_INSUFFICIENT_HORIZON_OOS_FOLDS",
                "evaluated": False,
                "retained": False,
                "formal_feature_set": False,
            }
            continue
        if len(evaluated_folds) != len(horizon_folds):
            horizon_results[str(horizon)] = {
                **common,
                "oos_ablation_status": "FAILED_INCOMPLETE_HORIZON_OOS_FOLDS",
                "evaluated": False,
                "retained": False,
                "formal_feature_set": False,
            }
            continue
        execution_evidence = _ablation_execution_evidence(baseline, augmented)
        if not bool(execution_evidence["passed"]):
            horizon_results[str(horizon)] = {
                **common,
                "oos_ablation_status": (
                    "FAILED_INSUFFICIENT_OOS_TRADES"
                    if not bool(execution_evidence["trade_sample_complete"])
                    else "FAILED_INCOMPLETE_EXECUTION_EVIDENCE"
                ),
                "evaluated": False,
                "execution_evidence": execution_evidence,
                "retained": False,
                "formal_feature_set": False,
            }
            continue
        comparison = compare_factor_groups(
            baseline,
            {str(output["factor_group"]): augmented},
            primary_metric="net_return",
            higher_is_better=True,
            minimum_mean_improvement=0.0,
            minimum_improved_fold_ratio=0.60,
            minimum_worst_fold_improvement=-0.002,
        )[0]
        horizon_results[str(horizon)] = {
            **common,
            "oos_ablation_status": "EVALUATED_OOS",
            "evaluated": True,
            "execution_evidence": execution_evidence,
            "metric": comparison.metric,
            "baseline_mean": comparison.baseline_mean,
            "augmented_mean": comparison.augmented_mean,
            "mean_improvement": comparison.mean_improvement,
            "bootstrap_lower_mean_improvement": (
                comparison.bootstrap_lower_mean_improvement
            ),
            "bootstrap_confidence": comparison.bootstrap_confidence,
            "bootstrap_samples": comparison.bootstrap_samples,
            "improved_fold_ratio": comparison.improved_fold_ratio,
            "worst_fold_improvement": comparison.worst_fold_improvement,
            "retained": comparison.retained,
            "formal_feature_set": comparison.retained,
        }
    all_horizons_evaluated = all(
        item["oos_ablation_status"] == "EVALUATED_OOS"
        for item in horizon_results.values()
    )
    retained_horizons = [
        int(horizon)
        for horizon, item in horizon_results.items()
        if bool(item.get("retained"))
    ]
    if all_horizons_evaluated:
        output["oos_ablation_status"] = "EVALUATED_OOS"
    elif any(bool(item.get("evaluated")) for item in horizon_results.values()):
        output["oos_ablation_status"] = "FAILED_INCOMPLETE_HORIZON_ABLATION"
    output["evaluated"] = all_horizons_evaluated
    output["aggregate_metrics_are_diagnostic_only"] = True
    output["all_applicable_horizons_evaluated"] = all_horizons_evaluated
    output["applicable_horizons"] = list(applicable_horizons)
    output["horizon_results"] = horizon_results
    output["retained_horizons"] = retained_horizons
    output["retained"] = bool(retained_horizons)
    output["formal_feature_set"] = (
        all_horizons_evaluated and bool(retained_horizons)
    )
    return output


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
                applicable_horizons = (
                    HORIZONS_SEC
                    if cadence == "all_horizons"
                    else (180, 900)
                    if cadence == "short"
                    else (7200, 14400, 86400)
                )
                groups.append(
                    _horizon_scoped_ablation_result(
                        evaluated[group], applicable_horizons
                    )
                )
                continue
            applicable_horizons = (
                HORIZONS_SEC
                if cadence == "all_horizons"
                else (180, 900)
                if cadence == "short"
                else (7200, 14400, 86400)
            )
            groups.append(
                _horizon_scoped_ablation_result({
                    "cadence": cadence,
                    "factor_group": group,
                    "factors": list(factors),
                    "oos_ablation_status": "FAILED_DATA_UNAVAILABLE",
                    "evaluated": False,
                    "pit_observation_count": 0,
                    "oos_fold_count": 0,
                    "retained": False,
                    "formal_feature_set": False,
                }, applicable_horizons)
            )
    all_evaluated = all(
        item.get("oos_ablation_status") == "EVALUATED_OOS" for item in groups
    )
    return {
        "method": (
            "identical purged walk-forward folds; fee-adjusted event-driven net return; "
            "paired moving-block bootstrap lower improvement bound"
        ),
        "baseline": "price_technical_only",
        "research_selection_policy": {
            "scope": "factor_ablation_only_not_release_eligible",
            "ranking": "predicted_net_return_minus_fixed_tail_penalty_times_predicted_mae",
            "selection_fraction": ABLATION_RESEARCH_SELECTION_FRACTION,
            "threshold_calibration": (
                "inner_walk_forward_oos_predictions_only_fixed_before_outer_oos"
            ),
            "tail_penalty": ABLATION_RESEARCH_TAIL_PENALTY,
            "production_lower_bound_gate_relaxed": False,
            "research_backtest_accepts_negative_edge_for_measurement": True,
        },
        "all_required_groups_evaluated": all_evaluated,
        "retained_factor_groups": [
            str(item["factor_group"]) for item in groups if bool(item.get("retained"))
        ],
        "formal_factor_groups": [
            str(item["factor_group"])
            for item in groups
            if bool(item.get("formal_feature_set"))
        ],
        "retained_factor_groups_by_horizon": {
            str(horizon): [
                str(item["factor_group"])
                for item in groups
                if horizon in set(item.get("retained_horizons") or [])
            ]
            for horizon in HORIZONS_SEC
        },
        "groups": groups,
        "blocker": (
            None
            if all_evaluated
            else "one or more required factor groups lacks complete direct-cost OOS ablation evidence"
        ),
    }


def _ablation_fold_metrics(report: object, signal_count: int) -> dict[str, object]:
    return {
        "net_return": float(getattr(report, "net_return", 0.0)),
        "signal_count": int(signal_count),
        "trade_count": len(getattr(report, "trades", ())),
        "direct_execution_cost_trade_count": int(
            getattr(report, "direct_execution_cost_trade_count", 0)
        ),
        "proxy_execution_cost_trade_count": int(
            getattr(report, "proxy_execution_cost_trade_count", 0)
        ),
        "execution_cost_evidence_complete": bool(
            getattr(report, "execution_cost_evidence_complete", False)
        ),
        "simulation_complete": bool(
            getattr(report, "simulation_complete", False)
        ),
        "unresolved_position_count": int(
            getattr(report, "unresolved_position_count", 0)
        ),
        "risk_policy_compliant": bool(
            getattr(report, "risk_policy_compliant", False)
        ),
        "risk_budget_breach_count": int(
            getattr(report, "risk_budget_breach_count", 0)
        ),
    }


def _ablation_execution_evidence(
    baseline_folds: Sequence[Mapping[str, object]],
    augmented_folds: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Require complete, direct-cost OOS executions in both ablation arms."""

    def summarize(folds: Sequence[Mapping[str, object]]) -> dict[str, object]:
        traded = [fold for fold in folds if int(fold.get("trade_count", 0)) > 0]
        return {
            "signals": sum(int(fold.get("signal_count", 0)) for fold in folds),
            "trades": sum(int(fold.get("trade_count", 0)) for fold in folds),
            "traded_folds": len(traded),
            "direct_execution_cost_trades": sum(
                int(fold.get("direct_execution_cost_trade_count", 0))
                for fold in folds
            ),
            "proxy_execution_cost_trades": sum(
                int(fold.get("proxy_execution_cost_trade_count", 0))
                for fold in folds
            ),
            "direct_execution_cost_evidence_complete": bool(traded)
            and all(
                bool(fold.get("execution_cost_evidence_complete", False))
                for fold in traded
            ),
            "simulation_complete": bool(folds)
            and all(bool(fold.get("simulation_complete", False)) for fold in folds),
            "unresolved_position_count": sum(
                int(fold.get("unresolved_position_count", 0)) for fold in folds
            ),
            "risk_policy_compliant": bool(folds)
            and all(bool(fold.get("risk_policy_compliant", False)) for fold in folds),
            "risk_budget_breach_count": sum(
                int(fold.get("risk_budget_breach_count", 0)) for fold in folds
            ),
        }

    baseline = summarize(baseline_folds)
    augmented = summarize(augmented_folds)
    trade_sample_complete = all(
        summary["trades"] >= MINIMUM_ABLATION_OOS_TRADES
        and summary["traded_folds"] >= MINIMUM_ABLATION_TRADED_FOLDS
        for summary in (baseline, augmented)
    )
    direct_cost_evidence_complete = all(
        bool(summary["direct_execution_cost_evidence_complete"])
        and int(summary["proxy_execution_cost_trades"]) == 0
        and int(summary["direct_execution_cost_trades"])
        == int(summary["trades"])
        for summary in (baseline, augmented)
    )
    simulation_complete = all(
        bool(summary["simulation_complete"])
        and int(summary["unresolved_position_count"]) == 0
        for summary in (baseline, augmented)
    )
    risk_policy_compliant = all(
        bool(summary["risk_policy_compliant"])
        and int(summary["risk_budget_breach_count"]) == 0
        for summary in (baseline, augmented)
    )
    blockers = []
    if not trade_sample_complete:
        blockers.append("insufficient_oos_trades")
    if not direct_cost_evidence_complete:
        blockers.append("incomplete_direct_execution_cost_evidence")
    if not simulation_complete:
        blockers.append("incomplete_execution_simulation")
    if not risk_policy_compliant:
        blockers.append("capital_risk_budget_breached")
    return {
        "passed": not blockers,
        "minimum_oos_trades_per_arm": MINIMUM_ABLATION_OOS_TRADES,
        "minimum_traded_folds_per_arm": MINIMUM_ABLATION_TRADED_FOLDS,
        "trade_sample_complete": trade_sample_complete,
        "direct_execution_cost_evidence_complete": direct_cost_evidence_complete,
        "simulation_complete": simulation_complete,
        "risk_policy_compliant": risk_policy_compliant,
        "blockers": blockers,
        "baseline": baseline,
        "augmented": augmented,
    }


def _failed_ablation_execution_result(
    *,
    cadence: str,
    group: str,
    factors: Sequence[str],
    fold_evidence: Sequence[Mapping[str, object]],
    baseline_folds: Sequence[Mapping[str, object]],
    augmented_folds: Sequence[Mapping[str, object]],
    extra: Mapping[str, object] | None = None,
) -> dict[str, object] | None:
    evidence = _ablation_execution_evidence(baseline_folds, augmented_folds)
    if bool(evidence["passed"]):
        return None
    result: dict[str, object] = {
        "cadence": cadence,
        "factor_group": group,
        "factors": list(factors),
        "oos_ablation_status": (
            "FAILED_INSUFFICIENT_OOS_TRADES"
            if not bool(evidence["trade_sample_complete"])
            else "FAILED_INCOMPLETE_EXECUTION_EVIDENCE"
        ),
        "evaluated": False,
        "pit_observation_count": sum(
            int(item.get("test_rows", 0)) for item in fold_evidence
        ),
        "oos_fold_count": len(baseline_folds),
        "execution_evidence": evidence,
        "retained": False,
        "formal_feature_set": False,
        "folds": list(fold_evidence),
    }
    result.update(dict(extra or {}))
    return result


def _evaluate_legacy_technical_ablation(
    datasets: Mapping[int, object],
    market: Mapping[str, Sequence[MarketBar]],
    selector: NestedWalkForwardSelector,
    backtest: EventDrivenBacktest,
    *,
    progress_callback: AblationProgressCallback | None = None,
) -> dict[str, dict[str, object]]:
    """Evaluate reusable legacy Brain indicators on untouched outer folds."""

    group = "legacy_brain_technical"
    columns = LEGACY_FACTOR_GROUPS[group]
    baseline_folds: list[dict[str, object]] = []
    augmented_folds: list[dict[str, object]] = []
    fold_evidence: list[dict[str, object]] = []
    for horizon, dataset in datasets.items():
        for fold in dataset.folds:
            train = dataset.development.iloc[fold.train_indices]
            test = dataset.development.iloc[fold.test_indices]
            train_mask, test_mask, coverage_evidence = _factor_fold_coverage(
                train, test, columns
            )
            eligible_train = train.loc[train_mask].reset_index(drop=True)
            eligible_test = test.loc[test_mask].reset_index(drop=True)
            if not bool(coverage_evidence["complete"]):
                _emit_ablation_progress(
                    progress_callback,
                    factor_group=group,
                    horizon_sec=horizon,
                    fold_id=fold.fold_id,
                    status="FAILED_INCOMPLETE_FACTOR_COVERAGE",
                    train_rows=len(eligible_train),
                    test_rows=len(eligible_test),
                )
                fold_evidence.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "status": "FAILED_INCOMPLETE_FACTOR_COVERAGE",
                        "train_rows": len(eligible_train),
                        "test_rows": len(eligible_test),
                        "factor_coverage": coverage_evidence,
                    }
                )
                continue
            if len(eligible_train) < 100 or len(eligible_test) < 30:
                _emit_ablation_progress(
                    progress_callback,
                    factor_group=group,
                    horizon_sec=horizon,
                    fold_id=fold.fold_id,
                    status="SKIPPED_INSUFFICIENT_PIT_ROWS",
                    train_rows=len(eligible_train),
                    test_rows=len(eligible_test),
                )
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
            _emit_ablation_progress(
                progress_callback,
                factor_group=group,
                horizon_sec=horizon,
                fold_id=fold.fold_id,
                status="STARTED",
                train_rows=len(eligible_train),
                test_rows=len(eligible_test),
            )
            baseline_selection = selector.select_and_fit(
                eligible_train,
                FEATURE_COLUMNS,
                score_calibration_quantile=(
                    1.0 - ABLATION_RESEARCH_SELECTION_FRACTION
                ),
                score_calibration_tail_penalty=ABLATION_RESEARCH_TAIL_PENALTY,
            )
            augmented_selection = selector.select_and_fit(
                eligible_train,
                FEATURE_COLUMNS + tuple(columns),
                score_calibration_quantile=(
                    1.0 - ABLATION_RESEARCH_SELECTION_FRACTION
                ),
                score_calibration_tail_penalty=ABLATION_RESEARCH_TAIL_PENALTY,
            )
            baseline_threshold = _ablation_oof_threshold(baseline_selection)
            augmented_threshold = _ablation_oof_threshold(augmented_selection)
            baseline_predictions = baseline_selection.model.predict(eligible_test)
            augmented_predictions = augmented_selection.model.predict(eligible_test)
            baseline_signals = _ablation_signals_from_predictions(
                eligible_test,
                baseline_predictions,
                horizon,
                minimum_score=baseline_threshold,
            )
            augmented_signals = _ablation_signals_from_predictions(
                eligible_test,
                augmented_predictions,
                horizon,
                minimum_score=augmented_threshold,
            )
            baseline_report = backtest.run(baseline_signals, market)
            augmented_report = backtest.run(augmented_signals, market)
            baseline_metrics = _ablation_fold_metrics(
                baseline_report, len(baseline_signals)
            )
            augmented_metrics = _ablation_fold_metrics(
                augmented_report, len(augmented_signals)
            )
            baseline_folds.append(baseline_metrics)
            augmented_folds.append(augmented_metrics)
            fold_evidence.append(
                {
                    "horizon_sec": horizon,
                    "fold_id": fold.fold_id,
                    "status": "EVALUATED_OOS",
                    "train_rows": len(eligible_train),
                    "test_rows": len(eligible_test),
                    "baseline_signals": len(baseline_signals),
                    "baseline_inner_oos_score_threshold": baseline_threshold,
                    "baseline_trades": len(baseline_report.trades),
                    "baseline_net_return": baseline_report.net_return,
                    "baseline_execution": baseline_metrics,
                    "baseline_prediction_gate": prediction_gate_diagnostics(
                        eligible_test,
                        baseline_predictions,
                        meta_threshold=baseline_selection.selected_config.meta_trade_probability,
                    ),
                    "augmented_signals": len(augmented_signals),
                    "augmented_inner_oos_score_threshold": augmented_threshold,
                    "augmented_trades": len(augmented_report.trades),
                    "augmented_net_return": augmented_report.net_return,
                    "augmented_execution": augmented_metrics,
                    "augmented_prediction_gate": prediction_gate_diagnostics(
                        eligible_test,
                        augmented_predictions,
                        meta_threshold=augmented_selection.selected_config.meta_trade_probability,
                    ),
                }
            )
            _emit_ablation_progress(
                progress_callback,
                factor_group=group,
                horizon_sec=horizon,
                fold_id=fold.fold_id,
                status="COMPLETED",
                train_rows=len(eligible_train),
                test_rows=len(eligible_test),
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
    insufficient_execution = _failed_ablation_execution_result(
        cadence="all_horizons",
        group=group,
        factors=columns,
        fold_evidence=fold_evidence,
        baseline_folds=baseline_folds,
        augmented_folds=augmented_folds,
        extra={
            "source": "legacy Brain causal OHLCV logic without current-snapshot broadcast"
        },
    )
    if insufficient_execution is not None:
        return {group: insufficient_execution}
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
            "execution_evidence": _ablation_execution_evidence(
                baseline_folds, augmented_folds
            ),
            "metric": comparison.metric,
            "baseline_mean": comparison.baseline_mean,
            "augmented_mean": comparison.augmented_mean,
            "mean_improvement": comparison.mean_improvement,
            "bootstrap_lower_mean_improvement": (
                comparison.bootstrap_lower_mean_improvement
            ),
            "bootstrap_confidence": comparison.bootstrap_confidence,
            "bootstrap_samples": comparison.bootstrap_samples,
            "improved_fold_ratio": comparison.improved_fold_ratio,
            "worst_fold_improvement": comparison.worst_fold_improvement,
            "retained": comparison.retained,
            "formal_feature_set": comparison.retained,
            "folds": fold_evidence,
        }
    }


def _evaluate_long_factor_ablation(
    datasets: Mapping[int, object],
    market: Mapping[str, Sequence[MarketBar]],
    selector: NestedWalkForwardSelector,
    backtest: EventDrivenBacktest,
    *,
    factor_groups: Mapping[str, tuple[str, ...]] = LONG_FACTOR_GROUPS,
    progress_callback: AblationProgressCallback | None = None,
) -> dict[str, dict[str, object]]:
    results: dict[str, dict[str, object]] = {}
    long_datasets = {
        horizon: dataset for horizon, dataset in datasets.items() if horizon >= 7200
    }
    for group, required_columns in factor_groups.items():
        columns = tuple(
            column
            for column in required_columns
            if long_datasets
            and all(
                column in dataset.development.columns
                for dataset in long_datasets.values()
            )
        )
        missing_required = tuple(
            column for column in required_columns if column not in columns
        )
        if not columns:
            results[group] = {
                "cadence": "medium_long",
                "factor_group": group,
                "factors": list(required_columns),
                "evaluated_factors": [],
                "missing_required_factors": list(missing_required),
                "oos_ablation_status": "FAILED_DATA_UNAVAILABLE",
                "evaluated": False,
                "pit_observation_count": 0,
                "oos_fold_count": 0,
                "retained": False,
                "formal_feature_set": False,
            }
            continue
        baseline_folds: list[dict[str, object]] = []
        augmented_folds: list[dict[str, object]] = []
        fold_evidence: list[dict[str, object]] = []
        for horizon, dataset in datasets.items():
            if horizon < 7200:
                continue
            for fold in dataset.folds:
                train = dataset.development.iloc[fold.train_indices]
                test = dataset.development.iloc[fold.test_indices]
                train_mask, test_mask, coverage_evidence = _factor_fold_coverage(
                    train, test, columns
                )
                eligible_train = train.loc[train_mask].reset_index(drop=True)
                eligible_test = test.loc[test_mask].reset_index(drop=True)
                if not bool(coverage_evidence["complete"]):
                    _emit_ablation_progress(
                        progress_callback,
                        factor_group=group,
                        horizon_sec=horizon,
                        fold_id=fold.fold_id,
                        status="FAILED_INCOMPLETE_FACTOR_COVERAGE",
                        train_rows=len(eligible_train),
                        test_rows=len(eligible_test),
                    )
                    fold_evidence.append(
                        {
                            "horizon_sec": horizon,
                            "fold_id": fold.fold_id,
                            "status": "FAILED_INCOMPLETE_FACTOR_COVERAGE",
                            "train_rows": len(eligible_train),
                            "test_rows": len(eligible_test),
                            "factor_coverage": coverage_evidence,
                        }
                    )
                    continue
                if len(eligible_train) < 100 or len(eligible_test) < 30:
                    _emit_ablation_progress(
                        progress_callback,
                        factor_group=group,
                        horizon_sec=horizon,
                        fold_id=fold.fold_id,
                        status="SKIPPED_INSUFFICIENT_PIT_ROWS",
                        train_rows=len(eligible_train),
                        test_rows=len(eligible_test),
                    )
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
                _emit_ablation_progress(
                    progress_callback,
                    factor_group=group,
                    horizon_sec=horizon,
                    fold_id=fold.fold_id,
                    status="STARTED",
                    train_rows=len(eligible_train),
                    test_rows=len(eligible_test),
                )
                baseline_selection = selector.select_and_fit(
                    eligible_train,
                    FEATURE_COLUMNS,
                    score_calibration_quantile=(
                        1.0 - ABLATION_RESEARCH_SELECTION_FRACTION
                    ),
                    score_calibration_tail_penalty=ABLATION_RESEARCH_TAIL_PENALTY,
                )
                augmented_selection = selector.select_and_fit(
                    eligible_train,
                    FEATURE_COLUMNS + tuple(columns),
                    score_calibration_quantile=(
                        1.0 - ABLATION_RESEARCH_SELECTION_FRACTION
                    ),
                    score_calibration_tail_penalty=ABLATION_RESEARCH_TAIL_PENALTY,
                )
                baseline_threshold = _ablation_oof_threshold(baseline_selection)
                augmented_threshold = _ablation_oof_threshold(augmented_selection)
                baseline_predictions = baseline_selection.model.predict(eligible_test)
                augmented_predictions = augmented_selection.model.predict(eligible_test)
                baseline_signals = _ablation_signals_from_predictions(
                    eligible_test,
                    baseline_predictions,
                    horizon,
                    minimum_score=baseline_threshold,
                )
                augmented_signals = _ablation_signals_from_predictions(
                    eligible_test,
                    augmented_predictions,
                    horizon,
                    minimum_score=augmented_threshold,
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
                baseline_metrics = _ablation_fold_metrics(
                    baseline_report, len(baseline_signals)
                )
                augmented_metrics = _ablation_fold_metrics(
                    augmented_report, len(augmented_signals)
                )
                baseline_folds.append(baseline_metrics)
                augmented_folds.append(augmented_metrics)
                fold_evidence.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "status": "EVALUATED_OOS",
                        "train_rows": len(eligible_train),
                        "test_rows": len(eligible_test),
                        "baseline_signals": len(baseline_signals),
                        "baseline_inner_oos_score_threshold": baseline_threshold,
                        "baseline_trades": len(baseline_report.trades),
                        "baseline_net_return": baseline_report.net_return,
                        "baseline_execution": baseline_metrics,
                        "baseline_prediction_gate": baseline_gate_diagnostics,
                        "augmented_signals": len(augmented_signals),
                        "augmented_inner_oos_score_threshold": augmented_threshold,
                        "augmented_trades": len(augmented_report.trades),
                        "augmented_net_return": augmented_report.net_return,
                        "augmented_execution": augmented_metrics,
                        "augmented_prediction_gate": augmented_gate_diagnostics,
                    }
                )
                _emit_ablation_progress(
                    progress_callback,
                    factor_group=group,
                    horizon_sec=horizon,
                    fold_id=fold.fold_id,
                    status="COMPLETED",
                    train_rows=len(eligible_train),
                    test_rows=len(eligible_test),
                )
        if len(baseline_folds) < 2:
            results[group] = {
                "cadence": "medium_long",
                "factor_group": group,
                "factors": list(required_columns),
                "evaluated_factors": list(columns),
                "missing_required_factors": list(missing_required),
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
        insufficient_execution = _failed_ablation_execution_result(
            cadence="medium_long",
            group=group,
            factors=required_columns,
            fold_evidence=fold_evidence,
            baseline_folds=baseline_folds,
            augmented_folds=augmented_folds,
            extra={
                "evaluated_factors": list(columns),
                "missing_required_factors": list(missing_required),
            },
        )
        if insufficient_execution is not None:
            results[group] = insufficient_execution
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
        complete_group = not missing_required
        results[group] = {
            "cadence": "medium_long",
            "factor_group": group,
            "factors": list(required_columns),
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
            "execution_evidence": _ablation_execution_evidence(
                baseline_folds, augmented_folds
            ),
            "metric": comparison.metric,
            "baseline_mean": comparison.baseline_mean,
            "augmented_mean": comparison.augmented_mean,
            "mean_improvement": comparison.mean_improvement,
            "bootstrap_lower_mean_improvement": (
                comparison.bootstrap_lower_mean_improvement
            ),
            "bootstrap_confidence": comparison.bootstrap_confidence,
            "bootstrap_samples": comparison.bootstrap_samples,
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
    factor_groups: Mapping[str, tuple[str, ...]] = SHORT_FACTOR_GROUPS,
    minimum_history_days: float = MINIMUM_SHORT_FACTOR_HISTORY_DAYS,
    progress_callback: AblationProgressCallback | None = None,
) -> dict[str, dict[str, object]]:
    """Ablate real short-horizon features only after sufficient PIT history exists."""

    results: dict[str, dict[str, object]] = {}
    coverage = dict(source_evidence.get("feature_coverage") or {})
    live_capture_audits = list(source_evidence.get("live_capture_audits") or [])
    historical_store_requires_import_receipt = bool(
        int(source_evidence.get("historical_archive_file_count", 0)) > 0
        or int(source_evidence.get("historical_api_batch_count", 0)) > 0
    )
    imported_audit_ids = {
        str(item.get("source_audit_id"))
        for item in list(source_evidence.get("pit_imports") or [])
        if isinstance(item, Mapping)
    }
    for group, columns in factor_groups.items():
        collection_evidence = dict(
            SHORT_FACTOR_COLLECTION_EVIDENCE.get(group) or {}
        )
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
        completed_source_days = [
            int(item.get("longest_consecutive_completed_days", 0))
            for item in required_coverage
            if isinstance(item, Mapping)
        ]
        minimum_consecutive_completed_days = (
            min(completed_source_days) if completed_source_days else 0
        )
        required_topic_prefixes = SHORT_FACTOR_LIVE_TOPIC_PREFIXES.get(group, ())
        qualifying_live_audits: list[dict[str, object]] = []
        for raw_audit in live_capture_audits:
            if not isinstance(raw_audit, Mapping):
                continue
            audit = dict(raw_audit)
            audit_symbols = {str(value).upper() for value in audit.get("symbols", [])}
            topic_counts = {
                str(topic): int(count)
                for topic, count in dict(audit.get("topic_counts") or {}).items()
            }
            topic_contracts_complete = all(
                any(
                    topic.startswith(prefix)
                    and topic.endswith(f".{symbol}")
                    and count > 0
                    for topic, count in topic_counts.items()
                )
                for symbol in SYMBOLS
                for prefix in required_topic_prefixes
            )
            continuous_days = float(audit.get("longest_interval_sec", 0.0)) / 86_400.0
            intervals = [
                item
                for item in list(audit.get("intervals") or [])
                if isinstance(item, Mapping)
            ]
            audited_feature_overlap_complete = all(
                any(
                    (
                        min(
                            pd.Timestamp(item["end"]),
                            pd.Timestamp(interval["ended_at"]),
                        )
                        - max(
                            pd.Timestamp(item["start"]),
                            pd.Timestamp(interval["started_at"]),
                        )
                    ).total_seconds()
                    / 86_400.0
                    >= minimum_history_days
                    for interval in intervals
                )
                for item in required_coverage
                if isinstance(item, Mapping)
            )
            if (
                str(audit.get("status")) == "completed"
                and set(SYMBOLS).issubset(audit_symbols)
                and topic_contracts_complete
                and audited_feature_overlap_complete
                and (
                    not historical_store_requires_import_receipt
                    or str(audit.get("audit_id")) in imported_audit_ids
                )
                and (
                    group != "liquidations"
                    or int(audit.get("liquidation_feature_count", 0)) > 0
                )
                and float(audit.get("maximum_gap_sec", math.inf)) <= 90.0
                and continuous_days >= minimum_history_days
            ):
                audit["continuous_coverage_days"] = continuous_days
                qualifying_live_audits.append(audit)
        live_capture_continuity_complete = bool(qualifying_live_audits)
        daily_manifest_continuity_complete = bool(
            completed_source_days
            and minimum_consecutive_completed_days >= math.ceil(minimum_history_days)
        )
        continuity_complete = (
            live_capture_continuity_complete
            if group == "liquidations"
            else (
                daily_manifest_continuity_complete
                or live_capture_continuity_complete
            )
        )
        if (
            missing_contracts
            or minimum_observed_days < minimum_history_days
            or not continuity_complete
        ):
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
                "minimum_consecutive_completed_source_days": (
                    minimum_consecutive_completed_days
                ),
                "daily_manifest_continuity_complete": (
                    daily_manifest_continuity_complete
                ),
                "live_capture_continuity_complete": (
                    live_capture_continuity_complete
                ),
                "qualifying_live_capture_audit_ids": [
                    str(item["audit_id"]) for item in qualifying_live_audits
                ],
                "required_live_topic_prefixes": list(required_topic_prefixes),
                "historical_store_requires_import_receipt": (
                    historical_store_requires_import_receipt
                ),
                "imported_audit_ids": sorted(imported_audit_ids),
                "missing_symbol_feature_contracts": missing_contracts,
                "collection_evidence": collection_evidence,
            }
            continue

        baseline_folds: list[dict[str, object]] = []
        augmented_folds: list[dict[str, object]] = []
        fold_evidence: list[dict[str, object]] = []
        for horizon, dataset in datasets.items():
            if horizon not in {180, 900}:
                continue
            for fold in dataset.folds:
                train = dataset.development.iloc[fold.train_indices]
                test = dataset.development.iloc[fold.test_indices]
                train_mask, test_mask, coverage_evidence = _factor_fold_coverage(
                    train, test, columns
                )
                eligible_train = train.loc[train_mask].reset_index(drop=True)
                eligible_test = test.loc[test_mask].reset_index(drop=True)
                if not bool(coverage_evidence["complete"]):
                    _emit_ablation_progress(
                        progress_callback,
                        factor_group=group,
                        horizon_sec=horizon,
                        fold_id=fold.fold_id,
                        status="FAILED_INCOMPLETE_FACTOR_COVERAGE",
                        train_rows=len(eligible_train),
                        test_rows=len(eligible_test),
                    )
                    fold_evidence.append(
                        {
                            "horizon_sec": horizon,
                            "fold_id": fold.fold_id,
                            "status": "FAILED_INCOMPLETE_FACTOR_COVERAGE",
                            "train_rows": len(eligible_train),
                            "test_rows": len(eligible_test),
                            "factor_coverage": coverage_evidence,
                        }
                    )
                    continue
                if len(eligible_train) < 1_000 or len(eligible_test) < 200:
                    _emit_ablation_progress(
                        progress_callback,
                        factor_group=group,
                        horizon_sec=horizon,
                        fold_id=fold.fold_id,
                        status="SKIPPED_INSUFFICIENT_PIT_ROWS",
                        train_rows=len(eligible_train),
                        test_rows=len(eligible_test),
                    )
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
                _emit_ablation_progress(
                    progress_callback,
                    factor_group=group,
                    horizon_sec=horizon,
                    fold_id=fold.fold_id,
                    status="STARTED",
                    train_rows=len(eligible_train),
                    test_rows=len(eligible_test),
                )
                baseline_selection = selector.select_and_fit(
                    eligible_train,
                    FEATURE_COLUMNS,
                    score_calibration_quantile=(
                        1.0 - ABLATION_RESEARCH_SELECTION_FRACTION
                    ),
                    score_calibration_tail_penalty=ABLATION_RESEARCH_TAIL_PENALTY,
                )
                augmented_selection = selector.select_and_fit(
                    eligible_train,
                    FEATURE_COLUMNS + tuple(columns),
                    score_calibration_quantile=(
                        1.0 - ABLATION_RESEARCH_SELECTION_FRACTION
                    ),
                    score_calibration_tail_penalty=ABLATION_RESEARCH_TAIL_PENALTY,
                )
                baseline_threshold = _ablation_oof_threshold(baseline_selection)
                augmented_threshold = _ablation_oof_threshold(augmented_selection)
                baseline_predictions = baseline_selection.model.predict(eligible_test)
                augmented_predictions = augmented_selection.model.predict(eligible_test)
                baseline_signals = _ablation_signals_from_predictions(
                    eligible_test,
                    baseline_predictions,
                    horizon,
                    minimum_score=baseline_threshold,
                )
                augmented_signals = _ablation_signals_from_predictions(
                    eligible_test,
                    augmented_predictions,
                    horizon,
                    minimum_score=augmented_threshold,
                )
                baseline_report = backtest.run(baseline_signals, market)
                augmented_report = backtest.run(augmented_signals, market)
                baseline_metrics = _ablation_fold_metrics(
                    baseline_report, len(baseline_signals)
                )
                augmented_metrics = _ablation_fold_metrics(
                    augmented_report, len(augmented_signals)
                )
                baseline_folds.append(baseline_metrics)
                augmented_folds.append(augmented_metrics)
                fold_evidence.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "status": "EVALUATED_OOS",
                        "train_rows": len(eligible_train),
                        "test_rows": len(eligible_test),
                        "baseline_signals": len(baseline_signals),
                        "baseline_inner_oos_score_threshold": baseline_threshold,
                        "baseline_trades": len(baseline_report.trades),
                        "baseline_net_return": baseline_report.net_return,
                        "baseline_execution": baseline_metrics,
                        "baseline_prediction_gate": prediction_gate_diagnostics(
                            eligible_test,
                            baseline_predictions,
                            meta_threshold=baseline_selection.selected_config.meta_trade_probability,
                        ),
                        "augmented_signals": len(augmented_signals),
                        "augmented_inner_oos_score_threshold": augmented_threshold,
                        "augmented_trades": len(augmented_report.trades),
                        "augmented_net_return": augmented_report.net_return,
                        "augmented_execution": augmented_metrics,
                        "augmented_prediction_gate": prediction_gate_diagnostics(
                            eligible_test,
                            augmented_predictions,
                            meta_threshold=augmented_selection.selected_config.meta_trade_probability,
                        ),
                    }
                )
                _emit_ablation_progress(
                    progress_callback,
                    factor_group=group,
                    horizon_sec=horizon,
                    fold_id=fold.fold_id,
                    status="COMPLETED",
                    train_rows=len(eligible_train),
                    test_rows=len(eligible_test),
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
                "minimum_required_history_days": minimum_history_days,
                "minimum_observed_history_days": minimum_observed_days,
                "minimum_consecutive_completed_source_days": (
                    minimum_consecutive_completed_days
                ),
                "daily_manifest_continuity_complete": (
                    daily_manifest_continuity_complete
                ),
                "live_capture_continuity_complete": (
                    live_capture_continuity_complete
                ),
                "qualifying_live_capture_audit_ids": [
                    str(item["audit_id"]) for item in qualifying_live_audits
                ],
                "historical_store_requires_import_receipt": (
                    historical_store_requires_import_receipt
                ),
                "imported_audit_ids": sorted(imported_audit_ids),
                "collection_evidence": collection_evidence,
                "folds": fold_evidence,
            }
            continue
        insufficient_execution = _failed_ablation_execution_result(
            cadence="short",
            group=group,
            factors=columns,
            fold_evidence=fold_evidence,
            baseline_folds=baseline_folds,
            augmented_folds=augmented_folds,
            extra={
                "collection_evidence": collection_evidence,
                "minimum_required_history_days": minimum_history_days,
                "minimum_observed_history_days": minimum_observed_days,
                "minimum_consecutive_completed_source_days": (
                    minimum_consecutive_completed_days
                ),
                "daily_manifest_continuity_complete": (
                    daily_manifest_continuity_complete
                ),
                "live_capture_continuity_complete": (
                    live_capture_continuity_complete
                ),
                "qualifying_live_capture_audit_ids": [
                    str(item["audit_id"]) for item in qualifying_live_audits
                ],
                "historical_store_requires_import_receipt": (
                    historical_store_requires_import_receipt
                ),
                "imported_audit_ids": sorted(imported_audit_ids),
            },
        )
        if insufficient_execution is not None:
            results[group] = insufficient_execution
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
            "minimum_required_history_days": minimum_history_days,
            "minimum_observed_history_days": minimum_observed_days,
            "minimum_consecutive_completed_source_days": (
                minimum_consecutive_completed_days
            ),
            "daily_manifest_continuity_complete": (
                daily_manifest_continuity_complete
            ),
            "live_capture_continuity_complete": (
                live_capture_continuity_complete
            ),
            "qualifying_live_capture_audit_ids": [
                str(item["audit_id"]) for item in qualifying_live_audits
            ],
            "historical_store_requires_import_receipt": (
                historical_store_requires_import_receipt
            ),
            "imported_audit_ids": sorted(imported_audit_ids),
            "pit_observation_count": sum(
                int(item.get("test_rows", 0)) for item in fold_evidence
            ),
            "oos_fold_count": len(baseline_folds),
            "execution_evidence": _ablation_execution_evidence(
                baseline_folds, augmented_folds
            ),
            "metric": comparison.metric,
            "baseline_mean": comparison.baseline_mean,
            "augmented_mean": comparison.augmented_mean,
            "mean_improvement": comparison.mean_improvement,
            "bootstrap_lower_mean_improvement": (
                comparison.bootstrap_lower_mean_improvement
            ),
            "bootstrap_confidence": comparison.bootstrap_confidence,
            "bootstrap_samples": comparison.bootstrap_samples,
            "improved_fold_ratio": comparison.improved_fold_ratio,
            "worst_fold_improvement": comparison.worst_fold_improvement,
            "retained": comparison.retained,
            "formal_feature_set": comparison.retained,
            "collection_evidence": collection_evidence,
            "folds": fold_evidence,
        }
    return results


__all__ = tuple(name for name in globals() if not name.startswith("__"))
