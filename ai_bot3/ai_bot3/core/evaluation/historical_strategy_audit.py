from __future__ import annotations

import math
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Iterable, Mapping, Sequence

from .statistical_governance import deflated_sharpe_ratio


@dataclass(frozen=True)
class HistoricalPrediction:
    prediction_id: int
    created_at: int
    settle_at: int
    symbol: str
    timeframe: str
    mode: str
    predicted_return: float
    predicted_direction: str | None
    confidence: float | None
    model_version: str | None
    actual_return: float


@dataclass(frozen=True)
class PortfolioAuditConfig:
    initial_equity_usdt: float = 100_000.0
    target_exposure_pct: float = 0.08
    max_gross_exposure_pct: float = 0.24
    minimum_signal_return: float = 0.0008
    minimum_confidence: float = 0.58
    round_trip_fee_bps: float = 11.0
    round_trip_slippage_bps: float = 6.0
    funding_bps: float = 0.0
    require_recorded_direction: bool = True
    require_confidence: bool = True
    require_model_version: bool = True


def load_settled_predictions(db_path: Path) -> list[HistoricalPrediction]:
    uri = f"file:{Path(db_path).resolve().as_posix()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        rows = connection.execute(
            """SELECT id, created_at, settle_at, symbol, timeframe, mode,
                      predicted_return, predicted_direction, confidence,
                      model_version, actual_return
                 FROM predictions
                WHERE settled=1 AND actual_return IS NOT NULL
                ORDER BY created_at, id"""
        ).fetchall()
    return [
        HistoricalPrediction(
            prediction_id=int(row[0]),
            created_at=int(row[1]),
            settle_at=int(row[2] or (int(row[1]) + 1)),
            symbol=str(row[3]),
            timeframe=str(row[4]),
            mode=str(row[5]),
            predicted_return=float(row[6] or 0.0),
            predicted_direction=str(row[7]).lower() if row[7] else None,
            confidence=float(row[8]) if row[8] is not None else None,
            model_version=str(row[9]) if row[9] else None,
            actual_return=float(row[10]),
        )
        for row in rows
    ]


def _direction(row: HistoricalPrediction, config: PortfolioAuditConfig) -> tuple[int, str | None]:
    recorded = str(row.predicted_direction or "").lower()
    if recorded in {"long", "up", "buy"}:
        return 1, None
    if recorded in {"short", "down", "sell"}:
        return -1, None
    if config.require_recorded_direction:
        return 0, "missing_recorded_direction"
    if abs(row.predicted_return) < config.minimum_signal_return:
        return 0, "below_signal_threshold"
    if row.predicted_return > 0:
        return 1, None
    if row.predicted_return < 0:
        return -1, None
    return 0, "flat_signal"


def _drawdown(equity_curve: Sequence[float]) -> float:
    peak = 0.0
    maximum = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            maximum = max(maximum, (peak - value) / peak)
    return maximum


def _daily_metrics(realizations: Sequence[tuple[int, float]], initial_equity: float, trial_count: int) -> dict[str, Any]:
    daily_pnl: dict[str, float] = defaultdict(float)
    for timestamp, pnl in realizations:
        day = datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat()
        daily_pnl[day] += pnl
    equity = initial_equity
    daily_returns: list[float] = []
    for day in sorted(daily_pnl):
        pnl = daily_pnl[day]
        daily_returns.append(pnl / equity if equity else 0.0)
        equity += pnl
    if len(daily_returns) < 2 or pstdev(daily_returns) <= 1e-12:
        return {
            "observed_days": len(daily_returns),
            "annualized_sharpe": None,
            "deflated_sharpe_probability": None,
        }
    daily_std = pstdev(daily_returns)
    daily_sharpe = mean(daily_returns) / daily_std
    try:
        dsr = deflated_sharpe_ratio(
            daily_sharpe,
            len(daily_returns),
            number_of_trials=max(1, int(trial_count)),
        )
    except ValueError:
        dsr = None
    return {
        "observed_days": len(daily_returns),
        "annualized_sharpe": daily_sharpe * math.sqrt(365.0),
        "deflated_sharpe_probability": dsr,
    }


def audit_portfolio(
    rows: Iterable[HistoricalPrediction],
    config: PortfolioAuditConfig | None = None,
    *,
    trial_count: int = 1,
) -> dict[str, Any]:
    cfg = config or PortfolioAuditConfig()
    ordered = sorted(rows, key=lambda row: (row.created_at, row.prediction_id))
    equity = float(cfg.initial_equity_usdt)
    active: list[dict[str, Any]] = []
    active_keys: set[tuple[str, str, str]] = set()
    realized_curve = [equity]
    realizations: list[tuple[int, float]] = []
    trade_net_returns: list[float] = []
    rejection_reasons: Counter[str] = Counter()
    gross_profit = 0.0
    gross_loss = 0.0

    def settle_due(timestamp: int) -> None:
        nonlocal equity, gross_profit, gross_loss
        due = [trade for trade in active if trade["settle_at"] <= timestamp]
        for trade in sorted(due, key=lambda item: (item["settle_at"], item["prediction_id"])):
            pnl = float(trade["net_pnl"])
            equity += pnl
            realized_curve.append(equity)
            realizations.append((int(trade["settle_at"]), pnl))
            trade_net_returns.append(float(trade["net_return_on_notional"]))
            if pnl >= 0:
                gross_profit += pnl
            else:
                gross_loss += abs(pnl)
            active.remove(trade)
            active_keys.discard(trade["key"])

    for row in ordered:
        settle_due(row.created_at)
        direction, rejection = _direction(row, cfg)
        if rejection:
            rejection_reasons[rejection] += 1
            continue
        if abs(row.predicted_return) < cfg.minimum_signal_return:
            rejection_reasons["below_signal_threshold"] += 1
            continue
        if cfg.require_confidence and (row.confidence is None or row.confidence < cfg.minimum_confidence):
            rejection_reasons["missing_or_low_confidence"] += 1
            continue
        if cfg.require_model_version and not row.model_version:
            rejection_reasons["missing_model_version"] += 1
            continue
        key = (row.symbol, row.timeframe, row.mode)
        if key in active_keys:
            rejection_reasons["overlapping_same_signal"] += 1
            continue
        gross_exposure = sum(float(trade["notional"]) for trade in active) / max(equity, 1e-12)
        remaining = max(0.0, cfg.max_gross_exposure_pct - gross_exposure)
        exposure = min(cfg.target_exposure_pct, remaining)
        if exposure <= 0:
            rejection_reasons["portfolio_exposure_cap"] += 1
            continue
        notional = equity * exposure
        cost_return = (
            cfg.round_trip_fee_bps + cfg.round_trip_slippage_bps + cfg.funding_bps
        ) / 10_000.0
        net_return = direction * row.actual_return - cost_return
        active.append(
            {
                "prediction_id": row.prediction_id,
                "settle_at": max(row.created_at + 1, row.settle_at),
                "key": key,
                "notional": notional,
                "net_return_on_notional": net_return,
                "net_pnl": notional * net_return,
            }
        )
        active_keys.add(key)

    settle_due(2**63 - 1)
    trades = len(trade_net_returns)
    wins = sum(value > 0 for value in trade_net_returns)
    total_return = equity / cfg.initial_equity_usdt - 1.0
    result = {
        "configuration": asdict(cfg),
        "records_seen": len(ordered),
        "eligible_trades": trades,
        "rejections": dict(sorted(rejection_reasons.items())),
        "initial_equity_usdt": cfg.initial_equity_usdt,
        "final_equity_usdt": equity,
        "net_profit_usdt": equity - cfg.initial_equity_usdt,
        "total_return": total_return,
        "realized_close_to_close_max_drawdown": _drawdown(realized_curve),
        "intratrade_drawdown_available": False,
        "win_rate_after_cost": wins / trades if trades else None,
        "profit_factor_after_cost": (
            gross_profit / gross_loss if gross_loss > 0 else None
        ),
        "average_trade_return_after_cost": mean(trade_net_returns) if trades else None,
        "cost_model": {
            "round_trip_fee_bps": cfg.round_trip_fee_bps,
            "round_trip_slippage_bps": cfg.round_trip_slippage_bps,
            "funding_bps": cfg.funding_bps,
        },
        "evidence_limits": [
            "settled prediction rows are not exchange fill receipts",
            "no intrabar path is stored, so intratrade drawdown and stop execution cannot be reconstructed",
            "legacy rows may not contain direction, confidence, model version, calibration, OOD, or source-quality gates",
        ],
    }
    result.update(_daily_metrics(realizations, cfg.initial_equity_usdt, trial_count))
    return result


__all__: Sequence[str] = (
    "HistoricalPrediction",
    "PortfolioAuditConfig",
    "audit_portfolio",
    "load_settled_predictions",
)
