from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class ProfitabilityThresholds:
    minimum_net_return: float = 0.0
    minimum_profit_factor: float = 1.20
    maximum_drawdown: float = 0.03
    minimum_bootstrap_expectancy: float = 0.0
    maximum_2x_cost_loss: float = 0.005
    minimum_positive_fold_ratio: float = 0.60
    maximum_concentration_share: float = 0.50
    minimum_trades: int = 30
    bootstrap_samples: int = 2000
    bootstrap_seed: int = 20260823


@dataclass(frozen=True)
class ProfitabilityGateResult:
    profitability_gate: str
    stage: str
    candidate_count: int
    live_count: int
    checks: Mapping[str, Mapping[str, object]]
    metrics: Mapping[str, object]
    blockers: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return self.profitability_gate == "PASSED"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _value(item: object, name: str, default: object = None) -> object:
    if isinstance(item, Mapping):
        return item.get(name, default)
    return getattr(item, name, default)


def _maximum_drawdown(pnls: Sequence[float], initial_equity: float) -> float:
    equity = initial_equity
    peak = equity
    maximum = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum


def _bootstrap_lower_expectancy(
    returns: np.ndarray,
    *,
    samples: int,
    seed: int,
) -> float | None:
    if len(returns) < 2:
        return None
    rng = np.random.default_rng(seed)
    block = max(1, int(round(math.sqrt(len(returns)))))
    means = np.empty(samples, dtype=float)
    for index in range(samples):
        selected: list[float] = []
        while len(selected) < len(returns):
            start = int(rng.integers(0, len(returns)))
            selected.extend(returns[(start + offset) % len(returns)] for offset in range(block))
        means[index] = float(np.mean(selected[: len(returns)]))
    return float(np.quantile(means, 0.05))


def _concentration(trades: Sequence[object], key: str) -> tuple[float, str | None]:
    pnl: dict[str, float] = defaultdict(float)
    for trade in trades:
        pnl[str(_value(trade, key, "unknown"))] += max(0.0, float(_value(trade, "net_pnl", 0.0)))
    total = sum(pnl.values())
    if total <= 0:
        return 1.0, None
    group, value = max(pnl.items(), key=lambda item: item[1])
    return value / total, group


def evaluate_profitability_gate(
    lockbox_trades: Iterable[object],
    walk_forward_folds: Sequence[Mapping[str, object]],
    *,
    initial_equity_usdt: float,
    two_x_cost_net_return: float,
    mark_to_market_max_drawdown: float | None = None,
    mark_to_market_evidence_complete: bool = False,
    execution_evidence_complete: bool = True,
    factor_ablation_complete: bool = True,
    thresholds: ProfitabilityThresholds | None = None,
) -> ProfitabilityGateResult:
    cfg = thresholds or ProfitabilityThresholds()
    trades = list(lockbox_trades)
    pnls = [float(_value(trade, "net_pnl", 0.0)) for trade in trades]
    returns = np.asarray([float(_value(trade, "net_return", 0.0)) for trade in trades], dtype=float)
    final_equity = initial_equity_usdt + sum(pnls)
    net_return = final_equity / initial_equity_usdt - 1.0 if initial_equity_usdt > 0 else -1.0
    profit = sum(value for value in pnls if value > 0)
    loss = abs(sum(value for value in pnls if value < 0))
    profit_factor = profit / loss if loss > 0 else None
    profit_factor_passed = (
        profit > 0 and loss == 0
    ) or (
        profit_factor is not None and profit_factor >= cfg.minimum_profit_factor
    )
    profit_factor_actual: float | str = (
        "Infinity" if profit > 0 and loss == 0 else float(profit_factor or 0.0)
    )
    realized_only_drawdown = _maximum_drawdown(pnls, initial_equity_usdt)
    mark_to_market_drawdown = (
        float(mark_to_market_max_drawdown)
        if mark_to_market_max_drawdown is not None
        else None
    )
    drawdown_passed = (
        bool(mark_to_market_evidence_complete)
        and mark_to_market_drawdown is not None
        and 0.0 <= mark_to_market_drawdown <= cfg.maximum_drawdown
    )
    bootstrap_lower = _bootstrap_lower_expectancy(
        returns,
        samples=cfg.bootstrap_samples,
        seed=cfg.bootstrap_seed,
    )
    fold_returns = [float(fold.get("net_return", 0.0)) for fold in walk_forward_folds]
    positive_fold_ratio = (
        sum(value > 0 for value in fold_returns) / len(fold_returns) if fold_returns else 0.0
    )
    symbol_share, symbol_group = _concentration(trades, "symbol")
    month_share, month_group = _concentration(trades, "month")
    regime_share, regime_group = _concentration(trades, "regime")
    maximum_share = max(symbol_share, month_share, regime_share)
    checks: dict[str, dict[str, object]] = {
        "execution_evidence": {
            "passed": bool(execution_evidence_complete),
            "actual": bool(execution_evidence_complete),
            "required": True,
        },
        "factor_ablation": {
            "passed": bool(factor_ablation_complete),
            "actual": bool(factor_ablation_complete),
            "required": True,
        },
        "minimum_trades": {"passed": len(trades) >= cfg.minimum_trades, "actual": len(trades), "threshold": cfg.minimum_trades},
        "lockbox_net_return": {"passed": net_return > cfg.minimum_net_return, "actual": net_return, "threshold": cfg.minimum_net_return},
        "profit_factor": {"passed": profit_factor_passed, "actual": profit_factor_actual, "threshold": cfg.minimum_profit_factor},
        "mark_to_market_drawdown": {
            "passed": drawdown_passed,
            "actual": mark_to_market_drawdown,
            "threshold": cfg.maximum_drawdown,
            "evidence_complete": bool(mark_to_market_evidence_complete),
            "required_method": "portfolio_mark_to_market_at_every_market_observation",
        },
        "bootstrap_lower_expectancy": {
            "passed": bootstrap_lower is not None and bootstrap_lower > cfg.minimum_bootstrap_expectancy,
            "actual": bootstrap_lower,
            "threshold": cfg.minimum_bootstrap_expectancy,
            "confidence": 0.95,
        },
        "two_x_cost_stress": {
            "passed": two_x_cost_net_return >= -cfg.maximum_2x_cost_loss,
            "actual": two_x_cost_net_return,
            "minimum": -cfg.maximum_2x_cost_loss,
        },
        "positive_walk_forward_folds": {
            "passed": positive_fold_ratio >= cfg.minimum_positive_fold_ratio,
            "actual": positive_fold_ratio,
            "threshold": cfg.minimum_positive_fold_ratio,
            "fold_count": len(fold_returns),
        },
        "return_concentration": {
            "passed": maximum_share <= cfg.maximum_concentration_share,
            "actual": maximum_share,
            "threshold": cfg.maximum_concentration_share,
            "symbol": {"share": symbol_share, "largest": symbol_group},
            "month": {"share": month_share, "largest": month_group},
            "regime": {"share": regime_share, "largest": regime_group},
        },
    }
    blockers = tuple(name for name, check in checks.items() if not bool(check["passed"]))
    passed = not blockers
    return ProfitabilityGateResult(
        profitability_gate="PASSED" if passed else "FAILED",
        stage="candidate" if passed else "rejected",
        candidate_count=1 if passed else 0,
        live_count=0,
        checks=checks,
        metrics={
            "initial_equity_usdt": initial_equity_usdt,
            "final_equity_usdt": final_equity,
            "trade_count": len(trades),
            "net_return": net_return,
            "profit_factor": profit_factor_actual,
            "max_drawdown": mark_to_market_drawdown,
            "realized_close_only_drawdown": realized_only_drawdown,
            "bootstrap_lower_expectancy": bootstrap_lower,
            "two_x_cost_net_return": two_x_cost_net_return,
            "positive_walk_forward_fold_ratio": positive_fold_ratio,
            "maximum_return_concentration_share": maximum_share,
        },
        blockers=blockers,
    )


def evaluate_development_gate(
    development_oos_trades: Iterable[object],
    walk_forward_folds: Sequence[Mapping[str, object]],
    **kwargs: object,
) -> ProfitabilityGateResult:
    """Apply the full economic gate before any final lockbox may be opened."""

    result = evaluate_profitability_gate(
        development_oos_trades,
        walk_forward_folds,
        **kwargs,
    )
    checks = dict(result.checks)
    checks["development_oos_net_return"] = checks.pop("lockbox_net_return")
    blockers = tuple(
        "development_oos_net_return" if blocker == "lockbox_net_return" else blocker
        for blocker in result.blockers
    )
    return ProfitabilityGateResult(
        profitability_gate=result.profitability_gate,
        stage="development_validated" if result.passed else "rejected",
        candidate_count=0,
        live_count=0,
        checks=checks,
        metrics={**result.metrics, "evaluation_scope": "development_oos"},
        blockers=blockers,
    )


def write_profitability_report(path: Path, result: ProfitabilityGateResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str), encoding="utf-8")
    temporary.replace(path)


__all__: Sequence[str] = (
    "ProfitabilityGateResult",
    "ProfitabilityThresholds",
    "evaluate_development_gate",
    "evaluate_profitability_gate",
    "write_profitability_report",
)
