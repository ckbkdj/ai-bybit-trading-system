from __future__ import annotations

import math
from typing import Iterable, Sequence

from .backtest import TradeResult


def prediction_metrics(
    probabilities_up: Sequence[float],
    actual_directions: Sequence[int],
    predicted_returns: Sequence[float],
    actual_returns: Sequence[float],
    *,
    lower_interval: Sequence[float] | None = None,
    upper_interval: Sequence[float] | None = None,
    quantile_predictions: dict[float, Sequence[float]] | None = None,
) -> dict[str, float]:
    if not (
        len(probabilities_up) == len(actual_directions) == len(predicted_returns) == len(actual_returns)
    ) or not probabilities_up:
        raise ValueError("prediction metric arrays must be non-empty and equal length")
    eps = 1e-12
    log_loss = -sum(
        y * math.log(max(eps, min(1 - eps, p)))
        + (1 - y) * math.log(max(eps, min(1 - eps, 1 - p)))
        for p, y in zip(probabilities_up, actual_directions)
    ) / len(probabilities_up)
    brier = sum((p - y) ** 2 for p, y in zip(probabilities_up, actual_directions)) / len(probabilities_up)
    direction_accuracy = sum((p >= 0.5) == bool(y) for p, y in zip(probabilities_up, actual_directions)) / len(probabilities_up)
    mae = sum(abs(p - a) for p, a in zip(predicted_returns, actual_returns)) / len(actual_returns)
    bins = 10
    calibration_error = 0.0
    for bin_index in range(bins):
        low, high = bin_index / bins, (bin_index + 1) / bins
        indices = [
            index for index, value in enumerate(probabilities_up)
            if low <= value < high or (bin_index == bins - 1 and value == 1)
        ]
        if indices:
            mean_probability = sum(probabilities_up[index] for index in indices) / len(indices)
            mean_actual = sum(actual_directions[index] for index in indices) / len(indices)
            calibration_error += len(indices) / len(probabilities_up) * abs(mean_probability - mean_actual)

    positives = [index for index, value in enumerate(actual_directions) if value == 1]
    negatives = [index for index, value in enumerate(actual_directions) if value == 0]
    if positives and negatives:
        wins = sum(
            1.0 if probabilities_up[p] > probabilities_up[n] else 0.5 if probabilities_up[p] == probabilities_up[n] else 0.0
            for p in positives for n in negatives
        )
        auc = wins / (len(positives) * len(negatives))
    else:
        auc = float("nan")
    result = {
        "log_loss": log_loss,
        "brier_score": brier,
        "calibration_error": calibration_error,
        "auc": auc,
        "direction_accuracy": direction_accuracy,
        "mae": mae,
    }
    if lower_interval is not None and upper_interval is not None:
        if len(lower_interval) != len(actual_returns) or len(upper_interval) != len(actual_returns):
            raise ValueError("prediction intervals must match actual returns")
        result["prediction_interval_coverage"] = sum(
            lower <= actual <= upper
            for lower, actual, upper in zip(lower_interval, actual_returns, upper_interval)
        ) / len(actual_returns)
    if quantile_predictions:
        losses = []
        for quantile, predictions in quantile_predictions.items():
            if not 0 < quantile < 1 or len(predictions) != len(actual_returns):
                raise ValueError("invalid quantile predictions")
            for prediction, actual in zip(predictions, actual_returns):
                error = actual - prediction
                losses.append(max(quantile * error, (quantile - 1) * error))
        result["pinball_loss"] = sum(losses) / len(losses)
    return result


def trading_metrics(results: Iterable[TradeResult], initial_equity: float) -> dict[str, float]:
    items = list(results)
    if initial_equity <= 0:
        raise ValueError("initial equity must be positive")
    equity = initial_equity
    peak = equity
    max_drawdown = 0.0
    wins = 0
    gross_profit = 0.0
    gross_loss = 0.0
    fees = 0.0
    per_trade_returns: list[float] = []
    positive_returns: list[float] = []
    negative_returns: list[float] = []
    turnover = 0.0
    slippage_bias = 0.0
    for result in items:
        equity += result.net_pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, (peak - equity) / peak if peak else 0.0)
        wins += result.net_pnl > 0
        gross_profit += max(0.0, result.net_pnl)
        gross_loss += abs(min(0.0, result.net_pnl))
        fees += result.fee + result.slippage_cost + result.funding_cost
        trade_return = result.net_pnl / result.filled_notional if result.filled_notional else 0.0
        per_trade_returns.append(trade_return)
        (positive_returns if trade_return > 0 else negative_returns).append(trade_return)
        turnover += result.filled_notional * 2
        slippage_bias += result.slippage_cost / result.filled_notional if result.filled_notional else 0.0
    mean_return = sum(per_trade_returns) / len(per_trade_returns) if per_trade_returns else 0.0
    variance = (
        sum((value - mean_return) ** 2 for value in per_trade_returns) / len(per_trade_returns)
        if per_trade_returns else 0.0
    )
    downside_variance = (
        sum(value ** 2 for value in negative_returns) / len(negative_returns)
        if negative_returns else 0.0
    )
    return {
        "net_pnl": equity - initial_equity,
        "net_return": equity / initial_equity - 1,
        "max_drawdown": max_drawdown,
        "win_rate": wins / len(items) if items else 0.0,
        "payoff_ratio": (
            (sum(positive_returns) / len(positive_returns))
            / abs(sum(negative_returns) / len(negative_returns))
            if positive_returns and negative_returns else float("inf")
        ),
        "profit_factor": gross_profit / gross_loss if gross_loss else float("inf"),
        "sharpe": mean_return / math.sqrt(variance) if variance > 0 else 0.0,
        "sortino": mean_return / math.sqrt(downside_variance) if downside_variance > 0 else 0.0,
        "turnover": turnover / initial_equity,
        "slippage_bias": slippage_bias / len(items) if items else 0.0,
        "cost_to_gross_profit": fees / gross_profit if gross_profit else float("inf"),
        "trade_count": float(len(items)),
    }
