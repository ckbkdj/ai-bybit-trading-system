from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from itertools import combinations
from pathlib import Path
from statistics import NormalDist
from typing import Any, Mapping, Sequence

import numpy as np


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def probabilistic_sharpe_ratio(
    observed_sharpe: float,
    benchmark_sharpe: float,
    sample_count: int,
    *,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    """Probability that the population Sharpe exceeds ``benchmark_sharpe``.

    The finite-sample adjustment includes non-normal skew and kurtosis. Sharpe
    values and moments must use the same return frequency.
    """

    if sample_count < 2 or kurtosis < 1:
        raise ValueError("insufficient samples or invalid kurtosis")
    variance_term = 1.0 - skewness * observed_sharpe + ((kurtosis - 1.0) / 4.0) * observed_sharpe**2
    if variance_term <= 0:
        raise ValueError("invalid Sharpe sampling variance")
    z_score = (
        (observed_sharpe - benchmark_sharpe)
        * math.sqrt(sample_count - 1)
        / math.sqrt(variance_term)
    )
    return NormalDist().cdf(z_score)


def expected_maximum_sharpe(number_of_trials: int, sharpe_std: float) -> float:
    """Expected best Sharpe produced by multiple independent zero-edge trials."""

    if number_of_trials <= 0 or sharpe_std < 0:
        raise ValueError("number_of_trials must be positive and sharpe_std non-negative")
    if number_of_trials == 1 or sharpe_std == 0:
        return 0.0
    gamma = 0.5772156649015329
    normal = NormalDist()
    n = float(number_of_trials)
    first = normal.inv_cdf(1.0 - 1.0 / n)
    second = normal.inv_cdf(1.0 - 1.0 / (n * math.e))
    return sharpe_std * ((1.0 - gamma) * first + gamma * second)


def deflated_sharpe_ratio(
    observed_sharpe: float,
    sample_count: int,
    *,
    number_of_trials: int,
    trial_sharpes: Sequence[float] = (),
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    """Selection-bias-adjusted probability that a strategy has positive skill."""

    if trial_sharpes:
        mean = sum(float(value) for value in trial_sharpes) / len(trial_sharpes)
        variance = sum((float(value) - mean) ** 2 for value in trial_sharpes) / len(trial_sharpes)
        sharpe_std = math.sqrt(variance)
    else:
        sharpe_std = 1.0 / math.sqrt(max(1, sample_count - 1))
    benchmark = expected_maximum_sharpe(number_of_trials, sharpe_std)
    return probabilistic_sharpe_ratio(
        observed_sharpe,
        benchmark,
        sample_count,
        skewness=skewness,
        kurtosis=kurtosis,
    )


def _performance_statistic(values: np.ndarray, metric: str) -> float:
    if metric == "mean_return":
        return float(np.mean(values))
    if metric != "sharpe":
        raise ValueError("CSCV metric must be mean_return or sharpe")
    deviation = float(np.std(values, ddof=1))
    mean = float(np.mean(values))
    if deviation <= 1e-15:
        return 0.0 if abs(mean) <= 1e-15 else math.copysign(1e12, mean)
    return mean / deviation


def _average_ranks(values: np.ndarray) -> np.ndarray:
    """Return ascending one-based ranks with exact ties assigned their mean rank."""

    order = np.argsort(values, kind="stable")
    ranks = np.empty(len(values), dtype=float)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        ranks[order[start:end]] = ((start + 1) + end) / 2.0
        start = end
    return ranks


def cscv_probability_of_backtest_overfitting(
    performance_matrix: Sequence[Sequence[float]] | np.ndarray,
    *,
    partitions: int = 8,
    minimum_rows_per_partition: int = 2,
    metric: str = "mean_return",
) -> dict[str, object]:
    """Estimate PBO using combinatorially symmetric cross-validation.

    Rows must be synchronous portfolio return observations and columns must be
    pre-registered strategy variants.  A small leading remainder is excluded
    so every contiguous partition has exactly the same number of rows, as the
    CSCV construction requires.  Ambiguous in-sample winners fail closed
    instead of being resolved with a favorable arbitrary tie-break.
    """

    matrix = np.asarray(performance_matrix, dtype=float)
    if matrix.ndim != 2:
        raise ValueError("CSCV performance matrix must be two-dimensional")
    row_count, strategy_count = matrix.shape
    if partitions < 4 or partitions % 2:
        raise ValueError("CSCV requires an even partition count of at least four")
    if minimum_rows_per_partition < 2:
        raise ValueError("CSCV requires at least two rows per partition")
    if strategy_count < 2:
        raise ValueError("CSCV requires at least two strategy variants")
    if row_count < partitions * minimum_rows_per_partition:
        return {
            "complete": False,
            "reason": "insufficient_synchronous_return_rows",
            "row_count": row_count,
            "strategy_count": strategy_count,
            "partitions": partitions,
            "minimum_rows_per_partition": minimum_rows_per_partition,
        }
    if not np.isfinite(matrix).all():
        return {
            "complete": False,
            "reason": "non_finite_synchronous_returns",
            "row_count": row_count,
            "strategy_count": strategy_count,
            "partitions": partitions,
        }

    excluded_leading_rows = row_count % partitions
    usable = matrix[excluded_leading_rows:]
    rows_per_partition = len(usable) // partitions
    blocks = tuple(
        usable[index * rows_per_partition : (index + 1) * rows_per_partition]
        for index in range(partitions)
    )
    logits: list[float] = []
    selected_is_performance: list[float] = []
    selected_oos_performance: list[float] = []
    oos_loss_count = 0
    half = partitions // 2
    all_blocks = set(range(partitions))
    for in_sample_blocks in combinations(range(partitions), half):
        out_of_sample_blocks = tuple(sorted(all_blocks.difference(in_sample_blocks)))
        in_sample = np.concatenate([blocks[index] for index in in_sample_blocks], axis=0)
        out_of_sample = np.concatenate(
            [blocks[index] for index in out_of_sample_blocks], axis=0
        )
        in_sample_scores = np.asarray(
            [
                _performance_statistic(in_sample[:, column], metric)
                for column in range(strategy_count)
            ],
            dtype=float,
        )
        maximum = float(np.max(in_sample_scores))
        winners = np.flatnonzero(in_sample_scores == maximum)
        if len(winners) != 1:
            return {
                "complete": False,
                "reason": "ambiguous_in_sample_strategy_winner",
                "row_count": row_count,
                "usable_row_count": len(usable),
                "strategy_count": strategy_count,
                "partitions": partitions,
                "combination_count": math.comb(partitions, half),
            }
        selected = int(winners[0])
        out_of_sample_scores = np.asarray(
            [
                _performance_statistic(out_of_sample[:, column], metric)
                for column in range(strategy_count)
            ],
            dtype=float,
        )
        relative_rank = float(_average_ranks(out_of_sample_scores)[selected]) / (
            strategy_count + 1.0
        )
        logit = math.log(relative_rank / (1.0 - relative_rank))
        logits.append(logit)
        selected_is_performance.append(float(in_sample_scores[selected]))
        selected_oos_performance.append(float(out_of_sample_scores[selected]))
        if out_of_sample_scores[selected] < 0:
            oos_loss_count += 1

    pbo = sum(value <= 0.0 for value in logits) / len(logits)
    is_values = np.asarray(selected_is_performance, dtype=float)
    oos_values = np.asarray(selected_oos_performance, dtype=float)
    degradation_slope = None
    if float(np.var(is_values)) > 1e-18:
        degradation_slope = float(
            np.cov(is_values, oos_values, ddof=0)[0, 1] / np.var(is_values)
        )
    return {
        "complete": True,
        "method": "combinatorially_symmetric_cross_validation",
        "metric": metric,
        "row_unit": "synchronous_portfolio_return_cluster",
        "row_count": row_count,
        "usable_row_count": len(usable),
        "excluded_leading_row_count": excluded_leading_rows,
        "strategy_count": strategy_count,
        "partitions": partitions,
        "rows_per_partition": rows_per_partition,
        "combination_count": len(logits),
        "probability_of_backtest_overfitting": pbo,
        "probability_of_oos_loss": oos_loss_count / len(logits),
        "performance_degradation_slope": degradation_slope,
        "logits": logits,
    }


def deflated_sharpe_evidence(
    selected_returns: Sequence[float] | np.ndarray,
    *,
    number_of_trials: int,
    trial_return_matrix: Sequence[Sequence[float]] | np.ndarray | None = None,
    minimum_sample_count: int = 20,
) -> dict[str, object]:
    """Build a daily-return DSR audit with a conservative Sharpe-variance floor."""

    returns = np.asarray(selected_returns, dtype=float)
    if returns.ndim != 1:
        raise ValueError("selected DSR returns must be one-dimensional")
    if number_of_trials < 1:
        raise ValueError("DSR number_of_trials must be positive")
    if len(returns) < minimum_sample_count:
        return {
            "complete": False,
            "reason": "insufficient_independent_return_clusters",
            "sample_count": len(returns),
            "minimum_sample_count": minimum_sample_count,
            "number_of_trials": number_of_trials,
        }
    if not np.isfinite(returns).all():
        return {
            "complete": False,
            "reason": "non_finite_selected_returns",
            "sample_count": len(returns),
            "number_of_trials": number_of_trials,
        }
    standard_deviation = float(np.std(returns, ddof=1))
    if standard_deviation <= 1e-15:
        return {
            "complete": False,
            "reason": "zero_selected_return_variance",
            "sample_count": len(returns),
            "number_of_trials": number_of_trials,
        }
    observed_sharpe = float(np.mean(returns) / standard_deviation)
    centered = returns - float(np.mean(returns))
    second_moment = float(np.mean(centered**2))
    skewness = float(np.mean(centered**3) / second_moment**1.5)
    kurtosis = float(np.mean(centered**4) / second_moment**2)

    trial_sharpes: list[float] = []
    if trial_return_matrix is not None:
        matrix = np.asarray(trial_return_matrix, dtype=float)
        if matrix.ndim != 2 or matrix.shape[0] != len(returns):
            raise ValueError("DSR trial return matrix must align with selected returns")
        if not np.isfinite(matrix).all():
            raise ValueError("DSR trial return matrix contains non-finite values")
        trial_sharpes = [
            _performance_statistic(matrix[:, column], "sharpe")
            for column in range(matrix.shape[1])
        ]
        if number_of_trials < len(trial_sharpes):
            raise ValueError("DSR trial count cannot be below the supplied strategy count")
    empirical_sharpe_std = (
        float(np.std(np.asarray(trial_sharpes, dtype=float), ddof=0))
        if trial_sharpes
        else 0.0
    )
    sampling_floor = 1.0 / math.sqrt(len(returns) - 1)
    sharpe_std = max(empirical_sharpe_std, sampling_floor)
    benchmark = expected_maximum_sharpe(number_of_trials, sharpe_std)
    try:
        probability = probabilistic_sharpe_ratio(
            observed_sharpe,
            benchmark,
            len(returns),
            skewness=skewness,
            kurtosis=kurtosis,
        )
    except ValueError as exc:
        return {
            "complete": False,
            "reason": f"invalid_deflated_sharpe_sampling_variance:{exc}",
            "sample_count": len(returns),
            "number_of_trials": number_of_trials,
            "observed_sharpe": observed_sharpe,
            "skewness": skewness,
            "kurtosis": kurtosis,
            "selection_bias_benchmark_sharpe": benchmark,
        }
    return {
        "complete": True,
        "method": "deflated_sharpe_ratio",
        "return_unit": "portfolio_cluster_return",
        "sharpe_frequency": "same_as_return_clusters_not_annualized",
        "sample_count": len(returns),
        "number_of_trials": number_of_trials,
        "observed_sharpe": observed_sharpe,
        "skewness": skewness,
        "kurtosis": kurtosis,
        "trial_sharpes": trial_sharpes,
        "empirical_trial_sharpe_std": empirical_sharpe_std,
        "sampling_error_sharpe_std_floor": sampling_floor,
        "sharpe_std_used": sharpe_std,
        "selection_bias_benchmark_sharpe": benchmark,
        "deflated_sharpe_probability": probability,
    }


def _utc_date(value: object) -> date:
    if isinstance(value, datetime):
        observed_at = value
    elif isinstance(value, str):
        observed_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("statistical return timestamp must be a datetime or ISO string")
    if observed_at.tzinfo is None:
        raise ValueError("statistical return timestamp must be timezone-aware")
    return observed_at.astimezone(timezone.utc).date()


def aligned_daily_mark_to_market_returns(
    reports: Sequence[object],
    evaluation_timestamps: Sequence[object],
) -> tuple[tuple[str, ...], np.ndarray]:
    """Align report equity curves to one continuous UTC calendar-day matrix."""

    if not reports:
        raise ValueError("at least one report is required for aligned daily returns")
    evaluation_days = {_utc_date(value) for value in evaluation_timestamps}
    if not evaluation_days:
        raise ValueError("evaluation timestamps are required for aligned daily returns")
    equity_by_report_and_day: list[dict[date, float]] = []
    all_days = set(evaluation_days)
    for report in reports:
        initial_equity = float(getattr(report, "initial_equity_usdt"))
        if initial_equity <= 0:
            raise ValueError("report initial equity must be positive")
        daily: dict[date, float] = {}
        for point in sorted(
            tuple(getattr(report, "equity_curve", ())),
            key=lambda item: getattr(item, "observed_at"),
        ):
            observed_day = _utc_date(getattr(point, "observed_at"))
            equity = float(getattr(point, "equity_usdt"))
            if not math.isfinite(equity) or equity <= 0:
                raise ValueError("report equity curve contains non-positive or invalid equity")
            daily[observed_day] = equity
            all_days.add(observed_day)
        equity_by_report_and_day.append(daily)
    first_day = min(evaluation_days)
    last_day = max(all_days)
    calendar_days = tuple(
        first_day + timedelta(days=offset)
        for offset in range((last_day - first_day).days + 1)
    )
    matrix = np.empty((len(calendar_days), len(reports)), dtype=float)
    for column, (report, daily) in enumerate(
        zip(reports, equity_by_report_and_day)
    ):
        previous_equity = float(getattr(report, "initial_equity_usdt"))
        for row, observed_day in enumerate(calendar_days):
            current_equity = daily.get(observed_day, previous_equity)
            matrix[row, column] = current_equity / previous_equity - 1.0
            previous_equity = current_equity
    return tuple(value.isoformat() for value in calendar_days), matrix


def statistical_overfit_evidence(
    selected_report: object,
    variant_reports: Sequence[object],
    evaluation_timestamps: Sequence[object],
    *,
    number_of_trials: int,
    partitions: int = 8,
) -> dict[str, object]:
    """Combine DSR and CSCV/PBO from the same synchronous MTM return grid."""

    if len(variant_reports) < 2:
        return {
            "complete": False,
            "reason": "fewer_than_two_pre_registered_strategy_variants",
            "number_of_trials": number_of_trials,
            "strategy_count": len(variant_reports),
        }
    try:
        dates, aligned = aligned_daily_mark_to_market_returns(
            (selected_report, *variant_reports), evaluation_timestamps
        )
    except (TypeError, ValueError) as exc:
        return {
            "complete": False,
            "reason": f"invalid_mark_to_market_return_grid:{exc}",
            "number_of_trials": number_of_trials,
            "strategy_count": len(variant_reports),
        }
    selected_returns = aligned[:, 0]
    trial_matrix = aligned[:, 1:]
    dsr = deflated_sharpe_evidence(
        selected_returns,
        number_of_trials=number_of_trials,
        trial_return_matrix=trial_matrix,
    )
    pbo = cscv_probability_of_backtest_overfitting(
        trial_matrix,
        partitions=partitions,
    )
    complete = bool(dsr.get("complete")) and bool(pbo.get("complete"))
    return {
        "complete": complete,
        "reason": (
            None
            if complete
            else str(dsr.get("reason") or pbo.get("reason") or "statistical_evidence_incomplete")
        ),
        "return_unit": "utc_calendar_day_portfolio_mark_to_market_return",
        "first_return_date": dates[0],
        "last_return_date": dates[-1],
        "return_row_count": len(dates),
        "number_of_trials": number_of_trials,
        "strategy_count": len(variant_reports),
        "combination_count": pbo.get("combination_count"),
        "deflated_sharpe_probability": dsr.get("deflated_sharpe_probability"),
        "probability_of_backtest_overfitting": pbo.get(
            "probability_of_backtest_overfitting"
        ),
        "deflated_sharpe": dsr,
        "cscv": pbo,
    }


def final_evaluation_statistical_evidence(
    selected_report: object,
    evaluation_timestamps: Sequence[object],
    *,
    number_of_trials: int,
    frozen_development_evidence: Mapping[str, object],
) -> dict[str, object]:
    """Score final returns once while carrying forward development-only PBO.

    Alternative variants must never be evaluated on a final lockbox. CSCV is
    therefore frozen on development, while DSR is recomputed on the one
    selected lockbox path with the full pre-registered trial count.
    """

    try:
        dates, aligned = aligned_daily_mark_to_market_returns(
            (selected_report,), evaluation_timestamps
        )
    except (TypeError, ValueError) as exc:
        return {
            "complete": False,
            "reason": f"invalid_final_mark_to_market_return_grid:{exc}",
            "number_of_trials": number_of_trials,
            "strategy_count": frozen_development_evidence.get("strategy_count"),
        }
    dsr = deflated_sharpe_evidence(
        aligned[:, 0],
        number_of_trials=number_of_trials,
    )
    development_complete = bool(frozen_development_evidence.get("complete"))
    pbo = frozen_development_evidence.get("probability_of_backtest_overfitting")
    complete = bool(dsr.get("complete")) and development_complete and pbo is not None
    return {
        "complete": complete,
        "reason": (
            None
            if complete
            else str(
                dsr.get("reason")
                or frozen_development_evidence.get("reason")
                or "development_cscv_evidence_incomplete"
            )
        ),
        "return_unit": "utc_calendar_day_portfolio_mark_to_market_return",
        "first_return_date": dates[0],
        "last_return_date": dates[-1],
        "return_row_count": len(dates),
        "number_of_trials": number_of_trials,
        "strategy_count": frozen_development_evidence.get("strategy_count"),
        "combination_count": frozen_development_evidence.get("combination_count"),
        "deflated_sharpe_probability": dsr.get("deflated_sharpe_probability"),
        "probability_of_backtest_overfitting": pbo,
        "deflated_sharpe": dsr,
        "cscv": {
            "source": "frozen_development_only_no_alternative_lockbox_scoring",
            "development_evidence": frozen_development_evidence.get("cscv"),
        },
    }


@dataclass(frozen=True)
class TrialRecord:
    trial_id: str
    model_family: str
    data_signature: str
    parameter_hash: str
    code_commit: str
    status: str
    metrics: Mapping[str, Any]


class TrialLedger:
    """Append-only ledger used to count every model/parameter trial."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            with connection:
                connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS research_trials(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id TEXT NOT NULL UNIQUE,
                    model_family TEXT NOT NULL,
                    data_signature TEXT NOT NULL,
                    parameter_hash TEXT NOT NULL,
                    code_commit TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_trials_family
                    ON research_trials(model_family, sequence);
                CREATE TABLE IF NOT EXISTS trial_events(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_trial_events_id
                    ON trial_events(trial_id, sequence);
                CREATE TABLE IF NOT EXISTS lockbox_evaluations(
                    lockbox_fingerprint TEXT PRIMARY KEY,
                    trial_id TEXT NOT NULL,
                    purpose TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_lockbox_trial_once
                    ON lockbox_evaluations(trial_id);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def parameter_hash(parameters: Mapping[str, Any]) -> str:
        encoded = json.dumps(parameters, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode()).hexdigest()[:24]

    def append(self, record: TrialRecord) -> bool:
        if record.status not in {"planned", "running", "completed", "failed", "rejected"}:
            raise ValueError("unsupported trial status")
        payload = json.dumps(dict(record.metrics), sort_keys=True, separators=(",", ":"), default=str)
        with closing(self._connect()) as connection:
            with connection:
                existing = connection.execute(
                    "SELECT * FROM research_trials WHERE trial_id=?", (record.trial_id,)
                ).fetchone()
                if existing:
                    same = (
                        existing["model_family"] == record.model_family
                        and existing["data_signature"] == record.data_signature
                        and existing["parameter_hash"] == record.parameter_hash
                        and existing["code_commit"] == record.code_commit
                        and existing["status"] == record.status
                        and existing["metrics_json"] == payload
                    )
                    if not same:
                        raise ValueError("trial_id already exists with different content")
                    return False
                connection.execute(
                    """INSERT INTO research_trials(
                        trial_id, model_family, data_signature, parameter_hash,
                        code_commit, status, metrics_json, recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        record.trial_id,
                        record.model_family,
                        record.data_signature,
                        record.parameter_hash,
                        record.code_commit,
                        record.status,
                        payload,
                        _utc_now(),
                    ),
                )
        return True

    def trial_count(self, model_family: str | None = None) -> int:
        with closing(self._connect()) as connection:
            if model_family is None:
                return int(connection.execute("SELECT COUNT(*) FROM research_trials").fetchone()[0])
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM research_trials WHERE model_family=?", (model_family,)
                ).fetchone()[0]
            )

    def append_event(
        self,
        trial_id: str,
        status: str,
        metrics: Mapping[str, Any] | None = None,
    ) -> int:
        """Append every lifecycle event, including failures and rejections."""

        if status not in {"planned", "running", "completed", "failed", "rejected"}:
            raise ValueError("unsupported trial event status")
        payload = json.dumps(dict(metrics or {}), sort_keys=True, separators=(",", ":"), default=str)
        with closing(self._connect()) as connection:
            with connection:
                cursor = connection.execute(
                    "INSERT INTO trial_events(trial_id,status,metrics_json,recorded_at) VALUES(?,?,?,?)",
                    (trial_id, status, payload, _utc_now()),
                )
                return int(cursor.lastrowid)

    def claim_lockbox(
        self,
        lockbox_fingerprint: str,
        trial_id: str,
        *,
        purpose: str = "final_evaluation",
    ) -> bool:
        """Claim an immutable lockbox once; never recycle it for parameter selection."""

        if purpose != "final_evaluation":
            raise ValueError("lockbox data may only be used for final_evaluation")
        if len(lockbox_fingerprint) != 64:
            raise ValueError("lockbox fingerprint must be a SHA-256 hex digest")
        with closing(self._connect()) as connection:
            with connection:
                existing = connection.execute(
                    "SELECT trial_id,purpose FROM lockbox_evaluations WHERE lockbox_fingerprint=?",
                    (lockbox_fingerprint,),
                ).fetchone()
                if existing:
                    if existing["trial_id"] == trial_id and existing["purpose"] == purpose:
                        return False
                    raise ValueError(
                        "lockbox was already consumed; repeated OOS tuning/evaluation is forbidden"
                    )
                prior_trial_claim = connection.execute(
                    "SELECT lockbox_fingerprint,purpose FROM lockbox_evaluations WHERE trial_id=?",
                    (trial_id,),
                ).fetchone()
                if prior_trial_claim:
                    raise ValueError(
                        "trial already claimed a different lockbox; repeated final evaluation is forbidden"
                    )
                connection.execute(
                    "INSERT INTO lockbox_evaluations(lockbox_fingerprint,trial_id,purpose,recorded_at) VALUES(?,?,?,?)",
                    (lockbox_fingerprint, trial_id, purpose, _utc_now()),
                )
        return True

    def lockbox_claim_count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM lockbox_evaluations").fetchone()[0])
