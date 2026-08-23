from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Mapping, Sequence


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
                connection.execute(
                    "INSERT INTO lockbox_evaluations(lockbox_fingerprint,trial_id,purpose,recorded_at) VALUES(?,?,?,?)",
                    (lockbox_fingerprint, trial_id, purpose, _utc_now()),
                )
        return True

    def lockbox_claim_count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM lockbox_evaluations").fetchone()[0])
