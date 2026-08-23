from __future__ import annotations

import os
import math
import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from ticket_store import ExecutionStore


@dataclass(frozen=True)
class SoakSlo:
    minimum_days: int = 30
    minimum_samples: int = 40_000
    max_sample_gap_seconds: float = 300.0
    max_memory_growth_mb_per_day: float = 25.0
    max_handle_growth_per_day: float = 20.0
    max_log_growth_mb_per_day: float = 100.0
    max_receipt_backlog: int = 0
    max_unexpected_restarts: int = 0
    max_unknown_orders: int = 0
    max_unknown_positions: int = 0
    max_stale_sources: int = 0
    max_exchange_clock_drift_sec: float = 2.0


class SoakMonitor:
    def __init__(
        self,
        store: ExecutionStore,
        *,
        log_paths: Iterable[Path] = (),
        run_id: str | None = None,
    ):
        self.store = store
        self.log_paths = tuple(Path(path) for path in log_paths)
        self.run_id = run_id or f"run_{uuid.uuid4().hex}"
        self.started = False

    def start(self) -> bool:
        if self.started:
            raise RuntimeError("soak monitor run is already started")
        unexpected = self.store.begin_service_run(self.run_id)
        self.started = True
        return unexpected

    def sample(
        self,
        *,
        websocket_reconnects: int = 0,
        reconcile_inconsistencies: int = 0,
        unknown_orders: int = 0,
        unknown_positions: int = 0,
        stale_sources: int = 0,
        exchange_clock_drift_sec: float = 0.0,
    ) -> dict[str, float]:
        process_metrics = self._process_metrics()
        counts = self.store.operational_counts()
        db_bytes = self.store.db_path.stat().st_size if self.store.db_path.exists() else 0
        wal_path = Path(str(self.store.db_path) + "-wal")
        log_bytes = sum(path.stat().st_size for path in self.log_paths if path.exists())
        metrics = {
            **process_metrics,
            **{name: float(value) for name, value in counts.items()},
            "execution_db_bytes": float(db_bytes),
            "execution_wal_bytes": float(wal_path.stat().st_size if wal_path.exists() else 0),
            "log_bytes": float(log_bytes),
            "websocket_reconnects": float(websocket_reconnects),
            "reconcile_inconsistencies": float(reconcile_inconsistencies),
            "unknown_orders": float(unknown_orders),
            "unknown_positions": float(unknown_positions),
            "stale_sources": float(stale_sources),
            "exchange_clock_drift_sec": (
                float(exchange_clock_drift_sec)
                if math.isfinite(float(exchange_clock_drift_sec))
                else 1_000_000_000.0
            ),
        }
        self.store.record_runtime_metrics(metrics, {"run_id": self.run_id})
        return metrics

    def stop(self) -> None:
        if self.started:
            self.store.finish_service_run(self.run_id)
            self.started = False

    @staticmethod
    def _process_metrics() -> dict[str, float]:
        try:
            import psutil

            process = psutil.Process(os.getpid())
            return {
                "process_rss_bytes": float(process.memory_info().rss),
                "process_handle_count": float(process.num_handles()),
                "process_thread_count": float(process.num_threads()),
            }
        except Exception:
            return {
                "process_rss_bytes": -1.0,
                "process_handle_count": -1.0,
                "process_thread_count": -1.0,
            }


def evaluate_soak(db_path: Path, slo: SoakSlo | None = None) -> dict[str, Any]:
    limits = slo or SoakSlo()
    uri = f"file:{Path(db_path).resolve().as_posix()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        first, last = connection.execute(
            "SELECT MIN(captured_at),MAX(captured_at) FROM runtime_metrics"
        ).fetchone()
        if not first or not last:
            return {"status": "BLOCKED", "reason": "no soak metrics", "observed_days": 0}
        start = datetime.fromisoformat(str(first).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
        observed_days = max(0.0, (end - start).total_seconds() / 86400)
        latest = {
            row["metric_name"]: float(row["metric_value"])
            for row in connection.execute(
                """SELECT m.metric_name,m.metric_value FROM runtime_metrics m
                   JOIN (
                       SELECT metric_name,MAX(sequence) AS sequence
                       FROM runtime_metrics GROUP BY metric_name
                   ) x ON x.sequence=m.sequence"""
            )
        }
        unexpected = float(
            connection.execute(
                """SELECT COALESCE(SUM(metric_value),0) FROM runtime_metrics
                   WHERE metric_name='unexpected_restart'"""
            ).fetchone()[0]
        )
        slopes = {
            name: _metric_slope_per_day(connection, name)
            for name in ("process_rss_bytes", "process_handle_count", "log_bytes")
        }
        sample_times = [
            datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
            for row in connection.execute(
                """SELECT captured_at FROM runtime_metrics
                   WHERE metric_name='process_rss_bytes' ORDER BY sequence"""
            )
        ]
        sample_count = len(sample_times)
        max_sample_gap = max(
            (
                (right - left).total_seconds()
                for left, right in zip(sample_times, sample_times[1:])
            ),
            default=0.0,
        )
    failures = []
    if observed_days < limits.minimum_days:
        failures.append(f"duration {observed_days:.2f}d < {limits.minimum_days}d")
    if sample_count < limits.minimum_samples:
        failures.append(f"samples {sample_count} < {limits.minimum_samples}")
    if max_sample_gap > limits.max_sample_gap_seconds:
        failures.append("sample continuity gap")
    if unexpected > limits.max_unexpected_restarts:
        failures.append("unexpected restarts")
    for metric, maximum in (
        ("receipt_outbox_backlog", limits.max_receipt_backlog),
        ("unknown_orders", limits.max_unknown_orders),
        ("unknown_positions", limits.max_unknown_positions),
        ("stale_sources", limits.max_stale_sources),
        ("duplicate_order_count", 0),
        ("reconcile_inconsistencies", 0),
    ):
        if latest.get(metric, 0) > maximum:
            failures.append(metric)
    if slopes["process_rss_bytes"] / (1024 * 1024) > limits.max_memory_growth_mb_per_day:
        failures.append("memory growth slope")
    if slopes["process_handle_count"] > limits.max_handle_growth_per_day:
        failures.append("handle growth slope")
    if slopes["log_bytes"] / (1024 * 1024) > limits.max_log_growth_mb_per_day:
        failures.append("log growth slope")
    if abs(latest.get("exchange_clock_drift_sec", 0)) > limits.max_exchange_clock_drift_sec:
        failures.append("exchange clock drift")
    return {
        "status": "PASS" if not failures else "BLOCKED",
        "observed_days": observed_days,
        "failures": failures,
        "latest_metrics": latest,
        "slopes_per_day": slopes,
        "sample_count": sample_count,
        "max_sample_gap_seconds": max_sample_gap,
    }


def _metric_slope_per_day(connection: sqlite3.Connection, name: str) -> float:
    rows = connection.execute(
        """SELECT captured_at,metric_value FROM runtime_metrics
           WHERE metric_name=? AND metric_value>=0 ORDER BY sequence""",
        (name,),
    ).fetchall()
    if len(rows) < 2:
        return 0.0
    start = datetime.fromisoformat(str(rows[0][0]).replace("Z", "+00:00"))
    x = [
        (datetime.fromisoformat(str(row[0]).replace("Z", "+00:00")) - start).total_seconds()
        / 86400
        for row in rows
    ]
    y = [float(row[1]) for row in rows]
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)
    denominator = sum((value - mean_x) ** 2 for value in x)
    return (
        sum((xv - mean_x) * (yv - mean_y) for xv, yv in zip(x, y)) / denominator
        if denominator > 0
        else 0.0
    )
