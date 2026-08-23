from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd

from .kline_feature_store import (
    KLINE_DERIVED_FEATURES,
    KlineFeatureStore,
    _stable_hash,
    add_kline_derived_features,
    timeframe_ms,
)


class FeatureStoreSemanticAuditor:
    """Read-only structural and semantic acceptance checks for a KlineFeatureStore."""

    def __init__(self, store: KlineFeatureStore):
        if not store.read_only:
            raise ValueError("semantic audit requires a read-only feature store")
        self.store = store

    def audit(self, *, recompute_groups: int = 25) -> dict[str, Any]:
        expected_hash = _stable_hash(list(KLINE_DERIVED_FEATURES), 16)
        with self.store._connect() as connection:
            raw_rows = int(connection.execute("SELECT COUNT(*) FROM raw_kline").fetchone()[0])
            enhanced_rows = int(
                connection.execute("SELECT COUNT(*) FROM enhanced_kline").fetchone()[0]
            )
            raw_duplicates = int(
                connection.execute(
                    """SELECT COALESCE(SUM(c-1), 0) FROM (
                        SELECT COUNT(*) AS c FROM raw_kline
                        GROUP BY symbol,timeframe,source,open_time HAVING c>1
                    )"""
                ).fetchone()[0]
            )
            enhanced_duplicates = int(
                connection.execute(
                    """SELECT COALESCE(SUM(c-1), 0) FROM (
                        SELECT COUNT(*) AS c FROM enhanced_kline
                        GROUP BY symbol,timeframe,source,feature_version,config_hash,
                                 schema_version,open_time HAVING c>1
                    )"""
                ).fetchone()[0]
            )
            invalid_ohlc = int(
                connection.execute(
                    """SELECT COUNT(*) FROM raw_kline
                       WHERE open<=0 OR high<=0 OR low<=0 OR close<=0 OR volume<0
                          OR high<MAX(open,close,low) OR low>MIN(open,close,high)
                          OR close_time<=open_time"""
                ).fetchone()[0]
            )
            invalid_json = int(
                connection.execute(
                    "SELECT COUNT(*) FROM enhanced_kline WHERE NOT json_valid(features_json)"
                ).fetchone()[0]
            )
            hash_mismatches = int(
                connection.execute(
                    "SELECT COUNT(*) FROM enhanced_kline WHERE feature_set_hash!=?",
                    (expected_hash,),
                ).fetchone()[0]
            )
            model_hash_mismatches = int(
                connection.execute(
                    """SELECT COUNT(*) FROM model_registry m
                       WHERE m.feature_set_hash IS NOT NULL
                         AND NOT EXISTS (
                           SELECT 1 FROM enhanced_kline e
                           WHERE e.feature_set_hash=m.feature_set_hash
                             AND e.feature_version=m.feature_version
                             AND e.schema_version=m.schema_version
                         )"""
                ).fetchone()[0]
            )
            groups = [
                dict(row)
                for row in connection.execute(
                    """SELECT symbol,timeframe,source,COUNT(*) AS rows,
                              MIN(open_time) AS start_open_time,
                              MAX(close_time) AS end_close_time
                       FROM raw_kline GROUP BY symbol,timeframe,source
                       ORDER BY symbol,timeframe,source"""
                ).fetchall()
            ]

        total_gaps = 0
        group_reports = []
        for group in groups:
            expected_step = timeframe_ms(group["timeframe"])
            with self.store._connect() as connection:
                gap_row = connection.execute(
                    """WITH ordered AS (
                         SELECT open_time,
                                open_time-LAG(open_time) OVER (ORDER BY open_time) AS delta
                         FROM raw_kline WHERE symbol=? AND timeframe=? AND source=?
                       )
                       SELECT COUNT(*) AS gaps FROM ordered
                       WHERE delta IS NOT NULL AND delta!=?""",
                    (
                        group["symbol"],
                        group["timeframe"],
                        group["source"],
                        expected_step,
                    ),
                ).fetchone()
            gaps = int(gap_row["gaps"] or 0)
            total_gaps += gaps
            group_reports.append({**group, "grid_gap_count": gaps})

        recomputations = []
        with self.store._connect() as connection:
            enhanced_groups = [
                dict(row)
                for row in connection.execute(
                    """SELECT symbol,timeframe,source,feature_version,config_hash,schema_version,
                              COUNT(*) AS rows
                       FROM enhanced_kline
                       GROUP BY symbol,timeframe,source,feature_version,config_hash,schema_version
                       ORDER BY symbol,timeframe,source,feature_version,config_hash,schema_version
                       LIMIT ?""",
                    (max(1, int(recompute_groups)),),
                ).fetchall()
            ]
        for group in enhanced_groups:
            result = self._recompute_group(group)
            recomputations.append(result)
        recompute_failures = sum(item["status"] == "FAIL" for item in recomputations)
        status = "PASS"
        if any(
            value > 0
            for value in (
                raw_duplicates,
                enhanced_duplicates,
                invalid_ohlc,
                invalid_json,
                hash_mismatches,
                model_hash_mismatches,
                recompute_failures,
            )
        ):
            status = "BLOCKED"
        elif total_gaps:
            status = "REVIEW_GAPS"
        return {
            "status": status,
            "raw_rows": raw_rows,
            "enhanced_rows": enhanced_rows,
            "raw_duplicate_rows": raw_duplicates,
            "enhanced_duplicate_rows": enhanced_duplicates,
            "invalid_ohlc_rows": invalid_ohlc,
            "invalid_feature_json_rows": invalid_json,
            "feature_hash_mismatch_rows": hash_mismatches,
            "model_feature_hash_mismatches": model_hash_mismatches,
            "continuous_grid_gap_count": total_gaps,
            "expected_feature_set_hash": expected_hash,
            "groups": group_reports,
            "random_recomputations": recomputations,
        }

    def _recompute_group(self, group: dict[str, Any]) -> dict[str, Any]:
        count = int(group["rows"])
        if count < 80:
            return {**group, "status": "SKIP_WARMUP", "max_abs_error": None}
        seed_text = "|".join(str(group[key]) for key in (
            "symbol", "timeframe", "source", "feature_version", "config_hash", "schema_version"
        ))
        seed = int(hashlib.sha256(seed_text.encode()).hexdigest()[:16], 16)
        offset = 64 + seed % max(1, count - 64)
        with self.store._connect() as connection:
            target = connection.execute(
                """SELECT open_time,features_json FROM enhanced_kline
                   WHERE symbol=? AND timeframe=? AND source=? AND feature_version=?
                     AND config_hash=? AND schema_version=?
                   ORDER BY open_time LIMIT 1 OFFSET ?""",
                (
                    group["symbol"], group["timeframe"], group["source"],
                    group["feature_version"], group["config_hash"], group["schema_version"],
                    offset,
                ),
            ).fetchone()
            raw_rows = connection.execute(
                """SELECT open_time,close_time,open,high,low,close,volume FROM raw_kline
                   WHERE symbol=? AND timeframe=? AND source=? AND open_time<=?
                   ORDER BY open_time DESC LIMIT 300""",
                (group["symbol"], group["timeframe"], group["source"], target["open_time"]),
            ).fetchall()
        frame = pd.DataFrame([dict(row) for row in reversed(raw_rows)])
        computed = add_kline_derived_features(frame).iloc[-1]
        persisted = json.loads(target["features_json"])
        errors = []
        within_tolerance = True
        for name in KLINE_DERIVED_FEATURES:
            expected = persisted.get(name)
            actual = computed.get(name)
            if expected is None and pd.isna(actual):
                continue
            if expected is None or not np.isfinite(float(actual)):
                errors.append(float("inf"))
                within_tolerance = False
            else:
                absolute_error = abs(float(expected) - float(actual))
                errors.append(absolute_error)
                tolerance = 1e-8 + 1e-7 * abs(float(expected))
                if absolute_error > tolerance:
                    within_tolerance = False
        maximum = max(errors, default=0.0)
        return {
            **group,
            "sample_open_time": int(target["open_time"]),
            "status": "PASS" if within_tolerance else "FAIL",
            "max_abs_error": maximum,
        }


def attrition_payload(store: KlineFeatureStore) -> list[dict[str, Any]]:
    return [asdict(store.dataset_attrition(spec)) for spec in store.load_mode_specs()]
