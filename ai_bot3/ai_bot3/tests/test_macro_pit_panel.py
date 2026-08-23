from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from core.evaluation.profitability_rebuild import LONG_FACTOR_GROUPS
from core.providers.fred_alfred_pit import FredAlfredPITStore
from core.training.macro_pit_panel import MacroPITFeatureSource


UTC = timezone.utc


def _insert_response_and_vix(database: Path, raw_path: Path) -> None:
    store = FredAlfredPITStore(database)
    body = b'{"observations":[]}'
    raw_path.write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()
    ingested_at = datetime(2026, 1, 3, tzinfo=UTC)
    with store.connect() as connection:
        connection.execute(
            """INSERT INTO fred_alfred_responses(
                   response_id,series_id,output_type,request_descriptor,
                   requested_at,received_at,http_status,content_length,
                   content_sha256,row_count,raw_response_path
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "response-1",
                "VIXCLS",
                4,
                json.dumps(
                    {
                        "series_id": "VIXCLS",
                        "output_type": 4,
                        "realtime_start": "2026-01-01",
                        "realtime_end": "2026-01-02",
                    },
                    sort_keys=True,
                ),
                "2026-01-03T00:00:00Z",
                ingested_at.isoformat().replace("+00:00", "Z"),
                200,
                len(body),
                digest,
                0,
                str(raw_path.resolve()),
            ),
        )
        for index, available_at in enumerate(
            (
                datetime(2026, 1, 1, 23, 59, 59, tzinfo=UTC),
                datetime(2026, 1, 2, 23, 59, 59, tzinfo=UTC),
            ),
            start=1,
        ):
            connection.execute(
                """INSERT INTO macro_pit_observations(
                       observation_id,name,value,unit,event_time,available_at,
                       ingested_at,source,series_id,observation_date,vintage_date
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    f"vix-{index}",
                    "vix_level",
                    17.0 + index,
                    "index_points",
                    (available_at - timedelta(hours=23)).isoformat().replace(
                        "+00:00", "Z"
                    ),
                    available_at.isoformat().replace("+00:00", "Z"),
                    ingested_at.isoformat().replace("+00:00", "Z"),
                    "fred.alfred.initial_release",
                    "VIXCLS",
                    available_at.date().isoformat(),
                    available_at.date().isoformat(),
                ),
            )
        connection.commit()


def test_macro_snapshot_hash_provenance_and_asof_staleness(tmp_path: Path):
    database = tmp_path / "macro.sqlite3"
    raw = tmp_path / "response.json"
    _insert_response_and_vix(database, raw)
    source = MacroPITFeatureSource(database)
    frozen_sequence = source.maximum_sequence()
    history, evidence = source.load(
        ["vix_level"], maximum_sequence=frozen_sequence
    )

    assert len(history) == 2
    assert evidence["response_count"] == 1
    assert evidence["raw_response_hashes_verified"] is True
    assert len(evidence["snapshot_sha256"]) == 64
    decisions = pd.DataFrame(
        {
            "decision_at": [
                datetime(2026, 1, 1, 12, tzinfo=UTC),
                datetime(2026, 1, 2, 12, tzinfo=UTC),
                datetime(2026, 1, 10, tzinfo=UTC),
            ]
        }
    )
    joined = source.join(decisions, names=["vix_level"], history=history)
    assert pd.isna(joined.loc[0, "vix_level"])
    assert joined.loc[1, "vix_level"] == 18.0
    assert pd.isna(joined.loc[2, "vix_level"])
    assert joined.loc[1, "vix_level__available_at"] <= joined.loc[1, "decision_at"]

    raw.write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="length|hash"):
        source.load(["vix_level"])


def test_macro_groups_use_observed_features_not_placeholder_surprises():
    factors = LONG_FACTOR_GROUPS["macro_vintage"]
    assert "fred_cpi_first_release_yoy_ratio" in factors
    assert "alfred_payrolls_mean_revision_delta" in factors
    assert "fred_vintage_surprise" not in factors
    assert "alfred_revision_surprise" not in factors
