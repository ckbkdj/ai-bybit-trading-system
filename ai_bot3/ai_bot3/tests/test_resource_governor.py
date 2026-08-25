from __future__ import annotations

from pathlib import Path

import pytest

from core.resource_governor import ResourceGovernor, validate_separate_data_roots


def test_disk_pressure_stops_optional_workloads_but_not_realtime_prediction():
    governor = ResourceGovernor()
    at_80 = governor.decide(total_bytes=1000, free_bytes=200)
    assert at_80.predictor_realtime_allowed is True
    assert at_80.backfill_allowed is False
    assert at_80.research_allowed is True

    at_90 = governor.decide(total_bytes=1000, free_bytes=100)
    assert at_90.predictor_realtime_allowed is True
    assert at_90.backfill_allowed is False
    assert at_90.research_allowed is False
    assert at_90.large_integrity_check_allowed is False


def test_data_roots_must_be_local_and_distinct(tmp_path: Path):
    resolved = validate_separate_data_roots(
        {
            "predictor": tmp_path / "predictor",
            "control": tmp_path / "control",
            "research": tmp_path / "research",
        }
    )
    assert len(set(resolved.values())) == 3
    with pytest.raises(ValueError, match="shared"):
        validate_separate_data_roots(
            {"predictor": tmp_path / "same", "control": tmp_path / "same"}
        )
    with pytest.raises(ValueError, match="SMB/NFS"):
        validate_separate_data_roots({"predictor": Path(r"\\server\share")})
