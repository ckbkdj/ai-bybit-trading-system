from __future__ import annotations

from pathlib import Path

import pytest

from api.control_plane_api import create_control_plane_router
from core.result_manager import ResultManager
from scripts.run_publication_worker import build_worker


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _predictor_environment(monkeypatch, *, production: bool = False) -> None:
    for name in ("BYBIT_API_KEY", "BYBIT_SECRET_KEY", "EXECUTION_DB_PATH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("APP_ENV", "production" if production else "development")
    monkeypatch.setenv("SERVICE_ROLE", "predictor")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setenv("BYBIT_TRADING_MODE", "shadow")
    monkeypatch.setenv("HOST_ID", "predictor-paper-01")
    monkeypatch.setenv("CLUSTER_ID", "two-node-paper")
    monkeypatch.setenv("DEPLOYMENT_ID", "two-node-paper-v1")
    monkeypatch.setenv("MAINNET_ALLOWED", "false")


def test_realtime_and_worker_use_one_canonical_outbox(
    monkeypatch,
    tmp_path: Path,
):
    _predictor_environment(monkeypatch)
    data_dir = tmp_path / "predictor-data"
    outbox_path = tmp_path / "publication" / "forecast-outbox.sqlite3"
    control_path = tmp_path / "control" / "control.sqlite3"
    monkeypatch.setenv("PREDICTOR_DATA_DIR", str(data_dir))
    monkeypatch.setenv("FORECAST_PUBLICATION_OUTBOX_DB", str(outbox_path))
    monkeypatch.delenv("FORECAST_PUBLICATION_OUTBOX", raising=False)
    monkeypatch.setenv("CONTROL_PLANE_DB", str(control_path))

    manager = ResultManager(tmp_path / "results", tickets_enabled=False)
    _, worker_outbox = build_worker()

    assert manager.publication_outbox_db == outbox_path.resolve()
    assert manager.publication_outbox.db_path == outbox_path.resolve()
    assert worker_outbox.db_path == outbox_path.resolve()


def test_conflicting_outbox_aliases_fail_closed(monkeypatch, tmp_path: Path):
    _predictor_environment(monkeypatch)
    monkeypatch.setenv(
        "FORECAST_PUBLICATION_OUTBOX_DB",
        str(tmp_path / "canonical.sqlite3"),
    )
    monkeypatch.setenv(
        "FORECAST_PUBLICATION_OUTBOX",
        str(tmp_path / "different.sqlite3"),
    )

    with pytest.raises(RuntimeError, match="conflicts"):
        ResultManager(tmp_path / "results", tickets_enabled=False)


def test_production_control_plane_never_creates_research_store(
    monkeypatch,
    tmp_path: Path,
):
    _predictor_environment(monkeypatch, production=True)
    control_path = tmp_path / "control" / "control.sqlite3"
    research_path = tmp_path / "forbidden-research" / "research.sqlite3"
    monkeypatch.setenv("CONTROL_PLANE_DB", str(control_path))
    monkeypatch.setenv("RESEARCH_JOB_DB", str(research_path))
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN", "global-control-token")
    monkeypatch.setenv(
        "CONTROL_PLANE_EXECUTOR_TOKENS",
        '{"executor-paper-01":"executor-token"}',
    )
    monkeypatch.setenv(
        "CONTROL_PLANE_CONSUMER_IDENTITIES",
        '{"executor-paper-01":"executor-paper-01"}',
    )

    router = create_control_plane_router(PROJECT_ROOT)

    assert router.control_repository.db_path == control_path.resolve()
    assert router.research_repository is None
    assert not research_path.exists()
