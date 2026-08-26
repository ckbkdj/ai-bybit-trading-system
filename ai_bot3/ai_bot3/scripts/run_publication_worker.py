"""Run the predictor publication worker without running prediction or research."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(REPOSITORY_ROOT))

from core.control_plane import ControlPlaneRepository  # noqa: E402
from core.publication_outbox import (  # noqa: E402
    ForecastPublicationOutbox,
    OutboxLimits,
    PublicationWorker,
)
from core.service_runtime import load_predictor_runtime  # noqa: E402


def _path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw if raw else default).expanduser().resolve()


def _publication_outbox_path(default: Path) -> Path:
    canonical = os.environ.get("FORECAST_PUBLICATION_OUTBOX_DB", "").strip()
    legacy = os.environ.get("FORECAST_PUBLICATION_OUTBOX", "").strip()
    if canonical and legacy:
        canonical_path = Path(canonical).expanduser().resolve()
        legacy_path = Path(legacy).expanduser().resolve()
        if canonical_path != legacy_path:
            raise RuntimeError(
                "FORECAST_PUBLICATION_OUTBOX_DB conflicts with legacy "
                "FORECAST_PUBLICATION_OUTBOX"
            )
        return canonical_path
    selected = canonical or legacy
    return Path(selected if selected else default).expanduser().resolve()


def build_worker() -> tuple[PublicationWorker, ForecastPublicationOutbox]:
    load_predictor_runtime()
    data_dir = _path("PREDICTOR_DATA_DIR", PROJECT_ROOT / "data")
    outbox = ForecastPublicationOutbox(
        _publication_outbox_path(data_dir / "forecast_publication_outbox.sqlite3"),
        limits=OutboxLimits(
            max_pending=int(os.environ.get("PUBLICATION_OUTBOX_MAX_PENDING", "100000")),
            max_bytes=int(
                os.environ.get("PUBLICATION_OUTBOX_MAX_BYTES", str(2 * 1024**3))
            ),
            max_oldest_age_seconds=int(
                os.environ.get("PUBLICATION_OUTBOX_MAX_AGE_SECONDS", str(7 * 24 * 3600))
            ),
            min_disk_free_bytes=int(
                os.environ.get("PREDICTOR_MIN_DISK_FREE_BYTES", str(1024**3))
            ),
        ),
    )
    control_db = _path("CONTROL_PLANE_DB", data_dir / "control_plane.sqlite3")
    return PublicationWorker(outbox, lambda: ControlPlaneRepository(control_db)), outbox


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    worker, outbox = build_worker()
    while True:
        result = worker.run_once(limit=args.limit)
        print(
            json.dumps(
                {"result": result, "outbox": outbox.metrics()},
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )
        if args.once:
            return 0
        time.sleep(max(0.25, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
