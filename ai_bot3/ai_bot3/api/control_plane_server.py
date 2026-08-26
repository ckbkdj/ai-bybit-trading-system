"""Minimal predictor-to-executor control-plane server.

This process deliberately excludes legacy UI endpoints, CCXT clients, LLM cache
refreshers and prediction-file presentation.  Its only responsibility is the
versioned forecast/ticket/receipt contract used between the two physical hosts.
"""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from api.control_plane_api import create_control_plane_router, validate_control_plane_bind
from core.service_runtime import load_predictor_runtime


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def create_app() -> FastAPI:
    identity = load_predictor_runtime()
    app = FastAPI(
        title="AI-Bybit Control Plane",
        version="two-node-control-plane.v1",
        docs_url=None if identity.app_environment.value == "production" else "/docs",
        redoc_url=None if identity.app_environment.value == "production" else "/redoc",
        openapi_url=None if identity.app_environment.value == "production" else "/openapi.json",
    )
    app.include_router(create_control_plane_router(PROJECT_ROOT))
    return app


app = create_app()


def main() -> None:
    host = os.environ.get("CONTROL_PLANE_BIND_HOST", "127.0.0.1").strip()
    port = int(os.environ.get("CONTROL_PLANE_BIND_PORT", "8000"))
    validate_control_plane_bind(host)
    uvicorn.run(app, host=host, port=port, access_log=False)


if __name__ == "__main__":
    main()
