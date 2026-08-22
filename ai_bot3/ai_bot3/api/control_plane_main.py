"""Minimal production entrypoint for the versioned prediction/trading control plane."""

from pathlib import Path

from fastapi import FastAPI

from api.control_plane_api import create_control_plane_router


PROJECT_ROOT = Path(__file__).resolve().parent.parent
app = FastAPI(title="AI Bot Control Plane", version="1.0.0")
app.include_router(create_control_plane_router(PROJECT_ROOT))
