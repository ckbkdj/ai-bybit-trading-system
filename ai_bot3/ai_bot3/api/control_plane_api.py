from __future__ import annotations

import hmac
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from contracts.event_impact_v1 import EventImpactVector
from contracts.execution_receipt_v1 import ExecutionReceipt
from core.control_plane import ControlPlaneRepository, ImmutableConflict
from core.jobs.job_store import ResearchJobStore
from core.jobs.research_job import ResearchState


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ClaimRequest(RequestModel):
    consumer_id: str = Field(min_length=1, max_length=80)
    lease_token: str = Field(min_length=8, max_length=100)
    lease_sec: int = Field(default=30, ge=5, le=3600)


class TicketEventRequest(RequestModel):
    event_id: str = Field(min_length=8, max_length=100)
    event_type: str = Field(min_length=1, max_length=80)
    created_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


class ResearchJobRequest(RequestModel):
    event_ids: list[str] = Field(min_length=1)
    data_cutoff: datetime


class ResearchTransitionRequest(RequestModel):
    target: ResearchState
    checkpoint: dict[str, Any] = Field(default_factory=dict)
    source_count: Optional[int] = Field(default=None, ge=0)
    primary_source_verified: Optional[bool] = None
    error: Optional[str] = None


def create_control_plane_router(project_root: Path) -> APIRouter:
    data_dir = project_root / "data"
    control_db = Path(os.environ.get("CONTROL_PLANE_DB", data_dir / "control_plane.sqlite3"))
    research_db = Path(os.environ.get("RESEARCH_JOB_DB", data_dir / "research_jobs.sqlite3"))
    contracts_dir = project_root / "contracts" / "schemas"
    control = ControlPlaneRepository(control_db)
    research = ResearchJobStore(research_db)

    def authorize(authorization: Optional[str] = Header(default=None)) -> None:
        required = os.environ.get("CONTROL_PLANE_API_TOKEN", "").strip()
        if not required:
            return
        supplied = ""
        if authorization and authorization.lower().startswith("bearer "):
            supplied = authorization[7:].strip()
        if not hmac.compare_digest(supplied, required):
            raise HTTPException(status_code=401, detail="invalid control-plane token")

    router = APIRouter(prefix="/v1", tags=["control-plane"], dependencies=[Depends(authorize)])

    @router.get("/health")
    def health():
        return {
            "status": "ok",
            "schema_versions": [
                "forecast-envelope.v1", "operation-ticket.v1", "execution-receipt.v1"
            ],
            "tickets_enabled": os.environ.get("AI_BOT_TICKETS_ENABLED", "true").lower()
            not in {"0", "false", "off"},
        }

    @router.get("/schema/{schema_name}")
    def schema(schema_name: str):
        allowed = {
            "forecast-envelope": "forecast-envelope.v1.json",
            "operation-ticket": "operation-ticket.v1.json",
            "execution-receipt": "execution-receipt.v1.json",
        }
        filename = allowed.get(schema_name)
        if not filename:
            raise HTTPException(status_code=404, detail="unknown schema")
        path = contracts_dir / filename
        if not path.exists():
            raise HTTPException(status_code=503, detail="schema artifact is unavailable")
        return JSONResponse(json.loads(path.read_text(encoding="utf-8")))

    @router.get("/forecasts/latest")
    def latest_forecast(symbol: Optional[str] = None):
        forecast = control.latest_forecast(symbol)
        if not forecast:
            raise HTTPException(status_code=404, detail="forecast not found")
        return forecast.model_dump(mode="json")

    @router.get("/forecasts/{forecast_id}")
    def forecast(forecast_id: str, revision: Optional[int] = Query(default=None, ge=1)):
        item = control.get_forecast(forecast_id, revision)
        if not item:
            raise HTTPException(status_code=404, detail="forecast not found")
        return item.model_dump(mode="json")

    @router.get("/tickets")
    def tickets(
        after_cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=500),
        consumer_id: Optional[str] = None,
    ):
        page, next_cursor = control.ticket_page(after_cursor, limit)
        return {
            "consumer_id": consumer_id,
            "after_cursor": after_cursor,
            "next_cursor": next_cursor,
            "items": [
                {"cursor": cursor, "ticket": item.model_dump(mode="json")}
                for cursor, item in page
            ],
            "tickets": [item.model_dump(mode="json") for _, item in page],
        }

    @router.get("/tickets/{ticket_id}")
    def ticket(ticket_id: str):
        item = control.get_ticket(ticket_id)
        if not item:
            raise HTTPException(status_code=404, detail="ticket not found")
        return item.model_dump(mode="json")

    @router.post("/tickets/{ticket_id}/claim")
    def claim(ticket_id: str, request: ClaimRequest):
        claimed = control.claim(
            ticket_id, request.consumer_id, request.lease_token, request.lease_sec
        )
        if not claimed:
            raise HTTPException(status_code=409, detail="ticket claim is unavailable")
        return {"claimed": True, "ticket_id": ticket_id, "consumer_id": request.consumer_id}

    @router.post("/tickets/{ticket_id}/events")
    def ticket_event(ticket_id: str, request: TicketEventRequest):
        try:
            inserted = control.append_ticket_event(
                ticket_id,
                request.event_id,
                request.event_type,
                request.created_at,
                request.payload,
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="ticket not found")
        except ImmutableConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"accepted": inserted, "event_id": request.event_id}

    @router.post("/executions")
    def execution(receipt: ExecutionReceipt):
        try:
            inserted = control.save_receipt(receipt)
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=404, detail="ticket not found")
        except ImmutableConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"accepted": inserted, "receipt_id": receipt.receipt_id}

    @router.post("/executions/reconcile")
    def reconcile(receipt: ExecutionReceipt):
        return execution(receipt)

    @router.post("/research/jobs")
    def create_research_job(request: ResearchJobRequest):
        job_id = research.create_job(request.event_ids, request.data_cutoff)
        return research.get(job_id)

    @router.get("/research/jobs/{job_id}")
    def research_job(job_id: str):
        item = research.get(job_id)
        if not item:
            raise HTTPException(status_code=404, detail="research job not found")
        return item

    @router.get("/research/jobs/{job_id}/revisions")
    def research_revisions(job_id: str):
        if not research.get(job_id):
            raise HTTPException(status_code=404, detail="research job not found")
        return [item.model_dump(mode="json") for item in research.revisions(job_id)]

    @router.post("/research/jobs/{job_id}/transition")
    def research_transition(job_id: str, request: ResearchTransitionRequest):
        try:
            research.transition(
                job_id,
                request.target,
                request.checkpoint,
                source_count=request.source_count,
                primary_source_verified=request.primary_source_verified,
                error=request.error,
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="research job not found")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return research.get(job_id)

    @router.post("/research/jobs/{job_id}/revisions")
    def research_revision(job_id: str, vector: EventImpactVector):
        try:
            revision = research.save_revision(job_id, vector)
        except KeyError:
            raise HTTPException(status_code=404, detail="research job not found")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"job_id": job_id, "revision": revision}

    router.control_repository = control
    router.research_repository = research
    return router
