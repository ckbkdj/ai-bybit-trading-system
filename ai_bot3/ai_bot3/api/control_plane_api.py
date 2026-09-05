from __future__ import annotations

import hmac
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
from core.service_runtime import load_predictor_runtime
from shadow_contracts.runtime import AppEnvironment


logger = logging.getLogger(__name__)


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


class ConsumerActivationRequest(RequestModel):
    consumer_id: str = Field(min_length=1, max_length=80)
    instance_id: str = Field(min_length=8, max_length=120)
    account_id: str = Field(min_length=8, max_length=120)
    lease_sec: int = Field(default=60, ge=5, le=3600)


@dataclass(frozen=True)
class AuthContext:
    consumer_id: str | None
    certificate_identity: str | None


def _json_string_map(name: str) -> dict[str, str]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{name} must be a JSON object") from exc
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) and isinstance(value, str) and key and value
        for key, value in payload.items()
    ):
        raise RuntimeError(f"{name} must map non-empty strings to non-empty strings")
    return {key.strip(): value.strip() for key, value in payload.items()}


def _configured_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw if raw else default).expanduser().resolve()


def validate_control_plane_bind(host: str) -> None:
    runtime = load_predictor_runtime(check_imports=False)
    if (
        runtime.app_environment is AppEnvironment.PRODUCTION
        and host.strip() in {"0.0.0.0", "::"}
        and os.environ.get("CONTROL_PLANE_TRUSTED_REVERSE_PROXY", "").lower()
        not in {"1", "true", "yes", "on"}
    ):
        raise RuntimeError(
            "production control plane cannot bind publicly without a trusted reverse proxy"
        )


def create_control_plane_router(project_root: Path) -> APIRouter:
    runtime = load_predictor_runtime(check_imports=False)
    production = runtime.app_environment is AppEnvironment.PRODUCTION
    data_dir = project_root / "data"
    control_db = _configured_path(
        "CONTROL_PLANE_DB", data_dir / "control_plane.sqlite3"
    )
    contracts_dir = project_root / "contracts" / "schemas"
    control = ControlPlaneRepository(control_db)

    # The production predictor is deliberately not a research node.  Do not even
    # create/migrate a research SQLite file there; every research route fails
    # closed before touching a repository.
    research: ResearchJobStore | None = None
    if not production:
        research_db = _configured_path(
            "RESEARCH_JOB_DB", data_dir / "research_jobs.sqlite3"
        )
        research = ResearchJobStore(research_db)

    global_token = os.environ.get("CONTROL_PLANE_API_TOKEN", "").strip()
    executor_tokens = _json_string_map("CONTROL_PLANE_EXECUTOR_TOKENS")
    certificate_identities = _json_string_map(
        "CONTROL_PLANE_CONSUMER_IDENTITIES"
    )
    if production and not global_token:
        raise RuntimeError("CONTROL_PLANE_API_TOKEN is required in production")
    if production and not executor_tokens:
        raise RuntimeError("production requires one token per executor")
    if production and set(executor_tokens) != set(certificate_identities):
        raise RuntimeError(
            "executor token and certificate-identity allowlists must have identical consumers"
        )

    def authorize(
        authorization: Optional[str] = Header(default=None),
        executor_consumer_id: Optional[str] = Header(
            default=None, alias="X-Executor-Consumer-ID"
        ),
        certificate_identity: Optional[str] = Header(
            default=None, alias="X-Client-Certificate-Identity"
        ),
    ) -> AuthContext:
        supplied = ""
        if authorization and authorization.lower().startswith("bearer "):
            supplied = authorization[7:].strip()
        required = global_token
        if executor_consumer_id and executor_consumer_id in executor_tokens:
            required = executor_tokens[executor_consumer_id]
        if required and not hmac.compare_digest(supplied, required):
            logger.warning(
                "control-plane authentication failed consumer_id=%s",
                executor_consumer_id or "unknown",
            )
            raise HTTPException(status_code=401, detail="invalid control-plane token")
        if production:
            expected_identity = certificate_identities.get(executor_consumer_id or "")
            if not expected_identity or not hmac.compare_digest(
                certificate_identity or "", expected_identity
            ):
                logger.warning(
                    "control-plane certificate identity rejected consumer_id=%s",
                    executor_consumer_id or "unknown",
                )
                raise HTTPException(status_code=403, detail="invalid executor identity")
        return AuthContext(executor_consumer_id, certificate_identity)

    def require_consumer(auth: object, consumer_id: str) -> None:
        if not production:
            return
        if not isinstance(auth, AuthContext) or auth.consumer_id != consumer_id:
            raise HTTPException(status_code=403, detail="consumer identity mismatch")

    def require_research() -> ResearchJobStore:
        if production or research is None:
            raise HTTPException(
                status_code=403,
                detail="research is disabled on predictor production",
            )
        return research

    router = APIRouter(prefix="/v1", tags=["control-plane"], dependencies=[Depends(authorize)])

    def latest_forecast_age_seconds() -> float | None:
        latest = control.latest_forecast()
        if latest is None:
            return None
        return max(
            0.0,
            (datetime.now(timezone.utc) - latest.time.created_at).total_seconds(),
        )

    @router.get("/health")
    def health():
        return {
            "status": "ok",
            "schema_versions": [
                "forecast-envelope.v1",
                "portfolio-intent.v1",
                "strategy-release-bundle.v1",
                "operation-ticket.v1",
                "execution-receipt.v1",
            ],
            "tickets_enabled": os.environ.get("AI_BOT_TICKETS_ENABLED", "true").lower()
            not in {"0", "false", "off"},
        }

    @router.get("/health/live")
    def health_live():
        return {"status": "live", "server_time": datetime.now(timezone.utc)}

    @router.get("/health/dependencies")
    def health_dependencies():
        try:
            metrics = control.backlog_metrics()
            return {
                "status": "ok",
                "control_plane_database": "ready",
                "research_database": "disabled" if production else "ready",
                "backlog": metrics,
                "latest_forecast_age_seconds": latest_forecast_age_seconds(),
            }
        except Exception as exc:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "degraded",
                    "control_plane_database": "unavailable",
                    "error": type(exc).__name__,
                },
            )

    @router.get("/health/ready")
    def health_ready():
        dependencies = health_dependencies()
        if isinstance(dependencies, JSONResponse):
            return dependencies
        return {
            "status": "ready",
            "deployment_environment": runtime.app_environment.value,
            "service_role": runtime.service_role.value,
            "execution_mode": runtime.execution_mode.value,
            "cluster_id": runtime.cluster_id,
            "deployment_id": runtime.deployment_id,
            "latest_forecast_age_seconds": latest_forecast_age_seconds(),
        }

    @router.get("/capabilities")
    def capabilities():
        return {
            "control_plane_version": "two-node-control-plane.v1",
            "supported_ticket_schemas": ["operation-ticket.v1"],
            "supported_receipt_schemas": ["execution-receipt.v1"],
            "minimum_executor_version": os.environ.get(
                "MINIMUM_EXECUTOR_VERSION", "1.0.0"
            ),
            "server_time": datetime.now(timezone.utc),
            "cluster_id": runtime.cluster_id,
            "deployment_id": runtime.deployment_id,
            "latest_forecast_age_seconds": latest_forecast_age_seconds(),
        }

    @router.get("/time")
    def server_time():
        return {
            "server_time": datetime.now(timezone.utc),
            "unix_time": time.time(),
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
        auth: AuthContext = Depends(authorize),
    ):
        effective_consumer = consumer_id or (
            auth.consumer_id if isinstance(auth, AuthContext) else None
        )
        if production and not effective_consumer:
            raise HTTPException(status_code=403, detail="consumer identity is required")
        if effective_consumer:
            require_consumer(auth, effective_consumer)
            page, next_cursor, skips = control.eligible_ticket_page(
                after_cursor, effective_consumer, limit=limit
            )
        else:
            page, next_cursor = control.ticket_page(after_cursor, limit)
            skips = {
                "scanned": len(page),
                "expired_skipped": 0,
                "superseded_skipped": 0,
                "consumer_ineligible_skipped": 0,
            }
        return {
            "consumer_id": effective_consumer,
            "after_cursor": after_cursor,
            "next_cursor": next_cursor,
            "items": [
                {"cursor": cursor, "ticket": item.model_dump(mode="json")}
                for cursor, item in page
            ],
            "tickets": [item.model_dump(mode="json") for _, item in page],
            "backlog": {**skips, **control.backlog_metrics()},
        }

    @router.get("/tickets/{ticket_id}")
    def ticket(ticket_id: str):
        item = control.get_ticket(ticket_id)
        if not item:
            raise HTTPException(status_code=404, detail="ticket not found")
        return item.model_dump(mode="json")

    @router.post("/tickets/{ticket_id}/claim")
    def claim(
        ticket_id: str,
        request: ClaimRequest,
        auth: AuthContext = Depends(authorize),
    ):
        require_consumer(auth, request.consumer_id)
        claim_epoch = control.claim(
            ticket_id, request.consumer_id, request.lease_token, request.lease_sec
        )
        if claim_epoch is None:
            raise HTTPException(
                status_code=409,
                detail={
                    "claimed": False,
                    "reason": "LEASE_ACTIVE",
                    "ticket_id": ticket_id,
                },
            )
        return {
            "claimed": True,
            "ticket_id": ticket_id,
            "consumer_id": request.consumer_id,
            "claim_epoch": claim_epoch,
        }

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
    def execution(
        receipt: ExecutionReceipt,
        auth: AuthContext = Depends(authorize),
    ):
        require_consumer(auth, receipt.consumer_id)
        try:
            inserted = control.save_receipt(receipt)
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=404, detail="ticket not found")
        except ImmutableConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"accepted": inserted, "receipt_id": receipt.receipt_id}

    @router.post("/consumers/activate")
    def activate_consumer(
        request: ConsumerActivationRequest,
        auth: AuthContext = Depends(authorize),
    ):
        require_consumer(auth, request.consumer_id)
        epoch = control.activate_consumer(
            request.consumer_id,
            request.instance_id,
            request.account_id,
            lease_sec=request.lease_sec,
        )
        if epoch is None:
            raise HTTPException(status_code=409, detail="consumer/account already active")
        return {
            "active": True,
            "consumer_id": request.consumer_id,
            "ownership_epoch": epoch,
        }

    @router.post("/executions/reconcile")
    def reconcile(
        receipt: ExecutionReceipt,
        auth: AuthContext = Depends(authorize),
    ):
        return execution(receipt, auth)

    @router.post("/research/jobs")
    def create_research_job(request: ResearchJobRequest):
        store = require_research()
        job_id = store.create_job(request.event_ids, request.data_cutoff)
        return store.get(job_id)

    @router.get("/research/jobs/{job_id}")
    def research_job(job_id: str):
        store = require_research()
        item = store.get(job_id)
        if not item:
            raise HTTPException(status_code=404, detail="research job not found")
        return item

    @router.get("/research/jobs/{job_id}/revisions")
    def research_revisions(job_id: str):
        store = require_research()
        if not store.get(job_id):
            raise HTTPException(status_code=404, detail="research job not found")
        return [item.model_dump(mode="json") for item in store.revisions(job_id)]

    @router.post("/research/jobs/{job_id}/transition")
    def research_transition(job_id: str, request: ResearchTransitionRequest):
        store = require_research()
        try:
            store.transition(
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
        return store.get(job_id)

    @router.post("/research/jobs/{job_id}/revisions")
    def research_revision(job_id: str, vector: EventImpactVector):
        store = require_research()
        try:
            revision = store.save_revision(job_id, vector)
        except KeyError:
            raise HTTPException(status_code=404, detail="research job not found")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"job_id": job_id, "revision": revision}

    router.control_repository = control
    router.research_repository = research
    return router
