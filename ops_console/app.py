"""Read-only operations console for the two-node production-paper deployment.

The console never creates tickets, never touches an execution database, and refuses
startup if a live/mainnet environment leaks into the process. It polls the existing
control-plane and executor health contracts, records state transitions in memory,
and can emit optional Telegram alerts.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import ssl
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


STATIC_ROOT = Path(__file__).resolve().parent / "static"


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class Target:
    name: str
    base_url: str
    token: str = ""
    consumer_id: str = ""
    certificate_identity: str = ""
    client_cert: str = ""
    client_key: str = ""
    ca_bundle: str = ""
    verify_tls: bool = True

    @property
    def configured(self) -> bool:
        return bool(self.base_url.strip())


@dataclass(frozen=True)
class Settings:
    app_environment: str
    console_token: str
    poll_seconds: float
    timeout_seconds: float
    predictor: Target
    executor: Target
    telegram_bot_token: str
    telegram_chat_id: str
    telegram_suppress_seconds: float

    @classmethod
    def load(cls) -> "Settings":
        environment = os.environ
        app_environment = environment.get("APP_ENV", "development").strip().lower()
        console_token = environment.get("OPS_CONSOLE_TOKEN", "").strip()
        if app_environment == "production" and len(console_token) < 16:
            raise RuntimeError("OPS_CONSOLE_TOKEN must contain at least 16 characters in production")

        forbidden = {
            "BYBIT_TRADING_MODE": environment.get("BYBIT_TRADING_MODE", "").strip().lower(),
            "EXECUTION_MODE": environment.get("EXECUTION_MODE", "").strip().lower(),
            "BYBIT_ENABLE_LIVE": environment.get("BYBIT_ENABLE_LIVE", "").strip().lower(),
            "MAINNET_ALLOWED": environment.get("MAINNET_ALLOWED", "").strip().lower(),
        }
        if forbidden["BYBIT_TRADING_MODE"] == "live" or forbidden["EXECUTION_MODE"] == "live":
            raise RuntimeError("operations console refuses a live execution environment")
        if _truthy(forbidden["BYBIT_ENABLE_LIVE"]) or _truthy(forbidden["MAINNET_ALLOWED"]):
            raise RuntimeError("operations console refuses mainnet-enabled configuration")

        try:
            poll_seconds = max(2.0, float(environment.get("OPS_POLL_SECONDS", "5")))
            timeout_seconds = max(0.5, float(environment.get("OPS_HTTP_TIMEOUT_SECONDS", "3")))
            suppress_seconds = max(
                30.0, float(environment.get("TELEGRAM_SUPPRESS_SECONDS", "300"))
            )
        except ValueError as exc:
            raise RuntimeError("operations console numeric settings are invalid") from exc

        predictor = Target(
            name="predictor",
            base_url=environment.get(
                "PREDICTOR_CONTROL_PLANE_URL", "http://control-plane:8000"
            ).rstrip("/"),
            token=environment.get("OPS_CONTROL_PLANE_TOKEN", "").strip(),
            consumer_id=environment.get("OPS_CONTROL_PLANE_CONSUMER_ID", "ops-console").strip(),
            certificate_identity=environment.get(
                "OPS_CONTROL_PLANE_CERT_IDENTITY", "ops-console"
            ).strip(),
            client_cert=environment.get("OPS_CONTROL_PLANE_MTLS_CERT", "").strip(),
            client_key=environment.get("OPS_CONTROL_PLANE_MTLS_KEY", "").strip(),
            ca_bundle=environment.get("OPS_CONTROL_PLANE_CA_BUNDLE", "").strip(),
            verify_tls=not _truthy(environment.get("OPS_CONTROL_PLANE_TLS_INSECURE")),
        )
        executor = Target(
            name="executor",
            base_url=environment.get("EXECUTOR_HEALTH_URL", "http://executor:8787").rstrip("/"),
            verify_tls=not _truthy(environment.get("OPS_EXECUTOR_TLS_INSECURE")),
            ca_bundle=environment.get("OPS_EXECUTOR_CA_BUNDLE", "").strip(),
        )
        return cls(
            app_environment=app_environment,
            console_token=console_token,
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
            predictor=predictor,
            executor=executor,
            telegram_bot_token=environment.get("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=environment.get("TELEGRAM_CHAT_ID", "").strip(),
            telegram_suppress_seconds=suppress_seconds,
        )


SETTINGS = Settings.load()
EVENTS: deque[dict[str, Any]] = deque(maxlen=200)
LAST_FINGERPRINT: str | None = None
LAST_TELEGRAM: dict[str, float] = {}


def _ssl_context(target: Target) -> ssl.SSLContext | None:
    if not target.base_url.lower().startswith("https://"):
        return None
    if not target.verify_tls:
        context = ssl._create_unverified_context()  # noqa: SLF001 - explicit lab override
    else:
        context = ssl.create_default_context(cafile=target.ca_bundle or None)
    if target.client_cert:
        context.load_cert_chain(target.client_cert, target.client_key or None)
    return context


def _request_json(target: Target, path: str, timeout: float) -> dict[str, Any]:
    started = time.monotonic()
    headers = {"Accept": "application/json", "User-Agent": "ai-bybit-ops-console/1"}
    if target.token:
        headers["Authorization"] = f"Bearer {target.token}"
    if target.consumer_id:
        headers["X-Executor-Consumer-ID"] = target.consumer_id
    if target.certificate_identity:
        headers["X-Client-Certificate-Identity"] = target.certificate_identity
    request = Request(f"{target.base_url}{path}", headers=headers, method="GET")
    status_code = 0
    raw = b""
    try:
        with urlopen(request, timeout=timeout, context=_ssl_context(target)) as response:
            status_code = int(response.status)
            raw = response.read(2 * 1024 * 1024)
    except HTTPError as exc:
        status_code = int(exc.code)
        raw = exc.read(2 * 1024 * 1024)
    except (URLError, TimeoutError, OSError, ssl.SSLError) as exc:
        return {
            "ok": False,
            "status_code": 0,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
            "error": f"{type(exc).__name__}: {exc}",
            "payload": None,
        }

    payload: Any = None
    if raw:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = {"raw": raw[:512].decode("utf-8", errors="replace")}
    return {
        "ok": 200 <= status_code < 300,
        "status_code": status_code,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
        "error": None,
        "payload": payload,
    }


def _payload(result: dict[str, Any]) -> dict[str, Any]:
    value = result.get("payload")
    return value if isinstance(value, dict) else {}


def _derive_snapshot(results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    control_live = results["control_live"]
    control_ready = results["control_ready"]
    executor_live = results["executor_live"]
    executor_ready = results["executor_ready"]
    capabilities = _payload(results["control_capabilities"])
    control_ready_payload = _payload(control_ready)
    executor_health = _payload(results["executor_health"])

    predictor_ready = bool(control_ready.get("ok")) and control_ready_payload.get("status") == "ready"
    executor_is_ready = bool(executor_ready.get("ok")) and _payload(executor_ready).get("status") == "ready"
    live_count = int(bool(control_live.get("ok"))) + int(bool(executor_live.get("ok")))
    if predictor_ready and executor_is_ready:
        overall = "ready"
    elif live_count:
        overall = "degraded"
    else:
        overall = "down"

    control_mode = str(control_ready_payload.get("execution_mode") or "unknown")
    executor_mode = str(executor_health.get("mode") or "unknown")
    executor_execution_mode = str(executor_health.get("execution_mode") or "unknown")
    paper_only = (
        control_mode == "paper"
        and executor_execution_mode == "paper"
        and executor_mode == "shadow"
    )
    safety = {
        "paper_only": paper_only,
        "mainnet_allowed": False,
        "private_exchange_api_expected": False,
        "control_execution_mode": control_mode,
        "executor_execution_mode": executor_execution_mode,
        "executor_mode": executor_mode,
        "kill_switch": executor_health.get("kill_switch"),
        "incident_mode": executor_health.get("incident_mode"),
        "dead_letter_count": executor_health.get("dead_letter_count"),
        "incomplete_ticket_count": executor_health.get("incomplete_ticket_count"),
    }
    if overall == "ready" and not paper_only:
        overall = "unsafe"

    return {
        "as_of": _utc_now(),
        "overall": overall,
        "predictor": {
            "ready": predictor_ready,
            "live": bool(control_live.get("ok")),
            "health": control_ready_payload,
            "dependencies": _payload(results["control_dependencies"]),
            "capabilities": capabilities,
            "latency_ms": control_ready.get("elapsed_ms"),
            "error": control_ready.get("error"),
        },
        "executor": {
            "ready": executor_is_ready,
            "live": bool(executor_live.get("ok")),
            "health": executor_health,
            "dependencies": _payload(results["executor_dependencies"]),
            "latency_ms": executor_ready.get("elapsed_ms"),
            "error": executor_ready.get("error"),
        },
        "safety": safety,
        "raw": results,
    }


async def collect_snapshot() -> dict[str, Any]:
    calls = {
        "control_live": (SETTINGS.predictor, "/v1/health/live"),
        "control_ready": (SETTINGS.predictor, "/v1/health/ready"),
        "control_dependencies": (SETTINGS.predictor, "/v1/health/dependencies"),
        "control_capabilities": (SETTINGS.predictor, "/v1/capabilities"),
        "executor_live": (SETTINGS.executor, "/v1/health/live"),
        "executor_ready": (SETTINGS.executor, "/v1/health/ready"),
        "executor_dependencies": (SETTINGS.executor, "/v1/health/dependencies"),
        "executor_health": (SETTINGS.executor, "/health"),
    }
    names = list(calls)
    tasks = [
        asyncio.to_thread(_request_json, target, path, SETTINGS.timeout_seconds)
        for target, path in calls.values()
    ]
    values = await asyncio.gather(*tasks)
    return _derive_snapshot(dict(zip(names, values, strict=True)))


def _event_fingerprint(snapshot: dict[str, Any]) -> str:
    material = {
        "overall": snapshot.get("overall"),
        "predictor_ready": snapshot.get("predictor", {}).get("ready"),
        "executor_ready": snapshot.get("executor", {}).get("ready"),
        "safety": snapshot.get("safety"),
    }
    return json.dumps(material, sort_keys=True, separators=(",", ":"))


def _telegram_send(message: str) -> None:
    if not (SETTINGS.telegram_bot_token and SETTINGS.telegram_chat_id):
        return
    body = urlencode(
        {
            "chat_id": SETTINGS.telegram_chat_id,
            "text": message,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request = Request(
        f"https://api.telegram.org/bot{SETTINGS.telegram_bot_token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            response.read(1024)
    except Exception:
        # Alerting failure must not crash the operations console.
        return


def _record_transition(snapshot: dict[str, Any]) -> None:
    global LAST_FINGERPRINT
    fingerprint = _event_fingerprint(snapshot)
    if fingerprint == LAST_FINGERPRINT:
        return
    previous = LAST_FINGERPRINT
    LAST_FINGERPRINT = fingerprint
    event = {
        "at": snapshot["as_of"],
        "overall": snapshot["overall"],
        "predictor_ready": snapshot["predictor"]["ready"],
        "executor_ready": snapshot["executor"]["ready"],
        "paper_only": snapshot["safety"]["paper_only"],
        "initial": previous is None,
    }
    EVENTS.appendleft(event)
    key = event["overall"]
    now = time.monotonic()
    if now - LAST_TELEGRAM.get(key, 0.0) < SETTINGS.telegram_suppress_seconds:
        return
    LAST_TELEGRAM[key] = now
    text = (
        "AI-Bybit practical status\n"
        f"overall={event['overall']}\n"
        f"predictor_ready={event['predictor_ready']}\n"
        f"executor_ready={event['executor_ready']}\n"
        f"paper_only={event['paper_only']}\n"
        f"at={event['at']}"
    )
    _telegram_send(text)


async def _monitor_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            snapshot = await collect_snapshot()
            await asyncio.to_thread(_record_transition, snapshot)
        except Exception as exc:
            EVENTS.appendleft(
                {
                    "at": _utc_now(),
                    "overall": "monitor_error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=SETTINGS.poll_seconds)
        except TimeoutError:
            continue


@asynccontextmanager
async def lifespan(_: FastAPI):
    stop = asyncio.Event()
    task = asyncio.create_task(_monitor_loop(stop), name="ops-console-monitor")
    try:
        yield
    finally:
        stop.set()
        await task


app = FastAPI(
    title="AI-Bybit Operations Console",
    version="1.0.0",
    docs_url=None if SETTINGS.app_environment == "production" else "/docs",
    redoc_url=None,
    lifespan=lifespan,
)
app.mount("/assets", StaticFiles(directory=STATIC_ROOT), name="assets")


def require_console_token(authorization: str | None = Header(default=None)) -> None:
    if not SETTINGS.console_token:
        return
    supplied = ""
    if authorization and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    if not hmac.compare_digest(supplied, SETTINGS.console_token):
        raise HTTPException(status_code=401, detail="invalid operations console token")


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_ROOT / "index.html")


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {
        "status": "alive",
        "app_environment": SETTINGS.app_environment,
        "mainnet_allowed": False,
    }


@app.get("/api/status", dependencies=[Depends(require_console_token)])
async def status() -> dict[str, Any]:
    snapshot = await collect_snapshot()
    await asyncio.to_thread(_record_transition, snapshot)
    return snapshot


@app.get("/api/events", dependencies=[Depends(require_console_token)])
def events() -> dict[str, Any]:
    return {"items": list(EVENTS), "count": len(EVENTS)}


@app.get("/api/config", dependencies=[Depends(require_console_token)])
def config() -> dict[str, Any]:
    return {
        "app_environment": SETTINGS.app_environment,
        "predictor_url": SETTINGS.predictor.base_url,
        "executor_url": SETTINGS.executor.base_url,
        "poll_seconds": SETTINGS.poll_seconds,
        "telegram_enabled": bool(
            SETTINGS.telegram_bot_token and SETTINGS.telegram_chat_id
        ),
        "read_only": True,
        "mainnet_allowed": False,
    }
