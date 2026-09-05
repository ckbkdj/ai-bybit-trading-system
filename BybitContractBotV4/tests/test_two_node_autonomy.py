from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from execution_service import PollResult
from contracts.operation_ticket_v1 import OperationTicket
from incident_modes import IncidentMode
from private_stream import PrivateStreamHandler
from runtime_config import TradingMode
from service_main import TradingExecutionService
from runtime_context import BybitRuntimeContext
from ticket_client import HandshakeResult, TicketHttpClient
from ticket_store import ExecutionStore


class _Reconciler:
    def __init__(self, calls):
        self.calls = calls

    def recover_all(self):
        self.calls.append("reconcile")


class _ConsumerService:
    def __init__(self, calls):
        self.calls = calls

    def run_once(self):
        self.calls.append("ticket_intake")
        return PollResult(0, 0, 0, 0, 0)


class _HandshakeClient:
    def __init__(self, result):
        self.result = result

    def handshake(self, **kwargs):
        return self.result


def _service(tmp_path: Path, handshake: HandshakeResult):
    calls = []
    service = TradingExecutionService.__new__(TradingExecutionService)
    service.settings = SimpleNamespace(
        mode=TradingMode.SHADOW,
        ticket_consumer_id="executor-a",
        position_owner_id="paper-owner-a",
        executor_version="1.0.0",
        cluster_id="cluster-a",
        deployment_id="deployment-a",
        max_control_plane_clock_drift_seconds=5,
        prediction_max_age_seconds=600,
    )
    service.store = ExecutionStore(tmp_path / "execution.sqlite3")
    service.stream_handler = PrivateStreamHandler(service.store)
    service.websocket = None
    service.reconciler = _Reconciler(calls)
    service.ticket_client = _HandshakeClient(handshake)
    service.consumer_service = _ConsumerService(calls)
    service.instance_id = "deployment-a:executor-a:instance-001"
    service.last_poll_at = None
    service.last_poll_result = None
    service.last_error = None
    service.last_reconciliation_error = None
    service.last_control_error = None
    service.last_control_handshake = None
    service.control_plane_ready = False
    service._control_freeze_owned = False
    service.last_ownership_audit = None
    service._reconcile_inconsistencies = 0
    service._verify_account_ownership = lambda: calls.append("ownership")
    service._enforce_max_holding = lambda: calls.append("max_holding")
    service._recover_take_profits = lambda: calls.append("take_profit_recovery")
    service._sample_soak = lambda: None
    return service, calls


def test_incompatible_predictor_freezes_intake_but_keeps_local_protection(tmp_path: Path):
    service, calls = _service(
        tmp_path,
        HandshakeResult(False, "ticket_schema_incompatible", 0.1, {}),
    )

    service.run_once()

    assert calls == ["reconcile", "ownership", "max_holding", "take_profit_recovery"]
    assert service.store.system_runtime()["reconciliation_complete"] == 1
    assert service.store.system_runtime()["incident_mode"] == IncidentMode.FREEZE_NEW_RISK.value
    assert service.control_plane_ready is False


def test_ready_handshake_allows_ticket_intake_only_after_reconciliation(tmp_path: Path):
    service, calls = _service(
        tmp_path,
        HandshakeResult(True, "ready", 0.1, {}, ownership_epoch=1),
    )

    service.run_once()

    assert calls == [
        "reconcile",
        "ownership",
        "max_holding",
        "take_profit_recovery",
        "ticket_intake",
    ]
    assert service.control_plane_ready is True


def test_stale_forecast_keeps_executor_health_not_ready(tmp_path: Path):
    service, _ = _service(
        tmp_path,
        HandshakeResult(
            True,
            "ready",
            0.1,
            {"latest_forecast_age_seconds": 601},
            ownership_epoch=1,
        ),
    )
    service.soak_monitor = SimpleNamespace(run_id="run-stale-forecast")
    service.run_once()
    health = service.health_snapshot()
    assert health["ready"] is False
    assert health["market_data"] == "stale"
    assert health["latest_forecast_age"] == 601


class _Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self.payload = payload or {}

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _RoutingSession:
    def __init__(self, capabilities, server_time):
        self.capability_payload = capabilities
        self.server_time_value = server_time
        self.activated = False

    def get(self, url, **kwargs):
        if url.endswith("/v1/capabilities"):
            return _Response(payload=self.capability_payload)
        if url.endswith("/v1/time"):
            return _Response(payload={"unix_time": self.server_time_value})
        raise AssertionError(url)

    def post(self, url, **kwargs):
        assert url.endswith("/v1/consumers/activate")
        self.activated = True
        return _Response(payload={"ownership_epoch": 1})


def _capabilities():
    return {
        "supported_ticket_schemas": ["operation-ticket.v1"],
        "supported_receipt_schemas": ["execution-receipt.v1"],
        "minimum_executor_version": "1.0.0",
        "cluster_id": "cluster-a",
        "deployment_id": "deployment-a",
    }


def test_schema_incompatibility_never_activates_consumer():
    capabilities = _capabilities()
    capabilities["supported_ticket_schemas"] = ["operation-ticket.v2"]
    session = _RoutingSession(capabilities, 0)
    client = TicketHttpClient("https://control.internal", session=session)
    result = client.handshake(
        consumer_id="executor-a",
        instance_id="deployment-a:executor-a:instance-001",
        account_id="paper-owner-a",
        executor_version="1.0.0",
        expected_cluster_id="cluster-a",
        expected_deployment_id="deployment-a",
        max_clock_skew_seconds=5,
    )
    assert result.reason == "ticket_schema_incompatible"
    assert session.activated is False


def test_clock_skew_never_activates_consumer(monkeypatch):
    session = _RoutingSession(_capabilities(), 1_000_000)
    client = TicketHttpClient("https://control.internal", session=session)
    monkeypatch.setattr("ticket_client.time.time", lambda: 1_000_010)
    result = client.handshake(
        consumer_id="executor-a",
        instance_id="deployment-a:executor-a:instance-001",
        account_id="paper-owner-a",
        executor_version="1.0.0",
        expected_cluster_id="cluster-a",
        expected_deployment_id="deployment-a",
        max_clock_skew_seconds=5,
    )
    assert result.reason == "clock_skew"
    assert abs(result.clock_skew_seconds) >= 10
    assert session.activated is False


def test_risk_reduction_does_not_call_predictor_regime_or_health(tmp_path: Path):
    from BybitContractBotV4.tests.test_execution_engine import ticket_payload

    payload = ticket_payload("tk_reduce_without_predictor_001")
    payload["intent"].update(
        action="CLOSE",
        side="SELL",
        position_effect="CLOSE_ONLY",
        target_exposure_pct=0,
        risk_budget_pct=0,
    )
    payload["entry"].update(order_type="MARKET", limit_price=None)
    payload["protection"] = None
    ticket = OperationTicket.model_validate(payload)

    class _PublicExchange:
        def fetch_ticker(self, symbol):
            return {"last": 100000, "bid": 99995, "ask": 100005}

        def fetch_time(self):
            return 1_000_000

    def forbidden(*args, **kwargs):
        raise AssertionError("risk reduction contacted predictor")

    store = ExecutionStore(tmp_path / "risk-reduction.sqlite3")
    context = BybitRuntimeContext(
        public_exchange=_PublicExchange(),
        account_client=object(),
        store=store,
        mode="shadow",
        regime_provider=forbidden,
        data_health_provider=forbidden,
    )
    assert context.market(ticket).market_regime == ticket.guards.observed_market_regime
    assert context.health(ticket).data_source_healthy is False
