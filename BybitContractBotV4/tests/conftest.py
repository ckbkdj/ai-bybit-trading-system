from __future__ import annotations


def pytest_configure(config):
    """Make the legacy fake model a confirmed-cancel exchange, not a REST oracle.

    The old execution-engine fixture expected a timeout cancellation to reach a
    terminal state in one reconciliation pass.  Production now correctly treats
    the REST response as asynchronous, so the fake must provide the missing
    authoritative observation through its subsequent ``find_order`` call.

    The dedicated ``AcceptedCancelButStillOpenGateway`` regression remains
    unchanged and proves that a REST payload labelled ``canceled`` is ignored
    while the exchange query still reports an open order.
    """

    from tests.test_execution_engine import FakeReconciliationGateway

    original = FakeReconciliationGateway.cancel_order
    if getattr(original, "_sets_confirmed_remote", False):
        return

    def cancel_then_confirm(self, symbol, bybit_order_id):
        response = original(self, symbol, bybit_order_id)
        self.remote = {
            "id": bybit_order_id,
            "status": "canceled",
            "info": {
                "orderId": bybit_order_id,
                "orderStatus": "Cancelled",
            },
        }
        return response

    cancel_then_confirm._sets_confirmed_remote = True
    FakeReconciliationGateway.cancel_order = cancel_then_confirm
