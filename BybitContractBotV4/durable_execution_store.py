from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Optional

from execution_state import ExecutionState, require_transition
from ticket_store import ExecutionStore, canonical, iso, utc_now


class DurableExecutionStore(ExecutionStore):
    """Safety extensions used by the active execution service.

    Daily PnL and cooldown state remain partitioned by ``risk_date``.  The manual
    kill switch is different: once enabled it must stay enabled until an explicit
    operator action disables it.  This store also preserves the distinction
    between an entry ticket and its deterministic child exit orders when a child
    is cancelled.
    """

    @staticmethod
    def _latest_kill_switch(connection) -> int:
        row = connection.execute(
            """SELECT kill_switch FROM risk_runtime
               ORDER BY updated_at DESC, risk_date DESC LIMIT 1"""
        ).fetchone()
        return int(row["kill_switch"] if row else 0)

    def kill_switch_enabled(self) -> bool:
        with closing(self.connect()) as connection:
            return bool(self._latest_kill_switch(connection))

    def _carry_kill_switch_to_today(self) -> None:
        """Create today's row with the previous manual switch before daily updates."""

        today = utc_now().date().isoformat()
        now = iso(utc_now())
        with self.transaction(immediate=True) as connection:
            today_row = connection.execute(
                "SELECT 1 FROM risk_runtime WHERE risk_date=?", (today,)
            ).fetchone()
            if today_row:
                return
            connection.execute(
                """INSERT INTO risk_runtime(risk_date, kill_switch, updated_at)
                   VALUES (?, ?, ?)""",
                (today, self._latest_kill_switch(connection), now),
            )

    def risk_runtime(self, at: Optional[datetime] = None) -> dict[str, Any]:
        point = (at or utc_now()).astimezone(timezone.utc)
        day = point.date().isoformat()
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM risk_runtime WHERE risk_date=?", (day,)
            ).fetchone()
            latest_kill_switch = self._latest_kill_switch(connection)
        if row:
            return dict(row)
        return {
            "risk_date": day,
            "realised_pnl": 0.0,
            "unrealised_pnl": 0.0,
            "consecutive_losses": 0,
            "cooldown_until": None,
            # Only the current operational snapshot inherits the manual switch.
            # A historical as-of query still reports that date's actual row/default.
            "kill_switch": latest_kill_switch if at is None else 0,
            "updated_at": iso(utc_now()),
        }

    def update_risk_runtime(self, **kwargs) -> None:
        self._carry_kill_switch_to_today()
        super().update_risk_runtime(**kwargs)

    def synchronize_risk_runtime(self, **kwargs) -> None:
        self._carry_kill_switch_to_today()
        super().synchronize_risk_runtime(**kwargs)

    def confirm_cancellation(
        self,
        cancel_ticket_id: str,
        target_order_link_id: str,
        raw: Optional[dict[str, Any]] = None,
    ) -> None:
        """Confirm an exchange cancellation without corrupting parent-ticket state.

        Cancelling the entry order may terminally cancel its ticket.  Cancelling a
        take-profit, stop, trailing or time-exit child only changes that child and
        the dedicated cancellation ticket; the already-filled entry ticket stays
        FILLED/PARTIALLY_FILLED and continues to own the live position.
        """

        now = utc_now()
        with self.transaction(immediate=True) as connection:
            target_order = connection.execute(
                """SELECT ticket_id,role,order_status FROM execution_orders
                   WHERE order_link_id=?""",
                (target_order_link_id,),
            ).fetchone()
            if not target_order:
                raise KeyError(target_order_link_id)
            cancel_row = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (cancel_ticket_id,)
            ).fetchone()
            if not cancel_row:
                raise KeyError(cancel_ticket_id)
            cancel_state = ExecutionState(cancel_row["state"])
            require_transition(cancel_state, ExecutionState.CANCELLED)

            # A late cancel acknowledgement must never overwrite a fill.
            connection.execute(
                """UPDATE execution_orders SET order_status='CANCELLED',raw_json=?,updated_at=?
                   WHERE order_link_id=? AND UPPER(order_status)!='FILLED'""",
                (canonical(raw or {})[0], iso(now), target_order_link_id),
            )

            target_row = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (target_order["ticket_id"],)
            ).fetchone()
            target_state = ExecutionState(target_row["state"])
            if target_order["role"] == "entry":
                if target_state not in {ExecutionState.CANCELLED, ExecutionState.FILLED}:
                    require_transition(target_state, ExecutionState.CANCELLED)
                    connection.execute(
                        "UPDATE tickets SET state=?,updated_at=? WHERE ticket_id=?",
                        (
                            ExecutionState.CANCELLED.value,
                            iso(now),
                            target_order["ticket_id"],
                        ),
                    )
                    self._append_event(
                        connection,
                        target_order["ticket_id"],
                        target_state,
                        ExecutionState.CANCELLED,
                        "entry_order_cancelled_by_ticket",
                        {
                            "cancel_ticket_id": cancel_ticket_id,
                            "order_link_id": target_order_link_id,
                        },
                        now,
                    )
                elif target_state is ExecutionState.FILLED:
                    self._append_event(
                        connection,
                        target_order["ticket_id"],
                        target_state,
                        target_state,
                        "late_entry_cancel_ignored_after_fill",
                        {
                            "cancel_ticket_id": cancel_ticket_id,
                            "order_link_id": target_order_link_id,
                        },
                        now,
                    )
            else:
                self._append_event(
                    connection,
                    target_order["ticket_id"],
                    target_state,
                    target_state,
                    "child_order_cancelled",
                    {
                        "cancel_ticket_id": cancel_ticket_id,
                        "order_link_id": target_order_link_id,
                        "role": target_order["role"],
                    },
                    now,
                )

            if cancel_state is not ExecutionState.CANCELLED:
                connection.execute(
                    "UPDATE tickets SET state=?,updated_at=? WHERE ticket_id=?",
                    (ExecutionState.CANCELLED.value, iso(now), cancel_ticket_id),
                )
                self._append_event(
                    connection,
                    cancel_ticket_id,
                    cancel_state,
                    ExecutionState.CANCELLED,
                    "cancellation_confirmed",
                    {"target_order_link_id": target_order_link_id},
                    now,
                )
