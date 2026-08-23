from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Optional

from ticket_store import ExecutionStore, iso, utc_now


class DurableExecutionStore(ExecutionStore):
    """ExecutionStore with a kill switch that does not expire at UTC midnight.

    Daily PnL and cooldown state remain partitioned by ``risk_date``.  The manual
    kill switch is different: once enabled it must stay enabled until an explicit
    operator action disables it.  The legacy store looked only at today's row,
    which silently returned false after a UTC date rollover.

    This subclass preserves the existing schema by carrying the most recently
    written kill-switch value into a newly created daily risk row before any PnL
    synchronization.  It is the store used by the active execution service.
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
