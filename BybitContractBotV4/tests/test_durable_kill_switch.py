from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from durable_execution_store import DurableExecutionStore
from ticket_store import iso


def test_manual_kill_switch_survives_new_utc_risk_day_and_pnl_sync():
    with tempfile.TemporaryDirectory() as directory:
        store = DurableExecutionStore(Path(directory) / "execution.sqlite3")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
        with store.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO risk_runtime(risk_date,kill_switch,updated_at)
                   VALUES (?,1,?)""",
                (yesterday, iso(datetime.now(timezone.utc) - timedelta(days=1))),
            )

        assert store.kill_switch_enabled() is True
        assert store.risk_runtime()["kill_switch"] == 1

        store.update_risk_runtime(
            realised_pnl=-5.0,
            unrealised_pnl=-2.0,
            trade_pnl=-1.0,
        )
        assert store.kill_switch_enabled() is True
        assert store.risk_runtime()["kill_switch"] == 1

        store.synchronize_risk_runtime(
            realised_pnl=-5.0,
            unrealised_pnl=-2.0,
            consecutive_losses=1,
            last_loss_at=datetime.now(timezone.utc),
        )
        assert store.kill_switch_enabled() is True
        assert store.risk_runtime()["kill_switch"] == 1

        store.set_kill_switch(False)
        assert store.kill_switch_enabled() is False
        assert store.risk_runtime()["kill_switch"] == 0
