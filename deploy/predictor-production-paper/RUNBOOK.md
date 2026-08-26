# Predictor production-paper runbook

This node runs only predictor-realtime, control-plane-api, market-collector, and publication-worker. Never install exchange private keys, the execution database, training, backfill, or research jobs.

## Preflight

1. Copy `.env.production-paper.example` to the protected runtime environment file and inject the control-plane tokens.
2. Keep `EXECUTION_MODE=paper`, `BYBIT_TRADING_MODE=shadow`, `MAINNET_ALLOWED=false`.
3. Use exactly one publication database path: `FORECAST_PUBLICATION_OUTBOX_DB`. Predictor-realtime and publication-worker must point to the same local SQLite file.
4. Run `python scripts/validate_production_paper_deployment.py`. It must report `status=PASS`, `mainnet_allowed=false`, `real_capital_at_risk=false`.
5. Verify WireGuard, the mTLS chain, per-executor token map, the four distinct local data directories, disk quota and NTP.
6. The control plane must start as `python -m uvicorn api.control_plane_main:app --host 127.0.0.1 --port 8000`; the legacy `api_server.py` is not the production control-plane entrypoint.

The production control plane does not construct a `ResearchJobStore` and must not create `research_jobs.sqlite3`. A third research node is mandatory for retraining and backfill.

At 80% disk utilization, stop any unsupported bulk data operation. At 90%, freeze new prediction publication and preserve the outbox; do not delete SQLite/WAL files. Never run full `integrity_check` during a prediction window—use `quick_check` against a snapshot.

During executor outage, leave prediction and publication enqueue running. Tickets expire naturally. On recovery, observe outbox replay, expired cursor fast-forward and no old ticket claims.

Rotate certificates by issuing an overlapping client certificate, updating the identity allowlist, validating a new handshake, then revoking the old certificate and token. Never log either token.
