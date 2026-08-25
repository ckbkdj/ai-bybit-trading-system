# Predictor continuity runbook

## Normal operation

Run predictor-realtime, publication-worker, control-plane-api, and market-collector as separate services and data roots. Check forecast age, publication outbox depth, disk utilization, process quotas, NTP, and control readiness. Never place exchange private credentials on this host.

Forecast result persistence is the primary completion boundary. Publishing is asynchronous: an unavailable executor or temporarily unwritable control plane must not block forecast generation or calibration. An enqueue failure is visible and fatal to that publication attempt; it is never silently ignored.

## Executor outage

Do not stop prediction. Confirm forecast count and local result timestamps continue increasing. Watch outbox capacity, oldest item age, free space, and retry error. Tickets expire while offline. Do not extend ticket validity to “help” recovery.

When connectivity returns, let publication-worker replay idempotently. Expired ticket records are archived and forecasts remain available; an expired operation ticket can never re-enter executable state. Confirm executor cursor fast-forward and zero claims for the expired backlog.

## Disk pressure

At 80%, stop backfill. At 90%, stop research. In the supported production topology neither workload runs on this node, so an alert means configuration drift. Keep real-time prediction running and preserve its configured reserve. Do not run full integrity_check during the prediction window; take a SQLite snapshot and use quick_check.

## Recovery

Restore one database at a time while its owning service is stopped and the maintenance marker exists. Verify checksum and quick_check before start. Never restore a predictor database to the executor or mount a remote database. After start, verify forecast freshness, outbox replay, API readiness, and no process remains in the wrong service role.
