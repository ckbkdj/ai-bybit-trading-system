# Predictor production-paper runbook

This node runs only predictor-realtime, control-plane-api, market-collector, and publication-worker. Never install exchange private keys, the execution database, training, backfill, or research jobs.

Preflight: verify the WireGuard peer, mTLS chain, per-executor token map, four distinct local data directories, disk quota, NTP, and EXECUTION_MODE=paper. The control plane remains loopback-only behind the mTLS proxy.

At 80% disk utilization, stop backfill. At 90%, stop research. Neither workload belongs on this host in the supported topology; a third research node is mandatory for retraining. Never run full integrity_check during a prediction window—use quick_check against a snapshot.

During executor outage, leave prediction and publication enqueue running. Tickets expire naturally. On recovery, observe outbox replay, expired cursor fast-forward, and no old ticket claims.

Rotate certificates by issuing an overlapping client certificate, updating the identity allowlist, validating a new handshake, then revoking the old certificate and token. Never log either token.
