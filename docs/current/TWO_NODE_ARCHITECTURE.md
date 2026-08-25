# Two-node production-paper architecture

Release scope is production configuration with paper execution only. It does not authorize model training, historical backfill, testnet orders, or mainnet.

## Failure domains

Predictor node:

- predictor-realtime atomically saves forecast results and enqueues publication records;
- publication-worker retries publication asynchronously and never calls the executor;
- control-plane-api owns its local contract database and exposes only HTTP schemas;
- market-collector owns a separate local public-market database;
- no Bybit private key, execution database, or execution implementation module is allowed.

Executor node:

- consumes OperationTicket only through the mTLS HTTP contract;
- owns its local execution/receipt database and dedicated account identity;
- runs reconcile, server-side stop recovery, reduce-only take profit, max-holding exit, cancel, close, and reduce without the predictor;
- never reads model files, prediction SQLite, PIT stores, training, backfill, or research jobs.

The nodes share no filesystem. SQLite, SMB, NFS, and model/PIT paths never cross the node boundary. WireGuard/private routing, TLS, mTLS, a unique executor token, certificate identity, consumer ownership lease, schema/version capability checks, and clock checks form the connection boundary.

## State flow

Forecast file -> publication outbox -> publication-worker -> control-plane SQLite -> mTLS HTTP -> executor ticket cursor -> execution SQLite -> receipt outbox -> mTLS HTTP -> control-plane receipt.

Every arrow crossing machines is an HTTP contract. Forecast production and local position protection have no synchronous dependency on the opposite node.

## Resource topology

The predictor host runs four separately limited services: predictor-realtime, control-plane-api, market-collector, and publication-worker. Each has its own data path, CPU quota, memory ceiling, priority, and IO weight. At 80% disk utilization backfill is forbidden; at 90% research is forbidden; real-time prediction remains scheduled. Full integrity_check is forbidden during prediction windows.

Retraining, research, and historical backfill require a third research node. This is an explicit architectural requirement, not an assumed capability of the two production nodes.

## Readiness

Risk increase requires exchange reconciliation, account ownership, position protection, control-plane handshake, compatible schemas/version, clock within limit, receipt delivery, and required market/private-stream health. Paper permits at most five seconds of control-plane skew; testnet/live templates use two seconds. Risk reduction does not depend on predictor readiness.
