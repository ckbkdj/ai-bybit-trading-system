# Two-node production-paper deployment

Use deploy/predictor-production-paper and deploy/executor-production-paper. The executor-testnet directory is an inactive future template and requires a separate human authorization task; do not start it here.

## Predictor node

Create four local data directories owned only by their service account. Install the locked Python dependencies. Copy the example environment without adding Bybit private keys. Inject control secrets from the host secret store. Install the four service definitions, WireGuard rules, mTLS reverse proxy, disk quota, and monitoring. Keep the API on loopback.

## Executor node

Create one local execution data directory. Do not copy models, PIT stores, prediction databases, research code paths, or predictor backups. Inject the executor-specific token and mTLS keypair. Verify consumer ID equals certificate identity, the endpoint is private HTTPS, NTP is within five seconds, and execution mode is paper.

## Acceptance

Run the two-node fault harness and full locked regression. Confirm two distinct process/data roots, all A-H scenarios pass, stress SLO passes, and all child processes exit. Then push the exact release SHA and require every CI job for that SHA to be green.

Before starting services, verify:

- EXECUTION_MODE=paper and legacy shadow mapping agrees;
- predictor contains no Bybit private key or execution DB setting;
- executor contains no model/PIT/training/backfill/research setting;
- every SQLite path is local and owned by exactly one service;
- token, mTLS, schema/version, cluster/deployment, time, and ownership gates pass;
- live_count=0 and mainnet_allowed=false.

The Production Paper PR remains unmerged. Testnet and mainnet are outside this deployment and require new explicit authorization.
