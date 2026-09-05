# Practical Shadow deployment

This is the operational path from a clean Git checkout to a real two-host
**production-paper / Shadow** run. It is not a profitability claim and it does
not authorize mainnet.

## What “practical” means here

A deployment is practical only when all of the following are true on the exact
same Git SHA:

1. predictor and executor run on separate hosts;
2. the control plane is reachable only through the WireGuard address and mTLS;
3. the executor remains `EXECUTION_MODE=paper` / `BYBIT_TRADING_MODE=shadow`;
4. public market data, prediction cadence, ticket intake, reconciliation and
   receipt delivery remain healthy;
5. the operations console reports both nodes READY and `paper_only=true`;
6. `scripts/physical_shadow_acceptance.py` passes for the chosen observation
   window and writes immutable JSON evidence;
7. no private Bybit credential is present on the production-paper executor;
8. `BYBIT_ENABLE_LIVE=false` and `MAINNET_ALLOWED=false` on both hosts.

A green GitHub Actions run alone is not this acceptance.

## Files

```text
deploy/docker/Dockerfile
deploy/practical/predictor.compose.yml
deploy/practical/executor.compose.yml
deploy/practical/shadow-lab.compose.yml
deploy/practical/.env.predictor.example
deploy/practical/.env.executor.example
deploy/practical/bootstrap-shadow-pki.sh
deploy/practical/up.sh
deploy/practical/up.ps1
ops_console/
scripts/physical_shadow_acceptance.py
scripts/testnet_admission.py
```

## 1. Prepare both hosts

Checkout the exact PR SHA on both machines and verify it:

```bash
git rev-parse HEAD
```

Install Docker Engine with the Compose v2 plugin. Synchronize both machines with
NTP. Create a private WireGuard route, for example:

```text
predictor: 10.70.0.1
executor:  10.70.0.2
```

Firewall policy:

- predictor TCP 8443: executor WireGuard IP only;
- executor TCP 8787: predictor/operations WireGuard IP only;
- operations console TCP 8790: loopback or trusted management IP only;
- no database directory is shared over NFS, SMB or Docker volume plugins.

## 2. Generate Shadow/Testnet mTLS material

On a trusted operations machine:

```bash
./deploy/practical/bootstrap-shadow-pki.sh \
  ./deploy/practical/pki predictor-paper.internal 10.70.0.1
```

Keep `pki/ca/ca.key` offline. Copy only `pki/executor/` to the executor host.
The predictor host uses `pki/predictor/`. The generated material is for
Shadow/Testnet practical validation; production certificate issuance may use
your existing private CA.

## 3. Predictor host

```bash
cp deploy/practical/.env.predictor.example deploy/practical/predictor.env
```

Replace every `<replace-...>` value and set `APP_CODE_COMMIT` to the exact SHA.
The control-plane token maps must contain separate identities for the executor
and the read-only operations console.

Mount reviewed model/config artifacts under `deploy/practical/runtime/` or set:

```text
PREDICTOR_MODELS_DIR=/absolute/path/to/models
PREDICTOR_CONFIG_FILE=/absolute/path/to/config.yml
```

Start the full predictor role and console:

```bash
PREDICTOR_WIREGUARD_BIND_IP=10.70.0.1 \
EXECUTOR_HEALTH_URL=http://10.70.0.2:8787 \
./deploy/practical/up.sh predictor up
```

Verify:

```bash
docker compose -f deploy/practical/predictor.compose.yml ps
curl http://127.0.0.1:8790/healthz
```

Open the console at `http://127.0.0.1:8790` through SSH port forwarding when
remote. Enter `OPS_CONSOLE_TOKEN` in the page; it is retained only in browser
session storage.

Telegram alerts are enabled only when `TELEGRAM_BOT_TOKEN` and
`TELEGRAM_CHAT_ID` are supplied. Alert failure never changes the trading state.

## 4. Executor host

```bash
cp deploy/practical/.env.executor.example deploy/practical/executor.env
```

Set the same `CLUSTER_ID`, `DEPLOYMENT_ID` and exact `APP_CODE_COMMIT` as the
predictor. Set `TICKET_API_BASE_URL=https://predictor-paper.internal:8443` and
install the executor-specific certificate directory.

Start:

```bash
EXECUTOR_HEALTH_BIND_IP=10.70.0.2 \
./deploy/practical/up.sh executor up
```

Paper mode deliberately contains no `BYBIT_API_KEY` or `BYBIT_SECRET_KEY`.
The executor may use Bybit public market endpoints but cannot submit a private
order through the Shadow gateway.

## 5. Machine acceptance

From a trusted operations host that has the executor client certificate:

```bash
export CONTROL_PLANE_API_TOKEN='<executor-specific-token>'
export APP_CODE_COMMIT="$(git rev-parse HEAD)"
python scripts/physical_shadow_acceptance.py \
  --control-plane-url https://predictor-paper.internal:8443 \
  --executor-url http://10.70.0.2:8787 \
  --consumer-id executor-paper-01 \
  --certificate-identity executor-paper-01 \
  --client-cert deploy/practical/pki/executor/executor-paper-01.crt \
  --client-key deploy/practical/pki/executor/executor-paper-01.key \
  --ca-bundle deploy/practical/pki/executor/control-plane-ca.crt \
  --expected-cluster-id two-node-paper \
  --expected-deployment-id two-node-paper-v1 \
  --duration-seconds 900 \
  --interval-seconds 5 \
  --output evidence/physical-shadow-acceptance.json
```

PASS requires every sample by default. The script is read-only: it creates no
forecast, ticket, order or receipt.

Then retain:

```text
evidence/physical-shadow-acceptance.json
container image digests
exact Git SHA
sanitized compose config
predictor/executor health snapshots
NTP and WireGuard evidence
```

## 6. Testnet admission

Testnet is a separate human-authorized step. Before any order scenario, use a
new dedicated testnet subaccount, no withdrawal permission, fixed IP, one-way
mode and explicit approval. The read-only admission check is:

```bash
export APP_ENV=production
export SERVICE_ROLE=executor
export EXECUTION_MODE=testnet
export BYBIT_TRADING_MODE=testnet
export BYBIT_TESTNET_EXPLICIT_PERMISSION=true
export BYBIT_TESTNET_NO_WITHDRAWAL_CONFIRMED=true
export BYBIT_TESTNET_FIXED_IP_CONFIRMED=true
python scripts/testnet_admission.py --output evidence/testnet-admission.json
```

It authenticates to Bybit testnet and reads markets, balance, positions and open
orders. It places zero orders. Execution scenarios must be run only after this
report passes and the user explicitly authorizes the testnet run.

## 7. Stop and rollback

```bash
./deploy/practical/up.sh executor down
./deploy/practical/up.sh predictor down
```

Back up each local SQLite file with its WAL/SHM before changing code. Never
clear the kill switch, high-water equity, ownership or reconciliation state by
editing a database. Restore the previous exact image/SHA and restart in Shadow.
