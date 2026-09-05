# Shadow deployment

Shadow is the only stage authorized by this release train.

1. Clone the RC branch into a new directory and record `git rev-parse HEAD`.
2. Use Python 3.11 on Linux or Python 3.12 on Windows.
3. Install only the matching lock with `python -m pip install --require-hashes -r <lock>`.
4. Copy `ai_bot3/ai_bot3/.env.shadow.example` and
   `BybitContractBotV4/.env.shadow.example` into an external secret/config
   store. Set `APP_CODE_COMMIT` to the recorded SHA.
5. Keep `BYBIT_TRADING_MODE=shadow`, `BYBIT_ENABLE_LIVE=false`, approval empty,
   private API keys empty, and service endpoints loopback-only.
6. Restore only artifacts declared in `runtime-data-manifest.v1.json`; verify
   schema, integrity and coverage before starting a writer.
7. Run full tests, schema validation, migration tests, truth regression and
   `scripts/run_shadow_e2e.py` before service startup.

Successful Shadow startup means software invariants are reproducible. It does
not prove profitability, exchange behavior or mainnet readiness.
