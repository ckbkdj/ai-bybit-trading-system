# Executor production-paper runbook

This node owns only its local execution SQLite and consumes `OperationTicket` over WireGuard plus TLS/mTLS. Do not mount predictor storage, SMB, NFS, model files, PIT stores or research paths.

## Preflight

1. Copy `.env.production-paper.example` to the protected runtime environment file and inject the unique executor token and mTLS files.
2. Keep `EXECUTION_MODE=paper`, `BYBIT_TRADING_MODE=shadow`, `BYBIT_ENABLE_LIVE=false`, `MAINNET_ALLOWED=false`.
3. Run `python main.py --preflight` from `BybitContractBotV4`. It must report:
   - `private_trading_api_enabled=false`
   - `mainnet_order_submission_enabled=false`
   - `real_capital_at_risk=false`
4. Run the repository-level `python scripts/validate_production_paper_deployment.py`.
5. Verify unique consumer/token/certificate identity, NTP, local database writability and compatible control-plane capabilities.

READY requires reconciliation, ownership, receipt delivery, version/schema, clock and control health.

When the predictor or network fails, ticket intake freezes. Continue exchange reconciliation, existing server-side stops, reduce-only take profit, max-holding exits, cancel, close and reduce. Receipts stay in the local outbox. Never clear a content-conflict kill switch automatically.

If a fetched ticket is still held by an unexpired claim from a crashed process, do **not** advance the consumer cursor past it. Retry after lease expiry or after reconciliation proves the ticket terminal. This prevents an unresolved ticket from being lost.

On recovery: reconcile first, renew ownership, retry receipts independently, fast-forward only expired/superseded backlog and permit increased risk only after READY.

Rotate the client certificate and unique token with an overlap window. Verify the new handshake and identity mapping before revoking the old pair.
