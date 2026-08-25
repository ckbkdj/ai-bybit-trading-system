# Executor production-paper runbook

This node owns only its local execution SQLite and consumes OperationTicket over WireGuard plus TLS/mTLS. Do not mount predictor storage, SMB, NFS, model files, PIT stores, or research paths.

Preflight: EXECUTION_MODE=paper, mainnet disabled, unique consumer/token/certificate identity, NTP healthy, local database writable, and control-plane capabilities compatible. READY requires reconciliation, ownership, receipt delivery, version/schema, clock, and control health.

When the predictor or network fails, ticket intake freezes. Continue exchange reconciliation, existing server-side stops, reduce-only take profit, max-holding exits, cancel, close, and reduce. Receipts stay in the local outbox. Never clear a content-conflict kill switch automatically.

On recovery: reconcile first, renew ownership, retry receipts independently, fast-forward expired backlog, skip superseded decisions, and permit increased risk only after READY.

Rotate the client certificate and unique token with an overlap window. Verify the new handshake and identity mapping before revoking the old pair.
