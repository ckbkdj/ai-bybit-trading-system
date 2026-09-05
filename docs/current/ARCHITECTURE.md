# Current architecture

The platform has two fail-closed services joined by versioned contracts:

```text
official/PIT inputs -> prediction + portfolio intent -> control-plane SQLite
                    -> shared OperationTicket JSON contract
                    -> execution service -> durable order/fill/receipt SQLite
```

`shadow_contracts/` is the canonical Python package for the ticket and receipt
contracts. Both service-local `contracts/` packages are compatibility facades,
so contract behavior cannot drift independently. JSON Schemas remain the wire
validation boundary.

The prediction service cannot emit an executable ticket without a current
strategy bundle, matching code/data hashes, a non-stale profitability gate and
complete evidence. The execution service independently validates the ticket,
risk limits, position mode, ownership, kill switch, duplicate identity and
release allowlist. Shadow mode never constructs a private exchange gateway.

Runtime identity resolves in this order: `APP_CODE_COMMIT`, ordinary
`git rev-parse HEAD`, legacy `.version-history/HEAD`; failure is fatal for
release tooling. Critical control and execution databases record migration ID,
UTC application time, code commit and schema checksum, and reject unsupported
future versions.
