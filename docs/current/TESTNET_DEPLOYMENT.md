# Testnet deployment preflight

Testnet is not authorized in the current task. A later human-approved task must
provide dedicated Bybit Testnet credentials and an isolated subaccount.

The template `BybitContractBotV4/.env.testnet.example` fixes these safety
values: mode `testnet`, live enablement false, dedicated subaccount true,
manual orders false and position mode `one_way`. The operator must also inject
the exact `APP_CODE_COMMIT`, an approved strategy release ID, TLS-verified
prediction/ticket endpoints and Testnet-only API credentials.

Before any Testnet order, require a clean-clone CI pass, database backup,
startup reconciliation, zero unknown positions/orders, private WebSocket
health, exchange clock agreement, instrument precision checks, rate-limit
handling and a human stop/rollback owner. Testnet evidence cannot be reused as
mainnet approval.
