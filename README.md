# AI-Bybit Shadow Platform v3

This repository is a fail-closed prediction and Bybit execution platform with a
real two-host production-paper path. It is not a profitability claim and it does
not authorize mainnet trading.

- [Practical two-host Shadow deployment](docs/current/PRACTICAL_DEPLOYMENT.md)
- [Read-only testnet admission](docs/current/TESTNET_ADMISSION.md)
- [Current release documentation](docs/current/README.md)
- [Two-node production-paper architecture](docs/current/TWO_NODE_ARCHITECTURE.md)
- [Two-node deployment](docs/current/TWO_NODE_DEPLOYMENT.md)
- [Shadow deployment](docs/current/SHADOW_DEPLOYMENT.md)
- [Testnet preflight](docs/current/TESTNET_DEPLOYMENT.md)
- [Profitability and mainnet status](docs/current/PROFITABILITY_STATUS.md)
- [Mainnet NO-GO gates](docs/current/MAINNET_NO_GO.md)

Practical tooling now includes hash-locked Docker targets, separate predictor and
executor Compose files, mTLS bootstrap, a read-only FastAPI/Vue operations
console, optional Telegram status alerts, physical two-node Shadow acceptance,
and read-only Bybit testnet admission.

PR #4 is retained as research history and must not be merged unchanged.
