# Bybit testnet admission

The repository can reach practical Shadow without a private exchange key.
Real testnet execution is deliberately gated because credentials and account
state are external evidence.

Before an execution scenario, all items must be true:

- dedicated Bybit testnet subaccount;
- fresh testnet API key and secret;
- withdrawal permission disabled;
- fixed egress IP confirmed;
- `BYBIT_POSITION_MODE=one_way`;
- `BYBIT_ALLOW_MANUAL_ORDERS=false`;
- no unknown open order or position;
- exact deployed Git SHA and approved strategy release ID;
- explicit user authorization for the testnet run;
- `BYBIT_ENABLE_LIVE=false` and `MAINNET_ALLOWED=false`.

`scripts/testnet_admission.py` performs only read operations. A PASS proves that
the fail-closed configuration loads, testnet authentication works, clock skew is
acceptable and the account is clean. It does not prove OPEN/REDUCE/CLOSE/CANCEL,
partial fills, cancel/fill races, WebSocket recovery or restart reconciliation;
those require the separately authorized execution acceptance session.
