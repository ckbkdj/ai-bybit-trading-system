# Two-node failure matrix

The automated acceptance harness runs predictor and executor suites concurrently in two Python processes with distinct temporary data roots. Durations are accelerated; no training, backfill, exchange credential, testnet order, or live endpoint is used.

| Case | Injection | Required invariant | Automated evidence |
|---|---|---|---|
| A | Executor unavailable for accelerated six hours | forecasts continue, outbox grows without synchronous wait, expired tickets are never revived | forecast publication outbox replay test |
| B | Predictor unavailable with executor position state | no new risk; reconciliation and protection callbacks continue; receipts remain durable | executor autonomy test |
| C | 100 duplicate/jitter deliveries | one order, one fill, monotonic state, cursor never regresses | 100-replay execution regression |
| D | Permanent 422 receipt | poison item dead-letters; later receipt and fetch continue | receipt outbox resilience test |
| E | Two executors claim one consumer/account | second instance cannot activate | consumer ownership test |
| F | Unsupported ticket schema | no activation or claim; local protection still runs | handshake/autonomy tests |
| G | Clock outside configured limit | no activation or new risk | clock-skew handshake test |
| H | Disk reaches 80/90% gates | optional work stops; predictor remains allowed; control data remains separate | resource governor and bounded stress probe |

Local acceptance command:

    python scripts/run_two_node_fault_acceptance.py --output two-node-fault-acceptance.json

The report must show status PASS, shared_sqlite false, execution_mode paper, live_count 0, mainnet_allowed false, and background_processes_remaining 0. CI repeats the same harness on the exact release SHA. A physical/VM drill remains the preferred deployment preflight and must use the same matrix before an operator installs either service.
