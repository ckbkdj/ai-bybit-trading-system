# Codex Profitability Completion Contract

Authoritative work order: GitHub Issue #3.

## Current status

```text
profitability_gate=FAILED
candidate_count=0
live_count=0
signals=0
trades=0
net_return=0
```

The project is incomplete. Zero trades and zero drawdown are not profitability and must not be reported as completion.

## Required first actions

1. Integrate the still-valid safety changes from `review/harden-async-cancel-ci-20260823` into this branch without discarding the profitability work.
2. Restore one authoritative horizon mapping:

```text
scalping=180
mid_short=900
trend=7200
trend_swing=14400
swing=86400
```

3. Close full-sample regime leakage, in-sample meta-label training, and in-sample residual quantile calibration.
4. Do not reuse lockbox fingerprint `893488f8cee82c568316cd54c6ec0017bf39d685ea17dc1aab95ed4a9a299741`.
5. Do not open a new lockbox until the development gate in Issue #3 passes.
6. Keep all execution in shadow. Never enable mainnet or auto-promote live.

## Completion condition

This branch is not complete until a fresh, once-only lockbox produces all of the following after fees, spread, slippage and funding:

```text
trade_count >= 100 overall
trade_count >= 30 per enabled horizon
net_return > 0
fee-adjusted win_rate >= 52%
profit_factor >= 1.20
bootstrap 95% lower expectancy > 0
positive walk-forward folds >= 60%
2x cost stress net return >= 0
max intratrade drawdown <= 3%
execution evidence complete
PIT factor ablations complete
production replay equals offline inference
all CI/authenticity tests pass
```

At most create `stage=candidate`, `candidate_count=1`, `live_count=0`. Never create live automatically.

## Not acceptable as completion

- 0 signals or 0 trades;
- only unit tests passing;
- only interfaces, documentation or failure reports completed;
- OHLCV proxies marked as real execution evidence;
- lowering thresholds, omitting costs, reusing lockbox data, or tuning after seeing lockbox results;
- leaving multi-factor groups as static `NOT_EVALUATED` placeholders;
- leaving `alpha_prediction` disconnected from production inference.

The detailed phased checklist, data requirements, tests, reports and release rules are in Issue #3. Keep that issue open until every completion condition is backed by reproducible machine evidence.