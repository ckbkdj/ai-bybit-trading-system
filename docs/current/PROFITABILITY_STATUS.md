# Current profitability status

```text
profitability_evidence=STALE_NOT_REGENERATED
profitability_gate=FAILED
candidate_count=0
live_count=0
mainnet_allowed=false
```

No profitability evaluation was run for the RC. Historical reports are under
`docs/evidence/history/<original-code-sha>/` and do not apply to the current
code. Zero candidates, zero live releases and zero trades are a fail-closed
state, not success.

The fee-adjusted win-rate, profit-factor, drawdown, positive-fold, doubled-cost
return and bootstrap lower-bound gates remain unchanged. Fees, slippage,
funding, latency and partial fills remain mandatory. Profitability work and a
new lockbox require a separate task.
