# Executor autonomy runbook

## Startup order

Validate local TLS/mTLS material and unique token, then capabilities and minimum executor version, cluster/deployment identity, control-plane clock, exchange reconciliation, account ownership, receipt retry, expired cursor fast-forward, superseded/latest decision eligibility, and READY. A failed schema/version/time/ownership check freezes ticket intake and prevents claims.

## Predictor or network outage

Enter FREEZE_NEW_RISK without treating the outage as exchange reconciliation failure. OPEN, INCREASE, and REPLACE are blocked. Continue:

- exchange order/fill/position reconciliation;
- existing server-side stop verification;
- reduce-only take profit recovery;
- max-holding reduce-only exit;
- cancel, close, and reduce;
- durable local receipt enqueue and retry scheduling.

Risk reduction must not query predictor market regime. Do not set a temporary transport outage to permanent kill switch; authentication failure freezes risk, while conflicting receipt content dead-letters and sets the kill switch for human review.

## Recovery

Reconcile exchange state first. Re-verify dedicated ownership, renew the single active consumer/account lease, retry each due receipt independently, fast-forward expired backlog, bulk-skip superseded decisions, and process only the latest eligible decision per symbol. Permit new risk only after READY.

For dead letters, compare the immutable receipt hash and remote content. Acknowledge only after operator resolution. Never delete the execution ledger to clear a delivery issue.
