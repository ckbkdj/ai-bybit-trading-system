# Incident modes

- Prediction stale, malformed or unavailable: emit no executable ticket.
- Candidate/model/data hash mismatch: fail candidate authorization.
- Control-plane or execution DB integrity/migration failure: refuse startup.
- Private WebSocket loss, ambiguous REST timeout or cancel/fill race: stop new
  exposure and reconcile authoritative exchange state.
- Duplicate ticket/order/fill: retain the first immutable identity and reject
  conflicting content.
- Clock drift, daily/weekly loss, drawdown or margin breach: engage the durable
  kill switch; UTC rollover must not silently clear unresolved incidents.
- Unknown/manual position on a dedicated account: block execution and require
  human reconciliation.
- Credential exposure: revoke externally, preserve evidence, rotate, and rerun
  current-tree plus reachable-history secret scans.

Incident response must preserve databases and reports. Do not delete evidence
or restart into a mode that can place orders before reconciliation completes.
