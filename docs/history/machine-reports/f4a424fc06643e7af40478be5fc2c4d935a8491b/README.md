# Archived machine reports

These files preserve the local machine state observed when the engineering
baseline `shadow-platform-v3-integration` was frozen at commit
`f4a424fc06643e7af40478be5fc2c4d935a8491b` on 2026-08-25.

The evaluation JSON files are historical outputs, not source-controlled truth
for the current runtime. Five of them contain the unfinished profitability
rebuild output that was present but uncommitted when the baseline was frozen.
The stop report records the same machine-local state and explicitly reports:

```text
profitability_gate=FAILED
candidate_count=0
live_count=0
mainnet=DISABLED
```

New runs write reports to `ai_bot3/ai_bot3/model_results/evaluation/`. That
directory is intentionally ignored so running a CLI cannot silently rewrite a
reviewed engineering snapshot.
