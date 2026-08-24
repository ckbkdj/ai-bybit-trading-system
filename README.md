# Versioned AI-to-Bybit Shadow Platform

This repository is a complete, maintainable engineering baseline for a
fail-closed prediction to Bybit execution system. It is not evidence of
profitability and it is not permission to trade live.

The frozen integration baseline is `shadow-platform-v3-integration` at
`f4a424fc06643e7af40478be5fc2c4d935a8491b`. Behavior-preserving cleanup is
isolated on `refactor/shadow-platform-v3-clean`; the source branch is retained.

Current safety state:

```text
profitability_gate=FAILED
candidate_count=0
live_count=0
mainnet=DISABLED
```

## Start here

- Baseline and refactor boundary: `docs/engineering_baseline.md`
- Architecture: `docs/architecture_v3_release_candidate.md`
- Profitability research path: `docs/profitability_first_alpha_rebuild.md`
- Runtime data contract: `runtime-data-manifest.json`
- Operations: `docs/operations_runbook.md`
- User guide: `docs/user_guide_v3.md`
- Mainnet default-NO-GO checklist: `docs/mainnet_go_live_checklist.md`
- Historical material: `docs/history/`

## Ordinary clone setup

Use the lock matching CI. On Windows with Python 3.12:

```powershell
python -m pip install --require-hashes -r requirements\windows-py312.lock
```

On Ubuntu with Python 3.11:

```bash
python -m pip install -r requirements/ubuntu-py311.lock
```

Copy only the relevant values from the three configuration templates:

- `.env.example` for shared process values;
- `ai_bot3/ai_bot3/.env.example` for prediction/research;
- `BybitContractBotV4/.env.example` for execution.

Never commit real credentials or runtime databases.

## Verify

```powershell
python -m pytest ai_bot3\ai_bot3\tests -q
python -m pytest BybitContractBotV4\tests -q
python scripts\run_shadow_e2e.py
```

The profitability CLI now works in a normal Git clone:

```powershell
python ai_bot3\ai_bot3\scripts\run_profitability_rebuild.py --help
```

Running the evaluation itself requires the external, untracked data declared in
`runtime-data-manifest.json`. Missing evidence fails closed; it must never be
filled with synthetic defaults.

PR #4 should not be merged unchanged. It is the engineering integration
baseline, not the reviewed structural-cleanup result.
