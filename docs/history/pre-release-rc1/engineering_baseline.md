# Shadow Platform v3 engineering baseline

Updated: 2026-08-25

## Frozen source

The complete engineering baseline is named `shadow-platform-v3-integration`
and is fixed at commit:

```text
f4a424fc06643e7af40478be5fc2c4d935a8491b
```

The source branch `codex/complete-profitability-alpha-v2` is preserved. The
behavior-preserving cleanup branch is
`refactor/shadow-platform-v3-clean`, created directly from that SHA.

"Complete engineering baseline" means the prediction/control-plane/execution
shadow loop, safety gates, PIT research path, evidence model, and regression
suite form a coherent base that can be maintained. It does not mean the
profitability gate passed, testnet/live was approved, or the baseline had
already received its structural cleanup.

## Current safety state

```text
profitability_gate=FAILED
candidate_count=0
live_count=0
mainnet=DISABLED
```

PR #4 represents the integration baseline and must not be merged as-is. Review
the cleanup branch separately after its locked CI and full regression evidence
are available.

## Cleanup boundaries

- Profitability CLI resolves HEAD in an ordinary Git clone and retains the
  local `.version-history` fallback.
- Machine-generated evaluation reports are ignored at their runtime path. The
  baseline machine snapshot is preserved under `docs/history/machine-reports/`.
- `.env.example` files cover workspace-shared, prediction, and execution
  configuration. Runtime data is declared by `runtime-data-manifest.json`.
- CI installs application dependencies from platform lock files and runs the
  prediction regression on Windows Python 3.12.
- `profitability_rebuild.py` is split into orchestration and reusable
  components without changing its old import surface.
- Bybit public PIT uses separate store, collector/ingestor, and audit modules;
  `bybit_public_pit.py` remains a compatibility facade.
- `shadow_contracts` is the single source for `OperationTicket` and
  `ExecutionReceipt`; both services keep compatibility facades.
- Superseded v1/v2, Phase 0, pre-v3 audit, work-order, and machine-output
  documents are under `docs/history/`.

The machine-readable source snapshot is `docs/current_head_manifest.json`.

## Clone and verify

Windows Python 3.12:

```powershell
python -m pip install --require-hashes -r requirements\windows-py312.lock
python -m pytest ai_bot3\ai_bot3\tests BybitContractBotV4\tests -q
python scripts\run_shadow_e2e.py
```

Ubuntu Python 3.11:

```bash
python -m pip install -r requirements/ubuntu-py311.lock
python -m pytest ai_bot3/ai_bot3/tests BybitContractBotV4/tests -q
python scripts/run_shadow_e2e.py
```

The profitability command is clone-safe, but a real evaluation still requires
the external runtime artifacts described by `runtime-data-manifest.json`:

```powershell
python ai_bot3\ai_bot3\scripts\run_profitability_rebuild.py --help
```
