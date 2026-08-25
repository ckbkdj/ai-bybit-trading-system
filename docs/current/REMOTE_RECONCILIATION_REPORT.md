# Remote reconciliation report

- Repository: `github.com/ckbkdj/ai-bybit-trading-system`
- Remote main: `88641b68a8bb1a7f2011bd8076dd7788976d3317`
- Frozen PR #4 head: `f4a424fc06643e7af40478be5fc2c4d935a8491b`
- Recovered local source head: `9cb1bb4bb695b4c2fdebec662371ca73acb8f56c`
- Recovery ref: `recovery/codex-alpha-local-head-20260825`

The remote recovery ref was verified by `ls-remote` to equal the recovered
local head. `main` is the ancestor of the 169-commit PR #4 chain, and the
recovered head adds one behavior-preserving cleanup commit. No uncommitted
source existed at reconciliation time. The repository uses the legacy
`.version-history` metadata layout locally; no second history was initialized.

PR #4 remains frozen and unmerged. RC1 was created from the latest `main` and
integrates the recovered final source state as one squash commit.
