# Supply-chain gates

Supported application locks are `requirements/windows-py312.lock`,
`requirements/windows-py311.lock` and `requirements/linux-py311.lock`. Each is
a target-platform transitive closure with SHA-256 hashes. Audit tooling is also
hash-locked in `requirements/audit.lock`.

CI installs with `--require-hashes`, validates `package-lock.json`, runs
`pip-audit` and `npm audit`, emits a CycloneDX JSON SBOM, scans the current tree
and all reachable Git blobs for secrets, and uploads the timestamped reports.
The vulnerability source is recorded honestly as a live PyPI/OSV query when it
does not publish a versioned database snapshot.

Any finding, skipped package, missing locked dependency, invalid hash, missing
Node integrity value, incomplete history scan or absent current attestation
blocks the release.

## Current local attestation

The release worktree was scanned on 2026-08-25 UTC. These reports are generated
locally and reproduced as CI artifacts; they are not used as source inputs.

- `pip-audit` 2.10.1 queried the live PyPI advisory database through OSV at
  `2026-08-25T02:27:53Z`. The provider does not publish a version identifier, so
  the database version is recorded as `unversioned-live-query`. All 91 locked
  Linux distributions were audited: 0 findings, 0 skipped, 0 missing.
- `npm` 11.3.0 completed audit report version 2 at
  `2026-08-25T02:29:52Z`: 2 resolved dependencies, 0 vulnerabilities.
- The CycloneDX 1.4 SBOM contains 91 components. Its local artifact SHA-256 is
  `1649a948395f3ec2299f93df1def2ae61bc0d3083f84618c54902c551fca28cb`.
- The combined fail-closed audit completed at `2026-08-25T02:29:10Z` with
  status `PASS`; it scanned 1,088 reachable Git blobs and found no credentials.
  The pip-audit report SHA-256 is
  `784b0871c1e016e4e278c9bfb945273e603cdeedec44895cd967c16f7d6e522d`.
