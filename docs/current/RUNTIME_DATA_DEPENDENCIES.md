# Runtime data dependencies

`runtime-data-manifest.v1.json` is the authoritative inventory and
`schemas/runtime-data-manifest.v1.schema.json` validates its shape. Every entry
declares the logical name, path environment variable, consumers, schema,
access mode, backup requirement, secret classification, minimum coverage and
health check.

Git contains no runtime databases, model bundles, market archives, credentials
or generated evaluation reports. A fresh clone may initialize empty Shadow
control/execution state. Research, candidate emission and Testnet fail closed
when their external artifacts are absent, stale, incomplete or hash-mismatched.

The lockbox path is deliberately empty and must remain unavailable until a new
development gate passes. Previously consumed lockboxes are never reused.
