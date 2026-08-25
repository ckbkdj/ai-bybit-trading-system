# Version rollback

A rollback unit is code commit, dependency lock, environment profile, schema
version, runtime-data snapshot and release manifest. Record all six before
deployment.

Stop writers, activate the kill switch, snapshot current state, and check the
target binary's maximum supported schema. Roll back code without data only when
the schema and contracts are backward compatible. Otherwise restore the exact
pre-migration snapshot to a new path. Run migrations, integrity checks,
restart/reconcile and Shadow E2E before traffic returns.

Rollback never changes profitability evidence, opens a lockbox, enables live
mode or bypasses the approved release ID.
