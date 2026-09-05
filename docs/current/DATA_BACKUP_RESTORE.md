# Data backup and restore

For every artifact with `backup_required=true`:

1. Stop or quiesce the sole writer and record code commit and schema version.
2. For SQLite, run a WAL checkpoint and integrity check. Preserve the database
   plus any required WAL/SHM files as one atomic snapshot.
3. Hash the snapshot, encrypt it at rest and store it outside the deployment.
4. Restore into a new path, verify hashes and `PRAGMA integrity_check`, then run
   the application migration gate without placing orders.
5. Promote only after reconciliation proves there are no unknown tickets,
   orders, fills, positions or outbox records.

Never downgrade an in-place database. If an older binary rejects a newer
schema, restore the matching pre-upgrade snapshot instead.
