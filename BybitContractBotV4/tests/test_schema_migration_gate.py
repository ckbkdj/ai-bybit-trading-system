from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ticket_store import ExecutionStore, SCHEMA_VERSION


class ExecutionSchemaMigrationTests(unittest.TestCase):
    def test_execution_store_refuses_newer_database(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "future.sqlite3"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    """CREATE TABLE schema_migrations(
                       version INTEGER PRIMARY KEY,applied_at TEXT NOT NULL)"""
                )
                connection.execute(
                    "INSERT INTO schema_migrations(version,applied_at) VALUES (?, 'future')",
                    (SCHEMA_VERSION + 1,),
                )
                connection.commit()
            with self.assertRaisesRegex(RuntimeError, "newer than supported"):
                ExecutionStore(path)

    def test_execution_store_records_version_checksum_and_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ExecutionStore(Path(directory) / "execution.sqlite3")
            with closing(store.connect()) as connection:
                row = connection.execute(
                    "SELECT * FROM schema_migrations WHERE version=?",
                    (SCHEMA_VERSION,),
                ).fetchone()
            self.assertTrue(row["migration_id"])
            self.assertTrue(row["schema_checksum"])
            self.assertTrue(row["code_commit"])

    def test_execution_store_refuses_checksum_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution.sqlite3"
            store = ExecutionStore(path)
            with store.transaction(immediate=True) as connection:
                connection.execute(
                    """UPDATE schema_migrations SET schema_checksum='tampered'
                       WHERE version=?""",
                    (SCHEMA_VERSION,),
                )
            with self.assertRaisesRegex(RuntimeError, "checksum"):
                ExecutionStore(path)


if __name__ == "__main__":
    unittest.main()
