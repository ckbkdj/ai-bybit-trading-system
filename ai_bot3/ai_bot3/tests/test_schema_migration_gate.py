from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from core.control_plane import ControlPlaneRepository, SCHEMA_VERSION


def test_control_plane_refuses_newer_database(tmp_path):
    path = tmp_path / "future-control.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO schema_migrations(version,applied_at) VALUES (?, 'future')",
            (SCHEMA_VERSION + 1,),
        )
        connection.commit()
    with pytest.raises(RuntimeError, match="newer than supported"):
        ControlPlaneRepository(path)


def test_control_plane_records_version_checksum_and_commit(tmp_path):
    repository = ControlPlaneRepository(tmp_path / "control.sqlite3")
    with repository.connect() as connection:
        row = connection.execute(
            "SELECT * FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
        ).fetchone()
    assert row["migration_id"]
    assert row["schema_checksum"]
    assert row["code_commit"]


def test_control_plane_refuses_checksum_drift(tmp_path):
    path = tmp_path / "drift-control.sqlite3"
    repository = ControlPlaneRepository(path)
    with repository.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE schema_migrations SET schema_checksum='tampered' WHERE version=?",
            (SCHEMA_VERSION,),
        )
    with pytest.raises(RuntimeError, match="checksum"):
        ControlPlaneRepository(path)
