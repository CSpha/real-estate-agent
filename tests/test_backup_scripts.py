from subprocess import CalledProcessError
from unittest.mock import patch

import psycopg2
import pytest

from scripts import verify_postgres_backup as verifier


@pytest.fixture(autouse=True)
def database_settings(monkeypatch):
    monkeypatch.setenv("DB_NAME", "realestate")


def test_other_database_backups_are_not_selected(tmp_path, monkeypatch):
    (tmp_path / "legacy_20260911.dump").write_bytes(b"other database")
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path))
    with patch.object(verifier, "connect") as connect:
        assert verifier.main() == 2
        connect.assert_not_called()


class FakeCursor:
    def __init__(self, execute_error=None):
        self.execute_error = execute_error
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, *args):
        self.statements.append(statement)
        if self.execute_error is not None:
            raise self.execute_error


class FakeConnection:
    def __init__(self, execute_error=None):
        self.cursor_obj = FakeCursor(execute_error)
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def make_dump(tmp_path):
    dump = tmp_path / "realestate_20260911T000000Z.dump"
    dump.write_bytes(b"mock dump")


def test_create_database_failure_never_attempts_drop(tmp_path, monkeypatch, capsys):
    make_dump(tmp_path)
    admin = FakeConnection(psycopg2.Error("create failed"))
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path))
    monkeypatch.setattr(verifier, "connect", lambda database: admin)

    assert verifier.main() == 1
    assert not any("DROP DATABASE" in str(item) for item in admin.cursor_obj.statements)
    assert "Restore verification failed" in capsys.readouterr().err


def test_restore_failure_drops_only_created_database(tmp_path, monkeypatch):
    make_dump(tmp_path)
    admin = FakeConnection()
    source = FakeConnection()
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path))

    monkeypatch.setattr(
        verifier,
        "connect",
        lambda database: admin if database == "postgres" else source,
    )
    monkeypatch.setattr(verifier, "snapshot", lambda connection: ({}, set(), set()))
    with patch.object(
        verifier.subprocess, "run", side_effect=CalledProcessError(1, ["pg_restore"])
    ):
        assert verifier.main() == 1

    drops = [
        str(item)
        for item in admin.cursor_obj.statements
        if "DROP DATABASE" in str(item)
    ]
    assert len(drops) == 1
    assert "restore_verify_" in drops[0]
    assert admin.closed


def test_restored_metadata_mismatch_fails_and_cleans_up(tmp_path, monkeypatch):
    make_dump(tmp_path)
    admin = FakeConnection()
    source = FakeConnection()
    restored = FakeConnection()
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path))

    def connect(database):
        if database == "postgres":
            return admin
        if database == "realestate":
            return source
        return restored

    monkeypatch.setattr(verifier, "connect", connect)
    snapshots = iter(
        [
            ({"public.listings": 2}, {("public", "listings_id_idx")}, set()),
            ({}, set(), set()),
        ]
    )
    monkeypatch.setattr(verifier, "snapshot", lambda connection: next(snapshots))
    with patch.object(verifier.subprocess, "run"):
        assert verifier.main() == 1

    assert any("DROP DATABASE" in str(item) for item in admin.cursor_obj.statements)
    assert admin.closed
