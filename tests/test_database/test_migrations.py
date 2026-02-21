"""Tests for schema migration system."""

import sqlite3
from pathlib import Path

import pytest

from src.database.repository import Repository

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


@pytest.fixture
def repo():
    r = Repository(":memory:")
    yield r
    r.close()


class TestMigrationApply:
    def test_creates_all_tables(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        tables = {
            row[0]
            for row in repo.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        expected = {
            "schema_version", "imports", "transactions",
            "allocations", "transfers", "receipt_matches", "api_usage",
        }
        assert expected.issubset(tables)

    def test_tracks_version(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        row = repo.conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        assert row[0] == 3

    def test_idempotent(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        repo.apply_migrations(MIGRATIONS_DIR)  # second run
        row = repo.conn.execute(
            "SELECT COUNT(*) FROM schema_version"
        ).fetchone()
        assert row[0] == 3  # one record per migration

    def test_receipt_lookup_status_column_exists(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        cols = {
            row[1]
            for row in repo.conn.execute("PRAGMA table_info(transactions)").fetchall()
        }
        assert "receipt_lookup_status" in cols

    def test_creates_indexes(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        indexes = {
            row[0]
            for row in repo.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        expected_indexes = {
            "idx_imports_file_hash",
            "idx_txn_account_date",
            "idx_txn_external_id",
            "idx_txn_import_hash",
            "idx_txn_dedup_key",
            "idx_txn_status",
            "idx_txn_import_id",
            "idx_alloc_txn",
            "idx_alloc_category",
            "idx_xfer_from",
            "idx_xfer_to",
            "idx_txn_normalized_desc",
        }
        assert expected_indexes.issubset(indexes)


class TestForeignKeys:
    def test_foreign_keys_enabled(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        row = repo.conn.execute("PRAGMA foreign_keys").fetchone()
        assert row[0] == 1

    def test_transaction_requires_valid_import(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        with pytest.raises(sqlite3.IntegrityError):
            repo.conn.execute(
                "INSERT INTO transactions"
                " (id, account_id, date, amount, raw_description,"
                "  import_id, import_hash, dedup_key)"
                " VALUES ('t1','acct','2026-01-01',-10.0,'TEST',"
                "  'nonexistent_import','hash','key')"
            )

    def test_allocation_requires_valid_transaction(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        with pytest.raises(sqlite3.IntegrityError):
            repo.conn.execute(
                "INSERT INTO allocations"
                " (id, transaction_id, category_id, amount)"
                " VALUES ('a1','nonexistent_txn','groceries',-10.0)"
            )


class TestUniqueConstraints:
    def test_imports_file_hash_unique(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        repo.conn.execute(
            "INSERT INTO imports (id, file_name, file_hash, status)"
            " VALUES ('i1','f1.qfx','abc123','pending')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            repo.conn.execute(
                "INSERT INTO imports (id, file_name, file_hash, status)"
                " VALUES ('i2','f2.qfx','abc123','pending')"
            )

    def test_api_usage_month_service_unique(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        repo.conn.execute(
            "INSERT INTO api_usage (id, month, service)"
            " VALUES ('u1','2026-01','claude_categorize')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            repo.conn.execute(
                "INSERT INTO api_usage (id, month, service)"
                " VALUES ('u2','2026-01','claude_categorize')"
            )

    def test_transfer_pair_unique(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        # Create the prerequisite import and transactions
        repo.conn.execute(
            "INSERT INTO imports (id, file_name, file_hash, status)"
            " VALUES ('imp1','f.qfx','hash1','completed')"
        )
        for tid in ("t1", "t2"):
            repo.conn.execute(
                "INSERT INTO transactions"
                " (id, account_id, date, amount, raw_description,"
                "  import_id, import_hash, dedup_key)"
                f" VALUES ('{tid}','acct','2026-01-01',-10.0,'TEST',"
                "  'imp1','ihash','dkey')"
            )
        repo.conn.execute(
            "INSERT INTO transfers"
            " (id, from_transaction_id, to_transaction_id,"
            "  transfer_type, match_method)"
            " VALUES ('x1','t1','t2','cc-payment','known_pattern')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            repo.conn.execute(
                "INSERT INTO transfers"
                " (id, from_transaction_id, to_transaction_id,"
                "  transfer_type, match_method)"
                " VALUES ('x2','t1','t2','cc-payment','known_pattern')"
            )
