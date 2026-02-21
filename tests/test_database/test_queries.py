"""Tests for complex queries in queries.py."""

from pathlib import Path

import pytest

from src.database.models import Allocation, Import, Transaction, Transfer
from src.database.repository import Repository
from src.database.queries import (
    batch_is_transfer,
    get_category_summary,
    get_reconciliation_data,
    get_status_counts,
    get_transactions_with_transfer_flag,
    is_transfer,
)

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


@pytest.fixture
def repo():
    r = Repository(":memory:")
    r.apply_migrations(MIGRATIONS_DIR)
    yield r
    r.close()


@pytest.fixture
def imp(repo):
    return repo.insert_import(
        Import(file_name="test.qfx", file_hash="qhash")
    )


def _txn(imp_id, **kw):
    defaults = dict(
        account_id="wf-checking", date="2026-01-15", amount=-50.0,
        raw_description="TEST", import_id=imp_id,
        import_hash="ih", dedup_key="dk",
    )
    defaults.update(kw)
    return Transaction(**defaults)


# ── is_transfer ────────────────────────────────────────────


class TestIsTransfer:
    def test_false_when_no_transfer(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        assert is_transfer(repo.conn, txn.id) is False

    def test_true_when_from_side(self, repo, imp):
        t1 = _txn(imp.id, amount=-100.0, import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, account_id="cap1-credit", amount=100.0,
                   import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transfer(Transfer(
            from_transaction_id=t1.id, to_transaction_id=t2.id,
            transfer_type="cc-payment", match_method="known_pattern",
        ))
        assert is_transfer(repo.conn, t1.id) is True

    def test_true_when_to_side(self, repo, imp):
        t1 = _txn(imp.id, amount=-100.0, import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, account_id="cap1-credit", amount=100.0,
                   import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transfer(Transfer(
            from_transaction_id=t1.id, to_transaction_id=t2.id,
            transfer_type="cc-payment", match_method="known_pattern",
        ))
        assert is_transfer(repo.conn, t2.id) is True


class TestBatchIsTransfer:
    def test_empty_list(self, repo):
        assert batch_is_transfer(repo.conn, []) == set()

    def test_no_transfers(self, repo, imp):
        t1 = _txn(imp.id, import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        assert batch_is_transfer(repo.conn, [t1.id, t2.id]) == set()

    def test_returns_both_sides(self, repo, imp):
        t1 = _txn(imp.id, amount=-100.0, import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, account_id="cap1-credit", amount=100.0,
                   import_hash="h2", dedup_key="k2")
        t3 = _txn(imp.id, amount=-25.0, import_hash="h3", dedup_key="k3")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transaction(t3)
        repo.insert_transfer(Transfer(
            from_transaction_id=t1.id, to_transaction_id=t2.id,
            transfer_type="cc-payment", match_method="known_pattern",
        ))
        result = batch_is_transfer(repo.conn, [t1.id, t2.id, t3.id])
        assert t1.id in result
        assert t2.id in result
        assert t3.id not in result

    def test_subset_query(self, repo, imp):
        t1 = _txn(imp.id, amount=-100.0, import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, account_id="cap1-credit", amount=100.0,
                   import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transfer(Transfer(
            from_transaction_id=t1.id, to_transaction_id=t2.id,
            transfer_type="cc-payment", match_method="known_pattern",
        ))
        # Query only one side
        result = batch_is_transfer(repo.conn, [t1.id])
        assert result == {t1.id}


# ── get_transactions_with_transfer_flag ────────────────────


class TestTransactionsWithTransferFlag:
    def test_flag_derived_correctly(self, repo, imp):
        t1 = _txn(imp.id, amount=-800.0, raw_description="CC PMT",
                   import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, amount=-50.0, raw_description="PHILZ COFFEE",
                   import_hash="h2", dedup_key="k2")
        t3 = _txn(imp.id, account_id="cap1-credit", amount=800.0,
                   raw_description="PAYMENT", import_hash="h3", dedup_key="k3")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transaction(t3)
        repo.insert_transfer(Transfer(
            from_transaction_id=t1.id, to_transaction_id=t3.id,
            transfer_type="cc-payment", match_method="known_pattern",
        ))
        rows = get_transactions_with_transfer_flag(repo.conn, "wf-checking")
        by_id = {r["id"]: r for r in rows}
        assert by_id[t1.id]["is_transfer"] == 1
        assert by_id[t2.id]["is_transfer"] == 0

    def test_date_filtering(self, repo, imp):
        for i, d in enumerate(["2026-01-10", "2026-01-20", "2026-01-30"]):
            t = _txn(imp.id, date=d, import_hash=f"h{i}", dedup_key=f"k{i}")
            repo.insert_transaction(t)
        rows = get_transactions_with_transfer_flag(
            repo.conn, "wf-checking",
            date_from="2026-01-15", date_to="2026-01-25",
        )
        assert len(rows) == 1
        assert rows[0]["date"] == "2026-01-20"


# ── get_category_summary ──────────────────────────────────


class TestCategorySummary:
    def test_aggregates_by_category(self, repo, imp):
        t1 = _txn(imp.id, date="2026-01-15", amount=-50.0,
                   import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, date="2026-01-20", amount=-30.0,
                   import_hash="h2", dedup_key="k2")
        t3 = _txn(imp.id, date="2026-01-25", amount=-20.0,
                   import_hash="h3", dedup_key="k3")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transaction(t3)
        repo.insert_allocation(Allocation(
            transaction_id=t1.id, category_id="groceries", amount=-50.0,
        ))
        repo.insert_allocation(Allocation(
            transaction_id=t2.id, category_id="groceries", amount=-30.0,
        ))
        repo.insert_allocation(Allocation(
            transaction_id=t3.id, category_id="coffee-d", amount=-20.0,
        ))
        summary = get_category_summary(repo.conn, "2026-01")
        by_cat = {r["category_id"]: r for r in summary}
        assert by_cat["groceries"]["total"] == -80.0
        assert by_cat["groceries"]["txn_count"] == 2
        assert by_cat["coffee-d"]["total"] == -20.0

    def test_excludes_other_months(self, repo, imp):
        t1 = _txn(imp.id, date="2026-01-15", import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, date="2026-02-15", import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_allocation(Allocation(
            transaction_id=t1.id, category_id="groceries", amount=-50.0,
        ))
        repo.insert_allocation(Allocation(
            transaction_id=t2.id, category_id="groceries", amount=-30.0,
        ))
        summary = get_category_summary(repo.conn, "2026-01")
        assert len(summary) == 1
        assert summary[0]["total"] == -50.0


# ── get_reconciliation_data ────────────────────────────────


class TestReconciliation:
    def test_balanced(self, repo, imp):
        # Two transactions summing to -150, with last balance = -150
        t1 = _txn(imp.id, date="2026-01-10", amount=-100.0,
                   import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, date="2026-01-15", amount=-50.0,
                   balance=-150.0, import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        result = get_reconciliation_data(repo.conn, "wf-checking")
        assert result["status"] == "balanced"

    def test_discrepancy(self, repo, imp):
        t1 = _txn(imp.id, date="2026-01-10", amount=-100.0,
                   import_hash="h1", dedup_key="k1")
        t2 = _txn(imp.id, date="2026-01-15", amount=-50.0,
                   balance=-999.0, import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        result = get_reconciliation_data(repo.conn, "wf-checking")
        assert result["status"] == "discrepancy"
        assert result["difference"] > 0

    def test_no_balance_data(self, repo, imp):
        t = _txn(imp.id, import_hash="h1", dedup_key="k1")
        repo.insert_transaction(t)
        result = get_reconciliation_data(repo.conn, "wf-checking")
        assert result["status"] == "no_balance_data"

    def test_empty_account(self, repo):
        result = get_reconciliation_data(repo.conn, "wf-checking")
        assert result["status"] == "no_balance_data"


# ── get_status_counts ──────────────────────────────────────


class TestStatusCounts:
    def test_empty_db(self, repo):
        counts = get_status_counts(repo.conn)
        assert counts["total_txns"] == 0
        assert counts["pending"] == 0
        assert counts["flagged"] == 0

    def test_counts_match(self, repo, imp):
        for i in range(3):
            t = _txn(imp.id, import_hash=f"h{i}", dedup_key=f"k{i}")
            repo.insert_transaction(t)
        repo.update_transaction_status(
            repo.get_uncategorized_transactions(1)[0].id,
            "categorized", 0.9, "merchant_auto",
        )
        counts = get_status_counts(repo.conn)
        assert counts["total_txns"] == 3
        assert counts["pending"] == 2
        assert counts["categorized"] == 1
        assert counts["total_imports"] == 1
