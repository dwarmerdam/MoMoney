"""Tests for Repository CRUD operations."""

from pathlib import Path

import pytest

from src.database.models import (
    Allocation,
    Import,
    ReceiptMatch,
    Transaction,
    Transfer,
)
from src.database.repository import Repository

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


@pytest.fixture
def repo():
    r = Repository(":memory:")
    r.apply_migrations(MIGRATIONS_DIR)
    yield r
    r.close()


@pytest.fixture
def sample_import():
    return Import(file_name="test.qfx", file_hash="sha256_abc")


@pytest.fixture
def sample_import_inserted(repo, sample_import):
    return repo.insert_import(sample_import)


def _make_txn(import_id: str, **overrides) -> Transaction:
    defaults = dict(
        account_id="wf-checking",
        date="2026-01-15",
        amount=-50.00,
        raw_description="PHILZ COFFEE SF",
        import_id=import_id,
        import_hash="hash_abc",
        dedup_key="wf-checking:2026-01-15:-5000",
    )
    defaults.update(overrides)
    return Transaction(**defaults)


# ── Import CRUD ────────────────────────────────────────────


class TestImportCrud:
    def test_insert_and_retrieve_by_hash(self, repo, sample_import):
        repo.insert_import(sample_import)
        found = repo.get_import_by_hash("sha256_abc")
        assert found is not None
        assert found.file_name == "test.qfx"
        assert found.status == "pending"

    def test_returns_none_for_unknown_hash(self, repo):
        assert repo.get_import_by_hash("nonexistent") is None

    def test_update_status(self, repo, sample_import_inserted):
        repo.update_import_status(
            sample_import_inserted.id, "completed",
            record_count=42, completed_at="2026-01-15T10:00:00",
        )
        found = repo.get_import_by_hash("sha256_abc")
        assert found.status == "completed"
        assert found.record_count == 42
        assert found.completed_at == "2026-01-15T10:00:00"

    def test_update_status_with_error(self, repo, sample_import_inserted):
        repo.update_import_status(
            sample_import_inserted.id, "failed",
            error_message="Parse error on line 42",
        )
        found = repo.get_import_by_hash("sha256_abc")
        assert found.status == "failed"
        assert "Parse error" in found.error_message


# ── Transaction CRUD ───────────────────────────────────────


class TestTransactionCrud:
    def test_insert_and_get(self, repo, sample_import_inserted):
        txn = _make_txn(sample_import_inserted.id)
        repo.insert_transaction(txn)
        found = repo.get_transaction(txn.id)
        assert found is not None
        assert found.amount == -50.00
        assert found.raw_description == "PHILZ COFFEE SF"
        assert found.status == "pending"

    def test_get_by_external_id(self, repo, sample_import_inserted):
        txn = _make_txn(
            sample_import_inserted.id,
            external_id="202601151",
        )
        repo.insert_transaction(txn)
        found = repo.get_transaction_by_external_id(
            "wf-checking", "202601151"
        )
        assert found is not None
        assert found.id == txn.id

    def test_get_by_external_id_wrong_account(self, repo, sample_import_inserted):
        txn = _make_txn(
            sample_import_inserted.id,
            external_id="202601151",
        )
        repo.insert_transaction(txn)
        found = repo.get_transaction_by_external_id(
            "amex-blue", "202601151"
        )
        assert found is None

    def test_get_by_import_hash(self, repo, sample_import_inserted):
        txn = _make_txn(sample_import_inserted.id)
        repo.insert_transaction(txn)
        found = repo.get_transactions_by_import_hash("hash_abc")
        assert len(found) == 1

    def test_get_by_dedup_key(self, repo, sample_import_inserted):
        txn = _make_txn(sample_import_inserted.id)
        repo.insert_transaction(txn)
        found = repo.get_transactions_by_dedup_key(
            "wf-checking:2026-01-15:-5000"
        )
        assert len(found) == 1

    def test_get_uncategorized(self, repo, sample_import_inserted):
        t1 = _make_txn(sample_import_inserted.id, import_hash="h1", dedup_key="k1")
        t2 = _make_txn(sample_import_inserted.id, import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.update_transaction_status(t1.id, "categorized", 0.95, "merchant_auto")
        pending = repo.get_uncategorized_transactions()
        assert len(pending) == 1
        assert pending[0].id == t2.id

    def test_update_status(self, repo, sample_import_inserted):
        txn = _make_txn(sample_import_inserted.id)
        repo.insert_transaction(txn)
        repo.update_transaction_status(
            txn.id, "categorized", confidence=0.95, method="merchant_auto"
        )
        found = repo.get_transaction(txn.id)
        assert found.status == "categorized"
        assert found.confidence == 0.95
        assert found.categorization_method == "merchant_auto"

    def test_batch_insert(self, repo, sample_import_inserted):
        txns = [
            _make_txn(sample_import_inserted.id, import_hash=f"h{i}", dedup_key=f"k{i}")
            for i in range(5)
        ]
        repo.insert_transactions_batch(txns)
        pending = repo.get_uncategorized_transactions(limit=10)
        assert len(pending) == 5

    def test_get_for_account_with_date_range(self, repo, sample_import_inserted):
        dates = ["2026-01-10", "2026-01-15", "2026-01-20"]
        for i, d in enumerate(dates):
            txn = _make_txn(
                sample_import_inserted.id,
                date=d, import_hash=f"h{i}", dedup_key=f"k{i}",
            )
            repo.insert_transaction(txn)
        results = repo.get_transactions_for_account(
            "wf-checking", date_from="2026-01-12", date_to="2026-01-18"
        )
        assert len(results) == 1
        assert results[0].date == "2026-01-15"

    def test_returns_none_for_missing(self, repo):
        assert repo.get_transaction("nonexistent") is None


# ── Allocation CRUD ────────────────────────────────────────


class TestAllocationCrud:
    def test_insert_and_get(self, repo, sample_import_inserted):
        txn = _make_txn(sample_import_inserted.id)
        repo.insert_transaction(txn)
        alloc = Allocation(
            transaction_id=txn.id,
            category_id="coffee-d",
            amount=-50.00,
            tags="shared",
            confidence=0.88,
        )
        repo.insert_allocation(alloc)
        found = repo.get_allocations_by_transaction(txn.id)
        assert len(found) == 1
        assert found[0].category_id == "coffee-d"
        assert found[0].tags == "shared"

    def test_multiple_allocations_split(self, repo, sample_import_inserted):
        txn = _make_txn(
            sample_import_inserted.id, amount=-100.00,
            raw_description="APPLE.COM/BILL",
        )
        repo.insert_transaction(txn)
        allocs = [
            Allocation(transaction_id=txn.id, category_id="youtube-premium", amount=-22.99),
            Allocation(transaction_id=txn.id, category_id="cloud-storage", amount=-9.99),
            Allocation(transaction_id=txn.id, category_id="movie-rentals", amount=-67.02),
        ]
        repo.insert_allocations_batch(allocs)
        found = repo.get_allocations_by_transaction(txn.id)
        assert len(found) == 3
        total = sum(a.amount for a in found)
        assert abs(total - (-100.00)) < 0.01

    def test_tags_comma_separated(self, repo, sample_import_inserted):
        txn = _make_txn(sample_import_inserted.id)
        repo.insert_transaction(txn)
        alloc = Allocation(
            transaction_id=txn.id, category_id="groceries",
            amount=-50.00, tags="shared,tax-deductible",
        )
        repo.insert_allocation(alloc)
        found = repo.get_allocations_by_transaction(txn.id)
        assert "shared" in found[0].tags
        assert "tax-deductible" in found[0].tags

    def test_batch_get_by_ids(self, repo, sample_import_inserted):
        t1 = _make_txn(sample_import_inserted.id, import_hash="h1", dedup_key="k1")
        t2 = _make_txn(sample_import_inserted.id, import_hash="h2", dedup_key="k2")
        t3 = _make_txn(sample_import_inserted.id, import_hash="h3", dedup_key="k3")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transaction(t3)
        repo.insert_allocation(Allocation(
            transaction_id=t1.id, category_id="groceries", amount=-50.00,
        ))
        repo.insert_allocation(Allocation(
            transaction_id=t2.id, category_id="coffee-d", amount=-5.00,
        ))
        # t3 has no allocation
        result = repo.get_allocations_by_transaction_ids([t1.id, t2.id, t3.id])
        assert len(result) == 2
        assert t1.id in result
        assert t2.id in result
        assert t3.id not in result
        assert result[t1.id][0].category_id == "groceries"

    def test_batch_get_empty_list(self, repo):
        assert repo.get_allocations_by_transaction_ids([]) == {}

    def test_get_transactions_by_import_id(self, repo, sample_import_inserted):
        t1 = _make_txn(sample_import_inserted.id, import_hash="h1", dedup_key="k1")
        t2 = _make_txn(sample_import_inserted.id, import_hash="h2", dedup_key="k2")
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        found = repo.get_transactions_by_import_id(sample_import_inserted.id)
        assert len(found) == 2
        ids = {t.id for t in found}
        assert t1.id in ids
        assert t2.id in ids


# ── Transfer CRUD ──────────────────────────────────────────


class TestTransferCrud:
    def _make_transfer_pair(self, repo, imp_id):
        t1 = _make_txn(
            imp_id, account_id="wf-checking",
            amount=-800.00, raw_description="CAPITAL ONE MOBILE PMT",
            import_hash="from_h", dedup_key="from_k",
        )
        t2 = _make_txn(
            imp_id, account_id="cap1-credit",
            amount=800.00, raw_description="PAYMENT THANK YOU",
            import_hash="to_h", dedup_key="to_k",
        )
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        return t1, t2

    def test_insert_and_get(self, repo, sample_import_inserted):
        t1, t2 = self._make_transfer_pair(repo, sample_import_inserted.id)
        xfer = Transfer(
            from_transaction_id=t1.id,
            to_transaction_id=t2.id,
            transfer_type="cc-payment",
            match_method="known_pattern",
            confidence=1.0,
        )
        repo.insert_transfer(xfer)
        found = repo.get_transfer_by_transaction(t1.id)
        assert found is not None
        assert found.transfer_type == "cc-payment"

    def test_get_by_either_side(self, repo, sample_import_inserted):
        t1, t2 = self._make_transfer_pair(repo, sample_import_inserted.id)
        xfer = Transfer(
            from_transaction_id=t1.id,
            to_transaction_id=t2.id,
            transfer_type="cc-payment",
            match_method="known_pattern",
        )
        repo.insert_transfer(xfer)
        # Find by to-side
        found = repo.get_transfer_by_transaction(t2.id)
        assert found is not None
        assert found.from_transaction_id == t1.id

    def test_find_transfer_candidates(self, repo, sample_import_inserted):
        imp_id = sample_import_inserted.id
        source = _make_txn(
            imp_id, account_id="wf-checking",
            amount=-800.00, date="2026-01-15",
            raw_description="CAPITAL ONE MOBILE PMT",
            import_hash="src_h", dedup_key="src_k",
        )
        match = _make_txn(
            imp_id, account_id="cap1-credit",
            amount=800.00, date="2026-01-16",
            raw_description="PAYMENT THANK YOU",
            import_hash="match_h", dedup_key="match_k",
        )
        decoy = _make_txn(
            imp_id, account_id="amex-blue",
            amount=50.00, date="2026-01-15",
            raw_description="SOME OTHER TXN",
            import_hash="decoy_h", dedup_key="decoy_k",
        )
        repo.insert_transaction(source)
        repo.insert_transaction(match)
        repo.insert_transaction(decoy)
        candidates = repo.find_transfer_candidates(source)
        assert len(candidates) == 1
        assert candidates[0].id == match.id

    def test_find_excludes_already_linked(self, repo, sample_import_inserted):
        imp_id = sample_import_inserted.id
        t1 = _make_txn(
            imp_id, account_id="wf-checking", amount=-100.00,
            import_hash="h1", dedup_key="k1",
        )
        t2 = _make_txn(
            imp_id, account_id="cap1-credit", amount=100.00,
            import_hash="h2", dedup_key="k2",
        )
        repo.insert_transaction(t1)
        repo.insert_transaction(t2)
        repo.insert_transfer(Transfer(
            from_transaction_id=t1.id, to_transaction_id=t2.id,
            transfer_type="cc-payment", match_method="known_pattern",
        ))
        # t2 is already linked, so searching for t1's candidates should return empty
        candidates = repo.find_transfer_candidates(t1)
        assert len(candidates) == 0


# ── Receipt Match CRUD ─────────────────────────────────────


class TestReceiptMatchCrud:
    def test_insert(self, repo, sample_import_inserted):
        txn = _make_txn(sample_import_inserted.id)
        repo.insert_transaction(txn)
        match = ReceiptMatch(
            transaction_id=txn.id,
            gmail_message_id="msg_abc123",
            match_type="apple_subset_sum",
            matched_items='[{"item": "YouTube Premium", "amount": 22.99}]',
            confidence=0.95,
        )
        repo.insert_receipt_match(match)
        # Verify via raw query
        row = repo.conn.execute(
            "SELECT * FROM receipt_matches WHERE transaction_id = ?",
            (txn.id,),
        ).fetchone()
        assert row is not None
        assert row["match_type"] == "apple_subset_sum"
        assert "YouTube Premium" in row["matched_items"]


# ── API Usage ──────────────────────────────────────────────


class TestApiUsage:
    def test_increment_creates_row(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        repo.increment_api_usage(
            "2026-01", "claude_categorize",
            requests=1, tokens_in=500, tokens_out=100, cost_cents=2,
        )
        cost = repo.get_monthly_cost("2026-01")
        assert cost == 2

    def test_increment_accumulates(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        repo.increment_api_usage("2026-01", "claude_categorize", cost_cents=3)
        repo.increment_api_usage("2026-01", "claude_categorize", cost_cents=5)
        cost = repo.get_monthly_cost("2026-01")
        assert cost == 8

    def test_separate_services(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        repo.increment_api_usage("2026-01", "claude_categorize", cost_cents=10)
        repo.increment_api_usage("2026-01", "claude_receipt_parse", cost_cents=3)
        cost = repo.get_monthly_cost("2026-01")
        assert cost == 13

    def test_zero_for_no_usage(self, repo):
        repo.apply_migrations(MIGRATIONS_DIR)
        cost = repo.get_monthly_cost("2099-12")
        assert cost == 0
