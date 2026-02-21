"""Tests for the Google Sheets override poller.

All tests use mock gspread objects and an in-memory SQLite DB.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.database.models import Allocation, Import, Transaction, Transfer
from src.database.repository import Repository
from src.sheets.overrides import (
    ALLOC_OVERRIDE_START,
    REVIEW_OVERRIDE_START,
    TXN_OVERRIDE_START,
    OverridePoller,
    PollResult,
    _cell_label,
)

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture
def repo():
    r = Repository(":memory:")
    r.apply_migrations(MIGRATIONS_DIR)
    yield r
    r.close()


@pytest.fixture
def imp(repo):
    return repo.insert_import(Import(file_name="test.qfx", file_hash="testhash"))


def _txn(imp_id, **kw) -> Transaction:
    defaults = dict(
        account_id="wf-checking", date="2026-01-15", amount=-50.00,
        raw_description="TEST MERCHANT", import_id=imp_id,
        import_hash="h1", dedup_key="dk1",
    )
    defaults.update(kw)
    return Transaction(**defaults)


def _make_txn_sheet_row(txn_id, overrides=None):
    """Build a mock Transactions sheet row with optional override columns.

    Returns a list matching TXN_HEADERS (15 cols) + 6 override columns.
    """
    base = [txn_id] + [""] * 14  # 15 data columns
    if overrides:
        # Pad to TXN_OVERRIDE_START then add overrides
        while len(base) < TXN_OVERRIDE_START:
            base.append("")
        base.extend(overrides)
    return base


def _make_alloc_sheet_row(alloc_id, txn_id, overrides=None):
    """Build a mock Allocations sheet row with optional override columns."""
    base = [alloc_id, txn_id] + [""] * 6  # 8 data columns
    if overrides:
        while len(base) < ALLOC_OVERRIDE_START:
            base.append("")
        base.extend(overrides)
    return base


def _make_review_sheet_row(txn_id, alloc_id="", overrides=None):
    """Build a mock Review sheet row with optional override columns.

    Returns a list matching REVIEW_HEADERS (21 cols) + up to 3 override columns.
    """
    # 21 data columns, txn_id at index 18, alloc_id at index 19
    base = [""] * 21
    base[18] = txn_id
    base[19] = alloc_id
    if overrides:
        while len(base) < REVIEW_OVERRIDE_START:
            base.append("")
        base.extend(overrides)
    return base


def _mock_spreadsheet(txn_rows=None, alloc_rows=None, review_rows=None):
    """Create a mock spreadsheet with controllable sheet data."""
    ss = MagicMock()

    def make_ws(rows):
        ws = MagicMock()
        ws.get_all_values.return_value = rows or []
        ws.update = MagicMock()
        return ws

    txn_ws = make_ws(txn_rows or [["header"]])
    alloc_ws = make_ws(alloc_rows or [["header"]])
    review_ws = make_ws(review_rows or [["header"]])

    def get_worksheet(name):
        if name == "Transactions":
            return txn_ws
        if name == "Allocations":
            return alloc_ws
        if name == "Review":
            return review_ws
        return MagicMock()

    ss.worksheet = MagicMock(side_effect=get_worksheet)
    return ss


# ── Cell label tests ──────────────────────────────────────


class TestCellLabel:
    def test_column_a(self):
        assert _cell_label(1, 1) == "A1"

    def test_column_z(self):
        assert _cell_label(1, 26) == "Z1"

    def test_column_aa(self):
        assert _cell_label(1, 27) == "AA1"

    def test_row_and_column(self):
        assert _cell_label(5, 3) == "C5"

    def test_column_p(self):
        """Column 16 = P (used for TXN override start)."""
        assert _cell_label(2, 16) == "P2"


# ── Override detection tests ──────────────────────────────


class TestOverrideDetection:
    def test_no_overrides_is_noop(self, repo):
        """All override columns blank → nothing applied."""
        header = ["header"]
        row = _make_txn_sheet_row("txn1", overrides=["", "", "", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[header, row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()
        assert result.overrides_applied == 0

    def test_empty_sheet_is_noop(self, repo):
        """Sheet with only header → nothing processed."""
        ss = _mock_spreadsheet(txn_rows=[["header"]])
        poller = OverridePoller(ss, repo)
        result = poller.poll()
        assert result.overrides_applied == 0

    def test_short_row_skipped(self, repo):
        """Row shorter than override start column → skipped."""
        ss = _mock_spreadsheet(txn_rows=[["header"], ["txn1", "val"]])
        poller = OverridePoller(ss, repo)
        result = poller.poll()
        assert result.overrides_applied == 0

    def test_blank_txn_id_skipped(self, repo):
        """Row with empty txn_id → skipped even if overrides present."""
        row = _make_txn_sheet_row("", overrides=["", "", "TRUE", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()
        assert result.overrides_applied == 0


# ── Transaction override application ─────────────────────


class TestTxnCategoryOverride:
    def test_apply_category_override_existing_alloc(self, repo, imp):
        """Category override in Transactions sheet updates existing allocation."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="home-insurance", amount=-344.09)
        repo.insert_allocation(alloc)

        row = _make_txn_sheet_row(txn.id, overrides=["car-insurance", "", "", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated_allocs = repo.get_allocations_by_transaction(txn.id)
        assert updated_allocs[0].category_id == "car-insurance"
        assert updated_allocs[0].source == "user"

    def test_apply_category_override_no_alloc(self, repo, imp):
        """Category override creates allocation when none exists."""
        txn = _txn(imp.id, amount=-75.00)
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["dining-out", "", "", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 1
        assert allocs[0].category_id == "dining-out"
        assert allocs[0].source == "user"

    def test_category_with_other_overrides(self, repo, imp):
        """Category + reviewed in the same row both apply."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_txn_sheet_row(txn.id, overrides=["coffee-d", "", "TRUE", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 2
        updated_allocs = repo.get_allocations_by_transaction(txn.id)
        assert updated_allocs[0].category_id == "coffee-d"
        updated_txn = repo.get_transaction(txn.id)
        assert updated_txn.status == "reviewed"

    def test_merchant_override_from_txn_sheet(self, repo, imp):
        """Merchant override in Transactions sheet updates normalized_description."""
        txn = _txn(imp.id, normalized_description="OLD MERCHANT NAME")
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "CORRECTED MERCHANT", "", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_transaction(txn.id)
        assert updated.normalized_description == "CORRECTED MERCHANT"

    def test_category_and_merchant_from_txn_sheet(self, repo, imp):
        """Category + merchant override together from Transactions sheet."""
        txn = _txn(imp.id, normalized_description="OLD NAME")
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_txn_sheet_row(txn.id, overrides=["coffee-d", "Philz Coffee", "", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 2
        updated_allocs = repo.get_allocations_by_transaction(txn.id)
        assert updated_allocs[0].category_id == "coffee-d"
        updated_txn = repo.get_transaction(txn.id)
        assert updated_txn.normalized_description == "Philz Coffee"


class TestReviewedOverride:
    def test_apply_reviewed_flag(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "", "TRUE", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_transaction(txn.id)
        assert updated.status == "reviewed"

    def test_reviewed_case_insensitive(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "", "true", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()
        assert result.overrides_applied == 1


class TestNeedsSplitOverride:
    def test_apply_needs_split(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "", "", "", "TRUE", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_transaction(txn.id)
        assert updated.status == "flagged"


class TestNotesOverride:
    def test_apply_notes_to_empty_memo(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "", "", "", "", "User note here"])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_transaction(txn.id)
        assert updated.memo == "User note here"

    def test_apply_notes_appends_to_existing(self, repo, imp):
        txn = _txn(imp.id, memo="Existing memo")
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "", "", "", "", "New note"])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        updated = repo.get_transaction(txn.id)
        assert "Existing memo" in updated.memo
        assert "New note" in updated.memo


class TestTransferLinkOverride:
    def test_apply_transfer_link(self, repo, imp):
        txn1 = _txn(imp.id, raw_description="OUTGOING", import_hash="h1", dedup_key="d1")
        txn2 = _txn(imp.id, raw_description="INCOMING", amount=50.00,
                     import_hash="h2", dedup_key="d2")
        repo.insert_transaction(txn1)
        repo.insert_transaction(txn2)

        row = _make_txn_sheet_row(txn1.id, overrides=["", "", "", txn2.id, "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        xfer = repo.get_transfer_by_transaction(txn1.id)
        assert xfer is not None
        assert xfer.from_transaction_id == txn1.id
        assert xfer.to_transaction_id == txn2.id
        assert xfer.match_method == "user_override"
        assert xfer.confidence == 1.0

    def test_duplicate_transfer_link_skipped(self, repo, imp):
        txn1 = _txn(imp.id, raw_description="OUT", import_hash="h1", dedup_key="d1")
        txn2 = _txn(imp.id, raw_description="IN", amount=50.00,
                     import_hash="h2", dedup_key="d2")
        repo.insert_transaction(txn1)
        repo.insert_transaction(txn2)
        # Create existing transfer
        repo.insert_transfer(Transfer(
            from_transaction_id=txn1.id, to_transaction_id=txn2.id,
            transfer_type="cc-payment", match_method="pattern",
        ))

        row = _make_txn_sheet_row(txn1.id, overrides=["", "", "", txn2.id, "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        # Should not create a duplicate — no override applied, counts as error
        assert result.overrides_applied == 0
        assert result.errors == 1

    def test_transfer_link_to_nonexistent_txn(self, repo, imp):
        """Link to non-existent transaction should fail gracefully."""
        txn1 = _txn(imp.id, raw_description="OUT", import_hash="h1", dedup_key="d1")
        repo.insert_transaction(txn1)

        # Link to transaction that doesn't exist
        row = _make_txn_sheet_row(txn1.id, overrides=["", "", "", "nonexistent-id", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 0
        assert result.errors == 1


# ── Allocation override application ──────────────────────


class TestMerchantOverride:
    def test_apply_merchant_override(self, repo, imp):
        txn = _txn(imp.id, normalized_description="OLD MERCHANT")
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["", "New Merchant Name"])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_transaction(txn.id)
        assert updated.normalized_description == "New Merchant Name"

    def test_merchant_override_recorded_in_actions(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["", "Corrected Name"])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert len(result.actions) == 1
        assert result.actions[0].column == "Override: Merchant"
        assert result.actions[0].new_value == "Corrected Name"
        assert result.actions[0].entity_id == txn.id

    def test_category_and_merchant_together(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["coffee-d", "Philz Coffee"])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 2
        updated_allocs = repo.get_allocations_by_transaction(txn.id)
        assert updated_allocs[0].category_id == "coffee-d"
        updated_txn = repo.get_transaction(txn.id)
        assert updated_txn.normalized_description == "Philz Coffee"


class TestCategoryOverride:
    def test_apply_category_override(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["coffee-d", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated_allocs = repo.get_allocations_by_transaction(txn.id)
        assert updated_allocs[0].category_id == "coffee-d"
        assert updated_allocs[0].source == "user"

    def test_category_override_recorded_in_actions(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["biz-saas", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert len(result.actions) == 1
        assert result.actions[0].column == "Override: Category"
        assert result.actions[0].new_value == "biz-saas"
        assert result.actions[0].old_value == "groceries"  # Captured for audit

    def test_category_override_nonexistent_alloc(self, repo, imp):
        """Category override for non-existent allocation fails gracefully."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_alloc_sheet_row("nonexistent-alloc", txn.id, overrides=["coffee-d", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 0
        assert result.errors == 1


# ── Cell clearing tests ──────────────────────────────────


class TestCellClearing:
    def test_txn_overrides_cleared_after_poll(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "", "TRUE", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        poller.poll()

        # Verify update was called to clear override columns
        txn_ws = ss.worksheet("Transactions")
        txn_ws.update.assert_called_once()
        call_args = txn_ws.update.call_args
        # Should clear 6 override columns (P through U)
        cell_range = call_args[0][0]
        assert cell_range.startswith("P")

    def test_alloc_overrides_cleared_after_poll(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["coffee-d", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        poller.poll()

        alloc_ws = ss.worksheet("Allocations")
        alloc_ws.update.assert_called_once()
        cell_range = alloc_ws.update.call_args[0][0]
        # Alloc overrides start at column I (index 8 → 1-indexed col 9)
        assert cell_range.startswith("I")


# ── Multiple overrides in single poll ────────────────────


class TestMultipleOverrides:
    def test_multiple_txn_overrides(self, repo, imp):
        """Apply reviewed + notes in the same row."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_txn_sheet_row(txn.id, overrides=["", "", "TRUE", "", "", "Check this"])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 2
        updated = repo.get_transaction(txn.id)
        assert updated.status == "reviewed"
        assert "Check this" in updated.memo

    def test_multiple_rows(self, repo, imp):
        """Multiple rows with overrides in the same sheet."""
        txn1 = _txn(imp.id, raw_description="TXN1", import_hash="h1", dedup_key="d1")
        txn2 = _txn(imp.id, raw_description="TXN2", import_hash="h2", dedup_key="d2")
        repo.insert_transaction(txn1)
        repo.insert_transaction(txn2)

        row1 = _make_txn_sheet_row(txn1.id, overrides=["", "", "TRUE", "", "", ""])
        row2 = _make_txn_sheet_row(txn2.id, overrides=["", "", "", "", "TRUE", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row1, row2])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 2
        assert repo.get_transaction(txn1.id).status == "reviewed"
        assert repo.get_transaction(txn2.id).status == "flagged"


# ── Poll result structure ────────────────────────────────


class TestPollResult:
    def test_default_values(self):
        r = PollResult()
        assert r.overrides_applied == 0
        assert r.errors == 0
        assert r.actions == []


# ── Review sheet overrides ────────────────────────────────


class TestReviewOverrides:
    def test_category_override_existing_alloc(self, repo, imp):
        """Category override in Review sheet updates existing allocation."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_review_sheet_row(txn.id, alloc.id, overrides=["coffee-d", "", ""])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated_allocs = repo.get_allocations_by_transaction(txn.id)
        assert updated_allocs[0].category_id == "coffee-d"
        assert updated_allocs[0].source == "user"

    def test_merchant_override(self, repo, imp):
        """Merchant override in Review sheet updates normalized_description."""
        txn = _txn(imp.id, normalized_description="OLD NAME")
        repo.insert_transaction(txn)

        row = _make_review_sheet_row(txn.id, overrides=["", "New Merchant", ""])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_transaction(txn.id)
        assert updated.normalized_description == "New Merchant"

    def test_reviewed_flag(self, repo, imp):
        """Reviewed override in Review sheet marks txn as reviewed."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_review_sheet_row(txn.id, overrides=["", "", "TRUE"])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_transaction(txn.id)
        assert updated.status == "reviewed"

    def test_create_allocation_no_existing(self, repo, imp):
        """Category override on row with no alloc_id creates new allocation."""
        txn = _txn(imp.id, amount=-75.00)
        repo.insert_transaction(txn)

        # No alloc_id → triggers _create_allocation_from_override
        row = _make_review_sheet_row(txn.id, alloc_id="", overrides=["dining-out", "", ""])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 1
        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 1
        assert allocs[0].category_id == "dining-out"
        assert allocs[0].source == "user"
        assert allocs[0].amount == -75.00
        assert allocs[0].confidence == 1.0

        # Transaction should be marked categorized
        updated = repo.get_transaction(txn.id)
        assert updated.status == "categorized"

    def test_multiple_overrides_same_row(self, repo, imp):
        """Category + merchant + reviewed all set in one Review row."""
        txn = _txn(imp.id, normalized_description="OLD")
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_review_sheet_row(
            txn.id, alloc.id, overrides=["coffee-d", "Philz Coffee", "TRUE"],
        )
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.overrides_applied == 3
        updated_allocs = repo.get_allocations_by_transaction(txn.id)
        assert updated_allocs[0].category_id == "coffee-d"
        updated_txn = repo.get_transaction(txn.id)
        assert updated_txn.normalized_description == "Philz Coffee"
        assert updated_txn.status == "reviewed"

    def test_clear_on_success(self, repo, imp):
        """Review override columns are cleared after successful poll."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        row = _make_review_sheet_row(txn.id, overrides=["", "", "TRUE"])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        poller.poll()

        review_ws = ss.worksheet("Review")
        review_ws.update.assert_called_once()
        cell_range = review_ws.update.call_args[0][0]
        # Review overrides start at column V (col 22 = 1-indexed)
        assert cell_range.startswith("V")

    def test_no_clear_on_error(self, repo, imp):
        """Review override columns NOT cleared if errors occurred."""
        # Category override with nonexistent allocation → error
        row = _make_review_sheet_row("nonexistent-txn", "nonexistent-alloc", overrides=["coffee-d", "", ""])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()

        assert result.errors >= 1
        review_ws = ss.worksheet("Review")
        review_ws.update.assert_not_called()

    def test_short_row_skipped(self, repo):
        """Review row shorter than REVIEW_OVERRIDE_START is skipped."""
        ss = _mock_spreadsheet(review_rows=[["header"], ["short", "row"]])
        poller = OverridePoller(ss, repo)
        result = poller.poll()
        assert result.overrides_applied == 0

    def test_blank_txn_id_skipped(self, repo):
        """Review row with empty txn_id is skipped."""
        row = _make_review_sheet_row("", overrides=["coffee-d", "", ""])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo)
        result = poller.poll()
        assert result.overrides_applied == 0


# ── Category validation tests ─────────────────────────────


def _mock_config_with_categories(valid_ids: list[str]):
    """Create a mock Config with a category tree returning the given IDs."""
    config = MagicMock()
    flat = {cat_id: {"category_id": cat_id, "name": cat_id} for cat_id in valid_ids}
    config.flatten_category_tree.return_value = flat
    return config


class TestCategoryValidation:
    def test_valid_category_passes(self, repo, imp):
        """Valid category_id is accepted without changes."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        config = _mock_config_with_categories(["groceries", "coffee-d", "dining-out"])
        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["coffee-d", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo, config=config)
        result = poller.poll()

        assert result.overrides_applied == 1
        assert result.errors == 0
        updated = repo.get_allocations_by_transaction(txn.id)
        assert updated[0].category_id == "coffee-d"

    def test_fuzzy_match_corrects_typo(self, repo, imp):
        """Typo 'grocceries' is auto-corrected to 'groceries'."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="dining-out", amount=-50.00)
        repo.insert_allocation(alloc)

        config = _mock_config_with_categories(["groceries", "coffee-d", "dining-out"])
        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["grocceries", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo, config=config)
        result = poller.poll()

        assert result.overrides_applied == 1
        assert result.errors == 0
        updated = repo.get_allocations_by_transaction(txn.id)
        assert updated[0].category_id == "groceries"

    def test_no_match_rejects(self, repo, imp):
        """Completely invalid category with no fuzzy match is rejected."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        config = _mock_config_with_categories(["groceries", "coffee-d", "dining-out"])
        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["xyzzy-invalid", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo, config=config)
        result = poller.poll()

        assert result.overrides_applied == 0
        assert result.errors == 1
        # Category should be unchanged
        updated = repo.get_allocations_by_transaction(txn.id)
        assert updated[0].category_id == "groceries"

    def test_no_config_passes_all(self, repo, imp):
        """Without config, any category_id passes validation."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        row = _make_alloc_sheet_row(alloc.id, txn.id, overrides=["any-random-string", ""])
        ss = _mock_spreadsheet(alloc_rows=[["header"], row])
        poller = OverridePoller(ss, repo)  # No config
        result = poller.poll()

        assert result.overrides_applied == 1
        updated = repo.get_allocations_by_transaction(txn.id)
        assert updated[0].category_id == "any-random-string"

    def test_fuzzy_match_txn_category_override(self, repo, imp):
        """Fuzzy match works through _apply_txn_category_override path."""
        txn = _txn(imp.id, amount=-75.00)
        repo.insert_transaction(txn)

        config = _mock_config_with_categories(["groceries", "coffee-d", "dining-out"])
        row = _make_txn_sheet_row(txn.id, overrides=["cofee-d", "", "", "", "", ""])
        ss = _mock_spreadsheet(txn_rows=[["header"], row])
        poller = OverridePoller(ss, repo, config=config)
        result = poller.poll()

        assert result.overrides_applied == 1
        allocs = repo.get_allocations_by_transaction(txn.id)
        assert allocs[0].category_id == "coffee-d"

    def test_fuzzy_match_review_create_allocation(self, repo, imp):
        """Fuzzy match works through _create_allocation_from_override path."""
        txn = _txn(imp.id, amount=-30.00)
        repo.insert_transaction(txn)

        config = _mock_config_with_categories(["groceries", "coffee-d", "dining-out"])
        row = _make_review_sheet_row(txn.id, alloc_id="", overrides=["dinng-out", "", ""])
        ss = _mock_spreadsheet(review_rows=[["header"], row])
        poller = OverridePoller(ss, repo, config=config)
        result = poller.poll()

        assert result.overrides_applied == 1
        allocs = repo.get_allocations_by_transaction(txn.id)
        assert allocs[0].category_id == "dining-out"
