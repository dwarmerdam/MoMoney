"""Tests for the Google Sheets push module.

All tests use mock gspread objects — no live Google API needed.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from src.database.models import Allocation, Import, Transaction, Transfer
from src.database.repository import Repository
from src.sheets.push import (
    ALLOC_DATA_HEADERS,
    ALLOC_HEADERS,
    BATCH_SIZE,
    CATEGORY_HEADERS,
    MAX_WRITES_PER_MINUTE,
    REVIEW_DATA_HEADERS,
    REVIEW_HEADERS,
    SHEET_ALLOCATIONS,
    SHEET_CATEGORIES,
    SHEET_REVIEW,
    SHEET_SUMMARY,
    SHEET_TRANSACTIONS,
    SHEET_TRANSFERS,
    SUMMARY_HEADERS,
    TRANSFER_HEADERS,
    TXN_DATA_HEADERS,
    TXN_HEADERS,
    PushResult,
    SheetsPush,
    alloc_to_row,
    category_to_row,
    review_to_row,
    summary_to_row,
    transfer_to_row,
    txn_to_row,
)

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture
def mock_spreadsheet():
    """Mock gspread Spreadsheet with worksheet stubs."""
    ss = MagicMock()
    worksheets = {}

    def get_worksheet(name):
        if name not in worksheets:
            ws = MagicMock()
            ws.append_rows = MagicMock()
            ws.clear = MagicMock()
            ws.update = MagicMock()
            worksheets[name] = ws
        return worksheets[name]

    ss.worksheet = MagicMock(side_effect=get_worksheet)
    return ss


@pytest.fixture
def push(mock_spreadsheet):
    return SheetsPush(mock_spreadsheet)


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
        account_id="wf-checking",
        date="2026-01-15",
        amount=-50.00,
        raw_description="TEST MERCHANT",
        import_id=imp_id,
        import_hash="h1",
        dedup_key="dk1",
    )
    defaults.update(kw)
    return Transaction(**defaults)


# ── Data transformation tests ─────────────────────────────


class TestTxnToRow:
    def test_all_fields(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-50.00,
            raw_description="DOORDASH*ORDER", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
            normalized_description="DOORDASH ORDER",
            memo="Lunch", txn_type="DEBIT", check_num="1234",
            status="categorized", confidence=0.95,
            categorization_method="merchant_auto",
        )
        row = txn_to_row(txn, transfer_flag=False)
        assert row[0] == txn.id
        assert row[1] == "2026-01-15"
        assert row[2] == -50.00
        assert row[3] == "DOORDASH*ORDER"
        assert row[4] == "DOORDASH ORDER"
        assert row[5] == "Lunch"
        assert row[6] == "DEBIT"
        assert row[7] == "1234"
        assert row[8] == "wf-checking"
        assert row[9] == "categorized"
        assert row[10] == 0.95
        assert row[11] == "merchant_auto"
        assert row[12] is False

    def test_receipt_lookup_status_included(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-50.00,
            raw_description="AMAZON.COM*ORDER", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
            receipt_lookup_status="no_match",
        )
        row = txn_to_row(txn, transfer_flag=False)
        assert row[13] == "no_match"

    def test_category_id_included(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-50.00,
            raw_description="TRADER JOES", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
        )
        row = txn_to_row(txn, transfer_flag=False, category_id="groceries")
        assert row[14] == "groceries"

    def test_null_fields_become_empty(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-10.00,
            raw_description="TEST", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
        )
        row = txn_to_row(txn, transfer_flag=False)
        assert row[4] == ""  # normalized_description
        assert row[5] == ""  # memo
        assert row[6] == ""  # txn_type
        assert row[7] == ""  # check_num
        assert row[10] == ""  # confidence
        assert row[11] == ""  # categorization_method
        assert row[13] == ""  # receipt_lookup_status
        assert row[14] == ""  # category_id

    def test_transfer_flag_true(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-500.00,
            raw_description="CAPITAL ONE PMT", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
        )
        row = txn_to_row(txn, transfer_flag=True)
        assert row[12] is True

    def test_row_length_matches_headers(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-10.00,
            raw_description="TEST", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
        )
        row = txn_to_row(txn, transfer_flag=False)
        assert len(row) == len(TXN_DATA_HEADERS)


class TestAllocToRow:
    def test_all_fields(self):
        alloc = Allocation(
            transaction_id="txn1", category_id="groceries",
            amount=-75.00, memo="Weekly shop", tags="food",
            source="auto", confidence=0.90,
        )
        row = alloc_to_row(alloc)
        assert row[1] == "txn1"
        assert row[2] == "groceries"
        assert row[3] == -75.00
        assert row[4] == "Weekly shop"
        assert row[5] == "food"
        assert row[6] == "auto"
        assert row[7] == 0.90

    def test_row_length_matches_headers(self):
        alloc = Allocation(transaction_id="t1", category_id="c1", amount=-10.00)
        row = alloc_to_row(alloc)
        assert len(row) == len(ALLOC_DATA_HEADERS)


class TestTransferToRow:
    def test_all_fields(self):
        xfer = Transfer(
            from_transaction_id="txn1", to_transaction_id="txn2",
            transfer_type="cc-payment", match_method="pattern",
            confidence=1.0,
        )
        row = transfer_to_row(xfer)
        assert row[1] == "txn1"
        assert row[2] == "txn2"
        assert row[3] == "cc-payment"
        assert row[4] == "pattern"
        assert row[5] == 1.0

    def test_row_length_matches_headers(self):
        xfer = Transfer(
            from_transaction_id="t1", to_transaction_id="t2",
            transfer_type="internal", match_method="auto",
        )
        row = transfer_to_row(xfer)
        assert len(row) == len(TRANSFER_HEADERS)


class TestSummaryToRow:
    def test_formats_correctly_without_lookup(self):
        row = summary_to_row("2026-01", {
            "category_id": "groceries", "total": -450.00, "txn_count": 12
        })
        assert row == ["2026-01", "groceries", "", "", "", "", -450.00, 12]

    def test_row_length_matches_headers(self):
        row = summary_to_row("2026-01", {
            "category_id": "c1", "total": 0, "txn_count": 0
        })
        assert len(row) == len(SUMMARY_HEADERS)


# ── Queue management tests ────────────────────────────────


class TestQueueManagement:
    def test_queue_append_accumulates(self, push):
        push.queue_append("Transactions", [["row1"]])
        push.queue_append("Transactions", [["row2"]])
        assert len(push.pending_appends["Transactions"]) == 2

    def test_queue_append_multiple_sheets(self, push):
        push.queue_append("Transactions", [["t1"]])
        push.queue_append("Allocations", [["a1"]])
        assert "Transactions" in push.pending_appends
        assert "Allocations" in push.pending_appends

    def test_flush_clears_queues(self, push):
        push.queue_append("Transactions", [["row1"]])
        push.flush()
        assert push.pending_appends == {}

    def test_flush_calls_worksheet_append(self, push, mock_spreadsheet):
        push.queue_append("Transactions", [["row1"], ["row2"]])
        push.flush()
        ws = mock_spreadsheet.worksheet("Transactions")
        ws.append_rows.assert_called_once_with(
            [["row1"], ["row2"]], value_input_option="RAW"
        )

    def test_flush_empty_is_noop(self, push, mock_spreadsheet):
        results = push.flush()
        assert results == []
        mock_spreadsheet.worksheet.assert_not_called()

    def test_queue_clear_and_flush(self, push, mock_spreadsheet):
        push.queue_clear("Transactions")
        push.flush()
        ws = mock_spreadsheet.worksheet("Transactions")
        ws.clear.assert_called_once()


# ── Batching tests ────────────────────────────────────────


class TestBatching:
    def test_100_rows_one_call(self, push, mock_spreadsheet):
        rows = [[f"row{i}"] for i in range(100)]
        push.queue_append("Transactions", rows)
        results = push.flush()
        ws = mock_spreadsheet.worksheet("Transactions")
        assert ws.append_rows.call_count == 1
        assert results[0].api_calls == 1
        assert results[0].rows_pushed == 100

    def test_250_rows_three_calls(self, push, mock_spreadsheet):
        rows = [[f"row{i}"] for i in range(250)]
        push.queue_append("Transactions", rows)
        results = push.flush()
        ws = mock_spreadsheet.worksheet("Transactions")
        assert ws.append_rows.call_count == 3
        assert results[0].api_calls == 3
        assert results[0].rows_pushed == 250

    def test_exact_200_two_calls(self, push, mock_spreadsheet):
        rows = [[f"row{i}"] for i in range(200)]
        push.queue_append("Transactions", rows)
        results = push.flush()
        ws = mock_spreadsheet.worksheet("Transactions")
        assert ws.append_rows.call_count == 2

    def test_single_row_one_call(self, push, mock_spreadsheet):
        push.queue_append("Transactions", [["single"]])
        results = push.flush()
        ws = mock_spreadsheet.worksheet("Transactions")
        assert ws.append_rows.call_count == 1
        assert results[0].rows_pushed == 1


# ── Rate limiting tests ──────────────────────────────────


class TestRateLimiting:
    def test_under_threshold_no_sleep(self, push):
        """Under 50 writes, flush should not sleep."""
        push.queue_append("T", [["row"]])
        with patch("time.sleep") as mock_sleep:
            push.flush()
            mock_sleep.assert_not_called()

    def test_writes_tracked(self, push):
        """Each flush adds timestamps to _write_times."""
        push.queue_append("T", [["row1"]])
        push.flush()
        assert len(push._write_times) == 1

        push.queue_append("T", [["row2"]])
        push.flush()
        assert len(push._write_times) == 2

    def test_old_timestamps_expire(self, push):
        """Timestamps older than 60 seconds are purged."""
        # Manually add old timestamps
        old = time.monotonic() - 61
        for _ in range(50):
            push._write_times.append(old)

        # This should purge all old ones and NOT sleep
        push.queue_append("T", [["row"]])
        with patch("time.sleep") as mock_sleep:
            push.flush()
            mock_sleep.assert_not_called()
        # Old ones purged, only the new write remains
        assert len(push._write_times) == 1

    def test_at_threshold_sleeps(self, push):
        """At 50 recent writes, flush should sleep."""
        now = time.monotonic()
        for i in range(50):
            push._write_times.append(now - 10 + i * 0.1)  # Recent timestamps

        push.queue_append("T", [["row"]])
        with patch("time.sleep") as mock_sleep:
            with patch("time.monotonic", side_effect=[
                now,       # _wait_for_rate_limit: now
                now + 55,  # _record_write
            ]):
                push.flush()
            mock_sleep.assert_called_once()


# ── High-level push methods ──────────────────────────────


class TestPushMethods:
    def test_push_transactions(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        result = push.push_transactions([txn], repo)
        assert result is not None
        assert result.rows_pushed == 1
        assert result.sheet == SHEET_TRANSACTIONS

    def test_push_transactions_empty(self, push, repo):
        result = push.push_transactions([], repo)
        assert result is None

    def test_push_allocations(self, push, mock_spreadsheet):
        alloc = Allocation(transaction_id="t1", category_id="groceries", amount=-50.00)
        result = push.push_allocations([alloc])
        assert result is not None
        assert result.sheet == SHEET_ALLOCATIONS

    def test_push_transfers(self, push, mock_spreadsheet):
        xfer = Transfer(
            from_transaction_id="t1", to_transaction_id="t2",
            transfer_type="cc-payment", match_method="pattern",
        )
        result = push.push_transfers([xfer])
        assert result is not None
        assert result.sheet == SHEET_TRANSFERS

    def test_push_summary(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id, date="2026-01-15")
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)
        result = push.push_summary("2026-01", repo)
        assert result is not None
        assert result.sheet == SHEET_SUMMARY

    def test_push_summary_empty_month(self, push, mock_spreadsheet, repo):
        result = push.push_summary("2099-12", repo)
        assert result is None


# ── Full rebuild ──────────────────────────────────────────


class TestFullRebuild:
    def test_clears_all_sheets(self, push, mock_spreadsheet, repo):
        results = push.full_rebuild(repo)
        # All 6 sheets should have been cleared
        cleared = set()
        for c in mock_spreadsheet.worksheet.call_args_list:
            cleared.add(c[0][0])
        assert SHEET_TRANSACTIONS in cleared
        assert SHEET_ALLOCATIONS in cleared
        assert SHEET_TRANSFERS in cleared
        assert SHEET_SUMMARY in cleared
        assert SHEET_CATEGORIES in cleared

    def test_pushes_headers(self, push, mock_spreadsheet, repo):
        push.full_rebuild(repo)
        # With empty DB, only headers should be pushed
        ws = mock_spreadsheet.worksheet(SHEET_TRANSACTIONS)
        # Headers are the first append_rows call
        calls = ws.append_rows.call_args_list
        assert len(calls) >= 1
        first_call_rows = calls[0][0][0]
        assert first_call_rows[0] == TXN_HEADERS

    def test_pushes_existing_data(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        push.full_rebuild(repo)

        # Transactions sheet should have header + 1 data row
        ws_txn = mock_spreadsheet.worksheet(SHEET_TRANSACTIONS)
        total_rows = sum(
            len(c[0][0]) for c in ws_txn.append_rows.call_args_list
        )
        assert total_rows >= 2  # header + 1 txn

    def test_pushes_summary_data(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id, date="2026-01-15")
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        push.full_rebuild(repo)

        # Summary sheet should have header + at least 1 summary row
        ws_sum = mock_spreadsheet.worksheet(SHEET_SUMMARY)
        total_rows = sum(
            len(c[0][0]) for c in ws_sum.append_rows.call_args_list
        )
        assert total_rows >= 2  # header + summary


# ── Header schema tests ──────────────────────────────────


class TestHeaders:
    def test_txn_headers_count(self):
        assert len(TXN_DATA_HEADERS) == 15
        assert len(TXN_HEADERS) == 21  # 15 data + 6 override

    def test_alloc_headers_count(self):
        assert len(ALLOC_DATA_HEADERS) == 8
        assert len(ALLOC_HEADERS) == 10  # 8 data + 2 override

    def test_transfer_headers_count(self):
        assert len(TRANSFER_HEADERS) == 6

    def test_summary_headers_count(self):
        assert len(SUMMARY_HEADERS) == 8

    def test_category_headers_count(self):
        assert len(CATEGORY_HEADERS) == 10

    def test_review_headers_count(self):
        assert len(REVIEW_DATA_HEADERS) == 21
        assert len(REVIEW_HEADERS) == 24  # 21 data + 3 override


# ── Review sheet tests ──────────────────────────────────


class TestReviewToRow:
    def test_with_allocation(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-50.00,
            raw_description="TRADER JOES", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
            normalized_description="Trader Joe's",
            status="categorized", confidence=0.95,
            categorization_method="merchant_auto",
        )
        alloc = Allocation(
            transaction_id=txn.id, category_id="groceries",
            amount=-50.00, source="auto", confidence=0.95,
        )
        row = review_to_row(txn, alloc, transfer_flag=False)
        assert row[0] == "2026-01-15"  # date
        assert row[1] == "wf-checking"  # account_id
        assert row[2] == -50.00  # amount
        assert row[3] == "Trader Joe's"  # normalized_description
        assert row[4] == "groceries"  # category_id
        assert row[5] == -50.00  # alloc_amount
        assert row[6] == 0.95  # confidence
        assert row[7] == "merchant_auto"  # method
        assert row[8] == "auto"  # source
        assert row[9] == "categorized"  # status
        assert row[10] is False  # is_transfer
        assert row[11] == "TRADER JOES"  # raw_description
        assert row[18] == txn.id  # transaction_id
        assert row[19] == alloc.id  # allocation_id
        assert len(row) == len(REVIEW_DATA_HEADERS)

    def test_without_allocation(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-25.00,
            raw_description="UNKNOWN MERCHANT", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
            status="pending",
        )
        row = review_to_row(txn, None, transfer_flag=False)
        assert row[4] == ""  # category_id empty
        assert row[5] == -25.00  # alloc_amount falls back to txn.amount
        assert row[8] == ""  # source empty
        assert row[19] == ""  # allocation_id empty
        assert row[20] == ""  # alloc_confidence empty
        assert len(row) == len(REVIEW_DATA_HEADERS)

    def test_transfer_flag(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-500.00,
            raw_description="CAPITAL ONE PMT", import_id="imp1",
            import_hash="h1", dedup_key="dk1",
        )
        row = review_to_row(txn, None, transfer_flag=True)
        assert row[10] is True


class TestPushReview:
    def test_push_review(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)
        result = push.push_review([txn], repo)
        assert result is not None
        assert result.rows_pushed == 1
        assert result.sheet == SHEET_REVIEW

    def test_push_review_empty(self, push, repo):
        result = push.push_review([], repo)
        assert result is None

    def test_push_review_split_transaction(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id, amount=-100.00)
        repo.insert_transaction(txn)
        allocs = [
            Allocation(transaction_id=txn.id, category_id="groceries", amount=-60.00),
            Allocation(transaction_id=txn.id, category_id="household", amount=-40.00),
        ]
        repo.insert_allocations_batch(allocs)
        result = push.push_review([txn], repo)
        assert result is not None
        assert result.rows_pushed == 2  # One row per allocation

    def test_push_review_no_allocation(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        # No allocation inserted
        result = push.push_review([txn], repo)
        assert result is not None
        assert result.rows_pushed == 1  # Still produces a row


class TestFullRebuildReview:
    def test_review_sheet_included(self, push, mock_spreadsheet, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        alloc = Allocation(transaction_id=txn.id, category_id="groceries", amount=-50.00)
        repo.insert_allocation(alloc)

        push.full_rebuild(repo)

        # Review sheet should have been cleared and populated
        accessed = set()
        for c in mock_spreadsheet.worksheet.call_args_list:
            accessed.add(c[0][0])
        assert SHEET_REVIEW in accessed

    def test_review_sheet_cleared(self, push, mock_spreadsheet, repo):
        push.full_rebuild(repo)
        ws = mock_spreadsheet.worksheet(SHEET_REVIEW)
        ws.clear.assert_called_once()


# ── Category validation tests ──────────────────────────


class TestCategoryValidation:
    def test_full_rebuild_with_config_applies_validation(self, push, mock_spreadsheet, repo):
        """full_rebuild with config calls add_validation on 3 sheets."""
        mock_config = MagicMock()
        mock_config.flatten_category_tree.return_value = {
            "groceries": {"category_id": "groceries", "name": "Groceries",
                          "level": 0, "level_0": "Groceries", "level_1": "",
                          "level_2": "", "level_3": "", "is_leaf": True,
                          "is_income": False, "is_transfer": False},
        }
        # Patch ValidationConditionType so validation code path runs
        # (it may be None if gspread couldn't fully import)
        mock_vctype = MagicMock()
        with patch("src.sheets.push.ValidationConditionType", mock_vctype):
            push.full_rebuild(repo, config=mock_config)

        # add_validation should have been called on Transactions, Allocations, Review
        for sheet_name in [SHEET_TRANSACTIONS, SHEET_ALLOCATIONS, SHEET_REVIEW]:
            ws = mock_spreadsheet.worksheet(sheet_name)
            ws.add_validation.assert_called_once()

    def test_full_rebuild_without_config_skips_validation(self, push, mock_spreadsheet, repo):
        """full_rebuild without config does not call add_validation."""
        push.full_rebuild(repo)
        for sheet_name in [SHEET_TRANSACTIONS, SHEET_ALLOCATIONS, SHEET_REVIEW]:
            ws = mock_spreadsheet.worksheet(sheet_name)
            assert not hasattr(ws, 'add_validation') or ws.add_validation.call_count == 0

    def test_validation_failure_does_not_break_rebuild(self, push, mock_spreadsheet, repo):
        """If add_validation throws, full_rebuild still returns results."""
        mock_config = MagicMock()
        mock_config.flatten_category_tree.return_value = {"x": {
            "category_id": "x", "name": "X", "level": 0,
            "level_0": "X", "level_1": "", "level_2": "", "level_3": "",
            "is_leaf": True, "is_income": False, "is_transfer": False,
        }}
        # Make add_validation raise on all worksheets
        for sheet_name in [SHEET_TRANSACTIONS, SHEET_ALLOCATIONS, SHEET_REVIEW]:
            ws = mock_spreadsheet.worksheet(sheet_name)
            ws.add_validation = MagicMock(side_effect=Exception("API error"))

        mock_vctype = MagicMock()
        with patch("src.sheets.push.ValidationConditionType", mock_vctype):
            results = push.full_rebuild(repo, config=mock_config)
        assert isinstance(results, list)
