"""Google Sheets push: unidirectional SQLite → Sheets sync.

Transforms database models into sheet rows, batches writes to respect
the Google Sheets API quota (50 writes/minute), and supports full rebuild.

The gspread client is injected via constructor for testability — tests
pass a mock, production passes the real authenticated client.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass

from src.database.models import Allocation, Transaction, Transfer
from src.database.queries import batch_is_transfer, get_category_summary
from src.database.repository import Repository

try:
    from gspread.utils import ValidationConditionType
except BaseException:
    # Catch ImportError and any other failure (e.g., broken cryptography
    # backend raising pyo3 PanicException) that prevents gspread from loading.
    # Validation is optional — tests and offline mode work without it.
    ValidationConditionType = None

logger = logging.getLogger(__name__)


# ── Sheet column schemas ─────────────────────────────────

TXN_DATA_HEADERS = [
    "id", "date", "amount", "raw_description", "normalized_description",
    "memo", "txn_type", "check_num", "account_id", "status",
    "confidence", "categorization_method", "is_transfer",
    "receipt_lookup_status", "category_id",
]
TXN_OVERRIDE_HEADERS = [
    "Override: Category", "Override: Merchant", "Override: Reviewed",
    "Override: Transfer Link", "Override: Needs Split", "Override: Notes",
]
TXN_HEADERS = TXN_DATA_HEADERS + TXN_OVERRIDE_HEADERS

ALLOC_DATA_HEADERS = [
    "id", "transaction_id", "category_id", "amount",
    "memo", "tags", "source", "confidence",
]
ALLOC_OVERRIDE_HEADERS = [
    "Override: Category", "Override: Merchant",
]
ALLOC_HEADERS = ALLOC_DATA_HEADERS + ALLOC_OVERRIDE_HEADERS

TRANSFER_HEADERS = [
    "id", "from_transaction_id", "to_transaction_id",
    "transfer_type", "match_method", "confidence",
]

SUMMARY_HEADERS = [
    "month", "category_id", "category_name", "level_0", "level_1",
    "level_2", "total_amount", "transaction_count",
]

CATEGORY_HEADERS = [
    "category_id", "name", "level", "level_0", "level_1",
    "level_2", "level_3", "is_leaf", "is_income", "is_transfer",
]

REVIEW_DATA_HEADERS = [
    "date", "account_id", "amount", "normalized_description",
    "category_id", "alloc_amount", "confidence", "categorization_method",
    "source", "status", "is_transfer", "raw_description", "memo",
    "receipt_lookup_status", "txn_type", "check_num",
    "alloc_memo", "alloc_tags",
    "transaction_id", "allocation_id", "alloc_confidence",
]
REVIEW_OVERRIDE_HEADERS = [
    "Override: Category", "Override: Merchant", "Override: Reviewed",
]
REVIEW_HEADERS = REVIEW_DATA_HEADERS + REVIEW_OVERRIDE_HEADERS

# Sheet tab names
SHEET_TRANSACTIONS = "Transactions"
SHEET_ALLOCATIONS = "Allocations"
SHEET_TRANSFERS = "Transfers"
SHEET_SUMMARY = "Summary"
SHEET_REVIEW = "Review"
SHEET_CATEGORIES = "Categories"

# Rate limiting
MAX_WRITES_PER_MINUTE = 50
BATCH_SIZE = 100


# ── Data transformation ──────────────────────────────────


def _val(v: object) -> str | float | int:
    """Convert a value for Sheets: None → empty string, else pass through."""
    if v is None:
        return ""
    return v


def txn_to_row(txn: Transaction, transfer_flag: bool, category_id: str | None = None) -> list:
    """Convert a Transaction dataclass to a Sheets row."""
    return [
        txn.id,
        txn.date,
        txn.amount,
        txn.raw_description,
        _val(txn.normalized_description),
        _val(txn.memo),
        _val(txn.txn_type),
        _val(txn.check_num),
        txn.account_id,
        txn.status,
        _val(txn.confidence),
        _val(txn.categorization_method),
        transfer_flag,
        _val(txn.receipt_lookup_status),
        _val(category_id),
    ]


def alloc_to_row(alloc: Allocation) -> list:
    """Convert an Allocation dataclass to a Sheets row."""
    return [
        alloc.id,
        alloc.transaction_id,
        alloc.category_id,
        alloc.amount,
        _val(alloc.memo),
        _val(alloc.tags),
        alloc.source,
        _val(alloc.confidence),
    ]


def transfer_to_row(xfer: Transfer) -> list:
    """Convert a Transfer dataclass to a Sheets row."""
    return [
        xfer.id,
        xfer.from_transaction_id,
        xfer.to_transaction_id,
        xfer.transfer_type,
        xfer.match_method,
        _val(xfer.confidence),
    ]


def summary_to_row(month: str, row: dict, cat_lookup: dict[str, dict] | None = None) -> list:
    """Convert a category summary dict to a Sheets row with hierarchy."""
    cat_id = row["category_id"]
    meta = (cat_lookup or {}).get(cat_id, {})
    return [
        month,
        cat_id,
        meta.get("name", ""),
        meta.get("level_0", ""),
        meta.get("level_1", ""),
        meta.get("level_2", ""),
        row["total"],
        row["txn_count"],
    ]


def category_to_row(entry: dict) -> list:
    """Convert a flat category metadata dict to a Categories sheet row."""
    return [
        entry["category_id"],
        entry["name"],
        entry["level"],
        entry["level_0"],
        entry["level_1"],
        entry["level_2"],
        entry["level_3"],
        entry["is_leaf"],
        entry["is_income"],
        entry["is_transfer"],
    ]


def review_to_row(
    txn: Transaction, alloc: Allocation | None, transfer_flag: bool,
) -> list:
    """Convert a (transaction, allocation) pair to a Review sheet row.

    For split transactions, call once per allocation. For transactions
    with no allocation (pending), pass alloc=None.
    """
    return [
        txn.date,
        txn.account_id,
        txn.amount,
        _val(txn.normalized_description),
        alloc.category_id if alloc else "",
        alloc.amount if alloc else txn.amount,
        _val(txn.confidence),
        _val(txn.categorization_method),
        alloc.source if alloc else "",
        txn.status,
        transfer_flag,
        txn.raw_description,
        _val(txn.memo),
        _val(txn.receipt_lookup_status),
        _val(txn.txn_type),
        _val(txn.check_num),
        _val(alloc.memo) if alloc else "",
        _val(alloc.tags) if alloc else "",
        txn.id,
        alloc.id if alloc else "",
        _val(alloc.confidence) if alloc else "",
    ]


# ── Batch result ─────────────────────────────────────────


@dataclass
class PushResult:
    """Result of a push operation."""
    sheet: str
    rows_pushed: int
    api_calls: int


# ── SheetsPush ───────────────────────────────────────────


class SheetsPush:
    """Unidirectional SQLite → Google Sheets sync with batching and rate limiting.

    Args:
        spreadsheet: A gspread.Spreadsheet instance (or mock).
    """

    def __init__(self, spreadsheet: object):
        self.spreadsheet = spreadsheet
        self.pending_appends: dict[str, list[list]] = {}
        self.pending_clears: set[str] = set()
        self._write_times: deque[float] = deque()

    # ── Queue operations ─────────────────────────────────

    def queue_append(self, sheet_name: str, rows: list[list]) -> None:
        """Queue rows for append to a sheet. Call flush() to execute."""
        if sheet_name not in self.pending_appends:
            self.pending_appends[sheet_name] = []
        self.pending_appends[sheet_name].extend(rows)

    def queue_clear(self, sheet_name: str) -> None:
        """Queue a sheet for clearing (used by full_rebuild)."""
        self.pending_clears.add(sheet_name)

    # ── Rate limiting ────────────────────────────────────

    def _wait_for_rate_limit(self) -> None:
        """Sleep if we're approaching the write rate limit."""
        now = time.monotonic()
        cutoff = now - 60.0
        # Purge timestamps older than 60 seconds
        while self._write_times and self._write_times[0] <= cutoff:
            self._write_times.popleft()

        if len(self._write_times) >= MAX_WRITES_PER_MINUTE:
            sleep_until = self._write_times[0] + 60.0
            time.sleep(sleep_until - now)

    def _record_write(self) -> None:
        """Record a write operation timestamp."""
        self._write_times.append(time.monotonic())

    # ── Flush ────────────────────────────────────────────

    def flush(self) -> list[PushResult]:
        """Execute all pending operations with rate limiting and batching.

        Returns a list of PushResult for each sheet that was written to.
        Continues processing remaining sheets if one fails.
        """
        results: list[PushResult] = []
        errors: list[str] = []

        # Process clears first
        for sheet_name in list(self.pending_clears):
            try:
                self._wait_for_rate_limit()
                ws = self.spreadsheet.worksheet(sheet_name)
                ws.clear()
                self._record_write()
                self.pending_clears.discard(sheet_name)
            except Exception as e:
                logger.error("Failed to clear sheet '%s': %s", sheet_name, e)
                errors.append(f"clear:{sheet_name}")

        # Process appends
        for sheet_name, rows in list(self.pending_appends.items()):
            if not rows:
                continue
            try:
                ws = self.spreadsheet.worksheet(sheet_name)
                api_calls = 0
                for i in range(0, len(rows), BATCH_SIZE):
                    chunk = rows[i : i + BATCH_SIZE]
                    self._wait_for_rate_limit()
                    ws.append_rows(chunk, value_input_option="RAW")
                    self._record_write()
                    api_calls += 1
                results.append(PushResult(
                    sheet=sheet_name,
                    rows_pushed=len(rows),
                    api_calls=api_calls,
                ))
                del self.pending_appends[sheet_name]
            except Exception as e:
                logger.error("Failed to push to sheet '%s': %s", sheet_name, e)
                errors.append(f"push:{sheet_name}")

        if errors:
            logger.warning("Sheets push completed with %d error(s): %s", len(errors), errors)

        return results

    # ── High-level push methods ──────────────────────────

    def push_transactions(self, txns: list[Transaction], repo: Repository) -> PushResult | None:
        """Transform and push transactions to the Transactions sheet."""
        if not txns:
            return None
        txn_ids = [t.id for t in txns]
        transfer_ids = batch_is_transfer(repo.conn, txn_ids)
        alloc_map = repo.get_allocations_by_transaction_ids(txn_ids)
        rows = []
        for txn in txns:
            flag = txn.id in transfer_ids
            allocs = alloc_map.get(txn.id, [])
            cat = allocs[0].category_id if allocs else None
            rows.append(txn_to_row(txn, flag, category_id=cat))
        self.queue_append(SHEET_TRANSACTIONS, rows)
        results = self.flush()
        return results[0] if results else None

    def push_allocations(self, allocs: list[Allocation]) -> PushResult | None:
        """Transform and push allocations to the Allocations sheet."""
        if not allocs:
            return None
        rows = [alloc_to_row(a) for a in allocs]
        self.queue_append(SHEET_ALLOCATIONS, rows)
        results = self.flush()
        return results[0] if results else None

    def push_transfers(self, xfers: list[Transfer]) -> PushResult | None:
        """Transform and push transfers to the Transfers sheet."""
        if not xfers:
            return None
        rows = [transfer_to_row(x) for x in xfers]
        self.queue_append(SHEET_TRANSFERS, rows)
        results = self.flush()
        return results[0] if results else None

    def push_review(self, txns: list[Transaction], repo: Repository) -> PushResult | None:
        """Transform and push transaction+allocation pairs to the Review sheet."""
        if not txns:
            return None
        txn_ids = [t.id for t in txns]
        transfer_ids = batch_is_transfer(repo.conn, txn_ids)
        alloc_map = repo.get_allocations_by_transaction_ids(txn_ids)
        rows = []
        for txn in txns:
            flag = txn.id in transfer_ids
            allocs = alloc_map.get(txn.id, [])
            if allocs:
                for a in allocs:
                    rows.append(review_to_row(txn, a, flag))
            else:
                rows.append(review_to_row(txn, None, flag))
        self.queue_append(SHEET_REVIEW, rows)
        results = self.flush()
        return results[0] if results else None

    def push_summary(self, month: str, repo: Repository) -> PushResult | None:
        """Compute and push monthly category summary."""
        summary = get_category_summary(repo.conn, month)
        if not summary:
            return None
        rows = [summary_to_row(month, s) for s in summary]
        self.queue_append(SHEET_SUMMARY, rows)
        results = self.flush()
        return results[0] if results else None

    def full_rebuild(self, repo: Repository, config=None) -> list[PushResult]:
        """Truncate all sheets and re-push all data from SQLite.

        Clears all six sheets, writes headers, then pushes all
        transactions, allocations, transfers, summary, review, and
        category hierarchy data.
        """
        sheet_names = [
            SHEET_TRANSACTIONS, SHEET_ALLOCATIONS,
            SHEET_TRANSFERS, SHEET_SUMMARY, SHEET_REVIEW,
            SHEET_CATEGORIES,
        ]
        for name in sheet_names:
            self.queue_clear(name)

        # Write headers
        self.queue_append(SHEET_TRANSACTIONS, [TXN_HEADERS])
        self.queue_append(SHEET_ALLOCATIONS, [ALLOC_HEADERS])
        self.queue_append(SHEET_TRANSFERS, [TRANSFER_HEADERS])
        self.queue_append(SHEET_SUMMARY, [SUMMARY_HEADERS])
        self.queue_append(SHEET_REVIEW, [REVIEW_HEADERS])
        self.queue_append(SHEET_CATEGORIES, [CATEGORY_HEADERS])

        # Build category hierarchy lookup
        cat_lookup: dict[str, dict] = {}
        if config is not None:
            cat_lookup = config.flatten_category_tree()
            cat_rows = [category_to_row(e) for e in cat_lookup.values()]
            self.queue_append(SHEET_CATEGORIES, cat_rows)

        # Fetch all data with batch queries (avoids N+1 per-transaction lookups)
        all_txn_rows = repo.conn.execute(
            "SELECT * FROM transactions ORDER BY date, rowid"
        ).fetchall()
        all_txns = [Repository._row_to_transaction(r) for r in all_txn_rows]

        txn_ids = [t.id for t in all_txns]
        transfer_ids = batch_is_transfer(repo.conn, txn_ids)
        alloc_map = repo.get_allocations_by_transaction_ids(txn_ids)

        txn_rows = []
        alloc_rows = []
        review_rows = []
        for txn in all_txns:
            flag = txn.id in transfer_ids
            allocs = alloc_map.get(txn.id, [])
            cat = allocs[0].category_id if allocs else None
            txn_rows.append(txn_to_row(txn, flag, category_id=cat))
            if allocs:
                for a in allocs:
                    alloc_rows.append(alloc_to_row(a))
                    review_rows.append(review_to_row(txn, a, flag))
            else:
                review_rows.append(review_to_row(txn, None, flag))

        self.queue_append(SHEET_TRANSACTIONS, txn_rows)
        self.queue_append(SHEET_ALLOCATIONS, alloc_rows)
        self.queue_append(SHEET_REVIEW, review_rows)

        all_xfers = repo.conn.execute(
            "SELECT * FROM transfers ORDER BY created_at"
        ).fetchall()
        xfer_rows = [transfer_to_row(Repository._row_to_transfer(r)) for r in all_xfers]
        self.queue_append(SHEET_TRANSFERS, xfer_rows)

        # Push summary for all months that have data
        months = repo.conn.execute(
            "SELECT DISTINCT SUBSTR(date, 1, 7) AS month"
            " FROM transactions ORDER BY month"
        ).fetchall()
        for month_row in months:
            month = month_row[0]
            summary = get_category_summary(repo.conn, month)
            for s in summary:
                self.queue_append(SHEET_SUMMARY,
                                  [summary_to_row(month, s, cat_lookup)])

        results = self.flush()

        # Apply data validation dropdowns to Override: Category columns
        if config is not None:
            self._apply_category_validation()

        return results

    # ── Data validation ──────────────────────────────────

    def _apply_category_validation(self) -> None:
        """Apply dropdown data validation to Override: Category columns.

        References Categories!A2:A so the dropdown stays in sync with
        the Categories sheet automatically. Safe across clear()+append_rows()
        cycles since clear() only removes values, not validation rules.
        """
        if ValidationConditionType is None:
            logger.warning("gspread ValidationConditionType not available, skipping validation")
            return

        # (sheet_name, column_letter) for each Override: Category column
        validation_targets = [
            (SHEET_TRANSACTIONS, "P"),   # col 16 = TXN_OVERRIDE_START + CATEGORY
            (SHEET_ALLOCATIONS, "I"),    # col 9  = ALLOC_OVERRIDE_START + CATEGORY
            (SHEET_REVIEW, "V"),         # col 22 = REVIEW_OVERRIDE_START + CATEGORY
        ]

        for sheet_name, col_letter in validation_targets:
            try:
                ws = self.spreadsheet.worksheet(sheet_name)
                ws.add_validation(
                    f"{col_letter}2:{col_letter}100000",
                    ValidationConditionType.one_of_range,
                    [f"={SHEET_CATEGORIES}!A2:A"],
                    showCustomUi=True,
                    strict=False,
                )
                logger.info("Applied category validation to %s column %s", sheet_name, col_letter)
            except Exception as e:
                logger.warning("Failed to apply validation to %s: %s", sheet_name, e)
