"""Override poller: read user corrections from Google Sheets and apply to SQLite.

Polls override columns in the Transactions, Allocations, and Review sheets,
applies non-blank values to the database, then clears the cells to
prevent duplicate application on the next poll cycle.
"""

from __future__ import annotations

import difflib
import logging
from dataclasses import dataclass, field

from src.database.models import Allocation, Transfer
from src.database.repository import Repository

logger = logging.getLogger(__name__)

# Override column positions (0-indexed from start of override columns)
# These follow immediately after the data columns in each sheet.

# Transactions sheet override columns (after column O = index 14)
TXN_OVERRIDE_START = 15  # Column P (15 data columns A-O)
TXN_OVERRIDE_CATEGORY = 0   # Offset from TXN_OVERRIDE_START
TXN_OVERRIDE_MERCHANT = 1
TXN_OVERRIDE_REVIEWED = 2
TXN_OVERRIDE_TRANSFER_LINK = 3
TXN_OVERRIDE_NEEDS_SPLIT = 4
TXN_OVERRIDE_NOTES = 5

# Allocations sheet override columns (after column H = index 7)
ALLOC_OVERRIDE_START = 8  # Column I
ALLOC_OVERRIDE_CATEGORY = 0  # Offset from ALLOC_OVERRIDE_START
ALLOC_OVERRIDE_MERCHANT = 1

# Review sheet override columns (after column U = index 20)
REVIEW_OVERRIDE_START = 21  # Column V
REVIEW_OVERRIDE_CATEGORY = 0   # Offset from REVIEW_OVERRIDE_START
REVIEW_OVERRIDE_MERCHANT = 1
REVIEW_OVERRIDE_REVIEWED = 2

# Review sheet ID column positions (0-indexed)
REVIEW_COL_TXN_ID = 18    # Column S = transaction_id
REVIEW_COL_ALLOC_ID = 19  # Column T = allocation_id

# Boolean-like values that should be treated as True
_TRUE_VALUES = frozenset({"true", "yes", "1", "x", "y"})


def _parse_bool(value: str) -> bool:
    """Parse a boolean-like string value flexibly.

    Accepts: TRUE, true, True, yes, YES, 1, x, X, y, Y
    Returns False for empty strings and other values.
    """
    return value.strip().lower() in _TRUE_VALUES


@dataclass
class OverrideAction:
    """Record of an override that was applied."""
    sheet: str
    row_index: int
    column: str
    entity_id: str
    old_value: str | None
    new_value: str


@dataclass
class PollResult:
    """Result of a poll cycle."""
    overrides_applied: int = 0
    errors: int = 0
    actions: list[OverrideAction] = field(default_factory=list)


class OverridePoller:
    """Polls Google Sheets override columns and applies corrections to SQLite.

    Args:
        spreadsheet: A gspread.Spreadsheet instance (or mock).
        repo: The database repository.
        config: Optional Config for category validation. When provided,
            category overrides are validated against the category tree
            with fuzzy matching for typos.
    """

    def __init__(self, spreadsheet: object, repo: Repository, config=None):
        self.spreadsheet = spreadsheet
        self.repo = repo
        self.config = config
        self._valid_category_ids: set[str] | None = None

    def poll(self) -> PollResult:
        """Read override columns, apply non-blank values, clear cells.

        Only clears override columns if all overrides in that sheet were
        applied successfully. If any errors occurred, the columns are left
        intact so failed overrides can be retried on the next poll cycle.

        Returns a PollResult with counts and action details.
        """
        result = PollResult()

        self._poll_transaction_overrides(result)
        self._poll_allocation_overrides(result)
        self._poll_review_overrides(result)

        return result

    # ── Transaction overrides ────────────────────────────

    def _poll_transaction_overrides(self, result: PollResult) -> None:
        """Read and apply overrides from the Transactions sheet."""
        ws = self.spreadsheet.worksheet("Transactions")
        all_values = ws.get_all_values()

        if len(all_values) <= 1:
            return  # Header only or empty

        errors_before = result.errors

        for row_idx, row in enumerate(all_values[1:], start=2):  # 1-indexed, skip header
            if len(row) <= TXN_OVERRIDE_START:
                continue

            txn_id = row[0]  # Column A = id
            if not txn_id:
                continue

            overrides = row[TXN_OVERRIDE_START:]

            # Override: Category
            new_cat = overrides[TXN_OVERRIDE_CATEGORY] if len(overrides) > TXN_OVERRIDE_CATEGORY else ""
            if new_cat.strip():
                self._apply_txn_category_override(txn_id, new_cat.strip(), result, row_idx)

            # Override: Merchant
            new_merchant = overrides[TXN_OVERRIDE_MERCHANT] if len(overrides) > TXN_OVERRIDE_MERCHANT else ""
            if new_merchant.strip():
                self._apply_merchant_override(txn_id, new_merchant.strip(), result, row_idx)

            # Override: Reviewed
            reviewed = overrides[TXN_OVERRIDE_REVIEWED] if len(overrides) > TXN_OVERRIDE_REVIEWED else ""
            if _parse_bool(reviewed):
                self._apply_reviewed_flag(txn_id, result, row_idx)

            # Override: Transfer Link
            transfer_link = overrides[TXN_OVERRIDE_TRANSFER_LINK] if len(overrides) > TXN_OVERRIDE_TRANSFER_LINK else ""
            if transfer_link.strip():
                self._apply_transfer_link(txn_id, transfer_link.strip(), result, row_idx)

            # Override: Needs Split
            needs_split = overrides[TXN_OVERRIDE_NEEDS_SPLIT] if len(overrides) > TXN_OVERRIDE_NEEDS_SPLIT else ""
            if _parse_bool(needs_split):
                self._apply_needs_split(txn_id, result, row_idx)

            # Override: Notes
            notes = overrides[TXN_OVERRIDE_NOTES] if len(overrides) > TXN_OVERRIDE_NOTES else ""
            if notes.strip():
                self._apply_notes(txn_id, notes.strip(), result, row_idx)

        # Only clear override columns if no errors occurred during this sheet's processing
        if result.errors == errors_before:
            self._clear_override_columns(ws, len(all_values), TXN_OVERRIDE_START, 6)
        else:
            logger.warning(
                "Skipping Transactions override clear: %d errors occurred",
                result.errors - errors_before,
            )

    # ── Allocation overrides ─────────────────────────────

    def _poll_allocation_overrides(self, result: PollResult) -> None:
        """Read and apply overrides from the Allocations sheet."""
        ws = self.spreadsheet.worksheet("Allocations")
        all_values = ws.get_all_values()

        if len(all_values) <= 1:
            return

        errors_before = result.errors

        for row_idx, row in enumerate(all_values[1:], start=2):
            if len(row) <= ALLOC_OVERRIDE_START:
                continue

            alloc_id = row[0]  # Column A = id
            txn_id = row[1]    # Column B = transaction_id
            if not alloc_id:
                continue

            overrides = row[ALLOC_OVERRIDE_START:]

            # Override: Category
            new_cat = overrides[ALLOC_OVERRIDE_CATEGORY] if len(overrides) > ALLOC_OVERRIDE_CATEGORY else ""
            if new_cat.strip():
                self._apply_category_override(alloc_id, txn_id, new_cat.strip(), result, row_idx)

            # Override: Merchant (updates transaction normalized_description)
            new_merchant = overrides[ALLOC_OVERRIDE_MERCHANT] if len(overrides) > ALLOC_OVERRIDE_MERCHANT else ""
            if new_merchant.strip():
                self._apply_merchant_override(txn_id, new_merchant.strip(), result, row_idx)

        # Only clear override columns if no errors occurred during this sheet's processing
        if result.errors == errors_before:
            self._clear_override_columns(ws, len(all_values), ALLOC_OVERRIDE_START, 2)
        else:
            logger.warning(
                "Skipping Allocations override clear: %d errors occurred",
                result.errors - errors_before,
            )

    # ── Category validation ────────────────────────────────

    def _get_valid_category_ids(self) -> set[str]:
        """Return the set of valid category IDs, cached per instance."""
        if self._valid_category_ids is None:
            if self.config is not None:
                self._valid_category_ids = set(self.config.flatten_category_tree().keys())
            else:
                self._valid_category_ids = set()
        return self._valid_category_ids

    def _validate_category(self, category_id: str) -> str | None:
        """Validate a category ID against the config tree.

        Returns the (possibly corrected) category_id, or None if invalid.
        When config is not set, all categories pass through unchanged.
        """
        valid_ids = self._get_valid_category_ids()
        if not valid_ids:
            return category_id  # No config → skip validation

        if category_id in valid_ids:
            return category_id

        # Fuzzy match
        matches = difflib.get_close_matches(category_id, valid_ids, n=1, cutoff=0.7)
        if matches:
            corrected = matches[0]
            logger.warning(
                "Category '%s' not found, auto-corrected to '%s'",
                category_id, corrected,
            )
            return corrected

        logger.error(
            "Category '%s' is invalid and no close match found (valid: %d categories)",
            category_id, len(valid_ids),
        )
        return None

    # ── Apply methods ────────────────────────────────────

    def _apply_reviewed_flag(self, txn_id: str, result: PollResult, row_idx: int) -> None:
        """Mark transaction as reviewed."""
        try:
            self.repo.update_transaction_status(txn_id, "reviewed")
            action = OverrideAction(
                sheet="Transactions", row_index=row_idx,
                column="Override: Reviewed", entity_id=txn_id,
                old_value=None, new_value="reviewed",
            )
            result.actions.append(action)
            result.overrides_applied += 1
            logger.info("Applied reviewed flag: txn %s", txn_id)
        except Exception:
            logger.exception("Failed to apply reviewed flag: txn %s", txn_id)
            result.errors += 1

    def _apply_transfer_link(
        self, txn_id: str, linked_txn_id: str, result: PollResult, row_idx: int,
    ) -> None:
        """Create a transfer record linking two transactions."""
        try:
            # Validate not linking to self
            if txn_id == linked_txn_id:
                logger.error("Cannot link transaction %s to itself", txn_id)
                result.errors += 1
                return

            existing = self.repo.get_transfer_by_transaction(txn_id)
            if existing:
                logger.warning(
                    "Transfer already exists for txn %s, skipping override", txn_id
                )
                result.errors += 1  # Count as error so override column isn't cleared
                return

            # Validate linked transaction exists
            linked_txn = self.repo.get_transaction(linked_txn_id)
            if linked_txn is None:
                logger.error(
                    "Linked transaction %s not found for transfer link from %s",
                    linked_txn_id, txn_id,
                )
                result.errors += 1
                return

            # Check if the linked transaction is already part of another transfer
            linked_existing = self.repo.get_transfer_by_transaction(linked_txn_id)
            if linked_existing:
                logger.warning(
                    "Linked transaction %s already in a transfer, cannot link from %s",
                    linked_txn_id, txn_id,
                )
                result.errors += 1
                return

            # Get source transaction for validation
            source_txn = self.repo.get_transaction(txn_id)
            if source_txn is None:
                logger.error("Source transaction %s not found for transfer link", txn_id)
                result.errors += 1
                return

            # Warn about same-account transfers (unusual but not blocked)
            if source_txn.account_id == linked_txn.account_id:
                logger.warning(
                    "Transfer link %s → %s: both transactions are in same account '%s'",
                    txn_id, linked_txn_id, source_txn.account_id,
                )

            # Warn if amounts don't approximately cancel (unusual but not blocked)
            amount_sum = source_txn.amount + linked_txn.amount
            if abs(amount_sum) > 0.01:
                logger.warning(
                    "Transfer link %s → %s: amounts don't cancel (%.2f + %.2f = %.2f)",
                    txn_id, linked_txn_id, source_txn.amount, linked_txn.amount, amount_sum,
                )

            xfer = Transfer(
                from_transaction_id=txn_id,
                to_transaction_id=linked_txn_id,
                transfer_type="user-linked",
                match_method="user_override",
                confidence=1.0,
            )
            self.repo.insert_transfer(xfer)
            action = OverrideAction(
                sheet="Transactions", row_index=row_idx,
                column="Override: Transfer Link", entity_id=txn_id,
                old_value=None, new_value=linked_txn_id,
            )
            result.actions.append(action)
            result.overrides_applied += 1
            logger.info("Created transfer link: %s → %s", txn_id, linked_txn_id)
        except Exception:
            logger.exception("Failed to apply transfer link: txn %s", txn_id)
            result.errors += 1

    def _apply_needs_split(self, txn_id: str, result: PollResult, row_idx: int) -> None:
        """Flag transaction for manual split review."""
        try:
            self.repo.update_transaction_status(txn_id, "flagged")
            action = OverrideAction(
                sheet="Transactions", row_index=row_idx,
                column="Override: Needs Split", entity_id=txn_id,
                old_value=None, new_value="flagged",
            )
            result.actions.append(action)
            result.overrides_applied += 1
            logger.info("Applied needs-split flag: txn %s", txn_id)
        except Exception:
            logger.exception("Failed to apply needs-split flag: txn %s", txn_id)
            result.errors += 1

    def _apply_notes(self, txn_id: str, notes: str, result: PollResult, row_idx: int) -> None:
        """Set the transaction memo to the override value."""
        try:
            txn = self.repo.get_transaction(txn_id)
            if txn is None:
                logger.warning("Transaction not found for notes override: %s", txn_id)
                result.errors += 1
                return

            new_memo = f"{txn.memo} | {notes}" if txn.memo else notes
            self.repo.conn.execute(
                "UPDATE transactions SET memo = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (new_memo, txn_id),
            )
            self.repo.conn.commit()
            action = OverrideAction(
                sheet="Transactions", row_index=row_idx,
                column="Override: Notes", entity_id=txn_id,
                old_value=txn.memo, new_value=new_memo,
            )
            result.actions.append(action)
            result.overrides_applied += 1
            logger.info("Applied notes override: txn %s", txn_id)
        except Exception:
            logger.exception("Failed to apply notes override: txn %s", txn_id)
            result.errors += 1

    def _apply_txn_category_override(
        self, txn_id: str, new_category: str,
        result: PollResult, row_idx: int,
    ) -> None:
        """Override category from the Transactions sheet.

        Finds the allocation for the transaction and updates it,
        or creates one if none exists.
        """
        try:
            if not new_category or len(new_category) > 100:
                logger.error(
                    "Invalid category_id '%s' for txn %s", new_category, txn_id
                )
                result.errors += 1
                return

            validated = self._validate_category(new_category)
            if validated is None:
                result.errors += 1
                return
            new_category = validated

            allocs = self.repo.get_allocations_by_transaction(txn_id)
            if allocs:
                # Update existing allocation
                alloc = allocs[0]
                self._apply_category_override(alloc.id, txn_id, new_category, result, row_idx)
            else:
                # No allocation yet — create one
                self._create_allocation_from_override(txn_id, new_category, result, row_idx)
        except Exception:
            logger.exception("Failed to apply txn category override: txn %s", txn_id)
            result.errors += 1

    def _apply_category_override(
        self, alloc_id: str, txn_id: str, new_category: str,
        result: PollResult, row_idx: int,
    ) -> None:
        """Update allocation category and mark source as user."""
        try:
            # Fetch current allocation to capture old_value
            row = self.repo.conn.execute(
                "SELECT category_id FROM allocations WHERE id = ?", (alloc_id,)
            ).fetchone()
            if row is None:
                logger.error("Allocation %s not found for category override", alloc_id)
                result.errors += 1
                return
            old_category = row[0]

            if not new_category or len(new_category) > 100:
                logger.error(
                    "Invalid category_id '%s' for allocation %s", new_category, alloc_id
                )
                result.errors += 1
                return

            validated = self._validate_category(new_category)
            if validated is None:
                result.errors += 1
                return
            new_category = validated

            self.repo.conn.execute(
                "UPDATE allocations SET category_id = ?, source = 'user'"
                " WHERE id = ?",
                (new_category, alloc_id),
            )
            self.repo.conn.commit()
            action = OverrideAction(
                sheet="Allocations", row_index=row_idx,
                column="Override: Category", entity_id=alloc_id,
                old_value=old_category, new_value=new_category,
            )
            result.actions.append(action)
            result.overrides_applied += 1
            logger.info(
                "Applied category override: alloc %s: %s → %s",
                alloc_id, old_category, new_category,
            )
        except Exception:
            logger.exception("Failed to apply category override: alloc %s", alloc_id)
            result.errors += 1

    def _apply_merchant_override(
        self, txn_id: str, new_merchant: str,
        result: PollResult, row_idx: int,
    ) -> None:
        """Update transaction normalized_description from Allocations sheet."""
        try:
            # Fetch current value to capture old_value
            row = self.repo.conn.execute(
                "SELECT normalized_description FROM transactions WHERE id = ?", (txn_id,)
            ).fetchone()
            if row is None:
                logger.error("Transaction %s not found for merchant override", txn_id)
                result.errors += 1
                return
            old_merchant = row[0]

            self.repo.conn.execute(
                "UPDATE transactions SET normalized_description = ?,"
                " updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (new_merchant, txn_id),
            )
            self.repo.conn.commit()
            action = OverrideAction(
                sheet="Allocations", row_index=row_idx,
                column="Override: Merchant", entity_id=txn_id,
                old_value=old_merchant, new_value=new_merchant,
            )
            result.actions.append(action)
            result.overrides_applied += 1
            logger.info(
                "Applied merchant override: txn %s: %s → %s",
                txn_id, old_merchant, new_merchant,
            )
        except Exception:
            logger.exception("Failed to apply merchant override: txn %s", txn_id)
            result.errors += 1

    # ── Review overrides ──────────────────────────────────

    def _poll_review_overrides(self, result: PollResult) -> None:
        """Read and apply overrides from the Review sheet."""
        try:
            ws = self.spreadsheet.worksheet("Review")
        except Exception:
            return  # Review sheet may not exist yet

        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return

        errors_before = result.errors

        for row_idx, row in enumerate(all_values[1:], start=2):
            if len(row) <= REVIEW_OVERRIDE_START:
                continue

            txn_id = row[REVIEW_COL_TXN_ID] if len(row) > REVIEW_COL_TXN_ID else ""
            alloc_id = row[REVIEW_COL_ALLOC_ID] if len(row) > REVIEW_COL_ALLOC_ID else ""
            if not txn_id:
                continue

            overrides = row[REVIEW_OVERRIDE_START:]

            # Override: Category
            new_cat = overrides[REVIEW_OVERRIDE_CATEGORY] if len(overrides) > REVIEW_OVERRIDE_CATEGORY else ""
            if new_cat.strip():
                if alloc_id:
                    self._apply_category_override(alloc_id, txn_id, new_cat.strip(), result, row_idx)
                else:
                    self._create_allocation_from_override(txn_id, new_cat.strip(), result, row_idx)

            # Override: Merchant
            new_merchant = overrides[REVIEW_OVERRIDE_MERCHANT] if len(overrides) > REVIEW_OVERRIDE_MERCHANT else ""
            if new_merchant.strip():
                self._apply_merchant_override(txn_id, new_merchant.strip(), result, row_idx)

            # Override: Reviewed
            reviewed = overrides[REVIEW_OVERRIDE_REVIEWED] if len(overrides) > REVIEW_OVERRIDE_REVIEWED else ""
            if _parse_bool(reviewed):
                self._apply_reviewed_flag(txn_id, result, row_idx)

        if result.errors == errors_before:
            self._clear_override_columns(ws, len(all_values), REVIEW_OVERRIDE_START, 3)
        else:
            logger.warning(
                "Skipping Review override clear: %d errors occurred",
                result.errors - errors_before,
            )

    def _create_allocation_from_override(
        self, txn_id: str, category: str, result: PollResult, row_idx: int,
    ) -> None:
        """Create a new allocation for an uncategorized transaction via Review override."""
        try:
            txn = self.repo.get_transaction(txn_id)
            if txn is None:
                logger.error("Transaction %s not found for Review category override", txn_id)
                result.errors += 1
                return

            if not category or len(category) > 100:
                logger.error("Invalid category_id '%s' for txn %s", category, txn_id)
                result.errors += 1
                return

            validated = self._validate_category(category)
            if validated is None:
                result.errors += 1
                return
            category = validated

            alloc = Allocation(
                transaction_id=txn_id,
                category_id=category,
                amount=txn.amount,
                source="user",
                confidence=1.0,
            )
            self.repo.insert_allocation(alloc)
            self.repo.update_transaction_status(txn_id, "categorized", confidence=1.0, method="user_override")
            action = OverrideAction(
                sheet="Review", row_index=row_idx,
                column="Override: Category", entity_id=txn_id,
                old_value=None, new_value=category,
            )
            result.actions.append(action)
            result.overrides_applied += 1
            logger.info("Created allocation from Review override: txn %s → %s", txn_id, category)
        except Exception:
            logger.exception("Failed to create allocation from Review override: txn %s", txn_id)
            result.errors += 1

    # ── Helpers ───────────────────────────────────────────

    @staticmethod
    def _clear_override_columns(
        ws: object, total_rows: int, start_col: int, num_cols: int,
    ) -> None:
        """Clear override columns by writing empty strings.

        Args:
            ws: gspread Worksheet.
            total_rows: Total rows including header.
            start_col: 0-indexed start column of overrides.
            num_cols: Number of override columns to clear.
        """
        if total_rows <= 1:
            return

        # Build a list of empty rows for the override columns
        data_rows = total_rows - 1  # exclude header
        empty_rows = [[""] * num_cols for _ in range(data_rows)]

        # gspread uses 1-indexed rows and columns
        start_cell = _cell_label(2, start_col + 1)  # Row 2 (after header), col 1-indexed
        end_cell = _cell_label(total_rows, start_col + num_cols)
        cell_range = f"{start_cell}:{end_cell}"

        try:
            ws.update(cell_range, empty_rows)
        except Exception:
            logger.exception("Failed to clear override columns %s", cell_range)


def _cell_label(row: int, col: int) -> str:
    """Convert 1-indexed (row, col) to A1 notation. E.g., (2, 14) → 'N2'."""
    label = ""
    c = col
    while c > 0:
        c, remainder = divmod(c - 1, 26)
        label = chr(65 + remainder) + label
    return f"{label}{row}"
