"""Mercury CSV parser.

Single CSV export contains transactions for all Mercury sub-accounts
(checking, savings, credit). Routes to the correct account via the
'Source Account' column.

Filters out Failed/Cancelled status rows.
Uses Timestamp column (sub-second precision) as external_id for dedup.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from .base import BaseParser, RawTransaction

logger = logging.getLogger(__name__)


class MercuryCsvParser(BaseParser):
    """Parse Mercury bank CSV exports.

    Args:
        account_routing: Maps 'Source Account' column values to internal
            account IDs. Example: {"Mercury Checking xx1234": "mercury-checking"}.
            Configure in config/accounts.yaml.
    """

    SKIP_STATUSES: set[str] = {"Failed", "Cancelled"}

    def __init__(self, account_routing: dict[str, str] | None = None):
        super().__init__()
        self.account_routing = account_routing or {}

    def detect(self, file_path: Path) -> bool:
        """Mercury CSVs have 'Source Account' and 'Mercury Category' columns."""
        try:
            with open(file_path, "r", errors="replace") as f:
                header = f.readline()
            return "Source Account" in header and "Mercury Category" in header
        except (OSError, UnicodeDecodeError):
            return False

    def parse(self, file_path: Path) -> list[RawTransaction]:
        transactions: list[RawTransaction] = []
        self.skipped_count = 0  # Reset for each parse

        with open(file_path, "r", newline="", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if self._should_skip(row):
                    self.skipped_count += 1
                    continue
                txn = self._parse_row(row)
                if txn is not None:
                    transactions.append(txn)
                else:
                    self.skipped_count += 1

        return transactions

    def _should_skip(self, row: dict) -> bool:
        status = row.get("Status", "").strip()
        return status in self.SKIP_STATUSES

    def _parse_row(self, row: dict) -> RawTransaction | None:
        source = row.get("Source Account", "").strip()
        account_id = self.account_routing.get(source)
        if account_id is None:
            if source and not hasattr(self, "_warned_accounts"):
                self._warned_accounts: set[str] = set()
            if source and source not in getattr(self, "_warned_accounts", set()):
                logger.warning(
                    "Skipping transactions for unmapped Mercury account: '%s' "
                    "(add to accounts.yaml with import_format: mercury_csv)",
                    source,
                )
                self._warned_accounts.add(source)
            return None

        date_str = row.get("Date (UTC)", "").strip()
        amount_str = row.get("Amount", "").strip()
        if not date_str or not amount_str:
            return None

        date = self._parse_date(date_str)
        if date is None:
            return None

        try:
            amount = float(amount_str.replace(",", ""))
        except (ValueError, TypeError):
            return None

        description = row.get("Description", "").strip()
        bank_desc = row.get("Bank Description", "").strip()
        memo = row.get("Note", "").strip() or None
        timestamp = row.get("Timestamp", "").strip() or None
        check_num = row.get("Check Number", "").strip() or None

        return RawTransaction(
            date=date,
            amount=amount,
            raw_description=description or bank_desc,
            account_id=account_id,
            memo=memo,
            txn_type=None,
            external_id=timestamp,
            check_num=check_num,
        )

    @staticmethod
    def _parse_date(date_str: str) -> str | None:
        """MM-DD-YYYY → YYYY-MM-DD. Already-correct dates pass through.

        Returns None if format is invalid.
        """
        parts = date_str.split("-")
        if len(parts) == 3:
            if len(parts[0]) == 4:
                # Validate YYYY-MM-DD format
                if all(p.isdigit() for p in parts):
                    return date_str
            else:
                # MM-DD-YYYY format
                mm, dd, yyyy = parts
                if mm.isdigit() and dd.isdigit() and yyyy.isdigit():
                    return f"{yyyy}-{mm}-{dd}"
        return None
