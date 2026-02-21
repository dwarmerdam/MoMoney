"""Budget app Register CSV parser for one-time historical import.

Parses budget app exports.
Maps budget app account names and categories to MoMoney IDs.

Format quirks:
- BOM marker (\\ufeff) at file start
- Amounts: "$1,477.40" with dollar sign and comma separator
- Separate Outflow/Inflow columns → signed amount = inflow - outflow
- Transfers: Payee starts with "Transfer : "
- Date format: MM/DD/YYYY
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from .base import BaseParser, RawTransaction

logger = logging.getLogger(__name__)


class BudgetAppCsvParser(BaseParser):
    """Parse budget app Register CSV exports.

    Args:
        category_map: Maps budget app category strings to new category IDs.
        account_routing: Maps budget app account names to internal account IDs.
            Example: {"My Checking": "primary-checking"}.
            Configure in config/accounts.yaml (budget_app_name field).
    """

    def __init__(
        self,
        category_map: dict | None = None,
        account_routing: dict[str, str] | None = None,
    ):
        super().__init__()
        self.category_map = category_map or {}
        self.account_routing = account_routing or {}

    def detect(self, file_path: Path) -> bool:
        """Budget app CSVs have 'Category Group/Category' and 'Outflow' columns."""
        try:
            with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
                header = f.readline()
            return "Category Group/Category" in header and "Outflow" in header
        except (OSError, UnicodeDecodeError):
            return False

    def parse(self, file_path: Path) -> list[RawTransaction]:
        transactions: list[RawTransaction] = []
        self.skipped_count = 0  # Reset for each parse

        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                txn = self._parse_row(row)
                if txn is not None:
                    transactions.append(txn)
                else:
                    self.skipped_count += 1

        return transactions

    def _parse_row(self, row: dict) -> RawTransaction | None:
        account_name = row.get("Account", "").strip()
        account_id = self.account_routing.get(account_name)
        if account_id is None:
            if account_name and not hasattr(self, "_warned_accounts"):
                self._warned_accounts: set[str] = set()
            if account_name and account_name not in getattr(self, "_warned_accounts", set()):
                logger.warning(
                    "Skipping transactions for unmapped budget app account: '%s' "
                    "(add budget_app_name to accounts.yaml)",
                    account_name,
                )
                self._warned_accounts.add(account_name)
            return None

        date_str = row.get("Date", "").strip()
        outflow_str = row.get("Outflow", "").strip()
        inflow_str = row.get("Inflow", "").strip()

        if not date_str:
            return None

        date = self._parse_date(date_str)
        if date is None:
            return None

        amount = self._parse_amount(outflow_str, inflow_str)
        if amount is None:
            return None

        payee = row.get("Payee", "").strip()
        memo = row.get("Memo", "").strip() or None
        budget_app_category = row.get("Category Group/Category", "").strip()

        # Look up budget app category in the category map
        mapped_category_id = self.category_map.get(budget_app_category)

        return RawTransaction(
            date=date,
            amount=amount,
            raw_description=payee,
            account_id=account_id,
            memo=memo,
            txn_type="TRANSFER" if self._is_transfer(payee) else None,
            budget_app_category_id=mapped_category_id,
        )

    @staticmethod
    def _parse_date(date_str: str) -> str | None:
        """MM/DD/YYYY → YYYY-MM-DD. Returns None if format is invalid."""
        parts = date_str.split("/")
        if len(parts) == 3:
            mm, dd, yyyy = parts
            # Basic validation
            if mm.isdigit() and dd.isdigit() and yyyy.isdigit():
                return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
        return None

    @staticmethod
    def _parse_amount(outflow: str, inflow: str) -> float | None:
        """Strip '$' and commas. Return inflow - outflow (signed).

        Returns None if amount cannot be parsed.
        """
        def clean(s: str) -> float:
            s = s.replace("$", "").replace(",", "").strip()
            return float(s) if s else 0.0
        try:
            return clean(inflow) - clean(outflow)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _is_transfer(payee: str) -> bool:
        return payee.startswith("Transfer : ") or payee.startswith("Transfer :")
