"""Base parser: shared interface, data structures, and utility functions."""

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RawTransaction:
    """Intermediate representation output by parsers, before DB insertion."""
    date: str              # YYYY-MM-DD (normalized by parser)
    amount: float          # signed: negative=outflow, positive=inflow
    raw_description: str
    account_id: str        # resolved by parser or routing config
    memo: str | None = None
    txn_type: str | None = None    # DEBIT, CREDIT, CHECK, INT, etc.
    external_id: str | None = None # FITID (QFX) or timestamp (Mercury)
    check_num: str | None = None
    balance: float | None = None
    budget_app_category_id: str | None = None  # Pre-mapped category from budget app import


class BaseParser(ABC):
    """Abstract base for all file parsers.

    Attributes:
        skipped_count: Number of rows skipped during parsing (e.g., due to
            missing account routing, invalid data). Check this after parse()
            to detect silent data loss.
    """

    def __init__(self):
        self.skipped_count: int = 0

    @abstractmethod
    def parse(self, file_path: Path) -> list[RawTransaction]:
        """Parse a bank file and return normalized transactions.

        Implementations should increment self.skipped_count when rows are
        skipped due to validation failures, missing account routing, etc.
        """

    @abstractmethod
    def detect(self, file_path: Path) -> bool:
        """Return True if this parser can handle the given file."""


def normalize_description(desc: str) -> str:
    """Normalize a bank transaction description for matching.

    - Uppercase
    - Strip long numbers (4+ digits)
    - Strip #123 patterns
    - Strip * and # decorators
    - Collapse whitespace
    """
    desc = desc.upper()
    desc = re.sub(r'\d{4,}', '', desc)
    desc = re.sub(r'#\d+', '', desc)
    desc = re.sub(r'[*#]', '', desc)
    desc = re.sub(r'\s{2,}', ' ', desc)
    return desc.strip()


def compute_import_hash(
    account_id: str, date: str, amount: float, raw_desc: str
) -> str:
    """Tier 3 dedup: SHA256(account|date|amount|raw_desc)."""
    key = f"{account_id}|{date}|{amount:.2f}|{raw_desc}"
    return hashlib.sha256(key.encode()).hexdigest()


def compute_dedup_key(account_id: str, date: str, amount: float) -> str:
    """Tier 4 dedup: {account_id}:{date}:{amount_cents}."""
    cents = int(round(amount * 100))
    return f"{account_id}:{date}:{cents}"


def compute_file_hash(file_path: Path) -> str:
    """Tier 1 dedup: SHA256 of entire file contents."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_ofx_date(dtposted: str) -> str | None:
    """Extract YYYY-MM-DD from OFX date strings.

    Handles formats:
        20241231120000.000[-7:MST]
        20241231120000[0:GMT]
        20241231110000.000[-7:PDT]
        20241231

    Returns None if the date string is too short or invalid.
    """
    if not dtposted or len(dtposted) < 8:
        return None
    digits = dtposted[:8]
    # Basic validation: should be all digits
    if not digits.isdigit():
        return None
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
