"""QFX/OFX parser for SGML format (no XML declaration).

Handles:
- Wells Fargo checking and savings (single-line SGML, no closing tags)
- Golden1 auto loan (multi-line SGML with indentation, has closing tags)

Both formats use BANKMSGSRSV1 path. Tags are <TAG>value with optional
</TAG> closing. We extract values using regex, not an XML parser.
"""

from __future__ import annotations

import re
from pathlib import Path

from .base import BaseParser, RawTransaction, parse_ofx_date


class QfxSgmlParser(BaseParser):
    """Parse OFX/QFX files in SGML format."""

    def __init__(self, account_id: str):
        super().__init__()
        self.account_id = account_id

    def detect(self, file_path: Path) -> bool:
        """SGML files start with OFXHEADER:100 (no <?xml)."""
        try:
            with open(file_path, "r", errors="replace") as f:
                head = f.read(200)
            return "OFXHEADER:100" in head and "<?xml" not in head.lower()
        except (OSError, UnicodeDecodeError):
            return False

    def parse(self, file_path: Path) -> list[RawTransaction]:
        with open(file_path, "r", errors="replace") as f:
            content = f.read()

        transactions: list[RawTransaction] = []
        self.skipped_count = 0  # Reset for each parse
        balance = self._extract_balance(content)

        for block in self._split_transactions(content):
            txn = self._parse_transaction_block(block, balance)
            if txn is not None:
                transactions.append(txn)
            else:
                self.skipped_count += 1

        return transactions

    def _split_transactions(self, content: str) -> list[str]:
        """Split content into individual STMTTRN blocks."""
        # Match from <STMTTRN> to </STMTTRN> or next <STMTTRN> or </BANKTRANLIST> or end-of-string
        pattern = r'<STMTTRN>(.*?)(?=</STMTTRN>|<STMTTRN>|</BANKTRANLIST>|\Z)'
        return re.findall(pattern, content, re.DOTALL)

    def _parse_transaction_block(
        self, block: str, balance: float | None
    ) -> RawTransaction | None:
        """Extract fields from a single STMTTRN block."""
        dtposted = self._extract_tag(block, "DTPOSTED")
        trnamt = self._extract_tag(block, "TRNAMT")
        fitid = self._extract_tag(block, "FITID")
        name = self._extract_tag(block, "NAME")
        memo = self._extract_tag(block, "MEMO")
        trntype = self._extract_tag(block, "TRNTYPE")
        checknum = self._extract_tag(block, "CHECKNUM")

        if not dtposted or not trnamt:
            return None

        # Parse date with validation
        date = parse_ofx_date(dtposted)
        if date is None:
            return None

        # Parse amount with error handling
        try:
            amount = float(trnamt)
        except (ValueError, TypeError):
            return None

        return RawTransaction(
            date=date,
            amount=amount,
            raw_description=name or "",
            account_id=self.account_id,
            memo=memo,
            txn_type=trntype,
            external_id=fitid,
            check_num=checknum,
            balance=balance,
        )

    def _extract_tag(self, block: str, tag: str) -> str | None:
        """Extract value for a SGML tag.

        Handles both:
            <TAG>value          (WF style, no closing tag)
            <TAG>value</TAG>    (Golden1 style, with closing)
            <TAG>value\\n        (value terminated by newline or next tag)
        """
        pattern = rf'<{tag}>([^<\n\r]*)'
        match = re.search(pattern, block)
        if match:
            return match.group(1).strip()
        return None

    def _extract_balance(self, content: str) -> float | None:
        """Extract LEDGERBAL > BALAMT if present."""
        match = re.search(r'<BALAMT>([^<\n\r]*)', content)
        if match:
            try:
                return float(match.group(1).strip())
            except ValueError:
                pass
        return None
