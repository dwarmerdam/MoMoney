"""QFX/OFX parser for XML format.

Handles:
- Capital One credit card (CREDITCARDMSGSRSV1 path, ACCTID=0217)
- Capital One savings (BANKMSGSRSV1 path, ACCTTYPE=SAVINGS)
- American Express credit card (CREDITCARDMSGSRSV1, FITID==REFNUM)

All have <?xml or <?OFX header and proper closing tags.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

from .base import BaseParser, RawTransaction, parse_ofx_date


class QfxXmlParser(BaseParser):
    """Parse OFX/QFX files in XML format."""

    def __init__(self, account_id: str):
        super().__init__()
        self.account_id = account_id

    def detect(self, file_path: Path) -> bool:
        """XML files have <?xml or <?OFX header."""
        try:
            with open(file_path, "r", errors="replace") as f:
                head = f.read(200)
            return ("<?xml" in head.lower() or "<?OFX" in head) and "<OFX>" in head
        except (OSError, UnicodeDecodeError):
            return False

    def parse(self, file_path: Path) -> list[RawTransaction]:
        content = self._read_and_clean(file_path)
        root = ET.fromstring(content)

        transactions: list[RawTransaction] = []
        self.skipped_count = 0  # Reset for each parse
        balance = self._extract_balance(root)

        # Find STMTTRN elements in either credit card or bank path
        for stmttrn in root.iter("STMTTRN"):
            txn = self._parse_element(stmttrn, balance)
            if txn is not None:
                transactions.append(txn)
            else:
                self.skipped_count += 1

        return transactions

    def _read_and_clean(self, file_path: Path) -> str:
        """Read file and strip OFX processing instructions that confuse ET."""
        with open(file_path, "r", errors="replace") as f:
            content = f.read()

        # Remove <?xml ... ?> and <?OFX ... ?> processing instructions
        content = re.sub(r'<\?xml[^?]*\?>', '', content)
        content = re.sub(r'<\?OFX[^?]*\?>', '', content)

        # Strip any leading whitespace/BOM
        content = content.strip().lstrip('\ufeff')

        # Ensure we have the OFX root
        if not content.startswith("<OFX>"):
            # Find <OFX> and trim anything before it
            idx = content.find("<OFX>")
            if idx >= 0:
                content = content[idx:]

        return content

    def _parse_element(
        self, elem: ET.Element, balance: float | None
    ) -> RawTransaction | None:
        """Extract fields from a STMTTRN XML element."""
        dtposted = self._text(elem, "DTPOSTED")
        trnamt = self._text(elem, "TRNAMT")

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
            raw_description=self._text(elem, "NAME") or self._text(elem, "MEMO") or "",
            account_id=self.account_id,
            memo=self._text(elem, "MEMO"),
            txn_type=self._text(elem, "TRNTYPE"),
            external_id=self._text(elem, "FITID"),
            check_num=self._text(elem, "CHECKNUM"),
            balance=balance,
        )

    def _extract_balance(self, root: ET.Element) -> float | None:
        """Extract LEDGERBAL > BALAMT if present."""
        ledgerbal = root.find(".//LEDGERBAL")
        if ledgerbal is not None:
            balamt = ledgerbal.find("BALAMT")
            if balamt is not None and balamt.text:
                try:
                    return float(balamt.text.strip())
                except ValueError:
                    pass
        return None

    @staticmethod
    def _text(parent: ET.Element, tag: str) -> str | None:
        el = parent.find(tag)
        if el is not None and el.text:
            return el.text.strip()
        return None
