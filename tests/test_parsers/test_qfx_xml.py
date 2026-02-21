"""Tests for QFX XML parser using synthetic test data."""

from pathlib import Path

import pytest

from src.parsers.qfx_xml import QfxXmlParser


def _xml_txn(trntype: str, date: str, amount: str, fitid: str, name: str,
             memo: str = "", refnum: str = "") -> str:
    """Build a single XML STMTTRN element."""
    parts = [
        f"<STMTTRN>",
        f"<TRNTYPE>{trntype}</TRNTYPE>",
        f"<DTPOSTED>{date}120000[0:GMT]</DTPOSTED>",
        f"<TRNAMT>{amount}</TRNAMT>",
        f"<FITID>{fitid}</FITID>",
    ]
    if refnum:
        parts.append(f"<REFNUM>{refnum}</REFNUM>")
    parts.append(f"<NAME>{name}</NAME>")
    if memo:
        parts.append(f"<MEMO>{memo}</MEMO>")
    parts.append("</STMTTRN>")
    return "\n".join(parts) + "\n"


def _write_xml_credit(
    tmp_path: Path,
    acctid: str = "1234",
    txns_block: str = "",
    filename: str = "credit.qfx",
) -> Path:
    """Write a minimal XML QFX file for credit card (CREDITCARDMSGSRSV1)."""
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<?OFX OFXHEADER="200" VERSION="220"?>
<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<DTSERVER>20260101120000</DTSERVER>
<LANGUAGE>ENG</LANGUAGE>
</SONRS>
</SIGNONMSGSRSV1>
<CREDITCARDMSGSRSV1>
<CCSTMTTRNRS>
<CCSTMTRS>
<CURDEF>USD</CURDEF>
<CCACCTFROM>
<ACCTID>{acctid}</ACCTID>
</CCACCTFROM>
<BANKTRANLIST>
<DTSTART>20260101120000</DTSTART>
<DTEND>20260131120000</DTEND>
{txns_block}
</BANKTRANLIST>
</CCSTMTRS>
</CCSTMTTRNRS>
</CREDITCARDMSGSRSV1>
</OFX>
"""
    f = tmp_path / filename
    f.write_text(content)
    return f


def _write_xml_savings(
    tmp_path: Path,
    acctid: str = "5678",
    txns_block: str = "",
    filename: str = "savings.qfx",
) -> Path:
    """Write a minimal XML QFX file for savings (BANKMSGSRSV1)."""
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<?OFX OFXHEADER="200" VERSION="220"?>
<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<DTSERVER>20260101120000</DTSERVER>
<LANGUAGE>ENG</LANGUAGE>
</SONRS>
</SIGNONMSGSRSV1>
<BANKMSGSRSV1>
<STMTTRNRS>
<STMTRS>
<CURDEF>USD</CURDEF>
<BANKACCTFROM>
<BANKID>000000000</BANKID>
<ACCTID>{acctid}</ACCTID>
<ACCTTYPE>SAVINGS</ACCTTYPE>
</BANKACCTFROM>
<BANKTRANLIST>
<DTSTART>20260101120000</DTSTART>
<DTEND>20260131120000</DTEND>
{txns_block}
</BANKTRANLIST>
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
"""
    f = tmp_path / filename
    f.write_text(content)
    return f


# Pre-built transaction blocks
CAPONE_CREDIT_TXNS = (
    _xml_txn("DEBIT", "20260115", "-45.18", "202601151505320", "DOORDASH*ORDER 12345")
    + _xml_txn("DEBIT", "20260116", "-15.99", "202601161505321", "Netflix.com")
    + _xml_txn("DEBIT", "20260118", "-85.00", "202601181505322", "COSTCO WHSE #1234")
    + _xml_txn("CREDIT", "20260120", "500.00", "202601201505323", "CAPITAL ONE MOBILE PMT")
    + _xml_txn("DEBIT", "20260122", "-25.00", "202601221505324", "AMAZON.COM*AMZN MKTP")
)

CAPONE_SAVINGS_TXNS = (
    _xml_txn("CREDIT", "20260115", "5.25", "S001", "Interest Paid", memo="Interest Paid")
    + _xml_txn("CREDIT", "20260120", "1000.00", "S002", "TRANSFER FROM CHECKING")
)

AMEX_TXNS = (
    _xml_txn("DEBIT", "20260110", "-35.00", "REF001", "DOORDASH*ORDER 99999",
             refnum="REF001")
    + _xml_txn("DEBIT", "20260112", "-42.50", "REF002", "INSTACART",
             refnum="REF002")
    + _xml_txn("DEBIT", "20260115", "-5.50", "REF003", "STARBUCKS STORE 12345",
             refnum="REF003")
    + _xml_txn("CREDIT", "20260120", "200.00", "REF004", "PAYMENT RECEIVED",
             refnum="REF004")
)


class TestDetect:
    def test_detects_xml_credit(self, tmp_path):
        f = _write_xml_credit(tmp_path, txns_block=CAPONE_CREDIT_TXNS)
        parser = QfxXmlParser("cap1-credit")
        assert parser.detect(f) is True

    def test_detects_xml_savings(self, tmp_path):
        f = _write_xml_savings(tmp_path, txns_block=CAPONE_SAVINGS_TXNS)
        parser = QfxXmlParser("cap1-savings")
        assert parser.detect(f) is True

    def test_rejects_sgml_file(self, tmp_path):
        f = tmp_path / "sgml.qfx"
        f.write_text("OFXHEADER:100\nDATA:OFXSGML\n<OFX></OFX>")
        parser = QfxXmlParser("wf-checking")
        assert parser.detect(f) is False

    def test_rejects_nonexistent(self):
        parser = QfxXmlParser("cap1-credit")
        assert parser.detect(Path("/nonexistent/file.qfx")) is False


class TestCapOneCredit:
    @pytest.fixture
    def txns(self, tmp_path):
        f = _write_xml_credit(tmp_path, txns_block=CAPONE_CREDIT_TXNS)
        parser = QfxXmlParser("cap1-credit")
        return parser.parse(f)

    def test_parses_transactions(self, txns):
        assert len(txns) == 5

    def test_dates_normalized(self, txns):
        for t in txns:
            assert len(t.date) == 10
            assert t.date[4] == "-"

    def test_account_id(self, txns):
        for t in txns:
            assert t.account_id == "cap1-credit"

    def test_has_debits_and_credits(self, txns):
        assert any(t.amount < 0 for t in txns)
        assert any(t.amount > 0 for t in txns)

    def test_external_ids_unique(self, txns):
        ids = [t.external_id for t in txns if t.external_id]
        assert len(ids) == len(set(ids))

    def test_known_first_transaction(self, txns):
        first = txns[0]
        assert first.date == "2026-01-15"
        assert first.amount == -45.18
        assert "DOORDASH" in first.raw_description
        assert first.external_id == "202601151505320"

    def test_descriptions_not_empty(self, txns):
        for t in txns:
            assert len(t.raw_description) > 0


class TestCapOneSavings:
    @pytest.fixture
    def txns(self, tmp_path):
        f = _write_xml_savings(tmp_path, txns_block=CAPONE_SAVINGS_TXNS)
        parser = QfxXmlParser("cap1-savings")
        return parser.parse(f)

    def test_parses_transactions(self, txns):
        assert len(txns) == 2

    def test_account_id(self, txns):
        for t in txns:
            assert t.account_id == "cap1-savings"

    def test_has_interest_payments(self, txns):
        interest = [t for t in txns if "Interest" in (t.memo or "")]
        assert len(interest) == 1

    def test_uses_bankmsgsrsv1_path(self, txns):
        """Savings uses BANKMSGSRSV1, not CREDITCARDMSGSRSV1."""
        assert len(txns) > 0


class TestAmex:
    @pytest.fixture
    def txns(self, tmp_path):
        f = _write_xml_credit(tmp_path, acctid="9876", txns_block=AMEX_TXNS)
        parser = QfxXmlParser("amex-blue")
        return parser.parse(f)

    def test_parses_transactions(self, txns):
        assert len(txns) == 4

    def test_account_id(self, txns):
        for t in txns:
            assert t.account_id == "amex-blue"

    def test_dates_in_2026(self, txns):
        for t in txns:
            assert t.date.startswith("2026")

    def test_has_doordash_transactions(self, txns):
        doordash = [t for t in txns if "DOORDASH" in t.raw_description.upper()]
        assert len(doordash) == 1

    def test_external_ids_present(self, txns):
        for t in txns:
            assert t.external_id is not None


class TestMultiFile:
    def test_two_files_no_overlap(self, tmp_path):
        """Two different files should have different FITIDs."""
        txns1 = _xml_txn("DEBIT", "20260115", "-10.00", "FILE1_001", "MERCHANT A")
        txns2 = _xml_txn("DEBIT", "20260215", "-20.00", "FILE2_001", "MERCHANT B")

        f1 = _write_xml_credit(tmp_path, txns_block=txns1, filename="file1.qfx")
        f2 = _write_xml_credit(tmp_path, txns_block=txns2, filename="file2.qfx")

        parser = QfxXmlParser("cap1-credit")
        parsed1 = parser.parse(f1)
        parsed2 = parser.parse(f2)

        ids_1 = {t.external_id for t in parsed1}
        ids_2 = {t.external_id for t in parsed2}
        assert len(ids_1 & ids_2) == 0
