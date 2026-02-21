"""Tests for QFX SGML parser using synthetic test data."""

from pathlib import Path

import pytest

from src.parsers.qfx_sgml import QfxSgmlParser


def _write_sgml_qfx(
    tmp_path: Path,
    account_id: str = "wf-checking",
    acctid: str = "1234567890",
    accttype: str = "CHECKING",
    txns_block: str = "",
    balance: str = "",
    filename: str = "test.qfx",
) -> Path:
    """Write a minimal SGML QFX file."""
    content = f"""OFXHEADER:100
DATA:OFXSGML
VERSION:102
<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0<SEVERITY>INFO</STATUS>
<DTSERVER>20260101
<LANGUAGE>ENG
</SONRS>
</SIGNONMSGSRSV1>
<BANKMSGSRSV1>
<STMTTRNRS>
<STMTRS>
<CURDEF>USD
<BANKACCTFROM>
<BANKID>111111111
<ACCTID>{acctid}
<ACCTTYPE>{accttype}
</BANKACCTFROM>
<BANKTRANLIST>
<DTSTART>20260101
<DTEND>20260131
{txns_block}
</BANKTRANLIST>
{balance}
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
"""
    f = tmp_path / filename
    f.write_text(content)
    return f


def _sgml_txn(trntype: str, date: str, amount: str, fitid: str, name: str) -> str:
    """Build a single SGML STMTTRN block."""
    return (
        f"<STMTTRN><TRNTYPE>{trntype}<DTPOSTED>{date}"
        f"<TRNAMT>{amount}<FITID>{fitid}<NAME>{name}\n"
    )


WF_CHECKING_TXNS = (
    _sgml_txn("DEBIT", "20260115", "-42.50", "202601151", "GROCERY STORE")
    + _sgml_txn("DEBIT", "20260116", "-2500.00", "202601161", "MORTGAGE PAYMENT")
    + _sgml_txn("CREDIT", "20260120", "5000.00", "202601201", "PAYROLL DEPOSIT")
    + _sgml_txn("DEBIT", "20260122", "-35.00", "202601221", "DOORDASH*ORDER 99999")
    + _sgml_txn("DEBIT", "20260125", "-15.99", "202601251", "Netflix.com")
)

GOLDEN1_TXNS = (
    _sgml_txn("CREDIT", "20260115", "800.00", "66886_84", "Payment Thank You")
    + _sgml_txn("INT", "20260115", "-55.17", "66886_84INT", "Interest Charge")
    + _sgml_txn("CREDIT", "20260215", "800.00", "66886_85", "Payment Thank You")
    + _sgml_txn("INT", "20260215", "-52.30", "66886_85INT", "Interest Charge")
)


class TestDetect:
    def test_detects_sgml_qfx(self, tmp_path):
        f = _write_sgml_qfx(tmp_path, txns_block=WF_CHECKING_TXNS)
        parser = QfxSgmlParser("wf-checking")
        assert parser.detect(f) is True

    def test_rejects_xml_file(self, tmp_path):
        f = tmp_path / "xml.qfx"
        f.write_text('<?xml version="1.0"?>\n<OFX></OFX>')
        parser = QfxSgmlParser("cap1-credit")
        assert parser.detect(f) is False

    def test_rejects_nonexistent(self):
        parser = QfxSgmlParser("wf-checking")
        assert parser.detect(Path("/nonexistent/file.qfx")) is False


class TestWellsFargoChecking:
    @pytest.fixture
    def txns(self, tmp_path):
        balance = "<LEDGERBAL><BALAMT>12345.67<DTASOF>20260131"
        f = _write_sgml_qfx(tmp_path, txns_block=WF_CHECKING_TXNS, balance=balance)
        parser = QfxSgmlParser("wf-checking")
        return parser.parse(f)

    def test_parses_transactions(self, txns):
        assert len(txns) == 5

    def test_all_dates_normalized(self, txns):
        for t in txns:
            assert len(t.date) == 10
            assert t.date[4] == "-" and t.date[7] == "-"

    def test_all_account_ids_correct(self, txns):
        for t in txns:
            assert t.account_id == "wf-checking"

    def test_amounts_are_signed(self, txns):
        has_negative = any(t.amount < 0 for t in txns)
        has_positive = any(t.amount > 0 for t in txns)
        assert has_negative
        assert has_positive

    def test_external_ids_present(self, txns):
        for t in txns:
            assert t.external_id is not None
            assert len(t.external_id) > 0

    def test_known_transaction(self, txns):
        first = txns[0]
        assert first.date == "2026-01-15"
        assert first.amount == -42.50
        assert "GROCERY" in first.raw_description
        assert first.external_id == "202601151"

    def test_has_balance(self, txns):
        assert any(t.balance is not None for t in txns)

    def test_descriptions_not_empty(self, txns):
        for t in txns:
            assert len(t.raw_description) > 0


class TestTruncatedFile:
    """B4: Last transaction without closing tag or BANKTRANLIST end."""

    def test_last_txn_no_closing_tag(self, tmp_path):
        content = (
            "OFXHEADER:100\n"
            "<BANKTRANLIST>\n"
            "<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>20260115<TRNAMT>-50.00"
            "<FITID>F1<NAME>FIRST TXN\n"
            "<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>20260116<TRNAMT>-25.00"
            "<FITID>F2<NAME>LAST TXN"
        )
        f = tmp_path / "truncated.qfx"
        f.write_text(content)
        parser = QfxSgmlParser("wf-checking")
        txns = parser.parse(f)
        assert len(txns) == 2
        assert txns[0].raw_description == "FIRST TXN"
        assert txns[1].raw_description == "LAST TXN"
        assert txns[1].amount == -25.00

    def test_single_txn_no_closing(self, tmp_path):
        content = (
            "OFXHEADER:100\n"
            "<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>20260120<TRNAMT>-10.00"
            "<FITID>F1<NAME>ONLY TXN"
        )
        f = tmp_path / "single.qfx"
        f.write_text(content)
        parser = QfxSgmlParser("wf-checking")
        txns = parser.parse(f)
        assert len(txns) == 1
        assert txns[0].raw_description == "ONLY TXN"


class TestWellsFargoSavings:
    def test_parses_and_routes(self, tmp_path):
        txn = _sgml_txn("CREDIT", "20260115", "50.00", "S1", "TRANSFER FROM CHECKING")
        f = _write_sgml_qfx(tmp_path, acctid="0987654321", txns_block=txn)
        parser = QfxSgmlParser("wf-savings")
        txns = parser.parse(f)
        assert len(txns) == 1
        assert txns[0].account_id == "wf-savings"


class TestGolden1:
    @pytest.fixture
    def txns(self, tmp_path):
        balance = "<LEDGERBAL><BALAMT>-15980.39<DTASOF>20260228"
        f = _write_sgml_qfx(
            tmp_path, acctid="9999999=1", accttype="CREDITLINE",
            txns_block=GOLDEN1_TXNS, balance=balance,
        )
        parser = QfxSgmlParser("golden1-auto")
        return parser.parse(f)

    def test_parses_transactions(self, txns):
        assert len(txns) == 4

    def test_paired_entries(self, txns):
        """Each loan payment generates CREDIT + INT entries."""
        credit_txns = [t for t in txns if t.txn_type == "CREDIT"]
        int_txns = [t for t in txns if t.txn_type == "INT"]
        assert len(credit_txns) == 2
        assert len(int_txns) == 2

    def test_int_entries_have_int_suffix(self, txns):
        int_txns = [t for t in txns if t.txn_type == "INT"]
        for t in int_txns:
            assert t.external_id.endswith("INT")

    def test_payment_and_interest_same_date(self, txns):
        credit_txns = [t for t in txns if t.txn_type == "CREDIT"]
        int_txns = [t for t in txns if t.txn_type == "INT"]
        credit_dates = {t.date for t in credit_txns}
        int_dates = {t.date for t in int_txns}
        assert credit_dates == int_dates

    def test_different_external_ids(self, txns):
        ids = [t.external_id for t in txns]
        assert len(ids) == len(set(ids)), "All FITIDs should be unique"

    def test_negative_balance(self, txns):
        assert any(t.balance is not None and t.balance < 0 for t in txns)

    def test_account_id_correct(self, txns):
        for t in txns:
            assert t.account_id == "golden1-auto"

    def test_known_first_payment(self, txns):
        credit_txns = sorted(
            [t for t in txns if t.txn_type == "CREDIT"],
            key=lambda t: t.date,
        )
        first = credit_txns[0]
        assert first.amount == 800.00
        assert first.external_id == "66886_84"
        assert "Payment" in first.raw_description
