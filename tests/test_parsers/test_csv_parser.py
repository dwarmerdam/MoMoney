"""Tests for Mercury CSV parser using synthetic test data."""

from pathlib import Path

import pytest

from src.parsers.csv_parser import MercuryCsvParser

# Full Mercury CSV header matching real export format
MERCURY_HEADER = (
    "Date (UTC),Description,Amount,Status,Source Account,"
    "Bank Description,Reference,Note,Last Four Digits,Name On Card,"
    "Mercury Category,Category,GL Code,Timestamp,Original Currency,"
    "Check Number,Tags,Cardholder Email,Tracking ID\n"
)


def _write_csv(tmp_path: Path, rows: str, filename: str = "mercury.csv") -> Path:
    """Write a Mercury CSV file with the standard header and given rows."""
    f = tmp_path / filename
    f.write_text(MERCURY_HEADER + rows)
    return f


class TestDetect:
    def test_detects_mercury_csv(self, tmp_path):
        f = _write_csv(tmp_path, "")
        parser = MercuryCsvParser()
        assert parser.detect(f) is True

    def test_rejects_non_mercury_csv(self, tmp_path):
        f = tmp_path / "other.csv"
        f.write_text("Date,Amount,Description\n2026-01-15,-10.00,Test\n")
        parser = MercuryCsvParser()
        assert parser.detect(f) is False

    def test_rejects_qfx(self, tmp_path):
        f = tmp_path / "test.qfx"
        f.write_text("OFXHEADER:100\n<OFX></OFX>\n")
        parser = MercuryCsvParser()
        assert parser.detect(f) is False

    def test_rejects_nonexistent(self):
        parser = MercuryCsvParser()
        assert parser.detect(Path("/nonexistent/file.csv")) is False


class TestParse:
    @pytest.fixture
    def csv_file(self, tmp_path):
        """Create a synthetic Mercury CSV with multiple accounts and transaction types."""
        rows = (
            # Checking transactions
            "01-15-2026,Amazon,-25.00,Sent,Mercury Checking ••1234,"
            "AMAZON.COM,,,,,Expense,,,01-15-2026 01:43:00,USD,,,,\n"
            "01-15-2026,Amazon,-25.00,Sent,Mercury Checking ••1234,"
            "AMAZON.COM,,,,,Expense,,,01-15-2026 02:02:00,USD,,,,\n"
            "01-15-2026,Amazon,-25.00,Sent,Mercury Checking ••1234,"
            "AMAZON.COM,,,,,Expense,,,01-15-2026 02:08:00,USD,,,,\n"
            "01-15-2026,Amazon,-25.00,Sent,Mercury Checking ••1234,"
            "AMAZON.COM,,,,,Expense,,,01-15-2026 02:10:00,USD,,,,\n"
            "01-20-2026,Anthropic,-20.00,Sent,Mercury Checking ••1234,"
            "ANTHROPIC API,,,,,Expense,,,01-20-2026 09:00:00,USD,,,,\n"
            "01-22-2026,IO AUTOPAY,-150.00,Sent,Mercury Checking ••1234,"
            "IO AUTOPAY,,,,,Transfer,,,01-22-2026 08:00:00,USD,,,,\n"
            # Credit transactions
            "01-10-2026,DoorDash,-35.00,Sent,Mercury Credit ••5678,"
            "DOORDASH*ORDER,,,,,Expense,,,01-10-2026 19:30:00,USD,,,,\n"
            "01-22-2026,IO AUTOPAY,150.00,Sent,Mercury Credit ••5678,"
            "IO AUTOPAY,,,,,Transfer,,,01-22-2026 08:00:01,USD,,,,\n"
            # Savings transaction
            "01-25-2026,Interest Payment,5.00,Sent,Mercury Savings ••9012,"
            "INTEREST PAYMENT,,,,,Income,,,01-25-2026 00:00:00,USD,,,,\n"
            # Failed transaction (should be skipped)
            "01-28-2026,Bad Transfer,-500.00,Failed,Mercury Checking ••1234,"
            "WIRE TRANSFER,,,,,Expense,,,01-28-2026 12:00:00,USD,,,,\n"
            # Cancelled transaction (should be skipped)
            "01-29-2026,Cancelled Txn,-100.00,Cancelled,Mercury Checking ••1234,"
            "TEST,,,,,Expense,,,01-29-2026 12:00:00,USD,,,,\n"
        )
        return _write_csv(tmp_path, rows)

    @pytest.fixture
    def parser(self):
        return MercuryCsvParser(account_routing={
            "Mercury Checking \u2022\u20221234": "mercury-checking",
            "Mercury Credit \u2022\u20225678": "mercury-credit",
            "Mercury Savings \u2022\u20229012": "mercury-savings",
        })

    @pytest.fixture
    def txns(self, parser, csv_file):
        return parser.parse(csv_file)

    def test_parses_transactions(self, txns):
        assert len(txns) > 0

    def test_filters_failed_and_cancelled(self, txns):
        """Failed and Cancelled status rows should be excluded."""
        # 11 total rows - 2 skipped (Failed + Cancelled) = 9
        assert len(txns) == 9

    def test_routes_to_multiple_accounts(self, txns):
        accounts = {t.account_id for t in txns}
        assert accounts == {"mercury-checking", "mercury-credit", "mercury-savings"}

    def test_mercury_credit_routing(self, txns):
        credit_txns = [t for t in txns if t.account_id == "mercury-credit"]
        assert len(credit_txns) == 2

    def test_mercury_checking_routing(self, txns):
        checking_txns = [t for t in txns if t.account_id == "mercury-checking"]
        assert len(checking_txns) == 6

    def test_mercury_savings_routing(self, txns):
        savings_txns = [t for t in txns if t.account_id == "mercury-savings"]
        assert len(savings_txns) == 1

    def test_dates_normalized(self, txns):
        for t in txns:
            assert len(t.date) == 10
            assert t.date[4] == "-" and t.date[7] == "-"
            year = int(t.date[:4])
            assert 2020 <= year <= 2030

    def test_amounts_signed(self, txns):
        has_negative = any(t.amount < 0 for t in txns)
        has_positive = any(t.amount > 0 for t in txns)
        assert has_negative
        assert has_positive

    def test_timestamps_as_external_ids(self, txns):
        for t in txns:
            assert t.external_id is not None, "Timestamp should be external_id"
            assert len(t.external_id) > 10

    def test_four_amazon_same_amount(self, txns):
        """Mercury file has 4x $25 Amazon charges on same day."""
        amazon_25 = [
            t for t in txns
            if t.amount == -25.00 and "Amazon" in t.raw_description
        ]
        assert len(amazon_25) == 4
        dates = {t.date for t in amazon_25}
        assert len(dates) == 1
        ext_ids = {t.external_id for t in amazon_25}
        assert len(ext_ids) == 4, "Same-amount same-day txns must have unique timestamps"

    def test_descriptions_not_empty(self, txns):
        for t in txns:
            assert len(t.raw_description) > 0

    def test_no_routing_returns_empty(self, csv_file):
        """Parser without routing returns no transactions."""
        parser = MercuryCsvParser()
        txns = parser.parse(csv_file)
        assert len(txns) == 0


class TestStatusFiltering:
    def test_skip_failed_status(self, tmp_path):
        rows = (
            "01-30-2026,Good Txn,-10.00,Sent,Mercury Checking ••1234,"
            "TEST,,,,,,,,01-30-2026 01:00:00,USD,,,,\n"
            "01-30-2026,Bad Txn,-20.00,Failed,Mercury Checking ••1234,"
            "TEST,,,,,,,,01-30-2026 02:00:00,USD,,,,\n"
        )
        f = _write_csv(tmp_path, rows)
        parser = MercuryCsvParser(account_routing={
            "Mercury Checking \u2022\u20221234": "mercury-checking",
        })
        txns = parser.parse(f)
        assert len(txns) == 1
        assert txns[0].raw_description == "Good Txn"

    def test_skip_cancelled_status(self, tmp_path):
        rows = (
            "01-30-2026,Good Txn,-10.00,Sent,Mercury Checking ••1234,"
            "TEST,,,,,,,,01-30-2026 01:00:00,USD,,,,\n"
            "01-30-2026,Cancelled Txn,-20.00,Cancelled,Mercury Checking ••1234,"
            "TEST,,,,,,,,01-30-2026 02:00:00,USD,,,,\n"
        )
        f = _write_csv(tmp_path, rows)
        parser = MercuryCsvParser(account_routing={
            "Mercury Checking \u2022\u20221234": "mercury-checking",
        })
        txns = parser.parse(f)
        assert len(txns) == 1


class TestDateParsing:
    def test_mm_dd_yyyy(self):
        assert MercuryCsvParser._parse_date("01-30-2026") == "2026-01-30"

    def test_preserves_yyyy_mm_dd(self):
        assert MercuryCsvParser._parse_date("2026-01-30") == "2026-01-30"
