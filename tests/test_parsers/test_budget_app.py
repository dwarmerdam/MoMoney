"""Tests for budget app Register CSV parser using synthetic test data."""

from pathlib import Path

import pytest

from src.parsers.budget_app import BudgetAppCsvParser


def _write_budget_app_csv(tmp_path: Path, rows: str, filename: str = "register.csv") -> Path:
    """Write a budget app CSV file with BOM and standard header."""
    header = (
        '"Account","Flag","Date","Payee","Category Group/Category",'
        '"Category Group","Category","Memo","Outflow","Inflow","Cleared"\n'
    )
    f = tmp_path / filename
    f.write_text("\ufeff" + header + rows)
    return f


class TestDetect:
    def test_detects_budget_app_csv(self, tmp_path):
        f = _write_budget_app_csv(tmp_path, "")
        parser = BudgetAppCsvParser()
        assert parser.detect(f) is True

    def test_rejects_mercury_csv(self, tmp_path):
        f = tmp_path / "mercury.csv"
        f.write_text(
            "Date (UTC),Description,Amount,Status,Source Account,"
            "Bank Description,Mercury Category,Timestamp\n"
        )
        parser = BudgetAppCsvParser()
        assert parser.detect(f) is False

    def test_rejects_nonexistent(self):
        parser = BudgetAppCsvParser()
        assert parser.detect(Path("/nonexistent/file.csv")) is False


class TestParse:
    @pytest.fixture
    def csv_file(self, tmp_path):
        """Create a synthetic budget app register CSV with various transaction types."""
        rows = (
            # Standard outflow
            '"My Checking","","01/29/2026","Autopay","Monthly Bills: Internet",'
            '"Monthly Bills","Internet","","$209.71","$0.00","Cleared"\n'
            # Inflow (cashback)
            '"My Checking","","01/29/2026","Credit Cashback","Income: Cashback",'
            '"Income","Cashback","rewards","$0.00","$3.15","Cleared"\n'
            # Large amount with commas
            '"My Checking","","01/15/2026","Payroll Deposit","Income: Earnings",'
            '"Income","Earnings","","$0.00","$5,477.40","Cleared"\n'
            # Credit card transaction
            '"Primary Credit","","01/20/2026","DoorDash","Monthly Needs: Meal Delivery",'
            '"Monthly Needs","Meal Delivery","order 12345","$35.00","$0.00","Cleared"\n'
            # Transfer
            '"My Checking","","01/22/2026","Transfer : Primary Credit","",'
            '"","","cc payment","$500.00","$0.00","Cleared"\n'
            # Savings account
            '"Primary Savings","","01/25/2026","Interest Payment","Income: Interest",'
            '"Income","Interest","","$0.00","$2.50","Cleared"\n'
            # Business checking
            '"Business Checking","","01/20/2026","Anthropic","Business: SaaS",'
            '"Business","SaaS","API charges","$20.00","$0.00","Cleared"\n'
            # Amex
            '"Blue Cash","","01/18/2026","Netflix.com","Monthly Bills: Netflix",'
            '"Monthly Bills","Netflix","","$15.99","$0.00","Cleared"\n'
        )
        return _write_budget_app_csv(tmp_path, rows)

    @pytest.fixture
    def parser(self):
        return BudgetAppCsvParser(account_routing={
            "My Checking": "wf-checking",
            "Primary Credit": "cap1-credit",
            "Primary Savings": "wf-savings",
            "Business Checking": "mercury-checking",
            "Blue Cash": "amex-blue",
            "Business Savings": "mercury-savings",
            "Auto Loan": "golden1-auto",
            "Savings Plus": "cap1-savings",
        })

    @pytest.fixture
    def txns(self, parser, csv_file):
        return parser.parse(csv_file)

    def test_parses_all_transactions(self, txns):
        assert len(txns) == 8

    def test_routes_to_known_accounts(self, txns):
        accounts = {t.account_id for t in txns}
        expected = {
            "wf-checking", "cap1-credit", "mercury-checking",
            "amex-blue", "wf-savings",
        }
        assert accounts == expected

    def test_dates_normalized(self, txns):
        for t in txns:
            assert len(t.date) == 10
            assert t.date[4] == "-" and t.date[7] == "-"

    def test_amounts_signed_correctly(self, txns):
        has_negative = any(t.amount < 0 for t in txns)
        has_positive = any(t.amount > 0 for t in txns)
        assert has_negative  # outflows
        assert has_positive  # inflows

    def test_first_transaction(self, txns):
        """First row: checking, Autopay, -$209.71."""
        first = txns[0]
        assert first.account_id == "wf-checking"
        assert first.amount == -209.71
        assert "Autopay" in first.raw_description
        assert first.date == "2026-01-29"

    def test_transfer_detection(self, txns):
        transfers = [t for t in txns if t.txn_type == "TRANSFER"]
        assert len(transfers) == 1
        assert transfers[0].raw_description == "Transfer : Primary Credit"

    def test_inflow_transaction(self, txns):
        """Second row: cashback, +$3.15."""
        second = txns[1]
        assert second.amount == 3.15

    def test_large_amount_with_commas(self, txns):
        """Payroll with $5,477.40 parsed correctly."""
        payroll = [t for t in txns if t.amount > 5000]
        assert len(payroll) == 1
        assert payroll[0].amount == 5477.40

    def test_descriptions_not_empty(self, txns):
        for t in txns:
            assert len(t.raw_description) > 0

    def test_no_routing_returns_empty(self, csv_file):
        """Parser without routing returns no transactions."""
        parser = BudgetAppCsvParser()
        txns = parser.parse(csv_file)
        assert len(txns) == 0


class TestAmountParsing:
    def test_outflow(self):
        assert BudgetAppCsvParser._parse_amount("$209.71", "$0.00") == -209.71

    def test_inflow(self):
        assert BudgetAppCsvParser._parse_amount("$0.00", "$3.15") == 3.15

    def test_large_amount_with_commas(self):
        assert BudgetAppCsvParser._parse_amount("$1,477.40", "$0.00") == -1477.40

    def test_zero(self):
        assert BudgetAppCsvParser._parse_amount("$0.00", "$0.00") == 0.0

    def test_empty_strings(self):
        assert BudgetAppCsvParser._parse_amount("", "") == 0.0


class TestDateParsing:
    def test_mm_dd_yyyy(self):
        assert BudgetAppCsvParser._parse_date("01/29/2026") == "2026-01-29"

    def test_single_digit_month(self):
        assert BudgetAppCsvParser._parse_date("1/5/2024") == "2024-01-05"


class TestTransferDetection:
    def test_standard_transfer(self):
        assert BudgetAppCsvParser._is_transfer("Transfer : Capital One") is True

    def test_not_transfer(self):
        assert BudgetAppCsvParser._is_transfer("Anthropic") is False

    def test_transfer_without_space(self):
        assert BudgetAppCsvParser._is_transfer("Transfer :Capital One") is True
