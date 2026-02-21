"""Tests for transfer detection module."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.categorize.transfer_detect import detect_transfer, detect_transfer_by_txn_type


def _make_config(transfers=None, rules=None):
    """Create a mock config with test transfer patterns."""
    config = MagicMock()

    if transfers is None:
        transfers = [
            {"pattern": "CAPITAL ONE MOBILE PMT",
             "from_account": "primary-checking", "to_account": "primary-credit",
             "type": "cc-payment"},
            {"pattern": "AMERICAN EXPRESS ACH PMT",
             "from_account": "primary-checking", "to_account": "secondary-credit",
             "type": "cc-payment"},
            {"pattern": "TO SAVINGS TRANSFER",
             "from_account": "primary-checking", "to_account": "primary-savings",
             "type": "savings-transfer"},
            {"pattern": "ONLINE TRANSFER REF",
             "from_account": "primary-checking", "to_account": "primary-savings",
             "type": "savings-transfer"},
            {"pattern": "BANK ETRANSFER",
             "from_account": "primary-checking", "to_account": "auto-loan",
             "type": "loan-payment"},
            {"pattern": "Credit Union",
             "from_account": "primary-checking", "to_account": "auto-loan",
             "type": "loan-payment"},
            {"pattern": "IO AUTOPAY",
             "from_account": "mercury-checking", "to_account": "mercury-credit",
             "type": "internal-transfer"},
            {"pattern": "IO PAYMENT",
             "from_account": "mercury-checking", "to_account": "mercury-credit",
             "type": "internal-transfer"},
            {"pattern": "Capital One - Savings",
             "from_account": "mercury-checking", "to_account": "cap1-savings",
             "type": "savings-transfer"},
        ]

    if rules is None:
        rules = {"transfers": transfers}
    else:
        rules.setdefault("transfers", transfers)

    config.rules = rules
    return config


class TestTransferDetection:
    def test_credit_card_payment(self):
        result = detect_transfer(
            "CAPITAL ONE MOBILE PMT 240115", "primary-checking", _make_config()
        )
        assert result is not None
        assert result.from_account == "primary-checking"
        assert result.to_account == "primary-credit"
        assert result.transfer_type == "cc-payment"

    def test_second_credit_card_payment(self):
        result = detect_transfer(
            "AMERICAN EXPRESS ACH PMT", "primary-checking", _make_config()
        )
        assert result is not None
        assert result.to_account == "secondary-credit"
        assert result.transfer_type == "cc-payment"

    def test_savings_transfer(self):
        result = detect_transfer(
            "TO SAVINGS TRANSFER", "primary-checking", _make_config()
        )
        assert result is not None
        assert result.to_account == "primary-savings"
        assert result.transfer_type == "savings-transfer"

    def test_online_transfer_ref(self):
        result = detect_transfer(
            "ONLINE TRANSFER REF #12345", "primary-checking", _make_config()
        )
        assert result is not None
        assert result.transfer_type == "savings-transfer"

    def test_loan_payment(self):
        result = detect_transfer(
            "BANK ETRANSFER", "primary-checking", _make_config()
        )
        assert result is not None
        assert result.to_account == "auto-loan"
        assert result.transfer_type == "loan-payment"

    def test_credit_union_loan_payment(self):
        result = detect_transfer(
            "Credit Union PAYMENT", "primary-checking", _make_config()
        )
        assert result is not None
        assert result.transfer_type == "loan-payment"

    def test_mercury_io_autopay(self):
        result = detect_transfer(
            "IO AUTOPAY", "mercury-checking", _make_config()
        )
        assert result is not None
        assert result.from_account == "mercury-checking"
        assert result.to_account == "mercury-credit"
        assert result.transfer_type == "internal-transfer"

    def test_mercury_io_payment(self):
        result = detect_transfer(
            "IO PAYMENT", "mercury-checking", _make_config()
        )
        assert result is not None
        assert result.transfer_type == "internal-transfer"

    def test_savings_from_business(self):
        result = detect_transfer(
            "Capital One - Savings XFER", "mercury-checking", _make_config()
        )
        assert result is not None
        assert result.to_account == "cap1-savings"
        assert result.transfer_type == "savings-transfer"

    def test_no_match_normal_merchant(self):
        result = detect_transfer(
            "PHILZ COFFEE SF CA", "primary-checking", _make_config()
        )
        assert result is None

    def test_no_match_wrong_account(self):
        """IO AUTOPAY only valid from mercury-checking, not primary-checking."""
        result = detect_transfer(
            "IO AUTOPAY", "primary-checking", _make_config()
        )
        assert result is None

    def test_case_insensitive(self):
        result = detect_transfer(
            "capital one mobile pmt", "primary-checking", _make_config()
        )
        assert result is not None

    def test_io_autopay_from_credit_side(self):
        """mercury-credit is the to_account, should still match."""
        result = detect_transfer(
            "IO AUTOPAY", "mercury-credit", _make_config()
        )
        assert result is not None


def _make_txn_type_config(accounts=None, name_map=None):
    """Create a mock config for txn_type detection tests."""
    config = MagicMock()

    if accounts is None:
        accounts = [
            {"id": "primary-checking", "name": "Primary Checking", "account_type": "checking"},
            {"id": "primary-credit", "name": "Capital One", "account_type": "credit"},
            {"id": "primary-savings", "name": "Joint Savings", "account_type": "savings"},
            {"id": "auto-loan", "name": "Auto Loan", "account_type": "loan"},
            {"id": "secondary-credit", "name": "Amex Blue", "account_type": "credit"},
        ]

    config.accounts = accounts

    if name_map is None:
        name_map = {}
        for acct in accounts:
            acct_id = acct.get("id", "")
            for key in ("name", "budget_app_name"):
                val = acct.get(key, "")
                if val:
                    name_map[val.upper()] = acct_id
    config.transfer_name_map = name_map

    def _account_by_id(aid):
        for a in accounts:
            if a["id"] == aid:
                return a
        return None

    config.account_by_id = _account_by_id
    return config


class TestTransferByTxnType:
    def test_basic_detection(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Capital One", -2700.0, "primary-checking", config
        )
        assert result is not None
        assert result.from_account == "primary-checking"
        assert result.to_account == "primary-credit"
        assert result.transfer_type == "cc-payment"

    def test_case_insensitive(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "transfer : capital one", -500.0, "primary-checking", config
        )
        assert result is not None
        assert result.to_account == "primary-credit"

    def test_no_match_without_prefix(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "CAPITAL ONE MOBILE PMT", -500.0, "primary-checking", config
        )
        assert result is None

    def test_no_match_wrong_type(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "DEBIT", "Transfer : Capital One", -500.0, "primary-checking", config
        )
        assert result is None

    def test_no_type_with_transfer_prefix(self):
        """Transfer : prefix alone is enough — txn_type=None is OK (e.g. budget app imports)."""
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            None, "Transfer : Capital One", -500.0, "primary-checking", config
        )
        assert result is not None
        assert result.from_account == "primary-checking"
        assert result.to_account == "primary-credit"
        assert result.transfer_type == "cc-payment"

    def test_same_account_skipped(self):
        """Self-referential transfers should be skipped."""
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Primary Checking", -100.0, "primary-checking", config
        )
        assert result is None

    def test_infers_cc_payment(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Capital One", -2700.0, "primary-checking", config
        )
        assert result is not None
        assert result.transfer_type == "cc-payment"

    def test_infers_savings_transfer(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Joint Savings", -75.0, "primary-checking", config
        )
        assert result is not None
        assert result.transfer_type == "savings-transfer"

    def test_infers_loan_payment(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Auto Loan", -800.0, "primary-checking", config
        )
        assert result is not None
        assert result.transfer_type == "loan-payment"

    def test_outflow_direction(self):
        """Negative amount means money leaves the current account."""
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Capital One", -2700.0, "primary-checking", config
        )
        assert result is not None
        assert result.from_account == "primary-checking"
        assert result.to_account == "primary-credit"

    def test_inflow_direction(self):
        """Positive amount means money enters the current account."""
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Primary Checking", 2700.0, "primary-credit", config
        )
        assert result is not None
        assert result.from_account == "primary-checking"
        assert result.to_account == "primary-credit"

    def test_unknown_name(self):
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Unknown Account", -100.0, "primary-checking", config
        )
        assert result is None

    def test_substring_match(self):
        """Account name '1234 Joint Savings' matched by 'Joint Savings'."""
        accounts = [
            {"id": "primary-checking", "name": "Primary Checking", "account_type": "checking"},
            {"id": "joint-savings", "name": "1234 Joint Savings", "account_type": "savings"},
        ]
        config = _make_txn_type_config(accounts=accounts)
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer : Joint Savings", -75.0, "primary-checking", config
        )
        assert result is not None
        assert result.to_account == "joint-savings"
        assert result.transfer_type == "savings-transfer"

    def test_colon_without_space(self):
        """Handle 'Transfer :Name' without space after colon."""
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "TRANSFER", "Transfer :Capital One", -500.0, "primary-checking", config
        )
        assert result is not None
        assert result.to_account == "primary-credit"

    def test_non_transfer_txn_type_rejected(self):
        """Non-TRANSFER txn_type (e.g. DEBIT) should not match even with Transfer : prefix."""
        config = _make_txn_type_config()
        result = detect_transfer_by_txn_type(
            "DEBIT", "Transfer : Capital One", -500.0, "primary-checking", config
        )
        assert result is None

    def test_transfer_alias_match(self):
        """transfer_aliases in account config should resolve via name_map."""
        accounts = [
            {"id": "primary-checking", "name": "Primary Checking", "account_type": "checking"},
            {"id": "wf-savings", "name": "Joint Savings", "account_type": "savings",
             "transfer_aliases": ["9999 Vacation"]},
        ]
        # Build name_map including aliases (mirrors Config.transfer_name_map)
        name_map = {}
        for acct in accounts:
            for key in ("name", "budget_app_name"):
                val = acct.get(key, "")
                if val:
                    name_map[val.upper()] = acct["id"]
            for alias in acct.get("transfer_aliases", []):
                if alias:
                    name_map[alias.upper()] = acct["id"]
        config = _make_txn_type_config(accounts=accounts, name_map=name_map)
        result = detect_transfer_by_txn_type(
            None, "Transfer : 9999 Vacation", -208.50, "primary-checking", config
        )
        assert result is not None
        assert result.to_account == "wf-savings"
        assert result.transfer_type == "savings-transfer"
