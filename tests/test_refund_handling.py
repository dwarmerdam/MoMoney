"""Tests for refund transaction handling across all pipeline stages.

Refunds are transactions with positive amounts (inflows) that represent
returns, credits, or adjustments. The system handles refunds implicitly —
there are no refund-specific code paths. These tests verify that positive
amounts flow correctly through parsing, dedup, categorization, and allocation.

Key behaviors verified:
  - Parsers preserve positive amounts from bank data
  - Dedup treats refunds as distinct from original charges
  - Merchant matching works regardless of amount sign
  - Amount-based rules (defined with negative ranges) don't match refunds
  - Allocations preserve the positive amount for correct reporting
  - Mercury business logic applies to refunds the same as charges
"""

from pathlib import Path

import pytest

from src.categorize.amount_rules import match_amount_rule
from src.categorize.merchant_match import match_merchant_auto, match_merchant_high
from src.categorize.pipeline import (
    CategorizeResult,
    apply_categorization,
    categorize_pending,
    categorize_transaction,
)
from src.categorize.transfer_detect import detect_transfer
from src.config import Config
from src.database.models import Allocation, Import, Transaction
from src.database.repository import Repository
from src.parsers.base import (
    RawTransaction,
    compute_dedup_key,
    compute_import_hash,
)

MIGRATIONS_DIR = Path(__file__).parent.parent / "src" / "database" / "migrations"


@pytest.fixture
def config():
    from tests.conftest import FIXTURE_CONFIG_DIR
    return Config(FIXTURE_CONFIG_DIR)


@pytest.fixture
def repo():
    r = Repository(":memory:")
    r.apply_migrations(MIGRATIONS_DIR)
    yield r
    r.close()


@pytest.fixture
def imp(repo):
    return repo.insert_import(Import(file_name="test.qfx", file_hash="refund_hash1"))


def _txn(imp_id, **kw) -> Transaction:
    defaults = dict(
        account_id="wf-checking",
        date="2026-01-15",
        amount=-50.00,
        raw_description="SOME MERCHANT",
        import_id=imp_id,
        import_hash="rh1",
        dedup_key="rdk1",
    )
    defaults.update(kw)
    return Transaction(**defaults)


# ── Parser: positive amount preservation ───────────────────


class TestParserPositiveAmounts:
    """Parsers must preserve the sign banks provide — refunds are positive."""

    def test_qfx_sgml_positive_amount(self, tmp_path):
        """QFX SGML parser preserves positive TRNAMT (refund/credit)."""
        from src.parsers.qfx_sgml import QfxSgmlParser

        content = (
            "OFXHEADER:100\nDATA:OFXSGML\n"
            "<OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS>\n"
            "<BANKTRANLIST>\n"
            "<STMTTRN><TRNTYPE>CREDIT<DTPOSTED>20260115<TRNAMT>25.00"
            "<FITID>20260115001<NAME>STARBUCKS REFUND\n"
            "</BANKTRANLIST>\n"
            "</STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>\n"
        )
        f = tmp_path / "refund.qfx"
        f.write_text(content)

        parser = QfxSgmlParser(account_id="wf-checking")
        txns = parser.parse(f)

        assert len(txns) == 1
        assert txns[0].amount == 25.00  # Positive = refund
        assert txns[0].txn_type == "CREDIT"
        assert txns[0].raw_description == "STARBUCKS REFUND"

    def test_qfx_xml_positive_amount(self, tmp_path):
        """QFX XML parser preserves positive TRNAMT (refund/credit)."""
        from src.parsers.qfx_xml import QfxXmlParser

        content = (
            '<?xml version="1.0"?>\n'
            '<OFX><CREDITCARDMSGSRSV1><CCSTMTTRNRS><CCSTMTRS>\n'
            '<BANKTRANLIST>\n'
            '<STMTTRN><TRNTYPE>CREDIT</TRNTYPE>'
            '<DTPOSTED>20260115120000[0:GMT]</DTPOSTED>'
            '<TRNAMT>42.50</TRNAMT>'
            '<FITID>320260150001</FITID>'
            '<NAME>AMAZON REFUND</NAME></STMTTRN>\n'
            '</BANKTRANLIST>\n'
            '</CCSTMTRS></CCSTMTTRNRS></CREDITCARDMSGSRSV1></OFX>\n'
        )
        f = tmp_path / "refund.qfx"
        f.write_text(content)

        parser = QfxXmlParser(account_id="cap1-credit")
        txns = parser.parse(f)

        assert len(txns) == 1
        assert txns[0].amount == 42.50
        assert txns[0].txn_type == "CREDIT"

    def test_mercury_csv_positive_amount(self, tmp_path):
        """Mercury CSV preserves positive Amount for refunds."""
        from src.parsers.csv_parser import MercuryCsvParser

        header = (
            "Date (UTC),Description,Amount,Status,Source Account,"
            "Bank Description,Note,Timestamp,Original Currency,"
            "Check Number,Name On Card,Cardholder Email,"
            "Mercury Category,Reference,Tracking ID\n"
        )
        row = (
            '01-15-2026,CHEWY.COM REFUND,15.99,Sent,'
            'Mercury Checking xx1234,CHEWY.COM REFUND,,'
            '01-15-2026 10:30:00,USD,,,,Expense,,\n'
        )
        f = tmp_path / "refund.csv"
        f.write_text(header + row)

        parser = MercuryCsvParser(account_routing={
            "Mercury Checking xx1234": "mercury-checking",
        })
        txns = parser.parse(f)

        assert len(txns) == 1
        assert txns[0].amount == 15.99  # Positive = refund
        assert txns[0].account_id == "mercury-checking"

    def test_budget_app_csv_refund_as_inflow(self, tmp_path):
        """Budget app CSV: refund appears as Inflow, yielding positive amount."""
        from src.parsers.budget_app import BudgetAppCsvParser

        content = (
            '"Account","Flag","Date","Payee","Category Group/Category",'
            '"Category Group","Category","Memo","Outflow","Inflow","Cleared"\n'
            '"My Checking","","01/15/2026","COSTCO REFUND","","",'
            '"","Return for item","$0.00","$89.50","Cleared"\n'
        )
        f = tmp_path / "refund.csv"
        f.write_text("\ufeff" + content)

        parser = BudgetAppCsvParser(account_routing={"My Checking": "wf-checking"})
        txns = parser.parse(f)

        assert len(txns) == 1
        assert txns[0].amount == 89.50  # inflow - outflow = 89.50 - 0 = +89.50
        assert txns[0].raw_description == "COSTCO REFUND"
        assert txns[0].account_id == "wf-checking"


# ── Dedup: refunds vs charges ──────────────────────────────


class TestDedupRefundHandling:
    """Refunds must NOT be flagged as duplicates of the original charge."""

    def test_import_hash_differs_for_refund(self):
        """Tier 3: import_hash includes signed amount, so refund != charge."""
        charge_hash = compute_import_hash("cap1-credit", "2026-01-15", -42.50, "AMAZON PURCHASE")
        refund_hash = compute_import_hash("cap1-credit", "2026-01-15", 42.50, "AMAZON PURCHASE")
        assert charge_hash != refund_hash

    def test_import_hash_differs_refund_description(self):
        """Tier 3: different description also makes hash differ."""
        charge_hash = compute_import_hash("cap1-credit", "2026-01-15", -42.50, "AMAZON PURCHASE")
        refund_hash = compute_import_hash("cap1-credit", "2026-01-15", 42.50, "AMAZON REFUND")
        assert charge_hash != refund_hash

    def test_dedup_key_differs_for_refund(self):
        """Tier 4: dedup_key encodes signed cents, so refund != charge."""
        charge_key = compute_dedup_key("cap1-credit", "2026-01-15", -42.50)
        refund_key = compute_dedup_key("cap1-credit", "2026-01-15", 42.50)
        assert charge_key != refund_key
        assert charge_key == "cap1-credit:2026-01-15:-4250"
        assert refund_key == "cap1-credit:2026-01-15:4250"

    def test_refund_not_duplicate_of_charge(self, repo, imp):
        """Full dedup: a refund for the same amount/day is NOT a duplicate."""
        from src.database.dedup import DedupEngine

        engine = DedupEngine(repo)

        # Import the original charge
        charge = [RawTransaction(
            date="2026-01-15", amount=-42.50,
            raw_description="AMAZON PURCHASE",
            account_id="cap1-credit", external_id="FIT001",
        )]
        result1 = engine.process_batch(charge, imp.id, source="bank")
        assert result1.new_count == 1

        # Import the refund (same day, opposite sign, different FITID)
        imp2 = repo.insert_import(Import(file_name="refund.qfx", file_hash="refund_hash2"))
        refund = [RawTransaction(
            date="2026-01-15", amount=42.50,
            raw_description="AMAZON REFUND",
            account_id="cap1-credit", external_id="FIT002",
        )]
        result2 = engine.process_batch(refund, imp2.id, source="bank")
        assert result2.new_count == 1
        assert result2.duplicate_count == 0

    def test_same_refund_twice_is_duplicate(self, repo, imp):
        """Importing the exact same refund file twice should deduplicate."""
        from src.database.dedup import DedupEngine

        engine = DedupEngine(repo)

        refund = [RawTransaction(
            date="2026-01-15", amount=42.50,
            raw_description="AMAZON REFUND",
            account_id="cap1-credit", external_id="FIT002",
        )]
        result1 = engine.process_batch(refund, imp.id, source="bank")
        assert result1.new_count == 1

        imp2 = repo.insert_import(Import(file_name="refund2.qfx", file_hash="refund_hash3"))
        result2 = engine.process_batch(refund, imp2.id, source="bank")
        assert result2.new_count == 0
        assert result2.duplicate_count == 1


# ── Categorization: refund merchant matching ────────────────


class TestRefundMerchantMatching:
    """Merchant matching is description-based and works regardless of sign."""

    def test_starbucks_refund_matches_auto(self, config, repo, imp):
        """Positive-amount Starbucks transaction matches merchant_auto."""
        txn = _txn(imp.id, raw_description="STARBUCKS STORE 12345", amount=5.50)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "coffee-d"
        assert result.confidence == 1.0

    def test_netflix_refund_matches_auto(self, config, repo, imp):
        """Netflix refund (positive) still matches merchant_auto."""
        txn = _txn(imp.id, raw_description="Netflix.com", amount=15.99)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "netflix"

    def test_doordash_refund_matches_auto(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="DOORDASH*ORDER 99999 REFUND", amount=35.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "meal-delivery"

    def test_chewy_refund_matches_auto(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="CHEWY.COM REFUND", amount=55.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "pet-supplies"

    def test_costco_refund_matches_high_confidence(self, config, repo, imp):
        """Costco refund (positive) matches high_confidence tier."""
        txn = _txn(imp.id, raw_description="COSTCO WHSE #1234 RETURN", amount=150.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_high"
        assert result.category_id == "groceries"

    def test_target_refund_matches_high_confidence(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="TARGET T-1234 RETURN", amount=25.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_high"
        assert result.category_id == "household-supplies"


# ── Amount rules: negative ranges don't match refunds ───────


class TestRefundAmountRules:
    """Amount rules use negative ranges — positive refund amounts skip them."""

    def test_apple_refund_skips_amount_rules(self, config, repo, imp):
        """APPLE.COM/BILL refund (+$9.99) doesn't match iCloud rule ([-9.99, -9.99])."""
        txn = _txn(imp.id, raw_description="APPLE.COM/BILL", amount=9.99)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # No negative-range amount rule matches; falls through to manual review
        assert result.method == "manual_review"
        assert result.category_id == "uncategorized"

    def test_apple_charge_matches_amount_rule(self, config, repo, imp):
        """Control: APPLE.COM/BILL charge (-$9.99) matches the iCloud rule."""
        txn = _txn(imp.id, raw_description="APPLE.COM/BILL", amount=-9.99)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "amount_rule"
        assert result.category_id == "cloud-storage"

    def test_csaa_refund_skips_amount_rules(self, config, repo, imp):
        """CSAA refund (+$200) doesn't match car-insurance rule ([-300, -100])."""
        txn = _txn(imp.id, raw_description="CSAA INSURANCE GROUP", amount=200.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "manual_review"

    def test_whole_foods_refund_skips_amount_rules(self, config, repo, imp):
        """Whole Foods refund (+$75) doesn't match negative ranges."""
        txn = _txn(imp.id, raw_description="WHOLEFDS MKT 10294", amount=75.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # Positive amount skips amount rules, falls through to manual_review
        assert result.method == "manual_review"

    def test_amount_rule_unit_positive_no_match(self, config):
        """Unit test: match_amount_rule returns None for positive amounts."""
        result = match_amount_rule("APPLE.COM/BILL", 22.99, config)
        assert result is None

    def test_amount_rule_unit_negative_matches(self, config):
        """Control: negative amount matches amount rule."""
        result = match_amount_rule("APPLE.COM/BILL", -22.99, config)
        assert result is not None
        assert result.category_id == "youtube-premium"

    def test_amazon_whole_dollar_refund_on_mercury(self, config, repo, imp):
        """Amazon whole-dollar refund (+$25.00) on Mercury.

        The whole_dollar check uses _is_whole_dollar(amount), which works
        for both positive and negative amounts. The amount rule DOES match
        positive whole-dollar amounts because whole_dollar rules don't use
        negative ranges.
        """
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="AMAZON.COM*REFUND", amount=25.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # Whole-dollar check passes for +25.00, matches biz-participant-comp
        # This is debatable behavior — a refund of participant compensation
        # might warrant a different category. But the current rules don't
        # distinguish by sign.
        assert result.method == "amount_rule"
        assert result.category_id == "biz-participant-comp"


# ── Transfer detection: refund vs transfer ──────────────────


class TestRefundTransferDetection:
    """Refunds from transfer counterparties should still match transfer patterns."""

    def test_capital_one_credit_positive_amount(self, config, repo, imp):
        """A positive-amount transaction matching transfer pattern is still a transfer.

        This is the correct side of a credit card payment — the credit card
        account sees a positive amount (reducing the balance).
        """
        txn = _txn(
            imp.id, account_id="cap1-credit",
            raw_description="CAPITAL ONE MOBILE PMT", amount=500.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "transfer"
        assert result.is_transfer is True
        assert result.category_id == "xfer-cc-payment"

    def test_io_autopay_positive_side(self, config, repo, imp):
        """Mercury credit side of IO AUTOPAY has positive amount — still a transfer."""
        txn = _txn(
            imp.id, account_id="mercury-credit",
            raw_description="IO AUTOPAY", amount=100.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "transfer"
        assert result.category_id == "xfer-internal"

    def test_transfer_detection_sign_agnostic(self, config):
        """Unit test: detect_transfer matches regardless of what amount would be."""
        # The function only looks at description and account_id, not amount
        match = detect_transfer("CAPITAL ONE MOBILE PMT 240115", "wf-checking", config)
        assert match is not None
        assert match.transfer_type == "cc-payment"


# ── Mercury business logic: refunds ─────────────────────────


class TestRefundMercuryBusiness:
    """Mercury business account overrides apply to refunds too."""

    def test_personal_merchant_refund_on_mercury_becomes_biz_other(self, config, repo, imp):
        """Starbucks refund on Mercury → biz-other (not coffee-d)."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="STARBUCKS STORE 12345 REFUND", amount=5.50,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-other"
        assert result.method == "merchant_auto"

    def test_biz_compatible_refund_keeps_category(self, config, repo, imp):
        """Anthropic refund on Mercury → stays biz-saas."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="ANTHROPIC API CREDIT", amount=20.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-saas"

    def test_cashback_on_mercury_positive(self, config, repo, imp):
        """Cashback on Mercury is income (positive) and Mercury-compatible."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="Mercury IO Cashback", amount=5.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "cashback"
        assert result.method == "merchant_auto"


# ── Allocation: positive amount preservation ─────────────────


class TestRefundAllocation:
    """Allocations must preserve the positive amount for correct reporting."""

    def test_refund_allocation_is_positive(self, config, repo, imp):
        """apply_categorization stores positive amount for refunds."""
        txn = _txn(
            imp.id, raw_description="Netflix.com", amount=15.99,
            import_hash="refund_alloc_1", dedup_key="refund_alloc_dk1",
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        apply_categorization(txn, result, repo)

        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 1
        assert allocs[0].amount == 15.99  # Positive, not negated
        assert allocs[0].category_id == "netflix"

    def test_charge_allocation_is_negative(self, config, repo, imp):
        """Control: charge allocations store negative amount."""
        txn = _txn(
            imp.id, raw_description="Netflix.com", amount=-15.99,
            import_hash="charge_alloc_1", dedup_key="charge_alloc_dk1",
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        apply_categorization(txn, result, repo)

        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 1
        assert allocs[0].amount == -15.99

    def test_refund_allocation_offsets_charge(self, config, repo, imp):
        """A refund allocation + charge allocation in same category sums to zero."""
        # Insert charge
        charge = _txn(
            imp.id, raw_description="STARBUCKS STORE 12345", amount=-5.50,
            import_hash="offset_charge", dedup_key="offset_charge_dk",
        )
        repo.insert_transaction(charge)
        r1 = categorize_transaction(charge, config)
        apply_categorization(charge, r1, repo)

        # Insert refund
        refund = _txn(
            imp.id, raw_description="STARBUCKS STORE 12345 REFUND", amount=5.50,
            import_hash="offset_refund", dedup_key="offset_refund_dk",
        )
        repo.insert_transaction(refund)
        r2 = categorize_transaction(refund, config)
        apply_categorization(refund, r2, repo)

        # Both should be in coffee-d category
        allocs_charge = repo.get_allocations_by_transaction(charge.id)
        allocs_refund = repo.get_allocations_by_transaction(refund.id)
        assert allocs_charge[0].category_id == "coffee-d"
        assert allocs_refund[0].category_id == "coffee-d"

        # Sum should be zero
        total = allocs_charge[0].amount + allocs_refund[0].amount
        assert total == pytest.approx(0.0)

    def test_refund_transaction_status_categorized(self, config, repo, imp):
        """Refund transaction gets status='categorized' when matched."""
        txn = _txn(
            imp.id, raw_description="Netflix.com", amount=15.99,
            import_hash="status_refund", dedup_key="status_refund_dk",
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        apply_categorization(txn, result, repo)

        updated = repo.get_transaction(txn.id)
        assert updated.status == "categorized"
        assert updated.categorization_method == "merchant_auto"

    def test_unmatched_refund_flagged(self, config, repo, imp):
        """Refund that can't be categorized gets flagged for review."""
        txn = _txn(
            imp.id, raw_description="RANDOM REFUND", amount=50.00,
            import_hash="flag_refund", dedup_key="flag_refund_dk",
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        apply_categorization(txn, result, repo)

        updated = repo.get_transaction(txn.id)
        assert updated.status == "flagged"


# ── categorize_pending: mixed batch with refunds ─────────────


class TestRefundBatch:
    """categorize_pending handles a mix of charges and refunds."""

    def test_mixed_batch_categorizes_correctly(self, config, repo, imp):
        """Batch with charges and refunds: all categorized by same rules."""
        txns = [
            _txn(imp.id, raw_description="STARBUCKS 12345", amount=-5.50,
                 import_hash="b1", dedup_key="bd1"),
            _txn(imp.id, raw_description="STARBUCKS 12345 RETURN", amount=5.50,
                 import_hash="b2", dedup_key="bd2"),
            _txn(imp.id, raw_description="Netflix.com", amount=-15.99,
                 import_hash="b3", dedup_key="bd3"),
            _txn(imp.id, raw_description="Netflix.com CREDIT", amount=15.99,
                 import_hash="b4", dedup_key="bd4"),
            _txn(imp.id, raw_description="UNKNOWN REFUND", amount=100.00,
                 import_hash="b5", dedup_key="bd5"),
        ]
        for t in txns:
            repo.insert_transaction(t)

        result = categorize_pending(repo, config)
        assert result.total == 5
        # 4 merchant matches (2 Starbucks + 2 Netflix), 1 manual review
        assert result.method_counts.get("merchant_auto", 0) == 4
        assert result.method_counts.get("manual_review", 0) == 1
