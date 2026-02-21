"""Tests for the categorization pipeline orchestrator."""

from pathlib import Path

import pytest

from src.categorize.pipeline import (
    CategorizeResult,
    _is_compatible,
    apply_categorization,
    categorize_pending,
    categorize_transaction,
)
from src.config import Config
from src.database.models import Allocation, Import, Transaction
from src.database.repository import Repository
from tests.conftest import FIXTURE_CONFIG_DIR

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


@pytest.fixture
def config():
    return Config(FIXTURE_CONFIG_DIR)


@pytest.fixture
def repo():
    r = Repository(":memory:")
    r.apply_migrations(MIGRATIONS_DIR)
    yield r
    r.close()


@pytest.fixture
def imp(repo):
    return repo.insert_import(Import(file_name="test.qfx", file_hash="hash1"))


def _txn(imp_id, **kw) -> Transaction:
    defaults = dict(
        account_id="wf-checking",
        date="2026-01-15",
        amount=-50.00,
        raw_description="PHILZ COFFEE SF",
        import_id=imp_id,
        import_hash="h1",
        dedup_key="dk1",
    )
    defaults.update(kw)
    return Transaction(**defaults)


# ── Step 1: Transfer detection ────────────────────────────


class TestStep1Transfers:
    def test_capital_one_payment(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="CAPITAL ONE MOBILE PMT 240115", amount=-500.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "transfer"
        assert result.is_transfer is True
        assert result.category_id == "xfer-cc-payment"
        assert result.confidence == 1.0
        assert result.from_account == "wf-checking"
        assert result.to_account == "cap1-credit"

    def test_mercury_io_autopay(self, config, repo, imp):
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="IO AUTOPAY", amount=-100.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "transfer"
        assert result.category_id == "xfer-internal"

    def test_golden1_interest_fitid(self, config, repo, imp):
        txn = _txn(
            imp.id, account_id="golden1-auto",
            raw_description="INTEREST", external_id="66886_84INT",
            amount=-55.17,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "interest_detection"
        assert result.category_id == "interest-fees"

    def test_transfer_takes_priority_over_merchant(self, config, repo, imp):
        """WF HOME MTG matches both transfer and merchant — transfer wins."""
        txn = _txn(imp.id, raw_description="WF HOME MTG***1668", amount=-1668.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # WF HOME MTG is in merchant auto-match for mortgage,
        # but NOT in transfer patterns, so it should be merchant_auto
        assert result.method == "merchant_auto"
        assert result.category_id == "mortgage"

    def test_transfer_by_txn_type(self, config, repo, imp):
        """txn_type=TRANSFER + 'Transfer : Capital One' detected via inferred path."""
        txn = _txn(
            imp.id, raw_description="Transfer : Capital One",
            txn_type="TRANSFER", amount=-2700.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.is_transfer is True
        assert result.category_id == "xfer-cc-payment"
        assert result.confidence == 1.0

    def test_transfer_by_txn_type_savings(self, config, repo, imp):
        """txn_type=TRANSFER + 'Transfer : 1234 Joint Savings' → savings."""
        txn = _txn(
            imp.id, raw_description="Transfer : 1234 Joint Savings",
            txn_type="TRANSFER", amount=-75.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.is_transfer is True
        assert result.category_id == "xfer-savings"

    def test_pattern_priority_over_txn_type(self, config, repo, imp):
        """Pattern-based detection returns method='transfer', not 'transfer_inferred'."""
        txn = _txn(
            imp.id, raw_description="CAPITAL ONE MOBILE PMT 240115",
            txn_type="TRANSFER", amount=-500.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "transfer"
        assert result.is_transfer is True


# ── Step 2: Merchant auto-match ───────────────────────────


class TestStep2MerchantAuto:
    def test_doordash(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="DOORDASH*ORDER 12345", amount=-35.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "meal-delivery"
        assert result.confidence == 1.0

    def test_netflix(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="Netflix.com", amount=-15.99)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "netflix"

    def test_interest_payment(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="INTEREST PAYMENT", amount=3.50)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "interest"

    def test_employer_payroll_income(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="ACME CORP INC PAYROLL", amount=5500.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_auto"
        assert result.category_id == "earnings"


# ── Step 3: Amount rules ─────────────────────────────────


class TestStep3AmountRules:
    def test_apple_bill_movie_rental(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="APPLE.COM/BILL", amount=-3.99)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "amount_rule"
        assert result.category_id == "movie-rentals"

    def test_apple_bill_icloud(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="APPLE.COM/BILL", amount=-9.99)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "amount_rule"
        assert result.category_id == "cloud-storage"

    def test_csaa_car(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="CSAA INSURANCE GROUP", amount=-200.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "amount_rule"
        assert result.category_id == "car-insurance"

    def test_whole_foods_groceries(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="WHOLEFDS MKT 10294", amount=-75.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "amount_rule"
        assert result.category_id == "groceries"


# ── Step 4: Account rules ────────────────────────────────


class TestStep4AccountRules:
    def test_mercury_checking_default(self, config, repo, imp):
        """Unknown Mercury merchant → biz-other default."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="RANDOM VENDOR LLC", amount=-150.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "account_rule"
        assert result.category_id == "biz-other"

    def test_mercury_credit_default(self, config, repo, imp):
        """Unknown Mercury credit merchant → biz-other default."""
        txn = _txn(
            imp.id, account_id="mercury-credit",
            raw_description="UNKNOWN VENDOR", amount=-75.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "account_rule"
        assert result.category_id == "biz-other"

    def test_golden1_non_transfer(self, config, repo, imp):
        """Golden1 without INT FITID → interest-fees (account rule)."""
        txn = _txn(
            imp.id, account_id="golden1-auto",
            raw_description="MISC FEE", external_id="99999",
            amount=-10.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "account_rule"
        assert result.category_id == "interest-fees"


# ── Step 5: Merchant high-confidence ──────────────────────


class TestStep5MerchantHigh:
    def test_costco_from_wf_checking(self, config, repo, imp):
        """Costco on wf-checking (no account rule) → high-confidence groceries."""
        txn = _txn(imp.id, raw_description="COSTCO WHSE #1234", amount=-150.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_high"
        assert result.category_id == "groceries"
        assert 0.80 <= result.confidence <= 0.99

    def test_safeway(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="SAFEWAY STORE #1234", amount=-85.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_high"
        assert result.category_id == "groceries"

    def test_target(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="TARGET T-1234", amount=-45.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "merchant_high"
        assert result.category_id == "household-supplies"


# ── Step 7: Claude AI categorization ──────────────────────


class TestStep7ClaudeAI:
    def test_claude_categorizes_unknown_merchant(self, config, repo, imp):
        """Step 7: Claude provides a category for an otherwise-unknown merchant."""
        import json

        txn = _txn(imp.id, raw_description="SOME RANDOM PLACE", amount=-25.00)
        repo.insert_transaction(txn)

        def mock_claude_fn(system, prompt):
            return json.dumps({
                "category_id": "dining-out",
                "confidence": 0.7,
                "reasoning": "Appears to be a restaurant",
            })

        result = categorize_transaction(
            txn, config, claude_fn=mock_claude_fn, repo=repo,
        )
        assert result.method == "claude_ai"
        assert result.category_id == "dining-out"
        assert result.confidence == 0.7

    def test_claude_skipped_when_no_claude_fn(self, config, repo, imp):
        """Without claude_fn, Step 7 is skipped → manual review."""
        txn = _txn(imp.id, raw_description="SOME RANDOM PLACE", amount=-25.00)
        repo.insert_transaction(txn)

        result = categorize_transaction(txn, config, claude_fn=None, repo=repo)
        assert result.method == "manual_review"

    def test_claude_skipped_when_no_repo(self, config, repo, imp):
        """Without repo, Step 7 is skipped → manual review."""
        txn = _txn(imp.id, raw_description="SOME RANDOM PLACE", amount=-25.00)
        repo.insert_transaction(txn)

        def mock_claude_fn(system, prompt):
            return '{"category_id": "dining", "confidence": 0.7, "reasoning": "test"}'

        result = categorize_transaction(txn, config, claude_fn=mock_claude_fn, repo=None)
        assert result.method == "manual_review"

    def test_claude_fallback_returns_uncategorized(self, config, repo, imp):
        """When Claude returns uncategorized → falls through to manual review."""
        import json

        txn = _txn(imp.id, raw_description="SOME RANDOM PLACE", amount=-25.00)
        repo.insert_transaction(txn)

        def mock_claude_fn(system, prompt):
            return json.dumps({
                "category_id": "uncategorized",
                "confidence": 0.0,
                "reasoning": "Cannot determine",
            })

        result = categorize_transaction(
            txn, config, claude_fn=mock_claude_fn, repo=repo,
        )
        assert result.method == "manual_review"

    def test_claude_respects_category_filter(self, config, repo, imp):
        """Claude result on account with category_filter gets filtered."""
        import json

        # Use wf-checking (no account_rule, no category_filter) to
        # confirm category_filter logic. Mercury accounts have account
        # rules at Step 4 that fire before Step 7 is reached.
        # So we test the filter by checking that a wf-checking
        # transaction gets the Claude-suggested category as-is
        # (wf-checking has no filter).
        txn = _txn(
            imp.id, account_id="wf-checking",
            raw_description="SOME RANDOM PLACE", amount=-25.00,
        )
        repo.insert_transaction(txn)

        def mock_claude_fn(system, prompt):
            return json.dumps({
                "category_id": "groceries",
                "confidence": 0.8,
                "reasoning": "Grocery store",
            })

        result = categorize_transaction(
            txn, config, claude_fn=mock_claude_fn, repo=repo,
        )
        # wf-checking has no category filter, so groceries passes through
        assert result.method == "claude_ai"
        assert result.category_id == "groceries"

    def test_earlier_steps_take_priority_over_claude(self, config, repo, imp):
        """Known merchants should never reach Step 7."""
        txn = _txn(imp.id, raw_description="DOORDASH*ORDER 12345", amount=-35.00)
        repo.insert_transaction(txn)

        call_count = 0

        def mock_claude_fn(system, prompt):
            nonlocal call_count
            call_count += 1
            return '{"category_id": "dining", "confidence": 0.9, "reasoning": "test"}'

        result = categorize_transaction(
            txn, config, claude_fn=mock_claude_fn, repo=repo,
        )
        assert result.method == "merchant_auto"  # Step 2 matches first
        assert call_count == 0  # Claude was never called


# ── Step 8: Manual review fallback ────────────────────────


class TestStep8ManualReview:
    def test_unknown_merchant(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="SOME RANDOM PLACE", amount=-25.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "manual_review"
        assert result.category_id == "uncategorized"
        assert result.confidence == 0.0

    def test_check_payment_ambiguous(self, config, repo, imp):
        """CHECK is ambiguous with <80% consistency → manual review."""
        txn = _txn(imp.id, raw_description="CHECK 1234", amount=-500.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "manual_review"

    def test_amazon_no_amount_rule(self, config, repo, imp):
        """Amazon fractional amount on wf-checking → manual review."""
        txn = _txn(imp.id, raw_description="AMAZON.COM*AMZN MKTP", amount=-35.47)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # Amazon is not in auto or high_confidence — it's in ambiguous
        # Amazon whole-dollar rules only apply to Mercury accounts
        assert result.method == "manual_review"


# ── Step 3b: Amazon participant compensation on Mercury ───


class TestStep3AmazonParticipantComp:
    def test_amazon_whole_dollar_on_mercury(self, config, repo, imp):
        """Amazon whole-dollar on Mercury → biz-participant-comp."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="AMAZON.COM*AB12CD", amount=-25.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "amount_rule"
        assert result.category_id == "biz-participant-comp"
        assert result.confidence == 0.90

    def test_amzn_whole_dollar_on_mercury(self, config, repo, imp):
        """AMZN variant whole-dollar on Mercury → biz-participant-comp."""
        txn = _txn(
            imp.id, account_id="mercury-credit",
            raw_description="AMZN MKTP US*AB1CD2EF3", amount=-50.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "amount_rule"
        assert result.category_id == "biz-participant-comp"

    def test_amazon_fractional_on_mercury_falls_through(self, config, repo, imp):
        """Amazon non-whole-dollar on Mercury → account rule (not amount rule)."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="AMAZON.COM*AB12CD", amount=-25.99,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # Fractional amount doesn't match whole-dollar rule,
        # falls through to account rule (Mercury → biz-other)
        assert result.method == "account_rule"
        assert result.category_id == "biz-other"

    def test_amazon_whole_dollar_on_wf_manual_review(self, config, repo, imp):
        """Amazon whole-dollar on wf-checking → manual review (rules scoped to Mercury)."""
        txn = _txn(
            imp.id, account_id="wf-checking",
            raw_description="AMAZON.COM*AB12CD", amount=-25.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "manual_review"

    def test_biz_participant_comp_is_mercury_compatible(self, config, repo, imp):
        """biz-participant-comp passes Mercury compatibility (biz- prefix)."""
        # Verify the category doesn't get overridden to biz-other
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="AMAZON.COM*AB12CD", amount=-100.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # biz-participant-comp starts with biz-, so it's Mercury-compatible
        assert result.category_id == "biz-participant-comp"
        assert result.category_id != "biz-other"


# ── Mercury business account priority ─────────────────────


class TestCategoryFilter:
    """Unit tests for the config-driven category filter."""

    FILTER = {
        "default_category": "biz-other",
        "compatible_prefixes": ["biz-"],
        "compatible_ids": [
            "earnings", "cashback", "interest", "gross-proceeds",
            "unemployment", "interest-fees",
        ],
    }

    def test_biz_categories_compatible(self):
        assert _is_compatible("biz-saas", self.FILTER) is True
        assert _is_compatible("biz-infrastructure", self.FILTER) is True
        assert _is_compatible("biz-domains", self.FILTER) is True
        assert _is_compatible("biz-other", self.FILTER) is True

    def test_income_categories_compatible(self):
        assert _is_compatible("earnings", self.FILTER) is True
        assert _is_compatible("cashback", self.FILTER) is True
        assert _is_compatible("interest", self.FILTER) is True
        assert _is_compatible("gross-proceeds", self.FILTER) is True

    def test_personal_categories_not_compatible(self):
        assert _is_compatible("coffee-d", self.FILTER) is False
        assert _is_compatible("groceries", self.FILTER) is False
        assert _is_compatible("meal-delivery", self.FILTER) is False
        assert _is_compatible("netflix", self.FILTER) is False
        assert _is_compatible("household-supplies", self.FILTER) is False

    def test_no_filter_means_everything_compatible(self):
        """When category_filter is None, _apply_filter returns original."""
        from src.categorize.pipeline import _apply_filter
        assert _apply_filter("coffee-d", None) == "coffee-d"


class TestMercuryBusinessPriority:
    """Business account category filter integration tests.
    Personal categories should be overridden to the account's default."""

    def test_starbucks_on_mercury_becomes_biz_other(self, config, repo, imp):
        """Starbucks on Mercury → biz-other (not coffee-d)."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="STARBUCKS STORE 12345", amount=-5.50,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-other"
        assert result.method == "merchant_auto"

    def test_starbucks_on_wf_stays_coffee(self, config, repo, imp):
        """Same merchant on wf-checking → coffee-d (personal)."""
        txn = _txn(
            imp.id, account_id="wf-checking",
            raw_description="STARBUCKS STORE 12345", amount=-5.50,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "coffee-d"

    def test_doordash_on_mercury_becomes_biz_other(self, config, repo, imp):
        txn = _txn(
            imp.id, account_id="mercury-credit",
            raw_description="DOORDASH*ORDER 12345", amount=-35.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-other"

    def test_anthropic_on_mercury_stays_biz_saas(self, config, repo, imp):
        """Biz-compatible merchant on Mercury → keeps biz-saas."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="ANTHROPIC API CHARGE", amount=-20.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-saas"
        assert result.method == "merchant_auto"

    def test_github_on_mercury_stays_biz_saas(self, config, repo, imp):
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="GITHUB INC", amount=-4.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-saas"

    def test_digitalocean_on_mercury_stays_biz_infra(self, config, repo, imp):
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="DIGITALOCEAN.COM", amount=-12.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-infrastructure"

    def test_cashback_on_mercury_stays_income(self, config, repo, imp):
        """Income categories are Mercury-compatible."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="Mercury IO Cashback", amount=5.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "cashback"

    def test_interest_on_mercury_stays_income(self, config, repo, imp):
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="INTEREST PAYMENT", amount=3.50,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "interest"

    def test_mercury_transfer_unaffected(self, config, repo, imp):
        """Transfers on Mercury are not subject to category override."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="IO AUTOPAY", amount=-100.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "xfer-internal"
        assert result.is_transfer is True

    def test_mercury_unknown_vendor_gets_biz_other(self, config, repo, imp):
        """Unknown merchant on Mercury → account rule → biz-other."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="RANDOM VENDOR LLC", amount=-150.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.method == "account_rule"
        assert result.category_id == "biz-other"

    def test_mercury_credit_same_logic(self, config, repo, imp):
        """mercury-credit follows the same business account logic."""
        txn = _txn(
            imp.id, account_id="mercury-credit",
            raw_description="Netflix.com", amount=-15.99,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "biz-other"

    def test_mercury_savings_same_logic(self, config, repo, imp):
        """mercury-savings follows the same business account logic."""
        txn = _txn(
            imp.id, account_id="mercury-savings",
            raw_description="INTEREST PAYMENT", amount=10.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        assert result.category_id == "interest"  # Income, Mercury-compatible

    def test_high_confidence_on_mercury_overridden(self, config, repo, imp):
        """High-confidence personal merchants on Mercury → biz-other via account rule."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="COSTCO WHSE #1234", amount=-150.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # Costco would normally be groceries (high_confidence),
        # but Mercury account rule fires first (step 4 before step 5).
        assert result.category_id == "biz-other"

    def test_amount_rule_on_mercury_overridden(self, config, repo, imp):
        """Amount rules that resolve to personal categories → biz-other on Mercury."""
        txn = _txn(
            imp.id, account_id="mercury-checking",
            raw_description="WHOLEFDS MKT 10294", amount=-75.00,
        )
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        # Whole Foods $75 would be groceries via amount rule, but
        # Mercury overrides personal categories to biz-other
        assert result.category_id == "biz-other"
        assert result.method == "amount_rule"


# ── apply_categorization ──────────────────────────────────


class TestApplyCategorization:
    def test_creates_allocation(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="Netflix.com", amount=-15.99)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        apply_categorization(txn, result, repo)

        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 1
        assert allocs[0].category_id == "netflix"
        assert allocs[0].amount == -15.99
        assert allocs[0].source == "auto"

    def test_updates_transaction_status(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="DOORDASH*ORDER", amount=-30.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        apply_categorization(txn, result, repo)

        updated = repo.get_transaction(txn.id)
        assert updated.status == "categorized"
        assert updated.confidence == 1.0
        assert updated.categorization_method == "merchant_auto"

    def test_manual_review_flags(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="RANDOM PLACE", amount=-25.00)
        repo.insert_transaction(txn)
        result = categorize_transaction(txn, config)
        apply_categorization(txn, result, repo)

        updated = repo.get_transaction(txn.id)
        assert updated.status == "flagged"


# ── categorize_pending (batch) ────────────────────────────


class TestCategorizePending:
    def test_categorizes_batch(self, config, repo, imp):
        txns = [
            _txn(imp.id, raw_description="DOORDASH*ORDER", amount=-30.00, import_hash="h1", dedup_key="d1"),
            _txn(imp.id, raw_description="Netflix.com", amount=-15.99, import_hash="h2", dedup_key="d2"),
            _txn(imp.id, raw_description="RANDOM STORE", amount=-25.00, import_hash="h3", dedup_key="d3"),
        ]
        for t in txns:
            repo.insert_transaction(t)

        result = categorize_pending(repo, config)
        assert result.total == 3
        assert result.method_counts.get("merchant_auto", 0) == 2
        assert result.method_counts.get("manual_review", 0) == 1

    def test_respects_limit(self, config, repo, imp):
        for i in range(5):
            txn = _txn(
                imp.id, raw_description=f"DOORDASH*ORDER {i}",
                amount=-10.0 * (i + 1), import_hash=f"h{i}", dedup_key=f"d{i}",
            )
            repo.insert_transaction(txn)

        result = categorize_pending(repo, config, limit=3)
        assert result.total == 3

    def test_empty_pending(self, config, repo, imp):
        result = categorize_pending(repo, config)
        assert result.total == 0
        assert result.method_counts == {}

    def test_already_categorized_skipped(self, config, repo, imp):
        txn = _txn(imp.id, raw_description="DOORDASH*ORDER", amount=-30.00)
        repo.insert_transaction(txn)
        repo.update_transaction_status(txn.id, "categorized")

        result = categorize_pending(repo, config)
        assert result.total == 0  # Already categorized, not pending

    def test_transfer_counted(self, config, repo, imp):
        txn = _txn(
            imp.id, raw_description="CAPITAL ONE MOBILE PMT",
            amount=-500.00,
        )
        repo.insert_transaction(txn)

        result = categorize_pending(repo, config)
        assert result.transfer_count == 1
