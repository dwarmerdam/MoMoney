"""Tests for merchant matching module."""

from src.categorize.merchant_match import match_merchant_auto, match_merchant_high
from src.config import Config
from tests.conftest import FIXTURE_CONFIG_DIR


def config():
    return Config(FIXTURE_CONFIG_DIR)


class TestAutoMatch:
    def test_doordash(self):
        result = match_merchant_auto("DOORDASH*ORDER 12345", config())
        assert result is not None
        assert result.category_id == "meal-delivery"
        assert result.confidence == 1.0
        assert result.tier == "auto"

    def test_instacart(self):
        result = match_merchant_auto("INSTACART HTTPSINSTACAR", config())
        assert result is not None
        assert result.category_id == "groceries"

    def test_chewy(self):
        result = match_merchant_auto("CHEWY.COM 800-672-4399", config())
        assert result is not None
        assert result.category_id == "pet-supplies"

    def test_netflix(self):
        result = match_merchant_auto("Netflix.com", config())
        assert result is not None
        assert result.category_id == "netflix"

    def test_spotify(self):
        result = match_merchant_auto("Spotify USA", config())
        assert result is not None
        assert result.category_id == "music-d"

    def test_peets_coffee(self):
        result = match_merchant_auto("PEETS COFFEE #123", config())
        assert result is not None
        assert result.category_id == "coffee-d"

    def test_starbucks(self):
        result = match_merchant_auto("STARBUCKS STORE 12345", config())
        assert result is not None
        assert result.category_id == "coffee-d"

    def test_wf_mortgage(self):
        result = match_merchant_auto("WF HOME MTG***1234", config())
        assert result is not None
        assert result.category_id == "mortgage"

    def test_interest_payment_exact(self):
        result = match_merchant_auto("INTEREST PAYMENT", config())
        assert result is not None
        assert result.category_id == "interest"

    def test_interest_payment_substring_no_match(self):
        """Interest Payment is exact match — substring shouldn't match."""
        result = match_merchant_auto("BANK INTEREST PAYMENT DEPOSIT", config())
        # "INTEREST PAYMENT" is exact match, and this contains it
        # so it won't match exact. But there's no contains rule for it.
        # Actually let me check — the rule is match: exact for "INTEREST PAYMENT"
        # "BANK INTEREST PAYMENT DEPOSIT" != "INTEREST PAYMENT" → no match
        assert result is None

    def test_employer_payroll(self):
        result = match_merchant_auto("ACME CORP INC PAYROLL", config())
        assert result is not None
        assert result.category_id == "earnings"

    def test_geico(self):
        result = match_merchant_auto("GEICO *AUTO", config())
        assert result is not None
        assert result.category_id == "car-insurance"

    def test_pgande(self):
        result = match_merchant_auto("PGANDE WEB ONLINE", config())
        assert result is not None
        assert result.category_id == "gas-electric"

    def test_pg_ampersand_e(self):
        result = match_merchant_auto("PG&E BILL PAYMENT", config())
        assert result is not None
        assert result.category_id == "gas-electric"

    def test_anthropic(self):
        result = match_merchant_auto("ANTHROPIC API CHARGE", config())
        assert result is not None
        assert result.category_id == "biz-saas"

    def test_github(self):
        result = match_merchant_auto("GITHUB INC", config())
        assert result is not None
        assert result.category_id == "biz-saas"

    def test_shell_gas(self):
        result = match_merchant_auto("SHELL OIL 57422", config())
        assert result is not None
        assert result.category_id == "auto-fuel"

    def test_kaiser(self):
        result = match_merchant_auto("KAISER HOSPITAL SF", config())
        assert result is not None
        assert result.category_id == "medical"

    def test_cashback(self):
        result = match_merchant_auto("Mercury IO Cashback", config())
        assert result is not None
        assert result.category_id == "cashback"

    def test_no_match(self):
        result = match_merchant_auto("RANDOM STORE 12345", config())
        assert result is None

    def test_case_insensitive(self):
        result = match_merchant_auto("doordash order", config())
        assert result is not None
        assert result.category_id == "meal-delivery"

    def test_lyft_exact_match(self):
        """Lyft is exact match — should only match 'Lyft' not 'LYFT RIDE 123'."""
        result = match_merchant_auto("Lyft", config())
        assert result is not None
        assert result.category_id == "weekend-lyft"

    def test_lyft_with_extra_text(self):
        """Lyft exact match should NOT match with extra text."""
        result = match_merchant_auto("LYFT *RIDE 12345", config())
        assert result is None

    def test_vet_clinic_exact(self):
        result = match_merchant_auto("Generic Veterinary Clinic", config())
        assert result is not None
        assert result.category_id == "vet-care"

    def test_vet_clinic_bank_variant(self):
        result = match_merchant_auto("GENERIC VETERINARY CLINI ANYTOWN CA", config())
        assert result is not None
        assert result.category_id == "vet-care"


class TestHighConfidenceMatch:
    def test_philz(self):
        result = match_merchant_high("PHILZ COFFEE SF", config())
        assert result is not None
        assert result.category_id == "coffee-d"
        assert result.tier == "high_confidence"
        assert 0.80 <= result.confidence <= 0.99

    def test_trader_joes(self):
        result = match_merchant_high("TRADER JOE'S #123", config())
        assert result is not None
        assert result.category_id == "groceries"
        assert result.confidence == 0.85

    def test_costco(self):
        result = match_merchant_high("COSTCO WHSE #1234", config())
        assert result is not None
        assert result.category_id == "groceries"
        assert result.confidence == 0.83

    def test_safeway(self):
        result = match_merchant_high("SAFEWAY STORE #1234", config())
        assert result is not None
        assert result.category_id == "groceries"
        assert result.confidence == 0.90

    def test_target(self):
        result = match_merchant_high("TARGET T-1234", config())
        assert result is not None
        assert result.category_id == "household-supplies"

    def test_walgreens(self):
        result = match_merchant_high("WALGREENS #1234", config())
        assert result is not None
        assert result.category_id == "household-supplies"

    def test_dollar_tree(self):
        result = match_merchant_high("DOLLAR TREE #5678", config())
        assert result is not None
        assert result.category_id == "household-personal"

    def test_aaa_insurance(self):
        result = match_merchant_high("AAA INSURANCE CO", config())
        assert result is not None
        assert result.category_id == "car-insurance"

    def test_no_match(self):
        result = match_merchant_high("RANDOM MERCHANT", config())
        assert result is None

    def test_auto_merchant_not_in_high(self):
        """DoorDash is auto tier — should NOT match high-confidence."""
        result = match_merchant_high("DOORDASH*ORDER", config())
        assert result is None


class TestTierPriority:
    def test_auto_before_high(self):
        """A merchant in both tiers should be found by auto first."""
        # This shouldn't happen in practice (merchants are in one tier),
        # but verify the lookup functions are independent
        auto = match_merchant_auto("STARBUCKS STORE", config())
        high = match_merchant_high("STARBUCKS STORE", config())
        assert auto is not None
        assert high is None  # Starbucks is only in auto
