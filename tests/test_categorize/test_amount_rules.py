"""Tests for amount-based and account-based categorization rules."""

from src.categorize.amount_rules import match_account_rule, match_amount_rule
from src.config import Config
from tests.conftest import FIXTURE_CONFIG_DIR


def config():
    return Config(FIXTURE_CONFIG_DIR)


class TestAppleBillAmountRules:
    def test_sd_movie_rental(self):
        result = match_amount_rule("APPLE.COM/BILL", -0.99, config())
        assert result is not None
        assert result.category_id == "movie-rentals"
        assert result.method == "amount_rule"

    def test_hd_movie_rental(self):
        result = match_amount_rule("APPLE.COM/BILL", -3.99, config())
        assert result is not None
        assert result.category_id == "movie-rentals"

    def test_icloud_storage(self):
        result = match_amount_rule("APPLE.COM/BILL", -9.99, config())
        assert result is not None
        assert result.category_id == "cloud-storage"

    def test_youtube_premium_family(self):
        result = match_amount_rule("APPLE.COM/BILL", -22.99, config())
        assert result is not None
        assert result.category_id == "youtube-premium"
        assert result.confidence == 0.90  # high confidence

    def test_peacock(self):
        result = match_amount_rule("APPLE.COM/BILL", -7.99, config())
        assert result is not None
        assert result.category_id == "peacock"

    def test_hulu_live_tv(self):
        result = match_amount_rule("APPLE.COM/BILL", -17.99, config())
        assert result is not None
        assert result.category_id == "hulu"

    def test_unknown_apple_amount(self):
        result = match_amount_rule("APPLE.COM/BILL", -15.00, config())
        assert result is None

    def test_case_insensitive(self):
        result = match_amount_rule("apple.com/bill", -9.99, config())
        assert result is not None
        assert result.category_id == "cloud-storage"


class TestCsaaInsuranceRules:
    def test_car_insurance(self):
        result = match_amount_rule("CSAA INSURANCE GROUP", -200.00, config())
        assert result is not None
        assert result.category_id == "car-insurance"

    def test_home_insurance(self):
        result = match_amount_rule("CSAA INSURANCE GROUP", -500.00, config())
        assert result is not None
        assert result.category_id == "home-insurance"

    def test_boundary_300_car(self):
        result = match_amount_rule("CSAA INSURANCE GROUP", -300.00, config())
        assert result is not None
        assert result.category_id == "car-insurance"

    def test_outside_range(self):
        result = match_amount_rule("CSAA INSURANCE GROUP", -50.00, config())
        assert result is None


class TestWholeFoodsRules:
    def test_small_purchase_snacks(self):
        result = match_amount_rule("WHOLEFDS MKT 10294", -15.00, config())
        assert result is not None
        assert result.category_id == "snacks-beer"

    def test_large_purchase_groceries(self):
        result = match_amount_rule("WHOLEFDS MKT 10294", -75.00, config())
        assert result is not None
        assert result.category_id == "groceries"

    def test_boundary_30_snacks(self):
        """At -30, should be snacks (upper bound of snacks range)."""
        result = match_amount_rule("WHOLEFDS MKT 10294", -30.00, config())
        assert result is not None
        # -30 is in [-30, 0] range for snacks and [-500, -30] for groceries
        # Since snacks rule comes first in YAML, it matches first
        assert result.category_id == "snacks-beer"


class TestAmazonParticipantComp:
    def test_whole_dollar_on_mercury(self):
        result = match_amount_rule("AMAZON.COM*AB12CD", -25.00, config(), account_id="mercury-checking")
        assert result is not None
        assert result.category_id == "biz-participant-comp"
        assert result.method == "amount_rule"
        assert result.confidence == 0.90  # high

    def test_whole_dollar_amzn_on_mercury(self):
        result = match_amount_rule("AMZN MKTP US*AB1CD2EF3", -50.00, config(), account_id="mercury-credit")
        assert result is not None
        assert result.category_id == "biz-participant-comp"

    def test_fractional_dollar_on_mercury_no_match(self):
        """Non-whole-dollar Amazon on Mercury should NOT match."""
        result = match_amount_rule("AMAZON.COM*AB12CD", -25.99, config(), account_id="mercury-checking")
        assert result is None

    def test_whole_dollar_on_wf_no_match(self):
        """Amazon whole-dollar on non-Mercury account should NOT match."""
        result = match_amount_rule("AMAZON.COM*AB12CD", -25.00, config(), account_id="wf-checking")
        assert result is None

    def test_whole_dollar_no_account_no_match(self):
        """Amazon whole-dollar without account_id should NOT match."""
        result = match_amount_rule("AMAZON.COM*AB12CD", -25.00, config())
        assert result is None

    def test_mercury_savings(self):
        result = match_amount_rule("AMAZON.COM*ORDER", -100.00, config(), account_id="mercury-savings")
        assert result is not None
        assert result.category_id == "biz-participant-comp"

    def test_various_whole_dollar_amounts(self):
        """Common participant compensation amounts."""
        for amount in [-10.00, -25.00, -50.00, -75.00, -100.00, -150.00]:
            result = match_amount_rule("AMAZON.COM*AB12CD", amount, config(), account_id="mercury-checking")
            assert result is not None, f"Failed for amount {amount}"
            assert result.category_id == "biz-participant-comp"


class TestNoMatch:
    def test_non_ambiguous_merchant(self):
        result = match_amount_rule("DOORDASH*ORDER", -25.00, config())
        assert result is None

    def test_random_merchant(self):
        result = match_amount_rule("RANDOM STORE", -100.00, config())
        assert result is None


class TestAccountRules:
    def test_mercury_checking(self):
        result = match_account_rule("mercury-checking", config())
        assert result is not None
        assert result.method == "account_rule"
        assert result.category_id == "biz-other"

    def test_mercury_savings(self):
        result = match_account_rule("mercury-savings", config())
        assert result is not None
        assert result.category_id == "biz-other"

    def test_mercury_credit(self):
        result = match_account_rule("mercury-credit", config())
        assert result is not None
        assert result.category_id == "biz-other"
        assert result.confidence == 0.60

    def test_golden1_auto(self):
        result = match_account_rule("golden1-auto", config())
        assert result is not None
        assert result.category_id == "interest-fees"
        assert result.confidence == 0.80

    def test_wf_checking_no_rule(self):
        result = match_account_rule("wf-checking", config())
        assert result is None

    def test_cap1_credit_no_rule(self):
        result = match_account_rule("cap1-credit", config())
        assert result is None
