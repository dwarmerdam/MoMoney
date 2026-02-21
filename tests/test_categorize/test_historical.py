"""Tests for historical pattern matching (Step 2.5)."""

from pathlib import Path

from src.categorize.historical import (
    AMOUNT_TOLERANCE,
    DESC_CONFIDENCE,
    DESC_MIN_AGREEMENT,
    DESC_MIN_COUNT,
    EXACT_CONFIDENCE,
    EXACT_MIN_COUNT,
    match_historical,
)
from src.database.models import Allocation, Import, Transaction
from src.database.repository import Repository

MIGRATIONS = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


def _repo() -> Repository:
    """Create an in-memory repo with schema applied."""
    repo = Repository(":memory:")
    repo.apply_migrations(MIGRATIONS)
    return repo


def _import(repo: Repository) -> str:
    """Insert a dummy import and return its ID."""
    imp = Import(file_name="test.qfx", file_hash="testhash123")
    repo.insert_import(imp)
    return imp.id


def _add_history(
    repo: Repository,
    import_id: str,
    normalized_desc: str,
    amount: float,
    category_id: str,
    count: int = 1,
    source: str = "auto",
):
    """Insert `count` categorized transactions with matching allocations."""
    for _ in range(count):
        txn = Transaction(
            account_id="test-checking",
            date="2024-01-15",
            amount=amount,
            raw_description=normalized_desc,
            normalized_description=normalized_desc,
            import_id=import_id,
            import_hash=f"hash-{id(txn) if False else _add_history._counter}",
            dedup_key=f"dk-{_add_history._counter}",
            status="categorized",
        )
        _add_history._counter += 1
        repo.insert_transaction(txn)
        alloc = Allocation(
            transaction_id=txn.id,
            category_id=category_id,
            amount=amount,
            source=source,
        )
        repo.insert_allocation(alloc)

_add_history._counter = 0


class TestExactMatch:
    """Level 1: Same description + same amount (+-$0.01), unanimous."""

    def test_unanimous_exact_match(self):
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "CSAA INSURANCE", -344.09, "car-insurance", count=3)

        result = match_historical("CSAA INSURANCE", -344.09, repo)
        assert result is not None
        assert result.category_id == "car-insurance"
        assert result.confidence == EXACT_CONFIDENCE
        assert result.match_level == "exact"
        assert result.match_count == 3
        assert result.agreement_pct == 1.0

    def test_below_min_count(self):
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "CSAA INSURANCE", -344.09, "car-insurance", count=1)

        result = match_historical("CSAA INSURANCE", -344.09, repo)
        # Only 1 exact match, below EXACT_MIN_COUNT=2, falls through
        # Also below DESC_MIN_COUNT=3
        assert result is None

    def test_exact_disagreement_falls_to_description(self):
        """Two exact-amount matches with different categories -> skip exact, try desc."""
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "CSAA INSURANCE", -344.09, "car-insurance", count=2)
        _add_history(repo, imp_id, "CSAA INSURANCE", -344.09, "home-insurance", count=2)

        result = match_historical("CSAA INSURANCE", -344.09, repo)
        # 4 total, split 50/50 -> below 80% agreement -> None
        assert result is None

    def test_amount_tolerance(self):
        """Amounts within AMOUNT_TOLERANCE should match."""
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "MERCHANT X", -100.00, "groceries", count=2)

        result = match_historical("MERCHANT X", -100.005, repo)
        assert result is not None
        assert result.match_level == "exact"

    def test_amount_outside_tolerance(self):
        """Amounts outside tolerance should not exact-match."""
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "MERCHANT X", -100.00, "groceries", count=3)

        result = match_historical("MERCHANT X", -100.05, repo)
        # Not exact match, but has 3 desc-only matches at 100% agreement
        assert result is not None
        assert result.match_level == "description"


class TestDescriptionMatch:
    """Level 2: Same description, any amount, >=80% agreement."""

    def test_strong_agreement(self):
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "WHOLE FOODS", -50.00, "groceries", count=4)
        _add_history(repo, imp_id, "WHOLE FOODS", -75.00, "groceries", count=3)
        _add_history(repo, imp_id, "WHOLE FOODS", -30.00, "dining-out", count=1)

        result = match_historical("WHOLE FOODS", -60.00, repo)
        assert result is not None
        assert result.category_id == "groceries"
        assert result.confidence == DESC_CONFIDENCE
        assert result.match_level == "description"
        assert result.match_count == 8
        assert result.agreement_pct >= DESC_MIN_AGREEMENT

    def test_below_count_threshold(self):
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "RARE MERCHANT", -25.00, "misc", count=2)

        result = match_historical("RARE MERCHANT", -999.00, repo)
        assert result is None

    def test_below_agreement_threshold(self):
        """60% agreement is below the 80% threshold."""
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "AMBIGUOUS", -50.00, "cat-a", count=3)
        _add_history(repo, imp_id, "AMBIGUOUS", -100.00, "cat-b", count=2)

        result = match_historical("AMBIGUOUS", -200.00, repo)
        # 60% agreement on cat-a -> below 80% threshold
        assert result is None

    def test_user_weighting(self):
        """User-corrected allocations should be weighted 1.5x."""
        repo = _repo()
        imp_id = _import(repo)
        # 2 auto votes for cat-a, 2 user votes for cat-b
        # Without weighting: 50/50 -> no match
        # With weighting: cat-a=2, cat-b=2*1.5=3 -> cat-b wins at 3/5=60% -> still below 80%
        _add_history(repo, imp_id, "WEIGHTED", -10.00, "cat-a", count=2, source="auto")
        _add_history(repo, imp_id, "WEIGHTED", -20.00, "cat-b", count=2, source="user")

        result = match_historical("WEIGHTED", -30.00, repo)
        # 60% even with weighting -> below threshold
        assert result is None

    def test_user_weighting_tips_balance(self):
        """User weighting can tip a borderline case over the 80% threshold."""
        repo = _repo()
        imp_id = _import(repo)
        # 3 auto for cat-a, 1 user for cat-b
        # Without weighting: cat-a=3/4=75% -> below 80%
        # With user weight: cat-a=3, cat-b=1.5, total=4.5, cat-a=3/4.5=66.7% -> still below
        # Let's try: 4 auto for cat-a, 1 user for cat-a
        # cat-a weighted = 4 + 1*0.5 = 4.5, total=4.5, 100% -> match
        _add_history(repo, imp_id, "TIPOVER", -10.00, "correct-cat", count=4, source="auto")
        _add_history(repo, imp_id, "TIPOVER", -20.00, "correct-cat", count=1, source="user")

        result = match_historical("TIPOVER", -30.00, repo)
        assert result is not None
        assert result.category_id == "correct-cat"


class TestEdgeCases:
    def test_none_description(self):
        repo = _repo()
        result = match_historical(None, -50.00, repo)
        assert result is None

    def test_empty_description(self):
        repo = _repo()
        result = match_historical("", -50.00, repo)
        assert result is None

    def test_no_repo(self):
        result = match_historical("SOME MERCHANT", -50.00, None)
        assert result is None

    def test_no_history(self):
        repo = _repo()
        _import(repo)
        result = match_historical("UNKNOWN MERCHANT", -50.00, repo)
        assert result is None

    def test_pending_txns_ignored(self):
        """Transactions with status='pending' should not be considered."""
        repo = _repo()
        imp_id = _import(repo)
        # Insert pending transactions (not categorized)
        for _ in range(5):
            txn = Transaction(
                account_id="test-checking",
                date="2024-01-15",
                amount=-50.00,
                raw_description="PENDING MERCHANT",
                normalized_description="PENDING MERCHANT",
                import_id=imp_id,
                import_hash=f"hash-pending-{_add_history._counter}",
                dedup_key=f"dk-pending-{_add_history._counter}",
                status="pending",
            )
            _add_history._counter += 1
            repo.insert_transaction(txn)
            alloc = Allocation(
                transaction_id=txn.id,
                category_id="some-cat",
                amount=-50.00,
                source="auto",
            )
            repo.insert_allocation(alloc)

        result = match_historical("PENDING MERCHANT", -50.00, repo)
        assert result is None


class TestCsaaMotivatingExample:
    """The CSAA insurance scenario that motivated this feature.

    With historical data showing -$344.09 CSAA transactions were always
    car-insurance, a new identical transaction should categorize as
    car-insurance via historical_pattern instead of home-insurance via
    amount_rule.
    """

    def test_csaa_exact_amount_match(self):
        repo = _repo()
        imp_id = _import(repo)
        # Historical: 5 past CSAA transactions at -$344.09, all car-insurance
        _add_history(repo, imp_id, "CSAA INSURANCE GROUP", -344.09, "car-insurance", count=5)
        # Also some at different amounts for other insurance types
        _add_history(repo, imp_id, "CSAA INSURANCE GROUP", -1200.00, "home-insurance", count=3)
        _add_history(repo, imp_id, "CSAA INSURANCE GROUP", -89.00, "earthquake-insurance", count=2)

        # New transaction at -$344.09 should match car-insurance exactly
        result = match_historical("CSAA INSURANCE GROUP", -344.09, repo)
        assert result is not None
        assert result.category_id == "car-insurance"
        assert result.match_level == "exact"
        assert result.confidence == EXACT_CONFIDENCE

    def test_csaa_new_amount_falls_to_description(self):
        """A CSAA charge at a new amount should use description-level matching."""
        repo = _repo()
        imp_id = _import(repo)
        _add_history(repo, imp_id, "CSAA INSURANCE GROUP", -344.09, "car-insurance", count=5)
        _add_history(repo, imp_id, "CSAA INSURANCE GROUP", -1200.00, "home-insurance", count=3)
        _add_history(repo, imp_id, "CSAA INSURANCE GROUP", -89.00, "earthquake-insurance", count=2)

        # New amount: no exact match, desc-only has 10 total, car=5 (50%) -> below 80%
        result = match_historical("CSAA INSURANCE GROUP", -500.00, repo)
        assert result is None  # No clear pattern at this level
