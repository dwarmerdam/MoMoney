"""Historical pattern matching for transaction categorization.

Queries past categorized transactions with the same normalized_description
and uses their category assignments to categorize new transactions. Two
match levels are tried in order:

1. Exact match (same description + same amount +-$0.01): requires >=2
   unanimous past matches -> confidence 0.95
2. Description-only match (same description, any amount): requires >=3
   past matches with >=80% agreement on one category -> confidence 0.85,
   with user-corrected allocations weighted 1.5x
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.database.repository import Repository

logger = logging.getLogger(__name__)

EXACT_MIN_COUNT = 2
EXACT_CONFIDENCE = 0.95
DESC_MIN_COUNT = 3
DESC_MIN_AGREEMENT = 0.80
DESC_CONFIDENCE = 0.85
USER_WEIGHT = 1.5
AMOUNT_TOLERANCE = 0.01


@dataclass
class HistoricalMatch:
    """Result of a historical pattern match."""
    category_id: str
    confidence: float
    match_level: str  # "exact" or "description"
    match_count: int
    agreement_pct: float


def match_historical(
    normalized_description: str | None,
    amount: float,
    repo: Repository | None,
) -> HistoricalMatch | None:
    """Find a historical categorization pattern for a transaction.

    Returns a HistoricalMatch if past transactions with the same
    normalized_description show a clear pattern, else None.
    """
    if not normalized_description or repo is None:
        return None

    rows = repo.get_historical_category_counts(normalized_description)
    if not rows:
        return None

    # --- Level 1: Exact match (same description + same amount) ---
    exact_rows = [
        r for r in rows
        if abs(r["amount"] - amount) <= AMOUNT_TOLERANCE
    ]
    if exact_rows:
        total = sum(r["cnt"] for r in exact_rows)
        if total >= EXACT_MIN_COUNT:
            # Check unanimity: all exact rows must agree on one category
            categories = {r["category_id"] for r in exact_rows}
            if len(categories) == 1:
                cat = exact_rows[0]["category_id"]
                logger.debug(
                    "Historical exact match: %s -> %s (%d matches)",
                    normalized_description, cat, total,
                )
                return HistoricalMatch(
                    category_id=cat,
                    confidence=EXACT_CONFIDENCE,
                    match_level="exact",
                    match_count=total,
                    agreement_pct=1.0,
                )

    # --- Level 2: Description-only match (any amount) ---
    total_weighted = sum(
        r["cnt"] + r["user_cnt"] * (USER_WEIGHT - 1)
        for r in rows
    )
    total_count = sum(r["cnt"] for r in rows)
    if total_count < DESC_MIN_COUNT:
        return None

    # Aggregate weighted votes by category
    cat_weights: dict[str, float] = {}
    for r in rows:
        w = r["cnt"] + r["user_cnt"] * (USER_WEIGHT - 1)
        cat_weights[r["category_id"]] = cat_weights.get(r["category_id"], 0) + w

    best_cat = max(cat_weights, key=cat_weights.get)
    best_weight = cat_weights[best_cat]
    agreement = best_weight / total_weighted if total_weighted > 0 else 0

    if agreement >= DESC_MIN_AGREEMENT:
        logger.debug(
            "Historical description match: %s -> %s (%.0f%% of %d)",
            normalized_description, best_cat, agreement * 100, total_count,
        )
        return HistoricalMatch(
            category_id=best_cat,
            confidence=DESC_CONFIDENCE,
            match_level="description",
            match_count=total_count,
            agreement_pct=agreement,
        )

    return None
