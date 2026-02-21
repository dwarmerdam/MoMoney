"""Merchant matching: maps transaction descriptions to categories.

Two tiers:
  - Auto (100% confidence): deterministic, safe to auto-assign
  - High-confidence (80-99%): auto-assign with default, flag exceptions
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.config import Config

logger = logging.getLogger(__name__)


@dataclass
class MerchantMatch:
    """Result of a merchant match."""
    category_id: str
    confidence: float
    merchant_name: str
    tier: str  # "auto" or "high_confidence"


def match_merchant_auto(
    description: str,
    config: Config,
) -> MerchantMatch | None:
    """Try to match against auto-assign merchants (100% confidence).

    Checks both raw and normalized descriptions. Match types:
      - contains: case-insensitive substring
      - exact: case-insensitive full string match
    """
    auto_merchants = config.merchants.get("auto", [])
    return _match_against_rules(description, auto_merchants, "auto", 1.0)


def match_merchant_high(
    description: str,
    config: Config,
) -> MerchantMatch | None:
    """Try to match against high-confidence merchants (80-99%).

    These are auto-assigned with the most common category, but may
    occasionally be wrong (e.g., Philz is usually coffee but sometimes
    groceries for beans).
    """
    high_merchants = config.merchants.get("high_confidence", [])
    return _match_against_rules(description, high_merchants, "high_confidence", None)


def _match_against_rules(
    description: str,
    rules: list[dict],
    tier: str,
    override_confidence: float | None,
) -> MerchantMatch | None:
    """Match a description against a list of merchant rules."""
    desc_upper = description.upper()

    for rule in rules:
        match_type = rule.get("match", "contains")
        pattern = rule.get("pattern", "")
        pattern_upper = pattern.upper()

        matched = False
        if match_type == "exact":
            matched = desc_upper == pattern_upper
        elif match_type == "contains":
            matched = pattern_upper in desc_upper

        if matched:
            category_id = rule.get("category_id")
            if not category_id:
                logger.warning(
                    "Merchant rule missing category_id for pattern '%s'", pattern
                )
                continue

            if override_confidence is not None:
                confidence = override_confidence
            else:
                confidence = rule.get("consistency", 85.0) / 100.0
            return MerchantMatch(
                category_id=category_id,
                confidence=confidence,
                merchant_name=rule.get("merchant_name", pattern),
                tier=tier,
            )
    return None
