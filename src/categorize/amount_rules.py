"""Amount-based and account-based categorization rules.

Amount rules resolve ambiguous merchants (Apple.com/Bill, CSAA, Whole Foods)
by checking the transaction amount against known ranges.

Account rules assign default categories based on the originating account
(e.g., business account → business default, loan non-transfer → interest-fees).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.config import Config

logger = logging.getLogger(__name__)


@dataclass
class RuleMatch:
    """Result of an amount or account rule match."""
    category_id: str
    confidence: float
    method: str  # "amount_rule" or "account_rule"
    note: str | None = None


# ── Confidence mapping ────────────────────────────────────

_CONFIDENCE_MAP = {
    "high": 0.90,
    "medium": 0.75,
    "low": 0.60,
}


# ── Amount rules ──────────────────────────────────────────


def _is_whole_dollar(amount: float) -> bool:
    """Return True if the amount has no fractional cents."""
    return abs(amount - round(amount)) < 0.005


def match_amount_rule(
    description: str,
    amount: float,
    config: Config,
    account_id: str | None = None,
) -> RuleMatch | None:
    """Check if an ambiguous merchant can be resolved by amount.

    Rules are defined in rules.yaml under amount_rules. Each rule
    specifies a merchant pattern and a list of amount ranges with
    their corresponding category_ids.

    Some rules are scoped to specific accounts (via the 'accounts' key)
    and some use whole_dollar matching instead of amount ranges.
    """
    desc_upper = description.upper()
    amount_rules = config.rules.get("amount_rules", [])

    for rule_set in amount_rules:
        pattern = rule_set.get("merchant_pattern", "")
        # Skip empty patterns - they would match everything
        if not pattern:
            continue
        if pattern.upper() not in desc_upper:
            continue

        # Account scoping: skip rule set if it requires specific accounts
        allowed_accounts = rule_set.get("accounts")
        if allowed_accounts and (account_id is None or account_id not in allowed_accounts):
            continue

        for rule in rule_set.get("rules", []):
            # Whole-dollar check (e.g., Amazon participant compensation)
            if rule.get("whole_dollar"):
                if not _is_whole_dollar(amount):
                    continue
            elif "amount_range" in rule:
                lo, hi = rule["amount_range"]
                # Ranges are stored as [more_negative, less_negative]
                # e.g., [-6.99, -2.00] means amount between -6.99 and -2.00
                range_lo = min(lo, hi)
                range_hi = max(lo, hi)
                if not (range_lo <= amount <= range_hi):
                    continue
            else:
                continue

            category_id = rule.get("category_id")
            if not category_id:
                logger.warning(
                    "Amount rule missing category_id for pattern '%s'", pattern
                )
                continue

            conf_label = rule.get("confidence", "medium")
            confidence = _CONFIDENCE_MAP.get(conf_label, 0.75)
            return RuleMatch(
                category_id=category_id,
                confidence=confidence,
                method="amount_rule",
                note=rule.get("note"),
            )
    return None


# ── Account rules ─────────────────────────────────────────


def match_account_rule(
    account_id: str,
    config: Config,
) -> RuleMatch | None:
    """Check if an account has a default categorization rule.

    Account rules apply when no higher-priority rule matched. Configured
    in rules.yaml under account_rules.
    """
    account_rules = config.rules.get("account_rules", [])

    for rule in account_rules:
        if rule.get("account") != account_id:
            continue

        # Account has a non-transfer default (Golden1)
        if "non_transfer_category" in rule:
            return RuleMatch(
                category_id=rule["non_transfer_category"],
                confidence=0.80,
                method="account_rule",
                note=rule.get("note"),
            )

        # Account has a default category
        if "default_category" in rule:
            return RuleMatch(
                category_id=rule["default_category"],
                confidence=0.60,
                method="account_rule",
                note=rule.get("note"),
            )
    return None
