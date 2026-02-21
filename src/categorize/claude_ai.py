"""Step 7: Claude AI categorization for unresolved transactions.

Sends individual transactions to Claude for category suggestions when
steps 1-6 of the pipeline have not found a match. Uses the same
claude_fn callback pattern as receipt_lookup for testability.

Monthly budget cap tracked via api_usage table.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from src.config import Config
from src.database.models import Transaction
from src.database.repository import Repository

logger = logging.getLogger(__name__)

# Monthly budget cap for Claude categorization (in cents) — $5/month
# Shared with receipt parsing budget
MONTHLY_BUDGET_CENTS = 500

# Cost estimate per Claude categorization call (in cents)
CLAUDE_CATEGORIZE_COST_CENTS = 2  # ~$0.02 per call


@dataclass
class ClaudeCategorizationResult:
    """Result from Claude AI categorization."""
    category_id: str
    confidence: float
    reasoning: str


def _build_category_list(config: Config) -> str:
    """Build a flat list of category IDs from config for the prompt."""
    ids: list[str] = []

    def _walk(nodes: list[dict]) -> None:
        for node in nodes:
            cat_id = node.get("id", "")
            if cat_id:
                ids.append(cat_id)
            children = node.get("children", [])
            if children:
                _walk(children)

    _walk(config.categories)
    return ", ".join(ids)


def categorize_single(
    txn: Transaction,
    config: Config,
    claude_fn,
    repo: Repository,
) -> ClaudeCategorizationResult | None:
    """Ask Claude to categorize a single transaction.

    Args:
        txn: Transaction to categorize.
        config: Application config (provides category list).
        claude_fn: Callable (system: str, prompt: str) -> str.
        repo: Repository for budget tracking.

    Returns:
        ClaudeCategorizationResult if Claude provided a valid suggestion,
        None if budget exceeded, parsing failed, or Claude couldn't categorize.
    """
    month = txn.date[:7] if txn.date else ""
    if not month:
        return None

    # Budget check
    current_cost = repo.get_monthly_cost(month)
    if current_cost >= MONTHLY_BUDGET_CENTS:
        logger.warning(
            "Monthly Claude budget exceeded (%d/%d cents), skipping categorization",
            current_cost, MONTHLY_BUDGET_CENTS,
        )
        return None

    category_list = _build_category_list(config)

    system_prompt = (
        "You are a personal finance categorizer. Given a bank transaction, "
        "assign it to the most appropriate category from the list provided. "
        "Return ONLY a JSON object with these fields:\n"
        '  - "category_id": the best matching category ID from the list\n'
        '  - "confidence": your confidence from 0.0 to 1.0\n'
        '  - "reasoning": brief explanation (one sentence)\n'
        "If you cannot determine a category, set category_id to \"uncategorized\" "
        "and confidence to 0.0.\n"
        "Return ONLY the JSON object, no other text."
    )

    user_prompt = (
        f"Transaction: {txn.raw_description}\n"
        f"Amount: ${abs(txn.amount):.2f}\n"
        f"Date: {txn.date}\n"
        f"Account: {txn.account_id}\n\n"
        f"Available categories: {category_list}"
    )

    try:
        response = claude_fn(system_prompt, user_prompt)

        # Track API usage
        repo.increment_api_usage(
            month, "claude_categorize",
            requests=1,
            cost_cents=CLAUDE_CATEGORIZE_COST_CENTS,
        )

        return _parse_response(response, config)
    except Exception:
        logger.exception("Claude categorization failed for txn %s", txn.id)
        return None


def _parse_response(response: str, config: Config) -> ClaudeCategorizationResult | None:
    """Parse Claude's JSON response into a categorization result."""
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [line for line in lines if not line.strip().startswith("```")]
        text = "\n".join(lines)

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logger.error("Failed to parse Claude categorization response: %s", text[:200])
        return None

    if not isinstance(data, dict):
        logger.error("Claude response is not a dict: %s", type(data))
        return None

    category_id = data.get("category_id", "")
    confidence = data.get("confidence", 0.0)
    reasoning = data.get("reasoning", "")

    if not category_id or category_id == "uncategorized":
        return None

    # Validate category_id exists in config
    valid_ids = _get_valid_category_ids(config)
    if category_id not in valid_ids:
        logger.warning(
            "Claude returned invalid category_id '%s', not in config", category_id
        )
        return None

    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 0.5

    # Clamp confidence
    confidence = max(0.0, min(1.0, confidence))

    return ClaudeCategorizationResult(
        category_id=category_id,
        confidence=confidence,
        reasoning=reasoning,
    )


def _get_valid_category_ids(config: Config) -> set[str]:
    """Build a set of all valid category IDs from config."""
    ids: set[str] = set()

    def _walk(nodes: list[dict]) -> None:
        for node in nodes:
            cat_id = node.get("id", "")
            if cat_id:
                ids.add(cat_id)
            children = node.get("children", [])
            if children:
                _walk(children)

    _walk(config.categories)
    return ids
