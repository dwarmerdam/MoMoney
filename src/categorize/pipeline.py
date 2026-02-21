"""Categorization pipeline: 9-step fallback chain.

Steps (in priority order):
1.  Transfer detection — known transfer patterns
2.  Merchant auto-match — 100% confidence merchants
2.5 Historical pattern match — past categorizations for same merchant
3.  Amount rules — disambiguate by transaction amount
4.  Account rules — default categories for specific accounts
5.  Merchant high-confidence — 80-99% confidence merchants
6.  Gmail receipt lookup — Apple/Amazon receipt matching
7.  Claude AI categorization — single-transaction AI suggestion
8.  Manual review — flag for user

Business account logic:
  Accounts with a category_filter in config have restricted categories.
  Merchant matches that resolve to incompatible categories are overridden
  to the account's default_category. Only categories matching the
  compatible_prefixes or compatible_ids pass through.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import logging

from src.categorize.amount_rules import match_account_rule, match_amount_rule
from src.categorize.claude_ai import categorize_single as claude_categorize_single
from src.categorize.historical import match_historical
from src.categorize.merchant_match import match_merchant_auto, match_merchant_high
from src.categorize.transfer_detect import (
    detect_interest,
    detect_transfer,
    detect_transfer_by_txn_type,
)
from src.config import Config
from src.database.models import Allocation, Transaction, Transfer
from src.database.repository import Repository

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from src.categorize.receipt_lookup import ReceiptLookup


# ── Category filter logic ─────────────────────────────────


def _is_compatible(category_id: str, cat_filter: dict) -> bool:
    """Return True if category_id is compatible with the account's filter."""
    prefixes = cat_filter.get("compatible_prefixes", ())
    if any(category_id.startswith(p) for p in prefixes):
        return True
    ids = cat_filter.get("compatible_ids", set())
    return category_id in ids


def _apply_filter(category_id: str, cat_filter: dict | None) -> str:
    """Apply account category filter.  Returns the original or overridden ID."""
    if cat_filter is None:
        return category_id
    if _is_compatible(category_id, cat_filter):
        return category_id
    return cat_filter.get("default_category", category_id)


@dataclass
class CategorizeResult:
    """Outcome of categorizing a single transaction."""
    category_id: str
    confidence: float
    method: str  # categorization_method value
    is_transfer: bool = False
    transfer_type: str | None = None
    from_account: str | None = None
    to_account: str | None = None
    _receipt_result: object = None  # ReceiptResult when method == "gmail_receipt"


def categorize_transaction(
    txn: Transaction,
    config: Config,
    receipt_lookup: ReceiptLookup | None = None,
    claude_fn=None,
    repo: Repository | None = None,
) -> CategorizeResult:
    """Run the 8-step categorization pipeline on a single transaction.

    Args:
        txn: Transaction to categorize.
        config: Application config.
        receipt_lookup: Optional ReceiptLookup for Step 6 (Gmail receipts).
            If None, Step 6 is skipped.
        claude_fn: Optional callable (system: str, prompt: str) -> str for
            Step 7 Claude AI categorization. If None, Step 7 is skipped.
        repo: Repository for Step 2.5 (historical patterns) and Step 7 (budget tracking).

    Returns the first matching result. Falls through to manual review
    if no step matches.

    Accounts with a category_filter in config will have incompatible
    merchant matches overridden to their default_category.
    """
    desc = txn.raw_description or ""
    cat_filter = config.category_filter_for(txn.account_id)
    transfer_cats = config.transfer_categories
    fallback = config.fallback_category

    # Step 1: Transfer detection (pattern-based, then txn_type fallback)
    transfer = detect_transfer(desc, txn.account_id, config)
    transfer_method = "transfer"
    if transfer is None:
        transfer = detect_transfer_by_txn_type(
            txn.txn_type, desc, txn.amount, txn.account_id, config
        )
        transfer_method = "transfer_inferred"
    if transfer is not None:
        cat_id = transfer_cats.get(transfer.transfer_type)
        if cat_id is None:
            # Fallback if transfer type not in config - use generic transfer category
            # or the configured fallback, with final safety fallback to "uncategorized"
            cat_id = transfer_cats.get("internal-transfer", fallback) or "uncategorized"
            logger.warning(
                "Transfer type '%s' not in transfer_categories config, using '%s'",
                transfer.transfer_type, cat_id,
            )
        return CategorizeResult(
            category_id=cat_id,
            confidence=1.0,
            method=transfer_method,
            is_transfer=True,
            transfer_type=transfer.transfer_type,
            from_account=transfer.from_account,
            to_account=transfer.to_account,
        )

    # Step 1b: Interest detection via FITID suffix (config-driven)
    interest = detect_interest(txn.external_id, txn.account_id, config)
    if interest is not None:
        return CategorizeResult(
            category_id=interest,
            confidence=1.0,
            method="interest_detection",
        )

    # Step 2: Merchant auto-match (100% confidence)
    merchant = match_merchant_auto(desc, config)
    if merchant is not None:
        cat_id = _apply_filter(merchant.category_id, cat_filter)
        return CategorizeResult(
            category_id=cat_id,
            confidence=merchant.confidence,
            method="merchant_auto",
        )

    # Step 2.5: Historical pattern matching
    if repo is not None and txn.normalized_description:
        hist = match_historical(txn.normalized_description, txn.amount, repo)
        if hist is not None:
            cat_id = _apply_filter(hist.category_id, cat_filter)
            return CategorizeResult(
                category_id=cat_id,
                confidence=hist.confidence,
                method="historical_pattern",
            )

    # Step 3: Amount-based rules (ambiguous merchants)
    amount_match = match_amount_rule(desc, txn.amount, config, account_id=txn.account_id)
    if amount_match is not None:
        cat_id = _apply_filter(amount_match.category_id, cat_filter)
        return CategorizeResult(
            category_id=cat_id,
            confidence=amount_match.confidence,
            method="amount_rule",
        )

    # Step 4: Account-based rules
    account_match = match_account_rule(txn.account_id, config)
    if account_match is not None:
        # Apply category filter for consistency with merchant rules
        cat_id = _apply_filter(account_match.category_id, cat_filter)
        return CategorizeResult(
            category_id=cat_id,
            confidence=account_match.confidence,
            method="account_rule",
        )

    # Step 5: Merchant high-confidence (80-99%)
    high_match = match_merchant_high(desc, config)
    if high_match is not None:
        cat_id = _apply_filter(high_match.category_id, cat_filter)
        return CategorizeResult(
            category_id=cat_id,
            confidence=high_match.confidence,
            method="merchant_high",
        )

    # Step 6: Gmail receipt lookup (Apple/Amazon)
    if receipt_lookup is not None:
        receipt_result = receipt_lookup.resolve(txn)
        if receipt_result is not None and receipt_result.matched:
            # Use first item's category, falling back if None or no items
            primary_cat = fallback
            if receipt_result.items:
                primary_cat = receipt_result.items[0].category_id or fallback
            # Apply category filter for consistency with other steps
            primary_cat = _apply_filter(primary_cat, cat_filter)
            return CategorizeResult(
                category_id=primary_cat,
                confidence=receipt_result.confidence,
                method="gmail_receipt",
                _receipt_result=receipt_result,
            )

    # Step 7: Claude AI categorization
    if claude_fn is not None and repo is not None:
        claude_result = claude_categorize_single(txn, config, claude_fn, repo)
        if claude_result is not None:
            cat_id = _apply_filter(claude_result.category_id, cat_filter)
            return CategorizeResult(
                category_id=cat_id,
                confidence=claude_result.confidence,
                method="claude_ai",
            )

    # Step 8: Manual review
    return CategorizeResult(
        category_id=fallback,
        confidence=0.0,
        method="manual_review",
    )


def apply_categorization(
    txn: Transaction,
    result: CategorizeResult,
    repo: Repository,
    receipt_lookup: ReceiptLookup | None = None,
) -> None:
    """Apply a categorization result: update transaction and create allocation.

    For gmail_receipt results with a ReceiptResult attached, delegates to
    ReceiptLookup.apply_result() to create split allocations.

    For transfer results, also attempts to find and link the counterpart
    transaction via find_transfer_candidates.
    """
    # Gmail receipt results create split allocations
    if result.method == "gmail_receipt" and result._receipt_result is not None:
        if receipt_lookup is not None:
            receipt_lookup.apply_result(txn, result._receipt_result)
            return

    status = "categorized" if result.method != "manual_review" else "flagged"

    # Insert allocation FIRST - if this fails, status remains unchanged
    allocation = Allocation(
        transaction_id=txn.id,
        category_id=result.category_id,
        amount=txn.amount,
        source="auto",
        confidence=result.confidence,
    )
    repo.insert_allocation(allocation)

    # Only update status AFTER allocation succeeds
    repo.update_transaction_status(
        txn.id,
        status=status,
        confidence=result.confidence,
        method=result.method,
    )

    # For transfers, try to find and link the counterpart transaction
    # Wrapped in try/except to prevent transfer link failures from breaking categorization
    if result.is_transfer and result.transfer_type:
        try:
            _try_link_transfer(txn, result, repo)
        except Exception as e:
            logger.warning("Failed to link transfer for %s: %s", txn.id, e)


def categorize_pending(
    repo: Repository,
    config: Config,
    limit: int = 100,
    receipt_lookup: ReceiptLookup | None = None,
    claude_fn=None,
) -> PipelineResult:
    """Categorize all pending transactions up to limit.

    Args:
        repo: Database repository.
        config: Application config.
        limit: Max transactions to process.
        receipt_lookup: Optional ReceiptLookup for Step 6.
        claude_fn: Optional callable for Step 7 Claude AI categorization.

    Returns summary counts of how many were categorized by each method.
    """
    pending = repo.get_uncategorized_transactions(limit=limit)
    method_counts: dict[str, int] = {}
    transfer_count = 0

    for txn in pending:
        result = categorize_transaction(
            txn, config, receipt_lookup=receipt_lookup,
            claude_fn=claude_fn, repo=repo,
        )
        apply_categorization(txn, result, repo, receipt_lookup=receipt_lookup)
        method_counts[result.method] = method_counts.get(result.method, 0) + 1
        if result.is_transfer:
            transfer_count += 1

    return PipelineResult(
        total=len(pending),
        method_counts=method_counts,
        transfer_count=transfer_count,
    )


@dataclass
class PipelineResult:
    """Summary of a pipeline run."""
    total: int
    method_counts: dict[str, int]
    transfer_count: int


def _try_link_transfer(
    txn: Transaction,
    result: CategorizeResult,
    repo: Repository,
) -> None:
    """Attempt to find and link the counterpart transaction for a transfer.

    Uses find_transfer_candidates to search for matching transactions
    (opposite amount, different account, within 5 days).
    """
    # Check if this transaction is already part of a transfer
    existing = repo.get_transfer_by_transaction(txn.id)
    if existing is not None:
        return  # Already linked

    # Find candidates for the counterpart
    candidates = repo.find_transfer_candidates(txn)
    if not candidates:
        logger.debug(
            "No transfer counterpart found for %s (%s)",
            txn.id, result.transfer_type,
        )
        return

    # Use the best match (closest date)
    counterpart = candidates[0]

    # Determine from/to direction:
    # 1. Use result.from_account/to_account if available and matching
    # 2. Otherwise fall back to amount sign (negative = outflow)
    if result.from_account and result.to_account:
        # Use the pattern-detected direction
        if txn.account_id == result.from_account:
            from_txn_id = txn.id
            to_txn_id = counterpart.id
        elif txn.account_id == result.to_account:
            from_txn_id = counterpart.id
            to_txn_id = txn.id
        else:
            # Account doesn't match pattern - fall back to amount sign
            if txn.amount < 0:
                from_txn_id = txn.id
                to_txn_id = counterpart.id
            else:
                from_txn_id = counterpart.id
                to_txn_id = txn.id
    else:
        # No pattern info - use amount sign
        # Negative = outflow (from this account), Positive = inflow (to this account)
        if txn.amount < 0:
            from_txn_id = txn.id
            to_txn_id = counterpart.id
        else:
            from_txn_id = counterpart.id
            to_txn_id = txn.id

    transfer = Transfer(
        from_transaction_id=from_txn_id,
        to_transaction_id=to_txn_id,
        transfer_type=result.transfer_type or "internal-transfer",
        match_method="pattern_auto_link",
        confidence=0.9,  # High confidence since pattern matched
    )
    repo.insert_transfer(transfer)
    logger.info(
        "Auto-linked transfer: %s → %s (%s)",
        from_txn_id, to_txn_id, result.transfer_type,
    )
