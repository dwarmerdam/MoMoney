"""Receipt lookup: Step 6 of the categorization pipeline.

Searches Gmail for Apple and Amazon receipts, uses Claude to extract
line items, then matches them to bank transactions via subset-sum
(Apple) or shipment-total (Amazon) matching.

Gracefully falls through if Gmail is unavailable or no match is found.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from itertools import combinations
from typing import TYPE_CHECKING

from src.config import Config
from src.database.models import Allocation, ReceiptMatch, Transaction
from src.database.repository import Repository

if TYPE_CHECKING:
    from src.gmail.client import GmailClient

logger = logging.getLogger(__name__)

# Apple charges aggregate subscriptions; amounts can vary widely
_APPLE_PATTERNS = ("APPLE.COM/BILL",)

# Amazon charges: multiple description variants
_AMAZON_PATTERNS = ("AMAZON", "AMZN")

# Matching tolerances
APPLE_TOLERANCE = 1.00       # $1 absolute tolerance
AMAZON_TOLERANCE_PCT = 0.05  # 5% relative tolerance

# Monthly budget cap for Claude API (in cents) — $5/month
MONTHLY_BUDGET_CENTS = 500

# Cost estimate per Claude receipt parse call (in cents)
CLAUDE_RECEIPT_PARSE_COST_CENTS = 3  # ~$0.03 per call


@dataclass
class ReceiptItem:
    """A single item extracted from a receipt email."""
    name: str
    amount: float
    category_id: str | None = None


@dataclass
class ParsedReceipt:
    """Parsed result from a single receipt email."""
    items: list[ReceiptItem] = field(default_factory=list)
    order_total: float | None = None
    shipment_total: float | None = None


@dataclass
class ReceiptResult:
    """Outcome of receipt lookup for a single transaction."""
    matched: bool = False
    items: list[ReceiptItem] = field(default_factory=list)
    gmail_message_id: str | None = None
    match_type: str | None = None
    confidence: float = 0.0


class ReceiptLookup:
    """Step 6: Gmail receipt lookup for Apple/Amazon transactions.

    Triggers only when:
    - Description matches APPLE.COM/BILL or AMAZON/AMZN
    - Previous pipeline steps did not resolve with high confidence
    - Transaction is from bank import (not budget app historical)

    Args:
        gmail: GmailClient instance (or mock).
        repo: Database repository.
        claude_fn: Callable that sends a prompt to Claude and returns the
            response text. Signature: (system: str, prompt: str) -> str.
            This is injected for testability — no direct Anthropic dependency.
    """

    def __init__(
        self,
        gmail: GmailClient,
        repo: Repository,
        claude_fn=None,
        config: Config | None = None,
    ):
        self.gmail = gmail
        self.repo = repo
        self.claude_fn = claude_fn
        self.config = config
        self._parse_cache: dict[str, ParsedReceipt] = {}

    def resolve(self, txn: Transaction) -> ReceiptResult | None:
        """Orchestrate receipt lookup for a single transaction.

        Returns ReceiptResult if a match was found, None if not a candidate
        or no match found (allowing pipeline to continue).

        Side effect: sets txn.receipt_lookup_status in the database.
        """
        merchant_type = self._get_merchant_type(txn)
        if merchant_type is None:
            return None  # Not a candidate — status stays null

        # Validate date before making any API calls
        if not txn.date or len(txn.date) < 7:
            logger.warning("Transaction %s has invalid date: '%s'", txn.id, txn.date)
            return None

        month = txn.date[:7]

        # Search Gmail for receipt emails
        messages = self.gmail.search_receipts(
            merchant_type, txn.date, txn.amount,
        )

        # Handle None (error) vs empty list (no results)
        if messages is None:
            logger.warning("Gmail search error for %s (%s)", txn.id, merchant_type)
            self.repo.set_receipt_lookup_status(txn.id, "error")
            return None

        # Track Gmail API usage only on successful search
        self.repo.increment_api_usage(
            month, "gmail_search", requests=1,
        )

        if not messages:
            logger.info("No Gmail receipts found for %s (%s)", txn.id, merchant_type)
            self.repo.set_receipt_lookup_status(txn.id, "no_email")
            return None

        # Fetch email bodies
        email_bodies = []
        for msg in messages[:5]:  # Cap at 5 emails
            body = self.gmail.get_message_body(msg["id"])
            if body:
                email_bodies.append((msg["id"], body))

        if not email_bodies:
            self.repo.set_receipt_lookup_status(txn.id, "no_email")
            return None

        # Extract receipts via Claude (per-email)
        receipts = self._claude_extract_receipts(txn, email_bodies, month)
        if not receipts:
            # Distinguish budget exceeded from other failures
            if self.claude_fn is not None and self.repo.get_monthly_cost(month) >= MONTHLY_BUDGET_CENTS:
                self.repo.set_receipt_lookup_status(txn.id, "budget_exceeded")
            else:
                self.repo.set_receipt_lookup_status(txn.id, "no_match")
            return None

        # Match based on merchant type
        if merchant_type == "apple":
            all_items = []
            for parsed in receipts.values():
                all_items.extend(parsed.items)
            if not all_items:
                self.repo.set_receipt_lookup_status(txn.id, "no_match")
                return None
            result = self._resolve_apple(txn, all_items, email_bodies)
        elif merchant_type == "amazon":
            result = self._resolve_amazon(txn, receipts)
        else:
            result = None

        if result is not None and result.matched:
            self.repo.set_receipt_lookup_status(txn.id, "matched")
        else:
            self.repo.set_receipt_lookup_status(txn.id, "no_match")

        return result

    def apply_result(
        self, txn: Transaction, result: ReceiptResult,
    ) -> None:
        """Apply a successful receipt match: create allocations and record match.

        Args:
            txn: The transaction being categorized.
            result: The ReceiptResult from resolve().
        """
        if not result.matched or not result.items:
            return

        # Save receipt match record
        match_record = ReceiptMatch(
            transaction_id=txn.id,
            gmail_message_id=result.gmail_message_id or "",
            match_type=result.match_type or "",
            matched_items=json.dumps([
                {"name": it.name, "amount": it.amount, "category_id": it.category_id}
                for it in result.items
            ]),
            confidence=result.confidence,
        )
        self.repo.insert_receipt_match(match_record)

        # Check if transaction already has allocations to avoid duplicates
        existing_allocs = self.repo.get_allocations_by_transaction(txn.id)
        if existing_allocs:
            logger.warning(
                "Transaction %s already has %d allocation(s), skipping receipt allocation creation",
                txn.id, len(existing_allocs),
            )
            return

        # Create allocations for each item
        # Match the sign of the original transaction (negative for charges, positive for refunds)
        sign = -1 if txn.amount < 0 else 1
        for item in result.items:
            alloc = Allocation(
                transaction_id=txn.id,
                category_id=item.category_id or "uncategorized",
                amount=sign * abs(item.amount),
                memo=item.name,
                source="gmail_receipt",
                confidence=result.confidence,
            )
            self.repo.insert_allocation(alloc)

        # Update transaction status
        self.repo.update_transaction_status(
            txn.id,
            status="categorized",
            confidence=result.confidence,
            method="gmail_receipt",
        )

    # ── Candidate detection ──────────────────────────────

    @staticmethod
    def _get_merchant_type(txn: Transaction) -> str | None:
        """Determine if transaction is an Apple or Amazon candidate.

        Returns "apple", "amazon", or None.
        """
        desc = (txn.raw_description or "").upper()

        for pattern in _APPLE_PATTERNS:
            if pattern in desc:
                return "apple"

        for pattern in _AMAZON_PATTERNS:
            if pattern in desc:
                return "amazon"

        return None

    # ── Apple subset-sum matching ─────────────────────────

    def _resolve_apple(
        self,
        txn: Transaction,
        items: list[ReceiptItem],
        email_bodies: list[tuple[str, str]],
    ) -> ReceiptResult | None:
        """Find a subset of receipt items that sums to the charge amount.

        Uses brute-force subset-sum since Apple receipts typically have
        <10 items. Tolerance: $1.
        """
        charge = abs(txn.amount)
        item_amounts = [it.amount for it in items]

        # Try all subsets (2^n where n < 10 is tractable)
        for size in range(1, len(items) + 1):
            for combo in combinations(range(len(items)), size):
                subset_total = sum(item_amounts[i] for i in combo)
                if abs(subset_total - charge) <= APPLE_TOLERANCE:
                    matched_items = [items[i] for i in combo]
                    msg_id = email_bodies[0][0] if email_bodies else None
                    return ReceiptResult(
                        matched=True,
                        items=matched_items,
                        gmail_message_id=msg_id,
                        match_type="apple_subset_sum",
                        confidence=0.85,
                    )

        logger.info("Apple subset-sum: no match for $%.2f from %d items", charge, len(items))
        return None

    # ── Amazon per-email matching ─────────────────────────

    def _resolve_amazon(
        self,
        txn: Transaction,
        receipts: dict[str, ParsedReceipt],
    ) -> ReceiptResult | None:
        """Match charge to receipt emails using per-email matching.

        Matching phases (in priority order):
        1. Per-email shipment_total match (most precise — matches bank charge directly)
        2. Per-email order_total match
        3. Per-email item total or subset-sum
        4. Cross-email item subset-sum (last resort)
        """
        charge = abs(txn.amount)
        if charge == 0:
            return None

        # Phase 1: Per-email shipment_total match
        for msg_id, parsed in receipts.items():
            if parsed.shipment_total and parsed.shipment_total > 0:
                if abs(parsed.shipment_total - charge) / charge <= AMAZON_TOLERANCE_PCT:
                    return ReceiptResult(
                        matched=True,
                        items=parsed.items,
                        gmail_message_id=msg_id,
                        match_type="amazon_shipment_total",
                        confidence=0.90,
                    )

        # Phase 2: Per-email order_total match
        for msg_id, parsed in receipts.items():
            if parsed.order_total and parsed.order_total > 0:
                if abs(parsed.order_total - charge) / charge <= AMAZON_TOLERANCE_PCT:
                    return ReceiptResult(
                        matched=True,
                        items=parsed.items,
                        gmail_message_id=msg_id,
                        match_type="amazon_order_total",
                        confidence=0.85,
                    )

        # Phase 3: Per-email item total / subset-sum
        for msg_id, parsed in receipts.items():
            if not parsed.items:
                continue
            result = self._subset_sum_match(
                charge, parsed.items, msg_id,
            )
            if result:
                return result

        # Phase 4: Cross-email combined subset-sum (only if multiple emails)
        if len(receipts) > 1:
            all_items = []
            first_msg_id = None
            for msg_id, parsed in receipts.items():
                if first_msg_id is None:
                    first_msg_id = msg_id
                all_items.extend(parsed.items)
            if all_items:
                result = self._subset_sum_match(
                    charge, all_items, first_msg_id, confidence=0.75,
                )
                if result:
                    return result

        all_items = [it for p in receipts.values() for it in p.items]
        total = sum(it.amount for it in all_items)
        logger.info(
            "Amazon: no match for $%.2f from %d emails, %d items (total: $%.2f)",
            charge, len(receipts), len(all_items), total,
        )
        return None

    @staticmethod
    def _subset_sum_match(
        charge: float,
        items: list[ReceiptItem],
        msg_id: str | None,
        confidence: float = 0.80,
    ) -> ReceiptResult | None:
        """Try total-match then subset-sum on a list of items.

        Returns ReceiptResult on match, None otherwise.
        """
        item_total = sum(it.amount for it in items)

        # Fast path: all items sum to charge
        if item_total > 0 and abs(item_total - charge) / charge <= AMAZON_TOLERANCE_PCT:
            return ReceiptResult(
                matched=True,
                items=items,
                gmail_message_id=msg_id,
                match_type="amazon_shipment",
                confidence=confidence,
            )

        # Subset-sum
        item_amounts = [it.amount for it in items]
        for size in range(1, len(items) + 1):
            for combo in combinations(range(len(items)), size):
                subset_total = sum(item_amounts[i] for i in combo)
                if subset_total > 0 and abs(subset_total - charge) / charge <= AMAZON_TOLERANCE_PCT:
                    matched_items = [items[i] for i in combo]
                    return ReceiptResult(
                        matched=True,
                        items=matched_items,
                        gmail_message_id=msg_id,
                        match_type="amazon_subset_sum",
                        confidence=confidence,
                    )

        return None

    # ── Claude receipt extraction ─────────────────────────

    def _claude_extract_receipts(
        self,
        txn: Transaction,
        email_bodies: list[tuple[str, str]],
        month: str,
    ) -> dict[str, ParsedReceipt]:
        """Send email text to Claude for structured receipt extraction.

        Uses per-message caching to avoid re-parsing emails already seen
        in this session. Each unique Gmail message_id is parsed once and
        cached for subsequent transactions.

        Returns a dict mapping msg_id → ParsedReceipt.
        """
        if self.claude_fn is None:
            logger.warning("No Claude function configured for receipt parsing")
            return {}

        # Separate cached from uncached messages
        receipts: dict[str, ParsedReceipt] = {}
        uncached: list[tuple[str, str]] = []

        for msg_id, body in email_bodies:
            if msg_id in self._parse_cache:
                receipts[msg_id] = self._parse_cache[msg_id]
            else:
                uncached.append((msg_id, body))

        # If everything was cached, return immediately — no Claude call
        if not uncached:
            logger.info(
                "All %d receipt emails cached, skipping Claude for txn %s",
                len(email_bodies), txn.id,
            )
            return receipts

        # Budget check (only when we actually need Claude)
        current_cost = self.repo.get_monthly_cost(month)
        if current_cost >= MONTHLY_BUDGET_CENTS:
            logger.warning(
                "Monthly Claude budget exceeded (%d/%d cents), skipping receipt parse",
                current_cost, MONTHLY_BUDGET_CENTS,
            )
            return receipts  # Return whatever was cached

        # Build category instruction once
        if self.config and self.config.receipt_categories:
            cat_list = ", ".join(self.config.receipt_categories)
            category_instruction = f'    - "category_id": suggest a category from this list: {cat_list}\n'
        else:
            category_instruction = '    - "category_id": suggest a category like "subscription", "digital-purchase", "software", etc.\n'
            logger.warning("No receipt_categories configured in rules.yaml")

        system_prompt = (
            "You are a receipt parser. Extract information from the email receipt below. "
            "Return a JSON object with:\n"
            '  - "items": array of line items, each with:\n'
            '    - "name": item/subscription name\n'
            '    - "amount": price as a positive number (e.g., 9.99)\n'
            f"{category_instruction}"
            '  - "order_total": the order total from the email as a positive number, or null if not found\n'
            '  - "shipment_total": the shipment/charge total from the email as a positive number, or null if not found\n'
            "Return ONLY the JSON object, no other text."
        )

        valid_categories = set(self.config.receipt_categories) if self.config and self.config.receipt_categories else set()

        # Parse each uncached email individually for per-message caching
        for msg_id, body in uncached:
            try:
                user_prompt = (
                    f"Transaction: {txn.raw_description} for ${abs(txn.amount):.2f} "
                    f"on {txn.date}\n\n"
                    f"Email receipt:\n{body}"
                )

                response = self.claude_fn(system_prompt, user_prompt)

                # Track API usage per call
                self.repo.increment_api_usage(
                    month, "claude_receipt_parse",
                    requests=1,
                    cost_cents=CLAUDE_RECEIPT_PARSE_COST_CENTS,
                )

                parsed = _parse_claude_response(response)

                # Validate category_ids
                for item in parsed.items:
                    if item.category_id and valid_categories and item.category_id not in valid_categories:
                        logger.warning(
                            "Claude returned invalid category_id '%s' for item '%s', using 'uncategorized'",
                            item.category_id, item.name,
                        )
                        item.category_id = "uncategorized"

                # Cache result for this message
                self._parse_cache[msg_id] = parsed
                receipts[msg_id] = parsed

            except Exception:
                logger.exception(
                    "Claude receipt extraction failed for msg %s (txn %s)",
                    msg_id, txn.id,
                )
                # Cache empty receipt so we don't retry this message
                self._parse_cache[msg_id] = ParsedReceipt()

        return receipts


def _parse_claude_response(response: str) -> ParsedReceipt:
    """Parse Claude's JSON response into a ParsedReceipt.

    Handles two formats:
    - Legacy list format: [{name, amount, category_id}, ...]
    - New object format: {items: [...], order_total, shipment_total}
    """
    # Strip markdown code fences if present
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logger.error("Failed to parse Claude response as JSON: %s", text[:200])
        return ParsedReceipt()

    # Legacy list format (backward compat)
    if isinstance(data, list):
        return ParsedReceipt(items=_parse_items_list(data))

    if not isinstance(data, dict):
        logger.error("Claude response is not a list or object: %s", type(data))
        return ParsedReceipt()

    # New object format with items + totals
    items = _parse_items_list(data.get("items", []))

    order_total = data.get("order_total")
    if order_total is not None:
        try:
            order_total = float(order_total)
        except (TypeError, ValueError):
            order_total = None

    shipment_total = data.get("shipment_total")
    if shipment_total is not None:
        try:
            shipment_total = float(shipment_total)
        except (TypeError, ValueError):
            shipment_total = None

    return ParsedReceipt(
        items=items,
        order_total=order_total,
        shipment_total=shipment_total,
    )


def _parse_items_list(data: list) -> list[ReceiptItem]:
    """Parse a list of dicts into ReceiptItem list."""
    items = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", "")
        amount = entry.get("amount", 0)
        category_id = entry.get("category_id")
        if name and amount > 0:
            items.append(ReceiptItem(
                name=str(name),
                amount=float(amount),
                category_id=category_id,
            ))
    return items
