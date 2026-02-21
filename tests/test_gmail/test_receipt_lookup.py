"""Tests for the receipt lookup module.

All tests use mock Gmail and Claude — no live APIs needed.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.categorize.receipt_lookup import (
    AMAZON_TOLERANCE_PCT,
    APPLE_TOLERANCE,
    MONTHLY_BUDGET_CENTS,
    ParsedReceipt,
    ReceiptItem,
    ReceiptLookup,
    ReceiptResult,
    _parse_claude_response,
)
from src.database.models import Allocation, Import, Transaction
from src.database.repository import Repository

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture
def repo():
    r = Repository(":memory:")
    r.apply_migrations(MIGRATIONS_DIR)
    yield r
    r.close()


@pytest.fixture
def imp(repo):
    return repo.insert_import(Import(file_name="test.qfx", file_hash="testhash"))


def _txn(imp_id, **kw) -> Transaction:
    defaults = dict(
        account_id="wf-checking", date="2026-01-15", amount=-45.97,
        raw_description="APPLE.COM/BILL", import_id=imp_id,
        import_hash="h1", dedup_key="dk1",
    )
    defaults.update(kw)
    return Transaction(**defaults)


def _mock_gmail(messages=None, bodies=None):
    """Create a mock GmailClient."""
    gmail = MagicMock()
    gmail.search_receipts.return_value = messages or []
    if bodies:
        gmail.get_message_body.side_effect = bodies
    else:
        gmail.get_message_body.return_value = ""
    return gmail


def _mock_claude_fn(response: str):
    """Create a mock Claude function that returns fixed response."""
    return MagicMock(return_value=response)


# ── Candidate detection tests ─────────────────────────────


class TestGetMerchantType:
    def test_apple_bill(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-45.97,
            raw_description="APPLE.COM/BILL", import_id="i1",
            import_hash="h1", dedup_key="dk1",
        )
        assert ReceiptLookup._get_merchant_type(txn) == "apple"

    def test_amazon_description(self):
        txn = Transaction(
            account_id="cap1-credit", date="2026-01-15", amount=-170.29,
            raw_description="AMAZON.COM*AB1234", import_id="i1",
            import_hash="h1", dedup_key="dk1",
        )
        assert ReceiptLookup._get_merchant_type(txn) == "amazon"

    def test_amzn_variant(self):
        txn = Transaction(
            account_id="cap1-credit", date="2026-01-15", amount=-25.00,
            raw_description="AMZN Mktp US*XY9876", import_id="i1",
            import_hash="h1", dedup_key="dk1",
        )
        assert ReceiptLookup._get_merchant_type(txn) == "amazon"

    def test_non_candidate(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-50.00,
            raw_description="DOORDASH*ORDER 12345", import_id="i1",
            import_hash="h1", dedup_key="dk1",
        )
        assert ReceiptLookup._get_merchant_type(txn) is None

    def test_case_insensitive(self):
        txn = Transaction(
            account_id="wf-checking", date="2026-01-15", amount=-9.99,
            raw_description="apple.com/bill", import_id="i1",
            import_hash="h1", dedup_key="dk1",
        )
        assert ReceiptLookup._get_merchant_type(txn) == "apple"


# ── Claude response parsing tests ─────────────────────────


class TestParseClaudeResponse:
    def test_legacy_json_array(self):
        """Legacy list format still works (backward compat)."""
        response = json.dumps([
            {"name": "YouTube Premium", "amount": 22.99, "category_id": "youtube-premium"},
            {"name": "iCloud Storage", "amount": 9.99, "category_id": "cloud-storage"},
        ])
        parsed = _parse_claude_response(response)
        assert len(parsed.items) == 2
        assert parsed.items[0].name == "YouTube Premium"
        assert parsed.items[0].amount == 22.99
        assert parsed.items[0].category_id == "youtube-premium"
        assert parsed.order_total is None
        assert parsed.shipment_total is None

    def test_new_dict_format_with_totals(self):
        """New dict format with items + order/shipment totals."""
        response = json.dumps({
            "items": [
                {"name": "Insta360 Camera", "amount": 170.29, "category_id": "electronics-d"},
            ],
            "order_total": 170.29,
            "shipment_total": 170.29,
        })
        parsed = _parse_claude_response(response)
        assert len(parsed.items) == 1
        assert parsed.items[0].name == "Insta360 Camera"
        assert parsed.order_total == 170.29
        assert parsed.shipment_total == 170.29

    def test_dict_format_null_totals(self):
        """Dict format with null totals."""
        response = json.dumps({
            "items": [{"name": "Widget", "amount": 9.99}],
            "order_total": None,
            "shipment_total": None,
        })
        parsed = _parse_claude_response(response)
        assert len(parsed.items) == 1
        assert parsed.order_total is None
        assert parsed.shipment_total is None

    def test_code_fenced_json(self):
        response = '```json\n[{"name": "Netflix", "amount": 15.99, "category_id": "netflix"}]\n```'
        parsed = _parse_claude_response(response)
        assert len(parsed.items) == 1
        assert parsed.items[0].name == "Netflix"

    def test_invalid_json(self):
        parsed = _parse_claude_response("not json at all")
        assert parsed.items == []

    def test_non_list_non_dict_response(self):
        parsed = _parse_claude_response('"just a string"')
        assert parsed.items == []

    def test_skips_invalid_entries(self):
        response = json.dumps([
            {"name": "Good Item", "amount": 9.99},
            {"name": "", "amount": 5.00},       # empty name
            {"name": "Zero", "amount": 0},       # zero amount
            "not a dict",                         # invalid entry
        ])
        parsed = _parse_claude_response(response)
        assert len(parsed.items) == 1
        assert parsed.items[0].name == "Good Item"

    def test_category_id_optional(self):
        response = json.dumps([
            {"name": "Item", "amount": 12.99},
        ])
        parsed = _parse_claude_response(response)
        assert len(parsed.items) == 1
        assert parsed.items[0].category_id is None

    def test_invalid_total_types_ignored(self):
        """Non-numeric totals are set to None."""
        response = json.dumps({
            "items": [{"name": "Item", "amount": 10.00}],
            "order_total": "not a number",
            "shipment_total": {"nested": "object"},
        })
        parsed = _parse_claude_response(response)
        assert len(parsed.items) == 1
        assert parsed.order_total is None
        assert parsed.shipment_total is None


# ── Apple subset-sum matching tests ───────────────────────


class TestResolveApple:
    def test_exact_single_item_match(self, repo, imp):
        """Single item that exactly matches charge amount."""
        txn = _txn(imp.id, amount=-22.99)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [ReceiptItem("YouTube Premium", 22.99, "youtube-premium")]
        result = lookup._resolve_apple(txn, items, [("msg1", "body")])

        assert result is not None
        assert result.matched is True
        assert result.match_type == "apple_subset_sum"
        assert len(result.items) == 1
        assert result.items[0].name == "YouTube Premium"

    def test_subset_sum_multiple_items(self, repo, imp):
        """Multiple items summing to charge within tolerance."""
        txn = _txn(imp.id, amount=-45.97)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("YouTube Premium", 22.99, "youtube-premium"),
            ReceiptItem("iCloud Storage", 9.99, "cloud-storage"),
            ReceiptItem("Apple TV+", 12.99, "apple-tv"),
            ReceiptItem("Unrelated Item", 5.99, "other-software-d"),
        ]
        # 22.99 + 9.99 + 12.99 = 45.97
        result = lookup._resolve_apple(txn, items, [("msg1", "body")])

        assert result is not None
        assert result.matched is True
        assert len(result.items) == 3
        matched_names = {it.name for it in result.items}
        assert "YouTube Premium" in matched_names
        assert "iCloud Storage" in matched_names
        assert "Apple TV+" in matched_names

    def test_tolerance_within_one_dollar(self, repo, imp):
        """Items sum within $1 tolerance still match."""
        txn = _txn(imp.id, amount=-23.50)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [ReceiptItem("YouTube Premium", 22.99, "youtube-premium")]
        # 23.50 - 22.99 = 0.51, within $1 tolerance
        result = lookup._resolve_apple(txn, items, [("msg1", "body")])

        assert result is not None
        assert result.matched is True

    def test_no_matching_subset(self, repo, imp):
        """No subset sums to charge → returns None."""
        txn = _txn(imp.id, amount=-100.00)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Item A", 22.99, "youtube-premium"),
            ReceiptItem("Item B", 9.99, "cloud-storage"),
        ]
        result = lookup._resolve_apple(txn, items, [("msg1", "body")])
        assert result is None

    def test_prefers_smaller_subset(self, repo, imp):
        """Should find the smallest subset first (size 1 before size 2)."""
        txn = _txn(imp.id, amount=-9.99)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("iCloud Storage", 9.99, "cloud-storage"),
            ReceiptItem("Tiny", 4.99, "other-software-d"),
            ReceiptItem("Also Tiny", 5.00, "other-software-d"),
        ]
        # Could match [0] alone or [1]+[2]
        result = lookup._resolve_apple(txn, items, [("msg1", "body")])

        assert result is not None
        assert len(result.items) == 1
        assert result.items[0].name == "iCloud Storage"


# ── Amazon matching tests (using receipts dict) ──────────


class TestResolveAmazon:
    def test_exact_shipment_match(self, repo, imp):
        """Items total matches charge exactly (per-email)."""
        txn = _txn(
            imp.id, amount=-170.29,
            raw_description="AMAZON.COM*AB1234",
        )
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Cat Food", 45.29, "pet-supplies"),
            ReceiptItem("Dog Bed", 89.00, "pet-supplies"),
            ReceiptItem("USB Cable", 36.00, "electronics-d"),
        ]
        # 45.29 + 89.00 + 36.00 = 170.29
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_shipment"
        assert len(result.items) == 3

    def test_within_five_percent_tolerance(self, repo, imp):
        """Items total within 5% of charge still matches."""
        txn = _txn(
            imp.id, amount=-100.00,
            raw_description="AMAZON.COM*ORDER",
        )
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Widget", 97.00, "household-supplies"),
        ]
        # 3% difference → within 5% tolerance
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)
        assert result is not None
        assert result.matched is True

    def test_outside_tolerance(self, repo, imp):
        """Items total more than 5% off → no match."""
        txn = _txn(
            imp.id, amount=-100.00,
            raw_description="AMAZON.COM*ORDER",
        )
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Widget", 80.00, "household-supplies"),
        ]
        # 20% difference → outside tolerance
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)
        assert result is None

    def test_zero_charge_returns_none(self, repo, imp):
        """Zero-amount charge returns None (division by zero guard)."""
        txn = _txn(imp.id, amount=0, raw_description="AMAZON.COM*FREE")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [ReceiptItem("Free Item", 0, "household-supplies")]
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)
        assert result is None


# ── End-to-end resolve tests ─────────────────────────────


class TestResolveOrchestration:
    def test_apple_end_to_end(self, repo, imp):
        """Full resolve flow for Apple: search → fetch → Claude → match."""
        txn = _txn(imp.id, amount=-22.99)
        repo.insert_transaction(txn)

        claude_response = json.dumps([
            {"name": "YouTube Premium", "amount": 22.99, "category_id": "youtube-premium"},
        ])

        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Your receipt from Apple.\nYouTube Premium $22.99"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        result = lookup.resolve(txn)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "apple_subset_sum"
        assert result.items[0].category_id == "youtube-premium"

    def test_amazon_end_to_end(self, repo, imp):
        """Full resolve flow for Amazon."""
        txn = _txn(
            imp.id, amount=-25.99,
            raw_description="AMAZON.COM*AB5678",
        )
        repo.insert_transaction(txn)

        claude_response = json.dumps([
            {"name": "Phone Case", "amount": 25.99, "category_id": "electronics-d"},
        ])

        gmail = _mock_gmail(
            messages=[{"id": "msg2", "threadId": "t2"}],
            bodies=["Your Amazon.com order\nPhone Case $25.99"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        result = lookup.resolve(txn)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_shipment"

    def test_amazon_end_to_end_with_totals(self, repo, imp):
        """Full resolve flow for Amazon with new dict format including totals."""
        txn = _txn(
            imp.id, amount=-170.29,
            raw_description="AMAZON.COM*AB1234",
        )
        repo.insert_transaction(txn)

        claude_response = json.dumps({
            "items": [
                {"name": "Insta360 Camera", "amount": 170.29, "category_id": "electronics-d"},
            ],
            "order_total": 170.29,
            "shipment_total": 170.29,
        })

        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Your Amazon.com order\nInsta360 Camera $170.29"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        result = lookup.resolve(txn)

        assert result is not None
        assert result.matched is True
        # shipment_total match takes priority (Phase 1)
        assert result.match_type == "amazon_shipment_total"
        assert result.confidence == 0.90

    def test_non_candidate_returns_none(self, repo, imp):
        """Non-Apple/Amazon transaction returns None immediately."""
        txn = _txn(imp.id, raw_description="DOORDASH*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)
        result = lookup.resolve(txn)

        assert result is None
        gmail.search_receipts.assert_not_called()

    def test_no_gmail_results_returns_none(self, repo, imp):
        """Gmail search returns empty → returns None."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = _mock_gmail(messages=[])
        lookup = ReceiptLookup(gmail, repo)
        result = lookup.resolve(txn)

        assert result is None

    def test_gmail_search_failure_returns_none(self, repo, imp):
        """Gmail API failure → graceful fallthrough."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = MagicMock()
        gmail.search_receipts.return_value = None  # Simulates caught exception (error)
        lookup = ReceiptLookup(gmail, repo)
        result = lookup.resolve(txn)

        assert result is None

    def test_no_claude_fn_returns_none(self, repo, imp):
        """No Claude function configured → returns None."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Receipt text"],
        )
        lookup = ReceiptLookup(gmail, repo, claude_fn=None)
        result = lookup.resolve(txn)

        assert result is None


# ── API usage tracking tests ──────────────────────────────


class TestApiUsageTracking:
    def test_gmail_search_tracked(self, repo, imp):
        """Gmail search API call is tracked."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = _mock_gmail(messages=[])
        lookup = ReceiptLookup(gmail, repo)
        lookup.resolve(txn)

        # Check api_usage was incremented
        row = repo.conn.execute(
            "SELECT request_count FROM api_usage"
            " WHERE month = '2026-01' AND service = 'gmail_search'"
        ).fetchone()
        assert row is not None
        assert row[0] == 1

    def test_claude_parse_tracked(self, repo, imp):
        """Claude receipt parse API call is tracked."""
        txn = _txn(imp.id, amount=-22.99)
        repo.insert_transaction(txn)

        claude_response = json.dumps([
            {"name": "YouTube Premium", "amount": 22.99, "category_id": "youtube-premium"},
        ])
        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Receipt text"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        lookup.resolve(txn)

        row = repo.conn.execute(
            "SELECT request_count FROM api_usage"
            " WHERE month = '2026-01' AND service = 'claude_receipt_parse'"
        ).fetchone()
        assert row is not None
        assert row[0] == 1

    def test_budget_exceeded_skips_claude(self, repo, imp):
        """When monthly budget is exceeded, Claude is not called."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        # Set cost above budget
        repo.increment_api_usage("2026-01", "claude_receipt_parse",
                                 cost_cents=MONTHLY_BUDGET_CENTS + 1)

        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Receipt text"],
        )
        claude_fn = _mock_claude_fn("[]")
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        result = lookup.resolve(txn)

        assert result is None
        claude_fn.assert_not_called()


# ── Apply result tests ────────────────────────────────────


class TestApplyResult:
    def test_creates_allocations_and_receipt_match(self, repo, imp):
        """Applying result creates allocations and receipt match record."""
        txn = _txn(imp.id, amount=-45.97)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        result = ReceiptResult(
            matched=True,
            items=[
                ReceiptItem("YouTube Premium", 22.99, "youtube-premium"),
                ReceiptItem("iCloud Storage", 9.99, "cloud-storage"),
                ReceiptItem("Apple TV+", 12.99, "apple-tv"),
            ],
            gmail_message_id="msg1",
            match_type="apple_subset_sum",
            confidence=0.85,
        )
        lookup.apply_result(txn, result)

        # Check allocations created
        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 3
        cats = {a.category_id for a in allocs}
        assert "youtube-premium" in cats
        assert "cloud-storage" in cats
        assert "apple-tv" in cats
        # All amounts should be negative
        for a in allocs:
            assert a.amount < 0
            assert a.source == "gmail_receipt"

        # Check receipt match saved
        row = repo.conn.execute(
            "SELECT * FROM receipt_matches WHERE transaction_id = ?",
            (txn.id,),
        ).fetchone()
        assert row is not None
        assert row["match_type"] == "apple_subset_sum"
        assert row["gmail_message_id"] == "msg1"

        # Check transaction status updated
        updated = repo.get_transaction(txn.id)
        assert updated.status == "categorized"
        assert updated.categorization_method == "gmail_receipt"

    def test_unmatched_result_is_noop(self, repo, imp):
        """Applying unmatched result does nothing."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        result = ReceiptResult(matched=False)
        lookup.apply_result(txn, result)

        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 0

    def test_items_without_category_use_uncategorized(self, repo, imp):
        """Items with no category_id default to 'uncategorized'."""
        txn = _txn(imp.id, amount=-15.00)
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        result = ReceiptResult(
            matched=True,
            items=[ReceiptItem("Unknown Item", 15.00, None)],
            gmail_message_id="msg1",
            match_type="amazon_shipment",
            confidence=0.80,
        )
        lookup.apply_result(txn, result)

        allocs = repo.get_allocations_by_transaction(txn.id)
        assert len(allocs) == 1
        assert allocs[0].category_id == "uncategorized"


# ── Receipt result structure tests ────────────────────────


class TestReceiptResult:
    def test_default_values(self):
        r = ReceiptResult()
        assert r.matched is False
        assert r.items == []
        assert r.gmail_message_id is None
        assert r.confidence == 0.0

    def test_receipt_item_fields(self):
        item = ReceiptItem("Test", 9.99, "netflix")
        assert item.name == "Test"
        assert item.amount == 9.99
        assert item.category_id == "netflix"

    def test_parsed_receipt_fields(self):
        pr = ParsedReceipt(
            items=[ReceiptItem("Item", 10.00)],
            order_total=10.00,
            shipment_total=10.00,
        )
        assert len(pr.items) == 1
        assert pr.order_total == 10.00
        assert pr.shipment_total == 10.00

    def test_parsed_receipt_defaults(self):
        pr = ParsedReceipt()
        assert pr.items == []
        assert pr.order_total is None
        assert pr.shipment_total is None


# ── Claude parse cache tests ────────────────────────────


class TestClaudeParseCache:
    def test_cache_hit_avoids_claude_call(self, repo, imp):
        """Second resolve for same emails should not call Claude again."""
        txn1 = _txn(imp.id, amount=-22.99, import_hash="h1", dedup_key="dk1")
        repo.insert_transaction(txn1)
        txn2 = _txn(imp.id, amount=-22.99, import_hash="h2", dedup_key="dk2")
        repo.insert_transaction(txn2)

        claude_response = json.dumps([
            {"name": "YouTube Premium", "amount": 22.99, "category_id": "youtube-premium"},
        ])

        gmail = MagicMock()
        gmail.search_receipts.return_value = [{"id": "msg1", "threadId": "t1"}]
        gmail.get_message_body.return_value = "Your receipt from Apple.\nYouTube Premium $22.99"
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)

        # First call — Claude should be called
        result1 = lookup.resolve(txn1)
        assert result1 is not None
        assert result1.matched is True
        assert claude_fn.call_count == 1

        # Second call — same email, should hit cache
        result2 = lookup.resolve(txn2)
        assert result2 is not None
        assert result2.matched is True
        assert claude_fn.call_count == 1  # No additional Claude call

    def test_partial_cache_hit(self, repo, imp):
        """When some emails are cached and some are not, only uncached get Claude call."""
        txn1 = _txn(imp.id, amount=-22.99, import_hash="h1", dedup_key="dk1")
        repo.insert_transaction(txn1)
        txn2 = _txn(imp.id, amount=-32.98, import_hash="h2", dedup_key="dk2")
        repo.insert_transaction(txn2)

        response1 = json.dumps([
            {"name": "YouTube Premium", "amount": 22.99, "category_id": "youtube-premium"},
        ])
        response2 = json.dumps([
            {"name": "iCloud Storage", "amount": 9.99, "category_id": "cloud-storage"},
        ])

        gmail = MagicMock()
        # First search returns 1 email, second returns 2 (one overlapping)
        gmail.search_receipts.side_effect = [
            [{"id": "msg1", "threadId": "t1"}],
            [{"id": "msg1", "threadId": "t1"}, {"id": "msg2", "threadId": "t2"}],
        ]
        gmail.get_message_body.side_effect = [
            "Receipt 1",    # txn1's msg1
            "Receipt 1",    # txn2's msg1 (fetched but Claude not called)
            "Receipt 2",    # txn2's msg2
        ]

        claude_fn = MagicMock(side_effect=[response1, response2])
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)

        # First call: msg1 uncached → Claude called once
        lookup.resolve(txn1)
        assert claude_fn.call_count == 1

        # Second call: msg1 cached, msg2 uncached → Claude called once more
        lookup.resolve(txn2)
        assert claude_fn.call_count == 2

    def test_cache_persists_across_resolves(self, repo, imp):
        """Cache on the ReceiptLookup instance survives across multiple resolve() calls."""
        claude_response = json.dumps([
            {"name": "Item", "amount": 10.00, "category_id": "electronics-d"},
        ])
        gmail = MagicMock()
        gmail.search_receipts.return_value = [{"id": "msg1", "threadId": "t1"}]
        gmail.get_message_body.return_value = "Receipt text"
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)

        for i in range(3):
            txn = _txn(
                imp.id, amount=-10.00, import_hash=f"h{i}", dedup_key=f"dk{i}",
                raw_description="AMAZON.COM*ORDER",
            )
            repo.insert_transaction(txn)
            lookup.resolve(txn)

        assert claude_fn.call_count == 1  # Only the first call hit Claude
        assert "msg1" in lookup._parse_cache


# ── Amazon subset-sum matching tests ─────────────────────


class TestAmazonSubsetSum:
    def test_subset_match_partial_shipment(self, repo, imp):
        """A subset of items matches the charge (partial shipment)."""
        txn = _txn(imp.id, amount=-45.29, raw_description="AMAZON.COM*AB1234")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Cat Food", 45.29, "pet-supplies"),
            ReceiptItem("Dog Bed", 89.00, "pet-supplies"),
            ReceiptItem("USB Cable", 36.00, "electronics-d"),
        ]
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_subset_sum"
        assert len(result.items) == 1
        assert result.items[0].name == "Cat Food"

    def test_subset_sum_multiple_items(self, repo, imp):
        """Multiple items summing to charge within tolerance."""
        txn = _txn(imp.id, amount=-81.29, raw_description="AMAZON.COM*AB1234")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Cat Food", 45.29, "pet-supplies"),
            ReceiptItem("USB Cable", 36.00, "electronics-d"),
            ReceiptItem("Dog Bed", 89.00, "pet-supplies"),
        ]
        # 45.29 + 36.00 = 81.29 exact match
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_subset_sum"
        assert len(result.items) == 2
        matched_names = {it.name for it in result.items}
        assert "Cat Food" in matched_names
        assert "USB Cable" in matched_names

    def test_total_match_preferred_over_subset(self, repo, imp):
        """When all items sum to charge, prefer total match (amazon_shipment)."""
        txn = _txn(imp.id, amount=-170.29, raw_description="AMAZON.COM*AB1234")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Cat Food", 45.29, "pet-supplies"),
            ReceiptItem("Dog Bed", 89.00, "pet-supplies"),
            ReceiptItem("USB Cable", 36.00, "electronics-d"),
        ]
        # 45.29 + 89.00 + 36.00 = 170.29 (total match)
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_shipment"
        assert len(result.items) == 3

    def test_subset_within_five_percent(self, repo, imp):
        """Subset match within 5% relative tolerance."""
        txn = _txn(imp.id, amount=-100.00, raw_description="AMAZON.COM*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Widget", 97.00, "household-supplies"),
            ReceiptItem("Gadget", 200.00, "electronics-d"),
        ]
        # 97.00 vs 100.00 = 3% off, within 5% tolerance
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_subset_sum"
        assert len(result.items) == 1
        assert result.items[0].name == "Widget"

    def test_no_matching_subset(self, repo, imp):
        """No subset within tolerance returns None."""
        txn = _txn(imp.id, amount=-100.00, raw_description="AMAZON.COM*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("Widget", 80.00, "household-supplies"),
            ReceiptItem("Gadget", 50.00, "electronics-d"),
        ]
        # 80 (20% off), 50 (50% off), 130 (30% off) — none within 5%
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)
        assert result is None

    def test_prefers_smaller_subset(self, repo, imp):
        """Should find smallest matching subset first."""
        txn = _txn(imp.id, amount=-36.00, raw_description="AMAZON.COM*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        items = [
            ReceiptItem("USB Cable", 36.00, "electronics-d"),
            ReceiptItem("Thing A", 18.00, "household-supplies"),
            ReceiptItem("Thing B", 18.00, "household-supplies"),
        ]
        # Could match [0] alone or [1]+[2]. Should prefer single item.
        receipts = {"msg1": ParsedReceipt(items=items)}
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert len(result.items) == 1
        assert result.items[0].name == "USB Cable"


# ── Amazon per-email matching tests ──────────────────────


class TestAmazonPerEmailMatching:
    def test_shipment_total_match(self, repo, imp):
        """Phase 1: shipment_total in email matches charge → highest confidence."""
        txn = _txn(imp.id, amount=-35.26, raw_description="AMAZON.COM*AB1234")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        receipts = {
            "msg1": ParsedReceipt(
                items=[
                    ReceiptItem("Book", 15.26, "books"),
                    ReceiptItem("Pen Set", 20.00, "office-supplies"),
                ],
                order_total=50.00,
                shipment_total=35.26,
            ),
        }
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_shipment_total"
        assert result.confidence == 0.90
        assert result.gmail_message_id == "msg1"
        assert len(result.items) == 2

    def test_order_total_match(self, repo, imp):
        """Phase 2: order_total matches charge when no shipment_total."""
        txn = _txn(imp.id, amount=-170.29, raw_description="AMAZON.COM*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        receipts = {
            "msg1": ParsedReceipt(
                items=[
                    ReceiptItem("Insta360 Camera", 170.29, "electronics-d"),
                ],
                order_total=170.29,
                shipment_total=None,
            ),
        }
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.match_type == "amazon_order_total"
        assert result.confidence == 0.85

    def test_shipment_total_preferred_over_order_total(self, repo, imp):
        """Phase 1 (shipment) has priority over Phase 2 (order)."""
        txn = _txn(imp.id, amount=-35.26, raw_description="AMAZON.COM*AB1234")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        receipts = {
            "msg1": ParsedReceipt(
                items=[ReceiptItem("Book", 35.26, "books")],
                order_total=35.26,
                shipment_total=35.26,
            ),
        }
        result = lookup._resolve_amazon(txn, receipts)

        assert result.match_type == "amazon_shipment_total"
        assert result.confidence == 0.90

    def test_per_email_match_ignores_unrelated_emails(self, repo, imp):
        """Per-email matching finds correct email among multiple unrelated ones."""
        txn = _txn(imp.id, amount=-35.26, raw_description="AMAZON.COM*AB1234")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        receipts = {
            "msg1": ParsedReceipt(
                items=[ReceiptItem("Insta360 Camera", 170.29, "electronics-d")],
                order_total=170.29,
                shipment_total=170.29,
            ),
            "msg2": ParsedReceipt(
                items=[
                    ReceiptItem("Book", 15.26, "books"),
                    ReceiptItem("Pen Set", 20.00, "office-supplies"),
                ],
                order_total=35.26,
                shipment_total=35.26,
            ),
        }
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.gmail_message_id == "msg2"
        assert result.match_type == "amazon_shipment_total"

    def test_cross_email_fallback(self, repo, imp):
        """Phase 4: cross-email subset-sum when no per-email match."""
        txn = _txn(imp.id, amount=-30.00, raw_description="AMAZON.COM*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        # Neither email matches alone, but items across emails do
        receipts = {
            "msg1": ParsedReceipt(
                items=[ReceiptItem("Widget A", 10.00, "household-supplies")],
            ),
            "msg2": ParsedReceipt(
                items=[ReceiptItem("Widget B", 20.00, "household-supplies")],
            ),
        }
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.matched is True
        assert result.confidence == 0.75  # Lower confidence for cross-email
        assert len(result.items) == 2

    def test_no_match_across_unrelated_emails(self, repo, imp):
        """Multiple emails but no combination matches → returns None."""
        txn = _txn(imp.id, amount=-50.00, raw_description="AMAZON.COM*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        receipts = {
            "msg1": ParsedReceipt(
                items=[ReceiptItem("Insta360 Camera", 170.29, "electronics-d")],
                order_total=170.29,
            ),
            "msg2": ParsedReceipt(
                items=[ReceiptItem("eero Router", 624.85, "electronics-d")],
                order_total=624.85,
            ),
        }
        result = lookup._resolve_amazon(txn, receipts)
        assert result is None

    def test_shipment_total_within_tolerance(self, repo, imp):
        """Shipment total match respects 5% tolerance."""
        txn = _txn(imp.id, amount=-100.00, raw_description="AMAZON.COM*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)

        receipts = {
            "msg1": ParsedReceipt(
                items=[ReceiptItem("Widget", 97.00, "household-supplies")],
                shipment_total=97.00,
            ),
        }
        # 3% off, within 5% tolerance
        result = lookup._resolve_amazon(txn, receipts)

        assert result is not None
        assert result.match_type == "amazon_shipment_total"
        assert result.confidence == 0.90


# ── Receipt lookup status tracking tests ──────────────────


def _get_status(repo, txn_id):
    """Helper: read receipt_lookup_status from the database."""
    row = repo.conn.execute(
        "SELECT receipt_lookup_status FROM transactions WHERE id = ?",
        (txn_id,),
    ).fetchone()
    return row[0] if row else None


class TestReceiptLookupStatus:
    def test_non_candidate_stays_null(self, repo, imp):
        """Non-Apple/Amazon transaction: status stays null."""
        txn = _txn(imp.id, raw_description="DOORDASH*ORDER")
        repo.insert_transaction(txn)

        gmail = _mock_gmail()
        lookup = ReceiptLookup(gmail, repo)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) is None

    def test_gmail_error_sets_error(self, repo, imp):
        """Gmail API failure → status = 'error'."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = MagicMock()
        gmail.search_receipts.return_value = None
        lookup = ReceiptLookup(gmail, repo)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "error"

    def test_no_gmail_results_sets_no_email(self, repo, imp):
        """Gmail returns empty list → status = 'no_email'."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = _mock_gmail(messages=[])
        lookup = ReceiptLookup(gmail, repo)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "no_email"

    def test_no_email_bodies_sets_no_email(self, repo, imp):
        """Gmail returns messages but bodies are empty → status = 'no_email'."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=[""],  # Empty body
        )
        lookup = ReceiptLookup(gmail, repo)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "no_email"

    def test_matching_failure_sets_no_match(self, repo, imp):
        """Emails found, items extracted, but no amount match → status = 'no_match'."""
        txn = _txn(imp.id, amount=-999.99)  # Won't match any items
        repo.insert_transaction(txn)

        claude_response = json.dumps([
            {"name": "YouTube Premium", "amount": 22.99, "category_id": "youtube-premium"},
        ])
        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Receipt text"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "no_match"

    def test_successful_match_sets_matched(self, repo, imp):
        """Successful receipt match → status = 'matched'."""
        txn = _txn(imp.id, amount=-22.99)
        repo.insert_transaction(txn)

        claude_response = json.dumps([
            {"name": "YouTube Premium", "amount": 22.99, "category_id": "youtube-premium"},
        ])
        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Receipt text"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "matched"

    def test_budget_exceeded_sets_budget_exceeded(self, repo, imp):
        """Claude budget exceeded → status = 'budget_exceeded'."""
        txn = _txn(imp.id)
        repo.insert_transaction(txn)

        repo.increment_api_usage("2026-01", "claude_receipt_parse",
                                 cost_cents=MONTHLY_BUDGET_CENTS + 1)

        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Receipt text"],
        )
        claude_fn = _mock_claude_fn("[]")
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "budget_exceeded"

    def test_amazon_match_sets_matched(self, repo, imp):
        """Amazon successful match → status = 'matched'."""
        txn = _txn(imp.id, amount=-25.99, raw_description="AMAZON.COM*AB5678")
        repo.insert_transaction(txn)

        claude_response = json.dumps([
            {"name": "Phone Case", "amount": 25.99, "category_id": "electronics-d"},
        ])
        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Amazon order receipt"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "matched"

    def test_amazon_no_match_sets_no_match(self, repo, imp):
        """Amazon emails found but no matching amount → status = 'no_match'."""
        txn = _txn(imp.id, amount=-999.99, raw_description="AMAZON.COM*AB5678")
        repo.insert_transaction(txn)

        claude_response = json.dumps([
            {"name": "Phone Case", "amount": 25.99, "category_id": "electronics-d"},
        ])
        gmail = _mock_gmail(
            messages=[{"id": "msg1", "threadId": "t1"}],
            bodies=["Amazon order receipt"],
        )
        claude_fn = _mock_claude_fn(claude_response)
        lookup = ReceiptLookup(gmail, repo, claude_fn=claude_fn)
        lookup.resolve(txn)

        assert _get_status(repo, txn.id) == "no_match"
