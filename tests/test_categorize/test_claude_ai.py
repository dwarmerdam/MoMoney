"""Tests for Claude AI categorization module (Step 7)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.categorize.claude_ai import (
    MONTHLY_BUDGET_CENTS,
    categorize_single,
    _build_category_list,
    _parse_response,
)
from src.database.models import Import, Transaction
from src.database.repository import Repository

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


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
        account_id="primary-checking", date="2026-01-15", amount=-25.00,
        raw_description="MYSTERY MERCHANT", import_id=imp_id,
        import_hash="h1", dedup_key="dk1",
    )
    defaults.update(kw)
    return Transaction(**defaults)


def _mock_config(categories=None):
    config = MagicMock()
    if categories is None:
        categories = [
            {"id": "groceries", "children": []},
            {"id": "dining", "children": [
                {"id": "coffee-d", "children": []},
                {"id": "restaurants-d", "children": []},
            ]},
            {"id": "transport", "children": []},
        ]
    config.categories = categories
    return config


class TestBuildCategoryList:
    def test_flat_categories(self):
        config = _mock_config([
            {"id": "groceries", "children": []},
            {"id": "transport", "children": []},
        ])
        result = _build_category_list(config)
        assert "groceries" in result
        assert "transport" in result

    def test_nested_categories(self):
        config = _mock_config()
        result = _build_category_list(config)
        assert "dining" in result
        assert "coffee-d" in result
        assert "restaurants-d" in result


class TestParseResponse:
    def test_valid_json(self):
        config = _mock_config()
        response = json.dumps({
            "category_id": "groceries",
            "confidence": 0.85,
            "reasoning": "Looks like a grocery store",
        })
        result = _parse_response(response, config)
        assert result is not None
        assert result.category_id == "groceries"
        assert result.confidence == 0.85
        assert result.reasoning == "Looks like a grocery store"

    def test_json_with_code_fences(self):
        config = _mock_config()
        response = '```json\n{"category_id": "transport", "confidence": 0.9, "reasoning": "Gas station"}\n```'
        result = _parse_response(response, config)
        assert result is not None
        assert result.category_id == "transport"

    def test_uncategorized_returns_none(self):
        config = _mock_config()
        response = json.dumps({
            "category_id": "uncategorized",
            "confidence": 0.0,
            "reasoning": "Cannot determine",
        })
        result = _parse_response(response, config)
        assert result is None

    def test_empty_category_returns_none(self):
        config = _mock_config()
        response = json.dumps({
            "category_id": "",
            "confidence": 0.5,
            "reasoning": "Unknown",
        })
        result = _parse_response(response, config)
        assert result is None

    def test_invalid_json_returns_none(self):
        config = _mock_config()
        result = _parse_response("not valid json", config)
        assert result is None

    def test_non_dict_returns_none(self):
        config = _mock_config()
        result = _parse_response("[1, 2, 3]", config)
        assert result is None

    def test_confidence_clamped(self):
        config = _mock_config()
        response = json.dumps({
            "category_id": "groceries",
            "confidence": 1.5,
            "reasoning": "Very sure",
        })
        result = _parse_response(response, config)
        assert result.confidence == 1.0


class TestCategorizeSingle:
    def test_successful_categorization(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        config = _mock_config()

        def mock_claude_fn(system, prompt):
            return json.dumps({
                "category_id": "groceries",
                "confidence": 0.75,
                "reasoning": "Looks like a grocery purchase",
            })

        result = categorize_single(txn, config, mock_claude_fn, repo)
        assert result is not None
        assert result.category_id == "groceries"
        assert result.confidence == 0.75

    def test_budget_exceeded_returns_none(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        config = _mock_config()

        # Exhaust the budget
        repo.increment_api_usage("2026-01", "claude_categorize",
                                  requests=1, cost_cents=MONTHLY_BUDGET_CENTS)

        def mock_claude_fn(system, prompt):
            return json.dumps({
                "category_id": "groceries",
                "confidence": 0.75,
                "reasoning": "test",
            })

        result = categorize_single(txn, config, mock_claude_fn, repo)
        assert result is None

    def test_claude_exception_returns_none(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        config = _mock_config()

        def mock_claude_fn(system, prompt):
            raise RuntimeError("API error")

        result = categorize_single(txn, config, mock_claude_fn, repo)
        assert result is None

    def test_no_date_returns_none(self, repo, imp):
        txn = _txn(imp.id, date="")
        repo.insert_transaction(txn)
        config = _mock_config()

        result = categorize_single(txn, config, lambda s, p: "{}", repo)
        assert result is None

    def test_tracks_api_usage(self, repo, imp):
        txn = _txn(imp.id)
        repo.insert_transaction(txn)
        config = _mock_config()

        def mock_claude_fn(system, prompt):
            return json.dumps({
                "category_id": "transport",
                "confidence": 0.8,
                "reasoning": "test",
            })

        categorize_single(txn, config, mock_claude_fn, repo)
        cost = repo.get_monthly_cost("2026-01")
        assert cost > 0
