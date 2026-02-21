"""Tests for Config.flatten_category_tree() and category sheet row helpers."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.config import Config
from src.sheets.push import (
    CATEGORY_HEADERS,
    SUMMARY_HEADERS,
    category_to_row,
    summary_to_row,
)


# ── Synthetic category tree (mirrors real 4-level structure) ──────


SYNTHETIC_TREE = [
    {
        "id": "income",
        "name": "Income",
        "is_income": True,
        "children": [
            {
                "id": "all-income",
                "name": "All Income Sources",
                "children": [
                    {"id": "interest", "name": "Interest"},
                    {"id": "cashback", "name": "Cashback"},
                ],
            },
        ],
    },
    {
        "id": "expenses",
        "name": "Expenses",
        "children": [
            {
                "id": "monthly-shared",
                "name": "Monthly Needs (Shared)",
                "children": [
                    {
                        "id": "insurance",
                        "name": "Insurance",
                        "children": [
                            {"id": "car-insurance", "name": "Car Insurance"},
                            {"id": "home-insurance", "name": "Home Owners Insurance"},
                        ],
                    },
                    {"id": "mortgage", "name": "Mortgage"},
                    {
                        "id": "utilities",
                        "name": "Utilities",
                        "children": [
                            {"id": "gas-electric", "name": "Gas and Electric"},
                        ],
                    },
                ],
            },
            {
                "id": "true-expenses",
                "name": "True Expenses Needs (Shared)",
                "children": [
                    {"id": "groceries", "name": "Groceries"},
                    {
                        "id": "taxes",
                        "name": "Taxes",
                        "children": [
                            {"id": "property-tax", "name": "Property Tax"},
                        ],
                    },
                ],
            },
            {
                "id": "personal-biz",
                "name": "Personal Biz & Misc (P)",
                "children": [
                    {"id": "coffee-d", "name": "Coffee (D)"},
                    {
                        "id": "side-business",
                        "name": "Side Business (SB)",
                        "children": [
                            {"id": "saas-sb", "name": "SaaS subscriptions (SB)"},
                        ],
                    },
                ],
            },
        ],
    },
    {
        "id": "transfers",
        "name": "Transfers",
        "is_transfer": True,
        "children": [
            {"id": "xfer-cc", "name": "Credit Card Payment"},
        ],
    },
]


@pytest.fixture
def config(tmp_path):
    """Config with synthetic categories tree."""
    import yaml

    cat_path = tmp_path / "categories.yaml"
    cat_path.write_text(yaml.dump({"tree": SYNTHETIC_TREE}))

    # Minimal stubs for other required config files
    for name in ("accounts", "merchants", "rules", "parsers", "budget_app_category_map"):
        (tmp_path / f"{name}.yaml").write_text(yaml.dump({"stub": True}))

    return Config(config_dir=tmp_path)


class TestFlattenCategoryTree:

    def test_returns_all_categories(self, config):
        flat = config.flatten_category_tree()
        expected_ids = {
            "income", "all-income", "interest", "cashback",
            "expenses", "monthly-shared", "insurance", "car-insurance",
            "home-insurance", "mortgage", "utilities", "gas-electric",
            "true-expenses", "groceries", "taxes", "property-tax",
            "personal-biz", "coffee-d", "side-business", "saas-sb",
            "transfers", "xfer-cc",
        }
        assert set(flat.keys()) == expected_ids

    def test_leaf_at_level_2(self, config):
        """Groceries is a leaf at depth 2 (Expenses > True Expenses > Groceries)."""
        flat = config.flatten_category_tree()
        g = flat["groceries"]
        assert g["name"] == "Groceries"
        assert g["level"] == 2
        assert g["level_0"] == "Expenses"
        assert g["level_1"] == "True Expenses Needs (Shared)"
        assert g["level_2"] == "Groceries"
        assert g["level_3"] == ""
        assert g["is_leaf"] is True

    def test_leaf_at_level_3(self, config):
        """Car Insurance is a leaf at depth 3."""
        flat = config.flatten_category_tree()
        ci = flat["car-insurance"]
        assert ci["name"] == "Car Insurance"
        assert ci["level"] == 3
        assert ci["level_0"] == "Expenses"
        assert ci["level_1"] == "Monthly Needs (Shared)"
        assert ci["level_2"] == "Insurance"
        assert ci["level_3"] == "Car Insurance"
        assert ci["is_leaf"] is True

    def test_non_leaf_node(self, config):
        """Insurance is a non-leaf at depth 2."""
        flat = config.flatten_category_tree()
        ins = flat["insurance"]
        assert ins["name"] == "Insurance"
        assert ins["level"] == 2
        assert ins["is_leaf"] is False
        assert ins["level_0"] == "Expenses"
        assert ins["level_1"] == "Monthly Needs (Shared)"
        assert ins["level_2"] == "Insurance"

    def test_income_flag_inherited(self, config):
        """Income categories inherit is_income from root."""
        flat = config.flatten_category_tree()
        assert flat["income"]["is_income"] is True
        assert flat["all-income"]["is_income"] is True
        assert flat["interest"]["is_income"] is True
        assert flat["cashback"]["is_income"] is True
        # Expense categories are not income
        assert flat["groceries"]["is_income"] is False

    def test_transfer_flag_inherited(self, config):
        """Transfer categories inherit is_transfer from root."""
        flat = config.flatten_category_tree()
        assert flat["transfers"]["is_transfer"] is True
        assert flat["xfer-cc"]["is_transfer"] is True
        # Non-transfer categories
        assert flat["groceries"]["is_transfer"] is False

    def test_root_nodes(self, config):
        """Root nodes are at level 0."""
        flat = config.flatten_category_tree()
        inc = flat["income"]
        assert inc["level"] == 0
        assert inc["level_0"] == "Income"
        assert inc["level_1"] == ""

        exp = flat["expenses"]
        assert exp["level"] == 0
        assert exp["level_0"] == "Expenses"

    def test_level_1_nodes(self, config):
        """Level 1 nodes (groups)."""
        flat = config.flatten_category_tree()
        ms = flat["monthly-shared"]
        assert ms["level"] == 1
        assert ms["level_0"] == "Expenses"
        assert ms["level_1"] == "Monthly Needs (Shared)"
        assert ms["level_2"] == ""

    def test_deep_4_level_path(self, config):
        """SaaS subscriptions (SB) is at depth 3: Expenses > Personal Biz > Side Business > SaaS."""
        flat = config.flatten_category_tree()
        s = flat["saas-sb"]
        assert s["level"] == 3
        assert s["level_0"] == "Expenses"
        assert s["level_1"] == "Personal Biz & Misc (P)"
        assert s["level_2"] == "Side Business (SB)"
        assert s["level_3"] == "SaaS subscriptions (SB)"

    def test_mortgage_leaf_at_level_2(self, config):
        """Mortgage is a direct leaf under Monthly Needs (Shared), no children."""
        flat = config.flatten_category_tree()
        m = flat["mortgage"]
        assert m["level"] == 2
        assert m["level_0"] == "Expenses"
        assert m["level_1"] == "Monthly Needs (Shared)"
        assert m["level_2"] == "Mortgage"
        assert m["level_3"] == ""
        assert m["is_leaf"] is True


class TestCategoryToRow:

    def test_row_length_matches_headers(self, config):
        flat = config.flatten_category_tree()
        for cat_id, entry in flat.items():
            row = category_to_row(entry)
            assert len(row) == len(CATEGORY_HEADERS), f"Row length mismatch for {cat_id}"

    def test_row_values(self, config):
        flat = config.flatten_category_tree()
        row = category_to_row(flat["car-insurance"])
        assert row[0] == "car-insurance"
        assert row[1] == "Car Insurance"
        assert row[2] == 3  # level
        assert row[3] == "Expenses"  # level_0
        assert row[4] == "Monthly Needs (Shared)"  # level_1
        assert row[5] == "Insurance"  # level_2
        assert row[6] == "Car Insurance"  # level_3
        assert row[7] is True  # is_leaf
        assert row[8] is False  # is_income
        assert row[9] is False  # is_transfer


class TestEnrichedSummaryToRow:

    def test_with_lookup(self, config):
        flat = config.flatten_category_tree()
        row_data = {"category_id": "groceries", "total": -487.50, "txn_count": 12}
        row = summary_to_row("2026-01", row_data, flat)
        assert len(row) == len(SUMMARY_HEADERS)
        assert row[0] == "2026-01"
        assert row[1] == "groceries"
        assert row[2] == "Groceries"  # category_name
        assert row[3] == "Expenses"  # level_0
        assert row[4] == "True Expenses Needs (Shared)"  # level_1
        assert row[5] == "Groceries"  # level_2
        assert row[6] == -487.50  # total
        assert row[7] == 12  # txn_count

    def test_without_lookup(self):
        """summary_to_row still works without category lookup (backward compat)."""
        row_data = {"category_id": "groceries", "total": -100.0, "txn_count": 5}
        row = summary_to_row("2026-02", row_data)
        assert len(row) == len(SUMMARY_HEADERS)
        assert row[1] == "groceries"
        assert row[2] == ""  # no name
        assert row[3] == ""  # no level_0

    def test_unknown_category_in_lookup(self, config):
        """Category not in tree still produces a row with empty hierarchy."""
        flat = config.flatten_category_tree()
        row_data = {"category_id": "unknown-cat", "total": -50.0, "txn_count": 1}
        row = summary_to_row("2026-03", row_data, flat)
        assert row[1] == "unknown-cat"
        assert row[2] == ""
        assert row[3] == ""
