"""Tests for `momoney category` CLI subcommands."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.cli import (
    cmd_category,
    main,
    _find_node,
    _find_parent,
    _print_tree,
)


# ── Helpers ──────────────────────────────────────────────


def _make_args(**kwargs):
    import argparse
    return argparse.Namespace(**kwargs)


MINI_TREE = [
    {
        "id": "income",
        "name": "Income",
        "is_income": True,
        "children": [
            {"id": "interest", "name": "Interest"},
            {"id": "cashback", "name": "Cashback"},
        ],
    },
    {
        "id": "expenses",
        "name": "Expenses",
        "children": [
            {
                "id": "monthly",
                "name": "Monthly Needs",
                "children": [
                    {"id": "groceries", "name": "Groceries"},
                    {"id": "mortgage", "name": "Mortgage"},
                ],
            },
            {
                "id": "wants",
                "name": "Wants",
                "children": [
                    {"id": "dining-out", "name": "Dining Out"},
                ],
            },
        ],
    },
]


@pytest.fixture
def config_dir(tmp_path):
    """Set up a temp config dir with a mini category tree."""
    cat_path = tmp_path / "categories.yaml"
    cat_path.write_text(yaml.dump({"tree": MINI_TREE}, sort_keys=False))
    for name in ("accounts", "merchants", "rules", "parsers", "budget_app_category_map"):
        (tmp_path / f"{name}.yaml").write_text(yaml.dump({"stub": True}))
    return tmp_path


# ── Tree utility tests ───────────────────────────────────


class TestFindNode:
    def test_find_root(self):
        assert _find_node(MINI_TREE, "income")["name"] == "Income"

    def test_find_leaf(self):
        assert _find_node(MINI_TREE, "groceries")["name"] == "Groceries"

    def test_find_nested(self):
        assert _find_node(MINI_TREE, "monthly")["name"] == "Monthly Needs"

    def test_not_found(self):
        assert _find_node(MINI_TREE, "nonexistent") is None


class TestFindParent:
    def test_root_node(self):
        siblings, parent = _find_parent(MINI_TREE, "income")
        assert parent is None
        assert siblings is MINI_TREE

    def test_nested_node(self):
        siblings, parent = _find_parent(MINI_TREE, "groceries")
        assert parent["id"] == "monthly"
        assert any(n["id"] == "groceries" for n in siblings)

    def test_not_found(self):
        siblings, parent = _find_parent(MINI_TREE, "nonexistent")
        assert siblings is None


class TestPrintTree:
    def test_prints_output(self, capsys):
        _print_tree(MINI_TREE)
        output = capsys.readouterr().out
        assert "Income" in output
        assert "Groceries" in output
        assert "Dining Out" in output


# ── CLI subcommand tests ─────────────────────────────────


class TestCategoryList:
    def test_list_prints_tree(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(command="category", category_command="list")
            ret = cmd_category(args)
        assert ret == 0
        output = capsys.readouterr().out
        assert "Income" in output
        assert "Groceries" in output

    def test_list_no_subcommand(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(command="category", category_command=None)
            ret = cmd_category(args)
        assert ret == 1


class TestCategoryAdd:
    def test_add_new_category(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="add",
                parent_id="wants", new_id="coffee", name="Coffee",
            )
            ret = cmd_category(args)
        assert ret == 0
        assert "Added" in capsys.readouterr().out

        # Verify it was written to YAML
        data = yaml.safe_load((config_dir / "categories.yaml").read_text())
        tree = data["tree"]
        wants = _find_node(tree, "wants")
        assert any(c["id"] == "coffee" for c in wants["children"])

    def test_add_duplicate_fails(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="add",
                parent_id="monthly", new_id="groceries", name="Groceries Again",
            )
            ret = cmd_category(args)
        assert ret == 1
        assert "already exists" in capsys.readouterr().out

    def test_add_nonexistent_parent_fails(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="add",
                parent_id="nonexistent", new_id="new-cat", name="New",
            )
            ret = cmd_category(args)
        assert ret == 1
        assert "not found" in capsys.readouterr().out


class TestCategoryRename:
    def test_rename_category(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="rename",
                id="groceries", name="Grocery Shopping",
            )
            ret = cmd_category(args)
        assert ret == 0
        assert "Renamed" in capsys.readouterr().out

        data = yaml.safe_load((config_dir / "categories.yaml").read_text())
        node = _find_node(data["tree"], "groceries")
        assert node["name"] == "Grocery Shopping"

    def test_rename_nonexistent_fails(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="rename",
                id="nonexistent", name="New Name",
            )
            ret = cmd_category(args)
        assert ret == 1
        assert "not found" in capsys.readouterr().out


class TestCategoryMove:
    def test_move_category(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="move",
                id="dining-out", new_parent_id="monthly",
            )
            ret = cmd_category(args)
        assert ret == 0
        assert "Moved" in capsys.readouterr().out

        data = yaml.safe_load((config_dir / "categories.yaml").read_text())
        monthly = _find_node(data["tree"], "monthly")
        assert any(c["id"] == "dining-out" for c in monthly["children"])
        # Should be removed from old parent
        wants = _find_node(data["tree"], "wants")
        assert not any(c["id"] == "dining-out" for c in wants.get("children", []))

    def test_move_nonexistent_fails(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="move",
                id="nonexistent", new_parent_id="monthly",
            )
            ret = cmd_category(args)
        assert ret == 1
        assert "not found" in capsys.readouterr().out

    def test_move_to_nonexistent_parent_fails(self, config_dir, capsys):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            args = _make_args(
                command="category", category_command="move",
                id="groceries", new_parent_id="nonexistent",
            )
            ret = cmd_category(args)
        assert ret == 1
        assert "not found" in capsys.readouterr().out


# ── Main dispatch tests ──────────────────────────────────


class TestCategoryMainDispatch:
    def test_category_subcommand_in_help(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.cli", "--help"],
            capture_output=True, text=True,
        )
        assert "category" in result.stdout

    def test_category_list_via_main(self, config_dir):
        with patch.dict("os.environ", {"FINANCE_CONFIG_DIR": str(config_dir)}):
            with patch("src.cli._setup_logging"):
                with pytest.raises(SystemExit) as exc:
                    main(["category", "list"])
                assert exc.value.code == 0
