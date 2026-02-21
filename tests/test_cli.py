"""Tests for src.cli — CLI argument parsing and command handlers.

Tests use main(argv=[...]) with mocked dependencies to avoid subprocess
issues (e.g., watch command runs forever) and to test command logic in
isolation without real database/sheets/config.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from src.cli import (
    cmd_import,
    cmd_import_budget_app,
    cmd_poll,
    cmd_push,
    cmd_reconcile,
    cmd_review,
    cmd_status,
    cmd_watch,
    main,
)


# ── Helpers ──────────────────────────────────────────────


def _make_args(**kwargs):
    """Create an argparse.Namespace with given attributes."""
    import argparse
    return argparse.Namespace(**kwargs)


def _mock_repo_with_schema():
    """Create a real in-memory repo with schema applied."""
    from src.database.repository import Repository
    repo = Repository(db_path=":memory:")
    migrations_dir = Path("src/database/migrations")
    if migrations_dir.exists():
        repo.apply_migrations(migrations_dir)
    return repo


# ── Argument parsing tests (subprocess) ──────────────────


class TestCliHelp:
    def test_help_exits_zero(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.cli", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "MoMoney personal finance tracker" in result.stdout

    def test_no_args_exits_zero(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.cli"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "usage:" in result.stdout.lower() or "MoMoney" in result.stdout


class TestCliSubcommands:
    def test_all_subcommands_listed_in_help(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.cli", "--help"],
            capture_output=True, text=True,
        )
        for cmd in ["import", "watch", "status", "review", "reconcile", "push", "poll", "import-budget-app", "category"]:
            assert cmd in result.stdout, f"Subcommand '{cmd}' not in help output"

    def test_import_subcommand_help(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.cli", "import", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "--file" in result.stdout

    def test_reconcile_accepts_optional_account(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.cli", "reconcile", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "account" in result.stdout.lower()

    def test_import_budget_app_requires_file_arg(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.cli", "import-budget-app"],
            capture_output=True, text=True,
        )
        assert result.returncode != 0  # argparse error, missing required arg


class TestMoMoneyEntryPoint:
    def test_momoney_command_exists(self):
        result = subprocess.run(
            ["momoney", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "MoMoney" in result.stdout


# ── main() dispatch tests ────────────────────────────────


class TestMainDispatch:
    def test_main_dispatches_to_handler(self):
        """main() dispatches to the correct command handler."""
        with patch("src.cli._COMMANDS", {"status": MagicMock(return_value=0)}):
            with patch("src.cli._setup_logging"):
                with pytest.raises(SystemExit) as exc:
                    main(["status"])
                assert exc.value.code == 0

    def test_main_no_command_shows_help(self, capsys):
        """main() with no command prints help and exits 0."""
        with patch("src.cli._setup_logging"):
            with pytest.raises(SystemExit) as exc:
                main([])
            assert exc.value.code == 0

    def test_main_accepts_argv(self):
        """main() accepts argv parameter for testability."""
        with patch("src.cli._COMMANDS", {"status": MagicMock(return_value=0)}):
            with patch("src.cli._setup_logging"):
                with pytest.raises(SystemExit) as exc:
                    main(["status"])
                assert exc.value.code == 0


# ── cmd_import tests ─────────────────────────────────────


class TestCmdImport:
    def test_import_single_file_success(self, tmp_path):
        """Import a single file successfully."""
        f = tmp_path / "test.qfx"
        f.write_text("<OFX>content</OFX>")

        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.file_name = "test.qfx"
        mock_result.status = "success"
        mock_result.new_count = 5
        mock_result.duplicate_count = 0
        mock_result.flagged_count = 0
        mock_pipeline.process_file.return_value = mock_result

        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_dedup"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.watcher.observer.ImportPipeline", return_value=mock_pipeline):
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(file=f, command="import")
            ret = cmd_import(args)

        assert ret == 0
        mock_pipeline.process_file.assert_called_once()
        mock_repo.close.assert_called_once()

    def test_import_file_not_found(self, tmp_path, capsys):
        """Import of nonexistent file returns error."""
        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_dedup"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.watcher.observer.ImportPipeline"):
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(file=tmp_path / "nonexistent.qfx", command="import")
            ret = cmd_import(args)

        assert ret == 1
        assert "not found" in capsys.readouterr().out.lower()

    def test_import_unsupported_extension(self, tmp_path, capsys):
        """Import of unsupported file type returns error."""
        f = tmp_path / "data.pdf"
        f.write_text("PDF content")

        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_dedup"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.watcher.observer.ImportPipeline"):
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(file=f, command="import")
            ret = cmd_import(args)

        assert ret == 1
        assert "unsupported" in capsys.readouterr().out.lower()

    def test_import_batch_no_files(self, tmp_path, capsys):
        """Batch import with no files returns 0."""
        watch_dir = tmp_path / "import"
        watch_dir.mkdir()

        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_dedup"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.cli._get_watch_dir", return_value=watch_dir), \
             patch("src.watcher.observer.ImportPipeline"):
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(file=None, command="import")
            ret = cmd_import(args)

        assert ret == 0
        assert "no pending" in capsys.readouterr().out.lower()

    def test_import_batch_processes_files(self, tmp_path, capsys):
        """Batch import processes all supported files in watch dir."""
        watch_dir = tmp_path / "import"
        watch_dir.mkdir()
        (watch_dir / "a.qfx").write_text("<OFX></OFX>")
        (watch_dir / "b.csv").write_text("data\n")
        (watch_dir / "readme.txt").write_text("ignore")

        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.file_name = "test"
        mock_result.status = "success"
        mock_result.new_count = 3
        mock_result.duplicate_count = 0
        mock_pipeline.process_file.return_value = mock_result

        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_dedup"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.cli._get_watch_dir", return_value=watch_dir), \
             patch("src.watcher.observer.ImportPipeline", return_value=mock_pipeline):
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(file=None, command="import")
            ret = cmd_import(args)

        assert ret == 0
        # Should process 2 files (a.qfx, b.csv) but not readme.txt
        assert mock_pipeline.process_file.call_count == 2
        output = capsys.readouterr().out
        assert "Processed 2 files" in output

    def test_import_error_result_returns_nonzero(self, tmp_path):
        """Single file import with error result returns 1."""
        f = tmp_path / "bad.qfx"
        f.write_text("<OFX></OFX>")

        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.file_name = "bad.qfx"
        mock_result.status = "error"
        mock_result.new_count = 0
        mock_result.duplicate_count = 0
        mock_result.flagged_count = 0
        mock_result.error_message = "Parse failed"
        mock_pipeline.process_file.return_value = mock_result

        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_dedup"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.watcher.observer.ImportPipeline", return_value=mock_pipeline):
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(file=f, command="import")
            ret = cmd_import(args)

        assert ret == 1


# ── cmd_watch tests ──────────────────────────────────────


class TestCmdWatch:
    def test_watch_starts_and_stops(self):
        """Watch command starts watcher and handles KeyboardInterrupt."""
        mock_watcher = MagicMock()
        mock_watcher.watch_dir = Path("/tmp/import")

        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_dedup"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.cli._get_watch_dir"), \
             patch("src.watcher.observer.ImportPipeline"), \
             patch("src.watcher.observer.FileWatcher", return_value=mock_watcher), \
             patch("src.cli.time.sleep", side_effect=KeyboardInterrupt()) as mock_sleep:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(command="watch")
            ret = cmd_watch(args)

        assert ret == 0
        mock_watcher.start.assert_called_once()
        mock_watcher.stop.assert_called_once()
        mock_repo.close.assert_called_once()


# ── cmd_status tests ─────────────────────────────────────


class TestCmdStatus:
    def test_status_displays_counts(self, capsys):
        """Status command shows transaction counts."""
        mock_counts = {
            "total_txns": 1000,
            "categorized": 950,
            "pending": 30,
            "flagged": 20,
            "total_transfers": 100,
            "total_imports": 5,
        }

        mock_repo = MagicMock()
        mock_repo.get_monthly_cost.return_value = 125

        with patch("src.cli._get_repo", return_value=mock_repo), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.queries.get_status_counts", return_value=mock_counts):
            args = _make_args(command="status")
            ret = cmd_status(args)

        assert ret == 0
        output = capsys.readouterr().out
        assert "1,000" in output
        assert "950" in output
        assert "30" in output
        assert "20" in output
        assert "$1.25" in output

    def test_status_empty_database(self, capsys):
        """Status with empty database shows zeros."""
        mock_counts = {
            "total_txns": 0,
            "categorized": 0,
            "pending": 0,
            "flagged": 0,
            "total_transfers": 0,
            "total_imports": 0,
        }

        mock_repo = MagicMock()
        mock_repo.get_monthly_cost.return_value = 0

        with patch("src.cli._get_repo", return_value=mock_repo), \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.queries.get_status_counts", return_value=mock_counts):
            args = _make_args(command="status")
            ret = cmd_status(args)

        assert ret == 0
        output = capsys.readouterr().out
        assert "$0.00" in output


# ── cmd_review tests ─────────────────────────────────────


class TestCmdReview:
    def test_review_no_items(self, capsys):
        """Review with no flagged items."""
        mock_repo = MagicMock()
        mock_repo.conn.execute.return_value.fetchall.return_value = []

        with patch("src.cli._get_repo", return_value=mock_repo), \
             patch("src.cli._get_migrations_dir"):
            args = _make_args(command="review")
            ret = cmd_review(args)

        assert ret == 0
        assert "no transactions pending" in capsys.readouterr().out.lower()

    def test_review_with_items(self, capsys):
        """Review lists flagged transactions."""
        mock_row = {
            "id": "txn-1",
            "date": "2025-01-15",
            "amount": -42.50,
            "account_id": "wf-checking",
            "raw_description": "UNKNOWN MERCHANT PURCHASE",
            "categorization_method": "manual_review",
            "confidence": None,
        }
        mock_rows = [mock_row]

        mock_repo = MagicMock()
        mock_repo.conn.execute.return_value.fetchall.return_value = mock_rows

        with patch("src.cli._get_repo", return_value=mock_repo), \
             patch("src.cli._get_migrations_dir"):
            args = _make_args(command="review")
            ret = cmd_review(args)

        assert ret == 0
        output = capsys.readouterr().out
        assert "2025-01-15" in output
        assert "42.50" in output
        assert "wf-checking" in output


# ── cmd_reconcile tests ──────────────────────────────────


class TestCmdReconcile:
    def test_reconcile_single_account_balanced(self, capsys):
        """Reconcile single account that's balanced."""
        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.queries.get_reconciliation_data") as mock_recon:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_recon.return_value = {
                "status": "balanced",
                "date": "2025-01-31",
                "statement_balance": 5000.00,
                "computed_balance": 5000.00,
                "difference": 0.00,
            }
            args = _make_args(command="reconcile", account="wf-checking")
            ret = cmd_reconcile(args)

        assert ret == 0
        output = capsys.readouterr().out
        assert "OK" in output
        assert "wf-checking" in output

    def test_reconcile_discrepancy_returns_nonzero(self, capsys):
        """Reconcile with discrepancy returns exit code 1."""
        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.queries.get_reconciliation_data") as mock_recon:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_recon.return_value = {
                "status": "discrepancy",
                "date": "2025-01-31",
                "statement_balance": 5000.00,
                "computed_balance": 4950.00,
                "difference": 50.00,
            }
            args = _make_args(command="reconcile", account="wf-checking")
            ret = cmd_reconcile(args)

        assert ret == 1
        output = capsys.readouterr().out
        assert "MISMATCH" in output

    def test_reconcile_no_balance_data(self, capsys):
        """Reconcile when account has no balance data."""
        with patch("src.cli._get_config"), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.queries.get_reconciliation_data") as mock_recon:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_recon.return_value = {"status": "no_balance_data"}
            args = _make_args(command="reconcile", account="mercury-checking")
            ret = cmd_reconcile(args)

        assert ret == 0
        assert "no balance data" in capsys.readouterr().out.lower()

    def test_reconcile_all_accounts(self, capsys):
        """Reconcile all accounts when no specific account given."""
        mock_config = MagicMock()
        mock_config.accounts = [
            {"id": "wf-checking"},
            {"id": "cap1-credit"},
        ]

        with patch("src.cli._get_config", return_value=mock_config), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.queries.get_reconciliation_data") as mock_recon:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_recon.return_value = {"status": "no_balance_data"}
            args = _make_args(command="reconcile", account=None)
            ret = cmd_reconcile(args)

        assert ret == 0
        assert mock_recon.call_count == 2


# ── cmd_push tests ───────────────────────────────────────


class TestCmdPush:
    def test_push_no_sheets_configured(self, capsys):
        """Push without sheets configured returns error."""
        with patch("src.cli._get_sheets", return_value=None):
            args = _make_args(command="push")
            ret = cmd_push(args)

        assert ret == 1
        assert "not configured" in capsys.readouterr().out.lower()

    def test_push_calls_full_rebuild(self, capsys):
        """Push calls full_rebuild on sheets with config."""
        mock_sheets = MagicMock()
        mock_push_result = MagicMock()
        mock_push_result.rows_pushed = 100
        mock_sheets.full_rebuild.return_value = [mock_push_result]
        mock_config = MagicMock()

        with patch("src.cli._get_sheets", return_value=mock_sheets), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.cli._get_config", return_value=mock_config):
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            args = _make_args(command="push")
            ret = cmd_push(args)

        assert ret == 0
        mock_sheets.full_rebuild.assert_called_once_with(mock_repo, mock_config)
        output = capsys.readouterr().out
        assert "100" in output


# ── cmd_poll tests ───────────────────────────────────────


class TestCmdPoll:
    def test_poll_no_sheets_configured(self, capsys):
        """Poll without sheets configured returns error."""
        with patch("src.cli._get_spreadsheet", return_value=None):
            args = _make_args(command="poll")
            ret = cmd_poll(args)

        assert ret == 1
        assert "not configured" in capsys.readouterr().out.lower()

    def test_poll_no_overrides(self, capsys):
        """Poll with no overrides found."""
        mock_ss = MagicMock()
        mock_result = MagicMock()
        mock_result.overrides_applied = 0
        mock_result.errors = 0
        mock_result.actions = []

        with patch("src.cli._get_spreadsheet", return_value=mock_ss), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_config") as mock_config_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.sheets.overrides.OverridePoller") as mock_poller_cls:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_config_fn.return_value = MagicMock()
            mock_poller = MagicMock()
            mock_poller.poll.return_value = mock_result
            mock_poller_cls.return_value = mock_poller

            args = _make_args(command="poll")
            ret = cmd_poll(args)

        assert ret == 0
        assert "no overrides" in capsys.readouterr().out.lower()
        mock_repo.close.assert_called_once()

    def test_poll_with_overrides(self, capsys):
        """Poll applies overrides and prints summary."""
        mock_ss = MagicMock()
        mock_action = MagicMock()
        mock_action.sheet = "Transactions"
        mock_action.row_index = 5
        mock_action.column = "Override: Category"
        mock_action.new_value = "coffee-d"
        mock_action.entity_id = "alloc-123"

        mock_result = MagicMock()
        mock_result.overrides_applied = 1
        mock_result.errors = 0
        mock_result.actions = [mock_action]

        with patch("src.cli._get_spreadsheet", return_value=mock_ss), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_config") as mock_config_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.sheets.overrides.OverridePoller") as mock_poller_cls:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_config_fn.return_value = MagicMock()
            mock_poller = MagicMock()
            mock_poller.poll.return_value = mock_result
            mock_poller_cls.return_value = mock_poller

            args = _make_args(command="poll")
            ret = cmd_poll(args)

        assert ret == 0
        output = capsys.readouterr().out
        assert "1 override" in output
        assert "coffee-d" in output

    def test_poll_with_errors_returns_nonzero(self, capsys):
        """Poll with errors returns exit code 1."""
        mock_ss = MagicMock()
        mock_result = MagicMock()
        mock_result.overrides_applied = 0
        mock_result.errors = 2
        mock_result.actions = []

        with patch("src.cli._get_spreadsheet", return_value=mock_ss), \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_config") as mock_config_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.sheets.overrides.OverridePoller") as mock_poller_cls:
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_config_fn.return_value = MagicMock()
            mock_poller = MagicMock()
            mock_poller.poll.return_value = mock_result
            mock_poller_cls.return_value = mock_poller

            args = _make_args(command="poll")
            ret = cmd_poll(args)

        assert ret == 1
        assert "2 error" in capsys.readouterr().out


# ── cmd_import_budget_app tests ────────────────────────────


class TestCmdImportBudgetApp:
    def test_import_budget_app_file_not_found(self, tmp_path, capsys):
        """import-budget-app with nonexistent file returns error."""
        args = _make_args(file=tmp_path / "nonexistent.csv", command="import-budget-app")
        ret = cmd_import_budget_app(args)
        assert ret == 1
        assert "not found" in capsys.readouterr().out.lower()

    def test_import_budget_app_duplicate_file(self, tmp_path, capsys):
        """import-budget-app with already-imported file is skipped."""
        f = tmp_path / "register.csv"
        f.write_text('\ufeff"Account","Flag","Date"\n')

        mock_dedup = MagicMock()
        mock_dedup.check_file_duplicate.return_value = True

        with patch("src.cli._get_config") as mock_config_fn, \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.dedup.DedupEngine", return_value=mock_dedup), \
             patch("src.parsers.budget_app.BudgetAppCsvParser") as mock_parser_cls:
            mock_config = MagicMock()
            mock_config.budget_app_category_map = {}
            mock_config_fn.return_value = mock_config
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_parser = MagicMock()
            mock_parser.parse.return_value = [MagicMock()]
            mock_parser_cls.return_value = mock_parser

            args = _make_args(file=f, command="import-budget-app")
            ret = cmd_import_budget_app(args)

        assert ret == 0
        assert "already been imported" in capsys.readouterr().out.lower()

    def test_import_budget_app_success(self, tmp_path, capsys):
        """import-budget-app successfully imports transactions."""
        f = tmp_path / "register.csv"
        f.write_text('\ufeff"Account","Flag","Date"\ndata\n')

        mock_dedup = MagicMock()
        mock_dedup.check_file_duplicate.return_value = False

        mock_txn = MagicMock()
        mock_txn.status = "pending"
        mock_txn.id = "txn-1"

        mock_batch = MagicMock()
        mock_batch.new_count = 10
        mock_batch.duplicate_count = 0
        mock_batch.flagged_count = 0
        mock_batch.transactions = [mock_txn]
        mock_dedup.process_batch.return_value = mock_batch

        with patch("src.cli._get_config") as mock_config_fn, \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.cli._get_sheets", return_value=None), \
             patch("src.database.dedup.DedupEngine", return_value=mock_dedup), \
             patch("src.parsers.budget_app.BudgetAppCsvParser") as mock_parser_cls, \
             patch("src.parsers.base.compute_file_hash", return_value="abc123"), \
             patch("src.categorize.pipeline.categorize_transaction") as mock_cat, \
             patch("src.categorize.pipeline.apply_categorization"):
            mock_config = MagicMock()
            mock_config.budget_app_category_map = {}
            mock_config_fn.return_value = mock_config
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_parser = MagicMock()
            mock_parser.parse.return_value = [MagicMock()]  # 1 raw txn
            mock_parser_cls.return_value = mock_parser
            mock_cat.return_value = MagicMock(method="transfer")

            args = _make_args(file=f, command="import-budget-app")
            ret = cmd_import_budget_app(args)

        assert ret == 0
        output = capsys.readouterr().out
        assert "10 new" in output
        mock_repo.close.assert_called_once()

    def test_import_budget_app_empty_file(self, tmp_path, capsys):
        """import-budget-app with no transactions returns 0."""
        f = tmp_path / "empty.csv"
        f.write_text('\ufeff"Account","Flag","Date"\n')

        with patch("src.cli._get_config") as mock_config_fn, \
             patch("src.cli._get_repo") as mock_repo_fn, \
             patch("src.cli._get_migrations_dir"), \
             patch("src.database.dedup.DedupEngine"), \
             patch("src.parsers.budget_app.BudgetAppCsvParser") as mock_parser_cls:
            mock_config = MagicMock()
            mock_config.budget_app_category_map = {}
            mock_config_fn.return_value = mock_config
            mock_repo = MagicMock()
            mock_repo_fn.return_value = mock_repo
            mock_parser = MagicMock()
            mock_parser.parse.return_value = []  # No transactions
            mock_parser_cls.return_value = mock_parser

            args = _make_args(file=f, command="import-budget-app")
            ret = cmd_import_budget_app(args)

        assert ret == 0
        assert "no transactions" in capsys.readouterr().out.lower()


# ── Helper function tests ────────────────────────────────


class TestHelpers:
    def test_get_watch_dir_default(self):
        """Default watch dir is 'import'."""
        from src.cli import _get_watch_dir
        with patch.dict("os.environ", {}, clear=True):
            assert _get_watch_dir() == Path("import")

    def test_get_watch_dir_from_env(self):
        """Watch dir can be set via env var."""
        from src.cli import _get_watch_dir
        with patch.dict("os.environ", {"FINANCE_WATCH_DIR": "/data/drop"}):
            assert _get_watch_dir() == Path("/data/drop")

    def test_get_sheets_not_configured(self):
        """_get_sheets returns None when env vars not set."""
        from src.cli import _get_sheets
        with patch.dict("os.environ", {}, clear=True):
            assert _get_sheets() is None

    def test_get_migrations_dir_default(self):
        """Default migrations dir."""
        from src.cli import _get_migrations_dir
        with patch.dict("os.environ", {}, clear=True):
            assert _get_migrations_dir() == Path("src/database/migrations")
