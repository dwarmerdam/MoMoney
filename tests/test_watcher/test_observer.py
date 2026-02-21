"""Tests for src/watcher/observer.py — file watcher and import pipeline.

Covers:
- wait_for_stable() behavior
- validate_file_completeness() for CSV and QFX
- detect_parser() auto-detection
- _extract_acctid() and _resolve_account_id() helpers
- ImportPipeline.process_file() orchestration
- FileWatcher event handling
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.watcher.observer import (
    DEFAULT_STABILITY_SECONDS,
    FileStabilityError,
    FileWatcher,
    ImportPipeline,
    ImportResult,
    SUPPORTED_EXTENSIONS,
    _extract_acctid,
    _resolve_account_id,
    detect_parser,
    validate_file_completeness,
    wait_for_stable,
)


# ── Helpers ──────────────────────────────────────────────


def _write_sgml_qfx(path: Path, acctid: str = "1234567890") -> None:
    """Write a minimal SGML QFX file (Wells Fargo style)."""
    content = f"""OFXHEADER:100
DATA:OFXSGML
VERSION:102
<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0<SEVERITY>INFO</STATUS>
<DTSERVER>20250101
<LANGUAGE>ENG
</SONRS>
</SIGNONMSGSRSV1>
<BANKMSGSRSV1>
<STMTTRNRS>
<STMTRS>
<CURDEF>USD
<BANKACCTFROM>
<BANKID>111111111
<ACCTID>{acctid}
<ACCTTYPE>CHECKING
</BANKACCTFROM>
<BANKTRANLIST>
<DTSTART>20250101
<DTEND>20250131
<STMTTRN>
<TRNTYPE>DEBIT
<DTPOSTED>20250115
<TRNAMT>-42.50
<FITID>202501151
<NAME>GROCERY STORE
</STMTTRN>
</BANKTRANLIST>
</STMTRS>
</STMTTRNRS>
</BANKMSGSRSV1>
</OFX>
"""
    path.write_text(content)


def _write_xml_qfx(path: Path, acctid: str = "0217") -> None:
    """Write a minimal XML QFX file (Capital One style)."""
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<?OFX OFXHEADER="200" VERSION="220"?>
<OFX>
<SIGNONMSGSRSV1>
<SONRS>
<STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
<DTSERVER>20250101120000</DTSERVER>
<LANGUAGE>ENG</LANGUAGE>
</SONRS>
</SIGNONMSGSRSV1>
<CREDITCARDMSGSRSV1>
<CCSTMTTRNRS>
<CCSTMTRS>
<CURDEF>USD</CURDEF>
<CCACCTFROM>
<ACCTID>{acctid}</ACCTID>
</CCACCTFROM>
<BANKTRANLIST>
<DTSTART>20250101120000</DTSTART>
<DTEND>20250131120000</DTEND>
<STMTTRN>
<TRNTYPE>DEBIT</TRNTYPE>
<DTPOSTED>20250115120000</DTPOSTED>
<TRNAMT>-25.00</TRNAMT>
<FITID>202501151505320</FITID>
<REFNUM>202501151505320</REFNUM>
<NAME>AMAZON.COM</NAME>
</STMTTRN>
</BANKTRANLIST>
</CCSTMTRS>
</CCSTMTTRNRS>
</CREDITCARDMSGSRSV1>
</OFX>
"""
    path.write_text(content)


def _write_mercury_csv(path: Path) -> None:
    """Write a minimal Mercury CSV file."""
    content = (
        '"Date","Description","Amount","Status","Bank Description",'
        '"Source Account","Mercury Category","Timestamp"\n'
        '"01-15-2025","Amazon",-25.00,"Sent","AMAZON.COM",'
        '"Mercury Checking \u2022\u20221234","Other","01-15-2025 10:30:00"\n'
    )
    path.write_text(content)


def _write_budget_app_csv(path: Path) -> None:
    """Write a minimal budget app CSV file with BOM."""
    content = (
        '\ufeff"Account","Flag","Date","Payee","Category Group/Category",'
        '"Category Group","Category","Memo","Outflow","Inflow","Cleared"\n'
        '"My Checking","","01/15/2025","Grocery Store",'
        '"Monthly Needs: Groceries","Monthly Needs","Groceries","",'
        '"$42.50","$0.00","Cleared"\n'
    )
    path.write_text(content)


def _make_config(accounts=None):
    """Create a mock Config with accounts list."""
    config = MagicMock()
    config.accounts = accounts or [
        {"id": "wf-checking", "qfx_acctid": "1234567890"},
        {"id": "wf-savings", "qfx_acctid": "0987654321"},
        {"id": "cap1-credit", "qfx_acctid": "0217"},
        {"id": "cap1-savings", "qfx_acctid": "7791"},
        {"id": "amex-blue", "qfx_acctid": "TESTACCTID12345|99999"},
        {"id": "golden1-auto", "qfx_acctid": "9999999=1"},
    ]
    config.budget_app_category_map = {}
    return config


# ── wait_for_stable() tests ──────────────────────────────


class TestWaitForStable:
    """Tests for wait_for_stable() file stability monitor."""

    def test_stable_file_returns_quickly(self, tmp_path):
        """A file that doesn't change should stabilize."""
        f = tmp_path / "test.qfx"
        f.write_text("stable content")

        # Use very short stability/check intervals for fast test
        with patch("src.watcher.observer.time.sleep"):
            wait_for_stable(f, stability_seconds=0, check_interval=0.01)
        # No exception = success

    def test_timeout_when_max_wait_exceeded(self, tmp_path):
        """A file that hasn't been stable long enough times out at max_wait."""
        f = tmp_path / "test.qfx"
        f.write_text("initial")

        # With stability_seconds=100 and max_wait=0.001, the file can never
        # be stable long enough before timeout kicks in
        with patch("src.watcher.observer.time.sleep"):
            with pytest.raises(TimeoutError, match="did not stabilize"):
                wait_for_stable(
                    f, stability_seconds=100, check_interval=0.01, max_wait=0.001,
                )

    def test_timeout_raises_with_filepath(self, tmp_path):
        """TimeoutError message includes the filepath."""
        f = tmp_path / "growing.csv"
        f.write_text("data")

        with patch("src.watcher.observer.time.sleep"):
            with pytest.raises(TimeoutError, match="growing.csv"):
                wait_for_stable(f, stability_seconds=100, max_wait=0.001)


# ── validate_file_completeness() tests ───────────────────


class TestValidateFileCompleteness:
    """Tests for validate_file_completeness() post-stability validation."""

    def test_csv_ending_with_newline(self, tmp_path):
        """CSV files ending with newline pass validation."""
        f = tmp_path / "valid.csv"
        f.write_text("header\nrow1\n")
        validate_file_completeness(f)  # No exception

    def test_csv_ending_with_cr(self, tmp_path):
        """CSV files ending with carriage return pass validation."""
        f = tmp_path / "valid.csv"
        f.write_bytes(b"header\r\nrow1\r")
        validate_file_completeness(f)  # No exception

    def test_csv_no_trailing_newline_fails(self, tmp_path):
        """CSV files without trailing newline raise FileStabilityError."""
        f = tmp_path / "incomplete.csv"
        f.write_text("header\nrow1")  # No trailing newline
        with pytest.raises(FileStabilityError, match="does not end with newline"):
            validate_file_completeness(f)

    def test_empty_csv_fails(self, tmp_path):
        """Empty CSV files raise FileStabilityError."""
        f = tmp_path / "empty.csv"
        f.write_text("")
        with pytest.raises(FileStabilityError, match="Empty CSV"):
            validate_file_completeness(f)

    def test_qfx_with_closing_tag(self, tmp_path):
        """QFX files with </OFX> pass validation."""
        f = tmp_path / "valid.qfx"
        _write_sgml_qfx(f)
        validate_file_completeness(f)  # No exception

    def test_qfx_missing_closing_tag(self, tmp_path):
        """QFX files without </OFX> raise FileStabilityError."""
        f = tmp_path / "incomplete.qfx"
        f.write_text("<OFX><BANKMSGSRSV1>...")
        with pytest.raises(FileStabilityError, match="missing closing </OFX>"):
            validate_file_completeness(f)

    def test_ofx_extension_validated(self, tmp_path):
        """OFX files are validated the same as QFX files."""
        f = tmp_path / "valid.ofx"
        f.write_text("<OFX>content</OFX>")
        validate_file_completeness(f)  # No exception

    def test_ofx_missing_tag_fails(self, tmp_path):
        """OFX files without </OFX> raise FileStabilityError."""
        f = tmp_path / "incomplete.ofx"
        f.write_text("<OFX>content truncated")
        with pytest.raises(FileStabilityError, match="missing closing </OFX>"):
            validate_file_completeness(f)

    def test_unknown_extension_passes(self, tmp_path):
        """Files with unknown extensions are not validated (no error)."""
        f = tmp_path / "data.txt"
        f.write_text("any content")
        validate_file_completeness(f)  # No exception


# ── _extract_acctid() tests ──────────────────────────────


class TestExtractAcctId:
    """Tests for ACCTID extraction from QFX/OFX files."""

    def test_sgml_acctid(self, tmp_path):
        """Extracts ACCTID from SGML QFX."""
        f = tmp_path / "test.qfx"
        _write_sgml_qfx(f, acctid="1234567890")
        assert _extract_acctid(f) == "1234567890"

    def test_xml_acctid(self, tmp_path):
        """Extracts ACCTID from XML QFX."""
        f = tmp_path / "test.qfx"
        _write_xml_qfx(f, acctid="0217")
        assert _extract_acctid(f) == "0217"

    def test_special_chars_acctid(self, tmp_path):
        """Extracts ACCTID with special characters."""
        f = tmp_path / "test.qfx"
        f.write_text("<ACCTID>TESTACCTID12345|99999\n</OFX>")
        assert _extract_acctid(f) == "TESTACCTID12345|99999"

    def test_golden1_acctid(self, tmp_path):
        """Extracts ACCTID with = character (Golden1)."""
        f = tmp_path / "test.qfx"
        f.write_text("<ACCTID>9999999=1\n</OFX>")
        assert _extract_acctid(f) == "9999999=1"

    def test_no_acctid_returns_none(self, tmp_path):
        """Files without ACCTID return None."""
        f = tmp_path / "test.qfx"
        f.write_text("<OFX>no account id here</OFX>")
        assert _extract_acctid(f) is None

    def test_unreadable_file_returns_none(self, tmp_path):
        """Unreadable files return None gracefully."""
        f = tmp_path / "nonexistent.qfx"
        assert _extract_acctid(f) is None


# ── _resolve_account_id() tests ──────────────────────────


class TestResolveAccountId:
    """Tests for QFX ACCTID to internal account_id mapping."""

    def test_maps_known_acctid(self):
        """Known ACCTID maps to internal account_id."""
        config = _make_config()
        assert _resolve_account_id("1234567890", config) == "wf-checking"
        assert _resolve_account_id("0217", config) == "cap1-credit"

    def test_unknown_acctid_falls_back(self):
        """Unknown ACCTID returns the raw value."""
        config = _make_config()
        assert _resolve_account_id("UNKNOWN123", config) == "UNKNOWN123"

    def test_none_config_returns_raw(self):
        """None config returns the raw ACCTID."""
        assert _resolve_account_id("1234567890", None) == "1234567890"


# ── detect_parser() tests ────────────────────────────────


class TestDetectParser:
    """Tests for auto-detection of file parsers."""

    def test_sgml_qfx_detected(self, tmp_path):
        """SGML QFX files (Wells Fargo) are detected correctly."""
        f = tmp_path / "wf-checking.qfx"
        _write_sgml_qfx(f, acctid="1234567890")
        config = _make_config()
        parser = detect_parser(f, config)
        from src.parsers.qfx_sgml import QfxSgmlParser
        assert isinstance(parser, QfxSgmlParser)
        assert parser.account_id == "wf-checking"

    def test_xml_qfx_detected(self, tmp_path):
        """XML QFX files (Capital One) are detected correctly."""
        f = tmp_path / "cap1-credit.qfx"
        _write_xml_qfx(f, acctid="0217")
        config = _make_config()
        parser = detect_parser(f, config)
        from src.parsers.qfx_xml import QfxXmlParser
        assert isinstance(parser, QfxXmlParser)
        assert parser.account_id == "cap1-credit"

    def test_mercury_csv_detected(self, tmp_path):
        """Mercury CSV files are detected correctly."""
        f = tmp_path / "mercury.csv"
        _write_mercury_csv(f)
        parser = detect_parser(f)
        from src.parsers.csv_parser import MercuryCsvParser
        assert isinstance(parser, MercuryCsvParser)

    def test_budget_app_csv_detected(self, tmp_path):
        """Budget app CSV files are detected correctly."""
        f = tmp_path / "register.csv"
        _write_budget_app_csv(f)
        parser = detect_parser(f)
        from src.parsers.budget_app import BudgetAppCsvParser
        assert isinstance(parser, BudgetAppCsvParser)

    def test_unknown_file_raises(self, tmp_path):
        """Unknown files raise ValueError."""
        f = tmp_path / "random.csv"
        f.write_text("not a recognized format\n")
        with pytest.raises(ValueError, match="No parser found"):
            detect_parser(f)

    def test_unsupported_extension_raises(self, tmp_path):
        """Unsupported extensions raise ValueError."""
        f = tmp_path / "data.pdf"
        f.write_text("PDF content")
        with pytest.raises(ValueError, match="No parser found"):
            detect_parser(f)

    def test_qfx_without_config_uses_raw_acctid(self, tmp_path):
        """QFX files without config use the raw ACCTID as account_id."""
        f = tmp_path / "test.qfx"
        _write_sgml_qfx(f, acctid="1234567890")
        parser = detect_parser(f, config=None)
        from src.parsers.qfx_sgml import QfxSgmlParser
        assert isinstance(parser, QfxSgmlParser)
        assert parser.account_id == "1234567890"

    def test_qfx_without_acctid_uses_unknown(self, tmp_path):
        """QFX files without ACCTID use 'unknown' as account_id."""
        f = tmp_path / "no-acctid.qfx"
        # Minimal SGML that detects as SGML but has no ACCTID
        f.write_text("OFXHEADER:100\n<OFX>\n<BANKMSGSRSV1>\n</OFX>\n")
        parser = detect_parser(f)
        from src.parsers.qfx_sgml import QfxSgmlParser
        assert isinstance(parser, QfxSgmlParser)
        assert parser.account_id == "unknown"


# ── ImportResult tests ───────────────────────────────────


class TestImportResult:
    """Tests for ImportResult dataclass."""

    def test_default_counts(self):
        result = ImportResult(file_name="test.qfx", status="success")
        assert result.new_count == 0
        assert result.duplicate_count == 0
        assert result.flagged_count == 0
        assert result.categorized_count == 0
        assert result.error_message is None

    def test_error_result(self):
        result = ImportResult(
            file_name="bad.csv", status="error",
            error_message="Parse failed",
        )
        assert result.status == "error"
        assert result.error_message == "Parse failed"


# ── ImportPipeline.process_file() tests ──────────────────


class TestImportPipeline:
    """Tests for the ImportPipeline orchestration."""

    def _make_pipeline(self, *, sheets=None, receipt_lookup=None):
        """Create an ImportPipeline with mock dependencies."""
        repo = MagicMock()
        config = _make_config()
        dedup = MagicMock()
        return ImportPipeline(
            repo=repo,
            config=config,
            dedup=dedup,
            sheets=sheets,
            receipt_lookup=receipt_lookup,
        ), repo, dedup

    def test_unsupported_extension(self, tmp_path):
        """Files with unsupported extensions return error result."""
        pipeline, _, _ = self._make_pipeline()
        f = tmp_path / "readme.txt"
        f.write_text("not a bank file")
        result = pipeline.process_file(f)
        assert result.status == "error"
        assert "Unsupported file extension" in result.error_message

    def test_duplicate_file_skipped(self, tmp_path):
        """Duplicate files (Tier 1 dedup) are skipped."""
        pipeline, _, dedup = self._make_pipeline()
        dedup.check_file_duplicate.return_value = True

        f = tmp_path / "dup.qfx"
        _write_sgml_qfx(f)
        result = pipeline.process_file(f)
        assert result.status == "duplicate"

    def test_successful_import(self, tmp_path):
        """Full successful import: parse, dedup, categorize."""
        pipeline, repo, dedup = self._make_pipeline()
        dedup.check_file_duplicate.return_value = False

        # Mock dedup batch result
        mock_txn = MagicMock()
        mock_txn.status = "pending"
        mock_txn.id = "txn-1"

        batch_result = MagicMock()
        batch_result.new_count = 1
        batch_result.duplicate_count = 0
        batch_result.flagged_count = 0
        batch_result.transactions = [mock_txn]
        dedup.process_batch.return_value = batch_result

        f = tmp_path / "test.qfx"
        _write_sgml_qfx(f, acctid="1234567890")

        with patch("src.watcher.observer.categorize_transaction") as mock_cat, \
             patch("src.watcher.observer.apply_categorization"):
            mock_cat.return_value = MagicMock(method="merchant_auto")
            result = pipeline.process_file(f)

        assert result.status == "success"
        assert result.new_count == 1
        assert result.categorized_count == 1
        repo.insert_import.assert_called_once()
        repo.update_import_status.assert_called_once()

    def test_empty_file_returns_success(self, tmp_path):
        """Files that parse to 0 transactions return success with 0 counts."""
        pipeline, repo, dedup = self._make_pipeline()
        dedup.check_file_duplicate.return_value = False

        f = tmp_path / "test.qfx"
        _write_sgml_qfx(f)

        with patch("src.watcher.observer.detect_parser") as mock_detect:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = []  # No transactions
            mock_parser.skipped_count = 0
            mock_detect.return_value = mock_parser
            result = pipeline.process_file(f)

        assert result.status == "success"
        assert result.new_count == 0

    def test_parse_error_records_error(self, tmp_path):
        """Parse errors are recorded in the import status."""
        pipeline, repo, dedup = self._make_pipeline()
        dedup.check_file_duplicate.return_value = False

        f = tmp_path / "bad.qfx"
        _write_sgml_qfx(f)

        with patch("src.watcher.observer.detect_parser") as mock_detect:
            mock_detect.side_effect = ValueError("No parser found")
            result = pipeline.process_file(f)

        assert result.status == "error"
        assert "No parser found" in result.error_message
        repo.update_import_status.assert_called_once()
        call_args = repo.update_import_status.call_args
        assert call_args[0][1] == "error"

    def test_sheets_push_called(self, tmp_path):
        """When sheets is configured, push_transactions is called."""
        mock_sheets = MagicMock()
        pipeline, repo, dedup = self._make_pipeline(sheets=mock_sheets)
        dedup.check_file_duplicate.return_value = False

        mock_txn = MagicMock()
        mock_txn.status = "pending"
        mock_txn.id = "txn-1"

        batch_result = MagicMock()
        batch_result.new_count = 1
        batch_result.duplicate_count = 0
        batch_result.flagged_count = 0
        batch_result.transactions = [mock_txn]
        dedup.process_batch.return_value = batch_result
        repo.get_allocations_by_transaction.return_value = []

        f = tmp_path / "test.qfx"
        _write_sgml_qfx(f)

        with patch("src.watcher.observer.categorize_transaction") as mock_cat, \
             patch("src.watcher.observer.apply_categorization"):
            mock_cat.return_value = MagicMock(method="merchant_auto")
            result = pipeline.process_file(f)

        assert result.status == "success"
        mock_sheets.push_transactions.assert_called_once()

    def test_sheets_error_does_not_fail_import(self, tmp_path):
        """Sheets push errors don't cause the import to fail."""
        mock_sheets = MagicMock()
        mock_sheets.push_transactions.side_effect = Exception("Sheets API error")
        pipeline, repo, dedup = self._make_pipeline(sheets=mock_sheets)
        dedup.check_file_duplicate.return_value = False

        mock_txn = MagicMock()
        mock_txn.status = "pending"
        mock_txn.id = "txn-1"

        batch_result = MagicMock()
        batch_result.new_count = 1
        batch_result.duplicate_count = 0
        batch_result.flagged_count = 0
        batch_result.transactions = [mock_txn]
        dedup.process_batch.return_value = batch_result

        f = tmp_path / "test.qfx"
        _write_sgml_qfx(f)

        with patch("src.watcher.observer.categorize_transaction") as mock_cat, \
             patch("src.watcher.observer.apply_categorization"):
            mock_cat.return_value = MagicMock(method="merchant_auto")
            result = pipeline.process_file(f)

        # Import succeeds despite Sheets failure
        assert result.status == "success"
        assert result.new_count == 1

    def test_only_pending_transactions_categorized(self, tmp_path):
        """Only transactions with status='pending' are categorized."""
        pipeline, repo, dedup = self._make_pipeline()
        dedup.check_file_duplicate.return_value = False

        pending_txn = MagicMock()
        pending_txn.status = "pending"
        pending_txn.id = "txn-1"

        dup_txn = MagicMock()
        dup_txn.status = "duplicate"
        dup_txn.id = "txn-2"

        batch_result = MagicMock()
        batch_result.new_count = 1
        batch_result.duplicate_count = 1
        batch_result.flagged_count = 0
        batch_result.transactions = [pending_txn, dup_txn]
        dedup.process_batch.return_value = batch_result

        f = tmp_path / "test.qfx"
        _write_sgml_qfx(f)

        with patch("src.watcher.observer.categorize_transaction") as mock_cat, \
             patch("src.watcher.observer.apply_categorization"):
            mock_cat.return_value = MagicMock(method="merchant_auto")
            result = pipeline.process_file(f)

        assert result.categorized_count == 1
        assert mock_cat.call_count == 1


# ── FileWatcher tests ────────────────────────────────────


class TestFileWatcher:
    """Tests for FileWatcher event handling."""

    def _make_watcher(self, tmp_path):
        """Create a FileWatcher with mock pipeline."""
        pipeline = MagicMock()
        watcher = FileWatcher(
            watch_dir=tmp_path / "drop",
            pipeline=pipeline,
            stability_seconds=0,
            check_interval=0.01,
        )
        return watcher, pipeline

    def test_on_created_processes_qfx(self, tmp_path):
        """on_created processes .qfx files."""
        watcher, pipeline = self._make_watcher(tmp_path)
        pipeline.process_file.return_value = ImportResult(
            file_name="test.qfx", status="success", new_count=1,
        )

        event = MagicMock()
        event.is_directory = False
        event.src_path = str(tmp_path / "drop" / "test.qfx")

        # Write actual file so stability check works
        drop = tmp_path / "drop"
        drop.mkdir()
        f = drop / "test.qfx"
        _write_sgml_qfx(f)

        with patch("src.watcher.observer.wait_for_stable"), \
             patch("src.watcher.observer.validate_file_completeness"):
            watcher.on_created(event)

        pipeline.process_file.assert_called_once()

    def test_on_created_skips_directories(self, tmp_path):
        """on_created ignores directory events."""
        watcher, pipeline = self._make_watcher(tmp_path)

        event = MagicMock()
        event.is_directory = True
        event.src_path = str(tmp_path / "drop" / "subdir")

        watcher.on_created(event)
        pipeline.process_file.assert_not_called()

    def test_on_created_skips_unsupported_extension(self, tmp_path):
        """on_created ignores files with unsupported extensions."""
        watcher, pipeline = self._make_watcher(tmp_path)

        event = MagicMock()
        event.is_directory = False
        event.src_path = str(tmp_path / "drop" / "readme.txt")

        watcher.on_created(event)
        pipeline.process_file.assert_not_called()

    def test_on_created_processes_csv(self, tmp_path):
        """on_created processes .csv files."""
        watcher, pipeline = self._make_watcher(tmp_path)
        pipeline.process_file.return_value = ImportResult(
            file_name="mercury.csv", status="success",
        )

        drop = tmp_path / "drop"
        drop.mkdir()
        f = drop / "mercury.csv"
        _write_mercury_csv(f)

        event = MagicMock()
        event.is_directory = False
        event.src_path = str(f)

        with patch("src.watcher.observer.wait_for_stable"), \
             patch("src.watcher.observer.validate_file_completeness"):
            watcher.on_created(event)

        pipeline.process_file.assert_called_once()

    def test_process_file_handles_stability_error(self, tmp_path):
        """_process_file catches FileStabilityError and returns error result."""
        watcher, pipeline = self._make_watcher(tmp_path)

        drop = tmp_path / "drop"
        drop.mkdir()
        f = drop / "incomplete.qfx"
        f.write_text("<OFX>no closing tag")

        with patch("src.watcher.observer.wait_for_stable"):
            with patch(
                "src.watcher.observer.validate_file_completeness",
                side_effect=FileStabilityError("missing </OFX>"),
            ):
                result = watcher._process_file(f)

        assert result.status == "error"
        assert "missing </OFX>" in result.error_message

    def test_process_file_handles_timeout(self, tmp_path):
        """_process_file catches TimeoutError and returns error result."""
        watcher, pipeline = self._make_watcher(tmp_path)

        drop = tmp_path / "drop"
        drop.mkdir()
        f = drop / "growing.qfx"
        f.write_text("content")

        with patch(
            "src.watcher.observer.wait_for_stable",
            side_effect=TimeoutError("File did not stabilize"),
        ):
            result = watcher._process_file(f)

        assert result.status == "error"
        assert "did not stabilize" in result.error_message

    def test_start_creates_watch_dir(self, tmp_path):
        """start() creates the watch directory if it doesn't exist."""
        watcher, _ = self._make_watcher(tmp_path)
        watch_dir = tmp_path / "drop"
        assert not watch_dir.exists()

        with patch("src.watcher.observer.PollingObserver", create=True) as MockObs:
            mock_obs = MagicMock()
            MockObs.return_value = mock_obs
            with patch(
                "watchdog.observers.polling.PollingObserver",
                return_value=mock_obs,
            ):
                watcher.start()

        assert watch_dir.exists()
        watcher.stop()

    def test_stop_without_start(self, tmp_path):
        """stop() when no observer is running doesn't raise."""
        watcher, _ = self._make_watcher(tmp_path)
        watcher.stop()  # No exception


# ── SUPPORTED_EXTENSIONS tests ───────────────────────────


class TestSupportedExtensions:
    """Tests for module constants."""

    def test_supported_extensions(self):
        assert ".qfx" in SUPPORTED_EXTENSIONS
        assert ".ofx" in SUPPORTED_EXTENSIONS
        assert ".csv" in SUPPORTED_EXTENSIONS
        assert ".pdf" not in SUPPORTED_EXTENSIONS
        assert ".txt" not in SUPPORTED_EXTENSIONS
