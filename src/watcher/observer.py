"""File watcher: PollingObserver + ImportPipeline orchestration.

Watches a drop folder for new bank files (.qfx, .ofx, .csv), waits for
file stability (size+mtime stable for 10s), validates file completeness,
auto-detects the parser, then runs the full import pipeline:
  detect → stable → parse → dedup → categorize → push

Uses PollingObserver as primary (not fallback) due to NAS/Docker volume
unreliability with inotify. 30-second polling interval.
"""

from __future__ import annotations

import logging
import time
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from watchdog.events import FileSystemEventHandler

from src.categorize.pipeline import (
    apply_categorization,
    categorize_transaction,
)
from src.config import Config
from src.database.dedup import DedupEngine
from src.database.models import Import
from src.database.repository import DuplicateImportError
from src.parsers.base import BaseParser, compute_file_hash
from src.parsers.csv_parser import MercuryCsvParser
from src.parsers.qfx_sgml import QfxSgmlParser
from src.parsers.qfx_xml import QfxXmlParser
from src.parsers.budget_app import BudgetAppCsvParser

if TYPE_CHECKING:
    from src.database.repository import Repository
    from src.sheets.push import SheetsPush

logger = logging.getLogger(__name__)

# File extensions we accept
SUPPORTED_EXTENSIONS = {".qfx", ".ofx", ".csv"}

# Default stability check parameters
DEFAULT_STABILITY_SECONDS = 10
DEFAULT_CHECK_INTERVAL = 2.0

# Default polling interval for PollingObserver
DEFAULT_POLL_INTERVAL = 30


@dataclass
class ImportResult:
    """Result of importing a single file."""
    file_name: str
    status: str  # "success", "duplicate", "error"
    new_count: int = 0
    duplicate_count: int = 0
    flagged_count: int = 0
    categorized_count: int = 0
    skipped_count: int = 0  # Rows skipped by parser (e.g., unmapped accounts)
    error_message: str | None = None


class FileStabilityError(Exception):
    """Raised when a file fails post-stability validation."""


# ── File stability & validation ──────────────────────────


def wait_for_stable(
    filepath: Path,
    stability_seconds: int = DEFAULT_STABILITY_SECONDS,
    check_interval: float = DEFAULT_CHECK_INTERVAL,
    max_wait: float = 300.0,
) -> None:
    """Wait until file size and mtime are stable for stability_seconds.

    Args:
        filepath: Path to the file to monitor.
        stability_seconds: Seconds of no change required.
        check_interval: How often to poll the file stats.
        max_wait: Maximum total seconds to wait before raising.

    Raises:
        TimeoutError: If file doesn't stabilize within max_wait.
    """
    prev_size = -1
    prev_mtime = -1.0
    stable_since: float | None = None
    start = time.monotonic()

    while True:
        if time.monotonic() - start > max_wait:
            raise TimeoutError(
                f"File did not stabilize within {max_wait}s: {filepath}"
            )

        stat = filepath.stat()
        if stat.st_size == prev_size and stat.st_mtime == prev_mtime:
            if stable_since is None:
                stable_since = time.monotonic()
            elif time.monotonic() - stable_since >= stability_seconds:
                return  # File is stable
        else:
            stable_since = None

        prev_size = stat.st_size
        prev_mtime = stat.st_mtime
        time.sleep(check_interval)


def validate_file_completeness(filepath: Path) -> None:
    """Post-stability validation: ensure file content is complete.

    - CSV files: must end with a newline
    - QFX/OFX files: must contain closing </OFX> tag

    Raises:
        FileStabilityError: If file appears incomplete.
    """
    suffix = filepath.suffix.lower()

    if suffix == ".csv":
        with open(filepath, "rb") as f:
            # Check last byte is newline
            f.seek(0, 2)  # Seek to end
            size = f.tell()
            if size == 0:
                raise FileStabilityError(f"Empty CSV file: {filepath}")
            f.seek(size - 1)
            last_byte = f.read(1)
            if last_byte not in (b"\n", b"\r"):
                raise FileStabilityError(
                    f"CSV file does not end with newline: {filepath}"
                )

    elif suffix in (".qfx", ".ofx"):
        content = filepath.read_text(errors="replace")
        if "</OFX>" not in content.upper():
            raise FileStabilityError(
                f"QFX/OFX file missing closing </OFX> tag: {filepath}"
            )


# ── Parser auto-detection ─────────────────────────────────


def _extract_acctid(filepath: Path) -> str | None:
    """Extract ACCTID from a QFX/OFX file.

    Works for both SGML and XML formats.
    """
    try:
        content = filepath.read_text(errors="replace")
        match = re.search(r'<ACCTID>([^<\n\r]+)', content)
        if match:
            return match.group(1).strip()
    except OSError:
        pass
    return None


def _resolve_account_id(acctid: str, config: Config | None) -> str:
    """Map a QFX ACCTID to an internal account_id using config.

    Falls back to the raw ACCTID if no config or no match.
    """
    if config is None:
        return acctid

    for acct in config.accounts:
        if acct.get("qfx_acctid") == acctid:
            return acct["id"]

    # Don't log the actual ACCTID as it's a sensitive bank account identifier
    logger.warning("No account mapping found for ACCTID (masked for security)")
    return acctid


def detect_parser(filepath: Path, config: Config | None = None) -> BaseParser:
    """Auto-detect the appropriate parser for a bank file.

    Tries each parser's detect() method. For QFX parsers, extracts the
    ACCTID from the file and maps it to an internal account_id using
    the accounts config.

    Args:
        filepath: Path to the bank file.
        config: Optional Config for budget app category map and account mapping.

    Returns:
        An instantiated parser ready to parse the file.

    Raises:
        ValueError: If no parser can handle the file.
    """
    suffix = filepath.suffix.lower()

    # CSV detection: Mercury vs budget app
    if suffix == ".csv":
        mercury = MercuryCsvParser(
            account_routing=config.mercury_account_routing if config else None,
        )
        if mercury.detect(filepath):
            return mercury

        budget_app = BudgetAppCsvParser(
            category_map=config.budget_app_category_map if config else None,
            account_routing=config.budget_app_account_routing if config else None,
        )
        if budget_app.detect(filepath):
            return budget_app

    # QFX/OFX detection: SGML vs XML
    if suffix in (".qfx", ".ofx"):
        # Extract account ID from file content
        acctid = _extract_acctid(filepath)
        account_id = _resolve_account_id(acctid, config) if acctid else "unknown"

        # Try SGML first (Wells Fargo, Golden1)
        sgml = QfxSgmlParser(account_id=account_id)
        if sgml.detect(filepath):
            return sgml

        # Try XML (Capital One, Amex)
        xml = QfxXmlParser(account_id=account_id)
        if xml.detect(filepath):
            return xml

    raise ValueError(f"No parser found for file: {filepath}")


# ── Import pipeline ──────────────────────────────────────


class ImportPipeline:
    """Orchestrate: detect → stable → parse → dedup → categorize → push.

    Args:
        repo: Database repository.
        config: Application config.
        dedup: Dedup engine.
        sheets: Optional SheetsPush for Google Sheets sync.
        receipt_lookup: Optional ReceiptLookup for Step 6.
    """

    def __init__(
        self,
        repo: Repository,
        config: Config,
        dedup: DedupEngine,
        sheets: SheetsPush | None = None,
        receipt_lookup=None,
        claude_fn=None,
    ):
        self.repo = repo
        self.config = config
        self.dedup = dedup
        self.sheets = sheets
        self.receipt_lookup = receipt_lookup
        self.claude_fn = claude_fn

    def process_file(self, filepath: Path) -> ImportResult:
        """Run the full import pipeline on a single file.

        Steps:
        1. Check file extension
        2. Tier 1 dedup: file hash check
        3. Record import in DB
        4. Auto-detect parser
        5. Parse file
        6. Tier 2-4 dedup + insert transactions
        7. Categorize new transactions
        8. Push to Sheets (if configured)

        Returns ImportResult with counts.
        """
        file_name = filepath.name

        # Step 1: Check extension
        if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
            return ImportResult(
                file_name=file_name,
                status="error",
                error_message=f"Unsupported file extension: {filepath.suffix}",
            )

        # Step 2: File hash dedup (Tier 1)
        file_hash = compute_file_hash(filepath)
        if self.dedup.check_file_duplicate(filepath):
            logger.info("Duplicate file skipped: %s", file_name)
            return ImportResult(file_name=file_name, status="duplicate")

        # Step 3: Record import (handles race condition via unique constraint)
        imp = Import(
            file_name=file_name,
            file_hash=file_hash,
            file_size=filepath.stat().st_size,
        )
        try:
            self.repo.insert_import(imp)
        except DuplicateImportError:
            # Race condition: another process imported this file between
            # our check and insert. This is the same as a duplicate.
            logger.info("Duplicate file (race): %s", file_name)
            return ImportResult(file_name=file_name, status="duplicate")

        try:
            # Step 4: Auto-detect parser
            parser = detect_parser(filepath, self.config)

            # Step 5: Parse
            raw_txns = parser.parse(filepath)
            skipped_count = parser.skipped_count

            if skipped_count > 0:
                logger.warning(
                    "Parser skipped %d row(s) in %s (check account routing config)",
                    skipped_count, file_name,
                )

            if not raw_txns:
                self.repo.update_import_status(
                    imp.id, "completed", record_count=0,
                )
                return ImportResult(
                    file_name=file_name, status="success", skipped_count=skipped_count,
                )

            # Step 6: Dedup + insert
            batch_result = self.dedup.process_batch(
                raw_txns, imp.id, source="bank",
            )

            # Step 7: Categorize new transactions
            # Include both pending and flagged (fuzzy dedup matches need categorization too)
            categorized_count = 0
            for txn in batch_result.transactions:
                if txn.status in ("pending", "flagged"):
                    result = categorize_transaction(
                        txn, self.config,
                        receipt_lookup=self.receipt_lookup,
                        claude_fn=self.claude_fn,
                        repo=self.repo,
                    )
                    apply_categorization(
                        txn, result, self.repo,
                        receipt_lookup=self.receipt_lookup,
                    )
                    categorized_count += 1

            # Step 8: Push to Sheets
            # Re-fetch transactions from DB so status/confidence/method are current
            if self.sheets is not None:
                try:
                    fresh_txns = self.repo.get_transactions_by_import_id(imp.id)
                    self.sheets.push_transactions(fresh_txns, self.repo)
                    txn_ids = [t.id for t in fresh_txns]
                    alloc_map = self.repo.get_allocations_by_transaction_ids(txn_ids)
                    allocs = [a for al in alloc_map.values() for a in al]
                    if allocs:
                        self.sheets.push_allocations(allocs)
                    self.sheets.push_review(fresh_txns, self.repo)
                except Exception:
                    logger.exception("Sheets push failed for %s", file_name)

            # Update import status
            self.repo.update_import_status(
                imp.id, "completed",
                record_count=len(raw_txns),
            )

            return ImportResult(
                file_name=file_name,
                status="success",
                new_count=batch_result.new_count,
                duplicate_count=batch_result.duplicate_count,
                flagged_count=batch_result.flagged_count,
                categorized_count=categorized_count,
                skipped_count=skipped_count,
            )

        except Exception as e:
            logger.exception("Import failed for %s", file_name)
            self.repo.update_import_status(
                imp.id, "error",
                error_message=str(e),
            )
            return ImportResult(
                file_name=file_name,
                status="error",
                error_message=str(e),
            )


# ── File watcher ─────────────────────────────────────────


class FileWatcher(FileSystemEventHandler):
    """Watch a drop folder for new bank files using PollingObserver.

    Primary watcher (not fallback). 30-second polling interval.
    Processes files sequentially to avoid database contention.

    Args:
        watch_dir: Directory to watch for new files.
        pipeline: ImportPipeline to process files.
        stability_seconds: Seconds of stability before processing.
        check_interval: Seconds between stability checks.
    """

    def __init__(
        self,
        watch_dir: Path,
        pipeline: ImportPipeline,
        stability_seconds: int = DEFAULT_STABILITY_SECONDS,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
    ):
        self.watch_dir = Path(watch_dir)
        self.pipeline = pipeline
        self.stability_seconds = stability_seconds
        self.check_interval = check_interval
        self._observer = None

    def start(self) -> None:
        """Start watching the drop folder."""
        from watchdog.observers.polling import PollingObserver

        if not self.watch_dir.exists():
            self.watch_dir.mkdir(parents=True, exist_ok=True)

        self._observer = PollingObserver(timeout=DEFAULT_POLL_INTERVAL)
        self._observer.schedule(self, str(self.watch_dir), recursive=False)
        self._observer.start()
        logger.info("Watching %s for new bank files", self.watch_dir)

    def stop(self) -> None:
        """Stop watching."""
        if self._observer is not None:
            self._observer.stop()
            self._observer.join()
            self._observer = None
            logger.info("File watcher stopped")

    def on_created(self, event) -> None:
        """Handle new file creation events."""
        if event.is_directory:
            return

        filepath = Path(event.src_path)

        # Skip unsupported extensions
        if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
            return

        logger.info("New file detected: %s", filepath.name)
        self._process_file(filepath)

    def _process_file(self, filepath: Path) -> ImportResult | None:
        """Wait for stability, validate, then import."""
        try:
            # Wait for file to be fully written
            wait_for_stable(
                filepath,
                stability_seconds=self.stability_seconds,
                check_interval=self.check_interval,
            )

            # Validate completeness
            validate_file_completeness(filepath)

            # Run import pipeline
            result = self.pipeline.process_file(filepath)
            logger.info(
                "Import result for %s: %s (new=%d, dup=%d, flagged=%d)",
                filepath.name, result.status,
                result.new_count, result.duplicate_count, result.flagged_count,
            )
            return result

        except FileStabilityError as e:
            logger.error("File validation failed: %s", e)
            return ImportResult(
                file_name=filepath.name,
                status="error",
                error_message=str(e),
            )
        except TimeoutError as e:
            logger.error("File stability timeout: %s", e)
            return ImportResult(
                file_name=filepath.name,
                status="error",
                error_message=str(e),
            )
        except Exception as e:
            logger.exception("Unexpected error processing %s", filepath.name)
            return ImportResult(
                file_name=filepath.name,
                status="error",
                error_message=str(e),
            )
