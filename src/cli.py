"""CLI entry point for MoMoney.

Commands:
    momoney import [--file PATH]     Import specific file or all pending
    momoney watch                    Start file watcher daemon
    momoney status                   Pending imports, review items, last sync
    momoney review                   List items needing manual review
    momoney reconcile [ACCOUNT]      Run balance reconciliation
    momoney push                     Force full Sheets rebuild
    momoney poll                     Poll Sheets for override edits
    momoney import-budget-app FILE   One-time budget app historical import
    momoney category list            Print category hierarchy tree
    momoney category add PARENT ID NAME   Add a new category
    momoney category rename ID NAME  Rename a category
    momoney category move ID PARENT  Move a category to a new parent
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    """Configure logging based on FINANCE_LOG_LEVEL env var."""
    level = os.environ.get("FINANCE_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _get_config():
    """Load application config from config directory."""
    from src.config import Config

    config_dir = os.environ.get("FINANCE_CONFIG_DIR", "config")
    return Config(config_dir=config_dir)


def _get_repo():
    """Create a Repository connected to the configured database."""
    from src.database.repository import Repository

    db_path = os.environ.get("FINANCE_DB_PATH", "finance.db")
    return Repository(db_path=db_path)


def _get_dedup(repo):
    """Create a DedupEngine."""
    from src.database.dedup import DedupEngine

    return DedupEngine(repo)


def _get_spreadsheet():
    """Open the Google Sheets spreadsheet if configured.

    Returns a gspread.Spreadsheet instance, or None if not configured.
    """
    spreadsheet_id = os.environ.get("FINANCE_SPREADSHEET_ID")
    credentials_path = os.environ.get("FINANCE_CREDENTIALS")

    if not spreadsheet_id or not credentials_path:
        return None

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        gc = gspread.authorize(creds)
        return gc.open_by_key(spreadsheet_id)
    except Exception as e:
        logging.getLogger(__name__).warning("Sheets not available: %s", e)
        return None


def _get_sheets():
    """Create a SheetsPush if credentials and spreadsheet ID are configured.

    Returns None if not configured (sheets push will be skipped).
    """
    spreadsheet = _get_spreadsheet()
    if spreadsheet is None:
        return None

    from src.sheets.push import SheetsPush

    return SheetsPush(spreadsheet)


def _make_claude_fn():
    """Create a Claude API callback for receipt/categorization parsing.

    Returns a callable (system: str, prompt: str) -> str, or None if
    ANTHROPIC_API_KEY is not set.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)

        def claude_fn(system: str, prompt: str) -> str:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text

        return claude_fn
    except Exception as e:
        logging.getLogger(__name__).warning("Claude API not available: %s", e)
        return None


def _get_receipt_lookup(config, repo):
    """Create a ReceiptLookup if Gmail credentials are configured.

    Requires FINANCE_CREDENTIALS and FINANCE_GMAIL_USER. The Claude callback
    is optional — without it, receipt email text won't be parsed but the
    feature degrades gracefully.

    Returns None if not configured (receipt lookup will be skipped).
    """
    credentials_path = os.environ.get("FINANCE_CREDENTIALS")
    gmail_user = os.environ.get("FINANCE_GMAIL_USER")

    if not credentials_path or not gmail_user:
        return None

    try:
        from src.gmail.client import GmailClient
        from src.categorize.receipt_lookup import ReceiptLookup

        gmail = GmailClient(credentials_path, gmail_user)
        claude_fn = _make_claude_fn()
        return ReceiptLookup(gmail, repo, claude_fn=claude_fn, config=config)
    except Exception as e:
        logging.getLogger(__name__).warning("Receipt lookup not available: %s", e)
        return None


def _get_watch_dir() -> Path:
    """Get the watch directory from env or default."""
    return Path(os.environ.get("FINANCE_WATCH_DIR", "import"))


def _get_migrations_dir() -> Path:
    """Get the migrations directory path."""
    return Path(os.environ.get(
        "FINANCE_MIGRATIONS_DIR", "src/database/migrations",
    ))


# ── Command handlers ─────────────────────────────────────


def cmd_import(args: argparse.Namespace) -> int:
    """Import bank file(s) via the ImportPipeline."""
    from src.watcher.observer import ImportPipeline, SUPPORTED_EXTENSIONS

    config = _get_config()
    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())
    dedup = _get_dedup(repo)
    sheets = _get_sheets()
    receipt_lookup = _get_receipt_lookup(config, repo)
    claude_fn = _make_claude_fn()

    pipeline = ImportPipeline(
        repo=repo, config=config, dedup=dedup, sheets=sheets,
        receipt_lookup=receipt_lookup, claude_fn=claude_fn,
    )

    try:
        if args.file:
            # Single file import
            filepath = args.file.resolve()
            if not filepath.exists():
                print(f"Error: File not found: {filepath}")
                return 1
            if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
                print(f"Error: Unsupported file type: {filepath.suffix}")
                return 1

            result = pipeline.process_file(filepath)
            print(
                f"{result.file_name}: {result.status}"
                f" (new={result.new_count}, dup={result.duplicate_count},"
                f" flagged={result.flagged_count})"
            )
            return 0 if result.status != "error" else 1

        # Batch: process all supported files in watch dir
        watch_dir = _get_watch_dir()
        if not watch_dir.exists():
            print(f"Watch directory not found: {watch_dir}")
            return 1

        files = [
            f for f in sorted(watch_dir.iterdir())
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
        ]
        if not files:
            print("No pending files found.")
            return 0

        total_new = 0
        total_dup = 0
        errors = 0
        for filepath in files:
            result = pipeline.process_file(filepath)
            print(
                f"  {result.file_name}: {result.status}"
                f" (new={result.new_count}, dup={result.duplicate_count})"
            )
            total_new += result.new_count
            total_dup += result.duplicate_count
            if result.status == "error":
                errors += 1

        print(f"\nProcessed {len(files)} files: {total_new} new, {total_dup} duplicates, {errors} errors")
        return 1 if errors else 0
    finally:
        repo.close()


def cmd_watch(args: argparse.Namespace) -> int:
    """Start the file watcher daemon."""
    from src.watcher.observer import FileWatcher, ImportPipeline

    config = _get_config()
    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())
    dedup = _get_dedup(repo)
    sheets = _get_sheets()
    receipt_lookup = _get_receipt_lookup(config, repo)
    claude_fn = _make_claude_fn()

    pipeline = ImportPipeline(
        repo=repo, config=config, dedup=dedup, sheets=sheets,
        receipt_lookup=receipt_lookup, claude_fn=claude_fn,
    )

    watcher = FileWatcher(
        watch_dir=_get_watch_dir(),
        pipeline=pipeline,
    )

    print(f"Watching {watcher.watch_dir} for bank files... (Ctrl+C to stop)")
    watcher.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping watcher...")
    finally:
        watcher.stop()
        repo.close()

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Display system status counts."""
    from src.database.queries import get_status_counts

    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())
    counts = get_status_counts(repo.conn)

    print("MoMoney Status")
    print("=" * 40)
    print(f"  Total transactions:  {counts['total_txns']:,}")
    print(f"  Categorized:         {counts['categorized']:,}")
    print(f"  Pending:             {counts['pending']:,}")
    print(f"  Flagged for review:  {counts['flagged']:,}")
    print(f"  Transfers:           {counts['total_transfers']:,}")
    print(f"  Total imports:       {counts['total_imports']:,}")

    # Show API costs for current month
    from datetime import datetime
    month = datetime.now().strftime("%Y-%m")
    cost_cents = repo.get_monthly_cost(month)
    print(f"\n  API cost ({month}):     ${cost_cents / 100:.2f}")

    repo.close()
    return 0


def cmd_review(args: argparse.Namespace) -> int:
    """List transactions flagged for manual review."""
    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())

    rows = repo.conn.execute(
        "SELECT id, date, amount, account_id, raw_description,"
        " categorization_method, confidence"
        " FROM transactions"
        " WHERE status = 'flagged'"
        " ORDER BY date DESC"
        " LIMIT 50"
    ).fetchall()

    if not rows:
        print("No transactions pending review.")
        return 0

    print(f"Transactions pending review ({len(rows)}):")
    print("-" * 80)
    for r in rows:
        conf = f"{r['confidence']:.0%}" if r["confidence"] is not None else "n/a"
        print(
            f"  {r['date']}  {r['amount']:>10.2f}  {r['account_id']:<18}"
            f"  {r['raw_description'][:30]:<30}  {conf}"
        )

    repo.close()
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    """Run balance reconciliation for one or all accounts."""
    from src.database.queries import get_reconciliation_data

    config = _get_config()
    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())

    if args.account:
        accounts = [args.account]
    else:
        accounts = [a["id"] for a in config.accounts]

    has_discrepancy = False
    for account_id in accounts:
        result = get_reconciliation_data(repo.conn, account_id)

        if result["status"] == "no_balance_data":
            print(f"  {account_id:<20}  No balance data")
            continue

        status = result["status"]
        if status == "discrepancy":
            has_discrepancy = True

        marker = "OK" if status == "balanced" else "MISMATCH"
        print(
            f"  {account_id:<20}  [{marker}]"
            f"  statement={result['statement_balance']:.2f}"
            f"  computed={result['computed_balance']:.2f}"
            f"  diff={result['difference']:.2f}"
            f"  as of {result['date']}"
        )

    repo.close()
    return 1 if has_discrepancy else 0


def cmd_push(args: argparse.Namespace) -> int:
    """Force a full Google Sheets rebuild."""
    sheets = _get_sheets()
    if sheets is None:
        print("Error: Sheets not configured. Set FINANCE_SPREADSHEET_ID and FINANCE_CREDENTIALS.")
        return 1

    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())
    config = _get_config()

    print("Rebuilding all sheets from SQLite...")
    results = sheets.full_rebuild(repo, config)
    total_rows = sum(r.rows_pushed for r in results)
    print(f"Done. Pushed {total_rows} rows across {len(results)} batches.")

    repo.close()
    return 0


def cmd_poll(args: argparse.Namespace) -> int:
    """Poll Google Sheets for override edits and apply to SQLite."""
    spreadsheet = _get_spreadsheet()
    if spreadsheet is None:
        print("Error: Sheets not configured. Set FINANCE_SPREADSHEET_ID and FINANCE_CREDENTIALS.")
        return 1

    from src.sheets.overrides import OverridePoller

    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())
    config = _get_config()

    poller = OverridePoller(spreadsheet, repo, config=config)

    print("Polling Sheets for overrides...")
    result = poller.poll()

    if result.overrides_applied == 0 and result.errors == 0:
        print("No overrides found.")
    else:
        print(f"Applied {result.overrides_applied} override(s), {result.errors} error(s).")
        for action in result.actions:
            print(f"  {action.sheet} row {action.row_index}: {action.column} = {action.new_value} ({action.entity_id})")

    repo.close()
    return 1 if result.errors else 0


def cmd_import_budget_app(args: argparse.Namespace) -> int:
    """One-time historical import from budget app Register CSV."""
    import time as _time
    from src.database.dedup import DedupEngine
    from src.database.models import Allocation
    from src.parsers.budget_app import BudgetAppCsvParser
    from src.categorize.pipeline import categorize_transaction, apply_categorization
    from src.categorize.transfer_detect import detect_transfer

    filepath = args.file.resolve()
    if not filepath.exists():
        print(f"Error: File not found: {filepath}")
        return 1

    start_time = _time.time()
    config = _get_config()
    repo = _get_repo()
    repo.apply_migrations(_get_migrations_dir())
    dedup = DedupEngine(repo)
    claude_fn = _make_claude_fn()

    try:
        # Parse budget app CSV
        parser = BudgetAppCsvParser(
            category_map=config.budget_app_category_map,
            account_routing=config.budget_app_account_routing,
        )
        print(f"Parsing {filepath.name}...")
        raw_txns = parser.parse(filepath)
        print(f"  Parsed {len(raw_txns)} transactions")

        if not raw_txns:
            print("No transactions found.")
            return 0

        # Build a lookup from (account, date, amount, desc) to budget_app_category_id
        # so we can apply pre-mapped categories after dedup
        budget_app_categories: dict[tuple, str] = {}
        for raw in raw_txns:
            if raw.budget_app_category_id:
                key = (raw.account_id, raw.date, raw.amount, raw.raw_description)
                budget_app_categories[key] = raw.budget_app_category_id

        # Process through dedup engine
        from src.parsers.base import compute_file_hash
        from src.database.models import Import

        file_hash = compute_file_hash(filepath)
        if dedup.check_file_duplicate(filepath):
            print("This file has already been imported (duplicate file hash).")
            return 0

        imp = Import(
            file_name=filepath.name,
            file_hash=file_hash,
            file_size=filepath.stat().st_size,
        )
        repo.insert_import(imp)

        try:
            batch_result = dedup.process_batch(raw_txns, imp.id, source="budget_app")
            print(
                f"  Dedup: {batch_result.new_count} new,"
                f" {batch_result.duplicate_count} duplicates,"
                f" {batch_result.flagged_count} flagged"
            )

            # Categorize new transactions
            # Use budget app's pre-mapped category when available, otherwise run pipeline
            categorized_budget_app = 0
            categorized_pipeline = 0
            categorize_errors = 0
            # Count both pending and flagged since we process both
            total_to_process = sum(
                1 for t in batch_result.transactions if t.status in ("pending", "flagged")
            )
            processed = 0

            print(f"  Categorizing {total_to_process} transactions...")

            for txn in batch_result.transactions:
                # Process both pending and flagged transactions
                # Flagged = fuzzy dedup match, but budget app import should still apply categories
                if txn.status not in ("pending", "flagged"):
                    continue

                # Check if we have a pre-mapped budget app category for this transaction
                key = (txn.account_id, txn.date, txn.amount, txn.raw_description)
                budget_app_cat = budget_app_categories.get(key)

                if budget_app_cat:
                    # Use the budget app category directly - no AI needed
                    try:
                        # Keep flagged status if it was flagged by dedup (needs review)
                        new_status = "flagged" if txn.status == "flagged" else "categorized"
                        allocation = Allocation(
                            transaction_id=txn.id,
                            category_id=budget_app_cat,
                            amount=txn.amount,
                            source="budget_app",
                            confidence=1.0,
                        )
                        # Insert allocation first - if it fails, status stays pending
                        repo.insert_allocation(allocation)
                        # Only update status after allocation succeeds
                        repo.update_transaction_status(
                            txn.id,
                            status=new_status,
                            confidence=1.0,
                            method="budget_app_import",
                        )
                        categorized_budget_app += 1

                        # Also check for transfers
                        # We still need to link transfer pairs even when using budget app categories
                        is_transfer = txn.txn_type == "TRANSFER"
                        transfer_match = detect_transfer(txn.raw_description or "", txn.account_id, config)
                        if is_transfer or transfer_match:
                            _try_link_budget_app_transfer(txn, transfer_match, repo)
                    except Exception as e:
                        logger.warning("Failed to categorize txn %s: %s", txn.id, e)
                        categorize_errors += 1
                else:
                    # No budget app category - run full pipeline (but skip Claude AI for bulk import)
                    try:
                        result = categorize_transaction(
                            txn, config, claude_fn=None, repo=repo,  # No Claude for bulk import
                        )
                        apply_categorization(txn, result, repo)
                        categorized_pipeline += 1
                    except Exception as e:
                        logger.warning("Failed to categorize txn %s: %s", txn.id, e)
                        categorize_errors += 1

                # Progress logging every 1000 transactions
                processed += 1
                if processed % 1000 == 0 and total_to_process > 0:
                    print(f"    Progress: {processed}/{total_to_process} ({100*processed//total_to_process}%)")

            cat_summary = f"  Categorized: {categorized_budget_app} from budget app, {categorized_pipeline} via pipeline"
            if categorize_errors > 0:
                cat_summary += f", {categorize_errors} errors"
            print(cat_summary)

            # Second pass: link any unlinked transfers now that all transactions exist
            # The first pass may have missed counterparts that weren't inserted yet
            print("  Linking transfer pairs...")
            transfer_links = _backfill_transfer_links(repo, imp.id)
            print(f"  Linked {transfer_links} transfer pairs")

            # Push to Sheets if configured
            # Re-fetch transactions from DB so status/confidence/method are current
            sheets = _get_sheets()
            if sheets is not None:
                try:
                    fresh_txns = repo.get_transactions_by_import_id(imp.id)
                    sheets.push_transactions(fresh_txns, repo)
                    txn_ids = [t.id for t in fresh_txns]
                    alloc_map = repo.get_allocations_by_transaction_ids(txn_ids)
                    allocs = [a for al in alloc_map.values() for a in al]
                    if allocs:
                        sheets.push_allocations(allocs)
                    sheets.push_review(fresh_txns, repo)
                    # Push transfers
                    from src.database.models import Transfer
                    xfer_rows = repo.conn.execute(
                        "SELECT * FROM transfers WHERE from_transaction_id IN"
                        " (SELECT id FROM transactions WHERE import_id = ?)"
                        " OR to_transaction_id IN"
                        " (SELECT id FROM transactions WHERE import_id = ?)",
                        (imp.id, imp.id),
                    ).fetchall()
                    if xfer_rows:
                        xfers = [repo._row_to_transfer(r) for r in xfer_rows]
                        sheets.push_transfers(xfers)
                    print("  Pushed to Sheets")
                except Exception as e:
                    print(f"  Warning: Sheets push failed: {e}")

            # Mark as "partial" if there were categorization errors, otherwise "completed"
            final_status = "partial" if categorize_errors > 0 else "completed"
            repo.update_import_status(
                imp.id, final_status, record_count=len(raw_txns),
            )
            elapsed = _time.time() - start_time
            status_msg = "with errors" if categorize_errors > 0 else ""
            print(f"\nBudget app import complete {status_msg}: {batch_result.new_count} new transactions in {elapsed:.1f}s.")

        except Exception as e:
            # Update import status to error so it's not stuck in "pending"
            repo.update_import_status(
                imp.id, "error", error_message=str(e)[:500],
            )
            logger.exception("Budget app import failed for %s", filepath.name)
            print(f"\nBudget app import failed: {e}")
            return 1

        return 0
    finally:
        repo.close()


def _try_link_budget_app_transfer(txn, transfer_match, repo) -> None:
    """Attempt to link a budget app transfer transaction to its counterpart.

    Similar to pipeline._try_link_transfer but simplified for budget app import.
    """
    from src.database.models import Transfer

    # Check if this transaction is already part of a transfer
    existing = repo.get_transfer_by_transaction(txn.id)
    if existing is not None:
        return  # Already linked

    # Find candidates for the counterpart
    candidates = repo.find_transfer_candidates(txn)
    if not candidates:
        return

    # Use the best match (closest date)
    counterpart = candidates[0]

    # Determine from/to based on pattern info or amount sign
    if transfer_match and transfer_match.from_account and transfer_match.to_account:
        if txn.account_id == transfer_match.from_account:
            from_txn_id = txn.id
            to_txn_id = counterpart.id
        elif txn.account_id == transfer_match.to_account:
            from_txn_id = counterpart.id
            to_txn_id = txn.id
        else:
            # Fall back to amount sign
            if txn.amount < 0:
                from_txn_id = txn.id
                to_txn_id = counterpart.id
            else:
                from_txn_id = counterpart.id
                to_txn_id = txn.id
    else:
        # No pattern info - use amount sign
        if txn.amount < 0:
            from_txn_id = txn.id
            to_txn_id = counterpart.id
        else:
            from_txn_id = counterpart.id
            to_txn_id = txn.id

    transfer_type = transfer_match.transfer_type if transfer_match else "internal-transfer"
    transfer = Transfer(
        from_transaction_id=from_txn_id,
        to_transaction_id=to_txn_id,
        transfer_type=transfer_type,
        match_method="budget_app_import_auto_link",
        confidence=0.9,
    )
    repo.insert_transfer(transfer)


def _backfill_transfer_links(repo, import_id: str) -> int:
    """Link unlinked transfer transactions from a budget app import.

    This second pass catches transfer pairs where the counterpart wasn't
    in the database yet during the first pass (due to CSV ordering).
    """
    from src.database.models import Transfer

    # Find unlinked transfers from this import
    rows = repo.conn.execute(
        """
        SELECT id, account_id, date, amount, raw_description
        FROM transactions
        WHERE import_id = ?
        AND raw_description LIKE 'Transfer%'
        AND id NOT IN (SELECT from_transaction_id FROM transfers)
        AND id NOT IN (SELECT to_transaction_id FROM transfers)
        """,
        (import_id,),
    ).fetchall()

    linked = 0
    for txn_id, acct, date, amount, desc in rows:
        # Find counterpart: different account, opposite amount, within 5 days, unlinked
        match = repo.conn.execute(
            """
            SELECT id FROM transactions
            WHERE account_id != ?
            AND ABS(amount + ?) < 0.01
            AND ABS(JULIANDAY(date) - JULIANDAY(?)) <= 5
            AND raw_description LIKE 'Transfer%'
            AND id NOT IN (SELECT from_transaction_id FROM transfers)
            AND id NOT IN (SELECT to_transaction_id FROM transfers)
            ORDER BY ABS(JULIANDAY(date) - JULIANDAY(?))
            LIMIT 1
            """,
            (acct, amount, date, date),
        ).fetchone()

        if match:
            from_id, to_id = (txn_id, match[0]) if amount < 0 else (match[0], txn_id)
            transfer = Transfer(
                from_transaction_id=from_id,
                to_transaction_id=to_id,
                transfer_type="internal-transfer",
                match_method="budget_app_import_backfill",
                confidence=0.9,
            )
            repo.insert_transfer(transfer)
            linked += 1

    return linked


# ── Category management ──────────────────────────────────


def _find_node(tree: list[dict], node_id: str) -> dict | None:
    """Recursively find a node by id in the category tree."""
    for node in tree:
        if node.get("id") == node_id:
            return node
        children = node.get("children", [])
        if children:
            found = _find_node(children, node_id)
            if found is not None:
                return found
    return None


def _find_parent(tree: list[dict], node_id: str) -> tuple[list[dict], dict | None]:
    """Find the parent children-list and parent node containing node_id.

    Returns (children_list, parent_node) where children_list is the list
    that contains the node, and parent_node is None if at root level.
    """
    for node in tree:
        for child in node.get("children", []):
            if child.get("id") == node_id:
                return node["children"], node
        children = node.get("children", [])
        if children:
            result_list, result_parent = _find_parent(children, node_id)
            if result_list is not None:
                return result_list, result_parent
    # Check root level
    for node in tree:
        if node.get("id") == node_id:
            return tree, None
    return None, None


def _save_categories(config_dir: Path, tree: list[dict]) -> None:
    """Write the category tree back to categories.yaml."""
    import yaml

    path = config_dir / "categories.yaml"
    with open(path, "w") as f:
        f.write("# Category hierarchy for MoMoney\n")
        f.write("# Auto-generated by `momoney category` commands\n\n")
        yaml.dump({"tree": tree}, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _print_tree(nodes: list[dict], prefix: str = "", is_last_list: list[bool] | None = None) -> None:
    """Print a tree of categories with box-drawing characters."""
    if is_last_list is None:
        is_last_list = []

    for i, node in enumerate(nodes):
        is_last = (i == len(nodes) - 1)
        connector = "\u2514\u2500\u2500 " if is_last else "\u251c\u2500\u2500 "

        # Build prefix from ancestor connectors
        line_prefix = ""
        for ancestor_is_last in is_last_list:
            line_prefix += "    " if ancestor_is_last else "\u2502   "

        name = node.get("name", node.get("id", "?"))
        cat_id = node.get("id", "")
        children = node.get("children", [])
        leaf_marker = "" if children else f"  [{cat_id}]"
        print(f"{line_prefix}{connector}{name}{leaf_marker}")

        if children:
            _print_tree(children, prefix, is_last_list + [is_last])


def cmd_category(args: argparse.Namespace) -> int:
    """Category management commands."""
    sub = args.category_command
    if sub is None:
        print("Usage: momoney category {list,add,rename,move}")
        return 1

    config = _get_config()
    tree = config.categories

    if sub == "list":
        return _cmd_category_list(tree)
    elif sub == "add":
        return _cmd_category_add(config, tree, args)
    elif sub == "rename":
        return _cmd_category_rename(config, tree, args)
    elif sub == "move":
        return _cmd_category_move(config, tree, args)
    return 1


def _cmd_category_list(tree: list[dict]) -> int:
    """Print the category hierarchy."""
    _print_tree(tree)
    return 0


def _cmd_category_add(config, tree: list[dict], args: argparse.Namespace) -> int:
    """Add a new category under a parent."""
    parent_id = args.parent_id
    new_id = args.new_id
    new_name = args.name

    # Validate parent exists
    parent = _find_node(tree, parent_id)
    if parent is None:
        print(f"Error: Parent category '{parent_id}' not found.")
        return 1

    # Validate new_id doesn't already exist
    if _find_node(tree, new_id) is not None:
        print(f"Error: Category '{new_id}' already exists.")
        return 1

    # Add child
    if "children" not in parent:
        parent["children"] = []
        parent["is_leaf"] = False
    parent["children"].append({"id": new_id, "name": new_name})

    config_dir = Path(os.environ.get("FINANCE_CONFIG_DIR", "config"))
    _save_categories(config_dir, tree)
    print(f"Added '{new_name}' ({new_id}) under '{parent_id}'.")
    return 0


def _cmd_category_rename(config, tree: list[dict], args: argparse.Namespace) -> int:
    """Rename an existing category."""
    node = _find_node(tree, args.id)
    if node is None:
        print(f"Error: Category '{args.id}' not found.")
        return 1

    old_name = node.get("name", "")
    node["name"] = args.name

    config_dir = Path(os.environ.get("FINANCE_CONFIG_DIR", "config"))
    _save_categories(config_dir, tree)
    print(f"Renamed '{args.id}': '{old_name}' -> '{args.name}'.")
    return 0


def _cmd_category_move(config, tree: list[dict], args: argparse.Namespace) -> int:
    """Move a category to a new parent."""
    node_id = args.id
    new_parent_id = args.new_parent_id

    # Find the node
    node = _find_node(tree, node_id)
    if node is None:
        print(f"Error: Category '{node_id}' not found.")
        return 1

    # Find new parent
    new_parent = _find_node(tree, new_parent_id)
    if new_parent is None:
        print(f"Error: Target parent '{new_parent_id}' not found.")
        return 1

    # Prevent moving into self or descendant
    if _find_node(node.get("children", []), new_parent_id) is not None:
        print(f"Error: Cannot move '{node_id}' into its own descendant '{new_parent_id}'.")
        return 1

    # Remove from current location
    old_siblings, _ = _find_parent(tree, node_id)
    if old_siblings is None:
        print(f"Error: Could not locate '{node_id}' in tree.")
        return 1
    old_siblings[:] = [n for n in old_siblings if n.get("id") != node_id]

    # Add to new parent
    if "children" not in new_parent:
        new_parent["children"] = []
        new_parent["is_leaf"] = False
    new_parent["children"].append(node)

    config_dir = Path(os.environ.get("FINANCE_CONFIG_DIR", "config"))
    _save_categories(config_dir, tree)
    print(f"Moved '{node_id}' to '{new_parent_id}'.")
    return 0


# ── Main entry point ─────────────────────────────────────


_COMMANDS = {
    "import": cmd_import,
    "watch": cmd_watch,
    "status": cmd_status,
    "review": cmd_review,
    "reconcile": cmd_reconcile,
    "push": cmd_push,
    "poll": cmd_poll,
    "import-budget-app": cmd_import_budget_app,
    "category": cmd_category,
}


def main(argv: list[str] | None = None):
    _setup_logging()

    parser = argparse.ArgumentParser(
        prog="momoney",
        description="MoMoney personal finance tracker",
    )
    subparsers = parser.add_subparsers(dest="command")

    # import
    import_p = subparsers.add_parser("import", help="Import bank file(s)")
    import_p.add_argument("--file", type=Path, help="Specific file to import")

    # watch
    subparsers.add_parser("watch", help="Start file watcher daemon")

    # status
    subparsers.add_parser("status", help="Show pending imports, review items, last sync")

    # review
    subparsers.add_parser("review", help="List items needing manual review")

    # reconcile
    recon_p = subparsers.add_parser("reconcile", help="Run balance reconciliation")
    recon_p.add_argument("account", nargs="?", help="Account ID to reconcile")

    # push
    subparsers.add_parser("push", help="Force full Sheets rebuild")

    # poll
    subparsers.add_parser("poll", help="Poll Sheets for override edits and apply to SQLite")

    # import-budget-app
    budget_app_p = subparsers.add_parser("import-budget-app", help="One-time budget app historical import")
    budget_app_p.add_argument("file", type=Path, help="Budget app Register CSV file")

    # category
    cat_p = subparsers.add_parser("category", help="Manage category hierarchy")
    cat_sub = cat_p.add_subparsers(dest="category_command")
    cat_sub.add_parser("list", help="Print category tree")
    cat_add_p = cat_sub.add_parser("add", help="Add a new category")
    cat_add_p.add_argument("parent_id", help="Parent category ID")
    cat_add_p.add_argument("new_id", help="New category ID (slug)")
    cat_add_p.add_argument("name", help="Display name for the new category")
    cat_rename_p = cat_sub.add_parser("rename", help="Rename a category")
    cat_rename_p.add_argument("id", help="Category ID to rename")
    cat_rename_p.add_argument("name", help="New display name")
    cat_move_p = cat_sub.add_parser("move", help="Move a category to a new parent")
    cat_move_p.add_argument("id", help="Category ID to move")
    cat_move_p.add_argument("new_parent_id", help="New parent category ID")

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    handler = _COMMANDS.get(args.command)
    if handler is None:
        print(f"Unknown command: {args.command}")
        sys.exit(1)

    sys.exit(handler(args))


if __name__ == "__main__":
    main()
