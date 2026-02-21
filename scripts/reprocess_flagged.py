#!/usr/bin/env python3
"""One-time script: re-process flagged transactions through the categorization pipeline.

Usage:
    python -m scripts.reprocess_flagged [--dry-run]

Flagged transactions are those that fell through all pipeline steps on initial import
(e.g. because a transfer alias was missing). This script re-runs them through the
full pipeline and applies the results.
"""

import argparse
import os
import sys
from pathlib import Path

from src.config import Config
from src.database.repository import Repository
from src.categorize.pipeline import categorize_transaction, apply_categorization


def main():
    parser = argparse.ArgumentParser(description="Re-process flagged transactions")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    args = parser.parse_args()

    config = Config()
    db_path = os.environ.get("FINANCE_DB_PATH", "finance.db")
    repo = Repository(db_path=db_path)
    migrations_dir = Path(os.environ.get("FINANCE_MIGRATIONS_DIR", "src/database/migrations"))
    repo.apply_migrations(migrations_dir)

    # Fetch all flagged transactions
    rows = repo.conn.execute(
        "SELECT * FROM transactions WHERE status = 'flagged' ORDER BY date"
    ).fetchall()
    flagged = [repo._row_to_transaction(r) for r in rows]

    if not flagged:
        print("No flagged transactions found.")
        return

    print(f"Found {len(flagged)} flagged transactions")

    recategorized = 0
    still_flagged = 0
    methods = {}

    for txn in flagged:
        result = categorize_transaction(txn, config, repo=repo)

        if args.dry_run:
            status = "categorized" if result.method != "manual_review" else "flagged"
            print(f"  [{status:>12}] {txn.date} {txn.amount:>10.2f}  {txn.raw_description[:50]}")
            print(f"               -> {result.method}: {result.category_id}")
            methods[result.method] = methods.get(result.method, 0) + 1
            if status == "categorized":
                recategorized += 1
            else:
                still_flagged += 1
            continue

        # Delete existing allocation (manual_review placeholder)
        repo.conn.execute(
            "DELETE FROM allocations WHERE transaction_id = ?", (txn.id,)
        )
        repo.conn.commit()

        # Apply new categorization (creates allocation, updates status, links transfers)
        apply_categorization(txn, result, repo)
        methods[result.method] = methods.get(result.method, 0) + 1

        if result.method != "manual_review":
            recategorized += 1
        else:
            still_flagged += 1

    print(f"\nResults:")
    print(f"  Re-categorized: {recategorized}")
    print(f"  Still flagged:  {still_flagged}")
    print(f"  By method:")
    for method, count in sorted(methods.items(), key=lambda x: -x[1]):
        print(f"    {method:25s} {count}")

    if args.dry_run:
        print("\n(dry run — no changes written)")


if __name__ == "__main__":
    main()
