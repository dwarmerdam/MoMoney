# Quick Start

Typical lifecycle for importing, reviewing, and managing transactions.

All commands below assume Docker on the NAS. Replace `sudo docker compose exec momoney` with `momoney` if running locally.

## 1. Import Transactions

**Drop a bank file** (`.qfx`, `.ofx`, or `.csv`) into the import directory. The watcher daemon picks it up automatically:

```bash
# Check watcher is running
sudo docker compose logs --tail 20 momoney

# Or import a file manually
sudo docker compose exec momoney momoney import --file /app/import/myfile.qfx
```

**One-time budget app historical import** (first-time setup only):

```bash
sudo docker compose exec momoney momoney import-budget-app /app/import/budget-register.csv
```

## 2. Check Status

```bash
sudo docker compose exec momoney momoney status
```

Shows total transactions, categorized, pending, flagged, transfers, and API costs.

## 3. Review Flagged Transactions

```bash
sudo docker compose exec momoney momoney review
```

Lists transactions the pipeline couldn't confidently categorize (flagged for manual review).

## 4. Push to Google Sheets

```bash
sudo docker compose exec momoney momoney push
```

Full rebuild of all six sheet tabs (Transactions, Allocations, Transfers, Summary, Review, Categories) from SQLite. The watcher also pushes incrementally after each import. Override: Category columns get dropdown validation referencing the Categories sheet.

## 5. Poll Overrides from Sheets

After entering overrides in Google Sheets, run `momoney poll` to apply them to SQLite:

```bash
sudo docker compose exec momoney momoney poll
```

The poller reads override columns, applies changes to the database, and clears the cells. Invalid category IDs are auto-corrected via fuzzy matching when possible.

## 6. Override / Correct in Google Sheets

Corrections are made by typing values into the override columns. Override: Category columns show a dropdown of valid categories (referencing the Categories sheet). Run `momoney poll` to apply overrides to the database.

### Transactions Tab (columns P-U)

All overrides are available from this tab, where you can see the full transaction context.

| Column | Override | What to type |
|--------|----------|--------------|
| P | Category | A category ID (e.g. `car-insurance`) |
| Q | Merchant | Corrected merchant name |
| R | Reviewed | `TRUE` to mark as reviewed |
| S | Transfer Link | Transaction ID of the counterpart |
| T | Needs Split | `TRUE` to flag for splitting |
| U | Notes | Free-text note (appended to memo) |

### Allocations Tab (columns I-J)

| Column | Override | What to type |
|--------|----------|--------------|
| I | Category | A category ID |
| J | Merchant | Corrected merchant name |

### Review Tab (columns V-X)

Only shows flagged/pending transactions.

| Column | Override | What to type |
|--------|----------|--------------|
| V | Category | A category ID |
| W | Merchant | Corrected merchant name |
| X | Reviewed | `TRUE` to mark as reviewed |

Category overrides set `source='user'` on the allocation, which gives them 1.5x weight in historical pattern matching for future imports.

## 7. Reconcile Balances

```bash
# All accounts
sudo docker compose exec momoney momoney reconcile

# Specific account
sudo docker compose exec momoney momoney reconcile wf-checking
```

Compares the last statement balance from imported files against the computed running balance.

## 8. Manage Categories

```bash
# List the full category tree
sudo docker compose exec momoney momoney category list

# Add a new category under a parent
sudo docker compose exec momoney momoney category add wants-extras-shared coffee "Coffee"

# Rename a category
sudo docker compose exec momoney momoney category rename coffee "Coffee Shops"

# Move a category to a different parent
sudo docker compose exec momoney momoney category move coffee monthly-shared
```

## 9. Re-process Flagged Transactions

After updating config (e.g. adding transfer aliases or merchant patterns), re-run flagged transactions through the pipeline:

```bash
# Dry run first
sudo docker compose exec momoney python -m scripts.reprocess_flagged --dry-run

# Apply
sudo docker compose exec momoney python -m scripts.reprocess_flagged
```

## Categorization Pipeline

Transactions flow through these steps in order. The first match wins:

1. **Transfer detection** -- known transfer patterns and `Transfer :` prefix
2. **Merchant auto-match** -- 100% confidence merchants from `merchants.yaml`
3. **Historical pattern** -- past categorizations for the same merchant/amount
4. **Amount rules** -- disambiguate by transaction amount (e.g. CSAA, Apple.com)
5. **Account rules** -- default categories for specific accounts
6. **Merchant high-confidence** -- 80-99% confidence merchants
7. **Gmail receipt lookup** -- Apple/Amazon receipt matching via Gmail API
8. **Claude AI** -- single-transaction AI categorization
9. **Manual review** -- flagged for human review in Sheets
