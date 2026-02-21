# CLAUDE.md

Template repository for a self-hosted personal finance tracker. Users create
their own private copy via GitHub's "Use this template" feature, then customize
the YAML config files for their own bank accounts and spending categories.

Imports bank transactions from QFX/OFX/CSV files, categorizes them through an
8-step pipeline, stores in SQLite, and pushes to Google Sheets as a review UI
with bidirectional overrides.

## Tech Stack

- Python 3.11+ (Docker image uses 3.12)
- SQLite (WAL mode) via raw SQL — no ORM
- Docker + Docker Compose for deployment (Synology NAS target)
- Google Sheets API (gspread), Gmail API, Anthropic Claude API

## Common Commands

```bash
# Install locally for development
pip install -e ".[dev]"

# Run all tests (820 tests)
pytest

# Run a single test file
pytest tests/test_categorize/test_pipeline.py

# Run tests matching a keyword
pytest -k "test_transfer"

# Build and run Docker container
docker compose up -d --build

# Run CLI commands inside Docker
docker compose exec momoney momoney status
docker compose exec momoney momoney import --file /app/import/file.qfx
docker compose exec momoney momoney review
docker compose exec momoney momoney push
docker compose exec momoney momoney poll
docker compose exec momoney momoney category list
```

## Project Structure

- `src/cli.py` — CLI entry point, 10 commands: watch, import, import-budget-app, status, review, reconcile, push, poll, category
- `src/config.py` — Loads all YAML config from `config/` directory
- `src/categorize/pipeline.py` — 8-step categorization orchestrator (the core business logic)
- `src/categorize/transfer_detect.py` — Step 1: transfer detection (pattern-based + txn_type fallback)
- `src/categorize/merchant_match.py` — Steps 2 and 5: merchant matching (auto and high tiers)
- `src/categorize/amount_rules.py` — Step 3: amount-based disambiguation
- `src/categorize/receipt_lookup.py` — Step 6: Gmail receipt parsing with subset-sum matching
- `src/categorize/claude_ai.py` — Step 7: Claude AI categorization
- `src/database/repository.py` — SQLite repository with migrations, CRUD, dedup
- `src/database/models.py` — Dataclass models (Transaction, Allocation, Import, etc.)
- `src/database/dedup.py` — 5-tier deduplication engine
- `src/database/migrations/` — SQL migration files (applied automatically on startup)
- `src/parsers/` — Bank file parsers: QFX SGML, QFX XML, CSV (Mercury), budget app CSV
- `src/gmail/` — Gmail API client for Apple/Amazon receipt lookup
- `src/sheets/` — Google Sheets push and override polling
- `src/watcher/` — File watcher daemon (PollingObserver, 30s interval)
- `config/examples/` — Template YAML configs with placeholder values
- `tests/` — 32 test files mirroring src/ structure

## Architecture Decisions

- **No ORM.** All SQL is in repository.py and migration files. Keep it that way.
- **Categorization pipeline is ordered.** Steps 1-8 are priority-ranked — earlier steps always win. Do not reorder without understanding the full fallback chain.
- **Config files are gitignored.** The 6 YAML files in `config/` contain personal financial data (account numbers, merchant patterns). Only `config/examples/` is tracked.
- **Google Sheets is optional.** All features degrade gracefully when Sheets/Gmail/Claude credentials are missing. The app always works offline with just SQLite.
- **Dedup is 5-tier.** File hash → external ID (FITID) → import hash → cross-format (bank vs budget app) → fuzzy dedup key. All five layers must be preserved for re-import safety.

## Important Conventions

- All database schema changes go in numbered SQL migration files under `src/database/migrations/` (e.g., `002_add_column.sql`). Migrations run automatically on startup.
- Transaction amounts are stored as floats. Debits are negative, credits are positive.
- The `account_id` field throughout the codebase refers to the internal string ID (e.g., `"wf-checking"`), not a bank account number. Bank account numbers (`qfx_acctid`) exist only in config and are never persisted to the database.
- Test files use synthetic data only — no real account numbers, merchant names, or transaction amounts.
- The `momoney` entry point is defined in `pyproject.toml` → `src.cli:main`.

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `FINANCE_DB_PATH` | No (default: `finance.db`) | SQLite database path |
| `FINANCE_WATCH_DIR` | No (default: `import`) | Bank file drop folder |
| `FINANCE_CREDENTIALS` | For Sheets/Gmail | Path to `service-account.json` |
| `FINANCE_GMAIL_USER` | For Gmail | Google Workspace email to impersonate |
| `FINANCE_SPREADSHEET_ID` | For Sheets | Google Sheets spreadsheet ID |
| `ANTHROPIC_API_KEY` | For Claude AI | Anthropic API key |
| `FINANCE_CONFIG_DIR` | No (default: `config`) | YAML config directory |
| `FINANCE_MIGRATIONS_DIR` | No (default: `src/database/migrations`) | SQL migrations directory |
| `FINANCE_LOG_LEVEL` | No (default: `INFO`) | Logging level |

## Template Repository

This is a **template repo**, not a collaborative project. There are no
external contributors, no PRs, and no issue tracker. Users create their own
private copy and customize it independently.

## Security Constraints

- NEVER commit `config/*.yaml`, `.env`, `credentials/`, `*.db`, or bank files (`*.qfx`, `*.csv`)
- NEVER store API keys, account numbers, or credentials in the database or source code
- Test data must use placeholder values — no real PII
- The credentials Docker volume is mounted read-only; do not change this
- See SECURITY.md for the full data protection model
