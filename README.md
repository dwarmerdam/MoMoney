# MoMoney

> **Template Repository** — Click **"Use this template"** on GitHub to create
> your own private copy. Do not fork or clone this repo directly.

Self-hosted personal finance tracker. Import bank transactions, auto-categorize, push to Google Sheets.

---

## Overview

MoMoney is a local-first personal finance application designed to run on a
Synology NAS (or any Docker host). It watches a folder for bank export files,
automatically parses and categorizes transactions, stores everything in a local
SQLite database, and pushes a read-only view to Google Sheets for review.

This repository is a **template** — it contains working code with placeholder
configuration so you can spin up your own instance. All personal data (account
numbers, merchant patterns, category structures) lives in gitignored YAML
config files that you customize after creating your copy.

**Why?** Most budget apps require manual transaction entry or expensive
subscriptions. MoMoney automates the entire workflow — download a file from
your bank, drop it in a folder, and the system handles the rest.

---

## Features

### 8-Step Categorization Pipeline

Every transaction runs through a prioritized fallback chain:

1. **Transfer Detection** — Recognizes inter-account transfers by config
   patterns (e.g., "CAPITAL ONE MOBILE PMT") and by "Transfer :" description
   prefix with account name resolution
2. **Merchant Auto-Match** — 100% confidence exact/contains matches from your
   merchant dictionary
3. **Amount Rules** — Disambiguates by transaction amount (e.g., Apple $9.99 =
   iCloud Storage vs. $22.99 = YouTube Premium)
4. **Account Rules** — Default categories for specific accounts (e.g., business
   checking defaults to a business category)
5. **Merchant High-Confidence** — 80-99% confidence broader pattern matches
6. **Gmail Receipt Lookup** — Searches Apple and Amazon receipt emails near
   transaction dates, uses Claude AI to parse line items
7. **Claude AI Categorization** — Sends ambiguous transactions to Claude for
   single-shot category suggestions
8. **Manual Review** — Flags unmatched transactions for human review

### Multi-Bank Support

| Format | Banks |
|--------|-------|
| QFX/OFX (SGML) | Wells Fargo, Golden 1, and other SGML-format banks |
| QFX/OFX (XML) | Capital One, American Express, and other XML-format banks |
| CSV | Mercury, budget app historical export |

Auto-detects file format by extension and content — no manual bank selection needed.

### Additional Capabilities

- **5-tier deduplication** — File hash, external ID, import hash, cross-format
  matching (bank vs budget app), and fuzzy dedup key prevent duplicate
  transactions across re-imports and across different source formats
- **Google Sheets UI** — Transactions, Allocations, Review, Transfers,
  Summary, and Categories tabs with override columns for correcting
  categories, linking transfers, and marking transactions as reviewed.
  Override: Category columns have dropdown validation referencing the
  Categories sheet. Run `momoney poll` to apply overrides back to SQLite.
- **File watcher daemon** — Polls the import directory every 30 seconds for
  new bank files; waits for file stability before processing
- **Business account filtering** — Restricts category assignments for business
  accounts to compatible categories only
- **Transfer aliases** — Maps closed or renamed accounts (e.g., "7187 Vacation")
  to active account IDs for transfer resolution
- **Receipt lookup status** — Tracks Gmail receipt lookup outcomes per
  transaction (matched, no_email, no_match, error, budget_exceeded)
- **Interest detection** — Recognizes interest entries by FITID suffix patterns
- **Balance reconciliation** — Compares statement balances against computed
  balances to catch discrepancies
- **API cost tracking** — Monitors Google Sheets and Anthropic API usage per
  month in the SQLite database
- **Budget app historical import** — One-time migration from a budget app with
  configurable category mapping
- **SQLite local-first** — All data stays on your hardware; Google Sheets is
  an optional read-only mirror

---

## Architecture

```
Bank Website          Docker Host (NAS)                    Google Cloud
─────────────         ───────────────────────              ────────────

QFX/CSV files    ──>  import/  ──>  File Watcher
                                        │
                                    Parse + Dedup
                                        │
                                    Categorize (8-step)  ──>  Gmail API
                                        │                     (receipt lookup)
                                    SQLite DB             ──>  Claude API
                                        │                     (AI categorization)
                                    Sheets Push    ──────>  Google Sheets
                                        │                   (read-only UI)
                                    Override Poller <──────  (user corrections)
```

Data flows from bank files → SQLite → Sheets. The only data that flows back
from Sheets is override corrections entered by the user in designated
columns, which the app polls and applies to SQLite automatically.

---

## Quick Start

### Prerequisites

- Docker and Docker Compose
- A Google Cloud service account with Sheets, Drive, and Gmail APIs enabled
  (see [SETUP.md](SETUP.md#2-google-cloud-setup) for detailed instructions)
- An Anthropic API key (optional, for Claude AI categorization)

### 1. Create Your Repository

Click **"Use this template"** > **"Create a new repository"** on GitHub.
Make your new repo **private** — it will contain your personal financial
configuration.

Then clone your new repo:

```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
```

### 2. Configure

Copy the example config files and customize them for your accounts:

```bash
cp config/examples/accounts.yaml.example config/accounts.yaml
cp config/examples/categories.yaml.example config/categories.yaml
cp config/examples/merchants.yaml.example config/merchants.yaml
cp config/examples/rules.yaml.example config/rules.yaml
cp config/examples/parsers.yaml.example config/parsers.yaml
cp config/examples/budget_app_category_map.yaml.example config/budget_app_category_map.yaml
```

Edit `config/accounts.yaml` to define your bank accounts. See
[SETUP.md Section 4](SETUP.md#4-project-configuration) for detailed
configuration guidance.

### 3. Set Up Environment

Create a `.env` file in the project root:

```bash
cat > .env << 'EOF'
FINANCE_GMAIL_USER=your-email@example.com
FINANCE_SPREADSHEET_ID=your-google-sheets-id
ANTHROPIC_API_KEY=sk-ant-your-key-here
EOF
```

### 4. Add Credentials

```bash
mkdir -p credentials
cp /path/to/service-account.json credentials/
```

### 5. Deploy

```bash
docker compose up -d --build
```

The container starts the file watcher daemon by default. Drop bank files into
`import/` and they'll be processed automatically.

See [SETUP.md](SETUP.md) for complete setup instructions including Google Cloud
configuration, Synology NAS deployment, and troubleshooting.

---

## Configuration

All configuration lives in `config/*.yaml` files. Example templates are
provided in `config/examples/`. These files are gitignored to keep personal
data out of version control.

| File | Purpose |
|------|---------|
| `accounts.yaml` | Bank account definitions (IDs, institutions, formats, QFX account IDs) |
| `categories.yaml` | Hierarchical category tree (income, expenses, transfers) |
| `merchants.yaml` | Merchant-to-category mapping with two confidence tiers |
| `rules.yaml` | Transfer patterns, amount-based disambiguation, receipt categories |
| `parsers.yaml` | Parser format reference for each bank |
| `budget_app_category_map.yaml` | Budget app-to-MoMoney category migration mapping |

See [SETUP.md Section 4.3](SETUP.md#43-configuration-files) for detailed
documentation of each configuration file.

---

## CLI Reference

All commands are available via the `momoney` entry point:

| Command | Description |
|---------|-------------|
| `momoney watch` | Start the file watcher daemon (default Docker command) |
| `momoney import [--file PATH]` | Import a specific file or all pending files in the watch directory |
| `momoney import-budget-app FILE` | One-time budget app register CSV import with category mapping |
| `momoney status` | Show transaction counts, pending items, and API costs |
| `momoney review` | List transactions flagged for manual review (top 50) |
| `momoney reconcile [ACCOUNT]` | Run balance reconciliation for one or all accounts |
| `momoney push` | Force full Google Sheets rebuild from SQLite |
| `momoney poll` | Poll Sheets for override corrections and apply to SQLite |
| `momoney category list` | Print the category hierarchy tree |
| `momoney category add` | Add a new category under a parent node |
| `momoney category rename` | Rename an existing category |
| `momoney category move` | Move a category to a different parent |

### Running Commands in Docker

```bash
docker compose exec momoney momoney status
docker compose exec momoney momoney import --file /app/import/bank-export.qfx
docker compose exec momoney momoney review
docker compose exec momoney momoney push
docker compose exec momoney momoney poll
docker compose exec momoney momoney category list

# View live logs
docker compose logs -f momoney
```

---

## Project Structure

```
MoMoney/
├── src/
│   ├── categorize/          # 8-step categorization pipeline
│   │   ├── pipeline.py      # Main orchestrator
│   │   ├── merchant.py      # Merchant matching (Steps 2, 5)
│   │   ├── transfer_detect.py # Transfer detection (Step 1)
│   │   ├── amount_rules.py  # Amount-based rules (Step 3)
│   │   └── claude_ai.py     # Claude AI categorization (Steps 6, 7)
│   ├── database/            # SQLite repository, models, migrations, dedup
│   ├── gmail/               # Gmail API client for receipt lookup
│   ├── parsers/             # QFX (SGML/XML), CSV, and budget app parsers
│   ├── sheets/              # Google Sheets push and override polling
│   ├── watcher/             # File watcher daemon with stability checking
│   ├── cli.py               # CLI entry point (10 commands)
│   └── config.py            # YAML configuration loader
├── scripts/                 # Utility scripts (reprocess_flagged.py)
├── tests/                   # 800+ tests across 32 test files
├── config/
│   └── examples/            # Configuration templates (6 YAML files)
├── Dockerfile               # Python 3.12-slim image
├── docker-compose.yaml      # Service definition with volume mounts
├── pyproject.toml           # Package metadata and dependencies
├── SETUP.md                 # Detailed setup, deployment, and maintenance guide
└── README.md                # This file
```

---

## Development

### Local Setup

```bash
# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Copy example configs
cp config/examples/*.example config/
# Rename: remove .example suffix from each file
```

### Running Tests

```bash
pytest
```

The test suite includes 820+ tests covering all modules: categorization
pipeline, database operations, file parsing, deduplication, Google Sheets
integration, Gmail receipt lookup, file watcher, CLI commands, Docker
configuration, and refund handling.

### Dependencies

| Package | Purpose |
|---------|---------|
| `watchdog` | File system monitoring (PollingObserver) |
| `anthropic` | Claude AI API client |
| `gspread` | Google Sheets client |
| `google-auth` | Google service account authentication |
| `google-api-python-client` | Gmail API client |
| `pyyaml` | YAML configuration parsing |
| `python-dateutil` | Date parsing utilities |

Requires Python 3.11+. Dependencies use minimum version ranges (`>=`) for broad
compatibility; for reproducible production builds, generate a pinned lock file
with `pip freeze > requirements.txt` after a successful install.

---

## Security

**Security Audit Status:** ✅ Passed (Last audit: 2026-02-02)
- CodeQL scan: 0 alerts
- Manual review: No critical vulnerabilities
- See [SECURITY.md](SECURITY.md) for complete audit results and security guidelines

MoMoney handles sensitive financial data. This section documents the security
model, what is and isn't protected, and the reasoning behind those decisions.

### Threat Model

MoMoney is a **single-user, self-hosted application** running on a local NAS
on a private network. The primary threats are:

- Physical theft of the NAS drives
- Unauthorized network access to the NAS
- Accidental exposure of credentials or config files via git

It is **not** designed for multi-user, multi-tenant, or cloud-hosted
deployments. The security model reflects this scope.

### What's Stored in the Database

The SQLite database contains financial transaction data imported from bank
files:

| Table | Sensitive Fields |
|-------|-----------------|
| `transactions` | Dates, amounts, merchant descriptions, balances, bank-issued FITIDs, receipt lookup status |
| `imports` | File names, file hashes, account IDs |
| `allocations` | Category assignments, split amounts |
| `transfers` | Linked transaction pairs between accounts |
| `receipt_matches` | Gmail message IDs, parsed receipt line items (JSON) |
| `api_usage` | Monthly API request counts and estimated costs |

### What's NOT Stored in the Database

- **No bank account numbers or routing numbers.** The `qfx_acctid` values
  from config are used transiently during import to route transactions to the
  correct account. They are never written to the database.
- **No API keys or credentials.** Anthropic API keys, Google service account
  JSON keys, and Gmail credentials are handled exclusively via environment
  variables and mounted files. They never touch the database.
- **No passwords, SSNs, or personal identity data.**

### No Field-Level Encryption (By Design)

The database does **not** encrypt individual fields (amounts, descriptions,
etc.). This is a deliberate decision:

1. **Breaks query functionality.** The CLI commands (`status`, `review`,
   `reconcile`, `push`) need to query, sort, aggregate, and compute over
   transaction amounts and dates. Encrypting these fields would require
   decrypting the entire dataset into memory for every operation, providing
   no meaningful protection for a local single-user application.

2. **Wrong layer of defense.** Field-level encryption protects against
   an attacker who has read access to the raw database file but not the
   decryption key. In a self-hosted deployment where the application and
   database live on the same machine, the decryption key must also be on
   that machine — making field-level encryption security theater.

3. **Data is mirrored to Google Sheets.** If Sheets sync is enabled,
   all transaction data is pushed to Google's servers. Encrypting the local
   copy while the same data sits unencrypted in a Google Sheet would be
   inconsistent.

4. **Full-disk encryption is the correct control.** Synology DSM supports
   volume-level encryption, which protects all data at rest (database,
   config files, credentials, bank exports) transparently and without
   breaking application functionality.

### Credential and Secret Management

| Secret | Storage | Protection |
|--------|---------|------------|
| Google service account key | `credentials/service-account.json` | Gitignored, Docker volume mounted read-only |
| Anthropic API key | `.env` file / environment variable | Gitignored, never logged |
| Gmail user email | `.env` file / environment variable | Gitignored |
| Spreadsheet ID | `.env` file / environment variable | Gitignored |
| Bank account numbers | `config/accounts.yaml` | Gitignored, never written to database |

### Configuration File Isolation

All six YAML config files (`accounts.yaml`, `categories.yaml`,
`merchants.yaml`, `rules.yaml`, `parsers.yaml`, `budget_app_category_map.yaml`)
are listed in `.gitignore`. Example templates in `config/examples/` contain
only placeholder values. This prevents accidental commits of personal
financial data (account numbers, merchant patterns, category structures).

### Docker Security

- The credentials volume is mounted **read-only** (`credentials/:ro`) —
  the application cannot modify or exfiltrate the service account key file
- Bank files in `import/` are processed and left in place; the application
  does not copy them elsewhere
- No ports are exposed by default — the application has no HTTP server or
  API endpoint

### Google API Scopes

The service account is granted the minimum required scopes:

| Scope | Purpose | Access Level |
|-------|---------|-------------|
| `gmail.readonly` | Receipt lookup for Apple/Amazon emails | Read-only |
| `spreadsheets` | Push transaction data to Google Sheets | Read/write |
| `drive` | Required by gspread for sheet access | Read/write (Sheets only) |

Gmail access is read-only. The application searches for receipt emails but
cannot send, delete, or modify any email.

### Git History Safety

The `.gitignore` excludes all sensitive files:

- `*.qfx`, `*.ofx`, `*.csv` (bank exports, except test fixtures)
- `config/*.yaml` (personal configuration)
- `.env`, `.env.*` (environment secrets)
- `credentials/`, `*.json` (service account keys)
- `*.db`, `*.sqlite` (database files)
- `data/`, `import/` (runtime data directories)

### Security Guidelines for Users

If you download and deploy this application, you are responsible for securing
your own instance. Follow these guidelines to protect your financial data.

#### Before You Start

- **Never commit personal config files.** The `.gitignore` is pre-configured
  to exclude `config/*.yaml`, `.env`, `credentials/`, bank files, and database
  files. Do not override this with `git add -f` unless you understand the
  consequences. If you accidentally commit sensitive data, a `git revert` is
  not sufficient — the data remains in git history. You would need to rewrite
  history with a tool like
  [git-filter-repo](https://github.com/newren/git-filter-repo).

- **Review example configs before customizing.** The files in
  `config/examples/` contain only placeholder values. When you create your
  own `config/accounts.yaml`, you will enter real bank account identifiers
  (`qfx_acctid`). These are used only at import time and are never written
  to the database, but they do exist in your local config file — protect it
  accordingly.

- **Create a dedicated Google Cloud project.** Do not reuse a GCP project
  that has broad permissions or other services. Create a project specifically
  for MoMoney so the service account's access is scoped to only your finance
  spreadsheet and Gmail.

#### Deployment Security

- **Enable volume encryption.** If deploying to a Synology NAS, enable
  DSM volume encryption. This protects all data at rest — database, config,
  credentials, and bank exports — transparently if the drives are physically
  stolen. Other NAS platforms and Linux hosts offer similar full-disk or
  partition encryption (LUKS, dm-crypt).

- **Restrict file permissions.** Set `chmod 600` on `finance.db`,
  `service-account.json`, and `.env` so only the owning user can read them.
  Inside Docker this is handled automatically, but if running locally or
  if these files are accessible via SMB/NFS shares, verify permissions
  explicitly.

- **Do not expose the NAS to the internet.** MoMoney has no authentication
  layer, no HTTP server, and no API endpoint. It is designed to run on a
  trusted local network. Do not port-forward Docker or SSH to the public
  internet. If you need remote access, use a VPN.

- **Use a dedicated service account.** The Google service account key
  (`service-account.json`) grants access to your Gmail (read-only) and
  Google Sheets (read/write). Treat it like a password. Store it only in the
  `credentials/` directory, which is Docker-mounted read-only and gitignored.

#### Ongoing Operations

- **Rotate the Google service account key** periodically. Delete old keys
  from the GCP console after replacing them. See
  [SETUP.md Section 9.4](SETUP.md#94-api-key-rotation) for step-by-step
  instructions.

- **Encrypt database backups.** If you copy `finance.db` to external storage,
  USB drives, or cloud backup services, encrypt the backup. The database
  contains your complete transaction history in plaintext.

- **Monitor API usage.** The `api_usage` table tracks Google and Anthropic
  API request counts. Run `momoney status` periodically to check for
  unexpected spikes that might indicate a leaked API key.

- **Review Google Sheets sharing.** The spreadsheet should be shared only
  with your service account email. Periodically verify that no additional
  users or "anyone with the link" access has been granted, as the sheet
  contains your full transaction data.

- **Delete bank files after import.** Once bank exports (QFX/CSV) are
  imported and verified, consider deleting them from the `import/` directory.
  The transaction data is persisted in SQLite; keeping the raw files around
  increases the surface area of sensitive data on disk.

---

## Documentation

- **[SETUP.md](SETUP.md)** — Complete setup guide covering Google Cloud
  configuration, Synology NAS deployment, data import procedures, day-to-day
  usage, maintenance, and troubleshooting
- **[SECURITY.md](SECURITY.md)** — Security model, threat assessment, and
  guidelines for safe deployment and operation

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.
