# MoMoney Setup Guide

Complete instructions for configuring, deploying, and maintaining MoMoney on a
Synology DS224+ NAS running Docker.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Google Cloud Setup](#2-google-cloud-setup)
3. [Google Sheets Setup](#3-google-sheets-setup)
4. [Project Configuration](#4-project-configuration)
5. [Synology NAS Deployment](#5-synology-nas-deployment)
6. [Importing Data](#6-importing-data)
7. [Day-to-Day Usage](#7-day-to-day-usage)
8. [CLI Reference](#8-cli-reference)
9. [Maintenance](#9-maintenance)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Prerequisites

### Hardware

- Synology DS224+ NAS (or any Docker-capable Synology model)
- DSM 7.2 or later
- Container Manager (Docker) package installed via Package Center

### Software

- Python 3.11+ (for local development; Docker image uses 3.12)
- Git
- A Google Workspace account (required for Gmail API domain-wide delegation)

### Bank Accounts Supported

MoMoney ships with example account definitions. Copy
`config/examples/accounts.yaml.example` to `config/accounts.yaml` and edit to
match your own banks. The examples cover these account types:

| Example Account ID | Institution | Format | Dedup Key |
|---|---|---|---|
| `primary-checking` | Your Bank | QFX (SGML) | FITID |
| `business-checking` | Mercury | CSV | Timestamp |
| `primary-savings` | Your Bank | QFX (SGML) | FITID |
| `primary-credit` | Capital One | QFX (XML) | FITID |
| `secondary-credit` | American Express | QFX (XML) | FITID |
| `auto-loan` | Your Credit Union | QFX (SGML) | FITID |

---

## 2. Google Cloud Setup

MoMoney uses a single GCP service account for both Google Sheets (read/write)
and Gmail (read-only receipt lookup). Domain-wide delegation is required for
Gmail impersonation.

### 2.1 Create a GCP Project

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Click **Select a project** > **New Project**
3. Name it `momoney` (or anything you prefer)
4. Click **Create**

### 2.2 Enable APIs

In your new project, go to **APIs & Services > Library** and enable:

- **Google Sheets API**
- **Google Drive API** (required by gspread for sheet access)
- **Gmail API**

### 2.3 Create a Service Account

1. Go to **APIs & Services > Credentials**
2. Click **Create Credentials > Service Account**
3. Name: `momoney-service`
4. Click **Create and Continue**
5. **Skip the optional role/access steps** — the wizard offers to assign
   project-level IAM roles (like "Editor" or "Viewer"), but MoMoney doesn't
   need them. Access to Sheets comes from sharing the spreadsheet with the
   service account email (step 3.2). Access to Gmail comes from domain-wide
   delegation (step 2.4). Click **Done**.
6. Click the newly created service account email to open its detail page
7. Go to the **Keys** tab
8. Click **Add Key > Create new key > JSON**
9. Save the downloaded file — this is your `service-account.json`

**Keep this file secure.** It grants access to your Gmail and Sheets. Never
commit it to git.

### 2.4 Configure Domain-Wide Delegation (Gmail)

Gmail API access requires domain-wide delegation because the service account
impersonates your Google Workspace user to read receipts.

1. On the service account detail page, click **Advanced settings** (or scroll down
   to see the **Domain-wide Delegation** section)
2. Under **Domain-wide Delegation**, copy the **Client ID** (a long numeric string)
3. **IMPORTANT**: Ignore the "Google Workspace Marketplace OAuth Client" section
   below — that is only for apps published to the Marketplace and is **not needed**
   for MoMoney. Do not click "Configure" for the OAuth consent screen.
4. Click **View Google Workspace Admin Console** (or go directly to
   [admin.google.com](https://admin.google.com))
5. In the Admin Console, navigate to **Security > Access and data control >
   API controls > Manage domain-wide delegation**
6. Click **Add new**
7. **Client ID**: paste the Client ID you copied in step 2
8. **OAuth scopes**: enter these three scopes (comma-separated):
   ```
   https://www.googleapis.com/auth/gmail.readonly,https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive
   ```
9. Click **Authorize**

**Note**: As of 2026, Google's UI no longer requires you to manually "enable"
domain-wide delegation on the service account. It's available by default. You
only need to copy the Client ID and authorize it in the Workspace Admin Console.

### 2.5 Note Your Gmail Address

The `FINANCE_GMAIL_USER` environment variable should be set to the Google
Workspace email address the service account will impersonate for receipt
lookups (e.g., `user@example.com`).

Gmail receipt lookup searches for Apple and Amazon receipt emails near
transaction dates, uses Claude AI to extract line items, then matches
them back to bank charges. This enables automatic split-allocation
categorization for aggregated charges (e.g., multiple Apple subscriptions
billed as one charge).

---

## 3. Google Sheets Setup

MoMoney uses a Google Sheet as a review UI. The app pushes data to the
sheet; you can enter overrides in special columns that the app polls and
applies back to SQLite.

### 3.1 Create the Spreadsheet

1. Go to [sheets.google.com](https://sheets.google.com)
2. Create a new blank spreadsheet
3. Name it `MoMoney Finance` (or anything)
4. Copy the **Spreadsheet ID** from the URL:
   ```
   https://docs.google.com/spreadsheets/d/SPREADSHEET_ID_HERE/edit
   ```
   This is your `FINANCE_SPREADSHEET_ID`.

### 3.2 Share with the Service Account

1. Click **Share** on the spreadsheet
2. Enter the service account email (looks like
   `momoney-service@your-project.iam.gserviceaccount.com`)
3. Grant **Editor** access
4. Uncheck "Notify people"
5. Click **Share**

### 3.3 Required Worksheets

The app auto-creates these tabs on first push, but you can create them
manually:

| Tab Name | Purpose | Override Columns |
|---|---|---|
| `Transactions` | One row per bank transaction | N: Reviewed, O: Transfer Link, P: Needs Split, Q: Notes |
| `Allocations` | One row per category split | I: New Category, J: Merchant |
| `Review` | Flagged transactions needing manual review | V: New Category, W: Merchant, X: Reviewed |
| `Transfers` | Linked transfer pairs | (none) |
| `Summary` | Monthly category totals | (none) |
| `Categories` | Flattened category hierarchy (auto-populated) | (none) |

### 3.4 Using Override Columns

To correct the app's categorization from the Sheet:

- **Reviewed** (Transactions col N): Type `TRUE` to mark a flagged
  transaction as reviewed/accepted
- **Transfer Link** (Transactions col O): Paste a transaction ID to
  manually link two transactions as a transfer pair
- **Needs Split** (Transactions col P): Type `TRUE` to flag a transaction
  for splitting into multiple allocations
- **New Category** (Allocations col I): Type a category ID (e.g.,
  `groceries`, `coffee-d`) to override the auto-assigned category
- **Merchant** (Allocations col J): Type a corrected merchant name to
  update the transaction's normalized description

The **Review** tab works the same way — override a flagged transaction's
category (col V), merchant (col W), or mark it reviewed (col X).

**Dropdown validation**: Override: Category columns on all three sheets
have data validation dropdowns referencing the Categories sheet. Users can
select from valid category IDs instead of typing them manually. Invalid
values typed directly are warned (with `strict=False`).

**Applying overrides**: Run `momoney poll` to read override values from
Sheets, apply them to SQLite, and clear the cells. Invalid category IDs
are auto-corrected via fuzzy matching when a close match exists.

```bash
docker compose exec momoney momoney poll
```

---

## 4. Project Configuration

### 4.1 Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `FINANCE_DB_PATH` | No | `finance.db` | Path to SQLite database file |
| `FINANCE_WATCH_DIR` | No | `import` | Drop folder for bank files |
| `FINANCE_CREDENTIALS` | Yes* | — | Path to `service-account.json` (auto-configured in Docker) |
| `FINANCE_GMAIL_USER` | No* | — | Google Workspace email for Gmail impersonation |
| `FINANCE_SPREADSHEET_ID` | Yes* | — | Google Sheets spreadsheet ID |
| `ANTHROPIC_API_KEY` | No* | — | Anthropic API key for Claude AI categorization and receipt parsing |
| `FINANCE_CONFIG_DIR` | No | `config` | Path to YAML config directory |
| `FINANCE_MIGRATIONS_DIR` | No | `src/database/migrations` | Path to SQL migrations |
| `FINANCE_LOG_LEVEL` | No | `INFO` | Logging level: DEBUG, INFO, WARNING, ERROR |

*Required for specific features. `FINANCE_CREDENTIALS` + `FINANCE_SPREADSHEET_ID`
for Sheets push. `FINANCE_CREDENTIALS` + `FINANCE_GMAIL_USER` for Gmail receipt
lookup. `ANTHROPIC_API_KEY` for Claude AI categorization (Step 7) and receipt
parsing. The app runs without them but skips those features.

### 4.2 Create a `.env` File

In the project root, create a `.env` file (already in `.gitignore`):

```bash
FINANCE_GMAIL_USER=user@example.com
FINANCE_SPREADSHEET_ID=your-spreadsheet-id-here
ANTHROPIC_API_KEY=sk-ant-...
```

Docker Compose reads `${VARIABLE:-default}` syntax from `.env`
automatically.

**Note about `FINANCE_CREDENTIALS`**: You don't need to set this in `.env`.
The `docker-compose.yaml` hard-codes it to `/app/credentials/service-account.json`.
Just place your `service-account.json` file in the `credentials/` directory
(see Section 5.2), and Docker handles the rest.

For local development (without Docker), set `FINANCE_CREDENTIALS` in your
shell environment:

```bash
export FINANCE_CREDENTIALS=/path/to/your/service-account.json
```

### 4.3 Configuration Files

All YAML config files live in `config/`. Example templates are provided in
`config/examples/`. Copy them and customize to match your accounts and
spending patterns:

```bash
cp config/examples/accounts.yaml.example config/accounts.yaml
cp config/examples/categories.yaml.example config/categories.yaml
cp config/examples/merchants.yaml.example config/merchants.yaml
cp config/examples/rules.yaml.example config/rules.yaml
cp config/examples/parsers.yaml.example config/parsers.yaml
cp config/examples/budget_app_category_map.yaml.example config/budget_app_category_map.yaml
```

The `config/*.yaml` files are gitignored so your personal configuration
(account numbers, category names, merchant mappings) stays out of version
control.

#### `config/accounts.yaml` — Bank Account Definitions

Each account entry needs:

```yaml
accounts:
  - id: wf-checking              # Internal ID used everywhere
    name: "My Checking"            # Display name
    institution: Wells Fargo
    account_type: checking        # checking, savings, credit, loan
    is_active: true
    import_format: wellsfargo_qfx # Parser to use
    qfx_acctid: "1234567890"     # ACCTID from your QFX files
```

**To find your `qfx_acctid`**: Open a downloaded QFX file in a text editor
and search for `<ACCTID>`. The value after that tag is your account ID.

**Optional account-level features:**

- **`category_filter`** — Restricts which categories can be assigned to
  transactions from this account. When a merchant match resolves to an
  incompatible category, the pipeline overrides it to `default_category`.
  Useful for business accounts where personal categories don't apply:

  ```yaml
  category_filter:
    default_category: biz-other
    compatible_prefixes: ["biz-"]
    compatible_ids:
      - earnings
      - cashback
      - interest-fees
  ```

- **`transfer_aliases`** — Additional names that should resolve to this
  account during transfer detection. Useful for closed sub-accounts or
  historical names that appear in budget app exports or bank descriptions:

  ```yaml
  transfer_aliases: ["7187 Vacation", "Old Savings Name"]
  ```

- **`interest_detection`** — Detects interest entries by FITID suffix.
  Some banks (e.g., Golden 1) append a suffix like `INT` to the FITID
  for interest charges:

  ```yaml
  interest_detection:
    fitid_suffix: "INT"
    category_id: interest-fees
  ```

See `config/examples/accounts.yaml.example` for complete examples.

#### `config/merchants.yaml` — Merchant-to-Category Mapping

Two tiers of merchant matching:

```yaml
auto:        # 100% confidence — exact or contains match
  - match: contains
    pattern: "STARBUCKS"
    category_id: coffee-d

high:        # 80-99% confidence — broader patterns
  - match: contains
    pattern: "COSTCO"
    category_id: groceries
    consistency: 0.68
```

#### `config/rules.yaml` — Transfer Patterns and Amount Rules

Defines how to detect inter-account transfers and disambiguate merchants
by amount:

```yaml
transfers:
  - pattern: "CAPITAL ONE MOBILE PMT"
    from_account: wf-checking
    to_account: cap1-credit
    type: cc-payment

# Transfer detection also works automatically for descriptions matching
# "Transfer : <account name>" (common in QFX files and budget app exports).
# The app resolves account names via the `name`, `budget_app_name`, and
# `transfer_aliases` fields in accounts.yaml — no rules.yaml entry needed.

amount_rules:
  - merchant_pattern: "APPLE.COM/BILL"
    rules:
      - amount_range: [-9.99, -9.99]
        category_id: cloud-storage
      - amount_range: [-22.99, -22.99]
        category_id: youtube-premium
```

**Additional rules.yaml sections:**

- **`transfer_categories`** — Maps transfer types (from transfer detection)
  to category IDs. Customize if your categories use different IDs:

  ```yaml
  transfer_categories:
    cc-payment: xfer-cc-payment
    savings-transfer: xfer-savings
    loan-payment: xfer-loan-payment
    internal-transfer: xfer-internal
  ```

- **`receipt_categories`** — Category IDs suggested to the Claude AI model
  when parsing Apple/Amazon email receipts. Should be leaf categories from
  your `categories.yaml`:

  ```yaml
  receipt_categories:
    - cloud-storage
    - youtube-premium
    - household-supplies
    - groceries
  ```

- **`fallback_category`** — Category assigned when no rule matches
  (these go into the manual review queue):

  ```yaml
  fallback_category: uncategorized
  ```

See `config/examples/rules.yaml.example` for complete examples.

#### `config/categories.yaml` — Category Hierarchy

Defines all valid category IDs. Add new categories here before referencing
them in merchants or rules.

#### `config/budget_app_category_map.yaml` — Budget App Migration Map

Maps old budget app category names to new category IDs. Only needed for the
one-time `momoney import-budget-app` command.

#### `config/parsers.yaml` — Parser Format Reference

Documents the field mapping for each bank's file format. This is a
reference file — the actual parsing logic is in `src/parsers/`.

---

## 5. Synology NAS Deployment

### 5.1 Directory Structure on the NAS

Create these directories on your Synology NAS. Using SSH or File Station:

```
/volume1/docker/momoney/
├── data/                    # SQLite database (persistent)
├── import/                  # Drop folder for bank files
├── credentials/             # Service account key (read-only mount)
│   └── service-account.json
└── .env                     # Environment overrides
```

```bash
# Via SSH
mkdir -p /volume1/docker/momoney/{data,import,credentials}
```

### 5.2 Copy the Service Account Key

Copy your `service-account.json` to the credentials directory:

```bash
scp service-account.json user@nas:/volume1/docker/momoney/credentials/
```

Set restrictive permissions:

```bash
chmod 600 /volume1/docker/momoney/credentials/service-account.json
```

### 5.3 Create the `.env` File

```bash
cat > /volume1/docker/momoney/.env << 'EOF'
FINANCE_GMAIL_USER=user@example.com
FINANCE_SPREADSHEET_ID=your-spreadsheet-id-here
EOF
```

### 5.4 Deploy with Container Manager (GUI)

If you prefer the Synology GUI over SSH:

1. Open **Container Manager** from DSM
2. Go to **Project** > **Create**
3. Project name: `momoney`
4. Path: `/volume1/docker/momoney`
5. Copy the project source code to `/volume1/docker/momoney/` (the repo
   root containing `Dockerfile`, `docker-compose.yaml`, `src/`, `config/`)
6. Container Manager will detect the `docker-compose.yaml`
7. Click **Build** then **Start**

### 5.5 Deploy with Docker Compose (SSH)

Alternatively, clone your repo directly on the NAS and use the CLI:

```bash
cd /volume1/docker/momoney

# Clone your private repo (created from the template)
git clone https://github.com/yourusername/your-repo-name.git .

# Copy credentials
cp /path/to/service-account.json credentials/

# Create .env
cat > .env << 'EOF'
FINANCE_GMAIL_USER=user@example.com
FINANCE_SPREADSHEET_ID=your-spreadsheet-id-here
EOF

# Build and start
docker compose up -d --build
```

### 5.6 Verify the Container

```bash
# Check it's running
docker compose ps

# View logs
docker compose logs -f momoney

# Expected output:
# 2026-01-31 12:00:00 INFO src.watcher.observer: Watching /app/import for new bank files
```

### 5.7 Volume Mapping Summary

| Container Path | Host Path | Mode | Purpose |
|---|---|---|---|
| `/app/data` | `./data` | rw | SQLite database |
| `/app/import` | `./import` | rw | Drop folder for bank files |
| `/app/credentials` | `./credentials` | **ro** | Service account key (read-only) |

The credentials volume is mounted read-only for security — the app only
needs to read the JSON key file.

---

## 6. Importing Data

### 6.1 One-Time Budget App Historical Import

If migrating from a previous budget app, export your register as CSV first:

1. In your budget app, go to **File > Export Budget > Register**
2. Save the CSV file
3. Run the import:

```bash
# Local
momoney import-budget-app /path/to/budget-register.csv

# Docker
docker compose exec momoney momoney import-budget-app /app/import/budget-register.csv
```

This maps budget app categories to MoMoney categories using
`config/budget_app_category_map.yaml` and imports all historical transactions.

### 6.2 Importing Bank Files (Manual)

Drop QFX/OFX/CSV files into the import directory:

```bash
# Copy files to the import folder
cp ~/Downloads/WF-checking-2026-01.qfx /volume1/docker/momoney/import/

# Or import a specific file manually
docker compose exec momoney momoney import --file /app/import/WF-checking-2026-01.qfx
```

### 6.3 Automatic File Watching

The default Docker CMD is `momoney watch`, which polls the import directory
every 30 seconds for new files. The workflow:

1. Download a QFX/CSV file from your bank's website
2. Copy/move it into the `import/` folder (via File Station, SMB share,
   or `scp`)
3. The watcher detects the file, waits for it to stabilize (10 seconds of
   no size/mtime changes), validates completeness, then runs the full
   pipeline:
   - **Parse** the file (auto-detects format by extension + content)
   - **Dedup** against existing transactions (5-tier: file hash →
     external ID → import hash → cross-format → fuzzy dedup key)
   - **Categorize** using the 8-step pipeline
   - **Push** new data to Google Sheets

### 6.4 Supported File Formats

| Extension | Format | Banks |
|---|---|---|
| `.qfx` | OFX/QFX (SGML or XML) | Wells Fargo, Capital One, Amex, Golden 1 |
| `.ofx` | OFX (same as QFX) | Any bank exporting OFX |
| `.csv` | Mercury CSV or budget app CSV | Mercury, budget app export |

The parser auto-detection reads the file header to determine which parser
to use. You do not need to specify the bank or format.

---

## 7. Day-to-Day Usage

### 7.1 Typical Workflow

1. **Download** bank files from your bank websites (weekly or monthly)
2. **Drop** them into the `import/` folder
3. The watcher processes them automatically
4. **Review** the Google Sheet for flagged transactions
5. **Override** categories or mark as reviewed using the override columns
6. **Poll** overrides: `docker compose exec momoney momoney poll`

### 7.2 Checking Status

```bash
docker compose exec momoney momoney status
```

Outputs counts of pending, categorized, flagged, and reviewed transactions.

### 7.3 Reviewing Flagged Transactions

```bash
docker compose exec momoney momoney review
```

Lists transactions that the categorization pipeline could not auto-categorize
(fell through to Step 8: manual review).

### 7.4 Force a Full Sheets Rebuild

If the Sheet gets out of sync or you want a clean refresh:

```bash
docker compose exec momoney momoney push
```

This clears all Sheet tabs and rebuilds from the SQLite database.

**Note**: `momoney import` and `momoney import-budget-app` push incrementally
(appending new data to Sheets). Only `momoney push` does a full rebuild.

### 7.5 Re-Processing Flagged Transactions

If you update config (e.g., add a `transfer_aliases` entry or new merchant
pattern) and want to re-run flagged transactions through the pipeline:

```bash
# Preview what would change
docker compose exec momoney python -m scripts.reprocess_flagged --dry-run

# Apply changes
docker compose exec momoney python -m scripts.reprocess_flagged
```

---

## 8. CLI Reference

All commands are available via the `momoney` entry point:

```
momoney import [--file PATH]     Import a specific file or batch-process all
                                 files in the watch directory

momoney watch                    Start the file watcher daemon (default Docker
                                 command). Polls every 30 seconds.

momoney status                   Show pending/categorized/flagged/reviewed
                                 transaction counts

momoney review                   List transactions flagged for manual review

momoney reconcile [ACCOUNT]      Run balance reconciliation for an account
                                 or all accounts

momoney push                     Force full Google Sheets rebuild from SQLite

momoney import-budget-app FILE         One-time import of budget app register CSV

momoney poll                     Poll Sheets for override corrections and
                                 apply to SQLite

momoney category list            Print the category hierarchy tree
momoney category add             Add a new category under a parent node
momoney category rename          Rename an existing category
momoney category move            Move a category to a different parent
```

### Running CLI Commands in Docker

Prefix with `docker compose exec momoney`:

```bash
docker compose exec momoney momoney status
docker compose exec momoney momoney import --file /app/import/file.qfx
docker compose exec momoney momoney push
docker compose exec momoney momoney poll
docker compose exec momoney momoney category list
```

---

## 9. Maintenance

### 9.1 Database Backups

The SQLite database lives at `data/finance.db`. Back it up regularly:

```bash
# Simple file copy (safe when app is idle)
cp /volume1/docker/momoney/data/finance.db \
   /volume1/backups/finance-$(date +%Y%m%d).db

# Using SQLite backup command (safe while app is running)
docker compose exec momoney \
  sqlite3 /app/data/finance.db ".backup '/app/data/finance-backup.db'"
```

For automated backups on Synology, use **Hyper Backup** pointed at
the `data/` directory, or set up a cron job via **Task Scheduler**:

1. Open **Control Panel > Task Scheduler**
2. Create a **Scheduled Task > User-defined script**
3. Schedule: daily at 3:00 AM
4. Script:
   ```bash
   cp /volume1/docker/momoney/data/finance.db \
      /volume1/backups/momoney/finance-$(date +%Y%m%d).db
   # Keep last 30 days
   find /volume1/backups/momoney/ -name "finance-*.db" -mtime +30 -delete
   ```

### 9.2 Updating the Application

Since your repo was created from a template, updates are managed by you.
If you've made local changes, simply rebuild:

```bash
cd /volume1/docker/momoney

# Rebuild and restart the container
docker compose up -d --build
```

Database migrations run automatically on startup (`repo.apply_migrations()`
is called before any command processes data).

### 9.3 Viewing Logs

```bash
# Live log stream
docker compose logs -f momoney

# Last 100 lines
docker compose logs --tail 100 momoney
```

Log level is controlled by `FINANCE_LOG_LEVEL`. Set to `DEBUG` for verbose
output during troubleshooting:

```bash
# In .env or docker-compose.yaml
FINANCE_LOG_LEVEL=DEBUG
```

Then restart: `docker compose restart momoney`

### 9.4 API Key Rotation

#### Rotating the Google Service Account Key

1. Go to **GCP Console > IAM & Admin > Service Accounts**
2. Click your service account
3. Go to **Keys** tab
4. Click **Add Key > Create new key > JSON**
5. Download the new key
6. Replace the old key on the NAS:
   ```bash
   cp new-service-account.json \
      /volume1/docker/momoney/credentials/service-account.json
   chmod 600 /volume1/docker/momoney/credentials/service-account.json
   ```
7. Restart the container: `docker compose restart momoney`
8. Verify it works: `docker compose exec momoney momoney status`
9. Go back to GCP Console and **delete the old key**

The credentials volume is mounted read-only so the app cannot modify
the key file. You must replace it from the host.

### 9.5 Managing Google API Quotas

**Google Sheets API**: 300 requests per minute per project. The app
implements rate limiting (50 writes/minute) to stay well under this.

**Gmail API**: 250 quota units per user per second. Receipt lookups are
infrequent (only for Apple/Amazon transactions that aren't matched by
merchant rules).

API usage is tracked in the `api_usage` table in SQLite:

```bash
docker compose exec momoney \
  sqlite3 /app/data/finance.db \
  "SELECT month, service, request_count, estimated_cost_cents FROM api_usage"
```

### 9.6 Disk Space

The SQLite database is compact. For reference:
- 14,420 historical transactions ≈ 5 MB
- WAL mode creates temporary `-wal` and `-shm` files alongside the
  database (auto-cleaned by SQLite)

Monitor disk usage:

```bash
du -sh /volume1/docker/momoney/data/
```

---

## 10. Troubleshooting

### Container won't start

```bash
# Check build errors
docker compose build momoney 2>&1 | tail -20

# Check runtime errors
docker compose logs momoney
```

Common causes:
- Missing `service-account.json` in `credentials/`
- Invalid YAML in config files
- Python dependency conflict

### "Sheets not available" warning

This means the Sheets connection failed. Check:

1. `FINANCE_SPREADSHEET_ID` is set and correct
2. `FINANCE_CREDENTIALS` points to a valid JSON key file
3. The service account email has Editor access to the spreadsheet
4. Google Sheets API is enabled in your GCP project

The app continues to work without Sheets — it just skips the push step.

### Gmail receipt lookup not working

1. Verify domain-wide delegation is configured (Section 2.4)
2. Check that `FINANCE_GMAIL_USER` matches a real Google Workspace email
3. Confirm the Gmail API is enabled in your GCP project
4. Check logs for auth errors:
   ```bash
   docker compose logs momoney | grep -i gmail
   ```

### Duplicate file detected but I want to re-import

The app tracks file hashes to prevent re-importing the same file. If you
need to re-process a file (e.g., after fixing config):

```bash
# Find the import record
docker compose exec momoney \
  sqlite3 /app/data/finance.db \
  "SELECT id, file_name, file_hash FROM imports WHERE file_name LIKE '%filename%'"

# Delete the import record (cascading delete removes transactions)
docker compose exec momoney \
  sqlite3 /app/data/finance.db \
  "DELETE FROM imports WHERE id = 'the-import-id'"
```

Then re-drop the file into `import/`.

### File watcher not detecting files

The watcher uses PollingObserver (30-second interval) because Docker
volumes on Synology NAS don't support inotify reliably. Files may take
up to 30 seconds to be detected, plus 10 seconds of stability checking.

If files still aren't detected:

1. Verify the file has a supported extension (`.qfx`, `.ofx`, `.csv`)
2. Check the file isn't empty or incomplete
3. Check permissions — the container runs as root by default and should
   have access to the mounted volume
4. Check logs for errors:
   ```bash
   docker compose logs -f momoney
   ```

### Resetting the database

To start fresh (destroys all data):

```bash
docker compose stop momoney
rm /volume1/docker/momoney/data/finance.db*
docker compose start momoney
```

The database and schema are recreated automatically on next startup.

---

## Appendix: Architecture Overview

```
Bank Website          Synology DS224+ NAS                  Google Cloud
─────────────         ──────────────────────               ────────────

QFX/CSV files    ──>  import/  ──>  File Watcher
                                        │
                                    Parse + Dedup
                                        │
                                    Categorize (8-step)  ──>  Gmail API
                                        │                     (receipt lookup)
                                    SQLite DB             ──>  Claude API
                                        │                     (AI categorization)
                                    Sheets Push    ──────>  Google Sheets
                                        │                   (review UI)
                                    Override Poller <──────  (user corrections)
```

Data flows from bank files → SQLite → Sheets. The only data that flows
back from Sheets is override corrections entered by the user in designated
columns, which the app polls and applies to SQLite automatically.
