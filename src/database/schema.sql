-- MoMoney Schema (source of truth)
-- This file is a human-readable reference. Actual DB creation
-- happens via migrations in migrations/001_initial.sql.

-- schema_version: tracks applied migrations
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    description TEXT,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- imports: tracks imported files for Tier 1 dedup
CREATE TABLE imports (
    id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    file_size INTEGER,
    account_id TEXT,
    record_count INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);
CREATE UNIQUE INDEX idx_imports_file_hash ON imports(file_hash);

-- transactions: one row per bank transaction
CREATE TABLE transactions (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    date TEXT NOT NULL,
    amount REAL NOT NULL,
    raw_description TEXT NOT NULL,
    normalized_description TEXT,
    memo TEXT,
    txn_type TEXT,
    check_num TEXT,
    balance REAL,
    external_id TEXT,
    import_id TEXT NOT NULL REFERENCES imports(id),
    import_hash TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'bank',
    status TEXT NOT NULL DEFAULT 'pending',
    confidence REAL,
    categorization_method TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_txn_account_date ON transactions(account_id, date);
CREATE INDEX idx_txn_external_id ON transactions(account_id, external_id);
CREATE INDEX idx_txn_import_hash ON transactions(import_hash);
CREATE INDEX idx_txn_dedup_key ON transactions(dedup_key);
CREATE INDEX idx_txn_status ON transactions(status);
CREATE INDEX idx_txn_import_id ON transactions(import_id);

-- allocations: one or more per transaction (splits)
CREATE TABLE allocations (
    id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL REFERENCES transactions(id),
    category_id TEXT NOT NULL,
    amount REAL NOT NULL,
    memo TEXT,
    tags TEXT,
    source TEXT NOT NULL DEFAULT 'auto',
    confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_alloc_txn ON allocations(transaction_id);
CREATE INDEX idx_alloc_category ON allocations(category_id);

-- transfers: links paired transactions
CREATE TABLE transfers (
    id TEXT PRIMARY KEY,
    from_transaction_id TEXT NOT NULL REFERENCES transactions(id),
    to_transaction_id TEXT NOT NULL REFERENCES transactions(id),
    transfer_type TEXT NOT NULL,
    match_method TEXT NOT NULL,
    confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(from_transaction_id, to_transaction_id)
);
CREATE INDEX idx_xfer_from ON transfers(from_transaction_id);
CREATE INDEX idx_xfer_to ON transfers(to_transaction_id);

-- receipt_matches: Gmail receipt lookup results
CREATE TABLE receipt_matches (
    id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL REFERENCES transactions(id),
    gmail_message_id TEXT NOT NULL,
    match_type TEXT NOT NULL,
    matched_items TEXT,
    confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(gmail_message_id, transaction_id)
);

-- api_usage: cost tracking for Claude + Gmail
CREATE TABLE api_usage (
    id TEXT PRIMARY KEY,
    month TEXT NOT NULL,
    service TEXT NOT NULL,
    request_count INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    estimated_cost_cents INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(month, service)
);
