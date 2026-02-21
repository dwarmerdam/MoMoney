-- Migration 003: Add index on normalized_description for historical pattern matching
-- Step 2.5 queries transactions by normalized_description to find past categorization
-- patterns for the same merchant.

CREATE INDEX IF NOT EXISTS idx_txn_normalized_desc
    ON transactions(normalized_description)
    WHERE normalized_description IS NOT NULL;
