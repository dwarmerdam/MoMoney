-- Migration 002: Add receipt_lookup_status to transactions
-- Tracks what happened during Gmail receipt lookup (Step 6):
--   NULL = not an Apple/Amazon candidate
--   "matched" = receipt matched successfully
--   "no_email" = Gmail search returned no results
--   "no_match" = emails found but amount didn't match
--   "error" = Gmail API error
--   "budget_exceeded" = Claude monthly budget hit

ALTER TABLE transactions ADD COLUMN receipt_lookup_status TEXT;
