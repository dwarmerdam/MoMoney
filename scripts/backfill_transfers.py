#!/usr/bin/env python3
"""One-off script to link unlinked transfer pairs."""
import sqlite3
import uuid

conn = sqlite3.connect('/app/data/finance.db')
c = conn.cursor()

# Find unlinked transfers
c.execute("""
    SELECT id, account_id, date, amount, raw_description
    FROM transactions
    WHERE raw_description LIKE 'Transfer%'
    AND id NOT IN (SELECT from_transaction_id FROM transfers)
    AND id NOT IN (SELECT to_transaction_id FROM transfers)
""")
unlinked = c.fetchall()
print(f"Unlinked: {len(unlinked)}")

linked = 0
for txn_id, acct, date, amount, desc in unlinked:
    c.execute("""
        SELECT id FROM transactions
        WHERE account_id != ?
        AND ABS(amount + ?) < 0.01
        AND ABS(JULIANDAY(date) - JULIANDAY(?)) <= 5
        AND raw_description LIKE 'Transfer%%'
        AND id NOT IN (SELECT from_transaction_id FROM transfers)
        AND id NOT IN (SELECT to_transaction_id FROM transfers)
        ORDER BY ABS(JULIANDAY(date) - JULIANDAY(?))
        LIMIT 1
    """, (acct, amount, date, date))
    match = c.fetchone()
    if match:
        from_id, to_id = (txn_id, match[0]) if amount < 0 else (match[0], txn_id)
        c.execute("""
            INSERT INTO transfers (id, from_transaction_id, to_transaction_id,
                                   transfer_type, match_method, confidence)
            VALUES (?, ?, ?, 'internal-transfer', 'backfill_script', 0.9)
        """, (str(uuid.uuid4()), from_id, to_id))
        linked += 1

conn.commit()
print(f"Linked: {linked}")

c.execute("SELECT count(*) FROM transfers")
print(f"Total transfers: {c.fetchone()[0]}")
conn.close()
