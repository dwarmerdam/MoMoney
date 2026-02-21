"""Complex queries that span multiple tables.

These go beyond single-table CRUD and implement derived fields,
aggregations, and reporting queries.
"""

from __future__ import annotations

import sqlite3


def is_transfer(conn: sqlite3.Connection, txn_id: str) -> bool:
    """Derive transfer status via EXISTS (no denormalized flag — S1)."""
    row = conn.execute(
        "SELECT EXISTS("
        "  SELECT 1 FROM transfers"
        "  WHERE from_transaction_id = ? OR to_transaction_id = ?"
        ")",
        (txn_id, txn_id),
    ).fetchone()
    return bool(row[0])


def batch_is_transfer(conn: sqlite3.Connection, txn_ids: list[str]) -> set[str]:
    """Return the subset of txn_ids that are part of a transfer.

    Chunked to stay within SQLite's variable limit.
    """
    if not txn_ids:
        return set()
    result: set[str] = set()
    chunk_size = 500
    for i in range(0, len(txn_ids), chunk_size):
        chunk = txn_ids[i : i + chunk_size]
        ph = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT from_transaction_id FROM transfers"
            f" WHERE from_transaction_id IN ({ph})"
            f" UNION"
            f" SELECT to_transaction_id FROM transfers"
            f" WHERE to_transaction_id IN ({ph})",
            chunk + chunk,
        ).fetchall()
        result.update(r[0] for r in rows)
    return result


def get_transactions_with_transfer_flag(
    conn: sqlite3.Connection,
    account_id: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Fetch transactions with is_transfer derived at query time."""
    sql = (
        "SELECT t.*,"
        "  EXISTS(SELECT 1 FROM transfers tr"
        "    WHERE tr.from_transaction_id = t.id"
        "       OR tr.to_transaction_id = t.id"
        "  ) AS is_transfer"
        " FROM transactions t"
        " WHERE t.account_id = ?"
    )
    params: list = [account_id]
    if date_from:
        sql += " AND t.date >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND t.date <= ?"
        params.append(date_to)
    sql += " ORDER BY t.date, t.rowid"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_category_summary(
    conn: sqlite3.Connection, month: str
) -> list[dict]:
    """Monthly spending by category. month format: 'YYYY-MM'."""
    rows = conn.execute(
        "SELECT a.category_id,"
        "  SUM(a.amount) AS total,"
        "  COUNT(*) AS txn_count"
        " FROM allocations a"
        " JOIN transactions t ON a.transaction_id = t.id"
        " WHERE t.date LIKE ? || '%'"
        " GROUP BY a.category_id"
        " ORDER BY total ASC",
        (month,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_reconciliation_data(
    conn: sqlite3.Connection, account_id: str
) -> dict:
    """Balance reconciliation: compare computed sum against last statement balance.

    Excludes transfers from the sum because internal transfers don't affect
    the actual account balance (they're just moves between tracked accounts).
    """
    last_with_balance = conn.execute(
        "SELECT date, balance FROM transactions"
        " WHERE account_id = ? AND balance IS NOT NULL"
        " ORDER BY date DESC, rowid DESC LIMIT 1",
        (account_id,),
    ).fetchone()

    if not last_with_balance:
        return {"status": "no_balance_data"}

    balance_date = last_with_balance["date"]
    statement_balance = last_with_balance["balance"]

    # Exclude transactions that are part of a transfer link
    # Transfers are internal moves that don't affect the real account balance
    computed = conn.execute(
        "SELECT COALESCE(SUM(t.amount), 0) FROM transactions t"
        " WHERE t.account_id = ? AND t.date <= ?"
        "   AND NOT EXISTS ("
        "     SELECT 1 FROM transfers tr"
        "     WHERE tr.from_transaction_id = t.id"
        "        OR tr.to_transaction_id = t.id"
        "   )",
        (account_id, balance_date),
    ).fetchone()[0]

    difference = abs(computed - statement_balance)
    return {
        "status": "balanced" if difference < 0.01 else "discrepancy",
        "date": balance_date,
        "statement_balance": statement_balance,
        "computed_balance": computed,
        "difference": difference,
    }


def get_status_counts(conn: sqlite3.Connection) -> dict:
    """Counts for the `momoney status` command."""
    row = conn.execute(
        "SELECT"
        "  (SELECT COUNT(*) FROM transactions) AS total_txns,"
        "  (SELECT COUNT(*) FROM transactions WHERE status = 'pending') AS pending,"
        "  (SELECT COUNT(*) FROM transactions WHERE status = 'flagged') AS flagged,"
        "  (SELECT COUNT(*) FROM transactions WHERE status = 'categorized') AS categorized,"
        "  (SELECT COUNT(*) FROM imports) AS total_imports,"
        "  (SELECT COUNT(*) FROM transfers) AS total_transfers"
    ).fetchone()
    return dict(row)
