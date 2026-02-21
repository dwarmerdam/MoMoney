"""Repository: CRUD operations against SQLite using raw SQL.

All methods take/return dataclass instances from models.py.
Connection management uses a single connection with WAL mode and
foreign keys enabled.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import (
    Allocation,
    ApiUsage,
    Import,
    ReceiptMatch,
    Transaction,
    Transfer,
)


class DuplicateImportError(Exception):
    """Raised when attempting to import a file with a hash that already exists."""

    def __init__(self, file_hash: str, existing_import_id: str | None = None):
        self.file_hash = file_hash
        self.existing_import_id = existing_import_id
        super().__init__(f"Import with file_hash '{file_hash}' already exists")


class Repository:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute("PRAGMA journal_mode = WAL")
        return self._conn

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ── Migrations ──────────────────────────────────────────

    def apply_migrations(self, migrations_dir: Path):
        """Apply all pending SQL migrations in order.

        Each migration runs in a transaction: if the SQL fails, the
        schema_version row is not inserted, allowing retry on next startup.
        """
        # Ensure schema_version exists for first run
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version ("
            "  version INTEGER PRIMARY KEY,"
            "  description TEXT,"
            "  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ")"
        )
        self.conn.commit()

        row = self.conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        current = row[0] or 0

        for sql_file in sorted(migrations_dir.glob("*.sql")):
            version = int(sql_file.name.split("_")[0])
            if version > current:
                try:
                    # Run migration in a transaction
                    self.conn.execute("BEGIN")
                    # executescript auto-commits, so we split statements manually
                    sql_text = sql_file.read_text()
                    for statement in sql_text.split(";"):
                        statement = statement.strip()
                        if statement:
                            self.conn.execute(statement)
                    self.conn.execute(
                        "INSERT INTO schema_version (version, description) VALUES (?, ?)",
                        (version, sql_file.stem),
                    )
                    self.conn.commit()
                except Exception:
                    self.conn.rollback()
                    raise

    # ── Imports ─────────────────────────────────────────────

    def insert_import(self, imp: Import) -> Import:
        """Insert an import record.

        Raises:
            DuplicateImportError: If a file with the same hash was already imported.
                This handles race conditions where two processes try to import
                the same file simultaneously.
        """
        try:
            self.conn.execute(
                "INSERT INTO imports (id, file_name, file_hash, file_size, account_id,"
                " record_count, status, error_message, created_at, completed_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (imp.id, imp.file_name, imp.file_hash, imp.file_size,
                 imp.account_id, imp.record_count, imp.status,
                 imp.error_message, imp.created_at, imp.completed_at),
            )
            self.conn.commit()
            return imp
        except sqlite3.IntegrityError as e:
            # Check if it's a duplicate file_hash (unique constraint violation)
            if "file_hash" in str(e) or "UNIQUE constraint failed" in str(e):
                existing = self.get_import_by_hash(imp.file_hash)
                raise DuplicateImportError(
                    imp.file_hash,
                    existing.id if existing else None,
                ) from e
            raise

    def get_import_by_hash(self, file_hash: str) -> Import | None:
        row = self.conn.execute(
            "SELECT * FROM imports WHERE file_hash = ?", (file_hash,)
        ).fetchone()
        return self._row_to_import(row) if row else None

    _IMPORT_UPDATE_COLS = frozenset({"record_count", "error_message", "completed_at"})

    def update_import_status(
        self, import_id: str, status: str, **kwargs
    ):
        # Validate kwargs - reject unknown column names to prevent silent bugs
        unknown = set(kwargs.keys()) - self._IMPORT_UPDATE_COLS
        if unknown:
            raise ValueError(f"Unknown columns for update_import_status: {unknown}")

        sets = ["status = ?"]
        vals: list = [status]
        for col in ("record_count", "error_message", "completed_at"):
            if col in kwargs:
                sets.append(f"{col} = ?")
                vals.append(kwargs[col])
        vals.append(import_id)
        self.conn.execute(
            f"UPDATE imports SET {', '.join(sets)} WHERE id = ?", vals
        )
        self.conn.commit()

    # ── Transactions ────────────────────────────────────────

    def insert_transaction(self, txn: Transaction) -> Transaction:
        self.conn.execute(
            "INSERT INTO transactions"
            " (id, account_id, date, amount, raw_description,"
            "  normalized_description, memo, txn_type, check_num, balance,"
            "  external_id, import_id, import_hash, dedup_key, source,"
            "  status, confidence, categorization_method, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (txn.id, txn.account_id, txn.date, txn.amount,
             txn.raw_description, txn.normalized_description,
             txn.memo, txn.txn_type, txn.check_num, txn.balance,
             txn.external_id, txn.import_id, txn.import_hash,
             txn.dedup_key, txn.source, txn.status,
             txn.confidence, txn.categorization_method,
             txn.created_at, txn.updated_at),
        )
        self.conn.commit()
        return txn

    def insert_transactions_batch(self, txns: list[Transaction]):
        """Insert multiple transactions atomically.

        Uses a transaction wrapper so either all inserts succeed or none do.
        """
        try:
            self.conn.execute("BEGIN")
            self.conn.executemany(
                "INSERT INTO transactions"
                " (id, account_id, date, amount, raw_description,"
                "  normalized_description, memo, txn_type, check_num, balance,"
                "  external_id, import_id, import_hash, dedup_key, source,"
                "  status, confidence, categorization_method, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (t.id, t.account_id, t.date, t.amount,
                     t.raw_description, t.normalized_description,
                     t.memo, t.txn_type, t.check_num, t.balance,
                     t.external_id, t.import_id, t.import_hash,
                     t.dedup_key, t.source, t.status,
                     t.confidence, t.categorization_method,
                     t.created_at, t.updated_at)
                    for t in txns
                ],
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def get_transaction(self, txn_id: str) -> Transaction | None:
        row = self.conn.execute(
            "SELECT * FROM transactions WHERE id = ?", (txn_id,)
        ).fetchone()
        return self._row_to_transaction(row) if row else None

    def get_transaction_by_external_id(
        self, account_id: str, external_id: str
    ) -> Transaction | None:
        row = self.conn.execute(
            "SELECT * FROM transactions"
            " WHERE account_id = ? AND external_id = ?",
            (account_id, external_id),
        ).fetchone()
        return self._row_to_transaction(row) if row else None

    def get_transactions_by_import_id(
        self, import_id: str
    ) -> list[Transaction]:
        rows = self.conn.execute(
            "SELECT * FROM transactions WHERE import_id = ?"
            " ORDER BY date, rowid",
            (import_id,),
        ).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    def get_transactions_by_import_hash(
        self, import_hash: str
    ) -> list[Transaction]:
        rows = self.conn.execute(
            "SELECT * FROM transactions WHERE import_hash = ?",
            (import_hash,),
        ).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    def get_transactions_by_dedup_key(
        self, dedup_key: str
    ) -> list[Transaction]:
        rows = self.conn.execute(
            "SELECT * FROM transactions WHERE dedup_key = ?",
            (dedup_key,),
        ).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    def get_transactions_by_account_and_date(
        self, account_id: str, date: str
    ) -> list[Transaction]:
        rows = self.conn.execute(
            "SELECT * FROM transactions WHERE account_id = ? AND date = ?",
            (account_id, date),
        ).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    def get_uncategorized_transactions(
        self, limit: int = 100
    ) -> list[Transaction]:
        rows = self.conn.execute(
            "SELECT * FROM transactions WHERE status = 'pending'"
            " ORDER BY date DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    _SENTINEL = object()

    def update_transaction_status(
        self, txn_id: str, status: str,
        confidence: float | None = _SENTINEL,
        method: str | None = _SENTINEL,
    ):
        sets = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
        vals: list = [status]
        if confidence is not self._SENTINEL:
            sets.append("confidence = ?")
            vals.append(confidence)
        if method is not self._SENTINEL:
            sets.append("categorization_method = ?")
            vals.append(method)
        vals.append(txn_id)
        self.conn.execute(
            f"UPDATE transactions SET {', '.join(sets)} WHERE id = ?",
            vals,
        )
        self.conn.commit()

    def set_receipt_lookup_status(self, txn_id: str, status: str) -> None:
        """Set the receipt_lookup_status for a transaction."""
        self.conn.execute(
            "UPDATE transactions SET receipt_lookup_status = ?,"
            " updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, txn_id),
        )
        self.conn.commit()

    def get_transactions_for_account(
        self, account_id: str, date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[Transaction]:
        sql = "SELECT * FROM transactions WHERE account_id = ?"
        params: list = [account_id]
        if date_from:
            sql += " AND date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND date <= ?"
            params.append(date_to)
        sql += " ORDER BY date, rowid"
        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    # ── Allocations ─────────────────────────────────────────

    def insert_allocation(self, alloc: Allocation) -> Allocation:
        self.conn.execute(
            "INSERT INTO allocations"
            " (id, transaction_id, category_id, amount, memo, tags,"
            "  source, confidence, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (alloc.id, alloc.transaction_id, alloc.category_id,
             alloc.amount, alloc.memo, alloc.tags, alloc.source,
             alloc.confidence, alloc.created_at),
        )
        self.conn.commit()
        return alloc

    def insert_allocations_batch(self, allocs: list[Allocation]):
        """Insert multiple allocations atomically.

        Uses a transaction wrapper so either all inserts succeed or none do.
        """
        try:
            self.conn.execute("BEGIN")
            self.conn.executemany(
                "INSERT INTO allocations"
                " (id, transaction_id, category_id, amount, memo, tags,"
                "  source, confidence, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (a.id, a.transaction_id, a.category_id, a.amount,
                     a.memo, a.tags, a.source, a.confidence, a.created_at)
                    for a in allocs
                ],
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def get_allocations_by_transaction(
        self, txn_id: str
    ) -> list[Allocation]:
        rows = self.conn.execute(
            "SELECT * FROM allocations WHERE transaction_id = ?",
            (txn_id,),
        ).fetchall()
        return [self._row_to_allocation(r) for r in rows]

    def get_allocations_by_transaction_ids(
        self, txn_ids: list[str]
    ) -> dict[str, list[Allocation]]:
        """Fetch allocations for multiple transactions in one query.

        Returns a dict mapping transaction_id to its list of Allocations.
        Chunked to stay within SQLite's variable limit.
        """
        if not txn_ids:
            return {}
        result: dict[str, list[Allocation]] = {}
        chunk_size = 500
        for i in range(0, len(txn_ids), chunk_size):
            chunk = txn_ids[i : i + chunk_size]
            ph = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT * FROM allocations WHERE transaction_id IN ({ph})",
                chunk,
            ).fetchall()
            for r in rows:
                alloc = self._row_to_allocation(r)
                result.setdefault(alloc.transaction_id, []).append(alloc)
        return result

    # ── Transfers ───────────────────────────────────────────

    def insert_transfer(self, xfer: Transfer) -> Transfer:
        self.conn.execute(
            "INSERT INTO transfers"
            " (id, from_transaction_id, to_transaction_id,"
            "  transfer_type, match_method, confidence, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (xfer.id, xfer.from_transaction_id, xfer.to_transaction_id,
             xfer.transfer_type, xfer.match_method,
             xfer.confidence, xfer.created_at),
        )
        self.conn.commit()
        return xfer

    def get_transfer_by_transaction(
        self, txn_id: str
    ) -> Transfer | None:
        row = self.conn.execute(
            "SELECT * FROM transfers"
            " WHERE from_transaction_id = ? OR to_transaction_id = ?",
            (txn_id, txn_id),
        ).fetchone()
        return self._row_to_transfer(row) if row else None

    def find_transfer_candidates(
        self, txn: Transaction, days: int = 5
    ) -> list[Transaction]:
        """Find potential transfer counterparts: opposite amount, different account, within N days."""
        rows = self.conn.execute(
            "SELECT * FROM transactions"
            " WHERE account_id != ?"
            "   AND ABS(amount + ?) < 0.01"
            "   AND ABS(JULIANDAY(date) - JULIANDAY(?)) <= ?"
            "   AND id NOT IN ("
            "     SELECT from_transaction_id FROM transfers"
            "     UNION SELECT to_transaction_id FROM transfers"
            "   )"
            " ORDER BY ABS(JULIANDAY(date) - JULIANDAY(?)) ASC",
            (txn.account_id, txn.amount, txn.date, days, txn.date),
        ).fetchall()
        return [self._row_to_transaction(r) for r in rows]

    # ── Receipt Matches ─────────────────────────────────────

    def insert_receipt_match(self, match: ReceiptMatch) -> ReceiptMatch:
        self.conn.execute(
            "INSERT INTO receipt_matches"
            " (id, transaction_id, gmail_message_id, match_type,"
            "  matched_items, confidence, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (match.id, match.transaction_id, match.gmail_message_id,
             match.match_type, match.matched_items,
             match.confidence, match.created_at),
        )
        self.conn.commit()
        return match

    # ── API Usage ───────────────────────────────────────────

    def increment_api_usage(
        self, month: str, service: str,
        requests: int = 1,
        tokens_in: int = 0, tokens_out: int = 0,
        cost_cents: int = 0,
    ):
        """Upsert api_usage row: increment counters for month+service."""
        self.conn.execute(
            "INSERT INTO api_usage"
            " (id, month, service, request_count, input_tokens,"
            "  output_tokens, estimated_cost_cents, updated_at)"
            " VALUES (hex(randomblob(16)), ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"
            " ON CONFLICT(month, service) DO UPDATE SET"
            "  request_count = request_count + excluded.request_count,"
            "  input_tokens = input_tokens + excluded.input_tokens,"
            "  output_tokens = output_tokens + excluded.output_tokens,"
            "  estimated_cost_cents = estimated_cost_cents + excluded.estimated_cost_cents,"
            "  updated_at = CURRENT_TIMESTAMP",
            (month, service, requests, tokens_in, tokens_out, cost_cents),
        )
        self.conn.commit()

    def get_monthly_cost(self, month: str) -> int:
        """Total estimated cost in cents for a given month."""
        row = self.conn.execute(
            "SELECT COALESCE(SUM(estimated_cost_cents), 0) FROM api_usage"
            " WHERE month = ?",
            (month,),
        ).fetchone()
        return row[0]

    # ── Historical Queries ────────────────────────────────────

    def get_historical_category_counts(
        self, normalized_description: str
    ) -> list[dict]:
        """Get category assignment counts for past transactions with the same normalized_description.

        Returns a list of dicts with keys: category_id, amount, cnt, user_cnt.
        Only considers transactions with status='categorized'.
        """
        rows = self.conn.execute(
            "SELECT a.category_id, t.amount,"
            "  COUNT(*) AS cnt,"
            "  SUM(CASE WHEN a.source = 'user' THEN 1 ELSE 0 END) AS user_cnt"
            " FROM transactions t"
            " JOIN allocations a ON a.transaction_id = t.id"
            " WHERE t.normalized_description = ?"
            "   AND t.status = 'categorized'"
            " GROUP BY a.category_id, t.amount",
            (normalized_description,),
        ).fetchall()
        return [
            {
                "category_id": r["category_id"],
                "amount": r["amount"],
                "cnt": r["cnt"],
                "user_cnt": r["user_cnt"],
            }
            for r in rows
        ]

    # ── Row Converters ──────────────────────────────────────

    @staticmethod
    def _row_to_import(row: sqlite3.Row) -> Import:
        return Import(
            id=row["id"], file_name=row["file_name"],
            file_hash=row["file_hash"], file_size=row["file_size"],
            account_id=row["account_id"],
            record_count=row["record_count"], status=row["status"],
            error_message=row["error_message"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )

    @staticmethod
    def _row_to_transaction(row: sqlite3.Row) -> Transaction:
        return Transaction(
            id=row["id"], account_id=row["account_id"],
            date=row["date"], amount=row["amount"],
            raw_description=row["raw_description"],
            normalized_description=row["normalized_description"],
            memo=row["memo"], txn_type=row["txn_type"],
            check_num=row["check_num"], balance=row["balance"],
            external_id=row["external_id"],
            import_id=row["import_id"],
            import_hash=row["import_hash"],
            dedup_key=row["dedup_key"], source=row["source"],
            status=row["status"], confidence=row["confidence"],
            categorization_method=row["categorization_method"],
            receipt_lookup_status=row["receipt_lookup_status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_allocation(row: sqlite3.Row) -> Allocation:
        return Allocation(
            id=row["id"], transaction_id=row["transaction_id"],
            category_id=row["category_id"], amount=row["amount"],
            memo=row["memo"], tags=row["tags"],
            source=row["source"], confidence=row["confidence"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_transfer(row: sqlite3.Row) -> Transfer:
        return Transfer(
            id=row["id"],
            from_transaction_id=row["from_transaction_id"],
            to_transaction_id=row["to_transaction_id"],
            transfer_type=row["transfer_type"],
            match_method=row["match_method"],
            confidence=row["confidence"],
            created_at=row["created_at"],
        )
