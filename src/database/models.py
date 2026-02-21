"""Dataclass models matching the SQLite schema.

Each dataclass corresponds to one table. Fields match column names exactly.
All primary keys are TEXT (UUID strings generated via uuid4()).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4


def _new_id() -> str:
    return str(uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Import:
    file_name: str
    file_hash: str
    id: str = field(default_factory=_new_id)
    file_size: int | None = None
    account_id: str | None = None
    record_count: int | None = None
    status: str = "pending"
    error_message: str | None = None
    created_at: str = field(default_factory=_now)
    completed_at: str | None = None


@dataclass
class Transaction:
    account_id: str
    date: str
    amount: float
    raw_description: str
    import_id: str
    import_hash: str
    dedup_key: str
    id: str = field(default_factory=_new_id)
    normalized_description: str | None = None
    memo: str | None = None
    txn_type: str | None = None
    check_num: str | None = None
    balance: float | None = None
    external_id: str | None = None
    source: str = "bank"
    status: str = "pending"
    confidence: float | None = None
    categorization_method: str | None = None
    receipt_lookup_status: str | None = None
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass
class Allocation:
    transaction_id: str
    category_id: str
    amount: float
    id: str = field(default_factory=_new_id)
    memo: str | None = None
    tags: str | None = None
    source: str = "auto"
    confidence: float | None = None
    created_at: str = field(default_factory=_now)


@dataclass
class Transfer:
    from_transaction_id: str
    to_transaction_id: str
    transfer_type: str
    match_method: str
    id: str = field(default_factory=_new_id)
    confidence: float | None = None
    created_at: str = field(default_factory=_now)


@dataclass
class ReceiptMatch:
    transaction_id: str
    gmail_message_id: str
    match_type: str
    id: str = field(default_factory=_new_id)
    matched_items: str | None = None
    confidence: float | None = None
    created_at: str = field(default_factory=_now)


@dataclass
class ApiUsage:
    month: str
    service: str
    id: str = field(default_factory=_new_id)
    request_count: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_cents: int = 0
    updated_at: str = field(default_factory=_now)
