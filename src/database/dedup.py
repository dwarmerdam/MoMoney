"""6-tier deduplication engine for transaction imports.

Tiers (evaluated in order, first match wins):
1. File hash — SHA256 of entire file rejects whole-file re-imports
2. External ID — FITID (QFX) or Timestamp (Mercury) per account
3. Import hash — SHA256(account|date|amount|raw_desc) exact content match
3.5. Cross-format — dedup_key match where sources differ (bank vs budget app)
3.6. Split-sum — bank total matches sum of budget app splits (or vice versa)
4. Fuzzy dedup — same account+date+amount with description similarity

Tier 3.5 catches cross-format duplicates: budget app imports use user-edited
payee names while QFX uses bank-provided names, so descriptions don't match
but the dedup_key (account+date+amount) does. Count-based matching
handles legitimate same-day same-amount transactions.

Tier 3.6 catches split-sum cross-format duplicates: a budget app may split a
single bank charge into multiple line items (e.g., bank -116.98 becomes
-99.99 + -16.99). The amounts differ individually but the splits
sum to the bank total. Only matches opposite-source transactions with
related descriptions on the same account+date.

Tier 4 flags for review rather than auto-rejecting, since legitimate
same-amount same-day transactions exist (e.g., 4x $25 Amazon charges
with different timestamps).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path

from src.database.models import Import, Transaction
from src.database.repository import Repository
from src.parsers.base import (
    RawTransaction,
    compute_dedup_key,
    compute_file_hash,
    compute_import_hash,
    normalize_description,
)


@dataclass
class DedupResult:
    """Outcome of dedup check for a single transaction."""
    status: str  # "new", "duplicate", "flagged"
    tier: str | None = None  # "file", "external_id", "import_hash", "cross_format", "split_sum", "fuzzy"
    matched_txn: Transaction | None = None
    confidence: float | None = None
    matched_splits: list[Transaction] | None = None


class DedupEngine:
    """Run 4-tier deduplication against the repository."""

    FUZZY_THRESHOLD: float = 0.85

    def __init__(self, repo: Repository):
        self.repo = repo

    # ── Tier 1: File hash ─────────────────────────────────

    def check_file_duplicate(self, file_path: Path) -> bool:
        """Return True if this exact file has already been imported."""
        file_hash = compute_file_hash(file_path)
        existing = self.repo.get_import_by_hash(file_hash)
        return existing is not None

    # ── Tier 2: External ID ───────────────────────────────

    def check_external_id(
        self, account_id: str, external_id: str | None
    ) -> Transaction | None:
        """Return existing transaction if same account+external_id exists."""
        if not external_id:
            return None
        return self.repo.get_transaction_by_external_id(account_id, external_id)

    # ── Tier 3: Import hash ───────────────────────────────

    def check_import_hash(
        self, import_hash: str, external_id: str | None = None
    ) -> Transaction | None:
        """Return existing transaction with the same import hash.

        If the incoming transaction has an external_id, only match against
        transactions that share the same external_id or have no external_id.
        This prevents false positives when legitimately different transactions
        share the same (account, date, amount, description) but have distinct
        external_ids (e.g., 4x $25 Amazon charges with different timestamps).
        """
        matches = self.repo.get_transactions_by_import_hash(import_hash)
        if not matches:
            return None
        if external_id:
            for m in matches:
                if m.external_id is None or m.external_id == external_id:
                    return m
            return None
        return matches[0]

    # ── Tier 3.5: Cross-format dedup ─────────────────────

    def check_cross_format_duplicate(
        self,
        dedup_key: str,
        has_external_id: bool,
    ) -> list[Transaction]:
        """Find cross-format matches for a dedup_key.

        Bank transactions have external_id; budget app transactions don't.
        Returns existing transactions of the *opposite* type:
        - If importing bank (has_external_id=True), returns budget app matches (no external_id)
        - If importing budget app (has_external_id=False), returns bank matches (has external_id)
        """
        candidates = self.repo.get_transactions_by_dedup_key(dedup_key)
        if not candidates:
            return []
        matches = []
        for c in candidates:
            if has_external_id and c.external_id is None:
                matches.append(c)
            elif not has_external_id and c.external_id is not None:
                matches.append(c)
        return matches

    # ── Tier 3.6: Split-sum cross-format dedup ──────────

    def check_split_sum_duplicate(
        self,
        account_id: str,
        date: str,
        amount: float,
        raw_description: str,
        has_external_id: bool,
    ) -> list[Transaction] | None:
        """Check if opposite-source splits on the same date sum to this amount.

        Returns the list of matching split transactions, or None.
        """
        candidates = self.repo.get_transactions_by_account_and_date(account_id, date)
        if not candidates:
            return None
        # Filter to opposite source type
        opposite = []
        for c in candidates:
            if has_external_id and c.external_id is None:
                opposite.append(c)
            elif not has_external_id and c.external_id is not None:
                opposite.append(c)
        if len(opposite) < 2:
            return None
        # Filter by related descriptions
        related = [c for c in opposite if _descriptions_related(raw_description, c.raw_description)]
        if len(related) < 2:
            return None
        # Try subset sum
        amounts = [c.amount for c in related]
        indices = _find_subset_sum(amounts, amount)
        if indices is not None:
            return [related[i] for i in indices]
        return None

    # ── Tier 4: Fuzzy dedup ───────────────────────────────

    def check_fuzzy_duplicate(
        self,
        dedup_key: str,
        raw_description: str,
        threshold: float | None = None,
    ) -> DedupResult:
        """Check for same account+date+amount with similar descriptions.

        Returns DedupResult with status "flagged" if a sufficiently similar
        existing transaction is found, "new" otherwise.
        """
        if threshold is None:
            threshold = self.FUZZY_THRESHOLD

        candidates = self.repo.get_transactions_by_dedup_key(dedup_key)
        if not candidates:
            return DedupResult(status="new")

        norm_desc = normalize_description(raw_description)
        best_match: Transaction | None = None
        best_score: float = 0.0

        for candidate in candidates:
            existing_desc = candidate.normalized_description or normalize_description(
                candidate.raw_description
            )
            score = description_similarity(norm_desc, existing_desc)
            if score > best_score:
                best_score = score
                best_match = candidate

        if best_score >= threshold:
            return DedupResult(
                status="flagged",
                tier="fuzzy",
                matched_txn=best_match,
                confidence=best_score,
            )
        return DedupResult(status="new")

    # ── Full pipeline ─────────────────────────────────────

    def deduplicate(self, raw: RawTransaction, import_id: str) -> DedupResult:
        """Run all 4 tiers on a single RawTransaction.

        Tier 1 (file hash) is checked separately before calling this.
        This method covers tiers 2-4.

        Note: import_id is accepted for API compatibility but not used here.
        The actual import_id assignment happens in process_batch().
        """
        _ = import_id  # Explicitly mark as intentionally unused
        # Tier 2: external ID
        existing = self.check_external_id(raw.account_id, raw.external_id)
        if existing is not None:
            return DedupResult(
                status="duplicate", tier="external_id", matched_txn=existing
            )

        # Tier 3: import hash
        import_hash = compute_import_hash(
            raw.account_id, raw.date, raw.amount, raw.raw_description
        )
        existing = self.check_import_hash(import_hash, raw.external_id)
        if existing is not None:
            return DedupResult(
                status="duplicate", tier="import_hash", matched_txn=existing
            )

        # Tier 3.5: cross-format dedup
        # Budget app imports use user-edited payee names (e.g., "Amazon") while QFX uses
        # bank names (e.g., "AMAZON.COM*MB2KR7PH0"). Descriptions differ but
        # dedup_key (account+date+amount) matches. Count-based matching is
        # handled in process_batch via _cross_format_used.
        dedup_key = compute_dedup_key(raw.account_id, raw.date, raw.amount)
        cross_matches = self.check_cross_format_duplicate(
            dedup_key, bool(raw.external_id)
        )
        if cross_matches:
            return DedupResult(
                status="duplicate",
                tier="cross_format",
                matched_txn=cross_matches[0],
            )

        # Tier 3.6: split-sum cross-format dedup
        # Check if opposite-source splits on the same date sum to this amount.
        split_matches = self.check_split_sum_duplicate(
            raw.account_id, raw.date, raw.amount,
            raw.raw_description, bool(raw.external_id),
        )
        if split_matches:
            return DedupResult(
                status="duplicate",
                tier="split_sum",
                matched_txn=split_matches[0],
                matched_splits=split_matches,
            )

        # Tier 4: fuzzy dedup
        # Skip fuzzy check if we have a unique external_id — the transaction
        # already passed tier 2, meaning its external_id is genuinely new.
        # This prevents flagging legitimate same-amount same-day charges
        # (e.g., 4x $25 Amazon on Mercury with different timestamps).
        if not raw.external_id:
            fuzzy_result = self.check_fuzzy_duplicate(
                dedup_key, raw.raw_description
            )
            if fuzzy_result.status == "flagged":
                return fuzzy_result

        return DedupResult(status="new")

    # ── Batch processing ──────────────────────────────────

    def process_batch(
        self,
        raw_txns: list[RawTransaction],
        import_id: str,
        source: str = "bank",
    ) -> BatchResult:
        """Deduplicate and insert a batch of parsed transactions.

        Returns counts of new, duplicate, and flagged transactions.
        New transactions are inserted into the database.
        Flagged transactions are inserted with status="flagged".
        """
        new_txns: list[Transaction] = []
        duplicates: list[DedupResult] = []
        flagged: list[Transaction] = []
        # Track identifiers seen within this batch to catch intra-batch dupes
        seen_external_ids: dict[tuple[str, str], bool] = {}  # (account_id, ext_id)
        seen_import_hashes: set[str] = set()
        # Track cross-format dedup key usage for count-based matching.
        # Key: dedup_key, Value: number of cross-format matches already consumed.
        # This ensures that if there are N bank txns and M budget app txns with the
        # same key, only min(N,M) are deduped and the rest import normally.
        cross_format_used: dict[str, int] = {}
        # Track transaction IDs consumed by split-sum matches to prevent
        # the same set of splits from being matched by multiple incoming txns.
        split_sum_consumed: set[str] = set()

        for raw in raw_txns:
            # Intra-batch dedup: check if we've already seen this external_id
            if raw.external_id:
                batch_key = (raw.account_id, raw.external_id)
                if batch_key in seen_external_ids:
                    duplicates.append(DedupResult(
                        status="duplicate", tier="external_id",
                    ))
                    continue

            # Intra-batch dedup: check if we've already seen this import_hash
            import_hash_early = compute_import_hash(
                raw.account_id, raw.date, raw.amount, raw.raw_description
            )
            if import_hash_early in seen_import_hashes and not raw.external_id:
                duplicates.append(DedupResult(
                    status="duplicate", tier="import_hash",
                ))
                continue

            result = self.deduplicate(raw, import_id)

            # For cross-format matches, enforce count-based limits.
            # If there are N cross-format matches for a dedup_key, only
            # the first N incoming transactions with that key are deduped.
            if result.status == "duplicate" and result.tier == "cross_format":
                dk = compute_dedup_key(raw.account_id, raw.date, raw.amount)
                cross_matches = self.check_cross_format_duplicate(
                    dk, bool(raw.external_id)
                )
                used = cross_format_used.get(dk, 0)
                if used < len(cross_matches):
                    cross_format_used[dk] = used + 1
                    duplicates.append(result)
                    continue
                else:
                    # All cross-format slots consumed — this is a genuinely
                    # new transaction (e.g., 3rd budget app entry when only 2 bank
                    # entries exist). Fall through to insert as new.
                    result = DedupResult(status="new")

            # For split-sum matches, ensure the matched splits haven't
            # already been consumed by a previous incoming transaction.
            if result.status == "duplicate" and result.tier == "split_sum":
                matched_ids = {t.id for t in (result.matched_splits or [])}
                if not matched_ids & split_sum_consumed:
                    split_sum_consumed.update(matched_ids)
                    duplicates.append(result)
                    continue
                else:
                    result = DedupResult(status="new")

            if result.status == "duplicate":
                duplicates.append(result)
                continue

            import_hash = import_hash_early
            dedup_key = compute_dedup_key(
                raw.account_id, raw.date, raw.amount
            )
            norm_desc = normalize_description(raw.raw_description)

            # Record identifiers for intra-batch dedup of later items
            if raw.external_id:
                seen_external_ids[(raw.account_id, raw.external_id)] = True
            seen_import_hashes.add(import_hash)

            status = "flagged" if result.status == "flagged" else "pending"
            txn = Transaction(
                account_id=raw.account_id,
                date=raw.date,
                amount=raw.amount,
                raw_description=raw.raw_description,
                normalized_description=norm_desc,
                memo=raw.memo,
                txn_type=raw.txn_type,
                check_num=raw.check_num,
                balance=raw.balance,
                external_id=raw.external_id,
                import_id=import_id,
                import_hash=import_hash,
                dedup_key=dedup_key,
                source=source,
                status=status,
            )

            if status == "flagged":
                flagged.append(txn)
            else:
                new_txns.append(txn)

        # Insert all new + flagged in one batch
        all_insert = new_txns + flagged
        if all_insert:
            self.repo.insert_transactions_batch(all_insert)

        return BatchResult(
            new_count=len(new_txns),
            duplicate_count=len(duplicates),
            flagged_count=len(flagged),
            transactions=new_txns + flagged,
        )


@dataclass
class BatchResult:
    """Summary of a batch dedup + insert operation."""
    new_count: int
    duplicate_count: int
    flagged_count: int
    transactions: list[Transaction]


def description_similarity(a: str, b: str) -> float:
    """Compute similarity ratio between two normalized descriptions.

    Uses SequenceMatcher which handles transpositions and partial matches
    well for bank descriptions.
    """
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _descriptions_related(desc_a: str, desc_b: str) -> bool:
    """Return True if two descriptions are related.

    Checks containment and shared significant words (3+ chars).
    Handles "APPLE.COM/BILL" vs "APPLE" and
    "US PATENT TRADEMARK" vs "US PATENT AND TRADEMARK OFFICE".
    """
    a = normalize_description(desc_a).upper()
    b = normalize_description(desc_b).upper()
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    words_a = {w for w in a.split() if len(w) >= 3}
    words_b = {w for w in b.split() if len(w) >= 3}
    return bool(words_a & words_b)


def _find_subset_sum(
    amounts: list[float], target: float, tolerance: float = 0.01
) -> list[int] | None:
    """Find a subset (size >= 2) of amounts that sums to target within tolerance.

    Returns indices of matching subset, or None. Brute-force for small n (max 20).
    """
    n = len(amounts)
    if n < 2 or n > 20:
        return None
    for size in range(2, n + 1):
        for combo in itertools.combinations(range(n), size):
            total = sum(amounts[i] for i in combo)
            if abs(total - target) <= tolerance:
                return list(combo)
    return None
