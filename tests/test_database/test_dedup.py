"""Tests for the 6-tier deduplication engine."""

from pathlib import Path

import pytest

from src.database.dedup import (
    BatchResult,
    DedupEngine,
    DedupResult,
    _descriptions_related,
    _find_subset_sum,
    description_similarity,
)
from src.database.models import Import, Transaction
from src.database.repository import Repository
from src.parsers.base import (
    RawTransaction,
    compute_dedup_key,
    compute_file_hash,
    compute_import_hash,
)

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "src" / "database" / "migrations"


@pytest.fixture
def repo():
    r = Repository(":memory:")
    r.apply_migrations(MIGRATIONS_DIR)
    yield r
    r.close()


@pytest.fixture
def engine(repo):
    return DedupEngine(repo)


@pytest.fixture
def imp(repo):
    return repo.insert_import(Import(file_name="test.qfx", file_hash="filehash1"))


def _raw(**kw) -> RawTransaction:
    defaults = dict(
        date="2026-01-15", amount=-50.0,
        raw_description="PHILZ COFFEE SF",
        account_id="wf-checking",
    )
    defaults.update(kw)
    return RawTransaction(**defaults)


def _insert_txn(repo, imp_id, source="bank", **kw) -> Transaction:
    raw = _raw(**kw)
    ih = compute_import_hash(raw.account_id, raw.date, raw.amount, raw.raw_description)
    dk = compute_dedup_key(raw.account_id, raw.date, raw.amount)
    txn = Transaction(
        account_id=raw.account_id, date=raw.date, amount=raw.amount,
        raw_description=raw.raw_description,
        normalized_description=raw.raw_description.upper(),
        import_id=imp_id, import_hash=ih, dedup_key=dk,
        external_id=raw.external_id, memo=raw.memo,
        txn_type=raw.txn_type, check_num=raw.check_num,
        balance=raw.balance, source=source,
    )
    repo.insert_transaction(txn)
    return txn


# ── description_similarity ────────────────────────────────


class TestDescriptionSimilarity:
    def test_identical(self):
        assert description_similarity("PHILZ COFFEE", "PHILZ COFFEE") == 1.0

    def test_completely_different(self):
        assert description_similarity("PHILZ COFFEE", "AMAZON PURCHASE") < 0.5

    def test_both_empty(self):
        assert description_similarity("", "") == 1.0

    def test_one_empty(self):
        assert description_similarity("PHILZ", "") == 0.0
        assert description_similarity("", "PHILZ") == 0.0

    def test_similar_descriptions(self):
        score = description_similarity(
            "CAPITAL ONE MOBILE PMT", "CAPITAL ONE MOBILE PAYMENT"
        )
        assert score > 0.8

    def test_slight_difference(self):
        score = description_similarity(
            "WHOLEFDS MKT 10294", "WHOLEFDS MKT 10295"
        )
        assert score > 0.85  # Very similar, small digit difference


# ── Tier 1: File hash ─────────────────────────────────────


class TestTier1FileHash:
    def test_new_file(self, engine, tmp_path):
        f = tmp_path / "new.qfx"
        f.write_text("unique content 12345")
        assert engine.check_file_duplicate(f) is False

    def test_duplicate_file(self, engine, repo, tmp_path):
        f = tmp_path / "existing.qfx"
        f.write_text("known content abc")
        file_hash = compute_file_hash(f)
        repo.insert_import(Import(file_name="existing.qfx", file_hash=file_hash))
        assert engine.check_file_duplicate(f) is True

    def test_different_content_same_name(self, engine, repo, tmp_path):
        repo.insert_import(Import(file_name="data.qfx", file_hash="oldhash"))
        f = tmp_path / "data.qfx"
        f.write_text("different content")
        assert engine.check_file_duplicate(f) is False


# ── Tier 2: External ID ───────────────────────────────────


class TestTier2ExternalId:
    def test_no_external_id(self, engine):
        result = engine.check_external_id("wf-checking", None)
        assert result is None

    def test_new_external_id(self, engine):
        result = engine.check_external_id("wf-checking", "202601151")
        assert result is None

    def test_duplicate_external_id(self, engine, repo, imp):
        _insert_txn(repo, imp.id, external_id="202601151")
        result = engine.check_external_id("wf-checking", "202601151")
        assert result is not None
        assert result.external_id == "202601151"

    def test_same_id_different_account(self, engine, repo, imp):
        _insert_txn(repo, imp.id, external_id="202601151")
        result = engine.check_external_id("cap1-credit", "202601151")
        assert result is None

    def test_golden1_paired_entries(self, engine, repo, imp):
        """Golden1 payment and interest have different FITIDs — both are new."""
        _insert_txn(
            repo, imp.id, account_id="golden1-auto",
            external_id="66886_84", amount=-389.32,
            raw_description="ONLINE PAYMENT",
        )
        # Interest entry has different FITID (with INT suffix)
        result = engine.check_external_id("golden1-auto", "66886_84INT")
        assert result is None  # Not a duplicate


# ── Tier 3: Import hash ───────────────────────────────────


class TestTier3ImportHash:
    def test_new_hash(self, engine):
        result = engine.check_import_hash("nonexistent_hash")
        assert result is None

    def test_duplicate_hash(self, engine, repo, imp):
        _insert_txn(repo, imp.id)
        ih = compute_import_hash("wf-checking", "2026-01-15", -50.0, "PHILZ COFFEE SF")
        result = engine.check_import_hash(ih)
        assert result is not None
        assert result.raw_description == "PHILZ COFFEE SF"

    def test_different_amount_different_hash(self, engine, repo, imp):
        _insert_txn(repo, imp.id, amount=-50.0)
        ih = compute_import_hash("wf-checking", "2026-01-15", -50.01, "PHILZ COFFEE SF")
        result = engine.check_import_hash(ih)
        assert result is None


# ── Tier 4: Fuzzy dedup ───────────────────────────────────


class TestTier4FuzzyDedup:
    def test_no_candidates(self, engine):
        dk = compute_dedup_key("wf-checking", "2026-01-15", -50.0)
        result = engine.check_fuzzy_duplicate(dk, "PHILZ COFFEE")
        assert result.status == "new"

    def test_similar_description_flagged(self, engine, repo, imp):
        _insert_txn(repo, imp.id, raw_description="PHILZ COFFEE SF CA")
        dk = compute_dedup_key("wf-checking", "2026-01-15", -50.0)
        result = engine.check_fuzzy_duplicate(dk, "PHILZ COFFEE SF")
        assert result.status == "flagged"
        assert result.tier == "fuzzy"
        assert result.confidence >= 0.85

    def test_different_description_passes(self, engine, repo, imp):
        _insert_txn(repo, imp.id, raw_description="STARBUCKS RESERVE")
        dk = compute_dedup_key("wf-checking", "2026-01-15", -50.0)
        result = engine.check_fuzzy_duplicate(dk, "PHILZ COFFEE SF")
        assert result.status == "new"

    def test_custom_threshold(self, engine, repo, imp):
        _insert_txn(repo, imp.id, raw_description="PHILZ COFFEE SF CA")
        dk = compute_dedup_key("wf-checking", "2026-01-15", -50.0)
        # Very high threshold — not flagged
        result = engine.check_fuzzy_duplicate(dk, "PHILZ COFFEE SF", threshold=0.99)
        assert result.status == "new"

    def test_exact_same_description(self, engine, repo, imp):
        _insert_txn(repo, imp.id, raw_description="PHILZ COFFEE SF")
        dk = compute_dedup_key("wf-checking", "2026-01-15", -50.0)
        result = engine.check_fuzzy_duplicate(dk, "PHILZ COFFEE SF")
        assert result.status == "flagged"
        assert result.confidence == 1.0


# ── Full pipeline (deduplicate) ───────────────────────────


class TestDeduplicate:
    def test_new_transaction(self, engine, imp):
        raw = _raw()
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "new"
        assert result.tier is None

    def test_caught_at_tier2(self, engine, repo, imp):
        _insert_txn(repo, imp.id, external_id="FITID123")
        raw = _raw(external_id="FITID123")
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "duplicate"
        assert result.tier == "external_id"

    def test_caught_at_tier3(self, engine, repo, imp):
        """No external_id, but same content → caught at import_hash."""
        _insert_txn(repo, imp.id)
        raw = _raw()  # Same content, no external_id
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "duplicate"
        assert result.tier == "import_hash"

    def test_caught_at_tier4(self, engine, repo, imp):
        """Different description but similar, same amount+date, no external_id → flagged."""
        _insert_txn(
            repo, imp.id, raw_description="PHILZ COFFEE SAN FRANCISCO",
        )
        raw = _raw(
            raw_description="PHILZ COFFEE SAN FRAN",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "flagged"
        assert result.tier == "fuzzy"

    def test_tier2_skipped_when_no_external_id(self, engine, repo, imp):
        """Budget app transactions have no external_id — falls through to tier 3."""
        _insert_txn(repo, imp.id)
        raw = _raw()  # No external_id
        result = engine.deduplicate(raw, imp.id)
        # Caught at tier 3 (import_hash), not tier 2
        assert result.status == "duplicate"
        assert result.tier == "import_hash"

    def test_different_account_same_content_is_new(self, engine, repo, imp):
        _insert_txn(repo, imp.id, account_id="wf-checking", external_id="F1")
        raw = _raw(account_id="cap1-credit", external_id="F2")
        result = engine.deduplicate(raw, imp.id)
        # Different account → different import_hash → new
        assert result.status == "new"


# ── Special cases ─────────────────────────────────────────


class TestSpecialCases:
    def test_golden1_payment_and_interest(self, engine, repo, imp):
        """Both entries imported — different FITIDs and different amounts."""
        _insert_txn(
            repo, imp.id, account_id="golden1-auto",
            external_id="66886_84", amount=-389.32,
            raw_description="ONLINE PAYMENT", txn_type="CREDIT",
        )
        # Interest entry
        raw = _raw(
            account_id="golden1-auto",
            external_id="66886_84INT", amount=-55.17,
            raw_description="INTEREST", txn_type="INT",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "new"

    def test_mercury_four_amazon_charges(self, engine, repo, imp):
        """4x $25 Amazon charges with different timestamps all import."""
        timestamps = [
            "01-30-2026 01:43:22", "01-30-2026 02:02:15",
            "01-30-2026 02:08:44", "01-30-2026 02:10:04",
        ]
        # Insert first 3
        for ts in timestamps[:3]:
            _insert_txn(
                repo, imp.id, account_id="mercury-credit",
                amount=-25.0, raw_description="Amazon",
                external_id=ts, date="2026-01-30",
            )
        # 4th should still be new (different timestamp/external_id)
        raw = _raw(
            account_id="mercury-credit",
            amount=-25.0, raw_description="Amazon",
            external_id=timestamps[3], date="2026-01-30",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "new"

    def test_budget_app_no_external_id_new(self, engine, imp):
        """Budget app imports have no external_id, new transactions pass."""
        raw = _raw(
            raw_description="Costco Wholesale",
            amount=-150.00, date="2024-03-15",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "new"

    def test_budget_app_duplicate_content(self, engine, repo, imp):
        """Budget app re-import with same content caught at tier 3."""
        _insert_txn(
            repo, imp.id, raw_description="Costco Wholesale",
            amount=-150.00, date="2024-03-15",
        )
        raw = _raw(
            raw_description="Costco Wholesale",
            amount=-150.00, date="2024-03-15",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "duplicate"
        assert result.tier == "import_hash"

    def test_budget_app_after_bank_cross_format(self, engine, repo, imp):
        """Budget app import catches bank duplicate via cross-format tier."""
        # Bank transaction already in DB (has external_id)
        _insert_txn(
            repo, imp.id, raw_description="AMAZON.COM*MB2KR7PH0",
            amount=-45.09, date="2026-01-22", external_id="FITID_AMZ_1",
        )
        # Budget app has user-edited payee name — completely different description
        raw = _raw(
            raw_description="Amazon",
            amount=-45.09, date="2026-01-22",
            # Budget app: no external_id
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "duplicate"
        assert result.tier == "cross_format"

    def test_bank_after_budget_app_cross_format(self, engine, repo, imp):
        """Bank import catches budget app duplicate via cross-format tier."""
        # Budget app transaction already in DB (no external_id)
        _insert_txn(
            repo, imp.id, raw_description="Apple",
            amount=-29.99, date="2026-01-26",
        )
        # QFX bank transaction has FITID and bank-style description
        raw = _raw(
            raw_description="APL*APPLE.COM/BILL",
            amount=-29.99, date="2026-01-26",
            external_id="FITID_APL_1",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "duplicate"
        assert result.tier == "cross_format"

    def test_same_source_not_cross_format(self, engine, repo, imp):
        """Two bank transactions with same dedup_key but different FITIDs are NOT cross-format dupes."""
        _insert_txn(
            repo, imp.id, raw_description="AMAZON PURCHASE 1",
            amount=-25.00, date="2026-01-30", external_id="TS_001",
        )
        # Second bank charge, different FITID
        raw = _raw(
            raw_description="AMAZON PURCHASE 2",
            amount=-25.00, date="2026-01-30",
            external_id="TS_002",
        )
        result = engine.deduplicate(raw, imp.id)
        # Both have external_id → not cross-format → new
        assert result.status == "new"

    def test_cross_format_different_amount_no_match(self, engine, repo, imp):
        """Cross-format dedup requires exact amount match via dedup_key."""
        _insert_txn(
            repo, imp.id, raw_description="AMAZON.COM*ORDER1",
            amount=-45.09, date="2026-01-22", external_id="FITID_1",
        )
        raw = _raw(
            raw_description="Amazon",
            amount=-35.26, date="2026-01-22",  # Different amount
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "new"

    def test_overlapping_date_range(self, engine, repo, imp):
        """Import with overlapping dates: existing txns skipped, new ones imported."""
        # Existing: Jan 10, 15
        for i, (d, ext) in enumerate([("2026-01-10", "F10"), ("2026-01-15", "F15")]):
            _insert_txn(
                repo, imp.id, date=d, external_id=ext,
                amount=-50.0, raw_description=f"TXN {i}",
            )
        # New import covers Jan 10–20, with 3 txns
        raw_txns = [
            _raw(date="2026-01-10", external_id="F10", raw_description="TXN 0"),
            _raw(date="2026-01-15", external_id="F15", raw_description="TXN 1"),
            _raw(date="2026-01-20", external_id="F20", raw_description="TXN 2"),
        ]
        results = [engine.deduplicate(r, imp.id) for r in raw_txns]
        assert results[0].status == "duplicate"  # F10 exists
        assert results[1].status == "duplicate"  # F15 exists
        assert results[2].status == "new"         # F20 is new


# ── Batch processing ──────────────────────────────────────


class TestProcessBatch:
    def test_all_new(self, engine, imp):
        raw_txns = [
            _raw(amount=-10.0 * (i + 1), raw_description=f"TXN{i}")
            for i in range(3)
        ]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.new_count == 3
        assert result.duplicate_count == 0
        assert result.flagged_count == 0
        assert len(result.transactions) == 3

    def test_mixed_new_and_duplicate(self, engine, repo, imp):
        _insert_txn(repo, imp.id, external_id="F1", raw_description="EXISTING")
        raw_txns = [
            _raw(external_id="F1", raw_description="EXISTING"),  # dup
            _raw(external_id="F2", raw_description="NEW TXN", amount=-75.0),
        ]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.new_count == 1
        assert result.duplicate_count == 1

    def test_flagged_transactions(self, engine, repo, imp):
        """No external_ids → fuzzy dedup flags similar description."""
        _insert_txn(
            repo, imp.id,
            raw_description="PHILZ COFFEE SAN FRANCISCO",
        )
        raw_txns = [
            _raw(
                raw_description="PHILZ COFFEE SAN FRAN",
            ),
        ]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.flagged_count == 1
        assert result.new_count == 0
        assert result.transactions[0].status == "flagged"

    def test_transactions_inserted_to_db(self, engine, repo, imp):
        raw_txns = [_raw(external_id="F1"), _raw(external_id="F2", amount=-30.0)]
        result = engine.process_batch(raw_txns, imp.id)
        # Verify they're in the database
        for txn in result.transactions:
            found = repo.get_transaction(txn.id)
            assert found is not None

    def test_normalized_description_set(self, engine, imp):
        raw_txns = [_raw(raw_description="Philz Coffee #1234")]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.transactions[0].normalized_description == "PHILZ COFFEE"

    def test_source_preserved(self, engine, imp):
        raw_txns = [_raw()]
        result = engine.process_batch(raw_txns, imp.id, source="budget_app_historical")
        assert result.transactions[0].source == "budget_app_historical"

    def test_empty_batch(self, engine, imp):
        result = engine.process_batch([], imp.id)
        assert result.new_count == 0
        assert result.duplicate_count == 0
        assert result.flagged_count == 0
        assert result.transactions == []

    def test_dedup_within_batch_external_id(self, engine, repo, imp):
        """Second identical external_id in same batch is caught as duplicate."""
        raw_txns = [
            _raw(external_id="F1"),
            _raw(external_id="F1"),  # Same external_id
        ]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.new_count == 1
        assert result.duplicate_count == 1

    def test_dedup_within_batch_import_hash(self, engine, repo, imp):
        """Second identical content (no external_id) in same batch caught as duplicate."""
        raw_txns = [
            _raw(raw_description="EXACT SAME TXN"),
            _raw(raw_description="EXACT SAME TXN"),  # Same content
        ]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.new_count == 1
        assert result.duplicate_count == 1

    def test_cross_format_budget_app_after_bank_batch(self, engine, repo, imp):
        """Budget app batch import dedupes against existing bank transactions."""
        # Pre-existing bank transactions
        _insert_txn(
            repo, imp.id, raw_description="AMAZON.COM*ORDER1",
            amount=-45.09, date="2026-01-22", external_id="FITID_1",
        )
        _insert_txn(
            repo, imp.id, raw_description="APL*APPLE.COM/BILL",
            amount=-29.99, date="2026-01-26", external_id="FITID_2",
        )
        # Budget app batch with overlapping + new transactions
        imp2 = repo.insert_import(Import(file_name="budget_app.csv", file_hash="budgetapphash"))
        raw_txns = [
            _raw(raw_description="Amazon", amount=-45.09, date="2026-01-22"),  # dup
            _raw(raw_description="Apple", amount=-29.99, date="2026-01-26"),   # dup
            _raw(raw_description="Costco", amount=-150.00, date="2024-03-15"), # new
        ]
        result = engine.process_batch(raw_txns, imp2.id, source="budget_app")
        assert result.duplicate_count == 2
        assert result.new_count == 1

    def test_cross_format_count_based_matching(self, engine, repo, imp):
        """Same-day same-amount: 2 bank + 3 budget app → 2 deduped, 1 new."""
        # Two bank transactions with same amount on same day (different FITIDs)
        _insert_txn(
            repo, imp.id, raw_description="AMAZON.COM*ORDER1",
            amount=-25.00, date="2026-01-30", external_id="TS_001",
        )
        _insert_txn(
            repo, imp.id, raw_description="AMAZON.COM*ORDER2",
            amount=-25.00, date="2026-01-30", external_id="TS_002",
        )
        # Three budget app entries with same amount/date — 2 should dedup, 1 is new
        imp2 = repo.insert_import(Import(file_name="budget_app.csv", file_hash="budgetapphash2"))
        raw_txns = [
            _raw(raw_description="Amazon order A", amount=-25.00, date="2026-01-30"),
            _raw(raw_description="Amazon order B", amount=-25.00, date="2026-01-30"),
            _raw(raw_description="Amazon order C", amount=-25.00, date="2026-01-30"),
        ]
        result = engine.process_batch(raw_txns, imp2.id, source="budget_app")
        assert result.duplicate_count == 2
        assert result.new_count == 1

    def test_cross_format_bank_after_budget_app_batch(self, engine, repo, imp):
        """Bank batch import dedupes against existing budget app transactions."""
        # Pre-existing budget app transactions (no external_id)
        _insert_txn(
            repo, imp.id, raw_description="Amazon",
            amount=-45.09, date="2026-01-22",
        )
        # Bank batch with overlapping + new transactions
        imp2 = repo.insert_import(Import(file_name="bank.qfx", file_hash="bankhash"))
        raw_txns = [
            _raw(
                raw_description="AMAZON.COM*MB2KR7PH0",
                amount=-45.09, date="2026-01-22", external_id="FITID_1",
            ),  # dup of budget app
            _raw(
                raw_description="WHOLE FOODS MKT",
                amount=-85.00, date="2026-02-01", external_id="FITID_2",
            ),  # new (Feb data)
        ]
        result = engine.process_batch(raw_txns, imp2.id, source="bank")
        assert result.duplicate_count == 1
        assert result.new_count == 1


# ── Helper function tests ────────────────────────────────


class TestDescriptionsRelated:
    def test_containment(self):
        assert _descriptions_related("APPLE", "APPLE.COM/BILL") is True

    def test_containment_reverse(self):
        assert _descriptions_related("APPLE.COM/BILL", "APPLE") is True

    def test_shared_word(self):
        assert _descriptions_related("US PATENT TRADEMARK", "US PATENT AND TRADEMARK OFFICE") is True

    def test_unrelated(self):
        assert _descriptions_related("AMAZON PURCHASE", "WHOLE FOODS MARKET") is False

    def test_short_words_ignored(self):
        # "US" is only 2 chars, shouldn't count as shared word alone
        assert _descriptions_related("US", "US") is True  # containment still works

    def test_empty(self):
        assert _descriptions_related("", "SOMETHING") is False


class TestFindSubsetSum:
    def test_basic_match(self):
        result = _find_subset_sum([-99.99, -16.99], -116.98)
        assert result is not None
        assert set(result) == {0, 1}

    def test_no_match(self):
        result = _find_subset_sum([-99.99, -16.99], -50.00)
        assert result is None

    def test_subset_of_larger(self):
        result = _find_subset_sum([-29.99, -5.99, -50.00], -35.98)
        assert result is not None
        assert set(result) == {0, 1}

    def test_too_few(self):
        result = _find_subset_sum([-100.0], -100.0)
        assert result is None

    def test_tolerance(self):
        result = _find_subset_sum([-99.99, -17.00], -116.98, tolerance=0.02)
        assert result is not None


# ── Intra-batch import_hash with external_ids ────────────


class TestIntraBatchImportHashExternalId:
    def test_same_hash_different_external_ids_all_new(self, engine, imp):
        """5 transactions with same date/amount/description but different external_ids → all new."""
        raw_txns = [
            _raw(
                date="2025-09-17", amount=-65.0,
                raw_description="US PATENT TRADEMARK",
                external_id=f"FITID_{i}",
            )
            for i in range(5)
        ]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.new_count == 5
        assert result.duplicate_count == 0

    def test_same_hash_no_external_id_deduped(self, engine, imp):
        """Without external_ids, intra-batch import_hash dedup still works."""
        raw_txns = [
            _raw(raw_description="EXACT SAME TXN"),
            _raw(raw_description="EXACT SAME TXN"),
        ]
        result = engine.process_batch(raw_txns, imp.id)
        assert result.new_count == 1
        assert result.duplicate_count == 1


# ── Cross-format + intra-batch for repeated charges ──────


class TestRepeatedChargesCrossFormat:
    def test_us_patent_five_charges(self, engine, repo, imp):
        """1 Budget app + 5 QFX with same amount → 1 cross-format dedup, 4 new."""
        # Pre-existing Budget app transaction (no external_id)
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="US Patent", amount=-65.0,
            date="2025-09-17",
        )
        # 5 QFX transactions with different FITIDs
        imp2 = repo.insert_import(Import(file_name="cap1.qfx", file_hash="cap1hash"))
        raw_txns = [
            _raw(
                date="2025-09-17", amount=-65.0,
                raw_description="US PATENT TRADEMARK",
                external_id=f"FITID_PATENT_{i}",
            )
            for i in range(5)
        ]
        result = engine.process_batch(raw_txns, imp2.id, source="bank")
        assert result.duplicate_count == 1  # 1 cross-format match
        assert result.new_count == 4        # 4 genuinely new


# ── Split-sum dedup ──────────────────────────────────────


class TestSplitSumDedup:
    def test_basic_split_sum(self, engine, repo, imp):
        """2 Budget app splits summing to bank total → QFX marked duplicate."""
        # Two Budget app splits (no external_id)
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Apple", amount=-99.99,
            date="2026-01-15",
        )
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Apple", amount=-16.99,
            date="2026-01-15",
        )
        # QFX unsplit total
        raw = _raw(
            raw_description="APL*APPLE.COM/BILL",
            amount=-116.98, date="2026-01-15",
            external_id="FITID_APPLE_1",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "duplicate"
        assert result.tier == "split_sum"
        assert result.matched_splits is not None
        assert len(result.matched_splits) == 2

    def test_split_sum_unrelated_not_matched(self, engine, repo, imp):
        """Unrelated transaction on same date doesn't pollute split-sum."""
        # Two Budget app splits with related descriptions
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Apple", amount=-29.99,
            date="2026-01-26",
        )
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Apple", amount=-5.99,
            date="2026-01-26",
        )
        # Unrelated Budget app transaction on same date
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Costco", amount=-50.00,
            date="2026-01-26",
        )
        # QFX total should match the two Apple splits, not Costco
        raw = _raw(
            raw_description="APL*APPLE.COM/BILL",
            amount=-35.98, date="2026-01-26",
            external_id="FITID_APPLE_2",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "duplicate"
        assert result.tier == "split_sum"
        assert len(result.matched_splits) == 2
        for t in result.matched_splits:
            assert "APPLE" in t.raw_description.upper()

    def test_split_sum_no_match_when_unrelated(self, engine, repo, imp):
        """No split-sum match when descriptions don't relate."""
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Target", amount=-80.00,
            date="2026-01-15",
        )
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Walmart", amount=-36.98,
            date="2026-01-15",
        )
        raw = _raw(
            raw_description="APL*APPLE.COM/BILL",
            amount=-116.98, date="2026-01-15",
            external_id="FITID_APPLE_3",
        )
        result = engine.deduplicate(raw, imp.id)
        assert result.status == "new"

    def test_split_sum_consumed_tracking(self, engine, repo, imp):
        """Same splits can't match two incoming transactions."""
        # 2 Budget app splits summing to -100
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Amazon", amount=-60.00,
            date="2026-02-01",
        )
        _insert_txn(
            repo, imp.id, source="budget_app",
            raw_description="Amazon", amount=-40.00,
            date="2026-02-01",
        )
        # 2 QFX charges of -100 each
        imp2 = repo.insert_import(Import(file_name="bank2.qfx", file_hash="bank2hash"))
        raw_txns = [
            _raw(
                raw_description="AMAZON.COM*ORDER1",
                amount=-100.00, date="2026-02-01",
                external_id="FITID_AMZ_A",
            ),
            _raw(
                raw_description="AMAZON.COM*ORDER2",
                amount=-100.00, date="2026-02-01",
                external_id="FITID_AMZ_B",
            ),
        ]
        result = engine.process_batch(raw_txns, imp2.id, source="bank")
        # 1st matches the splits, 2nd is new (splits already consumed)
        assert result.duplicate_count == 1
        assert result.new_count == 1
