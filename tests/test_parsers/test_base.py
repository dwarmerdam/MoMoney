"""Tests for parsers.base — normalization, hashing, date parsing."""

from pathlib import Path

from src.parsers.base import (
    compute_dedup_key,
    compute_file_hash,
    compute_import_hash,
    normalize_description,
    parse_ofx_date,
)


class TestNormalizeDescription:
    def test_uppercase(self):
        assert normalize_description("Philz Coffee") == "PHILZ COFFEE"

    def test_strip_long_numbers(self):
        assert normalize_description("VENMO PAYMENT 1036048383186") == "VENMO PAYMENT"

    def test_strip_hash_refs(self):
        assert normalize_description("CHECK #1234") == "CHECK"

    def test_strip_decorators(self):
        assert normalize_description("DD *DOORDASH TOPTHAI") == "DD DOORDASH TOPTHAI"

    def test_collapse_whitespace(self):
        assert normalize_description("MORTGAGE   MTG   PAYMENTS") == "MORTGAGE MTG PAYMENTS"

    def test_combined(self):
        result = normalize_description("ZELLE TO SAMPLE POOL SERVICES ON 01/23 REF # ABC0DEF1GH2I")
        assert "ZELLE" in result
        assert "SAMPLE" in result
        # hash ref # pattern should be stripped
        assert "#" not in result

    def test_empty(self):
        assert normalize_description("") == ""

    def test_real_wf_description(self):
        result = normalize_description("AMERICAN EXPRESS ACH PMT")
        assert result == "AMERICAN EXPRESS ACH PMT"


class TestParseOfxDate:
    def test_basic(self):
        assert parse_ofx_date("20241231") == "2024-12-31"

    def test_with_time_and_timezone(self):
        assert parse_ofx_date("20241231120000.000[-7:MST]") == "2024-12-31"

    def test_with_time_zone_gmt(self):
        assert parse_ofx_date("20241231120000[0:GMT]") == "2024-12-31"

    def test_with_pdt(self):
        assert parse_ofx_date("20240801110000.000[-7:PDT]") == "2024-08-01"


class TestComputeImportHash:
    def test_deterministic(self):
        h1 = compute_import_hash("wf-checking", "2026-01-15", -50.0, "PHILZ")
        h2 = compute_import_hash("wf-checking", "2026-01-15", -50.0, "PHILZ")
        assert h1 == h2

    def test_differs_on_any_field(self):
        base = compute_import_hash("wf-checking", "2026-01-15", -50.0, "PHILZ")
        diff_acct = compute_import_hash("amex-blue", "2026-01-15", -50.0, "PHILZ")
        diff_date = compute_import_hash("wf-checking", "2026-01-16", -50.0, "PHILZ")
        diff_amt = compute_import_hash("wf-checking", "2026-01-15", -50.01, "PHILZ")
        diff_desc = compute_import_hash("wf-checking", "2026-01-15", -50.0, "STARBUCKS")
        assert len({base, diff_acct, diff_date, diff_amt, diff_desc}) == 5


class TestComputeDedupKey:
    def test_format(self):
        key = compute_dedup_key("wf-checking", "2026-01-15", -50.00)
        assert key == "wf-checking:2026-01-15:-5000"

    def test_positive(self):
        key = compute_dedup_key("cap1-credit", "2026-01-15", 800.00)
        assert key == "cap1-credit:2026-01-15:80000"

    def test_fractional_cents(self):
        key = compute_dedup_key("wf-checking", "2026-01-15", -10.99)
        assert key == "wf-checking:2026-01-15:-1099"


class TestComputeFileHash:
    def test_deterministic(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        h1 = compute_file_hash(f)
        h2 = compute_file_hash(f)
        assert h1 == h2
        assert len(h1) == 64  # SHA256 hex

    def test_differs_for_different_content(self, tmp_path):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("hello")
        f2.write_text("world")
        assert compute_file_hash(f1) != compute_file_hash(f2)
