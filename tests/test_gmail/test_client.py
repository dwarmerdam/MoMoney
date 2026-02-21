"""Tests for the Gmail API client.

All tests use mocks — no live Google API needed.
Google API client libraries have dependency issues in the test
environment, so we mock them at the sys.modules level.
"""

from __future__ import annotations

import base64
import sys
from unittest.mock import MagicMock

import pytest

# Mock Google API dependencies before importing our module
# This avoids the broken cryptography/cffi chain in this env.
_mock_sa = MagicMock()
_mock_discovery = MagicMock()

_original_modules = {}
_mocked = {
    "google": MagicMock(),
    "google.auth": MagicMock(),
    "google.auth.crypt": MagicMock(),
    "google.auth._service_account_info": MagicMock(),
    "google.oauth2": MagicMock(),
    "google.oauth2.service_account": _mock_sa,
    "googleapiclient": MagicMock(),
    "googleapiclient.discovery": _mock_discovery,
}
for mod_name, mock_mod in _mocked.items():
    _original_modules[mod_name] = sys.modules.get(mod_name)
    sys.modules[mod_name] = mock_mod

from src.gmail.client import GmailClient, _decode_body, _extract_body  # noqa: E402


# ── Body extraction tests ────────────────────────────────


class TestExtractBody:
    def test_single_part_text(self):
        """Single-part message with data in body."""
        encoded = base64.urlsafe_b64encode(b"Hello World").decode()
        msg = {"payload": {"body": {"data": encoded}}}
        assert _extract_body(msg) == "Hello World"

    def test_multipart_prefers_plain(self):
        """Multipart message: text/plain preferred over text/html."""
        plain = base64.urlsafe_b64encode(b"Plain text").decode()
        html = base64.urlsafe_b64encode(b"<p>HTML</p>").decode()
        msg = {
            "payload": {
                "body": {},
                "parts": [
                    {"mimeType": "text/html", "body": {"data": html}},
                    {"mimeType": "text/plain", "body": {"data": plain}},
                ],
            }
        }
        assert _extract_body(msg) == "Plain text"

    def test_multipart_falls_back_to_html(self):
        """If no text/plain, use text/html."""
        html = base64.urlsafe_b64encode(b"<p>HTML only</p>").decode()
        msg = {
            "payload": {
                "body": {},
                "parts": [
                    {"mimeType": "text/html", "body": {"data": html}},
                ],
            }
        }
        assert _extract_body(msg) == "<p>HTML only</p>"

    def test_empty_payload(self):
        """Message with empty payload returns empty string."""
        msg = {"payload": {}}
        assert _extract_body(msg) == ""

    def test_no_parts_no_data(self):
        """Message with body but no data."""
        msg = {"payload": {"body": {}, "parts": []}}
        assert _extract_body(msg) == ""


class TestDecodeBody:
    def test_basic_decode(self):
        encoded = base64.urlsafe_b64encode(b"test content").decode()
        assert _decode_body(encoded) == "test content"

    def test_unicode_content(self):
        encoded = base64.urlsafe_b64encode("café résumé".encode()).decode()
        assert _decode_body(encoded) == "café résumé"


# ── Search query construction tests ──────────────────────


class TestSearchReceipts:
    def test_apple_query_construction(self):
        """Apple search uses correct from/subject and ±3 day window."""
        mock_service = MagicMock()
        mock_service.users().messages().list().execute.return_value = {
            "messages": [{"id": "msg1", "threadId": "t1"}]
        }

        client = GmailClient("/fake/sa.json", "user@example.com")
        client._service = mock_service  # Bypass auth

        results = client.search_receipts("apple", "2026-01-15", -45.97)

        assert len(results) == 1
        list_call = mock_service.users().messages().list
        call_kwargs = list_call.call_args[1]
        query = call_kwargs["q"]
        assert "no_reply@email.apple.com" in query
        assert "after:2026/01/12" in query  # -3 days
        assert "before:2026/01/16" in query  # +1 day

    def test_amazon_query_construction(self):
        """Amazon search uses correct from addresses and ±14 day window."""
        mock_service = MagicMock()
        mock_service.users().messages().list().execute.return_value = {
            "messages": []
        }

        client = GmailClient("/fake/sa.json", "user@example.com")
        client._service = mock_service

        results = client.search_receipts("amazon", "2026-01-15", -170.29)

        list_call = mock_service.users().messages().list
        call_kwargs = list_call.call_args[1]
        query = call_kwargs["q"]
        assert "auto-confirm@amazon.com" in query
        assert "shipment-tracking@amazon.com" in query
        assert "after:2026/01/01" in query  # -14 days
        assert "before:2026/01/16" in query  # +1 day

    def test_unknown_merchant_returns_none(self):
        """Unknown merchant type returns None (indicates error, not empty results)."""
        client = GmailClient("/fake/sa.json", "user@example.com")
        client._service = MagicMock()
        results = client.search_receipts("unknown", "2026-01-15", -10.00)
        assert results is None

    def test_api_error_returns_none(self):
        """Gmail API error returns None (distinguishable from empty results)."""
        mock_service = MagicMock()
        mock_service.users().messages().list().execute.side_effect = Exception("API error")

        client = GmailClient("/fake/sa.json", "user@example.com")
        client._service = mock_service

        results = client.search_receipts("apple", "2026-01-15", -22.99)
        assert results is None

    def test_no_results_returns_empty_list(self):
        """No matching emails returns empty list (not None)."""
        mock_service = MagicMock()
        mock_service.users().messages().list().execute.return_value = {
            "messages": []
        }

        client = GmailClient("/fake/sa.json", "user@example.com")
        client._service = mock_service

        results = client.search_receipts("apple", "2026-01-15", -22.99)
        assert results == []


class TestGetMessageBody:
    def test_fetches_and_decodes(self):
        """Successfully fetches and decodes a message body."""
        mock_service = MagicMock()
        encoded = base64.urlsafe_b64encode(b"Receipt body text").decode()
        mock_service.users().messages().get().execute.return_value = {
            "payload": {"body": {"data": encoded}}
        }

        client = GmailClient("/fake/sa.json", "user@example.com")
        client._service = mock_service

        body = client.get_message_body("msg123")
        assert body == "Receipt body text"

    def test_api_error_returns_empty(self):
        """API error when fetching message returns empty string."""
        mock_service = MagicMock()
        mock_service.users().messages().get().execute.side_effect = Exception("err")

        client = GmailClient("/fake/sa.json", "user@example.com")
        client._service = mock_service

        body = client.get_message_body("msg123")
        assert body == ""
