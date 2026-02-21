"""Gmail API client using domain-wide delegation.

Provides search and message retrieval for receipt lookup.
Auth: service account impersonating a Google Workspace user.
Scope: gmail.readonly.
"""

from __future__ import annotations

import base64
import logging
from datetime import datetime, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

GMAIL_SCOPE = ["https://www.googleapis.com/auth/gmail.readonly"]

# Search queries by merchant type
_APPLE_QUERY = (
    'from:no_reply@email.apple.com subject:"Your receipt from Apple"'
)
_AMAZON_QUERY = (
    "from:(auto-confirm@amazon.com OR digital-no-reply@amazon.com"
    " OR shipment-tracking@amazon.com)"
)


class GmailClient:
    """Gmail API client using domain-wide delegation.

    Args:
        service_account_file: Path to service account JSON key file.
        target_user: Google Workspace email to impersonate.
    """

    def __init__(self, service_account_file: str, target_user: str):
        self.service_account_file = service_account_file
        self.target_user = target_user
        self._service = None

    @property
    def service(self):
        """Lazy-initialize the Gmail API service."""
        if self._service is None:
            credentials = service_account.Credentials.from_service_account_file(
                self.service_account_file,
                scopes=GMAIL_SCOPE,
            )
            delegated = credentials.with_subject(self.target_user)
            self._service = build("gmail", "v1", credentials=delegated)
        return self._service

    def search_receipts(
        self,
        merchant_type: str,
        charge_date: str,
        charge_amount: float,
    ) -> list[dict] | None:
        """Search Gmail for receipt emails near charge date.

        Args:
            merchant_type: "apple" or "amazon".
            charge_date: Transaction date as "YYYY-MM-DD".
            charge_amount: Transaction amount (negative for charges).
                Used for logging context; actual amount matching is done
                by the caller using receipt line items.

        Returns:
            List of message dicts with "id" and "threadId" keys.
            Empty list if no matching emails found.
            None if an error occurred (allows caller to distinguish).
        """
        date = datetime.strptime(charge_date, "%Y-%m-%d")

        if merchant_type == "apple":
            base_query = _APPLE_QUERY
            before_days = 1
            after_days = 3
        elif merchant_type == "amazon":
            base_query = _AMAZON_QUERY
            before_days = 1
            after_days = 14
        else:
            logger.warning("Unknown merchant type: %s", merchant_type)
            return None

        after_date = (date - timedelta(days=after_days)).strftime("%Y/%m/%d")
        before_date = (date + timedelta(days=before_days)).strftime("%Y/%m/%d")
        query = f"{base_query} after:{after_date} before:{before_date}"

        try:
            results = (
                self.service.users()
                .messages()
                .list(userId="me", q=query, maxResults=10)
                .execute()
            )
            messages = results.get("messages", [])
            logger.info(
                "Gmail search for %s on %s ($%.2f): %d results",
                merchant_type, charge_date, abs(charge_amount), len(messages),
            )
            return messages
        except Exception:
            logger.exception(
                "Gmail search failed for %s on %s ($%.2f)",
                merchant_type, charge_date, abs(charge_amount),
            )
            return None

    def get_message_body(self, msg_id: str) -> str:
        """Fetch and decode an email message body.

        Prefers text/plain, falls back to text/html.

        Args:
            msg_id: Gmail message ID.

        Returns:
            Decoded email body text. Empty string on error.
        """
        try:
            msg = (
                self.service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )
            return _extract_body(msg)
        except Exception:
            logger.exception("Failed to fetch message %s", msg_id)
            return ""


def _extract_body(msg: dict) -> str:
    """Extract text body from Gmail API message response.

    Handles single-part, multipart, and nested multipart messages.
    Prefers text/plain, falls back to text/html.
    """
    payload = msg.get("payload", {})

    # Single part message
    if "body" in payload and payload["body"].get("data"):
        return _decode_body(payload["body"]["data"])

    # Collect text parts recursively from nested multipart
    text_plain = None
    text_html = None

    def _search_parts(parts: list[dict]) -> None:
        nonlocal text_plain, text_html
        for part in parts:
            mime = part.get("mimeType", "")
            data = part.get("body", {}).get("data", "")
            if mime == "text/plain" and data and text_plain is None:
                text_plain = data
            elif mime == "text/html" and data and text_html is None:
                text_html = data
            # Recurse into nested multipart parts
            nested = part.get("parts", [])
            if nested:
                _search_parts(nested)

    _search_parts(payload.get("parts", []))

    if text_plain:
        return _decode_body(text_plain)
    if text_html:
        return _decode_body(text_html)

    return ""


def _decode_body(data: str) -> str:
    """Decode base64url-encoded email body."""
    return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
