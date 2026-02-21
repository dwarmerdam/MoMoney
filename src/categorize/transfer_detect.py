"""Transfer detection: identifies inter-account transfers by description patterns."""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.config import Config


@dataclass
class TransferMatch:
    """Result of a transfer pattern match."""
    from_account: str
    to_account: str
    transfer_type: str  # cc-payment, savings-transfer, loan-payment, internal-transfer
    pattern: str


def detect_transfer(
    description: str,
    account_id: str,
    config: Config,
) -> TransferMatch | None:
    """Check if a transaction description matches a known transfer pattern.

    Matches are case-insensitive substring checks against the normalized
    or raw description. The account_id is used to verify directionality.
    """
    desc_upper = description.upper()
    transfers = config.rules.get("transfers", [])

    for rule in transfers:
        # Skip Golden1 INT special rule (handled separately)
        if "pattern_fitid_suffix" in rule:
            continue

        pattern = rule.get("pattern", "")
        # Skip empty patterns - they would match everything
        if not pattern:
            continue
        if pattern.upper() in desc_upper:
            from_acct = rule.get("from_account", "")
            to_acct = rule.get("to_account", "")
            # Skip rules with empty accounts - they would match incorrectly
            if not from_acct or not to_acct:
                continue
            # Match if the transaction's account is either side
            if account_id in (from_acct, to_acct):
                return TransferMatch(
                    from_account=from_acct,
                    to_account=to_acct,
                    transfer_type=rule.get("type", "transfer") or "internal-transfer",
                    pattern=pattern,
                )
    return None


_TRANSFER_PREFIX_RE = re.compile(r"^Transfer\s*:\s*(.+)", re.IGNORECASE)


def detect_transfer_by_txn_type(
    txn_type: str | None,
    description: str,
    amount: float,
    account_id: str,
    config: Config,
) -> TransferMatch | None:
    """Detect transfers using the bank-provided txn_type and 'Transfer : ' prefix.

    This is a fallback for transactions where the QFX <TRNTYPE> is TRANSFER
    and the payee name follows the 'Transfer : <account name>' format used
    by Wells Fargo, Capital One, and others.

    Returns None if:
    - txn_type is not TRANSFER
    - description doesn't start with 'Transfer :'
    - target account name can't be resolved
    - resolved account is the same as the source (self-referential)
    """
    m = _TRANSFER_PREFIX_RE.match(description)
    if not m:
        return None
    # Require either txn_type=TRANSFER or the "Transfer :" prefix alone
    # (budget app imports have the prefix but no txn_type)
    if txn_type and txn_type.upper() != "TRANSFER":
        return None

    target_name = m.group(1).strip()
    target_upper = target_name.upper()
    name_map = config.transfer_name_map

    # Try exact match first
    target_account = name_map.get(target_upper)

    # Substring match: target contains a config name, or config name contains target
    if target_account is None:
        for cfg_name, cfg_id in name_map.items():
            if cfg_name in target_upper or target_upper in cfg_name:
                target_account = cfg_id
                break

    if target_account is None or target_account == account_id:
        return None

    # Direction from amount sign
    if amount < 0:
        from_acct, to_acct = account_id, target_account
    else:
        from_acct, to_acct = target_account, account_id

    transfer_type = _infer_transfer_type(account_id, target_account, config)

    return TransferMatch(
        from_account=from_acct,
        to_account=to_acct,
        transfer_type=transfer_type,
        pattern=f"txn_type:Transfer : {target_name}",
    )


def _infer_transfer_type(
    account_id: str, other_account_id: str, config: Config
) -> str:
    """Infer transfer type from account types (checking, savings, credit, loan)."""
    acct = config.account_by_id(account_id)
    other = config.account_by_id(other_account_id)
    acct_type = (acct.get("account_type", "") if acct else "").lower()
    other_type = (other.get("account_type", "") if other else "").lower()

    types = {acct_type, other_type}
    if "credit" in types:
        return "cc-payment"
    if "savings" in types:
        return "savings-transfer"
    if "loan" in types:
        return "loan-payment"
    return "internal-transfer"


def detect_interest(
    external_id: str | None,
    account_id: str,
    config: Config,
) -> str | None:
    """Check if a transaction is an interest entry based on FITID suffix.

    Reads the interest_detection config for the account. Returns the
    category_id if matched, or None.
    """
    if not external_id:
        return None
    detection = config.interest_detection_for(account_id)
    if detection is None:
        return None
    suffix = detection.get("fitid_suffix", "")
    if suffix and external_id.upper().endswith(suffix.upper()):
        return detection.get("category_id", "interest-fees")
    return None
