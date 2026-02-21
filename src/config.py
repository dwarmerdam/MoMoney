"""YAML configuration loader for MoMoney.

Loads all 6 seed config files from the config/ directory:
  accounts.yaml, categories.yaml, merchants.yaml,
  rules.yaml, parsers.yaml, budget_app_category_map.yaml
"""

from pathlib import Path

import yaml


class Config:
    """Loads and provides access to all YAML configuration files."""

    def __init__(self, config_dir: Path | str = "config"):
        self.config_dir = Path(config_dir)
        if not self.config_dir.is_dir():
            raise FileNotFoundError(f"Config directory not found: {self.config_dir}")

        self._accounts: list[dict] | None = None
        self._categories: list[dict] | None = None
        self._merchants: dict | None = None
        self._rules: dict | None = None
        self._parsers: dict | None = None
        self._budget_app_category_map: dict | None = None

    def _load(self, filename: str) -> dict | list:
        path = self.config_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path) as f:
            try:
                data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise ValueError(f"Invalid YAML in {path}: {e}") from e
        if data is None:
            raise ValueError(f"Empty config file: {path}")
        return data

    @property
    def accounts(self) -> list[dict]:
        if self._accounts is None:
            data = self._load("accounts.yaml")
            self._accounts = data.get("accounts", data) if isinstance(data, dict) else data
        return self._accounts

    @property
    def categories(self) -> list[dict]:
        if self._categories is None:
            data = self._load("categories.yaml")
            if isinstance(data, dict):
                self._categories = data.get("tree", data.get("categories", data))
            else:
                self._categories = data
        return self._categories

    @property
    def merchants(self) -> dict:
        if self._merchants is None:
            self._merchants = self._load("merchants.yaml")
        return self._merchants

    @property
    def rules(self) -> dict:
        if self._rules is None:
            self._rules = self._load("rules.yaml")
        return self._rules

    @property
    def parsers(self) -> dict:
        """Load parsers.yaml configuration.

        Note: Currently unused in production code. Parser selection is
        handled dynamically in observer.py based on file extension.
        Retained for potential future use and backward compatibility.
        """
        if self._parsers is None:
            self._parsers = self._load("parsers.yaml")
        return self._parsers

    @property
    def budget_app_category_map_raw(self) -> dict:
        """Return the raw YAML structure of budget_app_category_map.yaml."""
        if self._budget_app_category_map is None:
            self._budget_app_category_map = self._load("budget_app_category_map.yaml")
        return self._budget_app_category_map

    @property
    def budget_app_category_map(self) -> dict[str, str]:
        """Return a flat dict mapping budget app category strings to category IDs.

        Flattens the grouped structure in budget_app_category_map.yaml into:
        {"Monthly Needs (Shared): Car Insurance": "car-insurance", ...}
        """
        raw = self.budget_app_category_map_raw
        flat: dict[str, str] = {}
        for group_name, entries in raw.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                budget_app_name = entry.get("budget_app")
                category_id = entry.get("category_id")
                if budget_app_name and category_id:
                    flat[budget_app_name] = category_id
        return flat

    def account_by_id(self, account_id: str) -> dict | None:
        for acct in self.accounts:
            if acct.get("id") == account_id:
                return acct
        return None

    def category_filter_for(self, account_id: str) -> dict | None:
        """Return the category_filter config for an account, or None.

        The filter dict has keys: default_category, compatible_prefixes,
        compatible_ids.  Used by the pipeline to override personal categories
        on business accounts.
        """
        acct = self.account_by_id(account_id)
        if acct is None:
            return None
        return acct.get("category_filter")

    def interest_detection_for(self, account_id: str) -> dict | None:
        """Return the interest_detection config for an account, or None.

        The dict has keys: fitid_suffix, category_id.
        """
        acct = self.account_by_id(account_id)
        if acct is None:
            return None
        return acct.get("interest_detection")

    @property
    def transfer_categories(self) -> dict[str, str]:
        """Map transfer type → category_id from rules.yaml."""
        return self.rules.get("transfer_categories", {})

    @property
    def receipt_categories(self) -> list[str]:
        """Category IDs for receipt parsing prompts from rules.yaml."""
        return self.rules.get("receipt_categories", [])

    @property
    def fallback_category(self) -> str:
        """Category assigned when no rule matches. Default: 'uncategorized'."""
        return self.rules.get("fallback_category", "uncategorized")

    @property
    def mercury_account_routing(self) -> dict[str, str]:
        """Derive Mercury CSV 'Source Account' → account_id routing from accounts."""
        routing: dict[str, str] = {}
        for acct in self.accounts:
            if acct.get("import_format") == "mercury_csv":
                # Use the account name as the Source Account column value
                name = acct.get("name", "")
                acct_id = acct.get("id", "")
                # Skip entries with empty name or id to avoid incorrect routing
                if name and acct_id:
                    routing[name] = acct_id
        return routing

    @property
    def transfer_name_map(self) -> dict[str, str]:
        """Map uppercased account display names to account IDs.

        Used by detect_transfer_by_txn_type() to resolve the target account
        from 'Transfer : <name>' descriptions in QFX files.
        """
        name_map: dict[str, str] = {}
        for acct in self.accounts:
            acct_id = acct.get("id", "")
            if not acct_id:
                continue
            for key in ("name", "budget_app_name"):
                val = acct.get(key, "")
                if val:
                    name_map[val.upper()] = acct_id
            for alias in acct.get("transfer_aliases", []):
                if alias:
                    name_map[alias.upper()] = acct_id
        return name_map

    @property
    def budget_app_account_routing(self) -> dict[str, str]:
        """Derive budget app account name → account_id routing from accounts."""
        routing: dict[str, str] = {}
        for acct in self.accounts:
            budget_app_name = acct.get("budget_app_name", "")
            acct_id = acct.get("id", "")
            # Skip entries with empty budget_app_name or id to avoid incorrect routing
            if budget_app_name and acct_id:
                routing[budget_app_name] = acct_id
        return routing

    def flatten_category_tree(self) -> dict[str, dict]:
        """Walk categories tree and return flat lookup: category_id → metadata.

        Each entry has keys: name, level, level_0, level_1, level_2, level_3,
        is_leaf, is_income, is_transfer.  Level columns use display names.
        """
        result: dict[str, dict] = {}

        def _walk(nodes: list[dict], depth: int, ancestors: list[str],
                  inherit_income: bool, inherit_transfer: bool) -> None:
            for node in nodes:
                cat_id = node.get("id", "")
                name = node.get("name", "")
                children = node.get("children", [])
                is_income = node.get("is_income", inherit_income)
                is_transfer = node.get("is_transfer", inherit_transfer)
                has_children = bool(children)
                is_leaf = not has_children

                if cat_id:
                    levels = list(ancestors) + [name]
                    # Pad to 4 levels (0-3)
                    while len(levels) < 4:
                        levels.append("")
                    result[cat_id] = {
                        "category_id": cat_id,
                        "name": name,
                        "level": depth,
                        "level_0": levels[0],
                        "level_1": levels[1],
                        "level_2": levels[2],
                        "level_3": levels[3],
                        "is_leaf": is_leaf,
                        "is_income": bool(is_income),
                        "is_transfer": bool(is_transfer),
                    }

                if has_children:
                    child_ancestors = list(ancestors) + ([name] if cat_id else [])
                    child_depth = depth + (1 if cat_id else 0)
                    _walk(children, child_depth, child_ancestors,
                          is_income, is_transfer)

        _walk(self.categories, 0, [], False, False)
        return result
