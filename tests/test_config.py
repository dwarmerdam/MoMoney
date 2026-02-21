"""Tests for src.config — YAML configuration loader."""

import pytest
from pathlib import Path
from src.config import Config
from tests.conftest import FIXTURE_CONFIG_DIR


class TestConfigInit:
    def test_loads_from_config_dir(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert config.config_dir == FIXTURE_CONFIG_DIR

    def test_raises_on_missing_directory(self):
        with pytest.raises(FileNotFoundError, match="Config directory not found"):
            Config("/nonexistent/path")

    def test_raises_on_file_not_directory(self, tmp_path):
        f = tmp_path / "not_a_dir.yaml"
        f.write_text("test: true")
        with pytest.raises(FileNotFoundError, match="Config directory not found"):
            Config(f)


class TestConfigAccounts:
    def test_loads_9_accounts(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert len(config.accounts) == 9

    def test_accounts_are_list_of_dicts(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert isinstance(config.accounts, list)
        for acct in config.accounts:
            assert isinstance(acct, dict)

    def test_all_accounts_have_required_fields(self):
        config = Config(FIXTURE_CONFIG_DIR)
        required = {"id", "name", "institution", "account_type", "import_format"}
        for acct in config.accounts:
            missing = required - acct.keys()
            assert not missing, f"Account {acct.get('id', '?')} missing fields: {missing}"

    def test_known_account_ids(self):
        config = Config(FIXTURE_CONFIG_DIR)
        ids = {a["id"] for a in config.accounts}
        expected = {
            "wf-checking", "wf-savings", "cap1-credit", "cap1-savings",
            "amex-blue", "golden1-auto", "mercury-checking", "mercury-savings",
            "mercury-credit",
        }
        assert ids == expected

    def test_lazy_loading(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert config._accounts is None
        _ = config.accounts
        assert config._accounts is not None

    def test_caches_after_first_load(self):
        config = Config(FIXTURE_CONFIG_DIR)
        first = config.accounts
        second = config.accounts
        assert first is second


class TestConfigCategories:
    def test_loads_categories(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert len(config.categories) > 0

    def test_categories_is_list(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert isinstance(config.categories, list)


class TestConfigMerchants:
    def test_loads_merchants(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert isinstance(config.merchants, dict)

    def test_has_two_tiers(self):
        config = Config(FIXTURE_CONFIG_DIR)
        m = config.merchants
        assert "auto" in m
        assert "high_confidence" in m


class TestConfigRules:
    def test_loads_rules(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert isinstance(config.rules, dict)

    def test_has_transfer_patterns(self):
        config = Config(FIXTURE_CONFIG_DIR)
        r = config.rules
        has_transfers = "transfer_patterns" in r or "transfers" in r or any(
            "transfer" in str(k).lower() for k in r.keys()
        )
        assert has_transfers, f"No transfer patterns found. Keys: {list(r.keys())}"


class TestConfigParsers:
    def test_loads_parsers(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert isinstance(config.parsers, dict)


class TestConfigBudgetAppMap:
    def test_loads_budget_app_category_map(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert isinstance(config.budget_app_category_map, dict)

    def test_has_category_groups(self):
        config = Config(FIXTURE_CONFIG_DIR)
        # Should have multiple top-level groups
        assert len(config.budget_app_category_map) > 5


class TestAccountById:
    def test_finds_existing_account(self):
        config = Config(FIXTURE_CONFIG_DIR)
        acct = config.account_by_id("wf-checking")
        assert acct is not None
        assert acct["id"] == "wf-checking"
        assert acct["institution"] == "Wells Fargo"

    def test_returns_none_for_missing(self):
        config = Config(FIXTURE_CONFIG_DIR)
        assert config.account_by_id("nonexistent") is None

    def test_finds_mercury_credit(self):
        config = Config(FIXTURE_CONFIG_DIR)
        acct = config.account_by_id("mercury-credit")
        assert acct is not None
        assert acct["account_type"] == "credit"
        assert acct["import_format"] == "mercury_csv"


class TestMissingConfigFile:
    def test_raises_on_missing_yaml(self, tmp_path):
        config = Config.__new__(Config)
        config.config_dir = tmp_path
        config._accounts = None
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            _ = config.accounts


class TestConfigErrorHandling:
    def test_raises_on_empty_yaml(self, tmp_path):
        """Empty YAML file returns None from safe_load — should raise ValueError."""
        (tmp_path / "accounts.yaml").write_text("")
        config = Config.__new__(Config)
        config.config_dir = tmp_path
        config._accounts = None
        with pytest.raises(ValueError, match="Empty config file"):
            _ = config.accounts

    def test_raises_on_malformed_yaml(self, tmp_path):
        """Malformed YAML should raise ValueError, not yaml.YAMLError."""
        (tmp_path / "accounts.yaml").write_text("{{invalid: yaml: [}")
        config = Config.__new__(Config)
        config.config_dir = tmp_path
        config._accounts = None
        with pytest.raises(ValueError, match="Invalid YAML"):
            _ = config.accounts
