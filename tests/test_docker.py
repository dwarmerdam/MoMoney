"""Tests for Phase 10: Docker configuration files.

Validates Dockerfile, docker-compose.yaml, and .dockerignore for
correctness without requiring a Docker daemon. Tests parse and
validate file structure, required directives, environment variables,
volumes, and security exclusions.
"""

from __future__ import annotations

from pathlib import Path

import yaml
import pytest


PROJECT_ROOT = Path(__file__).parent.parent

DOCKERFILE = PROJECT_ROOT / "Dockerfile"
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yaml"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"


# ── Dockerfile tests ─────────────────────────────────────


class TestDockerfile:
    """Validate Dockerfile structure and directives."""

    def test_dockerfile_exists(self):
        assert DOCKERFILE.exists()

    def test_base_image_is_python_slim(self):
        """Base image should be python:3.12-slim."""
        content = DOCKERFILE.read_text()
        assert "FROM python:3.12-slim" in content

    def test_workdir_is_app(self):
        """WORKDIR should be /app."""
        content = DOCKERFILE.read_text()
        assert "WORKDIR /app" in content

    def test_copies_pyproject_toml(self):
        """pyproject.toml must be copied for pip install."""
        content = DOCKERFILE.read_text()
        assert "COPY pyproject.toml" in content

    def test_copies_source_code(self):
        """src/ directory must be copied."""
        content = DOCKERFILE.read_text()
        assert "COPY src/" in content

    def test_copies_config(self):
        """config/ directory must be copied."""
        content = DOCKERFILE.read_text()
        assert "COPY config/" in content

    def test_pip_install_no_cache(self):
        """pip install should use --no-cache-dir for smaller image."""
        content = DOCKERFILE.read_text()
        assert "--no-cache-dir" in content

    def test_entrypoint_is_momoney(self):
        """ENTRYPOINT should be the momoney CLI."""
        content = DOCKERFILE.read_text()
        assert 'ENTRYPOINT ["momoney"]' in content

    def test_default_cmd_is_watch(self):
        """Default CMD should be 'watch'."""
        content = DOCKERFILE.read_text()
        assert 'CMD ["watch"]' in content

    def test_creates_volume_directories(self):
        """Default volume directories should be created."""
        content = DOCKERFILE.read_text()
        assert "/app/data" in content
        assert "/app/import" in content
        assert "/app/credentials" in content

    def test_source_copied_before_pip_install(self):
        """Source must be available before pip install (setuptools needs it)."""
        content = DOCKERFILE.read_text()
        lines = content.strip().split("\n")
        # Find positions
        copy_src_line = None
        pip_install_line = None
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("COPY src/"):
                copy_src_line = i
            if "pip install" in stripped:
                pip_install_line = i

        assert copy_src_line is not None, "COPY src/ not found"
        assert pip_install_line is not None, "pip install not found"
        assert copy_src_line < pip_install_line, \
            "COPY src/ must come before pip install"

    def test_no_credentials_copied(self):
        """Credentials should never be COPY'd into the image."""
        content = DOCKERFILE.read_text()
        assert "COPY credentials" not in content
        assert "service-account" not in content.lower()


# ── docker-compose.yaml tests ────────────────────────────


class TestDockerCompose:
    """Validate docker-compose.yaml structure."""

    @pytest.fixture()
    def compose(self):
        """Parse docker-compose.yaml."""
        return yaml.safe_load(COMPOSE_FILE.read_text())

    def test_compose_file_exists(self):
        assert COMPOSE_FILE.exists()

    def test_compose_valid_yaml(self):
        """docker-compose.yaml is valid YAML."""
        data = yaml.safe_load(COMPOSE_FILE.read_text())
        assert data is not None

    def test_no_deprecated_version_key(self, compose):
        """Modern compose files should not have deprecated version key."""
        assert "version" not in compose

    def test_momoney_service_exists(self, compose):
        """momoney service must be defined."""
        assert "momoney" in compose.get("services", {})

    def test_build_from_current_dir(self, compose):
        """Service builds from current directory."""
        svc = compose["services"]["momoney"]
        assert svc.get("build") == "."

    def test_restart_policy(self, compose):
        """Restart policy should be unless-stopped."""
        svc = compose["services"]["momoney"]
        assert svc.get("restart") == "unless-stopped"

    def test_data_volume_mounted(self, compose):
        """./data volume mounted for SQLite persistence."""
        svc = compose["services"]["momoney"]
        volumes = svc.get("volumes", [])
        data_vol = [v for v in volumes if "/app/data" in v]
        assert len(data_vol) == 1
        assert data_vol[0].startswith("./data:")

    def test_import_volume_mounted(self, compose):
        """./import volume mounted for file watching."""
        svc = compose["services"]["momoney"]
        volumes = svc.get("volumes", [])
        import_vol = [v for v in volumes if "/app/import" in v]
        assert len(import_vol) == 1
        assert import_vol[0].startswith("./import:")

    def test_credentials_volume_readonly(self, compose):
        """./credentials volume mounted read-only."""
        svc = compose["services"]["momoney"]
        volumes = svc.get("volumes", [])
        cred_vol = [v for v in volumes if "/app/credentials" in v]
        assert len(cred_vol) == 1
        assert ":ro" in cred_vol[0], "Credentials volume must be read-only"

    def test_three_volumes(self, compose):
        """Exactly 3 volumes should be mounted."""
        svc = compose["services"]["momoney"]
        assert len(svc.get("volumes", [])) == 3

    def test_env_finance_db_path(self, compose):
        """FINANCE_DB_PATH environment variable set."""
        env = compose["services"]["momoney"].get("environment", [])
        db_vars = [e for e in env if "FINANCE_DB_PATH" in e]
        assert len(db_vars) == 1
        assert "/app/data/finance.db" in db_vars[0]

    def test_env_finance_watch_dir(self, compose):
        """FINANCE_WATCH_DIR environment variable set."""
        env = compose["services"]["momoney"].get("environment", [])
        watch_vars = [e for e in env if "FINANCE_WATCH_DIR" in e]
        assert len(watch_vars) == 1
        assert "/app/import" in watch_vars[0]

    def test_env_finance_credentials(self, compose):
        """FINANCE_CREDENTIALS environment variable set."""
        env = compose["services"]["momoney"].get("environment", [])
        cred_vars = [e for e in env if "FINANCE_CREDENTIALS" in e]
        assert len(cred_vars) == 1
        assert "service-account.json" in cred_vars[0]

    def test_env_finance_gmail_user(self, compose):
        """FINANCE_GMAIL_USER environment variable set."""
        env = compose["services"]["momoney"].get("environment", [])
        gmail_vars = [e for e in env if "FINANCE_GMAIL_USER" in e]
        assert len(gmail_vars) == 1

    def test_env_finance_spreadsheet_id(self, compose):
        """FINANCE_SPREADSHEET_ID environment variable set."""
        env = compose["services"]["momoney"].get("environment", [])
        sheet_vars = [e for e in env if "FINANCE_SPREADSHEET_ID" in e]
        assert len(sheet_vars) == 1

    def test_six_environment_variables(self, compose):
        """Exactly 6 environment variables should be set."""
        env = compose["services"]["momoney"].get("environment", [])
        assert len(env) == 6

    def test_volume_paths_consistent_with_env(self, compose):
        """Volume mount paths match environment variable paths."""
        svc = compose["services"]["momoney"]
        env = svc.get("environment", [])
        volumes = svc.get("volumes", [])

        env_dict = {}
        for e in env:
            if "=" in e:
                key, val = e.split("=", 1)
                env_dict[key] = val

        # DB path should be inside the data volume
        assert env_dict["FINANCE_DB_PATH"].startswith("/app/data/")
        data_vol = [v for v in volumes if "/app/data" in v]
        assert len(data_vol) == 1

        # Watch dir should match the import volume
        assert env_dict["FINANCE_WATCH_DIR"] == "/app/import"
        import_vol = [v for v in volumes if "/app/import" in v]
        assert len(import_vol) == 1

        # Credentials path should be inside the credentials volume
        assert env_dict["FINANCE_CREDENTIALS"].startswith("/app/credentials/")
        cred_vol = [v for v in volumes if "/app/credentials" in v]
        assert len(cred_vol) == 1


# ── .dockerignore tests ──────────────────────────────────


class TestDockerignore:
    """Validate .dockerignore excludes sensitive and unnecessary files."""

    @pytest.fixture()
    def patterns(self):
        """Read and return .dockerignore patterns."""
        return DOCKERIGNORE.read_text()

    def test_dockerignore_exists(self):
        assert DOCKERIGNORE.exists()

    def test_excludes_git_directory(self, patterns):
        assert ".git/" in patterns

    def test_excludes_pycache(self, patterns):
        assert "__pycache__/" in patterns

    def test_excludes_tests(self, patterns):
        """Test files shouldn't be in production image."""
        assert "tests/" in patterns

    def test_excludes_credentials(self, patterns):
        """Credentials mounted at runtime, not baked in."""
        assert "credentials/" in patterns

    def test_excludes_data(self, patterns):
        """Data directory mounted at runtime."""
        assert "data/" in patterns

    def test_excludes_env_files(self, patterns):
        """Environment files should never be in the image."""
        assert ".env" in patterns

    def test_excludes_db_files(self, patterns):
        """SQLite database files should not be in the image."""
        assert "*.db" in patterns

    def test_excludes_markdown(self, patterns):
        """Documentation not needed in production image."""
        assert "*.md" in patterns

    def test_excludes_venv(self, patterns):
        """Virtual environments should not be in the image."""
        assert "venv/" in patterns or ".venv/" in patterns

    def test_excludes_import_dir(self, patterns):
        """Import directory mounted at runtime."""
        assert "import/" in patterns


# ── Cross-file consistency tests ─────────────────────────


class TestCrossFileConsistency:
    """Verify Dockerfile, compose, and project files are consistent."""

    def test_entrypoint_matches_pyproject_scripts(self):
        """Dockerfile ENTRYPOINT matches the script name in pyproject.toml."""
        pyproject = yaml.safe_load(
            (PROJECT_ROOT / "pyproject.toml").read_text()
        ) if False else None

        # Read pyproject manually (it's TOML, not YAML)
        pyproject_text = (PROJECT_ROOT / "pyproject.toml").read_text()
        assert 'momoney = "src.cli:main"' in pyproject_text

        dockerfile_text = DOCKERFILE.read_text()
        assert 'ENTRYPOINT ["momoney"]' in dockerfile_text

    def test_python_version_compatible(self):
        """Dockerfile Python version satisfies pyproject requires-python."""
        pyproject_text = (PROJECT_ROOT / "pyproject.toml").read_text()
        assert '>=3.11' in pyproject_text or '>=3.12' in pyproject_text

        dockerfile_text = DOCKERFILE.read_text()
        # python:3.12-slim satisfies >=3.11
        assert "python:3.12" in dockerfile_text

    def test_config_dir_available_in_image(self):
        """Config directory is copied and not dockerignored."""
        dockerfile_text = DOCKERFILE.read_text()
        assert "COPY config/" in dockerfile_text

        dockerignore_text = DOCKERIGNORE.read_text()
        assert "config/" not in dockerignore_text
