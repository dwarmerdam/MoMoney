# Security

This document describes the security model, audit results, and operational
guidelines for MoMoney. Since this is a **template repository** that you
deploy as your own private instance, you are responsible for securing your
own deployment.

---

## Security Audit Summary

**Last Updated:** 2026-02-02  
**Audit Scope:** Full repository security review for public release

### Audit Results

✅ **PASSED** - No critical security vulnerabilities identified  
✅ **CodeQL Scan** - 0 alerts (clean)  
✅ **Manual Review** - Comprehensive source code and configuration review completed

---

## Security Measures in Place

### 1. Credential Protection

| Asset | Protection Method | Status |
|-------|------------------|--------|
| Google service account keys | `.gitignore`, Docker read-only mount | ✅ Protected |
| Anthropic API keys | `.env` file (gitignored), environment variables | ✅ Protected |
| Gmail credentials | Environment variables, never logged | ✅ Protected |
| Database files | `.gitignore`, not in Docker image | ✅ Protected |
| Bank export files | `.gitignore`, volume-mounted only | ✅ Protected |
| Configuration files | `.gitignore` (personal configs), examples use placeholders | ✅ Protected |

**Implementation Details:**
- All sensitive files are listed in `.gitignore` to prevent accidental commits
- Docker containers mount credentials directory as **read-only** (`:ro`)
- Environment variables are never logged or echoed to console
- API keys are only accessed from environment, never hardcoded

### 2. Data Protection

**Database Security:**
- SQLite database is gitignored and not included in Docker images
- All SQL queries use parameterized statements (no SQL injection risk)
- No bank account numbers (`qfx_acctid`) are stored in the database
- Sensitive account identifiers are not logged (as of v1.1)

**File Operations:**
- File paths use `pathlib.Path` for safe cross-platform handling
- No user-controlled file paths that could lead to path traversal
- File validation performed before processing (extension, completeness checks)
- File hash deduplication prevents processing malicious duplicate files

**API Integration:**
- Gmail API uses read-only scope (`gmail.readonly`)
- Google Sheets API access limited to designated spreadsheet
- Service account follows principle of least privilege
- Domain-wide delegation scoped to minimum required scopes
- Domain-wide delegation [chosen over per-user OAuth](SETUP.md#25-why-domain-wide-delegation-not-oauth) because the app runs as a headless daemon with no browser for interactive consent

### 3. Code Security

**No Critical Vulnerabilities:**
- ✅ No SQL injection (parameterized queries throughout)
- ✅ No command injection (no subprocess or os.system usage)
- ✅ No code execution vulnerabilities (no eval/exec/`__import__`)
- ✅ No insecure deserialization (no pickle/marshal)
- ✅ No path traversal vulnerabilities
- ✅ No hardcoded secrets in source code

**Safe Practices:**
- YAML loaded with `yaml.safe_load()` (not `yaml.load()`)
- All file operations use context managers (`with open()`)
- Exception handling does not expose sensitive data
- Logging statements sanitized to avoid leaking account numbers

### 4. Git History Protection

**Clean History Verified:**
- ✅ No `.env` files ever committed
- ✅ No database files (`.db`) ever committed  
- ✅ No bank files (`.qfx`, `.ofx`, `.csv`) ever committed
- ✅ No service account keys (`.json`) ever committed
- ✅ No personal configuration files ever committed
- ✅ Configuration examples use placeholder values only

**Ongoing Protection:**
- `.gitignore` comprehensively excludes all sensitive file patterns
- Pre-commit review recommended: `git diff --cached` before every commit
- Test fixtures use synthetic data only (no real account numbers or amounts)

### 5. Docker Security

**Container Hardening:**
- Credentials volume mounted read-only (`:ro`)
- Base image is official Python 3.12-slim (minimal attack surface)
- No exposed ports (application has no HTTP server)
- No privileged mode or elevated capabilities
- Application runs as root inside container (isolated environment)

**Best Practices:**
- `.dockerignore` excludes sensitive files from build context
- Documentation and test files excluded from production image
- Only necessary Python dependencies installed
- Build uses `--no-cache-dir` to reduce image size

### 6. Third-Party Dependencies

All external dependencies are from trusted sources:

| Package | Purpose | Security Notes |
|---------|---------|----------------|
| `anthropic` | Claude AI API client | Official Anthropic SDK |
| `gspread` | Google Sheets client | Established library, 8K+ stars |
| `google-auth` | Google authentication | Official Google library |
| `google-api-python-client` | Gmail API | Official Google library |
| `watchdog` | File system monitoring | 6K+ stars, actively maintained |
| `pyyaml` | YAML parsing | Uses `safe_load()` only |

**Dependency Management:**
- `pyproject.toml` specifies minimum version ranges (e.g., `>=3.0.0`) for broad
  compatibility. For production deployments requiring fully reproducible builds,
  generate a pinned lock file via `pip freeze > requirements.txt` after a
  successful install and install from that instead
- No known CVEs in current dependency versions
- Regular updates recommended via `pip list --outdated`

---

## Security Guidelines for Users

### Before Deploying

1. **Never commit personal configuration files**
   - The `.gitignore` is pre-configured to exclude all sensitive files
   - Do not use `git add -f` to force-add ignored files
   - Review `git diff --cached` before every commit
   - If you accidentally commit sensitive data, rewrite history with [git-filter-repo](https://github.com/newren/git-filter-repo)

2. **Review configuration examples**
   - Files in `config/examples/` contain only placeholder values
   - When creating `config/accounts.yaml`, you will enter real bank account IDs
   - These account IDs are used only during import and are never written to the database
   - Protect your config directory with appropriate file permissions

3. **Use a dedicated Google Cloud project**
   - Create a new GCP project specifically for MoMoney
   - Do not reuse projects with broad permissions or other services
   - Limit service account access to your finance spreadsheet and Gmail only

### During Deployment

4. **Enable volume encryption**
   - Synology DSM: Enable volume encryption to protect data at rest
   - Linux: Use LUKS, dm-crypt, or equivalent
   - This protects all data (database, configs, credentials, imports) if drives are stolen

5. **Restrict file permissions**
   - Set `chmod 600` on `finance.db`, `service-account.json`, and `.env`
   - Docker handles this automatically inside containers
   - Verify permissions if accessing files via SMB/NFS shares

6. **Do not expose to the internet**
   - MoMoney has no authentication layer
   - Run only on trusted local networks
   - Do not port-forward Docker or SSH to the public internet
   - Use a VPN for remote access

7. **Treat service account keys like passwords**
   - Store only in the `credentials/` directory (gitignored, Docker-mounted read-only)
   - Never copy to unsecured locations
   - Never include in screenshots, documentation, or support requests

### Ongoing Operations

8. **Rotate service account keys periodically**
   - Delete old keys from GCP Console after rotation
   - See SETUP.md Section 9.4 for step-by-step instructions

9. **Encrypt database backups**
   - The database contains your complete transaction history in plaintext
   - Always encrypt backups before copying to external storage or cloud services

10. **Monitor API usage**
    - Run `momoney status` periodically to check API request counts
    - Unexpected spikes may indicate a leaked API key

11. **Review Google Sheets sharing settings**
    - The spreadsheet should be shared only with your service account email
    - Verify no "anyone with the link" access is granted
    - The sheet contains your full transaction data

12. **Delete bank files after import**
    - Once QFX/CSV files are imported and verified, consider deleting them
    - Transaction data is persisted in SQLite
    - Keeping raw files increases sensitive data surface area

### Committing Changes to Your Instance

13. **Review diffs before committing**
    - Always run `git diff --cached` before `git commit`
    - Verify no `.yaml` config files, `.env`, or `.db` files are staged
    - The `.gitignore` is pre-configured to catch these, but double-check

---

## Threat Model

MoMoney is a **single-user, self-hosted application** running on a local NAS on a private network.

### In Scope Threats

- **Physical theft of NAS drives** → Mitigated by volume encryption
- **Unauthorized network access to NAS** → Mitigated by no internet exposure, VPN-only remote access
- **Accidental credential exposure via git** → Mitigated by comprehensive `.gitignore`, clean history verification

### Out of Scope

- Multi-user deployments (not supported)
- Multi-tenant deployments (not supported)
- Cloud-hosted deployments (not the intended use case)
- Malicious actors with root access to the NAS (full-disk encryption is your defense)

---

## Why No Field-Level Encryption?

The database does **not** encrypt individual fields (amounts, descriptions, etc.). This is a deliberate architectural decision:

1. **Breaks query functionality:** CLI commands need to query, sort, aggregate, and compute over transaction amounts and dates. Encrypting these fields would require decrypting the entire dataset into memory for every operation.

2. **Wrong layer of defense:** Field-level encryption protects against an attacker with database read access but not the decryption key. In a self-hosted deployment where the application and database run on the same machine, the key must also be on that machine—making field-level encryption ineffective.

3. **Data is mirrored to Google Sheets:** If Sheets sync is enabled, all transaction data is pushed to Google's servers unencrypted. Encrypting the local copy while the same data sits unencrypted in a Google Sheet would be inconsistent.

4. **Full-disk encryption is the correct control:** Synology DSM, TrueNAS, and Linux hosts all support volume/partition encryption, which protects all data at rest (database, config, credentials, imports) transparently and without breaking functionality.

**Recommendation:** Enable full-disk or volume-level encryption on your NAS. This is the industry-standard approach for protecting data at rest in self-hosted applications.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.1 | 2026-02-02 | Fixed sensitive ACCTID logging in warning messages |
| 1.0 | 2026-02-02 | Initial security audit and documentation |

