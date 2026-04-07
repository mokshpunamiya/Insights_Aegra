# CI/CD Integration

## Overview

Eerly Studio is integrated into the monorepo CI pipeline via three GitHub Actions workflows. This document describes how eerly_studio participates in each.

## Workflows

### 1. `ci.yml` — Lint & Unit/Integration Tests

**Lint job** runs `ruff format --check .` and `ruff check .` across the entire repo, **including** `eerly_studio/`. All eerly_studio source files must be formatted with `ruff format` before pushing.

**test-api job** runs aegra-api unit/integration tests. To prevent `aegra.json`'s auth config (`eerly_studio.my_auth:auth`) from leaking into unit tests, tests that mock auth loading must also mock `load_auth_config`:

```python
patch("aegra_api.core.auth_middleware.load_auth_config", return_value=None)
```

### 2. `ci-eerly-studio.yml` — Eerly Studio Tests

Runs only on changes to `eerly_studio/**`. Uses `uv sync --all-packages` for workspace-wide install, then runs:

```bash
uv run --package eerly-studio pytest eerly_studio/tests
```

Tests use `sqlite+aiosqlite:///:memory:` via `DATABASE_URL_AUTH_TEST` env var.

### 3. `e2e.yml` — End-to-End Tests

Since `aegra.json` references eerly_studio for both auth and custom HTTP routes:

```json
{
  "auth": { "path": "eerly_studio.my_auth:auth" },
  "http": { "app": "eerly_studio.main:app" }
}
```

The E2E workflow must:

1. **Install workspace-wide**: `uv sync --all-packages` (not just `libs/aegra-api`)
2. **Set PYTHONPATH**: Include `eerly_studio/src` so the module is importable:
   ```
   PYTHONPATH: ${{ github.workspace }}/examples:${{ github.workspace }}/eerly_studio/src
   ```
3. **Run eerly_studio migrations**: Separate Alembic step with `DATABASE_URL_AUTH` env var (eerly_studio uses `DATABASE_URL_AUTH` from its `config.py`, **not** individual `POSTGRES_*` vars)
4. **Set `DATABASE_URL_AUTH`** on the server start step too, so eerly_studio can connect at runtime
5. **AUTH_TYPE**: Set to `noop` for E2E tests (skips token verification)

## Pre-Push Checklist

Run these commands **from the repo root** before every push to ensure CI passes:

### Step 1: Format code
```bash
uvx ruff format eerly_studio/
```

### Step 2: Fix lint errors
```bash
uvx ruff check --fix eerly_studio/
```
If unsafe fixes are needed (e.g., removing unused variables):
```bash
uvx ruff check --fix --unsafe-fixes eerly_studio/
```

### Step 3: Re-format after lint fixes
Lint auto-fixes (especially import sorting) can break formatting. Always re-format:
```bash
uvx ruff format eerly_studio/
```

### Step 4: Verify both pass clean
```bash
uvx ruff format --check eerly_studio/
uvx ruff check eerly_studio/
```
Both must show **zero errors**.

### Step 5: Run eerly_studio tests
```bash
uv run --package eerly-studio pytest eerly_studio/tests -q
```

### Step 6: Run aegra-api tests (if you changed auth or config)
```bash
uv run --package aegra-api pytest libs/aegra-api/tests/unit libs/aegra-api/tests/integration -q
```

## Common Pitfalls

| Problem | Cause | Fix |
|---|---|---|
| `ruff check` fails with `I001` (unsorted imports) | New imports added without sorting | Run `ruff check --fix` |
| `ruff check` fails with `UP045`/`UP007` | Using `Optional[X]` or `Union[X, Y]` | Use `X \| None` or `X \| Y` instead |
| `ruff check` fails with `F401` (unused import) | Import added but not used | Remove it or run `ruff check --fix` |
| `ruff check` fails with `E712` (`== True`) | SQLAlchemy filter using `== True` | Use `.is_(True)` for SQLAlchemy columns |
| aegra-api auth tests fail | `aegra.json` auth config leaks into mocked tests | Mock `load_auth_config` (see Workflows §1) |
| E2E migration fails with "Can't locate revision" | aegra-api and eerly_studio share `alembic_version` table | eerly_studio uses `version_table="alembic_version_eerly_studio"` in `env.py` |
| E2E migration fails with auth error | `POSTGRES_*` vars set but eerly_studio uses `DATABASE_URL_AUTH` | Set `DATABASE_URL_AUTH` as a full connection string |

## Ruff Configuration

Config is inherited from the root `pyproject.toml`:
- `line-length = 120`
- `target-version = "py312"`
- Enabled rules: `E`, `W`, `F`, `I`, `B`, `C4`, `UP`, `ARG`, `SIM`
