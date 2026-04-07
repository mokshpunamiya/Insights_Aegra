
# Eerly Studio Migration Guide

This guide explains how to manage database schema changes using Alembic with the `orm.py` pattern.

## 1. Creating a New Migration (Adding a Table)

To add a new table or modify an existing one:

### Step 1: Define Your Model
Define your SQLAlchemy model in `eerly_studio/src/eerly_studio/core/orm.py`.

Example (`eerly_studio/src/eerly_studio/core/orm.py`):
```python
from sqlalchemy import Column, Integer, String
# Base is already defined in orm.py

class MyNewTable(Base):
    __tablename__ = "my_new_table"
    id = Column(Integer, primary_key=True)
    name = Column(String)
```

### Step 2: Generate Migration Script

Run this command from your terminal (outside Docker):

**Shell / Bash:**
```bash
export DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/aegra"
uv run alembic -c eerly_studio/alembic.ini revision --autogenerate -m "Add my_new_table"
```

**PowerShell:**
```powershell
$env:DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/aegra"
uv run alembic -c eerly_studio/alembic.ini revision --autogenerate -m "Add my_new_table"
```

### Step 3: Verify the Script
Check the generated file in `eerly_studio/alembic/versions/`. Ensure the `upgrade()` and `downgrade()` functions look correct.

## 2. Applying Migrations

Migrations are **automatically applied** when the server starts.

To apply them manually locally:
```bash
# Using uv (local)
$env:DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/aegra"
uv run alembic -c eerly_studio/alembic.ini upgrade head
```

To apply them inside Docker without restarting:
```bash
docker-compose exec aegra alembic -c eerly_studio/alembic.ini upgrade head
```

## 3. Resolving "No Migrations" Confusion
If you see migration logs but no changes in the DB, it usually means:
1. You haven't generated a migration script yet (step 2 above).
2. Your models are not imported/registered in `env.py`.
