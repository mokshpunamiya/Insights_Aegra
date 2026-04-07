# Eerly Studio Development Guide

## Database Management

Eerly Studio uses a dual-database approach to safely separate internal authentication tables from general application tables, while still allowing them to reside in the same physical database without Alembic conflicts.

### The Database Managers
To maintain consistency with Aegra (but remain decoupled), Eerly Studio uses Manager classes to handle database connections.

1. **Auth DB (Users, Sessions)**:
   - **Import**: `from eerly_studio.core.database import db_manager`
   - **Usage**: `db_manager.get_session()` (Async session generator for Auth models)
2. **App DB (Application-specific tables)**:
   - **Import**: `from eerly_studio.core.database import app_db_manager`
   - **Usage**: `app_db_manager.get_session()` (Async session generator for App models)

### Migrations
Database schema changes are managed by **Alembic**, split into two configurations to prevent version table conflicts.

- **Auth DB Location**: `eerly_studio/alembic/` (Config: `alembic.ini`)
- **App DB Location**: `eerly_studio/alembic_app/` (Config: `alembic_app.ini`)
- **Runner**: `eerly_studio/src/eerly_studio/core/migrations.py`

#### Workflow for Auth DB
1.  **Modify Models**: Update `src/eerly_studio/core/orm.py`.
2.  **Generate Migration**:
    ```bash
    uv run --package eerly-studio alembic revision --autogenerate -m "Description"
    ```
3.  **Apply Migrations**: `uv run --package eerly-studio alembic upgrade head`

#### Workflow for App DB

To add a new table to the Application Database, follow these steps:

1.  **Create the Model**: 
    Create a new python file in `src/eerly_studio/models/app_db/` (e.g., `src/eerly_studio/models/app_db/artifacts.py`).
    Make sure your model inherits from `AppBase`.
    
    ```python
    from sqlalchemy import Integer, String
    from sqlalchemy.orm import Mapped, mapped_column
    from eerly_studio.core.orm import AppBase

    class Artifact(AppBase):
        __tablename__ = "artifacts"
        
        id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
        name: Mapped[str] = mapped_column(String, index=True, nullable=False)
    ```

2.  **Register the Model**:
    Import your new model in `src/eerly_studio/models/app_db/__init__.py` so Alembic can discover it during autogeneration.
    
    ```python
    # src/eerly_studio/models/app_db/__init__.py
    from .artifacts import Artifact
    ```

3.  **Generate Migration**:
    Run Alembic pointing to the App DB configuration to generate the migration script.
    ```bash
    uv run --package eerly-studio alembic -c alembic_app.ini revision --autogenerate -m "Add artifacts table"
    ```

4.  **Apply Migrations**: 
    Apply the changes to your local database to create the table.
    ```bash
    uv run --package eerly-studio alembic -c alembic_app.ini upgrade head
    ```

> **Note**: Programmatically, the app runs both sets of migrations automatically on startup (via `migrations.py`).

## Docker Custom Dependencies

If you need to install system dependencies (like `ffmpeg`, `nodejs`, etc.) for your Eerly Studio application, you do not need to modify the `Dockerfile` directly. 

Instead, use the included shell script:
- **Location**: `deployments/docker/install_dependencies.sh`

This script is automatically copied and executed during the Docker build process. Simply add your `apt-get install` commands to this file. This approach keeps the `Dockerfile` clean and isolates your environment-specific requirements.

## Decoupling Principles
- **Do Not Import Aegra Core**: Re-implement patterns locally.
- **Maintain Independence**: Eerly Studio should be deployable standalone.

## Registration Protection (X-Api-Key)

The `/register` endpoint can be locked down so only clients with a valid API key can create users.

**Setup:**
1. Set `REGISTER_API_KEY` in your `.env` (or environment):
   ```
   REGISTER_API_KEY=my-super-secret-key
   ```
2. All registration requests must include the header:
   ```
   X-Api-Key: my-super-secret-key
   ```
3. Requests without the header (or with a wrong key) get `403 Forbidden`.

> If `REGISTER_API_KEY` is **not set**, registration remains open (local dev default).

## Code Quality & Linting

To maintain code quality and prevent errors (e.g., import order issues, unused variables), always run linting checks after making code changes or building new features.

To check for linting errors:
```bash
uvx ruff check .
```

To **automatically fix** many linting errors (like removing unused imports):
```bash
uvx ruff check . --fix
```

To format code according to standards:
```bash
uvx ruff format .
```

> **Note:** Always ensure `uvx ruff check .` in the root directory passes to comply with project standards before concluding changes.

