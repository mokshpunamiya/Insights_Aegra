# Eerly Studio Architecture

## Overview
Eerly Studio is a standalone package responsible for authentication and user management in the Aegra ecosystem. It is designed to be decoupled from Aegra's core logic to ensure independent evolvability.

## Directory Structure
The package follows the `libs/aegra-api` structure:

- **`src/eerly_studio/api`**: FastAPI routers and endpoints.
  - `auth.py`: Authentication routes (`/login`, `/register`, etc.).
- **`src/eerly_studio/core`**: Core infrastructure and configuration.
  - `config.py`: Application settings (Pydantic).
  - `database.py`: Database connection and session management.
  - `orm.py`: SQLAlchemy ORM models (`AppUser`, `AppSession`).
- **`src/eerly_studio/services`**: Business logic and utilities.
  - `auth.py`: JWT handling, password hashing, and authentication services.
- **`alembic/`**: Database migration scripts.

## Database
Eerly Studio uses its own database connection (`DATABASE_URL_AUTH`) and manages its schema via Alembic.

## Integration
Aegra mounts the `eerly_studio` FastAPI app to serve authentication endpoints. Aegra's core uses a lightweight `my_auth.py` handler to verify tokens issued by Eerly Studio without accessing the auth database directly.
