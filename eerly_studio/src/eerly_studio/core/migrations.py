"""Database migration utilities for Eerly Studio.

Provides automatic Alembic migration support.
"""

import asyncio
from pathlib import Path

import structlog
from alembic import command
from alembic.config import Config

logger = structlog.get_logger(__name__)


def find_alembic_ini(filename: str = "alembic.ini") -> Path:
    """Find alembic.ini or alembic_app.ini file."""
    # 1. Look in workspace root (dev/docker) relative to this file
    # This file is in src/eerly_studio/core/migrations.py
    # alembic.ini is in eerly_studio/alembic.ini (root of package dir)

    current_dir = Path(__file__).resolve().parent
    # Go up: core -> eerly_studio -> src -> eerly_studio (package root)
    package_root = current_dir.parent.parent.parent

    ini_path = package_root / filename
    if ini_path.exists():
        return ini_path

    # Fallback to CWD if running from root
    cwd_ini = Path(filename).resolve()
    if cwd_ini.exists():
        return cwd_ini

    raise FileNotFoundError(f"Could not find {filename} in {package_root} or {cwd_ini}")


def get_alembic_config(filename: str = "alembic.ini") -> Config:
    """Create Alembic Config with correct paths."""
    ini_path = find_alembic_ini(filename)
    cfg = Config(str(ini_path))

    # Resolve script_location relative to ini_path
    script_location = cfg.get_main_option("script_location")
    if script_location and not Path(script_location).is_absolute():
        abs_script_location = str((ini_path.parent / script_location).resolve())
        cfg.set_main_option("script_location", abs_script_location)

    return cfg


def run_migrations() -> None:
    """Run all pending database migrations synchronously."""
    try:
        cfg = get_alembic_config("alembic.ini")
        logger.info("Running Eerly Studio Auth DB migrations...")
        command.upgrade(cfg, "head")

        cfg_app = get_alembic_config("alembic_app.ini")
        logger.info("Running Eerly Studio Application DB migrations...")
        command.upgrade(cfg_app, "head")

        logger.info("✅ Database migrations completed")
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        raise


async def run_migrations_async() -> None:
    """Run all pending database migrations (async-safe)."""
    await asyncio.to_thread(run_migrations)
