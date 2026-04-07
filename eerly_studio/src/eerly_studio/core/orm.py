"""SQLAlchemy ORM setup for Eerly Studio.

This module creates:
• `Base` – the declarative base used by our auth models.
• `AppBase` - the declarative base used by our application models.
• `async_session_maker` – a factory that hands out `AsyncSession` objects.
• `get_session` – FastAPI dependency helper for routers.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime

from sqlalchemy import TIMESTAMP, Boolean, ForeignKey, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()
AppBase = declarative_base()


class AppUser(Base):
    __tablename__ = "app_users"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    email: Mapped[str | None] = mapped_column(String, unique=True, index=True)
    display_name: Mapped[str | None] = mapped_column(String)
    password_hash: Mapped[str] = mapped_column(String, nullable=False)
    org_id: Mapped[str | None] = mapped_column(String)
    permissions: Mapped[list[str]] = mapped_column(JSONB, server_default=text("'[]'::jsonb"))
    disabled_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("now()"))


class AppSession(Base):
    __tablename__ = "app_sessions"

    session_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("app_users.user_id", ondelete="CASCADE"), nullable=False)
    refresh_token_hash: Mapped[str] = mapped_column(String, index=True, nullable=False)
    user_agent: Mapped[str | None] = mapped_column(String)
    ip_address: Mapped[str | None] = mapped_column(String)
    is_valid: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    refresh_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("now()"))
    last_activity_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=text("now()"))
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

async_session_maker: async_sessionmaker[AsyncSession] | None = None
app_async_session_maker: async_sessionmaker[AsyncSession] | None = None


def _get_session_maker() -> async_sessionmaker[AsyncSession]:
    """Return a cached async_sessionmaker bound to db_manager.engine."""
    global async_session_maker
    if async_session_maker is None:
        from eerly_studio.core.database import db_manager

        engine = db_manager.get_engine()
        async_session_maker = async_sessionmaker(engine, expire_on_commit=False)
    return async_session_maker


def _get_app_session_maker() -> async_sessionmaker[AsyncSession]:
    """Return a cached async_sessionmaker bound to app_db_manager.engine."""
    global app_async_session_maker
    if app_async_session_maker is None:
        from eerly_studio.core.database import app_db_manager

        engine = app_db_manager.get_engine()
        app_async_session_maker = async_sessionmaker(engine, expire_on_commit=False)
    return app_async_session_maker


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency that yields an AsyncSession for Auth DB."""
    maker = _get_session_maker()
    async with maker() as session:
        yield session


async def get_app_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency that yields an AsyncSession for App DB."""
    maker = _get_app_session_maker()
    async with maker() as session:
        yield session
