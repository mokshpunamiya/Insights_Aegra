import structlog
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from .config import settings

logger = structlog.get_logger(__name__)


class DatabaseManager:
    """Manages database connections for Eerly Studio (Auth DB)."""

    def __init__(self) -> None:
        self.engine: AsyncEngine | None = None
        self._session_maker: async_sessionmaker[AsyncSession] | None = None
        self._database_url = settings.DATABASE_URL_AUTH or settings.DATABASE_URL

    async def initialize(self) -> None:
        """Initialize database connections."""
        if self.engine:
            return

        logger.info("Initializing Eerly Studio database connection...")
        self.engine = create_async_engine(
            self._database_url,
            echo=False,
            # Add pool settings here if needed later (matching Aegra)
        )

        self._session_maker = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        logger.info("✅ Eerly Studio database initialized")

    async def close(self) -> None:
        """Close database connections."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self._session_maker = None
            logger.info("✅ Eerly Studio database closed")

    def get_engine(self) -> AsyncEngine:
        if not self.engine:
            raise RuntimeError("Database not initialized")
        return self.engine

    async def get_session(self):
        """Dependency for FastAPI to get a DB session."""
        if not self._session_maker:
            raise RuntimeError("Database not initialized")

        async with self._session_maker() as session:
            try:
                yield session
                # Start a new transaction for the next request automatically?
                # sqlalchemy async sessions are usually per request.
            except Exception:
                await session.rollback()
                raise
            # Session is closed automatically by the context manager


db_manager = DatabaseManager()


class AppDatabaseManager:
    """Manages database connections for the Application DB mapping in Eerly Studio."""

    def __init__(self) -> None:
        self.engine: AsyncEngine | None = None
        self._session_maker: async_sessionmaker[AsyncSession] | None = None
        # Explicitly force this to use application db
        self._database_url = settings.DATABASE_URL

    async def initialize(self) -> None:
        """Initialize database connections."""
        if self.engine:
            return

        logger.info("Initializing Eerly Studio Application DB connection...")
        self.engine = create_async_engine(
            self._database_url,
            echo=False,
        )

        self._session_maker = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        logger.info("✅ Eerly Studio Application database initialized")

    async def close(self) -> None:
        """Close database connections."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self._session_maker = None
            logger.info("✅ Eerly Studio Application database closed")

    def get_engine(self) -> AsyncEngine:
        if not self.engine:
            raise RuntimeError("Database not initialized")
        return self.engine

    async def get_session(self):
        """Dependency for FastAPI to get a DB session."""
        if not self._session_maker:
            raise RuntimeError("Database not initialized")

        async with self._session_maker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise


app_db_manager = AppDatabaseManager()


# Alias for dependency injection (matches previous usage)
async def get_db():
    async for session in db_manager.get_session():
        yield session


async def get_app_db():
    async for session in app_db_manager.get_session():
        yield session
