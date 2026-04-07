import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from eerly_studio.api.auth import router
from eerly_studio.core.database import db_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB

    if not os.getenv("TESTING"):
        from eerly_studio.core.migrations import run_migrations_async

        await run_migrations_async()

        await db_manager.initialize()
    yield
    # Close DB
    await db_manager.close()


app = FastAPI(title="Eerly Studio Auth Service", lifespan=lifespan)

app.include_router(router)
