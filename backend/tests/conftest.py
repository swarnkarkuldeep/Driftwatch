import os

import asyncpg
import pytest
from sqlalchemy import text

from driftwatch import db

ADMIN_DSN = os.environ.get(
    "TEST_ADMIN_DSN", "postgresql://driftwatch:driftwatch@localhost:5433/postgres"
)
TEST_DB_NAME = "driftwatch_test"
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    f"postgresql+asyncpg://driftwatch:driftwatch@localhost:5433/{TEST_DB_NAME}",
)


@pytest.fixture(scope="session")
async def test_db():
    """Create a fresh, empty test database once per test session."""
    admin = await asyncpg.connect(ADMIN_DSN)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{TEST_DB_NAME}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{TEST_DB_NAME}"')
    finally:
        await admin.close()
    db.init_engine(TEST_DATABASE_URL)
    yield TEST_DATABASE_URL
    await db.get_engine().dispose()


@pytest.fixture
async def clean_db(test_db):
    """Empty every table (except alembic's own). Only DB tests pay for this; pure unit tests never touch Postgres."""
    async with db.async_session() as s:
        rows = await s.execute(
            text(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename <> 'alembic_version'"
            )
        )
        tables = [r[0] for r in rows]
        if tables:
            names = ", ".join(f'"{t}"' for t in tables)
            await s.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
        await s.commit()


@pytest.fixture
async def session(clean_db):
    async with db.async_session() as s:
        yield s
