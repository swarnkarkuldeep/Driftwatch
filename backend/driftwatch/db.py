from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from driftwatch.config import get_settings

_engine: AsyncEngine | None = None
_factory: async_sessionmaker[AsyncSession] | None = None


def init_engine(url: str | None = None) -> AsyncEngine:
    """Create the process-wide engine (and its connection pool). Tests call this with their own URL."""
    global _engine, _factory
    _engine = create_async_engine(url or get_settings().database_url, pool_size=10, max_overflow=10)
    _factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


def get_engine() -> AsyncEngine:
    return _engine if _engine is not None else init_engine()


def async_session() -> AsyncSession:
    """Use as `async with async_session() as s:`. The session returns its connection to the pool on exit."""
    get_engine()
    assert _factory is not None
    return _factory()
