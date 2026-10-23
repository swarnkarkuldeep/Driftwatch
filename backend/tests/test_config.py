import pytest
from pydantic import ValidationError
from sqlalchemy import text

from driftwatch.config import Settings


def test_defaults():
    s = Settings()
    assert s.lease_seconds == 120
    assert s.heartbeat_seconds == 15
    assert s.max_infra_attempts == 3


def test_fetch_budget_covers_all_attempts_and_backoff():
    # 3 attempts x 15s timeout + backoff of 1s then 2s between them.
    s = Settings(fetch_timeout_seconds=15, max_site_retries=2, retry_backoff_base_seconds=1)
    assert s.max_fetch_budget_seconds == 3 * 15 + 1 + 2


def test_rejects_lease_shorter_than_fetch_budget():
    with pytest.raises(ValidationError):
        Settings(lease_seconds=30, fetch_timeout_seconds=15)


def test_heartbeat_must_be_less_than_half_lease():
    with pytest.raises(ValidationError):
        Settings(lease_seconds=120, heartbeat_seconds=60)


async def test_db_roundtrip(session):
    result = await session.execute(text("SELECT 1"))
    assert result.scalar_one() == 1
