from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://driftwatch:driftwatch@localhost:5433/driftwatch"

    # Queue lease: a claimed job is "owned" until lease_expires_at. The worker
    # extends it every heartbeat_seconds; if it dies, the lease runs out and the
    # reaper gives the job to someone else.
    lease_seconds: int = 120
    heartbeat_seconds: int = 15
    max_infra_attempts: int = 3

    # Fetching: one attempt is bounded by fetch_timeout_seconds; site failures
    # such as timeouts are retried max_site_retries times with doubling backoff.
    fetch_timeout_seconds: float = 15
    max_site_retries: int = 2
    retry_backoff_base_seconds: float = 1

    user_agent: str = "Driftwatch/0.1 (website monitor; student project)"

    @property
    def max_fetch_budget_seconds(self) -> float:
        """Worst-case wall time for one job's fetching, retries and backoff included."""
        attempts = self.max_site_retries + 1
        backoff = self.retry_backoff_base_seconds * (2**self.max_site_retries - 1)
        return attempts * self.fetch_timeout_seconds + backoff

    @model_validator(mode="after")
    def _check_lease_rule(self) -> "Settings":
        # The lease rule (DESIGN D5): a slow but healthy fetch must never look
        # like a dead worker, so the lease must comfortably exceed the fetch budget.
        if self.lease_seconds < 2 * self.max_fetch_budget_seconds:
            raise ValueError(
                f"lease_seconds ({self.lease_seconds}) must be at least 2x the max fetch "
                f"budget ({self.max_fetch_budget_seconds}s)"
            )
        # A worker must get several heartbeat chances before its lease expires.
        if self.heartbeat_seconds >= self.lease_seconds / 2:
            raise ValueError("heartbeat_seconds must be less than half of lease_seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
