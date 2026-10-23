# Driftwatch design decisions

Each entry records what we chose, what else we considered, and why. Entries are
numbered so the README and the interview-prep index can point at them. Spec:
`docs/superpowers/specs/2026-10-23-driftwatch-design.md`.

Entry template:

    ## Dn. Title
    **Decision:** what we chose.
    **Alternatives:** what else we weighed.
    **Why:** the reasons, in order of weight.
    **Trade-offs:** what this costs us and when it would stop being right.

## D0. Tooling and test setup (Phase 0)

**Decision:** Python 3.12 managed by `uv` (virtualenv in `backend/.venv`); Postgres 16 in Docker Compose, exposed on host port 5433; tests run on the host against a separate `driftwatch_test` database that is dropped and recreated once per test session and truncated per DB test; pytest-asyncio runs the whole session on one event loop (`asyncio_default_*_loop_scope = session`).

**Alternatives:** testcontainers (a throwaway Postgres per run); SQLite for tests; one event loop per test.

**Why:**
- The queue relies on Postgres-only behaviour (`FOR UPDATE SKIP LOCKED`, `ON CONFLICT`, row locks), so tests must run on real Postgres. SQLite would pass tests that prove nothing.
- Reusing the compose `db` service means no extra tooling and the same Postgres you run the app on.
- An async engine owns a connection pool, and pooled connections are bound to the event loop that created them. With a loop per test the shared pool would be used from a loop that no longer exists; one session-wide loop avoids that.
- Port 5433 avoids colliding with a Postgres already installed on the host.

**Trade-offs:** tests need `docker compose up -d db` first. Tests share one loop, so a test that leaks a background task can affect the next one (we'll watch for this; see D11 once written).

**Lease rule is enforced in code, not just documented:** `Settings` refuses `lease_seconds < 2 * max_fetch_budget_seconds` and `heartbeat_seconds >= lease_seconds / 2`. Full rationale lands in D5 (Phase 1).
