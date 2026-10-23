# Driftwatch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Driftwatch, a single-user website crawler and monitor whose Postgres job queue, crash recovery, politeness and change detection are each proven by tests.

**Architecture:** One job is one URL fetch, stored in Postgres. Workers claim jobs with a two-step `FOR UPDATE SKIP LOCKED` query (reserve a per-domain time slot, then claim a job of that domain), hold a heartbeat lease, and enqueue discovered links in the same transaction that completes the job. A scheduler loop and a sweeper run inside the worker process. FastAPI serves a React dashboard and an SSE progress stream.

**Tech Stack:** Python 3.12, FastAPI, httpx, selectolax, PostgreSQL + SQLAlchemy 2.0 async + Alembic, pytest + pytest-asyncio, React + TypeScript + Vite + Tailwind, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-23-driftwatch-design.md` (read it first; section numbers below refer to it).

## Current state (update at the end of every task and phase)

- **Execution:** native (the implementer does every task in-session; one fresh reviewer on the whole branch at the end). New sessions are expected every phase or two: resume from this file and `docs/DESIGN.md` alone.
- **Done:** Phase 0 (Task 0.1: scaffold, settings with lease rule, test DB; 5 tests pass).
- **Next:** Phase 1, Task 1.1 (core schema and migration). Alembic is not initialised yet: Task 1.1 runs `alembic init` (async template), creates `alembic.ini` and `alembic/env.py`, then the first migration.
- **Environment facts:** branch `build/driftwatch` (not main). Python 3.12 via `uv` (`cd backend && uv sync --python 3.12`; run tests with `uv run pytest`). The machine's default Python is 3.14, so never use bare `python` for project code. Postgres: `docker compose up -d db` (host port 5433); Docker Desktop must be running (start `C:\Users\kulde\AppData\Local\Programs\DockerDesktop\Docker Desktop.exe` if the daemon is down). Tests use DB `driftwatch_test`. `db.py` exposes `init_engine(url)`, `get_engine()`, `async_session()`; `conftest.py` fixtures: `test_db`, `clean_db`, `session` (pure unit tests do not touch the DB).
- **Open decisions:** none. Visual direction for the dashboard is chosen with the owner at Task 10.1.
- **Session handoff checklist:** tick the step checkboxes below as they pass; update Done/Next here; add the phase's DESIGN.md entries; give the owner 3-5 interviewer questions at the end of each phase.

## Global Constraints

- Python 3.12; async throughout (no blocking I/O in the event loop).
- Queue is Postgres `SELECT ... FOR UPDATE SKIP LOCKED`. No Redis, no Celery.
- Single user: no auth, accounts, billing, multi-tenancy.
- One job = one URL fetch. Dedup is `UNIQUE (crawl_id, normalized_url)` with `INSERT ... ON CONFLICT DO NOTHING`.
- Only the database clock (`now()`) is used for slots, leases and expiry.
- Correctness never depends on in-memory state shared between workers.
- Site failures: 404/410/DNS/connection-refused/SSL = definitive, no retry. Timeout/500/502/504 = 2 retries with backoff; retry success = `intermittent`. 429/503 = back off domain, re-queue, finding only after the retry budget.
- Infrastructure failures: lease expiry re-queues; `infra_attempts` cap 3, then `dead` (terminal, shown separately from findings).
- **Lease rule:** `lease_seconds` must be comfortably longer than the max fetch time including retries and backoff. `Settings` must refuse to start if `lease_seconds < 2 * max_fetch_budget_seconds`.
- Redirects: `follow_redirects` off; manual hops; chains longer than 2 hops and loops are findings.
- Internal = same `host:port` (netloc) as the seed, plus the netloc of the seed's final redirect target. External links are checked once per crawl (HEAD, fall back to GET) and never parsed.
- Defaults: heartbeat 15s, lease 120s, per-attempt timeout 15s, max URL length 2000, max depth 10, max pages 500 per crawl, max body 5 MB.
- Honest, configurable User-Agent.
- **Commits (owner-authorized):** commit once at the end of each phase, after all that phase's tests pass, with a conventional message (`feat:`, `test:`, `docs:`). Wherever a task says "Commit (ask owner)", read it as "stage for the phase commit". Anything else involving git (rebase, force-push, branch deletion, history rewrites) still needs the owner's explicit approval.
- Each phase ends with a stop for owner review. Every phase adds the DESIGN.md entries listed under it.
- Dashboard work uses the `frontend-design:frontend-design` skill.

## Review Focus

Inputs and conditions the spec implies but no obvious task exercises. Each has a test in the task named in brackets.

1. robots.txt `Disallow: /` for everything: crawl records `skipped_robots` jobs and still finishes, no hang [Task 5.2].
2. Seed URL redirects to another host (apex to www) or the seed itself is 404 or unreachable: crawl completes with a finding, internal host set is correct [Task 5.2].
3. Linked non-HTML files (PDF, image) and huge bodies: not parsed, body capped at 5 MB, no memory blowup [Task 4.2].
4. Malformed HTML and non-UTF-8 charsets: parser returns a result, never raises [Task 5.1].
5. `mailto:`, `tel:`, `javascript:`, `<base href>`, empty and fragment-only hrefs: ignored or resolved correctly, never enqueued as junk [Task 3.3].

## File Structure

```
docker-compose.yml          services: db, migrate, api, worker, fixture, frontend
backend/
  pyproject.toml  Dockerfile  alembic.ini  alembic/versions/
  driftwatch/
    config.py db.py models.py __main__.py(CLI)
    queue/    enqueue.py claim.py lease.py reaper.py completion.py
    crawl/    normalize.py traps.py links.py fetcher.py classify.py robots.py politeness.py parser.py worker.py
    analysis/ seo.py findings.py perf.py ssl_check.py content.py compare.py health.py
    scheduler.py  concurrency.py  debug.py
    api/      main.py routes.py sse.py schemas.py
    demos/    queue_demo.py leak_demo.py hang_demo.py
  tests/
fixture_site/  app.py expected.py __main__.py
bench/         run.py
frontend/      (Vite + React + TS + Tailwind)
docs/          DESIGN.md  concurrency-notes.md
```

---

# Phase 0: Foundation

Stop and review: `docker compose up -d db` then `pytest` passes.
DESIGN entries: none yet; create the DESIGN.md skeleton (entry template: Decision, Alternatives, Why, Trade-offs).

### Task 0.1: Scaffold, settings, test database

**Files:** Create `backend/pyproject.toml`, `backend/driftwatch/{__init__,config,db}.py`, `backend/tests/conftest.py`, `backend/tests/test_config.py`, `docker-compose.yml` (db only), `.gitignore`, `README.md` (stub), `docs/DESIGN.md`.

**Interfaces:**
- Produces: `Settings` (pydantic-settings) with fields `database_url`, `lease_seconds=120`, `heartbeat_seconds=15`, `fetch_timeout_seconds=15`, `max_site_retries=2`, `max_infra_attempts=3`, `max_fetch_budget_seconds` (property: attempts × timeout + total backoff), `user_agent`; `get_settings() -> Settings`. `db.py`: `engine`, `async_session() -> AsyncSession` context manager. `conftest.py`: session-scoped `test_db` (creates `driftwatch_test`), function-scoped `session` and `clean_db` (truncates tables).

- [x] **Step 1: Write failing tests** in `test_config.py`: `test_defaults` (lease 120, heartbeat 15, infra cap 3); `test_rejects_lease_shorter_than_fetch_budget` (`Settings(lease_seconds=30, fetch_timeout_seconds=15)` raises `ValidationError`); `test_heartbeat_must_be_less_than_half_lease`; `test_db_roundtrip` (`SELECT 1` through `async_session()` returns 1).
- [x] **Step 2:** `cd backend && pytest -q` expect FAIL (modules missing).
- [x] **Step 3: Implement** config validator for the lease rule and `db.py` per the interfaces; pytest `asyncio_mode = auto`; compose `db` service Postgres 16 with a healthcheck.
- [x] **Step 4:** `docker compose up -d db && pytest -q` expect PASS.
- [x] **Step 5: Commit** (ask owner) `chore: scaffold backend, settings with lease rule`.

---

# Phase 1: Schema and queue (the riskiest part, no crawling yet)

Stop and review: run `python -m driftwatch.demos.queue_demo --jobs 200 --workers 8 --kill-after 3` and watch a worker "crash", its jobs re-queued by the reaper, and 200 jobs finish with 0 duplicates. All tests in `tests/queue/` pass.
DESIGN entries: D1 Postgres instead of Redis/Celery; D2 job = one URL fetch; D3 two-step claim and domain slot; D4 dedup in the database; D5 lease, heartbeat, fencing and the lease rule; D6 dead-letter; D7 crawl completion and sweeper.
Teaching: `docs/concurrency-notes.md` part 1 (event loop, tasks, `gather`, why `await` is the only place switching happens).

### Task 1.1: Core schema and migration

**Files:** Create `backend/driftwatch/models.py`, `alembic/versions/0001_core.py`, `tests/queue/test_schema.py`.

**Interfaces:** Produces enums `JobKind(page|external)`, `JobStatus(pending|running|done|failed|dead|skipped)`, `CrawlStatus(running|finished|failed)`; models `Site`, `Crawl`, `Domain(host, next_allowed_fetch_at, delay_seconds)`, `Job(crawl_id, domain_id, url, normalized_url, path_key, kind, depth, status, available_at, lease_expires_at, worker_id, infra_attempts, site_attempts, claimed_at, finished_at)`. Constraints: `UNIQUE(crawl_id, normalized_url)`; partial unique index on `crawls(site_id) WHERE status='running'`; index on `jobs(domain_id, available_at) WHERE status='pending'`.

- [ ] **Step 1:** tests `test_duplicate_normalized_url_rejected` (raw second insert raises `IntegrityError`); `test_one_running_crawl_per_site` (second running crawl for same site raises); `test_migration_upgrade_downgrade` (alembic up then down leaves no tables).
- [ ] **Step 2:** run, expect FAIL. **Step 3:** implement models and migration. **Step 4:** run, expect PASS. **Step 5:** Commit (ask owner).

### Task 1.2: Enqueue with database dedup

**Files:** Create `driftwatch/queue/enqueue.py`, `tests/queue/test_enqueue.py`.

**Interfaces:** Produces `get_or_create_domain(session, host: str, default_delay: float) -> int`; `enqueue_job(session, *, crawl_id: int, domain_id: int, url: str, normalized_url: str, kind: JobKind, depth: int, path_key: str) -> int | None` (None on duplicate).

- [ ] **Step 1:** tests `test_duplicate_returns_none`; `test_concurrent_enqueue_same_url_one_row` (50 tasks, each with its own session, same URL: exactly one non-None result and one row); `test_get_or_create_domain_concurrent` (20 tasks, one row, same id).
- [ ] **Step 2-4:** fail, implement with `insert(...).on_conflict_do_nothing(...).returning(Job.id)` and `ON CONFLICT` upsert for domains, pass. **Step 5:** Commit (ask owner).

### Task 1.3: Two-step claim (domain slot, then job)

**Files:** Create `driftwatch/queue/claim.py`, `tests/queue/test_claim.py`.

**Interfaces:** Produces `ClaimedJob(id, crawl_id, domain_id, url, kind, depth, site_attempts)` and `claim_job(session, *, worker_id: str, lease_seconds: int) -> ClaimedJob | None`. One transaction: step 1 reserves the due domain (spec 5.2), step 2 claims one due pending job of it (`available_at <= now()`), sets `status=running`, `lease_expires_at`, `claimed_at`; if no job, roll back so the slot is not spent.

- [ ] **Step 1:** tests: `test_returns_none_when_empty`; `test_no_double_claim_under_concurrency` (200 jobs over 20 domains, delay 0, 16 concurrent claimers loop until None: every job claimed exactly once); `test_domain_slot_spacing` (one domain, delay 0.2s, 5 jobs, 5 concurrent claimers: sorted `claimed_at` gaps all >= 0.18s); `test_fairness` (domain A 100 jobs, domain B 2 jobs: both B jobs are among the first 4 claims); `test_slot_not_spent_without_job` (domain with only future `available_at` jobs keeps its `next_allowed_fetch_at`); `test_claim_respects_available_at`.
- [ ] **Step 2-4:** fail, implement the two statements, pass. **Step 5:** Commit (ask owner).

### Task 1.4: Heartbeat, complete, reschedule (with fencing)

**Files:** Create `driftwatch/queue/lease.py`, `tests/queue/test_lease.py`.

**Interfaces:** Produces `NewJob(url, normalized_url, kind, depth, path_key, domain_host)`; `heartbeat(session, job_id: int, worker_id: str, lease_seconds: int) -> bool`; `complete_job(session, job_id: int, worker_id: str, *, status: JobStatus, discovered: Sequence[NewJob] = ()) -> bool`; `reschedule_job(session, job_id: int, worker_id: str, *, delay_seconds: float) -> bool` (back to pending, `site_attempts + 1`). All three return False when the job is not `running` under that `worker_id` (fencing).

- [ ] **Step 1:** tests: `test_heartbeat_extends_lease`; `test_heartbeat_false_after_lease_lost`; `test_zombie_cannot_complete_requeued_job` (worker A claims, lease expires, reaper requeues, B claims; A's `complete_job` returns False and B's still works); `test_complete_is_atomic_with_children` (raise after the statements but before commit: job still `running`, no children); `test_children_deduped_on_complete`.
- [ ] **Step 2-4:** fail, implement (all conditional `UPDATE ... WHERE id AND worker_id AND status='running'`), pass. **Step 5:** Commit (ask owner).

### Task 1.5: Reaper (lease expiry, dead-letter)

**Files:** Create `driftwatch/queue/reaper.py`, `tests/queue/test_reaper.py`.

**Interfaces:** Produces `ReapResult(requeued: int, dead: int)` and `reap_expired(session, *, max_infra_attempts: int) -> ReapResult` using `FOR UPDATE SKIP LOCKED` on expired running jobs.

- [ ] **Step 1:** tests: `test_expired_requeued_with_attempt_incremented`; `test_dead_at_cap` (`infra_attempts` reaches 3 then status `dead`); `test_unexpired_untouched`; `test_two_concurrent_reapers_requeue_once` (attempt incremented exactly once).
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 1.6: Crawl completion and sweeper

**Files:** Create `driftwatch/queue/completion.py`, `tests/queue/test_completion.py`.

**Interfaces:** Produces `finalize_if_idle(session, crawl_id: int) -> bool` and `finalize_idle_crawls(session) -> list[int]`, using `UPDATE crawls SET status='finished', finished_at=now() WHERE id=? AND status='running' AND NOT EXISTS (open jobs)`. Open = `pending` or `running`.

- [ ] **Step 1:** tests: `test_not_finalized_with_open_jobs`; `test_terminal_states_all_count` (done, failed, dead, skipped); `test_exactly_one_finalizer` (10 concurrent `finalize_if_idle`: exactly one True); `test_concurrent_last_two_completions_then_sweep` (two workers complete the last two jobs simultaneously; after `finalize_idle_crawls` the crawl is finished once).
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 1.7: Queue demo and concurrency primer part 1

**Files:** Create `driftwatch/demos/queue_demo.py`, `docs/concurrency-notes.md`, `tests/queue/test_demo.py`.

**Interfaces:** Consumes Tasks 1.2-1.6. Produces `async def run_demo(jobs: int, workers: int, kill_after: float | None, lease_seconds: int = 3) -> DemoSummary(done: int, duplicates: int, requeued: int)`. Workers are async loops with a fake handler (`await asyncio.sleep(random)`); the "crash" cancels one worker task without releasing its job.

- [ ] **Step 1:** test `test_demo_completes_despite_crashed_worker` (jobs=30, workers=4, kill_after=0.5: `done == 30`, `duplicates == 0`, `requeued >= 1`).
- [ ] **Step 2-4:** fail, implement demo with a per-second progress line and `python -m` entry; write notes part 1; pass. **Step 5:** Commit (ask owner).

---

# Phase 2: Fixture site

Stop and review: `python -m fixture_site --port 8800`, browse it, open `/__expected` and see the exact list of known problems. `pytest tests/test_fixture_site.py` passes.
DESIGN entries: D8 fixture-driven testing (expected manifest as the oracle).

### Task 2.1: Fixture site with expected-findings manifest

**Files:** Create `fixture_site/{app.py,expected.py,__main__.py}`, `backend/tests/test_fixture_site.py`; modify `backend/tests/conftest.py`.

**Interfaces:** Produces a Starlette app via `create_app(crawl_delay: float = 0) -> Starlette`; `conftest` fixtures `fixture_server` (runs uvicorn in a thread on a free port, returns base URL) and `external_server` (a second instance, different port, for external-link tests). `expected.py` exports `EXPECTED` (dict: broken internal links, broken external links, redirect chain pages, loop pages, intermittent pages, SEO issues per page, noindex pages, duplicate-title groups, changing and noise-only pages) and `/__expected` serves it. Also `/__reset` (reset counters) and `/__mutate?fix=<key>` (fix one known problem, for compare tests), `/__stats` (request arrival timestamps per host, for rate-limit checks).
Routes (all specified in spec section 7, plus): `/redirect/chain/1` to `/redirect/chain/4` (3 hops) then `/ok`; `/redirect/loop/a` and `/b`; `/slow?ms=N`; `/calendar/{year}/{month}` with prev/next links forever; `/dup?a=1&b=2` and the reordered and `utm_` variants; `/seo/*` pages (missing title, duplicate title, missing meta description, missing h1, image without alt, noindex, broken canonical); `/changing` (real change per request after `/__mutate`), `/noise` (only a timestamp and an inline script differ per request); `/flaky` (500 twice then 200); `/ratelimited` (429 once then 200); `/robots.txt` with `Disallow: /private/` and configurable crawl-delay; `/file.pdf`; `/big` (6 MB); a Latin-1 page; a malformed-HTML page; a page with `mailto:`, `tel:`, `javascript:` and `<base href>` links.

- [ ] **Step 1:** tests (using httpx): `test_chain_has_three_hops`; `test_loop_never_terminates_on_its_own` (follow up to 10 hops, still redirecting); `test_calendar_links_to_next_month`; `test_flaky_fails_twice_then_succeeds_and_resets`; `test_noise_page_differs_only_by_timestamp`; `test_expected_manifest_lists_every_route_with_a_known_problem`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

---

# Phase 3: URL handling (pure logic, heavily tested)

Stop and review: `pytest tests/crawl/test_normalize.py tests/crawl/test_traps.py tests/crawl/test_links.py -v` and read the parametrized cases; they are the spec of the rules.
DESIGN entries: D9 URL normalization rules and the trailing-slash trade-off; D10 crawl-trap protection and why query-variant counting uses the database.

### Task 3.1: URL normalization

**Files:** Create `driftwatch/crawl/normalize.py`, `tests/crawl/test_normalize.py`.

**Interfaces:** Produces `normalize_url(url: str, *, base: str | None = None) -> str | None` (None for non-http(s) or unparseable) and `path_key(normalized_url: str) -> str` (host + path, no query).

- [ ] **Step 1:** parametrized tests: `HTTP://Example.COM/A` becomes `http://example.com/A`; default ports dropped; fragment removed; query sorted (`?b=2&a=1` to `?a=1&b=2`); `utm_*`, `gclid`, `fbclid` stripped, and a lone stripped param leaves no trailing `?`; `/about/` equals `/about` but `/` stays `/`; relative URL resolved against `base`; `mailto:x`, `javascript:void(0)`, `tel:1` return None; percent-encoding normalized; idempotence (`normalize(normalize(u)) == normalize(u)`) over all cases.
- [ ] **Step 2-4:** fail, implement with `urllib.parse`, pass. **Step 5:** Commit (ask owner).

### Task 3.2: Trap guard and admission

**Files:** Create `driftwatch/crawl/traps.py`, `tests/crawl/test_traps.py`.

**Interfaces:** Produces `TrapReason(enum: too_deep|too_long|repeating_segments|query_variants|page_cap)`; `check_static(normalized_url: str, depth: int, *, max_depth: int, max_url_length: int) -> TrapReason | None` (pure: depth, length, repeating segments such as `a/b/a/b/a/b` or one segment 3+ times); `async admit(session, *, crawl_id: int, normalized_url: str, depth: int, settings) -> TrapReason | None` (adds DB checks: more than 20 distinct query strings for one `path_key`, or the crawl already has `max_pages` jobs; the page-cap case sets `crawls.capped = true`).

- [ ] **Step 1:** tests: `test_depth_limit`; `test_url_length_limit`; `test_repeating_segment_pairs_detected`; `test_calendar_trap_stopped` (enqueue `/calendar/2020/01` onward, admission rejects by the time the page cap or repeating-pattern rule fires, bounded number of rows); `test_query_variant_cap` (21st distinct query on the same path rejected, same query again is just a dedup); `test_page_cap_marks_crawl_capped`.
- [ ] **Step 2-4:** fail, implement (add `capped` column via migration 0002), pass. **Step 5:** Commit (ask owner).

### Task 3.3: Link extraction and scope

**Files:** Create `driftwatch/crawl/links.py`, `tests/crawl/test_links.py`.

**Interfaces:** Produces `extract_links(html: str, base_url: str) -> list[str]` (absolute normalized candidates from `<a href>`, honoring `<base href>`) and `is_internal(normalized_url: str, internal_netlocs: set[str]) -> bool`.

- [ ] **Step 1:** tests (Review Focus 5): `test_ignores_mailto_tel_javascript_and_empty_and_fragment_only`; `test_base_href_changes_resolution`; `test_relative_paths_resolved`; `test_duplicate_links_collapsed`; `test_is_internal_uses_host_and_port` (`127.0.0.1:8800` vs `127.0.0.1:8801` are different sites).
- [ ] **Step 2-4:** fail, implement with selectolax, pass. **Step 5:** Commit (ask owner).

---

# Phase 4: Fetching, politeness, concurrency

Stop and review: `python -m driftwatch.demos.leak_demo` and `python -m driftwatch.demos.hang_demo` (read their printed task dumps), then `pytest tests/crawl/test_fetcher.py tests/crawl/test_classify.py tests/crawl/test_politeness.py -v`.
DESIGN entries: D11 concurrency model (N worker loops plus a semaphore on in-flight HTTP, one shared client, timeouts everywhere, why not threads); D12 politeness (robots.txt caching, crawl-delay, backoff, slow-response backoff, honest UA); D13 failure taxonomy (spec 5.6).
Teaching: `docs/concurrency-notes.md` part 2 (semaphores, connection pools, timeouts, leaks, hangs, how to read a task dump).

### Task 4.1: Concurrency helpers and the leak and hang demos

**Files:** Create `driftwatch/concurrency.py`, `driftwatch/debug.py`, `driftwatch/demos/{leak_demo,hang_demo}.py`, `tests/test_concurrency.py`.

**Interfaces:** Produces `bounded_gather(coros: Iterable[Awaitable[T]], limit: int) -> list[T]` (semaphore-bounded, preserves order, one failure does not orphan siblings: cancels them and re-raises); `dump_tasks() -> list[str]` (name, coroutine, awaiting-where for every task in the loop).

- [ ] **Step 1:** tests: `test_bounded_gather_never_exceeds_limit` (shared counter records peak == limit); `test_bounded_gather_preserves_order`; `test_failure_cancels_siblings` (no task left pending); `test_dump_tasks_shows_stuck_task` (a task awaiting an Event that is never set appears in the dump with its coroutine name).
- [ ] **Step 2-4:** fail, implement, pass; leak demo opens clients without closing and prints open-socket count versus the fixed version (`async with`); hang demo awaits without a timeout, then uses `asyncio.timeout` and prints `dump_tasks()`. **Step 5:** Commit (ask owner).

### Task 4.2: Fetcher

**Files:** Create `driftwatch/crawl/fetcher.py`, `tests/crawl/test_fetcher.py`.

**Interfaces:** Produces `FetchError(enum: dns|refused|ssl|timeout|too_many_redirects|redirect_loop|too_large)`; `Hop(url: str, status: int)`; `FetchResult(final_url: str, status_code: int | None, chain: list[Hop], error: FetchError | None, elapsed_ms: float, size_bytes: int, body: bytes | None, content_type: str | None)`; `async fetch(client: httpx.AsyncClient, url: str, *, method: str = "GET", max_hops: int = 10, max_body_bytes: int = 5_000_000) -> FetchResult`. Streams the body; reads the body only for `text/html`; manual redirects; loop = a URL repeated within one chain.

- [ ] **Step 1:** tests against the fixture: `test_chain_of_three_hops_recorded`; `test_redirect_loop_detected`; `test_404_status`; `test_timeout_maps_to_timeout_error` (`/slow?ms=3000`, timeout 0.5s); `test_connection_refused` (closed port); `test_ssl_error_mapped` (httpx `MockTransport` raising `ConnectError` with an `ssl.SSLError` cause); `test_pdf_body_not_read` (Review Focus 3: `body is None`, status 200); `test_big_body_capped` (`/big`: error `too_large`, bytes read at most the cap plus one chunk); `test_client_closed_no_leak` (open-connection count of the pool is 0 after the context exits).
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 4.3: Outcome classification

**Files:** Create `driftwatch/crawl/classify.py`, `tests/crawl/test_classify.py`.

**Interfaces:** Produces `Action(enum: ok|record_broken|retry|polite_backoff)`; `Classification(action: Action, outcome: str)` where outcome is one of `ok|intermittent|broken|skipped`; `classify(result: FetchResult, *, site_attempts: int, max_site_retries: int = 2) -> Classification` (pure, table-driven per spec 5.6; `intermittent` is decided by the caller when an `ok` follows `site_attempts > 0`, exposed as `final_outcome(first_failed: bool, ok: bool) -> str`).

- [ ] **Step 1:** parametrized tests, one row per rule: 404, 410, DNS, refused, SSL give `record_broken` immediately; timeout/500/502/504 give `retry` while attempts remain then `record_broken`; 429 and 503 give `polite_backoff` then `record_broken` after the budget; 200 gives `ok`; retry success gives `intermittent`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 4.4: Robots and per-domain politeness

**Files:** Create `driftwatch/crawl/robots.py`, `driftwatch/crawl/politeness.py`, `tests/crawl/test_robots.py`, `tests/crawl/test_politeness.py`; migration 0003 adds `domains.robots_txt`, `robots_fetched_at`, `robots_status`.

**Interfaces:** Produces `RobotsRules.parse(text: str, user_agent: str) -> RobotsRules` with `.allowed(path: str) -> bool` and `.crawl_delay: float | None`; `async load_rules(session, client, domain_id: int, host_origin: str, settings) -> RobotsRules` (cache in the DB with a 24h TTL; 404 means allow all; 5xx/timeout retried, allowed only after the retry budget, per spec 10a item 4); `async apply_backoff(session, domain_id: int, *, factor: float = 2.0, cap: float = 300.0) -> None` (doubles `delay_seconds`, pushes `next_allowed_fetch_at`); `async relax(session, domain_id: int, base_delay: float) -> None` (halves toward the base); `async set_delay_from_robots(session, domain_id: int, rules: RobotsRules, floor: float) -> None`.

- [ ] **Step 1:** tests: `test_disallow_prefix_blocks_path`; `test_disallow_all_blocks_everything` (Review Focus 1 rules level); `test_crawl_delay_parsed`; `test_404_allows_all`; `test_cache_hit_does_not_refetch` (second `load_rules` makes no request: check `/__stats`); `test_backoff_doubles_and_caps`; `test_relax_never_below_base`; `test_slow_response_triggers_backoff` (response over 3x the domain baseline calls `apply_backoff`).
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

---

# Phase 5: Worker end to end

Stop and review: `python -m driftwatch crawl http://127.0.0.1:8800 --workers 8` prints a table of pages, broken links, redirect problems, skipped and dead jobs; compare it with `/__expected`. Then kill a worker process mid-crawl (`python -m driftwatch.demos.crash_demo`) and see the crawl still finish.
DESIGN entries: D14 worker loop and heartbeat design; D15 redirect handling and the internal host set; D16 external link checks.

### Task 5.1: Page parser

**Files:** Create `driftwatch/crawl/parser.py`, `tests/crawl/test_parser.py`.

**Interfaces:** Produces `ParsedPage(title: str | None, meta_description: str | None, h1_count: int, images_missing_alt: list[str], robots_meta: str | None, canonical: str | None, links: list[str], main_text: str)` and `parse_page(body: bytes, content_type: str | None, base_url: str) -> ParsedPage` (decodes by header charset, then meta charset, then utf-8 with `errors="replace"`; never raises).

- [ ] **Step 1:** tests: `test_extracts_seo_fields`; `test_latin1_page_decoded` (Review Focus 4); `test_malformed_html_does_not_raise` (fixture malformed page, unclosed tags and stray `<`); `test_empty_body_returns_empty_page`; `test_images_missing_alt_counts_empty_alt_as_ok` (`alt=""` is valid, missing attribute is not).
- [ ] **Step 2-4:** fail, implement with selectolax, pass. **Step 5:** Commit (ask owner).

### Task 5.2: Worker loop

**Files:** Create `driftwatch/crawl/worker.py`, `driftwatch/models.py` (add `PageResult`), migration 0004, `tests/crawl/test_worker.py`.

**Interfaces:** Consumes everything above. Produces `async process_job(ctx: WorkerContext, job: ClaimedJob) -> None` (robots check, fetch with retries via `classify`, parse, admit and enqueue internal links and external link jobs, store `PageResult`, `complete_job` or `reschedule_job`, run `finalize_if_idle`); `async run_worker(settings, *, concurrency: int, worker_id: str, stop: asyncio.Event) -> None` (N loops, a heartbeat task per job that cancels the fetch if `heartbeat` returns False, shared `httpx.AsyncClient`, semaphore on in-flight requests, periodic reaper and sweeper tasks); `PageResult(job_id, crawl_id, url, status_code, response_ms, size_bytes, chain, outcome, error, content_hash, parsed fields)`.

- [ ] **Step 1:** tests: `test_crawl_of_fixture_visits_each_page_once` (every normalized URL appears once in `jobs`); `test_robots_disallow_all_skips_and_finishes` (Review Focus 1: all jobs `skipped`, crawl `finished`); `test_seed_redirect_to_other_host_is_internal` (Review Focus 2); `test_seed_404_finishes_with_failed_job`; `test_flaky_page_marked_intermittent`; `test_429_backs_off_domain_then_succeeds`; `test_external_links_checked_once_head_then_get`; `test_lost_lease_cancels_fetch` (heartbeat returns False, `PageResult` not written); `test_calendar_trap_bounded` (crawl finishes, jobs under the cap).
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 5.3: CLI

**Files:** Create `driftwatch/__main__.py`, `tests/test_cli.py`.

**Interfaces:** Produces `python -m driftwatch crawl <url> [--workers N] [--max-pages N]` (creates site and crawl, runs a worker in-process until the crawl finishes, prints a summary table) and `python -m driftwatch worker` (runs forever).

- [ ] **Step 1:** test `test_crawl_matches_expected_manifest` (run against the fixture; the set of broken links, redirect problems and skipped pages equals `EXPECTED`).
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 5.4: Crash recovery end to end

**Files:** Create `driftwatch/demos/crash_demo.py`, `tests/test_crash_recovery.py`.

- [ ] **Step 1:** test `test_killed_worker_process_crawl_still_finishes` (start worker A as a subprocess with `lease_seconds` overridden to 6, kill it with `Popen.kill()` mid-crawl, start worker B; the crawl finishes, no job is `running`, `infra_attempts >= 1` on at least one job, every URL has exactly one `PageResult`); `test_poison_url_goes_dead` (a handler that always kills the worker for one URL: that job ends `dead` after 3 attempts and the crawl still finishes).
- [ ] **Step 2-4:** fail, implement the demo (prints job states before and after), pass. **Step 5:** Commit (ask owner).

### Task 5.5: Rate limit holds across workers, including backoff

**Files:** Create `tests/crawl/test_rate_limit_across_workers.py`; the fixture's `/__stats` (Task 2.1) records server-side arrival timestamps per path prefix.

**Interfaces:** Consumes `run_worker` (Task 5.2), `/__stats` and `/__reset` (Task 2.1). Produces helper `gaps(timestamps: Sequence[float]) -> list[float]`.

- [ ] **Step 1: Write failing tests:** `test_three_workers_never_beat_the_domain_delay` (3 workers in one process plus a second `run_worker` task set with a different `worker_id`, 4 worker loops each; fixture domain delay 0.3s, crawl 30 pages; from `/__stats` arrival timestamps, `min(gaps(ts)) >= 0.3 - 0.02`); `test_gap_doubles_after_429` (seed `/ratelimited` among pages; the first request after the 429 arrives at least 2x the base delay later, and a later gap after a success is back at or below 2x, relaxing toward base, never below base); `test_crawl_delay_from_robots_is_the_floor` (fixture `crawl_delay=0.5` in robots.txt: `min(gaps) >= 0.48` even though configured default is 0.1).
- [ ] **Step 2:** run, expect FAIL until the fixture records timestamps and politeness is wired into the claim.
- [ ] **Step 3:** fix whatever the tests expose in `claim.py` / `politeness.py` / `worker.py` (no new design; this task proves the existing design).
- [ ] **Step 4:** run, expect PASS. **Step 5:** stage for the phase commit.

---

# Phase 6: Findings and SEO

Stop and review: after a CLI crawl, `python -m driftwatch findings <crawl_id>` lists every finding; compare against `/__expected`.
DESIGN entries: D17 findings model and identity (spec 5.9).

### Task 6.1: SEO checks

**Files:** Create `driftwatch/analysis/seo.py`, `tests/analysis/test_seo.py`.

**Interfaces:** Produces `FindingDraft(page_url: str, type: str, detail_key: str, detail: dict)`; `page_seo_findings(url: str, parsed: ParsedPage) -> list[FindingDraft]` (types `missing_title`, `missing_meta_description`, `missing_h1`, `image_missing_alt`, `noindex`, `canonical_invalid`); `async crawl_seo_findings(session, crawl_id: int) -> list[FindingDraft]` (`duplicate_title`, `duplicate_meta_description`, `canonical_broken` when the canonical target is 4xx/5xx in this crawl).

- [ ] **Step 1:** tests per type using fixture pages, each asserting exactly the findings in `EXPECTED` for that page and none on `/ok`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 6.2: Fetch-outcome findings and persistence

**Files:** Create `driftwatch/analysis/findings.py`, `Finding` model, migration 0005, `tests/analysis/test_findings.py`.

**Interfaces:** Produces `async record_findings(session, crawl_id: int) -> int` (turns `PageResult` outcomes into `broken_link`, `redirect_chain_too_long`, `redirect_loop`, `intermittent`, `timeout`, plus SEO drafts, writing `Finding` rows keyed `(crawl_id, page_url, type, detail_key)`); dead jobs stay out of `Finding` and are returned by `dead_jobs(session, crawl_id) -> list[Job]`.

- [ ] **Step 1:** tests: `test_broken_external_link_finding`; `test_chain_over_two_hops_flagged_but_two_hops_not`; `test_loop_finding`; `test_intermittent_is_its_own_finding_not_healthy`; `test_dead_jobs_not_findings`; `test_record_is_idempotent`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

---

# Phase 7: Performance, SSL, content change

Stop and review: crawl the fixture twice with `/__mutate?fix=changing` between runs and see a content diff for `/changing`, none for `/noise`; `python -m driftwatch ssl <host>` prints days left.
DESIGN entries: D18 slowness baseline (median and MAD); D19 change detection rules (each noise rule with its reason); D20 SSL check as a side step.

### Task 7.1: Slowness against the page's own baseline

**Files:** Create `driftwatch/analysis/perf.py`, `tests/analysis/test_perf.py`.

**Interfaces:** Produces `is_slow(history_ms: Sequence[float], new_ms: float, *, min_samples: int = 5) -> bool`. Rule: slow when `new_ms > median + 3 * max(1.4826 * MAD, 0.1 * median)` and `new_ms - median >= 200`. Fewer than `min_samples` never alerts.

- [ ] **Step 1:** tests: `test_fewer_than_min_samples_never_alerts`; `test_stable_history_small_wobble_ok`; `test_big_jump_alerts`; `test_zero_variance_uses_percent_floor`; `test_absolute_floor_suppresses_tiny_pages` (10ms to 40ms is not slow); `test_one_past_outlier_does_not_shift_baseline`.
- [ ] **Step 2-4:** fail, implement, pass; wire a `slow_page` finding into `record_findings` using the last 20 results per page. **Step 5:** Commit (ask owner).

### Task 7.2: SSL expiry

**Files:** Create `driftwatch/analysis/ssl_check.py`, `tests/analysis/test_ssl_check.py`.

**Interfaces:** Produces `days_from_not_after(not_after: str, now: datetime) -> int` (pure) and `async days_until_expiry(host: str, port: int = 443, *, timeout: float = 10) -> int | None` (None when the handshake fails); run once per HTTPS domain per crawl at finalization, storing `ssl_days_left` on the crawl's domain result and a `ssl_expiring` finding below 14 days.

- [ ] **Step 1:** tests: `test_days_from_not_after_parses_openssl_format`; `test_expired_is_negative`; `test_local_tls_server_reports_days` (conftest generates a throwaway cert with `cryptography` and runs a TLS echo server); `test_unreachable_returns_none`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 7.3: Content change detection

**Files:** Create `driftwatch/analysis/content.py`, `ContentSnapshot` model, migration 0006, `tests/analysis/test_content.py`.

**Interfaces:** Produces `extract_main_text(html: str) -> str` (drops `script`, `style`, `noscript`, comments, `nav`, `header`, `footer`, `aside`, hidden elements, then prefers `<main>` or `<article>`); `normalize_text(text: str) -> str` (collapse whitespace, strip date/time and "N minutes ago" patterns from a short documented list); `content_hash(text: str) -> str` (sha256); `make_diff(old: str, new: str) -> str` (line-based unified diff via `difflib`); `async record_content(session, site_id: int, url: str, html: str) -> ContentChange | None` (compare hash with the last snapshot: equal gives no new row and no diff; different stores a new snapshot and the diff).

- [ ] **Step 1:** tests: `test_noise_only_change_same_hash` (timestamp and script differences on `/noise`); `test_real_change_new_hash_and_diff_contains_new_line`; `test_unchanged_page_stores_nothing`; `test_whitespace_only_change_ignored`; `test_every_noise_rule_has_a_test` (parametrized over the noise rule list, one input per rule).
- [ ] **Step 2-4:** fail, implement, pass; call `record_content` from `process_job` for HTML pages. **Step 5:** Commit (ask owner).

---

# Phase 8: Comparison and health

Stop and review: crawl, `/__mutate?fix=broken_link_x`, crawl again; `python -m driftwatch compare <site_id>` shows one fixed, the rest ongoing.
DESIGN entries: D21 new/ongoing/fixed/unverified semantics; D22 health score formula.

### Task 8.1: Compare crawls

**Files:** Create `driftwatch/analysis/compare.py`, `tests/analysis/test_compare.py`.

**Interfaces:** Produces `FindingStatus(enum: new|ongoing|fixed|unverified)`; `async compare_crawl(session, site_id: int, crawl_id: int) -> dict[FindingStatus, list[Finding]]` (spec 5.9; previous crawl = latest earlier `finished` crawl; first crawl has everything `new`); store status on `Finding`.

- [ ] **Step 1:** tests: `test_first_crawl_all_new`; `test_same_site_twice_all_ongoing`; `test_fixed_requires_refetch_and_clean`; `test_unreached_page_findings_unverified` (second crawl capped before reaching the page); `test_intermittent_to_clean_is_fixed_of_intermittent_finding`; `test_new_finding_detected`.
- [ ] **Step 2-4:** fail, implement, pass; call after `record_findings` at finalization. **Step 5:** Commit (ask owner).

### Task 8.2: Health score

**Files:** Create `driftwatch/analysis/health.py`, `tests/analysis/test_health.py`.

**Interfaces:** Produces `health_score(counts: Mapping[str, int], pages_crawled: int) -> int` (0-100; 100 minus weighted penalties per page, severity weights documented in D22, floor 0).

- [ ] **Step 1:** tests: `test_clean_site_is_100`; `test_more_broken_links_lower_score`; `test_score_never_below_zero`; `test_penalty_scales_with_page_count` (same issue count on 10 vs 1000 pages scores higher on 1000).
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

---

# Phase 9: Scheduler and API

Stop and review: `docker compose up` (db, migrate, api, worker, fixture); `curl -X POST localhost:8000/api/sites -d '{"url":"http://fixture:8800","interval_hours":6}'`, then `curl -N localhost:8000/api/crawls/1/events` streams progress to `finished`.
DESIGN entries: D23 scheduler and duplicate-free enqueue; D24 SSE by polling the database (versus LISTEN/NOTIFY).

### Task 9.1: Scheduler

**Files:** Create `driftwatch/scheduler.py`, `tests/test_scheduler.py`; modify `worker.py` to run the scheduler loop.

**Interfaces:** Produces `async schedule_due_sites(session) -> list[int]` (a site is due when `enabled` and no crawl started within `interval_seconds`; creates the crawl and the seed job in one transaction; relies on the partial unique index, so a conflict means skip); `async trigger_now(session, site_id: int) -> tuple[int, bool]` (crawl id, created); `async run_scheduler(settings, stop: asyncio.Event)`.

- [ ] **Step 1:** tests: `test_due_site_gets_crawl_and_seed_job`; `test_site_with_running_crawl_skipped`; `test_not_due_site_skipped`; `test_ten_concurrent_schedulers_one_crawl`; `test_trigger_now_returns_running_crawl`; `test_disabled_site_never_scheduled`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 9.2: REST API

**Files:** Create `driftwatch/api/{main,routes,schemas}.py`, `tests/api/test_routes.py`; extend `docker-compose.yml` with `migrate`, `api`, `worker`, `fixture`.

**Interfaces:** Produces under `/api`: `GET/POST /sites`, `PATCH /sites/{id}` (schedule, enabled), `POST /sites/{id}/trigger`, `GET /sites` (health, last crawl, new-issue count), `GET /sites/{id}/findings?crawl_id=` grouped by type and status, `GET /sites/{id}/trends`, `GET /pages?site_id=&url=` (response-time history, findings, content changes with diffs), `GET /crawls/{id}`, `GET /debug/tasks` (calls `dump_tasks()`).

- [ ] **Step 1:** tests with `httpx.ASGITransport`: `test_create_site_validates_url`; `test_trigger_twice_returns_same_crawl`; `test_findings_grouped_with_status`; `test_page_history_ordered_by_time`; `test_unknown_site_404`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

### Task 9.3: SSE live progress

**Files:** Create `driftwatch/api/sse.py`, `tests/api/test_sse.py`.

**Interfaces:** Produces `GET /api/crawls/{id}/events` emitting `progress` events every second (`pages_crawled`, `queue_size`, `running`, `errors`, `dead`) and a final `finished` event, then closing; the generator exits when the client disconnects.

- [ ] **Step 1:** tests: `test_stream_emits_progress_then_finished`; `test_stream_stops_on_disconnect` (no pending task left, checked with `dump_tasks()`); `test_finished_crawl_emits_finished_immediately`.
- [ ] **Step 2-4:** fail, implement, pass. **Step 5:** Commit (ask owner).

---

# Phase 10: Dashboard (invoke `frontend-design:frontend-design` before each sub-phase)

Stop and review after each sub-phase by running `docker compose up` and using the UI in a browser.
DESIGN entries: D25 frontend architecture and API consumption; D26 visual direction (written after the owner picks one).

### Task 10.1: Scaffold, design direction, overview

**Files:** Create `frontend/` (Vite, React, TypeScript, Tailwind, Vitest), `frontend/src/{api,components,pages}`; add the `frontend` compose service.

**Interfaces:** Consumes the Task 9.2 routes. Produces typed API client `api.ts`, Overview page (sites with health score, last crawl time, new-issue counts), Add Site and edit-schedule dialogs, Trigger Now button. Pure helpers `groupFindings(findings): Record<type, Record<status, Finding[]>>` and `formatRelativeTime(iso: string, now: Date): string` live in `src/lib/`.

- [ ] **Step 1:** before writing code, present the owner two or three visual directions for a developer-tool look and let them choose. Then Vitest tests: `groupFindings` groups by type then status; `formatRelativeTime` cases ("just now", "5 min ago", "2 d ago").
- [ ] **Step 2-4:** fail, implement, pass; `npm run build` and `npx tsc --noEmit` clean; check the page in a browser. **Step 5:** Commit (ask owner).

### Task 10.2: Site detail

**Files:** Create `frontend/src/pages/SiteDetail.tsx`, trend chart component.

**Interfaces:** Issues grouped by type with new/fixed/ongoing/unverified badges and dead jobs shown separately; trend charts (issues and health over crawls, response time) using the `dataviz` skill guidance.

- [ ] **Step 1:** Vitest test `test_status_badge_counts_match_findings`. **Step 2-4:** fail, implement, pass, verify in browser. **Step 5:** Commit (ask owner).

### Task 10.3: Page detail and live progress

**Files:** Create `frontend/src/pages/PageDetail.tsx`, `DiffView.tsx`, `LiveCrawl.tsx`.

**Interfaces:** Response-time history, page findings, side-by-side content diff from the stored unified diff; live crawl panel via `EventSource` showing pages crawled, queue size and errors, closing the stream on `finished` and on unmount.

- [ ] **Step 1:** Vitest tests: `parseUnifiedDiff` turns a diff into added/removed/context rows; `LiveCrawl` closes its `EventSource` on unmount (mock `EventSource`). **Step 2-4:** fail, implement, pass, verify in browser against a running crawl. **Step 5:** Commit (ask owner).

---

# Phase 11: Benchmarks and documentation

Stop and review: `python -m bench.run --pages 1000` prints pages per second, total time, and "rate limit respected: yes"; README renders the Mermaid diagram.
DESIGN entries: D27 benchmark methodology; D28 known limitations (no pruning, no JavaScript rendering, single Postgres, polling SSE, robots 5xx policy).

### Task 11.1: Benchmark script

**Files:** Create `bench/run.py`, `tests/test_bench.py`; extend the fixture with `/gen/{n}` (a generated site of N linked pages, size from `FIXTURE_PAGES`).

**Interfaces:** Produces `async run_benchmark(pages: int, workers: int, batch_size: int, domain_delay: float) -> BenchResult(pages_per_second: float, total_seconds: float, min_gap_seconds: float, rate_limit_respected: bool)`; the rate-limit check reads the fixture's server-side arrival timestamps from `/__stats` (not worker clocks) and asserts every consecutive gap is at least `domain_delay` minus 10ms tolerance. Also compares `batch_size` 1 versus 5 and 1, 4, 16 workers.

- [ ] **Step 1:** tests: `test_rate_limit_check_detects_violation` (feed timestamps with a short gap, expect False); `test_benchmark_small_site_respects_limit` (100 pages, delay 0.05).
- [ ] **Step 2-4:** fail, implement, pass; run for 1,000 and 10,000 pages. **Step 5:** Commit (ask owner).

### Task 11.2: README and DESIGN polish

**Files:** Modify `README.md`, `docs/DESIGN.md`, `docs/concurrency-notes.md`.

- [ ] **Step 1:** README: Mermaid architecture diagram (api, worker with scheduler/reaper/sweeper, Postgres, fixture), `docker compose up` steps, how to run tests, benchmark table filled with the real numbers from Task 11.1, an "interview questions and where the answer lives" index into DESIGN.md.
- [ ] **Step 2:** Read DESIGN.md D1-D28 end to end against the code; fix drift. **Step 3:** from a clean clone, `docker compose up` works and `pytest` passes. **Step 4:** Commit (ask owner).

---

## Self-Review Notes

- Spec coverage: 5.1 (1.1-1.3), 5.2 (1.3, 4.4), 5.3 (1.2, 3.2), 5.4 (1.3, 11.1), 5.5 (1.6), 5.6 (4.3, 1.5), 5.7 (7.3), 5.8 (4.1, notes), 5.9 (8.1), section 6 (3-5), section 7 (2.1), section 8 (10.x), section 9 (11.x), 10a calls (4.4, 4.2, 5.2, 9.1, 7.2, 7.1).
- Known gap: the owner chose no pruning; recorded in D28.
