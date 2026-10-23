# Driftwatch: Design Spec

Date: 2026-10-23. Status: draft, awaiting owner approval.

## 1. Intent and why

Driftwatch is a single-user website crawler and monitor, built as a BTech CSE final-year project. Its purpose is to demonstrate backend and systems engineering in SDE interviews. The owner must be able to explain every design decision, so **clarity and correctness beat feature count**.

Owner's priorities, in order:
1. The Postgres job queue and crash recovery (SKIP LOCKED, leases, heartbeats, why Postgres over Redis/Celery), each claim proven by tests.
2. Politeness and per-domain rate limiting.
3. Concurrency (asyncio), explained from first principles while building. The owner is not yet confident with the event loop, semaphores, debugging hangs or leaked connections.
4. Change detection that is simple enough to justify every rule.

## 2. What it does

The user adds websites. Driftwatch crawls each on a schedule (e.g. every 6h, daily) and reports:
1. Broken links: 4xx/5xx, timeouts, redirect chains longer than 2 hops, redirect loops.
2. Performance: response time and page size per page; alert when a page is significantly slower than its own recent baseline (not a fixed threshold).
3. SEO: missing/duplicate titles and meta descriptions, missing h1, images without alt, accidental noindex, broken canonical.
4. Content changes: meaningful diffs between crawls, ignoring noise.
5. SSL: days until certificate expiry.

Each crawl is compared with the previous one: new, fixed, ongoing.

## 3. Goals, non-goals, anti-goals

**Goals:** correct, typed, well-structured code; every core mechanism testable against a known fixture site; real benchmark numbers for the resume; `docs/DESIGN.md` kept current.

**Non-goals:** authentication, accounts, billing, multi-tenancy, JavaScript rendering, distributed deployment beyond multiple worker processes on one Postgres.

**Anti-goals (failure even if it technically works):** a feature the owner cannot explain; magic (fuzzy similarity, ML, clever heuristics) in change detection; correctness that depends on in-memory state rather than the database; claims in the README without a test or benchmark behind them.

## 4. Constraints and stack (owner-specified)

Python 3.12, FastAPI, async throughout; httpx (async) + selectolax or BeautifulSoup; PostgreSQL with SQLAlchemy 2.0 async and Alembic; queue built on Postgres `SELECT ... FOR UPDATE SKIP LOCKED` (no Redis, no Celery); React + TypeScript + Vite + Tailwind; pytest + pytest-asyncio; everything runs with a single `docker compose up`. Dashboard work uses the frontend-design skill. Development is TDD, phase by phase, stopping after each phase for owner review.

## 5. Decisions made in brainstorming

### 5.1 Unit of work: one job is one URL fetch
Not one job per site-crawl. Reasons: a dead worker loses at most one page; fairness across sites is a claim-time choice; the queue is the crawl's state, so crawls resume. Cost: more rows and claims, and explicit crawl-completion logic.

### 5.2 Per-domain rate limiting: `domains.next_allowed_fetch_at`
Claim is one transaction, two steps:
1. Reserve a domain slot: conditional `UPDATE domains SET next_allowed_fetch_at = now() + delay ... WHERE id = (SELECT id FROM domains WHERE next_allowed_fetch_at <= now() AND <domain has pending jobs> ORDER BY next_allowed_fetch_at LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING ...`.
2. Claim one pending job of that domain with `FOR UPDATE SKIP LOCKED`. If none, roll back so the slot is not spent.

Properties: the row lock plus the `<= now()` predicate is a compare-and-swap, so two workers cannot take the same slot; ordering by oldest-due domain gives fairness (a 10,000-page site cannot starve a 20-page site); the database clock (`now()`) is the only clock. Slot is reserved at claim time, so it spaces request starts, not completions; a crash after reserving wastes one slot. 429/503 push `next_allowed_fetch_at` forward and double the stored delay; robots.txt `Crawl-delay` sets the base delay.
Rejected: in-process token bucket (breaks across worker processes), advisory locks (concurrency not rate), Redis (excluded).

### 5.3 Deduplication in the database
Unique constraint on `(crawl_id, normalized_url)` with `INSERT ... ON CONFLICT DO NOTHING`. Concurrent inserts of the same URL are serialized by the unique index. Normalization runs before insert. Redirect targets go through the same insert, so `/a -> /b` never fetches `/b` twice. First discovery wins the depth value.

### 5.4 Claim size
One job per claim by default; `batch_size` configurable for benchmarking. Because of 5.2, a batch means one job each from N distinct domains, never several from one domain. Heartbeat covers every job in a batch.

### 5.5 Crawl completion
A crawl is finished when it has no pending or running jobs. The predicate is stable (only running jobs create new ones, and a job enqueues its discovered links in the same transaction that marks it done), so a true reading is always correct and a false reading only delays detection. Workers check opportunistically on job completion; the scheduler loop sweeps periodically as the correctness backstop. Finalization is a conditional `UPDATE crawls SET status='finished' WHERE id=? AND status='running'`, so exactly one finalizer wins. No `pending_count` counter on the crawl row (hot row, drifts after crashes). Terminal job states: done, failed-as-finding, dead, skipped by max-pages cap.

### 5.6 Failure taxonomy
Two separate kinds of failure, never conflated.

**Site failures (become findings):**
- Definitive, no retry, finding recorded immediately: 404, 410, DNS failure, connection refused, SSL errors.
- Timeouts and 500/502/504: 2 retries with backoff. All attempts fail: recorded as broken. A retry succeeds: page recorded as **intermittent**, not healthy.
- 429 and 503 are politeness signals: back off the domain (5.2), re-queue the job, and record a finding only if it still fails after the retry budget.

**Infrastructure failures (the queue's job):**
- A lease that expires without a heartbeat re-queues the job and increments `infra_attempts`. At the cap (default 3) the job becomes **dead** (dead-letter), so a URL that crashes workers cannot loop forever. Dead jobs are terminal for completion and shown on the dashboard separately from site findings.
- **Lease rule (goes into DESIGN.md):** lease duration must be comfortably longer than the maximum fetch time including all retries and backoff, so a slow healthy fetch is never mistaken for a dead worker. Workers also heartbeat to extend the lease.

### 5.7 Change detection (keep simple, every rule justifiable)
Extract main content, strip an explicit short noise list (script, style, noscript, comments, timestamps/dates matching listed patterns), normalize whitespace, hash. Unchanged hash: skip cheaply. Changed hash: store a text diff. No fuzzy similarity, no ML. Each rule is one DESIGN.md line stating what it removes and why. Exact extraction and noise rules are left to the builder, subject to that constraint.

### 5.8 Concurrency teaching
Concurrency is introduced in small steps with explanation from first principles: event loop, tasks, semaphores, connection pooling, then what hangs and leaks look like and how to find them. DESIGN.md gets dedicated entries for queue/crash recovery, concurrency, and change detection.

## 6. Required engineering (from the owner's brief)

- **Politeness:** robots.txt incl. crawl-delay; per-domain rate limit; exponential backoff on 429/503 and slow responses; honest User-Agent.
- **URL normalization:** lowercase host, drop fragments, sort query params, strip tracking params (utm_*), trailing-slash handling.
- **Crawl traps:** max depth, max pages per crawl, max URL length, repeating path segments, unbounded query-param combinations.
- **Concurrency:** bounded by asyncio semaphores; configurable worker count.
- **Scheduler:** finds due sites and enqueues crawl jobs with no duplicates.
- **Scope:** crawl internal links only; check external links once per crawl (HEAD, fall back to GET).
- **Comparison:** each crawl compared with the previous: new / fixed / ongoing.

## 7. Fixture site and tests

A small static server in the repo with known problems: broken internal and external links, redirect chains, a redirect loop, slow endpoints, a calendar-style infinite trap, duplicate URLs differing in query params, SEO-problem pages, pages whose content changes between requests. It is built early. Tests and benchmarks run against it, so the expected findings are known exactly. Queue tests prove: no double-claim under concurrent workers, lease expiry re-queues, dead-lettering at the cap, rate limit never exceeded, completion detected under concurrent finishers.

## 8. Dashboard

Overview (health score, last crawl, new issue counts); site detail (issues by type with new/fixed/ongoing, trend charts); page detail (response-time history, issues, content diff); live crawl progress via SSE (pages crawled, queue size, errors); add site, edit schedule, trigger crawl now. Should look like a polished developer tool, not a generic admin template.

## 9. Benchmarks and deliverables

Benchmark script: pages/second, total crawl time for N pages, and proof the per-domain rate limit is never exceeded; also compares batch sizes. Results recorded in README. Deliverables: typed code, README with Mermaid architecture diagram, setup steps and benchmark results, `docs/DESIGN.md`.

### 5.9 Finding identity and comparison (owner-approved)
A finding is identified by (page URL, issue type, detail key). New: in this crawl, not in the previous. Ongoing: in both. Fixed: the page was re-fetched in this crawl and no longer has the finding. A page not reached (disappeared, or crawl cut off by the max-pages cap) keeps its old findings as ongoing-but-unverified, so a truncated crawl never makes problems look solved. Intermittent is part of the finding identity.

## 10a. Calls made by the designer (owner to check)

These were settled during the builder check, not by the owner. Change any you disagree with.

1. **Phase order:** fixture site, then schema and queue (claim, lease, heartbeat, recovery, with tests) before any real crawling; then fetch/parse, politeness, scheduler, findings and comparison, API, dashboard, benchmarks and docs polish. Each phase ends with something the owner can run, plus its tests.
2. **Teaching:** each phase explains its new concepts briefly in its own review notes and in DESIGN.md. The concurrency phase includes a reproducible leaked-connection demo and a hang-debugging exercise.
3. **External links:** jobs with `kind = external`, subject to the same domain rate limit and dedup. HEAD then GET fallback. Never parsed or followed. robots.txt does not block link checks, but the rate limit and an honest User-Agent still apply.
4. **robots.txt:** cached in the database per domain with a TTL. A disallowed URL is recorded as `skipped_robots`, not silently dropped, and does not count as broken. If robots.txt fails with 5xx or a timeout, retry later and treat the domain as allowed only after the retry budget; a 404 means allow all. The fixture site serves a robots.txt.
5. **Scheduling:** fixed intervals only (e.g. every 6h, daily), not cron. A due site with a crawl already running is skipped. "Trigger crawl now" while one is running returns the running crawl. Duplicate prevention is a conditional insert guarded by a partial unique index (one active crawl per site), so it holds even with several scheduler instances. The scheduler is a loop inside the worker process.
6. **SSL expiry:** checked once per crawl per HTTPS domain as a side step by a separate TLS connection, not a queue job type.
7. **Slowness alert:** needs history. A page with fewer than a minimum number of prior samples is never alerted.
8. **Redirects:** `follow_redirects` is off. Within one fetch job the fetcher follows hops manually, records the chain, flags chains over 2 hops and loops (a repeated URL within one chain). An internal final target is also inserted into the dedup table so it is not fetched twice.
9. **Compose and scale:** `docker compose up` starts Postgres, API, workers, the built frontend, and the fixture site; migrations run automatically on start. Benchmarks target 1,000 and 10,000 fixture pages. Old crawl rows are not pruned (noted as a known limitation).

## 10. Left to the builder

Table layouts and indexes; heartbeat interval; default numeric values (delays, caps, retry backoff, lease length subject to rule 5.6); parser choice (selectolax vs BeautifulSoup); main-content extraction method; health-score formula; baseline definition for the slowness alert (window and threshold); diff format; project layout; frontend visual direction.
