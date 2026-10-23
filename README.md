# Driftwatch

A website crawler and monitoring tool: broken links, performance regressions, SEO issues,
content changes and SSL expiry, tracked crawl over crawl. Single-user, runs locally.

Work in progress. Design decisions: `docs/DESIGN.md`. Plan: `docs/superpowers/plans/`.

## Development quick start

    docker compose up -d db
    cd backend
    uv sync --python 3.12
    uv run pytest
