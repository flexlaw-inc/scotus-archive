"""
Pytest configuration for the FLexlaw SCOTUS Archive test suite.

Tests fall into two categories:
  1. Unit tests — no DB required; test parsing, seed data structure, etc.
  2. Sanity-check tests — require a live DB connection; validate statistical
     properties of each release against published baselines.

Set DB_URL environment variable to enable sanity-check tests:
  export DB_URL="postgresql://john@localhost/legal_research"
"""

import os
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "db: mark test as requiring a database connection (skipped if DB_URL not set)",
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow (skipped with -m 'not slow')",
    )


@pytest.fixture(scope="session")
def db_conn():
    """Session-scoped DB connection. Skips if DB_URL is not set."""
    db_url = os.environ.get("DB_URL")
    if not db_url:
        pytest.skip("DB_URL not set — skipping DB-backed tests")
    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"Cannot connect to DB: {e}")


@pytest.fixture(scope="session")
def db_cur(db_conn):
    import psycopg2.extras
    cur = db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    yield cur
    cur.close()
