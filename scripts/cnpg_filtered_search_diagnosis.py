"""
CNPG Filtered Search Diagnosis — EXPLAIN ANALYZE at varying top_k

Sends a single filtered search query at different top_k values (10, 20, 50, 100)
against the 2.5M WoT dataset on CNPG. Uses EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
to capture the actual query execution plan from PostgreSQL.

Goal: determine *why* top_k >= 50 causes a performance cliff on filtered search.
Is it a plan change (seq scan vs index scan)? An HNSW limitation? Something else?

Usage:
    export PG_HOST="51.104.162.145"
    export PG_PASSWORD="<password>"
    uv run python scripts/cnpg_filtered_search_diagnosis.py
"""

import os
import sys
import time
from pathlib import Path

import numpy as np
import psycopg
from pgvector.psycopg import register_vector

sys.path.append(str(Path(__file__).parent.parent / "src"))
from shared.logger_structlog import setup_structlog

logger = setup_structlog()

# ---------------------------------------------------------------------------
# Connection parameters — override with env vars
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "51.104.162.145")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("VECTOR_PG_DATABASE", "benchmark_vectors")
PG_USER = os.getenv("PG_USER", "benchmark_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
TABLE_NAME = os.getenv("TABLE_NAME", "wot_chunks_2_5m")
SSLMODE = os.getenv("PGSSLMODE", "require")


def get_connection() -> psycopg.Connection:
    """Create a psycopg3 connection with vector type registered."""
    conn = psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=SSLMODE,
        autocommit=True,
    )
    register_vector(conn)
    return conn


def fetch_sample_query(conn: psycopg.Connection) -> tuple[np.ndarray, str]:
    """
    Grab one real embedding and one book_name from the table so we use
    realistic data for the diagnosis query.

    Returns:
        (embedding, book_name) tuple
    """
    row = conn.execute(
        f"SELECT embedding, metadata->>'book_name' AS book "
        f"FROM {TABLE_NAME} WHERE metadata->>'book_name' IS NOT NULL "
        f"LIMIT 1"
    ).fetchone()
    embedding = np.array(row[0], dtype=np.float32)
    book_name = row[1]
    logger.info("sample_fetched", book_name=book_name, embedding_dim=len(embedding))
    return embedding, book_name


def run_explain_analyze(
    conn: psycopg.Connection,
    embedding: np.ndarray,
    book_name: str,
    top_k: int,
) -> None:
    """
    Run the exact filtered search query with EXPLAIN (ANALYZE, BUFFERS)
    and log the full plan.
    """
    log = logger.bind(top_k=top_k, filter_value=book_name)

    # Build the same query the benchmark uses, wrapped in EXPLAIN ANALYZE
    query_sql = f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT id, 1 - (embedding <=> %s::vector) AS similarity
        FROM {TABLE_NAME}
        WHERE metadata->>'book_name' = %s
        ORDER BY embedding <=> %s::vector
        LIMIT {top_k}
    """

    log.info("executing_explain_analyze")
    t0 = time.perf_counter()
    rows = conn.execute(query_sql, (embedding, book_name, embedding)).fetchall()
    elapsed_ms = (time.perf_counter() - t0) * 1000

    # Collect the full plan text
    plan_lines = [r[0] for r in rows]
    plan_text = "\n".join(plan_lines)

    log.info(
        "explain_analyze_complete",
        elapsed_ms=round(elapsed_ms, 2),
        plan_lines=len(plan_lines),
    )

    # Log each line so the full plan is visible in structured logs
    for line in plan_lines:
        log.info("plan_line", line=line)

    # Also run the actual query (without EXPLAIN) to get the real latency
    actual_sql = f"""
        SELECT id, 1 - (embedding <=> %s::vector) AS similarity
        FROM {TABLE_NAME}
        WHERE metadata->>'book_name' = %s
        ORDER BY embedding <=> %s::vector
        LIMIT {top_k}
    """

    log.info("executing_actual_query")
    t0 = time.perf_counter()
    result = conn.execute(actual_sql, (embedding, book_name, embedding)).fetchall()
    actual_ms = (time.perf_counter() - t0) * 1000

    log.info(
        "actual_query_complete",
        elapsed_ms=round(actual_ms, 2),
        rows_returned=len(result),
    )


def main() -> None:
    """
    Run filtered search diagnosis at top_k = 1, 5, 10, 20, 50, 100.

    For each top_k value we run EXPLAIN ANALYZE to capture the query plan,
    then run the actual query to measure real latency — all single-threaded,
    no concurrency.
    """
    logger.info("=" * 70)
    logger.info("cnpg_filtered_search_diagnosis_start")
    logger.info("=" * 70)

    if not PG_PASSWORD:
        logger.error("missing_pg_password", hint="Set PG_PASSWORD env var")
        sys.exit(1)

    conn = get_connection()
    logger.info(
        "connected",
        host=PG_HOST,
        database=PG_DATABASE,
        table=TABLE_NAME,
    )

    # Verify row count
    count = conn.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    logger.info("table_row_count", count=count)

    # Fetch a real sample to use for all queries
    embedding, book_name = fetch_sample_query(conn)

    # Test at different top_k values
    top_k_values = [1, 5, 10, 20, 50, 100]

    for top_k in top_k_values:
        logger.info("=" * 50)
        run_explain_analyze(conn, embedding, book_name, top_k)

    logger.info("=" * 70)
    logger.info("diagnosis_complete")
    logger.info("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
