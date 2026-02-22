"""
CNPG Hybrid Search Diagnosis: EXPLAIN ANALYZE breakdown.

Runs the hybrid search query and its individual components to identify
where the performance gap comes from compared to pure vector search.

Tests:
  1. Pure vector search (baseline)
  2. Full-text search CTE alone
  3. Vector CTE with ROW_NUMBER window function
  4. Full hybrid RRF query
  5. Timed runs (no EXPLAIN) for accurate latency comparison

Usage:
    export PG_HOST="51.104.162.145"
    export PG_PASSWORD="<password>"
    uv run python scripts/cnpg_hybrid_search_diagnosis.py
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
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD, sslmode=SSLMODE,
        autocommit=True,
    )
    register_vector(conn)
    return conn


def run_explain(conn: psycopg.Connection, label: str, sql: str, params: tuple) -> None:
    """Run EXPLAIN ANALYZE and log every plan line."""
    log = logger.bind(test=label)
    log.info("running_explain_analyze")
    t0 = time.perf_counter()
    rows = conn.execute(sql, params).fetchall()
    elapsed = (time.perf_counter() - t0) * 1000
    for r in rows:
        log.info("plan", line=r[0])
    log.info("explain_done", elapsed_ms=round(elapsed, 2))


def run_timed(conn: psycopg.Connection, label: str, sql: str, params: tuple, repeats: int = 5) -> None:
    """Run the query multiple times (without EXPLAIN) and report timings."""
    log = logger.bind(test=label, repeats=repeats)
    timings = []
    for i in range(repeats):
        t0 = time.perf_counter()
        result = conn.execute(sql, params).fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        timings.append(elapsed)
        log.info("run", iteration=i + 1, elapsed_ms=round(elapsed, 2), rows=len(result))

    log.info(
        "timing_summary",
        min_ms=round(min(timings), 2),
        max_ms=round(max(timings), 2),
        avg_ms=round(sum(timings) / len(timings), 2),
        median_ms=round(sorted(timings)[len(timings) // 2], 2),
    )


def main() -> None:
    """Run hybrid search diagnosis tests."""
    logger.info("hybrid_search_diagnosis_start")

    if not PG_PASSWORD:
        logger.error("missing_pg_password", hint="Set PG_PASSWORD env var")
        sys.exit(1)

    conn = get_connection()
    logger.info("connected", host=PG_HOST, database=PG_DATABASE, table=TABLE_NAME)

    count = conn.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    logger.info("table_row_count", count=count)

    # Fetch a real sample
    row = conn.execute(
        f"SELECT embedding, content, metadata->>'book_name' "
        f"FROM {TABLE_NAME} "
        f"WHERE content IS NOT NULL AND content != '' "
        f"LIMIT 1"
    ).fetchone()
    emb = np.array(row[0], dtype=np.float32)
    content_text = row[1]
    # Use a common English word likely to have GIN matches
    keyword = "dragon"
    book = row[2]
    logger.info("sample_fetched", book=book, keyword=keyword, emb_dim=len(emb))

    top_k = 10
    vector_limit = top_k * 2

    # Check how many text matches 'dragon' produces
    text_match_count = conn.execute(
        f"SELECT count(*) FROM {TABLE_NAME} "
        f"WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s)",
        (keyword,)
    ).fetchone()[0]
    logger.info("text_match_count", keyword=keyword, matches=text_match_count)

    # ================================================================
    # TEST 1: Pure vector search (baseline)
    # ================================================================
    logger.info("=" * 60)
    sql_vector = (
        f"SELECT id, 1 - (embedding <=> %s::vector) AS similarity "
        f"FROM {TABLE_NAME} "
        f"ORDER BY embedding <=> %s::vector "
        f"LIMIT {top_k}"
    )
    run_explain(conn, "pure_vector_search", f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql_vector}", (emb, emb))
    run_timed(conn, "pure_vector_search_timed", sql_vector, (emb, emb))

    # ================================================================
    # TEST 2: Full-text search CTE alone
    # ================================================================
    logger.info("=" * 60)
    sql_text = (
        f"SELECT id, "
        f"  ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), "
        f"    plainto_tsquery('english', %s)) DESC) as rank, "
        f"  ts_rank(to_tsvector('english', content), plainto_tsquery('english', %s)) as text_score "
        f"FROM {TABLE_NAME} "
        f"WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s) "
        f"LIMIT {vector_limit}"
    )
    run_explain(conn, "text_search_alone", f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql_text}", (keyword, keyword, keyword))
    run_timed(conn, "text_search_alone_timed", sql_text, (keyword, keyword, keyword))

    # ================================================================
    # TEST 3: Vector CTE with ROW_NUMBER (extra overhead?)
    # ================================================================
    logger.info("=" * 60)
    sql_vector_rn = (
        f"SELECT id, "
        f"  ROW_NUMBER() OVER (ORDER BY embedding <=> %s::vector) as rank, "
        f"  1 - (embedding <=> %s::vector) as vector_similarity "
        f"FROM {TABLE_NAME} "
        f"LIMIT {vector_limit}"
    )
    run_explain(conn, "vector_with_row_number", f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql_vector_rn}", (emb, emb))
    run_timed(conn, "vector_with_row_number_timed", sql_vector_rn, (emb, emb))

    # ================================================================
    # TEST 4: Full hybrid RRF query
    # ================================================================
    logger.info("=" * 60)
    sql_hybrid = (
        f"WITH vector_search AS ( "
        f"  SELECT id, "
        f"    ROW_NUMBER() OVER (ORDER BY embedding <=> %s::vector) as rank, "
        f"    1 - (embedding <=> %s::vector) as vector_similarity "
        f"  FROM {TABLE_NAME} "
        f"  LIMIT {vector_limit} "
        f"), "
        f"text_search AS ( "
        f"  SELECT id, "
        f"    ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), "
        f"      plainto_tsquery('english', %s)) DESC) as rank, "
        f"    ts_rank(to_tsvector('english', content), plainto_tsquery('english', %s)) as text_score "
        f"  FROM {TABLE_NAME} "
        f"  WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s) "
        f"  LIMIT {vector_limit} "
        f"), "
        f"rrf_scores AS ( "
        f"  SELECT COALESCE(v.id, t.id) as id, "
        f"    (COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0)) as rrf_score, "
        f"    v.vector_similarity, t.text_score "
        f"  FROM vector_search v "
        f"  FULL OUTER JOIN text_search t ON v.id = t.id "
        f") "
        f"SELECT id, rrf_score as similarity "
        f"FROM rrf_scores "
        f"ORDER BY rrf_score DESC "
        f"LIMIT {top_k}"
    )
    run_explain(
        conn, "full_hybrid_rrf",
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql_hybrid}",
        (emb, emb, keyword, keyword, keyword),
    )
    run_timed(conn, "full_hybrid_rrf_timed", sql_hybrid, (emb, emb, keyword, keyword, keyword))

    # ================================================================
    # TEST 5: Hybrid with higher top_k (50) to see if it degrades
    # ================================================================
    logger.info("=" * 60)
    top_k_high = 50
    vector_limit_high = top_k_high * 2
    sql_hybrid_50 = (
        f"WITH vector_search AS ( "
        f"  SELECT id, "
        f"    ROW_NUMBER() OVER (ORDER BY embedding <=> %s::vector) as rank, "
        f"    1 - (embedding <=> %s::vector) as vector_similarity "
        f"  FROM {TABLE_NAME} "
        f"  LIMIT {vector_limit_high} "
        f"), "
        f"text_search AS ( "
        f"  SELECT id, "
        f"    ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), "
        f"      plainto_tsquery('english', %s)) DESC) as rank, "
        f"    ts_rank(to_tsvector('english', content), plainto_tsquery('english', %s)) as text_score "
        f"  FROM {TABLE_NAME} "
        f"  WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s) "
        f"  LIMIT {vector_limit_high} "
        f"), "
        f"rrf_scores AS ( "
        f"  SELECT COALESCE(v.id, t.id) as id, "
        f"    (COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0)) as rrf_score, "
        f"    v.vector_similarity, t.text_score "
        f"  FROM vector_search v "
        f"  FULL OUTER JOIN text_search t ON v.id = t.id "
        f") "
        f"SELECT id, rrf_score as similarity "
        f"FROM rrf_scores "
        f"ORDER BY rrf_score DESC "
        f"LIMIT {top_k_high}"
    )
    run_explain(
        conn, "hybrid_rrf_topk50",
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql_hybrid_50}",
        (emb, emb, keyword, keyword, keyword),
    )
    run_timed(conn, "hybrid_rrf_topk50_timed", sql_hybrid_50, (emb, emb, keyword, keyword, keyword))

    conn.close()
    logger.info("diagnosis_complete")


if __name__ == "__main__":
    main()
