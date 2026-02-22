"""
Async Results Database Module

Provides async-compatible result saving functions using psycopg3.
This module should be used in async contexts to avoid blocking the event loop
with synchronous database operations.

Supports database separation:
- Results Database: Stores benchmark metrics (summary tables only)
- Vector Database: Stores actual vector data for pgvector benchmarks

Usage:
    # For benchmark results storage
    async with get_results_db_async_connection() as conn:
        await save_retrieval_metrics_async(conn, ...)

    # Legacy connection (uses PG_DATABASE)
    async with get_async_connection() as conn:
        await save_retrieval_metrics_async(conn, ...)
"""

import os
import statistics
from contextlib import asynccontextmanager
from typing import List, Optional

import psycopg
from psycopg.rows import dict_row

from shared.logger_structlog import setup_structlog

logger = setup_structlog()


# =============================================================================
# Connection String Builders
# =============================================================================


def get_results_db_connection_string() -> str:
    """
    Build PostgreSQL connection string for results database (psycopg3).

    Uses RESULTS_PG_* environment variables if set, otherwise falls back to PG_*.

    Returns:
        str: Connection string for psycopg3.
    """
    host = os.getenv("RESULTS_PG_HOST", os.getenv("PG_HOST"))
    port = os.getenv("RESULTS_PG_PORT", os.getenv("PG_PORT", "5432"))
    database = os.getenv("RESULTS_PG_DATABASE", "benchmark_results")
    user = os.getenv("RESULTS_PG_USER", os.getenv("PG_USER"))
    password = os.getenv("RESULTS_PG_PASSWORD", os.getenv("PG_PASSWORD"))

    return f"host={host} port={port} dbname={database} user={user} password={password}"


def get_vector_db_connection_string() -> str:
    """
    Build PostgreSQL connection string for vector database (psycopg3).

    Uses VECTOR_PG_* environment variables if set, otherwise falls back to PG_*.

    Returns:
        str: Connection string for psycopg3.
    """
    host = os.getenv("VECTOR_PG_HOST", os.getenv("PG_HOST"))
    port = os.getenv("VECTOR_PG_PORT", os.getenv("PG_PORT", "5432"))
    database = os.getenv("VECTOR_PG_DATABASE", "vector_data")
    user = os.getenv("VECTOR_PG_USER", os.getenv("PG_USER"))
    password = os.getenv("VECTOR_PG_PASSWORD", os.getenv("PG_PASSWORD"))

    return f"host={host} port={port} dbname={database} user={user} password={password}"


def get_connection_string() -> str:
    """
    Legacy function: Build PostgreSQL connection string for psycopg3.

    Uses PG_DATABASE (default: vector_benchmark) for backward compatibility.

    Returns:
        str: Connection string for psycopg3.
    """
    return (
        f"host={os.getenv('PG_HOST')} "
        f"port={os.getenv('PG_PORT', '5432')} "
        f"dbname={os.getenv('PG_DATABASE', 'vector_benchmark')} "
        f"user={os.getenv('PG_USER')} "
        f"password={os.getenv('PG_PASSWORD')}"
    )


# =============================================================================
# Async Connection Context Managers
# =============================================================================


@asynccontextmanager
async def get_results_db_async_connection():
    """
    Async context manager for results database connections.

    Use this for saving benchmark metrics to the results database.

    Usage:
        async with get_results_db_async_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(...)
    """
    conninfo = get_results_db_connection_string()
    conn = await psycopg.AsyncConnection.connect(conninfo)
    try:
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def get_vector_db_async_connection():
    """
    Async context manager for vector database connections.

    Use this for pgvector benchmark operations (insert, query, etc.).

    Usage:
        async with get_vector_db_async_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(...)
    """
    conninfo = get_vector_db_connection_string()
    conn = await psycopg.AsyncConnection.connect(conninfo)
    try:
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def get_async_connection():
    """
    Legacy async context manager for database connections.

    Uses PG_DATABASE (default: vector_benchmark) for backward compatibility.

    Usage:
        async with get_async_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(...)
    """
    conninfo = get_connection_string()
    conn = await psycopg.AsyncConnection.connect(conninfo)
    try:
        yield conn
    finally:
        await conn.close()


# =============================================================================
# Table Setup Functions
# =============================================================================


async def setup_results_tables_async(conn: psycopg.AsyncConnection) -> None:
    """
    Create benchmark results tables if they don't exist (async version).

    Creates the following tables:
    - benchmark_insert_summary: Insert benchmark aggregated metrics
    - benchmark_insert_granular: Per-batch insert metrics (retained for historical data)
    - benchmark_index_summary: Index creation timing metrics
    - benchmark_retrieval_summary: Retrieval benchmark aggregated metrics
    - benchmark_retrieval_granular: Per-query latencies (retained for historical data)

    Args:
        conn: Async psycopg connection.

    Note:
        Granular tables are still created for backward compatibility with existing
        data but new benchmarks no longer write to them (Phase 3 refactoring).
    """
    async with conn.cursor() as cur:
        # INSERT BENCHMARK TABLES
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_insert_summary (
                id serial PRIMARY KEY,
                run_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                database_name varchar(100),
                test_location varchar(100),
                dataset_size int,
                database_config text,
                total_records int,
                batch_size int,
                num_batches int,
                total_time_seconds float,
                throughput_vectors_per_sec float,
                avg_batch_time_seconds float,
                median_batch_time_seconds float,
                min_batch_time_seconds float,
                max_batch_time_seconds float
            );
        """)

        # Granular table retained for backward compatibility
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_insert_granular (
                id bigserial PRIMARY KEY,
                summary_id int REFERENCES benchmark_insert_summary(id),
                batch_number int,
                batch_time_seconds float,
                cumulative_time_seconds float,
                cumulative_vectors_inserted int,
                batch_size_actual int,
                instantaneous_throughput float
            );
        """)

        # INDEXING BENCHMARK TABLES
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_index_summary (
                id serial PRIMARY KEY,
                run_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                database_name varchar(100),
                test_location varchar(100),
                dataset_size int,
                database_config text,
                index_type varchar(50),
                index_parameters jsonb,
                table_row_count int,
                total_build_time_seconds float,
                notes text
            );
        """)

        # RETRIEVAL / HYBRID BENCHMARK TABLES
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_retrieval_summary (
                id serial PRIMARY KEY,
                run_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                database_name varchar(100),
                test_location varchar(100),
                dataset_size int,
                database_config text,
                test_type varchar(50),
                index_type varchar(50),
                top_k int,
                concurrency_level int,
                total_queries int,
                total_duration_seconds float,
                qps float,
                avg_latency_seconds float,
                p50_latency_seconds float,
                p95_latency_seconds float,
                p99_latency_seconds float
            );
        """)

        # Granular table retained for backward compatibility
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_retrieval_granular (
                id bigserial PRIMARY KEY,
                summary_id int REFERENCES benchmark_retrieval_summary(id),
                query_index int,
                latency_seconds float,
                status varchar(20) DEFAULT 'success'
            );
        """)

    await conn.commit()


# =============================================================================
# Metrics Storage Functions
# =============================================================================


async def save_retrieval_metrics_async(
    conn: psycopg.AsyncConnection,
    database_name: str,
    test_location: str,
    dataset_size: int,
    database_config: str,
    test_type: str,
    index_type: str,
    top_k: int,
    concurrency: int,
    latencies: List[float],
    total_duration: float,
    save_granular: bool = False,
) -> Optional[int]:
    """
    Save retrieval benchmark results to PostgreSQL asynchronously.

    This is the async equivalent of results_db.save_retrieval_metrics().
    Use this in async contexts to avoid blocking the event loop.

    Args:
        conn: Async psycopg connection.
        database_name: Name of the database being benchmarked.
        test_location: Location where test was run.
        dataset_size: Number of vectors in the dataset.
        database_config: Configuration description.
        test_type: Type of test (Vector Search, Hybrid Search, etc.).
        index_type: Type of index used.
        top_k: Number of results requested.
        concurrency: Concurrency level.
        latencies: List of query latencies in seconds.
        total_duration: Total benchmark duration in seconds.
        save_granular: Whether to save individual query latencies (deprecated, default False).

    Returns:
        summary_id if successful, None otherwise.
    """
    valid_latencies = [lat for lat in latencies if lat is not None]
    if not valid_latencies:
        logger.warning("no_valid_latencies_to_save")
        return None

    qps = len(valid_latencies) / total_duration if total_duration > 0 else 0
    avg_lat = statistics.mean(valid_latencies)
    p50 = statistics.median(valid_latencies)

    # Calculate percentiles
    if len(valid_latencies) >= 20:
        p95 = statistics.quantiles(valid_latencies, n=20)[18]
    else:
        p95 = max(valid_latencies)

    if len(valid_latencies) >= 100:
        p99 = statistics.quantiles(valid_latencies, n=100)[98]
    else:
        p99 = max(valid_latencies)

    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO benchmark_retrieval_summary (
                    database_name, test_location, dataset_size, database_config,
                    test_type, index_type,
                    top_k, concurrency_level, total_queries, total_duration_seconds,
                    qps, avg_latency_seconds, p50_latency_seconds,
                    p95_latency_seconds, p99_latency_seconds
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                ) RETURNING id;
            """,
                (
                    database_name,
                    test_location,
                    dataset_size,
                    database_config,
                    test_type,
                    index_type,
                    top_k,
                    concurrency,
                    len(valid_latencies),
                    total_duration,
                    qps,
                    avg_lat,
                    p50,
                    p95,
                    p99,
                ),
            )

            result = await cur.fetchone()
            summary_id = result[0]

            # Granular metrics saving is deprecated but retained for backward compatibility
            # New benchmarks should not set save_granular=True
            if save_granular and valid_latencies:
                granular_data = [
                    (summary_id, i, lat, "success")
                    for i, lat in enumerate(latencies)
                    if lat is not None
                ]

                await cur.executemany(
                    """
                    INSERT INTO benchmark_retrieval_granular (
                        summary_id, query_index, latency_seconds, status
                    ) VALUES (%s, %s, %s, %s)
                """,
                    granular_data,
                )

        await conn.commit()
        logger.debug(
            "retrieval_metrics_saved_async",
            summary_id=summary_id,
            test_type=test_type,
            top_k=top_k,
            concurrency=concurrency,
            qps=round(qps, 2),
        )

        return summary_id

    except Exception as e:
        logger.error("async_save_failed", error=str(e))
        await conn.rollback()
        return None


async def save_retrieval_metrics_batch_async(
    conn: psycopg.AsyncConnection, results: List[dict]
) -> List[int]:
    """
    Batch save multiple retrieval results efficiently.

    More efficient than saving one at a time when you have multiple results.

    Args:
        conn: Async psycopg connection.
        results: List of dicts with keys matching save_retrieval_metrics_async params.

    Returns:
        List of summary_ids for saved results.
    """
    summary_ids = []

    try:
        async with conn.cursor() as cur:
            for r in results:
                valid_latencies = [lat for lat in r["latencies"] if lat is not None]
                if not valid_latencies:
                    continue

                qps = (
                    len(valid_latencies) / r["total_duration"]
                    if r["total_duration"] > 0
                    else 0
                )
                avg_lat = statistics.mean(valid_latencies)
                p50 = statistics.median(valid_latencies)
                p95 = (
                    statistics.quantiles(valid_latencies, n=20)[18]
                    if len(valid_latencies) >= 20
                    else max(valid_latencies)
                )
                p99 = (
                    statistics.quantiles(valid_latencies, n=100)[98]
                    if len(valid_latencies) >= 100
                    else max(valid_latencies)
                )

                await cur.execute(
                    """
                    INSERT INTO benchmark_retrieval_summary (
                        database_name, test_location, dataset_size, database_config,
                        test_type, index_type,
                        top_k, concurrency_level, total_queries, total_duration_seconds,
                        qps, avg_latency_seconds, p50_latency_seconds,
                        p95_latency_seconds, p99_latency_seconds
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                """,
                    (
                        r["database_name"],
                        r["test_location"],
                        r["dataset_size"],
                        r["database_config"],
                        r["test_type"],
                        r["index_type"],
                        r["top_k"],
                        r["concurrency"],
                        len(valid_latencies),
                        r["total_duration"],
                        qps,
                        avg_lat,
                        p50,
                        p95,
                        p99,
                    ),
                )

                result = await cur.fetchone()
                summary_ids.append(result[0])

        await conn.commit()
        logger.info("batch_retrieval_metrics_saved_async", count=len(summary_ids))

        return summary_ids

    except Exception as e:
        logger.error("async_batch_save_failed", error=str(e))
        await conn.rollback()
        return []
