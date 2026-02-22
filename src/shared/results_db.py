"""
Results Database Module

Provides PostgreSQL connection and result storage functions for benchmark metrics.
Supports database separation:
- Results Database: Stores benchmark metrics (summary tables only)
- Vector Database: Stores actual vector data for pgvector benchmarks

Environment Variables:
    Shared Server Settings:
        PG_HOST: PostgreSQL host (shared across databases)
        PG_PORT: PostgreSQL port (default: 5432)
        PG_USER: PostgreSQL user
        PG_PASSWORD: PostgreSQL password

    Database-Specific Settings:
        RESULTS_PG_DATABASE: Results database name (default: benchmark_results)
        VECTOR_PG_DATABASE: Vector data database name (default: vector_data)
        PG_DATABASE: Legacy database name (default: vector_benchmark) - for backward compatibility

Usage:
    # For benchmark results storage
    conn = get_results_db_connection()

    # For pgvector benchmark data (tables with actual vector embeddings)
    conn = get_vector_db_connection()

    # Legacy function (uses PG_DATABASE, defaults to vector_benchmark)
    conn = get_pg_connection()
"""

import os
import statistics
from typing import Optional

import psycopg2

from shared.logger_structlog import setup_structlog

logger = setup_structlog()


# =============================================================================
# Connection Functions
# =============================================================================


def get_results_db_connection():
    """
    Establish connection to the Results Database (metrics only).

    This database stores benchmark results:
    - benchmark_insert_summary
    - benchmark_index_summary
    - benchmark_retrieval_summary

    Environment Variables:
        RESULTS_PG_HOST: Overrides PG_HOST for results DB
        RESULTS_PG_DATABASE: Database name (default: benchmark_results)

    Returns:
        psycopg2.connection: PostgreSQL connection to results database.

    Raises:
        Exception: If connection fails.
    """
    log = logger.bind(task="get_results_db_connection")

    # Use RESULTS_PG_* environment variables if set, otherwise fall back to PG_*
    host = os.getenv("RESULTS_PG_HOST", os.getenv("PG_HOST"))
    port = os.getenv("RESULTS_PG_PORT", os.getenv("PG_PORT", "5432"))
    database = os.getenv("RESULTS_PG_DATABASE", "benchmark_results")
    user = os.getenv("RESULTS_PG_USER", os.getenv("PG_USER"))
    password = os.getenv("RESULTS_PG_PASSWORD", os.getenv("PG_PASSWORD"))

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        log.debug("results_db_connected", host=host, database=database)
        return conn
    except Exception as e:
        log.error("results_db_connection_failed", host=host, database=database, error=str(e))
        raise


def get_vector_db_connection():
    """
    Establish connection to the Vector Database (pgvector data).

    This database stores actual vector embeddings for pgvector benchmarks:
    - embeddings table (or custom table name)

    Environment Variables:
        VECTOR_PG_HOST: Overrides PG_HOST for vector DB
        VECTOR_PG_DATABASE: Database name (default: vector_data)

    Returns:
        psycopg2.connection: PostgreSQL connection to vector database.

    Raises:
        Exception: If connection fails.
    """
    log = logger.bind(task="get_vector_db_connection")

    # Use VECTOR_PG_* environment variables if set, otherwise fall back to PG_*
    host = os.getenv("VECTOR_PG_HOST", os.getenv("PG_HOST"))
    port = os.getenv("VECTOR_PG_PORT", os.getenv("PG_PORT", "5432"))
    database = os.getenv("VECTOR_PG_DATABASE", "vector_data")
    user = os.getenv("VECTOR_PG_USER", os.getenv("PG_USER"))
    password = os.getenv("VECTOR_PG_PASSWORD", os.getenv("PG_PASSWORD"))

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        log.debug("vector_db_connected", host=host, database=database)
        return conn
    except Exception as e:
        log.error("vector_db_connection_failed", host=host, database=database, error=str(e))
        raise


def get_pg_connection():
    """
    Legacy connection function for backward compatibility.

    Uses PG_DATABASE environment variable (default: vector_benchmark).
    New code should use get_results_db_connection() or get_vector_db_connection().

    Returns:
        psycopg2.connection: PostgreSQL connection.

    Raises:
        Exception: If connection fails.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT", "5432"),
            database=os.getenv("PG_DATABASE", "vector_benchmark"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
        )
        return conn
    except Exception as e:
        logger.error("pg_results_db_connection_failed", error=str(e))
        raise


# =============================================================================
# Table Setup Functions
# =============================================================================


def setup_results_tables(conn) -> None:
    """
    Create benchmark results tables if they don't exist.

    Creates the following tables:
    - benchmark_insert_summary: Insert benchmark aggregated metrics
    - benchmark_insert_granular: Per-batch insert metrics (retained for historical data)
    - benchmark_index_summary: Index creation timing metrics
    - benchmark_retrieval_summary: Retrieval benchmark aggregated metrics
    - benchmark_retrieval_granular: Per-query latencies (retained for historical data)

    Args:
        conn: PostgreSQL connection (from get_results_db_connection or get_pg_connection).

    Note:
        Granular tables are still created for backward compatibility with existing
        data but new benchmarks no longer write to them (Phase 3 refactoring).
    """
    with conn.cursor() as cur:
        # 1. INSERT BENCHMARK TABLES
        cur.execute("""
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
        cur.execute("""
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

        # 2. INDEXING BENCHMARK TABLES
        cur.execute("""
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

        # 3. RETRIEVAL / HYBRID BENCHMARK TABLES
        cur.execute("""
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_retrieval_granular (
                id bigserial PRIMARY KEY,
                summary_id int REFERENCES benchmark_retrieval_summary(id),
                query_index int,
                latency_seconds float,
                status varchar(20) DEFAULT 'success'
            );
        """)

    conn.commit()


# =============================================================================
# Metrics Storage Functions
# =============================================================================


def save_insert_metrics(
    conn,
    database_name: str,
    test_location: str,
    dataset_size: int,
    database_config: str,
    total_records: int,
    batch_size: int,
    batch_times: list[float],
    granular_metrics: list[dict],
    total_time: float,
) -> Optional[int]:
    """
    Save insert benchmark results to PostgreSQL.

    Stores summary metrics only. Granular metrics parameter is retained for
    backward compatibility but is no longer written to the database.

    Args:
        conn: PostgreSQL connection (from get_results_db_connection).
        database_name: Name of the database being benchmarked.
        test_location: Location where test was run.
        dataset_size: Number of vectors in the dataset.
        database_config: Configuration description.
        total_records: Total number of records inserted.
        batch_size: Batch size used for inserts.
        batch_times: List of per-batch insert times in seconds.
        granular_metrics: Deprecated - retained for API compatibility.
        total_time: Total benchmark duration in seconds.

    Returns:
        int: Summary ID of the inserted record.
    """
    overall_throughput = total_records / total_time if total_time > 0 else 0

    summary_data = {
        "database_name": database_name,
        "test_location": test_location,
        "dataset_size": dataset_size,
        "database_config": database_config,
        "total_records": total_records,
        "batch_size": batch_size,
        "num_batches": len(batch_times),
        "total_time_seconds": total_time,
        "throughput_vectors_per_sec": overall_throughput,
        "avg_batch_time_seconds": statistics.mean(batch_times) if batch_times else 0,
        "median_batch_time_seconds": statistics.median(batch_times) if batch_times else 0,
        "min_batch_time_seconds": min(batch_times) if batch_times else 0,
        "max_batch_time_seconds": max(batch_times) if batch_times else 0,
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO benchmark_insert_summary (
                database_name, test_location, dataset_size, database_config,
                total_records, batch_size, num_batches,
                total_time_seconds, throughput_vectors_per_sec, avg_batch_time_seconds,
                median_batch_time_seconds, min_batch_time_seconds, max_batch_time_seconds
            ) VALUES (
                %(database_name)s, %(test_location)s, %(dataset_size)s, %(database_config)s,
                %(total_records)s, %(batch_size)s, %(num_batches)s,
                %(total_time_seconds)s, %(throughput_vectors_per_sec)s, %(avg_batch_time_seconds)s,
                %(median_batch_time_seconds)s, %(min_batch_time_seconds)s, %(max_batch_time_seconds)s
            ) RETURNING id;
        """,
            summary_data,
        )

        summary_id = cur.fetchone()[0]

        # NOTE: Granular metrics writes removed as per Phase 3 refactoring.
        # The benchmark_insert_granular table is retained for backward compatibility
        # with historical data, but new benchmarks no longer write to it.

    conn.commit()
    logger.info("insert_metrics_saved", summary_id=summary_id, database=database_name)
    return summary_id


def save_retrieval_metrics(
    conn,
    database_name: str,
    test_location: str,
    dataset_size: int,
    database_config: str,
    test_type: str,
    index_type: str,
    top_k: int,
    concurrency: int,
    latencies: list[float],
    total_duration: float,
) -> Optional[int]:
    """
    Save retrieval benchmark results to PostgreSQL.

    Stores summary metrics only (aggregated latency statistics).

    Args:
        conn: PostgreSQL connection (from get_results_db_connection).
        database_name: Name of the database being benchmarked.
        test_location: Location where test was run.
        dataset_size: Number of vectors in the dataset.
        database_config: Configuration description.
        test_type: Type of test (Vector Search, Filtered Search, Hybrid Search).
        index_type: Type of index used.
        top_k: Number of results requested.
        concurrency: Concurrency level.
        latencies: List of query latencies in seconds.
        total_duration: Total benchmark duration in seconds.

    Returns:
        int: Summary ID of the inserted record, or None if no valid latencies.
    """
    valid_latencies = [lat for lat in latencies if lat is not None]
    if not valid_latencies:
        logger.warning("no_valid_latencies_to_save")
        return None

    # Calculate aggregate metrics
    qps = len(valid_latencies) / total_duration if total_duration > 0 else 0
    avg_lat = statistics.mean(valid_latencies)
    p50 = statistics.median(valid_latencies)

    # Calculate percentiles with fallback for small sample sizes
    if len(valid_latencies) >= 20:
        p95 = statistics.quantiles(valid_latencies, n=20)[18]
    else:
        p95 = max(valid_latencies)

    if len(valid_latencies) >= 100:
        p99 = statistics.quantiles(valid_latencies, n=100)[98]
    else:
        p99 = max(valid_latencies)

    with conn.cursor() as cur:
        cur.execute(
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

        summary_id = cur.fetchone()[0]

        # NOTE: Granular metrics writes removed as per Phase 3 refactoring.
        # The benchmark_retrieval_granular table is retained for backward compatibility
        # with historical data, but new benchmarks no longer write to it.

    conn.commit()
    logger.info(
        "retrieval_metrics_saved",
        summary_id=summary_id,
        test_type=test_type,
        top_k=top_k,
        concurrency=concurrency,
    )
    return summary_id
