"""
PostgreSQL Vector Search Benchmark - AsyncPG Driver

This benchmark uses the asyncpg driver for PostgreSQL, which provides:
- True async I/O with minimal GIL impact during network operations
- Native binary protocol for efficient data transfer
- Connection pooling with per-connection vector type registration

Implements three search scenarios consistent with other vector database benchmarks:
1. Vector Search - Pure vector similarity search
2. Filtered Search - Vector search with metadata filtering
3. Hybrid Search - RRF combination of vector and full-text search

Supports multiple datasets:
- Aerospace faults dataset (legacy): Uses aircraft_type filter in metadata
- WoT dataset: Uses book_name filter in metadata

Key implementation details:
- Connection pool created once and reused across all scenarios
- Pool size: max_concurrency + buffer (configurable)
- Vector type registered on each connection via pool init callback
- Semaphore-based concurrency control for consistent benchmarking
- Results saved to shared PostgreSQL results database
"""

import asyncio
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

import numpy as np
import asyncpg
from pgvector.asyncpg import register_vector

# Timeout for acquiring a connection from the pool (seconds).
# Prevents indefinite hangs when pool connections are broken.
POOL_ACQUIRE_TIMEOUT = float(os.getenv("POOL_ACQUIRE_TIMEOUT", "30"))

# Per-query timeout (seconds). Prevents single queries from blocking forever.
QUERY_TIMEOUT = float(os.getenv("QUERY_TIMEOUT", "120"))

# Maximum consecutive failures before recreating the pool.
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "10"))

# Add paths for imports
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent))

import common
from shared import dataset, results_db
from shared.logger_structlog import setup_structlog

logger = setup_structlog()

# Dataset type detection
DATASET_TYPE = "aerospace"


def detect_dataset_type_from_query_file() -> str:
    """
    Detect dataset type from query file path.

    Returns:
        str: 'wot' or 'aerospace'
    """
    if 'wot' in common.QUERY_FILE.lower():
        return 'wot'
    return 'aerospace'


def get_filter_field(dataset_type: str) -> str:
    """
    Get the filter field name based on dataset type.

    Args:
        dataset_type: 'wot' or 'aerospace'

    Returns:
        str: Filter field name (in metadata JSONB)
    """
    if dataset_type == 'wot':
        return 'book_name'
    else:
        return 'aircraft_type'


def get_filter_value(query: dict, dataset_type: str) -> str:
    """
    Get the filter value from query based on dataset type.

    Uses the standardized filters dict from query, which is populated by
    load_test_queries() based on the dataset format.

    Args:
        query: Query dict with 'filters' dict containing {field: value} pairs
        dataset_type: 'wot' or 'aerospace'

    Returns:
        str: Filter value
    """
    filters = query.get('filters', {})

    if dataset_type == 'wot':
        return filters.get('book_name', query.get('filter_value', '00. New Spring'))
    else:
        return filters.get('aircraft_type', query.get('aircraft_type', '737-NG')) or '737-NG'

# =============================================================================
# Connection Pool Management
# =============================================================================

async def init_db_connection(conn):
    """
    Initialize each connection in the pool.
    Registers the vector type for pgvector operations.
    """
    await register_vector(conn)


async def create_connection_pool() -> asyncpg.Pool:
    """
    Create an asyncpg connection pool with proper sizing and vector registration.

    Pool sizing strategy:
    - min_size: Keep connections warm to avoid cold-start latency
    - max_size: max_concurrency + buffer to handle peak load

    Resilience features:
    - command_timeout: prevents individual queries from hanging
    - Caller uses POOL_ACQUIRE_TIMEOUT on each acquire() call
    """
    if not common.validate_config():
        raise RuntimeError("Invalid database configuration")

    pool = await asyncpg.create_pool(
        host=common.PG_HOST,
        port=common.PG_PORT,
        user=common.PG_USER,
        password=common.PG_PASSWORD,
        database=common.PG_DATABASE,
        min_size=common.POOL_MIN_SIZE,
        max_size=common.POOL_MAX_SIZE,
        command_timeout=QUERY_TIMEOUT,
        init=init_db_connection
    )

    logger.info("connection_pool_created",
                driver="asyncpg",
                min_size=common.POOL_MIN_SIZE,
                max_size=common.POOL_MAX_SIZE,
                command_timeout=QUERY_TIMEOUT,
                acquire_timeout=POOL_ACQUIRE_TIMEOUT)

    return pool


async def recreate_connection_pool(old_pool: Optional[asyncpg.Pool]) -> asyncpg.Pool:
    """
    Close the old pool (if any) and create a fresh one.

    This is the recovery path when too many connections in the existing pool
    are broken (e.g., server closed idle connections via LoadBalancer timeout).
    """
    if old_pool is not None:
        try:
            await asyncio.wait_for(old_pool.close(), timeout=10)
        except Exception:
            old_pool.terminate()
    logger.warning("recreating_connection_pool", reason="excessive_failures")
    return await create_connection_pool()


# =============================================================================
# Query Execution Functions
# =============================================================================

async def execute_vector_search(
    pool: asyncpg.Pool,
    query_embedding: np.ndarray,
    top_k: int
) -> Optional[float]:
    """Execute a pure vector search and return latency in seconds."""
    try:
        async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT) as conn:
            t0 = time.perf_counter()

            query = f"""
                SELECT id, 1 - (embedding <=> $1) AS similarity
                FROM {common.TABLE_NAME}
                ORDER BY embedding <=> $1
                LIMIT {top_k}
            """
            await conn.fetch(query, query_embedding)

            return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e) or type(e).__name__)
        return None


async def execute_filtered_search(
    pool: asyncpg.Pool,
    query_embedding: np.ndarray,
    filter_field: str,
    filter_value: str,
    top_k: int
) -> Optional[float]:
    """Execute a filtered vector search and return latency in seconds."""
    try:
        async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT) as conn:
            t0 = time.perf_counter()

            query = f"""
                SELECT id, 1 - (embedding <=> $1) AS similarity
                FROM {common.TABLE_NAME}
                WHERE metadata->>'{filter_field}' = $2
                ORDER BY embedding <=> $1
                LIMIT {top_k}
            """
            await conn.fetch(query, query_embedding, filter_value)

            return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e) or type(e).__name__)
        return None


async def execute_hybrid_search(
    pool: asyncpg.Pool,
    query_embedding: np.ndarray,
    keyword: str,
    top_k: int
) -> Optional[float]:
    """Execute a hybrid (vector + text) search and return latency in seconds."""
    try:
        async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT) as conn:
            t0 = time.perf_counter()

            vector_limit = top_k * 2
            query = f"""
                WITH vector_search AS (
                    SELECT id, 
                           ROW_NUMBER() OVER (ORDER BY embedding <=> $1) as rank,
                           1 - (embedding <=> $1) as vector_similarity
                    FROM {common.TABLE_NAME}
                    LIMIT {vector_limit}
                ),
                text_search AS (
                    SELECT id,
                           ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), 
                                                               plainto_tsquery('english', $2)) DESC) as rank,
                           ts_rank(to_tsvector('english', content), plainto_tsquery('english', $2)) as text_score
                    FROM {common.TABLE_NAME}
                    WHERE to_tsvector('english', content) @@ plainto_tsquery('english', $2)
                    LIMIT {vector_limit}
                ),
                rrf_scores AS (
                    SELECT 
                        COALESCE(v.id, t.id) as id,
                        (COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0)) as rrf_score,
                        v.vector_similarity,
                        t.text_score
                    FROM vector_search v
                    FULL OUTER JOIN text_search t ON v.id = t.id
                )
                SELECT id, rrf_score as similarity
                FROM rrf_scores
                ORDER BY rrf_score DESC
                LIMIT {top_k}
            """
            await conn.fetch(query, query_embedding, keyword)

            return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e) or type(e).__name__)
        return None


# =============================================================================
# Benchmark Execution
# =============================================================================

async def run_concurrent_queries(
    pool: asyncpg.Pool,
    queries: List[dict],
    test_type: str,
    top_k: int,
    concurrency: int,
    num_queries: int,
    dataset_type: str
) -> tuple[List[float], float]:
    """
    Run queries with controlled concurrency using a semaphore.

    Includes a per-query timeout via asyncio.wait_for so that broken
    pool connections cannot cause the entire gather to hang indefinitely.

    Args:
        pool: Connection pool
        queries: List of query dictionaries
        test_type: "Vector Search", "Filtered Search", or "Hybrid Search"
        top_k: Number of results to return
        concurrency: Maximum concurrent queries
        num_queries: Total number of queries to execute
        dataset_type: 'wot' or 'aerospace'

    Returns:
        Tuple of (latencies list, total duration)
    """
    semaphore = asyncio.Semaphore(concurrency)
    filter_field = get_filter_field(dataset_type)

    # Overall timeout per individual query task: acquire + execute + buffer
    task_timeout = POOL_ACQUIRE_TIMEOUT + QUERY_TIMEOUT + 10

    async def bounded_query(idx: int) -> Optional[float]:
        async with semaphore:
            query = queries[idx % len(queries)]
            query_embedding = np.array(query['embedding'])

            if test_type == "Vector Search":
                return await execute_vector_search(pool, query_embedding, top_k)
            elif test_type == "Filtered Search":
                filter_value = get_filter_value(query, dataset_type)
                return await execute_filtered_search(pool, query_embedding, filter_field, filter_value, top_k)
            elif test_type == "Hybrid Search":
                keyword = query.get('keyword', query.get('text', ''))
                return await execute_hybrid_search(pool, query_embedding, keyword, top_k)
            return None

    async def safe_bounded_query(idx: int) -> Optional[float]:
        """Wrap bounded_query with a hard timeout to prevent hangs."""
        try:
            return await asyncio.wait_for(bounded_query(idx), timeout=task_timeout)
        except asyncio.TimeoutError:
            logger.warning("query_timed_out", idx=idx, timeout=task_timeout)
            return None

    start_time = time.perf_counter()

    tasks = [safe_bounded_query(i) for i in range(num_queries)]
    latencies = await asyncio.gather(*tasks)

    total_duration = time.perf_counter() - start_time

    return list(latencies), total_duration


async def run_benchmark_scenario(
    pool: asyncpg.Pool,
    queries: List[dict],
    test_type: str,
    top_k: int,
    concurrency: int,
    dataset_type: str
) -> int:
    """
    Run a specific benchmark scenario and save results.

    Returns:
        Number of failed queries (0 = healthy, high = pool may be broken).
    """
    log = logger.bind(test_type=test_type, top_k=top_k, concurrency=concurrency, dataset_type=dataset_type)
    log.info("starting_scenario", driver="asyncpg")

    latencies, total_duration = await run_concurrent_queries(
        pool, queries, test_type, top_k, concurrency, common.NUM_QUERIES, dataset_type
    )

    valid_latencies = [l for l in latencies if l is not None]
    failed_count = len(latencies) - len(valid_latencies)

    if failed_count > 0:
        log.warning("queries_failed", count=failed_count, total=len(latencies))

    qps = len(valid_latencies) / total_duration if total_duration > 0 else 0
    log.info("scenario_complete",
             duration_seconds=round(total_duration, 2),
             successful_queries=len(valid_latencies),
             qps=round(qps, 2))

    # Save results to PostgreSQL results database
    pg_conn = results_db.get_results_db_connection()
    results_db.setup_results_tables(pg_conn)
    results_db.save_retrieval_metrics(
        pg_conn,
        database_name=common.get_database_name(),
        test_location=common.TEST_LOCATION,
        dataset_size=common.DATASET_SIZE,
        database_config=common.DATABASE_CONFIG,
        test_type=test_type,
        index_type="HNSW",
        top_k=top_k,
        concurrency=concurrency,
        latencies=latencies,
        total_duration=total_duration
    )
    pg_conn.close()

    return failed_count


# =============================================================================
# Warmup Phase (Critical for fair benchmarking)
# =============================================================================

WARMUP_QUERIES = int(os.getenv("WARMUP_QUERIES", "50"))


async def warmup_index(pool: asyncpg.Pool, queries: List[dict]):
    """
    Warm up the HNSW index by running queries before benchmarking.
    
    This is CRITICAL for fair benchmarking because:
    1. The HNSW index must be loaded from disk into PostgreSQL's shared_buffers
    2. First queries can take 100-1000x longer due to disk I/O
    3. Without warmup, the first scenario (top_k=1) shows artificially bad performance
    
    We run warmup queries with different top_k values to ensure all index paths are warmed.
    """
    logger.info("warming_up_hnsw_index", warmup_queries=WARMUP_QUERIES)
    
    warmup_top_ks = [1, 10, 50, 100]  # Warm different parts of the index
    
    for top_k in warmup_top_ks:
        for i in range(WARMUP_QUERIES // len(warmup_top_ks)):
            query = queries[i % len(queries)]
            query_embedding = np.array(query['embedding'])
            try:
                async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT) as conn:
                    query_sql = f"""
                        SELECT id, 1 - (embedding <=> $1) AS similarity
                        FROM {common.TABLE_NAME}
                        ORDER BY embedding <=> $1
                        LIMIT {top_k}
                    """
                    await conn.fetch(query_sql, query_embedding)
            except Exception as e:
                logger.warning("warmup_query_failed", error=str(e))
    
    logger.info("warmup_complete", note="HNSW index now in shared_buffers")


# =============================================================================
# Main Entry Point
# =============================================================================

async def main_async():
    """Main async entry point."""
    logger.info("=" * 80)
    logger.info("PostgreSQL Vector Search Benchmark - AsyncPG Driver")
    logger.info("=" * 80)

    # Detect dataset type
    dataset_type = detect_dataset_type_from_query_file()
    logger.info("dataset_type_detected", dataset_type=dataset_type)

    logger.info("benchmark_config",
                driver="asyncpg",
                num_queries=common.NUM_QUERIES,
                concurrency_levels=common.CONCURRENCY_LEVELS,
                top_k_values=common.TOP_K_VALUES,
                pool_min=common.POOL_MIN_SIZE,
                pool_max=common.POOL_MAX_SIZE,
                dataset_type=dataset_type)

    # Load queries
    queries = dataset.load_test_queries(common.QUERY_FILE, limit=common.NUM_QUERIES)
    logger.info("queries_loaded", count=len(queries), query_file=common.QUERY_FILE)

    # Create connection pool (once, reuse for all scenarios)
    pool = await create_connection_pool()

    try:
        # CRITICAL: Warm up the HNSW index before benchmarking
        await warmup_index(pool, queries)

        test_types = ["Vector Search", "Filtered Search", "Hybrid Search"]
        consecutive_scenario_failures = 0
        prev_concurrency = None

        for test_type in test_types:
            logger.info("=" * 60)
            logger.info(f"Starting: {test_type}")
            logger.info("=" * 60)

            for concurrency in common.CONCURRENCY_LEVELS:
                # Reset pool when concurrency level changes.
                # Idle connections from lower-concurrency scenarios get closed
                # server-side (LoadBalancer idle timeout / PG tcp_keepalives)
                # between transitions.  A fresh pool avoids reusing stale conns.
                if prev_concurrency is not None and concurrency != prev_concurrency:
                    logger.info("resetting_pool_for_concurrency_change",
                                prev=prev_concurrency, next=concurrency)
                    pool = await recreate_connection_pool(pool)
                    await asyncio.sleep(1)
                prev_concurrency = concurrency

                for top_k in common.TOP_K_VALUES:
                    try:
                        failed = await run_benchmark_scenario(
                            pool, queries, test_type, top_k, concurrency, dataset_type
                        )

                        # Track consecutive high-failure scenarios for pool health
                        failure_ratio = failed / common.NUM_QUERIES if common.NUM_QUERIES > 0 else 0
                        if failure_ratio > 0.5:
                            consecutive_scenario_failures += 1
                            logger.warning("high_failure_ratio",
                                           ratio=round(failure_ratio, 3),
                                           consecutive=consecutive_scenario_failures)
                        else:
                            consecutive_scenario_failures = 0

                        # Recreate pool if multiple scenarios in a row have >50% failures
                        if consecutive_scenario_failures >= 2:
                            logger.error("pool_health_degraded",
                                         consecutive_failures=consecutive_scenario_failures)
                            pool = await recreate_connection_pool(pool)
                            consecutive_scenario_failures = 0
                            await asyncio.sleep(2)

                    except Exception as e:
                        logger.error("scenario_failed",
                                     test_type=test_type, top_k=top_k,
                                     concurrency=concurrency, error=str(e))
                        consecutive_scenario_failures += 1

                        if consecutive_scenario_failures >= 2:
                            pool = await recreate_connection_pool(pool)
                            consecutive_scenario_failures = 0
                            await asyncio.sleep(2)

                    # Brief pause between scenarios
                    await asyncio.sleep(0.5)

    finally:
        await pool.close()
        logger.info("connection_pool_closed")

    logger.info("benchmark_complete", driver="asyncpg")


def main():
    """Synchronous entry point."""
    try:
        asyncio.run(main_async())
    except Exception as e:
        logger.error("benchmark_failed", error=str(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
