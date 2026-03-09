"""
PostgreSQL Vector Search Benchmark - Psycopg3 Asynchronous Driver

This benchmark uses the psycopg3 (psycopg) async driver for PostgreSQL, which provides:
- Modern async/await interface
- AsyncConnectionPool with built-in connection management
- Async vector type registration via configure callback

Implements three search scenarios consistent with other vector database benchmarks:
1. Vector Search - Pure vector similarity search
2. Filtered Search - Vector search with metadata filtering
3. Hybrid Search - RRF combination of vector and full-text search

Supports multiple datasets:
- Aerospace faults dataset (legacy): Uses aircraft_type filter in metadata
- WoT dataset: Uses book_name filter in metadata

Key implementation details:
- AsyncConnectionPool created once and reused across all scenarios
- Pool size: max_concurrency + buffer (configurable)
- Vector type registered via pool configure callback
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
import psycopg
from psycopg_pool import AsyncConnectionPool
from pgvector.psycopg import register_vector_async

# Add paths for imports
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent))

import common
from shared import dataset, results_db
from shared.logger_structlog import setup_structlog

logger = setup_structlog()


# =============================================================================
# Dataset Type Detection
# =============================================================================

# Dataset type (detected at runtime)
DATASET_TYPE = "aerospace"


def detect_dataset_type_from_query_file() -> str:
    """
    Detect the dataset type based on query file name.

    Returns:
        str: 'wot' or 'aerospace'
    """
    if 'wot' in common.QUERY_FILE.lower():
        return 'wot'
    return 'aerospace'


def get_filter_field(dataset_type: str) -> str:
    """Get the filter field name based on dataset type."""
    if dataset_type == 'wot':
        return 'book_name'
    return 'aircraft_type'


def get_filter_value(query: dict, dataset_type: str) -> str:
    """
    Get the filter value from query based on dataset type.

    Uses the standardized filters dict from query.
    """
    filters = query.get('filters', {})

    if dataset_type == 'wot':
        return filters.get('book_name', query.get('filter_value', '00. New Spring'))
    return filters.get('aircraft_type', query.get('aircraft_type', '737-NG')) or '737-NG'


# =============================================================================
# Connection Pool Management
# =============================================================================

async def configure_connection(conn: psycopg.AsyncConnection):
    """
    Configure each connection in the pool.
    Registers the vector type for pgvector operations.
    """
    await register_vector_async(conn)


async def create_connection_pool() -> AsyncConnectionPool:
    """
    Create a psycopg3 AsyncConnectionPool with proper sizing and vector registration.
    
    Pool sizing strategy:
    - min_size: Keep connections warm to avoid cold-start latency
    - max_size: max_concurrency + buffer to handle peak load
    """
    if not common.validate_config():
        raise RuntimeError("Invalid database configuration")
    
    conninfo = common.get_connection_string()
    
    pool = AsyncConnectionPool(
        conninfo,
        min_size=common.POOL_MIN_SIZE,
        max_size=common.POOL_MAX_SIZE,
        configure=configure_connection,
        open=False
    )
    
    await pool.open()
    
    logger.info("connection_pool_created",
                driver="psycopg3-async",
                min_size=common.POOL_MIN_SIZE,
                max_size=common.POOL_MAX_SIZE)
    
    return pool


# =============================================================================
# Query Execution Functions
# =============================================================================

async def execute_vector_search(
    pool: AsyncConnectionPool,
    query_embedding: np.ndarray,
    top_k: int
) -> Optional[float]:
    """Execute a pure vector search and return latency in seconds."""
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                ef = common.compute_hnsw_ef_search(top_k)
                await cur.execute(f"SET hnsw.ef_search = {ef}")

                t0 = time.perf_counter()
                
                await cur.execute(
                    common.get_vector_search_query(),
                    (query_embedding, query_embedding, top_k)
                )
                await cur.fetchall()
                
                return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e))
        return None


async def execute_filtered_search(
    pool: AsyncConnectionPool,
    query_embedding: np.ndarray,
    filter_field: str,
    filter_value: str,
    top_k: int
) -> Optional[float]:
    """
    Execute a filtered vector search and return latency in seconds.

    Sets hnsw.iterative_scan before the query so pgvector expands the HNSW graph
    in batches until top_k filter-passing results are found, guaranteeing a full
    result set even for selective predicates.  The setting is RESET in a finally
    block so the connection returns to the pool in a clean state.

    Args:
        pool: Connection pool
        query_embedding: Vector embedding
        filter_field: Metadata field to filter on (e.g., 'book_name' or 'aircraft_type')
        filter_value: Value to filter by
        top_k: Number of results to return
    """
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                ef = common.compute_hnsw_ef_search(top_k)
                await cur.execute(f"SET hnsw.ef_search = {ef}")
                # Enable iterative graph expansion for filtered search so the planner
                # keeps walking the HNSW graph until it has collected top_k rows that
                # satisfy the WHERE predicate, rather than silently short-returning.
                await cur.execute(f"SET hnsw.iterative_scan = '{common.HNSW_ITERATIVE_SCAN}'")

                try:
                    t0 = time.perf_counter()

                    await cur.execute(
                        common.get_filtered_search_query(filter_field=filter_field),
                        (query_embedding, filter_value, query_embedding, top_k)
                    )
                    await cur.fetchall()

                    elapsed = time.perf_counter() - t0
                finally:
                    # Always reset — even if the query raised — so the connection
                    # goes back to the pool without iterative_scan still active.
                    await cur.execute("RESET hnsw.iterative_scan")

                return elapsed
    except Exception as e:
        logger.warning("query_failed", error=str(e))
        return None


async def execute_hybrid_search(
    pool: AsyncConnectionPool,
    query_embedding: np.ndarray,
    keyword: str,
    top_k: int
) -> Optional[float]:
    """Execute a hybrid (vector + text) search and return latency in seconds."""
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                ef = common.compute_hnsw_ef_search(top_k * 2)
                await cur.execute(f"SET hnsw.ef_search = {ef}")

                t0 = time.perf_counter()
                
                # Hybrid search parameters:
                # %s - vector for vector_search
                # %s - vector for similarity calculation
                # %s - top_k * 2 for vector limit
                # %s - keyword for text search (3 occurrences)
                # %s - top_k * 2 for text limit
                # %s - final top_k
                await cur.execute(
                    common.get_hybrid_search_query(),
                    (query_embedding, query_embedding, top_k * 2,
                     keyword, keyword, keyword, top_k * 2,
                     top_k)
                )
                await cur.fetchall()
                
                return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e))
        return None


# =============================================================================
# Warmup Phase (Critical for fair benchmarking)
# =============================================================================

WARMUP_QUERIES = int(os.getenv("WARMUP_QUERIES", "50"))


async def warmup_index(pool: AsyncConnectionPool, queries: List[dict]):
    """
    Warm up the HNSW index by running queries before benchmarking.
    
    This is CRITICAL for fair benchmarking because:
    1. The HNSW index must be loaded from disk into PostgreSQL's shared_buffers
    2. First queries can take 100-1000x longer due to disk I/O
    3. Without warmup, the first scenario (top_k=1) shows artificially bad performance
    """
    logger.info("warming_up_hnsw_index", warmup_queries=WARMUP_QUERIES)
    
    warmup_top_ks = [1, 10, 50, 100]
    
    for top_k in warmup_top_ks:
        for i in range(WARMUP_QUERIES // len(warmup_top_ks)):
            query = queries[i % len(queries)]
            query_embedding = np.array(query['embedding'])
            try:
                await execute_vector_search(pool, query_embedding, top_k)
            except Exception as e:
                logger.warning("warmup_query_failed", error=str(e))
    
    logger.info("warmup_complete", note="HNSW index now in shared_buffers")


# =============================================================================
# Benchmark Execution
# =============================================================================

async def run_concurrent_queries(
    pool: AsyncConnectionPool,
    queries: List[dict],
    test_type: str,
    top_k: int,
    concurrency: int,
    num_queries: int,
    dataset_type: str
) -> tuple[List[float], float]:
    """
    Run queries with controlled concurrency using a semaphore.

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
                # Use keyword field (extracted keywords for WoT, full text for aerospace)
                keyword = query.get('keyword', query.get('text', ''))
                return await execute_hybrid_search(pool, query_embedding, keyword, top_k)
            return None

    start_time = time.perf_counter()

    # Launch all queries - semaphore controls actual concurrency
    tasks = [bounded_query(i) for i in range(num_queries)]
    latencies = await asyncio.gather(*tasks)

    total_duration = time.perf_counter() - start_time

    return list(latencies), total_duration


async def run_benchmark_scenario(
    pool: AsyncConnectionPool,
    queries: List[dict],
    test_type: str,
    top_k: int,
    concurrency: int,
    dataset_type: str
):
    """Run a specific benchmark scenario and save results."""
    log = logger.bind(test_type=test_type, top_k=top_k, concurrency=concurrency, dataset_type=dataset_type)
    log.info("starting_scenario", driver="psycopg3-async")

    latencies, total_duration = await run_concurrent_queries(
        pool, queries, test_type, top_k, concurrency, common.NUM_QUERIES, dataset_type
    )
    
    valid_latencies = [l for l in latencies if l is not None]
    failed_count = len(latencies) - len(valid_latencies)
    
    if failed_count > 0:
        log.warning("queries_failed", count=failed_count)
    
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


# =============================================================================
# Main Entry Point
# =============================================================================

async def main_async():
    """Main async entry point."""
    logger.info("=" * 80)
    logger.info("PostgreSQL Vector Search Benchmark - Psycopg3 Asynchronous Driver")
    logger.info("=" * 80)

    # Detect dataset type from query file
    dataset_type = detect_dataset_type_from_query_file()
    logger.info("dataset_type_detected", dataset_type=dataset_type, query_file=common.QUERY_FILE)

    logger.info("benchmark_config",
                driver="psycopg3-async",
                num_queries=common.NUM_QUERIES,
                concurrency_levels=common.CONCURRENCY_LEVELS,
                top_k_values=common.TOP_K_VALUES,
                pool_min=common.POOL_MIN_SIZE,
                pool_max=common.POOL_MAX_SIZE,
                hnsw_ef_search_min=common.HNSW_EF_SEARCH_MIN,
                hnsw_ef_search_formula="max(HNSW_EF_SEARCH_MIN, top_k)",
                hnsw_iterative_scan=common.HNSW_ITERATIVE_SCAN)

    # Load queries
    queries = dataset.load_test_queries(common.QUERY_FILE, limit=common.NUM_QUERIES)
    logger.info("queries_loaded", count=len(queries))

    # Create connection pool (once, reuse for all scenarios)
    pool = await create_connection_pool()

    # CRITICAL: Warm up the HNSW index before benchmarking
    await warmup_index(pool, queries)

    try:
        test_types = ["Vector Search", "Filtered Search", "Hybrid Search"]

        for test_type in test_types:
            logger.info("=" * 60)
            logger.info(f"Starting: {test_type}")
            logger.info("=" * 60)

            # Loop order: concurrency (outer) → top_k (inner)
            # Consistent with Milvus and MongoDB benchmarks
            for concurrency in common.CONCURRENCY_LEVELS:
                for top_k in common.TOP_K_VALUES:
                    await run_benchmark_scenario(
                        pool, queries, test_type, top_k, concurrency, dataset_type
                    )
                    # Brief pause between scenarios
                    await asyncio.sleep(0.5)

    finally:
        await pool.close()
        logger.info("connection_pool_closed")

    logger.info("benchmark_complete", driver="psycopg3-async")


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
