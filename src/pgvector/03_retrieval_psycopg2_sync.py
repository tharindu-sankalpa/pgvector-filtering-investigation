"""
PostgreSQL Vector Search Benchmark - Psycopg2 Synchronous Driver

This benchmark uses the psycopg2 driver (synchronous) for PostgreSQL, which provides:
- Mature and stable PostgreSQL adapter
- ThreadedConnectionPool for concurrent access
- Requires vector type registration per connection

Implements three search scenarios consistent with other vector database benchmarks:
1. Vector Search - Pure vector similarity search
2. Filtered Search - Vector search with metadata filtering
3. Hybrid Search - RRF combination of vector and full-text search

Supports multiple datasets:
- Aerospace faults dataset (legacy): Uses aircraft_type filter in metadata
- WoT dataset: Uses book_name filter in metadata

Key implementation details:
- ThreadedConnectionPool created once and reused across all scenarios
- Pool size: max_concurrency + buffer (configurable)
- Vector type registered on pool warmup and tracked per connection
- ThreadPoolExecutor for concurrent query execution
- Results saved to shared PostgreSQL results database
"""

import os
import sys
import time
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import psycopg2
from psycopg2 import pool
from pgvector.psycopg2 import register_vector

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


# Thread-safe set to track which connections have vector registered
_registered_connections = set()


# =============================================================================
# Connection Pool Management
# =============================================================================

def create_connection_pool() -> pool.ThreadedConnectionPool:
    """
    Create a psycopg2 ThreadedConnectionPool with proper sizing.
    
    Pool sizing strategy:
    - min_size: Keep connections warm to avoid cold-start latency
    - max_size: max_concurrency + buffer to handle peak load
    """
    if not common.validate_config():
        raise RuntimeError("Invalid database configuration")
    
    conn_pool = pool.ThreadedConnectionPool(
        common.POOL_MIN_SIZE,
        common.POOL_MAX_SIZE,
        host=common.PG_HOST,
        port=common.PG_PORT,
        user=common.PG_USER,
        password=common.PG_PASSWORD,
        database=common.PG_DATABASE
    )
    
    logger.info("connection_pool_created",
                driver="psycopg2-sync",
                min_size=common.POOL_MIN_SIZE,
                max_size=common.POOL_MAX_SIZE)
    
    return conn_pool


def warmup_pool(conn_pool: pool.ThreadedConnectionPool):
    """
    Warm up the connection pool by pre-registering vector type on connections.
    
    This avoids the overhead of vector type registration during benchmark execution.
    """
    logger.info("warming_up_pool")
    warmup_conns = []
    
    for _ in range(common.POOL_MIN_SIZE):
        conn = conn_pool.getconn()
        ensure_vector_registered(conn)
        warmup_conns.append(conn)
    
    for conn in warmup_conns:
        conn_pool.putconn(conn)
    
    logger.info("pool_warmed_up", connections_registered=len(warmup_conns))


def ensure_vector_registered(conn):
    """
    Ensure vector type is registered for a connection.
    Uses connection id to track registration and avoid redundant DB calls.
    """
    conn_id = id(conn)
    if conn_id not in _registered_connections:
        register_vector(conn)
        _registered_connections.add(conn_id)


# =============================================================================
# Query Execution Functions
# =============================================================================

def execute_vector_search(
    conn_pool: pool.ThreadedConnectionPool,
    query_embedding: np.ndarray,
    top_k: int
) -> Optional[float]:
    """Execute a pure vector search and return latency in seconds."""
    conn = conn_pool.getconn()
    try:
        ensure_vector_registered(conn)
        
        with conn.cursor() as cur:
            t0 = time.perf_counter()
            
            cur.execute(
                common.get_vector_search_query(),
                (query_embedding, query_embedding, top_k)
            )
            cur.fetchall()
            
            return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e))
        return None
    finally:
        conn_pool.putconn(conn)


def execute_filtered_search(
    conn_pool: pool.ThreadedConnectionPool,
    query_embedding: np.ndarray,
    filter_field: str,
    filter_value: str,
    top_k: int
) -> Optional[float]:
    """
    Execute a filtered vector search and return latency in seconds.

    Args:
        conn_pool: Connection pool
        query_embedding: Vector embedding
        filter_field: Metadata field to filter on (e.g., 'book_name' or 'aircraft_type')
        filter_value: Value to filter by
        top_k: Number of results to return
    """
    conn = conn_pool.getconn()
    try:
        ensure_vector_registered(conn)

        with conn.cursor() as cur:
            t0 = time.perf_counter()

            cur.execute(
                common.get_filtered_search_query(filter_field=filter_field),
                (query_embedding, filter_value, query_embedding, top_k)
            )
            cur.fetchall()

            return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e))
        return None
    finally:
        conn_pool.putconn(conn)


def execute_hybrid_search(
    conn_pool: pool.ThreadedConnectionPool,
    query_embedding: np.ndarray,
    keyword: str,
    top_k: int
) -> Optional[float]:
    """Execute a hybrid (vector + text) search and return latency in seconds."""
    conn = conn_pool.getconn()
    try:
        ensure_vector_registered(conn)
        
        with conn.cursor() as cur:
            t0 = time.perf_counter()
            
            # Hybrid search parameters:
            # %s - vector for vector_search
            # %s - vector for similarity calculation
            # %s - top_k * 2 for vector limit
            # %s - keyword for text search (3 occurrences)
            # %s - top_k * 2 for text limit
            # %s - final top_k
            cur.execute(
                common.get_hybrid_search_query(),
                (query_embedding, query_embedding, top_k * 2,
                 keyword, keyword, keyword, top_k * 2,
                 top_k)
            )
            cur.fetchall()
            
            return time.perf_counter() - t0
    except Exception as e:
        logger.warning("query_failed", error=str(e))
        return None
    finally:
        conn_pool.putconn(conn)


# =============================================================================
# Warmup Phase (Critical for fair benchmarking)
# =============================================================================

WARMUP_QUERIES = int(os.getenv("WARMUP_QUERIES", "50"))


def warmup_index(conn_pool: pool.ThreadedConnectionPool, queries: List[dict]):
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
                execute_vector_search(conn_pool, query_embedding, top_k)
            except Exception as e:
                logger.warning("warmup_query_failed", error=str(e))
    
    logger.info("warmup_complete", note="HNSW index now in shared_buffers")


# =============================================================================
# Benchmark Execution
# =============================================================================

def run_benchmark_scenario(
    conn_pool: pool.ThreadedConnectionPool,
    queries: List[dict],
    test_type: str,
    top_k: int,
    concurrency: int,
    dataset_type: str
):
    """Run a specific benchmark scenario and save results."""
    log = logger.bind(test_type=test_type, top_k=top_k, concurrency=concurrency, dataset_type=dataset_type)
    log.info("starting_scenario", driver="psycopg2-sync")

    filter_field = get_filter_field(dataset_type)

    def worker(query_idx: int) -> Optional[float]:
        query = queries[query_idx % len(queries)]
        query_embedding = np.array(query['embedding'])

        if test_type == "Vector Search":
            return execute_vector_search(conn_pool, query_embedding, top_k)
        elif test_type == "Filtered Search":
            filter_value = get_filter_value(query, dataset_type)
            return execute_filtered_search(conn_pool, query_embedding, filter_field, filter_value, top_k)
        elif test_type == "Hybrid Search":
            # Use keyword field (extracted keywords for WoT, full text for aerospace)
            keyword = query.get('keyword', query.get('text', ''))
            return execute_hybrid_search(conn_pool, query_embedding, keyword, top_k)
        return None
    
    start_time = time.perf_counter()
    latencies = []
    
    # Use ThreadPoolExecutor for concurrent query execution
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, i) for i in range(common.NUM_QUERIES)]
        
        for future in as_completed(futures):
            latencies.append(future.result())
    
    total_duration = time.perf_counter() - start_time
    
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

def main():
    """Main entry point."""
    logger.info("=" * 80)
    logger.info("PostgreSQL Vector Search Benchmark - Psycopg2 Synchronous Driver")
    logger.info("=" * 80)

    # Detect dataset type from query file
    dataset_type = detect_dataset_type_from_query_file()
    logger.info("dataset_type_detected", dataset_type=dataset_type, query_file=common.QUERY_FILE)

    logger.info("benchmark_config",
                driver="psycopg2-sync",
                num_queries=common.NUM_QUERIES,
                concurrency_levels=common.CONCURRENCY_LEVELS,
                top_k_values=common.TOP_K_VALUES,
                pool_min=common.POOL_MIN_SIZE,
                pool_max=common.POOL_MAX_SIZE)

    # Load queries
    queries = dataset.load_test_queries(common.QUERY_FILE, limit=common.NUM_QUERIES)
    logger.info("queries_loaded", count=len(queries))

    # Create and warm up connection pool (once, reuse for all scenarios)
    conn_pool = create_connection_pool()
    warmup_pool(conn_pool)

    # CRITICAL: Warm up the HNSW index before benchmarking
    warmup_index(conn_pool, queries)

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
                    run_benchmark_scenario(
                        conn_pool, queries, test_type, top_k, concurrency, dataset_type
                    )
                    # Brief pause between scenarios
                    time.sleep(0.5)

    finally:
        conn_pool.closeall()
        logger.info("connection_pool_closed")

    logger.info("benchmark_complete", driver="psycopg2-sync")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("benchmark_failed", error=str(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)
